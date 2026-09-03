# 05. RPC Proxy — gRPC をメッシュ越しに透過させる

## 1. 責務と目的

アプリケーションは通常こう書く：

```rust
let ch = Channel::from_static("http://10.5.3.72:50051").connect().await?;
let mut client = EntityManagerClient::new(ch);
```

これは **相手の IP を知っている**ことが前提で、DDIL メッシュでは成立しない。
mesh-bus では：

```rust
// 常にローカルの mbd に繋ぐ。宛先はサービス名で指定
let ch = Channel::from_static("unix:///run/mesh-bus/app.sock").connect().await?;
let mut client = EntityManagerClient::with_interceptor(ch, add_header("mb-service",
    "lattice.entities.v1.EntityManager"));
```

`mbd` が `mb-service` ヘッダ（または後述の権威ホスト名）を見て、LSDB から提供ノードを解決し、メッシュ越しに転送する。
**アプリのコードとサーバ実装は無改造**で動くことを目標にする。

## 2. アーキテクチャ

```text
Client App                 mbd(A)                     mbd(B)                Server App
    │                        │                          │                        │
    │─ HTTP/2 (unix sock) ──▶│                          │                        │
    │  :authority or         │ 1. resolve service       │                        │
    │  mb-service header     │    → NodeId B            │                        │
    │                        │ 2. open circuit          │                        │
    │                        │──── CircuitOpen ────────▶│                        │
    │                        │      (over 03 P0)        │ 3. local dial          │
    │                        │                          │──── HTTP/2 ───────────▶│
    │                        │                          │   unix:/…/svc.sock or  │
    │                        │                          │   127.0.0.1:50051      │
    │─ request frames ──────▶│──── CircuitData ────────▶│──── frames ───────────▶│
    │◀─ response frames ─────│◀─── CircuitData ─────────│◀─── frames ────────────│
```

`mbd` は **HTTP/2 フレームレベルのプロキシ**として動く。Protobuf のデコードはしない（できない：スキーマを知らない）。

## 3. サービス解決

### 3.1 サーバ側の登録

サーバ App は起動時にローカル `mbd` に自分を登録する：

```protobuf
service ServiceRegistry {
  rpc Register(stream RegisterRequest) returns (stream RegisterAck);  // stream = 生存確認
}
message RegisterRequest {
  repeated string services = 1;      // 提供する gRPC サービス完全名
  string endpoint = 2;               // "unix:/run/app/entities.sock" or "127.0.0.1:50051"
  uint32 weight = 3;
  uint32 max_concurrent = 4;
}
```

`mbd` はこれを LSA の `services` に載せる（02）。stream が切れたら登録解除し、LSA を更新する。

### 3.2 クライアント側の解決

宛先の指定方法を 2 つ用意する：

| 方法 | 書き方 | 用途 |
|---|---|---|
| ヘッダ | `mb-service: lattice.entities.v1.EntityManager` | 明示的。推奨 |
| 権威ホスト名 | `:authority = entities.mesh` | 既存コードが `Channel::from_static("http://entities.mesh")` のまま動く |
| ノード指定 | `mb-node: b7e1…` | 特定ノードへ強制（デバッグ、地理的に決まっている場合） |

ヘッダも `:authority` も無ければ、**gRPC のパス `/pkg.Service/Method` からサービス名を抽出**する。これが最も透過的だが、同名サービスが複数ノードにある場合の選好指定ができないので既定は「パスから抽出 + weight/cost で選択」。

解決ロジック：

```text
candidates = DiscoveryIndex.services[name]
  filter: routes.contains(node) && node が生存（LSA 未失効）
  score  = route.cost * 100 / max(weight, 1)
  同点は NodeId のハッシュで安定選択（同じクライアントは同じサーバへ = 接続再利用）
選択後は circuit を張っている間そのノードに固定（sticky）
```

**フェイルオーバー**：選択ノードへの経路が消えた／CircuitOpen がタイムアウトしたら、次候補へ 1 回だけ再試行する。**べき等でない RPC を勝手に再試行しない**ため、再試行は「CircuitOpen が失敗した（=リクエストがまだサーバに渡っていない）」場合のみ。データ送信開始後の失敗はクライアントに `UNAVAILABLE` を返す。

## 4. 仮想回線 (Circuit)

多数の同時 RPC を、少数の Link ストリームに多重化する。

```protobuf
message CircuitOpen {
  uint64 circuit_id = 1;      // 発信側でユニーク (node-local counter)
  string service    = 2;
  string method     = 3;      // ログ・認可用。プロキシ判断には使わない
  map<string,string> metadata = 4;   // gRPC metadata（認可情報を含む）
  uint32 deadline_ms = 5;
  uint32 initial_window = 6;  // 受信ウィンドウ (bytes)
}
message CircuitAccept { uint64 circuit_id = 1; uint32 initial_window = 2; }
message CircuitReject { uint64 circuit_id = 1; uint32 grpc_status = 2; string message = 3; }
message CircuitData   { uint64 circuit_id = 1; bool end_stream = 2; bytes payload = 3; }
message CircuitWindow { uint64 circuit_id = 1; uint32 increment = 2; }   // フロー制御
message CircuitClose  { uint64 circuit_id = 1; uint32 grpc_status = 2; string message = 3; }
```

- `CircuitOpen/Accept/Reject/Close/Window` は **P0**（制御）、`CircuitData` は **P2**（既定。Topic 同様サービスごとに設定可能）
- `payload` は **gRPC の length-prefixed message そのもの**。HTTP/2 の DATA フレームのペイロードをそのまま運ぶ。HTTP/2 のフレーミング自体は各端の `mbd` が終端する
- **双方向ストリーミング**は自然にサポートされる。`end_stream` を方向ごとに持つ

### 4.1 なぜ HTTP/2 をそのままトンネルしないか

`Tunnel` チャネルで生の HTTP/2 バイト列を流す方が実装は簡単だが：

- HTTP/2 のフロー制御ウィンドウと 03 の QoS が二重になり、優先度が守られない
- HPACK の状態が経路に依存し、経路変更で壊れる
- RPC 単位の優先度・キャンセル・デッドラインをメッシュ側で扱えない

よって **各端で HTTP/2 を終端し、メッセージ単位で運ぶ**。この判断が RPC プロキシ設計の核心。

### 4.2 フロー制御

Circuit ごとに受信ウィンドウを持つ（HTTP/2 と同じ考え方だがメッシュ側で管理）。

```text
初期ウィンドウ 64 KB（リンク種別で調整: Satcom 16 KB）
受信側 mbd が App へ書き込み成功 → CircuitWindow{increment} を返す
送信側は window を使い切ったら App からの読み取りを止める（= gRPC のバックプレッシャがアプリまで伝わる）
```

これで **遅い受信者が中継ノードのメモリを食い潰さない**。

### 4.3 デッドラインとキャンセル

- gRPC の `grpc-timeout` ヘッダを `CircuitOpen.deadline_ms` に写す
- 経路 RTT を差し引かない（サーバ側で早めに切れる方が安全）
- クライアントが切断 → `CircuitClose{CANCELLED}` を P0 で送出。サーバ側 `mbd` はローカル gRPC 接続を RST_STREAM で閉じる
- **中継ノードは circuit を知らない**（03 の Unicast パケットとして通るだけ）。キャンセルは端-端

### 4.4 Circuit の寿命管理

| 状況 | 動作 |
|---|---|
| 経路消失（route なし）が 5 s 継続 | `UNAVAILABLE` でクローズ |
| デッドライン超過 | `DEADLINE_EXCEEDED` |
| 相手ノードの LSA 失効 | 即 `UNAVAILABLE` |
| アイドル 300 s | クローズ（長時間ストリームは keepalive を要求） |
| 同時 circuit 数上限 (既定 1024/ノード) | 新規は `RESOURCE_EXHAUSTED` |

## 5. サーバ側の接続管理

`mbd(B)` は登録された各サービスのローカルエンドポイントへ **HTTP/2 接続をプールする**（1〜4 本、`max_concurrent` に応じて）。
CircuitOpen ごとに新しい TCP/unix 接続を張るのは遅すぎる。

- プール接続が死んだら再接続。再接続中の CircuitOpen は 1 s 待ってから `UNAVAILABLE`
- `max_concurrent` を超える circuit は `RESOURCE_EXHAUSTED` で reject（メッシュ側でキューしない。キューは遅延を隠して悪化させる）

## 6. 認可

`CircuitOpen.metadata` に入る認証情報と、発信ノードの `NodeId`（03 のヘッダから、mTLS で検証済みの経路で来たもの）を使う。

```text
policy.rules:
  - service: "lattice.c2.v1.TaskingService"
    method:  "*"
    allow_roles: ["c2-operator"]
  - service: "lattice.entities.v1.EntityManager"
    method:  "Get*"
    allow_roles: ["*"]
```

- ロールは **発信ノードの証明書の拡張フィールド**から取る（06）。App が名乗るのではなくノード証明書に紐づく
- **判定はサーバ側 `mbd` で行う**。クライアント側でも事前チェックして無駄な送信を防ぐが、権威はサーバ側
- 判定結果は `CircuitReject{PERMISSION_DENIED}`

**重要な限界**：`CircuitData` の内容は E2E 暗号化しない（サーバ側 `mbd` が HTTP/2 に戻す必要があるため）。中継ノードから守るには **ホップ間 mTLS に依存する**。より強い保護が必要な RPC は、アプリケーション層で payload を暗号化する（mesh-bus はその鍵配布を 06 の API で提供する）。この非対称性は Pub/Sub（E2E 可能）と RPC（ホップ間のみ）の本質的な違いとして明記しておく。

## 7. 実装方針

```text
mb-rpc-proxy/
├── ingress.rs      App 側 HTTP/2 サーバ (hyper h2)。ストリーム→Circuit へ変換
├── egress.rs       Server App への HTTP/2 クライアント接続プール
├── resolver.rs     DiscoveryIndex + RouteTable からノード選択、キャッシュ
├── circuit.rs      Circuit 状態機械（I/O なし）: open/accept/data/window/close
├── registry.rs     ServiceRegistry gRPC 実装、LSA への反映
└── authz.rs        ポリシー評価
```

- `ingress`/`egress` は tokio + `h2` crate（tonic の下層）を直接使う。tonic はサーバ実装用で、透過プロキシにはフレームレベルの制御が要る
- `circuit.rs` だけが I/O なしコンポーネント。HTTP/2 の終端は必然的に I/O 側に置く（シミュレーションでは `circuit.rs` のみをテストし、HTTP/2 終端は netns 統合テストで検証）

## 8. 検証すべき性質（08 で自動化）

| 性質 | 検証 |
|---|---|
| 透過性：無改造の tonic クライアント／サーバが 3 ホップ越しに通信できる | netns で 3 ノード構成、既存の gRPC サンプルを動かす |
| 双方向ストリーミングが動く | 長時間 bidi ストリームで双方向にデータを流し続ける |
| フロー制御：遅い受信者で中継のメモリが増えない | サーバが 1 msg/s しか読まない状態で 10 MB/s 送信、中継 RSS を監視 |
| デッドライン伝播 | 100 ms デッドラインで 500 ms かかるサーバ → `DEADLINE_EXCEEDED` |
| キャンセル伝播 | クライアント切断でサーバ側ハンドラが 100 ms 以内に abort |
| フェイルオーバー：Open 失敗時のみ再試行 | サーバをデータ送信後に kill → 再試行されず `UNAVAILABLE` |
| 経路変更中の既存 circuit が生存する | 通信中に別経路へ切替、ストリームが切れないこと |
