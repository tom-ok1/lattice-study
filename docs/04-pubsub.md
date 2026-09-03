# 04. Pub/Sub — Topic モデル・購読セマンティクス・Store-and-Forward・Backfill

## 1. 責務

中央ブローカーなしに、Topic 単位の publish / subscribe を提供する。
DDIL 環境の要求から、通常の Pub/Sub にはない以下を満たす：

- 切断中に発生したメッセージを、再接続後に **Backfill** として受け取れる
- Backfill が **ライブ配信を妨げない**（帯域の余りだけを使う）
- 中継ノードは内容を読めない（E2E 暗号）
- 購読者は「最新値だけ欲しい」「今から欲しい」「t 以降を全部欲しい」を選べる

## 2. Topic モデル

```rust
pub struct TopicKey {
    pub name: String,        // "lattice.tracks.v1"
    pub partition: Bytes,    // asset_id 等。空 = 単一パーティション
}
```

`(name, partition)` を分けるのは、**購読・保存・優先度をパーティション単位で制御**したいから。
たとえば `tracks / asset=drone-17` だけを購読する、`tracks / asset=自分` だけを保存する、が自然に書ける。

### 2.1 Topic ポリシー（定義）

Topic ごとの振る舞いは **設定ファイル + LSA での配布**で決める。推測しない。

```toml
[[topics]]
name        = "lattice.c2.commands.v1"
priority    = "P0"
mode        = "log"          # log | latest
retention   = { duration = "72h", bytes = "64MB" }
e2e         = "required"     # required | optional | none
signed      = true           # 発行者署名を付ける（改竄検知を暗号化と別に）
compression = { codec = "zstd", dict = "c2-v1" }
max_rate    = { msgs = 100, bytes = "64KB", per = "1s" }

[[topics]]
name        = "lattice.tracks.v1"
priority    = "P1"
mode        = "latest"       # パーティションごとに最新 1 件のみ保持・conflate 可
retention   = { duration = "1h", bytes = "256MB" }
e2e         = "required"
signed      = false
compression = { codec = "zstd", dict = "tracks-v1" }
```

- `mode = latest`：03 の conflation を有効化。ストアも各パーティション 1 件（+短い履歴）
- `mode = log`：全件を順序どおり保持。Backfill 対象

ポリシーの配布：ミッション設定として事前配布 + `policy_hash` を LSA に載せて不一致を検出（06）。不一致ノードには警告を出すが通信は止めない（部分更新中の運用を殺さないため）。

## 3. メッセージエンベロープ

```protobuf
message Envelope {
  bytes  origin      = 1;   // 発行ノード NodeId
  string topic       = 2;
  bytes  partition   = 3;
  uint64 seq         = 4;   // (origin, topic, partition) ごとに単調増加
  uint64 ts_ms       = 5;   // 発行ノードのローカル時刻（参考）
  uint32 key_epoch   = 6;   // 使用した Topic 群鍵の世代（06）
  uint32 dict_id     = 7;   // zstd 辞書
  bytes  ciphertext  = 8;   // AES-256-GCM(plaintext = zstd(payload)), AAD = 上記フィールド
  bytes  signature   = 9;   // 任意。signed=true のとき origin 鍵で header+ciphertext に署名
}
```

- **順序と重複排除は `(origin, topic, partition, seq)`**。時刻に依存しない
- AAD にヘッダを含めるので、中継が topic や seq を書き換えると復号が失敗する
- 03 の `conflate_key` は送信元が `fxhash(origin ‖ partition)` として計算し、転送ヘッダに載せる。
  暗号文から導出しないのは、暗号文が毎回変わることと、中継ノードが本文を読めないことの両方による

## 4. 購読セマンティクス

```protobuf
message Subscribe {
  string topic     = 1;
  bytes  partition = 2;      // 空 = 全パーティション（ワイルドカード）
  StartAt start    = 3;
  Filter filter    = 4;      // 任意。ノード側で評価してネットワークを節約
  uint32 backfill_window = 5; // Backfill 同時要求バイト数（既定 256 KB）
}

message StartAt {
  oneof at {
    Empty  now      = 1;   // これ以降の live のみ
    Empty  latest   = 2;   // 各パーティションの最新 1 件 + 以降の live
    uint64 since_ms = 3;   // その時刻以降を Backfill してから live
    Cursor cursor   = 4;   // 前回の続きから（アプリが永続化した位置）
    Empty  complete = 5;   // 保持されている全履歴 + live
  }
}

// cursor = 各 origin ごとの最終受信 seq
message Cursor { map<string /*origin hex*/, uint64> last_seq = 1; }
```

`cursor` が最重要。アプリが自分の処理済み位置を永続化しておけば、再起動でも再接続でも **「取りこぼしたぶんだけ」** を取り直せる。時刻ではなく seq で表現するので時刻ずれに強い。

### 4.1 Filter

```protobuf
message Filter {
  repeated string partition_prefix = 1;   // partition の前方一致
  // 将来: CEL 式による payload フィルタ（要復号 → 購読者ノードでのみ評価可能）
}
```

E2E 暗号があるので **中継ノードは payload フィルタを評価できない**。フィルタは
(a) partition による絞り込み → LSA の TopicAd に載せて発行元でのフィルタ、
(b) 購読者ノードのローカル評価（帯域は節約できない）
の 2 段。(a) を主に使う設計にする。

## 5. 配信パス

```text
App(publisher) ──gRPC──▶ local mbd
                          ├─ 1. ポリシー適用（rate limit, 圧縮, 暗号化, 署名）
                          ├─ 2. ローカルストアに append（自分が STORE role のとき）
                          ├─ 3. DiscoveryIndex から購読者集合 S を取得
                          ├─ 4. S から自ノードを除き、ローカル購読者へ直接 deliver
                          └─ 5. 残りを 03 の Explicit Multicast で送出
                                    │
                          ┌─────────┴─────────┐
                       relay X             relay Y     ← 内容を読まない
                          │                   │
                    subscriber A       subscriber B
                          ├─ 6. 復号・検証
                          ├─ 7. dedup: (origin, seq) 既知なら破棄
                          ├─ 8. ローカルストアに append（STORE role のとき）
                          └─ 9. ローカル App へ gRPC stream で配信
```

## 6. Store-and-Forward

### 6.1 どのノードが保存するか

Topic ポリシーで `store_role` を決める：

| ロール | 保存内容 | 想定 |
|---|---|---|
| `publisher` | 自分が発行したもの | 全ノード既定。自分の発行分は必ず持つ |
| `subscriber` | 自分が購読したもの | アプリが再起動しても cursor から再開できる |
| `designated` | 設定で指定した Topic 全体 | 車両・地上局など容量のあるノードがリレー兼アーカイブになる |
| `none` | 保存しない | 極小ノード |

`designated` ストアの存在が **DDIL で効く**。ドローンが基地局と切れている間、中間の車両が持っていれば、ドローン復帰後に車両から Backfill できる（発行元まで戻らなくていい）。

### 6.2 ストレージ

```rust
pub trait Storage {
    fn append(&mut self, key: &TopicKey, origin: NodeId, seq: u64, env: &[u8]) -> Result<()>;
    fn scan(&self, key: &TopicKey, origin: NodeId, from_seq: u64, limit_bytes: usize)
        -> Result<Vec<(u64, Bytes)>>;
    fn latest(&self, key: &TopicKey, origin: NodeId) -> Result<Option<(u64, Bytes)>>;
    fn range(&self, key: &TopicKey, origin: NodeId) -> Result<Option<(u64, u64)>>; // tail, head
    fn prune(&mut self, key: &TopicKey, before: Instant, max_bytes: u64) -> Result<u64>;
}
```

- 実装：**`redb`**（純 Rust の組込 B-tree KV、ACID、mmap 不要）。キーは `(topic_hash, partition, origin, seq)` の連結でスキャンが順序どおりになる
- `mode=latest` の Topic は別テーブルで `(topic, partition, origin) → 最新 1 件`
- **fsync ポリシー**：P0 Topic は毎 append で fsync、それ以外は 200 ms ごとにバッチ。電源断で 200 ms 分を失うのは許容（軍用ハードは瞬断で落ちない前提）
- prune は 60 秒ごとのバックグラウンドタスク。retention の duration と bytes の両方を適用
- in-memory 実装（`BTreeMap`）をシミュレーション用に用意

### 6.3 ストレージ量の見積もり

tracks (P1, latest, 100 パーティション, 1 KB/msg, 1 Hz, 1h) ≈ 100 × 3600 × 1 KB = 360 MB → **latest モードなら 100 KB**。
c2 commands (P0, log, 10 msg/min, 2 KB, 72h) ≈ 86 MB。
現実的な範囲に収まる。`designated` ストアだけ数 GB を見込む。

## 7. Backfill

### 7.1 検出

購読者は各 `(topic, partition, origin)` について自分の `last_seq` を持つ。
LSA の `TopicAd.head_seq`（発行元が公開する最新 seq）と比較して **ギャップを検出**する。

```text
subscriber の last_seq(origin=D, tracks) = 1042
LSA(D).topics[tracks].head_seq            = 1187
→ 1043..=1187 が欠落。145 件を要求する
```

LSA が来るだけでギャップが分かるのが利点。ハートビートを別途持たなくていい。

### 7.2 要求先の選択

```text
候補 = DiscoveryIndex.topic_stores[topic] のうち
         tail_seq <= 1043 && head_seq >= 1043 を満たすノード
選択 = SPF cost が最小のノード（発行元とは限らない）
```

近い `designated` ストアから取れるので、細い遠距離リンクを使わずに済む。

### 7.3 プロトコル

```protobuf
message BackfillRequest {
  string topic = 1;  bytes partition = 2;  bytes origin = 3;
  uint64 from_seq = 4;  uint64 to_seq = 5;
  uint32 window_bytes = 6;    // これ以上は送るな（credit）
  uint64 request_id = 7;
}
message BackfillChunk {
  uint64 request_id = 1;
  repeated bytes envelopes = 2;   // そのまま（再暗号化しない。E2E のまま転送）
  uint64 next_seq = 3;            // 続きがある場合の再開位置
  bool   complete = 4;
  repeated Gap gaps = 5;          // 保持していない範囲（prune 済み）を明示
}
message Gap { uint64 from = 1; uint64 to = 2; }
```

- **すべて P3** で送る。03 の DRR で余り帯域のみを使う
- `window_bytes` が credit そのもの。応答側は window を超えたら止め、要求側が次の `BackfillRequest` を出す（pull 型）。中継の輻輳は要求側が window を絞ることで制御
- **`gaps` を明示する**のが重要。「送られてこない」と「そもそも存在しない」を区別できないと、購読者は永遠に待つ。prune 済み範囲は Gap で返し、購読者は cursor を進める
- 同時要求数の上限：`max_inflight_backfill = 4`（origin × topic の組ごとではなく、ノード全体で）

### 7.4 Live と Backfill の合流

```text
時刻 →

live  :                              P6  P7  P8  ──▶ App に即座に配信
backfill:  P3  P4  P5 ────────────────────────────▶ App に配信

App が見る順序（既定 = interleaved）:
  P6, P7, P3, P4, P8, P5, ...
```

購読オプションで 2 つのモードを提供：

| モード | 動作 | 用途 |
|---|---|---|
| `interleaved`（既定） | live を即配信、backfill は届き次第配信。App が順序を扱う | 状況認識。今の情報が最優先 |
| `ordered` | backfill 完了まで live をバッファし、seq 順に配信 | ログ処理、リプレイ |

`ordered` はバッファが溢れる（メモリ上限 32 MB）と backfill を諦めて Gap を通知し live に切り替える。**無限に待たない**。

配信時は各メッセージに `is_backfill: bool` と `seq` を付けるので、App が自分で判断することもできる。

## 8. コンポーネントとしての形（I/O なし）

```rust
pub enum PubSubEvent {
    LocalPublish { topic: TopicKey, payload: Bytes, opts: PublishOpts },
    LocalSubscribe { sub_id: SubId, req: Subscribe },
    LocalUnsubscribe(SubId),
    Inbound(Packet),                     // 03 から。Envelope または Backfill*
    DiscoveryUpdated(Arc<DiscoveryIndex>),
    StorageResult { req: StorageReqId, result: StorageResult },  // 非同期ストレージ
    Timer(PubSubTimer),                  // GapScan / BackfillRetry / Prune
}

pub enum PubSubAction {
    SendMulticast { dsts: Vec<NodeId>, priority: u8, conflate_key: u64, payload: Bytes },
    SendUnicast   { dst: NodeId, priority: u8, payload: Bytes },
    StorageOp(StorageOp),                // append / scan / prune。実行は runtime 側
    DeliverToApp { sub_id: SubId, msg: DeliveredMsg },
    SetTimer { timer: PubSubTimer, at: Instant },
    Metric(PubSubMetric),
    UpdateLocalTopicAds(Vec<TopicAd>),   // 02 の LSA に反映
}
```

**ストレージを非同期にする理由**：`redb` の書き込みは blocking。状態機械の中で呼ぶとシミュレーションが決定論的でなくなり、本番でも転送スレッドを止める。`StorageOp` を発行して結果をイベントで受ける形にすると、シミュレーションでは同期的な in-memory 実装で即座にイベントを返せる。

主要な内部状態：

```rust
pub struct PubSub {
    policies: Arc<TopicPolicies>,
    local_subs: HashMap<SubId, SubState>,      // filter, cursor, mode, backfill 進行状況
    my_seq: HashMap<TopicKey, u64>,            // 発行 seq
    seen: HashMap<(NodeId, TopicKey), u64>,    // dedup 用 last_seq（+ 小さな out-of-order 窓）
    backfill: BackfillManager,                 // inflight 要求、window、retry
    crypto: TopicCrypto,                       // 06
}
```

### 8.1 重複排除の out-of-order 窓

ECMP や再送で seq が前後することがある。`last_seq` だけだと順序が乱れた古いメッセージを落としてしまう。
`last_seq` + **64 bit のビットマップ**（直近 64 seq の受信有無）を持つ。TCP SACK と同じ発想。

## 9. アプリケーション API（ローカル gRPC）

```protobuf
service PubSubApi {
  rpc Publish(stream PublishRequest) returns (stream PublishAck);
  rpc Subscribe(SubscribeRequest) returns (stream Delivery);
  rpc GetCursor(GetCursorRequest) returns (Cursor);   // App が永続化するため
}

message Delivery {
  bytes  origin = 1;  uint64 seq = 2;  uint64 ts_ms = 3;
  bytes  payload = 4;         // 復号済み・展開済み
  bool   is_backfill = 5;
  bool   verified = 6;        // signed=true の Topic で署名検証が通ったか
  repeated Gap gaps = 7;      // 直前に確定した欠落範囲
}
```

- **Publish は stream + ack**。ack は「ローカル mbd が受理した（ストア済み／送出キュー投入済み）」を意味する。エンドツーエンドの到達確認ではない。到達確認が要るアプリは RPC（05）を使う
- 接続は unix domain socket。App は他ノードの存在もアドレスも知らない

## 10. 検証すべき性質（08 で自動化）

| 性質 | 検証 |
|---|---|
| 分断中の全メッセージが復旧後に届く | 2 分割 → 片側で 1000 publish → 結合 → 購読者が 1000 件受信（順不同可） |
| Backfill が live を阻害しない | Backfill 10 MB 進行中に live を送り、live の p99 遅延が Backfill なし時の 1.5 倍以内 |
| cursor 再開の正確性 | 購読者を任意時点で kill → cursor から再購読 → 欠落 0、重複は許容（at-least-once） |
| Gap の正しさ | 発行元で prune → Backfill 要求 → Gap が返り、購読者が無限待ちしない |
| latest モードの conflation | 帯域を絞って 1000 件送信 → 受信は少数だが最終値は一致 |
| dedup | 同一メッセージを 3 経路から届けて App への配信が 1 回 |
