# 00. 全体設計 — 分散環境向け P2P Service Bus ("mesh-bus")

> Anduril Lattice の DSB (Distributed Service Bus) / Lattice Mesh を公開情報から推定した構造を参考に、
> 「自分で同等のものを実装するなら」という前提で書いた設計ドキュメント。
> 以降、実装対象を **mesh-bus**、デーモンを **`mbd`**、CLI を **`mbtool`** と呼ぶ。

## 0. ドキュメント構成

| # | ファイル | 内容 |
|---|---|---|
| 00 | `00-overview.md` | 目的・前提・脅威モデル・レイヤー構成・crate 構成・横断的設計判断 |
| 01 | `01-transport.md` | Link 抽象、QUIC/TCP、近隣探索、フレーム形式、多重化 |
| 02 | `02-control-plane.md` | Node 識別、署名付き LSA、Gossip、LSDB、SPF、収束制御 |
| 03 | `03-data-plane.md` | オーバーレイ転送、QoS、フロー制御、ドロップポリシー、圧縮 |
| 04 | `04-pubsub.md` | Topic モデル、購読セマンティクス、Interest 伝播、Store-and-Forward、Backfill |
| 05 | `05-rpc-proxy.md` | gRPC 透過プロキシ、サービス発見、仮想回線 |
| 06 | `06-security.md` | 識別・認証・E2E 暗号・鍵配布・認可 |
| 07 | `07-observability.md` | メトリクス、ログ、`mbtool`、キャプチャ |
| 08 | `08-simulation-testing.md` | 決定論的シミュレーション、Fault Injection、テスト階層 |
| 09 | `09-roadmap.md` | 段階的実装計画（MVP → 本番相当） |

## 1. 目的と非目的

### 目的

DDIL (Denied, Disrupted, Intermittent, Limited bandwidth) 環境下で動く多数のノード（ドローン、車両、センサー、C2 端末）が、
中央ブローカーなしに以下を実現する。

1. **相互到達性の自律的な確立**：どのノードも隣接ノードだけを知っていれば、多段ホップで任意ノードに届く。
2. **Pub/Sub**：Topic ベースの配信。切断中のデータは再接続後に Backfill される。
3. **透過 RPC**：アプリケーションは `service-name` だけ指定すれば、どのノードで動いていても gRPC が届く。
4. **QoS**：帯域が細いとき、C2 コマンド > ライブトラック > テレメトリ > 履歴 Backfill の優先順が守られる。
5. **Zero Trust**：中継ノードが侵害されても、内容の読取・改竄・偽造ができない。経路情報の嘘も検出できる。
6. **決定論的に再現可能なテスト**：本番コードそのものを仮想時間・仮想ネットワーク上で走らせられる。

### 非目的（今回はやらない）

- IP レイヤーのルーティング（OSPF/BGP の置換）。mesh-bus は **アプリケーションレイヤーのオーバーレイ**である。下の IP 到達性は radio / SATCOM / Ethernet が提供する前提。
- 強い一貫性（Raft / Paxos 等の合意）。制御プレーンは **結果整合**で十分。合意が必要なアプリは別途持つ。
- 汎用メッセージキュー（Kafka 互換 API 等）。
- 数万ノード規模。目標は **数百ノード / 1 メッシュ**。それ以上は階層化（エリア分割）で対応する将来課題。

## 2. 前提条件と想定環境

| 項目 | 想定 |
|---|---|
| ノード数 | 3〜300 / メッシュ |
| リンク種別 | Ethernet (1Gbps, <1ms), 戦術無線 (数十kbps〜数Mbps, 50〜500ms, loss 0〜30%), SATCOM (数百kbps〜数Mbps, 500〜800ms) |
| MTU | 1200〜1500 bytes。無線では 500 bytes 程度まで落ちる可能性を考慮 |
| 断続性 | 数秒〜数時間の切断。分断 (partition) と再結合が日常的に起きる |
| 時刻 | GPS/PTP があれば同期。なければ数秒ずれる。**論理順序は時刻に依存させない** |
| ノードの計算資源 | ARM Cortex-A 級 SBC 〜 x86 サーバー。数百 MB RAM で動くこと |
| OS | Linux 主体。シミュレーションはホスト OS 不問 |
| 信頼 | ノードは物理的に奪取・侵害され得る。ネットワークは常に敵対的 |

## 3. 脅威モデル（要約。詳細は 06）

| 脅威 | 対策 |
|---|---|
| 盗聴 | ホップ間 mTLS (TLS 1.3) + Topic 単位の E2E 暗号 (AES-256-GCM) |
| 中継ノードによる改竄・偽造 | E2E AEAD タグ + 発行者署名（重要 Topic） |
| 偽の経路広告（トラフィック吸い込み） | LSA を発行者鍵で署名。**双方向アサーション**が揃った辺のみ経路計算に使用 |
| 過去メッセージの再生 | (origin, seq) による重複排除 + タイムスタンプ許容窓 |
| なりすまし参加 | ミッション CA 発行の証明書を必須。NodeId = 公開鍵ハッシュ |
| 奪取されたノード | 短命証明書 + Gossip で配布する失効リスト + Topic 群鍵のローテーション |
| DoS（フラッド） | ピア単位のレート制限、優先度キュー、未認証トラフィックの早期破棄 |

## 4. レイヤー構成

```text
┌──────────────────────────────────────────────────────────────┐
│  Application  (Entity Manager / Tasking / Sensor / C2 ...)   │
└──────────────┬────────────────────────────┬──────────────────┘
               │ gRPC (unix socket / lo)    │ Pub/Sub API (gRPC streaming)
┌──────────────▼────────────────────────────▼──────────────────┐
│  L5  Service Layer                                           │
│      ┌──────────────────┐   ┌──────────────────────────┐     │
│      │ RPC Proxy (05)   │   │ Pub/Sub Engine (04)      │     │
│      │ service resolve  │   │ topics / subscriptions   │     │
│      │ virtual circuit  │   │ store / backfill         │     │
│      └────────┬─────────┘   └────────────┬─────────────┘     │
├───────────────┴──────────────────────────┴───────────────────┤
│  L4  Overlay Forwarding / Data Plane (03)                    │
│      next-hop lookup · priority queues · flow control        │
│      explicit-multicast fan-out · compression                │
├───────────────────────────────┬──────────────────────────────┤
│  L3  Control Plane (02)       │  Security (06) は全層を横断  │
│      signed LSA · gossip      │  identity / mTLS / E2E keys  │
│      LSDB · SPF · route table │  authz policy                │
├───────────────────────────────┴──────────────────────────────┤
│  L2  Transport (01)                                          │
│      Link trait · QUIC (primary) / TCP+TLS (fallback)        │
│      neighbor discovery (UDP multicast + static + gossiped)  │
├──────────────────────────────────────────────────────────────┤
│  L1  OS network  (UDP / TCP sockets over radio, SATCOM, eth) │
└──────────────────────────────────────────────────────────────┘
```

各層の責務境界：

- **L2 Transport** は「隣接ノードとの 1 ホップ、認証済み、多重化されたバイトストリーム／データグラム」だけを提供する。経路は知らない。
- **L3 Control Plane** は「誰がどこにいて、誰と繋がっていて、何のサービス／Topic を持っているか」を結果整合で全ノードに配る。転送はしない。
- **L4 Data Plane** は L3 が計算した next-hop 表を引いて、優先度付きで L2 に流す。内容は知らない（暗号文のまま中継）。
- **L5 Service Layer** はアプリケーション向け API を提供し、L4 の上に Pub/Sub と RPC のセマンティクスを載せる。

## 5. 横断的な設計判断

### 5.1 言語・ランタイム

- **Rust (stable)**。理由：メモリ安全 + ゼロコスト抽象 + async。C++ でも作れるが、並行性バグの検出コストが違いすぎる。
- 非同期ランタイムは **tokio**。ただし後述の通り、コアロジックは tokio に依存させない。
- 主要 crate 候補：`quinn` (QUIC), `rustls` (TLS 1.3), `tonic`/`h2` (gRPC/HTTP2), `prost` (protobuf), `ring` or `aws-lc-rs` (FIPS 経路), `zstd`, `redb` (組込 KV), `tracing`, `prometheus`/`metrics`, `proptest`.

### 5.2 「I/O を持たないコア」— 決定論的シミュレーションのための最重要判断

制御プレーン、転送、Pub/Sub の**すべての判断ロジックは純粋な状態機械**として書く。

```rust
/// 全コンポーネント共通の形。I/O を一切行わない。
pub trait Component {
    type Event;    // 受信フレーム、タイマー発火、アプリからの要求 など
    type Action;   // フレーム送信、タイマー設定、アプリへの通知 など

    fn handle(&mut self, now: Instant, ev: Self::Event) -> Vec<Self::Action>;
}
```

- **本番**：tokio のタスクが socket から読んで `Event` を作り、`handle` を呼び、返った `Action` を socket に書く。
- **シミュレーション**：離散イベントスケジューラが仮想時刻で `Event` を投入し、`Action` を仮想ネットワークに流す。乱数はシード付き。

これにより **同じバイナリのコアを 1 プロセス内で 100 ノード分動かし、リンク断・遅延・分断を秒単位のスクリプトで再現**できる（08 参照）。

環境依存は以下の trait に閉じ込める：

```rust
pub trait Clock   { fn now(&self) -> Instant; }
pub trait Rng     { fn next_u64(&mut self) -> u64; }
pub trait Storage { /* append / scan / truncate、詳細は 04 */ }
pub trait Link    { /* send / recv / metrics、詳細は 01 */ }
```

### 5.3 識別子

| 識別子 | 定義 | 用途 |
|---|---|---|
| `NodeId` | 32 bytes = SHA-256(公開鍵 DER) | 経路の宛先、LSA の origin、証明書との突合 |
| `ServiceName` | UTF-8 文字列（例 `lattice.entities.v1.EntityManager`） | RPC 解決 |
| `TopicKey` | `(topic_name: String, partition: Bytes)` | Pub/Sub。partition は asset_id 等 |
| `Seq` | u64、発行者ごとに単調増加 | 順序・重複排除・Backfill 範囲指定 |
| `LsaSeq` | u64、origin ごとに単調増加（再起動後は永続化した値+1） | LSA の新旧判定 |

**NodeId を公開鍵から導出する**ことで、証明書がなくても「この NodeId の署名検証には必ずこの鍵」が成立し、経路広告の署名検証と mTLS のピア確認が同じ根に落ちる。

### 5.4 シリアライゼーション

- 制御プレーン・Pub/Sub エンベロープ・RPC メタデータ：**Protocol Buffers (proto3)**。前方互換のため未知フィールドは保持して転送する。
- 転送ヘッダ（L4）：**固定長バイナリ**。protobuf は可変長でパース負荷があるため、中継ノードがホットパスで触るヘッダは手書きにする。
- 署名対象は **canonical bytes**（protobuf の deterministic serialization を有効化し、署名前のバイト列をそのまま運ぶ）。再エンコードして検証しない。

### 5.5 時刻の扱い

- ノード間の時刻同期を**前提にしない**。順序はすべて `(origin, seq)` で決める。
- タイムスタンプは「参考情報」と「Backfill の範囲指定の補助」にのみ使う。
- LSA の鮮度判定は seq が主、timestamp は再生攻撃の許容窓（±5 分等）のみ。

### 5.6 障害時の振る舞いの原則

1. **Fail-static**：制御プレーンの情報が古くなっても、直前の経路表で転送し続ける。情報がないより古い方がいい。
2. **Live 優先**：帯域が足りないとき、履歴データより今のデータを通す。ライブ系の古いメッセージは捨てる（最新値だけが意味を持つ Topic）。
3. **Local first**：アプリケーションは常にローカルの `mbd` にだけ接続する。`mbd` が落ちたらアプリは切断を検知するだけで、他ノードのアドレスを知る必要はない。
4. **Explicit over implicit**：Topic のリテンション、優先度、E2E 暗号要否などは Topic 定義（ポリシー）で明示。推測しない。

## 6. crate / プロセス構成

```text
mesh-bus/
├── Cargo.toml                 (workspace)
├── proto/                     .proto 定義（control, pubsub, rpc, admin）
├── crates/
│   ├── mb-types/              NodeId, TopicKey, 共通エラー, 定数
│   ├── mb-crypto/             鍵, 署名, AEAD, HPKE, 証明書検証 (06)
│   ├── mb-wire/               L4 固定長ヘッダ, フレームコーデック (01/03)
│   ├── mb-transport/          Link trait, QUIC/TCP 実装, 近隣探索 (01)
│   ├── mb-control/            LSA, Gossip, LSDB, SPF, RouteTable (02)  ★I/O なし
│   ├── mb-forward/            転送エンジン, QoS スケジューラ (03)        ★I/O なし
│   ├── mb-pubsub/             Topic, 購読, Interest, Backfill (04)      ★I/O なし
│   ├── mb-store/              Storage trait, redb 実装, in-memory 実装 (04)
│   ├── mb-rpc-proxy/          gRPC プロキシ, 仮想回線 (05)
│   ├── mb-runtime/            tokio で ★ を駆動する glue, 設定ロード
│   ├── mb-sim/                決定論的シミュレータ, シナリオ DSL (08)
│   ├── mb-admin/              Admin gRPC API 定義とサーバ (07)
│   └── mb-metrics/            Prometheus exporter, tracing 設定 (07)
├── bins/
│   ├── mbd/                   デーモン本体
│   ├── mbtool/                CLI（Admin API クライアント）
│   └── mbsim/                 シナリオを実行するシミュレータ CLI
└── tests/
    ├── sim/                   シナリオファイル (.toml / .ron)
    └── netns/                 Linux netns + tc netem を使う実網テスト
```

プロセス構成（1 ノード）：

```text
┌──────────────── host ────────────────────────────────────┐
│  mbd (1 process)                                          │
│   ├─ admin API      unix:/run/mesh-bus/admin.sock         │
│   ├─ app API        unix:/run/mesh-bus/app.sock  (gRPC)   │
│   ├─ metrics        tcp:127.0.0.1:9600 (/metrics)         │
│   ├─ mesh listener  udp:*:7400 (QUIC) / tcp:*:7400 (fallback)
│   └─ discovery      udp multicast 239.77.66.1:7401        │
│                                                            │
│  app processes (Entity Manager 等) → app.sock に gRPC     │
└────────────────────────────────────────────────────────────┘
```

## 7. 主要パラメータ（初期値。すべて設定で上書き可）

| パラメータ | 初期値 | 根拠 |
|---|---|---|
| Hello 間隔 (discovery) | 2 s | 無線でも負荷が軽い。参加検知は数秒で十分 |
| Link keepalive | 1 s (QUIC PING) | 断検知 3 s のため |
| Link dead 判定 | 3 連続無応答 = 3 s | OSPF の DeadInterval/HelloInterval = 4 より積極的。無線の一時的ロスは cost 上昇で吸収 |
| LSA 再発行間隔 | 60 s | MaxAge の 1/5 |
| LSA MaxAge | 300 s | 分断中に古い経路が残り続けるのを防ぐ |
| Anti-entropy digest 交換 | 10 s / 隣接ごと | 分断復旧後の再同期を 10 s 以内に |
| SPF hold timer | 100 ms 初期、最大 5 s（指数バックオフ） | フラップ時の CPU 保護 |
| 転送 TTL | 32 hop | 300 ノードで直径 32 を超えることは想定しない |
| QoS クラス | P0 制御, P1 ライブ, P2 テレメトリ, P3 Backfill | 03 参照 |
| Topic 既定リテンション | 24 h or 256 MB / topic | ノードのストレージ想定から |
| 証明書有効期間 | 7 日 | DDIL で 24 h 更新は非現実的。失効は Gossip CRL で補う |
| Topic 群鍵ローテーション | 24 h または メンバー変更時 | 06 参照 |
