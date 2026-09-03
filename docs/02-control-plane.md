# 02. Control Plane — 署名付き LSA・Gossip・LSDB・SPF

## 1. 責務

全ノードが「メッシュ全体のトポロジ」「各ノードが提供するサービス／Topic」「各ノードのアドレス」を
**結果整合**で共有し、そこから **next-hop 経路表**を計算する。

設計の骨格は **OSPF の link-state ルーティングをアプリケーション層に移植したもの**。ただし以下が異なる：

| OSPF | mesh-bus |
|---|---|
| LSA は認証なし（または共有鍵 MD5） | LSA は origin の秘密鍵で署名。全受信者が検証 |
| 隣接関係は Hello の相互受信で確立 | **双方向アサーション**：A の LSA が B を、B の LSA が A を含むときだけ辺を採用 |
| フラッディングのみ | フラッディング + 周期的 digest 交換（anti-entropy）で分断復旧に強くする |
| ルータ ID、ネットワーク LSA など複数タイプ | LSA は 1 種類。ノード自身の状態を全部載せる |
| エリア分割で階層化 | 単一エリア（300 ノード上限）。階層化は将来課題 |

## 2. LSA (Link State Advertisement)

```protobuf
// proto/control.proto
syntax = "proto3";
package meshbus.control.v1;

message Lsa {
  bytes  origin       = 1;   // NodeId (32 bytes)
  uint64 seq          = 2;   // origin ごとに単調増加。再起動後も継続（永続化）
  uint64 issued_at_ms = 3;   // 参考。再生攻撃の許容窓判定にのみ使用
  uint32 ttl_sec      = 4;   // MaxAge。受信側は受信時刻 + ttl で失効

  repeated Adjacency  adjacencies = 10;  // 私が直接繋がっている相手
  repeated Address    addresses   = 11;  // 私に接続するためのアドレス
  repeated Service    services    = 12;  // 私が提供する gRPC サービス
  repeated TopicAd    topics      = 13;  // 私が publish / subscribe する Topic
  bytes               policy_hash = 14;  // 私が適用中の認可ポリシーのハッシュ（06）

  uint32 epoch        = 20;  // プロセス起動世代。同 seq でも epoch が新しければ採用
}

message Adjacency {
  bytes  peer     = 1;   // NodeId
  uint32 cost     = 2;   // 01 §7 で算出、1..65535
  uint32 kind     = 3;   // LinkKind
  uint32 mtu      = 4;
}

message Address {
  string addr        = 1;   // "10.0.0.5:7400"
  uint32 transports  = 2;   // bitmask: QUIC=1, TCP=2
  bool   direct_allowed = 3; // 機会的直接接続の許可
}

message Service {
  string name       = 1;   // "lattice.entities.v1.EntityManager"
  uint32 weight     = 2;   // 同名サービスが複数ノードにあるときの選好
}

message TopicAd {
  string topic      = 1;
  bytes  partition  = 2;   // 空 = 全パーティション
  uint32 role       = 3;   // bitmask: PUBLISH=1, SUBSCRIBE=2, STORE=4
  uint64 head_seq   = 4;   // PUBLISH 時: 最新 seq。STORE 時: 保持している最大 seq
  uint64 tail_seq   = 5;   // STORE 時: 保持している最小 seq（Backfill 提供範囲）
}

// 署名は canonical bytes に対して行い、バイト列をそのまま運ぶ
message SignedLsa {
  bytes lsa_bytes = 1;   // Lsa の deterministic serialization
  bytes signature = 2;   // ECDSA P-256 or Ed25519 over lsa_bytes
}
```

### 2.1 サイズの見積もり

隣接 8、アドレス 2、サービス 10、Topic 30 で **約 2〜3 KB**。300 ノードで LSDB は 1 MB 弱。
無線で 300 ノード全 LSA を 60 秒ごとに再発行すると 1 ノードあたり受信 ~15 KB/s。これは細い無線では重い。対策：

- **差分 LSA は採用しない**（複雑さと不整合リスクが高い）。代わりに LSA 再発行間隔を隣接リンクの最小帯域で自動調整（60 s → 最大 300 s）
- Topic 広告は「ノードごと」ではなく「Topic ごとに集約する」方式に将来移行できるよう、`topics` は別 LSA タイプに分離可能な設計にしておく

### 2.2 seq の永続化

`seq` は再起動をまたいで単調増加させる。ローカルストレージに保存し、起動時に `saved + 1000` から始める（クラッシュで書き込み前の seq を失っても追い越せるように）。
ストレージが失われた場合は `epoch` を上げる。受信側は「同 origin で epoch が大きければ seq を無視して採用」する。

## 3. 双方向アサーションによる辺の採用

```text
LSDB:
  A: adjacencies = [B(cost 10), C(cost 50)]
  B: adjacencies = [A(cost 12)]
  C: adjacencies = [D(cost 5)]           ← C は A を主張していない

グラフ構築:
  edge(A,B): A→B あり, B→A あり  → 採用。cost(A→B)=10, cost(B→A)=12（非対称のまま）
  edge(A,C): A→C あり, C→A なし  → 不採用
  edge(C,D): C→D あり, D→C なし  → 不採用（D の LSA 未着 or D が主張していない）
```

**なぜ必要か**：単方向で採用すると、侵害ノード M が「M は全ノードに cost 1 で繋がっている」と署名して広告するだけで全トラフィックを M に集約できる。双方向を要求すれば、M は **相手ノードの署名を偽造できない**ため、実際に接続を受け入れたノードとの辺しか作れない。

副作用：本当に非対称なリンク（受信専用の放送リンク）は表現できない。これは受け入れる（非目的）。

## 4. Gossip：フラッディング + Anti-Entropy

### 4.1 フラッディング（変化の即時伝播）

1. 自ノードの状態変化（Link Up/Down、cost のヒステリシス超過、サービス登録）→ 新 LSA 生成、seq+1、署名
2. 全隣接 Link の `Control` チャネルへ送信
3. 受信側：署名検証 → LSDB と比較 → **新しければ**インストールし、**受信した Link 以外**の全隣接へ転送。古ければ破棄（自分の持つ新しい方を送り返す：OSPF と同じ）
4. 重複排除は `(origin, epoch, seq)` で行う。LSDB 自体が dedup 表

### 4.2 Anti-Entropy（分断復旧・取りこぼし修復）

フラッディングだけでは、送信中にリンクが落ちた LSA は失われ、次の再発行（最大 60 s）まで届かない。分断が解消した瞬間に両側の LSDB が食い違うのも同じ。そこで隣接ごとに周期的（10 s）に **digest** を交換する。

```protobuf
message Digest {
  repeated DigestEntry entries = 1;   // 自分の LSDB の (origin, epoch, seq) 全件
}
message DigestEntry { bytes origin = 1; uint32 epoch = 2; uint64 seq = 3; }

message DigestReq {
  repeated bytes origins = 1;   // 相手の方が新しかった origin のリスト。フル LSA を要求
}
```

300 ノードで digest は 300 × 44 bytes ≈ 13 KB。10 秒に 1 回、隣接ごと。無線では重いので、**リンク種別が Radio/Satcom なら 30 s** に伸ばす。

digest を受信したら：

- 相手の方が新しい origin → `DigestReq` で要求
- 自分の方が新しい origin → その `SignedLsa` を送る（相手からの要求を待たない）

これで **分断復旧後、最悪 1 digest 周期 + 1 RTT で LSDB が一致**する。

### 4.3 Link Up 時の即時同期

新しい Link が Up したら digest 周期を待たず即座に digest を送る。OSPF の Database Description 交換に相当。

## 5. LSDB

```rust
pub struct Lsdb {
    entries: HashMap<NodeId, LsdbEntry>,
    // SPF 用に adjacency を高速に引くための逆引きは SPF 時に構築（毎回 O(E)）
}

pub struct LsdbEntry {
    lsa: Lsa,                  // デコード済み
    signed_bytes: Bytes,       // 転送用にそのまま保持（再署名しない）
    received_at: Instant,
    expires_at: Instant,       // received_at + ttl
    verified: bool,            // 署名検証済み（未検証は SPF に使わない）
}
```

- **失効**：`expires_at` を過ぎたエントリは SPF から除外し、さらに `ttl` 後に削除する（除外と削除を分けるのは、失効直後に再受信した古い LSA を「新しい」と誤認しないため）
- **自 LSA の再発行**：`ttl / 5` ごと。隣接リンクが Radio/Satcom のみなら `ttl / 2`
- **メモリ上限**：エントリ数 1000、超過は最古から削除（DoS 対策。正規ノードは 300 想定）

## 6. SPF と経路表

### 6.1 グラフ構築

LSDB から有向グラフを作る。辺 (u→v) は §3 の双方向条件を満たすもののみ。cost は `u` の LSA に書かれた値。

### 6.2 Dijkstra

自ノードを根に Dijkstra。出力は各宛先の `(total_cost, next_hop_link, path_len)`。

**ECMP / マルチパス**：cost が最小値の +10% 以内の next-hop を最大 2 つ保持する。
データプレーン（03）は **flow_id のハッシュで next-hop を選ぶ**（同一フローは同一経路、順序を崩さない）。
Backfill（P3）のみ、2 経路を並列に使って帯域を合算することを許す。

### 6.3 Hold timer（収束制御）

LSDB が変わるたびに SPF を回すとフラップ時に CPU を食う。

```text
初回変更 → 100 ms 後に SPF
SPF 実行直後に再変更 → 次は 200 ms 後、400 ms、… 最大 5 s
変更なし 10 s 続く → 100 ms にリセット
```

300 ノードで Dijkstra は < 1 ms なので、hold timer の主目的は**経路表の頻繁な差替えでフローの next-hop が揺れるのを防ぐ**こと。

### 6.4 経路表の公開

```rust
pub struct RouteTable {
    version: u64,
    routes: HashMap<NodeId, Route>,
}
pub struct Route {
    pub next_hops: SmallVec<[LinkId; 2]>,   // ECMP
    pub cost: u32,
    pub hops: u8,
}
```

データプレーンには `Arc<RouteTable>` を **atomic swap** で渡す（`arc-swap`）。転送ホットパスはロックを取らない。

## 7. 隣接の状態遷移とフラップ抑制

```text
                 Link Up
   ┌──────────┐ ────────▶ ┌──────────┐  hold-down 5s ┌──────────┐
   │   Down   │           │  Probing │ ─────────────▶│  Adjacent│
   └──────────┘ ◀──────── └──────────┘               └────┬─────┘
        ▲        Link Down                                │ Link Down
        └─────────────────────────────────────────────────┘
                     即時 LSA から除外・フラッド
```

- **Down は即時反映**（届かない経路を使い続けるのは最悪）
- **Up は 5 秒待ってから広告**（すぐ落ちるリンクを広告しない）。ただし他に経路がない孤立状態なら hold-down をスキップ
- 5 分に 3 回以上 Down したリンクは hold-down を 30 s に延長（damping）

## 8. サービス／Topic 発見

LSA の `services` / `topics` が発見情報そのもの。別の発見プロトコルは持たない。

- RPC プロキシ（05）：`ServiceName` → LSDB を走査 → 提供ノード集合 → SPF cost 最小（`weight` で補正）を選ぶ。走査結果は `RouteTable.version` をキーにキャッシュ
- Pub/Sub（04）：Topic ごとに `(publishers, subscribers, stores)` の集合を LSDB から導出。これが Interest 伝播の基盤

LSDB から導出した索引：

```rust
pub struct DiscoveryIndex {
    services: HashMap<ServiceName, Vec<(NodeId, u32 /*weight*/)>>,
    topic_subs: HashMap<TopicName, Vec<(NodeId, Partition)>>,
    topic_pubs: HashMap<TopicName, Vec<(NodeId, Partition, u64 /*head_seq*/)>>,
    topic_stores: HashMap<TopicName, Vec<(NodeId, Partition, u64, u64)>>,
}
```

LSDB 変更のたびに再構築（SPF と同じ hold timer 内）。

## 9. コンポーネントとしての形（I/O なし）

```rust
pub enum ControlEvent {
    LinkUp   { link: LinkId, peer: NodeId, metrics: LinkMetrics },
    LinkDown { link: LinkId, peer: NodeId },
    LinkMetrics { link: LinkId, metrics: LinkMetrics },
    Frame    { link: LinkId, frame: ControlFrame },   // SignedLsa / Digest / DigestReq
    LocalChange(LocalChange),                          // サービス登録、Topic 購読 等
    Timer(ControlTimer),                               // LsaRefresh / Digest(link) / SpfHold / LsaExpire
}

pub enum ControlAction {
    Send { link: LinkId, frame: ControlFrame },
    SetTimer { timer: ControlTimer, at: Instant },
    PublishRoutes(Arc<RouteTable>),
    PublishDiscovery(Arc<DiscoveryIndex>),
    ConnectTo { addr: SocketAddr, hint: NodeId },      // 機会的接続の依頼（01 §4.3）
    PersistSeq(u64),
}

pub struct ControlPlane {
    me: NodeId,
    signer: Box<dyn Signer>,       // 秘密鍵は crypto 層に隠す
    verifier: Box<dyn Verifier>,
    lsdb: Lsdb,
    adjacencies: HashMap<LinkId, AdjState>,
    local: LocalState,             // 自分のサービス・Topic・アドレス
    spf_hold: HoldTimer,
    my_seq: u64,
    // ...
}

impl Component for ControlPlane {
    type Event = ControlEvent;
    type Action = ControlAction;
    fn handle(&mut self, now: Instant, ev: ControlEvent) -> Vec<ControlAction> { /* ... */ }
}
```

`Signer` / `Verifier` は trait。シミュレーションでは検証コストを省くため「常に OK」の実装を差せる（署名ロジック自体は別の単体テストで検証）。

## 10. 検証すべき性質（08 で自動化）

| 性質 | 検証方法 |
|---|---|
| 収束：トポロジ変更後、全ノードの経路表が T 秒以内に一致する | 100 ノードのランダムグラフでリンク断・復旧を繰り返し、全ノードの `RouteTable` を比較 |
| 安全性：宛先に到達不能な経路を使わない | 各ノードの next-hop を辿って宛先に着くことを全ペアで確認 |
| 双方向条件：片側だけの広告が経路に現れない | 悪意ノードが偽 adjacency を広告するシナリオで、他ノードの経路表に該当辺がないことを確認 |
| 分断復旧：分断中に起きた変更が復旧後 1 digest 周期以内に全域へ届く | 2 分割 → 各側で変更 → 結合 → LSDB 一致時間を測定 |
| 署名不正の破棄 | 改竄 LSA を注入し、LSDB に入らないこと |
| フラップ抑制 | 1 リンクを 1 秒周期で on/off し、LSA 発行数が上限（damping）以下 |
