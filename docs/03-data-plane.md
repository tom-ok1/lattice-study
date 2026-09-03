# 03. Data Plane — オーバーレイ転送・QoS・フロー制御・圧縮

## 1. 責務

上位層（Pub/Sub、RPC）から渡された **オーバーレイパケット**を、02 が計算した経路表に従って next-hop Link へ送る。
また他ノードから届いたパケットを、宛先が自分なら上位へ、そうでなければ次のホップへ転送する。

原則：

- **内容を見ない**。ペイロードは E2E 暗号文であることが多く、中継ノードは読めない前提で設計する
- **優先度を守る**。帯域が細いリンクで P0 (制御・C2) が P3 (Backfill) に埋もれてはならない
- **バッファは有限**。溢れたらクラスごとのポリシーで捨てる。無限キューは遅延を無限にする

## 2. オーバーレイパケットヘッダ（L4）

固定 88 バイト。中継ノードのホットパスなので手書きバイナリ。

```text
offset  size  field
0       1     version (=1)
1       1     ptype          Unicast=1, Multicast=2, CircuitData=3, CircuitCtl=4
2       1     priority       0..3 (P0 最高)
3       1     ttl            初期 32、ホップごとに -1、0 で破棄
4       4     flags          bit0 reliable-hint, bit1 conflatable, bit2 compressed(payload)
8       32    dst            NodeId (Multicast のときは §5 のリスト先頭。後続は payload 内)
40      32    src            NodeId
72      8     flow_id        ECMP のハッシュキー。Pub/Sub: hash(topic, partition)。RPC: circuit_id
80      8     conflate_key   置換対象を識別するキー。conflatable=0 のとき 0
88      ...   payload
```

- `conflatable` フラグ：同じ `(src, flow_id, conflate_key)` の新しいパケットが来たら、キュー内の古い方を
  差し替えてよい（04 の `mode=latest` Topic 用）
- `conflate_key` は**送信元が計算してヘッダに載せる**。Pub/Sub では `fxhash(origin ‖ partition)`。
  ペイロードから導出しないのは、E2E 暗号化された本文を中継ノードが読めないため
- `reliable-hint`：datagram で送ってはいけない（QUIC DATAGRAM 不可）

## 3. 転送処理

```text
recv(Link L, pkt)
  ├─ ttl == 0            → drop (metric: ttl_exceeded)
  ├─ dst == me           → deliver_local(pkt)
  ├─ ptype == Multicast  → §5 fan-out
  └─ else
       route = routes.get(dst)
         ├─ None         → drop (metric: no_route)。P0 のみ 2 秒までキューに留めて経路出現を待つ
         └─ Some(r)
              nh = pick_next_hop(r, pkt.flow_id)    // ECMP
              nh == L かつ r.next_hops.len()==1 → drop (loop 検知)
              ttl -= 1
              enqueue(nh, pkt)
```

`deliver_local` はパケット種別で上位へ振り分ける：`Unicast/Multicast` → Pub/Sub、`Circuit*` → RPC プロキシ。

## 4. QoS スケジューラ（Link ごと）

### 4.1 クラス

| クラス | 用途 | キュー長 | 溢れ時ポリシー | 目標 |
|---|---|---|---|---|
| P0 | 制御プレーン (LSA/digest)、C2 コマンド、RPC 制御 | 256 pkt | **Tail-drop しない**：上位にバックプレッシャ (send 失敗を返す) | 最優先、遅延最小 |
| P1 | ライブトラック、位置、状態 | 512 pkt | **Conflate → 最古を drop**（最新値が意味を持つ） | 鮮度 |
| P2 | テレメトリ、ログ、RPC データ | 1024 pkt | Tail-drop | ベストエフォート |
| P3 | Backfill、バルク | 4096 pkt | 送信元へ credit 停止（送らせない） | 余った帯域のみ |

### 4.2 スケジューリングアルゴリズム

**厳密優先（P0）+ 重み付き DRR（P1〜P3）**。

```text
loop:
  credit = link.send_credit()
  if credit == 0: wait
  if !q[P0].empty(): send(q[P0].pop()); continue
  // DRR: P1:P2:P3 = 70:25:5 (quantum bytes)
  for cls in round_robin([P1,P2,P3]):
      deficit[cls] += quantum[cls]
      while deficit[cls] >= head_size(q[cls]) && !q[cls].empty():
          send(q[cls].pop()); deficit[cls] -= size
```

- P0 を厳密優先にする理由：C2 コマンドと経路更新は遅延が致命的。P0 の総量はレート制限（§6）で抑えるので飢餓は起こさない
- P3 の重み 5% は「他が空いていれば全帯域を使える」DRR の性質で、実質「余り帯域」を使うことになる
- 重みはリンク種別で変える設定を持つ（SATCOM では P3 を 1% に）

### 4.3 Conflation（P1）

P1 キューは `HashMap<(src, flow_id, key), QueueIndex>` を併設。`conflatable` パケットの enqueue 時に同キーの既存エントリがあれば **中身を差し替え、キュー位置は維持**する。
無線が詰まったとき「10 秒前の位置情報を 10 個送る」のではなく「今の位置を 1 個送る」ようになる。

## 5. Explicit Multicast（Pub/Sub の fan-out）

Pub/Sub の配信は「1 メッセージを N 購読ノードへ」だが、経路上の各リンクには 1 回だけ流したい。
中継ノードに Topic 状態を持たせない **Explicit Multicast (Xcast 方式)** を採用する。

```text
送信元 S: 購読者 = {A, B, C, D}
  routes: A,B → next-hop L1 / C,D → next-hop L2
  → L1 へ pkt{dst_list=[A,B]} を 1 個
  → L2 へ pkt{dst_list=[C,D]} を 1 個

中継 X（L1 の先）: dst_list=[A,B]
  routes: A → L3 / B → 自分（B == X なら deliver_local）
  → L3 へ pkt{dst_list=[A]}
```

- `Multicast` パケットのペイロード先頭に `dst_count: u16` + `NodeId × n` を置く。**ペイロード本体は共有**（`Bytes` のスライスで zero-copy）
- 購読者が 64 を超える Topic は宛先リストが 2 KB を超えるので、**送信元で 64 ずつ分割**する
- 中継ノードは Topic を知らないので **Topic の購読変更が中継に伝播する遅延がない**。経路表さえ新しければ正しく配れる
- 欠点：送信元が全購読者を知る必要がある → 02 の `DiscoveryIndex.topic_subs` から取る。購読者情報は LSA で来るので最大 LSA 伝播遅延（数秒）だけ配信開始が遅れる。これは許容

## 6. フロー制御とバックプレッシャ

### 6.1 ホップ間

Link の `send_credit()`（01）が 0 なら QoS キューに留まる。QUIC のストリームフロー制御がその下で働く。

### 6.2 端-端（多段ホップ）

多段ホップの途中リンクが細い場合、送信元が速く送りすぎると中継ノードのキューが溢れる。中継のドロップは無駄な帯域消費（既に上流を通ってきた）なので、**送信元で絞る**仕組みが必要。

方針：**クラス別に異なる仕組み**

| クラス | 端-端制御 |
|---|---|
| P0 | 送信元レート制限（トークンバケット、Topic ごとに設定。既定 100 pkt/s, 64 KB/s）。制御プレーンは自身の周期で自然に制限される |
| P1 | 制御しない。Conflation で自然に間引かれる。中継が捨てても「古い値」が消えるだけ |
| P2 | 送信元レート制限（既定 1 MB/s）。中継の Tail-drop を許容 |
| P3 | **明示的 credit**：Backfill は要求-応答（04）なので、要求側が `window` を指定し、応答側はその分だけ送る。中継のキュー深さは `mbtool` で観測し、window の既定値を調整 |

TCP 的な端-端輻輳制御は**実装しない**。理由：Pub/Sub の多対多で ACK ベースの制御を作ると複雑さが爆発する。代わりに「ライブ系は捨ててよい」「バルクは pull 型」という上位設計で帳尻を合わせる。

### 6.3 中継ノードの輻輳通知（将来）

キューが 80% を超えたら送信元に `CongestionHint{link_kind, depth}` を P0 で返し、送信元が P2 のレートを半減する。MVP では入れない。

## 7. 圧縮

- **ペイロード単位、送信元で zstd**。中継は触らない（E2E 暗号文は圧縮できないので、**暗号化の前に圧縮**する。順序：serialize → compress → encrypt）
- 閾値：256 bytes 未満は圧縮しない
- Topic ごとに **zstd 辞書**を持てる（Protobuf の track データは辞書で 3〜5 倍縮む）。辞書は Topic 定義（ポリシー）の一部として配布。辞書 ID を Pub/Sub エンベロープに載せる
- 制御プレーンの LSA / digest も同様に圧縮（フレームフラグ bit0。これはホップ間なので Link 層で行う）

## 8. コンポーネントとしての形（I/O なし）

```rust
pub enum ForwardEvent {
    Inbound  { link: LinkId, pkt: Packet },
    Outbound { pkt: Packet },                      // 上位層から
    OutboundMulticast { dsts: Vec<NodeId>, pkt: Packet },
    LinkCredit { link: LinkId, credit: usize },    // 送信可能になった通知
    RoutesUpdated(Arc<RouteTable>),
    LinkDown(LinkId),                              // そのリンクのキューを再ルーティング or drop
    Timer(ForwardTimer),                           // P0 の no-route 保留タイムアウト
}

pub enum ForwardAction {
    Send { link: LinkId, chan: Channel, frame: Bytes, datagram_ok: bool },
    DeliverPubSub(Packet),
    DeliverCircuit(Packet),
    Backpressure { flow_id: u64 },                 // P0 キュー満杯を上位へ
    SetTimer { timer: ForwardTimer, at: Instant },
    Metric(ForwardMetric),
}

pub struct Forwarder {
    me: NodeId,
    routes: Arc<RouteTable>,
    queues: HashMap<LinkId, LinkQueues>,   // LinkQueues = [VecDeque<Packet>; 4] + conflation map + DRR state
    pending_no_route: VecDeque<(Instant, Packet)>,
    rate_limiters: HashMap<u64 /*flow_id*/, TokenBucket>,
}
```

### 8.1 Link Down 時のキュー処理

- P0：経路表更新を待って別 next-hop へ再キュー（最大 2 s）
- P1：破棄（鮮度が意味。再送しても古い）
- P2：別 next-hop があれば再キュー、なければ破棄
- P3：破棄。Backfill は要求側が再要求する

## 9. パフォーマンス方針

- パケットは `Bytes`（参照カウント）で保持。ヘッダ書き換え（ttl）は先頭 88 bytes だけをコピーした新バッファ + payload は共有
- 300 ノード、無線環境なら 1 ノードの転送レートは数千 pkt/s 程度。**単一スレッドの状態機械で十分**。Ethernet 大量トラフィックが必要なら Link 単位でシャーディングするが MVP では不要
- ホットパスで確保するのはヘッダ 88 bytes のみ。ハッシュマップの lookup は `NodeId` の先頭 8 bytes を hash に使う（`FxHash`）

## 10. 検証すべき性質（08 で自動化）

| 性質 | 検証 |
|---|---|
| 優先度：P3 が飽和していても P0 の遅延が RTT + ε に収まる | 帯域 100 kbps リンクで P3 を無限に流しつつ P0 を送り、p99 遅延を測定 |
| Conflation：P1 の受信側が受け取る最後の値は送信側の最新値 | 帯域を絞って 1000 msg 送信、受信側の最終値 == 送信側の最終値 |
| ループしない | ランダムトポロジで ttl_exceeded カウントが 0（経路収束後） |
| Multicast：各リンクに同一メッセージが 1 回しか流れない | 送信元 1、購読者 20 で全リンクの送信カウントを検証 |
| Link Down 時の P0 保全 | 迂回路があるとき P0 の損失 0 |
