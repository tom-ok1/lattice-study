# 01. Transport 層 — Link 抽象・QUIC/TCP・近隣探索・フレーム多重化

## 1. 責務

隣接ノード（IP で直接届く相手）との **1 ホップの、認証済み、多重化されたチャネル**を提供する。
上位層に対して以下を約束する：

- 相手の `NodeId` は mTLS で検証済みである
- 「信頼性のあるストリーム」と「信頼性のないデータグラム」の 2 種類を提供する
- リンクの品質指標（RTT、損失率、推定帯域、MTU）を継続的に報告する
- 断を数秒以内に検知し、上位に通知する

経路や宛先の概念は持たない。

## 2. Link trait

```rust
pub struct LinkId(u64);                      // ローカルで一意。再接続で変わる

pub enum LinkKind { Ethernet, Radio, Satcom, Unknown }

pub struct LinkMetrics {
    pub rtt: Duration,          // 平滑化 RTT
    pub loss: f32,              // 0.0..1.0、直近 N 秒
    pub bw_estimate: u64,       // bytes/sec、輻輳制御の推定値
    pub mtu: u16,               // path MTU（QUIC の PMTUD 結果）
    pub kind: LinkKind,         // 設定 or インタフェース名から推定
    pub queue_depth: usize,     // 送信待ちバイト数（バックプレッシャ用）
}

/// I/O なしコアから見た Link。実装は mb-transport（tokio+quinn）または mb-sim。
pub trait Link {
    fn id(&self) -> LinkId;
    fn peer(&self) -> NodeId;
    fn metrics(&self) -> LinkMetrics;

    /// 信頼性あり・順序あり。chan は上位層が決める論理チャネル（下記 §5）。
    fn send_stream(&mut self, chan: Channel, frame: Bytes) -> Result<(), SendError>;

    /// 信頼性なし。MTU 超過は Err。ライブ値の最新のみが意味を持つ用途。
    fn send_datagram(&mut self, frame: Bytes) -> Result<(), SendError>;

    /// 送信可能なクレジット（bytes）。0 なら上位はキューに留める。
    fn send_credit(&self, chan: Channel) -> usize;
}

/// Transport → 上位へのイベント
pub enum LinkEvent {
    Up   { link: LinkId, peer: NodeId, metrics: LinkMetrics },
    Down { link: LinkId, peer: NodeId, reason: DownReason },
    MetricsChanged { link: LinkId, metrics: LinkMetrics },
    StreamFrame   { link: LinkId, chan: Channel, frame: Bytes },
    Datagram      { link: LinkId, frame: Bytes },
}
```

**`send_credit` を trait に置く理由**：フロー制御を Transport 内部に隠すと、上位の QoS スケジューラが「今この link にどれだけ流せるか」を知れず、優先度の低いデータが transport 内のバッファを埋めてしまう。クレジットを露出して **バッファは上位（QoS キュー）に置き、transport のバッファは最小**にする。

## 3. トランスポート選択

### 3.1 QUIC（第一候補）

理由：

| 要件 | QUIC が解決するもの |
|---|---|
| 1 接続で複数ストリーム、HoL ブロッキング回避 | ネイティブのストリーム多重化 |
| 断続リンクでの再接続コスト | 0-RTT 再接続、Connection Migration（無線の IP 変化に耐える） |
| TLS 1.3 内蔵 | 別途 TLS 層を持たなくていい |
| 信頼性なし送信 | DATAGRAM 拡張 (RFC 9221) |
| Path MTU | PMTUD 内蔵 |
| 輻輳制御の差替え | quinn は Cubic / BBR を選択可。無線向けに独自実装も差せる |

実装：`quinn` + `rustls`。

設定方針：

- `max_idle_timeout = 3s`、`keep_alive_interval = 1s`
- `initial_rtt` はリンク種別で変える（Ethernet 10ms / Radio 200ms / Satcom 600ms）。誤った初期値は最初の数秒の再送を無駄にする
- `congestion_controller`：Ethernet は Cubic、無線・SATCOM は **BBR**（損失ベースの Cubic は無線のランダムロスで帯域を過小評価する）
- ストリーム数上限は小さく（`max_concurrent_bidi_streams = 32`）。チャネル（§5）ごとに 1 ストリームを長寿命で使う
- `datagram_receive_buffer_size` を有効化

### 3.2 TCP + TLS 1.3（フォールバック）

QUIC が通らない環境（UDP を落とすファイアウォール、UDP 非対応の中継装置）向け。

- `tokio::net::TcpStream` + `tokio-rustls`
- 多重化は自前（§5 のフレーミングで `chan` を持つため、1 本の TCP に全チャネルを流す）
- HoL ブロッキングは避けられない。**P0/P1 と P2/P3 で TCP 接続を 2 本張る**ことで最悪ケースを緩和する
- データグラム API は `Err(Unsupported)` を返す。上位は stream に fallback する

### 3.3 選択ロジック

1. 相手アドレスに QUIC で 3 回（指数バックオフ 200ms/800ms/2s）試行
2. 失敗したら TCP を試行。TCP が成功したらその相手には TCP を記憶（1 時間）
3. 両方失敗したらそのアドレスは次の discovery まで保留

## 4. 近隣探索 (Neighbor Discovery)

3 つのソースを併用し、いずれかで見つかった相手に接続を試みる。

### 4.1 静的ピア（設定ファイル）

```toml
[[peers]]
addr = "10.20.0.5:7400"
node_id = "b7e1…"     # 省略可。指定時は mTLS 結果と一致必須
```

SATCOM 経由の地上局など、multicast が届かない相手向け。

### 4.2 UDP マルチキャスト Hello（同一セグメント）

- 宛先 `239.77.66.1:7401`、2 秒間隔
- ペイロード：`HelloV1 { node_id, listen_addrs: [SocketAddr], transports: [Quic, Tcp], epoch: u64, sig }`
- `sig` はノード鍵による署名。**署名の検証は接続前フィルタ**としてのみ使い、本当の認証は mTLS で行う（Hello の偽装で接続試行を誘発されても、mTLS で落ちる）
- `epoch` はプロセス起動時刻。相手が再起動したことを検知し、古い Link を即座に破棄する材料にする
- 受信レート制限：同一送信元 IP から 1 秒あたり 5 個超は破棄

### 4.3 Gossip で学んだアドレス（02 参照）

LSA に `listen_addrs` を含める。2 ホップ先のノードのアドレスを知っていれば、IP 到達性がある場合に直接リンクを張れる（メッシュが密になり、経路が短くなる）。

- ただし全ノードに全接続を試みると N² になる。**接続試行は「経路コストが改善する見込みがある相手」に限定**：現在の経路コストが閾値以上、かつ同一 /16 または明示的に `direct_allowed` なアドレス
- 上限 `max_opportunistic_links = 8`

### 4.4 接続の方向とグレア（同時接続）

A→B と B→A が同時に張られたら **NodeId が小さい側が initiator の接続を残し**、他方を閉じる。判定は mTLS 完了後、最初の `LinkHello` フレーム交換で行う。

## 5. チャネルと多重化

```rust
#[repr(u8)]
pub enum Channel {
    Control  = 0,   // LSA flood, digest, keepalive 拡張 (P0 相当)
    Rpc      = 1,   // 仮想回線（05）
    PubSubP0 = 2,   // C2 コマンド系 Topic
    PubSubP1 = 3,   // ライブトラック
    PubSubP2 = 4,   // テレメトリ
    PubSubP3 = 5,   // Backfill
    Tunnel   = 6,   // 多段ホップ用の生転送（03 の転送パケットはここ）
}
```

QUIC では **チャネル = 1 本の長寿命 bidi ストリーム**。TCP ではフレームヘッダの `chan` で区別。

RPC の仮想回線（05）は多数の同時 RPC を扱うため、`Rpc` チャネルの中でさらに `circuit_id` で多重化する（QUIC のストリームを RPC ごとに開かない）。理由：ストリーム open/close が相手側のイベントになりコストが高い、TCP fallback と挙動を揃えたい。

## 6. フレーム形式（L2）

QUIC ストリーム／TCP 上の区切り。**固定 8 バイトヘッダ + ペイロード**。

```text
 0                   1                   2                   3
 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1 2 3 4 5 6 7 8 9 0 1
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|  ver  | type  |     chan      |     flags     |   reserved    |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                        payload length (u32 BE)                |
+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
|                          payload ...                          |
```

- `ver` = 1。不一致は接続拒否
- `type`：`LinkHello=1, Lsa=2, Digest=3, DigestReq=4, Forward=5, Circuit=6, Credit=7, Ping=8`
- `flags`：bit0 = zstd 圧縮済み、bit1 = 断片化あり（TCP fallback で巨大ペイロードを分割する場合）
- `payload length` 上限 = 1 MiB。超過は接続切断（メモリ保護）

QUIC DATAGRAM で送るときはヘッダは同じだが `length` は冗長（フレーム境界が明示されるため）。実装を 1 本にするため同じコーデックを使う。

## 7. リンク品質の測定と cost への変換

制御プレーン（02）に渡す **cost** は以下で算出。OSPF の cost が「帯域の逆数」なのに対し、DDIL では遅延と損失も強く効く。

```text
cost = clamp(
    base(kind)
  + rtt_ms / 10
  + loss_penalty(loss)        // loss 0% → 0, 10% → 200, 30% → 1000
  + 1_000_000 / max(bw_estimate_bps, 1000) ,
  1, 65535)

base(Ethernet)=1, base(Radio)=50, base(Satcom)=200
```

**ヒステリシス**：cost の変動が ±20% 未満なら LSA を更新しない。無線の揺らぎで LSA が毎秒フラッドされることを防ぐ。20% を超えた変動も 5 秒に 1 回まで。

## 8. 実装方針（tokio 側）

```text
mb-transport
├── discovery/
│   ├── static.rs        設定ピアの周期的接続試行
│   ├── multicast.rs     Hello 送受信、レート制限
│   └── gossiped.rs      LSA 由来アドレスの機会的接続
├── quic.rs              quinn Endpoint、Connection → Link 実装
├── tcp.rs               TcpStream + rustls → Link 実装、自前 mux
├── codec.rs             8 バイトヘッダ、長さ制限、zstd
├── link_table.rs        LinkId ↔ peer NodeId、グレア解決、再接続バックオフ
└── metrics.rs           quinn stats → LinkMetrics、cost 計算、ヒステリシス
```

- 1 Link = 1 tokio タスク（送信）+ 1 タスク（受信）。受信タスクは `LinkEvent` を **単一の mpsc** でコア（mb-runtime）へ渡す。コアは単一タスクで状態機械を回す（ロック不要）
- 送信は「コアが `Action::Send{link, chan, frame}` を出す → link ごとの bounded mpsc (容量 = 数フレーム) → 送信タスク」。**mpsc を小さくして transport にバッファを溜めない**。溜めるのは 03 の QoS キュー
- 再接続バックオフ：200ms → ×2 → 最大 30s。Hello を再受信したら即リセット
- QUIC の `Connection::stats()` を 1 秒ごとにポーリングして `MetricsChanged` を出す（ヒステリシス通過時のみ）

## 9. 設計上の落とし穴と対応

| 落とし穴 | 対応 |
|---|---|
| 無線の一時的な 100% ロスで 3 s 以内に切断判定 → 経路フラップ | Link Down を即 LSA 反映するが、Up 復帰時は 5 s の hold-down を置いてから広告（02 §7） |
| QUIC の PMTUD が無線で収束しない | `initial_mtu = 1200`、`min_mtu = 576` を設定。datagram は常に 1100 bytes 以下に制限 |
| 多数ノードが同時起動し Hello が同時発火 | Hello 間隔に ±25% のジッタ |
| NAT 越え | 非目的。ノードは同一ネットワーク or 明示的な静的ピア設定前提。将来 QUIC の path validation で対応可能 |
| TCP fallback 時の HoL で P0 が遅延 | 高優先／低優先で TCP を 2 本に分離 |
