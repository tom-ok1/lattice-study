# lattice-study — 分散 P2P サービスバス設計スタディ

Anduril Lattice の DSB (Distributed Service Bus) / Lattice Mesh を公開情報から推定した構造をもとに、
「自分で同等のものを実装するならどう設計するか」を書き下ろした設計ドキュメント一式。

実装対象を **mesh-bus**、デーモンを `mbd`、CLI を `mbtool`、シミュレータを `mbsim` と呼ぶ。

## 読む順序

| # | ドキュメント | 内容 |
|---|---|---|
| 00 | [全体設計](docs/00-overview.md) | 目的・前提・脅威モデル・レイヤー構成・crate 構成・横断的判断 |
| 01 | [Transport](docs/01-transport.md) | Link 抽象、QUIC/TCP、近隣探索、フレーム多重化、cost 算出 |
| 02 | [Control Plane](docs/02-control-plane.md) | 署名付き LSA、双方向アサーション、Gossip、LSDB、SPF |
| 03 | [Data Plane](docs/03-data-plane.md) | オーバーレイ転送、優先度キュー、Conflation、Explicit Multicast |
| 04 | [Pub/Sub](docs/04-pubsub.md) | Topic モデル、購読セマンティクス、Store-and-Forward、Backfill |
| 05 | [RPC Proxy](docs/05-rpc-proxy.md) | gRPC 透過プロキシ、サービス発見、仮想回線 |
| 06 | [Security](docs/06-security.md) | 識別、mTLS、E2E 暗号、群鍵、認可 |
| 07 | [Observability](docs/07-observability.md) | メトリクス、`mbtool`、経路トレース、キャプチャ |
| 08 | [Simulation & Testing](docs/08-simulation-testing.md) | 決定論的シミュレーション、不変条件、Byzantine テスト |
| 09 | [Roadmap](docs/09-roadmap.md) | 段階的実装計画と学習用の最小構成 |

## この設計を貫く 5 つの判断

1. **コアは I/O を持たない状態機械**。制御・転送・Pub/Sub の判断ロジックは `Component` trait
   （`handle(now, event) -> Vec<Action>`）として書き、`tokio` に依存させない。これにより本番コードそのものを
   仮想時刻・仮想ネットワーク上でシードから完全再現できる。アーキテクチャ全体がこの制約から逆算されている。

2. **経路広告は署名 + 双方向アサーション**。A が「B に届く」と主張するだけでは辺にならず、B も「A に届く」と
   署名して初めて経路計算に使う。侵害ノードが偽の到達性を広告してトラフィックを吸い込む攻撃を構造的に防ぐ。

3. **中継ノードは Topic を知らないし、読めない**。配信は Explicit Multicast（宛先リストをパケットに載せる）
   なので中継に Topic 状態がなく、ペイロードは Topic 群鍵で E2E 暗号化されるので中継は読めない。
   保存専用ノードには群鍵を配らず、暗号文のまま Store-and-Forward させる。

4. **Backfill は pull 型で、余り帯域だけを使う**。購読者が LSA の `head_seq` からギャップを検出し、
   最も近いストアノードへ window 付きで要求する。優先度 P3 なので、ライブデータを決して押しのけない。
   保持していない範囲は Gap として明示的に返し、購読者を無限に待たせない。

5. **RPC は各端で HTTP/2 を終端し、メッセージ単位で運ぶ**。生の HTTP/2 をトンネルする方が簡単だが、
   フロー制御が二重になって QoS が壊れ、HPACK 状態が経路変更で壊れる。終端することで RPC 単位の
   優先度・デッドライン・キャンセルをメッシュ側で扱える。

## 元になった公開情報

- Anduril の Distributed Networks 系求人の要求技術（Rust、TCP/UDP、MTU、multicast、gossip、
  OSPF-style link-state routing、QoS、flow control、ECDSA、Prometheus/Grafana、deterministic simulation）
- 特許 US20250220426A1 "Lattice mesh"（TCP/QUIC 上の多重化、署名付き到達性アサーション、
  Pub/Sub の購読セマンティクス、live 優先 + backfill）
- Lattice 開発者ドキュメント（node 間の gRPC + Protocol Buffers）

推定に基づく設計であり、Anduril の実装を記述したものではない。

## 実装状況

最初の縦切りとして、I/O を持たない制御プレーンと最小ユニキャスト転送、それを実 TCP から駆動する control runtime を実装済み。

- `mb-types`: `NodeId`、`LinkId`、単調時刻、`Component` 境界
- `mb-control`: TTL 付き adjacency LSA、LSDB、フラッディング、Anti-Entropy、失効・再発行、双方向アサーション、SPF hold timer、Dijkstra
- イベント時刻と投入順で駆動する決定論的な 5 ノード用テストハーネス
- `mb-wire`: LSA/Digest protobuf、固定 8 byte Link header、固定 88 byte Forward header、1 MiB 上限、incremental decoder
- `mb-forward`: `RouteTable` による I/O なしのユニキャスト転送、TTL、no-route、ループ検知、有限 priority queue、byte credit、P0 strict priority + P1〜P3 DRR、P1 Conflation
- `mb-transport`: TLS なし・静的ピア限定の TCP adapter
- `mb-runtime`: TCP と control の Event/Action、単調時刻 Timer、再起動をまたぐ seq 永続化を接続する Tokio glue
- 仮想時刻による 5 ノードの経路収束・分断・短経路への再収束と、loopback TCP 上の動的な 3 ノード参加・経路収束テスト

```sh
cargo test --workspace
```
