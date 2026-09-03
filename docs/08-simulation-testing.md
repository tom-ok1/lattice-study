# 08. 決定論的シミュレーションとテスト戦略

## 1. なぜ最重要か

分散システムのバグは **タイミング依存で、再現しない**。
「リンク断のタイミングと LSA フラッドと SPF が重なると経路がループする」といった不具合を、実機で 1 週間走らせて 1 回踏むような開発では前に進めない。

目標：

> **本番コードそのものを、仮想時刻・仮想ネットワーク上で、シード 1 個から完全再現できる。**

これが成立すると：

- CI で数千シナリオを数分で回せる
- 失敗したらシードだけ持ち帰って手元で 100% 再現できる
- 実機で拾ったトポロジ（`mbtool sim-export`）を再生できる
- 「10 年に 1 度の 5 ノード同時断」を秒単位で試せる

FoundationDB が同じ手法で信頼性を作ったのが実例。

## 2. 決定論を成立させる条件

以下がすべて満たされて初めて「同じシードで同じ結果」になる。

| 非決定性の源 | 排除方法 |
|---|---|
| 実時刻 | `Clock` trait。シミュレーションは仮想時刻（離散イベント） |
| 乱数 | `Rng` trait。シード付き `ChaCha8Rng` |
| スレッドスケジューリング | **コアは単一スレッドの状態機械**。シミュレーションはイベントを順に処理 |
| `HashMap` の反復順 | シード固定のハッシャ（`FxHashMap` + 固定シード）、または順序が意味を持つ箇所は `BTreeMap` |
| I/O 完了順 | すべての I/O を `Action` として返し、シミュレータが順序を決める |
| アドレス・ポインタ値 | ID の生成をカウンタ化。ポインタを比較・ソートに使わない |
| `Instant::now()` の直接呼び出し | lint で禁止（`clippy.toml` の `disallowed-methods`） |
| 浮動小数点 | cost 計算は整数演算のみ。f32 は表示用メトリクスのみ |

**「コアは I/O を持たない状態機械」（00 §5.2）はこのためにある。** アーキテクチャ全体がこの制約から逆算されている。

## 3. シミュレータの構造

```rust
pub struct Sim {
    clock: VirtualClock,             // 現在の仮想時刻
    queue: BinaryHeap<ScheduledEvent>, // (at, seq, target, event) 時刻順。同時刻は seq で安定化
    nodes: Vec<SimNode>,
    net: VirtualNetwork,
    rng: ChaCha8Rng,
    seq: u64,                        // イベント投入順の tie-break
}

pub struct SimNode {
    id: NodeId,
    control: ControlPlane,           // ★ 本番と同一
    forward: Forwarder,              // ★ 本番と同一
    pubsub:  PubSub,                 // ★ 本番と同一
    circuits: CircuitManager,        // ★ 本番と同一
    storage: InMemoryStorage,        // Storage trait の別実装
    apps: Vec<SimApp>,               // 負荷生成 + 検証
}

pub struct VirtualNetwork {
    links: HashMap<(NodeId, NodeId), LinkModel>,
}

pub struct LinkModel {
    up: bool,
    latency: Distribution,     // 固定 / 正規 / パレート（テールを再現）
    jitter: Duration,
    loss: f32,
    bandwidth_bps: u64,        // トークンバケットで実際に絞る
    mtu: u16,
    reorder_prob: f32,
    duplicate_prob: f32,
    corrupt_prob: f32,         // 破損 → 受信側で検証失敗するはず
}
```

メインループ：

```rust
while let Some(ev) = self.queue.pop() {
    if ev.at > deadline { break }
    self.clock.set(ev.at);
    let actions = self.nodes[ev.target].handle(ev.at, ev.event);
    for a in actions {
        match a {
            Action::Send{link, frame, ..} => self.net.transmit(ev.at, link, frame, &mut self.queue, &mut self.rng),
            Action::SetTimer{timer, at}   => self.queue.push(ScheduledEvent{at, target: ev.target, event: Timer(timer), seq: self.next_seq()}),
            Action::Deliver{..}           => self.nodes[ev.target].apps.deliver(a),
            // ...
        }
    }
    self.invariants.check(&self.nodes, ev.at);   // §5
}
```

`VirtualNetwork::transmit` が帯域・遅延・損失を適用して受信イベントをキューに積む。帯域はリンクごとのトークンバケットで、送信完了時刻を計算して遅延に加算する（**細いリンクで大きなメッセージが実際に時間を食う**ことを再現する。これがないと Backfill と QoS のテストが無意味になる）。

## 4. シナリオ DSL

宣言的に書ける形にする。`.ron` か `.toml`。

```ron
Scenario(
    seed: 0xC0FFEE,
    duration: "10m",

    topology: Chain(nodes: ["drone-17", "vehicle-3", "ground-2", "c2-main"]),
    // 他: Star, Mesh(density: 0.3), Random(n: 100, seed: …), FromExport("state.json")

    link_defaults: LinkModel(
        latency: Normal(mean: "180ms", sd: "40ms"),
        loss: 0.04,
        bandwidth: "340kbps",
        mtu: 1200,
    ),

    topics: [
        Topic(name: "lattice.tracks.v1", mode: Latest, prio: P1),
        Topic(name: "lattice.c2.commands.v1", mode: Log, prio: P0),
    ],

    workload: [
        Publish(node: "drone-17", topic: "lattice.tracks.v1", rate: "10/s", size: "1KB"),
        Publish(node: "c2-main",  topic: "lattice.c2.commands.v1", rate: "0.2/s", size: "2KB"),
        Subscribe(node: "c2-main",  topic: "lattice.tracks.v1", start: Latest),
        Subscribe(node: "drone-17", topic: "lattice.c2.commands.v1", start: Cursor),
        Rpc(from: "c2-main", to_service: "lattice.entities.v1.EntityManager", rate: "1/s"),
    ],

    events: [
        At("30s",  LinkDown("vehicle-3", "ground-2")),
        At("2m",   LinkUp("vehicle-3", "ground-2")),
        At("3m",   Degrade("drone-17", "vehicle-3", loss: 0.35, bandwidth: "60kbps")),
        At("4m",   Partition(groups: [["drone-17","vehicle-3"], ["ground-2","c2-main"]])),
        At("6m",   Heal),
        At("7m",   NodeCrash("vehicle-3")),
        At("7m30s",NodeRestart("vehicle-3", keep_storage: true)),
        At("8m",   ClockSkew("drone-17", "+90s")),
    ],

    assertions: [
        AllPublishedDelivered(topic: "lattice.c2.commands.v1", within: "60s after Heal"),
        ConvergesWithin(after: "6m", limit: "20s"),
        NoMessageLoss(topic: "lattice.c2.commands.v1"),
        P0LatencyP99(below: "3s"),
        NoDuplicateDeliveryToApp,
    ],
)
```

`FromExport` で実機の状態を読み込めるのが 07 §4 との連携点。

## 5. 常時検査する不変条件 (Invariants)

シミュレーションの各ステップで自動チェックする。**これがシミュレータの価値の中心**。単にシナリオを流すだけでは、壊れていることに気づけない。

| # | 不変条件 | 破れたら何が起きているか |
|---|---|---|
| I1 | 経路表を辿って宛先に到達する（ループなし、ブラックホールなし） | SPF のバグ、LSDB 不整合 |
| I2 | 収束後、全ノードの LSDB が一致する（分断がない限り） | Gossip の取りこぼし、anti-entropy の欠陥 |
| I3 | LSA の `(origin, seq)` が単調増加 | seq 管理のバグ、再起動処理の欠陥 |
| I4 | 経路に使われる辺はすべて双方向アサーションを満たす | 02 §3 の実装ミス、偽広告の混入 |
| I5 | App に配信されるメッセージは重複しない（`(origin, topic, seq)` 一意） | dedup 窓の欠陥 |
| I6 | `mode=log` の Topic で、Backfill 完了後に欠落がない（Gap 宣言分を除く） | Backfill の範囲計算ミス |
| I7 | どのキューの深さも上限を超えない | バックプレッシャの欠落 |
| I8 | メモリ使用量（シミュレータが追跡する構造体サイズ）が単調増加しない | リーク |
| I9 | P0 の待ち時間が P3 の待ち時間を上回らない（同一リンク） | QoS スケジューラのバグ |
| I10 | 同一 nonce が同一鍵で二度使われない | 暗号の致命的欠陥 |
| I11 | Circuit の状態遷移が合法（Open→Accept→Data*→Close 等） | RPC 状態機械のバグ |
| I12 | 中継ノードのメモリに平文が現れない | E2E 暗号の実装ミス |

I10 と I12 はシミュレータが全ノードの内部を見られるからこそ検査できる。実機では困難。

## 6. Property-based / ランダムテスト

`proptest` でシナリオ自体を生成する。

```rust
proptest! {
    #[test]
    fn converges_under_random_churn(
        seed in any::<u64>(),
        n in 5usize..60,
        churn in prop::collection::vec(churn_event(), 0..200),
    ) {
        let mut sim = Sim::random_topology(seed, n);
        sim.apply_workload(standard_workload());
        for ev in churn { sim.schedule(ev) }
        sim.run(Duration::from_secs(600));
        sim.quiesce(Duration::from_secs(60));   // 変化を止めて収束させる
        prop_assert!(sim.all_lsdbs_equal());
        prop_assert!(sim.all_routes_valid());
        prop_assert_eq!(sim.undelivered_p0_messages(), 0);
    }
}
```

`quiesce`（churn を止めて安定させる期間）を置くのが重要。churn 中は当然一致しない。**「最終的には正しくなる」を検証する形にする**。

### 6.1 Swarm testing

パラメータ空間を一様に振るより、**極端な組み合わせに偏らせる**方がバグが出る。

```text
各実行でランダムに選ぶ:
  loss:       {0, 0.01, 0.3, 0.7} から
  latency:    {1ms, 200ms, 2s} から
  bandwidth:  {10Mbps, 300kbps, 20kbps} から
  churn rate: {なし, 稀, 秒ごと} から
```

「帯域 20 kbps + churn 毎秒 + loss 70%」のような現実離れした条件で壊れるコードは、現実の悪条件でも壊れる。

## 7. Fault Injection

| 障害 | 注入方法 |
|---|---|
| リンク断 / 復旧 | `LinkModel.up` |
| 分断 / 結合 | リンク集合の一括操作 |
| 片方向断 | 有向リンクの片方だけ down（**双方向条件のテストに必須**） |
| パケットロス / 並び替え / 重複 / 破損 | `LinkModel` の確率 |
| 帯域制限 | トークンバケット |
| ノードクラッシュ | 状態を捨てて再構築（`keep_storage` で永続化の有無を選ぶ） |
| クロックスキュー | ノードごとの時刻オフセット |
| ストレージ障害 | `InMemoryStorage` に注入する `fail_after_n_writes`、`corrupt_on_read` |
| 悪意ノード | `ByzantineNode` 実装（§8） |
| CPU 遅延 | ノードのイベント処理に人工的な仮想遅延を挿入（遅いノードの再現） |

## 8. Byzantine ノードのシミュレーション

セキュリティ設計（02 §3、06）が実際に効くかを検証する。

```rust
pub enum ByzantineBehavior {
    FalseAdjacency { claim_peers: Vec<NodeId>, cost: u32 },  // 全ノードに繋がっていると主張
    DropAll,                                                  // ブラックホール
    DropSelective { topic: TopicName },                       // 特定 Topic だけ捨てる
    ReplayOld { delay: Duration },                            // 古いメッセージを再送
    ForgeLsa { victim: NodeId },                              // 他ノードの LSA を偽造（署名は作れない）
    CorruptForward,                                           // 転送時に payload を改竄
    FloodLsa { rate: f64 },                                   // LSA を大量発行
    RevokedButActive,                                         // 失効後も動き続ける
}
```

各振る舞いに対する期待結果をアサートする：

```text
FalseAdjacency  → 他ノードの経路表にその辺が現れない（I4）
ForgeLsa        → 署名検証で破棄され LSDB に入らない
CorruptForward  → 受信側で AEAD 検証失敗、メトリクスに計上、App には届かない
FloodLsa        → レート制限が効き、正常ノードの CPU 使用が閾値以下
DropAll         → 迂回路がある限り到達性が維持される
RevokedButActive→ CRL 伝播後に新規リンク確立不可、群鍵ローテーション後に復号不能
```

## 9. テスト階層

```text
                    ┌──────────────────────────┐
                    │ 実機 / フィールド試験     │  月次。無線機材、実車両
                    ├──────────────────────────┤
                    │ netns + tc netem 統合     │  日次。実 QUIC/TLS/gRPC、5〜10 ノード
                    ├──────────────────────────┤
                    │ 決定論的シミュレーション   │  ★ CI 毎回。数百シナリオ、100 ノード
                    ├──────────────────────────┤
                    │ コンポーネント単体         │  CI 毎回。状態機械ごと
                    ├──────────────────────────┤
                    │ ファジング (cargo-fuzz)   │  継続実行。全パーサ
                    └──────────────────────────┘
```

**シミュレーション層が最も厚い**のがこの設計の特徴。通常のピラミッド（単体テストが最厚）とは違う。理由：このシステムのバグの大半は「単体では正しいコンポーネントの相互作用」にあるため。

### 9.1 netns 統合テストで検証すること

シミュレーションでは検証できない部分：

- 実際の QUIC ハンドシェイク、TLS 証明書検証、PMTUD
- 実際の HTTP/2 終端と tonic クライアント／サーバの互換性
- ソケットのエラー処理、`EMSGSIZE`、`ENOBUFS`
- fsync とストレージの実挙動
- CPU/メモリの実測

```bash
# 3 ノードチェーンを netns で構築し、無線を模擬
ip netns add n1; ip netns add n2; ip netns add n3
# n1-n2, n2-n3 を veth で接続
tc qdisc add dev veth12 root netem delay 180ms 40ms loss 4% rate 340kbit
```

### 9.2 CI 構成

| ジョブ | 内容 | 所要 |
|---|---|---|
| `unit` | 全 crate の単体テスト | 2 分 |
| `sim-quick` | 代表 30 シナリオ、固定シード | 3 分 |
| `sim-property` | proptest 200 ケース、ランダムシード | 15 分 |
| `sim-nightly` | swarm testing 5000 ケース、100 ノード | 4 時間 |
| `netns` | 統合テスト 10 本 | 10 分 |
| `fuzz` | OSS-Fuzz 相当、継続実行 | 常時 |
| `bench` | criterion によるスループット・レイテンシ回帰検出 | 10 分 |

失敗時は **シードとシナリオを artifact に保存**し、`mbsim replay <artifact>` で再現できるようにする。

## 10. 性能ベンチマーク（回帰検出）

| 指標 | 目標 | 測定 |
|---|---|---|
| 転送スループット（1 ノード、Ethernet） | > 100k pkt/s | criterion + netns |
| SPF（300 ノード） | < 2 ms | criterion |
| LSA 署名検証 | > 5k/s | criterion |
| AEAD 暗号化（1 KB） | > 200k/s | criterion |
| メモリ（300 ノード LSDB + 50 Topic） | < 200 MB RSS | netns 実測 |
| 起動から初回収束（10 ノード） | < 5 s | シミュレーション |

## 11. 実装方針

```text
mb-sim/
├── clock.rs         VirtualClock
├── net.rs           VirtualNetwork, LinkModel, トークンバケット
├── node.rs          SimNode（本番コンポーネントのホスト）
├── scenario.rs      DSL パーサ（serde + ron）
├── workload.rs      負荷生成 App
├── invariants.rs    I1〜I12 のチェッカ
├── byzantine.rs     悪意ノード実装
├── replay.rs        artifact からの再現、ステップ実行デバッガ
└── report.rs        結果レポート（タイムライン、メトリクス）
```

`mbsim` CLI：

```
mbsim run scenario.ron --seed 0xC0FFEE
mbsim replay failure-artifact.json --step        # 1 イベントずつ進めて状態を確認
mbsim fuzz --nodes 100 --duration 10m --count 1000
mbsim report last-run.json --format html         # タイムライン付き HTML レポート
```

`--step` のステップ実行デバッガが効く。「イベント 48213 で経路がループした」を特定したら、その直前まで進めて全ノードの LSDB を並べて見られる。

### 11.1 lint による決定論の強制

```toml
# clippy.toml
disallowed-methods = [
  { path = "std::time::Instant::now", reason = "Clock trait を使うこと" },
  { path = "std::time::SystemTime::now", reason = "Clock trait を使うこと" },
  { path = "rand::random", reason = "Rng trait を使うこと" },
  { path = "rand::thread_rng", reason = "Rng trait を使うこと" },
]
```

I/O なしコアの crate（`mb-control`, `mb-forward`, `mb-pubsub`）では `tokio` を依存に入れない。**Cargo の依存グラフで決定論を構造的に保証する**。
