# 次工程用プロンプト

以下を次の作業依頼としてそのまま使用してください。

---

このリポジトリの `README.md`、`docs/00-overview.md` から `docs/09-roadmap.md`、および現在の実装を確認し、
Phase 3 の最初の縦切りとして最小の決定論シミュレータに取り組んでください。

Phase 2 は次の必須範囲まで完了しています。

- 固定 Forward header、ユニキャスト、TTL、loop / no-route drop
- 有限 priority queue、incremental byte credit、P0 strict priority、P1〜P3 DRR
- P1 Conflation
- Explicit Multicast codec と決定的な fan-out
- Link Down 時の priority 別処理と P0 の2秒経路待機
- `mb-runtime` による control / forwarding core と TCP transport の接続
- Link ごとに Forward packet を1個だけ送信中にし、byte credit（容量）と `LinkWritable`（1 packet の送信許可）を分離する境界。TCP送信完了時は実送信 byte 数と次の許可を返す
- 実 loopback TCP 上の A-B-C 多段ユニキャストとMulticastテスト

Phase 2 の完了を優先したため、ECMP と `flow_id` hash、`mbtool trace`、Prometheus メトリクス、Linux
`tc netem` の性能試験は意図的に後続バックログへ延期しています。Phase 3 の開始時にこれらを混ぜないでください。
帯域制限下のQoSは、まず simulator の LinkModel で決定論的に検証し、実環境試験で後から追認します。

次の最小スコープは以下です。

- 新しい `mb-sim` crate を追加する
- 外部I/Oと実時刻に依存しない `VirtualClock` と、時刻・投入順で安定順序になる離散イベントキューを実装する
- `mb-control` と `mb-forward` の既存 `Component` を変更せずに駆動できる境界を作る
- 同じseedと入力から完全に同じイベント列が得られるテストを追加する
- 最初は固定遅延・損失なし・十分な帯域の2〜3ノードに限定する

今回含めないものは、確率損失、並び替え、churn DSL、50ノード負荷、ECMP、Pub/Sub、実socket、実時刻です。

必須の設計制約は次のとおりです。

- core crateへTokio、socket、ファイルI/O、実時刻取得を入れない
- eventの同時刻順序を明示的な連番で決め、HashMapのiteration順に依存しない
- production runtimeとsimulatorで同じ`Component`実装を使う
- 既存のユーザー変更を保持する

実装後は少なくとも以下を実行してください。

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

最後に、変更内容、検証結果、意図的な未実装範囲、次に進むべき一手を簡潔に報告してください。
コミットやPushは依頼された場合だけ行ってください。
