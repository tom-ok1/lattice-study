# 次工程用プロンプト

以下を次の作業依頼としてそのまま使用してください。

---

このリポジトリの `README.md`、`docs/00-overview.md` から `docs/09-roadmap.md`、および現在の実装を確認し、
Phase 4 の次の縦切りとして LSA ベースの Pub/Sub subscriber discovery に取り組んでください。
Phase 3 の決定論シミュレータは意図的に保留しています。

最小 live Pub/Sub は次の範囲まで実装済みです。

- I/O を持たない `mb-pubsub` の `Component` 境界
- Topic policy、ローカル購読、平文 Envelope、topic ごとの発行 sequence
- `last_seq + 64 bit bitmap` による out-of-order 対応 dedup
- 注入式 `DiscoveryIndex` から決定的な宛先集合を作り、64宛先単位で Explicit Multicast へ接続
- `latest + P1` の Conflation header 設定
- `mb-runtime` の publish / subscribe / discovery update API
- 実 loopback TCP 上の A-B-C 多段 live publish / subscribe テスト

次の最小スコープは以下です。

- `TopicAd` と subscription advertisement を制御プレーンのドメイン型へ追加する
- protobuf LSA codec に byte-preserving forwarding を壊さない形で `TopicAd` を追加する
- LSA/LSDB の更新から決定的な `DiscoveryIndex` を構築する
- runtime が手動 `update_pubsub_discovery` なしで Pub/Sub core を更新する
- 購読開始・解除後に subscriber 集合が収束するテストを追加する
- A-B-C の実 TCP テストから手動 discovery 注入を外す

今回含めないものは、Store-and-Forward、Backfill、暗号化、ローカル gRPC API、CLI、ECMP、Phase 3 の
シミュレータです。

必須の設計制約は次のとおりです。

- core crateへTokio、socket、ファイルI/O、実時刻取得を入れない
- discovery の反復順に `HashMap` の iteration 順を使わない
- LSA の byte-preserving forwarding と collection 上限を維持する
- 既存のユーザー変更を保持する

実装後は少なくとも以下を実行してください。

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

最後に、変更内容、検証結果、意図的な未実装範囲、次に進むべき一手を簡潔に報告してください。
コミットや Push は依頼された場合だけ行ってください。
