# 次工程用プロンプト

以下を次の作業依頼としてそのまま使用してください。

---

このリポジトリの `README.md`、`docs/00-overview.md` から `docs/09-roadmap.md`、および現在の実装を確認し、
Phase 4 の次の縦切りとして、アプリケーション向けローカル gRPC Pub/Sub API に取り組んでください。
Phase 2 の強化項目と Phase 3 の決定論シミュレータは意図的に保留しています。

最小 live Pub/Sub は次の範囲まで実装済みです。

- I/O を持たない `mb-pubsub` の `Component` 境界
- Topic policy、ローカル購読、平文 Envelope、topic ごとの発行 sequence
- `last_seq + 64 bit bitmap` による out-of-order 対応 dedup
- LSA の `TopicAd` から決定的な `DiscoveryIndex` を構築し、64宛先単位で Explicit Multicast へ接続
- `latest + P1` の Conflation header 設定
- `mb-runtime` の publish / subscribe / unsubscribe APIと、自動subscriber discovery
- 実 loopback TCP 上の A-B-C 多段 live publish / subscribe テスト

次の最小スコープは以下です。

- unix domain socket 上の tonic gRPC `PubSubApi` を追加する
- streaming `Publish` を `ControlRuntime::publish` へ接続し、受理・拒否を `PublishAck` として返す
- server-streaming `Subscribe` を subscription lifecycle と `PubSubOutcome::Delivered` へ接続する
- stream切断時に必ずunsubscribeし、LSAのsubscriber広告が収束して消えるようにする
- 遅いローカルsubscriberがruntime全体を停止させない、有限キューと明示的な切断方針を持たせる
- ローカルgRPC経由でA-B-C多段publish / subscribeが成立する実TCP統合テストを追加する

今回含めないものは、動的Topic作成、Store-and-Forward、Backfill、暗号化、CLI、ECMP、Phase 2 の
診断・メトリクス・`tc netem`強化、およびPhase 3のシミュレータです。

必須の設計制約は次のとおりです。

- core crateへTokio、socket、ファイルI/O、実時刻取得を入れない
- gRPC型をcore crateへ漏らさず、adapterをruntimeまたは専用API crateに置く
- subscriberごとの送信待ちで単一runtime loopをblockしない
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
