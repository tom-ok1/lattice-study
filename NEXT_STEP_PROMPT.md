# 次工程用プロンプト

以下を次の作業依頼としてそのまま使用してください。

---

このリポジトリの `README.md`、`docs/00-overview.md` から `docs/09-roadmap.md`、および現在の実装を確認し、次の最小実装に取り組んでください。

現在は次の縦切りまで完了しています。

- `mb-control` の LSA、LSDB、Anti-Entropy、失効、SPF hold timer、Dijkstra
- 5 ノードの初期収束、分断、短経路追加後の再収束を検証する決定論テスト
- `mb-wire` の固定 8 byte Link header と固定 88 byte Forward header
- Forward header の version、packet type、priority、flags、サイズ制限の検証
- `mb-forward` の I/O なし `Component` 境界
- `RouteTable` に従うユニキャスト転送、ローカル配送、TTL decrement
- TTL exceeded、no route、incoming link への折り返し、未対応 packet type の明示的 drop
- A-B-C の 2 hop 転送と Action 列の決定性テスト

次の目的は、**Phase 2 の次の縦切りとして、Link ごとの有限な優先度キューと送信 credit を `mb-forward` に追加し、P3 が滞留していても P0 が先に送信されることを決定論テストで保証すること**です。

まずドキュメントと依存関係を確認し、実装前に短く以下を示してください。

1. 今回含める最小スコープ
2. 今回含めないもの
3. 後続開発のブロッカーを作らないために守る境界
4. 完了条件

有力なスコープは以下です。

- `ForwardEvent::LinkCredit` と Link ごとの利用可能 byte credit を追加する
- 即時 `Send` せず、next-hop の priority queue に packet を積む
- P0 は厳密優先、P1〜P3 は決定的な DRR で選択する
- 各 priority queue に packet 数または byte 数の明示的な上限を設ける
- overflow を `Backpressure` または明示的な drop reason として返す
- credit を超える packet は送信せず、次の credit 通知まで保持する
- P3 を先に大量投入してから P0 を投入し、credit 付与時に P0 が先に `Send` されることを検証する
- 同じ Event 列から同じ Action 列と queue 状態が得られることを検証する

必須の設計制約は次のとおりです。

- `mb-forward` に `tokio`、socket、ファイル I/O、実時刻取得を入れない
- packet priority は payload ではなく 88 byte header だけから判断する
- キューは必ず有限にし、overflow 動作を型とテストで明示する
- DRR の巡回順と tie-break を決定的にする
- 既存のユニキャスト、TTL、no-route、loop 検知を壊さない
- Conflation、Explicit Multicast、ECMP、Link Down 再キュー、P0 no-route 待機、runtime 接続は今回含めない
- 既存のユーザー変更を保持する

実装後は少なくとも以下を実行してください。

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

最後に、変更内容、検証結果、意図的な未実装範囲、次に進むべき一手を簡潔に報告してください。コミットや Push は依頼された場合だけ行ってください。
