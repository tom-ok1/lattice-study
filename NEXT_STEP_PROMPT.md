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
- Link ごとの有限な P0〜P3 queue と incremental byte credit
- P0 strict priority、P1〜P3 の決定的 DRR、queue overflow の Backpressure / drop
- A-B-C の 2 hop 転送と Action 列の決定性テスト

次の目的は、**Phase 2 の次の縦切りとして P1 queue の Conflation を追加し、帯域待ちの間に同じキーの古い状態を最新 packet へ置き換えられるようにすること**です。

まずドキュメントと依存関係を確認し、実装前に短く以下を示してください。

1. 今回含める最小スコープ
2. 今回含めないもの
3. 後続開発のブロッカーを作らないために守る境界
4. 完了条件

有力なスコープは以下です。

- `ForwardFlags::CONFLATABLE` が付いた P1 packet だけを Conflation 対象にする
- key は payload を読まず `(source, flow_id, conflate_key)` から作る
- 同じ key が queue 内にあれば、queue 位置を維持したまま最新 packet へ置換する
- 置換時に `queued_bytes` を新しい packet sizeへ正しく更新する
- 異なる key、P0/P2/P3、flagなし packetは置換しない
- 同じ keyを1000回投入しても、credit付与後に最新packetだけが送られることを検証する
- Conflation後もDRR、queue上限、Action列の決定性が維持されることを検証する

必須の設計制約は次のとおりです。

- `mb-forward` に `tokio`、socket、ファイル I/O、実時刻取得を入れない
- packet priority は payload ではなく 88 byte header だけから判断する
- Conflation key は E2E 暗号化される可能性がある payload から導出しない
- 新しい packetへの置換でqueue内の順序を変えない
- 既存のcredit、P0 strict priority、DRR、queue overflowを壊さない
- Explicit Multicast、ECMP、Link Down 再キュー、P0 no-route 待機、runtime 接続は今回含めない
- 既存のユーザー変更を保持する

実装後は少なくとも以下を実行してください。

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

最後に、変更内容、検証結果、意図的な未実装範囲、次に進むべき一手を簡潔に報告してください。コミットや Push は依頼された場合だけ行ってください。
