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
- P1 かつ `CONFLATABLE` の packet を `(source, flow_id, conflate_key)` で置換する Conflation
- Conflation 時の queue 位置維持、`queued_bytes` 更新、満杯時の同一 key 置換
- A-B-C の 2 hop 転送と Action 列の決定性テスト

次の目的は、**Phase 2 の次の縦切りとして Explicit Multicast の fan-out を追加し、同じ next hop を通る複数宛先へ packet を Link ごとに一度だけ送れるようにすること**です。

まずドキュメントと依存関係を確認し、実装前に短く以下を示してください。

1. 今回含める最小スコープ
2. 今回含めないもの
3. 後続開発のブロッカーを作らないために守る境界
4. 完了条件

有力なスコープは以下です。

- `mb-forward` に宛先集合を受け取る明示的な multicast event を追加する
- 宛先は最大64件に制限し、空集合、重複、上限超過の扱いを明示する
- `RouteTable` の next hop ごとに宛先を決定的にグループ化する
- 同じ next hop の宛先には packet を一つだけ生成し、異なる next hop にはそれぞれ一つ生成する
- 自ノードを含む場合は local delivery し、到達不能な宛先を他の宛先から分離して扱う
- multicast の宛先リスト codec を境界検証つきで実装する
- 分岐後も payload の `Bytes` を可能な限り共有する
- 分岐トポロジで全宛先へ届き、各 Link の送信が一回だけであることと Action 列の決定性を検証する

必須の設計制約は次のとおりです。

- `mb-forward` に `tokio`、socket、ファイル I/O、実時刻取得を入れない
- 中継ノードに Topic や購読状態を持たせない
- 宛先リストとアプリケーション payload の境界を明示し、本文を解釈しない
- 同一入力からの next hop と Action の順序を決定的にする
- 既存のcredit、P0 strict priority、DRR、queue overflowを壊さない
- ECMP、Link Down 再キュー、P0 no-route 待機、Pub/Sub、runtime 接続は今回含めない
- 既存のユーザー変更を保持する

実装後は少なくとも以下を実行してください。

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

最後に、変更内容、検証結果、意図的な未実装範囲、次に進むべき一手を簡潔に報告してください。コミットや Push は依頼された場合だけ行ってください。
