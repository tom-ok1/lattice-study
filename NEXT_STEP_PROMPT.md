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
- `dst_count: u16` + `NodeId × n` 形式の Explicit Multicast codec と 64 宛先上限
- 空集合、重複、上限超過、切断された宛先リストの境界検証
- next hop ごとの決定的な multicast fan-out と Link ごとの単一 packet 送信
- local delivery、no-route、loop 宛先の分離と、分岐トポロジでの全宛先配送テスト
- Link Down 時の queue、credit、DRR 状態の除去
- P1/P3 の即時 drop、P2 の代替経路への再キュー
- P0 の 2 秒経路待機、期限切れ、stale timer の処理
- multicast の代替 next hop 別再分岐、TTL 維持、incoming Link への折り返し防止

次の目的は、**Phase 2 の次の縦切りとして ECMP と `flow_id` による next hop 選択を追加し、等コスト経路へflow単位で決定的に分散できるようにすること**です。

まずドキュメントと依存関係を確認し、実装前に短く以下を示してください。

1. 今回含める最小スコープ
2. 今回含めないもの
3. 後続開発のブロッカーを作らないために守る境界
4. 完了条件

有力な最小スコープは以下です。

- `Route` が等コストの複数 next hop を決定的な順序で保持できるようにする
- SPF で同一costの異なる first hopを失わず、重複なくRouteへ反映する
- `flow_id`からstableなindexを計算し、同一flowは常に同じnext hopを選ぶ
- Rustの実装やプロセスごとに変化するhash seedへ依存しない
- multicastは宛先ごとに選ばれたnext hopで従来どおりLink単位にまとめる
- Link Down再経路選択では切断済みcandidateを除外し、残るECMP経路を利用する
- 経路表、next hop選択、Action列の決定性テストを追加する

必須の設計制約は次のとおりです。

- `mb-forward` に `tokio`、socket、ファイル I/O、実時刻取得を入れない
- 中継ノードに Topic や購読状態を持たせない
- packet payload を解釈するのは既存の multicast 宛先リスト境界だけとし、本文を解釈しない
- Route candidate、Link、priority、queue内順序、Action の順序を決定的にする
- 既存のcredit、P0 strict priority、DRR、queue overflowを壊さない
- Link costの動的測定、通常送信時の P0 no-route 待機、Pub/Sub、runtime 接続は今回含めない
- 既存のユーザー変更を保持する

実装後は少なくとも以下を実行してください。

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

最後に、変更内容、検証結果、意図的な未実装範囲、次に進むべき一手を簡潔に報告してください。コミットや Push は依頼された場合だけ行ってください。
