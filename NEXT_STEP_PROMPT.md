# 次工程用プロンプト

以下を次の作業依頼としてそのまま使用してください。

---

このリポジトリの `README.md`、`docs/00-overview.md` から `docs/09-roadmap.md`、および現在の実装を確認し、次の最小実装に取り組んでください。

現在は次の縦切りまで完了しています。

- `mb-types`: `NodeId`、`LinkId`、`MonoTime`、I/O なしの `Component` 境界
- `mb-control`: 最小 LSA、LSDB、フラッディング、双方向アサーション、Dijkstra、`RouteTable`
- `Digest` / `DigestReq` による Link Up 時と周期的な Anti-Entropy
- `ControlEvent::Timer` / `ControlAction::SetTimer` と Tokio runtime のタイマー配送
- TTL による LSA の周期再発行、失効、compact tombstone 化
- 保存済み seq の次からの再開と、runtime のメモリ／ファイル永続化 adapter
- LSDB、LSA adjacency、Digest、DigestReq の要素数上限
- 100 ms 初期、最大 5 s、10 s quiet reset の SPF hold timer
- generation による stale SPF timer の無視と、burst 中の経路再計算・公開の集約
- 動的参加と LSA 取りこぼし復旧の決定論テスト、loopback TCP テスト

次の目的は、**Phase 1 の完了条件を決定論テストとして固定し、5 ノードの経路収束とトポロジ変更後の再収束を検証できる状態にすること**です。

まずドキュメントと依存関係を確認し、実装前に短く以下を示してください。

1. 今回含める最小スコープ
2. 今回含めないもの
3. 後続開発のブロッカーを作らないために守る境界
4. 完了条件

有力なスコープは以下です。

- A-B-C-D-E の 5 ノードチェーンを構築し、A から E が 4 hop になることを検証する
- B-C を切断し、分断された宛先が各ノードの経路表から 5 秒以内に消えることを検証する
- A-D の直接リンクを追加し、A から E が 2 hop の経路へ再収束することを検証する
- SPF hold timer を含む仮想時刻を明示的に進め、収束時刻をテスト結果から確認できるようにする
- 同じ入力から同じイベント列と経路表が得られる決定性を維持する

必須の設計制約は次のとおりです。

- `mb-control` に `tokio`、socket、ファイル I/O、実時刻取得を入れない
- runtime は Timer/Event を投入して Action を実行するだけにする
- テストハーネスは将来の `mb-sim` と同じく、イベント時刻と投入順で決定的に駆動する
- 既存の LSA lifecycle、Anti-Entropy、canonical LSA bytes の保持を壊さない
- `mb-forward`、ECMP、TLS、QUIC、Admin API、Pub/Sub、メトリクスは今回の目的に含めない
- 既存のユーザー変更を保持する

実装後は少なくとも以下を実行してください。

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

環境に Rust がなければ勝手に恒久インストールせず、一時ツールチェーンを使うか必要な許可を求めてください。最後に、変更内容、検証結果、意図的な未実装範囲、次に進むべき一手を簡潔に報告してください。コミットや Push は依頼された場合だけ行ってください。
