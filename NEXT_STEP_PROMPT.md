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
- 動的参加と LSA 取りこぼし復旧の決定論テスト、loopback TCP テスト

次の目的は、**SPF hold timer を実装し、LSA が短時間に集中して変化しても経路計算と `RouteTable` の差し替えをまとめられる状態にすること**です。

まずドキュメントと依存関係を確認し、実装前に短く以下を示してください。

1. 今回含める最小スコープ
2. 今回含めないもの
3. 後続開発のブロッカーを作らないために守る境界
4. 完了条件

有力なスコープは以下です。ただし、一度に広すぎる場合は独立して検証可能な単位へ縮小してください。

- LSDB の変更と SPF の実行を分離し、変更時には即時計算せず `SpfHold` timer を設定する
- 初回変更は 100 ms、連続変更は 200 ms、400 ms と伸ばし、最大 5 s に制限する
- 変更が 10 s なければ hold を 100 ms に戻す
- 古い世代の timer が新しいスケジュールを実行しないよう generation を持たせる
- hold 中の複数変更を 1 回の Dijkstra と最大 1 回の `PublishRoutes` にまとめる
- 仮想時刻テストで単発変更、burst、backoff、quiet reset、古い timer の無視を検証する

必須の設計制約は次のとおりです。

- `mb-control` に `tokio`、socket、ファイル I/O、実時刻取得を入れない
- runtime は Timer/Event を投入して Action を実行するだけにする
- 時刻同期を前提にせず、hold はローカル単調時刻で判定する
- 既存の LSA lifecycle、Anti-Entropy、canonical LSA bytes の保持を壊さない
- ECMP、TLS、QUIC、Admin API、Pub/Sub、メトリクスは今回の目的に含めない
- 既存のユーザー変更を保持する

実装後は少なくとも以下を実行してください。

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

環境に Rust がなければ勝手に恒久インストールせず、一時ツールチェーンを使うか必要な許可を求めてください。最後に、変更内容、検証結果、意図的な未実装範囲、次に進むべき一手を簡潔に報告してください。コミットや Push は依頼された場合だけ行ってください。
