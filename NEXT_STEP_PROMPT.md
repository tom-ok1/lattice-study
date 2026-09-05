# 次工程用プロンプト

以下を次の作業依頼としてそのまま使用してください。

---

このリポジトリの `README.md`、`docs/00-overview.md` から `docs/09-roadmap.md`、および現在の実装を確認し、次の最小実装に取り組んでください。

現在は次の縦切りまで完了しています。

- `mb-types`: `NodeId`、`LinkId`、`MonoTime`、I/O なしの `Component` 境界
- `mb-control`: 最小 LSA、LSDB、フラッディング、双方向アサーション、Dijkstra、`RouteTable`
- `Digest` / `DigestReq` による Link Up 時と周期的な Anti-Entropy
- `ControlEvent::Timer` / `ControlAction::SetTimer` と Tokio runtime のタイマー配送
- 動的参加と LSA 取りこぼし復旧の決定論テスト、loopback TCP テスト

次の目的は、**LSA の再発行・失効と seq の永続化を実装し、再起動や長期分断でも古い経路が残らず、新しい LSA が確実に採用される状態にすること**です。

まずドキュメントと依存関係を確認し、実装前に短く以下を示してください。

1. 今回含める最小スコープ
2. 今回含めないもの
3. 後続開発のブロッカーを作らないために守る境界
4. 完了条件

有力なスコープは以下です。ただし、一度に広すぎる場合は独立して検証可能な単位へ縮小してください。

- LSA に `ttl_sec` を追加し、受信時刻から失効時刻を管理する
- `LsaRefresh` / `LsaExpire` / 必要なら `LsaPurge` を既存の Timer 境界に追加する
- 失効した LSA を SPF から除外し、保持期間後に LSDB から削除する
- 古い LSA の再受信で失効状態が巻き戻らないようにする
- `ControlPlane` へ保存済み初期 seq を注入できるようにする
- `PersistSeq` を runtime の永続化 adapter へ接続する。ただしファイル I/O は `mb-control` に入れない
- LSDB エントリ数と protobuf 内の要素数に上限を設ける
- 仮想時刻テストで再発行、失効、削除、再起動後の seq 単調増加を検証する

必須の設計制約は次のとおりです。

- `mb-control` に `tokio`、socket、ファイル I/O、実時刻取得を入れない
- runtime は Timer/Event を投入して Action を実行するだけにする
- 時刻同期を前提にせず、鮮度はローカル単調時刻で判定する
- 既存の Anti-Entropy と canonical LSA bytes の保持を壊さない
- SPF hold、ECMP、TLS、QUIC、Admin API、Pub/Sub は今回の目的に含めない
- 既存のユーザー変更を保持する

実装後は少なくとも以下を実行してください。

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

環境に Rust がなければ勝手に恒久インストールせず、一時ツールチェーンを使うか必要な許可を求めてください。最後に、変更内容、検証結果、意図的な未実装範囲、次に進むべき一手を簡潔に報告してください。コミットや Push は依頼された場合だけ行ってください。
