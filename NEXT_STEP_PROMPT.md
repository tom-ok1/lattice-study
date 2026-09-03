# 次工程用プロンプト

以下を次の作業依頼としてそのまま使用してください。

---

このリポジトリの `README.md`、`docs/00-overview.md` から `docs/09-roadmap.md`、および現在の実装を確認し、次の最小実装に取り組んでください。

現在は次の縦切りまで完了しています。

- `mb-types`: `NodeId`、`LinkId`、`MonoTime`、I/O なしの `Component` 境界
- `mb-control`: 最小 LSA、LSDB、フラッディング、双方向アサーション、Dijkstra、`RouteTable`
- 3 ノードの決定論的インメモリテスト

次の目的は、**既存の `mb-control` の判断ロジックをネットワーク I/O と混ぜずに、実際の TCP 経路から駆動できることを証明する最小の縦切り**です。

まずドキュメントと依存関係を確認し、実装前に短く以下を示してください。

1. 今回含める最小スコープ
2. 今回含めないもの
3. 後続開発のブロッカーを作らないために守る境界
4. 完了条件

有力なスコープは以下です。ただし、一度に広すぎる場合は独立して検証可能な単位へ縮小してください。足場だけを作って終わらず、必ず動作する縦切りを完成させてください。

- 設計どおりの protobuf による最小 Control Frame 表現
- `mb-wire` の固定 8 byte フレームヘッダ、1 MiB 上限、incremental decode
- 静的ピアだけを扱う TLS なしの TCP adapter
- TCP の受信を `ControlEvent` へ変換し、`ControlAction::Send` を TCP へ戻す薄い runtime
- loopback 上で少なくとも 2 ノード、可能なら 3 ノードの LSA 収束を確認する統合テスト

必須の設計制約は次のとおりです。

- `mb-control` に `tokio`、socket、ファイル I/O、実時刻取得を入れない
- runtime は Event を投入して Action を実行するだけにする
- wire 上の未検証入力にサイズ上限を設け、panic させない
- 将来 TCP を QUIC に差し替えても control plane を変更しない
- TLS、QUIC、自動近隣探索、Admin API、Pub/Sub は今回の目的に必要でなければ実装しない
- 既存のユーザー変更を保持する

実装後は少なくとも以下を実行してください。

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
```

環境に Rust がなければ勝手に恒久インストールせず、一時ツールチェーンを使うか必要な許可を求めてください。最後に、変更内容、検証結果、意図的な未実装範囲、次に進むべき一手を簡潔に報告してください。作業が完了したら、内容に合ったメッセージでコミットし、現在のブランチを origin へ Push してください。
