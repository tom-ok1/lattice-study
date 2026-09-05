# 09. 実装ロードマップ

「学習目的で 3〜5 ノードの動くものを作る」から「本番相当」までを段階に分ける。
各段階は **動いて、テストがあって、次に進める** 状態で終わる。

## Phase 0 — 骨組み（1〜2 週）

**目標**：2 ノードが TCP で繋がり、互いの存在を認識してハートビートを交換する。

- [ ] Cargo workspace、`mb-types`（NodeId、TopicKey、エラー）
- [x] `mb-wire`：8 バイトフレームヘッダのコーデック + ラウンドトリップテスト
- [x] `mb-transport`：TCP のみ、TLS なし、静的ピア設定のみ
- [ ] `mbd` バイナリ：設定ロード、Link 確立、`LinkHello` 交換、ログ
- [ ] `Clock` / `Rng` trait と本番実装
- [ ] `mbtool status` / `mbtool peers`

**判定**：2 プロセスを起動して `mbtool peers` に相手が `up` で出る。

## Phase 1 — 制御プレーン（2〜3 週）★ 最も学びが大きい

**目標**：5 ノードのチェーン／メッシュで LSA が伝播し、経路表が収束する。リンクを切ると再収束する。

- [x] `mb-control`：LSA 構造、LSDB、フラッディング、重複排除
- [x] 双方向アサーションによるグラフ構築
- [x] Dijkstra + 経路表
- [ ] SPF hold timer
- [x] Anti-entropy digest 交換
- [x] LSA の失効（MaxAge）と再発行、seq 永続化
- [ ] `mbtool routes` / `mbtool lsdb` / `mbtool path`
- [ ] メトリクス：`mb_lsdb_entries`, `mb_routes_total`, `mb_unreachable_nodes`, `mb_spf_duration_seconds`

**この段階で `mb-control` は I/O を持たない**こと。ここを守らないと Phase 3 が破綻する。

**判定**：
```text
A ─ B ─ C ─ D ─ E   で A から E への経路が 4 hop
B-C を切断 → 全ノードで E が unreachable、5 秒以内に収束
A-D の直接リンクを追加 → A→E が 2 hop に変わる
```

**学習の核心**：ここで OSPF の link-state の全要素（LSA、LSDB、SPF、収束、フラップ）を自分の手で実装することになる。

## Phase 2 — データプレーン（1〜2 週）

**目標**：多段ホップで任意ノードにパケットが届く。優先度が守られる。

- [ ] `mb-forward`：80(88) バイトヘッダ、転送ループ、TTL、ループ検知
- [ ] 優先度キュー（P0 厳密 + P1〜P3 DRR）
- [ ] Conflation
- [ ] Explicit Multicast の fan-out
- [ ] Link Down 時のキュー処理
- [ ] ECMP と flow_id ハッシュ
- [ ] `mbtool trace`（経路トレース、キュー待ち時間つき）
- [ ] メトリクス：`mb_queue_depth_*`, `mb_queue_wait_seconds`, `mb_fwd_dropped_total`

**判定**：帯域を `tc netem` で 100 kbps に絞り、P3 を飽和させた状態で P0 の p99 遅延が RTT + 100 ms 以内。

## Phase 3 — シミュレータ（2 週）★ ここへの投資が後を決める

**目標**：Phase 1〜2 のコードを 50 ノードで、シードから完全再現できる形で回せる。

- [ ] `mb-sim`：VirtualClock、離散イベントキュー、VirtualNetwork
- [ ] LinkModel（遅延分布、損失、帯域トークンバケット、並び替え）
- [ ] シナリオ DSL（ron）と代表シナリオ 10 本
- [ ] 不変条件チェッカ I1〜I4, I7, I9
- [ ] `mbsim run` / `mbsim replay --step`
- [ ] CI に `sim-quick` を追加
- [ ] clippy の `disallowed-methods` で決定論を強制

**判定**：同じシードで 3 回走らせて、イベント列が完全一致する。50 ノードのランダム churn シナリオで I1〜I4 が破れない。

> Phase 3 を Phase 4 以降より先に置くのが重要。Pub/Sub や RPC を作ってからシミュレータを後付けすると、I/O が混ざったコードを引き剥がす大工事になる。

## Phase 4 — Pub/Sub 基本（2 週）

**目標**：Topic への publish が、多段ホップ越しに購読者へ届く。

- [ ] `mb-pubsub`：Envelope、Topic ポリシー、購読管理
- [ ] Explicit Multicast による配信（Phase 2 と接続）
- [ ] dedup（last_seq + 64 bit ビットマップ）
- [ ] アプリ向け gRPC API（Publish / Subscribe）
- [ ] `mbtool topics` / `mbtool watch` / `mbtool publish`
- [ ] シミュレーションの不変条件 I5 を追加

**判定**：5 ノードチェーンの端から端へ 10 Hz の publish が届く。中継ノードは購読していない。

## Phase 5 — Store-and-Forward と Backfill（2〜3 週）★ DDIL の核心

**目標**：分断中のメッセージが復旧後に届く。ライブを阻害しない。

- [ ] `mb-store`：Storage trait、in-memory 実装、`redb` 実装、prune
- [ ] `store_role` の実装（publisher / subscriber / designated / none）
- [ ] LSA の `TopicAd`（head_seq / tail_seq）でギャップ検出
- [ ] BackfillRequest / Chunk / Gap、window credit
- [ ] `interleaved` / `ordered` の合流モード
- [ ] cursor API
- [ ] `mbtool backfill` / `mbtool subscriptions`
- [ ] 不変条件 I6、シナリオ「分断 → 1000 publish → 結合」

**判定**：
```text
2 分割して片側で 1000 件 publish → 結合 → 60 秒以内に全件が届く
Backfill 進行中の live メッセージ p99 遅延が、Backfill なし時の 1.5 倍以内
prune 済み範囲が Gap として返り、購読者が無限待ちしない
```

## Phase 6 — セキュリティ（3 週）

**目標**：mTLS でメッシュが張られ、LSA が署名検証され、Topic が E2E 暗号化される。

- [ ] `mb-crypto`：NodeId 導出、Signer/Verifier、ファイル鍵実装
- [ ] X.509 検証（カスタム拡張、mission_id、NodeId 一致）
- [ ] `rustls` による mTLS（TCP に適用）
- [ ] LSA 署名と検証（Phase 1 に後付け）
- [ ] Topic 群鍵：決定論的導出、epoch、HPKE 配布
- [ ] AEAD 暗号化（nonce を型で保護）
- [ ] 発行者署名（P0 Topic）
- [ ] CRL Gossip
- [ ] 認可（topic_acl、rpc_acl、store_only ロール）
- [ ] `mb-sim` の Byzantine ノード、不変条件 I10・I12
- [ ] `cargo-fuzz` を全パーサに適用

**判定**：Byzantine シナリオ（FalseAdjacency、ForgeLsa、CorruptForward）が全て期待どおり無害化される。中継ノードのストアが暗号文であることを確認。

## Phase 7 — QUIC への移行（1〜2 週）

**目標**：TCP から QUIC に切り替え、HoL ブロッキングと再接続コストを改善する。

- [ ] `quinn` による Link 実装
- [ ] チャネル = 長寿命 bidi ストリーム
- [ ] DATAGRAM による P1 送信
- [ ] PMTUD、BBR、`initial_rtt` のリンク種別調整
- [ ] TCP をフォールバックとして維持、選択ロジック
- [ ] Connection Migration の動作確認

**判定**：同一シナリオで TCP 版と QUIC 版を比較し、損失 20% 環境で P1 の遅延が改善する。

## Phase 8 — RPC プロキシ（2〜3 週）

**目標**：無改造の tonic クライアント／サーバがメッシュ越しに通信する。

- [ ] `ServiceRegistry` とサービス登録の LSA 反映
- [ ] Circuit 状態機械（I/O なし）
- [ ] ingress：`h2` による HTTP/2 終端、ストリーム → Circuit
- [ ] egress：接続プール、ローカルサーバへの転送
- [ ] resolver：LSDB + cost によるノード選択、sticky
- [ ] フロー制御（CircuitWindow）
- [ ] デッドライン・キャンセル伝播
- [ ] 認可
- [ ] `mbtool services` / `mbtool circuits`
- [ ] 不変条件 I11、netns 統合テスト

**判定**：既存の gRPC サンプル（bidi streaming 含む）を 3 ホップ越しに改造なしで動かす。

## Phase 9 — 近隣探索と運用（2 週）

**目標**：設定なしで同一セグメントのノードが自動的に繋がる。現場診断が揃う。

- [ ] UDP マルチキャスト Hello、レート制限、ジッタ
- [ ] Gossip 由来アドレスへの機会的接続
- [ ] グレア解決
- [ ] リンク damping
- [ ] `mbtool capture`（pcapng + Wireshark ディセクタ）
- [ ] `mbtool health`、Prometheus アラートルール
- [ ] Grafana ダッシュボード
- [ ] `mbtool sim-export` / `mbsim` の `FromExport`
- [ ] 証明書の自動更新

**判定**：3 台の実機を同一 LAN に置くだけでメッシュが形成される。`mbtool sim-export` した状態がシミュレータで再生できる。

## Phase 10 — 堅牢化（継続）

- [ ] swarm testing の nightly 5000 ケース
- [ ] 性能ベンチマークの回帰検出
- [ ] FIPS 経路（`aws-lc-rs`）、HSM/PKCS#11
- [ ] メモリ・CPU のプロファイリングと最適化
- [ ] 実機フィールド試験（実無線、移動体）
- [ ] 階層化（エリア分割）の検討 — 300 ノードを超える場合
- [ ] 輻輳通知（03 §6.3）
- [ ] トラフィック解析対策（パディング、カバートラフィック）

---

## 学習用の最小構成（週末プロジェクト規模）

上記が長すぎる場合、**Phase 0 → 1 → 2 → 4 の縮小版**が最も学習効率が高い。

```text
1. TCP で 3 ノードを繋ぐ                         (半日)
2. JSON で LSA を交換、LSDB を作る                (1 日)
3. Dijkstra で経路表を計算                        (半日)
4. パケット転送（TTL つき）                       (半日)
5. 単純な Pub/Sub（暗号なし、Backfill なし）      (1 日)
6. リンクを切って再収束を観察                     (半日)
7. 切断中のメッセージをメモリに溜めて再送          (1 日)
```

**7 まで作ると、DDIL 環境の分散メッセージングの本質的な難しさが全部出てくる**：

- 誰が溜めるのか（発行元か、中継か）
- どこまで溜めるのか（メモリは有限）
- 再接続をどう検知するのか
- 何を再送すべきか（購読者が何を持っているか送信側は知らない）
- 再送がライブを詰まらせないか

この 5 つの問いに自分の答えを出せれば、04 と 03 のドキュメントに書いたことが「なぜそう設計したか」として腑に落ちる。

---

## 設計上の未解決事項（意図的に先送りしたもの）

| 項目 | 現状の判断 | 再検討の契機 |
|---|---|---|
| 300 ノード超のスケール | 単一エリアのまま。LSA 帯域が O(N²) で効いてくる | ノード数が 200 を超えたら階層化を設計 |
| Topic 広告の LSA サイズ | ノードごとに全 Topic を載せている | Topic 数が 100 を超えたら Topic 別 LSA に分離 |
| RPC の E2E 暗号 | ホップ間 mTLS のみ。中継から守れない | 高機密 RPC の要求が出たらアプリ層暗号 + 鍵配布 API |
| 端-端輻輳制御 | 実装しない（クラス別の代替で対応） | 中継のドロップが実運用で問題化したら |
| トラフィック解析対策 | なし | 電波傍受による作戦推定が脅威として上がったら |
| 強一貫性 | 提供しない | 分散ロックや合意が必要なアプリが出てきたら別レイヤーで |
| 非対称リンク | 双方向条件により表現不可 | 放送型リンクの要求が出たら、署名付き「受信専用宣言」を検討 |
