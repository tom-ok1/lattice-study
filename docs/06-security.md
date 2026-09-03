# 06. Security — 識別・認証・E2E 暗号・鍵配布・認可

## 1. 脅威モデル（詳細）

前提：**ノードは物理的に奪取され得る。ネットワークは完全に敵対的。**

| # | 脅威 | 想定される攻撃者 | 影響 |
|---|---|---|---|
| T1 | パッシブ盗聴 | 電波傍受 | 位置・作戦情報の漏洩 |
| T2 | アクティブ改竄・注入 | MITM | 偽トラック、偽コマンド |
| T3 | 再生攻撃 | 記録・再送 | 古いコマンドの再実行 |
| T4 | 偽経路広告（ブラックホール／シンクホール） | 侵害ノード | トラフィックの吸い込み・遮断 |
| T5 | 不正参加 | 拾得デバイス | メッシュ全体の閲覧 |
| T6 | 奪取ノードによる継続的な閲覧 | 敵に鹵獲された機体 | 全 Topic の平文閲覧 |
| T7 | DoS（計算・帯域・メモリ） | 任意 | 可用性喪失 |
| T8 | トラフィック解析 | 電波傍受 | 通信量から作戦意図の推定 |

対応の全体像：

```text
T1  ホップ間 TLS 1.3 + Topic E2E AEAD
T2  AEAD タグ + 発行者署名（重要 Topic）+ LSA 署名
T3  (origin, seq) 単調性 + 時刻許容窓 + 群鍵の epoch
T4  署名付き LSA + 双方向アサーション（02 §3）
T5  ミッション CA の証明書必須。NodeId = pubkey ハッシュ
T6  短命証明書 + CRL Gossip + 群鍵ローテーション + 前方秘匿
T7  未認証トラフィックの早期破棄、レート制限、LSDB サイズ上限
T8  非目的（MVP）。将来: パディング + カバートラフィック
```

## 2. 識別 (Identity)

### 2.1 鍵と NodeId

```text
ノード鍵ペア:  ECDSA P-256（FIPS 経路）または Ed25519（非 FIPS で高速）
NodeId      :  SHA-256(SubjectPublicKeyInfo DER) の 32 bytes
```

**NodeId を公開鍵から導出する**ことで：

- 証明書チェーンの検証と、LSA 署名の検証が同じ鍵に落ちる
- 「NodeId B を名乗るが鍵が違う」が構造的に不可能
- 証明書が失効しても NodeId は変わらない（再発行で同じ ID を保てる）

秘密鍵の保管：

| 環境 | 保管 |
|---|---|
| 本番（機体） | TPM 2.0 / セキュアエレメント。署名は HSM 内で行い鍵を出さない |
| 本番（サーバ） | PKCS#11 経由 HSM、なければ暗号化ファイル + OS 権限 |
| 開発・シミュレーション | ファイル（明示的に `--insecure-key-file` が必要） |

抽象：

```rust
pub trait Signer: Send + Sync {
    fn node_id(&self) -> NodeId;
    fn sign(&self, msg: &[u8]) -> Result<Signature>;   // HSM 実装では内部で完結
    fn algorithm(&self) -> SigAlg;
}
```

### 2.2 証明書

ミッションごとの中間 CA が発行する X.509。

```text
Root CA (offline, HSM)
  └─ Mission CA (作戦単位、有効期間 30 日)
       └─ Node cert (有効期間 7 日)
            Subject:  CN=drone-17
            SAN:      URI:meshbus://<NodeId hex>
            Extension (custom OID):
                roles      = ["sensor", "relay"]
                mission_id = "OP-2026-041"
                topics_pub = ["lattice.tracks.v1"]      # 発行を許可される Topic
                topics_sub = ["lattice.c2.commands.v1"] # 購読を許可される Topic
```

- **有効期間 7 日**：DDIL で 24 時間更新は非現実的（数日繋がらないことがある）。短命性は CRL Gossip で補う
- **ロールを証明書に入れる**：アプリが名乗るのではなく、配備時に決まる。奪取ノードがロールを詐称できない
- **Topic 許可を証明書に入れる**：発行元が発行権限を持つかを受信側が検証できる

### 2.3 失効

3 段構え：

1. **短い有効期間**（7 日）— 最終的な保険
2. **CRL Gossip** — 失効した NodeId のリストを署名付きで Gossip（LSA と同じ経路）。`RevocationList { revoked: [NodeId], issued_at, seq, ca_signature }`。サイズは小さいので全ノードが保持
3. **群鍵ローテーション** — 失効を検知したら Topic 群鍵を即座にローテーション（§4.3）。これが**実効的な締め出し**。証明書が有効に見えても新しい鍵をもらえない

CRL は **CA が署名**する。CA と繋がっていない分断側では新しい CRL が届かないが、群鍵ローテーションは分断側の Topic 権威ノードが実行できる（§4.3）。

## 3. ホップ間セキュリティ (mTLS)

01 の Link は必ず mTLS で確立する。

- **TLS 1.3 のみ**。TLS 1.2 以下は拒否
- 暗号スイート：`TLS_AES_256_GCM_SHA384`, `TLS_CHACHA20_POLY1305_SHA256`（FIPS モードでは前者のみ）
- 鍵交換：`X25519` / `secp256r1`（FIPS では後者）
- **相互認証必須**。クライアント証明書なしは即切断
- ピア検証：
  1. Mission CA までのチェーン検証
  2. `mission_id` が自分と一致
  3. CRL に載っていない
  4. SAN の NodeId と、鍵から導出した NodeId が一致
  5. 有効期限（クロックスキュー ±5 分を許容）

**クロックが信用できない場合**：GPS 同期がない機体では証明書の有効期限判定が困難。対策として「起動時に隣接ノードから時刻のヒントを得る（署名付き）」+「有効期限判定に ±12 時間の猶予」を持たせる設定を用意する。安全性は下がるがトレードオフとして明示する。

証明書更新（DDIL 対応）：

```text
残り有効期間 < 50% になったら更新を試みる
  → メッシュ越しに CA サービス（RPC）へ CSR
  → 届かなければ 1 時間ごとに再試行
  → 期限切れ 24h 前に警告メトリクス発火
  → 期限切れ → 新規リンク確立不可、既存リンクは維持（切り離すと孤立して復旧不能になるため）
```

**期限切れ後も既存リンクを維持する**のは意図的な判断。DDIL では「厳密に切る」ことが可用性を殺す。切るかどうかは設定 `strict_expiry = false`（既定）で選べる。

## 4. E2E 暗号 (Pub/Sub)

### 4.1 なぜホップ間だけでは不十分か

```text
Publisher ──TLS──▶ Relay X ──TLS──▶ Subscriber
                     ▲
                  ここで平文
```

Relay X が奪取されていたら全部読める。中継ノードは「転送はできるが読めない」状態にしたい。

```text
read     ×
modify   ×  (AEAD タグで検知)
forge    ×  (鍵を持たない)
forward  ○
```

### 4.2 方式：Topic 群鍵 (Group Key)

Topic ごとに対称鍵を持ち、購読を許可されたノードだけに配る。

```text
TopicKey(topic, epoch) = 32 bytes AES-256 鍵

暗号化: AES-256-GCM
  nonce = origin[0..4] ‖ seq (u64 BE) ‖ counter(u32)   ※ 12 bytes
  AAD   = Envelope のヘッダ部（origin, topic, partition, seq, ts, key_epoch）
  ct    = AEAD(key[epoch], nonce, zstd(payload), AAD)
```

**nonce の一意性**：`(origin, seq)` が一意なので nonce も一意。同じ鍵で同じ nonce を二度使わない（GCM の致命的失敗を回避）。ノード再起動後も seq が単調増加するので安全（02 §2.2 の seq 永続化がここでも効く）。

なぜ公開鍵ベース（各購読者宛に暗号化）でないか：購読者が 50 いると 1 メッセージあたり 50 回の暗号化と 50 個のヘッダが必要で、細い帯域では成立しない。群鍵なら 1 回。

### 4.3 群鍵の配布とローテーション

各 Topic に **鍵権威 (key authority)** ノードを設定で定める（通常は C2 ノード。冗長化のため 2〜3 ノード）。

```text
配布:
  購読者 S が鍵権威 A に KeyRequest{topic, epoch} を RPC（05 経由、P0）
  A は S の証明書の topics_sub を検証
  A は HPKE (RFC 9180, DHKEM-P256 + HKDF-SHA256 + AES-256-GCM) で S の公開鍵宛に鍵を封緘
  → S だけが開ける。中継は読めない

ローテーション（新 epoch の発行）:
  トリガ: (a) 24 時間経過  (b) メンバー離脱・失効検知  (c) 手動
  A が新 epoch の鍵を生成し、現メンバー全員へ HPKE で配布
  発行者は配布完了後（または猶予 60 s 後）に新 epoch で暗号化を開始
  受信者は旧 epoch の鍵を retention 期間ぶん保持（Backfill の復号に必要）
```

**分断への対応**：鍵権威と切れている購読者は新しい鍵をもらえず、新 epoch のメッセージを復号できない。対策：

- 鍵権威を 2〜3 ノードに冗長化し、鍵生成は **決定論的導出**にする：`key[epoch] = HKDF(mission_master_secret, topic ‖ epoch)`。どの権威も同じ鍵を導出できる
- `mission_master_secret` は配備時にプロビジョニングされ、権威ノードのみが持つ
- ただし決定論的導出では「メンバー離脱時のローテーション」で離脱者が epoch+1 を計算できてしまう。よって **離脱時のローテーションは新しいランダム secret への切り替え**を伴う（`mission_master_secret` の世代管理）

この二段構え（通常は決定論的導出で分断耐性、失効時はランダム切替で前方秘匿）が設計上の要。

### 4.4 発行者署名

`signed = true` の Topic では、暗号化に加えて **origin の秘密鍵で署名**する。

```text
signature = Sign(node_privkey, header_bytes ‖ ciphertext)
```

AEAD だけでは「群鍵を持つ誰でも偽造できる」（購読者が発行者になりすませる）。C2 コマンドなど、発行元の真正性が必要な Topic では署名を必須にする。

コスト：ECDSA P-256 の署名が ~50 µs、検証が ~150 µs。1000 msg/s なら CPU の 15%。**P0 Topic のみに限定**する。

受信側は `topics_pub` を証明書で検証：origin がその Topic の発行を許可されているか。

## 5. 認可 (Authorization)

3 つのレイヤーで判定する。

| レイヤー | 判定内容 | 権威 |
|---|---|---|
| リンク | このノードはメッシュに参加してよいか | mTLS + CRL |
| Topic | このノードは publish / subscribe してよいか | 証明書の `topics_pub` / `topics_sub` + 鍵権威の鍵配布 |
| RPC | このノードはこのサービス／メソッドを呼んでよいか | 証明書の `roles` + サーバ側ポリシー |

ポリシーファイル（全ノードに配布、`policy_hash` を LSA で公開して不一致を検出）：

```toml
[[topic_acl]]
topic = "lattice.c2.commands.v1"
publish_roles   = ["c2-operator"]
subscribe_roles = ["effector", "c2-operator", "relay-store"]

[[rpc_acl]]
service = "lattice.c2.v1.TaskingService"
method  = "*"
allow_roles = ["c2-operator"]
```

`relay-store` ロールに注意：`designated` ストア（04 §6.1）は Topic を保存するが、**平文を読む必要はない**。暗号文のまま保存できる。よって **ストアノードに群鍵を配らない**のが正しい設計。上記の例では `relay-store` を subscribe に入れているが、実際には「暗号文の保存だけを許可する」別ロール `store_only` を用意する：

```toml
[[topic_acl]]
topic = "lattice.c2.commands.v1"
publish_roles   = ["c2-operator"]
subscribe_roles = ["effector", "c2-operator"]     # 群鍵を受け取る
store_roles     = ["relay"]                        # 暗号文のまま保存・転送のみ
```

これで **中継・保存ノードが奪取されても Topic の内容は漏れない**。DDIL の Store-and-Forward と Zero Trust を両立させる要点。

## 6. DoS 対策

| 攻撃 | 対策 |
|---|---|
| 大量の TLS ハンドシェイク | 未確立接続数の上限（32）、送信元 IP ごとのレート制限（5/s）、QUIC Retry (address validation) を有効化 |
| 大量の LSA 注入 | 署名検証前に `(origin, seq)` で LSDB と比較して古ければ破棄（検証コストを払わない）。origin ごとの LSA 受理レート上限（1/s） |
| 巨大フレーム | 1 MiB 上限、超過は接続切断 |
| LSDB メモリ枯渇 | エントリ上限 1000、Mission CA 未検証の origin は受け入れない |
| Backfill 要求の乱発 | ノードあたり同時 4、応答レート上限 |
| 復号失敗の連打（CPU） | ピアごとに復号失敗カウント、閾値超過でそのピアからの当該 Topic を一時遮断 |

**署名検証の順序が重要**：安いチェック（seq 比較、サイズ、レート）を先に、高いチェック（署名検証）を後に。

## 7. 実装方針

```text
mb-crypto/
├── identity.rs     NodeId 導出、Signer/Verifier trait、HSM/PKCS#11 実装
├── cert.rs         X.509 検証、カスタム拡張のパース、CRL
├── tls.rs          rustls Config 構築（サーバ/クライアント）、検証コールバック
├── aead.rs         AES-256-GCM ラッパ、nonce 構築、誤用防止 API
├── hpke.rs         RFC 9180 実装（hpke crate）による鍵封緘
├── group_key.rs    epoch 管理、決定論的導出、キャッシュ、旧 epoch 保持
└── fips.rs         aws-lc-rs バックエンド切替（feature flag）
```

- 暗号ライブラリは **`aws-lc-rs`**（FIPS 140-3 検証済みモジュールを持つ）を既定にし、`ring` を非 FIPS 向け feature に置く
- **nonce の誤用を型で防ぐ**：`Nonce` は `(NodeId, seq)` からしか構築できない API にする。任意バイト列からの構築は `unsafe` 相当の明示的関数のみ
- 鍵はメモリ上で `zeroize` する
- 証明書検証は rustls の `ServerCertVerifier` / `ClientCertVerifier` をカスタム実装（NodeId 一致、mission_id、CRL の追加チェック）

## 8. 検証すべき性質

| 性質 | 検証 |
|---|---|
| 中継ノードが平文を得られない | 中継ノードのメモリダンプ／ログに平文が現れないことをテストで確認。ストアの中身が暗号文であること |
| 偽 LSA が経路に影響しない | 悪意ノードのシナリオ（02 §10） |
| nonce 再利用が起きない | seq 永続化を壊した状態（ストレージ削除）で epoch が上がり、鍵も変わることを確認 |
| 失効ノードが締め出される | CRL Gossip → 新規リンク拒否、群鍵ローテーション後に復号不能 |
| ポリシー違反の publish が拒否される | `topics_pub` にない Topic への publish が受信側で破棄される |
| クロックスキューでの誤切断がない | ノード間で ±10 分ずらしてリンクが張れること（猶予設定時） |
| ファジング | LSA / Envelope / Circuit フレームのパーサに `cargo-fuzz` を適用 |
