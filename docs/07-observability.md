# 07. Observability — メトリクス・ログ・CLI・キャプチャ

## 1. なぜここに紙幅を割くか

この種のシステムは **障害が現場でしか再現しない**。「山の向こうの中継車両との間で、たまに tracks が 30 秒遅れる」を、SSH も満足に繋がらない環境で診断する必要がある。

設計原則：

1. **ノード単体で完結する診断ができる**。中央の監視サーバに依存しない
2. **メッシュ越しに他ノードの状態を取れる**（監視自体がメッシュを使う）
3. **メトリクスは常時計測、コストは無視できる程度**（ホットパスは atomic のインクリメントのみ）
4. **相関できる**。1 つのメッセージが「どのノードのどのキューで何ミリ秒待ったか」を追える

## 2. メトリクス (Prometheus)

`127.0.0.1:9600/metrics` で公開。メッシュ越しの収集は Admin API（§4）経由。

### 2.1 Transport (01)

```
mb_link_up{peer,kind}                       gauge   1/0
mb_link_rtt_seconds{peer}                   gauge
mb_link_loss_ratio{peer}                    gauge
mb_link_bw_estimate_bytes{peer}             gauge
mb_link_mtu_bytes{peer}                     gauge
mb_link_flaps_total{peer}                   counter
mb_link_bytes_total{peer,dir,chan}          counter
mb_link_handshake_failures_total{reason}    counter   # cert_expired, crl, mission_mismatch, ...
```

### 2.2 Control Plane (02)

```
mb_lsdb_entries                             gauge
mb_lsdb_expired_total                       counter
mb_lsa_received_total{result}               counter   # accepted, stale, bad_sig, rate_limited
mb_lsa_sent_total                           counter
mb_spf_runs_total                           counter
mb_spf_duration_seconds                     histogram
mb_routes_total                             gauge
mb_route_changes_total                      counter
mb_unreachable_nodes                        gauge     # LSDB にあるが経路がない
mb_digest_exchanges_total{peer}             counter
mb_convergence_seconds                      histogram # 変更検知 → 経路表安定
```

`mb_unreachable_nodes` が **最も有用な単一指標**。「LSDB には見えているのに経路がない」は分断や双方向条件の不成立を示す。

### 2.3 Data Plane (03)

```
mb_fwd_packets_total{dir,prio}              counter   # in, out, transit
mb_fwd_dropped_total{reason,prio}           counter   # no_route, ttl, queue_full, link_down
mb_queue_depth_packets{link,prio}           gauge
mb_queue_depth_bytes{link,prio}             gauge
mb_queue_wait_seconds{prio}                 histogram # enqueue → send
mb_conflated_total{topic}                   counter
mb_compression_ratio{topic}                 summary
```

`mb_queue_wait_seconds{prio="P0"}` の p99 が SLO の中心。

### 2.4 Pub/Sub (04)

```
mb_pubsub_published_total{topic}            counter
mb_pubsub_delivered_total{topic,source}     counter   # source=live|backfill
mb_pubsub_dedup_dropped_total{topic}        counter
mb_pubsub_decrypt_failures_total{topic,reason} counter
mb_pubsub_gap_bytes{topic,origin}           gauge     # 検出済み未取得量
mb_backfill_inflight                        gauge
mb_backfill_bytes_total{direction}          counter
mb_store_bytes{topic}                       gauge
mb_store_pruned_bytes_total{topic}          counter
mb_e2e_latency_seconds{topic}               histogram # 発行 ts → 配信（時刻同期がある場合のみ有効）
```

### 2.5 RPC (05)

```
mb_rpc_circuits_open                        gauge
mb_rpc_circuits_total{service,status}       counter
mb_rpc_duration_seconds{service,method}     histogram
mb_rpc_resolve_failures_total{service,reason} counter
mb_rpc_window_stalls_total{service}         counter   # フロー制御で止まった回数
```

## 3. ログ

`tracing` + `tracing-subscriber`。

- 既定 `info`。`mbtool log-level <target> <level>` で実行時に変更（再起動なし）
- 出力：stderr（journald 前提）+ ローカルファイルのリングバッファ（100 MB）
- **構造化 JSON をファイルへ、人間可読を stderr へ**の 2 系統
- 高頻度イベント（パケット単位）は `trace` レベル + サンプリング（1/1000）

主要な span：

```text
link{peer=…}            → handshake, up, metrics 変化, down
spf{version=…}          → 所要時間、変化した経路数
circuit{id=…,service=…} → open, accept, first_byte, close
backfill{topic=…,origin=…} → request, chunk, complete, gap
```

### 3.1 相関 ID

Pub/Sub のメッセージには `(origin, topic, seq)` があるので、これがそのまま相関 ID になる。
RPC は `circuit_id` + 発信 NodeId。

分散トレーシング（OpenTelemetry）は **MVP では入れない**。理由：帯域を食う、収集器が DDIL で届かない。代わりに **メッセージの経路記録を任意で有効化**する（§5）。

## 4. `mbtool` — 現場診断 CLI

Admin API（unix socket の gRPC）のクライアント。`--node <NodeId>` を付けると **メッシュ越しに他ノードへ問い合わせる**（Admin API 自体が 05 の RPC で運ばれる）。

```
mbtool status
  Node      drone-17 (b7e1a2…)
  Uptime    4h12m   Cert expires in 5d3h
  Links     3 up / 1 down
  LSDB      47 entries (2 expired)   Routes 45   Unreachable 2
  Queues    P0 0  P1 12  P2 340  P3 4096(full)
  Stores    tracks 84MB / c2 2MB

mbtool peers
  PEER        KIND    STATE  RTT     LOSS   BW        MTU   COST  UP
  vehicle-3   radio   up     182ms   4.2%   340kbps   1200  71    4h10m
  drone-22    radio   up     95ms    18.1%  180kbps   1200  312   22m
  base-1      satcom  up     620ms   0.3%   1.2Mbps   1400  263   4h11m
  drone-09    radio   down   -       -      -         -     -     (5m ago, 7 flaps/1h)

mbtool routes [--dst <node>]
  DST         COST  HOPS  NEXT-HOP     ALT
  base-1      263   1     base-1       -
  c2-main     271   2     base-1       -
  drone-22    312   1     drone-22     vehicle-3(340)
  drone-09    -     -     UNREACHABLE  (LSDB entry age 5m)

mbtool path <dst>              # 経路を LSDB から再構成して表示
  drone-17 --71--> vehicle-3 --58--> ground-2 --142--> c2-main   total 271, 3 hops

mbtool lsdb [--origin <node>] [--verbose]
  ORIGIN      SEQ     EPOCH  AGE   ADJ  SVCS  TOPICS  SIG
  vehicle-3   10482   3      12s   4    2     18      ok
  drone-22    2201    1      3s    2    1     4       ok
  ghost-1     991     1      281s  1    0     0       ok  (EXPIRING)

mbtool topics
  TOPIC                    MODE    PRIO  PUB  SUB  STORE  RATE      GAP
  lattice.tracks.v1        latest  P1    12   3    -      420/s     0
  lattice.c2.commands.v1   log     P0    1    1    yes    0.2/s     14 msgs

mbtool subscriptions
  SUB  TOPIC                  START     CURSOR-LAG  BACKFILL
  1    lattice.tracks.v1      latest    0           -
  2    lattice.c2.commands.v1 cursor    14          in-progress 8KB/64KB

mbtool backfill               # 進行中の Backfill 一覧と進捗
mbtool services               # LSDB から見えるサービス一覧と提供ノード
mbtool circuits               # 進行中の RPC

mbtool watch <topic> [--decode <proto>]     # Topic を tail する（購読権限が要る）
mbtool publish <topic> --file msg.bin       # テスト用の発行

mbtool trace <dst> [--prio P1]              # §5 の経路トレース
mbtool capture --out mesh.pcap [--filter …] # §6
mbtool sim-export --out state.json          # 現在の LSDB を シミュレータ用に書き出す
```

**`mbtool sim-export` が現場対応で効く**：不具合が起きた実機の LSDB とリンク品質をそのままエクスポートし、手元のシミュレータ（08）で再現する。

## 5. 経路トレース

`traceroute` に相当。デバッグ用に **明示的に有効化したときだけ**動く（常時は帯域の無駄）。

```protobuf
message TracePacket {
  bytes  dst = 1;
  uint64 trace_id = 2;
  uint32 prio = 3;
  repeated Hop hops = 4;   // 各中継ノードが append
}
message Hop {
  bytes node = 1;
  uint64 recv_ts_us = 2;      // ローカル時刻（相対的な比較に使う）
  uint64 send_ts_us = 3;
  uint32 queue_wait_us = 4;   // ★ どのキューで何 µs 待ったか
  uint32 queue_depth = 5;
  uint32 link_rtt_ms = 6;
}
```

`queue_wait_us` が入るのが普通の traceroute との差。「遅い」原因が伝搬遅延なのかキューイングなのかを一発で切り分けられる。

```
mbtool trace c2-main --prio P3
  hop  node       queue_wait  depth  link_rtt  cumulative
  0    drone-17   2ms         12     -         2ms
  1    vehicle-3  8420ms      4096   182ms     8604ms      ← ここが犯人
  2    ground-2   3ms         8      58ms      8665ms
  3    c2-main    -           -      142ms     8807ms
```

トレースパケットは **元のパケットと同じ QoS クラスで送る**（P0 で送ると混雑を観測できない）。

## 6. パケットキャプチャ

`tcpdump` / Wireshark は **QUIC/TLS で暗号化されているため中身が見えない**。よってアプリケーション層のキャプチャを自前で持つ。

```
mbtool capture --out mesh.pcap --filter 'topic=lattice.tracks.v1 or prio=P0' --limit 100MB
```

- 出力は **pcapng の Custom Block** に mesh-bus フレームを格納。Wireshark の Lua ディセクタを同梱してヘッダをデコード表示できるようにする
- ペイロードは既定で **記録しない**（暗号文を保存しても意味がなく、鍵があれば漏洩源になる）。`--include-payload` は明示指定 + ログに記録
- リングバッファモード：常時 100 MB を保持し、`mbtool capture --dump` で直近を吸い出す。**障害の後から遡れる**のが現場で効く

TLS の鍵をエクスポートして Wireshark で QUIC を復号する経路（`SSLKEYLOGFILE`）も開発ビルドでのみ提供する。

## 7. ヘルスチェックとアラート

`mbd` が自己診断し、深刻度付きで公開する。

```
mbtool health
  [WARN]  cert expires in 5d3h                 → 更新できていない
  [WARN]  drone-09 unreachable for 5m
  [ERROR] P3 queue full on vehicle-3 for 12m   → Backfill が進んでいない
  [INFO]  2 LSDB entries expiring
```

Prometheus 側のアラートルール例：

```yaml
- alert: MeshPartition
  expr: mb_unreachable_nodes > 0
  for: 2m
- alert: P0Starvation
  expr: histogram_quantile(0.99, mb_queue_wait_seconds{prio="P0"}) > 1
  for: 1m
- alert: CertExpiringSoon
  expr: mb_cert_expiry_seconds < 86400
- alert: RouteFlapping
  expr: rate(mb_route_changes_total[5m]) > 1
  for: 5m
- alert: BackfillStalled
  expr: mb_pubsub_gap_bytes > 0 and rate(mb_backfill_bytes_total[10m]) == 0
  for: 10m
```

## 8. Grafana ダッシュボード構成

| パネル | 内容 |
|---|---|
| メッシュトポロジ | ノードとリンクをグラフ表示、cost で太さ、loss で色（Node Graph パネル） |
| リンク品質 | RTT / loss / bw の時系列、ピアごと |
| 収束 | route_changes、spf_duration、unreachable_nodes |
| QoS | キュー深さと待ち時間を優先度別に積み上げ |
| Pub/Sub | Topic ごとの publish/deliver レート、gap、backfill 進捗 |
| RPC | サービスごとのレイテンシ p50/p99、エラー率 |
| セキュリティ | handshake 失敗、復号失敗、cert 期限 |

## 9. 実装方針

- メトリクスは `metrics` crate + `metrics-exporter-prometheus`。ホットパスは `counter!().increment()`（atomic 1 命令）
- ラベルの基数に注意：`peer` は最大数十、`topic` は数十。**`origin` をラベルにしない**（300 ノード × Topic 数で爆発する）。origin 別は Admin API で取る
- Admin API は 05 の RPC で運ばれるので、**メッシュが壊れると他ノードの情報が取れない**。これは受け入れる（壊れたときに使うのはローカルの `mbtool` と `capture`）
