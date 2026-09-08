# Transparent L4 Overlay — mesh-unaware なアプリケーション通信

## 1. この文書の位置づけ

このプロジェクトは Anduril Lattice の公開 SDK、開発者資料、特許、求人情報から着想を得ているが、
Lattice の内部実装を再現するものではない。

公開 Lattice SDK から確認できるのは、アプリケーションが環境 endpoint に対して REST / gRPC API を利用する
外部インターフェースまでである。内部 mesh が TCP、Circuit、Pub/Sub をどのように組み合わせているかは
公開情報から確認できない。

設計を検討する中で、mesh-bus のより一般的な到達目標を次のように定めた。

> アプリケーションは mesh 固有 SDK、NodeId、next-hop を知らず、通常の service hostname と port を使って
> HTTP / gRPC / TCP 通信を行う。ローカルの代理プロセスが通信を透過的に捕捉し、複数 hop の mesh 上へ運ぶ。

これは Lattice の実装に関する主張ではなく、複数の OSS で実証されたプラクティスを、LSA ベースの自律分散制御、
DDIL 向け QoS、Native Pub/Sub と組み合わせる独自の設計方向である。

## 2. 目標アーキテクチャ

```text
Client App              mbd(A)                 mbd(B)              mbd(C)            Server App
    │                      │                      │                    │                    │
    │ TCP chat.mesh:443    │                      │                    │                    │
    ├─────────────────────▶│                      │                    │                    │
    │ normal HTTP/gRPC/TCP │ DNS/VIP → service   │                    │                    │
    │                      │ service → Node C    │                    │                    │
    │                      │ CircuitOpen/Data    │                    │                    │
    │                      ├─────────────────────▶├───────────────────▶│                    │
    │                      │   dst=Node C        │ route lookup only  │ dial allowlisted   │
    │                      │                      │                    ├───────────────────▶│
    │◀─────────────────────┴──────────────────────┴────────────────────┴────────────────────┤
```

発信元 `mbd` だけが service hostname を NodeId へ解決する。中継 Node は HTTP header や hostname を解析せず、
packet の最終 NodeId を RouteTable で next-hop へ写す。宛先 `mbd` は、事前登録され allowlist されたローカル
endpoint へ接続する。

「アプリケーションが mesh-unaware」とは、アプリケーションコードが mesh API を呼ばないという意味である。
OSやsidecarにはDNS、route、TUN、transparent proxy等の統合が必要であり、インフラ統合まで不要という意味ではない。

## 3. Native Pub/Subとの役割分担

透過 L4 と Pub/Sub は競合せず、同じ Forwarder を使う別の通信プリミティブである。

```text
Application / Domain API
        ├─ Transparent L4: service → 1 Node → unicast Circuit
        └─ Native Pub/Sub: topic → N subscribers → Explicit Multicast
                                      │
                                      ▼
                              common Forwarder / QoS
                                      │
                                      ▼
                                common Mesh Links
```

| 要求 | 選択 |
|---|---|
| 既存 HTTP / HTTPS / gRPC / SSE / WebSocket / TCP | 透過 L4 Circuit |
| 特定 service への request / response | 透過 L4 Circuit |
| Entity、Track、センサーデータの多対多配信 | Native Pub/Sub |
| subscriber との時間的分離、Store-and-Forward、Backfill | Native Pub/Sub |
| RPC 単位の deadline、cancellation、status、認可 | 任意の gRPC-aware adapter |

SDK / domain API の `PublishEntity` を受けたローカル service が内部で Topic へ変換する構成も可能である。
この場合、アプリケーションには通常の REST / gRPC API だけを見せ、Native Pub/Sub を内部配信機構として隠せる。

## 4. 参考 OSS と得られたプラクティス

### 4.1 OpenZiti — 最も近い service-based overlay

[OpenZiti](https://openziti.io/docs/learn/introduction/) は、controller、edge / fabric router、tunnelerからなる
zero-trust overlay meshを提供する。特に次が今回の目標に近い。

- [`intercept.v1`](https://openziti.io/docs/reference/config-types/) でDNS名 / IP / portをserviceとして捕捉する
- `host.v1` / `host.v2` でoverlayの出口をローカルendpointへ対応付ける
- [Tunneler](https://openziti.io/docs/reference/tunnelers/) がDNSとOS routeを更新し、既存アプリを透過的に参加させる
- Router Fabricがservice connectionを複数linkに多重化し、経路を選ぶ
- dial / bind policyにより、接続前にidentityとserviceの組を認可する

採用したいのは、**interceptとhostを分離する設定モデル、allowlist、service単位のCircuit** である。
一方、OpenZitiはcontrollerがservice、policy、circuitを管理する。mesh-busではこのcontrol planeをそのまま採らず、
LSA / LSDB / SPFによる自律分散と、分断時のローカル継続を維持する。

### 4.2 Istio Ambient — L4を基盤、L7を任意にする

[Istio Ambient](https://istio.io/latest/docs/ambient/overview/) は、Node単位の `ztunnel` がHTTPを解析せず、
L3 / L4、mTLS、認可、telemetryを担当し、必要なworkloadだけEnvoy waypointでL7処理を行う。

ここから **軽量なL4 secure overlayを既定にし、HTTP / gRPC解釈を任意adapterとして上に載せる** 層分離を採用する。
Kubernetes、CNI、`istiod` / xDS、routableなIP underlayを前提とする点はDDIL meshと異なる。

### 4.3 Yggdrasil / ZeroTier / Nebula — OSに通常のnetworkを見せる

- [Yggdrasil](https://yggdrasil-network.github.io/) は分散・self-healing・E2E暗号化されたIPv6 overlayを提供する
- [ZeroTier](https://docs.zerotier.com/protocol/) は暗号化P2P network上に仮想Ethernetを提供する
- [Nebula](https://nebula.defined.net/docs/guides/quick-start/) はLighthouseによる発見と仮想IP overlayを提供する

これらから **OSに仮想interface / IPを見せれば、既存アプリは通常のsocketを使える** ことを学ぶ。
一方、L2 / L3を全面的に仮想化すると、MTU、fragmentation、broadcast、TCP-over-TCP、OSごとの差異まで
設計対象が広がる。そのため最初の実装はservice / portを限定したL4 split proxyとし、全面的なIP overlayは
後続の比較対象とする。

### 4.4 Babel / batman-adv — 動的な無線mesh

[Babel (RFC 8966)](https://www.rfc-editor.org/info/rfc8966/) は、有線と無線の動的mesh向けに設計された
loop-avoidingなdistance-vector routing protocolである。
[batman-adv](https://github.com/open-mesh-mirror/batman-adv) はLinux kernelでEthernet frameをmulti-hop転送する。

これらはL4 proxyではないが、**リンク品質の変化、churn、局所情報からの再収束を第一級の要件にする** という点が
重要である。mesh-busは既存のLSA / SPFを維持し、無線向けmetric、hysteresis、damping、再収束テストを取り入れる。

### 4.5 libp2p — 明示的なP2P transport

[libp2p Circuit Relay](https://libp2p.io/docs/circuit-relay/) は、直接接続できないPeer間を第三者Relay経由で
接続するが、公式にtransparentではないとされ、アプリケーションがPeer IDとlibp2p protocolを理解する。

NAT越え、relay resource limit、E2E identityは参考になるが、mesh-unawareな既存アプリをそのまま収容する入口には
使わない。

## 5. 組み合わせる設計原則

1. **Service addressing**：`hostname + port + protocol` をNodeIdとは分離し、LSAで結果整合に広告する。
2. **Transparent intercept**：最初は明示HTTP CONNECT / SOCKSまたはlocal proxy、次にDNS + TUN / transparent listenerへ進む。
3. **Split proxy**：application TCPを両端`mbd`で終端し、その間は有限bufferのCircuitとして運ぶ。
4. **Hop-by-hop routing**：発信元だけがserviceを解決し、中継は最終NodeIdとRouteTableだけを見る。
5. **Protocol opacity by default**：HTTPS / gRPC TLSをpass-throughし、L7解釈を必須にしない。
6. **Optional L7**：deadline、cancellation、method認可が必要なserviceだけgRPC-aware adapterを使う。
7. **Zero-trust service exposure**：任意portへegressさせず、登録済みendpointとdial / bind policyで制限する。
8. **Finite flow control**：Circuitごとのwindowとqueue上限を持ち、遅いendpointが中継Nodeのmemoryを消費し続けない。
9. **DDIL semantics are explicit**：経路消失時にcloseするMVPと、offset / ack / retransmitでCircuitを継続する強化版を分ける。
10. **Native Pub/Sub remains native**：多対多、conflation、dedup、Store-and-Forward、BackfillをN本のCircuitで代用しない。

単一のOSSを模倣するのではなく、OpenZitiのservice intercept、Istio AmbientのL4 / L7分離、Yggdrasil等の
透過network、Babel系の動的mesh運用を、既存のLSA control plane、Explicit Multicast、QoSへ組み合わせる。
これにより、通常のdatacenter service meshより分断に強く、一般的なad-hoc routingよりapplicationの意図を扱える
networkになる可能性がある。

## 6. 未解決事項

- hostnameからNodeIdを引くDNS / VIP mappingを、分断中も衝突なく維持する方法
- 同一serviceを複数Nodeが提供するときのconnection単位のsticky選択
- 経路変更時に既存Circuitを継続するためのoffset、ack、retransmit、reorder window
- hop-by-hop TCP上にCircuitを載せる際のHoL blockingと二重flow control
- HTTPS pass-through時のSNI / certificate名とservice名の対応
- LinuxのTUN / TPROXYと、macOS / Windowsでのintercept方式の差
- transparent proxyに必要な権限と、Node上の他processからの分離
- 一般Internet宛通信を許可する場合のegress gatewayとpolicy

これらを解かずに「透過」を宣言すると、単に接続先を書き換えるSDKになってしまう。
最初の完了条件は、既存のHTTP・SSE・gRPC clientがmesh固有libraryをlinkせず、service hostnameを指定するだけで
3 hop越しに通信できることとする。
