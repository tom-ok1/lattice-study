use bytes::Bytes;
use mb_control::LinkCost;
use mb_runtime::{ControlRuntime, ForwardOutcome, RuntimeConfig};
use mb_transport::TcpEndpoint;
use mb_types::NodeId;
use mb_wire::{ForwardFlags, ForwardHeader, ForwardPacket, PacketType, Priority};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration};

fn node(value: u8) -> NodeId {
    let mut bytes = [0; 32];
    bytes[31] = value;
    NodeId::from_bytes(bytes)
}

fn cost(value: u16) -> LinkCost {
    LinkCost::new(value).expect("test link cost must be non-zero")
}

async fn wait_for_links(endpoint: &TcpEndpoint, expected: usize) {
    let mut counts = endpoint.subscribe_link_count();
    timeout(Duration::from_secs(5), async {
        loop {
            if *counts.borrow() == expected {
                return;
            }
            counts
                .changed()
                .await
                .expect("endpoint must remain available");
        }
    })
    .await
    .expect("static TCP links must become ready");
}

fn runtime_config(
    node_id: NodeId,
    peer_costs: impl IntoIterator<Item = (NodeId, LinkCost)>,
) -> RuntimeConfig {
    RuntimeConfig {
        node_id,
        epoch: 1,
        peer_costs: peer_costs.into_iter().collect::<BTreeMap<_, _>>(),
    }
}

fn packet(
    source: NodeId,
    destination: NodeId,
    priority: Priority,
    body: &'static [u8],
) -> ForwardPacket {
    ForwardPacket {
        header: ForwardHeader {
            packet_type: PacketType::Unicast,
            priority,
            ttl: 32,
            flags: ForwardFlags::empty(),
            destination,
            source,
            flow_id: 7,
            conflate_key: 0,
        },
        multicast_destinations: Vec::new(),
        payload: Bytes::from_static(body),
    }
}

async fn wait_for_delivery(outcomes: &mut mpsc::Receiver<ForwardOutcome>) -> ForwardPacket {
    timeout(Duration::from_secs(5), async {
        loop {
            if let ForwardOutcome::Delivered(packet) = outcomes
                .recv()
                .await
                .expect("forward runtime must remain available")
            {
                return packet;
            }
        }
    })
    .await
    .expect("packet must be delivered")
}

async fn wait_for_routed_delivery(
    runtime: &ControlRuntime,
    outcomes: &mut mpsc::Receiver<ForwardOutcome>,
    packet: ForwardPacket,
) -> ForwardPacket {
    timeout(Duration::from_secs(5), async {
        loop {
            runtime
                .send(packet.clone())
                .await
                .expect("runtime must accept a valid packet");
            match timeout(Duration::from_millis(25), outcomes.recv()).await {
                Ok(Some(ForwardOutcome::Delivered(delivered))) => return delivered,
                Ok(Some(_)) | Err(_) => {}
                Ok(None) => panic!("forward runtime stopped before delivery"),
            }
        }
    })
    .await
    .expect("route must converge and deliver the packet")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_converge_and_forward_over_real_loopback_tcp_links() {
    let [a, b, c] = [node(1), node(2), node(3)];
    let bind_addr: SocketAddr = "127.0.0.1:0"
        .parse()
        .expect("loopback socket address must parse");

    let (endpoint_a, events_a) = TcpEndpoint::bind(a, bind_addr, [b])
        .await
        .expect("A endpoint must bind");
    let (endpoint_b, events_b) = TcpEndpoint::bind(b, bind_addr, [a, c])
        .await
        .expect("B endpoint must bind");
    let (endpoint_c, events_c) = TcpEndpoint::bind(c, bind_addr, [b])
        .await
        .expect("C endpoint must bind");

    let runtime_a = ControlRuntime::spawn(
        runtime_config(a, [(b, cost(10))]),
        endpoint_a.clone(),
        events_a,
    )
    .expect("A runtime must start");
    let mut runtime_b = ControlRuntime::spawn(
        runtime_config(b, [(a, cost(10)), (c, cost(20))]),
        endpoint_b.clone(),
        events_b,
    )
    .expect("B runtime must start");
    let mut runtime_c = ControlRuntime::spawn(
        runtime_config(c, [(b, cost(20))]),
        endpoint_c.clone(),
        events_c,
    )
    .expect("C runtime must start");

    let mut outcomes_b = runtime_b
        .take_forward_outcomes()
        .expect("B application egress must be available once");
    let mut outcomes_c = runtime_c
        .take_forward_outcomes()
        .expect("C application egress must be available once");

    endpoint_a
        .connect(b, endpoint_b.local_addr())
        .await
        .expect("A-B TCP link must connect");
    wait_for_links(&endpoint_a, 1).await;
    wait_for_links(&endpoint_b, 1).await;
    let first_hop = wait_for_routed_delivery(
        &runtime_a,
        &mut outcomes_b,
        packet(a, b, Priority::P2, b"one-hop readiness"),
    )
    .await;
    assert_eq!(first_hop.header.destination, b);

    // C joins after A and B have already converged. Link-up digest exchange
    // must transfer A's preexisting LSA across B without a fresh A update.
    endpoint_b
        .connect(c, endpoint_c.local_addr())
        .await
        .expect("B-C TCP link must connect");
    wait_for_links(&endpoint_b, 2).await;
    wait_for_links(&endpoint_c, 1).await;

    let delivered = wait_for_routed_delivery(
        &runtime_a,
        &mut outcomes_c,
        packet(a, c, Priority::P2, b"two-hop unicast"),
    )
    .await;
    assert_eq!(delivered.header.source, a);
    assert_eq!(delivered.header.destination, c);
    assert_eq!(delivered.header.ttl, 30);
    assert_eq!(delivered.payload, Bytes::from_static(b"two-hop unicast"));

    runtime_a
        .send_multicast(
            vec![c, b],
            packet(a, NodeId::default(), Priority::P1, b"branched multicast"),
        )
        .await
        .expect("A must accept a multicast packet");
    let delivered_b = wait_for_delivery(&mut outcomes_b).await;
    let delivered_c = wait_for_delivery(&mut outcomes_c).await;
    assert_eq!(delivered_b.header.destination, b);
    assert_eq!(delivered_b.header.ttl, 31);
    assert_eq!(delivered_c.header.destination, c);
    assert_eq!(delivered_c.header.ttl, 30);
    assert_eq!(
        delivered_b.payload,
        Bytes::from_static(b"branched multicast")
    );
    assert_eq!(delivered_c.payload, delivered_b.payload);

    runtime_a.shutdown().await;
    runtime_b.shutdown().await;
    runtime_c.shutdown().await;
}
