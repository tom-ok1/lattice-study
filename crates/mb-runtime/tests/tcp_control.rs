use mb_control::LinkCost;
use mb_runtime::{ControlRuntime, RuntimeConfig, RuntimeSnapshot};
use mb_transport::TcpEndpoint;
use mb_types::NodeId;
use std::collections::BTreeMap;
use std::net::SocketAddr;
use tokio::sync::watch;
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

async fn wait_for_snapshot(
    snapshots: &mut watch::Receiver<RuntimeSnapshot>,
    predicate: impl Fn(&RuntimeSnapshot) -> bool,
) -> RuntimeSnapshot {
    timeout(Duration::from_secs(5), async {
        loop {
            {
                let snapshot = snapshots.borrow();
                if predicate(&snapshot) {
                    return snapshot.clone();
                }
            }
            snapshots
                .changed()
                .await
                .expect("control runtime must remain available");
        }
    })
    .await
    .expect("control planes must converge")
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_nodes_converge_over_real_loopback_tcp_links() {
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

    endpoint_a
        .connect(b, endpoint_b.local_addr())
        .await
        .expect("A-B TCP link must connect");
    endpoint_b
        .connect(c, endpoint_c.local_addr())
        .await
        .expect("B-C TCP link must connect");

    // Stage the static topology before starting any control plane. This tests
    // initial flooding without relying on the later anti-entropy milestone.
    wait_for_links(&endpoint_a, 1).await;
    wait_for_links(&endpoint_b, 2).await;
    wait_for_links(&endpoint_c, 1).await;

    let runtime_a = ControlRuntime::spawn(runtime_config(a, [(b, cost(10))]), endpoint_a, events_a)
        .expect("A runtime must start");
    let runtime_b = ControlRuntime::spawn(
        runtime_config(b, [(a, cost(10)), (c, cost(20))]),
        endpoint_b,
        events_b,
    )
    .expect("B runtime must start");
    let runtime_c = ControlRuntime::spawn(runtime_config(c, [(b, cost(20))]), endpoint_c, events_c)
        .expect("C runtime must start");

    let mut snapshots_a = runtime_a.subscribe();
    let mut snapshots_b = runtime_b.subscribe();
    let mut snapshots_c = runtime_c.subscribe();
    let converged =
        |snapshot: &RuntimeSnapshot| snapshot.lsdb_entries == 3 && snapshot.routes.len() == 2;
    let snapshot_a = wait_for_snapshot(&mut snapshots_a, converged).await;
    let snapshot_b = wait_for_snapshot(&mut snapshots_b, converged).await;
    let snapshot_c = wait_for_snapshot(&mut snapshots_c, converged).await;

    let a_to_c = snapshot_a
        .routes
        .get(&c)
        .expect("A must route to C through B");
    assert_eq!(a_to_c.cost, 30);
    assert_eq!(a_to_c.hops, 2);
    assert_eq!(snapshot_a.peers.get(&a_to_c.next_hop), Some(&b));

    let c_to_a = snapshot_c
        .routes
        .get(&a)
        .expect("C must route to A through B");
    assert_eq!(c_to_a.cost, 30);
    assert_eq!(c_to_a.hops, 2);
    assert_eq!(snapshot_c.peers.get(&c_to_a.next_hop), Some(&b));
    assert_eq!(
        snapshot_b.routes.get(&a).expect("B must route to A").hops,
        1
    );
    assert_eq!(
        snapshot_b.routes.get(&c).expect("B must route to C").hops,
        1
    );
    assert!(snapshot_a.persisted_seq > 0);
    assert!(snapshot_b.persisted_seq > 0);
    assert!(snapshot_c.persisted_seq > 0);

    runtime_a.shutdown().await;
    runtime_b.shutdown().await;
    runtime_c.shutdown().await;
}
