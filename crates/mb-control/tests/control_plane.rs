use mb_control::{
    Adjacency, ControlAction, ControlEvent, ControlFrame, ControlPlane, LinkCost, Lsa, LsaMessage,
};
use mb_types::{Component, LinkId, MonoTime, NodeId};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

fn node(value: u8) -> NodeId {
    let mut bytes = [0_u8; 32];
    bytes[31] = value;
    NodeId::from_bytes(bytes)
}

fn cost(value: u16) -> LinkCost {
    LinkCost::new(value).expect("test cost must be non-zero")
}

struct Harness {
    nodes: Vec<ControlPlane>,
    endpoints: BTreeMap<(usize, LinkId), (usize, LinkId)>,
    queue: VecDeque<(usize, ControlEvent)>,
    next_link: u64,
    now_ms: u64,
    event_log: Vec<String>,
}

impl Harness {
    fn new(ids: &[NodeId]) -> Self {
        Self {
            nodes: ids
                .iter()
                .copied()
                .map(|id| ControlPlane::new_unsecured(id, 1))
                .collect(),
            endpoints: BTreeMap::new(),
            queue: VecDeque::new(),
            next_link: 1,
            now_ms: 0,
            event_log: Vec::new(),
        }
    }

    fn connect(&mut self, left: usize, right: usize, link_cost: LinkCost) -> (LinkId, LinkId) {
        let left_link = self.allocate_link();
        let right_link = self.allocate_link();
        self.endpoints
            .insert((left, left_link), (right, right_link));
        self.endpoints
            .insert((right, right_link), (left, left_link));

        self.queue.push_back((
            left,
            ControlEvent::LinkUp {
                link: left_link,
                peer: self.nodes[right].node_id(),
                cost: link_cost,
            },
        ));
        self.queue.push_back((
            right,
            ControlEvent::LinkUp {
                link: right_link,
                peer: self.nodes[left].node_id(),
                cost: link_cost,
            },
        ));
        (left_link, right_link)
    }

    fn disconnect(&mut self, left: usize, left_link: LinkId) {
        let (right, right_link) = self
            .endpoints
            .remove(&(left, left_link))
            .expect("link must exist");
        self.endpoints.remove(&(right, right_link));
        self.queue
            .push_back((left, ControlEvent::LinkDown { link: left_link }));
        self.queue
            .push_back((right, ControlEvent::LinkDown { link: right_link }));
    }

    fn inject(&mut self, target: usize, incoming: LinkId, lsa: Lsa) {
        self.queue.push_back((
            target,
            ControlEvent::Frame {
                link: incoming,
                frame: ControlFrame::Lsa(LsaMessage {
                    lsa,
                    canonical_bytes: Arc::from([]),
                    signature: Arc::from([]),
                }),
            },
        ));
    }

    fn run_until_idle(&mut self) {
        let mut processed = 0_usize;
        while let Some((target, event)) = self.queue.pop_front() {
            processed += 1;
            assert!(processed < 10_000, "control plane did not quiesce");
            self.now_ms += 1;
            self.event_log.push(format!("{target}:{event:?}"));
            let actions = self.nodes[target].handle(MonoTime::from_millis(self.now_ms), event);
            for action in actions {
                if let ControlAction::Send { link, frame } = action {
                    if let Some((peer, peer_link)) = self.endpoints.get(&(target, link)).copied() {
                        self.queue.push_back((
                            peer,
                            ControlEvent::Frame {
                                link: peer_link,
                                frame,
                            },
                        ));
                    }
                }
            }
        }
    }

    fn allocate_link(&mut self) -> LinkId {
        let link = LinkId::new(self.next_link);
        self.next_link += 1;
        link
    }
}

#[test]
fn three_node_chain_converges_and_routes_over_the_middle_node() {
    let ids = [node(1), node(2), node(3)];
    let mut harness = Harness::new(&ids);
    let (a_to_b, _) = harness.connect(0, 1, cost(10));
    harness.connect(1, 2, cost(20));

    harness.run_until_idle();

    assert!(harness.nodes.iter().all(|plane| plane.lsdb_len() == 3));
    for origin in ids {
        let expected = harness.nodes[0].lsa(&origin);
        assert!(harness
            .nodes
            .iter()
            .all(|plane| plane.lsa(&origin) == expected));
    }
    let route = harness.nodes[0]
        .route_table()
        .get(&ids[2])
        .cloned()
        .expect("A must have a route to C");
    assert_eq!(route.next_hop, a_to_b);
    assert_eq!(route.cost, 30);
    assert_eq!(route.hops, 2);
}

#[test]
fn link_down_removes_the_partitioned_destination_without_a_loop() {
    let ids = [node(1), node(2), node(3)];
    let mut harness = Harness::new(&ids);
    harness.connect(0, 1, cost(10));
    let (b_to_c, _) = harness.connect(1, 2, cost(20));
    harness.run_until_idle();
    assert!(harness.nodes[0].route_table().get(&ids[2]).is_some());

    harness.disconnect(1, b_to_c);
    harness.run_until_idle();

    assert!(harness.nodes[0].route_table().get(&ids[2]).is_none());
    assert!(harness.nodes[1].route_table().get(&ids[2]).is_none());
    assert!(harness.nodes[2].route_table().get(&ids[0]).is_none());
}

#[test]
fn one_sided_adjacency_is_never_used_for_routing() {
    let ids = [node(1), node(2), node(3)];
    let attacker = node(99);
    let mut harness = Harness::new(&ids);
    let (a_to_b, _) = harness.connect(0, 1, cost(10));
    harness.connect(1, 2, cost(10));
    harness.run_until_idle();

    let b_from_a = harness
        .endpoints
        .get(&(0, a_to_b))
        .expect("A-B link must exist")
        .1;
    harness.inject(
        1,
        b_from_a,
        Lsa {
            origin: attacker,
            epoch: 1,
            seq: 1,
            adjacencies: vec![Adjacency {
                peer: ids[0],
                cost: cost(1),
            }],
        },
    );
    harness.run_until_idle();

    // The frame is not echoed to the incoming peer, but it is flooded onward.
    assert!(harness.nodes[1].lsa(&attacker).is_some());
    assert!(harness.nodes[2].lsa(&attacker).is_some());
    assert!(harness
        .nodes
        .iter()
        .all(|plane| plane.route_table().get(&attacker).is_none()));
}

#[test]
fn identical_inputs_produce_an_identical_event_sequence() {
    fn run() -> Vec<String> {
        let mut harness = Harness::new(&[node(1), node(2), node(3)]);
        harness.connect(0, 1, cost(7));
        harness.connect(1, 2, cost(11));
        harness.run_until_idle();
        harness.event_log
    }

    assert_eq!(run(), run());
}
