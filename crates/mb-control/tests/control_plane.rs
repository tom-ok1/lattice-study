use mb_control::{
    Adjacency, ControlAction, ControlEvent, ControlFrame, ControlPlane, ControlTimer, LinkCost,
    Lsa, LsaMessage, DIGEST_INTERVAL_MS,
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
        self.run_until_idle_while(|_, _| true);
    }

    fn run_until_idle_while(
        &mut self,
        mut should_deliver: impl FnMut(usize, &ControlEvent) -> bool,
    ) {
        let mut processed = 0_usize;
        while let Some((target, event)) = self.queue.pop_front() {
            processed += 1;
            assert!(processed < 10_000, "control plane did not quiesce");
            if !should_deliver(target, &event) {
                continue;
            }
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

#[test]
fn link_up_digest_synchronizes_preexisting_lsdb_entries() {
    let ids = [node(1), node(2), node(3)];
    let mut harness = Harness::new(&ids);
    harness.connect(0, 1, cost(10));
    harness.run_until_idle();

    assert_eq!(harness.nodes[0].lsdb_len(), 2);
    assert_eq!(harness.nodes[1].lsdb_len(), 2);
    assert_eq!(harness.nodes[2].lsdb_len(), 0);

    harness.connect(1, 2, cost(20));
    harness.run_until_idle();

    assert!(harness.nodes.iter().all(|plane| plane.lsdb_len() == 3));
    assert!(harness.nodes[0].route_table().get(&ids[2]).is_some());
    assert!(harness.nodes[2].route_table().get(&ids[0]).is_some());
}

#[test]
fn digest_timer_sends_a_summary_and_reschedules_itself() {
    let me = node(1);
    let peer = node(2);
    let link = LinkId::new(1);
    let mut plane = ControlPlane::new_unsecured(me, 1);

    let initial = plane.handle(
        MonoTime::ZERO,
        ControlEvent::LinkUp {
            link,
            peer,
            cost: cost(10),
        },
    );
    assert!(initial.iter().any(|action| {
        matches!(
            action,
            ControlAction::SetTimer {
                timer: ControlTimer::Digest(timer_link),
                at,
            } if *timer_link == link && at.as_millis() == DIGEST_INTERVAL_MS
        )
    }));

    let periodic = plane.handle(
        MonoTime::from_millis(DIGEST_INTERVAL_MS),
        ControlEvent::Timer(ControlTimer::Digest(link)),
    );
    assert!(periodic.iter().any(|action| {
        matches!(
            action,
            ControlAction::Send {
                link: target,
                frame: ControlFrame::Digest(entries),
            } if *target == link
                && entries.iter().any(|entry| entry.origin == me && entry.seq == 1)
        )
    }));
    assert!(periodic.iter().any(|action| {
        matches!(
            action,
            ControlAction::SetTimer {
                timer: ControlTimer::Digest(timer_link),
                at,
            } if *timer_link == link && at.as_millis() == DIGEST_INTERVAL_MS * 2
        )
    }));
}

#[test]
fn periodic_digest_repairs_an_lsa_lost_during_initial_flooding() {
    let ids = [node(1), node(2)];
    let mut harness = Harness::new(&ids);
    let (a_to_b, _) = harness.connect(0, 1, cost(10));

    // Simulate a one-way outage while both adjacencies come up. B's LSA
    // reaches A, but every initial control frame from A to B is lost.
    harness.run_until_idle_while(|target, event| {
        !(target == 1 && matches!(event, ControlEvent::Frame { .. }))
    });
    assert_eq!(harness.nodes[0].lsdb_len(), 2);
    assert_eq!(harness.nodes[1].lsdb_len(), 1);

    harness
        .queue
        .push_back((0, ControlEvent::Timer(ControlTimer::Digest(a_to_b))));
    harness.run_until_idle();

    assert_eq!(harness.nodes[1].lsdb_len(), 2);
    assert!(harness.nodes[1].route_table().get(&ids[0]).is_some());
}

#[test]
fn stale_digest_timer_is_ignored_after_link_down() {
    let link = LinkId::new(1);
    let mut plane = ControlPlane::new_unsecured(node(1), 1);
    plane.handle(
        MonoTime::ZERO,
        ControlEvent::LinkUp {
            link,
            peer: node(2),
            cost: cost(10),
        },
    );
    plane.handle(MonoTime::from_millis(1), ControlEvent::LinkDown { link });

    assert!(plane
        .handle(
            MonoTime::from_millis(DIGEST_INTERVAL_MS),
            ControlEvent::Timer(ControlTimer::Digest(link)),
        )
        .is_empty());
}

#[test]
fn newer_self_origin_lsa_is_not_reflected_back_to_the_peer() {
    let me = node(1);
    let link = LinkId::new(1);
    let mut plane = ControlPlane::new_unsecured(me, 1);
    plane.handle(
        MonoTime::ZERO,
        ControlEvent::LinkUp {
            link,
            peer: node(2),
            cost: cost(10),
        },
    );

    let actions = plane.handle(
        MonoTime::from_millis(1),
        ControlEvent::Frame {
            link,
            frame: ControlFrame::Lsa(LsaMessage {
                lsa: Lsa {
                    origin: me,
                    epoch: 1,
                    seq: 2,
                    adjacencies: Vec::new(),
                },
                canonical_bytes: Arc::from([]),
                signature: Arc::from([]),
            }),
        },
    );

    assert!(actions.is_empty());
    assert_eq!(plane.lsa(&me).map(|lsa| lsa.seq), Some(1));

    let digest_actions = plane.handle(
        MonoTime::from_millis(2),
        ControlEvent::Frame {
            link,
            frame: ControlFrame::Digest(vec![mb_control::DigestEntry {
                origin: me,
                epoch: 1,
                seq: 2,
            }]),
        },
    );
    assert!(digest_actions.is_empty());
}
