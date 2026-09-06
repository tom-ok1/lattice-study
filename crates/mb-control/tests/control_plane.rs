use mb_control::{
    Adjacency, ControlAction, ControlEvent, ControlFrame, ControlPlane, ControlTimer, LinkCost,
    Lsa, LsaMessage, DEFAULT_LSA_TTL_SEC, DIGEST_INTERVAL_MS, MAX_DIGEST_ENTRIES,
    MAX_DIGEST_REQ_ORIGINS, MAX_LSA_ADJACENCIES, MAX_LSDB_ENTRIES, SPF_INITIAL_HOLD_MS,
    SPF_MAX_HOLD_MS, SPF_QUIET_RESET_MS,
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

fn scheduled_spf(actions: &[ControlAction]) -> (ControlTimer, MonoTime) {
    actions
        .iter()
        .find_map(|action| match action {
            ControlAction::SetTimer {
                timer: timer @ ControlTimer::SpfHold { .. },
                at,
            } => Some((*timer, *at)),
            _ => None,
        })
        .expect("an SPF timer must be scheduled")
}

fn reciprocal_lsa(origin: NodeId, peer: NodeId, seq: u64) -> LsaMessage {
    LsaMessage {
        lsa: Lsa {
            origin,
            epoch: 1,
            seq,
            ttl_sec: DEFAULT_LSA_TTL_SEC,
            adjacencies: vec![Adjacency {
                peer,
                cost: cost(10),
            }],
        },
        canonical_bytes: Arc::from([]),
        signature: Arc::from([]),
    }
}

struct Harness {
    nodes: Vec<ControlPlane>,
    endpoints: BTreeMap<(usize, LinkId), (usize, LinkId)>,
    queue: VecDeque<(usize, ControlEvent)>,
    spf_timers: BTreeMap<(u64, u64), (usize, ControlTimer)>,
    next_link: u64,
    next_timer: u64,
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
            spf_timers: BTreeMap::new(),
            next_link: 1,
            next_timer: 0,
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
        loop {
            let scheduled = if let Some((target, event)) = self.queue.pop_front() {
                Some((self.now_ms.saturating_add(1), target, event))
            } else {
                self.spf_timers
                    .pop_first()
                    .map(|((at, _), (target, timer))| (at, target, ControlEvent::Timer(timer)))
            };
            let Some((at, target, event)) = scheduled else {
                break;
            };
            processed += 1;
            assert!(processed < 10_000, "control plane did not quiesce");
            if !should_deliver(target, &event) {
                continue;
            }
            self.now_ms = self.now_ms.saturating_add(1).max(at);
            self.event_log.push(format!("{target}:{event:?}"));
            let actions = self.nodes[target].handle(MonoTime::from_millis(self.now_ms), event);
            for action in actions {
                match action {
                    ControlAction::Send { link, frame } => {
                        if let Some((peer, peer_link)) =
                            self.endpoints.get(&(target, link)).copied()
                        {
                            self.queue.push_back((
                                peer,
                                ControlEvent::Frame {
                                    link: peer_link,
                                    frame,
                                },
                            ));
                        }
                    }
                    ControlAction::SetTimer {
                        timer: timer @ ControlTimer::SpfHold { .. },
                        at,
                    } => {
                        self.spf_timers
                            .insert((at.as_millis(), self.next_timer), (target, timer));
                        self.next_timer = self.next_timer.wrapping_add(1);
                    }
                    ControlAction::SetTimer { .. }
                    | ControlAction::PublishRoutes(_)
                    | ControlAction::PersistSeq(_) => {}
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
            ttl_sec: 300,
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
fn spf_hold_waits_for_deadline_and_coalesces_a_burst() {
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
    let (timer, at) = scheduled_spf(&initial);
    assert_eq!(at.as_millis(), SPF_INITIAL_HOLD_MS);

    for (received_at, seq) in [(10, 1), (20, 2), (30, 3)] {
        let actions = plane.handle(
            MonoTime::from_millis(received_at),
            ControlEvent::Frame {
                link,
                frame: ControlFrame::Lsa(reciprocal_lsa(peer, me, seq)),
            },
        );
        assert!(!actions.iter().any(|action| matches!(
            action,
            ControlAction::SetTimer {
                timer: ControlTimer::SpfHold { .. },
                ..
            }
        )));
    }

    let early = plane.handle(
        MonoTime::from_millis(SPF_INITIAL_HOLD_MS - 1),
        ControlEvent::Timer(timer),
    );
    assert_eq!(scheduled_spf(&early), (timer, at));
    assert!(plane.route_table().is_empty());

    let fired = plane.handle(at, ControlEvent::Timer(timer));
    assert_eq!(
        fired
            .iter()
            .filter(|action| matches!(action, ControlAction::PublishRoutes(_)))
            .count(),
        1
    );
    assert_eq!(plane.route_table().version, 1);
    assert!(plane.route_table().get(&peer).is_some());
}

#[test]
fn spf_hold_backs_off_to_the_cap_and_resets_after_quiet() {
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
    let (initial_timer, initial_at) = scheduled_spf(&initial);
    plane.handle(initial_at, ControlEvent::Timer(initial_timer));

    let mut now = initial_at.as_millis();
    for (seq, expected_hold) in [200, 400, 800, 1_600, 3_200, 5_000, 5_000]
        .into_iter()
        .enumerate()
    {
        now += 1;
        let actions = plane.handle(
            MonoTime::from_millis(now),
            ControlEvent::Frame {
                link,
                frame: ControlFrame::Lsa(reciprocal_lsa(peer, me, seq as u64 + 1)),
            },
        );
        let (timer, at) = scheduled_spf(&actions);
        assert_eq!(at.as_millis() - now, expected_hold);
        assert!(expected_hold <= SPF_MAX_HOLD_MS);
        now = at.as_millis();
        plane.handle(at, ControlEvent::Timer(timer));
    }

    let after_quiet = now + SPF_QUIET_RESET_MS;
    let actions = plane.handle(
        MonoTime::from_millis(after_quiet),
        ControlEvent::Frame {
            link,
            frame: ControlFrame::Lsa(reciprocal_lsa(peer, me, 100)),
        },
    );
    let (_, at) = scheduled_spf(&actions);
    assert_eq!(at.as_millis() - after_quiet, SPF_INITIAL_HOLD_MS);
}

#[test]
fn stale_spf_generation_cannot_run_a_new_schedule() {
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
    let (old_timer, initial_at) = scheduled_spf(&initial);
    plane.handle(initial_at, ControlEvent::Timer(old_timer));

    let changed_at = initial_at.as_millis() + 1;
    let changed = plane.handle(
        MonoTime::from_millis(changed_at),
        ControlEvent::Frame {
            link,
            frame: ControlFrame::Lsa(reciprocal_lsa(peer, me, 1)),
        },
    );
    let (current_timer, current_at) = scheduled_spf(&changed);
    assert_ne!(old_timer, current_timer);

    assert!(plane
        .handle(current_at, ControlEvent::Timer(old_timer))
        .is_empty());
    assert!(plane.route_table().get(&peer).is_none());

    let current = plane.handle(current_at, ControlEvent::Timer(current_timer));
    assert!(current
        .iter()
        .any(|action| matches!(action, ControlAction::PublishRoutes(_))));
    assert!(plane.route_table().get(&peer).is_some());
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
                    ttl_sec: 300,
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

#[test]
fn local_lsa_is_refreshed_before_it_expires() {
    let me = node(1);
    let link = LinkId::new(1);
    let mut plane = ControlPlane::new_unsecured(me, 1);
    let initial = plane.handle(
        MonoTime::ZERO,
        ControlEvent::LinkUp {
            link,
            peer: node(2),
            cost: cost(10),
        },
    );
    let refresh_at = u64::from(DEFAULT_LSA_TTL_SEC) * 1_000 / 5;
    assert!(initial.iter().any(|action| {
        matches!(
            action,
            ControlAction::SetTimer {
                timer: ControlTimer::LsaRefresh { epoch: 1, seq: 1 },
                at,
            } if at.as_millis() == refresh_at
        )
    }));

    let refreshed = plane.handle(
        MonoTime::from_millis(refresh_at),
        ControlEvent::Timer(ControlTimer::LsaRefresh { epoch: 1, seq: 1 }),
    );

    assert_eq!(plane.lsa(&me).map(|lsa| lsa.seq), Some(2));
    assert!(matches!(
        refreshed.first(),
        Some(ControlAction::PersistSeq(2))
    ));
    assert!(refreshed.iter().any(|action| {
        matches!(
            action,
            ControlAction::SetTimer {
                timer: ControlTimer::LsaRefresh { epoch: 1, seq: 2 },
                at,
            } if at.as_millis() == refresh_at * 2
        )
    }));
}

#[test]
fn remote_lsa_becomes_a_permanent_compact_tombstone() {
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
    let (initial_spf, initial_spf_at) = scheduled_spf(&initial);
    let received_at = MonoTime::from_millis(10);
    let peer_lsa = Lsa {
        origin: peer,
        epoch: 1,
        seq: 7,
        ttl_sec: DEFAULT_LSA_TTL_SEC,
        adjacencies: vec![Adjacency {
            peer: me,
            cost: cost(10),
        }],
    };
    let canonical_bytes: Arc<[u8]> = Arc::from([1_u8, 2, 3]);
    let receive_actions = plane.handle(
        received_at,
        ControlEvent::Frame {
            link,
            frame: ControlFrame::Lsa(LsaMessage {
                lsa: peer_lsa.clone(),
                canonical_bytes: Arc::clone(&canonical_bytes),
                signature: Arc::from([]),
            }),
        },
    );
    assert!(!receive_actions.iter().any(|action| matches!(
        action,
        ControlAction::SetTimer {
            timer: ControlTimer::SpfHold { .. },
            ..
        }
    )));
    plane.handle(initial_spf_at, ControlEvent::Timer(initial_spf));
    assert!(plane.route_table().get(&peer).is_some());
    assert_eq!(Arc::strong_count(&canonical_bytes), 2);

    let expires_at = received_at.as_millis() + u64::from(DEFAULT_LSA_TTL_SEC) * 1_000;
    let expiry_actions = plane.handle(
        MonoTime::from_millis(expires_at),
        ControlEvent::Timer(ControlTimer::LsaExpire {
            origin: peer,
            epoch: 1,
            seq: 7,
        }),
    );
    assert_eq!(plane.lsa_is_expired(&peer), Some(true));
    assert!(plane.route_table().get(&peer).is_some());
    let (expiry_spf, expiry_spf_at) = scheduled_spf(&expiry_actions);
    plane.handle(expiry_spf_at, ControlEvent::Timer(expiry_spf));
    assert!(plane.route_table().get(&peer).is_none());
    assert_eq!(Arc::strong_count(&canonical_bytes), 1);

    let stale_actions = plane.handle(
        MonoTime::from_millis(expires_at + 1),
        ControlEvent::Frame {
            link,
            frame: ControlFrame::Lsa(LsaMessage {
                lsa: peer_lsa.clone(),
                canonical_bytes: Arc::from([]),
                signature: Arc::from([]),
            }),
        },
    );
    assert!(stale_actions.is_empty());
    assert!(plane.lsa(&peer).is_none());
    assert_eq!(plane.lsa_received_at(&peer), None);
    assert_eq!(plane.lsa_is_expired(&peer), Some(true));

    let much_later = expires_at + u64::from(DEFAULT_LSA_TTL_SEC) * 10_000;
    let late_stale_actions = plane.handle(
        MonoTime::from_millis(much_later),
        ControlEvent::Frame {
            link,
            frame: ControlFrame::Lsa(LsaMessage {
                lsa: peer_lsa.clone(),
                canonical_bytes: Arc::from([]),
                signature: Arc::from([]),
            }),
        },
    );
    assert!(late_stale_actions.is_empty());
    assert!(plane.lsa(&peer).is_none());
    assert_eq!(plane.lsa_is_expired(&peer), Some(true));

    let mut newer_lsa = peer_lsa;
    newer_lsa.seq += 1;
    plane.handle(
        MonoTime::from_millis(much_later + 1),
        ControlEvent::Frame {
            link,
            frame: ControlFrame::Lsa(LsaMessage {
                lsa: newer_lsa,
                canonical_bytes: Arc::from([]),
                signature: Arc::from([]),
            }),
        },
    );
    assert_eq!(plane.lsa(&peer).map(|lsa| lsa.seq), Some(8));
    assert_eq!(plane.lsa_is_expired(&peer), Some(false));
}

#[test]
fn timers_for_a_replaced_lsa_are_ignored() {
    let me = node(1);
    let peer = node(2);
    let link = LinkId::new(1);
    let mut plane = ControlPlane::new_unsecured(me, 1);
    plane.handle(
        MonoTime::ZERO,
        ControlEvent::LinkUp {
            link,
            peer,
            cost: cost(10),
        },
    );

    for seq in [1, 2] {
        plane.handle(
            MonoTime::from_millis(seq),
            ControlEvent::Frame {
                link,
                frame: ControlFrame::Lsa(LsaMessage {
                    lsa: Lsa {
                        origin: peer,
                        epoch: 1,
                        seq,
                        ttl_sec: DEFAULT_LSA_TTL_SEC,
                        adjacencies: vec![Adjacency {
                            peer: me,
                            cost: cost(10),
                        }],
                    },
                    canonical_bytes: Arc::from([]),
                    signature: Arc::from([]),
                }),
            },
        );
    }

    assert!(plane
        .handle(
            MonoTime::from_millis(u64::from(DEFAULT_LSA_TTL_SEC) * 1_000 + 1),
            ControlEvent::Timer(ControlTimer::LsaExpire {
                origin: peer,
                epoch: 1,
                seq: 1,
            }),
        )
        .is_empty());
    assert_eq!(plane.lsa(&peer).map(|lsa| lsa.seq), Some(2));
    assert_eq!(plane.lsa_is_expired(&peer), Some(false));
}

#[test]
fn injected_last_sequence_is_advanced_on_first_origination() {
    let mut plane = ControlPlane::new_unsecured_with_seq(node(1), 3, 4_999);
    let actions = plane.handle(
        MonoTime::ZERO,
        ControlEvent::LinkUp {
            link: LinkId::new(1),
            peer: node(2),
            cost: cost(10),
        },
    );

    assert_eq!(plane.lsa(&node(1)).map(|lsa| lsa.seq), Some(5_000));
    assert!(matches!(
        actions.first(),
        Some(ControlAction::PersistSeq(5_000))
    ));
}

#[test]
fn oversized_control_collections_and_invalid_ttl_are_rejected() {
    let me = node(1);
    let peer = node(2);
    let link = LinkId::new(1);
    let mut plane = ControlPlane::new_unsecured(me, 1);
    plane.handle(
        MonoTime::ZERO,
        ControlEvent::LinkUp {
            link,
            peer,
            cost: cost(10),
        },
    );

    let invalid_lsa = LsaMessage {
        lsa: Lsa {
            origin: peer,
            epoch: 1,
            seq: 1,
            ttl_sec: 0,
            adjacencies: Vec::new(),
        },
        canonical_bytes: Arc::from([]),
        signature: Arc::from([]),
    };
    assert!(plane
        .handle(
            MonoTime::from_millis(1),
            ControlEvent::Frame {
                link,
                frame: ControlFrame::Lsa(invalid_lsa),
            },
        )
        .is_empty());

    let adjacency = Adjacency {
        peer: me,
        cost: cost(1),
    };
    let oversized_lsa = LsaMessage {
        lsa: Lsa {
            origin: peer,
            epoch: 1,
            seq: 1,
            ttl_sec: DEFAULT_LSA_TTL_SEC,
            adjacencies: vec![adjacency; MAX_LSA_ADJACENCIES + 1],
        },
        canonical_bytes: Arc::from([]),
        signature: Arc::from([]),
    };
    assert!(plane
        .handle(
            MonoTime::from_millis(2),
            ControlEvent::Frame {
                link,
                frame: ControlFrame::Lsa(oversized_lsa),
            },
        )
        .is_empty());
    assert!(plane
        .handle(
            MonoTime::from_millis(3),
            ControlEvent::Frame {
                link,
                frame: ControlFrame::Digest(vec![
                    mb_control::DigestEntry {
                        origin: peer,
                        epoch: 1,
                        seq: 1,
                    };
                    MAX_DIGEST_ENTRIES + 1
                ]),
            },
        )
        .is_empty());
    assert!(plane
        .handle(
            MonoTime::from_millis(4),
            ControlEvent::Frame {
                link,
                frame: ControlFrame::DigestReq(vec![peer; MAX_DIGEST_REQ_ORIGINS + 1]),
            },
        )
        .is_empty());
}

#[test]
fn lsdb_size_is_bounded() {
    fn numbered_node(value: u16) -> NodeId {
        let mut bytes = [0_u8; 32];
        bytes[30..].copy_from_slice(&value.to_be_bytes());
        NodeId::from_bytes(bytes)
    }

    let me = numbered_node(0);
    let link = LinkId::new(1);
    let mut plane = ControlPlane::new_unsecured(me, 1);
    plane.handle(
        MonoTime::ZERO,
        ControlEvent::LinkUp {
            link,
            peer: numbered_node(1),
            cost: cost(10),
        },
    );
    for value in 1..=u16::try_from(MAX_LSDB_ENTRIES).expect("limit fits in u16") {
        plane.handle(
            MonoTime::from_millis(u64::from(value)),
            ControlEvent::Frame {
                link,
                frame: ControlFrame::Lsa(LsaMessage {
                    lsa: Lsa {
                        origin: numbered_node(value),
                        epoch: 1,
                        seq: 1,
                        ttl_sec: DEFAULT_LSA_TTL_SEC,
                        adjacencies: Vec::new(),
                    },
                    canonical_bytes: Arc::from([]),
                    signature: Arc::from([]),
                }),
            },
        );
    }

    assert_eq!(plane.lsdb_len(), MAX_LSDB_ENTRIES);
    assert!(plane.lsa(&me).is_some());
    assert!(plane.lsa(&numbered_node(1_000)).is_none());

    let first_remote = numbered_node(1);
    plane.handle(
        MonoTime::from_millis(1 + u64::from(DEFAULT_LSA_TTL_SEC) * 1_000),
        ControlEvent::Timer(ControlTimer::LsaExpire {
            origin: first_remote,
            epoch: 1,
            seq: 1,
        }),
    );
    assert_eq!(plane.lsa_is_expired(&first_remote), Some(true));

    let unknown = numbered_node(1_001);
    plane.handle(
        MonoTime::from_millis(1_000_000),
        ControlEvent::Frame {
            link,
            frame: ControlFrame::Lsa(LsaMessage {
                lsa: Lsa {
                    origin: unknown,
                    epoch: 1,
                    seq: 1,
                    ttl_sec: DEFAULT_LSA_TTL_SEC,
                    adjacencies: Vec::new(),
                },
                canonical_bytes: Arc::from([]),
                signature: Arc::from([]),
            }),
        },
    );
    assert!(plane.lsa(&unknown).is_none());
    assert_eq!(plane.lsa_is_expired(&first_remote), Some(true));
}
