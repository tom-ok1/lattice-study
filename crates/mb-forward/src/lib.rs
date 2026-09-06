//! I/O-free overlay packet forwarding.
//!
//! The runtime or simulator supplies route updates and packets as events, then
//! executes the returned actions. This crate never reads clocks or performs I/O.

use mb_control::RouteTable;
use mb_types::{Component, LinkId, MonoTime, NodeId};
use mb_wire::{ForwardFlags, ForwardPacket, PacketType, Priority};
use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

pub const QUEUE_LIMIT_PACKETS: [usize; 4] = [256, 512, 1_024, 4_096];
pub const DRR_QUANTUM_BYTES: [usize; 3] = [14 * 1_024, 5 * 1_024, 1_024];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DropReason {
    TtlExceeded,
    NoRoute,
    LoopDetected,
    UnsupportedPacketType(PacketType),
    QueueFull(Priority),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ForwardEvent {
    Outbound(ForwardPacket),
    Inbound { link: LinkId, packet: ForwardPacket },
    RoutesUpdated(Arc<RouteTable>),
    LinkCredit { link: LinkId, bytes: usize },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ForwardAction {
    Send {
        link: LinkId,
        packet: ForwardPacket,
    },
    DeliverLocal(ForwardPacket),
    Drop {
        reason: DropReason,
        packet: ForwardPacket,
    },
    Backpressure {
        link: LinkId,
        packet: ForwardPacket,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LinkQueueSnapshot {
    pub available_credit_bytes: usize,
    pub packet_counts: [usize; 4],
    pub queued_bytes: [usize; 4],
    pub deficit_bytes: [usize; 3],
    pub next_drr_priority: Priority,
}

struct LinkQueues {
    queues: [VecDeque<ForwardPacket>; 4],
    queued_bytes: [usize; 4],
    available_credit_bytes: usize,
    deficit_bytes: [usize; 3],
    next_drr: usize,
    drr_needs_quantum: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ConflationKey {
    source: NodeId,
    flow_id: u64,
    conflate_key: u64,
}

impl ConflationKey {
    fn from_packet(packet: &ForwardPacket) -> Option<Self> {
        (packet.header.priority == Priority::P1
            && packet.header.flags.contains(ForwardFlags::CONFLATABLE))
        .then_some(Self {
            source: packet.header.source,
            flow_id: packet.header.flow_id,
            conflate_key: packet.header.conflate_key,
        })
    }
}

impl Default for LinkQueues {
    fn default() -> Self {
        Self {
            queues: std::array::from_fn(|_| VecDeque::new()),
            queued_bytes: [0; 4],
            available_credit_bytes: 0,
            deficit_bytes: [0; 3],
            next_drr: 0,
            drr_needs_quantum: true,
        }
    }
}

pub struct Forwarder {
    me: NodeId,
    routes: Arc<RouteTable>,
    links: BTreeMap<LinkId, LinkQueues>,
}

impl Forwarder {
    pub fn new(me: NodeId, routes: Arc<RouteTable>) -> Self {
        Self {
            me,
            routes,
            links: BTreeMap::new(),
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.me
    }

    pub fn route_table(&self) -> Arc<RouteTable> {
        Arc::clone(&self.routes)
    }

    pub fn queue_snapshot(&self, link: LinkId) -> Option<LinkQueueSnapshot> {
        self.links.get(&link).map(|state| LinkQueueSnapshot {
            available_credit_bytes: state.available_credit_bytes,
            packet_counts: std::array::from_fn(|index| state.queues[index].len()),
            queued_bytes: state.queued_bytes,
            deficit_bytes: state.deficit_bytes,
            next_drr_priority: drr_priority(state.next_drr),
        })
    }

    fn forward(&self, incoming: Option<LinkId>, mut packet: ForwardPacket) -> ForwardAction {
        if packet.header.ttl == 0 {
            return ForwardAction::Drop {
                reason: DropReason::TtlExceeded,
                packet,
            };
        }
        if packet.header.packet_type != PacketType::Unicast {
            return ForwardAction::Drop {
                reason: DropReason::UnsupportedPacketType(packet.header.packet_type),
                packet,
            };
        }
        if packet.header.destination == self.me {
            return ForwardAction::DeliverLocal(packet);
        }

        let Some(route) = self.routes.get(&packet.header.destination) else {
            return ForwardAction::Drop {
                reason: DropReason::NoRoute,
                packet,
            };
        };
        if incoming == Some(route.next_hop) {
            return ForwardAction::Drop {
                reason: DropReason::LoopDetected,
                packet,
            };
        }

        packet.header.ttl -= 1;
        ForwardAction::Send {
            link: route.next_hop,
            packet,
        }
    }

    fn process_packet(
        &mut self,
        incoming: Option<LinkId>,
        packet: ForwardPacket,
    ) -> Vec<ForwardAction> {
        match self.forward(incoming, packet) {
            ForwardAction::Send { link, packet } => self.enqueue(link, packet),
            action => vec![action],
        }
    }

    fn enqueue(&mut self, link: LinkId, packet: ForwardPacket) -> Vec<ForwardAction> {
        let priority = packet.header.priority;
        let queue_index = priority_index(priority);
        let state = self.links.entry(link).or_default();

        if let Some(key) = ConflationKey::from_packet(&packet) {
            if let Some(queued) = state.queues[queue_index]
                .iter_mut()
                .find(|queued| ConflationKey::from_packet(queued) == Some(key))
            {
                let previous_len = queued.encoded_len();
                let replacement_len = packet.encoded_len();
                *queued = packet;
                state.queued_bytes[queue_index] = state.queued_bytes[queue_index]
                    .saturating_sub(previous_len)
                    .saturating_add(replacement_len);
                return self.drain_link(link);
            }
        }

        if state.queues[queue_index].len() >= QUEUE_LIMIT_PACKETS[queue_index] {
            return if priority == Priority::P0 {
                vec![ForwardAction::Backpressure { link, packet }]
            } else {
                vec![ForwardAction::Drop {
                    reason: DropReason::QueueFull(priority),
                    packet,
                }]
            };
        }

        state.queued_bytes[queue_index] =
            state.queued_bytes[queue_index].saturating_add(packet.encoded_len());
        state.queues[queue_index].push_back(packet);
        self.drain_link(link)
    }

    fn grant_credit(&mut self, link: LinkId, bytes: usize) -> Vec<ForwardAction> {
        let state = self.links.entry(link).or_default();
        state.available_credit_bytes = state.available_credit_bytes.saturating_add(bytes);
        self.drain_link(link)
    }

    fn drain_link(&mut self, link: LinkId) -> Vec<ForwardAction> {
        let Some(state) = self.links.get_mut(&link) else {
            return Vec::new();
        };
        let mut actions = Vec::new();

        loop {
            if let Some(packet_len) = state.queues[0].front().map(ForwardPacket::encoded_len) {
                if packet_len > state.available_credit_bytes {
                    break;
                }
                let packet = state.queues[0]
                    .pop_front()
                    .expect("P0 queue front was checked");
                state.queued_bytes[0] = state.queued_bytes[0].saturating_sub(packet_len);
                state.available_credit_bytes -= packet_len;
                actions.push(ForwardAction::Send { link, packet });
                continue;
            }

            let a_packet_fits_credit = state.queues[1..].iter().any(|queue| {
                queue
                    .front()
                    .is_some_and(|packet| packet.encoded_len() <= state.available_credit_bytes)
            });
            if !a_packet_fits_credit {
                break;
            }

            let drr_index = state.next_drr;
            let queue_index = drr_index + 1;
            if state.queues[queue_index].is_empty() {
                state.deficit_bytes[drr_index] = 0;
                advance_drr(state);
                continue;
            }
            if state.drr_needs_quantum {
                state.deficit_bytes[drr_index] =
                    state.deficit_bytes[drr_index].saturating_add(DRR_QUANTUM_BYTES[drr_index]);
                state.drr_needs_quantum = false;
            }

            let packet_len = state.queues[queue_index]
                .front()
                .expect("non-empty DRR queue was checked")
                .encoded_len();
            if packet_len > state.deficit_bytes[drr_index]
                || packet_len > state.available_credit_bytes
            {
                advance_drr(state);
                continue;
            }

            let packet = state.queues[queue_index]
                .pop_front()
                .expect("DRR queue front was checked");
            state.deficit_bytes[drr_index] -= packet_len;
            state.queued_bytes[queue_index] =
                state.queued_bytes[queue_index].saturating_sub(packet_len);
            state.available_credit_bytes -= packet_len;
            actions.push(ForwardAction::Send { link, packet });
            if state.queues[queue_index].is_empty() {
                state.deficit_bytes[drr_index] = 0;
                advance_drr(state);
            }
        }

        actions
    }
}

fn priority_index(priority: Priority) -> usize {
    priority as usize
}

fn drr_priority(index: usize) -> Priority {
    match index {
        0 => Priority::P1,
        1 => Priority::P2,
        2 => Priority::P3,
        _ => unreachable!("DRR index is always in 0..3"),
    }
}

fn advance_drr(state: &mut LinkQueues) {
    state.next_drr = (state.next_drr + 1) % 3;
    state.drr_needs_quantum = true;
}

impl Component for Forwarder {
    type Event = ForwardEvent;
    type Action = ForwardAction;

    fn handle(&mut self, _now: MonoTime, event: Self::Event) -> Vec<Self::Action> {
        match event {
            ForwardEvent::Outbound(packet) => self.process_packet(None, packet),
            ForwardEvent::Inbound { link, packet } => self.process_packet(Some(link), packet),
            ForwardEvent::RoutesUpdated(routes) => {
                self.routes = routes;
                Vec::new()
            }
            ForwardEvent::LinkCredit { link, bytes } => self.grant_credit(link, bytes),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mb_control::Route;
    use mb_wire::{ForwardHeader, Priority, FORWARD_HEADER_LEN};
    use std::collections::BTreeMap;
    use std::mem::size_of;

    fn node(value: u8) -> NodeId {
        let mut bytes = [0_u8; 32];
        bytes[31] = value;
        NodeId::from_bytes(bytes)
    }

    fn route_table(
        entries: impl IntoIterator<Item = (NodeId, LinkId, u32, u8)>,
    ) -> Arc<RouteTable> {
        Arc::new(RouteTable::new(
            1,
            entries
                .into_iter()
                .map(|(destination, next_hop, cost, hops)| {
                    (
                        destination,
                        Route {
                            next_hop,
                            cost,
                            hops,
                        },
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        ))
    }

    fn packet(source: NodeId, destination: NodeId, ttl: u8) -> ForwardPacket {
        ForwardPacket {
            header: ForwardHeader {
                packet_type: PacketType::Unicast,
                priority: Priority::P1,
                ttl,
                flags: ForwardFlags::empty(),
                destination,
                source,
                flow_id: 42,
                conflate_key: 0,
            },
            payload: Bytes::from_static(b"end-to-end payload"),
        }
    }

    fn conflatable_packet(
        source: NodeId,
        destination: NodeId,
        flow_id: u64,
        conflate_key: u64,
        payload: Bytes,
    ) -> ForwardPacket {
        let mut packet = packet(source, destination, 32);
        packet.header.flags = ForwardFlags::from_bits(ForwardFlags::CONFLATABLE)
            .expect("conflatable is a supported flag");
        packet.header.flow_id = flow_id;
        packet.header.conflate_key = conflate_key;
        packet.payload = payload;
        packet
    }

    fn one_action(actions: Vec<ForwardAction>) -> ForwardAction {
        assert_eq!(actions.len(), 1);
        actions.into_iter().next().expect("one action was checked")
    }

    fn expect_send(action: ForwardAction, expected_link: LinkId) -> ForwardPacket {
        match action {
            ForwardAction::Send { link, packet } => {
                assert_eq!(link, expected_link);
                packet
            }
            other => panic!("expected send action, got {other:?}"),
        }
    }

    #[test]
    fn unicast_crosses_two_hops_and_preserves_the_payload() {
        let [a, b, c] = [node(1), node(2), node(3)];
        let [a_to_b, b_to_a, b_to_c, c_to_b] = [
            LinkId::new(1),
            LinkId::new(2),
            LinkId::new(3),
            LinkId::new(4),
        ];
        let mut forwarder_a =
            Forwarder::new(a, route_table([(b, a_to_b, 10, 1), (c, a_to_b, 20, 2)]));
        let mut forwarder_b =
            Forwarder::new(b, route_table([(a, b_to_a, 10, 1), (c, b_to_c, 10, 1)]));
        let mut forwarder_c =
            Forwarder::new(c, route_table([(a, c_to_b, 20, 2), (b, c_to_b, 10, 1)]));
        let original = packet(a, c, 32);

        assert!(forwarder_a
            .handle(
                MonoTime::ZERO,
                ForwardEvent::LinkCredit {
                    link: a_to_b,
                    bytes: original.encoded_len(),
                },
            )
            .is_empty());
        assert!(forwarder_b
            .handle(
                MonoTime::ZERO,
                ForwardEvent::LinkCredit {
                    link: b_to_c,
                    bytes: original.encoded_len(),
                },
            )
            .is_empty());

        let at_b = expect_send(
            one_action(
                forwarder_a.handle(MonoTime::ZERO, ForwardEvent::Outbound(original.clone())),
            ),
            a_to_b,
        );
        assert_eq!(at_b.header.ttl, 31);
        let at_c = expect_send(
            one_action(forwarder_b.handle(
                MonoTime::from_millis(1),
                ForwardEvent::Inbound {
                    link: b_to_a,
                    packet: at_b,
                },
            )),
            b_to_c,
        );
        assert_eq!(at_c.header.ttl, 30);

        let delivered = one_action(forwarder_c.handle(
            MonoTime::from_millis(2),
            ForwardEvent::Inbound {
                link: c_to_b,
                packet: at_c,
            },
        ));
        match delivered {
            ForwardAction::DeliverLocal(packet) => {
                assert_eq!(packet.header.ttl, 30);
                assert_eq!(packet.payload, original.payload);
                assert_eq!(packet.header.source, a);
                assert_eq!(packet.header.destination, c);
            }
            other => panic!("expected local delivery, got {other:?}"),
        }
    }

    #[test]
    fn invalid_forwarding_conditions_have_explicit_drop_reasons() {
        let [a, b, c] = [node(1), node(2), node(3)];
        let link = LinkId::new(1);
        let mut forwarder = Forwarder::new(a, route_table([(c, link, 10, 1)]));

        let ttl_exceeded = packet(b, a, 0);
        assert_eq!(
            one_action(forwarder.handle(
                MonoTime::ZERO,
                ForwardEvent::Inbound {
                    link,
                    packet: ttl_exceeded.clone(),
                },
            )),
            ForwardAction::Drop {
                reason: DropReason::TtlExceeded,
                packet: ttl_exceeded,
            }
        );

        let no_route = packet(a, b, 32);
        assert_eq!(
            one_action(forwarder.handle(MonoTime::ZERO, ForwardEvent::Outbound(no_route.clone()),)),
            ForwardAction::Drop {
                reason: DropReason::NoRoute,
                packet: no_route,
            }
        );

        let looped = packet(b, c, 32);
        assert_eq!(
            one_action(forwarder.handle(
                MonoTime::ZERO,
                ForwardEvent::Inbound {
                    link,
                    packet: looped.clone(),
                },
            )),
            ForwardAction::Drop {
                reason: DropReason::LoopDetected,
                packet: looped,
            }
        );

        let mut multicast = packet(a, c, 32);
        multicast.header.packet_type = PacketType::Multicast;
        assert_eq!(
            one_action(
                forwarder.handle(MonoTime::ZERO, ForwardEvent::Outbound(multicast.clone()),)
            ),
            ForwardAction::Drop {
                reason: DropReason::UnsupportedPacketType(PacketType::Multicast),
                packet: multicast,
            }
        );
    }

    #[test]
    fn route_updates_are_used_by_subsequent_packets() {
        let [a, b] = [node(1), node(2)];
        let link = LinkId::new(7);
        let mut forwarder = Forwarder::new(a, Arc::new(RouteTable::default()));
        let outbound = packet(a, b, 32);

        assert!(matches!(
            one_action(forwarder.handle(MonoTime::ZERO, ForwardEvent::Outbound(outbound.clone()))),
            ForwardAction::Drop {
                reason: DropReason::NoRoute,
                ..
            }
        ));
        assert!(forwarder
            .handle(
                MonoTime::from_millis(1),
                ForwardEvent::RoutesUpdated(route_table([(b, link, 10, 1)])),
            )
            .is_empty());
        assert!(forwarder
            .handle(
                MonoTime::from_millis(1),
                ForwardEvent::LinkCredit {
                    link,
                    bytes: outbound.encoded_len(),
                },
            )
            .is_empty());

        let sent = expect_send(
            one_action(
                forwarder.handle(MonoTime::from_millis(2), ForwardEvent::Outbound(outbound)),
            ),
            link,
        );
        assert_eq!(sent.header.ttl, 31);
    }

    #[test]
    fn identical_inputs_produce_identical_actions() {
        fn run() -> (Vec<ForwardAction>, Option<LinkQueueSnapshot>) {
            let [a, b] = [node(1), node(2)];
            let link = LinkId::new(1);
            let routes = route_table([(b, link, 10, 1)]);
            let mut forwarder = Forwarder::new(a, Arc::new(RouteTable::default()));
            let mut actions = forwarder.handle(MonoTime::ZERO, ForwardEvent::RoutesUpdated(routes));
            actions.extend(forwarder.handle(
                MonoTime::ZERO,
                ForwardEvent::LinkCredit {
                    link,
                    bytes: packet(a, b, 32).encoded_len(),
                },
            ));
            actions.extend(forwarder.handle(
                MonoTime::from_millis(1),
                ForwardEvent::Outbound(packet(a, b, 32)),
            ));
            (actions, forwarder.queue_snapshot(link))
        }

        assert_eq!(run(), run());
    }

    #[test]
    fn packets_wait_until_enough_incremental_credit_is_available() {
        let [a, b] = [node(1), node(2)];
        let link = LinkId::new(1);
        let mut forwarder = Forwarder::new(a, route_table([(b, link, 10, 1)]));
        let outbound = packet(a, b, 32);
        let packet_len = outbound.encoded_len();

        assert!(forwarder
            .handle(MonoTime::ZERO, ForwardEvent::Outbound(outbound))
            .is_empty());
        assert_eq!(
            forwarder.queue_snapshot(link),
            Some(LinkQueueSnapshot {
                available_credit_bytes: 0,
                packet_counts: [0, 1, 0, 0],
                queued_bytes: [0, packet_len, 0, 0],
                deficit_bytes: [0; 3],
                next_drr_priority: Priority::P1,
            })
        );

        assert!(forwarder
            .handle(
                MonoTime::from_millis(1),
                ForwardEvent::LinkCredit {
                    link,
                    bytes: packet_len - 1,
                },
            )
            .is_empty());
        let sent = one_action(forwarder.handle(
            MonoTime::from_millis(2),
            ForwardEvent::LinkCredit { link, bytes: 1 },
        ));
        assert_eq!(expect_send(sent, link).header.ttl, 31);

        let snapshot = forwarder
            .queue_snapshot(link)
            .expect("link queue state must remain observable");
        assert_eq!(snapshot.available_credit_bytes, 0);
        assert_eq!(snapshot.packet_counts, [0; 4]);
        assert_eq!(snapshot.queued_bytes, [0; 4]);
    }

    #[test]
    fn repeated_conflatable_updates_send_only_the_latest_packet() {
        fn run() -> (Vec<ForwardAction>, LinkQueueSnapshot, LinkQueueSnapshot) {
            let [a, b] = [node(1), node(2)];
            let link = LinkId::new(1);
            let mut forwarder = Forwarder::new(a, route_table([(b, link, 10, 1)]));

            for sequence in 0_u64..1_000 {
                let queued = conflatable_packet(
                    a,
                    b,
                    42,
                    7,
                    Bytes::copy_from_slice(&sequence.to_be_bytes()),
                );
                assert!(forwarder
                    .handle(MonoTime::ZERO, ForwardEvent::Outbound(queued))
                    .is_empty());
            }

            let before = forwarder
                .queue_snapshot(link)
                .expect("conflated packet must remain queued");
            let actions = forwarder.handle(
                MonoTime::from_millis(1),
                ForwardEvent::LinkCredit {
                    link,
                    bytes: FORWARD_HEADER_LEN + size_of::<u64>(),
                },
            );
            let after = forwarder
                .queue_snapshot(link)
                .expect("link queue state must remain observable");
            (actions, before, after)
        }

        let first = run();
        assert_eq!(first, run());
        let (actions, before, after) = first;
        assert_eq!(before.packet_counts, [0, 1, 0, 0]);
        assert_eq!(
            before.queued_bytes[1],
            FORWARD_HEADER_LEN + size_of::<u64>()
        );
        let sent = expect_send(one_action(actions), LinkId::new(1));
        assert_eq!(sent.payload.as_ref(), &999_u64.to_be_bytes());
        assert_eq!(after.packet_counts, [0; 4]);
        assert_eq!(after.queued_bytes, [0; 4]);
    }

    #[test]
    fn conflation_preserves_queue_position_and_tracks_replacement_size() {
        let [a, b] = [node(1), node(2)];
        let link = LinkId::new(1);
        let mut forwarder = Forwarder::new(a, route_table([(b, link, 10, 1)]));
        let first = conflatable_packet(a, b, 10, 20, Bytes::from_static(b"old"));
        let mut second = packet(a, b, 32);
        second.header.flow_id = 99;
        second.payload = Bytes::from_static(b"second");
        let large_replacement = conflatable_packet(a, b, 10, 20, Bytes::from(vec![b'n'; 1_000]));
        let replacement = conflatable_packet(a, b, 10, 20, Bytes::from_static(b"new"));

        assert!(forwarder
            .handle(MonoTime::ZERO, ForwardEvent::Outbound(first))
            .is_empty());
        assert!(forwarder
            .handle(MonoTime::ZERO, ForwardEvent::Outbound(second.clone()))
            .is_empty());
        assert!(forwarder
            .handle(
                MonoTime::ZERO,
                ForwardEvent::Outbound(large_replacement.clone())
            )
            .is_empty());

        let large_snapshot = forwarder
            .queue_snapshot(link)
            .expect("packets must remain queued without credit");
        assert_eq!(large_snapshot.packet_counts, [0, 2, 0, 0]);
        assert_eq!(
            large_snapshot.queued_bytes[1],
            large_replacement.encoded_len() + second.encoded_len()
        );
        assert!(forwarder
            .handle(MonoTime::ZERO, ForwardEvent::Outbound(replacement.clone()))
            .is_empty());

        let small_snapshot = forwarder
            .queue_snapshot(link)
            .expect("smaller replacement must remain queued");
        assert_eq!(small_snapshot.packet_counts, [0, 2, 0, 0]);
        assert_eq!(
            small_snapshot.queued_bytes[1],
            replacement.encoded_len() + second.encoded_len()
        );

        let actions = forwarder.handle(
            MonoTime::from_millis(1),
            ForwardEvent::LinkCredit {
                link,
                bytes: replacement.encoded_len() + second.encoded_len(),
            },
        );
        assert_eq!(actions.len(), 2);
        let sent_payloads = actions
            .into_iter()
            .map(|action| expect_send(action, link).payload)
            .collect::<Vec<_>>();
        assert_eq!(
            sent_payloads,
            vec![replacement.payload, Bytes::from_static(b"second")]
        );
    }

    #[test]
    fn conflation_requires_p1_flag_and_an_exact_header_key_match() {
        fn queued_count(first: ForwardPacket, second: ForwardPacket) -> usize {
            let [a, b] = [node(1), node(2)];
            let link = LinkId::new(1);
            let mut forwarder = Forwarder::new(a, route_table([(b, link, 10, 1)]));
            assert!(forwarder
                .handle(MonoTime::ZERO, ForwardEvent::Outbound(first))
                .is_empty());
            assert!(forwarder
                .handle(MonoTime::ZERO, ForwardEvent::Outbound(second))
                .is_empty());
            forwarder
                .queue_snapshot(link)
                .expect("packets must be queued")
                .packet_counts
                .into_iter()
                .sum()
        }

        let [a, b, other_source] = [node(1), node(2), node(3)];
        let base = conflatable_packet(a, b, 10, 20, Bytes::from_static(b"first"));

        let mut without_flag = base.clone();
        without_flag.header.flags = ForwardFlags::empty();
        assert_eq!(queued_count(without_flag.clone(), without_flag), 2);

        for priority in [Priority::P0, Priority::P2, Priority::P3] {
            let mut other_priority = base.clone();
            other_priority.header.priority = priority;
            assert_eq!(queued_count(other_priority.clone(), other_priority), 2);
        }

        let mut different_source = base.clone();
        different_source.header.source = other_source;
        assert_eq!(queued_count(base.clone(), different_source), 2);

        let mut different_flow = base.clone();
        different_flow.header.flow_id += 1;
        assert_eq!(queued_count(base.clone(), different_flow), 2);

        let mut different_key = base.clone();
        different_key.header.conflate_key += 1;
        assert_eq!(queued_count(base, different_key), 2);
    }

    #[test]
    fn a_full_p1_queue_still_accepts_a_matching_replacement() {
        let [a, b] = [node(1), node(2)];
        let link = LinkId::new(1);
        let mut forwarder = Forwarder::new(a, route_table([(b, link, 10, 1)]));

        for key in 0..QUEUE_LIMIT_PACKETS[1] as u64 {
            let queued = conflatable_packet(a, b, 42, key, Bytes::from_static(b"old"));
            assert!(forwarder
                .handle(MonoTime::ZERO, ForwardEvent::Outbound(queued))
                .is_empty());
        }

        let replacement = conflatable_packet(a, b, 42, 0, Bytes::from_static(b"latest"));
        assert!(forwarder
            .handle(MonoTime::ZERO, ForwardEvent::Outbound(replacement))
            .is_empty());
        assert_eq!(
            forwarder
                .queue_snapshot(link)
                .expect("full queue must remain observable")
                .packet_counts[1],
            QUEUE_LIMIT_PACKETS[1]
        );

        let overflow = conflatable_packet(
            a,
            b,
            42,
            QUEUE_LIMIT_PACKETS[1] as u64,
            Bytes::from_static(b"new key"),
        );
        assert!(matches!(
            one_action(forwarder.handle(MonoTime::ZERO, ForwardEvent::Outbound(overflow))),
            ForwardAction::Drop {
                reason: DropReason::QueueFull(Priority::P1),
                ..
            }
        ));
    }

    #[test]
    fn p0_is_sent_before_already_queued_p3() {
        let [a, b] = [node(1), node(2)];
        let link = LinkId::new(1);
        let mut forwarder = Forwarder::new(a, route_table([(b, link, 10, 1)]));
        let mut p3 = packet(a, b, 32);
        p3.header.priority = Priority::P3;
        p3.header.flow_id = 3;
        let mut p0 = packet(a, b, 32);
        p0.header.priority = Priority::P0;
        p0.header.flow_id = 0;
        let packet_len = p0.encoded_len();

        assert!(forwarder
            .handle(MonoTime::ZERO, ForwardEvent::Outbound(p3))
            .is_empty());
        assert!(forwarder
            .handle(MonoTime::ZERO, ForwardEvent::Outbound(p0))
            .is_empty());

        let first = expect_send(
            one_action(forwarder.handle(
                MonoTime::from_millis(1),
                ForwardEvent::LinkCredit {
                    link,
                    bytes: packet_len,
                },
            )),
            link,
        );
        assert_eq!(first.header.priority, Priority::P0);
        assert_eq!(
            forwarder
                .queue_snapshot(link)
                .expect("queue must exist")
                .packet_counts,
            [0, 0, 0, 1]
        );

        let second = expect_send(
            one_action(forwarder.handle(
                MonoTime::from_millis(2),
                ForwardEvent::LinkCredit {
                    link,
                    bytes: packet_len,
                },
            )),
            link,
        );
        assert_eq!(second.header.priority, Priority::P3);
    }

    #[test]
    fn drr_eventually_serves_every_non_empty_priority() {
        let [a, b] = [node(1), node(2)];
        let link = LinkId::new(1);
        let mut forwarder = Forwarder::new(a, route_table([(b, link, 10, 1)]));
        let priorities = [Priority::P1, Priority::P2, Priority::P3];
        let mut total_bytes = 0;

        for priority in priorities {
            for sequence in 0..3 {
                let mut queued = packet(a, b, 32);
                queued.header.priority = priority;
                queued.header.flow_id = priority_index(priority) as u64 * 10 + sequence;
                queued.payload = Bytes::from(vec![sequence as u8; 2_048]);
                total_bytes += queued.encoded_len();
                assert!(forwarder
                    .handle(MonoTime::ZERO, ForwardEvent::Outbound(queued))
                    .is_empty());
            }
        }

        let actions = forwarder.handle(
            MonoTime::from_millis(1),
            ForwardEvent::LinkCredit {
                link,
                bytes: total_bytes,
            },
        );
        let sent_priorities = actions
            .iter()
            .map(|action| match action {
                ForwardAction::Send { packet, .. } => packet.header.priority,
                other => panic!("credit must only produce sends, got {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(sent_priorities.len(), 9);
        assert!(priorities
            .into_iter()
            .all(|priority| sent_priorities.contains(&priority)));
        assert_eq!(
            forwarder
                .queue_snapshot(link)
                .expect("queue must exist")
                .packet_counts,
            [0; 4]
        );
    }

    #[test]
    fn bounded_queues_report_backpressure_or_drop() {
        let [a, b] = [node(1), node(2)];
        let link = LinkId::new(1);
        let mut forwarder = Forwarder::new(a, route_table([(b, link, 10, 1)]));

        for _ in 0..QUEUE_LIMIT_PACKETS[0] {
            let mut queued = packet(a, b, 32);
            queued.header.priority = Priority::P0;
            assert!(forwarder
                .handle(MonoTime::ZERO, ForwardEvent::Outbound(queued))
                .is_empty());
        }
        let mut overflow_p0 = packet(a, b, 32);
        overflow_p0.header.priority = Priority::P0;
        assert!(matches!(
            one_action(forwarder.handle(
                MonoTime::ZERO,
                ForwardEvent::Outbound(overflow_p0)
            )),
            ForwardAction::Backpressure { link: target, .. } if target == link
        ));

        for _ in 0..QUEUE_LIMIT_PACKETS[2] {
            let mut queued = packet(a, b, 32);
            queued.header.priority = Priority::P2;
            assert!(forwarder
                .handle(MonoTime::ZERO, ForwardEvent::Outbound(queued))
                .is_empty());
        }
        let mut overflow_p2 = packet(a, b, 32);
        overflow_p2.header.priority = Priority::P2;
        assert!(matches!(
            one_action(forwarder.handle(MonoTime::ZERO, ForwardEvent::Outbound(overflow_p2))),
            ForwardAction::Drop {
                reason: DropReason::QueueFull(Priority::P2),
                ..
            }
        ));

        assert_eq!(
            forwarder
                .queue_snapshot(link)
                .expect("queue must exist")
                .packet_counts,
            [QUEUE_LIMIT_PACKETS[0], 0, QUEUE_LIMIT_PACKETS[2], 0]
        );
    }
}
