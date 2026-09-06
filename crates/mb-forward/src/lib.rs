//! I/O-free overlay packet forwarding.
//!
//! The runtime or simulator supplies route updates and packets as events, then
//! executes the returned actions. This crate never reads clocks or performs I/O.

use mb_control::RouteTable;
use mb_types::{Component, LinkId, MonoTime, NodeId};
use mb_wire::{ForwardPacket, PacketType};
use std::sync::Arc;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DropReason {
    TtlExceeded,
    NoRoute,
    LoopDetected,
    UnsupportedPacketType(PacketType),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ForwardEvent {
    Outbound(ForwardPacket),
    Inbound { link: LinkId, packet: ForwardPacket },
    RoutesUpdated(Arc<RouteTable>),
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
}

pub struct Forwarder {
    me: NodeId,
    routes: Arc<RouteTable>,
}

impl Forwarder {
    pub fn new(me: NodeId, routes: Arc<RouteTable>) -> Self {
        Self { me, routes }
    }

    pub fn node_id(&self) -> NodeId {
        self.me
    }

    pub fn route_table(&self) -> Arc<RouteTable> {
        Arc::clone(&self.routes)
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
}

impl Component for Forwarder {
    type Event = ForwardEvent;
    type Action = ForwardAction;

    fn handle(&mut self, _now: MonoTime, event: Self::Event) -> Vec<Self::Action> {
        match event {
            ForwardEvent::Outbound(packet) => vec![self.forward(None, packet)],
            ForwardEvent::Inbound { link, packet } => vec![self.forward(Some(link), packet)],
            ForwardEvent::RoutesUpdated(routes) => {
                self.routes = routes;
                Vec::new()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mb_control::Route;
    use mb_wire::{ForwardFlags, ForwardHeader, Priority};
    use std::collections::BTreeMap;

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
        fn run() -> Vec<ForwardAction> {
            let [a, b] = [node(1), node(2)];
            let link = LinkId::new(1);
            let routes = route_table([(b, link, 10, 1)]);
            let mut forwarder = Forwarder::new(a, Arc::new(RouteTable::default()));
            let mut actions = forwarder.handle(MonoTime::ZERO, ForwardEvent::RoutesUpdated(routes));
            actions.extend(forwarder.handle(
                MonoTime::from_millis(1),
                ForwardEvent::Outbound(packet(a, b, 32)),
            ));
            actions
        }

        assert_eq!(run(), run());
    }
}
