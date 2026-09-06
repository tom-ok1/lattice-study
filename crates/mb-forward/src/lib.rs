//! I/O-free overlay packet forwarding.
//!
//! The runtime or simulator supplies route updates and packets as events, then
//! executes the returned actions. This crate never reads clocks or performs I/O.

use bytes::Bytes;
use mb_control::RouteTable;
use mb_types::{Component, LinkId, MonoTime, NodeId};
use mb_wire::{
    ForwardFlags, ForwardPacket, MulticastPayload, MulticastPayloadError, PacketType, Priority,
};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::Arc;

pub const QUEUE_LIMIT_PACKETS: [usize; 4] = [256, 512, 1_024, 4_096];
pub const DRR_QUANTUM_BYTES: [usize; 3] = [14 * 1_024, 5 * 1_024, 1_024];
pub const P0_ROUTE_WAIT_MS: u64 = 2_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DropReason {
    TtlExceeded,
    NoRoute,
    LoopDetected,
    LinkDown,
    RouteWaitExpired,
    InvalidMulticast(MulticastPayloadError),
    UnsupportedPacketType(PacketType),
    QueueFull(Priority),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ForwardEvent {
    Outbound(ForwardPacket),
    OutboundMulticast {
        destinations: Vec<NodeId>,
        packet: ForwardPacket,
    },
    Inbound {
        link: LinkId,
        packet: ForwardPacket,
    },
    RoutesUpdated(Arc<RouteTable>),
    /// Adds byte capacity available to Forward packets on this link.
    LinkCredit {
        link: LinkId,
        bytes: usize,
    },
    /// Grants permission to start at most one Forward-packet write.
    LinkWritable(LinkId),
    LinkDown(LinkId),
    Timer(ForwardTimer),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ForwardTimer {
    PendingP0 { generation: u64 },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ForwardAction {
    Send {
        link: LinkId,
        packet: ForwardPacket,
    },
    /// For multicast, the routing destination prefix has been removed and the
    /// payload contains only the opaque application body.
    DeliverLocal(ForwardPacket),
    Drop {
        reason: DropReason,
        packet: ForwardPacket,
    },
    Backpressure {
        link: LinkId,
        packet: ForwardPacket,
    },
    SetTimer {
        timer: ForwardTimer,
        at: MonoTime,
    },
}

#[cfg(test)]
#[derive(Clone, Debug, Eq, PartialEq)]
struct LinkQueueSnapshot {
    pub available_credit_bytes: usize,
    pub packet_counts: [usize; 4],
    pub queued_bytes: [usize; 4],
    pub deficit_bytes: [usize; 3],
    pub next_drr_priority: Priority,
}

struct LinkQueues {
    queues: [VecDeque<QueuedPacket>; 4],
    queued_bytes: [usize; 4],
    available_credit_bytes: usize,
    writable: bool,
    deficit_bytes: [usize; 3],
    next_drr: usize,
    drr_needs_quantum: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct QueuedPacket {
    incoming: Option<LinkId>,
    packet: ForwardPacket,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct PendingP0Batch {
    expires_at: MonoTime,
    packets: VecDeque<QueuedPacket>,
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
            writable: false,
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
    down_links: BTreeSet<LinkId>,
    pending_p0: BTreeMap<u64, PendingP0Batch>,
    next_pending_generation: u64,
}

impl Forwarder {
    pub fn new(me: NodeId, routes: Arc<RouteTable>) -> Self {
        Self {
            me,
            routes,
            links: BTreeMap::new(),
            down_links: BTreeSet::new(),
            pending_p0: BTreeMap::new(),
            next_pending_generation: 0,
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.me
    }

    pub fn route_table(&self) -> Arc<RouteTable> {
        Arc::clone(&self.routes)
    }

    pub fn pending_p0_len(&self) -> usize {
        self.pending_p0
            .values()
            .map(|batch| batch.packets.len())
            .sum()
    }

    #[cfg(test)]
    fn queue_snapshot(&self, link: LinkId) -> Option<LinkQueueSnapshot> {
        self.links.get(&link).map(|state| LinkQueueSnapshot {
            available_credit_bytes: state.available_credit_bytes,
            packet_counts: std::array::from_fn(|index| state.queues[index].len()),
            queued_bytes: state.queued_bytes,
            deficit_bytes: state.deficit_bytes,
            next_drr_priority: drr_priority(state.next_drr),
        })
    }

    fn forward_unicast(
        &self,
        incoming: Option<LinkId>,
        mut packet: ForwardPacket,
    ) -> ForwardAction {
        if packet.header.destination == self.me {
            return ForwardAction::DeliverLocal(packet);
        }

        let Some(route) = self.routes.get(&packet.header.destination) else {
            return ForwardAction::Drop {
                reason: DropReason::NoRoute,
                packet,
            };
        };
        if self.down_links.contains(&route.next_hop) {
            return ForwardAction::Drop {
                reason: DropReason::NoRoute,
                packet,
            };
        }
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

    fn process_outbound_multicast(
        &mut self,
        destinations: Vec<NodeId>,
        mut packet: ForwardPacket,
    ) -> Vec<ForwardAction> {
        packet.header.packet_type = PacketType::Multicast;
        packet.header.destination = NodeId::default();
        let multicast = match MulticastPayload::new(destinations, packet.payload.clone()) {
            Ok(multicast) => multicast,
            Err(error) => {
                return vec![ForwardAction::Drop {
                    reason: DropReason::InvalidMulticast(error),
                    packet,
                }];
            }
        };
        (packet.multicast_destinations, packet.payload) = multicast.into_parts();
        self.process_packet(None, packet)
    }

    fn process_packet(
        &mut self,
        incoming: Option<LinkId>,
        packet: ForwardPacket,
    ) -> Vec<ForwardAction> {
        if packet.header.ttl == 0 {
            return vec![ForwardAction::Drop {
                reason: DropReason::TtlExceeded,
                packet,
            }];
        }

        match packet.header.packet_type {
            PacketType::Unicast => match self.forward_unicast(incoming, packet) {
                ForwardAction::Send { link, packet } => self.enqueue(link, packet, incoming),
                action => vec![action],
            },
            PacketType::Multicast => self.forward_multicast(incoming, packet),
            unsupported => vec![ForwardAction::Drop {
                reason: DropReason::UnsupportedPacketType(unsupported),
                packet,
            }],
        }
    }

    fn forward_multicast(
        &mut self,
        incoming: Option<LinkId>,
        packet: ForwardPacket,
    ) -> Vec<ForwardAction> {
        let multicast = match MulticastPayload::new(
            packet.multicast_destinations.clone(),
            packet.payload.clone(),
        ) {
            Ok(multicast) => multicast,
            Err(error) => {
                return vec![ForwardAction::Drop {
                    reason: DropReason::InvalidMulticast(error),
                    packet,
                }];
            }
        };
        let (destinations, body) = multicast.into_parts();
        let mut deliver_local = false;
        let mut no_route = Vec::new();
        let mut looped = Vec::new();
        let mut by_link: BTreeMap<LinkId, Vec<NodeId>> = BTreeMap::new();

        for destination in destinations {
            if destination == self.me {
                deliver_local = true;
                continue;
            }
            let Some(route) = self.routes.get(&destination) else {
                no_route.push(destination);
                continue;
            };
            if self.down_links.contains(&route.next_hop) {
                no_route.push(destination);
                continue;
            }
            if incoming == Some(route.next_hop) {
                looped.push(destination);
                continue;
            }
            by_link.entry(route.next_hop).or_default().push(destination);
        }

        let mut actions = Vec::new();
        if deliver_local {
            let mut local = packet.clone();
            local.header.destination = self.me;
            local.multicast_destinations.clear();
            local.payload = body.clone();
            actions.push(ForwardAction::DeliverLocal(local));
        }
        if !no_route.is_empty() {
            actions.push(ForwardAction::Drop {
                reason: DropReason::NoRoute,
                packet: multicast_subset_packet(&packet, no_route, body.clone(), packet.header.ttl),
            });
        }
        if !looped.is_empty() {
            actions.push(ForwardAction::Drop {
                reason: DropReason::LoopDetected,
                packet: multicast_subset_packet(&packet, looped, body.clone(), packet.header.ttl),
            });
        }

        let next_ttl = packet.header.ttl - 1;
        for (link, destinations) in by_link {
            let branch = multicast_subset_packet(&packet, destinations, body.clone(), next_ttl);
            actions.extend(self.enqueue(link, branch, incoming));
        }
        actions
    }

    fn enqueue(
        &mut self,
        link: LinkId,
        packet: ForwardPacket,
        incoming: Option<LinkId>,
    ) -> Vec<ForwardAction> {
        if self.down_links.contains(&link) {
            return vec![ForwardAction::Drop {
                reason: DropReason::LinkDown,
                packet,
            }];
        }
        let priority = packet.header.priority;
        let queue_index = priority_index(priority);
        let state = self.links.entry(link).or_default();

        if let Some(key) = ConflationKey::from_packet(&packet) {
            if let Some(queued) = state.queues[queue_index]
                .iter_mut()
                .find(|queued| ConflationKey::from_packet(&queued.packet) == Some(key))
            {
                let previous_len = queued.packet.encoded_len();
                let replacement_len = packet.encoded_len();
                *queued = QueuedPacket { incoming, packet };
                state.queued_bytes[queue_index] = state.queued_bytes[queue_index]
                    .saturating_sub(previous_len)
                    .saturating_add(replacement_len);
                return self.try_send_one(link);
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
        state.queues[queue_index].push_back(QueuedPacket { incoming, packet });
        self.try_send_one(link)
    }

    fn grant_credit(&mut self, link: LinkId, bytes: usize) -> Vec<ForwardAction> {
        if self.down_links.contains(&link) {
            return Vec::new();
        }
        let state = self.links.entry(link).or_default();
        state.available_credit_bytes = state.available_credit_bytes.saturating_add(bytes);
        self.try_send_one(link)
    }

    fn mark_writable(&mut self, link: LinkId) -> Vec<ForwardAction> {
        if self.down_links.contains(&link) {
            return Vec::new();
        }
        self.links.entry(link).or_default().writable = true;
        self.try_send_one(link)
    }

    fn try_send_one(&mut self, link: LinkId) -> Vec<ForwardAction> {
        let Some(state) = self.links.get_mut(&link) else {
            return Vec::new();
        };
        if !state.writable {
            return Vec::new();
        }

        if let Some(packet_len) = state.queues[0]
            .front()
            .map(|queued| queued.packet.encoded_len())
        {
            if packet_len > state.available_credit_bytes {
                return Vec::new();
            }
            let queued = state.queues[0]
                .pop_front()
                .expect("P0 queue front was checked");
            state.queued_bytes[0] = state.queued_bytes[0].saturating_sub(packet_len);
            state.available_credit_bytes -= packet_len;
            state.writable = false;
            return vec![ForwardAction::Send {
                link,
                packet: queued.packet,
            }];
        }

        loop {
            let a_packet_fits_credit = state.queues[1..].iter().any(|queue| {
                queue.front().is_some_and(|queued| {
                    queued.packet.encoded_len() <= state.available_credit_bytes
                })
            });
            if !a_packet_fits_credit {
                return Vec::new();
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
                .packet
                .encoded_len();
            if packet_len > state.deficit_bytes[drr_index]
                || packet_len > state.available_credit_bytes
            {
                advance_drr(state);
                continue;
            }

            let queued = state.queues[queue_index]
                .pop_front()
                .expect("DRR queue front was checked");
            state.deficit_bytes[drr_index] -= packet_len;
            state.queued_bytes[queue_index] =
                state.queued_bytes[queue_index].saturating_sub(packet_len);
            state.available_credit_bytes -= packet_len;
            state.writable = false;
            if state.queues[queue_index].is_empty() {
                state.deficit_bytes[drr_index] = 0;
                advance_drr(state);
            }
            return vec![ForwardAction::Send {
                link,
                packet: queued.packet,
            }];
        }
    }

    fn handle_link_down(&mut self, now: MonoTime, link: LinkId) -> Vec<ForwardAction> {
        self.down_links.insert(link);
        let Some(state) = self.links.remove(&link) else {
            return Vec::new();
        };
        let [p0, p1, p2, p3] = state.queues;
        let mut actions = Vec::new();

        if !p0.is_empty() {
            let generation = self.next_pending_generation();
            let expires_at =
                MonoTime::from_millis(now.as_millis().saturating_add(P0_ROUTE_WAIT_MS));
            self.pending_p0.insert(
                generation,
                PendingP0Batch {
                    expires_at,
                    packets: p0,
                },
            );
            actions.push(ForwardAction::SetTimer {
                timer: ForwardTimer::PendingP0 { generation },
                at: expires_at,
            });
        }

        actions.extend(drop_queued_packets(p1, DropReason::LinkDown));
        for queued in p2 {
            let (mut routed, unresolved) = self.reroute_queued(queued);
            actions.append(&mut routed);
            actions.extend(unresolved.into_iter().map(|queued| ForwardAction::Drop {
                reason: DropReason::LinkDown,
                packet: queued.packet,
            }));
        }
        actions.extend(drop_queued_packets(p3, DropReason::LinkDown));
        actions
    }

    fn next_pending_generation(&mut self) -> u64 {
        loop {
            let generation = self.next_pending_generation;
            self.next_pending_generation = self.next_pending_generation.wrapping_add(1);
            if !self.pending_p0.contains_key(&generation) {
                return generation;
            }
        }
    }

    fn handle_routes_updated(
        &mut self,
        now: MonoTime,
        routes: Arc<RouteTable>,
    ) -> Vec<ForwardAction> {
        self.routes = routes;
        let referenced_links = self
            .routes
            .iter()
            .map(|(_, route)| route.next_hop)
            .collect::<BTreeSet<_>>();
        self.down_links
            .retain(|link| referenced_links.contains(link));
        self.retry_pending_p0(now)
    }

    fn retry_pending_p0(&mut self, now: MonoTime) -> Vec<ForwardAction> {
        let batches = std::mem::take(&mut self.pending_p0);
        let mut actions = Vec::new();

        for (generation, batch) in batches {
            if now >= batch.expires_at {
                actions.extend(drop_queued_packets(
                    batch.packets,
                    DropReason::RouteWaitExpired,
                ));
                continue;
            }

            let mut remaining = VecDeque::new();
            for queued in batch.packets {
                let (mut routed, unresolved) = self.reroute_queued(queued);
                actions.append(&mut routed);
                remaining.extend(unresolved);
            }
            if !remaining.is_empty() {
                self.pending_p0.insert(
                    generation,
                    PendingP0Batch {
                        expires_at: batch.expires_at,
                        packets: remaining,
                    },
                );
            }
        }
        actions
    }

    fn handle_timer(&mut self, now: MonoTime, timer: ForwardTimer) -> Vec<ForwardAction> {
        match timer {
            ForwardTimer::PendingP0 { generation } => {
                let Some(batch) = self.pending_p0.remove(&generation) else {
                    return Vec::new();
                };
                if now < batch.expires_at {
                    let expires_at = batch.expires_at;
                    self.pending_p0.insert(generation, batch);
                    return vec![ForwardAction::SetTimer {
                        timer,
                        at: expires_at,
                    }];
                }
                drop_queued_packets(batch.packets, DropReason::RouteWaitExpired)
            }
        }
    }

    fn reroute_queued(&mut self, queued: QueuedPacket) -> (Vec<ForwardAction>, Vec<QueuedPacket>) {
        match queued.packet.header.packet_type {
            PacketType::Unicast => self.reroute_unicast(queued),
            PacketType::Multicast => self.reroute_multicast(queued),
            unsupported => (
                vec![ForwardAction::Drop {
                    reason: DropReason::UnsupportedPacketType(unsupported),
                    packet: queued.packet,
                }],
                Vec::new(),
            ),
        }
    }

    fn reroute_unicast(&mut self, queued: QueuedPacket) -> (Vec<ForwardAction>, Vec<QueuedPacket>) {
        if queued.packet.header.destination == self.me {
            return (vec![ForwardAction::DeliverLocal(queued.packet)], Vec::new());
        }
        let Some(link) = self.reroute_link(queued.packet.header.destination, queued.incoming)
        else {
            return (Vec::new(), vec![queued]);
        };
        (
            self.enqueue(link, queued.packet, queued.incoming),
            Vec::new(),
        )
    }

    fn reroute_multicast(
        &mut self,
        queued: QueuedPacket,
    ) -> (Vec<ForwardAction>, Vec<QueuedPacket>) {
        let multicast = match MulticastPayload::new(
            queued.packet.multicast_destinations.clone(),
            queued.packet.payload.clone(),
        ) {
            Ok(multicast) => multicast,
            Err(error) => {
                return (
                    vec![ForwardAction::Drop {
                        reason: DropReason::InvalidMulticast(error),
                        packet: queued.packet,
                    }],
                    Vec::new(),
                );
            }
        };
        let (destinations, body) = multicast.into_parts();
        let mut deliver_local = false;
        let mut unresolved = Vec::new();
        let mut by_link: BTreeMap<LinkId, Vec<NodeId>> = BTreeMap::new();

        for destination in destinations {
            if destination == self.me {
                deliver_local = true;
            } else if let Some(link) = self.reroute_link(destination, queued.incoming) {
                by_link.entry(link).or_default().push(destination);
            } else {
                unresolved.push(destination);
            }
        }

        let mut actions = Vec::new();
        if deliver_local {
            let mut local = queued.packet.clone();
            local.header.destination = self.me;
            local.multicast_destinations.clear();
            local.payload = body.clone();
            actions.push(ForwardAction::DeliverLocal(local));
        }
        for (link, destinations) in by_link {
            let packet = multicast_subset_packet(
                &queued.packet,
                destinations,
                body.clone(),
                queued.packet.header.ttl,
            );
            actions.extend(self.enqueue(link, packet, queued.incoming));
        }

        let unresolved = if unresolved.is_empty() {
            Vec::new()
        } else {
            vec![QueuedPacket {
                incoming: queued.incoming,
                packet: multicast_subset_packet(
                    &queued.packet,
                    unresolved,
                    body,
                    queued.packet.header.ttl,
                ),
            }]
        };
        (actions, unresolved)
    }

    fn reroute_link(&self, destination: NodeId, incoming: Option<LinkId>) -> Option<LinkId> {
        let link = self.routes.get(&destination)?.next_hop;
        (!self.down_links.contains(&link) && incoming != Some(link)).then_some(link)
    }
}

fn drop_queued_packets(packets: VecDeque<QueuedPacket>, reason: DropReason) -> Vec<ForwardAction> {
    packets
        .into_iter()
        .map(|queued| ForwardAction::Drop {
            reason,
            packet: queued.packet,
        })
        .collect()
}

fn multicast_subset_packet(
    template: &ForwardPacket,
    destinations: Vec<NodeId>,
    body: Bytes,
    ttl: u8,
) -> ForwardPacket {
    let multicast = MulticastPayload::new(destinations, body)
        .expect("a non-empty subset of a validated multicast payload remains valid");
    let mut packet = template.clone();
    packet.header.packet_type = PacketType::Multicast;
    packet.header.destination = NodeId::default();
    packet.header.ttl = ttl;
    (packet.multicast_destinations, packet.payload) = multicast.into_parts();
    packet
}

fn priority_index(priority: Priority) -> usize {
    priority as usize
}

#[cfg(test)]
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

    fn handle(&mut self, now: MonoTime, event: Self::Event) -> Vec<Self::Action> {
        match event {
            ForwardEvent::Outbound(packet) => self.process_packet(None, packet),
            ForwardEvent::OutboundMulticast {
                destinations,
                packet,
            } => self.process_outbound_multicast(destinations, packet),
            ForwardEvent::Inbound { link, packet } => self.process_packet(Some(link), packet),
            ForwardEvent::RoutesUpdated(routes) => self.handle_routes_updated(now, routes),
            ForwardEvent::LinkCredit { link, bytes } => self.grant_credit(link, bytes),
            ForwardEvent::LinkWritable(link) => self.mark_writable(link),
            ForwardEvent::LinkDown(link) => self.handle_link_down(now, link),
            ForwardEvent::Timer(timer) => self.handle_timer(now, timer),
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
        route_table_version(1, entries)
    }

    fn route_table_version(
        version: u64,
        entries: impl IntoIterator<Item = (NodeId, LinkId, u32, u8)>,
    ) -> Arc<RouteTable> {
        Arc::new(RouteTable::new(
            version,
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
            multicast_destinations: Vec::new(),
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

    fn multicast_template(source: NodeId, ttl: u8, body: Bytes) -> ForwardPacket {
        let mut packet = packet(source, NodeId::default(), ttl);
        packet.payload = body;
        packet
    }

    fn encoded_multicast_packet(
        source: NodeId,
        ttl: u8,
        destinations: Vec<NodeId>,
        body: Bytes,
    ) -> ForwardPacket {
        let mut packet = multicast_template(source, ttl, Bytes::new());
        packet.header.packet_type = PacketType::Multicast;
        let multicast =
            MulticastPayload::new(destinations, body).expect("valid multicast test payload");
        (packet.multicast_destinations, packet.payload) = multicast.into_parts();
        packet
    }

    fn decode_multicast(packet: &ForwardPacket) -> MulticastPayload {
        assert_eq!(packet.header.packet_type, PacketType::Multicast);
        MulticastPayload::new(
            packet.multicast_destinations.clone(),
            packet.payload.clone(),
        )
        .expect("forwarded multicast payload must be valid")
    }

    fn grant_unlimited_credit(forwarder: &mut Forwarder, link: LinkId) {
        assert!(forwarder
            .handle(
                MonoTime::ZERO,
                ForwardEvent::LinkCredit {
                    link,
                    bytes: usize::MAX,
                },
            )
            .is_empty());
        assert!(forwarder
            .handle(MonoTime::ZERO, ForwardEvent::LinkWritable(link))
            .is_empty());
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
        assert!(forwarder_a
            .handle(MonoTime::ZERO, ForwardEvent::LinkWritable(a_to_b))
            .is_empty());
        assert!(forwarder_b
            .handle(MonoTime::ZERO, ForwardEvent::LinkWritable(b_to_c))
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
    fn multicast_fans_out_once_per_link_and_delivers_every_destination() {
        let [source, branch, a, b, c] = [node(1), node(2), node(3), node(4), node(5)];
        let [source_to_branch, source_to_c, branch_from_source, branch_to_a, branch_to_b] = [
            LinkId::new(1),
            LinkId::new(2),
            LinkId::new(3),
            LinkId::new(4),
            LinkId::new(5),
        ];
        let mut at_source = Forwarder::new(
            source,
            route_table([
                (branch, source_to_branch, 10, 1),
                (a, source_to_branch, 20, 2),
                (b, source_to_branch, 20, 2),
                (c, source_to_c, 10, 1),
            ]),
        );
        let mut at_branch = Forwarder::new(
            branch,
            route_table([(a, branch_to_a, 10, 1), (b, branch_to_b, 10, 1)]),
        );
        let mut at_a = Forwarder::new(a, Arc::new(RouteTable::default()));
        let mut at_b = Forwarder::new(b, Arc::new(RouteTable::default()));
        let mut at_c = Forwarder::new(c, Arc::new(RouteTable::default()));
        let body = Bytes::from_static(b"one body for every subscriber");

        grant_unlimited_credit(&mut at_source, source_to_branch);
        grant_unlimited_credit(&mut at_source, source_to_c);
        grant_unlimited_credit(&mut at_branch, branch_to_a);
        grant_unlimited_credit(&mut at_branch, branch_to_b);

        let source_actions = at_source.handle(
            MonoTime::ZERO,
            ForwardEvent::OutboundMulticast {
                destinations: vec![c, b, branch, a],
                packet: multicast_template(source, 32, body.clone()),
            },
        );
        assert_eq!(source_actions.len(), 2);
        let to_branch = expect_send(source_actions[0].clone(), source_to_branch);
        let to_c = expect_send(source_actions[1].clone(), source_to_c);
        assert_eq!(to_branch.header.ttl, 31);
        assert_eq!(decode_multicast(&to_branch).destinations(), &[branch, a, b]);
        assert_eq!(decode_multicast(&to_c).destinations(), &[c]);
        assert_eq!(to_branch.payload.as_ptr(), body.as_ptr());
        assert_eq!(to_c.payload.as_ptr(), body.as_ptr());

        let branch_actions = at_branch.handle(
            MonoTime::from_millis(1),
            ForwardEvent::Inbound {
                link: branch_from_source,
                packet: to_branch,
            },
        );
        assert_eq!(branch_actions.len(), 3);
        match &branch_actions[0] {
            ForwardAction::DeliverLocal(packet) => {
                assert_eq!(packet.header.destination, branch);
                assert_eq!(packet.payload, body);
            }
            other => panic!("expected branch-local delivery, got {other:?}"),
        }
        let to_a = expect_send(branch_actions[1].clone(), branch_to_a);
        let to_b = expect_send(branch_actions[2].clone(), branch_to_b);
        assert_eq!(decode_multicast(&to_a).destinations(), &[a]);
        assert_eq!(decode_multicast(&to_b).destinations(), &[b]);
        assert_eq!(to_a.payload.as_ptr(), body.as_ptr());
        assert_eq!(to_b.payload.as_ptr(), body.as_ptr());

        for (forwarder, incoming, packet, destination) in [
            (&mut at_a, LinkId::new(6), to_a, a),
            (&mut at_b, LinkId::new(7), to_b, b),
            (&mut at_c, LinkId::new(8), to_c, c),
        ] {
            match one_action(forwarder.handle(
                MonoTime::from_millis(2),
                ForwardEvent::Inbound {
                    link: incoming,
                    packet,
                },
            )) {
                ForwardAction::DeliverLocal(packet) => {
                    assert_eq!(packet.header.destination, destination);
                    assert_eq!(packet.payload, body);
                }
                other => panic!("expected leaf-local delivery, got {other:?}"),
            }
        }
    }

    #[test]
    fn multicast_isolates_local_no_route_loop_and_forwarded_destinations() {
        let [source, me, looped, forwarded, unreachable] =
            [node(1), node(2), node(3), node(4), node(5)];
        let [incoming, outgoing] = [LinkId::new(10), LinkId::new(20)];
        let mut forwarder = Forwarder::new(
            me,
            route_table([(looped, incoming, 10, 1), (forwarded, outgoing, 10, 1)]),
        );
        assert!(forwarder
            .handle(
                MonoTime::ZERO,
                ForwardEvent::LinkCredit {
                    link: outgoing,
                    bytes: usize::MAX,
                },
            )
            .is_empty());
        assert!(forwarder
            .handle(MonoTime::ZERO, ForwardEvent::LinkWritable(outgoing))
            .is_empty());
        let body = Bytes::from_static(b"isolated body");
        let inbound = encoded_multicast_packet(
            source,
            8,
            vec![forwarded, unreachable, me, looped],
            body.clone(),
        );

        let actions = forwarder.handle(
            MonoTime::ZERO,
            ForwardEvent::Inbound {
                link: incoming,
                packet: inbound,
            },
        );

        assert_eq!(actions.len(), 4);
        assert!(matches!(
            &actions[0],
            ForwardAction::DeliverLocal(packet) if packet.payload == body
        ));
        match &actions[1] {
            ForwardAction::Drop {
                reason: DropReason::NoRoute,
                packet,
            } => assert_eq!(decode_multicast(packet).destinations(), &[unreachable]),
            other => panic!("expected isolated no-route drop, got {other:?}"),
        }
        match &actions[2] {
            ForwardAction::Drop {
                reason: DropReason::LoopDetected,
                packet,
            } => assert_eq!(decode_multicast(packet).destinations(), &[looped]),
            other => panic!("expected isolated loop drop, got {other:?}"),
        }
        let sent = expect_send(actions[3].clone(), outgoing);
        assert_eq!(sent.header.ttl, 7);
        assert_eq!(decode_multicast(&sent).destinations(), &[forwarded]);
    }

    #[test]
    fn multicast_destination_order_does_not_change_actions() {
        fn run(destinations: Vec<NodeId>) -> Vec<ForwardAction> {
            let [source, a, b, c] = [node(1), node(2), node(3), node(4)];
            let [first, second] = [LinkId::new(1), LinkId::new(2)];
            let mut forwarder = Forwarder::new(
                source,
                route_table([(a, first, 10, 1), (b, first, 10, 1), (c, second, 10, 1)]),
            );
            for link in [first, second] {
                assert!(forwarder
                    .handle(
                        MonoTime::ZERO,
                        ForwardEvent::LinkCredit {
                            link,
                            bytes: usize::MAX,
                        },
                    )
                    .is_empty());
                assert!(forwarder
                    .handle(MonoTime::ZERO, ForwardEvent::LinkWritable(link))
                    .is_empty());
            }
            forwarder.handle(
                MonoTime::ZERO,
                ForwardEvent::OutboundMulticast {
                    destinations,
                    packet: multicast_template(source, 32, Bytes::from_static(b"deterministic")),
                },
            )
        }

        assert_eq!(
            run(vec![node(4), node(2), node(3)]),
            run(vec![node(3), node(4), node(2)])
        );
    }

    #[test]
    fn invalid_outbound_multicast_sets_are_dropped_explicitly() {
        let source = node(1);
        let mut forwarder = Forwarder::new(source, Arc::new(RouteTable::default()));
        let invalid_sets = [
            (Vec::new(), MulticastPayloadError::EmptyDestinations),
            (
                vec![node(2), node(2)],
                MulticastPayloadError::DuplicateDestination(node(2)),
            ),
            (
                (0..=mb_wire::MAX_MULTICAST_DESTINATIONS)
                    .map(|value| node(value as u8))
                    .collect(),
                MulticastPayloadError::TooManyDestinations(65),
            ),
        ];

        for (destinations, expected) in invalid_sets {
            assert!(matches!(
                one_action(forwarder.handle(
                    MonoTime::ZERO,
                    ForwardEvent::OutboundMulticast {
                        destinations,
                        packet: multicast_template(source, 32, Bytes::new()),
                    },
                )),
                ForwardAction::Drop {
                    reason: DropReason::InvalidMulticast(error),
                    ..
                } if error == expected
            ));
        }
    }

    #[test]
    fn link_down_removes_link_state_and_applies_priority_policies() {
        let [me, destination] = [node(1), node(2)];
        let failed = LinkId::new(10);
        let mut forwarder = Forwarder::new(me, route_table([(destination, failed, 10, 1)]));

        for (flow_id, priority) in [
            (0, Priority::P0),
            (1, Priority::P1),
            (2, Priority::P2),
            (3, Priority::P3),
        ] {
            let mut queued = packet(me, destination, 32);
            queued.header.flow_id = flow_id;
            queued.header.priority = priority;
            assert!(forwarder
                .handle(MonoTime::ZERO, ForwardEvent::Outbound(queued))
                .is_empty());
        }

        let actions = forwarder.handle(MonoTime::from_millis(10), ForwardEvent::LinkDown(failed));

        assert_eq!(actions.len(), 4);
        assert_eq!(
            actions[0],
            ForwardAction::SetTimer {
                timer: ForwardTimer::PendingP0 { generation: 0 },
                at: MonoTime::from_millis(2_010),
            }
        );
        for (action, priority) in
            actions[1..]
                .iter()
                .zip([Priority::P1, Priority::P2, Priority::P3])
        {
            assert!(matches!(
                action,
                ForwardAction::Drop {
                    reason: DropReason::LinkDown,
                    packet,
                } if packet.header.priority == priority
            ));
        }
        assert_eq!(forwarder.pending_p0_len(), 1);
        assert_eq!(forwarder.queue_snapshot(failed), None);
        assert!(forwarder
            .handle(
                MonoTime::from_millis(11),
                ForwardEvent::LinkCredit {
                    link: failed,
                    bytes: usize::MAX,
                },
            )
            .is_empty());
        assert!(forwarder
            .handle(MonoTime::from_millis(11), ForwardEvent::LinkDown(failed),)
            .is_empty());

        let mut after_down = packet(me, destination, 32);
        after_down.header.priority = Priority::P2;
        assert!(matches!(
            one_action(forwarder.handle(
                MonoTime::from_millis(12),
                ForwardEvent::Outbound(after_down),
            )),
            ForwardAction::Drop {
                reason: DropReason::NoRoute,
                ..
            }
        ));
    }

    #[test]
    fn link_down_reroutes_p2_without_decrementing_ttl_twice() {
        let [me, destination] = [node(1), node(2)];
        let [failed, alternate] = [LinkId::new(10), LinkId::new(20)];
        let mut forwarder = Forwarder::new(me, route_table([(destination, failed, 10, 1)]));
        let mut queued = packet(me, destination, 32);
        queued.header.priority = Priority::P2;

        assert!(forwarder
            .handle(MonoTime::ZERO, ForwardEvent::Outbound(queued))
            .is_empty());
        assert!(forwarder
            .handle(
                MonoTime::from_millis(1),
                ForwardEvent::RoutesUpdated(route_table_version(
                    2,
                    [(destination, alternate, 20, 2)],
                )),
            )
            .is_empty());
        grant_unlimited_credit(&mut forwarder, alternate);

        let sent = expect_send(
            one_action(forwarder.handle(MonoTime::from_millis(2), ForwardEvent::LinkDown(failed))),
            alternate,
        );

        assert_eq!(sent.header.priority, Priority::P2);
        assert_eq!(sent.header.ttl, 31);
        assert_eq!(forwarder.queue_snapshot(failed), None);
    }

    #[test]
    fn pending_p0_uses_new_route_and_ignores_its_stale_timer() {
        let [me, destination] = [node(1), node(2)];
        let [failed, alternate] = [LinkId::new(10), LinkId::new(20)];
        let mut forwarder = Forwarder::new(me, route_table([(destination, failed, 10, 1)]));
        let mut queued = packet(me, destination, 32);
        queued.header.priority = Priority::P0;
        assert!(forwarder
            .handle(MonoTime::ZERO, ForwardEvent::Outbound(queued))
            .is_empty());

        let timer = ForwardTimer::PendingP0 { generation: 0 };
        assert_eq!(
            one_action(
                forwarder.handle(MonoTime::from_millis(100), ForwardEvent::LinkDown(failed),)
            ),
            ForwardAction::SetTimer {
                timer,
                at: MonoTime::from_millis(2_100),
            }
        );
        assert_eq!(forwarder.pending_p0_len(), 1);
        assert_eq!(
            one_action(forwarder.handle(MonoTime::from_millis(1_000), ForwardEvent::Timer(timer),)),
            ForwardAction::SetTimer {
                timer,
                at: MonoTime::from_millis(2_100),
            }
        );
        grant_unlimited_credit(&mut forwarder, alternate);

        let sent = expect_send(
            one_action(forwarder.handle(
                MonoTime::from_millis(1_500),
                ForwardEvent::RoutesUpdated(route_table_version(
                    2,
                    [(destination, alternate, 20, 2)],
                )),
            )),
            alternate,
        );
        assert_eq!(sent.header.ttl, 31);
        assert_eq!(sent.header.priority, Priority::P0);
        assert_eq!(forwarder.pending_p0_len(), 0);
        assert!(forwarder
            .handle(MonoTime::from_millis(2_100), ForwardEvent::Timer(timer),)
            .is_empty());
    }

    #[test]
    fn pending_p0_expires_after_two_seconds_in_fifo_order() {
        let [me, destination] = [node(1), node(2)];
        let failed = LinkId::new(10);
        let mut forwarder = Forwarder::new(me, route_table([(destination, failed, 10, 1)]));
        for flow_id in [10, 20] {
            let mut queued = packet(me, destination, 32);
            queued.header.priority = Priority::P0;
            queued.header.flow_id = flow_id;
            assert!(forwarder
                .handle(MonoTime::ZERO, ForwardEvent::Outbound(queued))
                .is_empty());
        }
        let timer = ForwardTimer::PendingP0 { generation: 0 };
        assert_eq!(
            one_action(forwarder.handle(MonoTime::ZERO, ForwardEvent::LinkDown(failed))),
            ForwardAction::SetTimer {
                timer,
                at: MonoTime::from_millis(P0_ROUTE_WAIT_MS),
            }
        );
        assert_eq!(
            one_action(forwarder.handle(
                MonoTime::from_millis(P0_ROUTE_WAIT_MS - 1),
                ForwardEvent::Timer(timer),
            )),
            ForwardAction::SetTimer {
                timer,
                at: MonoTime::from_millis(P0_ROUTE_WAIT_MS),
            }
        );

        let expired = forwarder.handle(
            MonoTime::from_millis(P0_ROUTE_WAIT_MS),
            ForwardEvent::Timer(timer),
        );

        assert_eq!(expired.len(), 2);
        assert_eq!(
            expired
                .iter()
                .map(|action| match action {
                    ForwardAction::Drop {
                        reason: DropReason::RouteWaitExpired,
                        packet,
                    } => packet.header.flow_id,
                    other => panic!("expected expired P0 drop, got {other:?}"),
                })
                .collect::<Vec<_>>(),
            vec![10, 20]
        );
        assert_eq!(forwarder.pending_p0_len(), 0);
    }

    #[test]
    fn pending_multicast_p0_partially_recovers_and_expires_the_unresolved_subset() {
        let [me, a, b, unresolved] = [node(1), node(2), node(3), node(4)];
        let [failed, alternate] = [LinkId::new(10), LinkId::new(20)];
        let mut forwarder = Forwarder::new(
            me,
            route_table([
                (a, failed, 10, 1),
                (b, failed, 10, 1),
                (unresolved, failed, 10, 1),
            ]),
        );
        let body = Bytes::from_static(b"pending multicast body");
        let mut outbound = multicast_template(me, 32, body.clone());
        outbound.header.priority = Priority::P0;
        assert!(forwarder
            .handle(
                MonoTime::ZERO,
                ForwardEvent::OutboundMulticast {
                    destinations: vec![unresolved, b, a],
                    packet: outbound,
                },
            )
            .is_empty());
        let timer = ForwardTimer::PendingP0 { generation: 0 };
        assert_eq!(
            one_action(forwarder.handle(MonoTime::ZERO, ForwardEvent::LinkDown(failed))),
            ForwardAction::SetTimer {
                timer,
                at: MonoTime::from_millis(P0_ROUTE_WAIT_MS),
            }
        );
        grant_unlimited_credit(&mut forwarder, alternate);

        let recovered = expect_send(
            one_action(forwarder.handle(
                MonoTime::from_millis(1_000),
                ForwardEvent::RoutesUpdated(route_table_version(
                    2,
                    [(a, alternate, 20, 2), (b, alternate, 20, 2)],
                )),
            )),
            alternate,
        );
        assert_eq!(decode_multicast(&recovered).destinations(), &[a, b]);
        assert_eq!(recovered.header.ttl, 31);
        assert_eq!(recovered.payload.as_ptr(), body.as_ptr());
        assert_eq!(forwarder.pending_p0_len(), 1);

        match one_action(forwarder.handle(
            MonoTime::from_millis(P0_ROUTE_WAIT_MS),
            ForwardEvent::Timer(timer),
        )) {
            ForwardAction::Drop {
                reason: DropReason::RouteWaitExpired,
                packet,
            } => {
                assert_eq!(decode_multicast(&packet).destinations(), &[unresolved]);
                assert_eq!(packet.payload.as_ptr(), body.as_ptr());
            }
            other => panic!("expected unresolved multicast expiry, got {other:?}"),
        }
        assert_eq!(forwarder.pending_p0_len(), 0);
    }

    #[test]
    fn link_down_regroups_multicast_p2_by_alternate_next_hop() {
        let [me, a, b, c, unreachable] = [node(1), node(2), node(3), node(4), node(5)];
        let [failed, first, second] = [LinkId::new(10), LinkId::new(20), LinkId::new(30)];
        let initial_routes = route_table([
            (a, failed, 10, 1),
            (b, failed, 10, 1),
            (c, failed, 10, 1),
            (unreachable, failed, 10, 1),
        ]);
        let mut forwarder = Forwarder::new(me, initial_routes);
        let body = Bytes::from_static(b"shared rerouted multicast body");
        let mut outbound = multicast_template(me, 32, body.clone());
        outbound.header.priority = Priority::P2;
        assert!(forwarder
            .handle(
                MonoTime::ZERO,
                ForwardEvent::OutboundMulticast {
                    destinations: vec![unreachable, c, b, a],
                    packet: outbound,
                },
            )
            .is_empty());
        assert!(forwarder
            .handle(
                MonoTime::from_millis(1),
                ForwardEvent::RoutesUpdated(route_table_version(
                    2,
                    [(a, first, 20, 2), (b, first, 20, 2), (c, second, 20, 2),],
                )),
            )
            .is_empty());
        grant_unlimited_credit(&mut forwarder, first);
        grant_unlimited_credit(&mut forwarder, second);

        let actions = forwarder.handle(MonoTime::from_millis(2), ForwardEvent::LinkDown(failed));

        assert_eq!(actions.len(), 3);
        let to_first = expect_send(actions[0].clone(), first);
        let to_second = expect_send(actions[1].clone(), second);
        assert_eq!(decode_multicast(&to_first).destinations(), &[a, b]);
        assert_eq!(decode_multicast(&to_second).destinations(), &[c]);
        assert_eq!(to_first.header.ttl, 31);
        assert_eq!(to_second.header.ttl, 31);
        assert_eq!(to_first.payload.as_ptr(), body.as_ptr());
        assert_eq!(to_second.payload.as_ptr(), body.as_ptr());
        match &actions[2] {
            ForwardAction::Drop {
                reason: DropReason::LinkDown,
                packet,
            } => assert_eq!(decode_multicast(packet).destinations(), &[unreachable]),
            other => panic!("expected unreachable multicast branch drop, got {other:?}"),
        }
    }

    #[test]
    fn link_down_reroute_uses_existing_queue_overflow_policy() {
        let [me, destination] = [node(1), node(2)];
        let [failed, alternate] = [LinkId::new(10), LinkId::new(20)];
        let mut forwarder = Forwarder::new(me, route_table([(destination, failed, 10, 1)]));
        let mut on_failed = packet(me, destination, 32);
        on_failed.header.priority = Priority::P2;
        on_failed.header.flow_id = u64::MAX;
        assert!(forwarder
            .handle(MonoTime::ZERO, ForwardEvent::Outbound(on_failed))
            .is_empty());
        assert!(forwarder
            .handle(
                MonoTime::from_millis(1),
                ForwardEvent::RoutesUpdated(route_table_version(
                    2,
                    [(destination, alternate, 20, 2)],
                )),
            )
            .is_empty());
        for flow_id in 0..QUEUE_LIMIT_PACKETS[Priority::P2 as usize] as u64 {
            let mut queued = packet(me, destination, 32);
            queued.header.priority = Priority::P2;
            queued.header.flow_id = flow_id;
            assert!(forwarder
                .handle(MonoTime::ZERO, ForwardEvent::Outbound(queued))
                .is_empty());
        }

        assert!(matches!(
            one_action(forwarder.handle(
                MonoTime::from_millis(2),
                ForwardEvent::LinkDown(failed),
            )),
            ForwardAction::Drop {
                reason: DropReason::QueueFull(Priority::P2),
                packet,
            } if packet.header.flow_id == u64::MAX
        ));
        assert_eq!(
            forwarder
                .queue_snapshot(alternate)
                .expect("alternate queue must remain full")
                .packet_counts[Priority::P2 as usize],
            QUEUE_LIMIT_PACKETS[Priority::P2 as usize]
        );
    }

    #[test]
    fn link_down_reroute_never_reflects_a_packet_to_its_incoming_link() {
        let [source, me, destination] = [node(1), node(2), node(3)];
        let [incoming, failed] = [LinkId::new(10), LinkId::new(20)];
        let mut forwarder = Forwarder::new(me, route_table([(destination, failed, 10, 1)]));
        let mut inbound = packet(source, destination, 32);
        inbound.header.priority = Priority::P2;
        assert!(forwarder
            .handle(
                MonoTime::ZERO,
                ForwardEvent::Inbound {
                    link: incoming,
                    packet: inbound,
                },
            )
            .is_empty());
        assert!(forwarder
            .handle(
                MonoTime::from_millis(1),
                ForwardEvent::RoutesUpdated(route_table_version(
                    2,
                    [(destination, incoming, 20, 2)],
                )),
            )
            .is_empty());

        assert!(matches!(
            one_action(forwarder.handle(
                MonoTime::from_millis(2),
                ForwardEvent::LinkDown(failed),
            )),
            ForwardAction::Drop {
                reason: DropReason::LinkDown,
                packet,
            } if packet.header.ttl == 31
        ));
        assert_eq!(forwarder.queue_snapshot(incoming), None);
    }

    #[test]
    fn link_down_processing_is_deterministic() {
        fn run() -> (Vec<ForwardAction>, usize, Option<LinkQueueSnapshot>) {
            let [me, destination] = [node(1), node(2)];
            let [failed, alternate] = [LinkId::new(10), LinkId::new(20)];
            let mut forwarder = Forwarder::new(me, route_table([(destination, failed, 10, 1)]));
            for priority in [Priority::P0, Priority::P1, Priority::P2, Priority::P3] {
                let mut queued = packet(me, destination, 32);
                queued.header.priority = priority;
                assert!(forwarder
                    .handle(MonoTime::ZERO, ForwardEvent::Outbound(queued))
                    .is_empty());
            }
            assert!(forwarder
                .handle(
                    MonoTime::from_millis(1),
                    ForwardEvent::RoutesUpdated(route_table_version(
                        2,
                        [(destination, alternate, 20, 2)],
                    )),
                )
                .is_empty());
            grant_unlimited_credit(&mut forwarder, alternate);
            let actions =
                forwarder.handle(MonoTime::from_millis(2), ForwardEvent::LinkDown(failed));
            (
                actions,
                forwarder.pending_p0_len(),
                forwarder.queue_snapshot(alternate),
            )
        }

        assert_eq!(run(), run());
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

        let mut unsupported = packet(a, c, 32);
        unsupported.header.packet_type = PacketType::CircuitData;
        assert_eq!(
            one_action(
                forwarder.handle(MonoTime::ZERO, ForwardEvent::Outbound(unsupported.clone()),)
            ),
            ForwardAction::Drop {
                reason: DropReason::UnsupportedPacketType(PacketType::CircuitData),
                packet: unsupported,
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
        assert!(forwarder
            .handle(MonoTime::from_millis(1), ForwardEvent::LinkWritable(link),)
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
            actions.extend(forwarder.handle(MonoTime::ZERO, ForwardEvent::LinkWritable(link)));
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
            .handle(MonoTime::ZERO, ForwardEvent::LinkWritable(link))
            .is_empty());

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
    fn one_writable_event_releases_at_most_one_packet() {
        let [a, b] = [node(1), node(2)];
        let link = LinkId::new(1);
        let mut forwarder = Forwarder::new(a, route_table([(b, link, 10, 1)]));
        let first = packet(a, b, 32);
        let mut second = packet(a, b, 32);
        second.header.flow_id = 2;

        for queued in [first.clone(), second.clone()] {
            assert!(forwarder
                .handle(MonoTime::ZERO, ForwardEvent::Outbound(queued))
                .is_empty());
        }
        assert!(forwarder
            .handle(
                MonoTime::ZERO,
                ForwardEvent::LinkCredit {
                    link,
                    bytes: first.encoded_len() + second.encoded_len(),
                },
            )
            .is_empty());

        let sent = expect_send(
            one_action(
                forwarder.handle(MonoTime::from_millis(1), ForwardEvent::LinkWritable(link)),
            ),
            link,
        );
        assert_eq!(sent.header.flow_id, first.header.flow_id);
        assert_eq!(
            forwarder
                .queue_snapshot(link)
                .expect("second packet must remain queued")
                .packet_counts,
            [0, 1, 0, 0]
        );

        let sent = expect_send(
            one_action(
                forwarder.handle(MonoTime::from_millis(2), ForwardEvent::LinkWritable(link)),
            ),
            link,
        );
        assert_eq!(sent.header.flow_id, second.header.flow_id);
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
            assert!(forwarder
                .handle(MonoTime::from_millis(1), ForwardEvent::LinkWritable(link))
                .is_empty());
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

        assert!(forwarder
            .handle(
                MonoTime::from_millis(1),
                ForwardEvent::LinkCredit {
                    link,
                    bytes: replacement.encoded_len() + second.encoded_len(),
                },
            )
            .is_empty());
        let mut actions =
            forwarder.handle(MonoTime::from_millis(1), ForwardEvent::LinkWritable(link));
        actions
            .extend(forwarder.handle(MonoTime::from_millis(2), ForwardEvent::LinkWritable(link)));
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
        assert!(forwarder
            .handle(
                MonoTime::from_millis(1),
                ForwardEvent::LinkCredit {
                    link,
                    bytes: packet_len,
                },
            )
            .is_empty());

        let first = expect_send(
            one_action(
                forwarder.handle(MonoTime::from_millis(1), ForwardEvent::LinkWritable(link)),
            ),
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

        assert!(forwarder
            .handle(
                MonoTime::from_millis(2),
                ForwardEvent::LinkCredit {
                    link,
                    bytes: packet_len,
                },
            )
            .is_empty());
        let second = expect_send(
            one_action(
                forwarder.handle(MonoTime::from_millis(2), ForwardEvent::LinkWritable(link)),
            ),
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

        assert!(forwarder
            .handle(
                MonoTime::from_millis(1),
                ForwardEvent::LinkCredit {
                    link,
                    bytes: total_bytes,
                },
            )
            .is_empty());
        let mut actions = Vec::new();
        for sequence in 0..9 {
            let released = forwarder.handle(
                MonoTime::from_millis(2 + sequence),
                ForwardEvent::LinkWritable(link),
            );
            assert_eq!(released.len(), 1);
            actions.extend(released);
        }
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
