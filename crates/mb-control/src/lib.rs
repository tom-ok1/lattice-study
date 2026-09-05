//! I/O-free link-state control plane.
//!
//! The only way to affect this component is through [`ControlEvent`]. External
//! effects are returned as [`ControlAction`], which lets the same code run
//! under a Tokio runtime or a deterministic simulator.

use mb_types::{Component, LinkId, MonoTime, NodeId};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::sync::Arc;

pub const DIGEST_INTERVAL_MS: u64 = 10_000;

/// Cost of traversing one directed edge.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct LinkCost(u16);

impl LinkCost {
    pub fn new(value: u16) -> Result<Self, InvalidLinkCost> {
        if value == 0 {
            Err(InvalidLinkCost)
        } else {
            Ok(Self(value))
        }
    }

    pub const fn get(self) -> u16 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct InvalidLinkCost;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Adjacency {
    pub peer: NodeId,
    pub cost: LinkCost,
}

/// Minimal LSA used by the first control-plane slice.
///
/// Service, topic, address, and expiry fields can be added without changing
/// the event/action boundary around the control plane.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lsa {
    pub origin: NodeId,
    pub epoch: u32,
    pub seq: u64,
    pub adjacencies: Vec<Adjacency>,
}

/// Transport-neutral LSA container.
///
/// The unsecured implementation leaves `canonical_bytes` and `signature`
/// empty. The later protobuf/security adapter can populate both while keeping
/// forwarding byte-preserving.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LsaMessage {
    pub lsa: Lsa,
    pub canonical_bytes: Arc<[u8]>,
    pub signature: Arc<[u8]>,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct DigestEntry {
    pub origin: NodeId,
    pub epoch: u32,
    pub seq: u64,
}

impl DigestEntry {
    fn version(self) -> (u32, u64) {
        (self.epoch, self.seq)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum EntryRelation {
    RemoteOnly,
    RemoteNewer,
    SameVersion,
    LocalNewer,
    LocalOnly,
}

fn compare_versions(local: Option<(u32, u64)>, remote: Option<(u32, u64)>) -> EntryRelation {
    match (local, remote) {
        (None, Some(_)) => EntryRelation::RemoteOnly,
        (Some(_), None) => EntryRelation::LocalOnly,
        (Some(local), Some(remote)) if remote > local => EntryRelation::RemoteNewer,
        (Some(local), Some(remote)) if local > remote => EntryRelation::LocalNewer,
        (Some(_), Some(_)) => EntryRelation::SameVersion,
        (None, None) => unreachable!("an origin must exist in at least one digest"),
    }
}

pub trait LsaSigner: Send + Sync {
    fn sign(&self, lsa: Lsa) -> LsaMessage;
}

pub trait LsaVerifier: Send + Sync {
    fn verify(&self, message: &LsaMessage) -> bool;
}

/// Explicitly insecure signer/verifier for simulation and the first milestone.
#[derive(Debug, Default)]
pub struct UnsecuredLsaAuth;

impl LsaSigner for UnsecuredLsaAuth {
    fn sign(&self, lsa: Lsa) -> LsaMessage {
        LsaMessage {
            lsa,
            canonical_bytes: Arc::from([]),
            signature: Arc::from([]),
        }
    }
}

impl LsaVerifier for UnsecuredLsaAuth {
    fn verify(&self, _message: &LsaMessage) -> bool {
        true
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlFrame {
    Lsa(LsaMessage),
    Digest(Vec<DigestEntry>),
    DigestReq(Vec<NodeId>),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum ControlTimer {
    Digest(LinkId),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlEvent {
    LinkUp {
        link: LinkId,
        peer: NodeId,
        cost: LinkCost,
    },
    LinkDown {
        link: LinkId,
    },
    Frame {
        link: LinkId,
        frame: ControlFrame,
    },
    Timer(ControlTimer),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlAction {
    Send { link: LinkId, frame: ControlFrame },
    SetTimer { timer: ControlTimer, at: MonoTime },
    PublishRoutes(Arc<RouteTable>),
    PersistSeq(u64),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Route {
    pub next_hop: LinkId,
    pub cost: u32,
    pub hops: u8,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct RouteTable {
    pub version: u64,
    routes: BTreeMap<NodeId, Route>,
}

impl RouteTable {
    pub fn get(&self, destination: &NodeId) -> Option<&Route> {
        self.routes.get(destination)
    }

    pub fn iter(&self) -> impl Iterator<Item = (&NodeId, &Route)> {
        self.routes.iter()
    }

    pub fn len(&self) -> usize {
        self.routes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.routes.is_empty()
    }
}

#[derive(Clone, Debug)]
struct LsdbEntry {
    message: LsaMessage,
    received_at: MonoTime,
}

#[derive(Clone, Copy, Debug)]
struct LocalAdjacency {
    peer: NodeId,
    cost: LinkCost,
}

pub struct ControlPlane {
    me: NodeId,
    epoch: u32,
    my_seq: u64,
    adjacencies: BTreeMap<LinkId, LocalAdjacency>,
    lsdb: BTreeMap<NodeId, LsdbEntry>,
    routes: Arc<RouteTable>,
    signer: Box<dyn LsaSigner>,
    verifier: Box<dyn LsaVerifier>,
}

impl ControlPlane {
    pub fn new_unsecured(me: NodeId, epoch: u32) -> Self {
        Self::with_auth(
            me,
            epoch,
            Box::<UnsecuredLsaAuth>::default(),
            Box::<UnsecuredLsaAuth>::default(),
        )
    }

    pub fn with_auth(
        me: NodeId,
        epoch: u32,
        signer: Box<dyn LsaSigner>,
        verifier: Box<dyn LsaVerifier>,
    ) -> Self {
        Self {
            me,
            epoch,
            my_seq: 0,
            adjacencies: BTreeMap::new(),
            lsdb: BTreeMap::new(),
            routes: Arc::new(RouteTable::default()),
            signer,
            verifier,
        }
    }

    pub fn node_id(&self) -> NodeId {
        self.me
    }

    pub fn route_table(&self) -> Arc<RouteTable> {
        Arc::clone(&self.routes)
    }

    pub fn lsdb_len(&self) -> usize {
        self.lsdb.len()
    }

    pub fn lsa(&self, origin: &NodeId) -> Option<&Lsa> {
        self.lsdb.get(origin).map(|entry| &entry.message.lsa)
    }

    pub fn lsa_received_at(&self, origin: &NodeId) -> Option<MonoTime> {
        self.lsdb.get(origin).map(|entry| entry.received_at)
    }

    fn originate_lsa(&mut self, now: MonoTime) -> Vec<ControlAction> {
        self.my_seq = self
            .my_seq
            .checked_add(1)
            .expect("local LSA sequence exhausted");

        let mut best_by_peer: BTreeMap<NodeId, LinkCost> = BTreeMap::new();
        for adjacency in self.adjacencies.values() {
            best_by_peer
                .entry(adjacency.peer)
                .and_modify(|cost| *cost = (*cost).min(adjacency.cost))
                .or_insert(adjacency.cost);
        }
        let lsa = Lsa {
            origin: self.me,
            epoch: self.epoch,
            seq: self.my_seq,
            adjacencies: best_by_peer
                .into_iter()
                .map(|(peer, cost)| Adjacency { peer, cost })
                .collect(),
        };
        let message = self.signer.sign(lsa);
        self.install(message.clone(), now);

        let mut actions = vec![ControlAction::PersistSeq(self.my_seq)];
        actions.extend(self.flood(&message, None));
        if let Some(action) = self.recompute_routes() {
            actions.push(action);
        }
        actions
    }

    fn receive_lsa(
        &mut self,
        now: MonoTime,
        incoming: LinkId,
        message: LsaMessage,
    ) -> Vec<ControlAction> {
        if !self.adjacencies.contains_key(&incoming) || !self.verifier.verify(&message) {
            return Vec::new();
        }
        if message.lsa.origin == self.me {
            return self
                .lsdb
                .get(&self.me)
                .filter(|entry| {
                    Self::lsa_version(&entry.message.lsa) > Self::lsa_version(&message.lsa)
                })
                .map(|entry| self.send_lsa(incoming, &entry.message))
                .into_iter()
                .collect();
        }
        if !self.is_newer(&message.lsa) {
            return self
                .lsdb
                .get(&message.lsa.origin)
                .filter(|entry| {
                    Self::lsa_version(&entry.message.lsa) > Self::lsa_version(&message.lsa)
                })
                .map(|entry| self.send_lsa(incoming, &entry.message))
                .into_iter()
                .collect();
        }

        self.install(message.clone(), now);
        let mut actions = self.flood(&message, Some(incoming));
        if let Some(action) = self.recompute_routes() {
            actions.push(action);
        }
        actions
    }

    fn is_newer(&self, candidate: &Lsa) -> bool {
        match self.lsdb.get(&candidate.origin) {
            None => true,
            Some(current) => {
                (candidate.epoch, candidate.seq)
                    > (current.message.lsa.epoch, current.message.lsa.seq)
            }
        }
    }

    fn lsa_version(lsa: &Lsa) -> (u32, u64) {
        (lsa.epoch, lsa.seq)
    }

    fn digest(&self) -> Vec<DigestEntry> {
        self.lsdb
            .values()
            .map(|entry| DigestEntry {
                origin: entry.message.lsa.origin,
                epoch: entry.message.lsa.epoch,
                seq: entry.message.lsa.seq,
            })
            .collect()
    }

    fn send_digest(&self, link: LinkId) -> ControlAction {
        ControlAction::Send {
            link,
            frame: ControlFrame::Digest(self.digest()),
        }
    }

    fn send_lsa(&self, link: LinkId, message: &LsaMessage) -> ControlAction {
        ControlAction::Send {
            link,
            frame: ControlFrame::Lsa(message.clone()),
        }
    }

    fn next_digest_timer(now: MonoTime, link: LinkId) -> ControlAction {
        ControlAction::SetTimer {
            timer: ControlTimer::Digest(link),
            at: MonoTime::from_millis(now.as_millis().saturating_add(DIGEST_INTERVAL_MS)),
        }
    }

    fn receive_digest(&self, incoming: LinkId, entries: Vec<DigestEntry>) -> Vec<ControlAction> {
        if !self.adjacencies.contains_key(&incoming) {
            return Vec::new();
        }

        let mut remote = BTreeMap::<NodeId, (u32, u64)>::new();
        for entry in entries {
            remote
                .entry(entry.origin)
                .and_modify(|version| *version = (*version).max(entry.version()))
                .or_insert(entry.version());
        }

        let origins = self
            .lsdb
            .keys()
            .chain(remote.keys())
            .copied()
            .collect::<BTreeSet<_>>();

        let mut requested = Vec::new();
        let mut actions = Vec::new();
        for origin in origins {
            let local = self.lsdb.get(&origin);
            let local_version = local.map(|entry| Self::lsa_version(&entry.message.lsa));
            let remote_version = remote.get(&origin).copied();

            match compare_versions(local_version, remote_version) {
                EntryRelation::RemoteOnly | EntryRelation::RemoteNewer if origin != self.me => {
                    requested.push(origin);
                }
                EntryRelation::LocalOnly | EntryRelation::LocalNewer => {
                    if let Some(local) = local {
                        actions.push(self.send_lsa(incoming, &local.message));
                    }
                }
                EntryRelation::SameVersion
                | EntryRelation::RemoteOnly
                | EntryRelation::RemoteNewer => {}
            }
        }
        if !requested.is_empty() {
            actions.push(ControlAction::Send {
                link: incoming,
                frame: ControlFrame::DigestReq(requested),
            });
        }
        actions
    }

    fn receive_digest_req(&self, incoming: LinkId, origins: Vec<NodeId>) -> Vec<ControlAction> {
        if !self.adjacencies.contains_key(&incoming) {
            return Vec::new();
        }

        origins
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|origin| {
                self.lsdb
                    .get(&origin)
                    .map(|entry| self.send_lsa(incoming, &entry.message))
            })
            .collect()
    }

    fn install(&mut self, message: LsaMessage, now: MonoTime) {
        self.lsdb.insert(
            message.lsa.origin,
            LsdbEntry {
                message,
                received_at: now,
            },
        );
    }

    fn flood(&self, message: &LsaMessage, except: Option<LinkId>) -> Vec<ControlAction> {
        self.adjacencies
            .keys()
            .copied()
            .filter(|link| Some(*link) != except)
            .map(|link| ControlAction::Send {
                link,
                frame: ControlFrame::Lsa(message.clone()),
            })
            .collect()
    }

    fn handle_digest_timer(&self, now: MonoTime, link: LinkId) -> Vec<ControlAction> {
        if !self.adjacencies.contains_key(&link) {
            return Vec::new();
        }
        vec![self.send_digest(link), Self::next_digest_timer(now, link)]
    }

    fn recompute_routes(&mut self) -> Option<ControlAction> {
        let routes = self.shortest_paths();
        if routes == self.routes.routes {
            return None;
        }

        let table = Arc::new(RouteTable {
            version: self.routes.version + 1,
            routes,
        });
        self.routes = Arc::clone(&table);
        Some(ControlAction::PublishRoutes(table))
    }

    fn shortest_paths(&self) -> BTreeMap<NodeId, Route> {
        let graph = self.bidirectional_graph();
        let mut distance: BTreeMap<NodeId, u32> = BTreeMap::new();
        let mut path_hops: BTreeMap<NodeId, u8> = BTreeMap::new();
        let mut first_peer: BTreeMap<NodeId, NodeId> = BTreeMap::new();
        let mut pending = BinaryHeap::new();

        distance.insert(self.me, 0);
        path_hops.insert(self.me, 0);
        pending.push(Reverse((0_u32, self.me)));

        while let Some(Reverse((known_cost, node))) = pending.pop() {
            if distance.get(&node).copied() != Some(known_cost) {
                continue;
            }
            let Some(edges) = graph.get(&node) else {
                continue;
            };

            for edge in edges {
                let candidate_cost = known_cost.saturating_add(u32::from(edge.cost.get()));
                let candidate_hops = path_hops[&node].saturating_add(1);
                let candidate_first = if node == self.me {
                    edge.peer
                } else {
                    first_peer[&node]
                };
                let should_replace = match distance.get(&edge.peer) {
                    None => true,
                    Some(current_cost) if candidate_cost < *current_cost => true,
                    Some(current_cost) if candidate_cost == *current_cost => first_peer
                        .get(&edge.peer)
                        .is_some_and(|current_first| candidate_first < *current_first),
                    _ => false,
                };
                if should_replace {
                    distance.insert(edge.peer, candidate_cost);
                    path_hops.insert(edge.peer, candidate_hops);
                    first_peer.insert(edge.peer, candidate_first);
                    pending.push(Reverse((candidate_cost, edge.peer)));
                }
            }
        }

        distance
            .into_iter()
            .filter_map(|(destination, cost)| {
                if destination == self.me {
                    return None;
                }
                let peer = first_peer[&destination];
                let link = self.best_link_to(peer)?;
                Some((
                    destination,
                    Route {
                        next_hop: link,
                        cost,
                        hops: path_hops[&destination],
                    },
                ))
            })
            .collect()
    }

    fn bidirectional_graph(&self) -> BTreeMap<NodeId, Vec<Adjacency>> {
        let mut graph = BTreeMap::new();
        for (origin, entry) in &self.lsdb {
            let accepted = entry
                .message
                .lsa
                .adjacencies
                .iter()
                .filter(|edge| {
                    self.lsdb.get(&edge.peer).is_some_and(|peer_entry| {
                        peer_entry
                            .message
                            .lsa
                            .adjacencies
                            .iter()
                            .any(|reverse| reverse.peer == *origin)
                    })
                })
                .cloned()
                .collect::<Vec<_>>();
            graph.insert(*origin, accepted);
        }
        graph
    }

    fn best_link_to(&self, peer: NodeId) -> Option<LinkId> {
        self.adjacencies
            .iter()
            .filter(|(_, adjacency)| adjacency.peer == peer)
            .min_by_key(|(link, adjacency)| (adjacency.cost, **link))
            .map(|(link, _)| *link)
    }
}

impl Component for ControlPlane {
    type Event = ControlEvent;
    type Action = ControlAction;

    fn handle(&mut self, now: MonoTime, event: Self::Event) -> Vec<Self::Action> {
        match event {
            ControlEvent::LinkUp { link, peer, cost } => {
                self.adjacencies.insert(link, LocalAdjacency { peer, cost });
                let mut actions = self.originate_lsa(now);
                actions.push(self.send_digest(link));
                actions.push(Self::next_digest_timer(now, link));
                actions
            }
            ControlEvent::LinkDown { link } => {
                if self.adjacencies.remove(&link).is_none() {
                    Vec::new()
                } else {
                    self.originate_lsa(now)
                }
            }
            ControlEvent::Frame { link, frame } => match frame {
                ControlFrame::Lsa(message) => self.receive_lsa(now, link, message),
                ControlFrame::Digest(entries) => self.receive_digest(link, entries),
                ControlFrame::DigestReq(origins) => self.receive_digest_req(link, origins),
            },
            ControlEvent::Timer(ControlTimer::Digest(link)) => self.handle_digest_timer(now, link),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{compare_versions, EntryRelation};

    #[test]
    fn digest_entry_relations_are_explicit() {
        assert_eq!(
            compare_versions(None, Some((1, 1))),
            EntryRelation::RemoteOnly
        );
        assert_eq!(
            compare_versions(Some((1, 1)), Some((1, 2))),
            EntryRelation::RemoteNewer
        );
        assert_eq!(
            compare_versions(Some((1, 1)), Some((1, 1))),
            EntryRelation::SameVersion
        );
        assert_eq!(
            compare_versions(Some((2, 1)), Some((1, 99))),
            EntryRelation::LocalNewer
        );
        assert_eq!(
            compare_versions(Some((1, 1)), None),
            EntryRelation::LocalOnly
        );
    }
}
