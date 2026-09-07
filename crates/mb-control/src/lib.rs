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
pub const SPF_INITIAL_HOLD_MS: u64 = 100;
pub const SPF_MAX_HOLD_MS: u64 = 5_000;
pub const SPF_QUIET_RESET_MS: u64 = 10_000;
pub const DEFAULT_LSA_TTL_SEC: u32 = 300;
pub const MAX_LSA_TTL_SEC: u32 = 3_600;
pub const MAX_LSDB_ENTRIES: usize = 1_000;
pub const MAX_LSA_ADJACENCIES: usize = 256;
pub const MAX_DIGEST_ENTRIES: usize = MAX_LSDB_ENTRIES;
pub const MAX_DIGEST_REQ_ORIGINS: usize = MAX_LSDB_ENTRIES;

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
/// Service, topic, and address fields can be added without changing the
/// event/action boundary around the control plane.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Lsa {
    pub origin: NodeId,
    pub epoch: u32,
    pub seq: u64,
    pub ttl_sec: u32,
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
    SpfHold {
        generation: u64,
    },
    LsaRefresh {
        epoch: u32,
        seq: u64,
    },
    LsaExpire {
        origin: NodeId,
        epoch: u32,
        seq: u64,
    },
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
    pub fn new(version: u64, routes: BTreeMap<NodeId, Route>) -> Self {
        Self { version, routes }
    }

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
    version: (u32, u64),
    state: LsdbEntryState,
}

#[derive(Clone, Debug)]
enum LsdbEntryState {
    Active(ActiveLsa),
    Tombstone,
}

#[derive(Clone, Debug)]
struct ActiveLsa {
    message: LsaMessage,
    received_at: MonoTime,
    expires_at: MonoTime,
}

impl LsdbEntry {
    fn active(&self) -> Option<&ActiveLsa> {
        match &self.state {
            LsdbEntryState::Active(active) => Some(active),
            LsdbEntryState::Tombstone => None,
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct LocalAdjacency {
    peer: NodeId,
    cost: LinkCost,
}

#[derive(Clone, Copy, Debug)]
struct PendingSpf {
    generation: u64,
    at: MonoTime,
}

pub struct ControlPlane {
    me: NodeId,
    epoch: u32,
    my_seq: u64,
    adjacencies: BTreeMap<LinkId, LocalAdjacency>,
    lsdb: BTreeMap<NodeId, LsdbEntry>,
    routes: Arc<RouteTable>,
    pending_spf: Option<PendingSpf>,
    spf_generation: u64,
    spf_hold_ms: u64,
    last_lsdb_change_at: Option<MonoTime>,
    signer: Box<dyn LsaSigner>,
    verifier: Box<dyn LsaVerifier>,
}

impl ControlPlane {
    pub fn new_unsecured(me: NodeId, epoch: u32) -> Self {
        Self::new_unsecured_with_seq(me, epoch, 0)
    }

    /// Constructs a control plane whose next locally originated LSA will have
    /// a sequence number greater than `last_seq`.
    pub fn new_unsecured_with_seq(me: NodeId, epoch: u32, last_seq: u64) -> Self {
        Self::with_auth_and_seq(
            me,
            epoch,
            last_seq,
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
        Self::with_auth_and_seq(me, epoch, 0, signer, verifier)
    }

    pub fn with_auth_and_seq(
        me: NodeId,
        epoch: u32,
        last_seq: u64,
        signer: Box<dyn LsaSigner>,
        verifier: Box<dyn LsaVerifier>,
    ) -> Self {
        Self {
            me,
            epoch,
            my_seq: last_seq,
            adjacencies: BTreeMap::new(),
            lsdb: BTreeMap::new(),
            routes: Arc::new(RouteTable::default()),
            pending_spf: None,
            spf_generation: 0,
            spf_hold_ms: SPF_INITIAL_HOLD_MS,
            last_lsdb_change_at: None,
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

    /// Returns active LSAs in stable origin order for diagnostics and Admin APIs.
    pub fn lsas(&self) -> impl Iterator<Item = &Lsa> {
        self.lsdb
            .values()
            .filter_map(LsdbEntry::active)
            .map(|active| &active.message.lsa)
    }

    pub fn lsa(&self, origin: &NodeId) -> Option<&Lsa> {
        self.lsdb
            .get(origin)
            .and_then(LsdbEntry::active)
            .map(|active| &active.message.lsa)
    }

    pub fn lsa_received_at(&self, origin: &NodeId) -> Option<MonoTime> {
        self.lsdb
            .get(origin)
            .and_then(LsdbEntry::active)
            .map(|active| active.received_at)
    }

    pub fn lsa_is_expired(&self, origin: &NodeId) -> Option<bool> {
        self.lsdb
            .get(origin)
            .map(|entry| matches!(entry.state, LsdbEntryState::Tombstone))
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
            ttl_sec: DEFAULT_LSA_TTL_SEC,
            adjacencies: best_by_peer
                .into_iter()
                .map(|(peer, cost)| Adjacency { peer, cost })
                .collect(),
        };
        let message = self.signer.sign(lsa);
        let expiry = self
            .install(message.clone(), now)
            .expect("local LSA must fit in the reserved LSDB entry");

        let mut actions = vec![ControlAction::PersistSeq(self.my_seq)];
        actions.extend(self.flood(&message, None));
        actions.push(expiry);
        actions.push(Self::next_refresh_timer(now, &message.lsa));
        if let Some(action) = self.mark_spf_dirty(now) {
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
        if !self.adjacencies.contains_key(&incoming)
            || !Self::valid_lsa(&message.lsa)
            || !self.verifier.verify(&message)
        {
            return Vec::new();
        }
        if message.lsa.origin == self.me {
            return self
                .lsdb
                .get(&self.me)
                .filter(|entry| entry.version > Self::lsa_version(&message.lsa))
                .and_then(LsdbEntry::active)
                .map(|active| self.send_lsa(incoming, &active.message))
                .into_iter()
                .collect();
        }
        if !self.is_newer(&message.lsa) {
            return self
                .lsdb
                .get(&message.lsa.origin)
                .filter(|entry| entry.version > Self::lsa_version(&message.lsa))
                .and_then(LsdbEntry::active)
                .map(|active| self.send_lsa(incoming, &active.message))
                .into_iter()
                .collect();
        }

        let Some(expiry) = self.install(message.clone(), now) else {
            return Vec::new();
        };
        let mut actions = self.flood(&message, Some(incoming));
        actions.push(expiry);
        if let Some(action) = self.mark_spf_dirty(now) {
            actions.push(action);
        }
        actions
    }

    fn valid_lsa(lsa: &Lsa) -> bool {
        (1..=MAX_LSA_TTL_SEC).contains(&lsa.ttl_sec) && lsa.adjacencies.len() <= MAX_LSA_ADJACENCIES
    }

    fn is_newer(&self, candidate: &Lsa) -> bool {
        match self.lsdb.get(&candidate.origin) {
            None => true,
            Some(current) => (candidate.epoch, candidate.seq) > current.version,
        }
    }

    fn lsa_version(lsa: &Lsa) -> (u32, u64) {
        (lsa.epoch, lsa.seq)
    }

    fn digest(&self) -> Vec<DigestEntry> {
        self.lsdb
            .iter()
            .map(|(origin, entry)| DigestEntry {
                origin: *origin,
                epoch: entry.version.0,
                seq: entry.version.1,
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

    fn next_refresh_timer(now: MonoTime, lsa: &Lsa) -> ControlAction {
        let refresh_ms = Self::ttl_ms(lsa) / 5;
        ControlAction::SetTimer {
            timer: ControlTimer::LsaRefresh {
                epoch: lsa.epoch,
                seq: lsa.seq,
            },
            at: MonoTime::from_millis(now.as_millis().saturating_add(refresh_ms)),
        }
    }

    fn receive_digest(&self, incoming: LinkId, entries: Vec<DigestEntry>) -> Vec<ControlAction> {
        if !self.adjacencies.contains_key(&incoming) || entries.len() > MAX_DIGEST_ENTRIES {
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
            let local_version = local.map(|entry| entry.version);
            let remote_version = remote.get(&origin).copied();

            match compare_versions(local_version, remote_version) {
                EntryRelation::RemoteOnly | EntryRelation::RemoteNewer if origin != self.me => {
                    requested.push(origin);
                }
                EntryRelation::LocalOnly | EntryRelation::LocalNewer => {
                    if let Some(active) = local.and_then(LsdbEntry::active) {
                        actions.push(self.send_lsa(incoming, &active.message));
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
        if !self.adjacencies.contains_key(&incoming) || origins.len() > MAX_DIGEST_REQ_ORIGINS {
            return Vec::new();
        }

        origins
            .into_iter()
            .collect::<BTreeSet<_>>()
            .into_iter()
            .filter_map(|origin| {
                self.lsdb
                    .get(&origin)
                    .and_then(LsdbEntry::active)
                    .map(|active| self.send_lsa(incoming, &active.message))
            })
            .collect()
    }

    fn install(&mut self, message: LsaMessage, now: MonoTime) -> Option<ControlAction> {
        if !self.lsdb.contains_key(&message.lsa.origin) && self.lsdb.len() >= MAX_LSDB_ENTRIES {
            return None;
        }
        let expires_at =
            MonoTime::from_millis(now.as_millis().saturating_add(Self::ttl_ms(&message.lsa)));
        let origin = message.lsa.origin;
        let (epoch, seq) = Self::lsa_version(&message.lsa);
        self.lsdb.insert(
            origin,
            LsdbEntry {
                version: (epoch, seq),
                state: LsdbEntryState::Active(ActiveLsa {
                    message,
                    received_at: now,
                    expires_at,
                }),
            },
        );
        Some(ControlAction::SetTimer {
            timer: ControlTimer::LsaExpire { origin, epoch, seq },
            at: expires_at,
        })
    }

    fn ttl_ms(lsa: &Lsa) -> u64 {
        u64::from(lsa.ttl_sec).saturating_mul(1_000)
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

    fn handle_refresh_timer(&mut self, now: MonoTime, epoch: u32, seq: u64) -> Vec<ControlAction> {
        let is_current = self
            .lsdb
            .get(&self.me)
            .is_some_and(|entry| entry.version == (epoch, seq) && entry.active().is_some());
        if is_current {
            self.originate_lsa(now)
        } else {
            Vec::new()
        }
    }

    fn handle_expire_timer(
        &mut self,
        now: MonoTime,
        origin: NodeId,
        epoch: u32,
        seq: u64,
    ) -> Vec<ControlAction> {
        let Some(entry) = self.lsdb.get(&origin) else {
            return Vec::new();
        };
        if entry.version != (epoch, seq) {
            return Vec::new();
        }
        let Some(active) = entry.active() else {
            return Vec::new();
        };
        // The Tokio runtime normally delivers this timer only at or after its
        // deadline. Rescheduling is defensive for alternate drivers,
        // simulators, or manually injected early timer events.
        if now < active.expires_at {
            return vec![ControlAction::SetTimer {
                timer: ControlTimer::LsaExpire { origin, epoch, seq },
                at: active.expires_at,
            }];
        }

        self.lsdb
            .get_mut(&origin)
            .expect("entry checked above")
            .state = LsdbEntryState::Tombstone;
        self.mark_spf_dirty(now).into_iter().collect()
    }

    fn mark_spf_dirty(&mut self, now: MonoTime) -> Option<ControlAction> {
        let was_quiet = self.last_lsdb_change_at.is_some_and(|last_change| {
            now.as_millis().saturating_sub(last_change.as_millis()) >= SPF_QUIET_RESET_MS
        });
        self.last_lsdb_change_at = Some(now);

        if was_quiet {
            self.spf_hold_ms = SPF_INITIAL_HOLD_MS;
        }

        if self.pending_spf.is_some() && !was_quiet {
            return None;
        }

        self.spf_generation = self
            .spf_generation
            .checked_add(1)
            .expect("SPF timer generation exhausted");
        let at = MonoTime::from_millis(now.as_millis().saturating_add(self.spf_hold_ms));
        let pending = PendingSpf {
            generation: self.spf_generation,
            at,
        };
        self.pending_spf = Some(pending);
        Some(ControlAction::SetTimer {
            timer: ControlTimer::SpfHold {
                generation: pending.generation,
            },
            at,
        })
    }

    fn handle_spf_timer(&mut self, now: MonoTime, generation: u64) -> Vec<ControlAction> {
        let Some(pending) = self.pending_spf else {
            return Vec::new();
        };
        if pending.generation != generation {
            return Vec::new();
        }
        if now < pending.at {
            return vec![ControlAction::SetTimer {
                timer: ControlTimer::SpfHold { generation },
                at: pending.at,
            }];
        }

        self.pending_spf = None;
        self.spf_hold_ms = self.spf_hold_ms.saturating_mul(2).min(SPF_MAX_HOLD_MS);
        self.recompute_routes().into_iter().collect()
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
            let Some(active) = entry.active() else {
                continue;
            };
            let accepted = active
                .message
                .lsa
                .adjacencies
                .iter()
                .filter(|edge| {
                    self.lsdb.get(&edge.peer).is_some_and(|peer_entry| {
                        peer_entry.active().is_some_and(|peer_active| {
                            peer_active
                                .message
                                .lsa
                                .adjacencies
                                .iter()
                                .any(|reverse| reverse.peer == *origin)
                        })
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
            ControlEvent::Timer(timer) => match timer {
                ControlTimer::Digest(link) => self.handle_digest_timer(now, link),
                ControlTimer::SpfHold { generation } => self.handle_spf_timer(now, generation),
                ControlTimer::LsaRefresh { epoch, seq } => {
                    self.handle_refresh_timer(now, epoch, seq)
                }
                ControlTimer::LsaExpire { origin, epoch, seq } => {
                    self.handle_expire_timer(now, origin, epoch, seq)
                }
            },
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
