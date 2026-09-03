//! I/O-free link-state control plane.
//!
//! The only way to affect this component is through [`ControlEvent`]. External
//! effects are returned as [`ControlAction`], which lets the same code run
//! under a Tokio runtime or a deterministic simulator.

use mb_types::{Component, LinkId, MonoTime, NodeId};
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::sync::Arc;

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
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlAction {
    Send { link: LinkId, frame: ControlFrame },
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
        self.recompute_routes(&mut actions);
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
        if message.lsa.origin == self.me || !self.is_newer(&message.lsa) {
            return Vec::new();
        }

        self.install(message.clone(), now);
        let mut actions = self.flood(&message, Some(incoming));
        self.recompute_routes(&mut actions);
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

    fn recompute_routes(&mut self, actions: &mut Vec<ControlAction>) {
        let routes = self.shortest_paths();
        if routes == self.routes.routes {
            return;
        }

        let table = Arc::new(RouteTable {
            version: self.routes.version + 1,
            routes,
        });
        self.routes = Arc::clone(&table);
        actions.push(ControlAction::PublishRoutes(table));
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
                self.originate_lsa(now)
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
            },
        }
    }
}
