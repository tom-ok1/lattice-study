//! Tokio glue that drives the I/O-free control plane from transport events.

use bytes::Bytes;
use mb_control::{
    Adjacency, ControlAction, ControlEvent, ControlFrame, ControlPlane, ControlTimer, DigestEntry,
    InvalidLinkCost, LinkCost, Lsa, LsaMessage, RouteTable, MAX_DIGEST_ENTRIES,
    MAX_DIGEST_REQ_ORIGINS, MAX_LSA_ADJACENCIES, MAX_LSA_TTL_SEC,
};
use mb_transport::{LinkEvent, TcpEndpoint, TransportError};
use mb_types::{Component, LinkId, MonoTime, NodeId};
use mb_wire::{proto, Channel, FrameType, WireFrame};
use prost::Message;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BinaryHeap};
use std::error::Error;
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::{Duration, Instant};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SeqStoreError(String);

impl SeqStoreError {
    pub fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }
}

impl fmt::Display for SeqStoreError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Error for SeqStoreError {}

pub trait SeqStore: fmt::Debug + Send + Sync {
    fn load(&self) -> Result<u64, SeqStoreError>;
    fn persist(&self, seq: u64) -> Result<(), SeqStoreError>;
}

#[derive(Debug, Default)]
pub struct MemorySeqStore {
    seq: Mutex<u64>,
}

impl MemorySeqStore {
    pub fn with_seq(seq: u64) -> Self {
        Self {
            seq: Mutex::new(seq),
        }
    }
}

impl SeqStore for MemorySeqStore {
    fn load(&self) -> Result<u64, SeqStoreError> {
        self.seq
            .lock()
            .map(|seq| *seq)
            .map_err(|_| SeqStoreError::new("in-memory sequence store lock is poisoned"))
    }

    fn persist(&self, seq: u64) -> Result<(), SeqStoreError> {
        let mut stored = self
            .seq
            .lock()
            .map_err(|_| SeqStoreError::new("in-memory sequence store lock is poisoned"))?;
        if seq < *stored {
            return Err(SeqStoreError::new(format!(
                "refusing to move persisted sequence backward from {} to {seq}",
                *stored
            )));
        }
        *stored = seq;
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct FileSeqStore {
    path: PathBuf,
}

impl FileSeqStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }
}

impl SeqStore for FileSeqStore {
    fn load(&self) -> Result<u64, SeqStoreError> {
        let contents = match fs::read_to_string(&self.path) {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
            Err(error) => {
                return Err(SeqStoreError::new(format!(
                    "failed to read {}: {error}",
                    self.path.display()
                )))
            }
        };
        contents.trim().parse::<u64>().map_err(|error| {
            SeqStoreError::new(format!(
                "invalid sequence in {}: {error}",
                self.path.display()
            ))
        })
    }

    fn persist(&self, seq: u64) -> Result<(), SeqStoreError> {
        let previous = self.load()?;
        if seq < previous {
            return Err(SeqStoreError::new(format!(
                "refusing to move persisted sequence backward from {previous} to {seq}"
            )));
        }
        if let Some(parent) = self
            .path
            .parent()
            .filter(|parent| !parent.as_os_str().is_empty())
        {
            fs::create_dir_all(parent).map_err(|error| {
                SeqStoreError::new(format!("failed to create {}: {error}", parent.display()))
            })?;
        }
        let temporary = self.path.with_extension("tmp");
        let mut file = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&temporary)
            .map_err(|error| {
                SeqStoreError::new(format!("failed to open {}: {error}", temporary.display()))
            })?;
        writeln!(file, "{seq}").map_err(|error| {
            SeqStoreError::new(format!("failed to write {}: {error}", temporary.display()))
        })?;
        file.sync_all().map_err(|error| {
            SeqStoreError::new(format!("failed to sync {}: {error}", temporary.display()))
        })?;
        fs::rename(&temporary, &self.path).map_err(|error| {
            SeqStoreError::new(format!(
                "failed to replace {}: {error}",
                self.path.display()
            ))
        })
    }
}

#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    pub node_id: NodeId,
    pub epoch: u32,
    pub peer_costs: BTreeMap<NodeId, LinkCost>,
}

#[derive(Clone, Debug)]
pub struct RuntimeSnapshot {
    pub node_id: NodeId,
    pub lsdb_entries: usize,
    pub routes: Arc<RouteTable>,
    pub persisted_seq: u64,
    pub peers: BTreeMap<LinkId, NodeId>,
}

#[derive(Debug)]
pub enum RuntimeError {
    EndpointNodeMismatch {
        endpoint: NodeId,
        configured: NodeId,
    },
    UnexpectedFrame {
        frame_type: FrameType,
        channel: Channel,
    },
    Protobuf(prost::DecodeError),
    InvalidNodeIdLength(usize),
    InvalidLinkCost(u32),
    InvalidLsaTtl(u32),
    TooManyElements {
        field: &'static str,
        count: usize,
        limit: usize,
    },
    SequenceExhausted,
    SeqStore(SeqStoreError),
    Transport(TransportError),
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EndpointNodeMismatch {
                endpoint,
                configured,
            } => write!(
                f,
                "endpoint belongs to node {endpoint}, but runtime is configured for {configured}"
            ),
            Self::UnexpectedFrame {
                frame_type,
                channel,
            } => write!(
                f,
                "unexpected runtime frame type {frame_type:?} on channel {channel:?}"
            ),
            Self::Protobuf(error) => write!(f, "control protobuf is invalid: {error}"),
            Self::InvalidNodeIdLength(length) => {
                write!(f, "control-plane NodeId must be 32 bytes, got {length}")
            }
            Self::InvalidLinkCost(cost) => write!(f, "invalid link cost {cost}"),
            Self::InvalidLsaTtl(ttl) => write!(f, "invalid LSA ttl_sec {ttl}"),
            Self::TooManyElements {
                field,
                count,
                limit,
            } => write!(f, "{field} contains {count} elements, limit is {limit}"),
            Self::SequenceExhausted => f.write_str("LSA sequence is exhausted"),
            Self::SeqStore(error) => write!(f, "sequence persistence failed: {error}"),
            Self::Transport(error) => write!(f, "transport operation failed: {error}"),
        }
    }
}

impl Error for RuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protobuf(error) => Some(error),
            Self::SeqStore(error) => Some(error),
            Self::Transport(error) => Some(error),
            _ => None,
        }
    }
}

impl From<prost::DecodeError> for RuntimeError {
    fn from(value: prost::DecodeError) -> Self {
        Self::Protobuf(value)
    }
}

impl From<TransportError> for RuntimeError {
    fn from(value: TransportError) -> Self {
        Self::Transport(value)
    }
}

impl From<SeqStoreError> for RuntimeError {
    fn from(value: SeqStoreError) -> Self {
        Self::SeqStore(value)
    }
}

/// Handle for observing and stopping one control-plane runtime.
pub struct ControlRuntime {
    endpoint: TcpEndpoint,
    snapshots: watch::Receiver<RuntimeSnapshot>,
    driver: JoinHandle<()>,
}

impl ControlRuntime {
    pub fn spawn(
        config: RuntimeConfig,
        endpoint: TcpEndpoint,
        events: mpsc::Receiver<LinkEvent>,
    ) -> Result<Self, RuntimeError> {
        Self::spawn_with_seq_store(
            config,
            endpoint,
            events,
            Arc::new(MemorySeqStore::default()),
        )
    }

    pub fn spawn_with_seq_store(
        config: RuntimeConfig,
        endpoint: TcpEndpoint,
        events: mpsc::Receiver<LinkEvent>,
        seq_store: Arc<dyn SeqStore>,
    ) -> Result<Self, RuntimeError> {
        if endpoint.local_node() != config.node_id {
            return Err(RuntimeError::EndpointNodeMismatch {
                endpoint: endpoint.local_node(),
                configured: config.node_id,
            });
        }

        let persisted_seq = seq_store.load()?;
        if persisted_seq == u64::MAX {
            return Err(RuntimeError::SequenceExhausted);
        }
        let plane =
            ControlPlane::new_unsecured_with_seq(config.node_id, config.epoch, persisted_seq);
        let initial = RuntimeSnapshot {
            node_id: config.node_id,
            lsdb_entries: plane.lsdb_len(),
            routes: plane.route_table(),
            persisted_seq,
            peers: BTreeMap::new(),
        };
        let (snapshot_tx, snapshots) = watch::channel(initial);
        let driver_endpoint = endpoint.clone();
        let driver = tokio::spawn(run_control_loop(
            config,
            plane,
            driver_endpoint,
            events,
            snapshot_tx,
            seq_store,
            persisted_seq,
        ));
        Ok(Self {
            endpoint,
            snapshots,
            driver,
        })
    }

    pub fn snapshot(&self) -> RuntimeSnapshot {
        self.snapshots.borrow().clone()
    }

    pub fn subscribe(&self) -> watch::Receiver<RuntimeSnapshot> {
        self.snapshots.clone()
    }

    pub async fn shutdown(self) {
        self.endpoint.shutdown().await;
        self.driver.abort();
        let _ = self.driver.await;
    }
}

async fn run_control_loop(
    config: RuntimeConfig,
    mut plane: ControlPlane,
    endpoint: TcpEndpoint,
    mut events: mpsc::Receiver<LinkEvent>,
    snapshots: watch::Sender<RuntimeSnapshot>,
    seq_store: Arc<dyn SeqStore>,
    mut persisted_seq: u64,
) {
    let started_at = Instant::now();
    let mut peers = BTreeMap::new();
    let mut timers = BinaryHeap::<Reverse<(u64, u64, ControlTimer)>>::new();
    let mut next_timer_sequence = 0_u64;

    loop {
        let Some(input) = next_runtime_input(started_at, &mut events, &mut timers).await else {
            break;
        };

        let event = match input {
            RuntimeInput::Timer(timer) => ControlEvent::Timer(timer),
            RuntimeInput::Link(link_event) => {
                let Some(event) =
                    translate_link_event(&config, &endpoint, &mut peers, link_event).await
                else {
                    continue;
                };
                event
            }
        };

        let now_ms = elapsed_millis(started_at);
        let actions = plane.handle(MonoTime::from_millis(now_ms), event);
        for action in actions {
            match action {
                ControlAction::Send { link, frame } => {
                    let Ok(wire_frame) = encode_control_frame(&frame) else {
                        continue;
                    };
                    if endpoint.send(link, wire_frame).await.is_err() {
                        let _ = endpoint.close(link).await;
                    }
                }
                ControlAction::SetTimer { timer, at } => {
                    timers.push(Reverse((at.as_millis(), next_timer_sequence, timer)));
                    next_timer_sequence = next_timer_sequence.wrapping_add(1);
                }
                ControlAction::PublishRoutes(_) => {}
                ControlAction::PersistSeq(seq) => {
                    let store = Arc::clone(&seq_store);
                    let stored = tokio::task::spawn_blocking(move || store.persist(seq)).await;
                    if !matches!(stored, Ok(Ok(()))) {
                        return;
                    }
                    persisted_seq = seq;
                }
            }
        }

        snapshots.send_replace(RuntimeSnapshot {
            node_id: config.node_id,
            lsdb_entries: plane.lsdb_len(),
            routes: plane.route_table(),
            persisted_seq,
            peers: peers.clone(),
        });
    }
}

enum RuntimeInput {
    Link(LinkEvent),
    Timer(ControlTimer),
}

async fn next_runtime_input(
    started_at: Instant,
    events: &mut mpsc::Receiver<LinkEvent>,
    timers: &mut BinaryHeap<Reverse<(u64, u64, ControlTimer)>>,
) -> Option<RuntimeInput> {
    if let Some(Reverse((at_ms, _, timer))) = timers.peek().copied() {
        if at_ms <= elapsed_millis(started_at) {
            timers.pop();
            return Some(RuntimeInput::Timer(timer));
        }
    }

    let next_timer_at = timers.peek().map(|Reverse((at_ms, _, _))| *at_ms);
    tokio::select! {
        link_event = events.recv() => link_event.map(RuntimeInput::Link),
        _ = wait_for_timer(started_at, next_timer_at) => {
            let Reverse((_, _, timer)) = timers.pop()?;
            Some(RuntimeInput::Timer(timer))
        }
    }
}

async fn wait_for_timer(started_at: Instant, at_ms: Option<u64>) {
    match at_ms {
        Some(at_ms) => {
            tokio::time::sleep_until(started_at + Duration::from_millis(at_ms)).await;
        }
        None => std::future::pending().await,
    }
}

fn elapsed_millis(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

async fn translate_link_event(
    config: &RuntimeConfig,
    endpoint: &TcpEndpoint,
    peers: &mut BTreeMap<LinkId, NodeId>,
    event: LinkEvent,
) -> Option<ControlEvent> {
    match event {
        LinkEvent::Up { link, peer } => {
            let Some(cost) = config.peer_costs.get(&peer).copied() else {
                let _ = endpoint.close(link).await;
                return None;
            };
            peers.insert(link, peer);
            Some(ControlEvent::LinkUp { link, peer, cost })
        }
        LinkEvent::Down { link } => {
            peers.remove(&link);
            Some(ControlEvent::LinkDown { link })
        }
        LinkEvent::Frame { link, frame } => match decode_control_frame(frame) {
            Ok(frame) => Some(ControlEvent::Frame { link, frame }),
            Err(_) => {
                let _ = endpoint.close(link).await;
                None
            }
        },
    }
}

fn encode_control_frame(frame: &ControlFrame) -> Result<WireFrame, RuntimeError> {
    match frame {
        ControlFrame::Lsa(message) => {
            if !(1..=MAX_LSA_TTL_SEC).contains(&message.lsa.ttl_sec) {
                return Err(RuntimeError::InvalidLsaTtl(message.lsa.ttl_sec));
            }
            ensure_limit(
                "Lsa.adjacencies",
                message.lsa.adjacencies.len(),
                MAX_LSA_ADJACENCIES,
            )?;
            let lsa_bytes = if message.canonical_bytes.is_empty() {
                encode_lsa(&message.lsa)
            } else {
                message.canonical_bytes.to_vec()
            };
            let signed = proto::SignedLsa {
                lsa_bytes,
                signature: message.signature.to_vec(),
            };
            Ok(WireFrame::control(FrameType::Lsa, signed.encode_to_vec()))
        }
        ControlFrame::Digest(entries) => {
            ensure_limit("Digest.entries", entries.len(), MAX_DIGEST_ENTRIES)?;
            let digest = proto::Digest {
                entries: entries
                    .iter()
                    .map(|entry| proto::DigestEntry {
                        origin: entry.origin.as_bytes().to_vec(),
                        epoch: entry.epoch,
                        seq: entry.seq,
                    })
                    .collect(),
            };
            Ok(WireFrame::control(
                FrameType::Digest,
                digest.encode_to_vec(),
            ))
        }
        ControlFrame::DigestReq(origins) => {
            ensure_limit("DigestReq.origins", origins.len(), MAX_DIGEST_REQ_ORIGINS)?;
            let request = proto::DigestReq {
                origins: origins
                    .iter()
                    .map(|origin| origin.as_bytes().to_vec())
                    .collect(),
            };
            Ok(WireFrame::control(
                FrameType::DigestReq,
                request.encode_to_vec(),
            ))
        }
    }
}

fn decode_control_frame(frame: WireFrame) -> Result<ControlFrame, RuntimeError> {
    if frame.channel != Channel::Control {
        return Err(RuntimeError::UnexpectedFrame {
            frame_type: frame.frame_type,
            channel: frame.channel,
        });
    }
    match frame.frame_type {
        FrameType::Lsa => {
            let signed = proto::SignedLsa::decode(frame.payload)?;
            let encoded_lsa = Bytes::from(signed.lsa_bytes);
            let wire_lsa = proto::Lsa::decode(encoded_lsa.clone())?;
            let lsa = decode_lsa(wire_lsa)?;
            Ok(ControlFrame::Lsa(LsaMessage {
                lsa,
                canonical_bytes: Arc::from(encoded_lsa.as_ref()),
                signature: Arc::from(signed.signature),
            }))
        }
        FrameType::Digest => {
            let digest = proto::Digest::decode(frame.payload)?;
            ensure_limit("Digest.entries", digest.entries.len(), MAX_DIGEST_ENTRIES)?;
            let entries = digest
                .entries
                .into_iter()
                .map(|entry| {
                    Ok(DigestEntry {
                        origin: decode_node_id(&entry.origin)?,
                        epoch: entry.epoch,
                        seq: entry.seq,
                    })
                })
                .collect::<Result<Vec<_>, RuntimeError>>()?;
            Ok(ControlFrame::Digest(entries))
        }
        FrameType::DigestReq => {
            let request = proto::DigestReq::decode(frame.payload)?;
            ensure_limit(
                "DigestReq.origins",
                request.origins.len(),
                MAX_DIGEST_REQ_ORIGINS,
            )?;
            let origins = request
                .origins
                .into_iter()
                .map(|origin| decode_node_id(&origin))
                .collect::<Result<Vec<_>, RuntimeError>>()?;
            Ok(ControlFrame::DigestReq(origins))
        }
        _ => Err(RuntimeError::UnexpectedFrame {
            frame_type: frame.frame_type,
            channel: frame.channel,
        }),
    }
}

fn encode_lsa(lsa: &Lsa) -> Vec<u8> {
    proto::Lsa {
        origin: lsa.origin.as_bytes().to_vec(),
        seq: lsa.seq,
        ttl_sec: lsa.ttl_sec,
        adjacencies: lsa
            .adjacencies
            .iter()
            .map(|adjacency| proto::Adjacency {
                peer: adjacency.peer.as_bytes().to_vec(),
                cost: u32::from(adjacency.cost.get()),
            })
            .collect(),
        epoch: lsa.epoch,
    }
    .encode_to_vec()
}

fn decode_lsa(wire_lsa: proto::Lsa) -> Result<Lsa, RuntimeError> {
    let origin = decode_node_id(&wire_lsa.origin)?;
    if !(1..=MAX_LSA_TTL_SEC).contains(&wire_lsa.ttl_sec) {
        return Err(RuntimeError::InvalidLsaTtl(wire_lsa.ttl_sec));
    }
    ensure_limit(
        "Lsa.adjacencies",
        wire_lsa.adjacencies.len(),
        MAX_LSA_ADJACENCIES,
    )?;
    let adjacencies = wire_lsa
        .adjacencies
        .into_iter()
        .map(|adjacency| {
            let peer = decode_node_id(&adjacency.peer)?;
            let raw_cost = u16::try_from(adjacency.cost)
                .map_err(|_| RuntimeError::InvalidLinkCost(adjacency.cost))?;
            let cost = LinkCost::new(raw_cost)
                .map_err(|InvalidLinkCost| RuntimeError::InvalidLinkCost(adjacency.cost))?;
            Ok(Adjacency { peer, cost })
        })
        .collect::<Result<Vec<_>, RuntimeError>>()?;
    Ok(Lsa {
        origin,
        epoch: wire_lsa.epoch,
        seq: wire_lsa.seq,
        ttl_sec: wire_lsa.ttl_sec,
        adjacencies,
    })
}

fn ensure_limit(field: &'static str, count: usize, limit: usize) -> Result<(), RuntimeError> {
    if count > limit {
        Err(RuntimeError::TooManyElements {
            field,
            count,
            limit,
        })
    } else {
        Ok(())
    }
}

fn decode_node_id(bytes: &[u8]) -> Result<NodeId, RuntimeError> {
    let raw: [u8; 32] = bytes
        .try_into()
        .map_err(|_| RuntimeError::InvalidNodeIdLength(bytes.len()))?;
    Ok(NodeId::from_bytes(raw))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn node(value: u8) -> NodeId {
        let mut bytes = [0; 32];
        bytes[31] = value;
        NodeId::from_bytes(bytes)
    }

    #[test]
    fn protobuf_control_frame_round_trips_and_preserves_canonical_bytes() {
        let original_lsa = Lsa {
            origin: node(1),
            epoch: 7,
            seq: 42,
            ttl_sec: 300,
            adjacencies: vec![Adjacency {
                peer: node(2),
                cost: LinkCost::new(9).expect("cost must be valid"),
            }],
        };
        let frame = ControlFrame::Lsa(LsaMessage {
            lsa: original_lsa.clone(),
            canonical_bytes: Arc::from([]),
            signature: Arc::from([1, 2, 3]),
        });

        let wire = encode_control_frame(&frame).expect("control frame must encode");
        let decoded = decode_control_frame(wire).expect("control frame must decode");
        let ControlFrame::Lsa(message) = decoded else {
            panic!("decoded frame must remain an LSA");
        };
        assert_eq!(message.lsa, original_lsa);
        assert!(!message.canonical_bytes.is_empty());
        assert_eq!(message.signature.as_ref(), &[1, 2, 3]);

        let reencoded = encode_control_frame(&ControlFrame::Lsa(message.clone()))
            .expect("forwarded frame must encode");
        let signed =
            proto::SignedLsa::decode(reencoded.payload).expect("forwarded SignedLsa must decode");
        assert_eq!(signed.lsa_bytes, message.canonical_bytes.as_ref());
    }

    #[test]
    fn digest_frames_round_trip() {
        let frames = [
            ControlFrame::Digest(vec![DigestEntry {
                origin: node(1),
                epoch: 3,
                seq: 99,
            }]),
            ControlFrame::DigestReq(vec![node(1), node(2)]),
        ];

        for frame in frames {
            let wire = encode_control_frame(&frame).expect("digest frame must encode");
            let decoded = decode_control_frame(wire).expect("digest frame must decode");
            assert_eq!(decoded, frame);
        }
    }

    #[test]
    fn malformed_domain_values_are_rejected() {
        let invalid = proto::Lsa {
            origin: vec![0; 31],
            seq: 1,
            ttl_sec: 300,
            adjacencies: Vec::new(),
            epoch: 1,
        };
        let signed = proto::SignedLsa {
            lsa_bytes: invalid.encode_to_vec(),
            signature: Vec::new(),
        };
        let frame = WireFrame::control(FrameType::Lsa, signed.encode_to_vec());

        assert!(matches!(
            decode_control_frame(frame),
            Err(RuntimeError::InvalidNodeIdLength(31))
        ));
    }

    #[test]
    fn control_collection_limits_are_enforced_on_encode_and_decode() {
        let oversized_digest = ControlFrame::Digest(vec![
            DigestEntry {
                origin: node(1),
                epoch: 1,
                seq: 1,
            };
            MAX_DIGEST_ENTRIES + 1
        ]);
        assert!(matches!(
            encode_control_frame(&oversized_digest),
            Err(RuntimeError::TooManyElements {
                field: "Digest.entries",
                ..
            })
        ));

        let digest = proto::Digest {
            entries: vec![
                proto::DigestEntry {
                    origin: node(1).as_bytes().to_vec(),
                    epoch: 1,
                    seq: 1,
                };
                MAX_DIGEST_ENTRIES + 1
            ],
        };
        let wire = WireFrame::control(FrameType::Digest, digest.encode_to_vec());
        assert!(matches!(
            decode_control_frame(wire),
            Err(RuntimeError::TooManyElements {
                field: "Digest.entries",
                ..
            })
        ));

        let invalid_ttl = ControlFrame::Lsa(LsaMessage {
            lsa: Lsa {
                origin: node(1),
                epoch: 1,
                seq: 1,
                ttl_sec: 0,
                adjacencies: Vec::new(),
            },
            canonical_bytes: Arc::from([]),
            signature: Arc::from([]),
        });
        assert!(matches!(
            encode_control_frame(&invalid_ttl),
            Err(RuntimeError::InvalidLsaTtl(0))
        ));
    }

    #[test]
    fn file_sequence_store_round_trips_monotonically() {
        static NEXT_DIRECTORY: AtomicU64 = AtomicU64::new(0);
        let directory = std::env::temp_dir().join(format!(
            "mb-runtime-seq-test-{}-{}",
            std::process::id(),
            NEXT_DIRECTORY.fetch_add(1, Ordering::Relaxed)
        ));
        let path = directory.join("seq");
        let store = FileSeqStore::new(&path);

        assert_eq!(store.load().expect("missing store starts at zero"), 0);
        store.persist(41).expect("sequence must persist");
        assert_eq!(
            FileSeqStore::new(&path)
                .load()
                .expect("sequence must reload"),
            41
        );
        assert!(store.persist(40).is_err());
        assert_eq!(
            store.load().expect("failed write must not change value"),
            41
        );

        fs::remove_dir_all(&directory).expect("temporary sequence directory must be removable");
    }

    #[test]
    fn restart_continues_immediately_after_the_saved_sequence() {
        let saved = 41;
        let mut plane = ControlPlane::new_unsecured_with_seq(node(1), 1, saved);
        let actions = plane.handle(
            MonoTime::ZERO,
            ControlEvent::LinkUp {
                link: LinkId::new(1),
                peer: node(2),
                cost: LinkCost::new(1).expect("cost is valid"),
            },
        );

        assert!(matches!(
            actions.first(),
            Some(ControlAction::PersistSeq(seq)) if *seq == saved + 1
        ));
    }
}
