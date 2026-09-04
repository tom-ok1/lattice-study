//! Tokio glue that drives the I/O-free control plane from transport events.

use bytes::Bytes;
use mb_control::{
    Adjacency, ControlAction, ControlEvent, ControlFrame, ControlPlane, InvalidLinkCost, LinkCost,
    Lsa, LsaMessage, RouteTable,
};
use mb_transport::{LinkEvent, TcpEndpoint, TransportError};
use mb_types::{Component, LinkId, MonoTime, NodeId};
use mb_wire::{proto, Channel, FrameType, WireFrame};
use prost::Message;
use std::collections::BTreeMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;
use tokio::sync::{mpsc, watch};
use tokio::task::JoinHandle;
use tokio::time::Instant;

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
            Self::Transport(error) => write!(f, "transport operation failed: {error}"),
        }
    }
}

impl Error for RuntimeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Protobuf(error) => Some(error),
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
        if endpoint.local_node() != config.node_id {
            return Err(RuntimeError::EndpointNodeMismatch {
                endpoint: endpoint.local_node(),
                configured: config.node_id,
            });
        }

        let plane = ControlPlane::new_unsecured(config.node_id, config.epoch);
        let initial = RuntimeSnapshot {
            node_id: config.node_id,
            lsdb_entries: plane.lsdb_len(),
            routes: plane.route_table(),
            persisted_seq: 0,
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
) {
    let started_at = Instant::now();
    let mut persisted_seq = 0;
    let mut peers = BTreeMap::new();

    while let Some(link_event) = events.recv().await {
        let event = match link_event {
            LinkEvent::Up { link, peer } => {
                let Some(cost) = config.peer_costs.get(&peer).copied() else {
                    let _ = endpoint.close(link).await;
                    continue;
                };
                peers.insert(link, peer);
                ControlEvent::LinkUp { link, peer, cost }
            }
            LinkEvent::Down { link } => {
                peers.remove(&link);
                ControlEvent::LinkDown { link }
            }
            LinkEvent::Frame { link, frame } => match decode_control_frame(frame) {
                Ok(frame) => ControlEvent::Frame { link, frame },
                Err(_) => {
                    let _ = endpoint.close(link).await;
                    continue;
                }
            },
        };

        let elapsed = started_at.elapsed().as_millis();
        let now_ms = u64::try_from(elapsed).unwrap_or(u64::MAX);
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
                ControlAction::PublishRoutes(_) => {}
                ControlAction::PersistSeq(seq) => persisted_seq = seq,
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

fn encode_control_frame(frame: &ControlFrame) -> Result<WireFrame, RuntimeError> {
    match frame {
        ControlFrame::Lsa(message) => {
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
    }
}

fn decode_control_frame(frame: WireFrame) -> Result<ControlFrame, RuntimeError> {
    if frame.channel != Channel::Control || frame.frame_type != FrameType::Lsa {
        return Err(RuntimeError::UnexpectedFrame {
            frame_type: frame.frame_type,
            channel: frame.channel,
        });
    }
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

fn encode_lsa(lsa: &Lsa) -> Vec<u8> {
    proto::Lsa {
        origin: lsa.origin.as_bytes().to_vec(),
        seq: lsa.seq,
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
        adjacencies,
    })
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

    fn node(value: u8) -> NodeId {
        let mut bytes = [0; 32];
        bytes[31] = value;
        NodeId::from_bytes(bytes)
    }

    #[test]
    fn protobuf_control_frame_round_trips_and_preserves_canonical_bytes() {
        let frame = ControlFrame::Lsa(LsaMessage {
            lsa: Lsa {
                origin: node(1),
                epoch: 7,
                seq: 42,
                adjacencies: vec![Adjacency {
                    peer: node(2),
                    cost: LinkCost::new(9).expect("cost must be valid"),
                }],
            },
            canonical_bytes: Arc::from([]),
            signature: Arc::from([1, 2, 3]),
        });

        let wire = encode_control_frame(&frame).expect("control frame must encode");
        let decoded = decode_control_frame(wire).expect("control frame must decode");
        let ControlFrame::Lsa(message) = decoded;
        assert_eq!(
            message.lsa,
            match frame {
                ControlFrame::Lsa(message) => message.lsa,
            }
        );
        assert!(!message.canonical_bytes.is_empty());
        assert_eq!(message.signature.as_ref(), &[1, 2, 3]);

        let reencoded = encode_control_frame(&ControlFrame::Lsa(message.clone()))
            .expect("forwarded frame must encode");
        let signed =
            proto::SignedLsa::decode(reencoded.payload).expect("forwarded SignedLsa must decode");
        assert_eq!(signed.lsa_bytes, message.canonical_bytes.as_ref());
    }

    #[test]
    fn malformed_domain_values_are_rejected() {
        let invalid = proto::Lsa {
            origin: vec![0; 31],
            seq: 1,
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
}
