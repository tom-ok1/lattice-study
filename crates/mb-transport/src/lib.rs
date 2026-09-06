//! A minimal, deliberately insecure TCP transport for static peers.
//!
//! It knows about one-hop links and wire frames, but not control-plane LSAs or
//! routes. Peer identity is asserted by `LinkHello`; mTLS will authenticate it
//! in the security milestone.

use bytes::BytesMut;
use mb_types::{LinkId, NodeId};
use mb_wire::{proto, Channel, FrameDecoder, FrameEncoder, FrameError, FrameType, WireFrame};
use prost::Message;
use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, watch, Mutex};
use tokio::task::JoinHandle;

const LINK_QUEUE_CAPACITY: usize = 8;
const EVENT_QUEUE_CAPACITY: usize = 256;
const READ_CHUNK_CAPACITY: usize = 16 * 1024;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LinkEvent {
    Up {
        link: LinkId,
        peer: NodeId,
    },
    Down {
        link: LinkId,
    },
    Frame {
        link: LinkId,
        frame: WireFrame,
    },
    Sent {
        link: LinkId,
        frame_type: FrameType,
        channel: Channel,
        payload_bytes: usize,
    },
}

#[derive(Debug)]
pub enum TransportError {
    Io(io::Error),
    Wire(FrameError),
    Protobuf(prost::DecodeError),
    UnexpectedEndOfStream,
    ExpectedLinkHello,
    UnsupportedProtocolVersion(u32),
    InvalidNodeIdLength(usize),
    PeerMismatch { expected: NodeId, actual: NodeId },
    PeerNotAllowed(NodeId),
    LinkIdExhausted,
    UnknownLink(LinkId),
    LinkClosed(LinkId),
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(f, "TCP I/O failed: {error}"),
            Self::Wire(error) => write!(f, "wire frame is invalid: {error}"),
            Self::Protobuf(error) => write!(f, "LinkHello protobuf is invalid: {error}"),
            Self::UnexpectedEndOfStream => write!(f, "TCP stream ended before LinkHello arrived"),
            Self::ExpectedLinkHello => write!(f, "the first TCP frame must be LinkHello"),
            Self::UnsupportedProtocolVersion(version) => {
                write!(f, "peer uses unsupported protocol version {version}")
            }
            Self::InvalidNodeIdLength(length) => {
                write!(f, "peer NodeId must be 32 bytes, got {length}")
            }
            Self::PeerMismatch { expected, actual } => {
                write!(f, "expected peer {expected}, got {actual}")
            }
            Self::PeerNotAllowed(peer) => write!(f, "peer {peer} is not in the static allowlist"),
            Self::LinkIdExhausted => write!(f, "local LinkId space is exhausted"),
            Self::UnknownLink(link) => write!(f, "unknown link {}", link.get()),
            Self::LinkClosed(link) => write!(f, "link {} is closed", link.get()),
        }
    }
}

impl Error for TransportError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::Wire(error) => Some(error),
            Self::Protobuf(error) => Some(error),
            _ => None,
        }
    }
}

impl From<io::Error> for TransportError {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}

impl From<FrameError> for TransportError {
    fn from(value: FrameError) -> Self {
        Self::Wire(value)
    }
}

impl From<prost::DecodeError> for TransportError {
    fn from(value: prost::DecodeError) -> Self {
        Self::Protobuf(value)
    }
}

enum LinkCommand {
    Send(WireFrame),
    Close,
}

struct Inner {
    local_node: NodeId,
    local_addr: SocketAddr,
    allowed_peers: BTreeSet<NodeId>,
    next_link: AtomicU64,
    links: Mutex<BTreeMap<LinkId, mpsc::Sender<LinkCommand>>>,
    events: mpsc::Sender<LinkEvent>,
    link_count: watch::Sender<usize>,
    shutdown: watch::Sender<bool>,
    tasks: Mutex<Vec<JoinHandle<()>>>,
}

/// A TCP listener plus its currently established one-hop links.
#[derive(Clone)]
pub struct TcpEndpoint {
    inner: Arc<Inner>,
}

impl TcpEndpoint {
    pub async fn bind(
        local_node: NodeId,
        bind_addr: SocketAddr,
        allowed_peers: impl IntoIterator<Item = NodeId>,
    ) -> Result<(Self, mpsc::Receiver<LinkEvent>), TransportError> {
        let listener = TcpListener::bind(bind_addr).await?;
        let local_addr = listener.local_addr()?;
        let (event_tx, event_rx) = mpsc::channel(EVENT_QUEUE_CAPACITY);
        let (link_count, _) = watch::channel(0);
        let (shutdown, shutdown_rx) = watch::channel(false);
        let inner = Arc::new(Inner {
            local_node,
            local_addr,
            allowed_peers: allowed_peers.into_iter().collect(),
            next_link: AtomicU64::new(1),
            links: Mutex::new(BTreeMap::new()),
            events: event_tx,
            link_count,
            shutdown,
            tasks: Mutex::new(Vec::new()),
        });

        let endpoint = Self {
            inner: Arc::clone(&inner),
        };
        let accept_task = tokio::spawn(run_accept_loop(inner, listener, shutdown_rx));
        endpoint.inner.tasks.lock().await.push(accept_task);
        Ok((endpoint, event_rx))
    }

    pub fn local_node(&self) -> NodeId {
        self.inner.local_node
    }

    pub fn local_addr(&self) -> SocketAddr {
        self.inner.local_addr
    }

    pub fn subscribe_link_count(&self) -> watch::Receiver<usize> {
        self.inner.link_count.subscribe()
    }

    pub async fn connect(
        &self,
        expected_peer: NodeId,
        peer_addr: SocketAddr,
    ) -> Result<LinkId, TransportError> {
        if !self.inner.allowed_peers.contains(&expected_peer) {
            return Err(TransportError::PeerNotAllowed(expected_peer));
        }
        let stream = TcpStream::connect(peer_addr).await?;
        attach_stream(Arc::clone(&self.inner), stream, Some(expected_peer)).await
    }

    pub async fn send(&self, link: LinkId, frame: WireFrame) -> Result<(), TransportError> {
        let sender = self
            .inner
            .links
            .lock()
            .await
            .get(&link)
            .cloned()
            .ok_or(TransportError::UnknownLink(link))?;
        sender
            .send(LinkCommand::Send(frame))
            .await
            .map_err(|_| TransportError::LinkClosed(link))
    }

    pub async fn close(&self, link: LinkId) -> Result<(), TransportError> {
        let sender = self
            .inner
            .links
            .lock()
            .await
            .get(&link)
            .cloned()
            .ok_or(TransportError::UnknownLink(link))?;
        sender
            .send(LinkCommand::Close)
            .await
            .map_err(|_| TransportError::LinkClosed(link))
    }

    pub async fn shutdown(&self) {
        self.inner.shutdown.send_replace(true);
        let tasks = std::mem::take(&mut *self.inner.tasks.lock().await);
        for task in &tasks {
            task.abort();
        }
        for task in tasks {
            let _ = task.await;
        }
        self.inner.links.lock().await.clear();
        self.inner.link_count.send_replace(0);
    }
}

async fn run_accept_loop(
    inner: Arc<Inner>,
    listener: TcpListener,
    mut shutdown: watch::Receiver<bool>,
) {
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            accepted = listener.accept() => {
                let Ok((stream, _)) = accepted else {
                    break;
                };
                let connection_inner = Arc::clone(&inner);
                let task = tokio::spawn(async move {
                    let _ = attach_stream(connection_inner, stream, None).await;
                });
                inner.tasks.lock().await.push(task);
            }
        }
    }
}

async fn attach_stream(
    inner: Arc<Inner>,
    mut stream: TcpStream,
    expected_peer: Option<NodeId>,
) -> Result<LinkId, TransportError> {
    stream.set_nodelay(true)?;
    let (peer, buffered) = exchange_hello(&mut stream, inner.local_node).await?;
    if let Some(expected) = expected_peer {
        if peer != expected {
            return Err(TransportError::PeerMismatch {
                expected,
                actual: peer,
            });
        }
    }
    if !inner.allowed_peers.contains(&peer) {
        return Err(TransportError::PeerNotAllowed(peer));
    }

    let raw_link = inner
        .next_link
        .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
            current.checked_add(1)
        })
        .map_err(|_| TransportError::LinkIdExhausted)?;
    let link = LinkId::new(raw_link);
    let (command_tx, command_rx) = mpsc::channel(LINK_QUEUE_CAPACITY);
    {
        let mut links = inner.links.lock().await;
        links.insert(link, command_tx);
        inner.link_count.send_replace(links.len());
    }
    if inner
        .events
        .send(LinkEvent::Up { link, peer })
        .await
        .is_err()
    {
        remove_link(&inner, link).await;
        return Err(TransportError::LinkClosed(link));
    }

    let worker_inner = Arc::clone(&inner);
    let worker = tokio::spawn(async move {
        run_link(worker_inner, link, stream, buffered, command_rx).await;
    });
    inner.tasks.lock().await.push(worker);
    Ok(link)
}

async fn exchange_hello(
    stream: &mut TcpStream,
    local_node: NodeId,
) -> Result<(NodeId, BytesMut), TransportError> {
    let hello = proto::LinkHello {
        node_id: local_node.as_bytes().to_vec(),
        protocol_version: u32::from(mb_wire::FRAME_VERSION),
    };
    let frame = WireFrame::control(FrameType::LinkHello, hello.encode_to_vec());
    let encoded = FrameEncoder::encode(&frame)?;
    stream.write_all(&encoded).await?;

    let mut buffered = BytesMut::with_capacity(READ_CHUNK_CAPACITY);
    let mut decoder = FrameDecoder;
    let frame = loop {
        if let Some(frame) = decoder.decode(&mut buffered)? {
            break frame;
        }
        if stream.read_buf(&mut buffered).await? == 0 {
            return Err(TransportError::UnexpectedEndOfStream);
        }
    };
    if frame.frame_type != FrameType::LinkHello || frame.channel != Channel::Control {
        return Err(TransportError::ExpectedLinkHello);
    }
    let peer_hello = proto::LinkHello::decode(frame.payload)?;
    if peer_hello.protocol_version != u32::from(mb_wire::FRAME_VERSION) {
        return Err(TransportError::UnsupportedProtocolVersion(
            peer_hello.protocol_version,
        ));
    }
    let peer = node_id_from_bytes(&peer_hello.node_id)?;
    Ok((peer, buffered))
}

fn node_id_from_bytes(bytes: &[u8]) -> Result<NodeId, TransportError> {
    let raw: [u8; 32] = bytes
        .try_into()
        .map_err(|_| TransportError::InvalidNodeIdLength(bytes.len()))?;
    Ok(NodeId::from_bytes(raw))
}

async fn run_link(
    inner: Arc<Inner>,
    link: LinkId,
    stream: TcpStream,
    mut buffered: BytesMut,
    mut commands: mpsc::Receiver<LinkCommand>,
) {
    let (mut reader, mut writer) = stream.into_split();
    let mut decoder = FrameDecoder;
    let mut shutdown = inner.shutdown.subscribe();

    'connection: loop {
        loop {
            match decoder.decode(&mut buffered) {
                Ok(Some(frame)) => {
                    if inner
                        .events
                        .send(LinkEvent::Frame { link, frame })
                        .await
                        .is_err()
                    {
                        break 'connection;
                    }
                }
                Ok(None) => break,
                Err(_) => break 'connection,
            }
        }

        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            command = commands.recv() => {
                match command {
                    Some(LinkCommand::Send(frame)) => {
                        let frame_type = frame.frame_type;
                        let channel = frame.channel;
                        let payload_bytes = frame.payload.len();
                        let Ok(encoded) = FrameEncoder::encode(&frame) else {
                            break;
                        };
                        if writer.write_all(&encoded).await.is_err() {
                            break;
                        }
                        if inner.events.send(LinkEvent::Sent {
                            link,
                            frame_type,
                            channel,
                            payload_bytes,
                        }).await.is_err() {
                            break;
                        }
                    }
                    Some(LinkCommand::Close) | None => break,
                }
            }
            read = reader.read_buf(&mut buffered) => {
                match read {
                    Ok(0) | Err(_) => break,
                    Ok(_) => {}
                }
            }
        }
    }

    remove_link(&inner, link).await;
    let _ = inner.events.send(LinkEvent::Down { link }).await;
}

async fn remove_link(inner: &Inner, link: LinkId) {
    let mut links = inner.links.lock().await;
    links.remove(&link);
    inner.link_count.send_replace(links.len());
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use tokio::time::{timeout, Duration};

    fn node(value: u8) -> NodeId {
        let mut bytes = [0; 32];
        bytes[31] = value;
        NodeId::from_bytes(bytes)
    }

    #[tokio::test]
    async fn tcp_link_exchanges_incrementally_decoded_frames() {
        let a = node(1);
        let b = node(2);
        let bind_addr = "127.0.0.1:0".parse().expect("loopback address must parse");
        let (endpoint_a, mut events_a) = TcpEndpoint::bind(a, bind_addr, [b])
            .await
            .expect("A must bind");
        let (endpoint_b, mut events_b) = TcpEndpoint::bind(b, bind_addr, [a])
            .await
            .expect("B must bind");

        let link_a = endpoint_a
            .connect(b, endpoint_b.local_addr())
            .await
            .expect("A must connect to B");
        assert!(matches!(events_a.recv().await, Some(LinkEvent::Up { peer, .. }) if peer == b));
        let link_b = match timeout(Duration::from_secs(2), events_b.recv())
            .await
            .expect("B must observe the link")
            .expect("B event channel must remain open")
        {
            LinkEvent::Up { link, peer } => {
                assert_eq!(peer, a);
                link
            }
            other => panic!("expected link up, got {other:?}"),
        };

        let sent = WireFrame::control(FrameType::Lsa, Bytes::from_static(b"protobuf"));
        endpoint_a
            .send(link_a, sent.clone())
            .await
            .expect("frame must enqueue");
        let received = timeout(Duration::from_secs(2), events_b.recv())
            .await
            .expect("B must receive the frame")
            .expect("B event channel must remain open");
        assert_eq!(
            received,
            LinkEvent::Frame {
                link: link_b,
                frame: sent
            }
        );
        let completion = timeout(Duration::from_secs(2), events_a.recv())
            .await
            .expect("A must observe send completion")
            .expect("A event channel must remain open");
        assert_eq!(
            completion,
            LinkEvent::Sent {
                link: link_a,
                frame_type: FrameType::Lsa,
                channel: Channel::Control,
                payload_bytes: b"protobuf".len(),
            }
        );

        endpoint_a.shutdown().await;
        endpoint_b.shutdown().await;
    }

    #[tokio::test]
    async fn outbound_peer_identity_must_match_static_configuration() {
        let a = node(1);
        let b = node(2);
        let impostor = node(3);
        let bind_addr = "127.0.0.1:0".parse().expect("loopback address must parse");
        let (endpoint_a, _events_a) = TcpEndpoint::bind(a, bind_addr, [b, impostor])
            .await
            .expect("A must bind");
        let (endpoint_b, _events_b) = TcpEndpoint::bind(b, bind_addr, [a])
            .await
            .expect("B must bind");

        let error = endpoint_a
            .connect(impostor, endpoint_b.local_addr())
            .await
            .expect_err("the asserted identity must be checked");
        assert!(matches!(
            error,
            TransportError::PeerMismatch { expected, actual }
                if expected == impostor && actual == b
        ));

        endpoint_a.shutdown().await;
        endpoint_b.shutdown().await;
    }
}
