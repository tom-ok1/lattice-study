use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use bytes::Bytes;
use mb_control::LinkCost;
use mb_runtime::{ControlRuntime, FileSeqStore, ForwardOutcome, RuntimeConfig, RuntimeSnapshot};
use mb_transport::TcpEndpoint;
use mb_types::{LinkId, NodeId};
use mb_wire::{ForwardFlags, ForwardHeader, ForwardPacket, PacketType, Priority};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, VecDeque};
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

type ApiError = (StatusCode, String);

#[derive(Clone)]
struct AppState {
    name: String,
    numeric_id: u64,
    mesh_addr: SocketAddr,
    runtime: Arc<ControlRuntime>,
    outcomes: Arc<RwLock<VecDeque<OutcomeResponse>>>,
    started_at: Instant,
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
    node_id: u64,
    name: String,
    mesh_addr: String,
}

#[derive(Serialize)]
struct SnapshotResponse {
    id: u64,
    name: String,
    mesh_addr: String,
    now_ms: u64,
    peers: Vec<PeerResponse>,
    lsdb: Vec<LsaResponse>,
    routes: Vec<RouteResponse>,
    queues: Vec<QueueResponse>,
    events: Vec<EventResponse>,
    outcomes: Vec<OutcomeResponse>,
}

#[derive(Serialize)]
struct PeerResponse {
    link_id: u64,
    peer: u64,
}

#[derive(Serialize)]
struct LsaResponse {
    origin: u64,
    epoch: u32,
    sequence: u64,
    adjacencies: Vec<u64>,
}

#[derive(Serialize)]
struct RouteResponse {
    destination: u64,
    next_hop: u64,
    cost: u32,
    hops: u8,
}

#[derive(Serialize)]
struct QueueResponse {
    link_id: u64,
    packets: [usize; 4],
    bytes: [usize; 4],
}

#[derive(Serialize)]
struct EventResponse {
    sequence: u64,
    at_ms: u64,
    kind: &'static str,
    detail: String,
    link_id: Option<u64>,
    peer: Option<u64>,
}

#[derive(Clone, Serialize)]
struct OutcomeResponse {
    sequence: u64,
    at_ms: u64,
    kind: &'static str,
    detail: String,
    source: u64,
    destination: Option<u64>,
    priority: u8,
    flow_id: u64,
    payload_bytes: Option<usize>,
    payload_preview: String,
}

#[derive(Deserialize)]
struct AllowPeerRequest {
    peer: u64,
    cost: u16,
}

#[derive(Deserialize)]
struct ConnectRequest {
    peer: u64,
    address: String,
    cost: u16,
}

#[derive(Serialize)]
struct ConnectResponse {
    link_id: u64,
}

#[derive(Deserialize)]
struct SendRequest {
    destination: u64,
    priority: u8,
    flow_id: u64,
    payload: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let numeric_id = parse_arg("--id", None)?.parse::<u64>()?;
    let name = parse_arg("--name", Some(format!("node-{numeric_id}")))?;
    let mesh_port = parse_arg("--mesh-port", Some("0".into()))?.parse::<u16>()?;
    let admin_port = parse_arg("--admin-port", Some("0".into()))?.parse::<u16>()?;
    let seq_path = parse_arg("--seq-path", None)?;
    let node_id = node_id(numeric_id);
    let loopback = IpAddr::V4(Ipv4Addr::LOCALHOST);

    let (endpoint, events) =
        TcpEndpoint::bind(node_id, SocketAddr::new(loopback, mesh_port), []).await?;
    let mesh_addr = endpoint.local_addr();
    let mut runtime = ControlRuntime::spawn_with_seq_store(
        RuntimeConfig {
            node_id,
            epoch: 1,
            peer_costs: BTreeMap::new(),
        },
        endpoint,
        events,
        Arc::new(FileSeqStore::new(seq_path)),
    )?;
    runtime.enable_diagnostics();
    let outcome_log = Arc::new(RwLock::new(VecDeque::new()));
    if let Some(mut outcomes) = runtime.take_forward_outcomes() {
        let outcome_log = Arc::clone(&outcome_log);
        let started_at = Instant::now();
        tokio::spawn(async move {
            let mut sequence = 0_u64;
            while let Some(outcome) = outcomes.recv().await {
                sequence = sequence.wrapping_add(1);
                let entry = outcome_response(sequence, elapsed_millis(started_at), outcome);
                let mut log = outcome_log.write().await;
                if log.len() == 128 {
                    log.pop_front();
                }
                log.push_back(entry);
            }
        });
    }
    let state = AppState {
        name,
        numeric_id,
        mesh_addr,
        runtime: Arc::new(runtime),
        outcomes: outcome_log,
        started_at: Instant::now(),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/snapshot", get(snapshot))
        .route("/peers/allow", post(allow_peer))
        .route("/connect", post(connect))
        .route("/links/:link/close", post(close_link))
        .route("/send", post(send_packet))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(SocketAddr::new(loopback, admin_port)).await?;
    let admin_addr = listener.local_addr()?;
    println!("mblab-node {numeric_id} mesh={mesh_addr} admin={admin_addr}");
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    Ok(())
}

async fn health(State(state): State<AppState>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        node_id: state.numeric_id,
        name: state.name,
        mesh_addr: state.mesh_addr.to_string(),
    })
}

async fn snapshot(State(state): State<AppState>) -> Json<SnapshotResponse> {
    let outcomes = state.outcomes.read().await.iter().cloned().collect();
    Json(snapshot_response(
        &state,
        state.runtime.snapshot(),
        outcomes,
    ))
}

async fn allow_peer(
    State(state): State<AppState>,
    Json(request): Json<AllowPeerRequest>,
) -> Result<StatusCode, ApiError> {
    let cost = LinkCost::new(request.cost)
        .map_err(|_| bad_request("link cost must be greater than zero"))?;
    state.runtime.allow_peer(node_id(request.peer), cost).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn connect(
    State(state): State<AppState>,
    Json(request): Json<ConnectRequest>,
) -> Result<Json<ConnectResponse>, ApiError> {
    let address = request
        .address
        .parse::<SocketAddr>()
        .map_err(|error| bad_request(format!("invalid peer address: {error}")))?;
    let cost = LinkCost::new(request.cost)
        .map_err(|_| bad_request("link cost must be greater than zero"))?;
    let link = state
        .runtime
        .connect(node_id(request.peer), address, cost)
        .await
        .map_err(internal_error)?;
    Ok(Json(ConnectResponse {
        link_id: link.get(),
    }))
}

async fn close_link(
    State(state): State<AppState>,
    Path(link): Path<u64>,
) -> Result<StatusCode, ApiError> {
    state
        .runtime
        .close_link(LinkId::new(link))
        .await
        .map_err(internal_error)?;
    Ok(StatusCode::NO_CONTENT)
}

async fn send_packet(
    State(state): State<AppState>,
    Json(request): Json<SendRequest>,
) -> Result<StatusCode, ApiError> {
    let priority = Priority::try_from(request.priority)
        .map_err(|_| bad_request("priority must be between 0 and 3"))?;
    let packet = ForwardPacket {
        header: ForwardHeader {
            packet_type: PacketType::Unicast,
            priority,
            ttl: 32,
            flags: ForwardFlags::empty(),
            destination: node_id(request.destination),
            source: node_id(state.numeric_id),
            flow_id: request.flow_id,
            conflate_key: 0,
        },
        multicast_destinations: Vec::new(),
        payload: Bytes::from(request.payload),
    };
    state.runtime.send(packet).await.map_err(internal_error)?;
    Ok(StatusCode::ACCEPTED)
}

fn snapshot_response(
    state: &AppState,
    snapshot: RuntimeSnapshot,
    outcomes: Vec<OutcomeResponse>,
) -> SnapshotResponse {
    let peers = snapshot.peers.clone();
    SnapshotResponse {
        id: state.numeric_id,
        name: state.name.clone(),
        mesh_addr: state.mesh_addr.to_string(),
        now_ms: elapsed_millis(state.started_at),
        peers: snapshot
            .peers
            .into_iter()
            .map(|(link, peer)| PeerResponse {
                link_id: link.get(),
                peer: numeric_id(peer),
            })
            .collect(),
        lsdb: snapshot
            .lsas
            .into_iter()
            .map(|lsa| LsaResponse {
                origin: numeric_id(lsa.origin),
                epoch: lsa.epoch,
                sequence: lsa.seq,
                adjacencies: lsa
                    .adjacencies
                    .into_iter()
                    .map(|adjacency| numeric_id(adjacency.peer))
                    .collect(),
            })
            .collect(),
        routes: snapshot
            .routes
            .iter()
            .map(|(destination, route)| RouteResponse {
                destination: numeric_id(*destination),
                next_hop: route.next_hop.get(),
                cost: route.cost,
                hops: route.hops,
            })
            .collect(),
        queues: snapshot
            .queues
            .into_iter()
            .map(|(link, queue)| QueueResponse {
                link_id: link.get(),
                packets: queue.packet_counts,
                bytes: queue.queued_bytes,
            })
            .collect(),
        events: snapshot
            .recent_events
            .into_iter()
            .map(|event| EventResponse {
                sequence: event.sequence,
                at_ms: event.at.as_millis(),
                kind: event.kind,
                detail: event.detail,
                link_id: event.link.map(LinkId::get),
                peer: event
                    .link
                    .and_then(|link| peers.get(&link).copied())
                    .map(numeric_id),
            })
            .collect(),
        outcomes,
    }
}

fn outcome_response(sequence: u64, at_ms: u64, outcome: ForwardOutcome) -> OutcomeResponse {
    match outcome {
        ForwardOutcome::Delivered(packet) => packet_outcome(
            sequence,
            at_ms,
            "DELIVERED",
            "application delivery".into(),
            packet,
        ),
        ForwardOutcome::Dropped {
            reason,
            priority,
            flow_id,
            source,
        } => OutcomeResponse {
            sequence,
            at_ms,
            kind: "DROPPED",
            detail: format!("{reason:?}"),
            source: numeric_id(source),
            destination: None,
            priority: priority as u8,
            flow_id,
            payload_bytes: None,
            payload_preview: String::new(),
        },
        ForwardOutcome::Backpressure { link, packet } => packet_outcome(
            sequence,
            at_ms,
            "BACKPRESSURE",
            format!("link {}", link.get()),
            packet,
        ),
    }
}

fn packet_outcome(
    sequence: u64,
    at_ms: u64,
    kind: &'static str,
    detail: String,
    packet: ForwardPacket,
) -> OutcomeResponse {
    let preview_len = packet.payload.len().min(96);
    OutcomeResponse {
        sequence,
        at_ms,
        kind,
        detail,
        source: numeric_id(packet.header.source),
        destination: Some(numeric_id(packet.header.destination)),
        priority: packet.header.priority as u8,
        flow_id: packet.header.flow_id,
        payload_bytes: Some(packet.payload.len()),
        payload_preview: String::from_utf8_lossy(&packet.payload[..preview_len]).into_owned(),
    }
}

fn elapsed_millis(started_at: Instant) -> u64 {
    u64::try_from(started_at.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn node_id(value: u64) -> NodeId {
    let mut bytes = [0_u8; 32];
    bytes[24..].copy_from_slice(&value.to_be_bytes());
    NodeId::from_bytes(bytes)
}

fn numeric_id(value: NodeId) -> u64 {
    let mut bytes = [0_u8; 8];
    bytes.copy_from_slice(&value.as_bytes()[24..]);
    u64::from_be_bytes(bytes)
}

fn parse_arg(flag: &str, default: Option<String>) -> Result<String, Box<dyn std::error::Error>> {
    let mut args = env::args();
    while let Some(argument) = args.next() {
        if argument == flag {
            return args
                .next()
                .ok_or_else(|| format!("missing value for {flag}").into());
        }
    }
    default.ok_or_else(|| format!("missing required argument {flag}").into())
}

fn bad_request(message: impl Into<String>) -> ApiError {
    (StatusCode::BAD_REQUEST, message.into())
}

fn internal_error(error: impl std::fmt::Display) -> ApiError {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn numeric_node_ids_round_trip() {
        for value in [0, 1, 42, u64::MAX] {
            assert_eq!(numeric_id(node_id(value)), value);
        }
    }
}
