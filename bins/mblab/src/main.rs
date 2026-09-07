use axum::{
    extract::{Path, State},
    http::StatusCode,
    response::Html,
    routing::{delete, get, patch, post},
    Json, Router,
};
use bytes::BytesMut;
use mb_wire::{FrameDecoder, FrameEncoder, FrameType};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process::{self, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tokio::sync::{watch, Mutex};
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{sleep, Duration};

type ApiError = (StatusCode, String);

#[derive(Clone, Serialize)]
struct NodeInfo {
    id: u64,
    name: String,
    mesh_addr: String,
    admin_addr: String,
    pid: Option<u32>,
}

struct ManagedNode {
    info: NodeInfo,
    child: Option<Child>,
    started_offset_ms: u64,
}

#[derive(Clone, Serialize)]
struct LabLink {
    id: u64,
    a: u64,
    b: u64,
    up: bool,
    #[serde(skip)]
    desired_up: bool,
    cost: u16,
    latency_ms: u64,
    loss_percent: f64,
    bandwidth_kbps: u64,
}

struct Lab {
    started_at: std::time::Instant,
    node_path: PathBuf,
    state_dir: PathBuf,
    client: Client,
    nodes: Mutex<BTreeMap<u64, ManagedNode>>,
    links: Mutex<BTreeMap<u64, LabLink>>,
    proxies: Mutex<BTreeMap<u64, LinkProxyHandle>>,
    next_node: AtomicU64,
    next_link: AtomicU64,
    next_flow: AtomicU64,
    pace_ms: AtomicU64,
}

#[derive(Clone, Deserialize, Serialize)]
struct NodeSnapshot {
    id: u64,
    name: String,
    mesh_addr: String,
    now_ms: u64,
    peers: Vec<PeerSnapshot>,
    lsdb: Vec<LsaSnapshot>,
    routes: Vec<RouteSnapshot>,
    queues: Vec<QueueSnapshot>,
    events: Vec<EventSnapshot>,
    #[serde(default)]
    outcomes: Vec<OutcomeSnapshot>,
}

#[derive(Clone, Deserialize, Serialize)]
struct PeerSnapshot {
    link_id: u64,
    peer: u64,
}

#[derive(Clone, Deserialize, Serialize)]
struct LsaSnapshot {
    origin: u64,
    epoch: u32,
    sequence: u64,
    adjacencies: Vec<u64>,
}

#[derive(Clone, Deserialize, Serialize)]
struct RouteSnapshot {
    destination: u64,
    next_hop: u64,
    cost: u32,
    hops: u8,
}

#[derive(Clone, Deserialize, Serialize)]
struct QueueSnapshot {
    link_id: u64,
    packets: [usize; 4],
    bytes: [usize; 4],
}

#[derive(Clone, Deserialize, Serialize)]
struct EventSnapshot {
    sequence: u64,
    at_ms: u64,
    kind: String,
    detail: String,
    link_id: Option<u64>,
    peer: Option<u64>,
}

#[derive(Clone, Deserialize, Serialize)]
struct OutcomeSnapshot {
    sequence: u64,
    at_ms: u64,
    kind: String,
    detail: String,
    source: u64,
    destination: Option<u64>,
    priority: u8,
    flow_id: u64,
    payload_bytes: Option<usize>,
    payload_preview: String,
}

#[derive(Serialize)]
struct LabNodeState {
    #[serde(flatten)]
    info: NodeInfo,
    running: bool,
    snapshot: Option<NodeSnapshot>,
}

#[derive(Serialize)]
struct LabEvent {
    node: u64,
    name: String,
    sequence: u64,
    at_ms: u64,
    kind: String,
    detail: String,
    link_id: Option<u64>,
    peer: Option<u64>,
}

#[derive(Serialize)]
struct LabStateResponse {
    converged: bool,
    components: usize,
    now_ms: u64,
    pace_ms: u64,
    nodes: Vec<LabNodeState>,
    links: Vec<LabLink>,
    events: Vec<LabEvent>,
}

#[derive(Deserialize)]
struct AddNodeRequest {
    name: Option<String>,
    attach_to: Option<u64>,
}

#[derive(Deserialize)]
struct AddLinkRequest {
    a: u64,
    b: u64,
    #[serde(default = "default_link_cost")]
    cost: u16,
}

const fn default_link_cost() -> u16 {
    10
}

#[derive(Clone, Deserialize, Serialize)]
struct ConditionsRequest {
    latency_ms: u64,
    loss_percent: f64,
    bandwidth_kbps: u64,
}

#[derive(Deserialize)]
struct PaceRequest {
    pace_ms: u64,
}

#[derive(Deserialize)]
struct SendRequest {
    source: u64,
    destination: u64,
    priority: u8,
    payload: Option<String>,
}

struct LinkProxyHandle {
    conditions: watch::Sender<ConditionsRequest>,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<()>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let port = parse_arg("--port", "8080")?.parse::<u16>()?;
    let initial = parse_arg("--initial", "5")?.parse::<usize>()?;
    let pace_ms = parse_arg("--tick-ms", "220")?.parse::<u64>()?;
    let node_path = env::current_exe()?.with_file_name("mblab-node");
    if !node_path.exists() {
        return Err(format!(
            "{} is missing; run `cargo build --workspace` first",
            node_path.display()
        )
        .into());
    }
    let run_id = SystemTime::now().duration_since(UNIX_EPOCH)?.as_nanos();
    let state_dir = env::temp_dir().join(format!("mblab-{}-{run_id}", process::id()));
    std::fs::create_dir_all(&state_dir)?;
    let client = Client::builder().timeout(Duration::from_secs(2)).build()?;
    let lab = Arc::new(Lab {
        started_at: std::time::Instant::now(),
        node_path,
        state_dir,
        client,
        nodes: Mutex::new(BTreeMap::new()),
        links: Mutex::new(BTreeMap::new()),
        proxies: Mutex::new(BTreeMap::new()),
        next_node: AtomicU64::new(1),
        next_link: AtomicU64::new(1),
        next_flow: AtomicU64::new(1),
        pace_ms: AtomicU64::new(pace_ms),
    });

    let mut ids = Vec::new();
    for name in ["drone-17", "vehicle-3", "ground-2", "relay-west", "c2-main"]
        .into_iter()
        .take(initial.min(5))
    {
        ids.push(
            spawn_new_node(&lab, Some(name.to_string()))
                .await
                .map_err(startup_error)?,
        );
    }
    for index in 5..initial {
        ids.push(
            spawn_new_node(&lab, Some(format!("node-{}", index + 1)))
                .await
                .map_err(startup_error)?,
        );
    }
    let default_edges = [(0, 1), (1, 2), (0, 3), (3, 2), (2, 4), (3, 4)];
    for (left, right) in default_edges {
        if let (Some(a), Some(b)) = (ids.get(left), ids.get(right)) {
            connect_nodes(&lab, *a, *b, default_link_cost())
                .await
                .map_err(startup_error)?;
        }
    }
    if ids.len() > 5 {
        for pair in ids[4..].windows(2) {
            connect_nodes(&lab, pair[0], pair[1], default_link_cost())
                .await
                .map_err(startup_error)?;
        }
    }

    let app = Router::new()
        .route("/", get(index))
        .route("/api/state", get(api_state))
        .route("/api/nodes", post(add_node))
        .route("/api/nodes/:id", delete(remove_node))
        .route("/api/nodes/:id/crash", post(crash_node))
        .route("/api/nodes/:id/restart", post(restart_node))
        .route("/api/links", post(add_link))
        .route("/api/links/:id/toggle", post(toggle_link))
        .route("/api/links/:id/conditions", patch(set_conditions))
        .route("/api/settings/pace", patch(set_pace))
        .route("/api/packets", post(send_packet))
        .with_state(Arc::clone(&lab));

    let address = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
    let listener = tokio::net::TcpListener::bind(address).await?;
    println!("Mesh Lab running at http://{}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    shutdown_lab(&lab).await;
    Ok(())
}

async fn index() -> Html<&'static str> {
    Html(include_str!("../static/index.html"))
}

async fn api_state(State(lab): State<Arc<Lab>>) -> Json<LabStateResponse> {
    Json(collect_state(&lab).await)
}

async fn add_node(
    State(lab): State<Arc<Lab>>,
    Json(request): Json<AddNodeRequest>,
) -> Result<Json<NodeInfo>, ApiError> {
    let attach_to = if let Some(attach_to) = request.attach_to {
        require_running_node(&lab, attach_to).await?;
        Some(attach_to)
    } else {
        lab.nodes
            .lock()
            .await
            .values()
            .find(|node| node.child.is_some() && node.info.pid.is_some())
            .map(|node| node.info.id)
    };
    let id = spawn_new_node(&lab, request.name).await?;
    if let Some(peer) = attach_to {
        if let Err(error) = connect_nodes(&lab, peer, id, default_link_cost()).await {
            discard_node(&lab, id).await;
            return Err(error);
        }
    }
    let info = lab.nodes.lock().await[&id].info.clone();
    Ok(Json(info))
}

async fn discard_node(lab: &Arc<Lab>, id: u64) {
    if let Some(mut node) = lab.nodes.lock().await.remove(&id) {
        if let Some(mut child) = node.child.take() {
            let _ = child.kill().await;
        }
    }
    lab.links
        .lock()
        .await
        .retain(|_, link| link.a != id && link.b != id);
}

async fn remove_node(
    State(lab): State<Arc<Lab>>,
    Path(id): Path<u64>,
) -> Result<StatusCode, ApiError> {
    let incident = incident_link_ids(&lab, id, false).await;
    stop_proxies(&lab, &incident).await;
    let node = lab.nodes.lock().await.remove(&id);
    let Some(mut node) = node else {
        return Err(not_found("node"));
    };
    if let Some(mut child) = node.child.take() {
        let _ = child.kill().await;
    }
    lab.links
        .lock()
        .await
        .retain(|_, link| link.a != id && link.b != id);
    Ok(StatusCode::NO_CONTENT)
}

async fn crash_node(
    State(lab): State<Arc<Lab>>,
    Path(id): Path<u64>,
) -> Result<StatusCode, ApiError> {
    let child = {
        let mut nodes = lab.nodes.lock().await;
        let node = nodes.get_mut(&id).ok_or_else(|| not_found("node"))?;
        node.info.pid = None;
        node.child.take()
    };
    if let Some(mut child) = child {
        child.kill().await.map_err(internal_error)?;
    }
    let incident = incident_link_ids(&lab, id, false).await;
    stop_proxies(&lab, &incident).await;
    Ok(StatusCode::NO_CONTENT)
}

async fn restart_node(
    State(lab): State<Arc<Lab>>,
    Path(id): Path<u64>,
) -> Result<StatusCode, ApiError> {
    restart_managed_node(&lab, id).await?;
    let incident = incident_link_ids(&lab, id, true).await;
    for link_id in incident {
        match connect_existing_link(&lab, link_id).await {
            Ok(()) => {}
            Err((StatusCode::SERVICE_UNAVAILABLE, _)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn add_link(
    State(lab): State<Arc<Lab>>,
    Json(request): Json<AddLinkRequest>,
) -> Result<Json<LabLink>, ApiError> {
    let id = connect_nodes(&lab, request.a, request.b, request.cost).await?;
    Ok(Json(lab.links.lock().await[&id].clone()))
}

async fn toggle_link(
    State(lab): State<Arc<Lab>>,
    Path(id): Path<u64>,
) -> Result<Json<LabLink>, ApiError> {
    let link = lab
        .links
        .lock()
        .await
        .get(&id)
        .cloned()
        .ok_or_else(|| not_found("link"))?;
    if link.desired_up {
        {
            let mut links = lab.links.lock().await;
            let current = links.get_mut(&id).ok_or_else(|| not_found("link"))?;
            current.desired_up = false;
            current.up = false;
        }
        stop_proxies(&lab, &[id]).await;
    } else {
        connect_existing_link(&lab, id).await?;
        {
            let mut links = lab.links.lock().await;
            let current = links.get_mut(&id).ok_or_else(|| not_found("link"))?;
            current.desired_up = true;
            current.up = true;
        }
    }
    Ok(Json(lab.links.lock().await[&id].clone()))
}

async fn set_conditions(
    State(lab): State<Arc<Lab>>,
    Path(id): Path<u64>,
    Json(request): Json<ConditionsRequest>,
) -> Result<Json<LabLink>, ApiError> {
    if !(0.0..=100.0).contains(&request.loss_percent) {
        return Err(bad_request("loss_percent must be between 0 and 100"));
    }
    let updated = {
        let mut links = lab.links.lock().await;
        let current = links.get_mut(&id).ok_or_else(|| not_found("link"))?;
        current.latency_ms = request.latency_ms;
        current.loss_percent = request.loss_percent;
        current.bandwidth_kbps = request.bandwidth_kbps;
        current.clone()
    };
    update_proxy_conditions(&lab, id, &updated).await;
    Ok(Json(updated))
}

async fn set_pace(
    State(lab): State<Arc<Lab>>,
    Json(request): Json<PaceRequest>,
) -> Result<StatusCode, ApiError> {
    if request.pace_ms > 2_000 {
        return Err(bad_request("pace_ms must be between 0 and 2000"));
    }
    lab.pace_ms.store(request.pace_ms, Ordering::Relaxed);
    let links = lab
        .links
        .lock()
        .await
        .values()
        .filter(|link| link.desired_up)
        .cloned()
        .collect::<Vec<_>>();
    for link in links {
        update_proxy_conditions(&lab, link.id, &link).await;
    }
    Ok(StatusCode::NO_CONTENT)
}

async fn send_packet(
    State(lab): State<Arc<Lab>>,
    Json(request): Json<SendRequest>,
) -> Result<StatusCode, ApiError> {
    let admin = node_admin(&lab, request.source).await?;
    let response = lab
        .client
        .post(format!("http://{admin}/send"))
        .json(&serde_json::json!({
            "destination": request.destination,
            "priority": request.priority,
            "flow_id": lab.next_flow.fetch_add(1, Ordering::Relaxed),
            "payload": request.payload.unwrap_or_else(|| "mesh-lab probe".into())
        }))
        .send()
        .await
        .map_err(internal_error)?;
    ensure_success(response).await?;
    Ok(StatusCode::ACCEPTED)
}

async fn spawn_new_node(lab: &Arc<Lab>, name: Option<String>) -> Result<u64, ApiError> {
    let id = lab.next_node.fetch_add(1, Ordering::Relaxed);
    let name = name.unwrap_or_else(|| format!("node-{id}"));
    let managed = spawn_process(lab, id, name).await?;
    lab.nodes.lock().await.insert(id, managed);
    Ok(id)
}

async fn restart_managed_node(lab: &Arc<Lab>, id: u64) -> Result<(), ApiError> {
    let (name, child) = {
        let mut nodes = lab.nodes.lock().await;
        let node = nodes.get_mut(&id).ok_or_else(|| not_found("node"))?;
        node.info.pid = None;
        (node.info.name.clone(), node.child.take())
    };
    if let Some(mut child) = child {
        child.kill().await.map_err(internal_error)?;
    }
    let managed = spawn_process(lab, id, name).await?;
    lab.nodes.lock().await.insert(id, managed);
    Ok(())
}

async fn spawn_process(lab: &Arc<Lab>, id: u64, name: String) -> Result<ManagedNode, ApiError> {
    let mesh_port = reserve_port().await.map_err(internal_error)?;
    let admin_port = reserve_port().await.map_err(internal_error)?;
    let seq_path = lab.state_dir.join(format!("node-{id}.seq"));
    let mut command = Command::new(&lab.node_path);
    command
        .args([
            "--id",
            &id.to_string(),
            "--name",
            &name,
            "--mesh-port",
            &mesh_port.to_string(),
            "--admin-port",
            &admin_port.to_string(),
            "--seq-path",
            &seq_path.to_string_lossy(),
        ])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let started_offset_ms = u64::try_from(lab.started_at.elapsed().as_millis()).unwrap_or(u64::MAX);
    let child = command.spawn().map_err(internal_error)?;
    let pid = child.id();
    let info = NodeInfo {
        id,
        name,
        mesh_addr: format!("127.0.0.1:{mesh_port}"),
        admin_addr: format!("127.0.0.1:{admin_port}"),
        pid,
    };
    wait_for_health(&lab.client, &info.admin_addr).await?;
    Ok(ManagedNode {
        info,
        child: Some(child),
        started_offset_ms,
    })
}

async fn connect_nodes(lab: &Arc<Lab>, a: u64, b: u64, cost: u16) -> Result<u64, ApiError> {
    if a == b {
        return Err(bad_request("a link requires two different nodes"));
    }
    if cost == 0 {
        return Err(bad_request("link cost must be greater than zero"));
    }
    if lab
        .links
        .lock()
        .await
        .values()
        .any(|link| (link.a == a && link.b == b) || (link.a == b && link.b == a))
    {
        return Err(bad_request("a link between these nodes already exists"));
    }
    let id = lab.next_link.fetch_add(1, Ordering::Relaxed);
    let link = LabLink {
        id,
        a,
        b,
        up: false,
        desired_up: true,
        cost,
        latency_ms: 0,
        loss_percent: 0.0,
        bandwidth_kbps: 0,
    };
    lab.links.lock().await.insert(id, link);
    if let Err(error) = connect_existing_link(lab, id).await {
        lab.links.lock().await.remove(&id);
        stop_proxies(lab, &[id]).await;
        return Err(error);
    }
    if let Some(link) = lab.links.lock().await.get_mut(&id) {
        link.up = true;
    }
    Ok(id)
}

async fn connect_existing_link(lab: &Arc<Lab>, id: u64) -> Result<(), ApiError> {
    let link = lab
        .links
        .lock()
        .await
        .get(&id)
        .cloned()
        .ok_or_else(|| not_found("link"))?;
    let a_info = require_running_node(lab, link.a).await?;
    let b_info = require_running_node(lab, link.b).await?;
    stop_proxies(lab, &[id]).await;
    let target = b_info
        .mesh_addr
        .parse::<SocketAddr>()
        .map_err(internal_error)?;
    let (proxy_addr, proxy) = start_link_proxy(target, effective_conditions(lab, &link), id)
        .await
        .map_err(internal_error)?;
    allow_peer(&lab.client, &a_info.admin_addr, link.b, link.cost).await?;
    allow_peer(&lab.client, &b_info.admin_addr, link.a, link.cost).await?;
    let response = lab
        .client
        .post(format!("http://{}/connect", a_info.admin_addr))
        .json(&serde_json::json!({
            "peer": link.b,
            "address": proxy_addr.to_string(),
            "cost": link.cost
        }))
        .send()
        .await
        .map_err(internal_error);
    match response {
        Ok(response) => {
            if let Err(error) = ensure_success(response).await {
                stop_proxy(proxy).await;
                return Err(error);
            }
        }
        Err(error) => {
            stop_proxy(proxy).await;
            return Err(error);
        }
    }
    lab.proxies.lock().await.insert(id, proxy);
    Ok(())
}

async fn require_running_node(lab: &Arc<Lab>, id: u64) -> Result<NodeInfo, ApiError> {
    let (info, managed_running) = lab
        .nodes
        .lock()
        .await
        .get(&id)
        .map(|node| {
            (
                node.info.clone(),
                node.child.is_some() && node.info.pid.is_some(),
            )
        })
        .ok_or_else(|| not_found("node"))?;
    if managed_running {
        if let Ok(response) = lab
            .client
            .get(format!("http://{}/health", info.admin_addr))
            .send()
            .await
        {
            if response.status().is_success() {
                return Ok(info);
            }
        }
    }
    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        format!(
            "node {} ({}) process is stopped; restart it before creating a link",
            info.id, info.name
        ),
    ))
}

fn effective_conditions(lab: &Lab, link: &LabLink) -> ConditionsRequest {
    ConditionsRequest {
        latency_ms: link
            .latency_ms
            .saturating_add(lab.pace_ms.load(Ordering::Relaxed)),
        loss_percent: link.loss_percent,
        bandwidth_kbps: link.bandwidth_kbps,
    }
}

async fn update_proxy_conditions(lab: &Arc<Lab>, id: u64, link: &LabLink) {
    if let Some(proxy) = lab.proxies.lock().await.get(&id) {
        proxy
            .conditions
            .send_replace(effective_conditions(lab, link));
    }
}

async fn stop_proxy(proxy: LinkProxyHandle) {
    proxy.shutdown.send_replace(true);
    let _ = proxy.task.await;
}

async fn stop_proxies(lab: &Arc<Lab>, ids: &[u64]) {
    let proxies = {
        let mut active = lab.proxies.lock().await;
        ids.iter()
            .filter_map(|id| active.remove(id))
            .collect::<Vec<_>>()
    };
    for proxy in proxies {
        stop_proxy(proxy).await;
    }
}

async fn incident_link_ids(lab: &Arc<Lab>, node: u64, desired_only: bool) -> Vec<u64> {
    lab.links
        .lock()
        .await
        .values()
        .filter(|link| should_restore_link(link, node, desired_only))
        .map(|link| link.id)
        .collect()
}

fn should_restore_link(link: &LabLink, node: u64, desired_only: bool) -> bool {
    (link.a == node || link.b == node) && (!desired_only || link.desired_up)
}

async fn start_link_proxy(
    target: SocketAddr,
    conditions: ConditionsRequest,
    seed: u64,
) -> std::io::Result<(SocketAddr, LinkProxyHandle)> {
    let listener = TcpListener::bind((Ipv4Addr::LOCALHOST, 0)).await?;
    let address = listener.local_addr()?;
    let (conditions_tx, conditions_rx) = watch::channel(conditions);
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let task = tokio::spawn(run_link_proxy(
        listener,
        target,
        conditions_rx,
        shutdown_rx,
        seed,
    ));
    Ok((
        address,
        LinkProxyHandle {
            conditions: conditions_tx,
            shutdown: shutdown_tx,
            task,
        },
    ))
}

async fn run_link_proxy(
    listener: TcpListener,
    target: SocketAddr,
    conditions: watch::Receiver<ConditionsRequest>,
    mut shutdown: watch::Receiver<bool>,
    seed: u64,
) {
    let accepted = tokio::select! {
        accepted = listener.accept() => accepted.map(|(stream, _)| stream),
        _ = shutdown.changed() => return,
    };
    let Ok(downstream) = accepted else {
        return;
    };
    let upstream = tokio::select! {
        connected = TcpStream::connect(target) => connected,
        _ = shutdown.changed() => return,
    };
    let Ok(upstream) = upstream else {
        return;
    };
    let (downstream_read, downstream_write) = downstream.into_split();
    let (upstream_read, upstream_write) = upstream.into_split();
    let mut directions = JoinSet::new();
    directions.spawn(forward_frames(
        downstream_read,
        upstream_write,
        conditions.clone(),
        shutdown.clone(),
        seed.wrapping_mul(2),
    ));
    directions.spawn(forward_frames(
        upstream_read,
        downstream_write,
        conditions,
        shutdown,
        seed.wrapping_mul(2).wrapping_add(1),
    ));
    let _ = directions.join_next().await;
    directions.abort_all();
    while directions.join_next().await.is_some() {}
}

async fn forward_frames<R, W>(
    mut reader: R,
    mut writer: W,
    conditions: watch::Receiver<ConditionsRequest>,
    mut shutdown: watch::Receiver<bool>,
    seed: u64,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let mut buffered = BytesMut::with_capacity(16 * 1024);
    let mut decoder = FrameDecoder;
    let mut sequence = 0_u64;
    loop {
        loop {
            let frame = match decoder.decode(&mut buffered) {
                Ok(Some(frame)) => frame,
                Ok(None) => break,
                Err(_) => return,
            };
            let Ok(encoded) = FrameEncoder::encode(&frame) else {
                return;
            };
            sequence = sequence.wrapping_add(1);
            if frame.frame_type != FrameType::LinkHello {
                let current = conditions.borrow().clone();
                let bits = u64::try_from(encoded.len())
                    .unwrap_or(u64::MAX)
                    .saturating_mul(8);
                let serialization_ms = bits
                    .saturating_add(current.bandwidth_kbps.saturating_sub(1))
                    .checked_div(current.bandwidth_kbps)
                    .unwrap_or(0);
                let delay_ms = current.latency_ms.saturating_add(serialization_ms);
                if delay_ms > 0 {
                    tokio::select! {
                        _ = sleep(Duration::from_millis(delay_ms)) => {}
                        _ = shutdown.changed() => return,
                    }
                }
                let sample = mix64(seed ^ sequence) % 1_000_000;
                let loss_per_million = (current.loss_percent * 10_000.0).round() as u64;
                if sample < loss_per_million.min(1_000_000) {
                    continue;
                }
            }
            let written = tokio::select! {
                written = writer.write_all(&encoded) => written,
                _ = shutdown.changed() => return,
            };
            if written.is_err() {
                return;
            }
        }
        let read = tokio::select! {
            read = reader.read_buf(&mut buffered) => read,
            _ = shutdown.changed() => return,
        };
        match read {
            Ok(0) | Err(_) => return,
            Ok(_) => {}
        }
    }
}

fn mix64(mut value: u64) -> u64 {
    value ^= value >> 30;
    value = value.wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value ^= value >> 27;
    value = value.wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

async fn collect_state(lab: &Arc<Lab>) -> LabStateResponse {
    let infos = lab
        .nodes
        .lock()
        .await
        .values()
        .map(|node| (node.info.clone(), node.started_offset_ms))
        .collect::<Vec<_>>();
    let mut requests = JoinSet::new();
    for (info, started_offset_ms) in infos {
        let client = lab.client.clone();
        requests.spawn(async move {
            let snapshot = match client
                .get(format!("http://{}/snapshot", info.admin_addr))
                .send()
                .await
            {
                Ok(response) if response.status().is_success() => {
                    response.json::<NodeSnapshot>().await.ok()
                }
                _ => None,
            };
            (info, started_offset_ms, snapshot)
        });
    }
    let mut collected = Vec::new();
    while let Some(result) = requests.join_next().await {
        if let Ok(result) = result {
            collected.push(result);
        }
    }
    collected.sort_by_key(|(info, _, _)| info.id);
    let mut nodes = Vec::new();
    let mut events = Vec::new();
    for (info, started_offset_ms, snapshot) in collected {
        if let Some(snapshot) = &snapshot {
            events.extend(snapshot.events.iter().cloned().map(|event| LabEvent {
                node: info.id,
                name: info.name.clone(),
                sequence: event.sequence,
                at_ms: started_offset_ms.saturating_add(event.at_ms),
                kind: event.kind,
                detail: event.detail,
                link_id: event.link_id,
                peer: event.peer,
            }));
        }
        nodes.push(LabNodeState {
            info,
            running: snapshot.is_some(),
            snapshot,
        });
    }
    events.sort_by_key(|event| (event.at_ms, event.node, event.sequence));
    events.reverse();
    events.truncate(100);
    let live = nodes.iter().filter(|node| node.running).count();
    let live_ids = nodes
        .iter()
        .filter(|node| node.running)
        .map(|node| node.info.id)
        .collect::<BTreeSet<_>>();
    let mut links = lab.links.lock().await.values().cloned().collect::<Vec<_>>();
    for link in &mut links {
        let endpoint_sees_peer = |endpoint: u64, peer: u64| {
            nodes
                .iter()
                .find(|node| node.info.id == endpoint)
                .and_then(|node| node.snapshot.as_ref())
                .is_some_and(|snapshot| snapshot.peers.iter().any(|seen| seen.peer == peer))
        };
        link.up = endpoint_sees_peer(link.a, link.b) && endpoint_sees_peer(link.b, link.a);
    }
    let expected_adjacencies = live_ids
        .iter()
        .map(|id| {
            let peers = links
                .iter()
                .filter(|link| link.up && (link.a == *id || link.b == *id))
                .map(|link| if link.a == *id { link.b } else { link.a })
                .filter(|peer| live_ids.contains(peer))
                .collect::<BTreeSet<_>>();
            (*id, peers)
        })
        .collect::<BTreeMap<_, _>>();
    let mut unseen = live_ids.clone();
    let mut components = Vec::new();
    while let Some(start) = unseen.first().copied() {
        let mut component = BTreeSet::new();
        let mut pending = vec![start];
        while let Some(id) = pending.pop() {
            if !component.insert(id) {
                continue;
            }
            unseen.remove(&id);
            if let Some(peers) = expected_adjacencies.get(&id) {
                pending.extend(peers.iter().filter(|peer| !component.contains(peer)));
            }
        }
        components.push(component);
    }
    let lsdb_view = |snapshot: &NodeSnapshot, origins: &BTreeSet<u64>| {
        snapshot
            .lsdb
            .iter()
            .filter(|lsa| origins.contains(&lsa.origin))
            .map(|lsa| {
                let adjacencies = lsa
                    .adjacencies
                    .iter()
                    .copied()
                    .filter(|peer| live_ids.contains(peer))
                    .collect::<BTreeSet<_>>();
                (lsa.origin, (lsa.epoch, lsa.sequence, adjacencies))
            })
            .collect::<BTreeMap<_, _>>()
    };
    let converged = live > 0
        && components.iter().all(|component| {
            let canonical_lsdb = nodes.iter().find_map(|node| {
                component
                    .contains(&node.info.id)
                    .then(|| {
                        node.snapshot
                            .as_ref()
                            .map(|snapshot| lsdb_view(snapshot, component))
                    })
                    .flatten()
            });
            canonical_lsdb.is_some()
                && component.iter().all(|id| {
                    let Some(node) = nodes.iter().find(|node| node.info.id == *id) else {
                        return false;
                    };
                    let Some(snapshot) = node.snapshot.as_ref() else {
                        return false;
                    };
                    let expected_destinations = component
                        .iter()
                        .copied()
                        .filter(|destination| destination != id)
                        .collect::<BTreeSet<_>>();
                    let route_destinations = snapshot
                        .routes
                        .iter()
                        .map(|route| route.destination)
                        .collect::<BTreeSet<_>>();
                    let node_lsdb = lsdb_view(snapshot, component);
                    node_lsdb.len() == component.len()
                        && Some(&node_lsdb) == canonical_lsdb.as_ref()
                        && node_lsdb.iter().all(|(origin, (_, _, adjacencies))| {
                            expected_adjacencies.get(origin) == Some(adjacencies)
                        })
                        && route_destinations == expected_destinations
                })
        });
    LabStateResponse {
        converged,
        components: components.len(),
        now_ms: u64::try_from(lab.started_at.elapsed().as_millis()).unwrap_or(u64::MAX),
        pace_ms: lab.pace_ms.load(Ordering::Relaxed),
        nodes,
        links,
        events,
    }
}

async fn allow_peer(client: &Client, admin: &str, peer: u64, cost: u16) -> Result<(), ApiError> {
    let response = client
        .post(format!("http://{admin}/peers/allow"))
        .json(&serde_json::json!({ "peer": peer, "cost": cost }))
        .send()
        .await
        .map_err(internal_error)?;
    ensure_success(response).await?;
    Ok(())
}

async fn node_admin(lab: &Arc<Lab>, id: u64) -> Result<String, ApiError> {
    lab.nodes
        .lock()
        .await
        .get(&id)
        .map(|node| node.info.admin_addr.clone())
        .ok_or_else(|| not_found("node"))
}

async fn ensure_success(response: reqwest::Response) -> Result<reqwest::Response, ApiError> {
    if response.status().is_success() {
        Ok(response)
    } else {
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        Err((status, body))
    }
}

async fn wait_for_health(client: &Client, admin: &str) -> Result<(), ApiError> {
    for _ in 0..60 {
        if let Ok(response) = client.get(format!("http://{admin}/health")).send().await {
            if response.status().is_success() {
                return Ok(());
            }
        }
        sleep(Duration::from_millis(50)).await;
    }
    Err((
        StatusCode::GATEWAY_TIMEOUT,
        format!("mblab-node at {admin} did not become ready"),
    ))
}

async fn reserve_port() -> std::io::Result<u16> {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    listener.local_addr().map(|address| address.port())
}

fn parse_arg(flag: &str, default: &str) -> Result<String, Box<dyn std::error::Error>> {
    let mut args = env::args();
    while let Some(argument) = args.next() {
        if argument == flag {
            return args
                .next()
                .ok_or_else(|| format!("missing value for {flag}").into());
        }
    }
    Ok(default.to_string())
}

fn bad_request(message: impl Into<String>) -> ApiError {
    (StatusCode::BAD_REQUEST, message.into())
}

fn not_found(kind: &str) -> ApiError {
    (StatusCode::NOT_FOUND, format!("{kind} was not found"))
}

fn internal_error(error: impl std::fmt::Display) -> ApiError {
    (StatusCode::INTERNAL_SERVER_ERROR, error.to_string())
}

fn startup_error(error: ApiError) -> std::io::Error {
    std::io::Error::other(error.1)
}

async fn shutdown_signal() {
    let _ = tokio::signal::ctrl_c().await;
}

async fn shutdown_lab(lab: &Arc<Lab>) {
    let proxy_ids = lab.proxies.lock().await.keys().copied().collect::<Vec<_>>();
    stop_proxies(lab, &proxy_ids).await;
    let mut nodes = lab.nodes.lock().await;
    for node in nodes.values_mut() {
        if let Some(mut child) = node.child.take() {
            let _ = child.kill().await;
        }
    }
    drop(nodes);
    let _ = std::fs::remove_dir_all(&lab.state_dir);
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use mb_wire::{Channel, WireFrame};
    use tokio::time::timeout;

    fn link(desired_up: bool) -> LabLink {
        LabLink {
            id: 7,
            a: 1,
            b: 2,
            up: desired_up,
            desired_up,
            cost: 10,
            latency_ms: 0,
            loss_percent: 0.0,
            bandwidth_kbps: 0,
        }
    }

    #[test]
    fn restart_restores_only_desired_incident_links() {
        assert!(should_restore_link(&link(true), 1, true));
        assert!(!should_restore_link(&link(false), 1, true));
        assert!(!should_restore_link(&link(true), 3, true));
    }

    #[tokio::test]
    async fn proxy_passes_hello_but_drops_impaired_frames() {
        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("upstream must bind");
        let (proxy_addr, proxy) = start_link_proxy(
            upstream.local_addr().expect("upstream address"),
            ConditionsRequest {
                latency_ms: 0,
                loss_percent: 100.0,
                bandwidth_kbps: 0,
            },
            9,
        )
        .await
        .expect("proxy must start");
        let mut client = TcpStream::connect(proxy_addr)
            .await
            .expect("client must connect");
        let hello = WireFrame::control(FrameType::LinkHello, Bytes::from_static(b"hello"));
        let data = WireFrame {
            frame_type: FrameType::Lsa,
            channel: Channel::Control,
            payload: Bytes::from_static(b"dropped"),
        };
        client
            .write_all(&FrameEncoder::encode(&hello).expect("hello must encode"))
            .await
            .expect("hello must write");
        client
            .write_all(&FrameEncoder::encode(&data).expect("data must encode"))
            .await
            .expect("data must write");
        let (mut server, _) = upstream
            .accept()
            .await
            .expect("proxy must connect upstream");
        let mut buffered = BytesMut::new();
        let mut decoder = FrameDecoder;
        let received = timeout(Duration::from_secs(1), async {
            loop {
                if let Some(frame) = decoder.decode(&mut buffered).expect("valid frame") {
                    break frame;
                }
                server
                    .read_buf(&mut buffered)
                    .await
                    .expect("frame must read");
            }
        })
        .await
        .expect("hello must arrive");
        assert_eq!(received, hello);
        assert!(
            timeout(Duration::from_millis(100), server.read_buf(&mut buffered))
                .await
                .is_err()
        );
        stop_proxy(proxy).await;
    }

    #[tokio::test]
    async fn proxy_shutdown_interrupts_long_link_delay() {
        let upstream = TcpListener::bind((Ipv4Addr::LOCALHOST, 0))
            .await
            .expect("upstream must bind");
        let (proxy_addr, proxy) = start_link_proxy(
            upstream.local_addr().expect("upstream address"),
            ConditionsRequest {
                latency_ms: 60_000,
                loss_percent: 0.0,
                bandwidth_kbps: 0,
            },
            10,
        )
        .await
        .expect("proxy must start");
        let mut client = TcpStream::connect(proxy_addr)
            .await
            .expect("client must connect");
        let (_server, _) = upstream
            .accept()
            .await
            .expect("proxy must connect upstream");
        let data = WireFrame {
            frame_type: FrameType::Lsa,
            channel: Channel::Control,
            payload: Bytes::from_static(b"delayed"),
        };
        client
            .write_all(&FrameEncoder::encode(&data).expect("data must encode"))
            .await
            .expect("data must write");
        sleep(Duration::from_millis(20)).await;
        timeout(Duration::from_secs(1), stop_proxy(proxy))
            .await
            .expect("shutdown must interrupt the delay");
    }
}
