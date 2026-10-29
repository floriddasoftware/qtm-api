use axum::{
    extract::Path,
    http::{Method, StatusCode},
    routing::{get, post, put},
    Json, Router,
};

use qp_hd::{
    commands,
    economic_gate::QuantumTransaction,
    pathsregistry::{CoinType, PathsRegistry},
    purpose::{BIP39Handler, BIP39Language},
    qp44::{Flow, TotalMass},
};
use qtm_graph::graph::{
    frame::{Depth, Direction},
    Scale,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::CorsLayer;
use std::{
    collections::HashMap,
    path::{Path as FsPath, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};
const DOMAIN_SIZE: u16 = 2048;
const NUM_INDICES: usize = 12;
const MAX_SUBSTRATE_NAME_LENGTH: usize = 64;
const SUBSTRATE_FILES: [&str; 8] = [
    "perm.indices",
    "perm.entropy",
    "perm.dimension",
    "activation",
    "qtm.commitment",
    "qtm.coordinate",
    "qtm.sigma",
    "qtm.network",
];

type ApiResult<T> = Result<T, (StatusCode, Json<ApiError>)>;

#[derive(Serialize)]
struct ApiError {
    error: String,
}

#[derive(Deserialize)]
struct CreateRequest {
    name: String,
    #[serde(default)]
    indices: Option<String>,
    #[serde(default)]
    entropy: Option<String>,
    #[serde(default)]
    seed_source: Option<String>,
    #[serde(default)]
    phrase: Option<String>,
    #[serde(default)]
    language: Option<String>,
    #[serde(default)]
    passphrase: Option<String>,
}

#[derive(Serialize)]
struct QtmResponse {
    commitment: String,
    coordinate: String,
    sigma: String,
    network: String,
}

#[derive(Serialize)]
struct QtmFrameResponse {
    identifier: String,
    qtm: QtmResponse,
}

#[derive(Serialize)]
struct DirectionResponse {
    frame: DirectionFrameResponse,
    #[serde(skip_serializing_if = "Option::is_none")]
    selection: Option<TransitSelection>,
}

#[derive(Serialize)]
struct DirectionFrameResponse {
    qtm: QtmFrameResponse,
    event: EventMapResponse,
}

#[derive(Serialize)]
struct EventMapResponse {
    scale: String,
    runtime: RuntimeResponse,
    depth: DepthResponse,
}

#[derive(Serialize, Clone)]
struct DepthResponse {
    path: String,
    objects: Vec<DepthObjectResponse>,
}

#[derive(Serialize, Clone)]
struct DepthObjectResponse {
    position: usize,
    identifier: String,
    mass: String,
    from: String,
}

#[derive(Serialize, Clone)]
struct RuntimeResponse {
    unix_seconds: u64,
    subsec_nanos: u32,
}

#[derive(Serialize)]
struct ReceiveResponse {
    frame: QuantumTransactionFrame,
}

#[derive(Serialize)]
struct QuantumTransactionFrame {
    quantum_transaction: QuantumTransactionResponse,
    event: EventMapResponse,
}

#[derive(Serialize)]
struct QuantumTransactionResponse {
    amount: u64,
    qtm: QtmFrameResponse,
    constant: String,
}

#[derive(Serialize)]
struct SubstrateResponse {
    namespace: String,
    indices: String,
    entropy: String,
    dimension: String,
    commitment: String,
    coordinate: String,
    sigma: String,
    network: String,
}

#[derive(Serialize)]
struct EventStoreResponse {
    substrate: String,
    directory: String,
    events_directory: String,
    events: Vec<PersistedEventSummary>,
    runtime: RuntimeResponse,
}

#[derive(Serialize)]
struct PersistedEventSummary {
    kind: String,
    record: String,
    activation: String,
    dimension: String,
    structural_value: String,
    depth_objects: usize,
    retained_mass: String,
    // Persisted transition window; never recomputed by the API.
    delta: String,
    tau: String,
    gross_work: String,
    net_work: String,
    commitment: String,
    coordinate: String,
}

#[derive(Serialize)]
struct PersistedPayloadNetwork {
    id: String,
    name: String,
    registered: bool,
}

#[derive(Serialize)]
struct PersistedPayloadEntry {
    record: String,
    direction: String,
    network: PersistedPayloadNetwork,
    path: String,
    depth_identifier: String,
    objects: Vec<DepthObjectResponse>,
}

#[derive(Serialize)]
struct PersistedPayloadsResponse {
    substrate: String,
    record: String,
    directory: String,
    payloads: Vec<PersistedPayloadEntry>,
}

#[derive(Serialize)]
struct SubstrateListResponse {
    substrates: Vec<SubstrateEntry>,
}

#[derive(Serialize)]
struct SubstrateEntry {
    name: String,
    dimension: String,
    activations: String,
}

#[derive(Serialize)]
struct NetworkListResponse {
    networks: Vec<NetworkOption>,
}

#[derive(Serialize, Clone)]
struct NetworkOption {
    id: String,
    name: String,
    coin: u64,
    purpose: u64,
    path: String,
}

#[derive(Deserialize)]
struct TransitRequest {
    #[serde(default)]
    network: Option<String>,
    #[serde(default)]
    purpose: Option<u64>,
    #[serde(default)]
    coin: Option<u64>,
    #[serde(default)]
    account: Option<u64>,
    #[serde(default)]
    change: Option<u64>,
    #[serde(default)]
    external: Option<u64>,
    #[serde(default)]
    path: Option<String>,
    #[serde(default)]
    activation: u64,
}

#[derive(Serialize)]
struct TransitSelection {
    network: Option<String>,
    path: Option<String>,
    activation: u64,
    purpose: u64,
    coin: u64,
    account: u64,
    change: u64,
    external: u64,
}

#[derive(Deserialize)]
struct ReceiveRequest {
    amount: u64,
    flow: String,
}

/*
 * Activation/network budget API
 *
 * Canonical path:
 *   <substrate_root>/<substrate>/event/<activation>/<network>/budget
 *
 * Each file belongs to exactly one substrate, activation, and registered
 * network. Budget files use a simple key=value format to avoid requiring
 * an additional JSON persistence format.
 */
#[derive(Deserialize)]
struct BudgetRequest {
    min_events: u64,
    max_events: u64,
    max_retained_mass: u128,
}

#[derive(Serialize, Clone)]
struct EventBudget {
    min_events: u64,
    max_events: u64,
    max_retained_mass: String,
}

#[derive(Serialize)]
struct NetworkBudgetEntry {
    network: NetworkOption,
    activation: u64,
    configured: bool,
    budget: Option<EventBudget>,
}

#[derive(Serialize)]
struct ActivationBudgetsResponse {
    substrate: String,
    activation: u64,
    directory: String,
    budgets: Vec<NetworkBudgetEntry>,
}

#[derive(Deserialize, Default)]
struct ObservationStartRequest {
    #[serde(default)]
    listen_port: Option<u16>,
    #[serde(default)]
    path: Option<String>,
}

#[derive(Serialize, Clone)]
struct ObservationClipStatus {
    substrate: String,
    active: bool,
    direction: Option<String>,
}

#[derive(Serialize, Clone)]
struct ObservationPipelineStatus {
    substrate: String,
    activation: u64,
    network: String,
    commitment: String,
    listen_port: u16,
    running: bool,
    path: Option<String>,
    clips: Vec<ObservationClipStatus>,
    runtime: RuntimeResponse,
    message: String,
}

#[derive(Serialize)]
struct ObservationListResponse {
    substrate: String,
    activation: u64,
    pipelines: Vec<ObservationPipelineStatus>,
}

struct LiveObservation {
    status: ObservationPipelineStatus,
}

#[derive(Serialize)]
struct WirePacketsResponse {
    substrate: String,
    activation: u64,
    network: String,
    commitment: String,
    path: String,
    exists: bool,
    packets: Vec<WirePacketEntry>,
}

#[derive(Serialize)]
struct WirePacketEntry {
    name: String,
    path: String,
    bytes: u64,
}

/// Canonical persisted wire-packet file:
/// ~/.qp/substrates/<substrate>/activations/<activation>/event/<commitment>/wirepackets
fn wirepackets_directory(
    substrate_path: &FsPath,
    activation: u64,
    commitment: &str,
) -> ApiResult<PathBuf> {
    let commitment_bytes = hex::decode(commitment.trim()).map_err(|_| {
        error(
            StatusCode::BAD_REQUEST,
            "invalid commitment for wire-packet path",
        )
    })?;

    let commitment_bytes: [u8; 32] =
        commitment_bytes.try_into().map_err(|_| {
            error(
                StatusCode::BAD_REQUEST,
                "commitment must decode to exactly 32 bytes",
            )
        })?;

    Ok(substrate_path
        .join("activations")
        .join(activation.to_string())
        .join("event")
        .join(hex::encode(commitment_bytes))
        .join("wirepackets"))
}

async fn get_wirepackets(
    Path((name, activation, network)): Path<(String, u64, String)>,
) -> ApiResult<Json<WirePacketsResponse>> {
    if !valid_observation_network_id(&network) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid network identifier",
        ));
    }

    let lock = substrate_lock(&name);
    let _guard = lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let substrate_path = ensure_existing_substrate(&name)?;
    let commitment = read_substrate_commitment(&substrate_path)?;
    let commitment = commitment.trim().to_owned();

    let path = wirepackets_directory(
        &substrate_path,
        activation,
        &commitment,
    )?;

    // `persist_wirepackets()` writes one file, not a directory.
    let exists = path.is_file();
    let mut packets = Vec::new();

    if exists {
        let metadata = std::fs::metadata(&path).map_err(|e| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("unable to inspect wire-packets file: {e}"),
            )
        })?;

        packets.push(WirePacketEntry {
            name: "wirepackets".to_owned(),
            path: path.display().to_string(),
            bytes: metadata.len(),
        });
    }

    Ok(Json(WirePacketsResponse {
        substrate: name,
        activation,
        network,
        commitment,
        path: path.display().to_string(),
        exists,
        packets,
    }))
}
type ObservationKey = (String, u64, String);

static OBSERVATION_REGISTRY: OnceLock<Mutex<HashMap<ObservationKey, LiveObservation>>> =
    OnceLock::new();

fn observation_registry() -> &'static Mutex<HashMap<ObservationKey, LiveObservation>> {
    OBSERVATION_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()))
}

fn observation_key(
    substrate: &str,
    activation: u64,
    network: &str,
) -> ObservationKey {
    (
        substrate.to_string(),
        activation,
        network.to_string(),
    )
}

fn read_substrate_commitment(substrate_path: &FsPath) -> ApiResult<String> {
    required_file(&substrate_path.join("qtm.commitment"))
}

fn listen_port_from_commitment(
    commitment: &str,
    preferred: Option<u16>,
) -> Result<u16, String> {
    if let Some(port) = preferred {
        if (1024..=65535).contains(&port) {
            return Ok(port);
        }

        return Err(format!(
            "invalid observation listen port {port}; expected 1024..=65535"
        ));
    }

    let commitment = commitment.trim();

    if commitment.is_empty() {
        return Err("qtm.commitment is empty".to_string());
    }

    let mut acc: u32 = 0;

    for byte in commitment.bytes() {
        acc = acc.wrapping_mul(31).wrapping_add(u32::from(byte));
    }

    Ok(20_000 + (acc % 20_000) as u16)
}

fn is_bitcoin_network_id(network: &str) -> bool {
    let lower = network.trim().to_ascii_lowercase();

    matches!(
        lower.as_str(),
        "bitcoin-legacy"
            | "bitcoin-native-segwit"
            | "bitcoin"
            | "btc"
    ) || lower.contains("bitcoin")
}

fn valid_observation_network_id(network: &str) -> bool {
    !network.is_empty()
        && network.len() <= 64
        && network != "."
        && network != ".."
        && network.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'
        })
}

#[allow(dead_code)]
fn init_bitcoin_observation_clips(
    activation: u64,
    substrate: &str,
) -> Result<Vec<ObservationClipStatus>, String> {
    if substrate.trim().is_empty() {
        return Err("substrate must not be empty".to_string());
    }

    #[cfg(feature = "bit-miner")]
    {
        let pipeline =
            bit_miner::BitcoinObservationPipeline::new(activation, substrate)
                .map_err(|error| {
                    format!(
                        "failed to initialize Bitcoin observation pipeline \
                         for substrate '{substrate}' at activation {activation}: \
                         {error}"
                    )
                })?;

        let clips = pipeline.miner().clips.iter().map(|clip| {
            let window =
                bit_miner::RenderState::window(clip.substrate.clone());

            ObservationClipStatus {
                substrate: window.substrate,
                active: window.active,
                direction: None,
            }
        });

        let mut windows: Vec<ObservationClipStatus> = clips.collect();

        if windows.is_empty() {
            windows.push(ObservationClipStatus {
                substrate: substrate.to_string(),
                active: true,
                direction: None,
            });
        }

        Ok(windows)
    }

    #[cfg(not(feature = "bit-miner"))]
    {
        let _ = activation;

        Ok(vec![ObservationClipStatus {
            substrate: substrate.to_string(),
            active: true,
            direction: None,
        }])
    }
}

static SUBSTRATE_REGISTRY: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> =
    OnceLock::new();

fn substrate_lock(name: &str) -> Arc<Mutex<()>> {
    let registry = SUBSTRATE_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    Arc::clone(
        registry
            .entry(name.to_string())
            .or_insert_with(|| Arc::new(Mutex::new(()))),
    )
}

fn error(status: StatusCode, message: impl Into<String>) -> (StatusCode, Json<ApiError>) {
    (
        status,
        Json(ApiError {
            error: message.into(),
        }),
    )
}

// qp_hd reports failures by panicking (expect/panic!). Convert that into a
// JSON 500 so the client sees the cause instead of a dropped connection.
fn guarded<T>(label: &str, operation: impl FnOnce() -> T) -> ApiResult<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)).map_err(|payload| {
        let detail = payload
            .downcast_ref::<&str>()
            .map(|message| message.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown failure".to_string());
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("{label} failed in qp_hd: {detail}"),
        )
    })
}

fn read_trim(path: &FsPath) -> Option<String> {
    std::fs::read_to_string(path)
        .ok()
        .map(|value| value.trim().to_string())
}

fn required_file(path: &FsPath) -> ApiResult<String> {
    read_trim(path).ok_or_else(|| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unable to read substrate file: {}", path.display()),
        )
    })
}

fn valid_substrate_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_SUBSTRATE_NAME_LENGTH
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
}

fn parse_bip39_language(value: &str) -> Result<BIP39Language, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "english" => Ok(BIP39Language::English),
        "chinese_simplified" | "chinese-simplified" => Ok(BIP39Language::ChineseSimplified),
        "chinese_traditional" | "chinese-traditional" => Ok(BIP39Language::ChineseTraditional),
        "french" => Ok(BIP39Language::French),
        "italian" => Ok(BIP39Language::Italian),
        "japanese" => Ok(BIP39Language::Japanese),
        "korean" => Ok(BIP39Language::Korean),
        "spanish" => Ok(BIP39Language::Spanish),
        "czech" => Ok(BIP39Language::Czech),
        "portuguese" => Ok(BIP39Language::Portuguese),
        _ => Err("unsupported BIP39 language".to_string()),
    }
}

fn parse_indices(raw: &str) -> Result<[u16; NUM_INDICES], String> {
    let parsed = raw
        .split(',')
        .map(|part| {
            let value = part.trim().parse::<u16>().map_err(|_| {
                "indices must contain exactly 12 integers from 0 to 2047".to_string()
            })?;
            if value >= DOMAIN_SIZE {
                return Err("each index must be in the range 0..2048".to_string());
            }
            Ok(value)
        })
        .collect::<Result<Vec<u16>, String>>()?;

    if parsed.len() != NUM_INDICES {
        return Err("indices must contain exactly 12 comma-separated integers".to_string());
    }

    parsed
        .try_into()
        .map_err(|_| "indices must contain exactly 12 comma-separated integers".to_string())
}

fn substrate_json(name: &str) -> ApiResult<SubstrateResponse> {
    let path = commands::substrate_root().join(name);
    Ok(SubstrateResponse {
        namespace: path.display().to_string(),
        indices: required_file(&path.join("perm.indices"))?,
        entropy: required_file(&path.join("perm.entropy"))?,
        dimension: required_file(&path.join("perm.dimension"))?,
        commitment: required_file(&path.join("qtm.commitment"))?,
        coordinate: required_file(&path.join("qtm.coordinate"))?,
        sigma: required_file(&path.join("qtm.sigma"))?,
        network: required_file(&path.join("qtm.network"))?,
    })
}

fn ensure_existing_substrate(name: &str) -> ApiResult<PathBuf> {
    if !valid_substrate_name(name) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid substrate name"));
    }

    let path = commands::substrate_root().join(name);
    if !path.is_dir() {
        return Err(error(
            StatusCode::NOT_FOUND,
            format!("substrate does not exist: {name}"),
        ));
    }

    for filename in SUBSTRATE_FILES {
        let file = path.join(filename);
        if !file.is_file() {
            return Err(error(
                StatusCode::CONFLICT,
                format!("substrate state is incomplete: missing {}", file.display()),
            ));
        }
    }

    Ok(path)
}

fn list_substrates() -> ApiResult<SubstrateListResponse> {
    // The filesystem is the discovery mechanism. Listing is read-only:
    // do not create the canonical root just because a client requests /wallets.
    let root = commands::substrate_root();

    if !root.is_dir() {
        return Ok(SubstrateListResponse {
            substrates: Vec::new(),
        });
    }

    let entries = std::fs::read_dir(&root).map_err(|io_error| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unable to list substrates: {io_error}"),
        )
    })?;

    let mut substrates = Vec::new();

    for entry in entries {
        let entry = entry.map_err(|io_error| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("unable to read substrate entry: {io_error}"),
            )
        })?;

        let path = entry.path();
        if !path.is_dir() {
            continue;
        }

        let name = match entry.file_name().into_string() {
            Ok(name) if valid_substrate_name(&name) => name,
            _ => continue,
        };

        // Only expose actual substrate directories, not arbitrary folders.
        if !path.join("perm.indices").is_file()
            || !path.join("perm.entropy").is_file()
            || !path.join("perm.dimension").is_file()
        {
            continue;
        }

        substrates.push(SubstrateEntry {
            name,
            dimension: required_file(&path.join("perm.dimension"))?,
            activations: read_trim(&path.join("activation"))
                .unwrap_or_else(|| "0".to_string()),
        });
    }

    substrates.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(SubstrateListResponse { substrates })
}

fn network_option(
    id: &str,
    name: &str,
    coin: CoinType,
    purpose: u64,
    path: &str,
) -> NetworkOption {
    NetworkOption {
        id: id.to_string(),
        name: name.to_string(),
        coin: coin.retained_mass() as u64,
        purpose,
        path: path.to_string(),
    }
}

fn registered_networks() -> NetworkListResponse {
    let mut networks = Vec::new();

    for coin in CoinType::all() {
        let id = match coin {
            CoinType::Bitcoin => "bitcoin-legacy",
            CoinType::Ethereum => "ethereum",
            CoinType::Solana => "solana",
            CoinType::Tron => "tron",
        };

        networks.push(network_option(
            id,
            coin.name(),
            *coin,
            44,
            PathsRegistry::get_path(*coin),
        ));
    }

    networks.push(network_option(
        "bitcoin-native-segwit",
        "Bitcoin · Native SegWit (BIP 84)",
        CoinType::Bitcoin,
        84,
        "m/84'/0'/0'/0/0",
    ));

    networks.sort_by(|a, b| a.id.cmp(&b.id));
    NetworkListResponse { networks }
}

fn selected_network(id: &str) -> Option<NetworkOption> {
    registered_networks()
        .networks
        .into_iter()
        .find(|network| network.id == id)
}

fn scale_name(scale: Scale) -> &'static str {
    match scale {
        Scale::Minimal => "Minimal",
        Scale::Local => "Local",
        Scale::Recursive => "Recursive",
        Scale::Global => "Global",
    }
}

fn serialize_runtime(runtime: SystemTime) -> RuntimeResponse {
    let duration = runtime.duration_since(UNIX_EPOCH).unwrap_or_default();
    RuntimeResponse {
        unix_seconds: duration.as_secs(),
        subsec_nanos: duration.subsec_nanos(),
    }
}

fn qtm_frame(qtm: QtmResponse) -> QtmFrameResponse {
    QtmFrameResponse {
        identifier: qtm.commitment.clone(),
        qtm,
    }
}

fn depth_response(depth: &Depth) -> DepthResponse {
    DepthResponse {
        path: depth.path().to_string(),
        objects: depth
            .objects()
            .map(|(position, identifier, retain)| DepthObjectResponse {
                position,
                identifier: identifier.to_string(),
                mass: retain.mass.to_string(),
                from: retain.from.to_string(),
            })
            .collect(),
    }
}

fn write_depth_payload(record_path: &FsPath, network: &str, depth: &Depth) -> ApiResult<()> {
    // Restrict the network directory to a single path component.
    if network.is_empty()
        || network == "."
        || network == ".."
        || !network
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
    {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid network identifier for depth persistence",
        ));
    }

    let payload_path = record_path.join(network).join("payload");

    std::fs::create_dir_all(&payload_path).map_err(|e| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "unable to create depth payload directory {}: {e}",
                payload_path.display()
            ),
        )
    })?;

    let response = depth_response(depth);
    let path_file = payload_path.join("depth.path");
    let objects_file = payload_path.join("depth.objects");
    let mut objects = String::new();

    for object in &response.objects {
        // Hex-encode identifiers so tabs/newlines cannot corrupt the TSV format.
        objects.push_str(&format!(
            "{}\t{}\t{}\t{}\n",
            object.position,
            hex::encode(object.identifier.as_bytes()),
            object.mass,
            object.from
        ));
    }

    write_atomic(&path_file, response.path.as_bytes())?;
    write_atomic(&objects_file, objects.as_bytes())?;
    Ok(())
}

fn write_atomic(path: &FsPath, contents: &[u8]) -> ApiResult<()> {
    let parent = path.parent().ok_or_else(|| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "depth persistence path has no parent directory",
        )
    })?;

    let file_name = path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("depth");
    let temporary = parent.join(format!("{file_name}.tmp"));

    std::fs::write(&temporary, contents).map_err(|e| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unable to write {}: {e}", temporary.display()),
        )
    })?;

    std::fs::rename(&temporary, path).map_err(|e| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unable to persist {}: {e}", path.display()),
        )
    })?;

    Ok(())
}

fn latest_record_path(substrate_path: &FsPath, prefix: &str) -> ApiResult<PathBuf> {
    let events_path = substrate_path.join("events");

    let entries = std::fs::read_dir(&events_path).map_err(|e| {
        error(
            StatusCode::NOT_FOUND,
            format!(
                "event store is unavailable at {}: {e}",
                events_path.display()
            ),
        )
    })?;

    let mut records: Vec<(SystemTime, PathBuf)> = Vec::new();

    for entry in entries {
        let entry = entry.map_err(|e| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("unable to read event-store entry: {e}"),
            )
        })?;

        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();

        if !path.is_dir() || !name.starts_with(prefix) {
            continue;
        }

        let modified = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .unwrap_or(UNIX_EPOCH);

        records.push((modified, path));
    }

    records.sort_by(|a, b| b.0.cmp(&a.0));

    records
        .into_iter()
        .map(|(_, path)| path)
        .next()
        .ok_or_else(|| {
            error(
                StatusCode::NOT_FOUND,
                format!("no {prefix} record exists in the event store"),
            )
        })
}

fn parse_record_identity(record: &str) -> Option<(u64, u128)> {
    let rest = record
        .strip_prefix("event_")
        .or_else(|| record.strip_prefix("exile_"))?;

    let (activation, dimension) = rest.split_once('_')?;
    Some((activation.parse().ok()?, dimension.parse().ok()?))
}

fn canonical_record_path(
    substrate_path: &FsPath,
    kind: &str,
    activation: u64,
    dimension: u128,
) -> ApiResult<PathBuf> {
    let prefix = match kind {
        "transit" => "event",
        "exile" => "exile",
        _ => {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "record kind must be transit or exile",
            ));
        }
    };

    let path = substrate_path
        .join("events")
        .join(format!("{prefix}_{activation}_{dimension}"));

    if !path.is_dir() {
        return Err(error(
            StatusCode::NOT_FOUND,
            format!(
                "canonical {kind} record does not exist: {}",
                path.display()
            ),
        ));
    }

    Ok(path)
}

fn valid_record_name(record: &str) -> bool {
    (record.starts_with("event_") || record.starts_with("exile_"))
        && record
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
        && parse_record_identity(record).is_some()
}

fn read_persisted_depth(payload_path: &FsPath) -> ApiResult<DepthResponse> {
    let path_file = payload_path.join("depth.path");
    let objects_file = payload_path.join("depth.objects");

    let path = std::fs::read_to_string(&path_file).map_err(|e| {
        error(
            StatusCode::NOT_FOUND,
            format!(
                "persisted depth path is unavailable at {}: {e}",
                path_file.display()
            ),
        )
    })?;

    let contents = std::fs::read_to_string(&objects_file).map_err(|e| {
        error(
            StatusCode::NOT_FOUND,
            format!(
                "persisted depth objects are unavailable at {}: {e}",
                objects_file.display()
            ),
        )
    })?;

    let mut objects = Vec::new();

    for (line_number, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.splitn(4, '\t').collect();
        if fields.len() != 4 {
            return Err(error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "invalid persisted depth object at {}:{}",
                    objects_file.display(),
                    line_number + 1
                ),
            ));
        }

        let position = fields[0].parse::<usize>().map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid depth position at line {}", line_number + 1),
            )
        })?;

        let identifier_bytes = hex::decode(fields[1]).map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid depth identifier at line {}", line_number + 1),
            )
        })?;

        let identifier = String::from_utf8(identifier_bytes).map_err(|_| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!(
                    "depth identifier is not valid UTF-8 at line {}",
                    line_number + 1
                ),
            )
        })?;

        objects.push(DepthObjectResponse {
            position,
            identifier,
            mass: fields[2].to_string(),
            from: fields[3].to_string(),
        });
    }

    Ok(DepthResponse {
        path: path.trim().to_string(),
        objects,
    })
}

fn event_map_response(direction: Direction, depth: &Depth) -> EventMapResponse {
    let event = direction.into_event();
    EventMapResponse {
        scale: scale_name(event.scale).to_string(),
        runtime: serialize_runtime(event.runtime),
        depth: depth_response(depth),
    }
}

fn direction_response(
    direction: Direction,
    selection: Option<TransitSelection>,
    qtm: QtmResponse,
    depth: &Depth,
) -> DirectionResponse {
    DirectionResponse {
        frame: DirectionFrameResponse {
            qtm: qtm_frame(qtm),
            event: event_map_response(direction, depth),
        },
        selection,
    }
}

/* ---------- Activation/network budget persistence ---------- */

fn budget_directory(substrate_path: &FsPath, activation: u64, network: &str) -> PathBuf {
    substrate_path
        .join("event")
        .join(activation.to_string())
        .join(network)
}

fn budget_file_path(substrate_path: &FsPath, activation: u64, network: &str) -> PathBuf {
    budget_directory(substrate_path, activation, network).join("budget")
}

fn validate_budget(body: &BudgetRequest) -> Result<(), String> {
    if body.max_events == 0 {
        return Err("max_events must be greater than zero".to_string());
    }
    if body.min_events > body.max_events {
        return Err("min_events must be less than or equal to max_events".to_string());
    }
    if body.max_retained_mass == 0 {
        return Err("max_retained_mass must be greater than zero".to_string());
    }
    Ok(())
}

fn parse_budget(contents: &str, path: &FsPath) -> ApiResult<EventBudget> {
    let mut min_events: Option<u64> = None;
    let mut max_events: Option<u64> = None;
    let mut max_retained_mass: Option<u128> = None;

    for line in contents.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };

        let value = value.trim();
        match key.trim() {
            "min_events" => {
                min_events = Some(value.parse::<u64>().map_err(|_| {
                    error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("invalid min_events in {}", path.display()),
                    )
                })?);
            }
            "max_events" => {
                max_events = Some(value.parse::<u64>().map_err(|_| {
                    error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("invalid max_events in {}", path.display()),
                    )
                })?);
            }
            "max_retained_mass" => {
                max_retained_mass = Some(value.parse::<u128>().map_err(|_| {
                    error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("invalid max_retained_mass in {}", path.display()),
                    )
                })?);
            }
            _ => {}
        }
    }

    let budget = EventBudget {
        min_events: min_events.ok_or_else(|| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("budget file is missing min_events: {}", path.display()),
            )
        })?,
        max_events: max_events.ok_or_else(|| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("budget file is missing max_events: {}", path.display()),
            )
        })?,
        max_retained_mass: max_retained_mass
            .ok_or_else(|| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "budget file is missing max_retained_mass: {}",
                        path.display()
                    ),
                )
            })?
            .to_string(),
    };

    if budget.max_events == 0
        || budget.min_events > budget.max_events
        || budget.max_retained_mass == "0"
    {
        return Err(error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("invalid budget values in {}", path.display()),
        ));
    }

    Ok(budget)
}

fn load_network_budget(
    substrate_path: &FsPath,
    activation: u64,
    network: &str,
) -> ApiResult<Option<EventBudget>> {
    let path = budget_file_path(substrate_path, activation, network);
    if !path.exists() {
        return Ok(None);
    }

    let contents = std::fs::read_to_string(&path).map_err(|io_error| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unable to read budget {}: {io_error}", path.display()),
        )
    })?;

    Ok(Some(parse_budget(&contents, &path)?))
}

fn persist_network_budget(
    substrate_path: &FsPath,
    activation: u64,
    network: &str,
    body: &BudgetRequest,
) -> ApiResult<EventBudget> {
    validate_budget(body).map_err(|message| error(StatusCode::BAD_REQUEST, message))?;

    let directory = budget_directory(substrate_path, activation, network);
    std::fs::create_dir_all(&directory).map_err(|io_error| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "unable to create network budget directory {}: {io_error}",
                directory.display()
            ),
        )
    })?;

    let path = directory.join("budget");
    let temporary_path = directory.join("budget.tmp");
    let contents = format!(
        "min_events={}\nmax_events={}\nmax_retained_mass={}\n",
        body.min_events, body.max_events, body.max_retained_mass
    );

    std::fs::write(&temporary_path, contents).map_err(|io_error| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "unable to write temporary budget {}: {io_error}",
                temporary_path.display()
            ),
        )
    })?;

    std::fs::rename(&temporary_path, &path).map_err(|io_error| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unable to persist budget {}: {io_error}", path.display()),
        )
    })?;

    Ok(EventBudget {
        min_events: body.min_events,
        max_events: body.max_events,
        max_retained_mass: body.max_retained_mass.to_string(),
    })
}

async fn list_activation_budgets(
    Path((name, activation)): Path<(String, u64)>,
) -> ApiResult<Json<ActivationBudgetsResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let substrate_path = ensure_existing_substrate(&name)?;
    let directory = substrate_path.join("event").join(activation.to_string());
    let mut budgets = Vec::new();

    for network in registered_networks().networks {
        let budget = load_network_budget(&substrate_path, activation, &network.id)?;
        budgets.push(NetworkBudgetEntry {
            configured: budget.is_some(),
            network,
            activation,
            budget,
        });
    }

    Ok(Json(ActivationBudgetsResponse {
        substrate: name,
        activation,
        directory: directory.display().to_string(),
        budgets,
    }))
}

async fn update_activation_network_budget(
    Path((name, activation, network_id)): Path<(String, u64, String)>,
    Json(body): Json<BudgetRequest>,
) -> ApiResult<Json<NetworkBudgetEntry>> {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let substrate_path = ensure_existing_substrate(&name)?;

    let network = selected_network(&network_id).ok_or_else(|| {
        error(
            StatusCode::BAD_REQUEST,
            format!("unsupported network: {network_id}"),
        )
    })?;

    let budget = persist_network_budget(&substrate_path, activation, &network.id, &body)?;

    Ok(Json(NetworkBudgetEntry {
        network,
        activation,
        configured: true,
        budget: Some(budget),
    }))
}

/* --------------------------- Existing API --------------------------- */

async fn health() -> &'static str {
    "ok"
}

async fn preflight() -> StatusCode {
    StatusCode::NO_CONTENT
}

async fn list_networks() -> Json<NetworkListResponse> {
    Json(registered_networks())
}

async fn list_wallets() -> ApiResult<Json<SubstrateListResponse>> {
    Ok(Json(list_substrates()?))
}

async fn head_wallets() -> ApiResult<StatusCode> {
    list_substrates()?;
    Ok(StatusCode::OK)
}

async fn list_event_payloads(
    Path((name, record)): Path<(String, String)>,
) -> ApiResult<Json<PersistedPayloadsResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let substrate_path = ensure_existing_substrate(&name)?;

    if !valid_record_name(&record) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid event record identifier",
        ));
    }

    let record_path = substrate_path.join("events").join(&record);
    if !record_path.is_dir() {
        return Err(error(
            StatusCode::NOT_FOUND,
            format!("event record does not exist: {record}"),
        ));
    }

    let direction = if record.starts_with("event_") {
        "transit_inputs"
    } else {
        "exile_outputs"
    };

    let entries = std::fs::read_dir(&record_path).map_err(|e| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unable to read event record: {e}"),
        )
    })?;

    let mut payloads = Vec::new();

    for entry in entries {
        let entry = entry.map_err(|e| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("unable to read event payload entry: {e}"),
            )
        })?;

        let network_path = entry.path();
        if !network_path.is_dir() {
            continue;
        }

        let network_id = entry.file_name().to_string_lossy().to_string();
        // Recognize payloads only when both persisted depth files exist.
        let payload_path = network_path.join("payload");
        if !payload_path.join("depth.path").is_file()
            || !payload_path.join("depth.objects").is_file()
        {
            continue;
        }

        let depth = read_persisted_depth(&payload_path)?;
        let selected = selected_network(&network_id);
        let registered = selected.is_some();
        let network_name = selected
            .map(|network| network.name)
            .unwrap_or_else(|| network_id.clone());

        payloads.push(PersistedPayloadEntry {
            record: record.clone(),
            direction: direction.to_string(),
            network: PersistedPayloadNetwork {
                id: network_id,
                name: network_name,
                registered,
            },
            path: payload_path.display().to_string(),
            depth_identifier: depth.path,
            objects: depth.objects,
        });
    }

    payloads.sort_by(|a, b| a.network.id.cmp(&b.network.id));

    Ok(Json(PersistedPayloadsResponse {
        substrate: name,
        record,
        directory: record_path.display().to_string(),
        payloads,
    }))
}

async fn list_events(Path(name): Path<String>) -> ApiResult<Json<EventStoreResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let substrate_path = ensure_existing_substrate(&name)?;
    let events_path = substrate_path.join("events");
    let mut events = Vec::new();

    if events_path.is_dir() {
        let entries = std::fs::read_dir(&events_path).map_err(|e| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("unable to list events: {e}"),
            )
        })?;

        for entry in entries {
            let entry = entry.map_err(|e| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("unable to read event entry: {e}"),
                )
            })?;

            let record_path = entry.path();
            if !record_path.is_dir() {
                continue;
            }

            let record = entry.file_name().to_string_lossy().to_string();
            let kind = if record.starts_with("exile_") {
                "exile"
            } else if record.starts_with("event_") {
                "transit"
            } else {
                continue;
            };

            let activation = read_trim(&record_path.join("manifold.activations"))
                .or_else(|| {
                    parse_record_identity(&record)
                        .map(|(activation, _)| activation.to_string())
                })
                .unwrap_or_else(|| "unknown".to_string());

            let dimension = read_trim(&record_path.join("manifold.dimension"))
                .or_else(|| {
                    parse_record_identity(&record)
                        .map(|(_, dimension)| dimension.to_string())
                })
                .unwrap_or_else(|| "unknown".to_string());

            let structural_value = read_trim(&record_path.join("manifold.structural_value"))
                .unwrap_or_else(|| "unknown".to_string());

            let retained_mass = read_trim(&record_path.join("manifold.retained_mass"))
                .unwrap_or_else(|| "unknown".to_string());

            let depth_objects = read_trim(&record_path.join("depth.objects"))
                .map(|contents| {
                    contents
                        .lines()
                        .filter(|line| !line.trim().is_empty())
                        .count()
                })
                .unwrap_or(0);

            events.push(PersistedEventSummary {
                kind: kind.to_string(),
                record,
                activation,
                dimension,
                structural_value,
                depth_objects,
                retained_mass,
                // Read stored transition values; missing values stay unknown.
                delta: read_trim(&record_path.join("transition.delta"))
                    .unwrap_or_else(|| "unknown".to_string()),
                tau: read_trim(&record_path.join("transition.tau"))
                    .unwrap_or_else(|| "unknown".to_string()),
                gross_work: read_trim(&record_path.join("transition.gross_work"))
                    .unwrap_or_else(|| "unknown".to_string()),
                net_work: read_trim(&record_path.join("transition.net_work"))
                    .unwrap_or_else(|| "unknown".to_string()),
                commitment: read_trim(&record_path.join("qtm.commitment"))
                    .unwrap_or_else(|| "unknown".to_string()),
                coordinate: read_trim(&record_path.join("qtm.coordinate"))
                    .unwrap_or_else(|| "unknown".to_string()),
            });
        }
    }

    events.sort_by(|a, b| {
        let a_activation = a.activation.parse::<u64>().unwrap_or(0);
        let b_activation = b.activation.parse::<u64>().unwrap_or(0);
        a_activation
            .cmp(&b_activation)
            .then_with(|| a.record.cmp(&b.record))
    });

    Ok(Json(EventStoreResponse {
        substrate: name,
        directory: substrate_path.display().to_string(),
        events_directory: events_path.display().to_string(),
        events,
        runtime: serialize_runtime(SystemTime::now()),
    }))
}

async fn create_wallet(
    Json(body): Json<CreateRequest>,
) -> ApiResult<(StatusCode, Json<SubstrateResponse>)> {
    let name = body.name.trim();

    if !valid_substrate_name(name) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "name must contain 1-64 ASCII letters, digits, hyphens, or underscores",
        ));
    }

    let (indices, entropy) = match body
        .seed_source
        .as_deref()
        .unwrap_or("persistent")
        .trim()
        .to_ascii_lowercase()
        .as_str()
    {
        "persistent" | "custom" => {
            let raw_indices = body.indices.as_deref().ok_or_else(|| {
                error(
                    StatusCode::BAD_REQUEST,
                    "indices are required for persistent creation",
                )
            })?;

            let parsed = parse_indices(raw_indices)
                .map_err(|message| error(StatusCode::BAD_REQUEST, message))?;

            let entropy = body
                .entropy
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| error(StatusCode::BAD_REQUEST, "non-empty entropy is required"))?
                .to_string();

            (
                parsed.iter().map(u16::to_string).collect::<Vec<_>>().join(","),
                entropy,
            )
        }
        "bip39" => {
            let phrase = body
                .phrase
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| error(StatusCode::BAD_REQUEST, "a BIP39 phrase is required"))?;

            let language_name = body.language.as_deref().ok_or_else(|| {
                error(StatusCode::BAD_REQUEST, "select a BIP39 language in the UI")
            })?;

            let language = parse_bip39_language(language_name)
                .map_err(|message| error(StatusCode::BAD_REQUEST, message))?;

            let indices = BIP39Handler::mnemonic_indices(phrase, language).map_err(|e| {
                error(
                    StatusCode::BAD_REQUEST,
                    format!("invalid BIP39 mnemonic: {e}"),
                )
            })?;

            let entropy = BIP39Handler::mnemonic_entropy(
                phrase,
                language,
                body.passphrase.as_deref(),
            )
            .map_err(|e| {
                error(
                    StatusCode::BAD_REQUEST,
                    format!("unable to derive BIP39 conditioning entropy: {e}"),
                )
            })?;

            (
                indices.iter().map(u16::to_string).collect::<Vec<_>>().join(","),
                hex::encode(entropy),
            )
        }
        _ => {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "seed_source must be persistent, custom, or bip39",
            ));
        }
    };

    let lock = substrate_lock(name);
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let root = commands::substrate_root();

    std::fs::create_dir_all(&root).map_err(|io_error| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unable to access substrate root: {io_error}"),
        )
    })?;

    if root.join(name).exists() {
        return Err(error(
            StatusCode::CONFLICT,
            format!("substrate already exists: {name}"),
        ));
    }

    // Installation is delegated to the canonical qp_hd runtime.
    guarded("substrate creation", || {
        commands::qtm_create(name.to_string(), Some(indices), Some(entropy))
    })?;

    // qtm_create returns (), so confirm the runtime actually persisted a valid
    // substrate before returning HTTP 201 to the client.
    ensure_existing_substrate(name)?;

    Ok((StatusCode::CREATED, Json(substrate_json(name)?)))
}

async fn open_wallet(Path(name): Path<String>) -> ApiResult<Json<SubstrateResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    ensure_existing_substrate(&name)?;
    guarded("open", || commands::qtm_open(name.clone()))?;
    Ok(Json(substrate_json(&name)?))
}

async fn transit_wallet(
    Path(name): Path<String>,
    Json(body): Json<TransitRequest>,
) -> ApiResult<Json<DirectionResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let substrate_path = ensure_existing_substrate(&name)?;

    let preset = match body.network.as_deref() {
        Some(id) => Some(selected_network(id).ok_or_else(|| {
            error(
                StatusCode::BAD_REQUEST,
                format!("unsupported network: {id}"),
            )
        })?),
        None => None,
    };

    let purpose = body
        .purpose
        .or_else(|| preset.as_ref().map(|network| network.purpose))
        .unwrap_or(44);

    let coin = body
        .coin
        .or_else(|| preset.as_ref().map(|network| network.coin))
        .unwrap_or(0);

    let account = body.account.unwrap_or(0);
    let change = body.change.unwrap_or(0);
    let external = body.external.unwrap_or(0);

    let path = match body.path {
        Some(path) => {
            let path = path.trim();
            if path.is_empty() {
                return Err(error(StatusCode::BAD_REQUEST, "path must not be empty"));
            }
            Some(path.to_string())
        }
        None => preset.as_ref().map(|network| network.path.clone()),
    };

    let network_id = body
        .network
        .as_deref()
        .or_else(|| preset.as_ref().map(|network| network.id.as_str()))
        .unwrap_or("default")
        .to_string();

    let selection = TransitSelection {
        network: Some(network_id.clone()),
        path,
        activation: body.activation,
        purpose,
        coin,
        account,
        change,
        external,
    };

    let payload = TotalMass::new(
        purpose as u128,
        coin as u128,
        account as u128,
        change as u128,
        external as u128,
    )
    .memorize();

    // Execute the transition exactly once.
    let event = guarded("transit", || {
        commands::qtm_transit(&name, payload, body.activation)
    })?;
    let activation = event.heritage.state.activations();
    let dimension = event.heritage.state.dimension();

    let record_path = canonical_record_path(
        &substrate_path,
        "transit",
        activation,
        u128::from(dimension),
    )?;
    // Persist the actual input depth under this event and network.
    write_depth_payload(&record_path, &network_id, &event.depth)?;

    let qtm = QtmResponse {
        commitment: hex::encode(event.qtm.commitment),
        coordinate: hex::encode(event.qtm.coordinate),
        sigma: event.qtm.sigma.to_string(),
        network: event.qtm.net_work.to_string(),
    };

    let direction = Direction::from_heritage(event.heritage, Scale::Local);

    Ok(Json(direction_response(
        direction,
        Some(selection),
        qtm,
        &event.depth,
    )))
}

async fn receive_wallet(
    Path(name): Path<String>,
    Json(body): Json<ReceiveRequest>,
) -> ApiResult<Json<ReceiveResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    ensure_existing_substrate(&name)?;

    if body.amount == 0 {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "amount must be greater than zero",
        ));
    }

    let flow = match body.flow.trim().to_ascii_lowercase().as_str() {
        "receive" => Flow::Receive,
        "change" => Flow::Change,
        _ => {
            return Err(error(
                StatusCode::BAD_REQUEST,
                "flow must be receive or change",
            ));
        }
    };

    let transaction = guarded("receive", || QuantumTransaction::send(&name, body.amount))?;
    let transaction_response = QuantumTransactionResponse {
        amount: transaction.amount,
        qtm: qtm_frame(QtmResponse {
            commitment: hex::encode(transaction.qtm.commitment),
            coordinate: hex::encode(transaction.qtm.coordinate),
            sigma: transaction.qtm.sigma.to_string(),
            network: transaction.qtm.net_work.to_string(),
        }),
        constant: match &transaction.constant {
            qp_hd::PersistedEvent::Transit(_) => "transit".to_string(),
            qp_hd::PersistedEvent::Exile(_) => "exile".to_string(),
        },
    };

    let event = guarded("receive", || transaction.receive(&name, flow))?;
    let direction = Direction::from_heritage(event.heritage, Scale::Local);
    let event_response = event_map_response(direction, &event.depth);

    Ok(Json(ReceiveResponse {
        frame: QuantumTransactionFrame {
            quantum_transaction: transaction_response,
            event: event_response,
        },
    }))
}

async fn exile_depth(Path(name): Path<String>) -> ApiResult<Json<DepthResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let substrate_path = ensure_existing_substrate(&name)?;
    let record_path = latest_record_path(&substrate_path, "exile_")?;

    // The canonical exile depth is stored at the record root.
    if record_path.join("depth.path").is_file() && record_path.join("depth.objects").is_file() {
        return Ok(Json(read_persisted_depth(&record_path)?));
    }

    // Compatibility fallback for older records that stored depth under a network payload.
    let entries = std::fs::read_dir(&record_path).map_err(|e| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unable to read exile record: {e}"),
        )
    })?;

    let mut payload_directories = Vec::new();

    for entry in entries {
        let entry = entry.map_err(|e| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("unable to read exile network entry: {e}"),
            )
        })?;

        let payload = entry.path().join("payload");
        if payload.join("depth.path").is_file() && payload.join("depth.objects").is_file() {
            payload_directories.push(payload);
        }
    }

    payload_directories.sort();

    let payload_path = payload_directories.into_iter().next().ok_or_else(|| {
        error(
            StatusCode::NOT_FOUND,
            format!("no persisted exile depth exists for substrate: {name}"),
        )
    })?;

    Ok(Json(read_persisted_depth(&payload_path)?))
}

async fn exile_wallet(Path(name): Path<String>) -> ApiResult<Json<DirectionResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    let substrate_path = ensure_existing_substrate(&name)?;

    // Capture network identifiers before qtm_exile consumes the transit record.
    // qtm_exile locates the transit record through qp_hd's own activation
    // accessor, so use the same source here.
    let activation = guarded("activation lookup", || commands::load_activation(&name))?;

    let transit_path = commands::event_path_at_activation(&name, activation).ok_or_else(|| {
        error(
            StatusCode::NOT_FOUND,
            format!("no transit event exists at activation {activation}"),
        )
    })?;

    let mut network_ids = Vec::new();
    let entries = std::fs::read_dir(&transit_path).map_err(|e| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unable to inspect transit payloads: {e}"),
        )
    })?;

    for entry in entries {
        let entry = entry.map_err(|e| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("unable to inspect transit network: {e}"),
            )
        })?;

        let payload = entry.path().join("payload");
        if payload.join("depth.path").is_file() && payload.join("depth.objects").is_file() {
            network_ids.push(entry.file_name().to_string_lossy().to_string());
        }
    }

    network_ids.sort();
    network_ids.dedup();

    // Canonical balance/exile transition: persist resulting heritage and consume transit.
    let event = guarded("exile", || commands::qtm_exile(&name))?;
    let resulting_activation = event.heritage.state.activations();
    let dimension = event.heritage.state.dimension();

    let record_path = canonical_record_path(
        &substrate_path,
        "exile",
        resulting_activation,
        u128::from(dimension),
    )?;

    // Event root is canonical output; network payloads are persisted only for input networks.
    for network_id in &network_ids {
        write_depth_payload(&record_path, network_id, &event.depth)?;
    }

    let qtm = QtmResponse {
        commitment: hex::encode(event.qtm.commitment),
        coordinate: hex::encode(event.qtm.coordinate),
        sigma: event.qtm.sigma.to_string(),
        network: event.qtm.net_work.to_string(),
    };

    let direction = Direction::from_heritage(event.heritage, Scale::Local);

    Ok(Json(direction_response(
        direction,
        None,
        qtm,
        &event.depth,
    )))
}

/* ---------- Observation stream lifecycle (bit_minner manager) ----------
 *
 * The API no longer spawns a bit-miner OS process. The long-lived Mempool.space
 * stream is owned by bit_minner::observestream::ObservationStreamManager, which
 * keeps exactly one stream + pipeline per (substrate, activation). Network
 * routes share that stream. OBSERVATION_REGISTRY remains the HTTP status map.
 */

/// Asks the manager whether the stream for (substrate, activation) is running.
/// Never creates a pipeline. Must not be called while holding a std Mutex guard
/// (the future would not be Send).
async fn observation_stream_running(substrate: &str, activation: u64) -> bool {
    #[cfg(feature = "bit-miner")]
    {
        bit_miner::observestream::ObservationStreamManager::global()
            .is_running(substrate, activation)
            .await
    }

    #[cfg(not(feature = "bit-miner"))]
    {
        let _ = (substrate, activation);
        false
    }
}

/// Builds the clip status. `running` comes from the stream manager; `direction`
/// is a filesystem scan of persisted event_{activation}_ records only. It
/// describes persisted event state, not whether the stream is running.
fn observation_clip_status(
    substrate: &str,
    activation: u64,
    running: bool,
) -> Result<ObservationClipStatus, String> {
    if !valid_substrate_name(substrate) {
        return Err("invalid substrate name".into());
    }

    let substrate_path = commands::substrate_root().join(substrate);
    if !substrate_path.is_dir() {
        return Err(format!("substrate does not exist: {substrate}"));
    }

    let events_path = substrate_path.join("events");
    let prefix = format!("event_{activation}_");

    let direction = match std::fs::read_dir(&events_path) {
        Ok(entries) => {
            let found = entries
                .filter_map(Result::ok)
                .any(|entry| entry.file_name().to_string_lossy().starts_with(&prefix));

            if found {
                Some(format!("event activation {activation}"))
            } else {
                None
            }
        }
        Err(io_error) if io_error.kind() == std::io::ErrorKind::NotFound => None,
        Err(io_error) => return Err(format!("failed to read events: {io_error}")),
    };

    Ok(ObservationClipStatus {
        substrate: substrate.to_owned(),
        active: running,
        direction,
    })
}

/* ---------- Bitcoin observation pipeline handlers ---------- */

async fn start_bitcoin_observation(
    Path((name, activation, network)): Path<(String, u64, String)>,
    body: Option<Json<ObservationStartRequest>>,
) -> ApiResult<Json<ObservationPipelineStatus>> {
    if !valid_observation_network_id(&network) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid network identifier for observation pipeline",
        ));
    }

    if !is_bitcoin_network_id(&network) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            format!("observation pipeline currently supports Bitcoin networks only; got: {network}"),
        ));
    }

    // The std Mutex guard is scoped so it is dropped before any .await.
    let commitment = {
        let lock = substrate_lock(&name);
        let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());
        let substrate_path = ensure_existing_substrate(&name)?;
        read_substrate_commitment(&substrate_path)?
    };

    let request = body.map(|Json(value)| value).unwrap_or_default();

    // Candidate value derived from the commitment; this WebSocket stream does
    // not bind an Electrum listener.
    let listen_port = listen_port_from_commitment(&commitment, request.listen_port)
        .map_err(|message| error(StatusCode::BAD_REQUEST, message))?;

    let path_hint = request
        .path
        .map(|value| value.trim().to_string())
        .filter(|value| !value.is_empty());

    #[cfg(feature = "bit-miner")]
    let running = {
        let manager =
            bit_miner::observestream::ObservationStreamManager::global();

        
                  let substrate = name.clone();
        let commitment_hex = commitment.clone();

        manager
            .start(&name, activation, move |txid, outcome| {
                match outcome {
                    Ok(Some((finalized, retained))) => {
                        let commitment_bytes = match hex::decode(
                            commitment_hex.trim(),
                        )
                        .ok()
                        .and_then(|bytes| <[u8; 32]>::try_from(bytes).ok())
                        {
                            Some(bytes) => bytes,
                            None => {
                                eprintln!(
                                    "wirepackets persistence failed \
                                     substrate={} activation={} txid={}: \
                                     commitment must decode to 32 bytes",
                                    substrate, activation, txid,
                                );
                                return;
                            }
                        };

                        // Store the actual serialized packets from the
                        // finalized transaction, not reconstructed payloads.
                        let mut packets = Vec::<u8>::new();
                        let mut packet_count = 0usize;

                        for vout in 0..finalized.transaction.output.len() {
                            let Ok(vout) = u32::try_from(vout) else {
                                eprintln!(
                                    "wirepackets persistence failed \
                                     txid={txid}: output index exceeds u32"
                                );
                                return;
                            };

                            let Some(packet) =
                                finalized.wire_packet(vout, commitment_bytes)
                            else {
                                continue;
                            };

                            let bytes = packet.serialize();

                            if bytes.is_empty() {
                                eprintln!(
                                    "wirepackets persistence failed \
                                     txid={txid} vout={vout}: \
                                     packet serialization returned empty bytes"
                                );
                                return;
                            }

                            // Frame each serialized packet with its
                            // four-byte big-endian payload length.
                            let Ok(length) = u32::try_from(bytes.len()) else {
                                eprintln!(
                                    "wirepackets persistence failed \
                                     txid={txid} vout={vout}: packet too large"
                                );
                                return;
                            };

                            packets.extend_from_slice(&length.to_be_bytes());
                            packets.extend_from_slice(&bytes);
                            packet_count += 1;
                        }

                        if packet_count == 0 {
                            eprintln!(
                                "Bitcoin observation finalized txid={txid}, \
                                 but produced no wire packets"
                            );
                            return;
                        }

                        match commands::persist_wirepackets(
                            &substrate,
                            activation,
                            &commitment_bytes,
                            &packets,
                        ) {
                            Ok(()) => eprintln!(
                                "Bitcoin observation finalized txid={} \
                                 substrate={} activation={} \
                                 wirepackets={} retained={}",
                                txid,
                                substrate,
                                activation,
                                packet_count,
                                retained.len(),
                            ),
                            Err(error) => eprintln!(
                                "wirepackets persistence failed \
                                 substrate={} activation={} txid={}: {}",
                                substrate,
                                activation,
                                txid,
                                error,
                            ),
                        }
                    }

                    Ok(None) => eprintln!(
                        "Bitcoin observation processed txid={txid}; \
                         no finalized settlement"
                    ),

                    Err(error) => eprintln!(
                        "Bitcoin observation failed txid={txid}: {error}"
                    ),
                }
            })
            .await
            .map_err(|e| {
                error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "unable to start Bitcoin observation stream: {e}"
                    ),
                )
            })?;

        manager.is_running(&name, activation).await
    };

    #[cfg(not(feature = "bit-miner"))]
    let running: bool = return Err(error(
        StatusCode::NOT_IMPLEMENTED,
        "qtm-api was built without the bit-minner feature",
    ));

    let clip = observation_clip_status(&name, activation, running)
        .map_err(|message| error(StatusCode::INTERNAL_SERVER_ERROR, message))?;

    let status = ObservationPipelineStatus {
        substrate: name.clone(),
        activation,
        network: network.clone(),
        commitment: commitment.clone(),
        listen_port,
        running,
        path: path_hint,
        clips: vec![clip],
        runtime: serialize_runtime(SystemTime::now()),
        message: format!(
            "Bitcoin observation stream started substrate={name} activation={activation} network={network} commitment={commitment} listen_port_candidate={listen_port}"
        ),
    };

    observation_registry()
        .lock()
        .unwrap_or_else(|p| p.into_inner())
        .insert(
            observation_key(&name, activation, &network),
            LiveObservation { status: status.clone() },
        );

    Ok(Json(status))
}

async fn get_bitcoin_observation(
    Path((name, activation, network)): Path<(String, u64, String)>,
) -> ApiResult<Json<ObservationPipelineStatus>> {
    if !valid_observation_network_id(&network) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid network identifier for observation pipeline",
        ));
    }

    {
        let lock = substrate_lock(&name);
        let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());
        ensure_existing_substrate(&name)?;
    }

    let running = observation_stream_running(&name, activation).await;

    let clip = observation_clip_status(&name, activation, running)
        .map_err(|message| error(StatusCode::INTERNAL_SERVER_ERROR, message))?;

    let key = observation_key(&name, activation, &network);
    let mut registry = observation_registry()
        .lock()
        .unwrap_or_else(|p| p.into_inner());

    let live = registry.get_mut(&key).ok_or_else(|| {
        error(
            StatusCode::NOT_FOUND,
            format!("no observation status for substrate={name} activation={activation} network={network}"),
        )
    })?;

    live.status.running = running;
    live.status.clips = vec![clip];
    live.status.runtime = serialize_runtime(SystemTime::now());

    if !live.status.running {
        return Err(error(
            StatusCode::NOT_FOUND,
            format!("observation stream is not running for substrate={name} activation={activation}"),
        ));
    }

    Ok(Json(live.status.clone()))
}

async fn list_bitcoin_observations(
    Path((name, activation)): Path<(String, u64)>,
) -> ApiResult<Json<ObservationListResponse>> {
    {
        let lock = substrate_lock(&name);
        let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());
        ensure_existing_substrate(&name)?;
    }

    // Query the manager once for this substrate + activation.
    let running = observation_stream_running(&name, activation).await;

    let clip = observation_clip_status(&name, activation, running)
        .map_err(|message| error(StatusCode::INTERNAL_SERVER_ERROR, message))?;

    let mut registry = observation_registry()
        .lock()
        .unwrap_or_else(|p| p.into_inner());

    let mut pipelines: Vec<ObservationPipelineStatus> = registry
        .iter_mut()
        .filter(|(key, _)| key.0 == name && key.1 == activation)
        .filter_map(|(_, live)| {
            live.status.running = running;
            live.status.clips = vec![clip.clone()];
            live.status.runtime = serialize_runtime(SystemTime::now());

            live.status.running.then(|| live.status.clone())
        })
        .collect();

    pipelines.sort_by(|a, b| {
        a.network
            .cmp(&b.network)
            .then_with(|| a.commitment.cmp(&b.commitment))
            .then_with(|| a.listen_port.cmp(&b.listen_port))
    });

    Ok(Json(ObservationListResponse {
        substrate: name,
        activation,
        pipelines,
    }))
}

async fn stop_bitcoin_observation(
    Path((name, activation, network)): Path<(String, u64, String)>,
) -> ApiResult<Json<ObservationPipelineStatus>> {
    if !valid_observation_network_id(&network) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid network identifier for observation pipeline",
        ));
    }

    let commitment = {
        let lock = substrate_lock(&name);
        let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());
        let substrate_path = ensure_existing_substrate(&name)?;
        read_substrate_commitment(&substrate_path)?
    };

    #[cfg(feature = "bit-miner")]
    let stopped =
        bit_miner::observestream::ObservationStreamManager::global()
            .stop(&name, activation)
            .await;

    #[cfg(not(feature = "bit-miner"))]
    let stopped = false;

    let key = observation_key(&name, activation, &network);

    let mut status = {
        let mut registry = observation_registry()
            .lock()
            .unwrap_or_else(|p| p.into_inner());

        // The stream is shared per (substrate, activation), so every network
        // entry for it is no longer running once it is stopped.
        if stopped {
            for (entry_key, live) in registry.iter_mut() {
                if entry_key.0 == name && entry_key.1 == activation {
                    live.status.running = false;
                }
            }
        }

        match registry.remove(&key) {
            Some(live) => live.status,
            None if stopped => ObservationPipelineStatus {
                substrate: name.clone(),
                activation,
                network: network.clone(),
                commitment: commitment.clone(),
                listen_port: listen_port_from_commitment(&commitment, None).unwrap_or(0),
                running: false,
                path: None,
                clips: Vec::new(),
                runtime: serialize_runtime(SystemTime::now()),
                message: String::new(),
            },
            None => {
                return Err(error(
                    StatusCode::NOT_FOUND,
                    format!("no observation status for substrate={name} activation={activation} network={network}"),
                ));
            }
        }
    };

    status.running = false;
    status.clips = vec![
        observation_clip_status(&name, activation, false)
            .map_err(|message| error(StatusCode::INTERNAL_SERVER_ERROR, message))?,
    ];
    status.message = format!(
        "Bitcoin observation stream stopped={stopped} substrate={name} activation={activation} network={network}"
    );
    status.runtime = serialize_runtime(SystemTime::now());

    Ok(Json(status))
}

/*
 * Payload board — exclusive API entry into shared payload state (payload.rs).
 *
 * Model (from payload.rs + bit_miner::sharedtypes):
 *   PayloadKind::{Bitcoin, Ethereum, Solana, Tron}
 *   AuthorizedPayload { request: SettlementRequest, authorization_state, settlement_state }
 *   SettlementRequest { packet, authorization: Authorization { commitment, activation, locked } }
 *   AuthorizationState::{Unauthorized, Authorized, Spent, Invalidated}
 *   SettlementState::{Pending, Success, Failed}
 *
 * Contract:
 *   Observation click → GET .../networks/:network/payload-board (after observation active).
 *   Network-scoped board marks matching kind in scope; activation-wide returns all four kinds.
 *
 * OBSERVATION_REGISTRY is an in-memory control-plane status map only. It is not proof that
 * an Electrum listener is bound or that a SettlementRequest is carried. Without the
 * bit-minner feature, the stream cannot be started. The board reports these conditions
 * explicitly and does not synthesize AuthorizedPayload.
 *
 * Routes:
 *   GET /wallets/:name/activations/:activation/payload-board
 *   GET /wallets/:name/activations/:activation/networks/:network/payload-board
 */

#[derive(Serialize, Clone)]
struct AuthorizationView {
    /// Hex commitment when a real Authorization is available.
    commitment: Option<String>,
    activation: Option<u64>,
    /// Opaque locked structure is not expanded here.
    locked_present: bool,
}

#[derive(Serialize, Clone)]
struct SettlementRequestView {
    /// Present only when the runtime has supplied a SettlementRequest.
    carried: bool,
    authorization: Option<AuthorizationView>,
    /// WirePacket is opaque binary; only report presence.
    packet_present: bool,
}

#[derive(Serialize, Clone)]
struct AuthorizedPayloadView {
    /// Present only when runtime-carried; never a synthesized placeholder.
    carried: bool,
    request: Option<SettlementRequestView>,
    /// Unauthorized | Authorized | Spent | Invalidated when known.
    authorization_state: Option<String>,
    /// Pending | Success | Failed when known.
    settlement_state: Option<String>,
}

#[derive(Serialize, Clone)]
struct PayloadKindEntry {
    /// bitcoin | ethereum | solana | tron  (PayloadKind discriminant)
    kind: String,
    network: Option<NetworkOption>,
    /// Kind is in scope for this board request (network filter / activation-wide).
    in_scope: bool,
    /// Registered network exists for this kind.
    available: bool,
    /// Runtime-carried AuthorizedPayload only; None if not populated by the pipeline.
    authorized_payload: Option<AuthorizedPayloadView>,
    /// Depth from persisted event records matching this activation + network.
    depth_objects: Vec<DepthObjectResponse>,
    depth_path: Option<String>,
    depth_record: Option<String>,
}

#[derive(Serialize)]
struct ObservationControlPlane {
    /// True if OBSERVATION_REGISTRY has a running entry for this context.
    registry_running: bool,
    /// Commitment-derived candidate port (not proof a socket is listening).
    listen_port_candidate: Option<u16>,
    path_hint: Option<String>,
    /// Whether this binary was built with the bit-minner feature.
    bit_minner_feature: bool,
    /// True only when a live Electrum-compatible listener is known bound (not yet).
    electrum_listener_bound: bool,
    /// True only when a SettlementRequest was supplied by the runtime for this context.
    settlement_payload_carried: bool,
}

#[derive(Serialize)]
struct PayloadBoardResponse {
    substrate: String,
    activation: u64,
    network: Option<String>,
    observation: ObservationControlPlane,
    commitment: Option<String>,
    /// One entry per PayloadKind — simultaneous view; API remains authoritative.
    kinds: Vec<PayloadKindEntry>,
    runtime: RuntimeResponse,
    message: String,
}

fn payload_kind_id_for_network(network_id: &str) -> Option<&'static str> {
    let lower = network_id.trim().to_ascii_lowercase();
    if lower.contains("bitcoin") || lower == "btc" {
        Some("bitcoin")
    } else if lower.contains("ethereum") || lower == "eth" {
        Some("ethereum")
    } else if lower.contains("solana") || lower == "sol" {
        Some("solana")
    } else if lower.contains("tron") || lower == "trx" {
        Some("tron")
    } else {
        None
    }
}

fn record_matches_activation(record_name: &str, activation: u64) -> bool {
    // event_{activation}_{dimension} or exile_{activation}_{dimension}
    let rest = record_name
        .strip_prefix("event_")
        .or_else(|| record_name.strip_prefix("exile_"));
    let Some(rest) = rest else {
        return false;
    };
    let Some((act_str, _)) = rest.split_once('_') else {
        return false;
    };
    act_str.parse::<u64>().ok() == Some(activation)
}

/// Collect persisted depth only from event/exile records for this activation + network.
fn collect_depth_for_activation_network(
    substrate_path: &FsPath,
    activation: u64,
    network_id: &str,
) -> (Option<String>, Option<String>, Vec<DepthObjectResponse>) {
    let events_path = substrate_path.join("events");
    if !events_path.is_dir() {
        return (None, None, Vec::new());
    }

    let Ok(entries) = std::fs::read_dir(&events_path) else {
        return (None, None, Vec::new());
    };

    let mut best: Option<(SystemTime, String, PathBuf)> = None;

    for entry in entries.flatten() {
        let record_path = entry.path();
        if !record_path.is_dir() {
            continue;
        }
        let record_name = entry.file_name().to_string_lossy().to_string();
        if !record_matches_activation(&record_name, activation) {
            continue;
        }

        let payload = record_path.join(network_id).join("payload");
        if !(payload.join("depth.path").is_file() && payload.join("depth.objects").is_file()) {
            continue;
        }

        let modified = entry
            .metadata()
            .and_then(|m| m.modified())
            .unwrap_or(UNIX_EPOCH);

        match &best {
            Some((t, _, _)) if *t >= modified => {}
            _ => best = Some((modified, record_name, payload)),
        }
    }

    let Some((_, record_name, payload_path)) = best else {
        return (None, None, Vec::new());
    };

    match read_persisted_depth(&payload_path) {
        Ok(depth) => (Some(record_name), Some(depth.path), depth.objects),
        Err(_) => (Some(record_name), None, Vec::new()),
    }
}

fn bit_minner_enabled() -> bool {
    cfg!(feature = "bit-miner")
}

fn build_payload_board(
    name: &str,
    activation: u64,
    network_filter: Option<&str>,
) -> ApiResult<PayloadBoardResponse> {
    let substrate_path = ensure_existing_substrate(name)?;
    let commitment = read_trim(&substrate_path.join("qtm.commitment"));

    // Control-plane observation status only — not settlement proof.
    let (registry_running, listen_port_candidate, path_hint) = {
        let registry = observation_registry()
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner());

        if let Some(net) = network_filter {
            let key = observation_key(name, activation, net);
            match registry.get(&key).filter(|l| l.status.running) {
                Some(live) => (
                    true,
                    Some(live.status.listen_port),
                    live.status.path.clone(),
                ),
                None => (false, None, None),
            }
        } else {
            match registry.iter().find(|(k, live)| {
                k.0 == name && k.1 == activation && live.status.running
            }) {
                Some((_, live)) => (
                    true,
                    Some(live.status.listen_port),
                    live.status.path.clone(),
                ),
                None => (false, None, None),
            }
        }
    };

    // AuthorizedPayload is not synthesized. Until the observation pipeline
    // (bit_minner) exposes a real SettlementRequest for this context, carried=false.
    let settlement_payload_carried = false;
    let electrum_listener_bound = false;

    let registered = registered_networks().networks;
    let kind_order = ["bitcoin", "ethereum", "solana", "tron"];

    let mut kinds = Vec::new();

    for kind in kind_order {
        let matching: Vec<&NetworkOption> = registered
            .iter()
            .filter(|n| payload_kind_id_for_network(&n.id) == Some(kind))
            .collect();

        let in_scope = match network_filter {
            Some(net) => payload_kind_id_for_network(net) == Some(kind),
            None => true,
        };

        let primary = matching.first().copied();
        let network_id_for_depth = match network_filter {
            Some(net) if in_scope => Some(net.to_string()),
            _ => primary.map(|n| n.id.clone()).filter(|_| in_scope),
        };

        let (depth_record, depth_path, depth_objects) = match &network_id_for_depth {
            Some(id) => {
                collect_depth_for_activation_network(&substrate_path, activation, id)
            }
            None => (None, None, Vec::new()),
        };

        // Do not invent AuthorizedPayload. Report structure only when carried.
        let authorized_payload = if in_scope {
            Some(AuthorizedPayloadView {
                carried: settlement_payload_carried,
                request: None,
                authorization_state: None,
                settlement_state: None,
            })
        } else {
            None
        };

        kinds.push(PayloadKindEntry {
            kind: kind.to_string(),
            network: primary.cloned(),
            in_scope,
            available: primary.is_some(),
            authorized_payload,
            depth_objects,
            depth_path,
            depth_record,
        });
    }

    let observation = ObservationControlPlane {
        registry_running,
        listen_port_candidate,
        path_hint,
        bit_minner_feature: bit_minner_enabled(),
        electrum_listener_bound,
        settlement_payload_carried,
    };

    let message = if registry_running && !settlement_payload_carried {
        format!(
            "payload board open; observation registry active for substrate={name} activation={activation}, but no SettlementRequest is carried yet"
        )
    } else if registry_running {
        format!(
            "payload board open via observation substrate={name} activation={activation}"
        )
    } else {
        format!(
            "payload board for substrate={name} activation={activation} (observation registry not running)"
        )
    };

    Ok(PayloadBoardResponse {
        substrate: name.to_string(),
        activation,
        network: network_filter.map(str::to_string),
        observation,
        commitment,
        kinds,
        runtime: serialize_runtime(SystemTime::now()),
        message,
    })
}

async fn get_payload_board(
    Path((name, activation)): Path<(String, u64)>,
) -> ApiResult<Json<PayloadBoardResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());
    Ok(Json(build_payload_board(&name, activation, None)?))
}

async fn get_payload_board_for_network(
    Path((name, activation, network)): Path<(String, u64, String)>,
) -> ApiResult<Json<PayloadBoardResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|poisoned| poisoned.into_inner());

    if !valid_observation_network_id(&network) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid network identifier for payload board",
        ));
    }

    Ok(Json(build_payload_board(
        &name,
        activation,
        Some(network.as_str()),
    )?))
}

// qp_hd::commands::substrate_root() reads only HOME. On Windows, derive it
// from USERPROFILE before any thread starts.
fn resolve_home() {
    if std::env::var_os("HOME").is_some() {
        return;
    }
    match std::env::var_os("USERPROFILE") {
        // Single-threaded here; `unsafe` is required on edition 2024 only.
        #[allow(unused_unsafe)]
        Some(profile) => unsafe { std::env::set_var("HOME", profile) },
        None => {
            eprintln!("neither HOME nor USERPROFILE is set; qp_hd cannot resolve ~/.qp/substrates");
            std::process::exit(1);
        }
    }
}

fn main() {
    resolve_home();
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime")
        .block_on(serve());
}

async fn serve() {
    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([
            Method::GET,
            Method::HEAD,
            Method::POST,
            Method::PUT,
            Method::DELETE,
            Method::OPTIONS,
        ])
        .allow_headers(tower_http::cors::Any);

    let app = Router::new()
        .route("/health", get(health).options(preflight))
        .route("/networks", get(list_networks).options(preflight))
        .route(
            "/wallets",
            get(list_wallets)
                .post(create_wallet)
                .head(head_wallets)
                .options(preflight),
        )
        .route(
            "/wallets/",
            get(list_wallets)
                .post(create_wallet)
                .head(head_wallets)
                .options(preflight),
        )
        .route("/wallets/:name", get(open_wallet).options(preflight))
        .route("/wallets/:name/events", get(list_events).options(preflight))
        .route(
            "/wallets/:name/activations/:activation/budgets",
            get(list_activation_budgets).options(preflight),
        )
        .route(
            "/wallets/:name/activations/:activation/budgets/:network",
            put(update_activation_network_budget).options(preflight),
        )
        .route(
            "/wallets/:name/events/:record/payloads",
            get(list_event_payloads).options(preflight),
        )
        .route(
            "/wallets/:name/transit",
            post(transit_wallet).options(preflight),
        )
        .route(
            "/wallets/:name/receive",
            post(receive_wallet).options(preflight),
        )
        .route(
            "/wallets/:name/exile/depth",
            get(exile_depth).options(preflight),
        )
        .route(
            "/wallets/:name/exile",
            post(exile_wallet).options(preflight),
        )
        .route(
            "/wallets/:name/activations/:activation/networks/:network/observation",
            get(get_bitcoin_observation)
                .post(start_bitcoin_observation)
                .delete(stop_bitcoin_observation)
                .options(preflight),
        )
        .route(
            "/wallets/:name/activations/:activation/observations",
            get(list_bitcoin_observations).options(preflight),
        )
        // Payload board — exclusive entry into shared payload state (payload.rs).
        // Opened from Observation; not a second persistence layer.
        .route(
            "/wallets/:name/activations/:activation/payload-board",
            get(get_payload_board).options(preflight),
        )
        .route(
            "/wallets/:name/activations/:activation/networks/:network/wirepackets",
            get(get_wirepackets).options(preflight),
        )
        .route(
            "/wallets/:name/activations/:activation/networks/:network/payload-board",
            get(get_payload_board_for_network).options(preflight),
        )
        .layer(cors);

    let root = commands::substrate_root();
    println!("substrate_root={}", root.display());

    if root.is_dir() {
        match std::fs::read_dir(&root) {
            Ok(entries) => {
                let mut discovered = entries
                    .filter_map(Result::ok)
                    .filter(|entry| entry.path().is_dir())
                    .map(|entry| entry.file_name().to_string_lossy().into_owned())
                    .filter(|name| valid_substrate_name(name))
                    .collect::<Vec<_>>();

                discovered.sort();

                println!("substrates_discovered={}", discovered.len());

                for substrate in discovered {
                    let commitment_path =
                        root.join(&substrate).join("qtm.commitment");

                    match read_trim(&commitment_path) {
                        Some(commitment) if !commitment.trim().is_empty() => {
                            match listen_port_from_commitment(&commitment, None) {
                                Ok(port) => println!(
                                    "substrate={substrate} commitment={commitment} \
                                     electrum_port_candidate={port}"
                                ),
                                Err(e) => eprintln!(
                                    "substrate={substrate} commitment_port_error={e}"
                                ),
                            }
                        }
                        _ => println!(
                            "substrate={substrate} commitment=unavailable"
                        ),
                    }
                }
            }
            Err(e) => {
                eprintln!(
                    "unable to read substrate root {}: {e}",
                    root.display()
                );
            }
        }
    } else {
        println!(
            "substrates_discovered=0 (root does not exist; startup did not create it)"
        );
    }

    let port = std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|port| *port != 0)
        .unwrap_or(8080);

    let address = format!("0.0.0.0:{port}");

    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .unwrap_or_else(|e| {
            panic!("failed to bind qtm-api HTTP listener at {address}: {e}");
        });

    let bound_address = listener
        .local_addr()
        .expect("failed to obtain qtm-api listener address");

    println!("qtm-api HTTP listener bound at {bound_address}");
    println!("qtm-api health endpoint: /health");
    println!("qtm-api networks endpoint: /networks");
    println!("qtm-api wallets endpoint: /wallets");

    axum::serve(listener, app)
        .await
        .expect("qtm-api HTTP server failed");
}
