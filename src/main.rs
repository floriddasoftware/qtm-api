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
use std::{
    collections::HashMap,
    path::{Path as FsPath, PathBuf},
    sync::{Arc, Mutex, OnceLock},
    time::{SystemTime, UNIX_EPOCH},
};
use tower_http::cors::CorsLayer;

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

#[derive(Serialize)]
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

    let record_path =
        canonical_record_path(&substrate_path, "transit", activation, dimension)?;

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
        dimension,
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
        .route("/wallets/:name/transit", post(transit_wallet).options(preflight))
        .route("/wallets/:name/receive", post(receive_wallet).options(preflight))
        .route("/wallets/:name/exile/depth", get(exile_depth).options(preflight))
        .route("/wallets/:name/exile", post(exile_wallet).options(preflight))
        .layer(cors);

    // Miner-style startup discovery: use the canonical substrate root and
    // inspect existing directories directly. The API remains request-driven;
    // no parallel polling loop or secondary registry is introduced.
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
                    println!("substrate={substrate}");
                }
            }
            Err(e) => {
                eprintln!("unable to read substrate root {}: {e}", root.display());
            }
        }
    } else {
        println!("substrates_discovered=0 (root does not exist; startup did not create it)");
    }

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let address = format!("0.0.0.0:{port}");
    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .expect("failed to bind qtm-api listener");

    println!("qtm-api listening on http://{address}");
    axum::serve(listener, app)
        .await
        .expect("qtm-api server failed");
}
