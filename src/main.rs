
use axum::{
    extract::Path,
    http::{Method, StatusCode},
    routing::{get, post},
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

const SUBSTRATE_FILES: [&str; 7] = [
    "perm.indices",
    "perm.entropy",
    "perm.dimension",
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

fn parse_bip39_language(value: &str) -> Result<BIP39Language, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "english" => Ok(BIP39Language::English),
        "chinese_simplified" | "chinese-simplified" => {
            Ok(BIP39Language::ChineseSimplified)
        }
        "chinese_traditional" | "chinese-traditional" => {
            Ok(BIP39Language::ChineseTraditional)
        }
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

#[derive(Serialize)]
struct DepthResponse {
    path: String,
    objects: Vec<DepthObjectResponse>,
}

#[derive(Serialize)]
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

fn error(
    status: StatusCode,
    message: impl Into<String>,
) -> (StatusCode, Json<ApiError>) {
    (
        status,
        Json(ApiError {
            error: message.into(),
        }),
    )
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
        && name.bytes().all(|byte| {
            byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_'
        })
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
        return Err(
            "indices must contain exactly 12 comma-separated integers".to_string(),
        );
    }

    parsed.try_into().map_err(|_| {
        "indices must contain exactly 12 comma-separated integers".to_string()
    })
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

fn list_substrates() -> ApiResult<SubstrateListResponse> {
    let root = commands::substrate_root();

    std::fs::create_dir_all(&root).map_err(|io_error| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unable to access substrate root: {io_error}"),
        )
    })?;

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

        if !path.join("perm.indices").is_file()
            || !path.join("perm.entropy").is_file()
            || !path.join("perm.dimension").is_file()
        {
            continue;
        }

        let dimension = required_file(&path.join("perm.dimension"))?;

        let activations = read_trim(&path.join("manifold.activations"))
            .unwrap_or_else(|| "0".to_string());

        substrates.push(SubstrateEntry {
            name,
            dimension,
            activations,
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
    let duration = runtime
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();

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

fn event_map_response(
    direction: Direction,
    depth: &Depth,
) -> EventMapResponse {
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

fn ensure_existing_substrate(name: &str) -> ApiResult<PathBuf> {
    if !valid_substrate_name(name) {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "invalid substrate name",
        ));
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
                format!(
                    "substrate state is incomplete: missing {}",
                    file.display()
                ),
            ));
        }
    }

    Ok(path)
}

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
                .ok_or_else(|| {
                    error(
                        StatusCode::BAD_REQUEST,
                        "non-empty entropy is required",
                    )
                })?
                .to_string();

            (
                parsed
                    .iter()
                    .map(u16::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
                entropy,
            )
        }

        "bip39" => {
            let phrase = body
                .phrase
                .as_deref()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    error(
                        StatusCode::BAD_REQUEST,
                        "a BIP39 phrase is required",
                    )
                })?;

            let language_name = body.language.as_deref().ok_or_else(|| {
                error(
                    StatusCode::BAD_REQUEST,
                    "select a BIP39 language in the UI",
                )
            })?;

            let language = parse_bip39_language(language_name)
                .map_err(|message| error(StatusCode::BAD_REQUEST, message))?;

            let indices = BIP39Handler::mnemonic_indices(phrase, language)
                .map_err(|e| {
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
                indices
                    .iter()
                    .map(u16::to_string)
                    .collect::<Vec<_>>()
                    .join(","),
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
    let _guard = lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    let root = commands::substrate_root();

    std::fs::create_dir_all(&root).map_err(|io_error| {
        error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("unable to access substrate root: {io_error}"),
        )
    })?;

    let path = root.join(name);

    if path.exists() {
        return Err(error(
            StatusCode::CONFLICT,
            format!("substrate already exists: {name}"),
        ));
    }

    commands::qtm_create(name.to_string(), Some(indices), Some(entropy));

    let response = substrate_json(name)?;

    Ok((StatusCode::CREATED, Json(response)))
}

async fn open_wallet(
    Path(name): Path<String>,
) -> ApiResult<Json<SubstrateResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    ensure_existing_substrate(&name)?;

    commands::qtm_open(name.clone());

    Ok(Json(substrate_json(&name)?))
}

async fn transit_wallet(
    Path(name): Path<String>,
    Json(body): Json<TransitRequest>,
) -> ApiResult<Json<DirectionResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    ensure_existing_substrate(&name)?;

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
                return Err(error(
                    StatusCode::BAD_REQUEST,
                    "path must not be empty",
                ));
            }

            Some(path.to_string())
        }
        None => preset.as_ref().map(|network| network.path.clone()),
    };

    let selection = TransitSelection {
        network: body.network.clone(),
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

    let event = commands::qtm_transit(
        &name,
        payload,
        body.activation,
    );

    let qtm = QtmResponse {
        commitment: hex::encode(event.qtm.commitment),
        coordinate: hex::encode(event.qtm.coordinate),
        sigma: event.qtm.sigma.to_string(),
        network: event.qtm.net_work.to_string(),
    };

    let direction = Direction::from_heritage(
        event.heritage,
        Scale::Local,
    );

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
    let _guard = lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

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

    let transaction = QuantumTransaction::send(
        &name,
        body.amount,
    );

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

    let event = transaction.receive(&name, flow);

    let direction = Direction::from_heritage(
        event.heritage,
        Scale::Local,
    );

    let event_response = event_map_response(
        direction,
        &event.depth,
    );

    Ok(Json(ReceiveResponse {
        frame: QuantumTransactionFrame {
            quantum_transaction: transaction_response,
            event: event_response,
        },
    }))
}

async fn exile_wallet(
    Path(name): Path<String>,
) -> ApiResult<Json<DirectionResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    ensure_existing_substrate(&name)?;

    let event = commands::qtm_exile(&name);

    let qtm_response = QtmResponse {
        commitment: hex::encode(event.qtm.commitment),
        coordinate: hex::encode(event.qtm.coordinate),
        sigma: event.qtm.sigma.to_string(),
        network: event.qtm.net_work.to_string(),
    };

    let direction = Direction::from_heritage(
        event.heritage,
        Scale::Local,
    );

    Ok(Json(direction_response(
        direction,
        None,
        qtm_response,
        &event.depth,
    )))
}

#[tokio::main]
async fn main() {
    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([
            Method::GET,
            Method::HEAD,
            Method::POST,
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
        .route(
            "/wallets/:name",
            get(open_wallet).options(preflight),
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
            "/wallets/:name/exile",
            post(exile_wallet).options(preflight),
        )
        .layer(cors);

    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let address = format!("0.0.0.0:{port}");

    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .expect("failed to bind qtm-api listener");

    println!("qtm-api listening on http://{address}");
    println!(
        "substrate_root={}",
        commands::substrate_root().display()
    );

    axum::serve(listener, app)
        .await
        .expect("qtm-api server failed");
}
