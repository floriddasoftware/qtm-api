use axum::{
    extract::Path,
    http::{Method, StatusCode},
    routing::{get, post},
    Json, Router,
};
use qp_hd::commands;
use qp_hd::qp44::TotalMass;
use qtm_graph::graph::{Direction, Scale};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    path::{Path as FsPath, PathBuf},
    sync::{Arc, Mutex, OnceLock},
};
use tower_http::cors::CorsLayer;

const DOMAIN_SIZE: u16 = 2048;
const NUM_INDICES: usize = 12;
const MAX_SUBSTRATE_NAME_LENGTH: usize = 64;

type ApiResult<T> = Result<T, (StatusCode, Json<ApiError>)>;

#[derive(Serialize)]
struct ApiError {
    error: String,
}

#[derive(Deserialize)]
struct CreateRequest {
    name: String,
    indices: String,
    entropy: String,
}

#[derive(Deserialize)]
struct TransitRequest {
    activation: u64,
    purpose: u64,
    coin: u64,
    account: u64,
    change: u64,
    external: u64,
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
struct EventResponse {
    namespace: String,
    dimension: String,
    structural_value: String,
    activations: String,
    retained_mass: String,
    tau: String,
    delta: String,
    gross_work: String,
    net_work: String,
    commitment: String,
    coordinate: String,
    sigma: String,
}

static SUBSTRATE_REGISTRY: OnceLock<
    Mutex<HashMap<String, Arc<Mutex<()>>>>,
> = OnceLock::new();

fn substrate_lock(name: &str) -> Arc<Mutex<()>> {
    let registry = SUBSTRATE_REGISTRY.get_or_init(|| {
        Mutex::new(HashMap::new())
    });

    let mut registry = registry
        .lock()
        .unwrap_or_else(|error| error.into_inner());

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
            byte.is_ascii_alphanumeric()
                || byte == b'-'
                || byte == b'_'
        })
}

fn parse_indices(raw: &str) -> Result<[u16; NUM_INDICES], String> {
    let parsed = raw
        .split(',')
        .map(|part| {
            let value = part
                .trim()
                .parse::<u16>()
                .map_err(|_| {
                    "indices must contain exactly 12 integers from 0 to 2047"
                        .to_string()
                })?;

            if value >= DOMAIN_SIZE {
                return Err(
                    "each index must be in the range 0..2048".to_string(),
                );
            }

            Ok(value)
        })
        .collect::<Result<Vec<_>, _>>()?;

    if parsed.len() != NUM_INDICES {
        return Err(
            "indices must contain exactly 12 comma-separated integers"
                .to_string(),
        );
    }

    parsed.try_into().map_err(|_| {
        "indices must contain exactly 12 comma-separated integers"
            .to_string()
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut output = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        output.push(HEX[(byte >> 4) as usize] as char);
        output.push(HEX[(byte & 0x0f) as usize] as char);
    }

    output
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

fn event_response<E: commands::HeritageEvent>(
    name: &str,
    event: &E,
) -> (Direction, EventResponse) {
    let direction = Direction::from_heritage(
        event.heritage().to_owned(),
        Scale::Global,
    );

    let event_map = direction.event();
    let qtm = event.qtm();

    let response = EventResponse {
        namespace: commands::substrate_root()
            .join(name)
            .join("events")
            .display()
            .to_string(),
        dimension: event_map.dimension().to_string(),
        structural_value: event_map.structural_value().to_string(),
        activations: event_map.activations().to_string(),
        retained_mass: event_map.retained_mass().to_string(),
        tau: event_map.tau().to_string(),
        delta: event_map.delta().to_string(),
        gross_work: event_map.gross_work().to_string(),
        net_work: event_map.net_work().to_string(),
        commitment: hex_encode(&qtm.commitment),
        coordinate: hex_encode(&qtm.coordinate),
        sigma: qtm.sigma.to_string(),
    };

    (direction, response)
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

    Ok(path)
}

async fn health() -> &'static str {
    "ok"
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

    let indices = parse_indices(&body.indices)
        .map_err(|message| error(StatusCode::BAD_REQUEST, message))?;

    if body.entropy.trim().is_empty() {
        return Err(error(
            StatusCode::BAD_REQUEST,
            "non-empty entropy is required",
        ));
    }

    let lock = substrate_lock(name);
    let _guard = lock
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    let path = commands::substrate_root().join(name);

    if path.exists() {
        return Err(error(
            StatusCode::CONFLICT,
            format!("substrate already exists: {name}"),
        ));
    }

    let indices = indices
        .iter()
        .map(u16::to_string)
        .collect::<Vec<_>>()
        .join(",");

    commands::qtm_create(
        name.to_string(),
        Some(indices),
        Some(body.entropy),
    );

    let response = substrate_json(name)?;

    Ok((StatusCode::CREATED, Json(response)))
}

async fn open_wallet(
    Path(name): Path<String>,
) -> ApiResult<Json<SubstrateResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    ensure_existing_substrate(&name)?;

    commands::qtm_open(name.clone());

    Ok(Json(substrate_json(&name)?))
}

async fn transit_wallet(
    Path(name): Path<String>,
    Json(body): Json<TransitRequest>,
) -> ApiResult<Json<EventResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    ensure_existing_substrate(&name)?;

    let payload = TotalMass::new(
        body.purpose as u128,
        body.coin as u128,
        body.account as u128,
        body.change as u128,
        body.external as u128,
    )
    .memorize();

    let event = commands::qtm_transit(
        &name,
        payload,
        body.activation,
    );

    let (_, response) = event_response(&name, &event);

    Ok(Json(response))
}

async fn exile_wallet(
    Path(name): Path<String>,
) -> ApiResult<Json<EventResponse>> {
    let lock = substrate_lock(&name);
    let _guard = lock
        .lock()
        .unwrap_or_else(|error| error.into_inner());

    ensure_existing_substrate(&name)?;

    let event = commands::qtm_exile(&name);

    let (_, response) = event_response(&name, &event);

    Ok(Json(response))
}

#[tokio::main]
async fn main() {
    let cors = CorsLayer::new()
        .allow_origin(tower_http::cors::Any)
        .allow_methods([Method::GET, Method::POST])
        .allow_headers(tower_http::cors::Any);

    let app = Router::new()
        .route("/health", get(health))
        .route("/wallets", post(create_wallet))
        .route("/wallets/:name", get(open_wallet))
        .route("/wallets/:name/transit", post(transit_wallet))
        .route("/wallets/:name/exile", post(exile_wallet))
        .layer(cors);

    let port = std::env::var("PORT")
        .unwrap_or_else(|_| "8080".to_string());

    let address = format!("0.0.0.0:{port}");

    let listener = tokio::net::TcpListener::bind(&address)
        .await
        .expect("failed to bind qtm-api listener");

    println!("qtm-api listening on http://{address}");

    axum::serve(listener, app)
        .await
        .expect("qtm-api server failed");
}