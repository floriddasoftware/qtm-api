use axum::{
extract::Path,
http::{Method, StatusCode},
routing::{get, head, post},
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
time::{SystemTime, UNIX_EPOCH},
};
use tower_http::cors::CorsLayer;

const DOMAIN_SIZE: u16 = 2048;
const NUM_INDICES: usize = 12;
const MAX_SUBSTRATE_NAME_LENGTH: usize = 64;

type ApiResult = Result<T, (StatusCode, Json)>;

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
struct SubstrateListResponse {
substrates: Vec,
}

#[derive(Serialize)]
struct SubstrateEntry {
name: String,
dimension: String,
activations: String,
}

#[derive(Serialize)]
struct DirectionResponse {
event: EventMapResponse,
}

#[derive(Serialize)]
struct EventMapResponse {
activation: u64,
scale: String,
runtime: RuntimeResponse,
heritage: HeritageResponse,
}

#[derive(Serialize)]
struct RuntimeResponse {
unix_seconds: u64,
subsec_nanos: u32,
}

#[derive(Serialize)]
struct HeritageResponse {
state: StateResponse,
transition: TransitionResponse,
}

#[derive(Serialize)]
struct StateResponse {
dimension: String,
structural_value: String,
activations: String,
retained_mass: String,
}

#[derive(Serialize)]
struct TransitionResponse {
tau: String,
delta: String,
gross_work: String,
net_work: String,
mirror_bytes: String,
}

static SUBSTRATE_REGISTRY: OnceLock<
Mutex<HashMap<String, Arc<Mutex<()>>>>,

= OnceLock::new();

fn substrate_lock(name: &str) -> Arc<Mutex<()>> {
let registry =
SUBSTRATE_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));

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
message: impl Into,
) -> (StatusCode, Json) {
(
status,
Json(ApiError {
error: message.into(),
}),
)
}

fn read_trim(path: &FsPath) -> Option {
std::fs::read_to_string(path)
.ok()
.map(|value| value.trim().to_string())
}

fn required_file(path: &FsPath) -> ApiResult {
read_trim(path).ok_or_else(|| {
error(
StatusCode::INTERNAL_SERVER_ERROR,
format!(“unable to read substrate file: {}”, path.display()),
)
})
}

fn valid_substrate_name(name: &str) -> bool {
!name.is_empty()
&& name.len() <= MAX_SUBSTRATE_NAME_LENGTH
&& name != “.”
&& name != “..”
&& name.bytes().all(|byte| {
byte.is_ascii_alphanumeric() || byte == b’-’ || byte == b’_’
})
}

fn parse_indices(raw: &str) -> Result<[u16; NUM_INDICES], String> {
let parsed = raw
.split(’,’)
.map(|part| {
let value = part.trim().parse::().map_err(|_| {
“indices must contain exactly 12 integers from 0 to 2047”
.to_string()
})?;

        if value >= DOMAIN_SIZE {
            return Err("each index must be in the range 0..2048".to_string());
        }
        Ok(value)
    })
    .collect::<Result<Vec<u16>, String>>()?;
if parsed.len() != NUM_INDICES {
    return Err(
        "indices must contain exactly 12 comma-separated integers"
            .to_string(),
    );
}
parsed.try_into().map_err(|_| {
    "indices must contain exactly 12 comma-separated integers".to_string()
})

}

fn substrate_json(name: &str) -> ApiResult {
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

fn list_substrates() -> ApiResult {
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
    let name: String = match entry.file_name().into_string() {
        Ok(name) if valid_substrate_name(name.as_str()) => name,
        _ => continue,
    };
    if !path.join("perm.indices").is_file()
        || !path.join("perm.entropy").is_file()
        || !path.join("perm.dimension").is_file()
    {
        continue;
    }
    let dimension = required_file(&path.join("perm.dimension"))?;
    let activations =
        read_trim(&path.join("manifold.activations"))
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

fn scale_name(scale: Scale) -> &’static str {
match scale {
Scale::Minimal => “Minimal”,
Scale::Local => “Local”,
Scale::Recursive => “Recursive”,
Scale::Global => “Global”,
}
}

fn serialize_runtime(runtime: SystemTime) -> RuntimeResponse {
let duration = runtime.duration_since(UNIX_EPOCH).unwrap_or_default();

RuntimeResponse {
    unix_seconds: duration.as_secs(),
    subsec_nanos: duration.subsec_nanos(),
}

}

fn direction_response(direction: Direction) -> DirectionResponse {
let event = direction.into_event();

let activation = event.activation;
let scale = scale_name(event.scale).to_string();
let runtime = serialize_runtime(event.runtime);
let heritage = event.heritage;
DirectionResponse {
    event: EventMapResponse {
        activation,
        scale,
        runtime,
        heritage: HeritageResponse {
            state: StateResponse {
                dimension: heritage.state.dimension().to_string(),
                structural_value: heritage
                    .state
                    .structural_value()
                    .to_string(),
                activations: heritage.state.activations().to_string(),
                retained_mass: heritage.state.retained_mass().to_string(),
            },
            transition: TransitionResponse {
                tau: heritage.transition.tau.to_string(),
                delta: heritage.transition.delta.to_string(),
                gross_work: heritage.transition.gross_work.to_string(),
                net_work: heritage.transition.net_work.to_string(),
                mirror_bytes: heritage
                    .transition
                    .mirror_bytes
                    .iter()
                    .map(|byte| format!("{byte:02x}"))
                    .collect(),
            },
        },
    },
}

}

fn ensure_existing_substrate(name: &str) -> ApiResult {
if !valid_substrate_name(name) {
return Err(error(StatusCode::BAD_REQUEST, “invalid substrate name”));
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

async fn health() -> &’static str {
“ok”
}

async fn preflight() -> StatusCode {
StatusCode::NO_CONTENT
}

async fn list_wallets() -> ApiResult<Json> {
Ok(Json(list_substrates()?))
}

async fn head_wallets() -> ApiResult {
list_substrates()?;
Ok(StatusCode::OK)
}

async fn create_wallet(
Json(body): Json,
) -> ApiResult<(StatusCode, Json)> {
let name = body.name.trim();

if !valid_substrate_name(name) {
    return Err(error(
        StatusCode::BAD_REQUEST,
        "name must contain 1-64 ASCII letters, digits, hyphens, or underscores",
    ));
}
let indices =
    parse_indices(&body.indices).map_err(|message| {
        error(StatusCode::BAD_REQUEST, message)
    })?;
if body.entropy.trim().is_empty() {
    return Err(error(
        StatusCode::BAD_REQUEST,
        "non-empty entropy is required",
    ));
}
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
let indices = indices
    .iter()
    .map(u16::to_string)
    .collect::<Vec<_>>()
    .join(",");
commands::qtm_create(
    name.to_string(),
    Some(indices),
    Some(body.entropy.trim().to_string()),
);
let response = substrate_json(name)?;
Ok((StatusCode::CREATED, Json(response)))

}

async fn open_wallet(
Path(name): Path,
) -> ApiResult<Json> {
let lock = substrate_lock(&name);
let _guard = lock
.lock()
.unwrap_or_else(|poisoned| poisoned.into_inner());

ensure_existing_substrate(&name)?;
commands::qtm_open(name.clone());
Ok(Json(substrate_json(&name)?))

}

async fn transit_wallet(
Path(name): Path,
Json(body): Json,
) -> ApiResult<Json> {
let lock = substrate_lock(&name);
let _guard = lock
.lock()
.unwrap_or_else(|poisoned| poisoned.into_inner());

ensure_existing_substrate(&name)?;
let payload = TotalMass::new(
    body.purpose as u128,
    body.coin as u128,
    body.account as u128,
    body.change as u128,
    body.external as u128,
)
.memorize();
let event = commands::qtm_transit(&name, payload, body.activation);
let direction = Direction::from_heritage(event.heritage, Scale::Local);
Ok(Json(direction_response(direction)))

}

async fn exile_wallet(
Path(name): Path,
) -> ApiResult<Json> {
let lock = substrate_lock(&name);
let _guard = lock
.lock()
.unwrap_or_else(|poisoned| poisoned.into_inner());

ensure_existing_substrate(&name)?;
let event = commands::qtm_exile(&name);
let direction = Direction::from_heritage(event.heritage, Scale::Local);
Ok(Json(direction_response(direction)))

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
    .route(
        "/wallets",
        get(list_wallets)
            .post(create_wallet)
            .head(head(head_wallets))
            .options(preflight),
    )
    .route(
        "/wallets/",
        get(list_wallets)
            .post(create_wallet)
            .head(head(head_wallets))
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
        "/wallets/:name/exile",
        post(exile_wallet).options(preflight),
    )
    .layer(cors);
let port = std::env::var("PORT")
    .unwrap_or_else(|_| "8080".to_string());
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