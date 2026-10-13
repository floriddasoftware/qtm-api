use axum::{
extract::Path,
http::{Method, StatusCode},
routing::{get, post},
Json, Router,
};
use qp_hd::commands;
use qp_hd::qp44::TotalMass;
use serde_json::{json, Value};
use std::path::{Path as FsPath, PathBuf};
use std::time::SystemTime;
use tower_http::cors::CorsLayer;

const DOMAIN_SIZE: u16 = 2048;
const NUM_INDICES: usize = 12;
const MAX_SUBSTRATE_NAME_LENGTH: usize = 64;

fn read_trim(path: &FsPath) -> Option {
std::fs::read_to_string(path)
.ok()
.map(|s| s.trim().to_string())
}

fn valid_substrate_name(name: &str) -> bool {
!name.is_empty()
&& name.len() <= MAX_SUBSTRATE_NAME_LENGTH
&& name != “.”
&& name != “..”
&& name
.bytes()
.all(|b| b.is_ascii_alphanumeric() || b == b’-’ || b == b’_’)
}

fn parse_indices(raw: &str) -> Result<[u16; NUM_INDICES], String> {
let parsed = raw
.split(’,’)
.map(|part| {
let value = part
.trim()
.parse::()
.map_err(|_| “indices must contain exactly 12 integers from 0 to 2047”.to_string())?;

        if value >= DOMAIN_SIZE {
            return Err("each index must be in the range 0..2048".to_string());
        }
        Ok(value)
    })
    .collect::<Result<Vec<_>, _>>()?;
if parsed.len() != NUM_INDICES {
    return Err("indices must contain exactly 12 comma-separated integers".to_string());
}
parsed
    .try_into()
    .map_err(|_| "indices must contain exactly 12 comma-separated integers".to_string())

}

fn substrate_json(name: &str) -> Value {
let path = commands::substrate_root().join(name);

json!({
    "namespace": path.display().to_string(),
    "indices": read_trim(&path.join("perm.indices")).unwrap_or_default(),
    "entropy": read_trim(&path.join("perm.entropy")).unwrap_or_default(),
    "dimension": read_trim(&path.join("perm.dimension")).unwrap_or_default(),
    "commitment": read_trim(&path.join("qtm.commitment")).unwrap_or_default(),
    "coordinate": read_trim(&path.join("qtm.coordinate")).unwrap_or_default(),
    "sigma": read_trim(&path.join("qtm.sigma")).unwrap_or_default(),
    "network": read_trim(&path.join("qtm.network")).unwrap_or_default(),
})

}

fn activation_from_event_name(name: &str) -> Option {
let rest = name
.strip_prefix(“event_”)
.or_else(|| name.strip_prefix(“exile_”))?;

rest.split('_').next()?.parse::<u64>().ok()

}

fn latest_event_dir(name: &str) -> Option {
let events_root = commands::substrate_root().join(name).join(“events”);

std::fs::read_dir(events_root)
    .ok()?
    .filter_map(Result::ok)
    .filter_map(|entry| {
        let path = entry.path();
        if !path.is_dir() {
            return None;
        }
        let file_name = entry.file_name();
        let event_name = file_name.to_str()?;
        let activation = activation_from_event_name(event_name)?;
        let modified = entry
            .metadata()
            .and_then(|metadata| metadata.modified())
            .unwrap_or(SystemTime::UNIX_EPOCH);
        Some((activation, modified, path))
    })
    .max_by_key(|(activation, modified, _)| (*activation, *modified))
    .map(|(_, _, path)| path)

}

fn event_json(path: &FsPath) -> Value {
json!({
“namespace”: path.display().to_string(),
“dimension”: read_trim(&path.join(“manifold.dimension”)).unwrap_or_default(),
“structural_value”: read_trim(&path.join(“manifold.structural_value”)).unwrap_or_default(),
“activations”: read_trim(&path.join(“manifold.activations”)).unwrap_or_default(),
“retained_mass”: read_trim(&path.join(“manifold.retained_mass”)).unwrap_or_default(),
“tau”: read_trim(&path.join(“transition.tau”)).unwrap_or_default(),
“delta”: read_trim(&path.join(“transition.delta”)).unwrap_or_default(),
“gross_work”: read_trim(&path.join(“transition.gross_work”)).unwrap_or_default(),
“net_work”: read_trim(&path.join(“transition.net_work”)).unwrap_or_default(),
“commitment”: read_trim(&path.join(“qtm.commitment”)).unwrap_or_default(),
“coordinate”: read_trim(&path.join(“qtm.coordinate”)).unwrap_or_default(),
“sigma”: read_trim(&path.join(“qtm.sigma”)).unwrap_or_default(),
})
}

async fn create_wallet(Json(body): Json) -> (StatusCode, Json) {
let name = match body.get(“name”).and_then(Value::as_str) {
Some(name) if valid_substrate_name(name) => name.to_string(),
_ => {
return (
StatusCode::BAD_REQUEST,
Json(json!({
“error”: “name is required and must contain 1-64 ASCII letters, digits, hyphens, or underscores”
})),
);
}
};

let indices_raw = match body.get("indices").and_then(Value::as_str) {
    Some(indices) => indices,
    None => {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "indices are required as 12 comma-separated integers"
            })),
        );
    }
};
let indices = match parse_indices(indices_raw) {
    Ok(indices) => indices,
    Err(error) => {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": error })),
        );
    }
};
let entropy = match body
    .get("entropy")
    .and_then(Value::as_str)
    .filter(|entropy| !entropy.trim().is_empty())
{
    Some(entropy) => entropy.to_string(),
    None => {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({
                "error": "non-empty entropy is required"
            })),
        );
    }
};
let path = commands::substrate_root().join(&name);
if path.exists() {
    return (
        StatusCode::CONFLICT,
        Json(json!({
            "error": format!("substrate already exists: {}", name)
        })),
    );
}
let indices = indices
    .iter()
    .map(u16::to_string)
    .collect::<Vec<_>>()
    .join(",");
commands::qtm_create(
    name.clone(),
    Some(indices),
    Some(entropy),
);
if !path.join("perm.dimension").is_file()
    || !path.join("qtm.commitment").is_file()
{
    return (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "error": "substrate creation did not produce a valid PERM and QTM commitment"
        })),
    );
}
(
    StatusCode::CREATED,
    Json(substrate_json(&name)),
)

}

async fn open_wallet(Path(name): Path) -> (StatusCode, Json) {
if !valid_substrate_name(&name) {
return (
StatusCode::BAD_REQUEST,
Json(json!({ “error”: “invalid substrate name” })),
);
}

let path = commands::substrate_root().join(&name);
if !path.is_dir() {
    return (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": format!("substrate does not exist: {}", name)
        })),
    );
}
commands::qtm_open(name.clone());
(
    StatusCode::OK,
    Json(substrate_json(&name)),
)

}

async fn transit_wallet(
Path(name): Path,
Json(body): Json,
) -> (StatusCode, Json) {
if !valid_substrate_name(&name) {
return (
StatusCode::BAD_REQUEST,
Json(json!({ “error”: “invalid substrate name” })),
);
}

let path = commands::substrate_root().join(&name);
if !path.is_dir() {
    return (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": format!("substrate does not exist: {}", name)
        })),
    );
}
let activation = match body.get("activation").and_then(Value::as_u64) {
    Some(activation) => activation,
    None => {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "activation is required as an unsigned integer" })),
        );
    }
};
let purpose = match body.get("purpose").and_then(Value::as_u64) {
    Some(value) => value as u128,
    None => {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "purpose is required as an unsigned integer" })),
        );
    }
};
let coin = match body.get("coin").and_then(Value::as_u64) {
    Some(value) => value as u128,
    None => {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "coin is required as an unsigned integer" })),
        );
    }
};
let account = match body.get("account").and_then(Value::as_u64) {
    Some(value) => value as u128,
    None => {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "account is required as an unsigned integer" })),
        );
    }
};
let change = match body.get("change").and_then(Value::as_u64) {
    Some(value) => value as u128,
    None => {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "change is required as an unsigned integer" })),
        );
    }
};
let external = match body.get("external").and_then(Value::as_u64) {
    Some(value) => value as u128,
    None => {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({ "error": "external is required as an unsigned integer" })),
        );
    }
};
let payload = TotalMass::new(
    purpose,
    coin,
    account,
    change,
    external,
)
.memorize();
commands::qtm_transit(&name, payload, activation);
match latest_event_dir(&name) {
    Some(dir) => (
        StatusCode::OK,
        Json(event_json(&dir)),
    ),
    None => (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "error": "transit ran but no event was found on disk"
        })),
    ),
}

}

async fn exile_wallet(Path(name): Path) -> (StatusCode, Json) {
if !valid_substrate_name(&name) {
return (
StatusCode::BAD_REQUEST,
Json(json!({ “error”: “invalid substrate name” })),
);
}

let path = commands::substrate_root().join(&name);
if !path.is_dir() {
    return (
        StatusCode::NOT_FOUND,
        Json(json!({
            "error": format!("substrate does not exist: {}", name)
        })),
    );
}
commands::qtm_exile(&name);
match latest_event_dir(&name) {
    Some(dir) => (
        StatusCode::OK,
        Json(event_json(&dir)),
    ),
    None => (
        StatusCode::INTERNAL_SERVER_ERROR,
        Json(json!({
            "error": "exile ran but no event was found on disk"
        })),
    ),
}

}

#[tokio::main]
async fn main() {
let cors = CorsLayer::new()
.allow_origin(tower_http::cors::Any)
.allow_methods([Method::GET, Method::POST])
.allow_headers(tower_http::cors::Any);

let app = Router::new()
    .route("/health", get(|| async { "ok" }))
    .route("/wallets", post(create_wallet))
    .route("/wallets/:name", get(open_wallet))
    .route("/wallets/:name/transit", post(transit_wallet))
    .route("/wallets/:name/exile", post(exile_wallet))
    .layer(cors);
let port = std::env::var("PORT").unwrap_or_else(|_| "8080".to_string());
let address = format!("0.0.0.0:{}", port);
let listener = tokio::net::TcpListener::bind(&address)
    .await
    .expect("failed to bind qtm-api listener");
println!("qtm-api listening on http://{}", address);
axum::serve(listener, app)
    .await
    .expect("qtm-api server failed");

}