use axum::{
    extract::Path,
    http::{HeaderValue, Method, StatusCode},
    routing::{get, post},
    Json, Router,
};
use qp_hd::commands;
use qp_hd::qp44::TotalMass;
use serde_json::{json, Value};
use tower_http::cors::CorsLayer;

// TEMPORARY MOCK API.
//
// This stands in for calling into the real qp_hd crate (commands::qtm_create,
// qtm_open, qtm_transit, qtm_exile) until the qtm-graph dependency is
// available and qp-hd can actually compile.
//
// Every response shape here matches the fields qp-hd's commands.rs already
// writes to disk / prints today, so the Next.js frontend won't need to
// change at all once these handlers are swapped for real logic.

fn read_trim(path: &std::path::Path) -> Option<String> {
    std::fs::read_to_string(path).ok().map(|s| s.trim().to_string())
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

fn latest_event_dir(name: &str) -> Option<std::path::PathBuf> {
    let events_root = commands::substrate_root().join(name).join("events");
    std::fs::read_dir(events_root).ok()?
        .filter_map(Result::ok)
        .max_by_key(|e| e.metadata().and_then(|m| m.modified()).ok())
        .map(|e| e.path())
}

fn event_json(path: &std::path::Path) -> Value {
    json!({
        "namespace": path.display().to_string(),
        "dimension": read_trim(&path.join("manifold.dimension")).unwrap_or_default(),
        "structural_value": read_trim(&path.join("manifold.structural_value")).unwrap_or_default(),
        "activations": read_trim(&path.join("manifold.activations")).unwrap_or_default(),
        "retained_mass": read_trim(&path.join("manifold.retained_mass")).unwrap_or_default(),
        "tau": read_trim(&path.join("transition.tau")).unwrap_or_default(),
        "delta": read_trim(&path.join("transition.delta")).unwrap_or_default(),
        "gross_work": read_trim(&path.join("transition.gross_work")).unwrap_or_default(),
        "net_work": read_trim(&path.join("transition.net_work")).unwrap_or_default(),
        "commitment": read_trim(&path.join("qtm.commitment")).unwrap_or_default(),
        "coordinate": read_trim(&path.join("qtm.coordinate")).unwrap_or_default(),
        "sigma": read_trim(&path.join("qtm.sigma")).unwrap_or_default(),
    })
}

async fn create_wallet(Json(body): Json<Value>) -> (StatusCode, Json<Value>) {
    let name = body.get("name").and_then(|v| v.as_str()).unwrap_or("unnamed").to_string();
    let indices = body.get("indices").and_then(|v| v.as_str()).map(|s| s.to_string());

    if let Some(raw) = &indices {
        let parts: Vec<&str> = raw.split(',').collect();
        if parts.len() != 12 || !parts.iter().all(|v| v.trim().parse::<u16>().is_ok()) {
            return (StatusCode::BAD_REQUEST, Json(json!({ "error": "indices must be exactly 12 comma-separated numbers" })));
        }
    }

    let entropy = body.get("entropy").and_then(|v| v.as_str()).filter(|s| !s.is_empty()).map(|s| s.to_string());
    let path = commands::substrate_root().join(&name);

    if path.exists() {
        return (StatusCode::CONFLICT, Json(json!({ "error": format!("substrate already exists: {}", name) })));
    }

    commands::qtm_create(name.clone(), indices, entropy);
    (StatusCode::OK, Json(substrate_json(&name)))
}

async fn open_wallet(Path(name): Path<String>) -> (StatusCode, Json<Value>) {
    let path = commands::substrate_root().join(&name);
    if !path.exists() {
        return (StatusCode::NOT_FOUND, Json(json!({ "error": format!("substrate does not exist: {}", name) })));
    }
    commands::qtm_open(name.clone());
    (StatusCode::OK, Json(substrate_json(&name)))
}

async fn transit_wallet(Path(name): Path<String>, Json(body): Json<Value>) -> (StatusCode, Json<Value>) {
    let path = commands::substrate_root().join(&name);
    if !path.exists() {
        return (StatusCode::NOT_FOUND, Json(json!({ "error": format!("substrate does not exist: {}", name) })));
    }

    let activation = body.get("activation").and_then(|v| v.as_u64()).unwrap_or(0);
    let purpose = body.get("purpose").and_then(|v| v.as_u64()).unwrap_or(0) as u128;
    let coin = body.get("coin").and_then(|v| v.as_u64()).unwrap_or(0) as u128;
    let account = body.get("account").and_then(|v| v.as_u64()).unwrap_or(0) as u128;
    let change = body.get("change").and_then(|v| v.as_u64()).unwrap_or(0) as u128;
    let external = body.get("external").and_then(|v| v.as_u64()).unwrap_or(0) as u128;

    let payload: u128 = TotalMass::new(purpose, coin, account, change, external).memorize();
    commands::qtm_transit(&name, payload, activation);

    match latest_event_dir(&name) {
        Some(dir) => (StatusCode::OK, Json(event_json(&dir))),
        None => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": "transit ran but no event was found on disk" }))),
    }
}

async fn exile_wallet(Path(name): Path<String>) -> (StatusCode, Json<Value>) {
    let path = commands::substrate_root().join(&name);
    if !path.exists() {
        return (StatusCode::NOT_FOUND, Json(json!({ "error": format!("substrate does not exist: {}", name) })));
    }

    commands::qtm_exile(&name);

    match latest_event_dir(&name) {
        Some(dir) => (StatusCode::OK, Json(event_json(&dir))),
        None => (StatusCode::INTERNAL_SERVER_ERROR, Json(json!({ "error": "exile ran but no event was found on disk" }))),
    }
}

#[tokio::main]
async fn main() {
    let cors = CorsLayer::new()
        .allow_origin("http://localhost:3000".parse::<HeaderValue>().unwrap())
        .allow_methods([Method::GET, Method::POST])
        .allow_headers(tower_http::cors::Any);

    let app = Router::new()
        .route("/wallets", post(create_wallet))
        .route("/wallets/:name", get(open_wallet))
        .route("/wallets/:name/transit", post(transit_wallet))
        .route("/wallets/:name/exile", post(exile_wallet))
        .layer(cors);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:8080")
        .await
        .unwrap();

    println!("qtm-api listening on http://localhost:8080");

    axum::serve(listener, app).await.unwrap();
}