mod payload;
use bytes::Bytes;
use http_body_util::{BodyExt, Full};
use hyper::{
body::Incoming,
header::{self, HeaderValue},
server::conn::http1,
service::service_fn,
Method, Request, Response, StatusCode,
};
use hyper_util::rt::TokioIo;
use payload::EventPayload;
use qp_hd::{
commands,
economic_gate::QuantumTransaction,
load_budget,
pathsregistry::{CoinType, PathsRegistry},
purpose::{BIP39Handler, BIP39Language},
qp44::{Flow, TotalMass},
};
use qtm_graph::graph::{frame::Depth, Scale};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::{
collections::HashMap,
convert::Infallible,
io::ErrorKind,
path::{Path as FsPath, PathBuf},
process::Stdio,
sync::{Arc, Mutex, OnceLock},
time::{SystemTime, UNIX_EPOCH},
};
use tokio::{
net::TcpListener,
process::{Child, Command},
sync::Mutex as AsyncMutex,
};

#[cfg(feature = "bit-miner")]
use bitcoin::{
    bip32::{DerivationPath, Fingerprint, Xpriv, Xpub},
    Address as BitcoinAddress, Amount, NetworkKind, OutPoint, PublicKey, ScriptBuf, TxOut, WPubkeyHash, Witness,
};

#[cfg(feature = "bit-miner")]
use bit_miner::{
    bit_block::{self, QtmMasterKeys},
    bitcoinpsbt::{PrevoutResolver, ResolvedPrevout},
    broadcast::WirePacket,
    observestream::decode_wirepackets,
};

const DOMAIN_SIZE: u16 = 2048;
const NUM_INDICES: usize = 12;
const MAX_SUBSTRATE_NAME_LENGTH: usize = 64;
const WIREPACKETS_MAX_BYTES: u64 = 32 * 1024 * 1024;
const BITMINER_LOG_MAX_BYTES: usize = 1024 * 1024;
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
type HttpResponse = Response<Full<Bytes>>;
type ApiResult<T> = Result<T, HttpResponse>;
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
#[derive(Serialize, Clone)]
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
    heritage: EventHeritageView,
    qtm: QtmFrameResponse,
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
    constant_delta: String,
}

#[derive(Serialize)]
struct OpenResponse {
    substrate: SubstrateResponse,
    event: Option<PayloadEventView>,
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
    ordinal: u64,
    network: String,
}

#[derive(Deserialize)]
struct SubmissionDerivationRequest {
    network: String,
}

#[derive(Serialize)]
struct SubmissionDerivationResponse {
    substrate: String,
    activation: u64,
    submission: u64,
    qtm_activation: u64,
    network: String,
    derivation_path: String,
    directory: String,
    scriptpubkey: String,
    xpriv: String,
    xpub: String,
    zpub: String,
    address: String,
}

#[derive(Deserialize)]
struct BudgetRequest {
min_events: u64,
max_events: u64,
}
#[derive(Serialize, Clone)]
struct EventBudget {
min_events: u64,
max_events: u64,
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
#[derive(Serialize, Clone)]
struct ObservationClipStatus {
substrate: String,
active: bool,
direction: Option<String>,
}
#[derive(Serialize, Clone)]
struct WirePacketValueEntry {
txid: String,
vout: u32,
value_sat: u64,
script_pubkey: String,
commitment: String,
}
#[derive(Serialize)]
struct WirePacketsResponse {
substrate: String,
activation: u64,
network: String,
scope: &'static str,
path: String,
exists: bool,
bytes: Option<u64>,
file_route: Option<String>,
packets: Vec<WirePacketEntry>,
}
#[derive(Serialize)]
struct WirePacketEntry {
submission: u64,
name: String,
path: String,
bytes: u64,
packet_count: usize,
packets: Vec<WirePacketValueEntry>,
}
#[derive(Serialize, Clone)]
struct AuthorizationView {
commitment: Option<String>,
activation: Option<u64>,
locked_present: bool,
}
#[derive(Serialize, Clone)]
struct SettlementRequestView {
carried: bool,
authorization: Option<AuthorizationView>,
packet_present: bool,
}
#[derive(Serialize, Clone)]
struct AuthorizedPayloadView {
carried: bool,
request: Option<SettlementRequestView>,
authorization_state: Option<String>,
settlement_state: Option<String>,
}
#[derive(Serialize, Clone)]
struct EventStateView {
dimension: String,
structural_value: String,
retained_mass: String,
activations: u64,
}
#[derive(Serialize, Clone)]
struct EventTransitionView {
tau: String,
delta: String,
gross_work: String,
net_work: String,
mirror_bytes: String,
}
#[derive(Serialize, Clone)]
struct EventHeritageView {
state: EventStateView,
transition: EventTransitionView,
}
#[derive(Serialize, Clone)]
struct PayloadEventView {
record: String,
activation: u64,
scale: String,
runtime: RuntimeResponse,
heritage: EventHeritageView,
depth: DepthResponse,
}
#[derive(Serialize, Clone)]
struct PayloadKindEntry {
kind: String,
network: Option<NetworkOption>,
in_scope: bool,
available: bool,
authorized_payload: Option<AuthorizedPayloadView>,
depth_objects: Vec<DepthObjectResponse>,
depth_path: Option<String>,
depth_record: Option<String>,
}
#[derive(Serialize)]
struct ObservationControlPlane {
process_running: bool,
pid: Option<u32>,
bit_minner_feature: bool,
settlement_payload_carried: bool,
}
#[derive(Serialize)]
struct PayloadBoardResponse {
substrate: String,
activation: u64,
network: Option<String>,
observation: ObservationControlPlane,
commitment: Option<String>,
event: Option<PayloadEventView>,
kinds: Vec<PayloadKindEntry>,
runtime: RuntimeResponse,
message: String,
}
#[derive(Serialize)]
struct BitMinerStartResponse {
substrate: String,
activation: u64,
network: String,
pid: Option<u32>,
already_running: bool,
message: String,
}
#[derive(Serialize)]
struct BitMinerStatusResponse {
substrate: String,
activation: u64,
network: String,
running: bool,
pid: Option<u32>,
path: Option<String>,
event_path: Option<String>,
log_path: String,
clips: Vec<ObservationClipStatus>,
runtime: RuntimeResponse,
#[serde(skip_serializing_if = "Option::is_none")]
error: Option<String>,
}
#[derive(Serialize)]
struct BitMinerStopResponse {
substrate: String,
activation: u64,
network: String,
was_running: bool,
stopped: bool,
pid: Option<u32>,
message: String,
}
#[derive(Serialize)]
struct BitMinerProcessEntry {
substrate: String,
activation: u64,
network: String,
running: bool,
pid: Option<u32>,
}
#[derive(Serialize)]
struct BitMinerProcessListResponse {
substrate: String,
activation: u64,
pipelines: Vec<BitMinerProcessEntry>,
}
/* ---------- HTTP / Hyper Response & Body Helpers ---------- */

fn attach_cors(res: &mut HttpResponse) {
    let headers = res.headers_mut();
    headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
    headers.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("GET, HEAD, POST, PUT, DELETE, OPTIONS"));
    headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, HeaderValue::from_static("Accept, Content-Type, Authorization"));
    headers.insert(header::ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("600"));
}

fn bytes_response(status: StatusCode, content_type: &'static str, body: impl Into<Bytes>) -> HttpResponse {
    let mut res = Response::new(Full::new(body.into()));
    *res.status_mut() = status;
    res.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    attach_cors(&mut res);
    res
}

fn empty_response(status: StatusCode) -> HttpResponse {
let mut res = Response::new(Full::new(Bytes::new()));
*res.status_mut() = status;
attach_cors(&mut res);
res
}
fn json_response<T: Serialize>(status: StatusCode, value: &T) -> HttpResponse {
match serde_json::to_vec(value) {
Ok(vec) => bytes_response(status, "application/json", vec),
Err(e) => error(StatusCode::INTERNAL_SERVER_ERROR, format!("serialization error: {e}")),
}
}
fn error(status: StatusCode, message: impl Into<String>) -> HttpResponse {
json_response(status, &ApiError { error: message.into() })
}
async fn parse_json_body<T: DeserializeOwned>(req: Request<Incoming>) -> ApiResult<T> {
let collected = req
.into_body()
.collect()
.await
.map_err(|e| error(StatusCode::BAD_REQUEST, format!("unable to read request body: {e}")))?;
serde_json::from_slice(&collected.to_bytes())
.map_err(|e| error(StatusCode::BAD_REQUEST, format!("invalid JSON payload: {e}")))
}
fn parse_u64_param(raw: &str, label: &str) -> ApiResult<u64> {
raw.parse::<u64>()
.map_err(|_| error(StatusCode::BAD_REQUEST, format!("invalid {label}: {raw}")))
}
// ---------- Synchronization & Core Substrate Helpers ----------

static SUBSTRATE_REGISTRY: OnceLock<Mutex<HashMap<String, Arc<Mutex<()>>>>> = OnceLock::new();

fn substrate_lock(name: &str) -> Arc<Mutex<()>> {
    let registry = SUBSTRATE_REGISTRY.get_or_init(|| Mutex::new(HashMap::new()));
    let mut registry = registry.lock().unwrap_or_else(|p| p.into_inner());
    Arc::clone(registry.entry(name.to_string()).or_insert_with(|| Arc::new(Mutex::new(()))))
}

fn guarded<T>(label: &str, operation: impl FnOnce() -> T) -> ApiResult<T> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(operation)).map_err(|payload| {
        let detail = payload
            .downcast_ref::<&str>()
            .map(|message| message.to_string())
            .or_else(|| payload.downcast_ref::<String>().cloned())
            .unwrap_or_else(|| "unknown failure".to_string());

        error(StatusCode::INTERNAL_SERVER_ERROR, format!("{label} failed in qp_hd: {detail}"))
    })
}

fn read_trim(path: &FsPath) -> Option<String> {
    std::fs::read_to_string(path).ok().map(|value| value.trim().to_string())
}

fn required_file(path: &FsPath) -> ApiResult<String> {
    read_trim(path).ok_or_else(|| {
        error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to read substrate file: {}", path.display()))
    })
}

fn valid_substrate_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= MAX_SUBSTRATE_NAME_LENGTH
        && name != "."
        && name != ".."
        && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn valid_observation_network_id(network: &str) -> bool {
    !network.is_empty()
        && network.len() <= 64
        && network != "."
        && network != ".."
        && network.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn is_bitcoin_network_id(network: &str) -> bool {
    let lower = network.trim().to_ascii_lowercase();
    matches!(lower.as_str(), "bitcoin-legacy" | "bitcoin-native-segwit" | "bitcoin" | "btc") || lower.contains("bitcoin")
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
            let value = part
                .trim()
                .parse::<u16>()
                .map_err(|_| "indices must contain exactly 12 integers from 0 to 2047".to_string())?;

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
return Err(error(StatusCode::NOT_FOUND, format!("substrate does not exist: {name}")));
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
let root = commands::substrate_root();
if !root.is_dir() {
return Ok(SubstrateListResponse { substrates: Vec::new() });
}
let entries = std::fs::read_dir(&root)
.map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to list substrates: {e}")))?;
let mut substrates = Vec::new();
for entry in entries {
let entry = entry.map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to read entry: {e}")))?;
let path = entry.path();
if !path.is_dir() {
continue;
}
let name = match entry.file_name().into_string() {
Ok(n) if valid_substrate_name(&n) => n,
_ => continue,
};
if !path.join("perm.indices").is_file()
|| !path.join("perm.entropy").is_file()
|| !path.join("perm.dimension").is_file()
{
continue;
}
substrates.push(SubstrateEntry {
name,
dimension: required_file(&path.join("perm.dimension"))?,
activations: read_trim(&path.join("activation")).unwrap_or_else(|| "0".to_string()),
});
}
substrates.sort_by(|a, b| a.name.cmp(&b.name));
Ok(SubstrateListResponse { substrates })
}
fn network_option(id: &str, name: &str, coin: CoinType, purpose: u64, path: &str) -> NetworkOption {
    NetworkOption {
        id: id.to_string(),
        name: name.to_string(),
        coin: coin.retained_mass() as u64,
        purpose,
        path: path.to_string(),
    }
}

fn registered_networks() -> NetworkListResponse {
    let mut networks = CoinType::all()
        .iter()
        .map(|coin| {
            let id = match coin {
                CoinType::Bitcoin => "bitcoin-legacy",
                CoinType::Ethereum => "ethereum",
                CoinType::Solana => "solana",
                CoinType::Tron => "tron",
            };

            network_option(id, coin.name(), *coin, 44, PathsRegistry::get_path(*coin))
        })
        .collect::<Vec<_>>();

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
registered_networks().networks.into_iter().find(|n| n.id == id)
}

#[cfg(feature = "bit-miner")]
type BitBlockResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;

// =============================================================================
// BITCOIN BITBLOCK DELEGATION
//
// qtm-api supplies QTM state and network selection.
// bit_block owns Bitcoin key derivation, scripts and addresses.
// =============================================================================

#[cfg(feature = "bit-miner")]
fn bitblock_registered_path(network: &str) -> ApiResult<String> {
    let selected = selected_network(network).ok_or_else(|| {
        error(StatusCode::BAD_REQUEST, format!("network is not registered: {network}"))
    })?;

    if !is_bitcoin_network_id(network) {
        return Err(error(StatusCode::BAD_REQUEST, format!("network is not a Bitcoin network: {network}")));
    }

    Ok(selected.path)
}

// ---------------------------- EXTENDED KEYS ----------------------------

#[cfg(feature = "bit-miner")]
fn bitblock_qtm_xpriv(coordinate: [u8; 32], commitment: [u8; 32], activation: u64, network: NetworkKind) -> BitBlockResult<Xpriv> {
    bit_block::qtm_xpriv(coordinate, commitment, activation, network)
}

#[cfg(feature = "bit-miner")]
fn bitblock_qtm_master_fingerprint(coordinate: [u8; 32], commitment: [u8; 32], activation: u64, network: NetworkKind) -> BitBlockResult<Fingerprint> {
    bit_block::qtm_master_fingerprint(coordinate, commitment, activation, network)
}

#[cfg(feature = "bit-miner")]
fn bitblock_qtm_master_xpub(coordinate: [u8; 32], commitment: [u8; 32], activation: u64, network: NetworkKind) -> BitBlockResult<Xpub> {
    bit_block::qtm_master_xpub(coordinate, commitment, activation, network)
}

#[cfg(feature = "bit-miner")]
fn bitblock_qtm_master_zpub(coordinate: [u8; 32], commitment: [u8; 32], activation: u64, network: NetworkKind) -> BitBlockResult<String> {
    bit_block::qtm_master_zpub(coordinate, commitment, activation, network)
}

#[cfg(feature = "bit-miner")]
fn bitblock_xpub_to_zpub(xpub: &Xpub) -> String {
    bit_block::xpub_to_zpub(xpub)
}

#[cfg(feature = "bit-miner")]
fn bitblock_qtm_master_keys(coordinate: [u8; 32], commitment: [u8; 32], activation: u64, network: NetworkKind) -> BitBlockResult<QtmMasterKeys> {
    bit_block::qtm_master_keys(coordinate, commitment, activation, network)
}

// ------------------------ PATH AND CHILD DERIVATION ------------------------

#[cfg(feature = "bit-miner")]
fn bitblock_bip32_path_depth(path: &DerivationPath) -> usize {
    bit_block::bip32_path_depth(path)
}

#[cfg(feature = "bit-miner")]
fn bitblock_qtm_path_depth(activation: u64, path: &DerivationPath) -> BitBlockResult<u8> {
    bit_block::qtm_path_depth(activation, path)
}

#[cfg(feature = "bit-miner")]
fn bitblock_derive_qtm_child(coordinate: [u8; 32], commitment: [u8; 32], activation: u64, path: &str, network: NetworkKind) -> BitBlockResult<[u8; 32]> {
    bit_block::derive_qtm_child(coordinate, commitment, activation, path, network)
}

// ------------------------------ PUBLIC KEY ------------------------------

#[cfg(feature = "bit-miner")]
fn bitblock_derive_public_key(private_key: [u8; 32]) -> BitBlockResult<(PublicKey, WPubkeyHash)> {
    bit_block::derive_public_key(private_key)
}

// ----------------------------- SCRIPT PUBKEY -----------------------------

#[cfg(feature = "bit-miner")]
fn bitblock_p2pkh_script(private_key: [u8; 32]) -> BitBlockResult<ScriptBuf> {
    bit_block::p2pkh_script(private_key)
}

#[cfg(feature = "bit-miner")]
fn bitblock_p2wpkh_script(private_key: [u8; 32]) -> BitBlockResult<ScriptBuf> {
    bit_block::p2wpkh_script(private_key)
}

#[cfg(feature = "bit-miner")]
fn bitblock_taproot_script(private_key: [u8; 32]) -> BitBlockResult<ScriptBuf> {
    bit_block::taproot_script(private_key)
}

#[cfg(feature = "bit-miner")]
fn bitblock_derive_script_pubkey(private_key: [u8; 32]) -> BitBlockResult<ScriptBuf> {
    bit_block::derive_script_pubkey(private_key)
}

// -------------------------------- WITNESS --------------------------------

#[cfg(feature = "bit-miner")]
fn bitblock_derive_p2wpkh_witness(private_key: [u8; 32]) -> BitBlockResult<Witness> {
    bit_block::derive_p2wpkh_witness(private_key)
}

// ------------------------------- ADDRESSES -------------------------------

#[cfg(feature = "bit-miner")]
fn bitblock_p2pkh_address(private_key: [u8; 32]) -> BitBlockResult<BitcoinAddress> {
    bit_block::p2pkh_address(private_key)
}

#[cfg(feature = "bit-miner")]
fn bitblock_p2wpkh_address(private_key: [u8; 32]) -> BitBlockResult<BitcoinAddress> {
    bit_block::p2wpkh_address(private_key)
}

#[cfg(feature = "bit-miner")]
fn bitblock_qtm_bip44_address(coordinate: [u8; 32], commitment: [u8; 32], activation: u64, path: &str) -> BitBlockResult<BitcoinAddress> {
    bit_block::qtm_bip44_address(coordinate, commitment, activation, path)
}

#[cfg(feature = "bit-miner")]
fn bitblock_qtm_bip84_address(coordinate: [u8; 32], commitment: [u8; 32], activation: u64, path: &str) -> BitBlockResult<BitcoinAddress> {
    bit_block::qtm_bip84_address(coordinate, commitment, activation, path)
}

// =============================================================================
// SUBMISSION-SCOPED DERIVATION
//
// Each ordinal is a submission identifier. Derived values are persisted under
// {substrate}/activations/{activation}/event/{submission}/ and never use the
// current substrate activation or the latest wallet state.
// =============================================================================

#[cfg(feature = "bit-miner")]
fn submission_directory(name: &str, activation: u64, submission: u64) -> PathBuf {
    commands::substrate_root()
        .join(name)
        .join("activations")
        .join(activation.to_string())
        .join("event")
        .join(submission.to_string())
}

#[cfg(feature = "bit-miner")]
async fn handle_derive_submission(
    name: String,
    activation: u64,
    submission: u64,
    req: Request<Incoming>,
) -> HttpResponse {
    let body: SubmissionDerivationRequest = match parse_json_body(req).await {
        Ok(body) => body,
        Err(e) => return e,
    };

    if submission == 0 {
        return error(
            StatusCode::BAD_REQUEST,
            "submission must be greater than zero",
        );
    }

    let network_id = body.network.trim();

    if !valid_observation_network_id(network_id) {
        return error(StatusCode::BAD_REQUEST, "invalid network identifier");
    }

    let network = match selected_network(network_id) {
        Some(network) if is_bitcoin_network_id(network_id) => network,
        Some(_) => {
            return error(
                StatusCode::BAD_REQUEST,
                "submission derivation currently requires a Bitcoin network",
            );
        }
        None => {
            return error(
                StatusCode::BAD_REQUEST,
                format!("network is not registered: {network_id}"),
            );
        }
    };

    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());

    if let Err(e) = ensure_existing_substrate(&name) {
        return e;
    }

    // The ordinal identifies the persisted QTM event.
    // Derivation does not depend on WirePackets persistence.
    let persisted = match guarded("submission QTM lookup", || {
        commands::load_event_at_activation(&name, submission)
    }) {
        Ok(event) => event,
        Err(e) => return e,
    };

    let (coordinate, commitment, qtm_activation) = match persisted {
        qp_hd::PersistedEvent::Transit(event) => (
            event.qtm.coordinate,
            event.qtm.commitment,
            event.heritage.state.activations(),
        ),
        qp_hd::PersistedEvent::Exile(event) => (
            event.qtm.coordinate,
            event.qtm.commitment,
            event.heritage.state.activations(),
        ),
    };

    // bit_block owns extended-key derivation.
    let keys = match bitblock_qtm_master_keys(
        coordinate,
        commitment,
        qtm_activation,
        NetworkKind::Main,
    ) {
        Ok(keys) => keys,
        Err(e) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("extended-key derivation failed: {e}"),
            );
        }
    };

    // Derive the concrete child using the registered network path.
    let private_key = match bitblock_derive_qtm_child(
        coordinate,
        commitment,
        qtm_activation,
        &network.path,
        NetworkKind::Main,
    ) {
        Ok(key) => key,
        Err(e) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("submission child derivation failed: {e}"),
            );
        }
    };

    // Select the locking script and address from the registered purpose.
    let script = match network.purpose {
        44 => bitblock_p2pkh_script(private_key),
        84 => bitblock_p2wpkh_script(private_key),
        purpose => {
            return error(
                StatusCode::BAD_REQUEST,
                format!("unsupported Bitcoin script purpose: {purpose}"),
            );
        }
    };

    let script = match script {
        Ok(script) => script,
        Err(e) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("scriptPubKey derivation failed: {e}"),
            );
        }
    };

    let address = match network.purpose {
        44 => bitblock_qtm_bip44_address(
            coordinate,
            commitment,
            qtm_activation,
            &network.path,
        ),
        84 => bitblock_qtm_bip84_address(
            coordinate,
            commitment,
            qtm_activation,
            &network.path,
        ),
        _ => unreachable!(),
    };

    let address = match address {
        Ok(address) => address.to_string(),
        Err(e) => {
            return error(
                StatusCode::UNPROCESSABLE_ENTITY,
                format!("address derivation failed: {e}"),
            );
        }
    };

    let scriptpubkey = hex::encode(script.as_bytes());

    // Persist under the requested activation and selected submission ordinal.
    let directory = submission_directory(&name, activation, submission)
    .join(network_id);

if let Err(e) = std::fs::create_dir_all(&directory) {
    return error(
        StatusCode::INTERNAL_SERVER_ERROR,
        format!(
            "unable to create submission directory {}: {e}",
            directory.display()
        ),
    );
}
    // A submission directory represents one selected network.
    // Never silently overwrite its derivation with another network.
    if let Some(existing_network) = read_trim(&network_file) {
        if existing_network != network_id {
            return error(
                StatusCode::CONFLICT,
                format!(
                    "submission already derived for network {}; requested {}",
                    existing_network,
                    network_id
                ),
            );
        }
    }

    if let Err(e) = std::fs::create_dir_all(&directory) {
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            format!(
                "unable to create submission directory {}: {e}",
                directory.display()
            ),
        );
    }

        let persisted_values = [
    ("network", network_id.to_string()),
    ("qtm.activation", qtm_activation.to_string()),
    ("derivation.path", network.path.clone()),
    ("fingerprint", keys.fingerprint),
    ("scriptpubkey", scriptpubkey),
    ("xpriv", keys.xpriv),
    ("xpub", keys.xpub),
    ("zpub", keys.zpub),
    ("address", address),
];

    for (filename, value) in persisted_values {
        if let Err(e) = write_atomic(&directory.join(filename), value.as_bytes()) {
            return e;
        }
    }

    json_response(
        StatusCode::CREATED,
        &SubmissionDerivationResponse {
            substrate: name,
            activation,
            submission,
            qtm_activation,
            network: network_id.to_string(),
            derivation_path: network.path,
            directory: directory.display().to_string(),
            scriptpubkey: "scriptpubkey".to_string(),
            xpriv: "xpriv".to_string(),
            xpub: "xpub".to_string(),
            zpub: "zpub".to_string(),
            address: "address".to_string(),
        },
    )
}

#[cfg(feature = "bit-miner")]
fn handle_get_submission_value(
    name: String,
    activation: u64,
    submission: u64,
    network: String,
    field: String,
) -> HttpResponse {
    const FIELDS: [&str; 6] = [
        "fingerprint",
        "scriptpubkey",
        "xpriv",
        "xpub",
        "zpub",
        "address",
    ];

    if !FIELDS.contains(&field.as_str()) {
        return error(
            StatusCode::BAD_REQUEST,
            format!("unsupported submission value: {field}"),
        );
    }

    if !valid_observation_network_id(&network) {
        return error(
            StatusCode::BAD_REQUEST,
            "invalid network identifier",
        );
    }

    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());

    if let Err(e) = ensure_existing_substrate(&name) {
        return e;
    }

    let base = submission_directory(&name, activation, submission);
    let network_directory = base.join(&network);

    let directory = if network_directory.join("network").is_file() {
        network_directory
    } else if base.join("network").is_file()
        && read_trim(&base.join("network")).as_deref()
            == Some(network.as_str())
    {
        base
    } else {
        return error(
            StatusCode::NOT_FOUND,
            format!(
                "submission derivation not found: activation={} submission={} network={}",
                activation, submission, network
            ),
        );
    };

    let value = match read_trim(&directory.join(&field)) {
        Some(value) if !value.is_empty() => value,
        _ => {
            return error(
                StatusCode::NOT_FOUND,
                format!(
                    "submission value has not been derived: {}",
                    field
                ),
            );
        }
    };

    let mut response = bytes_response(
        StatusCode::OK,
        "text/plain; charset=utf-8",
        value,
    );

    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-store, no-cache, must-revalidate"),
    );
    response.headers_mut().insert(
        header::PRAGMA,
        HeaderValue::from_static("no-cache"),
    );

    response
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
let d = runtime.duration_since(UNIX_EPOCH).unwrap_or_default();
RuntimeResponse { unix_seconds: d.as_secs(), subsec_nanos: d.subsec_nanos() }
}
fn qtm_frame(qtm: QtmResponse) -> QtmFrameResponse {
QtmFrameResponse { identifier: qtm.commitment.clone(), qtm }
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
if !valid_observation_network_id(network) {
return Err(error(StatusCode::BAD_REQUEST, "invalid network identifier for depth persistence"));
}
let payload_path = record_path.join(network).join("payload");
std::fs::create_dir_all(&payload_path).map_err(|e| {
error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to create depth directory {}: {e}", payload_path.display()))
})?;
let response = depth_response(depth);
let mut objects = String::new();
for obj in &response.objects {
objects.push_str(&format!(
"{}\t{}\t{}\t{}\n",
obj.position,
hex::encode(obj.identifier.as_bytes()),
obj.mass,
obj.from
));
}
write_atomic(&payload_path.join("depth.path"), response.path.as_bytes())?;
write_atomic(&payload_path.join("depth.objects"), objects.as_bytes())
}
fn write_atomic(path: &FsPath, contents: &[u8]) -> ApiResult<()> {
let parent = path
.parent()
.ok_or_else(|| error(StatusCode::INTERNAL_SERVER_ERROR, "persistence path has no parent directory"))?;
let file_name = path.file_name().and_then(|n| n.to_str()).unwrap_or("depth");
let temporary = parent.join(format!("{file_name}.tmp"));
std::fs::write(&temporary, contents)
.map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to write {}: {e}", temporary.display())))?;
std::fs::rename(&temporary, path)
.map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to persist {}: {e}", path.display())))
}
fn latest_record_path(substrate_path: &FsPath, prefix: &str) -> ApiResult<PathBuf> {
    let events_path = substrate_path.join("events");
    let entries = std::fs::read_dir(&events_path).map_err(|e| {
        error(StatusCode::NOT_FOUND, format!("event store is unavailable at {}: {e}", events_path.display()))
    })?;

    let mut records = Vec::new();

    for entry in entries {
        let entry = entry.map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to read entry: {e}")))?;

        let path = entry.path();
        let name = entry.file_name().to_string_lossy().to_string();

        if path.is_dir() && name.starts_with(prefix) {
            let modified = entry.metadata().and_then(|metadata| metadata.modified()).unwrap_or(UNIX_EPOCH);
            records.push((modified, path));
        }
    }

    records.sort_by(|a, b| b.0.cmp(&a.0));

    records
        .into_iter()
        .map(|(_, path)| path)
        .next()
        .ok_or_else(|| error(StatusCode::NOT_FOUND, format!("no {prefix} record exists in the event store")))
}
fn parse_record_identity(record: &str) -> Option<(u64, u128)> {
    let rest = record.strip_prefix("event_").or_else(|| record.strip_prefix("exile_"))?;
    let (activation, dimension) = rest.split_once('_')?;
    Some((activation.parse().ok()?, dimension.parse().ok()?))
}
fn canonical_record_path(substrate_path: &FsPath, kind: &str, activation: u64, dimension: u128) -> ApiResult<PathBuf> {
    let prefix = match kind {
        "transit" => "event",
        "exile" => "exile",
        _ => {
            return Err(error(StatusCode::BAD_REQUEST, "record kind must be transit or exile"));
        }
    };

    let path = substrate_path.join("events").join(format!("{prefix}_{activation}_{dimension}"));

    if !path.is_dir() {
        return Err(error(
            StatusCode::NOT_FOUND,
            format!("canonical {kind} record does not exist: {}", path.display()),
        ));
    }

    Ok(path)
}
fn valid_record_name(record: &str) -> bool {
    (record.starts_with("event_") || record.starts_with("exile_"))
        && record.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        && parse_record_identity(record).is_some()
}

fn read_persisted_depth(payload_path: &FsPath) -> ApiResult<DepthResponse> {
    let path_file = payload_path.join("depth.path");
    let objects_file = payload_path.join("depth.objects");

    let path = std::fs::read_to_string(&path_file).map_err(|e| {
        error(StatusCode::NOT_FOUND, format!("persisted depth path unavailable at {}: {e}", path_file.display()))
    })?;

    let contents = std::fs::read_to_string(&objects_file).map_err(|e| {
        error(StatusCode::NOT_FOUND, format!("persisted depth objects unavailable at {}: {e}", objects_file.display()))
    })?;

    let mut objects = Vec::new();

    for (idx, line) in contents.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }

        let fields: Vec<&str> = line.splitn(4, '\t').collect();
        if fields.len() != 4 {
            return Err(error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid depth object at {}:{}", objects_file.display(), idx + 1),
            ));
        }

        let position = fields[0].parse::<usize>().map_err(|_| {
            error(StatusCode::INTERNAL_SERVER_ERROR, format!("invalid depth position at line {}", idx + 1))
        })?;

        let identifier_bytes = hex::decode(fields[1]).map_err(|_| {
            error(StatusCode::INTERNAL_SERVER_ERROR, format!("invalid depth identifier at line {}", idx + 1))
        })?;

        let identifier = String::from_utf8(identifier_bytes).map_err(|_| {
            error(StatusCode::INTERNAL_SERVER_ERROR, format!("depth identifier not UTF-8 at line {}", idx + 1))
        })?;

        objects.push(DepthObjectResponse {
            position,
            identifier,
            mass: fields[2].to_string(),
            from: fields[3].to_string(),
        });
    }

    Ok(DepthResponse { path: path.trim().to_string(), objects })
}

fn event_map_response(payload: EventPayload, qtm: QtmResponse) -> EventMapResponse {
    let event = payload.direction.into_event();
    let heritage = event.heritage();
    let state = &heritage.state;
    let transition = &heritage.transition;
    EventMapResponse {
        scale: scale_name(event.scale).to_string(),
        runtime: serialize_runtime(event.runtime),
        heritage: EventHeritageView {
            state: EventStateView {
                dimension: state.dimension().to_string(),
                structural_value: state.structural_value().to_string(),
                retained_mass: state.retained_mass().to_string(),
                activations: state.activations(),
            },
            transition: EventTransitionView {
                tau: transition.tau.to_string(),
                delta: transition.delta.to_string(),
                gross_work: transition.gross_work.to_string(),
                net_work: transition.net_work.to_string(),
                mirror_bytes: hex::encode(&transition.mirror_bytes),
            },
        },
        qtm: qtm_frame(qtm),
        depth: depth_response(&payload.depth),
    }
}

fn direction_response(payload: EventPayload, selection: Option<TransitSelection>, qtm: QtmResponse) -> DirectionResponse {
    DirectionResponse {
        frame: DirectionFrameResponse {
            qtm: qtm_frame(qtm.clone()),
            event: event_map_response(payload, qtm),
        },
        selection,
    }
}
// ---------- Activation/Network Budget Persistence ---------- /
fn budget_directory(substrate_path: &FsPath, activation: u64, network: &str) -> PathBuf {
substrate_path.join("event").join(activation.to_string()).join(network)
}
fn validate_budget(body: &BudgetRequest) -> Result<(), String> {
if body.max_events == 0 {
return Err("max_events must be greater than zero".to_string());
}
if body.min_events > body.max_events {
return Err("min_events must be less than or equal to max_events".to_string());
}
Ok(())
}

fn parse_budget(contents: &str, path: &FsPath) -> ApiResult<EventBudget> {
    let mut min_events = None;
    let mut max_events = None;

    for line in contents.lines() {
        let Some((key, value)) = line.split_once('=') else {
            continue;
        };

        match key.trim() {
            "min_events" => {
                min_events = Some(value.trim().parse::<u64>().map_err(|_| {
                    error(StatusCode::INTERNAL_SERVER_ERROR, format!("invalid min_events in {}", path.display()))
                })?);
            }
            "max_events" => {
                max_events = Some(value.trim().parse::<u64>().map_err(|_| {
                    error(StatusCode::INTERNAL_SERVER_ERROR, format!("invalid max_events in {}", path.display()))
                })?);
            }
            _ => {}
        }
    }

    let budget = EventBudget {
        min_events: min_events.ok_or_else(|| {
            error(StatusCode::INTERNAL_SERVER_ERROR, format!("missing min_events in {}", path.display()))
        })?,
        max_events: max_events.ok_or_else(|| {
            error(StatusCode::INTERNAL_SERVER_ERROR, format!("missing max_events in {}", path.display()))
        })?,
    };

    if budget.max_events == 0 || budget.min_events > budget.max_events {
        return Err(error(StatusCode::INTERNAL_SERVER_ERROR, format!("invalid budget values in {}", path.display())));
    }

    Ok(budget)
}

fn load_network_budget(substrate_path: &FsPath, activation: u64, network: &str) -> ApiResult<Option<EventBudget>> {
let path = budget_directory(substrate_path, activation, network).join("budget");
if !path.exists() {
return Ok(None);
}
let contents = std::fs::read_to_string(&path)
.map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to read budget {}: {e}", path.display())))?;
Ok(Some(parse_budget(&contents, &path)?))
}
fn persist_network_budget(substrate_path: &FsPath, activation: u64, network: &str, body: &BudgetRequest) -> ApiResult<EventBudget> {
validate_budget(body).map_err(|msg| error(StatusCode::BAD_REQUEST, msg))?;
let directory = budget_directory(substrate_path, activation, network);
std::fs::create_dir_all(&directory)
.map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to create budget dir {}: {e}", directory.display())))?;
let path = directory.join("budget");
let temporary_path = directory.join("budget.tmp");
let contents = format!("min_events={}\nmax_events={}\n", body.min_events, body.max_events);
std::fs::write(&temporary_path, contents)
.map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to write {}: {e}", temporary_path.display())))?;
std::fs::rename(&temporary_path, &path)
.map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to persist {}: {e}", path.display())))?;
Ok(EventBudget { min_events: body.min_events, max_events: body.max_events })
}
// ---------- WirePackets & Bit-Miner Integration ----------

fn validate_wirepackets_scope(name: &str, network: &str) -> ApiResult<()> {
    if !valid_substrate_name(name) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid substrate name"));
    }
    if !valid_observation_network_id(network) {
        return Err(error(StatusCode::BAD_REQUEST, "invalid network identifier"));
    }
    if !is_bitcoin_network_id(network) {
        return Err(error(StatusCode::BAD_REQUEST, "wire packets are persisted for Bitcoin settlement only"));
    }
    Ok(())
}

#[cfg(feature = "bit-miner")]
fn wire_packet_value_entries(packets: &[WirePacket]) -> Vec<WirePacketValueEntry> {
    packets
        .iter()
        .map(|p| WirePacketValueEntry {
            txid: p.txid().to_string(),
            vout: p.vout(),
            value_sat: p.value,
            script_pubkey: hex::encode(p.script_pubkey.as_bytes()),
            commitment: hex::encode(p.commitment),
        })
        .collect()
}

#[cfg(feature = "bit-miner")]
struct WirePacketPrevoutResolver {
    substrate: String,
    activation: u64,
}

#[cfg(feature = "bit-miner")]
impl WirePacketPrevoutResolver {
    fn new(substrate: &str, activation: u64) -> Self {
        Self { substrate: substrate.to_owned(), activation }
    }

    fn resolve_packet(&self, requested: &OutPoint, submission: u64, commitment: [u8; 32]) -> Result<WirePacket, std::io::Error> {
        use std::io::{Error, ErrorKind};

        let path = commands::wirepackets_path(&self.substrate, self.activation, submission);

        let bytes = std::fs::read(&path).map_err(|e| {
            Error::new(
                e.kind(),
                format!(
                    "unable to load parent wirepackets: substrate={} activation={} submission={} outpoint={}:{}: {e}",
                    self.substrate, self.activation, submission, requested.txid, requested.vout,
                ),
            )
        })?;

        let packets = decode_wirepackets(&bytes).map_err(|e| Error::new(ErrorKind::InvalidData, e.to_string()))?;

        for packet in packets {
            if packet.outpoint != *requested {
                continue;
            }

            if packet.commitment != commitment {
                return Err(Error::new(
                    ErrorKind::InvalidData,
                    format!(
                        "parent commitment mismatch: substrate={} activation={} submission={} outpoint={}:{}",
                        self.substrate, self.activation, submission, requested.txid, requested.vout,
                    ),
                ));
            }

            return Ok(packet);
        }

        Err(Error::new(
            ErrorKind::NotFound,
            format!(
                "previous output not found: substrate={} activation={} submission={} outpoint={}:{}",
                self.substrate, self.activation, submission, requested.txid, requested.vout,
            ),
        ))
    }
}

#[cfg(feature = "bit-miner")]
impl PrevoutResolver for WirePacketPrevoutResolver {
    fn resolve(&self, outpoint: &OutPoint, submission: u64, commitment: [u8; 32]) -> Result<ResolvedPrevout, Box<dyn std::error::Error + Send + Sync>> {
        let packet = self.resolve_packet(outpoint, submission, commitment)?;

        Ok(ResolvedPrevout {
            outpoint: packet.outpoint,
            output: TxOut {
                value: Amount::from_sat(packet.value),
                script_pubkey: packet.script_pubkey,
            },
            submission,
            commitment: packet.commitment,
        })
    }
}

fn electrum_port() -> u16 {
    std::env::var("ELECTRUM_PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|port| *port != 0)
        .unwrap_or(50001)
}

fn route_electrum_method(method: &str, params: &Value) -> Result<Value, Value> {
    match method {
        "server.ping" => Ok(Value::Null),

        "qtm.wirepackets.get" => {
            let args = params.as_array().ok_or_else(|| {
                json!({"code": -32602, "message": "expected [substrate, activation, network]"})
            })?;

            if args.len() != 3 {
                return Err(json!({"code": -32602, "message": "expected [substrate, activation, network]"}));
            }

            let name = args[0].as_str().ok_or_else(|| json!({"code": -32602, "message": "invalid substrate"}))?;
            let activation = args[1].as_u64().ok_or_else(|| json!({"code": -32602, "message": "invalid activation"}))?;
            let network = args[2].as_str().ok_or_else(|| json!({"code": -32602, "message": "invalid network"}))?;

            let response = load_wirepackets(name, activation, network).map_err(|response| {
                json!({
                    "code": -32000,
                    "message": format!("WirePackets request failed: HTTP {}", response.status().as_u16())
                })
            })?;

            serde_json::to_value(response).map_err(|e| {
                json!({"code": -32603, "message": format!("unable to serialize WirePackets: {e}")})
            })
        }

        _ => Err(json!({"code": -32601, "message": format!("method not found: {method}")})),
    }
}

async fn handle_electrum_connection<S>(stream: S)
where
    S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin,
{
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let (reader, mut writer) = tokio::io::split(stream);
    let mut lines = BufReader::new(reader).lines();

    loop {
        let line = match lines.next_line().await {
            Ok(Some(line)) => line,
            Ok(None) => break,
            Err(e) => {
                eprintln!("electrum read error: {e}");
                break;
            }
        };

        if line.trim().is_empty() {
            continue;
        }

        let response = match serde_json::from_str::<Value>(&line) {
            Ok(request) => {
                let id = request.get("id").cloned().unwrap_or(Value::Null);
                let method = request.get("method").and_then(Value::as_str).unwrap_or("").to_owned();
                let params = request.get("params").cloned().unwrap_or_else(|| json!([]));

                let result = tokio::task::spawn_blocking(move || route_electrum_method(&method, &params)).await;

                match result {
                    Ok(Ok(value)) => json!({"jsonrpc": "2.0", "id": id, "result": value}),
                    Ok(Err(error)) => json!({"jsonrpc": "2.0", "id": id, "error": error}),
                    Err(e) => json!({
                        "jsonrpc": "2.0",
                        "id": id,
                        "error": {"code": -32603, "message": format!("Electrum request failed: {e}")},
                    }),
                }
            }
            Err(e) => json!({
                "jsonrpc": "2.0",
                "id": null,
                "error": {"code": -32700, "message": format!("parse error: {e}")},
            }),
        };

        let mut output = match serde_json::to_vec(&response) {
            Ok(bytes) => bytes,
            Err(e) => {
                eprintln!("electrum serialization error: {e}");
                continue;
            }
        };

        output.push(b'\n');

        if let Err(e) = writer.write_all(&output).await {
            eprintln!("electrum write error: {e}");
            break;
        }
    }
}

async fn serve_electrum(listener: TcpListener) -> std::io::Result<()> {
    println!("qtm-api Electrum TCP listener bound at {}", listener.local_addr()?);

    loop {
        let (stream, peer) = listener.accept().await?;

        tokio::spawn(async move {
            handle_electrum_connection(stream).await;
            eprintln!("electrum session closed peer={peer}");
        });
    }
}

fn substrate_active_activation(substrate_path: &FsPath) -> Option<u64> {
    read_trim(&substrate_path.join("activation")).and_then(|v| v.trim().parse::<u64>().ok())
}

fn active_clips(activation: u64) -> Vec<ObservationClipStatus> {
    let mut names: Vec<String> = std::fs::read_dir(commands::substrate_root())
        .into_iter()
        .flatten()
        .flatten()
        .filter(|e| e.path().is_dir() && substrate_active_activation(&e.path()) == Some(activation))
        .filter_map(|e| e.file_name().into_string().ok())
        .collect();

    names.sort();

    names
        .into_iter()
        .map(|substrate| ObservationClipStatus { substrate, active: true, direction: None })
        .collect()
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
fn load_payload_event(name: &str, activation: u64) -> ApiResult<Option<(String, EventPayload)>> {
let Some(record_path) = commands::event_path_at_activation(name, activation) else {
return Ok(None);
};
let record = record_path.file_name().and_then(|n| n.to_str()).unwrap_or_default().to_string();
let event = guarded("payload event load", || commands::load_event_at_activation(name, activation))?;
let (heritage, depth) = match event {
qp_hd::PersistedEvent::Transit(ev) => (ev.heritage, ev.depth),
qp_hd::PersistedEvent::Exile(ev) => (ev.heritage, ev.depth),
};
Ok(Some((record, EventPayload::from_heritage(heritage, depth, Scale::Local))))
}
fn payload_event_view(record: String, payload: &EventPayload) -> PayloadEventView {
let event = payload.event();
let heritage = event.heritage();
let state = &heritage.state;
let transition = &heritage.transition;
PayloadEventView {
record,
activation: event.activation(),
scale: scale_name(event.scale()).to_string(),
runtime: serialize_runtime(event.runtime()),
heritage: EventHeritageView {
state: EventStateView {
dimension: state.dimension().to_string(),
structural_value: state.structural_value().to_string(),
retained_mass: state.retained_mass().to_string(),
activations: state.activations(),
},
transition: EventTransitionView {
tau: transition.tau.to_string(),
delta: transition.delta.to_string(),
gross_work: transition.gross_work.to_string(),
net_work: transition.net_work.to_string(),
mirror_bytes: hex::encode(&transition.mirror_bytes),
},
},
depth: depth_response(payload.depth()),
}
}
type BitMinerKey = String;
static BIT_MINER_PROCESSES: OnceLock<AsyncMutex<HashMap<BitMinerKey, Child>>> = OnceLock::new();
fn bit_miner_processes() -> &'static AsyncMutex<HashMap<BitMinerKey, Child>> {
BIT_MINER_PROCESSES.get_or_init(|| AsyncMutex::new(HashMap::new()))
}
fn bitminer_key(substrate: &str, activation: u64, network: &str) -> BitMinerKey {
format!("{substrate}:{activation}:{network}")
}
fn bitminer_log_path(substrate: &str, activation: u64, network: &str) -> PathBuf {
std::env::temp_dir().join(format!("bit-miner-{substrate}-{activation}-{network}.log"))
}
fn bitminer_log_tail(path: &FsPath) -> Option<String> {
let text = std::fs::read_to_string(path).ok()?;
let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
if lines.is_empty() {
return None;
}
let tail = lines[lines.len().saturating_sub(12)..].join("\n");
Some(if tail.len() > 2000 {
String::from_utf8_lossy(&tail.as_bytes()[tail.len() - 2000..]).into_owned()
} else {
tail
})
}

fn live_bitminer(processes: &mut HashMap<BitMinerKey, Child>, key: &str) -> ApiResult<(bool, Option<u32>)> {
    let Some(child) = processes.get_mut(key) else {
        return Ok((false, None));
    };

    match child.try_wait() {
        Ok(None) => Ok((true, child.id())),
        Ok(Some(_)) => {
            processes.remove(key);
            Ok((false, None))
        }
        Err(e) => Err(error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to inspect bit-miner process: {e}"))),
    }
}

async fn bitminer_process_state(name: &str, activation: u64, network: &str) -> ApiResult<(bool, Option<u32>)> {
    let mut processes = bit_miner_processes().lock().await;
    live_bitminer(&mut processes, &bitminer_key(name, activation, network))
}

async fn bitminer_process_state_any(name: &str, activation: u64) -> ApiResult<(bool, Option<u32>)> {
    let prefix = format!("{name}:{activation}:");
    let mut processes = bit_miner_processes().lock().await;

    let mut keys: Vec<String> = processes.keys().filter(|key| key.starts_with(&prefix)).cloned().collect();

    keys.sort();

    let mut first_pid = None;

    for key in keys {
        let (running, pid) = live_bitminer(&mut processes, &key)?;
        if running {
            first_pid = first_pid.or(pid);
        }
    }

    Ok((first_pid.is_some(), first_pid))
}

async fn bitminer_process_list(name: &str, activation: u64) -> ApiResult<Vec<BitMinerProcessEntry>> {
    let prefix = format!("{name}:{activation}:");
    let mut processes = bit_miner_processes().lock().await;

    let mut keys: Vec<String> = processes.keys().filter(|key| key.starts_with(&prefix)).cloned().collect();

    keys.sort();

    let mut pipelines = Vec::new();

    for key in keys {
        let network = key.strip_prefix(&prefix).unwrap_or_default().to_string();

        let (running, pid) = live_bitminer(&mut processes, &key)?;

        if running {
            pipelines.push(BitMinerProcessEntry {
                substrate: name.to_owned(),
                activation,
                network,
                running,
                pid,
            });
        }
    }

    Ok(pipelines)
}

async fn receive_pipeline_network(name: &str, activation: u64) -> ApiResult<NetworkOption> {
    let pipelines = bitminer_process_list(name, activation).await?;

    let network_id = match pipelines.as_slice() {
        [pipeline] => pipeline.network.as_str(),
        [] => {
            return Err(error(
                StatusCode::CONFLICT,
                format!("no running observation pipeline: substrate={name} activation={activation}"),
            ));
        }
        _ => {
            let networks = pipelines.iter().map(|pipeline| pipeline.network.as_str()).collect::<Vec<_>>().join(", ");

            return Err(error(
                StatusCode::CONFLICT,
                format!("multiple observation pipelines are active; receive requires one pipeline: {networks}"),
            ));
        }
    };

    selected_network(network_id).ok_or_else(|| {
        error(StatusCode::BAD_REQUEST, format!("pipeline network is not registered: {network_id}"))
    })
}

fn validate_bitminer_scope(name: &str, network: &str) -> ApiResult<()> {
if !valid_substrate_name(name) {
return Err(error(StatusCode::BAD_REQUEST, "invalid substrate name"));
}
if !valid_observation_network_id(network) || !is_bitcoin_network_id(network) {
return Err(error(StatusCode::BAD_REQUEST, "invalid Bitcoin network identifier"));
}
Ok(())
}
fn bit_minner_enabled() -> bool {
cfg!(feature = "bit-miner")
}
fn has_persisted_wirepackets(name: &str, activation: u64, max_events: Option<u64>) -> ApiResult<bool> {
let event_root = commands::substrate_root()
.join(name)
.join("activations")
.join(activation.to_string())
.join("event");
let entries = match std::fs::read_dir(&event_root) {
Ok(entries) => entries,
Err(e) if e.kind() == ErrorKind::NotFound => return Ok(false),
Err(e) => return Err(error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to inspect WirePackets: {e}"))),
};
for entry in entries {
let entry = entry.map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to read entry: {e}")))?;
if !entry.file_type().map_err(|e| error(StatusCode::INTERNAL_SERVER_ERROR, format!("entry error: {e}"))).map(|t| t.is_dir())? {
continue;
}
let Some(submission) = entry.file_name().to_str().and_then(|v| v.parse::<u64>().ok()) else {
continue;
};
if max_events.is_some_and(|limit| submission >= limit) {
continue;
}
let path = commands::wirepackets_path(name, activation, submission);
match std::fs::metadata(&path) {
Ok(m) if m.is_file() => return Ok(true),
Ok(_) => {}
Err(e) if e.kind() == ErrorKind::NotFound => {}
Err(e) => return Err(error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to inspect WirePackets: {e}"))),
}
}
Ok(false)
}
fn build_payload_board(
name: &str,
activation: u64,
network_filter: Option<&str>,
miner: (bool, Option<u32>),
) -> ApiResult<PayloadBoardResponse> {
let substrate_path = ensure_existing_substrate(name)?;
let commitment = read_trim(&substrate_path.join("qtm.commitment"));
let (process_running, pid) = miner;
let wirepackets_persisted = has_persisted_wirepackets(name, activation, None)?;
let loaded_event = load_payload_event(name, activation)?;
let (event, depth_record, depth) = match loaded_event {
Some((record, payload)) => {
let view = payload_event_view(record.clone(), &payload);
let d = depth_response(payload.depth());
(Some(view), Some(record), Some(d))
}
None => (None, None, None),
};
let registered = registered_networks().networks;
let kind_order = ["bitcoin", "ethereum", "solana", "tron"];
let mut kinds = Vec::with_capacity(kind_order.len());
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
let (depth_path, depth_objects, record) = match (&depth, in_scope) {
(Some(d), true) => (Some(d.path.clone()), d.objects.clone(), depth_record.clone()),
_ => (None, Vec::new(), None),
};
kinds.push(PayloadKindEntry {
kind: kind.to_string(),
network: primary.cloned(),
in_scope,
available: primary.is_some(),
authorized_payload: None,
depth_objects,
depth_path,
depth_record: record,
});
}
let observation = ObservationControlPlane {
process_running,
pid,
bit_minner_feature: bit_minner_enabled(),
settlement_payload_carried: wirepackets_persisted,
};
let message = format!(
"substrate={name} activation={activation}: miner process {}; wire packets {}",
if process_running { "running" } else { "not running" },
if wirepackets_persisted { "persisted" } else { "not persisted" }
);
Ok(PayloadBoardResponse {
substrate: name.to_string(),
activation,
network: network_filter.map(str::to_string),
observation,
commitment,
event,
kinds,
runtime: serialize_runtime(SystemTime::now()),
message,
})
}
// ---------- Endpoint Handlers (Adapted for Hyper) ---------- /
fn handle_health() -> HttpResponse {
bytes_response(StatusCode::OK, "text/plain; charset=utf-8", "ok")
}
fn handle_preflight() -> HttpResponse {
empty_response(StatusCode::NO_CONTENT)
}
fn handle_list_networks() -> HttpResponse {
json_response(StatusCode::OK, &registered_networks())
}
fn handle_list_wallets() -> HttpResponse {
match list_substrates() {
Ok(res) => json_response(StatusCode::OK, &res),
Err(err) => err,
}
}
fn handle_head_wallets() -> HttpResponse {
match list_substrates() {
Ok(_) => empty_response(StatusCode::OK),
Err(err) => err,
}
}

async fn handle_create_wallet(req: Request<Incoming>) -> HttpResponse {
    let body: CreateRequest = match parse_json_body(req).await {
        Ok(body) => body,
        Err(e) => return e,
    };

    let name = body.name.trim();

    if !valid_substrate_name(name) {
        return error(StatusCode::BAD_REQUEST, "name must contain 1-64 ASCII letters, digits, hyphens, or underscores");
    }

    let (indices, entropy) = match body.seed_source.as_deref().unwrap_or("persistent").trim().to_ascii_lowercase().as_str() {
        "persistent" | "custom" => {
            let Some(raw_indices) = body.indices.as_deref() else {
                return error(StatusCode::BAD_REQUEST, "indices are required for persistent creation");
            };

            let parsed = match parse_indices(raw_indices) {
                Ok(indices) => indices,
                Err(message) => return error(StatusCode::BAD_REQUEST, message),
            };

            let Some(entropy) = body.entropy.as_deref().map(str::trim).filter(|value| !value.is_empty()) else {
                return error(StatusCode::BAD_REQUEST, "non-empty entropy is required");
            };

            (
                parsed.iter().map(u16::to_string).collect::<Vec<_>>().join(","),
                entropy.to_string(),
            )
        }
        "bip39" => {
            let Some(phrase) = body.phrase.as_deref().map(str::trim).filter(|value| !value.is_empty()) else {
                return error(StatusCode::BAD_REQUEST, "a BIP39 phrase is required");
            };

            let Some(lang_name) = body.language.as_deref() else {
                return error(StatusCode::BAD_REQUEST, "select a BIP39 language in the UI");
            };

            let language = match parse_bip39_language(lang_name) {
                Ok(language) => language,
                Err(message) => return error(StatusCode::BAD_REQUEST, message),
            };

            let indices = match BIP39Handler::mnemonic_indices(phrase, language) {
                Ok(indices) => indices,
                Err(e) => return error(StatusCode::BAD_REQUEST, format!("invalid BIP39 mnemonic: {e}")),
            };

            let entropy = match BIP39Handler::mnemonic_entropy(phrase, language, body.passphrase.as_deref()) {
                Ok(entropy) => entropy,
                Err(e) => return error(StatusCode::BAD_REQUEST, format!("unable to derive BIP39 entropy: {e}")),
            };

            (
                indices.iter().map(u16::to_string).collect::<Vec<_>>().join(","),
                hex::encode(entropy),
            )
        }
        _ => {
            return error(StatusCode::BAD_REQUEST, "seed_source must be persistent, custom, or bip39");
        }
    };

    let lock = substrate_lock(name);
    let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());

    let root = commands::substrate_root();

    if let Err(e) = std::fs::create_dir_all(&root) {
        return error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to access substrate root: {e}"));
    }

    if root.join(name).exists() {
        return error(StatusCode::CONFLICT, format!("substrate already exists: {name}"));
    }

    if let Err(e) = guarded("substrate creation", || {
        commands::qtm_create(name.to_string(), Some(indices), Some(entropy))
    }) {
        return e;
    }

    if let Err(e) = ensure_existing_substrate(name) {
        return e;
    }

    match substrate_json(name) {
        Ok(response) => json_response(StatusCode::CREATED, &response),
        Err(e) => e,
    }
}


fn handle_open_wallet(name: String, activation: u64) -> HttpResponse {
    if activation == 0 {
        return error(
            StatusCode::BAD_REQUEST,
            "open activation ordinal must be greater than zero",
        );
    }

    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());

    let _substrate_path = match ensure_existing_substrate(&name) {
        Ok(path) => path,
        Err(e) => return e,
    };

    // Resolve only the UI-selected ordinal. Do not substitute the
    // substrate's current activation.
    let record_path = if let Some(path) =
        commands::exile_path_at_activation(&name, activation)
    {
        path
    } else if let Some(path) =
        commands::event_path_at_activation(&name, activation)
    {
        path
    } else {
        return error(
            StatusCode::NOT_FOUND,
            format!(
                "no persisted transit or exile event at selected ordinal {activation}"
            ),
        );
    };

    let record = record_path
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or_default()
        .to_string();

    // Load the exact selected event. This supports both persisted
    // transit and exile records without executing another transition.
    let persisted = match guarded(
        "selected ordinal event load",
        || commands::load_event_at_activation(&name, activation),
    ) {
        Ok(event) => event,
        Err(e) => return e,
    };

    let (heritage, depth) = match persisted {
        qp_hd::PersistedEvent::Transit(event) => {
            (event.heritage, event.depth)
        }
        qp_hd::PersistedEvent::Exile(event) => {
            (event.heritage, event.depth)
        }
    };

    let payload = EventPayload::from_heritage(
        heritage,
        depth,
        Scale::Local,
    );

    let event = Some(payload_event_view(record, &payload));

    let substrate = match substrate_json(&name) {
        Ok(substrate) => substrate,
        Err(e) => return e,
    };

    json_response(
        StatusCode::OK,
        &OpenResponse {
            substrate,
            event,
        },
    )
}

fn handle_list_events(name: String) -> HttpResponse {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());

    let substrate_path = match ensure_existing_substrate(&name) {
        Ok(path) => path,
        Err(err) => return err,
    };

    let events_path = substrate_path.join("events");
    let mut events = Vec::new();

    if events_path.is_dir() {
        let entries = match std::fs::read_dir(&events_path) {
            Ok(entries) => entries,
            Err(err) => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("unable to list events: {err}"),
                );
            }
        };

        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(err) => {
                    return error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("unable to read event entry: {err}"),
                    );
                }
            };

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

            let read_persisted = |field: &str| -> ApiResult<String> {
                read_trim(&record_path.join(field)).ok_or_else(|| {
                    error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "missing persisted event field: record={} field={}",
                            record,
                            field,
                        ),
                    )
                })
            };

            let activation = match read_persisted("manifold.activations") {
                Ok(value) => value,
                Err(err) => return err,
            };

            let activation_number = match activation.parse::<u64>() {
                Ok(value) => value,
                Err(_) => {
                    return error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!(
                            "invalid persisted activation: record={} activation={}",
                            record,
                            activation,
                        ),
                    );
                }
            };

            let dimension = match read_persisted("manifold.dimension") {
                Ok(value) => value,
                Err(err) => return err,
            };

            let structural_value =
                match read_persisted("manifold.structural_value") {
                    Ok(value) => value,
                    Err(err) => return err,
                };

            let retained_mass =
                match read_persisted("manifold.retained_mass") {
                    Ok(value) => value,
                    Err(err) => return err,
                };

            let delta = match read_persisted("transition.delta") {
                Ok(value) => value,
                Err(err) => return err,
            };

            let tau = match read_persisted("transition.tau") {
                Ok(value) => value,
                Err(err) => return err,
            };

            let gross_work =
                match read_persisted("transition.gross_work") {
                    Ok(value) => value,
                    Err(err) => return err,
                };

            let net_work =
                match read_persisted("transition.net_work") {
                    Ok(value) => value,
                    Err(err) => return err,
                };

            let commitment = match read_persisted("qtm.commitment") {
                Ok(value) => value,
                Err(err) => return err,
            };

            let coordinate = match read_persisted("qtm.coordinate") {
                Ok(value) => value,
                Err(err) => return err,
            };

            let depth_contents = match read_persisted("depth.objects") {
                Ok(value) => value,
                Err(err) => return err,
            };

            let depth_objects = depth_contents
                .lines()
                .filter(|line| !line.trim().is_empty())
                .count();

            events.push((
                activation_number,
                PersistedEventSummary {
                    kind: kind.to_string(),
                    record,
                    activation,
                    dimension,
                    structural_value,
                    depth_objects,
                    retained_mass,
                    delta,
                    tau,
                    gross_work,
                    net_work,
                    commitment,
                    coordinate,
                },
            ));
        }
    }

    events.sort_by(|a, b| {
        a.0.cmp(&b.0)
            .then_with(|| a.1.record.cmp(&b.1.record))
    });

    let events = events
        .into_iter()
        .map(|(_, event)| event)
        .collect();

    json_response(
        StatusCode::OK,
        &EventStoreResponse {
            substrate: name,
            directory: substrate_path.display().to_string(),
            events_directory: events_path.display().to_string(),
            events,
            runtime: serialize_runtime(SystemTime::now()),
        },
    )
}


fn handle_list_event_payloads(name: String, record: String) -> HttpResponse {
let lock = substrate_lock(&name);
let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());
let substrate_path = match ensure_existing_substrate(&name) {
Ok(p) => p,
Err(e) => return e,
};
if !valid_record_name(&record) {
return error(StatusCode::BAD_REQUEST, "invalid event record identifier");
}
let record_path = substrate_path.join("events").join(&record);
if !record_path.is_dir() {
return error(StatusCode::NOT_FOUND, format!("event record does not exist: {record}"));
}
let direction = if record.starts_with("event") { "transit_inputs" } else { "exile_outputs" };
let entries = match std::fs::read_dir(&record_path) {
Ok(e) => e,
Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to read event record: {e}")),
};
let mut payloads = Vec::new();
for entry in entries {
let entry = match entry {
Ok(e) => e,
Err(e) => return error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to read payload entry: {e}")),
};
let network_path = entry.path();
if !network_path.is_dir() {
continue;
}
let network_id = entry.file_name().to_string_lossy().to_string();
let payload_path = network_path.join("payload");
if !payload_path.join("depth.path").is_file() || !payload_path.join("depth.objects").is_file() {
continue;
}
let depth = match read_persisted_depth(&payload_path) {
Ok(d) => d,
Err(e) => return e,
};
let selected = selected_network(&network_id);
let registered = selected.is_some();
let network_name = selected.map(|n| n.name).unwrap_or_else(|| network_id.clone());
payloads.push(PersistedPayloadEntry {
record: record.clone(),
direction: direction.to_string(),
network: PersistedPayloadNetwork { id: network_id, name: network_name, registered },
path: payload_path.display().to_string(),
depth_identifier: depth.path,
objects: depth.objects,
});
}
payloads.sort_by(|a, b| a.network.id.cmp(&b.network.id));
json_response(
StatusCode::OK,
&PersistedPayloadsResponse { substrate: name, record, directory: record_path.display().to_string(), payloads },
)
}
fn handle_list_activation_budgets(name: String, activation: u64) -> HttpResponse {
let lock = substrate_lock(&name);
let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());
let substrate_path = match ensure_existing_substrate(&name) {
Ok(p) => p,
Err(e) => return e,
};
let directory = substrate_path.join("event").join(activation.to_string());
let mut budgets = Vec::new();
for network in registered_networks().networks {
let budget = match load_network_budget(&substrate_path, activation, &network.id) {
Ok(b) => b,
Err(e) => return e,
};
budgets.push(NetworkBudgetEntry { configured: budget.is_some(), network, activation, budget });
}
json_response(
StatusCode::OK,
&ActivationBudgetsResponse { substrate: name, activation, directory: directory.display().to_string(), budgets },
)
}
async fn handle_update_activation_network_budget(
name: String,
activation: u64,
network_id: String,
req: Request<Incoming>,
) -> HttpResponse {
let body: BudgetRequest = match parse_json_body(req).await {
Ok(b) => b,
Err(e) => return e,
};
let lock = substrate_lock(&name);
let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());
let substrate_path = match ensure_existing_substrate(&name) {
Ok(p) => p,
Err(e) => return e,
};
let Some(network) = selected_network(&network_id) else {
return error(StatusCode::BAD_REQUEST, format!("unsupported network: {network_id}"));
};
let budget = match persist_network_budget(&substrate_path, activation, &network.id, &body) {
Ok(b) => b,
Err(e) => return e,
};
json_response(StatusCode::OK, &NetworkBudgetEntry { network, activation, configured: true, budget: Some(budget) })
}
/// POST /wallets/:name/transit
async fn handle_transit_wallet(name: String, req: Request<Incoming>) -> HttpResponse {
    let body: TransitRequest = match parse_json_body(req).await {
        Ok(body) => body,
        Err(e) => return e,
    };

    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());

    let substrate_path = match ensure_existing_substrate(&name) {
        Ok(path) => path,
        Err(e) => return e,
    };

    let preset = match body.network.as_deref() {
        Some(id) => match selected_network(id) {
            Some(network) => Some(network),
            None => {
                return error(StatusCode::BAD_REQUEST, format!("unsupported network: {id}"));
            }
        },
        None => None,
    };

    let purpose = body.purpose.or_else(|| preset.as_ref().map(|n| n.purpose)).unwrap_or(44);
    let coin = body.coin.or_else(|| preset.as_ref().map(|n| n.coin)).unwrap_or(0);
    let account = body.account.unwrap_or(0);
    let change = body.change.unwrap_or(0);
    let external = body.external.unwrap_or(0);

    let path = match body.path {
        Some(path) => {
            let trimmed = path.trim();
            if trimmed.is_empty() {
                return error(StatusCode::BAD_REQUEST, "path must not be empty");
            }
            Some(trimmed.to_string())
        }
        None => preset.as_ref().map(|n| n.path.clone()),
    };

    let network_id = body
        .network
        .as_deref()
        .or_else(|| preset.as_ref().map(|n| n.id.as_str()))
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

    let event = match guarded("transit", || commands::qtm_transit(&name, payload, body.activation)) {
        Ok(event) => event,
        Err(e) => return e,
    };

    let activation = event.heritage.state.activations();
    let dimension = event.heritage.state.dimension();

    let record_path = match canonical_record_path(&substrate_path, "transit", activation, u128::from(dimension)) {
        Ok(path) => path,
        Err(e) => return e,
    };

    if let Err(e) = write_depth_payload(&record_path, &network_id, &event.depth) {
        return e;
    }

    let qtm = QtmResponse {
        commitment: hex::encode(event.qtm.commitment),
        coordinate: hex::encode(event.qtm.coordinate),
        sigma: event.qtm.sigma.to_string(),
        network: event.qtm.net_work.to_string(),
    };

    let event_payload = EventPayload::from_heritage(event.heritage, event.depth, Scale::Local);

    json_response(StatusCode::OK, &direction_response(event_payload, Some(selection), qtm))
}

/// POST /wallets/:name/receive
async fn handle_receive_wallet(name: String, req: Request<Incoming>) -> HttpResponse {
    let body: ReceiveRequest = match parse_json_body(req).await {
        Ok(body) => body,
        Err(e) => return e,
    };

    if body.amount == 0 || body.ordinal == 0 {
        return error(StatusCode::BAD_REQUEST, "amount and ordinal must be greater than zero");
    }

    if body.ordinal > body.amount {
        return error(StatusCode::BAD_REQUEST, format!("ordinal exceeds receive amount: maximum={}", body.amount));
    }

    let network_id = body.network.trim();

    if !valid_observation_network_id(network_id) {
        return error(StatusCode::BAD_REQUEST, "invalid network identifier");
    }

    let network = match selected_network(network_id) {
        Some(network) => network,
        None => {
            return error(StatusCode::BAD_REQUEST, format!("network is not registered: {network_id}"));
        }
    };

    let change_requested = match body.flow.trim().to_ascii_lowercase().as_str() {
        "receive" => false,
        "change" => true,
        _ => {
            return error(StatusCode::BAD_REQUEST, "flow must be receive or change");
        }
    };

    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());

    if let Err(e) = ensure_existing_substrate(&name) {
        return e;
    }

    // An ordinal remains a transit until that exact ordinal is executed.
    // Do not reconstruct it from the current activation or substitute another event.
    if commands::event_path_at_activation(&name, body.ordinal).is_none() {
        return error(
            StatusCode::NOT_FOUND,
            format!("receive requires a persisted transit at ordinal {}", body.ordinal),
        );
    }

    let transaction = match guarded("receive", || QuantumTransaction::send(&name, body.ordinal)) {
        Ok(transaction) => transaction,
        Err(e) => return e,
    };

    let constant_delta = match &transaction.constant {
        qp_hd::PersistedEvent::Exile(event) => event.heritage.transition.delta,
        qp_hd::PersistedEvent::Transit(..) => {
            return error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "QuantumTransaction::send returned a transit constant; expected persisted exile provenance",
            );
        }
    };

    let flow = if change_requested { Flow::Change(constant_delta) } else { Flow::Receive };

    let qtm = QtmResponse {
        commitment: hex::encode(transaction.qtm.commitment),
        coordinate: hex::encode(transaction.qtm.coordinate),
        sigma: transaction.qtm.sigma.to_string(),
        network: transaction.qtm.net_work.to_string(),
    };

    let transaction_response = QuantumTransactionResponse {
        amount: transaction.amount,
        qtm: qtm_frame(qtm.clone()),
        constant: "exile".to_string(),
        constant_delta: constant_delta.to_string(),
    };

    // Execute only the selected ordinal. Receive must preserve its persisted
    // event state and depth rather than creating another forward transition.
    let event = match guarded("receive", || transaction.receive(&name, flow)) {
        Ok(event) => event,
        Err(e) => return e,
    };

    let payload = EventPayload::from_heritage(event.heritage, event.depth, Scale::Local);

    json_response(
        StatusCode::OK,
        &ReceiveResponse {
            frame: QuantumTransactionFrame {
                quantum_transaction: transaction_response,
                event: event_map_response(payload, qtm),
            },
        },
    )
}

/// POST /wallets/:name/exile
fn handle_exile_wallet(name: String) -> HttpResponse {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());

    let substrate_path = match ensure_existing_substrate(&name) {
        Ok(path) => path,
        Err(e) => return e,
    };

    let activation = match guarded("activation lookup", || commands::load_activation(&name)) {
        Ok(activation) => activation,
        Err(e) => return e,
    };

    // Preserve an already-executed ordinal. Only a persisted transit
    // may be consumed by exile.
    let event = if let Some(exile_path) = commands::exile_path_at_activation(&name, activation) {
        match guarded("exile event lookup", || commands::load_event_at_activation(&name, activation)) {
            Ok(qp_hd::PersistedEvent::Exile(event)) => event,
            Ok(qp_hd::PersistedEvent::Transit(..)) => {
                return error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!(
                        "persisted exile path contains a transit event at activation {activation}: {}",
                        exile_path.display(),
                    ),
                );
            }
            Err(e) => return e,
        }
    } else {
        let Some(transit_path) = commands::event_path_at_activation(&name, activation) else {
            return error(
                StatusCode::NOT_FOUND,
                format!("no persisted transit or exile event exists at activation {activation}"),
            );
        };

        // Preserve the network-specific depth payloads when the transit
        // is materialized as an exile.
        let mut network_ids = Vec::new();

        let entries = match std::fs::read_dir(&transit_path) {
            Ok(entries) => entries,
            Err(e) => {
                return error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to inspect transit payloads: {e}"));
            }
        };

        for entry in entries {
            let entry = match entry {
                Ok(entry) => entry,
                Err(e) => {
                    return error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to inspect transit network: {e}"));
                }
            };

            let payload = entry.path().join("payload");

            if payload.join("depth.path").is_file() && payload.join("depth.objects").is_file() {
                network_ids.push(entry.file_name().to_string_lossy().to_string());
            }
        }

        network_ids.sort();
        network_ids.dedup();

        // Execute this ordinal once. qtm_exile consumes its persisted transit.
        let event = match guarded("exile", || commands::qtm_exile(&name)) {
            Ok(event) => event,
            Err(e) => return e,
        };

        let resulting_activation = event.heritage.state.activations();
        let dimension = event.heritage.state.dimension();

        let record_path = match canonical_record_path(&substrate_path, "exile", resulting_activation, u128::from(dimension)) {
            Ok(path) => path,
            Err(e) => return e,
        };

        for network_id in &network_ids {
            if let Err(e) = write_depth_payload(&record_path, network_id, &event.depth) {
                return e;
            }
        }

        event
    };

    let qtm = QtmResponse {
        commitment: hex::encode(event.qtm.commitment),
        coordinate: hex::encode(event.qtm.coordinate),
        sigma: event.qtm.sigma.to_string(),
        network: event.qtm.net_work.to_string(),
    };

    let payload = EventPayload::from_heritage(event.heritage, event.depth, Scale::Local);

    json_response(StatusCode::OK, &direction_response(payload, None, qtm))
}

fn load_wirepackets(name: &str, activation: u64, network: &str) -> ApiResult<WirePacketsResponse> {
    validate_wirepackets_scope(name, network)?;

    let lock = substrate_lock(name);
    let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());

    let substrate_path = ensure_existing_substrate(name)?;

    let budget = load_network_budget(&substrate_path, activation, network)?.ok_or_else(|| {
        error(StatusCode::NOT_FOUND, "no WirePackets budget configured for this activation and network")
    })?;

    let event_path = commands::substrate_root()
        .join(name)
        .join("activations")
        .join(activation.to_string())
        .join("event");

    let mut entries = Vec::new();
    let mut total_bytes = 0u64;

    for submission in 1..=budget.max_events {
        let path = commands::wirepackets_path(name, activation, submission);

        let metadata = match std::fs::metadata(&path) {
            Ok(metadata) if metadata.is_file() => metadata,
            Ok(_) => continue,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => {
                return Err(error(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("unable to inspect submission {submission}: {e}"),
                ));
            }
        };

        let bytes = std::fs::read(&path).map_err(|e| {
            error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to read submission {submission}: {e}"))
        })?;

        let next_size = total_bytes
            .checked_add(bytes.len() as u64)
            .ok_or_else(|| error(StatusCode::PAYLOAD_TOO_LARGE, "combined WirePackets size overflow"))?;

        if next_size > WIREPACKETS_MAX_BYTES {
            return Err(error(
                StatusCode::PAYLOAD_TOO_LARGE,
                format!("combined WirePackets exceeds {WIREPACKETS_MAX_BYTES}-byte limit"),
            ));
        }

        let packets = decode_wirepackets(&bytes).map_err(|e| {
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("invalid WirePackets for activation {activation}, submission {submission}: {e}"),
            )
        })?;

        let packet_count = packets.len();

        entries.push(WirePacketEntry {
            submission,
            name: submission.to_string(),
            path: path.display().to_string(),
            bytes: metadata.len(),
            packet_count,
            packets: wire_packet_value_entries(&packets),
        });

        total_bytes = next_size;
    }

    let exists = !entries.is_empty();

    Ok(WirePacketsResponse {
        substrate: name.to_owned(),
        activation,
        network: network.to_owned(),
        scope: "activation",
        path: event_path.display().to_string(),
        exists,
        bytes: exists.then_some(total_bytes),
        file_route: Some(format!(
            "/wallets/{name}/activations/{activation}/networks/{network}/wirepackets/file"
        )),
        packets: entries,
    })
}

fn handle_get_wirepackets(name: String, activation: u64, network: String) -> HttpResponse {
    match load_wirepackets(&name, activation, &network) {
        Ok(response) => json_response(StatusCode::OK, &response),
        Err(response) => response,
    }
}

fn handle_get_wirepackets_file(name: String, activation: u64, network: String) -> HttpResponse {
    if let Err(e) = validate_wirepackets_scope(&name, &network) {
        return e;
    }

    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());

    let substrate_path = match ensure_existing_substrate(&name) {
        Ok(path) => path,
        Err(e) => return e,
    };

    let budget = match load_network_budget(&substrate_path, activation, &network) {
        Ok(Some(budget)) => budget,
        Ok(None) => {
            return error(StatusCode::NOT_FOUND, "no WirePackets budget configured for this activation and network");
        }
        Err(e) => return e,
    };

    let persisted = match has_persisted_wirepackets(&name, activation, Some(budget.max_events)) {
        Ok(persisted) => persisted,
        Err(e) => return e,
    };

    let mut combined = Vec::new();

    if persisted {
        for submission in 1..=budget.max_events {
            let path = commands::wirepackets_path(&name, activation, submission);

            let metadata = match std::fs::metadata(&path) {
                Ok(metadata) => metadata,
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => {
                    return error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("unable to inspect submission {submission}: {e}"),
                    );
                }
            };

            if !metadata.is_file() {
                continue;
            }

            let bytes = match std::fs::read(&path) {
                Ok(bytes) => bytes,
                Err(e) if e.kind() == ErrorKind::NotFound => continue,
                Err(e) => {
                    return error(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("unable to read submission {submission}: {e}"),
                    );
                }
            };

            let next_size = match (combined.len() as u64).checked_add(bytes.len() as u64) {
                Some(size) => size,
                None => {
                    return error(StatusCode::PAYLOAD_TOO_LARGE, "combined WirePackets size overflow");
                }
            };

            if next_size > WIREPACKETS_MAX_BYTES {
                return error(
                    StatusCode::PAYLOAD_TOO_LARGE,
                    format!("combined WirePackets exceeds {WIREPACKETS_MAX_BYTES}-byte limit"),
                );
            }

            combined.extend_from_slice(&bytes);
        }
    }

    if combined.is_empty() {
        return error(StatusCode::NOT_FOUND, "no persisted WirePackets submissions within configured budget");
    }

    bytes_response(StatusCode::OK, "application/octet-stream", combined)
}

async fn handle_start_bitminer(name: String, activation: u64, network: String) -> HttpResponse {
    if let Err(e) = validate_bitminer_scope(&name, &network) {
        return e;
    }

    if let Err(e) = ensure_existing_substrate(&name) {
        return e;
    }

    if !bit_minner_enabled() && std::env::var_os("BIT_MINER_BIN").is_none() {
        return error(StatusCode::NOT_IMPLEMENTED, "bit-miner feature not enabled and BIT_MINER_BIN not set");
    }

    let key = bitminer_key(&name, activation, &network);
    let mut processes = bit_miner_processes().lock().await;

    let (running, pid) = match live_bitminer(&mut processes, &key) {
        Ok(state) => state,
        Err(e) => return e,
    };

    if running {
        return json_response(
            StatusCode::OK,
            &BitMinerStartResponse {
                substrate: name,
                activation,
                network,
                pid,
                already_running: true,
                message: "bit-miner process is already running for this network".into(),
            },
        );
    }

    let (executable, mode_args): (std::ffi::OsString, Vec<&str>) = match std::env::var_os("BIT_MINER_BIN") {
        Some(bin) => (bin, Vec::new()),
        None => match std::env::current_exe() {
            Ok(exe) => (exe.into_os_string(), vec!["--bit-miner"]),
            Err(e) => {
                return error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to locate executable: {e}"));
            }
        },
    };

    let log_path = bitminer_log_path(&name, activation, &network);

    let log_file = match std::fs::File::create(&log_path) {
        Ok(file) => file,
        Err(e) => {
            return error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to create log file: {e}"));
        }
    };

    let log_file_err = match log_file.try_clone() {
        Ok(file) => file,
        Err(e) => {
            return error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to open log file: {e}"));
        }
    };

    let child = match Command::new(&executable)
        .args(&mode_args)
        .arg(activation.to_string())
        .arg(&name)
        .arg(&network)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log_file))
        .stderr(Stdio::from(log_file_err))
        .kill_on_drop(true)
        .spawn()
    {
        Ok(child) => child,
        Err(e) => {
            return error(StatusCode::INTERNAL_SERVER_ERROR, format!("failed to start bit-miner: {e}"));
        }
    };

    let pid = child.id();
    processes.insert(key, child);

    json_response(
        StatusCode::OK,
        &BitMinerStartResponse {
            substrate: name,
            activation,
            network,
            pid,
            already_running: false,
            message: "bit-miner network pipeline started".into(),
        },
    )
}

async fn handle_get_bitminer_status(name: String, activation: u64, network: String) -> HttpResponse {
    if let Err(e) = validate_bitminer_scope(&name, &network) {
        return e;
    }
    if let Err(e) = ensure_existing_substrate(&name) {
        return e;
    }

    let (running, pid) = match bitminer_process_state(&name, activation, &network).await {
        Ok(state) => state,
        Err(e) => return e,
    };

    let log_path = bitminer_log_path(&name, activation, &network);
    let event_path = commands::event_path_at_activation(&name, activation).map(|path| path.display().to_string());
    let last_error = if running { None } else { bitminer_log_tail(&log_path) };

    json_response(
        StatusCode::OK,
        &BitMinerStatusResponse {
            substrate: name,
            activation,
            network,
            running,
            pid,
            path: event_path.clone(),
            event_path,
            log_path: log_path.display().to_string(),
            clips: active_clips(activation),
            runtime: serialize_runtime(SystemTime::now()),
            error: last_error,
        },
    )
}

async fn handle_stop_bitminer(name: String, activation: u64, network: String) -> HttpResponse {
    if let Err(e) = validate_bitminer_scope(&name, &network) {
        return e;
    }
    if let Err(e) = ensure_existing_substrate(&name) {
        return e;
    }

    let key = bitminer_key(&name, activation, &network);
    let mut processes = bit_miner_processes().lock().await;

    let Some(mut child) = processes.remove(&key) else {
        return json_response(
            StatusCode::OK,
            &BitMinerStopResponse {
                substrate: name,
                activation,
                network,
                was_running: false,
                stopped: false,
                pid: None,
                message: "no bit-miner process is registered for this network".into(),
            },
        );
    };

    let pid = child.id();

    match child.try_wait() {
        Ok(Some(_)) => {
            return json_response(
                StatusCode::OK,
                &BitMinerStopResponse {
                    substrate: name,
                    activation,
                    network,
                    was_running: false,
                    stopped: false,
                    pid: None,
                    message: "bit-miner process had already exited".into(),
                },
            );
        }
        Ok(None) => {}
        Err(e) => {
            return error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to inspect bit-miner process: {e}"));
        }
    }

    if let Err(e) = child.kill().await {
        return error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to stop bit-miner process: {e}"));
    }

    json_response(
        StatusCode::OK,
        &BitMinerStopResponse {
            substrate: name,
            activation,
            network,
            was_running: true,
            stopped: true,
            pid,
            message: "bit-miner network pipeline stopped".into(),
        },
    )
}

fn handle_get_bitminer_log(name: String, activation: u64, network: String) -> HttpResponse {
    if let Err(e) = validate_bitminer_scope(&name, &network) {
        return e;
    }
    if let Err(e) = ensure_existing_substrate(&name) {
        return e;
    }

    let path = bitminer_log_path(&name, activation, &network);
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(e) => {
            let status = if e.kind() == ErrorKind::NotFound { StatusCode::NOT_FOUND } else { StatusCode::INTERNAL_SERVER_ERROR };
            return error(status, format!("unable to read bit-miner log: {e}"));
        }
    };

    let start = bytes.len().saturating_sub(BITMINER_LOG_MAX_BYTES);
    let body = String::from_utf8_lossy(&bytes[start..]).into_owned();
    let mut response = bytes_response(StatusCode::OK, "text/plain; charset=utf-8", body);

    response.headers_mut().insert(header::CACHE_CONTROL, HeaderValue::from_static("no-store"));

    response
}

async fn handle_list_bitcoin_observations(name: String, activation: u64) -> HttpResponse {
    if !valid_substrate_name(&name) {
        return error(StatusCode::BAD_REQUEST, "invalid substrate name");
    }
    if let Err(e) = ensure_existing_substrate(&name) {
        return e;
    }

    match bitminer_process_list(&name, activation).await {
        Ok(pipelines) => json_response(
            StatusCode::OK,
            &BitMinerProcessListResponse { substrate: name, activation, pipelines },
        ),
        Err(e) => e,
    }
}

async fn handle_get_payload_board(name: String, activation: u64) -> HttpResponse {
    let miner = match bitminer_process_state_any(&name, activation).await {
        Ok(miner) => miner,
        Err(e) => return e,
    };

    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());

    match build_payload_board(&name, activation, None, miner) {
        Ok(board) => json_response(StatusCode::OK, &board),
        Err(e) => e,
    }
}

/// GET /wallets/:name/exile/depth
fn handle_exile_depth(name: String) -> HttpResponse {
    let lock = substrate_lock(&name);
    let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());

    let substrate_path = match ensure_existing_substrate(&name) {
        Ok(path) => path,
        Err(e) => return e,
    };

    let record_path = match latest_record_path(&substrate_path, "exile_") {
        Ok(path) => path,
        Err(e) => return e,
    };

    let record = record_path.file_name().and_then(|name| name.to_str()).unwrap_or_default().to_string();

    let entries = match std::fs::read_dir(&record_path) {
        Ok(entries) => entries,
        Err(e) => {
            return error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to read exile payloads: {e}"));
        }
    };

    let mut payloads = Vec::new();

    for entry in entries {
        let entry = match entry {
            Ok(entry) => entry,
            Err(e) => {
                return error(StatusCode::INTERNAL_SERVER_ERROR, format!("unable to read exile network: {e}"));
            }
        };

        let network_path = entry.path();
        if !network_path.is_dir() {
            continue;
        }

        let network_id = entry.file_name().to_string_lossy().into_owned();
        let payload_path = network_path.join("payload");

        if !payload_path.join("depth.path").is_file() || !payload_path.join("depth.objects").is_file() {
            continue;
        }

        let depth = match read_persisted_depth(&payload_path) {
            Ok(depth) => depth,
            Err(e) => return e,
        };

        let selected = selected_network(&network_id);
        let registered = selected.is_some();
        let network_name = selected.map(|network| network.name).unwrap_or_else(|| network_id.clone());

        payloads.push(PersistedPayloadEntry {
            record: record.clone(),
            direction: "exile_outputs".to_string(),
            network: PersistedPayloadNetwork { id: network_id, name: network_name, registered },
            path: payload_path.display().to_string(),
            depth_identifier: depth.path,
            objects: depth.objects,
        });
    }

    payloads.sort_by(|a, b| a.network.id.cmp(&b.network.id));

    json_response(
        StatusCode::OK,
        &PersistedPayloadsResponse {
            substrate: name,
            record,
            directory: record_path.display().to_string(),
            payloads,
        },
    )
}
async fn handle_get_payload_board_for_network(name: String, activation: u64, network: String) -> HttpResponse {
if !valid_observation_network_id(&network) {
return error(StatusCode::BAD_REQUEST, "invalid network identifier for payload board");
}
let miner = match bitminer_process_state(&name, activation, &network).await {
Ok(m) => m,
Err(e) => return e,
};
let lock = substrate_lock(&name);
let _guard = lock.lock().unwrap_or_else(|p| p.into_inner());
match build_payload_board(&name, activation, Some(network.as_str()), miner) {
Ok(board) => json_response(StatusCode::OK, &board),
Err(e) => e,
}
}
// ---------- Hyper Route Dispatcher ---------- */
async fn dispatch(req: Request<Incoming>) -> HttpResponse {
let method = req.method().clone();
if method == Method::OPTIONS {
return handle_preflight();
}
let raw_path = req.uri().path().to_string();
let trimmed = raw_path.trim_end_matches('/');
let path = if trimmed.is_empty() { "/" } else { trimmed };
let segments: Vec<&str> = path.trim_start_matches('/').split('/').filter(|s| !s.is_empty()).collect();
match (method, segments.as_slice()) {
(Method::GET, ["health"]) => handle_health(),
(Method::GET, ["networks"]) => handle_list_networks(),
(Method::GET, ["wallets"]) => handle_list_wallets(),
(Method::HEAD, ["wallets"]) => handle_head_wallets(),
(Method::POST, ["wallets"]) => handle_create_wallet(req).await,
(Method::GET, ["wallets", name, "activations", act, "open"]) => match parse_u64_param(act, "activation") {
Ok(a) => handle_open_wallet((*name).to_string(), a),
Err(e) => e,
},
(Method::GET, ["wallets", name, "events"]) => handle_list_events((*name).to_string()),
(Method::GET, ["wallets", name, "events", record, "payloads"]) => {
handle_list_event_payloads((*name).to_string(), (*record).to_string())
}
(Method::POST, ["wallets", name, "transit"]) => handle_transit_wallet((*name).to_string(), req).await,
(Method::POST, ["wallets", name, "receive"]) => handle_receive_wallet((*name).to_string(), req).await,
(Method::GET, ["wallets", name, "exile", "depth"]) => handle_exile_depth((*name).to_string()),
(Method::POST, ["wallets", name, "exile"]) => handle_exile_wallet((*name).to_string()),
(Method::GET, ["wallets", name, "activations", act, "budgets"]) => match parse_u64_param(act, "activation") {
Ok(a) => handle_list_activation_budgets((*name).to_string(), a),
Err(e) => e,
},
(Method::PUT, ["wallets", name, "activations", act, "budgets", net]) => match parse_u64_param(act, "activation") {
Ok(a) => handle_update_activation_network_budget((*name).to_string(), a, (*net).to_string(), req).await,
Err(e) => e,
},
(Method::GET, ["wallets", name, "activations", act, "observations"]) => match parse_u64_param(act, "activation") {
Ok(a) => handle_list_bitcoin_observations((*name).to_string(), a).await,
Err(e) => e,
},
(Method::GET, ["wallets", name, "activations", act, "payload-board"]) => match parse_u64_param(act, "activation") {
Ok(a) => handle_get_payload_board((*name).to_string(), a).await,
Err(e) => e,
},
(Method::GET, ["wallets", name, "activations", act, "networks", net, "observation"]) => {
match parse_u64_param(act, "activation") {
Ok(a) => handle_get_bitminer_status((*name).to_string(), a, (*net).to_string()).await,
Err(e) => e,
}
}
(Method::POST, ["wallets", name, "activations", act, "networks", net, "observation"]) => {
match parse_u64_param(act, "activation") {
Ok(a) => handle_start_bitminer((*name).to_string(), a, (*net).to_string()).await,
Err(e) => e,
}
}
(Method::DELETE, ["wallets", name, "activations", act, "networks", net, "observation"]) => {
match parse_u64_param(act, "activation") {
Ok(a) => handle_stop_bitminer((*name).to_string(), a, (*net).to_string()).await,
Err(e) => e,
}
}
(Method::GET, ["wallets", name, "activations", act, "networks", net, "observation", "log"]) => {
match parse_u64_param(act, "activation") {
Ok(a) => handle_get_bitminer_log((*name).to_string(), a, (*net).to_string()),
Err(e) => e,
}
}
(Method::GET, ["wallets", name, "activations", act, "networks", net, "wirepackets"]) => {
match parse_u64_param(act, "activation") {
Ok(a) => handle_get_wirepackets((*name).to_string(), a, (*net).to_string()),
Err(e) => e,
}
}
(Method::GET, ["wallets", name, "activations", act, "networks", net, "wirepackets", "file"]) => {
match parse_u64_param(act, "activation") {
Ok(a) => handle_get_wirepackets_file((*name).to_string(), a, (*net).to_string()),
Err(e) => e,
}
}
(Method::GET, ["wallets", name, "activations", act, "networks", net, "payload-board"]) => {
match parse_u64_param(act, "activation") {
Ok(a) => handle_get_payload_board_for_network((*name).to_string(), a, (*net).to_string()).await,
Err(e) => e,
}
}
#[cfg(feature = "bit-miner")]
(Method::GET, [
    "wallets",
    name,
    "activations",
    act,
    "submissions",
    sub,
    "networks",
    net,
    field,
]) => {
    match (
        parse_u64_param(act, "activation"),
        parse_u64_param(sub, "submission"),
    ) {
        (Ok(a), Ok(s)) => handle_get_submission_value(
            (*name).to_string(),
            a,
            s,
            (*net).to_string(),
            (*field).to_string(),
        ),
        (Err(e), _) | (_, Err(e)) => e,
    }
}
_ => error(StatusCode::NOT_FOUND, "route not found"),
}
}


//---------- Bit-Miner Child Worker ---------- */
#[cfg(feature = "bit-miner")]
fn run_bit_miner(args: &[String]) -> ! {
    use bit_miner::observestream::{MempoolSpaceStream, MempoolStreamMode};

    let activation = args.first().expect("missing activation").parse::<u64>().expect("invalid activation");

    let substrate = args.get(1).expect("missing substrate").clone();
    let network = args.get(2).expect("missing network").clone();

    if !valid_observation_network_id(&network) || !is_bitcoin_network_id(&network) {
        eprintln!("bit-miner: invalid Bitcoin network ID: {network}");
        std::process::exit(2);
    }

    let mut pipeline = bit_miner::BitcoinObservationPipeline::new(activation, &substrate, &network).unwrap_or_else(|e| {
        eprintln!("bit-miner: pipeline initialization failed: {e}");
        std::process::exit(1);
    });

    let pipeline_network = pipeline.network();

    if selected_network(pipeline_network).is_none() {
        eprintln!("bit-miner: pipeline network is not registered: {pipeline_network}");
        std::process::exit(2);
    }

    eprintln!("bit-miner: pipeline ready substrate={substrate} activation={activation} network={pipeline_network}");

    let resolver = WirePacketPrevoutResolver::new(&substrate, activation);

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("bit-miner runtime initialization failed");

    runtime.block_on(async move {
        loop {
            let mut observation_stream = match MempoolSpaceStream::connect(activation, MempoolStreamMode::TransactionIds).await {
                Ok(stream) => stream,
                Err(e) => {
                    eprintln!("bit-miner: stream connection failed: {e}");
                    continue;
                }
            };

            loop {
                match observation_stream.stream(&substrate, &mut pipeline, &resolver).await {
                    Ok(Ok(Some(_))) => {
                        eprintln!("bit-miner: transaction processed substrate={substrate} activation={activation}");
                    }
                    Ok(Ok(None)) => {}
                    Ok(Err(e)) => eprintln!("bit-miner: pipeline error: {e}"),
                    Err(e) => {
                        eprintln!("bit-miner: stream disconnected: {e}");
                        break;
                    }
                }
            }
        }
    });

    unreachable!("bit-miner stream loop is intentionally persistent");
}

fn resolve_home() {
#[cfg(windows)]
{
if std::env::var_os("HOME").filter(|v| !v.is_empty()).is_none() {
if let Some(profile) = std::env::var_os("USERPROFILE") {
unsafe {
std::env::set_var("HOME", profile);
}
}
}
}
}
fn main() {
resolve_home();
#[cfg(feature = "bit-miner")]
{
let _ = rustls::crypto::ring::default_provider().install_default();
let args: Vec<String> = std::env::args().collect();
if args.get(1).map(String::as_str) == Some("--bit-miner") {
run_bit_miner(&args[2..]);
}
}
#[cfg(not(feature = "bit-miner"))]
{
if std::env::args().nth(1).as_deref() == Some("--bit-miner") {
eprintln!("this qtm-api build does not include the bit-miner feature");
std::process::exit(2);
}
}
tokio::runtime::Builder::new_multi_thread()
.enable_all()
.build()
.expect("failed to build tokio runtime")
.block_on(serve());
}

async fn serve() {
    let root = commands::substrate_root();
    println!("substrate_root={}", root.display());

    if root.is_dir() {
        if let Ok(entries) = std::fs::read_dir(&root) {
            let mut discovered: Vec<String> = entries
                .filter_map(Result::ok)
                .filter(|entry| entry.path().is_dir())
                .map(|entry| entry.file_name().to_string_lossy().into_owned())
                .filter(|name| valid_substrate_name(name))
                .collect();

            discovered.sort();
            println!("substrates_discovered={}", discovered.len());

            for substrate in discovered {
                let commitment = read_trim(&root.join(&substrate).join("qtm.commitment"))
                    .filter(|value| !value.is_empty())
                    .unwrap_or_else(|| "unavailable".to_string());

                println!("substrate={substrate} commitment={commitment}");
            }
        }
    } else {
        println!("substrates_discovered=0 (root does not exist; startup did not create it)");
    }

    let port = std::env::var("PORT")
        .ok()
        .and_then(|value| value.parse::<u16>().ok())
        .filter(|port| *port != 0)
        .unwrap_or(8080);

    let electrum = electrum_port();

    if port == electrum {
        panic!("HTTP and Electrum cannot share port {port}");
    }

    let http_address = format!("0.0.0.0:{port}");
    let electrum_address = format!("0.0.0.0:{electrum}");

    let listener = TcpListener::bind(&http_address)
        .await
        .unwrap_or_else(|e| panic!("failed to bind qtm-api HTTP listener at {http_address}: {e}"));

    let electrum_listener = TcpListener::bind(&electrum_address)
        .await
        .unwrap_or_else(|e| panic!("failed to bind qtm-api Electrum listener at {electrum_address}: {e}"));

    println!(
        "qtm-api Hyper HTTP/1 listener bound at {}",
        listener.local_addr().expect("HTTP listener address unavailable")
    );

    tokio::spawn(async move {
        if let Err(e) = serve_electrum(electrum_listener).await {
            eprintln!("Electrum listener stopped: {e}");
        }
    });

    loop {
        let (stream, _) = match listener.accept().await {
            Ok(connection) => connection,
            Err(e) => {
                eprintln!("HTTP accept error: {e}");
                continue;
            }
        };

        tokio::spawn(async move {
            let io = TokioIo::new(stream);

            let service = service_fn(|req: Request<Incoming>| async move { Ok::<_, Infallible>(dispatch(req).await) });

            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                eprintln!("HTTP/1 connection error: {e}");
            }
        });
    }
}
