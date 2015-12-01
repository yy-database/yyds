//! Node directory lifecycle and runtime status persistence.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use yyds_catalog::Catalog;
use yyds_control::{LocalOwnership, NodeIdentity, NodeLease};
use yyds_execution::{KeyValueExecutor, LocalExecutor};
use yyds_kv::FileShard;
use yyds_types::{ShardEpoch, ShardId, ShardMap};

pub const STATUS_FILE: &str = "node.status.json";
pub const CONTROL_VERSION: u32 = 1;
pub const LOCAL_SHARD_ID: &str = "local-0";

/// Published runtime metadata for a running node process.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct RuntimeStatus {
    pub version: u32,
    pub generation: uuid::Uuid,
    pub pid: u32,
    pub started_at_unix_ms: u128,
    pub redis_address: String,
    pub pgsql_address: Option<String>,
    pub mysql_address: Option<String>,
    pub wire_address: Option<String>,
}

/// JSON shape returned by node status inspection.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeStatusSnapshot {
    pub directory: String,
    pub identity: NodeIdentity,
    pub ownership: String,
    pub runtime: Option<RuntimeStatus>,
    pub health: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_revision: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub routing_epoch: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub shard_count: Option<usize>,
}

/// Opens catalog-driven facts (schema revision, routing map) from an optional `.yyds` catalog file.
/// Returns fallback hardcoded facts when the catalog path is absent, missing, or lacks published routing.
fn load_catalog_facts(catalog_path: Option<&Path>) -> CatalogFacts {
    let Some(path) = catalog_path
    else {
        return CatalogFacts::fallback(LOCAL_SHARD_ID);
    };
    if !path.exists() {
        eprintln!("[yyds] WARNING: catalog file {} not found falling back to local-only shard", path.display());
        return CatalogFacts::fallback(LOCAL_SHARD_ID);
    }
    match Catalog::open(path) {
        Ok(catalog) => {
            let schema_revision = catalog.schema().map(|s| s.version);
            let (routing, shards) = match catalog.routing() {
                Ok(Some(map)) => (Some(map.epoch()), map.shards().to_vec()),
                Ok(None) => {
                    eprintln!("[yyds] WARNING: catalog has no published routing epoch falling back to local-only shard");
                    return CatalogFacts::fallback(LOCAL_SHARD_ID);
                }
                Err(error) => {
                    eprintln!("[yyds] WARNING: catalog routing load failed ({}) falling back to local-only shard", error);
                    return CatalogFacts::fallback(LOCAL_SHARD_ID);
                }
            };
            CatalogFacts { schema_revision, routing, shards, source: CatalogSource::File(path.to_path_buf()) }
        }
        Err(error) => {
            eprintln!("[yyds] WARNING: catalog open failed ({}) falling back to local-only shard", error);
            CatalogFacts::fallback(LOCAL_SHARD_ID)
        }
    }
}

/// Resolved routing and shard facts for a node directory.
#[derive(Debug, Clone)]
struct CatalogFacts {
    schema_revision: Option<u32>,
    routing: Option<ShardEpoch>,
    shards: Vec<ShardId>,
    source: CatalogSource,
}

/// Where the facts came from — used to build the health string.
#[derive(Debug, Clone)]
enum CatalogSource {
    /// Derived from a catalog file at this path.
    File(PathBuf),
    /// Hardcoded local-only fallback because catalog was absent or invalid.
    Fallback,
}

impl CatalogFacts {
    fn fallback(local_shard_id: &str) -> Self {
        let shard = ShardId(local_shard_id.to_string());
        Self { schema_revision: None, routing: Some(ShardEpoch(1)), shards: vec![shard], source: CatalogSource::Fallback }
    }

    fn health(&self) -> String {
        match (&self.source, self.schema_revision, self.routing) {
            (CatalogSource::File(path), Some(rev), Some(epoch)) => {
                format!(
                    "catalog-driven (path={}, schema_revision={}, routing_epoch={}, shards={})",
                    path.display(),
                    rev,
                    epoch.0,
                    self.shards.len()
                )
            }
            (CatalogSource::File(path), None, Some(epoch)) => {
                format!(
                    "catalog-driven-schema-pending (path={}, routing_epoch={}, shards={})",
                    path.display(),
                    epoch.0,
                    self.shards.len()
                )
            }
            (CatalogSource::File(path), Some(rev), None) => {
                format!("catalog-driven-routing-pending (path={}, schema_revision={})", path.display(), rev)
            }
            (CatalogSource::File(path), None, None) => {
                format!("catalog-driven-empty (path={})", path.display())
            }
            (CatalogSource::Fallback, _, _) => "local-fallback".into(),
        }
    }
}

/// Initializes a node directory and local shard storage.
pub fn init_node(data_dir: &Path, identity: NodeIdentity) -> Result<(), String> {
    let _lease = NodeLease::acquire(data_dir, identity).map_err(|error| error.to_string())?;
    open_executor(data_dir, None)?;
    Ok(())
}

/// Opens the local key-value executor for a node directory.
///
/// When `catalog_path` points to a published `.yyds` file the executor uses the catalog's
/// routing epoch and shard layout. When the catalog is absent or invalid it falls back
/// to a single `local-0` shard and emits a WARNING on stderr — silent hardcoding is gone.
pub fn open_executor(directory: &Path, catalog_path: Option<&Path>) -> Result<Arc<dyn KeyValueExecutor>, String> {
    let facts = load_catalog_facts(catalog_path);
    let epoch = facts.routing.ok_or_else(|| String::from("catalog routing failed unexpectedly"))?;
    let routing = ShardMap::new(epoch, facts.shards.clone()).map_err(|error| format!("routing: {error:?}"))?;

    let mut shards = std::collections::HashMap::new();
    for shard_id in &facts.shards {
        let shard_path = directory.join(format!("{}.yykv", shard_id.0));
        let shard = FileShard::open(&shard_path)
            .map_err(|error| format!("failed to open shard {} at {}: {error}", shard_id.0, shard_path.display()))?;
        shards.insert(shard_id.clone(), shard);
    }
    let executor = LocalExecutor::new(routing, shards).map_err(|error| error.to_string())?;
    Ok(Arc::new(executor))
}

/// Inspects node ownership and optional runtime status.
///
/// Accepts an optional catalog path so the health snapshot reflects catalog-driven facts
/// (schema revision, routing epoch, shard count) instead of always claiming
/// `local-ownership-only`.
pub fn inspect_node(data_dir: &Path, catalog_path: Option<&Path>) -> Result<NodeStatusSnapshot, String> {
    let state = NodeLease::inspect(data_dir).map_err(|error| error.to_string())?;
    let runtime = if state.ownership == LocalOwnership::Held { read_runtime_status(data_dir)? } else { None };
    let facts = load_catalog_facts(catalog_path);
    Ok(NodeStatusSnapshot {
        directory: state.directory.display().to_string(),
        identity: state.identity,
        ownership: match state.ownership {
            LocalOwnership::Held => "held".into(),
            LocalOwnership::Available => "available".into(),
        },
        runtime,
        health: facts.health(),
        schema_revision: facts.schema_revision,
        routing_epoch: facts.routing.map(|e| e.0),
        shard_count: Some(facts.shards.len()),
    })
}

/// Writes runtime status atomically into the node directory.
pub fn write_runtime_status(directory: &Path, runtime: &RuntimeStatus) -> Result<(), String> {
    let bytes = serde_json::to_vec(runtime).map_err(|error| error.to_string())?;
    let temporary = directory.join("node.status.json.tmp");
    fs::write(&temporary, bytes).map_err(|error| error.to_string())?;
    fs::rename(temporary, directory.join(STATUS_FILE)).map_err(|error| error.to_string())
}

/// Reads validated runtime status when present.
pub fn read_runtime_status(directory: &Path) -> Result<Option<RuntimeStatus>, String> {
    match File::open(directory.join(STATUS_FILE)) {
        Ok(input) => {
            if !input.metadata().map_err(|error| error.to_string())?.is_file() {
                return Err("runtime status must be a regular file".into());
            }
            let mut bytes = Vec::new();
            input.take(4097).read_to_end(&mut bytes).map_err(|error| error.to_string())?;
            if bytes.len() > 4096 {
                return Err("runtime status exceeds byte limit".into());
            }
            let status: RuntimeStatus =
                serde_json::from_slice(&bytes).map_err(|error| format!("corrupt runtime status: {error}"))?;
            if status.version != CONTROL_VERSION || status.generation.is_nil() {
                return Err("unsupported or invalid runtime status".into());
            }
            for address in [
                Some(status.redis_address.as_str()),
                status.pgsql_address.as_deref(),
                status.mysql_address.as_deref(),
                status.wire_address.as_deref(),
            ]
            .into_iter()
            .flatten()
            {
                if !address.starts_with("127.0.0.1:") && !address.starts_with("[::1]:") {
                    return Err("unsupported or invalid runtime status".into());
                }
            }
            Ok(Some(status))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}

/// Creates a stop marker for the given runtime generation.
pub fn request_stop(directory: &Path, generation: uuid::Uuid) -> Result<(), String> {
    let path = stop_path(directory, generation);
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

/// Waits until the node directory becomes available or the timeout elapses.
pub fn wait_until_stopped(directory: &Path, generation: uuid::Uuid, timeout: Duration) -> Result<(), String> {
    let deadline = Instant::now() + timeout;
    loop {
        match NodeLease::inspect(directory) {
            Ok(state) if state.ownership == LocalOwnership::Available => return Ok(()),
            Err(error) => return Err(error.to_string()),
            _ if Instant::now() >= deadline => return Err("timed out waiting for node stop".into()),
            _ => {
                if let Some(current) = read_runtime_status(directory)? {
                    if current.generation != generation {
                        return Err("node generation changed while stopping".into());
                    }
                }
                std::thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

/// Removes runtime status and stop marker files.
pub fn clear_runtime_artifacts(directory: &Path, generation: uuid::Uuid) {
    let _ = fs::remove_file(directory.join(STATUS_FILE));
    let _ = fs::remove_file(stop_path(directory, generation));
}

/// Returns whether a stop marker exists for the generation.
pub fn stop_requested(directory: &Path, generation: uuid::Uuid) -> bool {
    stop_path(directory, generation).exists()
}

/// Builds a new runtime status snapshot for a starting process.
pub fn new_runtime_status(
    redis_address: String,
    pgsql_address: Option<String>,
    mysql_address: Option<String>,
    wire_address: Option<String>,
) -> RuntimeStatus {
    RuntimeStatus {
        version: CONTROL_VERSION,
        generation: uuid::Uuid::new_v4(),
        pid: std::process::id(),
        started_at_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH).map(|value| value.as_millis()).unwrap_or(0),
        redis_address,
        pgsql_address,
        mysql_address,
        wire_address,
    }
}

fn stop_path(directory: &Path, generation: uuid::Uuid) -> PathBuf {
    directory.join(format!("node.stop.{generation}"))
}
