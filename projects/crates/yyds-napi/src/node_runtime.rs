//! Node directory lifecycle and runtime status persistence.

use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read},
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
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
}

/// Initializes a node directory and local shard storage.
pub fn init_node(data_dir: &Path, identity: NodeIdentity) -> Result<(), String> {
    let _lease = NodeLease::acquire(data_dir, identity).map_err(|error| error.to_string())?;
    open_executor(data_dir)?;
    Ok(())
}

/// Opens the local key-value executor for a node directory.
pub fn open_executor(directory: &Path) -> Result<Arc<dyn KeyValueExecutor>, String> {
    let shard_id = ShardId(LOCAL_SHARD_ID.into());
    let routing = ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).map_err(|error| format!("routing: {error:?}"))?;
    let shard = FileShard::open(directory.join(format!("{LOCAL_SHARD_ID}.yykv"))).map_err(|error| error.to_string())?;
    let executor =
        LocalExecutor::new(routing, std::collections::HashMap::from([(shard_id, shard)])).map_err(|error| error.to_string())?;
    Ok(Arc::new(executor))
}

/// Inspects node ownership and optional runtime status.
pub fn inspect_node(data_dir: &Path) -> Result<NodeStatusSnapshot, String> {
    let state = NodeLease::inspect(data_dir).map_err(|error| error.to_string())?;
    let runtime = if state.ownership == LocalOwnership::Held { read_runtime_status(data_dir)? } else { None };
    Ok(NodeStatusSnapshot {
        directory: state.directory.display().to_string(),
        identity: state.identity,
        ownership: match state.ownership {
            LocalOwnership::Held => "held".into(),
            LocalOwnership::Available => "available".into(),
        },
        runtime,
        health: "local-ownership-only".into(),
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
