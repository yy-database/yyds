use std::{path::PathBuf, sync::Arc, time::Duration};

use napi::bindgen_prelude::*;
use napi_derive::napi;
use yyds_control::{NodeIdentity, NodeLease};
use crate::node_runtime::{
    clear_runtime_artifacts, init_node, inspect_node, read_runtime_status, request_stop, stop_requested, wait_until_stopped,
    write_runtime_status, RuntimeStatus,
};

/// Cluster and node identity for a YYDS data directory.
#[napi]
pub struct NodeIdentityHandle {
    inner: NodeIdentity,
}

#[napi]
impl NodeIdentityHandle {
    #[napi(constructor)]
    pub fn new(cluster_id: String, node_id: String) -> Result<Self> {
        Ok(Self { inner: NodeIdentity::new(cluster_id, node_id).map_err(map_lease)? })
    }
}

/// Exclusive ownership of a node directory until released.
#[napi]
pub struct NodeLeaseHandle {
    pub(crate) inner: Arc<NodeLease>,
}

#[napi]
impl NodeLeaseHandle {
    #[napi]
    pub fn acquire(data_dir: String, identity: &NodeIdentityHandle) -> Result<Self> {
        let lease = NodeLease::acquire(PathBuf::from(data_dir), identity.inner.clone()).map_err(map_lease)?;
        Ok(Self { inner: Arc::new(lease) })
    }

    #[napi]
    pub fn data_dir(&self) -> String {
        self.inner.directory().display().to_string()
    }
}

#[napi(object)]
pub struct NodeStatusJson {
    pub directory: String,
    pub ownership: String,
    pub health: String,
    pub identity: String,
    pub runtime: Option<String>,
}

#[napi]
pub fn node_init(data_dir: String, identity: &NodeIdentityHandle) -> Result<()> {
    init_node(PathBuf::from(data_dir).as_path(), identity.inner.clone()).map_err(map_string)
}

#[napi]
pub fn node_inspect(data_dir: String) -> Result<NodeStatusJson> {
    let snapshot = inspect_node(PathBuf::from(data_dir).as_path()).map_err(map_string)?;
    Ok(NodeStatusJson {
        directory: snapshot.directory,
        ownership: snapshot.ownership,
        health: snapshot.health,
        identity: serde_json::to_string(&snapshot.identity).map_err(map_serde)?,
        runtime: snapshot
            .runtime
            .map(|runtime| serde_json::to_string(&runtime).map_err(map_serde))
            .transpose()?,
    })
}

#[napi]
pub fn node_read_runtime_status(data_dir: String) -> Result<Option<String>> {
    match read_runtime_status(PathBuf::from(data_dir).as_path()).map_err(map_string)? {
        Some(value) => Ok(Some(serde_json::to_string(&value).map_err(map_serde)?)),
        None => Ok(None),
    }
}

#[napi]
pub fn node_write_runtime_status(data_dir: String, status_json: String) -> Result<()> {
    let status: RuntimeStatus = serde_json::from_str(&status_json).map_err(map_serde)?;
    write_runtime_status(PathBuf::from(data_dir).as_path(), &status).map_err(map_string)
}

#[napi]
pub fn node_request_stop(data_dir: String, generation: String) -> Result<()> {
    let generation = uuid::Uuid::parse_str(&generation).map_err(map_uuid)?;
    request_stop(PathBuf::from(data_dir).as_path(), generation).map_err(map_string)
}

#[napi]
pub fn node_stop_requested(data_dir: String, generation: String) -> Result<bool> {
    let generation = uuid::Uuid::parse_str(&generation).map_err(map_uuid)?;
    Ok(stop_requested(PathBuf::from(data_dir).as_path(), generation))
}

#[napi]
pub fn node_clear_runtime_artifacts(data_dir: String, generation: String) -> Result<()> {
    let generation = uuid::Uuid::parse_str(&generation).map_err(map_uuid)?;
    clear_runtime_artifacts(PathBuf::from(data_dir).as_path(), generation);
    Ok(())
}

#[napi]
pub fn node_wait_until_stopped(data_dir: String, generation: String, timeout_ms: u32) -> Result<()> {
    let generation = uuid::Uuid::parse_str(&generation).map_err(map_uuid)?;
    wait_until_stopped(PathBuf::from(data_dir).as_path(), generation, Duration::from_millis(timeout_ms as u64))
        .map_err(map_string)
}

fn map_lease(error: yyds_control::LeaseError) -> Error {
    Error::from_reason(error.to_string())
}

fn map_string(error: String) -> Error {
    Error::from_reason(error)
}

fn map_serde(error: serde_json::Error) -> Error {
    Error::from_reason(error.to_string())
}

fn map_uuid(error: uuid::Error) -> Error {
    Error::from_reason(error.to_string())
}
