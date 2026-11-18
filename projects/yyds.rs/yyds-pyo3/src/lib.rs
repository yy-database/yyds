//! Python extension for YYDS node lifecycle and control-plane inspection.
//!
//! The surface is deliberately narrow: identity initialization, ownership and
//! runtime status inspection, stop-marker creation and a bounded stop wait. Runtime
//! status reading, the running generation and the stop coordination come from the
//! server node owner, so this module only maps parameters and results. It does not
//! expose SQL ASTs, wire packets, key-value commands or YYDB types.

use std::time::Duration;

use pyo3::{create_exception, exceptions::PyException, prelude::*};
use serde_json::{Value, json};
use yyds::{
    cluster::control::{LeaseError, LocalOwnership, NodeIdentity, NodeLease},
    runtime::{RuntimeStatus, read_runtime_status, request_stop, wait_until_stopped},
    server::node::{NodeControlError, RuntimeStatusError},
};

create_exception!(yyds_pyo3, YydsError, PyException);

/// 运行状态文件版本，取自服务端运行状态 owner。
const CONTROL_VERSION: u32 = yyds::runtime::CONTROL_VERSION;

#[pymodule]
fn yyds_pyo3(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add("YydsError", module.py().get_type::<YydsError>())?;
    module.add("CONTROL_VERSION", CONTROL_VERSION)?;
    module.add_function(wrap_pyfunction!(version, module)?)?;
    module.add_function(wrap_pyfunction!(node_init, module)?)?;
    module.add_function(wrap_pyfunction!(node_status, module)?)?;
    module.add_function(wrap_pyfunction!(node_request_stop, module)?)?;
    module.add_function(wrap_pyfunction!(node_wait_until_stopped, module)?)?;
    Ok(())
}

/// Native control-plane version shared with the Rust workspace.
#[pyfunction]
fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

/// Creates the node directory identity without starting a node process.
#[pyfunction]
fn node_init(data_dir: &str, cluster_id: &str, node_id: &str) -> PyResult<()> {
    let identity = NodeIdentity::new(cluster_id, node_id).map_err(map_lease)?;
    let lease = NodeLease::acquire(data_dir, identity).map_err(map_lease)?;
    drop(lease);
    Ok(())
}

/// Returns node identity, ownership and the published runtime status as JSON.
///
/// 返回给 Python 的键一律使用 snake_case；落盘文件仍是 camelCase，
/// 两者之间的翻译只在 [`runtime_view`] 这一处发生。
#[pyfunction]
fn node_status(data_dir: &str) -> PyResult<String> {
    let status = NodeLease::inspect(data_dir).map_err(map_lease)?;
    let runtime = if status.ownership == LocalOwnership::Held {
        read_runtime_status(&status.directory).map_err(map_status)?.as_ref().map(runtime_view)
    }
    else {
        None
    };
    Ok(json!({
        "directory": status.directory.display().to_string(),
        "identity": {
            "cluster_id": status.identity.cluster_id(),
            "node_id": status.identity.node_id(),
        },
        "ownership": match status.ownership {
            LocalOwnership::Held => "held",
            LocalOwnership::Available => "available",
        },
        "runtime": runtime,
    })
    .to_string())
}

/// 把服务端运行状态投影成 Python 侧惯用的 snake_case 键。
fn runtime_view(runtime: &RuntimeStatus) -> Value {
    json!({
        "version": runtime.version,
        "generation": runtime.generation.to_string(),
        "pid": runtime.pid,
        "started_at_unix_ms": runtime.started_at_unix_ms,
        "redis_address": runtime.redis_address,
        "pgsql_address": runtime.pgsql_address,
        "mysql_address": runtime.mysql_address,
        "wire_address": runtime.wire_address,
    })
}

/// Creates a stop marker for the generation published by the running node.
#[pyfunction]
fn node_request_stop(data_dir: &str) -> PyResult<()> {
    let status = NodeLease::inspect(data_dir).map_err(map_lease)?;
    let runtime = published_runtime(&status.directory)?;
    request_stop(&status.directory, runtime.generation).map_err(map_status)
}

/// Waits until the node directory is released or the timeout elapses.
#[pyfunction]
fn node_wait_until_stopped(data_dir: &str, timeout_ms: u64) -> PyResult<()> {
    let status = NodeLease::inspect(data_dir).map_err(map_lease)?;
    let runtime = published_runtime(&status.directory)?;
    wait_until_stopped(&status.directory, runtime.generation, Duration::from_millis(timeout_ms)).map_err(map_control)
}

/// 读取运行中节点发布的运行状态；未发布时按「宿主还没起来」拒绝。
fn published_runtime(directory: &std::path::Path) -> PyResult<RuntimeStatus> {
    read_runtime_status(directory)
        .map_err(map_status)?
        .ok_or_else(|| YydsError::new_err("node owner has not published runtime status"))
}

fn map_status(error: RuntimeStatusError) -> PyErr {
    let message = match error {
        RuntimeStatusError::Io(error) => format!("node status I/O error: {error}"),
        RuntimeStatusError::Corrupt(_) => String::from("corrupt runtime status"),
    };
    YydsError::new_err(message)
}

fn map_control(error: NodeControlError) -> PyErr {
    YydsError::new_err(error.to_string())
}

fn map_lease(error: LeaseError) -> PyErr {
    let message = match error {
        LeaseError::Io(error) => format!("node ownership I/O error: {error}"),
        LeaseError::Busy => String::from("node data directory is already owned"),
        LeaseError::InvalidIdentity => String::from("invalid cluster or node identity"),
        LeaseError::IdentityMismatch => String::from("node data directory identity mismatch"),
        LeaseError::CorruptIdentity => String::from("corrupt or unsupported node identity"),
    };
    YydsError::new_err(message)
}
