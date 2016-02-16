//! Python extension for YYDS node lifecycle and control-plane inspection.
//!
//! The surface is deliberately narrow: identity initialization, ownership and
//! runtime status inspection, stop-marker creation and a bounded stop wait. It
//! does not expose SQL ASTs, wire packets, key-value commands or YYDB types.

use std::{
    fs::OpenOptions,
    io::{self, Read},
    path::Path,
    time::{Duration, Instant},
};

use pyo3::{create_exception, exceptions::PyException, prelude::*};
use serde_json::{json, Value};
use yyds_control::{LeaseError, LocalOwnership, NodeIdentity, NodeLease};

create_exception!(yyds_pyo3, YydsError, PyException);

const CONTROL_VERSION: u32 = 1;
const STATUS_FILE: &str = "node.status.json";
const MAX_STATUS_BYTES: u64 = 4096;

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
#[pyfunction]
fn node_status(data_dir: &str) -> PyResult<String> {
    let status = NodeLease::inspect(data_dir).map_err(map_lease)?;
    let runtime = if status.ownership == LocalOwnership::Held {
        read_runtime_status(&status.directory).map_err(YydsError::new_err)?
    }
    else {
        None
    };
    Ok(json!({
        "directory": status.directory.display().to_string(),
        "identity": {
            "clusterId": status.identity.cluster_id(),
            "nodeId": status.identity.node_id(),
        },
        "ownership": match status.ownership {
            LocalOwnership::Held => "held",
            LocalOwnership::Available => "available",
        },
        "runtime": runtime,
    })
    .to_string())
}

/// Creates a stop marker for the generation published by the running node.
#[pyfunction]
fn node_request_stop(data_dir: &str) -> PyResult<()> {
    let status = NodeLease::inspect(data_dir).map_err(map_lease)?;
    let runtime = read_runtime_status(&status.directory)
        .map_err(YydsError::new_err)?
        .ok_or_else(|| YydsError::new_err("node owner has not published runtime status"))?;
    let generation =
        runtime.get("generation").and_then(Value::as_str).ok_or_else(|| YydsError::new_err("corrupt runtime status"))?;
    let marker = status.directory.join(format!("node.stop.{generation}"));
    match OpenOptions::new().write(true).create_new(true).open(marker) {
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(YydsError::new_err(format!("stop marker I/O error: {error}"))),
    }
}

/// Waits until the node directory is released or the timeout elapses.
#[pyfunction]
fn node_wait_until_stopped(data_dir: &str, timeout_ms: u64) -> PyResult<()> {
    let deadline = Instant::now() + Duration::from_millis(timeout_ms);
    loop {
        match NodeLease::inspect(data_dir) {
            Ok(state) if state.ownership == LocalOwnership::Available => return Ok(()),
            Ok(_) => {}
            Err(error) => return Err(map_lease(error)),
        }
        if Instant::now() >= deadline {
            return Err(YydsError::new_err("timed out waiting for node stop"));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

fn read_runtime_status(directory: &Path) -> Result<Option<Value>, String> {
    let path = directory.join(STATUS_FILE);
    let file = match std::fs::File::open(&path) {
        Ok(file) => file,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(format!("node status I/O error: {error}")),
    };
    if !file.metadata().map_err(|error| format!("node status I/O error: {error}"))?.is_file() {
        return Err("corrupt runtime status".into());
    }
    let mut bytes = Vec::new();
    file.take(MAX_STATUS_BYTES + 1).read_to_end(&mut bytes).map_err(|error| format!("node status I/O error: {error}"))?;
    if bytes.len() as u64 > MAX_STATUS_BYTES {
        return Err("corrupt runtime status".into());
    }
    let runtime: Value = serde_json::from_slice(&bytes).map_err(|_| String::from("corrupt runtime status"))?;
    if runtime.get("version").and_then(Value::as_u64) != Some(u64::from(CONTROL_VERSION))
        || !runtime.get("generation").and_then(Value::as_str).is_some_and(is_uuid_text)
    {
        return Err("corrupt runtime status".into());
    }
    // The Redis address is mandatory. SQL addresses stay optional so a node that
    // publishes only one of them remains inspectable.
    if !runtime.get("redisAddress").and_then(Value::as_str).is_some_and(is_loopback_address) {
        return Err("corrupt runtime status".into());
    }
    for key in ["pgsqlAddress", "mysqlAddress"] {
        match runtime.get(key) {
            None | Some(Value::Null) => {}
            Some(address) if address.as_str().is_some_and(is_loopback_address) => {}
            Some(_) => return Err("corrupt runtime status".into()),
        }
    }
    Ok(Some(runtime))
}

fn is_loopback_address(value: &str) -> bool {
    value.starts_with("127.0.0.1:") || value.starts_with("[::1]:")
}

fn is_uuid_text(value: &str) -> bool {
    let bytes = value.as_bytes();
    if bytes.len() != 36 {
        return false;
    }
    let mut significant = false;
    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 8 | 13 | 18 | 23) {
            if *byte != b'-' {
                return false;
            }
        }
        else if !byte.is_ascii_hexdigit() {
            return false;
        }
        else if *byte != b'0' {
            significant = true;
        }
    }
    significant
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
