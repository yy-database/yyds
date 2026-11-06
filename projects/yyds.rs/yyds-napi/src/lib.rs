//! Node N-API surface for YYDS — orthogonal runtime blocks, not a bundled CLI.

#![deny(clippy::all)]

use database::types::cluster::version;
use napi_derive::napi;

mod catalog;
mod executor;
mod mongo;
mod mysql;
mod node;
mod node_runtime;
mod pgsql;
mod redis;
mod schema;
mod wire;
mod wire_dispatch;
mod wire_protocol;
mod wire_serve;

/// Library version (matches `yyds-types::version()`).
#[napi]
pub fn yyds_version() -> String {
    version().to_string()
}

/// Lightweight health probe for native binding smoke tests.
#[napi]
pub fn ping() -> String {
    "ok".to_string()
}
