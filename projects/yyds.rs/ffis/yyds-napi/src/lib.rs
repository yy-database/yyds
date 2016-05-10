//! Node N-API surface for YYDS — orthogonal runtime blocks, not a bundled CLI.

#![deny(clippy::all)]

use napi_derive::napi;
use yyds_types::version;

mod catalog;
mod executor;
mod mongo;
mod mysql;
mod node;
mod node_runtime;
mod pgsql;
mod redis;
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
