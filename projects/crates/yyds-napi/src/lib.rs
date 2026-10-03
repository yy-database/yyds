//! Node N-API surface for YYDS.

#![deny(clippy::all)]

use napi_derive::napi;
use yyds_types::version;

mod sqlite;

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
