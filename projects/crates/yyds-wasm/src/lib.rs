//! Browser WebAssembly surface for YYDS.

#![deny(clippy::all)]

use wasm_bindgen::prelude::*;
use yyds_types::version;

/// Library version (matches `yyds-types::version()`).
#[wasm_bindgen(js_name = yydsVersion)]
pub fn wasm_yyds_version() -> String {
    version().to_string()
}

/// Lightweight health probe for WASM binding smoke tests.
#[wasm_bindgen]
pub fn ping() -> String {
    "ok".to_string()
}
