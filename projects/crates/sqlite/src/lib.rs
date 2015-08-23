//! SQLite binary-compatible passthrough for YYDS.
//!
//! This crate implements the SQLite format 3 container in **pure Rust**. YYDS
//! cannot route this surface through `yyds-kv` or other YY-system storage
//! optimizations because on-disk bytes must stay wire-compatible with the
//! reference format.

mod database;
mod format;
mod record;
mod table;

use yyds_types::{Error, Result};

pub use crate::database::{read_existing, SqliteDatabase};
pub use crate::table::{read_table, TableLimits, TableRow};
pub use crate::record::{decode_record, decode_varint, RecordLimits, RecordValue, TextEncoding};
pub use crate::format::{
    blank_database, decode_library_version, validate_database, validate_header,
    ENGINE_LIBRARY_VERSION, ENGINE_LIBRARY_VERSION_NUMBER, MAGIC,
};

/// Adapter identifier used in logs and diagnostics.
pub const BACKEND_ID: &str = "sqlite";

/// Short label for the binary-compatibility constraint.
pub const CONSTRAINT: &str = "binary-compatible passthrough";

/// Why YYDS does not optimize SQLite through the YY storage plane.
pub const WHY_NOT_YY_OPTIMIZED: &str =
    "SQLite requires binary-compatible on-disk bytes. YYDS stores VOS in `.yyds` and `.yykv` instead.";

fn matches_select_one(sql: &str) -> bool {
    let normalized = sql.trim().trim_end_matches(';').trim();
    normalized.eq_ignore_ascii_case("select 1")
}

fn matches_select_sqlite_version(sql: &str) -> bool {
    let normalized = sql.trim().trim_end_matches(';').trim();
    normalized.eq_ignore_ascii_case("select sqlite_version()")
}

/// Handles one SQL statement for the SQLite disguise CLI surface.
///
/// Only `SELECT 1` and `SELECT sqlite_version()` are supported. Other statements
/// are rejected because this path is not a SQL translation layer.
pub fn handle_sql(sql: &str) -> Result<Vec<String>> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Ok(vec![format!("sqlite3 (YYDS {})", yyds_types::version())]);
    }

    if matches_select_one(trimmed) {
        return Ok(vec!["1".into()]);
    }

    if matches_select_sqlite_version(trimmed) {
        return Ok(vec![sqlite_library_version()?]);
    }

    Err(Error::Unsupported("unsupported sqlite statement"))
}

/// Returns the YYDS SQLite engine library version string.
pub fn sqlite_library_version() -> Result<String> {
    Ok(ENGINE_LIBRARY_VERSION.to_string())
}
