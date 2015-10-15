//! SQLite binary-compatible format-3 snapshot reader (pure Rust).
//!
//! This crate inspects on-disk SQLite main files without linking any external
//! SQLite engine. Snapshot reads are bounded and do not recover WAL or journal
//! state. Neither path uses YYDS KV storage or distributed cluster execution.

mod database;
mod format;
mod record;
mod schema;
mod table;

pub use crate::{
    database::{read_existing, SqliteDatabase},
    format::{
        blank_database, decode_library_version, validate_database, validate_header, ENGINE_LIBRARY_VERSION,
        ENGINE_LIBRARY_VERSION_NUMBER, MAGIC,
    },
    record::{decode_record, decode_varint, RecordLimits, RecordValue, TextEncoding},
    schema::{read_named_table, read_schema, SchemaObject, SchemaObjectKind},
    table::{read_table, TableLimits, TableRow},
};

/// Adapter identifier used in logs and diagnostics.
pub const BACKEND_ID: &str = "sqlite";

/// Short label for the binary-compatibility constraint.
pub const CONSTRAINT: &str = "upstream SQLite format-3 engine";

/// Why YYDS does not optimize SQLite through the YY storage plane.
pub const WHY_NOT_YY_OPTIMIZED: &str =
    "SQLite requires binary-compatible on-disk bytes. YYDS stores VOS in `.yyds` and `.yykv` instead.";

/// Returns the YYDS SQLite format reader library version string.
pub fn sqlite_library_version() -> String {
    ENGINE_LIBRARY_VERSION.to_string()
}
