//! SQLite binary-compatible engine and bounded format-3 snapshot reader.
//!
//! SQL execution uses upstream SQLite's bundled C library through `rusqlite`.
//! Snapshot inspection is a separate bounded pure-Rust reader. Neither path
//! uses YYDS KV storage or distributed cluster execution.

mod database;
mod engine;
mod format;
mod record;
mod schema;
mod table;

pub use crate::{
    database::{read_existing, SqliteDatabase},
    engine::{SqliteEngine, SqliteQueryResult},
    format::{blank_database, decode_library_version, validate_database, validate_header, MAGIC},
    record::{decode_record, decode_varint, RecordLimits, RecordValue, TextEncoding},
    schema::{read_named_table, read_schema, SchemaObject, SchemaObjectKind},
    table::{read_table, TableLimits, TableRow},
};

pub use sqlite_provider::{JournalMode, SqliteProvider, SqliteValue, CONTRACT_VERSION};

/// Adapter identifier used in logs and diagnostics.
pub const BACKEND_ID: &str = "sqlite";

/// Short label for the binary-compatibility constraint.
pub const CONSTRAINT: &str = "upstream SQLite format-3 engine";

/// Why YYDS does not optimize SQLite through the YY storage plane.
pub const WHY_NOT_YY_OPTIMIZED: &str =
    "SQLite requires binary-compatible on-disk bytes. YYDS stores VOS in `.yyds` and `.yykv` instead.";

/// Returns the YYDS SQLite engine library version string.
pub fn sqlite_library_version() -> String {
    rusqlite::version().to_string()
}
