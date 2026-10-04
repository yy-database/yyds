//! Provider trait and open options.

use crate::{
    CatalogSnapshot, SqliteCapabilities, SqliteError, SqliteValue, StatementResult,
};

/// Options used when opening a file-backed SQLite provider.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    /// Open without write access and refuse to create a missing file.
    pub read_only: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self { read_only: false }
    }
}

/// Versioned SQLite execution surface shared by Iris adapters and YYDS tooling.
///
/// Implementations must preserve upstream SQLite locking, pager, transaction,
/// and journal/WAL behavior. Callers must not re-parse SQL inside the provider.
pub trait SqliteProvider {
    /// Advertised capabilities for this open connection.
    fn capabilities(&self) -> &SqliteCapabilities;

    /// Returns `SELECT sqlite_version()` from the active engine.
    fn sqlite_version(&self) -> Result<String, SqliteError>;

    /// Returns `SELECT sqlite_source_id()` from the active engine.
    fn source_id(&self) -> Result<String, SqliteError>;

    /// Executes exactly one statement with positional bind parameters.
    fn execute_one(&mut self, sql: &str, params: &[SqliteValue]) -> Result<StatementResult, SqliteError>;

    /// Executes a multi-statement batch and discards result rows.
    fn execute_batch(&mut self, sql: &str) -> Result<(), SqliteError>;

    /// Starts an immediate write transaction.
    fn begin_immediate(&mut self) -> Result<(), SqliteError>;

    /// Commits the active transaction.
    fn commit(&mut self) -> Result<(), SqliteError>;

    /// Rolls back the active transaction.
    fn rollback(&mut self) -> Result<(), SqliteError>;

    /// Inspects live catalog metadata through SQL metadata queries.
    fn inspect_catalog(&self) -> Result<CatalogSnapshot, SqliteError>;
}
