//! Independent SQLite engine access using the upstream SQLite implementation.

use std::path::Path;

use sqlite_provider::{OpenOptions, SqliteError, SqliteProvider, SqliteValue, StatementResult};
use sqlite_provider_rusqlite::RusqliteProvider;

/// Column labels and rows returned by one SQLite statement.
#[derive(Debug, Clone, PartialEq)]
pub struct SqliteQueryResult {
    /// Result column names, empty for statements without rows.
    pub columns: Vec<String>,
    /// Materialized result rows with exact integers and binary blobs.
    pub rows: Vec<Vec<SqliteValue>>,
    /// Number of changed rows reported by SQLite.
    pub changes: u64,
    /// Exact rowid of the last insert on this connection.
    pub last_insert_rowid: i64,
}

impl From<StatementResult> for SqliteQueryResult {
    fn from(value: StatementResult) -> Self {
        Self {
            columns: value.columns,
            rows: value.rows,
            changes: value.changes,
            last_insert_rowid: value.last_insert_rowid,
        }
    }
}

/// A real SQLite connection with upstream locking, pager, journal, and SQL behavior.
#[derive(Debug)]
pub struct SqliteEngine {
    provider: RusqliteProvider,
}

impl SqliteEngine {
    /// Opens a private in-memory SQLite database.
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        Ok(Self { provider: RusqliteProvider::open_in_memory().map_err(map_provider)? })
    }

    /// Opens or creates a SQLite file using standard SQLite locking and recovery.
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        Ok(Self { provider: RusqliteProvider::open(path).map_err(map_provider)? })
    }

    /// Opens only an existing SQLite file without creating a missing path.
    pub fn open_existing(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        Self::open(path)
    }

    /// Opens an existing SQLite file without write access or implicit creation.
    pub fn open_read_only(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        Ok(Self {
            provider: RusqliteProvider::open_with_options(path, OpenOptions { read_only: true }).map_err(map_provider)?,
        })
    }

    /// Executes exactly one statement and preserves native SQLite value types.
    pub fn execute(&self, sql: &str) -> rusqlite::Result<SqliteQueryResult> {
        self.execute_with_parameters(sql, &[])
    }

    /// Executes exactly one statement with all SQLite parameter slots bound in index order.
    pub fn execute_with_parameters(&self, sql: &str, parameters: &[SqliteValue]) -> rusqlite::Result<SqliteQueryResult> {
        self.provider.execute_one(sql, parameters).map_err(map_provider).map(SqliteQueryResult::from)
    }

    /// Executes a SQLite batch, discarding any result rows as `sqlite3_exec` does without a callback.
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.provider.execute_batch(sql).map_err(map_provider)
    }

    /// Returns the SQLite engine's native source id.
    pub fn source_id(&self) -> rusqlite::Result<String> {
        self.provider.source_id().map_err(map_provider)
    }

    /// Exposes the versioned provider surface used by Iris and `@yyds/sqlite/node`.
    pub fn provider(&self) -> &RusqliteProvider {
        &self.provider
    }
}

impl SqliteProvider for SqliteEngine {
    fn capabilities(&self) -> &sqlite_provider::SqliteCapabilities {
        self.provider.capabilities()
    }

    fn sqlite_version(&self) -> Result<String, SqliteError> {
        self.provider.sqlite_version()
    }

    fn source_id(&self) -> Result<String, SqliteError> {
        self.provider.source_id()
    }

    fn execute_one(&self, sql: &str, params: &[SqliteValue]) -> Result<StatementResult, SqliteError> {
        self.provider.execute_one(sql, params)
    }

    fn execute_batch(&self, sql: &str) -> Result<(), SqliteError> {
        self.provider.execute_batch(sql)
    }

    fn begin_immediate(&self) -> Result<(), SqliteError> {
        self.provider.begin_immediate()
    }

    fn commit(&self) -> Result<(), SqliteError> {
        self.provider.commit()
    }

    fn rollback(&self) -> Result<(), SqliteError> {
        self.provider.rollback()
    }

    fn inspect_catalog(&self) -> Result<sqlite_provider::CatalogSnapshot, SqliteError> {
        self.provider.inspect_catalog()
    }
}

fn map_provider(error: SqliteError) -> rusqlite::Error {
    rusqlite::Error::SqliteFailure(rusqlite::ffi::Error::new(1), Some(error.message))
}
