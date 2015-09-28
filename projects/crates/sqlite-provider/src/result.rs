//! Statement execution results.

use crate::SqliteValue;

/// Result of executing exactly one SQL statement.
#[derive(Debug, Clone, PartialEq)]
pub struct StatementResult {
    /// Result column labels in order.
    pub columns: Vec<String>,
    /// Materialized rows preserving SQLite storage classes.
    pub rows: Vec<Vec<SqliteValue>>,
    /// Rows changed by the statement.
    pub changes: u64,
    /// Last insert rowid for the connection after execution.
    pub last_insert_rowid: i64,
}
