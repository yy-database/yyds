//! Independent SQLite engine access using the upstream SQLite implementation.

use std::path::Path;

use rusqlite::{
    Connection, OpenFlags, params_from_iter,
    types::{ToSql, ToSqlOutput, ValueRef},
};

/// One SQL result cell preserving SQLite's runtime storage class.
#[derive(Debug, Clone, PartialEq)]
pub enum SqliteValue {
    /// SQL NULL.
    Null,
    /// Signed SQLite INTEGER.
    Integer(i64),
    /// SQLite REAL.
    Real(f64),
    /// SQLite TEXT.
    Text(Vec<u8>),
    /// SQLite BLOB.
    Blob(Vec<u8>),
}

impl ToSql for SqliteValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match self {
            Self::Null => ValueRef::Null,
            Self::Integer(value) => ValueRef::Integer(*value),
            Self::Real(value) => ValueRef::Real(*value),
            Self::Text(value) => ValueRef::Text(value),
            Self::Blob(value) => ValueRef::Blob(value),
        }))
    }
}

impl std::fmt::Display for SqliteValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => Ok(()),
            Self::Integer(value) => write!(formatter, "{value}"),
            Self::Real(value) => write!(formatter, "{value}"),
            Self::Text(value) => formatter.write_str(&String::from_utf8_lossy(value)),
            Self::Blob(value) => {
                formatter.write_str("x'")?;
                for byte in value {
                    write!(formatter, "{byte:02x}")?;
                }
                formatter.write_str("'")
            }
        }
    }
}

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

/// A real SQLite connection with upstream locking, pager, journal, and SQL behavior.
#[derive(Debug)]
pub struct SqliteEngine {
    connection: Connection,
}

impl SqliteEngine {
    /// Opens a private in-memory SQLite database.
    pub fn open_in_memory() -> rusqlite::Result<Self> {
        Ok(Self { connection: Connection::open_in_memory()? })
    }

    /// Opens or creates a SQLite file using standard SQLite locking and recovery.
    pub fn open(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        Ok(Self { connection: Connection::open(path)? })
    }

    /// Opens only an existing SQLite file without creating a missing path.
    pub fn open_existing(path: impl AsRef<Path>) -> rusqlite::Result<Self> {
        let connection = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE)?;
        Ok(Self { connection })
    }

    /// Executes exactly one statement and preserves native SQLite value types.
    pub fn execute(&self, sql: &str) -> rusqlite::Result<SqliteQueryResult> {
        self.execute_with_parameters(sql, &[])
    }

    /// Executes exactly one statement with all SQLite parameter slots bound in index order.
    pub fn execute_with_parameters(&self, sql: &str, parameters: &[SqliteValue]) -> rusqlite::Result<SqliteQueryResult> {
        let mut statement = self.connection.prepare(sql)?;
        let columns = statement.column_names().iter().map(|name| (*name).to_string()).collect::<Vec<_>>();
        let mut rows = Vec::new();
        if columns.is_empty() {
            statement.execute(params_from_iter(parameters))?;
        }
        else {
            let mut cursor = statement.query(params_from_iter(parameters))?;
            while let Some(row) = cursor.next()? {
                let mut values = Vec::with_capacity(columns.len());
                for index in 0..columns.len() {
                    values.push(match row.get_ref(index)? {
                        ValueRef::Null => SqliteValue::Null,
                        ValueRef::Integer(value) => SqliteValue::Integer(value),
                        ValueRef::Real(value) => SqliteValue::Real(value),
                        ValueRef::Text(value) => SqliteValue::Text(value.to_vec()),
                        ValueRef::Blob(value) => SqliteValue::Blob(value.to_vec()),
                    });
                }
                rows.push(values);
            }
        }
        Ok(SqliteQueryResult {
            columns,
            rows,
            changes: self.connection.changes(),
            last_insert_rowid: self.connection.last_insert_rowid(),
        })
    }

    /// Executes a SQLite batch, discarding any result rows as `sqlite3_exec` does without a callback.
    pub fn execute_batch(&self, sql: &str) -> rusqlite::Result<()> {
        self.connection.execute_batch(sql)
    }

    /// Returns the SQLite engine's native source id.
    pub fn source_id(&self) -> rusqlite::Result<String> {
        self.connection.query_row("SELECT sqlite_source_id()", [], |row| row.get(0))
    }
}
