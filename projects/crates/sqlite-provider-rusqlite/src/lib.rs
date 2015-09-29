//! Rusqlite reference implementation of the versioned SQLite provider contract.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::{cell::RefCell, path::Path};

use rusqlite::{
    Connection, OpenFlags, params_from_iter,
    types::{ToSql, ToSqlOutput, ValueRef},
};
use sqlite_provider::{
    CatalogColumn, CatalogSnapshot, CatalogTable, OpenOptions, SqliteCapabilities, SqliteError,
    SqliteProvider, SqliteValue, StatementResult,
};

/// Bundled upstream SQLite provider backed by `rusqlite`.
#[derive(Debug)]
pub struct RusqliteProvider {
    connection: RefCell<Connection>,
    capabilities: SqliteCapabilities,
}

impl RusqliteProvider {
    /// Opens a private in-memory database.
    pub fn open_in_memory() -> Result<Self, SqliteError> {
        let connection = Connection::open_in_memory().map_err(map_sqlite)?;
        Ok(Self { connection: RefCell::new(connection), capabilities: SqliteCapabilities::read_write() })
    }

    /// Opens or creates a file-backed database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, SqliteError> {
        Self::open_with_options(path, OpenOptions::default())
    }

    /// Opens a database using explicit contract open options.
    pub fn open_with_options(path: impl AsRef<Path>, options: OpenOptions) -> Result<Self, SqliteError> {
        let path = path.as_ref();
        if path.as_os_str() == ":memory:" {
            if options.read_only {
                return Err(SqliteError::policy("in-memory SQLite databases cannot be opened read-only"));
            }
            return Self::open_in_memory();
        }

        let connection = if options.read_only {
            Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY).map_err(map_sqlite)?
        }
        else {
            Connection::open(path).map_err(map_sqlite)?
        };
        let capabilities = if options.read_only { SqliteCapabilities::read_only() } else { SqliteCapabilities::read_write() };
        Ok(Self { connection: RefCell::new(connection), capabilities })
    }

    fn map_value(value: ValueRef<'_>) -> SqliteValue {
        match value {
            ValueRef::Null => SqliteValue::Null,
            ValueRef::Integer(value) => SqliteValue::Integer(value),
            ValueRef::Real(value) => SqliteValue::Real(value),
            ValueRef::Text(value) => SqliteValue::Text(value.to_vec()),
            ValueRef::Blob(value) => SqliteValue::Blob(value.to_vec()),
        }
    }
}

impl SqliteProvider for RusqliteProvider {
    fn capabilities(&self) -> &SqliteCapabilities {
        &self.capabilities
    }

    fn sqlite_version(&self) -> Result<String, SqliteError> {
        self.connection
            .borrow()
            .query_row("SELECT sqlite_version()", [], |row| row.get(0))
            .map_err(map_sqlite)
    }

    fn source_id(&self) -> Result<String, SqliteError> {
        self.connection
            .borrow()
            .query_row("SELECT sqlite_source_id()", [], |row| row.get(0))
            .map_err(map_sqlite)
    }

    fn execute_one(&self, sql: &str, params: &[SqliteValue]) -> Result<StatementResult, SqliteError> {
        let connection = self.connection.borrow();
        let mut statement = connection.prepare(sql).map_err(|error| map_sqlite_stmt(error, sql))?;
        let columns = statement.column_names().iter().map(|name| (*name).to_string()).collect::<Vec<_>>();
        let bind = params.iter().cloned().map(BindValue).collect::<Vec<_>>();
        let mut rows = Vec::new();
        if columns.is_empty() {
            statement.execute(params_from_iter(bind)).map_err(|error| map_sqlite_stmt(error, sql))?;
        }
        else {
            let mut cursor = statement.query(params_from_iter(bind)).map_err(|error| map_sqlite_stmt(error, sql))?;
            while let Some(row) = cursor.next().map_err(|error| map_sqlite_stmt(error, sql))? {
                let mut values = Vec::with_capacity(columns.len());
                for index in 0..columns.len() {
                    values.push(Self::map_value(row.get_ref(index).map_err(|error| map_sqlite_stmt(error, sql))?));
                }
                rows.push(values);
            }
        }

        Ok(StatementResult {
            columns,
            rows,
            changes: connection.changes(),
            last_insert_rowid: connection.last_insert_rowid(),
        })
    }

    fn execute_batch(&self, sql: &str) -> Result<(), SqliteError> {
        self.connection.borrow().execute_batch(sql).map_err(|error| map_sqlite_stmt(error, sql))
    }

    fn begin_immediate(&self) -> Result<(), SqliteError> {
        self.connection.borrow().execute_batch("BEGIN IMMEDIATE").map_err(map_sqlite)
    }

    fn commit(&self) -> Result<(), SqliteError> {
        self.connection.borrow().execute_batch("COMMIT").map_err(map_sqlite)
    }

    fn rollback(&self) -> Result<(), SqliteError> {
        self.connection.borrow().execute_batch("ROLLBACK").map_err(map_sqlite)
    }

    fn savepoint(&self, name: &str) -> Result<(), SqliteError> {
        let quoted = quote_savepoint(name)?;
        self.connection.borrow().execute_batch(&format!("SAVEPOINT {quoted}")).map_err(map_sqlite)
    }

    fn release_savepoint(&self, name: &str) -> Result<(), SqliteError> {
        let quoted = quote_savepoint(name)?;
        self.connection.borrow().execute_batch(&format!("RELEASE SAVEPOINT {quoted}")).map_err(map_sqlite)
    }

    fn rollback_to_savepoint(&self, name: &str) -> Result<(), SqliteError> {
        let quoted = quote_savepoint(name)?;
        self.connection.borrow().execute_batch(&format!("ROLLBACK TO SAVEPOINT {quoted}")).map_err(map_sqlite)
    }

    fn is_autocommit(&self) -> Result<bool, SqliteError> {
        Ok(self.connection.borrow().is_autocommit())
    }

    fn journal_mode(&self) -> Result<sqlite_provider::JournalMode, SqliteError> {
        let mode: String = self.connection.borrow().query_row("PRAGMA journal_mode", [], |row| row.get(0)).map_err(map_sqlite)?;
        Ok(match mode.to_ascii_lowercase().as_str() {
            "delete" => sqlite_provider::JournalMode::Delete,
            "wal" => sqlite_provider::JournalMode::Wal,
            _ => sqlite_provider::JournalMode::Other,
        })
    }

    fn checkpoint(&self) -> Result<(), SqliteError> {
        self.connection.borrow().execute_batch("PRAGMA wal_checkpoint(PASSIVE)").map_err(map_sqlite)
    }

    fn inspect_catalog(&self) -> Result<CatalogSnapshot, SqliteError> {
        let connection = self.connection.borrow();
        let mut tables = Vec::new();
        let mut stmt = connection
            .prepare("SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%' ORDER BY name")
            .map_err(map_sqlite)?;
        let names = stmt
            .query_map([], |row| row.get::<_, String>(0))
            .map_err(map_sqlite)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(map_sqlite)?;
        for name in names {
            let escaped = name.replace('"', "\"\"");
            let pragma = format!("PRAGMA table_info(\"{escaped}\")");
            let mut info = connection.prepare(&pragma).map_err(map_sqlite)?;
            let columns = info
                .query_map([], |row| {
                    Ok(CatalogColumn {
                        name: row.get(1)?,
                        type_name: row.get(2)?,
                        nullable: row.get::<_, i64>(3)? == 0,
                        primary_key: row.get::<_, i64>(5)? > 0,
                    })
                })
                .map_err(map_sqlite)?
                .collect::<Result<Vec<_>, _>>()
                .map_err(map_sqlite)?;
            tables.push(CatalogTable { name, columns });
        }
        Ok(CatalogSnapshot { tables })
    }
}

fn quote_savepoint(name: &str) -> Result<String, SqliteError> {
    if name.is_empty() || name.as_bytes().iter().any(|byte| *byte == 0) {
        return Err(SqliteError::policy("savepoint name must be non-empty and contain no NUL bytes"));
    }
    Ok(format!("\"{}\"", name.replace('"', "\"\"")))
}

struct BindValue(SqliteValue);

impl ToSql for BindValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::Borrowed(match &self.0 {
            SqliteValue::Null => ValueRef::Null,
            SqliteValue::Integer(value) => ValueRef::Integer(*value),
            SqliteValue::Real(value) => ValueRef::Real(*value),
            SqliteValue::Text(value) => ValueRef::Text(value),
            SqliteValue::Blob(value) => ValueRef::Blob(value),
        }))
    }
}

fn map_sqlite(error: rusqlite::Error) -> SqliteError {
    SqliteError::sqlite(error.to_string(), error.sqlite_error_code().map(|code| code as i32), None)
}

fn map_sqlite_stmt(error: rusqlite::Error, statement: &str) -> SqliteError {
    SqliteError::sqlite(error.to_string(), error.sqlite_error_code().map(|code| code as i32), Some(statement.to_string()))
}
