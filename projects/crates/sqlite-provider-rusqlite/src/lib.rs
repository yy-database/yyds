//! Rusqlite reference implementation of the versioned SQLite provider contract.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

use std::path::Path;

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
    connection: Connection,
    capabilities: SqliteCapabilities,
}

impl RusqliteProvider {
    /// Opens a private in-memory database.
    pub fn open_in_memory() -> Result<Self, SqliteError> {
        let connection = Connection::open_in_memory().map_err(map_sqlite)?;
        Ok(Self { connection, capabilities: SqliteCapabilities::read_write() })
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
        Ok(Self { connection, capabilities })
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
            .query_row("SELECT sqlite_version()", [], |row| row.get(0))
            .map_err(map_sqlite)
    }

    fn source_id(&self) -> Result<String, SqliteError> {
        self.connection
            .query_row("SELECT sqlite_source_id()", [], |row| row.get(0))
            .map_err(map_sqlite)
    }

    fn execute_one(&mut self, sql: &str, params: &[SqliteValue]) -> Result<StatementResult, SqliteError> {
        if self.capabilities.read_only && !sql.trim_start().to_ascii_uppercase().starts_with("SELECT") {
            return Err(SqliteError::unsupported("read-only SQLite provider rejected a mutating statement"));
        }

        let mut statement = self.connection.prepare(sql).map_err(|error| map_sqlite_stmt(error, sql))?;
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
            changes: self.connection.changes(),
            last_insert_rowid: self.connection.last_insert_rowid(),
        })
    }

    fn execute_batch(&mut self, sql: &str) -> Result<(), SqliteError> {
        if self.capabilities.read_only {
            return Err(SqliteError::unsupported("read-only SQLite provider rejected execute_batch"));
        }
        self.connection.execute_batch(sql).map_err(|error| map_sqlite_stmt(error, sql))
    }

    fn begin_immediate(&mut self) -> Result<(), SqliteError> {
        if self.capabilities.read_only {
            return Err(SqliteError::unsupported("read-only SQLite provider rejected begin_immediate"));
        }
        self.connection.execute_batch("BEGIN IMMEDIATE").map_err(map_sqlite)
    }

    fn commit(&mut self) -> Result<(), SqliteError> {
        self.connection.execute_batch("COMMIT").map_err(map_sqlite)
    }

    fn rollback(&mut self) -> Result<(), SqliteError> {
        self.connection.execute_batch("ROLLBACK").map_err(map_sqlite)
    }

    fn inspect_catalog(&self) -> Result<CatalogSnapshot, SqliteError> {
        let mut tables = Vec::new();
        let mut stmt = self
            .connection
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
            let mut info = self.connection.prepare(&pragma).map_err(map_sqlite)?;
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
