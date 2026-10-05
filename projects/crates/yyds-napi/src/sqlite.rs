use napi::bindgen_prelude::{BigInt, Buffer};
use napi_derive::napi;
use yyds_sqlite::{SchemaObjectKind, SqliteEngine, SqliteProvider, SqliteValue, TableLimits, read_named_table, read_schema, validate_database};

/// Optional resource bounds for each schema or table scan.
#[napi(object)]
pub struct SqliteReadLimits {
    /// Maximum b-tree and overflow pages visited.
    pub max_pages: Option<u32>,
    /// Maximum rows materialized.
    pub max_rows: Option<u32>,
    /// Maximum payload bytes per record.
    pub max_payload_bytes: Option<u32>,
    /// Maximum total record payload bytes per scan.
    pub max_total_payload_bytes: Option<u32>,
}

impl SqliteReadLimits {
    fn resolve(limits: Option<Self>) -> TableLimits {
        let default = TableLimits::default();
        match limits {
            None => default,
            Some(limits) => TableLimits {
                max_pages: limits.max_pages.map_or(default.max_pages, |value| value as usize),
                max_rows: limits.max_rows.map_or(default.max_rows, |value| value as usize),
                max_payload_bytes: limits.max_payload_bytes.map_or(default.max_payload_bytes, |value| value as usize),
                max_total_payload_bytes: limits
                    .max_total_payload_bytes
                    .map_or(default.max_total_payload_bytes, |value| value as usize),
            },
        }
    }
}

/// The five SQLite schema columns, with SQL retained as opaque source.
#[napi(object)]
pub struct SqliteSchemaObject {
    /// SQLite object type: table, index, view, or trigger.
    pub kind: String,
    /// Stored object name.
    pub name: String,
    /// Associated table or view name.
    pub table_name: String,
    /// Root page, absent for SQL NULL.
    pub root_page: Option<u32>,
    /// CREATE source, absent for SQL NULL.
    pub sql: Option<String>,
}

/// An undecoded SQLite table record with lossless rowid and binary payload.
#[napi(object)]
pub struct SqliteTableRow {
    /// Exact signed 64-bit SQLite rowid.
    pub rowid: BigInt,
    /// Record bytes after overflow assembly.
    pub payload: Buffer,
}

/// Owned, read-only main-file bytes, independent of the YYDS cluster runtime.
#[napi]
pub struct SqliteSnapshot {
    bytes: Vec<u8>,
}

/// A SQLite query result value with its native storage class.
#[napi(object)]
pub struct SqliteResultValue {
    /// SQLite storage class: null, integer, real, text, or blob.
    pub kind: String,
    /// Decimal signed integer when `kind` is integer.
    pub integer: Option<String>,
    /// Floating point value when `kind` is real.
    pub real: Option<f64>,
    /// Text value when `kind` is text.
    pub text: Option<String>,
    /// Lossless SQLite text bytes when the value is not valid UTF-8.
    pub text_bytes: Option<Buffer>,
    /// Binary value when `kind` is blob.
    pub blob: Option<Buffer>,
}

/// Result of executing one SQLite statement.
#[napi(object)]
pub struct SqliteQueryResult {
    /// Column labels in result order.
    pub columns: Vec<String>,
    /// Rows with lossless integer and binary values.
    pub rows: Vec<Vec<SqliteResultValue>>,
    /// Changed row count.
    pub changes: String,
    /// Last insert rowid as exact signed decimal text.
    pub last_insert_rowid: String,
}

/// One SQLite bind parameter preserving its storage class and raw bytes.
#[napi(object)]
pub struct SqliteParameter {
    /// SQLite storage class: null, integer, real, text, or blob.
    pub kind: String,
    /// Signed 64-bit integer as decimal text.
    pub integer: Option<String>,
    /// Floating-point value.
    pub real: Option<f64>,
    /// Exact TEXT bytes, including non-UTF-8 data.
    pub text: Option<Buffer>,
    /// Exact BLOB bytes.
    pub blob: Option<Buffer>,
}

/// A real SQLite connection backed by upstream SQLite, independent of YYDS storage.
#[napi]
pub struct SqliteConnection {
    engine: SqliteEngine,
}

#[napi]
impl SqliteConnection {
    /// Opens or creates a SQLite file, or opens an existing file read-only when requested.
    #[napi(constructor)]
    pub fn new(path: String, read_only: Option<bool>) -> napi::Result<Self> {
        let engine = match (path.as_str(), read_only.unwrap_or(false)) {
            (":memory:", false) => SqliteEngine::open_in_memory(),
            (":memory:", true) => {
                return Err(napi::Error::from_reason("SQLite in-memory databases cannot be opened read-only"));
            }
            (_, true) => SqliteEngine::open_read_only(&path),
            (_, false) => SqliteEngine::open(&path),
        }
        .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(Self { engine })
    }

    /// Executes one statement through the SQLite engine.
    #[napi]
    pub fn execute(&self, sql: String) -> napi::Result<SqliteQueryResult> {
        let result = self.engine.execute(&sql).map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(SqliteQueryResult {
            columns: result.columns,
            rows: result.rows.into_iter().map(|row| row.into_iter().map(result_value).collect()).collect(),
            changes: result.changes.to_string(),
            last_insert_rowid: result.last_insert_rowid.to_string(),
        })
    }

    /// Executes one statement with SQLite-native positional bind parameters.
    #[napi(js_name = "executeWithParameters")]
    pub fn execute_with_parameters(&self, sql: String, parameters: Vec<SqliteParameter>) -> napi::Result<SqliteQueryResult> {
        let parameters = parameters.into_iter().map(parameter_value).collect::<napi::Result<Vec<_>>>()?;
        let result = self
            .engine
            .execute_with_parameters(&sql, &parameters)
            .map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(SqliteQueryResult {
            columns: result.columns,
            rows: result.rows.into_iter().map(|row| row.into_iter().map(result_value).collect()).collect(),
            changes: result.changes.to_string(),
            last_insert_rowid: result.last_insert_rowid.to_string(),
        })
    }

    /// Executes a multi-statement SQLite batch and discards result rows.
    #[napi(js_name = "executeBatch")]
    pub fn execute_batch(&self, sql: String) -> napi::Result<()> {
        self.engine.execute_batch(&sql).map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    /// Starts an immediate write transaction.
    #[napi(js_name = "beginImmediate")]
    pub fn begin_immediate(&self) -> napi::Result<()> {
        self.engine.begin_immediate().map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    /// Commits the active transaction.
    #[napi]
    pub fn commit(&self) -> napi::Result<()> {
        self.engine.commit().map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    /// Rolls back the active transaction.
    #[napi]
    pub fn rollback(&self) -> napi::Result<()> {
        self.engine.rollback().map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    /// Creates a named savepoint.
    #[napi]
    pub fn savepoint(&self, name: String) -> napi::Result<()> {
        self.engine.savepoint(&name).map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    /// Releases a named savepoint.
    #[napi(js_name = "releaseSavepoint")]
    pub fn release_savepoint(&self, name: String) -> napi::Result<()> {
        self.engine.release_savepoint(&name).map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    /// Rolls back to a named savepoint without releasing it.
    #[napi(js_name = "rollbackToSavepoint")]
    pub fn rollback_to_savepoint(&self, name: String) -> napi::Result<()> {
        self.engine.rollback_to_savepoint(&name).map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    /// Reports whether the connection is outside a transaction.
    #[napi(js_name = "isAutocommit")]
    pub fn is_autocommit(&self) -> napi::Result<bool> {
        self.engine.is_autocommit().map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    /// Reports the observed SQLite journal mode.
    #[napi(js_name = "journalMode")]
    pub fn journal_mode(&self) -> napi::Result<String> {
        let mode = self.engine.journal_mode().map_err(|error| napi::Error::from_reason(error.to_string()))?;
        Ok(match mode {
            yyds_sqlite::JournalMode::Delete => "delete",
            yyds_sqlite::JournalMode::Wal => "wal",
            yyds_sqlite::JournalMode::Other => "other",
        }.into())
    }

    /// Performs a passive WAL checkpoint.
    #[napi]
    pub fn checkpoint(&self) -> napi::Result<()> {
        self.engine.checkpoint().map_err(|error| napi::Error::from_reason(error.to_string()))
    }

    /// Returns SQLite's native engine source id.
    #[napi]
    pub fn source_id(&self) -> napi::Result<String> {
        self.engine.source_id().map_err(|error| napi::Error::from_reason(error.to_string()))
    }
}

fn parameter_value(parameter: SqliteParameter) -> napi::Result<SqliteValue> {
    let populated = usize::from(parameter.integer.is_some())
        + usize::from(parameter.real.is_some())
        + usize::from(parameter.text.is_some())
        + usize::from(parameter.blob.is_some());
    let invalid = || napi::Error::from_reason("SQLite parameter fields do not match its storage class");
    match parameter.kind.as_str() {
        "null" if populated == 0 => Ok(SqliteValue::Null),
        "integer" if populated == 1 => parameter
            .integer
            .ok_or_else(invalid)?
            .parse::<i64>()
            .map(SqliteValue::Integer)
            .map_err(|_| napi::Error::from_reason("SQLite integer parameter is outside signed 64-bit range")),
        "real" if populated == 1 => parameter.real.map(SqliteValue::Real).ok_or_else(invalid),
        "text" if populated == 1 => parameter.text.map(|value| SqliteValue::Text(value.to_vec())).ok_or_else(invalid),
        "blob" if populated == 1 => parameter.blob.map(|value| SqliteValue::Blob(value.to_vec())).ok_or_else(invalid),
        _ => Err(invalid()),
    }
}

fn result_value(value: SqliteValue) -> SqliteResultValue {
    match value {
        SqliteValue::Null => {
            SqliteResultValue { kind: "null".into(), integer: None, real: None, text: None, text_bytes: None, blob: None }
        }
        SqliteValue::Integer(value) => SqliteResultValue {
            kind: "integer".into(),
            integer: Some(value.to_string()),
            real: None,
            text: None,
            text_bytes: None,
            blob: None,
        },
        SqliteValue::Real(value) => SqliteResultValue {
            kind: "real".into(),
            integer: None,
            real: Some(value),
            text: None,
            text_bytes: None,
            blob: None,
        },
        SqliteValue::Text(value) => match String::from_utf8(value.clone()) {
            Ok(text) => SqliteResultValue {
                kind: "text".into(),
                integer: None,
                real: None,
                text: Some(text),
                text_bytes: None,
                blob: None,
            },
            Err(_) => SqliteResultValue {
                kind: "text".into(),
                integer: None,
                real: None,
                text: None,
                text_bytes: Some(value.into()),
                blob: None,
            },
        },
        SqliteValue::Blob(value) => SqliteResultValue {
            kind: "blob".into(),
            integer: None,
            real: None,
            text: None,
            text_bytes: None,
            blob: Some(value.into()),
        },
    }
}

#[napi]
impl SqliteSnapshot {
    /// Copies a validated snapshot, bounded to 256 MiB unless overridden.
    #[napi(constructor)]
    pub fn new(bytes: Buffer, max_snapshot_bytes: Option<u32>) -> napi::Result<Self> {
        let maximum = max_snapshot_bytes.unwrap_or(256 * 1024 * 1024) as usize;
        if bytes.len() > maximum {
            return Err(napi::Error::from_reason("sqlite snapshot byte limit exceeded"));
        }
        validate_database(&bytes).map_err(native_error)?;
        let mut snapshot = Vec::new();
        snapshot.try_reserve_exact(bytes.len()).map_err(|_| napi::Error::from_reason("sqlite snapshot allocation failed"))?;
        snapshot.extend_from_slice(&bytes);
        Ok(Self { bytes: snapshot })
    }

    /// Returns typed schema objects without parsing their SQL definitions.
    #[napi]
    pub fn schema(&self, limits: Option<SqliteReadLimits>) -> napi::Result<Vec<SqliteSchemaObject>> {
        read_schema(&self.bytes, SqliteReadLimits::resolve(limits))
            .map(|objects| {
                objects
                    .into_iter()
                    .map(|object| SqliteSchemaObject {
                        kind: match object.kind {
                            SchemaObjectKind::Table => "table",
                            SchemaObjectKind::Index => "index",
                            SchemaObjectKind::View => "view",
                            SchemaObjectKind::Trigger => "trigger",
                        }
                        .into(),
                        name: object.name,
                        table_name: object.table_name,
                        root_page: object.root_page,
                        sql: object.sql,
                    })
                    .collect()
            })
            .map_err(native_error)
    }

    /// Reads raw records from an ordinary rowid table, not a SQL projection.
    #[napi]
    pub fn table(&self, name: String, limits: Option<SqliteReadLimits>) -> napi::Result<Vec<SqliteTableRow>> {
        read_named_table(&self.bytes, &name, SqliteReadLimits::resolve(limits))
            .map(|rows| {
                rows.into_iter()
                    .map(|row| SqliteTableRow { rowid: BigInt::from(row.rowid), payload: row.payload.into() })
                    .collect()
            })
            .map_err(native_error)
    }
}

fn native_error(error: yyds_types::Error) -> napi::Error {
    napi::Error::from_reason(error.to_string())
}
