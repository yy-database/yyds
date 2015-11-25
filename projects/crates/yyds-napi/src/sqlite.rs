#![allow(non_snake_case)]

use std::{path::PathBuf, sync::Arc};

use napi::bindgen_prelude::{BigInt, Buffer};
use napi_derive::napi;
use yyds_catalog::Catalog;
use yyds_execution::KeyValueExecutor;
use yyds_gateway::{bind_sql_catalog, execute_plan, plan_statement, SqlCatalog, SqlExecutionPlan, SqlValue};
use yyds_sqlite::{
    blank_database, decode_library_version, decode_record, read_existing, read_named_table, read_schema, validate_database,
    RecordLimits, RecordValue, SchemaObjectKind, TableLimits, TextEncoding,
};

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

/// One typed, decoded SQLite record field.
///
/// `kind` is the SQLite storage class (`null`, `integer`, `real`, `text`, or `blob`).
/// Exactly one of the `value_*` fields is populated per instance; `null` has no value.
#[napi(object)]
pub struct SqliteRecordValue {
    /// SQLite storage-class label.
    pub kind: String,
    /// Integer 64-bit signed value, present only when `kind == "integer"`.
    pub value_integer: Option<BigInt>,
    /// IEEE-754 binary64 value, present only when `kind == "real"`.
    pub value_real: Option<f64>,
    /// Decoded text string, present only when `kind == "text"`.
    pub value_text: Option<String>,
    /// Opaque blob bytes, present only when `kind == "blob"`.
    pub value_blob: Option<Buffer>,
}

/// A decoded SQLite row: rowid plus typed column values.
#[napi(object)]
pub struct SqliteDecodedRow {
    /// Exact signed 64-bit SQLite rowid.
    pub rowid: BigInt,
    /// Decoded storage-class columns in physical record order.
    pub columns: Vec<SqliteRecordValue>,
}

fn encode_encoding(name: Option<&str>) -> napi::Result<TextEncoding> {
    match name {
        None | Some("utf8") | Some("utf-8") | Some("UTF-8") => Ok(TextEncoding::Utf8),
        Some("utf16le") | Some("utf-16le") | Some("UTF-16LE") => Ok(TextEncoding::Utf16Le),
        Some("utf16be") | Some("utf-16be") | Some("UTF-16BE") => Ok(TextEncoding::Utf16Be),
        Some(other) => Err(napi::Error::from_reason(format!(
            "unsupported SQLite text encoding {other}; expected utf8, utf16le, or utf16be"
        ))),
    }
}

fn detect_encoding(bytes: &[u8]) -> napi::Result<TextEncoding> {
    if bytes.len() < 64 {
        return Err(napi::Error::from_reason("sqlite snapshot has no text encoding"));
    }
    match u32::from_be_bytes(bytes[56..60].try_into().unwrap()) {
        1 => Ok(TextEncoding::Utf8),
        2 => Ok(TextEncoding::Utf16Le),
        3 => Ok(TextEncoding::Utf16Be),
        _ => Err(napi::Error::from_reason("sqlite snapshot has unknown text encoding")),
    }
}

fn record_value(value: RecordValue<'_>) -> SqliteRecordValue {
    match value {
        RecordValue::Null => {
            SqliteRecordValue { kind: "null".into(), value_integer: None, value_real: None, value_text: None, value_blob: None }
        }
        RecordValue::Integer(int) => SqliteRecordValue {
            kind: "integer".into(),
            value_integer: Some(BigInt::from(int)),
            value_real: None,
            value_text: None,
            value_blob: None,
        },
        RecordValue::Real(real) => SqliteRecordValue {
            kind: "real".into(),
            value_integer: None,
            value_real: Some(real),
            value_text: None,
            value_blob: None,
        },
        RecordValue::Text(text) => SqliteRecordValue {
            kind: "text".into(),
            value_integer: None,
            value_real: None,
            value_text: Some(text),
            value_blob: None,
        },
        RecordValue::Blob(blob) => SqliteRecordValue {
            kind: "blob".into(),
            value_integer: None,
            value_real: None,
            value_text: None,
            value_blob: Some(blob.to_vec().into()),
        },
    }
}

fn resolve_encoding_name(bytes: &[u8]) -> napi::Result<&'static str> {
    if bytes.len() < 64 {
        return Ok("utf8");
    }
    Ok(match u32::from_be_bytes(bytes[56..60].try_into().unwrap()) {
        1 => "utf8",
        2 => "utf16le",
        3 => "utf16be",
        _ => return Err(napi::Error::from_reason("sqlite snapshot has unknown text encoding")),
    })
}

fn build_decoded_rows(
    bytes: &[u8],
    rows: Vec<yyds_sqlite::TableRow>,
    limits: TableLimits,
) -> napi::Result<Vec<SqliteDecodedRow>> {
    let encoding = detect_encoding(bytes)?;
    let record_limits = RecordLimits { max_columns: 1024, max_payload_bytes: limits.max_payload_bytes.max(1_048_576) };
    rows.into_iter()
        .map(|row| {
            decode_record(&row.payload, encoding, record_limits).map_err(native_error).map(|columns| SqliteDecodedRow {
                rowid: BigInt::from(row.rowid),
                columns: columns.into_iter().map(record_value).collect(),
            })
        })
        .collect()
}

/// Owned, read-only main-file bytes, independent of the YYDS cluster runtime.
#[napi]
pub struct SqliteSnapshot {
    bytes: Vec<u8>,
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

    /// Reads decoded typed records from an ordinary rowid table.
    #[napi]
    pub fn tableDecoded(&self, name: String, limits: Option<SqliteReadLimits>) -> napi::Result<Vec<SqliteDecodedRow>> {
        let resolved = SqliteReadLimits::resolve(limits);
        let rows = read_named_table(&self.bytes, &name, resolved).map_err(native_error)?;
        build_decoded_rows(&self.bytes, rows, resolved)
    }

    /// SQLite library version stamped in the snapshot header.
    #[napi]
    pub fn sqliteVersion(&self) -> napi::Result<String> {
        decode_library_version(&self.bytes).map_err(native_error)
    }

    /// Returns the decoded text encoding label (`utf8`, `utf16le`, or `utf16be`).
    #[napi]
    pub fn encoding(&self) -> napi::Result<String> {
        resolve_encoding_name(&self.bytes).map(|s| s.to_owned())
    }

    /// Health probe that re-validates the main-file format.
    #[napi]
    pub fn ping(&self) -> napi::Result<i32> {
        validate_database(&self.bytes).map_err(native_error)?;
        Ok(1)
    }

    /// Returns a view over the raw main-file bytes.
    #[napi]
    pub fn pages(&self) -> napi::Result<Buffer> {
        Ok(self.bytes.clone().into())
    }
}

/// A file-backed or in-memory SQLite snapshot handle.
///
/// This does not recover WAL or journal state. Without a bound SQL runtime,
/// SqliteDatabase is read-only and never executes SQL. Bind an executor + catalog
/// via [`Self::bindSqlRuntime`] to enable SELECT + INSERT/UPDATE/DELETE through
/// the YYDS SQL planner and KV execution plane.
#[napi]
pub struct SqliteDatabase {
    bytes: Vec<u8>,
    path: Option<PathBuf>,
    runtime: Option<Arc<SqliteRuntime>>,
}

#[napi]
impl SqliteDatabase {
    /// Opens a blank in-memory SQLite format-3 snapshot.
    #[napi(factory)]
    pub fn openInMemory() -> napi::Result<Self> {
        let bytes = blank_database().map_err(native_error)?;
        Ok(Self { bytes, path: None, runtime: None })
    }

    /// Opens or creates a file-backed SQLite snapshot.
    #[napi(factory)]
    pub fn open(path: String) -> napi::Result<Self> {
        let path_buf = PathBuf::from(&path);
        if path_buf.exists() {
            let bytes = read_existing(&path).map_err(native_error)?;
            return Ok(Self { bytes, path: Some(path_buf), runtime: None });
        }
        let bytes = blank_database().map_err(native_error)?;
        Ok(Self { bytes, path: Some(path_buf), runtime: None })
    }

    /// File path when file-backed, otherwise `null`.
    #[napi]
    pub fn path(&self) -> Option<String> {
        self.path.as_ref().map(|p| p.display().to_string())
    }

    /// Health probe that re-validates the main-file format.
    #[napi]
    pub fn ping(&self) -> napi::Result<i32> {
        validate_database(&self.bytes).map_err(native_error)?;
        Ok(1)
    }

    /// SQLite library version stamped in the snapshot header.
    #[napi]
    pub fn sqliteVersion(&self) -> napi::Result<String> {
        decode_library_version(&self.bytes).map_err(native_error)
    }

    /// Returns the decoded text encoding label (`utf8`, `utf16le`, or `utf16be`).
    #[napi]
    pub fn encoding(&self) -> napi::Result<String> {
        resolve_encoding_name(&self.bytes).map(|s| s.to_owned())
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

    /// Reads decoded typed records from an ordinary rowid table.
    #[napi]
    pub fn table(&self, name: String, limits: Option<SqliteReadLimits>) -> napi::Result<Vec<SqliteDecodedRow>> {
        let resolved = SqliteReadLimits::resolve(limits);
        let rows = read_named_table(&self.bytes, &name, resolved).map_err(native_error)?;
        build_decoded_rows(&self.bytes, rows, resolved)
    }

    /// Reads raw records (undecoded payload) from an ordinary rowid table.
    #[napi]
    pub fn tableRaw(&self, name: String, limits: Option<SqliteReadLimits>) -> napi::Result<Vec<SqliteTableRow>> {
        read_named_table(&self.bytes, &name, SqliteReadLimits::resolve(limits))
            .map(|rows| {
                rows.into_iter()
                    .map(|row| SqliteTableRow { rowid: BigInt::from(row.rowid), payload: row.payload.into() })
                    .collect()
            })
            .map_err(native_error)
    }

    /// Returns a view over the raw main-file bytes.
    #[napi]
    pub fn pages(&self) -> napi::Result<Buffer> {
        Ok(self.bytes.clone().into())
    }

    /// Binds a YYDS executor + catalog, enabling SELECT + INSERT/UPDATE/DELETE
    /// through the YYDS SQL planner and KV execution plane.
    ///
    /// `data_dir` 是 YYDS node 数据目录（含 local-0.yykv）。`catalog_path` 是
    /// `.yyds` catalog 文件路径。两者通常由同一个 YYDS node 产生。
    #[napi]
    pub fn bindSqlRuntime(&mut self, data_dir: String, catalog_path: String) -> napi::Result<()> {
        let executor = crate::node_runtime::open_executor(PathBuf::from(&data_dir).as_path(), Some(PathBuf::from(&catalog_path).as_path()))
            .map_err(|msg| napi::Error::from_reason(format!("open executor: {msg}")))?;
        let yyds_catalog =
            Catalog::open(&catalog_path).map_err(|msg| napi::Error::from_reason(format!("open catalog: {msg}")))?;
        let cat = SqlCatalog::from_yyds_catalog(&yyds_catalog)
            .map_err(|error| napi::Error::from_reason(format!("invalid YYDS catalog: {}", error.message)))?;
        self.runtime = Some(Arc::new(SqliteRuntime { executor, catalog: cat }));
        Ok(())
    }

    /// Executes one SQL statement through the YYDS planner.
    ///
    /// Only `WHERE pk = literal` predicates are supported. INSERT VALUES must
    /// start with the primary key literal. Requires a runtime bound via
    /// [`Self::bindSqlRuntime`].
    #[napi]
    pub fn execute(&self, sql: String) -> napi::Result<SqliteExecuteResult> {
        let runtime = self
            .runtime
            .as_ref()
            .ok_or_else(|| napi::Error::from_reason("SqliteDatabase has no SQL runtime bound; call bindSqlRuntime first"))?;

        let bound = bind_sql_catalog(&sql, &runtime.catalog, runtime.catalog.schema_version())
            .map_err(|error| napi::Error::from_reason(format!("SQL bind error: {}", error.message)))?;
        let plan = plan_statement(bound, &runtime.catalog)
            .map_err(|error| napi::Error::from_reason(format!("SQL plan error: {}", error.message)))?;

        let is_read = matches!(&plan, SqlExecutionPlan::Read(_) | SqlExecutionPlan::Scan(_));
        let result = execute_plan(runtime.executor.as_ref(), &plan)
            .map_err(|error| napi::Error::from_reason(format!("SQL execution error: {}", error.message)))?;

        if is_read {
            Ok(SqliteExecuteResult {
                columns: result.columns,
                rows: result.rows.into_iter().map(|row| row.into_iter().map(sql_value_to_record).collect()).collect(),
                changes: BigInt::from(0i64),
            })
        }
        else {
            Ok(SqliteExecuteResult { columns: Vec::new(), rows: Vec::new(), changes: BigInt::from(1i64) })
        }
    }
}

/// Decodes one SQLite record payload into typed storage-class values.
///
/// `encoding` defaults to `utf8`. Use the database header encoding when available
/// (see `SqliteDatabase.encoding()` or `SqliteSnapshot.encoding()`).
#[napi]
pub fn decodeRecord(payload: Buffer, encoding: Option<String>) -> napi::Result<Vec<SqliteRecordValue>> {
    let encoding = encode_encoding(encoding.as_deref())?;
    let values =
        decode_record(&payload, encoding, RecordLimits { max_columns: 1024, max_payload_bytes: payload.len().max(1_048_576) })
            .map_err(native_error)?;
    Ok(values.into_iter().map(record_value).collect())
}

fn native_error(error: yyds_types::Error) -> napi::Error {
    napi::Error::from_reason(error.to_string())
}

/// SQL 执行结果：SELECT 返回列+行，DML 返回 changes。
#[napi(object)]
pub struct SqliteExecuteResult {
    /// SELECT 返回：列名；DML 返回空数组。
    pub columns: Vec<String>,
    /// SELECT 返回：按 columns 顺序的行值；DML 返回空数组。
    pub rows: Vec<Vec<SqliteRecordValue>>,
    /// INSERT/UPDATE/DELETE 影响行数；SELECT 为 0。
    pub changes: BigInt,
}

/// 注入到 SqliteDatabase 的 YYDS SQL 执行 runtime。
struct SqliteRuntime {
    executor: Arc<dyn KeyValueExecutor>,
    catalog: SqlCatalog,
}

fn sql_value_to_record(value: SqlValue) -> SqliteRecordValue {
    match value {
        SqlValue::Null => {
            SqliteRecordValue { kind: "null".into(), value_integer: None, value_real: None, value_text: None, value_blob: None }
        }
        SqlValue::Number(n) => {
            if n.fract() == 0.0 {
                SqliteRecordValue {
                    kind: "integer".into(),
                    value_integer: Some(BigInt::from(n as i64)),
                    value_real: None,
                    value_text: None,
                    value_blob: None,
                }
            }
            else {
                SqliteRecordValue {
                    kind: "real".into(),
                    value_integer: None,
                    value_real: Some(n),
                    value_text: None,
                    value_blob: None,
                }
            }
        }
        SqlValue::String(s) => SqliteRecordValue {
            kind: "text".into(),
            value_integer: None,
            value_real: None,
            value_text: Some(s),
            value_blob: None,
        },
        SqlValue::Boolean(b) => SqliteRecordValue {
            kind: "integer".into(),
            value_integer: Some(BigInt::from(if b { 1i64 } else { 0 })),
            value_real: None,
            value_text: None,
            value_blob: None,
        },
    }
}
