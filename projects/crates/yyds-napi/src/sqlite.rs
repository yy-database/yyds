use napi::bindgen_prelude::{BigInt, Buffer};
use napi_derive::napi;
use yyds_sqlite::{SchemaObjectKind, TableLimits, read_named_table, read_schema, validate_database};

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
