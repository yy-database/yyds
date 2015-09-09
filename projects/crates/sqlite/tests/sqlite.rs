use yyds_sqlite::{
    BACKEND_ID, CONSTRAINT, MAGIC, SqliteDatabase, WHY_NOT_YY_OPTIMIZED, blank_database, decode_library_version, read_existing,
    sqlite_library_version, validate_database,
};

#[test]
fn sqlite_backend_metadata() {
    assert_eq!(BACKEND_ID, "sqlite");
    assert!(!CONSTRAINT.is_empty());
    assert!(WHY_NOT_YY_OPTIMIZED.contains("binary"));
}

#[test]
fn sqlite_blank_database_is_valid_format3() {
    let pages = blank_database().expect("blank");
    assert_eq!(pages.len(), 4096);
    assert_eq!(pages.get(..MAGIC.len()), Some(MAGIC));
    assert_eq!(validate_database(&pages).expect("validate"), 4096);
    assert_eq!(decode_library_version(&pages).expect("version"), rusqlite::version());
}

#[test]
fn reported_library_version_is_the_linked_sqlite_version() {
    assert_eq!(sqlite_library_version(), rusqlite::version());
}

#[test]
fn sqlite_live_ping_and_version() {
    let db = SqliteDatabase::open_in_memory().expect("open");
    assert_eq!(db.page_size(), 4096);
    assert_eq!(db.ping().expect("ping"), 1);
    assert_eq!(db.sqlite_version().expect("version"), rusqlite::version());
}

#[test]
fn sqlite_file_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("probe.sqlite");
    let db = SqliteDatabase::open(&path).expect("open file");
    assert_eq!(db.path(), Some(path.as_path()));
    assert_eq!(db.ping().expect("ping"), 1);

    let bytes = read_existing(&path).expect("read back");
    assert_eq!(bytes.len(), 4096);
    assert_eq!(decode_library_version(&bytes).expect("version"), rusqlite::version());
}

#[test]
fn sqlite_drop_preserves_external_changes() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("probe.sqlite");
    let db = SqliteDatabase::open(&path).expect("open file");
    let replacement = b"external writer owns these bytes";
    std::fs::write(&path, replacement).expect("external write");
    drop(db);
    assert_eq!(std::fs::read(&path).expect("read"), replacement);
}

#[test]
fn sqlite_file_flush_rejects_snapshot_write() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("probe.sqlite");
    let db = SqliteDatabase::open(&path).expect("open file");
    let original = std::fs::read(&path).expect("read");
    assert!(matches!(db.flush(), Err(yyds_types::Error::Unsupported(_))));
    drop(db);
    assert_eq!(std::fs::read(&path).expect("read"), original);
}

#[test]
fn upstream_engine_executes_sql_and_preserves_storage_classes() {
    use yyds_sqlite::{SqliteEngine, SqliteValue};

    let database = SqliteEngine::open_in_memory().unwrap();
    database
        .execute(
            "CREATE TABLE values_probe (integer_value INTEGER, real_value REAL, text_value TEXT, blob_value BLOB, null_value)",
        )
        .unwrap();
    database.execute("INSERT INTO values_probe VALUES (-9223372036854775808, 1.25, 'hello', X'00ff', NULL)").unwrap();
    let result =
        database.execute("SELECT integer_value, real_value, text_value, blob_value, null_value FROM values_probe").unwrap();
    assert_eq!(result.columns, ["integer_value", "real_value", "text_value", "blob_value", "null_value"]);
    assert_eq!(
        result.rows,
        vec![vec![
            SqliteValue::Integer(i64::MIN),
            SqliteValue::Real(1.25),
            SqliteValue::Text(b"hello".to_vec()),
            SqliteValue::Blob(vec![0, 255]),
            SqliteValue::Null,
        ]]
    );
    assert_eq!(database.execute("SELECT sqlite_version()").unwrap().rows[0][0].to_string(), rusqlite::version());
    assert!(database.execute("SELECT FROM").is_err());
    assert!(database.execute("SELECT 1; SELECT 2").is_err());
}

#[test]
fn upstream_engine_creates_binary_compatible_database_files() {
    use yyds_sqlite::SqliteEngine;

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("engine.sqlite");
    {
        let database = SqliteEngine::open(&path).unwrap();
        database.execute("PRAGMA journal_mode=WAL").unwrap();
        database.execute("CREATE TABLE sample (id INTEGER PRIMARY KEY, value BLOB)").unwrap();
        database.execute("INSERT INTO sample(value) VALUES (X'00ff')").unwrap();
        let row = database.execute("SELECT id, value FROM sample").unwrap();
        assert_eq!(row.rows[0][1].to_string(), "x'00ff'");
    }
    assert_eq!(std::fs::read(path).unwrap().get(..16), Some(&b"SQLite format 3\0"[..]));
}

#[test]
fn upstream_engine_read_only_open_never_creates_or_modifies_database() {
    use yyds_sqlite::SqliteEngine;

    let directory = tempfile::tempdir().unwrap();
    let missing = directory.path().join("missing.sqlite");
    assert!(SqliteEngine::open_read_only(&missing).is_err());
    assert!(!missing.exists());

    let path = directory.path().join("readonly.sqlite");
    {
        let database = SqliteEngine::open(&path).unwrap();
        database.execute("CREATE TABLE sample (value TEXT)").unwrap();
        database.execute("INSERT INTO sample VALUES ('kept')").unwrap();
    }
    let before = std::fs::read(&path).unwrap();
    let database = SqliteEngine::open_read_only(&path).unwrap();
    assert_eq!(database.execute("SELECT value FROM sample").unwrap().rows[0][0].to_string(), "kept");
    assert!(database.execute("INSERT INTO sample VALUES ('blocked')").is_err());
    drop(database);
    assert_eq!(std::fs::read(&path).unwrap(), before);
}
