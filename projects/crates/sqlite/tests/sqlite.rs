use yyds_sqlite::{
    BACKEND_ID, CONSTRAINT, ENGINE_LIBRARY_VERSION, MAGIC, SqliteDatabase, WHY_NOT_YY_OPTIMIZED, blank_database,
    decode_library_version, handle_sql, read_existing, validate_database,
};

#[test]
fn sqlite_backend_metadata() {
    assert_eq!(BACKEND_ID, "sqlite");
    assert!(!CONSTRAINT.is_empty());
    assert!(WHY_NOT_YY_OPTIMIZED.contains("binary"));
}

#[test]
fn sqlite_disguise_select_one() {
    let lines = handle_sql("SELECT 1").expect("select");
    assert_eq!(lines, vec!["1"]);
}

#[test]
fn sqlite_disguise_select_version() {
    let lines = handle_sql("SELECT sqlite_version()").expect("version");
    assert_eq!(lines, vec![ENGINE_LIBRARY_VERSION]);
}

#[test]
fn sqlite_blank_database_is_valid_format3() {
    let pages = blank_database().expect("blank");
    assert_eq!(pages.len(), 4096);
    assert_eq!(pages.get(..MAGIC.len()), Some(MAGIC));
    assert_eq!(validate_database(&pages).expect("validate"), 4096);
    assert_eq!(decode_library_version(&pages).expect("version"), ENGINE_LIBRARY_VERSION);
}

#[test]
fn sqlite_live_ping_and_version() {
    let db = SqliteDatabase::open_in_memory().expect("open");
    assert_eq!(db.page_size(), 4096);
    assert_eq!(db.ping().expect("ping"), 1);
    assert_eq!(db.sqlite_version().expect("version"), ENGINE_LIBRARY_VERSION);
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
    assert_eq!(decode_library_version(&bytes).expect("version"), ENGINE_LIBRARY_VERSION);
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
