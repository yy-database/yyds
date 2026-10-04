//! Contract conformance tests for the rusqlite reference provider.

use sqlite_provider::{
    CONTRACT_VERSION, OpenOptions, SqliteProvider, SqliteValue,
};
use sqlite_provider_rusqlite::RusqliteProvider;

#[test]
fn contract_capabilities_and_engine_identity() {
    let provider = RusqliteProvider::open_in_memory().expect("open");
    let caps = provider.capabilities();
    assert_eq!(caps.contract_version, CONTRACT_VERSION);
    assert!(caps.parameterized_statements);
    assert!(caps.batch_statements);
    assert!(caps.catalog_inspection);
    assert!(caps.transactions.begin_immediate);

    let version = provider.sqlite_version().expect("version");
    assert!(!version.is_empty());
    let source_id = provider.source_id().expect("source id");
    assert!(!source_id.is_empty());
}

#[test]
fn typed_values_transaction_and_catalog() {
    let mut provider = RusqliteProvider::open_in_memory().expect("open");
    provider
        .execute_batch(
            "CREATE TABLE demo (
                id INTEGER PRIMARY KEY,
                active INTEGER NOT NULL,
                payload BLOB
             );",
        )
        .expect("ddl");

    provider.begin_immediate().expect("begin");
    let inserted = provider
        .execute_one(
            "INSERT INTO demo(id, active, payload) VALUES (?1, ?2, ?3) RETURNING id, active, payload",
            &[
                SqliteValue::Integer(7),
                SqliteValue::Integer(1),
                SqliteValue::Blob(vec![0xDE, 0xAD]),
            ],
        )
        .expect("insert");
    assert_eq!(inserted.rows[0][0], SqliteValue::Integer(7));
    assert_eq!(inserted.rows[0][2], SqliteValue::Blob(vec![0xDE, 0xAD]));
    provider.commit().expect("commit");

    let catalog = provider.inspect_catalog().expect("catalog");
    let table = catalog.table("demo").expect("demo table");
    assert_eq!(table.columns.len(), 3);
    assert!(table.columns[0].primary_key);
}

#[test]
fn read_only_open_rejects_mutating_batch() {
    let path = std::env::temp_dir().join(format!(
        "sqlite-provider-ro-{}.db",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    {
        let mut writer = RusqliteProvider::open(&path).expect("create");
        writer.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY)").expect("ddl");
    }

    let mut reader = RusqliteProvider::open_with_options(&path, OpenOptions { read_only: true }).expect("read only");
    let err = reader.execute_batch("INSERT INTO t(id) VALUES (1)").expect_err("must reject write");
    assert!(err.message.contains("read-only") || err.message.contains("readonly"));
    let _ = std::fs::remove_file(path);
}
