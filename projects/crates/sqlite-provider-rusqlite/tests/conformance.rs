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

#[test]
fn nested_transaction_and_runtime_journal_contract_are_real() {
    let provider = RusqliteProvider::open_in_memory().expect("open");
    assert!(provider.is_autocommit().expect("autocommit"));
    provider.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY, value TEXT)").expect("ddl");
    provider.begin_immediate().expect("begin");
    assert!(!provider.is_autocommit().expect("transaction active"));
    provider.savepoint("user.savepoint").expect("savepoint");
    provider.execute_one("INSERT INTO t(id, value) VALUES (?1, ?2)", &[SqliteValue::Integer(1), SqliteValue::Text(b"discard".to_vec())]).expect("insert");
    provider.rollback_to_savepoint("user.savepoint").expect("rollback to savepoint");
    provider.release_savepoint("user.savepoint").expect("release");
    provider.commit().expect("commit");
    assert!(provider.is_autocommit().expect("autocommit"));
    let rows = provider.execute_one("SELECT count(*) FROM t", &[]).expect("count");
    assert_eq!(rows.rows[0][0], SqliteValue::Integer(0));
    assert!(matches!(provider.journal_mode().expect("journal mode"), sqlite_provider::JournalMode::Delete | sqlite_provider::JournalMode::Wal | sqlite_provider::JournalMode::Other));
    provider.checkpoint().expect("checkpoint");
}

#[test]
fn read_only_allows_cte_reads_and_native_rejects_writes() {
    let path = std::env::temp_dir().join(format!("sqlite-provider-ro-cte-{}.db", std::process::id()));
    {
        let provider = RusqliteProvider::open(&path).expect("create");
        provider.execute_batch("CREATE TABLE t(id INTEGER PRIMARY KEY)").expect("ddl");
    }
    let provider = RusqliteProvider::open_with_options(&path, OpenOptions { read_only: true }).expect("read only");
    let result = provider.execute_one("WITH rows AS (SELECT id FROM t) SELECT id FROM rows", &[]).expect("cte read");
    assert!(result.rows.is_empty());
    let error = provider.execute_one("WITH input(id) AS (SELECT 1) INSERT INTO t SELECT id FROM input", &[]).expect_err("native readonly rejection");
    assert!(matches!(error.code, sqlite_provider::SqliteErrorCode::SqliteNative));
    let _ = std::fs::remove_file(path);
}
