//! Ensures `SqliteEngine` exposes the shared provider contract.

use yyds_sqlite::{CONTRACT_VERSION, SqliteEngine, SqliteProvider};

#[test]
fn engine_implements_provider_contract() {
    let engine = SqliteEngine::open_in_memory().expect("open");
    assert_eq!(engine.capabilities().contract_version, CONTRACT_VERSION);
    assert_eq!(engine.provider().source_id().expect("source"), engine.source_id().expect("engine source"));
}
