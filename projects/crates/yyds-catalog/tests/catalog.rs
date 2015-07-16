use std::fs;

use tempfile::tempdir;
use yyds_catalog::{catalog_path, Catalog};
use yyds_types::ShardId;

const VOS: &str = "table User {\n    @@id: uuid,\n}\n";

#[test]
fn ensure_schema_and_register_shard_in_memory() {
    let mut catalog = Catalog::open_memory();
    catalog.ensure_schema(1, VOS).expect("ensure");
    catalog
        .register_shard(ShardId("shard-0".into()))
        .expect("register");

    let schema = catalog.schema().expect("schema");
    assert_eq!(schema.version, 1);
    assert_eq!(schema.document, VOS);
    assert!(catalog.identity().is_some());
    assert_eq!(catalog.shards().len(), 1);
}

#[test]
fn file_catalog_round_trip() {
    let dir = tempdir().expect("tempdir");
    let path = catalog_path(dir.path().join("cluster-a"));

    {
        let mut catalog = Catalog::open(&path).expect("open");
        catalog.ensure_schema(1, VOS).expect("ensure");
        catalog
            .register_shard(ShardId("shard-a".into()))
            .expect("register");
        catalog.flush().expect("flush");
    }

    let catalog = Catalog::open(&path).expect("reload");
    let schema = catalog.schema().expect("schema");
    assert_eq!(schema.version, 1);
    assert_eq!(schema.document, VOS);
    assert!(catalog.identity().is_some());
    assert_eq!(catalog.shards(), &[ShardId("shard-a".into())]);
    assert!(path.is_file());

    let bytes = fs::read(&path).expect("read");
    assert!(bytes.starts_with(yyds_catalog::MAGIC));
}

#[test]
fn ensure_schema_rejects_version_mismatch() {
    let mut catalog = Catalog::open_memory();
    catalog.ensure_schema(1, VOS).expect("ensure");
    let err = catalog.ensure_schema(2, VOS).expect_err("conflict");
    assert!(err.to_string().contains("schema version conflict"));
}

#[test]
fn identity_initialization_requires_explicit_call_for_empty_state() {
    let mut catalog = Catalog::open_memory();
    assert!(catalog.initialize_identity().is_err());
    catalog.ensure_schema(1, VOS).expect("ensure");
    assert!(catalog.identity().is_some());
    catalog.initialize_identity().expect("idempotent");
}
