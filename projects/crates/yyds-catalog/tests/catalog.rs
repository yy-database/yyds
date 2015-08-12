use std::fs;

use tempfile::tempdir;
use yyds_catalog::{catalog_path, Catalog};
use yyds_types::{ShardEpoch, ShardId};

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

#[test]
fn routing_epoch_round_trips_and_requires_monotonic_publish() {
    let dir = tempdir().expect("tempdir");
    let path = catalog_path(dir.path().join("routing"));
    let mut catalog = Catalog::open(&path).expect("open");
    catalog
        .publish_routing(ShardEpoch(1), vec![ShardId("a".into()), ShardId("b".into())])
        .expect("publish");
    assert_eq!(catalog.routing().expect("routing").expect("map").epoch(), ShardEpoch(1));
    assert!(catalog.publish_routing(ShardEpoch(1), vec![ShardId("a".into())]).is_err());
    catalog.flush().expect("flush");

    let reopened = Catalog::open(&path).expect("reopen");
    let map = reopened.routing().expect("routing").expect("map");
    assert_eq!(map.epoch(), ShardEpoch(1));
    assert_eq!(map.shards(), &[ShardId("a".into()), ShardId("b".into())]);
}

#[test]
fn resolved_contract_round_trips_and_rejects_conflicting_publish() {
    let source = "class T { id: uuid }";
    let projection = vos::parse_oak(source).expect("Oak parses").project_schema().expect("projection");
    let manifest = vos::contract::IdentityManifest {
        format_version: vos::contract::IDENTITY_MANIFEST_VERSION.into(),
        types: vec![vos::contract::TypeIdentity {
            canonical_path: vec!["T".into()],
            type_id: 1,
            kind: vos::contract::TypeContractKind::Class,
            fields: vec![vos::contract::FieldIdentity {
                canonical_name: "id".into(),
                field_id: 1,
                virtual_field_index: 0,
            }],
        }],
    };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    let dir = tempdir().expect("tempdir");
    let path = catalog_path(dir.path().join("contract"));
    let mut catalog = Catalog::open(&path).expect("open");
    assert!(catalog.publish_resolved_contract(contract.clone()).is_err());
    catalog.ensure_schema(1, source).expect("schema");
    catalog.publish_resolved_contract(contract.clone()).expect("publish");
    catalog.flush().expect("flush");

    let reopened = Catalog::open(&path).expect("reload");
    assert_eq!(reopened.resolved_contract(), Some(&contract));

    let mut conflicting = contract.clone();
    conflicting.schema_fingerprint = "0".repeat(64);
    assert!(reopened.clone().publish_resolved_contract(conflicting).is_err());
}
