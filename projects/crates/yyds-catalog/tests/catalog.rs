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
    assert!(catalog.resolved_contract().is_some());
    assert!(catalog.identity().is_none());
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
    assert!(catalog.resolved_contract().is_some());
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
    assert!(catalog.resolved_contract().is_some());
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

fn legacy_identity() -> vos::ast::CatalogSnapshot {
    serde_json::from_value(serde_json::json!({
        "revisions": { "ddl": 1, "semantic": 1, "layout_epoch": 0 },
        "types": [{
            "type_id": 37, "name": "User", "kind": "table",
            "fields": [{
                "field_id": 91, "virtual_field": 7, "current_name": "id",
                "source_order": 0, "ty": { "Builtin": "Uuid" }, "attrs": ["Primary"]
            }]
        }]
    })).expect("reviewed legacy identity")
}

fn legacy_v3_bytes(identity: &vos::ast::CatalogSnapshot) -> Vec<u8> {
    let schema = yyds_types::CatalogSchema { version: 7, document: VOS.into() };
    let mut bytes = yyds_catalog::encode(
        Some(&schema), Some(identity), Some(ShardEpoch(3)), &[ShardId("a".into())], None,
    ).expect("legacy fields");
    bytes.truncate(bytes.len() - 4);
    let offset = yyds_catalog::MAGIC.len();
    bytes[offset..offset + 4].copy_from_slice(&3u32.to_le_bytes());
    bytes
}

#[test]
fn legacy_v3_upgrade_preserves_ids_slots_routing_and_bytes_until_flush() {
    let dir = tempdir().expect("tempdir");
    let path = catalog_path(dir.path().join("legacy"));
    let bytes = legacy_v3_bytes(&legacy_identity());
    fs::write(&path, &bytes).expect("legacy file");
    let catalog = Catalog::open(&path).expect("migrate through Oak");
    let contract = catalog.resolved_contract().expect("contract");
    assert_eq!(contract.types[0].type_id, 37);
    assert_eq!(contract.types[0].fields[0].field_id, 91);
    assert_eq!(contract.types[0].fields[0].virtual_field_index, 7);
    assert_eq!(catalog.routing().unwrap().unwrap().epoch(), ShardEpoch(3));
    assert_eq!(catalog.shards(), &[ShardId("a".into())]);
    assert_eq!(fs::read(&path).unwrap(), bytes);
    catalog.flush().expect("persist upgrade");
    let reopened = Catalog::open(&path).expect("v4 reopen");
    assert_eq!(reopened.resolved_contract(), Some(contract));
    assert_eq!(reopened.identity(), catalog.identity());
}

#[test]
fn incomplete_legacy_identity_is_rejected_without_reassigning_ids() {
    let dir = tempdir().expect("tempdir");
    let path = catalog_path(dir.path().join("broken"));
    let mut identity = legacy_identity();
    identity.types[0].fields.clear();
    let bytes = legacy_v3_bytes(&identity);
    fs::write(&path, &bytes).expect("legacy file");
    assert!(Catalog::open(&path).is_err());
    assert_eq!(fs::read(&path).unwrap(), bytes);
}

#[test]
fn failed_schema_resolution_does_not_publish_partial_catalog() {
    let mut catalog = Catalog::open_memory();
    let before = catalog.clone();
    assert!(catalog.ensure_schema(1, "table User { @@id: uuid, value: Missing }").is_err());
    assert_eq!(catalog, before);
    catalog.ensure_schema(1, VOS).expect("retry with valid schema");
    assert!(catalog.identity().is_none());
    let contract = catalog.resolved_contract().unwrap();
    assert_eq!(contract.types[0].type_id, 1);
    assert_eq!(contract.types[0].fields[0].field_id, 1);
}
