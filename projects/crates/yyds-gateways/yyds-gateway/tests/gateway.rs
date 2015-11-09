use yyds_gateway::{
    bind_sql, GatewayConfig, GatewayKind, SqlBoundExpression, SqlBoundLiteral, SqlBoundProjection, SqlBoundStatement,
    ALL_GATEWAY_KINDS, FORBIDDEN_LEGACY_SURFACES,
};

fn catalog(source: &str) -> yyds_gateway::SqlCatalog {
    let projection = vos::parse_oak(source).expect("Oak VOS source").project_schema().expect("projection");
    let mut next_field_id = 1u64;
    let types = projection
        .types
        .iter()
        .enumerate()
        .map(|(type_index, entry)| vos::contract::TypeIdentity {
            canonical_path: entry.canonical_path.clone(),
            type_id: type_index as u64 + 1,
            kind: entry.kind,
            fields: entry
                .fields
                .iter()
                .enumerate()
                .map(|(field_index, field)| {
                    let field_id = next_field_id;
                    next_field_id += 1;
                    vos::contract::FieldIdentity {
                        canonical_name: field.canonical_name.clone(),
                        field_id,
                        virtual_field_index: field_index as u32,
                    }
                })
                .collect(),
        })
        .collect();
    let manifest = vos::contract::IdentityManifest { format_version: vos::contract::IDENTITY_MANIFEST_VERSION.into(), types };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    yyds_gateway::SqlCatalog::from_resolved_contract(7, &contract).expect("gateway catalog")
}

#[test]
fn sql_catalog_binding_consumes_published_yyds_identity() {
    let mut yyds_catalog = yyds_catalog::Catalog::open_memory();
    yyds_catalog.ensure_schema(7, "table users { @@id: i64 }\n").expect("publish VOS schema");
    let catalog = yyds_gateway::SqlCatalog::from_yyds_catalog(&yyds_catalog).expect("published identity");
    let bound = yyds_gateway::bind_sql_catalog("SELECT id FROM users", &catalog, 7).expect("catalog bind");
    assert!(matches!(bound, yyds_gateway::SqlCatalogBoundStatement::Select(_)));
}

#[test]
fn sql_catalog_binding_prefers_published_resolved_contract() {
    let source = "table users { @@id: i64 }\n";
    let projection = vos::parse_oak(source).expect("Oak parses").project_schema().expect("projection");
    let manifest = vos::contract::IdentityManifest {
        format_version: vos::contract::IDENTITY_MANIFEST_VERSION.into(),
        types: vec![vos::contract::TypeIdentity {
            canonical_path: vec!["users".into()],
            type_id: 1,
            kind: vos::contract::TypeContractKind::Table,
            fields: vec![vos::contract::FieldIdentity { canonical_name: "id".into(), field_id: 1, virtual_field_index: 0 }],
        }],
    };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    let mut yyds_catalog = yyds_catalog::Catalog::open_memory();
    yyds_catalog.ensure_schema(7, source).expect("schema");
    yyds_catalog.publish_resolved_contract(contract).expect("contract");
    let catalog = yyds_gateway::SqlCatalog::from_yyds_catalog(&yyds_catalog).expect("gateway catalog");
    assert_eq!(catalog.tables()[0].type_id, 1);
    assert_eq!(catalog.tables()[0].fields[0].field_id, 1);
}

#[test]
fn all_gateway_kinds_have_distinct_ids() {
    let ids: Vec<_> = ALL_GATEWAY_KINDS.iter().map(|kind| kind.id()).collect();
    let mut unique = ids.clone();
    unique.sort_unstable();
    unique.dedup();
    assert_eq!(ids.len(), unique.len());
}

#[test]
fn startup_banner_uses_crate_name_and_port() {
    let banner = yyds_gateway::startup_banner(&GatewayConfig::new(GatewayKind::Redis).with_port(6380));
    assert!(banner.contains("yyds-gateway-redis"));
    assert!(banner.contains("6380"));
}

#[test]
fn forbidden_legacy_surfaces_include_sql_gateway() {
    assert!(FORBIDDEN_LEGACY_SURFACES.contains(&"yyds-gateway/src/sql"));
    assert!(yyds_gateway::is_forbidden_legacy_surface("yyds-gateway/src/sql"));
    assert!(!FORBIDDEN_LEGACY_SURFACES.contains(&"oak-sql"));
    assert!(!yyds_gateway::is_forbidden_legacy_surface("oak-sql"));
}

#[test]
fn sql_surface_is_parsed_by_oak_without_creating_a_gateway_planner() {
    yyds_gateway::parse_sql("SELECT 1").expect("Oak parses the SQL compatibility surface");
}

#[test]
fn sql_binding_preserves_surface_shape_for_a_yyds_catalog_binder() {
    let bound = bind_sql("SELECT id, 1 AS constant FROM users WHERE id = 7").expect("bind");
    let select = match bound {
        SqlBoundStatement::Select(s) => s,
        other => panic!("expected SqlBoundStatement::Select, got {other:?}"),
    };
    assert_eq!(select.source.as_deref(), Some("users"));
    assert_eq!(select.projections.len(), 2);
    assert!(matches!(
        &select.projections[0],
        SqlBoundProjection::Expression {
            expression: SqlBoundExpression::Identifier(name),
            alias: None,
        } if name == "id"
    ));
    assert!(matches!(
        &select.projections[1],
        SqlBoundProjection::Expression {
            expression: SqlBoundExpression::Literal(SqlBoundLiteral::Number(value)),
            alias: Some(alias),
        } if value == "1" && alias == "constant"
    ));
    assert!(select.predicate.is_some());
}

#[test]
fn sql_binding_rejects_unimplemented_clauses_and_dml() {
    assert!(bind_sql("CREATE TABLE users (id i64)").is_err());
    assert!(bind_sql("SELECT id FROM users ORDER BY id").is_err());
}

#[test]
fn sql_catalog_binding_resolves_vos_type_and_field_identity() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\ntable admins { @@id: i64 }\n");
    let bound = yyds_gateway::bind_sql_catalog("SELECT id, name FROM users WHERE id = 7", &catalog, 7).expect("catalog bind");
    let select = match bound {
        yyds_gateway::SqlCatalogBoundStatement::Select(s) => s,
        other => panic!("expected SqlCatalogBoundStatement::Select, got {other:?}"),
    };
    let source = select.source.expect("source");
    assert_eq!(source.name, "users");
    assert_eq!(source.type_id, 1);
    assert_eq!(select.projections.len(), 2);
    assert!(matches!(
        &select.projections[0],
        yyds_gateway::SqlCatalogBoundProjection::Expression {
            expression: yyds_gateway::SqlCatalogBoundExpression::Column(column),
            alias: None,
        } if column.table_name == "users" && column.field_name == "id"
    ));
}

#[test]
fn sql_catalog_binding_rejects_unknown_names_and_schema_versions() {
    let catalog = catalog("table users { @@id: i64 }\n");
    let unknown_table = yyds_gateway::bind_sql_catalog("SELECT id FROM missing", &catalog, 7).expect_err("unknown table");
    assert!(unknown_table.message.contains("unknown SQL table"));
    let unknown_column = yyds_gateway::bind_sql_catalog("SELECT missing FROM users", &catalog, 7).expect_err("unknown column");
    assert!(unknown_column.message.contains("unknown SQL column"));
    let version = yyds_gateway::bind_sql_catalog("SELECT id FROM users", &catalog, 8).expect_err("schema version mismatch");
    assert!(version.message.contains("schema version mismatch"));
}

#[test]
fn sql_catalog_binding_rejects_ambiguous_unqualified_columns() {
    let snapshot: vos::ast::CatalogSnapshot = serde_json::from_value(serde_json::json!({
        "revisions": { "ddl": 1, "semantic": 1, "layout_epoch": 0 },
        "types": [{
            "type_id": 1, "name": "users", "kind": "table",
            "fields": [
                { "field_id": 1, "virtual_field": 0, "current_name": "id", "source_order": 0, "ty": { "Builtin": "I64" }, "attrs": [] },
                { "field_id": 2, "virtual_field": 1, "current_name": "id", "source_order": 1, "ty": { "Builtin": "I64" }, "attrs": [] }
            ]
        }]
    })).expect("legacy snapshot");
    let catalog = yyds_gateway::SqlCatalog::from_snapshot(7, &snapshot).expect("gateway catalog");
    let error = yyds_gateway::bind_sql_catalog("SELECT id FROM users", &catalog, 7).expect_err("ambiguous column");
    assert!(error.message.contains("ambiguous SQL column"));
}

// --- SQL → KV 执行层适配器 ---

fn make_executor_with_row(
    namespace: &str,
    rowid: &[u8],
    row_fields: &[(u64, &[u8])],
) -> yyds_execution::LocalExecutor<yyds_kv::MemoryShard> {
    use yyds_kv::{InlineValue, Key, KvStore, MemoryShard, StoredValue};
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let epoch = ShardEpoch(1);
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(epoch, vec![shard_id.clone()]).expect("routing");

    let mut shard = MemoryShard::new();
    let mut values = yyds_gateway::RowFieldValues::new();
    for (field_id, bytes) in row_fields {
        values.insert(*field_id, bytes.to_vec());
    }
    let row_bytes = yyds_gateway::encode_row(&values);
    shard.put(Key::new(namespace, rowid.to_vec()), StoredValue::Inline(InlineValue(row_bytes))).expect("put row");

    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    yyds_execution::LocalExecutor::new(routing, shards).expect("executor")
}

#[test]
fn sql_select_pk_equality_executes_against_kv_adapter() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = make_executor_with_row("table:1", b"42", &[(1, b"42"), (2, b"alice"), (3, b"30")]);

    let bound = yyds_gateway::bind_sql_catalog("SELECT id, name FROM users WHERE id = 42", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, &plan).expect("execute");

    assert_eq!(result.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0],
        vec![yyds_gateway::SqlValue::Number(42.0), yyds_gateway::SqlValue::String("alice".to_string()),]
    );
}

#[test]
fn sql_select_star_expands_all_fields() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_row("table:1", b"1", &[(1, b"1"), (2, b"bob")]);

    let bound = yyds_gateway::bind_sql_catalog("SELECT * FROM users WHERE id = 1", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, &plan).expect("execute");

    assert_eq!(result.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(result.rows[0].len(), 2);
}

#[test]
fn sql_select_pk_string_literal_uses_string_rowid() {
    let catalog = catalog("table users { @@id: utf8, score: i64 }\n");
    let executor = make_executor_with_row("table:1", b"alice", &[(1, b"alice"), (2, b"100")]);

    let bound =
        yyds_gateway::bind_sql_catalog("SELECT score FROM users WHERE id = 'alice'", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, &plan).expect("execute");

    assert_eq!(result.rows[0][0], yyds_gateway::SqlValue::Number(100.0));
}

#[test]
fn sql_select_missing_row_returns_empty_result() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    // namespace "table:1" 存在但 key "999" 没有
    let executor = make_executor_with_row("table:1", b"1", &[(1, b"1"), (2, b"exist")]);

    let bound = yyds_gateway::bind_sql_catalog("SELECT name FROM users WHERE id = 999", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, &plan).expect("execute");

    assert!(result.rows.is_empty());
}

#[test]
fn sql_select_without_pk_generates_scan_plan() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("pk-less query should now produce a scan plan");
    match plan {
        yyds_gateway::SqlExecutionPlan::Scan(scan) => {
            assert_eq!(scan.namespace, "table:1");
            assert!(scan.rowid_prefix.is_empty());
            assert!(scan.rowid_after.is_none());
            assert_eq!(scan.limit, 0);
            assert!(scan.remaining_predicate.is_none());
        }
        other => panic!("expected Scan plan, got {other:?}"),
    }
}

#[test]
fn sql_select_non_pk_equality_becomes_scan_with_remaining_predicate() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE name = 'foo'", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("non-pk equality should produce scan plan with remaining predicate");
    match plan {
        yyds_gateway::SqlExecutionPlan::Scan(scan) => {
            assert_eq!(scan.namespace, "table:1");
            assert!(scan.remaining_predicate.is_some()); // name = 'foo' stays as row-level filter
        }
        other => panic!("expected Scan plan, got {other:?}"),
    }
}

#[test]
fn sql_select_and_extracts_pk_and_filters_on_remaining() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = make_executor_with_row("table:1", b"5", &[(1, b"5"), (2, b"carol"), (3, b"25")]);

    // AND predicate 包含 pk 等值 + 额外条件；plan 应只提 pk，额外条件作为 remaining
    let bound = yyds_gateway::bind_sql_catalog("SELECT name, age FROM users WHERE id = 5 AND age = 25", &catalog, 7)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let read_plan = match &plan {
        yyds_gateway::SqlExecutionPlan::Read(p) => p,
        other => panic!("expected Read plan, got {other:?}"),
    };
    assert!(read_plan.remaining_predicate.is_some(), "AND 的右侧应留在 remaining");

    let result = yyds_gateway::execute_plan(&executor, &plan).expect("execute");
    assert_eq!(result.rows.len(), 1);
}

#[test]
fn sql_select_projects_binary_expression() {
    let catalog = catalog("table users { @@id: i64, x: i64, y: i64 }\n");
    let executor = make_executor_with_row("table:1", b"1", &[(1, b"1"), (2, b"10"), (3, b"3")]);

    // 先确认 Oak 支持哪些 projection 表达式
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id + 1 AS inc FROM users WHERE id = 1", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, &plan).expect("execute");

    assert_eq!(result.rows[0][0], yyds_gateway::SqlValue::Number(2.0));
}

#[test]
fn row_field_values_round_trip_tlv() {
    let mut row = yyds_gateway::RowFieldValues::new();
    row.insert(1, b"42".to_vec());
    row.insert(7, b"hello".to_vec());
    let bytes = yyds_gateway::encode_row(&row);
    let decoded = yyds_gateway::decode_row(&bytes).expect("decode");
    assert_eq!(decoded.get(1), Some(b"42".as_slice()));
    assert_eq!(decoded.get(7), Some(b"hello".as_slice()));
    assert_eq!(decoded.get(99), None);
}

// --- DML round-trip (INSERT / UPDATE / DELETE) ---

/// 构造一个**空** executor — 用于测试 INSERT 后 SELECT / UPDATE / DELETE。
fn empty_executor() -> yyds_execution::LocalExecutor<yyds_kv::MemoryShard> {
    use yyds_kv::MemoryShard;
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let epoch = ShardEpoch(1);
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(epoch, vec![shard_id.clone()]).expect("routing");
    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, MemoryShard::new());
    yyds_execution::LocalExecutor::new(routing, shards).expect("executor")
}

#[test]
fn sql_insert_then_select_round_trip() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = empty_executor();

    // INSERT INTO users VALUES (7, 'alice', 30)
    // Oak 解析后 INSERT VALUES 第一个值是 pk literal = 7
    let bound = yyds_gateway::bind_sql_catalog("INSERT INTO users VALUES (7, 'alice', 30)", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    yyds_gateway::execute_plan(&executor, &plan).expect("insert executes");

    // SELECT 出来验证
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id, name, age FROM users WHERE id = 7", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, &plan).expect("select after insert");

    assert_eq!(result.columns, vec!["id".to_string(), "name".to_string(), "age".to_string()]);
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0],
        vec![
            yyds_gateway::SqlValue::Number(7.0),
            yyds_gateway::SqlValue::String("alice".into()),
            yyds_gateway::SqlValue::Number(30.0),
        ]
    );
}

#[test]
fn sql_update_then_select_round_trip() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    // 预置 row: id=9, name=original, age=20
    let executor = make_executor_with_row("table:1", b"9", &[(1, b"9"), (2, b"original"), (3, b"20")]);

    // UPDATE users SET name='updated', age=99 WHERE id = 9
    let bound = yyds_gateway::bind_sql_catalog("UPDATE users SET name = 'updated', age = 99 WHERE id = 9", &catalog, 7)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    yyds_gateway::execute_plan(&executor, &plan).expect("update executes");

    // SELECT 验证
    let bound = yyds_gateway::bind_sql_catalog("SELECT name, age FROM users WHERE id = 9", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, &plan).expect("select after update");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::String("updated".into()), yyds_gateway::SqlValue::Number(99.0),]);
}

#[test]
fn sql_delete_removes_row() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_row("table:1", b"55", &[(1, b"55"), (2, b"victim")]);

    // DELETE FROM users WHERE id = 55
    let bound = yyds_gateway::bind_sql_catalog("DELETE FROM users WHERE id = 55", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    yyds_gateway::execute_plan(&executor, &plan).expect("delete executes");

    // SELECT 验证空
    let bound = yyds_gateway::bind_sql_catalog("SELECT name FROM users WHERE id = 55", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, &plan).expect("select after delete");
    assert!(result.rows.is_empty());
}

#[test]
fn sql_insert_columns_explicit_order_is_rejected_by_oak() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    // Oak SQL parser 当前只支持 `INSERT INTO t VALUES (...)`，不支持显式列顺序。
    // catalog_bind.rs 里留了按 catalog 字段顺序填充的路径，等 Oak parser 扩展后启用。
    let error = yyds_gateway::bind_sql_catalog("INSERT INTO users (age, @@id, name) VALUES (25, 11, 'bob')", &catalog, 7)
        .expect_err("explicit column INSERT not supported yet");
    assert!(error.message.contains("column/value") || error.message.contains("values"));
}

#[test]
fn sql_insert_pk_must_be_first_value() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    // VALUES 第一个必须是 pk literal — 如果 Oak 把 "1" 当作 name 值也会在 planner 层报错
    // 这里只是验证 planner 接受正确的形式
    let _bound = yyds_gateway::bind_sql_catalog("INSERT INTO users VALUES (1, 'one')", &catalog, 7).expect("insert bind");
}
