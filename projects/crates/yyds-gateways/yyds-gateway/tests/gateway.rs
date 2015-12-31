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
    assert!(bind_sql("SELECT * FROM a JOIN b").is_err()); // JOIN without ON
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
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("execute");

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
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("execute");

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
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("execute");

    assert_eq!(result.rows[0][0], yyds_gateway::SqlValue::Number(100.0));
}

#[test]
fn sql_select_missing_row_returns_empty_result() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    // namespace "table:1" 存在但 key "999" 没有
    let executor = make_executor_with_row("table:1", b"1", &[(1, b"1"), (2, b"exist")]);

    let bound = yyds_gateway::bind_sql_catalog("SELECT name FROM users WHERE id = 999", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("execute");

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

    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("execute");
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
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("execute");

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

fn make_executor_with_rows(
    namespace: &str,
    rows: &[(&[u8], &[(u64, &[u8])])],
) -> yyds_execution::LocalExecutor<yyds_kv::MemoryShard> {
    use yyds_kv::{InlineValue, Key, KvStore, MemoryShard, StoredValue};
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let epoch = ShardEpoch(1);
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(epoch, vec![shard_id.clone()]).expect("routing");

    let mut shard = MemoryShard::new();
    for (rowid, fields) in rows {
        let mut values = yyds_gateway::RowFieldValues::new();
        for (field_id, bytes) in *fields {
            values.insert(*field_id, bytes.to_vec());
        }
        let row_bytes = yyds_gateway::encode_row(&values);
        shard.put(Key::new(namespace, rowid.to_vec()), StoredValue::Inline(InlineValue(row_bytes))).expect("put row");
    }

    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
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
    yyds_gateway::execute_plan(&executor, None, &plan).expect("insert executes");

    // SELECT 出来验证
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id, name, age FROM users WHERE id = 7", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("select after insert");

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
fn sql_aggregate_functions_count_sum_avg_max_min() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = empty_executor();

    // Insert three rows: (7, alice, 30), (11, bob, 25), (13, carol, 35)
    for (id, name, age) in [(7i64, "alice", 30i64), (11, "bob", 25), (13, "carol", 35)] {
        let sql = format!("INSERT INTO users VALUES ({id}, '{name}', {age})");
        let bound = yyds_gateway::bind_sql_catalog(&sql, &catalog, 7).expect("bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        yyds_gateway::execute_plan(&executor, None, &plan).expect("insert");
    }

    // COUNT(*) → 3
    let bound = yyds_gateway::bind_sql_catalog("SELECT COUNT(*) FROM users", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec COUNT(*)");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Number(3.0)]);

    // COUNT(age) WHERE age > 28 → alice(30) + carol(35) = 2
    let bound = yyds_gateway::bind_sql_catalog("SELECT COUNT(age) FROM users WHERE age > 28", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec COUNT(col) WHERE");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Number(2.0)]);

    // SUM(age) → 30 + 25 + 35 = 90
    let bound = yyds_gateway::bind_sql_catalog("SELECT SUM(age) FROM users", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec SUM");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Number(90.0)]);

    // AVG(age) → 90 / 3 = 30
    let bound = yyds_gateway::bind_sql_catalog("SELECT AVG(age) FROM users", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec AVG");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Number(30.0)]);

    // MAX(age) → 35
    let bound = yyds_gateway::bind_sql_catalog("SELECT MAX(age) FROM users", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec MAX");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Number(35.0)]);

    // MIN(age) → 25
    let bound = yyds_gateway::bind_sql_catalog("SELECT MIN(age) FROM users", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec MIN");
    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::Number(25.0)]);
}

#[test]
fn sql_aggregate_rejects_mixed_aggregate_nonaggregate() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let result = yyds_gateway::bind_sql_catalog("SELECT id, COUNT(*) FROM users", &catalog, 7);
    assert!(result.is_ok());
    let bound = result.unwrap();
    let err = yyds_gateway::plan_statement(bound, &catalog).expect_err("planner should reject mixed");
    let err_debug = format!("{err:?}");
    assert!(err_debug.contains("cannot mix aggregate and non-aggregate"));
}

#[test]
fn sql_limit_with_where_scans_all_then_returns_limited() {
    // Regression: kv_limit 不能下推到 KV-scan 当有 WHERE（非 pk 条件）时。
    // 否则先 scan N 行再 filter 会返回不足 N 行的结果。
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = empty_executor();

    // 按 rowid 顺序（= id 升序）：(7,30), (11,20), (13,35), (17,22)
    for (id, _name, age) in [(7i64, "alice", 30i64), (11, "bob", 20), (13, "carol", 35), (17, "dan", 22)] {
        let sql = format!("INSERT INTO users VALUES ({id}, '', {age})");
        let bound = yyds_gateway::bind_sql_catalog(&sql, &catalog, 7).expect("bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        yyds_gateway::execute_plan(&executor, None, &plan).expect("insert");
    }

    // WHERE age > 22 LIMIT 2 → 预期：2 行 alice(30), carol(35)
    let bound = yyds_gateway::bind_sql_catalog("SELECT id, age FROM users WHERE age > 22 LIMIT 2", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec");

    // 顺序由 KV 实现决定（未指定 ORDER BY），只检查数量和值集合。
    assert_eq!(result.rows.len(), 2, "expected 2 rows, got {} — kv_limit pushdown bug", result.rows.len());
    let ages: Vec<f64> = result
        .rows
        .iter()
        .map(|r| {
            if let yyds_gateway::SqlValue::Number(n) = &r[1] {
                *n
            }
            else {
                unreachable!()
            }
        })
        .collect();
    assert!(ages.contains(&30.0), "missing alice(30)");
    assert!(ages.contains(&35.0), "missing carol(35)");
}

#[test]
fn sql_order_by_with_limit_offset_uses_top_n_heap() {
    // ORDER BY + (LIMIT 或 OFFSET) 必须正确排序后再应用分页。
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = empty_executor();

    // 按 age: bob=20, dan=22, alice=30, carol=35
    for (id, _name, age) in [(7i64, "alice", 30i64), (11, "bob", 20), (13, "carol", 35), (17, "dan", 22)] {
        let sql = format!("INSERT INTO users VALUES ({id}, '', {age})");
        let bound = yyds_gateway::bind_sql_catalog(&sql, &catalog, 7).expect("bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        yyds_gateway::execute_plan(&executor, None, &plan).expect("insert");
    }

    // ORDER BY age ASC LIMIT 2 → bob(20), dan(22)
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT name, age FROM users ORDER BY age ASC LIMIT 2", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec ASC+LIMIT");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][1], yyds_gateway::SqlValue::Number(20.0), "ASC first should be bob(20)");
    assert_eq!(result.rows[1][1], yyds_gateway::SqlValue::Number(22.0), "ASC second should be dan(22)");

    // ORDER BY age DESC LIMIT 2 → carol(35), alice(30)
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT name, age FROM users ORDER BY age DESC LIMIT 2", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec DESC+LIMIT");
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][1], yyds_gateway::SqlValue::Number(35.0), "DESC first should be carol(35)");
    assert_eq!(result.rows[1][1], yyds_gateway::SqlValue::Number(30.0), "DESC second should be alice(30)");

    // ORDER BY age ASC LIMIT 1 OFFSET 1 → dan(22) — 跳过 bob(20)
    let bound = yyds_gateway::bind_sql_catalog("SELECT name, age FROM users ORDER BY age ASC LIMIT 1 OFFSET 1", &catalog, 7)
        .expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec ASC+OFFSET+LIMIT");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], yyds_gateway::SqlValue::Number(22.0), "OFFSET 1 should be dan(22)");
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
    yyds_gateway::execute_plan(&executor, None, &plan).expect("update executes");

    // SELECT 验证
    let bound = yyds_gateway::bind_sql_catalog("SELECT name, age FROM users WHERE id = 9", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("select after update");

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
    yyds_gateway::execute_plan(&executor, None, &plan).expect("delete executes");

    // SELECT 验证空
    let bound = yyds_gateway::bind_sql_catalog("SELECT name FROM users WHERE id = 55", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("select after delete");
    assert!(result.rows.is_empty());
}

#[test]
fn sql_insert_column_list_must_name_catalog_columns() {
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    // `@@id` 是 catalog 里的主键注解，SQL 列名是 `id`；`@@` 前缀属于系统变量，不能当列名。
    let error = yyds_gateway::bind_sql_catalog("INSERT INTO users (age, @@id, name) VALUES (25, 11, 'bob')", &catalog, 7)
        .expect_err("system variable prefix is not a column");
    assert!(error.message.contains("unknown SQL column"), "{}", error.message);
}

#[test]
fn sql_insert_pk_must_be_first_value() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    // VALUES 第一个必须是 pk literal — 如果 Oak 把 "1" 当作 name 值也会在 planner 层报错
    // 这里只是验证 planner 接受正确的形式
    let _bound = yyds_gateway::bind_sql_catalog("INSERT INTO users VALUES (1, 'one')", &catalog, 7).expect("insert bind");
}

#[test]
fn sql_select_star_from_table_scans_all_rows() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    // 预置三行 — id=1/alice, id=2/bob, id=3/carol
    let executor = make_executor_with_rows(
        "table:1",
        &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")]), (b"3", &[(1, b"3"), (2, b"carol")])],
    );

    let bound = yyds_gateway::bind_sql_catalog("SELECT * FROM users", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    // 确认走 Scan 路径
    assert!(matches!(plan, yyds_gateway::SqlExecutionPlan::Scan(_)));
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("execute");

    assert_eq!(result.columns, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(result.rows.len(), 3);
    let ids: Vec<f64> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            yyds_gateway::SqlValue::Number(n) => *n,
            other => panic!("expected number, got {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![1.0, 2.0, 3.0]);
}

#[test]
fn sql_scan_applies_remaining_predicate_from_non_pk_equality() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")]), (b"3", &[(1, b"3"), (2, b"alice")])],
    );

    // WHERE name = 'alice' — 不是 pk 等值，planner 输出 ScanPlan 带 remaining_predicate
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT id, name FROM users WHERE name = 'alice'", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    assert!(matches!(plan, yyds_gateway::SqlExecutionPlan::Scan(_)));
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("execute");

    assert_eq!(result.rows.len(), 2);
    // alice 在 id=1 和 id=3 两行
    let ids: Vec<f64> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            yyds_gateway::SqlValue::Number(n) => *n,
            other => panic!("expected number, got {other:?}"),
        })
        .collect();
    assert!(ids.contains(&1.0));
    assert!(ids.contains(&3.0));
    assert_eq!(ids.len(), 2);
}

#[test]
fn sql_insert_multiple_then_select_star_returns_all() {
    let catalog = catalog("table items { @@id: i64, sku: utf8 }\n");
    let executor = empty_executor();

    // 连续 INSERT 三行 — 全部写入同一个 executor
    for sql in
        &["INSERT INTO items VALUES (1, 'A1')", "INSERT INTO items VALUES (2, 'B2')", "INSERT INTO items VALUES (3, 'C3')"]
    {
        let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7).expect("bind");
        let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
        yyds_gateway::execute_plan(&executor, None, &plan).expect("insert");
    }

    // SELECT * 验证 scan 路径返回所有三行
    let bound = yyds_gateway::bind_sql_catalog("SELECT * FROM items", &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    assert!(matches!(plan, yyds_gateway::SqlExecutionPlan::Scan(_)));
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("scan");

    assert_eq!(result.rows.len(), 3);
    let ids: Vec<f64> = result
        .rows
        .iter()
        .map(|row| match &row[0] {
            yyds_gateway::SqlValue::Number(n) => *n,
            other => panic!("expected number, got {other:?}"),
        })
        .collect();
    assert_eq!(ids, vec![1.0, 2.0, 3.0]);
}

#[test]
fn sql_select_pk_greater_pushes_after_cursor() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")]), (b"3", &[(1, b"3"), (2, b"carol")])],
    );

    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id > 1", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert_eq!(s.rowid_after.as_deref(), Some(b"1".as_ref()));
            assert!(s.rowid_before.is_none());
            assert_eq!(s.limit, 0);
        }
        other => panic!("expected Scan, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec");
    assert_eq!(result.rows.len(), 2);
    let ids: Vec<f64> = result
        .rows
        .iter()
        .map(|r| {
            if let yyds_gateway::SqlValue::Number(n) = &r[0] {
                *n
            }
            else {
                panic!()
            }
        })
        .collect();
    assert_eq!(ids, vec![2.0, 3.0]);
}

#[test]
fn sql_select_pk_less_pushes_before_cursor() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")]), (b"3", &[(1, b"3"), (2, b"carol")])],
    );

    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id < 3", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert!(s.rowid_after.is_none());
            assert_eq!(s.rowid_before.as_deref(), Some(b"3".as_ref()));
        }
        other => panic!("expected Scan, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec");
    assert_eq!(result.rows.len(), 2);
    let ids: Vec<f64> = result
        .rows
        .iter()
        .map(|r| {
            if let yyds_gateway::SqlValue::Number(n) = &r[0] {
                *n
            }
            else {
                panic!()
            }
        })
        .collect();
    assert_eq!(ids, vec![1.0, 2.0]);
}

#[test]
fn sql_select_pk_range_after_and_before_together() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[
            (b"1", &[(1, b"1"), (2, b"alice")]),
            (b"2", &[(1, b"2"), (2, b"bob")]),
            (b"3", &[(1, b"3"), (2, b"carol")]),
            (b"4", &[(1, b"4"), (2, b"dan")]),
        ],
    );

    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id > 1 AND id < 4", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert_eq!(s.rowid_after.as_deref(), Some(b"1".as_ref()));
            assert_eq!(s.rowid_before.as_deref(), Some(b"4".as_ref()));
        }
        other => panic!("expected Scan, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec");
    assert_eq!(result.rows.len(), 2);
    let ids: Vec<f64> = result
        .rows
        .iter()
        .map(|r| {
            if let yyds_gateway::SqlValue::Number(n) = &r[0] {
                *n
            }
            else {
                panic!()
            }
        })
        .collect();
    assert_eq!(ids, vec![2.0, 3.0]);
}

#[test]
fn sql_limit_pushes_down_to_scan() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[
            (b"1", &[(1, b"1"), (2, b"alice")]),
            (b"2", &[(1, b"2"), (2, b"bob")]),
            (b"3", &[(1, b"3"), (2, b"carol")]),
            (b"4", &[(1, b"4"), (2, b"dan")]),
            (b"5", &[(1, b"5"), (2, b"eve")]),
        ],
    );

    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users LIMIT 3", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert_eq!(s.limit, 3);
        }
        other => panic!("expected Scan, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec");
    assert_eq!(result.rows.len(), 3);
    let ids: Vec<f64> = result
        .rows
        .iter()
        .map(|r| {
            if let yyds_gateway::SqlValue::Number(n) = &r[0] {
                *n
            }
            else {
                panic!()
            }
        })
        .collect();
    assert_eq!(ids, vec![1.0, 2.0, 3.0]);
}

#[test]
fn sql_select_pk_like_prefix_pushes_rowid_prefix() {
    let catalog = catalog("table users { @@id: utf8, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[
            (b"alice", &[(1, b"alice"), (2, b"alice wang")]),
            (b"bob", &[(1, b"bob"), (2, b"bob smith")]),
            (b"carol", &[(1, b"carol"), (2, b"carol lee")]),
            (b"apple", &[(1, b"apple"), (2, b"apple pie")]),
            (b"apply", &[(1, b"apply"), (2, b"apply patch")]),
        ],
    );

    // LIKE 'app%' on pk → rowid_prefix = b"app" → scan pushes prefix
    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id LIKE 'app%'", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert_eq!(&s.rowid_prefix, b"app", "LIKE prefix should push rowid_prefix");
        }
        other => panic!("expected Scan for LIKE, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec");
    let ids: Vec<String> = result
        .rows
        .iter()
        .map(|r| {
            if let yyds_gateway::SqlValue::String(s) = &r[0] {
                s.to_string()
            }
            else {
                panic!()
            }
        })
        .collect();
    assert_eq!(ids, vec!["apple".to_string(), "apply".to_string()]);
}

#[test]
fn sql_select_pk_between_extracts_range() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[
            (b"1", &[(1, b"1"), (2, b"alice")]),
            (b"2", &[(1, b"2"), (2, b"bob")]),
            (b"3", &[(1, b"3"), (2, b"carol")]),
            (b"4", &[(1, b"4"), (2, b"dan")]),
            (b"5", &[(1, b"5"), (2, b"eve")]),
        ],
    );

    // 显式 AND：id >= 2 AND id <= 4
    // GreaterEqual(2) → after 必须 exclusive：key > (2-1) = key > 1 → after=b"1"
    // LessEqual(4) → before 必须 exclusive：key < (4+1) = key < 5 → before=b"5"
    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id >= 2 AND id <= 4", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert_eq!(s.rowid_after.as_deref(), Some(b"1".as_ref()));
            assert_eq!(s.rowid_before.as_deref(), Some(b"5".as_ref()));
        }
        other => panic!("expected Scan, got {other:?}"),
    }

    // BETWEEN 2 AND 4 → 展开同上
    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id BETWEEN 2 AND 4", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(s) => {
            assert_eq!(s.rowid_after.as_deref(), Some(b"1".as_ref()));
            assert_eq!(s.rowid_before.as_deref(), Some(b"5".as_ref()));
        }
        other => panic!("expected Scan for BETWEEN, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec");
    assert_eq!(result.rows.len(), 3);
}

#[test]
fn sql_select_pk_in_list_filters_rows() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_rows(
        "table:1",
        &[
            (b"1", &[(1, b"1"), (2, b"alice")]),
            (b"2", &[(1, b"2"), (2, b"bob")]),
            (b"3", &[(1, b"3"), (2, b"carol")]),
            (b"4", &[(1, b"4"), (2, b"dan")]),
            (b"5", &[(1, b"5"), (2, b"eve")]),
        ],
    );

    // id IN (2, 4, 5) — 展开成 OR 树，走 Scan + remaining_predicate
    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE id IN (2, 4, 5)", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match &plan {
        yyds_gateway::SqlExecutionPlan::Scan(_) => { /* IN 不做 pushdown，走 Scan */ }
        other => panic!("expected Scan for IN, got {other:?}"),
    }
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("exec");
    assert_eq!(result.rows.len(), 3);
    let ids: Vec<f64> = result
        .rows
        .iter()
        .map(|r| match &r[0] {
            yyds_gateway::SqlValue::Number(n) => *n,
            other => panic!("expected Number, got {other:?}"),
        })
        .collect();
    assert!(ids.contains(&2.0) && ids.contains(&4.0) && ids.contains(&5.0));
}

/// 直接用 yyds_gateway::parse_sql 看 AST，诊断 oak_sql parser 对各 WHERE 语法到底产啥。
fn probe_ast(sql: &str) -> String {
    let root = yyds_gateway::parse_sql(sql).unwrap_or_else(|e| panic!("parse fail {sql}: {e:?}"));
    let sel = match &root.statements[0] {
        oak_sql::ast::SqlStatement::Select(s) => s,
        _ => panic!("not select"),
    };
    format!("{:?}", sel.expr)
}

#[test]
fn probe_parser_basic_binary_ops() {
    let bin = "Some(Binary";
    assert!(probe_ast("SELECT * FROM t WHERE id = 1").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE id > 1").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE id >= 1").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE id < 1").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE id <= 1").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE id != 1").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE a = 1 AND b = 2").starts_with(bin));
    assert!(probe_ast("SELECT * FROM t WHERE a = 1 OR b = 2").starts_with(bin));
}

#[test]
fn probe_parser_between_in_like_is_null() {
    let v = probe_ast("SELECT * FROM t WHERE id BETWEEN 1 AND 10");
    eprintln!("BETWEEN → {v}");

    let v = probe_ast("SELECT * FROM t WHERE id IN (1, 2, 3)");
    eprintln!("IN → {v}");

    let v = probe_ast("SELECT * FROM t WHERE name LIKE 'apple-%'");
    eprintln!("LIKE → {v}");

    let v = probe_ast("SELECT * FROM t WHERE name IS NULL");
    eprintln!("IS NULL → {v}");

    eprintln!("---");
    // 用纯 infix 验证 LIKE/IN/IS token 是否被 lexer 正确识别
    // 如果 = 能产 Binary 但 LIKE 只产 Identifier，说明 infix 层没接上
    let v_eq = probe_ast("SELECT * FROM t WHERE id = 1");
    eprintln!("EQ → {v_eq}");
    let v_like = probe_ast("SELECT * FROM t WHERE name LIKE 'x'");
    eprintln!("LIKE2 → {v_like}");
}

// raw_children probe 已删——lexer bug 定位完毕，补 keyword 表后重跑 BETWEEN/IN/LIKE/IS NULL probe 验证

// ===== planner capability probes =====

#[test]
fn probe_planner_supports_order_by() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let result = yyds_gateway::bind_sql_catalog("SELECT id FROM users ORDER BY id", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog));
    match result {
        Ok(_) => eprintln!("ORDER BY: supported ✅"),
        Err(e) => eprintln!("ORDER BY: blocked ❌ — {e:?}"),
    }
}

#[test]
fn probe_planner_supports_group_by() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let result = yyds_gateway::bind_sql_catalog("SELECT name FROM users GROUP BY name", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog));
    match result {
        Ok(_) => eprintln!("GROUP BY: supported ✅"),
        Err(e) => eprintln!("GROUP BY: blocked ❌ — {e:?}"),
    }
}

#[test]
fn probe_planner_supports_distinct() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let result = yyds_gateway::bind_sql_catalog("SELECT DISTINCT name FROM users", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog));
    match result {
        Ok(_) => eprintln!("DISTINCT: supported ✅"),
        Err(e) => eprintln!("DISTINCT: blocked ❌ — {e:?}"),
    }
}

#[test]
fn probe_planner_supports_aggregates() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let queries =
        ["SELECT COUNT(*) FROM users", "SELECT MAX(id) FROM users", "SELECT SUM(id) FROM users", "SELECT AVG(id) FROM users"];
    for q in &queries {
        let r = yyds_gateway::bind_sql_catalog(q, &catalog, 7).and_then(|b| yyds_gateway::plan_statement(b, &catalog));
        match r {
            Ok(_) => eprintln!("{q}: supported ✅"),
            Err(e) => eprintln!("{q}: blocked ❌ — {e:?}"),
        }
    }
}

#[test]
fn probe_planner_supports_or_in_where() {
    // WHERE a = 1 OR b = 2  — OR 树能否正确走 remaining_predicate
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let plan = yyds_gateway::bind_sql_catalog("SELECT id FROM users WHERE name = 'alice' OR age > 20", &catalog, 7)
        .and_then(|b| yyds_gateway::plan_statement(b, &catalog))
        .expect("plan");
    match plan {
        yyds_gateway::SqlExecutionPlan::Scan(_) => eprintln!("OR in WHERE: Scan with remaining_predicate ✅"),
        other => panic!("expected Scan, got {other:?}"),
    }
}

#[test]
fn sql_update_rejects_when_remaining_predicate_fails() {
    // 预置 row: id=9, name='original', age=20
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = make_executor_with_row("table:1", b"9", &[(1, b"9"), (2, b"original"), (3, b"20")]);

    // UPDATE users SET name='updated' WHERE id = 9 AND age = 99
    // age=99 与实际 age=20 不匹配 — 应该影响 0 行且数据不变
    let bound = yyds_gateway::bind_sql_catalog("UPDATE users SET name = 'updated' WHERE id = 9 AND age = 99", &catalog, 7)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("update");

    assert_eq!(result.affected_rows, 0, "remaining predicate mismatch should affect 0 rows");

    // SELECT 验证：数据保持原样
    let bound = yyds_gateway::bind_sql_catalog("SELECT name, age FROM users WHERE id = 9", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("select");

    assert_eq!(result.rows[0], vec![yyds_gateway::SqlValue::String("original".into()), yyds_gateway::SqlValue::Number(20.0),]);
}

#[test]
fn sql_update_succeeds_when_remaining_predicate_matches() {
    // 预置 row: id=9, name='original', age=20
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64 }\n");
    let executor = make_executor_with_row("table:1", b"9", &[(1, b"9"), (2, b"original"), (3, b"20")]);

    // UPDATE users SET name='updated' WHERE id = 9 AND age = 20 — 应该成功
    let bound = yyds_gateway::bind_sql_catalog("UPDATE users SET name = 'updated' WHERE id = 9 AND age = 20", &catalog, 7)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("update");

    assert_eq!(result.affected_rows, 1, "matching remaining predicate should affect 1 row");

    // SELECT 验证：name 已更新
    let bound = yyds_gateway::bind_sql_catalog("SELECT name FROM users WHERE id = 9", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("select");

    assert_eq!(result.rows[0][0], yyds_gateway::SqlValue::String("updated".into()));
}

#[test]
fn sql_delete_rejects_when_remaining_predicate_fails() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = make_executor_with_row("table:1", b"55", &[(1, b"55"), (2, b"victim")]);

    // DELETE FROM users WHERE id = 55 AND name = 'other' — name 不匹配
    let bound = yyds_gateway::bind_sql_catalog("DELETE FROM users WHERE id = 55 AND name = 'other'", &catalog, 7)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("delete");

    assert_eq!(result.affected_rows, 0, "remaining predicate mismatch should affect 0 rows");

    // SELECT 验证：数据仍然存在
    let bound = yyds_gateway::bind_sql_catalog("SELECT name FROM users WHERE id = 55", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("select");

    assert!(!result.rows.is_empty(), "row should still exist when predicate fails");
}

#[test]
fn utf8_field_preserves_leading_zeros_through_decode() {
    // utf8 字段存 "001" — 不应该被误解析成 Number(1.0)
    let catalog = catalog("table users { @@id: i64, code: utf8 }\n");
    let executor = make_executor_with_row("table:1", b"9", &[(1, b"9"), (2, b"001")]);

    let bound = yyds_gateway::bind_sql_catalog("SELECT code FROM users WHERE id = 9", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("select");

    assert_eq!(result.rows.len(), 1);
    // 关键字段：必须是 String 不是 Number！
    assert_eq!(result.rows[0][0], yyds_gateway::SqlValue::String("001".into()));
}

#[test]
fn utf8_field_preserves_empty_string() {
    // utf8 字段存空字符串 — 不能被当成 NULL 或 Number
    let catalog = catalog("table users { @@id: i64, label: utf8 }\n");
    let executor = make_executor_with_row("table:1", b"1", &[(1, b"1"), (2, b"")]);

    let bound = yyds_gateway::bind_sql_catalog("SELECT label FROM users WHERE id = 1", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("select");

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], yyds_gateway::SqlValue::String(String::new()));
}

#[test]
fn predicate_on_utf8_field_does_not_numeric_coerce() {
    // code='001' 但我们用 code=1 去查 — 应该返回空（类型不同不匹配）
    let catalog = catalog("table users { @@id: i64, code: utf8 }\n");
    let executor = make_executor_with_row("table:1", b"5", &[(1, b"5"), (2, b"001")]);

    // 类型正确的匹配
    let bound =
        yyds_gateway::bind_sql_catalog("SELECT code FROM users WHERE id = 5 AND code = '001'", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("select");
    assert_eq!(result.rows.len(), 1, "string literal match should find row");

    // 类型错误：用数字 literal 查字符串字段 — 应该不匹配（宽松比较不做自动类型转换）
    let bound = yyds_gateway::bind_sql_catalog("SELECT code FROM users WHERE id = 5 AND code = 1", &catalog, 7).expect("bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("select");
    assert_eq!(result.rows.len(), 0, "number literal should not coerce match against string field");
}

// --- 无 FROM 的 SELECT（驱动/ORM 连接探测）与标量内建函数 ---

/// 驱动探测链路上的常量 SELECT：不触碰任何 namespace。
fn evaluate_source_less(sql: &str) -> yyds_gateway::SqlResult {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = empty_executor();
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    assert!(
        matches!(plan, yyds_gateway::SqlExecutionPlan::Scalar(_)),
        "source-less SELECT should plan as Scalar, got {plan:?}"
    );
    yyds_gateway::execute_plan(&executor, None, &plan).expect("execute")
}

#[test]
fn sql_source_less_select_returns_constant_row() {
    let result = evaluate_source_less("SELECT 1");
    assert_eq!(result.columns, vec!["1".to_string()]);
    assert_eq!(result.rows, vec![vec![yyds_gateway::SqlValue::Number(1.0)]]);
    assert_eq!(result.affected_rows, 0);
}

#[test]
fn sql_system_variable_requires_a_dialect_resolver() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    // 没有变量表方言（PostgreSQL 等）必须明确拒绝 `@@`，不能退化成列名查找。
    let error = yyds_gateway::bind_sql_catalog("SELECT @@version", &catalog, 7).expect_err("no system variables");
    assert!(error.message.contains("system variable"), "{}", error.message);
}

#[test]
fn sql_system_variable_binds_through_injected_resolver() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\n");
    let executor = empty_executor();
    // 方言解析器拿到的是客户端原本拼写（含 `@@` 与作用域前缀）。
    let resolver = |name: &str| -> Result<yyds_gateway::SqlBoundLiteral, yyds_gateway::SqlFrontendError> {
        Ok(yyds_gateway::SqlBoundLiteral::String(name.trim_start_matches("@@").to_string()))
    };
    let bound = yyds_gateway::bind_sql_catalog_with_variables("SELECT @@version, @@session.sql_mode", &catalog, 7, &resolver)
        .expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    assert!(matches!(plan, yyds_gateway::SqlExecutionPlan::Scalar(_)));
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("execute");
    assert_eq!(result.rows.len(), 1);
    assert_eq!(
        result.rows[0],
        vec![
            yyds_gateway::SqlValue::String("version".to_string()),
            yyds_gateway::SqlValue::String("session.sql_mode".to_string()),
        ]
    );
}

#[test]
fn sql_source_less_select_projects_literals_with_default_labels() {
    let result = evaluate_source_less("SELECT 1 AS one, 'x' AS label, TRUE AS flag, NULL AS missing");
    assert_eq!(result.columns, vec!["one", "label", "flag", "missing"]);
    assert_eq!(
        result.rows[0],
        vec![
            yyds_gateway::SqlValue::Number(1.0),
            yyds_gateway::SqlValue::String("x".into()),
            yyds_gateway::SqlValue::Boolean(true),
            yyds_gateway::SqlValue::Null,
        ]
    );
}

#[test]
fn sql_source_less_select_evaluates_version_probe() {
    let result = evaluate_source_less("SELECT VERSION()");
    assert_eq!(result.columns, vec!["VERSION".to_string()]);
    match &result.rows[0][0] {
        yyds_gateway::SqlValue::String(banner) => assert!(banner.contains("yyds"), "unexpected banner: {banner}"),
        other => panic!("VERSION() should return text, got {other:?}"),
    }
}

#[test]
fn sql_source_less_select_evaluates_now_probe() {
    let result = evaluate_source_less("SELECT NOW()");
    match &result.rows[0][0] {
        yyds_gateway::SqlValue::String(timestamp) => {
            assert_eq!(timestamp.len(), 19, "expected `YYYY-MM-DD HH:MM:SS`, got {timestamp}");
            assert!(timestamp.as_bytes()[4] == b'-' && timestamp.as_bytes()[10] == b' ', "unexpected shape: {timestamp}");
        }
        other => panic!("NOW() should return text, got {other:?}"),
    }
}

#[test]
fn sql_source_less_select_evaluates_string_and_numeric_scalars() {
    let result = evaluate_source_less(
        "SELECT LOWER('ABC'), UPPER('abc'), LENGTH('hello'), COALESCE(NULL, 'fallback'), ABS(-3), ROUND(2.567, 2)",
    );
    assert_eq!(result.rows[0], vec![
        yyds_gateway::SqlValue::String("abc".into()),
        yyds_gateway::SqlValue::String("ABC".into()),
        yyds_gateway::SqlValue::Number(5.0),
        yyds_gateway::SqlValue::String("fallback".into()),
        yyds_gateway::SqlValue::Number(3.0),
        yyds_gateway::SqlValue::Number(2.57),
    ]);
}

#[test]
fn sql_source_less_select_honours_limit_zero() {
    let result = evaluate_source_less("SELECT 1 LIMIT 0");
    assert_eq!(result.columns, vec!["1".to_string()]);
    assert!(result.rows.is_empty(), "LIMIT 0 must suppress the constant row");
}

#[test]
fn sql_source_less_select_rejects_where_clause() {
    let catalog = catalog("table users { @@id: i64 }\n");
    let bound = yyds_gateway::bind_sql_catalog("SELECT 1 WHERE 1 = 1", &catalog, 7).expect("catalog bind");
    let error = yyds_gateway::plan_statement(bound, &catalog).expect_err("WHERE without FROM must be rejected");
    assert!(error.message.contains("requires a FROM source"), "unexpected message: {}", error.message);
}

#[test]
fn sql_source_less_select_keeps_unary_minus_sign() {
    // 回归：Oak 曾把前导 `-` 当成缺失左操作数的二元减号，`SELECT -3` 返回 3。
    let result = evaluate_source_less("SELECT -3, ABS(-3), 1 - -2");
    assert_eq!(result.rows[0], vec![
        yyds_gateway::SqlValue::Number(-3.0),
        yyds_gateway::SqlValue::Number(3.0),
        yyds_gateway::SqlValue::Number(3.0),
    ]);
}

// --- JOIN（内连接） ---

/// 多 namespace 的 executor：JOIN 需要同时扫描基表与连接表。
fn make_executor_with_tables(
    tables: &[(&str, &[(&[u8], &[(u64, &[u8])])])],
) -> yyds_execution::LocalExecutor<yyds_kv::MemoryShard> {
    use yyds_kv::{InlineValue, Key, KvStore, MemoryShard, StoredValue};
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let epoch = ShardEpoch(1);
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(epoch, vec![shard_id.clone()]).expect("routing");

    let mut shard = MemoryShard::new();
    for (namespace, rows) in tables {
        for (rowid, fields) in *rows {
            let mut values = yyds_gateway::RowFieldValues::new();
            for (field_id, bytes) in *fields {
                values.insert(*field_id, bytes.to_vec());
            }
            let row_bytes = yyds_gateway::encode_row(&values);
            shard.put(Key::new(*namespace, rowid.to_vec()), StoredValue::Inline(InlineValue(row_bytes))).expect("put row");
        }
    }

    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    yyds_execution::LocalExecutor::new(routing, shards).expect("executor")
}

/// `users`(type 1: id=1, name=2) 与 `orders`(type 2: id=3, user_id=4, total=5)。
fn join_fixture() -> (yyds_gateway::SqlCatalog, yyds_execution::LocalExecutor<yyds_kv::MemoryShard>) {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\ntable orders { @@id: i64, user_id: i64, total: i64 }\n");
    let executor = make_executor_with_tables(&[
        ("table:1", &[(b"1", &[(1, b"1"), (2, b"alice")]), (b"2", &[(1, b"2"), (2, b"bob")])]),
        (
            "table:2",
            &[
                (b"10", &[(3, b"10"), (4, b"1"), (5, b"100")]),
                (b"11", &[(3, b"11"), (4, b"1"), (5, b"50")]),
                (b"12", &[(3, b"12"), (4, b"2"), (5, b"70")]),
                (b"13", &[(3, b"13"), (4, b"9"), (5, b"5")]),
            ],
        ),
    ]);
    (catalog, executor)
}

#[test]
fn sql_inner_join_projects_both_sides_with_where_and_order_by() {
    let (catalog, executor) = join_fixture();
    let sql = "SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id \
               WHERE orders.total > 60 ORDER BY orders.total";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    assert!(matches!(plan, yyds_gateway::SqlExecutionPlan::Join(_)), "got {plan:?}");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("execute");

    assert_eq!(result.columns, vec!["name".to_string(), "total".to_string()]);
    assert_eq!(
        result.rows,
        vec![
            vec![yyds_gateway::SqlValue::String("bob".into()), yyds_gateway::SqlValue::Number(70.0)],
            vec![yyds_gateway::SqlValue::String("alice".into()), yyds_gateway::SqlValue::Number(100.0)],
        ]
    );
}

#[test]
fn sql_inner_join_star_expands_every_joined_table() {
    let (catalog, executor) = join_fixture();
    let sql = "SELECT * FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.total";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("execute");

    assert_eq!(result.columns, vec!["id".to_string(), "name".to_string(), "id".to_string(), "user_id".to_string(), "total".to_string()]);
    assert_eq!(result.rows.len(), 3, "orders.user_id = 9 has no users match");
    let totals: Vec<f64> = result
        .rows
        .iter()
        .map(|row| match &row[4] {
            yyds_gateway::SqlValue::Number(n) => *n,
            other => panic!("expected Number, got {other:?}"),
        })
        .collect();
    assert_eq!(totals, vec![50.0, 70.0, 100.0]);
    assert_eq!(result.rows[0][1], yyds_gateway::SqlValue::String("alice".into()));
}

#[test]
fn sql_inner_join_without_matching_rows_returns_empty() {
    let (catalog, executor) = join_fixture();

    // ON 条件本身不命中。
    let sql = "SELECT users.name FROM users JOIN orders ON users.id = orders.user_id AND orders.total > 1000";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("execute");
    assert!(result.rows.is_empty(), "ON filter should drop every pair");

    // JOIN 命中但 WHERE 全部过滤掉。
    let sql = "SELECT users.name FROM users JOIN orders ON users.id = orders.user_id WHERE orders.total > 1000";
    let bound = yyds_gateway::bind_sql_catalog(sql, &catalog, 7).expect("catalog bind");
    let plan = yyds_gateway::plan_statement(bound, &catalog).expect("plan");
    let result = yyds_gateway::execute_plan(&executor, None, &plan).expect("execute");
    assert!(result.rows.is_empty(), "WHERE filter should drop every joined row");
}

#[test]
fn sql_join_rejects_ambiguous_unqualified_columns() {
    let (catalog, _executor) = join_fixture();

    // `id` 在两张表里都有，未限定必须报歧义而不是随便选一张。
    let error = yyds_gateway::bind_sql_catalog(
        "SELECT id FROM users JOIN orders ON users.id = orders.user_id",
        &catalog,
        7,
    )
    .expect_err("ambiguous column");
    assert!(error.message.contains("ambiguous SQL column: id"), "{}", error.message);

    // 引用不存在表的限定名。
    let error = yyds_gateway::bind_sql_catalog(
        "SELECT admins.id FROM users JOIN orders ON users.id = orders.user_id",
        &catalog,
        7,
    )
    .expect_err("unknown qualifier");
    assert!(error.message.contains("unknown SQL table qualifier"), "{}", error.message);
}

#[test]
fn sql_join_rejects_outer_join_aggregates_and_self_join() {
    let (catalog, _executor) = join_fixture();

    let outer = yyds_gateway::bind_sql_catalog(
        "SELECT users.name FROM users LEFT JOIN orders ON users.id = orders.user_id",
        &catalog,
        7,
    )
    .expect_err("left join");
    assert!(outer.message.contains("INNER JOIN only"), "{}", outer.message);

    let self_join =
        yyds_gateway::bind_sql_catalog("SELECT users.name FROM users JOIN users ON users.id = users.id", &catalog, 7)
            .expect_err("self join");
    assert!(self_join.message.contains("self join"), "{}", self_join.message);

    let aggregate = yyds_gateway::bind_sql_catalog(
        "SELECT SUM(orders.total) FROM users JOIN orders ON users.id = orders.user_id",
        &catalog,
        7,
    )
    .expect("catalog bind");
    let error = yyds_gateway::plan_statement(aggregate, &catalog).expect_err("aggregate over join");
    assert!(error.message.contains("aggregate projections"), "{}", error.message);
}

#[test]
fn sql_scalar_function_binding_rejects_unknown_name_and_bad_arity() {
    let catalog = catalog("table users { @@id: i64 }\n");
    let unknown = yyds_gateway::bind_sql_catalog("SELECT NO_SUCH_FN(1)", &catalog, 7).expect_err("unknown fn");
    assert!(unknown.message.contains("unknown SQL function"), "unexpected message: {}", unknown.message);

    let arity = yyds_gateway::bind_sql_catalog("SELECT LOWER()", &catalog, 7).expect_err("arity");
    assert!(arity.message.contains("LOWER() expects 1 argument"), "unexpected message: {}", arity.message);
}
