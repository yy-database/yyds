use yyds_gateway::{
    ALL_GATEWAY_KINDS, FORBIDDEN_LEGACY_SURFACES, GatewayConfig, GatewayKind, SqlBoundExpression, SqlBoundLiteral,
    SqlBoundProjection, SqlBoundStatement, bind_sql,
};

fn catalog(source: &str) -> yyds_gateway::SqlCatalog {
    let document = vos::parser::parse_document(source).expect("VOS document");
    let snapshot = vos::catalog_from_document(&document).expect("VOS catalog");
    yyds_gateway::SqlCatalog::from_snapshot(7, &snapshot).expect("gateway catalog")
}

#[test]
fn sql_catalog_binding_consumes_published_yyds_identity() {
    let mut yyds_catalog = yyds_catalog::Catalog::open_memory();
    yyds_catalog
        .ensure_schema(7, "table users { @@id: i64 }\n")
        .expect("publish VOS schema");
    let catalog = yyds_gateway::SqlCatalog::from_yyds_catalog(&yyds_catalog)
        .expect("published identity");
    let bound = yyds_gateway::bind_sql_catalog("SELECT id FROM users", &catalog, 7)
        .expect("catalog bind");
    assert!(matches!(
        bound,
        yyds_gateway::SqlCatalogBoundStatement::Select(_)
    ));
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
    let SqlBoundStatement::Select(select) = bound;
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
fn sql_binding_rejects_non_select_and_unimplemented_clauses() {
    assert!(bind_sql("INSERT INTO users VALUES (1)").is_err());
    assert!(bind_sql("SELECT id FROM users ORDER BY id").is_err());
}

#[test]
fn sql_catalog_binding_resolves_vos_type_and_field_identity() {
    let catalog = catalog("table users { @@id: i64, name: utf8 }\ntable admins { @@id: i64 }\n");
    let bound = yyds_gateway::bind_sql_catalog(
        "SELECT id, name FROM users WHERE id = 7",
        &catalog,
        7,
    )
    .expect("catalog bind");
    let yyds_gateway::SqlCatalogBoundStatement::Select(select) = bound;
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
    let unknown_table = yyds_gateway::bind_sql_catalog("SELECT id FROM missing", &catalog, 7)
        .expect_err("unknown table");
    assert!(unknown_table.message.contains("unknown SQL table"));
    let unknown_column = yyds_gateway::bind_sql_catalog("SELECT missing FROM users", &catalog, 7)
        .expect_err("unknown column");
    assert!(unknown_column.message.contains("unknown SQL column"));
    let version = yyds_gateway::bind_sql_catalog("SELECT id FROM users", &catalog, 8)
        .expect_err("schema version mismatch");
    assert!(version.message.contains("schema version mismatch"));
}

#[test]
fn sql_catalog_binding_rejects_ambiguous_unqualified_columns() {
    let document = vos::parser::parse_document("table users { @@id: i64, name: i64 }\n")
        .expect("VOS document");
    let mut snapshot = vos::catalog_from_document(&document).expect("VOS catalog");
    snapshot.types[0].fields[1].current_name = "id".into();
    let catalog = yyds_gateway::SqlCatalog::from_snapshot(7, &snapshot).expect("gateway catalog");
    let error = yyds_gateway::bind_sql_catalog("SELECT id FROM users", &catalog, 7)
        .expect_err("ambiguous column");
    assert!(error.message.contains("ambiguous SQL column"));
}
