#[test]
fn oak_parse_errors_recovered_as_ast_nodes_are_reported() {
    yyds_gateway::parse_sql("SELECT 1").unwrap();
    assert!(yyds_gateway::parse_sql("SELECT (").is_err());
}
