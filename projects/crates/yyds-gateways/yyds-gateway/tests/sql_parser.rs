#[test]
fn oak_parse_errors_recovered_as_ast_nodes_are_reported() {
    yyds_gateway::parse_sql("SELECT 1").unwrap();
    assert!(yyds_gateway::parse_sql("SELECT (").is_err());
}

#[test]
fn oak_session_statements_are_projected_without_gateway_parsing() {
    use yyds_gateway::{SqlSessionCommand, SqlTransactionAction, parse_session_command};

    assert_eq!(
        parse_session_command("SET NAMES utf8mb4").unwrap(),
        Some(SqlSessionCommand::SetNames { character_set: "utf8mb4".into(), collation: None })
    );
    assert_eq!(
        parse_session_command("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci").unwrap(),
        Some(SqlSessionCommand::SetNames { character_set: "utf8mb4".into(), collation: Some("utf8mb4_general_ci".into()) })
    );
    assert_eq!(parse_session_command("BEGIN").unwrap(), Some(SqlSessionCommand::Transaction(SqlTransactionAction::Begin)));
    assert_eq!(parse_session_command("SELECT 1").unwrap(), None);
    assert!(parse_session_command("SET NAMES").is_err());
}
