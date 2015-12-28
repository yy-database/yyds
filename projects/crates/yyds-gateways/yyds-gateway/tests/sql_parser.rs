#[test]
fn oak_parse_errors_recovered_as_ast_nodes_are_reported() {
    yyds_gateway::parse_sql("SELECT 1").unwrap();
    assert!(yyds_gateway::parse_sql("SELECT (").is_err());
}

#[test]
fn oak_prefix_unary_minus_survives_parsing() {
    // 回归：前缀 `-` 曾被当作缺失左操作数的二元减号，现已解析成 Unary 节点。
    use oak_sql::ast::{BinaryOperator, Expression, SqlStatement, UnaryOperator};

    let root = yyds_gateway::parse_sql("SELECT id FROM users WHERE age > -1").expect("parse");
    let SqlStatement::Select(select) = &root.statements[0] else { panic!("expected SELECT") };
    let predicate = select.expr.as_ref().expect("WHERE predicate");
    let Expression::Binary { op, right, .. } = predicate else { panic!("expected binary predicate, got {predicate:?}") };
    assert_eq!(*op, BinaryOperator::Greater);
    assert!(matches!(**right, Expression::Unary { op: UnaryOperator::Minus, .. }), "got {right:?}");

    let root = yyds_gateway::parse_sql("SELECT -3").expect("parse");
    let SqlStatement::Select(select) = &root.statements[0] else { panic!("expected SELECT") };
    assert!(matches!(select.items[0], oak_sql::ast::SelectItem::Expression { expr: Expression::Unary { op: UnaryOperator::Minus, .. }, .. }));
}

#[test]
fn oak_session_statements_are_projected_without_gateway_parsing() {
    use yyds_gateway::{parse_session_command, SqlSessionCommand, SqlTransactionAction};

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
