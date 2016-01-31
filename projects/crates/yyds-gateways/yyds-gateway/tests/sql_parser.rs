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
    let SqlStatement::Select(select) = &root.statements[0]
    else {
        panic!("expected SELECT")
    };
    let predicate = select.expr.as_ref().expect("WHERE predicate");
    let Expression::Binary { op, right, .. } = predicate
    else {
        panic!("expected binary predicate, got {predicate:?}")
    };
    assert_eq!(*op, BinaryOperator::Greater);
    assert!(matches!(**right, Expression::Unary { op: UnaryOperator::Minus, .. }), "got {right:?}");

    let root = yyds_gateway::parse_sql("SELECT -3").expect("parse");
    let SqlStatement::Select(select) = &root.statements[0]
    else {
        panic!("expected SELECT")
    };
    assert!(matches!(
        select.items[0],
        oak_sql::ast::SelectItem::Expression { expr: Expression::Unary { op: UnaryOperator::Minus, .. }, .. }
    ));
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
    // MySQL clients quote the charset and collation literals.
    assert_eq!(
        parse_session_command("SET NAMES 'utf8mb4' COLLATE 'utf8mb4_general_ci'").unwrap(),
        Some(SqlSessionCommand::SetNames { character_set: "utf8mb4".into(), collation: Some("utf8mb4_general_ci".into()) })
    );
    assert!(parse_session_command("SET NAMES 'utf8mb4'; DROP TABLE users").is_err());
    assert_eq!(parse_session_command("SELECT 1").unwrap(), None);
    assert!(parse_session_command("SET NAMES").is_err());
}

#[test]
fn oak_session_variable_statements_are_projected() {
    use yyds_gateway::{parse_session_command, SqlBoundExpression, SqlBoundLiteral, SqlSessionCommand, SqlVariableScope};

    assert_eq!(
        parse_session_command("SHOW VARIABLES").unwrap(),
        Some(SqlSessionCommand::ShowVariables { scope: None, pattern: None })
    );
    assert_eq!(
        parse_session_command("SHOW SESSION VARIABLES LIKE 'auto%'").unwrap(),
        Some(SqlSessionCommand::ShowVariables { scope: Some(SqlVariableScope::Session), pattern: Some("auto%".into()) })
    );
    assert_eq!(
        parse_session_command("SHOW server_version").unwrap(),
        Some(SqlSessionCommand::ShowParameter { scope: None, name: "server_version".into() })
    );
    assert_eq!(
        parse_session_command("SET autocommit = 0").unwrap(),
        Some(SqlSessionCommand::SetVariable {
            scope: None,
            name: "autocommit".into(),
            value: SqlBoundExpression::Literal(SqlBoundLiteral::Integer(0)),
        })
    );
    assert_eq!(
        parse_session_command("SET SESSION sql_mode = 'STRICT_TRANS_TABLES'").unwrap(),
        Some(SqlSessionCommand::SetVariable {
            scope: Some(SqlVariableScope::Session),
            name: "sql_mode".into(),
            value: SqlBoundExpression::Literal(SqlBoundLiteral::String("STRICT_TRANS_TABLES".into())),
        })
    );
    assert_eq!(
        parse_session_command("PRAGMA journal_mode = WAL").unwrap(),
        Some(SqlSessionCommand::Pragma {
            name: "journal_mode".into(),
            value: Some(SqlBoundExpression::Identifier("WAL".into())),
        })
    );
    assert_eq!(
        parse_session_command("PRAGMA user_version").unwrap(),
        Some(SqlSessionCommand::Pragma { name: "user_version".into(), value: None })
    );
    // `SET NAMES` 仍是字符集协商，不能被当作普通变量赋值。
    assert!(matches!(parse_session_command("SET NAMES utf8mb4").unwrap(), Some(SqlSessionCommand::SetNames { .. })));
    assert!(parse_session_command("SET autocommit").is_err());
}
