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
fn select_clauses_come_from_ast_fields() {
    use yyds_gateway::{bind_sql, SqlBoundExpression, SqlBoundStatement};

    let SqlBoundStatement::Select(select) = bind_sql("SELECT age, COUNT(*) FROM users GROUP BY 1").expect("bind")
    else {
        panic!("expected SELECT")
    };
    assert_eq!(select.group_by, Some(vec![SqlBoundExpression::Identifier("age".into())]));

    let SqlBoundStatement::Select(select) = bind_sql("SELECT age FROM users ORDER BY age DESC NULLS FIRST").expect("bind")
    else {
        panic!("expected SELECT")
    };
    assert!(select.order_by.is_some_and(|items| items.len() == 1));
}

#[test]
fn unsupported_upstream_forms_are_rejected_explicitly() {
    use yyds_gateway::bind_sql;

    for sql in [
        "SELECT DISTINCT ON (age) id FROM users",
        "SELECT age FROM users GROUP BY GROUPING SETS ((age))",
        "SELECT age FROM users GROUP BY ROLLUP(age)",
        "SELECT age FROM users GROUP BY CUBE(age)",
        "SELECT id FROM users UNION SELECT id FROM users",
        "SELECT id FROM users FOR UPDATE",
    ] {
        assert!(bind_sql(sql).is_err(), "`{sql}` must be rejected explicitly");
    }
}

/// Oak 现在把 `ON CONFLICT` / `RETURNING` / `INSERT ... SELECT` 建模进 AST。网关本轮
/// 不实现它们，必须显式拒绝，不能静默丢掉这些字段。
#[test]
fn insert_clauses_outside_the_subset_are_rejected_explicitly() {
    use yyds_gateway::{bind_sql, SqlErrorKind};

    let on_conflict =
        bind_sql("INSERT INTO users (name) VALUES ('John') ON CONFLICT (name) DO NOTHING").expect_err("ON CONFLICT must be rejected");
    assert_eq!(on_conflict.kind, SqlErrorKind::Unsupported);
    assert!(on_conflict.message.contains("ON CONFLICT"), "{}", on_conflict.message);

    let returning =
        bind_sql("INSERT INTO users (name) VALUES ('John') RETURNING id, name AS alias").expect_err("RETURNING must be rejected");
    assert_eq!(returning.kind, SqlErrorKind::Unsupported);
    assert!(returning.message.contains("RETURNING"), "{}", returning.message);

    let insert_select = bind_sql("INSERT INTO users (id, name) SELECT id, name FROM users").expect_err("INSERT ... SELECT must be rejected");
    assert_eq!(insert_select.kind, SqlErrorKind::Unsupported);
    assert!(insert_select.message.contains("INSERT ... SELECT"), "{}", insert_select.message);

    // 多行 VALUES 的每行必须与列数一致。
    let mismatched = bind_sql("INSERT INTO users (id, name) VALUES (1, 'one'), (2)").expect_err("ragged VALUES");
    assert!(mismatched.message.contains("INSERT"), "{}", mismatched.message);
}

#[test]
fn select_distinct_modifier_is_bound() {
    use yyds_gateway::{bind_sql, SqlBoundStatement};

    let SqlBoundStatement::Select(distinct) = bind_sql("SELECT DISTINCT id FROM users ORDER BY id").expect("bind")
    else {
        panic!("expected SELECT")
    };
    assert!(distinct.distinct);
    let SqlBoundStatement::Select(plain) = bind_sql("select id from users").expect("bind")
    else {
        panic!("expected SELECT")
    };
    assert!(!plain.distinct);
    let SqlBoundStatement::Select(all) = bind_sql("SELECT ALL id FROM users").expect("bind")
    else {
        panic!("expected SELECT")
    };
    assert!(!all.distinct);
    let SqlBoundStatement::Select(subquery) =
        bind_sql("SELECT id FROM users WHERE id IN (SELECT DISTINCT id FROM users)").expect("bind")
    else {
        panic!("expected SELECT")
    };
    let Some(yyds_gateway::SqlBoundExpression::InSubquery { select, .. }) = subquery.predicate
    else {
        panic!("expected IN (SELECT ...) predicate")
    };
    assert!(select.distinct);
    // `COUNT(DISTINCT ...)` 现已在网关绑定期通过。
    let r = bind_sql("SELECT COUNT(DISTINCT id) FROM users");
    assert!(r.is_ok(), "expected COUNT(DISTINCT) to bind");
    // `DISTINCT ON` 仍然不被支持。
    assert!(bind_sql("SELECT DISTINCT ON (id) id FROM users").is_err());
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
    // Both dialects also accept the verbose spelling for the same boundary.
    assert_eq!(
        parse_session_command("START TRANSACTION").unwrap(),
        Some(SqlSessionCommand::Transaction(SqlTransactionAction::Begin))
    );
    assert_eq!(
        parse_session_command("start transaction;").unwrap(),
        Some(SqlSessionCommand::Transaction(SqlTransactionAction::Begin))
    );
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
