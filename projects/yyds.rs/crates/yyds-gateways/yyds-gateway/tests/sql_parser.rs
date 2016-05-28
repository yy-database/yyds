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
        "SELECT id FROM users FOR UPDATE",
    ] {
        assert!(bind_sql(sql).is_err(), "`{sql}` must be rejected explicitly");
    }
}

/// 集合运算的操作数逐个绑定，`operators` 与操作数一一对齐，
/// 合并结果上的 `ORDER BY` / `LIMIT` 挂在集合运算本身。
#[test]
fn set_operations_bind_every_operand_and_the_combined_tail() {
    use yyds_gateway::{bind_sql, SqlBoundSetOperator, SqlBoundStatement};

    for (sql, expected) in [
        ("SELECT id FROM users UNION SELECT id FROM users", SqlBoundSetOperator::Union),
        ("SELECT id FROM users UNION ALL SELECT id FROM users", SqlBoundSetOperator::UnionAll),
        ("SELECT id FROM users INTERSECT SELECT id FROM users", SqlBoundSetOperator::Intersect),
        ("SELECT id FROM users EXCEPT SELECT id FROM users", SqlBoundSetOperator::Except),
    ] {
        let SqlBoundStatement::SetOperation(set) = bind_sql(sql).expect("bind")
        else {
            panic!("expected set operation for `{sql}`")
        };
        assert_eq!(set.operands.len(), 2, "{sql}");
        assert_eq!(set.operators, vec![expected], "{sql}");
        assert!(set.order_by.is_none() && set.limit.is_none() && set.offset.is_none(), "{sql}");
    }

    let SqlBoundStatement::SetOperation(set) =
        bind_sql("SELECT id FROM users UNION SELECT id FROM users ORDER BY id DESC LIMIT 2 OFFSET 1").expect("bind")
    else {
        panic!("expected set operation")
    };
    assert!(set.order_by.is_some_and(|items| items.len() == 1));
    assert_eq!(set.limit, Some(2));
    assert_eq!(set.offset, Some(1));
}

/// `INTERSECT` 比 `UNION` / `UNION ALL` / `EXCEPT` 结合更紧，同级运算符左结合。
/// Oak 给出的是扁平链，bind 阶段把它折成固定形状的树，planner 只按树计划。
#[test]
fn set_operations_fold_intersect_before_the_rest() {
    use yyds_gateway::{bind_sql, SqlBoundSetOperand, SqlBoundSetOperator, SqlBoundStatement};

    // 递归展开一层的运算符序列，叶子用 `None` 表示。
    fn shape(operand: &SqlBoundSetOperand) -> Vec<Option<SqlBoundSetOperator>> {
        match operand {
            SqlBoundSetOperand::Select(_) => vec![None],
            SqlBoundSetOperand::SetOperation(set) => {
                let mut out = Vec::new();
                for (index, operand) in set.operands.iter().enumerate() {
                    if index > 0 {
                        out.push(Some(set.operators[index - 1]));
                    }
                    out.extend(shape(operand));
                }
                out
            }
        }
    }

    fn top_level(sql: &str) -> (Vec<SqlBoundSetOperand>, Vec<SqlBoundSetOperator>) {
        let SqlBoundStatement::SetOperation(set) = bind_sql(sql).expect("bind")
        else {
            panic!("expected set operation for `{sql}`")
        };
        (set.operands, set.operators)
    }

    // `A UNION ALL B INTERSECT C` = `A UNION ALL (B INTERSECT C)`
    let sql = "SELECT id FROM users UNION ALL SELECT id FROM users INTERSECT SELECT id FROM users";
    let (operands, operators) = top_level(sql);
    assert_eq!(operands.len(), 2, "{sql}");
    assert_eq!(operators, vec![SqlBoundSetOperator::UnionAll], "{sql}");
    assert_eq!(
        shape(&operands[1]),
        vec![None, Some(SqlBoundSetOperator::Intersect), None],
        "`{sql}` must group the INTERSECT segment"
    );

    // `A UNION B EXCEPT C INTERSECT D` = `(A UNION B) EXCEPT (C INTERSECT D)`，同级左结合
    let sql = "SELECT id FROM users UNION SELECT id FROM users EXCEPT SELECT id FROM users INTERSECT SELECT id FROM users";
    let (operands, operators) = top_level(sql);
    assert_eq!(operands.len(), 3, "{sql}");
    assert_eq!(operators, vec![SqlBoundSetOperator::Union, SqlBoundSetOperator::Except], "{sql}");
    assert_eq!(shape(&operands[1]), vec![None], "{sql}");
    assert_eq!(
        shape(&operands[2]),
        vec![None, Some(SqlBoundSetOperator::Intersect), None],
        "`{sql}` must group the trailing INTERSECT segment"
    );

    // 整条链只有 `INTERSECT` 时，折叠出的层级直接成为顶层节点。
    let sql = "SELECT id FROM users INTERSECT SELECT id FROM users INTERSECT SELECT id FROM users";
    let SqlBoundStatement::SetOperation(set) = bind_sql(sql).expect("bind")
    else {
        panic!("expected set operation for `{sql}`")
    };
    assert_eq!(set.operands.len(), 3, "{sql}");
    assert_eq!(set.operators, vec![SqlBoundSetOperator::Intersect, SqlBoundSetOperator::Intersect], "{sql}");
}

/// Oak 把 `ON CONFLICT` / `RETURNING` / `INSERT ... SELECT` 建模进 AST。网关 bind 层保留
/// 冲突目标与动作、`RETURNING` 投影，方言与唯一约束校验留给 planner。
#[test]
fn insert_clauses_are_bound_for_dialect_planning() {
    use yyds_gateway::{bind_sql, SqlBoundConflictAction, SqlBoundProjection, SqlBoundStatement};

    let do_update =
        bind_sql("INSERT INTO users (name) VALUES ('John') ON CONFLICT (name) DO UPDATE SET name = 'Jane' WHERE name = 'John'")
            .expect("ON CONFLICT DO UPDATE binds");
    let SqlBoundStatement::Insert(insert) = do_update
    else {
        panic!("expected insert");
    };
    let conflict = insert.conflict.expect("conflict clause is retained");
    assert_eq!(conflict.columns, vec!["name".to_string()]);
    let SqlBoundConflictAction::DoUpdate { assignments, predicate } = conflict.action
    else {
        panic!("expected DO UPDATE");
    };
    assert_eq!(assignments.len(), 1);
    assert_eq!(assignments[0].column, "name");
    assert!(predicate.is_some(), "the DO UPDATE WHERE predicate is retained");

    let do_nothing =
        bind_sql("INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT (id) DO NOTHING").expect("DO NOTHING binds");
    let SqlBoundStatement::Insert(insert) = do_nothing
    else {
        panic!("expected insert");
    };
    let conflict = insert.conflict.expect("conflict clause is retained");
    assert_eq!(conflict.columns, vec!["id".to_string()]);
    assert!(matches!(conflict.action, SqlBoundConflictAction::DoNothing));

    let no_target =
        bind_sql("INSERT INTO users (id, name) VALUES (1, 'John') ON CONFLICT DO NOTHING").expect("target is optional");
    let SqlBoundStatement::Insert(insert) = no_target
    else {
        panic!("expected insert");
    };
    assert_eq!(insert.conflict.expect("conflict clause is retained").columns, Vec::<String>::new());

    let returning = bind_sql("INSERT INTO users (name) VALUES ('John') RETURNING id, name AS alias").expect("RETURNING binds");
    let SqlBoundStatement::Insert(insert) = returning
    else {
        panic!("expected insert");
    };
    let items = insert.returning.expect("RETURNING list is retained");
    assert_eq!(items.len(), 2);
    let SqlBoundProjection::Expression { alias, .. } = &items[1]
    else {
        panic!("expected expression projection");
    };
    assert_eq!(alias.as_deref(), Some("alias"));

    let insert_select = bind_sql("INSERT INTO users (id, name) SELECT id, name FROM users").expect("INSERT ... SELECT binds");
    let SqlBoundStatement::Insert(insert) = insert_select
    else {
        panic!("expected insert");
    };
    assert_eq!(insert.table, "users");
    assert_eq!(insert.columns, Some(vec!["id".to_string(), "name".to_string()]));
    assert!(insert.rows.is_empty(), "a SELECT source has no VALUES rows");
    let select = insert.select.expect("SELECT source is retained");
    assert!(matches!(
        select.source.as_ref(),
        Some(yyds_gateway::SqlBoundSource::Table { name, alias: None }) if name == "users"
    ));
    assert_eq!(select.projections.len(), 2);

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
