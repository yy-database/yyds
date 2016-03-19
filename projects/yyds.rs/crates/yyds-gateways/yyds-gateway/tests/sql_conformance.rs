//! SQL 行为验收集。
//!
//! 每个用例都在独立 fixture 上执行：准备数据 → 执行语句 → 核对返回行、影响行数
//! 与执行后的数据库状态。用例数据由 `yyds-gateway-conformance` 提供，同一份表也
//! 驱动真实 MySQL / PostgreSQL 客户端套件。已知上游 `oaks` 缺口以 `pending` 标注并
//! 跳过，绝不伪造通过。

use std::sync::Arc;

use yyds_execution::{KeyValueExecutor, LocalExecutor, LocalTransactionHandle, TransactionHandle};
use yyds_gateway::{
    bind_sql_catalog, execute_plan, plan_statement_for_dialect, substitute_sql_parameters, SqlCatalog, SqlDialect,
    SqlErrorKind, SqlParameterStyle, SqlResult, SqlValue,
};
use yyds_gateway_conformance::{
    crud_cases, distinct_cases, group_cases, null_order_cases, order_cases, returning_cases, rows, set_operation_cases,
    type_cases, upsert_cases, Case, Driver, Lit, DEFAULT_STATE_COLUMNS, REJECTED_SQL, SCHEMA_VERSION, SEED,
    USERS_SCHEMA,
};
use yyds_kv::MemoryShard;

struct Fixture {
    catalog: SqlCatalog,
    executor: Arc<dyn KeyValueExecutor>,
    dialect: SqlDialect,
}

impl Fixture {
    fn new() -> Self {
        Self::with_dialect(SqlDialect::Postgres)
    }

    fn with_dialect(dialect: SqlDialect) -> Self {
        let catalog = catalog(USERS_SCHEMA);
        let shard_id = yyds_types::ShardId("s1".into());
        let routing = yyds_types::ShardMap::new(yyds_types::ShardEpoch(1), vec![shard_id.clone()]).expect("routing");
        let mut shards = std::collections::HashMap::new();
        shards.insert(shard_id, MemoryShard::new());
        let executor = LocalExecutor::new(routing, shards).expect("executor");
        Self { catalog, executor: Arc::new(executor), dialect }
    }

    fn execute(&self, transaction: Option<&dyn TransactionHandle>, sql: &str) -> Result<SqlResult, String> {
        let bound = bind_sql_catalog(sql, &self.catalog, SCHEMA_VERSION).map_err(|error| error.message)?;
        let plan = plan_statement_for_dialect(bound, &self.catalog, self.dialect).map_err(|error| error.message)?;
        execute_plan(self.executor.as_ref(), transaction, &plan, self.dialect).map_err(|error| error.message)
    }

    fn execute_with(&self, sql: &str, parameters: &[Lit], style: SqlParameterStyle) -> Result<SqlResult, String> {
        let values = parameters.iter().copied().map(Lit::value).collect::<Vec<_>>();
        let sql = substitute_sql_parameters(sql, &values, style).map_err(|error| error.message)?;
        self.execute(None, &sql)
    }

    fn snapshot(&self, columns: &str) -> Vec<Vec<SqlValue>> {
        self.execute(None, &format!("SELECT {columns} FROM users ORDER BY id")).expect("state query").rows
    }
}

fn catalog(source: &str) -> SqlCatalog {
    let projection = vos::parse_oak(source).expect("Oak VOS source").project_schema().expect("projection");
    let mut next_field_id = 1u64;
    let types = projection
        .types
        .iter()
        .enumerate()
        .map(|(type_index, entry)| vos::types::TypeIdentity {
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
                    vos::types::FieldIdentity {
                        canonical_name: field.canonical_name.clone(),
                        field_id,
                        virtual_field_index: Some(field_index as u32),
                    }
                })
                .collect(),
        })
        .collect();
    let manifest = vos::types::IdentityManifest { format_version: vos::types::IDENTITY_MANIFEST_VERSION.into(), types };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    SqlCatalog::from_resolved_contract(SCHEMA_VERSION, &contract).expect("gateway catalog")
}

fn run_cases(cases: &[Case]) {
    let mut pending = Vec::new();
    for case in cases {
        if let Some(reason) = case.pending {
            pending.push(format!("{}: {reason}", case.name));
            continue;
        }
        for (dialect, driver) in [(SqlDialect::Postgres, Driver::Postgres), (SqlDialect::Mysql, Driver::Mysql)] {
            if case.only.is_some_and(|only| only != driver) {
                continue;
            }
            let fixture = Fixture::with_dialect(dialect);
            for sql in case.setup {
                fixture.execute(None, sql).unwrap_or_else(|error| panic!("{} setup `{sql}`: {error}", case.name));
            }
            let result = fixture
                .execute_with(case.sql, case.parameters, case.style)
                .unwrap_or_else(|error| panic!("{} `{}` under {driver:?}: {error}", case.name, case.sql));
            assert_eq!(
                result.rows,
                rows(case.expected_rows_for(driver)),
                "{} returned rows for `{}` under {driver:?}",
                case.name,
                case.sql
            );
            assert_eq!(
                result.affected_rows, case.expected_affected_rows,
                "{} affected rows for `{}` under {driver:?}",
                case.name, case.sql
            );
            assert_eq!(
                fixture.snapshot(case.state_columns),
                rows(case.expected_state),
                "{} database state after `{}` under {driver:?}",
                case.name,
                case.sql
            );
        }
    }
    if pending.is_empty() {
        eprintln!("pending `oaks` / protocol gaps: none");
    }
    else {
        eprintln!("pending `oaks` / protocol gaps:");
        for item in pending {
            eprintln!("  - {item}");
        }
    }
}

#[test]
fn sql_conformance_crud_and_predicates() {
    run_cases(&crud_cases());
}

#[test]
fn sql_conformance_types_aggregates_and_paging() {
    run_cases(&type_cases());
}

#[test]
fn sql_conformance_distinct() {
    run_cases(&distinct_cases());
}

#[test]
fn sql_conformance_group_by_and_having() {
    run_cases(&group_cases());
}

#[test]
fn sql_conformance_order_by() {
    run_cases(&order_cases());
}

#[test]
fn sql_conformance_null_ordering() {
    run_cases(&null_order_cases());
}

#[test]
fn sql_conformance_set_operations() {
    run_cases(&set_operation_cases());
}

/// `ON CONFLICT DO NOTHING` keeps the stored row and reports zero affected rows,
/// while `DO UPDATE` rewrites it and reports one. `INSERT ... RETURNING` projects
/// the rows the statement actually wrote.
#[test]
fn sql_conformance_upsert_and_returning() {
    run_cases(&upsert_cases());
    run_cases(&returning_cases());
}

#[test]
fn sql_group_by_projection_errors_are_rejected() {
    let fixture = Fixture::new();
    for sql in [
        "SELECT name, COUNT(*) FROM users GROUP BY age",
        "SELECT name FROM users GROUP BY age",
        "SELECT * FROM users GROUP BY age",
        "SELECT age FROM users HAVING COUNT(*) > 1",
        "SELECT name, COUNT(*) FROM users GROUP BY age HAVING name = 'alice'",
        "SELECT name FROM users GROUP BY age ORDER BY name",
    ] {
        let error = fixture.execute(None, sql).expect_err(sql);
        assert!(!error.is_empty(), "`{sql}` must report a reason");
    }
}

#[test]
fn sql_distinct_unsupported_shapes_are_rejected() {
    let fixture = Fixture::new();
    for sql in [
        "SELECT DISTINCT age, COUNT(*) FROM users GROUP BY age",
        "SELECT DISTINCT COUNT(*) FROM users",
        "SELECT DISTINCT age FROM users ORDER BY id",
    ] {
        let error = fixture.execute(None, sql).expect_err(sql);
        assert!(!error.is_empty(), "`{sql}` must report a reason");
    }
}

/// 共享矩阵中列出的上游形式必须在本进程路径上被显式拒绝，而不是被静默改写或
/// 解析成另一种语义。真实客户端套件对同一批语句断言协议错误码。
#[test]
fn sql_rejected_upstream_forms_report_a_reason() {
    let fixture = Fixture::new();
    for (label, sql) in REJECTED_SQL {
        assert!(fixture.execute(None, sql).is_err(), "`{label}` must be rejected explicitly");
    }
}

/// 表别名与自连接在两方言下必须一致：同一份 SQL 都要接受，同一份歧义都要按
/// `AmbiguousColumn` / `AmbiguousTable` 拒绝。wire 层再各自映射成 MySQL `1052` 与
/// PostgreSQL `42702`，网关层不因方言而分叉。
#[test]
fn sql_table_alias_and_self_join_agree_across_dialects() {
    for dialect in [SqlDialect::Postgres, SqlDialect::Mysql] {
        let fixture = Fixture::with_dialect(dialect);
        for sql in SEED {
            fixture.execute(None, sql).expect("seed");
        }

        let aliased = fixture
            .execute(None, "SELECT u.name FROM users u WHERE u.id = 1")
            .unwrap_or_else(|error| panic!("aliased read under {dialect:?}: {error}"));
        assert_eq!(aliased.rows, rows(&[&[Lit::Text("alice")]]), "alias under {dialect:?}");

        // 同一张表通过两个别名自连接，两侧各自保留自己的列值。
        let self_joined = fixture
            .execute(None, "SELECT a.id, b.name FROM users a JOIN users b ON a.id = b.id + 1 ORDER BY a.id")
            .unwrap_or_else(|error| panic!("self join under {dialect:?}: {error}"));
        assert_eq!(
            self_joined.rows,
            rows(&[&[Lit::Int(2), Lit::Text("alice")], &[Lit::Int(3), Lit::Text("bob")]]),
            "self join under {dialect:?}"
        );

        let ambiguous_column =
            bind_sql_catalog("SELECT name FROM users a JOIN users b ON a.id = b.id", &fixture.catalog, SCHEMA_VERSION)
                .expect_err("ambiguous shared column");
        assert_eq!(ambiguous_column.kind, SqlErrorKind::AmbiguousColumn, "under {dialect:?}: {}", ambiguous_column.message);

        let ambiguous_table =
            bind_sql_catalog("SELECT users.name FROM users JOIN users ON users.id = users.id", &fixture.catalog, SCHEMA_VERSION)
                .expect_err("duplicate unaliased source");
        assert_eq!(ambiguous_table.kind, SqlErrorKind::AmbiguousTable, "under {dialect:?}: {}", ambiguous_table.message);
    }
}

/// 派生表在两方言下必须一致：网关把 `(SELECT ...) x` 扁平化成同一张基表，内层过滤
/// 并入外层 `WHERE`，所以没有任何方言分叉；无法扁平化的形状两方言都必须显式拒绝。
#[test]
fn sql_derived_table_agrees_across_dialects() {
    for dialect in [SqlDialect::Postgres, SqlDialect::Mysql] {
        let fixture = Fixture::with_dialect(dialect);
        for sql in SEED {
            fixture.execute(None, sql).expect("seed");
        }

        let filtered = fixture
            .execute(None, "SELECT x.name FROM (SELECT * FROM users WHERE id = 2) x")
            .unwrap_or_else(|error| panic!("filtered derived table under {dialect:?}: {error}"));
        assert_eq!(filtered.rows, rows(&[&[Lit::Text("bob")]]), "derived filter under {dialect:?}");

        // 内层逐列投影覆盖整表时与 `*` 等价，列序不影响外层限定名。
        let projected = fixture
            .execute(None, "SELECT x.id, x.age FROM (SELECT name, age, id, score, active, tag FROM users) x ORDER BY x.id")
            .unwrap_or_else(|error| panic!("derived projection under {dialect:?}: {error}"));
        assert_eq!(
            projected.rows,
            rows(&[&[Lit::Int(1), Lit::Int(30)], &[Lit::Int(2), Lit::Int(20)], &[Lit::Int(3), Lit::Int(40)]]),
            "derived projection under {dialect:?}"
        );

        let rejected = fixture
            .execute(None, "SELECT * FROM (SELECT * FROM users LIMIT 1) x")
            .expect_err("inner LIMIT cannot be flattened");
        assert!(rejected.contains("derived table"), "under {dialect:?}: {rejected}");
    }
}

/// `IS` 谓词直接由 Oak 的 `IsNull` / `IsTruth` 节点绑定，网关不再从源码 span 文本
/// 回读关键字。这里只通过绑定后的表达式求值，验证 NULL 的三值逻辑。
#[test]
fn sql_is_predicates_follow_three_valued_logic() {
    let fixture = Fixture::new();

    let is_values = |sql: &str| fixture.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}`: {error}")).rows;
    assert_eq!(
        is_values("SELECT NULL IS NULL, NULL IS NOT NULL, NULL IS TRUE, NULL IS NOT TRUE, NULL IS FALSE, NULL IS NOT FALSE"),
        rows(&[&[Lit::Bool(true), Lit::Bool(false), Lit::Bool(false), Lit::Bool(true), Lit::Bool(false), Lit::Bool(true),]])
    );
    assert_eq!(
        is_values("SELECT TRUE IS TRUE, TRUE IS NOT TRUE, FALSE IS FALSE, FALSE IS NOT FALSE"),
        rows(&[&[Lit::Bool(true), Lit::Bool(false), Lit::Bool(true), Lit::Bool(false)]])
    );

    for sql in SEED {
        fixture.execute(None, sql).expect("seed");
    }
    assert_eq!(is_values("SELECT id FROM users WHERE tag IS NULL"), rows(&[&[Lit::Int(2)]]));
    assert_eq!(is_values("SELECT id FROM users WHERE active IS TRUE ORDER BY id"), rows(&[&[Lit::Int(1)], &[Lit::Int(3)]]));
    assert_eq!(is_values("SELECT id FROM users WHERE active IS NOT TRUE"), rows(&[&[Lit::Int(2)]]));
}

/// 纯整数形式的字面量绑定为 `i64`，只在小数点或指数出现时才走浮点。
/// 超出 `i64` 的整数与无法用有限 `f64` 表示的字面量都直接报错。
#[test]
fn sql_numeric_literals_keep_integer_precision() {
    let fixture = Fixture::new();
    let values = |sql: &str| fixture.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}`: {error}")).rows;

    assert_eq!(values("SELECT 9007199254740993"), rows(&[&[Lit::Int(9_007_199_254_740_993)]]));
    assert_eq!(values("SELECT 9223372036854775807"), rows(&[&[Lit::Int(i64::MAX)]]));
    assert_eq!(values("SELECT -9223372036854775808"), rows(&[&[Lit::Int(i64::MIN)]]));
    assert_eq!(values("SELECT 1.0"), rows(&[&[Lit::Float(1.0)]]));
    assert_eq!(
        values("SELECT 9007199254740993 = 9007199254740992, 9007199254740993 - 9007199254740992"),
        rows(&[&[Lit::Bool(false), Lit::Int(1)]])
    );

    assert!(fixture.execute(None, "SELECT 9223372036854775808").is_err());
    assert!(fixture.execute(None, "SELECT 1e309").is_err());
}

#[test]
fn sql_conformance_transaction_commit_and_rollback() {
    use Lit::{Float, Int, Null, Text};

    let fixture = Fixture::new();
    for sql in SEED {
        fixture.execute(None, sql).expect("seed");
    }
    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(Arc::clone(&fixture.executor)));

    // 事务内写入与扫描：本事务可见，事务外不可见。
    let inserted = fixture
        .execute(Some(handle.as_ref()), "INSERT INTO users VALUES (4, 'dave', 25, 0.5, FALSE, NULL)")
        .expect("transaction insert");
    assert_eq!(inserted.affected_rows, 1);
    let visible = fixture.execute(Some(handle.as_ref()), "SELECT id, name FROM users WHERE id = 4").expect("transaction read");
    assert_eq!(visible.rows, rows(&[&[Int(4), Text("dave")]]));
    assert_eq!(
        fixture.execute(None, "SELECT id FROM users WHERE id = 4").expect("outside read").rows,
        Vec::<Vec<SqlValue>>::new()
    );

    // 提交后状态必须持久。`COMMIT` 是 wire 层会话命令，核心层直接调用 handle。
    handle.commit().expect("commit");
    assert_eq!(
        fixture.snapshot(DEFAULT_STATE_COLUMNS),
        rows(&[
            &[Int(1), Text("alice"), Int(30), Float(1.5), Lit::Bool(true), Text("x")],
            &[Int(2), Text("bob"), Int(20), Float(2.5), Lit::Bool(false), Null],
            &[Int(3), Text("carol"), Int(40), Float(3.25), Lit::Bool(true), Text("")],
            &[Int(4), Text("dave"), Int(25), Float(0.5), Lit::Bool(false), Null],
        ])
    );

    // 回滚丢弃全部写入与更新，状态回到提交后的快照。
    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(Arc::clone(&fixture.executor)));
    let updated = fixture.execute(Some(handle.as_ref()), "UPDATE users SET age = 99 WHERE id = 1").expect("transaction update");
    assert_eq!(updated.affected_rows, 1);
    let deleted = fixture.execute(Some(handle.as_ref()), "DELETE FROM users WHERE id = 2").expect("transaction delete");
    assert_eq!(deleted.affected_rows, 1);
    let rolled_back = fixture.execute(Some(handle.as_ref()), "SELECT COUNT(*) FROM users").expect("transaction state");
    assert_eq!(rolled_back.rows, rows(&[&[Int(3)]]));
    handle.rollback();
    assert_eq!(
        fixture.snapshot(DEFAULT_STATE_COLUMNS),
        rows(&[
            &[Int(1), Text("alice"), Int(30), Float(1.5), Lit::Bool(true), Text("x")],
            &[Int(2), Text("bob"), Int(20), Float(2.5), Lit::Bool(false), Null],
            &[Int(3), Text("carol"), Int(40), Float(3.25), Lit::Bool(true), Text("")],
            &[Int(4), Text("dave"), Int(25), Float(0.5), Lit::Bool(false), Null],
        ])
    );
}

#[test]
fn sql_multi_row_insert_values_are_split_and_executed_together() {
    use Lit::{Int, Text};

    let fixture = Fixture::new();
    let result = fixture
        .execute(
            None,
            "INSERT INTO users VALUES (101, 'ana', 31, 1.0, TRUE, 'x'), (102, 'bob2', 32, 2.0, FALSE, NULL), (103, 'carol2', 33, 3.0, TRUE, '')",
        )
        .expect("multi-row insert");
    assert_eq!(result.affected_rows, 3, "multi-row insert must affect 3 rows");

    let count = fixture.execute(None, "SELECT COUNT(*) FROM users").expect("count after insert");
    assert_eq!(count.rows, rows(&[&[Int(3)]]), "three rows must be visible");

    let all = fixture.execute(None, "SELECT id, name FROM users ORDER BY id").expect("ordered rows");
    assert_eq!(
        all.rows,
        rows(&[
            &[Int(101), Text("ana")],
            &[Int(102), Text("bob2")],
            &[Int(103), Text("carol2")],
        ])
    );
}

/// MySQL `/ 0` 与 `% 0` 返回 NULL 值（wire 层另上报 1365 + 22012）。
/// PostgreSQL `/ 0` 与 `% 0` 抛 DivisionByZero 错误。
#[test]
fn sql_division_by_zero_is_dialect_aware() {
    use Lit::Null;

    let mysql = Fixture::with_dialect(SqlDialect::Mysql);
    let mysql_vals = |sql: &str| {
        mysql.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}` under MySQL: {error}")).rows
    };
    assert_eq!(mysql_vals("SELECT 1/0"), rows(&[&[Null]]));
    assert_eq!(mysql_vals("SELECT 0/0"), rows(&[&[Null]]));
    assert_eq!(mysql_vals("SELECT 10 % 0"), rows(&[&[Null]]));
    assert_eq!(mysql_vals("SELECT 3.14 / 0"), rows(&[&[Null]]));
    assert_eq!(mysql_vals("SELECT 3.14 % 0"), rows(&[&[Null]]));

    let pg = Fixture::with_dialect(SqlDialect::Postgres);
    for sql in ["SELECT 1/0", "SELECT 0/0", "SELECT 10 % 0", "SELECT 3.14 / 0", "SELECT 3.14 % 0"] {
        let result = pg.execute(None, sql).expect_err(sql);
        assert!(
            result.contains("division by zero"),
            "`{sql}` under PostgreSQL must surface the division-by-zero reason got: {result}"
        );
    }
}

/// 普通除法与模运算走方言正确分支：MySQL 整数除法返回浮点，PostgreSQL 整数除法截断。
#[test]
fn sql_division_and_modulo_keep_dialect_defaults() {
    use Lit::{Float, Int};

    let mysql = Fixture::with_dialect(SqlDialect::Mysql);
    let mysql_vals = |sql: &str| {
        mysql.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}` under MySQL: {error}")).rows
    };
    assert_eq!(mysql_vals("SELECT 7 / 2"), rows(&[&[Float(3.5)]]));
    assert_eq!(mysql_vals("SELECT 7 % 2"), rows(&[&[Int(1)]]));
    assert_eq!(mysql_vals("SELECT -7 % 2"), rows(&[&[Int(-1)]]));
    assert_eq!(mysql_vals("SELECT 7.0 / 2"), rows(&[&[Float(3.5)]]));

    let pg = Fixture::with_dialect(SqlDialect::Postgres);
    let pg_vals = |sql: &str| pg.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}` under Postgres: {error}")).rows;
    assert_eq!(pg_vals("SELECT 7 / 2"), rows(&[&[Int(3)]]));
    assert_eq!(pg_vals("SELECT 7 % 2"), rows(&[&[Int(1)]]));
    assert_eq!(pg_vals("SELECT -7 % 2"), rows(&[&[Int(-1)]]));
    assert_eq!(pg_vals("SELECT 7.0 / 2"), rows(&[&[Float(3.5)]]));
}

#[test]
fn sql_conformance_case_when() {
    use Lit::{Float, Int, Null, Text};
    let fixture = Fixture::new();
    let vals = |sql: &str| fixture.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}`: {error}")).rows;

    // 单 WHEN 命中
    assert_eq!(vals("SELECT CASE WHEN 1 = 1 THEN 'yes' ELSE 'no' END"), rows(&[&[Text("yes")]]));

    // 单 WHEN 不命中走 ELSE
    assert_eq!(vals("SELECT CASE WHEN 1 = 2 THEN 'yes' ELSE 'no' END"), rows(&[&[Text("no")]]));

    // 多 WHEN 取第一个命中
    assert_eq!(
        vals("SELECT CASE WHEN 1 = 2 THEN 'a' WHEN 1 = 1 THEN 'b' ELSE 'c' END"),
        rows(&[&[Text("b")]])
    );

    // 无 ELSE：全部落空返回 NULL
    assert_eq!(vals("SELECT CASE WHEN 1 = 2 THEN 'hit' END"), rows(&[&[Null]]));

    // 条件为 NULL 继续下一分支
    assert_eq!(
        vals("SELECT CASE WHEN NULL THEN 'a' WHEN 1 = 1 THEN 'b' END"),
        rows(&[&[Text("b")]])
    );

    // THEN 分支求值为 NULL 也是合法返回
    assert_eq!(vals("SELECT CASE WHEN 1 = 1 THEN NULL ELSE 'no' END"), rows(&[&[Null]]));

    // 与数字运算组合
    assert_eq!(
        vals("SELECT CASE WHEN 1 = 1 THEN 10 ELSE 20 END + 5"),
        rows(&[&[Int(15)]])
    );
}

#[test]
fn sql_conformance_cast() {
    use Lit::{Float, Int, Null, Text};
    let fixture = Fixture::new();
    let vals = |sql: &str| fixture.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}`: {error}")).rows;

    // INTEGER → TEXT
    assert_eq!(vals("SELECT CAST(42 AS TEXT)"), rows(&[&[Text("42")]]));

    // TEXT → INTEGER
    assert_eq!(vals("SELECT CAST('99' AS INTEGER)"), rows(&[&[Int(99)]]));

    // REAL → INTEGER (truncation via i64 cast)
    assert_eq!(vals("SELECT CAST(3.7 AS INTEGER)"), rows(&[&[Int(3)]]));

    // INTEGER → REAL
    assert_eq!(vals("SELECT CAST(7 AS REAL)"), rows(&[&[Float(7.0)]]));

    // TEXT → REAL
    assert_eq!(vals("SELECT CAST('3.14' AS FLOAT)"), rows(&[&[Float(3.14)]]));

    // NULL 输入始终返回 NULL
    assert_eq!(vals("SELECT CAST(NULL AS INTEGER)"), rows(&[&[Null]]));
    assert_eq!(vals("SELECT CAST(NULL AS TEXT)"), rows(&[&[Null]]));

    // 无法转换的字符串返回 NULL（与 SQLite 行为一致）
    assert_eq!(vals("SELECT CAST('oops' AS INTEGER)"), rows(&[&[Null]]));

    // 类型别名
    assert_eq!(vals("SELECT CAST(1 AS INT)"), rows(&[&[Int(1)]]));
    assert_eq!(vals("SELECT CAST('42' AS STRING)"), rows(&[&[Text("42")]]));

    // 布尔 → INTEGER
    assert_eq!(vals("SELECT CAST(TRUE AS INTEGER)"), rows(&[&[Int(1)]]));
    assert_eq!(vals("SELECT CAST(FALSE AS INT)"), rows(&[&[Int(0)]]));
}
