//! SQL 行为验收集。
//!
//! 每个用例都在独立 fixture 上执行：准备数据 → 执行语句 → 核对返回行、影响行数
//! 与执行后的数据库状态。用例数据由 `yyds-gateway-conformance` 提供，同一份表也
//! 驱动真实 MySQL / PostgreSQL 客户端套件。已知上游 `oaks` 缺口以 `pending` 标注并
//! 跳过，绝不伪造通过。

use std::sync::Arc;

use yyds_execution::{KeyValueExecutor, LocalExecutor, LocalTransactionHandle, TransactionHandle};
use yyds_gateway::{
    bind_sql_catalog, execute_plan, plan_statement, substitute_sql_parameters, SqlCatalog, SqlParameterStyle,
    SqlResult, SqlValue,
};
use yyds_gateway_conformance::{
    crud_cases, rows, type_cases, Case, Lit, DEFAULT_STATE_COLUMNS, SCHEMA_VERSION, SEED, USERS_SCHEMA,
};
use yyds_kv::MemoryShard;

struct Fixture {
    catalog: SqlCatalog,
    executor: Arc<dyn KeyValueExecutor>,
}

impl Fixture {
    fn new() -> Self {
        let catalog = catalog(USERS_SCHEMA);
        let shard_id = yyds_types::ShardId("s1".into());
        let routing = yyds_types::ShardMap::new(yyds_types::ShardEpoch(1), vec![shard_id.clone()]).expect("routing");
        let mut shards = std::collections::HashMap::new();
        shards.insert(shard_id, MemoryShard::new());
        let executor = LocalExecutor::new(routing, shards).expect("executor");
        Self { catalog, executor: Arc::new(executor) }
    }

    fn execute(&self, transaction: Option<&dyn TransactionHandle>, sql: &str) -> Result<SqlResult, String> {
        let bound = bind_sql_catalog(sql, &self.catalog, SCHEMA_VERSION).map_err(|error| error.message)?;
        let plan = plan_statement(bound, &self.catalog).map_err(|error| error.message)?;
        execute_plan(self.executor.as_ref(), transaction, &plan).map_err(|error| error.message)
    }

    fn execute_with(&self, sql: &str, parameters: &[Lit], style: SqlParameterStyle) -> Result<SqlResult, String> {
        let values = parameters.iter().copied().map(Lit::value).collect::<Vec<_>>();
        let sql = substitute_sql_parameters(sql, &values, style).map_err(|error| error.message)?;
        self.execute(None, &sql)
    }

    fn snapshot(&self, columns: &str) -> Vec<Vec<SqlValue>> {
        self.execute(None, &format!("SELECT {columns} FROM users ORDER BY id"))
            .expect("state query")
            .rows
    }
}

fn catalog(source: &str) -> SqlCatalog {
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
    let manifest = vos::contract::IdentityManifest {
        format_version: vos::contract::IDENTITY_MANIFEST_VERSION.into(),
        types,
    };
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
        let fixture = Fixture::new();
        for sql in case.setup {
            fixture.execute(None, sql).unwrap_or_else(|error| panic!("{} setup `{sql}`: {error}", case.name));
        }
        let result = fixture
            .execute_with(case.sql, case.parameters, case.style)
            .unwrap_or_else(|error| panic!("{} `{}`: {error}", case.name, case.sql));
        assert_eq!(result.rows, rows(case.expected_rows), "{} returned rows for `{}`", case.name, case.sql);
        assert_eq!(
            result.affected_rows, case.expected_affected_rows,
            "{} affected rows for `{}`",
            case.name, case.sql
        );
        assert_eq!(
            fixture.snapshot(case.state_columns),
            rows(case.expected_state),
            "{} database state after `{}`",
            case.name,
            case.sql
        );
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

/// `IS` 谓词直接由 Oak 的 `IsNull` / `IsTruth` 节点绑定，网关不再从源码 span 文本
/// 回读关键字。这里只通过绑定后的表达式求值，验证 NULL 的三值逻辑。
#[test]
fn sql_is_predicates_follow_three_valued_logic() {
    let fixture = Fixture::new();

    let is_values = |sql: &str| fixture.execute(None, sql).unwrap_or_else(|error| panic!("`{sql}`: {error}")).rows;
    assert_eq!(
        is_values("SELECT NULL IS NULL, NULL IS NOT NULL, NULL IS TRUE, NULL IS NOT TRUE, NULL IS FALSE, NULL IS NOT FALSE"),
        rows(&[&[
            Lit::Bool(true),
            Lit::Bool(false),
            Lit::Bool(false),
            Lit::Bool(true),
            Lit::Bool(false),
            Lit::Bool(true),
        ]])
    );
    assert_eq!(
        is_values("SELECT TRUE IS TRUE, TRUE IS NOT TRUE, FALSE IS FALSE, FALSE IS NOT FALSE"),
        rows(&[&[Lit::Bool(true), Lit::Bool(false), Lit::Bool(true), Lit::Bool(false)]])
    );

    for sql in SEED {
        fixture.execute(None, sql).expect("seed");
    }
    assert_eq!(is_values("SELECT id FROM users WHERE tag IS NULL"), rows(&[&[Lit::Int(2)]]));
    assert_eq!(
        is_values("SELECT id FROM users WHERE active IS TRUE ORDER BY id"),
        rows(&[&[Lit::Int(1)], &[Lit::Int(3)]])
    );
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
    let visible = fixture
        .execute(Some(handle.as_ref()), "SELECT id, name FROM users WHERE id = 4")
        .expect("transaction read");
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
    let updated =
        fixture.execute(Some(handle.as_ref()), "UPDATE users SET age = 99 WHERE id = 1").expect("transaction update");
    assert_eq!(updated.affected_rows, 1);
    let deleted =
        fixture.execute(Some(handle.as_ref()), "DELETE FROM users WHERE id = 2").expect("transaction delete");
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
