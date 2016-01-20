//! SQL 行为验收集。
//!
//! 每个用例都在独立 fixture 上执行：准备数据 → 执行语句 → 核对返回行、影响行数
//! 与执行后的数据库状态。用例数据只依赖 gateway 核心，不绑定任何 wire 协议。
//! 已知上游 `oaks` 缺口以 `pending` 标注并跳过，绝不伪造通过。

use std::sync::Arc;

use yyds_execution::{KeyValueExecutor, LocalExecutor, LocalTransactionHandle, TransactionHandle};
use yyds_gateway::{
    bind_sql_catalog, execute_plan, plan_statement, substitute_sql_parameters, SqlCatalog, SqlParameterStyle, SqlResult,
    SqlValue,
};
use yyds_kv::MemoryShard;

const SCHEMA_VERSION: u32 = 7;

/// 用例里的期望值 / 参数值。
#[derive(Debug, Clone, Copy, PartialEq)]
enum Lit {
    Int(i64),
    Float(f64),
    Text(&'static str),
    Bool(bool),
    Null,
}

impl Lit {
    fn value(self) -> SqlValue {
        match self {
            Self::Int(value) => SqlValue::Integer(value),
            Self::Float(value) => SqlValue::Number(value),
            Self::Text(value) => SqlValue::String(value.into()),
            Self::Bool(value) => SqlValue::Boolean(value),
            Self::Null => SqlValue::Null,
        }
    }
}

fn rows(rows: &[&[Lit]]) -> Vec<Vec<SqlValue>> {
    rows.iter().map(|row| row.iter().copied().map(Lit::value).collect()).collect()
}

struct Case {
    name: &'static str,
    setup: &'static [&'static str],
    sql: &'static str,
    parameters: &'static [Lit],
    style: SqlParameterStyle,
    expected_rows: &'static [&'static [Lit]],
    expected_affected_rows: u64,
    state_columns: &'static str,
    expected_state: &'static [&'static [Lit]],
    pending: Option<&'static str>,
}

struct Fixture {
    catalog: SqlCatalog,
    executor: Arc<dyn KeyValueExecutor>,
}

impl Fixture {
    fn new() -> Self {
        let catalog = catalog("table users { @@id: i64, name: utf8, age: i64, score: f64, active: bool, tag: utf8? }\n");
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

const DEFAULT_STATE_COLUMNS: &str = "id, name, age, score, active, tag";

/// 默认三行数据，覆盖空串、NULL、布尔与浮点。
const SEED: &[&str] = &[
    "INSERT INTO users VALUES (1, 'alice', 30, 1.5, TRUE, 'x')",
    "INSERT INTO users VALUES (2, 'bob', 20, 2.5, FALSE, NULL)",
    "INSERT INTO users VALUES (3, 'carol', 40, 3.25, TRUE, '')",
];

fn seed_state() -> &'static [&'static [Lit]] {
    &[
        &[Lit::Int(1), Lit::Text("alice"), Lit::Int(30), Lit::Float(1.5), Lit::Bool(true), Lit::Text("x")],
        &[Lit::Int(2), Lit::Text("bob"), Lit::Int(20), Lit::Float(2.5), Lit::Bool(false), Lit::Null],
        &[Lit::Int(3), Lit::Text("carol"), Lit::Int(40), Lit::Float(3.25), Lit::Bool(true), Lit::Text("")],
    ]
}

fn case(name: &'static str) -> CaseBuilder {
    CaseBuilder {
        case: Case {
            name,
            setup: SEED,
            sql: "",
            parameters: &[],
            style: SqlParameterStyle::QuestionMark,
            expected_rows: &[],
            expected_affected_rows: 0,
            state_columns: DEFAULT_STATE_COLUMNS,
            expected_state: seed_state(),
            pending: None,
        },
    }
}

struct CaseBuilder {
    case: Case,
}

impl CaseBuilder {
    fn setup(mut self, setup: &'static [&'static str]) -> Self {
        self.case.setup = setup;
        self
    }

    fn sql(mut self, sql: &'static str) -> Self {
        self.case.sql = sql;
        self
    }

    fn parameters(mut self, parameters: &'static [Lit]) -> Self {
        self.case.parameters = parameters;
        self
    }

    fn style(mut self, style: SqlParameterStyle) -> Self {
        self.case.style = style;
        self
    }

    fn rows(mut self, rows: &'static [&'static [Lit]]) -> Self {
        self.case.expected_rows = rows;
        self
    }

    fn affected(mut self, affected: u64) -> Self {
        self.case.expected_affected_rows = affected;
        self
    }

    fn state(mut self, columns: &'static str, state: &'static [&'static [Lit]]) -> Self {
        self.case.state_columns = columns;
        self.case.expected_state = state;
        self
    }

    fn build(self) -> Case {
        self.case
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
    use Lit::{Bool, Int, Null, Text};
    use SqlParameterStyle::{DollarNumber, QuestionMark};

    let cases = vec![
        case("select primary key")
            .sql("SELECT id, name FROM users WHERE id = 2")
            .rows(&[&[Int(2), Text("bob")]])
            .build(),
        case("select and predicate")
            .sql("SELECT id FROM users WHERE age > 20 AND active = TRUE ORDER BY id")
            .rows(&[&[Int(1)], &[Int(3)]])
            .build(),
        case("select or predicate")
            .sql("SELECT id FROM users WHERE age = 20 OR name = 'carol' ORDER BY id")
            .rows(&[&[Int(2)], &[Int(3)]])
            .build(),
        case("select not equal")
            .sql("SELECT id FROM users WHERE age <> 20 ORDER BY id")
            .rows(&[&[Int(1)], &[Int(3)]])
            .build(),
        case("select like prefix")
            .sql("SELECT id FROM users WHERE name LIKE 'a%'")
            .rows(&[&[Int(1)]])
            .build(),
        case("select like wildcard")
            .sql("SELECT id FROM users WHERE name LIKE '_ob'")
            .rows(&[&[Int(2)]])
            .build(),
        case("select in list")
            .sql("SELECT id FROM users WHERE age IN (20, 40) ORDER BY id")
            .rows(&[&[Int(2)], &[Int(3)]])
            .build(),
        case("select between")
            .sql("SELECT id FROM users WHERE age BETWEEN 20 AND 30 ORDER BY id")
            .rows(&[&[Int(1)], &[Int(2)]])
            .build(),
        case("select not predicate")
            .sql("SELECT id FROM users WHERE NOT (age = 20) ORDER BY id")
            .rows(&[&[Int(1)], &[Int(3)]])
            .build(),
        case("select is null")
            .sql("SELECT id FROM users WHERE tag IS NULL")
            .rows(&[&[Int(2)]])
            .build(),
        case("select is not null")
            .sql("SELECT id FROM users WHERE tag IS NOT NULL ORDER BY id")
            .rows(&[&[Int(1)], &[Int(3)]])
            .build(),
        case("select is true")
            .sql("SELECT id FROM users WHERE active IS TRUE ORDER BY id")
            .rows(&[&[Int(1)], &[Int(3)]])
            .build(),
        case("select is not true")
            .sql("SELECT id FROM users WHERE active IS NOT TRUE ORDER BY id")
            .rows(&[&[Int(2)]])
            .build(),
        case("select in subquery")
            .sql("SELECT id FROM users WHERE id IN (SELECT id FROM users WHERE age = 20)")
            .rows(&[&[Int(2)]])
            .build(),
        case("select null equality stays unknown")
            .sql("SELECT id FROM users WHERE tag = NULL")
            .rows(&[])
            .build(),
        case("select null three valued logic")
            .sql("SELECT id FROM users WHERE NULL AND TRUE")
            .rows(&[])
            .build(),
        case("select null not stays unknown")
            .sql("SELECT id FROM users WHERE NOT (NULL = NULL)")
            .rows(&[])
            .build(),
        case("parameter question mark numeric")
            .sql("SELECT id FROM users WHERE age >= ? ORDER BY id")
            .parameters(&[Int(30)])
            .style(QuestionMark)
            .rows(&[&[Int(1)], &[Int(3)]])
            .build(),
        case("parameter question mark text with leading zeros")
            .sql("SELECT id FROM users WHERE name = ?")
            .parameters(&[Text("007")])
            .style(QuestionMark)
            .rows(&[])
            .build(),
        case("parameter dollar number text and integer")
            .sql("SELECT id FROM users WHERE name = $1 AND age = $2")
            .parameters(&[Text("bob"), Int(20)])
            .style(DollarNumber)
            .rows(&[&[Int(2)]])
            .build(),
        case("parameter dollar number null")
            .sql("SELECT id FROM users WHERE tag = $1")
            .parameters(&[Null])
            .style(DollarNumber)
            .rows(&[])
            .build(),
        case("parameter dollar number boolean")
            .sql("SELECT id FROM users WHERE active = $1 ORDER BY id")
            .parameters(&[Bool(true)])
            .style(DollarNumber)
            .rows(&[&[Int(1)], &[Int(3)]])
            .build(),
        case("insert row")
            .setup(&[])
            .sql("INSERT INTO users VALUES (1, 'alice', 30, 1.5, TRUE, 'x')")
            .affected(1)
            .state(
                DEFAULT_STATE_COLUMNS,
                &[&[Int(1), Text("alice"), Int(30), Lit::Float(1.5), Bool(true), Text("x")]],
            )
            .build(),
        case("insert row with null")
            .setup(&[])
            .sql("INSERT INTO users VALUES (1, 'alice', 30, 1.5, TRUE, NULL)")
            .affected(1)
            .state(
                DEFAULT_STATE_COLUMNS,
                &[&[Int(1), Text("alice"), Int(30), Lit::Float(1.5), Bool(true), Null]],
            )
            .build(),
        case("update one row")
            .sql("UPDATE users SET age = 31 WHERE id = 1")
            .affected(1)
            .state(
                DEFAULT_STATE_COLUMNS,
                &[
                    &[Int(1), Text("alice"), Int(31), Lit::Float(1.5), Bool(true), Text("x")],
                    &[Int(2), Text("bob"), Int(20), Lit::Float(2.5), Bool(false), Null],
                    &[Int(3), Text("carol"), Int(40), Lit::Float(3.25), Bool(true), Text("")],
                ],
            )
            .build(),
        case("update compound predicate")
            .sql("UPDATE users SET tag = 'vip' WHERE id = 3 AND active = TRUE")
            .affected(1)
            .state(
                DEFAULT_STATE_COLUMNS,
                &[
                    &[Int(1), Text("alice"), Int(30), Lit::Float(1.5), Bool(true), Text("x")],
                    &[Int(2), Text("bob"), Int(20), Lit::Float(2.5), Bool(false), Null],
                    &[Int(3), Text("carol"), Int(40), Lit::Float(3.25), Bool(true), Text("vip")],
                ],
            )
            .build(),
        case("update skipped by remaining predicate")
            .sql("UPDATE users SET age = 99 WHERE id = 2 AND active = TRUE")
            .affected(0)
            .build(),
        case("update by non primary key predicate")
            .sql("UPDATE users SET age = 99 WHERE name = 'alice'")
            .affected(1)
            .state(
                DEFAULT_STATE_COLUMNS,
                &[
                    &[Int(1), Text("alice"), Int(99), Lit::Float(1.5), Bool(true), Text("x")],
                    &[Int(2), Text("bob"), Int(20), Lit::Float(2.5), Bool(false), Null],
                    &[Int(3), Text("carol"), Int(40), Lit::Float(3.25), Bool(true), Text("")],
                ],
            )
            .build(),
        case("update rewrites every matching row")
            .sql("UPDATE users SET tag = 'vip' WHERE active = TRUE")
            .affected(2)
            .state(
                DEFAULT_STATE_COLUMNS,
                &[
                    &[Int(1), Text("alice"), Int(30), Lit::Float(1.5), Bool(true), Text("vip")],
                    &[Int(2), Text("bob"), Int(20), Lit::Float(2.5), Bool(false), Null],
                    &[Int(3), Text("carol"), Int(40), Lit::Float(3.25), Bool(true), Text("vip")],
                ],
            )
            .build(),
        case("update by primary key range")
            .sql("UPDATE users SET age = 18 WHERE age > 20")
            .affected(2)
            .state(
                DEFAULT_STATE_COLUMNS,
                &[
                    &[Int(1), Text("alice"), Int(18), Lit::Float(1.5), Bool(true), Text("x")],
                    &[Int(2), Text("bob"), Int(20), Lit::Float(2.5), Bool(false), Null],
                    &[Int(3), Text("carol"), Int(18), Lit::Float(3.25), Bool(true), Text("")],
                ],
            )
            .build(),
        case("delete one row")
            .sql("DELETE FROM users WHERE id = 2")
            .affected(1)
            .state(
                DEFAULT_STATE_COLUMNS,
                &[
                    &[Int(1), Text("alice"), Int(30), Lit::Float(1.5), Bool(true), Text("x")],
                    &[Int(3), Text("carol"), Int(40), Lit::Float(3.25), Bool(true), Text("")],
                ],
            )
            .build(),
        case("delete skipped by remaining predicate")
            .sql("DELETE FROM users WHERE id = 2 AND age = 30")
            .affected(0)
            .build(),
        case("delete every matching row")
            .sql("DELETE FROM users WHERE active = TRUE")
            .affected(2)
            .state(
                DEFAULT_STATE_COLUMNS,
                &[&[Int(2), Text("bob"), Int(20), Lit::Float(2.5), Bool(false), Null]],
            )
            .build(),
        case("delete without predicate")
            .sql("DELETE FROM users")
            .affected(3)
            .state(DEFAULT_STATE_COLUMNS, &[])
            .build(),
    ];

    run_cases(&cases);
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
    assert_eq!(
        is_values("SELECT id FROM users WHERE tag IS NULL"),
        rows(&[&[Lit::Int(2)]])
    );
    assert_eq!(
        is_values("SELECT id FROM users WHERE active IS TRUE ORDER BY id"),
        rows(&[&[Lit::Int(1)], &[Lit::Int(3)]])
    );
    assert_eq!(
        is_values("SELECT id FROM users WHERE active IS NOT TRUE"),
        rows(&[&[Lit::Int(2)]])
    );
}

#[test]
fn sql_conformance_types_aggregates_and_paging() {
    use Lit::{Float, Int, Null, Text};

    let cases = vec![
        case("select preserves leading zeros")
            .setup(&["INSERT INTO users VALUES (1, '007', 30, 0.0, FALSE, '')"])
            .sql("SELECT name, tag FROM users WHERE id = 1")
            .rows(&[&[Text("007"), Text("")]])
            .state(
                DEFAULT_STATE_COLUMNS,
                &[&[Int(1), Text("007"), Int(30), Float(0.0), Lit::Bool(false), Text("")]],
            )
            .build(),
        case("select integer beyond f64 precision")
            .setup(&["INSERT INTO users VALUES (1, 'big', 9007199254740993, 0.0, FALSE, NULL)"])
            .sql("SELECT age FROM users WHERE id = 1")
            .rows(&[&[Int(9_007_199_254_740_993)]])
            .state(
                DEFAULT_STATE_COLUMNS,
                &[&[Int(1), Text("big"), Int(9_007_199_254_740_993), Float(0.0), Lit::Bool(false), Null]],
            )
            .build(),
        case("select int64 max")
            .setup(&["INSERT INTO users VALUES (1, 'max', 9223372036854775807, 0.0, FALSE, NULL)"])
            .sql("SELECT age FROM users WHERE age = 9223372036854775807")
            .rows(&[&[Int(i64::MAX)]])
            .state(
                DEFAULT_STATE_COLUMNS,
                &[&[Int(1), Text("max"), Int(i64::MAX), Float(0.0), Lit::Bool(false), Null]],
            )
            .build(),
        case("select float")
            .sql("SELECT score FROM users WHERE id = 2")
            .rows(&[&[Float(2.5)]])
            .build(),
        case("select null column")
            .sql("SELECT tag FROM users WHERE id = 2")
            .rows(&[&[Null]])
            .build(),
        case("count star")
            .sql("SELECT COUNT(*) FROM users")
            .rows(&[&[Int(3)]])
            .build(),
        case("count column")
            .sql("SELECT COUNT(tag) FROM users")
            .rows(&[&[Int(2)]])
            .build(),
        case("sum aggregate")
            .sql("SELECT SUM(age) FROM users")
            .rows(&[&[Int(90)]])
            .build(),
        case("avg aggregate")
            .sql("SELECT AVG(age) FROM users")
            .rows(&[&[Float(30.0)]])
            .build(),
        case("min and max aggregate")
            .sql("SELECT MIN(age), MAX(age) FROM users")
            .rows(&[&[Int(20), Int(40)]])
            .build(),
        case("order by descending")
            .sql("SELECT id FROM users ORDER BY age DESC")
            .rows(&[&[Int(3)], &[Int(1)], &[Int(2)]])
            .build(),
        case("limit and offset")
            .sql("SELECT id FROM users ORDER BY id LIMIT 1 OFFSET 1")
            .rows(&[&[Int(2)]])
            .build(),
        case("limit zero returns no rows")
            .sql("SELECT id FROM users ORDER BY id LIMIT 0")
            .rows(&[])
            .build(),
        case("order by beyond f64 precision")
            .setup(&[
                "INSERT INTO users VALUES (1, 'a', 9007199254740992, 0.0, FALSE, NULL)",
                "INSERT INTO users VALUES (2, 'b', 9007199254740993, 0.0, FALSE, NULL)",
            ])
            .sql("SELECT age FROM users ORDER BY age DESC")
            .rows(&[&[Int(9_007_199_254_740_993)], &[Int(9_007_199_254_740_992)]])
            .state(
                DEFAULT_STATE_COLUMNS,
                &[
                    &[Int(1), Text("a"), Int(9_007_199_254_740_992), Float(0.0), Lit::Bool(false), Null],
                    &[Int(2), Text("b"), Int(9_007_199_254_740_993), Float(0.0), Lit::Bool(false), Null],
                ],
            )
            .build(),
        case("sum beyond f64 precision")
            .setup(&[
                "INSERT INTO users VALUES (1, 'a', 9007199254740992, 0.0, FALSE, NULL)",
                "INSERT INTO users VALUES (2, 'b', 1, 0.0, FALSE, NULL)",
            ])
            .sql("SELECT SUM(age) FROM users")
            .rows(&[&[Int(9_007_199_254_740_993)]])
            .state(
                DEFAULT_STATE_COLUMNS,
                &[
                    &[Int(1), Text("a"), Int(9_007_199_254_740_992), Float(0.0), Lit::Bool(false), Null],
                    &[Int(2), Text("b"), Int(1), Float(0.0), Lit::Bool(false), Null],
                ],
            )
            .build(),
    ];

    run_cases(&cases);
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
    let updated = fixture
        .execute(Some(handle.as_ref()), "UPDATE users SET age = 99 WHERE id = 1")
        .expect("transaction update");
    assert_eq!(updated.affected_rows, 1);
    let deleted = fixture
        .execute(Some(handle.as_ref()), "DELETE FROM users WHERE id = 2")
        .expect("transaction delete");
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

