//! Shared SQL behavior case set for the YYDS gateway adapters.
//!
//! One case table drives both acceptance paths: the in-process executor suite in
//! `yyds-gateway/tests/sql_conformance.rs` and the real-client suites in the
//! `yyds-gateway-mysql` / `yyds-gateway-pgsql` reference tests. Protocol harnesses
//! render these cases into their own client scripts instead of restating them, so
//! a behavior expectation is declared exactly once.
//!
//! A case that cannot run through a real driver is listed by
//! [`driver_exclusion`] with the protocol reason. Nothing is skipped silently.

use yyds_gateway::{SqlParameterStyle, SqlValue};

/// PostgreSQL schema source shared by every acceptance path.
pub const USERS_SCHEMA: &str = "table users { @@id: i64, name: utf8, age: i64, score: f64, active: bool, tag: utf8? }\n";

/// Schema version expected by the gateway binder in tests.
pub const SCHEMA_VERSION: u32 = 7;

/// Expected / bound value in a case.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Lit {
    /// Exact signed 64-bit integer.
    Int(i64),
    /// IEEE-754 double.
    Float(f64),
    /// UTF-8 text.
    Text(&'static str),
    /// SQL boolean.
    Bool(bool),
    /// SQL NULL.
    Null,
}

impl Lit {
    /// Converts the expectation into the executor's value type.
    pub fn value(self) -> SqlValue {
        match self {
            Self::Int(value) => SqlValue::Integer(value),
            Self::Float(value) => SqlValue::Number(value),
            Self::Text(value) => SqlValue::String(value.into()),
            Self::Bool(value) => SqlValue::Boolean(value),
            Self::Null => SqlValue::Null,
        }
    }
}

/// Converts a row of expectations into executor values.
pub fn rows(rows: &[&[Lit]]) -> Vec<Vec<SqlValue>> {
    rows.iter().map(|row| row.iter().copied().map(Lit::value).collect()).collect()
}

/// One SQL behavior case: fixture setup, statement, parameters, expected result
/// rows, affected rows and the database state that must hold afterwards.
#[derive(Debug, Clone)]
pub struct Case {
    /// Stable case name used by both suites in failure output.
    pub name: &'static str,
    /// Statements executed before the case statement.
    pub setup: &'static [&'static str],
    /// Statement under test.
    pub sql: &'static str,
    /// Bound parameter values.
    pub parameters: &'static [Lit],
    /// Placeholder style the statement is written in.
    pub style: SqlParameterStyle,
    /// Expected result rows.
    pub expected_rows: &'static [&'static [Lit]],
    /// Protocol-specific result rows when a dialect rule changes the order (NULL sorting).
    pub driver_rows: Option<(Driver, &'static [&'static [Lit]])>,
    /// Expected affected rows for writes.
    pub expected_affected_rows: u64,
    /// Columns projected for the state check.
    pub state_columns: &'static str,
    /// Expected rows of the state check.
    pub expected_state: &'static [&'static [Lit]],
    /// Known gap in an upstream component, or `None` when the case must pass.
    pub pending: Option<&'static str>,
}

impl Case {
    /// Rows this protocol must observe, honoring a dialect-specific override.
    pub fn expected_rows_for(&self, driver: Driver) -> &'static [&'static [Lit]] {
        match self.driver_rows {
            Some((overridden, rows)) if overridden == driver => rows,
            _ => self.expected_rows,
        }
    }
}

/// Default state projection covering text, NULL, boolean and floating values.
pub const DEFAULT_STATE_COLUMNS: &str = "id, name, age, score, active, tag";

/// Default three-row fixture data.
pub const SEED: &[&str] = &[
    "INSERT INTO users VALUES (1, 'alice', 30, 1.5, TRUE, 'x')",
    "INSERT INTO users VALUES (2, 'bob', 20, 2.5, FALSE, NULL)",
    "INSERT INTO users VALUES (3, 'carol', 40, 3.25, TRUE, '')",
];

/// Default fixture state.
pub fn seed_state() -> &'static [&'static [Lit]] {
    &[
        &[Lit::Int(1), Lit::Text("alice"), Lit::Int(30), Lit::Float(1.5), Lit::Bool(true), Lit::Text("x")],
        &[Lit::Int(2), Lit::Text("bob"), Lit::Int(20), Lit::Float(2.5), Lit::Bool(false), Lit::Null],
        &[Lit::Int(3), Lit::Text("carol"), Lit::Int(40), Lit::Float(3.25), Lit::Bool(true), Lit::Text("")],
    ]
}

/// Starts a case with the default fixture and state expectations.
pub fn case(name: &'static str) -> CaseBuilder {
    CaseBuilder {
        case: Case {
            name,
            setup: SEED,
            sql: "",
            parameters: &[],
            style: SqlParameterStyle::QuestionMark,
            expected_rows: &[],
            driver_rows: None,
            expected_affected_rows: 0,
            state_columns: DEFAULT_STATE_COLUMNS,
            expected_state: seed_state(),
            pending: None,
        },
    }
}

/// Builder for [`Case`] values.
pub struct CaseBuilder {
    case: Case,
}

impl CaseBuilder {
    /// Replaces the fixture setup statements.
    pub fn setup(mut self, setup: &'static [&'static str]) -> Self {
        self.case.setup = setup;
        self
    }

    /// Sets the statement under test.
    pub fn sql(mut self, sql: &'static str) -> Self {
        self.case.sql = sql;
        self
    }

    /// Sets the bound parameters.
    pub fn parameters(mut self, parameters: &'static [Lit]) -> Self {
        self.case.parameters = parameters;
        self
    }

    /// Sets the placeholder style of the statement.
    pub fn style(mut self, style: SqlParameterStyle) -> Self {
        self.case.style = style;
        self
    }

    /// Sets the expected result rows.
    pub fn rows(mut self, rows: &'static [&'static [Lit]]) -> Self {
        self.case.expected_rows = rows;
        self
    }

    /// Overrides the expected rows for one protocol when a dialect rule applies.
    pub fn rows_for(mut self, driver: Driver, rows: &'static [&'static [Lit]]) -> Self {
        self.case.driver_rows = Some((driver, rows));
        self
    }

    /// Sets the expected affected rows.
    pub fn affected(mut self, affected: u64) -> Self {
        self.case.expected_affected_rows = affected;
        self
    }

    /// Sets the state projection and its expected rows.
    pub fn state(mut self, columns: &'static str, state: &'static [&'static [Lit]]) -> Self {
        self.case.state_columns = columns;
        self.case.expected_state = state;
        self
    }

    /// Finalizes the case.
    pub fn build(self) -> Case {
        self.case
    }
}

/// Fixture with two NULL tags for dialect-specific NULL ordering.
pub const NULL_ORDER_SEED: &[&str] = &[
    "INSERT INTO users VALUES (1, 'alice', 30, 1.5, TRUE, 'x')",
    "INSERT INTO users VALUES (2, 'bob', 20, 2.5, FALSE, NULL)",
    "INSERT INTO users VALUES (3, 'carol', 40, 3.25, TRUE, '')",
    "INSERT INTO users VALUES (4, 'dave', 30, 0.5, FALSE, NULL)",
];

/// State after [`NULL_ORDER_SEED`].
pub fn null_order_state() -> &'static [&'static [Lit]] {
    &[
        &[Lit::Int(1), Lit::Text("alice"), Lit::Int(30), Lit::Float(1.5), Lit::Bool(true), Lit::Text("x")],
        &[Lit::Int(2), Lit::Text("bob"), Lit::Int(20), Lit::Float(2.5), Lit::Bool(false), Lit::Null],
        &[Lit::Int(3), Lit::Text("carol"), Lit::Int(40), Lit::Float(3.25), Lit::Bool(true), Lit::Text("")],
        &[Lit::Int(4), Lit::Text("dave"), Lit::Int(30), Lit::Float(0.5), Lit::Bool(false), Lit::Null],
    ]
}

/// `NULL` ordering: PostgreSQL sorts `NULL` after every value, MySQL before.
pub fn null_order_cases() -> Vec<Case> {
    use Lit::{Int, Null, Text};

    vec![
        case("null ordering ascending")
            .setup(NULL_ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, null_order_state())
            .sql("SELECT id FROM users ORDER BY tag ASC")
            .rows(&[&[Int(3)], &[Int(1)], &[Int(2)], &[Int(4)]])
            .rows_for(Driver::Mysql, &[&[Int(2)], &[Int(4)], &[Int(3)], &[Int(1)]])
            .build(),
        case("null ordering descending")
            .setup(NULL_ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, null_order_state())
            .sql("SELECT id FROM users ORDER BY tag DESC")
            .rows(&[&[Int(2)], &[Int(4)], &[Int(1)], &[Int(3)]])
            .rows_for(Driver::Mysql, &[&[Int(1)], &[Int(3)], &[Int(2)], &[Int(4)]])
            .build(),
        case("null ordering with tie break")
            .setup(NULL_ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, null_order_state())
            .sql("SELECT id FROM users ORDER BY tag ASC, id DESC")
            .rows(&[&[Int(3)], &[Int(1)], &[Int(4)], &[Int(2)]])
            .rows_for(Driver::Mysql, &[&[Int(4)], &[Int(2)], &[Int(3)], &[Int(1)]])
            .build(),
        case("null ordering with limit")
            .setup(NULL_ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, null_order_state())
            .sql("SELECT id FROM users ORDER BY tag ASC LIMIT 2")
            .rows(&[&[Int(3)], &[Int(1)]])
            .rows_for(Driver::Mysql, &[&[Int(2)], &[Int(4)]])
            .build(),
        case("null ordering after distinct")
            .setup(NULL_ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, null_order_state())
            .sql("SELECT DISTINCT tag FROM users ORDER BY tag")
            .rows(&[&[Text("")], &[Text("x")], &[Null]])
            .rows_for(Driver::Mysql, &[&[Null], &[Text("")], &[Text("x")]])
            .build(),
        case("null ordering after group by")
            .setup(NULL_ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, null_order_state())
            .sql("SELECT tag, COUNT(*) FROM users GROUP BY tag ORDER BY tag")
            .rows(&[&[Text(""), Int(1)], &[Text("x"), Int(1)], &[Null, Int(2)]])
            .rows_for(Driver::Mysql, &[&[Null, Int(2)], &[Text(""), Int(1)], &[Text("x"), Int(1)]])
            .build(),
        case("nulls first overrides ascending default")
            .setup(NULL_ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, null_order_state())
            .sql("SELECT id FROM users ORDER BY tag ASC NULLS FIRST")
            .rows(&[&[Int(2)], &[Int(4)], &[Int(3)], &[Int(1)]])
            .build(),
        case("nulls last overrides ascending default")
            .setup(NULL_ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, null_order_state())
            .sql("SELECT id FROM users ORDER BY tag ASC NULLS LAST")
            .rows(&[&[Int(3)], &[Int(1)], &[Int(2)], &[Int(4)]])
            .build(),
        case("nulls first overrides descending default")
            .setup(NULL_ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, null_order_state())
            .sql("SELECT id FROM users ORDER BY tag DESC NULLS FIRST")
            .rows(&[&[Int(2)], &[Int(4)], &[Int(1)], &[Int(3)]])
            .build(),
        case("nulls last overrides descending default")
            .setup(NULL_ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, null_order_state())
            .sql("SELECT id FROM users ORDER BY tag DESC NULLS LAST")
            .rows(&[&[Int(1)], &[Int(3)], &[Int(2)], &[Int(4)]])
            .build(),
    ]
}

/// Predicates, projections and parameter binding cases.
pub fn crud_cases() -> Vec<Case> {
    use Lit::{Bool, Int, Null, Text};
    use SqlParameterStyle::{DollarNumber, QuestionMark};

    vec![
        case("select primary key").sql("SELECT id, name FROM users WHERE id = 2").rows(&[&[Int(2), Text("bob")]]).build(),
        case("select and predicate")
            .sql("SELECT id FROM users WHERE age > 20 AND active = TRUE ORDER BY id")
            .rows(&[&[Int(1)], &[Int(3)]])
            .build(),
        case("select or predicate")
            .sql("SELECT id FROM users WHERE age = 20 OR name = 'carol' ORDER BY id")
            .rows(&[&[Int(2)], &[Int(3)]])
            .build(),
        case("select not equal").sql("SELECT id FROM users WHERE age <> 20 ORDER BY id").rows(&[&[Int(1)], &[Int(3)]]).build(),
        case("select like prefix").sql("SELECT id FROM users WHERE name LIKE 'a%'").rows(&[&[Int(1)]]).build(),
        case("select like wildcard").sql("SELECT id FROM users WHERE name LIKE '_ob'").rows(&[&[Int(2)]]).build(),
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
        case("select is null").sql("SELECT id FROM users WHERE tag IS NULL").rows(&[&[Int(2)]]).build(),
        case("select is not null")
            .sql("SELECT id FROM users WHERE tag IS NOT NULL ORDER BY id")
            .rows(&[&[Int(1)], &[Int(3)]])
            .build(),
        case("select is not true").sql("SELECT id FROM users WHERE active IS NOT TRUE").rows(&[&[Int(2)]]).build(),
        case("select in subquery")
            .sql("SELECT id FROM users WHERE id IN (SELECT id FROM users WHERE age = 20)")
            .rows(&[&[Int(2)]])
            .build(),
        case("select null equality stays unknown").sql("SELECT id FROM users WHERE tag = NULL").rows(&[]).build(),
        case("select null three valued logic").sql("SELECT id FROM users WHERE NULL AND TRUE").rows(&[]).build(),
        case("select null not stays unknown").sql("SELECT id FROM users WHERE NOT (NULL = NULL)").rows(&[]).build(),
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
        case("parameter question mark null")
            .sql("SELECT id FROM users WHERE tag = ?")
            .parameters(&[Null])
            .style(QuestionMark)
            .rows(&[])
            .build(),
        case("parameter question mark boolean")
            .sql("SELECT id FROM users WHERE active = ? ORDER BY id")
            .parameters(&[Bool(true)])
            .style(QuestionMark)
            .rows(&[&[Int(1)], &[Int(3)]])
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
            .state(DEFAULT_STATE_COLUMNS, &[&[Int(1), Text("alice"), Int(30), Lit::Float(1.5), Bool(true), Text("x")]])
            .build(),
        case("insert row with null")
            .setup(&[])
            .sql("INSERT INTO users VALUES (1, 'alice', 30, 1.5, TRUE, NULL)")
            .affected(1)
            .state(DEFAULT_STATE_COLUMNS, &[&[Int(1), Text("alice"), Int(30), Lit::Float(1.5), Bool(true), Null]])
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
        case("delete skipped by remaining predicate").sql("DELETE FROM users WHERE id = 2 AND age = 30").affected(0).build(),
        case("delete every matching row")
            .sql("DELETE FROM users WHERE active = TRUE")
            .affected(2)
            .state(DEFAULT_STATE_COLUMNS, &[&[Int(2), Text("bob"), Int(20), Lit::Float(2.5), Bool(false), Null]])
            .build(),
        case("delete without predicate").sql("DELETE FROM users").affected(3).state(DEFAULT_STATE_COLUMNS, &[]).build(),
    ]
}

/// Types, aggregates, ordering and paging cases.
pub fn type_cases() -> Vec<Case> {
    use Lit::{Float, Int, Null, Text};

    vec![
        case("select preserves leading zeros")
            .setup(&["INSERT INTO users VALUES (1, '007', 30, 0.0, FALSE, '')"])
            .sql("SELECT name, tag FROM users WHERE id = 1")
            .rows(&[&[Text("007"), Text("")]])
            .state(DEFAULT_STATE_COLUMNS, &[&[Int(1), Text("007"), Int(30), Float(0.0), Lit::Bool(false), Text("")]])
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
            .state(DEFAULT_STATE_COLUMNS, &[&[Int(1), Text("max"), Int(i64::MAX), Float(0.0), Lit::Bool(false), Null]])
            .build(),
        case("select float").sql("SELECT score FROM users WHERE id = 2").rows(&[&[Float(2.5)]]).build(),
        case("select null column").sql("SELECT tag FROM users WHERE id = 2").rows(&[&[Null]]).build(),
        case("count star").sql("SELECT COUNT(*) FROM users").rows(&[&[Int(3)]]).build(),
        case("count column").sql("SELECT COUNT(tag) FROM users").rows(&[&[Int(2)]]).build(),
        case("sum aggregate").sql("SELECT SUM(age) FROM users").rows(&[&[Int(90)]]).build(),
        case("avg aggregate").sql("SELECT AVG(age) FROM users").rows(&[&[Float(30.0)]]).build(),
        case("min and max aggregate").sql("SELECT MIN(age), MAX(age) FROM users").rows(&[&[Int(20), Int(40)]]).build(),
        case("order by descending")
            .sql("SELECT id FROM users ORDER BY age DESC")
            .rows(&[&[Int(3)], &[Int(1)], &[Int(2)]])
            .build(),
        case("limit and offset").sql("SELECT id FROM users ORDER BY id LIMIT 1 OFFSET 1").rows(&[&[Int(2)]]).build(),
        case("limit zero returns no rows").sql("SELECT id FROM users ORDER BY id LIMIT 0").rows(&[]).build(),
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
    ]
}

/// Fixture with tied sort keys for multi-column `ORDER BY` coverage.
pub const ORDER_SEED: &[&str] = &[
    "INSERT INTO users VALUES (1, 'alice', 30, 1.5, TRUE, 'x')",
    "INSERT INTO users VALUES (2, 'bob', 20, 2.5, FALSE, NULL)",
    "INSERT INTO users VALUES (3, 'carol', 40, 3.25, TRUE, '')",
    "INSERT INTO users VALUES (4, 'dave', 30, 0.5, FALSE, 'y')",
];

/// State after [`ORDER_SEED`].
pub fn order_state() -> &'static [&'static [Lit]] {
    &[
        &[Lit::Int(1), Lit::Text("alice"), Lit::Int(30), Lit::Float(1.5), Lit::Bool(true), Lit::Text("x")],
        &[Lit::Int(2), Lit::Text("bob"), Lit::Int(20), Lit::Float(2.5), Lit::Bool(false), Lit::Null],
        &[Lit::Int(3), Lit::Text("carol"), Lit::Int(40), Lit::Float(3.25), Lit::Bool(true), Lit::Text("")],
        &[Lit::Int(4), Lit::Text("dave"), Lit::Int(30), Lit::Float(0.5), Lit::Bool(false), Lit::Text("y")],
    ]
}

/// Multi-column and mixed-direction `ORDER BY` over tied sort keys.
pub fn order_cases() -> Vec<Case> {
    use Lit::Int;

    vec![
        case("order by mixed directions")
            .setup(ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, order_state())
            .sql("SELECT id FROM users ORDER BY active DESC, age ASC")
            .rows(&[&[Int(1)], &[Int(3)], &[Int(2)], &[Int(4)]])
            .build(),
        case("order by mixed directions reversed")
            .setup(ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, order_state())
            .sql("SELECT id FROM users ORDER BY active ASC, age DESC")
            .rows(&[&[Int(4)], &[Int(2)], &[Int(3)], &[Int(1)]])
            .build(),
        case("order by expression and descending tie break")
            .setup(ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, order_state())
            .sql("SELECT id, age FROM users ORDER BY age * -1 ASC, id DESC")
            .rows(&[&[Int(3), Int(40)], &[Int(4), Int(30)], &[Int(1), Int(30)], &[Int(2), Int(20)]])
            .build(),
        case("order by keeps scan order for ties")
            .setup(ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, order_state())
            .sql("SELECT id FROM users ORDER BY active ASC")
            .rows(&[&[Int(2)], &[Int(4)], &[Int(1)], &[Int(3)]])
            .build(),
        case("order by multiple columns with limit and offset")
            .setup(ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, order_state())
            .sql("SELECT id FROM users ORDER BY age DESC LIMIT 2 OFFSET 1")
            .rows(&[&[Int(1)], &[Int(4)]])
            .build(),
        case("order by descending columns")
            .setup(ORDER_SEED)
            .state(DEFAULT_STATE_COLUMNS, order_state())
            .sql("SELECT id FROM users ORDER BY active DESC, id DESC")
            .rows(&[&[Int(3)], &[Int(1)], &[Int(4)], &[Int(2)]])
            .build(),
    ]
}

/// Fixture with duplicate projected values for `DISTINCT` de-duplication.
pub const DISTINCT_SEED: &[&str] = &[
    "INSERT INTO users VALUES (1, 'alice', 30, 1.5, TRUE, 'x')",
    "INSERT INTO users VALUES (2, 'alice', 30, 1.5, FALSE, 'x')",
    "INSERT INTO users VALUES (3, 'carol', 20, 3.25, TRUE, NULL)",
    "INSERT INTO users VALUES (4, 'bob', 30, 0.5, FALSE, '')",
];

/// State after [`DISTINCT_SEED`].
pub fn distinct_state() -> &'static [&'static [Lit]] {
    &[
        &[Lit::Int(1), Lit::Text("alice"), Lit::Int(30), Lit::Float(1.5), Lit::Bool(true), Lit::Text("x")],
        &[Lit::Int(2), Lit::Text("alice"), Lit::Int(30), Lit::Float(1.5), Lit::Bool(false), Lit::Text("x")],
        &[Lit::Int(3), Lit::Text("carol"), Lit::Int(20), Lit::Float(3.25), Lit::Bool(true), Lit::Null],
        &[Lit::Int(4), Lit::Text("bob"), Lit::Int(30), Lit::Float(0.5), Lit::Bool(false), Lit::Text("")],
    ]
}

/// `SELECT DISTINCT` de-duplication over one or more projected columns.
pub fn distinct_cases() -> Vec<Case> {
    use Lit::{Float, Int, Text};

    vec![
        case("distinct single column")
            .setup(DISTINCT_SEED)
            .state(DEFAULT_STATE_COLUMNS, distinct_state())
            .sql("SELECT DISTINCT age FROM users ORDER BY age")
            .rows(&[&[Int(20)], &[Int(30)]])
            .build(),
        case("distinct exact float values")
            .setup(DISTINCT_SEED)
            .state(DEFAULT_STATE_COLUMNS, distinct_state())
            .sql("SELECT DISTINCT score FROM users ORDER BY score")
            .rows(&[&[Float(0.5)], &[Float(1.5)], &[Float(3.25)]])
            .build(),
        case("distinct multi column")
            .setup(DISTINCT_SEED)
            .state(DEFAULT_STATE_COLUMNS, distinct_state())
            .sql("SELECT DISTINCT name, age FROM users ORDER BY name, age")
            .rows(&[&[Text("alice"), Int(30)], &[Text("bob"), Int(30)], &[Text("carol"), Int(20)]])
            .build(),
        case("distinct with limit and offset")
            .setup(DISTINCT_SEED)
            .state(DEFAULT_STATE_COLUMNS, distinct_state())
            .sql("SELECT DISTINCT age FROM users ORDER BY age LIMIT 1 OFFSET 1")
            .rows(&[&[Int(30)]])
            .build(),
    ]
}

/// Fixture whose nullable `tag` column has one all-NULL group.
pub const NULL_GROUP_SEED: &[&str] = &[
    "INSERT INTO users VALUES (1, 'alice', 30, 1.5, TRUE, NULL)",
    "INSERT INTO users VALUES (2, 'bob', 20, 2.5, FALSE, NULL)",
];

/// State after [`NULL_GROUP_SEED`].
pub fn null_group_state() -> &'static [&'static [Lit]] {
    &[
        &[Lit::Int(1), Lit::Text("alice"), Lit::Int(30), Lit::Float(1.5), Lit::Bool(true), Lit::Null],
        &[Lit::Int(2), Lit::Text("bob"), Lit::Int(20), Lit::Float(2.5), Lit::Bool(false), Lit::Null],
    ]
}

/// `GROUP BY` / `HAVING` with real grouped result rows.
pub fn group_cases() -> Vec<Case> {
    use Lit::{Bool, Int, Null, Text};

    vec![
        case("group by single column count")
            .sql("SELECT active, COUNT(*) FROM users GROUP BY active ORDER BY active")
            .rows(&[&[Bool(false), Int(1)], &[Bool(true), Int(2)]])
            .build(),
        case("group by single column sum")
            .sql("SELECT active, SUM(age) FROM users GROUP BY active ORDER BY active")
            .rows(&[&[Bool(false), Int(20)], &[Bool(true), Int(70)]])
            .build(),
        case("group by nullable key")
            .sql("SELECT tag, COUNT(*) FROM users GROUP BY tag ORDER BY tag")
            .rows(&[&[Text(""), Int(1)], &[Text("x"), Int(1)], &[Null, Int(1)]])
            .rows_for(Driver::Mysql, &[&[Null, Int(1)], &[Text(""), Int(1)], &[Text("x"), Int(1)]])
            .build(),
        case("group by multiple columns")
            .sql("SELECT active, tag, COUNT(*) FROM users GROUP BY active, tag ORDER BY active, tag")
            .rows(&[&[Bool(false), Null, Int(1)], &[Bool(true), Text(""), Int(1)], &[Bool(true), Text("x"), Int(1)]])
            .build(),
        case("having aggregate predicate")
            .sql("SELECT active, COUNT(*) FROM users GROUP BY active HAVING COUNT(*) > 1 ORDER BY active")
            .rows(&[&[Bool(true), Int(2)]])
            .build(),
        case("having grouped column predicate")
            .sql("SELECT active, SUM(age) FROM users GROUP BY active HAVING active = TRUE ORDER BY active")
            .rows(&[&[Bool(true), Int(70)]])
            .build(),
        case("having excludes every group")
            .sql("SELECT tag, COUNT(*) FROM users GROUP BY tag HAVING COUNT(*) > 5")
            .rows(&[])
            .build(),
        case("group by order over aggregate")
            .sql("SELECT active, COUNT(*) FROM users GROUP BY active ORDER BY COUNT(*) DESC")
            .rows(&[&[Bool(true), Int(2)], &[Bool(false), Int(1)]])
            .build(),
        case("group by empty table")
            .setup(&[])
            .state(DEFAULT_STATE_COLUMNS, &[])
            .sql("SELECT age, COUNT(*) FROM users GROUP BY age")
            .rows(&[])
            .build(),
        case("group by all null values")
            .setup(NULL_GROUP_SEED)
            .state(DEFAULT_STATE_COLUMNS, null_group_state())
            .sql("SELECT tag, COUNT(*) FROM users GROUP BY tag")
            .rows(&[&[Null, Int(2)]])
            .build(),
        case("group by ordinal position")
            .sql("SELECT age, COUNT(*) FROM users GROUP BY 1 ORDER BY age")
            .rows(&[&[Int(20), Int(1)], &[Int(30), Int(1)], &[Int(40), Int(1)]])
            .build(),
        case("having without group by keeps aggregate row")
            .sql("SELECT COUNT(*) FROM users HAVING COUNT(*) > 2")
            .rows(&[&[Int(3)]])
            .build(),
        case("having without group by excludes aggregate row")
            .sql("SELECT COUNT(*) FROM users HAVING COUNT(*) > 3")
            .rows(&[])
            .build(),
    ]
}

/// Set-operation behavior: `UNION` de-duplicates, `UNION ALL` keeps duplicates,
/// `INTERSECT` keeps common rows and `EXCEPT` keeps the left-only rows.
///
/// Every case carries an `ORDER BY` on the combined result because the merged row
/// order is otherwise implementation-defined.
pub fn set_operation_cases() -> Vec<Case> {
    use Lit::Int;

    vec![
        case("union removes duplicates")
            .sql("SELECT age FROM users WHERE id <= 2 UNION SELECT age FROM users WHERE id >= 2 ORDER BY age")
            .rows(&[&[Int(20)], &[Int(30)], &[Int(40)]])
            .build(),
        case("union all keeps duplicates")
            .sql("SELECT id FROM users WHERE id <= 2 UNION ALL SELECT id FROM users WHERE id >= 2 ORDER BY id")
            .rows(&[&[Int(1)], &[Int(2)], &[Int(2)], &[Int(3)]])
            .build(),
        case("intersect keeps common rows once")
            .sql("SELECT id FROM users WHERE id <= 2 INTERSECT SELECT id FROM users WHERE id >= 2 ORDER BY id")
            .rows(&[&[Int(2)]])
            .build(),
        case("except keeps left only rows")
            .sql("SELECT id FROM users WHERE id <= 2 EXCEPT SELECT id FROM users WHERE id >= 2 ORDER BY id")
            .rows(&[&[Int(1)]])
            .build(),
        case("set operation order by and limit cover the merged result")
            .sql("SELECT id FROM users UNION ALL SELECT id FROM users ORDER BY id DESC LIMIT 2")
            .rows(&[&[Int(3)], &[Int(3)]])
            .build(),
    ]
}

/// Upstream forms that the gateway must reject explicitly instead of guessing.
pub const REJECTED_SQL: &[(&str, &str)] = &[
    ("DISTINCT ON", "SELECT DISTINCT ON (age) id FROM users ORDER BY age"),
    ("GROUPING SETS", "SELECT age FROM users GROUP BY GROUPING SETS ((age))"),
    ("set operation column count", "SELECT id FROM users UNION SELECT id, name FROM users"),
    ("set operation column type", "SELECT id FROM users UNION SELECT name FROM users"),
    ("FOR UPDATE", "SELECT id FROM users FOR UPDATE"),
];

/// Every case that both the in-process suite and the real-client suites run.
pub fn all_cases() -> Vec<Case> {
    let mut cases = crud_cases();
    cases.extend(type_cases());
    cases.extend(distinct_cases());
    cases.extend(group_cases());
    cases.extend(order_cases());
    cases.extend(null_order_cases());
    cases.extend(set_operation_cases());
    cases
}

/// Real-client driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Driver {
    /// PostgreSQL protocol adapter.
    Postgres,
    /// MySQL protocol adapter.
    Mysql,
}

/// Client mode inside a driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientMode {
    /// psycopg 3 default text-format cursor.
    PostgresText,
    /// psycopg 3 `binary=True` cursor.
    PostgresBinary,
    /// mysql-connector normal cursor (`COM_QUERY`, client-side `%s` binding).
    MysqlText,
    /// mysql-connector `prepared=True` cursor (`COM_STMT_*`, server-side `?` binding).
    MysqlPrepared,
}

impl ClientMode {
    /// Driver that serves this mode.
    pub fn driver(self) -> Driver {
        match self {
            Self::PostgresText | Self::PostgresBinary => Driver::Postgres,
            Self::MysqlText | Self::MysqlPrepared => Driver::Mysql,
        }
    }

    /// Stable label used in test output.
    pub fn label(self) -> &'static str {
        match self {
            Self::PostgresText => "psycopg3 text",
            Self::PostgresBinary => "psycopg3 binary",
            Self::MysqlText => "mysql-connector text",
            Self::MysqlPrepared => "mysql-connector prepared",
        }
    }

    /// Placeholder spelling the client sends to the gateway.
    pub fn placeholder(self) -> &'static str {
        match self {
            Self::MysqlPrepared => "?",
            _ => "%s",
        }
    }

    /// Whether the client decodes `BOOLEAN` / `TINY(1)` as an integer.
    pub fn boolean_is_integer(self) -> bool {
        self.driver() == Driver::Mysql
    }
}

/// Explicit protocol exclusion for a case, or `None` when the case applies.
///
/// Exclusions are driver facts, not assertions we could not satisfy: `$n`
/// placeholders are PostgreSQL syntax and the MySQL protocol has neither an
/// equivalent placeholder nor a boolean parameter type.
pub fn driver_exclusion(case: &Case, driver: Driver) -> Option<&'static str> {
    match (driver, case.style) {
        (Driver::Mysql, SqlParameterStyle::DollarNumber) => {
            Some("MySQL binds `?` placeholders only: `$n` is PostgreSQL syntax")
        }
        (Driver::Mysql, _) if case.parameters.iter().any(|value| matches!(value, Lit::Bool(_))) => Some(
            "MySQL has no boolean parameter type: the driver converts a Python `bool` to `1`/`0` before it reaches \
             the gateway, so strict typed comparison cannot tell it from an integer bind against a `BOOLEAN` column",
        ),
        _ => None,
    }
}

/// Renders the applicable cases as a Python literal table.
///
/// The generated source is embedded into a client script; no expectations are
/// restated there. Excluded cases are returned with their reason so the caller
/// can print them.
pub fn render_python(mode: ClientMode) -> RenderedCases {
    let mut literal = String::from("CASES = [\n");
    let mut excluded = Vec::new();
    let mut applied = 0;
    for case in all_cases() {
        if let Some(reason) = driver_exclusion(&case, mode.driver()) {
            excluded.push((case.name.to_string(), reason.to_string()));
            continue;
        }
        if case.pending.is_some() {
            excluded.push((case.name.to_string(), case.pending.unwrap_or_default().to_string()));
            continue;
        }
        applied += 1;
        let mut notes = Vec::new();
        let parameters =
            case.parameters.iter().map(|value| lit_python(*value, mode, &mut notes)).collect::<Vec<_>>().join(", ");
        let expected_rows = case
            .expected_rows_for(mode.driver())
            .iter()
            .map(|row| {
                format!("[{}]", row.iter().map(|value| lit_python(*value, mode, &mut notes)).collect::<Vec<_>>().join(", "))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let expected_state = case
            .expected_state
            .iter()
            .map(|row| {
                format!("[{}]", row.iter().map(|value| lit_python(*value, mode, &mut notes)).collect::<Vec<_>>().join(", "))
            })
            .collect::<Vec<_>>()
            .join(", ");
        let setup = case.setup.iter().map(|sql| python_string(sql)).collect::<Vec<_>>().join(", ");
        let statement = rewrite_placeholders(case.sql, case.style, mode.placeholder());
        let state_sql = format!("SELECT {} FROM users ORDER BY id", case.state_columns);
        let write = !case.sql.trim_start().to_ascii_uppercase().starts_with("SELECT");
        if mode == ClientMode::MysqlText && case.parameters.contains(&Lit::Null) {
            notes.push("mysql text cursor binds parameters client-side, so NULL reaches the gateway as a literal".to_string());
        }
        let notes = notes.iter().map(|note| python_string(note)).collect::<Vec<_>>().join(", ");
        literal.push_str(&format!(
            "    {{\"name\": {}, \"setup\": [{}], \"sql\": {}, \"params\": [{}], \"rows\": [{}], \
             \"affected\": {}, \"write\": {}, \"state_sql\": {}, \"state\": [{}], \"notes\": [{}]}},\n",
            python_string(case.name),
            setup,
            python_string(&statement),
            parameters,
            expected_rows,
            case.expected_affected_rows,
            py_bool(write),
            python_string(&state_sql),
            expected_state,
            notes
        ));
    }
    literal.push_str("]\n");
    RenderedCases { literal, applied, excluded }
}

/// Generated Python case table and its accounting.
pub struct RenderedCases {
    /// `CASES = [...]` Python source.
    pub literal: String,
    /// Number of cases the mode runs.
    pub applied: usize,
    /// Cases excluded from this mode with their protocol reason.
    pub excluded: Vec<(String, String)>,
}

fn lit_python(value: Lit, mode: ClientMode, notes: &mut Vec<String>) -> String {
    match value {
        Lit::Int(value) => format!("(\"int\", {value})"),
        Lit::Float(value) => format!("(\"float\", {})", format_float(value)),
        Lit::Text(value) => format!("(\"str\", {})", python_string(value)),
        Lit::Bool(value) if mode.boolean_is_integer() => {
            let note = "mysql decodes BOOLEAN / TINY(1) as int 0 or 1".to_string();
            if !notes.contains(&note) {
                notes.push(note);
            }
            format!("(\"int\", {})", i32::from(value))
        }
        Lit::Bool(value) => format!("(\"bool\", {})", py_bool(value)),
        Lit::Null => "(\"null\", None)".to_string(),
    }
}

fn format_float(value: f64) -> String {
    let rendered = format!("{value}");
    if rendered.contains(['.', 'e', 'E']) {
        rendered
    }
    else {
        format!("{rendered}.0")
    }
}

fn py_bool(value: bool) -> &'static str {
    if value {
        "True"
    }
    else {
        "False"
    }
}

/// Escapes text as a double-quoted Python literal.
pub fn python_string(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for character in text.chars() {
        match character {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            other => out.push(other),
        }
    }
    out.push('"');
    out
}

/// Rewrites the case's placeholders into the spelling the client sends.
///
/// Placeholders inside quoted strings, identifiers and comments are copied
/// verbatim, mirroring the gateway's own parameter walker.
fn rewrite_placeholders(sql: &str, style: SqlParameterStyle, target: &str) -> String {
    let chars: Vec<char> = sql.chars().collect();
    let mut out = String::with_capacity(sql.len() + 8);
    let mut index = 0;
    while index < chars.len() {
        match chars[index] {
            quote @ ('\'' | '"' | '`') => {
                out.push(quote);
                index += 1;
                while index < chars.len() {
                    let current = chars[index];
                    out.push(current);
                    index += 1;
                    if current == '\\' && quote == '\'' {
                        if index < chars.len() {
                            out.push(chars[index]);
                            index += 1;
                        }
                        continue;
                    }
                    if current == quote {
                        if chars.get(index) == Some(&quote) {
                            out.push(quote);
                            index += 1;
                            continue;
                        }
                        break;
                    }
                }
            }
            '-' if chars.get(index + 1) == Some(&'-') => {
                while index < chars.len() && chars[index] != '\n' {
                    out.push(chars[index]);
                    index += 1;
                }
            }
            '/' if chars.get(index + 1) == Some(&'*') => {
                out.push('/');
                out.push('*');
                index += 2;
                while index < chars.len() {
                    if chars[index] == '*' && chars.get(index + 1) == Some(&'/') {
                        out.push('*');
                        out.push('/');
                        index += 2;
                        break;
                    }
                    out.push(chars[index]);
                    index += 1;
                }
            }
            '?' if style == SqlParameterStyle::QuestionMark => {
                out.push_str(target);
                index += 1;
            }
            '$' if style == SqlParameterStyle::DollarNumber && chars.get(index + 1).is_some_and(char::is_ascii_digit) => {
                index += 1;
                while index < chars.len() && chars[index].is_ascii_digit() {
                    index += 1;
                }
                out.push_str(target);
            }
            other => {
                out.push(other);
                index += 1;
            }
        }
    }
    out
}
