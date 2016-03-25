//! Oak SQL frontend entry point for compatibility gateways.
//!
//! This module only parses the SQL surface. It does not execute SQL, own a
//! query planner, or bypass the YY execution model.

use oak_sql::ast::{BinaryOperator, Expression, Literal, SelectItem, SqlStatement};

/// Structured failure category from the Oak SQL frontend, gateway binder,
/// planner or executor.
///
/// Protocol adapters map categories to wire error codes and SQLSTATE values.
/// They never inspect [`SqlFrontendError::message`] to recover the category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlErrorKind {
    /// Oak rejected the SQL surface.
    Syntax,
    /// The catalog does not publish the referenced table.
    UnknownTable,
    /// The visible scope has no table matching a column qualifier.
    UnknownTableQualifier,
    /// The catalog publishes more than one table with the requested name.
    AmbiguousTable,
    /// No visible column matches the requested name.
    UnknownColumn,
    /// More than one visible column matches an unqualified name.
    AmbiguousColumn,
    /// A write value cannot be represented by the target column type.
    TypeMismatch,
    /// A write supplies `NULL` for a primary key.
    NotNullViolation,
    /// The statement is valid SQL but outside the gateway's supported subset.
    Unsupported,
    /// A function name or arity is not part of the gateway subset.
    UnknownFunction,
    /// A `@@` system variable is not published by the gateway.
    UnknownVariable,
    /// Catalog construction or publication is inconsistent.
    Catalog,
    /// Parameter substitution or a bind parameter position is invalid.
    Parameter,
    /// A write would create a duplicate primary key.
    UniqueViolation,
    /// Optimistic concurrency rejected a read-modify-write.
    Conflict,
    /// The storage layer failed while executing the statement.
    Execution,
    /// Division or modulo by zero — PostgreSQL raises an error, MySQL returns NULL.
    DivisionByZero,
    /// `SUBSTRING` was given a negative length. PostgreSQL rejects it with SQLSTATE
    /// `22011`, while MySQL returns an empty string, so this category is PostgreSQL-only.
    InvalidSubstringLength,
}

/// Structured failure from the Oak SQL frontend or gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlFrontendError {
    /// Failure category consumed by protocol adapters.
    pub kind: SqlErrorKind,
    /// Diagnostic text returned by the failing gateway stage.
    pub message: String,
}

impl SqlFrontendError {
    /// Creates a categorized frontend error.
    pub fn new(kind: SqlErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into() }
    }

    /// Oak rejected the SQL surface.
    pub fn syntax(message: impl Into<String>) -> Self {
        Self::new(SqlErrorKind::Syntax, message)
    }

    /// A literal cannot be represented by the target column type.
    pub fn type_mismatch(message: impl Into<String>) -> Self {
        Self::new(SqlErrorKind::TypeMismatch, message)
    }

    /// The statement is outside the gateway subset.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::new(SqlErrorKind::Unsupported, message)
    }

    /// A catalog identity is missing or inconsistent.
    pub fn catalog(message: impl Into<String>) -> Self {
        Self::new(SqlErrorKind::Catalog, message)
    }
}

/// A session statement lowered from Oak's SQL syntax tree.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlSessionCommand {
    /// `SET NAMES` character-set negotiation.
    SetNames { character_set: String, collation: Option<String> },
    /// Transaction boundary used by SQL protocol sessions.
    Transaction(SqlTransactionAction),
    /// `SHOW [GLOBAL|SESSION] VARIABLES [LIKE 'pattern']` — variable table probe.
    ShowVariables {
        /// Requested scope prefix, absent when the client did not name one.
        scope: Option<SqlVariableScope>,
        /// Optional `LIKE` pattern covering the variable name.
        pattern: Option<String>,
    },
    /// `SHOW <name>` — single-parameter probe used by PostgreSQL clients.
    ShowParameter {
        /// Requested scope prefix, absent when the client did not name one.
        scope: Option<SqlVariableScope>,
        /// Parameter name as written by the client.
        name: String,
    },
    /// `SET [GLOBAL|SESSION|LOCAL] <name> = <value>` — session assignment.
    SetVariable {
        /// Requested scope prefix, absent when the client did not name one.
        scope: Option<SqlVariableScope>,
        /// Variable name as written by the client.
        name: String,
        /// Assigned value, unbound against any catalog table.
        value: SqlBoundExpression,
    },
    /// SQLite `PRAGMA <name> [= <value>]`.
    Pragma {
        /// Pragma name as written by the client.
        name: String,
        /// Optional assigned value.
        value: Option<SqlBoundExpression>,
    },
}

/// Scope prefix carried by `SET` and `SHOW` session statements.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlVariableScope {
    /// `GLOBAL` — server-wide scope.
    Global,
    /// `SESSION` — connection scope.
    Session,
    /// `LOCAL` — dialect-specific local scope.
    Local,
}

/// Transaction operation recognized by Oak and projected for protocol adapters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlTransactionAction {
    /// Start a transaction.
    Begin,
    /// Commit a transaction.
    Commit,
    /// Roll back a transaction.
    Rollback,
}

/// Parses one SQL surface statement through the official Oak SQL frontend.
pub fn parse_sql(source: &str) -> Result<oak_sql::ast::SqlRoot, SqlFrontendError> {
    let root = oak_sql::parse(source).map_err(SqlFrontendError::syntax)?;
    for statement in &root.statements {
        match statement {
            SqlStatement::Error { message, .. } => return Err(SqlFrontendError::syntax(message.to_string())),
            SqlStatement::Unknown { .. } => {}
            _ => {}
        }
    }
    if root.statements.is_empty() {
        return Err(bind_error("Oak produced no SQL statements"));
    }
    Ok(root)
}

/// Projects the small set of protocol session commands from Oak's SQL AST.
pub fn parse_session_command(source: &str) -> Result<Option<SqlSessionCommand>, SqlFrontendError> {
    let normalized = normalize_start_transaction(source).or_else(|| normalize_quoted_set_names(source));
    let root = parse_sql(normalized.as_deref().unwrap_or(source))?;
    if root.statements.len() != 1 {
        return Ok(None);
    }
    Ok(match &root.statements[0] {
        SqlStatement::SetNames(statement) => Some(SqlSessionCommand::SetNames {
            character_set: statement.character_set.to_string(),
            collation: statement.collation.as_deref().map(str::to_string),
        }),
        SqlStatement::Transaction(statement) => Some(SqlSessionCommand::Transaction(match statement.action {
            oak_sql::ast::TransactionAction::Begin => SqlTransactionAction::Begin,
            oak_sql::ast::TransactionAction::Commit => SqlTransactionAction::Commit,
            oak_sql::ast::TransactionAction::Rollback => SqlTransactionAction::Rollback,
        })),
        SqlStatement::SetVariable(statement) => Some(SqlSessionCommand::SetVariable {
            scope: statement.scope.map(project_variable_scope),
            name: statement.name.to_string(),
            value: bind_expression(&statement.value)?,
        }),
        SqlStatement::Show(statement) => {
            let scope = statement.scope.map(project_variable_scope);
            if statement.target.eq_ignore_ascii_case("VARIABLES") {
                Some(SqlSessionCommand::ShowVariables { scope, pattern: statement.like.as_deref().map(str::to_string) })
            }
            else {
                Some(SqlSessionCommand::ShowParameter { scope, name: statement.target.to_string() })
            }
        }
        SqlStatement::Pragma(statement) => Some(SqlSessionCommand::Pragma {
            name: statement.name.to_string(),
            value: statement.value.as_ref().map(bind_expression).transpose()?,
        }),
        _ => None,
    })
}

/// MySQL and PostgreSQL clients may spell the opening boundary as `START
/// TRANSACTION`; Oak's session grammar only reads `BEGIN`. Rewrite that one
/// statement and leave every other SQL surface untouched.
fn normalize_start_transaction(source: &str) -> Option<String> {
    let trimmed = source.trim();
    let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    let mut words = trimmed.split_whitespace();
    if !words.next()?.eq_ignore_ascii_case("START") {
        return None;
    }
    if !words.next()?.eq_ignore_ascii_case("TRANSACTION") {
        return None;
    }
    if words.next().is_some() {
        return None;
    }
    Some("BEGIN".to_string())
}

/// MySQL clients spell charset negotiation as `SET NAMES 'utf8mb4' COLLATE
/// 'utf8mb4_general_ci'`, while Oak's session grammar only reads bare
/// identifiers. Unquote simple identifier literals for this one statement and
/// leave every other SQL surface untouched.
fn normalize_quoted_set_names(source: &str) -> Option<String> {
    let trimmed = source.trim();
    let trimmed = trimmed.strip_suffix(';').unwrap_or(trimmed).trim_end();
    trimmed.get(..9).filter(|prefix| prefix.eq_ignore_ascii_case("SET NAMES"))?;
    let tokens = trimmed[9..].split_whitespace().collect::<Vec<_>>();
    let mut words = Vec::with_capacity(tokens.len());
    for token in tokens {
        words.push(unquote_simple_identifier(token)?);
    }
    if !(words.len() == 1 || (words.len() == 3 && words[1].eq_ignore_ascii_case("COLLATE"))) {
        return None;
    }
    Some(format!("SET NAMES {}", words.join(" ")))
}

fn unquote_simple_identifier(token: &str) -> Option<&str> {
    let inner = token.strip_prefix('\'').and_then(|rest| rest.strip_suffix('\'')).unwrap_or(token);
    if inner.is_empty() || !inner.chars().all(|character| character.is_ascii_alphanumeric() || character == '_') {
        return None;
    }
    Some(inner)
}

fn project_variable_scope(scope: oak_sql::ast::SessionScope) -> SqlVariableScope {
    match scope {
        oak_sql::ast::SessionScope::Global => SqlVariableScope::Global,
        oak_sql::ast::SessionScope::Session => SqlVariableScope::Session,
        oak_sql::ast::SessionScope::Local => SqlVariableScope::Local,
    }
}

/// Gateway-owned statement after Oak parsing and SQL-surface binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundStatement {
    /// One SQL SELECT request awaiting YYDS catalog binding.
    Select(SqlBoundSelect),
    /// One SQL INSERT request awaiting YYDS catalog binding.
    Insert(SqlBoundInsert),
    /// One SQL UPDATE request awaiting YYDS catalog binding.
    Update(SqlBoundUpdate),
    /// One SQL DELETE request awaiting YYDS catalog binding.
    Delete(SqlBoundDelete),
    /// One SQL set operation awaiting YYDS catalog binding.
    SetOperation(SqlBoundSetOperation),
}

/// SQL dialect that owns protocol-visible ordering rules.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum SqlDialect {
    /// PostgreSQL: `NULL` sorts after every non-NULL value.
    #[default]
    Postgres,
    /// MySQL: `NULL` sorts before every non-NULL value.
    Mysql,
}

impl SqlDialect {
    /// Whether `NULL` values sort before every non-NULL value.
    pub fn nulls_sort_first(self) -> bool {
        matches!(self, Self::Mysql)
    }
}

/// One non-recursive `WITH` definition retained by the gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundCte {
    /// Name the CTE is referenced by in `FROM` / `JOIN`.
    pub name: String,
    /// Bound body of the CTE.
    pub query: Box<SqlBoundSelect>,
}

/// SQL SELECT request owned by the compatibility gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundSelect {
    /// Common table expressions declared by this query, in source order.
    pub with: Vec<SqlBoundCte>,
    /// Optional source row as written by the client.
    pub source: Option<SqlBoundSource>,
    /// JOIN clauses in source order.
    pub joins: Vec<SqlBoundJoin>,
    /// Projection expressions in source order.
    pub projections: Vec<SqlBoundProjection>,
    /// Optional SQL WHERE expression.
    pub predicate: Option<SqlBoundExpression>,
    /// Optional SQL GROUP BY expressions.
    pub group_by: Option<Vec<SqlBoundExpression>>,
    /// Optional SQL HAVING expression (applied after GROUP BY aggregation).
    pub having: Option<SqlBoundExpression>,
    /// Optional SQL LIMIT count (parsed from the LIMIT clause's literal).
    pub limit: Option<usize>,
    /// Optional SQL OFFSET count (parsed from the LIMIT ... OFFSET clause).
    pub offset: Option<usize>,
    /// Optional SQL ORDER BY clause — supports multi-column ordering.
    pub order_by: Option<Vec<SqlBoundOrderByItem>>,
    /// Whether the projection list carries the `DISTINCT` modifier.
    pub distinct: bool,
}

/// One `FROM` / `JOIN` row source retained by the gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundSource {
    /// A catalog table reference, optionally renamed by an alias.
    Table {
        /// Catalog table name as written by the client.
        name: String,
        /// Optional alias that replaces the table name in the column scope.
        alias: Option<String>,
    },
    /// A derived table `(SELECT ...) alias` — the inner query keeps its own binding.
    Derived {
        /// Bound inner query. Table sources are the only shape the planner accepts.
        query: Box<SqlBoundSelect>,
        /// Alias that names the derived table in the column scope.
        alias: String,
    },
}

/// One JOIN clause retained by the gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundJoin {
    /// Join kind as written by the client.
    pub kind: SqlBoundJoinKind,
    /// Joined row source as written by the client.
    pub source: SqlBoundSource,
    /// Join condition from the `ON` clause.
    pub on: SqlBoundExpression,
}

/// SQL join kind retained by the gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlBoundJoinKind {
    /// `[INNER] JOIN` — only rows matching on both sides survive.
    Inner,
    /// `LEFT [OUTER] JOIN`.
    Left,
    /// `RIGHT [OUTER] JOIN`.
    Right,
    /// `FULL [OUTER] JOIN`.
    Full,
}

/// One SQL ORDER BY item — expression, direction and explicit NULL placement.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundOrderByItem {
    pub expr: SqlBoundExpression,
    pub direction: SqlBoundOrderDirection,
    pub nulls: Option<SqlNullsOrder>,
}

/// Sort direction for ORDER BY.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundOrderDirection {
    Asc,
    Desc,
}

/// Explicit `NULLS FIRST` / `NULLS LAST` written in ORDER BY.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlNullsOrder {
    First,
    Last,
}

/// SQL INSERT request owned by the compatibility gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundInsert {
    /// Target table name.
    pub table: String,
    /// Optional explicit column list. When absent the catalog will infer order.
    pub columns: Option<Vec<String>>,
    /// Bound values grouped by row. Empty when the source is a SELECT.
    pub rows: Vec<Vec<SqlBoundExpression>>,
    /// `INSERT ... SELECT` source query. `None` for a `VALUES` insert.
    pub select: Option<Box<SqlBoundSelect>>,
    /// `ON CONFLICT` clause, when one was written.
    pub conflict: Option<SqlBoundConflict>,
    /// `RETURNING` projection list, when one was written.
    pub returning: Option<Vec<SqlBoundProjection>>,
}

/// `ON CONFLICT` clause retained by the gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundConflict {
    /// Conflict target columns, normalized. Empty when the clause omitted its target.
    pub columns: Vec<String>,
    /// Action taken when the target row already exists.
    pub action: SqlBoundConflictAction,
}

/// Conflict action of an `ON CONFLICT` clause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundConflictAction {
    /// `DO NOTHING` — skip the conflicting row.
    DoNothing,
    /// `DO UPDATE SET ... [WHERE ...]` — rewrite the conflicting row in place.
    DoUpdate {
        /// Column → expression assignments applied to the existing row.
        assignments: Vec<SqlBoundAssignment>,
        /// Optional predicate restricting which conflicting rows are rewritten.
        predicate: Option<SqlBoundExpression>,
    },
}

/// SQL UPDATE request owned by the compatibility gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundUpdate {
    /// Target table name.
    pub table: String,
    /// Column → expression assignments in source order.
    pub assignments: Vec<SqlBoundAssignment>,
    /// Optional SQL WHERE expression.
    pub predicate: Option<SqlBoundExpression>,
}

/// One SET column = expression binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundAssignment {
    /// Column name as written by the client.
    pub column: String,
    /// Bound value expression.
    pub value: SqlBoundExpression,
}

/// SQL DELETE request owned by the compatibility gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundDelete {
    /// Target table name.
    pub table: String,
    /// Optional SQL WHERE expression.
    pub predicate: Option<SqlBoundExpression>,
}

/// SQL set operator retained by the gateway binder.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlBoundSetOperator {
    /// `UNION` — concatenation with duplicate elimination.
    Union,
    /// `UNION ALL` — concatenation keeping duplicates.
    UnionAll,
    /// `INTERSECT` — distinct rows present on both sides.
    Intersect,
    /// `EXCEPT` — distinct left rows absent from the right side.
    Except,
}

/// SQL set operation request owned by the compatibility gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundSetOperation {
    /// Left operand.
    pub left: Box<SqlBoundSelect>,
    /// Operator combining both operands.
    pub operator: SqlBoundSetOperator,
    /// Right operand.
    pub right: Box<SqlBoundSelect>,
    /// `ORDER BY` applied to the combined result.
    pub order_by: Option<Vec<SqlBoundOrderByItem>>,
    /// Optional `LIMIT` applied to the combined result.
    pub limit: Option<usize>,
    /// Optional `OFFSET` applied to the combined result.
    pub offset: Option<usize>,
}

/// One SQL projection expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundProjection {
    /// `*` projection, resolved later against the YYDS catalog.
    Star,
    /// Expression projection with an optional client alias.
    Expression {
        /// Bound expression.
        expression: SqlBoundExpression,
        /// Optional SQL alias.
        alias: Option<String>,
    },
}

/// SQL expression retained by the gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundExpression {
    /// Identifier that still needs catalog binding.
    Identifier(String),
    /// SQL literal preserving surface spelling where relevant.
    Literal(SqlBoundLiteral),
    /// A bare `*` argument, as written by `COUNT(*)`.
    Wildcard,
    /// SQL binary operator. Its null semantics remain SQL semantics.
    Binary {
        /// Left operand.
        left: Box<Self>,
        /// Operator.
        operator: SqlBoundBinaryOperator,
        /// Right operand.
        right: Box<Self>,
    },
    /// SQL unary operator.
    Unary {
        /// Operator spelling.
        operator: SqlBoundUnaryOperator,
        /// Operand.
        expression: Box<Self>,
    },
    /// A SQL function call — currently only aggregate functions (COUNT/SUM/AVG/MAX/MIN)
    /// are recognized as first-class. Unknown function names are rejected at bind time.
    Function {
        /// Canonical lower-case function name.
        name: String,
        /// Bound argument expressions.
        args: Vec<SqlBoundExpression>,
        /// Whether the aggregate uses `DISTINCT` quantifier. Only meaningful on COUNT/SUM/AVG.
        distinct: bool,
    },
    /// `expr IN (<subquery>)` / `expr NOT IN (<subquery>)`.
    InSubquery {
        /// Left-hand side expression.
        expression: Box<Self>,
        /// Bound subquery.
        select: Box<SqlBoundSelect>,
        /// Whether the predicate is spelled `NOT IN`.
        negated: bool,
    },
    /// `(SELECT ...)` used where a single value is expected.
    ScalarSubquery {
        /// Bound subquery whose one output column supplies the value.
        select: Box<SqlBoundSelect>,
    },
    /// `[NOT] EXISTS (<subquery>)`.
    Exists {
        /// Bound subquery whose rows are tested for existence.
        select: Box<SqlBoundSelect>,
        /// Whether the predicate is spelled `NOT EXISTS`.
        negated: bool,
    },
    /// `CASE WHEN cond THEN result ... [ELSE fallback] END`.
    Case {
        /// `(when, then)` branches in source order.
        branches: Vec<(Box<Self>, Box<Self>)>,
        /// Optional ELSE fallback.
        else_branch: Option<Box<Self>>,
    },
    /// `CAST(expr AS type_name)`.
    Cast {
        /// The expression being cast.
        expr: Box<Self>,
        /// The target type spelling as written (e.g. `INTEGER`, `TEXT`, `REAL`, `BLOB`).
        target: String,
    },
}

/// SQL aggregate function identity recognized by the gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlAggregateFunction {
    COUNT,
    SUM,
    AVG,
    MAX,
    MIN,
}

impl SqlAggregateFunction {
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "COUNT" => Some(Self::COUNT),
            "SUM" => Some(Self::SUM),
            "AVG" => Some(Self::AVG),
            "MAX" => Some(Self::MAX),
            "MIN" => Some(Self::MIN),
            _ => None,
        }
    }
    pub fn name(&self) -> &'static str {
        match self {
            Self::COUNT => "COUNT",
            Self::SUM => "SUM",
            Self::AVG => "AVG",
            Self::MAX => "MAX",
            Self::MIN => "MIN",
        }
    }
}

/// SQL scalar (non-aggregate) function identity recognized by the gateway.
///
/// These are the functions drivers and ORMs probe on connect. They take no
/// table source and evaluate to a single value per row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlScalarFunction {
    /// `VERSION()` — engine banner.
    Version,
    /// `DATABASE()` — current schema name.
    Database,
    /// `CURRENT_SCHEMA()` / `SCHEMA()` — current schema name.
    CurrentSchema,
    /// `NOW()` / `CURRENT_TIMESTAMP()` / bare `CURRENT_TIMESTAMP` — UTC session
    /// timestamp rendered as TEXT `YYYY-MM-DD HH:MM:SS`.
    Now,
    /// `CURRENT_DATE()` / bare `CURRENT_DATE` — UTC date rendered as TEXT `YYYY-MM-DD`.
    CurrentDate,
    /// `CURRENT_TIME()` / bare `CURRENT_TIME` — UTC time rendered as TEXT `HH:MM:SS`.
    CurrentTime,
    /// `CURDATE()` — MySQL spelling of [`Self::CurrentDate`], rejected by PostgreSQL.
    Curdate,
    /// `CURTIME()` — MySQL spelling of [`Self::CurrentTime`], rejected by PostgreSQL.
    Curtime,
    /// `LOWER(x)` / `LCASE(x)`.
    Lower,
    /// `UPPER(x)` / `UCASE(x)`.
    Upper,
    /// `LENGTH(x)` — PostgreSQL counts characters, MySQL counts UTF-8 bytes.
    Length,
    /// `CHAR_LENGTH(x)` / `CHARACTER_LENGTH(x)` / `LEN(x)` — character count in both dialects.
    CharLength,
    /// `SUBSTRING(x, start[, length])` / `SUBSTR(x, start[, length])` / `MID(x, start[, length])`.
    Substring,
    /// `TRIM(x[, chars])` / `BTRIM(x[, chars])`.
    Trim,
    /// `LTRIM(x[, chars])`.
    Ltrim,
    /// `RTRIM(x[, chars])`.
    Rtrim,
    /// `REPLACE(x, from, to)`.
    Replace,
    /// `CONCAT(a, b, ...)` — MySQL propagates NULL, PostgreSQL skips NULL arguments.
    Concat,
    /// `COALESCE(a, b, ...)` / `IFNULL(a, b)`.
    Coalesce,
    /// `ABS(x)`.
    Abs,
    /// `ROUND(x[, digits])`.
    Round,
}

impl SqlScalarFunction {
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "VERSION" => Some(Self::Version),
            "DATABASE" | "CURRENT_DATABASE" => Some(Self::Database),
            "CURRENT_SCHEMA" | "SCHEMA" => Some(Self::CurrentSchema),
            "NOW" | "CURRENT_TIMESTAMP" => Some(Self::Now),
            "CURRENT_DATE" => Some(Self::CurrentDate),
            "CURRENT_TIME" => Some(Self::CurrentTime),
            "CURDATE" => Some(Self::Curdate),
            "CURTIME" => Some(Self::Curtime),
            "LOWER" | "LCASE" => Some(Self::Lower),
            "UPPER" | "UCASE" => Some(Self::Upper),
            "LENGTH" => Some(Self::Length),
            "CHAR_LENGTH" | "CHARACTER_LENGTH" | "LEN" => Some(Self::CharLength),
            "SUBSTRING" | "SUBSTR" | "MID" => Some(Self::Substring),
            "TRIM" | "BTRIM" => Some(Self::Trim),
            "LTRIM" => Some(Self::Ltrim),
            "RTRIM" => Some(Self::Rtrim),
            "REPLACE" => Some(Self::Replace),
            "CONCAT" => Some(Self::Concat),
            "COALESCE" | "IFNULL" => Some(Self::Coalesce),
            "ABS" => Some(Self::Abs),
            "ROUND" => Some(Self::Round),
            _ => None,
        }
    }

    /// Niladic spellings the SQL standard writes without parentheses.
    ///
    /// Oak's SQL tree has no node for these keywords, so the catalog binder falls
    /// back to them only after an unqualified identifier fails to resolve to a
    /// column. `NOW` is deliberately absent: both dialects require `NOW()`.
    pub fn from_keyword(name: &str) -> Option<Self> {
        match name.to_ascii_uppercase().as_str() {
            "CURRENT_DATE" => Some(Self::CurrentDate),
            "CURRENT_TIME" => Some(Self::CurrentTime),
            "CURRENT_TIMESTAMP" => Some(Self::Now),
            _ => None,
        }
    }

    /// Canonical spelling used by diagnostics for this function identity.
    pub fn name(&self) -> &'static str {
        match self {
            Self::Version => "VERSION",
            Self::Database => "DATABASE",
            Self::CurrentSchema => "CURRENT_SCHEMA",
            Self::Now => "NOW",
            Self::CurrentDate => "CURRENT_DATE",
            Self::CurrentTime => "CURRENT_TIME",
            Self::Curdate => "CURDATE",
            Self::Curtime => "CURTIME",
            Self::Lower => "LOWER",
            Self::Upper => "UPPER",
            Self::Length => "LENGTH",
            Self::CharLength => "CHAR_LENGTH",
            Self::Substring => "SUBSTRING",
            Self::Trim => "TRIM",
            Self::Ltrim => "LTRIM",
            Self::Rtrim => "RTRIM",
            Self::Replace => "REPLACE",
            Self::Concat => "CONCAT",
            Self::Coalesce => "COALESCE",
            Self::Abs => "ABS",
            Self::Round => "ROUND",
        }
    }

    /// Expected argument count as `(minimum, maximum)`, where `maximum == None`
    /// means variadic.
    pub fn arity(&self) -> (usize, Option<usize>) {
        match self {
            Self::Version
            | Self::Database
            | Self::CurrentSchema
            | Self::Now
            | Self::CurrentDate
            | Self::CurrentTime
            | Self::Curdate
            | Self::Curtime => (0, Some(0)),
            Self::Lower | Self::Upper | Self::Length | Self::CharLength | Self::Abs => (1, Some(1)),
            Self::Round => (1, Some(2)),
            Self::Substring => (2, Some(3)),
            Self::Trim | Self::Ltrim | Self::Rtrim => (1, Some(2)),
            Self::Replace => (3, Some(3)),
            Self::Concat | Self::Coalesce => (1, None),
        }
    }
}

/// SQL literal retained by the gateway surface.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundLiteral {
    /// Integer literal kept exact as `i64`; never round-trips through `f64`.
    Integer(i64),
    /// Non-integer numeric SQL token (decimal point or exponent), kept in source spelling.
    Number(String),
    /// SQL string literal contents.
    String(String),
    /// SQL boolean literal.
    Boolean(bool),
    /// SQL NULL literal.
    Null,
}

/// SQL binary operator retained without changing SQL three-valued semantics.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlBoundBinaryOperator {
    /// `+`.
    Plus,
    /// `-`.
    Minus,
    /// `*`.
    Multiply,
    /// `/`.
    Divide,
    /// `%`.
    Modulo,
    /// `AND`.
    And,
    /// `OR`.
    Or,
    /// `=`.
    Equal,
    /// `<>` or `!=`.
    NotEqual,
    /// `<`.
    Less,
    /// `>`.
    Greater,
    /// `<=`.
    LessEqual,
    /// `>=`.
    GreaterEqual,
    /// `LIKE`.
    Like,
}

/// SQL unary operator retained by the gateway.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqlBoundUnaryOperator {
    /// Unary `+`.
    Plus,
    /// Unary `-`.
    Minus,
    /// `NOT`.
    Not,
    /// `IS NULL` — null-existence test, never compares against a value.
    IsNull,
    /// `IS NOT NULL`.
    IsNotNull,
    /// `IS [NOT] TRUE` / `IS [NOT] FALSE` — three-valued truth test, not a comparison.
    IsTruth {
        /// The boolean value the operand is tested against.
        value: bool,
        /// Whether the predicate is spelled `IS NOT`.
        negated: bool,
    },
}

/// Binds the supported SQL compatibility subset without executing it.
pub fn bind_sql(source: &str) -> Result<SqlBoundStatement, SqlFrontendError> {
    let root = parse_sql(source)?;
    if root.statements.len() != 1 {
        return Err(bind_error("SQL gateway accepts exactly one statement"));
    }
    match &root.statements[0] {
        SqlStatement::Select(select) => Ok(SqlBoundStatement::Select(bind_select_statement(select)?)),
        SqlStatement::Insert(insert) => {
            // `ON CONFLICT` 保留冲突目标与动作；方言与唯一约束校验在 planner 层完成。
            let conflict = match &insert.on_conflict {
                None => None,
                Some(conflict) => Some(SqlBoundConflict {
                    columns: conflict.columns.iter().map(|id| normalize_identifier(id.name.as_ref())).collect(),
                    action: match &conflict.action {
                        oak_sql::ast::OnConflictAction::DoNothing => SqlBoundConflictAction::DoNothing,
                        oak_sql::ast::OnConflictAction::DoUpdate { assignments, selection } => {
                            SqlBoundConflictAction::DoUpdate {
                                assignments: assignments
                                    .iter()
                                    .map(|assignment| {
                                        Ok(SqlBoundAssignment {
                                            column: normalize_identifier(assignment.column.name.as_ref()),
                                            value: bind_expression(&assignment.value)?,
                                        })
                                    })
                                    .collect::<Result<Vec<_>, SqlFrontendError>>()?,
                                predicate: selection.as_ref().map(bind_expression).transpose()?,
                            }
                        }
                    },
                }),
            };
            let returning = match &insert.returning {
                None => None,
                Some(clause) => Some(clause.items.iter().map(bind_projection).collect::<Result<Vec<_>, _>>()?),
            };
            let columns: Option<Vec<String>> = if insert.columns.is_empty() {
                None
            }
            else {
                Some(insert.columns.iter().map(|id| normalize_identifier(id.name.as_ref())).collect())
            };
            let mut rows = Vec::with_capacity(insert.rows.len());
            for row in &insert.rows {
                rows.push(row.iter().map(bind_expression).collect::<Result<Vec<_>, _>>()?);
            }
            // `INSERT ... SELECT` 复用唯一一份 SELECT 绑定，不另造一套值绑定。
            let select = match &insert.source {
                oak_sql::ast::InsertSource::Values => None,
                oak_sql::ast::InsertSource::Select(select) => Some(Box::new(bind_select_statement(select)?)),
            };
            if select.is_none() {
                let Some(width) = rows.first().map(Vec::len)
                else {
                    return Err(bind_error("INSERT has no VALUES rows"));
                };
                if rows.iter().any(|row| row.len() != width) {
                    return Err(bind_error("INSERT VALUES rows must all carry the same number of values"));
                }
                if let Some(cols) = &columns {
                    if cols.len() != width {
                        return Err(bind_error(&format!(
                            "INSERT column/value count mismatch: {} columns, {} values per row",
                            cols.len(),
                            width
                        )));
                    }
                }
            }
            Ok(SqlBoundStatement::Insert(SqlBoundInsert {
                table: normalize_identifier(insert.table_name.name.name.as_ref()),
                columns,
                rows,
                select,
                conflict,
                returning,
            }))
        }
        SqlStatement::Update(update) => {
            let assignments = update
                .assignments
                .iter()
                .map(|a| {
                    Ok(SqlBoundAssignment {
                        column: normalize_identifier(a.column.name.as_ref()),
                        value: bind_expression(&a.value)?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(SqlBoundStatement::Update(SqlBoundUpdate {
                table: normalize_identifier(update.table_name.name.name.as_ref()),
                assignments,
                predicate: update.selection.as_ref().map(bind_expression).transpose()?,
            }))
        }
        SqlStatement::Delete(delete) => Ok(SqlBoundStatement::Delete(SqlBoundDelete {
            table: normalize_identifier(delete.table_name.name.name.as_ref()),
            predicate: delete.selection.as_ref().map(bind_expression).transpose()?,
        })),
        SqlStatement::SetOperation(set) => Ok(SqlBoundStatement::SetOperation(bind_set_operation(set)?)),
        _ => Err(SqlFrontendError::unsupported("SQL gateway binding accepts SELECT/INSERT/UPDATE/DELETE only")),
    }
}

/// Binds the non-recursive `WITH` prefix into CTE definitions.
///
/// Each body goes through the same [`bind_select_statement`] path as every other
/// query, so a CTE can only carry shapes the gateway already binds. Oak rejects
/// `WITH RECURSIVE` while parsing, and an explicit column list renames the CTE
/// output — the flattened gateway source cannot express a rename.
fn bind_with_clause(clause: Option<&oak_sql::ast::WithClause>) -> Result<Vec<SqlBoundCte>, SqlFrontendError> {
    let Some(clause) = clause
    else {
        return Ok(Vec::new());
    };
    if clause.ctes.is_empty() {
        return Err(bind_error("SQL common table expression list must not be empty"));
    }
    let mut ctes: Vec<SqlBoundCte> = Vec::with_capacity(clause.ctes.len());
    for cte in &clause.ctes {
        let name = normalize_identifier(cte.name.name.as_ref());
        if ctes.iter().any(|existing| existing.name == name) {
            return Err(bind_error(&format!("SQL common table expression `{name}` is defined more than once")));
        }
        if !cte.columns.is_empty() {
            return Err(SqlFrontendError::unsupported(format!(
                "SQL common table expression `{name}` with an explicit column list is outside the gateway subset"
            )));
        }
        ctes.push(SqlBoundCte { name, query: Box::new(bind_select_statement(&cte.query)?) });
    }
    Ok(ctes)
}

/// Binds one Oak `SELECT` — shared by top-level statements and `IN (SELECT ...)` subqueries.
fn bind_select_statement(select: &oak_sql::ast::SelectStatement) -> Result<SqlBoundSelect, SqlFrontendError> {
    let with = bind_with_clause(select.with.as_ref())?;
    let distinct = match &select.quantifier {
        None | Some(oak_sql::ast::SelectQuantifier::All { .. }) => false,
        Some(oak_sql::ast::SelectQuantifier::Distinct { .. }) => true,
        Some(oak_sql::ast::SelectQuantifier::DistinctOn { .. }) => {
            return Err(SqlFrontendError::unsupported("SELECT DISTINCT ON is outside the gateway subset"));
        }
    };
    let projections = select.items.iter().map(bind_projection).collect::<Result<Vec<_>, _>>()?;
    let (limit, offset) = bind_limit_clause(select.limit.as_ref())?;
    let order_by = bind_order_by_clause(select.order_by.as_ref())?;
    let group_by = match &select.group_by {
        Some(clause) if !clause.items.is_empty() => {
            let mut expressions = Vec::with_capacity(clause.items.len());
            for item in &clause.items {
                expressions.push(match item {
                    oak_sql::ast::GroupByItem::Column(identifier) => {
                        SqlBoundExpression::Identifier(normalize_identifier(identifier.name.as_ref()))
                    }
                    oak_sql::ast::GroupByItem::Expression(expression) => bind_expression(expression)?,
                    oak_sql::ast::GroupByItem::Position { index, .. } => match projections.get(index.saturating_sub(1)) {
                        Some(SqlBoundProjection::Expression { expression, .. }) => expression.clone(),
                        Some(SqlBoundProjection::Star) => {
                            return Err(SqlFrontendError::unsupported(
                                "GROUP BY ordinal position cannot reference a star projection",
                            ));
                        }
                        None => {
                            return Err(SqlFrontendError::unsupported(
                                "GROUP BY ordinal position is outside the projection list",
                            ));
                        }
                    },
                });
            }
            Some(expressions)
        }
        _ => None,
    };
    let having = select.having.as_ref().map(|h| bind_expression(&h.condition)).transpose()?;
    let joins = select
        .joins
        .iter()
        .map(|join| {
            Ok(SqlBoundJoin {
                kind: match join.join_type {
                    oak_sql::ast::JoinType::Inner => SqlBoundJoinKind::Inner,
                    oak_sql::ast::JoinType::Left => SqlBoundJoinKind::Left,
                    oak_sql::ast::JoinType::Right => SqlBoundJoinKind::Right,
                    oak_sql::ast::JoinType::Full => SqlBoundJoinKind::Full,
                },
                source: bind_table_source(&join.table)?,
                on: join
                    .on
                    .as_ref()
                    .ok_or_else(|| bind_error("SQL JOIN requires an ON condition"))
                    .and_then(bind_expression)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SqlBoundSelect {
        with,
        source: select.from.as_ref().map(bind_table_source).transpose()?,
        joins,
        projections,
        predicate: select.expr.as_ref().map(bind_expression).transpose()?,
        group_by,
        having,
        limit,
        offset,
        order_by,
        distinct,
    })
}

/// Binds one Oak set operation. `ORDER BY` stays in surface form because it
/// references the combined output columns rather than catalog columns.
fn bind_set_operation(set: &oak_sql::ast::SetOperationStatement) -> Result<SqlBoundSetOperation, SqlFrontendError> {
    let (limit, offset) = bind_limit_clause(set.limit.as_ref())?;
    Ok(SqlBoundSetOperation {
        left: Box::new(bind_select_statement(&set.left)?),
        operator: match set.operator {
            oak_sql::ast::SetOperator::Union => SqlBoundSetOperator::Union,
            oak_sql::ast::SetOperator::UnionAll => SqlBoundSetOperator::UnionAll,
            oak_sql::ast::SetOperator::Intersect => SqlBoundSetOperator::Intersect,
            oak_sql::ast::SetOperator::Except => SqlBoundSetOperator::Except,
        },
        right: Box::new(bind_select_statement(&set.right)?),
        order_by: bind_order_by_clause(set.order_by.as_ref())?,
        limit,
        offset,
    })
}

/// Binds an Oak `LIMIT` clause into a count and an optional offset.
fn bind_limit_clause(clause: Option<&oak_sql::ast::LimitClause>) -> Result<(Option<usize>, Option<usize>), SqlFrontendError> {
    Ok(match clause {
        Some(clause) => {
            let count = extract_u64(&clause.limit).ok_or_else(|| bind_error("SQL LIMIT must be a non-negative integer literal"))?
                as usize;
            let offset = clause.offset.as_ref().and_then(extract_u64).map(|value| value as usize);
            (Some(count), offset)
        }
        None => (None, None),
    })
}

/// Binds an Oak `ORDER BY` clause into gateway order items.
fn bind_order_by_clause(
    clause: Option<&oak_sql::ast::OrderByClause>,
) -> Result<Option<Vec<SqlBoundOrderByItem>>, SqlFrontendError> {
    Ok(match clause {
        Some(clause) if !clause.items.is_empty() => {
            let items = clause
                .items
                .iter()
                .map(|item| {
                    Ok(SqlBoundOrderByItem {
                        expr: bind_expression(&item.expr)?,
                        direction: match item.direction {
                            oak_sql::ast::OrderDirection::Asc => SqlBoundOrderDirection::Asc,
                            oak_sql::ast::OrderDirection::Desc => SqlBoundOrderDirection::Desc,
                        },
                        nulls: item.nulls.map(|nulls| match nulls {
                            oak_sql::ast::NullsOrder::First => SqlNullsOrder::First,
                            oak_sql::ast::NullsOrder::Last => SqlNullsOrder::Last,
                        }),
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Some(items)
        }
        _ => None,
    })
}

/// Binds an Oak `FROM` / `JOIN` source into a catalog table reference or a
/// derived table. A derived table is a full nested `SELECT`, so it is bound
/// through the same [`bind_select_statement`] path as every other query.
fn bind_table_source(source: &oak_sql::ast::TableSource) -> Result<SqlBoundSource, SqlFrontendError> {
    match source {
        oak_sql::ast::TableSource::Table(table) => Ok(SqlBoundSource::Table {
            name: normalize_identifier(table.name.name.as_ref()),
            alias: table.alias.as_ref().map(|alias| normalize_identifier(alias.name.as_ref())),
        }),
        oak_sql::ast::TableSource::Derived { query, alias, .. } => {
            let alias =
                alias.as_ref().ok_or_else(|| bind_error("SQL derived table requires an alias"))?;
            Ok(SqlBoundSource::Derived {
                query: Box::new(bind_select_statement(query)?),
                alias: normalize_identifier(alias.name.as_ref()),
            })
        }
    }
}

fn bind_projection(item: &SelectItem) -> Result<SqlBoundProjection, SqlFrontendError> {
    match item {
        SelectItem::Star { .. } => Ok(SqlBoundProjection::Star),
        SelectItem::Expression { expr, alias, .. } => Ok(SqlBoundProjection::Expression {
            expression: bind_expression(expr)?,
            alias: alias.as_ref().map(|alias| normalize_identifier(alias.name.as_ref())),
        }),
    }
}

fn bind_expression(expression: &Expression) -> Result<SqlBoundExpression, SqlFrontendError> {
    match expression {
        Expression::Identifier(identifier) => {
            Ok(SqlBoundExpression::Identifier(normalize_identifier(identifier.name.as_ref())))
        }
        Expression::Literal(literal) => Ok(SqlBoundExpression::Literal(bind_literal(literal)?)),
        Expression::Wildcard { .. } => Ok(SqlBoundExpression::Wildcard),
        Expression::Binary { left, op, right, .. } => Ok(SqlBoundExpression::Binary {
            left: Box::new(bind_expression(left)?),
            operator: bind_binary_operator(*op)?,
            right: Box::new(bind_expression(right)?),
        }),
        Expression::Unary { op, expr, .. } => {
            // `-<integer>` 折叠成一个精确的负整数字面量：这样 `-9223372036854775808`
            // 仍落在 `i64` 内，而不是先解析成正数再溢出回浮点。
            if let oak_sql::ast::UnaryOperator::Minus = op {
                if let Expression::Literal(Literal::Number(text, _)) = expr.as_ref() {
                    if let Some(value) = negated_integer_literal(text) {
                        return Ok(SqlBoundExpression::Literal(SqlBoundLiteral::Integer(value)));
                    }
                }
            }
            Ok(SqlBoundExpression::Unary {
                operator: match op {
                    oak_sql::ast::UnaryOperator::Plus => SqlBoundUnaryOperator::Plus,
                    oak_sql::ast::UnaryOperator::Minus => SqlBoundUnaryOperator::Minus,
                    oak_sql::ast::UnaryOperator::Not => SqlBoundUnaryOperator::Not,
                },
                expression: Box::new(bind_expression(expr)?),
            })
        }
        Expression::IsNull { expr, negated, .. } => Ok(SqlBoundExpression::Unary {
            operator: if *negated { SqlBoundUnaryOperator::IsNotNull } else { SqlBoundUnaryOperator::IsNull },
            expression: Box::new(bind_expression(expr)?),
        }),
        Expression::IsTruth { expr, value, negated, .. } => Ok(SqlBoundExpression::Unary {
            operator: SqlBoundUnaryOperator::IsTruth { value: *value, negated: *negated },
            expression: Box::new(bind_expression(expr)?),
        }),
        Expression::Between { expr, low, high, negated, .. } => {
            // 先构造 `expr >= low AND expr <= high`
            let e = bind_expression(expr)?;
            let lo = bind_expression(low)?;
            let hi = bind_expression(high)?;
            let left = SqlBoundExpression::Binary {
                left: Box::new(e.clone()),
                operator: SqlBoundBinaryOperator::GreaterEqual,
                right: Box::new(lo),
            };
            let right = SqlBoundExpression::Binary {
                left: Box::new(e),
                operator: SqlBoundBinaryOperator::LessEqual,
                right: Box::new(hi),
            };
            let between = SqlBoundExpression::Binary {
                left: Box::new(left),
                operator: SqlBoundBinaryOperator::And,
                right: Box::new(right),
            };
            if *negated {
                Ok(SqlBoundExpression::Unary { operator: SqlBoundUnaryOperator::Not, expression: Box::new(between) })
            }
            else {
                Ok(between)
            }
        }
        Expression::InList { expr, list, negated, .. } => {
            // Expand IN into `expr = v1 OR expr = v2 OR ... OR expr = vN`
            if list.is_empty() {
                return Err(bind_error("IN list must not be empty"));
            }
            let bound_expr = bind_expression(expr)?;
            let mut or_tree: Option<SqlBoundExpression> = None;
            for item in list {
                let eq = SqlBoundExpression::Binary {
                    left: Box::new(bound_expr.clone()),
                    operator: SqlBoundBinaryOperator::Equal,
                    right: Box::new(bind_expression(item)?),
                };
                or_tree = match or_tree {
                    None => Some(eq),
                    Some(acc) => Some(SqlBoundExpression::Binary {
                        left: Box::new(acc),
                        operator: SqlBoundBinaryOperator::Or,
                        right: Box::new(eq),
                    }),
                };
            }
            let in_expr = or_tree.unwrap();
            if *negated {
                Ok(SqlBoundExpression::Unary { operator: SqlBoundUnaryOperator::Not, expression: Box::new(in_expr) })
            }
            else {
                Ok(in_expr)
            }
        }
        Expression::FunctionCall { name, args, distinct, .. } => {
            // Canonical lower-case name: description inference matches the
            // documented spelling, while execution stays case-insensitive.
            // Diagnostics keep the spelling the query used.
            let display = normalize_identifier(name.name.as_ref());
            let fname = display.to_ascii_lowercase();

            let bound_args = args.iter().map(bind_expression).collect::<Result<Vec<_>, _>>()?;
            // 聚合函数与标量内建函数是两类；未知名称在绑定期直接拒绝。
            if SqlAggregateFunction::from_name(&fname).is_some() {
                if bound_args.is_empty() {
                    return Err(SqlFrontendError::unsupported(
                        "aggregate functions require an argument",
                    ));
                }
                if *distinct && fname != "count" {
                    return Err(SqlFrontendError::unsupported(
                        "only COUNT supports the DISTINCT quantifier",
                    ));
                }
                return Ok(SqlBoundExpression::Function { name: fname, args: bound_args, distinct: *distinct });
            }
            let scalar = SqlScalarFunction::from_name(&fname).ok_or_else(|| {
                SqlFrontendError::new(
                    SqlErrorKind::UnknownFunction,
                    format!(
                        "unknown SQL function: {display} (supported: aggregate COUNT/SUM/AVG/MAX/MIN and scalar VERSION/DATABASE/CURRENT_SCHEMA/NOW/CURRENT_DATE/CURRENT_TIME/CURDATE/CURTIME/LOWER/UPPER/LENGTH/CHAR_LENGTH/SUBSTRING/TRIM/LTRIM/RTRIM/REPLACE/CONCAT/COALESCE/ABS/ROUND)"
                    ),
                )
            })?;
            let (minimum, maximum) = scalar.arity();
            let supplied = bound_args.len();
            if supplied < minimum || maximum.is_some_and(|max| supplied > max) {
                return Err(SqlFrontendError::new(
                    SqlErrorKind::UnknownFunction,
                    format!(
                        "{display}() expects {}{} argument(s), got {supplied}",
                        minimum,
                        match maximum {
                            Some(max) if max == minimum => String::new(),
                            Some(max) => format!("..{max}"),
                            None => "+".to_string(),
                        }
                    ),
                ));
            }
            Ok(SqlBoundExpression::Function { name: fname, args: bound_args, distinct: false })
        }
        Expression::InSubquery { expr, query, negated, .. } => Ok(SqlBoundExpression::InSubquery {
            expression: Box::new(bind_expression(expr)?),
            select: Box::new(bind_select_statement(query)?),
            negated: *negated,
        }),
        Expression::Case { branches, else_branch, .. } => {
            let bound_branches = branches
                .iter()
                .map(|(when, then)| {
                    let w = bind_expression(when)?;
                    let t = bind_expression(then)?;
                    Ok((Box::new(w), Box::new(t)))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let bound_else = match else_branch {
                Some(els) => Some(Box::new(bind_expression(els)?)),
                None => None,
            };
            Ok(SqlBoundExpression::Case { branches: bound_branches, else_branch: bound_else })
        }
        Expression::Cast { expr, target, .. } => {
            Ok(SqlBoundExpression::Cast {
                expr: Box::new(bind_expression(expr)?),
                target: target.to_string(),
            })
        }
        // 单独出现的 `(SELECT ...)` 是标量子查询：值语义由 planner 与执行层固定。
        Expression::Subquery { query, .. } => {
            Ok(SqlBoundExpression::ScalarSubquery { select: Box::new(bind_select_statement(query)?) })
        }
        Expression::Exists { query, negated, .. } => Ok(SqlBoundExpression::Exists {
            select: Box::new(bind_select_statement(query)?),
            negated: *negated,
        }),
        Expression::Vector { .. } => {
            Err(SqlFrontendError::unsupported("SQL vector literals are outside the gateway binding subset"))
        }
        Expression::Error { message, .. } => Err(bind_error(message)),
    }
}

/// Binds one Oak literal, keeping integer spellings exact and rejecting numeric
/// literals that no SQL value in this gateway can represent.
fn bind_literal(literal: &Literal) -> Result<SqlBoundLiteral, SqlFrontendError> {
    match literal {
        Literal::Number(text, _) => bind_number_literal(text),
        Literal::String(value, _) => Ok(SqlBoundLiteral::String(value.to_string())),
        Literal::Boolean(value, _) => Ok(SqlBoundLiteral::Boolean(*value)),
        Literal::Null(_) => Ok(SqlBoundLiteral::Null),
    }
}

/// Numeric literal binding: pure integer spellings must fit `i64` exactly,
/// decimal/exponent spellings must be finite `f64`. Out-of-range literals are
/// rejected instead of being silently truncated to a float.
fn bind_number_literal(text: &str) -> Result<SqlBoundLiteral, SqlFrontendError> {
    if !text.contains('.') && !text.contains('e') && !text.contains('E') {
        return match text.parse::<i64>() {
            Ok(value) => Ok(SqlBoundLiteral::Integer(value)),
            Err(_) => Err(bind_error(&format!("numeric literal out of range: {text}"))),
        };
    }
    match text.parse::<f64>() {
        Ok(value) if value.is_finite() => Ok(SqlBoundLiteral::Number(text.to_string())),
        _ => Err(bind_error(&format!("numeric literal out of range: {text}"))),
    }
}

/// Folds `-<integer literal>` into a negative `i64` when the magnitude fits,
/// including `9223372036854775808` for `i64::MIN`. Non-integer or wider
/// spellings return `None` and keep the unary node.
fn negated_integer_literal(text: &str) -> Option<i64> {
    if text.contains('.') || text.contains('e') || text.contains('E') {
        return None;
    }
    let magnitude = text.parse::<i128>().ok()?;
    i64::try_from(-magnitude).ok()
}

fn bind_binary_operator(operator: BinaryOperator) -> Result<SqlBoundBinaryOperator, SqlFrontendError> {
    Ok(match operator {
        BinaryOperator::Plus => SqlBoundBinaryOperator::Plus,
        BinaryOperator::Minus => SqlBoundBinaryOperator::Minus,
        BinaryOperator::Star => SqlBoundBinaryOperator::Multiply,
        BinaryOperator::Slash => SqlBoundBinaryOperator::Divide,
        BinaryOperator::Percent => SqlBoundBinaryOperator::Modulo,
        BinaryOperator::And => SqlBoundBinaryOperator::And,
        BinaryOperator::Or => SqlBoundBinaryOperator::Or,
        BinaryOperator::Equal => SqlBoundBinaryOperator::Equal,
        BinaryOperator::NotEqual => SqlBoundBinaryOperator::NotEqual,
        BinaryOperator::Less => SqlBoundBinaryOperator::Less,
        BinaryOperator::Greater => SqlBoundBinaryOperator::Greater,
        BinaryOperator::LessEqual => SqlBoundBinaryOperator::LessEqual,
        BinaryOperator::GreaterEqual => SqlBoundBinaryOperator::GreaterEqual,
        BinaryOperator::Like => SqlBoundBinaryOperator::Like,
    })
}

fn bind_error(message: &str) -> SqlFrontendError {
    SqlFrontendError::syntax(message)
}

/// Extracts a non-negative integer from an Oak SQL expression — only accepts
/// `Literal::Number` with a valid `u64` spelling.
fn extract_u64(expr: &Expression) -> Option<u64> {
    match expr {
        Expression::Literal(Literal::Number(n, _)) => n.parse::<u64>().ok(),
        _ => None,
    }
}

fn normalize_identifier(name: &str) -> String {
    name.trim().to_string()
}
