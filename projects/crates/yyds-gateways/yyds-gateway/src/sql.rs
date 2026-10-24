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

/// SQL SELECT request owned by the compatibility gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundSelect {
    /// Optional source table name as written by the client.
    pub source: Option<String>,
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

/// One JOIN clause retained by the gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundJoin {
    /// Join kind as written by the client.
    pub kind: SqlBoundJoinKind,
    /// Joined table name as written by the client.
    pub table: String,
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

/// One SQL ORDER BY item — expression + direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundOrderByItem {
    pub expr: SqlBoundExpression,
    pub direction: SqlBoundOrderDirection,
}

/// Sort direction for ORDER BY.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundOrderDirection {
    Asc,
    Desc,
}

/// SQL INSERT request owned by the compatibility gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundInsert {
    /// Target table name.
    pub table: String,
    /// Optional explicit column list. When absent the catalog will infer order.
    pub columns: Option<Vec<String>>,
    /// Values to insert, in column-list order (or catalog order when no columns).
    pub values: Vec<SqlBoundExpression>,
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
    /// `NOW()` / `CURRENT_TIMESTAMP()` — session timestamp.
    Now,
    /// `LOWER(x)` / `LCASE(x)`.
    Lower,
    /// `UPPER(x)` / `UCASE(x)`.
    Upper,
    /// `LENGTH(x)` / `CHAR_LENGTH(x)`.
    Length,
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
            "LOWER" | "LCASE" => Some(Self::Lower),
            "UPPER" | "UCASE" => Some(Self::Upper),
            "LENGTH" | "CHAR_LENGTH" | "CHARACTER_LENGTH" | "LEN" => Some(Self::Length),
            "COALESCE" | "IFNULL" => Some(Self::Coalesce),
            "ABS" => Some(Self::Abs),
            "ROUND" => Some(Self::Round),
            _ => None,
        }
    }

    /// Expected argument count as `(minimum, maximum)`, where `maximum == None`
    /// means variadic.
    pub fn arity(&self) -> (usize, Option<usize>) {
        match self {
            Self::Version | Self::Database | Self::CurrentSchema | Self::Now => (0, Some(0)),
            Self::Lower | Self::Upper | Self::Length | Self::Abs => (1, Some(1)),
            Self::Round => (1, Some(2)),
            Self::Coalesce => (1, None),
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
    let surface = scan_grouping_surface(source)?;
    let rewritten = rewrite_grouping_surface(source, &surface)?;
    let parse_source = rewritten.as_deref().unwrap_or(source);
    let root = parse_sql(parse_source)?;
    if root.statements.len() != 1 {
        return Err(bind_error("SQL gateway accepts exactly one statement"));
    }
    let modifiers = scan_select_modifiers(source);
    if modifiers.nulls_modifiers > 0 {
        return Err(SqlFrontendError::unsupported(
            "NULLS FIRST/LAST is not exposed by the pinned Oak SQL AST and is outside the gateway subset",
        ));
    }
    if !matches!(root.statements[0], SqlStatement::Select(_)) && modifiers.distinct_tokens > 0 {
        return Err(SqlFrontendError::unsupported("DISTINCT is supported only as the leading modifier of a SELECT statement"));
    }
    match &root.statements[0] {
        SqlStatement::Select(select) => {
            let distinct = modifiers.select_distinct.first().copied().unwrap_or(false);
            if modifiers.select_distinct.iter().skip(1).any(|value| *value) {
                return Err(SqlFrontendError::unsupported("SELECT DISTINCT inside a subquery is outside the gateway subset"));
            }
            if modifiers.distinct_tokens > usize::from(distinct) {
                return Err(SqlFrontendError::unsupported(
                    "DISTINCT is supported only as the leading modifier of a SELECT statement",
                ));
            }
            Ok(SqlBoundStatement::Select(bind_select_statement(select, distinct)?))
        }
        SqlStatement::Insert(insert) => {
            let columns = if insert.columns.is_empty() {
                None
            }
            else {
                Some(insert.columns.iter().map(|id| normalize_identifier(id.name.as_ref())).collect())
            };
            let values = insert.values.iter().map(bind_expression).collect::<Result<Vec<_>, _>>()?;
            Ok(SqlBoundStatement::Insert(SqlBoundInsert {
                table: normalize_identifier(insert.table_name.name.name.as_ref()),
                columns,
                values,
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
        _ => Err(SqlFrontendError::unsupported("SQL gateway binding accepts SELECT/INSERT/UPDATE/DELETE only")),
    }
}

/// `SELECT` modifiers recovered from the statement surface.
///
/// Oak's pinned SQL AST parses `SELECT DISTINCT` but does not publish the
/// modifier, so the gateway classifies each unquoted `SELECT` keyword from the
/// token surface. Quoted strings, quoted identifiers and comments are skipped,
/// and the scan never guesses at expression semantics.
struct SelectModifiers {
    /// One entry per unquoted `SELECT` keyword, in source order.
    select_distinct: Vec<bool>,
    /// Number of unquoted `DISTINCT` keyword tokens anywhere in the statement.
    distinct_tokens: usize,
    /// Number of `NULLS FIRST` / `NULLS LAST` modifiers in the statement.
    nulls_modifiers: usize,
}

fn scan_select_modifiers(source: &str) -> SelectModifiers {
    let bytes = source.as_bytes();
    let mut index = 0;
    let mut modifiers = SelectModifiers { select_distinct: Vec::new(), distinct_tokens: 0, nulls_modifiers: 0 };
    let mut awaiting_select_modifier = false;
    let mut awaiting_nulls_modifier = false;
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
            continue;
        }
        if byte == b'-' && bytes.get(index + 1) == Some(&b'-') {
            index = skip_line_comment(bytes, index + 2);
            continue;
        }
        if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index = skip_block_comment(bytes, index + 2);
            continue;
        }
        if byte == b'#' {
            index = skip_line_comment(bytes, index + 1);
            continue;
        }
        if matches!(byte, b'\'' | b'"' | b'`' | b'[') {
            if awaiting_select_modifier {
                modifiers.select_distinct.push(false);
                awaiting_select_modifier = false;
            }
            awaiting_nulls_modifier = false;
            index = skip_quoted(bytes, index, byte);
            continue;
        }
        if byte.is_ascii_alphabetic() || byte == b'_' {
            let start = index;
            while index < bytes.len() && (bytes[index].is_ascii_alphanumeric() || matches!(bytes[index], b'_' | b'$' | b'#')) {
                index += 1;
            }
            let word = &source[start..index];
            if awaiting_select_modifier {
                modifiers.select_distinct.push(word.eq_ignore_ascii_case("DISTINCT"));
                awaiting_select_modifier = false;
            }
            if awaiting_nulls_modifier && (word.eq_ignore_ascii_case("FIRST") || word.eq_ignore_ascii_case("LAST")) {
                modifiers.nulls_modifiers += 1;
            }
            awaiting_nulls_modifier = word.eq_ignore_ascii_case("NULLS");
            if word.eq_ignore_ascii_case("DISTINCT") {
                modifiers.distinct_tokens += 1;
            }
            if word.eq_ignore_ascii_case("SELECT") {
                awaiting_select_modifier = true;
            }
            continue;
        }
        if awaiting_select_modifier {
            modifiers.select_distinct.push(false);
            awaiting_select_modifier = false;
        }
        awaiting_nulls_modifier = false;
        index += 1;
    }
    if awaiting_select_modifier {
        modifiers.select_distinct.push(false);
    }
    modifiers
}

/// Top-level grouping clauses recovered from the statement surface.
///
/// Oak's pinned SQL AST builds `GroupByClause` only from child `Expression`
/// nodes, so a bare column (`GROUP BY age`) yields an empty column list, and a
/// bare `HAVING` condition fails to build at all. The gateway classifies the
/// clause bodies from the token surface and re-parses them with every item
/// parenthesized, which makes Oak wrap each item in an `Expression` node.
/// Nothing below guesses at expression semantics: every body is still parsed by
/// Oak.
#[derive(Default)]
struct GroupingSurface {
    /// Byte range of the `GROUP BY` body in the statement.
    group_body: Option<(usize, usize)>,
    /// Byte range of the `HAVING` body in the statement.
    having_body: Option<(usize, usize)>,
}

fn scan_grouping_surface(source: &str) -> Result<GroupingSurface, SqlFrontendError> {
    let bytes = source.as_bytes();
    let mut index = 0;
    let mut depth = 0usize;
    let mut words: Vec<(usize, usize, usize)> = Vec::new();
    while index < bytes.len() {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
            continue;
        }
        if byte == b'-' && bytes.get(index + 1) == Some(&b'-') {
            index = skip_line_comment(bytes, index + 2);
            continue;
        }
        if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index = skip_block_comment(bytes, index + 2);
            continue;
        }
        if byte == b'#' {
            index = skip_line_comment(bytes, index + 1);
            continue;
        }
        if matches!(byte, b'\'' | b'"' | b'`' | b'[') {
            index = skip_quoted(bytes, index, byte);
            continue;
        }
        if byte == b'(' {
            depth += 1;
            index += 1;
            continue;
        }
        if byte == b')' {
            depth = depth.saturating_sub(1);
            index += 1;
            continue;
        }
        if byte.is_ascii_alphabetic() || byte == b'_' {
            let start = index;
            while index < bytes.len() && (bytes[index].is_ascii_alphanumeric() || matches!(bytes[index], b'_' | b'$' | b'#')) {
                index += 1;
            }
            words.push((start, index, depth));
            continue;
        }
        index += 1;
    }
    if !words.first().is_some_and(|(start, end, _)| source[*start..*end].eq_ignore_ascii_case("SELECT")) {
        return Ok(GroupingSurface::default());
    }
    let mut surface = GroupingSurface::default();
    for (position, (start, end, word_depth)) in words.iter().enumerate() {
        let word = &source[*start..*end];
        let is_group = word.eq_ignore_ascii_case("GROUP") && group_keyword_is_followed_by(source, &words, position);
        let is_having = word.eq_ignore_ascii_case("HAVING");
        if !is_group && !is_having {
            continue;
        }
        if *word_depth > 0 {
            return Err(SqlFrontendError::unsupported("GROUP BY / HAVING inside a subquery is outside the gateway subset"));
        }
        if is_group {
            let (_, body_start, _) = words[position + 1];
            if surface.group_body.is_some() {
                return Err(SqlFrontendError::unsupported("duplicate GROUP BY is outside the gateway subset"));
            }
            let body_end = clause_body_end(source, &words, position + 2, &["HAVING", "ORDER", "LIMIT", "OFFSET"]);
            surface.group_body = Some((body_start, body_end));
        }
        else {
            if surface.having_body.is_some() {
                return Err(SqlFrontendError::unsupported("duplicate HAVING is outside the gateway subset"));
            }
            let body_end = clause_body_end(source, &words, position + 1, &["ORDER", "LIMIT", "OFFSET"]);
            surface.having_body = Some((*end, body_end));
        }
    }
    Ok(surface)
}

fn group_keyword_is_followed_by(source: &str, words: &[(usize, usize, usize)], position: usize) -> bool {
    match words.get(position + 1) {
        Some((start, end, depth)) if *depth == words[position].2 => source[*start..*end].eq_ignore_ascii_case("BY"),
        _ => false,
    }
}

/// First top-level clause keyword after `from`, or the end of the statement.
fn clause_body_end(source: &str, words: &[(usize, usize, usize)], from: usize, stops: &[&str]) -> usize {
    for (start, end, depth) in &words[from.min(words.len())..] {
        if *depth == 0 && stops.iter().any(|stop| source[*start..*end].eq_ignore_ascii_case(stop)) {
            return *start;
        }
    }
    statement_end(source)
}

fn statement_end(source: &str) -> usize {
    let mut end = source.len();
    while end > 0 && source.as_bytes()[end - 1].is_ascii_whitespace() {
        end -= 1;
    }
    if end > 0 && source.as_bytes()[end - 1] == b';' {
        end -= 1;
    }
    end
}

/// Splits a clause body on top-level commas, skipping comments and quoted text.
fn split_group_items(source: &str, start: usize, end: usize) -> Vec<(usize, usize)> {
    let bytes = source.as_bytes();
    let mut index = start;
    let mut depth = 0usize;
    let mut item_start = start;
    let mut items = Vec::new();
    while index < end {
        let byte = bytes[index];
        if byte.is_ascii_whitespace() {
            index += 1;
            continue;
        }
        if byte == b'-' && bytes.get(index + 1) == Some(&b'-') {
            index = skip_line_comment(bytes, index + 2);
            continue;
        }
        if byte == b'/' && bytes.get(index + 1) == Some(&b'*') {
            index = skip_block_comment(bytes, index + 2);
            continue;
        }
        if byte == b'#' {
            index = skip_line_comment(bytes, index + 1);
            continue;
        }
        if matches!(byte, b'\'' | b'"' | b'`' | b'[') {
            index = skip_quoted(bytes, index, byte);
            continue;
        }
        if byte == b'(' {
            depth += 1;
            index += 1;
            continue;
        }
        if byte == b')' {
            depth = depth.saturating_sub(1);
            index += 1;
            continue;
        }
        if byte == b',' && depth == 0 {
            items.push((item_start, index));
            index += 1;
            item_start = index;
            continue;
        }
        index += 1;
    }
    items.push((item_start, end));
    items
}

fn rewrite_grouping_surface(source: &str, surface: &GroupingSurface) -> Result<Option<String>, SqlFrontendError> {
    let mut replacements: Vec<(usize, usize, String)> = Vec::new();
    if let Some((start, end)) = surface.group_body {
        let items = split_group_items(source, start, end);
        let mut body = String::new();
        for (position, (item_start, item_end)) in items.iter().enumerate() {
            let item = source[*item_start..*item_end].trim();
            if item.is_empty() {
                return Err(bind_error("GROUP BY requires at least one column"));
            }
            if position > 0 {
                body.push_str(", ");
            }
            body.push('(');
            body.push_str(item);
            body.push(')');
        }
        replacements.push((start, end, body));
    }
    if let Some((start, end)) = surface.having_body {
        let condition = source[start..end].trim();
        if condition.is_empty() {
            return Err(bind_error("HAVING requires a condition"));
        }
        replacements.push((start, end, format!("({condition})")));
    }
    if replacements.is_empty() {
        return Ok(None);
    }
    replacements.sort_by(|left, right| right.0.cmp(&left.0));
    let mut rewritten = source.to_string();
    for (start, end, replacement) in replacements {
        rewritten.replace_range(start..end, &replacement);
    }
    Ok(Some(rewritten))
}

fn skip_line_comment(bytes: &[u8], mut index: usize) -> usize {
    while index < bytes.len() && bytes[index] != b'\n' {
        index += 1;
    }
    index
}

fn skip_block_comment(bytes: &[u8], mut index: usize) -> usize {
    while index + 1 < bytes.len() {
        if bytes[index] == b'*' && bytes[index + 1] == b'/' {
            return index + 2;
        }
        index += 1;
    }
    bytes.len()
}

fn skip_quoted(bytes: &[u8], mut index: usize, quote: u8) -> usize {
    let closing = if quote == b'[' { b']' } else { quote };
    index += 1;
    while index < bytes.len() {
        if bytes[index] == b'\\' && quote == b'\'' {
            index += 2;
            continue;
        }
        if bytes[index] == closing {
            if bytes.get(index + 1) == Some(&closing) {
                index += 2;
                continue;
            }
            return index + 1;
        }
        index += 1;
    }
    bytes.len()
}

/// Binds one Oak `SELECT` — shared by top-level statements and `IN (SELECT ...)` subqueries.
fn bind_select_statement(select: &oak_sql::ast::SelectStatement, distinct: bool) -> Result<SqlBoundSelect, SqlFrontendError> {
    let projections = select.items.iter().map(bind_projection).collect::<Result<Vec<_>, _>>()?;
    let (limit, offset) = match &select.limit {
        Some(clause) => {
            let n = extract_u64(&clause.limit).ok_or_else(|| bind_error("SQL LIMIT must be a non-negative integer literal"))?
                as usize;
            let off = clause.offset.as_ref().and_then(extract_u64).map(|v| v as usize);
            (Some(n), off)
        }
        None => (None, None),
    };
    let order_by = match &select.order_by {
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
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Some(items)
        }
        _ => None,
    };
    let group_by = match &select.group_by {
        Some(clause) if !clause.columns.is_empty() => {
            Some(clause.columns.iter().map(bind_expression).collect::<Result<Vec<_>, _>>()?)
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
                table: normalize_identifier(join.table.name.name.as_ref()),
                on: join
                    .on
                    .as_ref()
                    .ok_or_else(|| bind_error("SQL JOIN requires an ON condition"))
                    .and_then(bind_expression)?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SqlBoundSelect {
        source: select.from.as_ref().map(|table| normalize_identifier(table.name.name.as_ref())),
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
        Expression::FunctionCall { name, args, .. } => {
            // Canonical lower-case name: description inference matches the
            // documented spelling, while execution stays case-insensitive.
            // Diagnostics keep the spelling the query used.
            let display = normalize_identifier(name.name.as_ref());
            let fname = display.to_ascii_lowercase();
            let bound_args = args.iter().map(bind_expression).collect::<Result<Vec<_>, _>>()?;
            // 聚合函数与标量内建函数是两类；未知名称在绑定期直接拒绝。
            if SqlAggregateFunction::from_name(&fname).is_some() {
                return Ok(SqlBoundExpression::Function { name: fname, args: bound_args });
            }
            let scalar = SqlScalarFunction::from_name(&fname).ok_or_else(|| {
                SqlFrontendError::new(
                    SqlErrorKind::UnknownFunction,
                    format!(
                        "unknown SQL function: {display} (supported: aggregate COUNT/SUM/AVG/MAX/MIN and scalar VERSION/DATABASE/CURRENT_SCHEMA/NOW/LOWER/UPPER/LENGTH/COALESCE/ABS/ROUND)"
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
            Ok(SqlBoundExpression::Function { name: fname, args: bound_args })
        }
        Expression::InSubquery { expr, query, negated, .. } => Ok(SqlBoundExpression::InSubquery {
            expression: Box::new(bind_expression(expr)?),
            select: Box::new(bind_select_statement(query, false)?),
            negated: *negated,
        }),
        // 子查询只在 `IN (<subquery>)` 里有语义；单独出现的 `(SELECT ...)` 不在网关子集内。
        Expression::Subquery { .. } => {
            Err(SqlFrontendError::unsupported("SQL subquery is only supported as the right side of IN"))
        }
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
