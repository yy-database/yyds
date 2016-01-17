//! Oak SQL frontend entry point for compatibility gateways.
//!
//! This module only parses the SQL surface. It does not execute SQL, own a
//! query planner, or bypass the YY execution model.

use oak_sql::ast::{BinaryOperator, Expression, Literal, SelectItem, SqlStatement};

/// Structured failure from the Oak SQL frontend or gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlFrontendError {
    /// Diagnostic text returned by the Oak parser.
    pub message: String,
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
    let root = oak_sql::parse(source).map_err(|message| SqlFrontendError { message })?;
    for statement in &root.statements {
        match statement {
            SqlStatement::Error { message, .. } => return Err(SqlFrontendError { message: message.to_string() }),
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
    let root = parse_sql(source)?;
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
            value: bind_expression(&statement.value, source)?,
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
            value: statement.value.as_ref().map(|value| bind_expression(value, source)).transpose()?,
        }),
        _ => None,
    })
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
    /// Numeric SQL token, not yet coerced by a YYDS catalog binder.
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
}

/// Binds the supported SQL compatibility subset without executing it.
pub fn bind_sql(source: &str) -> Result<SqlBoundStatement, SqlFrontendError> {
    let root = parse_sql(source)?;
    if root.statements.len() != 1 {
        return Err(bind_error("SQL gateway accepts exactly one statement"));
    }
    match &root.statements[0] {
        SqlStatement::Select(select) => Ok(SqlBoundStatement::Select(bind_select_statement(source, select)?)),
        SqlStatement::Insert(insert) => {
            let columns = if insert.columns.is_empty() {
                None
            }
            else {
                Some(insert.columns.iter().map(|id| normalize_identifier(id.name.as_ref())).collect())
            };
            let values = insert.values.iter().map(|value| bind_expression(value, source)).collect::<Result<Vec<_>, _>>()?;
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
                        value: bind_expression(&a.value, source)?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok(SqlBoundStatement::Update(SqlBoundUpdate {
                table: normalize_identifier(update.table_name.name.name.as_ref()),
                assignments,
                predicate: update.selection.as_ref().map(|selection| bind_expression(selection, source)).transpose()?,
            }))
        }
        SqlStatement::Delete(delete) => Ok(SqlBoundStatement::Delete(SqlBoundDelete {
            table: normalize_identifier(delete.table_name.name.name.as_ref()),
            predicate: delete.selection.as_ref().map(|selection| bind_expression(selection, source)).transpose()?,
        })),
        _ => Err(bind_error("SQL gateway binding accepts SELECT/INSERT/UPDATE/DELETE only")),
    }
}

/// Binds one Oak `SELECT` — shared by top-level statements and `IN (SELECT ...)` subqueries.
fn bind_select_statement(source: &str, select: &oak_sql::ast::SelectStatement) -> Result<SqlBoundSelect, SqlFrontendError> {
    let projections = select.items.iter().map(|item| bind_projection(item, source)).collect::<Result<Vec<_>, _>>()?;
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
                        expr: bind_expression(&item.expr, source)?,
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
            Some(clause.columns.iter().map(|column| bind_expression(column, source)).collect::<Result<Vec<_>, _>>()?)
        }
        _ => None,
    };
    let having = select.having.as_ref().map(|h| bind_expression(&h.condition, source)).transpose()?;
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
                    .and_then(|on| bind_expression(on, source))?,
            })
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(SqlBoundSelect {
        source: select.from.as_ref().map(|table| normalize_identifier(table.name.name.as_ref())),
        joins,
        projections,
        predicate: select.expr.as_ref().map(|expr| bind_expression(expr, source)).transpose()?,
        group_by,
        having,
        limit,
        offset,
        order_by,
    })
}

fn bind_projection(item: &SelectItem, source: &str) -> Result<SqlBoundProjection, SqlFrontendError> {
    match item {
        SelectItem::Star { .. } => Ok(SqlBoundProjection::Star),
        SelectItem::Expression { expr, alias, .. } => Ok(SqlBoundProjection::Expression {
            expression: bind_expression(expr, source)?,
            alias: alias.as_ref().map(|alias| normalize_identifier(alias.name.as_ref())),
        }),
    }
}

fn bind_expression(expression: &Expression, source: &str) -> Result<SqlBoundExpression, SqlFrontendError> {
    match expression {
        Expression::Identifier(identifier) => {
            Ok(SqlBoundExpression::Identifier(normalize_identifier(identifier.name.as_ref())))
        }
        Expression::Literal(literal) => Ok(SqlBoundExpression::Literal(match literal {
            Literal::Number(value, _) => SqlBoundLiteral::Number(value.to_string()),
            Literal::String(value, _) => SqlBoundLiteral::String(value.to_string()),
            Literal::Boolean(value, _) => SqlBoundLiteral::Boolean(*value),
            Literal::Null(_) => SqlBoundLiteral::Null,
        })),
        Expression::Wildcard { .. } => Ok(SqlBoundExpression::Wildcard),
        Expression::Binary { left, op, right, .. } => {
            // `oak-sql` 把 `IS [NOT] NULL` 折成 `= NULL` / `<> NULL`。AST 只有比较运算符，
            // 因此从两个操作数的 span 之间恢复 `IS` 关键字，把它绑成独立的空值判断，
            // 而不是退化成与 NULL 的比较。
            if let Some(negated) = is_null_test(source, left, right) {
                return Ok(SqlBoundExpression::Unary {
                    operator: if negated { SqlBoundUnaryOperator::IsNotNull } else { SqlBoundUnaryOperator::IsNull },
                    expression: Box::new(bind_expression(left, source)?),
                });
            }
            Ok(SqlBoundExpression::Binary {
                left: Box::new(bind_expression(left, source)?),
                operator: bind_binary_operator(*op)?,
                right: Box::new(bind_expression(right, source)?),
            })
        }
        Expression::Unary { op, expr, .. } => Ok(SqlBoundExpression::Unary {
            operator: match op {
                oak_sql::ast::UnaryOperator::Plus => SqlBoundUnaryOperator::Plus,
                oak_sql::ast::UnaryOperator::Minus => SqlBoundUnaryOperator::Minus,
                oak_sql::ast::UnaryOperator::Not => SqlBoundUnaryOperator::Not,
            },
            expression: Box::new(bind_expression(expr, source)?),
        }),
        Expression::Between { expr, low, high, negated, .. } => {
            // 先构造 `expr >= low AND expr <= high`
            let e = bind_expression(expr, source)?;
            let lo = bind_expression(low, source)?;
            let hi = bind_expression(high, source)?;
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
            let bound_expr = bind_expression(expr, source)?;
            let mut or_tree: Option<SqlBoundExpression> = None;
            for item in list {
                let eq = SqlBoundExpression::Binary {
                    left: Box::new(bound_expr.clone()),
                    operator: SqlBoundBinaryOperator::Equal,
                    right: Box::new(bind_expression(item, source)?),
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
            let fname = normalize_identifier(name.name.as_ref());
            let bound_args = args.iter().map(|arg| bind_expression(arg, source)).collect::<Result<Vec<_>, _>>()?;
            // 聚合函数与标量内建函数是两类；未知名称在绑定期直接拒绝。
            if SqlAggregateFunction::from_name(&fname).is_some() {
                return Ok(SqlBoundExpression::Function { name: fname, args: bound_args });
            }
            let scalar = SqlScalarFunction::from_name(&fname).ok_or_else(|| {
                bind_error(&format!(
                    "unknown SQL function: {fname} (supported: aggregate COUNT/SUM/AVG/MAX/MIN and scalar VERSION/DATABASE/CURRENT_SCHEMA/NOW/LOWER/UPPER/LENGTH/COALESCE/ABS/ROUND)"
                ))
            })?;
            let (minimum, maximum) = scalar.arity();
            let supplied = bound_args.len();
            if supplied < minimum || maximum.is_some_and(|max| supplied > max) {
                return Err(bind_error(&format!(
                    "{fname}() expects {}{} argument(s), got {supplied}",
                    minimum,
                    match maximum {
                        Some(max) if max == minimum => String::new(),
                        Some(max) => format!("..{max}"),
                        None => "+".to_string(),
                    }
                )));
            }
            Ok(SqlBoundExpression::Function { name: fname, args: bound_args })
        }
        Expression::InSubquery { expr, query, negated, .. } => Ok(SqlBoundExpression::InSubquery {
            expression: Box::new(bind_expression(expr, source)?),
            select: Box::new(bind_select_statement(source, query)?),
            negated: *negated,
        }),
        // 子查询只在 `IN (<subquery>)` 里有语义；单独出现的 `(SELECT ...)` 不在网关子集内。
        Expression::Subquery { .. } => Err(bind_error("SQL subquery is only supported as the right side of IN")),
        Expression::Vector { .. } => Err(bind_error("SQL vector literals are outside the gateway binding subset")),
        Expression::Error { message, .. } => Err(bind_error(message)),
    }
}

/// Detects `IS [NOT] NULL` that Oak's builder folded into `= NULL` / `<> NULL`.
///
/// Returns `Some(negated)` when the text between the operands contains an `IS`
/// keyword, i.e. the comparison is a null test rather than a NULL comparison.
fn is_null_test(source: &str, left: &Expression, right: &Expression) -> Option<bool> {
    if !matches!(right, Expression::Literal(Literal::Null(_))) {
        return None;
    }
    let between = source.get(left.span().end..right.span().start)?;
    let mut has_is = false;
    let mut has_not = false;
    for token in between.split_whitespace() {
        match token.to_ascii_uppercase().as_str() {
            "IS" => has_is = true,
            "NOT" => has_not = true,
            _ => {}
        }
    }
    has_is.then_some(has_not)
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
    SqlFrontendError { message: message.into() }
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
