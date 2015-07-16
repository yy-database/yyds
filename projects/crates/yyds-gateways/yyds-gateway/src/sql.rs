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

/// Parses one SQL surface statement through the official Oak SQL frontend.
pub fn parse_sql(source: &str) -> Result<oak_sql::ast::SqlRoot, SqlFrontendError> {
    oak_sql::parse(source).map_err(|message| SqlFrontendError { message })
}

/// Gateway-owned statement after Oak parsing and SQL-surface binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlBoundStatement {
    /// One SQL SELECT request awaiting YYDS catalog binding.
    Select(SqlBoundSelect),
}

/// SQL SELECT request owned by the compatibility gateway.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlBoundSelect {
    /// Optional source table name as written by the client.
    pub source: Option<String>,
    /// Projection expressions in source order.
    pub projections: Vec<SqlBoundProjection>,
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
}

/// Binds the supported SQL compatibility subset without executing it.
pub fn bind_sql(source: &str) -> Result<SqlBoundStatement, SqlFrontendError> {
    let root = parse_sql(source)?;
    if root.statements.len() != 1 {
        return Err(bind_error("SQL gateway accepts exactly one statement"));
    }
    match &root.statements[0] {
        SqlStatement::Select(select) => {
            if !select.joins.is_empty()
                || select.group_by.is_some()
                || select.having.is_some()
                || select.order_by.is_some()
                || select.limit.is_some()
            {
                return Err(bind_error("SQL SELECT clauses are not in the gateway binding subset"));
            }
            let projections = select.items.iter().map(bind_projection).collect::<Result<Vec<_>, _>>()?;
            Ok(SqlBoundStatement::Select(SqlBoundSelect {
                source: select.from.as_ref().map(|table| normalize_identifier(table.name.name.as_ref())),
                projections,
                predicate: select.expr.as_ref().map(bind_expression).transpose()?,
            }))
        }
        _ => Err(bind_error("SQL gateway binding currently accepts SELECT only")),
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
        Expression::Literal(literal) => Ok(SqlBoundExpression::Literal(match literal {
            Literal::Number(value, _) => SqlBoundLiteral::Number(value.to_string()),
            Literal::String(value, _) => SqlBoundLiteral::String(value.to_string()),
            Literal::Boolean(value, _) => SqlBoundLiteral::Boolean(*value),
            Literal::Null(_) => SqlBoundLiteral::Null,
        })),
        Expression::Binary { left, op, right, .. } => Ok(SqlBoundExpression::Binary {
            left: Box::new(bind_expression(left)?),
            operator: bind_binary_operator(*op)?,
            right: Box::new(bind_expression(right)?),
        }),
        Expression::Unary { op, expr, .. } => Ok(SqlBoundExpression::Unary {
            operator: match op {
                oak_sql::ast::UnaryOperator::Plus => SqlBoundUnaryOperator::Plus,
                oak_sql::ast::UnaryOperator::Minus => SqlBoundUnaryOperator::Minus,
                oak_sql::ast::UnaryOperator::Not => SqlBoundUnaryOperator::Not,
            },
            expression: Box::new(bind_expression(expr)?),
        }),
        _ => Err(bind_error("SQL expression is outside the gateway binding subset")),
    }
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

fn normalize_identifier(name: &str) -> String {
    name.trim().to_string()
}
