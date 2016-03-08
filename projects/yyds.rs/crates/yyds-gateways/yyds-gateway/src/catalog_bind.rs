//! SQL compatibility binding against a published YYDS VOS catalog.

use crate::sql::{
    bind_sql, SqlBoundAssignment, SqlBoundBinaryOperator, SqlBoundDelete, SqlBoundExpression, SqlBoundInsert, SqlBoundJoinKind,
    SqlBoundLiteral, SqlBoundOrderDirection, SqlBoundProjection, SqlBoundSelect, SqlBoundStatement, SqlBoundUnaryOperator,
    SqlBoundUpdate, SqlErrorKind, SqlFrontendError, SqlNullsOrder,
};
use vos::types::{CanonicalScalar, CanonicalType, ResolvedCanonicalType};

/// Scalar types the gateway can project from a VOS field contract.
///
/// The gateway does not reproduce the full VOS type algebra — it keeps the
/// information needed to decode row bytes and validate SQL literals. Fields
/// whose type does not map cleanly fall back to [`SqlFieldType::Unknown`] and
/// leave interpretation to the runtime.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlFieldType {
    /// 64-bit signed integer (`i64` / `int64`).
    Int64,
    /// 32-bit signed integer (`i32` / `int32`).
    Int32,
    /// 64-bit floating point (`f64` / `float64`).
    Float64,
    /// UTF-8 string (`utf8`).
    String,
    /// Boolean (`bool`).
    Bool,
    /// Raw opaque bytes (`bytes`).
    Bytes,
    /// Optional wrapper — inner type + NULL. Box breaks the recursion
    Optional(Box<SqlFieldType>),
    /// Anything the gateway does not recognise stays opaque.
    Unknown,
}

impl SqlFieldType {
    /// Extract the innermost non-optional type.
    pub fn inner(&self) -> SqlFieldType {
        match self {
            SqlFieldType::Optional(inner) => inner.inner(),
            other => other.clone(),
        }
    }

    /// Whether NULL is a legal value for this field.
    pub fn is_nullable(&self) -> bool {
        matches!(self, SqlFieldType::Optional(_))
    }
}

/// Map a resolved VOS type to a gateway-facing [`SqlFieldType`].
fn project_resolved_type(ty: &ResolvedCanonicalType) -> SqlFieldType {
    match ty {
        ResolvedCanonicalType::Scalar(scalar) => match scalar {
            CanonicalScalar::I8 | CanonicalScalar::I16 | CanonicalScalar::I32 => SqlFieldType::Int32,
            CanonicalScalar::I64 | CanonicalScalar::U8 | CanonicalScalar::U16 | CanonicalScalar::U32 => SqlFieldType::Int64,
            CanonicalScalar::F32 | CanonicalScalar::F64 => SqlFieldType::Float64,
            CanonicalScalar::Utf8 | CanonicalScalar::Utf16 => SqlFieldType::String,
            CanonicalScalar::Bool => SqlFieldType::Bool,
            CanonicalScalar::Bytes => SqlFieldType::Bytes,
            CanonicalScalar::Uuid | CanonicalScalar::Date | CanonicalScalar::Time | CanonicalScalar::DateTime => {
                SqlFieldType::String
            }
            _ => SqlFieldType::Unknown,
        },
        ResolvedCanonicalType::Optional(inner) => SqlFieldType::Optional(Box::new(project_resolved_type(inner))),
        _ => SqlFieldType::Unknown,
    }
}

/// Map a schema-projection VOS type to a gateway-facing [`SqlFieldType`].
fn project_projection_type(ty: &CanonicalType) -> SqlFieldType {
    match ty {
        CanonicalType::Named(path) => match path.iter().map(|s| s.as_str()).collect::<Vec<_>>().as_slice() {
            ["i64"] | ["int64"] | ["Int64"] => SqlFieldType::Int64,
            ["i32"] | ["int32"] | ["Int32"] => SqlFieldType::Int32,
            ["f64"] | ["float64"] | ["Float64"] => SqlFieldType::Float64,
            ["utf8"] | ["String"] | ["string"] => SqlFieldType::String,
            ["bool"] | ["Bool"] => SqlFieldType::Bool,
            ["bytes"] | ["Bytes"] => SqlFieldType::Bytes,
            _ => SqlFieldType::Unknown,
        },
        CanonicalType::Optional(inner) => SqlFieldType::Optional(Box::new(project_projection_type(inner))),
        _ => SqlFieldType::Unknown,
    }
}

/// A table identity exposed by the YYDS catalog to a gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogTable {
    /// Stable YYDS-owned type identity.
    pub type_id: u64,
    /// VOS table name used for SQL lookup.
    pub name: String,
    /// Columns in catalog order.
    pub fields: Vec<SqlCatalogField>,
}

/// A column identity exposed by the YYDS catalog to a gateway binder.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogField {
    /// Stable YYDS-owned field identity.
    pub field_id: u64,
    /// VOS field name used for SQL lookup.
    pub name: String,
    /// Declared field type projected from the VOS contract.
    pub field_type: SqlFieldType,
}

/// The gateway's immutable view of one published VOS catalog revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalog {
    schema_version: u32,
    tables: Vec<SqlCatalogTable>,
}

impl SqlCatalog {
    /// Builds a gateway view from the newest contract published by YYDS.
    pub fn from_yyds_catalog(catalog: &yyds_catalog::Catalog) -> Result<Self, SqlFrontendError> {
        let schema = catalog.schema().ok_or_else(|| catalog_error("YYDS catalog has no published schema"))?;
        let contract = catalog.resolved_contract().ok_or_else(|| catalog_error("YYDS catalog has no resolved contract"))?;
        Self::from_resolved_contract(schema.version, contract)
    }

    /// Projects gateway table identities from a validated resolved contract.
    pub fn from_resolved_contract(
        schema_version: u32,
        contract: &vos::types::ResolvedContract,
    ) -> Result<Self, SqlFrontendError> {
        if schema_version == 0 {
            return Err(catalog_error("catalog schema version must be nonzero"));
        }
        let artifact = contract.to_json().map_err(|_| catalog_error("resolved contract encode failed"))?;
        vos::types::ResolvedContract::from_json(&artifact)
            .map_err(|error| catalog_error(&format!("invalid resolved VOS contract {}: {}", error.code, error.message)))?;
        let mut type_ids = std::collections::BTreeSet::new();
        let mut field_ids = std::collections::BTreeSet::new();
        let tables = contract
            .types
            .iter()
            .filter(|entry| entry.kind == vos::types::TypeContractKind::Table)
            .map(|entry| {
                if entry.type_id == 0 || !type_ids.insert(entry.type_id) {
                    return Err(catalog_error("invalid or duplicate catalog type identity"));
                }
                let name =
                    entry.canonical_path.last().cloned().ok_or_else(|| catalog_error("catalog table has no canonical name"))?;
                let fields = entry
                    .fields
                    .iter()
                    .map(|field| {
                        if field.field_id == 0 || !field_ids.insert(field.field_id) {
                            return Err(catalog_error("invalid or duplicate catalog field identity"));
                        }
                        Ok(SqlCatalogField {
                            field_id: field.field_id,
                            name: field.canonical_name.clone(),
                            field_type: project_resolved_type(&field.canonical_type),
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(SqlCatalogTable { type_id: entry.type_id, name, fields })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { schema_version, tables })
    }

    /// Returns the catalog schema revision used by this binding view.
    pub fn schema_version(&self) -> u32 {
        self.schema_version
    }

    /// Returns the catalog tables in VOS declaration order.
    pub fn tables(&self) -> &[SqlCatalogTable] {
        &self.tables
    }
}

/// A table reference after SQL names are bound to YYDS catalog identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundTable {
    /// Stable YYDS table identity.
    pub type_id: u64,
    /// Catalog table name.
    pub name: String,
}

/// A column reference after SQL names are bound to YYDS catalog identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundColumn {
    /// Stable YYDS table identity.
    pub type_id: u64,
    /// Stable YYDS field identity.
    pub field_id: u64,
    /// Catalog table name.
    pub table_name: String,
    /// Catalog field name.
    pub field_name: String,
}

/// SQL expression after catalog binding and before distributed planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlCatalogBoundExpression {
    /// A catalog-resolved column reference.
    Column(SqlCatalogBoundColumn),
    /// A SQL literal retaining gateway semantics.
    Literal(SqlBoundLiteral),
    /// A bare `*` argument; only `COUNT(*)` gives it meaning.
    Wildcard,
    /// A SQL function call (currently only aggregate functions).
    Function {
        /// Canonical lower-case function name.
        name: String,
        /// Catalog-bound arguments.
        args: Vec<SqlCatalogBoundExpression>,
        /// Whether the aggregate uses `DISTINCT` quantifier.
        distinct: bool,
    },
    /// A binary SQL expression.
    Binary {
        /// Left operand.
        left: Box<Self>,
        /// Operator.
        operator: SqlBoundBinaryOperator,
        /// Right operand.
        right: Box<Self>,
    },
    /// A unary SQL expression.
    Unary {
        /// Operator.
        operator: SqlBoundUnaryOperator,
        /// Operand.
        expression: Box<Self>,
    },
    /// `expr IN (<subquery>)` with the subquery already planned.
    InSubquery {
        /// Left-hand side expression.
        expression: Box<Self>,
        /// Executable plan for the subquery.
        plan: Box<crate::sql_planner::SqlExecutionPlan>,
        /// Whether the predicate is spelled `NOT IN`.
        negated: bool,
    },
    /// `IN` predicate whose subquery rows were materialized before evaluation.
    InValues {
        /// Left-hand side expression.
        expression: Box<Self>,
        /// Literal values produced by the subquery.
        values: Vec<SqlBoundLiteral>,
        /// Whether the predicate is spelled `NOT IN`.
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

/// SQL projection after catalog binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlCatalogBoundProjection {
    /// All columns of the selected catalog table.
    Star(SqlCatalogBoundTable),
    /// A resolved expression with an optional client alias.
    Expression {
        /// Resolved expression.
        expression: SqlCatalogBoundExpression,
        /// Optional SQL alias.
        alias: Option<String>,
    },
}

/// SQL SELECT request after catalog binding and before YYDS distributed planning.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundSelect {
    /// Selected table identity, if the query has a source.
    pub source: Option<SqlCatalogBoundTable>,
    /// JOIN clauses in source order, each bound to its own catalog table.
    pub joins: Vec<SqlCatalogBoundJoin>,
    /// Resolved projections.
    pub projections: Vec<SqlCatalogBoundProjection>,
    /// Resolved optional predicate.
    pub predicate: Option<SqlCatalogBoundExpression>,
    /// Resolved optional GROUP BY expressions.
    pub group_by: Option<Vec<SqlCatalogBoundExpression>>,
    /// Resolved optional HAVING expression.
    pub having: Option<SqlCatalogBoundExpression>,
    /// Optional LIMIT count, propagated from the gateway binder.
    pub limit: Option<usize>,
    /// Optional OFFSET count, propagated from the gateway binder.
    pub offset: Option<usize>,
    /// Optional ORDER BY items, propagated from the gateway binder.
    pub order_by: Option<Vec<SqlCatalogBoundOrderByItem>>,
    /// Whether the projection list carries the `DISTINCT` modifier.
    pub distinct: bool,
}

/// One ORDER BY item after SQL names are bound to YYDS catalog identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundOrderByItem {
    /// Ordering expression.
    pub expression: SqlCatalogBoundExpression,
    /// Sort direction.
    pub direction: SqlBoundOrderDirection,
    /// Explicit `NULLS FIRST` / `NULLS LAST`, when written.
    pub nulls: Option<SqlNullsOrder>,
}

/// One JOIN clause after SQL names are bound to YYDS catalog identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundJoin {
    /// Join kind retained from the SQL surface.
    pub kind: SqlBoundJoinKind,
    /// Joined table identity.
    pub table: SqlCatalogBoundTable,
    /// Join condition bound against the tables visible at this point.
    pub on: SqlCatalogBoundExpression,
}

/// SQL INSERT request after catalog binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundInsert {
    /// Target table identity.
    pub table: SqlCatalogBoundTable,
    /// Catalog fields in the order VALUES supplies them.
    pub fields: Vec<SqlCatalogBoundColumn>,
    /// Bound value expressions grouped by row. Every inner vec has length `fields.len()`.
    pub rows: Vec<Vec<SqlCatalogBoundExpression>>,
}

/// SQL UPDATE request after catalog binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundUpdate {
    /// Target table identity.
    pub table: SqlCatalogBoundTable,
    /// Catalog field → bound value assignments in source order.
    pub assignments: Vec<(SqlCatalogBoundColumn, SqlCatalogBoundExpression)>,
    /// Resolved optional predicate.
    pub predicate: Option<SqlCatalogBoundExpression>,
}

/// SQL DELETE request after catalog binding.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlCatalogBoundDelete {
    /// Target table identity.
    pub table: SqlCatalogBoundTable,
    /// Resolved optional predicate.
    pub predicate: Option<SqlCatalogBoundExpression>,
}

/// Gateway-owned SQL request after names are bound to a YYDS catalog revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlCatalogBoundStatement {
    /// One catalog-bound SELECT request awaiting distributed planning.
    Select(SqlCatalogBoundSelect),
    /// One catalog-bound INSERT request awaiting planning.
    Insert(SqlCatalogBoundInsert),
    /// One catalog-bound UPDATE request awaiting planning.
    Update(SqlCatalogBoundUpdate),
    /// One catalog-bound DELETE request awaiting planning.
    Delete(SqlCatalogBoundDelete),
}

/// Binds a parsed SQL surface to a specific catalog revision.
///
/// `@@`-prefixed references are rejected — use
/// [`bind_sql_catalog_with_variables`] on gateways that emulate a variable table.
pub fn bind_sql_catalog(
    source: &str,
    catalog: &SqlCatalog,
    expected_schema_version: u32,
) -> Result<SqlCatalogBoundStatement, SqlFrontendError> {
    bind_sql_catalog_with_variables(source, catalog, expected_schema_version, &reject_system_variable)
}

/// Binds a parsed SQL surface against a catalog revision, resolving system variables.
///
/// `variables` receives the whole `@@[scope.]name` spelling written by the
/// client and returns the literal the variable evaluates to. Dialects without
/// system variables keep using [`bind_sql_catalog`], which always rejects them.
pub fn bind_sql_catalog_with_variables(
    source: &str,
    catalog: &SqlCatalog,
    expected_schema_version: u32,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundStatement, SqlFrontendError> {
    if catalog.schema_version != expected_schema_version {
        return Err(catalog_error(&format!(
            "SQL catalog schema version mismatch: expected {expected_schema_version}, found {}",
            catalog.schema_version
        )));
    }
    let statement = bind_sql(source)?;
    match statement {
        SqlBoundStatement::Select(select) => bind_select(select, catalog, variables).map(SqlCatalogBoundStatement::Select),
        SqlBoundStatement::Insert(insert) => bind_insert(insert, catalog, variables).map(SqlCatalogBoundStatement::Insert),
        SqlBoundStatement::Update(update) => bind_update(update, catalog, variables).map(SqlCatalogBoundStatement::Update),
        SqlBoundStatement::Delete(delete) => bind_delete(delete, catalog, variables).map(SqlCatalogBoundStatement::Delete),
    }
}

/// Default resolver for dialects without `@@` system variables.
fn reject_system_variable(name: &str) -> Result<SqlBoundLiteral, SqlFrontendError> {
    Err(SqlFrontendError::new(
        SqlErrorKind::UnknownVariable,
        format!("SQL system variable {name} is not supported by this gateway"),
    ))
}

fn bind_insert(
    insert: SqlBoundInsert,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundInsert, SqlFrontendError> {
    let table = find_table(catalog, &insert.table)?;
    let source = [table.clone()];
    let bound_fields = match insert.columns {
        Some(cols) => cols.iter().map(|name| find_field(name, &source, catalog)).collect::<Result<Vec<_>, _>>()?,
        None => {
            // INSERT INTO t VALUES (...) 无列名 → 按 catalog 字段顺序
            let cat = catalog
                .tables
                .iter()
                .find(|c| c.type_id == table.type_id)
                .ok_or_else(|| catalog_error("catalog table identity is not published"))?;
            cat.fields
                .iter()
                .map(|f| SqlCatalogBoundColumn {
                    type_id: table.type_id,
                    field_id: f.field_id,
                    table_name: table.name.clone(),
                    field_name: f.name.clone(),
                })
                .collect()
        }
    };
    let field_count = bound_fields.len();
    if field_count == 0 {
        return Err(SqlFrontendError::syntax("INSERT has no target columns".to_string()));
    }
    let flat: Vec<SqlBoundExpression> = insert.rows.into_iter().flatten().collect();
    if flat.len() % field_count != 0 {
        return Err(SqlFrontendError::syntax(format!(
            "INSERT column/value count mismatch: {} columns, {} values (not divisible)",
            field_count,
            flat.len()
        )));
    }
    let row_count = flat.len() / field_count;
    if row_count == 0 {
        return Err(SqlFrontendError::syntax("INSERT has no VALUES".to_string()));
    }
    let mut rows = Vec::with_capacity(row_count);
    for chunk in flat.chunks(field_count) {
        let bound_row = chunk
            .iter()
            .map(|v| bind_expression(v.clone(), &source, catalog, variables))
            .collect::<Result<Vec<_>, _>>()?;
        rows.push(bound_row);
    }
    Ok(SqlCatalogBoundInsert { table, fields: bound_fields, rows })
}

fn bind_update(
    update: SqlBoundUpdate,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundUpdate, SqlFrontendError> {
    let table = find_table(catalog, &update.table)?;
    let source = [table.clone()];
    let assignments = update
        .assignments
        .into_iter()
        .map(|SqlBoundAssignment { column, value }| {
            let field = find_field(&column, &source, catalog)?;
            let bound_value = bind_expression(value, &source, catalog, variables)?;
            Ok((field, bound_value))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let predicate = update.predicate.map(|p| bind_expression(p, &source, catalog, variables)).transpose()?;
    Ok(SqlCatalogBoundUpdate { table, assignments, predicate })
}

fn bind_delete(
    delete: SqlBoundDelete,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundDelete, SqlFrontendError> {
    let table = find_table(catalog, &delete.table)?;
    let source = [table.clone()];
    let predicate = delete.predicate.map(|p| bind_expression(p, &source, catalog, variables)).transpose()?;
    Ok(SqlCatalogBoundDelete { table, predicate })
}

fn bind_select(
    select: SqlBoundSelect,
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundSelect, SqlFrontendError> {
    let source = select.source.as_deref().map(|name| find_table(catalog, name)).transpose()?;
    // JOIN 之后列名作用域包含基表与所有已连接表，ON 条件只能引用此前已出现的表。
    let mut sources: Vec<SqlCatalogBoundTable> = source.iter().cloned().collect();
    let mut joins: Vec<SqlCatalogBoundJoin> = Vec::with_capacity(select.joins.len());
    for join in select.joins {
        match join.kind {
            SqlBoundJoinKind::Inner | SqlBoundJoinKind::Left => {}
            SqlBoundJoinKind::Right => {
                return Err(SqlFrontendError::unsupported("SQL gateway does not support RIGHT JOIN yet"));
            }
            SqlBoundJoinKind::Full => {
                return Err(SqlFrontendError::unsupported("SQL gateway does not support FULL JOIN yet"));
            }
        }
        let table = find_table(catalog, &join.table)?;
        if sources.iter().any(|visible| visible.type_id == table.type_id) {
            return Err(SqlFrontendError::unsupported(
                "SQL self join requires table aliases, which the gateway does not support",
            ));
        }
        sources.push(table.clone());
        let on = bind_expression(join.on, &sources, catalog, variables)?;
        joins.push(SqlCatalogBoundJoin { kind: join.kind, table, on });
    }
    let mut projections: Vec<SqlCatalogBoundProjection> = Vec::new();
    for projection in select.projections {
        projections.extend(bind_projection(projection, &sources, catalog, variables)?);
    }
    let predicate = select.predicate.map(|expression| bind_expression(expression, &sources, catalog, variables)).transpose()?;
    let group_by = select
        .group_by
        .map(|exprs| exprs.into_iter().map(|e| bind_expression(e, &sources, catalog, variables)).collect::<Result<Vec<_>, _>>())
        .transpose()?;
    let having = select.having.map(|e| bind_expression(e, &sources, catalog, variables)).transpose()?;
    let order_by = match select.order_by {
        Some(items) => {
            let bound = items
                .into_iter()
                .map(|item| {
                    Ok(SqlCatalogBoundOrderByItem {
                        expression: bind_expression(item.expr, &sources, catalog, variables)?,
                        direction: item.direction,
                        nulls: item.nulls,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Some(bound)
        }
        None => None,
    };
    Ok(SqlCatalogBoundSelect {
        source,
        joins,
        projections,
        predicate,
        group_by,
        having,
        limit: select.limit,
        offset: select.offset,
        order_by,
        distinct: select.distinct,
    })
}

/// Binds one SQL projection into zero or more catalog projections (`*` expands one
/// entry per visible table so joined queries project both sides in source order).
fn bind_projection(
    projection: SqlBoundProjection,
    sources: &[SqlCatalogBoundTable],
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<Vec<SqlCatalogBoundProjection>, SqlFrontendError> {
    match projection {
        SqlBoundProjection::Star => {
            if sources.is_empty() {
                return Err(SqlFrontendError::unsupported("SQL star projection requires a catalog table source"));
            }
            Ok(sources.iter().cloned().map(SqlCatalogBoundProjection::Star).collect())
        }
        SqlBoundProjection::Expression { expression, alias } => Ok(vec![SqlCatalogBoundProjection::Expression {
            expression: bind_expression(expression, sources, catalog, variables)?,
            alias,
        }]),
    }
}

fn bind_expression(
    expression: SqlBoundExpression,
    sources: &[SqlCatalogBoundTable],
    catalog: &SqlCatalog,
    variables: &dyn Fn(&str) -> Result<SqlBoundLiteral, SqlFrontendError>,
) -> Result<SqlCatalogBoundExpression, SqlFrontendError> {
    match expression {
        SqlBoundExpression::Identifier(name) => {
            // `@@` 前缀属于会话变量而不是 catalog 列名，必须先交给方言解析器。
            if name.starts_with("@@") {
                return variables(&name).map(SqlCatalogBoundExpression::Literal);
            }
            let field = find_field(&name, sources, catalog)?;
            Ok(SqlCatalogBoundExpression::Column(field))
        }
        SqlBoundExpression::Literal(literal) => Ok(SqlCatalogBoundExpression::Literal(literal)),
        SqlBoundExpression::Wildcard => Ok(SqlCatalogBoundExpression::Wildcard),
        SqlBoundExpression::Binary { left, operator, right } => Ok(SqlCatalogBoundExpression::Binary {
            left: Box::new(bind_expression(*left, sources, catalog, variables)?),
            operator,
            right: Box::new(bind_expression(*right, sources, catalog, variables)?),
        }),
        SqlBoundExpression::Unary { operator, expression } => Ok(SqlCatalogBoundExpression::Unary {
            operator,
            expression: Box::new(bind_expression(*expression, sources, catalog, variables)?),
        }),
        SqlBoundExpression::Function { name, args, distinct } => {
            let bound_args =
                args.into_iter().map(|arg| bind_expression(arg, sources, catalog, variables)).collect::<Result<Vec<_>, _>>()?;
            Ok(SqlCatalogBoundExpression::Function { name, args: bound_args, distinct })
        }
        SqlBoundExpression::InSubquery { expression, select, negated } => {
            // 子查询自带 FROM 作用域，不继承外层 sources，因此不支持相关子查询。
            let bound_select = bind_select(*select, catalog, variables)?;
            let plan = crate::sql_planner::plan_select(bound_select, catalog)?;
            Ok(SqlCatalogBoundExpression::InSubquery {
                expression: Box::new(bind_expression(*expression, sources, catalog, variables)?),
                plan: Box::new(plan),
                negated,
            })
        }
        SqlBoundExpression::Case { branches, else_branch } => {
            let bound_branches = branches
                .into_iter()
                .map(|(when, then)| {
                    let w = bind_expression(*when, sources, catalog, variables)?;
                    let t = bind_expression(*then, sources, catalog, variables)?;
                    Ok((Box::new(w), Box::new(t)))
                })
                .collect::<Result<Vec<_>, _>>()?;
            let bound_else = match else_branch {
                Some(els) => Some(Box::new(bind_expression(*els, sources, catalog, variables)?)),
                None => None,
            };
            Ok(SqlCatalogBoundExpression::Case { branches: bound_branches, else_branch: bound_else })
        }
        SqlBoundExpression::Cast { expr, target } => {
            Ok(SqlCatalogBoundExpression::Cast {
                expr: Box::new(bind_expression(*expr, sources, catalog, variables)?),
                target,
            })
        }
    }
}

fn find_table(catalog: &SqlCatalog, name: &str) -> Result<SqlCatalogBoundTable, SqlFrontendError> {
    let matches = catalog.tables.iter().filter(|table| table.name == name).collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Err(SqlFrontendError::new(SqlErrorKind::UnknownTable, format!("unknown SQL table: {name}"))),
        [table] => Ok(SqlCatalogBoundTable { type_id: table.type_id, name: table.name.clone() }),
        _ => Err(SqlFrontendError::new(SqlErrorKind::AmbiguousTable, format!("ambiguous SQL table: {name}"))),
    }
}

/// Resolves a column reference against every table visible at the current scope.
///
/// `table.column` spellings resolve inside the named table. Unqualified names must
/// be unique across the visible tables — an ambiguity is reported instead of
/// guessing.
fn find_field(
    name: &str,
    sources: &[SqlCatalogBoundTable],
    catalog: &SqlCatalog,
) -> Result<SqlCatalogBoundColumn, SqlFrontendError> {
    if sources.is_empty() {
        return Err(SqlFrontendError::new(SqlErrorKind::UnknownColumn, "SQL column reference requires a table source"));
    }
    if let Some((qualifier, column)) = name.split_once('.') {
        let qualifier = qualifier.trim();
        let column = column.trim();
        let source = sources.iter().find(|source| source.name == qualifier).ok_or_else(|| {
            SqlFrontendError::new(SqlErrorKind::UnknownTableQualifier, format!("unknown SQL table qualifier: {qualifier}"))
        })?;
        return find_field_in_table(column, source, catalog)?.ok_or_else(|| {
            SqlFrontendError::new(SqlErrorKind::UnknownColumn, format!("unknown SQL column: {qualifier}.{column}"))
        });
    }
    let mut matches = Vec::new();
    for source in sources {
        if let Some(column) = find_field_in_table(name, source, catalog)? {
            matches.push(column);
        }
    }
    match matches.len() {
        0 => Err(SqlFrontendError::new(SqlErrorKind::UnknownColumn, format!("unknown SQL column: {name}"))),
        1 => Ok(matches.remove(0)),
        _ => Err(SqlFrontendError::new(
            SqlErrorKind::AmbiguousColumn,
            format!("ambiguous SQL column: {name} (qualify it with a table name)"),
        )),
    }
}

/// Looks one column up inside a single catalog table.
fn find_field_in_table(
    name: &str,
    source: &SqlCatalogBoundTable,
    catalog: &SqlCatalog,
) -> Result<Option<SqlCatalogBoundColumn>, SqlFrontendError> {
    let table = catalog
        .tables
        .iter()
        .find(|candidate| candidate.type_id == source.type_id)
        .ok_or_else(|| catalog_error("catalog table identity is not published"))?;
    let matches = table.fields.iter().filter(|field| field.name == name).collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Ok(None),
        [field] => Ok(Some(SqlCatalogBoundColumn {
            type_id: table.type_id,
            field_id: field.field_id,
            table_name: table.name.clone(),
            field_name: field.name.clone(),
        })),
        _ => Err(SqlFrontendError::new(SqlErrorKind::AmbiguousColumn, format!("ambiguous SQL column: {name}"))),
    }
}

/// Recursively collects the `field_id` of every column reference in an expression,
/// preserving first-appearance order and skipping duplicates.
pub(crate) fn collect_column_ids(expr: &SqlCatalogBoundExpression, out: &mut Vec<u64>) {
    match expr {
        SqlCatalogBoundExpression::Column(col) => {
            if !out.contains(&col.field_id) {
                out.push(col.field_id);
            }
        }
        SqlCatalogBoundExpression::Function { args, .. } => {
            for arg in args {
                collect_column_ids(arg, out);
            }
        }
        SqlCatalogBoundExpression::Binary { left, right, .. } => {
            collect_column_ids(left, out);
            collect_column_ids(right, out);
        }
        SqlCatalogBoundExpression::Unary { expression, .. } => collect_column_ids(expression, out),
        SqlCatalogBoundExpression::InSubquery { expression, .. } | SqlCatalogBoundExpression::InValues { expression, .. } => {
            collect_column_ids(expression, out)
        }
        SqlCatalogBoundExpression::Case { branches, else_branch } => {
            for (w, t) in branches {
                collect_column_ids(w, out);
                collect_column_ids(t, out);
            }
            if let Some(els) = else_branch {
                collect_column_ids(els, out);
            }
        }
        SqlCatalogBoundExpression::Cast { expr, .. } => collect_column_ids(expr, out),
        SqlCatalogBoundExpression::Literal(_) | SqlCatalogBoundExpression::Wildcard => {}
    }
}

fn catalog_error(message: &str) -> SqlFrontendError {
    SqlFrontendError::catalog(message)
}
