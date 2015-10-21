//! SQL → YYDS KV planner.
//!
//! 本模块负责把 catalog-bound 的 `SELECT` 请求翻译为一份可以直接喂给
//! `KeyValueExecutor` 的执行计划。计划本身不做读/写；它只决定：
//!
//! 1. **namespace** — 目标表的字符串标签，形如 `table:{type_id}`。
//! 2. **key** — 要读取的那一行的主键值（仅当 WHERE 里有 `pk = literal` 才能提取出来）。
//! 3. **projection fields** — 投影需要的字段 ID 列表，`*` 会在此时展开。
//! 4. **remaining predicate** — 从 WHERE 里剥掉主键等值之后剩下的表达式，拿到 row 后再做一次过滤。
//!
//! 目前只支持 **主键精确等值查询**。全表扫描、非主键等值、范围条件都没有对应
//! 的 `KeyValueExecutor` 原语，因此在 planner 层直接拒绝。

use crate::{
    catalog_bind::{
        SqlCatalog, SqlCatalogBoundColumn, SqlCatalogBoundExpression, SqlCatalogBoundProjection,
        SqlCatalogBoundSelect, SqlCatalogBoundStatement,
    },
    sql::{SqlBoundBinaryOperator, SqlBoundLiteral, SqlFrontendError},
};

/// 翻译一份 catalog-bound statement 为可执行计划。
pub fn plan_statement(
    statement: SqlCatalogBoundStatement,
    catalog: &SqlCatalog,
) -> Result<SqlExecutionPlan, SqlFrontendError> {
    match statement {
        SqlCatalogBoundStatement::Select(select) => plan_select(select, catalog),
    }
}

/// 计划一个 SELECT。
pub fn plan_select(select: SqlCatalogBoundSelect, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let source = select.source.as_ref().ok_or_else(|| planner_error("SQL gateway requires a FROM source"))?;
    let table = find_table(catalog, source.type_id)?;
    let namespace = format!("table:{}", source.type_id);
    let pk_field = primary_key_of(table)?;

    // 1. 从投影里收集字段，同时把 Star 展开。
    let mut projection_fields: Vec<(u64, String)> = Vec::new();
    let mut projection_expressions: Vec<(SqlCatalogBoundExpression, String)> = Vec::new();
    for projection in &select.projections {
        match projection {
            SqlCatalogBoundProjection::Star(_) => {
                for field in &table.fields {
                    projection_fields.push((field.field_id, field.name.clone()));
                }
            }
            SqlCatalogBoundProjection::Expression { expression, alias } => {
                if let SqlCatalogBoundExpression::Column(col) = expression {
                    projection_fields.push((col.field_id, alias.clone().unwrap_or_else(|| col.field_name.clone())));
                }
                else {
                    projection_expressions.push((expression.clone(), alias.clone().unwrap_or_else(|| expression_default_label(expression))));
                }
            }
        }
    }

    // 2. 从 predicate 里提取主键等值条件；剩下的作为二次过滤。
    let (pk_literal, remaining_predicate) = match select.predicate {
        None => (None, None),
        Some(expr) => extract_pk_equality(expr, pk_field.field_id)?,
    };

    let rowid = match pk_literal {
        Some(lit) => Some(pk_literal_to_rowid(&lit)?),
        None => {
            return Err(planner_error(&format!(
                "SQL gateway currently requires `{} = <literal>` in WHERE to extract a row key; \
                 range conditions or non-primary-key equality need a scan operator not present in KeyValueExecutor",
                pk_field.name
            )));
        }
    };

    Ok(SqlExecutionPlan {
        namespace,
        rowid: rowid.expect("rowid is Some when pk_literal is Some"),
        projection_fields,
        projection_expressions,
        remaining_predicate,
    })
}

/// 已计划好的 SELECT，可以直接翻译为 KeyValueCommand。
#[derive(Debug, Clone)]
pub struct SqlExecutionPlan {
    /// namespace 标签，形如 `table:{type_id}`。
    pub namespace: String,
    /// 要读取的 row 的主键值 bytes。
    pub rowid: Vec<u8>,
    /// 投影里直接映射到 catalog 字段的那些列：(field_id, display_name)。
    pub projection_fields: Vec<(u64, String)>,
    /// 投影里需要在 row 上做表达式求值的那些：(expression, display_name)。
    pub projection_expressions: Vec<(SqlCatalogBoundExpression, String)>,
    /// 从原 WHERE 里剥掉主键等值后剩下的表达式；`None` 表示没有剩余。
    pub remaining_predicate: Option<SqlCatalogBoundExpression>,
}

fn extract_pk_equality(
    predicate: SqlCatalogBoundExpression,
    pk_field_id: u64,
) -> Result<(Option<SqlBoundLiteral>, Option<SqlCatalogBoundExpression>), SqlFrontendError> {
    match predicate {
        SqlCatalogBoundExpression::Binary {
            left,
            operator: SqlBoundBinaryOperator::Equal,
            right,
        } => {
            let (maybe_col, maybe_literal) = match (&*left, &*right) {
                (SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit)) => (col.clone(), Some(lit.clone())),
                (SqlCatalogBoundExpression::Literal(lit), SqlCatalogBoundExpression::Column(col)) => (col.clone(), Some(lit.clone())),
                _ => (SqlCatalogBoundColumn { type_id: 0, field_id: 0, table_name: String::new(), field_name: String::new() }, None),
            };
            if maybe_col.field_id == pk_field_id {
                if let Some(lit) = maybe_literal {
                    return Ok((Some(lit.clone()), None));
                }
            }
            // 不是 pk 等值，整个 predicate 留在 remaining
            return Ok((None, Some(SqlCatalogBoundExpression::Binary {
                left,
                operator: SqlBoundBinaryOperator::Equal,
                right,
            })));
        }
        // AND 拆成两棵子树分别处理。两边都提出一个 pk 等值 —— 但这种情况已经由 first-match 覆盖，
        // 真正的目标是：如果 AND 的一边提取出了 pk 等值，另一边就是 remaining。
        SqlCatalogBoundExpression::Binary {
            left,
            operator: SqlBoundBinaryOperator::And,
            right,
        } => {
            let (left_pk, left_remaining) = extract_pk_equality(*left, pk_field_id)?;
            let (right_pk, right_remaining) = extract_pk_equality(*right, pk_field_id)?;
            match (left_pk, right_pk) {
                (Some(_), Some(_)) => Err(planner_error("conflicting primary-key equality inside AND")),
                (Some(lit), None) => Ok((Some(lit), left_remaining.or(right_remaining))),
                (None, Some(lit)) => Ok((Some(lit), right_remaining.or(left_remaining))),
                (None, None) => Ok((None, Some(SqlCatalogBoundExpression::Binary {
                    left: Box::new(
                        left_remaining.unwrap_or_else(|| SqlCatalogBoundExpression::Column(SqlCatalogBoundColumn {
                            type_id: 0,
                            field_id: 0,
                            table_name: String::new(),
                            field_name: String::new(),
                        })),
                    ),
                    operator: SqlBoundBinaryOperator::And,
                    right: Box::new(
                        right_remaining.unwrap_or_else(|| SqlCatalogBoundExpression::Column(SqlCatalogBoundColumn {
                            type_id: 0,
                            field_id: 0,
                            table_name: String::new(),
                            field_name: String::new(),
                        })),
                    ),
                }))),
            }
        }
        other => Ok((None, Some(other))),
    }
}

fn pk_literal_to_rowid(literal: &SqlBoundLiteral) -> Result<Vec<u8>, SqlFrontendError> {
    match literal {
        SqlBoundLiteral::Number(text) => Ok(text.as_bytes().to_vec()),
        SqlBoundLiteral::String(text) => Ok(text.as_bytes().to_vec()),
        SqlBoundLiteral::Boolean(value) => Ok((if *value { "1" } else { "0" }).as_bytes().to_vec()),
        SqlBoundLiteral::Null => Err(planner_error("primary key must not be NULL")),
    }
}

fn primary_key_of(table: &crate::SqlCatalogTable) -> Result<&crate::SqlCatalogField, SqlFrontendError> {
    // VOS 约定：第一个字段是主键（通常名为 @@id）。
    table.fields.first().ok_or_else(|| planner_error(&format!("table `{}` has no fields", table.name)))
}

fn find_table(catalog: &SqlCatalog, type_id: u64) -> Result<&crate::SqlCatalogTable, SqlFrontendError> {
    catalog.tables().iter().find(|t| t.type_id == type_id).ok_or_else(|| planner_error("catalog table is missing"))
}

fn expression_default_label(expr: &SqlCatalogBoundExpression) -> String {
    format!("{expr:?}")
}

fn planner_error(message: &str) -> SqlFrontendError {
    SqlFrontendError { message: message.into() }
}
