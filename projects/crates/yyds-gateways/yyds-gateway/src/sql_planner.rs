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
        SqlCatalog, SqlCatalogBoundColumn, SqlCatalogBoundDelete, SqlCatalogBoundExpression, SqlCatalogBoundInsert,
        SqlCatalogBoundProjection, SqlCatalogBoundSelect, SqlCatalogBoundStatement, SqlCatalogBoundUpdate,
    },
    sql::{SqlBoundBinaryOperator, SqlBoundLiteral, SqlFrontendError},
};

/// 翻译一份 catalog-bound statement 为可执行计划。
pub fn plan_statement(statement: SqlCatalogBoundStatement, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    match statement {
        SqlCatalogBoundStatement::Select(select) => plan_select(select, catalog),
        SqlCatalogBoundStatement::Insert(insert) => plan_insert(insert, catalog),
        SqlCatalogBoundStatement::Update(update) => plan_update(update, catalog),
        SqlCatalogBoundStatement::Delete(delete) => plan_delete(delete, catalog),
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
                    projection_expressions
                        .push((expression.clone(), alias.clone().unwrap_or_else(|| expression_default_label(expression))));
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

    Ok(SqlExecutionPlan::Read(ReadPlan {
        namespace,
        rowid: rowid.expect("rowid is Some when pk_literal is Some"),
        projection_fields,
        projection_expressions,
        remaining_predicate,
    }))
}

/// 计划一个 INSERT。要求 VALUES 里第一个值是主键 literal。
pub fn plan_insert(insert: SqlCatalogBoundInsert, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let table = find_table(catalog, insert.table.type_id)?;
    let namespace = format!("table:{}", table.type_id);

    let pk_bound = insert.fields.first().ok_or_else(|| planner_error("INSERT has no fields"))?;
    let pk_value = insert.values.first().ok_or_else(|| planner_error("INSERT has no values"))?;
    let rowid = literal_from_expression(pk_value)?;
    let rowid_bytes = pk_literal_to_rowid(&rowid)?;

    let mut write_fields = Vec::with_capacity(insert.fields.len());
    for (field, value) in insert.fields.iter().zip(insert.values.iter()) {
        let lit = literal_from_expression(value)?;
        write_fields.push(WriteField { field_id: field.field_id, value: lit });
        let _ = pk_bound;
    }
    Ok(SqlExecutionPlan::Insert(WritePlan { namespace, rowid: rowid_bytes, fields: write_fields }))
}

/// 计划一个 UPDATE。要求 WHERE pk = literal。
pub fn plan_update(update: SqlCatalogBoundUpdate, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let table = find_table(catalog, update.table.type_id)?;
    let namespace = format!("table:{}", table.type_id);
    let pk_field = primary_key_of(table)?;

    let rowid = match update.predicate {
        None => return Err(planner_error("UPDATE requires WHERE with `pk = literal`")),
        Some(expr) => {
            let (pk_literal, _) = extract_pk_equality(expr, pk_field.field_id)?;
            pk_literal.ok_or_else(|| planner_error(&format!("UPDATE WHERE must contain `{} = literal`", pk_field.name)))?
        }
    };
    let rowid_bytes = pk_literal_to_rowid(&rowid)?;

    let mut write_fields = Vec::new();
    for (field, value_expr) in &update.assignments {
        let lit = literal_from_expression(value_expr)?;
        write_fields.push(WriteField { field_id: field.field_id, value: lit });
    }
    Ok(SqlExecutionPlan::Update(WritePlan { namespace, rowid: rowid_bytes, fields: write_fields }))
}

/// 计划一个 DELETE。要求 WHERE pk = literal。
pub fn plan_delete(delete: SqlCatalogBoundDelete, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let table = find_table(catalog, delete.table.type_id)?;
    let namespace = format!("table:{}", table.type_id);
    let pk_field = primary_key_of(table)?;

    let rowid = match delete.predicate {
        None => return Err(planner_error("DELETE requires WHERE with `pk = literal`")),
        Some(expr) => {
            let (pk_literal, _) = extract_pk_equality(expr, pk_field.field_id)?;
            pk_literal.ok_or_else(|| planner_error(&format!("DELETE WHERE must contain `{} = literal`", pk_field.name)))?
        }
    };
    let rowid_bytes = pk_literal_to_rowid(&rowid)?;
    Ok(SqlExecutionPlan::Delete(DeletePlan { namespace, rowid: rowid_bytes }))
}

/// 从 bound expression 里提取 literal。目前 INSERT/UPDATE 的值必须是 literal。
fn literal_from_expression(expr: &SqlCatalogBoundExpression) -> Result<SqlBoundLiteral, SqlFrontendError> {
    match expr {
        SqlCatalogBoundExpression::Literal(lit) => Ok(lit.clone()),
        SqlCatalogBoundExpression::Column(col) => Err(planner_error(&format!(
            "SQL gateway write values must be literals; column reference `{}` is not supported in this first step",
            col.field_name
        ))),
        _ => Err(planner_error("SQL gateway write values must be simple literals")),
    }
}

/// 已计划好的 SQL 操作，可以直接翻译为 KeyValueCommand。
#[derive(Debug, Clone)]
pub enum SqlExecutionPlan {
    /// SELECT — namespace + rowid + 投影 + 剩余过滤。
    Read(ReadPlan),
    /// INSERT — PutIfAbsent（pk 必须由 VALUES 提供）。
    Insert(WritePlan),
    /// UPDATE — 先 Get + 再 Put（WHERE pk=literal 必须存在）。
    Update(WritePlan),
    /// DELETE — Delete（WHERE pk=literal 必须存在）。
    Delete(DeletePlan),
}

/// SELECT 执行计划。
#[derive(Debug, Clone)]
pub struct ReadPlan {
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

/// INSERT / UPDATE 执行计划。payload 由 execute 层做 TLV 编码。
#[derive(Debug, Clone)]
pub struct WritePlan {
    /// namespace 标签。
    pub namespace: String,
    /// 要写入的 row 的主键值 bytes。
    pub rowid: Vec<u8>,
    /// INSERT 时提供所有字段值；UPDATE 时提供要 SET 的字段。
    pub fields: Vec<WriteField>,
}

/// 一个要写入的字段。
#[derive(Debug, Clone)]
pub struct WriteField {
    /// field_id。
    pub field_id: u64,
    /// 要写入的值（必须是 Literal，复杂表达式暂不支持）。
    pub value: SqlBoundLiteral,
}

/// DELETE 执行计划。
#[derive(Debug, Clone)]
pub struct DeletePlan {
    pub namespace: String,
    pub rowid: Vec<u8>,
}

fn extract_pk_equality(
    predicate: SqlCatalogBoundExpression,
    pk_field_id: u64,
) -> Result<(Option<SqlBoundLiteral>, Option<SqlCatalogBoundExpression>), SqlFrontendError> {
    match predicate {
        SqlCatalogBoundExpression::Binary { left, operator: SqlBoundBinaryOperator::Equal, right } => {
            let (maybe_col, maybe_literal) = match (&*left, &*right) {
                (SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit)) => {
                    (col.clone(), Some(lit.clone()))
                }
                (SqlCatalogBoundExpression::Literal(lit), SqlCatalogBoundExpression::Column(col)) => {
                    (col.clone(), Some(lit.clone()))
                }
                _ => (
                    SqlCatalogBoundColumn { type_id: 0, field_id: 0, table_name: String::new(), field_name: String::new() },
                    None,
                ),
            };
            if maybe_col.field_id == pk_field_id {
                if let Some(lit) = maybe_literal {
                    return Ok((Some(lit.clone()), None));
                }
            }
            // 不是 pk 等值，整个 predicate 留在 remaining
            return Ok((
                None,
                Some(SqlCatalogBoundExpression::Binary { left, operator: SqlBoundBinaryOperator::Equal, right }),
            ));
        }
        // AND 拆成两棵子树分别处理。两边都提出一个 pk 等值 —— 但这种情况已经由 first-match 覆盖，
        // 真正的目标是：如果 AND 的一边提取出了 pk 等值，另一边就是 remaining。
        SqlCatalogBoundExpression::Binary { left, operator: SqlBoundBinaryOperator::And, right } => {
            let (left_pk, left_remaining) = extract_pk_equality(*left, pk_field_id)?;
            let (right_pk, right_remaining) = extract_pk_equality(*right, pk_field_id)?;
            match (left_pk, right_pk) {
                (Some(_), Some(_)) => Err(planner_error("conflicting primary-key equality inside AND")),
                (Some(lit), None) => Ok((Some(lit), left_remaining.or(right_remaining))),
                (None, Some(lit)) => Ok((Some(lit), right_remaining.or(left_remaining))),
                (None, None) => Ok((
                    None,
                    Some(SqlCatalogBoundExpression::Binary {
                        left: Box::new(left_remaining.unwrap_or_else(|| {
                            SqlCatalogBoundExpression::Column(SqlCatalogBoundColumn {
                                type_id: 0,
                                field_id: 0,
                                table_name: String::new(),
                                field_name: String::new(),
                            })
                        })),
                        operator: SqlBoundBinaryOperator::And,
                        right: Box::new(right_remaining.unwrap_or_else(|| {
                            SqlCatalogBoundExpression::Column(SqlCatalogBoundColumn {
                                type_id: 0,
                                field_id: 0,
                                table_name: String::new(),
                                field_name: String::new(),
                            })
                        })),
                    }),
                )),
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
