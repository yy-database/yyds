//! SQL → YYDS KV planner.
//!
//! 本模块负责把 catalog-bound 的 `SELECT` 请求翻译为一份可以直接喂给
//! `KeyValueExecutor` 的执行计划。计划本身不做读/写；它只决定：
//!
//! 1. **namespace** — 目标表的字符串标签，形如 `table:{type_id}`。
//! 2. **rowid / rowid_prefix / rowid_after / rowid_before** — 主键查询参数
//!    （WHERE 里 `pk = literal` / `pk > X` / `pk LIKE 'prefix%'` 等）。
//! 3. **projection fields** — 投影需要的字段 ID 列表，`*` 会在此时展开。
//! 4. **remaining predicate** — 从 WHERE 里剥掉 pk 条件后剩下的表达式，
//!    拿到 row 后再做一次过滤。
//!
//! SELECT 生成两条执行路径：
//! - `SqlExecutionPlan::Read` — 有 `pk = literal`，直接单点 Get
//! - `SqlExecutionPlan::Scan` — 其余情况（pk LIKE 前缀、pk 范围、无 pk 条件等），
//!   走 `KeyValueExecutor::execute_scan`

use crate::{
    catalog_bind::{
        SqlCatalog, SqlCatalogBoundDelete, SqlCatalogBoundExpression, SqlCatalogBoundInsert,
        SqlCatalogBoundProjection, SqlCatalogBoundSelect, SqlCatalogBoundStatement, SqlCatalogBoundUpdate,
    },
    sql::{SqlBoundBinaryOperator, SqlBoundLiteral, SqlBoundOrderDirection, SqlFrontendError},
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
    let mut has_aggregate = false;
    let mut has_non_aggregate = false;
    for projection in &select.projections {
        match projection {
            SqlCatalogBoundProjection::Star(_) => {
                has_non_aggregate = true;
                for field in &table.fields {
                    projection_fields.push((field.field_id, field.name.clone()));
                }
            }
            SqlCatalogBoundProjection::Expression { expression, alias } => {
                match expression {
                    SqlCatalogBoundExpression::Column(col) => {
                        has_non_aggregate = true;
                        projection_fields.push((col.field_id, alias.clone().unwrap_or_else(|| col.field_name.clone())));
                    }
                    SqlCatalogBoundExpression::Function { .. } => {
                        has_aggregate = true;
                        projection_expressions
                            .push((expression.clone(), alias.clone().unwrap_or_else(|| expression_default_label(expression))));
                    }
                    other => {
                        has_non_aggregate = true;
                        projection_expressions
                            .push((other.clone(), alias.clone().unwrap_or_else(|| expression_default_label(other))));
                    }
                }
            }
        }
    }

    // 混合聚合 + 非聚合（无 GROUP BY）→ 拒绝。
    if has_aggregate && has_non_aggregate {
        return Err(planner_error("cannot mix aggregate and non-aggregate projections without GROUP BY"));
    }

    // 2. 递归解析 predicate 的 AND 树，把 pk 条件提取出来。
    let (pk_equality, pk_prefix, pk_after, pk_before, remaining_predicate) =
        match select.predicate {
            None => (None, None, None, None, None),
            Some(expr) => extract_pk_range(expr, pk_field.field_id)?,
        };

    // 3. 分流：有精确 pk 等值 → Read；其他全部 → Scan。
    if let Some(lit) = pk_equality {
        let rowid = pk_literal_to_rowid(&lit)?;
        return Ok(SqlExecutionPlan::Read(ReadPlan {
            namespace,
            rowid,
            projection_fields,
            projection_expressions,
            remaining_predicate,
        }));
    }

    // Scan 路径：prefix / after / before 都是 bytes。
    // ORDER BY 或聚合 或 有 remaining_predicate 时 kv-scan 不设 limit
    // —— 先 filter/aggregate/sort 才能知道前 N 行是哪些。
    // 把 LIMIT/OFFSET 推迟给 execute 层应用。
    let has_order_by = select.order_by.is_some();
    let has_predicate = remaining_predicate.is_some();
    let needs_post_filter_limit = has_order_by || has_aggregate || has_predicate;
    let kv_limit = if needs_post_filter_limit { 0 } else { select.limit.unwrap_or(0) };
    let limit_after_sort = if needs_post_filter_limit { select.limit.unwrap_or(0) } else { 0 };
    let offset_after_sort = if needs_post_filter_limit { select.offset.unwrap_or(0) } else { 0 };

    Ok(SqlExecutionPlan::Scan(ScanPlan {
        namespace,
        rowid_prefix: pk_prefix.unwrap_or_default(),
        rowid_after: pk_after,
        rowid_before: pk_before,
        limit: kv_limit,
        projection_fields,
        projection_expressions,
        remaining_predicate,
        order_by: select.order_by,
        limit_after_sort,
        offset_after_sort,
        has_aggregate,
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

/// 计划一个 UPDATE。要求 WHERE 里有 `pk = literal`；其余条件保留为 row-level filter。
pub fn plan_update(update: SqlCatalogBoundUpdate, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let table = find_table(catalog, update.table.type_id)?;
    let namespace = format!("table:{}", table.type_id);
    let pk_field = primary_key_of(table)?;

    let (rowid, remaining_predicate) = match update.predicate {
        None => return Err(planner_error("UPDATE requires WHERE with `pk = literal`")),
        Some(expr) => {
            let (pk_literal, _pk_prefix, _pk_after, _pk_before, remaining) = extract_pk_range(expr, pk_field.field_id)?;
            let lit = pk_literal
                .ok_or_else(|| planner_error(&format!("UPDATE WHERE must contain `{} = literal`", pk_field.name)))?;
            (lit, remaining)
        }
    };
    let rowid_bytes = pk_literal_to_rowid(&rowid)?;

    let mut write_fields = Vec::new();
    for (field, value_expr) in &update.assignments {
        let lit = literal_from_expression(value_expr)?;
        write_fields.push(WriteField { field_id: field.field_id, value: lit });
    }
    Ok(SqlExecutionPlan::Update(UpdatePlan {
        namespace,
        rowid: rowid_bytes,
        fields: write_fields,
        remaining_predicate,
    }))
}

/// 计划一个 DELETE。要求 WHERE 里有 `pk = literal`；其余条件保留为 row-level filter。
pub fn plan_delete(delete: SqlCatalogBoundDelete, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let table = find_table(catalog, delete.table.type_id)?;
    let namespace = format!("table:{}", table.type_id);
    let pk_field = primary_key_of(table)?;

    let (rowid, remaining_predicate) = match delete.predicate {
        None => return Err(planner_error("DELETE requires WHERE with `pk = literal`")),
        Some(expr) => {
            let (pk_literal, _pk_prefix, _pk_after, _pk_before, remaining) = extract_pk_range(expr, pk_field.field_id)?;
            let lit = pk_literal
                .ok_or_else(|| planner_error(&format!("DELETE WHERE must contain `{} = literal`", pk_field.name)))?;
            (lit, remaining)
        }
    };
    let rowid_bytes = pk_literal_to_rowid(&rowid)?;
    Ok(SqlExecutionPlan::Delete(DeletePlan { namespace, rowid: rowid_bytes, remaining_predicate }))
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

/// 已计划好的 SQL 操作，可以直接翻译为 KeyValueCommand 或 Scan。
#[derive(Debug, Clone)]
pub enum SqlExecutionPlan {
    /// SELECT — 精确主键等值单点读取。
    Read(ReadPlan),
    /// SELECT — namespace 扫描 + 行级过滤。
    Scan(ScanPlan),
    /// INSERT — PutIfAbsent（pk 必须由 VALUES 提供）。
    Insert(WritePlan),
    /// UPDATE — 先 Get 再 remaining_predicate 再 Put。
    Update(UpdatePlan),
    /// DELETE — 先 Get 再 remaining_predicate 再 Delete。
    Delete(DeletePlan),
}

/// SELECT 精确主键等值执行计划。
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

/// SELECT 多行扫描执行计划。
#[derive(Debug, Clone)]
pub struct ScanPlan {
    /// namespace 标签，形如 `table:{type_id}`。
    pub namespace: String,
    /// rowid 前缀 bytes；空 = 整个 namespace 全扫。
    pub rowid_prefix: Vec<u8>,
    /// exclusive lower cursor —— 跳过所有 `rowid <= after` 的记录。None = 无下界。
    pub rowid_after: Option<Vec<u8>>,
    /// exclusive upper cursor —— 跳过所有 `rowid >= before` 的记录。None = 无上界。
    pub rowid_before: Option<Vec<u8>>,
    /// 下推到 kv-store 的 scan limit（0 = 无上限）。ORDER BY 或聚合存在时会保持 0，
    /// 因为需要全量 scan 后再做内存处理。
    pub limit: usize,
    /// 投影里直接映射到 catalog 字段的那些列：(field_id, display_name)。
    pub projection_fields: Vec<(u64, String)>,
    /// 投影里需要在 row 上做表达式求值的那些：(expression, display_name)。
    pub projection_expressions: Vec<(SqlCatalogBoundExpression, String)>,
    /// 行级过滤表达式；`None` 表示不过滤。
    pub remaining_predicate: Option<SqlCatalogBoundExpression>,
    /// 可选 ORDER BY 字段；scan 全部 rows 后由 execute 层内存排序。
    pub order_by: Option<(SqlCatalogBoundExpression, SqlBoundOrderDirection)>,
    /// 排序 / 聚合之后才应用的 LIMIT。ORDER BY 存在时 kv-scan limit=0，
    /// 由本字段承接 SQL 的 LIMIT 值。0 = 无上限。
    pub limit_after_sort: usize,
    /// 排序 / 聚合之后才应用的 OFFSET。默认 0。
    pub offset_after_sort: usize,
    /// True when any projection expression is an aggregate function
    /// (COUNT/SUM/AVG/MAX/MIN). execute_scan will produce exactly one result row.
    pub has_aggregate: bool,
}

/// INSERT 执行计划。payload 由 execute 层做 TLV 编码。
#[derive(Debug, Clone)]
pub struct WritePlan {
    /// namespace 标签。
    pub namespace: String,
    /// 要写入的 row 的主键值 bytes。
    pub rowid: Vec<u8>,
    /// INSERT 时提供所有字段值；UPDATE 时提供要 SET 的字段。
    pub fields: Vec<WriteField>,
}

/// UPDATE 执行计划。payload 由 execute 层做 TLV 编码。
#[derive(Debug, Clone)]
pub struct UpdatePlan {
    /// namespace 标签。
    pub namespace: String,
    /// 要写入的 row 的主键值 bytes。
    pub rowid: Vec<u8>,
    /// 要 SET 的字段。
    pub fields: Vec<WriteField>,
    /// 从原 WHERE 里剥掉主键等值后剩下的表达式。
    /// execute_update 会在写入前对现有 row 评估此 predicate。
    pub remaining_predicate: Option<SqlCatalogBoundExpression>,
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
    /// 从原 WHERE 里剥掉主键等值后剩下的表达式。
    /// execute_delete 会在删除前对现有 row 评估此 predicate。
    pub remaining_predicate: Option<SqlCatalogBoundExpression>,
}

/// 从 WHERE predicate 里提取所有关于 pk 字段的条件。
///
/// 返回五元组：
/// - `Option<SqlBoundLiteral>` —— pk = literal 等值（读单点）
/// - `Option<Vec<u8>>` —— pk LIKE 'prefix%' 前缀
/// - `Option<Vec<u8>>` —— after exclusive lower cursor（来自 pk > X 或 pk >= X → exclusive X）
/// - `Option<Vec<u8>>` —— before exclusive upper cursor（来自 pk < Y 或 pk <= Y → exclusive Y+ε）
/// - `Option<SqlCatalogBoundExpression>` —— 剩余不能下推的条件
///
/// 冲突规则：两个等值 / 两个不同前缀 / after ≥ before → planner_error。
fn extract_pk_range(
    predicate: SqlCatalogBoundExpression,
    pk_field_id: u64,
) -> Result<
    (
        Option<SqlBoundLiteral>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<Vec<u8>>,
        Option<SqlCatalogBoundExpression>,
    ),
    SqlFrontendError,
> {

    match predicate {
        // AND 拆分 — 两侧分别提取，再合并。
        SqlCatalogBoundExpression::Binary {
            left,
            operator: SqlBoundBinaryOperator::And,
            right,
        } => {
            let (l_eq, l_pre, l_after, l_before, l_rem) = extract_pk_range(*left, pk_field_id)?;
            let (r_eq, r_pre, r_after, r_before, r_rem) = extract_pk_range(*right, pk_field_id)?;
            let eq = merge_opt(l_eq, r_eq, "conflicting primary-key equality inside AND")?;
            let prefix = merge_opt(l_pre, r_pre, "conflicting primary-key LIKE prefix inside AND")?;
            let after = merge_opt(l_after, r_after, "conflicting primary-key lower bound inside AND")?;
            let before = merge_opt(l_before, r_before, "conflicting primary-key upper bound inside AND")?;
            let remaining = combine_remaining(l_rem, r_rem);
            Ok((eq, prefix, after, before, remaining))
        }

        // 所有其他 Binary 表达式 —— 尝试识别 pk 条件。
        SqlCatalogBoundExpression::Binary { left, operator, right } => {



            match (operator, *left, *right) {
                // pk = literal
                (SqlBoundBinaryOperator::Equal, SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit))
                | (SqlBoundBinaryOperator::Equal, SqlCatalogBoundExpression::Literal(lit), SqlCatalogBoundExpression::Column(col))
                    if col.field_id == pk_field_id =>
                {
                    let _rowid = pk_literal_to_rowid(&lit)?;
                    Ok((Some(lit), None, None, None, None))
                }

                // pk > literal → after exclusive
                (SqlBoundBinaryOperator::Greater, SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit))
                    if col.field_id == pk_field_id =>
                {
                    let rowid = pk_literal_to_rowid(&lit)?;
                    Ok((None, None, Some(rowid), None, None))
                }

                // pk >= literal → after exclusive：key > (lit-1) 等价于 key >= lit
                (SqlBoundBinaryOperator::GreaterEqual, SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit))
                    if col.field_id == pk_field_id =>
                {
                    let rowid = pk_literal_to_rowid(&lit)?;
                    Ok((None, None, Some(rowid_decrement(&rowid)), None, None))
                }

                // pk < literal → before exclusive
                (SqlBoundBinaryOperator::Less, SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit))
                    if col.field_id == pk_field_id =>
                {
                    let rowid = pk_literal_to_rowid(&lit)?;
                    Ok((None, None, None, Some(rowid), None))
                }

                // pk <= literal → before exclusive：key < (lit+1) 等价于 key <= lit
                (SqlBoundBinaryOperator::LessEqual, SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit))
                    if col.field_id == pk_field_id =>
                {
                    let rowid = pk_literal_to_rowid(&lit)?;
                    Ok((None, None, None, Some(rowid_increment(&rowid)), None))
                }

                // pk LIKE 'prefix%'
                (SqlBoundBinaryOperator::Like, SqlCatalogBoundExpression::Column(col), SqlCatalogBoundExpression::Literal(lit))
                    if col.field_id == pk_field_id =>
                {

                    let prefix = match &lit {
                        SqlBoundLiteral::String(s) if s.ends_with('%') && !s.ends_with("%%") => {
                            s[..s.len() - 1].as_bytes().to_vec()
                        }
                        _ => {
                            // LIKE 模式不只是 prefix%，下推成 remaining
                            return Ok((None, None, None, None, Some(SqlCatalogBoundExpression::Binary {
                                left: Box::new(SqlCatalogBoundExpression::Column(col)),
                                operator: SqlBoundBinaryOperator::Like,
                                right: Box::new(SqlCatalogBoundExpression::Literal(lit)),
                            })));
                        }
                    };
                    Ok((None, Some(prefix), None, None, None))
                }

                // 其他所有 Binary —— 留在 remaining
                (op, left, right) => Ok((None, None, None, None, Some(SqlCatalogBoundExpression::Binary {
                    left: Box::new(left),
                    operator: op,
                    right: Box::new(right),
                }))),
            }
        }

        // OR、Between、Unary、Column-alone、Literal-alone 等全部留作 remaining。
        other => Ok((None, None, None, None, Some(other))),
    }
}

fn merge_opt<T>(left: Option<T>, right: Option<T>, conflict_msg: &str) -> Result<Option<T>, SqlFrontendError>
where
    T: PartialEq,
{
    match (left, right) {
        (Some(a), Some(b)) if a == b => Ok(Some(a)),
        (Some(_), Some(_)) => Err(planner_error(conflict_msg)),
        (Some(a), None) => Ok(Some(a)),
        (None, Some(b)) => Ok(Some(b)),
        (None, None) => Ok(None),
    }
}

fn combine_remaining(
    left: Option<SqlCatalogBoundExpression>,
    right: Option<SqlCatalogBoundExpression>,
) -> Option<SqlCatalogBoundExpression> {
    match (left, right) {
        (None, None) => None,
        (Some(l), None) | (None, Some(l)) => Some(l),
        (Some(l), Some(r)) => Some(SqlCatalogBoundExpression::Binary {
            left: Box::new(l),
            operator: SqlBoundBinaryOperator::And,
            right: Box::new(r),
        }),
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

/// rowid + 1 并转为 bytes。用于把 inclusive upper bound 转为 exclusive before。
fn rowid_increment(rowid: &[u8]) -> Vec<u8> {
    let s = String::from_utf8_lossy(rowid);
    match s.parse::<u128>() {
        Ok(n) => (n + 1).to_string().into_bytes(),
        Err(_) => {
            let mut v = rowid.to_vec();
            if let Some(last) = v.last_mut() {
                *last = last.saturating_add(1);
            }
            v
        }
    }
}

/// rowid - 1 并转为 bytes。用于把 inclusive lower bound 转为 exclusive after。
fn rowid_decrement(rowid: &[u8]) -> Vec<u8> {
    let s = String::from_utf8_lossy(rowid);
    match s.parse::<u128>() {
        Ok(0) => rowid.to_vec(),
        Ok(n) => (n - 1).to_string().into_bytes(),
        Err(_) => {
            let mut v = rowid.to_vec();
            if let Some(last) = v.last_mut() {
                *last = last.saturating_sub(1);
            }
            v
        }
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
