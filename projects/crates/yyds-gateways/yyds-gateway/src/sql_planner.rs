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
        collect_column_ids, SqlCatalog, SqlCatalogBoundDelete, SqlCatalogBoundExpression, SqlCatalogBoundInsert,
        SqlCatalogBoundProjection, SqlCatalogBoundSelect, SqlCatalogBoundStatement, SqlCatalogBoundTable,
        SqlCatalogBoundUpdate, SqlFieldType,
    },
    sql::{
        SqlAggregateFunction, SqlBoundBinaryOperator, SqlBoundLiteral, SqlBoundOrderDirection, SqlErrorKind, SqlFrontendError,
    },
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
    if !select.joins.is_empty() {
        return plan_join_select(select, catalog);
    }
    // 无 FROM 的 SELECT 只做常量 / 内建函数求值（驱动探测 VERSION()、SELECT 1 等）。
    let Some(source) = select.source.as_ref()
    else {
        return plan_source_less_select(select);
    };
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
            SqlCatalogBoundProjection::Star(star_table) => {
                has_non_aggregate = true;
                let star_table = find_table(catalog, star_table.type_id)?;
                for field in &star_table.fields {
                    projection_fields.push((field.field_id, field.name.clone()));
                }
            }
            SqlCatalogBoundProjection::Expression { expression, alias } => match expression {
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
            },
        }
    }

    // 混合聚合 + 非聚合（无 GROUP BY）→ 拒绝。
    if has_aggregate && has_non_aggregate {
        return Err(planner_error("cannot mix aggregate and non-aggregate projections without GROUP BY"));
    }

    // 2. 递归解析 predicate 的 AND 树，把 pk 条件提取出来。
    let (pk_equality, pk_prefix, pk_after, pk_before, remaining_predicate) = match select.predicate {
        None => (None, None, None, None, None),
        Some(expr) => extract_pk_range(expr, pk_field.field_id)?,
    };

    // 3. 分流：有精确 pk 等值 → Read；其他全部 → Scan。
    // 先从 catalog table 收集 field_id → SqlFieldType 映射。
    let field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType> =
        table.fields.iter().map(|f| (f.field_id, f.field_type.clone())).collect();

    if let Some(lit) = pk_equality {
        let rowid = pk_literal_to_rowid(&lit)?;
        return Ok(SqlExecutionPlan::Read(ReadPlan {
            namespace,
            rowid,
            projection_fields,
            projection_expressions,
            remaining_predicate,
            // `LIMIT 0` 按 SQL 语义返回空结果集，单点读同样不能绕过。
            suppress_rows: select.limit == Some(0),
            field_types: field_types.clone(),
        }));
    }

    // Scan 路径：prefix / after / before 都是 bytes。
    // ORDER BY / GROUP BY / 聚合 / remaining_predicate 时 kv-scan 不设 limit
    // —— 先 filter/aggregate/sort/group 才能知道前 N 行是哪些。
    // 把 LIMIT/OFFSET 推迟给 execute 层应用。
    let has_order_by = select.order_by.is_some();
    let has_group_by = select.group_by.is_some();
    let has_predicate = remaining_predicate.is_some();
    let needs_post_filter_limit = has_order_by || has_group_by || has_aggregate || has_predicate;
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
        group_by: select.group_by,
        having: select.having,
        order_by: select.order_by,
        limit_after_sort,
        offset_after_sort,
        suppress_rows: select.limit == Some(0),
        has_aggregate,
        field_types,
    }))
}

/// 计划一个带 JOIN 的 SELECT。
///
/// 目前只做内连接：基表与每张连接表各自全表扫描，按 JOIN 顺序做嵌套循环合并，
/// 再套用 WHERE / 投影 / ORDER BY / LIMIT。主键下推、外连接、聚合暂不在网关子集内。
fn plan_join_select(select: SqlCatalogBoundSelect, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let SqlCatalogBoundSelect { source, joins, projections, predicate, group_by, having, limit, offset, order_by } = select;
    let base = source.ok_or_else(|| planner_error("SQL JOIN requires a FROM table"))?;
    if group_by.is_some() || having.is_some() {
        return Err(planner_error("SQL JOIN with GROUP BY / HAVING is not in the gateway subset yet"));
    }

    // 1. 投影展开：Star 按来源顺序展开整表，Column 直接映射，其余表达式保留。
    let mut projection_fields: Vec<(u64, String)> = Vec::new();
    let mut projection_expressions: Vec<(SqlCatalogBoundExpression, String)> = Vec::new();
    for projection in projections {
        match projection {
            SqlCatalogBoundProjection::Star(table) => {
                let table = find_table(catalog, table.type_id)?;
                for field in &table.fields {
                    projection_fields.push((field.field_id, field.name.clone()));
                }
            }
            SqlCatalogBoundProjection::Expression { expression, alias } => match expression {
                SqlCatalogBoundExpression::Column(col) => {
                    projection_fields.push((col.field_id, alias.unwrap_or_else(|| col.field_name.clone())));
                }
                other => {
                    if expression_has_aggregate(&other) {
                        return Err(planner_error("SQL JOIN with aggregate projections is not in the gateway subset yet"));
                    }
                    let label = alias.unwrap_or_else(|| expression_default_label(&other));
                    projection_expressions.push((other, label));
                }
            },
        }
    }

    // 2. 需要解码的字段集合：投影 + WHERE + ORDER BY + 每个 ON 条件。
    let mut needed: Vec<u64> = Vec::new();
    for (field_id, _) in &projection_fields {
        if !needed.contains(field_id) {
            needed.push(*field_id);
        }
    }
    for (expr, _) in &projection_expressions {
        collect_column_ids(expr, &mut needed);
    }
    if let Some(predicate) = &predicate {
        collect_column_ids(predicate, &mut needed);
    }
    if let Some(items) = &order_by {
        for (expr, _) in items {
            collect_column_ids(expr, &mut needed);
        }
    }
    for join in &joins {
        collect_column_ids(&join.on, &mut needed);
    }

    // 3. 每个来源只解码自己表的字段。
    let mut field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType> = std::collections::BTreeMap::new();
    let base = join_source_plan(base, None, catalog, &needed, &mut field_types)?;
    let mut join_sources = Vec::with_capacity(joins.len());
    for join in joins {
        join_sources.push(join_source_plan(join.table, Some(join.on), catalog, &needed, &mut field_types)?);
    }

    Ok(SqlExecutionPlan::Join(JoinPlan {
        base,
        joins: join_sources,
        projection_fields,
        projection_expressions,
        remaining_predicate: predicate,
        order_by,
        limit_after_sort: limit.unwrap_or(0),
        offset_after_sort: offset.unwrap_or(0),
        suppress_rows: limit == Some(0),
        field_types,
    }))
}

/// 构造一个 JOIN 数据来源计划：记录 namespace、ON 条件与该表需要解码的字段。
fn join_source_plan(
    table: SqlCatalogBoundTable,
    on: Option<SqlCatalogBoundExpression>,
    catalog: &SqlCatalog,
    needed: &[u64],
    field_types: &mut std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
) -> Result<JoinSourcePlan, SqlFrontendError> {
    let catalog_table = find_table(catalog, table.type_id)?;
    let mut needed_field_ids = Vec::new();
    for field in &catalog_table.fields {
        field_types.insert(field.field_id, field.field_type.clone());
        if needed.contains(&field.field_id) {
            needed_field_ids.push(field.field_id);
        }
    }
    Ok(JoinSourcePlan { namespace: format!("table:{}", catalog_table.type_id), needed_field_ids, on })
}

/// 计划一个没有 FROM 的 SELECT：只允许常量与内建函数，不访问任何 namespace。
fn plan_source_less_select(select: SqlCatalogBoundSelect) -> Result<SqlExecutionPlan, SqlFrontendError> {
    if select.predicate.is_some() {
        return Err(planner_error("SQL WHERE requires a FROM source"));
    }
    if select.group_by.is_some() {
        return Err(planner_error("SQL GROUP BY requires a FROM source"));
    }
    if select.having.is_some() {
        return Err(planner_error("SQL HAVING requires a FROM source"));
    }
    let mut projections: Vec<(SqlCatalogBoundExpression, String)> = Vec::with_capacity(select.projections.len());
    for projection in select.projections {
        match projection {
            SqlCatalogBoundProjection::Star(_) => {
                return Err(planner_error("SQL star projection requires a catalog table source"));
            }
            SqlCatalogBoundProjection::Expression { expression, alias } => {
                let label = alias.unwrap_or_else(|| scalar_projection_label(&expression));
                projections.push((expression, label));
            }
        }
    }
    if projections.is_empty() {
        return Err(planner_error("SQL SELECT without FROM requires at least one projection"));
    }
    // `LIMIT 0` 按 SQL 语义返回空结果集。
    let suppress_row = select.limit == Some(0);
    Ok(SqlExecutionPlan::Scalar(ScalarPlan { projections, suppress_row }))
}

/// 计划一个 INSERT。要求 VALUES 里第一个值是主键 literal。
pub fn plan_insert(insert: SqlCatalogBoundInsert, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let table = find_table(catalog, insert.table.type_id)?;
    let namespace = format!("table:{}", table.type_id);
    let field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType> =
        table.fields.iter().map(|f| (f.field_id, f.field_type.clone())).collect();

    let pk_bound = insert.fields.first().ok_or_else(|| planner_error("INSERT has no fields"))?;
    let pk_value = insert.values.first().ok_or_else(|| planner_error("INSERT has no values"))?;
    let rowid = literal_from_expression(pk_value)?;
    let rowid_bytes = pk_literal_to_rowid(&rowid)?;

    let mut write_fields = Vec::with_capacity(insert.fields.len());
    for (field, value) in insert.fields.iter().zip(insert.values.iter()) {
        let lit = literal_from_expression(value)?;
        let field_type = field_types.get(&field.field_id).cloned().unwrap_or(crate::catalog_bind::SqlFieldType::Unknown);
        check_write_literal(&lit, &field_type, &field.field_name)?;
        write_fields.push(WriteField { field_id: field.field_id, value: lit, field_type });
        let _ = pk_bound;
    }
    Ok(SqlExecutionPlan::Insert(WritePlan { namespace, rowid: rowid_bytes, fields: write_fields, field_types }))
}

/// 计划一个 UPDATE。`pk = literal` 走单点路径；其他 WHERE 都退化为范围扫描逐行改写。
pub fn plan_update(update: SqlCatalogBoundUpdate, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let table = find_table(catalog, update.table.type_id)?;
    let namespace = format!("table:{}", table.type_id);
    let pk_field = primary_key_of(table)?;
    let field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType> =
        table.fields.iter().map(|f| (f.field_id, f.field_type.clone())).collect();

    let mut write_fields = Vec::new();
    for (field, value_expr) in &update.assignments {
        let lit = literal_from_expression(value_expr)?;
        let field_type = field_types.get(&field.field_id).cloned().unwrap_or(crate::catalog_bind::SqlFieldType::Unknown);
        check_write_literal(&lit, &field_type, &field.field_name)?;
        write_fields.push(WriteField { field_id: field.field_id, value: lit, field_type });
    }

    let Some(predicate) = update.predicate
    else {
        return Ok(SqlExecutionPlan::UpdateScan(UpdateScanPlan {
            namespace,
            rowid_prefix: Vec::new(),
            rowid_after: None,
            rowid_before: None,
            fields: write_fields,
            predicate: None,
            field_types,
        }));
    };

    let (pk_literal, pk_prefix, pk_after, pk_before, remaining) = extract_pk_range(predicate.clone(), pk_field.field_id)?;
    if let Some(rowid_literal) = pk_literal {
        return Ok(SqlExecutionPlan::Update(UpdatePlan {
            namespace,
            rowid: pk_literal_to_rowid(&rowid_literal)?,
            fields: write_fields,
            remaining_predicate: remaining,
            field_types,
        }));
    }
    Ok(SqlExecutionPlan::UpdateScan(UpdateScanPlan {
        namespace,
        rowid_prefix: pk_prefix.unwrap_or_default(),
        rowid_after: pk_after,
        rowid_before: pk_before,
        fields: write_fields,
        predicate: remaining,
        field_types,
    }))
}

/// 计划一个 DELETE。`pk = literal` 走单点路径；其他 WHERE 都退化为范围扫描逐行删除。
pub fn plan_delete(delete: SqlCatalogBoundDelete, catalog: &SqlCatalog) -> Result<SqlExecutionPlan, SqlFrontendError> {
    let table = find_table(catalog, delete.table.type_id)?;
    let namespace = format!("table:{}", table.type_id);
    let pk_field = primary_key_of(table)?;
    let field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType> =
        table.fields.iter().map(|f| (f.field_id, f.field_type.clone())).collect();

    let Some(predicate) = delete.predicate
    else {
        return Ok(SqlExecutionPlan::DeleteScan(DeleteScanPlan {
            namespace,
            rowid_prefix: Vec::new(),
            rowid_after: None,
            rowid_before: None,
            predicate: None,
            field_types,
        }));
    };

    let (pk_literal, pk_prefix, pk_after, pk_before, remaining) = extract_pk_range(predicate.clone(), pk_field.field_id)?;
    if let Some(rowid_literal) = pk_literal {
        return Ok(SqlExecutionPlan::Delete(DeletePlan {
            namespace,
            rowid: pk_literal_to_rowid(&rowid_literal)?,
            remaining_predicate: remaining,
            field_types,
        }));
    }
    Ok(SqlExecutionPlan::DeleteScan(DeleteScanPlan {
        namespace,
        rowid_prefix: pk_prefix.unwrap_or_default(),
        rowid_after: pk_after,
        rowid_before: pk_before,
        predicate: remaining,
        field_types,
    }))
}

/// Rejects a write literal that the declared column type cannot represent, so a
/// string never lands in an integer column and reads back as the wrong value.
fn check_write_literal(literal: &SqlBoundLiteral, field_type: &SqlFieldType, column: &str) -> Result<(), SqlFrontendError> {
    let fits = match field_type.inner() {
        SqlFieldType::Unknown => true,
        SqlFieldType::Int64 | SqlFieldType::Int32 => match literal {
            SqlBoundLiteral::Null | SqlBoundLiteral::Integer(_) => true,
            SqlBoundLiteral::String(text) => text.parse::<i64>().is_ok(),
            SqlBoundLiteral::Number(_) | SqlBoundLiteral::Boolean(_) => false,
        },
        SqlFieldType::Float64 => match literal {
            SqlBoundLiteral::Null | SqlBoundLiteral::Integer(_) | SqlBoundLiteral::Number(_) => true,
            SqlBoundLiteral::String(text) => text.parse::<f64>().is_ok(),
            SqlBoundLiteral::Boolean(_) => false,
        },
        SqlFieldType::Bool => match literal {
            SqlBoundLiteral::Null | SqlBoundLiteral::Boolean(_) => true,
            SqlBoundLiteral::Integer(value) => *value == 0 || *value == 1,
            SqlBoundLiteral::String(text) => {
                matches!(text.to_ascii_lowercase().as_str(), "true" | "false" | "t" | "f" | "1" | "0")
            }
            SqlBoundLiteral::Number(_) => false,
        },
        SqlFieldType::String | SqlFieldType::Bytes => {
            matches!(literal, SqlBoundLiteral::Null | SqlBoundLiteral::String(_))
        }
        SqlFieldType::Optional(_) => unreachable!("inner() strips Optional"),
    };
    if fits {
        Ok(())
    }
    else {
        Err(SqlFrontendError::new(
            SqlErrorKind::TypeMismatch,
            format!("SQL value {:?} cannot be written to column `{column}`", literal),
        ))
    }
}

/// 从 bound expression 里提取 literal。目前 INSERT/UPDATE 的值必须是 literal。
fn literal_from_expression(expr: &SqlCatalogBoundExpression) -> Result<SqlBoundLiteral, SqlFrontendError> {
    match expr {
        SqlCatalogBoundExpression::Literal(lit) => Ok(lit.clone()),
        SqlCatalogBoundExpression::Column(col) => Err(planner_error(&format!(
            "SQL gateway write values must be literals; column reference `{}` is not supported in this first step",
            col.field_name
        ))),
        SqlCatalogBoundExpression::Wildcard => {
            Err(planner_error("SQL gateway write values must be literals; `*` is not a value"))
        }
        SqlCatalogBoundExpression::Function { .. }
        | SqlCatalogBoundExpression::Binary { .. }
        | SqlCatalogBoundExpression::Unary { .. }
        | SqlCatalogBoundExpression::InSubquery { .. }
        | SqlCatalogBoundExpression::InValues { .. } => Err(planner_error("SQL gateway write values must be simple literals")),
    }
}

/// 已计划好的 SQL 操作，可以直接翻译为 KeyValueCommand 或 Scan。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlExecutionPlan {
    /// SELECT — 精确主键等值单点读取。
    Read(ReadPlan),
    /// SELECT — namespace 扫描 + 行级过滤。
    Scan(ScanPlan),
    /// SELECT — 多表内连接（嵌套循环合并后再投影 / 排序 / 限流）。
    Join(JoinPlan),
    /// SELECT without FROM — constant and scalar builtin evaluation.
    Scalar(ScalarPlan),
    /// INSERT — PutIfAbsent（pk 必须由 VALUES 提供）。
    Insert(WritePlan),
    /// UPDATE — 先 Get 再 remaining_predicate 再 Put。
    Update(UpdatePlan),
    /// UPDATE — 扫描匹配行后逐行改写。
    UpdateScan(UpdateScanPlan),
    /// DELETE — 先 Get 再 remaining_predicate 再 Delete。
    Delete(DeletePlan),
    /// DELETE — 扫描匹配行后逐行删除。
    DeleteScan(DeleteScanPlan),
}

/// SELECT 精确主键等值执行计划。
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// `LIMIT 0`：结果集必须为空，不允许单点读绕过。
    pub suppress_rows: bool,
    /// field_id → SqlFieldType 映射，execute 层 decode_row 用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
}

/// SELECT 多行扫描执行计划。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanPlan {
    /// namespace 标签，形如 `table:{type_id}`。
    pub namespace: String,
    /// rowid 前缀 bytes；空 = 整个 namespace 全扫。
    pub rowid_prefix: Vec<u8>,
    /// exclusive lower cursor —— 跳过所有 `rowid <= after` 的记录。None = 无下界。
    pub rowid_after: Option<Vec<u8>>,
    /// exclusive upper cursor —— 跳过所有 `rowid >= before` 的记录。None = 无上界。
    pub rowid_before: Option<Vec<u8>>,
    /// 下推到 kv-store 的 scan limit（0 = 无上限）。ORDER BY / 聚合 / GROUP BY 存在时保持 0。
    pub limit: usize,
    /// 投影里直接映射到 catalog 字段的那些列：(field_id, display_name)。
    pub projection_fields: Vec<(u64, String)>,
    /// 投影里需要在 row 上做表达式求值的那些：(expression, display_name)。
    pub projection_expressions: Vec<(SqlCatalogBoundExpression, String)>,
    /// 行级过滤表达式；`None` 表示不过滤。
    pub remaining_predicate: Option<SqlCatalogBoundExpression>,
    /// 可选 GROUP BY 表达式列表；scan 全部 rows 后由 execute 层做 bucket 聚合。
    pub group_by: Option<Vec<SqlCatalogBoundExpression>>,
    /// 可选 HAVING 表达式；GROUP BY 聚合完后应用。
    pub having: Option<SqlCatalogBoundExpression>,
    /// 可选 ORDER BY 字段列表；scan 全部 rows 后由 execute 层内存排序。
    pub order_by: Option<Vec<(SqlCatalogBoundExpression, SqlBoundOrderDirection)>>,
    /// 排序 / 聚合之后才应用的 LIMIT。ORDER BY / 聚合 / GROUP BY 存在时 kv-scan limit=0，
    /// 由本字段承接 SQL 的 LIMIT 值。0 = 无上限。
    pub limit_after_sort: usize,
    /// 排序 / 聚合之后才应用的 OFFSET。默认 0。
    pub offset_after_sort: usize,
    /// True when any projection expression is an aggregate function
    /// (COUNT/SUM/AVG/MAX/MIN). execute_scan will produce aggregated result rows.
    pub has_aggregate: bool,
    /// `LIMIT 0`：结果集必须为空，包括聚合结果。
    pub suppress_rows: bool,
    /// field_id → SqlFieldType 映射，execute 层 decode_row 用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
}

/// SELECT 多表内连接执行计划（嵌套循环合并）。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinPlan {
    /// 基表来源。
    pub base: JoinSourcePlan,
    /// 依次连接的来源，每个带自己的 ON 条件。
    pub joins: Vec<JoinSourcePlan>,
    /// 投影里直接映射到 catalog 字段的那些列：(field_id, display_name)。
    pub projection_fields: Vec<(u64, String)>,
    /// 投影里需要在合并行上做表达式求值的那些：(expression, display_name)。
    pub projection_expressions: Vec<(SqlCatalogBoundExpression, String)>,
    /// 行级过滤表达式；`None` 表示不过滤。
    pub remaining_predicate: Option<SqlCatalogBoundExpression>,
    /// 可选 ORDER BY 字段列表；合并出全部行后由 execute 层内存排序。
    pub order_by: Option<Vec<(SqlCatalogBoundExpression, SqlBoundOrderDirection)>>,
    /// 排序之后才应用的 LIMIT。0 = 无上限。
    pub limit_after_sort: usize,
    /// 排序之后才应用的 OFFSET。默认 0。
    pub offset_after_sort: usize,
    /// `LIMIT 0`：结果集必须为空。
    pub suppress_rows: bool,
    /// field_id → SqlFieldType 映射，execute 层 decode_row / 求值用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
}

/// JOIN 的一个数据来源。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JoinSourcePlan {
    /// namespace 标签，形如 `table:{type_id}`。
    pub namespace: String,
    /// 需要从该表 TLV payload 里解码的字段（其余字段跳过）。
    pub needed_field_ids: Vec<u64>,
    /// 该来源的 ON 条件；基表为 `None`。
    pub on: Option<SqlCatalogBoundExpression>,
}

/// SELECT without FROM 执行计划：纯常量与标量内建函数求值，产出单行结果。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScalarPlan {
    /// 投影表达式与其结果列名。
    pub projections: Vec<(SqlCatalogBoundExpression, String)>,
    /// `LIMIT 0` 时按 SQL 语义返回空结果集。
    pub suppress_row: bool,
}

/// INSERT 执行计划。payload 由 execute 层做 TLV 编码。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WritePlan {
    /// namespace 标签。
    pub namespace: String,
    /// 要写入的 row 的主键值 bytes。
    pub rowid: Vec<u8>,
    /// INSERT 时提供所有字段值；UPDATE 时提供要 SET 的字段。
    pub fields: Vec<WriteField>,
    /// field_id → SqlFieldType 映射，execute 层 encode/eval 用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
}

/// UPDATE 执行计划。payload 由 execute 层做 TLV 编码。
#[derive(Debug, Clone, PartialEq, Eq)]
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
    /// field_id → SqlFieldType 映射，execute 层 eval predicate 用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
}

/// 一个要写入的字段。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteField {
    /// field_id。
    pub field_id: u64,
    /// 要写入的值（必须是 Literal，复杂表达式暂不支持）。
    pub value: SqlBoundLiteral,
    /// 从 catalog 投影的字段类型，execute 层编码校验用。
    pub field_type: crate::catalog_bind::SqlFieldType,
}

/// DELETE 执行计划。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeletePlan {
    pub namespace: String,
    pub rowid: Vec<u8>,
    /// 从原 WHERE 里剥掉主键等值后剩下的表达式。
    /// execute_delete 会在删除前对现有 row 评估此 predicate。
    pub remaining_predicate: Option<SqlCatalogBoundExpression>,
    /// field_id → SqlFieldType 映射，execute 层 eval predicate 用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
}

/// 多行 UPDATE 执行计划：扫描 rowid 区间，逐行评估完整 WHERE 后改写。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UpdateScanPlan {
    /// namespace 标签，形如 `table:{type_id}`。
    pub namespace: String,
    /// rowid 前缀 bytes；空 = 整个 namespace 全扫。
    pub rowid_prefix: Vec<u8>,
    /// exclusive lower cursor。
    pub rowid_after: Option<Vec<u8>>,
    /// exclusive upper cursor。
    pub rowid_before: Option<Vec<u8>>,
    /// 要 SET 的字段。
    pub fields: Vec<WriteField>,
    /// 已去掉可下推主键范围后的完整行级过滤条件；`None` 表示整段区间都匹配。
    pub predicate: Option<SqlCatalogBoundExpression>,
    /// field_id → SqlFieldType 映射，execute 层解码与求值用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
}

/// 多行 DELETE 执行计划：扫描 rowid 区间，逐行评估完整 WHERE 后删除。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeleteScanPlan {
    /// namespace 标签，形如 `table:{type_id}`。
    pub namespace: String,
    /// rowid 前缀 bytes；空 = 整个 namespace 全扫。
    pub rowid_prefix: Vec<u8>,
    /// exclusive lower cursor。
    pub rowid_after: Option<Vec<u8>>,
    /// exclusive upper cursor。
    pub rowid_before: Option<Vec<u8>>,
    /// 已去掉可下推主键范围后的完整行级过滤条件；`None` 表示整段区间都匹配。
    pub predicate: Option<SqlCatalogBoundExpression>,
    /// field_id → SqlFieldType 映射，execute 层解码与求值用。
    pub field_types: std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
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
    (Option<SqlBoundLiteral>, Option<Vec<u8>>, Option<Vec<u8>>, Option<Vec<u8>>, Option<SqlCatalogBoundExpression>),
    SqlFrontendError,
> {
    match predicate {
        // AND 拆分 — 两侧分别提取，再合并。
        SqlCatalogBoundExpression::Binary { left, operator: SqlBoundBinaryOperator::And, right } => {
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
                (
                    SqlBoundBinaryOperator::Equal,
                    SqlCatalogBoundExpression::Column(col),
                    SqlCatalogBoundExpression::Literal(lit),
                )
                | (
                    SqlBoundBinaryOperator::Equal,
                    SqlCatalogBoundExpression::Literal(lit),
                    SqlCatalogBoundExpression::Column(col),
                ) if col.field_id == pk_field_id => {
                    let _rowid = pk_literal_to_rowid(&lit)?;
                    Ok((Some(lit), None, None, None, None))
                }

                // pk > literal → after exclusive
                (
                    SqlBoundBinaryOperator::Greater,
                    SqlCatalogBoundExpression::Column(col),
                    SqlCatalogBoundExpression::Literal(lit),
                ) if col.field_id == pk_field_id => {
                    let rowid = pk_literal_to_rowid(&lit)?;
                    Ok((None, None, Some(rowid), None, None))
                }

                // pk >= literal → after exclusive：key > (lit-1) 等价于 key >= lit
                (
                    SqlBoundBinaryOperator::GreaterEqual,
                    SqlCatalogBoundExpression::Column(col),
                    SqlCatalogBoundExpression::Literal(lit),
                ) if col.field_id == pk_field_id => {
                    let rowid = pk_literal_to_rowid(&lit)?;
                    Ok((None, None, Some(rowid_decrement(&rowid)), None, None))
                }

                // pk < literal → before exclusive
                (
                    SqlBoundBinaryOperator::Less,
                    SqlCatalogBoundExpression::Column(col),
                    SqlCatalogBoundExpression::Literal(lit),
                ) if col.field_id == pk_field_id => {
                    let rowid = pk_literal_to_rowid(&lit)?;
                    Ok((None, None, None, Some(rowid), None))
                }

                // pk <= literal → before exclusive：key < (lit+1) 等价于 key <= lit
                (
                    SqlBoundBinaryOperator::LessEqual,
                    SqlCatalogBoundExpression::Column(col),
                    SqlCatalogBoundExpression::Literal(lit),
                ) if col.field_id == pk_field_id => {
                    let rowid = pk_literal_to_rowid(&lit)?;
                    Ok((None, None, None, Some(rowid_increment(&rowid)), None))
                }

                // pk LIKE 'prefix%'
                (
                    SqlBoundBinaryOperator::Like,
                    SqlCatalogBoundExpression::Column(col),
                    SqlCatalogBoundExpression::Literal(lit),
                ) if col.field_id == pk_field_id => {
                    let prefix = match &lit {
                        SqlBoundLiteral::String(s) if s.ends_with('%') && !s.ends_with("%%") => {
                            s[..s.len() - 1].as_bytes().to_vec()
                        }
                        _ => {
                            // LIKE 模式不只是 prefix%，下推成 remaining
                            return Ok((
                                None,
                                None,
                                None,
                                None,
                                Some(SqlCatalogBoundExpression::Binary {
                                    left: Box::new(SqlCatalogBoundExpression::Column(col)),
                                    operator: SqlBoundBinaryOperator::Like,
                                    right: Box::new(SqlCatalogBoundExpression::Literal(lit)),
                                }),
                            ));
                        }
                    };
                    Ok((None, Some(prefix), None, None, None))
                }

                // 其他所有 Binary —— 留在 remaining
                (op, left, right) => Ok((
                    None,
                    None,
                    None,
                    None,
                    Some(SqlCatalogBoundExpression::Binary { left: Box::new(left), operator: op, right: Box::new(right) }),
                )),
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
        SqlBoundLiteral::Integer(value) => Ok(value.to_string().into_bytes()),
        SqlBoundLiteral::Number(text) => Ok(text.as_bytes().to_vec()),
        SqlBoundLiteral::String(text) => Ok(text.as_bytes().to_vec()),
        SqlBoundLiteral::Boolean(value) => Ok((if *value { "1" } else { "0" }).as_bytes().to_vec()),
        SqlBoundLiteral::Null => Err(SqlFrontendError::new(SqlErrorKind::NotNullViolation, "primary key must not be NULL")),
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

/// 无 FROM 投影的默认列名：字面量取字面量文本，函数取大写函数名，
/// 其余表达式退回 debug 形状。驱动探测依赖这些名字（`SELECT 1` 的列名是 `1`）。
fn scalar_projection_label(expr: &SqlCatalogBoundExpression) -> String {
    match expr {
        SqlCatalogBoundExpression::Literal(lit) => match lit {
            SqlBoundLiteral::Integer(value) => value.to_string(),
            SqlBoundLiteral::Number(text) | SqlBoundLiteral::String(text) => text.clone(),
            SqlBoundLiteral::Boolean(value) => if *value { "true" } else { "false" }.to_string(),
            SqlBoundLiteral::Null => "NULL".to_string(),
        },
        SqlCatalogBoundExpression::Function { name, .. } => name.to_ascii_uppercase(),
        other => expression_default_label(other),
    }
}

fn expression_default_label(expr: &SqlCatalogBoundExpression) -> String {
    format!("{expr:?}")
}

/// 表达式里是否出现聚合函数（含嵌套在运算里）。
fn expression_has_aggregate(expr: &SqlCatalogBoundExpression) -> bool {
    match expr {
        SqlCatalogBoundExpression::Function { name, args } => {
            SqlAggregateFunction::from_name(name).is_some() || args.iter().any(expression_has_aggregate)
        }
        SqlCatalogBoundExpression::Binary { left, right, .. } => {
            expression_has_aggregate(left) || expression_has_aggregate(right)
        }
        SqlCatalogBoundExpression::Unary { expression, .. } => expression_has_aggregate(expression),
        SqlCatalogBoundExpression::InSubquery { expression, .. } | SqlCatalogBoundExpression::InValues { expression, .. } => {
            expression_has_aggregate(expression)
        }
        SqlCatalogBoundExpression::Column(_) | SqlCatalogBoundExpression::Literal(_) | SqlCatalogBoundExpression::Wildcard => {
            false
        }
    }
}

fn planner_error(message: &str) -> SqlFrontendError {
    SqlFrontendError::unsupported(message)
}
