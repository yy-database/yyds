//! 用 YYDS 的 `KeyValueExecutor` 执行一个已计划好的 SQL SELECT。
//!
//! 执行流程非常直接：
//!
//! 1. 从 `SqlExecutionPlan` 里拿到 namespace 和 rowid，构造一个 `KeyValueCommand::Get`。
//! 2. 调 `executor.execute_read_batch`（单个 key 也走 batch 以保持原子读接口一致）。
//! 3. 如果 `Get` 返回 `None`（row 不存在），产生空结果。
//! 4. 如果返回 `Some(bytes)`，按 TLV 格式反序列化成 `RowFieldValues`。
//! 5. 用 `eval_expression` 对每个投影求值，再用 `eval_expression` 对 `remaining_predicate` 做
//!    行级过滤（如果有）。
//! 6. 收集列名和行值组成 `SqlResult`。

use std::collections::BTreeMap;

use yyds_execution::{KeyValueExecutor, TransactionHandle};
use yyds_types::{KeyValueAction, KeyValueCommand, Namespace};

use crate::{
    catalog_bind::{collect_column_ids, SqlCatalogBoundExpression},
    sql::{
        SqlAggregateFunction, SqlBoundBinaryOperator, SqlBoundLiteral, SqlBoundOrderDirection, SqlBoundUnaryOperator,
        SqlDialect, SqlErrorKind, SqlFrontendError, SqlNullsOrder, SqlScalarFunction,
    },
    sql_planner::{
        DeletePlan, DeleteScanPlan, JoinPlan, JoinSourcePlan, ReadPlan, ScalarPlan, ScanPlan, SqlExecutionPlan, UpdatePlan,
        UpdateScanPlan, WritePlan,
    },
};

/// SQL 查询的结果集。
#[derive(Debug, Clone, PartialEq)]
pub struct SqlResult {
    /// 列名（按投影顺序）。
    pub columns: Vec<String>,
    /// 行（每行的列值按 `columns` 顺序）。
    pub rows: Vec<Vec<SqlValue>>,
    /// DML 影响行数；SELECT 保持 0。
    pub affected_rows: u64,
}

/// SQL 层面的值域。
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    /// 64 位有符号整数。整数列必须走这个变体，避免 `i64` 经 `f64` 往返时丢失精度。
    Integer(i64),
    /// 非整数数值（浮点字面量、除法、AVG 等）。
    Number(f64),
    /// 字符串。
    String(String),
    /// 布尔。
    Boolean(bool),
    /// NULL。
    Null,
}

/// 解析 SQL 数值字面量：不带小数点或指数的文本优先按 `i64` 精确解析，
/// 只有真正的小数/指数形式才转成 `f64`。
fn numeric_literal(text: &str) -> SqlValue {
    if !text.contains('.') && !text.contains('e') && !text.contains('E') {
        if let Ok(value) = text.parse::<i64>() {
            return SqlValue::Integer(value);
        }
    }
    match text.parse::<f64>() {
        Ok(value) if value.is_finite() => SqlValue::Number(value),
        _ => SqlValue::String(text.to_owned()),
    }
}

impl SqlValue {
    /// 从 KV 层拿到的原始 bytes 推断一个 SqlValue（无类型信息，宽松推断）。
    ///
    /// 如果 bytes 是合法 UTF-8 并且能解析为整数或浮点数，就返回 [`SqlValue::Integer`] /
    /// [`SqlValue::Number`]；否则返回 String。如果上游知道精确类型，应使用
    /// [`Self::from_row_bytes_typed`]。
    pub fn from_row_bytes(bytes: &[u8]) -> Self {
        let text = String::from_utf8_lossy(bytes).into_owned();
        if let Ok(int) = text.parse::<i64>() {
            return SqlValue::Integer(int);
        }
        if let Ok(float) = text.parse::<f64>() {
            if float.is_finite() {
                return SqlValue::Number(float);
            }
        }
        SqlValue::String(text)
    }

    /// 按声明的 `field_type` 精确解码 bytes。
    ///
    /// - `String` 字段永远返回 [`SqlValue::String`]，保留前导零、空串、非数字文本。
    /// - `Int64`/`Int32` 按整数解析。
    /// - `Float64` 按浮点解析。
    /// - `Bool` 识别 `true`/`false`/`1`/`0`。
    /// - `Unknown` / `Bytes` 退回到 [`Self::from_row_bytes`] 的宽松推断。
    ///
    /// 解码失败时降级为 [`SqlValue::String`] 保留原始 UTF-8 文本，
    /// 让上层继续处理而不是 panic。
    pub fn from_row_bytes_typed(bytes: &[u8], field_type: &crate::catalog_bind::SqlFieldType) -> Self {
        let inner = field_type.inner();
        match inner {
            // 字符串类型保留空串语义：空 bytes 是 `''`，不是 NULL。
            crate::catalog_bind::SqlFieldType::String => SqlValue::String(String::from_utf8_lossy(bytes).into_owned()),
            crate::catalog_bind::SqlFieldType::Int64 | crate::catalog_bind::SqlFieldType::Int32 => {
                if bytes.is_empty() {
                    return SqlValue::Null;
                }
                let text = String::from_utf8_lossy(bytes).into_owned();
                text.parse::<i64>().map(SqlValue::Integer).unwrap_or(SqlValue::String(text))
            }
            crate::catalog_bind::SqlFieldType::Float64 => {
                if bytes.is_empty() {
                    return SqlValue::Null;
                }
                let text = String::from_utf8_lossy(bytes).into_owned();
                text.parse::<f64>().ok().filter(|f| f.is_finite()).map(SqlValue::Number).unwrap_or(SqlValue::String(text))
            }
            crate::catalog_bind::SqlFieldType::Bool => {
                if bytes.is_empty() {
                    return SqlValue::Null;
                }
                let text = String::from_utf8_lossy(bytes).into_owned();
                match text.as_str() {
                    "true" | "True" | "TRUE" | "1" => SqlValue::Boolean(true),
                    "false" | "False" | "FALSE" | "0" => SqlValue::Boolean(false),
                    _ => SqlValue::String(text),
                }
            }
            crate::catalog_bind::SqlFieldType::Bytes => SqlValue::String(String::from_utf8_lossy(bytes).into_owned()),
            // 没有类型信息时只能宽松推断。gateway 写入 NULL 时会省略字段，
            // 因此这里把空 bytes 视为空串，真正缺失的字段由调用方映射为 NULL。
            crate::catalog_bind::SqlFieldType::Unknown => Self::from_row_bytes(bytes),
            crate::catalog_bind::SqlFieldType::Optional(_) => unreachable!("inner() strips Optional"),
        }
    }

    /// 从 SQL 字面量构造。
    pub fn from_literal(lit: &SqlBoundLiteral) -> Self {
        match lit {
            SqlBoundLiteral::Integer(value) => SqlValue::Integer(*value),
            SqlBoundLiteral::Number(text) => numeric_literal(text),
            SqlBoundLiteral::String(text) => SqlValue::String(text.clone()),
            SqlBoundLiteral::Boolean(v) => SqlValue::Boolean(*v),
            SqlBoundLiteral::Null => SqlValue::Null,
        }
    }
}

/// 一个 row 上各个字段的 bytes 值，key 是稳定的 field_id。
#[derive(Debug, Default, Clone)]
pub struct RowFieldValues {
    fields: BTreeMap<u64, Vec<u8>>,
}

impl RowFieldValues {
    /// 创建一个空 row。
    pub fn new() -> Self {
        Self { fields: BTreeMap::new() }
    }

    /// 插入/覆盖一个字段值（field_id → bytes）。
    pub fn insert(&mut self, field_id: u64, value: Vec<u8>) {
        self.fields.insert(field_id, value);
    }

    /// 删除一个字段。SQL NULL 在 row 编码里表示为字段缺失，因此
    /// `SET col = NULL` 必须移除已有字段而不是写入空 bytes。
    pub fn remove(&mut self, field_id: u64) {
        self.fields.remove(&field_id);
    }

    /// 按 field_id 取原始 bytes；不存在返回 None。
    pub fn get(&self, field_id: u64) -> Option<&[u8]> {
        self.fields.get(&field_id).map(|v| v.as_slice())
    }

    /// 合并另一行的字段。JOIN 后的一行同时携带两侧表的字段，
    /// catalog 保证 field_id 全局唯一，所以按键覆盖即等价于拼行。
    pub fn merge(&mut self, other: &Self) {
        for (field_id, bytes) in &other.fields {
            self.fields.insert(*field_id, bytes.clone());
        }
    }

    /// 按 `field_type` 精确解码 bytes，保留前导零和空串等字符串语义。
    /// 这是读取字段值的唯一入口，避免在已知 catalog 类型时退回无类型推断。
    pub fn value_typed(
        &self,
        field_id: u64,
        field_types: &std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    ) -> Option<SqlValue> {
        let bytes = self.fields.get(&field_id)?;
        let field_type = field_types.get(&field_id).cloned().unwrap_or(crate::catalog_bind::SqlFieldType::Unknown);
        Some(SqlValue::from_row_bytes_typed(bytes, &field_type))
    }
}

/// TLV row 序列化格式（field_id 大端 u64 + 长度大端 u32 + bytes，字段按 field_id 字典序排列）。
pub fn encode_row(values: &RowFieldValues) -> Vec<u8> {
    let mut out = Vec::new();
    let count = values.fields.len() as u16;
    out.extend_from_slice(&count.to_be_bytes());
    for (field_id, bytes) in &values.fields {
        out.extend_from_slice(&field_id.to_be_bytes());
        let len = bytes.len() as u32;
        out.extend_from_slice(&len.to_be_bytes());
        out.extend_from_slice(bytes);
    }
    out
}

/// 从 TLV 格式反序列化出 RowFieldValues。
pub fn decode_row(data: &[u8]) -> Result<RowFieldValues, SqlFrontendError> {
    let mut cursor = Cursor::new(data);
    let count = cursor.read_u16()?;
    let mut row = RowFieldValues::new();
    for _ in 0..count {
        let field_id = cursor.read_u64()?;
        let len = cursor.read_u32()? as usize;
        let bytes = cursor.read_bytes(len)?;
        row.insert(field_id, bytes);
    }
    Ok(row)
}

/// 部分解码：只收集 needed_ids 里的字段，跳过其他 TLV 条目。
/// 当 needed_ids 为空时表示调用方确实需要整行（例如尚未确定投影）。
pub fn decode_row_fields(data: &[u8], needed_ids: &[u64]) -> Result<RowFieldValues, SqlFrontendError> {
    let mut row = RowFieldValues::new();
    if needed_ids.is_empty() {
        return decode_row(data);
    }
    let needed: std::collections::HashSet<u64> = needed_ids.iter().copied().collect();
    let mut cursor = Cursor::new(data);
    let count = cursor.read_u16()?;
    for _ in 0..count {
        let field_id = cursor.read_u64()?;
        let len = cursor.read_u32()? as usize;
        if needed.contains(&field_id) {
            let bytes = cursor.read_bytes(len)?;
            row.insert(field_id, bytes);
        }
        else {
            // 跳过：手动推进 len 字节。
            let _ = cursor.read_bytes(len)?;
        }
    }
    Ok(row)
}

/// 从 ScanPlan 里收集执行时需要解码的 field_ids（projection + predicate + order_by + expressions）。
fn scan_needed_field_ids(plan: &ScanPlan) -> Vec<u64> {
    let mut ids: Vec<u64> = Vec::new();
    for (fid, _) in &plan.projection_fields {
        if !ids.contains(fid) {
            ids.push(*fid);
        }
    }
    for (expr, _) in &plan.projection_expressions {
        collect_column_ids(expr, &mut ids);
    }
    if let Some(pred) = &plan.remaining_predicate {
        collect_column_ids(pred, &mut ids);
    }
    if let Some(group_exprs) = &plan.group_by {
        for g in group_exprs {
            collect_column_ids(g, &mut ids);
        }
    }
    if let Some(having) = &plan.having {
        collect_column_ids(having, &mut ids);
    }
    if let Some(order_items) = &plan.order_by {
        for item in order_items {
            collect_column_ids(&item.expression, &mut ids);
        }
    }
    ids
}

/// 从 ReadPlan / DeletePlan 的表达式里收集执行时需要解码的 field_ids。
fn expression_needed_field_ids(
    projection_fields: &[(u64, String)],
    projection_expressions: &[(SqlCatalogBoundExpression, String)],
    predicate: Option<&SqlCatalogBoundExpression>,
) -> Vec<u64> {
    let mut ids: Vec<u64> = Vec::new();
    for (fid, _) in projection_fields {
        if !ids.contains(fid) {
            ids.push(*fid);
        }
    }
    for (expr, _) in projection_expressions {
        collect_column_ids(expr, &mut ids);
    }
    if let Some(predicate) = predicate {
        collect_column_ids(predicate, &mut ids);
    }
    ids
}

/// 执行一个已计划的 SQL 语句。
///
/// SELECT 返回单行结果集（或空集）；INSERT/UPDATE/DELETE 返回空结果集，
/// 影响行数记录在 `SqlResult.affected_rows`。
pub fn execute_plan(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &SqlExecutionPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    // 子查询必须在行级求值前先跑完并物化成值列表，因为 eval_expression 只有 row 上下文。
    let mut plan = plan.clone();
    resolve_subqueries(&mut plan, executor, transaction, dialect)?;
    let plan = &plan;
    match plan {
        SqlExecutionPlan::Read(read_plan) => execute_read(executor, transaction, read_plan, dialect),
        SqlExecutionPlan::Scan(scan_plan) => execute_scan(executor, transaction, scan_plan, dialect),
        SqlExecutionPlan::Join(join_plan) => execute_join(executor, transaction, join_plan, dialect),
        SqlExecutionPlan::Scalar(scalar_plan) => execute_scalar(scalar_plan, dialect),
        SqlExecutionPlan::Insert(write_plan) => execute_insert(executor, transaction, write_plan, dialect),
        SqlExecutionPlan::MultiInsert(write_plans) => execute_multi_insert(executor, transaction, write_plans, dialect),
        SqlExecutionPlan::Update(update_plan) => execute_update(executor, transaction, update_plan, dialect),
        SqlExecutionPlan::UpdateScan(update_plan) => execute_update_scan(executor, transaction, update_plan, dialect),
        SqlExecutionPlan::Delete(delete_plan) => execute_delete(executor, transaction, delete_plan, dialect),
        SqlExecutionPlan::DeleteScan(delete_plan) => execute_delete_scan(executor, transaction, delete_plan, dialect),
    }
}

/// 执行计划里所有 `IN (<subquery>)` 并替换成物化后的 `InValues`。
fn resolve_subqueries(
    plan: &mut SqlExecutionPlan,
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    dialect: SqlDialect,
) -> Result<(), SqlFrontendError> {
    match plan {
        SqlExecutionPlan::Read(plan) => {
            lower_optional(&mut plan.remaining_predicate, executor, transaction, dialect)?;
            for (expression, _) in &mut plan.projection_expressions {
                lower_in_place(expression, executor, transaction, dialect)?;
            }
        }
        SqlExecutionPlan::Scan(plan) => {
            lower_optional(&mut plan.remaining_predicate, executor, transaction, dialect)?;
            lower_optional(&mut plan.having, executor, transaction, dialect)?;
            if let Some(group_by) = &mut plan.group_by {
                for expression in group_by {
                    lower_in_place(expression, executor, transaction, dialect)?;
                }
            }
            if let Some(order_by) = &mut plan.order_by {
                for item in order_by {
                    lower_in_place(&mut item.expression, executor, transaction, dialect)?;
                }
            }
            for (expression, _) in &mut plan.projection_expressions {
                lower_in_place(expression, executor, transaction, dialect)?;
            }
        }
        SqlExecutionPlan::Join(plan) => {
            lower_optional(&mut plan.base.on, executor, transaction, dialect)?;
            for join in &mut plan.joins {
                lower_optional(&mut join.on, executor, transaction, dialect)?;
            }
            lower_optional(&mut plan.remaining_predicate, executor, transaction, dialect)?;
            if let Some(order_by) = &mut plan.order_by {
                for item in order_by {
                    lower_in_place(&mut item.expression, executor, transaction, dialect)?;
                }
            }
            for (expression, _) in &mut plan.projection_expressions {
                lower_in_place(expression, executor, transaction, dialect)?;
            }
        }
        SqlExecutionPlan::Scalar(plan) => {
            for (expression, _) in &mut plan.projections {
                lower_in_place(expression, executor, transaction, dialect)?;
            }
        }
        SqlExecutionPlan::Insert(_) | SqlExecutionPlan::MultiInsert(_) => {}
        SqlExecutionPlan::Update(plan) => {
            lower_optional(&mut plan.remaining_predicate, executor, transaction, dialect)?;
        }
        SqlExecutionPlan::UpdateScan(plan) => {
            lower_optional(&mut plan.predicate, executor, transaction, dialect)?;
        }
        SqlExecutionPlan::Delete(plan) => {
            lower_optional(&mut plan.remaining_predicate, executor, transaction, dialect)?;
        }
        SqlExecutionPlan::DeleteScan(plan) => {
            lower_optional(&mut plan.predicate, executor, transaction, dialect)?;
        }
    }
    Ok(())
}

fn lower_optional(
    slot: &mut Option<SqlCatalogBoundExpression>,
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    dialect: SqlDialect,
) -> Result<(), SqlFrontendError> {
    if let Some(expression) = slot.take() {
        *slot = Some(lower_expression(expression, executor, transaction, dialect)?);
    }
    Ok(())
}

fn lower_in_place(
    slot: &mut SqlCatalogBoundExpression,
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    dialect: SqlDialect,
) -> Result<(), SqlFrontendError> {
    let placeholder = SqlCatalogBoundExpression::Literal(SqlBoundLiteral::Null);
    let expression = std::mem::replace(slot, placeholder);
    *slot = lower_expression(expression, executor, transaction, dialect)?;
    Ok(())
}

/// 递归物化子查询：内层计划先执行，结果必须是单列，再转成 `InValues`。
fn lower_expression(
    expression: SqlCatalogBoundExpression,
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    dialect: SqlDialect,
) -> Result<SqlCatalogBoundExpression, SqlFrontendError> {
    Ok(match expression {
        SqlCatalogBoundExpression::Column(_) | SqlCatalogBoundExpression::Literal(_) | SqlCatalogBoundExpression::Wildcard => {
            expression
        }
        SqlCatalogBoundExpression::Function { name, args, distinct } => SqlCatalogBoundExpression::Function {
            name,
            args: args.into_iter().map(|arg| lower_expression(arg, executor, transaction, dialect)).collect::<Result<Vec<_>, _>>()?,
            distinct,
        },
        SqlCatalogBoundExpression::Binary { left, operator, right } => SqlCatalogBoundExpression::Binary {
            left: Box::new(lower_expression(*left, executor, transaction, dialect)?),
            operator,
            right: Box::new(lower_expression(*right, executor, transaction, dialect)?),
        },
        SqlCatalogBoundExpression::Unary { operator, expression } => SqlCatalogBoundExpression::Unary {
            operator,
            expression: Box::new(lower_expression(*expression, executor, transaction, dialect)?),
        },
        SqlCatalogBoundExpression::InValues { expression, values, negated } => SqlCatalogBoundExpression::InValues {
            expression: Box::new(lower_expression(*expression, executor, transaction, dialect)?),
            values,
            negated,
        },
        SqlCatalogBoundExpression::InSubquery { expression, plan, negated } => {
            let expression = Box::new(lower_expression(*expression, executor, transaction, dialect)?);
            let mut plan = *plan;
            resolve_subqueries(&mut plan, executor, transaction, dialect)?;
            let result = execute_plan(executor, transaction, &plan, dialect)?;
            if result.columns.len() != 1 {
                return Err(execute_error("IN subquery must select exactly one column"));
            }
            let values = result
                .rows
                .into_iter()
                .map(|row| row.into_iter().next().map(value_to_literal).unwrap_or(SqlBoundLiteral::Null))
                .collect();
            SqlCatalogBoundExpression::InValues { expression, values, negated }
        }
    })
}

/// 把子查询结果值换回 bound literal，让物化后的表达式保持可比较、可复用。
fn value_to_literal(value: SqlValue) -> SqlBoundLiteral {
    match value {
        SqlValue::Integer(value) => SqlBoundLiteral::Integer(value),
        SqlValue::Number(value) => SqlBoundLiteral::Number(format_number(value)),
        SqlValue::String(value) => SqlBoundLiteral::String(value),
        SqlValue::Boolean(value) => SqlBoundLiteral::Boolean(value),
        SqlValue::Null => SqlBoundLiteral::Null,
    }
}

/// 执行无 FROM 的 SELECT：对常量与标量内建函数求值，产出一行结果。
fn execute_scalar(plan: &ScalarPlan, dialect: SqlDialect) -> Result<SqlResult, SqlFrontendError> {
    let columns: Vec<String> = plan.projections.iter().map(|(_, label)| label.clone()).collect();
    if plan.suppress_row {
        return Ok(SqlResult { columns, rows: Vec::new(), affected_rows: 0 });
    }
    let row_values = RowFieldValues::new();
    let field_types = BTreeMap::new();
    let mut row = Vec::with_capacity(plan.projections.len());
    for (expression, _) in &plan.projections {
        row.push(eval_expression(expression, &row_values, &field_types, dialect)?);
    }
    Ok(SqlResult { columns, rows: vec![row], affected_rows: 0 })
}

fn execute_read(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &ReadPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    if plan.suppress_rows {
        return Ok(SqlResult { columns: read_column_labels(plan), rows: Vec::new(), affected_rows: 0 });
    }
    let namespace = Namespace(plan.namespace.clone());
    let command = KeyValueCommand { namespace, key: plan.rowid.clone(), action: KeyValueAction::Get };
    let batch_result = match transaction {
        Some(handle) => handle
            .execute_read_batch(&[command.clone()])
            .map_err(|e| execute_error(&format!("TransactionHandle.read_batch failed: {e}")))?,
        None => executor
            .execute_read_batch(&[command])
            .map_err(|e| execute_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?,
    };
    let result = batch_result.into_iter().next().ok_or_else(|| execute_error("KeyValueExecutor returned empty batch"))?;
    let row_bytes = match result {
        yyds_types::KeyValueResult::Get(Some(bytes)) => bytes,
        _ => return Ok(SqlResult { columns: read_column_labels(plan), rows: Vec::new(), affected_rows: 0 }),
    };
    // 只解码本次读取实际需要的字段；值仍由 `value_typed` 按 catalog 类型解释。
    let needed_ids =
        expression_needed_field_ids(&plan.projection_fields, &plan.projection_expressions, plan.remaining_predicate.as_ref());
    let row = decode_row_fields(&row_bytes, &needed_ids)?;

    if let Some(ref predicate) = plan.remaining_predicate {
        let v = eval_expression(predicate, &row,&plan.field_types, dialect)?;
        if !truthy(&v) {
            return Ok(SqlResult { columns: read_column_labels(plan), rows: Vec::new(), affected_rows: 0 });
        }
    }

    let columns = read_column_labels(plan);
    let mut row_values: Vec<SqlValue> = Vec::new();
    for (field_id, _) in &plan.projection_fields {
        row_values.push(row.value_typed(*field_id, &plan.field_types).unwrap_or(SqlValue::Null));
    }
    for (expr, _) in &plan.projection_expressions {
        row_values.push(eval_expression(expr, &row,&plan.field_types, dialect)?);
    }
    Ok(SqlResult { columns, rows: vec![row_values], affected_rows: 0 })
}

/// 执行一个多表 JOIN 计划：每个来源先整表扫描，再按 JOIN 顺序做嵌套循环合并。
///
/// INNER JOIN 只保留两侧匹配的组合；LEFT JOIN 额外保留左侧未匹配行（右侧字段缺失 → NULL）。
fn execute_join(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &JoinPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let columns = join_column_labels(plan);
    if plan.suppress_rows {
        return Ok(SqlResult { columns, rows: Vec::new(), affected_rows: 0 });
    }
    let mut rows = scan_join_source(executor, transaction, &plan.base)?;
    for source in &plan.joins {
        let right_rows = scan_join_source(executor, transaction, source)?;
        let is_left = matches!(source.kind, crate::sql::SqlBoundJoinKind::Left);
        let mut merged = Vec::new();
        for left in &rows {
            let mut matched_any = false;
            for right in &right_rows {
                let mut row = left.clone();
                row.merge(right);
                if let Some(on) = &source.on {
                    if !truthy(&eval_expression(on, &row, &plan.field_types, dialect)?) {
                        continue;
                    }
                }
                merged.push(row);
                matched_any = true;
            }
            if is_left && !matched_any {
                // LEFT JOIN: 左行未匹配任何右行时仍然保留，右表字段缺失 → NULL
                merged.push(left.clone());
            }
        }
        rows = merged;
    }

    let mut staged: Vec<(Vec<Option<SqlValue>>, Vec<SqlValue>)> = Vec::new();
    for row in &rows {
        if let Some(predicate) = &plan.remaining_predicate {
            if !truthy(&eval_expression(predicate, row,&plan.field_types, dialect)?) {
                continue;
            }
        }
        let sort_keys: Vec<Option<SqlValue>> = match &plan.order_by {
            Some(items) => items
                .iter()
                .map(|item| Ok(Some(eval_expression(&item.expression, row,&plan.field_types, dialect)?)))
                .collect::<Result<Vec<_>, _>>()?,
            None => Vec::new(),
        };
        let mut row_values: Vec<SqlValue> = Vec::new();
        for (field_id, _) in &plan.projection_fields {
            row_values.push(row.value_typed(*field_id, &plan.field_types).unwrap_or(SqlValue::Null));
        }
        for (expr, _) in &plan.projection_expressions {
            row_values.push(eval_expression(expr, row,&plan.field_types, dialect)?);
        }
        staged.push((sort_keys, row_values));
    }
    if plan.distinct {
        retain_distinct_projected_rows(&mut staged);
    }

    let nulls_first = plan.nulls_first;
    if let Some(order_items) = &plan.order_by {
        let orderings = order_items
            .iter()
            .map(|item| (item.direction.clone(), order_nulls_first(&item.direction, item.nulls, nulls_first)))
            .collect::<Vec<_>>();
        staged.sort_by(move |a, b| {
            for (idx, (dir, nulls_first)) in orderings.iter().enumerate() {
                let ord = compare_values_with_nulls(a.0[idx].as_ref(), b.0[idx].as_ref(), *nulls_first);
                if ord != std::cmp::Ordering::Equal {
                    return match dir {
                        crate::sql::SqlBoundOrderDirection::Asc => ord,
                        crate::sql::SqlBoundOrderDirection::Desc => ord.reverse(),
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    let offset = plan.offset_after_sort;
    let limit = plan.limit_after_sort;
    let rows: Vec<Vec<SqlValue>> =
        staged.into_iter().skip(offset).take(if limit == 0 { usize::MAX } else { limit }).map(|(_, r)| r).collect();
    Ok(SqlResult { columns, rows, affected_rows: 0 })
}

/// 整表扫描一个 JOIN 来源，只解码该表参与的字段。
fn scan_join_source(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    source: &JoinSourcePlan,
) -> Result<Vec<RowFieldValues>, SqlFrontendError> {
    let namespace = Namespace(source.namespace.clone());
    let scanned = scan_rows(executor, transaction, &namespace, &[], None, None, 0, "scan")?;
    let mut rows = Vec::with_capacity(scanned.len());
    for (_rowid, value_bytes) in scanned {
        rows.push(decode_row_fields(&value_bytes, &source.needed_field_ids)?);
    }
    Ok(rows)
}

/// 扫描一行 namespace。事务存在时必须走事务快照，否则同一事务内的写入对 SELECT 不可见。
fn scan_rows(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    namespace: &Namespace,
    prefix: &[u8],
    after: Option<&[u8]>,
    before: Option<&[u8]>,
    limit: usize,
    operation: &str,
) -> Result<Vec<(Vec<u8>, Vec<u8>)>, SqlFrontendError> {
    match transaction {
        Some(handle) => handle
            .execute_scan(namespace, prefix, after, before, limit)
            .map_err(|e| execute_error(&format!("TransactionHandle.{operation} failed: {e}"))),
        None => executor
            .execute_scan(namespace, prefix, after, before, limit)
            .map_err(|e| execute_error(&format!("KeyValueExecutor.{operation} failed: {e}"))),
    }
}

fn execute_scan(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &ScanPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let columns = scan_column_labels(plan);
    if plan.suppress_rows {
        return Ok(SqlResult { columns, rows: Vec::new(), affected_rows: 0 });
    }
    let namespace = Namespace(plan.namespace.clone());
    let scanned = scan_rows(
        executor,
        transaction,
        &namespace,
        &plan.rowid_prefix,
        plan.rowid_after.as_deref(),
        plan.rowid_before.as_deref(),
        plan.limit,
        "scan",
    )?;

    let needed_ids = scan_needed_field_ids(plan);

    // Top-N heap 优化：ORDER BY + (LIMIT 或 OFFSET) 时，用 BinaryHeap 保留 Top-N，
    // 避免先收集全量 rows 再排序。容量 = offset + limit（limit=0 视为无限，fallback 到 Vec）。
    let use_top_n = plan.order_by.is_some() && (plan.limit_after_sort > 0 || plan.offset_after_sort > 0);

    if plan.has_aggregate && plan.group_by.is_none() {
        // —— 全局聚合路径（无 GROUP BY）：需要访问所有 post-filter rows，无 heap 收益 ——
        let mut all_rows: Vec<RowFieldValues> = Vec::new();
        for (_rowid, value_bytes) in scanned {
            let row = decode_row_fields(&value_bytes, &needed_ids)?;
            if let Some(ref predicate) = plan.remaining_predicate {
                let v = eval_expression(predicate, &row,&plan.field_types, dialect)?;
                if !truthy(&v) {
                    continue;
                }
            }
            all_rows.push(row);
        }
        let mut result_row: Vec<SqlValue> = Vec::new();
        for (expr, _) in &plan.projection_expressions {
            result_row.push(eval_aggregate(expr, &all_rows, &plan.field_types, dialect)?);
        }
        if let Some(having) = &plan.having {
            let keep = eval_having(having, &all_rows, &[], &plan.field_types, dialect)?;
            if !truthy(&keep) {
                return Ok(SqlResult { columns, rows: Vec::new(), affected_rows: 0 });
            }
        }
        return Ok(SqlResult { columns, rows: vec![result_row], affected_rows: 0 });
    }

    if let Some(group_exprs) = &plan.group_by {
        // —— GROUP BY 路径：扫描后 bucket + 每 bucket 聚合 + HAVING + ORDER BY + LIMIT ——
        let mut all_rows: Vec<RowFieldValues> = Vec::new();
        for (_rowid, value_bytes) in scanned {
            let row = decode_row_fields(&value_bytes, &needed_ids)?;
            if let Some(ref predicate) = plan.remaining_predicate {
                let v = eval_expression(predicate, &row,&plan.field_types, dialect)?;
                if !truthy(&v) {
                    continue;
                }
            }
            all_rows.push(row);
        }

        // Bucket: rows → Vec<(group_key, rows)>
        let mut buckets: std::collections::BTreeMap<String, (Vec<SqlValue>, Vec<RowFieldValues>)> =
            std::collections::BTreeMap::new();
        for row in &all_rows {
            let key_vals: Vec<SqlValue> =
                group_exprs.iter().map(|e| eval_expression(e, row,&plan.field_types, dialect)).collect::<Result<Vec<_>, _>>()?;
            let key_str = serialize_group_key(&key_vals);
            buckets.entry(key_str).or_insert_with(|| (key_vals, Vec::new())).1.push(row.clone());
        }

        // 对每个 bucket 计算结果行
        let mut staged: Vec<(Vec<Option<SqlValue>>, Vec<SqlValue>)> = Vec::new();

        for (_key_str, (group_key_vals, bucket_rows)) in &buckets {
            let bucket_rows = bucket_rows;

            // projection_fields: 只允许分组列，值直接取自 group key
            let mut row_values: Vec<SqlValue> = Vec::new();
            for (field_id, _display_name) in &plan.projection_fields {
                let mut matched_key_idx: Option<usize> = None;
                for (idx, ge) in group_exprs.iter().enumerate() {
                    if let SqlCatalogBoundExpression::Column(col) = ge {
                        if col.field_id == *field_id {
                            matched_key_idx = Some(idx);
                            break;
                        }
                    }
                }
                let Some(idx) = matched_key_idx
                else {
                    return Err(execute_error("grouped projection references a column outside the GROUP BY key"));
                };
                row_values.push(group_key_vals[idx].clone());
            }
            // projection_expressions — eval_having 支持嵌套聚合与分组列上的标量函数
            for (expr, _) in &plan.projection_expressions {
                row_values.push(eval_having(expr, bucket_rows, group_exprs, &plan.field_types, dialect)?);
            }

            // HAVING — 用 eval_having 处理聚合函数引用
            if let Some(having) = &plan.having {
                let v = eval_having(having, bucket_rows, group_exprs, &plan.field_types, dialect)?;
                if !truthy(&v) {
                    continue;
                }
            }

            // ORDER BY sort_keys — 可以引用 group key 或聚合列
            let sort_keys: Vec<Option<SqlValue>> = match &plan.order_by {
                Some(items) => items
                    .iter()
                    .map(|item| Ok(Some(eval_having(&item.expression, bucket_rows, group_exprs, &plan.field_types, dialect)?)))
                    .collect::<Result<Vec<_>, _>>()?,
                None => Vec::new(),
            };
            staged.push((sort_keys, row_values));
        }

        // Sort if ORDER BY present — multi-column 逐列比较
        let nulls_first = plan.nulls_first;
        if let Some(order_items) = &plan.order_by {
            let orderings = order_items
                .iter()
                .map(|item| (item.direction.clone(), order_nulls_first(&item.direction, item.nulls, nulls_first)))
                .collect::<Vec<_>>();
            staged.sort_by(move |a, b| {
                for (idx, (dir, nulls_first)) in orderings.iter().enumerate() {
                    let ord = compare_values_with_nulls(a.0[idx].as_ref(), b.0[idx].as_ref(), *nulls_first);
                    if ord != std::cmp::Ordering::Equal {
                        return match dir {
                            crate::sql::SqlBoundOrderDirection::Asc => ord,
                            crate::sql::SqlBoundOrderDirection::Desc => ord.reverse(),
                        };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        let offset = plan.offset_after_sort;
        let limit = plan.limit_after_sort;
        let rows: Vec<Vec<SqlValue>> =
            staged.into_iter().skip(offset).take(if limit == 0 { usize::MAX } else { limit }).map(|(_, r)| r).collect();
        return Ok(SqlResult { columns, rows, affected_rows: 0 });
    }

    if false && use_top_n {
        // —— Top-N heap 路径（仅单列 ORDER BY，多列已走 Vec 路径） ——
        let item = plan.order_by.as_ref().unwrap().first().unwrap();
        let order_expr = &item.expression;
        let is_asc = matches!(&item.direction, SqlBoundOrderDirection::Asc);
        let heap_capacity = plan.offset_after_sort.max(1) + plan.limit_after_sort;
        let mut heap: std::collections::BinaryHeap<SortableRow> = std::collections::BinaryHeap::with_capacity(heap_capacity);

        for (_rowid, value_bytes) in scanned {
            let row = decode_row_fields(&value_bytes, &needed_ids)?;
            if let Some(ref predicate) = plan.remaining_predicate {
                let v = eval_expression(predicate, &row,&plan.field_types, dialect)?;
                if !truthy(&v) {
                    continue;
                }
            }

            let sort_key = Some(eval_expression(order_expr, &row,&plan.field_types, dialect)?);
            let mut row_values: Vec<SqlValue> = Vec::new();
            for (field_id, _) in &plan.projection_fields {
                row_values.push(row.value_typed(*field_id, &plan.field_types).unwrap_or(SqlValue::Null));
            }
            for (expr, _) in &plan.projection_expressions {
                row_values.push(eval_expression(expr, &row,&plan.field_types, dialect)?);
            }

            heap.push(SortableRow { sort_key, row_values, is_asc });
            if heap.len() > heap_capacity {
                heap.pop(); // 丢掉 heap 顶（ASC 时是最大的，DESC 时是最小的）
            }
        }

        // into_sorted_vec() 按 Ord Greater 优先（即 heap 顶先）返回降序 → reverse 后变成我们要的排序
        let rows: Vec<Vec<SqlValue>> = heap
            .into_sorted_vec()
            .into_iter()
            .rev()
            .skip(plan.offset_after_sort)
            .take(if plan.limit_after_sort == 0 { usize::MAX } else { plan.limit_after_sort })
            .map(|sr| sr.row_values)
            .collect();
        return Ok(SqlResult { columns, rows, affected_rows: 0 });
    }

    // —— 常规 Vec 路径：无 ORDER BY（kv-scan 顺序）或 ORDER BY 但无 LIMIT/OFFSET ——
    let mut staged: Vec<(Vec<Option<SqlValue>>, Vec<SqlValue>)> = Vec::new();
    for (_rowid, value_bytes) in scanned {
        let row = decode_row_fields(&value_bytes, &needed_ids)?;
        if let Some(ref predicate) = plan.remaining_predicate {
            let v = eval_expression(predicate, &row,&plan.field_types, dialect)?;
            if !truthy(&v) {
                continue;
            }
        }

        let sort_keys: Vec<Option<SqlValue>> = match &plan.order_by {
            Some(items) => items
                .iter()
                .map(|item| Ok(Some(eval_expression(&item.expression, &row,&plan.field_types, dialect)?)))
                .collect::<Result<Vec<_>, _>>()?,
            None => Vec::new(),
        };
        let mut row_values: Vec<SqlValue> = Vec::new();
        for (field_id, _) in &plan.projection_fields {
            row_values.push(row.value_typed(*field_id, &plan.field_types).unwrap_or(SqlValue::Null));
        }
        for (expr, _) in &plan.projection_expressions {
            row_values.push(eval_expression(expr, &row,&plan.field_types, dialect)?);
        }
        staged.push((sort_keys, row_values));
    }
    if plan.distinct {
        retain_distinct_projected_rows(&mut staged);
    }

    // Sort if ORDER BY present — multi-column, 逐列比较直到分出胜负。
    let nulls_first = plan.nulls_first;
    if let Some(order_items) = &plan.order_by {
        let orderings = order_items
            .iter()
            .map(|item| (item.direction.clone(), order_nulls_first(&item.direction, item.nulls, nulls_first)))
            .collect::<Vec<_>>();
        staged.sort_by(move |a, b| {
            for (idx, (dir, nulls_first)) in orderings.iter().enumerate() {
                let ord = compare_values_with_nulls(a.0[idx].as_ref(), b.0[idx].as_ref(), *nulls_first);
                if ord != std::cmp::Ordering::Equal {
                    return match dir {
                        crate::sql::SqlBoundOrderDirection::Asc => ord,
                        crate::sql::SqlBoundOrderDirection::Desc => ord.reverse(),
                    };
                }
            }
            std::cmp::Ordering::Equal
        });
    }

    let offset = plan.offset_after_sort;
    let limit = plan.limit_after_sort;
    let rows: Vec<Vec<SqlValue>> =
        staged.into_iter().skip(offset).take(if limit == 0 { usize::MAX } else { limit }).map(|(_, r)| r).collect();

    Ok(SqlResult { columns, rows, affected_rows: 0 })
}

/// 执行层内部：Top-N heap 用的可排序行。
/// Ord 语义：`is_asc` → sort_key 越大 Greater；`!is_asc` → sort_key 越小 Greater。
/// BinaryHeap（max-heap）会把 Greater 的堆顶 → ASC heap 顶是最大的（被 pop 掉，保留小的 N 个），
/// DESC heap 顶是最小的（被 pop 掉，保留大的 N 个）。
#[derive(Debug)]
struct SortableRow {
    sort_key: Option<SqlValue>,
    row_values: Vec<SqlValue>,
    is_asc: bool,
}

impl PartialEq for SortableRow {
    fn eq(&self, other: &Self) -> bool {
        matches!(compare_values(self.sort_key.as_ref(), other.sort_key.as_ref()), std::cmp::Ordering::Equal)
    }
}
impl Eq for SortableRow {}

impl PartialOrd for SortableRow {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortableRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let natural = compare_values(self.sort_key.as_ref(), other.sort_key.as_ref());
        if self.is_asc {
            natural
        }
        else {
            natural.reverse()
        }
    }
}

fn execute_insert(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &WritePlan,
    _dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let row = write_plan_to_row(plan);
    let bytes = encode_row(&row);
    let namespace = Namespace(plan.namespace.clone());
    let action = KeyValueAction::PutIfAbsent(bytes);
    let command = KeyValueCommand { namespace, key: plan.rowid.clone(), action };
    let result = match transaction {
        Some(handle) => handle.execute(command).map_err(|e| execute_error(&format!("TransactionHandle.insert failed: {e}")))?,
        None => executor.execute(command).map_err(|e| execute_error(&format!("KeyValueExecutor.insert failed: {e}")))?,
    };
    let affected = match result {
        yyds_types::KeyValueResult::PutIfAbsent { revision: Some(_) } => 1,
        yyds_types::KeyValueResult::PutIfAbsent { revision: None } => {
            return Err(SqlFrontendError::new(
                SqlErrorKind::UniqueViolation,
                format!("duplicate key value violates unique constraint: {}", plan.namespace),
            ));
        }
        _ => 0,
    };
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: affected })
}

fn execute_multi_insert(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plans: &[WritePlan],
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let mut total_affected = 0u64;
    for plan in plans {
        let single = execute_insert(executor, transaction, plan, dialect)?;
        total_affected = total_affected.saturating_add(single.affected_rows);
    }
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: total_affected })
}

fn execute_update(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &UpdatePlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());
    let get_cmd = KeyValueCommand { namespace: namespace.clone(), key: plan.rowid.clone(), action: KeyValueAction::Get };
    let batch_result = match transaction {
        Some(handle) => handle
            .execute_read_batch(&[get_cmd.clone()])
            .map_err(|e| execute_error(&format!("TransactionHandle.read_batch failed: {e}")))?,
        None => executor
            .execute_read_batch(&[get_cmd])
            .map_err(|e| execute_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?,
    };
    let existing_bytes = match batch_result.into_iter().next() {
        Some(yyds_types::KeyValueResult::Get(Some(b))) => b,
        _ => return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 0 }),
    };
    // UPDATE 必须保留未赋值的列，因此这里解码完整 TLV；字段值仍按
    // `plan.field_types` 走类型化解释，不会退回无类型猜测。
    let mut row = decode_row(&existing_bytes)?;

    // 先评估 remaining_predicate —— 不通过就不写，影响 0 行。
    if let Some(ref predicate) = plan.remaining_predicate {
        let v = eval_expression(predicate, &row,&plan.field_types, dialect)?;
        if !truthy(&v) {
            return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 0 });
        }
    }

    for field in &plan.fields {
        match sql_value_to_bytes(&field.value) {
            Some(bytes) => row.insert(field.field_id, bytes),
            None => row.remove(field.field_id),
        }
    }
    let encoded = encode_row(&row);
    let put_cmd = KeyValueCommand { namespace, key: plan.rowid.clone(), action: KeyValueAction::Put(encoded) };
    let put_result = match transaction {
        // 事务内 UPDATE 是 read-modify-write：把读到的 pre-image 交给 OCC，
        // 另一个事务在此期间提交同一行时 COMMIT 会被拒绝，而不是静默覆盖。
        Some(handle) => handle
            .write_if_unchanged(put_cmd, Some(existing_bytes))
            .map_err(|e| write_error(format!("TransactionHandle.update failed: {e}"), &e))?,
        None => executor.execute(put_cmd).map_err(|e| execute_error(&format!("KeyValueExecutor.update failed: {e}")))?,
    };
    let _ = put_result;
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 1 })
}

fn execute_delete(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &DeletePlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());

    // 事务内 DELETE 也要捕获 pre-image 供 OCC 校验；有 remaining_predicate 时还必须
    // 先 Get 拿到行评估，不通过就不删。
    let pre_image = if transaction.is_some() || plan.remaining_predicate.is_some() {
        let get_cmd = KeyValueCommand { namespace: namespace.clone(), key: plan.rowid.clone(), action: KeyValueAction::Get };
        let batch_result = match transaction {
            Some(handle) => handle
                .execute_read_batch(&[get_cmd.clone()])
                .map_err(|e| execute_error(&format!("TransactionHandle.read_batch failed: {e}")))?,
            None => executor
                .execute_read_batch(&[get_cmd])
                .map_err(|e| execute_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?,
        };
        let existing_bytes = match batch_result.into_iter().next() {
            Some(yyds_types::KeyValueResult::Get(Some(b))) => b,
            // 行已不存在 — 也是 0 影响行
            _ => return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 0 }),
        };
        if let Some(ref predicate) = plan.remaining_predicate {
            let needed_ids = expression_needed_field_ids(&[], &[], Some(predicate));
            let row = decode_row_fields(&existing_bytes, &needed_ids)?;
            let v = eval_expression(predicate, &row,&plan.field_types, dialect)?;
            if !truthy(&v) {
                return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 0 });
            }
        }
        Some(existing_bytes)
    }
    else {
        None
    };

    let action = KeyValueAction::Delete;
    let command = KeyValueCommand { namespace, key: plan.rowid.clone(), action };
    let delete_result = match transaction {
        // 事务内 DELETE 同样交给 OCC：pre-image 已被并发事务改写时 COMMIT 被拒绝。
        Some(handle) => handle
            .write_if_unchanged(command, pre_image)
            .map_err(|e| write_error(format!("TransactionHandle.delete failed: {e}"), &e))?,
        None => executor.execute(command).map_err(|e| execute_error(&format!("KeyValueExecutor.delete failed: {e}")))?,
    };
    let affected = match delete_result {
        yyds_types::KeyValueResult::Delete { removed: true } => 1,
        _ => 0,
    };
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: affected })
}

/// 多行 UPDATE：扫描 rowid 区间，逐行判定 WHERE 后改写，返回真实影响行数。
///
/// 事务内每行都把扫描到的 pre-image 交给 `write_if_unchanged`，保持上一轮的 OCC 语义。
fn execute_update_scan(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &UpdateScanPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());
    let scanned = scan_rows(
        executor,
        transaction,
        &namespace,
        &plan.rowid_prefix,
        plan.rowid_after.as_deref(),
        plan.rowid_before.as_deref(),
        0,
        "update_scan",
    )?;
    let mut affected = 0;
    for (rowid, pre_image) in scanned {
        let mut row = decode_row(&pre_image)?;
        if let Some(predicate) = &plan.predicate {
            let value = eval_expression(predicate, &row,&plan.field_types, dialect)?;
            if !truthy(&value) {
                continue;
            }
        }
        for field in &plan.fields {
            match sql_value_to_bytes(&field.value) {
                Some(bytes) => row.insert(field.field_id, bytes),
                None => row.remove(field.field_id),
            }
        }
        let command =
            KeyValueCommand { namespace: namespace.clone(), key: rowid, action: KeyValueAction::Put(encode_row(&row)) };
        match transaction {
            Some(handle) => handle
                .write_if_unchanged(command, Some(pre_image))
                .map_err(|e| write_error(format!("TransactionHandle.update_scan failed: {e}"), &e))?,
            None => {
                executor.execute(command).map_err(|e| execute_error(&format!("KeyValueExecutor.update_scan failed: {e}")))?
            }
        };
        affected += 1;
    }
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: affected })
}

/// 多行 DELETE：扫描 rowid 区间，逐行判定 WHERE 后删除，返回真实影响行数。
fn execute_delete_scan(
    executor: &dyn KeyValueExecutor,
    transaction: Option<&dyn TransactionHandle>,
    plan: &DeleteScanPlan,
    dialect: SqlDialect,
) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());
    let scanned = scan_rows(
        executor,
        transaction,
        &namespace,
        &plan.rowid_prefix,
        plan.rowid_after.as_deref(),
        plan.rowid_before.as_deref(),
        0,
        "delete_scan",
    )?;
    let mut affected = 0;
    for (rowid, pre_image) in scanned {
        if let Some(predicate) = &plan.predicate {
            let row = decode_row(&pre_image)?;
            let value = eval_expression(predicate, &row,&plan.field_types, dialect)?;
            if !truthy(&value) {
                continue;
            }
        }
        let command = KeyValueCommand { namespace: namespace.clone(), key: rowid, action: KeyValueAction::Delete };
        let result = match transaction {
            // 事务内逐行删除同样走 OCC：pre-image 被并发改写时 COMMIT 被拒绝。
            Some(handle) => handle
                .write_if_unchanged(command, Some(pre_image))
                .map_err(|e| write_error(format!("TransactionHandle.delete_scan failed: {e}"), &e))?,
            None => {
                executor.execute(command).map_err(|e| execute_error(&format!("KeyValueExecutor.delete_scan failed: {e}")))?
            }
        };
        if matches!(result, yyds_types::KeyValueResult::Delete { removed: true }) {
            affected += 1;
        }
    }
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: affected })
}

fn write_plan_to_row(plan: &WritePlan) -> RowFieldValues {
    let mut row = RowFieldValues::new();
    for field in &plan.fields {
        // NULL 不写入 TLV：字段缺失就是 NULL，空 bytes 留给真正的空串。
        if let Some(bytes) = sql_value_to_bytes(&field.value) {
            row.insert(field.field_id, bytes);
        }
    }
    row
}

fn sql_value_to_bytes(lit: &SqlBoundLiteral) -> Option<Vec<u8>> {
    match lit {
        SqlBoundLiteral::Integer(value) => Some(value.to_string().into_bytes()),
        SqlBoundLiteral::Number(text) => Some(text.as_bytes().to_vec()),
        SqlBoundLiteral::String(text) => Some(text.as_bytes().to_vec()),
        SqlBoundLiteral::Boolean(v) => Some((if *v { "1" } else { "0" }).as_bytes().to_vec()),
        SqlBoundLiteral::Null => None,
    }
}

fn read_column_labels(plan: &ReadPlan) -> Vec<String> {
    let mut labels = Vec::new();
    for (_, name) in &plan.projection_fields {
        labels.push(name.clone());
    }
    for (_, name) in &plan.projection_expressions {
        labels.push(name.clone());
    }
    labels
}

fn scan_column_labels(plan: &ScanPlan) -> Vec<String> {
    let mut labels = Vec::new();
    for (_, name) in &plan.projection_fields {
        labels.push(name.clone());
    }
    for (_, name) in &plan.projection_expressions {
        labels.push(name.clone());
    }
    labels
}

fn join_column_labels(plan: &JoinPlan) -> Vec<String> {
    let mut labels = Vec::new();
    for (_, name) in &plan.projection_fields {
        labels.push(name.clone());
    }
    for (_, name) in &plan.projection_expressions {
        labels.push(name.clone());
    }
    labels
}

fn truthy(v: &SqlValue) -> bool {
    match v {
        SqlValue::Boolean(b) => *b,
        SqlValue::Integer(n) => *n != 0,
        SqlValue::Number(n) => *n != 0.0,
        SqlValue::String(s) => !s.is_empty(),
        SqlValue::Null => false,
    }
}

/// SQL 三值逻辑里的真值：`NULL` 既不是 true 也不是 false。
fn sql_truth(v: &SqlValue) -> Option<bool> {
    match v {
        SqlValue::Null => None,
        other => Some(truthy(other)),
    }
}

fn sql_and(left: &SqlValue, right: &SqlValue) -> SqlValue {
    match (sql_truth(left), sql_truth(right)) {
        (Some(false), _) | (_, Some(false)) => SqlValue::Boolean(false),
        (Some(true), Some(true)) => SqlValue::Boolean(true),
        _ => SqlValue::Null,
    }
}

fn sql_or(left: &SqlValue, right: &SqlValue) -> SqlValue {
    match (sql_truth(left), sql_truth(right)) {
        (Some(true), _) | (_, Some(true)) => SqlValue::Boolean(true),
        (Some(false), Some(false)) => SqlValue::Boolean(false),
        _ => SqlValue::Null,
    }
}

/// 对一个聚合函数在所有收集到的行上求值，返回单个 SqlValue。
fn eval_aggregate(
    expr: &SqlCatalogBoundExpression,
    rows: &[RowFieldValues],
    field_types: &std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    let (name, args, distinct) = match expr {
        SqlCatalogBoundExpression::Function { name, args, distinct } => (name.clone(), args.clone(), *distinct),
        _ => return Err(execute_error(&format!("eval_aggregate: non-Function expression encountered: {expr:?}"))),
    };

    let agg =
        SqlAggregateFunction::from_name(&name).ok_or_else(|| execute_error(&format!("unknown aggregate function: {name}")))?;

    // `*` 只对 `COUNT(*)` 有意义；`SUM(*)` 一类没有可求值的列。
    if agg != SqlAggregateFunction::COUNT && args.iter().any(|arg| matches!(arg, SqlCatalogBoundExpression::Wildcard)) {
        return Err(execute_error(&format!("{} does not accept `*` as its argument", agg.name())));
    }
    if distinct && agg != SqlAggregateFunction::COUNT {
        return Err(execute_error(&format!("DISTINCT quantifier is only supported on COUNT for now ({})", agg.name())));
    }

    match agg {
        SqlAggregateFunction::COUNT => {
            if args.is_empty() || matches!(args.as_slice(), [SqlCatalogBoundExpression::Wildcard]) {
                // COUNT(*) — total row count after filter (DISTINCT 对 * 无意义).
                Ok(SqlValue::Integer(i64::try_from(rows.len()).unwrap_or(i64::MAX)))
            }
            else {
                if args.len() != 1 {
                    return Err(execute_error("COUNT accepts exactly one argument"));
                }
                if distinct {
                    // COUNT(DISTINCT col) — count distinct non-NULL values.
                    let mut seen: Vec<SqlValue> = Vec::new();
                    for row in rows {
                        let v = eval_expression(&args[0], row, field_types, dialect)?;
                        if !matches!(v, SqlValue::Null) && !seen.contains(&v) {
                            seen.push(v);
                        }
                    }
                    Ok(SqlValue::Integer(i64::try_from(seen.len()).unwrap_or(i64::MAX)))
                }
                else {
                    // COUNT(col) — count rows where col is not NULL.
                    let mut non_null: usize = 0;
                    for row in rows {
                        let v = eval_expression(&args[0], row, field_types, dialect)?;
                        if !matches!(v, SqlValue::Null) {
                            non_null += 1;
                        }
                    }
                    Ok(SqlValue::Integer(i64::try_from(non_null).unwrap_or(i64::MAX)))
                }
            }
        }
        SqlAggregateFunction::SUM => {
            // 整数列用 i128 精确累加，避免 `i64` 先转 `f64` 再求和造成精度丢失。
            // 出现浮点值时按 SQL 的数值提升规则整体转为 f64。
            let mut integer_sum: i128 = 0;
            let mut float_sum: f64 = 0.0;
            let mut has_float = false;
            let mut has_value = false;
            for row in rows {
                match eval_expression(&args[0], row,field_types, dialect)? {
                    SqlValue::Integer(n) => {
                        integer_sum += i128::from(n);
                        float_sum += n as f64;
                        has_value = true;
                    }
                    SqlValue::Number(n) => {
                        has_float = true;
                        float_sum += n;
                        has_value = true;
                    }
                    _ => {}
                }
            }
            if !has_value {
                Ok(SqlValue::Null)
            }
            else if has_float {
                Ok(SqlValue::Number(float_sum))
            }
            else {
                Ok(i64::try_from(integer_sum).map(SqlValue::Integer).unwrap_or(SqlValue::Number(integer_sum as f64)))
            }
        }
        SqlAggregateFunction::AVG => {
            let mut sum: f64 = 0.0;
            let mut count: usize = 0;
            for row in rows {
                let v = eval_expression(&args[0], row,field_types, dialect)?;
                match v {
                    SqlValue::Integer(n) => {
                        sum += n as f64;
                        count += 1;
                    }
                    SqlValue::Number(n) => {
                        sum += n;
                        count += 1;
                    }
                    _ => {}
                }
            }
            if count > 0 {
                Ok(SqlValue::Number(sum / count as f64))
            }
            else {
                Ok(SqlValue::Null)
            }
        }
        SqlAggregateFunction::MAX | SqlAggregateFunction::MIN => {
            let want_greater = agg == SqlAggregateFunction::MAX;
            let mut best: Option<SqlValue> = None;
            for row in rows {
                let value = eval_expression(&args[0], row,field_types, dialect)?;
                if !matches!(value, SqlValue::Integer(_) | SqlValue::Number(_)) {
                    continue;
                }
                best = Some(match best {
                    Some(current) => {
                        let order = compare_values(Some(&value), Some(&current));
                        let replace =
                            if want_greater { order == std::cmp::Ordering::Greater } else { order == std::cmp::Ordering::Less };
                        if replace {
                            value
                        }
                        else {
                            current
                        }
                    }
                    None => value,
                });
            }
            Ok(best.unwrap_or(SqlValue::Null))
        }
    }
}

fn eval_expression(
    expr: &SqlCatalogBoundExpression,
    row: &RowFieldValues,
    field_types: &std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    Ok(match expr {
        // 缺失字段就是 SQL NULL（写入 NULL 时字段会被省略），比较和投影都按 NULL 处理。
        SqlCatalogBoundExpression::Column(col) => row.value_typed(col.field_id, field_types).unwrap_or(SqlValue::Null),
        SqlCatalogBoundExpression::Literal(lit) => SqlValue::from_literal(lit),
        SqlCatalogBoundExpression::Wildcard => {
            return Err(execute_error("wildcard `*` is only valid as the argument of COUNT"))
        }
        SqlCatalogBoundExpression::Unary { operator, expression } => {
            let inner = eval_expression(expression, row, field_types, dialect)?;
            eval_unary(*operator, inner)?
        }
        SqlCatalogBoundExpression::Binary { left, operator, right } => {
            let l = eval_expression(left, row, field_types, dialect)?;
            let r = eval_expression(right, row, field_types, dialect)?;
            eval_binary(*operator, &l, &r, dialect)?
        }
        SqlCatalogBoundExpression::Function { name, args, distinct } => {
            let scalar = SqlScalarFunction::from_name(name)
                .ok_or_else(|| execute_error(&format!("aggregate function {name} cannot be evaluated on a single row")))?;
            let mut values = Vec::with_capacity(args.len());
            for arg in args {
                values.push(eval_expression(arg, row, field_types, dialect)?);
            }
            eval_scalar_function(scalar, &values)?
        }
        SqlCatalogBoundExpression::InValues { expression, values, negated } => {
            let target = eval_expression(expression, row, field_types, dialect)?;
            eval_in_values(target, values, *negated, dialect)?
        }
        // 子查询应当在执行前被 materialize 成 `InValues`；走到这里说明内部计划漏了 lowering。
        SqlCatalogBoundExpression::InSubquery { .. } => {
            return Err(execute_error("IN subquery was not materialized before execution"))
        }
    })
}

/// `IN` 成员判断：左值是 NULL 时结果未知，列表里的 NULL 让未命中变成未知。
fn eval_in_values(target: SqlValue, values: &[SqlBoundLiteral], negated: bool, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
    if matches!(target, SqlValue::Null) {
        return Ok(SqlValue::Null);
    }
    let mut found = false;
    let mut saw_null = false;
    for literal in values {
        match eval_binary(SqlBoundBinaryOperator::Equal, &target, &SqlValue::from_literal(literal), dialect)? {
            SqlValue::Boolean(true) => {
                found = true;
                break;
            }
            SqlValue::Null => saw_null = true,
            _ => {}
        }
    }
    Ok(if found {
        SqlValue::Boolean(!negated)
    }
    else if saw_null {
        SqlValue::Null
    }
    else {
        SqlValue::Boolean(negated)
    })
}

/// 对一行求值标量内建函数。参数已按从左到右求值完毕。
fn eval_scalar_function(function: SqlScalarFunction, args: &[SqlValue]) -> Result<SqlValue, SqlFrontendError> {
    use SqlValue::{Integer, Null, Number, String as Text};
    Ok(match function {
        SqlScalarFunction::Version => Text(crate::version_label()),
        // 未选择数据库 / schema 时，MySQL 与 PostgreSQL 都返回 NULL。
        SqlScalarFunction::Database | SqlScalarFunction::CurrentSchema => Null,
        SqlScalarFunction::Now => Text(current_timestamp()),
        SqlScalarFunction::Lower => match scalar_text(args, 0) {
            Some(text) => Text(text.to_lowercase()),
            None => Null,
        },
        SqlScalarFunction::Upper => match scalar_text(args, 0) {
            Some(text) => Text(text.to_uppercase()),
            None => Null,
        },
        SqlScalarFunction::Length => match scalar_text(args, 0) {
            Some(text) => Integer(i64::try_from(text.chars().count()).unwrap_or(i64::MAX)),
            None => Null,
        },
        SqlScalarFunction::Coalesce => args.iter().find(|value| **value != Null).cloned().unwrap_or(Null),
        SqlScalarFunction::Abs => match args.first() {
            Some(Integer(value)) => Integer(value.checked_abs().ok_or_else(|| execute_error("ABS integer overflow"))?),
            Some(Number(value)) => Number(value.abs()),
            Some(Null) | None => Null,
            Some(_) => Number(0.0),
        },
        SqlScalarFunction::Round => {
            let digits = match args.get(1) {
                Some(Integer(value)) => Some(*value as f64),
                Some(Number(value)) => Some(*value),
                _ => None,
            };
            match args.first() {
                Some(Integer(value)) => match digits {
                    Some(digits) if digits < 0.0 => {
                        let factor = 10f64.powi(digits.abs() as i32);
                        Number((*value as f64 / factor).round() * factor)
                    }
                    _ => Integer(*value),
                },
                Some(Number(value)) => match digits {
                    Some(digits) => {
                        let factor = 10f64.powi(digits as i32);
                        Number((value * factor).round() / factor)
                    }
                    None => Number(value.round()),
                },
                Some(Null) | None => Null,
                Some(_) => Number(0.0),
            }
        }
    })
}

/// 把第 `index` 个参数取成字符串；NULL 或缺失返回 `None`。
fn scalar_text(args: &[SqlValue], index: usize) -> Option<String> {
    match args.get(index) {
        Some(SqlValue::String(text)) => Some(text.clone()),
        Some(SqlValue::Integer(value)) => Some(value.to_string()),
        Some(SqlValue::Number(value)) => Some(format_number(*value)),
        Some(SqlValue::Boolean(value)) => Some(if *value { "1" } else { "0" }.to_string()),
        Some(SqlValue::Null) | None => None,
    }
}

/// Session timestamp in SQL `YYYY-MM-DD HH:MM:SS` form, derived from the host clock.
fn current_timestamp() -> String {
    let seconds =
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|elapsed| elapsed.as_secs() as i64).unwrap_or(0);
    let days = seconds.div_euclid(86_400);
    let remainder = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    format!("{year:04}-{month:02}-{day:02} {:02}:{:02}:{:02}", remainder / 3600, (remainder % 3600) / 60, remainder % 60)
}

/// Gregorian date from days since 1970-01-01 (Howard Hinnant's civil_from_days).
fn civil_from_days(days: i64) -> (i64, u32, u32) {
    let shifted = days + 719_468;
    let era = shifted.div_euclid(146_097);
    let day_of_era = shifted.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let year = year_of_era + era * 400;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_prime = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_prime + 2) / 5 + 1;
    let month = if month_prime < 10 { month_prime + 3 } else { month_prime - 9 };
    (if month <= 2 { year + 1 } else { year }, month as u32, day as u32)
}

/// Render a numeric SQL value with no trailing `.0` for integral values.
pub(crate) fn format_number(value: f64) -> String {
    if value.is_finite() && value.fract() == 0.0 && value.abs() < 1e15 {
        format!("{}", value as i64)
    }
    else {
        format!("{value}")
    }
}

fn eval_unary(op: SqlBoundUnaryOperator, operand: SqlValue) -> Result<SqlValue, SqlFrontendError> {
    Ok(match op {
        SqlBoundUnaryOperator::Plus => match operand {
            SqlValue::Integer(n) => SqlValue::Integer(n),
            SqlValue::Number(n) => SqlValue::Number(n),
            _ => SqlValue::Null,
        },
        SqlBoundUnaryOperator::Minus => match operand {
            SqlValue::Integer(n) => {
                SqlValue::Integer(n.checked_neg().ok_or_else(|| execute_error("integer overflow in unary minus"))?)
            }
            SqlValue::Number(n) => SqlValue::Number(-n),
            _ => SqlValue::Null,
        },
        // SQL 三值逻辑：`NOT NULL` 仍是 NULL。
        SqlBoundUnaryOperator::Not => match sql_truth(&operand) {
            Some(value) => SqlValue::Boolean(!value),
            None => SqlValue::Null,
        },
        // 存在性判断不是与 NULL 比较，因此结果永远是 TRUE/FALSE。
        SqlBoundUnaryOperator::IsNull => SqlValue::Boolean(matches!(operand, SqlValue::Null)),
        SqlBoundUnaryOperator::IsNotNull => SqlValue::Boolean(!matches!(operand, SqlValue::Null)),
        // 真值判断同样是三值谓词：`NULL IS TRUE` 与 `NULL IS FALSE` 都为 FALSE，
        // 因此永远不会返回 NULL。
        SqlBoundUnaryOperator::IsTruth { value, negated } => SqlValue::Boolean((sql_truth(&operand) == Some(value)) != negated),
    })
}

fn eval_binary(op: SqlBoundBinaryOperator, left: &SqlValue, right: &SqlValue, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
    use SqlValue::*;
    // AND / OR 必须先于 NULL 传播处理，否则 `NULL AND FALSE` 会被错误地求成 NULL。
    match op {
        SqlBoundBinaryOperator::And => return Ok(sql_and(left, right)),
        SqlBoundBinaryOperator::Or => return Ok(sql_or(left, right)),
        _ => {}
    }
    if matches!(left, Null) || matches!(right, Null) {
        return Ok(Null);
    }
    Ok(match (left, right) {
        (Integer(a), Integer(b)) => integer_binary(op, *a, *b, dialect)?,
        // 整数与浮点混合比较必须先走精确序关系，否则 2^53 以上的 `i64` 会先被
        // 舍入成 `f64`，导致 `9007199254740993 > 9007199254740992.0` 判成相等。
        (Integer(a), Number(b)) if is_comparison(op) => match compare_integer_float(*a, *b) {
            Some(ordering) => comparison_value(op, ordering),
            None => float_binary(op, *a as f64, *b, dialect)?,
        },
        (Number(a), Integer(b)) if is_comparison(op) => match compare_integer_float(*b, *a) {
            Some(ordering) => comparison_value(op, ordering.reverse()),
            None => float_binary(op, *a, *b as f64, dialect)?,
        },
        (Integer(a), Number(b)) => float_binary(op, *a as f64, *b, dialect)?,
        (Number(a), Integer(b)) => float_binary(op, *a, *b as f64, dialect)?,
        (Number(a), Number(b)) => float_binary(op, *a, *b, dialect)?,
        (String(a), String(b)) => match op {
            SqlBoundBinaryOperator::Equal => Boolean(a == b),
            SqlBoundBinaryOperator::NotEqual => Boolean(a != b),
            SqlBoundBinaryOperator::Less => Boolean(a < b),
            SqlBoundBinaryOperator::Greater => Boolean(a > b),
            SqlBoundBinaryOperator::LessEqual => Boolean(a <= b),
            SqlBoundBinaryOperator::GreaterEqual => Boolean(a >= b),
            SqlBoundBinaryOperator::Like => Boolean(like_match(a, b)),
            _ => Null,
        },
        (Boolean(a), Boolean(b)) => match op {
            SqlBoundBinaryOperator::Equal => Boolean(a == b),
            SqlBoundBinaryOperator::NotEqual => Boolean(a != b),
            _ => Null,
        },
        _ => Null,
    })
}

/// 比较类操作符（`=`/`<>`/`<`/`>`/`<=`/`>=`），其余是算术或逻辑操作符。
fn is_comparison(op: SqlBoundBinaryOperator) -> bool {
    matches!(
        op,
        SqlBoundBinaryOperator::Equal
            | SqlBoundBinaryOperator::NotEqual
            | SqlBoundBinaryOperator::Less
            | SqlBoundBinaryOperator::Greater
            | SqlBoundBinaryOperator::LessEqual
            | SqlBoundBinaryOperator::GreaterEqual
    )
}

/// 把一个精确序关系映射成布尔比较结果。调用方保证 `op` 是比较操作符。
fn comparison_value(op: SqlBoundBinaryOperator, ordering: std::cmp::Ordering) -> SqlValue {
    SqlValue::Boolean(match op {
        SqlBoundBinaryOperator::Equal => ordering.is_eq(),
        SqlBoundBinaryOperator::NotEqual => !ordering.is_eq(),
        SqlBoundBinaryOperator::Less => ordering.is_lt(),
        SqlBoundBinaryOperator::Greater => ordering.is_gt(),
        SqlBoundBinaryOperator::LessEqual => ordering.is_le(),
        SqlBoundBinaryOperator::GreaterEqual => ordering.is_ge(),
        _ => false,
    })
}

/// 精确比较 `i64` 与 `f64`。浮点是 NaN 时返回 `None`，调用方保留原有浮点比较语义。
///
/// `i64` 的上下界在 `f64` 中可精确表示：`i64::MIN as f64 == -2^63`，
/// 而 `i64::MAX as f64` 向上舍入到 `2^63`。因此先把浮点截断成整数再比较，
/// 只有整数部分相等时才用小数部分决定序关系，避免 `i64` 先转 `f64` 丢精度。
fn compare_integer_float(integer: i64, float: f64) -> Option<std::cmp::Ordering> {
    use std::cmp::Ordering;
    if float.is_nan() {
        return None;
    }
    const TWO_POW_63: f64 = 9_223_372_036_854_775_808.0;
    if float >= TWO_POW_63 {
        return Some(Ordering::Less);
    }
    if float < -TWO_POW_63 {
        return Some(Ordering::Greater);
    }
    let truncated = float.trunc();
    let truncated_integer = truncated as i64;
    match integer.cmp(&truncated_integer) {
        Ordering::Equal if float > truncated => Some(Ordering::Less),
        Ordering::Equal if float < truncated => Some(Ordering::Greater),
        other => Some(other),
    }
}

/// 整数之间的四则与比较：加、减、乘、模保持精确整数，只在溢出时显式报错。
fn integer_binary(op: SqlBoundBinaryOperator, a: i64, b: i64, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
    use SqlValue::{Boolean, Integer, Number};
    let overflow = || execute_error("integer overflow");
    let divide_by_zero = || match dialect {
        SqlDialect::Postgres => Err(division_by_zero_error()),
        SqlDialect::Mysql => Ok(SqlValue::Null),
    };
    Ok(match op {
        SqlBoundBinaryOperator::Plus => Integer(a.checked_add(b).ok_or_else(overflow)?),
        SqlBoundBinaryOperator::Minus => Integer(a.checked_sub(b).ok_or_else(overflow)?),
        SqlBoundBinaryOperator::Multiply => Integer(a.checked_mul(b).ok_or_else(overflow)?),
        // MySQL INT/INT → 提升到 f64 除法；PostgreSQL INT/INT → 整数截断
        SqlBoundBinaryOperator::Divide => {
            if b == 0 {
                return divide_by_zero();
            }
            match dialect {
                SqlDialect::Mysql => Number(a as f64 / b as f64),
                SqlDialect::Postgres => Integer(a.checked_div(b).ok_or_else(overflow)?),
            }
        }
        // 与 PostgreSQL / MySQL 一致，余数符号跟随被除数。
        SqlBoundBinaryOperator::Modulo => {
            if b == 0 {
                return divide_by_zero();
            }
            Integer(a.checked_rem(b).ok_or_else(overflow)?)
        }
        SqlBoundBinaryOperator::Equal => Boolean(a == b),
        SqlBoundBinaryOperator::NotEqual => Boolean(a != b),
        SqlBoundBinaryOperator::Less => Boolean(a < b),
        SqlBoundBinaryOperator::Greater => Boolean(a > b),
        SqlBoundBinaryOperator::LessEqual => Boolean(a <= b),
        SqlBoundBinaryOperator::GreaterEqual => Boolean(a >= b),
        SqlBoundBinaryOperator::Like | SqlBoundBinaryOperator::And | SqlBoundBinaryOperator::Or => SqlValue::Null,
    })
}

/// 至少一侧是浮点时按 f64 运算；比较也按 f64 语义。
fn float_binary(op: SqlBoundBinaryOperator, a: f64, b: f64, dialect: SqlDialect) -> Result<SqlValue, SqlFrontendError> {
    use SqlValue::{Boolean, Number};
    let divide_by_zero = || match dialect {
        SqlDialect::Postgres => Err(division_by_zero_error()),
        SqlDialect::Mysql => Ok(SqlValue::Null),
    };
    Ok(match op {
        SqlBoundBinaryOperator::Plus => Number(a + b),
        SqlBoundBinaryOperator::Minus => Number(a - b),
        SqlBoundBinaryOperator::Multiply => Number(a * b),
        SqlBoundBinaryOperator::Divide => {
            if b == 0.0 {
                return divide_by_zero();
            }
            Number(a / b)
        }
        SqlBoundBinaryOperator::Modulo => {
            if b == 0.0 {
                return divide_by_zero();
            }
            Number(a % b)
        }
        SqlBoundBinaryOperator::Equal => Boolean(a == b),
        SqlBoundBinaryOperator::NotEqual => Boolean(a != b),
        SqlBoundBinaryOperator::Less => Boolean(a < b),
        SqlBoundBinaryOperator::Greater => Boolean(a > b),
        SqlBoundBinaryOperator::LessEqual => Boolean(a <= b),
        SqlBoundBinaryOperator::GreaterEqual => Boolean(a >= b),
        SqlBoundBinaryOperator::Like | SqlBoundBinaryOperator::And | SqlBoundBinaryOperator::Or => SqlValue::Null,
    })
}

/// SQL LIKE 匹配。`%` 匹配 0+ 任意字符，`_` 匹配正好 1 个任意字符。
fn like_match(haystack: &str, pattern: &str) -> bool {
    let haystack_chars: Vec<char> = haystack.chars().collect();
    let pattern_chars: Vec<char> = pattern.chars().collect();
    let n = haystack_chars.len();
    let m = pattern_chars.len();

    // dp[i][j] = haystack[0..i] 能否匹配 pattern[0..j]
    let mut dp = vec![vec![false; m + 1]; n + 1];
    dp[0][0] = true;

    // pattern 前缀全是 % 的情况
    let mut j = 1;
    while j <= m && pattern_chars[j - 1] == '%' {
        dp[0][j] = true;
        j += 1;
    }

    for i in 1..=n {
        for j in 1..=m {
            match pattern_chars[j - 1] {
                '%' => {
                    // % 可以跳过（用 dp[i][j-1]）或吃掉当前 haystack 字符（用 dp[i-1][j]）
                    dp[i][j] = dp[i][j - 1] || dp[i - 1][j];
                }
                '_' => {
                    // _ 必须正好吃掉一个字符
                    dp[i][j] = dp[i - 1][j - 1];
                }
                p => {
                    dp[i][j] = dp[i - 1][j - 1] && haystack_chars[i - 1] == p;
                }
            }
        }
    }
    dp[n][m]
}

struct Cursor<'a> {
    data: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len() - self.pos
    }

    fn read_u16(&mut self) -> Result<u16, SqlFrontendError> {
        if self.remaining() < 2 {
            return Err(execute_error("row TLV truncated reading u16"));
        }
        let value = u16::from_be_bytes([self.data[self.pos], self.data[self.pos + 1]]);
        self.pos += 2;
        Ok(value)
    }

    fn read_u32(&mut self) -> Result<u32, SqlFrontendError> {
        if self.remaining() < 4 {
            return Err(execute_error("row TLV truncated reading u32"));
        }
        let bytes: [u8; 4] = self.data[self.pos..self.pos + 4].try_into().unwrap();
        let value = u32::from_be_bytes(bytes);
        self.pos += 4;
        Ok(value)
    }

    fn read_u64(&mut self) -> Result<u64, SqlFrontendError> {
        if self.remaining() < 8 {
            return Err(execute_error("row TLV truncated reading u64"));
        }
        let bytes: [u8; 8] = self.data[self.pos..self.pos + 8].try_into().unwrap();
        let value = u64::from_be_bytes(bytes);
        self.pos += 8;
        Ok(value)
    }

    fn read_bytes(&mut self, len: usize) -> Result<Vec<u8>, SqlFrontendError> {
        if self.remaining() < len {
            return Err(execute_error("row TLV truncated reading bytes"));
        }
        let out = self.data[self.pos..self.pos + len].to_vec();
        self.pos += len;
        Ok(out)
    }
}

/// 把 GROUP BY 的 key values 序列化成稳定字符串，用作 BTreeMap bucket key。
fn serialize_group_key(values: &[SqlValue]) -> String {
    use std::fmt::Write;
    let mut buf = String::new();
    for (i, v) in values.iter().enumerate() {
        if i > 0 {
            buf.push_str("|");
        }
        match v {
            SqlValue::Null => buf.push_str("NULL"),
            SqlValue::Integer(n) => {
                let _ = write!(buf, "I:{n}");
            }
            SqlValue::Number(n) => {
                let _ = write!(buf, "N:{n}");
            }
            SqlValue::String(s) => {
                buf.push_str("S:");
                buf.push_str(s);
            }
            SqlValue::Boolean(b) => {
                buf.push_str(if *b { "B:true" } else { "B:false" });
            }
        }
    }
    buf
}

/// 在 GROUP BY bucket 上下文中求值表达式。
/// 与 `eval_expression` 的区别：Function 会被当作聚合函数对 bucket_rows 求值；
/// Column 从 bucket 首行取（同一 bucket 内 group key 列值相同）。
fn eval_having(
    expr: &SqlCatalogBoundExpression,
    bucket_rows: &[RowFieldValues],
    group_exprs: &[SqlCatalogBoundExpression],
    field_types: &std::collections::BTreeMap<u64, crate::catalog_bind::SqlFieldType>,
    dialect: SqlDialect,
) -> Result<SqlValue, SqlFrontendError> {
    let first = bucket_rows.first();
    Ok(match expr {
        SqlCatalogBoundExpression::Column(col) => {
            first.and_then(|row| row.value_typed(col.field_id, field_types)).unwrap_or(SqlValue::Null)
        }
        SqlCatalogBoundExpression::Literal(lit) => SqlValue::from_literal(lit),
        SqlCatalogBoundExpression::Wildcard => {
            return Err(execute_error("wildcard `*` is only valid as the argument of COUNT"))
        }
        SqlCatalogBoundExpression::Function { name, args, distinct } => {
            if SqlAggregateFunction::from_name(name).is_some() {
                eval_aggregate(expr, bucket_rows, field_types, dialect)?
            }
            else if let Some(function) = SqlScalarFunction::from_name(name) {
                let mut values = Vec::with_capacity(args.len());
                for arg in args {
                    values.push(eval_having(arg, bucket_rows, group_exprs, field_types, dialect)?);
                }
                eval_scalar_function(function, &values)?
            }
            else {
                return Err(execute_error(&format!("unknown function in grouped expression: {name}")));
            }
        }
        SqlCatalogBoundExpression::Unary { operator, expression } => {
            let inner = eval_having(expression, bucket_rows, group_exprs, field_types, dialect)?;
            eval_unary(*operator, inner)?
        }
        SqlCatalogBoundExpression::Binary { left, operator, right } => {
            let l = eval_having(left, bucket_rows, group_exprs, field_types, dialect)?;
            let r = eval_having(right, bucket_rows, group_exprs, field_types, dialect)?;
            eval_binary(*operator, &l,&r, dialect)?
        }
        SqlCatalogBoundExpression::InValues { expression, values, negated } => {
            let target = eval_having(expression, bucket_rows, group_exprs, field_types, dialect)?;
            eval_in_values(target, values,*negated, dialect)?
        }
        SqlCatalogBoundExpression::InSubquery { .. } => {
            return Err(execute_error("IN subquery was not materialized before execution"))
        }
    })
}

fn execute_error(message: &str) -> SqlFrontendError {
    SqlFrontendError::new(SqlErrorKind::Execution, message)
}

fn division_by_zero_error() -> SqlFrontendError {
    SqlFrontendError::new(SqlErrorKind::DivisionByZero, "division by zero")
}

/// Storage failures that other transactions can resolve by retrying are
/// reported as serialization conflicts rather than generic execution errors.
fn write_error(message: String, error: &yyds_types::Error) -> SqlFrontendError {
    match error {
        yyds_types::Error::CasConflict { .. } => SqlFrontendError::new(SqlErrorKind::Conflict, message),
        _ => SqlFrontendError::new(SqlErrorKind::Execution, message),
    }
}

/// SQL value equality used by `SELECT DISTINCT`.
///
/// `NULL` equals `NULL` for de-duplication, integers and floats compare by exact
/// numeric value (never through a lossy `f64` cast for `i64`), and unrelated
/// storage variants are never collapsed into one row.
fn values_equal(left: &SqlValue, right: &SqlValue) -> bool {
    use std::cmp::Ordering;
    match (left, right) {
        (SqlValue::Integer(left), SqlValue::Integer(right)) => left == right,
        (SqlValue::Integer(integer), SqlValue::Number(float)) => {
            compare_integer_float(*integer, *float) == Some(Ordering::Equal)
        }
        (SqlValue::Number(float), SqlValue::Integer(integer)) => {
            compare_integer_float(*integer, *float) == Some(Ordering::Equal)
        }
        (SqlValue::Number(left), SqlValue::Number(right)) => {
            left.partial_cmp(right).unwrap_or(Ordering::Equal) == Ordering::Equal
        }
        (SqlValue::String(left), SqlValue::String(right)) => left == right,
        (SqlValue::Boolean(left), SqlValue::Boolean(right)) => left == right,
        (SqlValue::Null, SqlValue::Null) => true,
        _ => false,
    }
}

fn projected_rows_equal(left: &[SqlValue], right: &[SqlValue]) -> bool {
    left.len() == right.len() && left.iter().zip(right.iter()).all(|(left, right)| values_equal(left, right))
}

/// Keeps the first occurrence of each projected row, preserving scan order.
fn retain_distinct_projected_rows(staged: &mut Vec<(Vec<Option<SqlValue>>, Vec<SqlValue>)>) {
    let mut seen: Vec<Vec<SqlValue>> = Vec::new();
    staged.retain(|(_, row)| {
        if seen.iter().any(|candidate| projected_rows_equal(candidate, row)) {
            false
        }
        else {
            seen.push(row.clone());
            true
        }
    });
}

/// NULLs sort last (standard SQL behavior). Mixed types collate equal.
fn compare_values(a: Option<&SqlValue>, b: Option<&SqlValue>) -> std::cmp::Ordering {
    compare_values_with_nulls(a, b, false)
}

/// 解析 ORDER BY 项排序前的 NULL 置前标记：未写 `NULLS FIRST/LAST` 时用方言默认，
/// 显式子句则在 ASC 或 DESC 下分别映射为置前或置后。
fn order_nulls_first(direction: &SqlBoundOrderDirection, explicit: Option<SqlNullsOrder>, dialect_default: bool) -> bool {
    match explicit {
        None => dialect_default,
        Some(SqlNullsOrder::First) => matches!(direction, SqlBoundOrderDirection::Asc),
        Some(SqlNullsOrder::Last) => matches!(direction, SqlBoundOrderDirection::Desc),
    }
}

/// 比较两个排序键，NULL 位置由方言决定：PostgreSQL 把 NULL 视为最大值，
/// MySQL 把 NULL 视为最小值。
fn compare_values_with_nulls(a: Option<&SqlValue>, b: Option<&SqlValue>, nulls_first: bool) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (None, None) | (Some(SqlValue::Null), Some(SqlValue::Null)) => Ordering::Equal,
        (None | Some(SqlValue::Null), Some(_)) => {
            if nulls_first {
                Ordering::Less
            }
            else {
                Ordering::Greater
            }
        }
        (Some(_), None | Some(SqlValue::Null)) => {
            if nulls_first {
                Ordering::Greater
            }
            else {
                Ordering::Less
            }
        }
        (Some(x), Some(y)) => match (x, y) {
            (SqlValue::Integer(n1), SqlValue::Integer(n2)) => n1.cmp(n2),
            // 排序同样不能把 `i64` 先转 `f64`，否则超出 2^53 的行会并列。
            (SqlValue::Integer(n1), SqlValue::Number(n2)) => compare_integer_float(*n1, *n2).unwrap_or(Ordering::Equal),
            (SqlValue::Number(n1), SqlValue::Integer(n2)) => {
                compare_integer_float(*n2, *n1).map(Ordering::reverse).unwrap_or(Ordering::Equal)
            }
            (SqlValue::Number(n1), SqlValue::Number(n2)) => n1.partial_cmp(n2).unwrap_or(Ordering::Equal),
            (SqlValue::String(s1), SqlValue::String(s2)) => s1.cmp(s2),
            (SqlValue::Boolean(b1), SqlValue::Boolean(b2)) => b1.cmp(b2),
            _ => Ordering::Equal,
        },
    }
}
