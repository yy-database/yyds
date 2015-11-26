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

use yyds_execution::KeyValueExecutor;
use yyds_types::{KeyValueAction, KeyValueCommand, Namespace};

use crate::{
    catalog_bind::SqlCatalogBoundExpression,
    sql::{SqlAggregateFunction, SqlBoundBinaryOperator, SqlBoundLiteral, SqlBoundUnaryOperator, SqlFrontendError},
    sql_planner::{DeletePlan, ReadPlan, ScanPlan, SqlExecutionPlan, UpdatePlan, WritePlan},
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
    /// 数值（SQL 不区分整数和浮点数，gateway 用 f64 表示）。
    Number(f64),
    /// 字符串。
    String(String),
    /// 布尔。
    Boolean(bool),
    /// NULL。
    Null,
}

impl SqlValue {
    /// 从 KV 层拿到的原始 bytes 推断一个 SqlValue。
    ///
    /// 目前策略很朴素：如果 bytes 是合法 UTF-8 并且能解析为整数或浮点数，就返回 Number；
    /// 否则返回 String。如果上游知道精确类型，应避免这条推断路径。
    pub fn from_row_bytes(bytes: &[u8]) -> Self {
        let text = String::from_utf8_lossy(bytes).into_owned();
        if let Ok(int) = text.parse::<i64>() {
            return SqlValue::Number(int as f64);
        }
        if let Ok(float) = text.parse::<f64>() {
            if float.is_finite() {
                return SqlValue::Number(float);
            }
        }
        SqlValue::String(text)
    }

    /// 从 SQL 字面量构造。
    pub fn from_literal(lit: &SqlBoundLiteral) -> Self {
        match lit {
            SqlBoundLiteral::Number(text) => match text.parse::<f64>() {
                Ok(v) if v.is_finite() => SqlValue::Number(v),
                _ => SqlValue::String(text.clone()),
            },
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

    /// 按 field_id 取原始 bytes；不存在返回 None。
    pub fn get(&self, field_id: u64) -> Option<&[u8]> {
        self.fields.get(&field_id).map(|v| v.as_slice())
    }

    /// 按 field_id 取推断后的 SqlValue。
    pub fn value(&self, field_id: u64) -> Option<SqlValue> {
        self.fields.get(&field_id).map(|v| SqlValue::from_row_bytes(v))
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
/// 当 needed_ids 为空时退化为全量解码（兼容旧调用）。
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

/// 递归收集一个表达式里所有 Column 的 field_id。
fn collect_column_ids(expr: &SqlCatalogBoundExpression, out: &mut Vec<u64>) {
    match expr {
        SqlCatalogBoundExpression::Column(col) => {
            if !out.contains(&col.field_id) {
                out.push(col.field_id);
            }
        }
        SqlCatalogBoundExpression::Function { args, .. } => {
            for a in args {
                collect_column_ids(a, out);
            }
        }
        SqlCatalogBoundExpression::Binary { left, right, .. } => {
            collect_column_ids(left, out);
            collect_column_ids(right, out);
        }
        SqlCatalogBoundExpression::Unary { expression, .. } => {
            collect_column_ids(expression, out);
        }
        SqlCatalogBoundExpression::Literal(_) => {}
    }
}

/// 从 ScanPlan 里收集执行时需要解码的 field_ids（projection + predicate + order_by + expressions）。
fn scan_needed_field_ids(plan: &ScanPlan) -> Vec<u64> {
    let mut ids: Vec<u64> = Vec::new();
    for (fid, _) in &plan.projection_fields {
        if !ids.contains(fid) { ids.push(*fid); }
    }
    for (expr, _) in &plan.projection_expressions {
        collect_column_ids(expr, &mut ids);
    }
    if let Some(pred) = &plan.remaining_predicate {
        collect_column_ids(pred, &mut ids);
    }
    if let Some((order_expr, _)) = &plan.order_by {
        collect_column_ids(order_expr, &mut ids);
    }
    ids
}

/// 执行一个已计划的 SQL 语句。
///
/// SELECT 返回单行结果集（或空集）；INSERT/UPDATE/DELETE 返回空结果集，
/// 影响行数记录在 `SqlResult.affected_rows`。
pub fn execute_plan(executor: &dyn KeyValueExecutor, plan: &SqlExecutionPlan) -> Result<SqlResult, SqlFrontendError> {
    match plan {
        SqlExecutionPlan::Read(read_plan) => execute_read(executor, read_plan),
        SqlExecutionPlan::Scan(scan_plan) => execute_scan(executor, scan_plan),
        SqlExecutionPlan::Insert(write_plan) => execute_insert(executor, write_plan),
        SqlExecutionPlan::Update(update_plan) => execute_update(executor, update_plan),
        SqlExecutionPlan::Delete(delete_plan) => execute_delete(executor, delete_plan),
    }
}

fn execute_read(executor: &dyn KeyValueExecutor, plan: &ReadPlan) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());
    let command = KeyValueCommand { namespace, key: plan.rowid.clone(), action: KeyValueAction::Get };
    let batch = executor
        .execute_read_batch(&[command])
        .map_err(|e| execute_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?;

    let result = batch.into_iter().next().ok_or_else(|| execute_error("KeyValueExecutor returned empty batch"))?;
    let row_bytes = match result {
        yyds_types::KeyValueResult::Get(Some(bytes)) => bytes,
        _ => return Ok(SqlResult { columns: read_column_labels(plan), rows: Vec::new(), affected_rows: 0 }),
    };
    let row = decode_row(&row_bytes)?;

    if let Some(ref predicate) = plan.remaining_predicate {
        let v = eval_expression(predicate, &row)?;
        if !truthy(&v) {
            return Ok(SqlResult { columns: read_column_labels(plan), rows: Vec::new(), affected_rows: 0 });
        }
    }

    let columns = read_column_labels(plan);
    let mut row_values: Vec<SqlValue> = Vec::new();
    for (field_id, _) in &plan.projection_fields {
        row_values.push(row.value(*field_id).unwrap_or(SqlValue::Null));
    }
    for (expr, _) in &plan.projection_expressions {
        row_values.push(eval_expression(expr, &row)?);
    }
    Ok(SqlResult { columns, rows: vec![row_values], affected_rows: 0 })
}

fn execute_scan(executor: &dyn KeyValueExecutor, plan: &ScanPlan) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());
    let scanned = executor
        .execute_scan(&namespace, &plan.rowid_prefix, plan.rowid_after.as_deref(), plan.rowid_before.as_deref(), plan.limit)
        .map_err(|e| execute_error(&format!("KeyValueExecutor.scan failed: {e}")))?;

    let columns = scan_column_labels(plan);
    let needed_ids = scan_needed_field_ids(plan);

    // Top-N heap 优化：ORDER BY + (LIMIT 或 OFFSET) 时，用 BinaryHeap 保留 Top-N，
    // 避免先收集全量 rows 再排序。容量 = offset + limit（limit=0 视为无限，fallback 到 Vec）。
    let use_top_n =
        plan.order_by.is_some() && (plan.limit_after_sort > 0 || plan.offset_after_sort > 0);

    if plan.has_aggregate {
        // —— 聚合路径：需要访问所有 post-filter rows，无 heap 收益 ——
        let mut all_rows: Vec<RowFieldValues> = Vec::new();
        for (_rowid, value_bytes) in scanned {
            let row = decode_row_fields(&value_bytes, &needed_ids)?;
            if let Some(ref predicate) = plan.remaining_predicate {
                let v = eval_expression(predicate, &row)?;
                if !truthy(&v) { continue; }
            }
            all_rows.push(row);
        }
        let mut result_row: Vec<SqlValue> = Vec::new();
        for (expr, _) in &plan.projection_expressions {
            result_row.push(eval_aggregate(expr, &all_rows)?);
        }
        return Ok(SqlResult { columns, rows: vec![result_row], affected_rows: 0 });
    }

    if false && use_top_n {
        // —— Top-N heap 路径 ——
        let (order_expr, order_dir) = plan.order_by.as_ref().unwrap();
        let is_asc = matches!(order_dir, crate::sql::SqlBoundOrderDirection::Asc);
        let heap_capacity = plan.offset_after_sort.max(1) + plan.limit_after_sort;
        let mut heap: std::collections::BinaryHeap<SortableRow> = std::collections::BinaryHeap::with_capacity(heap_capacity);

        for (_rowid, value_bytes) in scanned {
            let row = decode_row_fields(&value_bytes, &needed_ids)?;
            if let Some(ref predicate) = plan.remaining_predicate {
                let v = eval_expression(predicate, &row)?;
                if !truthy(&v) { continue; }
            }

            let sort_key = Some(eval_expression(order_expr, &row)?);
            let mut row_values: Vec<SqlValue> = Vec::new();
            for (field_id, _) in &plan.projection_fields {
                row_values.push(row.value(*field_id).unwrap_or(SqlValue::Null));
            }
            for (expr, _) in &plan.projection_expressions {
                row_values.push(eval_expression(expr, &row)?);
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
    let mut staged: Vec<(Option<SqlValue>, Vec<SqlValue>)> = Vec::new();
    for (_rowid, value_bytes) in scanned {
        let row = decode_row_fields(&value_bytes, &needed_ids)?;
        if let Some(ref predicate) = plan.remaining_predicate {
            let v = eval_expression(predicate, &row)?;
            if !truthy(&v) { continue; }
        }

        let sort_key = match &plan.order_by {
            Some((expr, _)) => Some(eval_expression(expr, &row)?),
            None => None,
        };
        let mut row_values: Vec<SqlValue> = Vec::new();
        for (field_id, _) in &plan.projection_fields {
            row_values.push(row.value(*field_id).unwrap_or(SqlValue::Null));
        }
        for (expr, _) in &plan.projection_expressions {
            row_values.push(eval_expression(expr, &row)?);
        }
        staged.push((sort_key, row_values));
    }

    // Sort if ORDER BY present (无 LIMIT/OFFSET 时全量排序)。
    if let Some((_, ref direction)) = plan.order_by {
        let dir = direction.clone();
        staged.sort_by(move |a, b| {
            let ord = compare_values(a.0.as_ref(), b.0.as_ref());
            match dir {
                crate::sql::SqlBoundOrderDirection::Asc => ord,
                crate::sql::SqlBoundOrderDirection::Desc => ord.reverse(),
            }
        });
    }

    let offset = plan.offset_after_sort;
    let limit = plan.limit_after_sort;
    let rows: Vec<Vec<SqlValue>> = staged
        .into_iter()
        .skip(offset)
        .take(if limit == 0 { usize::MAX } else { limit })
        .map(|(_, r)| r)
        .collect();

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
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> { Some(self.cmp(other)) }
}

impl Ord for SortableRow {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let natural = compare_values(self.sort_key.as_ref(), other.sort_key.as_ref());
        if self.is_asc { natural } else { natural.reverse() }
    }
}

fn execute_insert(executor: &dyn KeyValueExecutor, plan: &WritePlan) -> Result<SqlResult, SqlFrontendError> {
    let row = write_plan_to_row(plan);
    let bytes = encode_row(&row);
    let namespace = Namespace(plan.namespace.clone());
    let action = KeyValueAction::PutIfAbsent(bytes);
    let command = KeyValueCommand { namespace, key: plan.rowid.clone(), action };
    let result = executor.execute(command).map_err(|e| execute_error(&format!("KeyValueExecutor.insert failed: {e}")))?;
    let affected = match result {
        yyds_types::KeyValueResult::PutIfAbsent { revision: Some(_) } => 1,
        _ => 0,
    };
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: affected })
}

fn execute_update(executor: &dyn KeyValueExecutor, plan: &UpdatePlan) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());
    let get_cmd = KeyValueCommand { namespace: namespace.clone(), key: plan.rowid.clone(), action: KeyValueAction::Get };
    let batch = executor
        .execute_read_batch(&[get_cmd])
        .map_err(|e| execute_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?;
    let existing_bytes = match batch.into_iter().next() {
        Some(yyds_types::KeyValueResult::Get(Some(b))) => b,
        _ => return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 0 }),
    };
    let mut row = decode_row(&existing_bytes)?;

    // 先评估 remaining_predicate —— 不通过就不写，影响 0 行。
    if let Some(ref predicate) = plan.remaining_predicate {
        let v = eval_expression(predicate, &row)?;
        if !truthy(&v) {
            return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 0 });
        }
    }

    for field in &plan.fields {
        row.insert(field.field_id, sql_value_to_bytes(&field.value));
    }
    let encoded = encode_row(&row);
    let put_cmd = KeyValueCommand { namespace, key: plan.rowid.clone(), action: KeyValueAction::Put(encoded) };
    let _ = executor.execute(put_cmd).map_err(|e| execute_error(&format!("KeyValueExecutor.update failed: {e}")))?;
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 1 })
}

fn execute_delete(executor: &dyn KeyValueExecutor, plan: &DeletePlan) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());

    // 如果有 remaining_predicate —— 先 Get 拿到行评估，不通过就不删。
    if let Some(ref predicate) = plan.remaining_predicate {
        let get_cmd = KeyValueCommand { namespace: namespace.clone(), key: plan.rowid.clone(), action: KeyValueAction::Get };
        let batch = executor
            .execute_read_batch(&[get_cmd])
            .map_err(|e| execute_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?;
        let existing_bytes = match batch.into_iter().next() {
            Some(yyds_types::KeyValueResult::Get(Some(b))) => b,
            // 行已不存在 — 也是 0 影响行
            _ => return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 0 }),
        };
        let row = decode_row(&existing_bytes)?;
        let v = eval_expression(predicate, &row)?;
        if !truthy(&v) {
            return Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: 0 });
        }
    }

    let action = KeyValueAction::Delete;
    let command = KeyValueCommand { namespace, key: plan.rowid.clone(), action };
    let result = executor.execute(command).map_err(|e| execute_error(&format!("KeyValueExecutor.delete failed: {e}")))?;
    let affected = match result {
        yyds_types::KeyValueResult::Delete { removed: true } => 1,
        _ => 0,
    };
    Ok(SqlResult { columns: Vec::new(), rows: Vec::new(), affected_rows: affected })
}

fn write_plan_to_row(plan: &WritePlan) -> RowFieldValues {
    let mut row = RowFieldValues::new();
    for field in &plan.fields {
        row.insert(field.field_id, sql_value_to_bytes(&field.value));
    }
    row
}

fn sql_value_to_bytes(lit: &SqlBoundLiteral) -> Vec<u8> {
    // TLV 里的原始 bytes — 让 decode 侧的 from_row_bytes 推断类型。
    match lit {
        SqlBoundLiteral::Number(text) => text.as_bytes().to_vec(),
        SqlBoundLiteral::String(text) => text.as_bytes().to_vec(),
        SqlBoundLiteral::Boolean(v) => (if *v { "1" } else { "0" }).as_bytes().to_vec(),
        SqlBoundLiteral::Null => Vec::new(),
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

fn truthy(v: &SqlValue) -> bool {
    match v {
        SqlValue::Boolean(b) => *b,
        SqlValue::Number(n) => *n != 0.0,
        SqlValue::String(s) => !s.is_empty(),
        SqlValue::Null => false,
    }
}

/// 对一个聚合函数在所有收集到的行上求值，返回单个 SqlValue。
fn eval_aggregate(expr: &SqlCatalogBoundExpression, rows: &[RowFieldValues]) -> Result<SqlValue, SqlFrontendError> {
    let (name, args) = match expr {
        SqlCatalogBoundExpression::Function { name, args } => (name.clone(), args.clone()),
        _ => return Err(execute_error(&format!("eval_aggregate: non-Function expression encountered: {expr:?}"))),
    };

    let agg = SqlAggregateFunction::from_name(&name)
        .ok_or_else(|| execute_error(&format!("unknown aggregate function: {name}")))?;

    match agg {
        SqlAggregateFunction::COUNT => {
            if args.is_empty() {
                // COUNT(*) — total row count after filter.
                Ok(SqlValue::Number(rows.len() as f64))
            }
            else {
                // COUNT(col) — count rows where col is not NULL.
                let mut non_null: usize = 0;
                for row in rows {
                    let v = eval_expression(&args[0], row)?;
                    if !matches!(v, SqlValue::Null) {
                        non_null += 1;
                    }
                }
                Ok(SqlValue::Number(non_null as f64))
            }
        }
        SqlAggregateFunction::SUM => {
            let mut sum: f64 = 0.0;
            let mut any_non_null = false;
            for row in rows {
                let v = eval_expression(&args[0], row)?;
                if let SqlValue::Number(n) = v {
                    sum += n;
                    any_non_null = true;
                }
            }
            if any_non_null { Ok(SqlValue::Number(sum)) } else { Ok(SqlValue::Null) }
        }
        SqlAggregateFunction::AVG => {
            let mut sum: f64 = 0.0;
            let mut count: usize = 0;
            for row in rows {
                let v = eval_expression(&args[0], row)?;
                if let SqlValue::Number(n) = v {
                    sum += n;
                    count += 1;
                }
            }
            if count > 0 { Ok(SqlValue::Number(sum / count as f64)) } else { Ok(SqlValue::Null) }
        }
        SqlAggregateFunction::MAX => {
            let mut max: Option<f64> = None;
            for row in rows {
                let v = eval_expression(&args[0], row)?;
                if let SqlValue::Number(n) = v {
                    max = Some(match max { Some(m) => n.max(m), None => n });
                }
            }
            Ok(max.map(SqlValue::Number).unwrap_or(SqlValue::Null))
        }
        SqlAggregateFunction::MIN => {
            let mut min: Option<f64> = None;
            for row in rows {
                let v = eval_expression(&args[0], row)?;
                if let SqlValue::Number(n) = v {
                    min = Some(match min { Some(m) => n.min(m), None => n });
                }
            }
            Ok(min.map(SqlValue::Number).unwrap_or(SqlValue::Null))
        }
    }
}

fn eval_expression(expr: &SqlCatalogBoundExpression, row: &RowFieldValues) -> Result<SqlValue, SqlFrontendError> {
    Ok(match expr {
        SqlCatalogBoundExpression::Column(col) => {
            row.value(col.field_id).ok_or_else(|| execute_error(&format!("column {} not present in row", col.field_name)))?
        }
        SqlCatalogBoundExpression::Literal(lit) => SqlValue::from_literal(lit),
        SqlCatalogBoundExpression::Unary { operator, expression } => {
            let inner = eval_expression(expression, row)?;
            eval_unary(*operator, inner)?
        }
        SqlCatalogBoundExpression::Binary { left, operator, right } => {
            let l = eval_expression(left, row)?;
            let r = eval_expression(right, row)?;
            eval_binary(*operator, &l, &r)?
        }
        SqlCatalogBoundExpression::Function { name, .. } => {
            return Err(execute_error(&format!(
                "aggregate function {name} cannot be evaluated on a single row"
            )));
        }
    })
}

fn eval_unary(op: SqlBoundUnaryOperator, operand: SqlValue) -> Result<SqlValue, SqlFrontendError> {
    Ok(match op {
        SqlBoundUnaryOperator::Plus => match operand {
            SqlValue::Number(n) => SqlValue::Number(n),
            other => other,
        },
        SqlBoundUnaryOperator::Minus => match operand {
            SqlValue::Number(n) => SqlValue::Number(-n),
            _ => SqlValue::Null,
        },
        SqlBoundUnaryOperator::Not => SqlValue::Boolean(!truthy(&operand)),
    })
}

fn eval_binary(op: SqlBoundBinaryOperator, left: &SqlValue, right: &SqlValue) -> Result<SqlValue, SqlFrontendError> {
    use SqlValue::*;
    Ok(match (left, right) {
        (Null, _) | (_, Null) => Null,
        (Number(a), Number(b)) => match op {
            SqlBoundBinaryOperator::Plus => Number(a + b),
            SqlBoundBinaryOperator::Minus => Number(a - b),
            SqlBoundBinaryOperator::Multiply => Number(a * b),
            SqlBoundBinaryOperator::Divide => Number(if *b == 0.0 { f64::NAN } else { a / b }),
            SqlBoundBinaryOperator::Modulo => Number(if *b == 0.0 { f64::NAN } else { a % b }),
            SqlBoundBinaryOperator::And => Boolean(truthy(left) && truthy(right)),
            SqlBoundBinaryOperator::Or => Boolean(truthy(left) || truthy(right)),
            SqlBoundBinaryOperator::Equal => Boolean(a == b),
            SqlBoundBinaryOperator::NotEqual => Boolean(a != b),
            SqlBoundBinaryOperator::Less => Boolean(a < b),
            SqlBoundBinaryOperator::Greater => Boolean(a > b),
            SqlBoundBinaryOperator::LessEqual => Boolean(a <= b),
            SqlBoundBinaryOperator::GreaterEqual => Boolean(a >= b),
            SqlBoundBinaryOperator::Like => Null,
        },
        (String(a), String(b)) => match op {
            SqlBoundBinaryOperator::Equal => Boolean(a == b),
            SqlBoundBinaryOperator::NotEqual => Boolean(a != b),
            SqlBoundBinaryOperator::Less => Boolean(a < b),
            SqlBoundBinaryOperator::Greater => Boolean(a > b),
            SqlBoundBinaryOperator::LessEqual => Boolean(a <= b),
            SqlBoundBinaryOperator::GreaterEqual => Boolean(a >= b),
            SqlBoundBinaryOperator::Like => Boolean(like_match(a, b)),
            SqlBoundBinaryOperator::And => Boolean(truthy(left) && truthy(right)),
            SqlBoundBinaryOperator::Or => Boolean(truthy(left) || truthy(right)),
            _ => Null,
        },
        (Boolean(a), Boolean(b)) => match op {
            SqlBoundBinaryOperator::And => Boolean(*a && *b),
            SqlBoundBinaryOperator::Or => Boolean(*a || *b),
            SqlBoundBinaryOperator::Equal => Boolean(a == b),
            SqlBoundBinaryOperator::NotEqual => Boolean(a != b),
            _ => Null,
        },
        _ => Null,
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

fn execute_error(message: &str) -> SqlFrontendError {
    SqlFrontendError { message: message.into() }
}

/// NULLs sort last (standard SQL behavior). Mixed types collate equal.
fn compare_values(a: Option<&SqlValue>, b: Option<&SqlValue>) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    match (a, b) {
        (None, None) | (Some(SqlValue::Null), Some(SqlValue::Null)) => Ordering::Equal,
        (None | Some(SqlValue::Null), Some(_)) => Ordering::Greater,
        (Some(_), None | Some(SqlValue::Null)) => Ordering::Less,
        (Some(x), Some(y)) => match (x, y) {
            (SqlValue::Number(n1), SqlValue::Number(n2)) => n1.partial_cmp(n2).unwrap_or(Ordering::Equal),
            (SqlValue::String(s1), SqlValue::String(s2)) => s1.cmp(s2),
            (SqlValue::Boolean(b1), SqlValue::Boolean(b2)) => b1.cmp(b2),
            _ => Ordering::Equal,
        },
    }
}
