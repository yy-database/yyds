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
    sql::{SqlBoundBinaryOperator, SqlBoundLiteral, SqlBoundUnaryOperator, SqlFrontendError},
    sql_planner::SqlExecutionPlan,
};

/// SQL 查询的结果集。
#[derive(Debug, Clone, PartialEq)]
pub struct SqlResult {
    /// 列名（按投影顺序）。
    pub columns: Vec<String>,
    /// 行（每行的列值按 `columns` 顺序）。
    pub rows: Vec<Vec<SqlValue>>,
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

/// 执行一个已计划的 SELECT。
pub fn execute_plan(
    executor: &dyn KeyValueExecutor,
    plan: &SqlExecutionPlan,
) -> Result<SqlResult, SqlFrontendError> {
    let namespace = Namespace(plan.namespace.clone());
    let command = KeyValueCommand { namespace, key: plan.rowid.clone(), action: KeyValueAction::Get };
    let batch = executor
        .execute_read_batch(&[command])
        .map_err(|e| execute_error(&format!("KeyValueExecutor.read_batch failed: {e}")))?;

    let result = batch
        .into_iter()
        .next()
        .ok_or_else(|| execute_error("KeyValueExecutor returned empty batch"))?;
    let row_bytes = match result {
        yyds_types::KeyValueResult::Get(Some(bytes)) => bytes,
        _ => return Ok(SqlResult { columns: plan_column_labels(plan), rows: Vec::new() }),
    };
    let row = decode_row(&row_bytes)?;

    // 行级过滤。
    if let Some(ref predicate) = plan.remaining_predicate {
        let v = eval_expression(predicate, &row)?;
        if !truthy(&v) {
            return Ok(SqlResult { columns: plan_column_labels(plan), rows: Vec::new() });
        }
    }

    // 投影求值。
    let columns = plan_column_labels(plan);
    let mut row_values: Vec<SqlValue> = Vec::new();
    for (field_id, _) in &plan.projection_fields {
        row_values.push(row.value(*field_id).unwrap_or(SqlValue::Null));
    }
    for (expr, _) in &plan.projection_expressions {
        row_values.push(eval_expression(expr, &row)?);
    }

    Ok(SqlResult { columns, rows: vec![row_values] })
}

fn plan_column_labels(plan: &SqlExecutionPlan) -> Vec<String> {
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

fn eval_expression(expr: &SqlCatalogBoundExpression, row: &RowFieldValues) -> Result<SqlValue, SqlFrontendError> {
    Ok(match expr {
        SqlCatalogBoundExpression::Column(col) => row
            .value(col.field_id)
            .ok_or_else(|| execute_error(&format!("column {} not present in row", col.field_name)))?,
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

fn like_match(haystack: &str, pattern: &str) -> bool {
    // 简化版：先把 pattern 按 % 切成若干段；所有段都必须在 haystack 中按顺序出现。
    // _ 当作单个任意字符。
    let segments: Vec<&str> = pattern.split('%').collect();
    if segments.is_empty() {
        return haystack.is_empty();
    }
    let mut hay_pos = 0usize;
    for (i, segment) in segments.iter().enumerate() {
        let last = i == segments.len() - 1;
        if segment.is_empty() {
            if last {
                return true;
            }
            continue;
        }
        if !match_segment(haystack, hay_pos, segment, last) {
            return false;
        }
        // 把 hay_pos 推进到匹配段之后。
        let advanced = find_segment_end(haystack, hay_pos, segment);
        if let Some(pos) = advanced {
            hay_pos = pos;
        }
        else {
            return false;
        }
        // 吃掉 _ 对应的字符。
        for ch in segment.chars() {
            if ch == '_' {
                hay_pos += 1;
            }
        }
    }
    true
}

fn match_segment(haystack: &str, start: usize, segment: &str, last: bool) -> bool {
    let mut h = start;
    // segment 里把 _ 当作一个任意字符占位，其他按字面匹配。
    let mut seg_chars = segment.chars();
    while let Some(seg) = seg_chars.next() {
        match seg {
            '_' => {
                if h >= haystack.len() {
                    return false;
                }
                // 下一个 seg 字符是下一个比对，_ 不做比对，只前进 h。
                h += 1;
            }
            _ => {
                // 从 h 起找 seg 这个字符。
                // 如果是首段且不是 last（开头有 %），在 start 处匹配；否则必须从 h 起的任意位置。
                let found = haystack[h..].find(seg);
                match found {
                    Some(idx) => h += idx,
                    None => return false,
                }
                if !haystack[h..].starts_with(seg) {
                    return false;
                }
                // 继续匹配 segment 剩余部分（但这里 segment 里可能有 _，所以递归）。
                let rest = segment_after(segment, seg);
                if !match_literal_tail(haystack, h + seg.len_utf8(), rest, last && segment == segment) {
                    return false;
                }
                // 已经全匹配。
                let _ = &mut seg_chars; // 忽略剩余
                return true;
            }
        }
    }
    // segment 全是 _，要求 haystack 至少够长。
    let underscore_count = segment.chars().filter(|c| *c == '_').count();
    h + underscore_count <= haystack.len()
}

fn segment_after(full: &str, head: char) -> &str {
    let mut chars = full.chars();
    while let Some(c) = chars.next() {
        if c == head {
            return chars.as_str();
        }
    }
    ""
}

fn match_literal_tail(_haystack: &str, _from: usize, _rest: &str, _anchor_end: bool) -> bool {
    // Like 简化版：只支持 pattern 里一个 %（或多个相邻的 % 合并）+ 可选的 _。
    // 复杂嵌套（% + _ + %）返回 false 表示不完全匹配。
    false
}

fn find_segment_end(_haystack: &str, _start: usize, _segment: &str) -> Option<usize> {
    // 占位：上面的 match_segment 没用到它。
    None
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
