//! Result column metadata derived from an executable SQL plan.
//!
//! Protocol adapters use this to answer statement-level `Describe` without
//! executing the statement and without inferring types from row values.

use std::collections::BTreeMap;

use crate::{
    catalog_bind::SqlFieldType,
    sql::{SqlBoundBinaryOperator, SqlBoundLiteral, SqlBoundUnaryOperator},
    sql_planner::{JoinPlan, ReadPlan, ReturningProjections, ScanPlan, SqlExecutionPlan},
    SqlCatalogBoundExpression,
};

/// One described result column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlColumn {
    /// Column label as reported to the client.
    pub name: String,
    /// Declared gateway field type.
    pub field_type: SqlFieldType,
}

/// Describes the result shape of a planned statement. Writes have no shape.
pub fn describe_plan(plan: &SqlExecutionPlan) -> Vec<SqlColumn> {
    match plan {
        SqlExecutionPlan::Read(plan) => describe_projections(plan),
        SqlExecutionPlan::Scan(plan) => describe_projections(plan),
        SqlExecutionPlan::Join(plan) => describe_projections(plan),
        // 集合运算的结果形状由左操作数定义：右侧已按列数与类型对齐校验过。
        SqlExecutionPlan::SetOperation(plan) => describe_plan(&plan.left),
        SqlExecutionPlan::Scalar(plan) => plan
            .projections
            .iter()
            .map(|(expression, label)| SqlColumn {
                name: label.clone(),
                field_type: infer_expression_type(expression, &BTreeMap::new()),
            })
            .collect(),
        SqlExecutionPlan::Insert(plan) => describe_returning(&plan.returning, &plan.field_types),
        SqlExecutionPlan::MultiInsert(plans) => plans
            .first()
            .map(|plan| describe_returning(&plan.returning, &plan.field_types))
            .unwrap_or_default(),
        SqlExecutionPlan::InsertSelect(plan) => describe_returning(&plan.returning, &plan.field_types),
        SqlExecutionPlan::Update(_)
        | SqlExecutionPlan::UpdateScan(_)
        | SqlExecutionPlan::Delete(_)
        | SqlExecutionPlan::DeleteScan(_) => Vec::new(),
    }
}

/// `INSERT ... RETURNING` 的结果形状：写语句只有带 `RETURNING` 时才有列定义。
fn describe_returning(
    returning: &Option<ReturningProjections>,
    field_types: &BTreeMap<u64, SqlFieldType>,
) -> Vec<SqlColumn> {
    let Some(projections) = returning
    else {
        return Vec::new();
    };
    projections
        .iter()
        .map(|(expression, label)| SqlColumn {
            name: label.clone(),
            field_type: infer_expression_type(expression, field_types),
        })
        .collect()
}

trait ProjectedPlan {
    fn projection_fields(&self) -> &[(u64, String)];
    fn projection_expressions(&self) -> &[(SqlCatalogBoundExpression, String)];
    fn field_types(&self) -> &BTreeMap<u64, SqlFieldType>;
}

impl ProjectedPlan for ReadPlan {
    fn projection_fields(&self) -> &[(u64, String)] {
        &self.projection_fields
    }

    fn projection_expressions(&self) -> &[(SqlCatalogBoundExpression, String)] {
        &self.projection_expressions
    }

    fn field_types(&self) -> &BTreeMap<u64, SqlFieldType> {
        &self.field_types
    }
}

impl ProjectedPlan for ScanPlan {
    fn projection_fields(&self) -> &[(u64, String)] {
        &self.projection_fields
    }

    fn projection_expressions(&self) -> &[(SqlCatalogBoundExpression, String)] {
        &self.projection_expressions
    }

    fn field_types(&self) -> &BTreeMap<u64, SqlFieldType> {
        &self.field_types
    }
}

impl ProjectedPlan for JoinPlan {
    fn projection_fields(&self) -> &[(u64, String)] {
        &self.projection_fields
    }

    fn projection_expressions(&self) -> &[(SqlCatalogBoundExpression, String)] {
        &self.projection_expressions
    }

    fn field_types(&self) -> &BTreeMap<u64, SqlFieldType> {
        &self.field_types
    }
}

/// Execution emits direct field projections before expression projections, so
/// the description follows the same order.
fn describe_projections(plan: &impl ProjectedPlan) -> Vec<SqlColumn> {
    let mut columns = Vec::with_capacity(plan.projection_fields().len() + plan.projection_expressions().len());
    for (field_id, label) in plan.projection_fields() {
        let field_type = plan.field_types().get(field_id).cloned().unwrap_or(SqlFieldType::Unknown).inner();
        columns.push(SqlColumn { name: label.clone(), field_type });
    }
    for (expression, label) in plan.projection_expressions() {
        columns.push(SqlColumn { name: label.clone(), field_type: infer_expression_type(expression, plan.field_types()) });
    }
    columns
}

/// Infers the declared type of an expression. The result only needs to be
/// accurate enough to pick a wire type — execution remains the source of truth.
fn infer_expression_type(expression: &SqlCatalogBoundExpression, field_types: &BTreeMap<u64, SqlFieldType>) -> SqlFieldType {
    use SqlCatalogBoundExpression::*;
    match expression {
        Column(column) => field_types.get(&column.row_id).cloned().unwrap_or(SqlFieldType::Unknown).inner(),
        Literal(literal) => match literal {
            SqlBoundLiteral::Integer(_) => SqlFieldType::Int64,
            SqlBoundLiteral::Number(_) => SqlFieldType::Float64,
            SqlBoundLiteral::String(_) => SqlFieldType::String,
            SqlBoundLiteral::Boolean(_) => SqlFieldType::Bool,
            SqlBoundLiteral::Null => SqlFieldType::Unknown,
        },
        Wildcard => SqlFieldType::Unknown,
        Function { name, args, distinct } => match name.as_str() {
            "count" | "length" => SqlFieldType::Int64,
            "avg" => SqlFieldType::Float64,
            "sum" | "max" | "min" | "abs" | "round" | "coalesce" => {
                args.first().map(|arg| infer_expression_type(arg, field_types)).unwrap_or(SqlFieldType::Unknown)
            }
            _ => SqlFieldType::String,
        },
        Binary { left, operator, right } => match operator {
            SqlBoundBinaryOperator::And
            | SqlBoundBinaryOperator::Or
            | SqlBoundBinaryOperator::Equal
            | SqlBoundBinaryOperator::NotEqual
            | SqlBoundBinaryOperator::Less
            | SqlBoundBinaryOperator::Greater
            | SqlBoundBinaryOperator::LessEqual
            | SqlBoundBinaryOperator::GreaterEqual
            | SqlBoundBinaryOperator::Like => SqlFieldType::Bool,
            SqlBoundBinaryOperator::Divide | SqlBoundBinaryOperator::Modulo => SqlFieldType::Float64,
            SqlBoundBinaryOperator::Plus | SqlBoundBinaryOperator::Minus | SqlBoundBinaryOperator::Multiply => {
                merge_numeric(infer_expression_type(left, field_types), infer_expression_type(right, field_types))
            }
        },
        Unary { operator, expression } => match operator {
            SqlBoundUnaryOperator::Not
            | SqlBoundUnaryOperator::IsNull
            | SqlBoundUnaryOperator::IsNotNull
            | SqlBoundUnaryOperator::IsTruth { .. } => SqlFieldType::Bool,
            SqlBoundUnaryOperator::Plus | SqlBoundUnaryOperator::Minus => infer_expression_type(expression, field_types),
        },
        InSubquery { .. } | InValues { .. } | Exists { .. } => SqlFieldType::Bool,
        // 标量子查询的声明类型就是它唯一输出列的类型，绑定期已保证只有一列。
        ScalarSubquery { plan } => {
            describe_plan(plan).first().map(|column| column.field_type.clone()).unwrap_or(SqlFieldType::Unknown)
        }
        Case { branches, else_branch } => {
            // 取第一个 THEN 分支的类型作为推断结果；更精确的类型合并留给执行期。
            branches
                .first()
                .map(|(_, then)| infer_expression_type(then, field_types))
                .or_else(|| else_branch.as_ref().map(|e| infer_expression_type(e, field_types)))
                .unwrap_or(SqlFieldType::Unknown)
        }
        Cast { target, .. } => cast_target_to_field_type(target),
    }
}

fn merge_numeric(left: SqlFieldType, right: SqlFieldType) -> SqlFieldType {
    use SqlFieldType::*;
    match (left.inner(), right.inner()) {
        (Float64, _) | (_, Float64) => Float64,
        (Int64, _) | (_, Int64) => Int64,
        (Int32, _) | (_, Int32) => Int32,
        (String, _) | (_, String) => String,
        _ => Unknown,
    }
}

fn cast_target_to_field_type(target: &str) -> SqlFieldType {
    let t = target.trim().to_ascii_uppercase();
    let base_type = t.split('(').next().unwrap_or(&t);
    match base_type {
        "INTEGER" | "INT" | "INT4" | "BIGINT" | "SMALLINT" => SqlFieldType::Int64,
        "REAL" | "DOUBLE" | "FLOAT" | "NUMBER" => SqlFieldType::Float64,
        "TEXT" | "STRING" | "VARCHAR" | "CHAR" | "CHARACTER" => SqlFieldType::String,
        _ => SqlFieldType::Unknown,
    }
}
