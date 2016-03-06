//! Result column metadata derived from an executable SQL plan.
//!
//! Protocol adapters use this to answer statement-level `Describe` without
//! executing the statement and without inferring types from row values.

use std::collections::BTreeMap;

use crate::{
    catalog_bind::SqlFieldType,
    sql::{SqlBoundBinaryOperator, SqlBoundLiteral, SqlBoundUnaryOperator},
    sql_planner::{JoinPlan, ReadPlan, ScanPlan, SqlExecutionPlan},
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
        SqlExecutionPlan::Scalar(plan) => plan
            .projections
            .iter()
            .map(|(expression, label)| SqlColumn {
                name: label.clone(),
                field_type: infer_expression_type(expression, &BTreeMap::new()),
            })
            .collect(),
        SqlExecutionPlan::Insert(_)
        | SqlExecutionPlan::MultiInsert(_)
        | SqlExecutionPlan::Update(_)
        | SqlExecutionPlan::UpdateScan(_)
        | SqlExecutionPlan::Delete(_)
        | SqlExecutionPlan::DeleteScan(_) => Vec::new(),
    }
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
        Column(column) => field_types.get(&column.field_id).cloned().unwrap_or(SqlFieldType::Unknown).inner(),
        Literal(literal) => match literal {
            SqlBoundLiteral::Integer(_) => SqlFieldType::Int64,
            SqlBoundLiteral::Number(_) => SqlFieldType::Float64,
            SqlBoundLiteral::String(_) => SqlFieldType::String,
            SqlBoundLiteral::Boolean(_) => SqlFieldType::Bool,
            SqlBoundLiteral::Null => SqlFieldType::Unknown,
        },
        Wildcard => SqlFieldType::Unknown,
        Function { name, args } => match name.as_str() {
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
        InSubquery { .. } | InValues { .. } => SqlFieldType::Bool,
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
