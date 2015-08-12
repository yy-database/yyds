//! SQL compatibility binding against a published YYDS VOS catalog.

use crate::sql::{
    bind_sql, SqlBoundExpression, SqlBoundLiteral, SqlBoundProjection, SqlBoundSelect,
    SqlBoundStatement, SqlBoundUnaryOperator, SqlBoundBinaryOperator, SqlFrontendError,
};

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
        let schema = catalog
            .schema()
            .ok_or_else(|| catalog_error("YYDS catalog has no published schema"))?;
        if let Some(contract) = catalog.resolved_contract() {
            return Self::from_resolved_contract(schema.version, contract);
        }
        let identity = catalog
            .identity()
            .ok_or_else(|| catalog_error("YYDS catalog has no published identity snapshot"))?;
        Self::from_snapshot(schema.version, identity)
    }

    /// Projects gateway table identities from a validated resolved contract.
    pub fn from_resolved_contract(
        schema_version: u32,
        contract: &vos::ResolvedContract,
    ) -> Result<Self, SqlFrontendError> {
        if schema_version == 0 {
            return Err(catalog_error("catalog schema version must be nonzero"));
        }
        contract.validate().map_err(|error| {
            catalog_error(&format!("invalid resolved VOS contract {}: {}", error.code, error.message))
        })?;
        let mut type_ids = std::collections::BTreeSet::new();
        let mut field_ids = std::collections::BTreeSet::new();
        let tables = contract
            .types
            .iter()
            .filter(|entry| entry.kind == vos::contract::TypeContractKind::Table)
            .map(|entry| {
                if entry.type_id == 0 || !type_ids.insert(entry.type_id) {
                    return Err(catalog_error("invalid or duplicate catalog type identity"));
                }
                let name = entry
                    .canonical_path
                    .last()
                    .cloned()
                    .ok_or_else(|| catalog_error("catalog table has no canonical name"))?;
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
                        })
                    })
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(SqlCatalogTable { type_id: entry.type_id, name, fields })
            })
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { schema_version, tables })
    }

    /// Projects identities supplied by the catalog publisher, without reallocating them.
    pub fn from_snapshot(schema_version: u32, snapshot: &vos::ast::CatalogSnapshot) -> Result<Self, SqlFrontendError> {
        if schema_version == 0 {
            return Err(catalog_error("catalog schema version must be nonzero"));
        }
        let mut type_ids = std::collections::BTreeSet::new();
        let mut field_ids = std::collections::BTreeSet::new();
        for entry in &snapshot.types {
            if entry.type_id.0 == 0 || !type_ids.insert(entry.type_id.0) {
                return Err(catalog_error("invalid or duplicate catalog type identity"));
            }
            for field in &entry.fields {
                if field.field_id.0 == 0 || !field_ids.insert(field.field_id.0) {
                    return Err(catalog_error("invalid or duplicate catalog field identity"));
                }
            }
        }
        let tables = snapshot
            .types
            .iter()
            .filter(|entry| entry.kind == vos::ast::TypeKind::Table)
            .map(|entry| SqlCatalogTable {
                type_id: entry.type_id.0,
                name: entry.name.clone(),
                fields: entry
                    .fields
                    .iter()
                    .map(|field| SqlCatalogField {
                        field_id: field.field_id.0,
                        name: field.current_name.clone(),
                    })
                    .collect(),
            })
            .collect();
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
    /// Resolved projections.
    pub projections: Vec<SqlCatalogBoundProjection>,
    /// Resolved optional predicate.
    pub predicate: Option<SqlCatalogBoundExpression>,
}

/// Gateway-owned SQL request after names are bound to a YYDS catalog revision.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlCatalogBoundStatement {
    /// One catalog-bound SELECT request awaiting distributed planning.
    Select(SqlCatalogBoundSelect),
}

/// Binds a parsed SQL surface to a specific catalog revision.
pub fn bind_sql_catalog(
    source: &str,
    catalog: &SqlCatalog,
    expected_schema_version: u32,
) -> Result<SqlCatalogBoundStatement, SqlFrontendError> {
    if catalog.schema_version != expected_schema_version {
        return Err(catalog_error(&format!(
            "SQL catalog schema version mismatch: expected {expected_schema_version}, found {}",
            catalog.schema_version
        )));
    }
    let statement = bind_sql(source)?;
    match statement {
        SqlBoundStatement::Select(select) => bind_select(select, catalog).map(SqlCatalogBoundStatement::Select),
    }
}

fn bind_select(
    select: SqlBoundSelect,
    catalog: &SqlCatalog,
) -> Result<SqlCatalogBoundSelect, SqlFrontendError> {
    let source = select
        .source
        .as_deref()
        .map(|name| find_table(catalog, name))
        .transpose()?;
    let projections = select
        .projections
        .into_iter()
        .map(|projection| bind_projection(projection, source.as_ref(), catalog))
        .collect::<Result<Vec<_>, _>>()?;
    let predicate = select
        .predicate
        .map(|expression| bind_expression(expression, source.as_ref(), catalog))
        .transpose()?;
    Ok(SqlCatalogBoundSelect { source, projections, predicate })
}

fn bind_projection(
    projection: SqlBoundProjection,
    source: Option<&SqlCatalogBoundTable>,
    catalog: &SqlCatalog,
) -> Result<SqlCatalogBoundProjection, SqlFrontendError> {
    match projection {
        SqlBoundProjection::Star => source
            .cloned()
            .map(SqlCatalogBoundProjection::Star)
            .ok_or_else(|| catalog_error("SQL star projection requires a catalog table source")),
        SqlBoundProjection::Expression { expression, alias } => Ok(SqlCatalogBoundProjection::Expression {
            expression: bind_expression(expression, source, catalog)?,
            alias,
        }),
    }
}

fn bind_expression(
    expression: SqlBoundExpression,
    source: Option<&SqlCatalogBoundTable>,
    catalog: &SqlCatalog,
) -> Result<SqlCatalogBoundExpression, SqlFrontendError> {
    match expression {
        SqlBoundExpression::Identifier(name) => {
            let field = find_field(&name, source, catalog)?;
            Ok(SqlCatalogBoundExpression::Column(field))
        }
        SqlBoundExpression::Literal(literal) => Ok(SqlCatalogBoundExpression::Literal(literal)),
        SqlBoundExpression::Binary { left, operator, right } => Ok(SqlCatalogBoundExpression::Binary {
            left: Box::new(bind_expression(*left, source, catalog)?),
            operator,
            right: Box::new(bind_expression(*right, source, catalog)?),
        }),
        SqlBoundExpression::Unary { operator, expression } => Ok(SqlCatalogBoundExpression::Unary {
            operator,
            expression: Box::new(bind_expression(*expression, source, catalog)?),
        }),
    }
}

fn find_table(catalog: &SqlCatalog, name: &str) -> Result<SqlCatalogBoundTable, SqlFrontendError> {
    let matches = catalog.tables.iter().filter(|table| table.name == name).collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Err(catalog_error(&format!("unknown SQL table: {name}"))),
        [table] => Ok(SqlCatalogBoundTable { type_id: table.type_id, name: table.name.clone() }),
        _ => Err(catalog_error(&format!("ambiguous SQL table: {name}"))),
    }
}

fn find_field(
    name: &str,
    source: Option<&SqlCatalogBoundTable>,
    catalog: &SqlCatalog,
) -> Result<SqlCatalogBoundColumn, SqlFrontendError> {
    let source = source.ok_or_else(|| catalog_error("SQL column reference requires a table source"))?;
    let table = catalog
        .tables
        .iter()
        .find(|candidate| candidate.type_id == source.type_id)
        .ok_or_else(|| catalog_error("catalog table identity is not published"))?;
    let matches = table
        .fields
        .iter()
        .filter(|field| field.name == name)
        .collect::<Vec<_>>();
    match matches.as_slice() {
        [] => Err(catalog_error(&format!("unknown SQL column: {name}"))),
        [field] => Ok(SqlCatalogBoundColumn {
            type_id: table.type_id,
            field_id: field.field_id,
            table_name: table.name.clone(),
            field_name: field.name.clone(),
        }),
        _ => Err(catalog_error(&format!("ambiguous SQL column: {name}"))),
    }
}

fn catalog_error(message: &str) -> SqlFrontendError {
    SqlFrontendError { message: message.into() }
}
