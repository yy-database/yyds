//! Catalog inspection shapes returned by providers.

/// One observed physical column.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogColumn {
    /// Physical column name.
    pub name: String,
    /// Declared type name from catalog metadata.
    pub type_name: String,
    /// Whether NULL is allowed.
    pub nullable: bool,
    /// Whether the column participates in the primary key.
    pub primary_key: bool,
}

/// One observed user table.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogTable {
    /// Physical table name.
    pub name: String,
    /// Observed columns in catalog order.
    pub columns: Vec<CatalogColumn>,
}

/// Provider catalog snapshot excluding caller-specific filtering.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CatalogSnapshot {
    /// Observed user tables.
    pub tables: Vec<CatalogTable>,
}

impl CatalogSnapshot {
    /// Finds a table by exact physical name.
    pub fn table(&self, name: &str) -> Option<&CatalogTable> {
        self.tables.iter().find(|table| table.name == name)
    }
}
