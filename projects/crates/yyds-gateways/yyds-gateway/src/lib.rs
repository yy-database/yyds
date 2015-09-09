//! Shared disguise gateway core for YYDS.
//!
//! Protocol adapters (`yyds-gateway-redis`, `yyds-gateway-mysql`, `yyds-gateway-pgsql`)
//! share configuration, banners, and legacy-surface guards from this crate.

use std::path::PathBuf;

use yyds_types::version;

mod catalog_bind;
mod config;
mod kind;
mod sql;

pub use crate::catalog_bind::{
    SqlCatalog, SqlCatalogBoundColumn, SqlCatalogBoundExpression, SqlCatalogBoundProjection, SqlCatalogBoundSelect,
    SqlCatalogBoundStatement, SqlCatalogBoundTable, SqlCatalogField, SqlCatalogTable, bind_sql_catalog,
};

pub use crate::{
    config::GatewayConfig,
    kind::{ALL_GATEWAY_KINDS, GatewayKind},
    sql::{
        SqlBoundBinaryOperator, SqlBoundExpression, SqlBoundLiteral, SqlBoundProjection, SqlBoundSelect, SqlBoundStatement,
        SqlBoundUnaryOperator, SqlFrontendError, SqlSessionCommand, SqlTransactionAction, bind_sql, parse_session_command,
        parse_sql,
    },
};

/// Legacy surfaces that must never be revived as YYDS product paths.
pub const FORBIDDEN_LEGACY_SURFACES: &[&str] = &[
    "yyds-gateway/src/sql",
    "yyds-odbc",
    "we-trust-sqlite",
    "we-trust-mysql",
    "we-trust-postgres",
    "we-trust-sqlserver",
    "query_with_sql",
    "SqlQuery",
];

/// Returns the YYDS version label used in disguise responses (`yyds-0.1.0`).
pub fn version_label() -> String {
    format!("yyds-{}", version())
}

/// Startup banner for a configured disguise gateway listener.
pub fn startup_banner(config: &GatewayConfig) -> String {
    format!("{} listening on {} (YYDS {version})", config.kind.crate_name(), config.port, version = version())
}

/// Returns true when `surface` is a forbidden legacy gateway path.
pub fn is_forbidden_legacy_surface(surface: &str) -> bool {
    FORBIDDEN_LEGACY_SURFACES.iter().any(|entry| entry.eq_ignore_ascii_case(surface))
}

/// Optional catalog path attached to a gateway process.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct GatewayCatalog {
    path: Option<PathBuf>,
}

impl GatewayCatalog {
    /// Creates an empty catalog binding.
    pub fn new() -> Self {
        Self::default()
    }

    /// Binds the gateway to a `.yyds` catalog file path.
    pub fn with_path(path: impl Into<PathBuf>) -> Self {
        Self { path: Some(path.into()) }
    }

    /// Returns the configured catalog path, if any.
    pub fn path(&self) -> Option<&PathBuf> {
        self.path.as_ref()
    }
}
