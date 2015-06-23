//! Oak SQL frontend entry point for compatibility gateways.
//!
//! This module only parses the SQL surface. It does not execute SQL, own a
//! query planner, or bypass the YY execution model.

/// Structured failure from the Oak SQL frontend.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlFrontendError {
    /// Diagnostic text returned by the Oak parser.
    pub message: String,
}

/// Parses one SQL surface statement through the official Oak SQL frontend.
pub fn parse_sql(source: &str) -> Result<oak_sql::ast::SqlRoot, SqlFrontendError> {
    oak_sql::parse(source).map_err(|message| SqlFrontendError { message })
}
