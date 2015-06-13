//! MySQL disguise gateway for YYDS.
//!
//! Accepts a tiny MySQL client surface (`SELECT 1`, `SELECT version()`) and
//! answers with YYDS-backed metadata. This is not a MySQL server implementation.

use yyds_gateway::{GatewayConfig, GatewayKind, version_label};
use yyds_types::{Error, Result};

/// Gateway kind implemented by this crate.
pub const GATEWAY_KIND: GatewayKind = GatewayKind::Mysql;

/// Gateway identifier used in logs and diagnostics.
pub const GATEWAY_ID: &str = GATEWAY_KIND.id();

/// Default MySQL disguise listen port.
pub const DEFAULT_PORT: u16 = GATEWAY_KIND.default_port();

fn matches_select_one(sql: &str) -> bool {
    let normalized = sql.trim().trim_end_matches(';').trim();
    normalized.eq_ignore_ascii_case("select 1")
}

fn matches_select_version(sql: &str) -> bool {
    let normalized = sql.trim().trim_end_matches(';').trim();
    normalized.eq_ignore_ascii_case("select version()")
}

/// Handles one SQL statement from a MySQL disguise client.
pub fn handle_sql(sql: &str) -> Result<Vec<String>> {
    let trimmed = sql.trim();
    if trimmed.is_empty() {
        return Ok(vec![format!("mysql  Ver {} for YYDS", version_label())]);
    }

    if matches_select_one(trimmed) {
        return Ok(vec!["1".into(), "1".into()]);
    }

    if matches_select_version(trimmed) {
        return Ok(vec![version_label()]);
    }

    Err(Error::Unsupported("unsupported mysql statement"))
}

/// Banner shown when a disguise server starts.
pub fn startup_banner(port: u16) -> String {
    yyds_gateway::startup_banner(&GatewayConfig::new(GATEWAY_KIND).with_port(port))
}
