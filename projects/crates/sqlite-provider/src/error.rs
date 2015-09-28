//! Structured provider errors.

use thiserror::Error;

/// Stable diagnostic codes for cross-product conformance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SqliteErrorCode {
    /// Upstream SQLite rejected the statement or connection state.
    SqliteNative,
    /// Host filesystem or path failure.
    Io,
    /// Contract or caller policy violation.
    Policy,
    /// Provider capability is not advertised for this connection.
    Unsupported,
    /// Parameter binding or value conversion failed before execution.
    InvalidParameter,
}

/// Structured error returned by every provider implementation.
#[derive(Debug, Error, Clone, PartialEq)]
#[error("{code:?}: {message}")]
pub struct SqliteError {
    /// Stable contract-side classification.
    pub code: SqliteErrorCode,
    /// Human-readable detail safe to surface in logs.
    pub message: String,
    /// Native SQLite result code when available.
    pub sqlite_result_code: Option<i32>,
    /// Statement text associated with the failure when known.
    pub statement: Option<String>,
}

impl SqliteError {
    /// Builds a native SQLite failure.
    pub fn sqlite(message: impl Into<String>, sqlite_result_code: Option<i32>, statement: Option<String>) -> Self {
        Self { code: SqliteErrorCode::SqliteNative, message: message.into(), sqlite_result_code, statement }
    }

    /// Builds a policy failure.
    pub fn policy(message: impl Into<String>) -> Self {
        Self { code: SqliteErrorCode::Policy, message: message.into(), sqlite_result_code: None, statement: None }
    }

    /// Builds an unsupported capability failure.
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self { code: SqliteErrorCode::Unsupported, message: message.into(), sqlite_result_code: None, statement: None }
    }
}
