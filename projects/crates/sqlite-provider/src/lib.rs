//! Versioned SQLite execution provider contract shared by Iris and YYDS.
//!
//! This crate defines capabilities, typed values, structured errors, catalog
//! inspection shapes, and the [`SqliteProvider`] trait. It does not depend on
//! `rusqlite`, YYDS cluster runtime, or Iris IR types.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod capability;
mod catalog;
mod error;
mod provider;
mod result;
mod value;

pub use capability::{JournalMode, SqliteCapabilities, TransactionCapability, WalCapability};
pub use catalog::{CatalogColumn, CatalogSnapshot, CatalogTable};
pub use error::{SqliteError, SqliteErrorCode};
pub use provider::{OpenOptions, SqliteProvider};
pub use result::StatementResult;
pub use value::SqliteValue;

/// Frozen contract version exchanged across Iris and YYDS provider surfaces.
pub const CONTRACT_VERSION: &str = "1.0.0";

/// Stable provider backend label for diagnostics.
pub const PROVIDER_BACKEND_ID: &str = "sqlite-provider";
