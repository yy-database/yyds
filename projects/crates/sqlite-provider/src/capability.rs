//! Advertised provider capabilities.

use crate::CONTRACT_VERSION;

/// Transaction support advertised by a provider connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TransactionCapability {
    /// Whether `BEGIN IMMEDIATE` is supported.
    pub begin_immediate: bool,
    /// Whether nested savepoints are supported.
    pub savepoints: bool,
}

impl TransactionCapability {
    /// Full transaction support used by bundled upstream SQLite engines.
    pub const FULL: Self = Self { begin_immediate: true, savepoints: true };
}

/// WAL and locking capability metadata (declarative, not a guarantee of mode).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalCapability {
    /// Provider can operate on WAL-mode files through upstream SQLite.
    pub wal_files: bool,
    /// Provider exposes journal mode inspection.
    pub inspect_journal_mode: bool,
}

impl WalCapability {
    /// Default upstream SQLite WAL capability advertisement.
    pub const UPSTREAM: Self = Self { wal_files: true, inspect_journal_mode: true };
}

/// Observed on-disk journal mode for a live connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JournalMode {
    /// DELETE journal mode.
    Delete,
    /// WAL journal mode.
    Wal,
    /// Other or unknown journal mode string from `PRAGMA journal_mode`.
    Other,
}

/// Capability snapshot for one open provider connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqliteCapabilities {
    /// Contract version implemented by the provider.
    pub contract_version: &'static str,
    /// Whether the connection was opened read-only.
    pub read_only: bool,
    /// Transaction capability.
    pub transactions: TransactionCapability,
    /// Single-statement execution with bind parameters.
    pub parameterized_statements: bool,
    /// Multi-statement batch execution without row callbacks.
    pub batch_statements: bool,
    /// Live catalog inspection through SQL metadata queries.
    pub catalog_inspection: bool,
    /// WAL / locking metadata.
    pub wal: WalCapability,
}

impl SqliteCapabilities {
    /// Default capabilities for a read/write bundled upstream SQLite connection.
    pub fn read_write() -> Self {
        Self {
            contract_version: CONTRACT_VERSION,
            read_only: false,
            transactions: TransactionCapability::FULL,
            parameterized_statements: true,
            batch_statements: true,
            catalog_inspection: true,
            wal: WalCapability::UPSTREAM,
        }
    }

    /// Capabilities for a read-only connection.
    pub fn read_only() -> Self {
        Self { read_only: true, ..Self::read_write() }
    }
}
