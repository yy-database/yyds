//! Protocol-neutral key/value commands consumed by YYDS execution.

use crate::Namespace;

/// One operation on a namespace-scoped opaque key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyValueCommand {
    /// Logical tenant or key-space boundary.
    pub namespace: Namespace,
    /// Opaque key bytes used for stable shard routing.
    pub key: Vec<u8>,
    /// Requested operation.
    pub action: KeyValueAction,
}

/// Read, replace, or delete one logical key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyValueAction {
    /// Return the current value, if any.
    Get,
    /// Check whether the key currently has a value.
    Exists,
    /// Replace the current value with these bytes.
    Put(Vec<u8>),
    /// Store these bytes only when the key is absent.
    PutIfAbsent(Vec<u8>),
    /// Remove the current value if it exists.
    Delete,
    /// Atomically add a signed delta to a decimal integer value, treating an absent key as zero.
    IncrementBy(i64),
}

/// Result of applying one [`KeyValueCommand`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyValueResult {
    /// Value returned by a read.
    Get(Option<Vec<u8>>),
    /// Whether the key currently has a value.
    Exists(bool),
    /// Revision assigned to a successful write.
    Put { revision: u64 },
    /// Revision assigned when a conditional write inserts the key, or `None` when it exists.
    PutIfAbsent { revision: Option<u64> },
    /// Whether a delete found and removed a value.
    Delete { removed: bool },
    /// Result of an atomic signed integer increment.
    Increment { value: i64 },
}
