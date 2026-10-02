use crate::{Key, StoredValue};

/// One versioned KV record inside a shard.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Record key.
    pub key: Key,
    /// Stored payload.
    pub value: StoredValue,
    /// Monotonic revision inside the shard.
    pub revision: u64,
}
