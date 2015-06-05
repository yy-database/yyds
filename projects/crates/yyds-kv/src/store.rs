use yyds_types::{Error, Result};

use crate::{Key, Record, StoredValue};

/// Shard-local KV surface. Implementations back one `.yykv` shard.
pub trait KvStore {
    /// Returns the latest record for `key`, if present.
    fn get(&self, key: &Key) -> Result<Option<Record>>;

    /// Inserts or replaces `value` and returns the new revision.
    fn put(&mut self, key: Key, value: StoredValue) -> Result<u64>;

    /// Deletes `key` when present. Returns whether a record was removed.
    fn delete(&mut self, key: &Key) -> Result<bool>;
}

/// In-memory shard used by unit tests and early integrations.
#[derive(Debug, Default)]
pub struct MemoryShard {
    records: std::collections::HashMap<Key, Record>,
    next_revision: u64,
}

impl MemoryShard {
    /// Creates an empty shard.
    pub fn new() -> Self {
        Self::default()
    }
}

impl KvStore for MemoryShard {
    fn get(&self, key: &Key) -> Result<Option<Record>> {
        Ok(self.records.get(key).cloned())
    }

    fn put(&mut self, key: Key, value: StoredValue) -> Result<u64> {
        self.next_revision += 1;
        let revision = self.next_revision;
        let record = Record {
            key: key.clone(),
            value,
            revision,
        };
        self.records.insert(key, record);
        Ok(revision)
    }

    fn delete(&mut self, key: &Key) -> Result<bool> {
        Ok(self.records.remove(key).is_some())
    }
}

/// Compare-and-set helper built on top of [`KvStore`].
pub fn compare_and_put(
    store: &mut dyn KvStore,
    key: Key,
    expect_revision: Option<u64>,
    value: StoredValue,
) -> Result<u64> {
    let current = store.get(&key)?;
    match (expect_revision, current) {
        (None, Some(_)) => Err(Error::CasConflict {
            key: key.display(),
        }),
        (Some(expected), Some(record)) if record.revision != expected => Err(Error::CasConflict {
            key: key.display(),
        }),
        _ => store.put(key, value),
    }
}
