#![deny(missing_debug_implementations)]
#![warn(missing_docs, rustdoc::missing_crate_level_docs)]
#![doc = include_str!("../readme.md")]

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use yyds_kv::{FileShard, InlineValue, Key, KvStore, StoredValue};
use yyds_types::{Error, KeyValueAction, KeyValueCommand, KeyValueResult, Namespace, ShardId, ShardMap};

/// Execution surface consumed by protocol adapters.
pub trait KeyValueExecutor: Debug + Send + Sync {
    /// Routes and applies one namespace-scoped command.
    fn execute(&self, command: KeyValueCommand) -> yyds_types::Result<KeyValueResult>;

    /// Reads an ordered batch atomically, preserving duplicate keys.
    /// Executors without an atomic read contract must reject the request.
    fn execute_read_batch(&self, _commands: &[KeyValueCommand]) -> yyds_types::Result<Vec<KeyValueResult>> {
        Err(Error::Unsupported("atomic batch reads are not implemented by this executor"))
    }

    /// Ordered prefix scan across all mounted shards for one namespace.
    ///
    /// Returns up to `limit` (key_bytes, inline_value_bytes) pairs whose key bytes start with
    /// `prefix`, compare greater than `after` (exclusive), less than `before` (exclusive), sorted
    /// by key bytes ascending.
    fn execute_scan(
        &self,
        _namespace: &Namespace,
        _prefix: &[u8],
        _after: Option<&[u8]>,
        _before: Option<&[u8]>,
        _limit: usize,
    ) -> yyds_types::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Err(Error::Unsupported("scan is not implemented by this executor"))
    }

    /// Opens a local transaction handle bound to this executor.
    ///
    /// The handle lets callers issue commands through a transaction boundary so that a gateway
    /// can bind `BEGIN` / `COMMIT` / `ROLLBACK` protocol verbs to a real typed object. Executors
    /// without transaction support return `Error::Unsupported`.
    fn begin_transaction(&self) -> yyds_types::Result<Box<dyn TransactionHandle>> {
        Err(Error::Unsupported("this executor does not support local transactions"))
    }
}

/// A local transaction handle that exposes the same command surface as the executor but
/// wraps it in a begin/commit/rollback contract.
///
/// The current reference implementation (`LocalTransactionHandle`) delegates each command
/// immediately to the underlying shard so that commits and rollbacks are currently no-ops.
/// This design is stable enough that gateway layers can bind BEGIN/COMMIT/ROLLBACK today,
/// and a future buffered implementation that batches writes until commit can replace it
/// without changing the gateway contract.
pub trait TransactionHandle: Debug {
    /// Executes one command within the transaction.
    fn execute(&self, command: KeyValueCommand) -> yyds_types::Result<KeyValueResult>;

    /// Reads an ordered batch within the transaction, preserving duplicate keys.
    fn execute_read_batch(&self, _commands: &[KeyValueCommand]) -> yyds_types::Result<Vec<KeyValueResult>> {
        Err(Error::Unsupported("transaction batch reads are not implemented by this handle"))
    }

    /// Finalises the transaction and persists any pending state.
    fn commit(self: Box<Self>) -> yyds_types::Result<()>;

    /// Aborts the transaction and discards any pending state.
    fn rollback(self: Box<Self>);
}

/// Routes commands to mounted local shards under one immutable routing map.
#[derive(Debug)]
pub struct LocalExecutor<S = FileShard> {
    routing: ShardMap,
    shards: HashMap<ShardId, Mutex<S>>,
}

impl<S> LocalExecutor<S>
where
    S: KvStore + Send + Debug,
{
    /// Creates a local executor and requires an exact mounted-shard match.
    pub fn new(routing: ShardMap, shards: HashMap<ShardId, S>) -> yyds_types::Result<Self> {
        if shards.len() != routing.shards().len() || routing.shards().iter().any(|shard| !shards.contains_key(shard)) {
            return Err(Error::Unsupported("mounted shards must exactly match the published routing map"));
        }
        Ok(Self { routing, shards: shards.into_iter().map(|(id, shard)| (id, Mutex::new(shard))).collect() })
    }

    /// Returns the immutable routing map used by this executor.
    pub fn routing(&self) -> &ShardMap {
        &self.routing
    }
}

/// Tracks the in-flight state of a single `(namespace, key)` pair inside a local transaction.
///
/// Only the final state per key is stored so that a second `Put` on the same key simply
/// overwrites the first without needing to replay a multi-value history.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BufferedState {
    /// The key has been set to this value inside the transaction.
    Value(Vec<u8>),
    /// The key has been deleted inside the transaction.
    Deleted,
}

/// Buffered local transaction handle.
///
/// Writes issued through this handle are held in a per-handle buffer until either
/// [`TransactionHandle::commit`] or [`TransactionHandle::rollback`] is called. Reads are
/// served by consulting the buffer first (snapshot reads), then delegating to the underlying
/// executor so that transactions observe their own writes.
#[derive(Debug)]
pub struct LocalTransactionHandle {
    executor: Arc<dyn KeyValueExecutor>,
    buffer: Mutex<HashMap<(String, Vec<u8>), BufferedState>>,
}

impl LocalTransactionHandle {
    /// Creates a new handle bound to an executor via a shared Arc.
    pub fn new(executor: Arc<dyn KeyValueExecutor>) -> Self {
        Self { executor, buffer: Mutex::new(HashMap::new()) }
    }

    /// Resolves the current observable value for `(namespace, key)` by merging the buffer
    /// over the underlying executor state.
    fn resolve(&self, namespace: &str, key: &[u8]) -> yyds_types::Result<Option<Vec<u8>>> {
        let buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
        if let Some(state) = buffer.get(&(namespace.to_owned(), key.to_vec())) {
            return Ok(match state {
                BufferedState::Value(bytes) => Some(bytes.clone()),
                BufferedState::Deleted => None,
            });
        }
        Ok(
            match self.executor.execute(KeyValueCommand {
                namespace: Namespace(namespace.to_owned()),
                key: key.to_vec(),
                action: KeyValueAction::Get,
            })? {
                KeyValueResult::Get(value) => value,
                _ => unreachable!("Get action always yields Get result"),
            },
        )
    }
}

impl TransactionHandle for LocalTransactionHandle {
    fn execute(&self, command: KeyValueCommand) -> yyds_types::Result<KeyValueResult> {
        let KeyValueCommand { namespace, key, action } = command;
        match action {
            KeyValueAction::Get => {
                let value = self.resolve(&namespace.0, &key)?;
                Ok(KeyValueResult::Get(value))
            }
            KeyValueAction::Exists => {
                let value = self.resolve(&namespace.0, &key)?;
                Ok(KeyValueResult::Exists(value.is_some()))
            }
            KeyValueAction::Put(value) => {
                let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
                buffer.insert((namespace.0, key), BufferedState::Value(value));
                Ok(KeyValueResult::Put { revision: 1 })
            }
            KeyValueAction::PutIfAbsent(value) => {
                let exists = self.resolve(&namespace.0, &key)?.is_some();
                if exists {
                    return Ok(KeyValueResult::PutIfAbsent { revision: None });
                }
                let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
                buffer.insert((namespace.0, key), BufferedState::Value(value));
                Ok(KeyValueResult::PutIfAbsent { revision: Some(1) })
            }
            KeyValueAction::PutIfPresent(value) => {
                let exists = self.resolve(&namespace.0, &key)?.is_some();
                if !exists {
                    return Ok(KeyValueResult::PutIfPresent { revision: None });
                }
                let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
                buffer.insert((namespace.0, key), BufferedState::Value(value));
                Ok(KeyValueResult::PutIfPresent { revision: Some(1) })
            }
            KeyValueAction::Delete => {
                let existed = self.resolve(&namespace.0, &key)?.is_some();
                let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
                buffer.insert((namespace.0, key), BufferedState::Deleted);
                Ok(KeyValueResult::Delete { removed: existed })
            }
            KeyValueAction::GetSet(new_value) => {
                let previous = self.resolve(&namespace.0, &key)?;
                let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
                buffer.insert((namespace.0, key), BufferedState::Value(new_value));
                Ok(KeyValueResult::GetSet(previous))
            }
            KeyValueAction::GetDelete => {
                let previous = self.resolve(&namespace.0, &key)?;
                let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
                buffer.insert((namespace.0, key), BufferedState::Deleted);
                Ok(KeyValueResult::GetDelete(previous))
            }
            KeyValueAction::IncrementBy(delta) => {
                let current_bytes = self.resolve(&namespace.0, &key)?;
                let current = match current_bytes {
                    Some(bytes) => parse_integer(&bytes)?,
                    None => 0,
                };
                let value = current.checked_add(delta).ok_or(Error::IntegerOverflow)?;
                let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
                buffer.insert((namespace.0, key), BufferedState::Value(value.to_string().into_bytes()));
                Ok(KeyValueResult::Increment { value })
            }
        }
    }

    fn execute_read_batch(&self, commands: &[KeyValueCommand]) -> yyds_types::Result<Vec<KeyValueResult>> {
        // This is intentionally deferred to Commit 2 so that the buffered snapshot-merge
        // implementation lands in its own reviewed change. For now the blanket Unsupported
        // keeps the gateway contract intact because gateway code still falls back to the
        // single-command `execute` path.
        Err(Error::Unsupported("transaction batch reads land in the next commit"))
    }

    fn commit(self: Box<Self>) -> yyds_types::Result<()> {
        let buffer = self.buffer.into_inner().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
        for ((namespace, key), state) in buffer {
            let command = match state {
                BufferedState::Value(value) => {
                    KeyValueCommand { namespace: Namespace(namespace), key, action: KeyValueAction::Put(value) }
                }
                BufferedState::Deleted => {
                    KeyValueCommand { namespace: Namespace(namespace), key, action: KeyValueAction::Delete }
                }
            };
            self.executor.execute(command)?;
        }
        Ok(())
    }

    fn rollback(self: Box<Self>) {
        // Dropping the handle discards the buffered HashMap; nothing touches the executor.
    }
}

impl<S> KeyValueExecutor for LocalExecutor<S>
where
    S: KvStore + Send + Debug,
{
    fn execute_read_batch(&self, commands: &[KeyValueCommand]) -> yyds_types::Result<Vec<KeyValueResult>> {
        if commands.iter().any(|command| command.action != KeyValueAction::Get) {
            return Err(Error::Unsupported("read batches accept only read actions"));
        }
        let decisions =
            commands.iter().map(|command| self.routing.route(&command.namespace.0, &command.key)).collect::<Vec<_>>();
        let mut guards = HashMap::new();
        for shard_id in self.routing.shards() {
            if decisions.iter().any(|decision| &decision.shard == shard_id) {
                let shard = self.shards.get(shard_id).ok_or(Error::Unsupported("routed shard is not mounted on this node"))?;
                guards.insert(shard_id.clone(), shard.lock().map_err(|_| Error::Corrupt("local shard lock poisoned"))?);
            }
        }
        commands
            .iter()
            .zip(decisions)
            .map(|(command, decision)| {
                let shard = guards.get_mut(&decision.shard).ok_or(Error::Unsupported("batch shard is not locked"))?;
                let key = Key::new(command.namespace.0.clone(), command.key.clone());
                let value = shard
                    .get(&key)?
                    .map(|record| match record.value {
                        StoredValue::Inline(value) => Ok(value.0),
                        StoredValue::Object(_) => {
                            Err(Error::Unsupported("object values are not available through the local KV executor"))
                        }
                    })
                    .transpose()?;
                Ok(KeyValueResult::Get(value))
            })
            .collect()
    }

    fn execute_scan(
        &self,
        namespace: &Namespace,
        prefix: &[u8],
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> yyds_types::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        // Collect results from every mounted shard (a process-local merge of multi-shard scan).
        // Each shard yields ordered records; we merge them sorted by key bytes.
        let mut shard_results: Vec<Vec<(Vec<u8>, Vec<u8>)>> = Vec::new();
        for shard_id in self.routing.shards() {
            let shard = self.shards.get(shard_id).ok_or(Error::Unsupported("shard not mounted"))?;
            let guard = shard.lock().map_err(|_| Error::Corrupt("local shard lock poisoned"))?;
            let records = guard.scan(namespace, prefix, after, before, 0)?; // per-shard unlimited; merge applies global limit
            let mut flat = Vec::with_capacity(records.len());
            for record in records {
                let inline = match record.value {
                    StoredValue::Inline(value) => value.0,
                    StoredValue::Object(_) => {
                        return Err(Error::Unsupported("object values are not available through scan"));
                    }
                };
                flat.push((record.key.bytes, inline));
            }
            shard_results.push(flat);
        }

        // K-way merge — each shard's slice is already sorted by key bytes.
        Ok(merge_sorted(&shard_results, limit))
    }

    fn execute(&self, command: KeyValueCommand) -> yyds_types::Result<KeyValueResult> {
        let decision = self.routing.route(&command.namespace.0, &command.key);
        let shard = self.shards.get(&decision.shard).ok_or(Error::Unsupported("routed shard is not mounted on this node"))?;
        let mut shard = shard.lock().map_err(|_| Error::Corrupt("local shard lock poisoned"))?;
        let key = Key::new(command.namespace.0, command.key);

        match command.action {
            KeyValueAction::Get => {
                let value = shard
                    .get(&key)?
                    .map(|record| match record.value {
                        StoredValue::Inline(value) => Ok(value.0),
                        StoredValue::Object(_) => {
                            Err(Error::Unsupported("object values are not available through the local KV executor"))
                        }
                    })
                    .transpose()?;
                Ok(KeyValueResult::Get(value))
            }
            KeyValueAction::Exists => Ok(KeyValueResult::Exists(shard.get(&key)?.is_some())),
            KeyValueAction::GetDelete => {
                let value = match shard.get(&key)? {
                    None => None,
                    Some(record) => match record.value {
                        StoredValue::Inline(value) => Some(value.0),
                        StoredValue::Object(_) => {
                            return Err(Error::Unsupported("object values are not available through the local KV executor"));
                        }
                    },
                };
                if value.is_some() {
                    shard.delete(&key)?;
                }
                Ok(KeyValueResult::GetDelete(value))
            }
            KeyValueAction::GetSet(value) => {
                let previous = match shard.get(&key)? {
                    None => None,
                    Some(record) => match record.value {
                        StoredValue::Inline(value) => Some(value.0),
                        StoredValue::Object(_) => {
                            return Err(Error::Unsupported("object values are not available through the local KV executor"));
                        }
                    },
                };
                shard.put(key, StoredValue::Inline(InlineValue(value)))?;
                Ok(KeyValueResult::GetSet(previous))
            }
            KeyValueAction::Put(value) => {
                let revision = shard.put(key, StoredValue::Inline(InlineValue(value)))?;
                Ok(KeyValueResult::Put { revision })
            }
            KeyValueAction::PutIfAbsent(value) => {
                if shard.get(&key)?.is_some() {
                    Ok(KeyValueResult::PutIfAbsent { revision: None })
                }
                else {
                    let revision = shard.put(key, StoredValue::Inline(InlineValue(value)))?;
                    Ok(KeyValueResult::PutIfAbsent { revision: Some(revision) })
                }
            }
            KeyValueAction::PutIfPresent(value) => {
                if shard.get(&key)?.is_none() {
                    Ok(KeyValueResult::PutIfPresent { revision: None })
                }
                else {
                    let revision = shard.put(key, StoredValue::Inline(InlineValue(value)))?;
                    Ok(KeyValueResult::PutIfPresent { revision: Some(revision) })
                }
            }
            KeyValueAction::Delete => {
                let removed = shard.delete(&key)?;
                Ok(KeyValueResult::Delete { removed })
            }
            KeyValueAction::IncrementBy(delta) => {
                let current = match shard.get(&key)? {
                    None => 0,
                    Some(record) => match record.value {
                        StoredValue::Inline(value) => parse_integer(&value.0)?,
                        StoredValue::Object(_) => return Err(Error::Unsupported("object values cannot be incremented")),
                    },
                };
                let value = current.checked_add(delta).ok_or(Error::IntegerOverflow)?;
                shard.put(key, StoredValue::Inline(InlineValue(value.to_string().into_bytes())))?;
                Ok(KeyValueResult::Increment { value })
            }
        }
    }
}

fn parse_integer(bytes: &[u8]) -> yyds_types::Result<i64> {
    let text = std::str::from_utf8(bytes).map_err(|_| Error::InvalidIntegerValue)?;
    let value = text.parse::<i64>().map_err(|_| Error::InvalidIntegerValue)?;
    if value.to_string() != text {
        return Err(Error::InvalidIntegerValue);
    }
    Ok(value)
}

/// K-way merge of already-sorted slices, returning up to `limit` pairs keyed by key bytes.
fn merge_sorted(slices: &[Vec<(Vec<u8>, Vec<u8>)>], limit: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
    if slices.is_empty() {
        return Vec::new();
    }
    let mut cursors: Vec<usize> = vec![0; slices.len()];
    let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    loop {
        let mut best_idx: Option<usize> = None;
        for (i, slice) in slices.iter().enumerate() {
            if cursors[i] < slice.len() {
                match best_idx {
                    None => best_idx = Some(i),
                    Some(best) if slice[cursors[i]].0 < slices[best][cursors[best]].0 => best_idx = Some(i),
                    _ => {}
                }
            }
        }
        match best_idx {
            None => break,
            Some(i) => {
                out.push(slices[i][cursors[i]].clone());
                cursors[i] += 1;
                if limit > 0 && out.len() >= limit {
                    break;
                }
            }
        }
    }
    out
}
