#![deny(missing_debug_implementations)]
#![warn(missing_docs, rustdoc::missing_crate_level_docs)]
#![doc = include_str!("../readme.md")]

use std::{collections::HashMap, fmt::Debug, sync::Mutex};

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
