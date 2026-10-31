#![deny(missing_debug_implementations)]
#![warn(missing_docs, rustdoc::missing_crate_level_docs)]
#![doc = include_str!("../readme.md")]

use std::{
    collections::HashMap,
    fmt::Debug,
    sync::{Arc, Mutex},
};

use yyds_kv::{FileShard, InlineValue, Key, KvStore, StoredValue};
use yyds_types::{
    Error, FenceToken, KeyValueAction, KeyValueCommand, KeyValueResult, Namespace, OwnerEntry, ShardId, ShardMap,
};

/// Execution surface consumed by protocol adapters.
pub trait KeyValueExecutor: Debug + Send + Sync {
    /// Routes and applies one namespace-scoped command.
    ///
    /// `fence` is an optional ownership token that a coordinator passes down to prove it
    /// still holds the lease for the target shard. When `Some(token)` is supplied, the
    /// executor must reject the request if its catalog-published owner record no longer
    /// matches the token's leader and term. `None` skips fencing (single-node gateways).
    fn execute(&self, fence: Option<&FenceToken>, command: KeyValueCommand) -> yyds_types::Result<KeyValueResult>;

    /// Reads an ordered batch atomically, preserving duplicate keys.
    /// Executors without an atomic read contract must reject the request.
    fn execute_read_batch(
        &self,
        fence: Option<&FenceToken>,
        _commands: &[KeyValueCommand],
    ) -> yyds_types::Result<Vec<KeyValueResult>> {
        let _ = fence;
        Err(Error::Unsupported("atomic batch reads are not implemented by this executor"))
    }

    /// Ordered prefix scan across all mounted shards for one namespace.
    ///
    /// Returns up to `limit` (key_bytes, inline_value_bytes) pairs whose key bytes start with
    /// `prefix`, compare greater than `after` (exclusive), less than `before` (exclusive), sorted
    /// by key bytes ascending.
    fn execute_scan(
        &self,
        fence: Option<&FenceToken>,
        _namespace: &Namespace,
        _prefix: &[u8],
        _after: Option<&[u8]>,
        _before: Option<&[u8]>,
        _limit: usize,
    ) -> yyds_types::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let _ = fence;
        Err(Error::Unsupported("scan is not implemented by this executor"))
    }

    /// Applies multiple commands as an atomic batch.
    ///
    /// Implementors must guarantee that the whole batch either applies completely or does not
    /// apply at all; a sequential fold over [`KeyValueExecutor::execute`] is not an acceptable
    /// implementation because it can leave a half-committed batch behind after a mid-batch
    /// failure. Executors without an atomic write contract must return `Error::Unsupported`
    /// (the default) instead of degrading to per-command execution.
    fn execute_batch(
        &self,
        fence: Option<&FenceToken>,
        commands: Vec<KeyValueCommand>,
    ) -> yyds_types::Result<Vec<KeyValueResult>> {
        let _ = (fence, commands);
        Err(Error::Unsupported("atomic batch writes are not implemented by this executor"))
    }

    /// Commits a buffered transaction with OCC validation under the caller's ownership context.
    ///
    /// Each element of `batch` carries the write command together with the original conditional
    /// (if any). Implementations must hold an exclusive executor-wide lock while re-checking
    /// every `BufferedCommit::condition` against the base store and then applying the writes
    /// atomically, so that a concurrent transaction cannot slip between validate and apply and a
    /// rejected condition never leaves a half-committed batch.
    ///
    /// `fence` is validated at the boundary where the writes are applied: when `Some(token)` is
    /// supplied the implementation must reject the commit if the token no longer matches the
    /// catalog-published owner of every routed shard. `None` keeps the single-node local mode
    /// (no owner context). Executors without an atomic OCC-validated commit contract must
    /// return `Error::Unsupported` (the default) instead of replaying commands one by one.
    ///
    /// The committed batch must route to exactly one shard: the single-node atomic commit scope
    /// is a single shard, and a batch whose writes spread across shards is rejected with a
    /// stable `Error::Unsupported` before anything is applied.
    fn commit_batch(&self, fence: Option<&FenceToken>, batch: Vec<BufferedCommit>) -> yyds_types::Result<()> {
        let _ = (fence, batch);
        Err(Error::Unsupported("atomic OCC-validated batch commits are not implemented by this executor"))
    }

    /// Opens a local transaction handle bound to this executor and the caller's ownership context.
    ///
    /// The handle lets callers issue commands through a transaction boundary so that a gateway
    /// can bind `BEGIN` / `COMMIT` / `ROLLBACK` protocol verbs to a real typed object. The
    /// `fence` (routing epoch + leader term + owner node) is captured at begin time and carried
    /// verbatim into [`KeyValueExecutor::commit_batch`] at commit time: a transaction opened by a
    /// coordinator that loses its lease later must fail at the commit boundary instead of
    /// committing under a default `None`. Single-node gateways pass `None` explicitly.
    ///
    /// The atomic scope of a committed transaction is **one shard** in this product phase:
    /// buffered writes that route to different shards are rejected with a stable
    /// `Error::Unsupported` at commit time and nothing is applied. Cross-shard (distributed)
    /// transactions are not disguised as ordinary local transactions. Executors without
    /// transaction support return `Error::Unsupported`.
    fn begin_transaction(self: Arc<Self>, _fence: Option<&FenceToken>) -> yyds_types::Result<Box<dyn TransactionHandle>> {
        Err(Error::Unsupported("this executor does not support local transactions"))
    }
}

/// One buffered write ready to be committed.
#[derive(Debug, Clone)]
pub struct BufferedCommit {
    /// The command to replay against the base store.
    pub command: KeyValueCommand,
    /// Optional original conditional that must still hold on the base store at commit time.
    pub condition: Option<CommitCondition>,
}

/// OCC pre-image condition checked at commit time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CommitCondition {
    /// The key must be absent from the base store (PutIfAbsent).
    PutIfAbsent,
    /// The key must be present in the base store (PutIfPresent).
    PutIfPresent,
    /// The base-store value must still equal this snapshot (`None` means the key must be absent).
    ///
    /// Used for read-modify-write SQL statements so that two transactions cannot both read the
    /// same pre-image and then overwrite each other in turn.
    ValueEquals(Option<Vec<u8>>),
}

/// A local transaction handle that exposes the same command surface as the executor but
/// wraps it in a begin/commit/rollback contract.
///
/// The reference implementation (`LocalTransactionHandle`) buffers every write until commit,
/// serves reads from that buffer before falling back to the executor, and re-checks OCC
/// pre-image conditions under the executor-wide lock at commit time.
pub trait TransactionHandle: Debug {
    /// Executes one command within the transaction.
    fn execute(&self, command: KeyValueCommand) -> yyds_types::Result<KeyValueResult>;

    /// Reads an ordered batch within the transaction, preserving duplicate keys.
    fn execute_read_batch(&self, _commands: &[KeyValueCommand]) -> yyds_types::Result<Vec<KeyValueResult>> {
        Err(Error::Unsupported("transaction batch reads are not implemented by this handle"))
    }

    /// Ordered prefix scan within the transaction, merging buffered writes over the executor.
    ///
    /// Handles without a transaction snapshot must reject the request instead of silently
    /// reading the base store, otherwise read-your-writes is violated.
    fn execute_scan(
        &self,
        _namespace: &Namespace,
        _prefix: &[u8],
        _after: Option<&[u8]>,
        _before: Option<&[u8]>,
        _limit: usize,
    ) -> yyds_types::Result<Vec<(Vec<u8>, Vec<u8>)>> {
        Err(Error::Unsupported("transaction scans are not implemented by this handle"))
    }

    /// Buffers a `Put` / `Delete` whose base-store pre-image must equal `expected` at commit time
    /// (`None` means the key must be absent).
    ///
    /// Callers use this for read-modify-write statements so that a concurrent commit between
    /// their read and their commit is rejected by OCC instead of being overwritten. Handles
    /// without OCC support must reject the request rather than degrade to an unconditional write.
    fn write_if_unchanged(&self, _command: KeyValueCommand, _expected: Option<Vec<u8>>) -> yyds_types::Result<KeyValueResult> {
        Err(Error::Unsupported("conditional transaction writes are not implemented by this handle"))
    }

    /// Finalises the transaction and persists any pending state.
    fn commit(self: Box<Self>) -> yyds_types::Result<()>;

    /// Aborts the transaction and discards any pending state.
    fn rollback(self: Box<Self>);

    /// Creates a named savepoint that can be restored to later.
    ///
    /// Handles without savepoint support must return `Error::Unsupported` rather than silently
    /// accepting the savepoint name, otherwise clients will believe rollback-to works when it
    /// does not.
    fn savepoint_create(&self, _name: &str) -> yyds_types::Result<()> {
        Err(Error::Unsupported("savepoints are not implemented by this handle"))
    }

    /// Rolls the transaction back to a previously created savepoint without ending it.
    ///
    /// Implementations must reject unknown savepoint names with `Error::Unsupported` or a
    /// dedicated error so callers can distinguish missing names from other failures.
    fn savepoint_rollback_to(&self, _name: &str) -> yyds_types::Result<()> {
        Err(Error::Unsupported("savepoints are not implemented by this handle"))
    }

    /// Removes a savepoint without rolling anything back. Subsequent rollback-to that name
    /// must fail.
    fn savepoint_release(&self, _name: &str) -> yyds_types::Result<()> {
        Err(Error::Unsupported("savepoints are not implemented by this handle"))
    }
}

/// Executor-wide mutex that serialises every [`LocalExecutor::execute`] and
/// [`LocalExecutor::execute_batch`] call so that batched writes appear atomic to the rest of
/// the process.
static EXECUTOR_GLOBAL_LOCK: Mutex<()> = Mutex::new(());

/// Routes commands to mounted local shards under one immutable routing map.
#[derive(Debug)]
pub struct LocalExecutor<S = FileShard> {
    routing: ShardMap,
    shards: HashMap<ShardId, Mutex<S>>,
    /// Snapshot of catalog-published owner records used for fencing validation.
    owners: Vec<OwnerEntry>,
}

impl<S> LocalExecutor<S>
where
    S: KvStore + Send + Debug,
{
    /// Creates a local executor and requires an exact mounted-shard match.
    ///
    /// `owners` is a snapshot of catalog-published shard owners used for fencing
    /// validation. When the catalog has not yet published any owner, pass an
    /// empty `Vec` — fencing checks will simply fail closed in that case (no
    /// request with `Some(fence)` can succeed until owners are published).
    pub fn new(routing: ShardMap, shards: HashMap<ShardId, S>, owners: Vec<OwnerEntry>) -> yyds_types::Result<Self> {
        if shards.len() != routing.shards().len() || routing.shards().iter().any(|shard| !shards.contains_key(shard)) {
            return Err(Error::Unsupported("mounted shards must exactly match the published routing map"));
        }
        Ok(Self { routing, shards: shards.into_iter().map(|(id, shard)| (id, Mutex::new(shard))).collect(), owners })
    }

    /// Returns the immutable routing map used by this executor.
    pub fn routing(&self) -> &ShardMap {
        &self.routing
    }

    /// Returns the current owner snapshot used for fencing validation.
    pub fn owners(&self) -> &[OwnerEntry] {
        &self.owners
    }

    /// Updates the owner snapshot used for fencing validation (e.g. after a catalog reload).
    pub fn set_owners(&mut self, owners: Vec<OwnerEntry>) {
        self.owners = owners;
    }

    /// Checks the optional `fence` token against the catalog-published owner of `shard_id`.
    ///
    /// - `None` fence → skip (single-node gateway path).
    /// - `Some(fence)` → find owner entry for `shard_id`; if missing, or leader/term mismatch,
    ///   reject with `Error::FencingRejected`.
    fn check_fence(&self, fence: Option<&FenceToken>, shard_id: &ShardId) -> yyds_types::Result<()> {
        let Some(fence) = fence
        else {
            return Ok(());
        };
        let owner = self.owners.iter().find(|o| o.shard_id == *shard_id).ok_or_else(|| Error::FencingRejected {
            fence: fence.clone(),
            current: OwnerEntry {
                shard_id: shard_id.clone(),
                node_id: yyds_types::ReplicaNodeId("<unpublished>".to_owned()),
                leader_term: yyds_types::LeaderTerm(0),
            },
        })?;
        if fence.leader != owner.node_id || fence.term != owner.leader_term {
            return Err(Error::FencingRejected { fence: fence.clone(), current: owner.clone() });
        }
        Ok(())
    }

    /// Runs `f` while holding the executor-wide write lock.
    fn with_global_lock<F, R>(f: F) -> yyds_types::Result<R>
    where
        F: FnOnce() -> yyds_types::Result<R>,
    {
        let _guard = EXECUTOR_GLOBAL_LOCK.lock().map_err(|_| Error::Corrupt("executor global lock poisoned"))?;
        f()
    }

    /// Executes one command against the shards **without** acquiring the global lock. The
    /// caller is responsible for holding [`EXECUTOR_GLOBAL_LOCK`] so that a batch remains
    /// atomic while composed out of individual writes.
    fn execute_single_locked(&self, command: KeyValueCommand) -> yyds_types::Result<KeyValueResult> {
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

/// Tracks the in-flight state of a single `(namespace, key)` pair inside a local transaction.
///
/// Only the final state per key is stored so that a second `Put` on the same key simply
/// overwrites the first without needing to replay a multi-value history. When the final
/// state was reached through a conditional write (PutIfAbsent / PutIfPresent / a captured
/// read-modify-write pre-image) that original condition is preserved so that
/// [`LocalTransactionHandle::commit`] can re-check it against the base store as part of OCC
/// validate-then-commit.
#[derive(Debug, Clone, PartialEq, Eq)]
enum BufferedState {
    /// The key has been set to this value inside the transaction. The optional conditional
    /// records whether the first write on this key was a PutIfAbsent or PutIfPresent so that
    /// commit-time OCC re-checks the correct pre-image.
    Value(Vec<u8>, Option<CommitCondition>),
    /// The key has been deleted inside the transaction. The optional conditional records the
    /// pre-image captured by a read-modify-write delete so that commit-time OCC can reject a
    /// concurrent writer.
    Deleted(Option<CommitCondition>),
}

/// Buffered local transaction handle.
///
/// Writes issued through this handle are held in a per-handle buffer until either
/// [`TransactionHandle::commit`] or [`TransactionHandle::rollback`] is called. Reads are
/// served by consulting the buffer first (snapshot reads), then delegating to the underlying
/// executor so that transactions observe their own writes.
///
/// Named savepoints live alongside the buffer as a stack of snapshots so that
/// [`TransactionHandle::savepoint_rollback_to`] can restore an earlier buffer state
/// without ending the transaction.
#[derive(Debug)]
pub struct LocalTransactionHandle {
    executor: Arc<dyn KeyValueExecutor>,
    /// Ownership context captured at `begin_transaction` time and replayed into the commit
    /// boundary. `None` is the explicit single-node local mode, never a silent default.
    fence: Option<FenceToken>,
    buffer: Mutex<HashMap<(String, Vec<u8>), BufferedState>>,
    savepoints: Mutex<Vec<(String, HashMap<(String, Vec<u8>), BufferedState>)>>,
}

impl LocalTransactionHandle {
    /// Creates a new handle bound to an executor via a shared Arc and the caller's fence context.
    ///
    /// Gateways should go through [`KeyValueExecutor::begin_transaction`] so the executor can
    /// enforce its transaction capability contract; this constructor backs that path and unit
    /// tests of the buffering semantics.
    pub fn new(executor: Arc<dyn KeyValueExecutor>, fence: Option<FenceToken>) -> Self {
        Self { executor, fence, buffer: Mutex::new(HashMap::new()), savepoints: Mutex::new(Vec::new()) }
    }

    /// Resolves the current observable value for `(namespace, key)` by merging the buffer
    /// over the underlying executor state.
    fn resolve(&self, namespace: &str, key: &[u8]) -> yyds_types::Result<Option<Vec<u8>>> {
        let buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
        if let Some(state) = buffer.get(&(namespace.to_owned(), key.to_vec())) {
            return Ok(match state {
                BufferedState::Value(bytes, _) => Some(bytes.clone()),
                BufferedState::Deleted(_) => None,
            });
        }
        Ok(
            match self.executor.execute(
                None,
                KeyValueCommand { namespace: Namespace(namespace.to_owned()), key: key.to_vec(), action: KeyValueAction::Get },
            )? {
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
                // 无条件 Put 会覆盖 buffer 里任何已有条件（包括之前的 PutIfAbsent/PutIfPresent），
                // 因为无条件 Put 意味着调用方已经决定不管 base 状态如何都要写。
                buffer.insert((namespace.0, key), BufferedState::Value(value, None));
                Ok(KeyValueResult::Put { revision: 1 })
            }
            KeyValueAction::PutIfAbsent(value) => {
                let exists = self.resolve(&namespace.0, &key)?.is_some();
                if exists {
                    return Ok(KeyValueResult::PutIfAbsent { revision: None });
                }
                let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
                buffer.insert((namespace.0, key), BufferedState::Value(value, Some(CommitCondition::PutIfAbsent)));
                Ok(KeyValueResult::PutIfAbsent { revision: Some(1) })
            }
            KeyValueAction::PutIfPresent(value) => {
                let exists = self.resolve(&namespace.0, &key)?.is_some();
                if !exists {
                    return Ok(KeyValueResult::PutIfPresent { revision: None });
                }
                let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
                buffer.insert((namespace.0, key), BufferedState::Value(value, Some(CommitCondition::PutIfPresent)));
                Ok(KeyValueResult::PutIfPresent { revision: Some(1) })
            }
            KeyValueAction::Delete => {
                let existed = self.resolve(&namespace.0, &key)?.is_some();
                let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
                // Delete 覆盖任何已有 Value 状态，Delete 本身不携带条件。
                buffer.insert((namespace.0, key), BufferedState::Deleted(None));
                Ok(KeyValueResult::Delete { removed: existed })
            }
            KeyValueAction::GetSet(new_value) => {
                let previous = self.resolve(&namespace.0, &key)?;
                let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
                buffer.insert((namespace.0, key), BufferedState::Value(new_value, None));
                Ok(KeyValueResult::GetSet(previous))
            }
            KeyValueAction::GetDelete => {
                let previous = self.resolve(&namespace.0, &key)?;
                let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
                buffer.insert((namespace.0, key), BufferedState::Deleted(None));
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
                buffer.insert((namespace.0, key), BufferedState::Value(value.to_string().into_bytes(), None));
                Ok(KeyValueResult::Increment { value })
            }
        }
    }

    fn execute_read_batch(&self, commands: &[KeyValueCommand]) -> yyds_types::Result<Vec<KeyValueResult>> {
        if commands.iter().any(|command| command.action != KeyValueAction::Get) {
            return Err(Error::Unsupported("transaction read batches accept only Get actions"));
        }
        commands
            .iter()
            .map(|command| {
                let value = self.resolve(&command.namespace.0, &command.key)?;
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
        // The base scan is requested without a limit so that buffered inserts can still be
        // merged into the ordered result before the caller's limit is applied.
        let mut merged: std::collections::BTreeMap<Vec<u8>, Option<Vec<u8>>> = std::collections::BTreeMap::new();
        for (key, value) in self.executor.execute_scan(None, namespace, prefix, after, before, 0)? {
            merged.insert(key, Some(value));
        }
        let buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
        for ((buffered_namespace, key), state) in buffer.iter() {
            if buffered_namespace != &namespace.0 || !scan_key_matches(key, prefix, after, before) {
                continue;
            }
            match state {
                BufferedState::Value(value, _) => {
                    merged.insert(key.clone(), Some(value.clone()));
                }
                BufferedState::Deleted(_) => {
                    merged.insert(key.clone(), None);
                }
            }
        }
        drop(buffer);
        let mut rows: Vec<(Vec<u8>, Vec<u8>)> =
            merged.into_iter().filter_map(|(key, value)| value.map(|value| (key, value))).collect();
        if limit > 0 {
            rows.truncate(limit);
        }
        Ok(rows)
    }

    fn write_if_unchanged(&self, command: KeyValueCommand, expected: Option<Vec<u8>>) -> yyds_types::Result<KeyValueResult> {
        let KeyValueCommand { namespace, key, action } = command;
        // `removed` must be resolved before taking the buffer lock because `resolve` uses it too.
        let removed = match action {
            KeyValueAction::Delete => self.resolve(&namespace.0, &key)?.is_some(),
            KeyValueAction::Put(_) => false,
            _ => return Err(Error::Unsupported("conditional transaction writes accept only Put and Delete")),
        };
        let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
        // The earliest write on this key already captured the authoritative base pre-image, so a
        // later write in the same transaction keeps that condition instead of replacing it with
        // the value the transaction has already observed.
        let condition = match buffer.get(&(namespace.0.clone(), key.clone())) {
            Some(BufferedState::Value(_, condition) | BufferedState::Deleted(condition)) => condition.clone(),
            None => Some(CommitCondition::ValueEquals(expected)),
        };
        match action {
            KeyValueAction::Put(value) => {
                buffer.insert((namespace.0, key), BufferedState::Value(value, condition));
                Ok(KeyValueResult::Put { revision: 1 })
            }
            KeyValueAction::Delete => {
                buffer.insert((namespace.0, key), BufferedState::Deleted(condition));
                Ok(KeyValueResult::Delete { removed })
            }
            _ => unreachable!("conditional transaction writes accept only Put and Delete"),
        }
    }

    fn commit(self: Box<Self>) -> yyds_types::Result<()> {
        let buffer = self.buffer.into_inner().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;

        // 组装 commit_batch 参数：最终 replay command + 可选 OCC 条件。
        // validate + batch apply 都在 executor.commit_batch 内部的同一提交锁区间里完成。
        // fence 是 begin_transaction 时捕获的事务上下文：owner 在事务期间切换时，
        // commit 边界必须用这个 token 拒绝半提交，而不是回落到默认 None 的无主提交。
        let mut batch = Vec::with_capacity(buffer.len());
        for ((namespace, key), state) in buffer {
            let (action, condition) = match state {
                BufferedState::Value(value, cond) => (KeyValueAction::Put(value), cond),
                BufferedState::Deleted(cond) => (KeyValueAction::Delete, cond),
            };
            batch.push(BufferedCommit { command: KeyValueCommand { namespace: Namespace(namespace), key, action }, condition });
        }

        self.executor.commit_batch(self.fence.as_ref(), batch)
    }

    fn rollback(self: Box<Self>) {
        // Dropping the handle discards the buffered HashMap; nothing touches the executor.
    }

    fn savepoint_create(&self, name: &str) -> yyds_types::Result<()> {
        let mut savepoints = self.savepoints.lock().map_err(|_| Error::Corrupt("transaction savepoint stack poisoned"))?;
        if savepoints.iter().any(|(existing, _)| existing == name) {
            return Err(Error::Unsupported(leak_str(&format!("savepoint `{name}` already exists"))));
        }
        let buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
        savepoints.push((name.to_owned(), buffer.clone()));
        Ok(())
    }

    fn savepoint_rollback_to(&self, name: &str) -> yyds_types::Result<()> {
        let mut savepoints = self.savepoints.lock().map_err(|_| Error::Corrupt("transaction savepoint stack poisoned"))?;
        let index = savepoints
            .iter()
            .rposition(|(existing, _)| existing == name)
            .ok_or_else(|| Error::Unsupported(leak_str(&format!("savepoint `{name}` does not exist"))))?;
        let (_, snapshot) = savepoints.remove(index);
        // Drop every savepoint strictly above the named one so later rollback-to cannot jump
        // past its own restore barrier.
        savepoints.truncate(index);
        drop(savepoints);
        let mut buffer = self.buffer.lock().map_err(|_| Error::Corrupt("transaction buffer poisoned"))?;
        *buffer = snapshot;
        Ok(())
    }

    fn savepoint_release(&self, name: &str) -> yyds_types::Result<()> {
        let mut savepoints = self.savepoints.lock().map_err(|_| Error::Corrupt("transaction savepoint stack poisoned"))?;
        let index = savepoints
            .iter()
            .rposition(|(existing, _)| existing == name)
            .ok_or_else(|| Error::Unsupported(leak_str(&format!("savepoint `{name}` does not exist"))))?;
        savepoints.remove(index);
        Ok(())
    }
}

impl<S> KeyValueExecutor for LocalExecutor<S>
where
    S: KvStore + Send + Debug + 'static,
{
    fn execute_read_batch(
        &self,
        fence: Option<&FenceToken>,
        commands: &[KeyValueCommand],
    ) -> yyds_types::Result<Vec<KeyValueResult>> {
        if commands.iter().any(|command| command.action != KeyValueAction::Get) {
            return Err(Error::Unsupported("read batches accept only read actions"));
        }
        let decisions =
            commands.iter().map(|command| self.routing.route(&command.namespace.0, &command.key)).collect::<Vec<_>>();
        // Fence check: every shard touched by the batch must pass.
        for decision in &decisions {
            self.check_fence(fence, &decision.shard)?;
        }
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
        fence: Option<&FenceToken>,
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
            // The scan reads every mounted shard, so fencing must guard the read path too:
            // a stale coordinator must not observe shard contents it no longer owns.
            self.check_fence(fence, shard_id)?;
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

    fn execute(&self, fence: Option<&FenceToken>, command: KeyValueCommand) -> yyds_types::Result<KeyValueResult> {
        let decision = self.routing.route(&command.namespace.0, &command.key);
        self.check_fence(fence, &decision.shard)?;
        Self::with_global_lock(|| self.execute_single_locked(command))
    }

    fn execute_batch(
        &self,
        fence: Option<&FenceToken>,
        commands: Vec<KeyValueCommand>,
    ) -> yyds_types::Result<Vec<KeyValueResult>> {
        // Fence check every command against its routed shard up-front.
        for command in &commands {
            let decision = self.routing.route(&command.namespace.0, &command.key);
            self.check_fence(fence, &decision.shard)?;
        }
        Self::with_global_lock(|| commands.into_iter().map(|command| self.execute_single_locked(command)).collect())
    }

    fn commit_batch(&self, fence: Option<&FenceToken>, batch: Vec<BufferedCommit>) -> yyds_types::Result<()> {
        // The entire fence validation, OCC pre-check and replay must sit inside one global-lock
        // holding interval; otherwise a concurrent `PutIfAbsent` from another transaction could
        // sneak between our re-check and our own replay and we would silently overwrite it.
        Self::with_global_lock(|| {
            // Single-shard atomic scope: the whole batch must route to one shard. A batch that
            // spreads across shards is a distributed transaction, which this phase does not
            // support; reject with a stable error before anything is applied.
            let mut batch_shard: Option<ShardId> = None;
            for entry in &batch {
                let decision = self.routing.route(&entry.command.namespace.0, &entry.command.key);
                match &batch_shard {
                    None => batch_shard = Some(decision.shard),
                    Some(scope) if *scope != decision.shard => {
                        return Err(Error::Unsupported(
                            "cross-shard batch commits are not supported; the atomic commit scope is a single shard",
                        ));
                    }
                    Some(_) => {}
                }
            }
            // Ownership must be proven at the boundary where the writes are applied: the routed
            // shard of the batch has to still belong to the fence's leader and term.
            if let Some(shard) = &batch_shard {
                self.check_fence(fence, shard)?;
            }
            for entry in &batch {
                if let Some(condition) = &entry.condition {
                    let base = self.execute_single_locked(KeyValueCommand {
                        namespace: entry.command.namespace.clone(),
                        key: entry.command.key.clone(),
                        action: KeyValueAction::Get,
                    })?;
                    let base_value = match base {
                        KeyValueResult::Get(value) => value,
                        _ => unreachable!("Get always yields a Get result"),
                    };
                    let exists = base_value.is_some();
                    match condition {
                        CommitCondition::PutIfAbsent if exists => {
                            return Err(Error::CasConflict {
                                key: format!("{}:{}", entry.command.namespace.0, String::from_utf8_lossy(&entry.command.key)),
                            });
                        }
                        CommitCondition::PutIfPresent if !exists => {
                            return Err(Error::CasConflict {
                                key: format!("{}:{}", entry.command.namespace.0, String::from_utf8_lossy(&entry.command.key)),
                            });
                        }
                        CommitCondition::ValueEquals(expected) if base_value != *expected => {
                            return Err(Error::CasConflict {
                                key: format!("{}:{}", entry.command.namespace.0, String::from_utf8_lossy(&entry.command.key)),
                            });
                        }
                        _ => {}
                    }
                }
            }
            for entry in batch {
                self.execute_single_locked(entry.command)?;
            }
            Ok(())
        })
    }

    fn begin_transaction(self: Arc<Self>, fence: Option<&FenceToken>) -> yyds_types::Result<Box<dyn TransactionHandle>> {
        // The local executor provides an atomic OCC-validated commit path, so it can open a
        // transaction. The caller's fence (routing epoch + leader term + owner node) is captured
        // into the handle and replayed verbatim at commit time; a single-node gateway passes
        // `None` explicitly rather than letting the handle default to an unowned commit.
        Ok(Box::new(LocalTransactionHandle::new(self, fence.cloned())))
    }
}

/// Applies the same prefix / exclusive-bound filters a shard scan uses to one buffered key.
fn scan_key_matches(key: &[u8], prefix: &[u8], after: Option<&[u8]>, before: Option<&[u8]>) -> bool {
    key.starts_with(prefix) && after.is_none_or(|after| key > after) && before.is_none_or(|before| key < before)
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

/// Boxes `text` on the heap and leaks the allocation so it can be passed to
/// APIs that require `&'static str`. Use only for error messages whose lifetime
/// is already bound to process exit; the amount per call is tiny and bounded
/// by the number of distinct savepoint name collisions.
fn leak_str(text: &str) -> &'static str {
    Box::leak(text.to_string().into_boxed_str())
}
