use std::{
    collections::{BTreeMap, HashSet},
    fs::{self, OpenOptions},
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
};

use yyds_types::{Error, Namespace, Result};

use crate::{Key, Record, StoredValue};

const MAGIC: &[u8] = b"YYKV\x01";

/// One mutation inside a shard-level atomic commit.
#[derive(Debug, Clone)]
pub enum ShardChange {
    /// Insert or replace a key with `value` at the next shard revision.
    Put {
        /// The key to insert or replace.
        key: Key,
        /// The post-commit value.
        value: StoredValue,
    },
    /// Remove a key when present.
    Delete {
        /// The key to remove.
        key: Key,
    },
}

/// Shard-local KV surface. Implementations back one `.yykv` shard.
pub trait KvStore {
    /// Returns the latest record for `key`, if present.
    fn get(&self, key: &Key) -> Result<Option<Record>>;

    /// Inserts or replaces `value` and returns the new revision.
    fn put(&mut self, key: Key, value: StoredValue) -> Result<u64>;

    /// Deletes `key` when present. Returns whether a record was removed.
    fn delete(&mut self, key: &Key) -> Result<bool>;

    /// Applies the whole change set as one atomic commit step.
    ///
    /// The contract is a single commit boundary: the changes are staged first (the complete
    /// post-commit state is produced and written durably), then applied atomically, and only
    /// then published to readers. Either every change in `changes` is committed and durable
    /// when this returns `Ok`, or nothing is when it returns `Err`. A sequential fold over
    /// [`Self::put`] / [`Self::delete`] is not an acceptable implementation because a crash
    /// between two persists leaves a half-committed batch on disk.
    ///
    /// Stores without a durable atomic commit boundary must return `Error::Unsupported`
    /// (the default) instead of degrading to per-change execution.
    fn commit_changes(&mut self, changes: Vec<ShardChange>) -> Result<()> {
        let _ = changes;
        Err(Error::Unsupported("store does not provide a durable atomic commit boundary"))
    }

    /// Whether [`Self::commit_changes`] survives process exit.
    ///
    /// Executors may only open transactions against stores that answer `true`; a store whose
    /// commit boundary is process-volatile (e.g. an in-memory shard) must keep the default
    /// `false` so transactions are rejected instead of being disguised by an executor-side
    /// mutex.
    fn durable_atomic_commit(&self) -> bool {
        false
    }

    /// Ordered prefix scan within one namespace.
    ///
    /// Returns up to `limit` records whose key bytes start with `prefix` and compare greater
    /// exclusive lower cursor; `before` is the exclusive upper cursor. `limit` of `0`
    /// means no cap. Results are sorted by key bytes ascending.
    fn scan(
        &self,
        namespace: &Namespace,
        prefix: &[u8],
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Record>>;
}

/// In-memory shard used by unit tests and early integrations.
#[derive(Debug, Default)]
pub struct MemoryShard {
    records: BTreeMap<Key, Record>,
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
        let record = Record { key: key.clone(), value, revision };
        self.records.insert(key, record);
        Ok(revision)
    }

    fn delete(&mut self, key: &Key) -> Result<bool> {
        Ok(self.records.remove(key).is_some())
    }

    fn scan(
        &self,
        namespace: &Namespace,
        prefix: &[u8],
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Record>> {
        Ok(collect_scan(&self.records, namespace, prefix, after, before, limit))
    }
}

/// File-backed shard storing one namespace.using the native `.yykv` snapshot format.
#[derive(Debug)]
pub struct FileShard {
    path: PathBuf,
    records: BTreeMap<Key, Record>,
    next_revision: u64,
}

impl FileShard {
    /// Opens an existing shard or creates an empty shard at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            let bytes = fs::read(&path)?;
            decode(&path, &bytes)
        }
        else {
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    fs::create_dir_all(parent)?;
                }
            }
            let shard = Self { path, records: BTreeMap::new(), next_revision: 0 };
            shard.persist()?;
            Ok(shard)
        }
    }

    /// Returns the backing `.yykv` path.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Persists the current shard snapshot.
    pub fn flush(&self) -> Result<()> {
        self.persist()
    }

    fn persist(&self) -> Result<()> {
        let bytes = encode_snapshot(&self.records, self.next_revision)?;
        write_snapshot_durable(&self.path, &bytes)
    }
}

impl KvStore for FileShard {
    fn get(&self, key: &Key) -> Result<Option<Record>> {
        Ok(self.records.get(key).cloned())
    }

    fn put(&mut self, key: Key, value: StoredValue) -> Result<u64> {
        let revision = self.next_revision.checked_add(1).ok_or(Error::Corrupt("shard revision exhausted"))?;
        let record = Record { key: key.clone(), value, revision };
        let previous = self.records.insert(key.clone(), record);
        self.next_revision = revision;
        if let Err(error) = self.persist() {
            self.next_revision = revision - 1;
            match previous {
                Some(record) => {
                    self.records.insert(key, record);
                }
                None => {
                    self.records.remove(&key);
                }
            }
            return Err(error);
        }
        Ok(revision)
    }

    fn delete(&mut self, key: &Key) -> Result<bool> {
        let Some(record) = self.records.remove(key)
        else {
            return Ok(false);
        };
        if let Err(error) = self.persist() {
            self.records.insert(key.clone(), record);
            return Err(error);
        }
        Ok(true)
    }

    fn commit_changes(&mut self, changes: Vec<ShardChange>) -> Result<()> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut staged_records = self.records.clone();
        let mut staged_revision = self.next_revision;
        for change in &changes {
            match change {
                ShardChange::Put { key, value } => {
                    staged_revision = staged_revision.checked_add(1).ok_or(Error::Corrupt("shard revision exhausted"))?;
                    staged_records.insert(key.clone(), Record { key: key.clone(), value: value.clone(), revision: staged_revision });
                }
                ShardChange::Delete { key } => {
                    staged_records.remove(key);
                }
            }
        }
        let bytes = encode_snapshot(&staged_records, staged_revision)?;
        // Durable commit point: the full post-commit snapshot is fsynced to a temp file and
        // atomically replaces the shard. Memory is only published after this returns `Ok`,
        // so a crash or storage failure here leaves the previous snapshot both visible and
        // durable — never a half-applied batch.
        write_snapshot_durable(&self.path, &bytes)?;
        self.records = staged_records;
        self.next_revision = staged_revision;
        Ok(())
    }

    fn durable_atomic_commit(&self) -> bool {
        true
    }

    fn scan(
        &self,
        namespace: &Namespace,
        prefix: &[u8],
        after: Option<&[u8]>,
        before: Option<&[u8]>,
        limit: usize,
    ) -> Result<Vec<Record>> {
        Ok(collect_scan(&self.records, namespace, prefix, after, before, limit))
    }
}

/// Shared scan logic: filter by namespace, prefix-match, exclusive cursors (after/before), limit.
fn collect_scan(
    records: &BTreeMap<Key, Record>,
    namespace: &Namespace,
    prefix: &[u8],
    after: Option<&[u8]>,
    before: Option<&[u8]>,
    limit: usize,
) -> Vec<Record> {
    let mut out = Vec::new();
    for (key, record) in records {
        if key.namespace != *namespace {
            continue;
        }
        if !key.bytes.starts_with(prefix) {
            continue;
        }
        if let Some(after_bytes) = after {
            if key.bytes.as_slice() <= after_bytes {
                continue;
            }
        }
        if let Some(before_bytes) = before {
            if key.bytes.as_slice() >= before_bytes {
                continue;
            }
        }
        out.push(record.clone());
        if limit > 0 && out.len() >= limit {
            break;
        }
    }
    out
}

fn encode_snapshot(records: &BTreeMap<Key, Record>, next_revision: u64) -> Result<Vec<u8>> {
    let mut bytes = Vec::new();
    bytes.write_all(MAGIC)?;
    bytes.write_all(&next_revision.to_le_bytes())?;
    write_count(&mut bytes, records.len())?;
    // BTreeMap iteration already yields namespace-then-bytes order — no manual sort needed.
    for record in records.values() {
        write_bytes(&mut bytes, record.key.namespace.0.as_bytes())?;
        write_bytes(&mut bytes, &record.key.bytes)?;
        bytes.write_all(&record.revision.to_le_bytes())?;
        match &record.value {
            StoredValue::Inline(value) => {
                bytes.write_all(&[0])?;
                write_bytes(&mut bytes, &value.0)?;
            }
            StoredValue::Object(value) => {
                bytes.write_all(&[1])?;
                write_bytes(&mut bytes, value.hash_hex.as_bytes())?;
            }
        }
    }
    Ok(bytes)
}

/// Writes `bytes` to a temp file beside `path`, fsyncs it, then atomically replaces `path`.
/// This is the durable commit point shared by single writes and batch commits.
fn write_snapshot_durable(path: &Path, bytes: &[u8]) -> Result<()> {
    let temp = path.with_file_name(format!(
        ".{}.yykv-tmp-{}",
        path.file_name().and_then(|name| name.to_str()).unwrap_or("shard"),
        std::process::id()
    ));
    let result = (|| -> Result<()> {
        let mut file = OpenOptions::new().create(true).truncate(true).write(true).open(&temp)?;
        file.write_all(bytes)?;
        file.sync_all()?;
        drop(file);
        replace_file(&temp, path)
    })();
    if result.is_err() {
        let _ = fs::remove_file(&temp);
    }
    result
}

fn decode(path: &Path, bytes: &[u8]) -> Result<FileShard> {
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0_u8; MAGIC.len()];
    read_exact(&mut cursor, &mut magic)?;
    if magic != MAGIC {
        return Err(Error::Corrupt("unknown yykv header"));
    }
    let next_revision = read_u64(&mut cursor)?;
    let count = read_u32(&mut cursor)? as usize;
    let mut records = BTreeMap::new();
    let mut revisions = HashSet::with_capacity(count);
    for _ in 0..count {
        let namespace = String::from_utf8(read_bytes(&mut cursor)?).map_err(|_| Error::Corrupt("shard namespace utf8"))?;
        let key = read_bytes(&mut cursor)?;
        let revision = read_u64(&mut cursor)?;
        if revision == 0 || revision > next_revision || !revisions.insert(revision) {
            return Err(Error::Corrupt("invalid shard revision"));
        }
        let value = match read_u8(&mut cursor)? {
            0 => StoredValue::Inline(crate::InlineValue(read_bytes(&mut cursor)?)),
            1 => StoredValue::Object(crate::ObjectRef {
                hash_hex: String::from_utf8(read_bytes(&mut cursor)?).map_err(|_| Error::Corrupt("shard object hash utf8"))?,
            }),
            _ => return Err(Error::Corrupt("invalid shard value kind")),
        };
        let record = Record { key: Key::new(namespace, key), value, revision };
        if records.insert(record.key.clone(), record).is_some() {
            return Err(Error::Corrupt("duplicate shard key"));
        }
    }
    if cursor.position() != bytes.len() as u64 {
        return Err(Error::Corrupt("trailing shard bytes"));
    }
    Ok(FileShard { path: path.to_path_buf(), records, next_revision })
}

fn write_count(bytes: &mut Vec<u8>, count: usize) -> Result<()> {
    let count = u32::try_from(count).map_err(|_| Error::Unsupported("too many shard records"))?;
    bytes.write_all(&count.to_le_bytes())?;
    Ok(())
}

fn write_bytes(bytes: &mut Vec<u8>, value: &[u8]) -> Result<()> {
    let length = u32::try_from(value.len()).map_err(|_| Error::Unsupported("shard value too large"))?;
    bytes.write_all(&length.to_le_bytes())?;
    bytes.write_all(value)?;
    Ok(())
}

fn read_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8> {
    let mut value = [0_u8; 1];
    read_exact(cursor, &mut value)?;
    Ok(value[0])
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut value = [0_u8; 4];
    read_exact(cursor, &mut value)?;
    Ok(u32::from_le_bytes(value))
}

fn read_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut value = [0_u8; 8];
    read_exact(cursor, &mut value)?;
    Ok(u64::from_le_bytes(value))
}

fn read_bytes(cursor: &mut Cursor<&[u8]>) -> Result<Vec<u8>> {
    let length = read_u32(cursor)? as usize;
    let position = cursor.position() as usize;
    let end = position.checked_add(length).ok_or(Error::Corrupt("shard length overflow"))?;
    if end > cursor.get_ref().len() {
        return Err(Error::Corrupt("unexpected shard eof"));
    }
    let mut value = vec![0_u8; length];
    read_exact(cursor, &mut value)?;
    Ok(value)
}

fn read_exact(cursor: &mut Cursor<&[u8]>, bytes: &mut [u8]) -> Result<()> {
    cursor.read_exact(bytes).map_err(|error| {
        if error.kind() == std::io::ErrorKind::UnexpectedEof {
            Error::Corrupt("unexpected shard eof")
        }
        else {
            Error::Io(error)
        }
    })
}

#[cfg(not(windows))]
fn replace_file(temp: &Path, destination: &Path) -> Result<()> {
    fs::rename(temp, destination).map_err(Error::Io)
}

#[cfg(windows)]
fn replace_file(temp: &Path, destination: &Path) -> Result<()> {
    use std::os::windows::ffi::OsStrExt;
    use windows_sys::Win32::Storage::FileSystem::{
        MoveFileExW, ReplaceFileW, MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH,
    };

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str().encode_wide().chain(Some(0)).collect()
    }

    let temp_wide = wide(temp);
    let destination_wide = wide(destination);
    let result = unsafe {
        if destination.exists() {
            ReplaceFileW(
                destination_wide.as_ptr(),
                temp_wide.as_ptr(),
                std::ptr::null(),
                0,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            )
        }
        else {
            MoveFileExW(temp_wide.as_ptr(), destination_wide.as_ptr(), MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH)
        }
    };
    if result == 0 {
        Err(Error::Io(std::io::Error::last_os_error()))
    }
    else {
        Ok(())
    }
}

/// Compare-and-set helper built on top of [`KvStore`].
pub fn compare_and_put(store: &mut dyn KvStore, key: Key, expect_revision: Option<u64>, value: StoredValue) -> Result<u64> {
    let current = store.get(&key)?;
    match (expect_revision, current) {
        (None, Some(_)) => Err(Error::CasConflict { key: key.display() }),
        (Some(expected), Some(record)) if record.revision != expected => Err(Error::CasConflict { key: key.display() }),
        _ => store.put(key, value),
    }
}
