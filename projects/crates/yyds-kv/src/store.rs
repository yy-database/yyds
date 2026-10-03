use std::{
    collections::{HashMap, HashSet},
    fs::{self, OpenOptions},
    io::{Cursor, Read, Write},
    path::{Path, PathBuf},
};

use yyds_types::{Error, Result};

use crate::{Key, Record, StoredValue};

const MAGIC: &[u8] = b"YYKV\x01";

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
        let record = Record { key: key.clone(), value, revision };
        self.records.insert(key, record);
        Ok(revision)
    }

    fn delete(&mut self, key: &Key) -> Result<bool> {
        Ok(self.records.remove(key).is_some())
    }
}

/// File-backed shard using the native `.yykv` snapshot format.
#[derive(Debug)]
pub struct FileShard {
    path: PathBuf,
    records: HashMap<Key, Record>,
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
            let shard = Self { path, records: HashMap::new(), next_revision: 0 };
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
        let bytes = encode(self)?;
        let temp = self.path.with_file_name(format!(
            ".{}.yykv-tmp-{}",
            self.path.file_name().and_then(|name| name.to_str()).unwrap_or("shard"),
            std::process::id()
        ));
        let result = (|| -> Result<()> {
            let mut file = OpenOptions::new().create(true).truncate(true).write(true).open(&temp)?;
            file.write_all(&bytes)?;
            file.sync_all()?;
            drop(file);
            replace_file(&temp, &self.path)
        })();
        if result.is_err() {
            let _ = fs::remove_file(&temp);
        }
        result
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
}

fn encode(shard: &FileShard) -> Result<Vec<u8>> {
    let mut records: Vec<&Record> = shard.records.values().collect();
    records.sort_by(|left, right| {
        left.key.namespace.0.cmp(&right.key.namespace.0).then_with(|| left.key.bytes.cmp(&right.key.bytes))
    });

    let mut bytes = Vec::new();
    bytes.write_all(MAGIC)?;
    bytes.write_all(&shard.next_revision.to_le_bytes())?;
    write_count(&mut bytes, records.len())?;
    for record in records {
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

fn decode(path: &Path, bytes: &[u8]) -> Result<FileShard> {
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0_u8; MAGIC.len()];
    read_exact(&mut cursor, &mut magic)?;
    if magic != MAGIC {
        return Err(Error::Corrupt("unknown yykv header"));
    }
    let next_revision = read_u64(&mut cursor)?;
    let count = read_u32(&mut cursor)? as usize;
    let mut records = HashMap::with_capacity(count);
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
        MOVEFILE_REPLACE_EXISTING, MOVEFILE_WRITE_THROUGH, MoveFileExW, ReplaceFileW,
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
    if result == 0 { Err(Error::Io(std::io::Error::last_os_error())) } else { Ok(()) }
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
