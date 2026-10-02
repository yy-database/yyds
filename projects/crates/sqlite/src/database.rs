//! File-backed and in-memory SQLite databases via the YYDS engine.

use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use yyds_types::{Error, Result};

use crate::format::{
    blank_database, validate_database, ENGINE_LIBRARY_VERSION, ENGINE_LIBRARY_VERSION_NUMBER,
    stamp_library_version,
};

/// Storage mode for [`SqliteDatabase`].
#[derive(Debug, Clone, PartialEq, Eq)]
enum Storage {
    Memory,
    File(PathBuf),
}

/// SQLite database opened by the YYDS pure-Rust engine.
pub struct SqliteDatabase {
    storage: Storage,
    page_size: usize,
    pages: Vec<u8>,
}

impl SqliteDatabase {
    /// Opens an in-memory SQLite database with a blank format-3 page.
    pub fn open_in_memory() -> Result<Self> {
        let pages = blank_database()?;
        let page_size = validate_database(&pages)?;
        Ok(Self {
            storage: Storage::Memory,
            page_size,
            pages,
        })
    }

    /// Opens or creates a file-backed SQLite database.
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        if path.exists() {
            let bytes = fs::read(&path).map_err(Error::Io)?;
            let page_size = validate_database(&bytes)?;
            return Ok(Self {
                storage: Storage::File(path),
                page_size,
                pages: bytes,
            });
        }

        if let Some(parent) = path.parent() {
            if !parent.as_os_str().is_empty() {
                fs::create_dir_all(parent).map_err(Error::Io)?;
            }
        }

        let pages = blank_database()?;
        let page_size = validate_database(&pages)?;
        let db = Self {
            storage: Storage::File(path),
            page_size,
            pages,
        };
        db.flush()?;
        Ok(db)
    }

    /// Returns the configured database path for file-backed databases.
    pub fn path(&self) -> Option<&Path> {
        match &self.storage {
            Storage::Memory => None,
            Storage::File(path) => Some(path),
        }
    }

    /// Returns the page size decoded from the database header.
    pub fn page_size(&self) -> usize {
        self.page_size
    }

    /// Returns the raw database bytes owned by this handle.
    pub fn pages(&self) -> &[u8] {
        &self.pages
    }

    /// Health probe for disguise tooling and connector smoke tests.
    pub fn ping(&self) -> Result<i32> {
        validate_database(&self.pages)?;
        Ok(1)
    }

    /// Returns the YYDS SQLite engine version (`3.45.0` today).
    pub fn sqlite_version(&self) -> Result<String> {
        validate_database(&self.pages)?;
        Ok(ENGINE_LIBRARY_VERSION.to_string())
    }

    /// Persists file-backed databases. In-memory databases are a no-op.
    pub fn flush(&self) -> Result<()> {
        let path = match &self.storage {
            Storage::Memory => return Ok(()),
            Storage::File(path) => path,
        };

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
            .map_err(Error::Io)?;
        file.write_all(&self.pages).map_err(Error::Io)?;
        file.flush().map_err(Error::Io)?;
        Ok(())
    }

    /// Re-stamps the format-3 library version field with the YYDS engine version.
    pub fn restamp_engine_version(&mut self) -> Result<()> {
        validate_database(&self.pages)?;
        stamp_library_version(&mut self.pages, ENGINE_LIBRARY_VERSION_NUMBER);
        Ok(())
    }
}

impl Drop for SqliteDatabase {
    fn drop(&mut self) {
        if matches!(self.storage, Storage::File(_)) {
            let _ = self.flush();
        }
    }
}

/// Reads an existing SQLite file without keeping it open.
pub fn read_existing(path: impl AsRef<Path>) -> Result<Vec<u8>> {
    let path = path.as_ref();
    let mut file = File::open(path).map_err(Error::Io)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).map_err(Error::Io)?;
    validate_database(&bytes)?;
    Ok(bytes)
}
