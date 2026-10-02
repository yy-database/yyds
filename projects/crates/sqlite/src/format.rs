//! SQLite format 3 header and blank-database materialization (pure Rust).

use yyds_types::{Error, Result};

/// SQLite format 3 magic prefix.
pub const MAGIC: &[u8] = b"SQLite format 3\0";

/// Default page size for newly created YYDS SQLite files.
pub const DEFAULT_PAGE_SIZE: usize = 4096;

/// YYDS SQLite engine library version reported by `sqlite_version()`.
pub const ENGINE_LIBRARY_VERSION: &str = "3.45.0";

/// SQLite library version integer (`MMmmpppp`) stamped into new files.
pub const ENGINE_LIBRARY_VERSION_NUMBER: u32 = 3_045_000;

/// Canonical blank database bytes (one 4096-byte page, validated by tests).
const BLANK_DATABASE: &[u8] = include_bytes!("assets/blank.sqlite");

/// Returns validated blank database bytes with the YYDS engine version stamped in.
pub fn blank_database() -> Result<Vec<u8>> {
    if BLANK_DATABASE.len() != DEFAULT_PAGE_SIZE {
        return Err(Error::Corrupt("blank sqlite asset has unexpected size"));
    }
    validate_header(BLANK_DATABASE)?;

    let mut pages = BLANK_DATABASE.to_vec();
    stamp_library_version(&mut pages, ENGINE_LIBRARY_VERSION_NUMBER);
    Ok(pages)
}

/// Validates the SQLite file header in `bytes`.
pub fn validate_header(bytes: &[u8]) -> Result<()> {
    if bytes.len() < 100 {
        return Err(Error::Corrupt("sqlite header too short"));
    }
    if bytes.get(..MAGIC.len()) != Some(MAGIC) {
        return Err(Error::Corrupt("sqlite magic mismatch"));
    }

    let page_size = decode_page_size(bytes)?;
    if page_size < 512 || !page_size.is_power_of_two() {
        return Err(Error::Corrupt("sqlite page size invalid"));
    }

    Ok(())
}

/// Validates an on-disk SQLite payload and returns its page size.
pub fn validate_database(bytes: &[u8]) -> Result<usize> {
    validate_header(bytes)?;
    let page_size = decode_page_size(bytes)?;
    if !bytes.len().is_multiple_of(page_size) {
        return Err(Error::Corrupt("sqlite payload is not page-aligned"));
    }
    Ok(page_size)
}

/// Decodes the SQLite library version string from a database header.
pub fn decode_library_version(bytes: &[u8]) -> Result<String> {
    validate_header(bytes)?;
    Ok(format_library_version(read_library_version_number(bytes)))
}

/// Writes the SQLite library version integer into `bytes` (header must exist).
pub fn stamp_library_version(bytes: &mut [u8], version_number: u32) {
    if bytes.len() >= 100 {
        bytes[96..100].copy_from_slice(&version_number.to_be_bytes());
    }
}

fn decode_page_size(bytes: &[u8]) -> Result<usize> {
    let raw = u16::from_be_bytes([bytes[16], bytes[17]]);
    let page_size = if raw == 1 {
        65_536
    } else {
        raw as usize
    };
    Ok(page_size)
}

fn read_library_version_number(bytes: &[u8]) -> u32 {
    u32::from_be_bytes([bytes[96], bytes[97], bytes[98], bytes[99]])
}

fn format_library_version(raw: u32) -> String {
    let major = raw / 1_000_000;
    let minor = (raw % 1_000_000) / 1_000;
    let patch = raw % 1_000;
    format!("{major}.{minor}.{patch}")
}
