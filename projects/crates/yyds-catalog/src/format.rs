//! On-disk `.yyds` catalog bytes (distinct from the embedded `.yydb` layout).

use std::io::{Cursor, Read, Write};

use yyds_types::{CatalogSchema, Error, Result, ShardId};

/// Catalog file magic (`YYDS` catalog plane, not `.yydb`).
pub const MAGIC: &[u8] = b"YYDS\x01";

const FORMAT_VERSION: u32 = 2;

/// Serialize catalog state to `.yyds` bytes.
pub fn encode(
    schema: Option<&CatalogSchema>,
    identity: Option<&vos::ast::CatalogSnapshot>,
    shards: &[ShardId],
) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    out.write_all(MAGIC)?;
    out.write_all(&FORMAT_VERSION.to_le_bytes())?;

    match schema {
        Some(schema) => {
            out.write_all(&1u32.to_le_bytes())?;
            out.write_all(&schema.version.to_le_bytes())?;
            let doc = schema.document.as_bytes();
            if doc.len() > u32::MAX as usize {
                return Err(Error::Unsupported("catalog schema document too large"));
            }
            out.write_all(&(doc.len() as u32).to_le_bytes())?;
            out.write_all(doc)?;
        }
        None => out.write_all(&0u32.to_le_bytes())?,
    }

    match identity {
        Some(identity) => {
            let json = serde_json::to_vec(identity).map_err(|_| Error::Corrupt("catalog identity encode"))?;
            if json.len() > u32::MAX as usize {
                return Err(Error::Unsupported("catalog identity too large"));
            }
            out.write_all(&1u32.to_le_bytes())?;
            out.write_all(&(json.len() as u32).to_le_bytes())?;
            out.write_all(&json)?;
        }
        None => out.write_all(&0u32.to_le_bytes())?,
    }

    if shards.len() > u32::MAX as usize {
        return Err(Error::Unsupported("too many shard registrations"));
    }
    out.write_all(&(shards.len() as u32).to_le_bytes())?;
    for shard in shards {
        let id = shard.0.as_bytes();
        if id.len() > u32::MAX as usize {
            return Err(Error::Unsupported("shard id too long"));
        }
        out.write_all(&(id.len() as u32).to_le_bytes())?;
        out.write_all(id)?;
    }

    Ok(out)
}

/// Parse catalog bytes previously written by [`encode`].
pub fn decode(
    bytes: &[u8],
) -> Result<(Option<CatalogSchema>, Option<vos::ast::CatalogSnapshot>, Vec<ShardId>)> {
    let mut cursor = Cursor::new(bytes);
    let mut magic = [0u8; MAGIC.len()];
    read_exact(&mut cursor, &mut magic)?;
    if magic.as_slice() != MAGIC {
        return Err(Error::Corrupt("unknown catalog header"));
    }

    let format_version = read_u32(&mut cursor)?;
    if format_version != 1 && format_version != FORMAT_VERSION {
        return Err(Error::Corrupt("unsupported catalog format version"));
    }

    let schema = match read_u32(&mut cursor)? {
        0 => None,
        1 => {
            let version = read_u32(&mut cursor)?;
            let len = read_u32(&mut cursor)? as usize;
            let mut doc = vec![0u8; len];
            read_exact(&mut cursor, &mut doc)?;
            let document = String::from_utf8(doc).map_err(|_| Error::Corrupt("catalog schema utf8"))?;
            Some(CatalogSchema { version, document })
        }
        _ => return Err(Error::Corrupt("invalid catalog schema presence flag")),
    };

    let identity = if format_version >= 2 {
        match read_u32(&mut cursor)? {
            0 => None,
            1 => {
                let len = read_u32(&mut cursor)? as usize;
                let mut json = vec![0u8; len];
                read_exact(&mut cursor, &mut json)?;
                Some(serde_json::from_slice(&json).map_err(|_| Error::Corrupt("catalog identity decode"))?)
            }
            _ => return Err(Error::Corrupt("invalid catalog identity presence flag")),
        }
    } else {
        None
    };

    let shard_count = read_u32(&mut cursor)? as usize;
    let mut shards = Vec::with_capacity(shard_count);
    for _ in 0..shard_count {
        let len = read_u32(&mut cursor)? as usize;
        let mut id = vec![0u8; len];
        read_exact(&mut cursor, &mut id)?;
        let id = String::from_utf8(id).map_err(|_| Error::Corrupt("catalog shard id utf8"))?;
        shards.push(ShardId(id));
    }

    if cursor.position() != bytes.len() as u64 {
        return Err(Error::Corrupt("trailing catalog bytes"));
    }

    Ok((schema, identity, shards))
}

fn read_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut buf = [0u8; 4];
    read_exact(cursor, &mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_exact(cursor: &mut Cursor<&[u8]>, buf: &mut [u8]) -> Result<()> {
    cursor
        .read_exact(buf)
        .map_err(|err| Error::Corrupt(match err.kind() {
            std::io::ErrorKind::UnexpectedEof => "unexpected catalog eof",
            _ => "catalog read failed",
        }))
}
