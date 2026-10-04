//! SQLite typed runtime values.

/// One SQLite storage-class value with lossless integer and binary payloads.
#[derive(Debug, Clone, PartialEq)]
pub enum SqliteValue {
    /// SQL NULL.
    Null,
    /// Signed SQLite INTEGER.
    Integer(i64),
    /// SQLite REAL.
    Real(f64),
    /// SQLite TEXT bytes (UTF-8 or opaque).
    Text(Vec<u8>),
    /// SQLite BLOB bytes.
    Blob(Vec<u8>),
}

impl SqliteValue {
    /// Returns the contract storage-class label.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::Null => "null",
            Self::Integer(_) => "integer",
            Self::Real(_) => "real",
            Self::Text(_) => "text",
            Self::Blob(_) => "blob",
        }
    }
}

impl std::fmt::Display for SqliteValue {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Null => Ok(()),
            Self::Integer(value) => write!(formatter, "{value}"),
            Self::Real(value) => write!(formatter, "{value}"),
            Self::Text(value) => formatter.write_str(&String::from_utf8_lossy(value)),
            Self::Blob(value) => {
                formatter.write_str("x'")?;
                for byte in value {
                    write!(formatter, "{byte:02x}")?;
                }
                formatter.write_str("'")
            }
        }
    }
}
