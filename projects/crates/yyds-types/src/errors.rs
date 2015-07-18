use std::{error, fmt, io, result};

/// Convenient alias used across YYDS APIs.
pub type Result<T> = result::Result<T, Error>;

/// Errors produced by YYDS catalog, shard, and wire paths.
#[derive(Debug)]
pub enum Error {
    /// Underlying filesystem or I/O failure.
    Io(io::Error),
    /// Shard or catalog bytes do not match the expected layout.
    Corrupt(&'static str),
    /// Requested key is absent from the shard.
    NotFound { key: String },
    /// Stored catalog schema version does not match the caller's expectation.
    SchemaConflict { expected: u32, found: u32 },
    /// A routing epoch is not newer than the published catalog epoch.
    RoutingConflict { expected: u64, found: u64 },
    /// Schema document failed VOS validation (shared language contract).
    Schema { message: String },
    /// Feature exists as a product surface but is not implemented yet.
    Unsupported(&'static str),
    /// Independent compare-and-set failed.
    CasConflict { key: String },
    /// VOS-authored UDF could not be lowered into the distributed execution model.
    Udf { name: String, message: String },
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(err) => write!(f, "io error: {err}"),
            Self::Corrupt(reason) => write!(f, "corrupt yyds data: {reason}"),
            Self::NotFound { key } => write!(f, "key not found: {key}"),
            Self::SchemaConflict { expected, found } => {
                write!(f, "catalog schema version conflict: expected {expected}, found {found}")
            }
            Self::RoutingConflict { expected, found } => {
                write!(f, "routing epoch conflict: expected newer than {expected}, found {found}")
            }
            Self::Schema { message } => write!(f, "VOS schema: {message}"),
            Self::Unsupported(feature) => write!(f, "unsupported: {feature}"),
            Self::CasConflict { key } => write!(f, "cas conflict on key: {key}"),
            Self::Udf { name, message } => write!(f, "UDF {name}: {message}"),
        }
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Io(err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(value: io::Error) -> Self {
        Self::Io(value)
    }
}
