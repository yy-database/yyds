#![deny(missing_debug_implementations, missing_copy_implementations)]
#![warn(missing_docs, rustdoc::missing_crate_level_docs)]
#![doc = include_str!("../readme.md")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/oovm/shape-rs/dev/projects/images/Trapezohedron.svg")]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/oovm/shape-rs/dev/projects/images/Trapezohedron.svg")]

mod errors;
mod identity;
mod schema;

pub use crate::errors::{Error, Result};
pub use crate::identity::{Namespace, ShardId, CATALOG_SUFFIX, SHARD_SUFFIX};
pub use crate::schema::{adopt_catalog_schema, validate_document, CatalogSchema, VOS_GIT_DEV};

/// Returns the `yyds-types` crate version.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
