#![deny(missing_debug_implementations, missing_copy_implementations)]
#![warn(missing_docs, rustdoc::missing_crate_level_docs)]
#![doc = include_str!("../readme.md")]
#![doc(html_logo_url = "https://raw.githubusercontent.com/oovm/shape-rs/dev/projects/images/Trapezohedron.svg")]
#![doc(html_favicon_url = "https://raw.githubusercontent.com/oovm/shape-rs/dev/projects/images/Trapezohedron.svg")]

mod errors;
mod execution;
mod identity;
mod schema;

pub use crate::errors::{Error, Result};
pub use crate::execution::{
    Approximation, Consistency, DistributedPlan, EvalError, Exchange, FileRef, Fragment,
    FragmentId, FragmentRole, LayoutField, Node, PlanValidationError, Program, RecordLayout,
    RecordLayoutError,
    RecordValue, RecordValueError, Type, Udf, UdfEffect, UdfPlacement, UdfValidationError,
    ValidatedDistributedPlan, ValidatedProgram, ValidatedUdf, ValidationError, Value,
    VectorMetric, VectorValue, VectorValueError, RetryPolicy,
};
pub use crate::identity::{CATALOG_SUFFIX, Namespace, SHARD_SUFFIX, ShardId};
pub use crate::schema::{CatalogSchema, VOS_GIT_DEV, adopt_catalog_schema, validate_document};
/// Returns the `yyds-types` crate version.
pub fn version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}
