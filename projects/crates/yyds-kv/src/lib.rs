#![deny(missing_debug_implementations)]
#![warn(missing_docs, rustdoc::missing_crate_level_docs)]
#![doc = include_str!("../readme.md")]

mod key;
mod record;
mod store;
mod value;

pub use crate::key::Key;
pub use crate::record::Record;
pub use crate::store::{compare_and_put, KvStore, MemoryShard};
pub use crate::value::{InlineValue, ObjectRef, StoredValue};
