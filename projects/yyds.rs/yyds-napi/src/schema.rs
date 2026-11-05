use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use database::query::SqlSchemaRegistry;

/// One persisted SQL schema registry shared by every gateway of a node.
///
/// Both SQL gateways must use the same instance: a per-gateway registry loaded
/// from the same file would allocate colliding type ids and then overwrite each
/// other's persisted state on every DDL.
#[napi]
pub struct SqlSchemaRegistryHandle {
    pub(crate) inner: Arc<SqlSchemaRegistry>,
}

#[napi]
impl SqlSchemaRegistryHandle {
    /// Opens `yyds-sql-schema/v0`, starting from an empty schema when absent.
    #[napi(factory)]
    pub fn open(path: String) -> Result<Self> {
        let registry = SqlSchemaRegistry::open(&path)
            .map_err(|error| Error::from_reason(format!("open SQL schema: {}", error.message)))?;
        Ok(Self { inner: Arc::new(registry) })
    }

    /// Current schema revision, bumped by every applied DDL.
    #[napi]
    pub fn revision(&self) -> u32 {
        self.inner.revision()
    }
}
