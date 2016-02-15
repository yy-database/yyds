use std::{net::SocketAddr, sync::Arc};

use napi::bindgen_prelude::*;
use napi_derive::napi;
use yyds_catalog::Catalog;
use yyds_gateway::SqlCatalog;
use yyds_gateway_pgsql::service::PgsqlService;

use crate::{executor::LocalExecutorHandle, node::NodeLeaseHandle};

/// Loopback PostgreSQL protocol listener bound to a node lease.
#[napi]
pub struct PgsqlServiceHandle {
    inner: Option<PgsqlService>,
}

#[napi]
impl PgsqlServiceHandle {
    /// Starts a protocol-only PostgreSQL listener (no SQL runtime).
    #[napi(constructor)]
    pub fn start(lease: &NodeLeaseHandle, port: u16) -> Result<Self> {
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let service = PgsqlService::start(Arc::clone(&lease.inner), address).map_err(map_io)?;
        Ok(Self { inner: Some(service) })
    }

    /// Starts a PostgreSQL listener with a YYDS SQL runtime for SELECT + DML execution.
    ///
    /// `catalog_path` 指向 `.yyds` catalog 文件，用于构造 SQL name-resolution 视图。
    #[napi(factory)]
    pub fn start_with_executor(
        lease: &NodeLeaseHandle,
        port: u16,
        executor: &LocalExecutorHandle,
        catalog_path: String,
    ) -> Result<Self> {
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let yyds_catalog = Catalog::open(&catalog_path).map_err(|msg| Error::from_reason(format!("open catalog: {msg}")))?;
        let schema_version = yyds_catalog
            .schema()
            .map(|s| s.version)
            .ok_or_else(|| Error::from_reason("YYDS catalog has no published schema"))?;
        let catalog = SqlCatalog::from_yyds_catalog(&yyds_catalog)
            .map_err(|error| Error::from_reason(format!("invalid YYDS catalog: {}", error.message)))?;
        let service = PgsqlService::start_with_executor(
            Arc::clone(&lease.inner),
            address,
            Arc::clone(&executor.inner),
            catalog,
            schema_version,
        )
        .map_err(map_io)?;
        Ok(Self { inner: Some(service) })
    }

    #[napi]
    pub fn address(&self) -> Result<String> {
        self.inner.as_ref().map(|service| service.address().to_string()).ok_or_else(missing)
    }

    #[napi]
    pub fn is_finished(&self) -> Result<bool> {
        self.inner.as_ref().map(PgsqlService::is_finished).ok_or_else(missing)
    }

    #[napi]
    pub fn stop(&mut self) -> Result<()> {
        let service = self.inner.take().ok_or_else(missing)?;
        service.stop().map_err(map_io)
    }
}

fn missing() -> Error {
    Error::from_reason("postgresql service is not running")
}

fn map_io(error: std::io::Error) -> Error {
    Error::from_reason(error.to_string())
}
