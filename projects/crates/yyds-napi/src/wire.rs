use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use crate::{wire_dispatch::WireContext, wire_serve::WireServer};
use napi::bindgen_prelude::*;
use napi_derive::napi;

use crate::executor::LocalExecutorHandle;

/// Loopback `/wire` listener for the YY native protocol.
#[napi]
pub struct WireServerHandle {
    inner: Option<WireServer>,
}

#[napi]
impl WireServerHandle {
    /// Open the catalog-driven executor from a node directory, then start
    /// the wire server on a loopback port.
    ///
    /// This is the **node start chain** entry point — one call wires up
    /// `open_executor` + `WireServer` instead of asking the caller to
    /// construct the executor separately.
    ///
    /// `catalog_path` is optional (pass an empty string to fall back to
    /// local-only shards, mirroring [`LocalExecutorHandle::open`]).
    #[napi]
    pub fn start_standalone(data_dir: String, catalog_path: String, port: u16) -> Result<Self> {
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let directory = PathBuf::from(&data_dir);
        let catalog = if catalog_path.is_empty() { None } else { Some(PathBuf::from(catalog_path)) };
        let server = WireServer::start_from_directory(address, directory.as_path(), catalog.as_deref())
            .map_err(|error| Error::from_reason(error))?;
        Ok(Self { inner: Some(server) })
    }

    /// Legacy manual start — pass a pre-built executor.
    #[napi]
    pub fn start_with_executor(port: u16, executor: &LocalExecutorHandle) -> Result<Self> {
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let context = WireContext::new(Arc::clone(&executor.inner));
        let server = WireServer::start(address, context).map_err(|error| Error::from_reason(error))?;
        Ok(Self { inner: Some(server) })
    }

    #[napi]
    pub fn address(&self) -> Result<String> {
        self.inner.as_ref().map(|server| server.address().to_string()).ok_or_else(missing)
    }

    #[napi]
    pub fn is_finished(&self) -> Result<bool> {
        self.inner.as_ref().map(WireServer::is_finished).ok_or_else(missing)
    }

    #[napi]
    pub fn stop(&mut self) -> Result<()> {
        let server = self.inner.take().ok_or_else(missing)?;
        server.stop().map_err(|error| Error::from_reason(error))
    }
}

fn missing() -> Error {
    Error::from_reason("wire server is not running")
}
