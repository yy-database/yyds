use std::{net::SocketAddr, path::PathBuf, sync::Arc};

use crate::{
    executor::LocalExecutorHandle,
    node_runtime::open_executor,
    wire_backend::{KvBackendFactory, serve_options},
};
use database::server::yydb::WireServe;
use napi::bindgen_prelude::*;
use napi_derive::napi;

/// Loopback `/wire` listener for the YY native protocol.
///
/// 线协议循环、鉴权门与权限门由上游 `database.server::yydb` 提供,本句柄只
/// 组装分片执行后端与启动参数,不再持有 serve/dispatch 状态机。
#[napi]
pub struct WireServerHandle {
    inner: Option<WireServe>,
}

#[napi]
impl WireServerHandle {
    /// Open the catalog-driven executor from a node directory, then start
    /// the wire server on a loopback port.
    ///
    /// This is the **node start chain** entry point — one call wires up
    /// `open_executor` + the shared wire serve instead of asking the caller
    /// to construct the executor separately.
    ///
    /// `catalog_path` is optional (pass an empty string to fall back to
    /// local-only shards, mirroring [`LocalExecutorHandle::open`]).
    #[napi]
    pub fn start_standalone(data_dir: String, catalog_path: String, port: u16) -> Result<Self> {
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let directory = PathBuf::from(&data_dir);
        let catalog = if catalog_path.is_empty() { None } else { Some(PathBuf::from(catalog_path)) };
        let executor = open_executor(directory.as_path(), catalog.as_deref()).map_err(Error::from_reason)?;
        let factory = Arc::new(KvBackendFactory::new(executor, None));
        let options = serve_options(address).map_err(Error::from_reason)?;
        let server = WireServe::start(options, factory).map_err(Error::from_reason)?;
        Ok(Self { inner: Some(server) })
    }

    /// Legacy manual start — pass a pre-built executor.
    #[napi]
    pub fn start_with_executor(port: u16, executor: &LocalExecutorHandle) -> Result<Self> {
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let factory = Arc::new(KvBackendFactory::new(Arc::clone(&executor.inner), None));
        let options = serve_options(address).map_err(Error::from_reason)?;
        let server = WireServe::start(options, factory).map_err(Error::from_reason)?;
        Ok(Self { inner: Some(server) })
    }

    #[napi]
    pub fn address(&self) -> Result<String> {
        self.inner.as_ref().map(|server| server.address().to_string()).ok_or_else(missing)
    }

    #[napi]
    pub fn is_finished(&self) -> Result<bool> {
        self.inner.as_ref().map(WireServe::is_finished).ok_or_else(missing)
    }

    #[napi]
    pub fn stop(&mut self) -> Result<()> {
        let server = self.inner.take().ok_or_else(missing)?;
        server.stop().map_err(Error::from_reason)
    }
}

fn missing() -> Error {
    Error::from_reason("wire server is not running")
}
