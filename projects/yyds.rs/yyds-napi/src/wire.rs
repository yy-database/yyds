use std::{net::SocketAddr, sync::Arc};

use crate::executor::LocalExecutorHandle;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use yyds::{
    server::yydb::WireServe,
    wire::{KvBackendFactory, serve_options},
};

/// Loopback `/wire` listener for the YY native protocol.
///
/// 线协议循环、鉴权门与权限门由上游 `database.server::yydb` 提供,本句柄只把
/// 节点已打开的分片执行器接进共享后端并启动监听,不自己装配执行器。
#[napi]
pub struct WireServerHandle {
    inner: Option<WireServe>,
}

#[napi]
impl WireServerHandle {
    /// Serves the shared YY wire protocol with the node's own executor.
    ///
    /// 传入节点已打开的执行器句柄,保证线服务与其它协议网关读写同一份分片状态。
    #[napi(constructor)]
    pub fn new(port: u16, executor: &LocalExecutorHandle) -> Result<Self> {
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
