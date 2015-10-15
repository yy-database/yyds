use std::net::SocketAddr;
use std::sync::Arc;

use napi::bindgen_prelude::*;
use napi_derive::napi;
use crate::wire_dispatch::WireContext;
use crate::wire_serve::WireServer;

use crate::executor::LocalExecutorHandle;

/// Loopback `/wire` listener for the YY native protocol.
#[napi]
pub struct WireServerHandle {
    inner: Option<WireServer>,
}

#[napi]
impl WireServerHandle {
    #[napi(constructor)]
    pub fn start(port: u16, executor: &LocalExecutorHandle) -> Result<Self> {
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

