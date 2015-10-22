use std::{net::SocketAddr, sync::Arc};

use napi::bindgen_prelude::*;
use napi_derive::napi;
use yyds_gateway_mysql::service::MysqlService;

use crate::node::NodeLeaseHandle;

/// Loopback MySQL protocol listener bound to a node lease.
#[napi]
pub struct MysqlServiceHandle {
    inner: Option<MysqlService>,
}

#[napi]
impl MysqlServiceHandle {
    #[napi(constructor)]
    pub fn start(lease: &NodeLeaseHandle, port: u16) -> Result<Self> {
        let address = SocketAddr::from(([127, 0, 0, 1], port));
        let service = MysqlService::start(Arc::clone(&lease.inner), address, None).map_err(map_io)?;
        Ok(Self { inner: Some(service) })
    }

    #[napi]
    pub fn address(&self) -> Result<String> {
        self.inner.as_ref().map(|service| service.address().to_string()).ok_or_else(missing)
    }

    #[napi]
    pub fn is_finished(&self) -> Result<bool> {
        self.inner.as_ref().map(MysqlService::is_finished).ok_or_else(missing)
    }

    #[napi]
    pub fn stop(&mut self) -> Result<()> {
        let service = self.inner.take().ok_or_else(missing)?;
        service.stop().map_err(map_io)
    }
}

fn missing() -> Error {
    Error::from_reason("mysql service is not running")
}

fn map_io(error: std::io::Error) -> Error {
    Error::from_reason(error.to_string())
}
