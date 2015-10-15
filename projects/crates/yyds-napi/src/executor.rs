use std::{path::PathBuf, sync::Arc};

use napi::bindgen_prelude::*;
use napi_derive::napi;
use yyds_execution::KeyValueExecutor;
use crate::node_runtime::open_executor;

/// Local key-value executor for one node directory.
#[napi]
pub struct LocalExecutorHandle {
    pub(crate) inner: Arc<dyn KeyValueExecutor>,
}

#[napi]
impl LocalExecutorHandle {
    #[napi(constructor)]
    pub fn open(data_dir: String) -> Result<Self> {
        Ok(Self { inner: open_executor(PathBuf::from(data_dir).as_path()).map_err(|error| Error::from_reason(error))? })
    }
}
