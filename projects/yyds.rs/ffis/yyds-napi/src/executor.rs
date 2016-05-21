use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use crate::node_runtime::open_executor;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use yyds_execution::KeyValueExecutor;

/// Local key-value executor for one node directory.
#[napi]
pub struct LocalExecutorHandle {
    pub(crate) inner: Arc<dyn KeyValueExecutor>,
}

#[napi]
impl LocalExecutorHandle {
    /// Opens the executor for one node directory with catalog-driven routing.
    ///
    /// `catalog_path` 指向 `.yyds` catalog 文件；传入空字符串时走本地 fallback。
    #[napi(constructor)]
    pub fn open(data_dir: String, catalog_path: Option<String>) -> Result<Self> {
        let catalog_owned = catalog_path.filter(|path| !path.is_empty()).map(|path| PathBuf::from(path));
        let catalog: Option<&Path> = catalog_owned.as_deref();
        Ok(Self {
            inner: open_executor(PathBuf::from(data_dir).as_path(), catalog).map_err(|error| Error::from_reason(error))?,
        })
    }
}
