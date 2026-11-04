use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use crate::node_runtime::open_executor;
use napi::bindgen_prelude::*;
use napi_derive::napi;
use yyds::execution::{ExpirySweeper, KeyValueExecutor};

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

    /// Reclaims every record whose expiry deadline has elapsed, returning how many
    /// records were physically removed across all mounted shards.
    ///
    /// This is the unbounded maintenance entry: it scans every shard completely. The node's
    /// background sweeper uses a per-shard budget instead so its CPU share stays bounded.
    #[napi]
    pub fn sweep_expired(&self) -> Result<u32> {
        let reclaimed = self.inner.sweep_expired(None).map_err(|error| Error::from_reason(error.to_string()))?.reclaimed;
        u32::try_from(reclaimed).map_err(|_| Error::from_reason("reclaimed record count exceeds u32"))
    }
}

/// Background periodic expiry reclamation driver bound to a local executor.
///
/// Dropping the handle without calling `stop` leaks the worker thread until process exit;
/// callers that own a node lifecycle must stop the sweeper explicitly.
#[napi]
pub struct ExpirySweeperHandle {
    inner: Option<ExpirySweeper>,
}

#[napi]
impl ExpirySweeperHandle {
    /// Starts a sweeper that reclaims expired records every `interval_ms` milliseconds.
    #[napi(factory)]
    pub fn start(executor: &LocalExecutorHandle, interval_ms: u32) -> Result<Self> {
        if interval_ms == 0 {
            return Err(Error::from_reason("expiry sweep interval must be positive"));
        }
        let sweeper = ExpirySweeper::start(Arc::clone(&executor.inner), Duration::from_millis(u64::from(interval_ms)))
            .map_err(|error| Error::from_reason(error.to_string()))?;
        Ok(Self { inner: Some(sweeper) })
    }

    /// Stops the sweeper and waits for its worker to exit.
    #[napi]
    pub fn stop(&mut self) -> Result<()> {
        let sweeper = self.inner.take().ok_or_else(|| Error::from_reason("expiry sweeper is not running"))?;
        sweeper.stop();
        Ok(())
    }
}
