//! Wire frame → YYDS kernel dispatch.
//!
//! Each incoming frame is decoded into a concrete kernel call: KV commands go
//! through [`KeyValueExecutor`], schema commands go through [`Catalog`]. This
//! keeps protocol parsing (wire.rs) separate from kernel execution.

use std::sync::Arc;

use database::{
    cluster::catalog::Catalog,
    execution::cluster::KeyValueExecutor,
    protocol::yy::{
        Frame, MessageType, decode_kv_get, decode_kv_put, decode_schema_ensure, encode_kv_get_ok, encode_schema_ok, error_frame,
    },
    query::cluster_kv::{KeyValueAction, KeyValueCommand, KeyValueResult},
    types::{Error as DatabaseError, SchemaVersion, cluster::Namespace},
};

/// Default namespace for YY-native protocol KV requests.
///
/// Redis adapter uses `redis:0`…`redis:15` (per `SELECT`). YY-native protocol
/// has no namespace concept on the wire, so we pin one here. **Cross-protocol
/// data sharing is explicitly NOT provided by Phase-1**: a key written via
/// the YY path (`Namespace("default")`) is invisible to the Redis path and
/// vice versa. Namespace mapping between protocol adapters will be designed
/// once the kernel contract (schema ↔ KV) is wired up.
pub const DEFAULT_NAMESPACE: &str = "default";

/// 内核侧失败统一按协议错误回复,诊断文本保留原始 `Display`,不按文案分支。
fn kernel_error(id: u32, message: impl Into<String>) -> Frame {
    error_frame(id, &DatabaseError::Protocol { message: message.into() })
}

/// Shared context for wire dispatch: KV executor + optional catalog.
///
/// The catalog is `Option<Arc<Mutex<Catalog>>>` because Phase-1 doesn't yet
/// wire catalog into the runtime — but dispatch needs the slot ready so the
/// connection between wire adapter and catalog happens without reworking
/// frame handlers later.
///
/// `bootstrap_tokens` is the server-side registry accepted per-connection
/// Authenticate frames (see wire_serve.rs handle_client). An empty Vec
/// means loopback-only anonymous dev mode — no Authenticate required.
pub struct WireContext {
    pub executor: Arc<dyn KeyValueExecutor>,
    /// Shared catalog for schema/identity operations. Phase-1 can be None
    /// (schema returns `None`, SchemaEnsure is rejected) but the struct
    /// holds the slot so the wiring point is explicit.
    pub catalog: std::sync::Mutex<Option<Arc<std::sync::Mutex<Catalog>>>>,
    /// Bootstrap tokens parsed from `YYDS_AUTH_TOKENS` by the server. An
    /// empty list signals anonymous-dev-mode (loopback bind only).
    pub bootstrap_tokens: Vec<String>,
}

impl WireContext {
    pub fn new(executor: Arc<dyn KeyValueExecutor>) -> Self {
        Self { executor, catalog: std::sync::Mutex::new(None), bootstrap_tokens: Vec::new() }
    }

    pub fn with_bootstrap_tokens(mut self, tokens: Vec<String>) -> Self {
        self.bootstrap_tokens = tokens;
        self
    }

    #[allow(dead_code)]
    pub fn with_catalog(self, catalog: Arc<std::sync::Mutex<Catalog>>) -> Self {
        *self.catalog.lock().unwrap() = Some(catalog);
        self
    }
}

/// Dispatch one wire frame to the YYDS kernel.
pub fn dispatch(ctx: &WireContext, request: &Frame) -> Frame {
    let id = request.request_id;
    match request.msg_type {
        MessageType::Hello => {
            let version = env!("CARGO_PKG_VERSION");
            Frame::new(MessageType::HelloOk, id, version.as_bytes().to_vec())
        }
        MessageType::Info => {
            let mut lines = Vec::new();
            lines.extend_from_slice(b"product=yyds\n");
            lines.extend_from_slice(env!("CARGO_PKG_VERSION").as_bytes());
            lines.push(b'\n');
            // Catalog schema version, if any.
            if let Some(ref catalog_arc) = *ctx.catalog.lock().unwrap() {
                if let Ok(catalog) = catalog_arc.lock() {
                    let line = match catalog.schema() {
                        Some(schema) => format!("schema.version={}\n", schema.version),
                        None => "schema.version=\n".into(),
                    };
                    lines.extend_from_slice(line.as_bytes());
                }
            }
            Frame::new(MessageType::InfoOk, id, lines)
        }
        MessageType::SchemaGet => {
            let guard = ctx.catalog.lock().unwrap();
            match guard.as_ref() {
                Some(catalog_arc) => match catalog_arc.lock() {
                    Ok(catalog) => {
                        let schema = catalog
                            .schema()
                            .map(|schema| SchemaVersion { version: schema.version, document: schema.document.clone() });
                        Frame::new(MessageType::SchemaGetOk, id, encode_schema_ok(schema))
                    }
                    Err(_) => kernel_error(id, "catalog lock poisoned"),
                },
                None => Frame::new(MessageType::SchemaGetOk, id, encode_schema_ok(None)),
            }
        }
        MessageType::SchemaEnsure => {
            let (version, document) = match decode_schema_ensure(&request.body) {
                Ok(pair) => pair,
                Err(error) => return error_frame(id, &error),
            };
            let guard = ctx.catalog.lock().unwrap();
            match guard.as_ref() {
                Some(catalog_arc) => match catalog_arc.lock() {
                    Ok(mut catalog) => match catalog.ensure_schema(version, &document) {
                        Ok(()) => {
                            if let Err(error) = catalog.flush() {
                                return kernel_error(id, error.to_string());
                            }
                            Frame::new(MessageType::SchemaEnsureOk, id, Vec::new())
                        }
                        Err(error) => kernel_error(id, error.to_string()),
                    },
                    Err(_) => kernel_error(id, "catalog lock poisoned"),
                },
                None => error_frame(id, &DatabaseError::Unsupported("YYDS wire catalog is not wired up yet")),
            }
        }
        MessageType::KvGet => {
            let key = match decode_kv_get(&request.body) {
                Ok(k) => k,
                Err(error) => return error_frame(id, &error),
            };
            let command = KeyValueCommand {
                namespace: Namespace(DEFAULT_NAMESPACE.to_string()),
                key: key.into_bytes(),
                action: KeyValueAction::Get,
            };
            match ctx.executor.execute(None, command) {
                Ok(KeyValueResult::Get(value)) => Frame::new(MessageType::KvGetOk, id, encode_kv_get_ok(value)),
                Ok(_) => kernel_error(id, "executor returned unexpected result for Get"),
                Err(error) => kernel_error(id, error.to_string()),
            }
        }
        MessageType::KvPut => {
            let (key, value) = match decode_kv_put(&request.body) {
                Ok(pair) => pair,
                Err(error) => return error_frame(id, &error),
            };
            let command = KeyValueCommand {
                namespace: Namespace(DEFAULT_NAMESPACE.to_string()),
                key: key.into_bytes(),
                action: KeyValueAction::Put(value),
            };
            match ctx.executor.execute(None, command) {
                Ok(KeyValueResult::Put { revision }) => {
                    let _ = revision; // revision not exposed on wire in Phase-1
                    Frame::new(MessageType::KvPutOk, id, Vec::new())
                }
                Ok(_) => kernel_error(id, "executor returned unexpected result for Put"),
                Err(error) => kernel_error(id, error.to_string()),
            }
        }
        // ---- Messages the server never initiates, or are handled above dispatch ----
        MessageType::HelloOk
        | MessageType::InfoOk
        | MessageType::SchemaGetOk
        | MessageType::SchemaEnsureOk
        | MessageType::KvGetOk
        | MessageType::KvPutOk
        | MessageType::MicroRegisterOk
        | MessageType::MicroHostInvoke
        | MessageType::MicroHostInvokeOk
        | MessageType::ScalarCallOk
        | MessageType::Authenticate
        | MessageType::AuthenticateOk
        | MessageType::Prepare
        | MessageType::PrepareOk
        | MessageType::ExecutePrepared
        | MessageType::ExecutePreparedOk
        | MessageType::Error => kernel_error(id, "unexpected client message type"),
        MessageType::MicroRegister | MessageType::ScalarCall => {
            error_frame(id, &DatabaseError::Unsupported("YYDS wire dispatch does not serve UDF micro frames yet"))
        }
        MessageType::Unknown(code) => kernel_error(id, format!("unknown msg_type {code}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use database::{
        cluster::topology::ShardMap,
        execution::cluster::LocalExecutor,
        storage::cluster_kv::MemoryShard,
        types::cluster::{ShardEpoch, ShardId},
    };
    use std::collections::HashMap;

    fn test_ctx() -> WireContext {
        let routing = ShardMap::new(ShardEpoch(1), vec![ShardId("local-0".into())]).unwrap();
        let shard_id = ShardId("local-0".into());
        let shard = MemoryShard::new();
        let mut shards = HashMap::new();
        shards.insert(shard_id, shard);
        let executor = LocalExecutor::new(routing, shards, Vec::new()).unwrap();
        WireContext::new(Arc::new(executor))
    }

    #[test]
    fn hello_returns_server_version() {
        let ctx = test_ctx();
        let request = Frame::new(MessageType::Hello, 1, Vec::new());
        let response = dispatch(&ctx, &request);
        assert_eq!(response.msg_type, MessageType::HelloOk);
        assert_eq!(response.request_id, 1);
        assert!(!response.body.is_empty());
    }
}
