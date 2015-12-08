//! Wire frame → YYDS kernel dispatch.
//!
//! Each incoming frame is decoded into a concrete kernel call: KV commands go
//! through [`KeyValueExecutor`], schema commands go through [`Catalog`]. This
//! keeps protocol parsing (wire.rs) separate from kernel execution.

use std::sync::Arc;

use yyds_catalog::Catalog;
use yyds_execution::KeyValueExecutor;
use yyds_types::{KeyValueAction, KeyValueCommand, Namespace};

use crate::wire_protocol::{
    decode_kv_get, decode_kv_put, decode_schema_ensure, encode_kv_get_ok, encode_schema_ok, error_frame, Frame, MessageType,
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

/// Error code returned when a requested operation is not yet wired up.
const UNSUPPORTED_CODE: &str = "yyds.unsupported";
/// Error code for malformed wire bodies.
const PROTOCOL_CODE: &str = "yyds.protocol.invalid";
/// Error code for kernel-level errors.
const KERNEL_CODE: &str = "yyds.kernel.error";

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
                        let schema = catalog.schema().map(|s| (s.version, s.document.as_str()));
                        Frame::new(MessageType::SchemaGetOk, id, encode_schema_ok(schema))
                    }
                    Err(_) => error_frame(id, KERNEL_CODE, "catalog lock poisoned"),
                },
                None => Frame::new(MessageType::SchemaGetOk, id, encode_schema_ok(None)),
            }
        }
        MessageType::SchemaEnsure => {
            let (version, document) = match decode_schema_ensure(&request.body) {
                Ok(pair) => pair,
                Err(error) => return error_frame(id, PROTOCOL_CODE, error),
            };
            let guard = ctx.catalog.lock().unwrap();
            match guard.as_ref() {
                Some(catalog_arc) => match catalog_arc.lock() {
                    Ok(mut catalog) => match catalog.ensure_schema(version, &document) {
                        Ok(()) => {
                            if let Err(error) = catalog.flush() {
                                return error_frame(id, KERNEL_CODE, error.to_string());
                            }
                            Frame::new(MessageType::SchemaEnsureOk, id, Vec::new())
                        }
                        Err(error) => error_frame(id, KERNEL_CODE, error.to_string()),
                    },
                    Err(_) => error_frame(id, KERNEL_CODE, "catalog lock poisoned"),
                },
                None => error_frame(id, UNSUPPORTED_CODE, "YYDS wire catalog is not wired up yet"),
            }
        }
        MessageType::KvGet => {
            let key = match decode_kv_get(&request.body) {
                Ok(k) => k,
                Err(error) => return error_frame(id, PROTOCOL_CODE, error),
            };
            let command = KeyValueCommand {
                namespace: Namespace(DEFAULT_NAMESPACE.to_string()),
                key: key.into_bytes(),
                action: KeyValueAction::Get,
            };
            match ctx.executor.execute(command) {
                Ok(yyds_types::KeyValueResult::Get(value)) => Frame::new(MessageType::KvGetOk, id, encode_kv_get_ok(value)),
                Ok(_) => error_frame(id, KERNEL_CODE, "executor returned unexpected result for Get"),
                Err(error) => error_frame(id, KERNEL_CODE, error.to_string()),
            }
        }
        MessageType::KvPut => {
            let (key, value) = match decode_kv_put(&request.body) {
                Ok(pair) => pair,
                Err(error) => return error_frame(id, PROTOCOL_CODE, error),
            };
            let command = KeyValueCommand {
                namespace: Namespace(DEFAULT_NAMESPACE.to_string()),
                key: key.into_bytes(),
                action: KeyValueAction::Put(value),
            };
            match ctx.executor.execute(command) {
                Ok(yyds_types::KeyValueResult::Put { revision }) => {
                    let _ = revision; // revision not exposed on wire in Phase-1
                    Frame::new(MessageType::KvPutOk, id, Vec::new())
                }
                Ok(_) => error_frame(id, KERNEL_CODE, "executor returned unexpected result for Put"),
                Err(error) => error_frame(id, KERNEL_CODE, error.to_string()),
            }
        }
        // ---- Messages the server never initiates, or are handled above dispatch ----
        MessageType::HelloOk
        | MessageType::InfoOk
        | MessageType::SchemaGetOk
        | MessageType::SchemaEnsureOk
        | MessageType::KvGetOk
        | MessageType::KvPutOk
        | MessageType::Authenticate
        | MessageType::AuthenticateOk
        | MessageType::Error => error_frame(id, PROTOCOL_CODE, "unexpected client message type"),
        MessageType::Unknown(code) => error_frame(id, PROTOCOL_CODE, format!("unknown msg_type {code}")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use yyds_execution::LocalExecutor;
    use yyds_kv::MemoryShard;
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    fn test_ctx() -> WireContext {
        let routing = ShardMap::new(ShardEpoch(1), vec![ShardId("local-0".into())]).unwrap();
        let shard_id = ShardId("local-0".into());
        let shard = MemoryShard::new();
        let mut shards = HashMap::new();
        shards.insert(shard_id, shard);
        let executor = LocalExecutor::new(routing, shards).unwrap();
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

    #[test]
    fn kv_put_then_kv_get_roundtrip() {
        let ctx = test_ctx();

        let put = Frame::new(MessageType::KvPut, 10, decode_kv_put_body("mykey", b"hello world"));
        let put_resp = dispatch(&ctx, &put);
        assert_eq!(put_resp.msg_type, MessageType::KvPutOk);
        assert_eq!(put_resp.request_id, 10);

        let get = Frame::new(MessageType::KvGet, 11, decode_kv_get_body("mykey"));
        let get_resp = dispatch(&ctx, &get);
        assert_eq!(get_resp.msg_type, MessageType::KvGetOk);
        assert_eq!(get_resp.request_id, 11);

        let decoded = encode_kv_get_ok_body(&get_resp.body);
        assert_eq!(decoded, Some(b"hello world".to_vec()));
    }

    #[test]
    fn kv_get_missing_returns_none_flag() {
        let ctx = test_ctx();
        let get = Frame::new(MessageType::KvGet, 1, decode_kv_get_body("nope"));
        let resp = dispatch(&ctx, &get);
        assert_eq!(resp.msg_type, MessageType::KvGetOk);
        assert_eq!(encode_kv_get_ok_body(&resp.body), None);
    }

    #[test]
    fn schema_get_returns_none_when_no_catalog() {
        let ctx = test_ctx();
        let request = Frame::new(MessageType::SchemaGet, 1, Vec::new());
        let resp = dispatch(&ctx, &request);
        assert_eq!(resp.msg_type, MessageType::SchemaGetOk);
        assert_eq!(resp.body.len(), 1);
        assert_eq!(resp.body[0], 0);
    }

    #[test]
    fn dispatch_rejects_client_only_types() {
        let ctx = test_ctx();
        let ok_types = [
            MessageType::HelloOk,
            MessageType::InfoOk,
            MessageType::SchemaGetOk,
            MessageType::SchemaEnsureOk,
            MessageType::KvGetOk,
            MessageType::KvPutOk,
            MessageType::Error,
        ];
        for mt in ok_types {
            let request = Frame::new(mt, 1, Vec::new());
            let resp = dispatch(&ctx, &request);
            assert_eq!(resp.msg_type, MessageType::Error);
        }
    }

    // Helpers that mirror wire body construction used by the TypeScript client.

    fn decode_kv_put_body(key: &str, value: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        push_u32(&mut body, key.len() as u32);
        body.extend_from_slice(key.as_bytes());
        push_u32(&mut body, value.len() as u32);
        body.extend_from_slice(value);
        body
    }

    fn decode_kv_get_body(key: &str) -> Vec<u8> {
        let mut body = Vec::new();
        push_u32(&mut body, key.len() as u32);
        body.extend_from_slice(key.as_bytes());
        body
    }

    fn encode_kv_get_ok_body(body: &[u8]) -> Option<Vec<u8>> {
        if body.len() < 1 {
            return None;
        }
        if body[0] == 0 {
            return None;
        }
        let mut offset = 1;
        let len = u32::from_le_bytes([body[offset], body[offset + 1], body[offset + 2], body[offset + 3]]);
        offset += 4;
        Some(body[offset..offset + len as usize].to_vec())
    }

    fn push_u32(out: &mut Vec<u8>, value: u32) {
        out.extend_from_slice(&value.to_le_bytes());
    }
}
