---
name: yyds-serve-protocol
description: >-
  YY wire protocol for YYDS nodes: frame layout, message types, TCP/WebSocket.
  Load when implementing YYDS serve endpoints or clients that speak VOS-native wire semantics.
---

# YY wire protocol (VOS-native serve)

Binary request/response framing for **YY-family** servers. YYDS nodes encode product magic **`YYDS`**; embedded [YYDB](https://github.com/yy-database/yydb.rs) reference hosts encode **`YYDB`**. Wire semantics are identical — frontends accept either magic and do not require a product match.

## Prefix: product magic + version digits

| Bytes | Field | Values |
|-------|-------|--------|
| 0–3 | product magic | ASCII `YYDB` or `YYDS` |
| 4–7 | wire version | four ASCII digits: **`0000`** (current), then **`0001`**, … |

Version digits gate compatibility. **Not** HTTP JSON or SQL.

## YYDS vs reference YYDB serve

| | Reference `yydb serve` | YYDS product |
|---|------------------------|------------|
| Magic | `YYDB` | `YYDS` |
| Deployment | Loopback-first embedded | Distributed nodes |
| ACL on wire | None (trust = bind + filesystem) | Permissions / audit at product layer (not in `0000` frame set) |

Do not re-implement YYDS RBAC inside YYDB reference serve.

## Transport

| Mode | Endpoint | Notes |
|------|----------|-------|
| TCP | `host:port` | Native clients |
| WebSocket | `ws://host:port/wire` | Same port; browser clients |

WebSocket carries **identical binary frames** after HTTP upgrade.

## Frame layout (little-endian after 8-byte prefix)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | product magic `YYDB` \| `YYDS` |
| 4 | 4 | version digits `0000` \| `0001` \| … |
| 8 | 2 | `msg_type` |
| 10 | 2 | `flags` (reserved, `0` in `0000`) |
| 12 | 4 | `request_id` |
| 16 | 4 | `body_len` |
| 20 | N | `body` |

Max body: **16 MiB**. Unknown magic or unsupported version → reject.

## Message types (`0000`)

| Code | Name | Direction | Body |
|------|------|-----------|------|
| 1 | `Hello` | C→S | empty |
| 2 | `HelloOk` | S→C | UTF-8 server version |
| 3 | `Info` | C→S | empty |
| 4 | `InfoOk` | S→C | UTF-8 diagnostics |
| 5 | `SchemaGet` | C→S | empty |
| 6 | `SchemaGetOk` | S→C | `u8 present`; if 1: version + VOS doc |
| 7 | `SchemaEnsure` | C→S | `u32 version` + VOS doc (server uses document only) |
| 8 | `SchemaEnsureOk` | S→C | empty |
| 9 | `KvGet` | C→S | key |
| 10 | `KvGetOk` | S→C | optional value |
| 11 | `KvPut` | C→S | key + value |
| 12 | `KvPutOk` | S→C | empty |
| 13 | `MicroRegister` | C→S | session TS micro metadata |
| 14 | `MicroRegisterOk` | S→C | empty |
| 15 | `MicroHostInvoke` | S→C | host callback during another request |
| 16 | `MicroHostInvokeOk` | C→S | scalar result |
| 17 | `ScalarCall` | C→S | scalar UDF invoke |
| 18 | `ScalarCallOk` | S→C | scalar result |
| 255 | `Error` | S→C | UTF-8 message |

TCP clients must handle server-pushed `MicroHostInvoke` before their own response completes.

## Out of scope for `0000`

Full distributed catalog RPC, CAS object streaming, TLS as product surface, `0001` layout (when shipped).
