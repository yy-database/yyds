# Binary protocol summary

The transport is **YY binary frames**, not JSON REST and not SQL. The DDL/query language side is **VOS**.

- Prefix: product magic **`YYDB` | `YYDS`** (the backend introducing itself, identical in effect, the frontend does not insist on either) plus a four-digit version **`0000`** (evolving to **`0001`**, ...)
- Header: `msg_type` / `flags` / `request_id` / `body_len` (little-endian)
- Transports: TCP, or WebSocket `/wire` (identical frame bodies)

The frontend speaks the protocol through **`@yydb/yydb-client`** (and this site / the WebUI) and never probes whether the backend is YYDB or YYDS. The package is named `yydb` only because this repository hosts the reference implementation.

Rust:

- [`yydb`](https://crates.io/crates/yydb): YYDB embedding facade
- [`yydb-client`](https://crates.io/crates/yydb-client): YYDB remote TCP client

Full reference (Agent skill): [`yydb-serve-protocol`](https://github.com/yy-database/yyds/tree/dev/projects/yydb.ts/packages/yydb-skills/skills/yydb-serve-protocol/SKILL.md) ([`@yydb/yyds-skills`](https://www.npmjs.com/package/@yydb/yyds-skills)).

Current frame types: Hello, Info, SchemaGet / SchemaEnsure, KvGet / KvPut (see the `msg_type` table in the skill).

A local demo lives on the site `/wire` page.
