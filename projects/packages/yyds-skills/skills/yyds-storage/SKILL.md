---
name: yyds-storage
description: >-
  YYDS distributed storage: `.yyds` catalog control plane and `.yykv` shard KV engine.
  Load when placing shards, replicating catalog truth, or comparing YYDS to embedded YYDB.
---

# YYDS storage model

YYDS is **not** the single-file `.yydb` embedded layout. The smallest engine crate is **`yyds-kv`**; catalog truth lives in **`yyds-catalog`**.

## Artifacts

| File | Crate | Role |
|------|-------|------|
| `.yyds` | `yyds-catalog` | Versioned VOS schema documents, shard registration, cluster catalog |
| `.yykv` | `yyds-kv` | Shard-local key/value records and placement |

## vs YYDB

| | YYDB | YYDS |
|---|------|------|
| Deployment | Embedded single process | Distributed nodes |
| Primary artifact | One `.yydb` file (+ wal/shm) | `.yyds` catalog + many `.yykv` shards |
| Schema home | Inside `.yydb` | `.yyds` catalog |
| SQL | Never | Never |
| Schema language | VOS | VOS (shared contract with YYDB) |

## `yyds-kv` responsibilities

- Shard placement and key encoding
- Record envelopes on the data plane
- **Does not** embed VOS typing inside individual KV records — typing is catalog-owned

Higher layers (catalog replication, VOS executor) build on `yyds-kv`.

## `yyds-catalog` responsibilities

- Cluster catalog truth shards execute under
- Versioned VOS documents and shard registration in `.yyds` files

## CAS / large objects

ObjectRef and chunk semantics align with the YY family CAS model. See `@yydb/yydb-skills` skill `yydb-bytes-storage` for historical CAS layout notes when comparing engines.

## SQLite passthrough

`yyds-sqlite` provides binary-compatible SQLite format-3 passthrough — not a YY-optimized store. Disguise package: `@yyds/sqlite`.
