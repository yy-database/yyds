---
name: yyds-gateways
description: >-
  YYDS protocol disguise gateways: Redis, MySQL, PostgreSQL, SQLite shims over @yyds/yyds.
  Load when packaging or operating wire-compatible gateway entrypoints.
---

# YYDS disguise gateways

YYDS ships **disguise** npm packages that install like familiar databases but run the YYDS runtime underneath. Shared gateway core: `projects/crates/yyds-gateways/yyds-gateway`.

## Packages

| npm | Disguise | Crate path |
|-----|----------|------------|
| `@yyds/redis` | Redis protocol | `projects/packages/redis` |
| `@yyds/mysql` | MySQL protocol | `projects/packages/mysql` |
| `@yyds/postgresql` | PostgreSQL protocol | `projects/packages/postgresql` |
| `@yyds/sqlite` | SQLite binary-compatible passthrough | `projects/packages/sqlite` |

Each disguise package **re-exports `@yyds/yyds`** — it does not fork the TypeScript facade.

## Example (`@yyds/redis`)

```bash
redis-cli PING
# PONG

redis-server
# [disguise] redis-server 0.1.0 ready on port 6379 (YYDS)
```

## Boundaries

- Disguise is a **protocol surface**, not SQL translation into VOS
- Gateway shims must not invent parallel schema or storage contracts
- Build native/WASM artifacts from repo root: `pnpm build:napi`, `pnpm build:wasm`
