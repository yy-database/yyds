YYDS
====

**Distributed VOS database** ? the network-facing sibling of embedded [YYDB](https://github.com/yy-database/yydb).

YYDS shares the **VOS schema contract** and CAS object semantics with YYDB, but its storage plane is **not** the
single-file `.yydb` layout. Versioned VOS documents live in `.yyds` catalog files and opaque shard records in `.yykv`
files. The smallest building block is **`yyds-kv`**.

## Layout

| Path | Role |
|------|------|
| `projects/yyds.rs/crates/yyds-kv` | Shard KV engine (`.yykv`) |
| `projects/yyds.rs/crates/yyds-catalog` | Catalog control plane (`.yyds`) |
| `projects/yyds.rs/crates/yyds-gateways/yyds-gateway` | Shared disguise gateway core |
| `projects/yyds.rs/crates/yyds-gateways/*` | Protocol disguise gateways (Redis, MySQL, PostgreSQL) |
| `projects/yyds.rs/crates/yyds-types` | Shared identity and error types |
| `projects/yyds.rs/ffis/yyds-napi` | Node-API binding (node lifecycle, wire, gateways) |
| `projects/yyds.ts/packages/yyds` | TypeScript facade (`@yydb/yyds`) |
| `projects/yyds.ts/platforms/yyds-*` | Platform Node-API binaries |
| `projects/yyds.ts/emulators/redis` | Redis disguise (`@yydb/redis`) |
| `projects/yyds.ts/emulators/mysql` | MySQL disguise (`@yydb/mysql`) |
| `projects/yyds.ts/emulators/postgresql` | PostgreSQL disguise (`@yydb/postgresql`) |
| `scripts/` | Workspace tooling (`format`, `build:napi`) |

## Storage model (vs YYDB)

| | YYDB | YYDS |
|---|------|------|
| Deployment | Embedded single process | Distributed nodes |
| Primary artifact | One `.yydb` file | `.yyds` catalog + many `.yykv` shards |
| Smallest engine crate | `yydb` | `yyds-kv` |
| Catalog crate | schema inside `.yydb` | `yyds-catalog` (`.yyds`) |
| Schema language | VOS (`vos-language` `dev`) | VOS (`vos-language` `dev`) |
| SQL | Never | Never |

## Learn more

- [Agent skills (`@yydb/yyds-skills`)](./projects/yyds.ts/packages/yyds-skills)
- [Embedded sibling YYDB](https://github.com/yy-database/yydb)
- [Repository and issue tracker](https://github.com/yy-database/yyds)
