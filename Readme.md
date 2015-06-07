YYDS
====

**Distributed VOS database** — the network-facing sibling of embedded [YYDB](https://github.com/yy-database/yydb.rs).

YYDS shares the **VOS schema contract** and CAS object semantics with YYDB, but its storage plane is **not** the single-file `.yydb` layout. Versioned VOS documents live in `.yyds` catalog files and opaque shard records in `.yykv` files. The smallest building block is **`yyds-kv`**.

## Layout

| Path | Role |
|------|------|
| `projects/crates/yyds-kv` | Shard KV engine (`.yykv`) |
| `projects/crates/yyds-types` | Shared identity and error types |
| `projects/crates/yyds-napi` | Node-API binding |
| `projects/crates/yyds-wasm` | Browser WebAssembly binding |
| `projects/packages/yyds` | TypeScript facade (`@yyds/yyds`) |
| `projects/packages/yyds-unknown-wasm32` | WASM artifacts for browsers |
| `projects/packages/yyds-*` | Platform Node-API binaries |
| `projects/packages/redis` | Redis disguise (`@yyds/redis`) |
| `projects/packages/mysql` | MySQL disguise (`@yyds/mysql`) |
| `projects/packages/postgresql` | PostgreSQL disguise (`@yyds/postgresql`) |
| `scripts/` | Workspace tooling (`format`, `build:napi`, `build:wasm`) |

## Storage model (vs YYDB)

| | YYDB | YYDS |
|---|------|------|
| Deployment | Embedded single process | Distributed nodes |
| Primary artifact | One `.yydb` file | `.yyds` catalog + many `.yykv` shards |
| Smallest engine crate | `yydb` | `yyds-kv` |
| Schema language | VOS (`vos-language` `dev`) | VOS (`vos-language` `dev`) |
| SQL | Never | Never |

## Change the initial commit

```shell
git commit --amend --message "🎂 Project initialized!" --date "2012-12-12"
```

## Emoji Comment

| Emoji  | Meaning                      |  
|--------|------------------------------|  
| 🎂     | Project initialized!         |  
| 🎉     | Release new version          |  
| 🧪🔮   | Experimental code            |   
| 🔧🐛🐞 | Bug fix                      |  
| 🔒     | Security fix                 |  
| 🐣🐤🐥 | Add feature                  |  
| 📝🎀   | Documentation                |  
| 🚀     | Performance improve!         |  
| 🚧     | Work in progress             |  
| 🚨     | Test coverage improve!       |  
| 🚥     | CI improve!                  |  
| 🔥🧨   | Remove code or files         |
| 🧹     | Code refactor                |
| 📈     | Add analytics or branch code |
| 🤖     | Automation fix               |
| 📦     | Update dependencies          |
