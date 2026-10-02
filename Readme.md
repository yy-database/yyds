YYDS
====

Rust monorepo for the YYDS scripting runtime.

## Layout

| Path | Role |
|------|------|
| `projects/crates/yyds-types` | Shared Rust types and core |
| `projects/crates/yyds-napi` | Node-API binding |
| `projects/crates/yyds-wasm` | Browser WebAssembly binding |
| `projects/packages/yyds` | TypeScript facade (`@yyds/yyds`) |
| `projects/packages/yyds-unknown-wasm32` | WASM artifacts for browsers |
| `projects/packages/yyds-*` | Platform Node-API binaries |
| `projects/packages/redis` | Redis disguise (`@yyds/redis`) |
| `projects/packages/mysql` | MySQL disguise (`@yyds/mysql`) |
| `projects/packages/postgresql` | PostgreSQL disguise (`@yyds/postgresql`) |
| `scripts/` | Workspace tooling (`format`, `build:napi`, `build:wasm`) |

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
