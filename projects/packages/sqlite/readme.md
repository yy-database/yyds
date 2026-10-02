# @yyds/sqlite

SQLite disguise for YYDS with **binary-compatible passthrough**.

On-disk `.sqlite` files must stay compatible with the format-3 container. YYDS implements that container in pure Rust (`yyds-sqlite`) and cannot replace it with `yyds-kv` or other YY-system optimizations on this path. The npm package mirrors the minimal `sqlite3` CLI surface for health checks and re-exports `@yyds/yyds`.

```bash
sqlite3 --version
# sqlite3 (YYDS 0.1.0)

sqlite3 :memory: "SELECT 1"
# 1
```
