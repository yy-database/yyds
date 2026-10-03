# yyds-sqlite

SQLite **foreign passthrough** for YYDS, implemented in **pure Rust**.

On-disk `.sqlite` / `.db` files are a stable external binary contract. YYDS cannot replace that layout with `yyds-kv`, `.yyds` catalog bytes, or other YY-system optimizations on this path. This crate owns the format-3 container (header validation, blank database materialization, file roundtrip) without `rusqlite`, bundled `libsqlite3`, or other native SQLite bindings.

This is **not** a disguise wire gateway (`yyds-gateway-*`) and **not** a VOS executor.

Existing files are read-only main-file snapshots. Dropping a handle never writes. Blank files are created exclusively. File-backed `flush()` rejects writes until a transactional pager exists. Header validation is not an integrity check or a consistent view of an active WAL database. SQL execution, journal recovery, locking, and C ABI compatibility are not implemented.
