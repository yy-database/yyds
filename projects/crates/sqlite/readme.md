# yyds-sqlite

SQLite **foreign passthrough** for YYDS, implemented in **pure Rust**.

On-disk `.sqlite` / `.db` files are a stable external binary contract. YYDS cannot replace that layout with `yyds-kv`, `.yyds` catalog bytes, or other YY-system optimizations on this path. This crate owns the format-3 container (header validation, blank database materialization, file roundtrip) without `rusqlite`, bundled `libsqlite3`, or other native SQLite bindings.

This is **not** a disguise wire gateway (`yyds-gateway-*`) and **not** a VOS executor.

Existing files are read-only main-file snapshots. Dropping a handle never writes. Blank files are created exclusively. File-backed `flush()` rejects writes until a transactional pager exists. Header validation is not an integrity check or a consistent view of an active WAL database. SQL execution, journal recovery, locking, and C ABI compatibility are not implemented.

Record payload decoding supports SQLite varints, signed integers, binary64, NULL, BLOB, and Unicode text in UTF-8/UTF-16LE/UTF-16BE under explicit size and column limits. Non-Unicode text is explicitly unsupported rather than silently replaced.

`read_table(bytes, root_page, TableLimits)` scans rowid table interior/leaf pages in signed rowid order and assembles overflow records. It also reads the schema table at root page 1. Page references, ordered key ranges, cell bounds/overlaps, overflow termination, and aggregate page/row/payload limits are checked. Index b-trees and WITHOUT ROWID tables are explicitly unsupported. This is a bounded snapshot reader, not a complete integrity checker, schema resolver, or SQL engine.

Run the independent reference-engine check with `YYDS_SQLITE_REFERENCE_PYTHON` set to a Python executable providing `sqlite3`, then `cargo test -p yyds-sqlite --test reference -- --ignored`. It verifies original SQLite files, schema records, table traversal, overflow BLOBs and signed rowid extremes at 512/4096/65536-byte page sizes, plus original-engine acceptance of the generated blank file. The reference subprocess has a 20-second deadline. This does not prove SQL or transaction compatibility.
