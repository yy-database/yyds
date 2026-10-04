# yyds-sqlite

SQLite compatibility crate for YYDS. SQL execution is provided by upstream SQLite via `rusqlite` with the bundled C library. A separate pure-Rust reader provides bounded format-3 snapshot inspection.

On-disk `.sqlite` / `.db` files use the upstream SQLite format and engine behavior. This path is independent of `yyds-kv`, `.yyds` catalog storage, and YYDS cluster execution. `SqliteEngine` opens in-memory and file databases, runs one statement per call, and inherits SQLite's pager, file locking, transactions, journal and WAL recovery. The N-API layer preserves signed 64-bit integers and BLOB bytes. It does not expose SQLite's C ABI or bind parameters.

This is **not** a disguise wire gateway (`yyds-gateway-*`) and **not** a VOS executor.

`SqliteDatabase` and `SqliteSnapshot` are read-only format inspection APIs. They do not recover WALs or represent a live connection. Use `SqliteEngine` for SQL and normal SQLite file semantics. Engine operations must pass through upstream SQLite and must not be emulated by writing container bytes directly.

Record payload decoding supports SQLite varints, signed integers, binary64, NULL, BLOB, and Unicode text in UTF-8/UTF-16LE/UTF-16BE under explicit size and column limits. Non-Unicode text is explicitly unsupported rather than silently replaced.

`read_table(bytes, root_page, TableLimits)` scans rowid table interior/leaf pages in signed rowid order and assembles overflow records. It also reads the schema table at root page 1. Page references, ordered key ranges, cell bounds/overlaps, overflow termination, and aggregate page/row/payload limits are checked. Index b-trees and WITHOUT ROWID tables are explicitly unsupported. This is a bounded snapshot reader, not a complete integrity checker or SQL engine.

`read_schema` returns typed table/index/view/trigger objects, preserving names, root pages and opaque CREATE source (including NULL SQL for automatic indexes). `read_named_table` and `SqliteDatabase::table` resolve ordinary rowid tables by ASCII-insensitive stored names. The main-file schema aliases `sqlite_schema` and `sqlite_master` are supported. Views and virtual tables are not executed. Limits apply to each schema/table scan separately. These APIs return raw records, not SQL projections or expanded INTEGER PRIMARY KEY values. CREATE source remains unparsed and belongs to the Oak SQL frontend when semantic resolution is implemented.

Run the independent format-reader oracle with `YYDS_SQLITE_REFERENCE_PYTHON` set to Python with `sqlite3`, then `cargo test -p yyds-sqlite --test reference -- --ignored`. It covers original files, page sizes, encodings, schema and row traversal. SQL compatibility is independently exercised by executing through the bundled engine and checking the created database from Python's original SQLite. This does not prove complete sqlite3 shell or C ABI compatibility.
