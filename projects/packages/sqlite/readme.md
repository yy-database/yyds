# @yyds/sqlite

SQLite compatibility uses the upstream SQLite engine, embedded through `rusqlite`'s bundled C library. The independent pure-Rust format-3 snapshot reader remains available for bounded read-only inspection.

SQLite files are not YYDS databases and are not routed through YYDS clusters or KV storage. The Node entry offers `SqliteConnection` for real SQL execution and `SqliteSnapshot` for bounded low-level inspection. Both require a matching native addon built with the SQLite engine.

```ts
import { readFileSync } from "node:fs";
import { SqliteConnection, SqliteSnapshot } from "@yyds/sqlite/node";

const db = new SqliteConnection("app.sqlite");
db.execute("CREATE TABLE IF NOT EXISTS samples (value TEXT, payload BLOB)");
const result = db.execute("SELECT value, payload FROM samples");
const snapshot = new SqliteSnapshot(readFileSync("app.sqlite"));
const schema = snapshot.schema();
```

Snapshots copy the supplied bytes and never write to the source file. The default snapshot budget is 256 MiB, configurable through the constructor's second argument. Callers reading files must also bound their input allocation. Each scan accepts `maxPages`, `maxRows`, `maxPayloadBytes`, and `maxTotalPayloadBytes` resource limits.

Rows contain an exact signed 64-bit `bigint` rowid and a raw SQLite record `Buffer`, not SQL-projected values. Schema entries expose `kind`, `name`, `tableName`, optional `rootPage`, and optional `sql`. SQL definitions are opaque, not parsed by this reader.

`SqliteConnection.execute` executes one statement per call and returns rows. `executeWithParameters` binds typed SQLite values. `executeBatch` runs multiple statements through SQLite and discards result rows, matching `sqlite3_exec` without a row callback. Connections can be opened with `{ readOnly: true }`, which uses SQLite's read-only open flag and refuses missing files. All engine operations use upstream SQLite's pager, locks, transactions, journaling, WAL recovery and standard format compatibility. Result integers are exact `bigint`, BLOBs remain binary Buffers, and text is returned as JavaScript strings or raw bytes when invalid UTF-8. Async execution and exported SQLite C ABI are not exposed. `SqliteSnapshot` is a separate read-only main-file parser and does not include uncheckpointed WAL state or act as a live handle or whole-database integrity checker.

The default entry exports types only. Browser/OPFS support is not implemented, and the YYDS WASM engine is not a SQLite engine. `sqlite3 FILE SQL` runs one real SQLite statement. The shell remains intentionally partial: interactive sessions, shell options and broad dot-command compatibility are not implemented.

The `sqlite3` command executes SQL through the upstream engine. `sqlite3 -readonly FILE SQL` opens an existing database read-only. `.tables` and `.schema` use SQLite itself. SQL and dot commands cannot be mixed in one invocation. The command is not a complete replacement for the upstream interactive shell. Running the source package requires Node with TypeScript stripping enabled.

SQL execution uses SQLite's own locking and recovery behavior. Snapshot construction copies the provided bytes and remains independent of live database sidecars.
