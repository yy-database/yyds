# @yyds/sqlite

Independent SQLite format-3 snapshot reader implemented in pure Rust.

SQLite files are not YYDS databases and are not routed through YYDS clusters or KV storage. The Node entry calls the independent `yyds-sqlite` native reader. It requires a matching native addon with snapshot support.

```ts
import { readFileSync } from "node:fs";
import { SqliteSnapshot } from "@yyds/sqlite/node";

const snapshot = new SqliteSnapshot(readFileSync("app.sqlite"));
const objects = snapshot.schema();
const rows = snapshot.table("samples");
```

Snapshots copy the supplied bytes and never write to the source file. The default snapshot budget is 256 MiB, configurable through the constructor's second argument. Callers reading files must also bound their input allocation. Each scan accepts `maxPages`, `maxRows`, `maxPayloadBytes`, and `maxTotalPayloadBytes` resource limits.

Rows contain an exact signed 64-bit `bigint` rowid and a raw SQLite record `Buffer`, not SQL-projected values. Schema entries expose `kind`, `name`, `tableName`, optional `rootPage`, and optional `sql`. SQL definitions are opaque, not parsed by this reader.

Only rowid-table scans are supported. Index scans, WITHOUT ROWID tables, virtual tables, view execution, SQL execution, pager locking, transactions, journal/WAL recovery, writes, and SQLite C ABI compatibility are not implemented. A main-file snapshot does not include uncheckpointed WAL changes and is not a live database connection or an integrity checker.

The default entry exports types only. Browser/OPFS support is not implemented, and the YYDS WASM engine is not a SQLite engine.

The `sqlite3` command supports only `sqlite3 FILE '.tables'` and `sqlite3 FILE '.schema'`, reading real schema records. `.tables` prints one name per line, not the original shell's column layout. SQL, interactive input, command patterns, shell options, and `:memory:` are rejected. The version banner identifies the snapshot tool, not a SQLite engine version. Running the source package requires Node with TypeScript stripping enabled.

CLI reads are bounded before allocation, reject nonempty WAL/journal sidecars, and detect ordinary file changes during reading. These checks do not implement SQLite locking: use an offline, checkpointed copy, not a concurrently modified database. The source file is never created or changed.
