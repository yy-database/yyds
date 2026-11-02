# Embed and serve

| Role                        | Package                                                                | Notes                                                                          |
|-----------------------------|------------------------------------------------------------------------|--------------------------------------------------------------------------------|
| YYDB in-process embedding   | [`yydb`](https://crates.io/crates/yydb) (Rust)                         | Rust hosts only, UDF                                                           |
| YYDB Rust remote client     | [`yydb-client`](https://crates.io/crates/yydb-client)                  | TCP → `yyds start`                                                             |
| **TS product package (recommended)** | [`@yydb/yyds`](https://www.npmjs.com/package/@yydb/yydb)      | **One dependency**: `Database.open("app.yydb")`, engine binaries managed automatically |
| TS wire protocol            | [`@yydb/yydb-client`](https://www.npmjs.com/package/@yydb/yydb-client) | WebUI / browser / advanced usage                                               |

TypeScript application developers should depend on **`@yydb/yyds`** and never download standalone engine binaries or hand-roll a `serve` themselves. Platform `.node` bindings install with npm through `optionalDependencies`, and the CLI ships with `@yydb/yyds`.

```ts
import { Database } from "@yydb/yydb";

const db = await Database.open("app.yydb");
await db.ensureSchema(`table Setting {
    @@id: uuid,
    key: utf8,
    value: utf8,
}`);
await db.put("k", "v");
await db.close();
```

Underneath it is still the same Rust kernel (a private loopback process), not a second JS storage implementation. The `YYDB`/`YYDS` frame magic is equivalent self-identification. Use YYDS when you need the heavy built-in capabilities such as authorization and audit.
