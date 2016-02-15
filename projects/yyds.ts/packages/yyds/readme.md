# @yydb/yyds

Node.js facade for running a local YYDS node: native lifecycle blocks, a small CLI, and helpers used by disguise server entrypoints.

## Exports

| Subpath | Use |
|---------|-----|
| `@yydb/yyds` / `@yydb/yyds/node` | Load native bindings, start or inspect a node |
| `@yydb/yyds/node/cluster` | `launchYydsServer` for `redis-server`, `mysqld`, `postgres` |
| `@yydb/yyds/node/runtime` | `runNodeStart`, `parseStartOptions` |

The `yyds` binary provides `init`, `start`, `status`, and `stop`. Native addons for your platform are pulled in automatically as optional dependencies.

## Example

```ts
import { runNodeStart } from "@yydb/yyds/node/runtime";

await runNodeStart({
  dataDir: "./node-data",
  clusterId: "local",
  nodeId: "node-1",
  redisPort: 6379,
});
```

```bash
yyds init --data-dir ./node-data --cluster-id local --node-id node-1
yyds start --data-dir ./node-data --cluster-id local --node-id node-1
yyds status --data-dir ./node-data
yyds stop --data-dir ./node-data
```

## Related packages

- [`@yydb/redis`](https://github.com/yy-database/yyds/tree/dev/projects/yyds.ts/packages/redis) â€?Redis protocol disguise
- [`@yydb/mysql`](https://github.com/yy-database/yyds/tree/dev/projects/yyds.ts/packages/mysql) â€?MySQL protocol disguise
- [`@yydb/postgresql`](https://github.com/yy-database/yyds/tree/dev/projects/yyds.ts/packages/postgresql) â€?PostgreSQL protocol disguise
