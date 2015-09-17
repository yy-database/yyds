# @yyds/redis

`redis-cli` is a RESP2 client for a running YYDS Redis protocol service. The service lifecycle belongs to the `yyds` tool, which starts the node and its protocol listener.

```bash
redis-cli PING
# PONG

redis-cli -h 127.0.0.1 -p 6379 SET key value
redis-cli -h 127.0.0.1 -p 6379 SET key value NX
redis-cli -h 127.0.0.1 -p 6379 INCR visits
redis-cli -h 127.0.0.1 -p 6379 INCRBY visits -2
redis-cli -n 1 GET key
```

The node-owned service supports binary `GET`, `SET` and `SET ... NX`, single-key `DEL`, and signed 64-bit `INCR`/`INCRBY`. Conditional set and increments are atomic within the current single-process executor and persisted by its mounted shard. Missing increment keys start at zero. Other `SET` options remain unsupported. These operations do not imply replicated or multi-node atomicity. The client supports one RESP2 command per invocation, host/port/database selection, simple strings, errors, integers, bulk values and arrays. It does not provide authentication, TLS, RESP3, interactive mode or full `redis-cli` compatibility. Server lifecycle is provided by `yyds`, not a second `redis-server` manager.

`redis-server` delegates to the `yyds` executable and holds the node lifecycle. Pass `--data-dir`, `--cluster-id`, and `--node-id`; set `YYDS_CLI_PATH` when the executable is not on `PATH`. The alias does not create a Redis-only supervisor.

The package API re-exports `@yyds/yyds` unchanged.
