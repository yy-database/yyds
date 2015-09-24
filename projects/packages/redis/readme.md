# @yyds/redis

`redis-cli` is a RESP2 client for a running YYDS Redis protocol service. The service lifecycle belongs to the `yyds` tool, which starts the node and its protocol listener.

```bash
redis-cli PING
# PONG

redis-cli -h 127.0.0.1 -p 6379 SET key value
redis-cli -h 127.0.0.1 -p 6379 SET key value NX
redis-cli -h 127.0.0.1 -p 6379 EXISTS key
redis-cli -h 127.0.0.1 -p 6379 GETDEL key
redis-cli -h 127.0.0.1 -p 6379 GETSET key replacement
redis-cli -h 127.0.0.1 -p 6379 INCR visits
redis-cli -h 127.0.0.1 -p 6379 INCRBY visits -2
redis-cli -h 127.0.0.1 -p 6379 DECRBY visits 2
redis-cli -n 1 GET key
```

The node-owned service supports binary `GET`, single-key `EXISTS`, atomic `GETDEL` and `GETSET`, `SET` and `SET ... NX`, single-key `DEL`, and signed 64-bit `INCR`/`INCRBY`/`DECR`/`DECRBY`. Conditional set, get-and-delete, get-and-replace and increments/decrements are atomic within the current single-process executor and persisted by its mounted shard. Missing integer keys start at zero. Multi-key `EXISTS` and other `SET` options remain unsupported. These operations do not imply replicated or multi-node atomicity. The client supports one RESP2 command per invocation, host/port/database selection, simple strings, errors, integers, bulk values and arrays. It does not provide authentication, TLS, RESP3, interactive mode or full `redis-cli` compatibility. Server lifecycle is provided by `yyds`, not a second `redis-server` manager.

`redis-server` delegates to the `yyds` executable and holds the node lifecycle. Pass `--data-dir`, `--cluster-id`, and `--node-id`; set `YYDS_CLI_PATH` when the executable is not on `PATH`. The alias does not create a Redis-only supervisor.

The package API re-exports `@yyds/yyds` unchanged.
