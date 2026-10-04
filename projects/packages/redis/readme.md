# @yyds/redis

`redis-cli` is a RESP2 client for a running YYDS Redis protocol service. The service lifecycle belongs to the `yyds` tool, which starts the node and its protocol listener.

```bash
redis-cli PING
# PONG

redis-cli -h 127.0.0.1 -p 6379 SET key value
redis-cli -n 1 GET key
```

The client currently supports one RESP2 command per invocation, host/port/database selection, simple strings, errors, integers, bulk values and arrays. It does not provide authentication, TLS, RESP3, interactive mode or full `redis-cli` compatibility. Server lifecycle is provided by `yyds`, not a second `redis-server` manager.

The package API re-exports `@yyds/yyds` unchanged.
