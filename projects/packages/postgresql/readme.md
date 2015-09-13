# @yyds/postgresql

`psql` is a PostgreSQL protocol-v3 client for a running YYDS PostgreSQL listener. The listener is started and maintained by the `yyds` tool.

```bash
psql -h 127.0.0.1 -p 5432 -U yyds -d yyds -c "BEGIN"
```

The client supports unauthenticated protocol-v3 startup, text-format simple-query results and server errors for one command per invocation. It does not implement TLS, password authentication, binary results, interactive sessions or full psql compatibility. SQL execution is limited to the session commands implemented by the server. Unsupported queries return the server's actual SQLSTATE rather than a fabricated result.

The package API re-exports `@yyds/yyds` unchanged.
