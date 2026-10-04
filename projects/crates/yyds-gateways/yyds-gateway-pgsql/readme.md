# yyds-gateway-pgsql

PostgreSQL gateway work for YYDS. The wire connection path negotiates protocol v3, declines SSL/GSS encryption requests, completes unauthenticated startup, publishes standard session parameters, and rejects SQL with explicit SQLSTATE `0A000` until the YYDS SQL execution contract is connected. Bind it to loopback only. It is not yet a PostgreSQL-compatible database server.
