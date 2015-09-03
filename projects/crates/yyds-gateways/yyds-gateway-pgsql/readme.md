# yyds-gateway-pgsql

PostgreSQL gateway work for YYDS. `service::PgsqlService` attaches a loopback listener to an existing node owner and stops its client workers with the node lifecycle. It does not create or maintain a separate cluster. The wire connection path negotiates protocol v3, declines SSL/GSS encryption requests, completes unauthenticated startup, publishes standard session parameters, and rejects SQL with explicit SQLSTATE `0A000` until the YYDS SQL execution contract is connected. Bind it to loopback only. It is not yet a PostgreSQL-compatible database server.
