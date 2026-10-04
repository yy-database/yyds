# @yyds/mysql

`mysql` is a MySQL classic-protocol client for a running YYDS MySQL listener. The listener is started and maintained by the `yyds` tool.

```bash
mysql -h 127.0.0.1 -P 3306 -u yyds -e "SET NAMES utf8mb4"

```

The client supports protocol-v10 startup, empty-password loopback sessions, optional database selection, `SET NAMES` acknowledgement and server error reporting for one command per invocation. Result-set rows, TLS, password authentication, interactive mode and full mysql-client compatibility are not implemented. Unsupported SQL returns the server's actual error instead of a fabricated result.

`mysqld` delegates to the `yyds` executable, including its shared Redis listener, and does not create a MySQL-only supervisor. Pass `--data-dir`, `--cluster-id`, and `--node-id`; set `YYDS_CLI_PATH` when the executable is not on `PATH`.

The package API re-exports `@yyds/yyds` unchanged.
