# @yyds/mysql

`mysql` is a MySQL classic-protocol client for a running YYDS MySQL listener. The listener is started and maintained by the `yyds` tool.

```bash
mysql -h 127.0.0.1 -P 3306 -u yyds -e "SET NAMES utf8mb4"
mysql -h 127.0.0.1 -P 3306 -u yyds -e "SELECT id, value FROM samples"

```

The client supports protocol-v10 startup, empty-password loopback sessions, optional database selection, `SET NAMES` acknowledgement, text-protocol result-set columns/rows and server error reporting for one command per invocation. Result values are printed as tab-separated text with `NULL` and control-character escaping. `-B`/`--batch` selects the tab-separated batch surface, `-N`/`--skip-column-names` omits the header, and `-r`/`--raw` disables value escaping. TLS, password authentication, binary result sets, interactive mode and full mysql-client compatibility are not implemented. Unsupported SQL returns the server's actual error instead of a fabricated result.

`mysqld` delegates to the `yyds` executable, including its shared Redis listener, and does not create a MySQL-only supervisor. Pass `--data-dir`, `--cluster-id`, and `--node-id`; set `YYDS_CLI_PATH` when the executable is not on `PATH`.

The package API re-exports `@yyds/yyds` unchanged.
