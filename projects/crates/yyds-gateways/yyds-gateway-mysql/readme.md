# yyds-gateway-mysql

MySQL protocol gateway work for YYDS. `service::MysqlService` attaches a loopback protocol-v10 listener to an existing node owner. It accepts protocol-4.1 clients using an empty password, rejects non-empty passwords, and returns an explicit unsupported error for every SQL query. SQL parsing must be supplied by Oak and execution must bind to YYDS. The current PyMySQL reference probe disables PyMySQL's implicit `SET NAMES` command to isolate protocol startup. This is not a MySQL-compatible database server. The listener does not create or maintain a separate cluster.
