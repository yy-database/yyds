"""Engine-parity probes with a real MySQL/PostgreSQL as the external truth source.

A driver-level assertion only proves that the gateway speaks the wire protocol
well enough for `mysql2`, `pg`, `PyMySQL` or `psycopg` to read an answer. It
does not prove the answer is what the real engine would have produced. This
runner closes that gap for the capabilities where the emulated server claims
engine behaviour:

1. run the probe against the external engine named by `YYDS_ORACLE_*`,
2. run the identical probe against the YYDS node under test,
3. compare the normalized outcome — returned rows, error number / SQLSTATE,
   `information_schema` projection, and `AUTO_INCREMENT` / `BIGSERIAL`
   continuation across connections.

Probes whose outcome differs are reported as `fail` with both sides recorded.
When no oracle endpoint is configured the status is `unverified`: the report
states that only driver-level evidence exists and nothing is counted as passed.

Oracle configuration (both variables of a pair are required to enable the
engine):

    YYDS_ORACLE_MYSQL_HOST / _PORT / _USER / _PASSWORD / _DATABASE
    YYDS_ORACLE_PG_HOST    / _PORT / _USER / _PASSWORD / _DATABASE

The probe creates and drops its own table, so the oracle account needs
`CREATE`/`DROP` on the target database.
"""

from __future__ import annotations

import os
import tempfile
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable

from harness import CaseResult, YydsNode, check, start_node

MYSQL_TABLE = "yyds_parity_mysql"
PG_TABLE = "yyds_parity_pg"
# The simulated node publishes one catalog for both gateways, so each engine
# needs its own table name. A real MySQL and a real PostgreSQL never share a
# database, making a shared probe name a harness artefact rather than a
# capability difference.

MYSQL_ENV = (
    "YYDS_ORACLE_MYSQL_HOST",
    "YYDS_ORACLE_MYSQL_PORT",
    "YYDS_ORACLE_MYSQL_USER",
    "YYDS_ORACLE_MYSQL_PASSWORD",
    "YYDS_ORACLE_MYSQL_DATABASE",
)
PG_ENV = (
    "YYDS_ORACLE_PG_HOST",
    "YYDS_ORACLE_PG_PORT",
    "YYDS_ORACLE_PG_USER",
    "YYDS_ORACLE_PG_PASSWORD",
    "YYDS_ORACLE_PG_DATABASE",
)

MYSQL_PROBES = (
    "select_literal",
    "create_table",
    "insert_autoincrement_first",
    "insert_autoincrement_second",
    "select_rows",
    "duplicate_key",
    "unknown_column",
    "unknown_table",
    "information_schema_columns",
    "information_schema_tables",
    "sequence_continues_on_new_connection",
)
PG_PROBES = (
    "select_literal",
    "create_table",
    "insert_bigserial_first",
    "insert_bigserial_second",
    "select_rows",
    "duplicate_key",
    "unknown_column",
    "unknown_table",
    "information_schema_columns",
    "information_schema_tables",
    "sequence_continues_on_new_connection",
)


@dataclass
class Oracle:
    """Connection details for one external engine."""

    host: str
    port: int
    user: str
    password: str
    database: str


def _oracle(names: tuple[str, ...]) -> Oracle | None:
    values = [os.environ.get(name, "").strip() for name in names]
    host, port, user, password, database = values
    if not host or not port:
        return None
    return Oracle(host=host, port=int(port), user=user, password=password, database=database)


def _normalize(value: Any) -> Any:
    """Makes driver-specific cell types comparable across the two servers."""

    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", "replace")
    if isinstance(value, Decimal):
        return float(value)
    if isinstance(value, memoryview):
        return bytes(value).decode("utf-8", "replace")
    return value


def _rows(cursor) -> list[tuple]:
    return [tuple(_normalize(cell) for cell in row) for row in cursor.fetchall()]


def _mysql_outcome(connection, sql: str, *, fetch: bool) -> tuple:
    """Runs one statement and returns `(kind, payload)` without raising."""

    with connection.cursor() as cursor:
        try:
            cursor.execute(sql)
            if fetch:
                return ("rows", _rows(cursor))
            return ("ok", cursor.rowcount)
        except Exception as error:  # noqa: BLE001 - the error code is the assertion
            errno = getattr(error, "errno", None)
            if not isinstance(errno, int) and error.args and isinstance(error.args[0], int):
                errno = error.args[0]
            return ("error", errno, getattr(error, "sqlstate", None))


def _pg_outcome(connection, sql: str, *, fetch: bool) -> tuple:
    with connection.cursor() as cursor:
        try:
            cursor.execute(sql)
            if fetch:
                return ("rows", _rows(cursor))
            return ("ok", cursor.rowcount)
        except Exception as error:  # noqa: BLE001 - the SQLSTATE is the assertion
            return ("error", getattr(error, "sqlstate", None))


def _mysql_probe_results(connect: Callable[[], Any]) -> dict[str, tuple]:
    """Runs the MySQL probe list against whatever endpoint `connect` opens."""

    table = MYSQL_TABLE
    results: dict[str, tuple] = {}
    connection = connect()
    try:
        _mysql_outcome(connection, f"DROP TABLE IF EXISTS {table}", fetch=False)
        results["select_literal"] = _mysql_outcome(connection, "SELECT 1", fetch=True)
        results["create_table"] = _mysql_outcome(
            connection,
            f"CREATE TABLE {table} (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(16) NOT NULL, score DOUBLE NULL) "
            "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            fetch=False,
        )
        with connection.cursor() as cursor:
            results["insert_autoincrement_first"] = _mysql_insert(cursor, f"INSERT INTO {table} (name) VALUES ('a')")
            results["insert_autoincrement_second"] = _mysql_insert(cursor, f"INSERT INTO {table} (name) VALUES ('b')")
        results["select_rows"] = _mysql_outcome(connection, f"SELECT id, name, score FROM {table} ORDER BY id", fetch=True)
        results["duplicate_key"] = _mysql_outcome(connection, f"INSERT INTO {table} (id, name) VALUES (1, 'dup')", fetch=False)
        results["unknown_column"] = _mysql_outcome(connection, f"SELECT missing_column FROM {table}", fetch=True)
        results["unknown_table"] = _mysql_outcome(connection, "SELECT id FROM yyds_parity_missing_table", fetch=True)
        results["information_schema_columns"] = _mysql_outcome(
            connection,
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY FROM information_schema.COLUMNS "
            f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}' ORDER BY ORDINAL_POSITION",
            fetch=True,
        )
        results["information_schema_tables"] = _mysql_outcome(
            connection,
            "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{table}'",
            fetch=True,
        )
    finally:
        connection.close()

    # A second connection stands in for the pool reconnect path: the
    # AUTO_INCREMENT counter is table state, so it must not restart at 1.
    continuation = connect()
    try:
        with continuation.cursor() as cursor:
            results["sequence_continues_on_new_connection"] = _mysql_insert(cursor, f"INSERT INTO {table} (name) VALUES ('c')")
        _mysql_outcome(continuation, f"DROP TABLE IF EXISTS {table}", fetch=False)
    finally:
        continuation.close()
    return results


def _mysql_insert(cursor, sql: str) -> tuple:
    try:
        cursor.execute(sql)
        return ("insert", getattr(cursor, "lastrowid", None))
    except Exception as error:  # noqa: BLE001 - the error code is the assertion
        errno = getattr(error, "errno", None)
        if not isinstance(errno, int) and error.args and isinstance(error.args[0], int):
            errno = error.args[0]
        return ("error", errno, getattr(error, "sqlstate", None))


def _pg_probe_results(connect: Callable[[], Any]) -> dict[str, tuple]:
    """Runs the PostgreSQL probe list against whatever endpoint `connect` opens."""

    table = PG_TABLE
    results: dict[str, tuple] = {}
    connection = connect()
    try:
        _pg_outcome(connection, f"DROP TABLE IF EXISTS {table}", fetch=False)
        results["select_literal"] = _pg_outcome(connection, "SELECT 1", fetch=True)
        results["create_table"] = _pg_outcome(
            connection,
            f"CREATE TABLE {table} (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, score DOUBLE PRECISION NULL)",
            fetch=False,
        )
        results["insert_bigserial_first"] = _pg_outcome(
            connection,
            f"INSERT INTO {table} (name) VALUES ('a') RETURNING id",
            fetch=True,
        )
        results["insert_bigserial_second"] = _pg_outcome(
            connection,
            f"INSERT INTO {table} (name) VALUES ('b') RETURNING id",
            fetch=True,
        )
        results["select_rows"] = _pg_outcome(connection, f"SELECT id, name, score FROM {table} ORDER BY id", fetch=True)
        results["duplicate_key"] = _pg_outcome(connection, f"INSERT INTO {table} (id, name) VALUES (1, 'dup')", fetch=False)
        results["unknown_column"] = _pg_outcome(connection, f"SELECT missing_column FROM {table}", fetch=True)
        results["unknown_table"] = _pg_outcome(connection, "SELECT id FROM yyds_parity_missing_table", fetch=True)
        results["information_schema_columns"] = _pg_outcome(
            connection,
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            f"WHERE table_schema = 'public' AND table_name = '{table}' ORDER BY ordinal_position",
            fetch=True,
        )
        results["information_schema_tables"] = _pg_outcome(
            connection,
            "SELECT table_name, table_type FROM information_schema.tables "
            f"WHERE table_schema = 'public' AND table_name = '{table}'",
            fetch=True,
        )
    finally:
        connection.close()

    continuation = connect()
    try:
        results["sequence_continues_on_new_connection"] = _pg_outcome(
            continuation,
            f"INSERT INTO {table} (name) VALUES ('c') RETURNING id",
            fetch=True,
        )
        _pg_outcome(continuation, f"DROP TABLE IF EXISTS {table}", fetch=False)
    finally:
        continuation.close()
    return results


def _mysqldb_connect(host: str, port: int, user: str, password: str, database: str) -> Callable[[], Any]:
    def connect():
        import mysql.connector as mysql

        return mysql.connect(
            host=host,
            port=port,
            user=user,
            password=password,
            database=database or None,
            autocommit=True,
            collation="utf8mb4_general_ci",
        )

    return connect


def _pg_connect(host: str, port: int, user: str, password: str, database: str) -> Callable[[], Any]:
    def connect():
        import psycopg

        return psycopg.connect(host=host, port=port, user=user, password=password, dbname=database or None, autocommit=True)

    return connect


def _compare(engine: str, real: dict[str, tuple], simulated: dict[str, tuple]) -> list[CaseResult]:
    results: list[CaseResult] = []
    for name, expected in real.items():
        actual = simulated.get(name)
        label = f"{engine}/oracle/{name}"
        if expected == actual:
            results.append(CaseResult(label, "pass", f"engine and gateway both returned {expected!r}"))
        else:
            results.append(CaseResult(label, "fail", f"engine returned {expected!r}, gateway returned {actual!r}"))
    return results


def _unverified(engine: str, probes: tuple[str, ...], reason: str) -> list[CaseResult]:
    return [CaseResult(f"{engine}/oracle/{name}", "unverified", reason) for name in probes]


def run_parity(node: YydsNode) -> list[CaseResult]:
    """Compares the emulated gateways against the configured external engines."""

    results: list[CaseResult] = []

    mysql_oracle = _oracle(MYSQL_ENV)
    if mysql_oracle is None:
        results.extend(_unverified("mysql", MYSQL_PROBES, "no oracle configured: only driver-level evidence, missing engine parity"))
    else:
        real = _mysql_probe_results(_mysqldb_connect(mysql_oracle.host, mysql_oracle.port, mysql_oracle.user, mysql_oracle.password, mysql_oracle.database))
        simulated = _mysql_probe_results(_mysqldb_connect(node.host, node.mysql_port, "yyds", "", ""))
        results.extend(_compare("mysql", real, simulated))

    pg_oracle = _oracle(PG_ENV)
    if pg_oracle is None:
        results.extend(_unverified("postgresql", PG_PROBES, "no oracle configured: only driver-level evidence, missing engine parity"))
    else:
        real = _pg_probe_results(_pg_connect(pg_oracle.host, pg_oracle.port, pg_oracle.user, pg_oracle.password, pg_oracle.database))
        # The simulated node publishes a single catalog for both SQL gateways.
        # A real MySQL and a real PostgreSQL never share a database, so the
        # PostgreSQL probes get their own node instead of colliding with MySQL's
        # table and row namespace.
        pg_node = start_node()
        try:
            simulated = _pg_probe_results(_pg_connect(pg_node.host, pg_node.pgsql_port, "yyds", "", "yyds"))
        finally:
            pg_node.stop()
        results.extend(_compare("postgresql", real, simulated))

    for engine in ("mysql", "postgresql"):
        results.append(_restart_sequence_probe(engine, use_catalog=True))
        results.append(_restart_sequence_probe(engine, use_catalog=False))
    return results


def _restart_sequence_probe(engine: str, *, use_catalog: bool) -> CaseResult:
    """Checks `AUTO_INCREMENT` / `BIGSERIAL` continues after a node process restart.

    The counter must live in durable storage: two inserts on the first process,
    one insert after a restart on the same data directory, and the third value
    must be 3 rather than restarting at 1. Both engines keep the table and the
    counter across a real restart, so this is engine behaviour, not an
    interpretation.

    `use_catalog` runs the node the way every other probe does. When it is
    false the node owns a persisted schema registry instead, which separates
    "the counter is not durable" from "catalog-derived DDL is not persisted".
    """

    data_dir = Path(tempfile.mkdtemp(prefix=f"yyds-{engine}-restart-"))
    cluster_id = f"restart-cluster-{os.getpid()}"
    node_id = f"restart-node-{os.getpid()}"
    label = f"{engine}/oracle/sequence_survives_node_restart"
    if not use_catalog:
        label += "_local_registry"
    mysql = engine == "mysql"
    table = "yyds_restart_mysql" if mysql else "yyds_restart_pg"

    def open_connection(node: YydsNode):
        if mysql:
            return _mysqldb_connect("127.0.0.1", node.mysql_port, "yyds", "", "")(), None
        return _pg_connect("127.0.0.1", node.pgsql_port, "yyds", "", "yyds")(), None

    def insert(connection) -> Any:
        if mysql:
            with connection.cursor() as cursor:
                cursor.execute(f"INSERT INTO {table} (name) VALUES ('x')")
                return cursor.lastrowid
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {table} (name) VALUES ('x') RETURNING id")
            return cursor.fetchone()[0]

    node = start_node(data_dir=data_dir, cluster_id=cluster_id, node_id=node_id, use_catalog=use_catalog)
    try:
        connection, _ = open_connection(node)
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"DROP TABLE IF EXISTS {table}")
                cursor.execute(
                    f"CREATE TABLE {table} (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(16) NOT NULL)"
                    if mysql
                    else f"CREATE TABLE {table} (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL)"
                )
            first = insert(connection)
            second = insert(connection)
        finally:
            connection.close()
        node.stop(wipe=False)
        node = start_node(data_dir=data_dir, cluster_id=cluster_id, node_id=node_id, use_catalog=use_catalog)
        connection, _ = open_connection(node)
        try:
            third = insert(connection)
        finally:
            connection.close()
    except Exception as error:  # noqa: BLE001 - the observed values are the assertion
        return CaseResult(label, "fail", f"restart probe raised {type(error).__name__}: {error}")
    finally:
        try:
            node.stop()
        except Exception:  # noqa: BLE001 - an already stopped node still leaves results
            pass
    if (first, second, third) == (1, 2, 3):
        return CaseResult(label, "pass", "ids before restart 1,2 and after restart 3")
    return CaseResult(label, "fail", f"ids were {first!r}, {second!r}, {third!r}; expected 1,2,3")


def assert_parity(node: YydsNode) -> None:
    """Fails when any configured engine diverges from the gateway."""

    divergences = [result for result in run_parity(node) if result.status == "fail"]
    check(not divergences, f"{len(divergences)} engine-parity divergences")
