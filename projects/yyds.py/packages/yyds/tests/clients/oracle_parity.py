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
from dataclasses import dataclass
from decimal import Decimal
from typing import Any, Callable

from harness import CaseResult, YydsNode, check

TABLE = "yyds_parity_probe"

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

    results: dict[str, tuple] = {}
    connection = connect()
    try:
        _mysql_outcome(connection, f"DROP TABLE IF EXISTS {TABLE}", fetch=False)
        results["select_literal"] = _mysql_outcome(connection, "SELECT 1", fetch=True)
        results["create_table"] = _mysql_outcome(
            connection,
            f"CREATE TABLE {TABLE} (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(16) NOT NULL, score DOUBLE NULL) "
            "ENGINE=InnoDB DEFAULT CHARSET=utf8mb4",
            fetch=False,
        )
        with connection.cursor() as cursor:
            results["insert_autoincrement_first"] = _mysql_insert(cursor, f"INSERT INTO {TABLE} (name) VALUES ('a')")
            results["insert_autoincrement_second"] = _mysql_insert(cursor, f"INSERT INTO {TABLE} (name) VALUES ('b')")
        results["select_rows"] = _mysql_outcome(connection, f"SELECT id, name, score FROM {TABLE} ORDER BY id", fetch=True)
        results["duplicate_key"] = _mysql_outcome(connection, f"INSERT INTO {TABLE} (id, name) VALUES (1, 'dup')", fetch=False)
        results["unknown_column"] = _mysql_outcome(connection, f"SELECT missing_column FROM {TABLE}", fetch=True)
        results["unknown_table"] = _mysql_outcome(connection, "SELECT id FROM yyds_parity_missing_table", fetch=True)
        results["information_schema_columns"] = _mysql_outcome(
            connection,
            "SELECT COLUMN_NAME, DATA_TYPE, IS_NULLABLE, COLUMN_KEY FROM information_schema.COLUMNS "
            f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{TABLE}' ORDER BY ORDINAL_POSITION",
            fetch=True,
        )
        results["information_schema_tables"] = _mysql_outcome(
            connection,
            "SELECT TABLE_NAME, TABLE_TYPE FROM information_schema.TABLES "
            f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = '{TABLE}'",
            fetch=True,
        )
    finally:
        connection.close()

    # A second connection stands in for the pool reconnect path: the
    # AUTO_INCREMENT counter is table state, so it must not restart at 1.
    continuation = connect()
    try:
        with continuation.cursor() as cursor:
            results["sequence_continues_on_new_connection"] = _mysql_insert(cursor, f"INSERT INTO {TABLE} (name) VALUES ('c')")
        _mysql_outcome(continuation, f"DROP TABLE IF EXISTS {TABLE}", fetch=False)
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

    results: dict[str, tuple] = {}
    connection = connect()
    try:
        _pg_outcome(connection, f"DROP TABLE IF EXISTS {TABLE}", fetch=False)
        results["select_literal"] = _pg_outcome(connection, "SELECT 1", fetch=True)
        results["create_table"] = _pg_outcome(
            connection,
            f"CREATE TABLE {TABLE} (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, score DOUBLE PRECISION NULL)",
            fetch=False,
        )
        results["insert_bigserial_first"] = _pg_outcome(
            connection,
            f"INSERT INTO {TABLE} (name) VALUES ('a') RETURNING id",
            fetch=True,
        )
        results["insert_bigserial_second"] = _pg_outcome(
            connection,
            f"INSERT INTO {TABLE} (name) VALUES ('b') RETURNING id",
            fetch=True,
        )
        results["select_rows"] = _pg_outcome(connection, f"SELECT id, name, score FROM {TABLE} ORDER BY id", fetch=True)
        results["duplicate_key"] = _pg_outcome(connection, f"INSERT INTO {TABLE} (id, name) VALUES (1, 'dup')", fetch=False)
        results["unknown_column"] = _pg_outcome(connection, f"SELECT missing_column FROM {TABLE}", fetch=True)
        results["unknown_table"] = _pg_outcome(connection, "SELECT id FROM yyds_parity_missing_table", fetch=True)
        results["information_schema_columns"] = _pg_outcome(
            connection,
            "SELECT column_name, data_type, is_nullable FROM information_schema.columns "
            f"WHERE table_schema = 'public' AND table_name = '{TABLE}' ORDER BY ordinal_position",
            fetch=True,
        )
        results["information_schema_tables"] = _pg_outcome(
            connection,
            "SELECT table_name, table_type FROM information_schema.tables "
            f"WHERE table_schema = 'public' AND table_name = '{TABLE}'",
            fetch=True,
        )
    finally:
        connection.close()

    continuation = connect()
    try:
        results["sequence_continues_on_new_connection"] = _pg_outcome(
            continuation,
            f"INSERT INTO {TABLE} (name) VALUES ('c') RETURNING id",
            fetch=True,
        )
        _pg_outcome(continuation, f"DROP TABLE IF EXISTS {TABLE}", fetch=False)
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
        simulated = _pg_probe_results(_pg_connect(node.host, node.pgsql_port, "yyds", "", "yyds"))
        results.extend(_compare("postgresql", real, simulated))

    return results


def assert_parity(node: YydsNode) -> None:
    """Fails when any configured engine diverges from the gateway."""

    divergences = [result for result in run_parity(node) if result.status == "fail"]
    check(not divergences, f"{len(divergences)} engine-parity divergences")
