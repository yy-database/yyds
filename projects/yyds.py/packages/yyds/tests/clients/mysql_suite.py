"""Real MySQL clients against the YYDS MySQL gateway.

`pymysql` escapes parameters on the client and sends one `COM_QUERY`, while
`mysql-connector-python` can additionally drive the server-side prepared
statement path through `COM_STMT_PREPARE` and `COM_STMT_EXECUTE`. Both drivers
run the shared case list, and the connector adds the prepared case on top.

The gateway publishes `utf8mb4_general_ci` as its only supported collation, so
the connector connects with that collation explicitly instead of its default
`utf8mb4_0900_ai_ci`.

Rows use the `users` table from `schema.vos`. `pymysql` owns the 200 id block
and the connector owns 210, so the PostgreSQL suite can share the same node.
"""

from __future__ import annotations

from contextlib import contextmanager
from functools import partial

from harness import CaseResult, YydsNode, check, run_cases

try:
    import pymysql
except ImportError:  # the matrix reports a missing driver as a failure, never a skip
    pymysql = None

try:
    import mysql.connector as mysql
except ImportError:
    mysql = None

DRIVER_BLOCKS = (("pymysql", 200), ("mysql-connector", 210))
INSERT_USER = "INSERT INTO users (id, name, age, score, active, tag) VALUES (%s, %s, %s, %s, %s, %s)"
COLLATION = "utf8mb4_general_ci"


def _pymysql_connect(node: YydsNode):
    return pymysql.connect(
        host=node.host,
        port=node.mysql_port,
        user="yyds",
        password="",
        charset="utf8mb4",
        autocommit=True,
    )


def _connector_connect(node: YydsNode):
    return mysql.connect(
        host=node.host,
        port=node.mysql_port,
        user="yyds",
        password="",
        collation=COLLATION,
        autocommit=True,
    )


@contextmanager
def _session(node: YydsNode, connect):
    connection = connect(node)
    try:
        yield connection
    finally:
        connection.close()


def _fetch(connection, sql: str, parameters=None) -> list[tuple]:
    with connection.cursor() as cursor:
        cursor.execute(sql, parameters)
        return list(cursor.fetchall())


def _errno(error) -> int | None:
    code = getattr(error, "errno", None)
    if isinstance(code, int):
        return code
    if error.args and isinstance(error.args[0], int):
        return error.args[0]
    return None


def _expect_errno(connection, sql: str, errno: int, parameters=None) -> None:
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, parameters)
    except Exception as error:  # noqa: BLE001 - the errno is the assertion
        code = _errno(error)
        check(code == errno, f"expected error {errno}, got {code!r}: {type(error).__name__}: {error}")
        return
    raise AssertionError(f"expected error {errno}, but the statement succeeded")


def _case_connect_and_select_literal(node: YydsNode, connect, base: int) -> None:
    with _session(node, connect) as connection:
        rows = _fetch(connection, "SELECT 1")
    check(rows == [(1,)], f"SELECT 1 returned {rows!r}")


def _case_result_types(node: YydsNode, connect, base: int) -> None:
    identifier = base + 1
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_USER, (identifier, "ada", 42, 1.5, True, None))
            cursor.execute("SELECT id, name, age, score, active, tag FROM users WHERE id = %s", (identifier,))
            rows = cursor.fetchall()
    check(len(rows) == 1, f"SELECT returned {len(rows)} rows, expected 1")
    row = rows[0]
    check(isinstance(row[0], int) and row[0] == identifier, f"integer column returned {row[0]!r}")
    check(row[1] == "ada", f"TEXT column returned {row[1]!r}")
    check(isinstance(row[2], int) and row[2] == 42, f"second integer column returned {row[2]!r}")
    check(isinstance(row[3], float) and row[3] == 1.5, f"REAL column returned {row[3]!r}")
    check(row[4] in (1, True), f"BOOLEAN column returned {row[4]!r}")
    check(row[5] is None, f"NULL column returned {row[5]!r}")


def _case_parameter_binding(node: YydsNode, connect, base: int) -> None:
    identifier = base + 2
    name = f"grace-{base}"
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_USER, (identifier, name, 36, 2.25, False, "vip"))
        rows = _fetch(connection, "SELECT name, active, tag FROM users WHERE name = %s", (name,))
    check(rows == [(name, 0, "vip")], f"bound text predicate returned {rows!r}")


def _case_transaction_commit(node: YydsNode, connect, base: int) -> None:
    identifier = base + 3
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute("BEGIN")
            cursor.execute(INSERT_USER, (identifier, "commit", 30, 3.5, True, None))
            cursor.execute("COMMIT")
        rows = _fetch(connection, "SELECT name FROM users WHERE id = %s", (identifier,))
    check(rows == [("commit",)], f"committed row is not visible: {rows!r}")


def _case_transaction_rollback(node: YydsNode, connect, base: int) -> None:
    identifier = base + 4
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute("BEGIN")
            cursor.execute(INSERT_USER, (identifier, "rollback", 31, 4.5, False, None))
            cursor.execute("ROLLBACK")
        rows = _fetch(connection, "SELECT name FROM users WHERE id = %s", (identifier,))
    check(rows == [], f"rolled back row is still visible: {rows!r}")


def _case_affected_rows(node: YydsNode, connect, base: int) -> None:
    identifier = base + 5
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_USER, (identifier, "rows", 20, 0.5, True, None))
            check(cursor.rowcount == 1, f"INSERT reported {cursor.rowcount} affected rows")
            cursor.execute("UPDATE users SET age = %s WHERE id = %s", (21, identifier))
            check(cursor.rowcount == 1, f"UPDATE reported {cursor.rowcount} affected rows")
            cursor.execute("UPDATE users SET age = %s WHERE id = %s", (22, identifier + 500))
            check(cursor.rowcount == 0, f"non-matching UPDATE reported {cursor.rowcount} affected rows")
            cursor.execute("DELETE FROM users WHERE id = %s", (identifier,))
            check(cursor.rowcount == 1, f"DELETE reported {cursor.rowcount} affected rows")
            cursor.execute("DELETE FROM users WHERE id = %s", (identifier,))
            check(cursor.rowcount == 0, f"second DELETE reported {cursor.rowcount} affected rows")


def _case_error_unknown_table(node: YydsNode, connect, base: int) -> None:
    with _session(node, connect) as connection:
        _expect_errno(connection, "SELECT id FROM missing_table", 1146)


def _case_error_unknown_column(node: YydsNode, connect, base: int) -> None:
    with _session(node, connect) as connection:
        _expect_errno(connection, "SELECT missing_column FROM users", 1054)


def _case_error_syntax(node: YydsNode, connect, base: int) -> None:
    with _session(node, connect) as connection:
        _expect_errno(connection, "SELEC 1", 1064)


def _case_error_unique_violation(node: YydsNode, connect, base: int) -> None:
    identifier = base + 6
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_USER, (identifier, "unique", 25, 1.0, True, None))
        _expect_errno(connection, INSERT_USER, 1062, (identifier, "unique-again", 26, 2.0, False, None))


def _case_reconnect(node: YydsNode, connect, base: int) -> None:
    identifier = base + 7
    first = connect(node)
    try:
        with first.cursor() as cursor:
            cursor.execute(INSERT_USER, (identifier, "reconnect", 27, 1.25, True, None))
    finally:
        first.close()
    with _session(node, connect) as connection:
        rows = _fetch(connection, "SELECT age FROM users WHERE id = %s", (identifier,))
    check(rows == [(27,)], f"row written before the disconnect is not readable after reconnecting: {rows!r}")


def _case_prepared_statement(node: YydsNode, connect, base: int) -> None:
    identifier = base + 8
    name = f"prepared-{base}"
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_USER, (identifier, name, 33, 3.75, True, None))
        with connection.cursor(prepared=True) as cursor:
            cursor.execute("SELECT name, score FROM users WHERE id = %s", (identifier,))
            rows = cursor.fetchall()
    check(len(rows) == 1, f"prepared SELECT returned {len(rows)} rows, expected 1")
    returned = rows[0][0]
    if isinstance(returned, (bytes, bytearray)):
        returned = bytes(returned).decode("utf-8")
    check(returned == name, f"prepared TEXT column returned {returned!r}")
    check(isinstance(rows[0][1], float) and rows[0][1] == 3.75, f"prepared REAL column returned {rows[0][1]!r}")


CASES = [
    ("connect_and_select_literal", _case_connect_and_select_literal),
    ("result_types", _case_result_types),
    ("parameter_binding", _case_parameter_binding),
    ("transaction_commit", _case_transaction_commit),
    ("transaction_rollback", _case_transaction_rollback),
    ("affected_rows", _case_affected_rows),
    ("error_unknown_table", _case_error_unknown_table),
    ("error_unknown_column", _case_error_unknown_column),
    ("error_syntax", _case_error_syntax),
    ("error_unique_violation", _case_error_unique_violation),
    ("reconnect", _case_reconnect),
]

EXTRA_CASES = {"mysql-connector": [("prepared_statement", _case_prepared_statement)]}


def run_mysql(node: YydsNode) -> list[CaseResult]:
    """Runs every MySQL driver against one node, failures keyed by driver."""

    modules = {"pymysql": pymysql, "mysql-connector": mysql}
    connectors = {"pymysql": _pymysql_connect, "mysql-connector": _connector_connect}
    results: list[CaseResult] = []
    for prefix, base in DRIVER_BLOCKS:
        cases = [*CASES, *EXTRA_CASES.get(prefix, [])]
        if modules[prefix] is None:
            results.extend(CaseResult(f"{prefix}/{name}", "fail", "driver not installed") for name, _ in cases)
            continue
        connect = connectors[prefix]
        results.extend(run_cases(prefix, [(name, partial(case, node, connect, base)) for name, case in cases]))
    return results
