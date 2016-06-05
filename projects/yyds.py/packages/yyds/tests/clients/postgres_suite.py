"""Real PostgreSQL clients against the YYDS PostgreSQL gateway.

`psycopg2` sends statements without parameters as SimpleQuery and statements
with parameters as Parse/Bind/Execute, while `psycopg3` only speaks the
extended protocol. Both paths are part of the gateway contract, so both
drivers run the same case list against one node.

Rows use the `users` table published by `schema.vos`. Each driver owns its own
id block, `psycopg2` at 100 and `psycopg3` at 110, so a shared node can also run
the MySQL suite at 200 and 210.
"""

from __future__ import annotations

from contextlib import contextmanager
from functools import partial

from harness import CaseResult, YydsNode, check, run_cases

try:
    import psycopg2
except ImportError:  # the matrix reports a missing driver as a failure, never a skip
    psycopg2 = None

try:
    import psycopg
except ImportError:
    psycopg = None

# Every driver owns a disjoint id block so two drivers can share one node.
DRIVER_BLOCKS = (("psycopg2", 100), ("psycopg3", 110))
INSERT_USER = "INSERT INTO users (id, name, age, score, active, tag) VALUES (%s, %s, %s, %s, %s, %s)"


def _psycopg2_connect(node: YydsNode):
    connection = psycopg2.connect(host=node.host, port=node.pgsql_port, user="yyds", dbname="yyds")
    connection.autocommit = True
    return connection


def _psycopg3_connect(node: YydsNode):
    return psycopg.connect(host=node.host, port=node.pgsql_port, user="yyds", dbname="yyds", autocommit=True)


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
        return cursor.fetchall()


def _expect_sqlstate(connection, sql: str, sqlstate: str, parameters=None) -> None:
    try:
        with connection.cursor() as cursor:
            cursor.execute(sql, parameters)
    except Exception as error:  # noqa: BLE001 - the SQLSTATE is the assertion
        state = getattr(error, "pgcode", None) or getattr(error, "sqlstate", None)
        check(
            state == sqlstate,
            f"expected SQLSTATE {sqlstate}, got {state!r} from {type(error).__name__}: {error}",
        )
        return
    raise AssertionError(f"expected SQLSTATE {sqlstate}, but the statement succeeded")


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
    check(row[4] is True, f"BOOLEAN column returned {row[4]!r}")
    check(row[5] is None, f"NULL column returned {row[5]!r}")


def _case_parameter_binding(node: YydsNode, connect, base: int) -> None:
    identifier = base + 2
    name = f"grace-{base}"
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_USER, (identifier, name, 36, 2.25, False, "vip"))
        rows = _fetch(connection, "SELECT name, active, tag FROM users WHERE name = %s", (name,))
    check(rows == [(name, False, "vip")], f"bound text predicate returned {rows!r}")


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
        _expect_sqlstate(connection, "SELECT id FROM missing_table", "42P01")


def _case_error_unknown_column(node: YydsNode, connect, base: int) -> None:
    with _session(node, connect) as connection:
        _expect_sqlstate(connection, "SELECT missing_column FROM users", "42703")


def _case_error_syntax(node: YydsNode, connect, base: int) -> None:
    with _session(node, connect) as connection:
        _expect_sqlstate(connection, "SELEC 1", "42601")


def _case_error_unique_violation(node: YydsNode, connect, base: int) -> None:
    identifier = base + 6
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute(INSERT_USER, (identifier, "unique", 25, 1.0, True, None))
        _expect_sqlstate(connection, INSERT_USER, "23505", (identifier, "unique-again", 26, 2.0, False, None))


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


def run_postgresql(node: YydsNode) -> list[CaseResult]:
    """Runs every PostgreSQL driver against one node, failures keyed by driver."""

    modules = {"psycopg2": psycopg2, "psycopg3": psycopg}
    connectors = {"psycopg2": _psycopg2_connect, "psycopg3": _psycopg3_connect}
    results: list[CaseResult] = []
    for prefix, base in DRIVER_BLOCKS:
        if modules[prefix] is None:
            results.extend(CaseResult(f"{prefix}/{name}", "fail", "driver not installed") for name, _ in CASES)
            continue
        connect = connectors[prefix]
        cases = [(name, partial(case, node, connect, base)) for name, case in CASES]
        results.extend(run_cases(prefix, cases))
    return results
