"""Binary payload cases for the YYDS SQL gateways.

The regular matrix in `postgres_suite` and `mysql_suite` stays on the `users`
table. Raw octets need their own `payloads` table, so this suite starts from
`payload_schema.vos` and is selected with `run_all.py --suite payloads`.

The cases pin the two boundaries the gateways document:

* PostgreSQL: `BYTEA` parameters are accepted in the binary format
  (`psycopg.Binary`, or the default `%s`/`%b` binding of `psycopg3`). The text
  format cannot express arbitrary octets, so `psycopg3`'s `%t` placeholder is
  rejected with SQLSTATE `0A000`. `psycopg2` interpolates client-side and spells
  `bytes` as `'\\x…'::bytea`, which the gateway refuses with `0A000` because the
  `::` operator carries no node. The supported text spelling is
  `CAST('<hex>' AS BYTEA)`.
* MySQL: `COM_STMT_EXECUTE` carries `BLOB` parameters as raw octets, while the
  text protocol only reaches the same value through `CAST('<hex>' AS BLOB)`.
  `mysql-connector` types `bytes` parameters as `STRING`, so its prepared path
  is text and non-UTF-8 payloads are rejected with `1235`. Interpolating the
  same payload into `COM_QUERY` is rejected with `1300` because the command must
  be valid UTF-8.

Every driver owns a disjoint id block so the suite can share one node.
"""

from __future__ import annotations

from contextlib import contextmanager
from functools import partial

from harness import CaseResult, YydsNode, check, run_cases

try:
    import psycopg2
except ImportError:  # the suite reports a missing driver as a failure, never a skip
    psycopg2 = None

try:
    import psycopg
except ImportError:
    psycopg = None

try:
    import pymysql
except ImportError:
    pymysql = None

try:
    import mysql.connector as mysql
except ImportError:
    mysql = None

from pathlib import Path

SCHEMA_PATH = Path(__file__).with_name("payload_schema.vos")

# NUL, 0xFF, 0xFE, NUL, `A`, an invalid UTF-8 continuation pair.
PAYLOAD = bytes([0x00, 0xFF, 0xFE, 0x00, 0x41, 0xC3, 0x28])
HEX_PAYLOAD = PAYLOAD.hex().upper()
# `mysql-connector` binds `bytes` as `STRING`, so its prepared path only
# round trips payloads that are valid UTF-8. NUL is legal UTF-8.
TEXT_SAFE_PAYLOAD = b"\x00abc"
# One octet above `SqlFieldType::BYTES_CAPACITY` (`65535`).
OVERSIZE_HEX = "00" * 65_536

PG_DRIVER_BLOCKS = (("psycopg2", 300), ("psycopg3", 310))
MYSQL_DRIVER_BLOCKS = (("pymysql", 320), ("mysql-connector", 330))

PG_INSERT_CAST = "INSERT INTO payloads (id, blob) VALUES (%s, CAST(%s AS BYTEA))"
PG_INSERT_NULL = "INSERT INTO payloads (id, blob) VALUES (%s, NULL)"
PG_SELECT = "SELECT blob FROM payloads WHERE id = %s"
MYSQL_INSERT_CAST = "INSERT INTO payloads (id, blob) VALUES (%s, CAST(%s AS BLOB))"
MYSQL_INSERT_NULL = "INSERT INTO payloads (id, blob) VALUES (%s, NULL)"
MYSQL_SELECT = "SELECT blob FROM payloads WHERE id = %s"
BYTEA_OID = 17
BLOB_TYPE = 252


def _psycopg2_connect(node: YydsNode):
    connection = psycopg2.connect(host=node.host, port=node.pgsql_port, user="yyds", dbname="yyds")
    connection.autocommit = True
    return connection


def _psycopg3_connect(node: YydsNode):
    return psycopg.connect(host=node.host, port=node.pgsql_port, user="yyds", dbname="yyds", autocommit=True)


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
        collation="utf8mb4_general_ci",
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


def _octets(value) -> bytes:
    check(isinstance(value, (bytes, bytearray, memoryview)), f"binary column returned {type(value).__name__}")
    return bytes(value)


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


def _errno(error) -> int | None:
    code = getattr(error, "errno", None)
    if isinstance(code, int):
        return code
    if error.args and isinstance(error.args[0], int):
        return error.args[0]
    return None


def _expect_errno(connection, sql: str, errno: int, parameters=None, prepared: bool = False) -> None:
    try:
        cursor = connection.cursor(prepared=True) if prepared else connection.cursor()
        with cursor:
            cursor.execute(sql, parameters)
    except Exception as error:  # noqa: BLE001 - the errno is the assertion
        code = _errno(error)
        check(code == errno, f"expected error {errno}, got {code!r}: {type(error).__name__}: {error}")
        return
    raise AssertionError(f"expected error {errno}, but the statement succeeded")


def _write_pg(connection, driver: str, identifier: int, payload: bytes) -> None:
    """Writes one payload through the channel the driver can actually express."""

    with connection.cursor() as cursor:
        if driver == "psycopg3":
            # `psycopg.Binary` binds OID 17 in the binary format.
            cursor.execute("INSERT INTO payloads (id, blob) VALUES (%s, %s)", (identifier, psycopg.Binary(payload)))
        else:
            # `psycopg2` always interpolates client-side, so the only lossless
            # spelling is an explicit hexadecimal `CAST`.
            cursor.execute(PG_INSERT_CAST, (identifier, payload.hex().upper()))


def _case_pg_binary_roundtrip(node: YydsNode, connect, driver: str, base: int) -> None:
    identifier = base + 1
    with _session(node, connect) as connection:
        _write_pg(connection, driver, identifier, PAYLOAD)
        rows = _fetch(connection, PG_SELECT, (identifier,))
    check(len(rows) == 1, f"SELECT returned {len(rows)} rows, expected 1")
    check(_octets(rows[0][0]) == PAYLOAD, f"payload came back as {_octets(rows[0][0])!r}")


def _case_pg_empty_payload(node: YydsNode, connect, driver: str, base: int) -> None:
    identifier = base + 2
    with _session(node, connect) as connection:
        _write_pg(connection, driver, identifier, b"")
        rows = _fetch(connection, PG_SELECT, (identifier,))
    check(len(rows) == 1, f"SELECT returned {len(rows)} rows, expected 1")
    check(rows[0][0] is not None, "an empty payload must not collapse into NULL")
    check(_octets(rows[0][0]) == b"", f"empty payload came back as {_octets(rows[0][0])!r}")


def _case_pg_null_payload(node: YydsNode, connect, driver: str, base: int) -> None:
    identifier = base + 3
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute(PG_INSERT_NULL, (identifier,))
        rows = _fetch(connection, PG_SELECT, (identifier,))
    check(rows == [(None,)], f"NULL payload came back as {rows!r}")


def _case_pg_length_and_metadata(node: YydsNode, connect, driver: str, base: int) -> None:
    identifier = base + 4
    with _session(node, connect) as connection:
        _write_pg(connection, driver, identifier, PAYLOAD)
        with connection.cursor() as cursor:
            cursor.execute("SELECT blob, LENGTH(blob) FROM payloads WHERE id = %s", (identifier,))
            rows = cursor.fetchall()
            description = cursor.description
    check(len(rows) == 1, f"SELECT returned {len(rows)} rows, expected 1")
    check(description[0][1] == BYTEA_OID, f"binary column OID is {description[0][1]}, expected {BYTEA_OID}")
    check(_octets(rows[0][0]) == PAYLOAD, f"payload came back as {_octets(rows[0][0])!r}")
    check(rows[0][1] == len(PAYLOAD), f"LENGTH reported {rows[0][1]}, expected {len(PAYLOAD)}")


def _case_pg_text_format_rejected(node: YydsNode, connect, driver: str, base: int) -> None:
    identifier = base + 5
    # `psycopg3` asks for the text format with `%t`, while `psycopg2` always
    # interpolates client-side as `'\x…'::bytea`. Neither spelling can carry the
    # payload, so the gateway must refuse both with `0A000`.
    sql = (
        "INSERT INTO payloads (id, blob) VALUES (%s, %t)"
        if driver == "psycopg3"
        else "INSERT INTO payloads (id, blob) VALUES (%s, %s)"
    )
    with _session(node, connect) as connection:
        _expect_sqlstate(connection, sql, "0A000", (identifier, PAYLOAD))
        rows = _fetch(connection, PG_SELECT, (identifier,))
    check(rows == [], f"the rejected write must not store a row: {rows!r}")


def _case_pg_non_hex_literal(node: YydsNode, connect, driver: str, base: int) -> None:
    identifier = base + 6
    with _session(node, connect) as connection:
        _expect_sqlstate(connection, PG_INSERT_CAST, "42601", (identifier, "ZZ"))


def _case_pg_oversize_rejected(node: YydsNode, connect, driver: str, base: int) -> None:
    identifier = base + 7
    with _session(node, connect) as connection:
        _expect_sqlstate(connection, PG_INSERT_CAST, "42804", (identifier, OVERSIZE_HEX))


def _case_pg_binary_result_format(node: YydsNode, connect, driver: str, base: int) -> None:
    """`psycopg3` can also ask for binary result rows."""

    identifier = base + 8
    with _session(node, connect) as connection:
        _write_pg(connection, driver, identifier, PAYLOAD)
        with connection.cursor(binary=True) as cursor:
            cursor.execute(PG_SELECT, (identifier,))
            rows = cursor.fetchall()
    check(len(rows) == 1, f"binary SELECT returned {len(rows)} rows, expected 1")
    check(_octets(rows[0][0]) == PAYLOAD, f"binary row payload came back as {_octets(rows[0][0])!r}")


PG_CASES = [
    ("binary_roundtrip", _case_pg_binary_roundtrip),
    ("empty_payload", _case_pg_empty_payload),
    ("null_payload", _case_pg_null_payload),
    ("length_and_metadata", _case_pg_length_and_metadata),
    ("text_format_rejected", _case_pg_text_format_rejected),
    ("error_non_hex_literal", _case_pg_non_hex_literal),
    ("error_oversize_payload", _case_pg_oversize_rejected),
]

PG_EXTRA_CASES = {"psycopg3": [("binary_result_format", _case_pg_binary_result_format)]}


def _case_mysql_hex_cast_roundtrip(node: YydsNode, connect, base: int) -> None:
    identifier = base + 1
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute(MYSQL_INSERT_CAST, (identifier, HEX_PAYLOAD))
        rows = _fetch(connection, MYSQL_SELECT, (identifier,))
    check(len(rows) == 1, f"SELECT returned {len(rows)} rows, expected 1")
    check(_octets(rows[0][0]) == PAYLOAD, f"payload came back as {_octets(rows[0][0])!r}")


def _case_mysql_empty_payload(node: YydsNode, connect, base: int) -> None:
    identifier = base + 2
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute(MYSQL_INSERT_CAST, (identifier, ""))
        rows = _fetch(connection, MYSQL_SELECT, (identifier,))
    check(len(rows) == 1, f"SELECT returned {len(rows)} rows, expected 1")
    check(rows[0][0] is not None, "an empty payload must not collapse into NULL")
    check(_octets(rows[0][0]) == b"", f"empty payload came back as {_octets(rows[0][0])!r}")


def _case_mysql_null_payload(node: YydsNode, connect, base: int) -> None:
    identifier = base + 3
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute(MYSQL_INSERT_NULL, (identifier,))
        rows = _fetch(connection, MYSQL_SELECT, (identifier,))
    check(rows == [(None,)], f"NULL payload came back as {rows!r}")


def _case_mysql_length_and_metadata(node: YydsNode, connect, base: int) -> None:
    identifier = base + 4
    with _session(node, connect) as connection:
        with connection.cursor() as cursor:
            cursor.execute(MYSQL_INSERT_CAST, (identifier, HEX_PAYLOAD))
        with connection.cursor() as cursor:
            cursor.execute("SELECT blob, LENGTH(blob) FROM payloads WHERE id = %s", (identifier,))
            rows = list(cursor.fetchall())
            description = cursor.description
    check(len(rows) == 1, f"SELECT returned {len(rows)} rows, expected 1")
    check(description[0][1] == BLOB_TYPE, f"binary column type is {description[0][1]}, expected {BLOB_TYPE}")
    check(_octets(rows[0][0]) == PAYLOAD, f"payload came back as {_octets(rows[0][0])!r}")
    check(rows[0][1] == len(PAYLOAD), f"LENGTH reported {rows[0][1]}, expected {len(PAYLOAD)}")


def _case_mysql_non_hex_literal(node: YydsNode, connect, base: int) -> None:
    identifier = base + 5
    with _session(node, connect) as connection:
        _expect_errno(connection, MYSQL_INSERT_CAST, 1064, (identifier, "ZZ"))


def _case_mysql_oversize_rejected(node: YydsNode, connect, base: int) -> None:
    identifier = base + 6
    with _session(node, connect) as connection:
        _expect_errno(connection, MYSQL_INSERT_CAST, 1366, (identifier, OVERSIZE_HEX))


def _case_mysql_prepared_text_safe_roundtrip(node: YydsNode, connect, base: int) -> None:
    identifier = base + 7
    with _session(node, connect) as connection:
        with connection.cursor(prepared=True) as cursor:
            cursor.execute("INSERT INTO payloads (id, blob) VALUES (%s, %s)", (identifier, TEXT_SAFE_PAYLOAD))
        rows = _fetch(connection, MYSQL_SELECT, (identifier,))
    check(len(rows) == 1, f"SELECT returned {len(rows)} rows, expected 1")
    check(_octets(rows[0][0]) == TEXT_SAFE_PAYLOAD, f"prepared payload came back as {_octets(rows[0][0])!r}")


def _case_mysql_prepared_non_utf8_rejected(node: YydsNode, connect, base: int) -> None:
    identifier = base + 8
    with _session(node, connect) as connection:
        # `mysql-connector` types `bytes` as `STRING`, so the prepared path
        # decodes the parameter as text and refuses non-UTF-8 octets.
        _expect_errno(
            connection,
            "INSERT INTO payloads (id, blob) VALUES (%s, %s)",
            1235,
            (identifier, PAYLOAD),
            prepared=True,
        )
        rows = _fetch(connection, MYSQL_SELECT, (identifier,))
    check(rows == [], f"the rejected write must not store a row: {rows!r}")


def _case_mysql_text_non_utf8_rejected(node: YydsNode, connect, base: int) -> None:
    identifier = base + 9
    with _session(node, connect) as connection:
        # `COM_QUERY` carries one UTF-8 query string, so raw octets interpolated
        # into the text protocol are rejected before planning.
        _expect_errno(
            connection,
            "INSERT INTO payloads (id, blob) VALUES (%s, %s)",
            1300,
            (identifier, PAYLOAD),
        )
        rows = _fetch(connection, MYSQL_SELECT, (identifier,))
    check(rows == [], f"the rejected write must not store a row: {rows!r}")


MYSQL_CASES = [
    ("hex_cast_roundtrip", _case_mysql_hex_cast_roundtrip),
    ("empty_payload", _case_mysql_empty_payload),
    ("null_payload", _case_mysql_null_payload),
    ("length_and_metadata", _case_mysql_length_and_metadata),
    ("error_non_hex_literal", _case_mysql_non_hex_literal),
    ("error_oversize_payload", _case_mysql_oversize_rejected),
]

MYSQL_EXTRA_CASES = {
    "mysql-connector": [
        ("prepared_text_safe_roundtrip", _case_mysql_prepared_text_safe_roundtrip),
        ("prepared_non_utf8_rejected", _case_mysql_prepared_non_utf8_rejected),
        ("text_non_utf8_rejected", _case_mysql_text_non_utf8_rejected),
    ]
}


def run_binary_payloads(node: YydsNode) -> list[CaseResult]:
    """Runs every driver over the binary payload cases, failures keyed by driver."""

    pg_modules = {"psycopg2": psycopg2, "psycopg3": psycopg}
    pg_connectors = {"psycopg2": _psycopg2_connect, "psycopg3": _psycopg3_connect}
    mysql_modules = {"pymysql": pymysql, "mysql-connector": mysql}
    mysql_connectors = {"pymysql": _pymysql_connect, "mysql-connector": _connector_connect}

    results: list[CaseResult] = []
    for prefix, base in PG_DRIVER_BLOCKS:
        cases = [*PG_CASES, *PG_EXTRA_CASES.get(prefix, [])]
        if pg_modules[prefix] is None:
            results.extend(CaseResult(f"{prefix}/{name}", "fail", "driver not installed") for name, _ in cases)
            continue
        connect = pg_connectors[prefix]
        results.extend(run_cases(prefix, [(name, partial(case, node, connect, prefix, base)) for name, case in cases]))
    for prefix, base in MYSQL_DRIVER_BLOCKS:
        cases = [*MYSQL_CASES, *MYSQL_EXTRA_CASES.get(prefix, [])]
        if mysql_modules[prefix] is None:
            results.extend(CaseResult(f"{prefix}/{name}", "fail", "driver not installed") for name, _ in cases)
            continue
        connect = mysql_connectors[prefix]
        results.extend(run_cases(prefix, [(name, partial(case, node, connect, base)) for name, case in cases]))
    return results
