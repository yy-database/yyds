"""Real PostgreSQL (psycopg 3) and MySQL (PyMySQL) drivers against a yyds node.

Mirrors `smoke.ts`: same cases, same assertions, same table names. The node is
started in schema-registry mode by `run.mjs`, so this script owns its DDL and
leaves `<data-dir>/sql-schema.v0.json` behind for the harness to inspect.

Run through `node projects/yyds.ts/packages/yyds-harness/run.mjs`, or by hand with `YYDS_PG_PORT` and
`YYDS_MYSQL_PORT` pointing at an already running node.
"""

from __future__ import annotations

import os
import sys
from typing import Callable

import psycopg
import pymysql

HOST = os.environ.get("YYDS_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("YYDS_PG_PORT", "0"))
MYSQL_PORT = int(os.environ.get("YYDS_MYSQL_PORT", "0"))
TAG = os.environ.get("YYDS_SMOKE_TAG", "py")
PG_TABLE = f"smoke_pg_users_{TAG}"
MYSQL_TABLE = f"smoke_mysql_users_{TAG}"

if PG_PORT <= 0 or MYSQL_PORT <= 0:
    print("YYDS_PG_PORT and YYDS_MYSQL_PORT must point at a running yyds node", file=sys.stderr)
    raise SystemExit(2)


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def pg_connect() -> psycopg.Connection:
    return psycopg.connect(
        host=HOST,
        port=PG_PORT,
        user="yyds",
        password="",
        dbname="yyds",
        autocommit=True,
    )


def mysql_connect() -> pymysql.connections.Connection:
    return pymysql.connect(
        host=HOST,
        port=MYSQL_PORT,
        user="yyds",
        password="",
        charset="utf8mb4",
        autocommit=True,
    )


def case_connect_and_select_pg() -> None:
    with pg_connect() as connection:
        row = connection.execute("SELECT 1").fetchone()
        check(row[0] == 1, f"SELECT 1 returned {row}")


def case_create_table_pg() -> None:
    with pg_connect() as connection:
        connection.execute(
            f"CREATE TABLE {PG_TABLE} (id BIGSERIAL PRIMARY KEY, email TEXT NOT NULL, note TEXT, UNIQUE (email))"
        )


def case_insert_select_pg() -> None:
    with pg_connect() as connection:
        connection.execute(f"INSERT INTO {PG_TABLE} (email, note) VALUES ('ada', 'first')")
        rows = connection.execute(f"SELECT id, note FROM {PG_TABLE} WHERE email = 'ada'").fetchall()
        check(len(rows) == 1, f"SELECT returned {len(rows)} rows, expected 1")
        check(int(rows[0][0]) > 0, f"BIGSERIAL assigned {rows[0][0]}")
        check(rows[0][1] == "first", f"note column returned {rows[0][1]}")


def case_update_pg() -> None:
    with pg_connect() as connection:
        result = connection.execute(f"UPDATE {PG_TABLE} SET note = 'updated' WHERE email = 'ada'")
        check(result.rowcount == 1, f"UPDATE reported {result.rowcount} rows")
        rows = connection.execute(f"SELECT note FROM {PG_TABLE} WHERE email = 'ada'").fetchall()
        check(rows[0][0] == "updated", f"updated note returned {rows[0][0]}")


def case_delete_pg() -> None:
    with pg_connect() as connection:
        connection.execute(f"INSERT INTO {PG_TABLE} (email, note) VALUES ('delete-me', 'temp')")
        result = connection.execute(f"DELETE FROM {PG_TABLE} WHERE email = 'delete-me'")
        check(result.rowcount == 1, f"DELETE reported {result.rowcount} rows")
        rows = connection.execute(f"SELECT note FROM {PG_TABLE} WHERE email = 'delete-me'").fetchall()
        check(len(rows) == 0, f"deleted row still visible: {rows}")


def case_transaction_commit_pg() -> None:
    writer = pg_connect()
    reader = pg_connect()
    try:
        writer.execute("BEGIN")
        writer.execute(f"INSERT INTO {PG_TABLE} (email, note) VALUES ('tx-commit', 'kept')")
        writer.execute("COMMIT")
        rows = reader.execute(f"SELECT note FROM {PG_TABLE} WHERE email = 'tx-commit'").fetchall()
        check(len(rows) == 1 and rows[0][0] == "kept", f"committed row returned {rows}")
    finally:
        writer.close()
        reader.close()


def case_transaction_rollback_pg() -> None:
    with pg_connect() as connection:
        connection.execute("BEGIN")
        connection.execute(f"INSERT INTO {PG_TABLE} (email, note) VALUES ('tx-rollback', 'discarded')")
        connection.execute("ROLLBACK")
        rows = connection.execute(f"SELECT note FROM {PG_TABLE} WHERE email = 'tx-rollback'").fetchall()
        check(len(rows) == 0, f"rolled back row still visible: {rows}")


def case_unique_insert_pg() -> None:
    with pg_connect() as connection:
        sqlstate = None
        try:
            connection.execute(f"INSERT INTO {PG_TABLE} (email, note) VALUES ('ada', 'duplicate')")
        except psycopg.Error as error:
            sqlstate = error.sqlstate
        check(sqlstate == "23505", f"duplicate INSERT returned {sqlstate}, expected 23505")
        rows = connection.execute(f"SELECT note FROM {PG_TABLE} WHERE email = 'ada'").fetchall()
        check(len(rows) == 1 and rows[0][0] == "updated", f"duplicate INSERT changed the row set: {rows}")


def case_unique_update_pg() -> None:
    with pg_connect() as connection:
        connection.execute(f"INSERT INTO {PG_TABLE} (email, note) VALUES ('bob', 'bob')")
        sqlstate = None
        try:
            connection.execute(f"UPDATE {PG_TABLE} SET email = 'ada' WHERE email = 'bob'")
        except psycopg.Error as error:
            sqlstate = error.sqlstate
        check(sqlstate == "23505", f"duplicate UPDATE returned {sqlstate}, expected 23505")
        rows = connection.execute(f"SELECT email FROM {PG_TABLE} WHERE note = 'bob'").fetchall()
        check(len(rows) == 1 and rows[0][0] == "bob", f"rejected UPDATE changed the row: {rows}")


def case_bigserial_continues_after_reconnect_pg() -> None:
    connection = pg_connect()
    first: list[int] = []
    try:
        connection.execute(f"INSERT INTO {PG_TABLE} (email, note) VALUES ('serial-a', 'a')")
        connection.execute(f"INSERT INTO {PG_TABLE} (email, note) VALUES ('serial-b', 'b')")
        rows = connection.execute(
            f"SELECT id FROM {PG_TABLE} WHERE note IN ('a', 'b') ORDER BY id"
        ).fetchall()
        first.extend(int(row[0]) for row in rows)
    finally:
        connection.close()
    check(len(first) == 2 and first[1] == first[0] + 1, f"consecutive BIGSERIAL values were {first}")
    connection = pg_connect()
    try:
        connection.execute(f"INSERT INTO {PG_TABLE} (email, note) VALUES ('serial-c', 'c')")
        row = connection.execute(f"SELECT id FROM {PG_TABLE} WHERE email = 'serial-c'").fetchone()
        check(int(row[0]) == first[1] + 1, f"BIGSERIAL after reconnect was {row[0]}, expected {first[1] + 1}")
    finally:
        connection.close()


def case_reconnect_reads_persisted_rows_pg() -> None:
    connection = pg_connect()
    try:
        connection.execute(f"INSERT INTO {PG_TABLE} (email, note) VALUES ('persist', 'kept')")
    finally:
        connection.close()
    connection = pg_connect()
    try:
        rows = connection.execute(f"SELECT note FROM {PG_TABLE} WHERE email = 'persist'").fetchall()
        check(len(rows) == 1 and rows[0][0] == "kept", f"row written before the disconnect is gone: {rows}")
    finally:
        connection.close()


def case_connect_and_select_mysql() -> None:
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT 1")
            check(cursor.fetchone()[0] == 1, "SELECT 1 must return 1")
    finally:
        connection.close()


def case_create_table_mysql() -> None:
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(
                f"CREATE TABLE {MYSQL_TABLE} (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
                "email VARCHAR(191) NOT NULL, note TEXT, UNIQUE KEY uq_smoke_email (email))"
            )
    finally:
        connection.close()


def case_insert_select_mysql() -> None:
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES ('ada', 'first')")
            cursor.execute(f"SELECT id, note FROM {MYSQL_TABLE} WHERE email = 'ada'")
            rows = cursor.fetchall()
            check(len(rows) == 1, f"SELECT returned {len(rows)} rows, expected 1")
            check(int(rows[0][0]) > 0, f"AUTO_INCREMENT assigned {rows[0][0]}")
            check(rows[0][1] == "first", f"note column returned {rows[0][1]}")
    finally:
        connection.close()


def case_prepared_select_mysql() -> None:
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT note FROM {MYSQL_TABLE} WHERE email = %s", ("ada",))
            rows = cursor.fetchall()
            check(len(rows) == 1 and rows[0][0] == "first", f"prepared SELECT returned {rows}")
    finally:
        connection.close()


def case_update_mysql() -> None:
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"UPDATE {MYSQL_TABLE} SET note = 'updated' WHERE email = 'ada'")
            check(cursor.rowcount == 1, f"UPDATE reported {cursor.rowcount} rows")
            cursor.execute(f"SELECT note FROM {MYSQL_TABLE} WHERE email = 'ada'")
            check(cursor.fetchone()[0] == "updated", "updated note mismatch")
    finally:
        connection.close()


def case_delete_mysql() -> None:
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES ('delete-me', 'temp')")
            cursor.execute(f"DELETE FROM {MYSQL_TABLE} WHERE email = 'delete-me'")
            check(cursor.rowcount == 1, f"DELETE reported {cursor.rowcount} rows")
            cursor.execute(f"SELECT note FROM {MYSQL_TABLE} WHERE email = 'delete-me'")
            check(len(cursor.fetchall()) == 0, "deleted row still visible")
    finally:
        connection.close()


def case_transaction_commit_mysql() -> None:
    writer = mysql_connect()
    reader = mysql_connect()
    try:
        with writer.cursor() as cursor:
            cursor.execute("BEGIN")
            cursor.execute(f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES ('tx-commit', 'kept')")
            cursor.execute("COMMIT")
        with reader.cursor() as cursor:
            cursor.execute(f"SELECT note FROM {MYSQL_TABLE} WHERE email = 'tx-commit'")
            rows = cursor.fetchall()
            check(len(rows) == 1 and rows[0][0] == "kept", f"committed row returned {rows}")
    finally:
        writer.close()
        reader.close()


def case_transaction_rollback_mysql() -> None:
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute("BEGIN")
            cursor.execute(f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES ('tx-rollback', 'discarded')")
            cursor.execute("ROLLBACK")
            cursor.execute(f"SELECT note FROM {MYSQL_TABLE} WHERE email = 'tx-rollback'")
            check(len(cursor.fetchall()) == 0, "rolled back row still visible")
    finally:
        connection.close()


def case_unique_insert_mysql() -> None:
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            errno = None
            try:
                cursor.execute(f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES ('ada', 'duplicate')")
            except pymysql.err.MySQLError as error:
                errno = error.args[0]
            check(errno == 1062, f"duplicate INSERT returned {errno}, expected 1062")
            cursor.execute(f"SELECT note FROM {MYSQL_TABLE} WHERE email = 'ada'")
            rows = cursor.fetchall()
            check(len(rows) == 1 and rows[0][0] == "updated", f"duplicate INSERT changed the row set: {rows}")
    finally:
        connection.close()


def case_unique_update_mysql() -> None:
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES ('bob', 'bob')")
            errno = None
            try:
                cursor.execute(f"UPDATE {MYSQL_TABLE} SET email = 'ada' WHERE email = 'bob'")
            except pymysql.err.MySQLError as error:
                errno = error.args[0]
            check(errno == 1062, f"duplicate UPDATE returned {errno}, expected 1062")
            cursor.execute(f"SELECT email FROM {MYSQL_TABLE} WHERE note = 'bob'")
            rows = cursor.fetchall()
            check(len(rows) == 1 and rows[0][0] == "bob", f"rejected UPDATE changed the row: {rows}")
    finally:
        connection.close()


def case_auto_increment_continues_after_reconnect_mysql() -> None:
    connection = mysql_connect()
    first: list[int] = []
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES ('serial-a', 'a')")
            cursor.execute(f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES ('serial-b', 'b')")
            cursor.execute(f"SELECT id FROM {MYSQL_TABLE} WHERE note IN ('a', 'b') ORDER BY id")
            first.extend(int(row[0]) for row in cursor.fetchall())
    finally:
        connection.close()
    check(len(first) == 2 and first[1] == first[0] + 1, f"consecutive AUTO_INCREMENT values were {first}")
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES ('serial-c', 'c')")
            cursor.execute(f"SELECT id FROM {MYSQL_TABLE} WHERE email = 'serial-c'")
            row = cursor.fetchone()
            check(int(row[0]) == first[1] + 1, f"AUTO_INCREMENT after reconnect was {row[0]}, expected {first[1] + 1}")
    finally:
        connection.close()


def case_reconnect_reads_persisted_rows_mysql() -> None:
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES ('persist', 'kept')")
    finally:
        connection.close()
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT note FROM {MYSQL_TABLE} WHERE email = 'persist'")
            rows = cursor.fetchall()
            check(len(rows) == 1 and rows[0][0] == "kept", f"row written before the disconnect is gone: {rows}")
    finally:
        connection.close()


PG_CASES: list[tuple[str, Callable[[], None]]] = [
    ("connect_and_select", case_connect_and_select_pg),
    ("create_table", case_create_table_pg),
    ("insert_select", case_insert_select_pg),
    ("update", case_update_pg),
    ("delete", case_delete_pg),
    ("transaction_commit", case_transaction_commit_pg),
    ("transaction_rollback", case_transaction_rollback_pg),
    ("unique_insert", case_unique_insert_pg),
    ("unique_update", case_unique_update_pg),
    ("bigserial_continues_after_reconnect", case_bigserial_continues_after_reconnect_pg),
    ("reconnect_reads_persisted_rows", case_reconnect_reads_persisted_rows_pg),
]

MYSQL_CASES: list[tuple[str, Callable[[], None]]] = [
    ("connect_and_select", case_connect_and_select_mysql),
    ("create_table", case_create_table_mysql),
    ("insert_select", case_insert_select_mysql),
    ("prepared_select", case_prepared_select_mysql),
    ("update", case_update_mysql),
    ("delete", case_delete_mysql),
    ("transaction_commit", case_transaction_commit_mysql),
    ("transaction_rollback", case_transaction_rollback_mysql),
    ("unique_insert", case_unique_insert_mysql),
    ("unique_update", case_unique_update_mysql),
    ("auto_increment_continues_after_reconnect", case_auto_increment_continues_after_reconnect_mysql),
    ("reconnect_reads_persisted_rows", case_reconnect_reads_persisted_rows_mysql),
]


def main() -> int:
    passed = 0
    failures: list[str] = []
    for driver, cases in (("postgres", PG_CASES), ("mysql", MYSQL_CASES)):
        for name, case in cases:
            try:
                case()
            except Exception as error:  # noqa: BLE001 - the smoke reports any driver failure
                message = f"{driver}/{name}: {type(error).__name__}: {error}"
                failures.append(message)
                print(f"FAIL {message}")
            else:
                passed += 1
                print(f"PASS {driver}/{name}")
    total = len(PG_CASES) + len(MYSQL_CASES)
    print(f"Python smoke {passed}/{total} passed, {len(failures)} failed")
    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
