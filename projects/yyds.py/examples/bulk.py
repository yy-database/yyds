"""下游验收示例：真实 PyMySQL / psycopg 驱动对 yyds 节点做 1000 行批写与重连后批读。

期望值由本示例按写入输入自行推导（行数、连续 id 区间、去重键数、抽样行），
不引用 yyds 内部常量，也不复用仓内用例表。节点由 `node e2e/run.mjs` 启动。
"""

from __future__ import annotations

import os
import sys

import psycopg
import pymysql

ROWS = 1000
CHUNK = 100
HOST = os.environ.get("YYDS_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("YYDS_PG_PORT", "0"))
MYSQL_PORT = int(os.environ.get("YYDS_MYSQL_PORT", "0"))
PG_TABLE = "example_bulk_pg_py"
MYSQL_TABLE = "example_bulk_mysql_py"

if PG_PORT <= 0 or MYSQL_PORT <= 0:
    print("YYDS_PG_PORT and YYDS_MYSQL_PORT must point at a running yyds node", file=sys.stderr)
    raise SystemExit(2)

EMAILS = [f"row-{index}@example.test" for index in range(1, ROWS + 1)]
NOTES = [f"note-{index}" for index in range(1, ROWS + 1)]


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def pg_connect() -> psycopg.Connection:
    return psycopg.connect(host=HOST, port=PG_PORT, user="yyds", password="", dbname="yyds", autocommit=True)


def mysql_connect() -> pymysql.connections.Connection:
    return pymysql.connect(host=HOST, port=MYSQL_PORT, user="yyds", password="", charset="utf8mb4", autocommit=True)


def bulk_pg() -> None:
    with pg_connect() as connection:
        connection.execute(f"DROP TABLE IF EXISTS {PG_TABLE}")
        connection.execute(
            f"CREATE TABLE {PG_TABLE} (id BIGSERIAL PRIMARY KEY, email TEXT NOT NULL, note TEXT, UNIQUE (email))"
        )
        for index, (email, note) in enumerate(zip(EMAILS, NOTES), start=1):
            # 显式主键：示例只验收批写批读，不绑定网关的序列分配路径。
            connection.execute(
                f"INSERT INTO {PG_TABLE} (id, email, note) VALUES (%s, %s, %s)", (index, email, note)
            )
        row = connection.execute(
            f"SELECT count(*), min(id), max(id) FROM {PG_TABLE}"
        ).fetchone()
        check(int(row[0]) == ROWS, f"postgres stored {row[0]} rows, expected {ROWS}")
        check(int(row[2]) - int(row[1]) + 1 == ROWS, f"postgres id range {row[1]}..{row[2]} is not contiguous")
        distinct = connection.execute(f"SELECT count(DISTINCT email) FROM {PG_TABLE}").fetchone()[0]
        check(int(distinct) == ROWS, f"postgres counted {distinct} distinct emails")
        spot = connection.execute(
            f"SELECT note FROM {PG_TABLE} WHERE email = %s", ("row-500@example.test",)
        ).fetchall()
        check(len(spot) == 1 and spot[0][0] == "note-500", f"postgres spot row was {spot}")

    with pg_connect() as reopened:
        total = reopened.execute(f"SELECT count(*) FROM {PG_TABLE}").fetchone()[0]
        check(int(total) == ROWS, f"postgres read {total} rows after reconnect, expected {ROWS}")


def bulk_mysql() -> None:
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute(f"DROP TABLE IF EXISTS {MYSQL_TABLE}")
            cursor.execute(
                f"CREATE TABLE {MYSQL_TABLE} (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
                "email VARCHAR(191) NOT NULL, note TEXT, UNIQUE KEY uq_example_email (email))"
            )
            for start in range(0, ROWS, CHUNK):
                end = min(start + CHUNK, ROWS)
                cursor.executemany(
                    f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES (%s, %s)",
                    list(zip(EMAILS[start:end], NOTES[start:end])),
                )
            cursor.execute(f"SELECT count(*), min(id), max(id) FROM {MYSQL_TABLE}")
            row = cursor.fetchone()
            check(int(row[0]) == ROWS, f"mysql stored {row[0]} rows, expected {ROWS}")
            check(int(row[2]) - int(row[1]) + 1 == ROWS, "mysql id range is not contiguous")
            cursor.execute(f"SELECT count(DISTINCT email) FROM {MYSQL_TABLE}")
            check(int(cursor.fetchone()[0]) == ROWS, "mysql distinct email count mismatch")
            cursor.execute(f"SELECT note FROM {MYSQL_TABLE} WHERE email = %s", ("row-500@example.test",))
            spot = cursor.fetchall()
            check(len(spot) == 1 and spot[0][0] == "note-500", f"mysql spot row was {spot}")
    finally:
        connection.close()

    reopened = mysql_connect()
    try:
        with reopened.cursor() as cursor:
            cursor.execute(f"SELECT count(*) FROM {MYSQL_TABLE}")
            check(int(cursor.fetchone()[0]) == ROWS, "mysql row count changed after reconnect")
    finally:
        reopened.close()


def main() -> int:
    failed = 0
    for name, body in (("pg", bulk_pg), ("mysql", bulk_mysql)):
        try:
            body()
        except Exception as error:  # noqa: BLE001 - the example reports any driver failure
            failed += 1
            print(f"FAIL examples/bulk.py {name}: {type(error).__name__}: {error}")
        else:
            print(f"PASS examples/bulk.py {name} {ROWS} rows")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
