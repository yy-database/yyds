"""Restart-recovery gate for a `yyds start` node.

Run in two phases around a node restart, reusing the same data directory:

  YYDS_RECOVERY_PHASE=seed     create tables, write rows, set a Redis key,
                               rename it, save expected state to JSON
  YYDS_RECOVERY_PHASE=verify   assert rows, sequences, duplicate-key codes and
                               the Redis value/deadline survived the restart

Drivers are the real `psycopg` (or `psycopg2`), `pymysql` and `redis` packages.
"""

import json
import os
import sys
import time

import pymysql
import redis

try:
    import psycopg
except ImportError:  # psycopg2 is installed in some environments
    import psycopg2 as psycopg

HOST = os.environ.get("YYDS_HOST", "127.0.0.1")
PG_PORT = int(os.environ["YYDS_PG_PORT"])
MYSQL_PORT = int(os.environ["YYDS_MYSQL_PORT"])
REDIS_PORT = int(os.environ["YYDS_REDIS_PORT"])
PHASE = os.environ["YYDS_RECOVERY_PHASE"]
STATE = os.environ["YYDS_RECOVERY_STATE"]

PG_TABLE = "recovery_pg_users"
MYSQL_TABLE = "recovery_mysql_users"
REDIS_SOURCE = "yyds:recovery:source"
REDIS_TARGET = "yyds:recovery:renamed"
REDIS_VALUE = "persisted-value"
REDIS_TTL_SECONDS = 120

passed = 0
failures = []


def check(condition, message):
    if not condition:
        raise AssertionError(message)


def case(name, body):
    global passed
    try:
        body()
    except Exception as error:  # noqa: BLE001 - report every driver error
        failures.append(f"{name}: {error}")
        print(f"FAIL {name}: {error}")
        return
    passed += 1
    print(f"PASS {name}")


def pg_sqlstate(error):
    return getattr(error, "sqlstate", None) or getattr(error, "pgcode", None)


def pg_connection():
    kwargs = {"host": HOST, "port": PG_PORT, "user": "yyds", "password": "", "dbname": "yyds"}
    return psycopg.connect(**kwargs)


def mysql_connection():
    return pymysql.connect(host=HOST, port=MYSQL_PORT, user="yyds", password="", database="yyds")


def redis_connection():
    return redis.Redis(host=HOST, port=REDIS_PORT, decode_responses=True)


def seed():
    def pg_seed():
        with pg_connection() as connection:
            connection.execute(
                f"CREATE TABLE {PG_TABLE} (id BIGSERIAL PRIMARY KEY, email TEXT NOT NULL, note TEXT, UNIQUE (email))"
            )
            connection.execute(f"INSERT INTO {PG_TABLE} (email, note) VALUES ('persisted', 'pg-row')")
            row = connection.execute(f"SELECT id FROM {PG_TABLE} WHERE email = 'persisted'").fetchone()
            check(row is not None and int(row[0]) > 0, f"seed row id was {row}")
            state["pg_id"] = int(row[0])
            print(f"[seed] pg id={state['pg_id']}")

    def mysql_seed():
        connection = mysql_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(
                    f"CREATE TABLE {MYSQL_TABLE} (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, "
                    "email VARCHAR(191) NOT NULL, note TEXT, UNIQUE KEY uq_recovery_email (email))"
                )
                cursor.execute(f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES ('persisted', 'mysql-row')")
                cursor.execute(f"SELECT id FROM {MYSQL_TABLE} WHERE email = 'persisted'")
                row = cursor.fetchone()
                check(row is not None and int(row[0]) > 0, f"seed row id was {row}")
                state["mysql_id"] = int(row[0])
                print(f"[seed] mysql id={state['mysql_id']}")
            connection.commit()
        finally:
            connection.close()

    def redis_seed():
        client = redis_connection()
        try:
            client.set(REDIS_SOURCE, REDIS_VALUE, ex=REDIS_TTL_SECONDS)
            client.rename(REDIS_SOURCE, REDIS_TARGET)
            ttl = client.ttl(REDIS_TARGET)
            check(0 < ttl <= REDIS_TTL_SECONDS, f"TTL after RENAME was {ttl}")
            state["redis_ttl"] = int(ttl)
            print(f"[seed] redis ttl={ttl}")
        finally:
            client.close()

    state = {}
    case("pg_seed", pg_seed)
    case("mysql_seed", mysql_seed)
    case("redis_seed", redis_seed)
    with open(STATE, "w", encoding="utf-8") as handle:
        json.dump(state, handle)


def verify():
    with open(STATE, encoding="utf-8") as handle:
        state = json.load(handle)

    def pg_rows_survived():
        with pg_connection() as connection:
            row = connection.execute(f"SELECT id, note FROM {PG_TABLE} WHERE email = 'persisted'").fetchone()
            check(row is not None, "persisted row disappeared after restart")
            check(int(row[0]) == state["pg_id"], f"pg id changed {state['pg_id']} -> {row[0]}")
            check(row[1] == "pg-row", f"pg note changed to {row[1]}")
            print(f"[verify] pg row id={row[0]} note={row[1]}")

    def pg_sequence_continues():
        with pg_connection() as connection:
            connection.execute(f"INSERT INTO {PG_TABLE} (email, note) VALUES ('after-restart', 'pg-next')")
            row = connection.execute(f"SELECT id FROM {PG_TABLE} WHERE email = 'after-restart'").fetchone()
            check(row is not None, "post-restart insert was not visible")
            expected = state["pg_id"] + 1
            check(int(row[0]) == expected, f"BIGSERIAL continued at {row[0]}, expected {expected}")
            print(f"[verify] pg next id={row[0]} expected={expected}")

    def pg_duplicate_still_fails():
        with pg_connection() as connection:
            try:
                connection.execute(f"INSERT INTO {PG_TABLE} (email, note) VALUES ('persisted', 'duplicate')")
                sqlstate = None
            except Exception as error:  # noqa: BLE001 - driver-specific exception type
                sqlstate = pg_sqlstate(error)
            check(sqlstate == "23505", f"duplicate INSERT after restart returned {sqlstate}")
            print("[verify] pg duplicate sqlstate=23505")

    def mysql_rows_survived():
        connection = mysql_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"SELECT id, note FROM {MYSQL_TABLE} WHERE email = 'persisted'")
                row = cursor.fetchone()
                check(row is not None, "persisted row disappeared after restart")
                check(int(row[0]) == state["mysql_id"], f"mysql id changed {state['mysql_id']} -> {row[0]}")
                check(row[1] == "mysql-row", f"mysql note changed to {row[1]}")
                print(f"[verify] mysql row id={row[0]} note={row[1]}")
        finally:
            connection.close()

    def mysql_sequence_continues():
        connection = mysql_connection()
        try:
            with connection.cursor() as cursor:
                cursor.execute(f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES ('after-restart', 'mysql-next')")
                cursor.execute(f"SELECT id FROM {MYSQL_TABLE} WHERE email = 'after-restart'")
                row = cursor.fetchone()
                check(row is not None, "post-restart insert was not visible")
                expected = state["mysql_id"] + 1
                check(int(row[0]) == expected, f"AUTO_INCREMENT continued at {row[0]}, expected {expected}")
                print(f"[verify] mysql next id={row[0]} expected={expected}")
            connection.commit()
        finally:
            connection.close()

    def mysql_duplicate_still_fails():
        connection = mysql_connection()
        try:
            with connection.cursor() as cursor:
                try:
                    cursor.execute(f"INSERT INTO {MYSQL_TABLE} (email, note) VALUES ('persisted', 'duplicate')")
                    error_number = None
                except Exception as error:  # noqa: BLE001 - pymysql raises its own type
                    error_number = error.args[0] if error.args else None
                check(error_number == 1062, f"duplicate INSERT after restart returned {error_number}")
                print("[verify] mysql duplicate errno=1062")
        finally:
            connection.close()

    def redis_value_and_deadline_survived():
        client = redis_connection()
        try:
            value = client.get(REDIS_TARGET)
            check(value == REDIS_VALUE, f"renamed key value was {value!r}")
            ttl_before = client.ttl(REDIS_TARGET)
            check(0 < ttl_before <= REDIS_TTL_SECONDS, f"TTL after restart was {ttl_before}")
            check(client.ttl(REDIS_SOURCE) == -2, "RENAME source key reappeared after restart")
            time.sleep(1.1)
            ttl_after = client.ttl(REDIS_TARGET)
            check(ttl_after < ttl_before, f"deadline did not advance: {ttl_before} -> {ttl_after}")
            print(f"[verify] redis value={value} ttl {ttl_before} -> {ttl_after}")
        finally:
            client.close()

    case("pg_rows_survived", pg_rows_survived)
    case("pg_sequence_continues", pg_sequence_continues)
    case("pg_duplicate_still_fails", pg_duplicate_still_fails)
    case("mysql_rows_survived", mysql_rows_survived)
    case("mysql_sequence_continues", mysql_sequence_continues)
    case("mysql_duplicate_still_fails", mysql_duplicate_still_fails)
    case("redis_value_and_deadline_survived", redis_value_and_deadline_survived)


if PHASE == "seed":
    seed()
elif PHASE == "verify":
    verify()
else:
    print(f"unknown YYDS_RECOVERY_PHASE={PHASE}", file=sys.stderr)
    sys.exit(2)

print(f"[recovery] {PHASE} passed={passed} failed={len(failures)}")
if failures:
    for failure in failures:
        print(f"  - {failure}")
sys.exit(0 if not failures else 1)
