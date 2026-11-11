"""Real-driver protocol conformance against a yyds node.

Mirrors `protocol.ts` with the Python drivers: redis-py, pymongo, psycopg 3 and
mysql-connector-python. Each assertion is derived from what the driver sent, so
the drivers themselves are the oracle for handshake, round-trip, missing-key and
error surfaces.

The Redis cases run twice, once on RESP2 and once on RESP3, because redis-py
negotiates RESP3 with `HELLO 3` on connect and then parses replies strictly: a
RESP2 null bulk string (`$-1`) inside a RESP3 session is a protocol error the
client answers by reconnecting until its timeout.

Run through `node projects/yyds.ts/packages/yyds-harness/run.mjs`, or by hand with the ``YYDS_*_PORT`` variables
pointing at an already running node.
"""

from __future__ import annotations

import os
import sys
from typing import Callable

import mysql.connector
import psycopg
import pymongo
import redis

HOST = os.environ.get("YYDS_HOST", "127.0.0.1")
REDIS_PORT = int(os.environ.get("YYDS_REDIS_PORT", "0"))
MONGO_PORT = int(os.environ.get("YYDS_MONGO_PORT", "0"))
PG_PORT = int(os.environ.get("YYDS_PG_PORT", "0"))
MYSQL_PORT = int(os.environ.get("YYDS_MYSQL_PORT", "0"))
TAG = os.environ.get("YYDS_PROTOCOL_TAG", "py")
REDIS_PREFIX = f"protocol:{TAG}"

for port, name in ((REDIS_PORT, "YYDS_REDIS_PORT"), (MONGO_PORT, "YYDS_MONGO_PORT"), (PG_PORT, "YYDS_PG_PORT"), (MYSQL_PORT, "YYDS_MYSQL_PORT")):
    if port <= 0:
        print(f"{name} must point at a running yyds node", file=sys.stderr)
        raise SystemExit(2)


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def redis_client(decode_responses: bool = True, protocol: int = 2) -> redis.Redis:
    return redis.Redis(
        host=HOST,
        port=REDIS_PORT,
        protocol=protocol,
        decode_responses=decode_responses,
        socket_timeout=30,
    )


def mongo_client() -> pymongo.MongoClient:
    return pymongo.MongoClient(
        f"mongodb://{HOST}:{MONGO_PORT}",
        directConnection=True,
        retryWrites=False,
        retryReads=False,
        serverSelectionTimeoutMS=30_000,
    )


def pg_connect() -> psycopg.Connection:
    return psycopg.connect(host=HOST, port=PG_PORT, user="yyds", password="", dbname="yyds", autocommit=True)


def mysql_connect() -> mysql.connector.MySQLConnection:
    return mysql.connector.connect(
        host=HOST,
        port=MYSQL_PORT,
        user="yyds",
        password="",
        charset="utf8mb4",
        autocommit=True,
    )


def case_redis_ping(protocol: int) -> None:
    client = redis_client(protocol=protocol)
    try:
        check(client.ping() is True, "PING did not answer PONG")
    finally:
        client.close()


def case_redis_kv_roundtrip_and_missing(protocol: int) -> None:
    client = redis_client(protocol=protocol)
    try:
        key = f"{REDIS_PREFIX}:kv"
        value = f"value-{TAG}-ü-🚀"
        check(client.set(key, value) is True, "SET did not answer OK")
        check(client.get(key) == value, "GET returned a different value")
        check(client.get(f"{REDIS_PREFIX}:absent") is None, "missing GET did not answer None")
        # The null reply must not cost the connection: the same client keeps answering.
        check(client.ping() is True, "connection stopped answering after a missing GET")
        check(client.delete(key) == 1, "DEL did not remove the key")
        check(client.get(key) is None, "deleted key is still readable")
    finally:
        client.close()


def case_redis_binary_payload_roundtrip(protocol: int) -> None:
    writer = redis_client(protocol=protocol)
    reader = redis_client(protocol=protocol, decode_responses=False)
    try:
        payload = b"\x00\x01\x02\x7f\x80\xfe\xff"
        key = f"{REDIS_PREFIX}:binary"
        writer.set(key, payload)
        check(reader.get(key) == payload, "binary GET did not return the written bytes")
        writer.delete(key)
    finally:
        writer.close()
        reader.close()


def case_redis_unsupported_command(protocol: int) -> None:
    client = redis_client(protocol=protocol)
    try:
        message = None
        try:
            client.execute_command("YYDSNOSUCHCOMMAND")
        except redis.ResponseError as error:
            message = str(error)
        check(message is not None, "unsupported command was accepted")
        check("unknown command" in message.lower(), f"unsupported command answered {message}, expected the upstream 'unknown command' wording")
        check(client.ping() is True, "connection stopped answering after the rejected command")
    finally:
        client.close()


def case_mongo_ping() -> None:
    client = mongo_client()
    try:
        reply = client.admin.command("ping")
        check(int(reply.get("ok", 0)) == 1, f"ping answered ok={reply.get('ok')}")
    finally:
        client.close()


def case_mongo_document_roundtrip_and_missing() -> None:
    client = mongo_client()
    try:
        collection = client[f"protocol_{TAG}"][f"protocol_{TAG}"]
        collection.delete_many({})
        document = {"_id": f"{TAG}-doc", "n": 7, "label": "kept"}
        collection.insert_one(document)
        check(collection.find_one({"_id": document["_id"]}) == document, "find_one returned a different document")
        check(collection.find_one({"_id": f"{TAG}-absent"}) is None, "missing find_one did not answer None")
        collection.delete_many({})
    finally:
        client.close()


def case_pg_version_stable() -> None:
    with pg_connect() as connection:
        version = connection.execute("SELECT version()").fetchone()[0]
    check(isinstance(version, str) and len(version) > 0, f"SELECT version() returned {version!r}")
    with pg_connect() as connection:
        again = connection.execute("SELECT version()").fetchone()[0]
    check(again == version, f"version changed between connections: {version} vs {again}")


def case_pg_unknown_table() -> None:
    with pg_connect() as connection:
        sqlstate = None
        try:
            connection.execute(f"SELECT note FROM protocol_absent_{TAG}")
        except psycopg.Error as error:
            sqlstate = error.sqlstate
        check(sqlstate == "42P01", f"unknown table answered sqlstate {sqlstate}, expected 42P01")
        check(connection.execute("SELECT 1").fetchone()[0] == 1, "connection stopped answering after the error")


def case_mysql_version_stable() -> None:
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT @@version")
            version = cursor.fetchone()[0]
    finally:
        connection.close()
    check(isinstance(version, str) and len(version) > 0, f"SELECT @@version returned {version!r}")
    connection = mysql_connect()
    try:
        with connection.cursor() as cursor:
            cursor.execute("SELECT @@version")
            check(cursor.fetchone()[0] == version, f"version changed between connections: {version}")
    finally:
        connection.close()


def case_mysql_unknown_table() -> None:
    connection = mysql_connect()
    try:
        errno = None
        with connection.cursor() as cursor:
            try:
                cursor.execute(f"SELECT note FROM protocol_absent_{TAG}")
            except mysql.connector.Error as error:
                errno = error.errno
            check(errno == 1146, f"unknown table answered errno {errno}, expected 1146")
            cursor.execute("SELECT 1")
            check(cursor.fetchone()[0] == 1, "connection stopped answering after the error")
    finally:
        connection.close()


CASES: list[tuple[str, str, Callable[[], None]]] = []
for protocol in (2, 3):
    suffix = f"resp{protocol}"
    CASES.extend(
        [
            ("redis", f"ping_answers_pong_{suffix}", lambda protocol=protocol: case_redis_ping(protocol)),
            (
                "redis",
                f"kv_roundtrip_and_missing_is_none_{suffix}",
                lambda protocol=protocol: case_redis_kv_roundtrip_and_missing(protocol),
            ),
            (
                "redis",
                f"binary_payload_roundtrip_{suffix}",
                lambda protocol=protocol: case_redis_binary_payload_roundtrip(protocol),
            ),
            (
                "redis",
                f"unsupported_command_is_rejected_and_connection_survives_{suffix}",
                lambda protocol=protocol: case_redis_unsupported_command(protocol),
            ),
        ]
    )
CASES.extend(
    [
        ("mongo", "ping_command_succeeds", case_mongo_ping),
        ("mongo", "document_roundtrip_and_missing_is_none", case_mongo_document_roundtrip_and_missing),
        ("postgres", "server_version_is_stable_across_connections", case_pg_version_stable),
        ("postgres", "unknown_table_is_structured_error_and_connection_survives", case_pg_unknown_table),
        ("mysql", "server_version_is_stable_across_connections", case_mysql_version_stable),
        ("mysql", "unknown_table_is_structured_error_and_connection_survives", case_mysql_unknown_table),
    ]
)


def main() -> int:
    passed = 0
    failures: list[str] = []
    for driver, name, body in CASES:
        try:
            body()
        except Exception as error:  # noqa: BLE001 - report every driver error uniformly
            failures.append(f"{driver}/{name}: {error}")
            print(f"FAIL {driver}/{name}: {error}")
            continue
        passed += 1
        print(f"PASS {driver}/{name}")
    print(f"PY protocol {passed}/{len(CASES)} passed, {len(failures)} failed")
    if failures:
        for failure in failures:
            print(failure, file=sys.stderr)
        return 1
    return 0


raise SystemExit(main())
