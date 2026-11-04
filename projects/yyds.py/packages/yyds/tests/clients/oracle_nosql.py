"""Redis and MongoDB engine-parity probes with real engines as the truth source.

A driver-level assertion only proves that `redis-py` / `node-redis` / `pymongo` can
read an answer from the gateway. It does not prove that the answer is what the real
engine would have produced. This runner closes that gap for the Redis and MongoDB
capabilities where the emulated servers claim engine behaviour:

1. run the probe against the external engine named by `YYDS_ORACLE_*`,
2. run the identical probe against the YYDS node under test,
3. compare the normalized outcome.

The probes are deliberately normalized facts (booleans, counts, bounded values,
sorted key sets) rather than raw numbers, so a wall-clock millisecond difference is
not reported as a semantic divergence while a reset deadline or a missing field is.

Probes whose outcome differs are reported as `fail` with both sides recorded. When
no oracle endpoint is configured the status is `unverified`: the report states that
only driver-level evidence exists and nothing is counted as passed.

Oracle configuration:

    YYDS_ORACLE_REDIS_URL   redis://host:port/0
    YYDS_ORACLE_MONGO_URL   mongodb://host:port/   (or a URI with credentials)

The probes create and delete only their own `parity:*` keys and their own
`yyds_parity` documents, so a shared oracle instance stays usable.
"""

from __future__ import annotations

import os
import time
from typing import Any, Callable

import redis
from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, OperationFailure

from harness import CaseResult, NodeFailure, start_mongo_node, start_redis_node

REDIS_URL_ENV = "YYDS_ORACLE_REDIS_URL"
MONGO_URL_ENV = "YYDS_ORACLE_MONGO_URL"

REDIS_PROBES = (
    "redis-oracle/ttl-wall-clock",
    "redis-oracle/rename-deadline-migration",
    "redis-oracle/hello-3-fields",
)
MONGO_PROBES = (
    "mongo-oracle/duplicate-id-11000",
    "mongo-oracle/empty-namespace-73",
)

# Fields a RESP3 `HELLO 3` reply must contain. Both engines carry these.
HELLO_REQUIRED = {"server", "proto", "id", "mode", "role", "modules"}
# Fields a real Redis adds that the emulated reply does not carry. The divergence is
# asserted against this list: a newly appearing field fails instead of being ignored.
HELLO_KNOWN_EXTRA = {"version", "os", "arch", "bits", "libc", "redis_version", "keystats", "listpack-entries"}


def _oracle(name: str) -> str | None:
    value = os.environ.get(name, "").strip()
    return value or None


def _unverified(prefix: str, probes: tuple[str, ...], reason: str) -> list[CaseResult]:
    return [CaseResult(name, "unverified", reason) for name in probes]


# ---------------------------------------------------------------------------
# Redis probes
# ---------------------------------------------------------------------------


def _redis_facts(connect: Callable[[], redis.Redis], connect_resp3: Callable[[], redis.Redis]) -> dict[str, Any]:
    client = connect()
    facts: dict[str, Any] = {}
    try:
        client.delete("parity:ttl", "parity:rename-src", "parity:rename-dst")

        # TTL is wall-clock: `EXPIRE 1` counts one real second, not one operation.
        client.set("parity:ttl", "value")
        client.expire("parity:ttl", 1)
        facts["ttl_initial_seconds"] = client.ttl("parity:ttl")
        facts["ttl_initial_has_millis"] = client.pttl("parity:ttl") > 0
        time.sleep(1.25)
        facts["ttl_key_exists_after_1_25s"] = client.exists("parity:ttl")
        facts["ttl_key_value_after_1_25s"] = client.get("parity:ttl")

        # `RENAME` carries the deadline instead of restarting it: a key with ~400ms
        # left must be gone 900ms after the rename, not alive for another second.
        client.set("parity:rename-src", "payload")
        client.pexpire("parity:rename-src", 1000)
        time.sleep(0.6)
        client.rename("parity:rename-src", "parity:rename-dst")
        remaining = client.pttl("parity:rename-dst")
        facts["rename_destination_value"] = client.get("parity:rename-dst")
        facts["rename_source_exists"] = client.exists("parity:rename-src")
        facts["rename_deadline_carried"] = 0 < remaining <= 1000
        time.sleep(0.9)
        facts["rename_destination_exists_after_deadline"] = client.exists("parity:rename-dst")
        client.delete("parity:rename-dst")
    finally:
        client.close()

    hello = connect_resp3()
    try:
        reply = hello.execute_command("HELLO", 3)
    finally:
        hello.close()
    facts["hello3_keys"] = sorted(str(key) for key in reply)
    facts["hello3_proto"] = reply.get("proto")
    return facts


def _compare_redis(real: dict[str, Any], simulated: dict[str, Any]) -> list[CaseResult]:
    results: list[CaseResult] = []
    ttl_names = ("ttl_initial_seconds", "ttl_initial_has_millis", "ttl_key_exists_after_1_25s", "ttl_key_value_after_1_25s")
    rename_names = (
        "rename_destination_value",
        "rename_source_exists",
        "rename_deadline_carried",
        "rename_destination_exists_after_deadline",
    )
    for probe, names in (("redis-oracle/ttl-wall-clock", ttl_names), ("redis-oracle/rename-deadline-migration", rename_names)):
        divergent = [f"{name}: engine={real.get(name)!r} gateway={simulated.get(name)!r}" for name in names if real.get(name) != simulated.get(name)]
        if divergent:
            results.append(CaseResult(probe, "fail", " | ".join(divergent)))
        else:
            results.append(CaseResult(probe, "pass", "engine and gateway agree on " + ", ".join(names)))

    real_keys = set(real.get("hello3_keys", []))
    simulated_keys = set(simulated.get("hello3_keys", []))
    missing_required = sorted(HELLO_REQUIRED - simulated_keys)
    unknown_extra = sorted(real_keys - simulated_keys - HELLO_KNOWN_EXTRA)
    invented = sorted(simulated_keys - real_keys)
    divergence = sorted(real_keys - simulated_keys)
    problems: list[str] = []
    if missing_required:
        problems.append(f"gateway hello3 reply is missing {missing_required}")
    if unknown_extra:
        problems.append(f"real hello3 carries undocumented fields {unknown_extra}")
    if invented:
        problems.append(f"gateway hello3 invents fields the engine does not send {invented}")
    if problems:
        results.append(CaseResult("redis-oracle/hello-3-fields", "fail", " | ".join(problems) + f" | engine={sorted(real_keys)} gateway={sorted(simulated_keys)}"))
    else:
        detail = f"engine={len(real_keys)} fields, gateway={len(simulated_keys)} fields"
        if divergence:
            detail += f", known divergence: {divergence}"
        results.append(CaseResult("redis-oracle/hello-3-fields", "pass", detail))
    return results


# ---------------------------------------------------------------------------
# MongoDB probes
# ---------------------------------------------------------------------------


def _mongo_facts(open_client: Callable[[], MongoClient]) -> dict[str, Any]:
    client = open_client()
    facts: dict[str, Any] = {}
    try:
        collection = client["yyds_parity"]["dup"]
        collection.delete_many({})
        collection.insert_one({"_id": "fixed", "v": 1})
        facts["duplicate_code"] = 0
        facts["duplicate_message_mentions_e11000"] = False
        try:
            collection.insert_one({"_id": "fixed", "v": 2})
        except DuplicateKeyError as error:
            facts["duplicate_code"] = error.code or 0
            facts["duplicate_message_mentions_e11000"] = "E11000" in str(error)
        facts["first_document_value"] = (collection.find_one({"_id": "fixed"}) or {}).get("v")
        facts["document_count"] = collection.count_documents({})

        commands = {
            "insert": {"insert": "", "documents": [{"v": 1}]},
            "find": {"find": "", "filter": {}},
            "delete": {"delete": "", "deletes": [{"q": {}, "limit": 0}]},
        }
        for label, command in commands.items():
            try:
                client["yyds_parity"].command(command)
                code = 0
            except OperationFailure as error:
                code = error.code or 0
            facts[f"empty_namespace_{label}_code"] = code
        collection.delete_many({})
    finally:
        client.close()
    return facts


def _compare_mongo(real: dict[str, Any], simulated: dict[str, Any]) -> list[CaseResult]:
    duplicate_names = ("duplicate_code", "duplicate_message_mentions_e11000", "first_document_value", "document_count")
    namespace_names = (
        "empty_namespace_insert_code",
        "empty_namespace_find_code",
        "empty_namespace_delete_code",
    )
    results: list[CaseResult] = []
    for probe, names in (("mongo-oracle/duplicate-id-11000", duplicate_names), ("mongo-oracle/empty-namespace-73", namespace_names)):
        divergent = [f"{name}: engine={real.get(name)!r} gateway={simulated.get(name)!r}" for name in names if real.get(name) != simulated.get(name)]
        if divergent:
            results.append(CaseResult(probe, "fail", " | ".join(divergent)))
        else:
            results.append(CaseResult(probe, "pass", "engine and gateway agree on " + ", ".join(names)))
    return results


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def run_parity() -> list[CaseResult]:
    """Compares the emulated Redis and MongoDB gateways against the configured engines."""

    results: list[CaseResult] = []

    redis_url = _oracle(REDIS_URL_ENV)
    if redis_url is None:
        results.extend(_unverified("redis", REDIS_PROBES, "no oracle configured: only driver-level evidence, missing engine parity"))
    else:
        node = start_redis_node()
        try:
            real = _redis_facts(
                lambda: redis.from_url(redis_url, db=0, decode_responses=True, socket_timeout=5),
                lambda: redis.from_url(redis_url, db=0, protocol=3, decode_responses=True, socket_timeout=5),
            )
            simulated = _redis_facts(
                lambda: redis.Redis(host=node.host, port=node.redis_port, db=0, decode_responses=True, socket_timeout=5),
                lambda: redis.Redis(host=node.host, port=node.redis_port, db=0, protocol=3, decode_responses=True, socket_timeout=5),
            )
        finally:
            node.stop()
        results.extend(_compare_redis(real, simulated))

    mongo_url = _oracle(MONGO_URL_ENV)
    if mongo_url is None:
        results.extend(_unverified("mongo", MONGO_PROBES, "no oracle configured: only driver-level evidence, missing engine parity"))
    else:
        node = start_mongo_node()
        try:
            real = _mongo_facts(lambda: MongoClient(mongo_url, directConnection=True, serverSelectionTimeoutMS=5000, retryWrites=False, retryReads=False))
            simulated = _mongo_facts(
                lambda: MongoClient(
                    f"mongodb://{node.host}:{node.mongo_port}",
                    directConnection=True,
                    serverSelectionTimeoutMS=5000,
                    retryWrites=False,
                    retryReads=False,
                )
            )
        finally:
            node.stop()
        results.extend(_compare_mongo(real, simulated))

    return results


__all__ = ["MONGO_PROBES", "REDIS_PROBES", "run_parity", "NodeFailure"]
