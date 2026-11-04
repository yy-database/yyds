"""Scenario 09 — `delete` over 1000 documents: zero, one, all, and two databases.

Target feature: delete counts and namespace scope.
Implementation risk: removing documents from the scan head instead of matching the
filter still returns a plausible `n`, so the exact surviving document set is asserted,
not just the count. A second database with the same collection name proves the namespace
is part of the key rather than a global table.
"""

from __future__ import annotations

import sys

from pymongo import MongoClient

from harness import CaseResult, start_mongo_node
from scenarios.support import (
    MONGO_URL_ENV,
    confirm,
    mongo_client,
    oracle_mongo,
    parity_against_oracle,
    run_cases,
    run_module,
)

SCENARIO_ID = "09-mongo-delete-namespace"
CASES = (
    "09-mongo-delete-namespace/mongo/delete-zero-one-all",
    "09-mongo-delete-namespace/mongo/cross-database-isolation",
    "09-mongo-delete-namespace/mongo/engine-parity",
)


def _seed(database, name: str, scale: int) -> None:
    collection = database[name]
    collection.delete_many({})
    collection.insert_many([{"_id": index, "n": index} for index in range(scale)], ordered=True)


def _count(collection) -> int:
    """Counts through a plain `find` scan: `count` / `aggregate` are not served here."""

    return sum(1 for _ in collection.find({}))


def _facts(database, scale: int) -> dict:
    zero = database["delete-zero"]
    _seed(database, "delete-zero", scale)
    zero_deleted = zero.delete_many({"n": -1}).deleted_count
    zero_count = _count(zero)

    one = database["delete-one"]
    _seed(database, "delete-one", scale)
    target = scale // 2
    one_deleted = one.delete_many({"n": target}).deleted_count
    one_count = _count(one)
    one_target_gone = one.find_one({"_id": target}) is None
    one_head_intact = one.find_one({"_id": 0}) is not None
    one_other_intact = one.find_one({"_id": target + 1}) is not None

    everything = database["delete-all"]
    _seed(database, "delete-all", scale)
    all_deleted = everything.delete_many({}).deleted_count
    all_count = _count(everything)

    database["isolation-a"]["shared"].delete_many({})
    database["isolation-b"]["shared"].delete_many({})
    database["isolation-a"]["shared"].insert_one({"_id": 1, "db": "a"})
    database["isolation-b"]["shared"].insert_one({"_id": 1, "db": "b"})
    database["isolation-a"]["shared"].delete_many({})
    isolated = {
        "a_empty": _count(database["isolation-a"]["shared"]) == 0,
        "b_intact": _count(database["isolation-b"]["shared"]) == 1,
        "b_value": (database["isolation-b"]["shared"].find_one({"_id": 1}) or {}).get("db"),
    }

    return {
        "zero_deleted": zero_deleted,
        "zero_count": zero_count,
        "one_deleted": one_deleted,
        "one_count": one_count,
        "one_target_gone": one_target_gone,
        "one_head_intact": one_head_intact,
        "one_other_intact": one_other_intact,
        "all_deleted": all_deleted,
        "all_count": all_count,
        **isolated,
    }


class _DatabaseView:
    """Addresses three databases by the names the scenario uses."""

    def __init__(self, base, isolation_a, isolation_b):
        self._base = base
        self._isolation_a = isolation_a
        self._isolation_b = isolation_b

    def __getitem__(self, name: str):
        if name == "isolation-a":
            return self._isolation_a
        if name == "isolation-b":
            return self._isolation_b
        return self._base[name]


def _engine_facts(client: MongoClient, scale: int) -> dict:
    names = ["yyds_parity_s09", "yyds_parity_s09a", "yyds_parity_s09b"]
    base, isolation_a, isolation_b = (client[name] for name in names)
    try:
        return _facts(_DatabaseView(base, isolation_a, isolation_b), scale)
    finally:
        for name in names:
            client.drop_database(name)


def run(scale: int = 1000) -> list[CaseResult]:
    node = start_mongo_node()
    results: list[CaseResult] = []
    gateway: dict = {}
    try:
        client = mongo_client(node.host, node.mongo_port)
        try:
            gateway = _facts(
                _DatabaseView(client["yyds"], client["s09a"], client["s09b"]), scale
            )

            def counts() -> None:
                confirm(gateway["zero_deleted"] == 0, f"a filter with no match deleted {gateway['zero_deleted']} documents")
                confirm(gateway["zero_count"] == scale, f"{scale - gateway['zero_count']} documents were removed by a no-match delete")
                confirm(gateway["one_deleted"] == 1, f"a one-match delete removed {gateway['one_deleted']} documents")
                confirm(gateway["one_count"] == scale - 1, f"a one-match delete left {gateway['one_count']}/{scale - 1} documents")
                confirm(gateway["one_target_gone"], "the matching document is still present")
                confirm(gateway["one_head_intact"], "delete moved a different document from the scan head")
                confirm(gateway["all_deleted"] == scale, f"an empty filter deleted {gateway['all_deleted']}/{scale} documents")
                confirm(gateway["all_count"] == 0, "an empty filter left documents behind")

            def isolation() -> None:
                confirm(gateway["a_empty"], "the delete did not empty its own database")
                confirm(gateway["b_intact"], "the delete crossed into another database")
                confirm(gateway["b_value"] == "b", "the surviving document is not the one written to the other database")

            results.extend(
                run_cases(
                    ((CASES[0], counts), (CASES[1], isolation)),
                    detail=f"{scale}-document delete behaves as declared",
                )
            )
        finally:
            client.close()

        results.append(
            parity_against_oracle(
                CASES[2],
                gateway,
                oracle_mongo,
                _engine_facts,
                (
                    "zero_deleted",
                    "zero_count",
                    "one_deleted",
                    "one_count",
                    "one_target_gone",
                    "one_head_intact",
                    "one_other_intact",
                    "all_deleted",
                    "all_count",
                    "a_empty",
                    "b_intact",
                    "b_value",
                ),
                scale,
                MONGO_URL_ENV,
            )
        )
    finally:
        node.stop()

    return results


if __name__ == "__main__":
    raise SystemExit(run_module(run, sys.argv[1:]))
