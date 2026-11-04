"""Scenario 10 — error-code truth: 59 for an unknown command, 73 for a bad namespace.

Target feature: wire-level error codes.
Implementation risk: an emulated server can answer a generic error for everything, which
drivers surface as "something failed" while client code that branches on `59` or `73`
silently takes the wrong path. The codes are asserted exactly, and namespaces the real
engine refuses are included so a permissive check cannot hide behind the empty-name case.
"""

from __future__ import annotations

import sys

from pymongo import MongoClient
from pymongo.errors import OperationFailure

from harness import CaseResult, start_mongo_node
from scenarios.support import (
    MONGO_URL_ENV,
    compare_facts,
    confirm,
    mongo_client,
    no_oracle,
    oracle_mongo,
    run_module,
)

SCENARIO_ID = "10-mongo-error-codes"
CASES = (
    "10-mongo-error-codes/mongo/unknown-command-59",
    "10-mongo-error-codes/mongo/invalid-namespace-73",
    "10-mongo-error-codes/mongo/engine-parity",
)


def _code(database, command: dict) -> int | None:
    try:
        database.command(command)
    except OperationFailure as error:
        return error.code
    return None


def _facts(database, scale: int) -> dict:
    codes = {
        "unknown_code": _code(database, {"yydsUnknownCommand": 1}),
        "empty_namespace_code": _code(database, {"insert": "", "documents": [{"_id": scale}]}),
        "empty_find_code": _code(database, {"find": "", "filter": {}}),
        "dotted_collection_code": _code(database, {"insert": "dotted.name", "documents": [{"_id": 1}]}),
        "dollar_collection_code": _code(database, {"insert": "money$name", "documents": [{"_id": 2}]}),
    }
    return codes


def _engine_facts(client: MongoClient, scale: int) -> dict:
    database = client["yyds_parity_s10"]
    try:
        return _facts(database, scale)
    finally:
        client.drop_database("yyds_parity_s10")


def run(scale: int = 1000) -> list[CaseResult]:
    node = start_mongo_node()
    results: list[CaseResult] = []
    gateway: dict = {}
    try:
        client = mongo_client(node.host, node.mongo_port)
        try:
            gateway = _facts(client["yyds"], scale)

            def unknown() -> None:
                confirm(
                    gateway["unknown_code"] == 59,
                    f"an unknown command answered code {gateway['unknown_code']}, expected 59",
                )

            def namespace() -> None:
                # Empty names and names carrying `$` are invalid on a real server (73).
                # Dots are legal in MongoDB collection names, so accepting them is the
                # correct behaviour and must not be counted as a permissive namespace check.
                for field, label in (
                    ("empty_namespace_code", "an empty collection name"),
                    ("empty_find_code", "an empty `find` collection"),
                    ("dollar_collection_code", "a collection name containing `$`"),
                ):
                    confirm(gateway[field] == 73, f"{label} answered code {gateway[field]}, expected 73")
                confirm(
                    gateway["dotted_collection_code"] is None,
                    f"a dotted collection name is legal in MongoDB but answered code {gateway['dotted_collection_code']}",
                )

            for name, case in ((CASES[0], unknown), (CASES[1], namespace)):
                try:
                    case()
                except Exception as error:  # noqa: BLE001
                    results.append(CaseResult(name, "fail", f"{type(error).__name__}: {error}"))
                else:
                    results.append(CaseResult(name, "pass", f"checked at scale {scale}"))
        finally:
            client.close()

        engine = oracle_mongo()
        if engine is None:
            results.append(no_oracle(CASES[2], MONGO_URL_ENV))
        else:
            try:
                engine_facts = _engine_facts(engine, scale)
            finally:
                engine.close()
            results.append(
                compare_facts(
                    CASES[2],
                    gateway,
                    engine_facts,
                    (
                        "unknown_code",
                        "empty_namespace_code",
                        "empty_find_code",
                        "dotted_collection_code",
                        "dollar_collection_code",
                    ),
                )
            )
    finally:
        node.stop()

    return results


if __name__ == "__main__":
    raise SystemExit(run_module(run, sys.argv[1:]))
