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

from harness import CaseResult, start_mongo_node
from scenarios.support import (
    MONGO_URL_ENV,
    command_error_code,
    confirm,
    mongo_client,
    oracle_mongo,
    parity_against_oracle,
    run_cases,
    run_module,
)

SCENARIO_ID = "10-mongo-error-codes"
CASES = (
    "10-mongo-error-codes/mongo/unknown-command-59",
    "10-mongo-error-codes/mongo/invalid-namespace-73",
    "10-mongo-error-codes/mongo/unsupported-stage-59",
    "10-mongo-error-codes/mongo/engine-parity",
)


def _facts(database, scale: int) -> dict:
    codes = {
        "unknown_code": command_error_code(database, {"yydsUnknownCommand": 1}),
        "empty_namespace_code": command_error_code(database, {"insert": "", "documents": [{"_id": scale}]}),
        "empty_find_code": command_error_code(database, {"find": "", "filter": {}}),
        "dotted_collection_code": command_error_code(database, {"insert": "dotted.name", "documents": [{"_id": 1}]}),
        "dollar_collection_code": command_error_code(database, {"insert": "money$name", "documents": [{"_id": 2}]}),
        # `$lookup` is a legal MongoDB stage the gateway does not implement, so the
        # risk is silently yielding an empty or unfiltered batch instead of saying so.
        "unsupported_stage_code": command_error_code(
            database,
            {"aggregate": "aggregate-probe", "pipeline": [{"$lookup": {"from": "other", "pipeline": []}}], "cursor": {}},
        ),
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

            def unsupported_stage() -> None:
                confirm(
                    gateway["unsupported_stage_code"] == 59,
                    f"an unimplemented `$lookup` stage answered code {gateway['unsupported_stage_code']}, expected 59",
                )

            results.extend(
                run_cases(
                    ((CASES[0], unknown), (CASES[1], namespace), (CASES[2], unsupported_stage)),
                    detail=f"checked at scale {scale}",
                )
            )
        finally:
            client.close()

        results.append(
            parity_against_oracle(
                CASES[3],
                gateway,
                oracle_mongo,
                _engine_facts,
                (
                    "unknown_code",
                    "empty_namespace_code",
                    "empty_find_code",
                    "dotted_collection_code",
                    "dollar_collection_code",
                    # `unsupported_stage_code` is deliberately left out: a real
                    # server runs `$lookup`, while this gateway declares 59 for
                    # stages it does not implement.
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
