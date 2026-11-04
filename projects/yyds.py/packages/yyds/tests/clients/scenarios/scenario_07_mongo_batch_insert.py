"""Scenario 07 — a 1000-document batch with duplicate `_id` values.

Target feature: insert semantics.
Implementation risk: pymongo sends a document-sequence payload; a duplicate `_id` in the
middle of a thousand documents must abort an ordered insert with code 11000 and `n=0`
while the first document survives, and an `ordered=False` batch must insert the valid
documents instead of stopping at the first duplicate.

The `ordered` flag is a real difference between the two engine contract points, so both
paths get their own case.
"""

from __future__ import annotations

import sys

from pymongo import MongoClient
from pymongo.errors import BulkWriteError

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

SCENARIO_ID = "07-mongo-batch-insert"
CASES = (
    "07-mongo-batch-insert/mongo/1000-documents",
    "07-mongo-batch-insert/mongo/duplicate-id-ordered",
    "07-mongo-batch-insert/mongo/duplicate-id-unordered",
    "07-mongo-batch-insert/mongo/engine-parity",
)

PRELOADED = 100


def _clear(collection) -> None:
    collection.delete_many({})


def _count(collection) -> int:
    """Counts through a plain `find` scan: `count` / `aggregate` are not served here."""

    return sum(1 for _ in collection.find({}))


def _facts(database, scale: int) -> dict:
    facts: dict = {}

    base = database["batch-base"]
    _clear(base)
    base.insert_many([{"_id": index, "n": index} for index in range(scale)], ordered=True)
    facts["base_count"] = _count(base)
    facts["base_first"] = base.find_one({"_id": 0})

    # Ordered abort: the very first document of the batch repeats an `_id`.
    ordered = database["batch-ordered"]
    _clear(ordered)
    ordered.insert_many([{"_id": index, "n": index} for index in range(PRELOADED)], ordered=True)
    batch = [{"_id": 0, "n": 999}] + [{"_id": scale + index, "n": index} for index in range(scale - 1)]
    try:
        ordered.insert_many(batch, ordered=True)
    except BulkWriteError as error:
        details = error.details
        facts["ordered_n_inserted"] = details.get("nInserted")
        facts["ordered_error_codes"] = sorted({item.get("code") for item in details.get("writeErrors", [])})
        facts["ordered_error_count"] = len(details.get("writeErrors", []))
    else:
        facts["ordered_n_inserted"] = "no error raised"
        facts["ordered_error_codes"] = []
        facts["ordered_error_count"] = 0
    facts["ordered_count"] = _count(ordered)
    facts["ordered_first_retained"] = ordered.find_one({"_id": 0}) == {"_id": 0, "n": 0}

    # Unordered batch: valid documents after the duplicates must still be inserted.
    unordered = database["batch-unordered"]
    _clear(unordered)
    unordered.insert_many([{"_id": index, "n": index} for index in range(PRELOADED)], ordered=True)
    mixed = [{"_id": index, "n": index} for index in range(scale)]
    try:
        unordered.insert_many(mixed, ordered=False)
    except BulkWriteError as error:
        details = error.details
        facts["unordered_n_inserted"] = details.get("nInserted")
        facts["unordered_error_codes"] = sorted({item.get("code") for item in details.get("writeErrors", [])})
        facts["unordered_error_count"] = len(details.get("writeErrors", []))
    else:
        facts["unordered_n_inserted"] = "no error raised"
        facts["unordered_error_codes"] = []
        facts["unordered_error_count"] = 0
    facts["unordered_count"] = _count(unordered)
    return facts


def _engine_facts(client: MongoClient, scale: int) -> dict:
    """The oracle keeps its own database so a shared instance stays usable."""

    database = client["yyds_parity_s07"]
    try:
        return _facts(database, scale)
    finally:
        client.drop_database("yyds_parity_s07")


def run(scale: int = 1000) -> list[CaseResult]:
    node = start_mongo_node()
    results: list[CaseResult] = []
    gateway: dict = {}
    try:
        client = mongo_client(node.host, node.mongo_port)
        try:
            gateway = _facts(client["yyds"], scale)

            def documents() -> None:
                confirm(gateway["base_count"] == scale, f"{gateway['base_count']}/{scale} documents were stored")
                confirm(gateway["base_first"] == {"_id": 0, "n": 0}, "a stored document did not round-trip")

            def ordered_abort() -> None:
                confirm(gateway["ordered_n_inserted"] == 0, f"an ordered abort must insert nothing, got {gateway['ordered_n_inserted']}")
                confirm(gateway["ordered_error_codes"] == [11000], f"unexpected error codes {gateway['ordered_error_codes']}")
                confirm(gateway["ordered_error_count"] == 1, "an ordered abort must report one write error")
                confirm(gateway["ordered_count"] == PRELOADED, f"the batch changed the collection ({gateway['ordered_count']} documents)")
                confirm(gateway["ordered_first_retained"], "the original document was overwritten by the rejected insert")

            def unordered_continue() -> None:
                confirm(
                    gateway["unordered_n_inserted"] == scale - PRELOADED,
                    f"an unordered batch must insert the {scale - PRELOADED} valid documents, inserted {gateway['unordered_n_inserted']}",
                )
                confirm(gateway["unordered_error_codes"] == [11000], f"unexpected error codes {gateway['unordered_error_codes']}")
                confirm(
                    gateway["unordered_error_count"] == PRELOADED,
                    f"an unordered batch must report all {PRELOADED} duplicates, reported {gateway['unordered_error_count']}",
                )
                confirm(gateway["unordered_count"] == scale, f"final collection holds {gateway['unordered_count']}/{scale} documents")

            results.extend(
                run_cases(
                    (
                        (CASES[0], documents),
                        (CASES[1], ordered_abort),
                        (CASES[2], unordered_continue),
                    ),
                    detail=f"{scale}-document batches behave as declared",
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
                    "base_count",
                    "base_first",
                    "ordered_n_inserted",
                    "ordered_error_codes",
                    "ordered_error_count",
                    "ordered_count",
                    "ordered_first_retained",
                    "unordered_n_inserted",
                    "unordered_error_codes",
                    "unordered_error_count",
                    "unordered_count",
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
