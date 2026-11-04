"""Scenario 08 — `find` over 1000 documents: sort, skip, filter, empty result, batches.

Target feature: cursor semantics.
Implementation risk: a gateway that scans the namespace head and ignores `sort`, `skip`,
non-`_id` filters and `batchSize` still returns plausible documents, so a shape-only
assertion would pass. Every case here pins the exact document set the real engine
returns, and the batch case reads the raw cursor reply so a single oversized
`firstBatch` is visible.
"""

from __future__ import annotations

import sys

from pymongo import MongoClient

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

SCENARIO_ID = "08-mongo-find-cursor"
CASES = (
    "08-mongo-find-cursor/mongo/sorted-window",
    "08-mongo-find-cursor/mongo/non-id-filter",
    "08-mongo-find-cursor/mongo/empty-batch",
    "08-mongo-find-cursor/mongo/range-filter-1000",
    "08-mongo-find-cursor/mongo/full-sort-1000",
    "08-mongo-find-cursor/mongo/batch-size",
    "08-mongo-find-cursor/mongo/engine-parity",
)

COLLECTION = "find-scale"
BATCH_SIZE = 250


def _facts(database, scale: int) -> dict:
    collection = database[COLLECTION]
    collection.delete_many({})
    # `_id` ascends while `n` descends, so key order and sort order disagree.
    collection.insert_many([{"_id": index, "n": scale - 1 - index} for index in range(scale)], ordered=True)

    sorted_window = [document["n"] for document in collection.find({}, sort=[("n", 1)], limit=10)]
    skipped_window = [
        document["_id"] for document in collection.find({}, sort=[("n", 1)], skip=scale - 10, limit=10)
    ]
    filtered = [document["_id"] for document in collection.find({"n": scale // 2})]
    empty = [document["_id"] for document in collection.find({"n": scale + 1})]
    # Return order without `sort` is unspecified, so compare as a set of ids.
    range_filtered = sorted(document["_id"] for document in collection.find({"n": {"$gte": scale // 2}}))
    full_sorted = [document["n"] for document in collection.find({}, sort=[("n", 1)])]

    reply = database.command({"find": COLLECTION, "batchSize": BATCH_SIZE})
    cursor = reply["cursor"]
    first_batch = len(cursor["firstBatch"])
    cursor_id = cursor["id"]
    drained = first_batch
    while cursor_id:
        reply = database.command(
            {"getMore": cursor_id, "collection": COLLECTION, "batchSize": BATCH_SIZE}
        )
        cursor = reply["cursor"]
        drained += len(cursor["nextBatch"])
        cursor_id = cursor["id"]

    return {
        "sorted_window": sorted_window,
        "skipped_window": skipped_window,
        "filtered": filtered,
        "empty": empty,
        "range_filtered": range_filtered,
        "full_sorted": full_sorted,
        "first_batch": first_batch,
        "cursor_opened": cursor_id != 0 or first_batch < scale,
        "drained": drained,
        # A plain `find` scan: `count` / `aggregate` are not served by this gateway.
        "documents": sum(1 for _ in collection.find({})),
    }


def _engine_facts(client: MongoClient, scale: int) -> dict:
    database = client["yyds_parity_s08"]
    try:
        return _facts(database, scale)
    finally:
        client.drop_database("yyds_parity_s08")


def run(scale: int = 1000) -> list[CaseResult]:
    node = start_mongo_node()
    results: list[CaseResult] = []
    gateway: dict = {}
    try:
        client = mongo_client(node.host, node.mongo_port)
        try:
            gateway = _facts(client["yyds"], scale)

            def sorted_window() -> None:
                confirm(
                    gateway["sorted_window"] == list(range(10)),
                    f"`sort` by `n` returned {gateway['sorted_window']}, expected the ten smallest values",
                )
                # Sorted by `n` ascending, `_id` descends: `scale-1, scale-2, ...`.
                # Skipping `scale-10` leaves the last ten of that sequence.
                expected_ids = list(range(scale - 1, -1, -1))[-10:]
                confirm(
                    gateway["skipped_window"] == expected_ids,
                    f"`skip` returned {gateway['skipped_window']}, expected {expected_ids}",
                )

            def filtered() -> None:
                confirm(
                    gateway["filtered"] == [scale - 1 - scale // 2],
                    f"a non-`_id` filter returned {gateway['filtered']}, expected the single matching document",
                )

            def empty_batch() -> None:
                confirm(gateway["empty"] == [], f"a filter with no match returned {len(gateway['empty'])} documents")

            def range_filter_1000() -> None:
                expected_ids = list(range(scale // 2))
                confirm(
                    len(gateway["range_filtered"]) == scale // 2,
                    f"`$gte` matched {len(gateway['range_filtered'])} of {scale} documents, expected {scale // 2}",
                )
                confirm(
                    gateway["range_filtered"] == expected_ids,
                    f"`$gte` returned ids {gateway['range_filtered'][:5]}..., expected {expected_ids[:5]}...",
                )

            def full_sort_1000() -> None:
                confirm(
                    len(gateway["full_sorted"]) == scale,
                    f"sorting returned {len(gateway['full_sorted'])} of {scale} documents",
                )
                confirm(
                    gateway["full_sorted"] == list(range(scale)),
                    f"sorting the full collection returned {gateway['full_sorted'][:5]}..., expected 0..{scale - 1}",
                )

            def batch_size() -> None:
                confirm(
                    gateway["first_batch"] == BATCH_SIZE,
                    f"`batchSize` {BATCH_SIZE} returned a first batch of {gateway['first_batch']} documents",
                )
                confirm(gateway["cursor_opened"], "a 1000-document scan must leave a live cursor after the first batch")
                confirm(gateway["drained"] == scale, f"draining the cursor yielded {gateway['drained']}/{scale} documents")

            for name, case in (
                (CASES[0], sorted_window),
                (CASES[1], filtered),
                (CASES[2], empty_batch),
                (CASES[3], range_filter_1000),
                (CASES[4], full_sort_1000),
                (CASES[5], batch_size),
            ):
                try:
                    case()
                except Exception as error:  # noqa: BLE001
                    results.append(CaseResult(name, "fail", f"{type(error).__name__}: {error}"))
                else:
                    results.append(CaseResult(name, "pass", f"{scale}-document scan behaves as declared"))
        finally:
            client.close()

        engine = oracle_mongo()
        if engine is None:
            results.append(no_oracle(CASES[6], MONGO_URL_ENV))
        else:
            try:
                engine_facts = _engine_facts(engine, scale)
            finally:
                engine.close()
            results.append(
                compare_facts(
                    CASES[6],
                    gateway,
                    engine_facts,
                    (
                        "sorted_window",
                        "skipped_window",
                        "filtered",
                        "empty",
                        "range_filtered",
                        "full_sorted",
                        "first_batch",
                        "cursor_opened",
                        "drained",
                        "documents",
                    ),
                )
            )
    finally:
        node.stop()

    return results


if __name__ == "__main__":
    raise SystemExit(run_module(run, sys.argv[1:]))
