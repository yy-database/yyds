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
    confirm,
    mongo_client,
    oracle_mongo,
    parity_against_oracle,
    run_cases,
    run_module,
)

SCENARIO_ID = "08-mongo-find-cursor"
CASES = (
    "08-mongo-find-cursor/mongo/sorted-window",
    "08-mongo-find-cursor/mongo/non-id-filter",
    "08-mongo-find-cursor/mongo/empty-batch",
    "08-mongo-find-cursor/mongo/range-filter-1000",
    "08-mongo-find-cursor/mongo/full-sort-1000",
    "08-mongo-find-cursor/mongo/count-1000",
    "08-mongo-find-cursor/mongo/count-skip-limit-boundary",
    "08-mongo-find-cursor/mongo/aggregate-count-1000",
    "08-mongo-find-cursor/mongo/aggregate-project",
    "08-mongo-find-cursor/mongo/batch-size",
    "08-mongo-find-cursor/mongo/find-skip-beyond-window",
    "08-mongo-find-cursor/mongo/engine-parity",
)

COLLECTION = "find-scale"
BATCH_SIZE = 250


def _probe(call):
    """Runs one probe, turning a driver failure into a fact the case can name."""
    try:
        return call()
    except Exception as error:  # noqa: BLE001 - the case reports the driver error
        return f"error: {type(error).__name__}: {error}"


def _facts(database, scale: int) -> dict:
    collection = database[COLLECTION]
    collection.delete_many({})
    # `_id` ascends while `n` descends, so key order and sort order disagree.
    collection.insert_many(
        [{"_id": index, "n": scale - 1 - index, "label": f"doc-{index:04d}"} for index in range(scale)],
        ordered=True,
    )

    sorted_window = [document["n"] for document in collection.find({}, sort=[("n", 1)], limit=10)]
    skipped_window = [
        document["_id"] for document in collection.find({}, sort=[("n", 1)], skip=scale - 10, limit=10)
    ]
    filtered = [document["_id"] for document in collection.find({"n": scale // 2})]
    empty = [document["_id"] for document in collection.find({"n": scale + 1})]
    # Return order without `sort` is unspecified, so compare as a set of ids.
    range_filtered = sorted(document["_id"] for document in collection.find({"n": {"$gte": scale // 2}}))
    full_sorted = [document["n"] for document in collection.find({}, sort=[("n", 1)])]

    # pymongo drives `count_documents` through `aggregate`, so both commands are
    # probed here. A gateway that answers 59 records that failure as a fact.
    count_all = _probe(lambda: collection.count_documents({}))
    count_range = _probe(
        lambda: database.command({"count": COLLECTION, "query": {"n": {"$gte": scale // 2}}})["n"]
    )
    count_documents_range = _probe(lambda: collection.count_documents({"n": {"$gte": scale // 2}}))
    aggregate_count = _probe(
        lambda: next(
            collection.aggregate(
                [
                    {"$match": {"n": {"$gte": scale // 2}}},
                    {"$group": {"_id": None, "total": {"$sum": 1}}},
                ]
            )
        )["total"]
    )
    projected = _probe(
        lambda: [
            dict(document)
            for document in collection.aggregate(
                [
                    {"$match": {"n": {"$eq": scale // 2}}},
                    {"$project": {"_id": 0, "label": 1}},
                ]
            )
        ]
    )

    # Boundary windows: a skip past the end must count and return nothing rather
    # than clamp to a plausible slice, and `limit: 0` means "no limit" in both
    # `count` and `find`.
    count_boundaries = {
        "skip_tail_limit_more": _probe(
            lambda: database.command({"count": COLLECTION, "query": {}, "skip": scale - 100, "limit": 200})["n"]
        ),
        "skip_exact_end": _probe(lambda: database.command({"count": COLLECTION, "query": {}, "skip": scale})["n"]),
        "skip_beyond_end": _probe(lambda: database.command({"count": COLLECTION, "query": {}, "skip": scale * 2})["n"]),
        "limit_zero": _probe(lambda: database.command({"count": COLLECTION, "query": {}, "limit": 0})["n"]),
        "skip_half_limit_zero": _probe(
            lambda: database.command({"count": COLLECTION, "query": {}, "skip": scale // 2, "limit": 0})["n"]
        ),
    }
    beyond_window = _probe(lambda: [document["_id"] for document in collection.find({}, skip=scale + 1)])
    unlimited_window = _probe(lambda: len(list(collection.find({}, limit=0))))

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
        "count_all": count_all,
        "count_range": count_range,
        "count_documents_range": count_documents_range,
        "count_boundaries": count_boundaries,
        "beyond_window": beyond_window,
        "unlimited_window": unlimited_window,
        "aggregate_count": aggregate_count,
        "projected": projected,
        "first_batch": first_batch,
        "cursor_opened": cursor_id != 0 or first_batch < scale,
        "drained": drained,
        # A plain `find` scan, kept as the reference count the other probes must match.
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

            def count_1000() -> None:
                confirm(
                    gateway["count_all"] == scale,
                    f"`count` returned {gateway['count_all']}, expected {scale}",
                )
                confirm(
                    gateway["count_range"] == scale // 2,
                    f"`count` with a `$gte` query returned {gateway['count_range']}, expected {scale // 2}",
                )
                confirm(
                    gateway["count_documents_range"] == scale // 2,
                    f"`count_documents` returned {gateway['count_documents_range']}, expected {scale // 2}",
                )

            def count_skip_limit_boundary() -> None:
                expected = {
                    "skip_tail_limit_more": 100,
                    "skip_exact_end": 0,
                    "skip_beyond_end": 0,
                    "limit_zero": scale,
                    "skip_half_limit_zero": scale - scale // 2,
                }
                for name, value in expected.items():
                    confirm(
                        gateway["count_boundaries"][name] == value,
                        f"`count` boundary `{name}` returned {gateway['count_boundaries'][name]!r}, expected {value}",
                    )

            def find_skip_beyond_window() -> None:
                confirm(
                    gateway["beyond_window"] == [],
                    f"`skip` past the end returned {gateway['beyond_window']}, expected no documents",
                )
                confirm(
                    gateway["unlimited_window"] == scale,
                    f"`limit: 0` returned {gateway['unlimited_window']}/{scale} documents",
                )

            def aggregate_count_1000() -> None:
                confirm(
                    gateway["aggregate_count"] == scale // 2,
                    f"`$group` counting returned {gateway['aggregate_count']}, expected {scale // 2}",
                )

            def aggregate_project() -> None:
                expected = [{"label": f"doc-{scale - 1 - scale // 2:04d}"}]
                confirm(
                    gateway["projected"] == expected,
                    f"`$project` returned {gateway['projected']}, expected {expected}",
                )

            def batch_size() -> None:
                confirm(
                    gateway["first_batch"] == BATCH_SIZE,
                    f"`batchSize` {BATCH_SIZE} returned a first batch of {gateway['first_batch']} documents",
                )
                confirm(gateway["cursor_opened"], "a 1000-document scan must leave a live cursor after the first batch")
                confirm(gateway["drained"] == scale, f"draining the cursor yielded {gateway['drained']}/{scale} documents")

            results.extend(
                run_cases(
                    (
                        (CASES[0], sorted_window),
                        (CASES[1], filtered),
                        (CASES[2], empty_batch),
                        (CASES[3], range_filter_1000),
                        (CASES[4], full_sort_1000),
                        (CASES[5], count_1000),
                        (CASES[6], count_skip_limit_boundary),
                        (CASES[7], aggregate_count_1000),
                        (CASES[8], aggregate_project),
                        (CASES[9], batch_size),
                        (CASES[10], find_skip_beyond_window),
                    ),
                    detail=f"{scale}-document scan behaves as declared",
                )
            )
        finally:
            client.close()

        results.append(
            parity_against_oracle(
                CASES[11],
                gateway,
                oracle_mongo,
                _engine_facts,
                (
                    "sorted_window",
                    "skipped_window",
                    "filtered",
                    "empty",
                    "range_filtered",
                    "full_sorted",
                    "count_all",
                    "count_range",
                    "count_documents_range",
                    # `count_boundaries` stays out on purpose: they exercise the
                    # legacy `count` command, whose availability depends on the
                    # server version, and the gateway behaviour is pinned locally.
                    "aggregate_count",
                    "projected",
                    "first_batch",
                    "cursor_opened",
                    "drained",
                    "documents",
                    "beyond_window",
                    "unlimited_window",
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
