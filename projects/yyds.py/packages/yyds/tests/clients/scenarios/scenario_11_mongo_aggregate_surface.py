"""Scenario 11 — the aggregation surface: implemented stages, refused stages, boundaries.

Target feature: `aggregate` stage admission.
Implementation risk: a gateway that answers an empty or unfiltered batch for a stage it
never implemented is indistinguishable from a correct empty result, so every unsupported
stage is pinned to code `59` — including one that follows an implemented `$match`, where a
partial reply would be the tempting shortcut. The implemented stages are pinned by exact
documents at scale: `$group` on empty input, `$project` exclusion over 1000 documents, and
a `$match` / `$group` / `$project` chain.
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

SCENARIO_ID = "11-mongo-aggregate-surface"
CASES = (
    "11-mongo-aggregate-surface/mongo/unimplemented-stages-59",
    "11-mongo-aggregate-surface/mongo/rejected-after-match-59",
    "11-mongo-aggregate-surface/mongo/group-empty-input",
    "11-mongo-aggregate-surface/mongo/project-exclusion-1000",
    "11-mongo-aggregate-surface/mongo/group-project-chain",
    "11-mongo-aggregate-surface/mongo/engine-parity",
)

COLLECTION = "aggregate-scale"
EMPTY_COLLECTION = "aggregate-empty"

# Every stage below is legal MongoDB syntax the gateway deliberately refuses. Names
# are kept so a failure says which stage stopped being rejected.
REFUSED_STAGES = {
    "$lookup": {"$lookup": {"from": COLLECTION, "localField": "n", "foreignField": "n", "as": "joined"}},
    "$sort": {"$sort": {"n": 1}},
    "$limit": {"$limit": 10},
    "$skip": {"$skip": 10},
    "$unwind": {"$unwind": "$label"},
    "$addFields": {"$addFields": {"extra": 1}},
    "$count": {"$count": "total"},
    "$out": {"$out": COLLECTION},
    "empty-stage": {},
}


def _probe(call):
    """Runs one aggregate, turning a driver failure into a fact the case can name."""
    try:
        return call()
    except Exception as error:  # noqa: BLE001 - the case reports the driver error
        return f"error: {type(error).__name__}: {error}"


def _facts(database, scale: int, *, probe_refusals: bool) -> dict:
    collection = database[COLLECTION]
    collection.delete_many({})
    database[EMPTY_COLLECTION].delete_many({})
    collection.insert_many(
        [{"_id": index, "n": scale - 1 - index, "label": f"doc-{index:04d}"} for index in range(scale)],
        ordered=True,
    )

    refused: dict = {}
    refused_after_match = None
    if probe_refusals:
        refused = {
            name: command_error_code(database, {"aggregate": COLLECTION, "pipeline": [stage], "cursor": {}})
            for name, stage in REFUSED_STAGES.items()
        }
        # An unsupported stage after an implemented `$match` must still answer 59: a
        # partially executed pipeline would look like a short but valid batch.
        refused_after_match = command_error_code(
            database,
            {
                "aggregate": COLLECTION,
                "pipeline": [{"$match": {"n": {"$gte": 0}}}, {"$sort": {"n": 1}}],
                "cursor": {},
            },
        )

    group_empty = _probe(
        lambda: [
            dict(document)
            for document in database[EMPTY_COLLECTION].aggregate(
                [{"$group": {"_id": None, "total": {"$sum": 1}}}]
            )
        ]
    )

    excluded = _probe(
        lambda: [dict(document) for document in collection.aggregate([{"$project": {"_id": 0}}])]
    )
    if isinstance(excluded, list):
        excluded_count = len(excluded)
        excluded_with_id = sum(1 for document in excluded if "_id" in document)
        excluded_with_n = sum(1 for document in excluded if "n" in document)
        excluded_labels = sorted(str(document.get("label")) for document in excluded)
    else:
        excluded_count = excluded_with_id = excluded_with_n = excluded
        excluded_labels = excluded

    group_project = _probe(
        lambda: [
            dict(document)
            for document in collection.aggregate(
                [
                    {"$match": {"n": {"$gte": scale // 2}}},
                    {"$group": {"_id": None, "total": {"$sum": 1}}},
                    {"$project": {"_id": 0, "total": 1}},
                ]
            )
        ]
    )

    return {
        "refused": refused,
        "refused_after_match": refused_after_match,
        "group_empty": group_empty,
        "excluded_count": excluded_count,
        "excluded_with_id": excluded_with_id,
        "excluded_with_n": excluded_with_n,
        "excluded_labels": excluded_labels,
        "group_project": group_project,
    }


def _engine_facts(client: MongoClient, scale: int) -> dict:
    database = client["yyds_parity_s11"]
    try:
        # A real engine implements the stages this gateway refuses, so only the
        # implemented-stage facts are comparable.
        return _facts(database, scale, probe_refusals=False)
    finally:
        client.drop_database("yyds_parity_s11")


def run(scale: int = 1000) -> list[CaseResult]:
    node = start_mongo_node()
    results: list[CaseResult] = []
    gateway: dict = {}
    try:
        client = mongo_client(node.host, node.mongo_port)
        try:
            gateway = _facts(client["yyds"], scale, probe_refusals=True)

            def refused_stages() -> None:
                for name, code in gateway["refused"].items():
                    confirm(
                        code == 59,
                        f"the `{name}` stage answered code {code}, expected 59",
                    )

            def refused_after_match() -> None:
                confirm(
                    gateway["refused_after_match"] == 59,
                    f"`$sort` following `$match` answered code {gateway['refused_after_match']}, expected 59",
                )

            def group_empty() -> None:
                confirm(
                    gateway["group_empty"] == [],
                    f"`$group` over an empty collection returned {gateway['group_empty']}, expected no documents",
                )

            def project_exclusion() -> None:
                confirm(
                    gateway["excluded_count"] == scale,
                    f"`$project` exclusion returned {gateway['excluded_count']}/{scale} documents",
                )
                confirm(
                    gateway["excluded_with_id"] == 0,
                    f"`$project` with `_id: 0` kept `_id` on {gateway['excluded_with_id']} documents",
                )
                confirm(
                    gateway["excluded_with_n"] == scale,
                    f"`$project` exclusion dropped `n` on {scale - gateway['excluded_with_n']} documents",
                )
                expected_labels = [f"doc-{index:04d}" for index in range(scale)]
                confirm(
                    gateway["excluded_labels"] == expected_labels,
                    "`$project` exclusion did not keep the whole 1000-document set",
                )

            def group_project_chain() -> None:
                confirm(
                    gateway["group_project"] == [{"total": scale // 2}],
                    f"`$match` / `$group` / `$project` returned {gateway['group_project']}, expected one `total` of {scale // 2}",
                )

            results.extend(
                run_cases(
                    (
                        (CASES[0], refused_stages),
                        (CASES[1], refused_after_match),
                        (CASES[2], group_empty),
                        (CASES[3], project_exclusion),
                        (CASES[4], group_project_chain),
                    ),
                    detail=f"aggregate surface checked at scale {scale}",
                )
            )
        finally:
            client.close()

        results.append(
            parity_against_oracle(
                CASES[5],
                gateway,
                oracle_mongo,
                _engine_facts,
                (
                    "group_empty",
                    "excluded_count",
                    "excluded_with_id",
                    "excluded_with_n",
                    "excluded_labels",
                    "group_project",
                    # `refused` and `refused_after_match` stay out on purpose: a real
                    # server implements `$sort` / `$limit` / `$unwind` while this
                    # gateway declares 59 for every stage outside `$match`, `$group`
                    # and `$project`.
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
