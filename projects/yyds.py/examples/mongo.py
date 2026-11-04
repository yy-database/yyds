"""下游验收示例：真实 pymongo 对 yyds Mongo 网关做 1000 文档写入、读取、排序与删除。

期望值由本示例按写入输入自行推导（文档数、抽样文档、排序结果、删除计数、重复键码值），
不看网关内部常量。节点由 `node e2e/run.mjs` 启动并通过 `YYDS_MONGO_PORT` 传入。

设置 `YYDS_ORACLE_MONGO_URL`（例如 `mongodb://127.0.0.1:27017`）时，同一批操作会在真实
MongoDB 上重放一次并对拍事实；没有 oracle 时只输出 `UNVERIFIED`，不计作通过。
"""

from __future__ import annotations

import os
import sys

from pymongo import MongoClient
from pymongo.errors import BulkWriteError
from pymongo.server_api import ServerApi

DOCUMENTS = 1000
HOST = os.environ.get("YYDS_HOST", "127.0.0.1")
PORT = int(os.environ.get("YYDS_MONGO_PORT", "0"))
ORACLE_URL = os.environ.get("YYDS_ORACLE_MONGO_URL")
DATABASE = "example_mongo"

if PORT <= 0:
    print("YYDS_MONGO_PORT must point at a running yyds node", file=sys.stderr)
    raise SystemExit(2)


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def connect() -> MongoClient:
    return MongoClient(
        HOST,
        PORT,
        directConnection=True,
        server_api=ServerApi("1"),
        retryWrites=False,
        retryReads=False,
        serverSelectionTimeoutMS=30_000,
    )


def facts(client: MongoClient, suffix: str) -> dict[str, object]:
    database = client[DATABASE]
    collection = database[f"items_{suffix}"]
    duplicates = database[f"duplicates_{suffix}"]
    collection.delete_many({})
    duplicates.delete_many({})

    payload = [{"_id": index, "n": index, "label": f"doc-{index:04d}"} for index in range(DOCUMENTS)]
    collection.insert_many(payload, ordered=True)

    scanned = sum(1 for _ in collection.find({}))
    check(scanned == DOCUMENTS, f"scanned {scanned} documents, expected {DOCUMENTS}")

    spot = collection.find_one({"_id": DOCUMENTS // 2})
    check(spot is not None and spot["label"] == "doc-0500", f"spot document was {spot!r}")

    ordered = [document["n"] for document in collection.find({}).sort("n", 1).limit(5)]
    check(ordered == [0, 1, 2, 3, 4], f"sorted prefix was {ordered!r}")

    error_code = None
    inserted_on_error = None
    try:
        duplicates.insert_many([{"_id": 0}, {"_id": 0}], ordered=True)
    except BulkWriteError as error:
        error_code = error.details["writeErrors"][0]["code"]
        inserted_on_error = error.details["nInserted"]
    else:
        raise AssertionError("duplicate identifiers were accepted")

    removed = collection.delete_many({}).deleted_count
    check(removed == DOCUMENTS, f"deleted {removed} documents, expected {DOCUMENTS}")

    return {
        "inserted": len(payload),
        "scanned": scanned,
        "sorted-prefix": ordered,
        "removed": removed,
        "error-code": error_code,
        "inserted-on-error": inserted_on_error,
        "absent-after-delete": collection.find_one({"_id": DOCUMENTS // 2}) is None,
    }


def compare(gateway: dict[str, object], oracle: dict[str, object]) -> None:
    for field in ("inserted", "scanned", "sorted-prefix", "removed", "error-code", "inserted-on-error", "absent-after-delete"):
        check(gateway[field] == oracle[field], f"{field} differs: gateway={gateway[field]!r} oracle={oracle[field]!r}")


def main() -> int:
    failed = 0
    try:
        gateway = facts(connect(), "gateway")
    except Exception as error:  # noqa: BLE001 - the example reports any driver failure
        failed += 1
        print(f"FAIL examples/mongo.py gateway: {type(error).__name__}: {error}")
    else:
        print(f"PASS examples/mongo.py gateway {DOCUMENTS} documents")

    if ORACLE_URL:
        try:
            oracle = facts(MongoClient(ORACLE_URL, server_api=ServerApi("1"), retryWrites=False), "oracle")
            compare(gateway, oracle)
        except Exception as error:  # noqa: BLE001
            failed += 1
            print(f"FAIL examples/mongo.py parity: {type(error).__name__}: {error}")
        else:
            print("PASS examples/mongo.py parity")
    else:
        print("UNVERIFIED examples/mongo.py parity: set YYDS_ORACLE_MONGO_URL to replay against a real MongoDB")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
