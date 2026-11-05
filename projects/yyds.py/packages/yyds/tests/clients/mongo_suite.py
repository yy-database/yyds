"""MongoDB real-client suite driven by pymongo against a node started by `yyds start`.

The gateway answers OP_MSG only and reads write payloads from document-sequence
sections, which is exactly how PyMongo encodes `insert.documents` and
`delete.deletes`. The client pins the stable API marker because that is what keeps
PyMongo on OP_MSG for the `hello` handshake instead of the legacy OP_QUERY probe
this gateway does not implement.
"""

from __future__ import annotations

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError, OperationFailure
from pymongo.server_api import ServerApi

from harness import CaseResult, NodeFailure, YydsNode, check, run_cases, start_mongo_node


def _client(node: YydsNode) -> MongoClient:
    return MongoClient(
        node.host,
        node.mongo_port,
        directConnection=True,
        server_api=ServerApi("1"),
        serverSelectionTimeoutMS=20_000,
        connectTimeoutMS=20_000,
        socketTimeoutMS=20_000,
        retryWrites=False,
        retryReads=False,
    )


def _case_handshake(node: YydsNode) -> None:
    client = _client(node)
    try:
        check(client.admin.command("ping").get("ok") == 1, "ping must answer ok: 1")
        hello = client.admin.command("hello")
        check(hello.get("isWritablePrimary") is True, "hello must advertise a writable primary")
        check(hello.get("minWireVersion") == 0, "hello must report minWireVersion 0")
        check(hello.get("maxWireVersion") == 17, "hello must report maxWireVersion 17")
    finally:
        client.close()


def _case_crud(node: YydsNode) -> None:
    client = _client(node)
    collection = client["yyds"]["py-crud"]
    try:
        # Insert payloads travel in a document-sequence section, so this proves the
        # section folding and not just the inline array path.
        collection.insert_one({"_id": "fixed", "v": 1})
        check(collection.find_one({"_id": "fixed"}) == {"_id": "fixed", "v": 1}, "the inserted document must round-trip")

        # The real server rejects a repeated `_id` with E11000 and keeps the original.
        try:
            collection.insert_one({"_id": "fixed", "v": 9})
        except DuplicateKeyError as error:
            check(error.code == 11000, f"a duplicate _id must answer code 11000, got {error.code}")
        else:
            raise AssertionError("a duplicate _id must be refused with code 11000")
        check(collection.find_one({"_id": "fixed"}) == {"_id": "fixed", "v": 1}, "the rejected insert must not overwrite")

        generated = collection.insert_one({"v": 2}).inserted_id
        check(generated is not None, "the gateway must assign an _id when one is missing")
        check(
            collection.find_one({"_id": generated}) == {"_id": generated, "v": 2},
            "a generated _id must address the stored document",
        )

        documents = list(collection.find({}))
        check(len(documents) == 2, f"find without a filter must scan the namespace, got {len(documents)} documents")

        check(collection.delete_one({"_id": "fixed"}).deleted_count == 1, "delete by _id must report one removal")
        check(collection.find_one({"_id": "fixed"}) is None, "the deleted document must be gone")
        check(collection.delete_one({"v": 2}).deleted_count == 1, "delete without _id must scan and remove")
        check(list(collection.find({})) == [], "the namespace must be empty after both deletes")
    finally:
        client.close()


def _case_count_and_aggregate(node: YydsNode) -> None:
    client = _client(node)
    collection = client["yyds"]["py-count"]
    try:
        collection.insert_many([{"a": 1}, {"a": 2}])
        counted = client["yyds"].command({"count": "py-count"})
        check(counted.get("ok") == 1, f"count must answer ok: 1, got {counted}")
        check(counted.get("n") == 2, f"count must report 2 documents, got {counted.get('n')}")

        aggregate = client["yyds"].command({"aggregate": "py-count", "pipeline": [], "cursor": {}})
        check(aggregate.get("ok") == 1, f"aggregate must answer ok: 1, got {aggregate}")
        first_batch = aggregate.get("cursor", {}).get("firstBatch", [])
        check(len(first_batch) == 2, f"aggregate must return 2 documents, got {len(first_batch)}")

        # `drop` is still unimplemented and must keep the gateway's unsupported code.
        try:
            client["yyds"].command({"drop": "py-count"})
        except OperationFailure as error:
            check(error.code == 59, f"drop must answer code 59, got {error.code}: {error}")
        else:
            raise AssertionError("drop must be refused with code 59")
    finally:
        client.close()


def _case_aggregate_empty_group(node: YydsNode) -> None:
    client = _client(node)
    collection = client["yyds"]["py-agg-group"]
    try:
        collection.delete_many({})
        empty = list(collection.aggregate([{"$group": {"_id": None, "n": {"$sum": 1}}}]))
        check(empty == [], f"a $group over no input must yield an empty batch, got {empty!r}")

        # A non-empty control proves the empty reply comes from the input, not a stub.
        collection.insert_many([{"_id": "g1", "v": 1}, {"_id": "g2", "v": 2}])
        counted = list(collection.aggregate([{"$group": {"_id": None, "n": {"$sum": 1}}}]))
        check(counted == [{"_id": None, "n": 2}], f"the same $group must fold two documents into one, got {counted!r}")
    finally:
        collection.delete_many({})
        client.close()


def _case_aggregate_projection_exclusion(node: YydsNode) -> None:
    client = _client(node)
    collection = client["yyds"]["py-agg-projection"]
    try:
        collection.delete_many({})
        collection.insert_many(
            [
                {"_id": "p1", "keep": "visible", "secret": "hidden"},
                {"_id": "p2", "keep": "other", "secret": "hidden"},
            ]
        )

        # An exclusion spec drops only the named field and keeps `_id` by default.
        masked = list(collection.aggregate([{"$project": {"secret": 0}}]))
        check(
            masked == [{"_id": "p1", "keep": "visible"}, {"_id": "p2", "keep": "other"}],
            f"excluding `secret` must keep everything else, got {masked!r}",
        )

        # `_id: 0` is a second exclusion rule and removes the identifier too.
        no_id = list(collection.aggregate([{"$project": {"_id": 0, "secret": 0}}]))
        check(
            no_id == [{"keep": "visible"}, {"keep": "other"}],
            f"excluding `_id` and `secret` must leave only `keep`, got {no_id!r}",
        )
    finally:
        collection.delete_many({})
        client.close()


def _case_cursor_batching_and_reclaim(node: YydsNode) -> None:
    client = _client(node)
    collection = client["yyds"]["py-cursor"]
    try:
        ids = [f"py-cursor-{index}" for index in range(5)]
        collection.insert_many([{"_id": key, "v": index} for index, key in enumerate(ids)])

        # A batch smaller than the result set forces the driver through `getMore`
        # frames until the server reports cursor id 0.
        observed = [document["_id"] for document in collection.find({}, batch_size=2)]
        check(observed == ids, f"a batched find must return every document in order, got {observed!r}")

        # Send the cursor commands directly so the id is observable: a killed cursor
        # must leave the server, and a later `getMore` answers cursor id not found.
        first = client["yyds"].command({"find": "py-cursor", "batchSize": 2})
        cursor = first.get("cursor", {})
        cursor_id = cursor.get("id")
        check(isinstance(cursor_id, int) and cursor_id != 0, f"a partial find must return a live cursor id, got {cursor_id!r}")
        check(len(cursor.get("firstBatch", [])) == 2, "the first batch must carry the requested size")

        killed = client["yyds"].command({"killCursors": "py-cursor", "cursors": [cursor_id]})
        check(killed.get("ok") == 1, f"killCursors must answer ok: 1, got {killed}")
        check(cursor_id in killed.get("cursorsKilled", []), f"killCursors must report the reclaimed id, got {killed}")
        try:
            client["yyds"].command({"getMore": cursor_id, "collection": "py-cursor"})
        except OperationFailure as error:
            check(error.code == 43, f"getMore on a reclaimed cursor must answer code 43, got {error.code}: {error}")
        else:
            raise AssertionError("getMore on a reclaimed cursor must be refused")

        # An id the server never issued is reported as not found instead of faked.
        absent = 999_999_999
        missing = client["yyds"].command({"killCursors": "py-cursor", "cursors": [absent]})
        check(missing.get("ok") == 1, f"killCursors must still answer ok: 1, got {missing}")
        check(absent in missing.get("cursorsNotFound", []), f"an unknown cursor id must be reported missing, got {missing}")
    finally:
        collection.delete_many({})
        client.close()


def _case_invalid_namespace(node: YydsNode) -> None:
    client = _client(node)
    try:
        for command in (
            {"insert": "", "documents": [{"v": 1}]},
            {"find": "", "filter": {}},
            {"delete": "", "deletes": [{"q": {}, "limit": 0}]},
        ):
            try:
                client["yyds"].command(command)
            except OperationFailure as error:
                check(error.code == 73, f"{command} must answer code 73, got {error.code}: {error}")
            else:
                raise AssertionError(f"{command} must be refused with code 73")
    finally:
        client.close()


def run_mongo(node: YydsNode | None = None) -> list[CaseResult]:
    """Runs the pymongo suite. The node is owned unless one is passed in."""

    owned = node is None
    if node is None:
        node = start_mongo_node()
    try:
        cases: list[tuple[str, object]] = [
            ("handshake", lambda: _case_handshake(node)),
            ("crud", lambda: _case_crud(node)),
            ("count-and-aggregate", lambda: _case_count_and_aggregate(node)),
            ("aggregate-empty-group", lambda: _case_aggregate_empty_group(node)),
            ("aggregate-projection-exclusion", lambda: _case_aggregate_projection_exclusion(node)),
            ("cursor-batching-and-reclaim", lambda: _case_cursor_batching_and_reclaim(node)),
            ("invalid-namespace", lambda: _case_invalid_namespace(node)),
        ]
        return run_cases("pymongo", cases)
    finally:
        if owned:
            node.stop()
