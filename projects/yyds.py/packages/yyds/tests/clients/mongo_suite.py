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


def _case_unsupported_commands(node: YydsNode) -> None:
    client = _client(node)
    try:
        for command in ({"count": "py-crud"}, {"drop": "py-crud"}):
            try:
                client["yyds"].command(command)
            except OperationFailure as error:
                check(error.code == 59, f"{command} must answer code 59, got {error.code}: {error}")
            else:
                raise AssertionError(f"{command} must be refused with code 59")
    finally:
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
            ("unsupported-commands", lambda: _case_unsupported_commands(node)),
            ("invalid-namespace", lambda: _case_invalid_namespace(node)),
        ]
        return run_cases("pymongo", cases)
    finally:
        if owned:
            node.stop()
