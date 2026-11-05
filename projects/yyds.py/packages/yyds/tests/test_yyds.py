"""Standard-library tests for the yyds control-plane package."""

import json
import os
import tempfile
import unittest
import uuid
from pathlib import Path
from typing import Any

SKIP_REASON = ""

# Tests run against the installed wheel and never add ``src`` to ``sys.path``,
# so the source-tree shim cannot shadow the compiled ``yyds_pyo3`` package.
try:
    from yyds import Node, YydsError, version
except ImportError as error:
    Node = None  # type: ignore[assignment]
    YydsError = Exception  # type: ignore[assignment,misc]
    version = None  # type: ignore[assignment]
    SKIP_REASON = f"installed yyds wheel is required: {error}"


def status_payload(generation: str) -> dict[str, Any]:
    return {
        "version": 1,
        "generation": generation,
        "pid": 1,
        "startedAtUnixMs": 0,
        "redisAddress": "127.0.0.1:1",
        "pgsqlAddress": None,
        "mysqlAddress": None,
        "wireAddress": None,
    }


@unittest.skipUnless(SKIP_REASON == "", SKIP_REASON)
class NodeTests(unittest.TestCase):
    def setUp(self) -> None:
        self._temporary = tempfile.TemporaryDirectory(prefix="yyds-py-test-")
        self.data_dir = Path(self._temporary.name)

    def tearDown(self) -> None:
        self._temporary.cleanup()

    def write_status(self, payload: dict[str, Any]) -> None:
        (self.data_dir / "node.status.json").write_text(json.dumps(payload), encoding="utf-8")

    def test_version_reports_native_version(self) -> None:
        reported = version()
        self.assertIsInstance(reported, str)
        self.assertRegex(reported, r"^\d+\.\d+\.\d+")

    def test_init_is_idempotent_and_status_is_available(self) -> None:
        node = Node(self.data_dir)
        node.init("example-cluster", "example-node")
        node.init("example-cluster", "example-node")

        status = node.status()
        self.assertEqual(status["ownership"], "available")
        self.assertEqual(status["identity"], {"clusterId": "example-cluster", "nodeId": "example-node"})
        self.assertIsNone(status["runtime"])
        self.assertTrue(os.path.samefile(status["directory"], self.data_dir))

    def test_init_rejects_identity_change(self) -> None:
        node = Node(self.data_dir)
        node.init("example-cluster", "example-node")
        with self.assertRaises(YydsError) as raised:
            node.init("other-cluster", "example-node")
        self.assertIn("identity mismatch", str(raised.exception))

    def test_init_rejects_invalid_identity(self) -> None:
        with self.assertRaises(YydsError) as raised:
            Node(self.data_dir).init("bad cluster", "example-node")
        self.assertEqual(str(raised.exception), "invalid cluster or node identity")

    def test_status_requires_an_initialized_directory(self) -> None:
        with self.assertRaises(YydsError):
            Node(self.data_dir).status()

    def test_request_stop_requires_published_runtime_status(self) -> None:
        node = Node(self.data_dir)
        node.init("example-cluster", "example-node")
        with self.assertRaises(YydsError) as raised:
            node.request_stop()
        self.assertEqual(str(raised.exception), "node owner has not published runtime status")

    def test_request_stop_is_idempotent_for_a_generation(self) -> None:
        generation = str(uuid.uuid4())
        node = Node(self.data_dir)
        node.init("example-cluster", "example-node")
        self.write_status(status_payload(generation))

        node.request_stop()
        node.request_stop(timeout_ms=None)

        marker = self.data_dir / f"node.stop.{generation}"
        self.assertTrue(marker.is_file())

    def test_request_stop_rejects_corrupt_runtime_status(self) -> None:
        node = Node(self.data_dir)
        node.init("example-cluster", "example-node")
        self.write_status({"version": 1, "generation": "not-a-uuid"})

        with self.assertRaises(YydsError) as raised:
            node.request_stop()
        self.assertEqual(str(raised.exception), "corrupt runtime status")

    def test_wait_until_stopped_returns_when_ownership_is_available(self) -> None:
        generation = str(uuid.uuid4())
        node = Node(self.data_dir)
        node.init("example-cluster", "example-node")
        self.write_status(status_payload(generation))

        node.request_stop(timeout_ms=1000)
        self.assertEqual(node.status()["ownership"], "available")


if __name__ == "__main__":
    unittest.main()
