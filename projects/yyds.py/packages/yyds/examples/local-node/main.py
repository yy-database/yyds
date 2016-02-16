"""Initialize a node directory, inspect it, then request a bounded stop."""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path

from yyds import Node, YydsError


def inspect(directory: Path, cluster_id: str, node_id: str, stop_timeout_ms: int) -> None:
    node = Node(directory)
    node.init(cluster_id, node_id)
    print("initialized:", json.dumps(node.status(), indent=2))

    try:
        node.request_stop(timeout_ms=stop_timeout_ms)
    except YydsError as error:
        print(f"stop request skipped: {error}")
    else:
        print("stop requested")


def main() -> None:
    parser = argparse.ArgumentParser(description="Local YYDS node control-plane example.")
    parser.add_argument("--data-dir", default=None, help="Directory to use, defaults to a temporary directory.")
    parser.add_argument("--cluster-id", default="example-cluster")
    parser.add_argument("--node-id", default="example-node")
    parser.add_argument("--stop-timeout-ms", type=int, default=5000)
    arguments = parser.parse_args()

    if arguments.data_dir:
        inspect(Path(arguments.data_dir), arguments.cluster_id, arguments.node_id, arguments.stop_timeout_ms)
        return
    with tempfile.TemporaryDirectory(prefix="yyds-python-example-") as directory:
        inspect(Path(directory), arguments.cluster_id, arguments.node_id, arguments.stop_timeout_ms)


if __name__ == "__main__":
    main()
