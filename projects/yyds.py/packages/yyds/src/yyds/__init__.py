"""Python control-plane client for local YYDS nodes."""

from __future__ import annotations

import json
import os
from typing import Any

from ._native import node_init, node_request_stop, node_status, node_wait_until_stopped, version
from .errors import YydsError

__all__ = ["Node", "YydsError", "version"]


class Node:
    """Synchronous control-plane handle for one local node directory."""

    def __init__(self, data_dir: str | os.PathLike[str]) -> None:
        self._data_dir = os.fspath(data_dir)

    @property
    def data_dir(self) -> str:
        """Directory this handle inspects."""
        return self._data_dir

    def init(self, cluster_id: str, node_id: str) -> None:
        """Creates the node identity and validates the directory."""
        node_init(self._data_dir, cluster_id, node_id)

    def status(self) -> dict[str, Any]:
        """Returns identity, ownership and published runtime status."""
        return json.loads(node_status(self._data_dir))

    def request_stop(self, timeout_ms: int | None = 5000) -> None:
        """Writes the stop marker and optionally waits for the node to release ownership."""
        node_request_stop(self._data_dir)
        if timeout_ms is not None:
            node_wait_until_stopped(self._data_dir, timeout_ms)
