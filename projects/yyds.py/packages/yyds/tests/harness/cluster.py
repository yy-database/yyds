"""三节点 loopback 集群编排。

``Cluster`` 组合 ``PortAllocator`` + ``CatalogWriter`` + 3 个 ``Node`` 实例。
"""

from __future__ import annotations

import logging
from pathlib import Path

from .catalog import CatalogWriter
from .node import Node, NodeStatus
from .ports import PortAllocator

log = logging.getLogger(__name__)

NODE_ID_PRIMARY = "node-primary"
NODE_ID_SECONDARY_1 = "node-1"
NODE_ID_SECONDARY_2 = "node-2"


class Cluster:
    """三节点 loopback 集群。"""

    def __init__(self, work_dir: Path, cluster_id: str = "harness-local") -> None:
        self._work_dir = Path(work_dir)
        self._cluster_id = cluster_id
        self._work_dir.mkdir(parents=True, exist_ok=True)
        self._catalog_path = self._work_dir / ".yyds"
        self._data_dirs = {
            NODE_ID_PRIMARY: self._work_dir / NODE_ID_PRIMARY,
            NODE_ID_SECONDARY_1: self._work_dir / NODE_ID_SECONDARY_1,
            NODE_ID_SECONDARY_2: self._work_dir / NODE_ID_SECONDARY_2,
        }
        self._ports = PortAllocator()
        allocated = self._ports.allocate(3)
        self._node_ports = {
            NODE_ID_PRIMARY: allocated[0],
            NODE_ID_SECONDARY_1: allocated[1],
            NODE_ID_SECONDARY_2: allocated[2],
        }
        self._catalog = CatalogWriter(self._catalog_path)
        self.nodes: list[Node] = [
            Node(
                node_id=nid,
                cluster_id=cluster_id,
                wire_port=self._node_ports[nid],
                data_dir=self._data_dirs[nid],
            )
            for nid in (NODE_ID_PRIMARY, NODE_ID_SECONDARY_1, NODE_ID_SECONDARY_2)
        ]

    # ------------------------------------------------------------------
    # 启动 / 停止
    # ------------------------------------------------------------------

    def start(self, catalog_epoch: int = 1) -> None:
        self._catalog.publish_epoch(
            catalog_epoch,
            owners=[
                ("shard-a", NODE_ID_PRIMARY, 1),
                ("shard-b", NODE_ID_SECONDARY_1, 1),
                ("shard-c", NODE_ID_SECONDARY_2, 1),
            ],
        )
        for node in self.nodes:
            node.start(self._catalog_path)
        self._log_status()

    def stop(self) -> None:
        for node in self.nodes:
            node.stop()
        self._ports.release_all()
        log.info("cluster stopped")

    def kill_primary(self) -> None:
        self.primary().kill()

    def restart_primary(self) -> None:
        primary = self.primary()
        primary.wipe_data_dir()
        primary.start(self._catalog_path)

    # ------------------------------------------------------------------
    # Node 访问器
    # ------------------------------------------------------------------

    def primary(self) -> Node:
        return self._find(NODE_ID_PRIMARY)

    def secondary(self, n: int) -> Node:
        if n == 1:
            return self._find(NODE_ID_SECONDARY_1)
        if n == 2:
            return self._find(NODE_ID_SECONDARY_2)
        raise IndexError(f"secondary index must be 1 or 2, got {n}")

    def _find(self, node_id: str) -> Node:
        for node in self.nodes:
            if node.node_id == node_id:
                return node
        raise KeyError(f"node {node_id} not found")

    # ------------------------------------------------------------------
    # 只读属性
    # ------------------------------------------------------------------

    @property
    def catalog(self) -> CatalogWriter:
        return self._catalog

    @property
    def catalog_path(self) -> Path:
        return self._catalog_path

    @property
    def base_port(self) -> int:
        return min(self._node_ports.values())

    @property
    def all_stub(self) -> bool:
        return all(n.stub for n in self.nodes)

    def _log_status(self) -> None:
        for node in self.nodes:
            log.info(
                "  node %s  port=%d  status=%s  stub=%s",
                node.node_id, node.wire_port, node.status.value, node.stub,
            )

    def __enter__(self) -> "Cluster":
        self.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()
