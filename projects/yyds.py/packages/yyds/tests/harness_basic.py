"""验收场景：基本写入 + 读回 + sanity checks。

pytest 用 ``python -m pytest tests/ -v`` 从 tests/ 目录发现。
harness 模块在 tests/harness/ 子包下。
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

# 让 tests/ 根目录能 import harness 子包 与 yyds 宿主包
_HERE = Path(__file__).resolve().parent
_SRC = _HERE.parent / "src"
for p in (str(_HERE), str(_SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)

from harness.cluster import Cluster  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s: %(message)s")


@pytest.fixture
def cluster(tmp_path: Path) -> Cluster:
    """三节点 loopback 集群（每个 node 独立 data-dir，共享 .yyds catalog）。"""
    c = Cluster(work_dir=tmp_path, cluster_id="pytest-local")
    c.start()
    yield c
    c.stop()


def test_basic_write_read(cluster: Cluster) -> None:
    """用例 1 — 基本写入 + 读回。

    真实路径：primary 上 CREATE TABLE → INSERT → SELECT。
    Stub 路径：PATH 里没有 ``yyds`` CLI 时 skip。
    """
    primary = cluster.primary()
    if primary.stub:
        pytest.skip("`yyds` CLI not found on PATH — install @yydb/yyds and `npm link`")
    primary.execute_sql("CREATE TABLE t (id INT, body BYTES)")
    primary.execute_sql("INSERT INTO t VALUES (1, ?)", [b"\x00\x01\x02"])
    rows = primary.execute_sql("SELECT body FROM t WHERE id = 1")
    assert rows[0]["body"] == b"\x00\x01\x02"


def test_cluster_three_nodes_exist(cluster: Cluster) -> None:
    """Sanity check：Cluster fixture 必须有 3 个 node。"""
    assert len(cluster.nodes) == 3
    assert cluster.primary().node_id == "node-primary"
    assert cluster.secondary(1).node_id == "node-1"
    assert cluster.secondary(2).node_id == "node-2"


def test_cluster_ports_distinct(cluster: Cluster) -> None:
    """Sanity check：三个 node 的 wire-port 互不相同。"""
    ports = [n.wire_port for n in cluster.nodes]
    assert len(set(ports)) == 3
    for p in ports:
        assert 1 <= p <= 65535


def test_catalog_published(cluster: Cluster) -> None:
    """Sanity check：Cluster.start() 后 catalog 应该在 epoch=1。"""
    assert cluster.catalog.epoch == 1
    assert cluster.catalog_path.exists()
