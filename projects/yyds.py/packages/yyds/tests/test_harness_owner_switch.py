"""验收场景 3 — kill primary 后新 primary 接管 owner。"""

from __future__ import annotations

import pytest
from pathlib import Path
import sys

_HERE = Path(__file__).resolve().parent
_SRC = _HERE.parent / "src"
for p in (str(_HERE), str(_SRC)):
    if p not in sys.path:
        sys.path.insert(0, p)

from harness.cluster import Cluster  # noqa: E402


@pytest.fixture
def cluster(tmp_path: Path) -> Cluster:
    c = Cluster(work_dir=tmp_path, cluster_id="pytest-switch")
    c.start()
    yield c
    c.stop()


def test_owner_switch(cluster: Cluster) -> None:
    """kill primary → 新 primary 应该接管 shard-a。

    stub skip 直到 Rust 侧 wire 协议 + fencing 实现。
    """
    primary = cluster.primary()
    if primary.stub:
        pytest.skip("`yyds` CLI not found on PATH — install @yydb/yyds and `npm link`")

    # 真实故障注入
    cluster.kill_primary()
    new_primary = cluster.secondary(1)
    cluster.restart_primary()
    # 新 primary 应能写 shard-a
    raise NotImplementedError("owner-switch protocol not yet implemented")
