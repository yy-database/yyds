"""验收场景 2 — 过期 epoch 的 owner 应该 fencing 拒绝写请求。"""

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
    c = Cluster(work_dir=tmp_path, cluster_id="pytest-fencing")
    c.start()
    yield c
    c.stop()


def test_stale_epoch_reject(cluster: Cluster) -> None:
    """stale epoch fencing — 旧 owner 提交新 epoch 被拒绝。

    stub skip 直到 Rust 侧 wire 协议实现。
    """
    primary = cluster.primary()
    if primary.stub:
        pytest.skip("`yyds` CLI not found on PATH — install @yydb/yyds and `npm link`")
    # 真实路径：用过期 fencing token 写 shard-a 应该被新 primary fencing 拒绝
    raise NotImplementedError("fencing protocol not yet implemented")
