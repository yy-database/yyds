"""YYDS 多节点 loopback 模拟 harness。

放在 ``packages/yyds/tests/harness/`` 下，和 ``test_yyds.py`` 同级目录。
测试入口是 ``tests/`` 目录下的 pytest（``python -m pytest tests/ -v``）。

**前置 gap**：当前 PATH 里没有 ``yyds`` CLI。``@yydb/yyds`` 包已有
``yyds start`` 节点启动入口，但未安装时 harness 自动走 stub 模式。

**硬约束**：所有二进制只能是 ``.node``（N-API binding），不独立创建 Rust bin target。
节点进程通过 ``yyds start`` CLI 启动（CLI 走 ``bin/yyds.mjs`` → ``loadYydsNative()`` → .node）。
"""

from .cluster import Cluster
from .node import Node, NodeStatus
from .ports import PortAllocator
from .catalog import CatalogWriter

__all__ = [
    "Cluster",
    "Node",
    "NodeStatus",
    "PortAllocator",
    "CatalogWriter",
]
