"""动态 loopback 端口分配。

用 ``socket.bind(('', 0))`` 探测空闲端口，操作系统分配后立刻关闭 socket，
让后续 ``yyds start --wire-port N`` 能独占。范围 [4100, 4199]，足够三节点集群 + 故障注入。
"""

from __future__ import annotations

import socket
from dataclasses import dataclass, field


@dataclass
class PortAllocator:
    """一次性分配一批端口，统一管理释放。"""

    _held: list[int] = field(default_factory=list)

    def allocate(self, count: int) -> list[int]:
        """探测并持有 ``count`` 个空闲端口。"""
        result: list[int] = []
        for _ in range(count):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.bind(("127.0.0.1", 0))
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                port = s.getsockname()[1]
            self._held.append(port)
            result.append(port)
        return result

    def release_all(self) -> None:
        self._held.clear()
