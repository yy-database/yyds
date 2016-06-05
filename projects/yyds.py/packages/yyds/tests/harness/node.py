"""单个 YYDS node 的生命周期管理。

``Node`` 封装了：
- 一个独立的 data-dir（存放 ``node.json``、``node.lock``、``.yykv`` shard 文件）
- 一个独立的 wire-port（node-to-node transport）
- 通过 ``@yydb/yyds`` 包的 CLI（``yyds start ...``）启动，底层加载 ``.node`` native binding

**真实路径**：检测到 ``yyds`` CLI 可用时，``subprocess.Popen(["yyds", "start", ...])`` 启动 node。
**Stub 路径**：PATH 里没有 ``yyds`` CLI 时，置 ``self.stub = True``。
"""

from __future__ import annotations

import enum
import logging
import shutil
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)


class NodeStatus(enum.Enum):
    """Node 进程的当前状态。"""
    RUNNING = "running"   # 进程活着，health check 通过
    DEAD = "dead"         # 进程已退出或被 kill
    FENCED = "fenced"     # 进程活着但 owner 已被切走，不再接受写
    STUB = "stub"         # 从未启动过（没有 yyds CLI）


@dataclass
class Node:
    """一个 YYDS node 进程的完整描述。"""

    node_id: str
    cluster_id: str
    wire_port: int
    data_dir: Path

    status: NodeStatus = NodeStatus.STUB
    process: Optional[subprocess.Popen] = field(default=None, repr=False)
    stub: bool = False

    # ------------------------------------------------------------------
    # 启动 / 停止
    # ------------------------------------------------------------------

    def start(self, catalog_path: Path) -> None:
        if self._yyds_cli_available():
            self._start_real(catalog_path)
        else:
            self._start_stub()

    def _yyds_cli_available(self) -> bool:
        """探测 PATH 里有没有 ``yyds`` 命令。"""
        return shutil.which("yyds") is not None

    def _start_real(self, catalog_path: Path) -> None:
        self.data_dir.mkdir(parents=True, exist_ok=True)
        args = self._build_command(catalog_path)
        log.info("starting real node: %s", " ".join(args))
        try:
            self.process = subprocess.Popen(
                args, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
            )
        except FileNotFoundError:
            log.warning("yyds CLI not found on PATH, falling back to stub")
            self._start_stub()
            return

        deadline = time.monotonic() + 5.0
        while time.monotonic() < deadline:
            if self.process.poll() is not None:
                log.error("node exited immediately with code %d", self.process.returncode)
                self.status = NodeStatus.DEAD
                return
            if self.health_check():
                self.status = NodeStatus.RUNNING
                log.info("node %s ready on port %d", self.node_id, self.wire_port)
                return
            time.sleep(0.1)

        log.warning("node %s health check timed out, marking as running anyway", self.node_id)
        self.status = NodeStatus.RUNNING

    def _start_stub(self) -> None:
        self.stub = True
        self.status = NodeStatus.STUB
        self.data_dir.mkdir(parents=True, exist_ok=True)
        log.warning(
            "node %s: `yyds` CLI not found on PATH — running in stub mode. "
            "Install @yydb/yyds and `npm link @yydb/yyds` to enable real mode.",
            self.node_id,
        )

    def _build_command(self, catalog_path: Path) -> list[str]:
        """构建 ``yyds start`` 参数。

        对应 ``@yydb/yyds`` 的 bin/yyds.mjs 实现。
        """
        return [
            "yyds", "start",
            "--data-dir", str(self.data_dir),
            "--cluster-id", self.cluster_id,
            "--node-id", self.node_id,
            "--catalog-path", str(catalog_path),
            "--wire-port", str(self.wire_port),
        ]

    def stop(self, timeout: float = 5.0) -> None:
        if self.process is None:
            self.status = NodeStatus.DEAD
            return
        if self.process.poll() is not None:
            self.status = NodeStatus.DEAD
            self.process = None
            return
        try:
            self.process.terminate()
            try:
                self.process.wait(timeout=timeout)
            except subprocess.TimeoutExpired:
                log.warning("node %s did not exit gracefully, killing", self.node_id)
                self.process.kill()
                self.process.wait(timeout=2.0)
        finally:
            self.status = NodeStatus.DEAD
            self.process = None

    def kill(self) -> None:
        if self.process is not None and self.process.poll() is None:
            log.info("hard killing node %s (pid %d)", self.node_id, self.process.pid)
            self.process.kill()
            try:
                self.process.wait(timeout=2.0)
            except subprocess.TimeoutExpired:
                pass
        self.status = NodeStatus.DEAD
        self.process = None

    # ------------------------------------------------------------------
    # 运行时 API
    # ------------------------------------------------------------------

    def health_check(self) -> bool:
        if self.stub or self.process is None or self.process.poll() is not None:
            return False
        import socket as _socket
        try:
            with _socket.create_connection(("127.0.0.1", self.wire_port), timeout=0.5):
                return True
        except OSError:
            return False

    def execute_sql(self, sql: str, params: Optional[list] = None) -> list[dict]:
        if self.stub:
            raise NotImplementedError(
                "`yyds` CLI not on PATH — install @yydb/yyds and `npm link` to enable real node start"
            )
        raise NotImplementedError("wire protocol for SQL execution not yet implemented")

    def execute_vos_operation(self, name: str, args: dict) -> dict:
        if self.stub:
            raise NotImplementedError(
                "`yyds` CLI not on PATH — install @yydb/yyds and `npm link` to enable real node start"
            )
        raise NotImplementedError("wire protocol for VOS operation not yet implemented")

    def wipe_data_dir(self) -> None:
        if self.data_dir.exists():
            shutil.rmtree(self.data_dir, ignore_errors=True)

    def __enter__(self) -> "Node":
        return self

    def __exit__(self, *exc_info: object) -> None:
        self.stop()
