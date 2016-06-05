"""Node lifecycle harness for the YYDS SQL real-client suites.

One `yyds start --catalog-path` process serves both SQL gateways from a
temporary data directory. Readiness comes from the address line the CLI
publishes plus a TCP probe, never from a fixed sleep, and every failure keeps
the child stdout and stderr for the report.
"""

from __future__ import annotations

import os
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[6]  # yyds.py/packages/yyds/tests/clients → repo root
CLI_PATH = REPO_ROOT / "projects/yyds.ts/packages/yyds/bin/yyds.mjs"
SCHEMA_PATH = Path(__file__).with_name("schema.vos")
SCHEMA_VERSION = 7
DEFAULT_TIMEOUT_SECONDS = 90.0
PROBE_INTERVAL_SECONDS = 0.05

_RUNNING_LINE = re.compile(r"^yyds running pid=\d+ mode=(\S+)")


class NodeFailure(RuntimeError):
    """The node never reached a usable state."""


def check(condition: bool, message: str) -> None:
    """Raises a named failure instead of relying on `assert`, which `-O` strips."""

    if not condition:
        raise AssertionError(message)


@dataclass
class CaseResult:
    """One real-client case outcome."""

    name: str
    status: str
    detail: str = ""


def run_cases(driver_prefix: str, cases: list[tuple[str, object]]) -> list[CaseResult]:
    """Runs every `(name, callable)` pair and records failures by name."""

    results: list[CaseResult] = []
    for name, case in cases:
        label = f"{driver_prefix}/{name}"
        try:
            case()
        except Exception as error:  # noqa: BLE001 - the matrix reports any driver failure
            results.append(CaseResult(label, "fail", f"{type(error).__name__}: {error}"))
        else:
            results.append(CaseResult(label, "pass"))
    return results


def _node_executable() -> str:
    return os.environ.get("YYDS_NODE") or shutil.which("node") or "node"


def _cli_arguments(args: list[str]) -> list[str]:
    return [_node_executable(), "--experimental-strip-types", str(CLI_PATH), *args]


def _free_port(reserved: set[int]) -> int:
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as probe:
            probe.bind(("127.0.0.1", 0))
            port = probe.getsockname()[1]
        if port not in reserved:
            reserved.add(port)
            return port


def _port_open(host: str, port: int) -> bool:
    try:
        with socket.create_connection((host, port), timeout=1.0):
            return True
    except OSError:
        return False


def _run_cli(args: list[str], timeout: float) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        _cli_arguments(args),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        timeout=timeout,
    )


def _pump(stream, sink: list[str]) -> None:
    for line in stream:
        sink.append(line.rstrip("\r\n"))


@dataclass
class YydsNode:
    """A running node plus the endpoints its gateways published."""

    host: str
    pgsql_port: int
    mysql_port: int
    redis_port: int
    data_dir: Path | None
    process: subprocess.Popen | None = None
    stdout_lines: list[str] = field(default_factory=list)
    stderr_lines: list[str] = field(default_factory=list)
    attached: bool = False

    def logs(self) -> str:
        stdout = "\n".join(self.stdout_lines)
        stderr = "\n".join(self.stderr_lines)
        return f"--- node stdout ---\n{stdout}\n--- node stderr ---\n{stderr}"

    def stop(self) -> None:
        if self.attached:
            return
        process = self.process
        if process is not None and self.data_dir is not None:
            try:
                _run_cli(["stop", "--data-dir", str(self.data_dir)], 20.0)
            except (OSError, subprocess.TimeoutExpired):
                pass
            try:
                process.wait(timeout=10.0)
            except subprocess.TimeoutExpired:
                process.terminate()
                try:
                    process.wait(timeout=10.0)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=10.0)
        self.process = None
        if self.data_dir is not None and self.data_dir.exists():
            shutil.rmtree(self.data_dir, ignore_errors=True)
        self.data_dir = None

    def __enter__(self) -> "YydsNode":
        return self

    def __exit__(self, *_: object) -> None:
        self.stop()


def _parse_addresses(line: str) -> dict[str, str]:
    addresses: dict[str, str] = {}
    for token in line.split():
        key, separator, value = token.partition("=")
        if separator:
            addresses[key] = value
    return addresses


def _wait_until_ready(node: YydsNode, timeout: float) -> None:
    deadline = time.monotonic() + timeout
    announced = False
    while time.monotonic() < deadline:
        process = node.process
        if process is not None and process.poll() is not None:
            raise NodeFailure(f"yyds start exited with code {process.returncode} before readiness\n{node.logs()}")
        for line in node.stdout_lines:
            match = _RUNNING_LINE.match(line)
            if match is None:
                continue
            if match.group(1) != "sql-runtime":
                raise NodeFailure(
                    f"node started in '{match.group(1)}' mode, so the catalog never reached the SQL runtimes\n"
                    + node.logs()
                )
            addresses = _parse_addresses(line)
            pgsql = addresses.get("pgsql", "null")
            mysql = addresses.get("mysql", "null")
            if pgsql in ("", "null") or mysql in ("", "null"):
                raise NodeFailure(f"node published no SQL gateway address: {line}")
            announced = True
        if announced and _port_open(node.host, node.pgsql_port) and _port_open(node.host, node.mysql_port):
            return
        time.sleep(PROBE_INTERVAL_SECONDS)
    raise NodeFailure(f"node did not accept SQL connections within {timeout:.0f}s\n{node.logs()}")


def start_node(*, timeout: float = DEFAULT_TIMEOUT_SECONDS, schema_path: Path = SCHEMA_PATH) -> YydsNode:
    """Starts one node with both SQL gateways mounted from a published catalog.

    `schema_path` selects the VOS catalog the node publishes. The binary payload
    suite needs its own `payloads` table, so it passes its own schema file while
    the regular matrix keeps `schema.vos`.
    """

    reserved: set[int] = set()
    redis_port = _free_port(reserved)
    pgsql_port = _free_port(reserved)
    mysql_port = _free_port(reserved)
    data_dir = Path(tempfile.mkdtemp(prefix="yyds-sql-clients-"))
    cluster_id = f"cluster-{os.getpid()}-{int(time.time())}"
    node_id = f"node-{os.getpid()}"

    node = YydsNode(
        host="127.0.0.1",
        pgsql_port=pgsql_port,
        mysql_port=mysql_port,
        redis_port=redis_port,
        data_dir=data_dir,
    )

    try:
        init = _run_cli(["init", "--data-dir", str(data_dir), "--cluster-id", cluster_id, "--node-id", node_id], 30.0)
        if init.returncode != 0:
            raise NodeFailure(f"yyds init exited with code {init.returncode}: {init.stderr.strip()}")
        catalog_path = data_dir / "catalog.yyds"
        publish = _run_cli(
            [
                "catalog",
                "--catalog-path",
                str(catalog_path),
                "--schema-file",
                str(schema_path),
                "--schema-version",
                str(SCHEMA_VERSION),
            ],
            30.0,
        )
        if publish.returncode != 0:
            raise NodeFailure(f"yyds catalog exited with code {publish.returncode}: {publish.stderr.strip()}")

        node.process = subprocess.Popen(
            _cli_arguments(
                [
                    "start",
                    "--data-dir",
                    str(data_dir),
                    "--cluster-id",
                    cluster_id,
                    "--node-id",
                    node_id,
                    "--catalog-path",
                    str(catalog_path),
                    "--redis-port",
                    str(redis_port),
                    "--pgsql-port",
                    str(pgsql_port),
                    "--mysql-port",
                    str(mysql_port),
                ]
            ),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        assert node.process.stdout is not None and node.process.stderr is not None
        threading.Thread(target=_pump, args=(node.process.stdout, node.stdout_lines), daemon=True).start()
        threading.Thread(target=_pump, args=(node.process.stderr, node.stderr_lines), daemon=True).start()
        _wait_until_ready(node, timeout)
        return node
    except Exception:
        node.stop()
        raise


def attach_node(*, host: str, pgsql_port: int, mysql_port: int) -> YydsNode:
    """Wraps an already running node so the suites can run without lifecycle control."""

    return YydsNode(
        host=host,
        pgsql_port=pgsql_port,
        mysql_port=mysql_port,
        redis_port=0,
        data_dir=None,
        attached=True,
    )


def main() -> int:
    """Prints the endpoints a freshly started node published, for manual runs."""

    try:
        node = start_node()
    except NodeFailure as error:
        print(error, file=sys.stderr)
        return 1
    try:
        print(f"host={node.host} pgsql_port={node.pgsql_port} mysql_port={node.mysql_port}")
        print("node is running, press Ctrl+C to stop")
        while True:
            time.sleep(1.0)
    except KeyboardInterrupt:
        return 0
    finally:
        node.stop()


if __name__ == "__main__":
    raise SystemExit(main())
