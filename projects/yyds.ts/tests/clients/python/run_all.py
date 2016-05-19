"""Runs the YYDS SQL real-client suites against a real node.

Without ports the script starts its own node from a temporary data directory,
publishes the VOS catalog, waits for the SQL runtimes to accept connections,
and cleans up afterwards. Passing `--postgresql-port` and `--mysql-port`
reuses an already running node instead.

`--suite payloads` runs the binary payload cases against a node published from
`payload_schema.vos`, so the regular matrix keeps its own `users` catalog.

Exit codes: 0 every case passed, 1 at least one case failed, 2 the node never
became usable.
"""

from __future__ import annotations

import argparse
import sys

import binary_payload_suite
import mysql_suite
import postgres_suite
from harness import CaseResult, NodeFailure, attach_node, start_node

GATEWAYS = {"postgresql": postgres_suite.run_postgresql, "mysql": mysql_suite.run_mysql}
SUITES = ("matrix", "payloads")


def _parse_arguments(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="run the YYDS SQL real-client suites")
    parser.add_argument("--suite", choices=SUITES, default="matrix")
    parser.add_argument("--gateway", choices=[*GATEWAYS, "all"], default="all")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--postgresql-port", type=int)
    parser.add_argument("--mysql-port", type=int)
    parser.add_argument("--timeout", type=float, default=90.0)
    arguments = parser.parse_args(argv)
    if arguments.suite == "payloads" and (arguments.gateway != "all" or arguments.postgresql_port or arguments.mysql_port):
        parser.error("--suite payloads always runs every driver against its own node")
    return arguments


def _selected_gateways(name: str) -> list[str]:
    return list(GATEWAYS) if name == "all" else [name]


def main(argv: list[str] | None = None) -> int:
    arguments = _parse_arguments(sys.argv[1:] if argv is None else argv)

    attached = arguments.postgresql_port is not None or arguments.mysql_port is not None
    if attached and (arguments.postgresql_port is None or arguments.mysql_port is None):
        print("reusing a node needs both --postgresql-port and --mysql-port", file=sys.stderr)
        return 2

    node = None
    try:
        if attached:
            node = attach_node(
                host=arguments.host,
                pgsql_port=arguments.postgresql_port,
                mysql_port=arguments.mysql_port,
            )
        elif arguments.suite == "payloads":
            node = start_node(timeout=arguments.timeout, schema_path=binary_payload_suite.SCHEMA_PATH)
        else:
            node = start_node(timeout=arguments.timeout)
    except NodeFailure as error:
        print(error, file=sys.stderr)
        return 2

    results: list[CaseResult] = []
    try:
        if arguments.suite == "payloads":
            results.extend(binary_payload_suite.run_binary_payloads(node))
        else:
            for gateway in _selected_gateways(arguments.gateway):
                results.extend(GATEWAYS[gateway](node))
    finally:
        if node is not None:
            node.stop()

    for result in results:
        detail = f" - {result.detail}" if result.detail else ""
        print(f"{result.status.upper()} {result.name}{detail}")

    failures = [result for result in results if result.status != "pass"]
    passed = len(results) - len(failures)
    print(f"{passed}/{len(results)} cases passed")
    if failures and node is not None:
        print(node.logs(), file=sys.stderr)
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
