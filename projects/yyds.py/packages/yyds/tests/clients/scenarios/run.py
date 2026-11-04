"""Runs the YYDS scale scenarios, one isolated process per implementation risk.

Each scenario module declares `SCENARIO_ID` and `CASES`, starts its own node, and
prints its results on one machine-readable line. This runner only spawns one
subprocess per scenario (so a crashed node cannot poison the next scenario), prints
what came back, and fails when a declared case was never reported.

Exit codes: 0 every case passed, 1 at least one case failed, 2 unusable arguments.
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path

from scenarios.support import MINIMUM_SCALE, RESULT_MARKER, CaseResult

HERE = Path(__file__).resolve().parent
CATALOG_PATH = HERE / "catalog.json"
CLIENT_DIR = HERE.parent


def load_catalog() -> dict:
    return json.loads(CATALOG_PATH.read_text(encoding="utf-8"))


def run_scenario(module_name: str, scale: int) -> list[CaseResult]:
    """Executes one scenario module in its own process and parses its results."""

    completed = subprocess.run(
        [sys.executable, "-m", f"scenarios.{module_name}", "--scale", str(scale)],
        cwd=str(CLIENT_DIR),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )
    line = next((text for text in completed.stdout.splitlines() if text.startswith(RESULT_MARKER)), None)
    if line is None:
        detail = (completed.stderr or completed.stdout).strip().splitlines()
        tail = " | ".join(detail[-8:]) if detail else f"exit code {completed.returncode} with no output"
        return [CaseResult(f"{module_name}/process", "fail", f"scenario process produced no results: {tail}")]
    payload = json.loads(line[len(RESULT_MARKER) :])
    return [CaseResult(**item) for item in payload]


def _status_row(result: CaseResult) -> str:
    return f"- {result.detail}" if result.detail else ""


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="run the YYDS scale scenarios")
    parser.add_argument("--list", action="store_true", help="print the scenario catalog and exit")
    parser.add_argument("--scenario", help="run one scenario by id")
    parser.add_argument("--all", action="store_true", help="run every catalog scenario")
    parser.add_argument("--scale", type=int, default=MINIMUM_SCALE)
    parser.add_argument("--require-oracle", action="store_true", help="fail when an engine-parity case has no engine")
    arguments = parser.parse_args(sys.argv[1:] if argv is None else argv)

    catalog = load_catalog()
    if arguments.list:
        for row in catalog["scenarios"]:
            print(
                f"{row['id']}\t{row['oracle']}\tscale={row['scale']}\tfeature={row['feature']}\n"
                f"  risk: {row['risk']}\n  cases: {', '.join(row['cases'])}"
            )
        return 0

    rows = catalog["scenarios"]
    if arguments.scenario:
        rows = [row for row in rows if row["id"] == arguments.scenario]
        if not rows:
            print(f"unknown scenario {arguments.scenario!r}", file=sys.stderr)
            return 2
    elif not arguments.all:
        parser.error("pass --list, --scenario <id> or --all")
    if arguments.scale < MINIMUM_SCALE:
        parser.error(f"--scale must be at least {MINIMUM_SCALE}")

    results: list[CaseResult] = []
    for row in rows:
        print(f"--- {row['id']} (scale={arguments.scale}, oracle={row['oracle']})", file=sys.stderr)
        results.extend(run_scenario(row["module"], arguments.scale))

    declared = {case for row in rows for case in row["cases"]}
    produced = {result.name for result in results}
    missing = sorted(declared - produced)
    for name in missing:
        results.append(CaseResult(name, "fail", "the scenario process never reported this declared case"))

    for result in results:
        print(f"{result.status.upper()} {result.name}{_status_row(result)}")

    failures = [result for result in results if result.status == "fail"]
    unverified = [result for result in results if result.status == "unverified"]
    passed = len(results) - len(failures) - len(unverified)
    print(f"{passed} passed, {len(failures)} failed, {len(unverified)} unverified")

    engine_rows = {row["id"] for row in rows if row["oracle"] != "client-only"}
    gated = [result for result in unverified if result.name.split("/")[0] in engine_rows]
    if gated:
        print(
            "unverified engine-parity cases have only driver-level evidence: set `YYDS_ORACLE_REDIS_URL` "
            "and `YYDS_ORACLE_MONGO_URL` to compare against real engines",
            file=sys.stderr,
        )
    if failures:
        return 1
    if gated and arguments.require_oracle:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
