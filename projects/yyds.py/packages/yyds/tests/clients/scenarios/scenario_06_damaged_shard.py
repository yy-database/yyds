"""Scenario 06 — a damaged `.yykv` at 1000 keys must be refused by the real process.

Target feature: shard integrity and the version gate.
Implementation risk: a checksum mismatch, a truncated body or a legacy v2 header could
be accepted, silently repaired, or rewritten to a fresh empty shard, all of which look
like success from a client.

The procedure is deliberately external: a real node process writes 1000 keys and exits,
the test damages the file bytes itself, then a second real process is started. The case
only passes when that process exits non-zero with a message naming the defect and leaves
the damaged bytes untouched. Our own decoder is never used as the judge.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

from harness import CaseResult, _run_cli, start_redis_node
from scenarios.support import confirm, digest, redis_client, run_module

SCENARIO_ID = "06-damaged-shard"
CASES = (
    "06-damaged-shard/redis/bit-flip",
    "06-damaged-shard/redis/truncated",
    "06-damaged-shard/redis/legacy-v2",
)

SHARD_NAME = "local-0.yykv"
BODY_OFFSET = 13  # magic (5 bytes) plus the little-endian body checksum


def _flip_body_bit(path: Path) -> None:
    data = bytearray(path.read_bytes())
    confirm(len(data) > BODY_OFFSET + 64, f"the shard is only {len(data)} bytes, too small to damage")
    data[BODY_OFFSET + 64] ^= 0x01
    path.write_bytes(bytes(data))


def _truncate_body(path: Path) -> None:
    data = path.read_bytes()
    path.write_bytes(data[: len(data) // 2])


def _legacy_v2_header(path: Path) -> None:
    data = bytearray(path.read_bytes())
    data[4] = 0x02
    path.write_bytes(bytes(data))


DAMAGE = (
    (CASES[0], _flip_body_bit, ("checksum",)),
    # A truncated body fails the same body checksum, so the only naming we can
    # require for it is the checksum refusal itself.
    (CASES[1], _truncate_body, ("checksum",)),
    (CASES[2], _legacy_v2_header, ("legacy", "v2")),
)


def _damage_case(name: str, mutate, expected_tokens: tuple[str, ...], scale: int) -> CaseResult:
    data_dir = Path(tempfile.mkdtemp(prefix="yyds-scenario-06-"))
    identity = {"cluster_id": "cluster-scenario-06", "node_id": "node-scenario-06"}
    try:
        node = start_redis_node(data_dir=data_dir, sweep_interval_ms=50, **identity)
        try:
            client = redis_client(node.host, node.redis_port)
            try:
                writer = client.pipeline(transaction=False)
                for index in range(scale):
                    writer.set(f"s06:k{index:04d}", f"v-{index:04d}".encode())
                writer.execute()
                confirm(client.exists("s06:k0000") == 1, "the seed write did not land")
            finally:
                client.close()
        finally:
            node.stop(wipe=False)

        shard = data_dir / SHARD_NAME
        confirm(shard.exists(), f"the node never created {SHARD_NAME}")

        mutate(shard)
        damaged_digest = digest([shard.read_bytes()])

        # `yyds init` is the real process that opens the shard, and the CLI is invoked
        # directly here: the shared harness wipes a caller-owned directory on a failed
        # start, which would destroy the damaged bytes before they can be compared.
        refusal = _run_cli(
            [
                "init",
                "--data-dir",
                str(data_dir),
                "--cluster-id",
                identity["cluster_id"],
                "--node-id",
                identity["node_id"],
            ],
            60.0,
        )

        confirm(refusal.returncode != 0, "a node accepted a damaged shard and opened it")
        message = f"exit {refusal.returncode}: {refusal.stderr.strip()}"
        lowered = message.lower()
        missing = [token for token in expected_tokens if token not in lowered]
        confirm(not missing, f"the refusal message never mentions {missing}: {message[-400:]}")
        confirm(shard.exists(), "the damaged shard disappeared instead of being refused")
        confirm(digest([shard.read_bytes()]) == damaged_digest, "the node rewrote the damaged shard instead of refusing it")
        return CaseResult(name, "pass", f"real process refused the shard: {message.splitlines()[0][:160]}")
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


def run(scale: int = 1000) -> list[CaseResult]:
    results: list[CaseResult] = []
    for name, mutate, tokens in DAMAGE:
        try:
            results.append(_damage_case(name, mutate, tokens, scale))
        except Exception as error:  # noqa: BLE001
            results.append(CaseResult(name, "fail", f"{type(error).__name__}: {error}"))
    return results


if __name__ == "__main__":
    raise SystemExit(run_module(run, sys.argv[1:]))
