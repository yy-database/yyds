"""Scenario 02 — 1000 keys spread over five `EXPIREAT` batches.

Target feature: background expiry reclamation (`sweep_expired`).
Implementation risk: the sweeper inspects at most 256 records per shard per tick. A
budgeted pass that restarts at the shard head on every tick never reaches the tail of a
1000-record shard, so keys stay on disk forever while reads pretend they are gone.

Reads alone cannot expose that defect: every read path already treats an expired record
as absent. The physical shard file is therefore the external witness — written by the
real node process, and measured by this test rather than by our own decoder.

Timing is measured, not assumed: the emulated gateway needs roughly 3 ms per write, so
an `EXPIREAT` batch can itself take seconds. The deadline window is derived from the
wall clock before and after the `PTTL` replies instead of from a fixed band.
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from harness import CaseResult, start_redis_node
from scenarios.support import (
    REDIS_URL_ENV,
    compare_facts,
    confirm,
    keys_for,
    no_oracle,
    oracle_redis,
    redis_client,
    run_module,
    shard_bytes,
    wait_until,
)

SCENARIO_ID = "02-expiry-sweep-budget"
CASES = (
    "02-expiry-sweep-budget/redis/five-expireat-batches",
    "02-expiry-sweep-budget/redis/sweep-tail-cursor",
    "02-expiry-sweep-budget/redis/engine-parity",
)

BATCH_OFFSETS = (3, 4, 5, 6, 7)
DEADLINE_TOLERANCE_MS = 400
TAIL_OFFSET_SECONDS = 3
TAIL_RECLAIM_FRACTION = 0.75


def _deadline_violations(pttls: list[int], deadlines: list[int], before: float, after: float) -> list[list[int]]:
    violations: list[list[int]] = []
    for index, pttl in enumerate(pttls):
        deadline_ms = deadlines[index] * 1000
        low = deadline_ms - (after * 1000) - DEADLINE_TOLERANCE_MS
        high = deadline_ms - (before * 1000) + DEADLINE_TOLERANCE_MS
        if not low <= pttl <= high:
            violations.append([deadlines[index] - int(before), pttl])
    return violations


def _batch_facts(client, prefix: str, scale: int, *, data_dir: Path | None = None) -> dict:
    """Writes 1000 keys, spreads five absolute deadlines over them, then waits them out."""

    keys = keys_for(prefix, scale)
    per_batch = scale // len(BATCH_OFFSETS)

    writer = client.pipeline(transaction=False)
    for key in keys:
        writer.set(key, b"x" * 64)
    writer.execute()
    peak_bytes = shard_bytes(data_dir) if data_dir is not None else 0

    # One absolute deadline per batch, measured against its own wall clock: the emulated
    # gateway needs seconds to accept a thousand `EXPIREAT` commands, so a single shared
    # base would let the first batch expire before the batch is even written.
    violations: list[list[int]] = []
    latest_deadline = 0
    for batch_index, offset in enumerate(BATCH_OFFSETS):
        batch = keys[batch_index * per_batch : (batch_index + 1) * per_batch]
        base = time.time()
        deadline = int(base) + offset
        latest_deadline = max(latest_deadline, deadline)
        expiring = client.pipeline(transaction=False)
        for key in batch:
            expiring.execute_command("EXPIREAT", key, deadline)
        expiring.execute()

        before = time.time()
        probes = client.pipeline(transaction=False)
        for key in batch:
            probes.pttl(key)
        pttls = probes.execute()
        after = time.time()
        violations.extend(_deadline_violations(pttls, [deadline] * len(batch), before, after))

    remaining = latest_deadline - time.time()
    if remaining > 0:
        time.sleep(remaining + 1.5)

    reader = client.pipeline(transaction=False)
    for key in keys:
        reader.get(key)
    reads = reader.execute()
    ttl_reader = client.pipeline(transaction=False)
    for key in keys:
        ttl_reader.ttl(key)
    ttls_after = ttl_reader.execute()

    return {
        "deadline_violations": violations,
        "present_after_deadline": sum(1 for value in reads if value is not None),
        "ttl_after_deadline_ok": all(ttl == -2 for ttl in ttls_after),
        "keys": len(keys),
        "peak_bytes": peak_bytes,
    }


def _tail_facts(client, prefix: str, scale: int, *, data_dir: Path | None = None) -> dict:
    """Puts 250 large expired records at the tail and 750 live records at the head.

    With a 256-record budget, a cursor that resumes from the last examined key needs
    four ticks to reach the tail. A cursor that restarts at the head examines the same
    live prefix forever and the 250 large records are never reclaimed.
    """

    live = (scale * 3) // 4
    keys = keys_for(prefix, scale)
    deadline = int(time.time()) + TAIL_OFFSET_SECONDS

    writer = client.pipeline(transaction=False)
    for index, key in enumerate(keys):
        writer.set(key, b"x" * 1024 if index >= live else b"live-0000000000")
    writer.execute()
    peak_bytes = shard_bytes(data_dir) if data_dir is not None else 0

    expiring = client.pipeline(transaction=False)
    for key in keys[live:]:
        expiring.execute_command("EXPIREAT", key, deadline)
    expiring.execute()

    time.sleep(TAIL_OFFSET_SECONDS + 0.8)

    reader = client.pipeline(transaction=False)
    for key in keys[live:]:
        reader.get(key)
    tail_reads = reader.execute()
    head_reader = client.pipeline(transaction=False)
    for key in keys[:live]:
        head_reader.get(key)
    head_reads = head_reader.execute()

    return {
        "tail_absent": sum(1 for value in tail_reads if value is None),
        "head_present": sum(1 for value in head_reads if value is not None),
        "tail_size": len(tail_reads),
        "head_size": len(head_reads),
        "deadline": deadline,
        "peak_bytes": peak_bytes,
    }


def run(scale: int = 1000) -> list[CaseResult]:
    node = start_redis_node(sweep_interval_ms=50)
    results: list[CaseResult] = []
    gateway: dict = {}
    tail: dict = {}
    try:
        client = redis_client(node.host, node.redis_port)
        try:
            gateway = _batch_facts(client, "s02:k", scale, data_dir=node.data_dir)
            peak = gateway["peak_bytes"]
            shrunk = wait_until(lambda: shard_bytes(node.data_dir) < peak * 0.25, timeout=10.0)
            final = shard_bytes(node.data_dir)

            def batches() -> None:
                confirm(
                    gateway["deadline_violations"] == [],
                    f"{len(gateway['deadline_violations'])} keys reported a deadline outside the batch window: "
                    f"{gateway['deadline_violations'][:5]} (seconds after the batch base, observed PTTL)",
                )
                confirm(
                    gateway["present_after_deadline"] == 0,
                    f"{gateway['present_after_deadline']}/{gateway['keys']} keys were still readable after their deadline",
                )
                confirm(gateway["ttl_after_deadline_ok"], "an elapsed key must report TTL -2")
                confirm(shrunk, f"the sweeper never reclaimed the shard (peak={peak} final={final})")

            try:
                batches()
            except Exception as error:  # noqa: BLE001
                results.append(CaseResult(CASES[0], "fail", f"{type(error).__name__}: {error}"))
            else:
                results.append(
                    CaseResult(CASES[0], "pass", f"5 batches expired, shard shrank {peak} -> {final} bytes across several ticks")
                )

            tail = _tail_facts(client, "s02t:k", scale, data_dir=node.data_dir)
            peak_tail = tail["peak_bytes"]
            freed = wait_until(lambda: shard_bytes(node.data_dir) <= peak_tail * TAIL_RECLAIM_FRACTION, timeout=10.0)
            final_tail = shard_bytes(node.data_dir)
            freed_fraction = 0 if peak_tail == 0 else (peak_tail - final_tail) / peak_tail

            def cursor() -> None:
                confirm(tail["tail_absent"] == tail["tail_size"], f"{tail['tail_size'] - tail['tail_absent']} tail keys still readable")
                confirm(tail["head_present"] == tail["head_size"], "the live head must stay readable")
                confirm(
                    freed,
                    f"the tail was never reclaimed: peak={peak_tail} final={final_tail} freed={freed_fraction:.0%}, "
                    "so the sweep cursor restarts at the shard head instead of resuming",
                )

            try:
                cursor()
            except Exception as error:  # noqa: BLE001
                results.append(CaseResult(CASES[1], "fail", f"{type(error).__name__}: {error}"))
            else:
                results.append(
                    CaseResult(CASES[1], "pass", f"tail reclaimed across ticks, freed {freed_fraction:.0%} of the shard")
                )
        finally:
            client.close()

        engine = oracle_redis()
        if engine is None:
            results.append(no_oracle(CASES[2], REDIS_URL_ENV))
        else:
            try:
                engine_batches = _batch_facts(engine, "parity-s02:", scale)
                engine_tail = _tail_facts(engine, "parity-s02t:", scale)
                engine.delete(*keys_for("parity-s02:", scale), *keys_for("parity-s02t:", scale))
            finally:
                engine.close()
            combined = dict(engine_batches, **engine_tail)
            gateway_combined = dict(gateway, **tail)
            results.append(
                compare_facts(
                    CASES[2],
                    gateway_combined,
                    combined,
                    (
                        "deadline_violations",
                        "present_after_deadline",
                        "ttl_after_deadline_ok",
                        "tail_absent",
                        "head_present",
                    ),
                )
            )
    finally:
        node.stop()

    return results


if __name__ == "__main__":
    raise SystemExit(run_module(run, sys.argv[1:]))
