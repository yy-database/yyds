"""Scenario 03 — 1000-key `RENAME` rotation and `RENAMENX` at scale.

Target feature: deadline migration on rename.
Implementation risk: a move that re-stamps the deadline, drops the value or leaves the
source behind. The rotation is unambiguous: key `i+1` must hold the value written to
key `i`, and its remaining deadline must only count down, never restart.

The gateway needs about 3 ms per command, so deadlines are compared per sampled key
against that key's own remaining time measured just before the rotation instead of
against a fixed band. A reset deadline is larger than the pre-rotation value and fails.
"""

from __future__ import annotations

import sys
import time

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
)

SCENARIO_ID = "03-rename-rotation"
CASES = (
    "03-rename-rotation/redis/1000-key-rotation",
    "03-rename-rotation/redis/renamenx-at-scale",
    "03-rename-rotation/redis/engine-parity",
)

WRITE_PX_MS = 60_000
PRE_ROTATION_SLEEP = 3.0
DEADLINE_TOLERANCE_MS = 50
PAIRS = 50


def _pttl_of(client, keys: list[str]) -> list[int]:
    probes = client.pipeline(transaction=False)
    for key in keys:
        probes.pttl(key)
    return probes.execute()


def _rotation_facts(client, prefix: str, scale: int) -> dict:
    keys = keys_for(prefix, scale + 1)
    values = [f"v-{index:04d}".encode() for index in range(scale)]
    sampled = list(range(0, scale, 10))

    writer = client.pipeline(transaction=False)
    for key, value in zip(keys, values):
        writer.execute_command("SET", key, value, "PX", WRITE_PX_MS)
    writer.execute()
    time.sleep(PRE_ROTATION_SLEEP)

    before = _pttl_of(client, [keys[index] for index in sampled])

    rotation = client.pipeline(transaction=False)
    for index in range(scale - 1, -1, -1):
        rotation.execute_command("RENAME", keys[index], keys[index + 1])
    renamed = rotation.execute()

    moved = client.pipeline(transaction=False)
    for key in keys[1:]:
        moved.get(key)
    moved_values = moved.execute()

    after = _pttl_of(client, [keys[index + 1] for index in sampled])

    misplaced = [keys[index + 1] for index, value in enumerate(moved_values) if value != values[index]]
    violations = [
        [index, before_value, after_value]
        for index, before_value, after_value in zip(sampled, before, after)
        if after_value <= 0 or after_value > before_value + DEADLINE_TOLERANCE_MS
    ]
    return {
        "rename_acked": sum(1 for reply in renamed if reply is True),
        "values_moved": scale - len(misplaced),
        "deadline_carried": len(violations) == 0,
        "deadline_violations": violations,
        "deadline_before_min": min(before) if before else 0,
        "deadline_after_max": max(after) if after else 0,
        "source_absent": client.exists(keys[0]) == 0,
    }


def _renamenx_facts(client, prefix: str, scale: int) -> dict:
    keys = keys_for(prefix, scale)
    free_keys = keys_for(prefix + "free-", PAIRS)
    values = [f"v-{index:04d}".encode() for index in range(scale)]

    writer = client.pipeline(transaction=False)
    for key, value in zip(keys, values):
        writer.execute_command("SET", key, value, "PX", WRITE_PX_MS)
    writer.execute()
    time.sleep(PRE_ROTATION_SLEEP)

    sources = [keys[index * 2] for index in range(PAIRS)]
    occupied = [keys[index * 2 + 1] for index in range(PAIRS)]
    before = _pttl_of(client, sources)

    rejected = client.pipeline(transaction=False)
    for index in range(PAIRS):
        rejected.execute_command("RENAMENX", keys[index * 2], keys[index * 2 + 1])
    reject_replies = rejected.execute()

    destination_reader = client.pipeline(transaction=False)
    for key in occupied:
        destination_reader.get(key)
    rejected_destinations = destination_reader.execute()
    after_reject = _pttl_of(client, sources)

    moved = client.pipeline(transaction=False)
    for index in range(PAIRS):
        moved.execute_command("RENAMENX", keys[index * 2], free_keys[index])
    move_replies = moved.execute()

    checker = client.pipeline(transaction=False)
    for index in range(PAIRS):
        checker.get(keys[index * 2])
        checker.get(free_keys[index])
    replies = checker.execute()
    source_values = replies[0::2]
    destination_values = replies[1::2]
    after_move = _pttl_of(client, free_keys)

    violations = [
        [index, before[index], after_reject[index], after_move[index]]
        for index in range(PAIRS)
        if after_reject[index] <= 0
        or after_reject[index] > before[index] + DEADLINE_TOLERANCE_MS
        or after_move[index] <= 0
        or after_move[index] > before[index] + DEADLINE_TOLERANCE_MS
    ]
    return {
        "occupied_rejected": sum(1 for reply in reject_replies if reply == 0),
        "rejected_destination_preserved": sum(
            1 for index, value in enumerate(rejected_destinations) if value == values[index * 2 + 1]
        ),
        "free_moves_accepted": sum(1 for reply in move_replies if reply == 1),
        "free_source_gone": sum(1 for value in source_values if value is None),
        "free_destination_moved": sum(
            1 for index, value in enumerate(destination_values) if value == values[index * 2]
        ),
        "deadline_carried": len(violations) == 0,
        "deadline_violations": violations,
        "pairs": PAIRS,
    }


def run(scale: int = 1000) -> list[CaseResult]:
    node = start_redis_node(sweep_interval_ms=50)
    results: list[CaseResult] = []
    rotation: dict = {}
    renamenx: dict = {}
    try:
        client = redis_client(node.host, node.redis_port)
        try:
            rotation = _rotation_facts(client, "s03:k", scale)

            def rotated() -> None:
                confirm(rotation["rename_acked"] == scale, f"{rotation['rename_acked']}/{scale} RENAMEs were acknowledged")
                confirm(rotation["values_moved"] == scale, f"{scale - rotation['values_moved']} values did not follow the key")
                confirm(
                    rotation["deadline_carried"],
                    f"{len(rotation['deadline_violations'])} sampled keys restarted or lost their deadline: "
                    f"{rotation['deadline_violations'][:5]} (index, before PTTL, after PTTL)",
                )
                confirm(rotation["source_absent"], "the first source key must be gone after the rotation")

            try:
                rotated()
            except Exception as error:  # noqa: BLE001
                results.append(CaseResult(CASES[0], "fail", f"{type(error).__name__}: {error}"))
            else:
                results.append(
                    CaseResult(
                        CASES[0],
                        "pass",
                        f"{scale} keys rotated, sampled deadline kept counting "
                        f"({rotation['deadline_before_min']} -> max {rotation['deadline_after_max']} ms)",
                    )
                )

            renamenx = _renamenx_facts(client, "s03n:k", scale)

            def conditional() -> None:
                confirm(renamenx["occupied_rejected"] == renamenx["pairs"], "an occupied destination must answer 0")
                confirm(
                    renamenx["rejected_destination_preserved"] == renamenx["pairs"],
                    "a rejected RENAMENX must keep the destination value",
                )
                confirm(renamenx["free_moves_accepted"] == renamenx["pairs"], "a free destination must answer 1")
                confirm(renamenx["free_source_gone"] == renamenx["pairs"], "a successful RENAMENX must drop the source key")
                confirm(renamenx["free_destination_moved"] == renamenx["pairs"], "a free RENAMENX must move the value")
                confirm(
                    renamenx["deadline_carried"],
                    f"{len(renamenx['deadline_violations'])} RENAMENX keys restarted or lost their deadline",
                )

            try:
                conditional()
            except Exception as error:  # noqa: BLE001
                results.append(CaseResult(CASES[1], "fail", f"{type(error).__name__}: {error}"))
            else:
                results.append(CaseResult(CASES[1], "pass", f"{renamenx['pairs']} RENAMENX pairs kept the deadline"))
        finally:
            client.close()

        engine = oracle_redis()
        if engine is None:
            results.append(no_oracle(CASES[2], REDIS_URL_ENV))
        else:
            try:
                engine_rotation = _rotation_facts(engine, "parity-s03:", scale)
                engine_renamenx = _renamenx_facts(engine, "parity-s03n:", scale)
                engine.delete(*keys_for("parity-s03:", scale + 1), *keys_for("parity-s03n:", scale))
                engine.delete(*keys_for("parity-s03n:free-", PAIRS))
            finally:
                engine.close()
            combined = dict(engine_rotation, **{f"renamenx_{name}": value for name, value in engine_renamenx.items()})
            gateway_combined = dict(rotation, **{f"renamenx_{name}": value for name, value in renamenx.items()})
            results.append(
                compare_facts(
                    CASES[2],
                    gateway_combined,
                    combined,
                    (
                        "rename_acked",
                        "values_moved",
                        "deadline_carried",
                        "source_absent",
                        "renamenx_occupied_rejected",
                        "renamenx_rejected_destination_preserved",
                        "renamenx_free_moves_accepted",
                        "renamenx_free_destination_moved",
                        "renamenx_deadline_carried",
                    ),
                )
            )
    finally:
        node.stop()

    return results


if __name__ == "__main__":
    raise SystemExit(run_module(run, sys.argv[1:]))
