"""Scenario 04 — 1000 keys survive two restarts with monotonic TTL.

Target feature: persistence and state rebuild.
Implementation risk: the node reopens its shard and either loses records, returns
different bytes, or resurrects a deadline to its original remaining time.

The evidence is client-side: the digest is computed from `GET` replies by redis-py,
never from our own `.yykv` decoder, so a decoder that agrees with itself cannot pass
this case. No other engine can reopen this data directory, so the catalog marks the
scenario `client-only` instead of pretending an engine comparison exists.
"""

from __future__ import annotations

import shutil
import sys
import tempfile
from pathlib import Path

from harness import CaseResult, start_redis_node
from scenarios.support import confirm, digest, keys_for, redis_client, run_module

SCENARIO_ID = "04-restart-persistence"
CASES = (
    "04-restart-persistence/redis/keys-and-checksum",
    "04-restart-persistence/redis/ttl-monotonic",
)

WRITE_PX_MS = 180_000
IDENTITY = {"cluster_id": "cluster-scenario-04", "node_id": "node-scenario-04"}


def _write(client, keys: list[str], expiring_from: int) -> None:
    writer = client.pipeline(transaction=False)
    for index, key in enumerate(keys):
        value = f"v-{index:04d}".encode()
        if index < expiring_from:
            writer.set(key, value)
        else:
            writer.execute_command("SET", key, value, "PX", WRITE_PX_MS)
    writer.execute()


def _state(client, keys: list[str], expiring_from: int) -> dict:
    reader = client.pipeline(transaction=False)
    for key in keys:
        reader.get(key)
    reads = reader.execute()

    deadlines = client.pipeline(transaction=False)
    for key in keys[expiring_from:]:
        deadlines.pttl(key)
    pttls = deadlines.execute()

    return {
        "present": sum(1 for value in reads if value is not None),
        "missing": [keys[index] for index, value in enumerate(reads) if value is None],
        "value_digest": digest(b"" if value is None else value for value in reads),
        "min_deadline": min(pttls) if pttls else 0,
        "max_deadline": max(pttls) if pttls else 0,
        "deadlines": pttls,
    }


def run(scale: int = 1000) -> list[CaseResult]:
    data_dir = Path(tempfile.mkdtemp(prefix="yyds-scenario-04-"))
    keys = keys_for("s04:k", scale)
    expiring_from = scale // 2
    results: list[CaseResult] = []
    try:
        write_client = None
        node = start_redis_node(data_dir=data_dir, sweep_interval_ms=50, **IDENTITY)
        try:
            write_client = redis_client(node.host, node.redis_port)
            _write(write_client, keys, expiring_from)
            baseline = _state(write_client, keys, expiring_from)
        finally:
            if write_client is not None:
                write_client.close()
            node.stop(wipe=False)

        restarts: list[dict] = []
        stop_error: str | None = None
        try:
            for _ in range(2):
                node = start_redis_node(data_dir=data_dir, sweep_interval_ms=50, **IDENTITY)
                client = redis_client(node.host, node.redis_port)
                try:
                    restarts.append(_state(client, keys, expiring_from))
                finally:
                    client.close()
                    node.stop(wipe=False)
        except Exception as error:  # noqa: BLE001 - a failed reopen is the failure under test
            stop_error = f"{type(error).__name__}: {error}"

        def checksum() -> None:
            if stop_error is not None:
                raise AssertionError(f"the node could not reopen its data directory: {stop_error}")
            for index, state in enumerate(restarts, start=1):
                confirm(state["present"] == scale, f"restart {index}: {scale - state['present']} keys are missing")
                confirm(state["missing"] == [], f"restart {index}: missing keys {state['missing'][:5]}")
                confirm(
                    state["value_digest"] == baseline["value_digest"],
                    f"restart {index}: the client-side value digest changed "
                    f"({baseline['value_digest'][:12]} -> {state['value_digest'][:12]})",
                )

        def monotonic() -> None:
            if stop_error is not None:
                raise AssertionError(f"the node could not reopen its data directory: {stop_error}")
            previous = baseline
            for index, state in enumerate(restarts, start=1):
                confirm(state["min_deadline"] > 0, f"restart {index}: a live deadline became -1")
                confirm(
                    state["max_deadline"] <= previous["max_deadline"],
                    f"restart {index}: a deadline was resurrected "
                    f"({previous['max_deadline']} -> {state['max_deadline']} ms)",
                )
                confirm(
                    state["min_deadline"] < previous["min_deadline"],
                    f"restart {index}: the deadline did not keep counting "
                    f"({previous['min_deadline']} -> {state['min_deadline']} ms)",
                )
                previous = state

        try:
            checksum()
        except Exception as error:  # noqa: BLE001
            results.append(CaseResult(CASES[0], "fail", f"{type(error).__name__}: {error}"))
        else:
            results.append(
                CaseResult(
                    CASES[0],
                    "pass",
                    f"{scale} keys and digest {baseline['value_digest'][:12]} survived two restarts (client-observed)",
                )
            )

        try:
            monotonic()
        except Exception as error:  # noqa: BLE001
            results.append(CaseResult(CASES[1], "fail", f"{type(error).__name__}: {error}"))
        else:
            bands = " -> ".join(str(state["min_deadline"]) for state in [baseline, *restarts])
            results.append(CaseResult(CASES[1], "pass", f"minimum deadline kept counting across restarts: {bands} ms"))
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)

    return results


if __name__ == "__main__":
    raise SystemExit(run_module(run, sys.argv[1:]))
