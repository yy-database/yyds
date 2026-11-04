"""Scenario 05 — 1000 keys where one value sits just under the 1 MiB frame cap.

Target feature: inbound frame limits.
Implementation risk: the Redis gateway bounds one request at `max_frame_bytes = 1 MiB`.
A large value must still round-trip byte-exactly below that cap, and a request above it
must be refused with a Redis-shaped error without killing the node, the connection or
the other 999 keys.

Real Redis accepts a 1.3 MB value (its `proto-max-bulk-len` is 512 MB), so the parity
case pins that divergence instead of treating agreement as required.
"""

from __future__ import annotations

import sys

from harness import CaseResult, start_redis_node
from scenarios.support import (
    REDIS_URL_ENV,
    compare_facts,
    confirm,
    digest,
    keys_for,
    no_oracle,
    oracle_redis,
    redis_client,
    run_module,
)

SCENARIO_ID = "05-oversize-value"
CASES = (
    "05-oversize-value/redis/1000-keys-with-large-value",
    "05-oversize-value/redis/above-frame-cap",
    "05-oversize-value/redis/engine-parity",
)

FRAME_CAP_BYTES = 1_048_576
BELOW_CAP_BYTES = 1_000_000
ABOVE_CAP_BYTES = 1_300_000
BIG_INDEX = 500


def _write(client, prefix: str, scale: int) -> tuple[list[bytes], list[bytes]]:
    keys = keys_for(prefix, scale)
    values = [b"x" * 16 for _ in range(scale)]
    values[BIG_INDEX] = b"L" * BELOW_CAP_BYTES

    writer = client.pipeline(transaction=False)
    for index, (key, value) in enumerate(zip(keys, values)):
        if index == BIG_INDEX:
            continue  # a 1 MB bulk must not share a pipeline buffer with 999 other commands
        writer.set(key, value)
    writer.execute()
    client.set(keys[BIG_INDEX], values[BIG_INDEX])
    return keys, values


def _facts(client, prefix: str, scale: int) -> dict:
    keys, values = _write(client, prefix, scale)
    reader = client.pipeline(transaction=False)
    for index, key in enumerate(keys):
        if index == BIG_INDEX:
            continue
        reader.get(key)
    reads = reader.execute()
    big = client.get(keys[BIG_INDEX])

    combined = list(reads)
    combined.insert(BIG_INDEX, big)
    mismatched = sum(1 for index, value in enumerate(combined) if value != values[index])

    oversize_error: str | None = None
    try:
        client.set(prefix + "huge", b"h" * ABOVE_CAP_BYTES)
    except Exception as error:  # noqa: BLE001 - the refusal type differs per client stack
        oversize_error = f"{type(error).__name__}: {error}"
    alive = client.ping() is True
    huge_absent = client.exists(prefix + "huge") == 0

    return {
        "below_cap_length": len(big) if isinstance(big, bytes) else 0,
        "mismatched": mismatched,
        "value_digest": digest(b"" if value is None else value for value in combined),
        "oversize_refused": oversize_error is not None,
        "oversize_error_class": (oversize_error or "").split(":", 1)[0] or None,
        "oversize_error_mentions_limit": bool(oversize_error and "limit" in oversize_error.lower()),
        "node_alive_after_oversize": alive,
        "oversize_key_absent": huge_absent,
        "keys": scale,
    }


def run(scale: int = 1000) -> list[CaseResult]:
    node = start_redis_node(sweep_interval_ms=50)
    results: list[CaseResult] = []
    gateway: dict = {}
    try:
        client = redis_client(node.host, node.redis_port)
        try:
            gateway = _facts(client, "s05:k", scale)

            def large() -> None:
                confirm(gateway["below_cap_length"] == BELOW_CAP_BYTES, f"the large value came back as {gateway['below_cap_length']} bytes")
                confirm(gateway["mismatched"] == 0, f"{gateway['mismatched']} keys returned different bytes")
                confirm(
                    gateway["value_digest"] == digest_of_expected(scale),
                    "the client-side digest of the mixed-size values does not match",
                )

            def capped() -> None:
                confirm(gateway["oversize_refused"], "a request above the frame cap must be refused")
                confirm(gateway["oversize_error_mentions_limit"], f"the refusal must name the request limit, got {gateway['oversize_error_class']}")
                confirm(gateway["node_alive_after_oversize"], "the node must keep serving after a refused frame")
                confirm(gateway["oversize_key_absent"], "a refused write must not create the key")

            try:
                large()
            except Exception as error:  # noqa: BLE001
                results.append(CaseResult(CASES[0], "fail", f"{type(error).__name__}: {error}"))
            else:
                results.append(
                    CaseResult(CASES[0], "pass", f"{scale} keys round-trip with one {BELOW_CAP_BYTES}-byte value below the cap")
                )

            try:
                capped()
            except Exception as error:  # noqa: BLE001
                results.append(CaseResult(CASES[1], "fail", f"{type(error).__name__}: {error}"))
            else:
                results.append(
                    CaseResult(CASES[1], "pass", f"{ABOVE_CAP_BYTES} bytes refused, node alive, key absent")
                )
        finally:
            client.close()

        engine = oracle_redis()
        if engine is None:
            results.append(no_oracle(CASES[2], REDIS_URL_ENV))
        else:
            try:
                engine_facts = _facts(engine, "parity-s05:", scale)
                engine.delete(*keys_for("parity-s05:", scale), "parity-s05:huge")
            finally:
                engine.close()
            results.append(
                compare_facts(
                    CASES[2],
                    gateway,
                    engine_facts,
                    (
                        "below_cap_length",
                        "mismatched",
                        "value_digest",
                        "node_alive_after_oversize",
                        "oversize_refused",
                    ),
                    # Real Redis accepts the 1.3 MB value, so its acceptance and the resulting
                    # key are the documented divergence, not a gateway bug.
                    expected_divergence={"oversize_refused": (False, True), "oversize_key_absent": (False, True)},
                )
            )
    finally:
        node.stop()

    return results


def digest_of_expected(scale: int) -> str:
    values = [b"x" * 16 for _ in range(scale)]
    values[BIG_INDEX] = b"L" * BELOW_CAP_BYTES
    return digest(values)


if __name__ == "__main__":
    raise SystemExit(run_module(run, sys.argv[1:]))
