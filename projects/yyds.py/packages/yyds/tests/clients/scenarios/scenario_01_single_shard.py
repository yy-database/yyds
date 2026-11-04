"""Scenario 01 — 1000 keys in the single published shard.

Target feature: the Redis key space at four-digit scale.
Implementation risk: the node publishes one routing shard, so a shard threshold or a
missing split shows up as unreadable keys rather than as an error, and a second
`.yykv` file could appear that no routing map published.

The assertions are per key, judged by redis-py: every write must be acknowledged,
every read must return the exact bytes written, no key may carry a deadline, and the
data directory must still hold exactly one shard file.
"""

from __future__ import annotations

import sys

from harness import CaseResult, start_redis_node
from scenarios.support import (
    compare_facts,
    confirm,
    digest,
    keys_for,
    no_oracle,
    oracle_redis,
    redis_client,
    run_module,
    shard_files,
    shard_bytes,
    REDIS_URL_ENV,
)

SCENARIO_ID = "01-single-shard"
CASES = (
    "01-single-shard/redis/1000-keys-write-read",
    "01-single-shard/redis/one-published-shard",
    "01-single-shard/redis/engine-parity",
)


def _facts(client, prefix: str, scale: int) -> dict:
    keys = keys_for(prefix, scale)
    values = [f"v-{index:04d}".encode() for index in range(scale)]

    writer = client.pipeline(transaction=False)
    for key, value in zip(keys, values):
        writer.set(key, value)
    acknowledgements = writer.execute()

    reader = client.pipeline(transaction=False)
    for key in keys:
        reader.get(key)
    reads = reader.execute()

    deadlines = client.pipeline(transaction=False)
    for key in keys:
        deadlines.ttl(key)
    ttls = deadlines.execute()

    mismatched = [keys[index] for index, value in enumerate(reads) if value != values[index]]
    return {
        "acked": sum(1 for reply in acknowledgements if reply is True),
        "present": sum(1 for value in reads if value is not None),
        "mismatched": len(mismatched),
        "value_digest": digest(b"" if value is None else value for value in reads),
        "keys_with_deadline": sum(1 for value in ttls if value != -1),
    }


def run(scale: int = 1000) -> list[CaseResult]:
    node = start_redis_node(sweep_interval_ms=50)
    results: list[CaseResult] = []
    try:
        client = redis_client(node.host, node.redis_port)
        try:
            gateway = _facts(client, "s01:k", scale)

            def write_read() -> None:
                confirm(gateway["acked"] == scale, f"{gateway['acked']}/{scale} writes were acknowledged")
                confirm(gateway["present"] == scale, f"{gateway['present']}/{scale} keys were readable")
                confirm(gateway["mismatched"] == 0, f"{gateway['mismatched']} keys returned different bytes")
                confirm(
                    gateway["value_digest"] == digest(f"v-{index:04d}".encode() for index in range(scale)),
                    "the client-side digest of the written values does not match the read values",
                )
                confirm(gateway["keys_with_deadline"] == 0, "keys without a TTL must report TTL -1")

            files = shard_files(node.data_dir)
            size = shard_bytes(node.data_dir)

            def one_shard() -> None:
                confirm(len(files) == 1, f"the node must publish exactly one shard file, found {[path.name for path in files]}")
                confirm(files[0].name == "local-0.yykv", f"unexpected shard file name {files[0].name}")
                confirm(size > 4 * scale, f"the shard file ({size} bytes) does not hold {scale} records")

            try:
                write_read()
            except Exception as error:  # noqa: BLE001 - the matrix reports any failure
                results.append(CaseResult(CASES[0], "fail", f"{type(error).__name__}: {error}"))
            else:
                results.append(CaseResult(CASES[0], "pass", f"{scale} keys round-trip, digest {gateway['value_digest'][:12]}"))

            try:
                one_shard()
            except Exception as error:  # noqa: BLE001
                results.append(CaseResult(CASES[1], "fail", f"{type(error).__name__}: {error}"))
            else:
                results.append(CaseResult(CASES[1], "pass", f"one shard file holds all {scale} keys, {size} bytes"))
        finally:
            client.close()

        engine = oracle_redis()
        if engine is None:
            results.append(no_oracle(CASES[2], REDIS_URL_ENV))
        else:
            try:
                engine_facts = _facts(engine, "parity-s01:", scale)
                engine.delete(*keys_for("parity-s01:", scale))
            finally:
                engine.close()
            results.append(
                compare_facts(
                    CASES[2],
                    gateway,
                    engine_facts,
                    ("acked", "present", "mismatched", "value_digest", "keys_with_deadline"),
                )
            )
    finally:
        node.stop()

    return results


if __name__ == "__main__":
    raise SystemExit(run_module(run, sys.argv[1:]))
