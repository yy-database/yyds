"""Shared plumbing for the YYDS scale scenarios.

Every scenario is a separate module, a separate process and a separate node, so one
implementation risk is isolated per run and cannot be masked by another scenario's
setup. This module only removes accidental duplication: client construction, oracle
lookup, fact comparison and the result protocol the runner reads back.

Two evidence classes are kept apart on purpose:

* an invariant case asserts what the gateway must do, judged by a real client,
* an engine-parity case compares the same normalized facts against a real Redis or
  MongoDB named by `YYDS_ORACLE_*`. Without an engine it stays `unverified` and is
  never counted as a pass.

Scenarios whose subject does not exist in another engine (`.yykv` damage, restart
persistence) declare `oracle: client-only` in the catalog and report the client-level
evidence they actually have.
"""

from __future__ import annotations

import argparse
import dataclasses
import hashlib
import json
import os
import time
from typing import Any, Callable, Iterable

import redis
from pymongo import MongoClient
from pymongo.server_api import ServerApi

from harness import CaseResult

MINIMUM_SCALE = 1000
REDIS_URL_ENV = "YYDS_ORACLE_REDIS_URL"
MONGO_URL_ENV = "YYDS_ORACLE_MONGO_URL"
RESULT_MARKER = "@@SCENARIO-RESULTS@@"


def parse_scale(argv: list[str]) -> int:
    """Reads `--scale`, refusing a scale below the catalog minimum."""

    parser = argparse.ArgumentParser(description="run one YYDS scale scenario")
    parser.add_argument("--scale", type=int, default=MINIMUM_SCALE)
    arguments = parser.parse_args(argv)
    if arguments.scale < MINIMUM_SCALE:
        parser.error(f"scenario scale must be at least {MINIMUM_SCALE}, got {arguments.scale}")
    return arguments.scale


def emit(results: list[CaseResult]) -> None:
    """Prints one machine-readable line the runner parses back."""

    print(RESULT_MARKER + json.dumps([dataclasses.asdict(result) for result in results], ensure_ascii=False))


def run_module(run: Callable[[int], list[CaseResult]], argv: list[str]) -> int:
    """Shared `__main__` body: parse the scale, run, emit, exit non-zero on failures."""

    results = run(parse_scale(argv))
    emit(results)
    return 1 if any(result.status == "fail" for result in results) else 0


def redis_client(host: str, port: int, *, protocol: int = 2) -> redis.Redis:
    """A RESP client with byte replies, so values are compared without decoding."""

    return redis.Redis(
        host=host,
        port=port,
        decode_responses=False,
        protocol=protocol,
        lib_name=None,
        lib_version=None,
        socket_timeout=30,
    )


def oracle_redis() -> redis.Redis | None:
    url = os.environ.get(REDIS_URL_ENV, "").strip()
    if not url:
        return None
    return redis.from_url(url, decode_responses=False, protocol=2, socket_timeout=30)


def mongo_client(host: str, port: int) -> MongoClient:
    """PyMongo pinned to OP_MSG, exactly as the existing MongoDB suite drives it."""

    return MongoClient(
        host,
        port,
        directConnection=True,
        server_api=ServerApi("1"),
        serverSelectionTimeoutMS=20_000,
        connectTimeoutMS=20_000,
        socketTimeoutMS=20_000,
        retryWrites=False,
        retryReads=False,
    )


def oracle_mongo() -> MongoClient | None:
    url = os.environ.get(MONGO_URL_ENV, "").strip()
    if not url:
        return None
    return MongoClient(url, server_api=ServerApi("1"), serverSelectionTimeoutMS=20_000)


def keys_for(prefix: str, scale: int) -> list[str]:
    return [f"{prefix}{index:04d}" for index in range(scale)]


def digest(parts: Iterable[bytes | str]) -> str:
    """Stable content fingerprint computed by the client, never by our own decoder."""

    accumulator = hashlib.sha256()
    for part in parts:
        accumulator.update(part if isinstance(part, bytes) else part.encode())
        accumulator.update(b"\x00")
    return accumulator.hexdigest()


def confirm(condition: bool, message: str) -> None:
    """Raises a named failure; `assert` is stripped under `-O` and must not be used."""

    if not condition:
        raise AssertionError(message)


def wait_until(predicate: Callable[[], bool], *, timeout: float, interval: float = 0.1) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return predicate()


def unverified(name: str, reason: str) -> CaseResult:
    return CaseResult(name, "unverified", reason)


def no_oracle(name: str, variable: str) -> CaseResult:
    return unverified(name, f"only driver-level evidence: set `{variable}` to compare against a real engine")


def compare_facts(
    name: str,
    gateway: dict[str, Any],
    engine: dict[str, Any] | None,
    fields: tuple[str, ...],
    *,
    expected_divergence: dict[str, tuple[Any, Any]] | None = None,
) -> CaseResult:
    """Compares normalized facts between the gateway and a real engine.

    `expected_divergence` pins a divergence we have already documented: the pair
    `(engine_value, gateway_value)` must match exactly, so a change in either engine
    or gateway shows up as a failure instead of being silently tolerated.
    """

    if engine is None:
        return CaseResult(name, "unverified", "only driver-level evidence: no engine endpoint is configured")

    pinned = expected_divergence or {}
    divergent: list[str] = []
    mismatched_pins: list[str] = []
    checked = (*fields, *(field for field in pinned if field not in fields))
    for field in checked:
        engine_value = engine.get(field)
        gateway_value = gateway.get(field)
        if field in pinned:
            expected = pinned[field]
            if (engine_value, gateway_value) != expected:
                mismatched_pins.append(
                    f"{field}: expected known divergence {expected!r}, got engine={engine_value!r} gateway={gateway_value!r}"
                )
            continue
        if engine_value != gateway_value:
            divergent.append(f"{field}: engine={engine_value!r} gateway={gateway_value!r}")

    problems = mismatched_pins + divergent
    if problems:
        return CaseResult(name, "fail", " | ".join(problems))
    pinned_text = ", ".join(f"{field}={pinned[field]!r}" for field in pinned) if pinned else "no pinned divergence"
    return CaseResult(name, "pass", f"engine and gateway agree on {', '.join(fields)} ({pinned_text})")


def shard_files(data_dir) -> list:
    return sorted(data_dir.glob("*.yykv"))


def shard_bytes(data_dir) -> int:
    return sum(path.stat().st_size for path in shard_files(data_dir))
