"""下游验收示例：真实 redis-py 对 yyds Redis 网关做 1000 键写入、读取、过期与删除。

期望值由本示例按写入输入自行推导（键数、字节值、TTL、删除计数），不看网关内部常量。
节点由 `node projects/yyds.ts/packages/yyds-harness/run.mjs` 启动并通过 `YYDS_REDIS_PORT` 传入。

设置 `YYDS_ORACLE_REDIS_URL`（例如 `redis://127.0.0.1:6379/0`）时，同一批操作会在真实
Redis 上重放一次并对拍事实；没有 oracle 时只输出 `UNVERIFIED`，不计作通过。
"""

from __future__ import annotations

import os
import sys

import redis

KEYS = 1000
HOST = os.environ.get("YYDS_HOST", "127.0.0.1")
PORT = int(os.environ.get("YYDS_REDIS_PORT", "0"))
ORACLE_URL = os.environ.get("YYDS_ORACLE_REDIS_URL")
PREFIX = "example:redis"

if PORT <= 0:
    print("YYDS_REDIS_PORT must point at a running yyds node", file=sys.stderr)
    raise SystemExit(2)


def check(condition: bool, message: str) -> None:
    if not condition:
        raise AssertionError(message)


def connect() -> redis.Redis:
    return redis.Redis(host=HOST, port=PORT, decode_responses=False, protocol=2, socket_timeout=30)


def facts(client: redis.Redis, suffix: str) -> dict[str, object]:
    """对同一个键空间做一轮写入、读取、过期和删除，返回客户端可见的事实。"""
    keys = [f"{PREFIX}:{suffix}:{index:04d}" for index in range(KEYS)]
    values = [f"value-{index:04d}".encode() for index in range(KEYS)]

    pipeline = client.pipeline(transaction=False)
    for key, value in zip(keys, values):
        pipeline.set(key, value)
    acknowledgements = pipeline.execute()
    check(len(acknowledgements) == KEYS and all(ack is True for ack in acknowledgements), "set acknowledgements were not all true")

    pipeline = client.pipeline(transaction=False)
    for key in keys:
        pipeline.get(key)
    stored = pipeline.execute()
    check(stored == values, "pipeline reads did not return the written bytes")

    check(client.get(keys[KEYS // 2]) == values[KEYS // 2], "spot read returned a different value")

    pipeline = client.pipeline(transaction=False)
    for key in keys:
        pipeline.ttl(key)
    deadlines = pipeline.execute()
    check(all(deadline == -1 for deadline in deadlines), f"keys unexpectedly carried deadlines: {sorted(set(deadlines))}")

    client.expire(keys[0], 5)
    live = client.ttl(keys[0])
    check(1 <= live <= 5, f"ttl after expire was {live}")

    pipeline = client.pipeline(transaction=False)
    for key in keys:
        pipeline.delete(key)
    removed = sum(pipeline.execute())
    check(removed == KEYS, f"deleted {removed} keys, expected {KEYS}")

    return {
        "written": len(acknowledgements),
        "read-back-equal": stored == values,
        "no-deadline": all(deadline == -1 for deadline in deadlines),
        "expire-ttl": live,
        "removed": removed,
        "absent-after-delete": client.get(keys[0]) is None,
    }


def compare(gateway: dict[str, object], oracle: dict[str, object]) -> None:
    for field in ("written", "read-back-equal", "no-deadline", "removed", "absent-after-delete"):
        check(gateway[field] == oracle[field], f"{field} differs: gateway={gateway[field]!r} oracle={oracle[field]!r}")
    check(abs(int(gateway["expire-ttl"]) - int(oracle["expire-ttl"])) <= 2, "expire-ttl differs by more than two seconds")


def main() -> int:
    failed = 0
    try:
        gateway = facts(connect(), "gateway")
    except Exception as error:  # noqa: BLE001 - the example reports any driver failure
        failed += 1
        print(f"FAIL examples/redis_roundtrip.py gateway: {type(error).__name__}: {error}")
    else:
        print(f"PASS examples/redis_roundtrip.py gateway {KEYS} keys")

    if ORACLE_URL:
        try:
            oracle = facts(redis.from_url(ORACLE_URL, decode_responses=False, protocol=2), "oracle")
            compare(gateway, oracle)
        except Exception as error:  # noqa: BLE001
            failed += 1
            print(f"FAIL examples/redis_roundtrip.py parity: {type(error).__name__}: {error}")
        else:
            print("PASS examples/redis_roundtrip.py parity")
    else:
        print("UNVERIFIED examples/redis_roundtrip.py parity: set YYDS_ORACLE_REDIS_URL to replay against a real Redis")

    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
