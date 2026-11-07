"""Redis real-client suite driven by redis-py against a node started by `yyds start`.

The single-command semantics come from one shared scenario table
(`projects/yyds.ts/packages/yyds/tests/protocol/redis/differential/expiry-scenarios.json`) that the
Node suite executes through node-redis, so the same operation sequence is observed
through two independent client stacks. Lifecycle cases (reconnect, restart, background
reclamation) are Python-local and use the same redis-py driver.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
import threading
import time
from pathlib import Path

import redis

from harness import (
    CaseResult,
    NodeFailure,
    YydsNode,
    _cli_arguments,
    _free_port,
    _port_open,
    _pump,
    _run_cli,
    check,
    run_cases,
)

REPO_ROOT = Path(__file__).resolve().parents[6]
SCENARIO_PATH = (
    REPO_ROOT / "projects/yyds.ts/packages/yyds/tests/protocol/redis/differential/expiry-scenarios.json"
)
SHARD_FILE = "local-0.yykv"

_RUNNING_LINE_PREFIX = "yyds running "


def start_redis_node(
    *,
    timeout: float = 90.0,
    sweep_interval_ms: int | None = None,
    data_dir: Path | None = None,
    cluster_id: str | None = None,
    node_id: str | None = None,
) -> YydsNode:
    """Starts one node serving only the Redis gateway (no catalog, no SQL listeners)."""

    reserved: set[int] = set()
    redis_port = _free_port(reserved)
    if data_dir is None:
        data_dir = Path(tempfile.mkdtemp(prefix="yyds-redis-clients-"))
    cluster_id = cluster_id or f"cluster-{os.getpid()}-{int(time.time())}"
    node_id = node_id or f"node-{os.getpid()}"

    node = YydsNode(host="127.0.0.1", pgsql_port=0, mysql_port=0, redis_port=redis_port, data_dir=data_dir)
    try:
        init = _run_cli(["init", "--data-dir", str(data_dir), "--cluster-id", cluster_id, "--node-id", node_id], 30.0)
        if init.returncode != 0:
            raise NodeFailure(f"yyds init exited with code {init.returncode}: {init.stderr.strip()}")

        args = [
            "start",
            "--data-dir",
            str(data_dir),
            "--cluster-id",
            cluster_id,
            "--node-id",
            node_id,
            "--redis-port",
            str(redis_port),
        ]
        if sweep_interval_ms is not None:
            args += ["--expiry-sweep-interval-ms", str(sweep_interval_ms)]
        node.process = subprocess.Popen(
            _cli_arguments(args),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
        )
        assert node.process.stdout is not None and node.process.stderr is not None
        threading.Thread(target=_pump, args=(node.process.stdout, node.stdout_lines), daemon=True).start()
        threading.Thread(target=_pump, args=(node.process.stderr, node.stderr_lines), daemon=True).start()

        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            if node.process.poll() is not None:
                raise NodeFailure(f"yyds start exited with code {node.process.returncode} before readiness\n{node.logs()}")
            announced = any(line.startswith(_RUNNING_LINE_PREFIX) for line in node.stdout_lines)
            if announced and _port_open(node.host, redis_port):
                return node
            time.sleep(0.05)
        raise NodeFailure(f"node did not accept Redis connections within {timeout:.0f}s\n{node.logs()}")
    except Exception:
        node.stop()
        raise


def _client(node: YydsNode, protocol: int = 2) -> redis.Redis:
    # RESP2 stays the default so most assertions keep the older wire shape. RESP3 is
    # opt-in for the optimistic-lock cases that must observe the dedicated null type.
    # Library metadata is skipped to keep every handshake minimal.
    return redis.Redis(
        host=node.host,
        port=node.redis_port,
        decode_responses=True,
        protocol=protocol,
        lib_name=None,
        lib_version=None,
    )


def _resolve(argument: object) -> str:
    if isinstance(argument, str):
        return argument
    marker = argument if isinstance(argument, dict) else {}
    if "unixSecondsFromNow" in marker:
        return str(int(time.time()) + int(marker["unixSecondsFromNow"]))
    if "unixMsFromNow" in marker:
        return str(int(time.time() * 1000) + int(marker["unixMsFromNow"]))
    raise AssertionError(f"unresolvable scenario argument {argument!r}")


def _check(expect: object, actual: object, label: str) -> None:
    if expect is None:
        check(actual is None, f"{label}: expected null, got {actual!r}")
        return
    if isinstance(expect, bool):
        raise AssertionError("boolean expectations are not part of the table")
    if isinstance(expect, int):
        check(actual == expect, f"{label}: expected {expect}, got {actual!r}")
        return
    if isinstance(expect, str):
        # redis-py maps simple-string OK replies of known commands to True.
        if expect == "OK" and actual is True:
            return
        check(actual == expect, f"{label}: expected {expect!r}, got {actual!r}")
        return
    if isinstance(expect, list):
        check(actual == expect, f"{label}: expected {expect!r}, got {actual!r}")
        return
    if isinstance(expect, dict) and "intBetween" in expect:
        low, high = expect["intBetween"]
        check(isinstance(actual, int) and low <= actual <= high, f"{label}: expected an integer within {[low, high]}, got {actual!r}")
        return
    raise AssertionError(f"unhandled expectation {expect!r} for {label}")


def _error_message(error: redis.exceptions.ResponseError) -> str:
    """redis-py strips the leading error code (``ERR ``) from the server message."""
    message = str(error)
    return message if message.startswith("ERR ") else f"ERR {message}"


def _run_scenario(node: YydsNode, scenario: dict) -> None:
    client = _client(node)
    try:
        for index, step in enumerate(scenario["steps"]):
            label = f"{scenario['name']}[{index}]"
            if "sleep" in step:
                time.sleep(step["sleep"] / 1000.0)
                continue
            args = [_resolve(argument) for argument in step["cmd"]]
            expect = step["expect"]
            if isinstance(expect, dict) and "error" in expect:
                try:
                    client.execute_command(*args)
                except redis.exceptions.ResponseError as error:
                    check(
                        _error_message(error) == expect["error"],
                        f"{label}: expected error {expect['error']!r}, got {_error_message(error)!r}",
                    )
                else:
                    raise AssertionError(f"{label}: expected {args[0]} to fail")
                continue
            _check(expect, client.execute_command(*args), label)
    finally:
        client.close()


def _differential_cases(node: YydsNode) -> list[tuple[str, object]]:
    table = json.loads(SCENARIO_PATH.read_text(encoding="utf-8"))
    return [
        (f"differential/{scenario['name']}", (lambda sc: lambda: _run_scenario(node, sc))(scenario))
        for scenario in table["scenarios"]
    ]


def _case_cross_connection(node: YydsNode) -> None:
    writer = _client(node)
    reader = _client(node)
    try:
        check(writer.set("py-shared", "v", px=60_000) is True, "writer set failed")
        observed = reader.pttl("py-shared")
        check(55_000 < observed <= 60_000, f"second connection must see the deadline, got {observed}")

        check(writer.set("py-sliding", "v", ex=1) is True, "sliding set failed")
        for _ in range(3):
            time.sleep(0.4)
            check(reader.execute_command("GETEX", "py-sliding", "EX", "1") == "v", "GETEX touch failed")
        check(writer.get("py-sliding") == "v", "sliding renewal must keep the key alive")

        check(writer.expire("py-shared", 0) == 1, "expire zero must delete")
        check(reader.get("py-shared") is None, "the delete must be visible cross-connection")
    finally:
        writer.close()
        reader.close()


def _case_reconnect(node: YydsNode) -> None:
    first = _client(node)
    first.set("py-reconnect", "v", px=30_000)
    first.close()

    second = _client(node)
    try:
        check(second.get("py-reconnect") == "v", "value must survive a reconnect")
        pttl = second.pttl("py-reconnect")
        check(0 < pttl <= 30_000, f"deadline must survive reconnect, got {pttl}")
    finally:
        second.close()


def _case_multi_exec(node: YydsNode) -> None:
    client = _client(node)
    try:
        at_seconds = int(time.time()) + 1_000
        pipe = client.pipeline(transaction=True)
        pipe.execute_command("SET", "py-tx", "v", "EX", 100)
        pipe.execute_command("GETEX", "py-tx", "EX", 500)
        pipe.execute_command("EXPIREAT", "py-tx", at_seconds)
        results = pipe.execute()
        check(results == [True, "v", 1], f"EXEC replay must preserve order, got {results!r}")
        ttl = client.ttl("py-tx")
        check(990 < ttl <= 1_000, f"EXEC replayed deadline, got {ttl}")

        # A runtime failure lands inside the EXEC result array and does not abort the rest.
        client.set("py-tx-text", "abc")
        pipe = client.pipeline(transaction=True)
        pipe.execute_command("INCR", "py-tx-text")
        pipe.execute_command("SET", "py-tx-after", "v")
        results = pipe.execute(raise_on_error=False)
        check(len(results) == 2, f"EXEC must return one element per command, got {results!r}")
        check(isinstance(results[0], redis.exceptions.ResponseError), f"runtime error must stay in the array, got {results[0]!r}")
        check(results[1] is True, f"the command after the error still applies, got {results[1]!r}")
        check(client.get("py-tx-after") == "v", "the post-error write must be visible")

        # Empty transactions and client-side queue resets behave.
        check(client.pipeline(transaction=True).execute() == [], "empty EXEC must return an empty array")
        pipe = client.pipeline(transaction=True)
        pipe.execute_command("SET", "py-tx-drop", "v")
        pipe.reset()
        check(client.get("py-tx-drop") is None, "a reset queue never reaches the server")
    finally:
        client.close()


def _case_multi_nested(node: YydsNode) -> None:
    client = _client(node)
    try:
        check(client.execute_command("MULTI") in (True, "OK"), "MULTI must answer OK")
        # redis-py maps the `QUEUED` reply to False, so only the queue's effect
        # is asserted: the nested call below must be refused, not the write.
        client.execute_command("SET", "py-nested", "v")

        # Real Redis refuses a second MULTI without dropping the open queue.
        try:
            client.execute_command("MULTI")
        except redis.exceptions.ResponseError as error:
            check(
                _error_message(error) == "ERR MULTI calls can not be nested",
                f"nested MULTI must be refused, got {_error_message(error)!r}",
            )
        else:
            raise AssertionError("nested MULTI must be refused")

        executed = client.execute_command("EXEC")
        check(isinstance(executed, list) and len(executed) == 1, f"EXEC must replay the one queued command, got {executed!r}")
        check(client.get("py-nested") == "v", "the queued write must commit on EXEC")
    finally:
        client.close()


def _case_multi_isolation(node: YydsNode) -> None:
    writer = _client(node)
    reader = _client(node)
    try:
        writer.set("py-tx-isolation", "old")

        writer.execute_command("MULTI")
        writer.execute_command("SET", "py-tx-isolation", "new")
        # The queued write belongs to the writer's connection only: a second
        # connection keeps reading the committed value until EXEC runs.
        check(reader.get("py-tx-isolation") == "old", "a queued write must not leak to another connection")

        executed = writer.execute_command("EXEC")
        check(isinstance(executed, list) and len(executed) == 1, f"EXEC must replay the one queued command, got {executed!r}")
        check(reader.get("py-tx-isolation") == "new", "EXEC must publish the queued write to every connection")
    finally:
        writer.close()
        reader.close()


def _case_discard(node: YydsNode) -> None:
    client = _client(node)
    try:
        try:
            client.execute_command("DISCARD")
        except redis.exceptions.ResponseError as error:
            check(
                _error_message(error) == "ERR DISCARD without MULTI",
                f"DISCARD outside a MULTI must be refused, got {_error_message(error)!r}",
            )
        else:
            raise AssertionError("DISCARD outside a MULTI must be refused")

        client.set("py-discard", "before")
        client.execute_command("MULTI")
        client.execute_command("SET", "py-discard", "after")
        check(client.execute_command("DISCARD") in (True, "OK"), "DISCARD must answer OK")
        check(client.get("py-discard") == "before", "DISCARD must drop the queued write")
    finally:
        client.close()


def _case_exec_empty(node: YydsNode) -> None:
    client = _client(node)
    try:
        check(client.execute_command("MULTI") in (True, "OK"), "MULTI must answer OK")
        check(client.execute_command("EXEC") == [], "EXEC on an empty queue must return an empty array")
        check(client.set("py-empty-tx", "v") is True, "the connection must stay usable after an empty EXEC")
        check(client.get("py-empty-tx") == "v", "a post-EXEC write must land")
    finally:
        client.close()


def _case_watch_abort(node: YydsNode, protocol: int) -> None:
    writer = _client(node, protocol)
    rival = _client(node, protocol)
    key = f"py-watch-abort-{protocol}"
    try:
        check(writer.execute_command("WATCH", key) in (True, "OK"), "WATCH must answer OK")
        check(rival.set(key, "rival") is True, "the rival write must land")
        check(writer.execute_command("MULTI") in (True, "OK"), "MULTI must answer OK")
        writer.execute_command("SET", key, "transaction")

        executed = writer.execute_command("EXEC")
        check(executed is None, f"EXEC must answer nil after a watched write, got {executed!r}")
        check(rival.get(key) == "rival", "an aborted transaction must not apply its queue")
        check(writer.ping() is True, "the connection must keep answering after an aborted EXEC")
    finally:
        writer.close()
        rival.close()


def _case_watch_unwatch_and_discard(node: YydsNode) -> None:
    watcher = _client(node)
    rival = _client(node)
    try:
        # UNWATCH releases the pin, so a rival write no longer voids the transaction.
        watcher.execute_command("WATCH", "py-watch-unwatch")
        rival.set("py-watch-unwatch", "rival")
        check(watcher.execute_command("UNWATCH") in (True, "OK"), "UNWATCH must answer OK")
        watcher.execute_command("MULTI")
        watcher.execute_command("SET", "py-watch-unwatch", "committed")
        executed = watcher.execute_command("EXEC")
        check(
            isinstance(executed, list) and len(executed) == 1,
            f"EXEC after UNWATCH must run the queue, got {executed!r}",
        )
        check(rival.get("py-watch-unwatch") == "committed", "the transaction must land after UNWATCH")

        # DISCARD also drops the pin: the next transaction runs despite the rival write.
        watcher.execute_command("WATCH", "py-watch-discard")
        rival.set("py-watch-discard", "dirty")
        watcher.execute_command("MULTI")
        watcher.execute_command("SET", "py-watch-discard", "queued")
        check(watcher.execute_command("DISCARD") in (True, "OK"), "DISCARD must answer OK")
        watcher.execute_command("MULTI")
        watcher.execute_command("SET", "py-watch-discard", "after-discard")
        executed = watcher.execute_command("EXEC")
        check(
            isinstance(executed, list) and len(executed) == 1,
            f"EXEC after DISCARD must run the queue, got {executed!r}",
        )
        check(rival.get("py-watch-discard") == "after-discard", "the transaction must land after DISCARD")
    finally:
        watcher.close()
        rival.close()


def _case_watch_inside_multi(node: YydsNode) -> None:
    client = _client(node)
    try:
        client.execute_command("MULTI")
        try:
            client.execute_command("WATCH", "py-watch-guard")
        except redis.exceptions.ResponseError as error:
            check(
                _error_message(error) == "ERR WATCH inside MULTI is not allowed",
                f"WATCH inside MULTI must be refused, got {_error_message(error)!r}",
            )
        else:
            raise AssertionError("WATCH inside MULTI must be refused")
        check(client.execute_command("DISCARD") in (True, "OK"), "DISCARD must close the rejected transaction")
    finally:
        client.close()


def _case_getex_options(node: YydsNode) -> None:
    client = _client(node)
    try:
        key = "py-getex-options"
        check(client.execute_command("SET", key, "v", "PX", "120000") is True, "SET must answer OK")

        check(client.execute_command("GETEX", key, "EX", "60") == "v", "GETEX EX must return the value")
        pttl = client.pttl(key)
        check(55_000 < pttl <= 60_000, f"GETEX EX must set a 60s deadline, got {pttl}")

        check(client.execute_command("GETEX", key, "PX", "30000") == "v", "GETEX PX must return the value")
        pttl = client.pttl(key)
        check(25_000 < pttl <= 30_000, f"GETEX PX must set a 30s deadline, got {pttl}")

        at_seconds = int(time.time()) + 120
        check(client.execute_command("GETEX", key, "EXAT", str(at_seconds)) == "v", "GETEX EXAT must return the value")
        pttl = client.pttl(key)
        check(110_000 < pttl <= 120_000, f"GETEX EXAT must set an absolute 120s deadline, got {pttl}")

        at_ms = int(time.time() * 1000) + 90_000
        check(client.execute_command("GETEX", key, "PXAT", str(at_ms)) == "v", "GETEX PXAT must return the value")
        pttl = client.pttl(key)
        check(80_000 < pttl <= 90_000, f"GETEX PXAT must set an absolute 90s deadline, got {pttl}")

        check(client.execute_command("GETEX", key, "PERSIST") == "v", "GETEX PERSIST must return the value")
        check(client.ttl(key) == -1, "GETEX PERSIST must clear the deadline")
        check(client.get(key) == "v", "GETEX PERSIST must keep the value")
    finally:
        client.close()


def _case_getex_without_options(node: YydsNode) -> None:
    client = _client(node)
    try:
        key = "py-getex-plain"
        check(client.execute_command("SET", key, "v", "PX", "120000") is True, "SET must answer OK")
        before = client.pttl(key)
        check(client.execute_command("GETEX", key) == "v", "GETEX without options must return the value")
        after = client.pttl(key)
        check(100_000 < after <= before, f"GETEX without options must keep the deadline, got {before} then {after}")
        check(before - after < 5_000, f"GETEX without options must not reset the deadline, got {before} then {after}")
    finally:
        client.close()


def _case_getex_conflicting_options(node: YydsNode) -> None:
    client = _client(node)
    try:
        key = "py-getex-conflict"
        check(client.execute_command("SET", key, "v", "PX", "120000") is True, "SET must answer OK")
        before = client.pttl(key)

        pairs = (
            ("EX+PX", ("GETEX", key, "EX", "60", "PX", "60000")),
            ("PERSIST+EXAT", ("GETEX", key, "PERSIST", "EXAT", "60")),
        )
        for label, args in pairs:
            try:
                client.execute_command(*args)
            except redis.exceptions.ResponseError as error:
                check(_error_message(error) == "ERR syntax error", f"{label}: expected a syntax error, got {_error_message(error)!r}")
            else:
                raise AssertionError(f"{label}: two expiry options must be refused")
            check(client.get(key) == "v", f"{label}: the rejected command must keep the value")

        after = client.pttl(key)
        check(100_000 < after <= before and before - after < 2_000, f"a rejected GETEX must keep the deadline, got {before} then {after}")
    finally:
        client.close()


def _case_getex_missing_and_invalid(node: YydsNode) -> None:
    client = _client(node)
    try:
        nil_cases = (
            ("plain", ("GETEX", "py-getex-missing")),
            ("invalid-number", ("GETEX", "py-getex-missing", "EX", "not-a-number")),
            ("zero", ("GETEX", "py-getex-missing", "EX", "0")),
            ("exat-zero", ("GETEX", "py-getex-missing", "EXAT", "0")),
        )
        for label, args in nil_cases:
            check(client.execute_command(*args) is None, f"GETEX on a missing key must answer nil ({label})")

        key = "py-getex-invalid"
        check(client.execute_command("SET", key, "v", "PX", "100000") is True, "SET must answer OK")
        before = client.pttl(key)
        failures = (
            (("GETEX", key, "EX", "not-a-number"), "ERR value is not an integer or out of range"),
            (("GETEX", key, "EX", "0"), "ERR invalid expire time in 'getex' command"),
            (("GETEX", key, "PX", "-5"), "ERR invalid expire time in 'getex' command"),
            (("GETEX", key, "EX"), "ERR syntax error"),
        )
        for args, message in failures:
            try:
                client.execute_command(*args)
            except redis.exceptions.ResponseError as error:
                check(_error_message(error) == message, f"{args[2]}: expected {message!r}, got {_error_message(error)!r}")
            else:
                raise AssertionError(f"{args[2]}: an existing key must report {message!r}")

        check(client.get(key) == "v", "a rejected GETEX must keep the value")
        after = client.pttl(key)
        check(90_000 < after <= before and before - after < 2_000, f"a rejected GETEX must keep the deadline, got {before} then {after}")
    finally:
        client.close()


def _case_expire_deadlines(node: YydsNode) -> None:
    client = _client(node)
    try:
        key = "py-expire-deadlines"
        check(client.execute_command("SET", key, "v", "PX", "120000") is True, "SET must answer OK")

        check(client.execute_command("EXPIRE", key, "60") == 1, "EXPIRE must report one applied key")
        pttl = client.pttl(key)
        check(55_000 < pttl <= 60_000, f"EXPIRE deadline was {pttl}")
        ttl = client.ttl(key)
        check(55 <= ttl <= 60, f"EXPIRE seconds deadline was {ttl}")

        check(client.execute_command("PEXPIRE", key, "30000") == 1, "PEXPIRE must report one applied key")
        pttl = client.pttl(key)
        check(25_000 < pttl <= 30_000, f"PEXPIRE deadline was {pttl}")

        at_seconds = int(time.time()) + 120
        check(client.execute_command("EXPIREAT", key, str(at_seconds)) == 1, "EXPIREAT must report one applied key")
        pttl = client.pttl(key)
        check(110_000 < pttl <= 120_000, f"EXPIREAT deadline was {pttl}")

        at_ms = int(time.time() * 1000) + 90_000
        check(client.execute_command("PEXPIREAT", key, str(at_ms)) == 1, "PEXPIREAT must report one applied key")
        pttl = client.pttl(key)
        check(80_000 < pttl <= 90_000, f"PEXPIREAT deadline was {pttl}")

        past_ms = int(time.time() * 1000) - 1_000
        check(client.execute_command("PEXPIREAT", key, str(past_ms)) == 1, "a past deadline must be accepted")
        check(client.exists(key) == 0, "a past deadline must remove the key")
        check(client.get(key) is None, "a past deadline must clear the value")

        check(client.execute_command("SET", key, "v", "PX", "120000") is True, "SET must answer OK")
        check(client.execute_command("EXPIRE", key, "0") == 1, "a zero TTL must report one applied key")
        check(client.exists(key) == 0, "a zero TTL must remove the key")
        check(client.execute_command("EXPIRE", "py-expire-absent", "10") == 0, "EXPIRE on a missing key must report zero")
    finally:
        client.close()


def _case_ttl_sentinels_and_persist(node: YydsNode) -> None:
    client = _client(node)
    try:
        key = "py-expire-sentinels"
        check(client.execute_command("SET", key, "v", "PX", "120000") is True, "SET must answer OK")
        check(client.pttl(key) > 100_000, "the key must carry a deadline before PERSIST")

        check(client.execute_command("PERSIST", key) == 1, "PERSIST must report the cleared deadline")
        check(client.ttl(key) == -1, "TTL must answer -1 without a deadline")
        check(client.pttl(key) == -1, "PTTL must answer -1 without a deadline")
        check(client.get(key) == "v", "PERSIST must keep the value")

        check(client.execute_command("PERSIST", key) == 0, "a key without a deadline cannot be persisted again")

        check(client.ttl("py-expire-absent") == -2, "TTL must answer -2 for a missing key")
        check(client.pttl("py-expire-absent") == -2, "PTTL must answer -2 for a missing key")
    finally:
        client.close()


def _case_set_keepttl(node: YydsNode) -> None:
    client = _client(node)
    try:
        key = "py-expire-keepttl"
        check(client.execute_command("SET", key, "v", "PX", "120000") is True, "SET must answer OK")
        before = client.pttl(key)

        check(client.execute_command("SET", key, "v2", "KEEPTTL") is True, "SET KEEPTTL must answer OK")
        after = client.pttl(key)
        check(100_000 < after <= before and before - after < 2_000, f"KEEPTTL changed the deadline from {before} to {after}")
        check(client.get(key) == "v2", "KEEPTTL must replace the value")

        check(client.execute_command("SET", key, "v3") is True, "a plain SET must answer OK")
        check(client.ttl(key) == -1, "a plain SET must clear the deadline")

        check(client.execute_command("SET", "py-expire-no-ttl", "v") is True, "SET must answer OK")
        check(client.execute_command("SET", "py-expire-no-ttl", "v2", "KEEPTTL") is True, "SET KEEPTTL must answer OK")
        check(client.pttl("py-expire-no-ttl") == -1, "KEEPTTL must not invent a deadline")
    finally:
        client.close()


def _case_expired_key_surface(node: YydsNode) -> None:
    client = _client(node)
    try:
        key = "py-expire-surface"
        check(client.execute_command("SET", key, "v", "PX", "100") is True, "SET must answer OK")
        time.sleep(0.25)

        check(client.exists(key) == 0, "EXISTS must hide an expired key")
        check(client.get(key) is None, "GET must answer nil for an expired key")
        check(client.type(key) == "none", "TYPE must answer none for an expired key")
        check(client.ttl(key) == -2, "TTL must answer -2 for an expired key")
        check(client.pttl(key) == -2, "PTTL must answer -2 for an expired key")

        check(client.execute_command("SET", key, "v", "PX", "60000") is True, "SET must answer OK")
        check(client.exists(key) == 1, "EXISTS must report a live key")
        check(client.type(key) == "string", "TYPE must report a live string")
        check(client.ttl(key) > 0, "TTL must stay positive for a live key")
    finally:
        client.close()


def _case_rename_rotation(node: YydsNode) -> None:
    client = _client(node)
    reader = _client(node)
    try:
        client.execute_command("SET", "py-rn-src", "session", "PX", "45000")
        check(client.execute_command("RENAME", "py-rn-src", "py-rn-dst") is True, "RENAME must answer OK")
        check(client.get("py-rn-dst") == "session", "the value must move with the key")
        check(client.get("py-rn-src") is None, "the source must be gone")
        pttl = client.pttl("py-rn-dst")
        check(0 < pttl <= 45_000, f"the remaining deadline must move too, got {pttl}")
        check(reader.get("py-rn-dst") == "session", "the move must be visible cross-connection")

        client.execute_command("SET", "py-rn-nx", "keep", "PX", "45000")
        client.execute_command("SET", "py-rn-blocker", "old")
        check(client.execute_command("RENAMENX", "py-rn-nx", "py-rn-blocker") == 0, "an occupied destination must reject the move")
        check(client.get("py-rn-nx") == "keep", "the rejected source must stay")
        check(client.get("py-rn-blocker") == "old", "the rejected destination must stay")
        check(client.execute_command("RENAMENX", "py-rn-nx", "py-rn-free") == 1, "a free destination must accept the move")
        check(client.get("py-rn-free") == "keep", "RENAMENX must move the value")
        check(client.get("py-rn-nx") is None, "RENAMENX must delete the source")

        pipe = client.pipeline(transaction=True)
        pipe.execute_command("RENAME", "py-rn-missing", "py-rn-target")
        pipe.ping()
        results = pipe.execute(raise_on_error=False)
        check(len(results) == 2, f"EXEC must return one element per command, got {results!r}")
        check(isinstance(results[0], redis.exceptions.ResponseError), f"the missing source must fail inside EXEC, got {results[0]!r}")
        check(results[1] is True, f"the command after the failure must still apply, got {results[1]!r}")
    finally:
        client.close()
        reader.close()


def _case_error_paths(node: YydsNode) -> None:
    client = _client(node)
    try:
        client.set("py-err", "v")
        failures = {
            ("EXPIREAT", "py-err", "9223372036854776"): "ERR invalid expire time in 'expireat' command",
            ("EXPIREAT", "py-err", "abc"): "ERR value is not an integer or out of range",
            ("SETEX", "py-err", "0", "v"): "ERR invalid expire time in 'setex' command",
            ("EXPIREAT",): "ERR wrong number of arguments for 'expireat' command",
            ("TTL", "py-err", "extra"): "ERR wrong number of arguments for 'ttl' command",
        }
        for args, message in failures.items():
            try:
                client.execute_command(*args)
            except redis.exceptions.ResponseError as error:
                check(_error_message(error) == message, f"{args[0]}: expected {message!r}, got {_error_message(error)!r}")
            else:
                raise AssertionError(f"{args[0]} must fail with {message!r}")
        check(client.persist("py-persist-missing") == 0, "PERSIST on a missing key must answer 0")
    finally:
        client.close()


def _shard_size(data_dir: Path) -> int:
    return (data_dir / SHARD_FILE).stat().st_size


def _case_background_reclaim(node: YydsNode) -> None:
    client = _client(node)
    prober = _client(node)
    try:
        check(client.set("py-sweeper-keep", "keep") is True, "survivor write failed")
        total = 1_500
        pipe = client.pipeline(transaction=False)
        for index in range(total):
            # Long enough to survive the write phase, short enough to expire together.
            pipe.execute_command("SET", f"py-bulk-{index}", "v" * 32, "PX", 4_000)
        results = pipe.execute()
        check(all(reply is True for reply in results), "every bulk write must be acked")
        peak = _shard_size(node.data_dir)

        deadline = time.monotonic() + 15.0
        current = peak
        while time.monotonic() < deadline:
            check(prober.ping() is True, "foreground traffic must keep flowing during reclamation")
            time.sleep(0.1)
            current = _shard_size(node.data_dir)
            if current * 2 < peak:
                break
        check(current * 2 < peak, f"background reclaim must shrink the shard without reads (peak={peak} current={current})")
        check(client.get("py-sweeper-keep") == "keep", "the survivor must stay readable")
        check(client.get("py-bulk-0") is None, "reclaimed keys read as absent")
        check(client.ttl("py-bulk-0") == -2, "reclaimed keys report no deadline")
    finally:
        client.close()
        prober.close()


def _case_restart_persistence() -> None:
    data_dir = Path(tempfile.mkdtemp(prefix="yyds-redis-restart-"))
    identity = {"cluster_id": "cluster-py-restart", "node_id": "node-py-restart"}
    try:
        first = start_redis_node(data_dir=data_dir, **identity)
        writer = _client(first)
        writer.set("py-restart-keep", "v")
        # Rotate the session key before shutdown: the value and deadline must survive the restart.
        writer.execute_command("SET", "py-restart-src", "v", "PX", 45_000)
        writer.execute_command("RENAME", "py-restart-src", "py-restart-long")
        writer.execute_command("SET", "py-restart-gone", "v", "PX", 600)
        writer.close()
        time.sleep(0.9)
        first.stop(wipe=False)

        previous = 45_000
        for _ in range(2):
            node = start_redis_node(data_dir=data_dir, **identity)
            client = _client(node)
            try:
                check(client.get("py-restart-keep") == "v", "non-expiring data survives restarts")
                check(client.get("py-restart-long") == "v", "a live deadline survives restarts")
                pttl = client.pttl("py-restart-long")
                check(0 < pttl <= previous, f"the deadline keeps counting across restarts (previous={previous} now={pttl})")
                previous = pttl
                check(client.get("py-restart-gone") is None, "a deadline elapsed while down stays absent")
            finally:
                client.close()
                node.stop(wipe=False)
    finally:
        shutil.rmtree(data_dir, ignore_errors=True)


def run_redis(node: YydsNode | None = None) -> list[CaseResult]:
    """Runs the redis-py suite. Lifecycle cases manage their own nodes."""

    owned = node is None
    if owned:
        node = start_redis_node(sweep_interval_ms=50)
    assert node is not None
    try:
        cases: list[tuple[str, object]] = _differential_cases(node)
        cases += [
            ("cross-connection", lambda: _case_cross_connection(node)),
            ("reconnect", lambda: _case_reconnect(node)),
            ("multi-exec", lambda: _case_multi_exec(node)),
            ("multi-nested", lambda: _case_multi_nested(node)),
            ("multi-isolation", lambda: _case_multi_isolation(node)),
            ("discard", lambda: _case_discard(node)),
            ("exec-empty", lambda: _case_exec_empty(node)),
            ("watch-abort-resp2", lambda: _case_watch_abort(node, 2)),
            ("watch-abort-resp3", lambda: _case_watch_abort(node, 3)),
            ("watch-unwatch-and-discard", lambda: _case_watch_unwatch_and_discard(node)),
            ("watch-inside-multi", lambda: _case_watch_inside_multi(node)),
            ("getex-option-ttls", lambda: _case_getex_options(node)),
            ("getex-without-options", lambda: _case_getex_without_options(node)),
            ("getex-conflicting-options", lambda: _case_getex_conflicting_options(node)),
            ("getex-missing-and-invalid", lambda: _case_getex_missing_and_invalid(node)),
            ("expire-deadlines", lambda: _case_expire_deadlines(node)),
            ("ttl-sentinels-and-persist", lambda: _case_ttl_sentinels_and_persist(node)),
            ("set-keepttl", lambda: _case_set_keepttl(node)),
            ("expired-key-surface", lambda: _case_expired_key_surface(node)),
            ("rename-rotation", lambda: _case_rename_rotation(node)),
            ("error-paths", lambda: _case_error_paths(node)),
            ("background-reclaim", lambda: _case_background_reclaim(node)),
            ("restart-persistence", _case_restart_persistence),
        ]
        return run_cases("redis-py", cases)
    finally:
        if owned:
            node.stop()
