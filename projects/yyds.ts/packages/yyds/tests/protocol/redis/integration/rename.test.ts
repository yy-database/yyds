import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { after, before } from "node:test";

import { startYydsNode, type YydsNode } from "@yydb/yyds-node-harness";

import { expectArray, expectBulk, expectError, expectInteger, expectSimple, RawClient } from "../support/raw-client.ts";

/**
 * Session rotation: a live record moves to a new key with its remaining deadline intact.
 * The wire assertions stay here because the shared scenario table only covers single-command
 * results, not the overwrite/same-key matrix this slice depends on.
 */

let node: YydsNode;
let client: RawClient;

before(async () => {
    node = await startYydsNode();
    client = await RawClient.connect(node.redis.host, node.redis.port);
});

after(async () => {
    client.destroy();
    await node.stop();
});

test("RENAME moves the value and the deadline, RENAMENX respects an occupied destination", async () => {
    expectSimple(await client.command("SET", "rn-src", "payload", "PX", "100000"), "OK");
    expectSimple(await client.command("SET", "rn-dst", "old"), "OK");

    expectSimple(await client.command("RENAME", "rn-src", "rn-dst"), "OK");
    expectBulk(await client.command("GET", "rn-dst"), "payload");
    expectBulk(await client.command("GET", "rn-src"), null);
    const moved = await client.command("PTTL", "rn-dst");
    assert.ok(
        moved.kind === "integer" && moved.value > 0 && moved.value <= 100_000,
        `the move must keep the remaining deadline, got ${JSON.stringify(moved)}`,
    );

    // An occupied destination makes RENAMENX a no-op: both records stay untouched.
    expectSimple(await client.command("SET", "rn-nx-src", "keep", "PX", "100000"), "OK");
    expectSimple(await client.command("SET", "rn-nx-dst", "blocker"), "OK");
    expectInteger(await client.command("RENAMENX", "rn-nx-src", "rn-nx-dst"), 0);
    expectBulk(await client.command("GET", "rn-nx-src"), "keep");
    expectBulk(await client.command("GET", "rn-nx-dst"), "blocker");

    // A free destination moves the record and reports the change.
    expectInteger(await client.command("RENAMENX", "rn-nx-src", "rn-nx-free"), 1);
    expectBulk(await client.command("GET", "rn-nx-free"), "keep");
    expectBulk(await client.command("GET", "rn-nx-src"), null);
    const nxDeadline = await client.command("PTTL", "rn-nx-free");
    assert.ok(
        nxDeadline.kind === "integer" && nxDeadline.value > 0 && nxDeadline.value <= 100_000,
        `RENAMENX must keep the remaining deadline, got ${JSON.stringify(nxDeadline)}`,
    );

    // Moving a key onto itself: RENAME succeeds without touching the record, RENAMENX reports
    // the destination as occupied.
    expectSimple(await client.command("SET", "rn-same", "v", "PX", "100000"), "OK");
    expectSimple(await client.command("RENAME", "rn-same", "rn-same"), "OK");
    expectInteger(await client.command("RENAMENX", "rn-same", "rn-same"), 0);
    expectBulk(await client.command("GET", "rn-same"), "v");
    const sameDeadline = await client.command("PTTL", "rn-same");
    assert.ok(
        sameDeadline.kind === "integer" && sameDeadline.value > 0 && sameDeadline.value <= 100_000,
        `a self rename must not clear the deadline, got ${JSON.stringify(sameDeadline)}`,
    );
});

test("RENAME reports a missing source and validates arity", async () => {
    expectError(await client.command("RENAME", "rn-missing", "rn-target"), "ERR no such key");
    expectError(await client.command("RENAMENX", "rn-missing", "rn-target"), "ERR no such key");
    expectError(await client.command("RENAME", "rn-only"), "ERR wrong number of arguments for 'rename' command");
    expectError(
        await client.command("RENAMENX", "rn-a", "rn-b", "rn-c"),
        "ERR wrong number of arguments for 'renamenx' command",
    );
});

test("MULTI queues RENAME and defers source lookup to EXEC", async () => {
    expectSimple(await client.command("MULTI"), "OK");
    expectSimple(await client.command("RENAME", "rn-tx-missing", "rn-tx-target"), "QUEUED");
    expectSimple(await client.command("PING"), "QUEUED");
    expectArray(await client.command("EXEC"), [
        { kind: "error", value: "ERR no such key" },
        { kind: "simple", value: "PONG" },
    ]);

    // A malformed arity is rejected while queuing and poisons the transaction: upstream
    // answers `EXEC` with `EXECABORT` instead of replaying the rest of the queue.
    expectSimple(await client.command("MULTI"), "OK");
    expectError(await client.command("RENAME", "rn-tx-only"), "ERR wrong number of arguments for 'rename' command");
    expectSimple(await client.command("PING"), "QUEUED");
    expectError(await client.command("EXEC"), "EXECABORT Transaction discarded because of previous errors.");
    expectSimple(await client.command("PING"), "PONG");
});

test("a rename and its deadline survive a node restart", { timeout: 120_000 }, async () => {
    const dataDir = await mkdtemp(join(tmpdir(), "yyds-rename-restart-"));
    const identity = { clusterId: "cluster-rename-restart", nodeId: "node-rename-restart" };
    try {
        const first = await startYydsNode({ dataDir, ...identity });
        const writer = await RawClient.connect(first.redis.host, first.redis.port);
        try {
            expectSimple(await writer.command("SET", "rn-restart-src", "session", "PX", "45000"), "OK");
            expectSimple(await writer.command("RENAME", "rn-restart-src", "rn-restart-dst"), "OK");
            const before = await writer.command("PTTL", "rn-restart-dst");
            assert.ok(before.kind === "integer" && before.value > 0, "the moved record must carry a deadline");
        } finally {
            writer.destroy();
        }
        await first.stop({ wipe: false });

        const second = await startYydsNode({ dataDir, ...identity });
        const reader = await RawClient.connect(second.redis.host, second.redis.port);
        try {
            expectBulk(await reader.command("GET", "rn-restart-dst"), "session");
            expectBulk(await reader.command("GET", "rn-restart-src"), null);
            const remaining = await reader.command("PTTL", "rn-restart-dst");
            assert.ok(
                remaining.kind === "integer" && remaining.value > 0 && remaining.value <= 45_000,
                `the deadline must keep counting across the restart, got ${JSON.stringify(remaining)}`,
            );
        } finally {
            reader.destroy();
            await second.stop();
        }
    } finally {
        await rm(dataDir, { recursive: true, force: true });
    }
});
