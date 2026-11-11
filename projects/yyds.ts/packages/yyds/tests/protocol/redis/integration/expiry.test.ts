import assert from "node:assert/strict";
import test, { after, before } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { startYydsNode, type YydsNode } from "@yydb/yyds-node-harness";
import { createClient } from "redis";

import { expectArray, expectBulk, expectError, expectInteger, expectSimple, RawClient } from "../support/raw-client.ts";

let node: YydsNode;

function driver() {
    const client = createClient({
        url: `redis://${node.redis.host}:${node.redis.port}`,
        RESP: 2,
        disableClientInfo: true,
    });
    client.on("error", () => undefined);
    return client;
}

before(async () => {
    node = await startYydsNode();
});

after(async () => {
    await node.stop();
});

test("relative and absolute deadlines count down through a real client", async () => {
    const client = driver();
    await client.connect();
    try {
        assert.equal(await client.set("exp-rel", "v", { EX: 100 }), "OK");
        const ttl = await client.ttl("exp-rel");
        assert.ok(ttl > 90 && ttl <= 100, `unexpected TTL ${ttl}`);

        // Absolute deadlines pin the same wall-clock boundary.
        const atSeconds = Math.floor(Date.now() / 1000) + 100;
        assert.equal(await client.sendCommand(["SET", "exp-abs", "v"]), "OK");
        assert.equal(await client.sendCommand(["EXPIREAT", "exp-abs", String(atSeconds)]), 1);
        const absTtl = await client.ttl("exp-abs");
        assert.ok(absTtl > 90 && absTtl <= 100, `unexpected absolute TTL ${absTtl}`);

        const atMs = Date.now() + 100_000;
        assert.equal(await client.sendCommand(["SET", "exp-abs-ms", "v", "PXAT", String(atMs)]), "OK");
        const pttl = Number(await client.sendCommand(["PTTL", "exp-abs-ms"]));
        assert.ok(pttl > 95_000 && pttl <= 100_000, `unexpected absolute PTTL ${pttl}`);

        // A past absolute deadline writes the record but reads as absent immediately.
        assert.equal(await client.sendCommand(["SET", "exp-past", "v", "EXAT", "1"]), "OK");
        assert.equal(await client.get("exp-past"), null);
        assert.equal(await client.ttl("exp-past"), -2);

        // A non-positive absolute deadline deletes the key, matching `EXPIRE <= 0`.
        assert.equal(await client.set("exp-zero", "v"), "OK");
        assert.equal(await client.sendCommand(["EXPIREAT", "exp-zero", "0"]), 1);
        assert.equal(await client.get("exp-zero"), null);
        assert.equal(await client.sendCommand(["EXPIREAT", "exp-zero", "-5"]), 0);
    } finally {
        await client.close();
    }
});

test("GETEX reads and touches through every option form", async () => {
    const client = driver();
    await client.connect();
    try {
        assert.equal(await client.set("gx", "payload", { EX: 100 }), "OK");

        assert.equal(await client.sendCommand(["GETEX", "gx", "EX", "500"]), "payload");
        const touched = await client.ttl("gx");
        assert.ok(touched > 490 && touched <= 500, `GETEX EX must re-touch the TTL, got ${touched}`);

        assert.equal(await client.sendCommand(["GETEX", "gx", "PERSIST"]), "payload");
        assert.equal(await client.ttl("gx"), -1);

        assert.equal(await client.sendCommand(["GETEX", "gx"]), "payload");
        assert.equal(await client.ttl("gx"), -1);

        const atSeconds = Math.floor(Date.now() / 1000) + 200;
        assert.equal(await client.sendCommand(["GETEX", "gx", "EXAT", String(atSeconds)]), "payload");
        const absTtl = await client.ttl("gx");
        assert.ok(absTtl > 190 && absTtl <= 200, `GETEX EXAT must pin the deadline, got ${absTtl}`);

        const atMs = Date.now() + 60_000;
        assert.equal(await client.sendCommand(["GETEX", "gx", "PXAT", String(atMs)]), "payload");
        const absPttl = Number(await client.sendCommand(["PTTL", "gx"]));
        assert.ok(absPttl > 55_000 && absPttl <= 60_000, `GETEX PXAT must pin the deadline, got ${absPttl}`);

        // A missing key reads as nil and touches nothing.
        assert.equal(await client.sendCommand(["GETEX", "gx-missing", "EX", "10"]), null);
        assert.equal(await client.exists("gx-missing"), 0);

        await assert.rejects(client.sendCommand(["GETEX", "gx", "EX", "0"]), /invalid expire time in 'getex'/);
        await assert.rejects(client.sendCommand(["GETEX", "gx", "FOO"]), /syntax error/);
        await assert.rejects(client.sendCommand(["GETEX", "gx", "EX", "1", "PERSIST"]), /syntax error/);
    } finally {
        await client.close();
    }
});

test("expiry state is visible across simultaneous connections", async () => {
    const writer = driver();
    const reader = driver();
    await writer.connect();
    await reader.connect();
    try {
        await writer.set("shared-ttl", "v", { PX: 60_000 });
        const observed = Number(await reader.sendCommand(["PTTL", "shared-ttl"]));
        assert.ok(observed > 55_000 && observed <= 60_000, `second connection must see the deadline, got ${observed}`);

        // Sliding renewal: reads through one connection keep the key alive for the other.
        await writer.sendCommand(["SET", "sliding", "v", "EX", "1"]);
        for (let touch = 0; touch < 3; touch += 1) {
            await sleep(400);
            assert.equal(await reader.sendCommand(["GETEX", "sliding", "EX", "1"]), "v");
        }
        assert.equal(await writer.get("sliding"), "v");

        await writer.sendCommand(["EXPIRE", "shared-ttl", "0"]);
        assert.equal(await reader.get("shared-ttl"), null);
    } finally {
        await writer.close();
        await reader.close();
    }
});

test("state survives a client reconnect while the deadline keeps counting", async () => {
    const first = driver();
    await first.connect();
    await first.set("reconnect-key", "v", { PX: 30_000 });
    await first.close();

    const second = driver();
    await second.connect();
    try {
        assert.equal(await second.get("reconnect-key"), "v");
        const pttl = Number(await second.sendCommand(["PTTL", "reconnect-key"]));
        assert.ok(pttl > 0 && pttl <= 30_000, `deadline must survive reconnect, got ${pttl}`);
    } finally {
        await second.close();
    }
});

test("MULTI queues expiry commands and EXEC replays them in order", async () => {
    const client = await RawClient.connect(node.redis.host, node.redis.port);
    try {
        const atSeconds = Math.floor(Date.now() / 1000) + 1_000;
        expectSimple(await client.command("MULTI"), "OK");
        expectSimple(await client.command("SET", "tx-exp", "v", "EX", "100"), "QUEUED");
        expectSimple(await client.command("GETEX", "tx-exp", "EX", "500"), "QUEUED");
        expectSimple(await client.command("EXPIREAT", "tx-exp", String(atSeconds)), "QUEUED");
        expectArray(await client.command("EXEC"), [
            { kind: "simple", value: "OK" },
            { kind: "bulk", value: "v" },
            { kind: "integer", value: 1 },
        ]);
        const ttl = await client.command("TTL", "tx-exp");
        assert.ok(ttl.kind === "integer" && ttl.value > 990 && ttl.value <= 1_000, `EXEC replayed deadline, got ${JSON.stringify(ttl)}`);

        // Queue-time rejections poison the transaction: `EXEC` answers `EXECABORT` and the
        // whole queue is dropped without a replay, like upstream Redis.
        expectSimple(await client.command("MULTI"), "OK");
        expectError(await client.command("EXPIREAT", "tx-exp", "abc"), "ERR value is not an integer or out of range");
        expectError(await client.command("GETEX", "tx-exp", "BADOPT"), "ERR syntax error");
        expectSimple(await client.command("SET", "tx-clean", "v"), "QUEUED");
        expectError(await client.command("EXEC"), "EXECABORT Transaction discarded because of previous errors.");
        expectBulk(await client.command("GET", "tx-clean"), null);

        // A runtime failure lands inside the EXEC result array and does not abort the rest.
        expectSimple(await client.command("SET", "tx-text", "abc"), "OK");
        expectSimple(await client.command("MULTI"), "OK");
        expectSimple(await client.command("INCR", "tx-text"), "QUEUED");
        expectSimple(await client.command("SET", "tx-after-error", "v"), "QUEUED");
        const results = await client.command("EXEC");
        assert.ok(results.kind === "array" && results.value.length === 2);
        assert.equal(results.value[0].kind, "error");
        assert.deepEqual(results.value[1], { kind: "simple", value: "OK" });
        expectBulk(await client.command("GET", "tx-after-error"), "v");
    } finally {
        client.destroy();
    }
});

test("expiry argument validation rejects bad integers and arities", async () => {
    const client = await RawClient.connect(node.redis.host, node.redis.port);
    try {
        expectError(await client.command("EXPIREAT", "k"), "ERR wrong number of arguments for 'expireat' command");
        expectError(await client.command("PEXPIREAT", "k", "05"), "ERR value is not an integer or out of range");
        expectError(await client.command("EXPIREAT", "k", "abc"), "ERR value is not an integer or out of range");
        // A timestamp that parses but overflows millisecond conversion is rejected.
        expectSimple(await client.command("SET", "exp-overflow", "v"), "OK");
        expectError(
            await client.command("EXPIREAT", "exp-overflow", "9223372036854776"),
            "ERR invalid expire time in 'expireat' command",
        );
        expectError(await client.command("SETEX", "k", "0", "v"), "ERR invalid expire time in 'setex' command");
        expectError(await client.command("SETEX", "k", "-3", "v"), "ERR invalid expire time in 'setex' command");
        expectError(await client.command("TTL", "k", "extra"), "ERR wrong number of arguments for 'ttl' command");
        expectInteger(await client.command("PERSIST", "persist-missing"), 0);
    } finally {
        client.destroy();
    }
});
