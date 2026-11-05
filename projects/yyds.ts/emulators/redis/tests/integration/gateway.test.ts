import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode } from "@yydb/yyds-node-harness";
import { createClient } from "redis";

type ProbeNode = Awaited<ReturnType<typeof startYydsNode>>;

async function connectRedis(node: ProbeNode) {
    // The gateway speaks RESP2 only and does not implement HELLO or CLIENT
    // SETINFO, so the driver must not send those handshake commands.
    const client = createClient({
        url: `redis://${node.redis.host}:${node.redis.port}`,
        RESP: 2,
        disableClientInfo: true,
    });
    client.on("error", () => undefined);
    await client.connect();
    return client;
}

test("node-redis drives string commands, batches and namespace isolation", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connectRedis(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    assert.equal(await client.ping(), "PONG");
    assert.equal(await client.set("greeting", "hello"), "OK");
    assert.equal(await client.get("greeting"), "hello");

    const binary = Buffer.from([0, 255, 65, 0]);
    assert.equal(await client.set("binary", binary), "OK");
    assert.equal(await client.strLen("binary"), binary.length);

    await client.set("one", "1");
    await client.set("two", "2");
    assert.deepEqual(await client.mGet(["one", "two", "one"]), ["1", "2", "1"]);

    assert.equal(await client.del("one"), 1);
    assert.equal(await client.del("one"), 0);
    assert.equal(await client.incr("counter"), 1);
    assert.equal(await client.incr("counter"), 2);

    // node-redis queues same-tick commands into one pipeline. A rejected command
    // must not desynchronize the commands queued behind it.
    const batched = await Promise.allSettled([
        client.set("batch-a", "a"),
        client.sendCommand(["AUTH", "secret"]),
        client.set("batch-b", "b"),
        client.get("batch-a"),
    ]);
    assert.deepEqual(
        batched.map((entry) => entry.status),
        ["fulfilled", "rejected", "fulfilled", "fulfilled"],
    );
    const auth = batched[1] as PromiseRejectedResult;
    assert.match(String(auth.reason), /unsupported/i);
    assert.equal(await client.get("batch-b"), "b");

    // SELECT switches the executor namespace, so keys do not leak across shards.
    await client.set("scoped", "zero");
    await client.select(2);
    assert.equal(await client.get("scoped"), null);
    await client.set("scoped", "two");
    assert.equal(await client.get("scoped"), "two");
    await client.select(0);
    assert.equal(await client.get("scoped"), "zero");

    // MULTI/EXEC is executor-backed: queued writes commit together on EXEC.
    const tx = await client.multi().set("tx", "1").incr("tx-counter").exec();
    assert.deepEqual(tx, ["OK", 1]);
    assert.equal(await client.get("tx"), "1");

    // AUTH is not implemented and must fail loudly.
    await assert.rejects(client.sendCommand(["AUTH", "secret"]), /unsupported/i);
    assert.equal(await client.ttl("scoped"), -1);
    assert.equal(await client.ttl("missing"), -2);
});

test("node-redis refuses a nested MULTI without dropping the open queue", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connectRedis(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    assert.equal(await client.sendCommand(["MULTI"]), "OK");
    assert.equal(await client.sendCommand(["SET", "nested-tx", "v"]), "QUEUED");
    await assert.rejects(client.sendCommand(["MULTI"]), /MULTI calls can not be nested/);

    // The refused nesting leaves the transaction open, so the queued write still commits.
    assert.deepEqual(await client.sendCommand(["EXEC"]), ["OK"]);
    assert.equal(await client.get("nested-tx"), "v");
});

test("node-redis keeps a queued MULTI write invisible to another connection", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const writer = await connectRedis(node);
    const reader = await connectRedis(node);
    t.after(async () => {
        await reader.close().catch(() => undefined);
        await writer.close().catch(() => undefined);
        await node.stop();
    });

    await writer.set("tx-isolation", "old");
    assert.equal(await writer.sendCommand(["MULTI"]), "OK");
    assert.equal(await writer.sendCommand(["SET", "tx-isolation", "new"]), "QUEUED");
    assert.equal(await reader.get("tx-isolation"), "old");

    assert.deepEqual(await writer.sendCommand(["EXEC"]), ["OK"]);
    assert.equal(await reader.get("tx-isolation"), "new");
});

test("node-redis DISCARD drops the queued write", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connectRedis(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    await assert.rejects(client.sendCommand(["DISCARD"]), /DISCARD without MULTI/);

    await client.set("discard-tx", "before");
    assert.equal(await client.sendCommand(["MULTI"]), "OK");
    assert.equal(await client.sendCommand(["SET", "discard-tx", "after"]), "QUEUED");
    assert.equal(await client.sendCommand(["DISCARD"]), "OK");
    assert.equal(await client.get("discard-tx"), "before");
});

test("node-redis returns an empty array for EXEC on an empty queue", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connectRedis(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    assert.equal(await client.sendCommand(["MULTI"]), "OK");
    assert.deepEqual(await client.sendCommand(["EXEC"]), []);

    // The connection leaves the transaction cleanly and accepts normal writes.
    assert.equal(await client.set("empty-tx", "v"), "OK");
    assert.equal(await client.get("empty-tx"), "v");
});
