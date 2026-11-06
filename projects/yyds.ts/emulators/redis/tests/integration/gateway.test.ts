import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode } from "@yydb/yyds-node-harness";
import { createClient } from "redis";

type ProbeNode = Awaited<ReturnType<typeof startYydsNode>>;

async function connectRedis(node: ProbeNode, protocol: 2 | 3 = 2) {
    // RESP2 is the default so most assertions keep the older wire shape. RESP3 is
    // opt-in for the optimistic-lock cases that must observe the dedicated null
    // type. CLIENT SETINFO is disabled to keep every handshake minimal.
    const client = createClient({
        url: `redis://${node.redis.host}:${node.redis.port}`,
        RESP: protocol,
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

async function assertWatchAbort(node: ProbeNode, protocol: 2 | 3) {
    const writer = await connectRedis(node, protocol);
    const rival = await connectRedis(node, protocol);
    const key = `watch-abort-resp${protocol}`;
    try {
        assert.equal(await writer.sendCommand(["WATCH", key]), "OK");
        assert.equal(await rival.set(key, "rival"), "OK");

        assert.equal(await writer.sendCommand(["MULTI"]), "OK");
        assert.equal(await writer.sendCommand(["SET", key, "transaction"]), "QUEUED");

        // A watched key changed by another connection turns EXEC into a protocol
        // null: `*-1` on RESP2 and `_` on RESP3. node-redis surfaces both as null.
        assert.equal(await writer.sendCommand(["EXEC"]), null);
        assert.equal(await rival.get(key), "rival");
        assert.equal(await writer.ping(), "PONG");
    } finally {
        await writer.close().catch(() => undefined);
        await rival.close().catch(() => undefined);
    }
}

test("node-redis aborts a watched transaction on RESP2", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    t.after(() => node.stop());

    await assertWatchAbort(node, 2);
});

test("node-redis aborts a watched transaction on RESP3", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    t.after(() => node.stop());

    await assertWatchAbort(node, 3);
});

test("node-redis releases a watch with UNWATCH and DISCARD", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const writer = await connectRedis(node);
    const rival = await connectRedis(node);
    t.after(async () => {
        await rival.close().catch(() => undefined);
        await writer.close().catch(() => undefined);
        await node.stop();
    });

    const key = "watch-release";

    await writer.set(key, "old");
    assert.equal(await writer.sendCommand(["WATCH", key]), "OK");
    assert.equal(await rival.set(key, "rival"), "OK");
    // UNWATCH drops the watch, so the following transaction commits even though
    // the key changed after it was watched.
    assert.equal(await writer.sendCommand(["UNWATCH"]), "OK");
    assert.equal(await writer.sendCommand(["MULTI"]), "OK");
    assert.equal(await writer.sendCommand(["SET", key, "after-unwatch"]), "QUEUED");
    assert.deepEqual(await writer.sendCommand(["EXEC"]), ["OK"]);
    assert.equal(await rival.get(key), "after-unwatch");

    assert.equal(await writer.sendCommand(["WATCH", key]), "OK");
    assert.equal(await rival.set(key, "rival-again"), "OK");
    // DISCARD closes the transaction and releases the watch with it.
    assert.equal(await writer.sendCommand(["MULTI"]), "OK");
    assert.equal(await writer.sendCommand(["SET", key, "discarded"]), "QUEUED");
    assert.equal(await writer.sendCommand(["DISCARD"]), "OK");
    assert.equal(await rival.get(key), "rival-again");

    assert.equal(await writer.sendCommand(["MULTI"]), "OK");
    assert.equal(await writer.sendCommand(["SET", key, "after-discard"]), "QUEUED");
    assert.deepEqual(await writer.sendCommand(["EXEC"]), ["OK"]);
    assert.equal(await rival.get(key), "after-discard");
});

test("node-redis refuses WATCH inside MULTI", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connectRedis(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    assert.equal(await client.sendCommand(["MULTI"]), "OK");
    await assert.rejects(
        client.sendCommand(["WATCH", "watch-inside-multi"]),
        /WATCH inside MULTI is not allowed/,
    );
    assert.equal(await client.sendCommand(["DISCARD"]), "OK");

    assert.equal(await client.sendCommand(["MULTI"]), "OK");
    assert.equal(await client.sendCommand(["SET", "watch-inside-multi", "v"]), "QUEUED");
    assert.deepEqual(await client.sendCommand(["EXEC"]), ["OK"]);
    assert.equal(await client.get("watch-inside-multi"), "v");
});

test("node-redis applies each GETEX expiry option to the stored deadline", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connectRedis(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    const key = "getex-options";
    assert.equal(await client.set(key, "v", { PX: 120_000 }), "OK");

    assert.equal(await client.sendCommand(["GETEX", key, "EX", "60"]), "v");
    let pttl = await client.pTTL(key);
    assert.ok(pttl > 55_000 && pttl <= 60_000, `EX deadline was ${pttl}`);

    assert.equal(await client.sendCommand(["GETEX", key, "PX", "30000"]), "v");
    pttl = await client.pTTL(key);
    assert.ok(pttl > 25_000 && pttl <= 30_000, `PX deadline was ${pttl}`);

    const atSeconds = Math.floor(Date.now() / 1000) + 120;
    assert.equal(await client.sendCommand(["GETEX", key, "EXAT", String(atSeconds)]), "v");
    pttl = await client.pTTL(key);
    assert.ok(pttl > 110_000 && pttl <= 120_000, `EXAT deadline was ${pttl}`);

    const atMillis = Date.now() + 90_000;
    assert.equal(await client.sendCommand(["GETEX", key, "PXAT", String(atMillis)]), "v");
    pttl = await client.pTTL(key);
    assert.ok(pttl > 80_000 && pttl <= 90_000, `PXAT deadline was ${pttl}`);

    assert.equal(await client.sendCommand(["GETEX", key, "PERSIST"]), "v");
    assert.equal(await client.pTTL(key), -1);
    assert.equal(await client.get(key), "v");
});

test("node-redis GETEX without options keeps the existing deadline", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connectRedis(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    const key = "getex-plain";
    assert.equal(await client.set(key, "v", { PX: 120_000 }), "OK");
    const before = await client.pTTL(key);
    assert.equal(await client.sendCommand(["GETEX", key]), "v");
    const after = await client.pTTL(key);
    assert.ok(after > 100_000 && after <= before, `a plain GETEX changed the deadline from ${before} to ${after}`);
    assert.ok(before - after < 5_000, `a plain GETEX reset the deadline from ${before} to ${after}`);
});

test("node-redis rejects conflicting GETEX options without touching the value", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connectRedis(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    const key = "getex-conflict";
    assert.equal(await client.set(key, "v", { PX: 120_000 }), "OK");
    const before = await client.pTTL(key);

    await assert.rejects(client.sendCommand(["GETEX", key, "EX", "60", "PX", "60000"]), /syntax error/);
    assert.equal(await client.get(key), "v");
    await assert.rejects(client.sendCommand(["GETEX", key, "PERSIST", "EXAT", "60"]), /syntax error/);
    assert.equal(await client.get(key), "v");

    const after = await client.pTTL(key);
    assert.ok(after > 100_000 && after <= before && before - after < 2_000, `a rejected GETEX changed the deadline from ${before} to ${after}`);
});

test("node-redis answers nil for a missing key and defers GETEX numeric validation", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connectRedis(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    // Option structure is checked before the lookup, but the numeric value is not
    // parsed until the key exists.
    assert.equal(await client.sendCommand(["GETEX", "getex-missing"]), null);
    assert.equal(await client.sendCommand(["GETEX", "getex-missing", "EX", "not-a-number"]), null);
    assert.equal(await client.sendCommand(["GETEX", "getex-missing", "EX", "0"]), null);
    assert.equal(await client.sendCommand(["GETEX", "getex-missing", "EXAT", "0"]), null);

    const key = "getex-invalid";
    assert.equal(await client.set(key, "v", { PX: 100_000 }), "OK");
    const before = await client.pTTL(key);

    await assert.rejects(client.sendCommand(["GETEX", key, "EX", "not-a-number"]), /value is not an integer or out of range/);
    await assert.rejects(client.sendCommand(["GETEX", key, "EX", "0"]), /invalid expire time in 'getex'/);
    await assert.rejects(client.sendCommand(["GETEX", key, "PX", "-5"]), /invalid expire time in 'getex'/);
    await assert.rejects(client.sendCommand(["GETEX", key, "EX"]), /syntax error/);

    assert.equal(await client.get(key), "v");
    const after = await client.pTTL(key);
    assert.ok(after > 90_000 && after <= before && before - after < 2_000, `a rejected GETEX changed the deadline from ${before} to ${after}`);
});
