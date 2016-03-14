import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode } from "@yydb/yyds-node-harness";
import { createClient } from "redis";

test("node-redis drives string commands, batches and namespace isolation", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    // The gateway speaks RESP2 only and does not implement HELLO or CLIENT
    // SETINFO, so the driver must not send those handshake commands.
    const client = createClient({
        url: `redis://${node.redis.host}:${node.redis.port}`,
        RESP: 2,
        disableClientInfo: true,
    });
    client.on("error", () => undefined);
    await client.connect();
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

    // MULTI/EXEC, AUTH and expiry are not implemented. They must fail loudly.
    await assert.rejects(client.multi().set("tx", "1").exec(), /unsupported/i);
    await assert.rejects(client.sendCommand(["AUTH", "secret"]), /unsupported/i);
    await assert.rejects(client.sendCommand(["MULTI"]), /unsupported/i);
    assert.equal(await client.ttl("scoped"), -1);
    assert.equal(await client.ttl("missing"), -2);
});
