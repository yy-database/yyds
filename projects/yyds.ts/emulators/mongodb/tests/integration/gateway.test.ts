import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode } from "@yydb/yyds-node-harness";
import { MongoClient } from "mongodb";

test("mongodb driver completes the handshake and sees unsupported collection commands", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode({ mongo: true });
    assert.ok(node.mongo, "the harness must expose the MongoDB probe address");
    const client = new MongoClient(`mongodb://${node.mongo.host}:${node.mongo.port}`, {
        directConnection: true,
        // The probe implements OP_MSG only. Without the stable-API marker the
        // driver opens with the legacy OP_QUERY handshake, which it rejects.
        serverApi: { version: "1", strict: false, deprecationErrors: false },
        serverSelectionTimeoutMS: 10_000,
        // The probe does not implement retryable writes or sessions.
        retryWrites: false,
        retryReads: false,
    });
    await client.connect();
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    const ping = await client.db("admin").command({ ping: 1 });
    assert.equal(ping.ok, 1);

    // The wire probe answers `hello`/`ping` only. Every collection command is an
    // explicit unsupported error rather than an empty or fabricated result.
    const collection = client.db("yyds").collection("probe");
    await assert.rejects(collection.find({}).toArray(), /unsupported/i);
    await assert.rejects(collection.insertOne({ value: 1 }), /unsupported/i);
    await assert.rejects(collection.deleteMany({}), /unsupported/i);
});
