import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode } from "@yydb/yyds-node-harness";
import { MongoClient } from "mongodb";

test("mongodb driver completes the handshake and runs document CRUD", { timeout: 60_000 }, async (t) => {
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

    // `insert`, `find` and `delete` are answered from the YYDS key-value
    // executor, so a document survives the round trip through the wire.
    const collection = client.db("yyds").collection("probe");
    const insert = await collection.insertOne({ value: 1 });
    assert.equal(insert.acknowledged, true);

    const found = await collection.find({}).toArray();
    assert.equal(found.length, 1);
    assert.equal(found[0]?.value, 1);

    const deleted = await collection.deleteMany({});
    assert.equal(deleted.deletedCount, 1);
    assert.deepEqual(await collection.find({}).toArray(), []);

    // Anything outside the CRUD surface stays an explicit unsupported error
    // rather than an empty or fabricated result.
    await assert.rejects(collection.updateOne({}, { $set: { value: 2 } }), /unsupported/i);
});
