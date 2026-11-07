import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode } from "@yydb/yyds-node-harness";
import { MongoClient } from "mongodb";

type ProbeNode = Awaited<ReturnType<typeof startYydsNode>>;

async function connectMongo(node: ProbeNode): Promise<MongoClient> {
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
    return client;
}

test("mongodb driver completes the handshake and runs document CRUD", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode({ mongo: true });
    const client = await connectMongo(node);
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

test("mongodb driver drains a batched cursor and killCursors reclaims it", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode({ mongo: true });
    const client = await connectMongo(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    const db = client.db("yyds");
    const collection = db.collection<{ _id: string; v: number }>("cursor-probe");
    const ids = [0, 1, 2, 3, 4].map((index) => `cursor-${index}`);
    await collection.deleteMany({});
    await collection.insertMany(ids.map((id, index) => ({ _id: id, v: index })));

    // A batch smaller than the result set forces the driver through `getMore`
    // frames until the server reports cursor id 0.
    const observed: string[] = [];
    for await (const document of collection.find({}, { batchSize: 2 })) {
        observed.push(document._id);
    }
    assert.deepEqual(observed, ids);

    // Send the cursor commands directly so the id is observable: a killed cursor
    // must leave the server, and a later `getMore` answers cursor id not found.
    const first = await db.command({ find: "cursor-probe", batchSize: 2 });
    const cursorId = first.cursor.id as number;
    assert.notEqual(cursorId, 0, "a partial find must return a live cursor id");
    assert.equal(first.cursor.firstBatch.length, 2);

    const killed = await db.command({ killCursors: "cursor-probe", cursors: [cursorId] });
    assert.equal(killed.ok, 1);
    assert.deepEqual(killed.cursorsKilled, [cursorId]);
    await assert.rejects(
        db.command({ getMore: cursorId, collection: "cursor-probe" }),
        (error: unknown) => (error as { code?: number }).code === 43,
    );

    // An id the server never issued is reported as not found instead of faked.
    const absent = 999_999_999;
    const missing = await db.command({ killCursors: "cursor-probe", cursors: [absent] });
    assert.equal(missing.ok, 1);
    assert.deepEqual(missing.cursorsNotFound, [absent]);

    await collection.deleteMany({});
});

test("aggregate $group over empty input returns no documents", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode({ mongo: true });
    const client = await connectMongo(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    const collection = client.db("yyds").collection<{ _id: string; v: number }>("group-probe");
    await collection.deleteMany({});
    const pipeline = [{ $group: { _id: null, n: { $sum: 1 } } }];
    assert.deepEqual(await collection.aggregate(pipeline).toArray(), []);

    // A non-empty control proves the empty reply comes from the input, not a stub.
    await collection.insertMany([{ _id: "g1", v: 1 }, { _id: "g2", v: 2 }]);
    assert.deepEqual(await collection.aggregate(pipeline).toArray(), [{ _id: null, n: 2 }]);

    await collection.deleteMany({});
});

test("aggregate $project exclusion drops only the excluded fields", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode({ mongo: true });
    const client = await connectMongo(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    const collection = client
        .db("yyds")
        .collection<{ _id: string; keep: string; secret: string }>("projection-probe");
    await collection.deleteMany({});
    await collection.insertMany([
        { _id: "p1", keep: "visible", secret: "hidden" },
        { _id: "p2", keep: "other", secret: "hidden" },
    ]);

    // An exclusion spec drops only the named field and keeps `_id` by default.
    assert.deepEqual(await collection.aggregate([{ $project: { secret: 0 } }]).toArray(), [
        { _id: "p1", keep: "visible" },
        { _id: "p2", keep: "other" },
    ]);

    // `_id: 0` is a second exclusion rule and removes the identifier too.
    assert.deepEqual(await collection.aggregate([{ $project: { _id: 0, secret: 0 } }]).toArray(), [
        { keep: "visible" },
        { keep: "other" },
    ]);

    await collection.deleteMany({});
});

test("aggregate stages run in pipeline order", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode({ mongo: true });
    const client = await connectMongo(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    const collection = client
        .db("yyds")
        .collection<{ _id: string; k: string; v: number }>("stage-probe");
    await collection.deleteMany({});
    await collection.insertMany([
        { _id: "s1", k: "a", v: 1 },
        { _id: "s2", k: "a", v: 2 },
        { _id: "s3", k: "b", v: 3 },
    ]);

    assert.deepEqual(
        await collection
            .aggregate([{ $match: { k: "a" } }, { $group: { _id: null, n: { $sum: 1 } } }])
            .toArray(),
        [{ _id: null, n: 2 }],
    );

    // Running the stages in the opposite order folds all three first, so the
    // following `$match` on `n: 2` finds nothing.
    assert.deepEqual(
        await collection
            .aggregate([{ $group: { _id: null, n: { $sum: 1 } } }, { $match: { n: 2 } }])
            .toArray(),
        [],
    );

    await collection.deleteMany({});
});

test("aggregate $project inclusion keeps only the listed fields", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode({ mongo: true });
    const client = await connectMongo(node);
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    const collection = client
        .db("yyds")
        .collection<{ _id: string; k: string; v: number }>("inclusion-probe");
    await collection.deleteMany({});
    await collection.insertMany([
        { _id: "i1", k: "a", v: 1 },
        { _id: "i2", k: "a", v: 2 },
        { _id: "i3", k: "b", v: 3 },
    ]);

    assert.deepEqual(await collection.aggregate([{ $project: { v: 1 } }]).toArray(), [
        { _id: "i1", v: 1 },
        { _id: "i2", v: 2 },
        { _id: "i3", v: 3 },
    ]);

    assert.deepEqual(await collection.aggregate([{ $project: { _id: 0, v: 1 } }]).toArray(), [
        { v: 1 },
        { v: 2 },
        { v: 3 },
    ]);

    await collection.deleteMany({});
});
