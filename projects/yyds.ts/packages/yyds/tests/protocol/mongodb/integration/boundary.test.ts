import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode } from "@yydb/yyds-node-harness";
import { MongoClient } from "mongodb";

interface ServerCodeFailure extends Error {
    code?: number;
}

test("mongodb boundary surface covers handshake docs, unique _id, cursors and unsupported verbs", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode({ mongo: true });
    assert.ok(node.mongo);
    const client = new MongoClient(`mongodb://${node.mongo.host}:${node.mongo.port}`, {
        directConnection: true,
        serverApi: { version: "1", strict: false, deprecationErrors: false },
        serverSelectionTimeoutMS: 10_000,
        retryWrites: false,
        retryReads: false,
    });
    await client.connect();
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    const db = client.db("yyds");

    // Both handshake verbs report the same static topology document.
    const hello = await db.command({ hello: 1 });
    assert.equal(hello.ok, 1);
    assert.equal(hello.isWritablePrimary, true);
    assert.equal(hello.maxWireVersion, 17);
    assert.equal(hello.minWireVersion, 0);
    const isMaster = await db.command({ isMaster: 1 });
    assert.equal(isMaster.ok, 1);
    assert.equal(isMaster.isWritablePrimary, true);

    const collection = db.collection<{ _id?: string; v?: number; a?: number }>("boundary");

    // An explicit `_id` is unique: the second write is E11000 and the first document
    // stays unchanged, matching the real server instead of an implicit overwrite.
    const first = await collection.insertOne({ _id: "fixed", v: 1 });
    assert.equal(first.insertedId, "fixed");
    assert.equal((await collection.find({ _id: "fixed" }).toArray())[0]?.v, 1);
    await assert.rejects(collection.insertOne({ _id: "fixed", v: 2 }), (error: unknown) => {
        assert.equal((error as ServerCodeFailure).code, 11000);
        return true;
    });
    assert.equal((await collection.find({ _id: "fixed" }).toArray())[0]?.v, 1);

    // The driver's `insertMany` encodes documents as an OP_MSG document-sequence
    // section. The gateway folds that section back into `documents`, so the driver
    // path and the inline raw command path must store the same payload.
    const inserted = await collection.insertMany([{ a: 9 }]);
    assert.equal(inserted.insertedCount, 1);
    const many = await db.command({ insert: "boundary", documents: [{ a: 1 }, { a: 2 }, { a: 3 }] });
    assert.equal(many.ok, 1);
    assert.equal(many.n, 3);

    // Multi-document reads and limits run over the scan path.
    assert.equal((await collection.find({}).toArray()).length, 5);
    assert.equal((await collection.find({}).limit(2).toArray()).length, 2);

    // The raw find reply carries the namespaced cursor even for empty batches.
    const raw = await db.command({ find: "boundary", filter: { _id: "no-such-id" }, limit: 1 });
    assert.equal(raw.cursor.ns, "yyds.boundary");
    assert.equal((raw.cursor.firstBatch ?? []).length, 0);

    // Delete boundaries: missing id deletes nothing, limit 1 drops one, limit 0 drops all.
    assert.equal((await collection.deleteOne({ _id: "missing" })).deletedCount, 0);
    assert.equal((await collection.deleteOne({})).deletedCount, 1);
    assert.equal((await collection.deleteMany({})).deletedCount, 4);
    assert.deepEqual(await collection.find({}).toArray(), []);

    // Namespaces are `db.collection`, so the same id in another database is absent.
    await db.collection<{ _id: string; v: string }>("iso").insertOne({ _id: "iso", v: "here" });
    assert.deepEqual(await client.db("other").collection<{ _id: string }>("iso").find({ _id: "iso" }).toArray(), []);

    // `count`, `aggregate` and `drop` are wired into the command surface; verbs outside
    // it still report the explicit unsupported code 59 rather than an empty result.
    await collection.insertMany([{ a: 4 }, { a: 5 }]);
    const counted = await db.command({ count: "boundary" });
    assert.equal(counted.ok, 1);
    assert.equal(counted.n, 2);
    const aggregate = await db.command({ aggregate: "boundary", pipeline: [], cursor: {} });
    assert.equal(aggregate.ok, 1);
    assert.equal(aggregate.cursor.ns, "yyds.boundary");
    assert.equal(aggregate.cursor.firstBatch.length, 2);
    const dropped = await db.command({ drop: "boundary" });
    assert.equal(dropped.ok, 1);
    await assert.rejects(db.command({ yydsNoSuchVerb: 1 }), (error: unknown) => {
        assert.equal((error as ServerCodeFailure).code, 59, "expected code 59 for an unknown verb");
        return true;
    });
});

test("mongodb answers an empty collection name with 73 and a duplicate _id with 11000", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode({ mongo: true });
    assert.ok(node.mongo);
    const client = new MongoClient(`mongodb://${node.mongo.host}:${node.mongo.port}`, {
        directConnection: true,
        serverApi: { version: "1", strict: false, deprecationErrors: false },
        serverSelectionTimeoutMS: 10_000,
        retryWrites: false,
        retryReads: false,
    });
    await client.connect();
    t.after(async () => {
        await client.close().catch(() => undefined);
        await node.stop();
    });

    const db = client.db("yyds");

    // A missing or empty collection name is `InvalidNamespace` (73), never a
    // silently-created collection: the response must carry the numeric code so
    // drivers surface it as MongoServerError rather than an empty result.
    for (const command of [
        { insert: "", documents: [{ v: 1 }] },
        { find: "", filter: {} },
        { delete: "", deletes: [{ q: {}, limit: 0 }] },
    ]) {
        await assert.rejects(db.command(command), (error: unknown) => {
            assert.equal((error as ServerCodeFailure).code, 73, `expected code 73 for ${JSON.stringify(command)}`);
            return true;
        });
    }

    // MongoDB's `_id` is a unique key: a repeated value must surface E11000 and the
    // first document must survive. Silent overwrite would be a fabricated semantic.
    const collection = db.collection<{ _id: string; v: number }>("dup");
    await collection.insertOne({ _id: "fixed", v: 1 });
    await assert.rejects(collection.insertOne({ _id: "fixed", v: 2 }), (error: unknown) => {
        assert.equal((error as ServerCodeFailure).code, 11000, "a duplicate _id must surface code 11000");
        return true;
    });
    assert.equal((await collection.find({ _id: "fixed" }).toArray())[0]?.v, 1, "the rejected insert must not overwrite");
});
