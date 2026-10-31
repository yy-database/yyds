import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode } from "@yydb/yyds-node-harness";
import { MongoClient } from "mongodb";

interface ServerCodeFailure extends Error {
    code?: number;
}

test("mongodb boundary surface covers handshake docs, id overwrite, cursors and unsupported verbs", { timeout: 60_000 }, async (t) => {
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

    const collection = db.collection("boundary");

    // An explicit _id write twice silently overwrites the stored document.
    const first = await collection.insertOne({ _id: "fixed", v: 1 });
    assert.equal(first.insertedId, "fixed");
    assert.equal((await collection.find({ _id: "fixed" }).toArray())[0]?.v, 1);
    await collection.insertOne({ _id: "fixed", v: 2 });
    assert.equal((await collection.find({ _id: "fixed" }).toArray())[0]?.v, 2);

    // The driver's `insertMany` encodes documents as an OP_MSG sequence section,
    // which the gateway does not parse; the inline `documents` array does work.
    await assert.rejects(collection.insertMany([{ a: 9 }]), /'documents' array/i);
    const many = await db.command({ insert: "boundary", documents: [{ a: 1 }, { a: 2 }, { a: 3 }] });
    assert.equal(many.ok, 1);
    assert.equal(many.n, 3);

    // Multi-document reads and limits run over the scan path.
    assert.equal((await collection.find({}).toArray()).length, 4);
    assert.equal((await collection.find({}).limit(2).toArray()).length, 2);

    // The raw find reply carries the namespaced cursor even for empty batches.
    const raw = await db.command({ find: "boundary", filter: { _id: "no-such-id" }, limit: 1 });
    assert.equal(raw.cursor.ns, "yyds.boundary");
    assert.equal((raw.cursor.firstBatch ?? []).length, 0);

    // Delete boundaries: missing id deletes nothing, limit 1 drops one, limit 0 drops all.
    assert.equal((await collection.deleteOne({ _id: "missing" })).deletedCount, 0);
    assert.equal((await collection.deleteOne({})).deletedCount, 1);
    assert.equal((await collection.deleteMany({})).deletedCount, 3);
    assert.deepEqual(await collection.find({}).toArray(), []);

    // Namespaces are `db.collection`, so the same id in another database is absent.
    await db.collection("iso").insertOne({ _id: "iso", v: "here" });
    assert.deepEqual(await client.db("other").collection("iso").find({ _id: "iso" }).toArray(), []);

    // Unimplemented verbs fail with the gateway's unsupported code 59.
    for (const command of [
        { count: "boundary" },
        { aggregate: "boundary", pipeline: [], cursor: {} },
        { drop: "boundary" },
    ]) {
        await assert.rejects(db.command(command), (error: unknown) => {
            assert.equal((error as ServerCodeFailure).code, 59, `expected code 59 for ${JSON.stringify(command)}`);
            return true;
        });
    }
});
