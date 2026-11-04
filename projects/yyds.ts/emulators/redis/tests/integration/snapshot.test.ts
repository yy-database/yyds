import assert from "node:assert/strict";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { loadYydsNative } from "@yydb/yyds";
import { startYydsNode } from "@yydb/yyds-node-harness";
import { createClient } from "redis";

const SHARD_FILE = "local-0.yykv";

function driver(host: string, port: number) {
    const client = createClient({ url: `redis://${host}:${port}`, RESP: 2, disableClientInfo: true });
    client.on("error", () => undefined);
    return client;
}

/** Seals one live v3 snapshot containing one key and returns the raw shard bytes. */
async function seedSnapshot(dataDir: string): Promise<Buffer> {
    const node = await startYydsNode({
        dataDir,
        clusterId: "cluster-yykv",
        nodeId: "node-yykv",
        expirySweepIntervalMs: 3_600_000,
    });
    const client = driver(node.redis.host, node.redis.port);
    await client.connect();
    assert.equal(await client.set("sealed", "value"), "OK");
    await client.close();
    await node.stop({ wipe: false });
    return readFile(join(dataDir, SHARD_FILE));
}

async function withSealedShard(body: (dataDir: string, bytes: Buffer) => Promise<void>): Promise<void> {
    const dataDir = await mkdtemp(join(tmpdir(), "yyds-yykv-"));
    try {
        await body(dataDir, await seedSnapshot(dataDir));
    } finally {
        await rm(dataDir, { recursive: true, force: true });
    }
}

test("a flipped snapshot byte is refused as corrupt instead of loading", { timeout: 120_000 }, async () => {
    await withSealedShard(async (dataDir, original) => {
        const damaged = Buffer.from(original);
        damaged[damaged.length - 1] ^= 0xff; // last body byte, checksum left untouched
        await writeFile(join(dataDir, SHARD_FILE), damaged);
        assert.throws(
            () => new (loadYydsNative().LocalExecutorHandle)(dataDir, null),
            /yykv checksum mismatch/,
            "a corrupted body must be reported, never opened as a valid snapshot",
        );
    });
});

test("a truncated snapshot is refused instead of loading an empty shard", { timeout: 120_000 }, async () => {
    await withSealedShard(async (dataDir, original) => {
        // Keep the magic and only three checksum bytes: the checksum itself cannot be read,
        // so a truncated file must fail loudly rather than be treated as an empty shard.
        await writeFile(join(dataDir, SHARD_FILE), original.subarray(0, 8));
        assert.throws(
            () => new (loadYydsNative().LocalExecutorHandle)(dataDir, null),
            /unexpected shard eof/,
            "a truncated shard must be reported, never opened as an empty snapshot",
        );
    });
});

test("a v2 snapshot is refused rather than read without a checksum", { timeout: 120_000 }, async () => {
    await withSealedShard(async (dataDir, original) => {
        const legacy = Buffer.from(original);
        legacy[4] = 0x02; // MAGIC carries the format version byte in position 4
        await writeFile(join(dataDir, SHARD_FILE), legacy);
        assert.throws(
            () => new (loadYydsNative().LocalExecutorHandle)(dataDir, null),
            /legacy yykv v2 snapshot/,
            "v2 has no checksum, so it must be rejected outright",
        );
    });
});
