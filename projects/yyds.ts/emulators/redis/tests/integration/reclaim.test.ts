import assert from "node:assert/strict";
import { stat } from "node:fs/promises";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { loadYydsNative } from "@yydb/yyds";
import { startYydsNode } from "@yydb/yyds-node-harness";
import { createClient } from "redis";

const SHARD_FILE = "local-0.yykv";

function driver(host: string, port: number) {
    const client = createClient({ url: `redis://${host}:${port}`, RESP: 2, disableClientInfo: true });
    client.on("error", () => undefined);
    return client;
}

async function writeKeys(client: ReturnType<typeof driver>, prefix: string, count: number, px: number): Promise<void> {
    const value = "v".repeat(32);
    const BATCH = 400;
    for (let base = 0; base < count; base += BATCH) {
        const writes: Promise<unknown>[] = [];
        for (let index = base; index < Math.min(base + BATCH, count); index += 1) {
            writes.push(client.sendCommand(["SET", `${prefix}-${index}`, value, "PX", String(px)]));
        }
        const results = await Promise.all(writes);
        assert.ok(results.every((reply) => reply === "OK"), "every short-TTL write must be acked");
    }
}

async function shardSize(dataDir: string): Promise<number> {
    return (await stat(join(dataDir, SHARD_FILE))).size;
}

test("one-shot sweepExpired reclaims elapsed records after the node stops", { timeout: 120_000 }, async () => {
    const dataDir = await mkdtemp(join(tmpdir(), "yyds-sweep-oneshot-"));
    const total = 600;
    try {
        // Park the background sweeper on a huge interval so the one-shot pass below is the
        // only reclamation acting on this directory.
        const node = await startYydsNode({
            dataDir,
            clusterId: "cluster-sweep",
            nodeId: "node-sweep",
            expirySweepIntervalMs: 3_600_000,
        });
        const client = driver(node.redis.host, node.redis.port);
        await client.connect();
        await client.set("survivor", "keep");
        await writeKeys(client, "oneshot", total, 80);
        await sleep(200); // every deadline elapses, and no read touches the keys again
        await client.close();
        await node.stop({ wipe: false });

        const before = await shardSize(dataDir);
        const native = loadYydsNative();
        const executor = new native.LocalExecutorHandle(dataDir, null);
        const reclaimed = executor.sweepExpired();
        assert.equal(reclaimed, total, "every elapsed record is reclaimed, the survivor stays");
        assert.equal(executor.sweepExpired(), 0, "a second pass finds nothing left");
        const after = await shardSize(dataDir);
        assert.ok(after < before, `snapshot must shrink after reclaim (before=${before} after=${after})`);
    } finally {
        await rm(dataDir, { recursive: true, force: true });
    }
});

test("the first background pass after a restart reclaims deadlines elapsed while down", { timeout: 120_000 }, async () => {
    const dataDir = await mkdtemp(join(tmpdir(), "yyds-sweep-restart-"));
    const identity = { clusterId: "cluster-sweep-restart", nodeId: "node-sweep-restart" };
    try {
        // Park the sweeper on a huge interval so nothing is reclaimed before the shutdown.
        const first = await startYydsNode({ dataDir, ...identity, expirySweepIntervalMs: 3_600_000 });
        const writer = driver(first.redis.host, first.redis.port);
        await writer.connect();
        await writer.set("restart-survivor", "keep");
        await writeKeys(writer, "restart", 800, 50);
        await sleep(150); // every short deadline elapses while the node is still up
        await writer.close();
        const before = await shardSize(dataDir);
        await first.stop({ wipe: false });

        // No reads after the restart: only the startup sweep can shrink the snapshot.
        const second = await startYydsNode({ dataDir, ...identity, expirySweepIntervalMs: 50 });
        const deadline = Date.now() + 15_000;
        let current = before;
        while (Date.now() < deadline) {
            await sleep(100);
            current = await shardSize(dataDir);
            if (current * 2 < before) break;
        }
        assert.ok(current * 2 < before, `the startup pass must reclaim elapsed records (before=${before} current=${current})`);

        const reader = driver(second.redis.host, second.redis.port);
        await reader.connect();
        try {
            assert.equal(await reader.get("restart-survivor"), "keep");
            assert.equal(await reader.get("restart-0"), null);
            assert.equal(await reader.ttl("restart-0"), -2);
        } finally {
            await reader.close();
            await second.stop();
        }
    } finally {
        await rm(dataDir, { recursive: true, force: true });
    }
});

test("a live rewrite without a deadline is never reclaimed", { timeout: 120_000 }, async () => {
    const node = await startYydsNode({ expirySweepIntervalMs: 5 });
    const writer = driver(node.redis.host, node.redis.port);
    const prober = driver(node.redis.host, node.redis.port);
    await writer.connect();
    await prober.connect();
    const keys = Array.from({ length: 64 }, (_, index) => `churn-${index}`);
    let probing = true;
    try {
        const probe = (async () => {
            while (probing) {
                assert.equal(await prober.ping(), "PONG");
                await sleep(10);
            }
        })();

        // Each round arms a 2ms deadline and then rewrites the key with no deadline while
        // the 5ms sweeper is running. A sweeper that deleted after the rewrite would lose
        // records that are no longer expired.
        for (let round = 0; round < 30; round += 1) {
            await Promise.all(keys.map((key) => writer.sendCommand(["SET", key, "short", "PX", "2"])));
            await sleep(3);
            await Promise.all(keys.map((key) => writer.sendCommand(["SET", key, `round-${round}`])));
            const present = Number(await writer.sendCommand(["EXISTS", ...keys]));
            assert.equal(present, keys.length, `round ${round}: every rewritten record must survive the sweeper`);
        }
        probing = false;
        await probe;

        // The rewrite cleared the deadline, so the shard keeps reporting a persistent record.
        const ttl = Number(await prober.sendCommand(["TTL", keys[0]]));
        assert.equal(ttl, -1, `a rewrite without KEEPTTL must clear the deadline, got ${ttl}`);
        assert.equal(await prober.get(keys[0]), "round-29");
    } finally {
        probing = false;
        await writer.close();
        await prober.close();
        await node.stop();
    }
});

test("background reclamation shrinks the shard without any read traffic", { timeout: 120_000 }, async () => {
    const node = await startYydsNode({ expirySweepIntervalMs: 50 });
    const client = driver(node.redis.host, node.redis.port);
    const prober = driver(node.redis.host, node.redis.port);
    await client.connect();
    await prober.connect();
    try {
        await client.set("bg-survivor", "keep");
        // The bulk deadline is long enough that every key survives the write phase and then
        // expires together, so the peak measurement captures the full unreclaimed bulk.
        const total = 2_000;
        await writeKeys(client, "bg", total, 4_000);
        const peak = await shardSize(node.dataDir);

        // While reclamation runs, a foreground probe keeps answering — reclaim never
        // surfaces on the request path.
        let probing = true;
        const probe = (async () => {
            while (probing) {
                assert.equal(await prober.ping(), "PONG");
                await sleep(25);
            }
        })();

        const deadline = Date.now() + 15_000;
        let current = peak;
        while (Date.now() < deadline) {
            await sleep(100);
            current = await shardSize(node.dataDir);
            if (current * 2 < peak) break; // reclaimed at least half of the expired bulk
        }
        probing = false;
        await probe;
        assert.ok(
            current * 2 < peak,
            `background sweep must reclaim without reads (peak=${peak} current=${current})`,
        );

        // Observable data is unchanged by physical reclaim: expired reads stay absent.
        assert.equal(await client.get("bg-survivor"), "keep");
        assert.equal(await client.get("bg-0"), null);
        assert.equal(await client.ttl("bg-0"), -2);
    } finally {
        await client.close();
        await prober.close();
        await node.stop();
    }
});
