import assert from "node:assert/strict";
import { stat } from "node:fs/promises";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { startYydsNode } from "@yydb/yyds-node-harness";
import { createClient } from "redis";

const SHARD_FILE = "local-0.yykv";

function driver(host: string, port: number) {
    const client = createClient({ url: `redis://${host}:${port}`, RESP: 2, disableClientInfo: true });
    client.on("error", () => undefined);
    return client;
}

async function shardSize(dataDir: string): Promise<number> {
    return (await stat(join(dataDir, SHARD_FILE))).size;
}

test("concurrent short-lived writes and reads stay consistent under reclamation", { timeout: 180_000 }, async () => {
    const node = await startYydsNode({ expirySweepIntervalMs: 50 });
    const connections = 8;
    const perConnection = 250;
    const clients: ReturnType<typeof driver>[] = [];
    try {
        for (let index = 0; index < connections; index += 1) {
            const client = driver(node.redis.host, node.redis.port);
            await client.connect();
            clients.push(client);
        }

        // Every connection writes its own short-lived batch while all connections keep
        // reading; reclamation lands in the middle of the traffic.
        await Promise.all(
            clients.map(async (client, index) => {
                const writes: Promise<unknown>[] = [];
                for (let n = 0; n < perConnection; n += 1) {
                    writes.push(client.sendCommand(["SET", `stress-${index}-${n}`, "payload", "PX", "2500"]));
                }
                const acked = await Promise.all(writes);
                assert.ok(acked.every((reply) => reply === "OK"));
                for (let n = 0; n < 30; n += 1) {
                    const value = await client.get(`stress-${(index + 1) % connections}-${n}`);
                    assert.ok(value === null || value === "payload", `reads only observe real states, got ${value}`);
                }
            }),
        );

        const peak = await shardSize(node.dataDir);

        // After the deadline plus generous sweep margin, every bulk key reads as absent and
        // the snapshot shrinks, while traffic keeps flowing.
        await sleep(2_500 + 2_000);
        const sample = await clients[0].sendCommand(["MGET", ...Array.from({ length: 8 }, (_, i) => `stress-${i}-0`)]);
        assert.ok(Array.isArray(sample) && sample.every((value) => value === null), "expired bulk stays absent");

        const deadline = Date.now() + 15_000;
        let current = peak;
        while (Date.now() < deadline) {
            current = await shardSize(node.dataDir);
            if (current * 2 < peak) break;
            await sleep(100);
        }
        assert.ok(current * 2 < peak, `reclamation shrinks the shard under load (peak=${peak} current=${current})`);

        await clients[0].set("stress-survivor", "keep");
        assert.equal(await clients[connections - 1].get("stress-survivor"), "keep");
    } finally {
        await Promise.all(clients.map((client) => client.close().catch(() => undefined)));
        await node.stop();
    }
});

test("deadlines survive repeated node restarts", { timeout: 180_000 }, async () => {
    const dataDir = await mkdtemp(join(tmpdir(), "yyds-restart-"));
    const identity = { dataDir, clusterId: "cluster-restart", nodeId: "node-restart" };
    try {
        const first = await startYydsNode(identity);
        const writer = driver(first.redis.host, first.redis.port);
        await writer.connect();
        await writer.set("restart-keep", "v");
        await writer.sendCommand(["SET", "restart-long", "v", "PX", "45000"]);
        // This key's deadline elapses while the node is down.
        await writer.sendCommand(["SET", "restart-gone", "v", "PX", "600"]);
        await writer.close();
        await sleep(900);
        await first.stop({ wipe: false });

        let previousPttl = 45_000;
        for (let cycle = 0; cycle < 2; cycle += 1) {
            const node = await startYydsNode(identity);
            const client = driver(node.redis.host, node.redis.port);
            await client.connect();
            try {
                assert.equal(await client.get("restart-keep"), "v", "non-expiring data survives restarts");
                assert.equal(await client.get("restart-long"), "v", "a live deadline survives restarts");
                const pttl = Number(await client.sendCommand(["PTTL", "restart-long"]));
                assert.ok(
                    pttl > 0 && pttl <= previousPttl,
                    `the deadline keeps counting across restarts (previous=${previousPttl} now=${pttl})`,
                );
                previousPttl = pttl;
                assert.equal(await client.get("restart-gone"), null, "a deadline elapsed while down stays absent");
                assert.equal(await client.sendCommand(["EXISTS", "restart-gone"]), 0);
            } finally {
                await client.close();
                await node.stop({ wipe: false });
            }
        }
    } finally {
        await rm(dataDir, { recursive: true, force: true });
    }
});
