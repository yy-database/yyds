import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { connect as netConnect } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { freePort, startYydsNode } from "@yydb/yyds-node-harness";
import { createClient } from "redis";

const CLI = fileURLToPath(new URL("../../../../bin/yyds.mjs", import.meta.url));
const SHARD_FILE = "local-0.yykv";
const CLUSTER_ID = "cluster-yykv";
const NODE_ID = "node-yykv";

/** Runs the real CLI as a child process and returns its exit code plus all output. */
function runCli(args: string[], timeoutMs = 30_000): Promise<{ code: number | null; output: string }> {
    return new Promise((resolve, reject) => {
        const child = spawn(process.execPath, ["--experimental-strip-types", CLI, ...args], { windowsHide: true });
        const chunks: Buffer[] = [];
        child.stdout.on("data", (chunk: Buffer) => chunks.push(chunk));
        child.stderr.on("data", (chunk: Buffer) => chunks.push(chunk));
        const timer = setTimeout(() => {
            child.kill();
            reject(new Error(`yyds ${args[0]} timed out after ${timeoutMs}ms`));
        }, timeoutMs);
        child.on("error", (error) => {
            clearTimeout(timer);
            reject(error);
        });
        child.on("exit", (code) => {
            clearTimeout(timer);
            resolve({ code, output: Buffer.concat(chunks).toString("utf8") });
        });
    });
}

/** Seals one live v3 snapshot containing one key and returns the raw shard bytes. */
async function seedSnapshot(dataDir: string): Promise<Buffer> {
    const node = await startYydsNode({ dataDir, clusterId: CLUSTER_ID, nodeId: NODE_ID, expirySweepIntervalMs: 3_600_000 });
    const client = createClient({ url: `redis://${node.redis.host}:${node.redis.port}`, RESP: 2, disableClientInfo: true });
    client.on("error", () => undefined);
    await client.connect();
    assert.equal(await client.set("sealed", "value"), "OK");
    await client.close();
    await node.stop({ wipe: false });
    return readFile(join(dataDir, SHARD_FILE));
}

/**
 * Starts the real node over a damaged snapshot and requires a loud refusal: the
 * process must exit non-zero with the corruption error, nothing may listen on the
 * Redis port, and the damaged file must stay untouched instead of being replaced
 * by a fresh empty shard.
 */
async function startRefusesDamagedShard(dataDir: string, pattern: RegExp): Promise<void> {
    const redisPort = await freePort();
    const pgsqlPort = await freePort();
    const mysqlPort = await freePort();
    const shardPath = join(dataDir, SHARD_FILE);
    const before = await readFile(shardPath);
    const result = await runCli([
        "start",
        "--data-dir",
        dataDir,
        "--cluster-id",
        CLUSTER_ID,
        "--node-id",
        NODE_ID,
        "--redis-port",
        String(redisPort),
        "--pgsql-port",
        String(pgsqlPort),
        "--mysql-port",
        String(mysqlPort),
    ]);
    assert.notEqual(result.code, 0, `the node must refuse to serve a damaged shard:\n${result.output}`);
    assert.match(result.output, pattern);

    // No listener may serve the damaged namespace: a real TCP client is refused.
    await assert.rejects(refusedConnection(redisPort), "no listener may serve a damaged shard");

    // The refusal must not rewrite the file: a recovery attempt keeps the evidence.
    assert.deepEqual(await readFile(shardPath), before, "a refused start must not replace the damaged shard");
}

/** Resolves only if a TCP handshake succeeds; rejects on refusal. */
function refusedConnection(port: number): Promise<void> {
    return new Promise((resolve, reject) => {
        const socket = netConnect({ host: "127.0.0.1", port });
        const timer = setTimeout(() => {
            socket.destroy();
            reject(new Error(`no connection state for port ${port} within 5s`));
        }, 5_000);
        socket.once("connect", () => {
            clearTimeout(timer);
            socket.destroy();
            resolve();
        });
        socket.once("error", (error) => {
            clearTimeout(timer);
            socket.destroy();
            reject(error);
        });
    });
}

async function withSealedShard(body: (dataDir: string, bytes: Buffer) => Promise<void>): Promise<void> {
    const dataDir = await mkdtemp(join(tmpdir(), "yyds-yykv-"));
    try {
        await body(dataDir, await seedSnapshot(dataDir));
    } finally {
        await rm(dataDir, { recursive: true, force: true });
    }
}

test("a flipped snapshot byte stops the real node with a checksum error", { timeout: 120_000 }, async () => {
    await withSealedShard(async (dataDir, original) => {
        const damaged = Buffer.from(original);
        damaged[damaged.length - 1] ^= 0xff; // last body byte, checksum left untouched
        await writeFile(join(dataDir, SHARD_FILE), damaged);
        await startRefusesDamagedShard(dataDir, /yykv checksum mismatch/);
    });
});

test("a truncated snapshot stops the real node instead of serving an empty shard", { timeout: 120_000 }, async () => {
    await withSealedShard(async (dataDir, original) => {
        // Keep the magic and only a partial checksum: reading the checksum itself must fail.
        await writeFile(join(dataDir, SHARD_FILE), original.subarray(0, 8));
        await startRefusesDamagedShard(dataDir, /unexpected shard eof/);
    });
});

test("a v2 snapshot stops the real node instead of being read without a checksum", { timeout: 120_000 }, async () => {
    await withSealedShard(async (dataDir, original) => {
        const legacy = Buffer.from(original);
        legacy[4] = 0x02; // MAGIC carries the format version byte in position 4
        await writeFile(join(dataDir, SHARD_FILE), legacy);
        await startRefusesDamagedShard(dataDir, /legacy yykv v2 snapshot/);
    });
});
