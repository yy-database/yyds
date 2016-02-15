import { setTimeout as sleep } from "node:timers/promises";

import type { YydsProtocol } from "./cluster.ts";
import { loadYydsNative } from "./load.ts";

const DEFAULT_REDIS_PORT = 6379;

export interface StartNodeOptions {
    dataDir: string;
    clusterId: string;
    nodeId: string;
    redisPort?: number;
    pgsqlPort?: number;
    mysqlPort?: number;
    wirePort?: number;
}

export function parseStartOptions(protocol: YydsProtocol, args: string[]): StartNodeOptions {
    const options: StartNodeOptions = {
        dataDir: "",
        clusterId: "",
        nodeId: "",
        redisPort: DEFAULT_REDIS_PORT,
    };
    const portFlags: Record<YydsProtocol, string> = {
        redis: "--redis-port",
        mysql: "--mysql-port",
        postgresql: "--pgsql-port",
    };
    const defaultPorts: Partial<Record<YydsProtocol, number>> = { mysql: 3306, postgresql: 5432 };

    for (let index = 0; index < args.length; index += 1) {
        const argument = args[index];
        if (["--port", "-p", "-P"].includes(argument)) {
            const value = Number(args[++index]);
            if (!Number.isInteger(value) || value < 0)
                throw new Error(`missing port after ${argument}`);
            if (protocol === "redis") options.redisPort = value;
            if (protocol === "mysql") options.mysqlPort = value;
            if (protocol === "postgresql") options.pgsqlPort = value;
            continue;
        }
        if (argument === "--data-dir") {
            options.dataDir = args[++index] ?? "";
            continue;
        }
        if (argument === "--cluster-id") {
            options.clusterId = args[++index] ?? "";
            continue;
        }
        if (argument === "--node-id") {
            options.nodeId = args[++index] ?? "";
            continue;
        }
        if (argument === "--redis-port") {
            options.redisPort = Number(args[++index]);
            continue;
        }
        if (argument === "--pgsql-port") {
            options.pgsqlPort = Number(args[++index]);
            continue;
        }
        if (argument === "--mysql-port") {
            options.mysqlPort = Number(args[++index]);
            continue;
        }
        if (argument === "--wire-port") {
            options.wirePort = Number(args[++index]);
            continue;
        }
        throw new Error(`unknown option '${argument}'`);
    }

    if (!options.dataDir || !options.clusterId || !options.nodeId) {
        throw new Error("start requires --data-dir, --cluster-id, and --node-id");
    }
    const defaultPort = defaultPorts[protocol];
    if (protocol === "mysql" && options.mysqlPort === undefined && defaultPort !== undefined) {
        options.mysqlPort = defaultPort;
    }
    if (protocol === "postgresql" && options.pgsqlPort === undefined && defaultPort !== undefined) {
        options.pgsqlPort = defaultPort;
    }
    return options;
}

/** Starts protocol listeners for one node directory and blocks until stop is requested. */
export async function runNodeStart(options: StartNodeOptions): Promise<void> {
    const native = loadYydsNative();
    const identity = new native.NodeIdentityHandle(options.clusterId, options.nodeId);
    const lease = native.NodeLeaseHandle.acquire(options.dataDir, identity);
    const executor = new native.LocalExecutorHandle(options.dataDir);
    const redis = new native.RedisServiceHandle(
        lease,
        options.redisPort ?? DEFAULT_REDIS_PORT,
        executor,
    );
    const pgsql =
        options.pgsqlPort === undefined
            ? undefined
            : new native.PgsqlServiceHandle(lease, options.pgsqlPort);
    const mysql =
        options.mysqlPort === undefined
            ? undefined
            : new native.MysqlServiceHandle(lease, options.mysqlPort);
    const wire =
        options.wirePort === undefined
            ? undefined
            : new native.WireServerHandle(options.wirePort, executor);

    const runtime = {
        version: 1,
        generation: crypto.randomUUID(),
        pid: process.pid,
        startedAtUnixMs: Date.now(),
        redisAddress: redis.address(),
        pgsqlAddress: pgsql?.address(),
        mysqlAddress: mysql?.address(),
        wireAddress: wire?.address(),
    };
    native.nodeWriteRuntimeStatus(options.dataDir, JSON.stringify(runtime));

    process.stdout.write(
        `yyds running pid=${runtime.pid} redis=${runtime.redisAddress} pgsql=${runtime.pgsqlAddress ?? "null"} mysql=${runtime.mysqlAddress ?? "null"} wire=${runtime.wireAddress ?? "null"}\n`,
    );

    while (true) {
        if (native.nodeStopRequested(options.dataDir, runtime.generation)) break;
        if (redis.isFinished()) break;
        if (pgsql?.isFinished()) break;
        if (mysql?.isFinished()) break;
        if (wire?.isFinished()) break;
        await sleep(50);
    }

    redis.stop();
    pgsql?.stop();
    mysql?.stop();
    wire?.stop();
    native.nodeClearRuntimeArtifacts(options.dataDir, runtime.generation);
}
