import { setTimeout as sleep } from "node:timers/promises";

import type { YydsProtocol } from "./cluster.ts";
import { loadYydsNative } from "./load.ts";

const DEFAULT_REDIS_PORT = 6379;

export interface StartNodeOptions {
    dataDir: string;
    clusterId: string;
    nodeId: string;
    catalogPath?: string;
    redisPort?: number;
    pgsqlPort?: number;
    mysqlPort?: number;
    mongoPort?: number;
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
        mongodb: "--mongo-port",
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
            if (protocol === "mongodb") options.mongoPort = value;
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
        if (argument === "--catalog-path") {
            options.catalogPath = args[++index] ?? "";
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
        if (argument === "--mongo-port") {
            options.mongoPort = Number(args[++index]);
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
    if (options.catalogPath !== undefined && options.catalogPath.length === 0) {
        // 空字符串视为未提供；避免将空值传给 Rust。
        options.catalogPath = undefined;
    }
    const defaultPort = defaultPorts[protocol];
    if (protocol === "mysql" && options.mysqlPort === undefined && defaultPort !== undefined) {
        options.mysqlPort = defaultPort;
    }
    if (protocol === "postgresql" && options.pgsqlPort === undefined && defaultPort !== undefined) {
        options.pgsqlPort = defaultPort;
    }
    void portFlags; // 变量声明用于协议完整性校验，运行时不再读取
    return options;
}

/** Starts protocol listeners for one node directory and blocks until stop is requested. */
export async function runNodeStart(options: StartNodeOptions): Promise<void> {
    const native = loadYydsNative();
    const identity = new native.NodeIdentityHandle(options.clusterId, options.nodeId);
    const lease = native.NodeLeaseHandle.acquire(options.dataDir, identity);
    const executor = new native.LocalExecutorHandle(options.dataDir, options.catalogPath);
    const redis = new native.RedisServiceHandle(
        lease,
        options.redisPort ?? DEFAULT_REDIS_PORT,
        executor,
    );

    // SQL runtimes 仅在提供了 catalog 时启动完整执行路径；否则保持协议壳。
    const hasRuntime = options.catalogPath !== undefined;
    const pgsql =
        options.pgsqlPort === undefined
            ? undefined
            : hasRuntime
                ? native.PgsqlServiceHandle.startWithExecutor(
                      lease,
                      options.pgsqlPort,
                      executor,
                      options.catalogPath!,
                  )
                : new native.PgsqlServiceHandle(lease, options.pgsqlPort);
    const mysql =
        options.mysqlPort === undefined
            ? undefined
            : hasRuntime
                ? native.MysqlServiceHandle.startWithExecutor(
                      lease,
                      options.mysqlPort,
                      executor,
                      options.catalogPath!,
                  )
                : new native.MysqlServiceHandle(lease, options.mysqlPort);
    const mongo =
        options.mongoPort === undefined
            ? undefined
            : new native.MongoServiceHandle(lease, options.mongoPort, executor);
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

    const runtimeTag = hasRuntime ? "sql-runtime" : "protocol-only";
    process.stdout.write(
        `yyds running pid=${runtime.pid} mode=${runtimeTag} redis=${runtime.redisAddress} pgsql=${runtime.pgsqlAddress ?? "null"} mysql=${runtime.mysqlAddress ?? "null"} mongo=${mongo?.address() ?? "null"} wire=${runtime.wireAddress ?? "null"}\n`,
    );

    while (true) {
        if (native.nodeStopRequested(options.dataDir, runtime.generation)) break;
        if (redis.isFinished()) break;
        if (pgsql?.isFinished()) break;
        if (mysql?.isFinished()) break;
        if (mongo?.isFinished()) break;
        if (wire?.isFinished()) break;
        await sleep(50);
    }

    redis.stop();
    pgsql?.stop();
    mysql?.stop();
    mongo?.stop();
    wire?.stop();
    native.nodeClearRuntimeArtifacts(options.dataDir, runtime.generation);
}
