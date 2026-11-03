import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";

import type { YydsProtocol } from "./cluster.ts";
import { loadYydsNative } from "./load.ts";

const DEFAULT_REDIS_PORT = 6379;
const DEFAULT_EXPIRY_SWEEP_INTERVAL_MS = 100;

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
    /** Background expiry reclamation period in milliseconds. Defaults to 100. */
    expirySweepIntervalMs?: number;
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
        if (argument === "--expiry-sweep-interval-ms") {
            const value = Number(args[++index]);
            if (!Number.isInteger(value) || value <= 0)
                throw new Error("expiry sweep interval must be a positive integer");
            options.expirySweepIntervalMs = value;
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
    const sweeper = native.ExpirySweeperHandle.start(
        executor,
        options.expirySweepIntervalMs ?? DEFAULT_EXPIRY_SWEEP_INTERVAL_MS,
    );
    const redis = new native.RedisServiceHandle(
        lease,
        options.redisPort ?? DEFAULT_REDIS_PORT,
        executor,
    );

    // 提供了 catalog 时走 catalog 派生的 SQL runtime，否则用本地持久化 schema registry。
    const hasRuntime =
        options.catalogPath !== undefined ||
        options.mysqlPort !== undefined ||
        options.pgsqlPort !== undefined;
    const schemaPath = join(options.dataDir, "sql-schema.v0.json");
    // 两个 SQL gateway 必须共用同一个 registry：各开一份会让 type id 撞车，
    // 把两张表映射到同一个 KV namespace，并在每次 DDL 时互相覆盖持久化状态。
    const sharedSchema =
        options.catalogPath === undefined && (options.pgsqlPort !== undefined || options.mysqlPort !== undefined)
            ? native.SqlSchemaRegistryHandle.open(schemaPath)
            : undefined;
    const pgsql =
        options.pgsqlPort === undefined
            ? undefined
            : options.catalogPath !== undefined
                ? native.PgsqlServiceHandle.startWithExecutor(
                      lease,
                      options.pgsqlPort,
                      executor,
                      options.catalogPath,
                  )
                : native.PgsqlServiceHandle.startWithSharedSchema(
                      lease,
                      options.pgsqlPort,
                      executor,
                      sharedSchema,
                  );
    const mysql =
        options.mysqlPort === undefined
            ? undefined
            : options.catalogPath !== undefined
                ? native.MysqlServiceHandle.startWithExecutor(
                      lease,
                      options.mysqlPort,
                      executor,
                      options.catalogPath,
                  )
                : native.MysqlServiceHandle.startWithSharedSchema(
                      lease,
                      options.mysqlPort,
                      executor,
                      sharedSchema,
                  );
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
    sweeper.stop();
    native.nodeClearRuntimeArtifacts(options.dataDir, runtime.generation);
}
