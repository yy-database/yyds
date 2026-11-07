// One-command SQL smoke: start a yyds node, run the TypeScript and Python
// real-driver suites against it, then stop the node.
//
// `node projects/yyds.ts/packages/yyds-harness/run.mjs` is the CI entry point. It needs no external database:
// the node starts in schema-registry mode, so `smoke.ts` / `smoke.py` create
// their own tables and the registry is persisted to
// `<data-dir>/sql-schema.v0.json`, which this script verifies after the runs.
//
// `YYDS_KEEP_DATA_DIR=1` keeps the temporary data directory for inspection.

import { spawn, spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, rmSync, statSync } from "node:fs";
import { createServer, Socket } from "node:net";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";

const ROOT = resolve(import.meta.dirname, "../../../..");
const CLI = join(ROOT, "projects/yyds.ts/packages/yyds/bin/yyds.mjs");
const START_TIMEOUT_MS = 90_000;
const RUNNING_LINE = /^yyds running pid=\d+ mode=(\S+)/;

if (!existsSync(CLI)) {
    console.error(`[yyds] CLI not found at ${CLI}`);
    process.exit(1);
}

function freePort() {
    return new Promise((resolvePort, reject) => {
        const server = createServer();
        server.on("error", reject);
        server.listen(0, "127.0.0.1", () => {
            const port = server.address().port;
            server.close(() => resolvePort(port));
        });
    });
}

function portOpen(port) {
    return new Promise((resolveProbe) => {
        const socket = new Socket();
        socket.once("connect", () => {
            socket.destroy();
            resolveProbe(true);
        });
        socket.once("error", () => resolveProbe(false));
        socket.setTimeout(500, () => {
            socket.destroy();
            resolveProbe(false);
        });
        socket.connect(port, "127.0.0.1");
    });
}

function runCli(args, timeout) {
    const result = spawnSync(process.execPath, ["--experimental-strip-types", CLI, ...args], {
        encoding: "utf8",
        timeout,
    });
    return { code: result.status ?? -1, stdout: result.stdout ?? "", stderr: result.stderr ?? "" };
}

const [redisPort, pgPort, mysqlPort, mongoPort] = await Promise.all([freePort(), freePort(), freePort(), freePort()]);
const dataDir = mkdtempSync(join(tmpdir(), "yyds-sql-smoke-"));
const clusterId = `smoke-cluster-${process.pid}`;
const nodeId = `smoke-node-${process.pid}`;
const schemaPath = join(dataDir, "sql-schema.v0.json");
const recoveryStatePath = join(dataDir, "recovery-state.json");
const host = "127.0.0.1";
const python = process.env.YYDS_PYTHON ?? "python";

const nodeArgs = [
    "--experimental-strip-types",
    CLI,
    "start",
    "--data-dir",
    dataDir,
    "--cluster-id",
    clusterId,
    "--node-id",
    nodeId,
    "--redis-port",
    String(redisPort),
    "--pgsql-port",
    String(pgPort),
    "--mysql-port",
    String(mysqlPort),
    "--mongo-port",
    String(mongoPort),
];

const stdoutLines = [];
const stderrLines = [];
let nodeProcess = null;
let exitCode = 1;

function nodeLogs() {
    return `--- node stdout ---\n${stdoutLines.join("\n")}\n--- node stderr ---\n${stderrLines.join("\n")}`;
}

async function stopNode() {
    if (nodeProcess === null) return;
    const stopped = runCli(["stop", "--data-dir", dataDir], 20_000);
    console.log(`[stop] exit=${stopped.code}`);
    const exited = new Promise((resolveExit) => nodeProcess.once("exit", resolveExit));
    const timeout = new Promise((resolveTimeout) => setTimeout(resolveTimeout, 10_000, "timeout"));
    if ((await Promise.race([exited, timeout])) === "timeout") {
        nodeProcess.kill();
        await exited;
    }
    nodeProcess = null;
}

function sleep(ms) {
    return new Promise((resolveSleep) => setTimeout(resolveSleep, ms));
}

async function waitUntilReady() {
    const deadline = Date.now() + START_TIMEOUT_MS;
    let announced = false;
    while (Date.now() < deadline) {
        if (nodeProcess.exitCode !== null) {
            throw new Error(`yyds start exited with code ${nodeProcess.exitCode} before readiness\n${nodeLogs()}`);
        }
        for (const line of stdoutLines) {
            const match = RUNNING_LINE.exec(line);
            if (match === null) continue;
            if (match[1] !== "sql-runtime") {
                throw new Error(`node started in '${match[1]}' mode, so the SQL gateways are not mounted\n${nodeLogs()}`);
            }
            if (line.includes("pgsql=null") || line.includes("mysql=null")) {
                throw new Error(`node published no SQL gateway address: ${line}`);
            }
            announced = true;
        }
        if (
            announced
            && (await portOpen(pgPort))
            && (await portOpen(mysqlPort))
            && (await portOpen(redisPort))
            && (await portOpen(mongoPort))
        ) {
            console.log(`[yyds] running pgsql=${pgPort} mysql=${mysqlPort} redis=${redisPort} mongo=${mongoPort}`);
            return;
        }
        await sleep(50);
    }
    throw new Error(`node did not accept SQL connections within ${START_TIMEOUT_MS / 1000}s\n${nodeLogs()}`);
}

async function startNode() {
    stdoutLines.length = 0;
    stderrLines.length = 0;
    console.log(`[yyds] ${[process.execPath, ...nodeArgs].join(" ")}`);
    nodeProcess = spawn(process.execPath, nodeArgs, { stdio: ["ignore", "pipe", "pipe"] });
    nodeProcess.stdout.setEncoding("utf8");
    nodeProcess.stderr.setEncoding("utf8");
    nodeProcess.stdout.on("data", (chunk) => {
        for (const line of chunk.split(/\r?\n/)) {
            if (line.length > 0) stdoutLines.push(line);
        }
    });
    nodeProcess.stderr.on("data", (chunk) => {
        for (const line of chunk.split(/\r?\n/)) {
            if (line.length > 0) stderrLines.push(line);
        }
    });
    await waitUntilReady();
}

async function waitPortClosed(port) {
    const deadline = Date.now() + 10_000;
    while (Date.now() < deadline) {
        if (!(await portOpen(port))) return;
        await sleep(50);
    }
    throw new Error(`port ${port} stayed open after yyds stop`);
}

function runSuite(label, command, args, env) {
    console.log(`[${label}] ${command} ${args.join(" ")}`);
    return new Promise((resolveRun) => {
        const child = spawn(command, args, { cwd: ROOT, stdio: "inherit", env });
        child.on("error", (error) => {
            console.error(`[${label}] failed to spawn: ${error.message}`);
            resolveRun(1);
        });
        child.on("exit", (code, signal) => {
            const exit = code ?? (signal ? 1 : 0);
            console.log(`[${label}] exit=${exit}${signal ? ` signal=${signal}` : ""}`);
            resolveRun(exit === 0 ? 0 : 1);
        });
    });
}

try {
    const init = runCli(["init", "--data-dir", dataDir, "--cluster-id", clusterId, "--node-id", nodeId], 30_000);
    console.log(`[yyds] init exit=${init.code}`);
    if (init.code !== 0) throw new Error(`yyds init failed: ${init.stderr.trim()}`);

    await startNode();

    const env = {
        ...process.env,
        YYDS_HOST: host,
        YYDS_PG_PORT: String(pgPort),
        YYDS_MYSQL_PORT: String(mysqlPort),
        YYDS_REDIS_PORT: String(redisPort),
        YYDS_MONGO_PORT: String(mongoPort),
    };
    let failures = 0;
    failures += await runSuite(
        "ts",
        process.execPath,
        ["--experimental-strip-types", join(import.meta.dirname, "smoke.ts")],
        { ...env, YYDS_SMOKE_TAG: "ts" },
    );
    failures += await runSuite("py", python, [join(import.meta.dirname, "smoke.py")], { ...env, YYDS_SMOKE_TAG: "py" });
    failures += await runSuite(
        "protocol-ts",
        process.execPath,
        ["--experimental-strip-types", join(import.meta.dirname, "protocol.ts")],
        { ...env, YYDS_PROTOCOL_TAG: "ts" },
    );
    failures += await runSuite("protocol-py", python, [join(import.meta.dirname, "protocol.py")], {
        ...env,
        YYDS_PROTOCOL_TAG: "py",
    });
    failures += await runSuite(
        "examples-ts",
        process.execPath,
        ["--experimental-strip-types", join(ROOT, "projects/yyds.ts/examples/bulk.ts")],
        env,
    );
    failures += await runSuite("examples-py", python, [join(ROOT, "projects/yyds.py/examples/bulk.py")], env);
    failures += await runSuite("examples-redis-py", python, [join(ROOT, "projects/yyds.py/examples/redis_roundtrip.py")], env);
    failures += await runSuite("examples-mongo-py", python, [join(ROOT, "projects/yyds.py/examples/mongo.py")], env);
    failures += await runSuite(
        "examples-redis-ts",
        process.execPath,
        ["--experimental-strip-types", join(ROOT, "projects/yyds.ts/examples/redis.ts")],
        env,
    );
    failures += await runSuite(
        "examples-mongo-ts",
        process.execPath,
        ["--experimental-strip-types", join(ROOT, "projects/yyds.ts/examples/mongo.ts")],
        env,
    );

    if (!existsSync(schemaPath)) throw new Error(`schema registry was not persisted at ${schemaPath}`);
    console.log(`[schema] ${schemaPath} (${statSync(schemaPath).size} bytes)`);

    // 重新启动同一数据目录，验证行、序列、唯一键和 Redis 截止期都真正落盘。
    const recoveryEnv = {
        ...env,
        YYDS_RECOVERY_STATE: recoveryStatePath,
    };
    failures += await runSuite("recovery-seed", python, [join(import.meta.dirname, "recovery.py")], {
        ...recoveryEnv,
        YYDS_RECOVERY_PHASE: "seed",
    });

    console.log("[restart] stopping node and restarting on the same data directory");
    await stopNode();
    for (const port of [redisPort, pgPort, mysqlPort, mongoPort]) await waitPortClosed(port);
    await startNode();

    failures += await runSuite("recovery-verify", python, [join(import.meta.dirname, "recovery.py")], {
        ...recoveryEnv,
        YYDS_RECOVERY_PHASE: "verify",
    });

    exitCode = failures === 0 ? 0 : 1;
    console.log(`[smoke] ${failures === 0 ? "PASS" : "FAIL"} suites=12 failed=${failures}`);
} catch (error) {
    console.error(`[smoke] error: ${error.message}`);
    console.error(nodeLogs());
    exitCode = 1;
} finally {
    await stopNode();
    if (process.env.YYDS_KEEP_DATA_DIR === "1") {
        console.log(`[data] kept ${dataDir}`);
    } else {
        rmSync(dataDir, { recursive: true, force: true });
    }
}

process.exit(exitCode);
