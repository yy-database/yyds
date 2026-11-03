// One-command SQL smoke: start a yyds node, run the TypeScript and Python
// real-driver suites against it, then stop the node.
//
// `node e2e/run.mjs` is the CI entry point. It needs no external database:
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

const ROOT = resolve(import.meta.dirname, "..");
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

const [redisPort, pgPort, mysqlPort] = await Promise.all([freePort(), freePort(), freePort()]);
const dataDir = mkdtempSync(join(tmpdir(), "yyds-sql-smoke-"));
const clusterId = `smoke-cluster-${process.pid}`;
const nodeId = `smoke-node-${process.pid}`;
const schemaPath = join(dataDir, "sql-schema.v0.json");
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
        if (announced && (await portOpen(pgPort)) && (await portOpen(mysqlPort))) {
            console.log(`[yyds] running pgsql=${pgPort} mysql=${mysqlPort} redis=${redisPort}`);
            return;
        }
        await new Promise((r) => setTimeout(r, 50));
    }
    throw new Error(`node did not accept SQL connections within ${START_TIMEOUT_MS / 1000}s\n${nodeLogs()}`);
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

    const env = { ...process.env, YYDS_HOST: host, YYDS_PG_PORT: String(pgPort), YYDS_MYSQL_PORT: String(mysqlPort) };
    let failures = 0;
    failures += await runSuite(
        "ts",
        process.execPath,
        ["--experimental-strip-types", join(ROOT, "e2e/smoke.ts")],
        { ...env, YYDS_SMOKE_TAG: "ts" },
    );
    failures += await runSuite("py", python, [join(ROOT, "e2e/smoke.py")], { ...env, YYDS_SMOKE_TAG: "py" });

    if (!existsSync(schemaPath)) throw new Error(`schema registry was not persisted at ${schemaPath}`);
    console.log(`[schema] ${schemaPath} (${statSync(schemaPath).size} bytes)`);

    exitCode = failures === 0 ? 0 : 1;
    console.log(`[smoke] ${failures === 0 ? "PASS" : "FAIL"} suites=2 failed=${failures}`);
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
