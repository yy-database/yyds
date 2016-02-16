import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";

const REPO_CLI = fileURLToPath(
    new URL("../../../packages/yyds/bin/yyds.mjs", import.meta.url),
);
const DEFAULT_STARTUP_TIMEOUT_MS = 20_000;
const STATUS_INTERVAL_MS = 200;

export interface YydsAddress {
    host: string;
    port: number;
}

export interface StartYydsNodeOptions {
    mysqlPort?: number;
    pgsqlPort?: number;
    redisPort?: number;
    mongoPort?: number;
    /** Starts the MongoDB handshake probe. Enabled by `mongoPort`. */
    mongo?: boolean;
    startupTimeoutMs?: number;
}

export interface YydsNode {
    dataDir: string;
    clusterId: string;
    nodeId: string;
    redis: YydsAddress;
    mysql: YydsAddress;
    pgsql: YydsAddress;
    mongo?: YydsAddress;
    stop: () => Promise<void>;
}

interface CliResult {
    code: number | null;
    stdout: string;
    stderr: string;
}

interface RuntimeStatus {
    redisAddress?: string | null;
    mysqlAddress?: string | null;
    pgsqlAddress?: string | null;
}

function runCli(args: string[], timeoutMs = 10_000): Promise<CliResult> {
    return new Promise((resolve, reject) => {
        const child = spawn(process.execPath, ["--experimental-strip-types", REPO_CLI, ...args], {
            windowsHide: true,
        });
        const stdout: Buffer[] = [];
        const stderr: Buffer[] = [];
        let settled = false;
        const timer = setTimeout(() => {
            if (settled) return;
            settled = true;
            child.kill();
            reject(new Error(`yyds ${args[0]} timed out after ${timeoutMs}ms`));
        }, timeoutMs);
        child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
        child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
        child.on("error", (error) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            reject(error);
        });
        child.on("exit", (code) => {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            resolve({
                code,
                stdout: Buffer.concat(stdout).toString("utf8"),
                stderr: Buffer.concat(stderr).toString("utf8"),
            });
        });
    });
}

function reservePort(used: Set<number>): Promise<number> {
    return new Promise((resolve, reject) => {
        const server = createServer();
        server.once("error", reject);
        server.listen(0, "127.0.0.1", () => {
            const address = server.address();
            if (address === null || typeof address === "string") {
                server.close();
                reject(new Error("could not reserve a loopback port"));
                return;
            }
            server.close(() => {
                if (used.has(address.port)) {
                    resolve(reservePort(used));
                    return;
                }
                used.add(address.port);
                resolve(address.port);
            });
        });
    });
}

/** Parses `127.0.0.1:6379` or `[::1]:6379` into a host and port pair. */
export function parseAddress(value: string): YydsAddress {
    const trimmed = value.trim();
    if (trimmed.startsWith("[")) {
        const end = trimmed.indexOf("]");
        if (end < 0) throw new Error(`invalid address '${value}'`);
        const port = Number(trimmed.slice(end + 2));
        if (!Number.isInteger(port) || port <= 0) throw new Error(`invalid address '${value}'`);
        return { host: trimmed.slice(1, end), port };
    }
    const separator = trimmed.lastIndexOf(":");
    if (separator <= 0) throw new Error(`invalid address '${value}'`);
    const port = Number(trimmed.slice(separator + 1));
    if (!Number.isInteger(port) || port <= 0) throw new Error(`invalid address '${value}'`);
    return { host: trimmed.slice(0, separator) || "127.0.0.1", port };
}

interface StatusSnapshot {
    ownership: string;
    runtime: RuntimeStatus | null;
}

function lastJsonLine(stdout: string): RuntimeStatus | null {
    const lines = stdout.trim().split(/\r?\n/);
    const last = lines[lines.length - 1] ?? "";
    try {
        const snapshot = JSON.parse(last) as StatusSnapshot;
        return snapshot.runtime;
    } catch {
        throw new Error(`unexpected status output: ${JSON.stringify(stdout)}`);
    }
}

function describeFailure(stderr: string, stdout: string): string {
    const detail = `${stderr}\n${stdout}`.trim();
    return detail.length > 0 ? detail : "no child output";
}

async function readStatus(dataDir: string): Promise<RuntimeStatus | null> {
    const result = await runCli(["status", "--data-dir", dataDir], 10_000);
    if (result.code !== 0) throw new Error(`status exited with ${result.code}: ${describeFailure(result.stderr, result.stdout)}`);
    return lastJsonLine(result.stdout);
}

interface NodePorts {
    redisPort: number;
    mysqlPort: number;
    pgsqlPort: number;
    mongoPort?: number;
}

function startChild(dataDir: string, clusterId: string, nodeId: string, ports: NodePorts) {
    const args = [
        "--experimental-strip-types",
        REPO_CLI,
        "start",
        "--data-dir",
        dataDir,
        "--cluster-id",
        clusterId,
        "--node-id",
        nodeId,
        "--redis-port",
        String(ports.redisPort),
        "--pgsql-port",
        String(ports.pgsqlPort),
        "--mysql-port",
        String(ports.mysqlPort),
    ];
    if (ports.mongoPort !== undefined) args.push("--mongo-port", String(ports.mongoPort));
    const child = spawn(process.execPath, args, { windowsHide: true });
    const stdout: Buffer[] = [];
    const stderr: Buffer[] = [];
    child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
    child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
    return {
        child,
        output: () => ({
            stdout: Buffer.concat(stdout).toString("utf8"),
            stderr: Buffer.concat(stderr).toString("utf8"),
        }),
    };
}

function killChild(child: ReturnType<typeof spawn>): void {
    if (child.exitCode === null && !child.killed) child.kill();
}

async function waitForAddresses(
    dataDir: string,
    startupTimeoutMs: number,
): Promise<RuntimeStatus> {
    const deadline = Date.now() + startupTimeoutMs;
    let lastError: unknown = null;
    while (Date.now() < deadline) {
        try {
            const runtime = await readStatus(dataDir);
            if (runtime?.redisAddress && runtime.mysqlAddress && runtime.pgsqlAddress) {
                return runtime;
            }
        } catch (error) {
            lastError = error;
        }
        await sleep(STATUS_INTERVAL_MS);
    }
    const suffix = lastError instanceof Error ? `: ${lastError.message}` : "";
    throw new Error(`node did not publish gateway addresses within ${startupTimeoutMs}ms${suffix}`);
}

/**
 * Starts one YYDS node in a unique temporary directory and waits until the
 * protocol listeners publish their addresses.
 */
export async function startYydsNode(options: StartYydsNodeOptions = {}): Promise<YydsNode> {
    const unique = new Set<number>();
    const redisPort = options.redisPort ?? (await reservePort(unique));
    const mysqlPort = options.mysqlPort ?? (await reservePort(unique));
    const pgsqlPort = options.pgsqlPort ?? (await reservePort(unique));
    const mongoPort =
        options.mongo || options.mongoPort !== undefined
            ? options.mongoPort ?? (await reservePort(unique))
            : undefined;
    const ports: NodePorts = { redisPort, mysqlPort, pgsqlPort, mongoPort };
    const startupTimeoutMs = options.startupTimeoutMs ?? DEFAULT_STARTUP_TIMEOUT_MS;
    const dataDir = await mkdtemp(join(tmpdir(), "yyds-node-"));
    const clusterId = `cluster-${Date.now()}`;
    const nodeId = `node-${process.pid}`;

    const init = await runCli(
        ["init", "--data-dir", dataDir, "--cluster-id", clusterId, "--node-id", nodeId],
        10_000,
    );
    if (init.code !== 0) {
        await rm(dataDir, { recursive: true, force: true });
        throw new Error(`init exited with ${init.code}: ${describeFailure(init.stderr, init.stdout)}`);
    }

    const { child, output } = startChild(dataDir, clusterId, nodeId, ports);
    let stopped = false;
    const stop = async () => {
        if (stopped) return;
        stopped = true;
        try {
            const result = await runCli(["stop", "--data-dir", dataDir], 15_000);
            if (result.code !== 0) killChild(child);
        } catch {
            killChild(child);
        }
        if (child.exitCode === null) {
            killChild(child);
            await new Promise<void>((resolve) => {
                const timer = setTimeout(resolve, 5_000);
                child.once("exit", () => {
                    clearTimeout(timer);
                    resolve();
                });
            });
        }
        killChild(child);
        await sleep(50);
        await rm(dataDir, { recursive: true, force: true });
    };

    try {
        const runtime = await waitForAddresses(dataDir, startupTimeoutMs);
        if (child.exitCode !== null) {
            throw new Error(
                `start exited with ${child.exitCode} before publishing addresses: ${describeFailure(output().stderr, output().stdout)}`,
            );
        }
        return {
            dataDir,
            clusterId,
            nodeId,
            redis: parseAddress(runtime.redisAddress as string),
            mysql: parseAddress(runtime.mysqlAddress as string),
            pgsql: parseAddress(runtime.pgsqlAddress as string),
            mongo: mongoPort === undefined ? undefined : parseAddress(`127.0.0.1:${mongoPort}`),
            stop,
        };
    } catch (error) {
        const detail = output();
        await stop();
        throw new Error(`${error instanceof Error ? error.message : String(error)}\n${describeFailure(detail.stderr, detail.stdout)}`);
    }
}

/** Finds a free loopback port for tests that need a custom listener. */
export async function freePort(): Promise<number> {
    return reservePort(new Set<number>());
}
