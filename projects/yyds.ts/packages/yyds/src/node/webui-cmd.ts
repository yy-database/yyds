import fs from "node:fs";
import { join } from "node:path";
import type { ChildProcess } from "node:child_process";
import type { Server } from "node:http";

import {
    openBrowser,
    resolveWebuiDist,
    spawnStart,
    startStaticServer,
    waitForWire,
    wsEndpointFromWirePort,
} from "./webui-static.ts";

export interface WebuiOptions {
    dataDir: string;
    clusterId: string;
    nodeId: string;
    wirePort: number;
    webuiPort: number;
    endpoint?: string;
    open: boolean;
}

function parsePort(raw: string | undefined, fallback: number, label: string): number {
    const value = Number(raw ?? fallback);
    if (!Number.isInteger(value) || value < 1 || value > 65535) {
        throw new Error(`invalid ${label}: ${raw ?? fallback}`);
    }
    return value;
}

export function parseWebuiArgs(args: string[]): WebuiOptions {
    const options: WebuiOptions = {
        dataDir: "",
        clusterId: "",
        nodeId: "",
        wirePort: 7700,
        webuiPort: 8780,
        open: true,
    };
    for (let index = 0; index < args.length; index += 1) {
        const flag = args[index];
        const value = args[index + 1];
        switch (flag) {
            case "--data-dir":
                options.dataDir = value ?? "";
                index += 1;
                break;
            case "--cluster-id":
                options.clusterId = value ?? "";
                index += 1;
                break;
            case "--node-id":
                options.nodeId = value ?? "";
                index += 1;
                break;
            case "--wire-port":
                options.wirePort = parsePort(value, options.wirePort, "wire-port");
                index += 1;
                break;
            case "--port":
                options.webuiPort = parsePort(value, options.webuiPort, "port");
                index += 1;
                break;
            case "--endpoint":
                options.endpoint = value ?? "";
                index += 1;
                break;
            case "--no-open":
                options.open = false;
                break;
            default:
                throw new Error(`unknown option '${flag}'`);
        }
    }
    if (!options.endpoint && (!options.dataDir || !options.clusterId || !options.nodeId)) {
        throw new Error(
            "webui requires --endpoint or --data-dir with --cluster-id and --node-id",
        );
    }
    return options;
}

export async function runWebui(options: WebuiOptions): Promise<void> {
    const endpoint = options.endpoint ?? wsEndpointFromWirePort(options.wirePort);
    const distRoot = resolveWebuiDist();
    if (!fs.existsSync(join(distRoot, "index.html"))) {
        throw new Error(
            "WebUI assets not found. Run `pnpm --filter @yydb/yyds build` to bundle webui/.",
        );
    }

    let startChild: ChildProcess | null = null;
    if (!options.endpoint) {
        startChild = spawnStart({
            dataDir: options.dataDir,
            clusterId: options.clusterId,
            nodeId: options.nodeId,
            wirePort: options.wirePort,
        });
        startChild.stdout?.on("data", (chunk) => process.stdout.write(chunk));
        startChild.stderr?.on("data", (chunk) => process.stderr.write(chunk));
        startChild.on("exit", (code) => {
            if (code && code !== 0) {
                console.error(`yyds start exited with code ${code}`);
            }
        });
        await waitForWire(endpoint, 8000);
    }

    const server = await startStaticServer(distRoot, options.webuiPort);
    const pageUrl = `http://127.0.0.1:${options.webuiPort}/?endpoint=${encodeURIComponent(endpoint)}`;

    console.log(`WebUI ${pageUrl}`);
    console.log(`Wire ${endpoint}`);

    if (options.open) {
        openBrowser(pageUrl);
    }

    await waitForShutdown(server, startChild);
}

function waitForShutdown(server: Server, startChild: ChildProcess | null): Promise<void> {
    return new Promise((resolveWait) => {
        const shutdown = () => {
            server.close();
            if (startChild && !startChild.killed) {
                startChild.kill();
            }
            resolveWait();
        };
        process.once("SIGINT", shutdown);
        process.once("SIGTERM", shutdown);
    });
}
