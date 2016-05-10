#!/usr/bin/env node
import { readFileSync } from "node:fs";

import { loadYydsNative } from "../src/node/load.ts";
import { parseStartOptions, runNodeStart } from "../src/node/runtime.ts";
import { parseWebuiArgs, runWebui } from "../src/node/webui-cmd.ts";

function usage() {
    return "usage: yyds <init|start|status|stop|webui|catalog> ...";
}

function startUsage() {
    return "usage: yyds start --data-dir DIR --cluster-id ID --node-id ID [--catalog-path FILE] [--redis-port PORT] [--pgsql-port PORT] [--mysql-port PORT] [--mongo-port PORT] [--wire-port PORT]";
}

function catalogUsage() {
    return "usage: yyds catalog --catalog-path FILE --schema-file FILE [--schema-version NUMBER]";
}

function webuiUsage() {
    return "usage: yyds webui [--endpoint URL | --data-dir DIR --cluster-id ID --node-id ID [--wire-port PORT]] [--port NUMBER] [--no-open]";
}

function parseCommonArgs(command, args) {
    const options = {
        dataDir: "",
        clusterId: "",
        nodeId: "",
        catalogPath: undefined,
        redisPort: 6379,
        pgsqlPort: undefined,
        mysqlPort: undefined,
        mongoPort: undefined,
        wirePort: undefined,
    };
    for (let index = 0; index < args.length; index += 1) {
        const flag = args[index];
        const value = args[index + 1];
        if (!value || value.startsWith("--")) throw new Error(`missing value for ${flag}`);
        switch (flag) {
            case "--data-dir":
                options.dataDir = value;
                index += 1;
                break;
            case "--cluster-id":
                options.clusterId = value;
                index += 1;
                break;
            case "--node-id":
                options.nodeId = value;
                index += 1;
                break;
            case "--catalog-path":
                if (command !== "start")
                    throw new Error(`option ${flag} is not valid for this command`);
                options.catalogPath = value;
                index += 1;
                break;
            case "--redis-port":
                if (command !== "start")
                    throw new Error(`option ${flag} is not valid for this command`);
                options.redisPort = Number(value);
                index += 1;
                break;
            case "--pgsql-port":
                if (command !== "start")
                    throw new Error(`option ${flag} is not valid for this command`);
                options.pgsqlPort = Number(value);
                index += 1;
                break;
            case "--mysql-port":
                if (command !== "start")
                    throw new Error(`option ${flag} is not valid for this command`);
                options.mysqlPort = Number(value);
                index += 1;
                break;
            case "--mongo-port":
                if (command !== "start")
                    throw new Error(`option ${flag} is not valid for this command`);
                options.mongoPort = Number(value);
                index += 1;
                break;
            case "--wire-port":
                if (command !== "start")
                    throw new Error(`option ${flag} is not valid for this command`);
                options.wirePort = Number(value);
                index += 1;
                break;
            default:
                throw new Error(`unknown option '${flag}'\n${usage()}`);
        }
    }
    if (!options.dataDir) throw new Error(usage());
    if ((command === "init" || command === "start") && (!options.clusterId || !options.nodeId)) {
        throw new Error(startUsage());
    }
    return options;
}

function runCatalog(native, args) {
    let catalogPath = "";
    let schemaFile = "";
    let schemaVersion = 1;
    for (let index = 0; index < args.length; index += 1) {
        const flag = args[index];
        const value = args[index + 1];
        if (!value || value.startsWith("--")) throw new Error(`missing value for ${flag}\n${catalogUsage()}`);
        switch (flag) {
            case "--catalog-path":
                catalogPath = value;
                index += 1;
                break;
            case "--schema-file":
                schemaFile = value;
                index += 1;
                break;
            case "--schema-version": {
                const parsed = Number(value);
                if (!Number.isInteger(parsed) || parsed <= 0) throw new Error(`invalid schema version '${value}'`);
                schemaVersion = parsed;
                index += 1;
                break;
            }
            default:
                throw new Error(`unknown option '${flag}'\n${catalogUsage()}`);
        }
    }
    if (!catalogPath || !schemaFile) throw new Error(catalogUsage());
    native.catalogPublishSchema(catalogPath, schemaVersion, readFileSync(schemaFile, "utf8"));
    console.log(`published schema version ${schemaVersion} into ${catalogPath}`);
}

async function main(args) {
    const command = args[0];
    const rest = args.slice(1);

    if (command === "webui") {
        await runWebui(parseWebuiArgs(rest));
        return;
    }

    const native = loadYydsNative();

    if (command === "catalog") {
        runCatalog(native, rest);
        return;
    }

    const options = parseCommonArgs(command, rest);

    switch (command) {
        case "init": {
            const identity = new native.NodeIdentityHandle(options.clusterId, options.nodeId);
            native.nodeInit(options.dataDir, identity);
            console.log(`initialized ${options.dataDir}`);
            return;
        }
        case "start":
            await runNodeStart({
                dataDir: options.dataDir,
                clusterId: options.clusterId,
                nodeId: options.nodeId,
                catalogPath: options.catalogPath,
                redisPort: options.redisPort,
                pgsqlPort: options.pgsqlPort,
                mysqlPort: options.mysqlPort,
                mongoPort: options.mongoPort,
                wirePort: options.wirePort,
            });
            return;
        case "status": {
            const snapshot = native.nodeInspect(options.dataDir);
            console.log(
                JSON.stringify({
                    directory: snapshot.directory,
                    identity: JSON.parse(snapshot.identity),
                    ownership: snapshot.ownership,
                    runtime: snapshot.runtime ? JSON.parse(snapshot.runtime) : null,
                    health: snapshot.health,
                }),
            );
            return;
        }
        case "stop": {
            const snapshot = native.nodeInspect(options.dataDir);
            if (snapshot.ownership === "available") throw new Error("node is not running");
            const runtime = snapshot.runtime ? JSON.parse(snapshot.runtime) : null;
            if (!runtime) throw new Error("node owner has not published runtime status");
            native.nodeRequestStop(options.dataDir, runtime.generation);
            native.nodeWaitUntilStopped(options.dataDir, runtime.generation, 10_000);
            console.log("stopped");
            return;
        }
        default:
            throw new Error(command ? `unknown command '${command}'` : `${usage()}\n${startUsage()}\n${webuiUsage()}\n${catalogUsage()}`);
    }
}

try {
    await main(process.argv.slice(2));
} catch (error) {
    console.error(`yyds: ${error.message}`);
    process.exitCode = 1;
}
