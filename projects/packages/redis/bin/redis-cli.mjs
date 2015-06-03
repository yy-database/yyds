#!/usr/bin/env node
import { loadDisguiseRuntime } from "../src/disguise.ts";

const runtime = loadDisguiseRuntime("redis");
const [command = "PING"] = process.argv.slice(2);
const upper = command.toUpperCase();

if (upper === "PING") {
    console.log("PONG");
    process.exit(0);
}

if (upper === "INFO") {
    console.log(`# Server`);
    console.log(`redis_version:yyds-${runtime.yydsVersion}`);
    console.log(`redis_mode:disguise`);
    console.log(`role:master`);
    process.exit(0);
}

console.error(`@yyds/redis: unknown command '${command}' (YYDS ${runtime.yydsVersion})`);
process.exit(1);
