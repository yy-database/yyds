#!/usr/bin/env node
import { loadDisguiseRuntime } from "../src/disguise.ts";

const runtime = loadDisguiseRuntime("postgresql");
const args = process.argv.slice(2);

if (args.includes("-V") || args.includes("--version")) {
    console.log(`psql (YYDS ${runtime.yydsVersion})`);
    process.exit(0);
}

const commandIndex = args.findIndex((arg) => arg === "-c" || arg === "--command");
const sql = commandIndex >= 0 ? String(args[commandIndex + 1] ?? "").trim() : "";

if (args.includes("\\conninfo") || sql === "\\conninfo") {
    console.log(`You are connected to database "yyds" as user "yyds" via YYDS ${runtime.yydsVersion}.`);
    process.exit(0);
}

if (/^select\s+1\s*;?$/i.test(sql)) {
    console.log(" ?column? ");
    console.log("----------");
    console.log("        1");
    console.log("(1 row)");
    process.exit(0);
}

if (/^select\s+version\s*\(\s*\)\s*;?$/i.test(sql)) {
    console.log(`yyds-${runtime.yydsVersion}`);
    process.exit(0);
}

if (!sql) {
    console.log(`psql (YYDS ${runtime.yydsVersion})`);
    console.log('Type "help" for help.');
    process.exit(0);
}

console.error(`@yyds/postgresql: unsupported input '${sql}' (YYDS ${runtime.yydsVersion})`);
process.exit(1);
