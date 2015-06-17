#!/usr/bin/env node
import { loadDisguiseRuntime } from "../src/disguise.ts";

const runtime = loadDisguiseRuntime("sqlite");
const args = process.argv.slice(2);

if (args.includes("-version") || args.includes("--version")) {
    console.log(`sqlite3 (YYDS ${runtime.yydsVersion})`);
    process.exit(0);
}

const databaseIndex = args.findIndex((arg) => !arg.startsWith("-"));
const database = databaseIndex >= 0 ? args[databaseIndex] : undefined;
const sqlIndex = databaseIndex >= 0 ? databaseIndex + 1 : -1;
const sql = sqlIndex >= 0 ? String(args[sqlIndex] ?? "").trim() : "";

if (!sql) {
    console.log(`sqlite3 (YYDS ${runtime.yydsVersion})`);
    if (database) {
        console.log(`binary-compatible passthrough (${database})`);
    }
    process.exit(0);
}

if (/^select\s+1\s*;?$/i.test(sql)) {
    console.log("1");
    process.exit(0);
}

if (/^select\s+sqlite_version\s*\(\s*\)\s*;?$/i.test(sql)) {
    console.log(`yyds-${runtime.yydsVersion}`);
    process.exit(0);
}

console.error(`@yyds/sqlite: unsupported statement '${sql}' (YYDS ${runtime.yydsVersion})`);
process.exit(1);
