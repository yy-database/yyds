#!/usr/bin/env node
import { loadDisguiseRuntime } from "../src/disguise.ts";

const runtime = loadDisguiseRuntime("mysql");
const args = process.argv.slice(2);

if (args.includes("-V") || args.includes("--version")) {
    console.log(`mysql  Ver yyds-${runtime.yydsVersion} for YYDS on ${process.platform}-${process.arch}`);
    process.exit(0);
}

const executeIndex = args.findIndex((arg) => arg === "-e" || arg === "--execute");
const sql = executeIndex >= 0 ? String(args[executeIndex + 1] ?? "").trim() : args.join(" ").trim();

if (!sql) {
    console.log(`mysql  Ver yyds-${runtime.yydsVersion} for YYDS on ${process.platform}-${process.arch}`);
    process.exit(0);
}

if (/^select\s+1\s*;?$/i.test(sql)) {
    console.log("1");
    console.log("1");
    process.exit(0);
}

if (/^select\s+version\s*\(\s*\)\s*;?$/i.test(sql)) {
    console.log(`yyds-${runtime.yydsVersion}`);
    process.exit(0);
}

console.error(`@yyds/mysql: unsupported statement '${sql}' (YYDS ${runtime.yydsVersion})`);
process.exit(1);
