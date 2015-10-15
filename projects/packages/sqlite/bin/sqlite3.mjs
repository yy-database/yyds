#!/usr/bin/env node
import { readFileSync } from "node:fs";

import { SqliteSnapshot } from "../src/node.ts";

const FORMAT_VERSION = "3.45.0";

function runDotCommand(snapshot, command) {
    if (command === ".tables") {
        const tables = snapshot
            .schema()
            .filter((object) => (object.kind === "table" || object.kind === "view") && !object.name.startsWith("sqlite_"))
            .map((object) => object.name)
            .sort();
        for (const name of tables) console.log(name);
        return;
    }
    if (command === ".schema") {
        const statements = snapshot
            .schema()
            .filter((object) => object.sql && !object.name.startsWith("sqlite_"))
            .map((object) => object.sql?.trimEnd() ?? "");
        for (const source of statements) {
            console.log(source.endsWith(";") ? source : `${source};`);
        }
        return;
    }
    throw new Error(`unsupported dot command '${command}'`);
}

function main(args) {
    if (args.length === 1 && ["--version", "-version"].includes(args[0])) {
        console.log(`sqlite3 ${FORMAT_VERSION} (YYDS pure-Rust format-3 reader)`);
        return;
    }
    if (args.length === 1 && ["--help", "-help"].includes(args[0])) {
        console.log(
            "Usage: sqlite3 DATABASE COMMAND\nSupported commands: .tables, .schema\nThis CLI inspects on-disk main-file bytes only; SQL execution is not available.",
        );
        return;
    }
    if (args.length !== 2 || args[0].startsWith("-") || args[0] === "") {
        throw new Error("expected DATABASE and one supported dot command (.tables or .schema)");
    }
    const [path, input] = args;
    const snapshot = new SqliteSnapshot(readFileSync(path));
    const command = input.trim();
    if (!command.startsWith(".")) {
        throw new Error("SQL execution is not available; use .tables or .schema");
    }
    runDotCommand(snapshot, command);
}

try {
    main(process.argv.slice(2));
} catch (error) {
    console.error(`@yydb/sqlite: ${error.message}`);
    process.exitCode = 1;
}
