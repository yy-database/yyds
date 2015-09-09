#!/usr/bin/env node
import { SqliteConnection } from "../src/node.ts";

function formatCell(cell) {
    switch (cell.kind) {
        case "null":
            return "";
        case "integer":
            return cell.value.toString();
        case "real":
            return String(cell.value);
        case "text":
            return cell.value;
        case "blob":
            return `X'${cell.value.toString("hex")}'`;
    }
}

function runDotCommand(database, command) {
    if (command === ".tables") {
        const tables = database.execute(
            "SELECT name FROM sqlite_schema WHERE type IN ('table', 'view') AND name NOT LIKE 'sqlite_%' ORDER BY name",
        );
        for (const [name] of tables.rows) console.log(name.value);
        return;
    }
    if (command === ".schema") {
        const schema = database.execute(
            "SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid",
        );
        for (const [sql] of schema.rows) {
            const source = sql.value.trimEnd();
            console.log(source.endsWith(";") ? source : `${source};`);
        }
        return;
    }
    throw new Error(`unsupported dot command '${command}'`);
}

function main(args) {
    if (args.length === 1 && ["--version", "-version"].includes(args[0])) {
        const version = new SqliteConnection(":memory:").execute("SELECT sqlite_version()")
            .rows[0][0];
        console.log(`sqlite3 ${version.value} (YYDS bundled SQLite)`);
        return;
    }
    if (args.length === 1 && ["--help", "-help"].includes(args[0])) {
        console.log(
            "Usage: sqlite3 DATABASE SQL\nExecutes one SQLite statement. Supported dot commands: .tables, .schema.",
        );
        return;
    }
    let readOnly = false;
    if (args[0] === "-readonly") {
        readOnly = true;
        args = args.slice(1);
    }
    if (args.length !== 2 || args[0].startsWith("-") || args[0] === "") {
        throw new Error("expected [-readonly] DATABASE and one SQL statement or supported dot command");
    }
    const [path, input] = args;
    const database = new SqliteConnection(path, { readOnly });
    if (input.trim().startsWith(".")) {
        runDotCommand(database, input.trim());
        return;
    }
    const result = database.execute(input);
    for (const row of result.rows) console.log(row.map(formatCell).join("|"));
}

try {
    main(process.argv.slice(2));
} catch (error) {
    console.error(`@yyds/sqlite: ${error.message}`);
    process.exitCode = 1;
}
