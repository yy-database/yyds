#!/usr/bin/env node
import { SqliteConnection } from "../src/node.ts";

function formatCell(cell, nullValue) {
    switch (cell.kind) {
        case "null":
            return nullValue;
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

function csvCell(value) {
    return /[",\r\n]/.test(value) ? `"${value.replaceAll('"', '""')}"` : value;
}

function printRows(result, options) {
    const rows = result.rows.map((row) => row.map((cell) => formatCell(cell, options.nullValue)));
    if (options.headers && result.columns.length > 0) rows.unshift(result.columns);
    if (options.mode === "csv") {
        for (const row of rows) console.log(row.map(csvCell).join(","));
        return;
    }
    if (options.mode === "column") {
        const widths = result.columns.map((name, index) =>
            rows.reduce((width, row) => Math.max(width, row[index]?.length ?? 0), name.length),
        );
        for (const row of rows) {
            console.log(
                row
                    .map((value, index) => {
                        const numeric = result.rows.some(
                            (source) =>
                                source[index]?.kind === "integer" || source[index]?.kind === "real",
                        );
                        return numeric
                            ? value.padStart(widths[index])
                            : value.padEnd(widths[index]);
                    })
                    .join("  ")
                    .trimEnd(),
            );
        }
        return;
    }
    for (const row of rows) console.log(row.join(options.separator));
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
            "Usage: sqlite3 [OPTIONS] DATABASE SQL\nOptions: -readonly -header -noheader -separator SEP -nullvalue TEXT -csv -column\nSupported dot commands: .tables, .schema.",
        );
        return;
    }
    const options = {
        readOnly: false,
        headers: false,
        separator: "|",
        nullValue: "",
        mode: "list",
    };
    while (args[0]?.startsWith("-")) {
        const option = args.shift();
        switch (option) {
            case "-readonly":
                options.readOnly = true;
                break;
            case "-header":
                options.headers = true;
                break;
            case "-noheader":
                options.headers = false;
                break;
            case "-csv":
                options.mode = "csv";
                break;
            case "-column":
                options.mode = "column";
                break;
            case "-separator":
            case "-nullvalue": {
                const value = args.shift();
                if (value === undefined) throw new Error(`missing value for ${option}`);
                if (option === "-separator") options.separator = value;
                else options.nullValue = value;
                break;
            }
            default:
                throw new Error(`unsupported option '${option}'`);
        }
    }
    if (args.length !== 2 || args[0].startsWith("-") || args[0] === "") {
        throw new Error("expected DATABASE and one SQL statement or supported dot command");
    }
    const [path, input] = args;
    const database = new SqliteConnection(path, { readOnly: options.readOnly });
    if (input.trim().startsWith(".")) {
        runDotCommand(database, input.trim());
        return;
    }
    const result = database.execute(input);
    printRows(result, options);
}

try {
    main(process.argv.slice(2));
} catch (error) {
    console.error(`@yyds/sqlite: ${error.message}`);
    process.exitCode = 1;
}
