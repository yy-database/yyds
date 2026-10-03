#!/usr/bin/env node
import { closeSync, fstatSync, openSync, readSync, statSync } from "node:fs";

const maximumBytes = 256 * 1024 * 1024;

function rejectRecoveryFiles(path) {
    for (const suffix of ["-wal", "-journal"]) {
        try {
            if (statSync(`${path}${suffix}`).size > 0) {
                throw new Error(
                    "journal/WAL recovery is not implemented, use a quiescent checkpointed file",
                );
            }
        } catch (error) {
            if (error.code !== "ENOENT") throw error;
        }
    }
}

function readSnapshot(path) {
    rejectRecoveryFiles(path);
    const descriptor = openSync(path, "r");
    try {
        const before = fstatSync(descriptor, { bigint: true });
        if (!before.isFile()) throw new Error("snapshot input must be a regular file");
        if (before.size > BigInt(maximumBytes))
            throw new Error("snapshot exceeds the 256 MiB byte limit");
        const bytes = Buffer.alloc(Number(before.size));
        let offset = 0;
        while (offset < bytes.length) {
            const count = readSync(descriptor, bytes, offset, bytes.length - offset, offset);
            if (count === 0) throw new Error("snapshot changed while reading");
            offset += count;
        }
        const extra = Buffer.alloc(1);
        const grew = readSync(descriptor, extra, 0, 1, offset) !== 0;
        const after = fstatSync(descriptor, { bigint: true });
        if (
            grew ||
            before.size !== after.size ||
            before.mtimeNs !== after.mtimeNs ||
            before.ctimeNs !== after.ctimeNs
        ) {
            throw new Error("snapshot changed while reading");
        }
        rejectRecoveryFiles(path);
        return bytes;
    } finally {
        closeSync(descriptor);
    }
}

async function main(args) {
    if (args.length === 1 && ["-version", "--version"].includes(args[0])) {
        console.log("YYDS SQLite snapshot tool (not a SQLite SQL engine)");
        return;
    }
    if (args.length === 1 && ["-help", "--help"].includes(args[0])) {
        console.log(
            "Usage: sqlite3 FILE '.tables' | '.schema'\nRead-only quiescent main-file snapshots only. SQL and interactive mode are not implemented.",
        );
        return;
    }
    if (args.length !== 2 || args[0].startsWith("-") || args[0] === ":memory:") {
        throw new Error(
            "expected an existing file and .tables or .schema, interactive mode and options are not implemented",
        );
    }
    const command = args[1].trim();
    if (command !== ".tables" && command !== ".schema") {
        throw new Error("only .tables and .schema are implemented, SQL execution is not supported");
    }
    const bytes = readSnapshot(args[0]);
    const { SqliteSnapshot } = await import("../src/node.ts");
    const objects = new SqliteSnapshot(bytes, maximumBytes).schema();
    if (command === ".tables") {
        const names = objects
            .filter(
                (object) =>
                    ["table", "view"].includes(object.kind) && !object.name.startsWith("sqlite_"),
            )
            .map((object) => object.name)
            .sort((left, right) => Buffer.compare(Buffer.from(left), Buffer.from(right)));
        for (const name of names) console.log(name);
    } else {
        for (const object of objects) {
            if (object.sql === undefined || object.name.startsWith("sqlite_")) continue;
            const source = object.sql.trimEnd();
            console.log(source.endsWith(";") ? source : `${source};`);
        }
    }
}

main(process.argv.slice(2)).catch((error) => {
    console.error(`@yyds/sqlite: ${error.message}`);
    process.exitCode = 1;
});
