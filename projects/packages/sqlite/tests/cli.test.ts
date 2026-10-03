import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, truncateSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { isYydsNativeInstalled } from "@yyds/yyds/node";

const cli = fileURLToPath(new URL("../bin/sqlite3.mjs", import.meta.url));

function run(...args: string[]) {
    const output = spawnSync(process.execPath, ["--experimental-strip-types", cli, ...args], {
        timeout: 10_000,
        encoding: "utf8",
    });
    assert.ifError(output.error);
    return output;
}

test("SQLite CLI rejects fabricated SQL, options, and missing commands", () => {
    const directory = mkdtempSync(join(tmpdir(), "yyds-sqlite-cli-errors-"));
    const missing = join(directory, "missing.sqlite");
    for (const args of [
        [missing, "SELECT 1"],
        [missing, "SELECT sqlite_version()"],
        [missing],
        [":memory:", ".tables"],
        ["-readonly", missing, ".tables"],
    ]) {
        const output = run(...args);
        assert.equal(output.status, 1);
        assert.equal(output.stdout, "");
    }
    assert.equal(run(missing, ".tables").status, 1);
    assert.equal(existsSync(missing), false);
    const banner = run("--version");
    assert.equal(banner.status, 0);
    assert.match(banner.stdout, /not a SQLite SQL engine/);
});

test("SQLite CLI rejects over-budget input and recovery sidecars before native loading", () => {
    const directory = mkdtempSync(join(tmpdir(), "yyds-sqlite-cli-budget-"));
    const path = join(directory, "oversized.sqlite");
    writeFileSync(path, "");
    truncateSync(path, 256 * 1024 * 1024 + 1);
    assert.match(run(path, ".schema").stderr, /byte limit/);
    for (const suffix of ["-wal", "-journal"]) {
        const database = join(directory, `sidecar${suffix}.sqlite`);
        writeFileSync(database, "");
        writeFileSync(`${database}${suffix}`, "pending recovery");
        const output = run(database, ".schema");
        assert.equal(output.status, 1);
        assert.match(output.stderr, /recovery is not implemented/);
    }
    assert.equal(run(directory, ".schema").status, 1);
});

test("SQLite CLI reads real tables and schema without changing the database", {
    skip: !isYydsNativeInstalled() || !process.env.YYDS_SQLITE_REFERENCE_PYTHON,
}, () => {
    const directory = mkdtempSync(join(tmpdir(), "yyds-sqlite-cli-reference-"));
    const path = join(directory, "reference.sqlite");
    const oracle = spawnSync(
        process.env.YYDS_SQLITE_REFERENCE_PYTHON!,
        [
            "-c",
            `
import json, sqlite3, sys
with sqlite3.connect(sys.argv[1]) as db:
    db.executescript('CREATE TABLE samples (value TEXT UNIQUE); CREATE VIEW sample_view AS SELECT value FROM samples; CREATE TRIGGER sample_trigger AFTER INSERT ON samples BEGIN SELECT 1; END;')
    print(json.dumps([row[0] for row in db.execute("SELECT sql FROM sqlite_schema WHERE sql IS NOT NULL AND name NOT LIKE 'sqlite_%' ORDER BY rowid")]))
`,
            path,
        ],
        { timeout: 15_000, encoding: "utf8" },
    );
    assert.ifError(oracle.error);
    assert.equal(oracle.status, 0, oracle.stderr);
    const before = readFileSync(path);
    const tables = run(path, ".tables");
    assert.equal(tables.status, 0, tables.stderr);
    assert.equal(tables.stdout, "sample_view\nsamples\n");
    const schema = run(path, ".schema");
    assert.equal(schema.status, 0, schema.stderr);
    const expected =
        (JSON.parse(oracle.stdout) as string[]).map((sql) => `${sql};`).join("\n") + "\n";
    assert.equal(schema.stdout, expected);
    assert.deepEqual(readFileSync(path), before);
    assert.equal(run(path, "SELECT 1").status, 1);
    assert.deepEqual(readFileSync(path), before);
});
