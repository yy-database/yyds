import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync } from "node:fs";
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

test("SQLite CLI reports the bundled engine and rejects incomplete arguments", {
    skip: !isYydsNativeInstalled(),
}, () => {
    const version = run("--version");
    assert.equal(version.status, 0, version.stderr);
    assert.match(version.stdout, /^sqlite3 \d+\.\d+\.\d+ \(YYDS bundled SQLite\)/);
    assert.equal(run(":memory:").status, 1);
    assert.equal(run(":memory:", "SELECT FROM").status, 1);
    assert.equal(run(":memory:", ".not-supported").status, 1);
    assert.equal(run("--unknown", "SELECT 1").status, 1);
});

test("SQLite CLI executes SQL, dot commands, and writes standard SQLite files", {
    skip: !isYydsNativeInstalled() || !process.env.YYDS_SQLITE_REFERENCE_PYTHON,
}, () => {
    const directory = mkdtempSync(join(tmpdir(), "yyds-sqlite-cli-engine-"));
    const path = join(directory, "real.sqlite");
    assert.equal(
        run(path, "CREATE TABLE samples (id INTEGER PRIMARY KEY, value TEXT, payload BLOB)").status,
        0,
    );
    assert.equal(run(path, "INSERT INTO samples VALUES (1, 'hello', X'00ff')").status, 0);
    const query = run(path, "SELECT id, value, payload, NULL FROM samples");
    assert.equal(query.status, 0, query.stderr);
    assert.equal(query.stdout, "1|hello|X'00ff'|\n");
    assert.equal(run(path, ".tables").stdout, "samples\n");
    assert.equal(
        run(path, ".schema").stdout,
        "CREATE TABLE samples (id INTEGER PRIMARY KEY, value TEXT, payload BLOB);\n",
    );
    assert.equal(readFileSync(path).subarray(0, 16).toString("binary"), "SQLite format 3\0");

    const check = spawnSync(
        process.env.YYDS_SQLITE_REFERENCE_PYTHON!,
        [
            "-c",
            `
import sqlite3, sys
with sqlite3.connect(sys.argv[1]) as db:
    assert db.execute('select id, value, payload from samples').fetchone() == (1, 'hello', b'\\x00\\xff')
    assert db.execute('pragma integrity_check').fetchone() == ('ok',)
`,
            path,
        ],
        { timeout: 10_000, encoding: "utf8" },
    );
    assert.ifError(check.error);
    assert.equal(check.status, 0, check.stderr);
    assert.equal(existsSync(`${path}-wal`), false);
});
