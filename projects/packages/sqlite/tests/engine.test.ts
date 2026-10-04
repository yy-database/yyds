import assert from "node:assert/strict";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { isYydsNativeInstalled } from "@yyds/yyds/node";
import { SqliteConnection } from "../src/node.ts";

test("native SQLite engine preserves storage classes and signed 64-bit values", {
    skip: !isYydsNativeInstalled(),
}, () => {
    const database = new SqliteConnection(":memory:");
    database.execute(
        "CREATE TABLE sample (id INTEGER, real_value REAL, text_value TEXT, blob_value BLOB)",
    );
    database.execute("INSERT INTO sample VALUES (-9223372036854775808, 1.25, 'hello', X'00ff')");
    const result = database.execute(
        "SELECT id, real_value, text_value, blob_value, NULL, CAST(X'ff' AS TEXT) FROM sample",
    );
    assert.deepEqual(result.columns, [
        "id",
        "real_value",
        "text_value",
        "blob_value",
        "NULL",
        "CAST(X'ff' AS TEXT)",
    ]);
    const [integer, real, text, blob, nullValue, rawText] = result.rows[0];
    assert.equal(integer.kind, "integer");
    if (integer.kind === "integer") assert.equal(integer.value, -(1n << 63n));
    assert.equal(real.kind, "real");
    if (real.kind === "real") assert.equal(real.value, 1.25);
    assert.equal(text.kind, "text");
    if (text.kind === "text") assert.equal(text.value, "hello");
    assert.equal(blob.kind, "blob");
    if (blob.kind === "blob") assert.deepEqual(blob.value, Buffer.from([0, 255]));
    assert.deepEqual(nullValue, { kind: "null" });
    assert.equal(rawText.kind, "text");
    if (rawText.kind === "text") assert.deepEqual(rawText.value, Buffer.from([255]));
    assert.equal(result.lastInsertRowid, 1n);
    assert.throws(() => database.execute("SELECT FROM"), /syntax error/i);
    assert.throws(() => database.execute("SELECT 1; SELECT 2"), /multiple statements/i);
});

test("native SQLite engine binds parameters without changing their storage classes", {
    skip: !isYydsNativeInstalled(),
}, () => {
    const database = new SqliteConnection(":memory:");
    database.execute("CREATE TABLE sample (low, high, text_value, blob_value, null_value)");
    const injection = "'); DROP TABLE sample; --";
    const inserted = database.executeWithParameters(
        "INSERT INTO sample VALUES (?, ?, ?, ?, ?)",
        [
            { kind: "integer", value: -(1n << 63n) },
            { kind: "integer", value: (1n << 63n) - 1n },
            { kind: "text", value: injection },
            { kind: "blob", value: Buffer.from([0, 255]) },
            { kind: "null" },
        ],
    );
    assert.equal(inserted.changes, 1n);
    const result = database.executeWithParameters(
        "SELECT low, high, text_value, blob_value, null_value FROM sample WHERE text_value = ?",
        [{ kind: "text", value: injection }],
    );
    assert.deepEqual(result.rows[0], [
        { kind: "integer", value: -(1n << 63n) },
        { kind: "integer", value: (1n << 63n) - 1n },
        { kind: "text", value: injection },
        { kind: "blob", value: Buffer.from([0, 255]) },
        { kind: "null" },
    ]);
    assert.throws(
        () => database.executeWithParameters("INSERT INTO sample VALUES (?, ?, ?, ?, ?)", []),
        /parameter/i,
    );
    assert.deepEqual(database.execute("SELECT count(*) FROM sample").rows[0][0], { kind: "integer", value: 1n });
    assert.deepEqual(database.executeWithParameters("SELECT ?2, ?1, ?2", [
        { kind: "real", value: 1.25 },
        { kind: "text", value: Buffer.from([255, 0, 128]) },
    ]).rows[0], [
        { kind: "text", value: Buffer.from([255, 0, 128]) },
        { kind: "real", value: 1.25 },
        { kind: "text", value: Buffer.from([255, 0, 128]) },
    ]);
    assert.throws(() => database.executeWithParameters("SELECT ?", [
        { kind: "integer", value: 1n << 63n },
    ]), /64-bit range/i);
    assert.throws(() => database.executeWithParameters("SELECT ?", [
        { kind: "null" }, { kind: "null" },
    ]), /parameter/i);
});

test("native SQLite engine executes batches with SQLite transaction semantics", {
    skip: !isYydsNativeInstalled(),
}, () => {
    const database = new SqliteConnection(":memory:");
    database.executeBatch(`
        CREATE TABLE sample (value INTEGER);
        INSERT INTO sample VALUES (1);
        INSERT INTO sample VALUES (2);
    `);
    assert.deepEqual(database.execute("SELECT value FROM sample ORDER BY value").rows, [
        [{ kind: "integer", value: 1n }],
        [{ kind: "integer", value: 2n }],
    ]);
    database.executeBatch("BEGIN; INSERT INTO sample VALUES (3); ROLLBACK;");
    assert.deepEqual(database.execute("SELECT value FROM sample ORDER BY value").rows, [
        [{ kind: "integer", value: 1n }],
        [{ kind: "integer", value: 2n }],
    ]);
    assert.throws(() => database.executeBatch("INSERT INTO sample VALUES (4); INVALID SQL;"));
    assert.deepEqual(database.execute("SELECT count(*) FROM sample").rows[0][0], { kind: "integer", value: 3n });
    assert.deepEqual(database.execute("SELECT max(value) FROM sample").rows[0][0], { kind: "integer", value: 4n });
});

test("native SQLite engine reads and writes files through SQLite's pager", {
    skip: !isYydsNativeInstalled(),
}, () => {
    const path = join(mkdtempSync(join(tmpdir(), "yyds-sqlite-engine-")), "live.sqlite");
    const database = new SqliteConnection(path);
    database.execute("PRAGMA journal_mode=WAL");
    database.execute("CREATE TABLE sample (value TEXT)");
    database.execute("INSERT INTO sample VALUES ('persisted')");
    const secondConnection = new SqliteConnection(path);
    const value = secondConnection.execute("SELECT value FROM sample").rows[0][0];
    assert.equal(value.kind, "text");
    if (value.kind === "text") assert.equal(value.value, "persisted");
});
