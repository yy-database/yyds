import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode, type YydsNode } from "@yydb/yyds-node-harness";
import mysql from "mysql2/promise";

const ROWS = 1000;

async function connect(node: YydsNode) {
    return mysql.createConnection({ host: node.mysql.host, port: node.mysql.port, user: "yyds", database: "yyds" });
}

const DDL = (table: string) =>
    `CREATE TABLE ${table} (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(16) NOT NULL, score DOUBLE NULL)`;

function literalRows(count: number) {
    return Array.from({ length: count }, (_, index) => `('n${index}', ${index})`).join(",");
}

test("mysql: 1000-row bulk insert, per-row read-back and ORDER BY boundaries", { timeout: 120_000 }, async (t) => {
    const node = await startYydsNode();
    const connection = await connect(node);
    t.after(async () => {
        await connection.end().catch(() => undefined);
        await node.stop();
    });
    const table = "scenario_bulk";

    await connection.query(`DROP TABLE IF EXISTS ${table}`);
    await connection.query(DDL(table));
    const [inserted] = await connection.query(`INSERT INTO ${table} (name, score) VALUES ${literalRows(ROWS)}`);
    assert.equal((inserted as { affectedRows: number }).affectedRows, ROWS);

    const [rows] = await connection.query(`SELECT id, name, score FROM ${table} ORDER BY id`);
    const stored = rows as Array<{ id: number; name: string; score: number }>;
    assert.equal(stored.length, ROWS);
    for (const [index, row] of stored.entries()) {
        assert.deepEqual({ id: row.id, name: row.name, score: Number(row.score) }, { id: index + 1, name: `n${index}`, score: index });
    }

    const checks: Array<[string, number[]]> = [
        [`SELECT id FROM ${table} ORDER BY id LIMIT 5 OFFSET 0`, [1, 2, 3, 4, 5]],
        [`SELECT id FROM ${table} ORDER BY id LIMIT 5 OFFSET 995`, [996, 997, 998, 999, 1000]],
        [`SELECT id FROM ${table} ORDER BY id LIMIT 5 OFFSET 1000`, []],
        [`SELECT id FROM ${table} ORDER BY score DESC, id DESC LIMIT 3`, [1000, 999, 998]],
    ];
    for (const [query, expected] of checks) {
        const [result] = await connection.query(query);
        assert.deepEqual((result as Array<{ id: number }>).map((row) => row.id), expected, query);
    }

    const [aggregate] = await connection.query(
        `SELECT COUNT(*) AS n, MIN(id) AS lo, MAX(id) AS hi, SUM(score) AS total FROM ${table}`,
    );
    assert.deepEqual(aggregate, [{ n: 1000, lo: 1, hi: 1000, total: 499500 }]);
});

test("mysql: prepared loop stores 1000 rows without an AUTO_INCREMENT gap", { timeout: 120_000 }, async (t) => {
    const node = await startYydsNode();
    const connection = await connect(node);
    t.after(async () => {
        await connection.end().catch(() => undefined);
        await node.stop();
    });
    const table = "scenario_prepared";

    await connection.query(`DROP TABLE IF EXISTS ${table}`);
    await connection.query(DDL(table));
    // `execute` uses the binary prepared-statement protocol, so the omitted
    // `AUTO_INCREMENT` column is allocated at execute time on every call.
    for (let index = 0; index < ROWS; index += 1) {
        await connection.execute(`INSERT INTO ${table} (name, score) VALUES (?, ?)`, [`n${index}`, index]);
    }
    const [rows] = await connection.query(`SELECT id, name, score FROM ${table} ORDER BY id`);
    const stored = rows as Array<{ id: number }>;
    assert.equal(stored.length, ROWS);
    assert.deepEqual(stored.map((row) => row.id), Array.from({ length: ROWS }, (_, index) => index + 1), "prepared inserts left an id gap");
});

test("mysql: AUTO_INCREMENT continues across connections and a node restart", { timeout: 120_000 }, async (t) => {
    let node = await startYydsNode();
    t.after(async () => {
        await node.stop({ wipe: false }).catch(() => undefined);
    });
    const table = "scenario_serial";

    const setup = await connect(node);
    try {
        await setup.query(`DROP TABLE IF EXISTS ${table}`);
        await setup.query(DDL(table));
        for (let index = 0; index < 100; index += 1) {
            await setup.execute(`INSERT INTO ${table} (name, score) VALUES (?, ?)`, [`a${index}`, index]);
        }
    } finally {
        await setup.end();
    }

    // A second connection must not repeat values allocated by the first one.
    const second = await connect(node);
    try {
        for (let index = 0; index < 100; index += 1) {
            await second.execute(`INSERT INTO ${table} (name, score) VALUES (?, ?)`, [`b${index}`, 100 + index]);
        }
    } finally {
        await second.end();
    }

    const before = await connect(node);
    const [ids] = await before.query(`SELECT id FROM ${table} ORDER BY id`);
    await before.end();
    assert.deepEqual((ids as Array<{ id: number }>).map((row) => row.id), Array.from({ length: 200 }, (_, index) => index + 1));

    await node.stop({ wipe: false });
    node = await startYydsNode({ dataDir: node.dataDir, clusterId: node.clusterId, nodeId: node.nodeId });

    const after = await connect(node);
    try {
        const [result] = await after.execute(`INSERT INTO ${table} (name, score) VALUES (?, ?)`, ["after", 200]);
        assert.equal((result as { insertId: number }).insertId, 201);
        const [count] = await after.query(`SELECT COUNT(*) AS n FROM ${table}`);
        assert.deepEqual(count, [{ n: 201 }]);
    } finally {
        await after.end();
    }
});

test("mysql: fifty transactions keep only the committed half", { timeout: 120_000 }, async (t) => {
    const node = await startYydsNode();
    const connection = await connect(node);
    t.after(async () => {
        await connection.end().catch(() => undefined);
        await node.stop();
    });
    const table = "scenario_transactions";

    await connection.query(`DROP TABLE IF EXISTS ${table}`);
    await connection.query(DDL(table));
    for (let batch = 0; batch < 50; batch += 1) {
        await connection.query("START TRANSACTION");
        const values = Array.from({ length: 20 }, (_, row) => `('t${batch}-${row}', ${batch * 20 + row})`).join(",");
        await connection.query(`INSERT INTO ${table} (name, score) VALUES ${values}`);
        if (batch % 2 === 0) await connection.query("COMMIT");
        else await connection.query("ROLLBACK");
    }
    const [aggregate] = await connection.query(`SELECT COUNT(*) AS n FROM ${table}`);
    assert.deepEqual(aggregate, [{ n: 500 }]);
    const [rows] = await connection.query(`SELECT id, name FROM ${table} ORDER BY id`);
    const stored = rows as Array<{ name: string }>;
    assert.equal(stored.length, 500);
    for (const row of stored) {
        const batch = Number(row.name.split("-")[0].slice(1));
        assert.equal(batch % 2, 0, `rolled-back row survived: ${row.name}`);
    }
});

test("mysql: information_schema projections stay driver-visible", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const connection = await connect(node);
    t.after(async () => {
        await connection.end().catch(() => undefined);
        await node.stop();
    });
    const table = "scenario_metadata";

    await connection.query(`DROP TABLE IF EXISTS ${table}`);
    await connection.query(DDL(table));

    const [tables] = await connection.query(
        `SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_TYPE = 'BASE TABLE' ORDER BY TABLE_NAME`,
    );
    assert.deepEqual((tables as Array<{ TABLE_NAME: string }>).map((row) => row.TABLE_NAME), [table]);

    const [columns] = await connection.execute(
        `SELECT COLUMN_NAME FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ? ORDER BY ORDINAL_POSITION`,
        [table],
    );
    assert.deepEqual((columns as Array<{ COLUMN_NAME: string }>).map((row) => row.COLUMN_NAME), ["id", "name", "score"]);

    // MySQL answers a `LIKE` predicate on metadata views, so the projection
    // filters on the same pattern a real client would send.
    const [filtered] = await connection.query(
        `SELECT TABLE_NAME FROM information_schema.TABLES WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE 'scenario_%'`,
    );
    assert.deepEqual((filtered as Array<{ TABLE_NAME: string }>).map((row) => row.TABLE_NAME), [table]);
});

test("mysql: ALTER TABLE ADD COLUMN backfills the declared default", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const connection = await connect(node);
    t.after(async () => {
        await connection.end().catch(() => undefined);
        await node.stop();
    });
    const table = "scenario_alter";

    await connection.query(`DROP TABLE IF EXISTS ${table}`);
    await connection.query(DDL(table));
    await connection.query(`INSERT INTO ${table} (name, score) VALUES ${literalRows(ROWS)}`);
    await connection.query(`ALTER TABLE ${table} ADD COLUMN note VARCHAR(8) DEFAULT 'n'`);

    // MySQL backfills the declared default for rows written before the column
    // existed, so every stored row reads back the same value.
    const [rows] = await connection.query(`SELECT id, note FROM ${table} ORDER BY id LIMIT 10`);
    assert.deepEqual((rows as Array<{ note: string | null }>).map((row) => row.note), Array.from({ length: 10 }, () => "n"));
});
