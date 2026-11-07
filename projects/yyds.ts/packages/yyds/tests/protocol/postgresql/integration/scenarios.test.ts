import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode, type YydsNode } from "@yydb/yyds-node-harness";
import pg from "pg";

// `pg` returns int8 as text by default. The scenarios compare ids numerically,
// so the test process parses int8 itself instead of asserting driver strings.
pg.types.setTypeParser(20, (value) => Number(value));

const ROWS = 1000;

async function connect(node: YydsNode) {
    const client = new pg.Client({ host: node.pgsql.host, port: node.pgsql.port, user: "yyds", database: "yyds" });
    await client.connect();
    return client;
}

const DDL = (table: string) =>
    `CREATE TABLE ${table} (id BIGSERIAL PRIMARY KEY, name VARCHAR(16) NOT NULL, score DOUBLE PRECISION NULL)`;

function literalRows(count: number) {
    return Array.from({ length: count }, (_, index) => `('n${index}', ${index})`).join(",");
}

test("postgresql: 1000-row bulk insert, per-row read-back and ORDER BY boundaries", { timeout: 120_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connect(node);
    t.after(async () => {
        await client.end().catch(() => undefined);
        await node.stop();
    });
    const table = "scenario_bulk";

    await client.query(`DROP TABLE IF EXISTS ${table}`);
    await client.query(DDL(table));
    const inserted = await client.query(`INSERT INTO ${table} (name, score) VALUES ${literalRows(ROWS)}`);
    assert.equal(inserted.rowCount, ROWS);

    // Every row is compared, not only the count: id, name and score must stay
    // paired after the single-statement write.
    const rows = await client.query(`SELECT id, name, score FROM ${table} ORDER BY id`);
    assert.equal(rows.rows.length, ROWS);
    for (const [index, row] of rows.rows.entries()) {
        assert.deepEqual({ id: row.id, name: row.name, score: Number(row.score) }, { id: index + 1, name: `n${index}`, score: index });
    }

    const checks: Array<[string, number[]]> = [
        [`SELECT id FROM ${table} ORDER BY id LIMIT 5 OFFSET 0`, [1, 2, 3, 4, 5]],
        [`SELECT id FROM ${table} ORDER BY id LIMIT 5 OFFSET 995`, [996, 997, 998, 999, 1000]],
        [`SELECT id FROM ${table} ORDER BY id LIMIT 5 OFFSET 1000`, []],
        [`SELECT id FROM ${table} ORDER BY score DESC, id DESC LIMIT 3`, [1000, 999, 998]],
    ];
    for (const [query, expected] of checks) {
        const result = await client.query(query);
        assert.deepEqual(result.rows.map((row) => row.id), expected, query);
    }

    // Aggregate boundaries at the same scale.
    const aggregate = await client.query(`SELECT COUNT(*) AS n, MIN(id) AS lo, MAX(id) AS hi, SUM(score) AS total FROM ${table}`);
    assert.deepEqual(aggregate.rows[0], { n: 1000, lo: 1, hi: 1000, total: 499500 });
});

test("postgresql: parameterised prepared loop stores 1000 rows without an id gap", { timeout: 120_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connect(node);
    t.after(async () => {
        await client.end().catch(() => undefined);
        await node.stop();
    });
    const table = "scenario_prepared";

    await client.query(`DROP TABLE IF EXISTS ${table}`);
    await client.query(DDL(table));
    // Every `query` with `$1`/`$2` goes through Parse/Bind/Describe/Execute. The
    // omitted `BIGSERIAL` column must be filled at execute time instead of being
    // reported as a missing primary key.
    for (let index = 0; index < ROWS; index += 1) {
        await client.query(`INSERT INTO ${table} (name, score) VALUES ($1, $2)`, [`n${index}`, index]);
    }
    const rows = await client.query(`SELECT id, name, score FROM ${table} ORDER BY id`);
    assert.equal(rows.rows.length, ROWS);
    const ids = rows.rows.map((row) => row.id);
    assert.deepEqual(ids, Array.from({ length: ROWS }, (_, index) => index + 1), "prepared inserts left an id gap");
});

test("postgresql: BIGSERIAL continues across connections and a node restart", { timeout: 120_000 }, async (t) => {
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
            await setup.query(`INSERT INTO ${table} (name, score) VALUES ($1, $2)`, [`a${index}`, index]);
        }
    } finally {
        await setup.end();
    }

    // A second connection must not repeat values allocated by the first one.
    const second = await connect(node);
    try {
        for (let index = 0; index < 100; index += 1) {
            await second.query(`INSERT INTO ${table} (name, score) VALUES ($1, $2)`, [`b${index}`, 100 + index]);
        }
    } finally {
        await second.end();
    }

    const before = await connect(node);
    const ids = (await before.query(`SELECT id FROM ${table} ORDER BY id`)).rows.map((row) => row.id);
    await before.end();
    assert.deepEqual(ids, Array.from({ length: 200 }, (_, index) => index + 1), "two connections collided or skipped ids");

    await node.stop({ wipe: false });
    node = await startYydsNode({ dataDir: node.dataDir, clusterId: node.clusterId, nodeId: node.nodeId });

    const after = await connect(node);
    try {
        const next = await after.query(`INSERT INTO ${table} (name, score) VALUES ($1, $2) RETURNING id`, ["after", 200]);
        assert.deepEqual(next.rows, [{ id: 201 }]);
        const count = await after.query(`SELECT COUNT(*) AS n FROM ${table}`);
        assert.equal(count.rows[0]?.n, 201);
    } finally {
        await after.end();
    }
});

test("postgresql: fifty transactions keep only the committed half", { timeout: 120_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connect(node);
    t.after(async () => {
        await client.end().catch(() => undefined);
        await node.stop();
    });
    const table = "scenario_transactions";

    await client.query(`DROP TABLE IF EXISTS ${table}`);
    await client.query(DDL(table));
    for (let batch = 0; batch < 50; batch += 1) {
        await client.query("BEGIN");
        const values = Array.from({ length: 20 }, (_, row) => `('t${batch}-${row}', ${batch * 20 + row})`).join(",");
        await client.query(`INSERT INTO ${table} (name, score) VALUES ${values}`);
        if (batch % 2 === 0) await client.query("COMMIT");
        else await client.query("ROLLBACK");
    }
    const aggregate = await client.query(`SELECT COUNT(*) AS n FROM ${table}`);
    assert.equal(aggregate.rows[0]?.n, 500);
    const rows = await client.query(`SELECT id, name, score FROM ${table} ORDER BY id`);
    assert.equal(rows.rows.length, 500);
    for (const row of rows.rows) {
        const batch = Number(row.name.split("-")[0].slice(1));
        assert.equal(batch % 2, 0, `rolled-back row survived: ${row.name}`);
    }
});

test("postgresql: information_schema projection pages deterministically", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connect(node);
    t.after(async () => {
        await client.end().catch(() => undefined);
        await node.stop();
    });
    const table = "scenario_metadata";

    await client.query(`DROP TABLE IF EXISTS ${table}`);
    await client.query(DDL(table));

    const first = await client.query(
        `SELECT column_name FROM information_schema.columns WHERE table_name = '${table}' ORDER BY ordinal_position LIMIT 2`,
    );
    assert.deepEqual(first.rows.map((row) => row.column_name), ["id", "name"]);

    // PostgreSQL pages metadata with `OFFSET`, so the projection has to honour
    // the same clause the engine does instead of demanding a second pass.
    const second = await client.query(
        `SELECT column_name FROM information_schema.columns WHERE table_name = '${table}' ORDER BY ordinal_position LIMIT 2 OFFSET 2`,
    );
    assert.deepEqual(second.rows.map((row) => row.column_name), ["score"]);

    // PostgreSQL aggregates over a metadata view like any other relation, so the
    // count has to reflect the three declared columns.
    const counted = await client.query(
        `SELECT COUNT(*) AS n FROM information_schema.columns WHERE table_name = '${table}'`,
    );
    assert.equal(Number(counted.rows[0]?.n), 3);
});

test("postgresql: ALTER TABLE ADD COLUMN backfills the declared default", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = await connect(node);
    t.after(async () => {
        await client.end().catch(() => undefined);
        await node.stop();
    });
    const table = "scenario_alter";

    await client.query(`DROP TABLE IF EXISTS ${table}`);
    await client.query(DDL(table));
    await client.query(`INSERT INTO ${table} (name, score) VALUES ${literalRows(ROWS)}`);
    await client.query(`ALTER TABLE ${table} ADD COLUMN note VARCHAR(8) DEFAULT 'n'`);

    // PostgreSQL backfills the declared default for rows written before the
    // column existed, so every stored row reads back the same value.
    const rows = await client.query(`SELECT id, note FROM ${table} ORDER BY id LIMIT 10`);
    assert.deepEqual(rows.rows.map((row) => row.note), Array.from({ length: 10 }, () => "n"));
});
