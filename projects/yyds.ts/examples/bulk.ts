// 下游验收示例：真实 mysql2 / pg 驱动对 yyds 节点做 1000 行批写与重连后批读。
//
// 期望值由本示例按写入输入自行推导（行数、连续 id 区间、去重键数、抽样行），
// 不引用 yyds 内部常量，也不复用仓内用例表。驱动复用 `e2e` 已安装的依赖。
//
// 节点由 `node e2e/run.mjs` 启动并通过 YYDS_PG_PORT / YYDS_MYSQL_PORT 传入。

import { createRequire } from "node:module";
import { join, resolve } from "node:path";

const ROOT = resolve(import.meta.dirname, "../../..");
const require = createRequire(join(ROOT, "e2e/package.json"));
const mysql = require("mysql2/promise");
const { Client } = require("pg");

const ROWS = 1000;
const CHUNK = 100;
const host = process.env.YYDS_HOST ?? "127.0.0.1";
const pgPort = Number(process.env.YYDS_PG_PORT);
const mysqlPort = Number(process.env.YYDS_MYSQL_PORT);
if (!Number.isInteger(pgPort) || !Number.isInteger(mysqlPort)) {
    console.error("YYDS_PG_PORT and YYDS_MYSQL_PORT must point at a running yyds node");
    process.exit(2);
}

const PG_TABLE = "example_bulk_pg";
const MYSQL_TABLE = "example_bulk_mysql";
const emails = Array.from({ length: ROWS }, (_, index) => `row-${index + 1}@example.test`);
const notes = Array.from({ length: ROWS }, (_, index) => `note-${index + 1}`);

function check(condition, message) {
    if (!condition) throw new Error(message);
}

async function pgConnect() {
    const client = new Client({ host, port: pgPort, user: "yyds", password: "", database: "yyds" });
    await client.connect();
    return client;
}

async function mysqlConnect() {
    return mysql.createConnection({ host, port: mysqlPort, user: "yyds", password: "", charset: "utf8mb4" });
}

async function bulkPg() {
    const client = await pgConnect();
    try {
        await client.query("DROP TABLE IF EXISTS " + PG_TABLE);
        await client.query(
            `CREATE TABLE ${PG_TABLE} (id BIGSERIAL PRIMARY KEY, email TEXT NOT NULL, note TEXT, UNIQUE (email))`,
        );
        for (let index = 0; index < ROWS; index += 1) {
            await client.query(`INSERT INTO ${PG_TABLE} (email, note) VALUES ($1, $2)`, [
                emails[index],
                notes[index],
            ]);
        }
        const [row] = (await client.query(`SELECT count(*) AS rows, min(id) AS low, max(id) AS high FROM ${PG_TABLE}`)).rows;
        check(Number(row.rows) === ROWS, `postgres stored ${row.rows} rows, expected ${ROWS}`);
        check(Number(row.high) - Number(row.low) + 1 === ROWS, `postgres id range ${row.low}..${row.high} is not contiguous`);
        const distinct = (await client.query(`SELECT count(DISTINCT email) AS total FROM ${PG_TABLE}`)).rows[0].total;
        check(Number(distinct) === ROWS, `postgres counted ${distinct} distinct emails, expected ${ROWS}`);
        const spot = (await client.query(`SELECT note FROM ${PG_TABLE} WHERE email = $1`, ["row-500@example.test"])).rows;
        check(spot.length === 1 && spot[0].note === "note-500", `postgres spot row was ${JSON.stringify(spot)}`);
    } finally {
        await client.end();
    }

    // 新连接重新读，行必须还在。
    const reopened = await pgConnect();
    try {
        const total = (await reopened.query(`SELECT count(*) AS rows FROM ${PG_TABLE}`)).rows[0].rows;
        check(Number(total) === ROWS, `postgres read ${total} rows after reconnect, expected ${ROWS}`);
    } finally {
        await reopened.end();
    }
}

async function bulkMysql() {
    const connection = await mysqlConnect();
    try {
        await connection.query(`DROP TABLE IF EXISTS ${MYSQL_TABLE}`);
        await connection.query(
            `CREATE TABLE ${MYSQL_TABLE} (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, `
                + "email VARCHAR(191) NOT NULL, note TEXT, UNIQUE KEY uq_example_email (email))",
        );
        for (let start = 0; start < ROWS; start += CHUNK) {
            const end = Math.min(start + CHUNK, ROWS);
            const values = [];
            for (let index = start; index < end; index += 1) values.push([emails[index], notes[index]]);
            await connection.query(`INSERT INTO ${MYSQL_TABLE} (email, note) VALUES ?`, [values]);
        }
        const [rows] = await connection.query(
            `SELECT count(*) AS rows, min(id) AS low, max(id) AS high FROM ${MYSQL_TABLE}`,
        );
        check(Number(rows[0].rows) === ROWS, `mysql stored ${rows[0].rows} rows, expected ${ROWS}`);
        check(Number(rows[0].high) - Number(rows[0].low) + 1 === ROWS, `mysql id range is not contiguous`);
        const [distinct] = await connection.query(`SELECT count(DISTINCT email) AS total FROM ${MYSQL_TABLE}`);
        check(Number(distinct[0].total) === ROWS, `mysql counted ${distinct[0].total} distinct emails`);
        const [spot] = await connection.query(`SELECT note FROM ${MYSQL_TABLE} WHERE email = ?`, ["row-500@example.test"]);
        check(spot.length === 1 && spot[0].note === "note-500", `mysql spot row was ${JSON.stringify(spot)}`);
    } finally {
        await connection.end();
    }

    const reopened = await mysqlConnect();
    try {
        const [rows] = await reopened.query(`SELECT count(*) AS rows FROM ${MYSQL_TABLE}`);
        check(Number(rows[0].rows) === ROWS, `mysql read ${rows[0].rows} rows after reconnect, expected ${ROWS}`);
    } finally {
        await reopened.end();
    }
}

const PG_ERR_TABLE = "example_err_pg";
const MYSQL_ERR_TABLE = "example_err_mysql";

// 结构化错误只断言驱动侧码值（PostgreSQL SQLSTATE / MySQL errno + SQLSTATE），
// 不看错误文案，也不看网关内部枚举。
async function pgExpect(client, label, sql, expected) {
    try {
        await client.query(sql);
    } catch (error) {
        check(
            error.code === expected.code,
            `${label}: expected SQLSTATE ${expected.code}, got ${error.code} (${error.message})`,
        );
        return;
    }
    throw new Error(`${label}: expected SQLSTATE ${expected.code}, no error raised`);
}

async function mysqlExpect(connection, label, sql, expected) {
    try {
        await connection.query(sql);
    } catch (error) {
        check(error.errno === expected.errno, `${label}: expected errno ${expected.errno}, got ${error.errno}`);
        check(
            error.sqlState === expected.sqlState,
            `${label}: expected SQLSTATE ${expected.sqlState}, got ${error.sqlState}`,
        );
        return;
    }
    throw new Error(`${label}: expected errno ${expected.errno}, no error raised`);
}

async function errorsPg() {
    const client = await pgConnect();
    try {
        await client.query(`DROP TABLE IF EXISTS ${PG_ERR_TABLE}`);
        await client.query(
            `CREATE TABLE ${PG_ERR_TABLE} (id BIGSERIAL PRIMARY KEY, email TEXT NOT NULL, `
                + "code VARCHAR(8), note TEXT, UNIQUE (email))",
        );
        await client.query(`INSERT INTO ${PG_ERR_TABLE} (id, email, code, note) VALUES (1, 'dup@example.test', 'ok', 'n1')`);
        await client.query(`INSERT INTO ${PG_ERR_TABLE} (id, email, code, note) VALUES (2, 'two@example.test', 'ok', 'n2')`);

        await pgExpect(client, "duplicate-key", `INSERT INTO ${PG_ERR_TABLE} (id, email, code, note) VALUES (3, 'dup@example.test', 'ok', 'n3')`, { code: "23505" });
        await pgExpect(client, "value-too-long", `INSERT INTO ${PG_ERR_TABLE} (id, email, code, note) VALUES (4, 'long@example.test', '0123456789abcdef', 'n4')`, { code: "22001" });
        await pgExpect(client, "invalid-grouping", `SELECT email, note FROM ${PG_ERR_TABLE} GROUP BY email`, { code: "42803" });
        await pgExpect(client, "scalar-subquery-multi-row", `SELECT (SELECT email FROM ${PG_ERR_TABLE}) AS x`, { code: "21000" });
        await pgExpect(client, "in-subquery-multi-column", `SELECT id FROM ${PG_ERR_TABLE} WHERE id IN (SELECT id, email FROM ${PG_ERR_TABLE})`, { code: "42601" });

        // 对外可观测的拒绝语义：客户端能按 SQLSTATE 区分「未实现」与「写错了」。
        await pgExpect(client, "unsupported-temporary-table", `CREATE TEMPORARY TABLE ${PG_ERR_TABLE}_tmp (id BIGINT)`, { code: "0A000" });
        await pgExpect(client, "unsupported-distinct-on", `SELECT DISTINCT ON (email) id FROM ${PG_ERR_TABLE} ORDER BY email`, { code: "0A000" });
        await pgExpect(client, "set-operation-column-count", `SELECT id FROM ${PG_ERR_TABLE} UNION SELECT id, email FROM ${PG_ERR_TABLE}`, { code: "42601" });
        await pgExpect(client, "cross-database-reference", `SELECT id FROM other.public.${PG_ERR_TABLE}`, { code: "42P01" });
    } finally {
        await client.end();
    }
}

async function errorsMysql() {
    const connection = await mysqlConnect();
    try {
        await connection.query(`DROP TABLE IF EXISTS ${MYSQL_ERR_TABLE}`);
        await connection.query(
            `CREATE TABLE ${MYSQL_ERR_TABLE} (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, `
                + "email VARCHAR(191) NOT NULL, code VARCHAR(8), note TEXT, UNIQUE KEY uq_error_email (email))",
        );
        await connection.query(`INSERT INTO ${MYSQL_ERR_TABLE} (id, email, code, note) VALUES (1, 'dup@example.test', 'ok', 'n1')`);
        await connection.query(`INSERT INTO ${MYSQL_ERR_TABLE} (id, email, code, note) VALUES (2, 'two@example.test', 'ok', 'n2')`);

        await mysqlExpect(connection, "duplicate-key", `INSERT INTO ${MYSQL_ERR_TABLE} (id, email, code, note) VALUES (3, 'dup@example.test', 'ok', 'n3')`, { errno: 1062, sqlState: "23000" });
        await mysqlExpect(connection, "value-too-long", `INSERT INTO ${MYSQL_ERR_TABLE} (id, email, code, note) VALUES (4, 'long@example.test', '0123456789abcdef', 'n4')`, { errno: 1406, sqlState: "22001" });
        await mysqlExpect(connection, "invalid-grouping", `SELECT email, note FROM ${MYSQL_ERR_TABLE} GROUP BY email`, { errno: 1055, sqlState: "42000" });
        await mysqlExpect(connection, "scalar-subquery-multi-row", `SELECT (SELECT email FROM ${MYSQL_ERR_TABLE}) AS x`, { errno: 1242, sqlState: "21000" });
        await mysqlExpect(connection, "in-subquery-multi-column", `SELECT id FROM ${MYSQL_ERR_TABLE} WHERE id IN (SELECT id, email FROM ${MYSQL_ERR_TABLE})`, { errno: 1241, sqlState: "21000" });

        await mysqlExpect(connection, "unsupported-temporary-table", `CREATE TEMPORARY TABLE ${MYSQL_ERR_TABLE}_tmp (id BIGINT)`, { errno: 1235, sqlState: "42000" });
        await mysqlExpect(connection, "unsupported-distinct-on", `SELECT DISTINCT ON (email) id FROM ${MYSQL_ERR_TABLE} ORDER BY email`, { errno: 1235, sqlState: "42000" });
        await mysqlExpect(connection, "set-operation-column-count", `SELECT id FROM ${MYSQL_ERR_TABLE} UNION SELECT id, email FROM ${MYSQL_ERR_TABLE}`, { errno: 1064, sqlState: "42000" });
        await mysqlExpect(connection, "cross-database-reference", `SELECT id FROM other.public.${MYSQL_ERR_TABLE}`, { errno: 1146, sqlState: "42S02" });
    } finally {
        await connection.end();
    }
}

let failed = 0;
for (const [name, body] of [["pg", bulkPg], ["mysql", bulkMysql], ["pg-errors", errorsPg], ["mysql-errors", errorsMysql]]) {
    try {
        await body();
        console.log(`PASS examples/bulk.ts ${name} ${ROWS} rows`);
    } catch (error) {
        failed += 1;
        console.log(`FAIL examples/bulk.ts ${name}: ${error?.message ?? error}`);
    }
}
process.exit(failed === 0 ? 0 : 1);
