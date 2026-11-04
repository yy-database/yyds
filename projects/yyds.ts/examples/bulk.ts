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
            // 显式主键：示例只验收批写批读，不绑定网关的序列分配路径。
            await client.query(`INSERT INTO ${PG_TABLE} (id, email, note) VALUES ($1, $2, $3)`, [
                index + 1,
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

let failed = 0;
for (const [name, body] of [["pg", bulkPg], ["mysql", bulkMysql]]) {
    try {
        await body();
        console.log(`PASS examples/bulk.ts ${name} ${ROWS} rows`);
    } catch (error) {
        failed += 1;
        console.log(`FAIL examples/bulk.ts ${name}: ${error?.message ?? error}`);
    }
}
process.exit(failed === 0 ? 0 : 1);
