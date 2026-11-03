// Real PostgreSQL and MySQL drivers against a `yyds start` node.
//
// The node is started in schema-registry mode (no `--catalog-path`), so this
// script owns its own DDL and the catalog is persisted to
// `<data-dir>/sql-schema.v0.json`. Every case opens its own connection unless
// the case explicitly checks commit visibility or reconnect behaviour.
//
// `YYDS_SMOKE_TAG` namespaces the tables so the Python suite can run against
// the same node without colliding with the rows written here.
//
// Run through `node e2e/run.mjs`, or by hand with `YYDS_PG_PORT` and
// `YYDS_MYSQL_PORT` pointing at an already running node.

import mysql from "mysql2/promise";
import { Client } from "pg";

const host = process.env.YYDS_HOST ?? "127.0.0.1";
const pgPort = Number(process.env.YYDS_PG_PORT);
const mysqlPort = Number(process.env.YYDS_MYSQL_PORT);
if (!Number.isInteger(pgPort) || !Number.isInteger(mysqlPort)) {
    console.error("YYDS_PG_PORT and YYDS_MYSQL_PORT must point at a running yyds node");
    process.exit(2);
}

const tag = process.env.YYDS_SMOKE_TAG ?? "ts";
const PG_TABLE = `smoke_pg_users_${tag}`;
const MYSQL_TABLE = `smoke_mysql_users_${tag}`;
let passed = 0;
const failures = [];

function check(condition, message) {
    if (!condition) throw new Error(message);
}

async function caseRun(driver, name, body) {
    try {
        await body();
    } catch (error) {
        failures.push(`${driver}/${name}: ${error?.message ?? error}`);
        console.log(`FAIL ${driver}/${name}: ${error?.message ?? error}`);
        return;
    }
    passed += 1;
    console.log(`PASS ${driver}/${name}`);
}

async function pgConnect() {
    const client = new Client({ host, port: pgPort, user: "yyds", password: "", database: "yyds" });
    await client.connect();
    return client;
}

async function pgQuery(client, sql, parameters) {
    return client.query(sql, parameters);
}

async function mysqlConnect() {
    return mysql.createConnection({ host, port: mysqlPort, user: "yyds", password: "", charset: "utf8mb4" });
}

async function mysqlQuery(connection, sql, parameters) {
    const [rows] = await connection.query(sql, parameters);
    return rows;
}

const pgCases = [
    [
        "connect_and_select",
        async () => {
            const client = await pgConnect();
            try {
                const rows = (await pgQuery(client, "SELECT 1")).rows;
                check(Number(Object.values(rows[0])[0]) === 1, `SELECT 1 returned ${JSON.stringify(rows)}`);
            } finally {
                await client.end();
            }
        },
    ],
    [
        "create_table",
        async () => {
            const client = await pgConnect();
            try {
                await pgQuery(client, `CREATE TABLE ${PG_TABLE} (id BIGSERIAL PRIMARY KEY, email TEXT NOT NULL, note TEXT, UNIQUE (email))`);
            } finally {
                await client.end();
            }
        },
    ],
    [
        "insert_select",
        async () => {
            const client = await pgConnect();
            try {
                await pgQuery(client, `INSERT INTO ${PG_TABLE} (email, note) VALUES ('ada', 'first')`);
                const rows = (await pgQuery(client, `SELECT id, note FROM ${PG_TABLE} WHERE email = 'ada'`)).rows;
                check(rows.length === 1, `SELECT returned ${rows.length} rows, expected 1`);
                check(Number(rows[0].id) > 0, `BIGSERIAL assigned ${rows[0].id}`);
                check(rows[0].note === "first", `note column returned ${rows[0].note}`);
            } finally {
                await client.end();
            }
        },
    ],
    [
        "update",
        async () => {
            const client = await pgConnect();
            try {
                const result = await pgQuery(client, `UPDATE ${PG_TABLE} SET note = 'updated' WHERE email = 'ada'`);
                check(result.rowCount === 1, `UPDATE reported ${result.rowCount} rows`);
                const rows = (await pgQuery(client, `SELECT note FROM ${PG_TABLE} WHERE email = 'ada'`)).rows;
                check(rows[0].note === "updated", `updated note returned ${rows[0].note}`);
            } finally {
                await client.end();
            }
        },
    ],
    [
        "delete",
        async () => {
            const client = await pgConnect();
            try {
                await pgQuery(client, `INSERT INTO ${PG_TABLE} (email, note) VALUES ('delete-me', 'temp')`);
                const result = await pgQuery(client, `DELETE FROM ${PG_TABLE} WHERE email = 'delete-me'`);
                check(result.rowCount === 1, `DELETE reported ${result.rowCount} rows`);
                const rows = (await pgQuery(client, `SELECT note FROM ${PG_TABLE} WHERE email = 'delete-me'`)).rows;
                check(rows.length === 0, `deleted row still visible: ${JSON.stringify(rows)}`);
            } finally {
                await client.end();
            }
        },
    ],
    [
        "transaction_commit",
        async () => {
            const client = await pgConnect();
            const reader = await pgConnect();
            try {
                await pgQuery(client, "BEGIN");
                await pgQuery(client, `INSERT INTO ${PG_TABLE} (email, note) VALUES ('tx-commit', 'kept')`);
                await pgQuery(client, "COMMIT");
                const rows = (await pgQuery(reader, `SELECT note FROM ${PG_TABLE} WHERE email = 'tx-commit'`)).rows;
                check(rows.length === 1 && rows[0].note === "kept", `committed row returned ${JSON.stringify(rows)}`);
            } finally {
                await client.end();
                await reader.end();
            }
        },
    ],
    [
        "transaction_rollback",
        async () => {
            const client = await pgConnect();
            try {
                await pgQuery(client, "BEGIN");
                await pgQuery(client, `INSERT INTO ${PG_TABLE} (email, note) VALUES ('tx-rollback', 'discarded')`);
                await pgQuery(client, "ROLLBACK");
                const rows = (await pgQuery(client, `SELECT note FROM ${PG_TABLE} WHERE email = 'tx-rollback'`)).rows;
                check(rows.length === 0, `rolled back row still visible: ${JSON.stringify(rows)}`);
            } finally {
                await client.end();
            }
        },
    ],
    [
        "unique_insert",
        async () => {
            const client = await pgConnect();
            try {
                let sqlstate = null;
                try {
                    await pgQuery(client, `INSERT INTO ${PG_TABLE} (email, note) VALUES ('ada', 'duplicate')`);
                } catch (error) {
                    sqlstate = error?.code ?? null;
                }
                check(sqlstate === "23505", `duplicate INSERT returned ${sqlstate}, expected 23505`);
                const rows = (await pgQuery(client, `SELECT note FROM ${PG_TABLE} WHERE email = 'ada'`)).rows;
                check(rows.length === 1 && rows[0].note === "updated", `duplicate INSERT changed the row set: ${JSON.stringify(rows)}`);
            } finally {
                await client.end();
            }
        },
    ],
    [
        "unique_update",
        async () => {
            const client = await pgConnect();
            try {
                await pgQuery(client, `INSERT INTO ${PG_TABLE} (email, note) VALUES ('bob', 'bob')`);
                let sqlstate = null;
                try {
                    await pgQuery(client, `UPDATE ${PG_TABLE} SET email = 'ada' WHERE email = 'bob'`);
                } catch (error) {
                    sqlstate = error?.code ?? null;
                }
                check(sqlstate === "23505", `duplicate UPDATE returned ${sqlstate}, expected 23505`);
                const rows = (await pgQuery(client, `SELECT email FROM ${PG_TABLE} WHERE note = 'bob'`)).rows;
                check(rows.length === 1 && rows[0].email === "bob", `rejected UPDATE changed the row: ${JSON.stringify(rows)}`);
            } finally {
                await client.end();
            }
        },
    ],
    [
        "bigserial_continues_after_reconnect",
        async () => {
            let client = await pgConnect();
            const first = [];
            try {
                await pgQuery(client, `INSERT INTO ${PG_TABLE} (email, note) VALUES ('serial-a', 'a')`);
                await pgQuery(client, `INSERT INTO ${PG_TABLE} (email, note) VALUES ('serial-b', 'b')`);
                first.push(...(await pgQuery(client, `SELECT id FROM ${PG_TABLE} WHERE note IN ('a', 'b') ORDER BY id`)).rows.map((row) => Number(row.id)));
            } finally {
                await client.end();
            }
            check(first.length === 2 && first[1] === first[0] + 1, `consecutive BIGSERIAL values were ${JSON.stringify(first)}`);
            client = await pgConnect();
            try {
                await pgQuery(client, `INSERT INTO ${PG_TABLE} (email, note) VALUES ('serial-c', 'c')`);
                const next = Number((await pgQuery(client, `SELECT id FROM ${PG_TABLE} WHERE email = 'serial-c'`)).rows[0].id);
                check(next === first[1] + 1, `BIGSERIAL after reconnect was ${next}, expected ${first[1] + 1}`);
            } finally {
                await client.end();
            }
        },
    ],
    [
        "reconnect_reads_persisted_rows",
        async () => {
            let client = await pgConnect();
            try {
                await pgQuery(client, `INSERT INTO ${PG_TABLE} (email, note) VALUES ('persist', 'kept')`);
            } finally {
                await client.end();
            }
            client = await pgConnect();
            try {
                const rows = (await pgQuery(client, `SELECT note FROM ${PG_TABLE} WHERE email = 'persist'`)).rows;
                check(rows.length === 1 && rows[0].note === "kept", `row written before the disconnect is gone: ${JSON.stringify(rows)}`);
            } finally {
                await client.end();
            }
        },
    ],
];

const mysqlCases = [
    [
        "connect_and_select",
        async () => {
            const connection = await mysqlConnect();
            try {
                const rows = await mysqlQuery(connection, "SELECT 1");
                check(Number(Object.values(rows[0])[0]) === 1, `SELECT 1 returned ${JSON.stringify(rows)}`);
            } finally {
                await connection.end();
            }
        },
    ],
    [
        "create_table",
        async () => {
            const connection = await mysqlConnect();
            try {
                await mysqlQuery(
                    connection,
                    `CREATE TABLE ${MYSQL_TABLE} (id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY, email VARCHAR(191) NOT NULL, note TEXT, UNIQUE KEY uq_smoke_email (email))`,
                );
            } finally {
                await connection.end();
            }
        },
    ],
    [
        "insert_select",
        async () => {
            const connection = await mysqlConnect();
            try {
                await mysqlQuery(connection, `INSERT INTO ${MYSQL_TABLE} (email, note) VALUES ('ada', 'first')`);
                const rows = await mysqlQuery(connection, `SELECT id, note FROM ${MYSQL_TABLE} WHERE email = 'ada'`);
                check(rows.length === 1, `SELECT returned ${rows.length} rows, expected 1`);
                check(Number(rows[0].id) > 0, `AUTO_INCREMENT assigned ${rows[0].id}`);
                check(rows[0].note === "first", `note column returned ${rows[0].note}`);
            } finally {
                await connection.end();
            }
        },
    ],
    [
        "prepared_select",
        async () => {
            const connection = await mysqlConnect();
            try {
                const [rows] = await connection.execute(`SELECT note FROM ${MYSQL_TABLE} WHERE email = ?`, ["ada"]);
                check(rows.length === 1 && rows[0].note === "first", `prepared SELECT returned ${JSON.stringify(rows)}`);
            } finally {
                await connection.end();
            }
        },
    ],
    [
        "update",
        async () => {
            const connection = await mysqlConnect();
            try {
                const result = await mysqlQuery(connection, `UPDATE ${MYSQL_TABLE} SET note = 'updated' WHERE email = 'ada'`);
                check(result.affectedRows === 1, `UPDATE reported ${result.affectedRows} rows`);
                const rows = await mysqlQuery(connection, `SELECT note FROM ${MYSQL_TABLE} WHERE email = 'ada'`);
                check(rows[0].note === "updated", `updated note returned ${rows[0].note}`);
            } finally {
                await connection.end();
            }
        },
    ],
    [
        "delete",
        async () => {
            const connection = await mysqlConnect();
            try {
                await mysqlQuery(connection, `INSERT INTO ${MYSQL_TABLE} (email, note) VALUES ('delete-me', 'temp')`);
                const result = await mysqlQuery(connection, `DELETE FROM ${MYSQL_TABLE} WHERE email = 'delete-me'`);
                check(result.affectedRows === 1, `DELETE reported ${result.affectedRows} rows`);
                const rows = await mysqlQuery(connection, `SELECT note FROM ${MYSQL_TABLE} WHERE email = 'delete-me'`);
                check(rows.length === 0, `deleted row still visible: ${JSON.stringify(rows)}`);
            } finally {
                await connection.end();
            }
        },
    ],
    [
        "transaction_commit",
        async () => {
            const connection = await mysqlConnect();
            const reader = await mysqlConnect();
            try {
                await mysqlQuery(connection, "BEGIN");
                await mysqlQuery(connection, `INSERT INTO ${MYSQL_TABLE} (email, note) VALUES ('tx-commit', 'kept')`);
                await mysqlQuery(connection, "COMMIT");
                const rows = await mysqlQuery(reader, `SELECT note FROM ${MYSQL_TABLE} WHERE email = 'tx-commit'`);
                check(rows.length === 1 && rows[0].note === "kept", `committed row returned ${JSON.stringify(rows)}`);
            } finally {
                await connection.end();
                await reader.end();
            }
        },
    ],
    [
        "transaction_rollback",
        async () => {
            const connection = await mysqlConnect();
            try {
                await mysqlQuery(connection, "BEGIN");
                await mysqlQuery(connection, `INSERT INTO ${MYSQL_TABLE} (email, note) VALUES ('tx-rollback', 'discarded')`);
                await mysqlQuery(connection, "ROLLBACK");
                const rows = await mysqlQuery(connection, `SELECT note FROM ${MYSQL_TABLE} WHERE email = 'tx-rollback'`);
                check(rows.length === 0, `rolled back row still visible: ${JSON.stringify(rows)}`);
            } finally {
                await connection.end();
            }
        },
    ],
    [
        "unique_insert",
        async () => {
            const connection = await mysqlConnect();
            try {
                let errno = null;
                try {
                    await mysqlQuery(connection, `INSERT INTO ${MYSQL_TABLE} (email, note) VALUES ('ada', 'duplicate')`);
                } catch (error) {
                    errno = error?.errno ?? null;
                }
                check(errno === 1062, `duplicate INSERT returned ${errno}, expected 1062`);
                const rows = await mysqlQuery(connection, `SELECT note FROM ${MYSQL_TABLE} WHERE email = 'ada'`);
                check(rows.length === 1 && rows[0].note === "updated", `duplicate INSERT changed the row set: ${JSON.stringify(rows)}`);
            } finally {
                await connection.end();
            }
        },
    ],
    [
        "unique_update",
        async () => {
            const connection = await mysqlConnect();
            try {
                await mysqlQuery(connection, `INSERT INTO ${MYSQL_TABLE} (email, note) VALUES ('bob', 'bob')`);
                let errno = null;
                try {
                    await mysqlQuery(connection, `UPDATE ${MYSQL_TABLE} SET email = 'ada' WHERE email = 'bob'`);
                } catch (error) {
                    errno = error?.errno ?? null;
                }
                check(errno === 1062, `duplicate UPDATE returned ${errno}, expected 1062`);
                const rows = await mysqlQuery(connection, `SELECT email FROM ${MYSQL_TABLE} WHERE note = 'bob'`);
                check(rows.length === 1 && rows[0].email === "bob", `rejected UPDATE changed the row: ${JSON.stringify(rows)}`);
            } finally {
                await connection.end();
            }
        },
    ],
    [
        "auto_increment_continues_after_reconnect",
        async () => {
            let connection = await mysqlConnect();
            const first = [];
            try {
                await mysqlQuery(connection, `INSERT INTO ${MYSQL_TABLE} (email, note) VALUES ('serial-a', 'a')`);
                await mysqlQuery(connection, `INSERT INTO ${MYSQL_TABLE} (email, note) VALUES ('serial-b', 'b')`);
                const rows = await mysqlQuery(connection, `SELECT id FROM ${MYSQL_TABLE} WHERE note IN ('a', 'b') ORDER BY id`);
                first.push(...rows.map((row) => Number(row.id)));
            } finally {
                await connection.end();
            }
            check(first.length === 2 && first[1] === first[0] + 1, `consecutive AUTO_INCREMENT values were ${JSON.stringify(first)}`);
            connection = await mysqlConnect();
            try {
                await mysqlQuery(connection, `INSERT INTO ${MYSQL_TABLE} (email, note) VALUES ('serial-c', 'c')`);
                const next = Number((await mysqlQuery(connection, `SELECT id FROM ${MYSQL_TABLE} WHERE email = 'serial-c'`))[0].id);
                check(next === first[1] + 1, `AUTO_INCREMENT after reconnect was ${next}, expected ${first[1] + 1}`);
            } finally {
                await connection.end();
            }
        },
    ],
    [
        "reconnect_reads_persisted_rows",
        async () => {
            let connection = await mysqlConnect();
            try {
                await mysqlQuery(connection, `INSERT INTO ${MYSQL_TABLE} (email, note) VALUES ('persist', 'kept')`);
            } finally {
                await connection.end();
            }
            connection = await mysqlConnect();
            try {
                const rows = await mysqlQuery(connection, `SELECT note FROM ${MYSQL_TABLE} WHERE email = 'persist'`);
                check(rows.length === 1 && rows[0].note === "kept", `row written before the disconnect is gone: ${JSON.stringify(rows)}`);
            } finally {
                await connection.end();
            }
        },
    ],
];

for (const [name, body] of pgCases) {
    await caseRun("postgres", name, body);
}
for (const [name, body] of mysqlCases) {
    await caseRun("mysql", name, body);
}

const total = pgCases.length + mysqlCases.length;
console.log(`TS smoke ${passed}/${total} passed, ${failures.length} failed`);
if (failures.length > 0) {
    for (const failure of failures) console.error(failure);
    process.exit(1);
}
