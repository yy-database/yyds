// Real-driver protocol conformance against a `yyds start` node.
//
// The Rust wire suites assert frame codecs and dispatch shapes against helpers
// that mirror the TypeScript client. This suite asserts the same guarantees
// through the external drivers instead: a driver that connects, parses the
// handshake, round-trips a value, reports a missing key, and survives an
// unsupported request is the oracle. No expectation is copied from gateway
// internals — every value is derived from what the driver sent.
//
// Run through `node e2e/run.mjs`, or by hand with the `YYDS_*_PORT` variables
// pointing at an already running node.

import { createRequire } from "node:module";
import { join, resolve } from "node:path";

const ROOT = resolve(import.meta.dirname, "..");
const require = createRequire(join(ROOT, "e2e/package.json"));
const Redis = require("ioredis");
const { MongoClient } = require("mongodb");
const mysql = require("mysql2/promise");
const { Client } = require("pg");

const host = process.env.YYDS_HOST ?? "127.0.0.1";
const redisPort = Number(process.env.YYDS_REDIS_PORT);
const mongoPort = Number(process.env.YYDS_MONGO_PORT);
const pgPort = Number(process.env.YYDS_PG_PORT);
const mysqlPort = Number(process.env.YYDS_MYSQL_PORT);
const tag = process.env.YYDS_PROTOCOL_TAG ?? "ts";
const REDIS_PREFIX = `protocol:${tag}`;
const MONGO_COLLECTION = `protocol_${tag}`;

let passed = 0;
const failures = [];

function check(condition, message) {
    if (!condition) throw new Error(message);
}

function requirePort(value, name) {
    if (!Number.isInteger(value) || value <= 0) throw new Error(`${name} must point at a running yyds node`);
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

// `enableReadyCheck: false` keeps ioredis from issuing `INFO` on connect, which
// this gateway answers with an error instead of fabricating server stats.
function redisConnect(options = {}) {
    return new Redis(`redis://${host}:${redisPort}`, {
        lazyConnect: false,
        connectTimeout: 30_000,
        commandTimeout: 30_000,
        enableReadyCheck: false,
        ...options,
    });
}

async function mongoConnect() {
    const client = new MongoClient(`mongodb://${host}:${mongoPort}`, {
        directConnection: true,
        retryWrites: false,
        retryReads: false,
        serverSelectionTimeoutMS: 30_000,
    });
    await client.connect();
    return client;
}

async function pgConnect() {
    const client = new Client({ host, port: pgPort, user: "yyds", password: "", database: "yyds" });
    await client.connect();
    return client;
}

function mysqlConnect() {
    return mysql.createConnection({ host, port: mysqlPort, user: "yyds", password: "", charset: "utf8mb4" });
}

const cases = [
    [
        "redis",
        "ping_answers_pong",
        async () => {
            const client = redisConnect();
            try {
                check((await client.ping()) === "PONG", "PING did not answer PONG");
            } finally {
                client.disconnect();
            }
        },
    ],
    [
        "redis",
        "kv_roundtrip_and_missing_is_null",
        async () => {
            const client = redisConnect();
            try {
                const key = `${REDIS_PREFIX}:kv`;
                const value = `value-${tag}-ü-🚀`;
                check((await client.set(key, value)) === "OK", "SET did not answer OK");
                check((await client.get(key)) === value, "GET returned a different value");
                check((await client.get(`${REDIS_PREFIX}:absent`)) === null, "missing GET did not answer null");
                check((await client.del(key)) === 1, "DEL did not remove the key");
                check((await client.get(key)) === null, "deleted key is still readable");
            } finally {
                client.disconnect();
            }
        },
    ],
    [
        "redis",
        "binary_payload_roundtrip",
        async () => {
            const writer = redisConnect();
            const reader = redisConnect({ returnBuffers: true });
            try {
                const payload = Buffer.from([0x00, 0x01, 0x02, 0x7f, 0x80, 0xfe, 0xff]);
                const key = `${REDIS_PREFIX}:binary`;
                await writer.set(key, payload);
                const stored = await reader.getBuffer(key);
                check(stored !== null && stored.equals(payload), `binary GET returned ${stored === null ? "null" : stored.toString("hex")}`);
                await writer.del(key);
            } finally {
                writer.disconnect();
                reader.disconnect();
            }
        },
    ],
    [
        "redis",
        "unsupported_command_is_rejected_and_connection_survives",
        async () => {
            const client = redisConnect();
            try {
                let message = null;
                try {
                    await client.call("YYDSNOSUCHCOMMAND");
                } catch (error) {
                    message = error?.message ?? String(error);
                }
                check(message !== null, "unsupported command was accepted");
                check(/unsupported command/i.test(message), `unsupported command answered ${message}`);
                check((await client.ping()) === "PONG", "connection stopped answering after the rejected command");
            } finally {
                client.disconnect();
            }
        },
    ],
    [
        "mongo",
        "ping_command_succeeds",
        async () => {
            const client = await mongoConnect();
            try {
                const reply = await client.db("admin").command({ ping: 1 });
                check(Number(reply.ok) === 1, `ping answered ok=${reply.ok}`);
            } finally {
                await client.close();
            }
        },
    ],
    [
        "mongo",
        "document_roundtrip_and_missing_is_null",
        async () => {
            const client = await mongoConnect();
            try {
                const collection = client.db(`protocol_${tag}`).collection(MONGO_COLLECTION);
                await collection.deleteMany({});
                const document = { _id: `${tag}-doc`, n: 7, label: "kept" };
                await collection.insertOne(document);
                const stored = await collection.findOne({ _id: document._id });
                check(JSON.stringify(stored) === JSON.stringify(document), `findOne returned ${JSON.stringify(stored)}`);
                check((await collection.findOne({ _id: `${tag}-absent` })) === null, "missing findOne did not answer null");
                await collection.deleteMany({});
            } finally {
                await client.close();
            }
        },
    ],
    [
        "postgres",
        "server_version_is_stable_across_connections",
        async () => {
            const first = await pgConnect();
            let version = null;
            try {
                version = Object.values((await first.query("SELECT version()")).rows[0])[0];
            } finally {
                await first.end();
            }
            check(typeof version === "string" && version.length > 0, `SELECT version() returned ${JSON.stringify(version)}`);
            const second = await pgConnect();
            try {
                const again = Object.values((await second.query("SELECT version()")).rows[0])[0];
                check(again === version, `version changed between connections: ${version} vs ${again}`);
            } finally {
                await second.end();
            }
        },
    ],
    [
        "postgres",
        "unknown_table_is_structured_error_and_connection_survives",
        async () => {
            const client = await pgConnect();
            try {
                let sqlstate = null;
                try {
                    await client.query(`SELECT note FROM protocol_absent_${tag}`);
                } catch (error) {
                    sqlstate = error?.code ?? null;
                }
                check(sqlstate === "42P01", `unknown table answered sqlstate ${sqlstate}, expected 42P01`);
                const rows = (await client.query("SELECT 1")).rows;
                check(Number(Object.values(rows[0])[0]) === 1, "connection stopped answering after the error");
            } finally {
                await client.end();
            }
        },
    ],
    [
        "mysql",
        "server_version_is_stable_across_connections",
        async () => {
            let version = null;
            let connection = await mysqlConnect();
            try {
                const [rows] = await connection.query("SELECT @@version");
                version = Object.values(rows[0])[0];
            } finally {
                await connection.end();
            }
            check(typeof version === "string" && version.length > 0, `SELECT @@version returned ${JSON.stringify(version)}`);
            connection = await mysqlConnect();
            try {
                const [rows] = await connection.query("SELECT @@version");
                check(Object.values(rows[0])[0] === version, `version changed between connections: ${version}`);
            } finally {
                await connection.end();
            }
        },
    ],
    [
        "mysql",
        "unknown_table_is_structured_error_and_connection_survives",
        async () => {
            const connection = await mysqlConnect();
            try {
                let errno = null;
                try {
                    await connection.query(`SELECT note FROM protocol_absent_${tag}`);
                } catch (error) {
                    errno = error?.errno ?? null;
                }
                check(errno === 1146, `unknown table answered errno ${errno}, expected 1146`);
                const [rows] = await connection.query("SELECT 1");
                check(Number(Object.values(rows[0])[0]) === 1, "connection stopped answering after the error");
            } finally {
                await connection.end();
            }
        },
    ],
];

requirePort(redisPort, "YYDS_REDIS_PORT");
requirePort(mongoPort, "YYDS_MONGO_PORT");
requirePort(pgPort, "YYDS_PG_PORT");
requirePort(mysqlPort, "YYDS_MYSQL_PORT");

for (const [driver, name, body] of cases) {
    await caseRun(driver, name, body);
}

console.log(`TS protocol ${passed}/${cases.length} passed, ${failures.length} failed`);
if (failures.length > 0) {
    for (const failure of failures) console.error(failure);
    process.exit(1);
}
