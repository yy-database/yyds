// 下游验收示例：真实 ioredis 对 yyds Redis 网关做 1000 键写入、读取、过期与删除。
//
// 期望值由本示例按写入输入自行推导（键数、字节值、TTL、删除计数），不看网关内部常量。
// 节点由 `node e2e/run.mjs` 启动并通过 `YYDS_REDIS_PORT` 传入，驱动复用 `e2e` 已安装的依赖。
//
// 设置 `YYDS_ORACLE_REDIS_URL`（例如 `redis://127.0.0.1:6379/0`）时，同一批操作会在真实
// Redis 上重放一次并对拍事实；没有 oracle 时只输出 `UNVERIFIED`，不计作通过。

import { createRequire } from "node:module";
import { join, resolve } from "node:path";

const ROOT = resolve(import.meta.dirname, "../../..");
const require = createRequire(join(ROOT, "e2e/package.json"));
const Redis = require("ioredis");

const KEYS = 1000;
const host = process.env.YYDS_HOST ?? "127.0.0.1";
const port = Number(process.env.YYDS_REDIS_PORT);
const oracleUrl = process.env.YYDS_ORACLE_REDIS_URL;
const PREFIX = "example:redis";

if (!Number.isInteger(port) || port <= 0) {
    console.error("YYDS_REDIS_PORT must point at a running yyds node");
    process.exit(2);
}

function check(condition, message) {
    if (!condition) throw new Error(message);
}

// `enableReadyCheck: false` keeps ioredis from issuing `INFO` on connect, which
// the gateway answers with an error instead of fabricating server stats.
function connect(url) {
    return new Redis(url, {
        lazyConnect: false,
        connectTimeout: 30_000,
        commandTimeout: 30_000,
        enableReadyCheck: false,
    });
}

async function facts(client, suffix) {
    const keys = Array.from({ length: KEYS }, (_, index) => `${PREFIX}:${suffix}:${String(index).padStart(4, "0")}`);
    const values = Array.from({ length: KEYS }, (_, index) => `value-${String(index).padStart(4, "0")}`);

    const writes = client.pipeline();
    for (let index = 0; index < KEYS; index += 1) writes.set(keys[index], values[index]);
    const acknowledgements = await writes.exec();
    check(
        acknowledgements.length === KEYS && acknowledgements.every(([error, ack]) => error === null && ack === "OK"),
        "set acknowledgements were not all OK",
    );

    const reads = client.pipeline();
    for (const key of keys) reads.get(key);
    const stored = (await reads.exec()).map(([error, value]) => {
        check(error === null, `pipeline read failed: ${error}`);
        return value;
    });
    check(JSON.stringify(stored) === JSON.stringify(values), "pipeline reads did not return the written bytes");

    check((await client.get(keys[KEYS / 2])) === values[KEYS / 2], "spot read returned a different value");

    const ttls = client.pipeline();
    for (const key of keys) ttls.ttl(key);
    const deadlines = (await ttls.exec()).map(([error, deadline]) => {
        check(error === null, `ttl failed: ${error}`);
        return deadline;
    });
    check(deadlines.every((deadline) => deadline === -1), `keys unexpectedly carried deadlines: ${new Set(deadlines)}`);

    await client.expire(keys[0], 5);
    const live = await client.ttl(keys[0]);
    check(live >= 1 && live <= 5, `ttl after expire was ${live}`);

    const removals = client.pipeline();
    for (const key of keys) removals.del(key);
    const removed = (await removals.exec())
        .map(([error, count]) => {
            check(error === null, `del failed: ${error}`);
            return Number(count);
        })
        .reduce((total, count) => total + count, 0);
    check(removed === KEYS, `deleted ${removed} keys, expected ${KEYS}`);

    return {
        written: acknowledgements.length,
        "read-back-equal": JSON.stringify(stored) === JSON.stringify(values),
        "no-deadline": deadlines.every((deadline) => deadline === -1),
        "expire-ttl": live,
        removed,
        "absent-after-delete": (await client.get(keys[0])) === null,
    };
}

function compare(gateway, oracle) {
    for (const field of ["written", "read-back-equal", "no-deadline", "removed", "absent-after-delete"]) {
        check(
            gateway[field] === oracle[field],
            `${field} differs: gateway=${JSON.stringify(gateway[field])} oracle=${JSON.stringify(oracle[field])}`,
        );
    }
    check(Math.abs(Number(gateway["expire-ttl"]) - Number(oracle["expire-ttl"])) <= 2, "expire-ttl differs by more than two seconds");
}

async function main() {
    let failed = 0;
    let gateway;
    const gatewayClient = connect(`redis://${host}:${port}`);
    try {
        gateway = await facts(gatewayClient, "gateway");
        console.log(`PASS examples/redis.ts gateway ${KEYS} keys`);
    } catch (error) {
        failed += 1;
        console.log(`FAIL examples/redis.ts gateway: ${error?.message ?? error}`);
    } finally {
        gatewayClient.disconnect();
    }

    if (oracleUrl) {
        const oracleClient = connect(oracleUrl);
        try {
            const oracle = await facts(oracleClient, "oracle");
            compare(gateway, oracle);
            console.log("PASS examples/redis.ts parity");
        } catch (error) {
            failed += 1;
            console.log(`FAIL examples/redis.ts parity: ${error?.message ?? error}`);
        } finally {
            oracleClient.disconnect();
        }
    } else {
        console.log("UNVERIFIED examples/redis.ts parity: set YYDS_ORACLE_REDIS_URL to replay against a real Redis");
    }

    return failed === 0 ? 0 : 1;
}

process.exit(await main());
