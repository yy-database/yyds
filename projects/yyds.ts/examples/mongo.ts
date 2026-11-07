// 下游验收示例：真实 mongodb driver 对 yyds Mongo 网关做 1000 文档写入、读取、排序与删除。
//
// 期望值由本示例按写入输入自行推导（文档数、抽样文档、排序结果、删除计数、重复键码值），
// 不看网关内部常量。节点由 `node projects/yyds.ts/packages/yyds-harness/run.mjs` 启动并通过 `YYDS_MONGO_PORT` 传入，
// 驱动复用 `yyds-harness` 已安装的依赖。
//
// 设置 `YYDS_ORACLE_MONGO_URL`（例如 `mongodb://127.0.0.1:27017`）时，同一批操作会在真实
// MongoDB 上重放一次并对拍事实；没有 oracle 时只输出 `UNVERIFIED`，不计作通过。

import { createRequire } from "node:module";
import { join, resolve } from "node:path";

const ROOT = resolve(import.meta.dirname, "../../..");
const require = createRequire(join(ROOT, "projects/yyds.ts/packages/yyds-harness/package.json"));
const { MongoClient } = require("mongodb");

const DOCUMENTS = 1000;
const host = process.env.YYDS_HOST ?? "127.0.0.1";
const port = Number(process.env.YYDS_MONGO_PORT);
const oracleUrl = process.env.YYDS_ORACLE_MONGO_URL;
const DATABASE = "example_mongo";

if (!Number.isInteger(port) || port <= 0) {
    console.error("YYDS_MONGO_PORT must point at a running yyds node");
    process.exit(2);
}

function check(condition, message) {
    if (!condition) throw new Error(message);
}

function connect(url) {
    return new MongoClient(url, {
        directConnection: true,
        retryWrites: false,
        retryReads: false,
        serverSelectionTimeoutMS: 30_000,
    });
}

async function facts(client, suffix) {
    const database = client.db(DATABASE);
    const collection = database.collection(`items_${suffix}`);
    const duplicates = database.collection(`duplicates_${suffix}`);
    await collection.deleteMany({});
    await duplicates.deleteMany({});

    const payload = Array.from({ length: DOCUMENTS }, (_, index) => ({
        _id: index,
        n: index,
        label: `doc-${String(index).padStart(4, "0")}`,
    }));
    await collection.insertMany(payload, { ordered: true });

    let scanned = 0;
    for await (const _document of collection.find({})) scanned += 1;
    check(scanned === DOCUMENTS, `scanned ${scanned} documents, expected ${DOCUMENTS}`);

    const spot = await collection.findOne({ _id: DOCUMENTS / 2 });
    check(spot !== null && spot.label === "doc-0500", `spot document was ${JSON.stringify(spot)}`);

    const ordered = (await collection.find({}).sort({ n: 1 }).limit(5).toArray()).map((document) => document.n);
    check(JSON.stringify(ordered) === JSON.stringify([0, 1, 2, 3, 4]), `sorted prefix was ${JSON.stringify(ordered)}`);

    let errorCode = null;
    let insertedOnError = null;
    try {
        await duplicates.insertMany([{ _id: 0 }, { _id: 0 }], { ordered: true });
    } catch (error) {
        errorCode = error?.writeErrors?.[0]?.code ?? error?.code ?? null;
        insertedOnError = error?.result?.insertedCount ?? error?.insertedCount ?? null;
        check(errorCode === 11000, `duplicate identifiers answered code ${errorCode}, expected 11000`);
    }

    const removed = (await collection.deleteMany({})).deletedCount;
    check(removed === DOCUMENTS, `deleted ${removed} documents, expected ${DOCUMENTS}`);

    return {
        inserted: payload.length,
        scanned,
        "sorted-prefix": ordered,
        removed,
        "error-code": errorCode,
        "inserted-on-error": insertedOnError,
        "absent-after-delete": (await collection.findOne({ _id: DOCUMENTS / 2 })) === null,
    };
}

function compare(gateway, oracle) {
    for (const field of ["inserted", "scanned", "sorted-prefix", "removed", "error-code", "inserted-on-error", "absent-after-delete"]) {
        check(
            JSON.stringify(gateway[field]) === JSON.stringify(oracle[field]),
            `${field} differs: gateway=${JSON.stringify(gateway[field])} oracle=${JSON.stringify(oracle[field])}`,
        );
    }
}

async function main() {
    let failed = 0;
    let gateway;
    const gatewayClient = connect(`mongodb://${host}:${port}`);
    try {
        await gatewayClient.connect();
        gateway = await facts(gatewayClient, "gateway");
        console.log(`PASS examples/mongo.ts gateway ${DOCUMENTS} documents`);
    } catch (error) {
        failed += 1;
        console.log(`FAIL examples/mongo.ts gateway: ${error?.message ?? error}`);
    } finally {
        await gatewayClient.close();
    }

    if (oracleUrl) {
        const oracleClient = connect(oracleUrl);
        try {
            await oracleClient.connect();
            const oracle = await facts(oracleClient, "oracle");
            compare(gateway, oracle);
            console.log("PASS examples/mongo.ts parity");
        } catch (error) {
            failed += 1;
            console.log(`FAIL examples/mongo.ts parity: ${error?.message ?? error}`);
        } finally {
            await oracleClient.close();
        }
    } else {
        console.log("UNVERIFIED examples/mongo.ts parity: set YYDS_ORACLE_MONGO_URL to replay against a real MongoDB");
    }

    return failed === 0 ? 0 : 1;
}

process.exit(await main());
