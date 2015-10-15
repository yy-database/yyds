import assert from "node:assert/strict";
import test from "node:test";

import { yydsStartArguments } from "../src/node/cluster.ts";
import { parseStartOptions } from "../src/node/runtime.ts";

test("@yydb/yyds node entry exposes native loader helpers", async () => {
    const node = await import("../src/node/index.ts");
    assert.equal(typeof node.loadYydsNative, "function");
    assert.equal(typeof node.isYydsNativeInstalled, "function");
    assert.equal(typeof node.runNodeStart, "function");
    assert.equal(typeof node.parseStartOptions, "function");
});

test("protocol server launchers map disguise ports into node start options", () => {
    assert.deepEqual(yydsStartArguments("redis", ["--data-dir", "node-data", "--port", "6380"]), [
        "--data-dir",
        "node-data",
        "--redis-port",
        "6380",
    ]);
    assert.deepEqual(yydsStartArguments("mysql", ["--data-dir", "node-data"]), [
        "--data-dir",
        "node-data",
        "--mysql-port",
        "3306",
    ]);
    assert.deepEqual(
        yydsStartArguments("postgresql", ["--data-dir", "node-data", "--port", "5544"]),
        ["--data-dir", "node-data", "--pgsql-port", "5544"],
    );
    assert.throws(() => yydsStartArguments("redis", ["--port"]), /missing port/);

    assert.deepEqual(
        parseStartOptions("redis", [
            "--data-dir",
            "node-data",
            "--cluster-id",
            "cluster",
            "--node-id",
            "node",
            "--redis-port",
            "6380",
        ]),
        {
            dataDir: "node-data",
            clusterId: "cluster",
            nodeId: "node",
            redisPort: 6380,
        },
    );
});
