import assert from "node:assert/strict";
import test from "node:test";
import { yydsStartArguments } from "../src/node/cluster.ts";

test("@yyds/yyds node entry exposes native loader helpers", async () => {
    const node = await import("../src/node/index.ts");
    assert.equal(typeof node.loadYydsNative, "function");
    assert.equal(typeof node.isYydsNativeInstalled, "function");
});

test("protocol server launchers delegate their ports to the yyds lifecycle", () => {
    assert.deepEqual(yydsStartArguments("redis", ["--data-dir", "node-data", "--port", "6380"]), [
        "start",
        "--data-dir",
        "node-data",
        "--redis-port",
        "6380",
    ]);
    assert.deepEqual(yydsStartArguments("mysql", ["--data-dir", "node-data"]), [
        "start",
        "--data-dir",
        "node-data",
        "--mysql-port",
        "3306",
    ]);
    assert.deepEqual(
        yydsStartArguments("postgresql", ["--data-dir", "node-data", "--port", "5544"]),
        ["start", "--data-dir", "node-data", "--pgsql-port", "5544"],
    );
    assert.throws(() => yydsStartArguments("redis", ["--port"]), /missing port/);
});
