import assert from "node:assert/strict";
import test from "node:test";

test("@yyds/redis re-exports @yyds/yyds", async () => {
    const redis = await import("../src/index.ts");
    assert.equal(typeof redis.initWasm, "function");
});

test("@yyds/redis node entry re-exports native loader", async () => {
    const node = await import("../src/node.ts");
    assert.equal(typeof node.loadYydsNative, "function");
});
