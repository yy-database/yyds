import assert from "node:assert/strict";
import test from "node:test";

test("@yyds/postgresql re-exports @yyds/yyds", async () => {
    const postgresql = await import("../src/index.ts");
    assert.equal(typeof postgresql.initWasm, "function");
});

test("@yyds/postgresql node entry re-exports native loader", async () => {
    const node = await import("../src/node.ts");
    assert.equal(typeof node.loadYydsNative, "function");
});
