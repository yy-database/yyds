import assert from "node:assert/strict";
import test from "node:test";

test("@yyds/mysql re-exports @yyds/yyds", async () => {
    const mysql = await import("../src/index.ts");
    assert.equal(typeof mysql.initWasm, "function");
});

test("@yyds/mysql node entry re-exports native loader", async () => {
    const node = await import("../src/node.ts");
    assert.equal(typeof node.loadYydsNative, "function");
});
