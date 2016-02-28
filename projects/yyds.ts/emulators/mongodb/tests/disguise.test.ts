import assert from "node:assert/strict";
import test from "node:test";

test("@yydb/mongodb re-exports @yydb/yyds", async () => {
    const mongodb = await import("../src/index.ts");
    assert.equal(typeof mongodb.loadYydsNative, "function");
});

test("@yydb/mongodb node entry re-exports native loader", async () => {
    const node = await import("../src/node.ts");
    assert.equal(typeof node.loadYydsNative, "function");
});
