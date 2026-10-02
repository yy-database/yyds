import assert from "node:assert/strict";
import test from "node:test";

import { WHY_NOT_YY_OPTIMIZED } from "../src/disguise.ts";

test("@yyds/sqlite re-exports @yyds/yyds", async () => {
    const sqlite = await import("../src/index.ts");
    assert.equal(typeof sqlite.initWasm, "function");
});

test("@yyds/sqlite node entry re-exports native loader", async () => {
    const node = await import("../src/node.ts");
    assert.equal(typeof node.loadYydsNative, "function");
});

test("@yyds/sqlite documents binary compatibility constraint", () => {
    assert.match(WHY_NOT_YY_OPTIMIZED, /binary/i);
    assert.match(WHY_NOT_YY_OPTIMIZED, /\.yyds/);
});
