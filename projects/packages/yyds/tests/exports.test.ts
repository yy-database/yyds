import assert from "node:assert/strict";
import test from "node:test";

test("@yyds/yyds node entry exposes native loader helpers", async () => {
    const node = await import("../src/node/index.ts");
    assert.equal(typeof node.loadYydsNative, "function");
    assert.equal(typeof node.isYydsNativeInstalled, "function");
});
