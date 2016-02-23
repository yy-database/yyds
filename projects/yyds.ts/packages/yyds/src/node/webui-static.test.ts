import assert from "node:assert/strict";
import { test } from "node:test";

import { wsEndpointFromWirePort } from "./webui-static.ts";

test("wsEndpointFromWirePort maps loopback wire port to WebSocket URL", () => {
    assert.equal(wsEndpointFromWirePort(7700), "ws://127.0.0.1:7700/wire");
});

test("wsEndpointFromWirePort accepts custom host", () => {
    assert.equal(wsEndpointFromWirePort(9001, "10.0.0.5"), "ws://10.0.0.5:9001/wire");
});
