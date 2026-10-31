import assert from "node:assert/strict";
import { mkdir, mkdtemp, writeFile } from "node:fs/promises";
import { createServer } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { isAbsolute } from "node:path";
import test from "node:test";
import type { AddressInfo } from "node:net";

import { parseWebuiArgs } from "../src/node/webui-cmd.ts";
import {
    resolveWebuiDist,
    startStaticServer,
    waitForWire,
} from "../src/node/webui-static.ts";

test("parseWebuiArgs applies defaults and accepts endpoint-only launches", () => {
    assert.deepEqual(
        parseWebuiArgs(["--data-dir", "d", "--cluster-id", "c", "--node-id", "n"]),
        {
            dataDir: "d",
            clusterId: "c",
            nodeId: "n",
            wirePort: 7700,
            webuiPort: 8780,
            open: true,
        },
    );

    const tuned = parseWebuiArgs([
        "--data-dir",
        "d",
        "--cluster-id",
        "c",
        "--node-id",
        "n",
        "--wire-port",
        "1234",
        "--port",
        "4321",
        "--no-open",
    ]);
    assert.equal(tuned.wirePort, 1234);
    assert.equal(tuned.webuiPort, 4321);
    assert.equal(tuned.open, false);

    const remote = parseWebuiArgs(["--endpoint", "ws://host:9/wire"]);
    assert.equal(remote.endpoint, "ws://host:9/wire");
    assert.equal(remote.dataDir, "");

    // A trailing flag without a value keeps the fallback port instead of throwing.
    assert.equal(parseWebuiArgs(["--endpoint", "e", "--wire-port"]).wirePort, 7700);
});

test("parseWebuiArgs rejects missing identity, bad ports and unknown flags", () => {
    assert.throws(() => parseWebuiArgs([]), /requires --endpoint/);
    assert.throws(() => parseWebuiArgs(["--data-dir", "d"]), /requires --endpoint/);
    assert.throws(
        () => parseWebuiArgs(["--endpoint", "e", "--wire-port", "0"]),
        /invalid wire-port: 0/,
    );
    assert.throws(
        () => parseWebuiArgs(["--endpoint", "e", "--wire-port", "1.5"]),
        /invalid wire-port/,
    );
    assert.throws(
        () => parseWebuiArgs(["--endpoint", "e", "--port", "70000"]),
        /invalid port: 70000/,
    );
    assert.throws(() => parseWebuiArgs(["--bogus"]), /unknown option '--bogus'/);
});

test("startStaticServer serves MIME-typed assets, 404s misses and blocks traversal", async (t) => {
    const parent = await mkdtemp(join(tmpdir(), "yyds-webui-"));
    const root = join(parent, "site");
    await mkdir(root, { recursive: true });
    await writeFile(join(root, "index.html"), "<html>root</html>");
    await writeFile(join(root, "app.js"), "console.log(1)");
    await writeFile(join(root, "data.bin"), "binary");
    await writeFile(join(parent, "secret.txt"), "top secret");

    const server = await startStaticServer(root, 0);
    const port = (server.address() as AddressInfo).port;
    const base = `http://127.0.0.1:${port}`;
    t.after(() => server.close());

    const index = await fetch(`${base}/`);
    assert.equal(index.status, 200);
    assert.equal(index.headers.get("content-type"), "text/html; charset=utf-8");
    assert.equal(await index.text(), "<html>root</html>");

    const script = await fetch(`${base}/app.js`);
    assert.equal(script.headers.get("content-type"), "text/javascript; charset=utf-8");

    const binary = await fetch(`${base}/data.bin`);
    assert.equal(binary.headers.get("content-type"), "application/octet-stream");

    const missing = await fetch(`${base}/nope.missing`);
    assert.equal(missing.status, 404);
    assert.equal(await missing.text(), "not found");

    // WHATWG URL parsing collapses a bare "%2e%2e" segment into "..", so the
    // traversal must hide in one encoded segment ("%2e%2e%2f") that survives
    // parsing and only becomes "../" after decodeURIComponent.
    const escape = await fetch(`${base}/%2e%2e%2fsecret.txt`);
    assert.equal(escape.status, 403);
    assert.equal(await escape.text(), "forbidden");
});

test("resolveWebuiDist returns an absolute candidate directory", () => {
    assert.ok(isAbsolute(resolveWebuiDist()));
});

test("waitForWire resolves without throwing when the wire is unreachable", async () => {
    const deadPort = await new Promise<number>((resolvePort, reject) => {
        const probe = createServer();
        probe.once("error", reject);
        probe.listen(0, "127.0.0.1", () => {
            const port = (probe.address() as AddressInfo).port;
            probe.close(() => resolvePort(port));
        });
    });
    await waitForWire(`ws://127.0.0.1:${deadPort}/wire`, 1_200);
});
