import assert from "node:assert/strict";
import test from "node:test";

import { yydsStartArguments } from "../src/node/cluster.ts";
import { isYydsNativeInstalled, loadYydsNative } from "../src/node/load.ts";
import { resolvePlatformPackage } from "../src/node/platform-packages.ts";
import { parseStartOptions } from "../src/node/runtime.ts";
import { parseAddress } from "./support/node-harness/src/index.ts";

test("yydsStartArguments maps every protocol port flag including -p and -P", () => {
    // MongoDB has no default port, so no flag is appended without an explicit one.
    assert.deepEqual(yydsStartArguments("mongodb", ["--data-dir", "d"]), ["--data-dir", "d"]);
    assert.deepEqual(yydsStartArguments("mongodb", ["--port", "27017"]), ["--mongo-port", "27017"]);
    assert.deepEqual(yydsStartArguments("postgresql", ["--data-dir", "d"]), [
        "--data-dir",
        "d",
        "--pgsql-port",
        "5432",
    ]);
    assert.deepEqual(yydsStartArguments("redis", ["-P", "6390"]), ["--redis-port", "6390"]);
    assert.deepEqual(yydsStartArguments("mysql", ["-p", "3307"]), ["--mysql-port", "3307"]);

    assert.throws(() => yydsStartArguments("redis", ["--port", "--data-dir", "d"]), {
        message: "missing port after --port",
    });
    assert.throws(() => yydsStartArguments("mysql", ["-P"]), { message: "missing port after -P" });
});

test("parseStartOptions covers defaults, runtime flags and rejections", () => {
    const identity = ["--data-dir", "d", "--cluster-id", "c", "--node-id", "n"];
    assert.deepEqual(parseStartOptions("redis", [...identity, "-P", "6390"]), {
        dataDir: "d",
        clusterId: "c",
        nodeId: "n",
        redisPort: 6390,
    });
    assert.deepEqual(parseStartOptions("mysql", identity), {
        dataDir: "d",
        clusterId: "c",
        nodeId: "n",
        redisPort: 6379,
        mysqlPort: 3306,
    });
    assert.deepEqual(parseStartOptions("postgresql", identity), {
        dataDir: "d",
        clusterId: "c",
        nodeId: "n",
        redisPort: 6379,
        pgsqlPort: 5432,
    });
    // MongoDB keeps its port undefined unless requested.
    assert.deepEqual(parseStartOptions("mongodb", identity), {
        dataDir: "d",
        clusterId: "c",
        nodeId: "n",
        redisPort: 6379,
    });

    assert.deepEqual(
        parseStartOptions("redis", [...identity, "--wire-port", "7700", "--catalog-path", "cat"]),
        {
            dataDir: "d",
            clusterId: "c",
            nodeId: "n",
            redisPort: 6379,
            catalogPath: "cat",
            wirePort: 7700,
        },
    );
    // An empty catalog path is dropped so Rust never sees a blank value.
    const blank = parseStartOptions("redis", [...identity, "--catalog-path", ""]);
    assert.equal(blank.catalogPath, undefined);

    assert.throws(() => parseStartOptions("redis", [...identity, "--bogus", "x"]), {
        message: "unknown option '--bogus'",
    });
    assert.throws(() => parseStartOptions("redis", ["--data-dir", "d"]), {
        message: "start requires --data-dir, --cluster-id, and --node-id",
    });
    assert.throws(() => parseStartOptions("redis", [...identity, "-P", "abc"]), {
        message: "missing port after -P",
    });
    assert.throws(() => parseStartOptions("redis", [...identity, "-P", "-1"]), {
        message: "missing port after -P",
    });
});

test("parseAddress accepts IPv4 and bracketed IPv6 and rejects malformed input", () => {
    assert.deepEqual(parseAddress("127.0.0.1:6379"), { host: "127.0.0.1", port: 6379 });
    assert.deepEqual(parseAddress("[::1]:5432"), { host: "::1", port: 5432 });
    assert.deepEqual(parseAddress(" 127.0.0.1:6380 "), { host: "127.0.0.1", port: 6380 });

    assert.throws(() => parseAddress("no-colon"), /invalid address/);
    assert.throws(() => parseAddress(":3306"), /invalid address/);
    assert.throws(() => parseAddress("127.0.0.1:0"), /invalid address/);
    assert.throws(() => parseAddress("127.0.0.1:abc"), /invalid address/);
    assert.throws(() => parseAddress("[::1"), /invalid address/);
});

test("resolvePlatformPackage names the binding for the running platform", () => {
    const key = `${process.platform}-${process.arch}`;
    const supported = ["win32-x64", "darwin-x64", "darwin-arm64", "linux-x64"].includes(key);
    if (supported) {
        assert.equal(resolvePlatformPackage(), `@yydb/yyds-${key}`);
    } else {
        assert.throws(() => resolvePlatformPackage(), /unsupported platform/);
    }
});

test("native binding smoke test reports version and answers ping", () => {
    if (!isYydsNativeInstalled()) return;
    const native = loadYydsNative();
    assert.equal(typeof native.yydsVersion(), "string");
    assert.ok(native.yydsVersion().length > 0);
    assert.equal(native.ping(), "ok");
});
