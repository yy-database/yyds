import assert from "node:assert/strict";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import net from "node:net";
import test from "node:test";

import { YydsClient } from "../src/client.ts";
import { YydsError } from "../src/errors.ts";
import { isYydsNativeInstalled } from "../src/node/load.ts";
import { Yyds } from "../src/yyds.ts";

interface EnvelopeExpectation {
    code: string;
    phase: string;
    execution?: string;
    retry?: string;
}

function assertEnvelope(error: unknown, expected: EnvelopeExpectation): void {
    assert.ok(error instanceof YydsError, `expected a YydsError, got ${String(error)}`);
    assert.equal(error.product, "yyds");
    assert.equal(error.code, expected.code);
    assert.equal(error.phase, expected.phase);
    if (expected.execution !== undefined) assert.equal(error.execution, expected.execution);
    if (expected.retry !== undefined) assert.equal(error.retry, expected.retry);
}

function envelopeCheck(expected: EnvelopeExpectation): (error: unknown) => true {
    return (error: unknown): true => {
        assertEnvelope(error, expected);
        return true;
    };
}

function nodeOptions(directory: string) {
    return {
        nodeId: "node-a",
        storage: { directory },
        cluster: { mode: "bootstrap" as const, clusterId: "cluster-a" },
        peer: { endpoint: { host: "127.0.0.1", port: 9101 }, tls: {} },
    };
}

function joinOptions(directory: string) {
    const options = nodeOptions(directory);
    return {
        ...options,
        cluster: { mode: "join" as const, seeds: [{ host: "127.0.0.1", port: 9100 }] },
    };
}

test("Yyds constructor rejects incomplete or malformed configuration at construction time", () => {
    const invalid: [string, unknown][] = [
        ["non-object options", "yyds"],
        ["missing nodeId", { storage: { directory: "d" }, cluster: { mode: "bootstrap", clusterId: "c" }, peer: { endpoint: { host: "127.0.0.1", port: 9101 }, tls: {} } }],
        ["empty storage directory", { ...nodeOptions(""), }],
        ["missing cluster", { nodeId: "n", storage: { directory: "d" }, peer: { endpoint: { host: "127.0.0.1", port: 9101 }, tls: {} } }],
        ["unknown cluster mode", { ...nodeOptions("d"), cluster: { mode: "replicate" } }],
        ["bootstrap without clusterId", { ...nodeOptions("d"), cluster: { mode: "bootstrap" } }],
        ["join with empty seeds", { ...nodeOptions("d"), cluster: { mode: "join", seeds: [] } }],
        ["join with invalid seed", { ...nodeOptions("d"), cluster: { mode: "join", seeds: [{ host: "127.0.0.1", port: 0 }] } }],
        ["missing peer", { nodeId: "n", storage: { directory: "d" }, cluster: { mode: "bootstrap", clusterId: "c" } }],
        ["peer endpoint without port", { ...nodeOptions("d"), peer: { endpoint: { host: "127.0.0.1" }, tls: {} } }],
        ["peer tls without object", { ...nodeOptions("d"), peer: { endpoint: { host: "127.0.0.1", port: 9101 }, tls: "on" } }],
        ["peer tls ca with non-buffer entry", { ...nodeOptions("d"), peer: { endpoint: { host: "127.0.0.1", port: 9101 }, tls: { ca: ["pem"] } } }],
        ["non-positive sweep interval", { ...nodeOptions("d"), expirySweepIntervalMs: 0 }],
    ];
    for (const [label, options] of invalid) {
        assert.throws(
            () => new Yyds(options as never),
            envelopeCheck({ code: "yyds.configuration.invalid", phase: "configuration", execution: "not-sent", retry: "never" }),
            label,
        );
    }
});

test("Yyds join mode reports the missing native join primitive instead of pretending", async () => {
    const node = new Yyds(joinOptions(join(tmpdir(), "yyds-authoring-join")));
    assert.equal(node.state, "new");
    assert.equal(node.clusterId, null);
    await assert.rejects(node.open(), envelopeCheck({ code: "yyds.join.unavailable", phase: "open", execution: "not-sent", retry: "never" }));
    assert.equal(node.state, "failed");
    await assert.rejects(node.open(), envelopeCheck({ code: "yyds.state.invalid", phase: "open" }));
    const caps = node.capabilities();
    assert.equal(caps.cluster.qualified, false);
    assert.equal(caps.cluster.members, 0);
    assert.ok(caps.gaps.includes("cluster.join: no native join primitive; a bootstrap node stays single-member and is not cluster-qualified"));
});

test("Yyds capabilities report a single-node bootstrap without cluster qualification", () => {
    const node = new Yyds(nodeOptions(join(tmpdir(), "yyds-authoring-caps")));
    const caps = node.capabilities();
    assert.equal(caps.product, "yyds");
    assert.equal(caps.identity.nodeId, "node-a");
    assert.equal(caps.identity.clusterId, "cluster-a");
    assert.equal(caps.identity.mode, "bootstrap");
    assert.equal(caps.lifecycle.state, "new");
    assert.equal(caps.cluster.qualified, false);
    assert.equal(caps.cluster.replication, false);
    assert.equal(caps.cluster.failover, false);
    assert.equal(caps.cluster.reconfiguration, false);
    assert.equal(caps.cluster.members, 0);
    assert.equal(caps.peer.transport, false);
    assert.ok(caps.gaps.length >= 3);
});

test("YydsClient constructor rejects empty and malformed endpoint configuration", () => {
    const invalid: [string, unknown][] = [
        ["non-object options", undefined],
        ["missing endpoints", {}],
        ["empty endpoints", { endpoints: [] }],
        ["endpoint without host", { endpoints: [{ port: 9100 }] }],
        ["endpoint with zero port", { endpoints: [{ host: "127.0.0.1", port: 0 }] }],
        ["endpoint with out-of-range port", { endpoints: [{ host: "127.0.0.1", port: 70000 }] }],
        ["pool with min above max", { endpoints: [{ host: "127.0.0.1", port: 9100 }], pool: { min: 4, max: 2 } }],
        ["discovery with zero refresh interval", { endpoints: [{ host: "127.0.0.1", port: 9100 }], discovery: { refreshIntervalMs: 0 } }],
        ["negative connect timeout", { endpoints: [{ host: "127.0.0.1", port: 9100 }], connectTimeoutMs: -1 }],
        ["auth without password", { endpoints: [{ host: "127.0.0.1", port: 9100 }], auth: { username: "root" } }],
        ["tls with scalar identity", { endpoints: [{ host: "127.0.0.1", port: 9100 }], tls: { identity: "cert" } }],
    ];
    for (const [label, options] of invalid) {
        assert.throws(
            () => new YydsClient(options as never),
            envelopeCheck({ code: "yyds.configuration.invalid", phase: "configuration", execution: "not-sent", retry: "never" }),
            label,
        );
    }

    const client = new YydsClient({
        endpoints: [{ host: "127.0.0.1", port: 9100 }, { host: "127.0.0.1", port: 9101 }],
        auth: { username: "root", password: "secret" },
        pool: { max: 8 },
        discovery: { refreshIntervalMs: 5000, maxRedirects: 3 },
    });
    assert.equal(client.state, "new");
});

test("YydsClient reports the missing wire client primitive honestly across the lifecycle", async () => {
    const client = new YydsClient({ endpoints: [{ host: "127.0.0.1", port: 9100 }] });
    await assert.rejects(client.connect(), envelopeCheck({ code: "yyds.client.transport.unavailable", phase: "connect", execution: "not-sent", retry: "never" }));
    assert.equal(client.state, "failed");
    await assert.rejects(client.connect(), envelopeCheck({ code: "yyds.client.state.invalid", phase: "connect" }));
    await assert.rejects(client.table({ name: "User" }), envelopeCheck({ code: "yyds.client.not_connected", phase: "execute", execution: "not-sent" }));
    await assert.rejects(client.prepare({ query: 1 }), envelopeCheck({ code: "yyds.client.not_connected", phase: "execute" }));
    await assert.rejects(client.transaction(async () => "never"), envelopeCheck({ code: "yyds.client.not_connected", phase: "execute" }));

    const caps = client.capabilities();
    assert.equal(caps.product, "yyds");
    assert.deepEqual(caps.endpoints, [{ host: "127.0.0.1", port: 9100 }]);
    assert.equal(caps.dataPlane.table, false);
    assert.equal(caps.dataPlane.prepare, false);
    assert.equal(caps.dataPlane.transaction, false);
    assert.equal(caps.management.membershipChanges, false);
    assert.equal(caps.management.reconfiguration, false);
    assert.equal(caps.configured.auth, false);
    assert.ok(caps.gaps.length >= 3);

    await client.close();
    assert.equal(client.state, "closed");
    await client.close();
    await assert.rejects(client.connect(), envelopeCheck({ code: "yyds.client.state.invalid", phase: "connect" }));
});

test("author surface classes are exported from the node entry", async () => {
    const node = await import("../src/node/index.ts");
    assert.equal(typeof node.Yyds, "function");
    assert.equal(typeof node.YydsListener, "function");
    assert.equal(typeof node.YydsClient, "function");
    assert.equal(typeof node.YydsError, "function");
    assert.ok(Array.isArray(node.YYDS_SURFACE_GAPS));
});

test("Yyds bootstrap opens on native primitives and serves the loopback application port", async () => {
    if (!isYydsNativeInstalled()) return;
    const directory = await mkdtemp(join(tmpdir(), "yyds-authoring-"));
    const node = new Yyds(nodeOptions(directory));
    try {
        await Promise.all([node.open(), node.open()]);
        assert.equal(node.state, "open");
        await node.open();

        const caps = node.capabilities();
        assert.equal(caps.cluster.qualified, false);
        assert.equal(caps.cluster.members, 1);
        assert.equal(caps.application.wireProtocol, true);
        assert.equal(typeof caps.version, "string");

        await assert.rejects(
            node.listen({ endpoint: { host: "0.0.0.0", port: 0 } }),
            envelopeCheck({ code: "yyds.listen.host.unsupported", phase: "open" }),
        );
        await assert.rejects(
            node.listen({ authentication: { token: "t-secret" } }),
            envelopeCheck({ code: "yyds.listen.authentication.unavailable", phase: "open" }),
        );

        const listener = await node.listen({ endpoint: { host: "127.0.0.1", port: 0 } });
        assert.ok(listener.endpoint.port > 0);
        assert.equal(listener.endpoint.host, "127.0.0.1");
        assert.equal(listener.address, `127.0.0.1:${listener.endpoint.port}`);
        await new Promise<void>((resolve, reject) => {
            const socket = net.connect(listener.endpoint.port, "127.0.0.1");
            socket.once("connect", () => {
                socket.destroy();
                resolve();
            });
            socket.once("error", reject);
        });

        const second = await node.listen();
        assert.ok(second.endpoint.port > 0);
        await listener.close();
        assert.equal(listener.closed, true);
        await listener.close();
        await second.close();

        await node.close();
        assert.equal(node.state, "closed");
        await node.close();
        await assert.rejects(node.open(), envelopeCheck({ code: "yyds.state.invalid", phase: "open" }));
        await assert.rejects(node.listen(), envelopeCheck({ code: "yyds.state.invalid", phase: "open" }));
    } finally {
        await rm(directory, { recursive: true, force: true });
    }
});

test("Yyds close on a never-opened node is terminal and idempotent", async () => {
    const node = new Yyds(nodeOptions(join(tmpdir(), "yyds-authoring-idle")));
    await node.close();
    assert.equal(node.state, "closed");
    await node.close();
    await assert.rejects(node.open(), envelopeCheck({ code: "yyds.state.invalid", phase: "open" }));
});
