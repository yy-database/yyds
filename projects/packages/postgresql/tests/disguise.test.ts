import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import test from "node:test";
import { fileURLToPath } from "node:url";

const cli = fileURLToPath(new URL("../bin/psql.mjs", import.meta.url));

function backendMessage(tag: string, payload: Buffer) {
    const frame = Buffer.alloc(5 + payload.length);
    frame[0] = tag.charCodeAt(0);
    frame.writeUInt32BE(payload.length + 4, 1);
    payload.copy(frame, 5);
    return frame;
}

function runCli(args: string[]) {
    return new Promise<{ code: number | null; stdout: string; stderr: string }>(
        (resolve, reject) => {
            const child = spawn(process.execPath, ["--experimental-strip-types", cli, ...args]);
            let stdout = "";
            let stderr = "";
            const timer = setTimeout(() => child.kill(), 7000);
            child.stdout.setEncoding("utf8").on("data", (chunk) => (stdout += chunk));
            child.stderr.setEncoding("utf8").on("data", (chunk) => (stderr += chunk));
            child.on("error", reject);
            child.on("close", (code) => {
                clearTimeout(timer);
                resolve({ code, stdout, stderr });
            });
        },
    );
}

test("@yyds/postgresql re-exports @yyds/yyds", async () => {
    const postgresql = await import("../src/index.ts");
    assert.equal(typeof postgresql.initWasm, "function");
});

test("@yyds/postgresql node entry re-exports native loader", async () => {
    const node = await import("../src/node.ts");
    assert.equal(typeof node.loadYydsNative, "function");
});

test("psql sends protocol-v3 queries and displays server rows and errors", async (context) => {
    const queries: string[] = [];
    let startup = "";
    const server = createServer((socket) => {
        let pending = Buffer.alloc(0);
        let started = false;
        socket.on("data", (chunk) => {
            pending = Buffer.concat([pending, chunk]);
            if (!started) {
                if (pending.length < 4) return;
                const length = pending.readUInt32BE();
                if (length < 8 || pending.length < length) return;
                const startupPacket = pending.subarray(8, length);
                startup = startupPacket.toString("utf8");
                pending = pending.subarray(length);
                started = true;
                const parameter = Buffer.from("server_version\0YYDS-test\0");
                const auth = Buffer.from([0, 0, 0, 0]);
                socket.write(
                    Buffer.concat([
                        backendMessage("R", auth),
                        backendMessage("S", parameter),
                        backendMessage("K", Buffer.alloc(8)),
                        backendMessage("Z", Buffer.from("I")),
                    ]),
                );
            }
            while (pending.length >= 5) {
                const length = pending.readUInt32BE(1);
                if (pending.length < length + 1) return;
                const tag = String.fromCharCode(pending[0]);
                const payload = pending.subarray(5, length + 1);
                pending = pending.subarray(length + 1);
                assert.equal(tag, "Q");
                const sql = payload.toString("utf8").replace(/\0$/, "");
                queries.push(sql);
                if (sql === "SELECT 1") {
                    const description = Buffer.concat([
                        Buffer.from([0, 1]),
                        Buffer.from("answer\0"),
                        Buffer.alloc(18),
                    ]);
                    const row = Buffer.concat([Buffer.from([0, 1, 0, 0, 0, 1]), Buffer.from("1")]);
                    socket.write(
                        Buffer.concat([
                            backendMessage("T", description),
                            backendMessage("D", row),
                            backendMessage("C", Buffer.from("SELECT 1\0")),
                            backendMessage("Z", Buffer.from("I")),
                        ]),
                    );
                } else if (sql === "SELECT 1; SELECT 2") {
                    const first = Buffer.concat([Buffer.from([0, 1]), Buffer.from("one\0"), Buffer.alloc(18)]);
                    const second = Buffer.concat([Buffer.from([0, 1]), Buffer.from("two\0"), Buffer.alloc(18)]);
                    const row = (value: string) => Buffer.concat([Buffer.from([0, 1, 0, 0, 0, value.length]), Buffer.from(value)]);
                    socket.write(Buffer.concat([
                        backendMessage("T", first), backendMessage("D", row("1")), backendMessage("C", Buffer.from("SELECT 1\0")),
                        backendMessage("T", second), backendMessage("D", row("2")), backendMessage("C", Buffer.from("SELECT 1\0")),
                        backendMessage("Z", Buffer.from("I")),
                    ]));
                } else if (sql === "UPDATE samples SET value = 1") {
                    socket.write(Buffer.concat([backendMessage("C", Buffer.from("UPDATE 1\0")), backendMessage("Z", Buffer.from("I"))]));
                } else {
                    const error = Buffer.from("SERROR\0C42601\0Mserver syntax error\0\0");
                    socket.write(
                        Buffer.concat([
                            backendMessage("E", error),
                            backendMessage("Z", Buffer.from("I")),
                        ]),
                    );
                }
            }
        });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    context.after(
        () =>
            new Promise<void>((resolve, reject) =>
                server.close((error) => (error ? reject(error) : resolve())),
            ),
    );
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const args = ["-h", "127.0.0.1", "-p", String(address.port), "-U", "alice", "-d", "analytics"];

    const result = await runCli([...args, "-c", "SELECT 1"]);
    assert.equal(result.code, 0, result.stderr);
    assert.equal(result.stdout, "answer\n1\n");
    assert.match(startup, /user\0alice\0database\0analytics\0/);

    const failure = await runCli([...args, "-c", "SELECT FROM"]);
    assert.equal(failure.code, 1);
    assert.match(failure.stderr, /42601: server syntax error/);
    assert.deepEqual(queries, ["SELECT 1", "SELECT FROM"]);

    const multiple = await runCli([...args, "-c", "SELECT 1; SELECT 2"]);
    assert.equal(multiple.code, 0, multiple.stderr);
    assert.equal(multiple.stdout, "one\n1\ntwo\n2\n");

    const tuplesOnly = await runCli([...args, "-t", "-c", "UPDATE samples SET value = 1"]);
    assert.equal(tuplesOnly.code, 0, tuplesOnly.stderr);
    assert.equal(tuplesOnly.stdout, "");
});

test("psql applies field and NULL output settings to text rows", async (context) => {
    const server = createServer((socket) => {
        let pending = Buffer.alloc(0);
        let started = false;
        socket.on("data", (chunk) => {
            pending = Buffer.concat([pending, chunk]);
            if (!started) {
                if (pending.length < 4) return;
                const length = pending.readUInt32BE();
                if (pending.length < length) return;
                pending = pending.subarray(length);
                started = true;
                socket.write(Buffer.concat([backendMessage("R", Buffer.from([0, 0, 0, 0])), backendMessage("Z", Buffer.from("I"))]));
            }
            while (pending.length >= 5) {
                const length = pending.readUInt32BE(1);
                if (pending.length < length + 1) return;
                pending = pending.subarray(length + 1);
                const description = Buffer.concat([Buffer.from([0, 2]), Buffer.from("left\0"), Buffer.alloc(18), Buffer.from("right\0"), Buffer.alloc(18)]);
                const row = Buffer.from([0, 2, 0, 0, 0, 1, 65, 255, 255, 255, 255]);
                socket.write(Buffer.concat([backendMessage("T", description), backendMessage("D", row), backendMessage("C", Buffer.from("SELECT 1\0")), backendMessage("Z", Buffer.from("I"))]));
            }
        });
    });
    await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
    context.after(() => new Promise<void>((resolve, reject) => server.close((error) => (error ? reject(error) : resolve()))));
    const address = server.address();
    assert.ok(address && typeof address === "object");
    const result = await runCli(["-p", String(address.port), "-F", ",", "-P", "null=(none)", "-c", "SELECT 1"]);
    assert.equal(result.code, 0, result.stderr);
    assert.equal(result.stdout, "left,right\nA,(none)\n");
});
