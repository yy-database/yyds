import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import test from "node:test";
import { fileURLToPath } from "node:url";

const cli = fileURLToPath(new URL("../bin/mysql.mjs", import.meta.url));

function packet(sequence: number, payload: Buffer) {
    return Buffer.concat([
        Buffer.from([
            payload.length & 255,
            (payload.length >>> 8) & 255,
            (payload.length >>> 16) & 255,
            sequence,
        ]),
        payload,
    ]);
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

function capabilities() {
    return (
        0x00000001 |
        0x00000004 |
        0x00000008 |
        0x00000200 |
        0x00002000 |
        0x00008000 |
        0x00020000 |
        0x00080000
    );
}

function handshake() {
    const caps = capabilities();
    const payload = Buffer.concat([
        Buffer.from([10]),
        Buffer.from("8.0.36-yyds\0"),
        Buffer.from([1, 0, 0, 0]),
        Buffer.from("yydsseed\0"),
        Buffer.from([
            caps & 255,
            (caps >>> 8) & 255,
            45,
            2,
            0,
            (caps >>> 16) & 255,
            (caps >>> 24) & 255,
            21,
        ]),
        Buffer.alloc(10),
        Buffer.from("yydsseed1234\0mysql_native_password\0"),
    ]);
    return packet(0, payload);
}

function takePacket(buffer: Buffer) {
    if (buffer.length < 4) return null;
    const length = buffer[0] | (buffer[1] << 8) | (buffer[2] << 16);
    if (buffer.length < length + 4) return null;
    return { sequence: buffer[3], payload: buffer.subarray(4, length + 4), consumed: length + 4 };
}

test("@yyds/mysql re-exports @yyds/yyds", async () => {
    const mysql = await import("../src/index.ts");
    assert.equal(typeof mysql.initWasm, "function");
});

test("@yyds/mysql node entry re-exports native loader", async () => {
    const node = await import("../src/node.ts");
    assert.equal(typeof node.loadYydsNative, "function");
});

test("mysql client handshakes with the server and reports actual SQL errors", async (context) => {
    const statements: string[] = [];
    let username = "";
    let database = "";
    const server = createServer((socket) => {
        let pending = Buffer.alloc(0);
        let authenticated = false;
        socket.write(handshake());
        socket.on("data", (chunk) => {
            pending = Buffer.concat([pending, chunk]);
            while (true) {
                const parsed = takePacket(pending);
                if (parsed === null) return;
                pending = pending.subarray(parsed.consumed);
                if (!authenticated) {
                    assert.equal(parsed.sequence, 1);
                    const flags = parsed.payload.readUInt32LE(0);
                    let offset = 32;
                    const userEnd = parsed.payload.indexOf(0, offset);
                    username = parsed.payload.toString("utf8", offset, userEnd);
                    offset = userEnd + 1;
                    const authLength = parsed.payload[offset++];
                    assert.equal(authLength, 0);
                    offset += authLength;
                    if (flags & 8) {
                        const databaseEnd = parsed.payload.indexOf(0, offset);
                        database = parsed.payload.toString("utf8", offset, databaseEnd);
                    }
                    authenticated = true;
                    socket.write(packet(2, Buffer.from([0, 0, 0, 2, 0, 0, 0])));
                } else {
                    assert.equal(parsed.sequence, 0);
                    assert.equal(parsed.payload[0], 3);
                    const sql = parsed.payload.toString("utf8", 1);
                    statements.push(sql);
                    if (sql.toLowerCase().startsWith("set names"))
                        socket.write(packet(1, Buffer.from([0, 0, 0, 2, 0, 0, 0])));
                    else {
                        const error = Buffer.concat([
                            Buffer.from([255, 0xd3, 0x04, 35]),
                            Buffer.from("42000"),
                            Buffer.from("YYDS MySQL SQL execution is not implemented"),
                        ]);
                        socket.write(packet(1, error));
                    }
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
    const args = ["-h", "127.0.0.1", "-P", String(address.port), "-u", "alice", "-D", "analytics"];

    const session = await runCli([...args, "-e", "SET NAMES utf8mb4"]);
    assert.equal(session.code, 0, session.stderr);
    assert.equal(session.stdout, "Query OK\n");
    assert.equal(username, "alice");
    assert.equal(database, "analytics");

    const failure = await runCli([...args, "-e", "SELECT 1"]);
    assert.equal(failure.code, 1);
    assert.match(failure.stderr, /1235 \(42000\): YYDS MySQL SQL execution is not implemented/);
    assert.deepEqual(statements, ["SET NAMES utf8mb4", "SELECT 1"]);
});
