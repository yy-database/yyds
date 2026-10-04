import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { createServer } from "node:net";
import test from "node:test";
import { fileURLToPath } from "node:url";

const cli = fileURLToPath(new URL("../bin/redis-cli.mjs", import.meta.url));

function takeCommand(buffer: Buffer) {
    if (buffer.length === 0) return null;
    if (buffer[0] !== 42) throw new Error("expected RESP array request");
    const arrayEnd = buffer.indexOf("\r\n");
    if (arrayEnd < 0) return null;
    const count = Number(buffer.toString("ascii", 1, arrayEnd));
    if (!Number.isInteger(count) || count < 1 || count > 1024)
        throw new Error("invalid RESP argument count");
    let offset = arrayEnd + 2;
    const args: Buffer[] = [];
    for (let index = 0; index < count; index += 1) {
        const headerEnd = buffer.indexOf("\r\n", offset);
        if (headerEnd < 0 || buffer[offset] !== 36) return null;
        const length = Number(buffer.toString("ascii", offset + 1, headerEnd));
        const start = headerEnd + 2;
        const end = start + length;
        if (buffer.length < end + 2) return null;
        args.push(buffer.subarray(start, end));
        offset = end + 2;
    }
    return { args, consumed: offset };
}

function runCli(args: string[]) {
    return new Promise<{ code: number | null; stdout: Buffer; stderr: Buffer }>(
        (resolve, reject) => {
            const child = spawn(process.execPath, ["--experimental-strip-types", cli, ...args]);
            const stdout: Buffer[] = [];
            const stderr: Buffer[] = [];
            const timer = setTimeout(() => child.kill(), 7000);
            child.stdout.on("data", (chunk: Buffer) => stdout.push(chunk));
            child.stderr.on("data", (chunk: Buffer) => stderr.push(chunk));
            child.on("error", reject);
            child.on("close", (code) => {
                clearTimeout(timer);
                resolve({ code, stdout: Buffer.concat(stdout), stderr: Buffer.concat(stderr) });
            });
        },
    );
}

test("@yyds/redis re-exports @yyds/yyds", async () => {
    const redis = await import("../src/index.ts");
    assert.equal(typeof redis.initWasm, "function");
});

test("@yyds/redis node entry re-exports native loader", async () => {
    const node = await import("../src/node.ts");
    assert.equal(typeof node.loadYydsNative, "function");
});

test("redis-cli sends RESP2 commands and prints binary replies", async (context) => {
    const requests: Buffer[][] = [];
    const server = createServer((socket) => {
        let pending = Buffer.alloc(0);
        socket.on("data", (chunk) => {
            pending = Buffer.concat([pending, chunk]);
            while (true) {
                const parsed = takeCommand(pending);
                if (parsed === null) return;
                pending = pending.subarray(parsed.consumed);
                requests.push(parsed.args);
                if (parsed.args[0].equals(Buffer.from("SELECT"))) {
                    socket.write("+OK\r\n");
                } else if (parsed.args[0].equals(Buffer.from("GET"))) {
                    socket.write(Buffer.from([36, 50, 13, 10, 0]));
                    setTimeout(() => socket.write(Buffer.from([255, 13, 10])), 5);
                } else if (parsed.args[0].equals(Buffer.from("INCRBY"))) {
                    socket.write(":9223372036854775807\r\n");
                } else {
                    socket.write("+OK\r\n");
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

    const set = await runCli([
        "-h",
        "127.0.0.1",
        "-p",
        String(address.port),
        "SET",
        "key",
        "value",
    ]);
    assert.equal(set.code, 0, set.stderr.toString());
    assert.equal(set.stdout.toString(), "OK\n");
    assert.deepEqual(requests[0], [Buffer.from("SET"), Buffer.from("key"), Buffer.from("value")]);

    const get = await runCli([
        "-h",
        "127.0.0.1",
        "-p",
        String(address.port),
        "-n",
        "1",
        "--raw",
        "GET",
        "key",
    ]);
    assert.equal(get.code, 0, get.stderr.toString());
    assert.deepEqual(get.stdout, Buffer.from([0, 255, 10]));
    assert.deepEqual(requests.slice(1), [
        [Buffer.from("SELECT"), Buffer.from("1")],
        [Buffer.from("GET"), Buffer.from("key")],
    ]);
    const integer = await runCli(["-p", String(address.port), "INCRBY", "key", "-1"]);
    assert.equal(integer.code, 0, integer.stderr.toString());
    assert.equal(integer.stdout.toString(), "9223372036854775807\n");
    assert.deepEqual(requests.at(-1), [
        Buffer.from("INCRBY"),
        Buffer.from("key"),
        Buffer.from("-1"),
    ]);
});
