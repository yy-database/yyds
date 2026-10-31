import assert from "node:assert/strict";
import { createConnection, type Socket } from "node:net";
import test, { after, before } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";

import { startYydsNode, type YydsNode } from "@yydb/yyds-node-harness";

type Reply =
    | { kind: "simple"; value: string }
    | { kind: "error"; value: string }
    | { kind: "integer"; value: number }
    | { kind: "bulk"; value: string | null }
    | { kind: "array"; value: Reply[] };

function encode(...args: string[]): string {
    let frame = `*${args.length}\r\n`;
    for (const argument of args) {
        frame += `$${Buffer.byteLength(argument)}\r\n${argument}\r\n`;
    }
    return frame;
}

function tryParse(buffer: Buffer): { reply: Reply; consumed: number } | null {
    if (buffer.length === 0) return null;
    const lineEnd = buffer.indexOf("\r\n");
    const type = String.fromCharCode(buffer[0]);
    if (type === "+" || type === "-" || type === ":") {
        if (lineEnd < 0) return null;
        const payload = buffer.subarray(1, lineEnd).toString("utf8");
        const consumed = lineEnd + 2;
        if (type === "+") return { reply: { kind: "simple", value: payload }, consumed };
        if (type === "-") return { reply: { kind: "error", value: payload }, consumed };
        return { reply: { kind: "integer", value: Number(payload) }, consumed };
    }
    if (type === "$") {
        if (lineEnd < 0) return null;
        const length = Number(buffer.subarray(1, lineEnd).toString("ascii"));
        if (length === -1) return { reply: { kind: "bulk", value: null }, consumed: lineEnd + 2 };
        const start = lineEnd + 2;
        const end = start + length;
        if (buffer.length < end + 2) return null;
        return { reply: { kind: "bulk", value: buffer.subarray(start, end).toString("utf8") }, consumed: end + 2 };
    }
    if (type === "*") {
        if (lineEnd < 0) return null;
        const count = Number(buffer.subarray(1, lineEnd).toString("ascii"));
        let offset = lineEnd + 2;
        const elements: Reply[] = [];
        for (let index = 0; index < count; index += 1) {
            const parsed = tryParse(buffer.subarray(offset));
            if (parsed === null) return null;
            elements.push(parsed.reply);
            offset += parsed.consumed;
        }
        return { reply: { kind: "array", value: elements }, consumed: offset };
    }
    return null;
}

class RawClient {
    private buffer: Buffer = Buffer.alloc(0);
    private waiters: Array<{ resolve: (reply: Reply) => void; reject: (reason: Error) => void }> = [];
    private closed = false;
    private closeReason: Error | null = null;

    private readonly socket: Socket;

    private constructor(socket: Socket) {
        this.socket = socket;
        socket.on("error", () => undefined);
        socket.on("data", (chunk: Buffer) => {
            this.buffer = Buffer.concat([this.buffer, chunk]);
            this.drain();
        });
        socket.on("close", () => {
            this.closed = true;
            this.closeReason = new Error("socket closed by peer");
            while (this.waiters.length > 0) this.waiters.shift()?.reject(new Error("socket closed"));
        });
    }

    static connect(host: string, port: number): Promise<RawClient> {
        return new Promise((resolveConnect, rejectConnect) => {
            const socket = createConnection({ host, port });
            socket.once("connect", () => resolveConnect(new RawClient(socket)));
            socket.once("error", rejectConnect);
        });
    }

    private drain(): void {
        while (this.waiters.length > 0) {
            const parsed = tryParse(this.buffer);
            if (parsed === null) return;
            this.buffer = this.buffer.subarray(parsed.consumed);
            this.waiters.shift()?.resolve(parsed.reply);
        }
    }

    command(...args: string[]): Promise<Reply> {
        return new Promise((resolveCommand, rejectCommand) => {
            if (this.closed) {
                rejectCommand(this.closeReason ?? new Error("socket closed"));
                return;
            }
            this.waiters.push({ resolve: resolveCommand, reject: rejectCommand });
            this.socket.write(encode(...args));
            this.drain();
        });
    }

    sendRaw(bytes: string): void {
        this.socket.write(bytes);
    }

    nextReply(): Promise<Reply> {
        return new Promise((resolveReply, rejectReply) => {
            if (this.closed) {
                rejectReply(this.closeReason ?? new Error("socket closed"));
                return;
            }
            this.waiters.push({ resolve: resolveReply, reject: rejectReply });
            this.drain();
        });
    }

    closedPromise(): Promise<void> {
        if (this.closed) return Promise.resolve();
        return new Promise((resolveClose) => this.socket.once("close", resolveClose));
    }

    destroy(): void {
        this.socket.destroy();
    }
}

const expectSimple = (reply: Reply, value: string) => assert.deepEqual(reply, { kind: "simple", value });
const expectError = (reply: Reply, value: string) => assert.deepEqual(reply, { kind: "error", value });
const expectInteger = (reply: Reply, value: number) => assert.deepEqual(reply, { kind: "integer", value });
const expectBulk = (reply: Reply, value: string | null) => assert.deepEqual(reply, { kind: "bulk", value });

let node: YydsNode;

before(async () => {
    node = await startYydsNode();
});

after(async () => {
    await node.stop();
});

test("PING, ECHO and QUIT follow RESP2 arity rules", async () => {
    const client = await RawClient.connect(node.redis.host, node.redis.port);
    try {
        expectSimple(await client.command("PING"), "PONG");
        expectBulk(await client.command("PING", "hi"), "hi");
        expectError(await client.command("PING", "a", "b"), "ERR wrong number of arguments for 'ping' command");
        expectBulk(await client.command("echo", "case-insensitive"), "case-insensitive");
        expectError(await client.command("ECHO"), "ERR wrong number of arguments for 'echo' command");
        expectError(await client.command("ECHO", "a", "b"), "ERR wrong number of arguments for 'echo' command");
        expectSimple(await client.command("QUIT"), "OK");
        await client.closedPromise();
    } finally {
        client.destroy();
    }
});

test("SELECT rejects non-decimal and out-of-range database indexes", async () => {
    const client = await RawClient.connect(node.redis.host, node.redis.port);
    try {
        expectError(await client.command("SELECT", "abc"), "ERR DB index is out of range");
        expectError(await client.command("SELECT", "-1"), "ERR DB index is out of range");
        expectError(await client.command("SELECT", "16"), "ERR DB index is out of range");
        expectError(await client.command("SELECT", "1", "2"), "ERR wrong number of arguments for 'select' command");
        expectSimple(await client.command("SELECT", "15"), "OK");
        expectSimple(await client.command("SELECT", "0"), "OK");
    } finally {
        client.destroy();
    }
});

test("EXPIRE and TTL cover missing keys, bad integers and zero TTL deletes", async () => {
    const client = await RawClient.connect(node.redis.host, node.redis.port);
    try {
        expectInteger(await client.command("EXPIRE", "exp-missing", "100"), 0);
        expectError(await client.command("EXPIRE", "exp-a"), "ERR wrong number of arguments for 'expire' command");
        expectError(await client.command("EXPIRE", "exp-a", "1", "2"), "ERR wrong number of arguments for 'expire' command");
        expectError(await client.command("EXPIRE", "exp-a", "+5"), "ERR value is not an integer or out of range");
        expectError(await client.command("EXPIRE", "exp-a", "05"), "ERR value is not an integer or out of range");
        expectError(await client.command("EXPIRE", "exp-a", "abc"), "ERR value is not an integer or out of range");

        expectSimple(await client.command("SET", "exp-a", "1"), "OK");
        expectInteger(await client.command("EXPIRE", "exp-a", "100"), 1);
        const ttl = await client.command("TTL", "exp-a");
        assert.ok(ttl.kind === "integer" && ttl.value > 0 && ttl.value <= 100, `unexpected TTL ${JSON.stringify(ttl)}`);
        expectError(await client.command("TTL", "exp-a", "extra"), "ERR wrong number of arguments for 'ttl' command");

        expectInteger(await client.command("TTL", "exp-missing"), -2);
        expectSimple(await client.command("SET", "exp-b", "1"), "OK");
        expectInteger(await client.command("TTL", "exp-b"), -1);
        expectInteger(await client.command("PTTL", "exp-b"), -1);

        // A zero or negative TTL behaves like DEL and reports success.
        expectInteger(await client.command("EXPIRE", "exp-b", "0"), 1);
        expectBulk(await client.command("GET", "exp-b"), null);
        expectInteger(await client.command("TTL", "exp-b"), -2);
    } finally {
        client.destroy();
    }
});

test("PEXPIRE purges the key once the deadline passes", async () => {
    const client = await RawClient.connect(node.redis.host, node.redis.port);
    try {
        expectSimple(await client.command("SET", "pexp", "v"), "OK");
        expectInteger(await client.command("PEXPIRE", "pexp", "60"), 1);
        const before = await client.command("PTTL", "pexp");
        assert.ok(before.kind === "integer" && before.value > 0 && before.value <= 60);
        await sleep(120);
        expectInteger(await client.command("PTTL", "pexp"), -2);
        expectBulk(await client.command("GET", "pexp"), null);
    } finally {
        client.destroy();
    }
});

test("TYPE, STRLEN, APPEND, EXISTS and MGET report value shapes", async () => {
    const client = await RawClient.connect(node.redis.host, node.redis.port);
    try {
        expectSimple(await client.command("TYPE", "shape-missing"), "none");
        expectSimple(await client.command("SET", "shape-a", "hello"), "OK");
        expectSimple(await client.command("TYPE", "shape-a"), "string");
        expectInteger(await client.command("STRLEN", "shape-missing"), 0);
        expectInteger(await client.command("STRLEN", "shape-a"), 5);

        expectInteger(await client.command("APPEND", "app", "ab"), 2);
        expectInteger(await client.command("APPEND", "app", "cd"), 4);
        expectBulk(await client.command("GET", "app"), "abcd");
        expectError(await client.command("APPEND", "app"), "ERR wrong number of arguments for 'append' command");

        expectInteger(await client.command("EXISTS", "shape-a", "shape-missing", "app"), 2);
        expectError(await client.command("EXISTS"), "ERR wrong number of arguments for 'exists' command");

        expectDeepEqualArray(await client.command("MGET", "shape-a", "mget-missing"), [
            { kind: "bulk", value: "hello" },
            { kind: "bulk", value: null },
        ]);
        expectError(await client.command("MGET"), "ERR wrong number of arguments");

        expectError(await client.command("LPUSH", "list", "v"), "ERR unsupported command");
    } finally {
        client.destroy();
    }
});

function expectDeepEqualArray(reply: Reply, elements: Reply[]): void {
    assert.ok(reply.kind === "array");
    assert.deepEqual(reply.value, elements);
}

test("SET NX and XX map to conditional puts while other options are refused", async () => {
    const client = await RawClient.connect(node.redis.host, node.redis.port);
    try {
        expectSimple(await client.command("SET", "nx", "first", "NX"), "OK");
        expectBulk(await client.command("SET", "nx", "second", "NX"), null);
        expectBulk(await client.command("GET", "nx"), "first");

        expectBulk(await client.command("SET", "xx", "value", "XX"), null);
        expectSimple(await client.command("SET", "xx", "value"), "OK");
        expectSimple(await client.command("SET", "xx", "other", "xx"), "OK");

        expectError(await client.command("SET", "opt", "v", "EX", "10"), "ERR unsupported SET options");
        expectError(await client.command("SET", "opt", "v", "NX", "EX", "10"), "ERR unsupported SET options");
        expectError(await client.command("SET", "only-key"), "ERR wrong number of arguments");
    } finally {
        client.destroy();
    }
});

test("INCR family enforces canonical integers and executor errors stay errors", async () => {
    const client = await RawClient.connect(node.redis.host, node.redis.port);
    try {
        expectInteger(await client.command("INCR", "num"), 1);
        expectInteger(await client.command("INCRBY", "num", "5"), 6);
        expectInteger(await client.command("DECR", "num"), 5);
        expectInteger(await client.command("DECRBY", "num", "2"), 3);
        expectInteger(await client.command("DECRBY", "num", "-1"), 4);

        expectError(await client.command("INCRBY", "num", "+5"), "ERR value is not an integer or out of range");
        expectError(await client.command("INCRBY", "num", "05"), "ERR value is not an integer or out of range");
        expectError(await client.command("INCRBY", "num", "abc"), "ERR value is not an integer or out of range");
        expectError(await client.command("INCRBY", "num"), "ERR wrong number of arguments");
        expectError(await client.command("INCR"), "ERR wrong number of arguments");

        expectSimple(await client.command("SET", "num-text", "abc"), "OK");
        const failed = await client.command("INCR", "num-text");
        assert.equal(failed.kind, "error");
    } finally {
        client.destroy();
    }
});

test("GETSET and GETDEL return the previous value", async () => {
    const client = await RawClient.connect(node.redis.host, node.redis.port);
    try {
        expectSimple(await client.command("SET", "swap", "old"), "OK");
        expectBulk(await client.command("GETSET", "swap", "new"), "old");
        expectBulk(await client.command("GET", "swap"), "new");
        expectBulk(await client.command("GETSET", "swap-fresh", "v"), null);
        expectBulk(await client.command("GET", "swap-fresh"), "v");

        expectBulk(await client.command("GETDEL", "swap"), "new");
        expectBulk(await client.command("GET", "swap"), null);
        expectBulk(await client.command("GETDEL", "swap"), null);
        expectError(await client.command("GETSET", "only-key"), "ERR wrong number of arguments");
    } finally {
        client.destroy();
    }
});

test("MULTI queues with syntax checks, EXEC replays and DISCARD drops", async () => {
    const client = await RawClient.connect(node.redis.host, node.redis.port);
    try {
        expectError(await client.command("EXEC"), "ERR EXEC without MULTI");
        expectError(await client.command("DISCARD"), "ERR DISCARD without MULTI");

        expectSimple(await client.command("MULTI"), "OK");
        expectError(await client.command("MULTI"), "ERR MULTI calls can not be nested");
        expectSimple(await client.command("SET", "multi-a", "1"), "QUEUED");
        expectSimple(await client.command("INCR", "multi-counter"), "QUEUED");
        // Syntax errors surface at queue time, before EXEC replays anything.
        expectError(await client.command("ECHO"), "ERR wrong number of arguments for 'echo' command");
        expectError(await client.command("FOOBAR", "x"), "ERR unsupported command");
        expectDeepEqualArray(await client.command("EXEC"), [
            { kind: "simple", value: "OK" },
            { kind: "integer", value: 1 },
        ]);
        expectBulk(await client.command("GET", "multi-a"), "1");
        expectBulk(await client.command("GET", "multi-counter"), "1");

        // DISCARD drops the queue without replaying anything.
        expectSimple(await client.command("MULTI"), "OK");
        expectSimple(await client.command("SET", "multi-drop", "x"), "QUEUED");
        expectSimple(await client.command("DISCARD"), "OK");
        expectBulk(await client.command("GET", "multi-drop"), null);

        // An empty transaction replies with a zero-length array.
        expectSimple(await client.command("MULTI"), "OK");
        expectDeepEqualArray(await client.command("EXEC"), []);
    } finally {
        client.destroy();
    }
});

test("inline commands and empty frames are protocol errors that close the connection", async () => {
    const inline = await RawClient.connect(node.redis.host, node.redis.port);
    inline.sendRaw("PING\r\n");
    expectError(await inline.nextReply(), "ERR Protocol error or request limit exceeded");
    await inline.closedPromise();

    const empty = await RawClient.connect(node.redis.host, node.redis.port);
    empty.sendRaw("*0\r\n");
    expectError(await empty.nextReply(), "ERR Protocol error or request limit exceeded");
    await empty.closedPromise();

    const badTerminator = await RawClient.connect(node.redis.host, node.redis.port);
    badTerminator.sendRaw("*1\r\n$3\r\nPING\r\n");
    expectError(await badTerminator.nextReply(), "ERR Protocol error or request limit exceeded");
    await badTerminator.closedPromise();
});
