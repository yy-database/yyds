import assert from "node:assert/strict";
import { createConnection, type Socket } from "node:net";

export type Reply =
    | { kind: "simple"; value: string }
    | { kind: "error"; value: string }
    | { kind: "integer"; value: number }
    | { kind: "bulk"; value: string | null }
    | { kind: "array"; value: Reply[] };

export function encode(...args: string[]): string {
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

/** Minimal RESP2 client that keeps the wire visible for protocol-level assertions. */
export class RawClient {
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

export const expectSimple = (reply: Reply, value: string) => assert.deepEqual(reply, { kind: "simple", value });
export const expectError = (reply: Reply, value: string) => assert.deepEqual(reply, { kind: "error", value });
export const expectInteger = (reply: Reply, value: number) => assert.deepEqual(reply, { kind: "integer", value });
export const expectBulk = (reply: Reply, value: string | null) => assert.deepEqual(reply, { kind: "bulk", value });

export function expectArray(reply: Reply, elements: Reply[]): void {
    assert.ok(reply.kind === "array");
    assert.deepEqual(reply.value, elements);
}
