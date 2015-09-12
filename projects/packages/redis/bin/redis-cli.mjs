#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { createConnection } from "node:net";

const { version } = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
const maxRequestBytes = 1024 * 1024;
const maxReplyBytes = 8 * 1024 * 1024;

function parseValue(buffer, offset = 0, depth = 0) {
    if (depth > 32 || offset >= buffer.length) return null;
    const marker = buffer[offset];
    const lineEnd = buffer.indexOf("\r\n", offset + 1);
    if (lineEnd < 0) return null;
    const line = buffer.toString("utf8", offset + 1, lineEnd);
    const next = lineEnd + 2;
    if (marker === 43 || marker === 45)
        return { value: { kind: marker === 43 ? "string" : "error", value: line }, next };
    if (marker === 58) {
        if (!/^-?\d+$/.test(line)) throw new Error("invalid RESP integer response");
        return { value: { kind: "integer", value: Number(line) }, next };
    }
    if (marker === 36) {
        const length = Number(line);
        if (!Number.isSafeInteger(length) || length < -1)
            throw new Error("invalid RESP bulk length");
        if (length === -1) return { value: { kind: "null" }, next };
        if (length > maxReplyBytes) throw new Error("RESP bulk reply exceeds byte limit");
        const end = next + length;
        if (buffer.length < end + 2) return null;
        if (buffer[end] !== 13 || buffer[end + 1] !== 10)
            throw new Error("invalid RESP bulk terminator");
        return { value: { kind: "bulk", value: buffer.subarray(next, end) }, next: end + 2 };
    }
    if (marker === 42) {
        const count = Number(line);
        if (!Number.isSafeInteger(count) || count < -1 || count > 1024)
            throw new Error("invalid RESP array length");
        if (count === -1) return { value: { kind: "null" }, next };
        const values = [];
        let cursor = next;
        for (let index = 0; index < count; index += 1) {
            const parsed = parseValue(buffer, cursor, depth + 1);
            if (parsed === null) return null;
            values.push(parsed.value);
            cursor = parsed.next;
        }
        return { value: { kind: "array", value: values }, next: cursor };
    }
    throw new Error("unsupported RESP2 response type");
}

function encodeCommand(args) {
    const values = args.map((value) => Buffer.from(value, "utf8"));
    const encoded = Buffer.concat([
        Buffer.from(`*${values.length}\r\n`),
        ...values.flatMap((value) => [
            Buffer.from(`$${value.length}\r\n`),
            value,
            Buffer.from("\r\n"),
        ]),
    ]);
    if (encoded.length > maxRequestBytes) throw new Error("Redis command exceeds byte limit");
    return encoded;
}

function writeValue(value, raw) {
    if (value.kind === "error") throw new Error(value.value);
    if (value.kind === "null") {
        process.stdout.write("(nil)\n");
        return;
    }
    if (value.kind === "array") {
        for (const item of value.value) writeValue(item, raw);
        return;
    }
    if (value.kind === "bulk") {
        process.stdout.write(raw ? value.value : value.value.toString("utf8"));
        process.stdout.write("\n");
        return;
    }
    process.stdout.write(`${value.value}\n`);
}

function parseArguments(args) {
    const options = { host: "127.0.0.1", port: 6379, database: 0, raw: false };
    const command = [];
    for (let index = 0; index < args.length; index += 1) {
        const arg = args[index];
        if (arg === "--raw") options.raw = true;
        else if (["-h", "--host", "-p", "--port", "-n", "--db"].includes(arg)) {
            const value = args[++index];
            if (value === undefined) throw new Error(`missing value for ${arg}`);
            if (arg === "-h" || arg === "--host") options.host = value;
            else {
                const parsed = Number(value);
                const minimum = arg === "-n" || arg === "--db" ? 0 : 1;
                if (
                    !Number.isInteger(parsed) ||
                    parsed < minimum ||
                    parsed > (arg === "-n" || arg === "--db" ? 15 : 65535)
                ) {
                    throw new Error(`invalid value for ${arg}`);
                }
                if (arg === "-p" || arg === "--port") options.port = parsed;
                else options.database = parsed;
            }
        } else if (arg.startsWith("-")) throw new Error(`unsupported option '${arg}'`);
        else command.push(arg);
    }
    if (command.length === 0) command.push("PING");
    return { options, command };
}

function exchange(options, commands) {
    return new Promise((resolve, reject) => {
        const socket = createConnection({ host: options.host, port: options.port });
        let pending = Buffer.alloc(0);
        let commandIndex = 0;
        let timer;
        const allCommands =
            options.database === 0 ? [commands] : [["SELECT", String(options.database)], commands];
        const finish = (error, value) => {
            clearTimeout(timer);
            socket.destroy();
            if (error) reject(error);
            else resolve(value);
        };
        timer = setTimeout(() => finish(new Error("Redis connection timed out")), 5000);
        socket.on("connect", () => socket.write(encodeCommand(allCommands[commandIndex])));
        socket.on("data", (chunk) => {
            try {
                pending = Buffer.concat([pending, chunk]);
                if (pending.length > maxReplyBytes)
                    throw new Error("RESP reply exceeds byte limit");
                const parsed = parseValue(pending);
                if (parsed === null) return;
                pending = pending.subarray(parsed.next);
                if (commandIndex === 0 && options.database !== 0) {
                    if (parsed.value.kind === "error") throw new Error(parsed.value.value);
                    if (parsed.value.kind !== "string" || parsed.value.value !== "OK") {
                        throw new Error("Redis SELECT did not return OK");
                    }
                    commandIndex += 1;
                    socket.write(encodeCommand(allCommands[commandIndex]));
                    return;
                }
                finish(null, parsed.value);
            } catch (error) {
                finish(error);
            }
        });
        socket.on("error", (error) => finish(error));
    });
}

async function main(args) {
    if (args.length === 1 && ["--version", "-V"].includes(args[0])) {
        console.log(`redis-cli ${version} (YYDS RESP2 client)`);
        return;
    }
    if (args.length === 1 && args[0] === "--help") {
        console.log("Usage: redis-cli [-h HOST] [-p PORT] [-n DB] [--raw] [COMMAND [ARG ...]]");
        return;
    }
    const { options, command } = parseArguments(args);
    writeValue(await exchange(options, command), options.raw);
}

main(process.argv.slice(2)).catch((error) => {
    console.error(`@yyds/redis: ${error.message}`);
    process.exitCode = 1;
});
