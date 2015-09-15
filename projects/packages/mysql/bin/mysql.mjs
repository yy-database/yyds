#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { createConnection } from "node:net";

const { version } = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
const maxPacketBytes = 1024 * 1024;
const CLIENT_LONG_PASSWORD = 0x00000001;
const CLIENT_CONNECT_WITH_DB = 0x00000008;
const CLIENT_PROTOCOL_41 = 0x00000200;
const CLIENT_TRANSACTIONS = 0x00002000;
const CLIENT_SECURE_CONNECTION = 0x00008000;
const CLIENT_PLUGIN_AUTH = 0x00080000;

function packet(sequence, payload) {
    if (payload.length > 0xffffff) throw new Error("MySQL packet exceeds protocol limit");
    const header = Buffer.from([
        payload.length & 255,
        (payload.length >>> 8) & 255,
        (payload.length >>> 16) & 255,
        sequence,
    ]);
    return Buffer.concat([header, payload]);
}

function createReader(socket) {
    let pending = Buffer.alloc(0);
    let wake;
    let failure;
    socket.on("data", (chunk) => {
        pending = Buffer.concat([pending, chunk]);
        wake?.();
    });
    socket.on("error", (error) => {
        failure = error;
        wake?.();
    });
    return async function read(expectedSequence) {
        while (pending.length < 4) {
            if (failure) throw failure;
            await new Promise((resolve) => {
                wake = resolve;
            });
        }
        const length = pending[0] | (pending[1] << 8) | (pending[2] << 16);
        if (length > maxPacketBytes) throw new Error("MySQL packet exceeds byte limit");
        while (pending.length < length + 4) {
            if (failure) throw failure;
            await new Promise((resolve) => {
                wake = resolve;
            });
        }
        const sequence = pending[3];
        const payload = pending.subarray(4, length + 4);
        pending = pending.subarray(length + 4);
        if (sequence !== expectedSequence)
            throw new Error(`unexpected MySQL packet sequence ${sequence}`);
        return payload;
    };
}

function nulTerminated(payload, offset) {
    const end = payload.indexOf(0, offset);
    if (end < 0) throw new Error("malformed MySQL handshake string");
    return { value: payload.toString("utf8", offset, end), next: end + 1 };
}

function parseHandshake(payload) {
    if (payload.length < 34 || payload[0] !== 10) throw new Error("unsupported MySQL handshake");
    let offset = nulTerminated(payload, 1).next + 4;
    offset += 8;
    if (payload[offset++] !== 0 || offset + 2 > payload.length)
        throw new Error("malformed MySQL handshake");
    let capabilities = payload.readUInt16LE(offset);
    offset += 2;
    if (offset >= payload.length) throw new Error("truncated MySQL capabilities");
    offset += 1 + 2;
    if (offset + 2 > payload.length) throw new Error("truncated MySQL extended capabilities");
    capabilities |= payload.readUInt16LE(offset) << 16;
    offset += 2;
    if (offset + 11 > payload.length) throw new Error("truncated MySQL authentication metadata");
    const authLength = payload[offset++];
    offset += 10 + Math.max(13, authLength - 8);
    const plugin = nulTerminated(payload, offset).value;
    if (plugin !== "mysql_native_password")
        throw new Error(`unsupported MySQL authentication plugin '${plugin}'`);
    return capabilities;
}

function handshakeResponse(options, capabilities) {
    let flags =
        CLIENT_LONG_PASSWORD |
        CLIENT_PROTOCOL_41 |
        CLIENT_TRANSACTIONS |
        CLIENT_SECURE_CONNECTION |
        CLIENT_PLUGIN_AUTH;
    if (options.database) flags |= CLIENT_CONNECT_WITH_DB;
    flags &= capabilities;
    const fixed = Buffer.alloc(32);
    fixed.writeUInt32LE(flags, 0);
    fixed.writeUInt32LE(maxPacketBytes, 4);
    fixed[8] = 45;
    const fields = [Buffer.from(options.user), Buffer.from([0]), Buffer.from([0])];
    if (options.database) fields.push(Buffer.from(options.database), Buffer.from([0]));
    if (flags & CLIENT_PLUGIN_AUTH) fields.push(Buffer.from("mysql_native_password\0"));
    return Buffer.concat([fixed, ...fields]);
}

function serverError(payload) {
    if (payload.length < 3 || payload[0] !== 0xff) throw new Error("malformed MySQL error packet");
    const code = payload.readUInt16LE(1);
    let offset = 3;
    let state = "";
    if (payload[offset] === 35 && payload.length >= offset + 6) {
        state = payload.toString("ascii", offset + 1, offset + 6);
        offset += 6;
    }
    return new Error(`${code}${state ? ` (${state})` : ""}: ${payload.toString("utf8", offset)}`);
}

function optionsFrom(args) {
    const options = { host: "127.0.0.1", port: 3306, user: "yyds", database: "", sql: "" };
    for (let index = 0; index < args.length; index += 1) {
        const option = args[index];
        const key = {
            "-h": "host",
            "--host": "host",
            "-P": "port",
            "--port": "port",
            "-u": "user",
            "--user": "user",
            "-D": "database",
            "--database": "database",
            "-e": "sql",
            "--execute": "sql",
        }[option];
        if (!key) throw new Error(`unsupported option '${option}'`);
        const value = args[++index];
        if (value === undefined || value.includes("\0") || Buffer.byteLength(value) > 64 * 1024)
            throw new Error(`invalid value for ${option}`);
        if (key === "port") {
            const port = Number(value);
            if (!Number.isInteger(port) || port < 1 || port > 65535)
                throw new Error("invalid MySQL port");
            options.port = port;
        } else options[key] = value;
    }
    if (!options.sql) throw new Error("one SQL statement is required with -e");
    return options;
}

async function run(options) {
    const socket = createConnection({ host: options.host, port: options.port });
    socket.setTimeout(5000, () => socket.destroy(new Error("MySQL connection timed out")));
    const read = createReader(socket);
    try {
        await new Promise((resolve, reject) => {
            socket.once("connect", resolve);
            socket.once("error", reject);
        });
        const capabilities = parseHandshake(await read(0));
        socket.write(packet(1, handshakeResponse(options, capabilities)));
        const authentication = await read(2);
        if (authentication[0] === 0xff) throw serverError(authentication);
        if (authentication[0] !== 0x00)
            throw new Error("unsupported MySQL authentication response");
        const query = Buffer.concat([Buffer.from([0x03]), Buffer.from(options.sql, "utf8")]);
        if (query.length > maxPacketBytes) throw new Error("MySQL query exceeds byte limit");
        socket.write(packet(0, query));
        const response = await read(1);
        if (response[0] === 0xff) throw serverError(response);
        if (response[0] !== 0x00)
            throw new Error("MySQL result sets are not supported by this client build");
        console.log("Query OK");
    } finally {
        socket.destroy();
    }
}

async function main(args) {
    if (args.length === 1 && ["-V", "--version"].includes(args[0])) {
        console.log(`mysql  Ver ${version} for YYDS`);
        return;
    }
    if (args.length === 1 && args[0] === "--help") {
        console.log("Usage: mysql [-h HOST] [-P PORT] [-u USER] [-D DATABASE] -e SQL");
        return;
    }
    await run(optionsFrom(args));
}

main(process.argv.slice(2)).catch((error) => {
    console.error(`@yyds/mysql: ${error.message}`);
    process.exitCode = 1;
});
