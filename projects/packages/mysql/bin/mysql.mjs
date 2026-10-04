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
    socket.on("end", () => {
        failure = new Error("MySQL closed before a complete packet");
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
    if (!(capabilities & CLIENT_PROTOCOL_41) || !(capabilities & CLIENT_SECURE_CONNECTION)) {
        throw new Error("MySQL protocol 4.1 with secure-connection framing is required");
    }
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

function lengthEncoded(payload, offset) {
    if (offset >= payload.length) throw new Error("truncated MySQL length-encoded value");
    const marker = payload[offset++];
    if (marker < 251) return { length: marker, next: offset };
    if (marker === 251) return { length: null, next: offset };
    const width = { 252: 2, 253: 3, 254: 8 }[marker];
    if (!width || offset + width > payload.length)
        throw new Error("invalid MySQL length-encoded value");
    const length = width === 8 ? payload.readBigUInt64LE(offset) : BigInt(payload.readUIntLE(offset, width));
    if (length > BigInt(maxPacketBytes)) throw new Error("MySQL value exceeds byte limit");
    return { length: Number(length), next: offset + width };
}

function lengthEncodedString(payload, offset) {
    const value = lengthEncoded(payload, offset);
    if (value.length === null) return { value: null, next: value.next };
    const end = value.next + value.length;
    if (end > payload.length) throw new Error("truncated MySQL string");
    return { value: payload.subarray(value.next, end), next: end };
}

function columnName(payload) {
    let offset = 0;
    let name;
    for (let index = 0; index < 6; index += 1) {
        const field = lengthEncodedString(payload, offset);
        if (field.value === null) throw new Error("invalid MySQL column definition");
        if (index === 4) name = field.value;
        offset = field.next;
    }
    const fixed = lengthEncoded(payload, offset);
    if (fixed.length !== 12 || fixed.next + 12 !== payload.length)
        throw new Error("malformed MySQL column metadata");
    return name.toString("utf8");
}

function eofPacket(payload) {
    if (payload.length !== 5 || payload[0] !== 0xfe)
        throw new Error("expected MySQL EOF packet");
    if (payload.readUInt16LE(3) & 8) throw new Error("multiple MySQL result sets are not supported");
}

function displayField(value, raw) {
    if (value === null) return "NULL";
    if (raw) return value.toString("utf8");
    return value.toString("utf8").replaceAll("\\", "\\\\").replaceAll("\0", "\\0")
        .replaceAll("\t", "\\t").replaceAll("\n", "\\n").replaceAll("\r", "\\r");
}

async function resultSet(response, read, options) {
    const count = lengthEncoded(response, 0);
    if (count.next !== response.length || count.length === null || count.length < 1 || count.length > 1024)
        throw new Error("invalid MySQL result column count");
    let sequence = 2;
    let totalBytes = response.length;
    const next = async () => {
        const payload = await read(sequence);
        sequence = (sequence + 1) & 255;
        totalBytes += payload.length;
        if (totalBytes > 8 * maxPacketBytes) throw new Error("MySQL result exceeds byte limit");
        if (payload[0] === 0xff) throw serverError(payload);
        return payload;
    };
    const columns = [];
    for (let index = 0; index < count.length; index += 1) columns.push(columnName(await next()));
    eofPacket(await next());
    const rows = [];
    while (true) {
        const payload = await next();
        if (payload[0] === 0xfe && payload.length < 9) {
            eofPacket(payload);
            break;
        }
        if (rows.length >= 100_000) throw new Error("MySQL result exceeds row limit");
        const row = [];
        let offset = 0;
        for (let index = 0; index < count.length; index += 1) {
            const field = lengthEncodedString(payload, offset);
            row.push(displayField(field.value, options.raw));
            offset = field.next;
        }
        if (offset !== payload.length) throw new Error("MySQL row does not match column count");
        rows.push(row);
    }
    if (!options.skipColumnNames)
        console.log(columns.map((name) => displayField(Buffer.from(name), options.raw)).join("\t"));
    for (const row of rows) console.log(row.join("\t"));
}

function optionsFrom(args) {
    const options = {
        host: "127.0.0.1",
        port: 3306,
        user: "yyds",
        database: "",
        sql: "",
        raw: false,
        skipColumnNames: false,
    };
    for (let index = 0; index < args.length; index += 1) {
        const option = args[index];
        if (option === "-B" || option === "--batch") continue;
        if (option === "-N" || option === "--skip-column-names") {
            options.skipColumnNames = true;
            continue;
        }
        if (option === "-r" || option === "--raw") {
            options.raw = true;
            continue;
        }
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
        if (response[0] === 0x00) console.log("Query OK");
        else await resultSet(response, read, options);
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
        console.log("Usage: mysql [-B] [-N] [-r] [-h HOST] [-P PORT] [-u USER] [-D DATABASE] -e SQL");
        return;
    }
    await run(optionsFrom(args));
}

main(process.argv.slice(2)).catch((error) => {
    console.error(`@yyds/mysql: ${error.message}`);
    process.exitCode = 1;
});
