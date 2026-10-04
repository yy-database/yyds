#!/usr/bin/env node
import { readFileSync } from "node:fs";
import { createConnection } from "node:net";

const { version } = JSON.parse(readFileSync(new URL("../package.json", import.meta.url), "utf8"));
const maximum = 16 * 1024 * 1024;

function message(tag, payload) {
    const header = Buffer.alloc(5);
    header[0] = tag.charCodeAt(0);
    header.writeUInt32BE(payload.length + 4, 1);
    return Buffer.concat([header, payload]);
}

function argumentsFor(args) {
    const options = {
        host: "127.0.0.1",
        port: 5432,
        user: "yyds",
        database: "yyds",
        sql: "",
        headers: true,
        separator: "|",
        nullValue: "",
    };
    for (let index = 0; index < args.length; index += 1) {
        const option = args[index];
        if (option === "-t" || option === "--tuples-only") {
            options.headers = false;
            continue;
        }
        if (option === "-F" || option === "--field-separator") {
            const value = args[++index];
            if (value === undefined || value.includes("\0")) throw new Error(`invalid value for ${option}`);
            options.separator = value;
            continue;
        }
        if (option === "-P" || option === "--pset") {
            const value = args[++index];
            if (value === undefined || !value.startsWith("null=")) throw new Error("only psql null output is configurable");
            options.nullValue = value.slice(5);
            continue;
        }
        const key = {
            "-h": "host",
            "--host": "host",
            "-p": "port",
            "--port": "port",
            "-U": "user",
            "--username": "user",
            "-d": "database",
            "--dbname": "database",
            "-c": "sql",
            "--command": "sql",
        }[option];
        if (!key) throw new Error(`unsupported option '${option}'`);
        const value = args[++index];
        if (value === undefined || value.includes("\0") || Buffer.byteLength(value) > 64 * 1024)
            throw new Error(`invalid value for ${option}`);
        if (key === "port") {
            const port = Number(value);
            if (!Number.isInteger(port) || port < 1 || port > 65535)
                throw new Error("invalid port");
            options.port = port;
        } else options[key] = value;
    }
    if (!options.sql || !options.user || !options.database)
        throw new Error("a nonempty command, user and database are required");
    return options;
}

function errorResponse(payload) {
    const fields = {};
    let offset = 0;
    while (offset < payload.length && payload[offset] !== 0) {
        const code = String.fromCharCode(payload[offset++]);
        const end = payload.indexOf(0, offset);
        if (end < 0) throw new Error("malformed PostgreSQL error response");
        fields[code] = payload.toString("utf8", offset, end);
        offset = end + 1;
    }
    return new Error(`${fields.C ?? "unknown"}: ${fields.M ?? "PostgreSQL error"}`);
}

function columnsFrom(payload) {
    if (payload.length < 2) throw new Error("truncated RowDescription");
    const count = payload.readUInt16BE();
    const names = [];
    let offset = 2;
    for (let index = 0; index < count; index += 1) {
        const end = payload.indexOf(0, offset);
        if (end < 0 || end + 19 > payload.length) throw new Error("truncated column description");
        if (payload.readUInt16BE(end + 17) !== 0)
            throw new Error("binary PostgreSQL results are unsupported");
        names.push(payload.toString("utf8", offset, end));
        offset = end + 19;
    }
    if (offset !== payload.length) throw new Error("trailing RowDescription bytes");
    return names;
}

function rowFrom(payload, count) {
    if (payload.length < 2 || payload.readUInt16BE() !== count)
        throw new Error("invalid DataRow column count");
    let offset = 2;
    const values = [];
    for (let index = 0; index < count; index += 1) {
        if (offset + 4 > payload.length) throw new Error("truncated DataRow length");
        const length = payload.readInt32BE(offset);
        offset += 4;
        if (length === -1) values.push(null);
        else {
            if (length < 0 || offset + length > payload.length)
                throw new Error("invalid DataRow length");
            values.push(payload.toString("utf8", offset, offset + length));
            offset += length;
        }
    }
    if (offset !== payload.length) throw new Error("trailing DataRow bytes");
    return values;
}

function query(options) {
    return new Promise((resolve, reject) => {
        const socket = createConnection({ host: options.host, port: options.port });
        let pending = Buffer.alloc(0);
        let started = false;
        let authenticated = false;
        let currentResult = null;
        let settled = false;
        let received = 0;
        const results = [];
        const timer = setTimeout(() => finish(new Error("PostgreSQL connection timed out")), 5000);
        function finish(error) {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            socket.destroy();
            if (error) reject(error);
            else resolve(results);
        }
        socket.on("connect", () => {
            const parameters = Buffer.from(
                `user\0${options.user}\0database\0${options.database}\0client_encoding\0UTF8\0application_name\0yyds-psql\0\0`,
            );
            const header = Buffer.alloc(8);
            header.writeUInt32BE(parameters.length + 8);
            header.writeUInt32BE(196608, 4);
            socket.write(Buffer.concat([header, parameters]));
        });
        socket.on("data", (chunk) => {
            try {
                received += chunk.length;
                if (received > maximum) throw new Error("PostgreSQL response exceeds byte limit");
                pending = Buffer.concat([pending, chunk]);
                while (pending.length >= 5) {
                    const length = pending.readUInt32BE(1);
                    if (length < 4 || length > maximum)
                        throw new Error("invalid PostgreSQL message length");
                    if (pending.length < length + 1) return;
                    const tag = String.fromCharCode(pending[0]);
                    const payload = pending.subarray(5, length + 1);
                    pending = pending.subarray(length + 1);
                    if (tag === "E") throw errorResponse(payload);
                    if (tag === "R") {
                        if (payload.length !== 4 || payload.readUInt32BE() !== 0)
                            throw new Error("PostgreSQL authentication is unsupported");
                        authenticated = true;
                    } else if (tag === "Z") {
                        if (
                            !authenticated ||
                            payload.length !== 1 ||
                            ![73, 84, 69].includes(payload[0])
                        )
                            throw new Error("invalid ReadyForQuery");
                        if (started) {
                            if (currentResult !== null) throw new Error("PostgreSQL result ended before CommandComplete");
                            finish();
                            return;
                        }
                        started = true;
                        const sql = Buffer.from(`${options.sql}\0`);
                        if (sql.length + 4 > maximum)
                            throw new Error("PostgreSQL query exceeds byte limit");
                        socket.write(message("Q", sql));
                    } else if (tag === "T") {
                        if (!started || currentResult !== null) throw new Error("unexpected PostgreSQL RowDescription");
                        currentResult = { columns: columnsFrom(payload), rows: [], tag: "" };
                    } else if (tag === "D") {
                        if (!started || currentResult === null) throw new Error("DataRow without RowDescription");
                        currentResult.rows.push(rowFrom(payload, currentResult.columns.length));
                    }
                    else if (tag === "C") {
                        if (!started || payload.length < 2 || payload.indexOf(0) !== payload.length - 1)
                            throw new Error("invalid CommandComplete");
                        const result = currentResult ?? { columns: [], rows: [], tag: "" };
                        result.tag = payload.toString("utf8", 0, payload.length - 1);
                        results.push(result);
                        currentResult = null;
                    } else if (tag === "I") {
                        if (!started || payload.length !== 0 || currentResult !== null)
                            throw new Error("invalid EmptyQueryResponse");
                    } else if (!["S", "K", "N"].includes(tag))
                        throw new Error(`unsupported PostgreSQL response '${tag}'`);
                }
            } catch (error) {
                finish(error);
            }
        });
        socket.on("error", finish);
        socket.on("end", () => finish(new Error("PostgreSQL closed before ReadyForQuery")));
    });
}

async function main(args) {
    if (args.length === 1 && ["-V", "--version"].includes(args[0])) {
        console.log(`psql (YYDS ${version})`);
        return;
    }
    if (args.length === 1 && args[0] === "--help") {
        console.log("Usage: psql [-h HOST] [-p PORT] [-U USER] [-d DATABASE] [-t] [-F SEP] [-P null=TEXT] -c SQL");
        return;
    }
    const options = argumentsFor(args);
    const results = await query(options);
    for (const result of results) {
        if (options.headers && result.columns.length) console.log(result.columns.join(options.separator));
        for (const row of result.rows) console.log(row.map((value) => value ?? options.nullValue).join(options.separator));
        if (result.columns.length === 0) console.log(result.tag);
    }
}

main(process.argv.slice(2)).catch((error) => {
    console.error(`@yyds/postgresql: ${error.message}`);
    process.exitCode = 1;
});
