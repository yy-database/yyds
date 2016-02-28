#!/usr/bin/env node
import { createConnection } from "node:net";
import { randomUUID } from "node:crypto";

const defaultHost = "127.0.0.1";
const defaultPort = 27017;

// OP_MSG wire constants
const OP_MSG = 2013;
const OP_REPLY = 1;

function encodeVarInt(value) {
    const bytes = [];
    let remaining = value;
    do {
        let byte = remaining & 0x7f;
        remaining >>>= 7;
        if (remaining !== 0) byte |= 0x80;
        bytes.push(byte);
    } while (remaining !== 0);
    return Buffer.from(bytes);
}

function encodeDocument(value) {
    const lengthBuf = Buffer.alloc(4);
    let body = Buffer.alloc(0);
    if (value !== null && typeof value === "object") {
        for (const [key, item] of Object.entries(value)) {
            body = Buffer.concat([body, Buffer.from("\x02" + key + "\x00"), Buffer.alloc(4)]);
            // Placeholder — full BSON encoding is out of scope for the handshake probe
            const placeholder = Buffer.from("\x01\x02\x03\x04\x05\x06\x07\x08");
            body = Buffer.concat([body, placeholder]);
            // Overwrite the first 4 bytes with the inner length
            const innerLen = Buffer.alloc(4);
            innerLen.writeInt32LE(8 + 1 + 1);
            body.copy(innerLen, body.length - 12, body.length - 16);
            innerLen.copy(body, body.length - 16);
        }
    }
    const end = Buffer.from("\x00");
    lengthBuf.writeInt32LE(4 + body.length + 1);
    return Buffer.concat([lengthBuf, body, end]);
}

function encodeOpMsg(command, db) {
    const sectionKind = Buffer.from([0x00]);
    const encoded = Buffer.concat([sectionKind, encodeDocument(command), encodeDocument({})]);
    const totalLen = Buffer.alloc(4);
    totalLen.writeInt32LE(4 + 8 + 1 + 24 + encoded.length);
    const requestId = Buffer.alloc(4);
    const responseTo = Buffer.alloc(4);
    responseTo.writeInt32LE(-1);
    const opcode = Buffer.alloc(4);
    opcode.writeInt32LE(OP_MSG);
    const flags = Buffer.alloc(4);
    return Buffer.concat([totalLen, requestId, responseTo, opcode, flags, encoded]);
}

export function exchange(options, command, db = "admin") {
    return new Promise((resolve, reject) => {
        const socket = createConnection({ host: options.host, port: options.port });
        let pending = Buffer.alloc(0);
        let settled = false;
        const timer = setTimeout(() => finish(new Error("MongoDB connection timed out")), 5000);
        function finish(error, value) {
            if (settled) return;
            settled = true;
            clearTimeout(timer);
            socket.destroy();
            if (error) reject(error);
            else resolve(value);
        }
        socket.on("connect", () => {
            socket.write(encodeOpMsg(command, db));
        });
        socket.on("data", (chunk) => {
            pending = Buffer.concat([pending, chunk]);
            if (pending.length < 4) return;
            const length = pending.readInt32LE();
            if (pending.length < length) return;
            finish(null, pending.subarray(0, length));
        });
        socket.on("error", finish);
        socket.on("end", () => finish(new Error("MongoDB closed before a complete reply")));
    });
}

async function main(args) {
    let host = defaultHost;
    let port = defaultPort;
    const commandName = "ping";
    for (let index = 0; index < args.length; index += 1) {
        const arg = args[index];
        if (["-h", "--host"].includes(arg)) {
            host = args[++index];
        } else if (["-p", "--port"].includes(arg)) {
            port = Number(args[++index]);
        } else if (["--help", "-V", "--version"].includes(arg)) {
            console.log("Usage: mongo [-h HOST] [-p PORT] [COMMAND [ARG ...]]");
            console.log("A minimal MongoDB OP_MSG client for the YYDS handshake probe.");
            return;
        }
    }
    const command = { ping: 1, $db: "admin" };
    try {
        const reply = await exchange({ host, port }, command);
        const opcode = reply.readInt32LE(12);
        if (opcode === OP_MSG) {
            console.log("ok");
        } else {
            console.log("unknown reply opcode " + opcode);
        }
    } catch (error) {
        console.error("@yydb/mongo: " + error.message);
        process.exitCode = 1;
    }
}

main(process.argv.slice(2));
