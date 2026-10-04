import { loadYydsSqliteNative } from "@yyds/yyds/node";
import type {
    SqliteConnectionBinding,
    SqliteReadLimits,
    SqliteSchemaObject,
    SqliteSnapshotBinding,
    SqliteTableRow,
    SqliteQueryResultBinding,
} from "@yyds/yyds/node";

export type {
    SqliteQueryResultBinding,
    SqliteReadLimits,
    SqliteResultValueBinding,
    SqliteSchemaObject,
    SqliteTableRow,
} from "@yyds/yyds/node";

export type SqliteCell =
    | { kind: "null" }
    | { kind: "integer"; value: bigint }
    | { kind: "real"; value: number }
    | { kind: "text"; value: string | Buffer }
    | { kind: "blob"; value: Buffer };

export type SqliteParameter = SqliteCell;

export interface SqliteQueryResult {
    columns: string[];
    rows: SqliteCell[][];
    changes: bigint;
    lastInsertRowid: bigint;
}

/** A real SQLite connection, backed by upstream SQLite's pager and SQL engine. */
export class SqliteConnection {
    readonly #native: SqliteConnectionBinding;

    constructor(path: string, options: { readOnly?: boolean } = {}) {
        if (typeof path !== "string" || path.length === 0) {
            throw new TypeError("SQLite database path must be a non-empty string");
        }
        if (typeof options !== "object" || options === null || Object.keys(options).some((key) => key !== "readOnly") || options.readOnly !== undefined && typeof options.readOnly !== "boolean") {
            throw new TypeError("SQLite connection options must contain an optional boolean readOnly");
        }
        const native = loadYydsSqliteNative();
        this.#native = new native.SqliteConnection(path, options.readOnly ?? false);
    }

    execute(sql: string): SqliteQueryResult {
        if (typeof sql !== "string" || sql.length === 0) {
            throw new TypeError("SQLite SQL must be a non-empty string");
        }
        const result = this.#native.execute(sql);
        return {
            columns: result.columns,
            rows: result.rows.map((row) => row.map(decodeCell)),
            changes: BigInt(result.changes),
            lastInsertRowid: BigInt(result.lastInsertRowid),
        };
    }

    executeWithParameters(sql: string, parameters: SqliteParameter[]): SqliteQueryResult {
        if (typeof sql !== "string" || sql.length === 0) {
            throw new TypeError("SQLite SQL must be a non-empty string");
        }
        if (!Array.isArray(parameters)) {
            throw new TypeError("SQLite parameters must be an array");
        }
        const bound = parameters.map(encodeParameter);
        const result = this.#native.executeWithParameters(sql, bound);
        return {
            columns: result.columns,
            rows: result.rows.map((row) => row.map(decodeCell)),
            changes: BigInt(result.changes),
            lastInsertRowid: BigInt(result.lastInsertRowid),
        };
    }

    executeBatch(sql: string): void {
        if (typeof sql !== "string" || sql.length === 0) {
            throw new TypeError("SQLite SQL batch must be a non-empty string");
        }
        this.#native.executeBatch(sql);
    }

    sourceId(): string {
        return this.#native.sourceId();
    }
}

function encodeParameter(value: SqliteParameter): NonNullable<Parameters<SqliteConnectionBinding["executeWithParameters"]>[1][number]> {
    switch (value.kind) {
        case "null":
            return { kind: "null" };
        case "integer":
            return { kind: "integer", integer: value.value.toString() };
        case "real":
            return { kind: "real", real: value.value };
        case "text":
            return { kind: "text", text: typeof value.value === "string" ? Buffer.from(value.value, "utf8") : value.value };
        case "blob":
            return { kind: "blob", blob: value.value };
    }
}

function decodeCell(
    value: NonNullable<SqliteQueryResultBinding["rows"][number][number]>,
): SqliteCell {
    switch (value.kind) {
        case "null":
            return { kind: "null" };
        case "integer":
            if (value.integer === undefined)
                throw new Error("native SQLite integer result omitted its value");
            return { kind: "integer", value: BigInt(value.integer) };
        case "real":
            if (value.real === undefined)
                throw new Error("native SQLite real result omitted its value");
            return { kind: "real", value: value.real };
        case "text":
            if (value.text !== undefined) return { kind: "text", value: value.text };
            if (value.textBytes !== undefined) return { kind: "text", value: value.textBytes };
            throw new Error("native SQLite text result omitted its value");
        case "blob":
            if (value.blob === undefined)
                throw new Error("native SQLite blob result omitted its value");
            return { kind: "blob", value: value.blob };
    }
}

/** Read-only, copied main-file bytes. WAL/journal recovery is not performed. */
export class SqliteSnapshot {
    readonly #native: SqliteSnapshotBinding;

    constructor(bytes: Uint8Array, maxSnapshotBytes = 256 * 1024 * 1024) {
        unsignedLimit(maxSnapshotBytes, "maxSnapshotBytes");
        if (!(bytes instanceof Uint8Array)) {
            throw new TypeError("SQLite snapshot must be a Uint8Array or Buffer");
        }
        if (bytes.byteLength > maxSnapshotBytes) {
            throw new RangeError("sqlite snapshot byte limit exceeded");
        }
        const native = loadYydsSqliteNative();
        this.#native = new native.SqliteSnapshot(Buffer.from(bytes), maxSnapshotBytes);
    }

    schema(limits?: SqliteReadLimits): SqliteSchemaObject[] {
        return this.#native.schema(validateLimits(limits));
    }

    table(name: string, limits?: SqliteReadLimits): SqliteTableRow[] {
        if (typeof name !== "string") {
            throw new TypeError("SQLite table name must be a string");
        }
        return this.#native.table(name, validateLimits(limits));
    }
}

function unsignedLimit(value: number, name: string): void {
    if (!Number.isInteger(value) || value < 0 || value > 0xffff_ffff) {
        throw new RangeError(`${name} must be an unsigned 32-bit integer`);
    }
}

function validateLimits(limits?: SqliteReadLimits): SqliteReadLimits | undefined {
    if (limits === undefined) return undefined;
    if (limits === null || typeof limits !== "object" || Array.isArray(limits)) {
        throw new TypeError("SQLite scan limits must be an object");
    }
    const keys = new Set(["maxPages", "maxRows", "maxPayloadBytes", "maxTotalPayloadBytes"]);
    for (const [key, value] of Object.entries(limits)) {
        if (!keys.has(key)) throw new TypeError(`Unknown SQLite scan limit: ${key}`);
        if (value !== undefined) unsignedLimit(value, key);
    }
    return limits;
}
