import { loadYydsSqliteNative } from "@yyds/yyds/node";
import type {
    SqliteReadLimits,
    SqliteSchemaObject,
    SqliteSnapshotBinding,
    SqliteTableRow,
} from "@yyds/yyds/node";

export type { SqliteReadLimits, SqliteSchemaObject, SqliteTableRow } from "@yyds/yyds/node";

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
