/** Bounds apply independently to each schema or table scan. */
export interface SqliteReadLimits {
    maxPages?: number;
    maxRows?: number;
    maxPayloadBytes?: number;
    maxTotalPayloadBytes?: number;
}

/** SQL source is opaque. No CREATE statement is parsed or executed. */
export interface SqliteSchemaObject {
    kind: "table" | "index" | "view" | "trigger";
    name: string;
    tableName: string;
    rootPage?: number;
    sql?: string;
}

/** Raw storage values, not SQL results or INTEGER PRIMARY KEY expansion. */
export interface SqliteTableRow {
    rowid: bigint;
    payload: Buffer;
}

/** One decoded SQLite storage-class field. Exactly one `value*` is populated. */
export interface SqliteRecordValue {
    kind: "null" | "integer" | "real" | "text" | "blob";
    valueInteger?: bigint;
    valueReal?: number;
    valueText?: string;
    valueBlob?: Buffer;
}

/** A decoded SQLite row with storage-class typed columns. */
export interface SqliteDecodedRow {
    rowid: bigint;
    columns: SqliteRecordValue[];
}

/** Binding surface for SqliteSnapshot (read-only main-file buffer). */
export interface SqliteSnapshotBinding {
    schema(limits?: SqliteReadLimits): SqliteSchemaObject[];
    table(name: string, limits?: SqliteReadLimits): SqliteTableRow[];
    tableDecoded(name: string, limits?: SqliteReadLimits): SqliteDecodedRow[];
    sqliteVersion(): string;
    encoding(): string;
    ping(): number;
    pages(): Buffer;
}

/** Binding surface for SqliteDatabase (file-backed or in-memory handle). */
export interface SqliteDatabaseBinding {
    path(): string | null;
    ping(): number;
    sqliteVersion(): string;
    encoding(): string;
    schema(limits?: SqliteReadLimits): SqliteSchemaObject[];
    table(name: string, limits?: SqliteReadLimits): SqliteDecodedRow[];
    tableRaw(name: string, limits?: SqliteReadLimits): SqliteTableRow[];
    pages(): Buffer;
}

/** Combined SQLite native addon surface. */
export interface YydsSqliteBindings {
    SqliteSnapshot: new (bytes: Buffer, maxSnapshotBytes?: number) => SqliteSnapshotBinding;
    SqliteDatabase: {
        openInMemory(): SqliteDatabaseBinding;
        open(path: string): SqliteDatabaseBinding;
    };
    decodeRecord(payload: Buffer, encoding?: string): SqliteRecordValue[];
}
