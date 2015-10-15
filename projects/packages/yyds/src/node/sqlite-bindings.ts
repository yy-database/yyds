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

export interface SqliteSnapshotBinding {
    schema(limits?: SqliteReadLimits): SqliteSchemaObject[];
    table(name: string, limits?: SqliteReadLimits): SqliteTableRow[];
}

/** An independent pure-Rust SQLite snapshot reader hosted in the native addon. */
export interface YydsSqliteBindings {
    SqliteSnapshot: new (bytes: Buffer, maxSnapshotBytes?: number) => SqliteSnapshotBinding;
}
