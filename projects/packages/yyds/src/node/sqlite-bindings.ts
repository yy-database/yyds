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

/** An independent SQLite reader hosted in the same native addon. */
export interface YydsSqliteBindings {
    SqliteSnapshot: new (bytes: Buffer, maxSnapshotBytes?: number) => SqliteSnapshotBinding;
    SqliteConnection: new (path: string) => SqliteConnectionBinding;
}

export interface SqliteConnectionBinding {
    execute(sql: string): SqliteQueryResultBinding;
    sourceId(): string;
}

export interface SqliteQueryResultBinding {
    columns: string[];
    rows: SqliteResultValueBinding[][];
    changes: string;
    lastInsertRowid: string;
}

export interface SqliteResultValueBinding {
    kind: "null" | "integer" | "real" | "text" | "blob";
    integer?: string;
    real?: number;
    text?: string;
    textBytes?: Buffer;
    blob?: Buffer;
}
