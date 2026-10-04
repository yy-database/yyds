/**
 * `@yyds/yyds/node` — Node N-API facade.
 *
 * Browser code must use the default `@yyds/yyds` entry or `@yyds/yyds/wasm`.
 */

export type { YydsBindings } from "../bindings.ts";
export { isYydsNativeInstalled, loadYydsNative, loadYydsSqliteNative } from "./load.ts";
export type {
    SqliteConnectionBinding,
    SqliteQueryResultBinding,
    SqliteReadLimits,
    SqliteResultValueBinding,
    SqliteSchemaObject,
    SqliteSnapshotBinding,
    SqliteTableRow,
    YydsSqliteBindings,
} from "./sqlite-bindings.ts";
