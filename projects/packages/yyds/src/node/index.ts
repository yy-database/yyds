/**
 * `@yydb/yyds/node` — Node N-API facade.
 *
 * Browser code must use the default `@yydb/yyds` entry or `@yydb/yyds/wasm`.
 */

export type { YydsBindings } from "../bindings.ts";
export type { YydsProtocol } from "./cluster.ts";
export { launchYydsServer, yydsStartArguments } from "./cluster.ts";
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
