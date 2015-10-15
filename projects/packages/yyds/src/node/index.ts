/**
 * `@yydb/yyds/node` — Node N-API facade.
 *
 * Browser code must use the default `@yydb/yyds` entry or `@yydb/yyds/wasm`.
 */

export type { YydsBindings, NodeStatusBinding, ServiceBinding } from "../bindings.ts";
export type { YydsProtocol } from "./cluster.ts";
export { launchYydsServer, yydsStartArguments } from "./cluster.ts";
export { isYydsNativeInstalled, loadYydsNative, loadYydsSqliteNative } from "./load.ts";
export type { StartNodeOptions } from "./runtime.ts";
export { parseStartOptions, runNodeStart } from "./runtime.ts";
export type {
    SqliteReadLimits,
    SqliteSchemaObject,
    SqliteSnapshotBinding,
    SqliteTableRow,
    YydsSqliteBindings,
} from "./sqlite-bindings.ts";
