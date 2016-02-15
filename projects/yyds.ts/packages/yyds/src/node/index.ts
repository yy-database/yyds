/**
 * `@yydb/yyds/node` — Node N-API facade.
 *
 * Browser hosts are not supported in this package yet.
 */

export type { YydsBindings, NodeStatusBinding, ServiceBinding } from "../bindings.ts";
export type { YydsProtocol } from "./cluster.ts";
export { launchYydsServer, yydsStartArguments } from "./cluster.ts";
export { isYydsNativeInstalled, loadYydsNative } from "./load.ts";
export type { StartNodeOptions } from "./runtime.ts";
export { parseStartOptions, runNodeStart } from "./runtime.ts";
