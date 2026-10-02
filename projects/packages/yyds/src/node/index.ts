/**
 * `@yyds/yyds/node` — Node N-API facade.
 *
 * Browser code must use the default `@yyds/yyds` entry or `@yyds/yyds/wasm`.
 */

export type { YydsBindings } from "../bindings.ts";
export { isYydsNativeInstalled, loadYydsNative } from "./load.ts";
