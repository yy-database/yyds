/**
 * `@yydb/yyds` default export — browser / Worker facade (WASM inside).
 *
 * Node apps use `@yydb/yyds/node`. Low-level WASM loader: `@yydb/yyds/wasm`.
 */

export { initWasm, ping, yydsVersion, type InitWasmOptions, type WasmInitInput } from "../wasm/index.ts";
