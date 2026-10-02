/**
 * `@yyds/yyds` default export — browser / Worker facade (WASM inside).
 *
 * Node apps use `@yyds/yyds/node`. Low-level WASM loader: `@yyds/yyds/wasm`.
 */

export { initWasm, ping, yydsVersion, type InitWasmOptions, type WasmInitInput } from "../wasm/index.ts";
