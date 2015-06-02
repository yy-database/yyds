import initGlue, * as glue from "../lib/yyds_wasm.js";

export type WasmInitInput = URL | Request | Response | ArrayBuffer | Uint8Array | WebAssembly.Module;

export type InitWasmOptions = {
    /** When omitted, loads the bundled `lib/yyds_wasm_bg.wasm` asset. */
    module?: WasmInitInput;
};

const glueApi = glue as {
    yydsVersion?: () => string;
    ping?: () => string;
};

let ready = false;

function assertReady(): void {
    if (!ready) {
        throw new Error("@yyds/yyds-unknown-wasm32: call initWasm() before semantic core methods");
    }
}

async function resolveDefaultWasmBytes(): Promise<ArrayBuffer | Uint8Array | URL> {
    const wasmUrl = new URL("../lib/yyds_wasm_bg.wasm", import.meta.url);
    if (typeof process !== "undefined" && process.versions?.node) {
        const { readFileSync } = await import("node:fs");
        const { fileURLToPath } = await import("node:url");
        return readFileSync(fileURLToPath(wasmUrl));
    }
    return wasmUrl;
}

async function normalizeInitInput(input: WasmInitInput | undefined): Promise<WasmInitInput | ArrayBuffer | Uint8Array> {
    if (input === undefined) {
        return resolveDefaultWasmBytes();
    }
    if (
        input instanceof URL ||
        input instanceof Request ||
        input instanceof Response ||
        input instanceof ArrayBuffer ||
        input instanceof Uint8Array ||
        input instanceof WebAssembly.Module
    ) {
        return input;
    }
    throw new Error(`@yyds/yyds-unknown-wasm32: unsupported WASM init input (${typeof input})`);
}

/** One-time WASM init. Required before semantic core methods. */
export async function initWasm(options: InitWasmOptions = {}): Promise<void> {
    if (ready) {
        return;
    }
    const moduleOrPath = await normalizeInitInput(options.module);
    await initGlue({ module_or_path: moduleOrPath });
    ready = true;
}

/** Library version (matches `yyds-types::version()`). */
export function yydsVersion(): string {
    assertReady();
    if (!glueApi.yydsVersion) {
        throw new Error("@yyds/yyds-unknown-wasm32: yydsVersion export missing, rebuild wasm artifacts");
    }
    return glueApi.yydsVersion();
}

/** Lightweight health probe for WASM binding smoke tests. */
export function ping(): string {
    assertReady();
    if (!glueApi.ping) {
        throw new Error("@yyds/yyds-unknown-wasm32: ping export missing, rebuild wasm artifacts");
    }
    return glueApi.ping();
}

/** @internal Reset init gate (binding tests only). */
export function resetWasmBindingForTests(): void {
    ready = false;
}
