/** Shared Node and WASM binding surface for `@yydb/yyds`. */
export interface YydsBindings {
    yydsVersion(): string;
    ping(): string;
}
