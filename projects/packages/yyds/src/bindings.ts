/** Shared Node and WASM binding surface for `@yyds/yyds`. */
export interface YydsBindings {
    yydsVersion(): string;
    ping(): string;
}
