import { loadYydsNative } from "@yyds/yyds/node";

export type DisguiseRuntime = {
    product: string;
    yydsVersion: string;
    ping: string;
};

/** Why this package does not route through YY-optimized storage. */
export const WHY_NOT_YY_OPTIMIZED =
    "SQLite requires binary-compatible on-disk bytes. YYDS stores VOS in .yyds and .yykv instead.";

export function loadDisguiseRuntime(product: string): DisguiseRuntime {
    try {
        const native = loadYydsNative();
        return {
            product,
            yydsVersion: native.yydsVersion(),
            ping: native.ping(),
        };
    } catch (error) {
        throw new Error(
            `@yyds/${product}: native runtime missing. Install @yyds/yyds and run pnpm build:napi`,
            { cause: error },
        );
    }
}
