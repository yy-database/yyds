import { loadYydsNative } from "@yyds/yyds/node";

export type DisguiseRuntime = {
    product: string;
    yydsVersion: string;
    ping: string;
};

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
