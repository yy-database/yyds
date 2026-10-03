import { createRequire } from "node:module";

import type { YydsBindings } from "../bindings.ts";
import { resolvePlatformPackage } from "./platform-packages.ts";
import type { YydsSqliteBindings } from "./sqlite-bindings.ts";

const require = createRequire(import.meta.url);

function normalizeBinding(module: Record<string, unknown>): YydsBindings {
    const binding = (module.default ?? module) as Record<string, unknown>;
    return {
        yydsVersion: () => String((binding.yydsVersion as () => string)()),
        ping: () => String((binding.ping as () => string)()),
    };
}

/** Load the platform-specific Node-API binding. */
export function loadYydsNative(): YydsBindings {
    const specifier = resolvePlatformPackage();
    return normalizeBinding(require(specifier) as Record<string, unknown>);
}

/** Load the independent SQLite snapshot reader, not a YYDS execution session. */
export function loadYydsSqliteNative(): YydsSqliteBindings {
    const module = require(resolvePlatformPackage()) as Record<string, unknown>;
    const binding = (module.default ?? module) as Record<string, unknown>;
    if (typeof binding.SqliteSnapshot !== "function") {
        throw new Error(
            "Native addon lacks the SQLite snapshot reader. Rebuild the native package.",
        );
    }
    return { SqliteSnapshot: binding.SqliteSnapshot as YydsSqliteBindings["SqliteSnapshot"] };
}

/** Whether the current platform optional dependency is installed. */
export function isYydsNativeInstalled(): boolean {
    try {
        resolvePlatformPackage();
        require.resolve(resolvePlatformPackage());
        return true;
    } catch {
        return false;
    }
}
