import { createRequire } from "node:module";

import type { YydsBindings } from "../bindings.ts";
import { resolvePlatformPackage } from "./platform-packages.ts";

const require = createRequire(import.meta.url);

function bindingModule(): Record<string, unknown> {
    const module = require(resolvePlatformPackage()) as Record<string, unknown>;
    return (module.default ?? module) as Record<string, unknown>;
}

/** Load the platform-specific Node-API binding. */
export function loadYydsNative(): YydsBindings {
    const binding = bindingModule();
    return binding as unknown as YydsBindings;
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
