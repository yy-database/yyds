import type { StartNodeOptions } from "./runtime.ts";
import { parseStartOptions, runNodeStart } from "./runtime.ts";

export type YydsProtocol = "redis" | "mysql" | "postgresql" | "mongodb";

/** Maps disguise-server `--port` flags to protocol-specific yyds start flags. */
export function yydsStartArguments(protocol: YydsProtocol, args: string[]): string[] {
    const portFlags: Record<YydsProtocol, string> = {
        redis: "--redis-port",
        mysql: "--mysql-port",
        postgresql: "--pgsql-port",
        mongodb: "--mongo-port",
    };
    const defaultPorts: Partial<Record<YydsProtocol, number>> = {
        mysql: 3306,
        postgresql: 5432,
    };
    const normalized: string[] = [];
    let sawPort = false;

    for (let index = 0; index < args.length; index += 1) {
        const argument = args[index];
        if (["--port", "-p", "-P"].includes(argument)) {
            const value = args[index + 1];
            if (value === undefined || value.startsWith("--")) {
                throw new Error(`missing port after ${argument}`);
            }
            normalized.push(portFlags[protocol], value);
            sawPort = true;
            index += 1;
            continue;
        }
        normalized.push(argument);
    }

    const defaultPort = defaultPorts[protocol];
    if (!sawPort && defaultPort !== undefined) {
        normalized.push(portFlags[protocol], String(defaultPort));
    }
    return normalized;
}

/** Runs a node lifecycle in-process through orthogonal native blocks. */
export async function launchYydsServer(protocol: YydsProtocol, args: string[]): Promise<number> {
    try {
        await runNodeStart(parseStartOptions(protocol, yydsStartArguments(protocol, args)));
        return 0;
    } catch (error) {
        const message = error instanceof Error ? error.message : String(error);
        throw new Error(message);
    }
}

export type { StartNodeOptions };
