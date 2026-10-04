import { spawn } from "node:child_process";

export type YydsProtocol = "redis" | "mysql" | "postgresql";

const portFlags: Record<YydsProtocol, string> = {
    redis: "--redis-port",
    mysql: "--mysql-port",
    postgresql: "--pgsql-port",
};
const defaultPorts: Partial<Record<YydsProtocol, number>> = { mysql: 3306, postgresql: 5432 };

export function yydsStartArguments(protocol: YydsProtocol, args: string[]): string[] {
    const result = ["start"];
    let protocolPortProvided = false;
    for (let index = 0; index < args.length; index += 1) {
        const argument = args[index];
        if (["--port", "-p", "-P"].includes(argument)) {
            const value = args[++index];
            if (value === undefined || value.startsWith("-"))
                throw new Error(`missing port after ${argument}`);
            result.push(portFlags[protocol], value);
            protocolPortProvided = true;
        } else {
            result.push(argument);
            if (argument === portFlags[protocol]) protocolPortProvided = true;
        }
    }
    const defaultPort = defaultPorts[protocol];
    if (!protocolPortProvided && defaultPort !== undefined)
        result.push(portFlags[protocol], String(defaultPort));
    return result;
}

export function launchYydsServer(protocol: YydsProtocol, args: string[]): Promise<number> {
    const executable = process.env.YYDS_CLI_PATH || "yyds";
    const child = spawn(executable, yydsStartArguments(protocol, args), {
        stdio: "inherit",
        windowsHide: true,
    });
    const forward = (signal: NodeJS.Signals) => child.kill(signal);
    const onInterrupt = () => forward("SIGINT");
    const onTerminate = () => forward("SIGTERM");
    process.once("SIGINT", onInterrupt);
    process.once("SIGTERM", onTerminate);

    return new Promise((resolve, reject) => {
        child.once("error", (error) =>
            reject(new Error(`cannot start '${executable}': ${error.message}`)),
        );
        child.once("close", (code, signal) => {
            process.removeListener("SIGINT", onInterrupt);
            process.removeListener("SIGTERM", onTerminate);
            resolve(code ?? (signal ? 1 : 0));
        });
    });
}
