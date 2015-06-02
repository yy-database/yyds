const PLATFORM_PACKAGES: Record<string, string> = {
    "win32-x64": "@yyds/yyds-win32-x64",
    "darwin-x64": "@yyds/yyds-darwin-x64",
    "darwin-arm64": "@yyds/yyds-darwin-arm64",
    "linux-x64": "@yyds/yyds-linux-x64",
};

export function resolvePlatformPackage(): string {
    const key = `${process.platform}-${process.arch}`;
    const specifier = PLATFORM_PACKAGES[key];
    if (!specifier) {
        throw new Error(`@yyds/yyds/node: unsupported platform ${key}`);
    }
    return specifier;
}
