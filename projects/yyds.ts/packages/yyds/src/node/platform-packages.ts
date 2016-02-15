const PLATFORM_PACKAGES: Record<string, string> = {
    "win32-x64": "@yydb/yyds-win32-x64",
    "darwin-x64": "@yydb/yyds-darwin-x64",
    "darwin-arm64": "@yydb/yyds-darwin-arm64",
    "linux-x64": "@yydb/yyds-linux-x64",
};

export function resolvePlatformPackage(): string {
    const key = `${process.platform}-${process.arch}`;
    const specifier = PLATFORM_PACKAGES[key];
    if (!specifier) {
        throw new Error(`@yydb/yyds/node: unsupported platform ${key}`);
    }
    return specifier;
}
