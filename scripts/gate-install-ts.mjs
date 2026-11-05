#!/usr/bin/env node
/**
 * Install-state gate for the TypeScript packages.
 *
 * `exports` pointing at `src/*.ts` only works inside the workspace. A tarball
 * installed by a user runs on plain Node, so every package is packed, unpacked
 * into a fresh directory outside the repo, and imported without any TS loader.
 * The native binding is loaded and pinged through the installed `exports`, so a
 * missing `dist` build fails here instead of at the user's first import.
 */
import { spawnSync } from "node:child_process";
import {
    mkdirSync,
    mkdtempSync,
    readdirSync,
    readFileSync,
    rmSync,
    writeFileSync,
} from "node:fs";
import { dirname, join, resolve } from "node:path";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const PACKAGES = [
    { name: "@yydb/yyds", dir: "projects/yyds.ts/packages/yyds" },
    { name: "@yydb/redis", dir: "projects/yyds.ts/emulators/redis" },
    { name: "@yydb/mysql", dir: "projects/yyds.ts/emulators/mysql" },
    { name: "@yydb/postgresql", dir: "projects/yyds.ts/emulators/postgresql" },
    { name: "@yydb/mongodb", dir: "projects/yyds.ts/emulators/mongodb" },
];
const PLATFORM_PACKAGES = {
    "win32-x64": "yyds-win32-x64",
    "darwin-x64": "yyds-darwin-x64",
    "darwin-arm64": "yyds-darwin-arm64",
    "linux-x64": "yyds-linux-x64",
};

function fail(message) {
    console.error(`FAIL install-ts: ${message}`);
    process.exit(1);
}

function quote(argument) {
    return /[ \t"]/.test(argument) ? `"${argument.replaceAll('"', '\\"')}"` : argument;
}

function report(command, args, result) {
    if (result.status !== 0) {
        fail(`${command} ${args.join(" ")} exited with ${result.status}\n${result.stdout ?? ""}${result.stderr ?? ""}`);
    }
    return result.stdout ?? "";
}

/** pnpm and tar are shell shims on Windows, so run them through the shell. */
function runShell(command, args, cwd) {
    const result = spawnSync([command, ...args.map(quote)].join(" "), {
        cwd,
        encoding: "utf8",
        shell: true,
    });
    return report(command, args, result);
}

function runNode(args, cwd) {
    const result = spawnSync(process.execPath, args, { cwd, encoding: "utf8" });
    return report(process.execPath, args, result);
}

function build(dir) {
    runShell("pnpm", ["exec", "tsc", "-p", "tsconfig.build.json"], dir);
}

function pack(dir, packDir) {
    for (const entry of readdirSync(packDir)) {
        if (entry.endsWith(".tgz")) rmSync(join(packDir, entry));
    }
    runShell("pnpm", ["pack", "--pack-destination", packDir], dir);
    const tarballs = readdirSync(packDir).filter((entry) => entry.endsWith(".tgz"));
    if (tarballs.length !== 1) fail(`expected one tarball from ${dir}, got ${tarballs.length}`);
    return join(packDir, tarballs[0]);
}

function unpack(tarball, destination) {
    mkdirSync(destination, { recursive: true });
    runShell("tar", ["-xzf", tarball, "-C", destination, "--strip-components=1"], dirname(destination));
}

function installPackage(pkg, installDir, packDir) {
    build(join(root, pkg.dir));
    const tarball = pack(join(root, pkg.dir), packDir);
    const dest = join(installDir, "node_modules", ...pkg.name.split("/"));
    unpack(tarball, dest);
    for (const field of ["exports", "files"]) {
        const manifest = JSON.parse(readFileSync(join(dest, "package.json"), "utf8"));
        if (typeof manifest[field] !== "object") fail(`${pkg.name} tarball has no ${field}`);
    }
}

const platformKey = `${process.platform}-${process.arch}`;
const platformPackage = PLATFORM_PACKAGES[platformKey];
if (!platformPackage) fail(`no platform package mapped for ${platformKey}`);

const tempRoot = mkdtempSync(join(tmpdir(), "yyds-install-gate-"));
const packDir = join(tempRoot, "packs");
const installDir = join(tempRoot, "app");
mkdirSync(packDir, { recursive: true });
mkdirSync(installDir, { recursive: true });
writeFileSync(join(installDir, "package.json"), `${JSON.stringify({ name: "yyds-install-gate", private: true, type: "module" })}\n`);

try {
    for (const pkg of PACKAGES) installPackage(pkg, installDir, packDir);

    const platformDir = join(root, "projects/yyds.ts/platforms", platformPackage);
    const platformTarball = pack(platformDir, packDir);
    unpack(platformTarball, join(installDir, "node_modules", "@yydb", platformPackage));

    const probe = join(installDir, "probe.mjs");
    writeFileSync(
        probe,
        `import assert from "node:assert/strict";
const yyds = await import("@yydb/yyds/node");
assert.equal(typeof yyds.yydsStartArguments, "function");
assert.deepEqual(yyds.yydsStartArguments("redis", ["--port", "6380"]), ["--redis-port", "6380"]);
assert.equal(yyds.isYydsNativeInstalled(), true, "the platform package must resolve after install");
const native = yyds.loadYydsNative();
assert.equal(typeof native.yydsVersion(), "string");
assert.ok(native.yydsVersion().length > 0);
assert.equal(native.ping(), "ok");
for (const name of ["redis", "mysql", "postgresql", "mongodb"]) {
    const emulator = await import("@yydb/" + name + "/node");
    assert.equal(typeof emulator.loadYydsNative, "function", name + " must re-export loadYydsNative");
    assert.equal(emulator.loadYydsNative().ping(), "ok", name + " must reach the native runtime");
}
console.log("PASS install-ts: imports and native calls succeeded on " + process.platform + "-" + process.arch);
`,
    );
    const probeOutput = runNode([probe], installDir).trim();
    if (probeOutput) console.log(probeOutput);
    console.log(`native ping ok, 5 packages resolved from ${installDir}`);
} finally {
    rmSync(tempRoot, { recursive: true, force: true });
}
