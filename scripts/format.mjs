#!/usr/bin/env node
/**
 * Format the YYDS workspace:
 *   - Rust: `cargo fmt`
 *   - JSON / JS / TS: Biome (`biome.json`: 4 spaces, line width 100)
 *
 * `biome.json#files.includes` keeps the gate on source code only:
 *   - formatted: `package.json`, `biome.json`, `scripts/**`, `projects/packages/**`
 *   - `node_modules`, `dist`, `target` are install/build outputs
 *   - `pnpm-lock.yaml` is a package-manager artifact
 *   - `*.md` is out of scope: Biome's Markdown formatter is experimental and docs
 *     are not part of this gate
 *
 *   node scripts/format.mjs           # write
 *   node scripts/format.mjs --check   # check only
 */
import { execFileSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const rootDir = join(dirname(fileURLToPath(import.meta.url)), "..");
const checkOnly = process.argv.includes("--check");

/**
 * @param {string} file
 * @param {string[]} args
 */
function runFile(file, args) {
    console.log(`$ ${file} ${args.join(" ")}`);
    execFileSync(file, args, {
        cwd: rootDir,
        stdio: "inherit",
        env: process.env,
        shell: process.platform === "win32",
    });
}

console.log(checkOnly ? "=== Format check ===\n" : "=== Format (write) ===\n");

console.log("--- Rust ---");
runFile("cargo", ["fmt", "--all", ...(checkOnly ? ["--check"] : [])]);

console.log("\n--- Biome (json/js/ts, 4 spaces) ---");
if (checkOnly) {
    // `biome format` has no `--check` in Biome 2. `check` reports formatter
    // differences as errors once lint and assist actions are disabled.
    runFile("pnpm", [
        "exec",
        "biome",
        "check",
        "--formatter-enabled=true",
        "--linter-enabled=false",
        "--assist-enabled=false",
        ".",
    ]);
} else {
    runFile("pnpm", ["exec", "biome", "format", "--write", "."]);
}

console.log(checkOnly ? "\n=== Format check passed ===" : "\n=== Format complete ===");
