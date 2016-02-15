/**
 * Publish prepared projects/yyds.ts/* packages. Skips versions already on the registry
 * (idempotent retry after partial failure).
 *
 * Env:
 *   PUBLISH_PACKAGES — space-separated package dir names (optional)
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

const root = process.cwd();
const PLATFORM_DIRS = new Set(["yyds-win32-x64", "yyds-linux-x64", "yyds-darwin-x64", "yyds-darwin-arm64"]);
const EMULATOR_DIRS = new Set(["redis", "mysql", "postgresql"]);
const dirs = (
    process.env.PUBLISH_PACKAGES ||
    "yyds-win32-x64 yyds-linux-x64 yyds-darwin-x64 yyds-darwin-arm64 yyds redis mysql postgresql"
)
    .trim()
    .split(/\s+/)
    .filter(Boolean);

function resolvePackageDir(dirName) {
    if (PLATFORM_DIRS.has(dirName)) {
        return path.join(root, "projects", "yyds.ts", "platforms", dirName);
    }
    if (EMULATOR_DIRS.has(dirName)) {
        return path.join(root, "projects", "yyds.ts", "emulators", dirName);
    }
    return path.join(root, "projects", "yyds.ts", "packages", dirName);
}

function npmViewVersion(name, version) {
    const r = spawnSync(
        "npm",
        ["view", `${name}@${version}`, "version", "--registry", "https://registry.npmjs.org"],
        { encoding: "utf8", shell: true },
    );
    if (r.status !== 0) return null;
    return (r.stdout || "").trim() || null;
}

function publishDir(dirName) {
    const dir = resolvePackageDir(dirName);
    const pkgPath = path.join(dir, "package.json");
    const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
    const { name, version } = pkg;
    if (!name || !version) {
        throw new Error(`invalid package.json in ${dirName}`);
    }

    const existing = npmViewVersion(name, version);
    if (existing === version) {
        console.log(`skip ${name}@${version} (already published)`);
        return "skipped";
    }

    console.log(`publish ${name}@${version}`);
    const r = spawnSync("npm", ["publish", "--access", "public"], {
        cwd: dir,
        stdio: "inherit",
        shell: true,
        env: process.env,
    });
    if (r.status !== 0) {
        const again = npmViewVersion(name, version);
        if (again === version) {
            console.log(`skip ${name}@${version} (exists after publish error)`);
            return "skipped";
        }
        throw new Error(`npm publish failed for ${name}@${version}`);
    }
    return "published";
}

const summary = { published: [], skipped: [] };
for (const dir of dirs) {
    const status = publishDir(dir);
    summary[status === "published" ? "published" : "skipped"].push(dir);
}
console.log(JSON.stringify(summary));
