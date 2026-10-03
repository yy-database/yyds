/**
 * Publish prepared projects/packages/* packages. Skips versions already on the registry
 * (idempotent retry after partial failure).
 *
 * Env:
 *   PUBLISH_PACKAGES — space-separated package dir names (optional)
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

const root = process.cwd();
const dirs = (
    process.env.PUBLISH_PACKAGES ||
    "yyds-win32-x64 yyds-linux-x64 yyds-darwin-x64 yyds-darwin-arm64 yyds-unknown-wasm32 yyds redis mysql postgresql sqlite"
)
    .trim()
    .split(/\s+/)
    .filter(Boolean);

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
    const dir = path.join(root, "projects", "packages", dirName);
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
