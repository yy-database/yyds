/**
 * Create (if needed) a GitHub Release for the current tag and upload engine zips.
 * Existing release / existing assets are skipped (retry-safe).
 */
import fs from "node:fs";
import path from "node:path";
import { spawnSync } from "node:child_process";

const tag = (process.env.GITHUB_REF_NAME || "").trim();
const repo = (process.env.GITHUB_REPOSITORY || "").trim();
const enginesDir = process.env.ENGINES_DIR || "_engines";
const zipsDir = process.env.ZIPS_DIR || "_release-zips";

if (!tag.startsWith("v")) {
    console.error("GITHUB_REF_NAME must be a v* tag");
    process.exit(1);
}
if (!repo) {
    console.error("GITHUB_REPOSITORY is required");
    process.exit(1);
}

const packages = [
    { pkg: "yyds-win32-x64", bin: "yyds-win32-x64-msvc.node" },
    { pkg: "yyds-linux-x64", bin: "yyds-linux-x64-gnu.node" },
    { pkg: "yyds-darwin-x64", bin: "yyds-darwin-x64.node" },
    { pkg: "yyds-darwin-arm64", bin: "yyds-darwin-arm64.node" },
];

function run(cmd, args, opts = {}) {
    return spawnSync(cmd, args, {
        encoding: "utf8",
        shell: false,
        ...opts,
    });
}

function gh(args, opts = {}) {
    return run("gh", args, {
        env: {
            ...process.env,
            GH_TOKEN: process.env.GH_TOKEN || process.env.GITHUB_TOKEN || "",
        },
        ...opts,
    });
}

function walkFiles(dir, acc = []) {
    if (!fs.existsSync(dir)) return acc;
    for (const name of fs.readdirSync(dir)) {
        const full = path.join(dir, name);
        if (fs.statSync(full).isDirectory()) walkFiles(full, acc);
        else acc.push(full);
    }
    return acc;
}

function findEngine(bin) {
    const files = walkFiles(enginesDir);
    return files.find((f) => path.basename(f) === bin);
}

fs.mkdirSync(zipsDir, { recursive: true });

const version = tag.slice(1);
for (const { pkg, bin } of packages) {
    const src = findEngine(bin);
    if (!src) {
        console.error(`missing engine binary ${bin}`);
        process.exit(1);
    }
    const zipName = `${pkg}-${version}.zip`;
    const zipPath = path.join(zipsDir, zipName);
    if (fs.existsSync(zipPath)) {
        fs.unlinkSync(zipPath);
    }
    const r = run("zip", ["-j", zipPath, src], { shell: process.platform === "win32" });
    if (r.status !== 0) {
        console.error(r.stderr || r.stdout);
        process.exit(r.status ?? 1);
    }
    console.log(`wrote ${zipPath}`);
}

let release = gh(["release", "view", tag, "--repo", repo, "--json", "id"]);
if (release.status !== 0) {
    const create = gh(["release", "create", tag, "--repo", repo, "--title", tag, "--generate-notes"]);
    if (create.status !== 0) {
        console.error(create.stderr || create.stdout);
        process.exit(create.status ?? 1);
    }
    console.log(`created release ${tag}`);
}

for (const { pkg } of packages) {
    const zipName = `${pkg}-${version}.zip`;
    const zipPath = path.join(zipsDir, zipName);
    const assetName = zipName;
    const listed = gh(["release", "view", tag, "--repo", repo, "--json", "assets"]);
    if (listed.status === 0) {
        try {
            const assets = JSON.parse(listed.stdout).assets || [];
            if (assets.some((a) => a.name === assetName)) {
                console.log(`skip asset ${assetName} (exists)`);
                continue;
            }
        } catch {
            // continue upload
        }
    }
    const upload = gh(["release", "upload", tag, zipPath, "--repo", repo, "--clobber"]);
    if (upload.status !== 0) {
        console.error(upload.stderr || upload.stdout);
        process.exit(upload.status ?? 1);
    }
    console.log(`uploaded ${assetName}`);
}
