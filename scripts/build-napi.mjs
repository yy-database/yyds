import { execSync } from "node:child_process";
import { copyFileSync, mkdirSync, readdirSync, unlinkSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const napiDir = join(root, "projects/crates/yyds-napi");

/** @type {Record<string, { packageDir: string, fileName: string }>} */
const ARTIFACTS = {
    "index.win32-x64-msvc.node": {
        packageDir: "yyds-win32-x64",
        fileName: "yyds-win32-x64-msvc.node",
    },
    "index.linux-x64-gnu.node": {
        packageDir: "yyds-linux-x64",
        fileName: "yyds-linux-x64-gnu.node",
    },
    "index.darwin-x64.node": {
        packageDir: "yyds-darwin-x64",
        fileName: "yyds-darwin-x64.node",
    },
    "index.darwin-arm64.node": {
        packageDir: "yyds-darwin-arm64",
        fileName: "yyds-darwin-arm64.node",
    },
};

function findBuiltNodes(dir) {
    return readdirSync(dir).filter((name) => name.endsWith(".node"));
}

execSync("pnpm exec napi build --platform --release", { cwd: napiDir, stdio: "inherit", shell: true });

const built = findBuiltNodes(napiDir);
if (built.length === 0) {
    throw new Error(`No .node artifacts under ${napiDir}`);
}

let copied = 0;
for (const name of built) {
    const mapping = ARTIFACTS[name];
    if (!mapping) {
        console.warn(`skip unmapped artifact: ${name}`);
        continue;
    }
    const libDir = join(root, "projects/packages", mapping.packageDir, "lib");
    mkdirSync(libDir, { recursive: true });
    for (const existing of readdirSync(libDir).filter((entry) => entry.endsWith(".node"))) {
        unlinkSync(join(libDir, existing));
    }
    const dest = join(libDir, mapping.fileName);
    copyFileSync(join(napiDir, name), dest);
    console.log(`copied ${name} -> ${dest}`);
    copied += 1;
}

if (copied === 0) {
    throw new Error(`Built nodes (${built.join(", ")}) did not match any platform package mapping`);
}
