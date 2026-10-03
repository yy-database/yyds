import fs from "node:fs";
import path from "node:path";

const root = process.cwd();

/** Prefer explicit INPUT_VERSION; else strip leading `v` from tag `vX.Y.Z`. */
function resolveVersion() {
    const input = (process.env.INPUT_VERSION || "").trim().replace(/^v/i, "");
    if (input) return input;
    const ref = (process.env.GITHUB_REF || "").trim();
    const fromRef = ref.match(/^refs\/tags\/v(.+)$/i);
    if (fromRef) return fromRef[1];
    const name = (process.env.GITHUB_REF_NAME || "").trim();
    if (/^v\d+\.\d+\.\d+/i.test(name)) return name.slice(1);
    return "";
}

const version = resolveVersion();

function walkFiles(dir, acc = []) {
    if (!fs.existsSync(dir)) return acc;
    for (const name of fs.readdirSync(dir)) {
        const full = path.join(dir, name);
        const st = fs.statSync(full);
        if (st.isDirectory()) walkFiles(full, acc);
        else acc.push(full);
    }
    return acc;
}

function findArtifact(basename) {
    const files = walkFiles(path.join(root, "_engines"));
    const hit = files.find((f) => path.basename(f) === basename);
    if (!hit) {
        throw new Error(`missing artifact ${basename} under _engines/`);
    }
    return hit;
}

function copyDirContents(srcDir, destDir) {
    fs.mkdirSync(destDir, { recursive: true });
    for (const name of fs.readdirSync(srcDir)) {
        const src = path.join(srcDir, name);
        const dest = path.join(destDir, name);
        if (fs.statSync(src).isDirectory()) {
            copyDirContents(src, dest);
        } else {
            fs.copyFileSync(src, dest);
            try {
                fs.chmodSync(dest, 0o755);
            } catch {
                // windows
            }
        }
    }
}

function writeJson(filePath, data) {
    fs.writeFileSync(filePath, `${JSON.stringify(data, null, 2)}\n`);
}

function pinWorkspaceVersions(j, ver) {
    if (!ver) return;
    for (const section of ["dependencies", "optionalDependencies", "devDependencies"]) {
        const deps = j[section];
        if (!deps) continue;
        for (const key of Object.keys(deps)) {
            if (deps[key] === "workspace:*") {
                deps[key] = ver;
            }
        }
    }
}

function preparePackageJson(pkgDir) {
    const pkgPath = path.join(pkgDir, "package.json");
    const j = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
    delete j.private;
    if (version) {
        j.version = version;
        pinWorkspaceVersions(j, version);
    }
    const ghRepo = (process.env.GITHUB_REPOSITORY || "").trim() || "yy-database/yyds";
    const directory = path.relative(root, pkgDir).split(path.sep).join("/");
    j.repository = {
        type: "git",
        url: `git+https://github.com/${ghRepo}.git`,
        directory,
    };
    j.homepage = `https://github.com/${ghRepo}/tree/dev/${directory}#readme`;
    j.bugs = { url: `https://github.com/${ghRepo}/issues` };
    j.publishConfig = { ...(j.publishConfig || {}), access: "public" };
    if (!j.license) j.license = "MPL-2.0";
    writeJson(pkgPath, j);
}

const platforms = [
    ["yyds-win32-x64", "yyds-win32-x64-msvc.node"],
    ["yyds-linux-x64", "yyds-linux-x64-gnu.node"],
    ["yyds-darwin-x64", "yyds-darwin-x64.node"],
    ["yyds-darwin-arm64", "yyds-darwin-arm64.node"],
];

for (const [pkg, binding] of platforms) {
    const destDir = path.join(root, "projects", "packages", pkg, "lib");
    fs.mkdirSync(destDir, { recursive: true });
    for (const existing of fs.readdirSync(destDir).filter((name) => name.endsWith(".node"))) {
        fs.unlinkSync(path.join(destDir, existing));
    }
    const dest = path.join(destDir, binding);
    fs.copyFileSync(findArtifact(binding), dest);
    try {
        fs.chmodSync(dest, 0o755);
    } catch {
        // windows
    }
    preparePackageJson(path.join(root, "projects", "packages", pkg));
}

const wasmSrc = path.join(root, "_engines", "engine-wasm");
if (!fs.existsSync(wasmSrc)) {
    throw new Error("missing _engines/engine-wasm");
}
const wasmLib = path.join(root, "projects", "packages", "yyds-unknown-wasm32", "lib");
fs.rmSync(wasmLib, { recursive: true, force: true });
copyDirContents(wasmSrc, wasmLib);
preparePackageJson(path.join(root, "projects", "packages", "yyds-unknown-wasm32"));

for (const dir of ["yyds", "redis", "mysql", "postgresql", "sqlite"]) {
    preparePackageJson(path.join(root, "projects", "packages", dir));
}

console.log("npm packages prepared", version || "(package.json versions)");
