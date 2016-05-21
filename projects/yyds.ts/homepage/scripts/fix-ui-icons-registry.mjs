/**
 * @vmz/ui-icons@0.3.8 publishes `./registry` as TypeScript. VMZ dev cannot strip types
 * from node_modules. Copy precompiled JS (from vmz build vendor output) into the package.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const homepage = path.resolve(here, "..");
const vendorSrc = path.join(homepage, "vendor", "vmz", "ui-icons", "src");
const targetPkg = path.join(homepage, "node_modules", "@vmz", "ui-icons");
const targetDist = path.join(targetPkg, "dist");

if (!fs.existsSync(vendorSrc) || !fs.existsSync(targetPkg)) {
    process.exit(0);
}

/**
 * Two workspace projects resolve `@vmz/ui-icons` to one shared pnpm store
 * entry, so their postinstall passes patch the same files at the same time.
 * Windows reports the collision as a transient `EBUSY`/`EPERM`.
 */
function withWindowsRetry(action) {
    for (let attempt = 0; ; attempt += 1) {
        try {
            return action();
        } catch (error) {
            const transient =
                error &&
                (error.code === "EBUSY" || error.code === "EPERM" || error.code === "EACCES");
            if (!transient || attempt >= 40) throw error;
            Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 50);
        }
    }
}

fs.mkdirSync(targetDist, { recursive: true });
for (const name of fs.readdirSync(vendorSrc)) {
    if (!name.endsWith(".js")) continue;
    withWindowsRetry(() => fs.copyFileSync(path.join(vendorSrc, name), path.join(targetDist, name)));
}

const pkgPath = path.join(targetPkg, "package.json");
const pkg = JSON.parse(fs.readFileSync(pkgPath, "utf8"));
pkg.exports = { ...(pkg.exports || {}), "./registry": "./dist/registry.js" };
const files = new Set([...(pkg.files || []), "dist"]);
pkg.files = [...files];
withWindowsRetry(() => fs.writeFileSync(pkgPath, `${JSON.stringify(pkg, null, 2)}\n`, "utf8"));
