import { execSync } from "node:child_process";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const root = join(dirname(fileURLToPath(import.meta.url)), "..");
const crateDir = join(root, "projects/crates/yyds-wasm");
const outDir = join(root, "projects/packages/yyds-unknown-wasm32/lib");

execSync(`wasm-pack build "${crateDir}" --target web --out-dir "${outDir}" --release`, {
    cwd: root,
    stdio: "inherit",
    shell: true,
});
