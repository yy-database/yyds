import { join } from "node:path";

/** Rust workspace lives under `projects/yyds.rs/`, not the repo root. */
export const RUST_WS_DIR = "projects/yyds.rs";
export const RUST_MANIFEST = join(RUST_WS_DIR, "Cargo.toml");
export const NAPI_MANIFEST = join(RUST_WS_DIR, "ffis/yyds-napi/Cargo.toml");
