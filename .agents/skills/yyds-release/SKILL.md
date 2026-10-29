---
name: yyds-release
description: >-
  YYDS npm Trusted Publisher release: Nifty placeholder/trust, tag `vX.Y.Z`,
  change-logs, GitHub Release notes, CI workflows. Load when publishing
  `@yydb/*`, cutting versions, OIDC on `yy-database/yyds`, or syncing release notes.
---

# YYDS release

Maintainer skill. Integrators use `@yydb/yyds-skills`, not this file.

## Workflows

| Workflow | File | Trigger | Purpose |
|----------|------|---------|---------|
| Check Rust | `.github/workflows/check-rust.yml` | push/PR `dev` / `master` | `rustfmt`, `cargo test` |
| Check TypeScript | `.github/workflows/check-typescript.yml` | push/PR `dev` / `master` | wasm, pnpm typecheck, napi, tests |
| Check Python | `.github/workflows/check-python.yml` | push/PR path filters | `yyds-pyo3` + SQL real clients |
| Release | `.github/workflows/release-npm.yml` | push tag `v*.*.*` | npm OIDC for `publish.packages` |

Release does **not** gate on CI. Tag `vX.Y.Z` ? npm version `X.Y.Z`.

## `nifty.config.ts`

| Key | Purpose |
|-----|---------|
| `authorMap` | `documentation/maintenance/author-github.json` |
| `changelog.repo` | `yy-database/yyds` |
| `changelog.releasesDir` | `documentation/maintenance/releases` |
| `trust.npm.repo` | `yy-database/yyds` |
| `trust.npm.file` | `release-npm.yml` |
| `trust.npm.environment` | `NPM_PUBLISH` |
| `publish.packages` | Engine + gateways for tag release and **`pnpm placeholder:trust`** |

Integrator skills: **`projects/yyds.ts/packages/yyds-skills`** (`@yydb/yyds-skills`). Use **`pnpm placeholder:trust:skills`** (`nifty trust --only @yydb/yyds-skills`).

## Placeholder bootstrap

```bash
pnpm placeholder:publish
pnpm placeholder:publish:skills
```

## Trusted Publisher

```bash
pnpm placeholder:trust
pnpm placeholder:trust:skills
```

| Field | Value |
|-------|-------|
| Organization or user | `yy-database` |
| Repository | `yyds` |
| Workflow filename | `release-npm.yml` |
| Environment name | `NPM_PUBLISH` |

## `publish.packages`

`@yydb/yyds`, `@yydb/yyds-win32-x64`, `@yydb/yyds-linux-x64`, `@yydb/yyds-darwin-x64`, `@yydb/yyds-darwin-arm64`, `@yydb/redis`, `@yydb/mysql`, `@yydb/postgresql`

## Maintainer release notes

`documentation/maintenance/` is **not** user-facing product documentation. Never link internal design or decision-record paths from committed files here.

| Path | Purpose |
|------|---------|
| `documentation/maintenance/author-github.json` | Commit email ? GitHub login |
| `documentation/maintenance/releases/vX.Y.Z.md` | Published GitHub Release body (commit this) |
| `documentation/maintenance/releases/vX.Y.Z.reference.md` | Nifty draft index (**gitignored**) |

```bash
# Draft from tags (writes gitignored *.reference.md)
nifty change-logs --version X.Y.Z --write

# Hand-edit into the published body, then sync GitHub Release
gh release create vX.Y.Z --title vX.Y.Z --notes-file documentation/maintenance/releases/vX.Y.Z.md --verify-tag
gh release edit vX.Y.Z --notes-file documentation/maintenance/releases/vX.Y.Z.md
```

## Release checklist

1. Land work on `dev`, keep CI green
2. Bump workspace packages (`?? Bump yyds workspace packages to X.Y.Z`)
3. Tag `vX.Y.Z` on the bump commit, push branch + tag (OIDC publish)
4. `nifty change-logs --version X.Y.Z --write` then edit `releases/vX.Y.Z.md` then `gh release create` / `edit`
5. `nifty trust` only for **new** package names not yet on Trusted Publisher
