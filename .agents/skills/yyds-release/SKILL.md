---
name: yyds-release
description: >-
  YYDS npm Trusted Publisher release: Nifty placeholder/trust, tag `vX.Y.Z`, CI workflows.
  Load when publishing `@yyds/*`, cutting versions, or OIDC on `yy-database/yyds`.
---

# YYDS release

Maintainer skill. Integrators use `@yyds/yyds-skills`, not this file.

## Workflows

| Workflow | File | Trigger | Purpose |
|----------|------|---------|---------|
| CI | `.github/workflows/ci.yml` | push/PR `dev` / `master` | `rustfmt`, `cargo test`, pnpm build/test |
| Release | `.github/workflows/release-npm.yml` | push tag `v*.*.*` | npm OIDC for `publish.packages` |

Release does **not** gate on CI. Tag `vX.Y.Z` → npm version `X.Y.Z`.

## `nifty.config.ts`

| Key | Purpose |
|-----|---------|
| `trust.repo` | `yy-database/yyds` |
| `trust.file` | `release-npm.yml` |
| `trust.environment` | `NPM_PUBLISH` |
| `publish.packages` | Engine + gateways for tag release and **`pnpm placeholder:trust`** |

Integrator skills: **`projects/packages/yyds-skills`** (`@yyds/yyds-skills`). Use **`pnpm placeholder:trust:skills`** (`nifty trust --only @yyds/yyds-skills`).

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

`@yyds/yyds`, `@yyds/yyds-unknown-wasm32`, `@yyds/yyds-win32-x64`, `@yyds/yyds-linux-x64`, `@yyds/yyds-darwin-x64`, `@yyds/yyds-darwin-arm64`, `@yyds/redis`, `@yyds/mysql`, `@yyds/postgresql`, `@yyds/sqlite`
