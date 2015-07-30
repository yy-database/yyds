# GitHub Actions / npm publish for YYDS

## Workflow split

| Workflow | Trigger | Purpose |
|----------|---------|---------|
| **CI** (`../.github/workflows/ci.yml`) | push/PR to `dev` / `master` | `rustfmt`, `cargo test`, pnpm build/test |
| **Release** (`../.github/workflows/release-npm.yml`) | push tag `v*.*.*` only | npm + GitHub Release native-binding zips |

Release does **not** depend on CI. Tag `vX.Y.Z` → package version `X.Y.Z`.

```text
git tag v0.0.2
git push origin v0.0.2
```

## Auth: Trusted Publishing (OIDC)

No long-lived `NPM_TOKEN` for publish. Docs: https://docs.npmjs.com/trusted-publishers/

npm scope: **`@yyds`**

### Bootstrap `0.0.0` placeholders (once)

```text
npm login --auth-type=web
node scripts/publish-npm-placeholders.mjs
```

### Trusted Publisher fields (each package)

| Field | Value |
|-------|-------|
| Organization or user | `yy-database` |
| Repository | `yyds` |
| Workflow filename | `release-npm.yml` |
| Environment name | `NPM_PUBLISH` |
| Allowed actions | `npm publish` |

CLI helper: `node scripts/configure-trusted-publishers.mjs`

## Release job requirements

- `publish-npm`: `environment: NPM_PUBLISH`, `id-token: write`, Node ≥ 22.14, npm ≥ 11.5.1, no `NODE_AUTH_TOKEN`
- `github-release`: `contents: write`
