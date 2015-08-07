# AGENTS

YYDS hybrid monorepo: `projects/crates/` (Rust), `projects/packages/` (`@yyds/*` npm).

## Maintainer skills (`.agents/skills/`)

For repository agents only. Do not link these from user-facing READMEs.

| Skill | Path | When to load |
|-------|------|--------------|
| YYDS release | `.agents/skills/yyds-release/SKILL.md` | npm publish, placeholders, Trusted Publisher, tag `v*.*.*` |

## Downstream skills (`@yyds/yyds-skills`)

User- and integrator-facing Agent Skills live in `projects/packages/yyds-skills/`.

Do not recreate root `documentation/` or `/docs`.

## Quick commands

```bash
pnpm install
pnpm test
pnpm placeholder:publish          # nifty publish --placeholder (publish.packages)
pnpm placeholder:publish:skills   # nifty publish --placeholder --package @yyds/yyds-skills
pnpm placeholder:trust            # OIDC trust for publish.packages
pnpm placeholder:trust:skills     # OIDC trust for @yyds/yyds-skills
```
