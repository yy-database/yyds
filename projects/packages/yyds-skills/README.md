# `@yyds/yyds-skills`

Agent Skills for integrators: distributed storage layout, YY wire protocol, and disguise gateways. Install when you need an agent to implement or review YYDS nodes, clients, or gateway shims.

## Install

```bash
npx skills add @yyds/yyds-skills --skill yyds-serve-protocol -y
```

List skills or install from the monorepo path before npm publish:

```bash
npx skills add @yyds/yyds-skills --list
npx skills add ./projects/packages/yyds-skills --skill yyds-storage -y
```

Requires Node.js 18+ for the installer. Skills are docs-only and do not replace `@yyds/yyds` or the Rust engine crates.

## Skills

| Skill | Use when |
|-------|----------|
| `yyds-serve-protocol` | TCP/WebSocket wire frames shared with YYDB family |
| `yyds-storage` | `.yyds` catalog + `.yykv` shard data plane |
| `yyds-gateways` | Redis / MySQL / PostgreSQL / SQLite disguise packages |
