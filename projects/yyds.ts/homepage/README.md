# `@yydb/yyds-homepage`

YYDS product homepage and Chinese user guides. Built with [`@vmz/vmz`](https://www.npmjs.com/package/@vmz/vmz) `0.3.8`.

## Commands

```bash
pnpm --filter @yydb/yyds-homepage dev
pnpm --filter @yydb/yyds-homepage build
pnpm --filter @yydb/yyds-homepage document:check
```

## Routes

| Path | Purpose |
|------|---------|
| `/welcome` | Full-screen welcome |
| `/` | Product landing |
| `/d/` | Integrated Chinese documentation |
| `/wire` | Local `@yydb/yydb-client` frame encode/decode demo |

## Related packages

| Package | Use |
|---------|-----|
| [`@yydb/yyds`](https://www.npmjs.com/package/@yydb/yyds) | Node.js facade and CLI |
| [`@yydb/yydb-client`](https://www.npmjs.com/package/@yydb/yydb-client) | Browser and wire client |
| [`@yydb/yyds-webui`](https://www.npmjs.com/package/@yydb/yyds-webui) | Local node explorer |
| [`@yydb/yyds-skills`](https://www.npmjs.com/package/@yydb/yyds-skills) | Agent skills for integrators |

[YYDS overview](https://github.com/yy-database/yyds) · [Rust API](https://docs.rs/yyds)
