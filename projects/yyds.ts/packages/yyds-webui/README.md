# `@yydb/yyds-webui`

Local browser UI for a running YYDS wire endpoint — inspect server info and VOS schema, try key/value reads and writes while developing. Built on [VMZ](https://github.com/doki-land/vmz-framework) and `@vmz/ui` console components.

## Quick start

Open a node in one step:

```text
yyds webui --data-dir ./node-data --cluster-id local --node-id node-1
```

This starts `yyds start` with `--wire-port`, serves the WebUI on `http://127.0.0.1:8780`, and opens your browser with the wire endpoint pre-filled. The page connects automatically when launched from the CLI.

## Manual workflow

```text
yyds start --data-dir ./node-data --cluster-id local --node-id node-1 --wire-port 7700
yyds webui --endpoint ws://127.0.0.1:7700/wire --no-open
```

Or develop the UI package directly:

```text
pnpm --filter @yydb/yyds-webui dev
```

`pnpm --filter @yydb/yyds build` bundles `dist/cdn` into `@yydb/yyds/webui` so `yyds webui` works from a single published package.

## Capabilities

The UI uses [`@yydb/yydb-client`](https://www.npmjs.com/package/@yydb/yydb-client) over WebSocket `/wire`:

- server information;
- load or ensure a VOS schema and version;
- read and write string key/value records.

The reference node has no authentication. Keep the UI and endpoint on your machine or a trusted private network.

[`@yydb/yyds`](https://www.npmjs.com/package/@yydb/yyds) · [YYDS overview](https://github.com/yy-database/yyds)
