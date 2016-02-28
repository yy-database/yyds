# @yydb/mongodb

`mongo` is a minimal OP_MSG client that speaks to a running YYDS MongoDB handshake probe. The probe is started and maintained by the `mongod` wrapper, which delegates to the `yyds` executable.

```bash
mongod --data-dir DIR --cluster-id ID --node-id ID --port 27017
mongo --host 127.0.0.1 --port 27017 ping
```

`mongod` starts the YYDS node including its other protocol listeners and does not create a MongoDB-only supervisor. The probe answers `hello` / `ping` and explicitly rejects every collection command rather than fabricating results. Pass `--data-dir`, `--cluster-id`, and `--node-id`; set `YYDS_CLI_PATH` when the executable is not on `PATH`.

The package API re-exports `@yyds/yyds` unchanged.
