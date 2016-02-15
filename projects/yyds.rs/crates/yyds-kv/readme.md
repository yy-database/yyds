# yyds-kv

The smallest storage unit in YYDS: a shard-local key/value engine backed by `.yykv` files.

This crate is **not** the embedded `.yydb` engine. It owns shard placement, key encoding, and record envelopes for the distributed data plane. **VOS typing lives in the `.yyds` catalog**, not inside individual KV records. Higher layers (catalog replication, VOS executor) build on top of `yyds-kv`.
