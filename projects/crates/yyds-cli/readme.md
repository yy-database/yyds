# yyds

The YYDS-owned node and cluster lifecycle command. `yyds` owns the node data-directory lease and starts protocol services attached to that node. Its first Redis execution path is a process-local, single-shard `.yykv` executor. It supports binary `GET`, option-free `SET`, single-key `DEL`, and connection-local `SELECT` for databases 0 through 15, but does not provide replication, elections, remote shard execution, or cluster health.

Commands:

```text
yyds init --data-dir DIR --cluster-id ID --node-id ID --redis-port PORT
yyds start --data-dir DIR --cluster-id ID --node-id ID --redis-port PORT
yyds status --data-dir DIR
yyds stop --data-dir DIR
```

`init` creates the local `.yykv` shard. `start` keeps the lease until every attached service stops. `stop` writes a local control request and waits for the owner to release the lease. `status` reports persisted identity, process metadata, local lease ownership, and attached listener addresses. It does not claim membership, quorum, replication, leader authority, or distributed query readiness.

The command is intentionally local-only. No remote administration, authentication, TLS, durable membership, or distributed health protocol is implemented yet.
