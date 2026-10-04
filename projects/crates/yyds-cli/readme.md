# yyds

The YYDS-owned cluster lifecycle command. `yyds` owns the node data-directory lease and starts protocol services attached to that node. Redis is currently the only attached service and remains a bounded loopback protocol probe, not a storage or cluster-health implementation.

Commands:

```text
yyds init --data-dir DIR --cluster-id ID --node-id ID --redis-port PORT
yyds start --data-dir DIR --cluster-id ID --node-id ID --redis-port PORT
yyds status --data-dir DIR
yyds stop --data-dir DIR
```

`start` keeps the lease until every attached service stops. `stop` writes a local control request and waits for the owner to release the lease. `status` reports persisted identity, process metadata, local lease ownership, and attached listener addresses. It does not claim membership, quorum, replication, leader authority, or query readiness.

The command is intentionally local-only. No remote administration, authentication, TLS, durable membership, or distributed health protocol is implemented yet.
