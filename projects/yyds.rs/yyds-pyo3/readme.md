# yyds-pyo3

Python extension module for YYDS node lifecycle and control-plane inspection, published as the `yyds` package by maturin.

The module deliberately exposes only `version`, `node_init`, `node_status`, `node_request_stop` and `node_wait_until_stopped`. It maps `yyds-control` ownership and the versioned runtime status file into stable Python exceptions. SQL ASTs, wire packets, key-value commands and YYDB types stay outside this boundary.

Initialization creates the node identity and validates the data directory. It does not bootstrap shard storage, so a node process started from the `yyds` CLI remains the only storage owner. `node_request_stop` requires a running node to have published `node.status.json`, and the stop marker is idempotent for that generation.
