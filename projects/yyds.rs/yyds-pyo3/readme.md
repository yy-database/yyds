# yyds-pyo3

Python extension module for YYDS node lifecycle and control-plane inspection, published as the `yyds` package by maturin.

The module deliberately exposes only `version`, `node_init`, `node_status`, `node_request_stop` and `node_wait_until_stopped`. Directory ownership comes from the cluster control API, and runtime status reading, the running generation and the stop wait come from the `yyds` runtime facade, which delegates them to the server node owner. This crate only maps parameters, results and error messages into stable Python exceptions. SQL ASTs, wire packets, key-value commands and YYDB types stay outside this boundary.

Initialization creates the node identity and validates the data directory. It does not bootstrap shard storage, so a node process started from the `yyds` CLI remains the only storage owner. `node_request_stop` requires a running node to have published `node.status.json`, and the stop marker is idempotent for that generation.
