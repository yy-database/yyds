# yyds (Python)

Synchronous Python control-plane client for local YYDS nodes. The package is a
thin wrapper around the `yyds_pyo3` extension module and keeps the surface
narrow: identity initialization, ownership and runtime status inspection,
stop-marker creation and a bounded stop wait. SQL ASTs, wire packets, key-value
commands and storage internals are not exposed.

## Build

The extension module is compiled with [maturin](https://www.maturin.rs/) from the
`yyds-pyo3` Rust crate:

```powershell
pip install maturin
cd projects/yyds.py/packages/yyds
maturin develop
```

## Usage

```python
from yyds import Node

node = Node("./data")
node.init("example-cluster", "example-node")
print(node.status()["ownership"])
node.request_stop(timeout_ms=5000)
```

`request_stop` creates a stop marker for the generation published by a running
node and then waits for the directory lock to be released. Without a running
node it raises `YydsError` instead of writing an orphan marker. Failure
messages are stable strings and never contain Rust debug output.

## Tests

```powershell
python -m unittest discover -s projects/yyds.py/packages/yyds/tests
```

Tests fail rather than skip when the native module is importable but broken, and
skip with an explicit reason when `yyds_pyo3` has not been built yet.
