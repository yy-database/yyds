"""Thin aliases for the native ``yyds_pyo3`` extension module."""

from yyds_pyo3 import (
    CONTROL_VERSION,
    YydsError,
    node_init,
    node_request_stop,
    node_status,
    node_wait_until_stopped,
    version,
)

__all__ = [
    "CONTROL_VERSION",
    "YydsError",
    "node_init",
    "node_request_stop",
    "node_status",
    "node_wait_until_stopped",
    "version",
]
