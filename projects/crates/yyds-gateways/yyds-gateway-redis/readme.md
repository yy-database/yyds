# yyds-gateway-redis

Redis protocol compatibility work for YYDS. The Rust `yyds-redis-server` binary serves bounded RESP2 PING, ECHO and QUIT probes on IPv4 loopback only. It preserves binary arguments and pipelines, limits connections to 64, and uses a 30-second I/O timeout.

Run `cargo run -p yyds-gateway-redis --bin yyds-redis-server -- --port 6379`, then use an original `redis-cli -h 127.0.0.1 -p 6379 PING`. This is a protocol probe, not a database server: storage commands, authentication, TLS and RESP3 are not implemented. The npm launcher is not yet connected to this listener.
