# Serve security model

<div class="warn">

The reference implementation **`yyds start` has no authorization system** (no login / users / roles / ACL). Any client that reaches the socket can read and write the opened database. Frontend protocol clients carry no authentication either.

</div>

The **YYDB** product design therefore is:

1. **Bind loopback only by default** (`127.0.0.1` / `::1`).
2. Non-loopback binds require an explicit `--insecure-bind`, which prints a loud warning at startup.
3. **Never expose `yyds start` to the public internet.**
4. Local file permissions (who can read `.yydb`, who can start the process) remain part of the trust boundary.

When you need a control plane that is heavy by default — **built-in authorization, automatic audit and tuning reports** — use **YYDS** (for distributed enterprise customers) instead of growing YYDB into a mini YYDS. On the wire, the `YYDB`/`YYDS` magic is just the backend introducing itself with identical effect. The frontend does not insist on either, but the product capabilities differ entirely.
