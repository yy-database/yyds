# YY family documentation (published with this repository)

The frontend and the WebUI share `@yydb/yydb-client` and only speak the **YY binary protocol + VOS**. The `YYDB`/`YYDS` frame magic is the backend introducing itself (identical in effect, and the frontend does not insist on either).

| Product  | Positioning                                                                                              |
|----------|----------------------------------------------------------------------------------------------------------|
| **YYDB** | Light: single-file embedding, no built-in authorization/audit/reporting, this repository is the reference for `yyds start` |
| **YYDS** | Heavy: distributed enterprise deployments, with **default** built-in authorization, automatic audit and tuning reports |

Documentation pages can run a local frame codec demo and never connect to a live serve by default.
