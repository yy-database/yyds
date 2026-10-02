# yyds-types

Shared identity and error types for **YYDS**, the distributed VOS database.

YYDS is the network-facing sibling of embedded [YYDB](https://github.com/yy-database/yydb.rs). It does **not** reuse the single-file `.yydb` layout. Catalog truth lives in `.yyds` files and data shards in `.yykv` files.
