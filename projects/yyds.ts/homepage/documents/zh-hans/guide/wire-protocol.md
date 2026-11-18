# 二进制协议摘要

传输层是 **YY 二进制帧**，不是 JSON REST，也不是 SQL。DDL/查询语言侧是 **VOS**。

- 前缀：产品魔数 **`YYDB` | `YYDS`**（后端自称，效果相同，前端不强求一致）+ 四位版本 **`0000`**（演进 **`0001`**、…）
- 头：`msg_type` / `flags` / `request_id` / `body_len`（小端）
- 传输：TCP，或 WebSocket `/wire`（帧体相同）

前端用 **`@yydb/yydb-client`**（以及本站 / WebUI）按协议说话，**不探测**后端是 YYDB 还是 YYDS。包名带 `yydb` 只因为当前仓库提供参考实现。

Rust：

- [`yydb`](https://crates.io/crates/yydb)：YYDB 嵌入式门面
- [`yydb-client`](https://crates.io/crates/yydb-client)：YYDB 远程 TCP 客户端

完整说明（Agent skill）：[`yydb-serve-protocol`](https://github.com/yy-database/yyds/tree/dev/projects/yydb.ts/packages/yydb-skills/skills/yydb-serve-protocol/SKILL.md)（[`@yydb/yyds-skills`](https://www.npmjs.com/package/@yydb/yyds-skills)）。

当前帧类型：Hello、Info、SchemaGet / SchemaEnsure、KvGet / KvPut、MicroRegister / MicroHostInvoke、ScalarCall、Authenticate（`19`）、Prepare / ExecutePrepared，错误统一为 `Error`（`255`，body 是结构化诊断 JSON），完整消息码表见 skill。

YYDB 与 YYDS 共用同一套消息码与编解码，唯一 owner 是 Rust 共享协议包；`msg_type` 不按产品分叉，也不保留旧编号兼容。

本地演示见站点 `/wire` 页面。
