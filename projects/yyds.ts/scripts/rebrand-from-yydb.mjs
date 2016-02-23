/**
 * One-shot rebrand helper after copying yydb homepage/webui skeleton.
 */
import fs from "node:fs";
import path from "node:path";

const roots = [
    path.resolve("homepage"),
    path.resolve("packages/yyds-webui"),
];

const replacements = [
    ["@yydb/yydb-homepage", "@yydb/yyds-homepage"],
    ["@yydb/yydb-webui", "@yydb/yyds-webui"],
    ["@yydb/yydb-skills", "@yydb/yyds-skills"],
    ["@yydb/yydb-client", "@yydb/yydb-client"],
    ["projects/yydb.ts/packages/yydb-webui", "projects/yyds.ts/packages/yyds-webui"],
    ["projects/yydb.ts/homepage", "projects/yyds.ts/homepage"],
    ["github.com/yy-database/yydb", "github.com/yy-database/yyds"],
    ["yydb webui", "yyds webui"],
    ["yydb serve", "yyds start"],
    ["YYDB WebUI", "YYDS WebUI"],
    ["YYDB —", "YYDS —"],
    ["欢迎 · YYDB", "欢迎 · YYDS"],
    ["线协议 · YYDB", "线协议 · YYDS"],
    ["安装 · YYDB", "安装 · YYDS"],
    ["brandShort: 'YYDB'", "brandShort: 'YYDS'"],
    ["brandFull: 'YY Database'", "brandFull: 'YY Distributed Store'"],
    ["webuiTitle: 'YYDB WebUI'", "webuiTitle: 'YYDS WebUI'"],
    ["本地数据库管理", "分布式节点管理"],
    ["参考实现 yydb serve", "参考实现 yyds start"],
    ["../yydb-webui", "../yyds-webui"],
    ["@yydb/yydb`", "@yydb/yyds`"],
    ["@yydb/yydb ", "@yydb/yyds "],
];

const textExt = new Set([
    ".vmz",
    ".ts",
    ".json",
    ".json5",
    ".md",
    ".mjs",
    ".css",
    ".html",
]);

function walk(dir, files = []) {
    for (const entry of fs.readdirSync(dir, { withFileTypes: true })) {
        if (entry.name === "node_modules" || entry.name === "dist") continue;
        const full = path.join(dir, entry.name);
        if (entry.isDirectory()) walk(full, files);
        else if (textExt.has(path.extname(entry.name))) files.push(full);
    }
    return files;
}

for (const root of roots) {
    if (!fs.existsSync(root)) continue;
    for (const file of walk(root)) {
        let text = fs.readFileSync(file, "utf8");
        let next = text;
        for (const [from, to] of replacements) {
            next = next.split(from).join(to);
        }
        if (next !== text) fs.writeFileSync(file, next, "utf8");
    }
}

console.log("rebrand complete");
