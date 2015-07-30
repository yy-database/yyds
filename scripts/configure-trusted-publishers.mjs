/**
 * Configure npm Trusted Publisher via registry API for @yyds packages.
 *
 *   npm login --auth-type=web
 *   node scripts/configure-trusted-publishers.mjs
 */
import fs from "node:fs";
import os from "os";
import path from "path";
import https from "https";
import { spawnSync } from "node:child_process";

const repo = "yy-database/yyds";
const workflowFile = "release-npm.yml";
const environment = "NPM_PUBLISH";
const packages = [
    "@yyds/yyds-win32-x64",
    "@yyds/yyds-linux-x64",
    "@yyds/yyds-darwin-x64",
    "@yyds/yyds-darwin-arm64",
    "@yyds/yyds-unknown-wasm32",
    "@yyds/yyds",
    "@yyds/redis",
    "@yyds/mysql",
    "@yyds/postgresql",
    "@yyds/sqlite",
];

function readToken() {
    const npmrc = fs.readFileSync(path.join(os.homedir(), ".npmrc"), "utf8");
    const m = npmrc.match(/\/\/registry\.npmjs\.org\/:_authToken=(.+)/);
    if (!m) throw new Error("no registry.npmjs.org auth token; run npm login --auth-type=web");
    return m[1].trim();
}

function request(method, urlPath, { token, body } = {}) {
    return new Promise((resolve, reject) => {
        const data = body ? JSON.stringify(body) : null;
        const headers = {
            accept: "application/json",
            authorization: `Bearer ${token}`,
        };
        if (data) {
            headers["content-type"] = "application/json";
            headers["content-length"] = Buffer.byteLength(data);
        }
        const req = https.request(
            {
                method,
                hostname: "registry.npmjs.org",
                path: urlPath,
                headers,
            },
            (res) => {
                let buf = "";
                res.on("data", (c) => (buf += c));
                res.on("end", () => {
                    let json = null;
                    try {
                        json = buf ? JSON.parse(buf) : null;
                    } catch {
                        json = null;
                    }
                    resolve({ status: res.statusCode, body: buf, json });
                });
            },
        );
        req.on("error", reject);
        if (data) req.write(data);
        req.end();
    });
}

function trustBody() {
    return [
        {
            type: "github",
            claims: {
                repository: repo,
                workflow_ref: { file: workflowFile },
                environment,
            },
            permissions: ["createPackage"],
        },
    ];
}

const token = readToken();
console.log("token ok, configuring", packages.length, "packages");

for (const pkg of packages) {
    const enc = encodeURIComponent(pkg);
    const urlPath = `/-/package/${enc}/trust`;
    console.log(`\n=== ${pkg} ===`);

    const res = await request("POST", urlPath, { token, body: trustBody() });
    if (res.status === 200 || res.status === 201 || res.status === 409) {
        console.log(res.status === 409 ? "already exists (409)" : "configured");
        continue;
    }
    console.error("unexpected", res.status, res.body.slice(0, 500));
    process.exit(1);
}

console.log("\n=== verify ===");
for (const pkg of packages) {
    const enc = encodeURIComponent(pkg);
    const res = await request("GET", `/-/package/${enc}/trust`, { token });
    const ok =
        res.status === 200 && Array.isArray(res.json) && res.json.some((x) => x?.type === "github");
    console.log(ok ? "ok" : "MISSING", pkg, res.status);
}
