#!/usr/bin/env node
import { launchYydsServer } from "@yyds/yyds/node/cluster";

const args = process.argv.slice(2);
if (args.length === 1 && args[0] === "--help") {
    console.log("Usage: mysqld --data-dir DIR --cluster-id ID --node-id ID [--port PORT]");
    console.log("Starts the YYDS node and its protocol listener through the yyds cluster tool.");
} else {
    launchYydsServer("mysql", args)
        .then((code) => {
            process.exitCode = code;
        })
        .catch((error) => {
            console.error("@yyds/mysql: " + error.message);
            process.exitCode = 127;
        });
}
