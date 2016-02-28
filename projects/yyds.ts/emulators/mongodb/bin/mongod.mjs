#!/usr/bin/env node
import { launchYydsServer } from "@yydb/yyds/node/cluster";

const args = process.argv.slice(2);
if (args.length === 1 && args[0] === "--help") {
    console.log("Usage: mongod --data-dir DIR --cluster-id ID --node-id ID [--port PORT]");
    console.log("Starts the YYDS node and its MongoDB handshake probe through the yyds cluster tool.");
} else {
    launchYydsServer("mongodb", args)
        .then((code) => {
            process.exitCode = code;
        })
        .catch((error) => {
            console.error("@yydb/mongodb: " + error.message);
            process.exitCode = 127;
        });
}
