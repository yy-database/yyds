#!/usr/bin/env node
import { loadDisguiseRuntime } from "../src/disguise.ts";

const runtime = loadDisguiseRuntime("mysql");

console.log(`[disguise] mysqld ${runtime.yydsVersion} ready on port 3306 (YYDS)`);
console.log(`[disguise] backend ping: ${runtime.ping}`);
