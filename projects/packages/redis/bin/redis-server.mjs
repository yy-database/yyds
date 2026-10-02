#!/usr/bin/env node
import { loadDisguiseRuntime } from "../src/disguise.ts";

const runtime = loadDisguiseRuntime("redis");

console.log(`[disguise] redis-server ${runtime.yydsVersion} ready on port 6379 (YYDS)`);
console.log(`[disguise] backend ping: ${runtime.ping}`);
