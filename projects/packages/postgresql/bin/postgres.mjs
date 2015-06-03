#!/usr/bin/env node
import { loadDisguiseRuntime } from "../src/disguise.ts";

const runtime = loadDisguiseRuntime("postgresql");

console.log(`[disguise] postgres ${runtime.yydsVersion} ready on port 5432 (YYDS)`);
console.log(`[disguise] backend ping: ${runtime.ping}`);
