#!/usr/bin/env node
/**
 * Execution-count gate for the Redis and MongoDB real-client suites.
 *
 * A suite can lose cases without turning red: a deleted file, a filtered glob, an
 * early `t.skip`, or a driver import failure that silently drops work all leave a
 * green run with fewer assertions. Each suite therefore has a minimum executed
 * case count, and any skipped case is a failure.
 *
 * Usage: `node scripts/assert-test-count.mjs <gate>...` where the gate names are
 * `redis-ts`, `mongo-ts`, `redis-py`, `mongo-py`. With no argument every gate runs.
 */

import { spawnSync } from "node:child_process";

const ANSI = /\u001B\[[0-9;]*m/g;

function parseNodeTest(output) {
    const text = output.replace(ANSI, "");
    const passed = text.match(/^\s*\u2139?\s*pass (\d+)\s*$/m);
    const skipped = text.match(/^\s*\u2139?\s*skipped (\d+)\s*$/m);
    if (!passed) {
        return { error: "could not read the node:test pass summary" };
    }
    return {
        passed: Number(passed[1]),
        skipped: skipped ? Number(skipped[1]) : 0,
    };
}

function parsePythonSuite(output) {
    const text = output.replace(ANSI, "");
    const passed = text.split("\n").filter((line) => line.startsWith("PASS ")).length;
    const skipped = text.split("\n").filter((line) => line.startsWith("SKIP ")).length;
    if (passed === 0) {
        return { error: "the python suite reported no passing cases" };
    }
    return { passed, skipped };
}

const PYTHON_CLIENTS = "projects/yyds.py/packages/yyds/tests/clients/run_all.py";

const GATES = {
    "redis-ts": {
        minimum: 38,
        command: `pnpm --filter @yydb/redis test`,
        parse: parseNodeTest,
    },
    "mongo-ts": {
        minimum: 4,
        command: `pnpm --filter @yydb/mongodb test`,
        parse: parseNodeTest,
    },
    "redis-py": {
        minimum: 14,
        command: `python ${PYTHON_CLIENTS} --suite redis`,
        parse: parsePythonSuite,
    },
    "mongo-py": {
        minimum: 1,
        command: `python ${PYTHON_CLIENTS} --suite mongo`,
        parse: parsePythonSuite,
    },
};

function runGate(name, gate) {
    const result = spawnSync(gate.command, { encoding: "utf8", shell: true, maxBuffer: 64 * 1024 * 1024 });
    const output = `${result.stdout ?? ""}${result.stderr ?? ""}`;
    if (result.status !== 0) {
        process.stdout.write(output);
        return { name, ok: false, reason: `suite exited with ${result.status}` };
    }
    const parsed = gate.parse(output);
    if (parsed.error) {
        process.stdout.write(output);
        return { name, ok: false, reason: parsed.error };
    }
    if (parsed.skipped > 0) {
        process.stdout.write(output);
        return { name, ok: false, reason: `${parsed.skipped} skipped case(s)` };
    }
    if (parsed.passed < gate.minimum) {
        return {
            name,
            ok: false,
            reason: `${parsed.passed} executed case(s), below the ${gate.minimum} floor`,
        };
    }
    return { name, ok: true, reason: `${parsed.passed} executed case(s), floor ${gate.minimum}, 0 skipped` };
}

const requested = process.argv.slice(2);
const names = requested.length > 0 ? requested : Object.keys(GATES);
const unknown = names.filter((name) => GATES[name] === undefined);
if (unknown.length > 0) {
    console.error(`unknown gate(s): ${unknown.join(", ")} (known: ${Object.keys(GATES).join(", ")})`);
    process.exit(2);
}

const outcomes = names.map((name) => runGate(name, GATES[name]));
for (const outcome of outcomes) {
    console.log(`${outcome.ok ? "PASS" : "FAIL"} ${outcome.name}: ${outcome.reason}`);
}
process.exit(outcomes.every((outcome) => outcome.ok) ? 0 : 1);
