import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test, { after, before } from "node:test";
import { setTimeout as sleep } from "node:timers/promises";
import { fileURLToPath } from "node:url";

import { startYydsNode, type YydsNode } from "@yydb/yyds-node-harness";
import { createClient } from "redis";

/**
 * Differential driver: the scenario table in `../differential/expiry-scenarios.json` is the
 * single source of truth, executed here through node-redis and by the Python suite through
 * redis-py. Both interpreters must observe identical results against the same gateway.
 */

type Expectation =
    | string
    | number
    | null
    | Array<string | null>
    | { intBetween: [number, number] }
    | { error: string };

type Step =
    | { cmd: Array<string | { unixSecondsFromNow: number } | { unixMsFromNow: number }>; expect: Expectation }
    | { sleep: number };

interface Scenario {
    name: string;
    steps: Step[];
}

const table = JSON.parse(
    readFileSync(fileURLToPath(new URL("../differential/expiry-scenarios.json", import.meta.url)), "utf8"),
) as { scenarios: Scenario[] };

function resolveArgument(argument: unknown): string {
    if (typeof argument === "string") return argument;
    const marker = argument as { unixSecondsFromNow?: number; unixMsFromNow?: number };
    if (marker.unixSecondsFromNow !== undefined)
        return String(Math.floor(Date.now() / 1000) + marker.unixSecondsFromNow);
    if (marker.unixMsFromNow !== undefined) return String(Date.now() + marker.unixMsFromNow);
    throw new Error(`unresolvable scenario argument ${JSON.stringify(argument)}`);
}

function checkExpectation(expect: Expectation, actual: unknown, label: string): void {
    if (expect === null) {
        assert.equal(actual, null, label);
        return;
    }
    if (typeof expect === "number") {
        assert.equal(actual, expect, label);
        return;
    }
    if (typeof expect === "string") {
        assert.equal(actual, expect, label);
        return;
    }
    if (Array.isArray(expect)) {
        assert.deepEqual(actual, expect, label);
        return;
    }
    if ("intBetween" in expect) {
        const value = Number(actual);
        assert.ok(
            Number.isInteger(value) && value >= expect.intBetween[0] && value <= expect.intBetween[1],
            `${label}: expected an integer within ${expect.intBetween}, got ${JSON.stringify(actual)}`,
        );
        return;
    }
    throw new Error(`unhandled expectation ${JSON.stringify(expect)} for ${label}`);
}

let node: YydsNode;
let client: ReturnType<typeof createClient>;

before(async () => {
    node = await startYydsNode();
    client = createClient({
        url: `redis://${node.redis.host}:${node.redis.port}`,
        RESP: 2,
        disableClientInfo: true,
    });
    client.on("error", () => undefined);
    await client.connect();
});

after(async () => {
    await client.close();
    await node.stop();
});

for (const scenario of table.scenarios) {
    test(`differential scenario ${scenario.name} matches the shared table`, async () => {
        for (const [index, step] of scenario.steps.entries()) {
            const label = `${scenario.name}[${index}]`;
            if ("sleep" in step) {
                await sleep(step.sleep);
                continue;
            }
            const args = step.cmd.map((argument) => resolveArgument(argument));
            if (typeof step.expect === "object" && step.expect !== null && "error" in step.expect) {
                const failure = await client.sendCommand(args).then(
                    () => null,
                    (error: Error) => error,
                );
                assert.ok(failure !== null, `${label}: expected ${args[0]} to fail`);
                assert.equal(failure.message, step.expect.error, label);
                continue;
            }
            const actual = await client.sendCommand(args);
            checkExpectation(step.expect, actual, label);
        }
    });
}
