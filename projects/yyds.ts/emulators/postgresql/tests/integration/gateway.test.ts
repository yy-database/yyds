import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode } from "@yydb/yyds-node-harness";
import pg from "pg";

interface PgFailure {
    code?: string;
    message: string;
}

function expectSqlState(state: string) {
    return (error: unknown) => {
        const failure = error as PgFailure;
        assert.equal(failure.code, state, `expected SQLSTATE ${state}, got ${failure.message}`);
        return true;
    };
}

test("pg drives startup, simple and extended queries, and protocol transactions", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = new pg.Client({
        host: node.pgsql.host,
        port: node.pgsql.port,
        user: "yyds",
        database: "yyds",
    });
    await client.connect();
    t.after(async () => {
        await client.end().catch(() => undefined);
        await node.stop();
    });

    // Simple query protocol reaches the gateway and fails before planning,
    // because the CLI starts a protocol listener without a SQL runtime.
    await assert.rejects(client.query("SELECT 1 AS value"), expectSqlState("0A000"));

    // Extended query protocol (Parse/Bind/Execute) reports the same boundary and
    // therefore never produces a RowDescription for the driver to map.
    await assert.rejects(
        client.query("SELECT $1::int AS value", [1]),
        expectSqlState("0A000"),
    );

    // Protocol-only transaction handling survives without a runtime.
    await client.query("BEGIN");
    await assert.rejects(client.query("SELECT 1"), expectSqlState("0A000"));
    await assert.rejects(client.query("SELECT 2"), expectSqlState("25P02"));
    await client.query("ROLLBACK");

    // After ROLLBACK the aborted state is cleared and the next failure is 0A000.
    await assert.rejects(client.query("SELECT 3"), expectSqlState("0A000"));

    await client.query("BEGIN");
    await client.query("COMMIT");

    // TLS is not implemented to any degree, so the server rejects the SSLRequest
    // instead of negotiating a downgraded connection.
    const tls = new pg.Client({
        host: node.pgsql.host,
        port: node.pgsql.port,
        user: "yyds",
        database: "yyds",
        ssl: { rejectUnauthorized: false },
    });
    await assert.rejects(
        tls.connect(),
        (error: unknown) => {
            assert.match((error as Error).message, /does not support SSL/i);
            return true;
        },
    );

    // Authentication is not implemented. A password is accepted without being
    // verified, which is recorded here as a boundary rather than as support.
    const unauthenticated = new pg.Client({
        host: node.pgsql.host,
        port: node.pgsql.port,
        user: "yyds",
        password: "ignored-by-the-gateway",
        database: "yyds",
    });
    await unauthenticated.connect();
    await assert.rejects(unauthenticated.query("SELECT 1"), expectSqlState("0A000"));
    await unauthenticated.end();
});
