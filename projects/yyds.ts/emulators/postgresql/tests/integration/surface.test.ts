import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode } from "@yydb/yyds-node-harness";
import pg from "pg";

interface PgFailure extends Error {
    code?: string;
}

function expectSqlState(state: string, message?: RegExp) {
    return (error: unknown) => {
        const failure = error as PgFailure;
        assert.equal(failure.code, state, `expected SQLSTATE ${state}, got ${failure.message}`);
        if (message) assert.match(failure.message, message);
        return true;
    };
}

test("pg session surface covers parameters, dialect rejections and empty queries", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = new pg.Client({ host: node.pgsql.host, port: node.pgsql.port, user: "yyds", database: "yyds" });
    t.after(async () => {
        await client.end().catch(() => undefined);
        await node.stop();
    });
    await client.connect();

    // SHOW server_version agrees with the startup ParameterStatus "YYDS <version>" shape.
    const version = await client.query("SHOW server_version");
    assert.equal(version.rows.length, 1);
    assert.match(String(version.rows[0]?.server_version), /^YYDS \d+\.\d+/);

    // search_path follows the most recent SET and defaults to the PostgreSQL convention.
    const initialPath = await client.query("SHOW search_path");
    assert.equal(String(initialPath.rows[0]?.search_path), "$user, public");
    await client.query("SET search_path = 'yyds_schema'");
    const movedPath = await client.query("SHOW search_path");
    assert.equal(String(movedPath.rows[0]?.search_path), "yyds_schema");

    await assert.rejects(
        client.query("SET search_path = 123"),
        expectSqlState("22P02", /invalid value for parameter "search_path": expected string literal/),
    );

    await assert.rejects(
        client.query("SET yyds_mystery = 'x'"),
        expectSqlState("42704", /unrecognized configuration parameter "yyds_mystery"/),
    );
    await assert.rejects(
        client.query("SHOW yyds_mystery"),
        expectSqlState("42704", /unrecognized configuration parameter "yyds_mystery"/),
    );

    // Errors outside a transaction must not poison the next statement.
    const stillAlive = await client.query("SHOW server_version");
    assert.equal(stillAlive.rows.length, 1);

    await assert.rejects(
        client.query("SHOW VARIABLES"),
        expectSqlState("42601", /SHOW VARIABLES is MySQL syntax/),
    );
    await assert.rejects(
        client.query("PRAGMA foreign_keys = OFF"),
        expectSqlState("42601", /PRAGMA is not PostgreSQL syntax/),
    );
    await assert.rejects(
        client.query("SAVEPOINT sp_one"),
        expectSqlState("25001", /SAVEPOINT is only valid inside a transaction/),
    );
    await assert.rejects(
        client.query("SELEKT 1"),
        expectSqlState("42601", /Oak could not parse/),
    );

    await assert.rejects(client.query(""), expectSqlState("42601", /Oak could not parse/));
});
