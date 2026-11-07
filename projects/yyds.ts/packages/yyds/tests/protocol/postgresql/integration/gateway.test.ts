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

function expectError(state: string, message: RegExp) {
    return (error: unknown) => {
        const failure = error as PgFailure;
        assert.equal(failure.code, state, `expected SQLSTATE ${state}, got ${failure.message}`);
        assert.match(failure.message, message);
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

    // The shipped CLI mounts a SQL runtime by default, so the simple query
    // protocol executes and returns a real result set. A bare integer literal
    // describes as `Int64` (OID 20), which `pg` renders as a string.
    const simple = await client.query("SELECT 1 AS value");
    assert.deepEqual(simple.rows, [{ value: "1" }]);

    // Extended query protocol (Parse/Bind/Execute) binds a parameter into the
    // statement text before planning.
    const extended = await client.query("SELECT $1 AS value", [7]);
    assert.deepEqual(extended.rows, [{ value: "7" }]);

    // `::` has no Oak AST node, so the gateway refuses it instead of silently
    // dropping the cast and the column alias.
    await assert.rejects(client.query("SELECT 1::int AS value"), expectSqlState("0A000"));

    await client.query("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR(191))");
    await client.query("INSERT INTO users (id, name) VALUES (1, 'ada')");
    const updated = await client.query("UPDATE users SET name = 'grace' WHERE id = 1");
    assert.equal(updated.rowCount, 1);
    const found = await client.query("SELECT name FROM users WHERE id = 1");
    assert.deepEqual(found.rows, [{ name: "grace" }]);

    // ROLLBACK discards the statement written inside the transaction.
    await client.query("BEGIN");
    await client.query("INSERT INTO users (id, name) VALUES (2, 'lin')");
    await client.query("ROLLBACK");
    const afterRollback = await client.query("SELECT name FROM users ORDER BY id");
    assert.deepEqual(afterRollback.rows, [{ name: "grace" }]);

    // COMMIT makes the statement visible to later sessions of the same node.
    await client.query("BEGIN");
    await client.query("INSERT INTO users (id, name) VALUES (3, 'mika')");
    await client.query("COMMIT");
    const afterCommit = await client.query("SELECT name FROM users ORDER BY id");
    assert.deepEqual(afterCommit.rows, [{ name: "grace" }, { name: "mika" }]);

    // A duplicate key aborts the open transaction until ROLLBACK clears it.
    await client.query("BEGIN");
    await assert.rejects(
        client.query("INSERT INTO users (id, name) VALUES (1, 'duplicate')"),
        expectSqlState("23505"),
    );
    await assert.rejects(client.query("SELECT 1"), expectSqlState("25P02"));
    await client.query("ROLLBACK");

    // After ROLLBACK the aborted state is cleared and statements execute again.
    const recovered = await client.query("SELECT 2 AS value");
    assert.deepEqual(recovered.rows, [{ value: "2" }]);

    // Structured catalog errors keep their own SQLSTATE.
    await assert.rejects(client.query("SELECT id FROM missing_table"), expectError("42P01", /unknown SQL table/));
    await assert.rejects(client.query("SELECT missing_column FROM users"), expectError("42703", /unknown SQL column/));

    // Execution-time semantic errors are classified instead of becoming XX000.
    await assert.rejects(
        client.query("SELECT id, name, COUNT(*) FROM users GROUP BY id"),
        expectError("42803", /GROUP BY/i),
    );
    await assert.rejects(
        client.query("SELECT id FROM users WHERE id IN (SELECT id, name FROM users)"),
        expectError("42601", /exactly one column/i),
    );
    await assert.rejects(
        client.query("SELECT id FROM users WHERE id = (SELECT id FROM users)"),
        expectError("21000", /more than one row/i),
    );

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
    const anonymous = await unauthenticated.query("SELECT 1 AS value");
    assert.deepEqual(anonymous.rows, [{ value: "1" }]);
    await unauthenticated.end();
});
