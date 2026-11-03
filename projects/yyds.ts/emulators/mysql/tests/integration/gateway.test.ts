import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode } from "@yydb/yyds-node-harness";
import mysql from "mysql2/promise";

interface ServerError {
    errno?: number;
    sqlState?: string;
    message: string;
}

function expectErrno(code: number) {
    return (error: unknown) => {
        const failure = error as ServerError;
        assert.equal(failure.errno, code, `expected MySQL error ${code}, got ${failure.message}`);
        return true;
    };
}

test("mysql2 drives connection, session commands and gateway boundaries", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const connection = await mysql.createConnection({
        host: node.mysql.host,
        port: node.mysql.port,
        user: "yyds",
        database: "yyds",
    });
    t.after(async () => {
        await connection.end().catch(() => undefined);
        await node.stop();
    });

    // A real result set with column metadata is available without a SQL runtime.
    const [variables, fields] = await connection.query(
        "SHOW VARIABLES LIKE 'transaction_isolation'",
    );
    const rows = variables as Array<{ Variable_name?: string; Value?: string }>;
    assert.equal(rows.length, 1);
    assert.equal(rows[0]?.Variable_name, "transaction_isolation");
    assert.equal(rows[0]?.Value, "READ-COMMITTED");
    assert.deepEqual(
        (fields as Array<{ name: string }>).map((field) => field.name),
        ["Variable_name", "Value"],
    );

    const [names] = await connection.query("SET NAMES utf8mb4");
    assert.equal((names as { affectedRows: number }).affectedRows, 0);

    await assert.rejects(
        connection.query("SET NAMES latin1"),
        expectErrno(1115),
    );

    // The shipped CLI mounts a SQL runtime by default, so engine statements execute.
    const [selected] = await connection.query("SELECT 1 AS value");
    assert.deepEqual(selected, [{ value: 1 }]);

    await connection.query("CREATE TABLE users (id BIGINT PRIMARY KEY, name VARCHAR(191))");
    await connection.query("INSERT INTO users (id, name) VALUES (1, 'ada')");
    const [updated] = await connection.query("UPDATE users SET name = 'grace' WHERE id = 1");
    assert.equal((updated as { affectedRows: number }).affectedRows, 1);
    const [found] = await connection.query("SELECT name FROM users WHERE id = 1");
    assert.deepEqual(found, [{ name: "grace" }]);
    const [deleted] = await connection.query("DELETE FROM users WHERE id = 1");
    assert.equal((deleted as { affectedRows: number }).affectedRows, 1);

    await connection.beginTransaction();
    await connection.query("INSERT INTO users (id, name) VALUES (2, 'lin')");
    await connection.rollback();
    const [afterRollback] = await connection.query("SELECT id FROM users");
    assert.deepEqual(afterRollback, []);

    // Failures must not desynchronize the session.
    const [recovered] = await connection.query("SHOW VARIABLES LIKE 'version'");
    const versionRows = recovered as Array<{ Variable_name?: string; Value?: string }>;
    assert.equal(versionRows.length, 1);
    assert.match(versionRows[0]?.Value ?? "", /yyds/);

    await assert.rejects(connection.query("SELECT ("), expectErrno(1064));
});
