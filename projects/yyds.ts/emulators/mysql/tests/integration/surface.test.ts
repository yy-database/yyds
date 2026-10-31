import assert from "node:assert/strict";
import test from "node:test";

import { startYydsNode } from "@yydb/yyds-node-harness";
import mysql from "mysql2/promise";

interface ServerError {
    errno?: number;
    sqlState?: string;
    sqlMessage?: string;
    message: string;
}

function expectServerFailure(code: number, sqlState: string, sqlMessage: string) {
    return (error: unknown) => {
        const failure = error as ServerError;
        assert.equal(failure.errno, code, `expected MySQL error ${code}, got ${failure.message}`);
        assert.equal(failure.sqlState, sqlState);
        assert.equal(failure.sqlMessage, sqlMessage);
        return true;
    };
}

test("MySQL session surface covers collations, variables and rejected statements", { timeout: 60_000 }, async (t) => {
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

    await connection.ping();

    // Only utf8mb4_general_ci is accepted as an explicit collation.
    await connection.query("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci");
    await assert.rejects(
        connection.query("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci"),
        expectServerFailure(1273, "HY000", "unsupported utf8mb4 collation"),
    );

    // autocommit is tracked per session and reflected by SHOW VARIABLES.
    const [initial] = await connection.query("SHOW VARIABLES LIKE 'autocommit'");
    assert.deepEqual((initial as Array<Record<string, string>>).map((row) => row.Value), ["ON"]);
    await connection.query("SET autocommit = 0");
    const [off] = await connection.query("SHOW VARIABLES LIKE 'autocommit'");
    assert.deepEqual((off as Array<Record<string, string>>).map((row) => row.Value), ["OFF"]);
    await connection.query("SET autocommit = 1");
    const [on] = await connection.query("SHOW VARIABLES LIKE 'autocommit'");
    assert.deepEqual((on as Array<Record<string, string>>).map((row) => row.Value), ["ON"]);

    await assert.rejects(
        connection.query("SET yyds_mystery_variable = 1"),
        expectServerFailure(1193, "HY000", "Unknown system variable 'yyds_mystery_variable'"),
    );

    // SHOW with a `%` wildcard narrows the emulated variable table.
    const [versioned] = await connection.query("SHOW VARIABLES LIKE 'version%'");
    assert.deepEqual(
        (versioned as Array<Record<string, string>>).map((row) => row.Variable_name).sort(),
        ["version", "version_comment"],
    );

    // MySQL-only statements the gateway refuses with distinct 1064 messages.
    await assert.rejects(
        connection.query("SHOW DATABASES"),
        expectServerFailure(1064, "42000", "MySQL requires SHOW VARIABLES instead of SHOW DATABASES"),
    );
    await assert.rejects(
        connection.query("PRAGMA foreign_keys = OFF"),
        expectServerFailure(1064, "42000", "PRAGMA is not MySQL syntax"),
    );

    // SAVEPOINT outside a transaction is rejected at the session layer.
    await assert.rejects(
        connection.query("SAVEPOINT sp_one"),
        expectServerFailure(1305, "42000", "SAVEPOINT is only valid inside a transaction"),
    );

    // COMMIT and ROLLBACK are protocol-shell OK packets without an open transaction.
    const [committed] = await connection.query("COMMIT");
    assert.equal((committed as { affectedRows?: number }).affectedRows, 0);
    const [rolledBack] = await connection.query("ROLLBACK");
    assert.equal((rolledBack as { affectedRows?: number }).affectedRows, 0);

    // Prepared statements still require a SQL runtime the harness node does not have.
    await assert.rejects(
        connection.execute("SELECT 1 AS value"),
        expectServerFailure(1235, "42000", "YYDS MySQL SQL execution is not implemented (no SQL runtime)"),
    );
});
