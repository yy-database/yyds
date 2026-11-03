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

    // UTF-8 collation names are session metadata; common driver defaults are accepted.
    await connection.query("SET NAMES utf8mb4 COLLATE utf8mb4_general_ci");
    await connection.query("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci");
    await assert.rejects(
        connection.query("SET NAMES utf8mb4 COLLATE utf8mb4_unknown_ci"),
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

    // Unsupported metadata views are refused with the shared metadata error code.
    await assert.rejects(connection.query("SHOW DATABASES"), (error: ServerError) => {
        assert.equal(error.errno, 1235, error.message);
        assert.equal(error.sqlState, "42000");
        assert.match(error.sqlMessage ?? "", /not a supported metadata view/);
        return true;
    });
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

    // Prepared statements run against the mounted SQL runtime.
    const [prepared] = await connection.execute("SELECT 1 AS value");
    assert.deepEqual(prepared, [{ value: 1 }]);
});
