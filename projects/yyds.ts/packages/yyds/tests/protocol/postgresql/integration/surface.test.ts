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
        // Engine wording is asserted only where the gateway reproduces it
        // verbatim; elsewhere the SQLSTATE is the contract.
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

    // Gateway identity, not an engine value: the emulated server advertises
    // itself as `YYDS <version>` instead of claiming a PostgreSQL release.
    const version = await client.query("SHOW server_version");
    assert.equal(version.rows.length, 1);
    assert.match(String(version.rows[0]?.server_version), /^YYDS \d+\.\d+/);

    // search_path follows the most recent SET and defaults to the PostgreSQL
    // convention, which prints `$user` quoted because it is an identifier.
    const initialPath = await client.query("SHOW search_path");
    assert.equal(String(initialPath.rows[0]?.search_path), '"$user", public');
    await client.query("SET search_path = 'yyds_schema'");
    const movedPath = await client.query("SHOW search_path");
    assert.equal(String(movedPath.rows[0]?.search_path), "yyds_schema");

    await assert.rejects(
        client.query("SET search_path = 123"),
        expectSqlState("22023", /invalid value for parameter "search_path": "123"/),
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
        expectSqlState("42601"),
    );
    await assert.rejects(
        client.query("PRAGMA foreign_keys = OFF"),
        expectSqlState("42601"),
    );
    await assert.rejects(
        client.query("SAVEPOINT sp_one"),
        expectSqlState("25P01"),
    );
    await assert.rejects(
        client.query("SELEKT 1"),
        expectSqlState("42601"),
    );

    // An empty simple query answers EmptyQueryResponse instead of a parse error.
    const empty = await client.query("");
    assert.deepEqual(empty.rows, []);
    assert.deepEqual(empty.fields, []);
});

test("pg DDL subset and catalogue projections stay driver-visible", { timeout: 60_000 }, async (t) => {
    const node = await startYydsNode();
    const client = new pg.Client({ host: node.pgsql.host, port: node.pgsql.port, user: "yyds", database: "yyds" });
    t.after(async () => {
        await client.end().catch(() => undefined);
        await node.stop();
    });
    await client.connect();

    await client.query("CREATE TABLE ddl_probe (id INT, name VARCHAR(191) NOT NULL, PRIMARY KEY (id))");
    await client.query("ALTER TABLE ddl_probe ADD COLUMN score DOUBLE PRECISION");
    await client.query("CREATE UNIQUE INDEX ddl_probe_name_key ON ddl_probe (name)");
    await client.query("INSERT INTO ddl_probe (id, name, score) VALUES (1, 'alpha', 0.5)");
    await assert.rejects(
        client.query("INSERT INTO ddl_probe (id, name, score) VALUES (2, 'alpha', 0.75)"),
        expectSqlState("23505", /duplicate key/),
    );

    await client.query("DROP INDEX ddl_probe_name_key");
    const rows = await client.query("SELECT id, name FROM ddl_probe ORDER BY id");
    assert.deepEqual(rows.rows, [{ id: 1, name: "alpha" }]);

    const tables = await client.query(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE' ORDER BY table_name",
    );
    assert.deepEqual(tables.rows.map((row: { table_name: string }) => row.table_name), ["ddl_probe"]);

    const columns = await client.query(
        "SELECT column_name FROM information_schema.columns WHERE table_schema = 'public' AND table_name = 'ddl_probe' ORDER BY ordinal_position",
    );
    assert.deepEqual(columns.rows.map((row: { column_name: string }) => row.column_name), ["id", "name", "score"]);

    await assert.rejects(client.query("CREATE TABLE batch_one (id INT); CREATE TABLE batch_one (id INT); SELECT 1"), (error: unknown) => {
        assert.match((error as PgFailure).message, /statement 2 of 3 failed at batch offset/);
        return true;
    });

    await client.query("DROP TABLE ddl_probe");
    await assert.rejects(client.query("SELECT id FROM ddl_probe"), expectSqlState("42P01", /unknown SQL table/));
});
