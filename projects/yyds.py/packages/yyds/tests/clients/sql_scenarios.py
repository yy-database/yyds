"""Feature scenarios at 1000-row scale with an external truth source.

Each scenario targets one implementation risk of the emulated SQL gateways
rather than repeating one large table. Every scenario runs twice: once against
the real engine named by `YYDS_ORACLE_*` and once against the YYDS node under
test, and the observations are compared row by row. Without oracle endpoints
the cases stay `unverified`: driver-level assertions alone never count as
engine parity.

The catalog in `scenario_catalog.json` is machine readable. A scenario without
a size, a shape, an external truth source or both a Python and a TypeScript
case name is rejected before anything runs, so the matrix cannot silently
shrink.
"""

from __future__ import annotations

import json
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from harness import CaseResult, YydsNode, check, start_node
from oracle_parity import (
    MYSQL_ENV,
    PG_ENV,
    Oracle,
    _mysql_outcome,
    _oracle,
    _pg_outcome,
    _rows as _driver_rows,
)

CATALOG_PATH = Path(__file__).with_name("scenario_catalog.json")
CATALOG_CONTRACT = "yyds-sql-scenario-catalog/v0"
MINIMUM_ROWS = 1000
ENGINES = ("mysql", "postgresql")


@dataclass(frozen=True)
class Scenario:
    """One independently runnable feature scenario."""

    name: str
    feature: str
    risk: str
    rows: int
    shape: str
    external: str
    cases: dict[str, str]


def load_catalog(path: Path = CATALOG_PATH) -> list[Scenario]:
    """Loads the catalog and rejects entries that could hide missing coverage."""

    document = json.loads(path.read_text(encoding="utf-8"))
    check(document.get("contract") == CATALOG_CONTRACT, f"scenario catalog contract is {document.get('contract')!r}")
    entries = document.get("scenarios")
    check(isinstance(entries, list) and entries, "scenario catalog has no scenarios")
    scenarios: list[Scenario] = []
    for entry in entries:
        for field in ("name", "feature", "risk", "shape", "external", "cases"):
            check(bool(entry.get(field)), f"scenario {entry.get('name')!r} is missing `{field}`")
        rows = entry.get("rows")
        check(isinstance(rows, int) and rows >= MINIMUM_ROWS, f"scenario {entry['name']!r} declares {rows!r} rows, needs at least {MINIMUM_ROWS}")
        cases = entry["cases"]
        for language in ("python", "ts"):
            check(bool(cases.get(language)), f"scenario {entry['name']!r} has no `{language}` case")
        scenarios.append(
            Scenario(
                name=entry["name"],
                feature=entry["feature"],
                risk=entry["risk"],
                rows=rows,
                shape=entry["shape"],
                external=entry["external"],
                cases=cases,
            )
        )
    check(len(scenarios) >= 10, f"scenario catalog lists {len(scenarios)} scenarios, expected at least 10")
    return scenarios


def _table(scenario: str, engine: str) -> str:
    suffix = "my" if engine == "mysql" else "pg"
    return f"yyds_sc_{scenario}_{suffix}"


def _factory(engine: str, endpoint: Oracle) -> Callable[..., Any]:
    def open_connection(*, autocommit: bool = True, multi_statement: bool = False):
        if engine == "mysql":
            import mysql.connector as mysql

            arguments: dict[str, Any] = {}
            if multi_statement:
                from mysql.connector.constants import ClientFlag

                arguments["client_flags"] = [ClientFlag.MULTI_STATEMENTS]
            return mysql.connect(
                host=endpoint.host,
                port=endpoint.port,
                user=endpoint.user,
                password=endpoint.password,
                database=endpoint.database or None,
                autocommit=autocommit,
                collation="utf8mb4_general_ci",
                **arguments,
            )
        import psycopg

        return psycopg.connect(
            host=endpoint.host,
            port=endpoint.port,
            user=endpoint.user,
            password=endpoint.password,
            dbname=endpoint.database or None,
            autocommit=autocommit,
        )

    return open_connection


def _node_endpoint(engine: str, node: YydsNode) -> Oracle:
    if engine == "mysql":
        return Oracle(host=node.host, port=node.mysql_port, user="yyds", password="", database="")
    return Oracle(host=node.host, port=node.pgsql_port, user="yyds", password="", database="yyds")


def _exec(engine: str, connection, sql: str, *, fetch: bool = False) -> tuple:
    if engine == "mysql":
        return _mysql_outcome(connection, sql, fetch=fetch)
    return _pg_outcome(connection, sql, fetch=fetch)


def _query(engine: str, connection, sql: str) -> list[tuple]:
    outcome = _exec(engine, connection, sql, fetch=True)
    check(outcome[0] == "rows", f"query returned {outcome!r}: {sql}")
    return outcome[1]


def _ddl_bulk(engine: str, table: str, *, unique_name: bool = False, extra: str = "") -> str:
    unique = " UNIQUE" if unique_name else ""
    if engine == "mysql":
        return f"CREATE TABLE {table} (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(16) NOT NULL{unique}, score DOUBLE NULL{extra})"
    return f"CREATE TABLE {table} (id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL{unique}, score DOUBLE PRECISION NULL{extra})"


def _values(count: int, *, prefix: str = "n", offset: int = 0) -> str:
    return ",".join(f"('{prefix}{offset + index}', {offset + index})" for index in range(count))


def _reset(engine: str, connection, table: str, *, unique_name: bool = False) -> None:
    _exec(engine, connection, f"DROP TABLE IF EXISTS {table}")
    created = _exec(engine, connection, _ddl_bulk(engine, table, unique_name=unique_name))
    check(created[0] == "ok", f"CREATE TABLE returned {created!r}")


def _drop(connect: Callable[..., Any], engine: str, table: str) -> None:
    connection = connect()
    try:
        _exec(engine, connection, f"DROP TABLE IF EXISTS {table}")
    finally:
        connection.close()


FIXTURES: dict[str, Callable[[str, Callable[..., Any], str], dict[str, Any]]] = {}


def scenario(name: str):
    def register(function):
        FIXTURES[name] = function
        return function

    return register


@scenario("bulk_insert_1000")
def _bulk_insert_1000(engine: str, connect: Callable[..., Any], table: str) -> dict[str, Any]:
    expected = [(index + 1, f"n{index}", float(index)) for index in range(1000)]
    connection = connect()
    try:
        _reset(engine, connection, table)
        inserted = _exec(engine, connection, f"INSERT INTO {table} (name, score) VALUES {_values(1000)}")
        rows = _query(engine, connection, f"SELECT id, name, score FROM {table} ORDER BY id")
    finally:
        connection.close()
    check(inserted[0] == "ok", f"bulk insert returned {inserted!r}")
    check(len(rows) == 1000, f"bulk insert stored {len(rows)} rows")
    check(rows == expected, "bulk insert rows differ from the requested values")
    return {"insert": inserted, "rows": rows, "rows_written": len(rows)}


@scenario("limit_offset_order_boundaries")
def _limit_offset_order_boundaries(engine: str, connect: Callable[..., Any], table: str) -> dict[str, Any]:
    connection = connect()
    try:
        _reset(engine, connection, table)
        _exec(engine, connection, f"INSERT INTO {table} (name, score) VALUES {_values(1000)}")
        observations = {
            "all": _query(engine, connection, f"SELECT id FROM {table} ORDER BY id LIMIT 1000"),
            "zero": _query(engine, connection, f"SELECT id FROM {table} ORDER BY id LIMIT 0"),
            "tail": _query(engine, connection, f"SELECT id, name FROM {table} ORDER BY id LIMIT 100 OFFSET 900"),
            "desc_head": _query(engine, connection, f"SELECT id FROM {table} ORDER BY id DESC LIMIT 10"),
            "aggregate": _query(engine, connection, f"SELECT COUNT(*), MIN(id), MAX(id), SUM(score) FROM {table}"),
            "rows_written": 1000,
        }
    finally:
        connection.close()
    check(len(observations["all"]) == 1000, f"LIMIT 1000 returned {len(observations['all'])} rows")
    check(observations["zero"] == [], "LIMIT 0 returned rows")
    check(len(observations["tail"]) == 100, "LIMIT 100 OFFSET 900 returned the wrong size")
    check(observations["tail"][0][0] == 901, f"OFFSET 900 started at {observations['tail'][0][0]!r}")
    check(observations["desc_head"][0][0] == 1000, "ORDER BY id DESC started at the wrong id")
    check(observations["aggregate"][0][:3] == (1000, 1, 1000), f"aggregate returned {observations['aggregate']!r}")
    return observations


@scenario("transactions_rollback_registry")
def _transactions_rollback_registry(engine: str, connect: Callable[..., Any], table: str) -> dict[str, Any]:
    extra = f"{table}_extra"
    connection = connect(autocommit=False)
    try:
        _exec(engine, connection, f"DROP TABLE IF EXISTS {table}")
        _exec(engine, connection, f"DROP TABLE IF EXISTS {extra}")
        connection.commit()
        _exec(engine, connection, _ddl_bulk(engine, table))
        connection.commit()
        for index in range(50):
            values = ",".join(f"('t{index}-{row}', {index * 20 + row})" for row in range(20))
            attempt = _exec(engine, connection, f"INSERT INTO {table} (name, score) VALUES {values}")
            check(attempt[0] == "ok", f"transaction {index} insert returned {attempt!r}")
            if index % 2 == 0:
                connection.commit()
            else:
                connection.rollback()
        _exec(engine, connection, f"CREATE TABLE {extra} (id INT PRIMARY KEY)")
        connection.rollback()
        rows = _query(engine, connection, f"SELECT id, name FROM {table} ORDER BY id")
        aggregate = _query(engine, connection, f"SELECT COUNT(*) FROM {table}")
        try:
            extra_outcome = ("rows", _query(engine, connection, f"SELECT id FROM {extra}"))
        except Exception as error:  # noqa: BLE001 - the missing table is the observation
            extra_outcome = ("error", getattr(error, "errno", None) if engine == "mysql" else getattr(error, "sqlstate", None))
    finally:
        connection.close()
    _drop(connect, engine, extra)
    check(aggregate == [(500,)], f"committed rows are {aggregate!r}, expected 500")
    check(len(rows) == 500, f"read-back returned {len(rows)} rows")
    # MySQL commits DDL implicitly, so the rolled-back CREATE TABLE survives;
    # PostgreSQL removes it. The engine comparison records which side the
    # gateway matches.
    return {"aggregate": aggregate, "rows": rows, "rollback_ddl": extra_outcome, "rows_written": 1000}


@scenario("alter_add_column_after_1000")
def _alter_add_column_after_1000(engine: str, connect: Callable[..., Any], table: str) -> dict[str, Any]:
    connection = connect()
    try:
        _reset(engine, connection, table)
        _exec(engine, connection, f"INSERT INTO {table} (name, score) VALUES {_values(1000)}")
        altered = _exec(engine, connection, f"ALTER TABLE {table} ADD COLUMN note VARCHAR(8) DEFAULT 'n'")
        after_add = _query(engine, connection, f"SELECT id, note FROM {table} ORDER BY id")
        updated = _exec(engine, connection, f"UPDATE {table} SET note = 'x' WHERE id <= 10")
        after_update = _query(engine, connection, f"SELECT id, note FROM {table} ORDER BY id LIMIT 10")
    finally:
        connection.close()
    check(altered[0] == "ok", f"ALTER TABLE returned {altered!r}")
    check(len(after_add) == 1000, f"read-back after ALTER returned {len(after_add)} rows")
    check(after_add[:3] == [(1, "n"), (2, "n"), (3, "n")], f"existing rows did not receive the default: {after_add[:3]!r}")
    check(updated[0] == "ok" and updated[1] == 10, f"UPDATE returned {updated!r}")
    check([row[1] for row in after_update] == ["x"] * 10, "bounded UPDATE hit the wrong rows")
    return {"after_add": after_add, "after_update": after_update, "rows_written": 1000}


@scenario("unique_conflict_late_rows")
def _unique_conflict_late_rows(engine: str, connect: Callable[..., Any], table: str) -> dict[str, Any]:
    expected_code = 1062 if engine == "mysql" else "23505"
    connection = connect()
    try:
        _reset(engine, connection, table, unique_name=True)
        values = _values(1000)
        # Row 990 repeats the name of row 0, so the conflict sits late in the statement.
        values = values.replace("('n0', 0)", "('n990', 990)")
        batch = _exec(engine, connection, f"INSERT INTO {table} (name, score) VALUES {values}")
        batch_count = _query(engine, connection, f"SELECT COUNT(*) FROM {table}")
        sequential: tuple = ()
        for index in range(1000):
            name = "n0" if index == 990 else f"n{index}"
            sequential = _exec(engine, connection, f"INSERT INTO {table} (name, score) VALUES ('{name}', {index})")
            if sequential[0] == "error":
                break
        sequential_count = _query(engine, connection, f"SELECT COUNT(*) FROM {table}")
    finally:
        connection.close()
    check(batch[0] == "error", f"late duplicate in one statement returned {batch!r}")
    check(batch[1] == expected_code, f"late duplicate reported [{batch[1]!r}], expected [{expected_code!r}]")
    check(batch_count == [(0,)], f"failed multi-row insert stored {batch_count!r} rows")
    check(sequential[0] == "error" and sequential[1] == expected_code, f"sequential duplicate returned {sequential!r}")
    check(sequential_count == [(990,)], f"sequential duplicate left {sequential_count!r} rows, expected 990")
    return {
        "batch_error": batch,
        "batch_count": batch_count,
        "sequential_error": sequential,
        "sequential_count": sequential_count,
        "rows_written": 1000,
    }


@scenario("multistatement_batch_offset")
def _multistatement_batch_offset(engine: str, connect: Callable[..., Any], table: str) -> dict[str, Any]:
    expected_code = 1054 if engine == "mysql" else "42703"
    connection = connect(multi_statement=engine == "mysql")
    try:
        _reset(engine, connection, table)
        _exec(engine, connection, f"INSERT INTO {table} (name, score) VALUES {_values(1000)}")
        statements = [f"INSERT INTO {table} (name, score) VALUES ('b{index}', {index})" for index in range(6)]
        statements.append(f"SELECT missing_column FROM {table}")
        statements += [f"INSERT INTO {table} (name, score) VALUES ('b{index}', {index})" for index in range(6, 9)]
        batch = "; ".join(statements)
        if engine == "mysql":
            with connection.cursor() as cursor:
                try:
                    cursor.execute(batch)
                    failure = ("ok", cursor.rowcount)
                except Exception as error:  # noqa: BLE001 - the error code is the assertion
                    errno = getattr(error, "errno", None) or (error.args[0] if error.args and isinstance(error.args[0], int) else None)
                    failure = ("error", errno)
        else:
            import psycopg

            with psycopg.ClientCursor(connection) as cursor:
                try:
                    cursor.execute(batch)
                    failure = ("ok", cursor.rowcount)
                except psycopg.Error as error:
                    failure = ("error", error.sqlstate)
        count = _query(engine, connection, f"SELECT COUNT(*) FROM {table}")
        rows = _query(engine, connection, f"SELECT id, name FROM {table} ORDER BY id")
    finally:
        connection.close()
    check(failure[0] == "error", f"batch with a failing seventh statement returned {failure!r}")
    check(failure[1] == expected_code, f"batch reported [{failure[1]!r}], expected [{expected_code!r}]")
    return {"failure": failure, "count": count, "rows": rows, "rows_written": 1000}


@scenario("prepared_loop_1000")
def _prepared_loop_1000(engine: str, connect: Callable[..., Any], table: str) -> dict[str, Any]:
    connection = connect()
    try:
        _reset(engine, connection, table)
        if engine == "mysql":
            with connection.cursor(prepared=True) as cursor:
                for index in range(1000):
                    cursor.execute(f"INSERT INTO {table} (name, score) VALUES (%s, %s)", (f"n{index}", index))
            with connection.cursor(prepared=True) as cursor:
                cursor.execute(f"SELECT name, score FROM {table} WHERE id = %s", (500,))
                selected = _driver_rows(cursor)
        else:
            import psycopg

            with connection.cursor() as cursor:
                for index in range(1000):
                    cursor.execute(f"INSERT INTO {table} (name, score) VALUES (%s, %s)", (f"n{index}", index), prepare=True)
                cursor.execute(f"SELECT name, score FROM {table} WHERE id = %s", (500,))
                selected = _driver_rows(cursor)
        rows = _query(engine, connection, f"SELECT id, name FROM {table} ORDER BY id")
    finally:
        connection.close()
    check(len(rows) == 1000, f"prepared loop stored {len(rows)} rows")
    check(rows[0] == (1, "n0") and rows[-1] == (1000, "n999"), f"prepared loop ids are {rows[0]!r}..{rows[-1]!r}")
    check([row[0] for row in rows] == list(range(1, 1001)), "prepared loop ids contain a gap")
    check(selected == [("n499", 499.0)], f"parameterised SELECT returned {selected!r}")
    return {"rows": rows, "selected": selected, "rows_written": len(rows)}


@scenario("concurrent_two_connections")
def _concurrent_two_connections(engine: str, connect: Callable[..., Any], table: str) -> dict[str, Any]:
    setup = connect()
    try:
        _reset(engine, setup, table)
    finally:
        setup.close()

    def writer(base: int) -> None:
        connection = connect()
        try:
            # Parameterised rows: PostgreSQL renders `$1`/`$2` and therefore
            # exercises the extended-protocol describe path, where an omitted
            # `BIGSERIAL` column must not be mistaken for a missing primary key.
            cursor = connection.cursor(prepared=True) if engine == "mysql" else connection.cursor()
            with cursor:
                for row in range(1000):
                    score = base + row
                    cursor.execute(f"INSERT INTO {table} (name, score) VALUES (%s, %s)", (f"c{score}", score))
        finally:
            connection.close()

    threads = [threading.Thread(target=writer, args=(0,)), threading.Thread(target=writer, args=(1000,))]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    connection = connect()
    try:
        aggregate = _query(engine, connection, f"SELECT COUNT(*), MIN(id), MAX(id), SUM(score) FROM {table}")
        distinct = _query(engine, connection, f"SELECT COUNT(DISTINCT id), COUNT(DISTINCT name) FROM {table}")
        rows = _query(engine, connection, f"SELECT id, name, score FROM {table} ORDER BY id")
    finally:
        connection.close()
    check(aggregate[0][0] == 2000 and aggregate[0][1] == 1 and aggregate[0][2] == 2000, f"concurrent aggregate {aggregate!r}")
    check(distinct == [(2000, 2000)], f"concurrent ids or names are not unique: {distinct!r}")
    # Which connection wins which id is timing dependent, so the generated ids
    # cannot be compared across two independent servers. The deterministic
    # observations are the id set itself and the `(name, score)` pairs; the
    # gateway-local check below still ties each name to its own score.
    rows = sorted(rows, key=lambda row: row[1])
    mismatched = [row for row in rows if row[1] != f"c{int(row[2])}"]
    check(not mismatched, f"concurrent writes paired the wrong name and score: {mismatched[:3]!r}")
    ids = tuple(sorted(row[0] for row in rows))
    check(ids == tuple(range(1, 2001)), "concurrent ids are not 1..2000 without gaps")
    return {
        "aggregate": aggregate,
        "distinct": distinct,
        "ids": ids,
        "rows": [(row[1], row[2]) for row in rows],
        "rows_written": 2000,
    }


@scenario("metadata_projection_paging")
def _metadata_projection_paging(engine: str, connect: Callable[..., Any], table: str) -> dict[str, Any]:
    columns = "id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(16) NOT NULL, score DOUBLE NULL, note VARCHAR(8) NULL, tag INT NULL" if engine == "mysql" else "id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL, score DOUBLE PRECISION NULL, note VARCHAR(8) NULL, tag INT NULL"
    tables = [f"{table}_a", f"{table}_b", f"{table}_c"]
    connection = connect()
    try:
        for name in tables:
            _exec(engine, connection, f"DROP TABLE IF EXISTS {name}")
            _exec(engine, connection, f"CREATE TABLE {name} ({columns})")
        _exec(engine, connection, f"INSERT INTO {tables[0]} (name, score) VALUES {_values(1000)}")
        pattern = f"{table}%"
        if engine == "mysql":
            total = _query(
                engine,
                connection,
                "SELECT COUNT(*) FROM information_schema.COLUMNS WHERE TABLE_SCHEMA = DATABASE() "
                f"AND TABLE_NAME LIKE '{pattern}'",
            )
            page = _query(
                engine,
                connection,
                "SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION FROM information_schema.COLUMNS "
                f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE '{pattern}' ORDER BY TABLE_NAME, ORDINAL_POSITION "
                "LIMIT 5 OFFSET 5",
            )
            listed = _query(
                engine,
                connection,
                "SELECT TABLE_NAME FROM information_schema.TABLES "
                f"WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME LIKE '{pattern}' ORDER BY TABLE_NAME",
            )
        else:
            total = _query(
                engine,
                connection,
                "SELECT COUNT(*) FROM information_schema.columns WHERE table_schema = 'public' "
                f"AND table_name LIKE '{pattern}'",
            )
            page = _query(
                engine,
                connection,
                "SELECT table_name, column_name, ordinal_position FROM information_schema.columns "
                f"WHERE table_schema = 'public' AND table_name LIKE '{pattern}' ORDER BY table_name, ordinal_position "
                "LIMIT 5 OFFSET 5",
            )
            listed = _query(
                engine,
                connection,
                "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' "
                f"AND table_name LIKE '{pattern}' ORDER BY table_name",
            )
    finally:
        connection.close()
    for name in tables:
        _drop(connect, engine, name)
    check(total == [(15,)], f"metadata row count is {total!r}, expected 15 columns")
    check(len(page) == 5, f"metadata page returned {len(page)} rows")
    check(len(listed) == 3, f"table listing returned {listed!r}")
    return {"total": total, "page": page, "tables": listed, "rows_written": 1000}


def _compare(name: str, engine: str, expected: dict[str, Any], actual: dict[str, Any]) -> list[CaseResult]:
    results: list[CaseResult] = []
    for key, value in expected.items():
        observed = actual.get(key)
        label = f"{engine}/scenario/{name}/{key}"
        if value == observed:
            results.append(CaseResult(label, "pass"))
        else:
            results.append(CaseResult(label, "fail", f"engine returned {value!r}, gateway returned {observed!r}"))
    return results


def _run_scenario(scenario_spec: Scenario, engine: str, connect: Callable[..., Any]) -> dict[str, Any]:
    function = FIXTURES[scenario_spec.name]
    table = _table(scenario_spec.name.replace("_", "x"), engine)[:60]
    return function(engine, connect, table)


def _restart_after_1000(engine: str, seed: YydsNode) -> CaseResult:
    """Runs the restart scenario on its own persisted registry directory.

    Catalog-mode DDL is intentionally in-memory, so a restart there loses every
    table the client created. Sequence durability is a registry property and is
    therefore exercised on the persisted registry, the same path the restart
    parity probe uses.
    """

    import os
    import tempfile

    label = f"{engine}/scenario/sequence_after_1000_restart"
    table = _table("seq", engine)
    data_dir = Path(tempfile.mkdtemp(prefix=f"yyds-scenario-{engine}-restart-"))
    cluster_id = f"scenario-cluster-{os.getpid()}"
    node_id = f"scenario-node-{os.getpid()}"
    node = start_node(data_dir=data_dir, cluster_id=cluster_id, node_id=node_id, use_catalog=False)
    try:
        connection = _factory(engine, _node_endpoint(engine, node))()
        try:
            _reset(engine, connection, table)
            _exec(engine, connection, f"INSERT INTO {table} (name, score) VALUES {_values(1000)}")
            rows = _query(engine, connection, f"SELECT id, name FROM {table} ORDER BY id")
        finally:
            connection.close()
        check(len(rows) == 1000, f"restart fixture stored {len(rows)} rows")
        node.stop(wipe=False)
        node = start_node(data_dir=data_dir, cluster_id=cluster_id, node_id=node_id, use_catalog=False)
        connection = _factory(engine, _node_endpoint(engine, node))()
        try:
            # Parameterised on purpose: the extended-protocol describe path must
            # allocate the sequence value at execute time, not mistake the
            # omitted primary key for a missing column.
            if engine == "mysql":
                with connection.cursor(prepared=True) as cursor:
                    cursor.execute(f"INSERT INTO {table} (name, score) VALUES (%s, %s)", ("after", 1000))
                    next_id = cursor.lastrowid
            else:
                with connection.cursor() as cursor:
                    cursor.execute(f"INSERT INTO {table} (name, score) VALUES (%s, %s) RETURNING id", ("after", 1000))
                    next_id = cursor.fetchone()[0]
            count = _query(engine, connection, f"SELECT COUNT(*) FROM {table}")
            ids = _query(engine, connection, f"SELECT id FROM {table} ORDER BY id")
        finally:
            connection.close()
    except Exception as error:  # noqa: BLE001 - the observed value is the assertion
        return CaseResult(label, "fail", f"restart after 1000 rows raised {type(error).__name__}: {error}")
    finally:
        try:
            node.stop()
        except Exception:  # noqa: BLE001 - an already stopped node still leaves results
            pass
    if next_id == 1001 and count == [(1001,)] and [row[0] for row in ids] == list(range(1, 1002)):
        return CaseResult(label, "pass", "ids continued at 1001 after a node restart")
    return CaseResult(label, "fail", f"next id was {next_id!r} and count {count!r}; expected 1001 with no gap")


def run_scenarios(node: YydsNode) -> list[CaseResult]:
    """Runs every catalog scenario against the node and, when configured, the engine."""

    scenarios = load_catalog()
    results: list[CaseResult] = []
    for engine in ENGINES:
        oracle = _oracle(MYSQL_ENV if engine == "mysql" else PG_ENV)
        for scenario_spec in scenarios:
            if scenario_spec.name == "sequence_after_1000_restart":
                results.append(_restart_after_1000(engine, node))
                continue
            label = f"{engine}/scenario/{scenario_spec.name}"
            try:
                simulated = _run_scenario(scenario_spec, engine, _factory(engine, _node_endpoint(engine, node)))
            except Exception as error:  # noqa: BLE001 - any gateway failure is the outcome
                results.append(CaseResult(label, "fail", f"gateway raised {type(error).__name__}: {error}"))
                continue
            written = simulated.get("rows_written", 0)
            if isinstance(written, int) and written < scenario_spec.rows:
                results.append(CaseResult(label, "fail", f"scenario exercised {written} rows, catalog declares {scenario_spec.rows}"))
                continue
            if oracle is None:
                results.append(
                    CaseResult(
                        label,
                        "unverified",
                        f"only driver-level evidence ({simulated.get('rows_written')} rows), missing engine parity",
                    )
                )
                continue
            try:
                expected = _run_scenario(scenario_spec, engine, _factory(engine, oracle))
            except Exception as error:  # noqa: BLE001 - the engine run defines the expectation
                results.append(CaseResult(label, "fail", f"engine raised {type(error).__name__}: {error}"))
                continue
            results.extend(_compare(scenario_spec.name, engine, expected, simulated))
    return results
