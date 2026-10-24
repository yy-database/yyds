use std::{
    collections::HashMap,
    ffi::OsString,
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use yyds_control::{LocalOwnership, NodeIdentity, NodeLease};
use yyds_execution::{KeyValueExecutor, LocalExecutor};
use yyds_gateway::SqlCatalog;
use yyds_gateway_conformance::{
    python_string, render_python, ClientMode, SCHEMA_VERSION, USERS_SCHEMA,
};
use yyds_gateway_pgsql::service::PgsqlService;

fn owner_directory() -> PathBuf {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "yyds-pgsql-reference-{}-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn reference_python() -> OsString {
    std::env::var_os("YYDS_PGSQL_REFERENCE_PYTHON").expect("reference Python executable")
}

fn lease(directory: &PathBuf) -> NodeLease {
    NodeLease::acquire(directory, NodeIdentity::new("reference-cluster", "reference-node").unwrap()).unwrap()
}

/// Runs an embedded psycopg2 script against the bound gateway port.
fn run_reference_client(script: &str, port: u16) {
    run_reference_client_with_timeout(script, port, Duration::from_secs(12));
}

fn run_reference_client_with_timeout(script: &str, port: u16, timeout: Duration) {
    // Keep the generated program on disk so a failing assertion can be mapped
    // back to the exact client line that produced it.
    let path = std::env::temp_dir().join(format!("yyds-pgsql-reference-{port}.py"));
    std::fs::write(&path, script).unwrap();
    // Scripts can exceed the Windows command-line limit, so feed the program
    // through stdin and keep the port as the only argument.
    let mut child = Command::new(reference_python())
        .arg("-")
        .arg(port.to_string())
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    {
        let mut stdin = child.stdin.take().unwrap();
        std::io::Write::write_all(&mut stdin, script.as_bytes()).unwrap();
    }
    let deadline = Instant::now() + timeout;
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!(
                "reference client exceeded deadline, script at {}: {}",
                path.display(),
                String::from_utf8_lossy(&output.stderr)
            );
        }
        thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    assert!(
        output.status.success(),
        "reference client failed, script at {}: {}",
        path.display(),
        String::from_utf8_lossy(&output.stderr)
    );
    std::fs::remove_file(&path).ok();
}

/// Builds the gateway catalog from a VOS schema source.
fn catalog(source: &str) -> SqlCatalog {
    let projection = vos::parse_oak(source).expect("Oak VOS source").project_schema().expect("projection");
    let mut next_field_id = 1u64;
    let types = projection
        .types
        .iter()
        .enumerate()
        .map(|(type_index, entry)| vos::contract::TypeIdentity {
            canonical_path: entry.canonical_path.clone(),
            type_id: type_index as u64 + 1,
            kind: entry.kind,
            fields: entry
                .fields
                .iter()
                .enumerate()
                .map(|(field_index, field)| {
                    let field_id = next_field_id;
                    next_field_id += 1;
                    vos::contract::FieldIdentity {
                        canonical_name: field.canonical_name.clone(),
                        field_id,
                        virtual_field_index: field_index as u32,
                    }
                })
                .collect(),
        })
        .collect();
    let manifest = vos::contract::IdentityManifest { format_version: vos::contract::IDENTITY_MANIFEST_VERSION.into(), types };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    SqlCatalog::from_resolved_contract(SCHEMA_VERSION, &contract).expect("gateway catalog")
}

fn empty_executor() -> LocalExecutor<yyds_kv::MemoryShard> {
    use yyds_kv::MemoryShard;
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let epoch = ShardEpoch(1);
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(epoch, vec![shard_id.clone()]).expect("routing");
    let mut shards = HashMap::new();
    shards.insert(shard_id, MemoryShard::new());
    LocalExecutor::new(routing, shards).expect("executor")
}

#[test]
#[ignore = "requires YYDS_PGSQL_REFERENCE_PYTHON with psycopg2 installed"]
fn psycopg2_establishes_a_protocol_v3_session() {
    let directory = owner_directory();
    let owner = Arc::new(lease(&directory));
    let service = PgsqlService::start(owner, "127.0.0.1:0".parse().unwrap()).unwrap();
    let script = r#"
import psycopg2, sys
connection = psycopg2.connect(host='127.0.0.1', port=int(sys.argv[1]), user='yyds',
                              dbname='yyds', application_name='yyds-reference',
                              connect_timeout=4, sslmode='prefer')
assert connection.server_version >= 160000
try:
    connection.cursor().execute('select 1')
except psycopg2.Error as error:
    assert error.pgcode == '0A000', (error.pgcode, str(error))
else:
    raise AssertionError('unsupported SQL unexpectedly returned a result')
assert connection.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_INERROR
try:
    connection.cursor().execute('select 2')
except psycopg2.Error as error:
    assert error.pgcode == '25P02', (error.pgcode, str(error))
else:
    raise AssertionError('aborted transaction unexpectedly accepted a query')
connection.rollback()
assert connection.get_transaction_status() == psycopg2.extensions.TRANSACTION_STATUS_IDLE
try:
    connection.cursor().execute('select (')
except psycopg2.Error as error:
    assert error.pgcode == '42601', (error.pgcode, str(error))
else:
    raise AssertionError('malformed SQL unexpectedly passed Oak parsing')
connection.rollback()
connection.close()
print('psycopg2 protocol-v3 startup and explicit unsupported-query response passed')
"#;
    run_reference_client(script, service.address().port());
    service.stop().unwrap();
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}

#[test]
#[ignore = "requires YYDS_PGSQL_REFERENCE_PYTHON with psycopg (3.x) installed"]
fn psycopg3_binary_results_report_catalog_types_and_round_trip_binary_parameters() {
    let directory = owner_directory();
    let owner = Arc::new(lease(&directory));
    let catalog =
        catalog("table metrics { @@id: i64, score: f64, active: bool, blob: bytes, small: i32 }\n");
    let executor: Arc<dyn KeyValueExecutor> = Arc::new(empty_executor());
    let service =
        PgsqlService::start_with_executor(owner, "127.0.0.1:0".parse().unwrap(), executor, catalog, 7).unwrap();
    let script = r#"
import psycopg, sys

def connect():
    return psycopg.connect(host='127.0.0.1', port=int(sys.argv[1]), user='yyds',
                           dbname='yyds', connect_timeout=4, sslmode='prefer')

connection = connect()
with connection.cursor() as cursor:
    cursor.execute("INSERT INTO metrics VALUES (1, 1.5, TRUE, 'bin', 7)")
connection.commit()

# Binary result formats: every column decodes natively instead of degrading to text.
with connection.cursor(binary=True) as cursor:
    cursor.execute("SELECT id, score, active, blob, small FROM metrics WHERE id = %s", (1,))
    assert [column.type_code for column in cursor.description] == [20, 701, 16, 17, 23], \
        [column.type_code for column in cursor.description]
    row = cursor.fetchone()
    assert row == (1, 1.5, True, b'bin', 7), row
    assert isinstance(row[0], int) and isinstance(row[1], float) and isinstance(row[4], int), row

# An empty result set still describes the catalog column types, not `text`.
with connection.cursor(binary=True) as cursor:
    cursor.execute("SELECT id, score, active, blob, small FROM metrics WHERE id = %s", (2,))
    assert [column.type_code for column in cursor.description] == [20, 701, 16, 17, 23], \
        [column.type_code for column in cursor.description]
    assert cursor.fetchall() == []
connection.commit()

connection.close()
print('psycopg3 binary rows and parameters passed')
"#;
    run_reference_client(script, service.address().port());
    service.stop().unwrap();
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}

#[test]
#[ignore = "requires YYDS_PGSQL_REFERENCE_PYTHON with psycopg2 installed"]
fn psycopg2_round_trips_typed_results_parameters_transactions_and_conflicts() {
    let directory = owner_directory();
    let owner = Arc::new(lease(&directory));
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64, score: f64, active: bool }\n");
    let executor: Arc<dyn KeyValueExecutor> = Arc::new(empty_executor());
    let service =
        PgsqlService::start_with_executor(owner, "127.0.0.1:0".parse().unwrap(), executor, catalog, 7).unwrap();
    let script = r#"
import psycopg2, sys

def connect():
    return psycopg2.connect(host='127.0.0.1', port=int(sys.argv[1]), user='yyds',
                            dbname='yyds', application_name='yyds-acceptance',
                            connect_timeout=4, sslmode='prefer')

connection = connect()
assert connection.server_version >= 160000
cursor = connection.cursor()

# 1. SimpleQuery INSERT/UPDATE/DELETE
cursor.execute("INSERT INTO users VALUES (1, 'alice', 30, 1.5, 1)")
assert cursor.rowcount == 1, cursor.rowcount
connection.commit()

# 1b. Multi-row UPDATE and DELETE without a primary-key equality
cursor.execute("INSERT INTO users VALUES (3, 'bob', 20, 0.5, 1)")
assert cursor.rowcount == 1, cursor.rowcount
cursor.execute("UPDATE users SET age = 31 WHERE active = TRUE")
assert cursor.rowcount == 2, cursor.rowcount
cursor.execute("DELETE FROM users WHERE active = TRUE")
assert cursor.rowcount == 2, cursor.rowcount
connection.rollback()
cursor.execute("SELECT id, age FROM users WHERE active = TRUE")
assert cursor.fetchall() == [(1, 30)]

# 3. Extended query: text-format values cast by the declared column OIDs
cursor.execute("SELECT id, name, age, score, active FROM users WHERE id = %s", (1,))
assert [entry[0] for entry in cursor.description] == ['id', 'name', 'age', 'score', 'active']
assert [entry[1] for entry in cursor.description] == [20, 25, 20, 701, 16], [entry[1] for entry in cursor.description]
row = cursor.fetchone()
assert row == (1, 'alice', 30, 1.5, True), row
assert isinstance(row[0], int) and isinstance(row[2], int) and isinstance(row[3], float) and isinstance(row[4], bool), row
connection.commit()

# 3. Text parameter and NULL parameter
cursor.execute("SELECT id FROM users WHERE name = %s", ('alice',))
assert cursor.fetchall() == [(1,)]
cursor.execute("SELECT id FROM users WHERE name = %s", (None,))
assert cursor.fetchall() == []
connection.commit()

# 4. Text with a leading zero must not be coerced to an integer
cursor.execute("INSERT INTO users VALUES (2, '007', 0, 0.25, 0)")
connection.commit()
cursor.execute("SELECT name, age FROM users WHERE id = %s", (2,))
assert cursor.fetchone() == ('007', 0)
connection.commit()

# 5. Rollback discards the buffered insert
cursor.execute("INSERT INTO users VALUES (3, 'temp', 5, 0.0, 1)")
connection.rollback()
cursor.execute("SELECT id FROM users WHERE id = %s", (3,))
assert cursor.fetchall() == []
connection.commit()

# 6. Error responses keep SQLSTATE fidelity
try:
    cursor.execute("SELECT id FROM missing")
except psycopg2.Error as error:
    assert error.pgcode == '42P01', (error.pgcode, str(error))
else:
    raise AssertionError('missing table unexpectedly resolved')
connection.rollback()
try:
    cursor.execute("SELECT (")
except psycopg2.Error as error:
    assert error.pgcode == '42601', (error.pgcode, str(error))
else:
    raise AssertionError('malformed SQL unexpectedly passed Oak parsing')
connection.rollback()
try:
    cursor.execute("SELECT nope FROM users")
except psycopg2.Error as error:
    assert error.pgcode == '42703', (error.pgcode, str(error))
else:
    raise AssertionError('unknown column unexpectedly resolved')
connection.rollback()
try:
    cursor.execute("INSERT INTO users VALUES (9, 42, 0, 0.0, 1)")
except psycopg2.Error as error:
    assert error.pgcode == '42804', (error.pgcode, str(error))
else:
    raise AssertionError('integer write into a text column unexpectedly passed')
connection.rollback()
try:
    cursor.execute("INSERT INTO users VALUES (NULL, 'x', 0, 0.0, 1)")
except psycopg2.Error as error:
    assert error.pgcode == '23502', (error.pgcode, str(error))
else:
    raise AssertionError('NULL primary key unexpectedly passed')
connection.rollback()
try:
    cursor.execute("INSERT INTO users VALUES (1, 'dup', 0, 0.0, 1)")
except psycopg2.Error as error:
    assert error.pgcode == '23505', (error.pgcode, str(error))
else:
    raise AssertionError('duplicate primary key unexpectedly passed')
connection.rollback()

# 7. Concurrent read-modify-write must surface a serialization failure at COMMIT
other = connect()
other_cursor = other.cursor()
cursor.execute("UPDATE users SET age = 41 WHERE id = %s", (1,))
other_cursor.execute("UPDATE users SET age = 42 WHERE id = %s", (1,))
other.commit()
try:
    connection.commit()
except psycopg2.Error as error:
    assert error.pgcode == '40001', (error.pgcode, str(error))
else:
    raise AssertionError('stale transaction commit unexpectedly succeeded')
connection.rollback()
cursor.execute("SELECT age FROM users WHERE id = %s", (1,))
assert cursor.fetchone() == (42,)
connection.commit()
other.close()
connection.close()
print('psycopg2 typed results, parameters, transactions and conflicts passed')
"#;
    run_reference_client(script, service.address().port());
    service.stop().unwrap();
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}

/// Runs the shared SQL case table through a real psycopg 3 client.
///
/// Each case runs inside its own transaction that is rolled back afterwards, so
/// every case observes the seeded fixture only. Text and binary cursors cover
/// both result formats the gateway advertises.
#[test]
#[ignore = "requires YYDS_PGSQL_REFERENCE_PYTHON with psycopg (3.x) installed"]
fn psycopg3_runs_the_shared_sql_cases_in_text_and_binary_modes() {
    let directory = owner_directory();
    let owner = Arc::new(lease(&directory));
    let catalog = catalog(USERS_SCHEMA);
    let executor: Arc<dyn KeyValueExecutor> = Arc::new(empty_executor());
    let service = PgsqlService::start_with_executor(
        owner,
        "127.0.0.1:0".parse().unwrap(),
        executor,
        catalog,
        SCHEMA_VERSION,
    )
    .unwrap();
    for mode in [ClientMode::PostgresText, ClientMode::PostgresBinary] {
        run_psycopg3_conformance(&service, mode);
    }
    service.stop().unwrap();
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}

fn run_psycopg3_conformance(service: &PgsqlService, mode: ClientMode) {
    let rendered = render_python(mode);
    eprintln!("{}: {} cases, {} excluded", mode.label(), rendered.applied, rendered.excluded.len());
    for (name, reason) in &rendered.excluded {
        eprintln!("  excluded `{name}`: {reason}");
    }
    let skipped = rendered
        .excluded
        .iter()
        .map(|(name, reason)| format!("({}, {})", python_string(name), python_string(reason)))
        .collect::<Vec<_>>()
        .join(", ");
    let binary = if mode == ClientMode::PostgresBinary { "True" } else { "False" };
    let script = format!(
        "import psycopg, sys\n\n{}\nSKIPPED = [{}]\nBINARY = {}\n{}",
        rendered.literal, skipped, binary, PSYCOPG3_CONFORMANCE_BODY
    );
    run_reference_client_with_timeout(&script, service.address().port(), Duration::from_secs(60));
}

const PSYCOPG3_CONFORMANCE_BODY: &str = r#"
def connect():
    return psycopg.connect(host='127.0.0.1', port=int(sys.argv[1]), user='yyds',
                           dbname='yyds', application_name='yyds-conformance',
                           connect_timeout=4, sslmode='prefer')

def expect(expected, actual, context):
    kind, value = expected
    if kind == 'null':
        ok = actual is None
    elif kind == 'int':
        ok = type(actual) is int and actual == value
    elif kind == 'float':
        ok = type(actual) is float and actual == value
    elif kind == 'bool':
        ok = type(actual) is bool and actual == value
    elif kind == 'str':
        ok = type(actual) is str and actual == value
    else:
        ok = False
    assert ok, f'{context}: expected {kind} {value!r}, got {type(actual).__name__} {actual!r}'

def check_rows(actual, expected, context):
    assert len(actual) == len(expected), \
        f'{context}: expected {len(expected)} rows, got {len(actual)}: {actual!r}'
    for index, (row, want) in enumerate(zip(actual, expected)):
        assert len(row) == len(want), f'{context} row {index}: expected {len(want)} columns, got {len(row)}'
        for expected_cell, actual_cell in zip(want, row):
            expect(expected_cell, actual_cell, f'{context} row {index}')

connection = connect()
cursor = connection.cursor(binary=BINARY)

for case in CASES:
    for statement in case['setup']:
        cursor.execute(statement)
    if case['params']:
        cursor.execute(case['sql'], tuple(value for _, value in case['params']))
    else:
        cursor.execute(case['sql'])
    if case['write']:
        assert cursor.rowcount == case['affected'], \
            f"{case['name']}: expected {case['affected']} affected rows, got {cursor.rowcount}"
    else:
        check_rows(cursor.fetchall(), case['rows'], case['name'] + ' result')
    cursor.execute(case['state_sql'])
    check_rows(cursor.fetchall(), case['state'], case['name'] + ' state')
    for note in case['notes']:
        print(f"note {case['name']}: {note}")
    connection.rollback()

def sqlstate(sql, params=None):
    try:
        if params is None:
            cursor.execute(sql)
        else:
            cursor.execute(sql, params)
    except psycopg.Error as error:
        connection.rollback()
        return error.sqlstate
    connection.rollback()
    raise AssertionError(f'{sql} unexpectedly succeeded')

# Driver-level boundaries the shared statements cannot express per mode.
cursor.execute("INSERT INTO users VALUES (1, 'alice', 30, 1.5, TRUE, 'x')")
cursor.execute("SELECT id FROM users WHERE name = %s", (None,))
assert cursor.fetchall() == [], 'NULL parameter must stay unknown for equality'
cursor.execute("INSERT INTO users VALUES (2, 'max', 9223372036854775807, 0.0, FALSE, NULL)")
cursor.execute("SELECT age FROM users WHERE age = %s", (9223372036854775807,))
got = cursor.fetchall()
assert got == [(9223372036854775807,)] and type(got[0][0]) is int, got
cursor.execute("SELECT id, name FROM users WHERE id = %s", (-1,))
assert [column.type_code for column in cursor.description] == [20, 25], \
    [column.type_code for column in cursor.description]
assert cursor.fetchall() == [], 'empty result must still describe column types'
cursor.execute("UPDATE users SET age = 31 WHERE id = %s", (1,))
assert cursor.rowcount == 1, cursor.rowcount
cursor.execute("DELETE FROM users WHERE id = %s", (1,))
assert cursor.rowcount == 1, cursor.rowcount
connection.rollback()
cursor.execute("SELECT COUNT(*) FROM users")
assert cursor.fetchall() == [(0,)], 'rollback must discard every buffered write'
connection.rollback()

def sqlstate_is(sql, expected):
    actual = sqlstate(sql)
    assert actual == expected, f'{sql}: expected SQLSTATE {expected}, got {actual}'

sqlstate_is("SELECT id FROM missing", '42P01')
sqlstate_is("SELECT nope FROM users", '42703')
sqlstate_is("SELECT (", '42601')
sqlstate_is("INSERT INTO users VALUES (NULL, 'x', 0, 0.0, TRUE, NULL)", '23502')

try:
    cursor.execute("INSERT INTO users VALUES (5, 42, 0, 0.0, TRUE, NULL)")
except psycopg.Error as error:
    assert error.sqlstate == '42804', error.sqlstate
else:
    raise AssertionError('integer write into a text column unexpectedly passed')
try:
    cursor.execute("SELECT COUNT(*) FROM users")
except psycopg.Error as error:
    assert error.sqlstate == '25P02', error.sqlstate
else:
    raise AssertionError('aborted transaction unexpectedly accepted a query')
connection.rollback()

cursor.execute("INSERT INTO users VALUES (7, 'dup', 0, 0.0, TRUE, NULL)")
connection.commit()
sqlstate_is("INSERT INTO users VALUES (7, 'dup', 0, 0.0, TRUE, NULL)", '23505')
cursor.execute("DELETE FROM users WHERE id = %s", (7,))
connection.commit()

# Concurrent read-modify-write must fail at COMMIT with a serialization error.
cursor.execute("INSERT INTO users VALUES (8, 'conflict', 0, 0.0, TRUE, NULL)")
connection.commit()
other = connect()
other_cursor = other.cursor(binary=BINARY)
cursor.execute("UPDATE users SET age = 41 WHERE id = %s", (8,))
other_cursor.execute("UPDATE users SET age = 42 WHERE id = %s", (8,))
other.commit()
try:
    connection.commit()
except psycopg.Error as error:
    assert error.sqlstate == '40001', error.sqlstate
else:
    raise AssertionError('stale transaction commit unexpectedly succeeded')
connection.rollback()
cursor.execute("SELECT age FROM users WHERE id = %s", (8,))
winning = cursor.fetchone()
assert winning == (42,), winning
connection.rollback()
other.close()

# Leave the database empty so the second cursor format starts from the fixture.
cursor.execute("DELETE FROM users WHERE id = %s", (8,))
connection.commit()
cursor.execute("SELECT COUNT(*) FROM users")
assert cursor.fetchall() == [(0,)], 'boundary block must restore an empty database'
connection.rollback()

for name, reason in SKIPPED:
    print(f'excluded {name}: {reason}')
connection.close()
print('psycopg3 shared SQL cases passed')
"#;