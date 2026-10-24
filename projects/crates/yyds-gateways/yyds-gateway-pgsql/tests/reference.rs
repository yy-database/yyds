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
    let mut child = Command::new(reference_python())
        .arg("-c")
        .arg(script)
        .arg(port.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(12);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!("reference client exceeded deadline: {}", String::from_utf8_lossy(&output.stderr));
        }
        thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
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
    SqlCatalog::from_resolved_contract(7, &contract).expect("gateway catalog")
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

# 2. Extended query: text-format values cast by the declared column OIDs
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
