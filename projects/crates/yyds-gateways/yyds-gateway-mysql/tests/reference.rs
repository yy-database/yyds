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
use yyds_gateway_mysql::service::MysqlService;

fn owner_directory() -> PathBuf {
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "yyds-mysql-reference-{}-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        SEQUENCE.fetch_add(1, Ordering::Relaxed)
    ))
}

fn reference_python() -> OsString {
    std::env::var_os("YYDS_MYSQL_REFERENCE_PYTHON").expect("reference Python executable")
}

fn lease(directory: &PathBuf) -> NodeLease {
    NodeLease::acquire(directory, NodeIdentity::new("reference-cluster", "reference-node").unwrap()).unwrap()
}

/// Runs an embedded PyMySQL script against the bound gateway port.
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
#[ignore = "requires YYDS_MYSQL_REFERENCE_PYTHON with PyMySQL installed"]
fn pymysql_connects_and_receives_explicit_unsupported_query_error() {
    let directory = owner_directory();
    let owner = Arc::new(lease(&directory));
    let service = MysqlService::start(owner, "127.0.0.1:0".parse().unwrap(), None).unwrap();
    let script = r#"
import pymysql, sys
connection = pymysql.connect(host='127.0.0.1', port=int(sys.argv[1]), user='yyds',
                             password='', database='yyds', connect_timeout=4,
                             read_timeout=4, write_timeout=4, autocommit=None)
assert connection.character_set_name() == 'utf8mb4'
try:
    with connection.cursor() as cursor:
        cursor.execute('select 1')
except pymysql.err.NotSupportedError as error:
    assert error.args[0] == 1235, error.args
else:
    raise AssertionError('unsupported SQL unexpectedly returned a result')
try:
    with connection.cursor() as cursor:
        cursor.execute('select (')
except pymysql.err.ProgrammingError as error:
    assert error.args[0] == 1064, error.args
else:
    raise AssertionError('malformed SQL unexpectedly passed Oak parsing')
connection.close()
print('PyMySQL protocol-v10 startup and explicit unsupported-query response passed')
"#;
    run_reference_client(script, service.address().port());
    service.stop().unwrap();
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}

#[test]
#[ignore = "requires YYDS_MYSQL_REFERENCE_PYTHON with mysql-connector-python installed"]
fn mysql_connector_prepared_statements_round_trip_typed_parameters() {
    let directory = owner_directory();
    let owner = Arc::new(lease(&directory));
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64, score: f64, active: bool }\n");
    let executor: Arc<dyn KeyValueExecutor> = Arc::new(empty_executor());
    let service =
        MysqlService::start_with_executor(owner, "127.0.0.1:0".parse().unwrap(), executor, catalog, 7).unwrap();
    let script = r#"
import mysql.connector, sys

connection = mysql.connector.connect(host='127.0.0.1', port=int(sys.argv[1]), user='yyds',
                                     password='', database='yyds', connection_timeout=4,
                                     charset='utf8mb4', collation='utf8mb4_general_ci',
                                     autocommit=True, use_pure=True)
cursor = connection.cursor(prepared=True)

# Integer, string, double and boolean parameters through COM_STMT_EXECUTE.
cursor.execute("INSERT INTO users VALUES (?, ?, ?, ?, ?)", (1, 'alice', 30, 1.5, True))
assert cursor.rowcount == 1, cursor.rowcount
cursor.execute("INSERT INTO users VALUES (?, ?, ?, ?, ?)", (2, 'bob', 25, 0.5, False))
assert cursor.rowcount == 1, cursor.rowcount

cursor.execute("SELECT id, name, age, score, active FROM users WHERE id = ?", (1,))
assert [column[0] for column in cursor.description] == ['id', 'name', 'age', 'score', 'active'], cursor.description
assert [column[1] for column in cursor.description] == [8, 253, 8, 5, 1], [column[1] for column in cursor.description]
row = cursor.fetchone()
assert row[:2] == (1, 'alice'), row
assert row[2] == 30 and abs(row[3] - 1.5) < 1e-9 and row[4] == 1, row
assert isinstance(row[0], int) and isinstance(row[3], float), row

# An empty result set keeps the declared column types.
cursor.execute("SELECT id, name FROM users WHERE id = ?", (99,))
assert cursor.fetchall() == [], cursor.fetchall()
assert [column[1] for column in cursor.description] == [8, 253], cursor.description

# NULL parameters are carried through the execute NULL bitmap.
cursor.execute("SELECT id FROM users WHERE name = ?", (None,))
assert cursor.fetchall() == [], cursor.fetchall()

# Statement handles can be reset and reused.
cursor.execute("SELECT name FROM users WHERE id = ?", (2,))
assert cursor.fetchall() == [('bob',)], 'reset statement must stay usable'

connection.close()
print('mysql-connector prepared statements passed')
"#;
    run_reference_client(script, service.address().port());
    service.stop().unwrap();
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}

#[test]
#[ignore = "requires YYDS_MYSQL_REFERENCE_PYTHON with PyMySQL installed"]
fn pymysql_round_trips_typed_results_parameters_transactions_and_conflicts() {
    let directory = owner_directory();
    let owner = Arc::new(lease(&directory));
    let catalog = catalog("table users { @@id: i64, name: utf8, age: i64, score: f64, active: bool }\n");
    let executor: Arc<dyn KeyValueExecutor> = Arc::new(empty_executor());
    let service =
        MysqlService::start_with_executor(owner, "127.0.0.1:0".parse().unwrap(), executor, catalog, 7).unwrap();
    let script = r#"
import pymysql, sys

def connect():
    return pymysql.connect(host='127.0.0.1', port=int(sys.argv[1]), user='yyds', password='',
                           database='yyds', connect_timeout=4, read_timeout=4, write_timeout=4,
                           autocommit=True)

connection = connect()
assert connection.character_set_name() == 'utf8mb4'
cursor = connection.cursor()

# 1. SimpleQuery INSERT and typed text result set
cursor.execute("INSERT INTO users VALUES (1, 'alice', 30, 1.5, 1)")
assert cursor.rowcount == 1, cursor.rowcount
cursor.execute("SELECT id, name, age, score, active FROM users WHERE id = %s", (1,))
assert [entry[0] for entry in cursor.description] == ['id', 'name', 'age', 'score', 'active']
assert [entry[1] for entry in cursor.description] == [8, 253, 8, 5, 1], [entry[1] for entry in cursor.description]
row = cursor.fetchone()
assert row == (1, 'alice', 30, 1.5, 1), row
assert isinstance(row[0], int) and isinstance(row[2], int) and isinstance(row[3], float), row

# 1b. Multi-row UPDATE and DELETE without a primary-key equality
connection.begin()
cursor.execute("INSERT INTO users VALUES (3, 'bob', 20, 0.5, 1)")
assert cursor.rowcount == 1, cursor.rowcount
cursor.execute("UPDATE users SET age = 31 WHERE active = TRUE")
assert cursor.rowcount == 2, cursor.rowcount
cursor.execute("DELETE FROM users WHERE active = TRUE")
assert cursor.rowcount == 2, cursor.rowcount
connection.rollback()
cursor.execute("SELECT id, age FROM users WHERE active = TRUE")
assert cursor.fetchall() == ((1, 30),)

# 3. Client-side parameter binding keeps text and NULL distinct
cursor.execute("SELECT name FROM users WHERE name = %s", ('alice',))
assert cursor.fetchall() == (('alice',),)
cursor.execute("SELECT id FROM users WHERE name = %s", (None,))
assert cursor.fetchall() == ()

# 3. Text with a leading zero must not be coerced to an integer
cursor.execute("INSERT INTO users VALUES (2, '007', 0, 0.25, 0)")
cursor.execute("SELECT name, age FROM users WHERE id = %s", (2,))
assert cursor.fetchone() == ('007', 0)

# 4. Rollback discards buffered writes and commit persists them
connection.begin()
cursor.execute("INSERT INTO users VALUES (3, 'temp', 5, 0.0, 1)")
connection.rollback()
cursor.execute("SELECT id FROM users WHERE id = %s", (3,))
assert cursor.fetchall() == ()
connection.begin()
cursor.execute("INSERT INTO users VALUES (4, 'kept', 5, 0.0, 1)")
connection.commit()
cursor.execute("SELECT id FROM users WHERE id = %s", (4,))
assert cursor.fetchall() == ((4,),)

# 5. Error responses carry the gateway's MySQL error codes
try:
    cursor.execute("SELECT id FROM missing")
except pymysql.err.ProgrammingError as error:
    assert error.args[0] == 1146, error.args
else:
    raise AssertionError('missing table unexpectedly resolved')
try:
    cursor.execute("SELECT nope FROM users")
except pymysql.err.Error as error:
    assert error.args[0] == 1054, error.args
else:
    raise AssertionError('unknown column unexpectedly resolved')
try:
    cursor.execute("SELECT (")
except pymysql.err.ProgrammingError as error:
    assert error.args[0] == 1064, error.args
else:
    raise AssertionError('malformed SQL unexpectedly passed Oak parsing')
try:
    cursor.execute("INSERT INTO users VALUES (9, 42, 0, 0.0, 1)")
except pymysql.err.Error as error:
    assert error.args[0] == 1366, error.args
else:
    raise AssertionError('integer write into a text column unexpectedly passed')
try:
    cursor.execute("INSERT INTO users VALUES (NULL, 'x', 0, 0.0, 1)")
except pymysql.err.Error as error:
    assert error.args[0] == 1048, error.args
else:
    raise AssertionError('NULL primary key unexpectedly passed')
try:
    cursor.execute("INSERT INTO users VALUES (1, 'dup', 0, 0.0, 1)")
except pymysql.err.Error as error:
    assert error.args[0] == 1062, error.args
else:
    raise AssertionError('duplicate primary key unexpectedly passed')

# 6. Concurrent read-modify-write must surface a serialization failure at COMMIT
other = connect()
other_cursor = other.cursor()
connection.begin()
cursor.execute("UPDATE users SET age = 41 WHERE id = %s", (1,))
other.begin()
other_cursor.execute("UPDATE users SET age = 42 WHERE id = %s", (1,))
other.commit()
try:
    connection.commit()
except pymysql.err.Error as error:
    assert error.args[0] == 1213, error.args
else:
    raise AssertionError('stale transaction commit unexpectedly succeeded')
cursor.execute("SELECT age FROM users WHERE id = %s", (1,))
assert cursor.fetchone() == (42,)

other.close()
connection.close()
print('PyMySQL typed results, parameters, transactions and conflicts passed')
"#;
    run_reference_client(script, service.address().port());
    service.stop().unwrap();
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}
