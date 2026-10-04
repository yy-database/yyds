use std::{
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use yyds_control::{LocalOwnership, NodeIdentity, NodeLease};
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

#[test]
#[ignore = "requires YYDS_MYSQL_REFERENCE_PYTHON with PyMySQL installed"]
fn pymysql_connects_and_receives_explicit_unsupported_query_error() {
    let python = std::env::var_os("YYDS_MYSQL_REFERENCE_PYTHON").expect("reference Python executable");
    let directory = owner_directory();
    let owner =
        Arc::new(NodeLease::acquire(&directory, NodeIdentity::new("reference-cluster", "reference-node").unwrap()).unwrap());
    let service = MysqlService::start(owner, "127.0.0.1:0".parse().unwrap()).unwrap();
    let port = service.address().port();
    let script = r#"
import pymysql, sys
# Keep the probe on wire startup and the requested query, without disguising
# PyMySQL's implicit SET NAMES initialization as supported SQL execution.
pymysql.connections.Connection.set_character_set = lambda self, charset, collation=None: None
connection = pymysql.connect(host='127.0.0.1', port=int(sys.argv[1]), user='yyds',
                             password='', database='yyds', connect_timeout=4,
                             read_timeout=4, write_timeout=4, autocommit=None)
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
    let mut child = Command::new(python)
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
    service.stop().unwrap();
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}
