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

#[test]
#[ignore = "requires YYDS_PGSQL_REFERENCE_PYTHON with psycopg2 installed"]
fn psycopg2_establishes_a_protocol_v3_session() {
    let python = std::env::var_os("YYDS_PGSQL_REFERENCE_PYTHON").expect("reference Python executable");
    let directory = owner_directory();
    let owner =
        Arc::new(NodeLease::acquire(&directory, NodeIdentity::new("reference-cluster", "reference-node").unwrap()).unwrap());
    let service = PgsqlService::start(owner, "127.0.0.1:0".parse().unwrap()).unwrap();
    let port = service.address().port();
    let script = r#"
import psycopg2, sys
connection = psycopg2.connect(host='127.0.0.1', port=int(sys.argv[1]), user='yyds',
                              dbname='yyds', application_name='yyds-reference',
                              connect_timeout=4, sslmode='prefer')
assert connection.server_version >= 160000
connection.autocommit = True
try:
    connection.cursor().execute('select 1')
except psycopg2.Error as error:
    assert error.pgcode == '0A000', (error.pgcode, str(error))
else:
    raise AssertionError('unsupported SQL unexpectedly returned a result')
try:
    connection.cursor().execute('select (')
except psycopg2.Error as error:
    assert error.pgcode == '42601', (error.pgcode, str(error))
else:
    raise AssertionError('malformed SQL unexpectedly passed Oak parsing')
connection.close()
print('psycopg2 protocol-v3 startup and explicit unsupported-query response passed')
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
