use std::{
    net::TcpListener,
    process::{Command, Stdio},
    thread,
    time::{Duration, Instant},
};

use yyds_gateway_pgsql::connection::serve_connection;

#[test]
#[ignore = "requires YYDS_PGSQL_REFERENCE_PYTHON with psycopg2 installed"]
fn psycopg2_establishes_a_protocol_v3_session() {
    let python = std::env::var_os("YYDS_PGSQL_REFERENCE_PYTHON").expect("reference Python executable");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_connection(stream)
    });
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
    server.join().unwrap().unwrap();
}
