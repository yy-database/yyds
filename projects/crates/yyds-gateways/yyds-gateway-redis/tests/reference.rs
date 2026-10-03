use std::{
    net::TcpListener,
    process::{Command, Stdio},
    time::{Duration, Instant},
};

use yyds_gateway_redis::{connection::serve_connection, resp::RequestLimits};

#[test]
#[ignore = "requires YYDS_REDIS_REFERENCE_PYTHON and the original redis Python client"]
fn original_redis_client_probes_and_pipeline() {
    let python = std::env::var_os("YYDS_REDIS_REFERENCE_PYTHON").expect("reference Python executable");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    listener.set_nonblocking(true).unwrap();
    let worker = std::thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            match listener.accept() {
                Ok((stream, _)) => return serve_connection(stream, RequestLimits::default()),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock && Instant::now() < deadline => {
                    std::thread::sleep(Duration::from_millis(10));
                }
                Err(error) => return Err(error),
            }
        }
    });
    let script = r#"
import redis, sys
print('reference: imported client', file=sys.stderr, flush=True)
with redis.Redis(host='127.0.0.1', port=int(sys.argv[1]), protocol=2,
                 socket_timeout=3, socket_connect_timeout=3, decode_responses=False) as client:
    assert client.ping() is True
    print('reference: ping', file=sys.stderr, flush=True)
    payload = bytes(range(256))
    assert client.echo(payload) == payload
    print('reference: echo', file=sys.stderr, flush=True)
    with client.pipeline(transaction=False) as pipeline:
        pipeline.ping()
        pipeline.echo(b'')
        pipeline.echo(payload)
        assert pipeline.execute() == [True, b'', payload]
    print('reference: pipeline', file=sys.stderr, flush=True)
    try:
        client.get('unsupported-storage-operation')
    except redis.ResponseError as error:
        assert 'unsupported command' in str(error)
    else:
        raise AssertionError('storage command was incorrectly accepted')
    assert client.ping() is True
print(redis.__version__)
"#;
    let mut child = Command::new(python)
        .arg("-c")
        .arg(script)
        .arg(port.to_string())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(15);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() >= deadline {
            child.kill().unwrap();
            let output = child.wait_with_output().unwrap();
            panic!("reference client exceeded subprocess deadline: {}", String::from_utf8_lossy(&output.stderr));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    worker.join().unwrap().unwrap();
}
