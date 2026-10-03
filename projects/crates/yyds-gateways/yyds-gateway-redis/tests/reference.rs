use std::{
    process::{Command, Stdio},
    sync::Arc,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use yyds_control::{LocalOwnership, NodeIdentity, NodeLease};
use yyds_gateway_redis::{resp::RequestLimits, service::RedisService};

#[test]
#[ignore = "requires YYDS_REDIS_REFERENCE_PYTHON and the original redis Python client"]
fn original_redis_client_probes_and_pipeline() {
    let python = std::env::var_os("YYDS_REDIS_REFERENCE_PYTHON").expect("reference Python executable");
    let directory = std::env::temp_dir().join(format!(
        "yyds-redis-reference-node-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()
    ));
    let owner =
        Arc::new(NodeLease::acquire(&directory, NodeIdentity::new("reference-cluster", "reference-node").unwrap()).unwrap());
    let service = RedisService::start(owner, "127.0.0.1:0".parse().unwrap(), RequestLimits::default()).unwrap();
    let port = service.address().port();
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
    service.stop().unwrap();
    assert_eq!(NodeLease::inspect(directory).unwrap().ownership, LocalOwnership::Available);
}
