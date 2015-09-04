use std::{
    fs,
    io::{BufRead, Read, Write},
    net::{Shutdown, SocketAddr, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn directory() -> std::path::PathBuf {
    static SEQUENCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    std::env::temp_dir().join(format!(
        "yyds-cli-{}-{}-{}",
        std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        SEQUENCE.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
    ))
}

fn command(directory: &std::path::Path, action: &str) -> std::process::Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_yyds"));
    command.args([action, "--data-dir"]).arg(directory);
    if matches!(action, "init" | "start") {
        command.args(["--cluster-id", "cluster-a", "--node-id", "node-a"]);
    }
    command.output().unwrap()
}

#[test]
fn init_status_and_stop_contract_is_explicit() {
    let directory = directory();
    let initialized = command(&directory, "init");
    assert!(initialized.status.success(), "{}", String::from_utf8_lossy(&initialized.stderr));
    let status = command(&directory, "status");
    assert!(status.status.success(), "{}", String::from_utf8_lossy(&status.stderr));
    let body = String::from_utf8(status.stdout).unwrap();
    assert!(body.contains("\"available\""));
    assert!(directory.join("local-0.yykv").is_file());
    let stopped = command(&directory, "stop");
    assert!(!stopped.status.success());
    assert!(fs::read(directory.join("node.lock")).is_ok());
}

#[test]
fn init_rejects_identity_reassignment() {
    let directory = directory();
    assert!(command(&directory, "init").status.success());
    let output = Command::new(env!("CARGO_BIN_EXE_yyds"))
        .args(["init", "--data-dir"])
        .arg(&directory)
        .args(["--cluster-id", "other", "--node-id", "node-a"])
        .output()
        .unwrap();
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("identity mismatch"));
}

#[test]
fn start_status_stop_and_restart_are_one_node_lifecycle() {
    let directory = directory();
    assert!(command(&directory, "init").status.success());
    let child = spawn_start(&directory);
    wait_for(&directory, |body| body.contains("\"held\"") && body.contains("\"redisAddress\""));
    let status = command(&directory, "status");
    assert!(status.status.success(), "{}", String::from_utf8_lossy(&status.stderr));
    let body: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(body["health"], "local-ownership-only");
    let address = body["runtime"]["redisAddress"].as_str().unwrap();
    assert!(std::net::TcpStream::connect(address).is_ok());
    let address = address.parse().unwrap();
    let pgsql_address: SocketAddr = body["runtime"]["pgsqlAddress"].as_str().unwrap().parse().unwrap();
    let mut pgsql = pgsql_connect(pgsql_address);
    pgsql.write_all(b"Q\0\0").unwrap();
    assert_eq!(redis_exchange(address, &[b"SET", b"persisted", b"\0\xff"]), b"+OK\r\n");
    let stopped = command(&directory, "stop");
    assert!(stopped.status.success(), "{}", String::from_utf8_lossy(&stopped.stderr));
    assert!(child.wait_with_output().unwrap().status.success());
    assert!(TcpStream::connect_timeout(&pgsql_address, Duration::from_secs(1)).is_err());
    let mut probe = [0];
    assert!(!matches!(pgsql.read(&mut probe), Ok(count) if count > 0));
    wait_for(&directory, |body| body.contains("\"available\"") && body.contains("\"runtime\":null"));

    let child = spawn_start(&directory);
    wait_for(&directory, |body| body.contains("\"held\""));
    let status = command(&directory, "status");
    let body: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    let address = body["runtime"]["redisAddress"].as_str().unwrap().parse().unwrap();
    assert_eq!(redis_exchange(address, &[b"GET", b"persisted"]), b"$2\r\n\0\xff\r\n");
    let again = command(&directory, "stop");
    assert!(again.status.success(), "{}", String::from_utf8_lossy(&again.stderr));
    assert!(child.wait_with_output().unwrap().status.success());
}

#[test]
fn pgsql_bind_failure_rolls_back_node_start() {
    let directory = directory();
    assert!(command(&directory, "init").status.success());
    let occupied = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = occupied.local_addr().unwrap().port().to_string();
    let output = Command::new(env!("CARGO_BIN_EXE_yyds"))
        .args(["start", "--data-dir"])
        .arg(&directory)
        .args(["--cluster-id", "cluster-a", "--node-id", "node-a", "--redis-port", "0", "--pgsql-port", &port])
        .output()
        .unwrap();
    assert!(!output.status.success());
    let status = command(&directory, "status");
    assert!(status.status.success());
    let body: serde_json::Value = serde_json::from_slice(&status.stdout).unwrap();
    assert_eq!(body["ownership"], "available");
    assert!(body["runtime"].is_null());
    assert!(!directory.join("node.status.json").exists());
}

fn redis_exchange(address: SocketAddr, arguments: &[&[u8]]) -> Vec<u8> {
    let mut request = Vec::new();
    write!(request, "*{}\r\n", arguments.len()).unwrap();
    for argument in arguments {
        write!(request, "${}\r\n", argument.len()).unwrap();
        request.extend_from_slice(argument);
        request.extend_from_slice(b"\r\n");
    }
    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    client.write_all(&request).unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    response
}

fn pgsql_connect(address: SocketAddr) -> TcpStream {
    let mut client = TcpStream::connect_timeout(&address, Duration::from_secs(3)).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let payload = b"user\0yyds\0database\0yyds\0\0";
    client.write_all(&((payload.len() + 8) as u32).to_be_bytes()).unwrap();
    client.write_all(&196608_u32.to_be_bytes()).unwrap();
    client.write_all(payload).unwrap();
    loop {
        let mut header = [0; 5];
        client.read_exact(&mut header).unwrap();
        let length = u32::from_be_bytes(header[1..].try_into().unwrap()) as usize;
        assert!((4..=4096).contains(&length));
        let mut response = vec![0; length - 4];
        client.read_exact(&mut response).unwrap();
        assert_ne!(header[0], b'E');
        if header[0] == b'Z' {
            assert_eq!(response, b"I");
            return client;
        }
    }
}

fn spawn_start(directory: &std::path::Path) -> Child {
    let mut child = Command::new(env!("CARGO_BIN_EXE_yyds"))
        .args(["start", "--data-dir"])
        .arg(directory)
        .args(["--cluster-id", "cluster-a", "--node-id", "node-a", "--redis-port", "0", "--pgsql-port", "0"])
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit())
        .spawn()
        .unwrap();
    let output = child.stdout.take().unwrap();
    let (sender, receiver) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        for line in std::io::BufReader::new(output).lines().map_while(Result::ok) {
            if line.contains("yyds running") {
                let _ = sender.send(());
                break;
            }
        }
    });
    receiver.recv_timeout(Duration::from_secs(10)).expect("bounded start readiness log");
    child
}

fn wait_for(directory: &std::path::Path, predicate: impl Fn(&str) -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    loop {
        let output = command(directory, "status");
        if output.status.success() && predicate(&String::from_utf8_lossy(&output.stdout)) {
            return;
        }
        assert!(Instant::now() < deadline, "timed out waiting for node status");
        thread::sleep(Duration::from_millis(20));
    }
}
