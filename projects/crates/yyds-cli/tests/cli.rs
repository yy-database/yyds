use std::{
    fs,
    io::{BufRead, Read, Write},
    net::{Shutdown, SocketAddr, TcpStream},
    process::{Child, Command, Stdio},
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

fn directory() -> std::path::PathBuf {
    std::env::temp_dir().join(format!("yyds-cli-{}", SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos()))
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
    assert_eq!(redis_exchange(address, &[b"SET", b"persisted", b"\0\xff"]), b"+OK\r\n");
    let stopped = command(&directory, "stop");
    assert!(stopped.status.success(), "{}", String::from_utf8_lossy(&stopped.stderr));
    assert!(child.wait_with_output().unwrap().status.success());
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

fn spawn_start(directory: &std::path::Path) -> Child {
    let mut child = Command::new(env!("CARGO_BIN_EXE_yyds"))
        .args(["start", "--data-dir"])
        .arg(directory)
        .args(["--cluster-id", "cluster-a", "--node-id", "node-a", "--redis-port", "0"])
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
