use std::{
    fs,
    io::BufRead,
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
    let stopped = command(&directory, "stop");
    assert!(stopped.status.success(), "{}", String::from_utf8_lossy(&stopped.stderr));
    assert!(child.wait_with_output().unwrap().status.success());
    wait_for(&directory, |body| body.contains("\"available\"") && body.contains("\"runtime\":null"));

    let child = spawn_start(&directory);
    wait_for(&directory, |body| body.contains("\"held\""));
    let again = command(&directory, "stop");
    assert!(again.status.success(), "{}", String::from_utf8_lossy(&again.stderr));
    assert!(child.wait_with_output().unwrap().status.success());
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
