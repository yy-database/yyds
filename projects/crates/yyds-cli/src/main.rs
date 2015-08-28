use std::{
    fs::{self, File, OpenOptions},
    io::{self, Read},
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use yyds_control::{LocalOwnership, NodeIdentity, NodeLease};
use yyds_gateway_redis::{DEFAULT_PORT, resp::RequestLimits, service::RedisService};

const STATUS_FILE: &str = "node.status.json";
const CONTROL_VERSION: u32 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Command {
    Init,
    Start,
    Status,
    Stop,
}

#[derive(Debug)]
struct Arguments {
    command: Command,
    data_dir: PathBuf,
    cluster_id: Option<String>,
    node_id: Option<String>,
    redis_port: u16,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RuntimeStatus {
    version: u32,
    generation: uuid::Uuid,
    pid: u32,
    started_at_unix_ms: u128,
    redis_address: SocketAddr,
}

fn main() {
    if let Err(error) = run(std::env::args().skip(1).collect()) {
        eprintln!("yyds: {error}");
        std::process::exit(1);
    }
}

fn run(arguments: Vec<String>) -> Result<(), String> {
    let parsed = parse_arguments(&arguments)?;
    match parsed.command {
        Command::Init => init(&parsed),
        Command::Start => start(&parsed),
        Command::Status => status(&parsed),
        Command::Stop => stop(&parsed),
    }
}

fn parse_arguments(arguments: &[String]) -> Result<Arguments, String> {
    let command = match arguments.first().map(String::as_str) {
        Some("init") => Command::Init,
        Some("start") => Command::Start,
        Some("status") => Command::Status,
        Some("stop") => Command::Stop,
        Some(value) => return Err(format!("unknown command `{value}`")),
        None => return Err(usage()),
    };
    let mut data_dir = None;
    let mut cluster_id = None;
    let mut node_id = None;
    let mut redis_port = DEFAULT_PORT;
    let mut index = 1;
    let mut seen = std::collections::HashSet::new();
    while index < arguments.len() {
        let flag = arguments[index].as_str();
        if !seen.insert(flag) {
            return Err(format!("duplicate option `{flag}`"));
        }
        if command != Command::Start && flag == "--redis-port"
            || matches!(command, Command::Status | Command::Stop) && matches!(flag, "--cluster-id" | "--node-id")
        {
            return Err(format!("option `{flag}` is not valid for this command"));
        }
        let value = arguments.get(index + 1).ok_or_else(usage)?;
        if value.is_empty() || value.starts_with("--") {
            return Err(format!("missing value for `{flag}`"));
        }
        match flag {
            "--data-dir" => data_dir = Some(PathBuf::from(value)),
            "--cluster-id" => cluster_id = Some(value.clone()),
            "--node-id" => node_id = Some(value.clone()),
            "--redis-port" => redis_port = value.parse().map_err(|_| "invalid --redis-port".to_string())?,
            _ => return Err(format!("unknown option `{flag}`\n{}", usage())),
        }
        index += 2;
    }
    Ok(Arguments { command, data_dir: data_dir.ok_or_else(usage)?, cluster_id, node_id, redis_port })
}

fn usage() -> String {
    "usage: yyds <init|start|status|stop> --data-dir DIR [--cluster-id ID --node-id ID] [--redis-port PORT]".into()
}

fn identity(arguments: &Arguments) -> Result<NodeIdentity, String> {
    NodeIdentity::new(
        arguments.cluster_id.clone().ok_or("--cluster-id is required")?,
        arguments.node_id.clone().ok_or("--node-id is required")?,
    )
    .map_err(|error| error.to_string())
}

fn init(arguments: &Arguments) -> Result<(), String> {
    let lease = NodeLease::acquire(&arguments.data_dir, identity(arguments)?).map_err(|error| error.to_string())?;
    println!("initialized {}", lease.directory().display());
    Ok(())
}

fn start(arguments: &Arguments) -> Result<(), String> {
    let lease = Arc::new(NodeLease::acquire(&arguments.data_dir, identity(arguments)?).map_err(|error| error.to_string())?);
    remove_if_present(&arguments.data_dir.join(STATUS_FILE))?;
    let address = SocketAddr::from(([127, 0, 0, 1], arguments.redis_port));
    let service =
        RedisService::start(Arc::clone(&lease), address, RequestLimits::default()).map_err(|error| error.to_string())?;
    let runtime = RuntimeStatus {
        version: CONTROL_VERSION,
        generation: uuid::Uuid::new_v4(),
        pid: std::process::id(),
        started_at_unix_ms: SystemTime::now().duration_since(UNIX_EPOCH).map_err(|error| error.to_string())?.as_millis(),
        redis_address: service.address(),
    };
    write_status(&arguments.data_dir, &runtime)?;
    println!("yyds running pid={} redis={}", runtime.pid, runtime.redis_address);
    let stop_path = stop_path(&arguments.data_dir, runtime.generation);
    while !stop_path.exists() && !service.is_finished() {
        thread::sleep(Duration::from_millis(50));
    }
    let result = service.stop().map_err(|error| error.to_string());
    let _ = fs::remove_file(arguments.data_dir.join(STATUS_FILE));
    let _ = fs::remove_file(stop_path);
    result
}

fn status(arguments: &Arguments) -> Result<(), String> {
    let state = NodeLease::inspect(&arguments.data_dir).map_err(|error| error.to_string())?;
    let runtime = if state.ownership == LocalOwnership::Held { read_status(&arguments.data_dir)? } else { None };
    println!(
        "{}",
        serde_json::to_string(&serde_json::json!({
            "directory": state.directory,
            "identity": state.identity,
            "ownership": match state.ownership { LocalOwnership::Held => "held", LocalOwnership::Available => "available" },
            "runtime": runtime,
            "health": "local-ownership-only"
        }))
        .map_err(|error| error.to_string())?
    );
    Ok(())
}

fn stop(arguments: &Arguments) -> Result<(), String> {
    let status = NodeLease::inspect(&arguments.data_dir).map_err(|error| error.to_string())?;
    if status.ownership == LocalOwnership::Available {
        return Err("node is not running".into());
    }
    let runtime = read_status(&arguments.data_dir)?.ok_or("node owner has not published CLI runtime status")?;
    let path = stop_path(&arguments.data_dir, runtime.generation);
    match OpenOptions::new().write(true).create_new(true).open(path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
        Err(error) => return Err(error.to_string()),
    }
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        match NodeLease::inspect(&arguments.data_dir) {
            Ok(state) if state.ownership == LocalOwnership::Available => {
                println!("stopped");
                return Ok(());
            }
            Err(error) => return Err(error.to_string()),
            _ if std::time::Instant::now() >= deadline => return Err("timed out waiting for node stop".into()),
            _ => {
                if let Some(current) = read_status(&arguments.data_dir)? {
                    if current.generation != runtime.generation {
                        return Err("node generation changed while stopping".into());
                    }
                }
                thread::sleep(Duration::from_millis(50));
            }
        }
    }
}

fn stop_path(directory: &Path, generation: uuid::Uuid) -> PathBuf {
    directory.join(format!("node.stop.{generation}"))
}

fn remove_if_present(path: &Path) -> Result<(), String> {
    match fs::remove_file(path) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.to_string()),
    }
}

fn write_status(directory: &Path, runtime: &RuntimeStatus) -> Result<(), String> {
    let bytes = serde_json::to_vec(runtime).map_err(|error| error.to_string())?;
    let temporary = directory.join("node.status.json.tmp");
    fs::write(&temporary, bytes).map_err(|error| error.to_string())?;
    fs::rename(temporary, directory.join(STATUS_FILE)).map_err(|error| error.to_string())
}

fn read_status(directory: &Path) -> Result<Option<RuntimeStatus>, String> {
    match File::open(directory.join(STATUS_FILE)) {
        Ok(input) => {
            if !input.metadata().map_err(|error| error.to_string())?.is_file() {
                return Err("runtime status must be a regular file".into());
            }
            let mut bytes = Vec::new();
            input.take(4097).read_to_end(&mut bytes).map_err(|error| error.to_string())?;
            if bytes.len() > 4096 {
                return Err("runtime status exceeds byte limit".into());
            }
            let status: RuntimeStatus =
                serde_json::from_slice(&bytes).map_err(|error| format!("corrupt runtime status: {error}"))?;
            if status.version != CONTROL_VERSION || status.generation.is_nil() || !status.redis_address.ip().is_loopback() {
                return Err("unsupported or invalid runtime status".into());
            }
            Ok(Some(status))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.to_string()),
    }
}
