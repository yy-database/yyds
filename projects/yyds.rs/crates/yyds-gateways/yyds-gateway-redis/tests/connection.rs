use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{atomic::AtomicBool, Arc},
    time::Duration,
};
use yyds_execution::{KeyValueExecutor, LocalExecutor};
use yyds_gateway_redis::{
    bind::bind,
    connection::{serve_cancellable_with_executor, serve_connection},
    resp::RequestLimits,
};
use yyds_kv::{FileShard, Key, KvStore, MemoryShard};
use yyds_types::{Namespace, ShardEpoch, ShardId, ShardMap};

fn exchange(chunks: &[&[u8]], limits: RequestLimits) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_connection(stream, limits).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    client.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    for chunk in chunks {
        client.write_all(chunk).unwrap()
    }
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    worker.join().unwrap();
    response
}

#[test]
fn fragmented_ping_and_binary_echo_pipeline() {
    assert_eq!(
        exchange(&[b"*1\r", b"\n$4\r\nping\r\n*2\r\n$4\r\nECHO\r\n$5\r\n\0\xff\r\n!\r\n",], RequestLimits::default()),
        b"+PONG\r\n$5\r\n\0\xff\r\n!\r\n"
    );
}

#[test]
fn command_errors_do_not_desynchronize_pipeline() {
    assert_eq!(
        exchange(&[b"*1\r\n$3\r\nGET\r\n*1\r\n$4\r\nECHO\r\n*1\r\n$4\r\nPING\r\n"], RequestLimits::default()),
        b"-ERR unsupported command\r\n-ERR wrong number of arguments for 'echo' command\r\n+PONG\r\n"
    );
}

#[test]
fn ping_message_is_a_bulk_reply_and_quit_closes() {
    assert_eq!(
        exchange(&[b"*2\r\n$4\r\nPING\r\n$0\r\n\r\n*1\r\n$4\r\nQUIT\r\n"], RequestLimits::default()),
        b"$0\r\n\r\n+OK\r\n"
    );
}

#[test]
fn protocol_errors_and_truncation_close_cleanly() {
    assert_eq!(exchange(&[b"*1\r\n$-1\r\n"], RequestLimits::default()), b"-ERR Protocol error or request limit exceeded\r\n");
    assert_eq!(exchange(&[b"*1\r\n$4\r\nPI"], RequestLimits::default()), b"-ERR Truncated request\r\n");
}

#[test]
fn frame_limit_applies_per_command_even_when_read_contains_pipeline() {
    let frame = b"*1\r\n$4\r\nPING\r\n";
    assert_eq!(
        exchange(&[frame, frame], RequestLimits { max_frame_bytes: frame.len(), ..RequestLimits::default() }),
        b"+PONG\r\n+PONG\r\n"
    );
}

#[test]
fn executor_backed_session_runs_binary_set_get_and_delete_pipeline() {
    let shard = ShardId("local".into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, MemoryShard::new())]),
            Vec::new(),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_cancellable_with_executor(stream, RequestLimits::default(), Arc::new(AtomicBool::new(false)), executor).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    client
        .write_all(
            b"*3\r\n$3\r\nSET\r\n$4\r\n\0key\r\n$3\r\n\0\xff!\r\n*2\r\n$6\r\nEXISTS\r\n$4\r\n\0key\r\n*2\r\n$6\r\nEXISTS\r\n$7\r\nmissing\r\n*4\r\n$3\r\nSET\r\n$4\r\n\0key\r\n$3\r\nnew\r\n$2\r\nNX\r\n*2\r\n$3\r\nGET\r\n$4\r\n\0key\r\n*2\r\n$3\r\nDEL\r\n$4\r\n\0key\r\n*2\r\n$3\r\nGET\r\n$4\r\n\0key\r\n*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n*2\r\n$3\r\nGET\r\n$7\r\ncounter\r\n*3\r\n$3\r\nSET\r\n$3\r\nbad\r\n$3\r\nabc\r\n*2\r\n$4\r\nINCR\r\n$3\r\nbad\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    worker.join().unwrap();
    assert_eq!(
        response,
        b"+OK\r\n:1\r\n:0\r\n$-1\r\n$3\r\n\0\xff!\r\n:1\r\n$-1\r\n:1\r\n$1\r\n1\r\n+OK\r\n-ERR value is not an integer or out of range\r\n"
    );
}

#[test]
fn executor_backed_mget_returns_ordered_duplicate_and_null_values() {
    let shard = ShardId("local".into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, MemoryShard::new())]),
            Vec::new(),
        )
        .unwrap(),
    );
    let namespace = Namespace("redis:0".into());
    executor.execute(None, bind(&[b"SET", b"first", b"value"], &namespace).unwrap()).unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_cancellable_with_executor(stream, RequestLimits::default(), Arc::new(AtomicBool::new(false)), executor).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    client.write_all(b"*4\r\n$4\r\nMGET\r\n$5\r\nfirst\r\n$7\r\nmissing\r\n$5\r\nfirst\r\n").unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    worker.join().unwrap();
    assert_eq!(response, b"*3\r\n$5\r\nvalue\r\n$-1\r\n$5\r\nvalue\r\n");
}

#[test]
fn select_changes_the_namespace_for_the_connection() {
    let shard = ShardId("local".into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, MemoryShard::new())]),
            Vec::new(),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let worker = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_cancellable_with_executor(stream, RequestLimits::default(), Arc::new(AtomicBool::new(false)), executor).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    client
        .write_all(
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\none\r\n*2\r\n$6\r\nSELECT\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\ntwo\r\n*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n*2\r\n$6\r\nSELECT\r\n$2\r\n16\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    worker.join().unwrap();
    assert_eq!(response, b"+OK\r\n+OK\r\n+OK\r\n+OK\r\n$3\r\none\r\n-ERR DB index is out of range\r\n");
}

fn executor_server() -> (TcpStream, std::thread::JoinHandle<()>) {
    let shard = ShardId("local".into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, MemoryShard::new())]),
            Vec::new(),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_cancellable_with_executor(stream, RequestLimits::default(), Arc::new(AtomicBool::new(false)), executor).unwrap();
    });
    let client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    (client, server)
}

/// File-backed executor so cross-key commits (rename) have a durable commit boundary.
fn file_executor_server() -> (TcpStream, std::thread::JoinHandle<()>, std::path::PathBuf) {
    let stamp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    let directory = std::env::temp_dir().join(format!("yyds-rename-{}-{stamp}", std::process::id()));
    std::fs::create_dir_all(&directory).unwrap();
    let path = directory.join("local-0.yykv");
    let shard = ShardId("local".into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, FileShard::open(&path).unwrap())]),
            Vec::new(),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = std::thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_cancellable_with_executor(stream, RequestLimits::default(), Arc::new(AtomicBool::new(false)), executor).unwrap();
    });
    let client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    (client, server, path)
}

#[test]
fn ttl_and_type_mock_for_existing_and_missing_keys() {
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n*2\r\n$3\r\nTTL\r\n$7\r\nmissing\r\n*2\r\n$4\r\nTYPE\r\n$3\r\nkey\r\n*2\r\n$4\r\nTYPE\r\n$7\r\nmissing\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    assert_eq!(response, b"+OK\r\n:-1\r\n:-2\r\n+string\r\n+none\r\n");
}

#[test]
fn strlen_returns_value_length_and_zero_for_missing() {
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nhello\r\n*2\r\n$6\r\nSTRLEN\r\n$3\r\nkey\r\n*2\r\n$6\r\nSTRLEN\r\n$7\r\nmissing\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    assert_eq!(response, b"+OK\r\n:5\r\n:0\r\n");
}

#[test]
fn append_creates_key_when_absent_and_merges_when_present() {
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*3\r\n$6\r\nAPPEND\r\n$3\r\nkey\r\n$3\r\none\r\n*3\r\n$6\r\nAPPEND\r\n$3\r\nkey\r\n$3\r\ntwo\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    assert_eq!(response, b":3\r\n:6\r\n$6\r\nonetwo\r\n");
}

#[test]
fn multi_key_exists_counts_present_and_absent() {
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*3\r\n$3\r\nSET\r\n$3\r\naaa\r\n$1\r\n1\r\n*3\r\n$3\r\nSET\r\n$3\r\nbbb\r\n$1\r\n2\r\n*4\r\n$6\r\nEXISTS\r\n$3\r\naaa\r\n$7\r\nmissing\r\n$3\r\nbbb\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    assert_eq!(response, b"+OK\r\n+OK\r\n:2\r\n");
}

// ---------- MULTI / EXEC / DISCARD ----------

#[test]
fn multi_exec_queue_and_run_basic_commands() {
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n*1\r\n$4\r\nEXEC\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    // MULTI: OK, SET: QUEUED, GET: QUEUED, EXEC: *2\r\n+OK\r\n$3\r\nbar\r\n
    assert_eq!(response, b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+OK\r\n$3\r\nbar\r\n");
}

#[test]
fn multi_exec_empty_queue_yields_empty_array() {
    let (mut client, server) = executor_server();
    client.write_all(b"*1\r\n$5\r\nMULTI\r\n*1\r\n$4\r\nEXEC\r\n").unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    assert_eq!(response, b"+OK\r\n*0\r\n");
}

#[test]
fn discard_clears_queue_and_nested_multi_is_rejected() {
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n*1\r\n$5\r\nMULTI\r\n*1\r\n$7\r\nDISCARD\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    // MULTI: OK, SET: QUEUED, MULTI: error (nested), DISCARD: OK, GET: $-1 (queue discarded)
    assert_eq!(response, b"+OK\r\n+QUEUED\r\n-ERR MULTI calls can not be nested\r\n+OK\r\n$-1\r\n");
}

#[test]
fn exec_without_multi_and_discard_without_multi_are_rejected() {
    let (mut client, server) = executor_server();
    client.write_all(b"*1\r\n$4\r\nEXEC\r\n*1\r\n$7\r\nDISCARD\r\n").unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    assert_eq!(response, b"-ERR EXEC without MULTI\r\n-ERR DISCARD without MULTI\r\n");
}

#[test]
fn multi_exec_nested_ping_echo_are_buffered_and_flattened() {
    // PING and ECHO are not executor-backed, so they are also queued and executed under EXEC.
    let (mut client, server) = executor_server();
    client
        .write_all(b"*1\r\n$5\r\nMULTI\r\n*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n*1\r\n$4\r\nEXEC\r\n")
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    assert_eq!(response, b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+PONG\r\n$5\r\nhello\r\n");
}

#[test]
fn multi_exec_command_syntax_error_is_rejected_at_queue_time() {
    // GET needs 1 argument inside MULTI — a missing argument is a syntax error and must be
    // reported immediately (no QUEUED).
    let (mut client, server) = executor_server();
    client.write_all(b"*1\r\n$5\r\nMULTI\r\n*1\r\n$3\r\nGET\r\n*1\r\n$4\r\nEXEC\r\n").unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    // GET inside MULTI with no args → bind() fails with WrongArity → immediate error, EXEC runs empty queue.
    assert_eq!(response, b"+OK\r\n-ERR wrong number of arguments\r\n*0\r\n");
}

// ---------- SET options / TTL ----------

/// 把一条命令编码成 RESP2 数组并发送。
fn send_command(client: &mut TcpStream, arguments: &[&str]) {
    let mut frame = format!("*{}\r\n", arguments.len());
    for argument in arguments {
        frame.push_str(&format!("${}\r\n{}\r\n", argument.len(), argument));
    }
    client.write_all(frame.as_bytes()).unwrap();
}

/// 读一条 CRLF 结尾的回复行（保留行内容，去掉换行）。
fn read_line(client: &mut TcpStream) -> String {
    let mut line = Vec::new();
    loop {
        let mut byte = [0u8; 1];
        client.read_exact(&mut byte).unwrap();
        if byte[0] == b'\n' {
            break;
        }
        line.push(byte[0]);
    }
    String::from_utf8(line).expect("reply line is UTF-8")
}

fn read_integer(client: &mut TcpStream) -> i64 {
    let line = read_line(client);
    line[1..].trim_end().parse().unwrap_or_else(|_| panic!("not an integer reply: {line:?}"))
}

/// 读一条 bulk 回复；`$-1` 返回 `None`，空 bulk 返回 `Some(空)`。
fn read_bulk(client: &mut TcpStream) -> Option<Vec<u8>> {
    let header = read_line(client);
    assert_eq!(header.as_bytes().first(), Some(&b'$'), "expected a bulk header, got {header:?}");
    let length: i64 = header[1..].trim_end().parse().unwrap_or_else(|_| panic!("not a bulk header: {header:?}"));
    if length < 0 {
        return None;
    }
    let mut body = vec![0_u8; length as usize];
    client.read_exact(&mut body).unwrap();
    let mut terminator = [0_u8; 2];
    client.read_exact(&mut terminator).unwrap();
    assert_eq!(terminator, [b'\r', b'\n']);
    Some(body)
}

fn finish(client: TcpStream, server: std::thread::JoinHandle<()>) {
    client.shutdown(Shutdown::Write).unwrap();
    server.join().unwrap();
}

/// `SET k v EX 5` 写入相对过期时间，`TTL` 读回近似值，到期后键消失。
#[test]
fn set_ex_sets_a_relative_ttl_and_expires_the_key() {
    let (mut client, server) = executor_server();
    send_command(&mut client, &["SET", "k1", "one", "EX", "5"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["TTL", "k1"]);
    let ttl = read_integer(&mut client);
    assert!((4..=5).contains(&ttl), "TTL right after `EX 5` must be about 5, got {ttl}");

    send_command(&mut client, &["SET", "k1", "again", "EX", "1"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    std::thread::sleep(Duration::from_millis(1_500));
    send_command(&mut client, &["GET", "k1"]);
    assert_eq!(read_bulk(&mut client), None);
    send_command(&mut client, &["TTL", "k1"]);
    assert_eq!(read_integer(&mut client), -2);
    send_command(&mut client, &["EXISTS", "k1"]);
    assert_eq!(read_integer(&mut client), 0);

    finish(client, server);
}

/// `SET NX` / `SET XX` 的条件失败返回 nil，既不写入也不改动 TTL。
#[test]
fn set_nx_and_xx_conditions_control_the_write_and_keep_the_ttl() {
    let (mut client, server) = executor_server();
    send_command(&mut client, &["SET", "k2", "fresh", "NX"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["SET", "k2", "other", "NX"]);
    assert_eq!(read_bulk(&mut client), None);
    send_command(&mut client, &["GET", "k2"]);
    assert_eq!(read_bulk(&mut client), Some(b"fresh".to_vec()));

    send_command(&mut client, &["SET", "missing", "value", "XX"]);
    assert_eq!(read_bulk(&mut client), None);
    send_command(&mut client, &["SET", "k2", "updated", "XX"]);
    assert_eq!(read_line(&mut client), "+OK\r");

    // 条件失败不得清除已存在的过期时间。
    send_command(&mut client, &["SET", "k3", "v", "EX", "5"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["TTL", "k3"]);
    assert!((4..=5).contains(&read_integer(&mut client)));
    send_command(&mut client, &["SET", "k3", "nope", "NX"]);
    assert_eq!(read_bulk(&mut client), None);
    send_command(&mut client, &["TTL", "k3"]);
    let ttl = read_integer(&mut client);
    assert!((0..=5).contains(&ttl), "a failed `NX` must leave the TTL untouched, got {ttl}");

    finish(client, server);
}

/// `KEEPTTL` 保留旧过期时间，无 TTL 选项的覆盖写与 `GETSET` / `DEL` 清除它。
#[test]
fn set_keep_ttl_and_plain_overwrite_follow_expiry_rules() {
    let (mut client, server) = executor_server();
    send_command(&mut client, &["SET", "k4", "v", "EX", "5"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["TTL", "k4"]);
    assert!((4..=5).contains(&read_integer(&mut client)));

    send_command(&mut client, &["SET", "k4", "v2", "KEEPTTL"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["TTL", "k4"]);
    let kept = read_integer(&mut client);
    assert!((0..=5).contains(&kept), "`KEEPTTL` must keep the expiry, got {kept}");

    send_command(&mut client, &["SET", "k4", "v3"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["TTL", "k4"]);
    assert_eq!(read_integer(&mut client), -1, "an overwrite without TTL options must clear the expiry");

    // `GETSET` 同样清除 TTL。
    send_command(&mut client, &["SET", "k5", "v", "EX", "30"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["GETSET", "k5", "replaced"]);
    assert_eq!(read_bulk(&mut client), Some(b"v".to_vec()));
    send_command(&mut client, &["TTL", "k5"]);
    assert_eq!(read_integer(&mut client), -1);

    // `DEL` 之后 `KEEPTTL` 无过期时间可保留，证明 DEL 清除了过期表记录。
    send_command(&mut client, &["SET", "k6", "v", "EX", "30"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["DEL", "k6"]);
    assert_eq!(read_integer(&mut client), 1);
    send_command(&mut client, &["SET", "k6", "v", "KEEPTTL"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["TTL", "k6"]);
    assert_eq!(read_integer(&mut client), -1);

    finish(client, server);
}

/// 非法 `SET` 选项按 Redis 错误拒绝，且每条命令仍是单包单响应。
#[test]
fn invalid_set_options_report_redis_errors_and_keep_the_stream_aligned() {
    let (mut client, server) = executor_server();
    let cases: &[(&[&str], &str)] = &[
        (&["SET", "bad", "v", "NX", "XX"], "-ERR syntax error\r"),
        (&["SET", "bad", "v", "EX"], "-ERR syntax error\r"),
        (&["SET", "bad", "v", "EX", "abc"], "-ERR value is not an integer or out of range\r"),
        (&["SET", "bad", "v", "EX", "0"], "-ERR invalid expire time in 'set' command\r"),
        (&["SET", "bad", "v", "EX", "-1"], "-ERR invalid expire time in 'set' command\r"),
        (&["SET", "bad", "v", "EX", "1", "KEEPTTL"], "-ERR syntax error\r"),
        (&["SET", "bad", "v", "EXAT", "0"], "-ERR invalid expire time in 'set' command\r"),
        (&["SET", "bad", "v", "PXAT", "abc"], "-ERR value is not an integer or out of range\r"),
        (&["SET", "bad", "v", "EX", "1", "EXAT", "1"], "-ERR syntax error\r"),
    ];
    for (arguments, expected) in cases {
        send_command(&mut client, arguments);
        assert_eq!(read_line(&mut client), *expected, "unexpected reply for {arguments:?}");
    }
    // 失败的命令没有留下写入，后续命令仍正常解析。
    send_command(&mut client, &["GET", "bad"]);
    assert_eq!(read_bulk(&mut client), None);
    send_command(&mut client, &["PING"]);
    assert_eq!(read_line(&mut client), "+PONG\r");

    finish(client, server);
}

/// `CLIENT SETINFO` / `SETNAME` / `GETNAME` 是连接池握手用到的最小集合。
#[test]
fn client_setname_getname_and_setinfo_are_session_local() {
    let (mut client, server) = executor_server();
    send_command(&mut client, &["CLIENT", "GETNAME"]);
    assert_eq!(read_bulk(&mut client), Some(Vec::new()), "an unset name is an empty bulk string");

    send_command(&mut client, &["CLIENT", "SETNAME", "pool-1"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["CLIENT", "GETNAME"]);
    assert_eq!(read_bulk(&mut client), Some(b"pool-1".to_vec()));

    send_command(&mut client, &["CLIENT", "SETINFO", "LIB-NAME", "redis-py"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["CLIENT", "SETINFO", "LIB-VER", "5.0.1"]);
    assert_eq!(read_line(&mut client), "+OK\r");

    // 未知属性与其他子命令继续显式拒绝。
    send_command(&mut client, &["CLIENT", "SETINFO", "FOO", "x"]);
    assert_eq!(read_line(&mut client), "-ERR Unrecognized option 'FOO'\r");
    send_command(&mut client, &["CLIENT", "ID"]);
    assert_eq!(read_line(&mut client), "-ERR Unknown subcommand or wrong number of arguments for 'ID'. Try CLIENT HELP.\r");

    // `SETINFO` 不覆盖名字，`GETNAME` 仍返回 SETNAME 设置的值。
    send_command(&mut client, &["CLIENT", "GETNAME"]);
    assert_eq!(read_bulk(&mut client), Some(b"pool-1".to_vec()));
    send_command(&mut client, &["PING"]);
    assert_eq!(read_line(&mut client), "+PONG\r");

    finish(client, server);
}

// ---------- EXPIRE / TTL / PTTL ----------

#[test]
fn expire_sets_ttl_and_ttl_reports_countdown() {
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$2\r\n10\r\n*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    // SET +OK, EXPIRE :1, TTL :<n> where n is between 5 and 10.
    assert!(response.starts_with(b"+OK\r\n:1\r\n:"), "unexpected response: {response:?}");
    // Find the third line (TTL response).
    let lines: Vec<&[u8]> = response.split(|&b| b == b'\n').collect();
    assert_eq!(lines.len(), 4); // "+OK\r" ":1\r" ":<n>\r" ""
    let ttl_line = lines[2]; // e.g. ":9\r"
    assert!(ttl_line.starts_with(b":"));
    let digits = &ttl_line[1..ttl_line.iter().position(|&b| b == b'\r').unwrap()];
    let ttl_value: i64 = std::str::from_utf8(digits).unwrap().parse().unwrap();
    assert!((5..=10).contains(&ttl_value), "TTL out of expected range: {ttl_value}");
}

#[test]
fn ttl_returns_neg1_for_no_expiry_and_neg2_for_absent_key() {
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n*2\r\n$3\r\nTTL\r\n$7\r\nmissing\r\n*2\r\n$4\r\nPTTL\r\n$3\r\nkey\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    assert_eq!(response, b"+OK\r\n:-1\r\n:-2\r\n:-1\r\n");
}

#[test]
fn expire_zero_deletes_key() {
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$1\r\n0\r\n*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    assert_eq!(response, b"+OK\r\n:1\r\n$-1\r\n");
}

#[test]
fn expired_key_is_removed_on_access() {
    let (mut client, server) = executor_server();
    client.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$1\r\n1\r\n").unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));
    client
        .write_all(b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n*2\r\n$6\r\nEXISTS\r\n$3\r\nkey\r\n")
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    // GET after expiry → $-1, TTL → -2, EXISTS → 0
    assert_eq!(response, b"+OK\r\n:1\r\n$-1\r\n:-2\r\n:0\r\n");
}

// ---------- MULTI/EXEC conformance: queue runtime errors ----------

#[test]
fn multi_exec_queue_runtime_wrongtype_is_reported_in_result_array() {
    // SET a string value, then INCR it in the same transaction. The INCR cannot parse
    // "hello" as an integer, so executor returns an error. Redis semantics: per-command
    // runtime errors land in the EXEC result array, not at queue time.
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$3\r\nnum\r\n$5\r\nhello\r\n*2\r\n$4\r\nINCR\r\n$3\r\nnum\r\n*1\r\n$4\r\nEXEC\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    // MULTI: OK, SET: QUEUED, INCR: QUEUED, EXEC: *2 elements
    assert!(response.starts_with(b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n"), "response={response:?}");
    // First element inside EXEC result: +OK (SET succeeded)
    // Second element: an error reply starting with -ERR (INCR failed on non-integer)
    let exec_body = &response[response.windows(4).position(|w| w == b"*2\r\n").unwrap() + 4..];
    assert!(exec_body.starts_with(b"+OK\r\n"), "first result must be SET OK: {exec_body:?}");
    let after_ok = &exec_body[5..]; // skip "+OK\r\n" which is 5 bytes
    assert!(after_ok.starts_with(b"-ERR "), "second result must be an error: {after_ok:?}");
}

// ---------- MULTI/EXEC conformance: queue syntax errors ----------

#[test]
fn multi_exec_unknown_command_is_rejected_at_queue_time() {
    // A completely unknown command (no bind() match, not whitelisted in
    // validate_command_for_queue) must be rejected at QUEUE time with an
    // immediate error — it never enters the queue and EXEC sees an empty
    // queue. This differs from real Redis where unknown commands go in the
    // queue but EXEC aborts the whole transaction.
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*1\r\n$5\r\nMULTI\r\n*1\r\n$6\r\nFOOBAR\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n*1\r\n$4\r\nEXEC\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    assert!(
        response.starts_with(b"+OK\r\n-ERR unsupported command"),
        "unknown command must be rejected at queue time: {response:?}"
    );
    // SET got queued successfully, EXEC returns a 1-element array
    assert!(response.ends_with(b"\r\n*1\r\n+OK\r\n"), "SET should still run: {response:?}");
}

#[test]
fn multi_exec_mixed_syntax_error_and_good_command_queue_independent() {
    // First command has wrong arity inside MULTI → immediate error.
    // Second valid command still goes QUEUED (not blocked by the first error).
    // EXEC only runs the valid queued commands.
    let (mut client, server) = executor_server();
    client
        .write_all(b"*1\r\n$5\r\nMULTI\r\n*1\r\n$3\r\nGET\r\n*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$3\r\nval\r\n*1\r\n$4\r\nEXEC\r\n")
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    assert_eq!(response, b"+OK\r\n-ERR wrong number of arguments\r\n+QUEUED\r\n*1\r\n+OK\r\n");
}

// ---------- MULTI/EXEC conformance: session isolation ----------

#[test]
fn multi_exec_session_isolation_two_clients_does_not_leak_state() {
    // Client A opens MULTI and queues commands but never EXECs.
    // Client B (on a separate connection) still runs normal commands
    // as if nothing happened. Per-ConnectionState must not be shared.
    let shard = ShardId("local".into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, MemoryShard::new())]),
            Vec::new(),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let stopping = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let stop_b = Arc::clone(&stopping);
    let exec_a = Arc::clone(&executor);

    let server = std::thread::spawn(move || {
        while !stop_b.load(std::sync::atomic::Ordering::Acquire) {
            match listener.accept() {
                Ok((stream, _)) => {
                    let cancel = Arc::clone(&stop_b);
                    let exec_clone = Arc::clone(&exec_a);
                    std::thread::spawn(move || {
                        let _ = serve_cancellable_with_executor(stream, RequestLimits::default(), cancel, exec_clone);
                    });
                }
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(_) => break,
            }
        }
    });

    // Client A: MULTI → +OK
    let mut client_a = TcpStream::connect(address).unwrap();
    client_a.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    client_a.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
    client_a.write_all(b"*1\r\n$5\r\nMULTI\r\n").unwrap();
    client_a.flush().unwrap();
    let mut a_buf = [0u8; 5];
    client_a.read_exact(&mut a_buf).unwrap();
    assert_eq!(&a_buf, b"+OK\r\n", "A must enter MULTI");

    // Client A: SET foo bar → +QUEUED
    client_a.write_all(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n").unwrap();
    client_a.flush().unwrap();
    let mut a_q = [0u8; 9];
    client_a.read_exact(&mut a_q).unwrap();
    assert_eq!(&a_q, b"+QUEUED\r\n", "A's SET must queue");

    // Client B: PING — must get PONG immediately, unaffected by A's MULTI
    let mut client_b = TcpStream::connect(address).unwrap();
    client_b.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    client_b.set_write_timeout(Some(Duration::from_secs(3))).unwrap();
    client_b.write_all(b"*1\r\n$4\r\nPING\r\n").unwrap();
    client_b.flush().unwrap();
    let mut b_buf = [0u8; 7];
    client_b.read_exact(&mut b_buf).unwrap();
    assert_eq!(&b_buf, b"+PONG\r\n", "B's PING must be answered directly");

    // Client A does DISCARD, then GET foo → $-1 (SET was never flushed)
    client_a.write_all(b"*1\r\n$7\r\nDISCARD\r\n*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n").unwrap();
    client_a.shutdown(Shutdown::Write).unwrap();
    let mut a_rest = Vec::new();
    client_a.read_to_end(&mut a_rest).unwrap();
    assert_eq!(a_rest, b"+OK\r\n$-1\r\n");

    stopping.store(true, std::sync::atomic::Ordering::Release);
    drop(client_a);
    drop(client_b);
    server.join().unwrap();
}

// ---------- MULTI/EXEC conformance: EXPIRE inside transaction ----------

#[test]
fn multi_exec_queue_set_and_expire_executes_expiry_correctly() {
    // SET key value + EXPIRE key 2 inside MULTI → EXEC → GET returns value,
    // TTL reports positive remaining time.
    // NOTE: We deliberately do NOT test expiry-firing-after-wait here, that
    // requires wall-clock sleep and would make the suite slow. The TTL value
    // read right after EXEC already proves EXPIRE was correctly queued and
    // executed under the transaction.
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*1\r\n$5\r\nMULTI\r\n*3\r\n$3\r\nSET\r\n$2\r\nk1\r\n$5\r\nhello\r\n*3\r\n$6\r\nEXPIRE\r\n$2\r\nk1\r\n$1\r\n2\r\n*1\r\n$4\r\nEXEC\r\n*2\r\n$3\r\nGET\r\n$2\r\nk1\r\n*2\r\n$3\r\nTTL\r\n$2\r\nk1\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();

    // MULTI: OK, SET: QUEUED, EXPIRE: QUEUED
    assert!(response.starts_with(b"+OK\r\n+QUEUED\r\n+QUEUED\r\n"), "queue phase mismatch: {response:?}");
    // EXEC must contain 2 results: +OK for SET, :1 for EXPIRE
    assert!(response.windows(13).any(|w| w == b"*2\r\n+OK\r\n:1\r\n"), "EXEC results must be [OK, 1]: {response:?}");
    // GET after EXEC → $5\r\nhello\r\n
    assert!(response.windows(11).any(|w| w == b"$5\r\nhello\r\n"), "GET after EXEC must return value: {response:?}");
    // Response must end with a TTL reply starting with ":"
    assert!(response.ends_with(b"\r\n") && response.len() > 5, "response too short: {response:?}");
    let ttl_start = response.iter().rposition(|&b| b == b':').expect("TTL reply must contain ':'");
    let ttl_end = response.iter().rposition(|&b| b == b'\r').expect("TTL reply must end with \\r");
    let ttl_bytes = &response[ttl_start + 1..ttl_end];
    let ttl_val: i64 = std::str::from_utf8(ttl_bytes).unwrap().parse().unwrap();
    assert!((0..=2).contains(&ttl_val), "TTL out of range: {ttl_val}");
}

// ---------- record-level expiry (shared across connections) ----------

/// 启动一个 listener：每条连接共享同一个执行器，过期时间随 KV 记录保存。
fn shared_expiry_server() -> (std::net::SocketAddr, Arc<AtomicBool>, std::thread::JoinHandle<()>) {
    let shard = ShardId("local".into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
            HashMap::from([(shard, MemoryShard::new())]),
            Vec::new(),
        )
        .unwrap(),
    );
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    listener.set_nonblocking(true).unwrap();
    let stopping = Arc::new(AtomicBool::new(false));
    let server = {
        let stopping = Arc::clone(&stopping);
        std::thread::spawn(move || {
            while !stopping.load(std::sync::atomic::Ordering::Acquire) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let cancel = Arc::clone(&stopping);
                        let executor = Arc::clone(&executor);
                        std::thread::spawn(move || {
                            let _ = serve_cancellable_with_executor(stream, RequestLimits::default(), cancel, executor);
                        });
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        std::thread::sleep(Duration::from_millis(5));
                    }
                    Err(_) => break,
                }
            }
        })
    };
    (address, stopping, server)
}

fn connect(address: std::net::SocketAddr) -> TcpStream {
    let client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(5))).unwrap();
    client.set_write_timeout(Some(Duration::from_secs(5))).unwrap();
    client
}

fn stop_shared_server(stopping: &Arc<AtomicBool>, server: std::thread::JoinHandle<()>) {
    stopping.store(true, std::sync::atomic::Ordering::Release);
    server.join().unwrap();
}

/// 连接 A 写入的 TTL 对同一 listener 上的连接 B 可见；B `DEL` 之后 A 读到 `-2`。
#[test]
fn ttl_is_shared_between_two_connections_on_one_listener() {
    let (address, stopping, server) = shared_expiry_server();

    let mut client_a = connect(address);
    send_command(&mut client_a, &["SET", "shared", "v", "EX", "100"]);
    assert_eq!(read_line(&mut client_a), "+OK\r");

    let mut client_b = connect(address);
    send_command(&mut client_b, &["TTL", "shared"]);
    let ttl = read_integer(&mut client_b);
    assert!((95..=100).contains(&ttl), "connection B must observe the TTL set by connection A, got {ttl}");

    send_command(&mut client_b, &["DEL", "shared"]);
    assert_eq!(read_integer(&mut client_b), 1);

    send_command(&mut client_a, &["TTL", "shared"]);
    assert_eq!(read_integer(&mut client_a), -2, "B's DEL must drop the key for A as well");

    drop(client_a);
    drop(client_b);
    stop_shared_server(&stopping, server);
}

/// 写入连接断开后，另一条连接仍能看到同一键的 TTL，证明过期表不是会话作用域。
#[test]
fn ttl_survives_the_disconnect_of_the_setting_connection() {
    let (address, stopping, server) = shared_expiry_server();

    let mut client_a = connect(address);
    send_command(&mut client_a, &["SET", "ghost", "v", "EX", "100"]);
    assert_eq!(read_line(&mut client_a), "+OK\r");
    drop(client_a);
    std::thread::sleep(Duration::from_millis(100));

    let mut client_b = connect(address);
    send_command(&mut client_b, &["TTL", "ghost"]);
    let ttl = read_integer(&mut client_b);
    assert!((95..=100).contains(&ttl), "the TTL must outlive the connection that set it, got {ttl}");
    send_command(&mut client_b, &["GET", "ghost"]);
    assert_eq!(read_bulk(&mut client_b), Some(b"v".to_vec()));

    drop(client_b);
    stop_shared_server(&stopping, server);
}

// ---------- PERSIST / SETEX / APPEND expiry rules ----------

/// `PERSIST` 清除过期时间；对无过期或不存在的键返回 `0`。
#[test]
fn persist_clears_the_expiry_of_a_key() {
    let (mut client, server) = executor_server();
    send_command(&mut client, &["SET", "p1", "v", "EX", "100"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["PERSIST", "p1"]);
    assert_eq!(read_integer(&mut client), 1);
    send_command(&mut client, &["TTL", "p1"]);
    assert_eq!(read_integer(&mut client), -1);
    send_command(&mut client, &["PERSIST", "p1"]);
    assert_eq!(read_integer(&mut client), 0);
    send_command(&mut client, &["PERSIST", "missing"]);
    assert_eq!(read_integer(&mut client), 0);
    finish(client, server);
}

/// `SETEX` 等价 `SET` + `EX`；非正或非整数 TTL 被拒绝且不写入。
#[test]
fn setex_writes_value_with_expiry_and_rejects_bad_ttl() {
    let (mut client, server) = executor_server();
    send_command(&mut client, &["SETEX", "s1", "100", "payload"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["GET", "s1"]);
    assert_eq!(read_bulk(&mut client), Some(b"payload".to_vec()));
    send_command(&mut client, &["TTL", "s1"]);
    let ttl = read_integer(&mut client);
    assert!((95..=100).contains(&ttl), "SETEX must leave about 100 seconds of TTL, got {ttl}");
    send_command(&mut client, &["SETEX", "bad", "0", "v"]);
    assert_eq!(read_line(&mut client), "-ERR invalid expire time in 'setex' command\r");
    send_command(&mut client, &["SETEX", "bad", "abc", "v"]);
    assert_eq!(read_line(&mut client), "-ERR invalid expire time in 'setex' command\r");
    send_command(&mut client, &["GET", "bad"]);
    assert_eq!(read_bulk(&mut client), None);
    finish(client, server);
}

/// `APPEND` 保留已有 TTL；键不存在时新建无过期键。
#[test]
fn append_keeps_the_existing_ttl() {
    let (mut client, server) = executor_server();
    send_command(&mut client, &["SET", "a1", "hello", "EX", "100"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["APPEND", "a1", " world"]);
    assert_eq!(read_integer(&mut client), 11);
    send_command(&mut client, &["TTL", "a1"]);
    let ttl = read_integer(&mut client);
    assert!((95..=100).contains(&ttl), "APPEND must keep the TTL, got {ttl}");
    send_command(&mut client, &["APPEND", "fresh", "seed"]);
    assert_eq!(read_integer(&mut client), 4);
    send_command(&mut client, &["TTL", "fresh"]);
    assert_eq!(read_integer(&mut client), -1);
    finish(client, server);
}

// ---------- EXPIREAT / PEXPIREAT / SET EXAT|PXAT / GETEX ----------

fn now_unix_secs() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs() as i64
}

/// `EXPIREAT` 把截止期钉在绝对墙钟时刻，`TTL` 随即按剩余时间倒数。
#[test]
fn expireat_pins_an_absolute_deadline_and_ttl_counts_down() {
    let (mut client, server) = executor_server();
    send_command(&mut client, &["SET", "w1", "v"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    let at = now_unix_secs() + 100;
    send_command(&mut client, &["EXPIREAT", "w1", &at.to_string()]);
    assert_eq!(read_integer(&mut client), 1);
    send_command(&mut client, &["TTL", "w1"]);
    let ttl = read_integer(&mut client);
    assert!((95..=100).contains(&ttl), "EXPIREAT must leave about 100 seconds, got {ttl}");
    send_command(&mut client, &["EXPIREAT", "missing", &at.to_string()]);
    assert_eq!(read_integer(&mut client), 0, "an absent key reports 0");
    finish(client, server);
}

/// 非正绝对截止期等价删除，与 `EXPIRE <= 0` 同形。
#[test]
fn expireat_non_positive_deletes_the_key() {
    let (mut client, server) = executor_server();
    send_command(&mut client, &["SET", "w2", "v"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["EXPIREAT", "w2", "0"]);
    assert_eq!(read_integer(&mut client), 1);
    send_command(&mut client, &["GET", "w2"]);
    assert_eq!(read_bulk(&mut client), None);
    send_command(&mut client, &["EXPIREAT", "w2", "-5"]);
    assert_eq!(read_integer(&mut client), 0);
    finish(client, server);
}

/// `SET … EXAT/PXAT` 写入即钉绝对截止期；过去时刻写入后立即缺失。
#[test]
fn set_exat_pxat_write_absolute_deadlines() {
    let (mut client, server) = executor_server();
    let at_secs = now_unix_secs() + 100;
    send_command(&mut client, &["SET", "w3", "v", "EXAT", &at_secs.to_string()]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["TTL", "w3"]);
    let ttl = read_integer(&mut client);
    assert!((95..=100).contains(&ttl), "SET EXAT must leave about 100 seconds, got {ttl}");

    let at_ms = now_unix_secs() * 1000 + 100_000;
    send_command(&mut client, &["SET", "w4", "v", "PXAT", &at_ms.to_string()]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["PTTL", "w4"]);
    let pttl = read_integer(&mut client);
    assert!((95_000..=100_000).contains(&pttl), "SET PXAT must leave about 100000 ms, got {pttl}");

    // 过去的绝对时刻允许写入，但记录立即被读侧过滤为缺失。
    send_command(&mut client, &["SET", "w5", "v", "EXAT", "1"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["GET", "w5"]);
    assert_eq!(read_bulk(&mut client), None);
    send_command(&mut client, &["TTL", "w5"]);
    assert_eq!(read_integer(&mut client), -2);
    finish(client, server);
}

/// `GETEX` 原子读+触设：续期、清除与只读三种形态，以及缺键不触设。
#[test]
fn getex_reads_and_touches_the_ttl() {
    let (mut client, server) = executor_server();
    send_command(&mut client, &["SET", "g1", "payload", "EX", "100"]);
    assert_eq!(read_line(&mut client), "+OK\r");

    // 读回值并把 TTL 触设为新的相对截止期（滑动续期）。
    send_command(&mut client, &["GETEX", "g1", "EX", "500"]);
    assert_eq!(read_bulk(&mut client), Some(b"payload".to_vec()));
    send_command(&mut client, &["TTL", "g1"]);
    let ttl = read_integer(&mut client);
    assert!((495..=500).contains(&ttl), "GETEX EX must re-touch the TTL, got {ttl}");

    // `PERSIST` 触设后清除过期。
    send_command(&mut client, &["GETEX", "g1", "PERSIST"]);
    assert_eq!(read_bulk(&mut client), Some(b"payload".to_vec()));
    send_command(&mut client, &["TTL", "g1"]);
    assert_eq!(read_integer(&mut client), -1);

    // 无选项只读，不改过期状态。
    send_command(&mut client, &["GETEX", "g1"]);
    assert_eq!(read_bulk(&mut client), Some(b"payload".to_vec()));
    send_command(&mut client, &["TTL", "g1"]);
    assert_eq!(read_integer(&mut client), -1);

    // 缺键返回 nil 且不触设任何状态。
    send_command(&mut client, &["GETEX", "missing", "EX", "10"]);
    assert_eq!(read_bulk(&mut client), None);
    send_command(&mut client, &["EXISTS", "missing"]);
    assert_eq!(read_integer(&mut client), 0);

    // 非法选项与非法时间戳在读之前就被拒绝。
    send_command(&mut client, &["GETEX", "g1", "EX", "0"]);
    assert_eq!(read_line(&mut client), "-ERR invalid expire time in 'getex' command\r");
    send_command(&mut client, &["GETEX", "g1", "FOO"]);
    assert_eq!(read_line(&mut client), "-ERR syntax error\r");
    send_command(&mut client, &["GETEX", "g1", "EX", "1", "PERSIST"]);
    assert_eq!(read_line(&mut client), "-ERR syntax error\r");
    finish(client, server);
}

// ---------- RENAME / RENAMENX ----------

/// 会话轮换：值与剩余截止期在同一次提交内迁移，`RENAMENX` 尊重已存在的目标。
#[test]
fn rename_moves_the_value_and_deadline_in_one_commit() {
    let (mut client, server, path) = file_executor_server();

    send_command(&mut client, &["SET", "src", "payload", "PX", "100000"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["SET", "dst", "old"]);
    assert_eq!(read_line(&mut client), "+OK\r");

    send_command(&mut client, &["RENAME", "src", "dst"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["GET", "dst"]);
    assert_eq!(read_bulk(&mut client), Some(b"payload".to_vec()));
    send_command(&mut client, &["GET", "src"]);
    assert_eq!(read_bulk(&mut client), None);
    send_command(&mut client, &["PTTL", "dst"]);
    let pttl = read_integer(&mut client);
    assert!((1..=100_000).contains(&pttl), "迁移必须保留剩余截止期，实际 {pttl} ms");

    // 目标已存在时 RENAMENX 返回 0，源与目标都保持不变。
    send_command(&mut client, &["SET", "nx-src", "keep", "PX", "100000"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["SET", "nx-dst", "blocker"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["RENAMENX", "nx-src", "nx-dst"]);
    assert_eq!(read_integer(&mut client), 0);
    send_command(&mut client, &["GET", "nx-src"]);
    assert_eq!(read_bulk(&mut client), Some(b"keep".to_vec()));
    send_command(&mut client, &["GET", "nx-dst"]);
    assert_eq!(read_bulk(&mut client), Some(b"blocker".to_vec()));

    // 目标不存在时 RENAMENX 迁移并返回 1。
    send_command(&mut client, &["RENAMENX", "nx-src", "nx-free"]);
    assert_eq!(read_integer(&mut client), 1);
    send_command(&mut client, &["GET", "nx-free"]);
    assert_eq!(read_bulk(&mut client), Some(b"keep".to_vec()));

    // 同键：RENAME 是无操作成功，RENAMENX 视为目标已存在。
    send_command(&mut client, &["SET", "same", "v", "PX", "100000"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["RENAME", "same", "same"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["RENAMENX", "same", "same"]);
    assert_eq!(read_integer(&mut client), 0);
    send_command(&mut client, &["GET", "same"]);
    assert_eq!(read_bulk(&mut client), Some(b"v".to_vec()));

    finish(client, server);
    // 迁移已进入快照：重开后目标记录与截止期仍在，源键已物理消失。
    let mut reopened = FileShard::open(&path).unwrap();
    let destination = reopened.get(&Key::new("redis:0", b"dst")).unwrap().expect("目标记录必须在重开后存在");
    assert!(destination.expires_at.is_some(), "剩余截止期必须随记录一起持久化");
    assert!(reopened.get(&Key::new("redis:0", b"src")).unwrap().is_none(), "源键必须已删除");
    std::fs::remove_file(&path).unwrap();
    std::fs::remove_dir(path.parent().unwrap()).unwrap();
}

#[test]
fn rename_reports_missing_source_and_arity() {
    let (mut client, server) = executor_server();

    send_command(&mut client, &["RENAME", "missing", "dest"]);
    assert_eq!(read_line(&mut client), "-ERR no such key\r");
    send_command(&mut client, &["RENAMENX", "missing", "dest"]);
    assert_eq!(read_line(&mut client), "-ERR no such key\r");

    send_command(&mut client, &["RENAME", "only"]);
    assert_eq!(read_line(&mut client), "-ERR wrong number of arguments for 'rename' command\r");
    send_command(&mut client, &["RENAMENX", "a", "b", "c"]);
    assert_eq!(read_line(&mut client), "-ERR wrong number of arguments for 'renamenx' command\r");
    finish(client, server);
}

/// 队列期只校验 arity，源缺失等运行期语义推迟到 EXEC，并作为数组元素返回错误。
#[test]
fn rename_runtime_semantics_are_deferred_to_exec() {
    let (mut client, server) = executor_server();
    send_command(&mut client, &["MULTI"]);
    assert_eq!(read_line(&mut client), "+OK\r");
    send_command(&mut client, &["RENAME", "missing", "dest"]);
    assert_eq!(read_line(&mut client), "+QUEUED\r");
    send_command(&mut client, &["PING"]);
    assert_eq!(read_line(&mut client), "+QUEUED\r");
    send_command(&mut client, &["EXEC"]);
    assert_eq!(read_line(&mut client), "*2\r");
    assert_eq!(read_line(&mut client), "-ERR no such key\r");
    assert_eq!(read_line(&mut client), "+PONG\r");
    finish(client, server);
}
