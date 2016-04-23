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
use yyds_kv::MemoryShard;
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
        )
        .unwrap(),
    );
    let namespace = Namespace("redis:0".into());
    executor.execute(bind(&[b"SET", b"first", b"value"], &namespace).unwrap()).unwrap();
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
    client
        .write_all(b"*1\r\n$5\r\nMULTI\r\n*1\r\n$4\r\nEXEC\r\n")
        .unwrap();
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
    assert_eq!(
        response,
        b"+OK\r\n+QUEUED\r\n-ERR MULTI calls can not be nested\r\n+OK\r\n$-1\r\n"
    );
}

#[test]
fn exec_without_multi_and_discard_without_multi_are_rejected() {
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*1\r\n$4\r\nEXEC\r\n*1\r\n$7\r\nDISCARD\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    assert_eq!(
        response,
        b"-ERR EXEC without MULTI\r\n-ERR DISCARD without MULTI\r\n"
    );
}

#[test]
fn multi_exec_nested_ping_echo_are_buffered_and_flattened() {
    // PING and ECHO are not executor-backed, so they are also queued and executed under EXEC.
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*1\r\n$5\r\nMULTI\r\n*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$5\r\nhello\r\n*1\r\n$4\r\nEXEC\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    assert_eq!(
        response,
        b"+OK\r\n+QUEUED\r\n+QUEUED\r\n*2\r\n+PONG\r\n$5\r\nhello\r\n"
    );
}

#[test]
fn multi_exec_command_syntax_error_is_rejected_at_queue_time() {
    // GET needs 1 argument inside MULTI — a missing argument is a syntax error and must be
    // reported immediately (no QUEUED).
    let (mut client, server) = executor_server();
    client
        .write_all(
            b"*1\r\n$5\r\nMULTI\r\n*1\r\n$3\r\nGET\r\n*1\r\n$4\r\nEXEC\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    // GET inside MULTI with no args → bind() fails with WrongArity → immediate error, EXEC runs empty queue.
    assert_eq!(
        response,
        b"+OK\r\n-ERR wrong number of arguments\r\n*0\r\n"
    );
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
    assert!(
        response.starts_with(b"+OK\r\n:1\r\n:"),
        "unexpected response: {response:?}"
    );
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
    client
        .write_all(
            b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n*3\r\n$6\r\nEXPIRE\r\n$3\r\nkey\r\n$1\r\n1\r\n",
        )
        .unwrap();
    std::thread::sleep(std::time::Duration::from_secs(2));
    client
        .write_all(
            b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n*2\r\n$3\r\nTTL\r\n$3\r\nkey\r\n*2\r\n$6\r\nEXISTS\r\n$3\r\nkey\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    server.join().unwrap();
    // GET after expiry → $-1, TTL → -2, EXISTS → 0
    assert_eq!(response, b"+OK\r\n:1\r\n$-1\r\n:-2\r\n:0\r\n");
}
