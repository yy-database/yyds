use std::{
    collections::HashMap,
    io::{Read, Write},
    net::{Shutdown, TcpListener, TcpStream},
    sync::{Arc, atomic::AtomicBool},
    time::Duration,
};
use yyds_execution::LocalExecutor;
use yyds_gateway_redis::{
    connection::{serve_cancellable_with_executor, serve_connection},
    resp::RequestLimits,
};
use yyds_kv::MemoryShard;
use yyds_types::{ShardEpoch, ShardId, ShardMap};

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
            b"*3\r\n$3\r\nSET\r\n$4\r\n\0key\r\n$3\r\n\0\xff!\r\n*4\r\n$3\r\nSET\r\n$4\r\n\0key\r\n$3\r\nnew\r\n$2\r\nNX\r\n*2\r\n$3\r\nGET\r\n$4\r\n\0key\r\n*2\r\n$3\r\nDEL\r\n$4\r\n\0key\r\n*2\r\n$3\r\nGET\r\n$4\r\n\0key\r\n*2\r\n$4\r\nINCR\r\n$7\r\ncounter\r\n*2\r\n$3\r\nGET\r\n$7\r\ncounter\r\n*3\r\n$3\r\nSET\r\n$3\r\nbad\r\n$3\r\nabc\r\n*2\r\n$4\r\nINCR\r\n$3\r\nbad\r\n",
        )
        .unwrap();
    client.shutdown(Shutdown::Write).unwrap();
    let mut response = Vec::new();
    client.read_to_end(&mut response).unwrap();
    worker.join().unwrap();
    assert_eq!(
        response,
        b"+OK\r\n$-1\r\n$3\r\n\0\xff!\r\n:1\r\n$-1\r\n:1\r\n$1\r\n1\r\n+OK\r\n-ERR value is not an integer or out of range\r\n"
    );
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
