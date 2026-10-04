use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

use yyds_gateway_pgsql::{connection::serve_connection, wire::encode_message};

const PROTOCOL_V3: u32 = 196_608;

fn read_message(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 5];
    stream.read_exact(&mut header).unwrap();
    let length = u32::from_be_bytes(header[1..5].try_into().unwrap()) as usize;
    let mut payload = vec![0; length - 4];
    stream.read_exact(&mut payload).unwrap();
    (header[0], payload)
}

fn receive_until_ready(stream: &mut TcpStream) -> Vec<(u8, Vec<u8>)> {
    let mut messages = Vec::new();
    loop {
        let message = read_message(stream);
        let ready = message.0 == b'Z';
        messages.push(message);
        if ready {
            return messages;
        }
    }
}

fn send_query(stream: &mut TcpStream, sql: &str) {
    let mut payload = sql.as_bytes().to_vec();
    payload.push(0);
    stream.write_all(&encode_message(b'Q', &payload, 1024).unwrap()).unwrap();
}

fn start_server() -> (TcpStream, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_connection(stream).unwrap();
    });
    let mut client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    let mut startup = Vec::from(0_u32.to_be_bytes());
    startup.extend_from_slice(&PROTOCOL_V3.to_be_bytes());
    startup.extend_from_slice(b"user\0test\0\0");
    let length = startup.len() as u32;
    startup[..4].copy_from_slice(&length.to_be_bytes());
    client.write_all(&startup).unwrap();
    let startup_responses = receive_until_ready(&mut client);
    assert_eq!(startup_responses.last().unwrap(), &(b'Z', b"I".to_vec()));
    (client, server)
}

#[test]
fn transaction_commands_track_failed_state_and_rollback_recovery() {
    let (mut client, server) = start_server();

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"BEGIN\0".to_vec()), (b'Z', b"T".to_vec())]);

    send_query(&mut client, "SELECT 1");
    let failed = receive_until_ready(&mut client);
    assert_eq!(failed[0].0, b'E');
    assert!(failed[0].1.windows(5).any(|window| window == b"0A000"));
    assert_eq!(failed.last().unwrap(), &(b'Z', b"E".to_vec()));

    send_query(&mut client, "SELECT 2");
    let aborted = receive_until_ready(&mut client);
    assert_eq!(aborted[0].0, b'E');
    assert!(aborted[0].1.windows(5).any(|window| window == b"25P02"));
    assert_eq!(aborted.last().unwrap(), &(b'Z', b"E".to_vec()));

    send_query(&mut client, "BEGIN");
    let still_failed = receive_until_ready(&mut client);
    assert_eq!(still_failed[0].0, b'E');
    assert!(still_failed[0].1.windows(5).any(|window| window == b"25P02"));
    assert_eq!(still_failed.last().unwrap(), &(b'Z', b"E".to_vec()));

    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"ROLLBACK\0".to_vec()), (b'Z', b"I".to_vec())]);

    send_query(&mut client, "SELECT (");
    let syntax_error = receive_until_ready(&mut client);
    assert_eq!(syntax_error[0].0, b'E');
    assert!(syntax_error[0].1.windows(5).any(|window| window == b"42601"));
    assert_eq!(syntax_error.last().unwrap(), &(b'Z', b"I".to_vec()));

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "SELECT 1");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"E".to_vec()));
    send_query(&mut client, "COMMIT");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"ROLLBACK\0".to_vec()), (b'Z', b"I".to_vec())]);

    drop(client);
    server.join().unwrap();
}
