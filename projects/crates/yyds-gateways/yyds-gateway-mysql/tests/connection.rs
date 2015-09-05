use std::{
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::Duration,
};

use yyds_gateway_mysql::{
    connection::serve_connection,
    wire::{decode_packet, encode_packet},
};

const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;

fn read_packet(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 4];
    stream.read_exact(&mut header).unwrap();
    let length = usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).unwrap();
    (header[3], payload)
}

fn handshake_response(password: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&(CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH).to_le_bytes());
    payload.extend_from_slice(&1_048_576_u32.to_le_bytes());
    payload.push(45);
    payload.extend_from_slice(&[0; 23]);
    payload.extend_from_slice(b"test-user\0");
    payload.push(password.len() as u8);
    payload.extend_from_slice(password);
    payload.extend_from_slice(b"mysql_native_password\0");
    payload
}

fn start_server() -> (TcpStream, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_connection(stream).unwrap();
    });
    let client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    (client, server)
}

#[test]
fn protocol_v10_handshake_accepts_empty_password_then_rejects_sql_explicitly() {
    let (mut client, server) = start_server();
    let (sequence, handshake) = read_packet(&mut client);
    assert_eq!(sequence, 0);
    assert_eq!(handshake[0], 10);
    assert!(handshake.windows(b"mysql_native_password".len()).any(|window| window == b"mysql_native_password"));

    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet(&mut client), (2, vec![0, 0, 0, 2, 0, 0, 0]));

    client.write_all(&encode_packet(0, b"\x03select 1").unwrap()).unwrap();
    let (sequence, error) = read_packet(&mut client);
    assert_eq!(sequence, 1);
    assert_eq!(error[0], 0xff);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1235);
    assert_eq!(&error[3..9], b"#42000");
    drop(client);
    server.join().unwrap();
}

#[test]
fn nonempty_password_is_rejected_and_sql_is_not_interpreted() {
    let (mut client, server) = start_server();
    let _ = read_packet(&mut client);
    client.write_all(&encode_packet(1, &handshake_response(b"secret")).unwrap()).unwrap();
    let (sequence, error) = read_packet(&mut client);
    assert_eq!(sequence, 2);
    assert_eq!(error[0], 0xff);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1045);
    server.join().unwrap();
}

#[test]
fn packet_codec_output_matches_connection_packet_decoder() {
    let frame = encode_packet(9, b"binary\0payload").unwrap();
    let packet = decode_packet(&frame, 1024).unwrap().unwrap();
    assert_eq!(packet.sequence, 9);
    assert_eq!(packet.payload, b"binary\0payload");
}
