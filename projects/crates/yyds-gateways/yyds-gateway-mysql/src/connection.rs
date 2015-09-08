//! Bounded loopback MySQL protocol-v10 startup and explicit query rejection.

use std::{
    io::{self, Read, Write},
    net::TcpStream,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use crate::wire::{MAX_PACKET_PAYLOAD, PacketError, encode_packet};
use yyds_gateway::parse_sql;

const SERVER_CAPABILITIES: u32 =
    0x0000_0001 | 0x0000_0004 | 0x0000_0008 | 0x0000_0200 | 0x0000_2000 | 0x0000_8000 | 0x0002_0000 | 0x0008_0000;
const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
const CLIENT_SSL: u32 = 0x0000_0800;
const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
const CLIENT_PLUGIN_AUTH_LENENC: u32 = 0x0020_0000;
const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
const MAX_PACKET: usize = 1024 * 1024;

/// Performs a loopback-only unauthenticated startup and rejects SQL explicitly.
pub fn serve_connection(stream: TcpStream) -> io::Result<()> {
    if !stream.peer_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "MySQL requires loopback until authentication and TLS are implemented",
        ));
    }
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    serve_session(stream)
}

/// Serves a loopback session that can be interrupted by the owning node.
pub fn serve_cancellable(stream: TcpStream, stopping: Arc<AtomicBool>) -> io::Result<()> {
    if !stream.peer_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "MySQL requires loopback until authentication and TLS are implemented",
        ));
    }
    stream.set_nonblocking(true)?;
    serve_session(CancellableSocket { stream, stopping })
}

fn serve_session(mut stream: impl Read + Write) -> io::Result<()> {
    write_packet(&mut stream, 0, &handshake())?;
    let response = match read_packet(&mut stream)? {
        Some((1, response)) => response,
        Some(_) => return Err(invalid_data("invalid MySQL handshake sequence")),
        None => return Ok(()),
    };
    let (username, empty_password) = match parse_handshake_response(&response) {
        Ok(parsed) => parsed,
        Err(error) => {
            write_error(&mut stream, 2, 1043, "08S01", error)?;
            return Ok(());
        }
    };
    if username.is_empty() || !empty_password {
        write_error(&mut stream, 2, 1045, "28000", "access denied: loopback accepts only an empty password")?;
        return Ok(());
    }
    write_ok(&mut stream, 2)?;

    loop {
        let Some((sequence, payload)) = read_packet(&mut stream)?
        else {
            return Ok(());
        };
        if sequence != 0 {
            write_error(&mut stream, sequence.wrapping_add(1), 1047, "08S01", "invalid command sequence")?;
            continue;
        }
        if payload.is_empty() {
            write_error(&mut stream, sequence.wrapping_add(1), 1047, "08S01", "empty command")?;
            continue;
        }
        match payload[0] {
            0x01 => return Ok(()),
            0x03 => {
                let sql = match std::str::from_utf8(&payload[1..]) {
                    Ok(sql) => sql,
                    Err(_) => {
                        write_error(&mut stream, sequence.wrapping_add(1), 1300, "HY000", "query is not valid UTF-8")?;
                        continue;
                    }
                };
                if parse_sql(sql).is_err() {
                    write_error(&mut stream, sequence.wrapping_add(1), 1064, "42000", "Oak could not parse the SQL statement")?;
                }
                else {
                    write_error(
                        &mut stream,
                        sequence.wrapping_add(1),
                        1235,
                        "42000",
                        "YYDS MySQL SQL execution is not implemented",
                    )?;
                }
            }
            _ => write_error(&mut stream, sequence.wrapping_add(1), 1047, "08S01", "unsupported MySQL command")?,
        }
    }
}

fn handshake() -> Vec<u8> {
    let mut payload = Vec::with_capacity(80);
    payload.push(10);
    payload.extend_from_slice(b"8.0.36-yyds\0");
    payload.extend_from_slice(&1_u32.to_le_bytes());
    payload.extend_from_slice(b"yydsseed");
    payload.push(0);
    payload.extend_from_slice(&(SERVER_CAPABILITIES as u16).to_le_bytes());
    payload.push(45);
    payload.extend_from_slice(&2_u16.to_le_bytes());
    payload.extend_from_slice(&((SERVER_CAPABILITIES >> 16) as u16).to_le_bytes());
    payload.push(21);
    payload.extend_from_slice(&[0; 10]);
    payload.extend_from_slice(b"yydsseed1234");
    payload.push(0);
    payload.extend_from_slice(b"mysql_native_password\0");
    payload
}

fn parse_handshake_response(payload: &[u8]) -> Result<(String, bool), &'static str> {
    if payload.len() < 32 {
        return Err("truncated handshake response");
    }
    let capabilities = u32::from_le_bytes(payload[..4].try_into().map_err(|_| "invalid capabilities")?);
    if capabilities & CLIENT_PROTOCOL_41 == 0 || capabilities & CLIENT_SSL != 0 {
        return Err("protocol 4.1 is required and TLS is unavailable");
    }
    let mut cursor = 32;
    let username = take_nul_bytes(payload, &mut cursor)?;
    let auth_response = if capabilities & CLIENT_PLUGIN_AUTH_LENENC != 0 {
        let auth_length = take_lenenc(payload, &mut cursor)?;
        let start = cursor;
        advance(payload, &mut cursor, auth_length)?;
        &payload[start..cursor]
    }
    else if capabilities & CLIENT_SECURE_CONNECTION != 0 {
        let auth_length = usize::from(*payload.get(cursor).ok_or("missing auth response length")?);
        cursor += 1;
        let start = cursor;
        advance(payload, &mut cursor, auth_length)?;
        &payload[start..cursor]
    }
    else {
        take_nul_bytes(payload, &mut cursor)?
    };
    if capabilities & CLIENT_CONNECT_WITH_DB != 0 {
        let _ = take_nul_bytes(payload, &mut cursor)?;
    }
    if capabilities & CLIENT_PLUGIN_AUTH != 0 && cursor < payload.len() {
        let _ = take_nul_bytes(payload, &mut cursor)?;
    }
    let username = String::from_utf8(username.to_vec()).map_err(|_| "username is not UTF-8")?;
    Ok((username, auth_response.is_empty()))
}

fn take_nul_bytes<'a>(payload: &'a [u8], cursor: &mut usize) -> Result<&'a [u8], &'static str> {
    let rest = payload.get(*cursor..).ok_or("truncated handshake response")?;
    let length = rest.iter().position(|byte| *byte == 0).ok_or("unterminated handshake field")?;
    let value = &rest[..length];
    *cursor += length + 1;
    Ok(value)
}

fn take_lenenc(payload: &[u8], cursor: &mut usize) -> Result<usize, &'static str> {
    let first = *payload.get(*cursor).ok_or("missing auth response length")?;
    *cursor += 1;
    match first {
        0..=250 => Ok(usize::from(first)),
        251 => Err("NULL auth response length is invalid"),
        252 => take_integer(payload, cursor, 2),
        253 => take_integer(payload, cursor, 3),
        254 => take_integer(payload, cursor, 8),
        _ => Err("invalid auth response length"),
    }
}

fn take_integer(payload: &[u8], cursor: &mut usize, length: usize) -> Result<usize, &'static str> {
    let bytes = payload.get(*cursor..*cursor + length).ok_or("truncated length-encoded integer")?;
    *cursor += length;
    let mut value = 0_usize;
    for (shift, byte) in bytes.iter().enumerate() {
        if shift * 8 >= usize::BITS as usize && *byte != 0 {
            return Err("length-encoded integer overflow");
        }
        if shift * 8 < usize::BITS as usize {
            value |= usize::from(*byte) << (shift * 8);
        }
    }
    Ok(value)
}

fn advance(payload: &[u8], cursor: &mut usize, length: usize) -> Result<(), &'static str> {
    if length > MAX_PACKET || payload.get(*cursor..*cursor + length).is_none() {
        return Err("truncated or oversized auth response");
    }
    *cursor += length;
    Ok(())
}

fn read_packet(stream: &mut impl Read) -> io::Result<Option<(u8, Vec<u8>)>> {
    let mut header = [0; 4];
    let mut read = 0;
    while read < header.len() {
        match stream.read(&mut header[read..]) {
            Ok(0) if read == 0 => return Ok(None),
            Ok(0) => return Err(invalid_data("truncated MySQL packet header")),
            Ok(count) => read += count,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    let length = usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    if length > MAX_PACKET.min(MAX_PACKET_PAYLOAD) {
        return Err(invalid_data("MySQL packet exceeds byte limit"));
    }
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload)?;
    Ok(Some((header[3], payload)))
}

fn write_packet(stream: &mut impl Write, sequence: u8, payload: &[u8]) -> io::Result<()> {
    let packet = encode_packet(sequence, payload).map_err(packet_error)?;
    stream.write_all(&packet)
}

fn write_ok(stream: &mut impl Write, sequence: u8) -> io::Result<()> {
    write_packet(stream, sequence, &[0, 0, 0, 2, 0, 0, 0])
}

fn write_error(stream: &mut impl Write, sequence: u8, code: u16, state: &str, message: &str) -> io::Result<()> {
    let mut payload = Vec::with_capacity(message.len() + 9);
    payload.push(0xff);
    payload.extend_from_slice(&code.to_le_bytes());
    payload.push(b'#');
    payload.extend_from_slice(state.as_bytes());
    payload.extend_from_slice(message.as_bytes());
    write_packet(stream, sequence, &payload)
}

fn packet_error(error: PacketError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("invalid MySQL packet: {error:?}"))
}

fn invalid_data(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

struct CancellableSocket {
    stream: TcpStream,
    stopping: Arc<AtomicBool>,
}

impl CancellableSocket {
    fn attempt<T>(&mut self, mut operation: impl FnMut(&mut TcpStream) -> io::Result<T>) -> io::Result<T> {
        let deadline = Instant::now() + Duration::from_secs(30);
        loop {
            if self.stopping.load(Ordering::Acquire) {
                return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "MySQL service stopping"));
            }
            match operation(&mut self.stream) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "MySQL socket I/O timeout"));
                    }
                    std::thread::sleep(Duration::from_millis(5));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                result => return result,
            }
        }
    }
}

impl Read for CancellableSocket {
    fn read(&mut self, bytes: &mut [u8]) -> io::Result<usize> {
        self.attempt(|stream| stream.read(bytes))
    }
}

impl Write for CancellableSocket {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.attempt(|stream| stream.write(bytes))
    }

    fn flush(&mut self) -> io::Result<()> {
        self.attempt(Write::flush)
    }
}
