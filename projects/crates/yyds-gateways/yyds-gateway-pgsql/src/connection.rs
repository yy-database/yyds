//! PostgreSQL protocol-v3 connection establishment and explicit query rejection.

use std::{
    io::{self, Read, Write},
    net::TcpStream,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

use crate::wire::{FrameError, StartupParameterError, StartupParameters, decode_message, decode_startup, encode_message};
use yyds_gateway::parse_sql;

const PROTOCOL_V3: u32 = 196_608;
const SSL_REQUEST: u32 = 80_877_103;
const GSSENC_REQUEST: u32 = 80_877_104;
const CANCEL_REQUEST: u32 = 80_877_102;
const MAX_STARTUP_FRAME: usize = 64 * 1024;
const MAX_MESSAGE_FRAME: usize = 16 * 1024 * 1024;
static NEXT_SECRET: AtomicU32 = AtomicU32::new(1);

/// Establishes an unauthenticated protocol-v3 session, then rejects unsupported SQL explicitly.
///
/// The caller must restrict the peer to loopback until authentication and TLS are implemented.
pub fn serve_connection(stream: TcpStream) -> io::Result<()> {
    if !stream.peer_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "PostgreSQL requires loopback until authentication and TLS are implemented",
        ));
    }
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    serve_session(stream)
}

/// Serves a loopback session with cooperative service-stop cancellation.
pub fn serve_cancellable(stream: TcpStream, stopping: Arc<AtomicBool>) -> io::Result<()> {
    if !stream.peer_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "PostgreSQL requires loopback until authentication and TLS are implemented",
        ));
    }
    stream.set_nonblocking(true)?;
    serve_session(CancellableSocket { stream, stopping })
}

fn serve_session(mut stream: impl Read + Write) -> io::Result<()> {
    let mut pending = Vec::new();
    let parameters = loop {
        let packet = read_startup(&mut stream, &mut pending)?;
        match packet.code {
            SSL_REQUEST | GSSENC_REQUEST => stream.write_all(b"N")?,
            CANCEL_REQUEST => return Ok(()),
            PROTOCOL_V3 => match StartupParameters::parse(&packet.payload) {
                Ok(parameters) => break parameters,
                Err(error) => {
                    write_fatal(&mut stream, "08P01", &format_startup_error(error))?;
                    return Ok(());
                }
            },
            _ => {
                write_fatal(&mut stream, "0A000", "unsupported PostgreSQL protocol version")?;
                return Ok(());
            }
        }
    };

    send_authentication_ok(&mut stream)?;
    send_parameter(&mut stream, "server_version", "16.0")?;
    send_parameter(&mut stream, "server_encoding", "UTF8")?;
    send_parameter(&mut stream, "client_encoding", "UTF8")?;
    send_parameter(&mut stream, "DateStyle", "ISO, MDY")?;
    send_parameter(&mut stream, "integer_datetimes", "on")?;
    send_parameter(&mut stream, "standard_conforming_strings", "on")?;
    send_parameter(&mut stream, "TimeZone", "UTC")?;
    if let Some(application_name) = parameters.get("application_name") {
        send_parameter(&mut stream, "application_name", application_name)?;
    }
    send_backend_key(&mut stream)?;
    send_ready(&mut stream)?;

    loop {
        let message = match read_message(&mut stream, &mut pending) {
            Ok(Some(message)) => message,
            Ok(None) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(()),
            Err(error) => return Err(error),
        };
        match message.tag {
            b'X' if message.payload.is_empty() => return Ok(()),
            b'Q' => {
                let Some(sql_bytes) = message.payload.strip_suffix(&[0])
                else {
                    write_error_response(&mut stream, "08P01", "malformed PostgreSQL simple-query message")?;
                    send_ready(&mut stream)?;
                    continue;
                };
                if sql_bytes.contains(&0) {
                    write_error_response(&mut stream, "08P01", "embedded NUL in PostgreSQL query")?;
                }
                else if let Ok(sql) = std::str::from_utf8(sql_bytes) {
                    match parse_sql(sql) {
                        Err(_) => write_error_response(&mut stream, "42601", "Oak could not parse the SQL statement")?,
                        Ok(_) => write_error_response(&mut stream, "0A000", "PostgreSQL query execution is not implemented")?,
                    }
                }
                else {
                    write_error_response(&mut stream, "22021", "query is not valid UTF-8")?;
                }
                send_ready(&mut stream)?;
            }
            _ => {
                write_error_response(&mut stream, "08P01", "unsupported PostgreSQL frontend message")?;
                send_ready(&mut stream)?;
            }
        }
    }
}

struct OwnedStartup {
    code: u32,
    payload: Vec<u8>,
}

struct OwnedMessage {
    tag: u8,
    payload: Vec<u8>,
}

fn read_startup(stream: &mut impl Read, pending: &mut Vec<u8>) -> io::Result<OwnedStartup> {
    loop {
        match decode_startup(pending, MAX_STARTUP_FRAME) {
            Ok(Some(packet)) => {
                let result = OwnedStartup { code: packet.code, payload: packet.payload.to_vec() };
                pending.drain(..packet.consumed);
                return Ok(result);
            }
            Ok(None) => read_more(stream, pending)?,
            Err(error) => return Err(frame_error(error)),
        }
    }
}

fn read_message(stream: &mut impl Read, pending: &mut Vec<u8>) -> io::Result<Option<OwnedMessage>> {
    loop {
        match decode_message(pending, MAX_MESSAGE_FRAME) {
            Ok(Some(message)) => {
                let result = OwnedMessage { tag: message.tag, payload: message.payload.to_vec() };
                pending.drain(..message.consumed);
                return Ok(Some(result));
            }
            Ok(None) => {
                if let Err(error) = read_more(stream, pending) {
                    if error.kind() == io::ErrorKind::UnexpectedEof {
                        return if pending.is_empty() {
                            Ok(None)
                        }
                        else {
                            Err(io::Error::new(io::ErrorKind::InvalidData, "truncated PostgreSQL message"))
                        };
                    }
                    return Err(error);
                }
            }
            Err(error) => return Err(frame_error(error)),
        }
    }
}

fn read_more(stream: &mut impl Read, pending: &mut Vec<u8>) -> io::Result<()> {
    let mut chunk = [0; 8192];
    let read = stream.read(&mut chunk)?;
    if read == 0 {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "PostgreSQL peer closed"));
    }
    pending.extend_from_slice(&chunk[..read]);
    Ok(())
}

fn send_authentication_ok(stream: &mut impl Write) -> io::Result<()> {
    write_message(stream, b'R', &0_u32.to_be_bytes())
}

fn send_parameter(stream: &mut impl Write, key: &str, value: &str) -> io::Result<()> {
    let mut payload = Vec::with_capacity(key.len() + value.len() + 2);
    payload.extend_from_slice(key.as_bytes());
    payload.push(0);
    payload.extend_from_slice(value.as_bytes());
    payload.push(0);
    write_message(stream, b'S', &payload)
}

fn send_backend_key(stream: &mut impl Write) -> io::Result<()> {
    let mut payload = Vec::with_capacity(8);
    payload.extend_from_slice(&std::process::id().to_be_bytes());
    payload.extend_from_slice(&NEXT_SECRET.fetch_add(1, Ordering::Relaxed).to_be_bytes());
    write_message(stream, b'K', &payload)
}

fn send_ready(stream: &mut impl Write) -> io::Result<()> {
    write_message(stream, b'Z', b"I")
}

fn write_error_response(stream: &mut impl Write, code: &str, message: &str) -> io::Result<()> {
    let mut payload = Vec::new();
    payload.push(b'S');
    payload.extend_from_slice(b"ERROR\0C");
    payload.extend_from_slice(code.as_bytes());
    payload.extend_from_slice(b"\0M");
    payload.extend_from_slice(message.as_bytes());
    payload.extend_from_slice(b"\0\0");
    write_message(stream, b'E', &payload)
}

fn write_fatal(stream: &mut impl Write, code: &str, message: &str) -> io::Result<()> {
    let mut payload = Vec::new();
    payload.push(b'S');
    payload.extend_from_slice(b"FATAL\0C");
    payload.extend_from_slice(code.as_bytes());
    payload.extend_from_slice(b"\0M");
    payload.extend_from_slice(message.as_bytes());
    payload.extend_from_slice(b"\0\0");
    write_message(stream, b'E', &payload)
}

fn write_message(stream: &mut impl Write, tag: u8, payload: &[u8]) -> io::Result<()> {
    let encoded = encode_message(tag, payload, MAX_MESSAGE_FRAME).map_err(frame_error)?;
    stream.write_all(&encoded)
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
                return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "PostgreSQL service stopping"));
            }
            match operation(&mut self.stream) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "PostgreSQL socket I/O timeout"));
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

fn frame_error(error: FrameError) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("invalid PostgreSQL frame: {error:?}"))
}

fn format_startup_error(error: StartupParameterError) -> String {
    format!("invalid PostgreSQL startup parameters: {error:?}")
}
