//! Stateless RESP2 protocol probes, without direct access to database storage.

use std::{
    io::{self, Read, Write},
    net::TcpStream,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};

use yyds_execution::KeyValueExecutor;
use yyds_types::{KeyValueResult, Namespace};

use crate::{
    bind::{BindError, bind},
    resp::{Request, RequestLimits, decode_request},
};

/// Serves one bounded RESP2 connection with a 30-second I/O timeout.
/// Only PING, ECHO and QUIT are supported. Database commands require a binder.
pub fn serve_connection(stream: TcpStream, limits: RequestLimits) -> io::Result<()> {
    stream.set_nonblocking(false)?;
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    serve_io(stream, limits, None)
}

/// Serves a node-owned connection with cooperative cancellation of all socket I/O.
pub fn serve_cancellable(stream: TcpStream, limits: RequestLimits, stopping: Arc<AtomicBool>) -> io::Result<()> {
    stream.set_nonblocking(true)?;
    serve_io(CancellableSocket { stream, stopping }, limits, None)
}

/// Serves a node-owned RESP2 connection using the YYDS key/value execution contract.
pub fn serve_cancellable_with_executor(
    stream: TcpStream,
    limits: RequestLimits,
    stopping: Arc<AtomicBool>,
    executor: Arc<dyn KeyValueExecutor>,
) -> io::Result<()> {
    stream.set_nonblocking(true)?;
    serve_io(CancellableSocket { stream, stopping }, limits, Some(executor))
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
                return Err(io::Error::new(io::ErrorKind::ConnectionAborted, "node service stopping"));
            }
            match operation(&mut self.stream) {
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return Err(io::Error::new(io::ErrorKind::TimedOut, "socket I/O timeout"));
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

fn serve_io(
    mut stream: impl Read + Write,
    limits: RequestLimits,
    executor: Option<Arc<dyn KeyValueExecutor>>,
) -> io::Result<()> {
    let mut pending = Vec::new();
    let mut chunk = [0u8; 4096];
    let mut namespace = Namespace("redis:0".into());
    loop {
        loop {
            match decode_request(&pending, limits) {
                Ok(Some(request)) => {
                    let consumed = request.consumed;
                    if !respond(&mut stream, &request, executor.as_deref(), &mut namespace)? {
                        return Ok(());
                    }
                    pending.drain(..consumed);
                }
                Ok(None) => break,
                Err(_) => {
                    stream.write_all(b"-ERR Protocol error or request limit exceeded\r\n")?;
                    return Ok(());
                }
            }
        }
        let available = limits.max_frame_bytes.saturating_sub(pending.len()).min(chunk.len());
        if available == 0 {
            stream.write_all(b"-ERR Protocol error or request limit exceeded\r\n")?;
            return Ok(());
        }
        let received = match stream.read(&mut chunk[..available]) {
            Ok(received) => received,
            Err(error) if matches!(error.kind(), io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock) => return Ok(()),
            Err(error) => return Err(error),
        };
        if received == 0 {
            if !pending.is_empty() {
                stream.write_all(b"-ERR Truncated request\r\n")?
            }
            return Ok(());
        }
        pending.extend_from_slice(&chunk[..received]);
    }
}

fn respond(
    stream: &mut impl Write,
    request: &Request<'_>,
    executor: Option<&dyn KeyValueExecutor>,
    namespace: &mut Namespace,
) -> io::Result<bool> {
    let command = request.arguments[0];
    let count = request.arguments.len();
    if command.eq_ignore_ascii_case(b"PING") {
        match count {
            1 => stream.write_all(b"+PONG\r\n")?,
            2 => write_bulk(stream, request.arguments[1])?,
            _ => stream.write_all(b"-ERR wrong number of arguments for 'ping' command\r\n")?,
        }
    }
    else if command.eq_ignore_ascii_case(b"ECHO") {
        if count == 2 {
            write_bulk(stream, request.arguments[1])?
        }
        else {
            stream.write_all(b"-ERR wrong number of arguments for 'echo' command\r\n")?
        }
    }
    else if command.eq_ignore_ascii_case(b"QUIT") {
        if count == 1 {
            stream.write_all(b"+OK\r\n")?;
            return Ok(false);
        }
        stream.write_all(b"-ERR wrong number of arguments for 'quit' command\r\n")?;
    }
    else if command.eq_ignore_ascii_case(b"SELECT") && executor.is_some() {
        if count != 2 {
            stream.write_all(b"-ERR wrong number of arguments for 'select' command\r\n")?;
        }
        else if let Some(database) = parse_database(request.arguments[1]) {
            *namespace = Namespace(format!("redis:{database}"));
            stream.write_all(b"+OK\r\n")?;
        }
        else {
            stream.write_all(b"-ERR DB index is out of range\r\n")?;
        }
    }
    else if let Some(executor) = executor {
        match bind(&request.arguments, &namespace) {
            Ok(command) => match executor.execute(command) {
                Ok(KeyValueResult::Get(Some(value))) => write_bulk(stream, &value)?,
                Ok(KeyValueResult::Get(None)) => stream.write_all(b"$-1\r\n")?,
                Ok(KeyValueResult::Put { .. }) => stream.write_all(b"+OK\r\n")?,
                Ok(KeyValueResult::Increment { value }) => write!(stream, ":{value}\r\n")?,
                Ok(KeyValueResult::Delete { removed }) => stream.write_all(if removed { b":1\r\n" } else { b":0\r\n" })?,
                Err(error) => write_error(stream, &error.to_string())?,
            },
            Err(BindError::WrongArity) => stream.write_all(b"-ERR wrong number of arguments\r\n")?,
            Err(BindError::UnsupportedOptions) => stream.write_all(b"-ERR unsupported SET options\r\n")?,
            Err(BindError::InvalidInteger) => stream.write_all(b"-ERR value is not an integer or out of range\r\n")?,
            Err(BindError::EmptyCommand) => stream.write_all(b"-ERR empty command\r\n")?,
            Err(BindError::UnsupportedCommand) => stream.write_all(b"-ERR unsupported command\r\n")?,
        }
    }
    else {
        stream.write_all(b"-ERR unsupported command\r\n")?;
    }
    Ok(true)
}

fn parse_database(bytes: &[u8]) -> Option<u8> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let database = bytes.iter().try_fold(0_u8, |value, digit| value.checked_mul(10)?.checked_add(digit - b'0'))?;
    (database < 16).then_some(database)
}

fn write_error(stream: &mut impl Write, message: &str) -> io::Result<()> {
    let sanitized = message
        .chars()
        .map(|character| if matches!(character, '\r' | '\n') { ' ' } else { character })
        .take(512)
        .collect::<String>();
    write!(stream, "-ERR {sanitized}\r\n")
}

fn write_bulk(stream: &mut impl Write, bytes: &[u8]) -> io::Result<()> {
    write!(stream, "${}\r\n", bytes.len())?;
    stream.write_all(bytes)?;
    stream.write_all(b"\r\n")
}
