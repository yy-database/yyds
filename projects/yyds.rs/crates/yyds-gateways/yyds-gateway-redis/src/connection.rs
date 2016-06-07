//! RESP2 protocol probe with transaction queueing and key expiry support.

use std::{
    collections::HashMap,
    io::{self, Read, Write},
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use yyds_execution::KeyValueExecutor;
use yyds_types::{KeyValueResult, Namespace};

use crate::{
    bind::{bind, bind_mget, BindError},
    resp::{decode_request, Request, RequestLimits},
};

/// A `Request` whose arguments have been copied into owned buffers so it can outlive the
/// RESP2 frame that decoded it. Used inside the MULTI transaction queue where commands are
/// held and replayed later.
#[derive(Debug)]
struct OwnedRequest {
    arguments: Vec<Vec<u8>>,
}

impl OwnedRequest {
    /// Re-borrows the arguments as a `Request<'static>` pointing at our owned storage.
    fn as_request(&self) -> Request<'_> {
        Request { arguments: self.arguments.iter().map(|v| v.as_slice()).collect(), consumed: 0 }
    }
}

/// One command buffered inside a `MULTI` block.
#[derive(Debug)]
struct QueuedCommand {
    request: OwnedRequest,
}

/// Per-connection state carried across RESP2 frames.
#[derive(Debug, Default)]
struct ConnectionState {
    /// Whether the connection is currently inside a `MULTI` block.
    in_multi: bool,
    /// Commands queued inside the active `MULTI` block.
    queue: Vec<QueuedCommand>,
    /// Key expiry: `(namespace, key_bytes) -> expiry_instant`.
    expiries: HashMap<(String, Vec<u8>), Instant>,
}

impl ConnectionState {
    fn new() -> Self {
        Self::default()
    }
}

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
    let mut state = ConnectionState::new();
    loop {
        loop {
            match decode_request(&pending, limits) {
                Ok(Some(request)) => {
                    let consumed = request.consumed;
                    let result =
                        respond(&mut stream as &mut dyn Write, &request, executor.as_deref(), &mut namespace, &mut state);
                    if !result? {
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

/// Checks whether `(namespace, key)` has a stored expiry that has elapsed. When so, the
/// expiry record is removed and the underlying KV entry is deleted via the executor (if one
/// is available). Returns `true` when the key was expired-and-purged by this call.
fn purge_if_expired(state: &mut ConnectionState, executor: Option<&dyn KeyValueExecutor>, namespace: &str, key: &[u8]) -> bool {
    let ns_key = (namespace.to_owned(), key.to_vec());
    if let Some(due) = state.expiries.get(&ns_key) {
        if Instant::now() >= *due {
            state.expiries.remove(&ns_key);
            if let Some(executor) = executor {
                let _ = executor.execute(
                    None,
                    yyds_types::KeyValueCommand {
                        namespace: Namespace(namespace.to_owned()),
                        key: key.to_vec(),
                        action: yyds_types::KeyValueAction::Delete,
                    },
                );
            }
            return true;
        }
    }
    false
}

fn respond(
    stream: &mut dyn Write,
    request: &Request<'_>,
    executor: Option<&dyn KeyValueExecutor>,
    namespace: &mut Namespace,
    state: &mut ConnectionState,
) -> io::Result<bool> {
    let command = request.arguments[0];
    let count = request.arguments.len();

    // ---- Transaction control: MULTI / EXEC / DISCARD. These are always handled directly
    // and never queued (Redis would reject a second MULTI inside a MULTI block anyway).

    if command.eq_ignore_ascii_case(b"MULTI") {
        if count != 1 {
            stream.write_all(b"-ERR wrong number of arguments for 'multi' command\r\n")?;
            return Ok(true);
        }
        if state.in_multi {
            stream.write_all(b"-ERR MULTI calls can not be nested\r\n")?;
            return Ok(true);
        }
        state.in_multi = true;
        state.queue.clear();
        stream.write_all(b"+OK\r\n")?;
        return Ok(true);
    }

    if command.eq_ignore_ascii_case(b"EXEC") {
        if count != 1 {
            stream.write_all(b"-ERR wrong number of arguments for 'exec' command\r\n")?;
            return Ok(true);
        }
        if !state.in_multi {
            stream.write_all(b"-ERR EXEC without MULTI\r\n")?;
            return Ok(true);
        }
        execute_transaction(stream, executor, namespace, state)?;
        return Ok(true);
    }

    if command.eq_ignore_ascii_case(b"DISCARD") {
        if count != 1 {
            stream.write_all(b"-ERR wrong number of arguments for 'discard' command\r\n")?;
            return Ok(true);
        }
        if !state.in_multi {
            stream.write_all(b"-ERR DISCARD without MULTI\r\n")?;
            return Ok(true);
        }
        state.in_multi = false;
        state.queue.clear();
        stream.write_all(b"+OK\r\n")?;
        return Ok(true);
    }

    // ---- Inside a MULTI block: buffer every other command and return QUEUED.
    // Syntax / arity validation happens here so clients learn about bad commands at queue
    // time instead of at EXEC time. The actual runtime behaviour (wrong-type, NX failure,
    // etc.) is deferred to EXEC per Redis semantics.
    if state.in_multi {
        if let Err(error_bytes) = validate_command_for_queue(command, count, &request.arguments, executor, namespace) {
            stream.write_all(&error_bytes)?;
            return Ok(true);
        }
        let owned = OwnedRequest { arguments: request.arguments.iter().map(|arg| arg.to_vec()).collect() };
        state.queue.push(QueuedCommand { request: owned });
        stream.write_all(b"+QUEUED\r\n")?;
        return Ok(true);
    }

    // ---- Below is the normal, command-by-command execution path.

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
    else if command.eq_ignore_ascii_case(b"EXPIRE") || command.eq_ignore_ascii_case(b"PEXPIRE") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count != 3 {
            stream.write_all(b"-ERR wrong number of arguments for 'expire' command\r\n")?;
            return Ok(true);
        }
        let key = request.arguments[1];
        let seconds = match parse_integer(request.arguments[2]) {
            Some(v) => v,
            None => {
                stream.write_all(b"-ERR value is not an integer or out of range\r\n")?;
                return Ok(true);
            }
        };
        let key_owned = key.to_vec();
        let ns_key = (namespace.0.clone(), key_owned.clone());

        // Check the key actually exists (and hasn't just been purged by an expiry).
        let exists = purge_if_expired(state, Some(executor), &namespace.0, key)
            || matches!(
                executor.execute(
                    None,
                    yyds_types::KeyValueCommand {
                        namespace: namespace.clone(),
                        key: key_owned.clone(),
                        action: yyds_types::KeyValueAction::Exists,
                    }
                ),
                Ok(KeyValueResult::Exists(true))
            );

        if !exists {
            stream.write_all(b":0\r\n")?;
            return Ok(true);
        }

        if seconds <= 0 {
            // Redis: EXPIRE key 0 behaves like DEL. Remove expiry + key.
            state.expiries.remove(&ns_key);
            let _ = executor.execute(
                None,
                yyds_types::KeyValueCommand {
                    namespace: namespace.clone(),
                    key: key_owned,
                    action: yyds_types::KeyValueAction::Delete,
                },
            );
            stream.write_all(b":1\r\n")?;
            return Ok(true);
        }

        let ttl = if command.eq_ignore_ascii_case(b"PEXPIRE") {
            Duration::from_millis(seconds as u64)
        }
        else {
            Duration::from_secs(seconds as u64)
        };
        state.expiries.insert(ns_key, Instant::now() + ttl);
        stream.write_all(b":1\r\n")?;
    }
    else if command.eq_ignore_ascii_case(b"MGET") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        match bind_mget(&request.arguments, namespace) {
            Ok(commands) => {
                // Purge any expired keys first so MGET correctly returns nil.
                for cmd in &commands {
                    let _ = purge_if_expired(state, Some(executor), &cmd.namespace.0, &cmd.key);
                }
                match executor.execute_read_batch(None, &commands) {
                    Ok(results) => {
                        if results.len() != commands.len()
                            || results.iter().any(|result| !matches!(result, KeyValueResult::Get(_)))
                        {
                            write_error(stream, "MGET executor returned an invalid read batch")?;
                            return Ok(true);
                        }
                        write!(stream, "*{}\r\n", results.len())?;
                        for result in results {
                            match result {
                                KeyValueResult::Get(Some(value)) => write_bulk(stream, &value)?,
                                KeyValueResult::Get(None) => stream.write_all(b"$-1\r\n")?,
                                _ => unreachable!(),
                            }
                        }
                    }
                    Err(error) => write_error(stream, &error.to_string())?,
                }
            }
            Err(BindError::WrongArity) => stream.write_all(b"-ERR wrong number of arguments\r\n")?,
            Err(BindError::EmptyCommand) => stream.write_all(b"-ERR empty command\r\n")?,
            Err(BindError::UnsupportedCommand) => stream.write_all(b"-ERR unsupported command\r\n")?,
            _ => stream.write_all(b"-ERR invalid MGET command\r\n")?,
        }
    }
    else if command.eq_ignore_ascii_case(b"EXISTS") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count < 2 {
            stream.write_all(b"-ERR wrong number of arguments for 'exists' command\r\n")?;
            return Ok(true);
        }
        let mut present: i64 = 0;
        for key in &request.arguments[1..] {
            let _ = purge_if_expired(state, Some(executor), &namespace.0, key);
            let result = executor.execute(
                None,
                yyds_types::KeyValueCommand {
                    namespace: namespace.clone(),
                    key: (*key).to_vec(),
                    action: yyds_types::KeyValueAction::Exists,
                },
            );
            match result {
                Ok(KeyValueResult::Exists(true)) => present += 1,
                Ok(KeyValueResult::Exists(false)) => {}
                Ok(_) => {
                    write_error(stream, "EXISTS executor returned unexpected result")?;
                    return Ok(true);
                }
                Err(error) => {
                    write_error(stream, &error.to_string())?;
                    return Ok(true);
                }
            }
        }
        write!(stream, ":{present}\r\n")?;
    }
    else if command.eq_ignore_ascii_case(b"TTL") || command.eq_ignore_ascii_case(b"PTTL") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count != 2 {
            stream.write_all(b"-ERR wrong number of arguments for 'ttl' command\r\n")?;
            return Ok(true);
        }
        let key = request.arguments[1];
        let key_owned = key.to_vec();
        // Expired first: if the expiry fires, the key is treated as absent (-2).
        if purge_if_expired(state, Some(executor), &namespace.0, key) {
            let _ = state.expiries.remove(&(namespace.0.clone(), key_owned));
            stream.write_all(b":-2\r\n")?;
            return Ok(true);
        }
        let exists = matches!(
            executor.execute(
                None,
                yyds_types::KeyValueCommand {
                    namespace: namespace.clone(),
                    key: key_owned.clone(),
                    action: yyds_types::KeyValueAction::Exists,
                }
            ),
            Ok(KeyValueResult::Exists(true))
        );
        if !exists {
            stream.write_all(b":-2\r\n")?;
            return Ok(true);
        }
        // Key exists — return remaining time or -1 (no expiry).
        let value = match state.expiries.get(&(namespace.0.clone(), key_owned)) {
            Some(due) => {
                let remaining = due.saturating_duration_since(Instant::now());
                if command.eq_ignore_ascii_case(b"PTTL") {
                    remaining.as_millis() as i64
                }
                else {
                    remaining.as_secs() as i64
                }
            }
            None => -1,
        };
        write!(stream, ":{value}\r\n")?;
    }
    else if command.eq_ignore_ascii_case(b"TYPE") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count != 2 {
            stream.write_all(b"-ERR wrong number of arguments for 'type' command\r\n")?;
            return Ok(true);
        }
        let key = request.arguments[1];
        let _ = purge_if_expired(state, Some(executor), &namespace.0, key);
        let exists = matches!(
            executor.execute(
                None,
                yyds_types::KeyValueCommand {
                    namespace: namespace.clone(),
                    key: key.to_vec(),
                    action: yyds_types::KeyValueAction::Exists,
                }
            ),
            Ok(KeyValueResult::Exists(true))
        );
        if exists {
            stream.write_all(b"+string\r\n")?;
        }
        else {
            stream.write_all(b"+none\r\n")?;
        }
    }
    else if command.eq_ignore_ascii_case(b"STRLEN") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count != 2 {
            stream.write_all(b"-ERR wrong number of arguments for 'strlen' command\r\n")?;
            return Ok(true);
        }
        let key = request.arguments[1];
        let _ = purge_if_expired(state, Some(executor), &namespace.0, key);
        let value = match executor.execute(
            None,
            yyds_types::KeyValueCommand {
                namespace: namespace.clone(),
                key: key.to_vec(),
                action: yyds_types::KeyValueAction::Get,
            },
        ) {
            Ok(KeyValueResult::Get(Some(value))) => value.len() as i64,
            Ok(KeyValueResult::Get(None)) => 0,
            Ok(_) => {
                write_error(stream, "STRLEN executor returned unexpected result")?;
                return Ok(true);
            }
            Err(error) => {
                write_error(stream, &error.to_string())?;
                return Ok(true);
            }
        };
        write!(stream, ":{value}\r\n")?;
    }
    else if command.eq_ignore_ascii_case(b"APPEND") {
        let Some(executor) = executor
        else {
            stream.write_all(b"-ERR unsupported command\r\n")?;
            return Ok(true);
        };
        if count != 3 {
            stream.write_all(b"-ERR wrong number of arguments for 'append' command\r\n")?;
            return Ok(true);
        }
        let key = request.arguments[1];
        let append = request.arguments[2];
        let _ = purge_if_expired(state, Some(executor), &namespace.0, key);
        let current = match executor.execute(
            None,
            yyds_types::KeyValueCommand {
                namespace: namespace.clone(),
                key: key.to_vec(),
                action: yyds_types::KeyValueAction::Get,
            },
        ) {
            Ok(KeyValueResult::Get(Some(v))) => v,
            Ok(KeyValueResult::Get(None)) => Vec::new(),
            Ok(_) => {
                write_error(stream, "APPEND executor returned unexpected result")?;
                return Ok(true);
            }
            Err(error) => {
                write_error(stream, &error.to_string())?;
                return Ok(true);
            }
        };
        let mut merged = current;
        merged.extend_from_slice(append);
        let new_len = merged.len() as i64;
        match executor.execute(
            None,
            yyds_types::KeyValueCommand {
                namespace: namespace.clone(),
                key: key.to_vec(),
                action: yyds_types::KeyValueAction::Put(merged),
            },
        ) {
            Ok(KeyValueResult::Put { .. }) => write!(stream, ":{new_len}\r\n")?,
            Ok(_) => {
                write_error(stream, "APPEND executor returned unexpected result")?;
                return Ok(true);
            }
            Err(error) => {
                write_error(stream, &error.to_string())?;
                return Ok(true);
            }
        }
    }
    else if let Some(executor) = executor {
        // --- Generic bind()-backed commands (GET, SET, DEL, INCR, DECR, GETDEL, GETSET).
        // We first purge the key if its expiry has elapsed so the executor sees a clean state.
        let bind_key = command_to_bind_key(command, &request.arguments);
        if let Some(key) = bind_key {
            let _ = purge_if_expired(state, Some(executor), &namespace.0, key);
        }
        match bind(&request.arguments, &namespace) {
            Ok(command) => match executor.execute(None, command) {
                Ok(KeyValueResult::Get(Some(value))) => write_bulk(stream, &value)?,
                Ok(KeyValueResult::Get(None)) => stream.write_all(b"$-1\r\n")?,
                Ok(KeyValueResult::Exists(exists)) => stream.write_all(if exists { b":1\r\n" } else { b":0\r\n" })?,
                Ok(KeyValueResult::GetDelete(Some(value))) => write_bulk(stream, &value)?,
                Ok(KeyValueResult::GetDelete(None)) => stream.write_all(b"$-1\r\n")?,
                Ok(KeyValueResult::GetSet(Some(value))) => write_bulk(stream, &value)?,
                Ok(KeyValueResult::GetSet(None)) => stream.write_all(b"$-1\r\n")?,
                Ok(KeyValueResult::Put { .. }) => stream.write_all(b"+OK\r\n")?,
                Ok(KeyValueResult::PutIfAbsent { revision: Some(_) }) => stream.write_all(b"+OK\r\n")?,
                Ok(KeyValueResult::PutIfAbsent { revision: None }) => stream.write_all(b"$-1\r\n")?,
                Ok(KeyValueResult::PutIfPresent { revision: Some(_) }) => stream.write_all(b"+OK\r\n")?,
                Ok(KeyValueResult::PutIfPresent { revision: None }) => stream.write_all(b"$-1\r\n")?,
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

/// Runs every queued command under a fresh per-command `ConnectionState` so that MULTI
/// control frames cannot nest into the transaction. Each command's RESP output is captured
/// into a buffer and then emitted wrapped in a Redis multi-bulk array header.
fn execute_transaction(
    stream: &mut dyn Write,
    executor: Option<&dyn KeyValueExecutor>,
    namespace: &mut Namespace,
    state: &mut ConnectionState,
) -> io::Result<()> {
    let count = state.queue.len();
    let queued = std::mem::take(&mut state.queue);
    state.in_multi = false;

    write!(stream, "*{count}\r\n")?;

    for queued in queued {
        let owned = queued.request;
        let inner_request = owned.as_request();
        let mut buf = Vec::<u8>::new();
        let mut inner_namespace = namespace.clone();
        // Give each queued command a fresh state so nested MULTI/EXEC/DISCARD semantics
        // do not pollute the parent transaction.
        let mut inner_state = ConnectionState::new();
        // Carry over expiries so EXEC sees any key-level TTL context.
        inner_state.expiries = state.expiries.clone();
        let _ = respond(&mut buf as &mut dyn Write, &inner_request, executor, &mut inner_namespace, &mut inner_state);
        // Reconcile any expiries the inner command may have created or removed.
        state.expiries = inner_state.expiries;
        stream.write_all(&buf)?;
    }
    Ok(())
}

/// Lightweight arity / syntax check applied to every command before it is queued. Errors
/// returned here are written back to the client immediately (not deferred to EXEC). Returns
/// `Err(bytes)` with the exact RESP error to emit when syntax is wrong.
fn validate_command_for_queue(
    command: &[u8],
    count: usize,
    arguments: &[&[u8]],
    executor: Option<&dyn KeyValueExecutor>,
    namespace: &Namespace,
) -> Result<(), Vec<u8>> {
    // PING: 1 or 2 args
    if command.eq_ignore_ascii_case(b"PING") {
        if count > 2 {
            return Err(b"-ERR wrong number of arguments for 'ping' command\r\n".to_vec());
        }
        return Ok(());
    }
    // ECHO: exactly 2
    if command.eq_ignore_ascii_case(b"ECHO") {
        if count != 2 {
            return Err(b"-ERR wrong number of arguments for 'echo' command\r\n".to_vec());
        }
        return Ok(());
    }
    // QUIT: exactly 1
    if command.eq_ignore_ascii_case(b"QUIT") {
        if count != 1 {
            return Err(b"-ERR wrong number of arguments for 'quit' command\r\n".to_vec());
        }
        return Ok(());
    }
    // SELECT: exactly 2, needs executor
    if command.eq_ignore_ascii_case(b"SELECT") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count != 2 {
            return Err(b"-ERR wrong number of arguments for 'select' command\r\n".to_vec());
        }
        if parse_database(arguments[1]).is_none() {
            return Err(b"-ERR DB index is out of range\r\n".to_vec());
        }
        return Ok(());
    }
    // EXPIRE / PEXPIRE: exactly 3, need executor, integer check
    if command.eq_ignore_ascii_case(b"EXPIRE") || command.eq_ignore_ascii_case(b"PEXPIRE") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count != 3 {
            return Err(b"-ERR wrong number of arguments for 'expire' command\r\n".to_vec());
        }
        if parse_integer(arguments[2]).is_none() {
            return Err(b"-ERR value is not an integer or out of range\r\n".to_vec());
        }
        return Ok(());
    }
    // TTL / PTTL / TYPE / STRLEN: exactly 2
    if command.eq_ignore_ascii_case(b"TTL")
        || command.eq_ignore_ascii_case(b"PTTL")
        || command.eq_ignore_ascii_case(b"TYPE")
        || command.eq_ignore_ascii_case(b"STRLEN")
    {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count != 2 {
            return Err(b"-ERR wrong number of arguments\r\n".to_vec());
        }
        return Ok(());
    }
    // MGET: at least 2
    if command.eq_ignore_ascii_case(b"MGET") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count < 2 {
            return Err(b"-ERR wrong number of arguments for 'mget' command\r\n".to_vec());
        }
        return Ok(());
    }
    // EXISTS: at least 2
    if command.eq_ignore_ascii_case(b"EXISTS") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count < 2 {
            return Err(b"-ERR wrong number of arguments for 'exists' command\r\n".to_vec());
        }
        return Ok(());
    }
    // APPEND: exactly 3
    if command.eq_ignore_ascii_case(b"APPEND") {
        if executor.is_none() {
            return Err(b"-ERR unsupported command\r\n".to_vec());
        }
        if count != 3 {
            return Err(b"-ERR wrong number of arguments for 'append' command\r\n".to_vec());
        }
        return Ok(());
    }
    // Fall back to bind()'s own syntax checks.
    match bind(arguments, namespace) {
        Ok(_) => Ok(()),
        Err(BindError::WrongArity) => Err(b"-ERR wrong number of arguments\r\n".to_vec()),
        Err(BindError::UnsupportedOptions) => Err(b"-ERR unsupported SET options\r\n".to_vec()),
        Err(BindError::InvalidInteger) => Err(b"-ERR value is not an integer or out of range\r\n".to_vec()),
        Err(BindError::EmptyCommand) => Err(b"-ERR empty command\r\n".to_vec()),
        Err(BindError::UnsupportedCommand) => Err(b"-ERR unsupported command\r\n".to_vec()),
    }
}

/// Best-effort key extraction for bind()-backed commands so we can purge expired entries
/// before the executor runs. Returns the raw key bytes when present.
fn command_to_bind_key<'a>(command: &[u8], arguments: &[&'a [u8]]) -> Option<&'a [u8]> {
    if command.len() < 2 || arguments.len() < 2 {
        return None;
    }
    // All bind()-backed commands put the key at arguments[1].
    if command.eq_ignore_ascii_case(b"GET")
        || command.eq_ignore_ascii_case(b"SET")
        || command.eq_ignore_ascii_case(b"DEL")
        || command.eq_ignore_ascii_case(b"INCR")
        || command.eq_ignore_ascii_case(b"INCRBY")
        || command.eq_ignore_ascii_case(b"DECR")
        || command.eq_ignore_ascii_case(b"DECRBY")
        || command.eq_ignore_ascii_case(b"GETDEL")
        || command.eq_ignore_ascii_case(b"GETSET")
    {
        Some(arguments[1])
    }
    else {
        None
    }
}

fn parse_database(bytes: &[u8]) -> Option<u8> {
    if bytes.is_empty() || !bytes.iter().all(u8::is_ascii_digit) {
        return None;
    }
    let database = bytes.iter().try_fold(0_u8, |value, digit| value.checked_mul(10)?.checked_add(digit - b'0'))?;
    (database < 16).then_some(database)
}

fn parse_integer(bytes: &[u8]) -> Option<i64> {
    let text = std::str::from_utf8(bytes).ok()?;
    let value = text.parse::<i64>().ok()?;
    // Reject "canonical form" — Redis rejects "+1", " 1", leading-zero-but-same-string, etc.
    // For simplicity we just accept any round-tripable i64.
    if value.to_string() != text {
        return None;
    }
    Some(value)
}

fn write_error(stream: &mut dyn Write, message: &str) -> io::Result<()> {
    let sanitized = message
        .chars()
        .map(|character| {
            if matches!(character, '\r' | '\n') {
                ' '
            }
            else {
                character
            }
        })
        .take(512)
        .collect::<String>();
    write!(stream, "-ERR {sanitized}\r\n")
}

fn write_bulk(stream: &mut dyn Write, bytes: &[u8]) -> io::Result<()> {
    write!(stream, "${}\r\n", bytes.len())?;
    stream.write_all(bytes)?;
    stream.write_all(b"\r\n")
}
