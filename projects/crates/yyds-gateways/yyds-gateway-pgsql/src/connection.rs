//! PostgreSQL protocol-v3 connection establishment and explicit query rejection.

use std::{
    io::{self, Read, Write},
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, AtomicU32, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use crate::wire::{decode_message, decode_startup, encode_message, FrameError, StartupParameterError, StartupParameters};
use yyds_execution::{KeyValueExecutor, LocalTransactionHandle, TransactionHandle};
use yyds_gateway::{
    bind_sql_catalog, execute_plan, parse_session_command, plan_statement, SqlCatalog, SqlExecutionPlan, SqlSessionCommand,
    SqlTransactionAction, SqlValue,
};

const PROTOCOL_V3: u32 = 196_608;
const SSL_REQUEST: u32 = 80_877_103;
const GSSENC_REQUEST: u32 = 80_877_104;
const CANCEL_REQUEST: u32 = 80_877_102;
const MAX_STARTUP_FRAME: usize = 64 * 1024;
const MAX_MESSAGE_FRAME: usize = 16 * 1024 * 1024;
static NEXT_SECRET: AtomicU32 = AtomicU32::new(1);

/// Injects YYDS SQL execution into PostgreSQL SimpleQuery.
#[derive(Debug)]
pub struct PgsqlSqlRuntime {
    /// Bound YYDS catalog view used for SQL name resolution.
    pub catalog: SqlCatalog,
    /// Schema version expected by the gateway binder.
    pub schema_version: u32,
    /// KV execution plane.
    pub executor: Arc<dyn KeyValueExecutor>,
}

/// Establishes an unauthenticated protocol-v3 session, then rejects unsupported SQL explicitly.
///
/// The caller must restrict the peer to loopback until authentication and TLS are implemented.
pub fn serve_connection(stream: TcpStream, runtime: Option<Arc<PgsqlSqlRuntime>>) -> io::Result<()> {
    if !stream.peer_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "PostgreSQL requires loopback until authentication and TLS are implemented",
        ));
    }
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    serve_session(stream, runtime)
}

/// Serves a loopback session with cooperative service-stop cancellation.
pub fn serve_cancellable(
    stream: TcpStream,
    stopping: Arc<AtomicBool>,
    runtime: Option<Arc<PgsqlSqlRuntime>>,
) -> io::Result<()> {
    if !stream.peer_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "PostgreSQL requires loopback until authentication and TLS are implemented",
        ));
    }
    stream.set_nonblocking(true)?;
    serve_session(CancellableSocket { stream, stopping }, runtime)
}

fn serve_session(mut stream: impl Read + Write, runtime: Option<Arc<PgsqlSqlRuntime>>) -> io::Result<()> {
    let mut pending = Vec::new();
    let mut session = TransactionSession::new();
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
    send_ready(&mut stream, &session)?;

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
                    session.fail();
                    send_ready(&mut stream, &session)?;
                    continue;
                };
                if sql_bytes.contains(&0) {
                    write_error_response(&mut stream, "08P01", "embedded NUL in PostgreSQL query")?;
                    session.fail();
                }
                else if let Ok(sql) = std::str::from_utf8(sql_bytes) {
                    handle_simple_query(&mut stream, sql, &mut session, runtime.as_ref())?;
                }
                else {
                    write_error_response(&mut stream, "22021", "query is not valid UTF-8")?;
                    session.fail();
                }
                send_ready(&mut stream, &session)?;
            }
            _ => {
                write_error_response(&mut stream, "08P01", "unsupported PostgreSQL frontend message")?;
                send_ready(&mut stream, &session)?;
            }
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum TransactionStatus {
    Idle,
    InTransaction,
    Failed,
}

/// Session-scoped transaction state: the PostgreSQL wire protocol ReadyForQuery byte
/// plus a real storage transaction handle that writes and row reads are bound to.
struct TransactionSession {
    /// The executor Arc stays alive for as long as a transaction is open so the handle's
    /// internal references remain valid.
    executor: Option<Arc<dyn KeyValueExecutor>>,
    handle: Option<Box<dyn TransactionHandle + 'static>>,
    /// Tracks whether we are inside a transaction block even without a runtime handle
    /// (e.g. the PostgreSQL protocol test).
    protocol_only_transaction: bool,
    failed: bool,
}

impl TransactionSession {
    fn new() -> Self {
        Self { executor: None, handle: None, protocol_only_transaction: false, failed: false }
    }

    fn as_status(&self) -> TransactionStatus {
        if self.failed {
            TransactionStatus::Failed
        }
        else if self.handle.is_some() || self.protocol_only_transaction {
            TransactionStatus::InTransaction
        }
        else {
            TransactionStatus::Idle
        }
    }

    fn fail(&mut self) {
        if self.handle.is_some() || self.protocol_only_transaction {
            self.failed = true;
        }
    }

    fn in_transaction(&self) -> bool {
        self.handle.is_some() || self.protocol_only_transaction
    }

    fn ready_byte(&self) -> u8 {
        self.as_status().ready_byte()
    }
}

impl Default for TransactionSession {
    fn default() -> Self {
        Self::new()
    }
}

impl TransactionStatus {
    fn ready_byte(self) -> u8 {
        match self {
            Self::Idle => b'I',
            Self::InTransaction => b'T',
            Self::Failed => b'E',
        }
    }
}

fn handle_simple_query(
    stream: &mut impl Write,
    sql: &str,
    session: &mut TransactionSession,
    runtime: Option<&Arc<PgsqlSqlRuntime>>,
) -> io::Result<()> {
    match parse_session_command(sql) {
        Err(_) => {
            session.fail();
            write_error_response(stream, "42601", "Oak could not parse the SQL statement")
        }
        Ok(Some(SqlSessionCommand::Transaction(action))) => match action {
            SqlTransactionAction::Begin if session.failed => write_error_response(
                stream,
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block",
            ),
            SqlTransactionAction::Begin => {
                match runtime {
                    Some(rt) => {
                        // The gateway constructs the handle directly because the generic
                        // `KeyValueExecutor::begin_transaction` default returns `Unsupported`.
                        let executor = rt.executor.clone();
                        let handle = LocalTransactionHandle::new(executor.clone());
                        session.executor = Some(executor);
                        session.handle = Some(Box::new(handle));
                        session.protocol_only_transaction = false;
                        session.failed = false;
                    }
                    None => {
                        // No runtime — stay in protocol-only mode. execute_plan will receive
                        // `None` for the handle and go through the executor directly.
                        session.executor = None;
                        session.handle = None;
                        session.protocol_only_transaction = true;
                        session.failed = false;
                    }
                }
                write_command_complete(stream, "BEGIN")
            }
            SqlTransactionAction::Commit if session.failed => {
                if let Some(handle) = session.handle.take() {
                    handle.rollback();
                }
                session.executor = None;
                session.handle = None;
                session.protocol_only_transaction = false;
                session.failed = false;
                write_command_complete(stream, "ROLLBACK")
            }
            SqlTransactionAction::Commit => {
                if let Some(handle) = session.handle.take() {
                    if let Err(err) = handle.commit() {
                        write_error_response(stream, "58P01", &format!("COMMIT failed: {err}"))?;
                        return Ok(());
                    }
                }
                session.executor = None;
                session.handle = None;
                session.protocol_only_transaction = false;
                session.failed = false;
                write_command_complete(stream, "COMMIT")
            }
            SqlTransactionAction::Rollback => {
                if let Some(handle) = session.handle.take() {
                    handle.rollback();
                }
                session.executor = None;
                session.handle = None;
                session.protocol_only_transaction = false;
                session.failed = false;
                write_command_complete(stream, "ROLLBACK")
            }
        },
        Ok(Some(SqlSessionCommand::SetVariable { .. })) => {
            if session.failed {
                return write_error_response(
                    stream,
                    "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block",
                );
            }
            write_command_complete(stream, "SET")
        }
        Ok(Some(SqlSessionCommand::ShowParameter { name, .. })) => {
            if session.failed {
                return write_error_response(
                    stream,
                    "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block",
                );
            }
            match pg_parameter(&name) {
                Some(value) => {
                    write_row_description(stream, &[name.clone()])?;
                    write_data_row(stream, &[SqlValue::String(value.to_string())])?;
                    write_command_complete(stream, "SHOW")
                }
                None => {
                    session.fail();
                    write_error_response(stream, "42704", &format!("unrecognized configuration parameter \"{name}\""))
                }
            }
        }
        Ok(Some(SqlSessionCommand::ShowVariables { .. })) => {
            session.fail();
            write_error_response(stream, "42601", "SHOW VARIABLES is MySQL syntax, PostgreSQL uses SHOW <parameter>")
        }
        Ok(Some(SqlSessionCommand::Pragma { .. })) => {
            session.fail();
            write_error_response(stream, "42601", "PRAGMA is not PostgreSQL syntax")
        }
        Ok(Some(SqlSessionCommand::SetNames { .. })) | Ok(None) => {
            if session.failed {
                return write_error_response(
                    stream,
                    "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block",
                );
            }
            match runtime {
                Some(rt) => execute_simple_query(stream, sql, rt, session),
                None => {
                    session.fail();
                    write_error_response(stream, "0A000", "PostgreSQL query execution is not implemented (no SQL runtime)")
                }
            }
        }
    }
}

/// SimpleQuery 的 SQL bind → plan → execute 路径。
fn execute_simple_query(
    stream: &mut impl Write,
    sql: &str,
    runtime: &Arc<PgsqlSqlRuntime>,
    session: &mut TransactionSession,
) -> io::Result<()> {
    let bound = match bind_sql_catalog(sql, &runtime.catalog, runtime.schema_version) {
        Ok(b) => b,
        Err(error) => {
            session.fail();
            return write_error_response(stream, "42P01", &format!("SQL bind error: {}", error.message));
        }
    };
    let plan = match plan_statement(bound, &runtime.catalog) {
        Ok(p) => p,
        Err(error) => {
            session.fail();
            return write_error_response(stream, "42883", &format!("SQL plan error: {}", error.message));
        }
    };
    let is_read = matches!(&plan, SqlExecutionPlan::Read(_) | SqlExecutionPlan::Scan(_) | SqlExecutionPlan::Scalar(_));
    let handle_ref: Option<&dyn TransactionHandle> = session.handle.as_deref();
    let result = match execute_plan(runtime.executor.as_ref(), handle_ref, &plan) {
        Ok(r) => r,
        Err(error) => {
            session.fail();
            return write_error_response(stream, "58P01", &format!("SQL execution error: {}", error.message));
        }
    };
    if is_read {
        write_row_description(stream, &result.columns)?;
        for row in &result.rows {
            write_data_row(stream, row)?;
        }
        write_command_complete(stream, &format!("SELECT {}", result.rows.len()))?;
    }
    else {
        let affected = result.affected_rows;
        let tag = match &plan {
            SqlExecutionPlan::Insert(_) => format!("INSERT 0 {}", affected),
            SqlExecutionPlan::Update(_) => format!("UPDATE {}", affected),
            SqlExecutionPlan::Delete(_) => format!("DELETE {}", affected),
            SqlExecutionPlan::Read(_) | SqlExecutionPlan::Scan(_) | SqlExecutionPlan::Scalar(_) => unreachable!(),
        };
        write_command_complete(stream, &tag)?;
    }
    Ok(())
}

/// Emulated PostgreSQL configuration parameters exposed through `SHOW <name>`.
const PG_PARAMETERS: &[(&str, &str)] = &[
    ("application_name", ""),
    ("client_encoding", "UTF8"),
    ("client_min_messages", "notice"),
    ("datestyle", "ISO, MDY"),
    ("default_transaction_isolation", "read committed"),
    ("extra_float_digits", "3"),
    ("integer_datetimes", "on"),
    ("intervalstyle", "postgres"),
    ("server_encoding", "UTF8"),
    ("server_version", "16.4"),
    ("search_path", "yy_public"),
    ("standard_conforming_strings", "on"),
    ("timezone", "UTC"),
    ("transaction_isolation", "read committed"),
];

/// Case-insensitive lookup in [`PG_PARAMETERS`].
fn pg_parameter(name: &str) -> Option<&'static str> {
    let needle = name.trim().to_ascii_lowercase();
    PG_PARAMETERS.iter().find(|(key, _)| *key == needle).map(|(_, value)| *value)
}

/// PostgreSQL RowDescription message ('T') — name only，所有列 text 格式。
fn write_row_description(stream: &mut impl Write, columns: &[String]) -> io::Result<()> {
    let mut payload = Vec::with_capacity(64);
    payload.push(b'T');
    let mut body = Vec::with_capacity(48);
    body.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for col in columns {
        body.extend_from_slice(col.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_i32.to_be_bytes()); // table_oid = 0
        body.extend_from_slice(&0_i16.to_be_bytes()); // column_attr = 0
        body.extend_from_slice(&0_i32.to_be_bytes()); // type_oid = unspecified
        body.extend_from_slice(&(-1_i16).to_be_bytes()); // type_size = variable
        body.extend_from_slice(&(-1_i32).to_be_bytes()); // type_mod = -1
        body.extend_from_slice(&0_i16.to_be_bytes()); // format_code = text
    }
    payload.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    payload.extend_from_slice(&body);
    stream.write_all(&payload)
}

/// PostgreSQL DataRow message ('D') — text encoding for each column value。
fn write_data_row(stream: &mut impl Write, row: &[SqlValue]) -> io::Result<()> {
    let mut payload = Vec::with_capacity(64);
    payload.push(b'D');
    let mut body = Vec::with_capacity(48);
    body.extend_from_slice(&(row.len() as i16).to_be_bytes());
    for value in row {
        match value {
            SqlValue::Null => body.extend_from_slice(&(-1_i32).to_be_bytes()),
            other => {
                let text = sql_value_to_pg_text(other);
                let bytes = text.as_bytes();
                body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
                body.extend_from_slice(bytes);
            }
        }
    }
    payload.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    payload.extend_from_slice(&body);
    stream.write_all(&payload)
}

fn sql_value_to_pg_text(value: &SqlValue) -> String {
    match value {
        SqlValue::Number(n) => format!("{n}"),
        SqlValue::String(s) => s.clone(),
        SqlValue::Boolean(b) => {
            if *b {
                "t".into()
            }
            else {
                "f".into()
            }
        }
        SqlValue::Null => unreachable!(),
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

fn send_ready(stream: &mut impl Write, session: &TransactionSession) -> io::Result<()> {
    write_message(stream, b'Z', &[session.ready_byte()])
}

fn write_command_complete(stream: &mut impl Write, tag: &str) -> io::Result<()> {
    let mut payload = Vec::with_capacity(tag.len() + 1);
    payload.extend_from_slice(tag.as_bytes());
    payload.push(0);
    write_message(stream, b'C', &payload)
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
