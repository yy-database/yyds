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
use yyds_execution::KeyValueExecutor;
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
    let mut transaction_status = TransactionStatus::Idle;
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
    send_ready(&mut stream, transaction_status)?;

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
                    transaction_status.fail();
                    send_ready(&mut stream, transaction_status)?;
                    continue;
                };
                if sql_bytes.contains(&0) {
                    write_error_response(&mut stream, "08P01", "embedded NUL in PostgreSQL query")?;
                    transaction_status.fail();
                }
                else if let Ok(sql) = std::str::from_utf8(sql_bytes) {
                    handle_simple_query(&mut stream, sql, &mut transaction_status, runtime.as_deref())?;
                }
                else {
                    write_error_response(&mut stream, "22021", "query is not valid UTF-8")?;
                    transaction_status.fail();
                }
                send_ready(&mut stream, transaction_status)?;
            }
            _ => {
                write_error_response(&mut stream, "08P01", "unsupported PostgreSQL frontend message")?;
                send_ready(&mut stream, transaction_status)?;
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

impl TransactionStatus {
    fn fail(&mut self) {
        if *self == Self::InTransaction {
            *self = Self::Failed;
        }
    }

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
    status: &mut TransactionStatus,
    runtime: Option<&PgsqlSqlRuntime>,
) -> io::Result<()> {
    match parse_session_command(sql) {
        Err(_) => {
            status.fail();
            write_error_response(stream, "42601", "Oak could not parse the SQL statement")
        }
        Ok(Some(SqlSessionCommand::Transaction(action))) => match action {
            SqlTransactionAction::Begin if *status == TransactionStatus::Failed => write_error_response(
                stream,
                "25P02",
                "current transaction is aborted, commands ignored until end of transaction block",
            ),
            SqlTransactionAction::Begin => {
                *status = TransactionStatus::InTransaction;
                write_command_complete(stream, "BEGIN")
            }
            SqlTransactionAction::Commit if *status == TransactionStatus::Failed => {
                *status = TransactionStatus::Idle;
                write_command_complete(stream, "ROLLBACK")
            }
            SqlTransactionAction::Commit => {
                *status = TransactionStatus::Idle;
                write_command_complete(stream, "COMMIT")
            }
            SqlTransactionAction::Rollback => {
                *status = TransactionStatus::Idle;
                write_command_complete(stream, "ROLLBACK")
            }
        },
        Ok(Some(SqlSessionCommand::SetNames { .. })) | Ok(None) => {
            if *status == TransactionStatus::Failed {
                return write_error_response(
                    stream,
                    "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block",
                );
            }
            match runtime {
                Some(rt) => execute_simple_query(stream, sql, rt, status),
                None => {
                    status.fail();
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
    runtime: &PgsqlSqlRuntime,
    status: &mut TransactionStatus,
) -> io::Result<()> {
    let bound = match bind_sql_catalog(sql, &runtime.catalog, runtime.schema_version) {
        Ok(b) => b,
        Err(error) => {
            status.fail();
            return write_error_response(stream, "42P01", &format!("SQL bind error: {}", error.message));
        }
    };
    let plan = match plan_statement(bound, &runtime.catalog) {
        Ok(p) => p,
        Err(error) => {
            status.fail();
            return write_error_response(stream, "42883", &format!("SQL plan error: {}", error.message));
        }
    };
    let is_read = matches!(&plan, SqlExecutionPlan::Read(_));
    let result = match execute_plan(runtime.executor.as_ref(), &plan) {
        Ok(r) => r,
        Err(error) => {
            status.fail();
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
        let tag = match &plan {
            SqlExecutionPlan::Insert(_) => "INSERT 0 1",
            SqlExecutionPlan::Update(_) => "UPDATE 1",
            SqlExecutionPlan::Delete(_) => "DELETE 1",
            SqlExecutionPlan::Read(_) => unreachable!(),
        };
        write_command_complete(stream, tag)?;
    }
    Ok(())
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

fn send_ready(stream: &mut impl Write, status: TransactionStatus) -> io::Result<()> {
    write_message(stream, b'Z', &[status.ready_byte()])
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
