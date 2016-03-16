//! PostgreSQL protocol-v3 connection establishment and explicit query rejection.

use std::{
    collections::HashMap,
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
    bind_sql_catalog, describe_plan, execute_plan, neutralized_sql_parameters, parse_session_command,
    plan_statement_for_dialect, substitute_sql_parameters, SqlBoundExpression, SqlBoundLiteral, SqlCatalog,
    SqlCatalogBoundStatement, SqlColumn, SqlDialect, SqlErrorKind, SqlExecutionPlan, SqlFieldType, SqlFrontendError,
    SqlParameterStyle, SqlSessionCommand, SqlTransactionAction, SqlValue,
};

/// PostgreSQL type OIDs supported by the gateway.
const OID_BOOL: u32 = 16;
const OID_BYTEA: u32 = 17;
const OID_NAME: u32 = 19;
const OID_INT8: u32 = 20;
const OID_INT2: u32 = 21;
const OID_INT4: u32 = 23;
const OID_TEXT: u32 = 25;
const OID_FLOAT4: u32 = 700;
const OID_FLOAT8: u32 = 701;
const OID_VARCHAR: u32 = 1043;

/// Reported PostgreSQL version — kept identical in the startup `server_version`
/// parameter and `SHOW server_version` so the two sources agree.
const PG_SERVER_VERSION: &str = "16.4";

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
    send_parameter(&mut stream, "server_version", PG_SERVER_VERSION)?;
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

    let mut extended = ExtendedSession::default();
    let mut extended_failed = false;

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
            tag @ (b'P' | b'B' | b'D' | b'E' | b'C' | b'H' | b'S') => {
                if extended_failed && !matches!(tag, b'S' | b'H') {
                    // 扩展查询出错后，PostgreSQL 会丢弃同一批次的后续消息直到 Sync。
                    continue;
                }
                let step = match tag {
                    b'P' => handle_parse(&mut stream, &message.payload, &mut extended),
                    b'B' => handle_bind(&mut stream, &message.payload, &mut extended, runtime.as_ref()),
                    b'D' => handle_describe(&mut stream, &message.payload, &mut extended, &mut session, runtime.as_ref()),
                    b'E' => handle_execute(&mut stream, &message.payload, &mut extended, &mut session, runtime.as_ref()),
                    b'C' => handle_close(&mut stream, &message.payload, &mut extended),
                    b'H' => stream.flush().map(|_| ExtendedStep::Continue),
                    b'S' => {
                        extended_failed = false;
                        send_ready(&mut stream, &session).map(|_| ExtendedStep::Continue)
                    }
                    _ => Ok(ExtendedStep::Continue),
                }?;
                if step == ExtendedStep::Failed {
                    extended_failed = true;
                    session.fail();
                }
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
/// Also holds session-scoped GUC overrides that `SET` mutates and `SHOW` queries.
struct TransactionSession {
    /// The executor Arc stays alive for as long as a transaction is open so the handle's
    /// internal references remain valid.
    executor: Option<Arc<dyn KeyValueExecutor>>,
    handle: Option<Box<dyn TransactionHandle + 'static>>,
    /// Tracks whether we are inside a transaction block even without a runtime handle
    /// (e.g. the PostgreSQL protocol test).
    protocol_only_transaction: bool,
    failed: bool,
    /// `search_path` as last set via `SET search_path = ...`. Defaults to `"public"`.
    search_path: String,
}

impl TransactionSession {
    fn new() -> Self {
        Self {
            executor: None,
            handle: None,
            protocol_only_transaction: false,
            failed: false,
            search_path: "public".to_string(),
        }
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
                        let state = match &err {
                            yyds_types::Error::CasConflict { .. } => "40001",
                            _ => "58P01",
                        };
                        write_error_response(stream, state, &format!("COMMIT failed: {err}"))?;
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
        Ok(Some(SqlSessionCommand::SetVariable { name, value, .. })) => {
            if session.failed {
                return write_error_response(
                    stream,
                    "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block",
                );
            }
            let needle = name.trim().to_ascii_lowercase();
            match needle.as_str() {
                "search_path" => match value {
                    SqlBoundExpression::Literal(SqlBoundLiteral::String(path)) => {
                        session.search_path = path.clone();
                    }
                    _ => {
                        session.fail();
                        return write_error_response(
                            stream,
                            "22P02",
                            &format!("invalid value for parameter \"search_path\": expected string literal"),
                        );
                    }
                },
                _ => {
                    if pg_parameter(&name).is_none() {
                        session.fail();
                        return write_error_response(
                            stream,
                            "42704",
                            &format!("unrecognized configuration parameter \"{name}\""),
                        );
                    }
                    // 其他 PG_PARAMETERS 已知参数接受为 no-op（gateway 不模拟写入）
                }
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
            let needle = name.trim().to_ascii_lowercase();
            let value: Option<String> = if needle == "search_path" {
                Some(session.search_path.clone())
            }
            else {
                pg_parameter(&name).map(|s| s.to_string())
            };
            match value {
                Some(value) => {
                    write_row_description(stream, &[SqlColumn { name: name.clone(), field_type: SqlFieldType::String }], &[0])?;
                    write_data_row(
                        stream,
                        &[SqlValue::String(value)],
                        &[SqlColumn { name: name.clone(), field_type: SqlFieldType::String }],
                        &[0],
                    )?;
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
    match run_statement(sql, runtime, session) {
        Ok(outcome) => write_execution_outcome(stream, &outcome),
        Err((code, message)) => write_error_response(stream, &code, &message),
    }
}

/// 执行结果的统一形状，供 SimpleQuery 与扩展查询共用。
struct ExecutionOutcome {
    is_read: bool,
    description: Vec<SqlColumn>,
    rows: Vec<Vec<SqlValue>>,
    command_tag: String,
}

/// bind → plan → execute，错误已映射为 SQLSTATE，并已标记事务失败。
fn run_statement(
    sql: &str,
    runtime: &Arc<PgsqlSqlRuntime>,
    session: &mut TransactionSession,
) -> Result<ExecutionOutcome, (String, String)> {
    // 扩展查询不经过 `handle_simple_query`，语法错误必须在这里映射为 `42601`
    // 而不是被后面的 catalog 绑定错误统一归为 `42P01`。
    if parse_session_command(sql).is_err() {
        session.fail();
        return Err(("42601".to_string(), "Oak could not parse the SQL statement".to_string()));
    }
    let bound = bind_sql_catalog(sql, &runtime.catalog, runtime.schema_version).map_err(|error| {
        session.fail();
        statement_error("bind", error)
    })?;
    let plan = plan_statement_for_dialect(bound, &runtime.catalog, SqlDialect::Postgres).map_err(|error| {
        session.fail();
        statement_error("plan", error)
    })?;
    let is_read = matches!(
        &plan,
        SqlExecutionPlan::Read(_)
            | SqlExecutionPlan::Scan(_)
            | SqlExecutionPlan::Join(_)
            | SqlExecutionPlan::Scalar(_)
            | SqlExecutionPlan::SetOperation(_)
    );
    // Derive the result shape from the catalog plan so an empty result set
    // reports the declared type instead of falling back to `text`.
    let description = describe_plan(&plan);
    let handle_ref: Option<&dyn TransactionHandle> = session.handle.as_deref();
    let result = execute_plan(runtime.executor.as_ref(), handle_ref, &plan, SqlDialect::Postgres).map_err(|error| {
        session.fail();
        statement_error("execution", error)
    })?;
    let command_tag = if is_read {
        format!("SELECT {}", result.rows.len())
    }
    else {
        let affected = result.affected_rows;
        match &plan {
            SqlExecutionPlan::Insert(_) | SqlExecutionPlan::MultiInsert(_) | SqlExecutionPlan::InsertSelect(_) => {
                format!("INSERT 0 {affected}")
            }
            SqlExecutionPlan::Update(_) | SqlExecutionPlan::UpdateScan(_) => format!("UPDATE {affected}"),
            SqlExecutionPlan::Delete(_) | SqlExecutionPlan::DeleteScan(_) => format!("DELETE {affected}"),
            SqlExecutionPlan::Read(_)
            | SqlExecutionPlan::Scan(_)
            | SqlExecutionPlan::Join(_)
            | SqlExecutionPlan::Scalar(_)
            | SqlExecutionPlan::SetOperation(_) => {
                format!("SELECT {}", result.rows.len())
            }
        }
    };
    debug_assert_eq!(description.len(), result.columns.len(), "described columns must match executed columns");
    Ok(ExecutionOutcome { is_read, description, rows: result.rows, command_tag })
}

/// Maps a structured gateway failure to its PostgreSQL SQLSTATE. Protocol code
/// never inspects diagnostic text to recover the category.
fn pg_sqlstate(kind: SqlErrorKind) -> &'static str {
    use SqlErrorKind::*;
    match kind {
        Syntax => "42601",
        UnknownTable | UnknownTableQualifier => "42P01",
        AmbiguousTable | AmbiguousColumn => "42702",
        UnknownColumn => "42703",
        TypeMismatch => "42804",
        NotNullViolation => "23502",
        Unsupported => "0A000",
        UnknownFunction => "42883",
        UnknownVariable => "42704",
        Catalog => "XX000",
        Parameter => "08P01",
        UniqueViolation => "23505",
        Conflict => "40001",
        Execution => "XX000",
        DivisionByZero => "22012",
    }
}

fn statement_error(stage: &str, error: SqlFrontendError) -> (String, String) {
    (pg_sqlstate(error.kind).to_string(), format!("SQL {stage} error: {}", error.message))
}

fn write_execution_outcome(stream: &mut impl Write, outcome: &ExecutionOutcome) -> io::Result<()> {
    if outcome.is_read {
        let formats = vec![0_i16; outcome.description.len()];
        write_row_description(stream, &outcome.description, &formats)?;
        for row in &outcome.rows {
            write_data_row(stream, row, &outcome.description, &formats)?;
        }
    }
    write_command_complete(stream, &outcome.command_tag)
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ExtendedStep {
    Continue,
    Failed,
}

/// 扩展查询协议的会话状态：命名/匿名 statement 与 portal。
#[derive(Default)]
struct ExtendedSession {
    statements: HashMap<String, PreparedStatement>,
    portals: HashMap<String, Portal>,
}

struct PreparedStatement {
    sql: String,
    parameter_oids: Vec<u32>,
    /// Catalog-derived result shape, computed once on the first `Describe(S)`
    /// or `Bind`. Writes describe to an empty shape.
    description: Option<Vec<SqlColumn>>,
}

struct Portal {
    sql: String,
    parameters: Vec<SqlValue>,
    description: Vec<SqlColumn>,
    /// Requested result formats: empty means all text, one entry applies to all
    /// columns, otherwise one entry per described column.
    result_formats: Vec<i16>,
    outcome: Option<Result<ExecutionOutcome, (String, String)>>,
    sent_rows: usize,
}

fn handle_parse(stream: &mut impl Write, payload: &[u8], extended: &mut ExtendedSession) -> io::Result<ExtendedStep> {
    let mut cursor = 0;
    let (Some(name), Some(sql)) = (take_cstring(payload, &mut cursor), take_cstring(payload, &mut cursor))
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Parse message");
    };
    let Some(parameter_count) = take_i16(payload, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Parse message");
    };
    if parameter_count < 0 {
        return extended_protocol_error(stream, "08P01", "negative PostgreSQL parameter count");
    }
    let mut parameter_oids = Vec::with_capacity(parameter_count as usize);
    for _ in 0..parameter_count {
        let Some(oid) = take_u32(payload, &mut cursor)
        else {
            return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Parse parameter types");
        };
        parameter_oids.push(oid);
    }
    extended.statements.insert(name.to_string(), PreparedStatement { sql: sql.to_string(), parameter_oids, description: None });
    write_message(stream, b'1', &[])?;
    Ok(ExtendedStep::Continue)
}

fn handle_bind(
    stream: &mut impl Write,
    payload: &[u8],
    extended: &mut ExtendedSession,
    runtime: Option<&Arc<PgsqlSqlRuntime>>,
) -> io::Result<ExtendedStep> {
    let mut cursor = 0;
    let (Some(portal_name), Some(statement_name)) = (take_cstring(payload, &mut cursor), take_cstring(payload, &mut cursor))
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind message");
    };
    let Some(statement) = extended.statements.get_mut(statement_name)
    else {
        return extended_protocol_error(stream, "26000", &format!("prepared statement \"{statement_name}\" does not exist"));
    };
    let sql = statement.sql.clone();
    let parameter_oids = statement.parameter_oids.clone();

    let Some(format_count) = take_i16(payload, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind message");
    };
    let mut parameter_formats = Vec::with_capacity(format_count.max(0) as usize);
    for _ in 0..format_count.max(0) {
        let Some(format) = take_i16(payload, &mut cursor)
        else {
            return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind format codes");
        };
        if format != 0 && format != 1 {
            return extended_protocol_error(stream, "08P01", &format!("unsupported PostgreSQL format code {format}"));
        }
        parameter_formats.push(format);
    }

    let Some(parameter_count) = take_i16(payload, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind message");
    };
    if parameter_formats.len() > 1 && parameter_formats.len() != parameter_count.max(0) as usize {
        return extended_protocol_error(stream, "08P01", "PostgreSQL parameter format count does not match parameters");
    }
    let mut parameters = Vec::with_capacity(parameter_count.max(0) as usize);
    for index in 0..parameter_count.max(0) {
        let Some(length) = take_i32(payload, &mut cursor)
        else {
            return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind parameter length");
        };
        if length < 0 {
            parameters.push(SqlValue::Null);
            continue;
        }
        let Some(bytes) = take_bytes(payload, &mut cursor, length as usize)
        else {
            return extended_protocol_error(stream, "08P01", "truncated PostgreSQL Bind parameter");
        };
        let oid = parameter_oids.get(index as usize).copied().unwrap_or(0);
        let format = match parameter_formats.len() {
            0 => 0,
            1 => parameter_formats[0],
            _ => parameter_formats[index as usize],
        };
        let decoded = if format == 1 { binary_parameter_value(oid, bytes) } else { parameter_value(oid, bytes) };
        match decoded {
            Ok(value) => parameters.push(value),
            Err((code, message)) => return extended_protocol_error(stream, &code, &message),
        }
    }

    let Some(result_format_count) = take_i16(payload, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind result formats");
    };
    let mut result_formats = Vec::with_capacity(result_format_count.max(0) as usize);
    for _ in 0..result_format_count.max(0) {
        let Some(format) = take_i16(payload, &mut cursor)
        else {
            return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Bind result formats");
        };
        if format != 0 && format != 1 {
            return extended_protocol_error(stream, "08P01", &format!("unsupported PostgreSQL format code {format}"));
        }
        result_formats.push(format);
    }

    if statement.description.is_none() {
        match statement_description(&sql, runtime) {
            Ok(description) => statement.description = Some(description),
            Err((code, message)) => return extended_protocol_error(stream, &code, &message),
        }
    }
    let description = statement.description.clone().unwrap_or_default();
    if result_formats.len() > 1 && result_formats.len() != description.len() {
        return extended_protocol_error(stream, "08P01", "PostgreSQL result format count does not match columns");
    }

    extended
        .portals
        .insert(portal_name.to_string(), Portal { sql, parameters, description, result_formats, outcome: None, sent_rows: 0 });
    write_message(stream, b'2', &[])?;
    Ok(ExtendedStep::Continue)
}

fn handle_describe(
    stream: &mut impl Write,
    payload: &[u8],
    extended: &mut ExtendedSession,
    session: &mut TransactionSession,
    runtime: Option<&Arc<PgsqlSqlRuntime>>,
) -> io::Result<ExtendedStep> {
    let Some((&kind, rest)) = payload.split_first()
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Describe message");
    };
    let mut cursor = 0;
    let Some(name) = take_cstring(rest, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Describe target");
    };
    match kind {
        b'S' => {
            let Some(statement) = extended.statements.get_mut(name)
            else {
                return extended_protocol_error(stream, "26000", &format!("prepared statement \"{name}\" does not exist"));
            };
            let parameters = statement_parameter_description(&statement.sql, &statement.parameter_oids);
            if statement.description.is_none() {
                match statement_description(&statement.sql, runtime) {
                    Ok(description) => statement.description = Some(description),
                    Err((code, message)) => {
                        write_error_response(stream, &code, &message)?;
                        return Ok(ExtendedStep::Failed);
                    }
                }
            }
            write_parameter_description(stream, &parameters)?;
            match statement.description.as_deref() {
                Some([]) | None => write_message(stream, b'n', &[])?,
                Some(description) => {
                    let formats = vec![0_i16; description.len()];
                    write_row_description(stream, description, &formats)?;
                }
            }
            Ok(ExtendedStep::Continue)
        }
        b'P' => {
            // Describe(portal) reports shape only. Execution belongs to Execute.
            let Some(portal) = extended.portals.get(name)
            else {
                return extended_protocol_error(stream, "34000", &format!("portal \"{name}\" does not exist"));
            };
            if session.failed {
                write_error_response(
                    stream,
                    "25P02",
                    "current transaction is aborted, commands ignored until end of transaction block",
                )?;
                return Ok(ExtendedStep::Failed);
            }
            if portal.description.is_empty() {
                write_message(stream, b'n', &[])?;
            }
            else {
                let formats = resolve_formats(&portal.result_formats, portal.description.len());
                write_row_description(stream, &portal.description, &formats)?;
            }
            Ok(ExtendedStep::Continue)
        }
        _ => extended_protocol_error(stream, "08P01", "unsupported PostgreSQL Describe target"),
    }
}

/// Number of placeholders the SQL surface references, padding declared
/// parameter OIDs with zero (`unspecified`) when the client sent fewer.
fn statement_parameter_description(sql: &str, declared: &[u32]) -> Vec<u32> {
    let referenced = neutralized_sql_parameters(sql, SqlParameterStyle::DollarNumber).map(|(_, count)| count).unwrap_or(0);
    let mut parameters = declared.to_vec();
    parameters.resize(parameters.len().max(referenced), 0);
    parameters
}

/// Resolves Bind result formats into one entry per described column.
fn resolve_formats(formats: &[i16], columns: usize) -> Vec<i16> {
    match formats.len() {
        0 => vec![0; columns],
        1 => vec![formats[0]; columns],
        _ => formats.to_vec(),
    }
}

/// Derives the statement result shape from the catalog without executing.
fn statement_description(sql: &str, runtime: Option<&Arc<PgsqlSqlRuntime>>) -> Result<Vec<SqlColumn>, (String, String)> {
    let Some(runtime) = runtime
    else {
        return Err(("0A000".to_string(), "PostgreSQL query execution is not implemented (no SQL runtime)".to_string()));
    };
    let (neutral_sql, _) = neutralized_sql_parameters(sql, SqlParameterStyle::DollarNumber)
        .map_err(|error| statement_error("parameter", error))?;
    let bound = bind_sql_catalog(&neutral_sql, &runtime.catalog, runtime.schema_version)
        .map_err(|error| statement_error("bind", error))?;
    if !matches!(bound, SqlCatalogBoundStatement::Select(_) | SqlCatalogBoundStatement::SetOperation(_)) {
        return Ok(Vec::new());
    }
    let plan = plan_statement_for_dialect(bound, &runtime.catalog, SqlDialect::Postgres)
        .map_err(|error| statement_error("plan", error))?;
    Ok(describe_plan(&plan))
}

fn handle_execute(
    stream: &mut impl Write,
    payload: &[u8],
    extended: &mut ExtendedSession,
    session: &mut TransactionSession,
    runtime: Option<&Arc<PgsqlSqlRuntime>>,
) -> io::Result<ExtendedStep> {
    let mut cursor = 0;
    let Some(name) = take_cstring(payload, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Execute message");
    };
    let Some(max_rows) = take_i32(payload, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Execute row limit");
    };
    let Some(portal) = extended.portals.get_mut(name)
    else {
        return extended_protocol_error(stream, "34000", &format!("portal \"{name}\" does not exist"));
    };
    prepare_portal(portal, session, runtime);
    let Some(outcome) = portal.outcome.as_ref()
    else {
        unreachable!("prepare_portal always stores an outcome");
    };
    match outcome {
        Ok(outcome) => {
            let command_tag = outcome.command_tag.clone();
            if outcome.is_read {
                let start = portal.sent_rows.min(outcome.rows.len());
                let end = if max_rows > 0 { (start + max_rows as usize).min(outcome.rows.len()) } else { outcome.rows.len() };
                let formats = resolve_formats(&portal.result_formats, outcome.description.len());
                let description = outcome.description.clone();
                let chunk: Vec<Vec<SqlValue>> = outcome.rows[start..end].to_vec();
                portal.sent_rows = end;
                for row in &chunk {
                    write_data_row(stream, row, &description, &formats)?;
                }
                if end < outcome.rows.len() {
                    write_message(stream, b's', &[])?;
                    return Ok(ExtendedStep::Continue);
                }
            }
            write_command_complete(stream, &command_tag)?;
            Ok(ExtendedStep::Continue)
        }
        Err((code, message)) => {
            write_error_response(stream, code, message)?;
            Ok(ExtendedStep::Failed)
        }
    }
}

fn handle_close(stream: &mut impl Write, payload: &[u8], extended: &mut ExtendedSession) -> io::Result<ExtendedStep> {
    let Some((&kind, rest)) = payload.split_first()
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Close message");
    };
    let mut cursor = 0;
    let Some(name) = take_cstring(rest, &mut cursor)
    else {
        return extended_protocol_error(stream, "08P01", "malformed PostgreSQL Close target");
    };
    match kind {
        b'S' => {
            extended.statements.remove(name);
        }
        b'P' => {
            extended.portals.remove(name);
        }
        _ => return extended_protocol_error(stream, "08P01", "unsupported PostgreSQL Close target"),
    }
    write_message(stream, b'3', &[])?;
    Ok(ExtendedStep::Continue)
}

/// Bind 只登记参数，真正执行推迟到 Describe / Execute，结果缓存避免重复写入。
fn prepare_portal(portal: &mut Portal, session: &mut TransactionSession, runtime: Option<&Arc<PgsqlSqlRuntime>>) {
    if portal.outcome.is_some() {
        return;
    }
    if session.failed {
        portal.outcome = Some(Err((
            "25P02".to_string(),
            "current transaction is aborted, commands ignored until end of transaction block".to_string(),
        )));
        return;
    }
    let Some(runtime) = runtime
    else {
        session.fail();
        portal.outcome =
            Some(Err(("0A000".to_string(), "PostgreSQL query execution is not implemented (no SQL runtime)".to_string())));
        return;
    };
    let sql = match substitute_sql_parameters(&portal.sql, &portal.parameters, SqlParameterStyle::DollarNumber) {
        Ok(sql) => sql,
        Err(error) => {
            session.fail();
            portal.outcome = Some(Err(statement_error("parameter", error)));
            return;
        }
    };
    portal.outcome = Some(run_statement(&sql, runtime, session));
}

fn parameter_value(oid: u32, bytes: &[u8]) -> Result<SqlValue, (String, String)> {
    let text = std::str::from_utf8(bytes).map_err(|_| ("22021".to_string(), "parameter is not valid UTF-8".to_string()))?;
    match oid {
        0 => Ok(infer_parameter_value(text)),
        16 => match text {
            "t" | "true" | "1" => Ok(SqlValue::Boolean(true)),
            "f" | "false" | "0" => Ok(SqlValue::Boolean(false)),
            _ => Err(("22P02".to_string(), format!("invalid input syntax for type boolean: \"{text}\""))),
        },
        20 | 21 | 23 | 26 => text
            .parse::<i64>()
            .map(SqlValue::Integer)
            .map_err(|_| ("22P02".to_string(), format!("invalid input syntax for integer: \"{text}\""))),
        700 | 701 => text
            .parse::<f64>()
            .map(SqlValue::Number)
            .map_err(|_| ("22P02".to_string(), format!("invalid input syntax for double precision: \"{text}\""))),
        _ => Ok(SqlValue::String(text.to_string())),
    }
}

/// Decodes a binary-format Bind parameter by its declared OID. An unspecified
/// OID cannot be decoded without text syntax and is rejected explicitly.
fn binary_parameter_value(oid: u32, bytes: &[u8]) -> Result<SqlValue, (String, String)> {
    let width_error = |expected: usize| {
        ("08P01".to_string(), format!("binary parameter for type OID {oid} expects {expected} bytes, got {}", bytes.len()))
    };
    match oid {
        0 => Err(("42P18".to_string(), "cannot determine data type of a binary parameter".to_string())),
        OID_BOOL => match bytes {
            [0] => Ok(SqlValue::Boolean(false)),
            [1] => Ok(SqlValue::Boolean(true)),
            _ => Err(width_error(1)),
        },
        OID_INT2 => match bytes {
            [a, b] => Ok(SqlValue::Integer(i16::from_be_bytes([*a, *b]).into())),
            _ => Err(width_error(2)),
        },
        OID_INT4 => match bytes {
            [a, b, c, d] => Ok(SqlValue::Integer(i32::from_be_bytes([*a, *b, *c, *d]).into())),
            _ => Err(width_error(4)),
        },
        OID_INT8 => match bytes {
            [a, b, c, d, e, f, g, h] => Ok(SqlValue::Integer(i64::from_be_bytes([*a, *b, *c, *d, *e, *f, *g, *h]))),
            _ => Err(width_error(8)),
        },
        OID_FLOAT4 => match bytes {
            [a, b, c, d] => Ok(SqlValue::Number(f32::from_be_bytes([*a, *b, *c, *d]).into())),
            _ => Err(width_error(4)),
        },
        OID_FLOAT8 => match bytes {
            [a, b, c, d, e, f, g, h] => Ok(SqlValue::Number(f64::from_be_bytes([*a, *b, *c, *d, *e, *f, *g, *h]))),
            _ => Err(width_error(8)),
        },
        OID_TEXT | OID_VARCHAR | OID_NAME | OID_BYTEA => std::str::from_utf8(bytes)
            .map(|text| SqlValue::String(text.to_string()))
            .map_err(|_| ("22021".to_string(), "binary parameter is not valid UTF-8".to_string())),
        _ => Err(("0A000".to_string(), format!("binary input for parameter type OID {oid} is not supported"))),
    }
}

/// Parse 未提供 OID 时的宽松推断，保持 `"007"` 这类文本不被数字转义。
fn infer_parameter_value(text: &str) -> SqlValue {
    if text.parse::<i64>().map(|value| value.to_string() == text).unwrap_or(false) {
        return SqlValue::Integer(text.parse().expect("checked integer"));
    }
    if (text.contains('.') || text.contains('e') || text.contains('E')) && text.parse::<f64>().is_ok() {
        return SqlValue::Number(text.parse().expect("checked float"));
    }
    SqlValue::String(text.to_string())
}

fn write_parameter_description(stream: &mut impl Write, oids: &[u32]) -> io::Result<()> {
    let mut payload = Vec::with_capacity(2 + oids.len() * 4);
    payload.extend_from_slice(&(oids.len() as i16).to_be_bytes());
    for oid in oids {
        payload.extend_from_slice(&oid.to_be_bytes());
    }
    write_message(stream, b't', &payload)
}

fn extended_protocol_error(stream: &mut impl Write, code: &str, message: &str) -> io::Result<ExtendedStep> {
    write_error_response(stream, code, message)?;
    Ok(ExtendedStep::Failed)
}

fn take_bytes<'a>(payload: &'a [u8], cursor: &mut usize, length: usize) -> Option<&'a [u8]> {
    let end = cursor.checked_add(length)?;
    let bytes = payload.get(*cursor..end)?;
    *cursor = end;
    Some(bytes)
}

fn take_cstring<'a>(payload: &'a [u8], cursor: &mut usize) -> Option<&'a str> {
    let rest = payload.get(*cursor..)?;
    let position = rest.iter().position(|byte| *byte == 0)?;
    let bytes = &rest[..position];
    *cursor += position + 1;
    std::str::from_utf8(bytes).ok()
}

fn take_i16(payload: &[u8], cursor: &mut usize) -> Option<i16> {
    let bytes = take_bytes(payload, cursor, 2)?;
    Some(i16::from_be_bytes([bytes[0], bytes[1]]))
}

fn take_i32(payload: &[u8], cursor: &mut usize) -> Option<i32> {
    let bytes = take_bytes(payload, cursor, 4)?;
    Some(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn take_u32(payload: &[u8], cursor: &mut usize) -> Option<u32> {
    let bytes = take_bytes(payload, cursor, 4)?;
    Some(u32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
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
    ("server_version", PG_SERVER_VERSION),
    ("search_path", "public"),
    ("standard_conforming_strings", "on"),
    ("timezone", "UTC"),
    ("transaction_isolation", "read committed"),
];

/// Case-insensitive lookup in [`PG_PARAMETERS`].
fn pg_parameter(name: &str) -> Option<&'static str> {
    let needle = name.trim().to_ascii_lowercase();
    PG_PARAMETERS.iter().find(|(key, _)| *key == needle).map(|(_, value)| *value)
}

/// PostgreSQL type OID for a described gateway field type.
fn pg_oid(field_type: &SqlFieldType) -> u32 {
    match field_type.inner() {
        SqlFieldType::Int32 => OID_INT4,
        SqlFieldType::Int64 => OID_INT8,
        SqlFieldType::Float64 => OID_FLOAT8,
        SqlFieldType::Bool => OID_BOOL,
        SqlFieldType::Bytes => OID_BYTEA,
        SqlFieldType::String | SqlFieldType::Unknown | SqlFieldType::Optional(_) => OID_TEXT,
    }
}

/// PostgreSQL RowDescription message ('T') — catalog-derived OIDs and the
/// format codes requested at Bind.
fn write_row_description(stream: &mut impl Write, columns: &[SqlColumn], formats: &[i16]) -> io::Result<()> {
    let mut payload = Vec::with_capacity(64);
    payload.push(b'T');
    let mut body = Vec::with_capacity(48);
    body.extend_from_slice(&(columns.len() as i16).to_be_bytes());
    for (index, column) in columns.iter().enumerate() {
        let oid = pg_oid(&column.field_type);
        body.extend_from_slice(column.name.as_bytes());
        body.push(0);
        body.extend_from_slice(&0_i32.to_be_bytes()); // table_oid = 0
        body.extend_from_slice(&0_i16.to_be_bytes()); // column_attr = 0
        body.extend_from_slice(&oid.to_be_bytes());
        body.extend_from_slice(&type_size(oid).to_be_bytes());
        body.extend_from_slice(&(-1_i32).to_be_bytes()); // type_mod = -1
        body.extend_from_slice(&formats.get(index).copied().unwrap_or(0).to_be_bytes());
    }
    payload.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    payload.extend_from_slice(&body);
    stream.write_all(&payload)
}

fn type_size(oid: u32) -> i16 {
    match oid {
        OID_BOOL => 1,
        OID_INT2 => 2,
        OID_INT4 | OID_FLOAT4 => 4,
        OID_INT8 | OID_FLOAT8 => 8,
        _ => -1,
    }
}

/// PostgreSQL DataRow message ('D') — text or binary encoding per column.
fn write_data_row(stream: &mut impl Write, row: &[SqlValue], columns: &[SqlColumn], formats: &[i16]) -> io::Result<()> {
    let mut payload = Vec::with_capacity(64);
    payload.push(b'D');
    let mut body = Vec::with_capacity(48);
    body.extend_from_slice(&(row.len() as i16).to_be_bytes());
    for (index, value) in row.iter().enumerate() {
        if matches!(value, SqlValue::Null) {
            body.extend_from_slice(&(-1_i32).to_be_bytes());
            continue;
        }
        let oid = columns.get(index).map(|column| pg_oid(&column.field_type)).unwrap_or(OID_TEXT);
        let bytes = if formats.get(index).copied().unwrap_or(0) == 1 {
            match pg_binary_value(oid, value) {
                Ok(bytes) => bytes,
                Err((code, message)) => {
                    return Err(io::Error::new(io::ErrorKind::InvalidData, format!("{code}: {message}")));
                }
            }
        }
        else {
            sql_value_to_pg_text(value).into_bytes()
        };
        body.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
        body.extend_from_slice(&bytes);
    }
    payload.extend_from_slice(&(body.len() as i32 + 4).to_be_bytes());
    payload.extend_from_slice(&body);
    stream.write_all(&payload)
}

/// Encodes a value in the big-endian binary representation of its column OID.
fn pg_binary_value(oid: u32, value: &SqlValue) -> Result<Vec<u8>, (String, String)> {
    match oid {
        OID_BOOL => Ok(vec![u8::from(truthy_value(value)?)]),
        OID_INT2 => {
            let value = integer_value(value)?;
            let narrow = i16::try_from(value).map_err(|_| ("22003".to_string(), format!("smallint out of range: {value}")))?;
            Ok(narrow.to_be_bytes().to_vec())
        }
        OID_INT4 => {
            let value = integer_value(value)?;
            let narrow = i32::try_from(value).map_err(|_| ("22003".to_string(), format!("integer out of range: {value}")))?;
            Ok(narrow.to_be_bytes().to_vec())
        }
        OID_INT8 => Ok(integer_value(value)?.to_be_bytes().to_vec()),
        OID_FLOAT4 => Ok((float_value(value)? as f32).to_be_bytes().to_vec()),
        OID_FLOAT8 => Ok(float_value(value)?.to_be_bytes().to_vec()),
        OID_TEXT | OID_VARCHAR | OID_NAME => Ok(sql_value_to_pg_text(value).into_bytes()),
        OID_BYTEA => match value {
            SqlValue::String(text) => Ok(text.as_bytes().to_vec()),
            other => Ok(sql_value_to_pg_text(other).into_bytes()),
        },
        _ => Err(("0A000".to_string(), format!("binary output for type OID {oid} is not supported"))),
    }
}

fn integer_value(value: &SqlValue) -> Result<i64, (String, String)> {
    match value {
        SqlValue::Integer(value) => Ok(*value),
        SqlValue::Boolean(value) => Ok(i64::from(*value)),
        SqlValue::Number(value) if value.fract() == 0.0 => Ok(*value as i64),
        SqlValue::String(text) => {
            text.parse::<i64>().map_err(|_| ("22P02".to_string(), format!("invalid input syntax for integer: \"{text}\"")))
        }
        other => Err(("22P02".to_string(), format!("value {other:?} is not a binary integer"))),
    }
}

fn float_value(value: &SqlValue) -> Result<f64, (String, String)> {
    match value {
        SqlValue::Integer(value) => Ok(*value as f64),
        SqlValue::Number(value) => Ok(*value),
        SqlValue::Boolean(value) => Ok(if *value { 1.0 } else { 0.0 }),
        SqlValue::String(text) => text
            .parse::<f64>()
            .map_err(|_| ("22P02".to_string(), format!("invalid input syntax for double precision: \"{text}\""))),
        other => Err(("22P02".to_string(), format!("value {other:?} is not a binary float"))),
    }
}

fn truthy_value(value: &SqlValue) -> Result<bool, (String, String)> {
    match value {
        SqlValue::Boolean(value) => Ok(*value),
        SqlValue::Integer(value) => Ok(*value != 0),
        SqlValue::String(text) => match text.as_str() {
            "t" | "true" | "1" => Ok(true),
            "f" | "false" | "0" => Ok(false),
            _ => Err(("22P02".to_string(), format!("invalid input syntax for type boolean: \"{text}\""))),
        },
        other => Err(("22P02".to_string(), format!("value {other:?} is not a binary boolean"))),
    }
}

fn sql_value_to_pg_text(value: &SqlValue) -> String {
    match value {
        SqlValue::Integer(n) => n.to_string(),
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
