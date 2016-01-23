//! Bounded loopback MySQL protocol-v10 startup and SQL execution.

use std::{
    io::{self, Read, Write},
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use yyds_execution::{KeyValueExecutor, LocalTransactionHandle, TransactionHandle};
use yyds_gateway::{
    bind_sql_catalog_with_variables, execute_plan, parse_session_command, plan_statement, SqlBoundExpression, SqlBoundLiteral,
    SqlCatalog, SqlExecutionPlan, SqlFrontendError, SqlResult, SqlSessionCommand, SqlTransactionAction, SqlValue,
};

use crate::wire::{encode_packet, PacketError, MAX_PACKET_PAYLOAD};

const SERVER_CAPABILITIES: u32 =
    0x0000_0001 | 0x0000_0004 | 0x0000_0008 | 0x0000_0200 | 0x0000_2000 | 0x0000_8000 | 0x0002_0000 | 0x0008_0000;
const CLIENT_PROTOCOL_41: u32 = 0x0000_0200;
const CLIENT_SSL: u32 = 0x0000_0800;
const CLIENT_SECURE_CONNECTION: u32 = 0x0000_8000;
const CLIENT_PLUGIN_AUTH: u32 = 0x0008_0000;
const CLIENT_PLUGIN_AUTH_LENENC: u32 = 0x0020_0000;
const CLIENT_CONNECT_WITH_DB: u32 = 0x0000_0008;
const MAX_PACKET: usize = 1024 * 1024;

/// Emulated `SHOW VARIABLES` table. Values describe what this gateway actually
/// implements: no strict mode, a single-node engine, and utf8mb4 text.
const MYSQL_VARIABLES: &[(&str, &str)] = &[
    ("autocommit", "ON"),
    ("character_set_client", "utf8mb4"),
    ("character_set_connection", "utf8mb4"),
    ("character_set_results", "utf8mb4"),
    ("character_set_server", "utf8mb4"),
    ("collation_connection", "utf8mb4_general_ci"),
    ("collation_server", "utf8mb4_general_ci"),
    ("init_connect", ""),
    ("lower_case_table_names", "0"),
    ("max_allowed_packet", "67108864"),
    ("net_write_timeout", "60"),
    ("sql_mode", ""),
    ("time_zone", "SYSTEM"),
    ("transaction_isolation", "READ-COMMITTED"),
    ("tx_isolation", "READ-COMMITTED"),
    ("version", "8.0.36-yyds"),
    ("version_comment", "YYDS MySQL gateway"),
    ("wait_timeout", "28800"),
];

/// Runtime dependencies injected into a MySQL connection so SQL SELECT can execute.
#[derive(Debug)]
pub struct MysqlSqlRuntime {
    /// Bound YYDS catalog view used for SQL name resolution.
    pub catalog: SqlCatalog,
    /// Schema version expected by the gateway binder.
    pub schema_version: u32,
    /// KV execution plane.
    pub executor: Arc<dyn KeyValueExecutor>,
}

/// Resolves a `SELECT @@version` style system-variable probe against [`MYSQL_VARIABLES`].
///
/// The scope prefix (`@@global.` / `@@session.` / `@@local.`) is accepted and
/// ignored: the gateway keeps one immutable variable table for every scope.
/// Numeric-looking values are returned as numbers so drivers reading
/// `@@max_allowed_packet` or `@@lower_case_table_names` get integer semantics.
fn mysql_system_variable(name: &str) -> Result<SqlBoundLiteral, SqlFrontendError> {
    let bare = name.strip_prefix("@@").unwrap_or(name);
    let variable = bare.rsplit('.').next().unwrap_or(bare).to_ascii_lowercase();
    let value = MYSQL_VARIABLES
        .iter()
        .find(|(candidate, _)| *candidate == variable)
        .map(|(_, value)| *value)
        .ok_or_else(|| SqlFrontendError { message: format!("Unknown system variable '{variable}'").into() })?;
    if !value.is_empty() && value.chars().all(|ch| ch.is_ascii_digit()) {
        if let Ok(number) = value.parse::<i64>() {
            return Ok(SqlBoundLiteral::Integer(number));
        }
    }
    Ok(SqlBoundLiteral::String(value.to_string()))
}

/// Performs a loopback-only unauthenticated startup and executes SQL against the provided runtime.
pub fn serve_connection(stream: TcpStream, runtime: Option<Arc<MysqlSqlRuntime>>) -> io::Result<()> {
    if !stream.peer_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "MySQL requires loopback until authentication and TLS are implemented",
        ));
    }
    stream.set_read_timeout(Some(Duration::from_secs(30)))?;
    stream.set_write_timeout(Some(Duration::from_secs(30)))?;
    serve_session(stream, runtime)
}

/// Serves a loopback session that can be interrupted by the owning node.
pub fn serve_cancellable(
    stream: TcpStream,
    stopping: Arc<AtomicBool>,
    runtime: Option<Arc<MysqlSqlRuntime>>,
) -> io::Result<()> {
    if !stream.peer_addr()?.ip().is_loopback() {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "MySQL requires loopback until authentication and TLS are implemented",
        ));
    }
    stream.set_nonblocking(true)?;
    serve_session(CancellableSocket { stream, stopping }, runtime)
}

/// MySQL session-scoped transaction state. Unlike PostgreSQL there is no wire-protocol
/// ReadyForQuery byte for transaction mode, so we only track the storage handle and failure.
struct MysqlTransactionSession {
    executor: Option<Arc<dyn KeyValueExecutor>>,
    handle: Option<Box<dyn TransactionHandle + 'static>>,
    failed: bool,
}

impl MysqlTransactionSession {
    fn new() -> Self {
        Self { executor: None, handle: None, failed: false }
    }

    fn fail(&mut self) {
        if self.handle.is_some() {
            self.failed = true;
        }
    }
}

impl Default for MysqlTransactionSession {
    fn default() -> Self {
        Self::new()
    }
}

fn serve_session(mut stream: impl Read + Write, runtime: Option<Arc<MysqlSqlRuntime>>) -> io::Result<()> {
    let mut session = MysqlTransactionSession::new();
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
                match parse_session_command(sql) {
                    Ok(Some(SqlSessionCommand::SetNames { character_set, .. }))
                        if !character_set.eq_ignore_ascii_case("utf8mb4") =>
                    {
                        write_error(&mut stream, sequence.wrapping_add(1), 1115, "42000", "unknown character set")?;
                    }
                    Ok(Some(SqlSessionCommand::SetNames { collation, .. }))
                        if collation
                            .as_deref()
                            .is_none_or(|collation| collation.eq_ignore_ascii_case("utf8mb4_general_ci")) =>
                    {
                        write_ok(&mut stream, sequence.wrapping_add(1))?;
                    }
                    Ok(Some(SqlSessionCommand::SetNames { .. })) => {
                        write_error(&mut stream, sequence.wrapping_add(1), 1273, "HY000", "unsupported utf8mb4 collation")?;
                    }
                    Ok(Some(SqlSessionCommand::Transaction(action))) => match action {
                        SqlTransactionAction::Begin if session.failed => {
                            write_error(
                                &mut stream,
                                sequence.wrapping_add(1),
                                40001,
                                "HY000",
                                "current transaction is aborted",
                            )?;
                        }
                        SqlTransactionAction::Begin => {
                            let Some(rt) = runtime.as_ref()
                            else {
                                write_error(&mut stream, sequence.wrapping_add(1), 1235, "42000", "no SQL runtime")?;
                                continue;
                            };
                            let executor = rt.executor.clone();
                            let handle = LocalTransactionHandle::new(executor.clone());
                            session.executor = Some(executor);
                            session.handle = Some(Box::new(handle));
                            session.failed = false;
                            write_ok(&mut stream, sequence.wrapping_add(1))?;
                        }
                        SqlTransactionAction::Commit if session.failed => {
                            if let Some(handle) = session.handle.take() {
                                handle.rollback();
                            }
                            session.executor = None;
                            session.failed = false;
                            write_ok(&mut stream, sequence.wrapping_add(1))?;
                        }
                        SqlTransactionAction::Commit => {
                            let mut commit_error = None;
                            if let Some(handle) = session.handle.take() {
                                if let Err(err) = handle.commit() {
                                    commit_error = Some(err);
                                }
                            }
                            session.executor = None;
                            session.failed = false;
                            match commit_error {
                                // OCC 拒绝必须回报给客户端，否则并发写会被静默丢弃。
                                Some(err @ yyds_types::Error::CasConflict { .. }) => write_error(
                                    &mut stream,
                                    sequence.wrapping_add(1),
                                    1213,
                                    "40001",
                                    &format!("COMMIT failed: serialization conflict: {err}"),
                                )?,
                                Some(err) => write_error(
                                    &mut stream,
                                    sequence.wrapping_add(1),
                                    1105,
                                    "HY000",
                                    &format!("COMMIT failed: {err}"),
                                )?,
                                None => write_ok(&mut stream, sequence.wrapping_add(1))?,
                            }
                        }
                        SqlTransactionAction::Rollback => {
                            if let Some(handle) = session.handle.take() {
                                handle.rollback();
                            }
                            session.executor = None;
                            session.failed = false;
                            write_ok(&mut stream, sequence.wrapping_add(1))?;
                        }
                    },
                    Ok(Some(SqlSessionCommand::ShowVariables { pattern, .. })) => {
                        let mut rows = Vec::new();
                        for (name, value) in MYSQL_VARIABLES {
                            if pattern.as_deref().is_none_or(|pattern| like_matches(pattern, name)) {
                                rows.push(vec![SqlValue::String((*name).to_string()), SqlValue::String((*value).to_string())]);
                            }
                        }
                        let result =
                            SqlResult { columns: vec!["Variable_name".into(), "Value".into()], rows, affected_rows: 0 };
                        write_result_set(&mut stream, sequence.wrapping_add(1), &result)?;
                    }
                    Ok(Some(SqlSessionCommand::SetVariable { name, value, .. })) => {
                        // 引擎没有隐式事务：每条语句立即提交，因此 `autocommit=0` 会造成
                        // 客户端误判事务边界，必须显式拒绝而不是静默接受。
                        if name.eq_ignore_ascii_case("autocommit") && !variable_truthy(&value) {
                            session.fail();
                            write_error(
                                &mut stream,
                                sequence.wrapping_add(1),
                                1235,
                                "42000",
                                "autocommit=0 is not supported: statements commit immediately",
                            )?;
                        }
                        else {
                            write_ok(&mut stream, sequence.wrapping_add(1))?;
                        }
                    }
                    Ok(Some(SqlSessionCommand::ShowParameter { name, .. })) => {
                        session.fail();
                        write_error(
                            &mut stream,
                            sequence.wrapping_add(1),
                            1064,
                            "42000",
                            &format!("MySQL requires SHOW VARIABLES instead of SHOW {name}"),
                        )?;
                    }
                    Ok(Some(SqlSessionCommand::Pragma { .. })) => {
                        session.fail();
                        write_error(&mut stream, sequence.wrapping_add(1), 1064, "42000", "PRAGMA is not MySQL syntax")?;
                    }
                    Err(_) => {
                        // 语法错误与“未接入执行器”必须是不同错误，否则没有 runtime 时
                        // 客户端会把语法错误误判为不支持的能力。
                        session.fail();
                        write_error(
                            &mut stream,
                            sequence.wrapping_add(1),
                            1064,
                            "42000",
                            "Oak could not parse the SQL statement",
                        )?;
                    }
                    Ok(None) => {
                        if session.failed {
                            write_error(
                                &mut stream,
                                sequence.wrapping_add(1),
                                40001,
                                "HY000",
                                "current transaction is aborted",
                            )?;
                            continue;
                        }
                        match runtime.as_ref() {
                            Some(rt) => execute_sql(&mut stream, sequence.wrapping_add(1), sql, rt, &mut session)?,
                            None => write_error(
                                &mut stream,
                                sequence.wrapping_add(1),
                                1235,
                                "42000",
                                "YYDS MySQL SQL execution is not implemented (no SQL runtime)",
                            )?,
                        }
                    }
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

/// 把一条 SQL 走完整的 bind → plan → execute → wire 路径。
/// 根据 plan variant 分发：SELECT → ResultSet；INSERT/UPDATE/DELETE → OK(AffectedRows=1)。
fn execute_sql(
    stream: &mut impl Write,
    sequence: u8,
    sql: &str,
    runtime: &Arc<MysqlSqlRuntime>,
    session: &mut MysqlTransactionSession,
) -> io::Result<()> {
    let bound = match bind_sql_catalog_with_variables(sql, &runtime.catalog, runtime.schema_version, &mysql_system_variable) {
        Ok(bound) => bound,
        Err(error) => {
            session.fail();
            write_error(stream, sequence, 1064, "42000", &format!("SQL bind error: {}", error.message))?;
            return Ok(());
        }
    };
    let plan = match plan_statement(bound, &runtime.catalog) {
        Ok(plan) => plan,
        Err(error) => {
            session.fail();
            write_error(stream, sequence, 1235, "42000", &format!("SQL plan error: {}", error.message))?;
            return Ok(());
        }
    };

    // DML 写操作 —— 先 execute 再回 OK packet
    let is_write = !matches!(
        plan,
        SqlExecutionPlan::Read(_) | SqlExecutionPlan::Scan(_) | SqlExecutionPlan::Join(_) | SqlExecutionPlan::Scalar(_)
    );
    let handle_ref: Option<&dyn TransactionHandle> = session.handle.as_deref();
    let result = match execute_plan(runtime.executor.as_ref(), handle_ref, &plan) {
        Ok(result) => result,
        Err(error) => {
            session.fail();
            write_error(stream, sequence, 5000, "HY000", &format!("SQL execution error: {}", error.message))?;
            return Ok(());
        }
    };
    if is_write {
        write_ok_with_affected(stream, sequence, result.affected_rows)?;
        return Ok(());
    }
    write_result_set(stream, sequence, &result)
}

/// 带 affected_rows 的 MySQL OK packet（INSERT/UPDATE/DELETE）。
fn write_ok_with_affected(stream: &mut impl Write, sequence: u8, affected: u64) -> io::Result<()> {
    let mut payload = Vec::with_capacity(11);
    payload.push(0x00); // OK identifier
    encode_length_encoded(&mut payload, affected);
    payload.extend_from_slice(&0_u64.to_le_bytes()); // last_insert_id = 0
    payload.extend_from_slice(&0x0002_u16.to_le_bytes()); // status flags (AUTOCOMMIT)
    write_packet(stream, sequence, &payload)
}

/// 把 SqlResult 编码成 MySQL text ResultSet 协议包。
fn write_result_set(stream: &mut impl Write, mut sequence: u8, result: &SqlResult) -> io::Result<()> {
    // 包 1: column count
    let count = result.columns.len();
    let mut col_count = Vec::new();
    encode_length_encoded(&mut col_count, count as u64);
    write_packet(stream, sequence, &col_count)?;
    sequence = sequence.wrapping_add(1);

    // 包 2..N: column definitions
    for (index, col) in result.columns.iter().enumerate() {
        let def = encode_column_definition(col, column_type(&result.rows, index));
        write_packet(stream, sequence, &def)?;
        sequence = sequence.wrapping_add(1);
    }

    // EOF after columns
    write_eof(stream, sequence)?;
    sequence = sequence.wrapping_add(1);

    // Row packets
    for row in &result.rows {
        let mut payload = Vec::new();
        for value in row {
            encode_sql_value(&mut payload, value);
        }
        write_packet(stream, sequence, &payload)?;
        sequence = sequence.wrapping_add(1);
    }

    // EOF after rows
    write_eof(stream, sequence)
}

/// 按首批非空值推断 MySQL 列类型：`LONGLONG`、`DOUBLE`、`TINY`、`VAR_STRING`。
fn column_type(rows: &[Vec<SqlValue>], index: usize) -> u8 {
    rows.iter()
        .find_map(|row| match row.get(index) {
            Some(SqlValue::Integer(_)) => Some(0x08), // MYSQL_TYPE_LONGLONG
            Some(SqlValue::Number(_)) => Some(0x05),  // MYSQL_TYPE_DOUBLE
            Some(SqlValue::Boolean(_)) => Some(0x01), // MYSQL_TYPE_TINY
            Some(SqlValue::String(_)) => Some(0xFD),  // MYSQL_TYPE_VAR_STRING
            Some(SqlValue::Null) | None => None,
        })
        .unwrap_or(0xFD)
}

fn encode_column_definition(name: &str, column_type: u8) -> Vec<u8> {
    let mut out = Vec::new();
    encode_length_encoded(&mut out, b"def".len() as u64);
    out.extend_from_slice(b"def");
    encode_length_encoded(&mut out, 0); // schema
    encode_length_encoded(&mut out, 0); // table
    encode_length_encoded(&mut out, 0); // org_table
    encode_length_encoded(&mut out, name.len() as u64);
    out.extend_from_slice(name.as_bytes());
    encode_length_encoded(&mut out, 0); // org_name
    out.push(0x0C); // fixed length
    out.extend_from_slice(&0x000C_u16.to_le_bytes()); // charset = utf8_bin
    out.extend_from_slice(&255_u32.to_le_bytes()); // column_length
    out.push(column_type);
    out.extend_from_slice(&0_u16.to_le_bytes()); // flags
    out.push(if column_type == 0x05 { 31 } else { 0 }); // decimals
    out.extend_from_slice(&[0u8; 2]); // filler
    out
}

fn encode_sql_value(out: &mut Vec<u8>, value: &SqlValue) {
    match value {
        SqlValue::Null => out.push(0xFB),
        SqlValue::Integer(n) => {
            let text = n.to_string();
            encode_length_encoded(out, text.len() as u64);
            out.extend_from_slice(text.as_bytes());
        }
        SqlValue::Number(n) => {
            let text = format!("{n}");
            encode_length_encoded(out, text.len() as u64);
            out.extend_from_slice(text.as_bytes());
        }
        SqlValue::String(s) => {
            encode_length_encoded(out, s.len() as u64);
            out.extend_from_slice(s.as_bytes());
        }
        SqlValue::Boolean(b) => {
            let text = if *b { "1" } else { "0" };
            encode_length_encoded(out, text.len() as u64);
            out.extend_from_slice(text.as_bytes());
        }
    }
}

fn encode_length_encoded(out: &mut Vec<u8>, value: u64) {
    match value {
        0..=250 => out.push(value as u8),
        251 => out.extend_from_slice(&[252, 0, 0]),
        252..=0xFFFF => {
            out.push(252);
            out.extend_from_slice(&(value as u16).to_le_bytes());
        }
        0x10000..=0xFFFF_FFFF => {
            out.push(253);
            out.extend_from_slice(&(value as u32).to_le_bytes());
        }
        _ => {
            out.push(254);
            out.extend_from_slice(&value.to_le_bytes());
        }
    }
}

fn write_eof(stream: &mut impl Write, sequence: u8) -> io::Result<()> {
    // 简化 EOF packet: 0xFE + 0x0000 (warnings) + 0x0200 (status: SERVER_STATUS_AUTOCOMMIT)
    write_packet(stream, sequence, &[0xFE, 0x00, 0x00, 0x02, 0x00])
}

/// MySQL `LIKE` matching for `SHOW VARIABLES LIKE 'pattern'` — `%` matches any
/// run of bytes and `_` matches one byte, case-insensitively.
fn like_matches(pattern: &str, value: &str) -> bool {
    fn walk(pattern: &[u8], value: &[u8]) -> bool {
        match pattern.first() {
            None => value.is_empty(),
            Some(b'%') => walk(&pattern[1..], value) || (!value.is_empty() && walk(pattern, &value[1..])),
            Some(b'_') => !value.is_empty() && walk(&pattern[1..], &value[1..]),
            Some(byte) => {
                value.first().is_some_and(|candidate| candidate.eq_ignore_ascii_case(byte)) && walk(&pattern[1..], &value[1..])
            }
        }
    }
    walk(pattern.to_ascii_lowercase().as_bytes(), value.to_ascii_lowercase().as_bytes())
}

/// Whether a `SET` value means "on" in MySQL boolean spelling.
fn variable_truthy(value: &SqlBoundExpression) -> bool {
    fn word(text: &str) -> bool {
        matches!(text.trim().to_ascii_lowercase().as_str(), "1" | "on" | "true" | "yes" | "default")
    }
    match value {
        SqlBoundExpression::Literal(SqlBoundLiteral::Boolean(flag)) => *flag,
        SqlBoundExpression::Literal(SqlBoundLiteral::Integer(number)) => *number != 0,
        SqlBoundExpression::Literal(SqlBoundLiteral::Number(number)) => word(number),
        SqlBoundExpression::Literal(SqlBoundLiteral::String(text)) => word(text),
        SqlBoundExpression::Identifier(name) => word(name),
        _ => true,
    }
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
