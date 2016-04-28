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
        serve_connection(stream, None).unwrap();
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
fn session_variable_statements_follow_postgresql_semantics() {
    let (mut client, server) = start_server();

    send_query(&mut client, "SET client_encoding = 'UTF8'");
    assert_eq!(receive_until_ready(&mut client), vec![(b'C', b"SET\0".to_vec()), (b'Z', b"I".to_vec())]);

    // `SHOW server_version` must surface the crate version from the gateway,
    // not a hard-coded PostgreSQL release tag.
    send_query(&mut client, "SHOW server_version");
    let shown = receive_until_ready(&mut client);
    assert_eq!(shown.len(), 4);
    assert_eq!(shown[0].0, b'T');
    assert_eq!(shown[1].0, b'D');
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(
        text.contains("YYDS ") && text.contains(env!("CARGO_PKG_VERSION")),
        "server_version must carry the YYDS crate version, got {text}"
    );
    assert_eq!(shown[2], (b'C', b"SHOW\0".to_vec()));
    assert_eq!(shown[3], (b'Z', b"I".to_vec()));

    // `SHOW search_path` defaults to PostgreSQL convention `"$user, public"`.
    send_query(&mut client, "SHOW search_path");
    let shown = receive_until_ready(&mut client);
    assert_eq!(shown[1].0, b'D');
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(
        text.contains("$user") && text.contains("public"),
        "default search_path must include $user and public, got {text}"
    );

    // `SET search_path` mutates the session; a subsequent `SHOW` must reflect it.
    send_query(&mut client, "SET search_path = 'public'");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SET\0".to_vec()));
    send_query(&mut client, "SHOW search_path");
    let shown = receive_until_ready(&mut client);
    assert_eq!(shown[1].0, b'D');
    let text = String::from_utf8_lossy(&shown[1].1);
    assert!(
        text.contains("public"),
        "after SET search_path = 'public', SHOW must echo it, got {text}"
    );

    // MySQL 专有语法在 PostgreSQL 下必须报语法错误
    send_query(&mut client, "SHOW VARIABLES");
    let mysql_only = receive_until_ready(&mut client);
    assert_eq!(mysql_only[0].0, b'E');
    assert!(mysql_only[0].1.windows(5).any(|window| window == b"42601"));
    assert_eq!(mysql_only.last().unwrap(), &(b'Z', b"I".to_vec()));

    send_query(&mut client, "PRAGMA user_version");
    let pragma = receive_until_ready(&mut client);
    assert_eq!(pragma[0].0, b'E');
    assert!(pragma[0].1.windows(5).any(|window| window == b"42601"));

    // 未知参数必须明确报未识别，而不是返回空值
    send_query(&mut client, "SHOW nope_param");
    let unknown = receive_until_ready(&mut client);
    assert_eq!(unknown[0].0, b'E');
    assert!(unknown[0].1.windows(5).any(|window| window == b"42704"));
    assert_eq!(unknown.last().unwrap(), &(b'Z', b"I".to_vec()));

    drop(client);
    server.join().unwrap();
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

fn assert_sqlstate(client: &mut TcpStream, sql: &str, state: &[u8]) {
    send_query(client, sql);
    let responses = receive_until_ready(client);
    assert_eq!(responses[0].0, b'E', "{sql} must fail");
    assert!(
        responses[0].1.windows(state.len()).any(|window| window == state),
        "{sql} must report SQLSTATE {}, got {}",
        String::from_utf8_lossy(state),
        String::from_utf8_lossy(&responses[0].1)
    );
    assert_eq!(responses.last().unwrap(), &(b'Z', b"I".to_vec()), "{sql} leaves the session ready");
}

/// Like `assert_sqlstate` but expects the session to be in Failed state
/// after the error, i.e. the SQL was issued inside a transaction block.
fn assert_sqlstate_in_tx(client: &mut TcpStream, sql: &str, state: &[u8]) {
    send_query(client, sql);
    let responses = receive_until_ready(client);
    assert_eq!(responses[0].0, b'E', "{sql} must fail");
    assert!(
        responses[0].1.windows(state.len()).any(|window| window == state),
        "{sql} must report SQLSTATE {}, got {}",
        String::from_utf8_lossy(state),
        String::from_utf8_lossy(&responses[0].1)
    );
    assert_eq!(responses.last().unwrap(), &(b'Z', b"E".to_vec()), "{sql} leaves the session in transaction-failed state");
}

#[test]
fn sqlstate_reflects_structured_gateway_errors() {
    let (mut client, server) = start_server_with_runtime();

    assert_sqlstate(&mut client, "SELECT id FROM missing", b"42P01");
    assert_sqlstate(&mut client, "SELECT nope FROM users", b"42703");
    assert_sqlstate(&mut client, "SELECT nope.id FROM users", b"42P01");
    assert_sqlstate(&mut client, "SELECT id FROM users INNER JOIN orders ON users.id = orders.user_id", b"42702");
    assert_sqlstate(&mut client, "SELECT ( FROM users", b"42601");
    assert_sqlstate(&mut client, "SELECT nope_fn(name) FROM users", b"42883");
    // 负长度在 PostgreSQL 是 `22011`；MySQL 对同一表达式返回空串。
    assert_sqlstate(&mut client, "SELECT SUBSTRING('abcdef', 2, -1)", b"22011");
    // `CURDATE()` / `CURTIME()` 与 `EXTRACT` 是 MySQL 拼写或未实现的语义，按未知函数报 `42883`。
    assert_sqlstate(&mut client, "SELECT CURDATE()", b"42883");
    assert_sqlstate(&mut client, "SELECT CURTIME()", b"42883");
    assert_sqlstate(&mut client, "SELECT EXTRACT(id) FROM users", b"42883");
    // 精确整数越界报 `22003`，与除零 `22012` 分开。
    assert_sqlstate(&mut client, "SELECT 9223372036854775807 + 1", b"22003");
    assert_sqlstate(&mut client, "SELECT 9223372036854775807 * 2", b"22003");
    // 外连接已受支持：未限定列名报歧义，而不是「不支持」。
    assert_sqlstate(&mut client, "SELECT id FROM users LEFT JOIN orders ON users.id = orders.user_id", b"42702");
    assert_sqlstate(&mut client, "SELECT id FROM users RIGHT JOIN orders ON users.id = orders.user_id", b"42702");
    assert_sqlstate(&mut client, "SELECT id FROM users FULL JOIN orders ON users.id = orders.user_id", b"42702");
    assert_sqlstate(&mut client, "INSERT INTO users VALUES (1, 42)", b"42804");
    assert_sqlstate(&mut client, "INSERT INTO users VALUES (NULL, 'x')", b"23502");

    send_query(&mut client, "INSERT INTO users VALUES (1, 'alice')");
    let inserted = receive_until_ready(&mut client);
    assert_eq!(inserted[0].0, b'C');
    assert_sqlstate(&mut client, "INSERT INTO users VALUES (1, 'bob')", b"23505");

    // 同名列限定后可正常查询：匹配行带回右表值，未匹配左行保留并把右侧列填 NULL。
    send_query(&mut client, "INSERT INTO users VALUES (2, 'bob')");
    assert_eq!(receive_until_ready(&mut client)[0].0, b'C');
    send_query(&mut client, "INSERT INTO orders VALUES (1, 1)");
    assert_eq!(receive_until_ready(&mut client)[0].0, b'C');
    send_query(
        &mut client,
        "SELECT users.id, orders.user_id FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id",
    );
    let joined = receive_until_ready(&mut client);
    assert_eq!(joined[0].0, b'T', "a qualified outer join returns a result set");
    let rows: Vec<Vec<Option<Vec<u8>>>> =
        joined.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    assert_eq!(
        rows,
        vec![vec![Some(b"1".to_vec()), Some(b"1".to_vec())], vec![Some(b"2".to_vec()), None]],
        "the matched left row carries the right value and the unmatched one stays NULL"
    );

    drop(client);
    server.join().unwrap();
}

/// `INSERT ... SELECT` 必须按写语句返回 `INSERT 0 N` 命令标签，而不是结果集。
#[test]
fn insert_select_reports_affected_rows_as_a_write() {
    let (mut client, server) = start_server_with_runtime();

    send_query(&mut client, "INSERT INTO users (id, name) SELECT 1, 'alice'");
    let inserted = receive_until_ready(&mut client);
    assert_eq!(inserted[0], (b'C', b"INSERT 0 1\0".to_vec()));
    assert_eq!(inserted.last().unwrap(), &(b'Z', b"I".to_vec()));

    // 源查询读同一张表，写入另一张表。
    send_query(&mut client, "INSERT INTO orders (id, user_id) SELECT id, id FROM users WHERE id = 1");
    let copied = receive_until_ready(&mut client);
    assert_eq!(copied[0], (b'C', b"INSERT 0 1\0".to_vec()));
    assert_eq!(copied.last().unwrap(), &(b'Z', b"I".to_vec()));

    send_query(&mut client, "SELECT user_id FROM orders WHERE id = 1");
    let rows = receive_until_ready(&mut client);
    assert_eq!(rows[0].0, b'T');

    assert_sqlstate(&mut client, "INSERT INTO users (id, name) SELECT 1, 'bob'", b"23505");
    assert_sqlstate(&mut client, "INSERT INTO users (id, name) SELECT id FROM users", b"42601");
    assert_sqlstate(&mut client, "INSERT INTO users (id, name) SELECT name, name FROM users", b"42804");

    // `ON CONFLICT DO NOTHING` 跳过冲突行，命令标签的计数只含真正写入的行。
    send_query(&mut client, "INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO NOTHING");
    let skipped = receive_until_ready(&mut client);
    assert_eq!(skipped[0], (b'C', b"INSERT 0 0\0".to_vec()));
    send_query(&mut client, "INSERT INTO orders (id, user_id) SELECT id, id FROM users ON CONFLICT (id) DO NOTHING");
    let skipped_many = receive_until_ready(&mut client);
    assert_eq!(skipped_many[0], (b'C', b"INSERT 0 0\0".to_vec()));
    // `DO UPDATE` 命中既有行时改写，并按写语句返回命令标签。
    send_query(&mut client, "INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (id) DO UPDATE SET name = 'bob'");
    let updated = receive_until_ready(&mut client);
    assert_eq!(updated[0], (b'C', b"INSERT 0 1\0".to_vec()));
    send_query(&mut client, "INSERT INTO orders (id, user_id) SELECT id, id FROM users ON CONFLICT (id) DO UPDATE SET user_id = 7");
    let updated_many = receive_until_ready(&mut client);
    assert_eq!(updated_many[0], (b'C', b"INSERT 0 1\0".to_vec()));

    send_query(&mut client, "SELECT name FROM users WHERE id = 1");
    let rewritten = receive_until_ready(&mut client);
    assert_eq!(rewritten[1].0, b'D');
    assert!(rewritten[1].1.windows(3).any(|window| window == b"bob"), "the conflicting row is rewritten");

    // 不符唯一约束的目标继续显式拒绝。
    assert_sqlstate(
        &mut client,
        "INSERT INTO users (id, name) VALUES (1, 'bob') ON CONFLICT (name) DO NOTHING",
        b"0A000",
    );

    // `INSERT ... RETURNING` 在简单查询路径上返回列定义与数据行，命令标签仍是写语句。
    send_query(&mut client, "INSERT INTO users (id, name) VALUES (2, 'carol') RETURNING id, name");
    let returning = receive_until_ready(&mut client);
    assert_eq!(returning.len(), 4, "RowDescription, DataRow, CommandComplete, ReadyForQuery");
    assert_eq!(returning[0].0, b'T');
    assert!(returning[0].1.windows(3).any(|window| window == b"id\0"), "the description names `id`");
    assert!(returning[0].1.windows(5).any(|window| window == b"name\0"), "the description names `name`");
    assert_eq!(returning[1].0, b'D');
    assert!(returning[1].1.windows(5).any(|window| window == b"carol"), "the returned row carries the inserted value");
    assert_eq!(returning[2], (b'C', b"INSERT 0 1\0".to_vec()));

    // 多行 `RETURNING` 每行一条 DataRow。
    send_query(&mut client, "INSERT INTO users (id, name) VALUES (3, 'dave'), (4, 'erin') RETURNING id");
    let returned_many = receive_until_ready(&mut client);
    assert_eq!(returned_many.iter().filter(|message| message.0 == b'D').count(), 2);
    assert_eq!(returned_many.last().unwrap().0, b'Z');

    drop(client);
    server.join().unwrap();
}

/// 带 SQL runtime 的服务端，用于验证 PostgreSQL 语法下 `@@` 必须被拒绝。
fn start_server_with_runtime() -> (TcpStream, thread::JoinHandle<()>) {
    use std::sync::Arc;
    use yyds_execution::LocalExecutor;
    use yyds_gateway::SqlCatalog;
    use yyds_gateway_pgsql::connection::{serve_connection, PgsqlSqlRuntime};
    use yyds_kv::MemoryShard;
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let source = "table users { @@id: i64, name: utf8 }\ntable orders { @@id: i64, user_id: i64 }\ntable metrics { @@id: i64, score: f64, active: bool, blob: bytes, small: i32 }\n";
    let projection = vos::parse_oak(source).expect("Oak parses").project_schema().expect("projection");
    let manifest = vos::types::IdentityManifest {
        format_version: vos::types::IDENTITY_MANIFEST_VERSION.into(),
        types: vec![
            vos::types::TypeIdentity {
                canonical_path: vec!["users".into()],
                type_id: 1,
                kind: vos::types::TypeContractKind::Table,
                fields: vec![
                    vos::types::FieldIdentity { canonical_name: "id".into(), field_id: 1, virtual_field_index: Some(0) },
                    vos::types::FieldIdentity { canonical_name: "name".into(), field_id: 2, virtual_field_index: Some(1) },
                ],
            },
            vos::types::TypeIdentity {
                canonical_path: vec!["orders".into()],
                type_id: 2,
                kind: vos::types::TypeContractKind::Table,
                fields: vec![
                    vos::types::FieldIdentity { canonical_name: "id".into(), field_id: 3, virtual_field_index: Some(0) },
                    vos::types::FieldIdentity { canonical_name: "user_id".into(), field_id: 4, virtual_field_index: Some(1) },
                ],
            },
            vos::types::TypeIdentity {
                canonical_path: vec!["metrics".into()],
                type_id: 3,
                kind: vos::types::TypeContractKind::Table,
                fields: vec![
                    vos::types::FieldIdentity { canonical_name: "id".into(), field_id: 5, virtual_field_index: Some(0) },
                    vos::types::FieldIdentity { canonical_name: "score".into(), field_id: 6, virtual_field_index: Some(1) },
                    vos::types::FieldIdentity { canonical_name: "active".into(), field_id: 7, virtual_field_index: Some(2) },
                    vos::types::FieldIdentity { canonical_name: "blob".into(), field_id: 8, virtual_field_index: Some(3) },
                    vos::types::FieldIdentity { canonical_name: "small".into(), field_id: 9, virtual_field_index: Some(4) },
                ],
            },
        ],
    };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    let catalog = SqlCatalog::from_resolved_contract(7, &contract).expect("gateway catalog");

    let shard = MemoryShard::new();
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).expect("routing");
    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    let executor = LocalExecutor::new(routing, shards).expect("executor");

    let runtime = Arc::new(PgsqlSqlRuntime { catalog, schema_version: 7, executor: Arc::new(executor) });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_connection(stream, Some(runtime)).unwrap();
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
fn mysql_style_system_variables_are_rejected_with_a_runtime() {
    let (mut client, server) = start_server_with_runtime();

    // `@@` 在 PostgreSQL 里不是变量语法，必须按未知参数报错而不是当作列名。
    send_query(&mut client, "SELECT @@version");
    let rejected = receive_until_ready(&mut client);
    assert_eq!(rejected[0].0, b'E');
    assert!(rejected[0].1.windows(5).any(|window| window == b"42704"));
    assert!(String::from_utf8_lossy(&rejected[0].1).contains("system variable"));
    assert_eq!(rejected.last().unwrap(), &(b'Z', b"I".to_vec()));

    // 普通 SELECT 仍然走通 runtime（证明拒绝发生在绑定层而不是通用「未实现」分支）。
    send_query(&mut client, "SELECT id FROM users");
    let rows = receive_until_ready(&mut client);
    assert_eq!(rows[0].0, b'T');
    assert_eq!(rows.last().unwrap(), &(b'Z', b"I".to_vec()));

    drop(client);
    server.join().unwrap();
}

fn send_message(client: &mut TcpStream, tag: u8, payload: &[u8]) {
    client.write_all(&encode_message(tag, payload, 64 * 1024).unwrap()).unwrap();
}

fn cstring(value: &str) -> Vec<u8> {
    let mut bytes = value.as_bytes().to_vec();
    bytes.push(0);
    bytes
}

/// `name`, `type OID`, `type size` and `format code` for each described column.
fn parse_row_description(payload: &[u8]) -> Vec<(String, u32, i16, i16)> {
    let mut offset = 0;
    let count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    offset += 2;
    let mut columns = Vec::with_capacity(count);
    for _ in 0..count {
        let end = payload[offset..].iter().position(|byte| *byte == 0).unwrap() + offset;
        let name = String::from_utf8(payload[offset..end].to_vec()).unwrap();
        offset = end + 1 + 4 + 2;
        let oid = u32::from_be_bytes(payload[offset..offset + 4].try_into().unwrap());
        offset += 4;
        let size = i16::from_be_bytes([payload[offset], payload[offset + 1]]);
        offset += 2 + 4;
        let format = i16::from_be_bytes([payload[offset], payload[offset + 1]]);
        offset += 2;
        columns.push((name, oid, size, format));
    }
    columns
}

fn parse_data_row(payload: &[u8]) -> Vec<Option<Vec<u8>>> {
    let count = i16::from_be_bytes([payload[0], payload[1]]) as usize;
    let mut offset = 2;
    let mut values = Vec::with_capacity(count);
    for _ in 0..count {
        let length = i32::from_be_bytes(payload[offset..offset + 4].try_into().unwrap());
        offset += 4;
        if length < 0 {
            values.push(None);
            continue;
        }
        let length = length as usize;
        values.push(Some(payload[offset..offset + length].to_vec()));
        offset += length;
    }
    values
}

#[test]
fn describe_statement_reports_catalog_types_and_binary_rows_round_trip() {
    let (mut client, server) = start_server_with_runtime();

    // 空结果集也必须按 catalog 声明类型描述，而不是退化成 text。
    let mut parse = cstring("s2");
    parse.extend_from_slice(&cstring("SELECT id, name FROM users WHERE id = $1"));
    parse.extend_from_slice(&1_i16.to_be_bytes());
    parse.extend_from_slice(&20_u32.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    let mut describe = vec![b'S'];
    describe.extend_from_slice(&cstring("s2"));
    send_message(&mut client, b'D', &describe);
    send_message(&mut client, b'S', &[]);
    let described = receive_until_ready(&mut client);
    assert_eq!(described[0].0, b'1');
    assert_eq!(described[1].0, b't');
    assert_eq!(described[2].0, b'T');
    let columns = parse_row_description(&described[2].1);
    assert_eq!(columns, vec![("id".to_string(), 20, 8, 0), ("name".to_string(), 25, -1, 0)]);

    send_query(&mut client, "INSERT INTO metrics VALUES (1, 1.5, 1, 'bin', 7)");
    let inserted = receive_until_ready(&mut client);
    assert_eq!(inserted[0].0, b'C');

    let mut parse = cstring("s1");
    parse.extend_from_slice(&cstring("SELECT id, score, active, blob, small FROM metrics WHERE id = $1"));
    parse.extend_from_slice(&1_i16.to_be_bytes());
    parse.extend_from_slice(&20_u32.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    send_message(&mut client, b'D', &describe_of(&cstring("s1")));
    send_message(&mut client, b'S', &[]);
    let described = receive_until_ready(&mut client);
    let columns = parse_row_description(&described[2].1);
    assert_eq!(
        columns,
        vec![
            ("id".to_string(), 20, 8, 0),
            ("score".to_string(), 701, 8, 0),
            ("active".to_string(), 16, 1, 0),
            ("blob".to_string(), 17, -1, 0),
            ("small".to_string(), 23, 4, 0),
        ]
    );

    let mut bind = cstring("p1");
    bind.extend_from_slice(&cstring("s1"));
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&8_i32.to_be_bytes());
    bind.extend_from_slice(&1_i64.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    let mut describe = vec![b'P'];
    describe.extend_from_slice(&cstring("p1"));
    send_message(&mut client, b'D', &describe);
    send_message(&mut client, b'S', &[]);
    let described = receive_until_ready(&mut client);
    assert_eq!(described[0].0, b'2');
    assert_eq!(described[1].0, b'T');
    let columns = parse_row_description(&described[1].1);
    assert!(columns.iter().all(|(_, _, _, format)| *format == 1), "binary formats requested at Bind must be reported");

    let mut bind = cstring("p2");
    bind.extend_from_slice(&cstring("s1"));
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&8_i32.to_be_bytes());
    bind.extend_from_slice(&1_i64.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    let mut execute = cstring("p2");
    execute.extend_from_slice(&0_i32.to_be_bytes());
    send_message(&mut client, b'E', &execute);
    send_message(&mut client, b'S', &[]);
    let executed = receive_until_ready(&mut client);
    assert_eq!(executed[0].0, b'2');
    assert_eq!(executed[1].0, b'D');
    let row = parse_data_row(&executed[1].1);
    assert_eq!(row.len(), 5);
    assert_eq!(row[0], Some(1_i64.to_be_bytes().to_vec()));
    assert_eq!(row[1], Some(1.5_f64.to_be_bytes().to_vec()));
    assert_eq!(row[2], Some(vec![1]));
    assert_eq!(row[3], Some(b"bin".to_vec()));
    assert_eq!(row[4], Some(7_i32.to_be_bytes().to_vec()));

    // 不支持的二进制参数类型必须报 0A000 并指明 OID。
    let mut parse = cstring("s3");
    parse.extend_from_slice(&cstring("SELECT name FROM users WHERE id = $1"));
    parse.extend_from_slice(&1_i16.to_be_bytes());
    parse.extend_from_slice(&1184_u32.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    let mut bind = cstring("p3");
    bind.extend_from_slice(&cstring("s3"));
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&1_i16.to_be_bytes());
    bind.extend_from_slice(&8_i32.to_be_bytes());
    bind.extend_from_slice(&0_i64.to_be_bytes());
    bind.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'B', &bind);
    send_message(&mut client, b'S', &[]);
    let rejected = receive_until_ready(&mut client);
    let error = rejected.iter().find(|(tag, _)| *tag == b'E').expect("binary parameter type must be rejected");
    let text = String::from_utf8_lossy(&error.1);
    assert!(text.contains("0A000") && text.contains("1184"), "{text}");

    drop(client);
    server.join().unwrap();
}

/// 窗口函数的排名列经 `Describe` 下发的是声明的 `int8`（OID 20，8 字节），
/// 不按结果行推断，也不退化成 `text`。
#[test]
fn window_rank_columns_are_described_as_int8() {
    let (mut client, server) = start_server_with_runtime();

    send_query(&mut client, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob'), (3, 'carol')");
    let inserted = receive_until_ready(&mut client);
    assert_eq!(inserted[0], (b'C', b"INSERT 0 3\0".to_vec()));

    let mut parse = cstring("w1");
    parse.extend_from_slice(&cstring(
        "SELECT id, ROW_NUMBER() OVER (ORDER BY id), RANK() OVER (ORDER BY id), \
         DENSE_RANK() OVER (ORDER BY id) FROM users",
    ));
    parse.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    send_message(&mut client, b'D', &describe_of(&cstring("w1")));
    send_message(&mut client, b'S', &[]);
    let described = receive_until_ready(&mut client);
    assert_eq!(described[0].0, b'1');
    assert_eq!(described[1].0, b't');
    assert_eq!(described[2].0, b'T');
    assert_eq!(
        parse_row_description(&described[2].1),
        vec![
            ("id".to_string(), 20, 8, 0),
            ("ROW_NUMBER".to_string(), 20, 8, 0),
            ("RANK".to_string(), 20, 8, 0),
            ("DENSE_RANK".to_string(), 20, 8, 0),
        ]
    );

    // 实际结果也是 64 位整数：文本格式下每列就是排名文本，行序由 `ORDER BY id` 决定。
    send_query(&mut client, "SELECT id, ROW_NUMBER() OVER (ORDER BY id) FROM users ORDER BY id");
    let rows = receive_until_ready(&mut client);
    assert_eq!(rows[0].0, b'T');
    assert_eq!(
        parse_row_description(&rows[0].1),
        vec![("id".to_string(), 20, 8, 0), ("ROW_NUMBER".to_string(), 20, 8, 0)]
    );
    let data: Vec<Vec<Option<Vec<u8>>>> =
        rows.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    assert_eq!(data.len(), 3);
    assert_eq!(data[0], vec![Some(b"1".to_vec()), Some(b"1".to_vec())]);
    assert_eq!(data[2], vec![Some(b"3".to_vec()), Some(b"3".to_vec())]);

    drop(client);
    server.join().unwrap();
}

/// 嵌套调用不改变 `Describe` 的类型：聚合实参里的标量调用让外层聚合照旧下发
/// `int8`，`COALESCE(LOWER(name), UPPER(name))` 照旧下发 `text`。
#[test]
fn nested_call_columns_keep_declared_types() {
    let (mut client, server) = start_server_with_runtime();

    send_query(&mut client, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')");
    let inserted = receive_until_ready(&mut client);
    assert_eq!(inserted[0], (b'C', b"INSERT 0 2\0".to_vec()));

    let mut parse = cstring("n1");
    parse
        .extend_from_slice(&cstring("SELECT SUM(LENGTH(name)) AS total_len, SUM(CHAR_LENGTH(name)) AS total_chars FROM users"));
    parse.extend_from_slice(&0_i16.to_be_bytes());
    send_message(&mut client, b'P', &parse);
    send_message(&mut client, b'D', &describe_of(&cstring("n1")));
    send_message(&mut client, b'S', &[]);
    let described = receive_until_ready(&mut client);
    assert_eq!(
        parse_row_description(&described[2].1),
        vec![("total_len".to_string(), 20, 8, 0), ("total_chars".to_string(), 20, 8, 0)]
    );

    send_query(&mut client, "SELECT SUM(LENGTH(name)), SUM(CHAR_LENGTH(name)) FROM users");
    let rows = receive_until_ready(&mut client);
    let data: Vec<Vec<Option<Vec<u8>>>> =
        rows.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    assert_eq!(data, vec![vec![Some(b"8".to_vec()), Some(b"8".to_vec())]]);

    send_query(&mut client, "SELECT COALESCE(LOWER(name), UPPER(name)) AS name_label FROM users ORDER BY id");
    let rows = receive_until_ready(&mut client);
    assert_eq!(parse_row_description(&rows[0].1), vec![("name_label".to_string(), 25, -1, 0)]);
    let data: Vec<Vec<Option<Vec<u8>>>> =
        rows.iter().filter(|(tag, _)| *tag == b'D').map(|(_, payload)| parse_data_row(payload)).collect();
    assert_eq!(data, vec![vec![Some(b"alice".to_vec())], vec![Some(b"bob".to_vec())]]);

    drop(client);
    server.join().unwrap();
}

fn describe_of(statement: &[u8]) -> Vec<u8> {
    let mut describe = vec![b'S'];
    describe.extend_from_slice(statement);
    describe
}

/// `SAVEPOINT sp`、`ROLLBACK TO sp`、`RELEASE SAVEPOINT sp` 在 PostgreSQL 协议上
/// 各自发出对应的 CommandComplete 标签，并保持 ReadyForQuery 在事务内是 `T`。
/// 未知名字或重复名字的 SQLSTATE 都是 `3B000`。
#[test]
fn pgsql_savepoint_commands_emit_command_complete_tags() {
    let (mut client, server) = start_server_with_runtime();

    // 事务外发 SAVEPOINT 必须报 25001 (active_sql_transaction)。
    send_query(&mut client, "SAVEPOINT sp");
    let outside_tx = receive_until_ready(&mut client);
    assert_eq!(outside_tx[0].0, b'E');
    assert!(outside_tx[0].1.windows(5).any(|window| window == b"25001"));
    assert_eq!(outside_tx.last().unwrap(), &(b'Z', b"I".to_vec()));

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));

    // 成功创建 savepoint → CommandComplete "SAVEPOINT" + ReadyForQuery(T)
    send_query(&mut client, "SAVEPOINT sp");
    let created = receive_until_ready(&mut client);
    assert_eq!(created[0], (b'C', b"SAVEPOINT\0".to_vec()));
    assert_eq!(created.last().unwrap(), &(b'Z', b"T".to_vec()));

    // 重复名字报 3B000，事务内错误后期望 ReadyForQuery(E)。
    assert_sqlstate_in_tx(&mut client, "SAVEPOINT sp", b"3B000");
    // 事务失败后必须 ROLLBACK 才能继续。
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));

    // 重新开始测试 ROLLBACK TO / RELEASE 的错误码。
    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "SAVEPOINT sp");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));

    assert_sqlstate_in_tx(&mut client, "ROLLBACK TO nope", b"3B000");

    // 事务失败后必须 ROLLBACK + 重新 BEGIN 才能继续测试 RELEASE。
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));
    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "SAVEPOINT sp");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));

    assert_sqlstate_in_tx(&mut client, "RELEASE SAVEPOINT nope", b"3B000");

    // ROLLBACK TO 成功路径 — 独立事务。
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));
    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "SAVEPOINT sp");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));
    send_query(&mut client, "ROLLBACK TO sp");
    let rolled = receive_until_ready(&mut client);
    assert_eq!(rolled[0], (b'C', b"ROLLBACK\0".to_vec()));
    assert_eq!(rolled.last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "COMMIT");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));

    // RELEASE SAVEPOINT 成功路径 — 独立事务（RELEASE 不移除写状态只移除名字）。
    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "SAVEPOINT sp");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));
    send_query(&mut client, "RELEASE SAVEPOINT sp");
    let released = receive_until_ready(&mut client);
    assert_eq!(released[0], (b'C', b"RELEASE\0".to_vec()));
    assert_eq!(released.last().unwrap(), &(b'Z', b"T".to_vec()));
    send_query(&mut client, "COMMIT");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));

    drop(client);
    server.join().unwrap();
}

/// 事务内写→savepoint→写→rollback-to 必须把 PostgreSQL 状态恢复到 savepoint 时刻。
#[test]
fn pgsql_savepoint_rollback_restores_snapshot() {
    let (mut client, server) = start_server_with_runtime();

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));

    send_query(&mut client, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 2\0".to_vec()));

    send_query(&mut client, "SAVEPOINT sp");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));

    send_query(&mut client, "INSERT INTO users VALUES (3, 'carol')");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 1\0".to_vec()));

    // ROLLBACK TO 之后只剩 savepoint 之前的行。
    send_query(&mut client, "ROLLBACK TO sp");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"ROLLBACK\0".to_vec()));

    send_query(&mut client, "SELECT COUNT(*) FROM users");
    let count = receive_until_ready(&mut client);
    assert_eq!(parse_data_row(&count[1].1), vec![Some(b"2".to_vec())]);

    send_query(&mut client, "COMMIT");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));

    drop(client);
    server.join().unwrap();
}

/// `RELEASE SAVEPOINT sp` 移除命名但不回滚状态。
/// 之后 ROLLBACK TO 该名字必须报 3B000。
#[test]
fn pgsql_savepoint_release_removes_name_without_dropping_state() {
    let (mut client, server) = start_server_with_runtime();

    send_query(&mut client, "BEGIN");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"T".to_vec()));

    send_query(&mut client, "SAVEPOINT a");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));
    send_query(&mut client, "SAVEPOINT b");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"SAVEPOINT\0".to_vec()));

    send_query(&mut client, "INSERT INTO users VALUES (1, 'alice'), (2, 'bob')");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"INSERT 0 2\0".to_vec()));

    send_query(&mut client, "RELEASE SAVEPOINT a");
    assert_eq!(receive_until_ready(&mut client)[0], (b'C', b"RELEASE\0".to_vec()));

    // 释放之后状态仍然可见。
    send_query(&mut client, "SELECT COUNT(*) FROM users");
    let count = receive_until_ready(&mut client);
    assert_eq!(parse_data_row(&count[1].1), vec![Some(b"2".to_vec())]);

    // 已释放的名字不可再 rollback。
    assert_sqlstate_in_tx(&mut client, "ROLLBACK TO a", b"3B000");

    // 失败事务需要 COMMIT/ROLLBACK 恢复。
    send_query(&mut client, "ROLLBACK");
    assert_eq!(receive_until_ready(&mut client).last().unwrap(), &(b'Z', b"I".to_vec()));

    drop(client);
    server.join().unwrap();
}
