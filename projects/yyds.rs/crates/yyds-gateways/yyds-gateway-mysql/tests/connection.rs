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
        serve_connection(stream, None).unwrap();
    });
    let client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    (client, server)
}

#[test]
fn protocol_v10_handshake_accepts_charset_setup_then_rejects_sql_explicitly() {
    let (mut client, server) = start_server();
    let (sequence, handshake) = read_packet(&mut client);
    assert_eq!(sequence, 0);
    assert_eq!(handshake[0], 10);
    assert!(handshake.windows(b"mysql_native_password".len()).any(|window| window == b"mysql_native_password"));

    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet(&mut client), (2, vec![0, 0, 0, 2, 0, 0, 0]));

    client.write_all(&encode_packet(0, b"\x03SET NAMES utf8mb4").unwrap()).unwrap();
    assert_eq!(read_packet(&mut client), (1, vec![0, 0, 0, 2, 0, 0, 0]));

    client.write_all(&encode_packet(0, b"\x03SET NAMES latin1").unwrap()).unwrap();
    let (sequence, error) = read_packet(&mut client);
    assert_eq!(sequence, 1);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1115);

    client.write_all(&encode_packet(0, b"\x03SET NAMES utf8mb4 COLLATE utf8mb4_bin").unwrap()).unwrap();
    let (sequence, error) = read_packet(&mut client);
    assert_eq!(sequence, 1);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1273);

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

fn start_server_with_runtime() -> (TcpStream, thread::JoinHandle<()>) {
    use std::sync::Arc;
    use yyds_execution::LocalExecutor;
    use yyds_gateway::SqlCatalog;
    use yyds_gateway_mysql::connection::{serve_connection, MysqlSqlRuntime};
    use yyds_kv::{InlineValue, Key, KvStore, MemoryShard, StoredValue};
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let source = "table users { @@id: i64, name: utf8, age: i64 }\n";
    let projection = vos::parse_oak(source).expect("Oak parses").project_schema().expect("projection");
    let manifest = vos::types::IdentityManifest {
        format_version: vos::types::IDENTITY_MANIFEST_VERSION.into(),
        types: vec![vos::types::TypeIdentity {
            canonical_path: vec!["users".into()],
            type_id: 1,
            kind: vos::types::TypeContractKind::Table,
            fields: vec![
                vos::types::FieldIdentity { canonical_name: "id".into(), field_id: 1, virtual_field_index: Some(0) },
                vos::types::FieldIdentity { canonical_name: "name".into(), field_id: 2, virtual_field_index: Some(1) },
                vos::types::FieldIdentity { canonical_name: "age".into(), field_id: 3, virtual_field_index: Some(2) },
            ],
        }],
    };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    let catalog = SqlCatalog::from_resolved_contract(7, &contract).expect("gateway catalog");

    let mut shard = MemoryShard::new();
    // 预置 row: id=42, name=alice, age=30
    let mut values = yyds_gateway::RowFieldValues::new();
    values.insert(1, b"42".to_vec());
    values.insert(2, b"alice".to_vec());
    values.insert(3, b"30".to_vec());
    let row_bytes = yyds_gateway::encode_row(&values);
    shard.put(Key::new("table:1", b"42".to_vec()), StoredValue::Inline(InlineValue(row_bytes))).expect("put row");

    let epoch = ShardEpoch(1);
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(epoch, vec![shard_id.clone()]).expect("routing");
    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    let executor = LocalExecutor::new(routing, shards).expect("executor");

    let runtime = Arc::new(MysqlSqlRuntime { catalog, schema_version: 7, executor: Arc::new(executor) });
    spawn_runtime_server(runtime)
}

fn spawn_runtime_server(
    runtime: std::sync::Arc<yyds_gateway_mysql::connection::MysqlSqlRuntime>,
) -> (TcpStream, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let rt = runtime;
    let server = thread::spawn(move || {
        let (stream, _) = listener.accept().unwrap();
        serve_connection(stream, Some(rt)).unwrap();
    });
    let client = TcpStream::connect(address).unwrap();
    client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
    (client, server)
}

/// 向 shard 写入一整行（TLV 编码），供多表 fixture 共用。
fn put_row(shard: &mut yyds_kv::MemoryShard, namespace: &str, rowid: &[u8], fields: &[(u64, &[u8])]) {
    use yyds_kv::{InlineValue, Key, KvStore, StoredValue};
    let mut values = yyds_gateway::RowFieldValues::new();
    for (field_id, bytes) in fields {
        values.insert(*field_id, bytes.to_vec());
    }
    shard
        .put(Key::new(namespace, rowid.to_vec()), StoredValue::Inline(InlineValue(yyds_gateway::encode_row(&values))))
        .expect("put row");
}

/// 两表 runtime：`users`(id=1, name=2) 与 `orders`(id=3, user_id=4, total=5)。
fn start_server_with_join_runtime() -> (TcpStream, thread::JoinHandle<()>) {
    use std::sync::Arc;
    use yyds_execution::LocalExecutor;
    use yyds_gateway::SqlCatalog;
    use yyds_gateway_mysql::connection::MysqlSqlRuntime;
    use yyds_kv::MemoryShard;
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let source = "table users { @@id: i64, name: utf8 }\ntable orders { @@id: i64, user_id: i64, total: i64 }\n";
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
                    vos::types::FieldIdentity { canonical_name: "total".into(), field_id: 5, virtual_field_index: Some(2) },
                ],
            },
        ],
    };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    let catalog = SqlCatalog::from_resolved_contract(7, &contract).expect("gateway catalog");

    let mut shard = MemoryShard::new();
    put_row(&mut shard, "table:1", b"1", &[(1, b"1"), (2, b"alice")]);
    put_row(&mut shard, "table:1", b"2", &[(1, b"2"), (2, b"bob")]);
    put_row(&mut shard, "table:2", b"10", &[(3, b"10"), (4, b"1"), (5, b"100")]);
    put_row(&mut shard, "table:2", b"12", &[(3, b"12"), (4, b"2"), (5, b"70")]);

    let epoch = ShardEpoch(1);
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(epoch, vec![shard_id.clone()]).expect("routing");
    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    let executor = LocalExecutor::new(routing, shards).expect("executor");

    let runtime = Arc::new(MysqlSqlRuntime { catalog, schema_version: 7, executor: Arc::new(executor) });
    spawn_runtime_server(runtime)
}

fn read_packet_full(stream: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 4];
    stream.read_exact(&mut header).unwrap();
    let length = usize::from(header[0]) | (usize::from(header[1]) << 8) | (usize::from(header[2]) << 16);
    let mut payload = vec![0; length];
    stream.read_exact(&mut payload).unwrap();
    (header[3], payload)
}

/// 解 MySQL length-encoded integer：返回 (value, consumed_bytes)
fn read_lenenc(data: &[u8]) -> (u64, usize) {
    let first = data[0];
    match first {
        0..=250 => (first as u64, 1),
        251 => (0, 1), // NULL
        252 => (u16::from_le_bytes([data[1], data[2]]) as u64, 3),
        253 => (u32::from_le_bytes([data[1], data[2], data[3], 0]) as u64, 4),
        254 => (u64::from_le_bytes([data[1], data[2], data[3], data[4], data[5], data[6], data[7], data[8]]), 9),
        _ => unreachable!(),
    }
}

fn read_lenenc_str(data: &[u8]) -> (&[u8], usize) {
    let (len, consumed) = read_lenenc(data);
    if len == 0 && consumed == 1 && data[0] == 251 {
        (&[], 1) // NULL
    }
    else {
        (&data[consumed..consumed + len as usize], consumed + len as usize)
    }
}

#[test]
fn mysql_select_with_runtime_returns_result_set() {
    let (mut client, server) = start_server_with_runtime();
    let _ = read_packet_full(&mut client); // handshake

    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    let (seq, ok) = read_packet_full(&mut client);
    assert_eq!(seq, 2);
    assert_eq!(ok[0], 0); // OK packet

    // SET NAMES utf8mb4
    client.write_all(&encode_packet(0, b"\x03SET NAMES utf8mb4").unwrap()).unwrap();
    let (seq, ok) = read_packet_full(&mut client);
    assert_eq!(seq, 1);
    assert_eq!(ok[0], 0);

    // SELECT id, name FROM users WHERE id = 42
    client.write_all(&encode_packet(0, b"\x03SELECT id, name FROM users WHERE id = 42").unwrap()).unwrap();

    // pkt 1: column count
    let (mut seq, col_count_pkt) = read_packet_full(&mut client);
    assert_eq!(seq, 1);
    let (col_count, consumed) = read_lenenc(&col_count_pkt);
    assert_eq!(col_count, 2);
    assert_eq!(consumed, 1);

    // pkt 2: column def for 'id'
    let (_, col_def) = read_packet_full(&mut client);
    // 跳过 "def" + schema + table + org_table，跳过 name 部分
    // 简化：直接找 'id' 和 'name' 的 UTF-8 字节
    let col_name = extract_column_name(&col_def);
    assert_eq!(col_name, "id");

    // pkt 3: column def for 'name'
    let (_, col_def) = read_packet_full(&mut client);
    let col_name = extract_column_name(&col_def);
    assert_eq!(col_name, "name");

    // pkt 4: EOF after columns
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    // pkt 5: row [42, alice]
    let (_, row_pkt) = read_packet_full(&mut client);
    let (val1, c1) = read_lenenc_str(&row_pkt);
    assert_eq!(val1, b"42");
    let (val2, c2) = read_lenenc_str(&row_pkt[c1..]);
    assert_eq!(val2, b"alice");
    assert_eq!(c1 + c2, row_pkt.len());

    // pkt 6: EOF after rows
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    drop(client);
    server.join().unwrap();
}

#[test]
fn mysql_system_variable_probes_return_single_row() {
    let (mut client, server) = start_server_with_runtime();
    let _ = read_packet_full(&mut client); // handshake
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    let _ = read_packet_full(&mut client);
    client.write_all(&encode_packet(0, b"\x03SET NAMES utf8mb4").unwrap()).unwrap();
    let _ = read_packet_full(&mut client);

    // SELECT @@version —— 无 FROM，返回单行单列
    client.write_all(&encode_packet(0, b"\x03SELECT @@version").unwrap()).unwrap();
    let (_, col_count) = read_packet_full(&mut client);
    assert_eq!(read_lenenc(&col_count).0, 1);
    let _ = read_packet_full(&mut client); // column definition
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);
    let (_, row) = read_packet_full(&mut client);
    let (value, consumed) = read_lenenc_str(&row);
    assert_eq!(value, b"8.0.36-yyds");
    assert_eq!(consumed, row.len());
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    // 作用域前缀被接受，数值型变量按数字返回
    client.write_all(&encode_packet(0, b"\x03SELECT @@session.lower_case_table_names").unwrap()).unwrap();
    let (_, col_count) = read_packet_full(&mut client);
    assert_eq!(read_lenenc(&col_count).0, 1);
    let _ = read_packet_full(&mut client); // column definition
    let _ = read_packet_full(&mut client); // EOF
    let (_, row) = read_packet_full(&mut client);
    assert_eq!(read_lenenc_str(&row).0, b"0");
    let _ = read_packet_full(&mut client); // EOF

    // 未知变量按 MySQL 语义报错
    client.write_all(&encode_packet(0, b"\x03SELECT @@nope").unwrap()).unwrap();
    let (_, error) = read_packet_full(&mut client);
    assert_eq!(error[0], 0xff);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1193);
    assert!(error.windows(b"Unknown system variable".len()).any(|window| window == b"Unknown system variable"));

    drop(client);
    server.join().unwrap();
}

#[test]
fn mysql_select_missing_row_returns_empty_result_set() {
    let (mut client, server) = start_server_with_runtime();
    let _ = read_packet_full(&mut client);
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    let _ = read_packet_full(&mut client);
    client.write_all(&encode_packet(0, b"\x03SET NAMES utf8mb4").unwrap()).unwrap();
    let _ = read_packet_full(&mut client);

    client.write_all(&encode_packet(0, b"\x03SELECT id FROM users WHERE id = 999").unwrap()).unwrap();

    let (_, col_count_pkt) = read_packet_full(&mut client);
    let (col_count, _) = read_lenenc(&col_count_pkt);
    assert_eq!(col_count, 1);

    let (_, col_def) = read_packet_full(&mut client);
    assert_eq!(extract_column_name(&col_def), "id");

    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    // 没有 row packet，直接 EOF
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    drop(client);
    server.join().unwrap();
}

#[test]
fn session_variable_statements_report_gateway_capabilities() {
    let (mut client, server) = start_server();
    let _ = read_packet(&mut client);
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet(&mut client), (2, vec![0, 0, 0, 2, 0, 0, 0]));

    // SHOW VARIABLES LIKE 'autocommit' -> 2 列 / 1 行
    client.write_all(&encode_packet(0, b"\x03SHOW VARIABLES LIKE 'autocommit'").unwrap()).unwrap();
    let (sequence, col_count) = read_packet(&mut client);
    assert_eq!(sequence, 1);
    assert_eq!(read_lenenc(&col_count).0, 2);
    let (_, col_def) = read_packet(&mut client);
    assert_eq!(extract_column_name(&col_def), "Variable_name");
    let (_, col_def) = read_packet(&mut client);
    assert_eq!(extract_column_name(&col_def), "Value");
    let (_, eof) = read_packet(&mut client);
    assert_eq!(eof[0], 0xFE);
    let (_, row) = read_packet(&mut client);
    let (name, consumed) = read_lenenc_str(&row);
    assert_eq!(name, b"autocommit");
    let (value, _) = read_lenenc_str(&row[consumed..]);
    assert_eq!(value, b"ON");
    let (_, eof) = read_packet(&mut client);
    assert_eq!(eof[0], 0xFE);

    // SET autocommit = 1 -> OK
    client.write_all(&encode_packet(0, b"\x03SET autocommit = 1").unwrap()).unwrap();
    assert_eq!(read_packet(&mut client), (1, vec![0, 0, 0, 2, 0, 0, 0]));

    // SET autocommit = 0 — 真实切换 session 状态，OK 包里不再带 SERVER_STATUS_AUTOCOMMIT。
    client.write_all(&encode_packet(0, b"\x03SET autocommit = 0").unwrap()).unwrap();
    assert_eq!(read_packet(&mut client), (1, vec![0, 0, 0, 0, 0, 0, 0]));

    // PRAGMA 不是 MySQL 语法
    client.write_all(&encode_packet(0, b"\x03PRAGMA user_version").unwrap()).unwrap();
    let (sequence, error) = read_packet(&mut client);
    assert_eq!(sequence, 1);
    assert_eq!(error[0], 0xff);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1064);

    // PostgreSQL 风格 SHOW <parameter> 在 MySQL 下也不成立
    client.write_all(&encode_packet(0, b"\x03SHOW server_version").unwrap()).unwrap();
    let (sequence, error) = read_packet(&mut client);
    assert_eq!(sequence, 1);
    assert_eq!(error[0], 0xff);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1064);

    drop(client);
    server.join().unwrap();
}

#[test]
fn mysql_inner_join_returns_joined_result_set() {
    let (mut client, server) = start_server_with_join_runtime();
    let _ = read_packet_full(&mut client); // handshake
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    let (seq, ok) = read_packet_full(&mut client);
    assert_eq!(seq, 2);
    assert_eq!(ok[0], 0);
    client.write_all(&encode_packet(0, b"\x03SET NAMES utf8mb4").unwrap()).unwrap();
    let _ = read_packet_full(&mut client);

    // JOIN 必须以结果集（而不是 OK packet）返回，ORDER BY 在连接之后应用
    client
        .write_all(
            &encode_packet(
                0,
                b"\x03SELECT users.name, orders.total FROM users JOIN orders ON users.id = orders.user_id ORDER BY orders.total",
            )
            .unwrap(),
        )
        .unwrap();

    let (_, col_count_pkt) = read_packet_full(&mut client);
    assert_eq!(read_lenenc(&col_count_pkt).0, 2);
    let (_, col_def) = read_packet_full(&mut client);
    assert_eq!(extract_column_name(&col_def), "name");
    let (_, col_def) = read_packet_full(&mut client);
    assert_eq!(extract_column_name(&col_def), "total");
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    // bob/70 在前，alice/100 在后
    let (_, row) = read_packet_full(&mut client);
    let (name, consumed) = read_lenenc_str(&row);
    assert_eq!(name, b"bob");
    let (total, _) = read_lenenc_str(&row[consumed..]);
    assert_eq!(total, b"70");

    let (_, row) = read_packet_full(&mut client);
    let (name, consumed) = read_lenenc_str(&row);
    assert_eq!(name, b"alice");
    let (total, _) = read_lenenc_str(&row[consumed..]);
    assert_eq!(total, b"100");

    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    drop(client);
    server.join().unwrap();
}

fn extract_column_name(col_def: &[u8]) -> String {
    let mut cursor = 0usize;
    // def
    let (_, c) = read_lenenc(col_def);
    cursor += c + 3; // "def"
                     // schema
    let (_, c) = read_lenenc(&col_def[cursor..]);
    cursor += c;
    // table
    let (_, c) = read_lenenc(&col_def[cursor..]);
    cursor += c;
    // org_table
    let (_, c) = read_lenenc(&col_def[cursor..]);
    cursor += c;
    // name
    let (name_bytes, c) = read_lenenc_str(&col_def[cursor..]);
    String::from_utf8(name_bytes.to_vec()).unwrap_or_default()
}

/// MySQL 报文：0x03 + SQL 文本。
fn sql_payload(sql: &str) -> Vec<u8> {
    let mut payload = vec![0x03];
    payload.extend_from_slice(sql.as_bytes());
    payload
}

/// 发送 `COM_STMT_PREPARE` 并读完 OK + 参数定义 + 列定义，返回语句 id 与元数据。
fn prepare(client: &mut TcpStream, sql: &str) -> (u32, u16, u16, Vec<String>, Vec<u8>) {
    let mut payload = vec![0x16];
    payload.extend_from_slice(sql.as_bytes());
    client.write_all(&encode_packet(0, &payload).unwrap()).unwrap();
    let (sequence, ok) = read_packet_full(client);
    assert_eq!(sequence, 1);
    assert_eq!(ok[0], 0x00, "prepare failed for `{sql}`: {ok:?}");
    let statement_id = u32::from_le_bytes([ok[1], ok[2], ok[3], ok[4]]);
    let columns = u16::from_le_bytes([ok[5], ok[6]]);
    let parameters = u16::from_le_bytes([ok[7], ok[8]]);
    for _ in 0..parameters {
        let (_, definition) = read_packet_full(client);
        assert_eq!(extract_column_name(&definition), "?");
    }
    if parameters > 0 {
        let (_, eof) = read_packet_full(client);
        assert_eq!(eof[0], 0xFE);
    }
    let mut names = Vec::new();
    let mut types = Vec::new();
    for _ in 0..columns {
        let (_, definition) = read_packet_full(client);
        names.push(extract_column_name(&definition));
        types.push(column_type_byte(&definition));
    }
    if columns > 0 {
        let (_, eof) = read_packet_full(client);
        assert_eq!(eof[0], 0xFE);
    }
    (statement_id, columns, parameters, names, types)
}

/// 发送 `COM_STMT_PREPARE` 并断言失败，返回 (error_code, sqlstate)。
fn prepare_error(client: &mut TcpStream, sql: &str) -> (u16, String) {
    let mut payload = vec![0x16];
    payload.extend_from_slice(sql.as_bytes());
    client.write_all(&encode_packet(0, &payload).unwrap()).unwrap();
    let (_, packet) = read_packet_full(client);
    assert_eq!(packet[0], 0xFF, "prepare unexpectedly succeeded for `{sql}`: {packet:?}");
    (u16::from_le_bytes([packet[1], packet[2]]), String::from_utf8_lossy(&packet[4..9]).into_owned())
}

/// 构造 `COM_STMT_EXECUTE`：`rebind` 为假时复用语句上一次绑定的参数类型。
fn execute_payload(statement_id: u32, types: &[(u8, bool)], values: &[Option<&[u8]>], rebind: bool) -> Vec<u8> {
    let mut payload = vec![0x17];
    payload.extend_from_slice(&statement_id.to_le_bytes());
    payload.push(0); // CURSOR_TYPE_NO_CURSOR
    payload.extend_from_slice(&1_u32.to_le_bytes()); // iteration count
    if values.is_empty() {
        return payload;
    }
    let mut bitmap = vec![0_u8; values.len().div_ceil(8)];
    for (index, value) in values.iter().enumerate() {
        if value.is_none() {
            bitmap[index / 8] |= 1 << (index % 8);
        }
    }
    payload.extend_from_slice(&bitmap);
    payload.push(u8::from(rebind));
    if rebind {
        assert_eq!(types.len(), values.len(), "one type per parameter");
        for (code, unsigned) in types {
            payload.push(*code);
            payload.push(if *unsigned { 0x80 } else { 0x00 });
        }
    }
    for value in values.iter().flatten() {
        payload.extend_from_slice(value);
    }
    payload
}

/// 以单字节长度前缀编码字符串参数（够用的 length-encoded string）。
fn lenenc_bytes(bytes: &[u8]) -> Vec<u8> {
    assert!(bytes.len() <= 250, "fixture strings stay in the one-byte length range");
    let mut out = vec![bytes.len() as u8];
    out.extend_from_slice(bytes);
    out
}

/// 读出列定义里的 `MYSQL_TYPE_*` 代码。
fn column_type_byte(definition: &[u8]) -> u8 {
    let mut cursor = 0usize;
    // def, schema, table, org_table, name, org_name
    for _ in 0..6 {
        let (length, consumed) = read_lenenc(&definition[cursor..]);
        cursor += consumed + length as usize;
    }
    definition[cursor + 1 + 2 + 4]
}

/// 发送 `COM_STMT_EXECUTE` 并读取第一个响应包。
fn execute(client: &mut TcpStream, payload: &[u8]) -> (u8, Vec<u8>) {
    client.write_all(&encode_packet(0, payload).unwrap()).unwrap();
    read_packet_full(client)
}

#[test]
fn prepared_statements_round_trip_typed_parameters_and_results() {
    let (mut client, server) = start_server_with_runtime();
    let _ = read_packet_full(&mut client); // handshake
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    let _ = read_packet_full(&mut client);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    // 列类型来自 catalog 声明，而不是结果行推断。
    let (select_id, columns, parameters, names, types) = prepare(&mut client, "SELECT id, name FROM users WHERE id = ?");
    assert_eq!((columns, parameters), (2, 1));
    assert_eq!(names, vec!["id".to_string(), "name".to_string()]);
    assert_eq!(types, vec![0x08, 0xFD]);

    let id_value = 42_i64.to_le_bytes();
    let payload = execute_payload(select_id, &[(0x08, false)], &[Some(&id_value)], true);
    let (_, count) = execute(&mut client, &payload);
    assert_eq!(read_lenenc(&count).0, 2);
    let (_, definition) = read_packet_full(&mut client);
    assert_eq!(extract_column_name(&definition), "id");
    let (_, definition) = read_packet_full(&mut client);
    assert_eq!(extract_column_name(&definition), "name");
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);
    // prepared statement 的结果行使用 binary protocol：header + NULL 位图 + 类型化值。
    let (_, row) = read_packet_full(&mut client);
    assert_eq!(row[0], 0x00, "binary row header");
    assert_eq!(row[1], 0x00, "no NULL columns");
    assert_eq!(i64::from_le_bytes(row[2..10].try_into().unwrap()), 42);
    assert_eq!(row[10] as usize, b"alice".len());
    assert_eq!(&row[11..], b"alice");
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    // 复用上一次的参数类型（new_params_bound = 0）。
    let payload = execute_payload(select_id, &[], &[Some(&id_value)], false);
    let (_, count) = execute(&mut client, &payload);
    assert_eq!(read_lenenc(&count).0, 2);
    let _ = read_packet_full(&mut client);
    let _ = read_packet_full(&mut client);
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);
    let (_, row) = read_packet_full(&mut client);
    assert_eq!(i64::from_le_bytes(row[2..10].try_into().unwrap()), 42);
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    // NULL 通过位图传入，结果集为空但仍返回列定义。
    let (null_id, columns, parameters, _, _) = prepare(&mut client, "SELECT id FROM users WHERE name = ?");
    assert_eq!((columns, parameters), (1, 1));
    let payload = execute_payload(null_id, &[(0xFD, false)], &[None], true);
    let (_, count) = execute(&mut client, &payload);
    assert_eq!(read_lenenc(&count).0, 1);
    let (_, definition) = read_packet_full(&mut client);
    assert_eq!(extract_column_name(&definition), "id");
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    // 写入语句没有结果列，执行后按 affected rows 回 OK。
    let (insert_id, columns, parameters, _, _) = prepare(&mut client, "INSERT INTO users (id, name, age) VALUES (?, ?, ?)");
    assert_eq!((columns, parameters), (0, 3));
    let name_value = lenenc_bytes(b"carol");
    let age_value = 40_i64.to_le_bytes();
    let insert_value = 100_i64.to_le_bytes();
    let payload = execute_payload(
        insert_id,
        &[(0x08, false), (0xFD, false), (0x08, false)],
        &[Some(&insert_value), Some(&name_value), Some(&age_value)],
        true,
    );
    let (_, ok) = execute(&mut client, &payload);
    assert_eq!(ok[0], 0x00, "insert execute must answer OK: {ok:?}");
    assert_eq!(read_lenenc(&ok[1..]).0, 1);
    assert_eq!(expect_single_value(&mut client, "SELECT name FROM users WHERE id = 100"), b"carol".to_vec());

    // RESET 保留语句并回 OK，CLOSE 之后句柄不可再用。
    let mut reset = vec![0x1A];
    reset.extend_from_slice(&select_id.to_le_bytes());
    let (_, ok) = execute(&mut client, &reset);
    assert_eq!(ok[0], 0x00);
    let mut close = vec![0x19];
    close.extend_from_slice(&select_id.to_le_bytes());
    client.write_all(&encode_packet(0, &close).unwrap()).unwrap();
    let payload = execute_payload(select_id, &[], &[Some(&id_value)], false);
    let (_, error) = execute(&mut client, &payload);
    assert_eq!(error[0], 0xFF);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1243);

    drop(client);
    server.join().unwrap();
}

#[test]
fn prepared_statements_report_catalog_errors_and_reject_unknown_parameter_types() {
    let (mut client, server) = start_server_with_runtime();
    let _ = read_packet_full(&mut client);
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    let _ = read_packet_full(&mut client);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    let (code, state) = prepare_error(&mut client, "SELECT id FROM missing");
    assert_eq!((code, state.as_str()), (1146, "42S02"));
    let (code, state) = prepare_error(&mut client, "SELECT nope FROM users");
    assert_eq!((code, state.as_str()), (1054, "42S22"));
    let (code, state) = prepare_error(&mut client, "SELECT ( FROM users");
    assert_eq!((code, state.as_str()), (1064, "42000"));

    // 未实现的参数类型必须显式报错，不能静默按文本处理。
    let (statement_id, _, _, _, _) = prepare(&mut client, "SELECT id FROM users WHERE id = ?");
    let unsupported = 0_i64.to_le_bytes();
    let payload = execute_payload(statement_id, &[(0x10, false)], &[Some(&unsupported)], true);
    let (_, error) = execute(&mut client, &payload);
    assert_eq!(error[0], 0xFF);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1235);
    assert!(
        error.windows(4).any(|window| window == b"0x10"),
        "error must name the unsupported type: {}",
        String::from_utf8_lossy(&error)
    );

    // 未绑定的语句句柄按 MySQL 语义报 1243。
    let mut unknown = vec![0x17];
    unknown.extend_from_slice(&9_999_u32.to_le_bytes());
    unknown.push(0);
    unknown.extend_from_slice(&1_u32.to_le_bytes());
    let (_, error) = execute(&mut client, &unknown);
    assert_eq!(error[0], 0xFF);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1243);

    drop(client);
    server.join().unwrap();
}

/// 发送一条语句并读取第一个响应包。
fn exchange(client: &mut TcpStream, sql: &str) -> (u8, Vec<u8>) {
    client.write_all(&encode_packet(0, &sql_payload(sql)).unwrap()).unwrap();
    read_packet_full(client)
}

/// 发送一条语句并断言 OK 包。
fn expect_ok(client: &mut TcpStream, sql: &str) {
    let (_, packet) = exchange(client, sql);
    assert_eq!(packet[0], 0x00, "expected OK for `{sql}`, got {:?}", packet);
}

/// 发送一条语句并断言 ERR 包，返回 (error_code, sqlstate)。
fn expect_error(client: &mut TcpStream, sql: &str) -> (u16, String) {
    let (_, packet) = exchange(client, sql);
    assert_eq!(packet[0], 0xFF, "expected ERR for `{sql}`, got {:?}", packet);
    let code = u16::from_le_bytes([packet[1], packet[2]]);
    let state = String::from_utf8_lossy(&packet[4..9]).into_owned();
    (code, state)
}

/// 读取一个 SELECT 的首行首列。
fn expect_single_value(client: &mut TcpStream, sql: &str) -> Vec<u8> {
    client.write_all(&encode_packet(0, &sql_payload(sql)).unwrap()).unwrap();
    let (_, count) = read_packet_full(client);
    let columns = read_lenenc(&count).0 as usize;
    for _ in 0..columns {
        read_packet_full(client);
    }
    let (_, eof) = read_packet_full(client);
    assert_eq!(eof[0], 0xFE);
    let (_, row) = read_packet_full(client);
    let (value, _) = read_lenenc_str(&row);
    let value = value.to_vec();
    // 读掉行结束的 EOF，避免客户端带着未读数据关闭连接触发 RST。
    let (_, eof) = read_packet_full(client);
    assert_eq!(eof[0], 0xFE);
    value
}

/// `INSERT ... SELECT` 必须按写语句回 OK 包，affected rows 即源查询命中行数。
#[test]
fn mysql_insert_select_writes_rows_and_reports_affected_rows() {
    let (mut client, server) = start_server_with_runtime();
    let _ = read_packet_full(&mut client); // handshake
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet_full(&mut client).1[0], 0x00);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    let (_, ok) = exchange(&mut client, "INSERT INTO users (id, name, age) SELECT 43, 'bob', 20");
    assert_eq!(ok[0], 0x00, "expected OK packet, got {:?}", ok);
    assert_eq!(ok[1], 1, "inserted row must be reported as affected rows");
    assert_eq!(expect_single_value(&mut client, "SELECT name FROM users WHERE id = 43"), b"bob");

    // 源查询命中已存在的主键 → 唯一约束错误。
    assert_eq!(
        expect_error(&mut client, "INSERT INTO users (id, name, age) SELECT id, name, age FROM users"),
        (1062, "23000".into())
    );
    // 列数不符 → 语法错误，类型不符 → 类型错误。
    assert_eq!(expect_error(&mut client, "INSERT INTO users (id, name, age) SELECT id FROM users"), (1064, "42000".into()));
    assert_eq!(
        expect_error(&mut client, "INSERT INTO users (id, name, age) SELECT name, name, name FROM users"),
        (1366, "HY000".into())
    );
    // MySQL 没有 `ON CONFLICT` 语法，必须按语法错误拒绝而不是近似成 `INSERT IGNORE`。
    assert_eq!(
        expect_error(&mut client, "INSERT INTO users (id, name, age) VALUES (44, 'carol', 40) ON CONFLICT (id) DO NOTHING"),
        (1064, "42000".into())
    );
    // `INSERT ... RETURNING` 同样不在 MySQL 语法里，不能静默丢掉投影。
    assert_eq!(
        expect_error(&mut client, "INSERT INTO users (id, name, age) VALUES (45, 'dave', 50) RETURNING id"),
        (1064, "42000".into())
    );

    drop(client);
    server.join().unwrap();
}

/// 两条连接共享同一个 runtime，接受两次连接后各起一个服务线程。
fn start_two_client_server() -> (TcpStream, TcpStream, thread::JoinHandle<()>) {
    use std::sync::Arc;
    use yyds_execution::LocalExecutor;
    use yyds_gateway::SqlCatalog;
    use yyds_gateway_mysql::connection::MysqlSqlRuntime;
    use yyds_kv::MemoryShard;
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let source = "table users { @@id: i64, name: utf8 }\n";
    let projection = vos::parse_oak(source).expect("Oak parses").project_schema().expect("projection");
    let manifest = vos::types::IdentityManifest {
        format_version: vos::types::IDENTITY_MANIFEST_VERSION.into(),
        types: vec![vos::types::TypeIdentity {
            canonical_path: vec!["users".into()],
            type_id: 1,
            kind: vos::types::TypeContractKind::Table,
            fields: vec![
                vos::types::FieldIdentity { canonical_name: "id".into(), field_id: 1, virtual_field_index: Some(0) },
                vos::types::FieldIdentity { canonical_name: "name".into(), field_id: 2, virtual_field_index: Some(1) },
            ],
        }],
    };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    let catalog = SqlCatalog::from_resolved_contract(7, &contract).expect("gateway catalog");

    let mut shard = MemoryShard::new();
    put_row(&mut shard, "table:1", b"42", &[(1, b"42"), (2, b"alice")]);

    let epoch = ShardEpoch(1);
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(epoch, vec![shard_id.clone()]).expect("routing");
    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    let executor = LocalExecutor::new(routing, shards).expect("executor");

    let runtime = Arc::new(MysqlSqlRuntime { catalog, schema_version: 7, executor: Arc::new(executor) });
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let address = listener.local_addr().unwrap();
    let server = thread::spawn(move || {
        let (first, _) = listener.accept().unwrap();
        let (second, _) = listener.accept().unwrap();
        let runtime_a = Arc::clone(&runtime);
        let runtime_b = runtime;
        let a = thread::spawn(move || serve_connection(first, Some(runtime_a)).unwrap());
        let b = thread::spawn(move || serve_connection(second, Some(runtime_b)).unwrap());
        a.join().unwrap();
        b.join().unwrap();
    });
    let mut client_a = TcpStream::connect(address).unwrap();
    let mut client_b = TcpStream::connect(address).unwrap();
    for client in [&mut client_a, &mut client_b] {
        client.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
        let _ = read_packet_full(client); // handshake
        client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
        let (_, ok) = read_packet_full(client);
        assert_eq!(ok[0], 0x00);
        expect_ok(client, "SET NAMES utf8mb4");
    }
    (client_a, client_b, server)
}

#[test]
fn mysql_commit_reports_occ_conflict_instead_of_silently_dropping_the_write() {
    let (mut first, mut second, server) = start_two_client_server();

    expect_ok(&mut first, "BEGIN");
    expect_ok(&mut second, "BEGIN");

    // 两个会话读到同一 pre-image，然后各自 read-modify-write。
    expect_ok(&mut first, "UPDATE users SET name = 't1' WHERE id = 42");
    expect_ok(&mut second, "UPDATE users SET name = 't2' WHERE id = 42");

    expect_ok(&mut first, "COMMIT");
    let (code, state) = expect_error(&mut second, "COMMIT");
    assert_eq!(code, 1213, "expected MySQL deadlock/serialization error code");
    assert_eq!(state, "40001", "expected SQLSTATE serialization failure");

    // 后提交事务的值不能覆盖先提交者。
    assert_eq!(expect_single_value(&mut first, "SELECT name FROM users WHERE id = 42"), b"t1".to_vec());

    drop(first);
    drop(second);
    server.join().unwrap();
}

#[test]
fn error_numbers_reflect_structured_gateway_errors() {
    let (mut client, server) = start_server_with_join_runtime();
    let _ = read_packet_full(&mut client); // handshake
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet_full(&mut client).1[0], 0x00);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    let cases: &[(&str, u16, &str)] = &[
        ("SELECT id FROM missing", 1146, "42S02"),
        ("SELECT nope.id FROM users", 1146, "42S02"),
        ("SELECT nope FROM users", 1054, "42S22"),
        ("SELECT id FROM users INNER JOIN orders ON users.id = orders.user_id", 1052, "23000"),
        ("PRAGMA user_version", 1064, "42000"),
        ("SELECT nope_fn(name) FROM users", 1305, "42000"),
        // MySQL 只有 `TRIM(chars FROM str)`，两参数函数形式是 PostgreSQL 语法。
        ("SELECT TRIM('xxaxx', 'x')", 1064, "42000"),
        // `EXTRACT` 没有依据的语义，按未知函数拒绝（`1305`），不编造抽取行为。
        ("SELECT EXTRACT(age) FROM users", 1305, "42000"),
        // 日期函数不接受参数。
        ("SELECT CURDATE(1)", 1305, "42000"),
        // 精确整数越界是 MySQL 的 `1690`，SQLSTATE 与 PostgreSQL 同为 `22003`。
        ("SELECT 9223372036854775807 + 1", 1690, "22003"),
        // 外连接已受支持：未限定列名报歧义，而不是「不支持」。
        ("SELECT id FROM users LEFT JOIN orders ON users.id = orders.user_id", 1052, "23000"),
        ("SELECT id FROM users RIGHT JOIN orders ON users.id = orders.user_id", 1052, "23000"),
        ("SELECT id FROM users FULL JOIN orders ON users.id = orders.user_id", 1052, "23000"),
        ("INSERT INTO users (id, name) VALUES (3, 42)", 1366, "HY000"),
        ("INSERT INTO users (id, name) VALUES (NULL, 'x')", 1048, "23000"),
        ("INSERT INTO users (id, name) VALUES (1, 'dup')", 1062, "23000"),
    ];
    for (sql, expected_code, expected_state) in cases {
        let (code, state) = expect_error(&mut client, sql);
        assert_eq!((code, state.as_str()), (*expected_code, *expected_state), "unexpected error for `{sql}`");
    }

    drop(client);
    server.join().unwrap();
}

/// 同名列限定后可正常执行：未匹配左行保留，右侧列以 `NULL`（`0xFB`）补齐。
#[test]
fn mysql_left_join_keeps_unmatched_left_rows_with_null_right_side() {
    let (mut client, server) = start_server_with_join_runtime();
    let _ = read_packet_full(&mut client); // handshake
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet_full(&mut client).1[0], 0x00);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    // 第三个用户没有订单，必须作为未匹配左行保留。
    expect_ok(&mut client, "INSERT INTO users (id, name) VALUES (3, 'carol')");

    let sql = "SELECT users.id, orders.total FROM users LEFT JOIN orders ON users.id = orders.user_id ORDER BY users.id";
    client.write_all(&encode_packet(0, &sql_payload(sql)).unwrap()).unwrap();
    let (_, count_packet) = read_packet_full(&mut client);
    assert_eq!(read_lenenc(&count_packet).0, 2, "`{sql}` projects two columns");
    let (_, definition) = read_packet_full(&mut client);
    assert_eq!(extract_column_name(&definition), "id");
    let (_, definition) = read_packet_full(&mut client);
    assert_eq!(extract_column_name(&definition), "total");
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    let mut joined = Vec::new();
    for _ in 0..3 {
        let (_, row) = read_packet_full(&mut client);
        let (id, consumed) = read_lenenc_str(&row);
        let (total, _) = read_lenenc_str(&row[consumed..]);
        joined.push((id.to_vec(), total.to_vec()));
    }
    assert_eq!(
        joined,
        vec![(b"1".to_vec(), b"100".to_vec()), (b"2".to_vec(), b"70".to_vec()), (b"3".to_vec(), Vec::new())],
        "the matched left rows carry the right value and the unmatched one stays `NULL`"
    );
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    drop(client);
    server.join().unwrap();
}

/// 窗口函数的排名列在 MySQL 协议上必须声明为 `LONGLONG`（`0x08`），与普通整数列
/// 一致。列类型取自计划的声明类型，不按返回值的文本形状推断成 `VAR_STRING`。
#[test]
fn window_rank_columns_are_declared_as_longlong() {
    let (mut client, server) = start_server_with_runtime();
    let _ = read_packet_full(&mut client); // handshake
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet_full(&mut client).1[0], 0x00);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    let sql = "SELECT id, ROW_NUMBER() OVER (ORDER BY id), RANK() OVER (ORDER BY id) FROM users";
    client.write_all(&encode_packet(0, &sql_payload(sql)).unwrap()).unwrap();

    let (_, count_packet) = read_packet_full(&mut client);
    assert_eq!(read_lenenc(&count_packet).0, 3);

    for name in ["id", "ROW_NUMBER", "RANK"] {
        let (_, definition) = read_packet_full(&mut client);
        assert_eq!(extract_column_name(&definition), name);
        assert_eq!(column_type_byte(&definition), 0x08, "`{name}` must be declared as `LONGLONG`");
    }

    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    let (_, row) = read_packet_full(&mut client);
    let (id, consumed) = read_lenenc_str(&row);
    assert_eq!(id, b"42");
    let (number, consumed_number) = read_lenenc_str(&row[consumed..]);
    assert_eq!(number, b"1");
    let (rank, consumed_rank) = read_lenenc_str(&row[consumed + consumed_number..]);
    assert_eq!(rank, b"1");
    assert_eq!(consumed + consumed_number + consumed_rank, row.len());

    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    drop(client);
    server.join().unwrap();
}

/// 嵌套调用不改变列的声明类型：聚合实参里的标量调用让外层聚合照旧声明
/// `LONGLONG`，`COALESCE(LOWER(name), UPPER(name))` 照旧声明 `VAR_STRING`。
#[test]
fn nested_call_columns_keep_declared_types() {
    let (mut client, server) = start_server_with_runtime();
    let _ = read_packet_full(&mut client); // handshake
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet_full(&mut client).1[0], 0x00);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    let sql = "SELECT SUM(LENGTH(name)) AS total_len, SUM(CHAR_LENGTH(name)) AS total_chars FROM users";
    client.write_all(&encode_packet(0, &sql_payload(sql)).unwrap()).unwrap();

    let (_, count_packet) = read_packet_full(&mut client);
    assert_eq!(read_lenenc(&count_packet).0, 2);
    for name in ["total_len", "total_chars"] {
        let (_, definition) = read_packet_full(&mut client);
        assert_eq!(extract_column_name(&definition), name);
        assert_eq!(column_type_byte(&definition), 0x08, "`{name}` must be declared as `LONGLONG`");
    }
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    let (_, row) = read_packet_full(&mut client);
    let (first, consumed) = read_lenenc_str(&row);
    let (second, consumed_second) = read_lenenc_str(&row[consumed..]);
    assert_eq!((first, second), (b"5".as_slice(), b"5".as_slice()));
    assert_eq!(consumed + consumed_second, row.len());
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    let sql = "SELECT COALESCE(LOWER(name), UPPER(name)) AS name_label FROM users";
    client.write_all(&encode_packet(0, &sql_payload(sql)).unwrap()).unwrap();

    let (_, count_packet) = read_packet_full(&mut client);
    assert_eq!(read_lenenc(&count_packet).0, 1);
    let (_, definition) = read_packet_full(&mut client);
    assert_eq!(extract_column_name(&definition), "name_label");
    assert_eq!(column_type_byte(&definition), 0xFD, "`COALESCE` must be declared as `VAR_STRING`");
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    let (_, row) = read_packet_full(&mut client);
    assert_eq!(read_lenenc_str(&row), (b"alice".as_slice(), row.len()));
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    drop(client);
    server.join().unwrap();
}

/// `CURDATE()` / `CURTIME()` 是 MySQL 拼写，与 `NOW()` 一样返回 UTC 文本。
#[test]
fn mysql_date_spellings_render_utc_text_rows() {
    let (mut client, server) = start_server_with_runtime();
    let _ = read_packet_full(&mut client); // handshake
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet_full(&mut client).1[0], 0x00);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    let date = expect_single_value(&mut client, "SELECT CURDATE()");
    assert_eq!(date.len(), 10, "`CURDATE()` renders `YYYY-MM-DD`, got {:?}", String::from_utf8_lossy(&date));
    let time = expect_single_value(&mut client, "SELECT CURTIME()");
    assert_eq!(time.len(), 8, "`CURTIME()` renders `HH:MM:SS`, got {:?}", String::from_utf8_lossy(&time));
    let stamp = expect_single_value(&mut client, "SELECT NOW()");
    assert_eq!(stamp.len(), 19, "`NOW()` renders `YYYY-MM-DD HH:MM:SS`, got {:?}", String::from_utf8_lossy(&stamp));

    drop(client);
    server.join().unwrap();
}

/// `SAVEPOINT sp_name`、`ROLLBACK TO sp_name`、`RELEASE sp_name` 是事务内的
/// 有线协议命令：MySQL 全部回 OK 包，错误名则按 `1305` 拒绝。事务外发出
/// 的 `SAVEPOINT` 必须显式失败，而不是被静默忽略。
#[test]
fn mysql_savepoint_commands_emit_ok_and_validate_names() {
    let (mut client, server) = start_server_with_runtime();
    let _ = read_packet_full(&mut client); // handshake
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet_full(&mut client).1[0], 0x00);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    // 事务外发 SAVEPOINT 必须报错：MySQL 规定 savepoint 只能在事务内。
    let (code, state) = expect_error(&mut client, "SAVEPOINT sp");
    assert_eq!((code, state.as_str()), (1305, "42000"));

    expect_ok(&mut client, "BEGIN");
    expect_ok(&mut client, "SAVEPOINT sp");
    expect_ok(&mut client, "SAVEPOINT another");

    // 重复名字按 MySQL 语义报 1305。
    let (code, state) = expect_error(&mut client, "SAVEPOINT sp");
    assert_eq!((code, state.as_str()), (1305, "42000"));

    // 未知名字的 ROLLBACK TO / RELEASE SAVEPOINT 同码。
    let (code, state) = expect_error(&mut client, "ROLLBACK TO nope");
    assert_eq!((code, state.as_str()), (1305, "42000"));
    let (code, state) = expect_error(&mut client, "RELEASE SAVEPOINT nope");
    assert_eq!((code, state.as_str()), (1305, "42000"));

    expect_ok(&mut client, "ROLLBACK TO another");
    expect_ok(&mut client, "RELEASE SAVEPOINT sp");

    drop(client);
    server.join().unwrap();
}

/// 事务内写→savepoint→写→rollback-to 必须把状态恢复到 savepoint 时刻，
/// savepoint 之后的写入全部丢弃。
#[test]
fn mysql_savepoint_rollback_restores_snapshot() {
    let (mut client, server) = start_server_with_join_runtime();
    let _ = read_packet_full(&mut client);
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet_full(&mut client).1[0], 0x00);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    expect_ok(&mut client, "BEGIN");
    expect_ok(&mut client, "INSERT INTO users (id, name) VALUES (3, 'carol')");
    expect_ok(&mut client, "SAVEPOINT sp");
    expect_ok(&mut client, "INSERT INTO users (id, name) VALUES (4, 'dave')");

    // ROLLBACK TO 之后只看到 savepoint 之前的写入。
    expect_ok(&mut client, "ROLLBACK TO sp");
    assert_eq!(expect_single_value(&mut client, "SELECT COUNT(*) FROM users"), b"3".to_vec());
    let name = expect_single_value(&mut client, "SELECT name FROM users WHERE id = 3");
    assert_eq!(name, b"carol");

    expect_ok(&mut client, "COMMIT");
    assert_eq!(expect_single_value(&mut client, "SELECT COUNT(*) FROM users"), b"3".to_vec());

    drop(client);
    server.join().unwrap();
}

/// `RELEASE SAVEPOINT sp` 从 savepoint 栈上移除命名条目，但不回滚任何状态。
/// 之后 ROLLBACK TO 该名字必须失败。
/// 注意：ROLLBACK TO 失败会标记 session 为 failed，需要显式 ROLLBACK 才能恢复。
#[test]
fn mysql_savepoint_release_removes_name_without_dropping_state() {
    let (mut client, server) = start_server_with_join_runtime();
    let _ = read_packet_full(&mut client);
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet_full(&mut client).1[0], 0x00);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    expect_ok(&mut client, "BEGIN");
    expect_ok(&mut client, "SAVEPOINT a");
    expect_ok(&mut client, "SAVEPOINT b");
    expect_ok(&mut client, "INSERT INTO users (id, name) VALUES (3, 'carol')");

    expect_ok(&mut client, "RELEASE SAVEPOINT a");
    // 释放之后状态仍然可见——RELEASE 不回滚。
    assert_eq!(expect_single_value(&mut client, "SELECT COUNT(*) FROM users"), b"3".to_vec());

    // 已释放的名字不可再 rollback。
    let (code, state) = expect_error(&mut client, "ROLLBACK TO a");
    assert_eq!((code, state.as_str()), (1305, "42000"));

    // ROLLBACK TO 失败会标记 session 为 failed，必须显式 ROLLBACK 才能继续。
    expect_ok(&mut client, "ROLLBACK");
    // 整个事务回滚，INSERT id=3 也被撤销。
    assert_eq!(expect_single_value(&mut client, "SELECT COUNT(*) FROM users"), b"2".to_vec());

    drop(client);
    server.join().unwrap();
}

/// MySQL `SET unknown_var = value` must report error 1193 with SQLSTATE HY000
/// rather than being silently accepted or using the wrong SQLSTATE class.
/// Known-but-unsupported variables such as `time_zone` must be accepted.
#[test]
fn mysql_set_rejects_unknown_variables_with_1193_hy000() {
    let (mut client, server) = start_server_with_runtime();
    let _ = read_packet_full(&mut client);
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet_full(&mut client).1[0], 0x00);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    // 未知变量 → 1193 / HY000，不能静默吞掉。
    let (code, state) = expect_error(&mut client, "SET nope_var = 1");
    assert_eq!((code, state.as_str()), (1193, "HY000"));

    // 带作用域前缀的未知变量同样报错。
    let (code, state) = expect_error(&mut client, "SET @@session.nope_var = 1");
    assert_eq!((code, state.as_str()), (1193, "HY000"));

    // 已知但 gateway 暂不实际生效的变量（如 time_zone、character_set_client）
    // 必须接受为 no-op，不报 unknown。
    expect_ok(&mut client, "SET time_zone = '+00:00'");
    expect_ok(&mut client, "SET character_set_client = 'utf8mb4'");
    expect_ok(&mut client, "SET transaction_isolation = 'READ-COMMITTED'");

    // autocommit 切换仍被跟踪到 session 状态：SET autocommit=0 的 OK 包
    // 必须不带 SERVER_STATUS_AUTOCOMMIT，SET autocommit=1 必须带上。
    let (_, ok) = exchange(&mut client, "SET autocommit = 0");
    assert_eq!(ok[0], 0x00);
    assert_eq!(u16::from_le_bytes([ok[3], ok[4]]) & 0x0002, 0, "autocommit=0 must clear SERVER_STATUS_AUTOCOMMIT");

    let (_, ok) = exchange(&mut client, "SET autocommit = 1");
    assert_eq!(ok[0], 0x00);
    assert_ne!(u16::from_le_bytes([ok[3], ok[4]]) & 0x0002, 0, "autocommit=1 must set SERVER_STATUS_AUTOCOMMIT");

    drop(client);
    server.join().unwrap();
}

/// 读出列定义里的字符集 id（MySQL `binary` 为 63）。
fn column_charset(definition: &[u8]) -> u16 {
    let mut cursor = 0usize;
    // def, schema, table, org_table, name, org_name
    for _ in 0..6 {
        let (length, consumed) = read_lenenc(&definition[cursor..]);
        cursor += consumed + length as usize;
    }
    // fixed length (1) + charset (2)
    u16::from_le_bytes([definition[cursor + 1], definition[cursor + 2]])
}

/// 含 `bytes?` 列的 runtime：`payloads(id, blob)`，无预置行。
fn start_server_with_payload_runtime() -> (TcpStream, thread::JoinHandle<()>) {
    use std::sync::Arc;
    use yyds_execution::LocalExecutor;
    use yyds_gateway::SqlCatalog;
    use yyds_gateway_mysql::connection::MysqlSqlRuntime;
    use yyds_kv::MemoryShard;
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    let source = "table payloads { @@id: i64, blob: bytes? }\n";
    let projection = vos::parse_oak(source).expect("Oak parses").project_schema().expect("projection");
    let manifest = vos::types::IdentityManifest {
        format_version: vos::types::IDENTITY_MANIFEST_VERSION.into(),
        types: vec![vos::types::TypeIdentity {
            canonical_path: vec!["payloads".into()],
            type_id: 1,
            kind: vos::types::TypeContractKind::Table,
            fields: vec![
                vos::types::FieldIdentity { canonical_name: "id".into(), field_id: 1, virtual_field_index: Some(0) },
                vos::types::FieldIdentity { canonical_name: "blob".into(), field_id: 2, virtual_field_index: Some(1) },
            ],
        }],
    };
    let contract = vos::resolve_contract(&projection, &manifest).expect("resolved contract");
    let catalog = SqlCatalog::from_resolved_contract(7, &contract).expect("gateway catalog");

    let shard = MemoryShard::new();
    let epoch = ShardEpoch(1);
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(epoch, vec![shard_id.clone()]).expect("routing");
    let mut shards = std::collections::HashMap::new();
    shards.insert(shard_id, shard);
    let executor = LocalExecutor::new(routing, shards).expect("executor");

    let runtime = Arc::new(MysqlSqlRuntime { catalog, schema_version: 7, executor: Arc::new(executor) });
    spawn_runtime_server(runtime)
}

/// `bytes` 列在二进制协议（`COM_STMT_EXECUTE`）上原样往返 NUL、非 UTF-8 与空载荷，
/// 结果列声明为 `BLOB` + binary 字符集 63。文本协议只能经十六进制 `CAST` 写入，
/// 非 UTF-8 的查询文本按 `1300` 稳定拒绝。
#[test]
fn blob_columns_round_trip_raw_octets_and_reject_text_only_paths() {
    let (mut client, server) = start_server_with_payload_runtime();
    let _ = read_packet_full(&mut client); // handshake
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet_full(&mut client).1[0], 0x00);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    // NUL、0xFF、0xFE、非 UTF-8 尾字节。
    let payload: &[u8] = &[0x00, 0xFF, 0xFE, 0x00, 0x41, 0xC3, 0x28];

    let (insert_id, columns, parameters, _, _) = prepare(&mut client, "INSERT INTO payloads (id, blob) VALUES (?, ?)");
    assert_eq!((columns, parameters), (0, 2));

    let id_one = 1_i64.to_le_bytes();
    let blob_one = lenenc_bytes(payload);
    let statement = execute_payload(insert_id, &[(0x08, false), (0xFC, false)], &[Some(&id_one), Some(&blob_one)], true);
    let (_, ok) = execute(&mut client, &statement);
    assert_eq!(ok[0], 0x00, "binary `BLOB` parameter must round trip: {ok:?}");
    assert_eq!(read_lenenc(&ok[1..]).0, 1);

    // 零长载荷必须是 0 长度，而不是被当成 NULL。
    let id_two = 2_i64.to_le_bytes();
    let blob_two = lenenc_bytes(b"");
    let statement = execute_payload(insert_id, &[], &[Some(&id_two), Some(&blob_two)], false);
    let (_, ok) = execute(&mut client, &statement);
    assert_eq!(ok[0], 0x00, "empty `BLOB` payload must be accepted: {ok:?}");

    // 文本协议无法携带任意 octets，唯一通道是十六进制 `CAST`。
    expect_ok(&mut client, "INSERT INTO payloads (id, blob) VALUES (3, CAST('00FF' AS BLOB))");
    // NULL 载荷由字段缺失表示。
    expect_ok(&mut client, "INSERT INTO payloads (id, blob) VALUES (4, NULL)");

    // prepared 结果列：`id` 为 `LONGLONG`，`blob` 为 `BLOB` 且字符集 63（binary）。
    let (select_id, columns, parameters, names, types) = prepare(&mut client, "SELECT id, blob FROM payloads ORDER BY id");
    assert_eq!((columns, parameters), (2, 0));
    assert_eq!(names, vec!["id".to_string(), "blob".to_string()]);
    assert_eq!(types, vec![0x08, 0xFC]);
    // prepare 阶段已经校验过列定义，这里再确认二进制结果集的声明。
    let statement = execute_payload(select_id, &[], &[], true);
    let (_, count) = execute(&mut client, &statement);
    assert_eq!(read_lenenc(&count).0, 2);
    let (_, definition) = read_packet_full(&mut client);
    assert_eq!(extract_column_name(&definition), "id");
    assert_eq!(column_type_byte(&definition), 0x08);
    let (_, definition) = read_packet_full(&mut client);
    assert_eq!(extract_column_name(&definition), "blob");
    assert_eq!(column_type_byte(&definition), 0xFC);
    assert_eq!(column_charset(&definition), 63, "`BLOB` columns must declare the binary character set");
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    // 二进制行：header + NULL 位图 + 类型化值。两列只需 1 字节位图。
    let (_, row) = read_packet_full(&mut client);
    assert_eq!(row[0], 0x00, "binary row header");
    assert_eq!(row[1], 0x00, "id=1 carries no NULL columns");
    assert_eq!(i64::from_le_bytes(row[2..10].try_into().unwrap()), 1);
    let (blob, consumed) = read_lenenc_str(&row[10..]);
    assert_eq!(blob, payload, "raw octets must survive the binary result row");
    assert_eq!(10 + consumed, row.len());

    let (_, row) = read_packet_full(&mut client);
    assert_eq!(row[1], 0x00);
    assert_eq!(i64::from_le_bytes(row[2..10].try_into().unwrap()), 2);
    assert_eq!(read_lenenc_str(&row[10..]), (&[][..], 1), "empty payload is a zero-length value");

    let (_, row) = read_packet_full(&mut client);
    assert_eq!(i64::from_le_bytes(row[2..10].try_into().unwrap()), 3);
    assert_eq!(read_lenenc_str(&row[10..]).0, &[0x00, 0xFF], "text `CAST` writes the hexadecimal payload");

    let (_, row) = read_packet_full(&mut client);
    assert_eq!(i64::from_le_bytes(row[2..10].try_into().unwrap()), 4);
    assert_eq!(row[1], 0x08, "`blob` NULL bit (column index 1 + 2) must be set in the bitmap");
    assert_eq!(row.len(), 10, "NULL columns carry no value bytes");
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    // 文本协议结果：列元数据仍是 `BLOB` + charset 63，行载荷原样发出。
    client.write_all(&encode_packet(0, &sql_payload("SELECT blob FROM payloads WHERE id = 1")).unwrap()).unwrap();
    let (_, count) = read_packet_full(&mut client);
    assert_eq!(read_lenenc(&count).0, 1);
    let (_, definition) = read_packet_full(&mut client);
    assert_eq!(extract_column_name(&definition), "blob");
    assert_eq!(column_type_byte(&definition), 0xFC);
    assert_eq!(column_charset(&definition), 63);
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);
    let (_, row) = read_packet_full(&mut client);
    assert_eq!(read_lenenc_str(&row), (payload, row.len()));
    let (_, eof) = read_packet_full(&mut client);
    assert_eq!(eof[0], 0xFE);

    // 文本协议无法承载非 UTF-8 的查询文本，必须稳定拒绝而不是猜测重建。
    let mut query = vec![0x03];
    query.extend_from_slice(b"SELECT '");
    query.push(0xFF);
    query.extend_from_slice(b"' AS raw");
    client.write_all(&encode_packet(0, &query).unwrap()).unwrap();
    let (_, error) = read_packet_full(&mut client);
    assert_eq!(error[0], 0xFF, "non-UTF-8 query text must be rejected: {error:?}");
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1300);

    // 非法十六进制拼写按语法错误拒绝，不截断成部分载荷。
    assert_eq!(
        expect_error(&mut client, "INSERT INTO payloads (id, blob) VALUES (5, CAST('0G' AS BLOB))"),
        (1064, "42000".into())
    );

    // 超出声明容量在 bind 期拒绝，而不是写入截断值。
    let oversized = "00".repeat(yyds_gateway::SqlFieldType::BYTES_CAPACITY + 1);
    let oversized_sql = format!("INSERT INTO payloads (id, blob) VALUES (5, CAST('{oversized}' AS BLOB))");
    assert_eq!(expect_error(&mut client, &oversized_sql), (1366, "HY000".into()));

    drop(client);
    server.join().unwrap();
}

/// 事务内语法错误或唯一约束违反会把 MySQL session 切到 failed 状态，
/// 后续语句被拒绝。ROLLBACK 之后恢复。
#[test]
fn mysql_transaction_error_recovery_after_unique_violation() {
    let (mut client, server) = start_server_with_runtime();
    let _ = read_packet_full(&mut client);
    client.write_all(&encode_packet(1, &handshake_response(&[])).unwrap()).unwrap();
    assert_eq!(read_packet_full(&mut client).1[0], 0x00);
    expect_ok(&mut client, "SET NAMES utf8mb4");

    expect_ok(&mut client, "BEGIN");

    // users 预置了 id=42，重复写入触发唯一约束 → 1062，session 标记 failed。
    let (code, state) = expect_error(&mut client, "INSERT INTO users VALUES (42, 'dup', 0)");
    assert_eq!((code, state.as_str()), (1062, "23000"));

    // Failed 事务内后续普通 SELECT 被拒绝（error 40001）。
    let (code, _) = expect_error(&mut client, "SELECT id FROM users");
    assert_eq!(code, 40001, "failed transaction must abort subsequent statements");

    // ROLLBACK 回到正常状态。
    expect_ok(&mut client, "ROLLBACK");

    // 恢复后正常查询通过。
    let _ = expect_single_value(&mut client, "SELECT id FROM users WHERE id = 42");

    drop(client);
    server.join().unwrap();
}
