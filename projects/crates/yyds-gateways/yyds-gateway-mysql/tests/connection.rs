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
    let manifest = vos::contract::IdentityManifest {
        format_version: vos::contract::IDENTITY_MANIFEST_VERSION.into(),
        types: vec![vos::contract::TypeIdentity {
            canonical_path: vec!["users".into()],
            type_id: 1,
            kind: vos::contract::TypeContractKind::Table,
            fields: vec![
                vos::contract::FieldIdentity { canonical_name: "id".into(), field_id: 1, virtual_field_index: 0 },
                vos::contract::FieldIdentity { canonical_name: "name".into(), field_id: 2, virtual_field_index: 1 },
                vos::contract::FieldIdentity { canonical_name: "age".into(), field_id: 3, virtual_field_index: 2 },
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
    let manifest = vos::contract::IdentityManifest {
        format_version: vos::contract::IDENTITY_MANIFEST_VERSION.into(),
        types: vec![
            vos::contract::TypeIdentity {
                canonical_path: vec!["users".into()],
                type_id: 1,
                kind: vos::contract::TypeContractKind::Table,
                fields: vec![
                    vos::contract::FieldIdentity { canonical_name: "id".into(), field_id: 1, virtual_field_index: 0 },
                    vos::contract::FieldIdentity { canonical_name: "name".into(), field_id: 2, virtual_field_index: 1 },
                ],
            },
            vos::contract::TypeIdentity {
                canonical_path: vec!["orders".into()],
                type_id: 2,
                kind: vos::contract::TypeContractKind::Table,
                fields: vec![
                    vos::contract::FieldIdentity { canonical_name: "id".into(), field_id: 3, virtual_field_index: 0 },
                    vos::contract::FieldIdentity { canonical_name: "user_id".into(), field_id: 4, virtual_field_index: 1 },
                    vos::contract::FieldIdentity { canonical_name: "total".into(), field_id: 5, virtual_field_index: 2 },
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
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1064);
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

    // SET autocommit = 0 与引擎「语句立即提交」矛盾，必须显式拒绝
    client.write_all(&encode_packet(0, b"\x03SET autocommit = 0").unwrap()).unwrap();
    let (sequence, error) = read_packet(&mut client);
    assert_eq!(sequence, 1);
    assert_eq!(error[0], 0xff);
    assert_eq!(u16::from_le_bytes([error[1], error[2]]), 1235);

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
    let manifest = vos::contract::IdentityManifest {
        format_version: vos::contract::IDENTITY_MANIFEST_VERSION.into(),
        types: vec![vos::contract::TypeIdentity {
            canonical_path: vec!["users".into()],
            type_id: 1,
            kind: vos::contract::TypeContractKind::Table,
            fields: vec![
                vos::contract::FieldIdentity { canonical_name: "id".into(), field_id: 1, virtual_field_index: 0 },
                vos::contract::FieldIdentity { canonical_name: "name".into(), field_id: 2, virtual_field_index: 1 },
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
