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

    send_query(&mut client, "SHOW server_version");
    let shown = receive_until_ready(&mut client);
    assert_eq!(shown.len(), 4);
    assert_eq!(shown[0].0, b'T');
    assert_eq!(shown[1].0, b'D');
    assert!(shown[1].1.windows(4).any(|window| window == b"16.4"));
    assert_eq!(shown[2], (b'C', b"SHOW\0".to_vec()));
    assert_eq!(shown[3], (b'Z', b"I".to_vec()));

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

/// 带 SQL runtime 的服务端，用于验证 PostgreSQL 语法下 `@@` 必须被拒绝。
fn start_server_with_runtime() -> (TcpStream, thread::JoinHandle<()>) {
    use std::sync::Arc;
    use yyds_execution::LocalExecutor;
    use yyds_gateway::SqlCatalog;
    use yyds_gateway_pgsql::connection::{serve_connection, PgsqlSqlRuntime};
    use yyds_kv::{KvStore, MemoryShard};
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

    // `@@` 在 PostgreSQL 里不是变量语法，必须报绑定错误而不是当作列名。
    send_query(&mut client, "SELECT @@version");
    let rejected = receive_until_ready(&mut client);
    assert_eq!(rejected[0].0, b'E');
    assert!(rejected[0].1.windows(5).any(|window| window == b"42P01"));
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
