use std::collections::HashMap;

use yyds_execution::{KeyValueExecutor, LocalExecutor};
use yyds_kv::FileShard;
use yyds_types::{KeyValueAction, KeyValueCommand, KeyValueResult, Namespace, ShardEpoch, ShardId, ShardMap};

fn command(action: KeyValueAction, key: &[u8]) -> KeyValueCommand {
    KeyValueCommand { namespace: Namespace("redis:0".into()), key: key.to_vec(), action }
}

#[test]
fn local_executor_routes_and_persists_binary_commands() {
    let directory = tempfile::tempdir().unwrap();
    let shard_id = ShardId("shard-a".into());
    let second_shard_id = ShardId("shard-b".into());
    let routing = ShardMap::new(ShardEpoch(3), vec![shard_id.clone(), second_shard_id.clone()]).unwrap();
    let shard = FileShard::open(directory.path().join("shard-a.yykv")).unwrap();
    let second_shard = FileShard::open(directory.path().join("shard-b.yykv")).unwrap();
    let mut shards = HashMap::new();
    shards.insert(shard_id, shard);
    shards.insert(second_shard_id.clone(), second_shard);
    let executor = LocalExecutor::new(routing, shards).unwrap();
    let key = (0..100)
        .map(|index| format!("key-{index}").into_bytes())
        .find(|key| executor.routing().route("redis:0", key).shard == second_shard_id)
        .unwrap();

    assert_eq!(executor.execute(command(KeyValueAction::Get, &key)).unwrap(), KeyValueResult::Get(None));
    assert_eq!(executor.execute(command(KeyValueAction::Exists, &key)).unwrap(), KeyValueResult::Exists(false));
    assert_eq!(
        executor.execute(command(KeyValueAction::Put(vec![0, 0xff, 0x80]), &key)).unwrap(),
        KeyValueResult::Put { revision: 1 },
    );
    assert_eq!(executor.execute(command(KeyValueAction::Get, &key)).unwrap(), KeyValueResult::Get(Some(vec![0, 0xff, 0x80])),);
    assert_eq!(executor.execute(command(KeyValueAction::Exists, &key)).unwrap(), KeyValueResult::Exists(true));
    assert_eq!(executor.execute(command(KeyValueAction::Delete, &key)).unwrap(), KeyValueResult::Delete { removed: true });
    assert_eq!(executor.execute(command(KeyValueAction::Exists, &key)).unwrap(), KeyValueResult::Exists(false));
    assert_eq!(executor.execute(command(KeyValueAction::Delete, &key)).unwrap(), KeyValueResult::Delete { removed: false });
    assert_eq!(executor.routing().epoch(), ShardEpoch(3));

    drop(executor);
    let reopened = FileShard::open(directory.path().join("shard-a.yykv")).unwrap();
    let reopened_second = FileShard::open(directory.path().join("shard-b.yykv")).unwrap();
    let mut shards = HashMap::new();
    shards.insert(ShardId("shard-a".into()), reopened);
    shards.insert(second_shard_id.clone(), reopened_second);
    let executor =
        LocalExecutor::new(ShardMap::new(ShardEpoch(3), vec![ShardId("shard-a".into()), second_shard_id]).unwrap(), shards)
            .unwrap();
    assert_eq!(executor.execute(command(KeyValueAction::Get, &key)).unwrap(), KeyValueResult::Get(None));
}

#[test]
fn executor_rejects_incomplete_shard_mounts() {
    let routing = ShardMap::new(ShardEpoch(1), vec![ShardId("required".into())]).unwrap();
    assert!(LocalExecutor::<FileShard>::new(routing, HashMap::new()).is_err());
}

#[test]
fn increments_are_atomic_persisted_and_errors_do_not_write() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("counter.yykv");
    let id = ShardId("counter".into());
    let routing = ShardMap::new(ShardEpoch(1), vec![id.clone()]).unwrap();
    let executor =
        std::sync::Arc::new(LocalExecutor::new(routing, HashMap::from([(id, FileShard::open(&path).unwrap())])).unwrap());
    let mut workers = Vec::new();
    for _ in 0..4 {
        let executor = std::sync::Arc::clone(&executor);
        workers.push(std::thread::spawn(move || {
            for _ in 0..10 {
                executor.execute(command(KeyValueAction::IncrementBy(1), b"counter")).unwrap();
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(executor.execute(command(KeyValueAction::Get, b"counter")).unwrap(), KeyValueResult::Get(Some(b"40".to_vec())));
    executor.execute(command(KeyValueAction::Put(i64::MAX.to_string().into_bytes()), b"max")).unwrap();
    assert!(executor.execute(command(KeyValueAction::IncrementBy(1), b"max")).is_err());
    for value in [b"01".as_slice(), b"+1", b"-0", b"abc", b""] {
        executor.execute(command(KeyValueAction::Put(value.to_vec()), b"invalid")).unwrap();
        assert!(executor.execute(command(KeyValueAction::IncrementBy(1), b"invalid")).is_err());
        assert_eq!(
            executor.execute(command(KeyValueAction::Get, b"invalid")).unwrap(),
            KeyValueResult::Get(Some(value.to_vec()))
        );
    }
    drop(executor);
    use yyds_kv::KvStore;
    let shard = FileShard::open(&path).unwrap();
    assert_eq!(
        shard.get(&yyds_kv::Key::new("redis:0", b"counter")).unwrap().unwrap().value,
        yyds_kv::StoredValue::Inline(yyds_kv::InlineValue(b"40".to_vec()))
    );
    assert_eq!(
        shard.get(&yyds_kv::Key::new("redis:0", b"max")).unwrap().unwrap().value,
        yyds_kv::StoredValue::Inline(yyds_kv::InlineValue(i64::MAX.to_string().into_bytes()))
    );
}

#[test]
fn put_if_absent_is_atomic_and_does_not_replace_existing_values() {
    let shard_id = ShardId("conditional".into());
    let executor = std::sync::Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).unwrap(),
            HashMap::from([(shard_id, yyds_kv::MemoryShard::new())]),
        )
        .unwrap(),
    );
    let mut workers = Vec::new();
    for index in 0..8 {
        let executor = std::sync::Arc::clone(&executor);
        workers.push(std::thread::spawn(move || {
            executor.execute(command(KeyValueAction::PutIfAbsent(vec![index]), b"winner")).unwrap()
        }));
    }
    let results = workers.into_iter().map(|worker| worker.join().unwrap()).collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| matches!(result, KeyValueResult::PutIfAbsent { revision: Some(_) })).count(), 1);
    assert_eq!(results.iter().filter(|result| matches!(result, KeyValueResult::PutIfAbsent { revision: None })).count(), 7);
    let original = executor.execute(command(KeyValueAction::Get, b"winner")).unwrap();
    assert!(matches!(original, KeyValueResult::Get(Some(ref value)) if value.len() == 1 && value[0] < 8));
    assert_eq!(
        executor.execute(command(KeyValueAction::PutIfAbsent(b"second".to_vec()), b"winner")).unwrap(),
        KeyValueResult::PutIfAbsent { revision: None },
    );
    assert_eq!(executor.execute(command(KeyValueAction::Get, b"winner")).unwrap(), original);
}
