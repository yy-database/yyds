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
    assert_eq!(
        executor.execute(command(KeyValueAction::Put(vec![0, 0xff, 0x80]), &key)).unwrap(),
        KeyValueResult::Put { revision: 1 },
    );
    assert_eq!(executor.execute(command(KeyValueAction::Get, &key)).unwrap(), KeyValueResult::Get(Some(vec![0, 0xff, 0x80])),);
    assert_eq!(executor.execute(command(KeyValueAction::Delete, &key)).unwrap(), KeyValueResult::Delete { removed: true });
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
