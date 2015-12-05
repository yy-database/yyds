use std::collections::HashMap;

use boost_fs::temp::create_folder;
use yyds_execution::{KeyValueExecutor, LocalExecutor};
use yyds_kv::{FileShard, InlineValue, Key, KvStore, MemoryShard, StoredValue};
use yyds_types::{KeyValueAction, KeyValueCommand, KeyValueResult, Namespace, ShardEpoch, ShardId, ShardMap};

fn command(action: KeyValueAction, key: &[u8]) -> KeyValueCommand {
    KeyValueCommand { namespace: Namespace("redis:0".into()), key: key.to_vec(), action }
}

#[test]
fn batch_execution_preserves_order_and_duplicates() {
    let directory = create_folder().unwrap();
    let shard_id = ShardId("shard-a".into());
    let executor = LocalExecutor::new(
        ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).unwrap(),
        HashMap::from([(shard_id, FileShard::open(directory.path().join("shard-a.yykv")).unwrap())]),
    )
    .unwrap();
    executor.execute(command(KeyValueAction::Put(b"two".to_vec()), b"second")).unwrap();
    let results = executor
        .execute_read_batch(&[
            command(KeyValueAction::Get, b"second"),
            command(KeyValueAction::Get, b"missing"),
            command(KeyValueAction::Get, b"second"),
        ])
        .unwrap();
    assert_eq!(
        results,
        vec![KeyValueResult::Get(Some(b"two".to_vec())), KeyValueResult::Get(None), KeyValueResult::Get(Some(b"two".to_vec())),]
    );
}

#[test]
fn local_executor_routes_and_persists_binary_commands() {
    let directory = create_folder().unwrap();
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
    let directory = create_folder().unwrap();
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

#[test]
fn get_delete_is_atomic_under_competing_readers() {
    let shard_id = ShardId("get-delete".into());
    let executor = std::sync::Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).unwrap(),
            HashMap::from([(shard_id, yyds_kv::MemoryShard::new())]),
        )
        .unwrap(),
    );
    executor.execute(command(KeyValueAction::Put(b"payload".to_vec()), b"once")).unwrap();
    let mut workers = Vec::new();
    for _ in 0..8 {
        let executor = std::sync::Arc::clone(&executor);
        workers.push(std::thread::spawn(move || executor.execute(command(KeyValueAction::GetDelete, b"once")).unwrap()));
    }
    let results = workers.into_iter().map(|worker| worker.join().unwrap()).collect::<Vec<_>>();
    assert_eq!(
        results
            .iter()
            .filter(|result| matches!(result, KeyValueResult::GetDelete(Some(value)) if value == b"payload" ))
            .count(),
        1
    );
    assert_eq!(results.iter().filter(|result| matches!(result, KeyValueResult::GetDelete(None))).count(), 7);
    assert_eq!(executor.execute(command(KeyValueAction::Get, b"once")).unwrap(), KeyValueResult::Get(None));
}

#[test]
fn scan_merges_multiple_shards_into_globally_sorted_order() {
    // Construct two MemoryShards with deliberately interleaved keys to prove K-way merge.
    let shard_a_id = ShardId("shard-a".into());
    let shard_b_id = ShardId("shard-b".into());
    let routing = ShardMap::new(ShardEpoch(1), vec![shard_a_id.clone(), shard_b_id.clone()]).unwrap();

    // shard_a holds "z" and "1" (two extremes)
    let mut shard_a = MemoryShard::new();
    shard_a.put(Key::new("n", b"z"), StoredValue::Inline(InlineValue(b"Z".to_vec()))).unwrap();
    shard_a.put(Key::new("n", b"1"), StoredValue::Inline(InlineValue(b"ONE".to_vec()))).unwrap();
    // shard_b holds "m" and "3"
    let mut shard_b = MemoryShard::new();
    shard_b.put(Key::new("n", b"m"), StoredValue::Inline(InlineValue(b"M".to_vec()))).unwrap();
    shard_b.put(Key::new("n", b"3"), StoredValue::Inline(InlineValue(b"THREE".to_vec()))).unwrap();

    let shards = HashMap::from([(shard_a_id, shard_a), (shard_b_id, shard_b)]);
    let executor = LocalExecutor::new(routing, shards).unwrap();

    let results = executor.execute_scan(&Namespace("n".into()), b"", None, None, 0).unwrap();
    let keys: Vec<&[u8]> = results.iter().map(|(k, _)| k.as_slice()).collect();
    // K-way merge should produce globally sorted order regardless of which shard holds which key
    assert_eq!(keys, vec![b"1", b"3", b"m", b"z"]);
}

#[test]
fn scan_applies_prefix_after_and_limit_globally() {
    let shard_a_id = ShardId("shard-a".into());
    let shard_b_id = ShardId("shard-b".into());
    let routing = ShardMap::new(ShardEpoch(1), vec![shard_a_id.clone(), shard_b_id.clone()]).unwrap();

    let mut shard_a = MemoryShard::new();
    for &k in &[b"a-10" as &[u8], b"a-20", b"b-30"] {
        shard_a.put(Key::new("t", k), StoredValue::Inline(InlineValue(k.to_vec()))).unwrap();
    }
    let mut shard_b = MemoryShard::new();
    for &k in &[b"a-11" as &[u8], b"a-30", b"b-40"] {
        shard_b.put(Key::new("t", k), StoredValue::Inline(InlineValue(k.to_vec()))).unwrap();
    }

    let shards = HashMap::from([(shard_a_id, shard_a), (shard_b_id, shard_b)]);
    let executor = LocalExecutor::new(routing, shards).unwrap();

    // prefix "a-" should merge a-10, a-11, a-20, a-30
    let r = executor.execute_scan(&Namespace("t".into()), b"a-", None, None, 0).unwrap();
    assert_eq!(r.len(), 4);
    let keys: Vec<&[u8]> = r.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"a-10", b"a-11", b"a-20", b"a-30"]);

    // after "a-11" exclusive: a-20, a-30 only
    let r = executor.execute_scan(&Namespace("t".into()), b"a-", Some(b"a-11"), None, 0).unwrap();
    let keys: Vec<&[u8]> = r.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"a-20", b"a-30"]);

    // limit 2 on full scan: first two globally
    let r = executor.execute_scan(&Namespace("t".into()), b"", None, None, 2).unwrap();
    let keys: Vec<&[u8]> = r.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"a-10", b"a-11"]);
}

#[test]
fn transaction_handle_exposes_begin_commit_and_rollback_contract() {
    use std::sync::Arc;
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let directory = create_folder().unwrap();
    let shard_id = ShardId("txn-a".into());
    let executor = Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(7), vec![shard_id.clone()]).unwrap(),
            HashMap::from([(shard_id, FileShard::open(directory.path().join("txn-a.yykv")).unwrap())]),
        )
        .unwrap(),
    );

    // Pre-populate one key so we know the plain executor round-trips correctly.
    executor.execute(command(KeyValueAction::Put(b"initial".to_vec()), b"seed")).unwrap();
    assert_eq!(
        executor.execute(command(KeyValueAction::Get, b"seed")).unwrap(),
        KeyValueResult::Get(Some(b"initial".to_vec()))
    );

    // Grab a typed handle first to confirm it delegates through the trait method.
    let typed_handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    typed_handle.execute(command(KeyValueAction::Put(b"hello".to_vec()), b"val")).unwrap();
    typed_handle.commit().unwrap();

    let result = executor.execute(command(KeyValueAction::Get, b"val")).unwrap();
    assert_eq!(result, KeyValueResult::Get(Some(b"hello".to_vec())));

    // Second handle proves multiple transactions can be opened from the same executor.
    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::IncrementBy(41), b"counter")).unwrap();
    handle.commit().unwrap();
    let counter = executor.execute(command(KeyValueAction::IncrementBy(1), b"counter")).unwrap();
    assert_eq!(counter, KeyValueResult::Increment { value: 42 });

    // Rollback is a valid terminal action.
    let rollback_handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    rollback_handle.execute(command(KeyValueAction::Put(b"pending".to_vec()), b"temp")).unwrap();
    rollback_handle.rollback();
}

fn make_executor() -> std::sync::Arc<LocalExecutor<MemoryShard>> {
    let shard_id = ShardId("txn-mem".into());
    std::sync::Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).unwrap(),
            HashMap::from([(shard_id, MemoryShard::new())]),
        )
        .unwrap(),
    )
}

#[test]
fn rollback_discards_insert() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    executor.execute(command(KeyValueAction::Get, b"foo")).unwrap();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::Put(b"bar".to_vec()), b"foo")).unwrap();
    handle.rollback();

    assert_eq!(executor.execute(command(KeyValueAction::Get, b"foo")).unwrap(), KeyValueResult::Get(None));
}

#[test]
fn rollback_discards_update_keeping_original() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    executor.execute(command(KeyValueAction::Put(b"original".to_vec()), b"key")).unwrap();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::Put(b"new".to_vec()), b"key")).unwrap();
    handle.rollback();

    assert_eq!(
        executor.execute(command(KeyValueAction::Get, b"key")).unwrap(),
        KeyValueResult::Get(Some(b"original".to_vec()))
    );
}

#[test]
fn commit_persists_insert() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::Put(b"hello".to_vec()), b"foo")).unwrap();
    handle.commit().unwrap();

    assert_eq!(executor.execute(command(KeyValueAction::Get, b"foo")).unwrap(), KeyValueResult::Get(Some(b"hello".to_vec())));
}

#[test]
fn commit_persists_update() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    executor.execute(command(KeyValueAction::Put(b"old".to_vec()), b"key")).unwrap();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::Put(b"new".to_vec()), b"key")).unwrap();
    handle.commit().unwrap();

    assert_eq!(executor.execute(command(KeyValueAction::Get, b"key")).unwrap(), KeyValueResult::Get(Some(b"new".to_vec())));
}

#[test]
fn transaction_sees_own_writes_before_commit() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::Put(b"pending".to_vec()), b"temp")).unwrap();

    // Inside the transaction we read our own write.
    assert_eq!(handle.execute(command(KeyValueAction::Get, b"temp")).unwrap(), KeyValueResult::Get(Some(b"pending".to_vec())));

    // Outside, the plain executor still sees nothing.
    assert_eq!(executor.execute(command(KeyValueAction::Get, b"temp")).unwrap(), KeyValueResult::Get(None));

    handle.rollback();
}

#[test]
fn transaction_read_batch_merges_snapshot_view() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    executor.execute(command(KeyValueAction::Put(b"original".to_vec()), b"k1")).unwrap();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::Put(b"txn-v".to_vec()), b"k1")).unwrap();
    handle.execute(command(KeyValueAction::Put(b"new".to_vec()), b"k2")).unwrap();

    let results =
        handle.execute_read_batch(&[command(KeyValueAction::Get, b"k1"), command(KeyValueAction::Get, b"k2")]).unwrap();
    assert_eq!(results, vec![KeyValueResult::Get(Some(b"txn-v".to_vec())), KeyValueResult::Get(Some(b"new".to_vec())),]);

    handle.rollback();
}

#[test]
fn transaction_delete_is_undone_by_rollback() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    executor.execute(command(KeyValueAction::Put(b"alive".to_vec()), b"k")).unwrap();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    assert_eq!(handle.execute(command(KeyValueAction::Delete, b"k")).unwrap(), KeyValueResult::Delete { removed: true });

    // Inside the transaction the key is gone.
    assert_eq!(handle.execute(command(KeyValueAction::Exists, b"k")).unwrap(), KeyValueResult::Exists(false));

    handle.rollback();

    // After rollback the plain executor sees the original.
    assert_eq!(executor.execute(command(KeyValueAction::Get, b"k")).unwrap(), KeyValueResult::Get(Some(b"alive".to_vec())));
}

#[test]
fn transaction_increment_rollback_leaves_base_untouched() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    executor.execute(command(KeyValueAction::Put(b"10".to_vec()), b"counter")).unwrap();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::IncrementBy(5), b"counter")).unwrap();
    handle.rollback();

    assert_eq!(executor.execute(command(KeyValueAction::Get, b"counter")).unwrap(), KeyValueResult::Get(Some(b"10".to_vec())));
}
