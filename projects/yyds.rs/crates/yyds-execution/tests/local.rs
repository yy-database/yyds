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
        Vec::new(),
    )
    .unwrap();
    executor.execute(None, command(KeyValueAction::Put(b"two".to_vec()), b"second")).unwrap();
    let results = executor
        .execute_read_batch(
            None,
            &[
                command(KeyValueAction::Get, b"second"),
                command(KeyValueAction::Get, b"missing"),
                command(KeyValueAction::Get, b"second"),
            ],
        )
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
    let executor = LocalExecutor::new(routing, shards, Vec::new()).unwrap();
    let key = (0..100)
        .map(|index| format!("key-{index}").into_bytes())
        .find(|key| executor.routing().route("redis:0", key).shard == second_shard_id)
        .unwrap();

    assert_eq!(executor.execute(None, command(KeyValueAction::Get, &key)).unwrap(), KeyValueResult::Get(None));
    assert_eq!(executor.execute(None, command(KeyValueAction::Exists, &key)).unwrap(), KeyValueResult::Exists(false));
    assert_eq!(
        executor.execute(None, command(KeyValueAction::Put(vec![0, 0xff, 0x80]), &key)).unwrap(),
        KeyValueResult::Put { revision: 1 },
    );
    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, &key)).unwrap(),
        KeyValueResult::Get(Some(vec![0, 0xff, 0x80])),
    );
    assert_eq!(executor.execute(None, command(KeyValueAction::Exists, &key)).unwrap(), KeyValueResult::Exists(true));
    assert_eq!(
        executor.execute(None, command(KeyValueAction::Delete, &key)).unwrap(),
        KeyValueResult::Delete { removed: true }
    );
    assert_eq!(executor.execute(None, command(KeyValueAction::Exists, &key)).unwrap(), KeyValueResult::Exists(false));
    assert_eq!(
        executor.execute(None, command(KeyValueAction::Delete, &key)).unwrap(),
        KeyValueResult::Delete { removed: false }
    );
    assert_eq!(executor.routing().epoch(), ShardEpoch(3));

    drop(executor);
    let reopened = FileShard::open(directory.path().join("shard-a.yykv")).unwrap();
    let reopened_second = FileShard::open(directory.path().join("shard-b.yykv")).unwrap();
    let mut shards = HashMap::new();
    shards.insert(ShardId("shard-a".into()), reopened);
    shards.insert(second_shard_id.clone(), reopened_second);
    let executor = LocalExecutor::new(
        ShardMap::new(ShardEpoch(3), vec![ShardId("shard-a".into()), second_shard_id]).unwrap(),
        shards,
        Vec::new(),
    )
    .unwrap();
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, &key)).unwrap(), KeyValueResult::Get(None));
}

#[test]
fn executor_rejects_incomplete_shard_mounts() {
    let routing = ShardMap::new(ShardEpoch(1), vec![ShardId("required".into())]).unwrap();
    assert!(LocalExecutor::<FileShard>::new(routing, HashMap::new(), Vec::new()).is_err());
}

#[test]
fn increments_are_atomic_persisted_and_errors_do_not_write() {
    let directory = create_folder().unwrap();
    let path = directory.path().join("counter.yykv");
    let id = ShardId("counter".into());
    let routing = ShardMap::new(ShardEpoch(1), vec![id.clone()]).unwrap();
    let executor = std::sync::Arc::new(
        LocalExecutor::new(routing, HashMap::from([(id, FileShard::open(&path).unwrap())]), Vec::new()).unwrap(),
    );
    let mut workers = Vec::new();
    for _ in 0..4 {
        let executor = std::sync::Arc::clone(&executor);
        workers.push(std::thread::spawn(move || {
            for _ in 0..10 {
                executor.execute(None, command(KeyValueAction::IncrementBy(1), b"counter")).unwrap();
            }
        }));
    }
    for worker in workers {
        worker.join().unwrap();
    }
    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, b"counter")).unwrap(),
        KeyValueResult::Get(Some(b"40".to_vec()))
    );
    executor.execute(None, command(KeyValueAction::Put(i64::MAX.to_string().into_bytes()), b"max")).unwrap();
    assert!(executor.execute(None, command(KeyValueAction::IncrementBy(1), b"max")).is_err());
    for value in [b"01".as_slice(), b"+1", b"-0", b"abc", b""] {
        executor.execute(None, command(KeyValueAction::Put(value.to_vec()), b"invalid")).unwrap();
        assert!(executor.execute(None, command(KeyValueAction::IncrementBy(1), b"invalid")).is_err());
        assert_eq!(
            executor.execute(None, command(KeyValueAction::Get, b"invalid")).unwrap(),
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
            Vec::new(),
        )
        .unwrap(),
    );
    let mut workers = Vec::new();
    for index in 0..8 {
        let executor = std::sync::Arc::clone(&executor);
        workers.push(std::thread::spawn(move || {
            executor.execute(None, command(KeyValueAction::PutIfAbsent(vec![index]), b"winner")).unwrap()
        }));
    }
    let results = workers.into_iter().map(|worker| worker.join().unwrap()).collect::<Vec<_>>();
    assert_eq!(results.iter().filter(|result| matches!(result, KeyValueResult::PutIfAbsent { revision: Some(_) })).count(), 1);
    assert_eq!(results.iter().filter(|result| matches!(result, KeyValueResult::PutIfAbsent { revision: None })).count(), 7);
    let original = executor.execute(None, command(KeyValueAction::Get, b"winner")).unwrap();
    assert!(matches!(original, KeyValueResult::Get(Some(ref value)) if value.len() == 1 && value[0] < 8));
    assert_eq!(
        executor.execute(None, command(KeyValueAction::PutIfAbsent(b"second".to_vec()), b"winner")).unwrap(),
        KeyValueResult::PutIfAbsent { revision: None },
    );
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, b"winner")).unwrap(), original);
}

#[test]
fn get_delete_is_atomic_under_competing_readers() {
    let shard_id = ShardId("get-delete".into());
    let executor = std::sync::Arc::new(
        LocalExecutor::new(
            ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).unwrap(),
            HashMap::from([(shard_id, yyds_kv::MemoryShard::new())]),
            Vec::new(),
        )
        .unwrap(),
    );
    executor.execute(None, command(KeyValueAction::Put(b"payload".to_vec()), b"once")).unwrap();
    let mut workers = Vec::new();
    for _ in 0..8 {
        let executor = std::sync::Arc::clone(&executor);
        workers.push(std::thread::spawn(move || executor.execute(None, command(KeyValueAction::GetDelete, b"once")).unwrap()));
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
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, b"once")).unwrap(), KeyValueResult::Get(None));
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
    let executor = LocalExecutor::new(routing, shards, Vec::new()).unwrap();

    let results = executor.execute_scan(None, &Namespace("n".into()), b"", None, None, 0).unwrap();
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
    let executor = LocalExecutor::new(routing, shards, Vec::new()).unwrap();

    // prefix "a-" should merge a-10, a-11, a-20, a-30
    let r = executor.execute_scan(None, &Namespace("t".into()), b"a-", None, None, 0).unwrap();
    assert_eq!(r.len(), 4);
    let keys: Vec<&[u8]> = r.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"a-10", b"a-11", b"a-20", b"a-30"]);

    // after "a-11" exclusive: a-20, a-30 only
    let r = executor.execute_scan(None, &Namespace("t".into()), b"a-", Some(b"a-11"), None, 0).unwrap();
    let keys: Vec<&[u8]> = r.iter().map(|(k, _)| k.as_slice()).collect();
    assert_eq!(keys, vec![b"a-20", b"a-30"]);

    // limit 2 on full scan: first two globally
    let r = executor.execute_scan(None, &Namespace("t".into()), b"", None, None, 2).unwrap();
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
            Vec::new(),
        )
        .unwrap(),
    );

    // Pre-populate one key so we know the plain executor round-trips correctly.
    executor.execute(None, command(KeyValueAction::Put(b"initial".to_vec()), b"seed")).unwrap();
    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, b"seed")).unwrap(),
        KeyValueResult::Get(Some(b"initial".to_vec()))
    );

    // Grab a typed handle first to confirm it delegates through the trait method.
    let typed_handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    typed_handle.execute(command(KeyValueAction::Put(b"hello".to_vec()), b"val")).unwrap();
    typed_handle.commit().unwrap();

    let result = executor.execute(None, command(KeyValueAction::Get, b"val")).unwrap();
    assert_eq!(result, KeyValueResult::Get(Some(b"hello".to_vec())));

    // Second handle proves multiple transactions can be opened from the same executor.
    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::IncrementBy(41), b"counter")).unwrap();
    handle.commit().unwrap();
    let counter = executor.execute(None, command(KeyValueAction::IncrementBy(1), b"counter")).unwrap();
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
            Vec::new(),
        )
        .unwrap(),
    )
}

#[test]
fn rollback_discards_insert() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    executor.execute(None, command(KeyValueAction::Get, b"foo")).unwrap();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::Put(b"bar".to_vec()), b"foo")).unwrap();
    handle.rollback();

    assert_eq!(executor.execute(None, command(KeyValueAction::Get, b"foo")).unwrap(), KeyValueResult::Get(None));
}

#[test]
fn rollback_discards_update_keeping_original() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    executor.execute(None, command(KeyValueAction::Put(b"original".to_vec()), b"key")).unwrap();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::Put(b"new".to_vec()), b"key")).unwrap();
    handle.rollback();

    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, b"key")).unwrap(),
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

    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, b"foo")).unwrap(),
        KeyValueResult::Get(Some(b"hello".to_vec()))
    );
}

#[test]
fn commit_persists_update() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    executor.execute(None, command(KeyValueAction::Put(b"old".to_vec()), b"key")).unwrap();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::Put(b"new".to_vec()), b"key")).unwrap();
    handle.commit().unwrap();

    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, b"key")).unwrap(),
        KeyValueResult::Get(Some(b"new".to_vec()))
    );
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
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, b"temp")).unwrap(), KeyValueResult::Get(None));

    handle.rollback();
}

#[test]
fn transaction_read_batch_merges_snapshot_view() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    executor.execute(None, command(KeyValueAction::Put(b"original".to_vec()), b"k1")).unwrap();

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
    executor.execute(None, command(KeyValueAction::Put(b"alive".to_vec()), b"k")).unwrap();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    assert_eq!(handle.execute(command(KeyValueAction::Delete, b"k")).unwrap(), KeyValueResult::Delete { removed: true });

    // Inside the transaction the key is gone.
    assert_eq!(handle.execute(command(KeyValueAction::Exists, b"k")).unwrap(), KeyValueResult::Exists(false));

    handle.rollback();

    // After rollback the plain executor sees the original.
    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, b"k")).unwrap(),
        KeyValueResult::Get(Some(b"alive".to_vec()))
    );
}

#[test]
fn transaction_scan_merges_buffered_writes_over_base() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    executor.execute(None, command(KeyValueAction::Put(b"base".to_vec()), b"a")).unwrap();
    executor.execute(None, command(KeyValueAction::Put(b"base".to_vec()), b"c")).unwrap();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::Put(b"txn".to_vec()), b"b")).unwrap();
    handle.execute(command(KeyValueAction::Put(b"txn".to_vec()), b"a")).unwrap();
    handle.execute(command(KeyValueAction::Delete, b"c")).unwrap();

    let rows = handle.execute_scan(&Namespace("redis:0".into()), b"", None, None, 0).unwrap();
    assert_eq!(rows, vec![(b"a".to_vec(), b"txn".to_vec()), (b"b".to_vec(), b"txn".to_vec())]);

    // Base store is untouched until commit.
    let base_rows = executor.execute_scan(None, &Namespace("redis:0".into()), b"", None, None, 0).unwrap();
    assert_eq!(base_rows, vec![(b"a".to_vec(), b"base".to_vec()), (b"c".to_vec(), b"base".to_vec())]);

    handle.rollback();
}

#[test]
fn transaction_scan_applies_prefix_and_bounds_to_buffered_writes() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    for key in [b"p-1".as_slice(), b"p-2".as_slice(), b"q-1".as_slice()] {
        handle.execute(command(KeyValueAction::Put(b"v".to_vec()), key)).unwrap();
    }

    let namespace = Namespace("redis:0".into());
    assert_eq!(
        handle.execute_scan(&namespace, b"p-", None, None, 0).unwrap(),
        vec![(b"p-1".to_vec(), b"v".to_vec()), (b"p-2".to_vec(), b"v".to_vec()),]
    );
    assert_eq!(
        handle.execute_scan(&namespace, b"", Some(b"p-1"), Some(b"q-1"), 0).unwrap(),
        vec![(b"p-2".to_vec(), b"v".to_vec()),]
    );
    assert_eq!(handle.execute_scan(&namespace, b"", None, None, 1).unwrap(), vec![(b"p-1".to_vec(), b"v".to_vec())]);

    handle.rollback();
}

#[test]
fn write_if_unchanged_occ_rejects_stale_pre_image_at_commit() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};
    use yyds_types::Error;

    let executor = make_executor();
    executor.execute(None, command(KeyValueAction::Put(b"pre".to_vec()), b"k")).unwrap();

    // Both transactions read the same pre-image, then both try a read-modify-write Put.
    let t1: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    let t2: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    t1.write_if_unchanged(command(KeyValueAction::Put(b"t1".to_vec()), b"k"), Some(b"pre".to_vec())).unwrap();
    t2.write_if_unchanged(command(KeyValueAction::Put(b"t2".to_vec()), b"k"), Some(b"pre".to_vec())).unwrap();

    t1.commit().unwrap();
    let err = t2.commit().unwrap_err();
    assert!(matches!(err, Error::CasConflict { .. }), "expected CasConflict, got: {err:?}");

    assert_eq!(executor.execute(None, command(KeyValueAction::Get, b"k")).unwrap(), KeyValueResult::Get(Some(b"t1".to_vec())));
}

#[test]
fn write_if_unchanged_delete_rejects_recreated_key_at_commit() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};
    use yyds_types::Error;

    let executor = make_executor();
    executor.execute(None, command(KeyValueAction::Put(b"pre".to_vec()), b"k")).unwrap();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.write_if_unchanged(command(KeyValueAction::Delete, b"k"), Some(b"pre".to_vec())).unwrap();

    // Concurrent writer replaces the pre-image before the delete commits.
    executor.execute(None, command(KeyValueAction::Put(b"new".to_vec()), b"k")).unwrap();

    let err = handle.commit().unwrap_err();
    assert!(matches!(err, Error::CasConflict { .. }), "expected CasConflict, got: {err:?}");
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, b"k")).unwrap(), KeyValueResult::Get(Some(b"new".to_vec())));
}

#[test]
fn transaction_increment_rollback_leaves_base_untouched() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    executor.execute(None, command(KeyValueAction::Put(b"10".to_vec()), b"counter")).unwrap();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::IncrementBy(5), b"counter")).unwrap();
    handle.rollback();

    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, b"counter")).unwrap(),
        KeyValueResult::Get(Some(b"10".to_vec()))
    );
}

// ──────────────────────────────────────────────────────────────────────────────
// OCC validate-then-commit tests
// ──────────────────────────────────────────────────────────────────────────────

/// 并发 PutIfAbsent OCC：两个事务都从 base 看到 key 不存在，只有先 commit 的成功，
/// 后 commit 的被 OCC 拒绝（因为 re-check 时 key 已被前一个事务写入）。
#[test]
fn concurrent_put_if_absent_occ_rejects_late_commit() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};
    use yyds_types::Error;

    let executor = make_executor();

    // 两个事务同时开始。resolve 在事务执行期间都看到 base 里 key 不存在。
    let t1: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    let t2: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));

    t1.execute(command(KeyValueAction::PutIfAbsent(b"first".to_vec()), b"race-key")).unwrap();
    t2.execute(command(KeyValueAction::PutIfAbsent(b"second".to_vec()), b"race-key")).unwrap();

    // t1 先 commit → OCC check: base 里 key 不存在 ✓ → 成功
    t1.commit().unwrap();

    // t2 后 commit → OCC check: base 里 key 已存在 ✗ → CasConflict
    let err = t2.commit().unwrap_err();
    assert!(matches!(err, Error::CasConflict { .. }), "expected CasConflict, got: {err:?}");

    // base 里最终值只能是 t1 的写入（"first"），不能是 "second"
    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, b"race-key")).unwrap(),
        KeyValueResult::Get(Some(b"first".to_vec()))
    );
}

/// PutIfAbsent 事务与外部无条件 Put 并发提交：外部 Put 先持久化，
/// PutIfAbsent 事务 commit 时 OCC re-check 发现 key 已存在 → 被拒。
#[test]
fn put_if_absent_vs_external_put_occ() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};
    use yyds_types::Error;

    let executor = make_executor();

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    handle.execute(command(KeyValueAction::PutIfAbsent(b"txn-val".to_vec()), b"k")).unwrap();

    // 外部在事务 commit 之前先无条件 Put 同一个 key
    executor.execute(None, command(KeyValueAction::Put(b"external".to_vec()), b"k")).unwrap();

    // 事务 commit → OCC check base → key 已存在 → CasConflict
    let err = handle.commit().unwrap_err();
    assert!(matches!(err, Error::CasConflict { .. }));

    // 外部写入不受影响
    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, b"k")).unwrap(),
        KeyValueResult::Get(Some(b"external".to_vec()))
    );
}

/// OCC re-check 使用纯 base store，不会被自己 transaction buffer 里的写入误导。
/// 构造一个事务：先 PutIfAbsent(k)（事务内看到 k 不存在 → 通过），
/// 然后再无条件 Put(k) → buffer 最终状态是 Value(?, None)，不带条件，
/// commit 直接 batch apply（不走 OCC 路径）。
/// 但如果另一个线程在两次写入之间 commit 了，我们的第二次无条件 Put 的 buffer
/// 会覆盖第一次的条件，所以最终条件是 None，OCC 不 re-check，直接 Put。
/// 这是当前设计的预期行为：无条件 Put 覆盖条件。
#[test]
fn unconditional_put_overrides_prior_conditional_in_buffer() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor = make_executor();
    // base: k 不存在

    let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    // PutIfAbsent → 事务内看到 k 不存在 → 成功进 buffer，带 PutIfAbsent 条件
    handle.execute(command(KeyValueAction::PutIfAbsent(b"ifabsent".to_vec()), b"k")).unwrap();
    // 然后无条件 Put 覆盖同一个 key → buffer 条件被清掉
    handle.execute(command(KeyValueAction::Put(b"unconditional".to_vec()), b"k")).unwrap();
    handle.commit().unwrap();

    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, b"k")).unwrap(),
        KeyValueResult::Get(Some(b"unconditional".to_vec()))
    );
}

/// 多 key 事务中一个 PutIfAbsent 失败，整个 commit 被回滚（其他无条件写也不持久化）。
/// 这通过 CasConflict 返回 Err 且没有部分写入来验证。
#[test]
fn single_occ_failure_aborts_entire_batch() {
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};
    use yyds_types::Error;

    let executor = make_executor();

    // base: "a" 不存在, "b" 不存在
    // T1: Put("a"), PutIfAbsent("b")
    // 外部先 Put("b")
    // T1 commit: OCC re-check base "b" → 存在 → CasConflict
    //           → "a" 和 "b" 都不应被写入
    let txn: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor.clone()));
    txn.execute(command(KeyValueAction::Put(b"t1-a".to_vec()), b"a")).unwrap();
    txn.execute(command(KeyValueAction::PutIfAbsent(b"t1-b".to_vec()), b"b")).unwrap();

    // 外部先写 "b"
    executor.execute(None, command(KeyValueAction::Put(b"ext-b".to_vec()), b"b")).unwrap();

    let err = txn.commit().unwrap_err();
    assert!(matches!(err, Error::CasConflict { .. }), "expected CasConflict, got {err:?}");

    // "a" 也不应该被写入（整个 batch 被 abort）
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, b"a")).unwrap(), KeyValueResult::Get(None));
    // "b" 保持外部写入
    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, b"b")).unwrap(),
        KeyValueResult::Get(Some(b"ext-b".to_vec()))
    );
}

/// 全局锁保证并发 PutIfAbsent 同一 key 只有一个事务能成功 commit。
/// 用 Barrier 让所有线程先完成 resolve（都看到 base key 不存在），然后同时 commit。
/// commit 被全局锁串行化，只有第一个能通过 OCC re-check，其他被 CasConflict 拒绝。
#[test]
fn global_lock_serialises_concurrent_commits() {
    use std::sync::{Arc, Barrier};
    use yyds_execution::{LocalTransactionHandle, TransactionHandle};

    let executor: Arc<dyn KeyValueExecutor> = make_executor();
    const N: usize = 4;
    let barrier = Arc::new(Barrier::new(N));

    let mut workers = Vec::new();
    for _ in 0..N {
        let executor = Arc::clone(&executor);
        let barrier = Arc::clone(&barrier);
        workers.push(std::thread::spawn(move || {
            let handle: Box<dyn TransactionHandle> = Box::new(LocalTransactionHandle::new(executor));
            // resolve: base 里 shared 不存在 → 进 buffer，带 PutIfAbsent 条件
            handle.execute(command(KeyValueAction::PutIfAbsent(b"winner".to_vec()), b"shared")).unwrap();
            // 所有线程都 resolve 完再一起开始 commit
            barrier.wait();
            handle.commit().is_ok()
        }));
    }
    let mut success_count = 0u32;
    for w in workers {
        if w.join().unwrap() {
            success_count += 1;
        }
    }
    assert_eq!(success_count, 1, "only one PutIfAbsent on a shared key should succeed");

    // 验证 base 里确实有值
    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, b"shared")).unwrap(),
        KeyValueResult::Get(Some(b"winner".to_vec()))
    );
}

// ────────────────────────────────────────────────────────────────
// Fencing 测试组
// ────────────────────────────────────────────────────────────────

use yyds_types::{FenceToken, LeaderTerm, OwnerEntry, ReplicaNodeId};

fn make_fence_test_executor(owners: Vec<OwnerEntry>) -> LocalExecutor<MemoryShard> {
    let shard_id = ShardId("s1".into());
    let routing = ShardMap::new(ShardEpoch(1), vec![shard_id.clone()]).unwrap();
    let shards = HashMap::from([(shard_id, MemoryShard::new())]);
    LocalExecutor::new(routing, shards, owners).unwrap()
}

#[test]
fn fencing_pass_when_token_matches_catalog_owner() {
    let shard_id = ShardId("s1".into());
    let node_a = ReplicaNodeId("node-a".to_owned());
    let term = LeaderTerm(3);

    let owners = vec![OwnerEntry { shard_id: shard_id.clone(), node_id: node_a.clone(), leader_term: term }];
    let executor = make_fence_test_executor(owners);

    let fence = Some(FenceToken { epoch: ShardEpoch(1), term, leader: node_a });
    let result = executor.execute(fence.as_ref(), command(KeyValueAction::Put(b"hello".to_vec()), b"k")).unwrap();
    assert!(matches!(result, KeyValueResult::Put { .. }));

    // 读也应该通过
    let read = executor.execute(fence.as_ref(), command(KeyValueAction::Get, b"k")).unwrap();
    assert_eq!(read, KeyValueResult::Get(Some(b"hello".to_vec())));
}

#[test]
fn fencing_rejects_when_node_id_has_switched() {
    let shard_id = ShardId("s1".into());
    let node_a = ReplicaNodeId("node-a".to_owned());
    let node_b = ReplicaNodeId("node-b".to_owned());
    let term_b = LeaderTerm(5);

    // catalog 已发布 owner 切换到 node-b / term=5
    let owners = vec![OwnerEntry { shard_id: shard_id.clone(), node_id: node_b.clone(), leader_term: term_b }];
    let executor = make_fence_test_executor(owners);

    // 旧 leader（node-a / term=3）携带过期 fence 发起请求
    let stale_fence = Some(FenceToken { epoch: ShardEpoch(1), term: LeaderTerm(3), leader: node_a });
    let err = executor.execute(stale_fence.as_ref(), command(KeyValueAction::Put(b"stale".to_vec()), b"k")).unwrap_err();

    match err {
        yyds_types::Error::FencingRejected { fence, current } => {
            assert_eq!(fence.leader, ReplicaNodeId("node-a".to_owned()));
            assert_eq!(fence.term, LeaderTerm(3));
            assert_eq!(current.node_id, ReplicaNodeId("node-b".to_owned()));
            assert_eq!(current.leader_term, LeaderTerm(5));
        }
        other => panic!("expected FencingRejected, got {other:?}"),
    }
}

#[test]
fn fencing_rejects_when_term_only_mismatches() {
    let shard_id = ShardId("s1".into());
    let node_a = ReplicaNodeId("node-a".to_owned());
    let owners = vec![OwnerEntry { shard_id, node_id: node_a.clone(), leader_term: LeaderTerm(7) }];
    let executor = make_fence_test_executor(owners);

    // 同一个 node，但是 term 过期了
    let stale_fence = Some(FenceToken { epoch: ShardEpoch(1), term: LeaderTerm(5), leader: node_a });
    let err = executor.execute(stale_fence.as_ref(), command(KeyValueAction::Put(b"x".to_vec()), b"k")).unwrap_err();

    assert!(matches!(err, yyds_types::Error::FencingRejected { .. }));
}

#[test]
fn fencing_rejects_when_shard_has_no_published_owner() {
    // 空 owners 快照 — coordinator 过早调用 fence
    let executor = make_fence_test_executor(Vec::new());

    let fence = Some(FenceToken { epoch: ShardEpoch(1), term: LeaderTerm(1), leader: ReplicaNodeId("node-a".to_owned()) });
    let err = executor.execute(fence.as_ref(), command(KeyValueAction::Put(b"x".to_vec()), b"k")).unwrap_err();
    assert!(matches!(err, yyds_types::Error::FencingRejected { .. }));
}

#[test]
fn fencing_is_skipped_when_none() {
    // 即使 owners 里有不同的 owner, fence=None 也不做任何校验
    let shard_id = ShardId("s1".into());
    let node_a = ReplicaNodeId("node-a".to_owned());
    let owners = vec![OwnerEntry { shard_id, node_id: node_a.clone(), leader_term: LeaderTerm(1) }];
    let executor = make_fence_test_executor(owners);

    // None fence 应该无条件通过 — 这是单节点 gateway 的正常路径
    let result = executor.execute(None, command(KeyValueAction::Put(b"gateway".to_vec()), b"k")).unwrap();
    assert!(matches!(result, KeyValueResult::Put { .. }));
    let read = executor.execute(None, command(KeyValueAction::Get, b"k")).unwrap();
    assert_eq!(read, KeyValueResult::Get(Some(b"gateway".to_vec())));
}

#[test]
fn fencing_guards_execute_read_batch() {
    let shard_id = ShardId("s1".into());
    let node_b = ReplicaNodeId("node-b".to_owned());
    let owners = vec![OwnerEntry { shard_id, node_id: node_b.clone(), leader_term: LeaderTerm(5) }];
    let executor = make_fence_test_executor(owners);

    // 先通过 fence=None 写入一些数据
    executor.execute(None, command(KeyValueAction::Put(b"v1".to_vec()), b"k")).unwrap();

    let stale_fence =
        Some(FenceToken { epoch: ShardEpoch(1), term: LeaderTerm(3), leader: ReplicaNodeId("node-a".to_owned()) });
    let batch = vec![command(KeyValueAction::Get, b"k"), command(KeyValueAction::Get, b"k")];
    let err = executor.execute_read_batch(stale_fence.as_ref(), &batch).unwrap_err();
    assert!(matches!(err, yyds_types::Error::FencingRejected { .. }));

    // None fence 应该正常
    let results = executor.execute_read_batch(None, &batch).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn fencing_guards_execute_batch() {
    let shard_id = ShardId("s1".into());
    let node_b = ReplicaNodeId("node-b".to_owned());
    let owners = vec![OwnerEntry { shard_id, node_id: node_b.clone(), leader_term: LeaderTerm(5) }];
    let executor = make_fence_test_executor(owners);

    let stale_fence =
        Some(FenceToken { epoch: ShardEpoch(1), term: LeaderTerm(3), leader: ReplicaNodeId("node-a".to_owned()) });
    let batch = vec![command(KeyValueAction::Put(b"x1".to_vec()), b"k1"), command(KeyValueAction::Put(b"x2".to_vec()), b"k2")];
    let err = executor.execute_batch(stale_fence.as_ref(), batch).unwrap_err();
    assert!(matches!(err, yyds_types::Error::FencingRejected { .. }));

    // 确保没有东西被写入（fence 在 batch 执行前被拒绝）
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, b"k1")).unwrap(), KeyValueResult::Get(None));
}

#[test]
fn fencing_guards_execute_scan() {
    let node_b = ReplicaNodeId("node-b".to_owned());
    let owners = vec![OwnerEntry { shard_id: ShardId("s1".into()), node_id: node_b.clone(), leader_term: LeaderTerm(5) }];
    let executor = make_fence_test_executor(owners);

    // 先通过 fence=None 写入数据（单节点本地路径）
    executor.execute(None, command(KeyValueAction::Put(b"v1".to_vec()), b"k")).unwrap();

    // 过期 fence 必须拒绝 scan 读路径
    let stale_fence =
        Some(FenceToken { epoch: ShardEpoch(1), term: LeaderTerm(3), leader: ReplicaNodeId("node-a".to_owned()) });
    let err = executor.execute_scan(stale_fence.as_ref(), &Namespace("redis:0".into()), b"", None, None, 0).unwrap_err();
    assert!(matches!(err, yyds_types::Error::FencingRejected { .. }));

    // None fence 放行（单节点 gateway 路径），匹配的 fence 也放行
    let results = executor.execute_scan(None, &Namespace("redis:0".into()), b"", None, None, 0).unwrap();
    assert_eq!(results, vec![(b"k".to_vec(), b"v1".to_vec())]);
    let fresh_fence = Some(FenceToken { epoch: ShardEpoch(1), term: LeaderTerm(5), leader: node_b });
    let results = executor.execute_scan(fresh_fence.as_ref(), &Namespace("redis:0".into()), b"", None, None, 0).unwrap();
    assert_eq!(results, vec![(b"k".to_vec(), b"v1".to_vec())]);
}

#[test]
fn commit_batch_rejects_stale_fence() {
    let node_b = ReplicaNodeId("node-b".to_owned());
    let owners = vec![OwnerEntry { shard_id: ShardId("s1".into()), node_id: node_b.clone(), leader_term: LeaderTerm(5) }];
    let executor = make_fence_test_executor(owners);

    let batch = vec![
        yyds_execution::BufferedCommit { command: command(KeyValueAction::Put(b"x1".to_vec()), b"k1"), condition: None },
        yyds_execution::BufferedCommit { command: command(KeyValueAction::Put(b"x2".to_vec()), b"k2"), condition: None },
    ];
    let stale_fence =
        Some(FenceToken { epoch: ShardEpoch(1), term: LeaderTerm(3), leader: ReplicaNodeId("node-a".to_owned()) });
    let err = executor.commit_batch(stale_fence.as_ref(), batch).unwrap_err();
    assert!(matches!(err, yyds_types::Error::FencingRejected { .. }));

    // 整批都不允许落盘
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, b"k1")).unwrap(), KeyValueResult::Get(None));
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, b"k2")).unwrap(), KeyValueResult::Get(None));

    // 匹配的 fence 提交成功
    let fresh_fence = Some(FenceToken { epoch: ShardEpoch(1), term: LeaderTerm(5), leader: node_b });
    let batch =
        vec![yyds_execution::BufferedCommit { command: command(KeyValueAction::Put(b"ok".to_vec()), b"k1"), condition: None }];
    executor.commit_batch(fresh_fence.as_ref(), batch).unwrap();
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, b"k1")).unwrap(), KeyValueResult::Get(Some(b"ok".to_vec())));
}

#[test]
fn commit_batch_occ_conflict_leaves_no_partial_apply() {
    let executor = make_fence_test_executor(Vec::new());

    // 第二个条目的 PutIfAbsent 条件在 base 已存在时会冲突；第一个条目是无条件 Put。
    // OCC 预检必须在应用任何写入之前扫描完整批次，因此冲突时整批不应用（无半提交）。
    executor.execute(None, command(KeyValueAction::Put(b"existing".to_vec()), b"conflict-key")).unwrap();
    let batch = vec![
        yyds_execution::BufferedCommit {
            command: command(KeyValueAction::Put(b"first".to_vec()), b"plain-key"),
            condition: None,
        },
        yyds_execution::BufferedCommit {
            command: command(KeyValueAction::Put(b"second".to_vec()), b"conflict-key"),
            condition: Some(yyds_execution::CommitCondition::PutIfAbsent),
        },
    ];
    let err = executor.commit_batch(None, batch).unwrap_err();
    assert!(matches!(err, yyds_types::Error::CasConflict { .. }), "expected CasConflict, got {err:?}");

    // 无条件写入的 plain-key 绝不能被半提交
    assert_eq!(executor.execute(None, command(KeyValueAction::Get, b"plain-key")).unwrap(), KeyValueResult::Get(None));
    // conflict-key 保持原值
    assert_eq!(
        executor.execute(None, command(KeyValueAction::Get, b"conflict-key")).unwrap(),
        KeyValueResult::Get(Some(b"existing".to_vec()))
    );
}

// ────────────────────────────────────────────────────────────────
// 默认接口合同测试：无原子批写/OCC 提交能力的 executor 必须显式拒绝
// ────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct MinimalExecutor;

impl KeyValueExecutor for MinimalExecutor {
    fn execute(&self, _fence: Option<&FenceToken>, _command: KeyValueCommand) -> yyds_types::Result<KeyValueResult> {
        Ok(KeyValueResult::Get(None))
    }
}

#[test]
fn default_execute_batch_contract_returns_unsupported() {
    let executor = MinimalExecutor;
    let err = executor.execute_batch(None, vec![command(KeyValueAction::Put(b"v".to_vec()), b"k")]).unwrap_err();
    match err {
        yyds_types::Error::Unsupported(feature) => assert!(feature.contains("atomic batch writes"), "got {feature}"),
        other => panic!("expected Unsupported, got {other:?}"),
    }
}

#[test]
fn default_commit_batch_contract_returns_unsupported() {
    let executor = MinimalExecutor;
    let batch = vec![yyds_execution::BufferedCommit {
        command: command(KeyValueAction::Put(b"v".to_vec()), b"k"),
        condition: Some(yyds_execution::CommitCondition::PutIfAbsent),
    }];
    let err = executor.commit_batch(None, batch).unwrap_err();
    match err {
        yyds_types::Error::Unsupported(feature) => {
            assert!(feature.contains("atomic OCC-validated batch commits"), "got {feature}");
        }
        other => panic!("expected Unsupported, got {other:?}"),
    }
}
