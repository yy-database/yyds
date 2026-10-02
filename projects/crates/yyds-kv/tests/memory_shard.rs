use yyds_kv::{compare_and_put, InlineValue, Key, KvStore, MemoryShard, StoredValue};

#[test]
fn put_get_round_trip() {
    let mut shard = MemoryShard::new();
    let key = Key::new("users", b"alice");
    let revision = shard
        .put(key.clone(), StoredValue::Inline(InlineValue(b"yyds".to_vec())))
        .expect("put");

    let record = shard.get(&key).expect("get").expect("record");
    assert_eq!(revision, record.revision);
    assert_eq!(record.value, StoredValue::Inline(InlineValue(b"yyds".to_vec())));
}

#[test]
fn compare_and_put_rejects_stale_revision() {
    let mut shard = MemoryShard::new();
    let key = Key::new("users", b"bob");
    let first = shard
        .put(key.clone(), StoredValue::Inline(InlineValue(vec![1])))
        .expect("put");

    let err = compare_and_put(
        &mut shard,
        key.clone(),
        Some(first - 1),
        StoredValue::Inline(InlineValue(vec![2])),
    )
    .expect_err("cas conflict");

    assert!(err.to_string().contains("cas conflict"));
}
