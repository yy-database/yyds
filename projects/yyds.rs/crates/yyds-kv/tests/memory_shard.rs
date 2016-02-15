use yyds_kv::{compare_and_put, InlineValue, Key, KvStore, MemoryShard, StoredValue};
use yyds_types::Namespace;

#[test]
fn put_get_round_trip() {
    let mut shard = MemoryShard::new();
    let key = Key::new("users", b"alice");
    let revision = shard.put(key.clone(), StoredValue::Inline(InlineValue(b"yyds".to_vec()))).expect("put");

    let record = shard.get(&key).expect("get").expect("record");
    assert_eq!(revision, record.revision);
    assert_eq!(record.value, StoredValue::Inline(InlineValue(b"yyds".to_vec())));
}

#[test]
fn compare_and_put_rejects_stale_revision() {
    let mut shard = MemoryShard::new();
    let key = Key::new("users", b"bob");
    let first = shard.put(key.clone(), StoredValue::Inline(InlineValue(vec![1]))).expect("put");

    let err = compare_and_put(&mut shard, key.clone(), Some(first - 1), StoredValue::Inline(InlineValue(vec![2])))
        .expect_err("cas conflict");

    assert!(err.to_string().contains("cas conflict"));
}

#[test]
fn scan_returns_all_records_in_namespace_ordered_by_key() {
    let mut shard = MemoryShard::new();
    let ns = Namespace("users".into());
    for (k, v) in &[("carol", b"c".to_vec()), ("alice", b"a".to_vec()), ("bob", b"b".to_vec())] {
        shard.put(Key::new("users", k.as_bytes()), StoredValue::Inline(InlineValue(v.clone()))).unwrap();
    }

    let results = shard.scan(&ns, b"", None, None, 0).unwrap();
    let keys: Vec<&[u8]> = results.iter().map(|r| r.key.bytes.as_slice()).collect();
    assert_eq!(keys, vec![b"alice".as_slice(), b"bob", b"carol"]);
    assert_eq!(results.len(), 3);
}

#[test]
fn scan_prefix_filters_to_matching_keys() {
    let mut shard = MemoryShard::new();
    let ns = Namespace("table:7".into());
    for &k in &[b"10" as &[u8], b"11", b"20", b"21"] {
        shard.put(Key::new("table:7", k), StoredValue::Inline(InlineValue(k.to_vec()))).unwrap();
    }

    let results = shard.scan(&ns, b"1", None, None, 0).unwrap();
    let keys: Vec<&[u8]> = results.iter().map(|r| r.key.bytes.as_slice()).collect();
    assert_eq!(keys, vec![b"10", b"11"]);
}

#[test]
fn scan_after_is_exclusive_cursor() {
    let mut shard = MemoryShard::new();
    let ns = Namespace("n".into());
    for &k in &[b"0" as &[u8], b"1", b"2", b"3"] {
        shard.put(Key::new("n", k), StoredValue::Inline(InlineValue(k.to_vec()))).unwrap();
    }

    let results = shard.scan(&ns, b"", Some(b"1"), None, 0).unwrap();
    let keys: Vec<&[u8]> = results.iter().map(|r| r.key.bytes.as_slice()).collect();
    assert_eq!(keys, vec![b"2", b"3"]);
}

#[test]
fn scan_limit_caps_results() {
    let mut shard = MemoryShard::new();
    let ns = Namespace("n".into());
    for &k in &[b"a" as &[u8], b"b", b"c", b"d", b"e"] {
        shard.put(Key::new("n", k), StoredValue::Inline(InlineValue(k.to_vec()))).unwrap();
    }

    let results = shard.scan(&ns, b"", None, None, 2).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn scan_isolated_between_namespaces() {
    let mut shard = MemoryShard::new();
    shard.put(Key::new("a", b"1"), StoredValue::Inline(InlineValue(b"v1".to_vec()))).unwrap();
    shard.put(Key::new("b", b"1"), StoredValue::Inline(InlineValue(b"v2".to_vec()))).unwrap();

    let a = shard.scan(&Namespace("a".into()), b"", None, None, 0).unwrap();
    let b = shard.scan(&Namespace("b".into()), b"", None, None, 0).unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(a[0].key.namespace.0, "a");
    assert_eq!(b.len(), 1);
    assert_eq!(b[0].key.namespace.0, "b");
}

#[test]
fn scan_empty_namespace_returns_nothing() {
    let mut shard = MemoryShard::new();
    shard.put(Key::new("used", b"x"), StoredValue::Inline(InlineValue(b"y".to_vec()))).unwrap();
    let results = shard.scan(&Namespace("unused".into()), b"", None, None, 0).unwrap();
    assert!(results.is_empty());
}
