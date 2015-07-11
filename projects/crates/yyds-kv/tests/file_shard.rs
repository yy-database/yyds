use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};

use yyds_kv::{FileShard, InlineValue, Key, KvStore, ObjectRef, StoredValue};

fn temp_path(label: &str) -> PathBuf {
    let nonce = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos();
    std::env::temp_dir().join(format!("yyds-kv-{label}-{nonce}.yykv"))
}

fn cleanup(path: &PathBuf) {
    let _ = fs::remove_file(path);
}

#[test]
fn file_shard_round_trips_records_and_revisions() {
    let path = temp_path("round-trip");
    let mut shard = FileShard::open(&path).expect("open");
    let inline_key = Key::new("tenant-a", b"inline".to_vec());
    let object_key = Key::new("tenant-b", b"object".to_vec());
    assert_eq!(
        shard.put(object_key.clone(), StoredValue::Object(ObjectRef { hash_hex: "ab12".into() }),).expect("put object"),
        1
    );
    assert_eq!(shard.put(inline_key.clone(), StoredValue::Inline(InlineValue(b"payload".to_vec()))).expect("put inline"), 2);
    drop(shard);

    let mut reopened = FileShard::open(&path).expect("reopen");
    assert_eq!(reopened.get(&inline_key).unwrap().unwrap().revision, 2);
    assert_eq!(reopened.get(&object_key).unwrap().unwrap().revision, 1);
    assert!(reopened.delete(&inline_key).unwrap());
    drop(reopened);

    let reopened = FileShard::open(&path).expect("reopen after delete");
    assert_eq!(reopened.get(&inline_key).unwrap(), None);
    assert_eq!(reopened.get(&object_key).unwrap().unwrap().revision, 1);
    cleanup(&path);
}

#[test]
fn file_shard_rejects_corrupt_bytes() {
    let path = temp_path("corrupt");
    let shard = FileShard::open(&path).expect("open");
    drop(shard);
    let mut bytes = fs::read(&path).expect("read");
    bytes[0] ^= 0xff;
    fs::write(&path, bytes).expect("corrupt");
    assert!(FileShard::open(&path).is_err());
    cleanup(&path);
}
