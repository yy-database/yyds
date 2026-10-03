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

#[test]
fn failed_snapshot_publication_preserves_records_and_revision() {
    let path = temp_path("failed-publication");
    let backup = path.with_extension("saved");
    let mut shard = FileShard::open(&path).expect("open");
    let existing = Key::new("tenant", b"existing".to_vec());
    let inserted = Key::new("tenant", b"inserted".to_vec());
    let original = StoredValue::Inline(InlineValue(b"original".to_vec()));
    let replacement = StoredValue::Inline(InlineValue(b"replacement".to_vec()));
    assert_eq!(shard.put(existing.clone(), original.clone()).unwrap(), 1);
    let published = fs::read(&path).unwrap();
    fs::rename(&path, &backup).unwrap();
    fs::create_dir(&path).unwrap();

    assert!(shard.put(existing.clone(), replacement.clone()).is_err());
    let record = shard.get(&existing).unwrap().expect("original remains visible");
    assert_eq!(record.value, original);
    assert_eq!(record.revision, 1);
    assert!(shard.put(inserted.clone(), replacement.clone()).is_err());
    assert_eq!(shard.get(&inserted).unwrap(), None);
    assert!(shard.delete(&existing).is_err());
    assert_eq!(shard.get(&existing).unwrap(), Some(record));
    assert_eq!(fs::read(&backup).unwrap(), published);

    fs::remove_dir(&path).unwrap();
    fs::rename(&backup, &path).unwrap();
    let reopened = FileShard::open(&path).unwrap();
    assert_eq!(reopened.get(&existing).unwrap().unwrap().value, original);
    assert_eq!(reopened.get(&inserted).unwrap(), None);
    drop(reopened);
    assert_eq!(shard.put(existing.clone(), replacement.clone()).unwrap(), 2);
    drop(shard);
    let reopened = FileShard::open(&path).unwrap();
    let record = reopened.get(&existing).unwrap().unwrap();
    assert_eq!(record.value, replacement);
    assert_eq!(record.revision, 2);
    drop(reopened);
    cleanup(&path);
}
