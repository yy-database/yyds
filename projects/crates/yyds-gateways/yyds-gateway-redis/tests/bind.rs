use std::collections::HashMap;

use yyds_execution::{KeyValueExecutor, LocalExecutor};
use yyds_gateway_redis::bind::{BindError, bind};
use yyds_kv::MemoryShard;
use yyds_types::{KeyValueResult, Namespace, ShardEpoch, ShardId, ShardMap};

#[test]
fn redis_binding_executes_binary_values_and_isolates_namespaces() {
    let shard = ShardId("a".into());
    let executor = LocalExecutor::new(
        ShardMap::new(ShardEpoch(1), vec![shard.clone()]).unwrap(),
        HashMap::from([(shard, MemoryShard::new())]),
    )
    .unwrap();
    let namespace = Namespace("redis:0".into());
    let key = b"\0\xffkey";
    let value = b"\0\xfevalue";
    let result = executor.execute(bind(&[b"sEt", key, value], &namespace).unwrap()).unwrap();
    assert_eq!(result, KeyValueResult::Put { revision: 1 });
    let result = executor.execute(bind(&[b"GET", key], &namespace).unwrap()).unwrap();
    assert_eq!(result, KeyValueResult::Get(Some(value.to_vec())));
    let result = executor.execute(bind(&[b"GET", key], &Namespace("redis:1".into())).unwrap()).unwrap();
    assert_eq!(result, KeyValueResult::Get(None));
    let result = executor.execute(bind(&[b"DEL", key], &namespace).unwrap()).unwrap();
    assert_eq!(result, KeyValueResult::Delete { removed: true });
    let result = executor.execute(bind(&[b"DEL", key], &namespace).unwrap()).unwrap();
    assert_eq!(result, KeyValueResult::Delete { removed: false });
}

#[test]
fn unsupported_shapes_are_rejected_before_execution() {
    let namespace = Namespace("redis:0".into());
    for (arguments, expected) in [
        (vec![], BindError::EmptyCommand),
        (vec![b"GET".as_slice()], BindError::WrongArity),
        (vec![b"SET".as_slice(), b"key"], BindError::WrongArity),
        (vec![b"SET".as_slice(), b"key", b"value", b"NX"], BindError::UnsupportedOptions),
        (vec![b"DEL".as_slice(), b"one", b"two"], BindError::WrongArity),
        (vec![b"INCR".as_slice(), b"key"], BindError::UnsupportedCommand),
    ] {
        assert_eq!(bind(&arguments, &namespace), Err(expected));
    }
}
