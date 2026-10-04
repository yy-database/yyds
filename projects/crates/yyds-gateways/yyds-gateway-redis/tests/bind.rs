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
    assert_eq!(executor.execute(bind(&[b"EXISTS", key], &namespace).unwrap()).unwrap(), KeyValueResult::Exists(true));
    assert_eq!(executor.execute(bind(&[b"EXISTS", b"missing"], &namespace).unwrap()).unwrap(), KeyValueResult::Exists(false));
    assert_eq!(
        executor.execute(bind(&[b"SET", b"getdel-key", value], &namespace).unwrap()).unwrap(),
        KeyValueResult::Put { revision: 2 },
    );
    assert_eq!(
        executor.execute(bind(&[b"GETDEL", b"getdel-key"], &namespace).unwrap()).unwrap(),
        KeyValueResult::GetDelete(Some(value.to_vec()))
    );
    assert_eq!(executor.execute(bind(&[b"GETDEL", b"getdel-key"], &namespace).unwrap()).unwrap(), KeyValueResult::GetDelete(None));
    assert_eq!(
        executor.execute(bind(&[b"SET", key, b"replacement", b"nx"], &namespace).unwrap()).unwrap(),
        KeyValueResult::PutIfAbsent { revision: None },
    );
    assert_eq!(
        executor.execute(bind(&[b"SET", b"new-key", value, b"NX"], &namespace).unwrap()).unwrap(),
        KeyValueResult::PutIfAbsent { revision: Some(3) },
    );
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
        (vec![b"EXISTS".as_slice()], BindError::WrongArity),
        (vec![b"EXISTS".as_slice(), b"one", b"two"], BindError::WrongArity),
        (vec![b"GETDEL".as_slice()], BindError::WrongArity),
        (vec![b"SET".as_slice(), b"key"], BindError::WrongArity),
        (vec![b"SET".as_slice(), b"key", b"value", b"XX"], BindError::UnsupportedOptions),
        (vec![b"SET".as_slice(), b"key", b"value", b"NX", b"GET"], BindError::UnsupportedOptions),
        (vec![b"DEL".as_slice(), b"one", b"two"], BindError::WrongArity),
        (vec![b"INCR".as_slice()], BindError::WrongArity),
        (vec![b"INCRBY".as_slice(), b"key", b"01"], BindError::InvalidInteger),
    ] {
        assert_eq!(bind(&arguments, &namespace), Err(expected));
    }
}
