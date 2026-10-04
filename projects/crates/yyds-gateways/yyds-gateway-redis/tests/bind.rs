use std::collections::HashMap;

use yyds_execution::{KeyValueExecutor, LocalExecutor};
use yyds_gateway_redis::bind::{BindError, bind, bind_mget};
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
    assert_eq!(
        executor.execute(bind(&[b"GETDEL", b"getdel-key"], &namespace).unwrap()).unwrap(),
        KeyValueResult::GetDelete(None)
    );
    assert_eq!(
        executor.execute(bind(&[b"GETSET", b"getset-key", b"new"], &namespace).unwrap()).unwrap(),
        KeyValueResult::GetSet(None),
    );
    assert_eq!(
        executor.execute(bind(&[b"GETSET", b"getset-key", b"replacement"], &namespace).unwrap()).unwrap(),
        KeyValueResult::GetSet(Some(b"new".to_vec())),
    );
    assert_eq!(
        executor.execute(bind(&[b"SET", key, b"replacement", b"nx"], &namespace).unwrap()).unwrap(),
        KeyValueResult::PutIfAbsent { revision: None },
    );
    assert_eq!(
        executor.execute(bind(&[b"SET", b"new-key", value, b"NX"], &namespace).unwrap()).unwrap(),
        KeyValueResult::PutIfAbsent { revision: Some(5) },
    );
    assert_eq!(
        executor.execute(bind(&[b"SET", b"new-key", b"updated", b"XX"], &namespace).unwrap()).unwrap(),
        KeyValueResult::PutIfPresent { revision: Some(6) },
    );
    assert_eq!(
        executor.execute(bind(&[b"SET", b"absent", b"updated", b"XX"], &namespace).unwrap()).unwrap(),
        KeyValueResult::PutIfPresent { revision: None },
    );
    let result = executor.execute(bind(&[b"GET", key], &namespace).unwrap()).unwrap();
    assert_eq!(result, KeyValueResult::Get(Some(value.to_vec())));
    let result = executor.execute(bind(&[b"GET", key], &Namespace("redis:1".into())).unwrap()).unwrap();
    assert_eq!(result, KeyValueResult::Get(None));
    assert_eq!(
        executor.execute(bind(&[b"DECR", b"counter"], &namespace).unwrap()).unwrap(),
        KeyValueResult::Increment { value: -1 },
    );
    assert_eq!(
        executor.execute(bind(&[b"DECRBY", b"counter", b"2"], &namespace).unwrap()).unwrap(),
        KeyValueResult::Increment { value: -3 },
    );
    assert_eq!(
        executor.execute(bind(&[b"decrby", b"counter", b"-3"], &namespace).unwrap()).unwrap(),
        KeyValueResult::Increment { value: 0 },
    );
    executor.execute(bind(&[b"SET", b"minimum", b"-9223372036854775808"], &namespace).unwrap()).unwrap();
    assert!(executor.execute(bind(&[b"DECR", b"minimum"], &namespace).unwrap()).is_err());
    assert_eq!(
        executor.execute(bind(&[b"GET", b"minimum"], &namespace).unwrap()).unwrap(),
        KeyValueResult::Get(Some(b"-9223372036854775808".to_vec())),
    );
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
        (vec![b"GETSET".as_slice(), b"key"], BindError::WrongArity),
        (vec![b"SET".as_slice(), b"key"], BindError::WrongArity),
        (vec![b"SET".as_slice(), b"key", b"value", b"EX"], BindError::UnsupportedOptions),
        (vec![b"SET".as_slice(), b"key", b"value", b"NX", b"GET"], BindError::UnsupportedOptions),
        (vec![b"DEL".as_slice(), b"one", b"two"], BindError::WrongArity),
        (vec![b"INCR".as_slice()], BindError::WrongArity),
        (vec![b"INCRBY".as_slice(), b"key", b"01"], BindError::InvalidInteger),
        (vec![b"DECRBY".as_slice(), b"key", b"-9223372036854775808"], BindError::InvalidInteger),
        (vec![b"DECR".as_slice()], BindError::WrongArity),
        (vec![b"DECRBY".as_slice(), b"key"], BindError::WrongArity),
    ] {
        assert_eq!(bind(&arguments, &namespace), Err(expected));
    }
}

#[test]
fn mget_binding_preserves_order_and_duplicate_keys() {
    let namespace = Namespace("redis:0".into());
    let commands = bind_mget(&[b"MGET", b"first", b"second", b"first"], &namespace).unwrap();
    assert_eq!(commands.len(), 3);
    assert_eq!(commands[0].key, b"first");
    assert_eq!(commands[1].key, b"second");
    assert_eq!(commands[2].key, b"first");
    assert!(matches!(commands[0].action, yyds_types::KeyValueAction::Get));
    assert_eq!(bind_mget(&[b"MGET"], &namespace), Err(BindError::WrongArity));
}
