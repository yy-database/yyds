use yyds_types::{KeyValueAction, KeyValueCommand, KeyValueResult, Namespace};

#[test]
fn key_value_contract_preserves_namespace_and_opaque_bytes() {
    let command = KeyValueCommand {
        namespace: Namespace("redis:0".into()),
        key: vec![0, 0xff, b'k'],
        action: KeyValueAction::Put(vec![0, 0xfe, b'v']),
    };

    assert_eq!(command.namespace.0, "redis:0");
    assert_eq!(command.key, [0, 0xff, b'k']);
    assert_eq!(command.action, KeyValueAction::Put(vec![0, 0xfe, b'v']));
    match KeyValueResult::Get(Some(vec![0, 0xfe, b'v'])) {
        KeyValueResult::Get(Some(value)) => assert_eq!(value, [0, 0xfe, b'v']),
        result => panic!("unexpected result: {result:?}"),
    }
    assert_eq!(KeyValueResult::Put { revision: 9 }, KeyValueResult::Put { revision: 9 });
    assert_eq!(KeyValueResult::Delete { removed: true }, KeyValueResult::Delete { removed: true });
    assert_eq!(KeyValueAction::IncrementBy(-2), KeyValueAction::IncrementBy(-2));
    assert_eq!(KeyValueResult::Increment { value: -2 }, KeyValueResult::Increment { value: -2 });
}
