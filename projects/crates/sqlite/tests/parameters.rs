use yyds_sqlite::{SqliteEngine, SqliteValue};

#[test]
fn parameters_preserve_storage_classes_and_do_not_become_sql_source() {
    let engine = SqliteEngine::open_in_memory().unwrap();
    engine.execute("CREATE TABLE sample (low, high, real_value, text_value, blob_value, null_value)").unwrap();
    let values = vec![
        SqliteValue::Integer(i64::MIN),
        SqliteValue::Integer(i64::MAX),
        SqliteValue::Real(1.25),
        SqliteValue::Text(b"'); DROP TABLE sample; --\0text".to_vec()),
        SqliteValue::Blob(vec![0, 255]),
        SqliteValue::Null,
    ];
    let inserted = engine.execute_with_parameters("INSERT INTO sample VALUES (?, ?, ?, ?, ?, ?)", &values).unwrap();
    assert_eq!(inserted.changes, 1);
    assert_eq!(engine.execute("SELECT * FROM sample").unwrap().rows, vec![values]);
}

#[test]
fn parameter_slot_order_includes_numbered_and_repeated_named_parameters() {
    let engine = SqliteEngine::open_in_memory().unwrap();
    let parameters = [SqliteValue::Integer(7), SqliteValue::Text(b"name".to_vec())];
    let result = engine.execute_with_parameters("SELECT ?2, ?1, ?2", &parameters).unwrap();
    assert_eq!(result.rows, vec![vec![parameters[1].clone(), parameters[0].clone(), parameters[1].clone()]]);
    let result = engine.execute_with_parameters("SELECT :value, :value", &parameters[..1]).unwrap();
    assert_eq!(result.rows, vec![vec![parameters[0].clone(), parameters[0].clone()]]);
}

#[test]
fn incorrect_parameter_count_is_rejected_without_writing() {
    let engine = SqliteEngine::open_in_memory().unwrap();
    engine.execute("CREATE TABLE sample (value)").unwrap();
    for parameters in [vec![], vec![SqliteValue::Null, SqliteValue::Null]] {
        assert!(engine.execute_with_parameters("INSERT INTO sample VALUES (?)", &parameters).is_err());
    }
    assert!(engine.execute_with_parameters("SELECT 1; INSERT INTO sample VALUES (1)", &[]).is_err());
    assert!(engine.execute("SELECT value FROM sample").unwrap().rows.is_empty());
}

#[test]
fn binding_binary_text_preserves_text_storage_class() {
    let engine = SqliteEngine::open_in_memory().unwrap();
    let value = SqliteValue::Text(vec![255, 0, 128]);
    let result = engine.execute_with_parameters("SELECT ?, typeof(?)", &[value.clone(), value.clone()]).unwrap();
    assert_eq!(result.rows, vec![vec![value, SqliteValue::Text(b"text".to_vec())]]);
}
