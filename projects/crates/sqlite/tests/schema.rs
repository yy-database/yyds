use yyds_sqlite::{SchemaObjectKind, SqliteDatabase, TableLimits, blank_database, read_named_table, read_schema};

fn varint(value: usize) -> Vec<u8> {
    if value < 128 { vec![value as u8] } else { vec![0x80 | (value >> 7) as u8, (value & 0x7f) as u8] }
}

fn record(kind: &str, name: &str, root: Option<i8>, sql: Option<&str>) -> Vec<u8> {
    let mut serials = Vec::new();
    let mut body = Vec::new();
    for text in [kind, name, name] {
        serials.extend(varint(text.len() * 2 + 13));
        body.extend_from_slice(text.as_bytes());
    }
    serials.push(if root.is_some() { 1 } else { 0 });
    if let Some(root) = root {
        body.push(root as u8);
    }
    if let Some(sql) = sql {
        serials.extend(varint(sql.len() * 2 + 13));
        body.extend_from_slice(sql.as_bytes());
    }
    else {
        serials.push(0);
    }
    let mut payload = vec![(serials.len() + 1) as u8];
    payload.extend(serials);
    payload.extend(body);
    payload
}

fn schema_file(records: Vec<Vec<u8>>) -> Vec<u8> {
    let mut bytes = blank_database().unwrap();
    bytes.resize(8192, 0);
    bytes[28..32].copy_from_slice(&2u32.to_be_bytes());
    bytes[56..60].copy_from_slice(&1u32.to_be_bytes());
    bytes[103..105].copy_from_slice(&(records.len() as u16).to_be_bytes());
    let mut content = 4096;
    for (index, payload) in records.iter().enumerate() {
        let length = varint(payload.len());
        content -= payload.len() + length.len() + 1;
        let pointer = 108 + index * 2;
        bytes[pointer..pointer + 2].copy_from_slice(&(content as u16).to_be_bytes());
        bytes[content..content + length.len()].copy_from_slice(&length);
        bytes[content + length.len()] = (index + 1) as u8;
        bytes[content + length.len() + 1..content + length.len() + 1 + payload.len()].copy_from_slice(payload);
    }
    bytes[105..107].copy_from_slice(&(content as u16).to_be_bytes());
    bytes[4096] = 13;
    bytes[4101..4103].copy_from_slice(&4096u16.to_be_bytes());
    bytes
}

#[test]
fn typed_schema_retains_names_roots_and_opaque_sql() {
    let sql = "CREATE TABLE \"例 table\"(value TEXT)";
    let bytes = schema_file(vec![record("table", "例 table", Some(2), Some(sql)), record("index", "auto", Some(2), None)]);
    let objects = read_schema(&bytes, TableLimits::default()).unwrap();
    assert_eq!(objects.len(), 2);
    assert_eq!(objects[0].kind, SchemaObjectKind::Table);
    assert_eq!(objects[0].name, "例 table");
    assert_eq!(objects[0].table_name, "例 table");
    assert_eq!(objects[0].root_page, Some(2));
    assert_eq!(objects[0].sql.as_deref(), Some(sql));
    assert_eq!(objects[1].kind, SchemaObjectKind::Index);
    assert_eq!(objects[1].sql, None);
    assert!(read_named_table(&bytes, "例 TABLE", TableLimits::default()).unwrap().is_empty());
    assert!(read_named_table(&bytes, "missing", TableLimits::default()).is_err());
}

#[test]
fn empty_database_and_schema_aliases() {
    let limits = TableLimits::default();
    assert!(read_schema(&[], limits).unwrap().is_empty());
    assert!(read_named_table(&[], "SQLITE_MASTER", limits).unwrap().is_empty());
    let database = SqliteDatabase::open_in_memory().unwrap();
    assert!(database.schema(limits).unwrap().is_empty());
    assert!(database.table("sqlite_schema", limits).unwrap().is_empty());
    assert!(database.table("not_present", limits).is_err());
}

#[test]
fn views_virtual_tables_and_schema_aliasing_are_not_executed() {
    for (kind, root) in [("view", Some(0)), ("table", Some(0)), ("table", None), ("table", Some(1))] {
        let bytes = schema_file(vec![record(kind, "probe", root, Some("opaque source"))]);
        assert!(read_named_table(&bytes, "probe", TableLimits::default()).is_err());
    }
    let mut bytes = schema_file(vec![record("table", "probe", Some(2), Some("opaque source"))]);
    bytes[4096] = 10;
    assert!(read_named_table(&bytes, "probe", TableLimits::default()).is_err());
}

#[test]
fn malformed_schema_fields_and_missing_encoding_are_rejected() {
    for payload in [
        record("unknown", "probe", Some(2), Some("opaque")),
        record("table", "", Some(2), Some("opaque")),
        record("table", "probe", Some(-1), Some("opaque")),
        record("view", "probe", Some(2), Some("opaque")),
        vec![2, 0],
    ] {
        assert!(read_schema(&schema_file(vec![payload]), TableLimits::default()).is_err());
    }
    let mut bytes = schema_file(vec![record("table", "probe", Some(2), Some("opaque"))]);
    bytes[56..60].fill(0);
    assert!(read_schema(&bytes, TableLimits::default()).is_err());
}

#[test]
fn ambiguous_names_and_resource_limits_are_rejected() {
    let bytes =
        schema_file(vec![record("table", "probe", Some(2), Some("opaque")), record("table", "PROBE", Some(2), Some("opaque"))]);
    assert!(read_named_table(&bytes, "Probe", TableLimits::default()).is_err());
    assert!(read_schema(&bytes, TableLimits { max_rows: 1, ..TableLimits::default() }).is_err());
    assert!(read_schema(&bytes, TableLimits { max_payload_bytes: 1, ..TableLimits::default() }).is_err());
}
