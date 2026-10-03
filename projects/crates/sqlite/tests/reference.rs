use std::{
    process::{Command, Stdio},
    time::{Duration, Instant},
};
use yyds_sqlite::{
    RecordLimits, RecordValue, SchemaObjectKind, SqliteDatabase, TableLimits, TextEncoding, decode_record, decode_varint,
    read_existing, read_named_table, read_schema, read_table, validate_database,
};

#[test]
#[ignore = "requires YYDS_SQLITE_REFERENCE_PYTHON pointing to Python with sqlite3"]
fn reference_sqlite_files_and_blank_database_interoperate() {
    let python = std::env::var_os("YYDS_SQLITE_REFERENCE_PYTHON").expect("reference Python executable");
    let dir = tempfile::tempdir().unwrap();
    let blank = dir.path().join("blank.sqlite");
    drop(SqliteDatabase::open(&blank).unwrap());
    let script = r#"
import pathlib, sqlite3, sys
root = pathlib.Path(sys.argv[1])
with sqlite3.connect(root / 'blank.sqlite') as db:
    assert db.execute('pragma integrity_check').fetchone() == ('ok',)
    assert db.execute('select count(*) from sqlite_schema').fetchone() == (0,)
for page_size, encoding in [(512, 'UTF-8'), (4096, 'UTF-16le'), (65536, 'UTF-16be')]:
    with sqlite3.connect(root / f'{page_size}.sqlite') as db:
        db.execute(f'pragma page_size={page_size}')
        db.execute(f"pragma encoding='{encoding}'")
        db.execute('create table samples (id integer primary key, payload blob, name text)')
        ids = [-9223372036854775808, *range(-100, 100), 9223372036854775807]
        db.executemany('insert into samples values (?, ?, ?)', [(rowid, bytes(range(256)) * (300 if abs(rowid) > 100 else 40), '您好🌸') for rowid in ids])
        db.execute('create table record_probe (negative integer, huge integer, fraction real, name text, data blob, absent)')
        values = (-8388608, -9223372036854775808, 1.5, '您好🌸', b'\x00\xff', None)
        db.execute('insert into record_probe values (?, ?, ?, ?, ?, ?)', values)
        db.execute('create index sample_names on samples(name)')
        db.execute('create view sample_view as select id from samples')
        db.execute('create trigger sample_touch after update on samples begin select 1; end')
        db.execute('create table keyed (key text primary key, value integer) without rowid')
        db.execute('create table unique_probe (value text unique)')
        db.commit()
        assert db.execute('pragma integrity_check').fetchone() == ('ok',)
        assert db.execute('select * from record_probe').fetchone() == values
        root_page = db.execute("select rootpage from sqlite_schema where name='record_probe'").fetchone()[0]
        print('probe', page_size, root_page)
        root_page = db.execute("select rootpage from sqlite_schema where name='samples'").fetchone()[0]
        assert [row[0] for row in db.execute('select id from samples order by id')] == ids
        print('samples', page_size, root_page)
        for kind, name, table_name, root_page, sql in db.execute('select type, name, tbl_name, rootpage, sql from sqlite_schema order by rowid'):
            encoded_sql = '-' if sql is None else sql.encode().hex()
            encoded_root = '-' if root_page is None else str(root_page)
            print('schema', page_size, kind, name.encode().hex(), table_name.encode().hex(), encoded_root, encoded_sql)
print(sqlite3.sqlite_version)
"#;
    let stdout = dir.path().join("reference.stdout");
    let stderr = dir.path().join("reference.stderr");
    let mut child = Command::new(python)
        .arg("-c")
        .arg(script)
        .arg(dir.path())
        .stdin(Stdio::null())
        .stdout(std::fs::File::create(&stdout).unwrap())
        .stderr(std::fs::File::create(&stderr).unwrap())
        .spawn()
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if Instant::now() >= deadline {
            let _ = child.kill();
            let _ = child.wait();
            panic!("SQLite reference deadline exceeded: {}", std::fs::read_to_string(&stderr).unwrap());
        }
        std::thread::sleep(Duration::from_millis(10));
    };
    assert!(status.success(), "{}", std::fs::read_to_string(&stderr).unwrap());
    let metadata = std::fs::read_to_string(stdout).unwrap();
    for page_size in [512, 4096, 65536] {
        let path = dir.path().join(format!("{page_size}.sqlite"));
        let original = std::fs::read(&path).unwrap();
        assert_eq!(validate_database(&original).unwrap(), page_size);
        assert_eq!(read_existing(&path).unwrap(), original);
        let prefix = format!("probe {page_size} ");
        let root: usize = metadata.lines().find_map(|line| line.strip_prefix(&prefix)).unwrap().parse().unwrap();
        let page = &original[(root - 1) * page_size..root * page_size];
        assert_eq!(page[0], 13);
        assert_eq!(u16::from_be_bytes([page[3], page[4]]), 1);
        let cell = usize::from(u16::from_be_bytes([page[8], page[9]]));
        let (payload_length, length_bytes) = decode_varint(&page[cell..]).unwrap();
        let (rowid, rowid_bytes) = decode_varint(&page[cell + length_bytes..]).unwrap();
        assert_eq!(rowid, 1);
        let body = cell + length_bytes + rowid_bytes;
        let encoding = match page_size {
            512 => TextEncoding::Utf8,
            4096 => TextEncoding::Utf16Le,
            _ => TextEncoding::Utf16Be,
        };
        assert_eq!(
            decode_record(&page[body..body + payload_length as usize], encoding, RecordLimits::default()).unwrap(),
            vec![
                RecordValue::Integer(-8388608),
                RecordValue::Integer(i64::MIN),
                RecordValue::Real(1.5),
                RecordValue::Text("您好🌸".into()),
                RecordValue::Blob(b"\0\xff"),
                RecordValue::Null,
            ]
        );
        let prefix = format!("samples {page_size} ");
        let root: u32 = metadata.lines().find_map(|line| line.strip_prefix(&prefix)).unwrap().parse().unwrap();
        assert_eq!(original[(root as usize - 1) * page_size], 5);
        let rows = read_table(&original, root, TableLimits::default()).unwrap();
        let expected_ids: Vec<i64> = std::iter::once(i64::MIN).chain(-100..100).chain(std::iter::once(i64::MAX)).collect();
        assert_eq!(rows.iter().map(|row| row.rowid).collect::<Vec<_>>(), expected_ids);
        for row in rows {
            let repetitions = if row.rowid == i64::MIN || row.rowid == i64::MAX { 300 } else { 40 };
            let expected_blob: Vec<u8> = (0..repetitions).flat_map(|_| 0u8..=255).collect();
            assert_eq!(
                decode_record(&row.payload, encoding, RecordLimits::default()).unwrap(),
                vec![RecordValue::Null, RecordValue::Blob(&expected_blob), RecordValue::Text("您好🌸".into())]
            );
        }
        let schema = read_schema(&original, TableLimits::default()).unwrap();
        let expected: Vec<_> = metadata.lines().filter(|line| line.starts_with(&format!("schema {page_size} "))).collect();
        assert_eq!(schema.len(), expected.len());
        for (object, expected) in schema.iter().zip(expected) {
            let kind = match object.kind {
                SchemaObjectKind::Table => "table",
                SchemaObjectKind::Index => "index",
                SchemaObjectKind::View => "view",
                SchemaObjectKind::Trigger => "trigger",
            };
            let hex = |value: &str| value.as_bytes().iter().map(|byte| format!("{byte:02x}")).collect::<String>();
            let root = object.root_page.map_or_else(|| "-".into(), |root| root.to_string());
            let sql = object.sql.as_deref().map_or_else(|| "-".into(), hex);
            assert_eq!(
                format!("schema {page_size} {kind} {} {} {root} {sql}", hex(&object.name), hex(&object.table_name)),
                expected
            );
        }
        assert_eq!(
            read_named_table(&original, "SAMPLES", TableLimits::default()).unwrap(),
            read_table(&original, root, TableLimits::default()).unwrap()
        );
        assert!(read_named_table(&original, "keyed", TableLimits::default()).is_err());
        assert!(read_named_table(&original, "sample_view", TableLimits::default()).is_err());
        let snapshot = SqliteDatabase::open(&path).unwrap();
        assert_eq!(snapshot.schema(TableLimits::default()).unwrap(), schema);
        assert_eq!(snapshot.table("samples", TableLimits::default()).unwrap().len(), 202);
        assert_eq!(snapshot.pages(), original);
        drop(snapshot);
        assert_eq!(std::fs::read(path).unwrap(), original);
    }
}
