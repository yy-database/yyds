use std::process::Command;
use yyds_sqlite::{
    RecordLimits, RecordValue, SqliteDatabase, TextEncoding, decode_record, decode_varint, read_existing, validate_database,
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
        db.execute('insert into samples values (?, ?, ?)', (1, bytes(range(256)) * 40, 'SQLite binary oracle'))
        db.execute('create table record_probe (negative integer, huge integer, fraction real, name text, data blob, absent)')
        values = (-8388608, -9223372036854775808, 1.5, '您好🌸', b'\x00\xff', None)
        db.execute('insert into record_probe values (?, ?, ?, ?, ?, ?)', values)
        db.commit()
        assert db.execute('pragma integrity_check').fetchone() == ('ok',)
        assert db.execute('select * from record_probe').fetchone() == values
        root_page = db.execute("select rootpage from sqlite_schema where name='record_probe'").fetchone()[0]
        print('probe', page_size, root_page)
print(sqlite3.sqlite_version)
"#;
    let output = Command::new(python).arg("-c").arg(script).arg(dir.path()).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    let metadata = String::from_utf8(output.stdout).unwrap();
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
        let snapshot = SqliteDatabase::open(&path).unwrap();
        assert_eq!(snapshot.pages(), original);
        drop(snapshot);
        assert_eq!(std::fs::read(path).unwrap(), original);
    }
}
