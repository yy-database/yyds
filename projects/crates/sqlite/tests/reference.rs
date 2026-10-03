use std::process::Command;
use yyds_sqlite::{SqliteDatabase, read_existing, validate_database};

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
        db.commit()
        assert db.execute('pragma integrity_check').fetchone() == ('ok',)
print(sqlite3.sqlite_version)
"#;
    let output = Command::new(python).arg("-c").arg(script).arg(dir.path()).output().unwrap();
    assert!(output.status.success(), "{}", String::from_utf8_lossy(&output.stderr));
    for page_size in [512, 4096, 65536] {
        let path = dir.path().join(format!("{page_size}.sqlite"));
        let original = std::fs::read(&path).unwrap();
        assert_eq!(validate_database(&original).unwrap(), page_size);
        assert_eq!(read_existing(&path).unwrap(), original);
        let snapshot = SqliteDatabase::open(&path).unwrap();
        assert_eq!(snapshot.pages(), original);
        drop(snapshot);
        assert_eq!(std::fs::read(path).unwrap(), original);
    }
}
