use yyds_gateway_mysql::{handle_sql, DEFAULT_PORT, GATEWAY_ID};

#[test]
fn mysql_select_one() {
    let lines = handle_sql("SELECT 1").expect("select");
    assert_eq!(lines, vec!["1", "1"]);
}

#[test]
fn mysql_select_version() {
    let lines = handle_sql("SELECT version()").expect("version");
    assert_eq!(lines.len(), 1);
    assert!(lines[0].starts_with("yyds-"));
    assert_eq!(GATEWAY_ID, "mysql");
    assert_eq!(DEFAULT_PORT, 3306);
}
