use yyds_sqlite::{blank_database, validate_database, validate_header};

#[test]
fn empty_file_is_an_uninitialized_database_not_a_header() {
    assert_eq!(validate_database(&[]).expect("empty database"), 4096);
    assert!(validate_header(&[]).is_err());
}

#[test]
fn malformed_structural_header_fields_are_rejected() {
    let original = blank_database().unwrap();
    for (offset, value) in [(18, 0), (19, 3), (21, 63), (22, 31), (23, 31), (47, 5), (59, 4), (72, 1), (91, 1)] {
        let mut corrupt = original.clone();
        corrupt[offset] = value;
        assert!(validate_header(&corrupt).is_err(), "offset {offset}");
    }
    for length in 0..100 {
        assert!(validate_header(&original[..length]).is_err());
    }
}

#[test]
fn reserved_space_cannot_reduce_usable_page_below_480_bytes() {
    let mut bytes = blank_database().unwrap();
    bytes[16..18].copy_from_slice(&512u16.to_be_bytes());
    bytes[20] = 33;
    assert!(validate_header(&bytes).is_err());
    bytes[20] = 32;
    validate_header(&bytes).expect("480 usable bytes");
}

#[test]
fn valid_page_count_is_checked_but_stale_count_is_not_authoritative() {
    let mut bytes = blank_database().unwrap();
    bytes[28..32].copy_from_slice(&2u32.to_be_bytes());
    let counter = bytes[24..28].to_vec();
    bytes[92..96].copy_from_slice(&counter);
    assert!(validate_database(&bytes).is_err());
    bytes[92..96].copy_from_slice(&u32::MAX.to_be_bytes());
    assert_eq!(validate_database(&bytes).unwrap(), 4096);
}

#[test]
fn page_size_sentinel_and_wal_format_are_recognized() {
    let mut bytes = blank_database().unwrap();
    bytes.resize(65536, 0);
    bytes[16..18].copy_from_slice(&1u16.to_be_bytes());
    bytes[18..20].copy_from_slice(&[2, 2]);
    assert_eq!(validate_database(&bytes).unwrap(), 65536);
}
