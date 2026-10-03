use yyds_sqlite::{TableLimits, blank_database, read_table};

fn leaf(payload: &[u8]) -> Vec<u8> {
    let mut bytes = blank_database().unwrap();
    let cell = 4096 - payload.len() - 2;
    bytes[103..105].copy_from_slice(&1u16.to_be_bytes());
    bytes[105..107].copy_from_slice(&(cell as u16).to_be_bytes());
    bytes[108..110].copy_from_slice(&(cell as u16).to_be_bytes());
    bytes[cell] = payload.len() as u8;
    bytes[cell + 1] = 1;
    bytes[cell + 2..].copy_from_slice(payload);
    bytes
}

#[test]
fn blank_schema_and_small_leaf_scan() {
    assert!(read_table(&blank_database().unwrap(), 1, TableLimits::default()).unwrap().is_empty());
    let rows = read_table(&leaf(&[2, 1, 42]), 1, TableLimits::default()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].rowid, 1);
    assert_eq!(rows[0].payload, [2, 1, 42]);
}

#[test]
fn explicit_row_payload_total_and_page_limits() {
    let bytes = leaf(&[2, 1, 42]);
    for limits in [
        TableLimits { max_rows: 0, ..TableLimits::default() },
        TableLimits { max_payload_bytes: 2, ..TableLimits::default() },
        TableLimits { max_total_payload_bytes: 2, ..TableLimits::default() },
        TableLimits { max_pages: 0, ..TableLimits::default() },
    ] {
        assert!(read_table(&bytes, 1, limits).is_err());
    }
}

#[test]
fn invalid_roots_and_index_pages_are_rejected() {
    let mut bytes = blank_database().unwrap();
    assert!(read_table(&[], 1, TableLimits::default()).is_err());
    for root in [0, 2, u32::MAX] {
        assert!(read_table(&bytes, root, TableLimits::default()).is_err());
    }
    bytes[100] = 10;
    assert!(read_table(&bytes, 1, TableLimits::default()).is_err());
}

#[test]
fn cell_header_reserved_tail_and_overlap_corruption_is_rejected() {
    let original = leaf(&[2, 1, 42]);
    for pointer in [0u16, 100, 108, 4096, u16::MAX] {
        let mut bytes = original.clone();
        bytes[108..110].copy_from_slice(&pointer.to_be_bytes());
        assert!(read_table(&bytes, 1, TableLimits::default()).is_err());
    }
    let mut bytes = original.clone();
    bytes[20] = 10;
    assert!(read_table(&bytes, 1, TableLimits::default()).is_err());
    let mut bytes = original.clone();
    bytes[107] = 61;
    assert!(read_table(&bytes, 1, TableLimits::default()).is_err());
    let mut bytes = original;
    bytes[103..105].copy_from_slice(&2u16.to_be_bytes());
    bytes[110..112].copy_from_slice(&4092u16.to_be_bytes());
    assert!(read_table(&bytes, 1, TableLimits::default()).is_err());
}

fn overflow_table(next: u32) -> Vec<u8> {
    let mut bytes = blank_database().unwrap();
    bytes.resize(8192, 0);
    bytes[28..32].copy_from_slice(&2u32.to_be_bytes());
    bytes[103..105].copy_from_slice(&1u16.to_be_bytes());
    let cell = 4096 - 908 - 3 - 4;
    bytes[105..107].copy_from_slice(&(cell as u16).to_be_bytes());
    bytes[108..110].copy_from_slice(&(cell as u16).to_be_bytes());
    bytes[cell..cell + 3].copy_from_slice(&[0xa7, 0x08, 1]);
    bytes[cell + 3..cell + 3 + 908].fill(42);
    bytes[cell + 3 + 908..4096].copy_from_slice(&2u32.to_be_bytes());
    bytes[4096..4100].copy_from_slice(&next.to_be_bytes());
    bytes[4100..8192].fill(42);
    bytes
}

#[test]
fn overflow_assembly_and_termination_checks() {
    let rows = read_table(&overflow_table(0), 1, TableLimits::default()).unwrap();
    assert_eq!(rows[0].payload, vec![42; 5000]);
    for next in [1, 2, 3] {
        assert!(read_table(&overflow_table(next), 1, TableLimits::default()).is_err());
    }
    let mut bytes = overflow_table(0);
    bytes[4092..4096].copy_from_slice(&0u32.to_be_bytes());
    assert!(read_table(&bytes, 1, TableLimits::default()).is_err());
    assert!(read_table(&overflow_table(0), 1, TableLimits { max_pages: 1, ..TableLimits::default() }).is_err());
}

#[test]
fn interior_cycles_and_duplicate_children_are_rejected() {
    let mut bytes = blank_database().unwrap();
    bytes[100] = 5;
    bytes[108..112].copy_from_slice(&1u32.to_be_bytes());
    assert!(read_table(&bytes, 1, TableLimits::default()).is_err());
    bytes[108..112].copy_from_slice(&2u32.to_be_bytes());
    bytes.resize(8192, 0);
    bytes[28..32].copy_from_slice(&2u32.to_be_bytes());
    bytes[4096] = 13;
    bytes[4101..4103].copy_from_slice(&4096u16.to_be_bytes());
    assert!(read_table(&bytes, 1, TableLimits::default()).unwrap().is_empty());
    bytes[103..105].copy_from_slice(&1u16.to_be_bytes());
    bytes[105..107].copy_from_slice(&4091u16.to_be_bytes());
    bytes[112..114].copy_from_slice(&4091u16.to_be_bytes());
    bytes[4091..4095].copy_from_slice(&2u32.to_be_bytes());
    bytes[4095] = 1;
    assert!(read_table(&bytes, 1, TableLimits::default()).is_err());
}

#[test]
fn oversized_varints_and_truncated_cell_bodies_are_rejected() {
    for tail in [vec![0xff], vec![0x01], vec![0x01, 0xff], vec![0xff; 9]] {
        let mut bytes = blank_database().unwrap();
        let cell = 4096 - tail.len();
        bytes[103..105].copy_from_slice(&1u16.to_be_bytes());
        bytes[105..107].copy_from_slice(&(cell as u16).to_be_bytes());
        bytes[108..110].copy_from_slice(&(cell as u16).to_be_bytes());
        bytes[cell..].copy_from_slice(&tail);
        assert!(read_table(&bytes, 1, TableLimits::default()).is_err());
    }
}

#[test]
fn signed_rowid_order_is_not_unsigned_varint_order() {
    let mut bytes = leaf(&[2, 1, 42]);
    bytes[103..105].copy_from_slice(&2u16.to_be_bytes());
    bytes[105..107].copy_from_slice(&4080u16.to_be_bytes());
    bytes[108..110].copy_from_slice(&4080u16.to_be_bytes());
    bytes[110..112].copy_from_slice(&4091u16.to_be_bytes());
    bytes[4080] = 1;
    bytes[4081..4090].fill(0xff);
    bytes[4090] = 0;
    let rows = read_table(&bytes, 1, TableLimits::default()).unwrap();
    assert_eq!(rows.iter().map(|row| row.rowid).collect::<Vec<_>>(), [-1, 1]);
    bytes[108..110].copy_from_slice(&4091u16.to_be_bytes());
    bytes[110..112].copy_from_slice(&4080u16.to_be_bytes());
    assert!(read_table(&bytes, 1, TableLimits::default()).is_err());
}
