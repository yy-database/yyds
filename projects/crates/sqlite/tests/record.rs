use yyds_sqlite::{RecordLimits, RecordValue, TextEncoding, decode_record, decode_varint};

#[test]
fn varint_boundaries_and_truncation() {
    assert_eq!(decode_varint(&[0]).unwrap(), (0, 1));
    assert_eq!(decode_varint(&[127]).unwrap(), (127, 1));
    assert_eq!(decode_varint(&[129, 0]).unwrap(), (128, 2));
    assert_eq!(decode_varint(&[255; 9]).unwrap(), (u64::MAX, 9));
    for length in 0..9 {
        assert!(decode_varint(&[255; 9][..length]).is_err())
    }
}

#[test]
fn scalar_serial_types_preserve_signed_numbers_and_binary_bytes() {
    let mut payload = vec![10, 0, 1, 3, 6, 7, 8, 9, 16, 19];
    payload.push(128);
    payload.extend_from_slice(&[128, 0, 0]);
    payload.extend_from_slice(&i64::MIN.to_be_bytes());
    payload.extend_from_slice(&1.5f64.to_bits().to_be_bytes());
    payload.extend_from_slice(b"\0\xffabc");
    assert_eq!(
        decode_record(&payload, TextEncoding::Utf8, RecordLimits::default()).unwrap(),
        vec![
            RecordValue::Null,
            RecordValue::Integer(-128),
            RecordValue::Integer(-8388608),
            RecordValue::Integer(i64::MIN),
            RecordValue::Real(1.5),
            RecordValue::Integer(0),
            RecordValue::Integer(1),
            RecordValue::Blob(b"\0\xff"),
            RecordValue::Text("abc".into()),
        ]
    );
}

#[test]
fn utf16_endianness_and_surrogate_pairs() {
    for encoding in [TextEncoding::Utf16Le, TextEncoding::Utf16Be] {
        let text = "您好🌸";
        let words = text.encode_utf16().collect::<Vec<_>>();
        let mut payload = vec![2, 13 + (words.len() * 4) as u8];
        for word in words {
            payload.extend_from_slice(&if encoding == TextEncoding::Utf16Le { word.to_le_bytes() } else { word.to_be_bytes() });
        }
        assert_eq!(decode_record(&payload, encoding, RecordLimits::default()).unwrap(), vec![RecordValue::Text(text.into())]);
    }
}

#[test]
fn malformed_records_and_limits_fail_without_panics() {
    for payload in [b"".as_slice(), &[0], &[2], &[2, 10], &[2, 11], &[2, 128], &[2, 6, 0], &[1, 0], &[2, 15, 255]] {
        assert!(decode_record(payload, TextEncoding::Utf8, RecordLimits::default()).is_err(), "{payload:?}");
    }
    assert!(decode_record(&[2, 15, 0], TextEncoding::Utf16Le, RecordLimits::default()).is_err());
    assert!(decode_record(&[3, 0, 0], TextEncoding::Utf8, RecordLimits { max_columns: 1, ..RecordLimits::default() }).is_err());
    assert!(decode_record(&[1], TextEncoding::Utf8, RecordLimits { max_payload_bytes: 0, ..RecordLimits::default() }).is_err());
}
