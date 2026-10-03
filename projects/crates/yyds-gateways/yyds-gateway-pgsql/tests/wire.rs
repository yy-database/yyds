use yyds_gateway_pgsql::wire::{FrameError, decode_message, decode_startup, encode_message};

#[test]
fn startup_length_includes_code_and_preserves_pipeline() {
    let ssl = [0, 0, 0, 8, 4, 210, 22, 47];
    let pipeline = [ssl.as_slice(), ssl.as_slice()].concat();
    for boundary in 0..ssl.len() {
        assert_eq!(decode_startup(&ssl[..boundary], 8), Ok(None))
    }
    let packet = decode_startup(&pipeline, 8).unwrap().unwrap();
    assert_eq!(packet.code, 80877103);
    assert!(packet.payload.is_empty());
    assert_eq!(packet.consumed, 8);
    assert_eq!(decode_startup(&pipeline[packet.consumed..], 8).unwrap().unwrap(), packet);
}

#[test]
fn typed_message_binary_roundtrip_and_fragmentation() {
    let frame = encode_message(b'd', b"\0\xff\r\n", 1024).unwrap();
    assert_eq!(&frame[..5], &[b'd', 0, 0, 0, 8]);
    for boundary in 0..frame.len() {
        assert_eq!(decode_message(&frame[..boundary], 1024), Ok(None))
    }
    let message = decode_message(&frame, 1024).unwrap().unwrap();
    assert_eq!(message.tag, b'd');
    assert_eq!(message.payload, b"\0\xff\r\n");
    assert_eq!(message.consumed, frame.len());
}

#[test]
fn empty_typed_message_and_pipeline_boundary() {
    let frame = encode_message(b'X', b"", 5).unwrap();
    let pipeline = [frame.as_slice(), frame.as_slice()].concat();
    let message = decode_message(&pipeline, 5).unwrap().unwrap();
    assert!(message.payload.is_empty());
    assert_eq!(message.consumed, 5);
}

#[test]
fn invalid_and_advertised_oversized_lengths_are_rejected_early() {
    assert_eq!(decode_startup(&[0, 0, 0, 7], 1024), Err(FrameError::InvalidLength));
    assert_eq!(decode_startup(&[0, 0, 4, 1], 1024), Err(FrameError::FrameTooLarge));
    assert_eq!(decode_message(&[b'Q', 0, 0, 0, 3], 1024), Err(FrameError::InvalidLength));
    assert_eq!(decode_message(&[b'Q', 0, 0, 4, 0], 1024), Err(FrameError::FrameTooLarge));
    assert_eq!(decode_message(&[b'Q', 255, 255, 255, 255], 1024), Err(FrameError::FrameTooLarge));
    assert_eq!(encode_message(b'Q', b"", 4), Err(FrameError::FrameTooLarge));
}
