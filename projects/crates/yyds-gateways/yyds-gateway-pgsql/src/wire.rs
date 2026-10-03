//! PostgreSQL startup and typed message framing, without SQL interpretation.

/// One startup packet. Its code identifies a protocol version or special request.
#[derive(Debug, PartialEq, Eq)]
pub struct StartupPacket<'input> {
    /// Protocol version, SSL/GSS request code, or cancellation request code.
    pub code: u32,
    /// Bytes after the code, interpreted by the connection state machine.
    pub payload: &'input [u8],
    /// Consumed bytes including length and code.
    pub consumed: usize,
}

/// One typed message borrowing binary payload bytes.
#[derive(Debug, PartialEq, Eq)]
pub struct Message<'input> {
    /// Message identifier, interpreted according to direction and session state.
    pub tag: u8,
    /// Binary message payload.
    pub payload: &'input [u8],
    /// Consumed bytes including the tag and length word.
    pub consumed: usize,
}

/// Malformed framing or a resource limit violation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameError {
    /// Length is smaller than its own mandatory header.
    InvalidLength,
    /// Total encoded frame length exceeds the caller's limit.
    FrameTooLarge,
}

/// Decodes an untyped startup packet. Does not accept or authenticate a session.
pub fn decode_startup(input: &[u8], max_frame: usize) -> Result<Option<StartupPacket<'_>>, FrameError> {
    if input.len() < 4 {
        return Ok(None);
    }
    let length = read_u32(input) as usize;
    if length < 8 {
        return Err(FrameError::InvalidLength);
    }
    if length > max_frame {
        return Err(FrameError::FrameTooLarge);
    }
    if input.len() < length {
        return Ok(None);
    }
    Ok(Some(StartupPacket { code: read_u32(&input[4..]), payload: &input[8..length], consumed: length }))
}

/// Decodes one typed message. The length word includes itself, not the tag.
pub fn decode_message(input: &[u8], max_frame: usize) -> Result<Option<Message<'_>>, FrameError> {
    if input.len() < 5 {
        return Ok(None);
    }
    let length = read_u32(&input[1..]) as usize;
    if length < 4 {
        return Err(FrameError::InvalidLength);
    }
    let consumed = length.checked_add(1).ok_or(FrameError::FrameTooLarge)?;
    if consumed > max_frame {
        return Err(FrameError::FrameTooLarge);
    }
    if input.len() < consumed {
        return Ok(None);
    }
    Ok(Some(Message { tag: input[0], payload: &input[5..consumed], consumed }))
}

/// Encodes a typed message under a caller-provided total frame limit.
pub fn encode_message(tag: u8, payload: &[u8], max_frame: usize) -> Result<Vec<u8>, FrameError> {
    let length = payload.len().checked_add(4).ok_or(FrameError::FrameTooLarge)?;
    let encoded_length = u32::try_from(length).map_err(|_| FrameError::FrameTooLarge)?;
    let consumed = length.checked_add(1).ok_or(FrameError::FrameTooLarge)?;
    if consumed > max_frame {
        return Err(FrameError::FrameTooLarge);
    }
    let mut bytes = Vec::with_capacity(consumed);
    bytes.push(tag);
    bytes.extend_from_slice(&encoded_length.to_be_bytes());
    bytes.extend_from_slice(payload);
    Ok(bytes)
}

fn read_u32(input: &[u8]) -> u32 {
    u32::from_be_bytes(input[..4].try_into().expect("checked message header"))
}
