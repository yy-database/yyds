//! YY wire protocol codec — shared frame layout for TCP and WebSocket.
//!
//! Phase-1 subset: frame layout + Hello / Info / SchemaGet / SchemaEnsure /
//! KvGet / KvPut / Error bodies. UDF / Micro paths come later.

use std::io::{Read, Write};

/// Product magic accepted on inbound frames.
pub const MAGIC_YYDB: &[u8; 4] = b"YYDB";
/// Product magic accepted on inbound frames (YYDS self-claim).
pub const MAGIC_YYDS: &[u8; 4] = b"YYDS";
/// Wire version digits accepted by this server (`0000`).
pub const WIRE_VERSION: &[u8; 4] = b"0000";
/// Header size: magic(4) + version(4) + msg_type(2) + flags(2) + request_id(4) + body_len(4).
pub const HEADER_LEN: usize = 20;
/// Absolute cap on body length.
pub const MAX_BODY_LEN: u32 = 16 * 1024 * 1024;

/// Wire message type codes — mirrored from `yydb-protocol` so the TypeScript
/// client (which encodes using `MsgType`) sees identical numbers.
#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageType {
    Hello = 1,
    HelloOk = 2,
    Info = 3,
    InfoOk = 4,
    SchemaGet = 5,
    SchemaGetOk = 6,
    SchemaEnsure = 7,
    SchemaEnsureOk = 8,
    KvGet = 9,
    KvGetOk = 10,
    KvPut = 11,
    KvPutOk = 12,
    Authenticate = 13,
    AuthenticateOk = 14,
    Error = 255,
    /// Unknown message type — still framed so the peer can answer with Error.
    Unknown(u16),
}

impl MessageType {
    pub fn from_u16(value: u16) -> Self {
        match value {
            1 => Self::Hello,
            2 => Self::HelloOk,
            3 => Self::Info,
            4 => Self::InfoOk,
            5 => Self::SchemaGet,
            6 => Self::SchemaGetOk,
            7 => Self::SchemaEnsure,
            8 => Self::SchemaEnsureOk,
            9 => Self::KvGet,
            10 => Self::KvGetOk,
            11 => Self::KvPut,
            12 => Self::KvPutOk,
            13 => Self::Authenticate,
            14 => Self::AuthenticateOk,
            255 => Self::Error,
            other => Self::Unknown(other),
        }
    }

    pub fn as_u16(self) -> u16 {
        match self {
            Self::Hello => 1,
            Self::HelloOk => 2,
            Self::Info => 3,
            Self::InfoOk => 4,
            Self::SchemaGet => 5,
            Self::SchemaGetOk => 6,
            Self::SchemaEnsure => 7,
            Self::SchemaEnsureOk => 8,
            Self::KvGet => 9,
            Self::KvGetOk => 10,
            Self::KvPut => 11,
            Self::KvPutOk => 12,
            Self::Authenticate => 13,
            Self::AuthenticateOk => 14,
            Self::Error => 255,
            Self::Unknown(code) => code,
        }
    }

    /// Returns true for frame types the server never sends to clients.
    #[allow(dead_code)]
    pub fn is_client_only(self) -> bool {
        matches!(
            self,
            Self::HelloOk
                | Self::InfoOk
                | Self::SchemaGetOk
                | Self::SchemaEnsureOk
                | Self::KvGetOk
                | Self::KvPutOk
                | Self::AuthenticateOk
                | Self::Error
        )
    }
}

/// One wire frame.
#[derive(Debug, Clone)]
pub struct Frame {
    pub msg_type: MessageType,
    pub flags: u16,
    pub request_id: u32,
    pub body: Vec<u8>,
}

impl Frame {
    pub fn new(msg_type: MessageType, request_id: u32, body: impl Into<Vec<u8>>) -> Self {
        Self { msg_type, flags: 0, request_id, body: body.into() }
    }

    /// Encode for on-wire transport using `YYDS` product magic.
    pub fn encode(&self) -> Result<Vec<u8>, String> {
        self.encode_as(MAGIC_YYDS)
    }

    /// Encode with an explicit product magic (YYDB or YYDS).
    pub fn encode_as(&self, product: &[u8; 4]) -> Result<Vec<u8>, String> {
        if product != MAGIC_YYDB && product != MAGIC_YYDS {
            return Err("wire product magic must be YYDB or YYDS".into());
        }
        if self.body.len() > MAX_BODY_LEN as usize {
            return Err("wire body too large".into());
        }
        let mut out = Vec::with_capacity(HEADER_LEN + self.body.len());
        out.extend_from_slice(product);
        out.extend_from_slice(WIRE_VERSION);
        out.extend_from_slice(&self.msg_type.as_u16().to_le_bytes());
        out.extend_from_slice(&self.flags.to_le_bytes());
        out.extend_from_slice(&self.request_id.to_le_bytes());
        out.extend_from_slice(&(self.body.len() as u32).to_le_bytes());
        out.extend_from_slice(&self.body);
        Ok(out)
    }
}

/// Decode one frame from a complete buffer (header + body).
pub fn decode_frame(bytes: &[u8]) -> Result<Frame, String> {
    if bytes.len() < HEADER_LEN {
        return Err("wire frame truncated header".into());
    }
    let magic = &bytes[0..4];
    let version = &bytes[4..8];
    if magic != MAGIC_YYDB && magic != MAGIC_YYDS {
        return Err("wire frame bad product magic (want YYDB|YYDS)".into());
    }
    if version != WIRE_VERSION {
        return Err(format!("wire version not supported (want 0000, got {})", String::from_utf8_lossy(version)));
    }
    let msg_raw = u16::from_le_bytes([bytes[8], bytes[9]]);
    let flags = u16::from_le_bytes([bytes[10], bytes[11]]);
    let request_id = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    let body_len = u32::from_le_bytes([bytes[16], bytes[17], bytes[18], bytes[19]]);
    if body_len > MAX_BODY_LEN {
        return Err("wire body too large".into());
    }
    let need = HEADER_LEN + body_len as usize;
    if bytes.len() < need {
        return Err("wire frame truncated body".into());
    }
    Ok(Frame { msg_type: MessageType::from_u16(msg_raw), flags, request_id, body: bytes[HEADER_LEN..need].to_vec() })
}

/// Read exactly one frame from a blocking reader (TCP).
pub fn read_frame<R: Read>(reader: &mut R) -> Result<Frame, String> {
    let mut header = [0_u8; HEADER_LEN];
    reader.read_exact(&mut header).map_err(|e| e.to_string())?;
    let body_len = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
    if body_len > MAX_BODY_LEN {
        return Err("wire body too large".into());
    }
    let mut body = vec![0_u8; body_len as usize];
    if body_len > 0 {
        reader.read_exact(&mut body).map_err(|e| e.to_string())?;
    }
    let mut full = Vec::with_capacity(HEADER_LEN + body.len());
    full.extend_from_slice(&header);
    full.extend_from_slice(&body);
    decode_frame(&full)
}

/// Write one frame to a blocking writer (TCP).
pub fn write_frame<W: Write>(writer: &mut W, frame: &Frame) -> Result<(), String> {
    let bytes = frame.encode()?;
    writer.write_all(&bytes).map_err(|e| e.to_string())?;
    writer.flush().map_err(|e| e.to_string())?;
    Ok(())
}

// -------- Body codec (Phase-1 subset) --------

fn read_u32(buf: &[u8], offset: &mut usize) -> Result<u32, String> {
    if *offset + 4 > buf.len() {
        return Err("wire body truncated u32".into());
    }
    let value = u32::from_le_bytes([buf[*offset], buf[*offset + 1], buf[*offset + 2], buf[*offset + 3]]);
    *offset += 4;
    Ok(value)
}

fn read_bytes<'a>(buf: &'a [u8], offset: &mut usize, len: u32) -> Result<&'a [u8], String> {
    let len = len as usize;
    if *offset + len > buf.len() {
        return Err("wire body truncated bytes".into());
    }
    let slice = &buf[*offset..*offset + len];
    *offset += len;
    Ok(slice)
}

fn push_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn push_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    push_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

// --- SchemaGetOk ---

pub fn encode_schema_ok(schema: Option<(u32, &str)>) -> Vec<u8> {
    match schema {
        None => vec![0],
        Some((version, document)) => {
            let mut body = vec![1];
            push_u32(&mut body, version);
            push_bytes(&mut body, document.as_bytes());
            body
        }
    }
}

pub fn decode_schema_ensure(body: &[u8]) -> Result<(u32, String), String> {
    let mut offset = 0;
    let version = read_u32(body, &mut offset)?;
    let doc_len = read_u32(body, &mut offset)?;
    let doc = read_bytes(body, &mut offset, doc_len)?;
    let document = std::str::from_utf8(doc).map_err(|_| "schema document is not utf-8")?.to_owned();
    Ok((version, document))
}

// --- KvGet / KvPut ---

pub fn decode_kv_get(body: &[u8]) -> Result<String, String> {
    let mut offset = 0;
    let key_len = read_u32(body, &mut offset)?;
    let key = read_bytes(body, &mut offset, key_len)?;
    Ok(std::str::from_utf8(key).map_err(|_| "kv key is not utf-8")?.to_owned())
}

pub fn decode_kv_put(body: &[u8]) -> Result<(String, Vec<u8>), String> {
    let mut offset = 0;
    let key_len = read_u32(body, &mut offset)?;
    let key = read_bytes(body, &mut offset, key_len)?;
    let key = std::str::from_utf8(key).map_err(|_| "kv key is not utf-8")?.to_owned();
    let val_len = read_u32(body, &mut offset)?;
    let value = read_bytes(body, &mut offset, val_len)?.to_vec();
    Ok((key, value))
}

pub fn encode_kv_get_ok(value: Option<Vec<u8>>) -> Vec<u8> {
    match value {
        None => vec![0],
        Some(bytes) => {
            let mut body = vec![1];
            push_bytes(&mut body, &bytes);
            body
        }
    }
}

// --- Error frame ---

pub fn encode_error_body(code: &str, message: &str) -> Vec<u8> {
    let mut body = Vec::new();
    push_bytes(&mut body, code.as_bytes());
    push_bytes(&mut body, message.as_bytes());
    body
}

pub fn error_frame(request_id: u32, code: &str, message: impl Into<String>) -> Frame {
    Frame::new(MessageType::Error, request_id, encode_error_body(code, &message.into()))
}

// --- Authenticate ---

/// Encode a bootstrap token into the Authenticate request body.
pub fn encode_authenticate_body(token: &str) -> Vec<u8> {
    let mut body = Vec::new();
    push_bytes(&mut body, token.as_bytes());
    body
}

/// Decode an Authenticate request body into the raw token string.
pub fn decode_authenticate_body(body: &[u8]) -> Result<String, String> {
    let mut offset = 0;
    let token_len = read_u32(body, &mut offset)?;
    let token = read_bytes(body, &mut offset, token_len)?;
    std::str::from_utf8(token).map(|s| s.to_owned()).map_err(|_| "auth token is not utf-8".into())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frame_roundtrip_accepts_both_magics() {
        let original = Frame::new(MessageType::Hello, 42, Vec::new());
        let yydb_bytes = original.encode_as(MAGIC_YYDB).unwrap();
        let yyds_bytes = original.encode_as(MAGIC_YYDS).unwrap();

        let yydb_decoded = decode_frame(&yydb_bytes).unwrap();
        let yyds_decoded = decode_frame(&yyds_bytes).unwrap();
        assert_eq!(yydb_decoded.request_id, 42);
        assert_eq!(yyds_decoded.request_id, 42);
        assert_eq!(yydb_decoded.msg_type, MessageType::Hello);
    }

    #[test]
    fn frame_rejects_unknown_magic() {
        let mut bytes = [0_u8; HEADER_LEN];
        bytes[0..4].copy_from_slice(b"XXXX");
        let result = decode_frame(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn frame_rejects_bad_version() {
        let mut bytes = [0_u8; HEADER_LEN];
        bytes[0..4].copy_from_slice(MAGIC_YYDB);
        bytes[4..8].copy_from_slice(b"9999");
        let result = decode_frame(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn kv_put_body_roundtrip() {
        let (key, value) = decode_kv_put(&[3, 0, 0, 0, b'f', b'o', b'o', 5, 0, 0, 0, b'h', b'e', b'l', b'l', b'o']).unwrap();
        assert_eq!(key, "foo");
        assert_eq!(value, b"hello");
    }

    #[test]
    fn schema_ok_roundtrip() {
        let body = encode_schema_ok(Some((7, "table Foo { bar: Text }")));
        let mut offset = 1;
        assert_eq!(read_u32(&body, &mut offset).unwrap(), 7);
        let doc_len = read_u32(&body, &mut offset).unwrap() as usize;
        assert_eq!(&body[offset..offset + doc_len], b"table Foo { bar: Text }");
    }

    #[test]
    fn schema_ok_none_flag() {
        let body = encode_schema_ok(None);
        assert_eq!(body[0], 0);
        assert_eq!(body.len(), 1);
    }

    #[test]
    fn message_type_roundtrip() {
        for code in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 255] {
            let mt = MessageType::from_u16(code);
            assert_eq!(mt.as_u16(), code);
        }
        let unknown = MessageType::from_u16(999);
        assert_eq!(unknown, MessageType::Unknown(999));
        assert_eq!(unknown.as_u16(), 999);
    }
}
