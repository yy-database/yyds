//! MongoDB wire disguise gateway for YYDS.
//!
//! Accepts the MongoDB OP_MSG binary frame protocol and answers with
//! YYDS-backed document responses. This is not a MongoDB server implementation;
//! it presents a bounded document surface on top of YYDS KV execution.

use yyds_gateway::{version_label, GatewayConfig, GatewayKind};
use yyds_types::{Error, Result};

pub mod connection;
pub mod service;
pub mod wire;

use wire::Frame;

/// Gateway kind implemented by this crate.
pub const GATEWAY_KIND: GatewayKind = GatewayKind::Mongo;

/// Gateway identifier used in logs and diagnostics.
pub const GATEWAY_ID: &str = GATEWAY_KIND.id();

/// Default MongoDB disguise listen port.
pub const DEFAULT_PORT: u16 = GATEWAY_KIND.default_port();

/// Handles a MongoDB-style command and returns a minimal disguise response body.
///
/// First step: implement only `hello`/`ping` handshake replies so an official
/// MongoDB driver can establish a connection and inspect wire version.
pub fn handle_command(command_name: &str) -> Result<Vec<u8>> {
    match command_name {
        "hello" | "isMaster" => Ok(build_hello_bson()),
        "ping" => Ok(build_ok_bson(1)),
        other if other.is_empty() => Err(Error::Unsupported("empty mongo command")),
        _other => Err(Error::Unsupported("unsupported mongo command")),
    }
}

fn build_hello_bson() -> Vec<u8> {
    let mut doc = Vec::new();
    doc.extend_from_slice(&0_u32.to_le_bytes()); // placeholder
    // isWritablePrimary: bool true
    doc.push(0x08); doc.extend_from_slice(b"isWritablePrimary\x00"); doc.push(1);
    // maxWireVersion: int32 17
    doc.push(0x10); doc.extend_from_slice(b"maxWireVersion\x00"); doc.extend_from_slice(&17_i32.to_le_bytes());
    // minWireVersion: int32 0
    doc.push(0x10); doc.extend_from_slice(b"minWireVersion\x00"); doc.extend_from_slice(&0_i32.to_le_bytes());
    // ok: double 1.0
    doc.push(0x01); doc.extend_from_slice(b"ok\x00"); doc.extend_from_slice(&1.0_f64.to_le_bytes());
    // terminator
    doc.push(0);
    let len = doc.len() as u32;
    doc[0..4].copy_from_slice(&len.to_le_bytes());
    doc
}

fn build_ok_bson(ok: i32) -> Vec<u8> {
    let mut doc = Vec::new();
    doc.extend_from_slice(&0_u32.to_le_bytes()); // placeholder
    // ok: double (Mongo 惯例用 double)
    doc.push(0x01); doc.extend_from_slice(b"ok\x00");
    let v = if ok >= 1 { 1.0_f64 } else { 0.0_f64 };
    doc.extend_from_slice(&v.to_le_bytes());
    doc.push(0);
    let len = doc.len() as u32;
    doc[0..4].copy_from_slice(&len.to_le_bytes());
    doc
}

/// Banner shown when a disguise server starts.
pub fn startup_banner(port: u16) -> String {
    yyds_gateway::startup_banner(&GatewayConfig::new(GATEWAY_KIND).with_port(port))
}

/// Extracts the command name from an inbound OP_MSG frame.
///
/// MongoDB OP_MSG body layout:
///   [flags u32] [kind byte=0x00] [BSON document] [optional checksum u32]
/// The first element in that BSON document has the command name as its key
/// (e.g. `{ "hello": 1, ... }`). We read just enough BSON to pull that key.
pub fn command_name_from_frame(frame: &Frame) -> Option<&'static str> {
    // Skip OP_MSG flags (4) + section kind (1)
    let mut cursor = frame.body.get(5..)?;
    // Skip optional checksum hint — only matters once we fully validate the body.
    let _skip_checksum = frame.header.total_length as usize > frame.body.len();

    // Read BSON document length
    let doc_len = u32::from_le_bytes(cursor.get(..4)?.try_into().ok()?) as usize;
    cursor = cursor.get(4..)?;
    // doc_len includes the 4-byte length field itself which we just skipped.
    if doc_len < 5 || doc_len.saturating_sub(4) > cursor.len() {
        return None;
    }
    // Skip the first element's type byte (0x10 = int32, 0x01 = double, etc.)
    if cursor.is_empty() {
        return None;
    }
    cursor = &cursor[1..];
    // Read the cstring key until null terminator
    let end = cursor.iter().position(|&b| b == 0)?;
    let name = std::str::from_utf8(&cursor[..end]).ok()?;
    // Canonicalize common aliases we expect from real drivers.
    match name {
        "hello" => Some("hello"),
        "isMaster" | "ismaster" => Some("isMaster"),
        "ping" => Some("ping"),
        other => {
            // Avoid allocating: we only need to route well-known commands here.
            let _ = other;
            // String-match for later expansion once a full BSON parser lands.
            Some("unknown")
        }
    }
}

/// A minimal BSON error document returned for commands we deliberately reject.
pub fn generic_error_body(_command: &str) -> Vec<u8> {
    // { ok: 0, errmsg: "unsupported" } — 29 bytes BSON envelope.
    let mut buf = Vec::with_capacity(29);
    // doc length placeholder
    buf.extend_from_slice(&0_u32.to_le_bytes());
    // ok: int32 0
    buf.push(0x10);
    buf.extend_from_slice(b"ok\x00");
    buf.extend_from_slice(&0_i32.to_le_bytes());
    // errmsg: string "unsupported"
    buf.push(0x02);
    buf.extend_from_slice(b"errmsg\x00");
    let msg = b"unsupported";
    buf.extend_from_slice(&(msg.len() as i32 + 1).to_le_bytes());
    buf.extend_from_slice(msg);
    buf.push(0);
    // doc terminator
    buf.push(0);
    // Fix doc length
    let len = buf.len() as u32;
    buf[0..4].copy_from_slice(&len.to_le_bytes());
    buf
}
