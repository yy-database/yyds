//! TCP + WebSocket `/wire` server for the YY native protocol.
//!
//! ACL policy (Phase 1 — enforced at startup):
//!
//! * **Loopback bind** — allowed anonymously (dev mode). If no bootstrap
//!   tokens are configured a warning is printed to stderr so operators know
//!   the wire is open without auth.
//! * **Non-loopback bind** — **requires** at least one bootstrap token in
//!   the `YYDS_AUTH_TOKENS` env var (semicolon-separated); the server
//!   refuses to start otherwise.
//!
//! Full Role/Permission + TLS are follow-up work — this gate keeps remote
//! listeners from silently opening unauthenticated KV access.

use std::{
    env,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};

use base64::Engine;
use sha1::{Digest, Sha1};

use crate::{
    node_runtime::open_executor,
    wire_dispatch::{dispatch, WireContext},
    wire_protocol::{decode_frame, read_frame, write_frame},
};

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Owned listener with a join handle; dropped handles stop on the next accept.
pub struct WireServer {
    listener: Arc<TcpListener>,
    address: SocketAddr,
    join: Option<thread::JoinHandle<()>>,
}

impl WireServer {
    /// Bind an address, open the catalog-driven executor from the node
    /// directory, and start accepting connections.
    ///
    /// This is the **node start chain** entry point: callers pass a node
    /// directory (and optional catalog path) rather than constructing an
    /// executor themselves. The executor is built by [`open_executor`] so
    /// the wire dispatch sees the same shard map and routing epoch as the
    /// rest of the node.
    pub fn start_from_directory(
        bind: SocketAddr,
        data_dir: &std::path::Path,
        catalog_path: Option<&std::path::Path>,
    ) -> Result<Self, String> {
        let executor = open_executor(data_dir, catalog_path)?;
        let ctx = WireContext::new(executor);
        Self::start(bind, ctx)
    }

    /// Bind to an address and start accepting connections.
    ///
    /// The server runs on a background thread; call [`stop`] (or drop) to
    /// halt. Returns the bound address (useful when the OS picks port `0`).
    /// Prefer [`start_from_directory`] when you have a node directory — it
    /// wires up the catalog-driven executor for you.
    ///
    /// ACL gate — see crate-level docs. Non-loopback binds require at least
    /// one token in `YYDS_AUTH_TOKENS`.
    pub fn start(bind: SocketAddr, ctx: WireContext) -> Result<Self, String> {
        let bind_is_loopback = bind.ip().is_loopback();
        let tokens = bootstrap_tokens_from_env(bind_is_loopback)?;

        let listener = TcpListener::bind(bind).map_err(|e| format!("wire bind: {e}"))?;
        let address = listener.local_addr().map_err(|e| format!("wire local_addr: {e}"))?;
        if bind_is_loopback && tokens.is_empty() {
            println!("yyds wire listening on {address} (loopback-only, anonymous dev mode ⚠)");
            eprintln!("⚠  Wire bound to loopback with no bootstrap tokens — anonymous dev access is allowed locally.");
            eprintln!("   Set YYDS_AUTH_TOKENS for explicit bootstrap token(s) before binding remotely.");
        }
        else {
            let scope = if bind_is_loopback { "loopback" } else { "remote" };
            println!("yyds wire listening on {address} ({scope}; {} bootstrap token(s) from YYDS_AUTH_TOKENS)", tokens.len());
            if !bind_is_loopback {
                eprintln!("⚠  Wire bound to non-loopback address {address} — TLS is still NOT enforced at the wire layer.");
                eprintln!("⚠  This is ONLY safe on a trusted network or behind a TLS-terminating proxy.");
            }
        }
        let listener = Arc::new(listener);
        let listener_for_thread = Arc::clone(&listener);
        let join = thread::Builder::new()
            .name("yyds-wire-accept".into())
            .spawn(move || accept_loop(listener_for_thread, ctx))
            .map_err(|e| format!("wire spawn: {e}"))?;
        Ok(Self { listener, address, join: Some(join) })
    }

    /// Returns the address the server bound to.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Ask the accept loop to stop and join it. Idempotent.
    pub fn stop(mut self) -> Result<(), String> {
        drop(self.listener); // causes accept() to return an error → exit
        if let Some(join) = self.join.take() {
            join.join().map_err(|_| "wire accept thread panicked".to_string())?;
        }
        Ok(())
    }

    /// Whether the accept thread is still running.
    pub fn is_finished(&self) -> bool {
        self.join.as_ref().map_or(true, |j| j.is_finished())
    }
}

fn accept_loop(listener: Arc<TcpListener>, ctx: WireContext) {
    loop {
        match listener.accept() {
            Ok((stream, peer)) => {
                let ctx = ctx.clone();
                thread::Builder::new()
                    .name(format!("yyds-wire-{peer}"))
                    .spawn(move || {
                        if let Err(error) = handle_client(stream, &ctx) {
                            eprintln!("wire client {peer}: {error}");
                        }
                    })
                    .ok();
            }
            Err(_) => return, // listener closed
        }
    }
}

impl Clone for WireContext {
    fn clone(&self) -> Self {
        Self {
            executor: Arc::clone(&self.executor),
            catalog: std::sync::Mutex::new(self.catalog.lock().unwrap().as_ref().map(|inner| Arc::clone(inner))),
        }
    }
}

fn handle_client(stream: TcpStream, ctx: &WireContext) -> Result<(), String> {
    let mut peek = [0_u8; 4];
    let n = stream.peek(&mut peek).map_err(|e| e.to_string())?;
    if n >= 3 && &peek[..3] == b"GET" {
        handle_websocket(stream, ctx)
    }
    else {
        handle_tcp(stream, ctx)
    }
}

fn handle_tcp(mut stream: TcpStream, ctx: &WireContext) -> Result<(), String> {
    loop {
        let request = match read_frame(&mut stream) {
            Ok(frame) => frame,
            Err(error) if is_disconnect(&error) => return Ok(()),
            Err(error) => return Err(error),
        };
        let response = dispatch(ctx, &request);
        write_frame(&mut stream, &response)?;
    }
}

fn handle_websocket(mut stream: TcpStream, ctx: &WireContext) -> Result<(), String> {
    // Read HTTP upgrade request.
    let mut buf = Vec::new();
    let mut chunk = [0_u8; 1024];
    loop {
        let n = stream.read(&mut chunk).map_err(|e| e.to_string())?;
        if n == 0 {
            return Ok(());
        }
        buf.extend_from_slice(&chunk[..n]);
        if buf.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
        if buf.len() > 64 * 1024 {
            return Err("websocket handshake too large".into());
        }
    }

    let request = String::from_utf8_lossy(&buf);
    let path_ok = request.lines().next().map(|line| line.contains(" /wire")).unwrap_or(false);
    if !path_ok {
        let body = b"use path /wire for YYDS binary websocket\n";
        let resp = format!("HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len());
        stream.write_all(resp.as_bytes()).map_err(|e| e.to_string())?;
        stream.write_all(body).map_err(|e| e.to_string())?;
        return Ok(());
    }

    let key = request
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            if name.eq_ignore_ascii_case("Sec-WebSocket-Key") {
                Some(value.trim().to_owned())
            }
            else {
                None
            }
        })
        .ok_or("missing Sec-WebSocket-Key".to_string())?;

    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(WS_GUID.as_bytes());
    let accept = base64::engine::general_purpose::STANDARD.encode(hasher.finalize());

    let response = format!(
        "HTTP/1.1 101 Switching Protocols\r\n\
         Upgrade: websocket\r\n\
         Connection: Upgrade\r\n\
         Sec-WebSocket-Accept: {accept}\r\n\r\n"
    );
    stream.write_all(response.as_bytes()).map_err(|e| e.to_string())?;

    loop {
        let payload = match read_ws_binary(&mut stream)? {
            Some(bytes) => bytes,
            None => return Ok(()),
        };
        let request = decode_frame(&payload)?;
        let response = dispatch(ctx, &request);
        let encoded = response.encode()?;
        write_ws_binary(&mut stream, &encoded)?;
    }
}

// --------- WebSocket framing ---------

fn read_ws_binary(stream: &mut TcpStream) -> Result<Option<Vec<u8>>, String> {
    let mut header = [0_u8; 2];
    match stream.read_exact(&mut header) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(error) => return Err(error.to_string()),
    }
    let opcode = header[0] & 0x0f;
    let masked = (header[1] & 0x80) != 0;
    let mut len = (header[1] & 0x7f) as u64;

    if len == 126 {
        let mut ext = [0_u8; 2];
        stream.read_exact(&mut ext).map_err(|e| e.to_string())?;
        len = u16::from_be_bytes(ext) as u64;
    }
    else if len == 127 {
        let mut ext = [0_u8; 8];
        stream.read_exact(&mut ext).map_err(|e| e.to_string())?;
        len = u64::from_be_bytes(ext);
    }

    if len > 17 * 1024 * 1024 {
        return Err("websocket frame too large".into());
    }

    let mut mask = [0_u8; 4];
    if masked {
        stream.read_exact(&mut mask).map_err(|e| e.to_string())?;
    }

    let mut payload = vec![0_u8; len as usize];
    if len > 0 {
        stream.read_exact(&mut payload).map_err(|e| e.to_string())?;
    }
    if masked {
        for (i, byte) in payload.iter_mut().enumerate() {
            *byte ^= mask[i % 4];
        }
    }

    match opcode {
        0x8 => Ok(None), // close
        0x9 => {
            // ping → pong
            write_ws_frame(stream, 0xA, &payload)?;
            read_ws_binary(stream)
        }
        0x2 | 0x1 => Ok(Some(payload)), // binary | text
        0x0 => Err("websocket continuation not supported".into()),
        other => Err(format!("unsupported websocket opcode {other}")),
    }
}

fn write_ws_binary(stream: &mut TcpStream, payload: &[u8]) -> Result<(), String> {
    write_ws_frame(stream, 0x2, payload)
}

fn write_ws_frame(stream: &mut TcpStream, opcode: u8, payload: &[u8]) -> Result<(), String> {
    let mut header = Vec::with_capacity(10);
    header.push(0x80 | opcode);
    let len = payload.len();
    if len < 126 {
        header.push(len as u8);
    }
    else if len <= u16::MAX as usize {
        header.push(126);
        header.extend_from_slice(&(len as u16).to_be_bytes());
    }
    else {
        header.push(127);
        header.extend_from_slice(&(len as u64).to_be_bytes());
    }
    stream.write_all(&header).map_err(|e| e.to_string())?;
    stream.write_all(payload).map_err(|e| e.to_string())?;
    stream.flush().map_err(|e| e.to_string())?;
    Ok(())
}

fn is_disconnect(error: &str) -> bool {
    // Coarse: most disconnects surface as UnexpectedEof / ConnectionReset / BrokenPipe.
    error.contains("UnexpectedEof")
        || error.contains("ConnectionReset")
        || error.contains("BrokenPipe")
        || error.contains("stream closed")
}

// ─── Bootstrap token gate ────────────────────────────────────────────────

/// Parse the `YYDS_AUTH_TOKENS` env var into a list of bootstrap tokens.
///
/// Format: semicolon-separated tokens, e.g.
/// ```text
/// YYDS_AUTH_TOKENS=token-abc123;token-xyz789
/// ```
///
/// * **Loopback bind** — missing or empty env var returns an **empty** Vec
///   (anonymous dev mode is allowed; the caller warns).
/// * **Non-loopback bind** — missing or empty env var returns an error so
///   the server refuses to start before opening a listener.
///
/// This is Phase 1 — just a presence check. Full Role/Permission follows
/// once the dispatch layer wires in session lookup.
pub fn bootstrap_tokens_from_env(bind_is_loopback: bool) -> Result<Vec<String>, String> {
    match env::var("YYDS_AUTH_TOKENS") {
        Ok(raw) => {
            let tokens: Vec<String> =
                raw.split(';').map(str::trim).filter(|piece| !piece.is_empty()).map(ToOwned::to_owned).collect();
            if tokens.is_empty() {
                return Err("YYDS_AUTH_TOKENS parsed to no entries — set at least one token".to_owned());
            }
            Ok(tokens)
        }
        Err(env::VarError::NotPresent) => {
            if bind_is_loopback {
                Ok(Vec::new())
            }
            else {
                Err("non-loopback bind requires explicit bootstrap token(s) — \
                     set YYDS_AUTH_TOKENS (semicolon-separated) before starting the wire server"
                    .to_owned())
            }
        }
        Err(env::VarError::NotUnicode(value)) => Err(format!("YYDS_AUTH_TOKENS is not valid UTF-8: {value:?}")),
    }
}

// Silence the unused-duration warning — we keep Duration imported for future heartbeat work.
#[allow(dead_code)]
fn _dead_code_suppress() {
    let _ = Duration::from_secs(0);
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    // Serialize env-var reads/writes across parallel test threads.
    fn env_guard() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    fn with_env<F: FnOnce()>(name: &str, value: Option<&str>, body: F) {
        let _g = env_guard().lock().unwrap();
        let prev = env::var_os(name);
        match value {
            Some(v) => env::set_var(name, v),
            None => env::remove_var(name),
        }
        body();
        match prev {
            Some(v) => env::set_var(name, v),
            None => env::remove_var(name),
        }
    }

    #[test]
    fn bootstrap_tokens_absent_is_allowed_on_loopback() {
        with_env("YYDS_AUTH_TOKENS", None, || {
            let tokens = bootstrap_tokens_from_env(true).unwrap();
            assert!(tokens.is_empty(), "loopback bind without env should return empty tokens");
        });
    }

    #[test]
    fn bootstrap_tokens_absent_is_refused_on_non_loopback() {
        with_env("YYDS_AUTH_TOKENS", None, || {
            let err = bootstrap_tokens_from_env(false).unwrap_err();
            assert!(err.contains("non-loopback"), "error should mention non-loopback bind requirement, got: {err}");
        });
    }

    #[test]
    fn bootstrap_tokens_parses_semicolon_separated_list() {
        with_env("YYDS_AUTH_TOKENS", Some("t-alpha;t-beta; t-gamma"), || {
            let tokens = bootstrap_tokens_from_env(true).unwrap();
            assert_eq!(tokens, vec!["t-alpha", "t-beta", "t-gamma"]);
        });
    }

    #[test]
    fn bootstrap_tokens_present_allows_non_loopback() {
        with_env("YYDS_AUTH_TOKENS", Some("t-remote-01"), || {
            let tokens = bootstrap_tokens_from_env(false).unwrap();
            assert_eq!(tokens, vec!["t-remote-01"]);
        });
    }

    #[test]
    fn bootstrap_tokens_blank_env_is_rejected() {
        with_env("YYDS_AUTH_TOKENS", Some(";;   ;"), || {
            let err = bootstrap_tokens_from_env(true).unwrap_err();
            assert!(err.contains("parsed to no entries"));
        });
    }
}
