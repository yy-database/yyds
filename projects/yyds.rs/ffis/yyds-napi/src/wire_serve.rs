//! TCP + WebSocket `/wire` server for the YY native protocol.
//!
//! ACL policy (Phase 2 — enforced per-connection):
//!
//! * **Loopback bind** — allowed anonymously (dev mode). If no bootstrap
//!   tokens are configured a warning is printed to stderr so operators know
//!   the wire is open without auth. Each loopback connection starts as
//!   *already authenticated* (anonymous dev mode).
//! * **Non-loopback bind** — **requires** at least one bootstrap token in
//!   the `YYDS_AUTH_TOKENS` env var (semicolon-separated); the server
//!   refuses to start otherwise. Each connection starts *unauthenticated*
//!   and must send a valid Authenticate frame before any KV/Schema command
//!   is dispatched.
//!
//! Full Role/Permission + TLS are follow-up work — this gate keeps remote
//! listeners from silently opening unauthenticated KV access.

use std::{
    env,
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    thread,
    time::Duration,
};

use base64::Engine;
use sha1::{Digest, Sha1};

use crate::{
    node_runtime::open_executor,
    wire_dispatch::{dispatch, WireContext},
    wire_protocol::{decode_authenticate_body, decode_frame, error_frame, read_frame, write_frame, Frame, MessageType},
};

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
/// Error code returned when a connection issues commands before authenticating.
const AUTH_CODE: &str = "yyds.auth.required";
/// Error code returned when an Authenticate frame carries a wrong token.
const AUTH_FAILED_CODE: &str = "yyds.auth.failed";

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
        let bind_is_loopback = bind.ip().is_loopback();
        let tokens = bootstrap_tokens_from_env(bind_is_loopback)?;
        let executor = open_executor(data_dir, catalog_path)?;
        let ctx = WireContext::new(executor).with_bootstrap_tokens(tokens);
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
        let ctx = ctx.with_bootstrap_tokens(tokens.clone());

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
                        if let Err(error) = handle_client(stream, &ctx, peer.ip().is_loopback()) {
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
            bootstrap_tokens: self.bootstrap_tokens.clone(),
        }
    }
}

/// Per-connection authentication state.
///
/// Each connection gets its own `Arc<ConnectionState>` — loopback peers start
/// as authenticated (anonymous dev mode), remote peers must send a valid
/// Authenticate frame before any KV/Schema command is dispatched.
pub struct ConnectionState {
    pub authenticated: AtomicBool,
}

impl ConnectionState {
    pub fn new(initial: bool) -> Self {
        Self { authenticated: AtomicBool::new(initial) }
    }

    pub fn is_authenticated(&self) -> bool {
        self.authenticated.load(Ordering::Acquire)
    }

    pub fn set_authenticated(&self) {
        self.authenticated.store(true, Ordering::Release);
    }
}

/// Frame pre-dispatch gate — enforces per-connection authentication.
///
/// * **Authenticated** — dispatch normally (Hello / Info / Schema / KV all pass).
/// * **Unauthenticated** — only `Hello` and `Authenticate` frames are handled
///   (Hello returns the version as before; Authenticate validates the token).
///   Everything else → `Error` frame with [`AUTH_CODE`].
pub fn process_frame_with_auth(ctx: &WireContext, state: &ConnectionState, request: &Frame) -> Frame {
    let id = request.request_id;

    // Hello always passes — it's the handshake, pre-authentication.
    if request.msg_type == MessageType::Hello {
        return dispatch(ctx, request);
    }

    // Authenticate frame → validate token → flip state → return AuthenticateOk or Error.
    if request.msg_type == MessageType::Authenticate {
        return handle_authenticate(ctx, state, request);
    }

    // Business frames require authenticated state.
    if !state.is_authenticated() {
        return error_frame(id, AUTH_CODE, "authentication required: send Authenticate(frame 13) with a bootstrap token first");
    }

    dispatch(ctx, request)
}

fn handle_authenticate(ctx: &WireContext, state: &ConnectionState, request: &Frame) -> Frame {
    let id = request.request_id;
    let token = match decode_authenticate_body(&request.body) {
        Ok(t) => t,
        Err(e) => return error_frame(id, "yyds.protocol.invalid", format!("Authenticate body: {e}")),
    };

    if ctx.bootstrap_tokens.contains(&token) {
        state.set_authenticated();
        Frame::new(MessageType::AuthenticateOk, id, Vec::new())
    }
    else {
        error_frame(id, AUTH_FAILED_CODE, "bootstrap token not recognized")
    }
}

fn handle_client(stream: TcpStream, ctx: &WireContext, peer_is_loopback: bool) -> Result<(), String> {
    let state = Arc::new(ConnectionState::new(peer_is_loopback));
    let mut peek = [0_u8; 4];
    let n = stream.peek(&mut peek).map_err(|e| e.to_string())?;
    if n >= 3 && &peek[..3] == b"GET" {
        handle_websocket(stream, ctx, state)
    }
    else {
        handle_tcp(stream, ctx, state)
    }
}

fn handle_tcp(mut stream: TcpStream, ctx: &WireContext, state: Arc<ConnectionState>) -> Result<(), String> {
    loop {
        let request = match read_frame(&mut stream) {
            Ok(frame) => frame,
            Err(error) if is_disconnect(&error) => return Ok(()),
            Err(error) => return Err(error),
        };
        let response = process_frame_with_auth(ctx, &state, &request);
        write_frame(&mut stream, &response)?;
    }
}

fn handle_websocket(mut stream: TcpStream, ctx: &WireContext, state: Arc<ConnectionState>) -> Result<(), String> {
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
        let response = process_frame_with_auth(ctx, &state, &request);
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

    // ─── Authentication state machine tests ────────────────────────────────
    //
    // These don't spin up a real TCP server — they exercise
    // `process_frame_with_auth` + `ConnectionState` directly. The point is
    // to prove the four required flows (loopback anonymous, remote blocked,
    // remote authed, remote bad-token) without socket scaffolding.

    use std::collections::HashMap;
    use yyds_execution::LocalExecutor;
    use yyds_kv::MemoryShard;
    use yyds_types::{ShardEpoch, ShardId, ShardMap};

    fn auth_test_ctx() -> WireContext {
        let routing = ShardMap::new(ShardEpoch(1), vec![ShardId("local-0".into())]).unwrap();
        let shard_id = ShardId("local-0".into());
        let shard = MemoryShard::new();
        let mut shards = HashMap::new();
        shards.insert(shard_id, shard);
        let executor = LocalExecutor::new(routing, shards, Vec::new()).unwrap();
        WireContext::new(Arc::new(executor)).with_bootstrap_tokens(vec!["t-secret".to_owned(), "t-fallback".to_owned()])
    }

    fn kv_get_body(key: &str) -> Vec<u8> {
        let mut body = Vec::new();
        let bytes = key.as_bytes();
        body.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
        body.extend_from_slice(bytes);
        body
    }

    #[test]
    fn auth_loopback_connection_allows_kv_without_authenticate() {
        let ctx = auth_test_ctx();
        let state = ConnectionState::new(true); // loopback default = authenticated
        let request = Frame::new(MessageType::KvGet, 1, kv_get_body("any-key"));
        let response = process_frame_with_auth(&ctx, &state, &request);
        assert_eq!(response.msg_type, MessageType::KvGetOk, "loopback anonymous should pass KvGet");
        assert!(state.is_authenticated());
    }

    #[test]
    fn auth_remote_connection_blocks_kv_before_authenticate() {
        let ctx = auth_test_ctx();
        let state = ConnectionState::new(false); // remote default = unauthenticated
        let request = Frame::new(MessageType::KvGet, 1, kv_get_body("any-key"));
        let response = process_frame_with_auth(&ctx, &state, &request);
        assert_eq!(response.msg_type, MessageType::Error);
        assert!(
            String::from_utf8_lossy(&response.body).contains(AUTH_CODE),
            "error body should mention AUTH_CODE `{AUTH_CODE}`, got: {}",
            String::from_utf8_lossy(&response.body)
        );
    }

    #[test]
    fn auth_remote_connection_allows_hello_before_authenticate() {
        let ctx = auth_test_ctx();
        let state = ConnectionState::new(false);
        let request = Frame::new(MessageType::Hello, 1, Vec::new());
        let response = process_frame_with_auth(&ctx, &state, &request);
        assert_eq!(response.msg_type, MessageType::HelloOk, "Hello must be allowed pre-auth");
    }

    #[test]
    fn auth_remote_connection_accepts_correct_token() {
        let ctx = auth_test_ctx();
        let state = ConnectionState::new(false);
        let body = crate::wire_protocol::encode_authenticate_body("t-secret");
        let request = Frame::new(MessageType::Authenticate, 1, body);
        let response = process_frame_with_auth(&ctx, &state, &request);
        assert_eq!(response.msg_type, MessageType::AuthenticateOk);
        assert!(state.is_authenticated(), "ConnectionState must flip to authenticated on success");

        // Second command should now pass.
        let kv = Frame::new(MessageType::KvGet, 2, kv_get_body("k"));
        let kv_resp = process_frame_with_auth(&ctx, &state, &kv);
        assert_eq!(kv_resp.msg_type, MessageType::KvGetOk);
    }

    #[test]
    fn auth_remote_connection_rejects_wrong_token() {
        let ctx = auth_test_ctx();
        let state = ConnectionState::new(false);
        let body = crate::wire_protocol::encode_authenticate_body("wrong-token");
        let request = Frame::new(MessageType::Authenticate, 1, body);
        let response = process_frame_with_auth(&ctx, &state, &request);
        assert_eq!(response.msg_type, MessageType::Error);
        assert!(
            String::from_utf8_lossy(&response.body).contains(AUTH_FAILED_CODE),
            "error body should mention AUTH_FAILED_CODE `{AUTH_FAILED_CODE}`, got: {}",
            String::from_utf8_lossy(&response.body)
        );
        assert!(!state.is_authenticated(), "state must remain unauthenticated after bad token");

        // Business frames still blocked.
        let kv = Frame::new(MessageType::KvGet, 2, kv_get_body("k"));
        let kv_resp = process_frame_with_auth(&ctx, &state, &kv);
        assert_eq!(kv_resp.msg_type, MessageType::Error);
    }
}
