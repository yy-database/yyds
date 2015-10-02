//! TCP + WebSocket `/wire` server for the YY native protocol.
//!
//! Loopback-first (mirrors `yydb-server` — no ACL). The listener accepts
//! plain TCP connections and WebSocket upgrades to `/wire`, dispatching each
//! frame through [`dispatch`].

use std::{
    io::{self, Read, Write},
    net::{SocketAddr, TcpListener, TcpStream},
    sync::Arc,
    thread,
    time::Duration,
};

use sha1::{Digest, Sha1};
use base64::Engine;

use crate::wire::{decode_frame, read_frame, write_frame};
use crate::wire_dispatch::{dispatch, WireContext};

const WS_GUID: &str = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

/// Owned listener with a join handle; dropped handles stop on the next accept.
pub struct WireServer {
    listener: Arc<TcpListener>,
    address: SocketAddr,
    join: Option<thread::JoinHandle<()>>,
}

impl WireServer {
    /// Bind to a loopback address and start accepting connections.
    ///
    /// The server runs on a background thread; call [`stop`] (or drop) to
    /// halt. Returns the bound address (useful when the OS picks port `0`).
    pub fn start(bind: SocketAddr, ctx: WireContext) -> Result<Self, String> {
        if !bind.ip().is_loopback() {
            return Err(format!(
                "wire listener refuses non-loopback bind `{bind}` — YYDS wire has no ACL; \
                 use 127.0.0.1 / ::1"
            ));
        }
        let listener = TcpListener::bind(bind).map_err(|e| format!("wire bind: {e}"))?;
        let address = listener.local_addr().map_err(|e| format!("wire local_addr: {e}"))?;
        println!("yyds wire listening on {address} (TCP + ws://{address}/wire; no ACL)");
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
            catalog: std::sync::Mutex::new(
                self.catalog.lock().unwrap().as_ref().map(|inner| Arc::clone(inner)),
            ),
        }
    }
}

fn handle_client(stream: TcpStream, ctx: &WireContext) -> Result<(), String> {
    let mut peek = [0_u8; 4];
    let n = stream.peek(&mut peek).map_err(|e| e.to_string())?;
    if n >= 3 && &peek[..3] == b"GET" {
        handle_websocket(stream, ctx)
    } else {
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
    let path_ok = request
        .lines()
        .next()
        .map(|line| line.contains(" /wire"))
        .unwrap_or(false);
    if !path_ok {
        let body = b"use path /wire for YYDS binary websocket\n";
        let resp = format!(
            "HTTP/1.1 404 Not Found\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
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
            } else {
                None
            }
        })
        .ok_or("missing Sec-WebSocket-Key".to_string())?;

    let mut hasher = Sha1::new();
    hasher.update(key.as_bytes());
    hasher.update(WS_GUID.as_bytes());
    let accept = base64::engine::general_purpose::STANDARD
        .encode(hasher.finalize());

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
    } else if len == 127 {
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
        0x8 => Ok(None),                                      // close
        0x9 => {                                              // ping → pong
            write_ws_frame(stream, 0xA, &payload)?;
            read_ws_binary(stream)
        }
        0x2 | 0x1 => Ok(Some(payload)),                       // binary | text
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
    } else if len <= u16::MAX as usize {
        header.push(126);
        header.extend_from_slice(&(len as u16).to_be_bytes());
    } else {
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

// Silence the unused-duration warning — we keep Duration imported for future heartbeat work.
#[allow(dead_code)]
fn _dead_code_suppress() {
    let _ = Duration::from_secs(0);
}
