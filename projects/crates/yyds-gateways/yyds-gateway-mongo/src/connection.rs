//! Per-connection OP_MSG loop.
//!
//! First step: read a frame, call `crate::handle_command` with the command
//! name extracted from the OP_MSG body, and write the reply. BSON doc
//! extraction lands once the wire skeleton is stable.

use std::{
    io::{self},
    net::TcpStream,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
};

use yyds_execution::KeyValueExecutor;

use crate::{
    handle_command,
    wire::{read_frame, write_frame, FrameLimits},
};

/// Runs the OP_MSG request/reply loop on a single TCP stream.
///
/// `cancellation` is polled after every frame so a listener shutdown can
/// interrupt idle connections.
pub fn serve_cancellable(stream: TcpStream, limits: FrameLimits, cancellation: Arc<AtomicBool>) -> io::Result<()> {
    serve_inner(stream, limits, cancellation, None)
}

/// Same as [`serve_cancellable`] but additionally runs command bodies through
/// a YYDS KV executor. Currently unused — lands when the handshake skeleton compiles.
pub fn serve_cancellable_with_executor(
    stream: TcpStream,
    limits: FrameLimits,
    cancellation: Arc<AtomicBool>,
    executor: Arc<dyn KeyValueExecutor>,
) -> io::Result<()> {
    serve_inner(stream, limits, cancellation, Some(executor))
}

fn serve_inner(
    mut stream: TcpStream,
    limits: FrameLimits,
    cancellation: Arc<AtomicBool>,
    _executor: Option<Arc<dyn KeyValueExecutor>>,
) -> io::Result<()> {
    while !cancellation.load(Ordering::Acquire) {
        let frame = match read_frame(&mut stream, limits) {
            Ok(Some(f)) => f,
            Ok(None) => return Ok(()),
            Err(error) => {
                tracing_command_error(error);
                return Ok(());
            }
        };

        let body = match crate::command_name_from_frame(&frame) {
            Some(name) => match handle_command(name) {
                Ok(body) => body,
                Err(_) => crate::generic_error_body(name),
            },
            None => crate::generic_error_body("unknown"),
        };

        if let Err(error) = write_frame(&mut stream, 0, frame.header.request_id, &body) {
            tracing_command_error(error);
            return Ok(());
        }
    }
    Ok(())
}

fn tracing_command_error(_error: impl std::fmt::Debug) {
    // Placeholder — real logging lands when a tracing subscriber is wired in.
}
