//! Node-owned lifecycle for the bounded RESP2 protocol probe.

use std::{
    io::{self, Write},
    net::{SocketAddr, TcpListener},
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{self, JoinHandle},
    time::Duration,
};

use yyds_control::NodeLease;

use crate::{connection::serve_cancellable, resp::RequestLimits};

const MAX_CLIENTS: usize = 64;

/// A loopback protocol listener attached to an existing node owner.
/// Dropping the service shuts down all connections and joins its workers.
#[derive(Debug)]
pub struct RedisService {
    address: SocketAddr,
    stopping: Arc<AtomicBool>,
    worker: Option<JoinHandle<io::Result<()>>>,
}

impl RedisService {
    /// Starts a protocol probe without creating a new node or cluster identity.
    pub fn start(owner: Arc<NodeLease>, address: SocketAddr, limits: RequestLimits) -> io::Result<Self> {
        if !address.ip().is_loopback() {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "Redis requires loopback until authentication and TLS are implemented",
            ));
        }
        let listener = TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        let address = listener.local_addr()?;
        let stopping = Arc::new(AtomicBool::new(false));
        let signal = Arc::clone(&stopping);
        let worker = thread::Builder::new().name("yyds-redis-listener".into()).spawn(move || {
            let _owner = owner;
            listen(listener, signal, limits)
        })?;
        Ok(Self { address, stopping, worker: Some(worker) })
    }

    /// Actual bound address, including the assigned port when configured with zero.
    pub fn address(&self) -> SocketAddr {
        self.address
    }

    /// Whether the accept worker has exited, not database or cluster health.
    pub fn is_finished(&self) -> bool {
        self.worker.as_ref().is_none_or(JoinHandle::is_finished)
    }

    /// Interrupts sockets and waits for all owned threads to exit.
    pub fn stop(mut self) -> io::Result<()> {
        self.shutdown()
    }

    fn shutdown(&mut self) -> io::Result<()> {
        self.stopping.store(true, Ordering::Release);
        if let Some(worker) = self.worker.take() {
            worker.join().map_err(|_| io::Error::other("Redis accept worker panicked"))??;
        }
        Ok(())
    }
}

impl Drop for RedisService {
    fn drop(&mut self) {
        let _ = self.shutdown();
    }
}

#[derive(Debug)]
struct Clients {
    workers: Vec<JoinHandle<()>>,
    stopping: Arc<AtomicBool>,
}

impl Clients {
    fn reap(&mut self) {
        let mut index = 0;
        while index < self.workers.len() {
            if self.workers[index].is_finished() {
                let worker = self.workers.swap_remove(index);
                let _ = worker.join();
            }
            else {
                index += 1;
            }
        }
    }
}

impl Drop for Clients {
    fn drop(&mut self) {
        self.stopping.store(true, Ordering::Release);
        for worker in self.workers.drain(..) {
            let _ = worker.join();
        }
    }
}

fn listen(listener: TcpListener, stopping: Arc<AtomicBool>, limits: RequestLimits) -> io::Result<()> {
    let mut clients = Clients { workers: Vec::new(), stopping: Arc::clone(&stopping) };
    while !stopping.load(Ordering::Acquire) {
        clients.reap();
        match listener.accept() {
            Ok((mut stream, _)) => {
                if stopping.load(Ordering::Acquire) {
                    break;
                }
                stream.set_nonblocking(false)?;
                if clients.workers.len() >= MAX_CLIENTS {
                    stream.set_write_timeout(Some(Duration::from_secs(1)))?;
                    let _ = stream.write_all(b"-ERR maximum client count reached\r\n");
                    continue;
                }
                let cancellation = Arc::clone(&stopping);
                let worker = thread::Builder::new().name("yyds-redis-client".into()).spawn(move || {
                    let _ = serve_cancellable(stream, limits, cancellation);
                })?;
                clients.workers.push(worker);
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => thread::sleep(Duration::from_millis(5)),
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}
