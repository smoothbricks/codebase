//! Workspace listeners an install carries across a gateway restart (05_gateway.md "An install
//! never refuses a client connection").
//!
//! A restart closes every workspace listener, and a stock client such as Nx reads the refused
//! connection that follows as fatal; it cannot wait for the restart, so the listening sockets must
//! outlive it. The install serves [`HandoffServer`] beside the control socket while it restarts the
//! gateway. The old gateway, as its drain begins, [`give`]s it a duplicate of every TCP workspace
//! listener; the new gateway [`take`]s them before it binds anything. Between the two the
//! install's copies keep each socket listening, so the kernel queues a client's connection instead
//! of refusing it, and the new gateway serves it once the session is restored.
//!
//! Every failure falls back to what a restart did before: no install serving the socket, an old
//! gateway that gives nothing, or a failed take all leave the new gateway binding fresh, and the
//! install closes whatever it holds before a failed taker binds.
//!
//! Frames are [`shell_host`](cowshed_core::runtime::shell_host)'s: a 4-byte little-endian length
//! and a JSON [`Frame`], one listener's descriptor riding each `listener` frame.

use std::io::{self, Write as _};
use std::net::{SocketAddr, TcpListener};
use std::os::fd::{AsRawFd as _, BorrowedFd, OwnedFd};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use cowshed_core::runtime::shell_host::{read_frame, send_with_descriptors};
use serde::{Deserialize, Serialize};

/// The handoff socket beside the gateway's control socket, under the store the sandbox denies.
pub fn handoff_path(control_socket: &Path) -> PathBuf {
    control_socket.with_file_name("listener-handoff.sock")
}

/// Guards only against a hung peer: every frame is a few bytes the peer has already decided to
/// send, so a healthy exchange takes microseconds.
const HANDOFF_BOUND: Duration = Duration::from_secs(30);

#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "frame", rename_all = "kebab-case")]
enum Frame {
    /// A draining gateway gives `listeners` listener frames.
    Give { listeners: usize },
    /// A starting gateway asks for every listener held.
    Take,
    /// One listener, its descriptor attached.
    Listener { address: SocketAddr },
    /// No more listener frames.
    End,
    /// The receiver holds every descriptor it was sent.
    Ok,
}

fn send(stream: &UnixStream, frame: &Frame, descriptor: Option<BorrowedFd<'_>>) -> io::Result<()> {
    let payload = serde_json::to_vec(frame).map_err(io::Error::other)?;
    let length = u32::try_from(payload.len()).map_err(io::Error::other)?;
    let mut bytes = length.to_le_bytes().to_vec();
    bytes.extend_from_slice(&payload);
    let descriptors: Vec<_> = descriptor.iter().map(|fd| fd.as_raw_fd()).collect();
    let sent = send_with_descriptors(stream.as_raw_fd(), &bytes, &descriptors)?;
    (&*stream).write_all(&bytes[sent..])
}

/// Wait, holding no lock, until `fd` is readable: a frame arrived, a connection is queued, or
/// the peer closed. `None` waits for as long as it takes; `Some` fails past the bound.
fn readable(fd: std::os::fd::RawFd, bound: Option<Duration>) -> io::Result<()> {
    let mut readable = libc::pollfd {
        fd,
        events: libc::POLLIN,
        revents: 0,
    };
    let bound = match bound {
        Some(bound) => i32::try_from(bound.as_millis()).map_err(io::Error::other)?,
        None => -1,
    };
    loop {
        // SAFETY: one valid pollfd, live for the call.
        match unsafe { libc::poll(&mut readable, 1, bound) } {
            0 => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "the listener handoff peer sent nothing",
                ));
            }
            answered if answered > 0 => return Ok(()),
            _ => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
}

/// The next frame and the descriptors that rode it. The wait for it holds no lock; the receive
/// itself runs under the fork lock, because a received descriptor is made close-on-exec in a
/// second step, and a child forked between the two would hold a listening socket for its life.
fn receive(stream: &UnixStream) -> io::Result<(Frame, Vec<OwnedFd>)> {
    readable(stream.as_raw_fd(), Some(HANDOFF_BOUND))?;
    let (payload, descriptors) =
        cowshed_os::fork_lock::create(|| read_frame(stream))?.ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the listener handoff peer closed",
            )
        })?;
    let frame = serde_json::from_slice(&payload).map_err(io::Error::other)?;
    Ok((frame, descriptors))
}

/// The one descriptor a `listener` frame carries.
fn one(mut descriptors: Vec<OwnedFd>) -> io::Result<OwnedFd> {
    match (descriptors.pop(), descriptors.is_empty()) {
        (Some(descriptor), true) => Ok(descriptor),
        _ => Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "a listener frame carries exactly one descriptor",
        )),
    }
}

/// Connect to the handoff socket, or `None` when no install serves it: no socket file, or one a
/// crashed install left that nothing listens on.
fn connect(path: &Path) -> io::Result<Option<UnixStream>> {
    let stream = match cowshed_os::fork_lock::create(|| UnixStream::connect(path)) {
        Ok(stream) => stream,
        Err(error)
            if matches!(
                error.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ) =>
        {
            return Ok(None);
        }
        Err(error) => return Err(error),
    };
    stream.set_write_timeout(Some(HANDOFF_BOUND))?;
    Ok(Some(stream))
}

/// Give the install serving `path` a duplicate of each listener, as a draining gateway's first
/// step: how many it took, `0` when no install serves `path`.
pub fn give(path: &Path, listeners: &[(SocketAddr, BorrowedFd<'_>)]) -> io::Result<usize> {
    let Some(stream) = connect(path)? else {
        return Ok(0);
    };
    send(
        &stream,
        &Frame::Give {
            listeners: listeners.len(),
        },
        None,
    )?;
    for (address, descriptor) in listeners {
        send(
            &stream,
            &Frame::Listener { address: *address },
            Some(*descriptor),
        )?;
    }
    match receive(&stream)? {
        (Frame::Ok, _) => Ok(listeners.len()),
        (other, _) => Err(unexpected(&other)),
    }
}

/// Take every listener the install serving `path` holds, as a starting gateway's first step,
/// before it binds anything. Empty when no install serves `path`. On a failure after connecting,
/// this waits until the install has closed its copies, so the caller's own bind cannot meet them.
pub fn take(path: &Path) -> io::Result<Vec<(SocketAddr, TcpListener)>> {
    let Some(stream) = connect(path)? else {
        return Ok(Vec::new());
    };
    let taken = take_on(&stream);
    if taken.is_err() {
        // The install closes what it holds and then this connection: its EOF says the ports are
        // free. The read's own bound guards only a hung install.
        let _ = stream.shutdown(std::net::Shutdown::Write);
        let _ = stream.set_read_timeout(Some(HANDOFF_BOUND));
        let _ = io::Read::read_to_end(&mut &stream, &mut Vec::new());
    }
    taken
}

fn take_on(stream: &UnixStream) -> io::Result<Vec<(SocketAddr, TcpListener)>> {
    send(stream, &Frame::Take, None)?;
    let mut taken = Vec::new();
    loop {
        match receive(stream)? {
            (Frame::Listener { address }, descriptors) => {
                taken.push((address, TcpListener::from(one(descriptors)?)));
            }
            (Frame::End, _) => break,
            (other, _) => return Err(unexpected(&other)),
        }
    }
    send(stream, &Frame::Ok, None)?;
    Ok(taken)
}

fn unexpected(frame: &Frame) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("unexpected listener handoff frame {frame:?}"),
    )
}

/// The install's side: serves the handoff socket from a thread of its own, so it answers while
/// the install blocks on `launchctl`. Dropping it stops serving, removes the socket and closes
/// every listener nobody took.
pub struct HandoffServer {
    path: PathBuf,
    stop: Arc<AtomicBool>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl HandoffServer {
    /// Serve `path`, replacing a socket a crashed install left there.
    pub fn start(path: &Path) -> io::Result<Self> {
        match std::fs::symlink_metadata(path) {
            Ok(metadata) => {
                use std::os::unix::fs::FileTypeExt as _;
                if !metadata.file_type().is_socket() {
                    return Err(io::Error::new(
                        io::ErrorKind::AlreadyExists,
                        format!("{} exists and is not a socket", path.display()),
                    ));
                }
                std::fs::remove_file(path)?;
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(error),
        }
        let listener = cowshed_os::fork_lock::create(|| UnixListener::bind(path))?;
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        let stop = Arc::new(AtomicBool::new(false));
        let thread = std::thread::Builder::new()
            .name("listener-handoff".to_owned())
            .spawn({
                let stop = Arc::clone(&stop);
                move || serve(&listener, &stop)
            })?;
        Ok(Self {
            path: path.to_path_buf(),
            stop,
            thread: Some(thread),
        })
    }
}

impl Drop for HandoffServer {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::SeqCst);
        // Wakes the accept, which then sees `stop`.
        let _ = UnixStream::connect(&self.path);
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

fn serve(listener: &UnixListener, stop: &AtomicBool) {
    // The listeners held since the last give; dropped (closed) once taken or when serving ends.
    let mut held: Vec<(SocketAddr, OwnedFd)> = Vec::new();
    loop {
        // The wait holds no lock: a spawn meanwhile must not wait for a gateway to give.
        let accepted = readable(listener.as_raw_fd(), None)
            .and_then(|()| cowshed_os::fork_lock::create(|| listener.accept()));
        let stream = match accepted {
            Ok((stream, _)) => stream,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
            Err(error) => {
                eprintln!("cowshed: the listener handoff stopped accepting: {error}");
                return;
            }
        };
        if stop.load(Ordering::SeqCst) {
            return;
        }
        if !same_user(&stream) {
            continue;
        }
        if let Err(error) = exchange(&stream, &mut held) {
            eprintln!("cowshed: a listener handoff exchange failed: {error}");
        }
    }
}

fn exchange(stream: &UnixStream, held: &mut Vec<(SocketAddr, OwnedFd)>) -> io::Result<()> {
    stream.set_write_timeout(Some(HANDOFF_BOUND))?;
    match receive(stream)? {
        (Frame::Give { listeners }, _) => {
            let mut given = Vec::with_capacity(listeners);
            for _ in 0..listeners {
                match receive(stream)? {
                    (Frame::Listener { address }, descriptors) => {
                        given.push((address, one(descriptors)?));
                    }
                    (other, _) => return Err(unexpected(&other)),
                }
            }
            *held = given;
            eprintln!(
                "cowshed: holding {} workspace listener(s) across the gateway restart",
                held.len()
            );
            send(stream, &Frame::Ok, None)
        }
        (Frame::Take, _) => {
            let sent = held.iter().try_for_each(|(address, descriptor)| {
                use std::os::fd::AsFd as _;
                send(
                    stream,
                    &Frame::Listener { address: *address },
                    Some(descriptor.as_fd()),
                )
            });
            let answered = sent
                .and_then(|()| send(stream, &Frame::End, None))
                .and_then(|()| receive(stream));
            // Taken or not, these copies close now: a taker holds its own, and one that failed
            // binds fresh once this connection's EOF says these are gone.
            held.clear();
            match answered? {
                (Frame::Ok, _) => Ok(()),
                (other, _) => Err(unexpected(&other)),
            }
        }
        (other, _) => Err(unexpected(&other)),
    }
}

/// Only this user's processes may give or take a workspace listener; the socket's mode and the
/// sandbox's store deny already say so, and the peer's credential checks it.
fn same_user(stream: &UnixStream) -> bool {
    let mut uid = 0;
    let mut gid = 0;
    // SAFETY: a connected socket and two writable out-parameters.
    let read = unsafe { libc::getpeereid(stream.as_raw_fd(), &mut uid, &mut gid) };
    // SAFETY: `geteuid` takes no arguments and cannot fail.
    read == 0 && uid == unsafe { libc::geteuid() }
}

#[cfg(test)]
mod tests {
    use std::io::Read as _;
    use std::os::fd::AsFd as _;

    use super::*;

    fn socket_path(name: &str) -> PathBuf {
        // `/tmp`, not TMPDIR: a Unix socket path has to fit in `sun_path`.
        PathBuf::from(format!(
            "/tmp/cs-handoff-{name}-{}.sock",
            std::process::id()
        ))
    }

    fn listener() -> (SocketAddr, TcpListener) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        (listener.local_addr().expect("address"), listener)
    }

    /// The whole point: a connection made while neither gateway holds the listener is queued,
    /// not refused, and the taker accepts it.
    #[test]
    fn a_listener_given_and_taken_queues_the_connections_made_between() {
        let path = socket_path("round-trip");
        let server = HandoffServer::start(&path).expect("serve");
        let (address, old) = listener();
        assert_eq!(give(&path, &[(address, old.as_fd())]).expect("give"), 1);
        drop(old);

        let mut queued = std::net::TcpStream::connect(address)
            .expect("the install's copy keeps the port listening");

        let mut taken = take(&path).expect("take");
        assert_eq!(taken.len(), 1);
        let (taken_address, new) = taken.remove(0);
        assert_eq!(taken_address, address);
        let (mut accepted, _) = new.accept().expect("the queued connection");
        accepted.write_all(b"served").expect("answer");
        drop(accepted);
        let mut answer = String::new();
        queued.read_to_string(&mut answer).expect("read");
        assert_eq!(answer, "served");
        drop(server);
        assert!(!path.exists(), "the socket goes with the server");
    }

    /// With no install serving the socket, a draining gateway gives nothing and a starting one
    /// takes nothing: a restart is exactly what it was without the handoff.
    #[test]
    fn with_no_install_serving_nothing_is_given_or_taken() {
        let path = socket_path("absent");
        let (address, old) = listener();
        assert_eq!(give(&path, &[(address, old.as_fd())]).expect("give"), 0);
        assert!(take(&path).expect("take").is_empty());
        // A socket file a crashed install left, with nothing listening on it.
        drop(cowshed_os::fork_lock::create(|| UnixListener::bind(&path)).expect("bind"));
        assert!(take(&path).expect("take").is_empty());
        std::fs::remove_file(&path).expect("cleanup");
    }

    /// A taker that fails after connecting still finds the ports free once `take` returns: the
    /// install closed its copies before it closed the connection.
    #[test]
    fn after_a_failed_take_the_install_holds_no_listener() {
        let path = socket_path("failed-take");
        let server = HandoffServer::start(&path).expect("serve");
        let (address, old) = listener();
        give(&path, &[(address, old.as_fd())]).expect("give");
        drop(old);

        let stream = connect(&path).expect("connect").expect("served");
        send(&stream, &Frame::Take, None).expect("ask");
        let (frame, descriptors) = receive(&stream).expect("the listener frame");
        assert!(matches!(frame, Frame::Listener { .. }));
        drop(descriptors);
        // A taker that fails here answers nothing and closes its half.
        stream
            .shutdown(std::net::Shutdown::Write)
            .expect("half-close");
        let _ = io::Read::read_to_end(&mut &stream, &mut Vec::new());

        TcpListener::bind(address).expect("the port is free once the install closed its copy");
        drop(server);
    }
}
