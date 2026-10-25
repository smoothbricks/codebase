//! Bounded waits on another process (specs/cowshed/05_gateway.md, "Deadlines").
//!
//! The decisions (a budget's remaining time, a holder's silence, what an expiry names) are
//! `lmao_core::deadline`'s, made over the injected clock and tested there. This module is the
//! shell. It maps a connection's reads and writes and the tokio timer onto those decisions, and
//! reports the outcome as a [`WaitError`]. A [`PeerStream`] always carries a bound: either one
//! request's [`Deadline`] or a held connection's [`Idle`] silence. So every read, write, flush and
//! shutdown through it ends by its bound, and a wait on a peer without one cannot be written.

use std::fmt;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::ExitStatus;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::task::{Context, Poll, ready};
use std::time::Duration;

pub use lmao_core::deadline::{
    Bound, Deadline, Expired, Idle, LatencyClass, Progress, RetryClass, Silent,
};
use lmao_core::{BlockedReason, Clock};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::UnixStream;
use tokio::time::Sleep;

use crate::process::{ExitWatch, RunState};

/// One request and its one answer from a running cowshed daemon over a local socket.
pub const CONTROL: Bound = Bound::new(LatencyClass::Control, Duration::from_secs(5));
/// Connecting to a daemon that should already be running, and its hello.
pub const HANDSHAKE: Bound = Bound::new(LatencyClass::Handshake, Duration::from_secs(10));
/// A child's exit once its pipes closed or it was killed.
pub const REAP: Bound = Bound::new(LatencyClass::Reap, Duration::from_secs(5));
/// How often a holder says it is still there on a connection it holds open for a long wait.
pub const HEARTBEAT: Duration = Duration::from_secs(5);
/// The resolution of the timers that wake a wait.
const TIMER_TICK: Duration = Duration::from_millis(1);

/// The silence a long wait allows its holder: three missed heartbeats.
pub const HEARTBEAT_SILENCE: Duration = Duration::from_secs(15);

/// The clock the timers that wake a wait run on: tokio's, so a test that pauses tokio's time
/// measures its deadlines in that same paused time.
struct TokioClock {
    origin: tokio::time::Instant,
}

impl Clock for TokioClock {
    fn wall_nanos(&self) -> i64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |since| {
                i64::try_from(since.as_nanos()).unwrap_or(i64::MAX)
            })
    }

    fn monotonic_nanos(&self) -> u64 {
        u64::try_from(self.origin.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }
}

static CLOCK: LazyLock<TokioClock> = LazyLock::new(|| TokioClock {
    origin: tokio::time::Instant::now(),
});

/// The monotonic clock every deadline in a cowshed process is measured on.
pub fn clock() -> &'static dyn Clock {
    &*CLOCK
}

/// Start `op`'s budget now.
pub fn start(op: &'static str, bound: Bound) -> Deadline {
    Deadline::start(op, bound, clock())
}

/// The interval a holder heartbeats on: first due one [`HEARTBEAT`] from now, and a beat that
/// comes late delays the next rather than bunching.
pub fn beats() -> tokio::time::Interval {
    let mut beats = tokio::time::interval_at(tokio::time::Instant::now() + HEARTBEAT, HEARTBEAT);
    beats.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    beats
}

/// Start measuring the silence of `op`'s holder now.
pub fn idle(op: &'static str, silence: Duration) -> Idle {
    Idle::start(op, silence, clock())
}

/// Where a peer listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerAddress {
    Unix(PathBuf),
    Tcp(SocketAddr),
    /// One end of a socketpair the peer inherited: no address names it, its pid does.
    Pair,
}

/// What the kernel said about the process at the other end.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerPid {
    Known(i32),
    /// The kernel reports no pid for this kind of connection (TCP), or did not for this one.
    NotReported,
    /// Asking the kernel failed; the reason is kept, not the call.
    Unread(String),
}

/// The process a wait is on, named as the reader of an expiry needs it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Peer {
    pub role: &'static str,
    pub address: PeerAddress,
    pub pid: PeerPid,
}

impl Peer {
    pub fn unix(role: &'static str, socket: &Path) -> Self {
        Self {
            role,
            address: PeerAddress::Unix(socket.to_path_buf()),
            pid: PeerPid::NotReported,
        }
    }

    /// The peer with the pid the kernel reports for `connection`.
    pub fn credited(mut self, connection: &UnixStream) -> Self {
        self.pid = match connection.peer_cred() {
            Ok(credential) => credential
                .pid()
                .map_or(PeerPid::NotReported, PeerPid::Known),
            Err(error) => PeerPid::Unread(error.to_string()),
        };
        self
    }

    /// The peer at the other end of a socketpair, the child `pid` that inherited it.
    pub fn paired(role: &'static str, pid: i32) -> Self {
        Self {
            role,
            address: PeerAddress::Pair,
            pid: PeerPid::Known(pid),
        }
    }

    pub fn tcp(role: &'static str, address: SocketAddr) -> Self {
        Self {
            role,
            address: PeerAddress::Tcp(address),
            pid: PeerPid::NotReported,
        }
    }
}

impl fmt::Display for Peer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.address {
            PeerAddress::Unix(socket) => {
                write!(formatter, "{} at {}", self.role, socket.display())?
            }
            PeerAddress::Tcp(address) => write!(formatter, "{} at {address}", self.role)?,
            PeerAddress::Pair => write!(formatter, "{} on a socketpair", self.role)?,
        }
        match &self.pid {
            PeerPid::Known(pid) => write!(formatter, " (pid {pid})"),
            PeerPid::NotReported => Ok(()),
            PeerPid::Unread(reason) => write!(formatter, " (pid unread: {reason})"),
        }
    }
}

/// How a wait on another process failed. Its retry class follows from the variant
/// ([`WaitError::retry_class`]): only an absent peer is `Blocked`; every expiry and loss is
/// terminal, so nothing retries around it.
#[derive(Debug, thiserror::Error)]
pub enum WaitError {
    #[error("{0}")]
    Expired(Box<Expired<Peer>>),
    #[error("{0}")]
    Silent(Box<Silent<Peer>>),
    #[error(
        "PEER_LOST: {op} on {peer}: the peer ended the connection after {elapsed:?}; last progress: {last}"
    )]
    PeerLost {
        op: &'static str,
        peer: Box<Peer>,
        elapsed: Duration,
        last: Progress,
    },
    #[error("BLOCKED: {op}: no {peer} is there to answer: {source}")]
    Absent {
        op: &'static str,
        peer: Box<Peer>,
        #[source]
        source: io::Error,
    },
    #[error("{op} on {peer} failed after {elapsed:?} (last progress: {last}): {source}")]
    Io {
        op: &'static str,
        peer: Box<Peer>,
        elapsed: Duration,
        last: Progress,
        #[source]
        source: io::Error,
    },
}

impl WaitError {
    /// The lmao error code this failure is reported under.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Expired(_) => "TIMEOUT",
            Self::Silent(_) => "SILENT",
            Self::PeerLost { .. } => "PEER_LOST",
            Self::Absent { .. } => "BLOCKED",
            Self::Io { .. } => "IO",
        }
    }

    pub fn retry_class(&self) -> RetryClass {
        match self {
            Self::Absent { peer, .. } => RetryClass::Blocked(BlockedReason::Service {
                name: peer.to_string(),
            }),
            Self::Expired(_) | Self::Silent(_) | Self::PeerLost { .. } | Self::Io { .. } => {
                RetryClass::Terminal
            }
        }
    }

    pub fn peer(&self) -> &Peer {
        match self {
            Self::Expired(expired) => &expired.peer,
            Self::Silent(silent) => &silent.holder,
            Self::PeerLost { peer, .. } | Self::Absent { peer, .. } | Self::Io { peer, .. } => peer,
        }
    }
}

/// What bounds a [`PeerStream`]'s waits.
#[derive(Debug, Clone, Copy)]
enum Bounding {
    /// One request's budget, from before its connect to its answer.
    Request(Deadline),
    /// A held connection: its holder's silence since the last bytes it sent.
    Held(Idle),
}

impl Bounding {
    fn op(&self) -> &'static str {
        match self {
            Self::Request(deadline) => deadline.op(),
            Self::Held(idle) => idle.op(),
        }
    }

    fn remaining(&self) -> Duration {
        match self {
            Self::Request(deadline) => deadline.remaining(clock()),
            Self::Held(idle) => idle.remaining(clock()),
        }
        .unwrap_or(Duration::ZERO)
    }

    fn elapsed(&self) -> Duration {
        match self {
            Self::Request(deadline) => deadline.elapsed(clock()),
            Self::Held(idle) => idle.elapsed(clock()),
        }
    }
}

/// A connection to another process whose every wait ends by its bound.
///
/// It is an `AsyncRead + AsyncWrite`, so the protocol's own framing reads and writes through it
/// unchanged. Each byte read or written is progress. A held connection's silence starts over on
/// every byte its holder sends. A wait that runs past the bound fails with an `io::Error` of kind
/// `TimedOut` carrying the [`WaitError`]. [`PeerStream::fail`] recovers the error, typed, from
/// whatever the framing returned.
pub struct PeerStream<S> {
    io: S,
    peer: Peer,
    bounding: Bounding,
    last: Progress,
    timer: Pin<Box<Sleep>>,
}

impl<S> fmt::Debug for PeerStream<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeerStream")
            .field("peer", &self.peer)
            .field("bounding", &self.bounding)
            .field("last", &self.last)
            .finish_non_exhaustive()
    }
}

impl PeerStream<UnixStream> {
    /// Connect to `role` at `socket` within `deadline`, which then bounds the request on it.
    pub async fn connect(
        role: &'static str,
        socket: &Path,
        deadline: Deadline,
    ) -> Result<Self, WaitError> {
        let peer = Peer::unix(role, socket);
        let io = establish(&deadline, &peer, UnixStream::connect(socket)).await?;
        let peer = peer.credited(&io);
        Ok(Self::connected(io, peer, deadline))
    }
}

/// Run `connect` to `peer` within `deadline`: a peer whose address does not exist is absent, and
/// a connect still pending at the bound has expired with nothing sent. A refused connect is an
/// I/O failure: only the caller can tell whether a socket that refuses still has its holder
/// attached (a full queue) or none (a peer that left).
pub async fn establish<S>(
    deadline: &Deadline,
    peer: &Peer,
    connect: impl Future<Output = io::Result<S>>,
) -> Result<S, WaitError> {
    match within(deadline, peer, Progress::Nothing, connect).await? {
        Ok(io) => Ok(io),
        Err(source) if source.kind() == io::ErrorKind::NotFound => Err(WaitError::Absent {
            op: deadline.op(),
            peer: Box::new(peer.clone()),
            source,
        }),
        Err(source) => Err(WaitError::Io {
            op: deadline.op(),
            peer: Box::new(peer.clone()),
            elapsed: deadline.elapsed(clock()),
            last: Progress::Nothing,
            source,
        }),
    }
}

/// Wait for `work`, which stands for an answer from `peer`, within what is left of `deadline`.
pub async fn within<T>(
    deadline: &Deadline,
    peer: &Peer,
    last: Progress,
    work: impl Future<Output = T>,
) -> Result<T, WaitError> {
    let expired = || WaitError::Expired(Box::new(deadline.expired(peer.clone(), last, clock())));
    let remaining = deadline.remaining(clock()).ok_or_else(expired)?;
    tokio::time::timeout(remaining, work)
        .await
        .map_err(|_| expired())
}

/// A child process a wait is on, named as the reader of an expiry needs it: what it is to the
/// waiter, its pid, and what the kernel showed it doing when the wait gave up.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildPeer {
    pub role: &'static str,
    pub pid: u32,
    pub state: RunState,
}

impl ChildPeer {
    /// `role` at `pid`, in the state the kernel shows it in now.
    pub fn observed(role: &'static str, pid: u32) -> Self {
        let state = libc::pid_t::try_from(pid).map_or_else(
            |error| RunState::Unread(error.to_string()),
            crate::process::run_state,
        );
        Self { role, pid, state }
    }
}

impl fmt::Display for ChildPeer {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{} pid {} ({})", self.role, self.pid, self.state)
    }
}

/// How waiting for a child to exit failed. Terminal either way: nothing waits again, and the
/// child is left as it is.
#[derive(Debug, thiserror::Error)]
pub enum ReapError {
    #[error("{0}")]
    Expired(Box<Expired<ChildPeer>>),
    #[error("{op}: waiting for {role} pid {pid} failed after {elapsed:?}: {source}")]
    Wait {
        op: &'static str,
        role: &'static str,
        pid: u32,
        elapsed: Duration,
        #[source]
        source: io::Error,
    },
}

impl ReapError {
    /// The lmao error code this failure is reported under.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Expired(_) => "TIMEOUT",
            Self::Wait { .. } => "IO",
        }
    }

    pub const fn retry_class(&self) -> RetryClass {
        RetryClass::Terminal
    }
}

/// Wait for `child`, `role` to this process, to exit within what is left of `deadline`: its
/// status, or the expiry naming its pid and the state the kernel shows it in, with `last` the
/// last thing this process did to it (the signal that should have ended it, or nothing). The exit
/// is watched (`ExitWatch`), never polled. Expiry ends the wait, never the child: one stuck in
/// an uninterruptible kernel call stays as it is, named.
pub fn reap(
    role: &'static str,
    child: &mut std::process::Child,
    deadline: &Deadline,
    last: Progress,
) -> Result<ExitStatus, ReapError> {
    let pid = child.id();
    let failed = |source| ReapError::Wait {
        op: deadline.op(),
        role,
        pid,
        elapsed: deadline.elapsed(clock()),
        source,
    };
    let expired = || {
        ReapError::Expired(Box::new(deadline.expired(
            ChildPeer::observed(role, pid),
            last,
            clock(),
        )))
    };
    // A child already collected, or exited and collected now, answers before any watch, so a pid
    // that was freed and reused is never watched.
    if let Some(status) = child.try_wait().map_err(failed)? {
        return Ok(status);
    }
    // No watch is a child that exited since (Darwin refuses to watch a zombie).
    if let Some(watch) = ExitWatch::of(pid).map_err(failed)? {
        loop {
            // A spent budget still looks once: an exited child answers its status.
            match watch.within(deadline.remaining(clock()).unwrap_or_default()) {
                Ok(true) => break,
                Ok(false) => return Err(expired()),
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(failed(error)),
            }
        }
    }
    // WHY unbounded: the child has exited, so its status is collected at once.
    child.wait().map_err(failed)
}

/// Hand `child`, whose wait expired, to a thread that collects its exit whenever it comes and
/// says so, so a child that leaves an uninterruptible kernel call late is reaped, never left a
/// zombie for the life of this process. Nothing waits for that thread.
pub fn collect_later(role: &'static str, mut child: std::process::Child) {
    std::thread::spawn(move || {
        let pid = child.id();
        // WHY unbounded: nothing waits on this thread; it only collects the exit when it comes.
        match child.wait() {
            Ok(status) => {
                crate::eprintln!("cowshed: {role} pid {pid} ended after its wait expired: {status}")
            }
            Err(error) => crate::eprintln!(
                "cowshed: {role} pid {pid} could not be collected after its wait expired: {error}"
            ),
        }
    });
}

/// [`reap`] for a child tokio spawned, whose exit tokio's own driver watches.
pub async fn reap_async(
    role: &'static str,
    child: &mut tokio::process::Child,
    deadline: &Deadline,
    last: Progress,
) -> Result<ExitStatus, ReapError> {
    // A child tokio has already collected keeps its status and has no pid: it answers at once,
    // and no pid of it is ever read again.
    let pid = match (child.try_wait(), child.id()) {
        (Ok(Some(status)), _) => return Ok(status),
        (Ok(None), Some(pid)) => pid,
        (Ok(None), None) => {
            return Err(ReapError::Wait {
                op: deadline.op(),
                role,
                pid: 0,
                elapsed: deadline.elapsed(clock()),
                source: io::Error::other("tokio reports neither an exit status nor a pid"),
            });
        }
        (Err(source), pid) => {
            return Err(ReapError::Wait {
                op: deadline.op(),
                role,
                pid: pid.unwrap_or_default(),
                elapsed: deadline.elapsed(clock()),
                source,
            });
        }
    };
    let failed = |source| ReapError::Wait {
        op: deadline.op(),
        role,
        pid,
        elapsed: deadline.elapsed(clock()),
        source,
    };
    let expired = || {
        ReapError::Expired(Box::new(deadline.expired(
            ChildPeer::observed(role, pid),
            last,
            clock(),
        )))
    };
    // A spent budget still polls the child once before the timer: an exited child answers.
    let remaining = deadline.remaining(clock()).unwrap_or_default();
    match tokio::time::timeout(remaining, child.wait()).await {
        Ok(status) => status.map_err(failed),
        Err(_) => Err(expired()),
    }
}

/// How running a child to its end within a deadline ([`output_within`]) failed.
#[derive(Debug, thiserror::Error)]
pub enum RunError {
    #[error("{op}: {role} could not be started: {source}")]
    Spawn {
        op: &'static str,
        role: &'static str,
        #[source]
        source: io::Error,
    },
    #[error(transparent)]
    Reap(#[from] ReapError),
    #[error(transparent)]
    Output(#[from] OutputError),
}

impl RunError {
    /// The lmao error code this failure is reported under.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Spawn { .. } => "IO",
            Self::Reap(failure) => failure.code(),
            Self::Output(failure) => failure.code(),
        }
    }
}

/// Run `command`, `role` to this process, as [`crate::fork_lock::Run::output_locked`] does (no
/// input, both outputs collected), within `deadline`: from its spawn to its exit and its
/// output's end. Past the bound the run fails naming the child, its pid and its state, and the
/// child is left as it is.
pub fn output_within(
    role: &'static str,
    command: &mut std::process::Command,
    deadline: &Deadline,
) -> Result<std::process::Output, RunError> {
    use crate::fork_lock::Spawn as _;

    let mut child = command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn_locked()
        .map_err(|source| RunError::Spawn {
            op: deadline.op(),
            role,
            source,
        })?;
    let pumps = OutputPumps::start(&mut child);
    let status = reap(role, &mut child, deadline, Progress::Nothing)?;
    let (stdout, stderr) = pumps.join(role, child.id(), deadline)?;
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

/// One of a child's two output streams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputStream {
    Stdout,
    Stderr,
}

impl fmt::Display for OutputStream {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::Stdout => "stdout",
            Self::Stderr => "stderr",
        })
    }
}

/// How collecting an exited child's output failed.
#[derive(Debug, thiserror::Error)]
pub enum OutputError {
    /// The child exited, but a stream was still open at the bound: a process that inherited the
    /// pipe from it, one it started, holds it (code `TIMEOUT`, terminal).
    #[error("{expired}; its {stream} is still open: a process it started holds the pipe")]
    Held {
        stream: OutputStream,
        expired: Box<Expired<ChildPeer>>,
    },
    #[error("{op}: reading the {stream} of {role} pid {pid} failed: {source}")]
    Read {
        op: &'static str,
        role: &'static str,
        pid: u32,
        stream: OutputStream,
        #[source]
        source: io::Error,
    },
}

impl OutputError {
    /// The lmao error code this failure is reported under.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Held { .. } => "TIMEOUT",
            Self::Read { .. } => "IO",
        }
    }
}

/// A child's piped stdout and stderr, each read to its end on a thread of its own while the child
/// runs, as `Command::output` reads them, so a verbose child never wedges on a full pipe. Once
/// nobody will collect the output (the pumps are dropped, a join gave up), each pump drops what
/// it reads and keeps draining to its holder's close: a held pipe costs one blocked thread and no
/// memory, and its holder is never sent `SIGPIPE`.
pub struct OutputPumps {
    started: Vec<OutputStream>,
    done: mpsc::Receiver<(OutputStream, io::Result<Vec<u8>>)>,
    abandoned: Arc<AtomicBool>,
}

impl Drop for OutputPumps {
    fn drop(&mut self) {
        self.abandoned.store(true, Ordering::Release);
    }
}

impl OutputPumps {
    /// Start reading `child`'s piped stdout and stderr; a stream that is not piped reads empty.
    pub fn start(child: &mut std::process::Child) -> Self {
        let (sent, done) = mpsc::channel();
        let abandoned = Arc::new(AtomicBool::new(false));
        let mut started = Vec::with_capacity(2);
        let mut pump = |stream, pipe: Option<Box<dyn std::io::Read + Send>>| {
            let Some(mut pipe) = pipe else { return };
            started.push(stream);
            let sent = sent.clone();
            let abandoned = Arc::clone(&abandoned);
            std::thread::spawn(move || {
                let mut bytes = Vec::new();
                let mut chunk = [0_u8; 64 * 1024];
                let read = loop {
                    match pipe.read(&mut chunk) {
                        Ok(0) => break Ok(bytes),
                        Ok(_) if abandoned.load(Ordering::Acquire) => bytes = Vec::new(),
                        Ok(length) => bytes.extend_from_slice(&chunk[..length]),
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                        Err(error) => break Err(error),
                    }
                };
                // A collector that gave up has already failed naming this stream's holder.
                let _ = sent.send((stream, read));
            });
        };
        pump(
            OutputStream::Stdout,
            child
                .stdout
                .take()
                .map(|pipe| Box::new(pipe) as Box<dyn std::io::Read + Send>),
        );
        pump(
            OutputStream::Stderr,
            child
                .stderr
                .take()
                .map(|pipe| Box::new(pipe) as Box<dyn std::io::Read + Send>),
        );
        Self {
            started,
            done,
            abandoned,
        }
    }

    /// The bytes each stream carried to its end, the child `role` at `pid` having exited and
    /// been collected, within what is left of `deadline`; a spent budget still takes what has
    /// already arrived. A stream still open at the bound is held by a process that inherited the
    /// pipe: the wait ends naming the child and that stream, and the pump drains it, keeping
    /// nothing, until its holder closes it. The collected child's pid is never read again.
    pub fn join(
        self,
        role: &'static str,
        pid: u32,
        deadline: &Deadline,
    ) -> Result<(Vec<u8>, Vec<u8>), OutputError> {
        let mut stdout = Vec::new();
        let mut stderr = Vec::new();
        let mut open = self.started.clone();
        while let Some(&first) = open.first() {
            let (stream, read) = match self
                .done
                .recv_timeout(deadline.remaining(clock()).unwrap_or_default())
            {
                Ok(done) => done,
                Err(mpsc::RecvTimeoutError::Timeout) => {
                    return Err(OutputError::Held {
                        stream: first,
                        expired: Box::new(deadline.expired(
                            ChildPeer {
                                role,
                                pid,
                                state: RunState::Gone,
                            },
                            Progress::State("exited"),
                            clock(),
                        )),
                    });
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => {
                    return Err(OutputError::Read {
                        op: deadline.op(),
                        role,
                        pid,
                        stream: first,
                        source: io::Error::other("its pump ended without an answer"),
                    });
                }
            };
            let bytes = read.map_err(|source| OutputError::Read {
                op: deadline.op(),
                role,
                pid,
                stream,
                source,
            })?;
            match stream {
                OutputStream::Stdout => stdout = bytes,
                OutputStream::Stderr => stderr = bytes,
            }
            open.retain(|pending| *pending != stream);
        }
        Ok((stdout, stderr))
    }
}

impl<S> PeerStream<S> {
    /// `io`, already connected to `peer` within `deadline`, which bounds the request on it.
    pub fn connected(io: S, peer: Peer, deadline: Deadline) -> Self {
        let bounding = Bounding::Request(deadline);
        Self {
            timer: Box::pin(tokio::time::sleep(bounding.remaining())),
            io,
            peer,
            bounding,
            last: Progress::Connected,
        }
    }

    pub fn peer(&self) -> &Peer {
        &self.peer
    }

    /// The connection itself, for what only it can say (its peer's credentials). Reading or
    /// writing through it would bypass the bound.
    pub fn get_ref(&self) -> &S {
        &self.io
    }

    pub fn op(&self) -> &'static str {
        self.bounding.op()
    }

    pub fn last(&self) -> Progress {
        self.last
    }

    /// Bound the next request on this connection by `deadline`.
    pub fn request(&mut self, deadline: Deadline) {
        self.rebound(Bounding::Request(deadline));
    }

    /// Hold the connection for a long wait: its holder must send something at least every
    /// `idle` bound, and may take as long as its work does.
    pub fn hold(&mut self, idle: Idle) {
        self.rebound(Bounding::Held(idle));
    }

    /// Hold a held connection's holder to `idle` of silence from here on; the wait's start and
    /// last progress stay. A request's budget is left as it is.
    pub fn silence(&mut self, idle: Duration) {
        if let Bounding::Held(held) = &mut self.bounding {
            held.set_silence(idle);
            self.rearm();
        }
    }

    fn rebound(&mut self, bounding: Bounding) {
        self.bounding = bounding;
        self.rearm();
    }

    fn rearm(&mut self) {
        let at = tokio::time::Instant::now() + self.bounding.remaining();
        self.timer.as_mut().reset(at);
    }

    fn progressed(&mut self, progress: Progress) {
        self.last = progress;
        if let Bounding::Held(idle) = &mut self.bounding {
            idle.observe(progress, clock());
            self.rearm();
        }
    }

    /// The failure of the wait in progress: the bound it ran past.
    fn expiry(&self) -> WaitError {
        match &self.bounding {
            Bounding::Request(deadline) => WaitError::Expired(Box::new(deadline.expired(
                self.peer.clone(),
                self.last,
                clock(),
            ))),
            Bounding::Held(idle) => {
                WaitError::Silent(Box::new(idle.silent(self.peer.clone(), clock())))
            }
        }
    }

    /// The timer only wakes the wait; the bound decides it. Tokio's timer fires on a millisecond
    /// tick, so it can wake a little before the bound: with more than a tick left the timer is
    /// re-armed for the rest, which also lets time pass before the next decision.
    fn poll_bound<T>(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<T>> {
        loop {
            ready!(self.timer.as_mut().poll(cx));
            if self.bounding.remaining() < TIMER_TICK {
                return Poll::Ready(Err(io::Error::new(io::ErrorKind::TimedOut, self.expiry())));
            }
            self.rearm();
        }
    }

    /// `error`, which an operation through this stream returned, as the typed failure of the
    /// wait: the expiry it carries, the peer ending the connection, or the I/O error itself.
    pub fn fail(&self, error: io::Error) -> WaitError {
        let error = match error.downcast::<WaitError>() {
            Ok(wait) => return wait,
            Err(error) => error,
        };
        match error.kind() {
            io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe => WaitError::PeerLost {
                op: self.bounding.op(),
                peer: Box::new(self.peer.clone()),
                elapsed: self.bounding.elapsed(),
                last: self.last,
            },
            _ => self.io_failure(error),
        }
    }

    /// The peer ended the connection before the answer the caller needed.
    pub fn lost(&self) -> WaitError {
        self.fail(io::Error::from(io::ErrorKind::UnexpectedEof))
    }

    fn io_failure(&self, source: io::Error) -> WaitError {
        WaitError::Io {
            op: self.bounding.op(),
            peer: Box::new(self.peer.clone()),
            elapsed: self.bounding.elapsed(),
            last: self.last,
            source,
        }
    }
}

impl<S: AsyncRead + Unpin> AsyncRead for PeerStream<S> {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        let before = buf.filled().len();
        match Pin::new(&mut this.io).poll_read(cx, buf) {
            Poll::Ready(Ok(())) => {
                let read = buf.filled().len() - before;
                if read != 0 {
                    this.progressed(this.last.received(read as u64));
                }
                Poll::Ready(Ok(()))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => this.poll_bound(cx),
        }
    }
}

impl<S: AsyncWrite + Unpin> AsyncWrite for PeerStream<S> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        match Pin::new(&mut this.io).poll_write(cx, buf) {
            Poll::Ready(Ok(written)) => {
                this.last = this.last.sent(written as u64);
                Poll::Ready(Ok(written))
            }
            Poll::Ready(Err(error)) => Poll::Ready(Err(error)),
            Poll::Pending => this.poll_bound(cx),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.io).poll_flush(cx) {
            Poll::Pending => this.poll_bound(cx),
            ready => ready,
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        match Pin::new(&mut this.io).poll_shutdown(cx) {
            Poll::Pending => this.poll_bound(cx),
            ready => ready,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fork_lock::Spawn as _;
    use crate::process::RunState;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    const SHORT_REAP: Bound = Bound::new(LatencyClass::Reap, Duration::from_millis(50));

    /// Kill the process group `leader` leads, which this test started.
    fn kill_group(leader: u32) {
        let group = libc::pid_t::try_from(leader).expect("a pid");
        // SAFETY: a negative pid addresses only the group this test's own child leads.
        assert_eq!(
            unsafe { libc::kill(-group, libc::SIGKILL) },
            0,
            "kill the test's group"
        );
    }

    #[test]
    fn a_child_that_never_exits_is_not_waited_for_past_its_bound() {
        let mut child = std::process::Command::new("/bin/sleep")
            .arg("600")
            .spawn_locked()
            .expect("spawn sleep");
        let pid = child.id();
        let failure = reap(
            "probe child",
            &mut child,
            &start("reap probe", SHORT_REAP),
            Progress::Nothing,
        )
        .expect_err("sleep 600 does not exit within 50 ms");
        let ReapError::Expired(expired) = &failure else {
            panic!("expected TIMEOUT, got {failure}");
        };
        assert_eq!(expired.op, "reap probe");
        assert_eq!(expired.peer.pid, pid);
        assert_eq!(expired.peer.state, RunState::Sleeping, "{failure}");
        assert!(expired.elapsed >= SHORT_REAP.within, "{failure}");
        assert_eq!(failure.code(), "TIMEOUT");
        // The expiry left the child running: it is reaped once killed.
        child.kill().expect("kill sleep");
        let status = reap(
            "probe child",
            &mut child,
            &start("reap probe", REAP),
            Progress::Signalled {
                signal: libc::SIGKILL,
            },
        )
        .expect("a killed child is reaped");
        use std::os::unix::process::ExitStatusExt as _;
        assert_eq!(status.signal(), Some(libc::SIGKILL));
    }

    #[test]
    fn an_exited_childs_output_held_by_its_own_child_expires_naming_the_stream() {
        use std::os::unix::process::CommandExt as _;
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "/bin/sleep 600 & printf started"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("spawn sh");
        let pid = child.id();
        let pumps = OutputPumps::start(&mut child);
        let status = reap(
            "probe child",
            &mut child,
            &start("reap probe", REAP),
            Progress::Nothing,
        )
        .expect("sh exits once it started sleep");
        assert!(status.success(), "{status}");
        let failure = pumps
            .join("probe child", pid, &start("collect output", SHORT_REAP))
            .expect_err("sleep holds both pipes");
        kill_group(pid);
        let OutputError::Held { stream, expired } = &failure else {
            panic!("expected the held pipe, got {failure}");
        };
        assert_eq!(*stream, OutputStream::Stdout);
        assert_eq!(expired.peer.pid, pid);
        assert_eq!(expired.op, "collect output");
        assert!(expired.elapsed >= SHORT_REAP.within, "{failure}");
        assert_eq!(failure.code(), "TIMEOUT");
    }

    #[test]
    fn a_command_run_within_a_deadline_answers_its_output_or_names_the_child_it_waits_on() {
        let output = output_within(
            "probe command",
            std::process::Command::new("/bin/sh")
                .args(["-c", "printf out; printf err >&2; exit 3"]),
            &start("run probe", REAP),
        )
        .expect("sh ends at once");
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(
            (output.stdout.as_slice(), output.stderr.as_slice()),
            (&b"out"[..], &b"err"[..])
        );

        use std::os::unix::process::CommandExt as _;
        let failure = output_within(
            "probe command",
            std::process::Command::new("/bin/sleep")
                .arg("600")
                .process_group(0),
            &start("run probe", SHORT_REAP),
        )
        .expect_err("sleep 600 does not end within 50 ms");
        let RunError::Reap(ReapError::Expired(expired)) = &failure else {
            panic!("expected TIMEOUT, got {failure}");
        };
        kill_group(expired.peer.pid);
        assert_eq!(expired.peer.role, "probe command");
        assert_eq!(failure.code(), "TIMEOUT");
    }

    #[test]
    fn a_spent_deadline_still_answers_a_child_that_has_exited() {
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "exit 4"])
            .spawn_locked()
            .expect("spawn sh");
        let exit = crate::process::ExitWatch::of(child.id()).expect("watch sh");
        if let Some(exit) = exit {
            exit.wait().expect("sh exits");
        }
        let spent = start("reap probe", Bound::new(LatencyClass::Reap, Duration::ZERO));
        let status = reap("probe child", &mut child, &spent, Progress::Nothing)
            .expect("an exited child is answered whatever the budget left");
        assert_eq!(status.code(), Some(4));
    }

    #[tokio::test]
    async fn a_spent_deadline_still_answers_a_tokio_child_that_has_exited() {
        use crate::fork_lock::Spawn as _;
        let mut child = tokio::process::Command::new("/bin/sh")
            .args(["-c", "exit 5"])
            .spawn_locked()
            .expect("spawn sh");
        let exit = crate::process::ExitWatch::of(child.id().expect("a pid")).expect("watch sh");
        if let Some(exit) = exit {
            exit.wait().expect("sh exits");
        }
        let spent = start("reap probe", Bound::new(LatencyClass::Reap, Duration::ZERO));
        let status = reap_async("probe child", &mut child, &spent, Progress::Nothing)
            .await
            .expect("an exited child is answered whatever the budget left");
        assert_eq!(status.code(), Some(5));
    }

    #[test]
    fn a_child_with_only_stderr_piped_answers_its_stderr() {
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "printf err >&2"])
            .stderr(std::process::Stdio::piped())
            .spawn_locked()
            .expect("spawn sh");
        let pid = child.id();
        let pumps = OutputPumps::start(&mut child);
        reap(
            "probe child",
            &mut child,
            &start("reap probe", REAP),
            Progress::Nothing,
        )
        .expect("sh exits");
        let (stdout, stderr) = pumps
            .join("probe child", pid, &start("collect output", REAP))
            .expect("the one pipe closes with the child");
        assert!(stdout.is_empty());
        assert_eq!(stderr, b"err");
    }

    #[test]
    fn an_exited_childs_output_is_joined_whole() {
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "printf out; printf err >&2"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn_locked()
            .expect("spawn sh");
        let pid = child.id();
        let pumps = OutputPumps::start(&mut child);
        reap(
            "probe child",
            &mut child,
            &start("reap probe", REAP),
            Progress::Nothing,
        )
        .expect("sh exits");
        let (stdout, stderr) = pumps
            .join("probe child", pid, &start("collect output", REAP))
            .expect("both pipes close with the child");
        assert_eq!(stdout, b"out");
        assert_eq!(stderr, b"err");
    }

    const SHORT: Bound = Bound::new(LatencyClass::Control, Duration::from_millis(50));

    // The streams are in memory: a kernel socket's readiness arrives outside the runtime, so
    // paused time could pass a bound before the runtime saw bytes already sent.
    fn probe() -> Peer {
        Peer::unix("probe daemon", Path::new("/probe.sock"))
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_that_takes_the_request_and_stays_silent_expires_naming_what_was_sent() {
        let (client, _server) = tokio::io::duplex(64);
        let mut stream = PeerStream::connected(client, probe(), start("status", SHORT));
        stream
            .write_all(b"{\"op\":\"status\"}\n")
            .await
            .expect("the request is written");
        let mut line = String::new();
        let error = BufReader::new(&mut stream)
            .read_line(&mut line)
            .await
            .expect_err("no answer comes");
        let failure = stream.fail(error);
        let WaitError::Expired(expired) = &failure else {
            panic!("expected TIMEOUT, got {failure}");
        };
        assert_eq!(expired.op, "status");
        assert_eq!(expired.class, LatencyClass::Control);
        assert_eq!(expired.last, Progress::Sent { bytes: 16 });
        // The bound holds to the timer's tick.
        assert!(
            expired.elapsed > Duration::from_millis(49)
                && expired.elapsed <= Duration::from_millis(50),
            "{failure}"
        );
        assert_eq!(failure.code(), "TIMEOUT");
        assert_eq!(failure.retry_class(), RetryClass::Terminal);
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_that_ends_the_connection_before_its_answer_is_lost() {
        let (client, server) = tokio::io::duplex(64);
        drop(server);
        let mut stream = PeerStream::connected(client, probe(), start("hello", SHORT));
        let mut byte = [0_u8; 1];
        let error = stream
            .read_exact(&mut byte)
            .await
            .expect_err("the peer is gone");
        let failure = stream.fail(error);
        assert!(
            matches!(
                &failure,
                WaitError::PeerLost {
                    op: "hello",
                    last: Progress::Connected,
                    ..
                }
            ),
            "{failure}"
        );
        assert_eq!(failure.retry_class(), RetryClass::Terminal);
    }

    #[tokio::test(start_paused = true)]
    async fn a_held_connection_waits_while_its_holder_speaks_and_fails_silent_after() {
        let (client, mut server) = tokio::io::duplex(64);
        let mut stream = PeerStream::connected(client, probe(), start("hold", SHORT));
        stream.hold(idle("job terminal", Duration::from_millis(80)));
        let holder = tokio::spawn(async move {
            for _ in 0..5 {
                tokio::time::sleep(Duration::from_millis(30)).await;
                server.write_all(b".").await.expect("a heartbeat");
            }
            // Alive, connected, and silent from here on.
            server
        });
        let mut received = Vec::new();
        let error = stream
            .read_to_end(&mut received)
            .await
            .expect_err("the holder falls silent");
        assert_eq!(received, b".....");
        let failure = stream.fail(error);
        let WaitError::Silent(silent) = &failure else {
            panic!("expected SILENT, got {failure}");
        };
        assert_eq!(silent.op, "job terminal");
        assert_eq!(silent.last, Progress::Received { bytes: 5 });
        assert!(
            silent.silent_for > Duration::from_millis(79)
                && silent.silent_for <= Duration::from_millis(80),
            "{failure}"
        );
        assert!(
            silent.elapsed > Duration::from_millis(229)
                && silent.elapsed <= Duration::from_millis(230),
            "{failure}"
        );
        drop(holder.await.expect("the holder ran"));
    }

    #[tokio::test(start_paused = true)]
    async fn a_peer_with_no_socket_is_blocked_on_that_service() {
        let socket = Path::new("/nonexistent-cowshed-deadline/peer.sock");
        let failure = PeerStream::connect("workspace supervisor", socket, start("hello", SHORT))
            .await
            .expect_err("nothing listens there");
        assert!(
            matches!(&failure, WaitError::Absent { op: "hello", .. }),
            "{failure}"
        );
        assert_eq!(
            failure.retry_class(),
            RetryClass::Blocked(BlockedReason::Service {
                name: "workspace supervisor at /nonexistent-cowshed-deadline/peer.sock".to_owned(),
            })
        );
    }
}
