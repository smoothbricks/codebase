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
use std::os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd};
use std::os::unix::ffi::OsStrExt as _;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::process::ExitStatus;
use std::sync::LazyLock;
use std::sync::mpsc;
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
/// One `launchctl` call launchd answers from its own state (`print`, `bootstrap`).
pub const LAUNCHCTL: Bound = Bound::new(LatencyClass::Ffi, Duration::from_secs(10));
/// One `codesign` signature or verification of a binary cowshed installs.
pub const CODESIGN: Bound = Bound::new(LatencyClass::Ffi, Duration::from_secs(10));
/// How often a holder says it is still there on a connection it holds open for a long wait.
pub const HEARTBEAT: Duration = Duration::from_secs(5);
/// The resolution of the timers that wake a wait.
pub const TIMER_TICK: Duration = Duration::from_millis(1);

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

    /// The peer with the pid the kernel reports for `connection` ([`peer_pid`]): a tokio stream
    /// or a std one.
    pub fn credited(mut self, connection: &impl AsFd) -> Self {
        self.pid = match peer_pid(connection) {
            Ok(pid) if pid > 0 => PeerPid::Known(pid),
            Ok(_) => PeerPid::NotReported,
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

/// For a caller whose failures are `io::Error`: the kind it matches on is kept (an absent peer's
/// `NotFound`, a refusal's `ConnectionRefused`, an expiry's `TimedOut`), and the message is the
/// typed failure's, naming the op, the peer and the time waited.
impl From<WaitError> for io::Error {
    fn from(failure: WaitError) -> Self {
        let kind = match &failure {
            WaitError::Expired(_) | WaitError::Silent(_) => io::ErrorKind::TimedOut,
            WaitError::PeerLost { .. } => io::ErrorKind::UnexpectedEof,
            WaitError::Absent { source, .. } | WaitError::Io { source, .. } => source.kind(),
        };
        io::Error::new(kind, failure)
    }
}
/// For a caller whose failures are `io::Error`: an expiry is `TimedOut`, a failed wait keeps its
/// own kind, and the message is the typed failure's, naming the child and its state.
impl From<ReapError> for io::Error {
    fn from(failure: ReapError) -> Self {
        let kind = match &failure {
            ReapError::Expired(_) => io::ErrorKind::TimedOut,
            ReapError::Wait { source, .. } => source.kind(),
        };
        io::Error::new(kind, failure)
    }
}

/// As [`ReapError`]'s: a held pipe is `TimedOut`, a failed read keeps its own kind.
impl From<OutputError> for io::Error {
    fn from(failure: OutputError) -> Self {
        let kind = match &failure {
            OutputError::Held { .. } => io::ErrorKind::TimedOut,
            OutputError::Read { source, .. } => source.kind(),
        };
        io::Error::new(kind, failure)
    }
}

/// As [`ReapError`]'s; a spawn's failure is the spawn's own error, as the unbounded run gave it.
impl From<RunError> for io::Error {
    fn from(failure: RunError) -> Self {
        match failure {
            RunError::Spawn { source, .. } => source,
            RunError::Reap(failure) => failure.into(),
            RunError::Output(failure) => failure.into(),
            silent @ RunError::Silent(_) => io::Error::new(io::ErrorKind::TimedOut, silent),
            RunError::Log { source, .. } => source,
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
    wait: Waiting,
    timer: Pin<Box<Sleep>>,
}

impl<S> fmt::Debug for PeerStream<S> {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("PeerStream")
            .field("wait", &self.wait)
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
        let io = connect_unix(&deadline, &peer, socket).await?;
        let peer = peer.credited(&io);
        Ok(Self::connected(io, peer, deadline))
    }
}

/// Connect to `peer`'s unix socket at `socket` within `deadline`: the one unix-socket connect of
/// a cowshed process (each crate's `clippy.toml` refuses the raw ones). Darwin makes a socket in
/// two steps, socket then close-on-exec, so each poll of the connect runs under the fork lock's
/// creation fence ([`crate::fork_lock::create`]): no child spawned meanwhile keeps the
/// connection past its `exec`, where the peer would never see it close.
pub async fn connect_unix(
    deadline: &Deadline,
    peer: &Peer,
    socket: &Path,
) -> Result<UnixStream, WaitError> {
    // WHY unbounded: `establish` ends it at `deadline`.
    #[allow(clippy::disallowed_methods)]
    let mut connect = std::pin::pin!(UnixStream::connect(socket));
    let fenced = std::future::poll_fn(|context| {
        crate::fork_lock::create(|| Ok(connect.as_mut().poll(context)))
            .expect("polling the connection runs an infallible closure")
    });
    establish(deadline, peer, fenced).await
}

/// Run `connect` to `peer` within `deadline`: a peer whose address does not exist is absent, and
/// a connect still pending at the bound has expired with nothing sent. A refused connect is an
/// I/O failure: Darwin refuses a full queue as it refuses a socket file nothing is attached to, so
/// a caller that must tell a peer that left from one that is there asks [`occupant`].
pub async fn establish<S>(
    deadline: &Deadline,
    peer: &Peer,
    connect: impl Future<Output = io::Result<S>>,
) -> Result<S, WaitError> {
    within(deadline, peer, Progress::Nothing, connect)
        .await?
        .map_err(|source| unconnected(deadline, peer, source))
}

/// A connect to `peer` that failed with `source`: absent when nothing is at its address, an I/O
/// failure otherwise.
fn unconnected(deadline: &Deadline, peer: &Peer, source: io::Error) -> WaitError {
    if source.kind() == io::ErrorKind::NotFound {
        WaitError::Absent {
            op: deadline.op(),
            peer: Box::new(peer.clone()),
            source,
        }
    } else {
        WaitError::Io {
            op: deadline.op(),
            peer: Box::new(peer.clone()),
            elapsed: deadline.elapsed(clock()),
            last: Progress::Nothing,
            source,
        }
    }
}

/// What holds the file at a unix socket path, as far as the kernel shows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Occupant {
    /// Nothing is there.
    Absent,
    /// A socket file no socket is attached to: nothing will accept on it again.
    Stopped,
    /// A socket file a socket is still attached to, whoever holds it.
    Live,
}

/// Whether a socket is still attached to the socket file at `path`. Neither a stream connection
/// nor its listener's process says so. Measured on Darwin, a live listener whose queue is full
/// refuses a stream connection, as does a socket that is bound and not listening; a listener
/// still accepts after the process that created it exited, from a child that inherited it, and
/// the kernel's peer identity names that exited creator. A datagram connection is refused only
/// when no socket at all is attached to the file -- every descriptor of it closed, in every
/// process -- and otherwise fails as the wrong type or, to a datagram socket, is made: Darwin's
/// `unp_connect` and Linux's `unix_find_bsd` find the attached socket before they compare types,
/// and neither looks at a queue. Measured on Darwin, a killed listener's file is detached by the
/// time its NOTE_EXIT is delivered. No bind attaches a socket to a file that already exists, so
/// a file found detached stays detached. Anything at `path` but a socket file, and any other
/// answer, is an error, never stopped.
pub fn occupant(path: &Path) -> io::Result<Occupant> {
    use std::os::unix::fs::FileTypeExt as _;
    match std::fs::symlink_metadata(path) {
        Ok(metadata) if metadata.file_type().is_socket() => {}
        Ok(_) => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("{} is not a socket", path.display()),
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Occupant::Absent),
        Err(error) => return Err(error),
    }
    match std::os::unix::net::UnixDatagram::unbound()?.connect(path) {
        Err(error) if error.kind() == io::ErrorKind::ConnectionRefused => Ok(Occupant::Stopped),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Occupant::Absent),
        Err(error) if error.raw_os_error() == Some(libc::EPROTOTYPE) => Ok(Occupant::Live),
        Ok(()) => Ok(Occupant::Live),
        Err(error) => Err(error),
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

/// Wait for the child at `pid`, `role` to this process, to exit within what is left of
/// `deadline`, leaving it unreaped for its owner to collect at once: the expiry names its pid and
/// the state the kernel shows it in, with `last` the last thing this process did to it (the signal
/// that should have ended it, or nothing). `pid` is the caller's own unreaped child, so it names no
/// other process. The exit is watched (`ExitWatch`), never polled. Expiry ends the wait, never the
/// child: one stuck in an uninterruptible kernel call stays as it is, named.
pub fn await_exit(
    role: &'static str,
    pid: u32,
    deadline: &Deadline,
    last: Progress,
) -> Result<(), ReapError> {
    let failed = |source| ReapError::Wait {
        op: deadline.op(),
        role,
        pid,
        elapsed: deadline.elapsed(clock()),
        source,
    };
    // No watch is a child that has exited (Darwin refuses to watch a zombie).
    if let Some(watch) = ExitWatch::of(pid).map_err(failed)? {
        loop {
            // A spent budget still looks once: an exited child answers.
            match watch.within(deadline.remaining(clock()).unwrap_or_default()) {
                Ok(true) => break,
                Ok(false) => {
                    return Err(ReapError::Expired(Box::new(deadline.expired(
                        ChildPeer::observed(role, pid),
                        last,
                        clock(),
                    ))));
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(failed(error)),
            }
        }
    }
    Ok(())
}

/// Collect the exit of `pid`, `role` to this process, that [`await_exit`] has seen, through
/// `look`, a collection that never blocks (`try_wait`, or `waitpid` with `WNOHANG`). Darwin
/// reports a process exiting before `wait` can collect it (measured: 17 of 2000 children were not
/// yet waitable right after `NOTE_EXIT` or `ESRCH`), so `look` runs again each timer tick until
/// it answers, within what is left of `deadline`. A process still in its exit at the bound (an
/// uninterruptible close) is named with its kernel state, and stays as it is.
pub fn collect_exited<T>(
    role: &'static str,
    pid: u32,
    deadline: &Deadline,
    last: Progress,
    mut look: impl FnMut() -> io::Result<Option<T>>,
) -> Result<T, ReapError> {
    loop {
        match look() {
            Ok(Some(collected)) => return Ok(collected),
            Ok(None) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(source) => {
                return Err(ReapError::Wait {
                    op: deadline.op(),
                    role,
                    pid,
                    elapsed: deadline.elapsed(clock()),
                    source,
                });
            }
        }
        if deadline.remaining(clock()).is_none() {
            return Err(ReapError::Expired(Box::new(deadline.expired(
                ChildPeer::observed(role, pid),
                last,
                clock(),
            ))));
        }
        std::thread::sleep(TIMER_TICK);
    }
}

/// Wait for `child`, `role` to this process, to exit within what is left of `deadline`: its
/// status, or the expiry of [`await_exit`] or [`collect_exited`] naming it.
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
    // A child already collected, or exited and collected now, answers before any watch, so a pid
    // that was freed and reused is never watched.
    if let Some(status) = child.try_wait().map_err(failed)? {
        return Ok(status);
    }
    await_exit(role, pid, deadline, last)?;
    collect_exited(role, pid, deadline, last, || child.try_wait())
}

/// Hand `child`, whose wait expired, to a thread that collects its exit whenever it comes and
/// says so, so a child that leaves an uninterruptible kernel call late is reaped, never left a
/// zombie for the life of this process. Nothing waits for that thread.
// WHY unbounded: nothing waits on this thread; it only collects the exit when it comes.
#[allow(clippy::disallowed_methods)]
pub fn collect_later(role: &'static str, mut child: std::process::Child) {
    std::thread::spawn(move || {
        let pid = child.id();
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

/// [`collect_later`] for a child tokio spawned. A tokio child cannot become a std one, and nothing
/// clears its `kill_on_drop` once spawned, so a task owns it until it exits and never drops it
/// before. That task ends with the runtime: a child still running when its process ends is then
/// killed by its own `kill_on_drop`, which is the process ending, not the wait that gave up on it.
pub fn collect_later_async(role: &'static str, mut child: tokio::process::Child) {
    let pid = child.id().unwrap_or_default();
    tokio::spawn(async move {
        // WHY unbounded: nothing waits on this task; it only collects the exit when it comes.
        #[allow(clippy::disallowed_methods)]
        let waited = child.wait().await;
        match waited {
            Ok(status) => {
                crate::eprintln!("cowshed: {role} pid {pid} ended after its wait expired: {status}")
            }
            Err(error) => crate::eprintln!(
                "cowshed: {role} pid {pid} could not be collected after its wait expired: {error}"
            ),
        }
    });
}

/// [`collect_later`] for a raw child `pid` of this process: a thread collects its exit whenever it
/// comes and says so. The pid is the collector's from here on; its caller signals it no more.
// WHY unbounded: nothing waits on this thread; it only collects the exit when it comes.
#[allow(clippy::disallowed_methods)]
pub fn collect_pid_later(role: &'static str, pid: libc::pid_t) {
    std::thread::spawn(move || {
        loop {
            // SAFETY: `pid` is this process's own unreaped child, which only this thread collects
            // from here on; a null status pointer is allowed.
            let waited = unsafe { libc::waitpid(pid, std::ptr::null_mut(), 0) };
            if waited == pid {
                crate::eprintln!("cowshed: {role} pid {pid} ended after its wait expired");
                return;
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                crate::eprintln!(
                    "cowshed: {role} pid {pid} could not be collected after its wait expired: {error}"
                );
                return;
            }
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
    // WHY unbounded: the timeout around it ends it at the deadline.
    #[allow(clippy::disallowed_methods)]
    let waited = tokio::time::timeout(remaining, child.wait()).await;
    match waited {
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
    /// A run whose output and CPU are its heartbeat ([`run_by_progress`]) showed neither, and
    /// waited behind no live holder, for its whole silence (code `SILENT`, terminal).
    #[error("{0}")]
    Silent(Box<SilentRun>),
    /// The run's output could not be written to its log (code `IO`).
    #[error("{op}: writing the output of {role} pid {pid} to its log failed: {source}")]
    Log {
        op: &'static str,
        role: &'static str,
        pid: u32,
        #[source]
        source: io::Error,
    },
}

/// A run that went silent ([`RunError::Silent`]): the silence, naming the child, and the running
/// members of its process group as the kernel showed them, the hung process among them, each by
/// pid, command and state. When the group's CPU could not be read, why.
#[derive(Debug)]
pub struct SilentRun {
    pub silent: Silent<ChildPeer>,
    pub group: io::Result<Vec<crate::process::GroupMember>>,
    pub cpu_unread: Option<io::Error>,
}

impl fmt::Display for SilentRun {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{}; no CPU advanced in its process group",
            self.silent
        )?;
        if let Some(error) = &self.cpu_unread {
            write!(formatter, " (its CPU could not be read: {error})")?;
        }
        match &self.group {
            Ok(members) if members.is_empty() => {
                formatter.write_str("; its group has no running process")
            }
            Ok(members) => {
                formatter.write_str("; its group's running processes:")?;
                for member in members {
                    write!(formatter, " {member};")?;
                }
                Ok(())
            }
            Err(error) => write!(formatter, "; its group could not be listed: {error}"),
        }
    }
}

/// What a run's spent silence decides ([`run_by_progress`]), given the CPU its group had used at
/// the last look, the CPU it has used now (`None` when it could not be read), and the live holder
/// the run waits behind, if any.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum AtSilence {
    /// The group's CPU advanced: the run works, silently.
    Worked,
    /// No CPU advanced, but the run waits in line behind this live holder: queued, not silent.
    Queued(String),
    /// Nothing moved: no output, no CPU, no holder ahead.
    Silent,
}

pub(crate) fn at_silence(last: Option<u64>, now: Option<u64>, queued: Option<String>) -> AtSilence {
    match (last, now, queued) {
        (Some(last), Some(now), _) if now > last => AtSilence::Worked,
        (_, _, Some(holder)) => AtSilence::Queued(holder),
        _ => AtSilence::Silent,
    }
}

impl RunError {
    /// The lmao error code this failure is reported under.
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Spawn { .. } => "IO",
            Self::Reap(failure) => failure.code(),
            Self::Output(failure) => failure.code(),
            Self::Silent(_) => "SILENT",
            Self::Log { .. } => "IO",
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
    let status = match reap(role, &mut child, deadline, Progress::Nothing) {
        Ok(status) => status,
        Err(failure) => {
            collect_later(role, child);
            return Err(failure.into());
        }
    };
    let (stdout, stderr) = pumps.join(role, child.id(), deadline)?;
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

/// Run `command`, `role` to this process, with its stdout and stderr appended to `log` as they
/// come, under [`run_by_progress`]'s silence: a long run has no total ceiling; its output and its
/// group's CPU are its heartbeat (specs/cowshed/05_gateway.md, "Deadlines"). The run waits behind
/// no holder.
pub fn run_to_log(
    role: &'static str,
    command: &mut std::process::Command,
    mut log: std::fs::File,
    silence: Duration,
) -> Result<ExitStatus, RunError> {
    use std::io::Write as _;
    run_by_progress(
        role,
        command,
        silence,
        &mut |_, bytes| log.write_all(bytes),
        &mut || None,
    )
}

/// Run `command`, `role` to this process, as [`output_within`] does (no input, both outputs
/// collected), under [`run_by_progress`]'s silence instead of a total: `queued` names the live
/// holder the run waits behind, if any.
pub fn output_by_progress(
    role: &'static str,
    command: &mut std::process::Command,
    silence: Duration,
    queued: &mut dyn FnMut() -> Option<String>,
) -> Result<std::process::Output, RunError> {
    let (mut stdout, mut stderr) = (Vec::new(), Vec::new());
    let status = run_by_progress(
        role,
        command,
        silence,
        &mut |stream, bytes| {
            match stream {
                OutputStream::Stdout => stdout.extend_from_slice(bytes),
                OutputStream::Stderr => stderr.extend_from_slice(bytes),
            }
            Ok(())
        },
        queued,
    )?;
    Ok(std::process::Output {
        status,
        stdout,
        stderr,
    })
}

/// Run `command`, `role` to this process, as the leader of a process group of its own, handing
/// each chunk of its stdout and stderr to `sink` as it comes, for as long as it shows progress
/// (AGENTS.md rule 15; 05_gateway.md "Deadlines"). Each chunk restarts `silence` ([`idle`]).
/// When the silence is spent, [`at_silence`] decides: CPU that advanced anywhere in the group
/// since the last look (a hook computing in silence) restarts it; so does a live holder `queued`
/// names, the run waiting in line behind it, which is said on stderr; with neither the run fails
/// `SILENT`, naming the child, its kernel state, the bytes it wrote and every running member of
/// its group by pid and command. The group is then sent SIGTERM and the child reaped within
/// [`REAP`], and collected later past that. Otherwise its status, once both streams have ended
/// and it is reaped within [`REAP`]. Its stdin is null.
pub fn run_by_progress(
    role: &'static str,
    command: &mut std::process::Command,
    silence: Duration,
    sink: &mut dyn FnMut(OutputStream, &[u8]) -> io::Result<()>,
    queued: &mut dyn FnMut() -> Option<String>,
) -> Result<ExitStatus, RunError> {
    use crate::fork_lock::Spawn as _;
    use std::os::unix::process::CommandExt as _;
    const OP: &str = "run to log";

    let mut child = command
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .process_group(0)
        .spawn_locked()
        .map_err(|source| RunError::Spawn {
            op: OP,
            role,
            source,
        })?;
    let pid = child.id();
    let group = libc::pid_t::try_from(pid).ok();
    let read_cpu = || group.map(crate::process::group_cpu);
    let mut cpu = read_cpu().and_then(Result::ok);
    let pumps = OutputPumps::start(&mut child);
    let mut open = pumps.started.len();
    let mut heard = idle(OP, silence);
    let mut written = 0_u64;
    let failure = loop {
        if open == 0 {
            break None;
        }
        let wait = heard.remaining(clock()).unwrap_or_default();
        match pumps.pumped.recv_timeout(wait) {
            Ok(Pumped::Chunk(stream, bytes)) => {
                if let Err(source) = sink(stream, &bytes) {
                    break Some(RunError::Log {
                        op: OP,
                        role,
                        pid,
                        source,
                    });
                }
                written += u64::try_from(bytes.len()).unwrap_or(u64::MAX);
                heard.observe(Progress::Received { bytes: written }, clock());
            }
            Ok(Pumped::End(_)) => open -= 1,
            Ok(Pumped::Failed(stream, source)) => {
                break Some(RunError::Output(OutputError::Read {
                    op: OP,
                    role,
                    pid,
                    stream,
                    source,
                }));
            }
            // The timer's tick can wake the wait a hair early; only a spent silence is silent.
            Err(mpsc::RecvTimeoutError::Timeout) if heard.remaining(clock()).is_some() => {}
            Err(mpsc::RecvTimeoutError::Timeout) => {
                let (now, mut cpu_unread) = match read_cpu() {
                    Some(Ok(now)) => (Some(now), None),
                    Some(Err(error)) => (None, Some(error)),
                    None => (None, None),
                };
                if cpu.is_none() && cpu_unread.is_none() {
                    cpu_unread = Some(io::Error::other(
                        "its group's CPU had no earlier reading to compare with",
                    ));
                }
                let decided = at_silence(cpu, now, queued());
                cpu = now.or(cpu);
                // A window of work or queueing restarts the silence; the bytes written stay the
                // last progress, which a later silence names.
                let written = Progress::Received { bytes: written };
                match decided {
                    AtSilence::Worked => heard.observe(written, clock()),
                    AtSilence::Queued(holder) => {
                        crate::eprintln!("cowshed: {role} pid {pid} waits in line behind {holder}");
                        heard.observe(written, clock());
                    }
                    AtSilence::Silent => {
                        break Some(RunError::Silent(Box::new(SilentRun {
                            silent: heard.silent(ChildPeer::observed(role, pid), clock()),
                            group: group.map_or_else(
                                || Err(io::Error::other(format!("pid {pid} names no group"))),
                                crate::process::group_members,
                            ),
                            cpu_unread,
                        })));
                    }
                }
            }
            // Every pump has ended and said so.
            Err(mpsc::RecvTimeoutError::Disconnected) => break None,
        }
    };
    let reap_budget = start(OP, REAP);
    let reaped = match failure {
        None => reap(
            role,
            &mut child,
            &reap_budget,
            Progress::State("output ended"),
        ),
        Some(failure) => {
            if let Some(group) = group {
                // SAFETY: `group` is the process group this process's own child leads.
                unsafe { libc::killpg(group, libc::SIGTERM) };
            }
            let signalled = Progress::Signalled {
                signal: libc::SIGTERM,
            };
            if reap(role, &mut child, &reap_budget, signalled).is_err() {
                collect_later(role, child);
            }
            return Err(failure);
        }
    };
    reaped.map_err(|failure| {
        collect_later(role, child);
        RunError::Reap(failure)
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

/// What a pump has from its stream: the next bytes, the stream's end, or the read that failed.
enum Pumped {
    Chunk(OutputStream, Vec<u8>),
    End(OutputStream),
    Failed(OutputStream, io::Error),
}

/// A child's piped stdout and stderr, each read on a thread of its own while the child runs, as
/// `Command::output` reads them, so a verbose child never wedges on a full pipe. Each pump hands
/// its bytes over as they arrive; once the pumps are dropped (a join gave up, a run failed), what
/// was read and not collected is freed with them, and each pump reads on, keeping nothing, until
/// its holder closes the pipe: a held pipe costs one blocked thread and no memory, and its holder
/// is never sent `SIGPIPE`.
pub struct OutputPumps {
    started: Vec<OutputStream>,
    pumped: mpsc::Receiver<Pumped>,
}

impl OutputPumps {
    /// Start reading `child`'s piped stdout and stderr; a stream that is not piped reads empty.
    pub fn start(child: &mut std::process::Child) -> Self {
        use std::io::Read as _;

        let (sent, pumped) = mpsc::channel();
        let mut started = Vec::with_capacity(2);
        let mut pump = |stream, pipe: Option<Box<dyn std::io::Read + Send>>| {
            let Some(mut pipe) = pipe else { return };
            started.push(stream);
            let sent = sent.clone();
            std::thread::spawn(move || {
                let mut chunk = [0_u8; 64 * 1024];
                // A send refused is a collector that is gone: the bytes are dropped, and the
                // pump drains on to the holder's close.
                loop {
                    match pipe.read(&mut chunk) {
                        Ok(0) => {
                            let _ = sent.send(Pumped::End(stream));
                            return;
                        }
                        Ok(length) => {
                            let _ = sent.send(Pumped::Chunk(stream, chunk[..length].to_vec()));
                        }
                        Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                        Err(error) => {
                            let _ = sent.send(Pumped::Failed(stream, error));
                            return;
                        }
                    }
                }
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
        Self { started, pumped }
    }

    /// The bytes each stream carried to its end, the child `role` at `pid` having exited and
    /// been collected, within what is left of `deadline`; a spent budget still takes what has
    /// already arrived. A stream still open at the bound is held by a process that inherited the
    /// pipe: the wait ends naming the child and that stream. The collected child's pid is never
    /// read again.
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
            match self
                .pumped
                .recv_timeout(deadline.remaining(clock()).unwrap_or_default())
            {
                Ok(Pumped::Chunk(OutputStream::Stdout, bytes)) => stdout.extend_from_slice(&bytes),
                Ok(Pumped::Chunk(OutputStream::Stderr, bytes)) => stderr.extend_from_slice(&bytes),
                Ok(Pumped::End(stream)) => open.retain(|pending| *pending != stream),
                Ok(Pumped::Failed(stream, source)) => {
                    return Err(OutputError::Read {
                        op: deadline.op(),
                        role,
                        pid,
                        stream,
                        source,
                    });
                }
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
            }
        }
        Ok((stdout, stderr))
    }
}

/// What a connection's waits are on and bounded by, and how far the last one got: the state
/// [`PeerStream`] and [`PeerSocket`] share, and the failures they name from it.
#[derive(Debug)]
struct Waiting {
    peer: Peer,
    bounding: Bounding,
    last: Progress,
}

impl Waiting {
    fn request(peer: Peer, deadline: Deadline) -> Self {
        Self {
            peer,
            bounding: Bounding::Request(deadline),
            last: Progress::Connected,
        }
    }

    /// Record `progress`, and answer whether it started a held wait's silence over.
    fn progressed(&mut self, progress: Progress) -> bool {
        self.last = progress;
        let Bounding::Held(idle) = &mut self.bounding else {
            return false;
        };
        idle.observe(progress, clock());
        true
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

    /// The expiry as the `io::Error` a read or write returns through the protocol's framing.
    fn timed_out(&self) -> io::Error {
        io::Error::new(io::ErrorKind::TimedOut, self.expiry())
    }

    fn fail(&self, error: io::Error) -> WaitError {
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
            _ => WaitError::Io {
                op: self.bounding.op(),
                peer: Box::new(self.peer.clone()),
                elapsed: self.bounding.elapsed(),
                last: self.last,
                source: error,
            },
        }
    }

    fn lost(&self) -> WaitError {
        self.fail(io::Error::from(io::ErrorKind::UnexpectedEof))
    }
}

impl<S> PeerStream<S> {
    /// `io`, already connected to `peer` within `deadline`, which bounds the request on it.
    pub fn connected(io: S, peer: Peer, deadline: Deadline) -> Self {
        let wait = Waiting::request(peer, deadline);
        Self {
            timer: Box::pin(tokio::time::sleep(wait.bounding.remaining())),
            io,
            wait,
        }
    }

    pub fn peer(&self) -> &Peer {
        &self.wait.peer
    }

    /// The connection itself, for what only it can say (its peer's credentials). Reading or
    /// writing through it would bypass the bound.
    pub fn get_ref(&self) -> &S {
        &self.io
    }

    pub fn op(&self) -> &'static str {
        self.wait.bounding.op()
    }

    pub fn last(&self) -> Progress {
        self.wait.last
    }

    /// Bound the next request on this connection by `deadline`.
    pub fn request(&mut self, deadline: Deadline) {
        self.wait.bounding = Bounding::Request(deadline);
        self.rearm();
    }

    /// Hold the connection for a long wait: its holder must send something at least every
    /// `idle` bound, and may take as long as its work does.
    pub fn hold(&mut self, idle: Idle) {
        self.wait.bounding = Bounding::Held(idle);
        self.rearm();
    }

    /// Hold a held connection's holder to `idle` of silence from here on; the wait's start and
    /// last progress stay. A request's budget is left as it is.
    pub fn silence(&mut self, idle: Duration) {
        if let Bounding::Held(held) = &mut self.wait.bounding {
            held.set_silence(idle);
            self.rearm();
        }
    }

    fn rearm(&mut self) {
        let at = tokio::time::Instant::now() + self.wait.bounding.remaining();
        self.timer.as_mut().reset(at);
    }

    fn progressed(&mut self, progress: Progress) {
        if self.wait.progressed(progress) {
            self.rearm();
        }
    }

    /// The timer only wakes the wait; the bound decides it. Tokio's timer fires on a millisecond
    /// tick, so it can wake a little before the bound: with more than a tick left the timer is
    /// re-armed for the rest, which also lets time pass before the next decision.
    fn poll_bound<T>(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<T>> {
        loop {
            ready!(self.timer.as_mut().poll(cx));
            if self.wait.bounding.remaining() < TIMER_TICK {
                return Poll::Ready(Err(self.wait.timed_out()));
            }
            self.rearm();
        }
    }

    /// `error`, which an operation through this stream returned, as the typed failure of the
    /// wait: the expiry it carries, the peer ending the connection, or the I/O error itself.
    pub fn fail(&self, error: io::Error) -> WaitError {
        self.wait.fail(error)
    }

    /// The peer ended the connection before the answer the caller needed.
    pub fn lost(&self) -> WaitError {
        self.wait.lost()
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
                    this.progressed(this.wait.last.received(read as u64));
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
                this.wait.last = this.wait.last.sent(written as u64);
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

/// The pid the kernel reports for the process at the other end of the connected Unix stream
/// `connection`: `LOCAL_PEERPID` on Darwin, `SO_PEERCRED` on Linux. It is the process that
/// connected or made the pair, read from this connection, never whoever owns its path.
pub fn peer_pid(connection: &impl AsFd) -> io::Result<libc::pid_t> {
    let descriptor = connection.as_fd().as_raw_fd();
    #[cfg(target_vendor = "apple")]
    {
        let mut pid: libc::pid_t = 0;
        let mut length = socklen_of::<libc::pid_t>()?;
        // SAFETY: `connection` keeps `descriptor` open through the call, and the pointer and
        // length describe one initialized `pid_t`.
        let read = unsafe {
            libc::getsockopt(
                descriptor,
                libc::SOL_LOCAL,
                libc::LOCAL_PEERPID,
                std::ptr::from_mut(&mut pid).cast(),
                &mut length,
            )
        };
        answered::<libc::pid_t>(read, length, "LOCAL_PEERPID")?;
        Ok(pid)
    }
    #[cfg(target_os = "linux")]
    {
        let mut credentials = libc::ucred {
            pid: 0,
            uid: 0,
            gid: 0,
        };
        let mut length = socklen_of::<libc::ucred>()?;
        // SAFETY: `connection` keeps `descriptor` open through the call, and the pointer and
        // length describe one initialized `ucred`.
        let read = unsafe {
            libc::getsockopt(
                descriptor,
                libc::SOL_SOCKET,
                libc::SO_PEERCRED,
                std::ptr::from_mut(&mut credentials).cast(),
                &mut length,
            )
        };
        answered::<libc::ucred>(read, length, "SO_PEERCRED")?;
        Ok(credentials.pid)
    }
    #[cfg(not(any(target_vendor = "apple", target_os = "linux")))]
    {
        let _ = descriptor;
        Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "this kernel reports no peer pid",
        ))
    }
}

/// The size of one `T` as a `getsockopt` buffer length.
#[cfg(any(target_vendor = "apple", target_os = "linux"))]
fn socklen_of<T>() -> io::Result<libc::socklen_t> {
    libc::socklen_t::try_from(std::mem::size_of::<T>()).map_err(io::Error::other)
}

/// `getsockopt`'s return `read` for `option`, which was to fill `length` bytes of one `T`.
#[cfg(any(target_vendor = "apple", target_os = "linux"))]
fn answered<T>(read: libc::c_int, length: libc::socklen_t, option: &str) -> io::Result<()> {
    if read != 0 {
        return Err(io::Error::last_os_error());
    }
    let expected = std::mem::size_of::<T>();
    if usize::try_from(length).ok() != Some(expected) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("{option} answered {length} bytes, not {expected}"),
        ));
    }
    Ok(())
}

/// The blocking twin of [`PeerStream`], for a client that runs no async runtime: a connected Unix
/// stream whose every read and write ends by the bound of the wait in progress.
///
/// Connecting is one nonblocking connect and a poll for writability clipped to what is left of
/// the request's deadline. The stream stays nonblocking: a read or write that would block polls
/// for readiness clipped to the wait's bound, and the bound, not the poll, decides the expiry.
/// No socket timeout option is set: Darwin refuses `setsockopt` once the peer shut down, even
/// while a whole answer is still readable, and a kernel timeout restarts on every partial read.
///
/// It is a `Read + Write`, so the protocol's own framing reads and writes through it unchanged. A
/// wait past its bound fails with an `io::Error` of kind `TimedOut` carrying the [`WaitError`],
/// which [`PeerSocket::fail`] recovers, typed, from whatever the framing returned.
#[derive(Debug)]
pub struct PeerSocket {
    stream: std::os::unix::net::UnixStream,
    wait: Waiting,
}

impl PeerSocket {
    /// Connect to `role` at `socket` within `deadline`, which then bounds the request on it, and
    /// name the pid the kernel reports for the listener. The failures are [`establish`]'s:
    /// nothing at the path is `BLOCKED`, a connect still pending at the bound is `TIMEOUT` with
    /// nothing sent, and a refusal, a full listen queue's included, is an I/O failure, never a
    /// retry.
    pub fn connect(
        role: &'static str,
        socket: &Path,
        deadline: Deadline,
    ) -> Result<Self, WaitError> {
        let peer = Peer::unix(role, socket);
        let stream = open(socket, &deadline, &peer)?;
        let peer = peer.credited(&stream);
        Ok(Self {
            stream,
            wait: Waiting::request(peer, deadline),
        })
    }

    /// `stream`, already connected to `peer` within `deadline` (one end of a socketpair, say),
    /// which bounds the request on it. The stream is made nonblocking here.
    pub fn connected(
        stream: std::os::unix::net::UnixStream,
        peer: Peer,
        deadline: Deadline,
    ) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        Ok(Self {
            stream,
            wait: Waiting::request(peer, deadline),
        })
    }

    pub fn peer(&self) -> &Peer {
        &self.wait.peer
    }

    pub fn op(&self) -> &'static str {
        self.wait.bounding.op()
    }

    pub fn last(&self) -> Progress {
        self.wait.last
    }

    /// Bound the next request on this connection by `deadline`.
    pub fn request(&mut self, deadline: Deadline) {
        self.wait.bounding = Bounding::Request(deadline);
    }

    /// Hold the connection for a long wait: its holder must show progress at least every `idle`
    /// bound, and may take as long as its work does.
    pub fn hold(&mut self, idle: Idle) {
        self.wait.bounding = Bounding::Held(idle);
    }

    /// The holder showed `progress` the protocol names (a heartbeat frame's stage, say), which
    /// the bytes that carried it do not say. A held wait's silence starts over.
    pub fn progressed(&mut self, progress: Progress) {
        self.wait.progressed(progress);
    }

    /// `error`, which an operation through this connection returned, as the typed failure of
    /// the wait: the expiry it carries, the peer ending the connection, or the I/O error itself.
    pub fn fail(&self, error: io::Error) -> WaitError {
        self.wait.fail(error)
    }

    /// The peer ended the connection before the answer the caller needed.
    pub fn lost(&self) -> WaitError {
        self.wait.lost()
    }

    /// Sleep until the stream is ready for `events` or the wait's bound is spent. The poll only
    /// wakes the wait; the bound decides it, so a wake before the bound polls again.
    fn ready(&self, events: libc::c_short) -> io::Result<()> {
        loop {
            let remaining = self.wait.bounding.remaining();
            if remaining.is_zero() {
                return Err(self.wait.timed_out());
            }
            match poll_ready(self.stream.as_fd(), events, remaining) {
                Ok(true) => return Ok(()),
                Ok(false) => {}
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }
}

impl io::Read for PeerSocket {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        loop {
            match io::Read::read(&mut &self.stream, buffer) {
                Ok(read) => {
                    if read != 0 {
                        let received = self.wait.last.received(read as u64);
                        self.wait.progressed(received);
                    }
                    return Ok(read);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.ready(libc::POLLIN)?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }
}

impl io::Write for PeerSocket {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        loop {
            match io::Write::write(&mut &self.stream, bytes) {
                Ok(written) => {
                    self.wait.last = self.wait.last.sent(written as u64);
                    return Ok(written);
                }
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    self.ready(libc::POLLOUT)?;
                }
                Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
                Err(error) => return Err(error),
            }
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// One nonblocking connect to `peer` at `socket`, its completion awaited within what is left of
/// `deadline`. The stream stays nonblocking: every later wait on it polls within its own bound.
fn open(
    socket: &Path,
    deadline: &Deadline,
    peer: &Peer,
) -> Result<std::os::unix::net::UnixStream, WaitError> {
    let refused = |source| unconnected(deadline, peer, source);
    let expired = || {
        WaitError::Expired(Box::new(deadline.expired(
            peer.clone(),
            Progress::Nothing,
            clock(),
        )))
    };
    let (address, length) = unix_address(socket).map_err(refused)?;
    // Darwin has no SOCK_CLOEXEC: the descriptor is made and marked under the fork lock, so no
    // spawn forks between the two steps and carries it past its exec.
    let descriptor = crate::fork_lock::create(|| {
        // SAFETY: socket makes a fresh descriptor, owned exactly once below.
        let raw = unsafe { libc::socket(libc::AF_UNIX, libc::SOCK_STREAM, 0) };
        if raw == -1 {
            return Err(io::Error::last_os_error());
        }
        // SAFETY: `raw` is the fresh descriptor socket just returned, which nothing else owns.
        let descriptor = unsafe { OwnedFd::from_raw_fd(raw) };
        // SAFETY: fcntl changes only the flags of this owned descriptor.
        if unsafe { libc::fcntl(raw, libc::F_SETFD, libc::FD_CLOEXEC) } == -1
            || unsafe { libc::fcntl(raw, libc::F_SETFL, libc::O_NONBLOCK) } == -1
        {
            return Err(io::Error::last_os_error());
        }
        Ok(descriptor)
    })
    .map_err(refused)?;
    // A full AF_UNIX listen queue answers EAGAIN at once: a refusal, not a pending connect, and
    // no leave to try again. A connect a signal interrupted is made again, and one that answers
    // it is already connected has connected.
    loop {
        if deadline.remaining(clock()).is_none() {
            return Err(expired());
        }
        // SAFETY: `address` is a whole pathname sockaddr_un whose checked `length` fits it, and
        // `descriptor` stays open through the call.
        let connected = unsafe {
            libc::connect(
                descriptor.as_raw_fd(),
                std::ptr::from_ref(&address).cast(),
                length,
            )
        };
        if connected == 0 {
            return Ok(descriptor.into());
        }
        let error = io::Error::last_os_error();
        match error.raw_os_error() {
            Some(libc::EINTR) => {}
            Some(libc::EISCONN) => return Ok(descriptor.into()),
            Some(libc::EINPROGRESS | libc::EALREADY) => break,
            _ => return Err(refused(error)),
        }
    }
    loop {
        let remaining = deadline.remaining(clock()).ok_or_else(expired)?;
        match poll_ready(descriptor.as_fd(), libc::POLLOUT, remaining) {
            Ok(true) => break,
            Ok(false) => {}
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(refused(error)),
        }
    }
    let stream = std::os::unix::net::UnixStream::from(descriptor);
    if let Some(error) = stream.take_error().map_err(refused)? {
        return Err(refused(error));
    }
    // A clear SO_ERROR says no failure was recorded, not that the connect completed: the peer's
    // address does.
    stream.peer_addr().map_err(refused)?;
    Ok(stream)
}

/// `socket` as a pathname `sockaddr_un` and its length. A path that is empty, holds a NUL or does
/// not fit `sun_path` is invalid input.
fn unix_address(socket: &Path) -> io::Result<(libc::sockaddr_un, libc::socklen_t)> {
    // SAFETY: every field of sockaddr_un, integers and a byte array, admits zero.
    let mut address: libc::sockaddr_un = unsafe { std::mem::zeroed() };
    let path = socket.as_os_str().as_bytes();
    if path.is_empty() || path.len() >= address.sun_path.len() || path.contains(&0) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!(
                "the socket path {} is empty, holds a NUL, or is longer than sockaddr_un holds",
                socket.display()
            ),
        ));
    }
    address.sun_family = libc::sa_family_t::try_from(libc::AF_UNIX).map_err(io::Error::other)?;
    for (target, byte) in address.sun_path.iter_mut().zip(path) {
        *target = libc::c_char::from_ne_bytes([*byte]);
    }
    let length = std::mem::offset_of!(libc::sockaddr_un, sun_path) + path.len() + 1;
    let length = libc::socklen_t::try_from(length).map_err(io::Error::other)?;
    #[cfg(target_vendor = "apple")]
    {
        address.sun_len = u8::try_from(length).map_err(io::Error::other)?;
    }
    Ok((address, length))
}

/// `within` as a `poll(2)` timeout: rounded up to the poll's millisecond, so a poll never wakes
/// before the bound it was asked to reach. A truncated timeout wakes up to a millisecond early, and
/// a caller that reads the poll's timeout as its bound's expiry then reports it before the bound.
pub fn poll_timeout(within: Duration) -> libc::c_int {
    let millis = within
        .as_millis()
        .saturating_add(u128::from(!within.subsec_nanos().is_multiple_of(1_000_000)));
    libc::c_int::try_from(millis).unwrap_or(libc::c_int::MAX)
}

/// Wait up to `within` for `descriptor` to be ready for `events`, and answer whether it is. The
/// caller decides its bound again after every wake.
fn poll_ready(
    descriptor: BorrowedFd<'_>,
    events: libc::c_short,
    within: Duration,
) -> io::Result<bool> {
    let timeout = poll_timeout(within);
    let mut pending = libc::pollfd {
        fd: descriptor.as_raw_fd(),
        events,
        revents: 0,
    };
    // SAFETY: `pending` is one initialized pollfd, exclusively borrowed for the call, naming a
    // descriptor the caller keeps open.
    match unsafe { libc::poll(&raw mut pending, 1, timeout) } {
        -1 => Err(io::Error::last_os_error()),
        ready => Ok(ready != 0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::fork_lock::Spawn as _;
    use crate::process::RunState;
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

    const SHORT_REAP: Bound = Bound::new(LatencyClass::Reap, Duration::from_millis(50));

    /// A bound becomes a poll timeout no shorter than itself: a part millisecond rounds up, a
    /// whole one stays, and one past the poll's range waits as long as a poll can.
    #[test]
    fn a_poll_timeout_never_ends_before_its_bound() {
        for (within, millis) in [
            (Duration::ZERO, 0),
            (Duration::from_nanos(1), 1),
            (Duration::from_micros(49_187), 50),
            (Duration::from_millis(50), 50),
            (Duration::from_nanos(50_000_001), 51),
            (Duration::from_secs(u64::MAX), libc::c_int::MAX),
        ] {
            assert_eq!(poll_timeout(within), millis, "{within:?}");
        }
    }

    /// Kill the process group `leader` leads, which this test started. A group with no member
    /// left is already ended: the assertions that follow say what ended it.
    fn kill_group(leader: u32) {
        let group = libc::pid_t::try_from(leader).expect("a pid");
        // SAFETY: a negative pid addresses only the group this test's own child leads.
        let killed = unsafe { libc::kill(-group, libc::SIGKILL) };
        let error = io::Error::last_os_error();
        assert!(
            killed == 0 || error.raw_os_error() == Some(libc::ESRCH),
            "kill the test's group: {error}"
        );
    }

    /// The pid of the child a run failure names: every failure after the spawn names it.
    fn started_child(failure: &RunError) -> Option<u32> {
        match failure {
            RunError::Spawn { .. } => None,
            RunError::Reap(ReapError::Expired(expired))
            | RunError::Output(OutputError::Held { expired, .. }) => Some(expired.peer.pid),
            RunError::Reap(ReapError::Wait { pid, .. })
            | RunError::Output(OutputError::Read { pid, .. })
            | RunError::Log { pid, .. } => Some(*pid),
            RunError::Silent(run) => Some(run.silent.holder.pid),
        }
    }

    #[test]
    fn a_child_that_never_exits_is_not_waited_for_past_its_bound() {
        let mut child = std::process::Command::new("sleep")
            .arg("600")
            .spawn_locked()
            .expect("spawn sleep");
        let pid = child.id();
        let expiry = reap(
            "probe child",
            &mut child,
            &start("reap probe", SHORT_REAP),
            Progress::Nothing,
        );
        // The expiry left the child running: it is killed and collected before any assertion can
        // panic, so a failed one leaves no sleeper behind.
        child.kill().expect("kill sleep");
        let collected = reap(
            "probe child",
            &mut child,
            &start("reap probe", REAP),
            Progress::Signalled {
                signal: libc::SIGKILL,
            },
        );
        let failure = expiry.expect_err("sleep 600 does not exit within 50 ms");
        let ReapError::Expired(expired) = &failure else {
            panic!("expected TIMEOUT, got {failure}");
        };
        assert_eq!(expired.op, "reap probe");
        assert_eq!(expired.peer.pid, pid);
        // Alive at the bound, in a state a child shows right after it started: running its
        // start, in an uninterruptible kernel wait during it, or asleep in nanosleep. One the
        // wait had collected would be a zombie or gone.
        assert!(
            matches!(
                expired.peer.state,
                RunState::Running | RunState::Sleeping | RunState::Uninterruptible
            ),
            "{failure}"
        );
        assert!(expired.elapsed >= SHORT_REAP.within, "{failure}");
        assert_eq!(failure.code(), "TIMEOUT");
        use std::os::unix::process::ExitStatusExt as _;
        let status = collected.expect("a killed child is reaped");
        assert_eq!(status.signal(), Some(libc::SIGKILL));
    }

    #[test]
    fn an_exited_childs_output_held_by_its_own_child_expires_naming_the_stream() {
        use std::os::unix::process::CommandExt as _;
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "sleep 600 & printf started"])
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("spawn sh");
        let pid = child.id();
        let pumps = OutputPumps::start(&mut child);
        let exit = reap(
            "probe child",
            &mut child,
            &start("reap probe", REAP),
            Progress::Nothing,
        );
        let held = pumps.join("probe child", pid, &start("collect output", SHORT_REAP));
        // Its sleep outlives it, holding both pipes: the group is ended before any assertion can
        // panic, so a failed one leaves no sleeper behind.
        kill_group(pid);
        let status = exit.expect("sh exits once it started sleep");
        assert!(status.success(), "{status}");
        let failure = held.expect_err("sleep holds both pipes");
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
            std::process::Command::new("sleep")
                .arg("600")
                .process_group(0),
            &start("run probe", SHORT_REAP),
        )
        .expect_err("sleep 600 does not end within 50 ms");
        // `output_within` hands a child it stopped waiting for to the background: its group is ended
        // before any assertion can panic, whatever the failure, so a failed one leaves no sleeper behind.
        if let Some(pid) = started_child(&failure) {
            kill_group(pid);
        }
        let RunError::Reap(ReapError::Expired(expired)) = &failure else {
            panic!("expected TIMEOUT, got {failure}");
        };
        assert_eq!(expired.peer.role, "probe command");
        assert_eq!(failure.code(), "TIMEOUT");
    }
    #[test]
    fn a_raw_child_not_exited_at_the_bound_expires_naming_it_and_stays_unreaped() {
        let mut child = std::process::Command::new("/bin/sleep")
            .arg("600")
            .spawn_locked()
            .expect("spawn sleep");
        let pid = child.id();
        let failure = await_exit(
            "probe child",
            pid,
            &start("exit probe", SHORT_REAP),
            Progress::Nothing,
        )
        .expect_err("sleep 600 does not exit within 50 ms");
        let ReapError::Expired(expired) = &failure else {
            panic!("expected TIMEOUT, got {failure}");
        };
        assert_eq!((expired.op, expired.peer.pid), ("exit probe", pid));
        child.kill().expect("kill sleep");
        await_exit(
            "probe child",
            pid,
            &start("exit probe", REAP),
            Progress::Signalled {
                signal: libc::SIGKILL,
            },
        )
        .expect("a killed child exits");
        use std::os::unix::process::ExitStatusExt as _;
        let status = collect_exited(
            "probe child",
            pid,
            &start("collect probe", REAP),
            Progress::Signalled {
                signal: libc::SIGKILL,
            },
            || child.try_wait(),
        )
        .expect("the exit await_exit saw is collected within the reap bound");
        assert_eq!(status.signal(), Some(libc::SIGKILL));
    }

    #[test]
    fn a_seen_exit_never_collectable_at_the_bound_expires_naming_the_child() {
        let pid = std::process::id();
        let mut looks = 0;
        let failure = collect_exited(
            "probe child",
            pid,
            &start("collect probe", SHORT_REAP),
            Progress::Signalled {
                signal: libc::SIGKILL,
            },
            || {
                looks += 1;
                Ok(None::<()>)
            },
        )
        .expect_err("nothing ever collects");
        let ReapError::Expired(expired) = &failure else {
            panic!("expected TIMEOUT, got {failure}");
        };
        assert_eq!((expired.op, expired.peer.pid), ("collect probe", pid));
        assert!(looks > 1, "the collection looks again each tick: {looks}");
    }

    #[test]
    fn a_seen_exit_collectable_late_is_returned() {
        let mut looks = 0;
        let collected = collect_exited(
            "probe child",
            std::process::id(),
            &start("collect probe", REAP),
            Progress::Nothing,
            || {
                looks += 1;
                Ok((looks == 3).then_some(7))
            },
        )
        .expect("the third look collects");
        assert_eq!(collected, 7);
    }

    /// A log file of this test's own, removed on every exit path.
    struct LogFile(std::path::PathBuf);

    impl LogFile {
        fn new(label: &str) -> Self {
            Self(std::env::temp_dir().join(format!(
                "cowshed-run-log-{label}-{}-{}",
                std::process::id(),
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .map_or(0, |since| since.as_nanos())
            )))
        }

        fn create(&self) -> std::fs::File {
            std::fs::File::create(&self.0).expect("create the log")
        }
    }

    impl Drop for LogFile {
        fn drop(&mut self) {
            let _ = std::fs::remove_file(&self.0);
        }
    }

    #[test]
    fn a_run_to_log_appends_both_streams_and_answers_the_exit() {
        let log = LogFile::new("exit");
        let status = run_to_log(
            "probe command",
            std::process::Command::new("/bin/sh").args(["-c", "echo out; echo err >&2; exit 3"]),
            log.create(),
            REAP.within,
        )
        .expect("sh writes and exits");
        assert_eq!(status.code(), Some(3));
        let written = std::fs::read_to_string(&log.0).expect("read the log");
        assert!(
            written.contains("out\n") && written.contains("err\n"),
            "{written:?}"
        );
    }

    #[test]
    fn a_run_to_log_that_goes_silent_fails_naming_its_hung_process_and_is_ended() {
        let log = LogFile::new("silent");
        let failure = run_to_log(
            "probe command",
            std::process::Command::new("/bin/sh").args(["-c", "echo started; sleep 600"]),
            log.create(),
            Duration::from_millis(50),
        )
        .expect_err("sleep 600 writes nothing and computes nothing within 50 ms");
        let RunError::Silent(run) = &failure else {
            panic!("expected SILENT, got {failure}");
        };
        assert_eq!(failure.code(), "SILENT");
        assert_eq!(run.silent.op, "run to log");
        assert_eq!(run.silent.holder.role, "probe command");
        assert_eq!(run.silent.last, Progress::Received { bytes: 8 });
        let members = run.group.as_ref().expect("the silent run's group");
        assert!(
            members.iter().any(|member| member.command == "sleep 600"),
            "the hung process is named: {failure}"
        );
        assert_eq!(
            crate::process::run_state(libc::pid_t::try_from(run.silent.holder.pid).expect("a pid")),
            RunState::Gone,
            "the silent child was ended and reaped"
        );
        assert_eq!(
            std::fs::read_to_string(&log.0).expect("read the log"),
            "started\n"
        );
    }

    /// A child that spins without writing is working: at a spent silence its group's CPU has
    /// advanced, so the run goes on. The runner's `queued` callback marks each look; the test ends
    /// the child at the second, so the run answering that kill, not `SILENT`, is the proof that
    /// the first look counted the CPU as progress.
    #[test]
    fn a_child_that_spins_in_silence_is_progress_at_each_look() {
        use std::os::unix::process::ExitStatusExt as _;
        let leader = std::cell::Cell::new(None::<libc::pid_t>);
        let looks = std::cell::Cell::new(0_u32);
        let status = run_by_progress(
            "probe command",
            std::process::Command::new("/bin/sh").args(["-c", "echo $$; while :; do :; done"]),
            Duration::from_millis(100),
            &mut |_, bytes| {
                let pid = std::str::from_utf8(bytes)
                    .ok()
                    .and_then(|line| line.trim().parse().ok());
                leader.set(leader.get().or(pid));
                Ok(())
            },
            &mut || {
                looks.set(looks.get() + 1);
                if looks.get() == 2 {
                    let pid = leader.get().expect("the child said its pid before it spun");
                    // SAFETY: `pid` is the run's own unreaped child.
                    assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
                }
                None
            },
        )
        .expect("a spinning child is progress until it is ended");
        assert_eq!(status.signal(), Some(libc::SIGKILL));
        assert_eq!(looks.get(), 2);
    }

    /// What a spent silence decides from the group's CPU at the last look and now, and the live
    /// holder the run waits behind.
    #[test]
    fn a_spent_silence_is_work_queueing_or_silence() {
        let holder = || Some("pid 7 `cargo build`".to_owned());
        let cases = [
            ("CPU advanced", Some(5), Some(6), None, AtSilence::Worked),
            (
                "CPU advanced while queued",
                Some(5),
                Some(6),
                holder(),
                AtSilence::Worked,
            ),
            (
                "no CPU, a holder ahead",
                Some(5),
                Some(5),
                holder(),
                AtSilence::Queued("pid 7 `cargo build`".to_owned()),
            ),
            (
                "no CPU, nobody ahead",
                Some(5),
                Some(5),
                None,
                AtSilence::Silent,
            ),
            (
                "CPU unread, nobody ahead",
                Some(5),
                None,
                None,
                AtSilence::Silent,
            ),
            ("no baseline yet", None, Some(9), None, AtSilence::Silent),
            (
                "CPU unread, a holder ahead",
                None,
                None,
                holder(),
                AtSilence::Queued("pid 7 `cargo build`".to_owned()),
            ),
        ];
        for (case, last, now, queued, expected) in cases {
            assert_eq!(at_silence(last, now, queued), expected, "{case}");
        }
    }

    #[tokio::test]
    async fn a_unix_connect_answers_the_listener_or_names_its_absence() {
        // A short path: a unix socket's address holds about a hundred bytes.
        let directory =
            SocketDirectory(std::env::temp_dir().join(format!("cowshed-c-{}", std::process::id())));
        std::fs::create_dir(&directory.0).expect("create the socket directory");
        let socket = directory.0.join("s.sock");
        let peer = Peer::unix("probe daemon", &socket);
        let absent = connect_unix(&start("connect probe", CONTROL), &peer, &socket)
            .await
            .expect_err("nothing listens yet");
        assert_eq!(absent.code(), "BLOCKED", "{absent}");
        assert_eq!(io::Error::from(absent).kind(), io::ErrorKind::NotFound);
        let _listener = tokio::net::UnixListener::bind(&socket).expect("listen");
        let connected = connect_unix(&start("connect probe", CONTROL), &peer, &socket)
            .await
            .expect("the listener takes the connection");
        assert_eq!(
            peer.clone().credited(&connected).pid,
            PeerPid::Known(libc::pid_t::try_from(std::process::id()).expect("a pid"))
        );
    }

    /// The socket's directory, removed on every exit path.
    struct SocketDirectory(std::path::PathBuf);

    impl Drop for SocketDirectory {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(&self.0)
                && error.kind() != io::ErrorKind::NotFound
            {
                panic!("remove {}: {error}", self.0.display());
            }
        }
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

    #[tokio::test]
    async fn a_tokio_child_collected_later_is_left_running_and_reaped_once_it_exits() {
        use crate::fork_lock::Spawn as _;
        let mut child = tokio::process::Command::new("/bin/sh")
            .args(["-c", "read line"])
            .stdin(std::process::Stdio::piped())
            .kill_on_drop(true)
            .spawn_locked()
            .expect("spawn sh");
        let pid = libc::pid_t::try_from(child.id().expect("a pid")).expect("a pid_t");
        let stdin = child.stdin.take().expect("sh's stdin");
        collect_later_async("probe child", child);
        // Dropped instead of owned, the child would be killed now (`kill_on_drop`).
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            matches!(
                crate::process::run_state(pid),
                RunState::Running | RunState::Sleeping
            ),
            "the child is left running: {}",
            crate::process::run_state(pid)
        );
        // Its stdin closed, sh exits, and the task collects it: no zombie is left. A collection that
        // never comes is a hang the no-progress bound names, not a deadline of this test.
        drop(stdin);
        loop {
            match crate::process::run_state(pid) {
                RunState::Gone => break,
                RunState::Unread(why) => panic!("the child's state is unread: {why}"),
                RunState::Running
                | RunState::Sleeping
                | RunState::Uninterruptible
                | RunState::Stopped
                | RunState::Zombie => tokio::time::sleep(Duration::from_millis(10)).await,
            }
        }
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

    // The blocking twin on a real socketpair: its far end is the test's own, silent or closed as
    // the case needs, so no thread runs and the deadline's clock alone decides.
    fn paired(op: &'static str, bound: Bound) -> (PeerSocket, std::os::unix::net::UnixStream) {
        let (near, far) = std::os::unix::net::UnixStream::pair().expect("a socketpair");
        let peer = Peer::paired("probe holder", 0).credited(&near);
        let socket = PeerSocket::connected(near, peer, start(op, bound)).expect("made nonblocking");
        (socket, far)
    }

    fn own_pid() -> i32 {
        i32::try_from(std::process::id()).expect("a pid")
    }

    #[test]
    fn a_blocking_request_whose_peer_stays_silent_expires_naming_what_was_sent() {
        use std::io::{Read as _, Write as _};
        let (mut socket, far) = paired("status", SHORT);
        assert_eq!(
            socket.peer().pid,
            PeerPid::Known(own_pid()),
            "the pair's maker"
        );
        socket
            .write_all(b"{\"op\":\"status\"}\n")
            .expect("the request is written");
        let mut byte = [0_u8; 1];
        let error = socket.read_exact(&mut byte).expect_err("no answer comes");
        let failure = socket.fail(error);
        drop(far);
        let WaitError::Expired(expired) = &failure else {
            panic!("expected TIMEOUT, got {failure}");
        };
        assert_eq!(expired.op, "status");
        assert_eq!(expired.last, Progress::Sent { bytes: 16 });
        assert_eq!(expired.peer.pid, PeerPid::Known(own_pid()));
        assert!(expired.elapsed >= SHORT.within, "{failure}");
        assert_eq!(failure.code(), "TIMEOUT");
        assert_eq!(failure.retry_class(), RetryClass::Terminal);
    }

    #[test]
    fn a_blocking_held_wait_reads_its_holders_progress_and_fails_silent_after() {
        use std::io::{Read as _, Write as _};
        let (mut socket, mut far) = paired("hold", SHORT);
        let silence = Duration::from_millis(80);
        socket.hold(idle("job terminal", silence));
        far.write_all(b"..").expect("two heartbeats");
        let mut received = [0_u8; 2];
        socket
            .read_exact(&mut received)
            .expect("the holder's progress");
        socket.progressed(Progress::State("running"));
        let error = socket
            .read_exact(&mut received)
            .expect_err("the holder falls silent");
        let failure = socket.fail(error);
        drop(far);
        let WaitError::Silent(silent) = &failure else {
            panic!("expected SILENT, got {failure}");
        };
        assert_eq!(silent.op, "job terminal");
        assert_eq!(silent.last, Progress::State("running"));
        assert!(silent.silent_for >= silence, "{failure}");
        assert_eq!(failure.code(), "SILENT");
    }

    #[test]
    fn a_blocking_wait_whose_peer_closes_before_its_answer_is_lost() {
        use std::io::Read as _;
        let (mut socket, far) = paired("hello", SHORT);
        drop(far);
        let mut byte = [0_u8; 1];
        let error = socket.read_exact(&mut byte).expect_err("the peer is gone");
        let failure = socket.fail(error);
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
        assert_eq!(failure.code(), "PEER_LOST");
    }

    #[test]
    fn a_blocking_connect_with_no_socket_is_blocked_on_that_service() {
        let socket = Path::new("/nonexistent-cowshed-deadline/peer.sock");
        let failure = PeerSocket::connect("workspace supervisor", socket, start("hello", SHORT))
            .expect_err("nothing listens there");
        assert!(
            matches!(&failure, WaitError::Absent { op: "hello", .. }),
            "{failure}"
        );
        assert_eq!(failure.code(), "BLOCKED");
    }

    #[test]
    fn a_blocking_connect_reaches_its_listener_and_names_its_pid() {
        // A socket's path is used as spelled, so it takes the short fixture parent (08_testing.md,
        // "Fixtures"): the workspace's runtime link, else /tmp, never the sandbox's TMPDIR.
        let parent = std::env::var_os("COWSHED_RUNTIME_LINK")
            .map_or_else(|| PathBuf::from("/tmp"), PathBuf::from);
        let unique = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("after the epoch")
            .subsec_nanos();
        let socket = parent.join(format!("cs-ps-{}-{unique}.sock", std::process::id()));
        let listener = std::os::unix::net::UnixListener::bind(&socket).expect("bind the listener");
        let connected = PeerSocket::connect("probe daemon", &socket, start("hello", SHORT));
        std::fs::remove_file(&socket).expect("remove the socket");
        drop(listener);
        let connected = connected.expect("the listener's queue takes the connect");
        assert_eq!(connected.peer().pid, PeerPid::Known(own_pid()));
        assert_eq!(connected.last(), Progress::Connected);
    }
}
