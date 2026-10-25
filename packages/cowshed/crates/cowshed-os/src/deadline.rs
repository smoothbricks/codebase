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
use std::sync::LazyLock;
use std::task::{Context, Poll, ready};
use std::time::Duration;

pub use lmao_core::deadline::{
    Bound, Deadline, Expired, Idle, LatencyClass, Progress, RetryClass, Silent,
};
use lmao_core::{BlockedReason, Clock};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::UnixStream;
use tokio::time::Sleep;

/// One request and its one answer from a running cowshed daemon over a local socket.
pub const CONTROL: Bound = Bound::new(LatencyClass::Control, Duration::from_secs(5));
/// Connecting to a daemon that should already be running, and its hello.
pub const HANDSHAKE: Bound = Bound::new(LatencyClass::Handshake, Duration::from_secs(10));
/// A child's exit once its pipes closed or it was killed.
pub const REAP: Bound = Bound::new(LatencyClass::Reap, Duration::from_secs(5));
/// How often a holder says it is still there on a connection it holds open for a long wait.
pub const HEARTBEAT: Duration = Duration::from_secs(5);
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

/// Start measuring the silence of `op`'s holder now.
pub fn idle(op: &'static str, silence: Duration) -> Idle {
    Idle::start(op, silence, clock())
}

/// Where a peer listens.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PeerAddress {
    Unix(PathBuf),
    Tcp(SocketAddr),
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
        let mut peer = Peer::unix(role, socket);
        let io = establish(&deadline, &peer, UnixStream::connect(socket)).await?;
        peer.pid = match io.peer_cred() {
            Ok(credential) => credential
                .pid()
                .map_or(PeerPid::NotReported, PeerPid::Known),
            Err(error) => PeerPid::Unread(error.to_string()),
        };
        Ok(Self::connected(io, peer, deadline))
    }
}

/// Run `connect` to `peer` within `deadline`: a missing or refusing peer is absent, and a connect
/// still pending at the bound has expired with nothing sent.
pub async fn establish<S>(
    deadline: &Deadline,
    peer: &Peer,
    connect: impl Future<Output = io::Result<S>>,
) -> Result<S, WaitError> {
    match within(deadline, peer, Progress::Nothing, connect).await? {
        Ok(io) => Ok(io),
        Err(source)
            if matches!(
                source.kind(),
                io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
            ) =>
        {
            Err(WaitError::Absent {
                op: deadline.op(),
                peer: Box::new(peer.clone()),
                source,
            })
        }
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

    fn poll_bound<T>(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<T>> {
        ready!(self.timer.as_mut().poll(cx));
        Poll::Ready(Err(io::Error::new(io::ErrorKind::TimedOut, self.expiry())))
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
    use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};

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
        assert_eq!(expired.elapsed, Duration::from_millis(50), "{failure}");
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
        assert_eq!(silent.silent_for, Duration::from_millis(80), "{failure}");
        assert_eq!(silent.elapsed, Duration::from_millis(230), "{failure}");
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
