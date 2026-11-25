//! Land turns: one land of a repository at a time, in the order the lands asked (05_gateway.md
//! "Land turns").
//!
//! A bare `flock(2)` on one lock file serialized lands but never ordered them: every waiter is
//! woken when the holder releases and the first to retry wins, so a land that asks at the moment
//! of a release takes the turn ahead of one that has waited for minutes. Measured 2026-10-21: a
//! 72-commit train queued behind one land, and when that land released, a land that had asked
//! after the train took the turn and held it for 36 minutes.
//!
//! So a land asks the gateway, whose actor keeps every repository's queue: first asked, first
//! served, the head holding the turn. The gateway names the asker by the kernel's identity of its
//! connection's peer and tells it who holds the turn and how many are ahead until the turn is its
//! own. A turn is held by processes, never by descriptors: by its asker until the asker ends it,
//! closes its connection or exits, and by each command the asker binds to it until that command
//! exits. Measured 2026-10-25: tickets held through an inherited `flock(2)` description kept a turn
//! held for as long as any descendant of the land lived, the image cleanup's deletion children
//! among them, and listed tickets whose processes had exited.
//!
//! The head then takes the outside lock, the file lands took before every caller asked for turns.
//! A process outside the queue holding it records nothing, so while the head waits it names the
//! processes with that file open from one descriptor census per report. Measured 2026-10-21: a
//! land waited 1002 s behind an outside holder having said once that "a land that did not ask
//! for a turn" held it, and nothing named the process.

use std::io;
use std::os::fd::RawFd;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine as _;
use cowshed_os::fork_lock::Fenced;
use cowshed_os::process::Holder;
use serde::{Deserialize, Serialize};
use sha2::{Digest as _, Sha256};
use tokio::io::AsyncWriteExt as _;
use tokio::net::UnixStream;
use tokio::sync::{mpsc, oneshot, watch};
use zeroize::Zeroize as _;

use crate::api::dto::LandTurnName;
use crate::census::CensusScope;
use crate::held_lock::{HeldLock, Opened};

/// The gateway control operation that asks for a land turn: the connection that asks it is the
/// asker's place in the queue for as long as it is open.
pub const LAND_TURN_OP: &str = "land-turn";
/// The one-shot gateway control operation that lists every repository's land turns.
pub const LAND_TURNS_OP: &str = "land-turns";
/// The one-shot gateway control operation that withdraws every install currently waiting for the
/// land turns (never one already fenced).
pub const LAND_TURNS_WITHDRAW_OP: &str = "land-turns-withdraw";
/// The gateway control operation that resumes a fenced land turn after a planned gateway restart.
pub const LAND_TURN_RESUME_OP: &str = "land-turn-resume";
/// How long a `"class":"bounded"` land or ref turn may stay held once taken (05_gateway.md "Deadlines").
pub const BOUNDED_TURN_DEADLINE: Duration = Duration::from_secs(120);

/// The directory of every repository's land records: a declared land's `lands.log` and its
/// commands' output (02_workspaces.md "A declared land"), beside the install slot in the host's
/// support directory, which only this user writes. The queues themselves are the gateway's.
pub fn land_turn_root(support_directory: &Path) -> PathBuf {
    support_directory.join("land-turn")
}

/// The class of a land or ref turn ask (05_gateway.md "Land turns").
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub enum TurnClass {
    /// A short, bounded turn (such as `cowshed doc-commit`): may enter behind a non-bounded ticket
    /// while an install waits, parks once only bounded tickets remain, and expires at
    /// [`BOUNDED_TURN_DEADLINE`] once held.
    Bounded,
}

/// Mint a 32-byte cryptographically random resume token (encoded as unpadded base64url) together
/// with its SHA-256 digest (05_gateway.md "Land turns").
pub fn mint_resume_token() -> io::Result<(String, [u8; 32])> {
    let mut raw = [0_u8; 32];
    getrandom::fill(&mut raw).map_err(|error| {
        io::Error::other(format!(
            "cannot generate random bytes for land-turn resume token: {error}"
        ))
    })?;
    let token = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(raw);
    raw.zeroize();
    let digest = hash_resume_token(&token);
    Ok((token, digest))
}

/// SHA-256 digest of a resume token string, stored in gateway memory and the handoff snapshot so
/// the raw token is never persisted or exposed.
pub fn hash_resume_token(token: &str) -> [u8; 32] {
    Sha256::digest(token.as_bytes()).into()
}

/// What a land asks for: its repository's queue, the workspace it lands, and the command its turn
/// runs, which the queue names to whoever waits behind it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TurnAsk {
    pub repo: LandTurnName,
    pub workspace: String,
    pub command: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub class: Option<TurnClass>,
    /// Opt into fencing within one heartbeat and surviving planned gateway restarts. Omitted or
    /// `false` for legacy callers, which are never sent `{"turn":"fence"}`.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub resumable: bool,
}

impl TurnAsk {
    /// Whether this ask requests a bounded turn (`"class":"bounded"`).
    pub const fn is_bounded(&self) -> bool {
        matches!(self.class, Some(TurnClass::Bounded))
    }
}

/// Format a turn deadline in human units: whole seconds as `<n> s`, sub-second or non-whole as
/// `<ms> ms`.
pub fn format_bound(bound_ms: u64) -> String {
    if bound_ms > 0 && bound_ms.is_multiple_of(1000) {
        format!("{} s", bound_ms / 1000)
    } else {
        format!("{bound_ms} ms")
    }
}

/// One land's place in its repository's queue, as the gateway records it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Ticket {
    /// The order the land asked in: lower holds the turn first.
    pub number: u64,
    /// The process that asked for the turn, as the kernel named its connection's peer.
    pub pid: u32,
    /// The workspace the land lands.
    pub workspace: String,
    /// When the land asked, in milliseconds since the Unix epoch.
    pub asked_unix_ms: u64,
    /// The command the turn runs.
    pub command: String,
    /// The commands bound to the turn that have not exited, by pid: each holds the turn until it
    /// exits, and nothing it starts holds it.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub bound: Vec<u32>,
    /// Whether this ticket's asker has acknowledged a fence for a planned gateway restart (or has
    /// already departed, leaving only bound commands).
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub fenced: bool,
}

/// A land that left its queue without ending its turn: its asker closed its connection, was
/// killed or crashed before it recorded how its command ended.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct Abandonment {
    /// When the gateway found it gone, in milliseconds since the Unix epoch.
    pub at_unix_ms: u64,
    pub ticket: Ticket,
    /// Whether it held the turn or still waited when it went.
    pub reason: String,
}

/// What an asker sends on its connection once its turn is held (or queued, for `Fenced`).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "step", rename_all = "kebab-case", deny_unknown_fields)]
pub enum TurnStep {
    /// Bind the command `pid` to the turn: it holds the turn until it exits.
    Bind { pid: u32 },
    /// Acknowledge a gateway fence request (`{"turn":"fence"}`) for a planned restart.
    Fenced,
    /// End the asker's hold, recording how its command ended.
    End { outcome: String },
}

/// What the gateway answers on a land-turn connection.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "turn", rename_all = "kebab-case", deny_unknown_fields)]
pub enum TurnAnswer {
    /// The asker waits behind `ahead` tickets, the first of which, `holder`, holds the turn: said
    /// when it asks and whenever its queue changes.
    Queued {
        ticket: Ticket,
        holder: Ticket,
        ahead: usize,
        /// Delivered once on a resumable ticket's first numbered answer; never repeated or stored
        /// on `Ticket`.
        #[serde(
            default,
            rename = "resumeToken",
            skip_serializing_if = "Option::is_none"
        )]
        resume_token: Option<String>,
    },
    /// The turn is the asker's.
    Held {
        ticket: Ticket,
        /// Delivered once on a resumable ticket's first numbered answer; never repeated or stored
        /// on `Ticket`.
        #[serde(
            default,
            rename = "resumeToken",
            skip_serializing_if = "Option::is_none"
        )]
        resume_token: Option<String>,
    },
    /// The command `pid` holds the turn until it exits.
    Bound { pid: u32 },
    /// The gateway refuses the ask or the step, and why.
    Refused { error: String },
    /// An install waits for the turns or fences them: the ask is parked, never numbered yet. The
    /// asker keeps reading the same connection, which tells it its turn once the gateway numbers it,
    /// or closes as the install's restart drains the gateway; then the asker waits for the install
    /// slot and asks the gateway the install leaves (05_gateway.md "The read fences the turns").
    Fenced,
    /// An install wants to restart the gateway: the resumable asker finishes any in-flight `bind`
    /// or `end` and replies `{"step":"fenced"}` within one heartbeat (`HEARTBEAT`, 5 s).
    Fence,
    /// The install lifted its fence without restarting: the fenced ticket resumes in place on the
    /// same connection without reconnecting.
    Unfenced,
    /// The replacing gateway re-attached the adopted ticket after `land-turn-resume`.
    Resumed { ticket: Ticket },
    /// A heartbeat sent by the gateway while a ticket is fenced on an open connection
    /// (`HEARTBEAT` / `HEARTBEAT_SILENCE`).
    Heartbeat { progress: u64 },
    /// A bounded ticket exceeded its deadline once held: the gateway ends the ticket and closes
    /// its connection, and the holder treats this as a lost turn naming the bound
    /// (05_gateway.md "Deadlines").
    Expired {
        #[serde(rename = "boundMs")]
        bound_ms: u64,
    },
}

/// What a waiting land is told: who holds the turn and how many tickets are ahead of its own.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Wait {
    /// Behind `ahead` tickets, the first of which, `holder`, holds the turn.
    Queued { holder: Ticket, ahead: usize },
    /// At the head, waiting for `lock`, which a process outside the queue holds: a land started
    /// before every caller asked for turns, or anything else that took the file.
    Outside {
        lock: PathBuf,
        holders: OutsideHolders,
    },
    /// The ask met an install that waits for the turns or fences them: it is parked until that
    /// install's fence lifts, or waits for the install holding the slot `lock` when the install's
    /// restart closes the connection, and then asks the gateway that install leaves.
    Install { lock: PathBuf },
}

/// The gateway a land asks for its turn, and the install slot whose installs restart it
/// (05_gateway.md "The read fences the turns"): an ask whose connection is refused, or closed after
/// it was answered `fenced`, while an install holds the slot waits for that install and asks again
/// of the gateway it leaves.
#[derive(Clone, Debug)]
pub struct TurnGateway {
    /// The gateway's control socket.
    pub socket: PathBuf,
    /// The host support directory whose `install.lock` is the install slot.
    pub support: PathBuf,
    /// The census that names the install holding the slot while a land waits for it.
    pub census: CensusScope,
}

impl TurnGateway {
    /// The gateway at `socket`, restarted by this host's installs: the slot in `$HOME`'s support
    /// directory, its holder named by the host store's census.
    pub fn host(socket: PathBuf) -> crate::Result<Self> {
        Ok(Self {
            socket,
            support: crate::host_dirs::support_directory(
                &crate::gateway_sessions::canonical_home()?
            ),
            census: crate::storage::bootstrap::CanonicalRoots::global().census(),
        })
    }

    /// The install slot's lock file.
    pub fn slot(&self) -> PathBuf {
        self.support.join(crate::host_dirs::INSTALL_LOCK_NAME)
    }
}
/// Who holds the outside lock, as one census of the processes with that file open shows it.
/// Darwin names no `flock(2)` owner, so an open descriptor is the evidence, as for `lsof`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum OutsideHolders {
    /// The processes other than the waiting land with the file open, by pid, each once: the
    /// holder is one of them.
    Open(Vec<Holder>),
    /// No other process has it open: the holder let go as the census ran, or is not this user's
    /// to see.
    NoneOpen,
    /// The census could not be taken, and why.
    Unknown(String),
}
/// What the land `me`, waiting for the outside lock with its own descriptor open on it, says holds
/// that lock, given one census `open` of the processes with the file open. Pure: the census is
/// the caller's.
pub fn outside_holders(me: i32, open: Result<Vec<Holder>, String>) -> OutsideHolders {
    match open {
        Err(error) => OutsideHolders::Unknown(error),
        Ok(mut open) => {
            open.retain(|holder| holder.pid != me);
            open.sort_by_key(|holder| holder.pid);
            open.dedup_by_key(|holder| holder.pid);
            if open.is_empty() {
                OutsideHolders::NoneOpen
            } else {
                OutsideHolders::Open(open)
            }
        }
    }
}

/// One buffered reader/writer over a land-turn connection: cancel-safe async line reads and
/// non-blocking drains share one buffer, so no second reader ever splits the stream.
///
/// It is not tokio's `Lines<BufReader<UnixStream>>` plus `UnixStream::try_read`, for two reasons
/// (reviewed 2026-11-25, tokio 1.53.1):
/// - `try_read` is gated on the reactor's cached readiness (`runtime/io/registration.rs:175-177`,
///   which starts at `Ready::EMPTY`, `scheduled_io.rs:176-184`), and on the CLI's current_thread
///   runtime the driver polls the kernel only when the thread parks. `loss()` and `end()` run
///   synchronously right after a step returns, before any park, so `try_read` answers WouldBlock
///   without calling read(2) while the gateway's `expired` line and EOF sit in the kernel buffer:
///   the race sb-batch-4's gate hit. The drain here is a kernel-level `recv(MSG_DONTWAIT)`.
/// - `Lines` keeps a partial line in private state (`io/util/lines.rs:23-29`) and `BufReader` has
///   no push-back (`io/util/buf_reader.rs:58,65` warn against reading the inner stream), so a sync
///   drain beneath a live `Lines` would lose or reorder bytes.
#[derive(Debug)]
struct Connection {
    stream: Fenced<UnixStream>,
    buf: Vec<u8>,
}

#[derive(Debug)]
enum NonBlockingLine {
    Line(String),
    Eof,
    Pending,
}

impl Connection {
    fn new(stream: Fenced<UnixStream>) -> Self {
        #[cfg(target_os = "macos")]
        {
            let on: libc::c_int = 1;
            // SAFETY: `stream` is a live owned Unix socket descriptor and `on` is a live `c_int`.
            unsafe {
                libc::setsockopt(
                    std::os::fd::AsRawFd::as_raw_fd(&*stream),
                    libc::SOL_SOCKET,
                    libc::SO_NOSIGPIPE,
                    std::ptr::from_ref(&on).cast(),
                    libc::socklen_t::try_from(std::mem::size_of::<libc::c_int>()).unwrap_or(4),
                );
            }
        }
        Self {
            stream,
            buf: Vec::new(),
        }
    }

    fn take_buffered_line(&mut self) -> io::Result<Option<String>> {
        let Some(pos) = self.buf.iter().position(|&byte| byte == b'\n') else {
            return Ok(None);
        };
        let mut line: Vec<u8> = self.buf.drain(..=pos).collect();
        line.pop();
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        String::from_utf8(line)
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    fn take_remaining_line(&mut self) -> io::Result<Option<String>> {
        if self.buf.is_empty() {
            return Ok(None);
        }
        let mut line = std::mem::take(&mut self.buf);
        if line.last() == Some(&b'\r') {
            line.pop();
        }
        String::from_utf8(line)
            .map(Some)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
    }

    /// Read directly from the kernel receive queue without blocking and without relying on
    /// Tokio's cached reactor readiness flag.
    fn recv_nonblocking(&self, chunk: &mut [u8]) -> io::Result<usize> {
        let fd = std::os::fd::AsRawFd::as_raw_fd(&*self.stream);
        loop {
            // SAFETY: `fd` is a live owned socket descriptor and `chunk` is a writable stack slice.
            let rc = unsafe {
                libc::recv(
                    fd,
                    chunk.as_mut_ptr().cast(),
                    chunk.len(),
                    libc::MSG_DONTWAIT,
                )
            };
            if let Ok(read) = usize::try_from(rc) {
                return Ok(read);
            }
            let error = io::Error::last_os_error();
            if error.kind() != io::ErrorKind::Interrupted {
                return Err(error);
            }
        }
    }

    /// Cancel-safe async read of the next newline-delimited line, or `Ok(None)` at EOF.
    async fn next_line(&mut self) -> io::Result<Option<String>> {
        let mut chunk = [0_u8; 512];
        loop {
            if let Some(line) = self.take_buffered_line()? {
                return Ok(Some(line));
            }
            self.stream.readable().await?;
            match self.stream.try_read(&mut chunk) {
                Ok(0) => return self.take_remaining_line(),
                Ok(read) => self.buf.extend_from_slice(&chunk[..read]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => continue,
                Err(error) => return Err(error),
            }
        }
    }

    /// Non-blocking drain of one line or EOF already sitting in the buffer or kernel receive queue.
    fn try_next_line(&mut self) -> io::Result<NonBlockingLine> {
        let mut chunk = [0_u8; 512];
        loop {
            if let Some(line) = self.take_buffered_line()? {
                return Ok(NonBlockingLine::Line(line));
            }
            match self.recv_nonblocking(&mut chunk) {
                Ok(0) => {
                    return match self.take_remaining_line()? {
                        Some(line) => Ok(NonBlockingLine::Line(line)),
                        None => Ok(NonBlockingLine::Eof),
                    };
                }
                Ok(read) => self.buf.extend_from_slice(&chunk[..read]),
                Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
                    return Ok(NonBlockingLine::Pending);
                }
                Err(error) => return Err(error),
            }
        }
    }

    fn try_write_all(&self, bytes: &[u8]) -> io::Result<()> {
        let fd = std::os::fd::AsRawFd::as_raw_fd(&*self.stream);
        #[cfg(target_os = "linux")]
        let flags = libc::MSG_DONTWAIT | libc::MSG_NOSIGNAL;
        #[cfg(not(target_os = "linux"))]
        let flags = libc::MSG_DONTWAIT;
        let mut written = 0;
        while written < bytes.len() {
            let rest = &bytes[written..];
            // SAFETY: `fd` is a live owned socket descriptor and `rest` is a valid byte slice.
            let rc = unsafe { libc::send(fd, rest.as_ptr().cast(), rest.len(), flags) };
            match usize::try_from(rc) {
                Ok(0) => {
                    return Err(io::Error::new(
                        io::ErrorKind::WriteZero,
                        format!("{written} of its {} bytes were written", bytes.len()),
                    ));
                }
                Ok(wrote) => written += wrote,
                Err(_) => {
                    let error = io::Error::last_os_error();
                    if error.kind() != io::ErrorKind::Interrupted {
                        return Err(error);
                    }
                }
            }
        }
        Ok(())
    }

    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        self.stream.write_all(bytes).await
    }

    fn raw_fd(&self) -> RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&*self.stream)
    }

    fn into_std(self) -> io::Result<(Fenced<std::os::unix::net::UnixStream>, Vec<u8>)> {
        let Self { stream, buf } = self;
        let std_stream = stream.try_map(UnixStream::into_std)?;
        Ok((std_stream, buf))
    }

    fn from_std(stream: Fenced<std::os::unix::net::UnixStream>, buf: Vec<u8>) -> io::Result<Self> {
        stream.set_nonblocking(true)?;
        let stream = stream.try_map(UnixStream::from_std)?;
        Ok(Self { stream, buf })
    }
}

/// The recorded reason a held turn was lost, shared by `lost()`, `bound()`, `loss()` and `end()`.
#[derive(Clone, Debug, Eq, PartialEq)]
enum Loss {
    Expired {
        bound_ms: u64,
    },
    Closed,
    Unexpected(String),
    Io {
        kind: io::ErrorKind,
        message: String,
    },
}

impl Loss {
    fn from_line(line: String) -> Self {
        match decode(&line) {
            Ok(TurnAnswer::Expired { bound_ms }) => Self::Expired { bound_ms },
            _ => Self::Unexpected(line),
        }
    }

    fn from_io(error: &io::Error) -> Self {
        Self::Io {
            kind: error.kind(),
            message: error.to_string(),
        }
    }

    fn to_io_error(&self) -> io::Error {
        match self {
            Self::Expired { bound_ms } => expired_error(*bound_ms),
            Self::Closed => io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "the gateway closed the connection that holds the turn",
            ),
            Self::Unexpected(line) => io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the gateway sent {line:?} to a held turn that asked nothing"),
            ),
            Self::Io { kind, message } => io::Error::new(*kind, message.clone()),
        }
    }
}

/// Guard returned by [`Turn::prepare_bind`]: keeps any incoming `{"turn":"fence"}` deferred until
/// the child spawned with [`Turn::bind_at_exec`] finishes [`Turn::bound`] (or until this guard is
/// dropped if spawn fails).
#[derive(Debug)]
pub struct BindGuard {
    cancel: Option<(mpsc::UnboundedSender<DriverCmd>, u64)>,
}

impl Drop for BindGuard {
    fn drop(&mut self) {
        if let Some((tx, seq)) = self.cancel.take() {
            let _ = tx.send(DriverCmd::CancelBind { seq });
        }
    }
}

#[derive(Debug)]
struct DirectTurn {
    connection: Connection,
    /// The connection's descriptor, open for as long as `connection` is: a child armed by
    /// [`Turn::bind_at_exec`] writes its bind to it before its exec.
    socket: RawFd,
    /// The first loss reason observed on this turn's connection, reused by every subsequent check.
    loss: Option<Loss>,
}

enum DriverCmd {
    PrepareBind {
        reply: oneshot::Sender<io::Result<(RawFd, u64)>>,
    },
    CancelBind {
        seq: u64,
    },
    Bound {
        pid: u32,
        reply: oneshot::Sender<io::Result<()>>,
    },
    WaitActive {
        reply: oneshot::Sender<io::Result<()>>,
    },
    SyncLoss {
        reply: std::sync::mpsc::Sender<Option<Loss>>,
    },
    End {
        outcome: String,
        reply: std::sync::mpsc::Sender<io::Result<()>>,
    },
}

struct ResumableTurn {
    socket: Arc<AtomicI32>,
    resumed_across_restart: Arc<AtomicBool>,
    loss: Arc<Mutex<Option<Loss>>>,
    loss_rx: watch::Receiver<Option<Loss>>,
    cmd_tx: mpsc::UnboundedSender<DriverCmd>,
}

impl std::fmt::Debug for ResumableTurn {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ResumableTurn")
            .field("socket", &self.socket.load(Ordering::Acquire))
            .field(
                "resumed_across_restart",
                &self.resumed_across_restart.load(Ordering::Acquire),
            )
            .finish_non_exhaustive()
    }
}

#[derive(Debug)]
enum TurnBackend {
    Direct(DirectTurn),
    Resumable(ResumableTurn),
}

/// A held turn, the asker's connection to the gateway. The gateway holds the turn for as long as
/// the asker holds this, or any command bound to it ([`Self::bind_at_exec`]) runs. Ending it records
/// how the land's command ended; dropping it, or this process's exit, ends the asker's hold without
/// an outcome, which the gateway records as an abandonment.
#[derive(Debug)]
pub struct Turn {
    ticket: Ticket,
    outside: Option<HeldLock>,
    backend: TurnBackend,
}

impl Turn {
    fn direct(
        ticket: Ticket,
        connection: Connection,
        socket: RawFd,
        outside: Option<HeldLock>,
    ) -> Self {
        Self {
            ticket,
            outside,
            backend: TurnBackend::Direct(DirectTurn {
                connection,
                socket,
                loss: None,
            }),
        }
    }

    fn resumable(
        ticket: Ticket,
        connection: Connection,
        outside: Option<HeldLock>,
        gateway: TurnGateway,
        resume_token: String,
        resumed_across_restart: bool,
    ) -> io::Result<Self> {
        let initial_fd = connection.raw_fd();
        let (std_stream, buf) = connection.into_std()?;
        let socket = Arc::new(AtomicI32::new(initial_fd));
        let resumed_flag = Arc::new(AtomicBool::new(resumed_across_restart));
        let shared_loss = Arc::new(Mutex::new(None));
        let (loss_tx, loss_rx) = watch::channel(None);
        let (cmd_tx, cmd_rx) = mpsc::unbounded_channel();

        let thread_socket = Arc::clone(&socket);
        let thread_resumed = Arc::clone(&resumed_flag);
        let thread_loss = Arc::clone(&shared_loss);
        let ticket_number = ticket.number;

        std::thread::Builder::new()
            .name("land-turn-driver".to_owned())
            .spawn(move || {
                let runtime = match tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        record_shared_loss(&thread_loss, &loss_tx, Loss::from_io(&error));
                        return;
                    }
                };
                runtime.block_on(run_resumable_driver(
                    std_stream,
                    buf,
                    ticket_number,
                    gateway,
                    resume_token,
                    thread_socket,
                    thread_resumed,
                    thread_loss,
                    loss_tx,
                    cmd_rx,
                ));
            })?;

        Ok(Self {
            ticket,
            outside,
            backend: TurnBackend::Resumable(ResumableTurn {
                socket,
                resumed_across_restart: resumed_flag,
                loss: shared_loss,
                loss_rx,
                cmd_tx,
            }),
        })
    }

    /// The ticket that holds the turn, as the gateway numbered it.
    pub fn ticket(&self) -> &Ticket {
        &self.ticket
    }

    /// Whether this turn resumed across a planned gateway restart (`land-turn-resume`), either
    /// while queued or while held.
    pub fn resumed_across_restart(&self) -> bool {
        match &self.backend {
            TurnBackend::Direct(_) => false,
            TurnBackend::Resumable(resumable) => {
                resumable.resumed_across_restart.load(Ordering::Acquire)
            }
        }
    }

    /// Wait until the turn is active (not mid-fence or mid-restart), or return its loss reason if
    /// the turn was lost.
    pub async fn wait_active(&mut self) -> io::Result<()> {
        match &mut self.backend {
            TurnBackend::Direct(direct) => match direct.loss_check() {
                Some(loss) => Err(loss),
                None => Ok(()),
            },
            TurnBackend::Resumable(resumable) => {
                let (reply, answer) = oneshot::channel();
                if resumable
                    .cmd_tx
                    .send(DriverCmd::WaitActive { reply })
                    .is_err()
                {
                    return Err(resumable.current_loss_or_closed());
                }
                match answer.await {
                    Ok(result) => result,
                    Err(_) => Err(resumable.current_loss_or_closed()),
                }
            }
        }
    }

    /// Wait until the turn is active and pause acknowledging any incoming `{"turn":"fence"}` until
    /// [`Self::bound`] completes (or the returned [`BindGuard`] drops on spawn error), so
    /// [`Self::bind_at_exec`] writes to the live post-resume socket and is never refused with
    /// `fenced_bind_refused`.
    pub async fn prepare_bind(&mut self) -> io::Result<BindGuard> {
        match &mut self.backend {
            TurnBackend::Direct(direct) => match direct.loss_check() {
                Some(loss) => Err(loss),
                None => Ok(BindGuard { cancel: None }),
            },
            TurnBackend::Resumable(resumable) => {
                let (reply, answer) = oneshot::channel();
                if resumable
                    .cmd_tx
                    .send(DriverCmd::PrepareBind { reply })
                    .is_err()
                {
                    return Err(resumable.current_loss_or_closed());
                }
                let (_fd, seq) = match answer.await {
                    Ok(result) => result?,
                    Err(_) => return Err(resumable.current_loss_or_closed()),
                };
                Ok(BindGuard {
                    cancel: Some((resumable.cmd_tx.clone(), seq)),
                })
            }
        }
    }

    /// Arm `command` to bind itself to the turn as it starts: between its fork and its exec the
    /// child writes the bind step naming its own pid on the turn's connection, which it shares
    /// until its exec closes it (close-on-exec). The gateway reads that step before this process's
    /// end, which follows it on the connection, so a land killed at any point after the fork leaves
    /// the command holding the turn, and one killed before the fork started nothing. A child that
    /// cannot write the step fails before its exec, so its spawn fails and the command never runs.
    /// The gateway's answer is read by [`Self::bound`] once the spawn has returned; a spawn that
    /// fails after the child wrote its step leaves that answer unread, so its caller ends the turn.
    pub fn bind_at_exec(&self, command: &mut std::process::Command) {
        use std::os::unix::process::CommandExt as _;

        let socket = match &self.backend {
            TurnBackend::Direct(direct) => direct.socket,
            TurnBackend::Resumable(resumable) => resumable.socket.load(Ordering::Acquire),
        };
        // SAFETY: the hook runs in the forked child before its exec and calls only getpid and
        // write(2), both async-signal-safe, on a stack buffer; it allocates nothing.
        unsafe {
            command.pre_exec(move || write_bind(socket, std::process::id()));
        }
    }

    /// Return the turn's loss reason if the turn is already lost, draining without blocking any
    /// answer (such as `expired`) or EOF the gateway already wrote before closing the connection.
    /// Any unread `bound` or `refused` reply from a child whose `exec` failed after `write_bind`
    /// is skipped rather than treated as a loss. Once recorded, every later call to `loss()`,
    /// `lost()`, `bound()`, or `end()` reports that same reason.
    pub fn loss(&mut self) -> Option<io::Error> {
        match &mut self.backend {
            TurnBackend::Direct(direct) => direct.loss_check(),
            TurnBackend::Resumable(resumable) => {
                if let Some(loss) = resumable.recorded_loss() {
                    return Some(loss.to_io_error());
                }
                let (reply, answer) = std::sync::mpsc::channel();
                if resumable
                    .cmd_tx
                    .send(DriverCmd::SyncLoss { reply })
                    .is_err()
                {
                    return Some(resumable.current_loss_or_closed());
                }
                match answer.recv() {
                    Ok(Some(loss)) => Some(loss.to_io_error()),
                    Ok(None) => None,
                    Err(_) => Some(resumable.current_loss_or_closed()),
                }
            }
        }
    }

    /// The gateway's answer to the bind the command `pid`, armed by [`Self::bind_at_exec`] and
    /// just spawned, wrote: `Ok` once the gateway holds the turn for it until it exits. A command
    /// that exited before the gateway could watch it holds nothing.
    pub async fn bound(&mut self, pid: u32) -> io::Result<()> {
        match &mut self.backend {
            TurnBackend::Direct(direct) => direct.bound(self.ticket.number, pid).await,
            TurnBackend::Resumable(resumable) => {
                let (reply, answer) = oneshot::channel();
                if resumable
                    .cmd_tx
                    .send(DriverCmd::Bound { pid, reply })
                    .is_err()
                {
                    return Err(resumable.current_loss_or_closed());
                }
                match answer.await {
                    Ok(result) => result,
                    Err(_) => Err(resumable.current_loss_or_closed()),
                }
            }
        }
    }

    /// End the asker's hold, recording `outcome` (how the command ended). The turn passes on once
    /// every command bound to it has exited too. If the gateway already ended or closed the turn
    /// (including an unread `expired` answer waiting on the connection when send fails with EPIPE),
    /// that loss reason is returned and reported on stderr rather than an abandonment.
    pub fn end(self, outcome: String) -> io::Result<()> {
        let ticket_number = self.ticket.number;
        match self.backend {
            TurnBackend::Direct(direct) => direct.end(ticket_number, outcome),
            TurnBackend::Resumable(resumable) => {
                let already_known = resumable.recorded_loss().is_some();
                let (reply, answer) = std::sync::mpsc::channel();
                if resumable
                    .cmd_tx
                    .send(DriverCmd::End {
                        outcome: outcome.clone(),
                        reply,
                    })
                    .is_err()
                {
                    let loss = resumable.current_loss_or_closed();
                    if !already_known {
                        cowshed_os::eprintln!(
                            "cowshed: land turn ticket {ticket_number}: its end ({outcome}) was \
                             not sent: {loss}",
                        );
                    }
                    return Err(loss);
                }
                match answer.recv() {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(error)) => {
                        if !already_known {
                            cowshed_os::eprintln!(
                                "cowshed: land turn ticket {ticket_number}: its end ({outcome}) \
                                 was not sent: {error}",
                            );
                        }
                        Err(error)
                    }
                    Err(_) => {
                        let loss = resumable.current_loss_or_closed();
                        if !already_known {
                            cowshed_os::eprintln!(
                                "cowshed: land turn ticket {ticket_number}: its end ({outcome}) \
                                 was not sent: {loss}",
                            );
                        }
                        Err(loss)
                    }
                }
            }
        }
    }

    /// The outside lock's descriptor, for the command to inherit
    /// ([`crate::held_lock::keep_across_exec`]): the kernel keeps that lock until the command, and
    /// every process it hands the descriptor to, has closed it.
    pub fn outside_descriptor(&self) -> Option<RawFd> {
        self.outside.as_ref().map(HeldLock::descriptor)
    }

    /// Resolves once the gateway has closed the connection that holds the turn unfenced, or a
    /// fenced turn fails to resume or times out, and never before: the turn is lost with it.
    /// Cancel-safe, so a holder waits on it beside its command.
    pub async fn lost(&mut self) -> io::Error {
        match &mut self.backend {
            TurnBackend::Direct(direct) => direct.lost().await,
            TurnBackend::Resumable(resumable) => loop {
                if let Some(loss) = resumable.loss_rx.borrow().clone() {
                    return loss.to_io_error();
                }
                if resumable.loss_rx.changed().await.is_err() {
                    return resumable.current_loss_or_closed();
                }
            },
        }
    }
}

impl DirectTurn {
    fn loss_check(&mut self) -> Option<io::Error> {
        if let Some(loss) = &self.loss {
            return Some(loss.to_io_error());
        }
        let loss = loop {
            match self.connection.try_next_line() {
                Ok(NonBlockingLine::Pending) => return None,
                Ok(NonBlockingLine::Line(line)) => match decode(&line) {
                    Ok(TurnAnswer::Expired { bound_ms }) => break Loss::Expired { bound_ms },
                    Ok(TurnAnswer::Bound { .. } | TurnAnswer::Refused { .. }) => continue,
                    _ => break Loss::Unexpected(line),
                },
                Ok(NonBlockingLine::Eof) => break Loss::Closed,
                Err(error) => break Loss::from_io(&error),
            }
        };
        let io_error = loss.to_io_error();
        self.loss = Some(loss);
        Some(io_error)
    }

    async fn bound(&mut self, ticket_number: u64, pid: u32) -> io::Result<()> {
        if let Some(loss) = &self.loss {
            return Err(loss.to_io_error());
        }
        match answer(&mut self.connection).await {
            Ok(TurnAnswer::Bound { pid: bound }) if bound == pid => Ok(()),
            Ok(TurnAnswer::Expired { bound_ms }) => {
                let loss = Loss::Expired { bound_ms };
                let error = loss.to_io_error();
                self.loss = Some(loss);
                Err(error)
            }
            Ok(TurnAnswer::Refused { error }) => Err(io::Error::other(format!(
                "the gateway refused to bind pid {pid} to land turn ticket {ticket_number}: \
                 {error}"
            ))),
            Ok(other) => Err(unexpected(&other, "a bind")),
            Err(error) => {
                let loss = if error.kind() == io::ErrorKind::UnexpectedEof {
                    Loss::Closed
                } else {
                    Loss::from_io(&error)
                };
                let io_error = loss.to_io_error();
                self.loss = Some(loss);
                Err(io_error)
            }
        }
    }

    fn end(mut self, ticket_number: u64, outcome: String) -> io::Result<()> {
        let already_known = self.loss.is_some();
        if let Some(loss) = self.loss_check() {
            if !already_known {
                cowshed_os::eprintln!(
                    "cowshed: land turn ticket {ticket_number}: its end ({outcome}) was not sent: \
                     {loss}",
                );
            }
            return Err(loss);
        }
        let sent = line(&TurnStep::End {
            outcome: outcome.clone(),
        })
        .and_then(|line| self.connection.try_write_all(&line));
        if let Err(error) = sent {
            if let Some(loss) = self.loss_check() {
                cowshed_os::eprintln!(
                    "cowshed: land turn ticket {ticket_number}: its end ({outcome}) was not sent: \
                     {loss}",
                );
                return Err(loss);
            }
            cowshed_os::eprintln!(
                "cowshed: land turn ticket {ticket_number}: its end ({outcome}) was not sent to \
                 the gateway: {error}; the gateway records the turn abandoned",
            );
            return Err(error);
        }
        Ok(())
    }

    async fn lost(&mut self) -> io::Error {
        if let Some(loss) = &self.loss {
            return loss.to_io_error();
        }
        let loss = match self.connection.next_line().await {
            Ok(None) => Loss::Closed,
            Ok(Some(line)) => Loss::from_line(line),
            Err(error) => Loss::from_io(&error),
        };
        let io_error = loss.to_io_error();
        self.loss = Some(loss);
        io_error
    }
}

impl ResumableTurn {
    fn recorded_loss(&self) -> Option<Loss> {
        self.loss
            .lock()
            .expect("land-turn loss lock poisoned")
            .clone()
    }

    fn current_loss_or_closed(&self) -> io::Error {
        self.recorded_loss().unwrap_or(Loss::Closed).to_io_error()
    }
}

fn record_shared_loss(
    shared: &Arc<Mutex<Option<Loss>>>,
    loss_tx: &watch::Sender<Option<Loss>>,
    loss: Loss,
) -> Loss {
    let mut guard = shared.lock().expect("land-turn loss lock poisoned");
    if let Some(existing) = guard.as_ref() {
        return existing.clone();
    }
    *guard = Some(loss.clone());
    drop(guard);
    loss_tx.send_replace(Some(loss.clone()));
    loss
}

/// Connect to `gateway` after a fenced drain, waiting for any install holding `install.lock`,
/// then connecting within `HANDSHAKE`, sending `land-turn-resume` within `CONTROL`, and reading
/// the resume answer within `HEARTBEAT_SILENCE`.
async fn reconnect_and_resume(
    gateway: &TurnGateway,
    ticket_number: u64,
    resume_token: &str,
) -> io::Result<(Connection, RawFd, TurnAnswer)> {
    #[derive(Serialize)]
    struct Resuming<'a> {
        op: &'static str,
        ticket: u64,
        token: &'a str,
    }

    let wait_install = |gateway: &TurnGateway| {
        let (census, support) = (gateway.census.clone(), gateway.support.clone());
        async move {
            tokio::task::spawn_blocking(move || {
                crate::host_dirs::wait_for_running_install(&census, &support)
            })
            .await
            .map_err(io::Error::other)?
        }
    };

    wait_install(gateway).await?;

    let socket = gateway.socket.as_path();
    let deadline = tokio::time::Instant::now() + cowshed_os::deadline::HANDSHAKE.within;
    let stream = loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "cannot reach the gateway at {} within {} s to resume land turn ticket \
                     {ticket_number}",
                    socket.display(),
                    cowshed_os::deadline::HANDSHAKE.within.as_secs(),
                ),
            ));
        }
        let budget = cowshed_os::deadline::start(
            "land-turn-resume connect",
            cowshed_os::deadline::Bound::new(
                cowshed_os::deadline::LatencyClass::Handshake,
                remaining,
            ),
        );
        let peer = cowshed_os::deadline::Peer::unix("gateway", socket);
        match cowshed_os::deadline::connect_unix(&budget, &peer, socket).await {
            Ok(stream) => break stream,
            Err(wait_error) => {
                let error = io::Error::from(wait_error);
                if matches!(
                    error.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
                ) {
                    if wait_install(gateway).await? {
                        continue;
                    }
                    let pause = Duration::from_millis(25)
                        .min(deadline.saturating_duration_since(tokio::time::Instant::now()));
                    if !pause.is_zero() {
                        tokio::time::sleep(pause).await;
                        continue;
                    }
                }
                return Err(io::Error::new(
                    error.kind(),
                    format!(
                        "cannot reach the gateway at {} to resume land turn ticket \
                         {ticket_number}: {error}",
                        socket.display()
                    ),
                ));
            }
        }
    };
    let descriptor = std::os::fd::AsRawFd::as_raw_fd(&stream);
    let mut connection = Connection::new(Fenced::new(stream));
    let payload = line(&Resuming {
        op: LAND_TURN_RESUME_OP,
        ticket: ticket_number,
        token: resume_token,
    })?;
    tokio::time::timeout(
        cowshed_os::deadline::CONTROL.within,
        connection.write_all(&payload),
    )
    .await
    .map_err(|_| {
        io::Error::new(
            io::ErrorKind::TimedOut,
            format!(
                "timed out writing land-turn-resume for ticket {ticket_number} to {}",
                socket.display()
            ),
        )
    })??;
    loop {
        let next = tokio::time::timeout(
            cowshed_os::deadline::HEARTBEAT_SILENCE,
            answer(&mut connection),
        )
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                format!(
                    "the gateway at {} went silent answering land-turn-resume for ticket \
                     {ticket_number}",
                    socket.display()
                ),
            )
        })??;
        match next {
            TurnAnswer::Heartbeat { .. } => continue,
            TurnAnswer::Resumed { .. } | TurnAnswer::Held { .. } | TurnAnswer::Queued { .. } => {
                return Ok((connection, descriptor, next));
            }
            TurnAnswer::Refused { error } => {
                return Err(io::Error::other(format!(
                    "the gateway at {} refused to resume land turn ticket {ticket_number}: \
                     {error}",
                    socket.display()
                )));
            }
            other => return Err(unexpected(&other, "a resume")),
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
async fn run_resumable_driver(
    std_stream: Fenced<std::os::unix::net::UnixStream>,
    buf: Vec<u8>,
    ticket_number: u64,
    gateway: TurnGateway,
    resume_token: String,
    socket_fd: Arc<AtomicI32>,
    resumed_flag: Arc<AtomicBool>,
    shared_loss: Arc<Mutex<Option<Loss>>>,
    loss_tx: watch::Sender<Option<Loss>>,
    mut cmd_rx: mpsc::UnboundedReceiver<DriverCmd>,
) {
    let mut connection = match Connection::from_std(std_stream, buf) {
        Ok(connection) => connection,
        Err(error) => {
            record_shared_loss(&shared_loss, &loss_tx, Loss::from_io(&error));
            return;
        }
    };
    socket_fd.store(connection.raw_fd(), Ordering::Release);

    let mut fenced = false;
    let mut bind_in_flight: Option<u64> = None;
    let mut next_bind_seq: u64 = 1;
    let mut pending_fence = false;
    let mut pending_bind_answers: std::collections::VecDeque<TurnAnswer> =
        std::collections::VecDeque::new();
    let mut deferred_prepare_bind: Vec<oneshot::Sender<io::Result<(RawFd, u64)>>> = Vec::new();
    let mut deferred_wait_active: Vec<oneshot::Sender<io::Result<()>>> = Vec::new();
    let mut deferred_end: Option<(String, std::sync::mpsc::Sender<io::Result<()>>)> = None;
    let mut silence = Box::pin(tokio::time::sleep(cowshed_os::deadline::HEARTBEAT_SILENCE));

    let fail_all =
        |loss: Loss,
         deferred_prepare_bind: &mut Vec<oneshot::Sender<io::Result<(RawFd, u64)>>>,
         deferred_wait_active: &mut Vec<oneshot::Sender<io::Result<()>>>,
         deferred_end: &mut Option<(String, std::sync::mpsc::Sender<io::Result<()>>)>| {
            let loss = record_shared_loss(&shared_loss, &loss_tx, loss);
            for reply in deferred_prepare_bind.drain(..) {
                let _ = reply.send(Err(loss.to_io_error()));
            }
            for reply in deferred_wait_active.drain(..) {
                let _ = reply.send(Err(loss.to_io_error()));
            }
            if let Some((_, reply)) = deferred_end.take() {
                let _ = reply.send(Err(loss.to_io_error()));
            }
        };

    let ack_fence = async |connection: &mut Connection| -> Result<(), Loss> {
        let payload = line(&TurnStep::Fenced).map_err(|error| Loss::from_io(&error))?;
        connection
            .write_all(&payload)
            .await
            .map_err(|error| Loss::from_io(&error))
    };

    loop {
        if !fenced {
            for reply in deferred_wait_active.drain(..) {
                let _ = reply.send(Ok(()));
            }
            if bind_in_flight.is_none()
                && let Some(reply) = deferred_prepare_bind.pop()
            {
                let seq = next_bind_seq;
                next_bind_seq += 1;
                bind_in_flight = Some(seq);
                pending_bind_answers.clear();
                let _ = reply.send(Ok((connection.raw_fd(), seq)));
            }
            if let Some((outcome, reply)) = deferred_end.take() {
                let result = finish_active_end(&mut connection, outcome).await;
                if let Err(loss) = &result {
                    let loss = record_shared_loss(&shared_loss, &loss_tx, loss.clone());
                    let _ = reply.send(Err(loss.to_io_error()));
                } else {
                    let _ = reply.send(Ok(()));
                }
                return;
            }
        }

        tokio::select! {
            biased;
            cmd = cmd_rx.recv() => {
                let Some(cmd) = cmd else { return };
                match cmd {
                    DriverCmd::PrepareBind { reply } => {
                        if fenced {
                            deferred_prepare_bind.push(reply);
                        } else {
                            let seq = next_bind_seq;
                            next_bind_seq += 1;
                            bind_in_flight = Some(seq);
                            pending_bind_answers.clear();
                            let _ = reply.send(Ok((connection.raw_fd(), seq)));
                        }
                    }
                    DriverCmd::CancelBind { seq } => {
                        if bind_in_flight == Some(seq) {
                            bind_in_flight = None;
                            pending_bind_answers.clear();
                            if pending_fence && !fenced {
                                pending_fence = false;
                                if let Err(loss) = ack_fence(&mut connection).await {
                                    fail_all(
                                        loss,
                                        &mut deferred_prepare_bind,
                                        &mut deferred_wait_active,
                                        &mut deferred_end,
                                    );
                                    return;
                                }
                                fenced = true;
                                silence.as_mut().reset(
                                    tokio::time::Instant::now()
                                        + cowshed_os::deadline::HEARTBEAT_SILENCE,
                                );
                            }
                        }
                    }
                    DriverCmd::WaitActive { reply } => {
                        if fenced {
                            deferred_wait_active.push(reply);
                        } else {
                            let _ = reply.send(Ok(()));
                        }
                    }
                    DriverCmd::SyncLoss { reply } => {
                        if fenced {
                            let _ = reply.send(None);
                            continue;
                        }
                        let mut found = None;
                        loop {
                            match connection.try_next_line() {
                                Ok(NonBlockingLine::Pending) => break,
                                Ok(NonBlockingLine::Line(line)) => match decode(&line) {
                                    Ok(TurnAnswer::Expired { bound_ms }) => {
                                        found = Some(Loss::Expired { bound_ms });
                                        break;
                                    }
                                    Ok(TurnAnswer::Fence) => {
                                        if bind_in_flight.is_some() {
                                            pending_fence = true;
                                        } else if let Err(loss) = ack_fence(&mut connection).await {
                                            found = Some(loss);
                                            break;
                                        } else {
                                            fenced = true;
                                            silence.as_mut().reset(
                                                tokio::time::Instant::now()
                                                    + cowshed_os::deadline::HEARTBEAT_SILENCE,
                                            );
                                            break;
                                        }
                                    }
                                    Ok(
                                        TurnAnswer::Bound { .. }
                                        | TurnAnswer::Refused { .. }
                                        | TurnAnswer::Heartbeat { .. }
                                        | TurnAnswer::Unfenced,
                                    ) => continue,
                                    _ => {
                                        found = Some(Loss::Unexpected(line));
                                        break;
                                    }
                                },
                                Ok(NonBlockingLine::Eof) => {
                                    found = Some(Loss::Closed);
                                    break;
                                }
                                Err(error) => {
                                    found = Some(Loss::from_io(&error));
                                    break;
                                }
                            }
                        }
                        if let Some(loss) = found {
                            fail_all(
                                loss.clone(),
                                &mut deferred_prepare_bind,
                                &mut deferred_wait_active,
                                &mut deferred_end,
                            );
                            let _ = reply.send(Some(loss));
                            return;
                        }
                        let _ = reply.send(None);
                    }
                    DriverCmd::Bound { pid, reply } => {
                        let mut resolved = None;
                        while let Some(queued) = pending_bind_answers.pop_front() {
                            match queued {
                                TurnAnswer::Bound { pid: bound } if bound == pid => {
                                    resolved = Some(Ok(()));
                                    break;
                                }
                                TurnAnswer::Bound { .. } => {}
                                TurnAnswer::Refused { error } => {
                                    resolved = Some(Err(io::Error::other(format!(
                                        "the gateway refused to bind pid {pid} to land turn \
                                         ticket {ticket_number}: {error}"
                                    ))));
                                    break;
                                }
                                _ => {}
                            }
                        }
                        let outcome = match resolved {
                            Some(result) => result,
                            None => loop {
                                match answer(&mut connection).await {
                                    Ok(TurnAnswer::Bound { pid: bound }) if bound == pid => {
                                        break Ok(());
                                    }
                                    Ok(TurnAnswer::Bound { .. }) => {}
                                    Ok(TurnAnswer::Fence) => {
                                        pending_fence = true;
                                    }
                                    Ok(TurnAnswer::Heartbeat { .. }) => {}
                                    Ok(TurnAnswer::Expired { bound_ms }) => {
                                        let loss = Loss::Expired { bound_ms };
                                        fail_all(
                                            loss.clone(),
                                            &mut deferred_prepare_bind,
                                            &mut deferred_wait_active,
                                            &mut deferred_end,
                                        );
                                        let _ = reply.send(Err(loss.to_io_error()));
                                        return;
                                    }
                                    Ok(TurnAnswer::Refused { error }) => {
                                        break Err(io::Error::other(format!(
                                            "the gateway refused to bind pid {pid} to land turn \
                                             ticket {ticket_number}: {error}"
                                        )));
                                    }
                                    Ok(other) => break Err(unexpected(&other, "a bind")),
                                    Err(error) => {
                                        let loss = if error.kind() == io::ErrorKind::UnexpectedEof {
                                            Loss::Closed
                                        } else {
                                            Loss::from_io(&error)
                                        };
                                        fail_all(
                                            loss.clone(),
                                            &mut deferred_prepare_bind,
                                            &mut deferred_wait_active,
                                            &mut deferred_end,
                                        );
                                        let _ = reply.send(Err(loss.to_io_error()));
                                        return;
                                    }
                                }
                            },
                        };
                        bind_in_flight = None;
                        if pending_fence && !fenced {
                            pending_fence = false;
                            if let Err(loss) = ack_fence(&mut connection).await {
                                fail_all(
                                    loss,
                                    &mut deferred_prepare_bind,
                                    &mut deferred_wait_active,
                                    &mut deferred_end,
                                );
                                let _ = reply.send(outcome);
                                return;
                            }
                            fenced = true;
                            silence.as_mut().reset(
                                tokio::time::Instant::now()
                                    + cowshed_os::deadline::HEARTBEAT_SILENCE,
                            );
                        }
                        let _ = reply.send(outcome);
                    }
                    DriverCmd::End { outcome, reply } => {
                        if fenced {
                            deferred_end = Some((outcome, reply));
                        } else {
                            let result = finish_active_end(&mut connection, outcome).await;
                            if let Err(loss) = &result {
                                let loss =
                                    record_shared_loss(&shared_loss, &loss_tx, loss.clone());
                                let _ = reply.send(Err(loss.to_io_error()));
                            } else {
                                let _ = reply.send(Ok(()));
                            }
                            return;
                        }
                    }
                }
            }
            next = connection.next_line() => {
                match next {
                    Ok(Some(line)) => {
                        if fenced {
                            silence.as_mut().reset(
                                tokio::time::Instant::now()
                                    + cowshed_os::deadline::HEARTBEAT_SILENCE,
                            );
                        }
                        match decode(&line) {
                            Ok(TurnAnswer::Fence) => {
                                if bind_in_flight.is_some() {
                                    pending_fence = true;
                                } else if !fenced {
                                    if let Err(loss) = ack_fence(&mut connection).await {
                                        fail_all(
                                            loss,
                                            &mut deferred_prepare_bind,
                                            &mut deferred_wait_active,
                                            &mut deferred_end,
                                        );
                                        return;
                                    }
                                    fenced = true;
                                    silence.as_mut().reset(
                                        tokio::time::Instant::now()
                                            + cowshed_os::deadline::HEARTBEAT_SILENCE,
                                    );
                                }
                            }
                            Ok(TurnAnswer::Heartbeat { .. }) => {}
                            Ok(TurnAnswer::Unfenced) => {
                                fenced = false;
                            }
                            Ok(TurnAnswer::Expired { bound_ms }) => {
                                fail_all(
                                    Loss::Expired { bound_ms },
                                    &mut deferred_prepare_bind,
                                    &mut deferred_wait_active,
                                    &mut deferred_end,
                                );
                                return;
                            }
                            Ok(answer @ (TurnAnswer::Bound { .. } | TurnAnswer::Refused { .. })) => {
                                if pending_bind_answers.len() < 16 {
                                    pending_bind_answers.push_back(answer);
                                }
                            }
                            _ => {
                                fail_all(
                                    Loss::Unexpected(line),
                                    &mut deferred_prepare_bind,
                                    &mut deferred_wait_active,
                                    &mut deferred_end,
                                );
                                return;
                            }
                        }
                    }
                    Ok(None) if fenced => {
                        socket_fd.store(-1, Ordering::Release);
                        let mut resuming = std::pin::pin!(reconnect_and_resume(
                            &gateway,
                            ticket_number,
                            &resume_token,
                        ));
                        let reconnected = loop {
                            tokio::select! {
                                result = &mut resuming => break result,
                                cmd = cmd_rx.recv() => {
                                    let Some(cmd) = cmd else { return };
                                    match cmd {
                                        DriverCmd::PrepareBind { reply } => {
                                            deferred_prepare_bind.push(reply);
                                        }
                                        DriverCmd::CancelBind { .. } => {}
                                        DriverCmd::WaitActive { reply } => {
                                            deferred_wait_active.push(reply);
                                        }
                                        DriverCmd::SyncLoss { reply } => {
                                            let _ = reply.send(None);
                                        }
                                        DriverCmd::End { outcome, reply } => {
                                            deferred_end = Some((outcome, reply));
                                        }
                                        DriverCmd::Bound { pid, reply } => {
                                            let _ = reply.send(Err(io::Error::other(format!(
                                                "cannot bind pid {pid} while land turn ticket \
                                                 {ticket_number} is resuming across a gateway \
                                                 restart"
                                            ))));
                                        }
                                    }
                                }
                            }
                        };
                        match reconnected {
                            Ok((new_conn, new_fd, _answer)) => {
                                connection = new_conn;
                                socket_fd.store(new_fd, Ordering::Release);
                                resumed_flag.store(true, Ordering::Release);
                                fenced = false;
                            }
                            Err(error) => {
                                fail_all(
                                    Loss::from_io(&error),
                                    &mut deferred_prepare_bind,
                                    &mut deferred_wait_active,
                                    &mut deferred_end,
                                );
                                return;
                            }
                        }
                    }
                    Ok(None) => {
                        fail_all(
                            Loss::Closed,
                            &mut deferred_prepare_bind,
                            &mut deferred_wait_active,
                            &mut deferred_end,
                        );
                        return;
                    }
                    Err(error) => {
                        fail_all(
                            Loss::from_io(&error),
                            &mut deferred_prepare_bind,
                            &mut deferred_wait_active,
                            &mut deferred_end,
                        );
                        return;
                    }
                }
            }
            () = &mut silence, if fenced => {
                fail_all(
                    Loss::Io {
                        kind: io::ErrorKind::TimedOut,
                        message: "the gateway went silent while this land turn was fenced"
                            .to_owned(),
                    },
                    &mut deferred_prepare_bind,
                    &mut deferred_wait_active,
                    &mut deferred_end,
                );
                return;
            }
        }
    }
}

async fn finish_active_end(connection: &mut Connection, outcome: String) -> Result<(), Loss> {
    loop {
        match connection.try_next_line() {
            Ok(NonBlockingLine::Pending) => break,
            Ok(NonBlockingLine::Line(line)) => match decode(&line) {
                Ok(TurnAnswer::Expired { bound_ms }) => return Err(Loss::Expired { bound_ms }),
                Ok(
                    TurnAnswer::Bound { .. }
                    | TurnAnswer::Refused { .. }
                    | TurnAnswer::Fence
                    | TurnAnswer::Heartbeat { .. }
                    | TurnAnswer::Unfenced,
                ) => continue,
                _ => return Err(Loss::Unexpected(line)),
            },
            Ok(NonBlockingLine::Eof) => return Err(Loss::Closed),
            Err(error) => return Err(Loss::from_io(&error)),
        }
    }
    let payload = line(&TurnStep::End { outcome }).map_err(|error| Loss::from_io(&error))?;
    let sent = match connection.try_write_all(&payload) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            connection.write_all(&payload).await
        }
        Err(error) => Err(error),
    };
    if let Err(error) = sent {
        loop {
            match connection.try_next_line() {
                Ok(NonBlockingLine::Pending) => return Err(Loss::from_io(&error)),
                Ok(NonBlockingLine::Line(line)) => match decode(&line) {
                    Ok(TurnAnswer::Expired { bound_ms }) => {
                        return Err(Loss::Expired { bound_ms });
                    }
                    Ok(
                        TurnAnswer::Bound { .. }
                        | TurnAnswer::Refused { .. }
                        | TurnAnswer::Fence
                        | TurnAnswer::Heartbeat { .. }
                        | TurnAnswer::Unfenced,
                    ) => continue,
                    _ => return Err(Loss::Unexpected(line)),
                },
                Ok(NonBlockingLine::Eof) => return Err(Loss::Closed),
                Err(_) => return Err(Loss::from_io(&error)),
            }
        }
    }
    Ok(())
}

/// One JSON line.
fn line(value: &impl Serialize) -> io::Result<Vec<u8>> {
    let mut line = serde_json::to_vec(value).map_err(io::Error::other)?;
    line.push(b'\n');
    Ok(line)
}

async fn send(stream: &mut Fenced<UnixStream>, value: &impl Serialize) -> io::Result<()> {
    stream.write_all(&line(value)?).await
}

/// Write the bind step of `pid`, `{"step":"bind","pid":<pid>}` and a newline, to `socket`, with
/// no allocation on any path: it runs in a forked child before its exec ([`Turn::bind_at_exec`]),
/// and a failed write answers the kernel's errno ([`io::Error::last_os_error`], stored inline),
/// which the spawn reports to the parent.
fn write_bind(socket: RawFd, pid: u32) -> io::Result<()> {
    const PREFIX: &[u8] = br#"{"step":"bind","pid":"#;
    let mut line = [0_u8; PREFIX.len() + 10 + 2];
    line[..PREFIX.len()].copy_from_slice(PREFIX);
    let mut digits = [0_u8; 10];
    let mut count = 0;
    let mut rest = pid;
    loop {
        // The remainder is below ten, so its low byte is the digit.
        digits[count] = b'0' + (rest % 10).to_le_bytes()[0];
        count += 1;
        rest /= 10;
        if rest == 0 {
            break;
        }
    }
    let mut end = PREFIX.len();
    for digit in digits[..count].iter().rev() {
        line[end] = *digit;
        end += 1;
    }
    line[end..end + 2].copy_from_slice(b"}\n");
    end += 2;
    let mut written = 0;
    while written < end {
        let rest = &line[written..end];
        // SAFETY: `rest` is a live stack buffer of `rest.len()` bytes; `send`/`write` are
        // async-signal-safe per POSIX.
        #[cfg(target_os = "linux")]
        let wrote =
            unsafe { libc::send(socket, rest.as_ptr().cast(), rest.len(), libc::MSG_NOSIGNAL) };
        #[cfg(not(target_os = "linux"))]
        let wrote = unsafe { libc::write(socket, rest.as_ptr().cast(), rest.len()) };
        match usize::try_from(wrote) {
            Ok(wrote) => written += wrote,
            Err(_) => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
    Ok(())
}

/// The gateway's next answer. A gateway that refuses the request before it reads it as a land turn
/// (an older build, which does not know the operation) answers a control failure,
/// `{"ok":false,…}`, which is returned as that refusal.
async fn answer(connection: &mut Connection) -> io::Result<TurnAnswer> {
    let Some(line) = connection.next_line().await? else {
        return Err(io::Error::new(
            io::ErrorKind::UnexpectedEof,
            "the gateway closed the land turn's connection",
        ));
    };
    decode(&line)
}

fn decode(line: &str) -> io::Result<TurnAnswer> {
    #[derive(Deserialize)]
    struct Failure {
        ok: bool,
        error: Option<String>,
    }
    serde_json::from_str::<TurnAnswer>(line).or_else(|error| {
        match serde_json::from_str::<Failure>(line) {
            Ok(Failure { ok: false, error }) => Ok(TurnAnswer::Refused {
                error: error.unwrap_or_else(|| "it gave no reason".to_owned()),
            }),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("the gateway answered the land turn with {line:?}: {error}"),
            )),
        }
    })
}

fn expired_error(bound_ms: u64) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!(
            "expired: exceeded bounded-turn deadline ({})",
            format_bound(bound_ms)
        ),
    )
}

fn unexpected(answer: &TurnAnswer, step: &str) -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        format!("the gateway answered {step} with {answer:?}"),
    )
}

/// The outside wait of the land `me`, from one census of the processes with `outside`'s lock
/// open (05_gateway.md "Land turns"; the one holder census 02_workspaces.md admits outside a
/// destructive operation, "Holders cowshed does not run"). It reports and decides nothing.
fn outside_wait(census: &CensusScope, lock: &Path, me: i32) -> Wait {
    let open = crate::build_volume::nx::holders(census, lock).map_err(|error| error.to_string());
    Wait::Outside {
        lock: lock.to_path_buf(),
        holders: outside_holders(me, open),
    }
}

/// The lock file of lands that do not ask for turns, kept and never removed, and the census that
/// names who has it open while a land waits for it: the census of the store the land serves.
#[derive(Clone, Copy, Debug)]
pub struct OutsideLock<'a> {
    pub path: &'a Path,
    pub census: &'a CensusScope,
}

/// Connect to `gateway` across an install that restarts it
/// ([`crate::runtime::supervisor_manager::connect_across_install`]) and send `ask`: the
/// connection its answers come on, and its descriptor.
async fn asked(gateway: &TurnGateway, ask: &TurnAsk) -> io::Result<(Connection, RawFd)> {
    #[derive(Serialize)]
    struct Asking<'a> {
        op: &'static str,
        #[serde(flatten)]
        ask: &'a TurnAsk,
    }
    let socket = gateway.socket.as_path();
    let connect = || async move {
        let budget =
            cowshed_os::deadline::start("land-turn connect", cowshed_os::deadline::HANDSHAKE);
        let peer = cowshed_os::deadline::Peer::unix("gateway", socket);
        cowshed_os::deadline::connect_unix(&budget, &peer, socket)
            .await
            .map_err(io::Error::from)
    };
    let stream = crate::runtime::supervisor_manager::connect_across_install(
        &gateway.census,
        &gateway.support,
        connect,
    )
    .await
    .map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("cannot reach the gateway at {}: {error}", socket.display()),
        )
    })?;
    let descriptor = std::os::fd::AsRawFd::as_raw_fd(&stream);
    let mut stream = Fenced::new(stream);
    send(
        &mut stream,
        &Asking {
            op: LAND_TURN_OP,
            ask,
        },
    )
    .await?;
    Ok((Connection::new(stream), descriptor))
}

/// What [`queued`] returns once the turn is held.
struct Acquired {
    ticket: Ticket,
    resume_token: Option<String>,
    resumed_across_restart: bool,
}

/// Read the gateway's answers to `ask` on `connection` until its turn is held, telling `waiting`
/// who holds it whenever that changes and every `report_every` while it does not: the acquired
/// ticket, or `None` when the gateway closed the connection after it answered `fenced` (parked
/// ask). A parked ask is numbered on its own connection; one the install's restart drains is asked
/// again. A queued resumable ticket that receives `{"turn":"fence"}` acknowledges
/// `{"step":"fenced"}`, stays alive across heartbeats, unfences in place on `{"turn":"unfenced"}`,
/// or reconnects and resumes via `land-turn-resume` on fenced EOF. A close with no `fenced` before
/// it is an error.
async fn queued(
    connection: &mut Connection,
    descriptor: &mut RawFd,
    gateway: &TurnGateway,
    ask: &TurnAsk,
    report_every: Duration,
    waiting: &mut impl FnMut(&Wait),
) -> io::Result<Option<Acquired>> {
    let mut told: Option<Wait> = None;
    let mut ticket_number: Option<u64> = None;
    let mut resume_token: Option<String> = None;
    let mut fenced = false;
    let mut resumed_across_restart = false;
    let start = tokio::time::Instant::now() + report_every;
    let mut cadence = tokio::time::interval_at(start, report_every);
    let mut silence = Box::pin(tokio::time::sleep(cowshed_os::deadline::HEARTBEAT_SILENCE));
    loop {
        tokio::select! {
            line = connection.next_line() => {
                let line = match line? {
                    Some(line) => {
                        if fenced {
                            silence.as_mut().reset(
                                tokio::time::Instant::now()
                                    + cowshed_os::deadline::HEARTBEAT_SILENCE,
                            );
                        }
                        line
                    }
                    None if fenced => {
                        let (Some(number), Some(token)) = (ticket_number, resume_token.as_deref()) else {
                            return Err(io::Error::new(
                                io::ErrorKind::UnexpectedEof,
                                "the gateway closed the fenced land turn's connection without a \
                                 resume token",
                            ));
                        };
                        let (new_conn, new_fd, resumed) =
                            reconnect_and_resume(gateway, number, token).await?;
                        *connection = new_conn;
                        *descriptor = new_fd;
                        fenced = false;
                        resumed_across_restart = true;
                        match resumed {
                            TurnAnswer::Held { ticket, .. } => {
                                return Ok(Some(Acquired {
                                    ticket,
                                    resume_token,
                                    resumed_across_restart,
                                }));
                            }
                            TurnAnswer::Resumed { .. } => continue,
                            TurnAnswer::Queued { holder, ahead, .. } => {
                                let wait = Wait::Queued { holder, ahead };
                                if told.as_ref() != Some(&wait) {
                                    waiting(&wait);
                                    cadence.reset();
                                }
                                told = Some(wait);
                                continue;
                            }
                            other => return Err(unexpected(&other, "a resume")),
                        }
                    }
                    None if matches!(told, Some(Wait::Install { .. })) => return Ok(None),
                    None => {
                        return Err(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "the gateway closed the land turn's connection",
                        ));
                    }
                };
                let wait = match decode(&line)? {
                    TurnAnswer::Held {
                        ticket,
                        resume_token: token,
                    } => {
                        if token.is_some() {
                            resume_token = token;
                        }
                        return Ok(Some(Acquired {
                            ticket,
                            resume_token,
                            resumed_across_restart,
                        }));
                    }
                    TurnAnswer::Fenced => Wait::Install {
                        lock: gateway.slot(),
                    },
                    TurnAnswer::Queued {
                        ticket,
                        holder,
                        ahead,
                        resume_token: token,
                    } => {
                        ticket_number = Some(ticket.number);
                        if token.is_some() {
                            resume_token = token;
                        }
                        Wait::Queued { holder, ahead }
                    }
                    TurnAnswer::Fence => {
                        let payload = self::line(&TurnStep::Fenced)?;
                        connection.write_all(&payload).await?;
                        fenced = true;
                        silence.as_mut().reset(
                            tokio::time::Instant::now()
                                + cowshed_os::deadline::HEARTBEAT_SILENCE,
                        );
                        continue;
                    }
                    TurnAnswer::Heartbeat { .. } => continue,
                    TurnAnswer::Unfenced => {
                        fenced = false;
                        continue;
                    }
                    TurnAnswer::Refused { error } => {
                        return Err(io::Error::other(format!(
                            "the gateway at {} refused the land turn {}: {error}",
                            gateway.socket.display(),
                            ask.repo,
                        )));
                    }
                    other @ (TurnAnswer::Bound { .. }
                    | TurnAnswer::Expired { .. }
                    | TurnAnswer::Resumed { .. }) => {
                        return Err(unexpected(&other, "an ask"));
                    }
                };
                if told.as_ref() != Some(&wait) {
                    waiting(&wait);
                    cadence.reset();
                }
                told = Some(wait);
            },
            _ = cadence.tick() => {
                if let Some(wait) = &told {
                    waiting(wait);
                }
            }
            () = &mut silence, if fenced => {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "the gateway went silent while this queued land turn was fenced",
                ));
            }
        }
    }
}
/// Ask `gateway` for `ask`'s turn and wait for it, then take `outside` exclusively too when given.
/// `waiting` is told who holds the turn and how many are ahead, that an install holds the ask back,
/// or which processes have the outside lock open, whenever that changes and every `report_every`
/// while it does not.
///
/// An ask parked behind an install is numbered on its own connection once the install's fence
/// lifts. One whose connection is refused while an install holds the slot, or that the gateway
/// closes after it answered `fenced`, as the install's restart drains it, waits for that install
/// and asks again of the gateway it leaves (05_gateway.md "The read fences the turns"). A fence is
/// held only by an install's open connection, which the gateway lifts when it closes, so a fence
/// met again is waited for the same way.
pub async fn take(
    gateway: &TurnGateway,
    ask: &TurnAsk,
    outside: Option<OutsideLock<'_>>,
    report_every: Duration,
    mut waiting: impl FnMut(&Wait),
) -> io::Result<Turn> {
    let (acquired, connection, descriptor) = loop {
        let (mut connection, mut descriptor) = asked(gateway, ask).await?;
        if let Some(acquired) = queued(
            &mut connection,
            &mut descriptor,
            gateway,
            ask,
            report_every,
            &mut waiting,
        )
        .await?
        {
            break (acquired, connection, descriptor);
        }
        let (census, support) = (gateway.census.clone(), gateway.support.clone());
        tokio::task::spawn_blocking(move || {
            crate::host_dirs::wait_for_running_install(&census, &support)
        })
        .await
        .map_err(io::Error::other)??;
    };
    // The outside lock is the held lock every other holder of it takes, recorded on its inode.
    // Its wait runs on a thread of its own before the first census, so no census delays it.
    let outside = match outside {
        None => None,
        Some(OutsideLock { path: lock, census }) => Some(match Opened::open(lock)?.try_hold()? {
            Ok(held) => held,
            Err(opened) => {
                let me = i32::try_from(std::process::id()).map_err(io::Error::other)?;
                let mut holding = tokio::task::spawn_blocking(move || opened.hold());
                let mut cadence = tokio::time::interval(report_every);
                loop {
                    tokio::select! {
                        held = &mut holding => break held.map_err(io::Error::other)??,
                        _ = cadence.tick() => {
                            let (lock, census) = (lock.to_path_buf(), census.clone());
                            let wait = tokio::task::spawn_blocking(move || outside_wait(&census, &lock, me))
                                .await
                                .map_err(io::Error::other)?;
                            waiting(&wait);
                        }
                    }
                }
            }
        }),
    };
    if let Some(resume_token) = acquired.resume_token {
        Turn::resumable(
            acquired.ticket,
            connection,
            outside,
            gateway.clone(),
            resume_token,
            acquired.resumed_across_restart,
        )
    } else {
        Ok(Turn::direct(
            acquired.ticket,
            connection,
            descriptor,
            outside,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use proptest::prelude::*;

    use super::*;

    fn ticket(number: u64) -> Ticket {
        Ticket {
            number,
            pid: 41,
            workspace: "topic".to_owned(),
            asked_unix_ms: 7,
            command: "land".to_owned(),
            bound: vec![42],
            fenced: false,
        }
    }

    #[test]
    fn an_answer_and_a_control_failure_both_decode_and_anything_else_is_invalid() {
        let held = TurnAnswer::Held {
            ticket: ticket(3),
            resume_token: None,
        };
        let line = String::from_utf8(line(&held).unwrap()).unwrap();
        assert_eq!(decode(line.trim_end()).unwrap(), held);
        assert_eq!(
            decode(
                r#"{"ok":false,"code":"invalid-request","error":"unknown gateway control operation"}"#
            )
            .unwrap(),
            TurnAnswer::Refused {
                error: "unknown gateway control operation".to_owned()
            },
        );
        assert_eq!(
            decode(r#"{"ok":true}"#).unwrap_err().kind(),
            io::ErrorKind::InvalidData
        );
    }

    #[test]
    fn a_step_names_its_kind_and_a_ticket_without_bound_commands_omits_them() {
        assert_eq!(
            serde_json::to_string(&TurnStep::Bind { pid: 9 }).unwrap(),
            r#"{"step":"bind","pid":9}"#
        );
        let mut free = ticket(1);
        free.bound.clear();
        assert!(!serde_json::to_string(&free).unwrap().contains("bound"));
    }

    #[test]
    fn the_bind_a_child_writes_before_its_exec_is_the_bind_step() {
        use std::io::Read as _;
        use std::os::fd::AsRawFd as _;

        for pid in [0, 7, 41_235, u32::MAX] {
            let (ours, theirs) = std::os::unix::net::UnixStream::pair().unwrap();
            write_bind(theirs.as_raw_fd(), pid).unwrap();
            drop(theirs);
            let mut written = String::new();
            (&ours).read_to_string(&mut written).unwrap();
            let step = written.strip_suffix('\n').expect("one line");
            assert_eq!(
                serde_json::from_str::<TurnStep>(step).unwrap(),
                TurnStep::Bind { pid }
            );
            assert_eq!(
                step,
                serde_json::to_string(&TurnStep::Bind { pid }).unwrap()
            );
        }
    }

    #[tokio::test]
    async fn end_drains_unread_expired_answer_after_peer_close() {
        use tokio::io::AsyncWriteExt as _;

        let (client, mut server) = UnixStream::pair().expect("unix stream pair");
        let descriptor = std::os::fd::AsRawFd::as_raw_fd(&client);
        let turn = Turn::direct(
            ticket(1),
            Connection::new(Fenced::new(client)),
            descriptor,
            None,
        );
        server
            .write_all(b"{\"turn\":\"expired\",\"boundMs\":250}\n")
            .await
            .expect("write expired");
        drop(server);

        let error = turn
            .end("exited 0".to_owned())
            .expect_err("end must report the unread expired answer");
        assert_eq!(error.kind(), io::ErrorKind::TimedOut);
        assert_eq!(
            error.to_string(),
            "expired: exceeded bounded-turn deadline (250 ms)"
        );
    }

    #[tokio::test]
    async fn loss_after_bind_epipe_drains_unread_expired_and_reuses_it_for_lost_and_end() {
        use cowshed_os::test_backstop::CommandBackstop as _;
        use tokio::io::AsyncWriteExt as _;

        let (client, mut server) = UnixStream::pair().expect("unix stream pair");
        let descriptor = std::os::fd::AsRawFd::as_raw_fd(&client);
        let mut turn = Turn::direct(
            ticket(1),
            Connection::new(Fenced::new(client)),
            descriptor,
            None,
        );
        server
            .write_all(b"{\"turn\":\"expired\",\"boundMs\":250}\n")
            .await
            .expect("write expired");
        drop(server);

        // Child pre_exec write_bind fails with EPIPE on the closed socket.
        let mut command = std::process::Command::new("true");
        turn.bind_at_exec(&mut command);
        let spawn_err = command
            .status_bounded()
            .expect_err("bind_at_exec on closed socket fails before exec");
        assert_eq!(spawn_err.kind(), io::ErrorKind::BrokenPipe);

        let loss = turn
            .loss()
            .expect("loss drains the unread expired answer after EPIPE");
        assert_eq!(
            loss.to_string(),
            "expired: exceeded bounded-turn deadline (250 ms)"
        );
        assert_eq!(
            turn.lost().await.to_string(),
            "expired: exceeded bounded-turn deadline (250 ms)"
        );
        assert_eq!(
            turn.end("refused".to_owned())
                .expect_err("end repeats the recorded loss")
                .to_string(),
            "expired: exceeded bounded-turn deadline (250 ms)"
        );
    }

    #[tokio::test]
    async fn end_reports_closed_connection_on_eof_without_expired() {
        let (client, server) = UnixStream::pair().expect("unix stream pair");
        let descriptor = std::os::fd::AsRawFd::as_raw_fd(&client);
        let turn = Turn::direct(
            ticket(1),
            Connection::new(Fenced::new(client)),
            descriptor,
            None,
        );
        drop(server);

        let error = turn
            .end("exited 0".to_owned())
            .expect_err("end on closed socket fails");
        assert_eq!(error.kind(), io::ErrorKind::UnexpectedEof);
        assert!(
            error
                .to_string()
                .contains("closed the connection that holds the turn"),
            "{error}"
        );
    }

    proptest! {
        /// Whatever the census lists, the outside wait names each process with the lock open but
        /// the waiting land itself, once, by pid ascending, as the first census row of that pid
        /// lists it; no such process is `NoneOpen`, never an empty list; a census that could not
        /// be taken is said with its reason. The reference is the same answer kept in a map.
        #[test]
        fn the_outside_wait_names_every_other_process_with_the_lock_open_once(
            me in 1_i32..16,
            rows in prop::collection::vec((1_i32..16, "[a-z]{1,6}"), 0..12),
            failure in prop::option::of("[a-z ]{1,12}"),
        ) {
            let open: Vec<Holder> = rows
                .into_iter()
                .map(|(pid, command)| Holder { pid, command })
                .collect();
            let census = failure.clone().map_or_else(|| Ok(open.clone()), Err);
            let mut reference = BTreeMap::new();
            for holder in &open {
                if holder.pid != me {
                    reference.entry(holder.pid).or_insert_with(|| holder.clone());
                }
            }
            let expected = match failure {
                Some(reason) => OutsideHolders::Unknown(reason),
                None if reference.is_empty() => OutsideHolders::NoneOpen,
                None => OutsideHolders::Open(reference.into_values().collect()),
            };
            prop_assert_eq!(outside_holders(me, census), expected);
        }
    }
}
