//! The gateway's jobserver fifos (05_gateway.md, "Host CPU budget": one fifo per
//! client process).
//!
//! On a grant the gateway creates one fifo here, keeps its own `O_RDWR` descriptor
//! until the drain so the token bytes survive while no client has it open, and
//! writes `g − 1` token bytes into it: the client's implicit make token is the
//! first. When the client's connection closes the gateway drains the bytes left,
//! unlinks the fifo and the caller returns all `g` tokens to the ledger, so
//! tokens a killed process read and never wrote back are credited too. A restart
//! forgets every grant, so startup removes every fifo left here.

use std::fs::{File, OpenOptions};
use std::io::{self, Read as _, Write as _};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::FileTypeExt as _;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

/// The byte a GNU make jobserver hands out as one token.
const TOKEN_BYTE: u8 = b'+';

/// How often a lent pool below its want looks at its fifo (05_gateway.md "All at once").
const GROWTH_CHECK: std::time::Duration = std::time::Duration::from_secs(1);
/// How long the ledger refuses a pool's asks, asked every check, before the pool says it starves.
const STARVED_AFTER: std::time::Duration = std::time::Duration::from_secs(3);
/// The longest a pool waits to offer a token again after offered ones sat idle.
const OFFER_BACK_OFF_MAX: std::time::Duration = std::time::Duration::from_secs(30);

/// Whole-grant admission, never a request to resize a live FIFO. Neither class has an admission
/// deadline: a request waits in the ledger's order until it is granted or its client leaves.
/// These are the classes that hold tokens, so they are the ledger's; a resident holds none and
/// asks with its own operation (`resident-pool`), never as a jobserver class.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum JobserverAdmission {
    #[default]
    Background,
    Gate,
}

/// The fifos of one gateway, in its private runtime directory.
#[derive(Clone, Debug)]
pub(crate) struct JobserverFifos {
    directory: std::sync::Arc<PathBuf>,
    counter: std::sync::Arc<AtomicU64>,
}

impl JobserverFifos {
    /// The trusted job identity chooses the workspace IPC scope; clients cannot name a path.
    pub(crate) fn create_pool(&self, source: PoolSource, incarnation: &str) -> io::Result<CpuPool> {
        let store = self.directory.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "jobserver directory has no runtime parent",
            )
        })?;
        let directory = cowshed_gateway_types::cpu_pool_directory(store, incarnation)
            .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
        CpuPool::start(
            PoolScope {
                directory: std::sync::Arc::new(directory),
                gate: None,
            },
            source,
        )
    }

    /// Name the fifo directory, sweeping a previous gateway's leftover fifos.
    pub(crate) fn start(directory: PathBuf) -> Self {
        // Sweep the fifos a previous gateway left in the directory when it
        // exists: a restart forgot those grants, and their bytes belong to
        // nobody. The directory itself is created at the first grant, so
        // gateway start neither creates the control socket's parent (masking
        // its absence) nor fails on it.
        if let Ok(entries) = std::fs::read_dir(&directory) {
            for entry in entries.flatten() {
                if entry.file_type().is_ok_and(|kind| kind.is_fifo()) {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        Self {
            directory: std::sync::Arc::new(directory),
            counter: std::sync::Arc::new(AtomicU64::new(0)),
        }
    }

    /// Create one fifo holding `tokens - 1` token bytes, kept open by the
    /// gateway's own descriptor until [`JobserverFifo::drain_and_unlink`].
    pub(crate) fn create(&self, tokens: usize) -> io::Result<JobserverFifo> {
        std::fs::create_dir_all(self.directory.as_ref() as &Path)?;
        let sequence = self.counter.fetch_add(1, Ordering::Relaxed);
        let path = self
            .directory
            .join(format!("jobserver-{}-{sequence}.fifo", std::process::id()));
        JobserverFifo::make(path, tokens)
    }
}

/// One granted client's fifo, held open by the gateway until the drain.
#[derive(Debug)]
pub(crate) struct JobserverFifo {
    path: PathBuf,
    file: File,
}

impl JobserverFifo {
    /// Make the fifo at `path`, mode `0600`, holding `tokens - 1` token bytes: its client's
    /// implicit make token is the first.
    fn make(path: PathBuf, tokens: usize) -> io::Result<Self> {
        let c_path = std::ffi::CString::new(path.as_os_str().as_bytes()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "jobserver fifo path has a NUL")
        })?;
        // SAFETY: c_path is a valid NUL-terminated path for the duration of the call.
        if unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) } != 0 {
            return Err(io::Error::last_os_error());
        }
        let opened = OpenOptions::new().read(true).write(true).open(&path);
        let mut file = match opened {
            Ok(file) => file,
            Err(error) => {
                let _ = std::fs::remove_file(&path);
                return Err(error);
            }
        };
        if tokens > 1
            && let Err(error) = file.write_all(&vec![TOKEN_BYTE; tokens - 1])
        {
            let _ = std::fs::remove_file(&path);
            return Err(error);
        }
        Ok(Self { path, file })
    }

    /// The path the granted answer names for `CARGO_MAKEFLAGS`.
    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// Drain the token bytes left in the fifo and unlink it. A cargo that
    /// outlives its holder keeps its own descriptor and runs over budget
    /// until it finishes; what it still holds is not the gateway's to reclaim.
    pub(crate) fn drain_and_unlink(mut self) {
        // SAFETY: the descriptor is this fifo's own, opened above.
        let flags = unsafe { libc::fcntl(self.file.as_raw_fd(), libc::F_GETFL) };
        if flags >= 0 {
            // SAFETY: same descriptor; read what is already there, never wait.
            unsafe {
                libc::fcntl(
                    self.file.as_raw_fd(),
                    libc::F_SETFL,
                    flags | libc::O_NONBLOCK,
                );
            }
            let mut discard = [0_u8; 4096];
            while self.file.read(&mut discard).is_ok_and(|read| read > 0) {}
        }
        let _ = std::fs::remove_file(&self.path);
    }
}

use std::os::unix::io::AsRawFd as _;

/// Where a pool's children take their tokens.
pub(crate) enum PoolSource {
    /// A background grant's own allocation of `g` tokens: `g − 1` bytes in the pool's make
    /// jobserver fifo, which its holder's command reads as `CARGO_MAKEFLAGS`, and the holder's own
    /// implicit token. A child asking the pool's socket takes from that same count ([`FifoTokens`]),
    /// so a Cargo reading the fifo and a nested runner holding a grant never count one token twice.
    Lent(std::num::NonZeroUsize),
    /// A lent pool whose host grant is below what it asked: it tops up from the host ledger
    /// ([`Growth`]).
    Growing {
        tokens: std::num::NonZeroUsize,
        growing: Growing,
    },
    /// A gate grant: each child asks the host budget itself, at gate class, for the gate's
    /// checkout, ranked by the gate job's own admission. A land's tasks therefore rank ahead of
    /// background work at every token any holder returns, and an older land's tasks ahead of a
    /// younger one's; no allocation is held idle between tasks and no running grant needs to
    /// shrink. The gate's own `tokens` sit in the pool's fifo for its own command's Cargo; the
    /// relay lends none of them.
    Relay {
        host: crate::cpu_budget::CpuBudget,
        checkout: std::sync::Arc<str>,
        repo: std::sync::Arc<str>,
        job: crate::cpu_budget::JobAge,
        tokens: std::num::NonZeroUsize,
    },
    /// A resident job, which holds no token: each child asks the host budget itself, at
    /// background class, for the resident's checkout, exactly as a root background request
    /// does. A daemon idle between requests therefore holds nothing, and each request it runs
    /// takes its tokens when it starts and returns them when it ends. It has no fifo.
    Resident {
        host: crate::cpu_budget::CpuBudget,
        checkout: std::sync::Arc<str>,
    },
    /// A one-token job on probation (05_gateway.md "A one-token job starts on probation"): a
    /// lent pool of one, its fifo holding no bytes, whose holder's own token is the probation
    /// token, which no child takes. [`CpuPool::grant_probation`] makes it the job's granted
    /// token, which the pool then lends as any lent pool of one does.
    Probation,
}

/// What a lent pool tops up through (05_gateway.md "All at once"): its host stake's grower, the
/// count it asked for, and what names its job in a starvation.
pub(crate) struct Growing {
    pub(crate) grower: crate::cpu_budget::Grower,
    pub(crate) want: usize,
    pub(crate) named: String,
}

/// Where a pool serves, and the gate job it serves when it serves one: the directory every pool
/// of its workspace's jobs lies in, and, for a gate's root pool and every pool served under it,
/// the gate job, so the host budget knows that job's pools (05_gateway.md "A fork or attach
/// waits for its repository's gate").
#[derive(Clone)]
struct PoolScope {
    directory: std::sync::Arc<PathBuf>,
    gate: Option<(crate::cpu_budget::CpuBudget, crate::cpu_budget::JobAge)>,
}

/// What a pool's socket children are granted from.
#[derive(Clone)]
enum PoolLedger {
    /// The pool's own tokens, in its fifo.
    Fifo(FifoTokens),
    /// The host budget, asked once per child for `checkout` at `rank`.
    Host {
        budget: crate::cpu_budget::CpuBudget,
        checkout: std::sync::Arc<str>,
        rank: crate::cpu_budget::Rank,
    },
}

/// One pool endpoint serving a granted job's children, with the make jobserver fifo its own
/// command reads when it holds tokens.
pub(crate) struct CpuPool {
    path: PathBuf,
    fifo: Option<JobserverFifo>,
    stop: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<io::Result<()>>>,
    /// A probation pool's lender, until its grant frees the holder's token to lend.
    probation: Option<FifoTokens>,
}

impl CpuPool {
    fn start(scope: PoolScope, source: PoolSource) -> io::Result<Self> {
        let gate = match &source {
            PoolSource::Relay { host, job, .. } => Some((host.clone(), *job)),
            _ => scope.gate,
        };
        let directory = scope.directory;
        use std::os::unix::fs::{DirBuilderExt as _, PermissionsExt as _};
        std::fs::DirBuilder::new()
            .recursive(true)
            .mode(0o700)
            .create(directory.as_path())?;
        // Flat, opaque endpoints stay within Darwin's sun_path bound and cannot be guessed
        // from the job's public id. Sandbox policy admits connect/metadata, never directory data;
        // of the fifo beside it, reads and writes of its tokens and nothing else.
        let fifo = match &source {
            PoolSource::Lent(tokens)
            | PoolSource::Growing { tokens, .. }
            | PoolSource::Relay { tokens, .. } => Some(JobserverFifo::make(
                directory.join(opaque_name()),
                tokens.get(),
            )?),
            PoolSource::Probation => Some(JobserverFifo::make(directory.join(opaque_name()), 1)?),
            PoolSource::Resident { .. } => None,
        };
        let path = directory.join(opaque_name());
        let listener = cowshed_os::fork_lock::Fenced::create(|| {
            let listener = std::os::unix::net::UnixListener::bind(&path)?;
            listener.set_nonblocking(true)?;
            tokio::io::unix::AsyncFd::new(listener)
        })?;
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600))?;
        if let Some((budget, job)) = &gate {
            budget.gate_pool(*job, std::sync::Arc::from(path.as_path()));
        }
        let scope = PoolScope {
            directory: std::sync::Arc::clone(&directory),
            gate,
        };
        let made = || {
            fifo.as_ref()
                .expect("a lent pool made its fifo above")
                .path()
        };
        let mut probation = None;
        let ledger = match source {
            PoolSource::Lent(_) => PoolLedger::Fifo(FifoTokens::start(made(), true, None)?),
            PoolSource::Growing { tokens, growing } => {
                let shell = GrowthShell {
                    growth: Growth::new(
                        std::time::Instant::now(),
                        growing.want.saturating_sub(tokens.get()),
                    ),
                    granted: tokens.get(),
                    want: growing.want,
                    grower: growing.grower,
                    named: growing.named,
                    pool: path.clone(),
                };
                PoolLedger::Fifo(FifoTokens::start(made(), true, Some(shell))?)
            }
            PoolSource::Probation => {
                let tokens = FifoTokens::start(made(), false, None)?;
                probation = Some(tokens.clone());
                PoolLedger::Fifo(tokens)
            }
            PoolSource::Relay {
                host,
                checkout,
                repo,
                job,
                ..
            } => PoolLedger::Host {
                budget: host,
                checkout,
                rank: crate::cpu_budget::Rank::GateTask { repo, job },
            },
            PoolSource::Resident { host, checkout } => PoolLedger::Host {
                budget: host,
                checkout,
                rank: crate::cpu_budget::Rank::Background,
            },
        };
        let (stop, mut stopped) = tokio::sync::oneshot::channel();
        let retiring_path = path.clone();
        let task = tokio::spawn(async move {
            let mut children = tokio::task::JoinSet::new();
            let (children_stop, child_stopped) = tokio::sync::watch::channel(());
            loop {
                tokio::select! {
                    biased;
                    _ = &mut stopped => break,
                    accepted = async {
                        loop {
                            let mut ready = listener.readable().await?;
                            if let Ok(result) = ready.try_io(|listener| accept_pool_connection(listener.get_ref())) {
                                break result;
                            }
                        }
                    } => match accepted {
                        Ok(stream) => {
                            children.spawn(serve_pool_until_retired(stream, ledger.clone(), scope.clone(), child_stopped.clone()));                        }
                        Err(error) => { cowshed_os::eprintln!("cowshed: CPU pool listener failed: {error}"); break; }
                    },
                    ended = children.join_next(), if !children.is_empty() => {
                        if let Some(Err(error)) = ended { cowshed_os::eprintln!("cowshed: CPU pool child failed: {error}"); }
                    }
                }
            }
            let retired = retire_pool_listener(listener.get_ref(), &retiring_path).await;
            drop(listener);
            drop(children_stop);
            let mut ended = 0;
            while let Some(result) = children.join_next().await {
                match result {
                    Ok(true) => ended += 1,
                    Ok(false) => {}
                    Err(error) => {
                        cowshed_os::eprintln!(
                            "cowshed: CPU pool child failed during retirement: {error}"
                        )
                    }
                }
            }
            match &retired {
                Ok(refused) => cowshed_os::eprintln!(
                    "cowshed: CPU pool {} listener closed: refused {refused} queued connections, ended {ended} accepted connections (pool-retired)",
                    retiring_path.display()
                ),
                Err(error) => cowshed_os::eprintln!(
                    "cowshed: CPU pool {} retirement failed: {error}",
                    retiring_path.display()
                ),
            }
            retired.map(|_| ())
        });
        Ok(Self {
            path,
            fifo,
            stop: Some(stop),
            task: Some(task),
            probation,
        })
    }

    pub(crate) fn path(&self) -> &Path {
        &self.path
    }

    /// A probation pool's job is granted its token: the holder's own token is the job's now, and
    /// the pool lends it as any lent pool of one does. Any other pool is unchanged.
    pub(crate) fn grant_probation(&mut self) {
        if let Some(tokens) = self.probation.take() {
            tokens.free_implicit();
        }
    }

    /// The make jobserver fifo the pool's own command reads; none for a resident's.
    pub(crate) fn fifo(&self) -> Option<&Path> {
        self.fifo.as_ref().map(JobserverFifo::path)
    }

    pub(crate) async fn close(mut self) -> io::Result<()> {
        drop(self.stop.take());
        if let Some(task) = self.task.take() {
            task.await
                .map_err(|error| io::Error::other(format!("CPU pool task failed: {error}")))??;
        }
        if let Some(fifo) = self.fifo.take() {
            fifo.drain_and_unlink();
        }
        Ok(())
    }
}

impl Drop for CpuPool {
    fn drop(&mut self) {
        // Dropping the signal requests the same queued-connection drain as explicit close.
        // The task retains the listener until that drain completes; it must not be aborted.
        drop(self.stop.take());
        if let Some(fifo) = self.fifo.take() {
            fifo.drain_and_unlink();
        }
    }
}

fn accept_pool_connection(
    listener: &std::os::unix::net::UnixListener,
) -> io::Result<cowshed_os::fork_lock::Fenced<tokio::net::UnixStream>> {
    cowshed_os::fork_lock::Fenced::create(|| {
        let (stream, _) = listener.accept()?;
        stream.set_nonblocking(true)?;
        tokio::net::UnixStream::from_std(stream)
    })
}

async fn retire_pool_listener(
    listener: &std::os::unix::net::UnixListener,
    path: &Path,
) -> io::Result<usize> {
    match std::fs::remove_file(path) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => return Err(error),
    }
    let mut refused = 0;
    loop {
        // No new path lookup can reach this listener. Drain its kernel accept queue before
        // closing it: aborting the listener can strand a concurrent Darwin connect forever.
        // Tokio's readiness cache can still be empty while the kernel already has connections.
        match accept_pool_connection(listener) {
            Ok(stream) => {
                refused += 1;
                refuse_retired_pool(stream).await;
            }
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => return Ok(refused),
            Err(error) => return Err(error),
        }
    }
}

/// Answer a connection the retiring pool will not serve, then close it only after the client
/// closes its end. Linux resets a unix stream closed while it still holds unread bytes, such as
/// a request the pool never read, so the client would read the refusal and then `ECONNRESET`
/// where the end of stream belongs; macOS reports the end of stream either way. Shutting down the
/// write half gives the client its end of stream now; the detached read discards whatever the
/// client still sends, however late its request arrives, so retirement never waits on a client.
async fn refuse_retired_pool(mut stream: cowshed_os::fork_lock::Fenced<tokio::net::UnixStream>) {
    use tokio::io::AsyncWriteExt as _;

    if let Err(error) = stream
        .write_all(b"{\"ok\":false,\"code\":\"pool-retired\",\"error\":\"the CPU pool owner's lease ended\"}\n")
        .await
    {
        cowshed_os::eprintln!("cowshed: CPU pool could not deliver pool-retired: {error}");
    }
    if let Err(error) = stream.shutdown().await {
        cowshed_os::eprintln!("cowshed: CPU pool could not close refused connection: {error}");
    }
    tokio::spawn(async move {
        if let Err(error) = tokio::io::copy(&mut stream, &mut tokio::io::sink()).await {
            cowshed_os::eprintln!(
                "cowshed: CPU pool could not read refused connection to its end: {error}"
            );
        }
    });
}

async fn serve_pool_until_retired(
    mut stream: cowshed_os::fork_lock::Fenced<tokio::net::UnixStream>,
    ledger: PoolLedger,
    scope: PoolScope,
    mut stopped: tokio::sync::watch::Receiver<()>,
) -> bool {
    let retired = tokio::select! {
        biased;
        _ = stopped.changed() => true,
        _ = serve_pool(&mut stream, ledger, scope) => false,
    };
    if retired {
        refuse_retired_pool(stream).await;
    }
    retired
}

/// A fresh name no job id predicts: 22 base64url characters of a random UUID.
fn opaque_name() -> String {
    use base64::Engine as _;
    base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(uuid::Uuid::new_v4().as_bytes())
}

/// A lent pool's free tokens: the bytes in its make jobserver fifo, which Cargo reads directly,
/// and the holder's own implicit token while no child holds it. A child asking the pool's socket
/// takes from the same count, in the order children ask: the head takes `min(want, free)` once
/// one token is free, as one whole grant. Lending the implicit token is what lets a one-token pool
/// admit the nested job its command waits on, instead of deadlocking on an empty fifo
/// (05_gateway.md "Cargo reads its holder's jobserver").
#[derive(Clone)]
struct FifoTokens {
    messages: tokio::sync::mpsc::UnboundedSender<FifoMessage>,
}

enum FifoMessage {
    Ask {
        want: std::num::NonZeroUsize,
        granted: tokio::sync::oneshot::Sender<FifoGrant>,
    },
    Return {
        bytes: usize,
        implicit: bool,
    },
}

/// One child's tokens from a [`FifoTokens`]: bytes read out of the fifo, and the implicit token
/// when it was lent. Dropping it writes the bytes back and frees the implicit token.
struct FifoGrant {
    bytes: usize,
    implicit: bool,
    returns: tokio::sync::mpsc::UnboundedSender<FifoMessage>,
}

impl FifoGrant {
    fn tokens(&self) -> usize {
        self.bytes + usize::from(self.implicit)
    }
}

impl Drop for FifoGrant {
    fn drop(&mut self) {
        // A lender that is gone has drained and unlinked its fifo: nothing is owed to it.
        let _ = self.returns.send(FifoMessage::Return {
            bytes: self.bytes,
            implicit: self.implicit,
        });
    }
}

impl FifoTokens {
    /// Lend the tokens in the fifo at `path` through a descriptor of the lender's own, open for
    /// reading and writing so it never sees end-of-file, and nonblocking so a read takes only
    /// what is there. The holder's own token is lent too when `implicit` is free, and not while
    /// it is the probation token ([`Self::free_implicit`]). The lender runs while any handle or
    /// grant of it is alive.
    fn start(path: &Path, implicit: bool, growth: Option<GrowthShell>) -> io::Result<Self> {
        use std::os::unix::fs::OpenOptionsExt as _;
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(path)?;
        let fifo = tokio::io::unix::AsyncFd::with_interest(file, tokio::io::Interest::READABLE)?;
        let (messages, receiver) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(lend(fifo, receiver, messages.downgrade(), implicit, growth));
        Ok(Self { messages })
    }

    /// The holder's own token is free to lend from now on: its probation was granted.
    fn free_implicit(&self) {
        // A lender that is gone lends nothing.
        let _ = self.messages.send(FifoMessage::Return {
            bytes: 0,
            implicit: true,
        });
    }

    /// Queue for up to `want` tokens. The receiver resolves to the grant; dropping it leaves the
    /// queue. A receiver that errors means the lender is gone.
    fn ask(&self, want: std::num::NonZeroUsize) -> tokio::sync::oneshot::Receiver<FifoGrant> {
        let (granted, receiver) = tokio::sync::oneshot::channel();
        let _ = self.messages.send(FifoMessage::Ask { want, granted });
        receiver
    }
}

/// A lent pool's top-up, decided from what each check sees (05_gateway.md "All at once"), at the
/// instants it is given. Its fifo empty, it asks the host ledger for one token at a time up to its
/// want; a token it added that sits in the fifo goes straight back. An offer that sat idle backs
/// the next one off, doubling to [`OFFER_BACK_OFF_MAX`], and an offer a reader took resets that;
/// a child queued on its socket is asked for at once. A refused ask is asked again every check, and
/// a refusal that lasts [`STARVED_AFTER`] is said once a spell.
#[derive(Debug)]
struct Growth {
    /// The tokens it may still add: its want less its grant.
    room: usize,
    /// The tokens it added and holds.
    extra: usize,
    /// A token added at the last ask, not yet seen taken or idle.
    offered: bool,
    back_off: std::time::Duration,
    next_offer: std::time::Instant,
    /// Since when the ledger has refused its asks, its fifo empty throughout.
    refused_since: Option<std::time::Instant>,
    /// Whether this refused spell's starvation was said.
    starved: bool,
}

#[derive(Debug, Eq, PartialEq)]
enum GrowthStep {
    /// Give back up to this many added tokens, read out of the fifo.
    GiveBack(usize),
    /// Ask the host ledger for one token.
    Ask,
    Wait,
}

impl Growth {
    fn new(now: std::time::Instant, room: usize) -> Self {
        Self {
            room,
            extra: 0,
            offered: false,
            back_off: std::time::Duration::ZERO,
            next_offer: now,
            refused_since: None,
            starved: false,
        }
    }

    /// What to do, seeing `idle` token bytes in the fifo and `waiting` children queued on the
    /// pool's socket.
    fn step(&mut self, now: std::time::Instant, idle: usize, waiting: usize) -> GrowthStep {
        if std::mem::take(&mut self.offered) {
            self.back_off = if idle == 0 {
                std::time::Duration::ZERO
            } else {
                (self.back_off * 2).clamp(GROWTH_CHECK, OFFER_BACK_OFF_MAX)
            };
            self.next_offer = now + self.back_off;
        }
        if idle > 0 {
            self.refused_since = None;
            self.starved = false;
            return if self.extra > 0 {
                GrowthStep::GiveBack(idle.min(self.extra))
            } else {
                GrowthStep::Wait
            };
        }
        if self.extra < self.room && (waiting > 0 || now >= self.next_offer) {
            GrowthStep::Ask
        } else {
            GrowthStep::Wait
        }
    }

    /// `tokens` added tokens were read out of the fifo and given back.
    fn gave_back(&mut self, tokens: usize) {
        self.extra -= tokens;
    }

    /// What the ledger answered an ask at `now`: the refused spell's length once, when the
    /// ledger has refused every ask for [`STARVED_AFTER`].
    fn heard(
        &mut self,
        now: std::time::Instant,
        outcome: crate::cpu_budget::TopUpOutcome,
    ) -> Option<std::time::Duration> {
        use crate::cpu_budget::TopUpOutcome;
        match outcome {
            TopUpOutcome::Granted => {
                self.extra += 1;
                self.offered = true;
                self.refused_since = None;
                self.starved = false;
                None
            }
            TopUpOutcome::Settled => {
                self.room = self.extra;
                None
            }
            TopUpOutcome::Full | TopUpOutcome::Waiting => {
                // Refused: ask again at the next check, so the spell is measured asking.
                self.back_off = std::time::Duration::ZERO;
                self.next_offer = now;
                let refused = now.saturating_duration_since(*self.refused_since.get_or_insert(now));
                (!self.starved && refused >= STARVED_AFTER).then(|| {
                    self.starved = true;
                    refused
                })
            }
        }
    }
}

/// The lender's top-up: [`Growth`] over its fifo and its host stake, and what names it.
struct GrowthShell {
    growth: Growth,
    granted: usize,
    want: usize,
    grower: crate::cpu_budget::Grower,
    named: String,
    pool: PathBuf,
}

impl GrowthShell {
    /// One check; `false` once the pool can top up no more (its budget gone, or its fifo failed).
    async fn check(&mut self, fifo: &File, waiting: usize) -> bool {
        let now = std::time::Instant::now();
        let idle = match idle_tokens(fifo) {
            Ok(idle) => idle,
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: CPU pool {} cannot count its idle tokens and tops up no more: {error}",
                    self.pool.display()
                );
                return false;
            }
        };
        match self.growth.step(now, idle, waiting) {
            GrowthStep::Wait => true,
            GrowthStep::GiveBack(tokens) => match read_tokens(fifo, tokens) {
                Ok(read) => {
                    for _ in 0..read {
                        self.grower.give_back();
                    }
                    self.growth.gave_back(read);
                    true
                }
                Err(error) => {
                    cowshed_os::eprintln!(
                        "cowshed: CPU pool {} cannot read its idle tokens back and tops up no more: {error}",
                        self.pool.display()
                    );
                    false
                }
            },
            GrowthStep::Ask => {
                let Ok(top_up) = self.grower.top_up().await else {
                    return false;
                };
                if top_up.outcome == crate::cpu_budget::TopUpOutcome::Granted
                    && let Err(error) = write_tokens(fifo, 1)
                {
                    self.grower.give_back();
                    cowshed_os::eprintln!(
                        "cowshed: CPU pool {} cannot add a token to its fifo and tops up no more: {error}",
                        self.pool.display()
                    );
                    return false;
                }
                if let Some(refused) = self.growth.heard(now, top_up.outcome) {
                    // A reader blocked on the fifo is invisible to the pool: the empty fifo stands
                    // for it. Only a child queued on the pool's socket is counted.
                    cowshed_os::eprintln!(
                        "cowshed: cpu-pool STARVED pool={} job={:?} held={} want={} refused_ms={} socket_waiters={waiting} cause={} ledger_free={} ledger_total={} ledger_queued={}",
                        self.pool.display(),
                        self.named,
                        self.granted + self.growth.extra,
                        self.want,
                        refused.as_millis(),
                        top_up.outcome.as_str(),
                        top_up.free,
                        top_up.total,
                        top_up.queued,
                    );
                }
                true
            }
        }
    }
}

/// The token bytes in `fifo` now, unread.
fn idle_tokens(fifo: &File) -> io::Result<usize> {
    let mut bytes: libc::c_int = 0;
    // SAFETY: FIONREAD writes one int, through a pointer to this stack slot, about the
    // descriptor this lender owns.
    if unsafe { libc::ioctl(fifo.as_raw_fd(), libc::FIONREAD, &mut bytes) } < 0 {
        return Err(io::Error::last_os_error());
    }
    usize::try_from(bytes).map_err(io::Error::other)
}

/// The lender: grants its queue's head from the fifo and the implicit token, takes returns, and
/// waits for the fifo to become readable while its head finds nothing free. A pool below its want
/// checks its top-up every [`GROWTH_CHECK`].
async fn lend(
    fifo: tokio::io::unix::AsyncFd<File>,
    mut messages: tokio::sync::mpsc::UnboundedReceiver<FifoMessage>,
    returns: tokio::sync::mpsc::WeakUnboundedSender<FifoMessage>,
    mut implicit: bool,
    mut growth: Option<GrowthShell>,
) {
    let mut checks = growth.as_ref().map(|_| {
        let mut checks = tokio::time::interval(GROWTH_CHECK);
        checks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        checks
    });
    let mut queue = std::collections::VecDeque::<(
        std::num::NonZeroUsize,
        tokio::sync::oneshot::Sender<FifoGrant>,
    )>::new();
    loop {
        while let Some((want, granted)) = queue.front() {
            if granted.is_closed() {
                queue.pop_front();
                continue;
            }
            let want = want.get();
            let bytes = match read_tokens(fifo.get_ref(), want) {
                Ok(bytes) => bytes,
                Err(error) => {
                    cowshed_os::eprintln!(
                        "cowshed: CPU pool cannot read its jobserver fifo: {error}"
                    );
                    return;
                }
            };
            let lend_implicit = bytes < want && implicit;
            if bytes == 0 && !lend_implicit {
                break;
            }
            implicit &= !lend_implicit;
            let Some(returns) = returns.upgrade() else {
                return;
            };
            let (_, granted) = queue.pop_front().expect("the head was just read");
            // A child that left since the check hands its grant straight back through Drop.
            let _ = granted.send(FifoGrant {
                bytes,
                implicit: lend_implicit,
                returns,
            });
        }
        tokio::select! {
            message = messages.recv() => match message {
                None => return,
                Some(FifoMessage::Ask { want, granted }) => queue.push_back((want, granted)),
                Some(FifoMessage::Return { bytes, implicit: returned }) => {
                    implicit |= returned;
                    if let Err(error) = write_tokens(fifo.get_ref(), bytes) {
                        cowshed_os::eprintln!("cowshed: CPU pool cannot return {bytes} token(s) to its jobserver fifo: {error}");
                        return;
                    }
                }
            },
            // Readiness is cleared before the read it allows: bytes that arrive after it raise
            // it again, and bytes that arrived before it are read at the top of the loop.
            ready = fifo.readable(), if !queue.is_empty() => match ready {
                Ok(mut guard) => guard.clear_ready(),
                Err(error) => {
                    cowshed_os::eprintln!("cowshed: CPU pool cannot wait on its jobserver fifo: {error}");
                    return;
                }
            },
            _ = async {
                match checks.as_mut() {
                    Some(checks) => {
                        checks.tick().await;
                    }
                    None => std::future::pending().await,
                }
            } => {
                if let Some(shell) = growth.as_mut()
                    && !shell.check(fifo.get_ref(), queue.len()).await
                {
                    growth = None;
                    checks = None;
                }
            },
        }
    }
}

/// Read up to `want` token bytes that are there now.
fn read_tokens(mut fifo: &File, want: usize) -> io::Result<usize> {
    let mut chunk = [0_u8; 64];
    let mut taken = 0;
    while taken < want {
        let size = (want - taken).min(chunk.len());
        match fifo.read(&mut chunk[..size]) {
            Ok(0) => break,
            Ok(read) => taken += read,
            Err(error) if error.kind() == io::ErrorKind::WouldBlock => break,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(taken)
}

/// Write `tokens` token bytes back. A fifo's buffer holds far more than a host's cores, so the
/// write never finds it full.
fn write_tokens(mut fifo: &File, tokens: usize) -> io::Result<()> {
    let chunk = [TOKEN_BYTE; 64];
    let mut left = tokens;
    while left > 0 {
        let size = left.min(chunk.len());
        fifo.write_all(&chunk[..size])?;
        left -= size;
    }
    Ok(())
}

#[derive(serde::Deserialize)]
#[serde(tag = "op", deny_unknown_fields)]
enum PoolRequest {
    #[serde(rename = "cpu-pool")]
    Take {
        want: std::num::NonZeroUsize,
        #[serde(default)]
        children: bool,
    },
}

#[derive(serde::Serialize)]
#[serde(tag = "lease", rename_all = "kebab-case")]
enum PoolGrant<'a> {
    Queued {
        ok: bool,
    },
    Granted {
        ok: bool,
        tokens: usize,
        #[serde(skip_serializing_if = "Option::is_none")]
        pool: Option<&'a Path>,
        /// The child pool's make jobserver fifo, for the child's own command.
        #[serde(skip_serializing_if = "Option::is_none")]
        fifo: Option<&'a Path>,
    },
}

async fn pool_reply(
    writer: &mut cowshed_os::fork_lock::Fenced<tokio::net::UnixStream>,
    answer: &impl serde::Serialize,
) -> io::Result<()> {
    use tokio::io::AsyncWriteExt as _;
    let mut frame = serde_json::to_vec(answer).map_err(io::Error::other)?;
    frame.push(b'\n');
    writer.write_all(&frame).await
}

/// What a child waits on: a lent pool's fifo tokens, or a host-ledger place.
enum Admission {
    Fifo(tokio::sync::oneshot::Receiver<FifoGrant>),
    Host {
        stake: crate::cpu_budget::Stake,
        granted: tokio::sync::oneshot::Receiver<usize>,
    },
}

/// What a granted child holds until its connection ends: dropping it returns the tokens.
enum Held {
    Fifo { _grant: FifoGrant },
    Host { stake: crate::cpu_budget::Stake },
}

impl Admission {
    /// The grant and its count; `Err` when the ledger that would grant it is gone, carrying a
    /// host stake so its place leaves the queue only once the child has been told.
    async fn granted(self) -> Result<(Held, usize), Option<crate::cpu_budget::Stake>> {
        match self {
            Self::Fifo(granted) => {
                let grant = granted.await.map_err(|_| None)?;
                let tokens = grant.tokens();
                Ok((Held::Fifo { _grant: grant }, tokens))
            }
            Self::Host { stake, granted } => match granted.await {
                Ok(tokens) => Ok((Held::Host { stake }, tokens)),
                Err(_) => Err(Some(stake)),
            },
        }
    }
}

async fn serve_pool(
    stream: &mut cowshed_os::fork_lock::Fenced<tokio::net::UnixStream>,
    ledger: PoolLedger,
    scope: PoolScope,
) {
    use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, BufReader};
    // The pid names the child in the ledger's status; nothing is decided by it.
    let pid = stream
        .peer_cred()
        .ok()
        .and_then(|credentials| credentials.pid());
    let mut reader = BufReader::new(stream);
    let mut frame = Vec::new();
    let parsed = match (&mut reader).take(4097).read_until(b'\n', &mut frame).await {
        Ok(_) if frame.len() <= 4096 && frame.ends_with(b"\n") => {
            serde_json::from_slice::<PoolRequest>(&frame)
        }
        Ok(_) => {
            let _ = pool_reply(reader.get_mut(), &serde_json::json!({"ok":false,"code":"invalid-request","error":"a CPU pool request is one line of at most 4096 bytes"})).await;
            return;
        }
        Err(error) => {
            cowshed_os::eprintln!("cowshed: cannot read CPU pool request: {error}");
            return;
        }
    };
    let PoolRequest::Take { want, children } = match parsed {
        Ok(request) => request,
        Err(error) => {
            let _ = pool_reply(
                reader.get_mut(),
                &serde_json::json!({"ok":false,"code":"invalid-request","error":error.to_string()}),
            )
            .await;
            return;
        }
    };
    // A host ledger cut off for an install names it to the child it refuses.
    let budget = match &ledger {
        PoolLedger::Host { budget, .. } => Some(budget.clone()),
        PoolLedger::Fifo(_) => None,
    };
    let admission = match ledger {
        PoolLedger::Fifo(fifo) => Admission::Fifo(fifo.ask(want)),
        PoolLedger::Host {
            budget,
            checkout,
            rank,
        } => {
            let claim = crate::disk_lease::Claim::new("child CPU pool", pid);
            let (stake, granted) = budget.ask(checkout, want, claim, rank);
            Admission::Host { stake, granted }
        }
    };
    if pool_reply(reader.get_mut(), &PoolGrant::Queued { ok: true })
        .await
        .is_err()
    {
        return;
    }
    let mut byte = [0_u8; 1];
    let (held, tokens) = tokio::select! {
        granted = admission.granted() => match granted {
            Ok(granted) => granted,
            Err(stake) => {
                let refusal = match budget.as_ref().and_then(crate::cpu_budget::CpuBudget::installing) {
                    Some(install) => serde_json::json!({
                        "ok": false,
                        "code": cowshed_gateway_types::INSTALL_RESTART_CODE,
                        "error": crate::cpu_budget::cut_off_by(install),
                        "install": install,
                    }),
                    None => serde_json::json!({"ok":false,"code":"rejected","error":"the parent CPU pool stopped"}),
                };
                let _ = pool_reply(reader.get_mut(), &refusal).await;
                drop(stake);
                return;
            }
        },
        _ = reader.read(&mut byte) => return,
    };
    let pool = if children {
        let granted = std::num::NonZeroUsize::new(tokens).expect("a CPU grant is nonzero");
        // A host grant below the ask tops up from the host ledger; a pool's own grant is final.
        let source = match &held {
            Held::Host { stake } if want.get() > tokens => PoolSource::Growing {
                tokens: granted,
                growing: Growing {
                    grower: stake.grower(),
                    want: want.get(),
                    named: format!("child CPU pool of pid {pid:?}"),
                },
            },
            _ => PoolSource::Lent(granted),
        };
        match CpuPool::start(scope, source) {
            Ok(pool) => Some(pool),
            Err(error) => {
                let _ = pool_reply(reader.get_mut(), &serde_json::json!({"ok":false,"code":"rejected","error":format!("cannot serve child CPU pool: {error}")})).await;
                return;
            }
        }
    } else {
        None
    };
    if pool_reply(
        reader.get_mut(),
        &PoolGrant::Granted {
            ok: true,
            tokens,
            pool: pool.as_ref().map(CpuPool::path),
            fifo: pool.as_ref().and_then(CpuPool::fifo),
        },
    )
    .await
    .is_err()
    {
        return;
    }
    // End of stream or post-request bytes end the local allocation; a child never releases the
    // parent's upstream grant and never takes a second host allocation while it is borrowed.
    let _ = reader.read(&mut byte).await;
    if let Some(pool) = pool
        && let Err(error) = pool.close().await
    {
        cowshed_os::eprintln!("cowshed: child CPU pool did not close: {error}");
    }
    drop(held);
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;
    fn directory() -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!("cowshed-jobserver-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&root).unwrap();
        let directory = root.join("jobserver");
        (root, directory)
    }

    #[test]
    fn a_grant_fifo_holds_one_fewer_token_than_the_grant_and_drains_clean() {
        let (_root, directory) = directory();
        let fifos = JobserverFifos::start(directory.clone());
        let fifo = fifos.create(4).unwrap();
        let path = fifo.path().to_path_buf();
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        // A client takes two of the three token bytes, then dies without
        // writing them back: the drain removes what is left and the path.
        let mut client = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut taken = [0_u8; 2];
        client.read_exact(&mut taken).unwrap();
        assert_eq!(taken, [TOKEN_BYTE; 2]);
        drop(client);
        fifo.drain_and_unlink();
        assert!(!path.exists());
    }

    #[tokio::test]
    async fn workspace_pool_binds_and_serves_with_a_real_fixture_length_store() {
        use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _, BufReader};

        let root = crate::control::test_fixture_dir::secure_fixture_dir_in(
            &std::fs::canonicalize("/tmp").unwrap(),
            &format!(
                "cowshed-dtest-{}-{}-cli",
                std::process::id(),
                &uuid::Uuid::new_v4().simple().to_string()[..4]
            ),
        );
        let store = root.path().join("store");
        let incarnation = "0123456789abcdef0123456789abcdef";
        let previous_layout = store
            .join("run/cpu-pool")
            .join(incarnation)
            .join("abcdefghijklmnopqrstuv");
        assert!(previous_layout.as_os_str().as_encoded_bytes().len() >= 104);
        let fifos = JobserverFifos::start(store.join("jobserver"));
        let pool = fifos
            .create_pool(
                PoolSource::Lent(std::num::NonZeroUsize::new(2).unwrap()),
                incarnation,
            )
            .expect("the actual private pool binds");
        let endpoint = pool.path().to_owned();
        assert_eq!(
            endpoint.parent(),
            Some(
                cowshed_gateway_types::cpu_pool_directory(&store, incarnation)
                    .unwrap()
                    .as_path()
            )
        );
        assert!(endpoint.as_os_str().as_encoded_bytes().len() < 104);
        assert_eq!(
            std::fs::metadata(&endpoint).unwrap().permissions().mode() & 0o777,
            0o600
        );
        let stream = tokio::net::UnixStream::connect(&endpoint).await.unwrap();
        let mut reader = BufReader::new(stream);
        reader
            .get_mut()
            .write_all(b"{\"op\":\"cpu-pool\",\"want\":2}\n")
            .await
            .unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        let queued: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(queued["lease"], "queued");
        line.clear();
        reader.read_line(&mut line).await.unwrap();
        let granted: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(granted["lease"], "granted");
        assert_eq!(granted["tokens"], 2);
        let fifo = pool.fifo().expect("a lent pool has a fifo").to_owned();
        pool.close().await.expect("the actual pool retires");
        line.clear();
        reader.read_line(&mut line).await.unwrap();
        let retired: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(retired["ok"], false);
        assert_eq!(retired["code"], "pool-retired");
        line.clear();
        assert_eq!(reader.read_line(&mut line).await.unwrap(), 0);
        assert!(!endpoint.exists());
        assert!(!fifo.exists(), "closing the pool unlinks its fifo");
    }

    /// One held `cpu-pool` connection, answered `queued`.
    async fn child(endpoint: &Path, want: usize) -> tokio::io::BufReader<tokio::net::UnixStream> {
        use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};
        let mut reader =
            tokio::io::BufReader::new(tokio::net::UnixStream::connect(endpoint).await.unwrap());
        reader
            .get_mut()
            .write_all(format!("{{\"op\":\"cpu-pool\",\"want\":{want}}}\n").as_bytes())
            .await
            .unwrap();
        let mut line = String::new();
        reader.read_line(&mut line).await.unwrap();
        assert!(line.contains("\"queued\""), "{line}");
        reader
    }

    /// The tokens of the child's grant, if it arrives within `within`.
    async fn granted_within(
        reader: &mut tokio::io::BufReader<tokio::net::UnixStream>,
        within: std::time::Duration,
    ) -> Option<u64> {
        use tokio::io::AsyncBufReadExt as _;
        let mut line = String::new();
        tokio::time::timeout(within, reader.read_line(&mut line))
            .await
            .ok()?
            .unwrap();
        let granted: serde_json::Value = serde_json::from_str(&line).unwrap();
        assert_eq!(granted["lease"], "granted", "{granted}");
        granted["tokens"].as_u64()
    }

    /// A lent pool's free tokens are its fifo's bytes and its holder's own token: a Cargo reading
    /// the fifo and a child asking the socket draw on one count, never on two.
    #[tokio::test]
    async fn a_lent_pool_lends_its_fifo_bytes_and_its_holders_own_token_and_nothing_twice() {
        let root = crate::control::test_fixture_dir::secure_fixture_dir_in(
            &crate::control::test_fixture_dir::scratch_parent(),
            &format!(
                "cowshed-lend-{}",
                &uuid::Uuid::new_v4().simple().to_string()[..8]
            ),
        );
        let fifos = JobserverFifos::start(root.path().join("store/jobserver"));
        let pool = fifos
            .create_pool(
                PoolSource::Lent(std::num::NonZeroUsize::new(3).unwrap()),
                "0123456789abcdef0123456789abcdef",
            )
            .unwrap();
        let endpoint = pool.path().to_owned();
        let fifo = pool.fifo().expect("a lent pool has a fifo").to_owned();
        assert_eq!(
            std::fs::metadata(&fifo).unwrap().permissions().mode() & 0o777,
            0o600
        );
        // A Cargo reading the fifo takes both of its bytes, as its jobserver does for two rustc.
        let mut cargo = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&fifo)
            .unwrap();
        let mut taken = [0_u8; 2];
        cargo.read_exact(&mut taken).unwrap();
        assert_eq!(
            taken, [TOKEN_BYTE; 2],
            "a pool of 3 puts 2 bytes in its fifo"
        );
        // A child asking for every core is lent the holder's own token, the one left.
        let mut first = child(&endpoint, 8).await;
        let window = std::time::Duration::from_millis(200);
        assert_eq!(granted_within(&mut first, window).await, Some(1));
        // Nothing is free: the next child waits, whatever it asks for.
        let mut second = child(&endpoint, 8).await;
        assert_eq!(granted_within(&mut second, window).await, None);
        // Cargo writes its tokens back: the waiting child takes both of them.
        cargo.write_all(&taken).unwrap();
        assert_eq!(
            granted_within(&mut second, std::time::Duration::from_secs(5)).await,
            Some(2)
        );
        // The child's close puts them back in the fifo, where Cargo reads them again.
        drop(second);
        let mut again = [0_u8; 2];
        tokio::task::spawn_blocking(move || {
            cargo.read_exact(&mut again).unwrap();
            again
        })
        .await
        .unwrap();
        drop(first);
        pool.close().await.unwrap();
        assert!(!fifo.exists());
    }

    #[tokio::test]
    async fn retiring_a_pool_answers_every_connection_still_in_its_accept_queue() {
        use tokio::io::AsyncReadExt as _;

        for explicit_close in [true, false] {
            let scratch = crate::control::test_fixture_dir::secure_fixture_dir(&format!(
                "cspr-{}",
                &uuid::Uuid::new_v4().simple().to_string()[..8]
            ));
            let root = scratch.path().join("s");
            let pool = CpuPool::start(
                PoolScope {
                    directory: std::sync::Arc::new(root.clone()),
                    gate: None,
                },
                PoolSource::Lent(std::num::NonZeroUsize::new(2).unwrap()),
            )
            .unwrap();
            let endpoint = pool.path().to_owned();
            let fifo = pool.fifo().expect("a lent pool has a fifo").to_owned();
            // This current-thread runtime has not polled the listener task yet. All eight
            // real connections are in its kernel accept queue, not in a serving task.
            let mut clients = Vec::new();
            for _ in 0..8 {
                let mut client = cowshed_os::fork_lock::create(|| {
                    std::os::unix::net::UnixStream::connect(&endpoint)
                })
                .unwrap();
                client
                    .write_all(b"{\"op\":\"cpu-pool\",\"want\":1}\n")
                    .unwrap();
                client.set_nonblocking(true).unwrap();
                clients.push(tokio::net::UnixStream::from_std(client).unwrap());
            }
            if explicit_close {
                pool.close().await.unwrap();
            } else {
                drop(pool);
            }
            for mut client in clients {
                let mut response = String::new();
                client.read_to_string(&mut response).await.unwrap();
                assert!(
                    !response.is_empty(),
                    "retirement silently dropped a queued connection"
                );
                let response: serde_json::Value = serde_json::from_str(&response).unwrap();
                assert_eq!(response["ok"], false);
                assert_eq!(response["code"], "pool-retired");
            }
            assert!(!endpoint.exists());
            assert!(!fifo.exists(), "retirement unlinks the pool's fifo");
        }
    }

    fn after(origin: std::time::Instant, seconds: u64) -> std::time::Instant {
        origin + std::time::Duration::from_secs(seconds)
    }

    /// A pool granted less than it asked tops up while its fifo is empty and gives an added token
    /// back once it sits idle; an offer a reader took is followed at once, an idle one backs off.
    #[test]
    fn a_pool_below_its_want_tops_up_while_empty_and_gives_back_what_sits_idle() {
        use crate::cpu_budget::TopUpOutcome::Granted;
        let origin = std::time::Instant::now();
        let mut growth = Growth::new(origin, 3);
        assert_eq!(growth.step(origin, 0, 0), GrowthStep::Ask);
        assert_eq!(growth.heard(origin, Granted), None);
        // A reader took it: the next is offered at once.
        assert_eq!(growth.step(after(origin, 1), 0, 0), GrowthStep::Ask);
        assert_eq!(growth.heard(after(origin, 1), Granted), None);
        // This one sits idle: it goes back, and the next offer waits a check.
        assert_eq!(growth.step(after(origin, 2), 1, 0), GrowthStep::GiveBack(1));
        growth.gave_back(1);
        assert_eq!(growth.extra, 1);
        assert_eq!(growth.step(after(origin, 2), 0, 0), GrowthStep::Wait);
        assert_eq!(growth.step(after(origin, 3), 0, 0), GrowthStep::Ask);
        assert_eq!(growth.heard(after(origin, 3), Granted), None);
        // Idle again: the back-off doubles, and a child queued on the socket overrides it.
        assert_eq!(growth.step(after(origin, 4), 1, 0), GrowthStep::GiveBack(1));
        growth.gave_back(1);
        assert_eq!(growth.step(after(origin, 5), 0, 0), GrowthStep::Wait);
        assert_eq!(growth.step(after(origin, 5), 0, 1), GrowthStep::Ask);
        assert_eq!(growth.step(after(origin, 6), 0, 0), GrowthStep::Ask);
        // Base tokens returned to the fifo give back only what was added.
        assert_eq!(growth.heard(after(origin, 6), Granted), None);
        assert_eq!(growth.step(after(origin, 7), 4, 0), GrowthStep::GiveBack(2));
    }

    /// Up to its want and no further; a ledger that says it is settled stops the asks.
    #[test]
    fn a_pool_tops_up_to_its_want_and_stops_once_settled() {
        use crate::cpu_budget::TopUpOutcome::{Granted, Settled};
        let origin = std::time::Instant::now();
        let mut growth = Growth::new(origin, 1);
        assert_eq!(growth.step(origin, 0, 0), GrowthStep::Ask);
        assert_eq!(growth.heard(origin, Granted), None);
        assert_eq!(growth.step(after(origin, 1), 0, 5), GrowthStep::Wait);
        let mut settled = Growth::new(origin, 4);
        assert_eq!(settled.step(origin, 0, 0), GrowthStep::Ask);
        assert_eq!(settled.heard(origin, Settled), None);
        assert_eq!(settled.step(after(origin, 1), 0, 1), GrowthStep::Wait);
    }

    /// A pool whose fifo stays empty while the ledger tops up nothing says it starves within
    /// [`STARVED_AFTER`], once a spell, and again only after its fifo held a token.
    #[test]
    fn an_empty_pool_the_ledger_cannot_top_up_says_it_starves_once_a_spell() {
        use crate::cpu_budget::TopUpOutcome::{Full, Waiting};
        let origin = std::time::Instant::now();
        let mut growth = Growth::new(origin, 17);
        for second in 0..3 {
            assert_eq!(growth.step(after(origin, second), 0, 0), GrowthStep::Ask);
            assert_eq!(growth.heard(after(origin, second), Full), None);
        }
        assert_eq!(growth.step(after(origin, 3), 0, 0), GrowthStep::Ask);
        assert_eq!(
            growth.heard(after(origin, 3), Waiting),
            Some(std::time::Duration::from_secs(3))
        );
        assert_eq!(growth.step(after(origin, 4), 0, 0), GrowthStep::Ask);
        assert_eq!(
            growth.heard(after(origin, 4), Full),
            None,
            "said once a spell"
        );
        assert_eq!(growth.step(after(origin, 5), 1, 0), GrowthStep::Wait);
        assert_eq!(growth.step(after(origin, 6), 0, 0), GrowthStep::Ask);
        assert_eq!(growth.heard(after(origin, 8), Full), None, "a new spell");
        assert_eq!(growth.heard(after(origin, 10), Full), None);
        assert_eq!(
            growth.heard(after(origin, 11), Full),
            Some(std::time::Duration::from_secs(3))
        );
    }

    /// The pool's own back-off is no refusal: an idle offer delays the next ask, and the spell a
    /// starvation measures starts at the first refused ask, which is then asked every check.
    #[test]
    fn a_pools_back_off_never_counts_as_starving() {
        use crate::cpu_budget::TopUpOutcome::{Full, Granted};
        let origin = std::time::Instant::now();
        let mut growth = Growth::new(origin, 4);
        assert_eq!(growth.step(origin, 0, 0), GrowthStep::Ask);
        assert_eq!(growth.heard(origin, Granted), None);
        assert_eq!(growth.step(after(origin, 1), 1, 0), GrowthStep::GiveBack(1));
        growth.gave_back(1);
        for second in 1..2 {
            assert_eq!(growth.step(after(origin, second), 0, 0), GrowthStep::Wait);
        }
        assert_eq!(growth.step(after(origin, 2), 0, 0), GrowthStep::Ask);
        assert_eq!(growth.heard(after(origin, 2), Granted), None);
        assert_eq!(growth.step(after(origin, 3), 1, 0), GrowthStep::GiveBack(1));
        growth.gave_back(1);
        for second in 3..5 {
            assert_eq!(growth.step(after(origin, second), 0, 0), GrowthStep::Wait);
        }
        assert_eq!(growth.step(after(origin, 5), 0, 0), GrowthStep::Ask);
        assert_eq!(
            growth.heard(after(origin, 5), Full),
            None,
            "the first refusal"
        );
        assert_eq!(
            growth.step(after(origin, 6), 0, 0),
            GrowthStep::Ask,
            "asked every check"
        );
        assert_eq!(growth.heard(after(origin, 7), Full), None);
        assert_eq!(
            growth.heard(after(origin, 8), Full),
            Some(std::time::Duration::from_secs(3))
        );
    }

    #[test]
    fn startup_removes_leftover_fifos_and_nothing_else() {
        let (_root, directory) = directory();
        std::fs::create_dir_all(&directory).unwrap();
        let leftover = directory.join("jobserver-1-0.fifo");
        let c_path = std::ffi::CString::new(leftover.as_os_str().as_bytes()).unwrap();
        // SAFETY: c_path is a valid NUL-terminated path for the call.
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        let other = directory.join("control.sock-note");
        std::fs::write(&other, b"keep").unwrap();
        let _fifos = JobserverFifos::start(directory);
        assert!(!leftover.exists());
        assert!(other.exists());
    }
}
