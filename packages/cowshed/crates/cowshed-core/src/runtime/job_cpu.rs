//! Supervisor-owned CPU admission (05_gateway.md, Host CPU budget; 11_shell.md, Supervisor).
//! The connection is the root allocation; cancelling a pending future or ending its job closes
//! it under the process fork fence, returns whole tokens and revokes the private child pool.

use cowshed_os::fork_lock::Fenced;
use std::fmt;
use std::future::Future;
use std::num::{NonZeroU64, NonZeroUsize};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use cowshed_gateway_types::{
    INSTALL_RESTART_CODE, InstallRestart, JobserverAnswer, JobserverJob, JobserverRequest,
    LeaseState, ResidentPoolRequest,
};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;

use crate::api::dto::{ExecClass, JobId};
use crate::error::{CowshedError, CpuAdmissionRefusal, Result};
use crate::metadata::WorkspaceIncarnation;
use crate::repository::RepoId;

#[derive(Debug)]
pub struct JobCpuAsk {
    pub class: ExecClass,
    pub want: NonZeroUsize,
    pub checkout: PathBuf,
    pub command: String,
    pub job: (RepoId, WorkspaceIncarnation, JobId),
    /// The live CPU pool of the process that asked for the job, when it holds one: a
    /// token-holding job waits on it and on the host ledger at once, and the first grant serves
    /// it (05_gateway.md "Work a holder starts draws on its grant"). A resident holds no token,
    /// so it asks the host through its own relay pool as before.
    pub parent: Option<String>,
}

/// The live pool this process inherited, which the jobs it asks for draw on: the controller
/// runs in the requester's process (in-process for the CLI and every embedder; `cowshed
/// controller` inherits its embedder's environment), so its descriptor is the requester's.
pub fn requester_cpu_pool() -> Option<String> {
    cowshed_gateway_types::inherited_cpu_pool(|name| std::env::var(name).ok())
        .and_then(|pool| pool.into_os_string().into_string().ok())
}

pub trait JobCpuAdmission: Send + Sync + fmt::Debug + 'static {
    fn admit(&self, ask: JobCpuAsk) -> Pin<Box<dyn Future<Output = Result<JobCpuGrant>> + Send>>;
    /// One background token for a shell activation that has no command yet to use it -- a
    /// resident's start, or a shell pool's spare (05_gateway.md "A resident holds no token";
    /// 11_shell.md, spare) -- for `checkout`, `command` naming what activates. It waits as any
    /// background ask does, and the token returns when the stake drops. `parent`, a holder's live
    /// CPU pool, is asked alongside the host ledger, and the first grant serves the stake
    /// (05_gateway.md "Work a holder starts draws on its grant", "A resident's start holds one
    /// token").
    fn activation(
        &self,
        checkout: PathBuf,
        command: String,
        parent: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<ActivationStake>> + Send>>;
}

#[derive(Debug)]
pub struct GatewayJobCpu {
    socket: Arc<PathBuf>,
}
impl GatewayJobCpu {
    pub fn new(socket: PathBuf) -> Self {
        Self {
            socket: Arc::new(socket),
        }
    }
}

enum Holder {
    Gateway { _connection: Fenced<UnixStream> },
    Injected { _owner: Box<dyn Send + Sync> },
}

/// The tokens a job holds and the make jobserver fifo that holds them beside its pool, which the
/// job's command reads as `CARGO_MAKEFLAGS` (05_gateway.md "Cargo reads its holder's jobserver").
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct HeldTokens {
    pub tokens: NonZeroUsize,
    pub fifo: PathBuf,
}

/// A job's standing with the host budget: the tokens it holds and their fifo, or none for a
/// resident, and the live pool its descendants borrow from -- the grant's own tokens, a gate's
/// relay, or a resident's relay that asks the host for each child.
pub struct JobCpuGrant {
    held: Option<HeldTokens>,
    pool: PathBuf,
    /// A resident's one background token while its shell activates, dropped at its command's
    /// start ([`Self::started`]).
    start: Option<ActivationStake>,
    _holder: Holder,
}
impl fmt::Debug for JobCpuGrant {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("JobCpuGrant")
            .field("held", &self.held)
            .field("pool", &self.pool)
            .field("start", &self.start.is_some())
            .finish_non_exhaustive()
    }
}
impl JobCpuGrant {
    /// An injected admission provider owns the real allocation represented by this holder.
    /// Production uses the gateway connection; this seam also lets external behavioral fixtures
    /// supply their own owning allocation without pretending an environment count is a grant.
    pub fn new(held: HeldTokens, pool: PathBuf, holder: Box<dyn Send + Sync>) -> Self {
        Self {
            held: Some(held),
            pool,
            start: None,
            _holder: Holder::Injected { _owner: holder },
        }
    }
    /// An injected resident standing: a pool and no tokens.
    pub fn resident(pool: PathBuf, holder: Box<dyn Send + Sync>) -> Self {
        Self {
            held: None,
            pool,
            start: None,
            _holder: Holder::Injected { _owner: holder },
        }
    }
    /// This grant, holding `stake` until its command starts.
    #[must_use]
    pub fn holding_until_started(mut self, stake: ActivationStake) -> Self {
        self.start = Some(stake);
        self
    }
    /// The job's command started: a start stake returns its token. Whether one was held.
    pub fn started(&mut self) -> bool {
        self.start.take().is_some()
    }
    /// The tokens the job holds; `None` for a resident, which holds none.
    pub fn tokens(&self) -> Option<NonZeroUsize> {
        self.held.as_ref().map(|held| held.tokens)
    }
    /// The tokens and their fifo; `None` for a resident.
    pub fn held(&self) -> Option<&HeldTokens> {
        self.held.as_ref()
    }
    pub fn pool(&self) -> &Path {
        &self.pool
    }
}

/// One background token held while a shell activates ([`JobCpuAdmission::activation`]). The
/// token returns to the host ledger when this drops.
pub struct ActivationStake {
    _holder: Holder,
}
impl fmt::Debug for ActivationStake {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("ActivationStake").finish_non_exhaustive()
    }
}
impl ActivationStake {
    /// An injected stake: `holder` owns the allocation it stands for.
    pub fn new(holder: Box<dyn Send + Sync>) -> Self {
        Self {
            _holder: Holder::Injected { _owner: holder },
        }
    }
}

const MAX_ANSWER: u64 = 1024 * 1024;
const ACK_BOUND: Duration = Duration::from_secs(2);

async fn answer(reader: &mut BufReader<Fenced<UnixStream>>) -> Result<JobserverAnswer> {
    let mut bytes = Vec::new();
    reader
        .take(MAX_ANSWER + 1)
        .read_until(b'\n', &mut bytes)
        .await
        .map_err(|error| {
            CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
                detail: error.to_string(),
            })
        })?;
    if bytes.is_empty()
        || bytes.len() > usize::try_from(MAX_ANSWER).expect("the framing bound fits usize")
        || !bytes.ends_with(b"\n")
    {
        return Err(CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
            detail: "the gateway closed or sent an oversized/incomplete jobserver answer"
                .to_owned(),
        }));
    }
    serde_json::from_slice(&bytes).map_err(|error| {
        CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
            detail: error.to_string(),
        })
    })
}

/// The refusal a gateway's answer names: a cut-off by an install keeps that install, every other
/// refusal is kept as the gateway said it.
fn refused(answer: JobserverAnswer) -> CowshedError {
    let code = answer.code.unwrap_or_else(|| "refused".to_owned());
    let error = answer
        .error
        .unwrap_or_else(|| "the gateway gave no diagnostic".to_owned());
    CowshedError::cpu_admission(match answer.install {
        Some(install) if code == INSTALL_RESTART_CODE => {
            CpuAdmissionRefusal::CutOffByInstall { install, error }
        }
        _ => CpuAdmissionRefusal::GatewayRefused { code, error },
    })
}

/// Connect to the gateway at `socket`, write `line`, and read its `queued` then `granted`
/// answers: the grant and the connection that holds it.
async fn exchange(socket: &Path, line: &[u8]) -> Result<(JobserverAnswer, Fenced<UnixStream>)> {
    let mut connect = std::pin::pin!(UnixStream::connect(socket));
    // Darwin creates a socket then sets CLOEXEC. Fence each poll, never an async wait,
    // so no concurrently spawned child can keep this lease descriptor beyond exec.
    let stream = std::future::poll_fn(|cx| {
        cowshed_os::fork_lock::create(|| Ok(connect.as_mut().poll(cx)))
            .expect("polling the connection runs an infallible closure")
    })
    .await
    .map_err(|error| {
        CowshedError::cpu_admission(CpuAdmissionRefusal::GatewayUnavailable {
            socket: socket.to_path_buf(),
            error: error.to_string(),
        })
    })?;
    let mut connection = Fenced::new(stream);
    connection.write_all(line).await.map_err(|error| {
        CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
            detail: error.to_string(),
        })
    })?;
    let mut reader = BufReader::new(connection);
    let queued = tokio::time::timeout(ACK_BOUND, answer(&mut reader))
        .await
        .map_err(|_| {
            CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
                detail: "the gateway did not acknowledge jobserver admission within 2000 ms"
                    .to_owned(),
            })
        })??;
    if !queued.ok {
        return Err(refused(queued));
    }
    if queued.lease != Some(LeaseState::Queued) {
        return Err(CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
            detail: "expected the jobserver queued acknowledgement".to_owned(),
        }));
    }
    let granted = answer(&mut reader).await?;
    if !granted.ok {
        return Err(refused(granted));
    }
    if granted.lease != Some(LeaseState::Granted) {
        return Err(CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
            detail: "expected a jobserver grant after queued".to_owned(),
        }));
    }
    Ok((granted, reader.into_inner()))
}

/// One request line for the gateway: the request serialised once and newline-terminated, so
/// every attempt at the same ask sends the same bytes.
fn request_line(request: serde_json::Result<Vec<u8>>) -> Result<Vec<u8>> {
    let mut line = request.map_err(|error| {
        CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
            detail: error.to_string(),
        })
    })?;
    line.push(b'\n');
    Ok(line)
}

/// How long a cut-off ask waits before it asks again while the install's gateway comes up.
const INSTALL_RETRY_EVERY: Duration = Duration::from_millis(100);

/// The install that cut this ask off, when the gateway's refusal named one.
fn cut_off_install(error: &CowshedError) -> Option<InstallRestart> {
    match error.cpu_admission_source()? {
        CpuAdmissionRefusal::CutOffByInstall { install, .. } => Some(install.clone()),
        _ => None,
    }
}

/// The gateway's socket is absent or refuses: the old gateway is gone and the one an install
/// starts has not bound yet.
fn gateway_unreachable(error: &CowshedError) -> bool {
    matches!(
        error.cpu_admission_source(),
        Some(CpuAdmissionRefusal::GatewayUnavailable { .. })
    )
}

/// `exchange`, with the one retry an install's restart earns (05_gateway.md "A queued CPU ask
/// outlives an install"): a cut-off ask is sent again, with the same bytes, to the gateway the
/// install starts, for no longer than the install's `servesWithinMs`. Every other outcome is
/// returned as it arrived.
async fn exchange_across_install(
    socket: &Path,
    line: &[u8],
) -> Result<(JobserverAnswer, Fenced<UnixStream>)> {
    let install = match exchange(socket, line).await {
        Ok(granted) => return Ok(granted),
        Err(error) => match cut_off_install(&error) {
            Some(install) => install,
            None => return Err(error),
        },
    };
    let started = tokio::time::Instant::now();
    let deadline = started + Duration::from_millis(install.serves_within_ms);
    loop {
        tokio::time::sleep(INSTALL_RETRY_EVERY).await;
        if tokio::time::Instant::now() >= deadline {
            let waited_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
            return Err(CowshedError::cpu_admission(
                CpuAdmissionRefusal::InstallDidNotServe { install, waited_ms },
            ));
        }
        match exchange(socket, line).await {
            Ok(granted) => return Ok(granted),
            Err(error) if gateway_unreachable(&error) => {}
            Err(error) => match cut_off_install(&error) {
                Some(again) if again.pid == install.pid => {}
                _ => return Err(error),
            },
        }
    }
}

/// `command` cut to the gateway's 512-byte bound at a character boundary.
fn wire_command(command: &str) -> &str {
    let mut end = command.len().min(512);
    while !command.is_char_boundary(end) {
        end -= 1;
    }
    &command[..end]
}

impl JobCpuAdmission for GatewayJobCpu {
    fn admit(&self, ask: JobCpuAsk) -> Pin<Box<dyn Future<Output = Result<JobCpuGrant>> + Send>> {
        let socket = Arc::clone(&self.socket);
        Box::pin(async move {
            let checkout = ask.checkout.to_string_lossy();
            let command = wire_command(&ask.command);
            let job = JobserverJob {
                repo_id: ask.job.0.as_str(),
                workspace_incarnation: ask.job.1.as_str(),
                job_id: NonZeroU64::new(ask.job.2.get()).expect("a validated JobId is nonzero"),
            };
            // The ledger knows two token-holding classes (05_gateway.md "Bands"): interactive
            // asks as background, whose tokens it shares, and either resident asks for a pool alone.
            let frame = match ask.class {
                ExecClass::Background | ExecClass::Interactive => {
                    serde_json::to_vec(&JobserverRequest::background(
                        ask.want,
                        &checkout,
                        command,
                        job,
                        ask.parent.as_deref(),
                    ))
                }
                // A gate always takes its place in the repository's host queue. Borrowing a
                // parent's token would start a second gate without that admission.
                ExecClass::Gate => {
                    serde_json::to_vec(&JobserverRequest::gate(ask.want, &checkout, command, job))
                }
                ExecClass::Resident | ExecClass::InteractiveResident => {
                    serde_json::to_vec(&ResidentPoolRequest::new(&checkout, command, job))
                }
            };
            let line = request_line(frame)?;
            let (granted, connection) = exchange_across_install(&socket, &line).await?;
            // A token-holding grant names its tokens, its fifo and its pool; a resident's names
            // only its pool, since it holds no token and its children ask the host through it.
            let (held, pool) = match (ask.class, granted.tokens, granted.cpu_pool, granted.fifo) {
                (
                    ExecClass::Background | ExecClass::Interactive | ExecClass::Gate,
                    Some(tokens),
                    Some(pool),
                    Some(fifo),
                ) => (Some(HeldTokens { tokens, fifo }), pool),
                (ExecClass::Resident | ExecClass::InteractiveResident, None, Some(pool), None) => {
                    (None, pool)
                }
                (class, ..) => {
                    return Err(CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
                        detail: format!(
                            "the {class:?} grant does not have its class's shape: tokens, FIFO \
                             and live child pool for a token-holding class, the pool alone for \
                             a resident"
                        ),
                    }));
                }
            };
            let store = socket.parent().ok_or_else(|| {
                CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
                    detail: "the gateway socket has no runtime parent".to_owned(),
                })
            })?;
            let scope = cowshed_gateway_types::cpu_pool_directory(store, ask.job.1.as_str())
                .map_err(|error| {
                    CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
                        detail: error.to_string(),
                    })
                })?;
            if held.as_ref().is_some_and(|held| {
                held.tokens > ask.want || held.fifo.parent() != Some(scope.as_path())
            }) || pool.parent() != Some(scope.as_path())
            {
                return Err(CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
                    detail:
                        "the gateway grant exceeds its request or escapes its workspace pool scope"
                            .to_owned(),
                }));
            }
            cowshed_os::timing::event("jobserver", || {
                format!(
                    "grant class={:?} tokens={}/{} pool={} checkout={}",
                    ask.class,
                    held.as_ref().map_or(0, |held| held.tokens.get()),
                    ask.want,
                    pool.display(),
                    ask.checkout.display()
                )
            });
            Ok(JobCpuGrant {
                held,
                pool,
                start: None,
                _holder: Holder::Gateway {
                    _connection: connection,
                },
            })
        })
    }

    fn activation(
        &self,
        checkout: PathBuf,
        command: String,
        parent: Option<String>,
    ) -> Pin<Box<dyn Future<Output = Result<ActivationStake>> + Send>> {
        let socket = Arc::clone(&self.socket);
        Box::pin(async move {
            let frame = serde_json::to_vec(&JobserverRequest::activation(
                &checkout.to_string_lossy(),
                wire_command(&command),
                parent.as_deref(),
            ));
            let line = request_line(frame)?;
            let (granted, connection) = exchange_across_install(&socket, &line).await?;
            // One token and the fifo that holds it, and no pool: nothing draws on a stake.
            match (granted.tokens, granted.cpu_pool) {
                (Some(tokens), None) if tokens == NonZeroUsize::MIN => {}
                (tokens, pool) => {
                    return Err(CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
                        detail: format!(
                            "an activation stake is one token and no pool, not {tokens:?} \
                             tokens and pool {pool:?}"
                        ),
                    }));
                }
            }
            cowshed_os::timing::event("jobserver", || {
                format!(
                    "grant activation tokens=1/1 checkout={} command={command}",
                    checkout.display()
                )
            });
            Ok(ActivationStake {
                _holder: Holder::Gateway {
                    _connection: connection,
                },
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use cowshed_gateway_types::cpu_pool_directory;
    use serde_json::json;
    use tokio::net::UnixListener;
    use tokio::task::JoinHandle;

    use super::*;

    // These tests run on the real clock. A paused clock can auto-advance past the 2 s ACK bound
    // while a real socket's readiness is still pending, so a live gateway looks dead. The real
    // clock keeps every bound unreached on success; assertions check request order, not elapsed
    // time.

    /// The workspace incarnation the test gateway's pools are scoped by.
    const INCARNATION: &str = "0123456789abcdef0123456789abcdef";

    /// One gateway lifetime: after `after` it binds the socket and answers each of its `asks` in
    /// turn, each ask being the lines written after its request line is read.
    struct Generation {
        after: Duration,
        asks: Vec<Vec<String>>,
    }

    /// A fresh directory under the scratch parent, short enough that its gateway socket path fits `sun_path`.
    fn new_store() -> PathBuf {
        let dir = crate::temp_parent::scratch_parent().join(format!(
            "cs-jc-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        std::fs::create_dir_all(&dir).expect("create the test store");
        dir
    }

    fn background() -> JobCpuAsk {
        JobCpuAsk {
            class: ExecClass::Background,
            want: NonZeroUsize::MIN,
            checkout: PathBuf::from("/abs/checkout"),
            command: "cargo test".to_owned(),
            job: (
                RepoId::parse("acme/widget").expect("a repository"),
                WorkspaceIncarnation::new(INCARNATION).expect("a workspace incarnation"),
                JobId::new(1).expect("a job"),
            ),
            parent: None,
        }
    }

    fn queued_line() -> String {
        json!({"ok": true, "lease": "queued"}).to_string()
    }

    /// A grant of one token whose fifo and pool lie in the store's pool scope.
    fn grant_line(store: &Path) -> String {
        let scope = cpu_pool_directory(store, INCARNATION).expect("the pool scope");
        json!({
            "ok": true,
            "lease": "granted",
            "tokens": 1,
            "fifo": scope.join("fifo"),
            "cpuPool": scope.join("pool"),
        })
        .to_string()
    }

    fn cut_off_line(pid: u32, serves_within_ms: u64) -> String {
        json!({
            "ok": false,
            "code": INSTALL_RESTART_CODE,
            "error": "the install restarts the gateway",
            "install": {
                "pid": pid,
                "binary": "/abs/path/cowshed",
                "servesWithinMs": serves_within_ms,
            },
        })
        .to_string()
    }

    fn refusal_line(code: &str, error: &str) -> String {
        json!({"ok": false, "code": code, "error": error}).to_string()
    }

    /// Serves one ask on `listener`: records its request line in `received`, then writes `replies`.
    async fn serve_ask(listener: &UnixListener, replies: &[String], received: &mut Vec<Vec<u8>>) {
        let (stream, _) = listener.accept().await.expect("accept an ask");
        let (read, mut write) = stream.into_split();
        let mut request = Vec::new();
        BufReader::new(read)
            .read_until(b'\n', &mut request)
            .await
            .expect("read an ask");
        received.push(request);
        for reply in replies {
            write
                .write_all(format!("{reply}\n").as_bytes())
                .await
                .expect("write a reply");
        }
    }

    /// Plays the gateway at `socket` with real sockets. The first generation binds before this
    /// returns, so the first ask finds it; each later one binds once its delay has passed and the
    /// one before has gone. The task yields every request line the gateway received, in order.
    fn play_gateway(socket: PathBuf, generations: Vec<Generation>) -> JoinHandle<Vec<Vec<u8>>> {
        let mut generations = generations.into_iter();
        let first = generations
            .next()
            .expect("a gateway has a first generation");
        assert_eq!(
            first.after,
            Duration::ZERO,
            "the first generation binds at once"
        );
        let listener = UnixListener::bind(&socket).expect("bind the first gateway generation");
        tokio::spawn(async move {
            let mut received = Vec::new();
            for ask in first.asks {
                serve_ask(&listener, &ask, &mut received).await;
            }
            drop(listener);
            for generation in generations {
                tokio::time::sleep(generation.after).await;
                if socket.exists() {
                    std::fs::remove_file(&socket).expect("remove the stale gateway socket");
                }
                let listener =
                    UnixListener::bind(&socket).expect("bind the next gateway generation");
                for ask in generation.asks {
                    serve_ask(&listener, &ask, &mut received).await;
                }
            }
            received
        })
    }

    /// Plays a gateway that answers every ask with `replies` and counts the asks it has served.
    fn spawn_counting_gateway(
        socket: PathBuf,
        replies: Vec<String>,
    ) -> (JoinHandle<()>, Arc<AtomicUsize>) {
        let listener = UnixListener::bind(&socket).expect("bind the counting gateway");
        let served = Arc::new(AtomicUsize::new(0));
        let counted = Arc::clone(&served);
        let task = tokio::spawn(async move {
            let mut received = Vec::new();
            loop {
                serve_ask(&listener, &replies, &mut received).await;
                counted.fetch_add(1, Ordering::SeqCst);
            }
        });
        (task, served)
    }

    /// Asks a gateway that answers `replies` once and returns the refusal, together with how many
    /// asks that gateway served two seconds later: a retry would show there.
    async fn refusal_and_served(replies: Vec<String>) -> (CowshedError, usize) {
        let dir = new_store();
        let socket = dir.join("gateway.sock");
        let (gateway, served) = spawn_counting_gateway(socket.clone(), replies);
        let error = GatewayJobCpu::new(socket)
            .admit(background())
            .await
            .expect_err("the gateway refuses the ask");
        tokio::time::sleep(Duration::from_secs(2)).await;
        gateway.abort();
        std::fs::remove_dir_all(&dir).expect("remove the test store");
        (error, served.load(Ordering::SeqCst))
    }

    #[tokio::test]
    async fn a_cut_off_ask_is_asked_again_once_the_install_gateway_serves() {
        let dir = new_store();
        let socket = dir.join("gateway.sock");
        let gateway = play_gateway(
            socket.clone(),
            vec![
                Generation {
                    after: Duration::ZERO,
                    asks: vec![vec![queued_line(), cut_off_line(4242, 10_000)]],
                },
                Generation {
                    after: Duration::from_millis(250),
                    asks: vec![vec![queued_line(), grant_line(&dir)]],
                },
            ],
        );
        let grant = GatewayJobCpu::new(socket)
            .admit(background())
            .await
            .expect("the retry is granted");
        drop(grant);
        let received = gateway.await.expect("the gateway task");
        assert_eq!(received.len(), 2, "the ask, then its one retry");
        assert_eq!(
            received[0], received[1],
            "the retry sends the same request line"
        );
        std::fs::remove_dir_all(&dir).expect("remove the test store");
    }

    #[tokio::test]
    async fn the_same_install_still_draining_is_asked_again() {
        let dir = new_store();
        let socket = dir.join("gateway.sock");
        let gateway = play_gateway(
            socket.clone(),
            vec![
                Generation {
                    after: Duration::ZERO,
                    asks: vec![
                        vec![queued_line(), cut_off_line(4242, 10_000)],
                        vec![queued_line(), cut_off_line(4242, 10_000)],
                    ],
                },
                Generation {
                    after: Duration::from_millis(250),
                    asks: vec![vec![queued_line(), grant_line(&dir)]],
                },
            ],
        );
        let grant = GatewayJobCpu::new(socket)
            .admit(background())
            .await
            .expect("the retry is granted");
        drop(grant);
        let received = gateway.await.expect("the gateway task");
        assert_eq!(
            received.len(),
            3,
            "the ask, the draining gateway's cut-off, and the grant"
        );
        assert!(
            received.iter().all(|line| *line == received[0]),
            "every retry sends the same request line"
        );
        std::fs::remove_dir_all(&dir).expect("remove the test store");
    }

    #[tokio::test]
    async fn an_install_other_than_the_first_is_returned_as_it_arrived() {
        let dir = new_store();
        let socket = dir.join("gateway.sock");
        let gateway = play_gateway(
            socket.clone(),
            vec![
                Generation {
                    after: Duration::ZERO,
                    asks: vec![vec![queued_line(), cut_off_line(4242, 10_000)]],
                },
                Generation {
                    after: Duration::from_millis(250),
                    asks: vec![vec![queued_line(), cut_off_line(5555, 10_000)]],
                },
            ],
        );
        let error = GatewayJobCpu::new(socket)
            .admit(background())
            .await
            .expect_err("the other install's refusal is returned");
        assert_eq!(
            error.cpu_admission_source(),
            Some(&CpuAdmissionRefusal::CutOffByInstall {
                install: InstallRestart {
                    pid: 5555,
                    binary: PathBuf::from("/abs/path/cowshed"),
                    serves_within_ms: 10_000,
                },
                error: "the install restarts the gateway".to_owned(),
            })
        );
        let received = gateway.await.expect("the gateway task");
        assert_eq!(
            received.len(),
            2,
            "no retry follows another install's refusal"
        );
        std::fs::remove_dir_all(&dir).expect("remove the test store");
    }

    #[tokio::test]
    async fn an_install_that_never_serves_is_given_up_on_at_its_bound() {
        let dir = new_store();
        let socket = dir.join("gateway.sock");
        let gateway = play_gateway(
            socket.clone(),
            vec![Generation {
                after: Duration::ZERO,
                asks: vec![vec![queued_line(), cut_off_line(4242, 1_000)]],
            }],
        );
        let error = GatewayJobCpu::new(socket)
            .admit(background())
            .await
            .expect_err("the install never serves");
        match error.cpu_admission_source() {
            Some(CpuAdmissionRefusal::InstallDidNotServe { install, .. }) => {
                assert_eq!(install.pid, 4242);
            }
            other => panic!("expected the install's bound to be named, got {other:?}"),
        }
        assert!(
            error.message.contains(
                "the install 4242 (/abs/path/cowshed) restarting the gateway did not bring up one \
                 that serves within 1000 ms; waited "
            ),
            "{}",
            error.message
        );
        let received = gateway.await.expect("the gateway task");
        assert_eq!(
            received.len(),
            1,
            "nothing serves after the bound, so no ask follows"
        );
        std::fs::remove_dir_all(&dir).expect("remove the test store");
    }

    #[tokio::test]
    async fn any_other_refusal_is_returned_unchanged_and_never_asked_again() {
        let (error, served) = refusal_and_served(vec![
            queued_line(),
            refusal_line("rejected", "the gate is closed"),
        ])
        .await;
        assert_eq!(
            error.cpu_admission_source(),
            Some(&CpuAdmissionRefusal::GatewayRefused {
                code: "rejected".to_owned(),
                error: "the gate is closed".to_owned(),
            })
        );
        assert_eq!(served, 1, "a refusal of another code is never asked again");
    }

    #[tokio::test]
    async fn an_install_restart_naming_no_install_is_a_plain_refusal() {
        let (error, served) = refusal_and_served(vec![
            queued_line(),
            refusal_line(INSTALL_RESTART_CODE, "no install named"),
        ])
        .await;
        assert_eq!(
            error.cpu_admission_source(),
            Some(&CpuAdmissionRefusal::GatewayRefused {
                code: INSTALL_RESTART_CODE.to_owned(),
                error: "no install named".to_owned(),
            })
        );
        assert_eq!(
            served, 1,
            "a cut-off that names no install is never asked again"
        );
    }

    #[tokio::test]
    async fn an_activation_is_asked_again_across_an_install() {
        let dir = new_store();
        let socket = dir.join("gateway.sock");
        let gateway = play_gateway(
            socket.clone(),
            vec![
                Generation {
                    after: Duration::ZERO,
                    asks: vec![vec![queued_line(), cut_off_line(4242, 10_000)]],
                },
                Generation {
                    after: Duration::from_millis(250),
                    asks: vec![vec![
                        queued_line(),
                        json!({"ok": true, "lease": "granted", "tokens": 1}).to_string(),
                    ]],
                },
            ],
        );
        let stake = GatewayJobCpu::new(socket)
            .activation(
                PathBuf::from("/abs/checkout"),
                "cargo test".to_owned(),
                None,
            )
            .await
            .expect("the retry is granted");
        drop(stake);
        let received = gateway.await.expect("the gateway task");
        assert_eq!(
            received.len(),
            2,
            "the activation's ask, then its one retry"
        );
        std::fs::remove_dir_all(&dir).expect("remove the test store");
    }

    /// The request frame an admission of `class` sends, read off a fake gateway that never
    /// answers: the admission is still waiting for its grant when the frame is read.
    async fn request_frame(class: ExecClass) -> serde_json::Value {
        let root = crate::temp_root::TempRoot::new_in(
            &crate::temp_parent::canonical_parent(),
            "cowshed-job-cpu-frame",
        );
        let socket = root.join("gateway.sock");
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind the fake gateway");
        let admission = GatewayJobCpu::new(socket).admit(JobCpuAsk {
            class,
            want: NonZeroUsize::new(8).expect("eight"),
            checkout: root.join("checkout"),
            command: "nx run-many -t gate".to_owned(),
            job: (
                RepoId::parse("acme/widget").expect("repo"),
                WorkspaceIncarnation::new(format!("{:032x}", 7)).expect("incarnation"),
                JobId::new(7).expect("job"),
            ),
            parent: Some("/requester/pool.sock".to_owned()),
        });
        let gateway = async {
            let (stream, _) = listener.accept().await.expect("the admission connects");
            let mut line = String::new();
            BufReader::new(stream)
                .read_line(&mut line)
                .await
                .expect("the request frame");
            line
        };
        tokio::select! {
            _ = admission => panic!("an admission the gateway never answered ended"),
            line = gateway => serde_json::from_str(&line).expect("a JSON frame"),
        }
    }

    /// A gate asks its repository's gate queue: the gate class, the job it serves, and never its
    /// requester's pool, which would start it outside that queue. Background work asks as
    /// background and may draw on its requester's pool. The exec's scope is not on the wire, so
    /// a host gate queues exactly as a gate in the sandbox does, and host background work does
    /// not queue as a gate.
    #[tokio::test]
    async fn a_gate_asks_its_repositorys_gate_queue_and_background_work_does_not() {
        let gate = request_frame(ExecClass::Gate).await;
        assert_eq!(
            (
                &gate["op"],
                &gate["class"],
                &gate["job"]["repoId"],
                &gate["job"]["jobId"]
            ),
            (
                &json!("jobserver"),
                &json!("gate"),
                &json!("acme/widget"),
                &json!(7)
            ),
            "{gate}"
        );
        assert!(gate.get("parentPool").is_none(), "{gate}");
        assert!(gate.get("scope").is_none(), "{gate}");

        let background = request_frame(ExecClass::Background).await;
        assert_eq!(
            (&background["class"], &background["parentPool"]),
            (&json!("background"), &json!("/requester/pool.sock")),
            "{background}"
        );
    }
}
