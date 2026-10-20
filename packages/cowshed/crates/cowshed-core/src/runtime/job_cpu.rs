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
    JobserverAnswer, JobserverJob, JobserverRequest, LeaseState, ResidentPoolRequest,
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
    /// background ask does, and the token returns when the stake drops.
    fn activation(
        &self,
        checkout: PathBuf,
        command: String,
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

fn refused(answer: JobserverAnswer) -> CowshedError {
    CowshedError::cpu_admission(CpuAdmissionRefusal::GatewayRefused {
        code: answer.code.unwrap_or_else(|| "refused".to_owned()),
        error: answer
            .error
            .unwrap_or_else(|| "the gateway gave no diagnostic".to_owned()),
    })
}

/// Connect to the gateway at `socket`, write `frame`, and read its `queued` then `granted`
/// answers: the grant and the connection that holds it.
async fn exchange(
    socket: &Path,
    frame: serde_json::Result<Vec<u8>>,
) -> Result<(JobserverAnswer, Fenced<UnixStream>)> {
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
    let mut frame = frame.map_err(|error| {
        CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
            detail: error.to_string(),
        })
    })?;
    frame.push(b'\n');
    connection.write_all(&frame).await.map_err(|error| {
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
            let (granted, connection) = exchange(&socket, frame).await?;
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
    ) -> Pin<Box<dyn Future<Output = Result<ActivationStake>> + Send>> {
        let socket = Arc::clone(&self.socket);
        Box::pin(async move {
            let frame = serde_json::to_vec(&JobserverRequest::activation(
                &checkout.to_string_lossy(),
                wire_command(&command),
            ));
            let (granted, connection) = exchange(&socket, frame).await?;
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
