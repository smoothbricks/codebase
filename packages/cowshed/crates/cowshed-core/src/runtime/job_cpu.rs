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
    JobserverAnswer, JobserverClass, JobserverJob, JobserverRequest, LeaseState,
};
use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _, BufReader};
use tokio::net::UnixStream;

use crate::api::dto::JobId;
use crate::error::{CowshedError, CpuAdmissionRefusal, Result};
use crate::metadata::WorkspaceIncarnation;
use crate::repository::RepoId;

#[derive(Debug)]
pub struct JobCpuAsk {
    pub class: JobserverClass,
    pub want: NonZeroUsize,
    pub checkout: PathBuf,
    pub command: String,
    pub job: (RepoId, WorkspaceIncarnation, JobId),
}

pub trait JobCpuAdmission: Send + Sync + fmt::Debug + 'static {
    fn admit(&self, ask: JobCpuAsk) -> Pin<Box<dyn Future<Output = Result<JobCpuGrant>> + Send>>;
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

pub struct JobCpuGrant {
    tokens: NonZeroUsize,
    pool: PathBuf,
    _holder: Holder,
}
impl fmt::Debug for JobCpuGrant {
    fn fmt(&self, out: &mut fmt::Formatter<'_>) -> fmt::Result {
        out.debug_struct("JobCpuGrant")
            .field("tokens", &self.tokens)
            .field("pool", &self.pool)
            .finish_non_exhaustive()
    }
}
impl JobCpuGrant {
    /// An injected admission provider owns the real allocation represented by this holder.
    /// Production uses the gateway connection; this seam also lets external behavioral fixtures
    /// supply their own owning allocation without pretending an environment count is a grant.
    pub fn new(tokens: NonZeroUsize, pool: PathBuf, holder: Box<dyn Send + Sync>) -> Self {
        Self {
            tokens,
            pool,
            _holder: Holder::Injected { _owner: holder },
        }
    }
    pub fn tokens(&self) -> NonZeroUsize {
        self.tokens
    }
    pub fn pool(&self) -> &Path {
        &self.pool
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

impl JobCpuAdmission for GatewayJobCpu {
    fn admit(&self, ask: JobCpuAsk) -> Pin<Box<dyn Future<Output = Result<JobCpuGrant>> + Send>> {
        let socket = Arc::clone(&self.socket);
        Box::pin(async move {
            let mut connect = std::pin::pin!(UnixStream::connect(socket.as_path()));
            // Darwin creates a socket then sets CLOEXEC. Fence each poll, never an async wait,
            // so no concurrently spawned child can keep this lease descriptor beyond exec.
            let stream = std::future::poll_fn(|cx| {
                cowshed_os::fork_lock::create(|| Ok(connect.as_mut().poll(cx)))
                    .expect("polling the connection runs an infallible closure")
            })
            .await
            .map_err(|error| {
                CowshedError::cpu_admission(CpuAdmissionRefusal::GatewayUnavailable {
                    socket: socket.as_ref().clone(),
                    error: error.to_string(),
                })
            })?;
            let mut connection = Fenced::new(stream);
            let checkout = ask.checkout.to_string_lossy();
            let mut end = ask.command.len().min(512);
            while !ask.command.is_char_boundary(end) {
                end -= 1;
            }
            let command = &ask.command[..end];
            let job = JobserverJob {
                repo_id: ask.job.0.as_str(),
                workspace_incarnation: ask.job.1.as_str(),
                job_id: NonZeroU64::new(ask.job.2.get()).expect("a validated JobId is nonzero"),
            };
            let request = match ask.class {
                JobserverClass::Background => {
                    JobserverRequest::background(ask.want, &checkout, command, job)
                }
                JobserverClass::Gate => JobserverRequest::gate(ask.want, &checkout, command, job),
            };
            let mut frame = serde_json::to_vec(&request).map_err(|error| {
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
                        detail:
                            "the gateway did not acknowledge jobserver admission within 2000 ms"
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
            let (Some(tokens), Some(pool), Some(_fifo)) =
                (granted.tokens, granted.cpu_pool, granted.fifo)
            else {
                return Err(CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
                    detail: "jobserver grant omitted its tokens, FIFO or live child pool"
                        .to_owned(),
                }));
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
            if tokens > ask.want || pool.parent() != Some(scope.as_path()) {
                return Err(CowshedError::cpu_admission(CpuAdmissionRefusal::Protocol {
                    detail:
                        "the gateway grant exceeds its request or escapes its workspace pool scope"
                            .to_owned(),
                }));
            }
            let connection = reader.into_inner();
            cowshed_os::timing::event("jobserver", || {
                format!(
                    "grant class={:?} tokens={}/{} pool={} checkout={}",
                    ask.class,
                    tokens,
                    ask.want,
                    pool.display(),
                    ask.checkout.display()
                )
            });
            Ok(JobCpuGrant {
                tokens,
                pool,
                _holder: Holder::Gateway {
                    _connection: connection,
                },
            })
        })
    }
}
