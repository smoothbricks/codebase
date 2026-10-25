use std::{
    collections::BTreeSet,
    fmt,
    net::SocketAddr,
    num::NonZeroUsize,
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::{Deserialize, Serialize};
use subtle::ConstantTimeEq as _;
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader},
    net::{TcpStream, UnixStream},
    sync::mpsc,
};
use zeroize::{Zeroize, Zeroizing};

use cowshed_gateway_types::{
    CPU_TOKENS_OP, CpuBudgetStatus, DISK_LEASE_OP, DaemonStatus, DiskClass, EgressGrant,
    EgressMode, FIRST_LAUNCH_OP, FirstLaunch, FirstLaunchProbe, HostPattern, JOBSERVER_OP,
    LeaseClass, LeaseState, MirrorRoute, PhaseShare, RAM_DETACH_OP, RAM_LEASE_OP, RESIDENT_POOL_OP,
    RamAttachRefusal, RamDetached, RamDiskAttached, RamDiskDevice, RamDiskEntry, RamDiskSectors,
    RamDisks, TASK_ADMIT_OP, TaskClass, TaskGrant, TaskOutcome, TaskQueueStatus, TaskRunCaps,
    WorkspaceCa, WorkspaceEndpoint, WorkspacePolicy, WorkspaceSession, WorkspaceToken,
};

use crate::{
    actor::{GatewayError, GatewayHandle},
    config::{CONTROL_TCP_ADDR, StartupProbe},
    cpu_budget::CpuBudget,
    disk_lease::{Claim, DiskLeases},
    interfaces::AuditEvent,
    repo_mirror::{MirrorInfo, RepoMirrorError, RepoMirrorHandle, RepoMirrorRequest},
    sim_broker::{
        SimBrokerError, SimBrokerHandle, SimDevice, SimInstallApproval, SimProjectConfig,
    },
    telemetry::{AuditTailHandle, AuditTailQuery},
};

const MAX_CONTROL_MESSAGE: u64 = 1024 * 1024;
/// The longest checkout path a `cpu-tokens` request may name; a longer one is refused.
const MAX_CHECKOUT: usize = 4096;
const MAX_AUDIT_TAIL_LIMIT: usize = 1024;
const CONTROLLER_CREDENTIAL_BYTES: usize = 32;
const CONTROLLER_CREDENTIAL_PREFIX: &str = "cctl1_";

trait ControlIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> ControlIo for T where T: AsyncRead + AsyncWrite + Send + Unpin {}
type BoxControlIo = Box<dyn ControlIo>;

#[derive(Clone, Debug)]
enum ControlEndpoint {
    Unix(PathBuf),
    Tcp {
        address: SocketAddr,
        credential_file: PathBuf,
    },
}

/// Host-side client used by an independently running coordinator.
#[derive(Clone, Debug)]
pub struct GatewayControlClient {
    endpoint: ControlEndpoint,
}

impl GatewayControlClient {
    pub fn new(socket: PathBuf) -> Result<Self, ControlError> {
        if !socket.is_absolute() {
            return Err(ControlError::InvalidSocketPath);
        }
        Ok(Self {
            endpoint: ControlEndpoint::Unix(socket),
        })
    }

    pub fn new_tcp(address: SocketAddr, credential_file: PathBuf) -> Result<Self, ControlError> {
        if address != CONTROL_TCP_ADDR || !credential_file.is_absolute() {
            return Err(ControlError::InvalidTcpEndpoint);
        }
        Ok(Self {
            endpoint: ControlEndpoint::Tcp {
                address,
                credential_file,
            },
        })
    }

    pub async fn status(&self) -> Result<DaemonStatus, ControlError> {
        self.send(&ControlRequest::Status)
            .await?
            .status
            .ok_or(ControlError::InvalidResponse)
    }

    /// [`Self::status`] from a daemon that must answer within `within` of taking the request: one
    /// that took it and stays silent is [`ControlError::Unanswered`], not a wait without end
    /// (specs/cowshed/06_cli.md, "doctor").
    pub async fn status_within(&self, within: Duration) -> Result<DaemonStatus, ControlError> {
        let response: ControlResponse = self
            .exchange_within(&ControlRequest::Status, Some(within))
            .await?;
        check_response(&response)?;
        response.status.ok_or(ControlError::InvalidResponse)
    }

    /// Observe one installed workspace revision without requesting the host session census.
    pub async fn session_current(
        &self,
        workspace_id: &str,
        revision: u64,
    ) -> Result<bool, ControlError> {
        self.send(&ControlRequest::SessionCurrent {
            workspace_id: workspace_id.to_owned(),
            revision,
        })
        .await?
        .current
        .ok_or(ControlError::InvalidResponse)
    }

    /// The host CPU budget's ledger (05_gateway.md "Host CPU budget", Wire): who holds and who
    /// waits for tokens, and the gate queue in the order returned tokens go to it.
    pub async fn cpu_budget(&self) -> Result<CpuBudgetStatus, ControlError> {
        self.send(&ControlRequest::CpuBudget)
            .await?
            .cpu_budget
            .ok_or(ControlError::InvalidResponse)
    }

    /// Clear the project's Nx accepted-task store (03_caches.md, "Nx accepted-task artifact
    /// service"). A refusal while the project's Nx work is in flight is [`ControlFailureCode::Busy`]
    /// and names that work.
    pub async fn clear_nx_cache(
        &self,
        repo_id: &cowshed_core::repository::RepoId,
    ) -> Result<cowshed_core::api::NxStoreClearResult, ControlError> {
        self.send(&ControlRequest::NxCacheClear {
            repo_id: repo_id.clone(),
        })
        .await?
        .nx_cache_cleared
        .ok_or(ControlError::InvalidResponse)
    }

    /// [`Self::cpu_budget`] answered within `within`, as [`Self::status_within`].
    pub async fn cpu_budget_within(
        &self,
        within: Duration,
    ) -> Result<CpuBudgetStatus, ControlError> {
        let response: ControlResponse = self
            .exchange_within(&ControlRequest::CpuBudget, Some(within))
            .await?;
        check_response(&response)?;
        response.cpu_budget.ok_or(ControlError::InvalidResponse)
    }

    /// A first-launch measurement answered within `within`, as [`Self::status_within`]. The daemon
    /// makes it by two timed launches, so it needs far more time than a status ask.
    pub async fn first_launch_within(&self, within: Duration) -> Result<FirstLaunch, ControlError> {
        let response: ControlResponse = self
            .exchange_within(&ControlRequest::FirstLaunch, Some(within))
            .await?;
        check_response(&response)?;
        response.first_launch.ok_or(ControlError::InvalidResponse)
    }

    /// The gate jobs a gateway restart now would cut off (05_gateway.md, "An install refuses
    /// while a land or a gate runs"): every gate job whose runners hold tokens and every gate job a
    /// waiting request serves, oldest first. It reads `gateJobs` and `gateQueue` of the
    /// `cpu-budget` answer and nothing else of the ledger, so an install reads the gates of a
    /// running gateway of another build, whose ledger has fields this one lacks or does not know.
    pub async fn gates_in_flight(
        &self,
    ) -> Result<BTreeSet<cowshed_core::api::GateJobRef>, ControlError> {
        let response: GatesResponse = self.exchange(&ControlRequest::CpuBudget).await?;
        if !response.ok {
            return Err(ControlError::Rejected {
                code: response.code.ok_or(ControlError::InvalidResponse)?,
                message: response.error.ok_or(ControlError::InvalidResponse)?,
            });
        }
        let gates = response.cpu_budget.ok_or(ControlError::InvalidResponse)?;
        Ok(gates
            .gate_jobs
            .into_iter()
            .chain(gates.gate_queue)
            .map(|gate| cowshed_core::api::GateJobRef {
                job: gate.job,
                checkout: gate.checkout,
            })
            .collect())
    }

    pub async fn install(&self, session: &WorkspaceSession) -> Result<(), ControlError> {
        session
            .validate()
            .map_err(|error| ControlError::InvalidSession(error.to_string()))?;
        let wire = SessionWire::from(session);
        self.send(&ControlRequest::Install { session: wire })
            .await?;
        Ok(())
    }

    pub async fn remove(
        &self,
        workspace_id: &str,
        expected_revision: u64,
    ) -> Result<(), ControlError> {
        self.send(&ControlRequest::Remove {
            workspace_id: workspace_id.to_owned(),
            expected_revision,
        })
        .await?;
        Ok(())
    }

    pub async fn repo_mirror(
        &self,
        request: &RepoMirrorRequest,
    ) -> Result<MirrorInfo, ControlError> {
        self.send(&ControlRequest::RepoMirror {
            request: request.clone(),
        })
        .await?
        .mirror_info
        .ok_or(ControlError::InvalidResponse)
    }

    pub async fn configure_simulator(&self, config: &SimProjectConfig) -> Result<(), ControlError> {
        self.send(&ControlRequest::SimConfigure {
            config: config.clone(),
        })
        .await?;
        Ok(())
    }

    pub async fn approve_simulator_install(
        &self,
        approval: &SimInstallApproval,
    ) -> Result<(), ControlError> {
        self.send(&ControlRequest::SimApprove {
            approval: approval.clone(),
        })
        .await?;
        Ok(())
    }

    pub async fn list_simulator_devices(
        &self,
        repo_id: &str,
    ) -> Result<Vec<SimDevice>, ControlError> {
        self.send(&ControlRequest::SimList {
            repo_id: repo_id.to_owned(),
        })
        .await?
        .sim_devices
        .ok_or(ControlError::InvalidResponse)
    }

    pub async fn boot_simulator_device(
        &self,
        repo_id: &str,
        device: &str,
    ) -> Result<(), ControlError> {
        self.send(&ControlRequest::SimBoot {
            repo_id: repo_id.to_owned(),
            device: device.to_owned(),
        })
        .await?;
        Ok(())
    }

    pub async fn audit_tail(
        &self,
        follow: bool,
    ) -> Result<mpsc::Receiver<Result<AuditEvent, ControlError>>, ControlError> {
        self.audit_tail_query(None, None, MAX_AUDIT_TAIL_LIMIT, follow)
            .await
    }

    pub async fn audit_tail_query(
        &self,
        workspace_id: Option<&str>,
        after_sequence: Option<u64>,
        limit: usize,
        follow: bool,
    ) -> Result<mpsc::Receiver<Result<AuditEvent, ControlError>>, ControlError> {
        if limit == 0 || limit > MAX_AUDIT_TAIL_LIMIT {
            return Err(ControlError::InvalidAuditTailLimit);
        }
        let request = ControlRequest::AuditTail {
            workspace_id: workspace_id.map(str::to_owned),
            after_sequence,
            limit,
            follow,
        };
        let (mut stream, encoded) = self.connect_and_encode(&request).await?;
        stream.write_all(&encoded).await?;
        stream.shutdown().await?;
        let mut reader = BufReader::new(stream);
        let first = read_response_line(&mut reader).await?;
        check_response(&first)?;
        let initial = first.audit_events.unwrap_or_default();
        let capacity = limit.max(1);
        let (sender, receiver) = mpsc::channel(capacity);
        tokio::spawn(async move {
            for event in initial {
                if sender.send(Ok(event)).await.is_err() {
                    return;
                }
            }
            if !follow {
                return;
            }
            loop {
                match read_response_line(&mut reader).await {
                    Ok(response) => {
                        if let Some(event) = response.audit_event {
                            if sender.send(Ok(event)).await.is_err() {
                                return;
                            }
                        } else if let Err(error) = check_response(&response) {
                            let _ = sender.send(Err(error)).await;
                            return;
                        } else {
                            let _ = sender.send(Err(ControlError::InvalidResponse)).await;
                            return;
                        }
                    }
                    Err(ControlError::UnexpectedEof) => return,
                    Err(error) => {
                        let _ = sender.send(Err(error)).await;
                        return;
                    }
                }
            }
        });
        Ok(receiver)
    }

    async fn send(&self, request: &ControlRequest) -> Result<ControlResponse, ControlError> {
        let response = self.exchange(request).await?;
        check_response(&response)?;
        Ok(response)
    }

    /// One request, and its one answer line read as `T`.
    async fn exchange<T: serde::de::DeserializeOwned>(
        &self,
        request: &ControlRequest,
    ) -> Result<T, ControlError> {
        self.exchange_within(request, None).await
    }

    /// [`Self::exchange`], whose answer must arrive within `within` of the request being sent,
    /// when a bound is given.
    async fn exchange_within<T: serde::de::DeserializeOwned>(
        &self,
        request: &ControlRequest,
        within: Option<Duration>,
    ) -> Result<T, ControlError> {
        let (mut stream, encoded) = self.connect_and_encode(request).await?;
        stream.write_all(&encoded).await?;
        stream.shutdown().await?;
        let mut reader = BufReader::new(stream);
        match within {
            None => read_answer_line(&mut reader).await,
            Some(within) => tokio::time::timeout(within, read_answer_line(&mut reader))
                .await
                .map_err(|_| ControlError::Unanswered { within })?,
        }
    }

    async fn connect_and_encode(
        &self,
        request: &ControlRequest,
    ) -> Result<(BoxControlIo, Zeroizing<Vec<u8>>), ControlError> {
        let (stream, mut encoded) = match &self.endpoint {
            ControlEndpoint::Unix(socket) => {
                let encoded = serde_json::to_vec(request)
                    .map_err(|error| ControlError::Encoding(error.to_string()))?;
                (
                    Box::new(UnixStream::connect(socket).await?) as BoxControlIo,
                    encoded,
                )
            }
            ControlEndpoint::Tcp {
                address,
                credential_file,
            } => {
                let credential = read_controller_credential(credential_file)?;
                let envelope = AuthenticatedControlRequestOut {
                    controller_credential: credential.as_str(),
                    request,
                };
                let encoded = serde_json::to_vec(&envelope)
                    .map_err(|error| ControlError::Encoding(error.to_string()))?;
                (
                    Box::new(TcpStream::connect(address).await?) as BoxControlIo,
                    encoded,
                )
            }
        };
        encoded.push(b'\n');
        if encoded.len() > MAX_CONTROL_MESSAGE as usize {
            return Err(ControlError::MessageTooLarge);
        }
        Ok((stream, Zeroizing::new(encoded)))
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuthenticatedControlRequestOut<'a> {
    controller_credential: &'a str,
    request: &'a ControlRequest,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
enum ControlRequest {
    SessionCurrent {
        #[serde(rename = "workspaceId")]
        workspace_id: String,
        revision: u64,
    },
    Status,
    Install {
        session: SessionWire,
    },
    Remove {
        #[serde(rename = "workspaceId")]
        workspace_id: String,
        #[serde(rename = "expectedRevision")]
        expected_revision: u64,
    },
    AuditTail {
        #[serde(rename = "workspaceId")]
        workspace_id: Option<String>,
        #[serde(rename = "afterSequence")]
        after_sequence: Option<u64>,
        limit: usize,
        follow: bool,
    },
    RepoMirror {
        request: RepoMirrorRequest,
    },
    SimConfigure {
        config: SimProjectConfig,
    },
    SimApprove {
        approval: SimInstallApproval,
    },
    SimList {
        #[serde(rename = "repoId")]
        repo_id: String,
    },
    SimBoot {
        #[serde(rename = "repoId")]
        repo_id: String,
        device: String,
    },
    /// The host CPU budget's ledger.
    CpuBudget,
    /// One unit's terminal outcome: the fact its dependents read.
    TaskTerminal {
        unit: String,
        outcome: TaskOutcome,
    },
    /// The task server's queue.
    TaskQueue,
    /// Clear one project's Nx accepted-task store.
    NxCacheClear {
        #[serde(rename = "repoId")]
        repo_id: cowshed_core::repository::RepoId,
    },
    /// Detach the RAM disk `registry_entry` registers as `device` (05_gateway.md, "RAM disks").
    RamDetach {
        device: RamDiskDevice,
        #[serde(rename = "registryEntry")]
        registry_entry: RamDiskEntry,
    },
    /// Measure a fresh executable's first and second launch (05_gateway.md, "Control plane").
    FirstLaunch,
}

/// A request as the control socket reads it: a one-shot [`ControlRequest`] answered and closed,
/// or a disk lease or CPU tokens, held for as long as its client keeps the connection open.
enum Incoming {
    Request(ControlRequest),
    /// A RAM disk held for the connection until it keeps or releases it (05_gateway.md, "RAM
    /// disks").
    RamLease {
        sectors: RamDiskSectors,
    },
    DiskLease {
        class: DiskClass,
        share: PhaseShare,
        command: String,
    },
    CpuTokens {
        want: NonZeroUsize,
        checkout: String,
        command: String,
    },
    Jobserver {
        want: NonZeroUsize,
        checkout: String,
        command: String,
        options: JobserverOptions,
    },
    /// A resident job's pool: no token, no queue place; each child it serves asks the host.
    ResidentPool {
        checkout: String,
        command: String,
        job: JobserverJobIn,
    },
    TaskAdmit {
        unit: String,
        owner: String,
        incarnation: Option<String>,
        deps: Vec<String>,
        leases: Vec<LeaseClass>,
        want: usize,
        priority: u32,
        urgent: bool,
        class: TaskClass,
        cost: Option<u64>,
        deadline: Option<u64>,
        caps: Option<TaskRunCaps>,
        command: String,
    },
}

/// A `disk-lease` request's fields. Its keys are checked against the operation's allowed set
/// before this is read, so the `op` tag is the one key it ignores. A request without `share`
/// shares its phase.
#[derive(Deserialize)]
struct DiskLeaseIn {
    class: DiskClass,
    #[serde(default)]
    share: PhaseShare,
    command: String,
}

/// A `cpu-tokens` request's fields, read the way [`DiskLeaseIn`] is.
#[derive(Deserialize)]
struct CpuTokensIn {
    want: NonZeroUsize,
    checkout: String,
    command: String,
}

/// A `task-admit` request's fields, read the way [`CpuTokensIn`] is. A unit with no `want` asks
/// for no CPU; `deadline` is an absolute cache-domain deadline in epoch milliseconds.
#[derive(Deserialize)]
struct TaskAdmitIn {
    unit: String,
    owner: String,
    #[serde(default)]
    incarnation: Option<String>,
    #[serde(default)]
    deps: Vec<String>,
    #[serde(default)]
    leases: Vec<LeaseClass>,
    #[serde(default)]
    want: usize,
    #[serde(default)]
    priority: u32,
    #[serde(default)]
    urgent: bool,
    #[serde(default)]
    class: TaskClass,
    #[serde(default)]
    cost: Option<u64>,
    #[serde(default)]
    deadline: Option<u64>,
    #[serde(default)]
    caps: Option<TaskRunCaps>,
    command: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobserverIn {
    #[serde(flatten)]
    tokens: CpuTokensIn,
    #[serde(default)]
    class: crate::jobserver::JobserverAdmission,
    #[serde(default)]
    pool: bool,
    job: Option<JobserverJobIn>,
    /// The requester's live CPU pool, which the job waits on beside the host ledger: the first
    /// grant serves it.
    parent_pool: Option<String>,
    /// The requester can start the job before its grant and stop it when its probation ends
    /// (05_gateway.md "A one-token job starts on probation").
    #[serde(default)]
    probation: bool,
}

struct JobserverOptions {
    admission: crate::cpu_budget::Class,
    pool_job: Option<JobserverJobIn>,
    parent_pool: Option<PathBuf>,
    /// The requester offered to run the job on probation.
    probation: bool,
}

impl JobserverOptions {
    /// Whether this ask may be lent the probation token (05_gateway.md "A one-token job starts on
    /// probation"): a background ask for one token, for a job whose pool it asks for, naming no
    /// requester's pool, from a requester that can stop the job it starts. A larger ask's command
    /// sizes its runners when it starts, and a grant never grows.
    fn lendable(&self, want: NonZeroUsize) -> bool {
        self.probation
            && want == NonZeroUsize::MIN
            && self.admission == crate::cpu_budget::Class::Background
            && self.pool_job.is_some()
            && self.parent_pool.is_none()
    }
}

/// A `resident-pool` request's fields: the checkout its children's asks are for, what the
/// resident runs, and the trusted job identity that scopes its pool. It names no `want`.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct ResidentPoolIn {
    checkout: String,
    command: String,
    job: JobserverJobIn,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct JobserverJobIn {
    repo_id: cowshed_core::repository::RepoId,
    workspace_incarnation: String,
    job_id: std::num::NonZeroU64,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AuthenticatedControlRequestIn {
    controller_credential: String,
    request: serde_json::Value,
}

/// What [`GatewayControlClient::gates_in_flight`] reads of a `cpu-budget` answer: its outcome and
/// the gate work, and nothing else. Unknown fields are not refused here, unlike everywhere else
/// on this wire: an install reads the ledger of the gateway it is about to replace.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GatesResponse {
    ok: bool,
    code: Option<ControlFailureCode>,
    error: Option<String>,
    cpu_budget: Option<GatesWire>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GatesWire {
    gate_jobs: Vec<GateWire>,
    gate_queue: Vec<GateWire>,
}

/// A running gate job or a waiting gate-class request, by the gate job it serves.
#[derive(Deserialize)]
struct GateWire {
    checkout: String,
    job: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ControlResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<DaemonStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    current: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    audit_events: Option<Vec<AuditEvent>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    audit_event: Option<AuditEvent>,
    #[serde(skip_serializing_if = "Option::is_none")]
    mirror_info: Option<MirrorInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    sim_devices: Option<Vec<SimDevice>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<ControlFailureCode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    lease: Option<LeaseState>,
    /// How many CPU tokens a `cpu-tokens` grant holds.
    #[serde(skip_serializing_if = "Option::is_none")]
    tokens: Option<usize>,
    /// The jobserver fifo a `jobserver` grant names for `CARGO_MAKEFLAGS`.
    #[serde(skip_serializing_if = "Option::is_none")]
    fifo: Option<String>,

    #[serde(skip_serializing_if = "Option::is_none")]
    cpu_pool: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    cpu_budget: Option<CpuBudgetStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    queue: Option<cowshed_gateway_types::cpu_budget::CpuQueueWait>,
    /// What a selected task-server unit runs under.
    #[serde(skip_serializing_if = "Option::is_none")]
    task_grant: Option<TaskGrant>,
    /// The task server's queue fold.
    #[serde(skip_serializing_if = "Option::is_none")]
    task_queue: Option<TaskQueueStatus>,
    /// What an `nx-cache-clear` deleted.
    #[serde(skip_serializing_if = "Option::is_none")]
    nx_cache_cleared: Option<cowshed_core::api::NxStoreClearResult>,
    /// The install that cut a queued CPU ask off, beside [`ControlFailureCode::InstallRestart`].
    #[serde(skip_serializing_if = "Option::is_none")]
    install: Option<cowshed_gateway_types::InstallRestart>,
    /// The whole disk a `ram-lease` attached and the registry entry it registers as.
    #[serde(skip_serializing_if = "Option::is_none")]
    ram_disk: Option<RamDiskAttached>,
    /// What is known of a `ram-detach`: `terminal`, `not-started` or `unknown`.
    #[serde(skip_serializing_if = "Option::is_none")]
    ram_detach: Option<RamDetached>,
    /// The measurement a `first-launch` made: the first and second launch of a fresh executable.
    #[serde(skip_serializing_if = "Option::is_none")]
    first_launch: Option<FirstLaunch>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ControlFailureCode {
    InvalidRequest,
    Unauthorized,
    InvalidSession,
    RevisionFence,
    /// The operation names a workspace with no installed session.
    NotInstalled,
    EndpointConflict,
    AddressInUse,
    NotAdmitted,
    BrokerRejected,
    /// The daemon's startup pass has not restored its sessions yet: a session change now would
    /// race that restore. Retry once status reports no `healing`.
    Healing,
    Rejected,
    /// Work the operation would cut off is running; the message names it. Retry once it ends.
    Busy,
    /// A queued CPU ask an install cut off as it restarts the gateway; `install` names it. Ask
    /// the gateway it starts, once ([`cowshed_gateway_types::INSTALL_RESTART_CODE`]).
    InstallRestart,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SessionWire {
    workspace_id: String,
    repo_id: String,
    revision: u64,
    endpoint: EndpointWire,
    token: String,
    ca_certificate_pem: String,
    ca_private_key_pem: String,
    policy: PolicyWire,
}

impl Drop for SessionWire {
    fn drop(&mut self) {
        self.token.zeroize();
        self.ca_private_key_pem.zeroize();
    }
}

impl From<&WorkspaceSession> for SessionWire {
    fn from(session: &WorkspaceSession) -> Self {
        Self {
            workspace_id: session.workspace_id.clone(),
            repo_id: session.repo_id.clone(),
            revision: session.revision,
            endpoint: EndpointWire::from(&session.endpoint),
            token: session.token.encode(),
            ca_certificate_pem: session.ca.certificate_pem.clone(),
            ca_private_key_pem: session.ca.private_key_pem.to_string(),
            policy: PolicyWire::from(&session.policy),
        }
    }
}

impl SessionWire {
    fn into_session(mut self) -> Result<WorkspaceSession, ControlError> {
        let token = WorkspaceToken::parse(&self.token)
            .map_err(|error| ControlError::InvalidSession(error.to_string()))?;
        let ca = WorkspaceCa::new(
            std::mem::take(&mut self.ca_certificate_pem),
            std::mem::take(&mut self.ca_private_key_pem),
        )
        .map_err(|error| ControlError::InvalidSession(error.to_string()))?;
        let policy = self.policy.to_policy()?;
        let session = WorkspaceSession {
            workspace_id: self.workspace_id.clone(),
            repo_id: self.repo_id.clone(),
            revision: self.revision,
            endpoint: self.endpoint.to_endpoint()?,
            token,
            ca,
            policy,
        };
        session
            .validate()
            .map_err(|error| ControlError::InvalidSession(error.to_string()))?;
        Ok(session)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "kebab-case", deny_unknown_fields)]
enum EndpointWire {
    Tcp {
        address: String,
        #[serde(rename = "blockSize")]
        block_size: u16,
    },
    Unix {
        path: PathBuf,
    },
}

impl From<&WorkspaceEndpoint> for EndpointWire {
    fn from(endpoint: &WorkspaceEndpoint) -> Self {
        match endpoint {
            WorkspaceEndpoint::Tcp {
                address,
                block_size,
            } => Self::Tcp {
                address: address.to_string(),
                block_size: *block_size,
            },
            WorkspaceEndpoint::Unix(path) => Self::Unix { path: path.clone() },
        }
    }
}

impl EndpointWire {
    fn to_endpoint(&self) -> Result<WorkspaceEndpoint, ControlError> {
        match self {
            Self::Tcp {
                address,
                block_size,
            } => address
                .parse::<SocketAddr>()
                .map(|address| WorkspaceEndpoint::Tcp {
                    address,
                    block_size: *block_size,
                })
                .map_err(|_| ControlError::InvalidSession("invalid TCP endpoint".to_owned())),
            Self::Unix { path } => Ok(WorkspaceEndpoint::Unix(path.clone())),
        }
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct PolicyWire {
    grants: Vec<GrantWire>,
    mirrors: Vec<MirrorWire>,
}

impl From<&WorkspacePolicy> for PolicyWire {
    fn from(policy: &WorkspacePolicy) -> Self {
        Self {
            grants: policy.grants.iter().map(GrantWire::from).collect(),
            mirrors: policy.mirrors.iter().map(MirrorWire::from).collect(),
        }
    }
}

impl PolicyWire {
    fn to_policy(&self) -> Result<WorkspacePolicy, ControlError> {
        let policy = WorkspacePolicy {
            grants: self
                .grants
                .iter()
                .map(GrantWire::to_grant)
                .collect::<Result<_, _>>()?,
            mirrors: self
                .mirrors
                .iter()
                .map(MirrorWire::to_route)
                .collect::<Result<_, _>>()?,
        };
        policy
            .validate()
            .map_err(|error| ControlError::InvalidSession(error.to_string()))?;
        Ok(policy)
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct GrantWire {
    host: String,
    port: u16,
    mode: EgressMode,
    methods: Vec<String>,
    path_prefixes: Vec<String>,
}

impl From<&EgressGrant> for GrantWire {
    fn from(grant: &EgressGrant) -> Self {
        let host = match &grant.host {
            HostPattern::Exact(host) => host.clone(),
            HostPattern::Wildcard(host) => format!("*.{host}"),
            HostPattern::Ip(ip) => ip.to_string(),
        };
        Self {
            host,
            port: grant.port,
            mode: grant.mode,
            methods: grant.methods.iter().cloned().collect(),
            path_prefixes: grant.path_prefixes.clone(),
        }
    }
}

impl GrantWire {
    fn to_grant(&self) -> Result<EgressGrant, ControlError> {
        Ok(EgressGrant {
            host: HostPattern::parse(&self.host)
                .map_err(|error| ControlError::InvalidSession(error.to_string()))?,
            port: self.port,
            mode: self.mode,
            methods: self.methods.iter().cloned().collect::<BTreeSet<_>>(),
            path_prefixes: self.path_prefixes.clone(),
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MirrorWire {
    upstream_origin: String,
    admitted_prefixes: Vec<String>,
    credentialed: bool,
}

impl From<&MirrorRoute> for MirrorWire {
    fn from(route: &MirrorRoute) -> Self {
        Self {
            upstream_origin: route.target().origin(),
            admitted_prefixes: route.admitted_prefixes.clone(),
            credentialed: route.credentialed,
        }
    }
}

impl MirrorWire {
    fn to_route(&self) -> Result<MirrorRoute, ControlError> {
        MirrorRoute::new(
            &self.upstream_origin,
            self.admitted_prefixes.clone(),
            self.credentialed,
        )
        .map_err(|error| ControlError::InvalidSession(error.to_string()))
    }
}

#[derive(Clone)]
pub(crate) struct ControlServices {
    pub handle: GatewayHandle,
    pub repo_mirror: RepoMirrorHandle,
    pub sim_broker: SimBrokerHandle,
    pub audit_tail: Option<AuditTailHandle>,
    pub startup: Option<Arc<dyn StartupProbe>>,
    pub disk_leases: DiskLeases,
    pub cpu_budget: CpuBudget,
    pub task_server: crate::task_server::TaskServer,
    pub jobserver: crate::jobserver::JobserverFifos,
    pub ram_disks: Option<Arc<dyn RamDisks>>,
    pub ram_leases: crate::ram_lease::Records,
    pub first_launch: Option<Arc<dyn FirstLaunchProbe>>,
}

impl ControlServices {
    /// The refusal of a session install or removal while the startup pass still heals mounts and
    /// restores sessions from them; `None` once it has finished.
    fn healing(&self) -> Option<ControlResponse> {
        self.startup.as_ref()?.healing()?;
        Some(failure(
            ControlFailureCode::Healing,
            "the gateway has not finished mounting its projects and restoring their sessions \
             since it started; session changes are refused until it has"
                .to_owned(),
        ))
    }
}

impl fmt::Debug for ControlServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlServices")
            .field("handle", &self.handle)
            .field("repo_mirror", &self.repo_mirror)
            .field("sim_broker", &self.sim_broker)
            .field("audit_tail", &self.audit_tail)
            .field("startup", &self.startup)
            .field("disk_leases", &self.disk_leases)
            .field("cpu_budget", &self.cpu_budget)
            .field("task_server", &self.task_server)
            .field("ram_disks", &self.ram_disks)
            .field("ram_leases", &self.ram_leases)
            .field("first_launch", &self.first_launch)
            .finish()
    }
}

pub(crate) struct ControllerCredential([u8; CONTROLLER_CREDENTIAL_BYTES]);

impl ControllerCredential {
    pub(crate) fn from_file(path: &Path) -> Result<Self, ControlError> {
        let encoded = read_controller_credential(path)?;
        let raw = encoded
            .strip_prefix(CONTROLLER_CREDENTIAL_PREFIX)
            .ok_or(ControlError::InvalidControllerCredential)?;
        let decoded = URL_SAFE_NO_PAD
            .decode(raw)
            .map_err(|_| ControlError::InvalidControllerCredential)?;
        let bytes = decoded
            .try_into()
            .map_err(|_| ControlError::InvalidControllerCredential)?;
        Ok(Self(bytes))
    }

    fn matches(&self, candidate: &str) -> bool {
        let mut decoded = [0_u8; CONTROLLER_CREDENTIAL_BYTES];
        let raw = candidate
            .strip_prefix(CONTROLLER_CREDENTIAL_PREFIX)
            .unwrap_or("");
        let valid = URL_SAFE_NO_PAD
            .decode(raw)
            .ok()
            .filter(|bytes| bytes.len() == CONTROLLER_CREDENTIAL_BYTES)
            .is_some_and(|bytes| {
                decoded.copy_from_slice(&bytes);
                true
            });
        bool::from(self.0.ct_eq(&decoded)) && valid
    }
}

impl Drop for ControllerCredential {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl fmt::Debug for ControllerCredential {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("ControllerCredential([REDACTED])")
    }
}

pub(crate) async fn serve_control_unix(stream: UnixStream, services: ControlServices) {
    // The pid names a disk-lease holder the gateway has to report; nothing is decided by it.
    let pid = stream
        .peer_cred()
        .ok()
        .and_then(|credentials| credentials.pid());
    let (reader, writer) = stream.into_split();
    serve(reader, writer, pid, parse_control_request, services).await;
}

pub(crate) async fn serve_control_tcp(
    stream: TcpStream,
    credential: std::sync::Arc<ControllerCredential>,
    services: ControlServices,
) {
    let (reader, writer) = stream.into_split();
    let parse = |bytes: &[u8]| {
        let mut envelope: AuthenticatedControlRequestIn = serde_json::from_slice(bytes)
            .map_err(|error| ControlError::Encoding(error.to_string()))?;
        if !credential.matches(&envelope.controller_credential) {
            return Err(ControlError::Unauthorized);
        }
        envelope.controller_credential.zeroize();
        parse_control_request_value(envelope.request)
    };
    serve(reader, writer, None, parse, services).await;
}

/// Read the connection's one request line and answer it. A one-shot request must be followed by
/// the client's EOF, as it always was; a disk lease is held until that EOF comes.
async fn serve<R, W>(
    mut reader: R,
    writer: W,
    pid: Option<i32>,
    parse: impl FnOnce(&[u8]) -> Result<Incoming, ControlError>,
    services: ControlServices,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    match read_request_line(&mut reader)
        .await
        .and_then(|line| parse(&line))
    {
        Ok(Incoming::RamLease { sectors }) => {
            hold_ram_lease(
                reader,
                writer,
                sectors,
                services.ram_disks.clone(),
                services.ram_leases.clone(),
            )
            .await;
        }
        Ok(Incoming::DiskLease {
            class,
            share,
            command,
        }) => {
            let (tenure, granted) =
                services
                    .disk_leases
                    .ask(class, share, Claim::new(&command, pid));
            hold(
                reader,
                writer,
                tenure,
                granted,
                |()| lease(LeaseState::Granted),
                "disk-lease scheduler",
                scheduler_stopped,
            )
            .await;
        }
        Ok(Incoming::CpuTokens {
            want,
            checkout,
            command,
        }) => {
            let (stake, granted) = services.cpu_budget.ask(
                Arc::from(checkout),
                want,
                Claim::new(&command, pid),
                crate::cpu_budget::Rank::Background,
            );
            let budget = services.cpu_budget.clone();
            hold(
                reader,
                writer,
                stake,
                granted,
                |tokens| ControlResponse {
                    tokens: Some(tokens),
                    ..lease(LeaseState::Granted)
                },
                "CPU budget",
                |_| cpu_ask_cut_off(&budget),
            )
            .await;
        }
        Ok(Incoming::Jobserver {
            want,
            checkout,
            command,
            options,
        }) => {
            // An exec from a holder waits on its requester's live pool and on the host ledger at
            // once: the first grant serves it and the other ask is withdrawn (05_gateway.md "Work
            // a holder starts draws on its grant"). Only a pool that cannot be reached is absent.
            let parent = match options.parent_pool.as_deref() {
                None => None,
                Some(path) => match connect_parent(path).await {
                    Ok(stream) => Some((path.to_path_buf(), stream)),
                    Err(error) => {
                        cowshed_os::eprintln!(
                            "cowshed: the requester's CPU pool {} does not answer ({error}); `{command}` asks the host budget alone",
                            path.display()
                        );
                        None
                    }
                },
            };
            let checkout: Arc<str> = Arc::from(checkout);
            let claim = Claim::new(&command, pid);
            let rank = options.admission.rank();
            let lendable = options.lendable(want);
            let admission = services.cpu_budget.ask_jobserver(
                Arc::clone(&checkout),
                want,
                claim,
                rank,
                lendable,
            );
            hold_jobserver(
                reader,
                writer,
                want,
                admission,
                parent,
                services.jobserver.clone(),
                options,
                services.cpu_budget.clone(),
                checkout,
                &command,
            )
            .await;
        }
        Ok(Incoming::ResidentPool {
            checkout,
            command,
            job,
        }) => {
            hold_resident_pool(
                reader,
                writer,
                &services.jobserver,
                services.cpu_budget.clone(),
                Arc::from(checkout),
                &command,
                &job,
            )
            .await;
        }
        Ok(Incoming::TaskAdmit {
            unit,
            owner,
            incarnation,
            deps,
            leases,
            want,
            priority,
            urgent,
            class,
            cost,
            deadline,
            caps,
            command,
        }) => {
            let facts = crate::task_server::TaskFacts {
                unit: Arc::from(unit),
                owner: Arc::from(owner),
                incarnation: incarnation.map(Arc::from),
                deps: deps.into_iter().map(Arc::from).collect(),
                leases,
                want,
                priority,
                urgent,
                class,
                cost,
                deadline,
                caps,
                command: Arc::from(command),
                pid,
            };
            let (tenure, granted) = services.task_server.ask(facts);
            hold(
                reader,
                writer,
                tenure,
                granted,
                |answer| match answer {
                    Ok(grant) => ControlResponse {
                        task_grant: Some(grant),
                        ..lease(LeaseState::Granted)
                    },
                    Err(refusal) => failure(ControlFailureCode::Rejected, refusal.to_string()),
                },
                "task scheduler",
                scheduler_stopped,
            )
            .await;
        }
        Ok(Incoming::Request(request)) => {
            let request = expect_eof(&mut reader).await.map(|()| request);
            serve_request(writer, request, services).await;
        }
        Err(error) => serve_request(writer, Err(error), services).await,
    }
}

/// Hold one lease for this connection — a disk lease or CPU tokens — whose stake is `stake`:
/// answer `queued` at once, the `granted` answer once the scheduler admits it, then hold until the
/// client closes. A client that closes while queued leaves the queue; either way, dropping the
/// stake gives back whatever it held. A scheduler that drops the grant is answered `stopped`.
async fn hold<R, W, S, G>(
    mut reader: R,
    mut writer: W,
    stake: S,
    granted: tokio::sync::oneshot::Receiver<G>,
    answer: impl FnOnce(G) -> ControlResponse,
    scheduler: &str,
    stopped: impl FnOnce(&str) -> ControlResponse,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    let asked = std::time::Instant::now();
    if write_response(&mut writer, &lease(LeaseState::Queued))
        .await
        .is_err()
    {
        return;
    }
    let mut discard = [0_u8; 64];
    let grant = tokio::select! {
        answer = granted => match answer {
            Ok(grant) => grant,
            Err(_) => {
                let _ = write_response(&mut writer, &stopped(scheduler)).await;
                return;
            }
        },
        // EOF, a broken socket, or bytes a lease client never sends: it is gone or confused, and
        // either way it leaves the queue.
        _ = reader.read(&mut discard) => return,
    };
    let response = answer(grant);
    if write_response(&mut writer, &response).await.is_err() {
        return;
    }
    if matches!(response.lease, Some(LeaseState::Granted)) {
        cowshed_os::eprintln!(
            "cowshed: timing lease-grant scheduler={scheduler:?} elapsed_ns={}",
            asked.elapsed().as_nanos(),
        );
    }
    while let Ok(read) = reader.read(&mut discard).await
        && read > 0
    {}
    drop(stake);
}

/// Hold one jobserver grant: answer `queued` at once; on grant, create the
/// fifo, name it in the granted answer, and hold until the client closes. The
/// fifo is then drained and unlinked, and dropping the stake returns all of
/// its tokens, including any a killed client read and never wrote back.
///
/// An exec from a holder (`parent`, its requester's live pool) waits on that pool and on the
/// host ledger at once, and the first grant serves it (05_gateway.md "Work a holder starts draws
/// on its grant"). The loser is withdrawn, which is said on stderr and as a timing event: the
/// host stake dropped leaves the ledger's queue, and the pool connection closed leaves the pool's.
/// A pool that answers and refuses before the host grants refuses the job. One ask is never
/// granted twice, and each grant is accounted by the ledger that made it.
#[allow(clippy::too_many_arguments)]
async fn hold_jobserver<R, W>(
    mut reader: R,
    mut writer: W,
    want: NonZeroUsize,
    crate::cpu_budget::JobserverAsk {
        stake,
        granted,
        queued,
        lent,
    }: crate::cpu_budget::JobserverAsk,
    parent: Option<(PathBuf, cowshed_os::fork_lock::Fenced<UnixStream>)>,
    fifos: crate::jobserver::JobserverFifos,
    options: JobserverOptions,
    budget: CpuBudget,
    checkout: Arc<str>,
    command: &str,
) where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let queue = match queued.await {
        Ok(mut queue) => {
            if let Some(wait) = &mut queue {
                wait.request.repository = wait
                    .request
                    .repository
                    .take()
                    .or_else(|| options.pool_job.as_ref().map(|job| job.repo_id.to_string()));
            }
            queue
        }
        Err(_) => {
            let _ = write_response(&mut writer, &cpu_ask_cut_off(&budget)).await;
            return;
        }
    };
    if write_response(
        &mut writer,
        &ControlResponse {
            queue,
            ..lease(LeaseState::Queued)
        },
    )
    .await
    .is_err()
    {
        return;
    }
    let asked = std::time::Instant::now();
    let job = match &options.pool_job {
        Some(job) => format!(
            "{}/{} job {}",
            job.repo_id, job.workspace_incarnation, job.job_id
        ),
        None => command.to_owned(),
    };
    let refuse = |path: &Path, why: String| {
        failure(
            ControlFailureCode::Rejected,
            format!(
                "the requester's CPU pool {} {why} before the host granted `{job}`; the job is refused",
                path.display()
            ),
        )
    };
    let mut parent = match parent {
        None => None,
        Some((path, stream)) => {
            let mut stream = BufReader::new(stream);
            let request = format!(
                "{}\n",
                serde_json::json!({ "op": "cpu-pool", "want": want })
            );
            if let Err(error) = stream.get_mut().write_all(request.as_bytes()).await {
                let why = format!("cannot be asked ({error})");
                let _ = write_response(&mut writer, &refuse(&path, why)).await;
                return;
            }
            Some((path, stream))
        }
    };
    /// Which ledger's grant arrived first, and its count; or the probation token, lent first.
    enum Winner {
        Host(usize),
        Pool(usize),
        Probation,
    }
    let mut granted = granted;
    let mut discard = [0_u8; 64];
    let winner = tokio::select! {
        biased;
        answer = &mut granted => match answer {
            Ok(tokens) => Winner::Host(tokens),
            Err(_) => {
                let _ = write_response(&mut writer, &cpu_ask_cut_off(&budget)).await;
                return;
            }
        },
        answer = async {
            match parent.as_mut() {
                Some((path, stream)) => parent_grant(stream, want)
                    .await
                    .map_err(|why| refuse(path, why)),
                None => std::future::pending().await,
            }
        } => match answer {
            Ok(tokens) => Winner::Pool(tokens),
            Err(refusal) => {
                let _ = write_response(&mut writer, &refusal).await;
                return;
            }
        },
        // An ask that is granted, or never may be lent, is never told: its sender is dropped.
        () = async {
            let told = match lent {
                Some(lent) => lent.await.is_ok(),
                None => false,
            };
            if !told {
                std::future::pending::<()>().await;
            }
        } => Winner::Probation,
        _ = reader.read(&mut discard) => return,
    };
    let waited = asked.elapsed().as_millis();
    let tokens = match (winner, parent) {
        (Winner::Host(tokens), None) => tokens,
        (Winner::Host(tokens), Some((path, stream))) => {
            // Closing the pool connection leaves the pool's queue.
            drop(stream);
            withdrawn(
                &job,
                tokens,
                "the host ledger",
                &format!("the requester's CPU pool {}", path.display()),
                waited,
            );
            tokens
        }
        (Winner::Pool(tokens), Some((path, stream))) => {
            // Dropping the stake leaves the host ledger's queue; a host grant that raced in
            // returns with it.
            drop(stake);
            withdrawn(
                &job,
                tokens,
                &format!("the requester's CPU pool {}", path.display()),
                "the host ledger",
                waited,
            );
            hold_parent_grant(
                reader,
                writer,
                stream,
                &path,
                (tokens, asked),
                fifos,
                options.pool_job,
            )
            .await;
            return;
        }
        (Winner::Pool(_), None) => unreachable!("only a requester's pool grants from a pool"),
        (Winner::Probation, None) => {
            let job = options
                .pool_job
                .expect("only an ask for its job's pool is lent the probation token");
            hold_probation(reader, writer, stake, granted, &fifos, &job, &budget, asked).await;
            return;
        }
        (Winner::Probation, Some(_)) => {
            unreachable!("an ask naming its requester's pool is never lent the probation token")
        }
    };
    let source = options.pool_job.as_ref().map(|job| {
        // A gate's children rank as the gate job does, by its age, at every return; background
        // children share the grant they were lent.
        let source = match options.admission {
            crate::cpu_budget::Class::Gate(repo) => crate::jobserver::PoolSource::Relay {
                host: budget.clone(),
                checkout,
                repo,
                job: stake.job(),
                tokens: std::num::NonZeroUsize::new(tokens).expect("a CPU grant is nonzero"),
            },
            crate::cpu_budget::Class::Background => crate::jobserver::PoolSource::Lent(
                std::num::NonZeroUsize::new(tokens).expect("a CPU grant is nonzero"),
            ),
        };
        (source, job)
    });
    let Some(served) = answer_grant(
        &mut writer,
        &fifos,
        tokens,
        source,
        asked,
        LeaseState::Granted,
    )
    .await
    else {
        return;
    };
    // Hold until the client closes. The fifo keeps the grant's `g − 1` bytes for the grant's
    // whole life: nothing writes more.
    while let Ok(read) = reader.read(&mut discard).await
        && read > 0
    {}
    served.close().await;
    drop(stake);
}

/// Hold a one-token job on probation (05_gateway.md "A one-token job starts on probation"): serve
/// its pool, whose holder's own token is the probation token, and answer `probation`; answer
/// `granted`, naming the same pool and fifo, when the ledger grants the job its one token, and
/// hold that grant until the client closes. The client's `probation-over` returns the probation
/// token and keeps the ask's place; its close, or any other line, leaves the queue.
#[allow(clippy::too_many_arguments)]
async fn hold_probation<R, W>(
    reader: R,
    mut writer: W,
    stake: crate::cpu_budget::Stake,
    mut granted: tokio::sync::oneshot::Receiver<usize>,
    fifos: &crate::jobserver::JobserverFifos,
    job: &JobserverJobIn,
    budget: &CpuBudget,
    asked: std::time::Instant,
) where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let Some(mut served) = answer_grant(
        &mut writer,
        fifos,
        1,
        Some((crate::jobserver::PoolSource::Probation, job)),
        asked,
        LeaseState::Probation,
    )
    .await
    else {
        return;
    };
    let mut lines = BufReader::new(reader);
    let mut line = String::new();
    loop {
        line.clear();
        tokio::select! {
            biased;
            answer = &mut granted => match answer {
                Ok(tokens) => {
                    if let Served::Pool(pool) = &mut served {
                        pool.grant_probation();
                    }
                    if write_response(&mut writer, &served.answer(tokens, LeaseState::Granted))
                        .await
                        .is_err()
                    {
                        served.close().await;
                        return;
                    }
                    cowshed_os::eprintln!(
                        "cowshed: timing lease-grant scheduler=\"jobserver\" elapsed_ns={} tokens={tokens} after=probation",
                        asked.elapsed().as_nanos(),
                    );
                    break;
                }
                Err(_) => {
                    let _ = write_response(&mut writer, &cpu_ask_cut_off(budget)).await;
                    served.close().await;
                    return;
                }
            },
            read = async { (&mut lines).take(4097).read_line(&mut line).await } => match read {
                Ok(read) if read > 0 && line.ends_with('\n') && probation_over(&line) => {
                    stake.probation_over();
                }
                // EOF, a broken socket, or a line a probation client never sends: it is gone
                // or confused, and either way it leaves the queue.
                _ => {
                    served.close().await;
                    return;
                }
            },
        }
    }
    // Hold the grant until the client closes. A `probation-over` that crossed the grant is
    // discarded with whatever else it sends.
    let mut discard = [0_u8; 64];
    while let Ok(read) = lines.read(&mut discard).await
        && read > 0
    {}
    served.close().await;
    drop(stake);
}

/// Whether `line` is exactly a probation's end: `{"op":"probation-over"}`.
fn probation_over(line: &str) -> bool {
    serde_json::from_str::<serde_json::Value>(line).is_ok_and(|value| {
        value.as_object().is_some_and(|object| {
            object.len() == 1
                && object.get("op").and_then(serde_json::Value::as_str)
                    == Some(cowshed_gateway_types::PROBATION_OVER_OP)
        })
    })
}

/// What a jobserver grant is served as: a job's private pool, whose make jobserver fifo holds the
/// grant's tokens and lends them to its children, or a bare fifo for a client that serves none.
enum Served {
    Pool(crate::jobserver::CpuPool),
    Fifo(crate::jobserver::JobserverFifo),
}

impl Served {
    /// The fifo the grant names for `CARGO_MAKEFLAGS`.
    fn fifo(&self) -> &Path {
        match self {
            Self::Pool(pool) => pool.fifo().expect("a token-holding pool has a fifo"),
            Self::Fifo(fifo) => fifo.path(),
        }
    }

    /// The answer naming `tokens` of this grant, its fifo and, for a job, its pool.
    fn answer(&self, tokens: usize, state: LeaseState) -> ControlResponse {
        ControlResponse {
            cpu_pool: match self {
                Self::Pool(pool) => Some(pool.path().to_string_lossy().into_owned()),
                Self::Fifo(_) => None,
            },
            tokens: Some(tokens),
            fifo: Some(self.fifo().to_string_lossy().into_owned()),
            ..lease(state)
        }
    }

    /// Close the pool and drain and unlink the fifo; tokens a client still holds are its own.
    async fn close(self) {
        match self {
            Self::Pool(pool) => {
                if let Err(error) = pool.close().await {
                    cowshed_os::eprintln!("cowshed: jobserver CPU pool did not close: {error}");
                }
            }
            Self::Fifo(fifo) => fifo.drain_and_unlink(),
        }
    }
}

/// Serve a granted jobserver — for a job, its private pool and that pool's fifo; otherwise a
/// fifo of its own — and answer it as `state`: `granted`, or `probation` for a job lent the
/// probation token. `None` once it cannot be made, which is answered, or the client is gone.
async fn answer_grant<W>(
    writer: &mut W,
    fifos: &crate::jobserver::JobserverFifos,
    tokens: usize,
    pool: Option<(crate::jobserver::PoolSource, &JobserverJobIn)>,
    asked: std::time::Instant,
    state: LeaseState,
) -> Option<Served>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let served = match pool {
        None => fifos
            .create(tokens)
            .map(Served::Fifo)
            .map_err(|error| format!("cannot create the jobserver fifo: {error}")),
        Some((source, job)) => fifos
            .create_pool(source, &job.workspace_incarnation)
            .map(|pool| {
                cowshed_os::timing::event("jobserver", || {
                    format!(
                        "pool {tokens} for {}/{} job {} at {}",
                        job.repo_id,
                        job.workspace_incarnation,
                        job.job_id,
                        pool.path().display()
                    )
                });
                Served::Pool(pool)
            })
            .map_err(|error| format!("cannot serve the job's CPU pool: {error}")),
    };
    let served = match served {
        Ok(served) => served,
        Err(error) => {
            let _ = write_response(writer, &failure(ControlFailureCode::Rejected, error)).await;
            return None;
        }
    };
    if write_response(writer, &served.answer(tokens, state))
        .await
        .is_err()
    {
        served.close().await;
        return None;
    }
    cowshed_os::eprintln!(
        "cowshed: timing lease-grant scheduler=\"jobserver\" elapsed_ns={} tokens={tokens} lease={state:?}",
        asked.elapsed().as_nanos(),
    );
    Some(served)
}

/// Connect to a requester's live CPU pool. Only a pool that cannot be reached is absent.
async fn connect_parent(path: &Path) -> std::io::Result<cowshed_os::fork_lock::Fenced<UnixStream>> {
    let mut connect = std::pin::pin!(UnixStream::connect(path));
    // Darwin creates a socket then sets CLOEXEC: fence each poll so no concurrently spawned
    // child keeps this descriptor past exec.
    let stream = std::future::poll_fn(|cx| {
        cowshed_os::fork_lock::create(|| Ok(connect.as_mut().poll(cx)))
            .expect("polling the connection runs an infallible closure")
    })
    .await?;
    Ok(cowshed_os::fork_lock::Fenced::new(stream))
}

/// What a requester's pool granted: the first `granted` line after its `queued`, a whole count
/// between one and `want`, or why it gave none. Its refusal is the job's refusal.
async fn parent_grant<R>(parent: &mut R, want: NonZeroUsize) -> Result<usize, String>
where
    R: tokio::io::AsyncBufRead + Unpin,
{
    use tokio::io::AsyncBufReadExt as _;
    loop {
        let mut line = String::new();
        match parent.read_line(&mut line).await {
            Ok(0) => return Err("closed without granting".to_owned()),
            Ok(_) => {}
            Err(error) => return Err(format!("stopped answering ({error})")),
        }
        let answer: serde_json::Value = serde_json::from_str(&line)
            .map_err(|error| format!("answered with no JSON line ({error})"))?;
        if answer["ok"] != true {
            let code = answer["code"].as_str().unwrap_or("refused");
            let error = answer["error"].as_str().unwrap_or("no diagnostic");
            return Err(format!("refused ({code}: {error})"));
        }
        match answer["lease"].as_str() {
            Some("queued") => continue,
            Some("granted") => {
                return answer["tokens"]
                    .as_u64()
                    .and_then(|tokens| usize::try_from(tokens).ok())
                    .filter(|tokens| (1..=want.get()).contains(tokens))
                    .ok_or_else(|| format!("granted {} tokens for {want}", answer["tokens"]));
            }
            _ => return Err(format!("answered {}", line.trim_end())),
        }
    }
}

/// Say that `job`'s first grant came from `winner` and its ask of `loser` was withdrawn, on
/// stderr and as a jobserver timing event (05_gateway.md "Work a holder starts draws on its
/// grant").
fn withdrawn(job: &str, tokens: usize, winner: &str, loser: &str, waited_ms: u128) {
    cowshed_os::eprintln!(
        "cowshed: `{job}` was granted {tokens} token(s) by {winner}; its ask of {loser} was withdrawn after {waited_ms} ms"
    );
    cowshed_os::timing::event("jobserver", || {
        format!(
            "grant {job} tokens={tokens} by {winner}; {loser} ask withdrawn after {waited_ms}ms"
        )
    });
}

/// Serve a job the grant its requester's live pool made, as a host grant is served: its fifo
/// and, for a job, its own lent pool. The host ledger holds no place for it, since its tokens are
/// the requester's. The pool's end ends the job's accounting, never its work: its own pool lends
/// nothing more and the job runs on to its own exit, as across a gateway restart. The client's
/// close returns the tokens to the pool.
async fn hold_parent_grant<R, W>(
    mut reader: R,
    mut writer: W,
    mut parent: BufReader<cowshed_os::fork_lock::Fenced<UnixStream>>,
    parent_path: &Path,
    (tokens, asked): (usize, std::time::Instant),
    fifos: crate::jobserver::JobserverFifos,
    pool_job: Option<JobserverJobIn>,
) where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    let mut discard = [0_u8; 64];
    let source = pool_job.as_ref().map(|job| {
        (
            crate::jobserver::PoolSource::Lent(
                NonZeroUsize::new(tokens).expect("a CPU grant is nonzero"),
            ),
            job,
        )
    });
    let Some(served) = answer_grant(
        &mut writer,
        &fifos,
        tokens,
        source,
        asked,
        LeaseState::Granted,
    )
    .await
    else {
        return;
    };
    let mut served = Some(served);
    let mut parent_open = true;
    let mut parent_byte = [0_u8; 1];
    loop {
        tokio::select! {
            read = reader.read(&mut discard) => if !matches!(read, Ok(read) if read > 0) { break },
            // A pool says nothing after its grant: any byte or end is the end of that grant.
            _ = parent.read(&mut parent_byte), if parent_open => {
                parent_open = false;
                cowshed_os::eprintln!(
                    "cowshed: the requester's CPU pool {} ended its {tokens}-token grant; the job runs on over budget until it ends, and its own pool lends nothing more",
                    parent_path.display()
                );
                // Its own pool's fifo goes with it: a Cargo that holds it open keeps what it reads.
                if matches!(served, Some(Served::Pool(_)))
                    && let Some(pool) = served.take()
                {
                    pool.close().await;
                }
            }
        }
    }
    if let Some(served) = served {
        served.close().await;
    }
    drop(parent);
}

/// Hold one resident job's pool: answer `queued`, then at once `granted` with the pool and no
/// tokens, since a resident takes no place in the ledger; serve it until the client closes. Each
/// child that asks the pool asks the host budget itself, at background class, for `checkout`
/// (05_gateway.md "A resident holds no token"), so the resident costs tokens only while a
/// request of its runs, and closing the pool ends every child's local grant with it.
async fn hold_resident_pool<R, W>(
    mut reader: R,
    mut writer: W,
    fifos: &crate::jobserver::JobserverFifos,
    budget: CpuBudget,
    checkout: Arc<str>,
    command: &str,
    job: &JobserverJobIn,
) where
    R: tokio::io::AsyncRead + Unpin,
    W: tokio::io::AsyncWrite + Unpin,
{
    // The same two answers every held CPU request gets, so a supervisor reads one protocol.
    if write_response(&mut writer, &lease(LeaseState::Queued))
        .await
        .is_err()
    {
        return;
    }
    let source = crate::jobserver::PoolSource::Resident {
        host: budget,
        checkout,
    };
    let pool = match fifos.create_pool(source, &job.workspace_incarnation) {
        Ok(pool) => pool,
        Err(error) => {
            let _ = write_response(
                &mut writer,
                &failure(
                    ControlFailureCode::Rejected,
                    format!("cannot serve the resident job's CPU pool: {error}"),
                ),
            )
            .await;
            return;
        }
    };
    cowshed_os::timing::event("jobserver", || {
        format!(
            "resident pool for {}/{} job {} at {}: {command}",
            job.repo_id,
            job.workspace_incarnation,
            job.job_id,
            pool.path().display()
        )
    });
    let answer = ControlResponse {
        cpu_pool: Some(pool.path().to_string_lossy().into_owned()),
        ..lease(LeaseState::Granted)
    };
    if write_response(&mut writer, &answer).await.is_ok() {
        let mut discard = [0_u8; 64];
        while let Ok(read) = reader.read(&mut discard).await
            && read > 0
        {}
    }
    if let Err(error) = pool.close().await {
        cowshed_os::eprintln!("cowshed: resident CPU pool did not close: {error}");
    }
}

/// A checkout a CPU request names: a path of 1 to [`MAX_CHECKOUT`] bytes.
fn checked_checkout(checkout: &str) -> Result<(), ControlError> {
    if checkout.is_empty() || checkout.len() > MAX_CHECKOUT {
        return Err(ControlError::Encoding(format!(
            "a cpu-tokens checkout must name a path of 1 to {MAX_CHECKOUT} bytes"
        )));
    }
    Ok(())
}

/// The trusted job identity a private CPU pool is scoped by: a JavaScript-safe job id and a
/// workspace incarnation the pool directory layout accepts.
fn checked_job(job: JobserverJobIn) -> Result<JobserverJobIn, ControlError> {
    if job.job_id.get() > 9_007_199_254_740_991 {
        return Err(ControlError::Encoding(
            "jobId must be a positive JavaScript-safe integer".to_owned(),
        ));
    }
    cowshed_gateway_types::cpu_pool_directory(Path::new("."), &job.workspace_incarnation)
        .map_err(|error| ControlError::Encoding(error.to_string()))?;
    Ok(job)
}

fn parse_control_request(bytes: &[u8]) -> Result<Incoming, ControlError> {
    let value =
        serde_json::from_slice(bytes).map_err(|error| ControlError::Encoding(error.to_string()))?;
    parse_control_request_value(value)
}

fn parse_control_request_value(value: serde_json::Value) -> Result<Incoming, ControlError> {
    let object = value
        .as_object()
        .ok_or_else(|| ControlError::Encoding("control request must be an object".to_owned()))?;
    let op = object
        .get("op")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ControlError::Encoding("control request op is required".to_owned()))?;
    let allowed: &[&str] = match op {
        "status" => &["op"],
        "session-current" => &["op", "workspaceId", "revision"],
        "install" => &["op", "session"],
        "remove" => &["op", "workspaceId", "expectedRevision"],
        "audit-tail" => &["op", "workspaceId", "afterSequence", "limit", "follow"],
        "repo-mirror" => &["op", "request"],
        "sim-configure" => &["op", "config"],
        "sim-approve" => &["op", "approval"],
        "sim-list" => &["op", "repoId"],
        "sim-boot" => &["op", "repoId", "device"],
        "cpu-budget" => &["op"],
        "task-terminal" => &["op", "unit", "outcome"],
        "task-queue" => &["op"],
        "nx-cache-clear" => &["op", "repoId"],
        RAM_LEASE_OP => &["op", "sectors"],
        RAM_DETACH_OP => &["op", "device", "registryEntry"],
        FIRST_LAUNCH_OP => &["op"],
        TASK_ADMIT_OP => &[
            "op",
            "unit",
            "owner",
            "incarnation",
            "deps",
            "leases",
            "want",
            "priority",
            "urgent",
            "class",
            "cost",
            "deadline",
            "caps",
            "command",
        ],
        DISK_LEASE_OP => &["op", "class", "share", "command"],
        CPU_TOKENS_OP => &["op", "want", "checkout", "command"],
        JOBSERVER_OP => &[
            "op",
            "want",
            "checkout",
            "command",
            "class",
            "pool",
            "job",
            "parentPool",
            "probation",
        ],
        RESIDENT_POOL_OP => &["op", "checkout", "command", "job"],
        _ => {
            return Err(ControlError::Encoding(
                "unknown gateway control operation".to_owned(),
            ));
        }
    };
    if object.keys().any(|key| !allowed.contains(&key.as_str())) {
        return Err(ControlError::Encoding(
            "gateway control request contains an unknown field".to_owned(),
        ));
    }
    let encoding = |error: serde_json::Error| ControlError::Encoding(error.to_string());
    if op == RAM_LEASE_OP {
        #[derive(Deserialize)]
        struct RamLeaseIn {
            sectors: RamDiskSectors,
        }
        let RamLeaseIn { sectors } = serde_json::from_value(value).map_err(encoding)?;
        return Ok(Incoming::RamLease { sectors });
    }
    if op == DISK_LEASE_OP {
        let DiskLeaseIn {
            class,
            share,
            command,
        } = serde_json::from_value(value).map_err(encoding)?;
        return Ok(Incoming::DiskLease {
            class,
            share,
            command,
        });
    }
    if op == RESIDENT_POOL_OP {
        let ResidentPoolIn {
            checkout,
            command,
            job,
        } = serde_json::from_value(value).map_err(encoding)?;
        checked_checkout(&checkout)?;
        return Ok(Incoming::ResidentPool {
            checkout,
            command,
            job: checked_job(job)?,
        });
    }
    let jobserver = op == JOBSERVER_OP;
    if op == CPU_TOKENS_OP || jobserver {
        let (tokens, options) = if jobserver {
            let JobserverIn {
                tokens,
                class,
                pool,
                job,
                parent_pool,
                probation,
            } = serde_json::from_value(value).map_err(encoding)?;
            if let Some(parent) = &parent_pool
                && (!Path::new(parent).is_absolute()
                    || parent.len() > MAX_CHECKOUT
                    || parent.contains('\0'))
            {
                return Err(ControlError::Encoding(format!(
                    "a parentPool is an absolute socket path of at most {MAX_CHECKOUT} bytes \
                     without NUL"
                )));
            }
            let pool_job = if pool {
                let job = job.ok_or_else(|| {
                    ControlError::Encoding(
                        "a private CPU pool requires the trusted job identity".to_owned(),
                    )
                })?;
                Some(checked_job(job)?)
            } else {
                None
            };
            let admission = match class {
                crate::jobserver::JobserverAdmission::Background => {
                    crate::cpu_budget::Class::Background
                }
                crate::jobserver::JobserverAdmission::Gate => {
                    let job = pool_job.as_ref().ok_or_else(|| {
                        ControlError::Encoding(
                            "a gate requires a private CPU pool and the trusted repository/job identity".to_owned(),
                        )
                    })?;
                    if parent_pool.is_some() {
                        return Err(ControlError::Encoding(
                            "a gate is admitted by the host repository queue, never a parentPool"
                                .to_owned(),
                        ));
                    }
                    crate::cpu_budget::Class::Gate(Arc::from(job.repo_id.as_str()))
                }
            };
            (
                tokens,
                JobserverOptions {
                    admission,
                    pool_job,
                    parent_pool: parent_pool.map(PathBuf::from),
                    probation,
                },
            )
        } else {
            (
                serde_json::from_value(value).map_err(encoding)?,
                JobserverOptions {
                    admission: crate::cpu_budget::Class::Background,
                    pool_job: None,
                    parent_pool: None,
                    probation: false,
                },
            )
        };
        let CpuTokensIn {
            want,
            checkout,
            command,
        } = tokens;
        checked_checkout(&checkout)?;
        return Ok(if jobserver {
            Incoming::Jobserver {
                want,
                checkout,
                command,
                options,
            }
        } else {
            Incoming::CpuTokens {
                want,
                checkout,
                command,
            }
        });
    }
    if op == TASK_ADMIT_OP {
        let TaskAdmitIn {
            unit,
            owner,
            incarnation,
            deps,
            leases,
            want,
            priority,
            urgent,
            class,
            cost,
            deadline,
            caps,
            command,
        } = serde_json::from_value(value).map_err(encoding)?;
        for named in [&unit, &owner] {
            if named.is_empty() || named.len() > MAX_CHECKOUT {
                return Err(ControlError::Encoding(format!(
                    "a task-admit unit and owner must name 1 to {MAX_CHECKOUT} bytes"
                )));
            }
        }
        if deps.len() > 64 {
            return Err(ControlError::Encoding(
                "a task-admit request may declare at most 64 dependencies".to_owned(),
            ));
        }
        return Ok(Incoming::TaskAdmit {
            unit,
            owner,
            incarnation,
            deps,
            leases,
            want,
            priority,
            urgent,
            class,
            cost,
            deadline,
            caps,
            command,
        });
    }
    serde_json::from_value(value)
        .map(Incoming::Request)
        .map_err(encoding)
}

async fn serve_request<W>(
    mut writer: W,
    request: Result<ControlRequest, ControlError>,
    services: ControlServices,
) where
    W: AsyncWrite + Unpin,
{
    let (response, mut subscription) = match request {
        Ok(request) => dispatch(request, &services).await,
        Err(error) => {
            let code = if matches!(error, ControlError::Unauthorized) {
                ControlFailureCode::Unauthorized
            } else {
                ControlFailureCode::InvalidRequest
            };
            (failure(code, error.to_string()), None)
        }
    };
    if write_response(&mut writer, &response).await.is_err() {
        return;
    }
    if let Some(receiver) = subscription.as_mut() {
        while let Some(event) = receiver.recv().await {
            let response = ControlResponse {
                audit_event: Some(event),
                ..success()
            };
            if write_response(&mut writer, &response).await.is_err() {
                break;
            }
        }
    }
    let _ = writer.shutdown().await;
}

async fn dispatch(
    request: ControlRequest,
    services: &ControlServices,
) -> (ControlResponse, Option<mpsc::Receiver<AuditEvent>>) {
    match request {
        ControlRequest::SessionCurrent {
            workspace_id,
            revision,
        } => (
            match services
                .handle
                .session_current(workspace_id, revision)
                .await
            {
                Ok(current) => ControlResponse {
                    current: Some(current),
                    ..success()
                },
                Err(error) => rejected(error),
            },
            None,
        ),
        ControlRequest::Status => (
            match services.handle.status().await {
                Ok(status) => ControlResponse {
                    status: Some(status),
                    ..success()
                },
                Err(error) => rejected(error),
            },
            None,
        ),
        ControlRequest::Install { session } => (
            match (services.healing(), session.into_session()) {
                (Some(refused), _) => refused,
                (None, Ok(session)) => match services.handle.install(session).await {
                    Ok(()) => success(),
                    Err(error) => rejected(error),
                },
                (None, Err(error)) => invalid_session(error.to_string()),
            },
            None,
        ),
        ControlRequest::Remove {
            workspace_id,
            expected_revision,
        } => (
            match services.healing() {
                Some(refused) => refused,
                None => match services
                    .handle
                    .remove(workspace_id, expected_revision)
                    .await
                {
                    Ok(()) => success(),
                    Err(error) => rejected(error),
                },
            },
            None,
        ),
        ControlRequest::AuditTail {
            workspace_id,
            after_sequence,
            limit,
            follow,
        } => {
            if limit == 0 || limit > MAX_AUDIT_TAIL_LIMIT {
                return (
                    failure(
                        ControlFailureCode::InvalidRequest,
                        "audit tail limit must be between 1 and 1024".to_owned(),
                    ),
                    None,
                );
            }
            let Some(tail) = services.audit_tail.as_ref() else {
                return (
                    failure(
                        ControlFailureCode::Rejected,
                        "audit tail is unavailable for this injected gateway".to_owned(),
                    ),
                    None,
                );
            };
            let query = AuditTailQuery {
                workspace_id,
                after_sequence,
                limit,
            };
            let subscription = if follow {
                match tail.subscribe(query.clone()).await {
                    Ok(subscription) => Some(subscription),
                    Err(error) => {
                        return (
                            failure(ControlFailureCode::Rejected, error.to_string()),
                            None,
                        );
                    }
                }
            } else {
                None
            };
            match tail.query(query).await {
                Ok(events) => (
                    ControlResponse {
                        audit_events: Some(events),
                        ..success()
                    },
                    subscription,
                ),
                Err(error) => (
                    failure(ControlFailureCode::Rejected, error.to_string()),
                    None,
                ),
            }
        }
        ControlRequest::RepoMirror { request } => (
            match services.repo_mirror.mirror(request).await {
                Ok(info) => ControlResponse {
                    mirror_info: Some(info),
                    ..success()
                },
                Err(error) => repo_rejected(error),
            },
            None,
        ),
        ControlRequest::SimConfigure { config } => (
            match services.sim_broker.configure(config).await {
                Ok(()) => success(),
                Err(error) => sim_rejected(error),
            },
            None,
        ),
        ControlRequest::SimApprove { approval } => (
            match services.sim_broker.approve(approval).await {
                Ok(()) => success(),
                Err(error) => sim_rejected(error),
            },
            None,
        ),
        ControlRequest::SimList { repo_id } => (
            match services.sim_broker.list_devices(repo_id).await {
                Ok(devices) => ControlResponse {
                    sim_devices: Some(devices),
                    ..success()
                },
                Err(error) => sim_rejected(error),
            },
            None,
        ),
        ControlRequest::SimBoot { repo_id, device } => (
            match services.sim_broker.boot_device(repo_id, device).await {
                Ok(()) => success(),
                Err(error) => sim_rejected(error),
            },
            None,
        ),
        ControlRequest::CpuBudget => (
            match services.cpu_budget.status().await {
                Some(budget) => ControlResponse {
                    cpu_budget: Some(budget),
                    ..success()
                },
                None => failure(
                    ControlFailureCode::Rejected,
                    "the gateway's CPU budget has stopped".to_owned(),
                ),
            },
            None,
        ),
        ControlRequest::TaskTerminal { unit, outcome } => (
            match services.task_server.terminal(&unit, outcome).await {
                Ok(()) => success(),
                Err(refusal) => failure(ControlFailureCode::NotAdmitted, refusal.to_string()),
            },
            None,
        ),
        ControlRequest::TaskQueue => (
            match services.task_server.queue().await {
                Some(queue) => ControlResponse {
                    task_queue: Some(queue),
                    ..success()
                },
                None => failure(
                    ControlFailureCode::Rejected,
                    "the gateway's task scheduler has stopped".to_owned(),
                ),
            },
            None,
        ),
        ControlRequest::NxCacheClear { repo_id } => (
            match services
                .handle
                .clear_nx_cache(repo_id.as_str().to_owned())
                .await
            {
                Ok(entries) => ControlResponse {
                    nx_cache_cleared: Some(cowshed_core::api::NxStoreClearResult {
                        repo_id,
                        entries,
                    }),
                    ..success()
                },
                Err(error) => rejected(error),
            },
            None,
        ),
        ControlRequest::RamDetach {
            device,
            registry_entry,
        } => (
            detach_ram_disk(services.ram_disks.clone(), device, registry_entry).await,
            None,
        ),
        ControlRequest::FirstLaunch => (
            measure_first_launch(services.first_launch.clone()).await,
            None,
        ),
    }
}

/// Hold one `ram-lease` (05_gateway.md, "RAM disks"): attach and record a disk, answer it, then
/// settle the lease by the one line its owner writes next ([`crate::ram_lease::settle`]). The
/// attach and every detach run on blocking threads no connection owns, so a connection cut off in
/// either still finishes it; one cut off while it waits for its owner leaves the record to the next
/// gateway's startup scan.
async fn hold_ram_lease<R, W>(
    mut reader: R,
    mut writer: W,
    sectors: RamDiskSectors,
    disks: Option<Arc<dyn RamDisks>>,
    records: crate::ram_lease::Records,
) where
    R: AsyncRead + Unpin,
    W: AsyncWrite + Unpin,
{
    use crate::ram_lease::{self, Refused, Reply, Settlement};
    let Some(disks) = disks else {
        let refusal = failure(
            ControlFailureCode::Rejected,
            "this gateway attaches no RAM disk: its daemon supplied no disk runner".to_owned(),
        );
        let _ = write_response(&mut writer, &refusal).await;
        return;
    };
    let (attacher, attaching) = (Arc::clone(&disks), records.clone());
    let attached =
        tokio::task::spawn_blocking(move || ram_lease::attach(&*attacher, &attaching, sectors))
            .await;
    let lease = match attached {
        Ok(Ok(lease)) => lease,
        Ok(Err(refused)) => {
            let refusal = match refused {
                Refused::Attach(refusal @ RamAttachRefusal::TooLarge { .. }) => {
                    failure(ControlFailureCode::InvalidRequest, refusal.to_string())
                }
                Refused::Attach(refusal @ RamAttachRefusal::Failed(_)) => {
                    failure(ControlFailureCode::Rejected, refusal.to_string())
                }
                Refused::Unrecorded { error, detached } => failure(
                    ControlFailureCode::Rejected,
                    format!(
                        "the gateway attached a RAM disk but could not record its lease ({error}); \
                         its detach: {detached}"
                    ),
                ),
            };
            let _ = write_response(&mut writer, &refusal).await;
            return;
        }
        Err(error) => {
            let refusal = failure(
                ControlFailureCode::Rejected,
                format!("the RAM disk attach thread failed: {error}"),
            );
            let _ = write_response(&mut writer, &refusal).await;
            return;
        }
    };
    let held = ControlResponse {
        ram_disk: Some(lease.clone()),
        ..success()
    };
    let ending = match write_response(&mut writer, &held).await {
        Ok(()) => {
            let line = read_request_line(&mut reader).await.ok();
            ram_lease::Ending::read(line.as_ref().map(|line| line.as_slice()))
        }
        Err(_) => ram_lease::Ending::Gone,
    };
    let (reply, why) = match ram_lease::settle(ending) {
        Settlement::Forget => {
            let forgotten = records.forget(lease.registry_entry);
            match ram_lease::after_forget(forgotten.is_ok()) {
                None => {
                    let _ = write_response(&mut writer, &success()).await;
                    return;
                }
                Some(reply) => (
                    reply,
                    format!(
                        "was kept, but its record could not be forgotten ({})",
                        forgotten
                            .err()
                            .map_or_else(String::new, |error| error.to_string())
                    ),
                ),
            }
        }
        Settlement::Detach(reply) => (
            reply,
            match reply {
                Reply::Outcome => "was released by its owner",
                Reply::Invalid => "ended with a line that is no end",
                Reply::Rejected => "could not be kept",
                Reply::Nobody => "lost its owner before it was kept or released",
            }
            .to_owned(),
        ),
    };
    let (detacher, detaching, ended, said) = (Arc::clone(&disks), records, lease, why.clone());
    let detached = match tokio::task::spawn_blocking(move || {
        ram_lease::detach(&*detacher, &detaching, &ended, &said)
    })
    .await
    {
        Ok(detached) => detached,
        Err(error) if error.is_cancelled() => RamDetached::NotStarted {
            reason: format!("the RAM disk detach thread was cancelled before it ran: {error}"),
        },
        Err(error) => RamDetached::Unknown {
            reason: format!("the RAM disk detach thread failed after it started: {error}"),
        },
    };
    let response = match reply {
        Reply::Outcome => ControlResponse {
            ram_detach: Some(detached),
            ..success()
        },
        Reply::Invalid => failure(
            ControlFailureCode::InvalidRequest,
            format!(
                "a ram-lease ends with one {{\"end\":\"keep\"}} or {{\"end\":\"release\"}} line; \
                 the gateway detached the disk: {detached}"
            ),
        ),
        Reply::Rejected => failure(
            ControlFailureCode::Rejected,
            format!("the RAM disk lease {why}; the gateway detached the disk: {detached}"),
        ),
        Reply::Nobody => return,
    };
    let _ = write_response(&mut writer, &response).await;
}

/// Measure a fresh executable's first launch on a blocking thread: each launch runs a process to
/// its exit, and the probe waits for both (05_gateway.md, "Control plane").
async fn measure_first_launch(probe: Option<Arc<dyn FirstLaunchProbe>>) -> ControlResponse {
    let Some(probe) = probe else {
        return failure(
            ControlFailureCode::Rejected,
            "this gateway measures no first launch: its daemon supplied no probe".to_owned(),
        );
    };
    match tokio::task::spawn_blocking(move || probe.measure()).await {
        Ok(Ok(first_launch)) => ControlResponse {
            first_launch: Some(first_launch),
            ..success()
        },
        Ok(Err(message)) => failure(ControlFailureCode::Rejected, message),
        Err(_) => failure(
            ControlFailureCode::Rejected,
            "the first-launch probe thread failed".to_owned(),
        ),
    }
}

/// The connection's one request line, newline included. Nothing may follow it in what has
/// arrived so far: a one-shot client half-closes after its line, and a lease client sends nothing
/// more. Bytes are read in chunks rather than through a buffered reader, so every byte of a
/// controller credential stays in zeroized memory.
async fn read_request_line<R>(reader: &mut R) -> Result<Zeroizing<Vec<u8>>, ControlError>
where
    R: AsyncRead + Unpin,
{
    let mut line = Zeroizing::new(Vec::new());
    let mut chunk = Zeroizing::new([0_u8; 4096]);
    loop {
        let read = reader.read(&mut chunk[..]).await?;
        if read == 0 {
            return Err(ControlError::MessageTooLarge);
        }
        let start = line.len();
        line.extend_from_slice(&chunk[..read]);
        if let Some(offset) = line[start..].iter().position(|byte| *byte == b'\n') {
            let end = start + offset + 1;
            if end != line.len()
                || end > MAX_CONTROL_MESSAGE as usize
                || line[..end - 1].contains(&b'\r')
            {
                return Err(ControlError::MessageTooLarge);
            }
            return Ok(line);
        }
        if line.len() > MAX_CONTROL_MESSAGE as usize {
            return Err(ControlError::MessageTooLarge);
        }
    }
}

/// The client's EOF after a one-shot request: anything else is a second message.
async fn expect_eof<R>(reader: &mut R) -> Result<(), ControlError>
where
    R: AsyncRead + Unpin,
{
    let mut extra = [0_u8; 1];
    match reader.read(&mut extra).await? {
        0 => Ok(()),
        _ => Err(ControlError::MessageTooLarge),
    }
}

async fn read_response_line<R>(reader: &mut BufReader<R>) -> Result<ControlResponse, ControlError>
where
    R: AsyncRead + Unpin,
{
    read_answer_line(reader).await
}

/// One answer line of at most [`MAX_CONTROL_MESSAGE`] bytes, read as `T`.
async fn read_answer_line<T, R>(reader: &mut BufReader<R>) -> Result<T, ControlError>
where
    T: serde::de::DeserializeOwned,
    R: AsyncRead + Unpin,
{
    let mut bytes = Vec::new();
    reader
        .take(MAX_CONTROL_MESSAGE + 1)
        .read_until(b'\n', &mut bytes)
        .await?;
    if bytes.is_empty() {
        return Err(ControlError::UnexpectedEof);
    }
    if bytes.len() > MAX_CONTROL_MESSAGE as usize || !bytes.ends_with(b"\n") {
        return Err(ControlError::InvalidResponse);
    }
    serde_json::from_slice(&bytes).map_err(|error| ControlError::Encoding(error.to_string()))
}

async fn write_response<W>(writer: &mut W, response: &ControlResponse) -> Result<(), ControlError>
where
    W: AsyncWrite + Unpin,
{
    let mut encoded =
        serde_json::to_vec(response).map_err(|error| ControlError::Encoding(error.to_string()))?;
    encoded.push(b'\n');
    if encoded.len() > MAX_CONTROL_MESSAGE as usize {
        return Err(ControlError::MessageTooLarge);
    }
    writer.write_all(&encoded).await?;
    Ok(())
}

fn check_response(response: &ControlResponse) -> Result<(), ControlError> {
    if response.ok {
        Ok(())
    } else {
        Err(ControlError::Rejected {
            code: response.code.ok_or(ControlError::InvalidResponse)?,
            message: response
                .error
                .clone()
                .ok_or(ControlError::InvalidResponse)?,
        })
    }
}

/// Detach a RAM disk on a blocking thread no connection owns: once dispatched, the detach runs to
/// its end and the exact `hdiutil` child is reaped whether or not the requester still waits, so its
/// disconnect, death or job retirement never cancels it (05_gateway.md, "RAM disks"). Every
/// outcome is said on the gateway's stderr, a started detach's by its own thread, so it is said
/// even when no connection remains to hear the answer.
async fn detach_ram_disk(
    disks: Option<Arc<dyn RamDisks>>,
    device: RamDiskDevice,
    entry: RamDiskEntry,
) -> ControlResponse {
    let say = move |device: &RamDiskDevice, detached: &RamDetached| {
        cowshed_os::eprintln!("cowshed: ram-detach {device} entry={entry} outcome={detached}");
    };
    let detached = match disks {
        None => {
            let detached = RamDetached::NotStarted {
                reason: "this gateway detaches no RAM disk: its daemon supplied no disk runner"
                    .to_owned(),
            };
            say(&device, &detached);
            detached
        }
        Some(disks) => {
            let target = device.clone();
            match tokio::task::spawn_blocking(move || {
                let detached = disks.detach(
                    &target,
                    entry,
                    cowshed_core::api::dto::MAX_INLINE_OUTPUT_BYTES,
                );
                say(&target, &detached);
                detached
            })
            .await
            {
                Ok(detached) => detached,
                Err(error) => {
                    // A blocking task is cancelled only before it starts running.
                    let detached = if error.is_cancelled() {
                        RamDetached::NotStarted {
                            reason: format!(
                                "the RAM disk detach thread was cancelled before it ran: {error}"
                            ),
                        }
                    } else {
                        RamDetached::Unknown {
                            reason: format!(
                                "the RAM disk detach thread failed after it started: {error}"
                            ),
                        }
                    };
                    say(&device, &detached);
                    detached
                }
            }
        }
    };
    ControlResponse {
        ram_detach: Some(detached),
        ..success()
    }
}
fn success() -> ControlResponse {
    ControlResponse {
        ok: true,
        status: None,
        current: None,
        audit_events: None,
        audit_event: None,
        mirror_info: None,
        sim_devices: None,
        code: None,
        error: None,
        lease: None,
        tokens: None,
        fifo: None,
        cpu_pool: None,
        cpu_budget: None,
        queue: None,
        task_grant: None,
        task_queue: None,
        nx_cache_cleared: None,
        install: None,
        ram_disk: None,
        ram_detach: None,
        first_launch: None,
    }
}

fn lease(state: LeaseState) -> ControlResponse {
    ControlResponse {
        lease: Some(state),
        ..success()
    }
}

fn failure(code: ControlFailureCode, error: String) -> ControlResponse {
    ControlResponse {
        ok: false,
        code: Some(code),
        error: Some(error),
        ..success()
    }
}

/// A scheduler that dropped a waiter's grant has stopped.
fn scheduler_stopped(scheduler: &str) -> ControlResponse {
    failure(
        ControlFailureCode::Rejected,
        format!("the gateway's {scheduler} has stopped"),
    )
}

/// The refusal of a queued CPU ask whose grant the budget dropped: cut off for the install
/// restarting the gateway, which it names, so its client asks the gateway that install starts
/// once (05_gateway.md "A queued CPU ask outlives an install"); otherwise the budget stopped.
fn cpu_ask_cut_off(budget: &CpuBudget) -> ControlResponse {
    match budget.installing() {
        Some(install) => ControlResponse {
            install: Some(install.clone()),
            ..failure(
                ControlFailureCode::InstallRestart,
                crate::cpu_budget::cut_off_by(install),
            )
        },
        None => scheduler_stopped("CPU budget"),
    }
}

fn invalid_session(error: String) -> ControlResponse {
    failure(ControlFailureCode::InvalidSession, error)
}

fn rejected(error: GatewayError) -> ControlResponse {
    let code = match &error {
        GatewayError::StaleRevision | GatewayError::RevisionMismatch { .. } => {
            ControlFailureCode::RevisionFence
        }
        GatewayError::EndpointInUse => ControlFailureCode::EndpointConflict,
        GatewayError::Io(source) if source.kind() == std::io::ErrorKind::AddrInUse => {
            ControlFailureCode::AddressInUse
        }
        GatewayError::UnknownWorkspace => ControlFailureCode::NotInstalled,
        GatewayError::Config(_) | GatewayError::Tls(_) => ControlFailureCode::InvalidSession,
        GatewayError::NxCacheClear(refusal) if refusal.is_busy() => ControlFailureCode::Busy,
        _ => ControlFailureCode::Rejected,
    };
    failure(code, error.to_string())
}

fn repo_rejected(error: RepoMirrorError) -> ControlResponse {
    let code = match error {
        RepoMirrorError::NotAdmitted | RepoMirrorError::ScopeMismatch => {
            ControlFailureCode::NotAdmitted
        }
        _ => ControlFailureCode::BrokerRejected,
    };
    failure(code, error.to_string())
}

fn sim_rejected(error: SimBrokerError) -> ControlResponse {
    failure(ControlFailureCode::BrokerRejected, error.to_string())
}

fn read_controller_credential(path: &Path) -> Result<Zeroizing<String>, ControlError> {
    use std::{
        io::Read as _,
        os::unix::fs::{MetadataExt as _, OpenOptionsExt as _, PermissionsExt as _},
    };

    let metadata = std::fs::symlink_metadata(path)?;
    if !metadata.is_file()
        || metadata.file_type().is_symlink()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.permissions().mode() & 0o777 != 0o600
        || metadata.len() > 128
    {
        return Err(ControlError::InvalidControllerCredential);
    }
    let file = std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(path)?;
    let opened = file.metadata()?;
    if opened.dev() != metadata.dev() || opened.ino() != metadata.ino() {
        return Err(ControlError::InvalidControllerCredential);
    }
    let mut encoded = String::new();
    file.take(129).read_to_string(&mut encoded)?;
    while matches!(encoded.as_bytes().last(), Some(b'\n' | b'\r')) {
        encoded.pop();
    }
    let credential = Zeroizing::new(encoded);
    let expected_len = CONTROLLER_CREDENTIAL_PREFIX.len()
        + URL_SAFE_NO_PAD
            .encode([0_u8; CONTROLLER_CREDENTIAL_BYTES])
            .len();
    if credential.len() != expected_len {
        return Err(ControlError::InvalidControllerCredential);
    }
    Ok(credential)
}

#[derive(Debug, Error)]
pub enum ControlError {
    #[error("gateway control socket path must be absolute")]
    InvalidSocketPath,
    #[error("gateway control TCP endpoint or credential path is invalid")]
    InvalidTcpEndpoint,
    #[error("gateway controller credential is invalid")]
    InvalidControllerCredential,
    #[error("gateway control authentication failed")]
    Unauthorized,
    #[error("gateway control message exceeds 1 MiB")]
    MessageTooLarge,
    #[error("gateway audit tail limit is invalid")]
    InvalidAuditTailLimit,
    #[error("gateway control session is invalid: {0}")]
    InvalidSession(String),
    #[error("gateway control encoding failed: {0}")]
    Encoding(String),
    #[error("gateway control response is invalid")]
    InvalidResponse,
    #[error("gateway control stream ended")]
    UnexpectedEof,
    #[error("gateway control rejected operation ({code:?}): {message}")]
    Rejected {
        code: ControlFailureCode,
        message: String,
    },
    #[error("gateway control I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("the gateway took the request and gave no answer within {within:?}")]
    Unanswered { within: Duration },
}

#[cfg(test)]
#[path = "../tests/support/fixture_dir.rs"]
pub(crate) mod test_fixture_dir;
#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt as _;

    use super::*;
    use uuid::Uuid;

    #[test]
    fn controller_credential_domain_rejects_data_plane_tokens() {
        let bytes = [7_u8; CONTROLLER_CREDENTIAL_BYTES];
        let credential = ControllerCredential(bytes);
        let data_token = URL_SAFE_NO_PAD.encode(bytes);
        let encoded = format!("{CONTROLLER_CREDENTIAL_PREFIX}{data_token}");
        assert!(!credential.matches(&data_token));
        assert!(credential.matches(&encoded));
        assert!(!format!("{credential:?}").contains(&data_token));
    }

    #[test]
    fn request_schema_rejects_unknown_fields_verbs_and_envelope_shapes() {
        for request in [
            r#"{"op":"status","token":"workspace-token"}"#,
            r#"{"op":"session-current","workspaceId":"ws","revision":1,"extra":true}"#,
            r#"{"op":"session-current","workspaceId":"ws"}"#,
            r#"{"op":"session-current","revision":1}"#,
            r#"{"op":"session-current","workspaceId":"ws","revision":-1}"#,
            r#"{"op":"repo-mirror","request":{"workspaceId":"ws","repoId":"repo","remote":"https://example.test/repo"},"push":true}"#,
            r#"{"op":"sim-openurl","url":"demo://value"}"#,
            r#"{"op":"unknown"}"#,
            r#"{"op":"disk-lease","class":"storage","command":"diskutil","pid":1}"#,
            r#"{"op":"disk-lease","class":"compute","command":"diskutil"}"#,
            r#"{"op":"disk-lease","class":"storage","share":"half","command":"diskutil"}"#,
            r#"{"op":"disk-lease","class":"storage"}"#,
            r#"{"op":"cpu-tokens","want":0,"checkout":"/w","command":"nextest"}"#,
            r#"{"op":"cpu-tokens","want":-1,"checkout":"/w","command":"nextest"}"#,
            r#"{"op":"cpu-tokens","want":4,"checkout":"","command":"nextest"}"#,
            r#"{"op":"cpu-tokens","want":4,"command":"nextest"}"#,
            r#"{"op":"cpu-tokens","want":4,"checkout":"/w","command":"nextest","pid":1}"#,
            r#"{"op":"cpu-budget","verbose":true}"#,
            r#"{"op":"first-launch","x":1}"#,
        ] {
            assert!(parse_control_request(request.as_bytes()).is_err());
        }
        assert!(matches!(
            parse_control_request(
                br#"{"op":"disk-lease","class":"namespace","command":"/sbin/umount /x"}"#
            ),
            Ok(Incoming::DiskLease {
                class: DiskClass::Namespace,
                share: PhaseShare::Shared,
                command,
            }) if command == "/sbin/umount /x"
        ));
        assert!(matches!(
            parse_control_request(
                br#"{"op":"disk-lease","class":"storage","share":"alone","command":"/usr/bin/hdiutil detach"}"#
            ),
            Ok(Incoming::DiskLease {
                class: DiskClass::Storage,
                share: PhaseShare::Alone,
                command,
            }) if command == "/usr/bin/hdiutil detach"
        ));
        assert!(matches!(
            parse_control_request(
                br#"{"op":"cpu-tokens","want":64,"checkout":"/w","command":"cargo nextest run"}"#
            ),
            Ok(Incoming::CpuTokens { want, checkout, command })
                if want.get() == 64 && checkout == "/w" && command == "cargo nextest run"
        ));
        assert!(matches!(
            parse_control_request(br#"{"op":"cpu-budget"}"#),
            Ok(Incoming::Request(ControlRequest::CpuBudget))
        ));
        assert!(matches!(
            parse_control_request(br#"{"op":"nx-cache-clear","repoId":"acme/widget"}"#),
            Ok(Incoming::Request(ControlRequest::NxCacheClear { repo_id })) if repo_id.as_str() == "acme/widget"
        ));
        assert!(
            parse_control_request(br#"{"op":"nx-cache-clear","repoId":"not a repo"}"#).is_err()
        );
        assert!(
            parse_control_request(br#"{"op":"nx-cache-clear","repoId":"acme/widget","all":true}"#)
                .is_err()
        );

        assert!(matches!(
            parse_control_request(br#"{"op":"first-launch"}"#),
            Ok(Incoming::Request(ControlRequest::FirstLaunch))
        ));

        // A RAM lease names a positive size and nothing else: no argv reaches hdiutil.
        assert!(matches!(
            parse_control_request(br#"{"op":"ram-lease","sectors":131072}"#),
            Ok(Incoming::RamLease { sectors }) if sectors.get() == 131_072
        ));
        for refused in [
            &br#"{"op":"ram-lease","sectors":0}"#[..],
            br#"{"op":"ram-lease","sectors":-1}"#,
            br#"{"op":"ram-lease","sectors":"131072"}"#,
            br#"{"op":"ram-lease"}"#,
            br#"{"op":"ram-lease","sectors":131072,"argv":["-force"]}"#,
            br#"{"op":"ram-attach","sectors":131072}"#,
        ] {
            assert!(parse_control_request(refused).is_err());
        }

        // A RAM detach names a whole disk and its registry entry and nothing else: no force.
        assert!(matches!(
            parse_control_request(br#"{"op":"ram-detach","device":"/dev/disk7","registryEntry":"0x100000abc"}"#),
            Ok(Incoming::Request(ControlRequest::RamDetach { device, registry_entry }))
                if device.as_str() == "/dev/disk7" && registry_entry.get() == 0x1_0000_0abc
        ));
        for refused in [
            &br#"{"op":"ram-detach","device":"/dev/disk7"}"#[..],
            br#"{"op":"ram-detach","registryEntry":"0x100000abc"}"#,
            br#"{"op":"ram-detach","device":"/dev/disk7","registryEntry":"0x100000abc","force":true}"#,
            br#"{"op":"ram-detach","device":"/dev/disk7s1","registryEntry":"0x100000abc"}"#,
            br#"{"op":"ram-detach","device":"/dev/disk7","registryEntry":"0x100000ABC"}"#,
            br#"{"op":"ram-detach","device":"/dev/disk7","registryEntry":"0x0"}"#,
            br#"{"op":"ram-detach","device":"/dev/disk7","registryEntry":4294970044}"#,
        ] {
            assert!(parse_control_request(refused).is_err());
        }
        assert!(
            parse_control_request(
                format!(
                    r#"{{"op":"cpu-tokens","want":1,"checkout":"{}","command":"x"}}"#,
                    "c".repeat(MAX_CHECKOUT + 1)
                )
                .as_bytes()
            )
            .is_err()
        );
        assert!(
            serde_json::from_str::<AuthenticatedControlRequestIn>(
                r#"{"controllerCredential":"secret","request":{"op":"status"},"extra":true}"#,
            )
            .is_err()
        );
    }

    #[test]
    fn controller_credential_file_requires_real_owned_mode_0600_file() {
        let root = std::env::temp_dir().join(format!("cowshed-control-{}", Uuid::new_v4()));
        std::fs::create_dir(&root).expect("root");
        let path = root.join("controller.credential");
        let value = format!(
            "{CONTROLLER_CREDENTIAL_PREFIX}{}\n",
            URL_SAFE_NO_PAD.encode([9_u8; CONTROLLER_CREDENTIAL_BYTES])
        );
        std::fs::write(&path, value).expect("write credential");
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).expect("mode");
        assert!(ControllerCredential::from_file(&path).is_ok());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).expect("mode");
        assert!(matches!(
            ControllerCredential::from_file(&path),
            Err(ControlError::InvalidControllerCredential)
        ));
        let _ = std::fs::remove_dir_all(root);
    }

    #[tokio::test]
    async fn request_frame_is_bounded_and_requires_exactly_one_message() {
        let (mut writer, mut reader) = tokio::io::duplex(128);
        writer
            .write_all(b"{\"op\":\"status\"}\n{\"op\":\"status\"}\n")
            .await
            .expect("write");
        writer.shutdown().await.expect("shutdown");
        assert!(matches!(
            read_request_line(&mut reader).await,
            Err(ControlError::MessageTooLarge)
        ));

        let (mut writer, mut reader) = tokio::io::duplex(128);
        writer
            .write_all(b"{\"op\":\"status\"}\n")
            .await
            .expect("write");
        assert!(read_request_line(&mut reader).await.is_ok());
        writer
            .write_all(b"{\"op\":\"status\"}\n")
            .await
            .expect("write");
        assert!(matches!(
            expect_eof(&mut reader).await,
            Err(ControlError::MessageTooLarge)
        ));
    }

    /// An install reads the gates of the gateway it is about to replace, which is of another
    /// build (05_gateway.md "An install refuses while a land or a gate runs"). A ledger that lacks
    /// a field this build reports (here `holders`, with a `running` this build no longer reads)
    /// still names its gates, while the full ledger read refuses it: a refused read would let the
    /// install restart the gateway without refusing, and cut a running land off.
    #[tokio::test]
    async fn an_install_reads_the_gates_of_a_ledger_from_another_build() {
        // Under `/tmp`: a bind path is capped at `sun_path`'s 104 bytes.
        let id = Uuid::new_v4().simple().to_string();
        let root = test_fixture_dir::secure_fixture_dir(&format!("csgates-{}", &id[..8]));
        let socket = root.path().join("gateway.sock");
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind");
        let answer = serde_json::json!({"ok": true, "cpuBudget": {"total": 4, "held": 3,
            "checkouts": [{"checkout": "/w/land", "held": 2, "running": 2, "waiting": 1},
                {"checkout": "/w/f", "held": 1, "running": 1, "waiting": 0}],
            "gateJobs": [{"checkout": "/w/land", "job": 3, "held": 2, "running": 2}],
            "gateQueue": [{"checkout": "/w/other", "job": 5, "want": 2},
                {"checkout": "/w/land", "job": 3, "want": 1}],
            "background": [{"checkout": "/w/f", "held": 1, "running": 1, "waiting": 0}]}});
        let answering = tokio::spawn(async move {
            for _ in 0..2 {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let request = read_request_line(&mut stream).await.expect("one request");
                assert_eq!(request.as_slice(), b"{\"op\":\"cpu-budget\"}\n");
                stream
                    .write_all(format!("{answer}\n").as_bytes())
                    .await
                    .expect("answer");
            }
        });
        let client = GatewayControlClient::new(socket).expect("client");
        assert_eq!(
            client.gates_in_flight().await.expect("the gates"),
            BTreeSet::from([
                cowshed_core::api::GateJobRef {
                    job: 3,
                    checkout: "/w/land".into(),
                },
                cowshed_core::api::GateJobRef {
                    job: 5,
                    checkout: "/w/other".into(),
                },
            ])
        );
        assert!(
            client.cpu_budget().await.is_err(),
            "the full ledger of another build is not this build's"
        );
        answering.await.expect("the answers");
    }

    /// A daemon that takes the request and never answers is refused once the asker's bound
    /// elapses, naming the bound; that elapsing is this scenario's event.
    #[tokio::test]
    async fn a_daemon_that_takes_status_and_stays_silent_is_unanswered_within_the_bound() {
        let id = Uuid::new_v4().simple().to_string();
        let root = test_fixture_dir::secure_fixture_dir(&format!("csilent-{}", &id[..8]));
        let socket = root.path().join("gateway.sock");
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind");
        let (taken, answered) = tokio::sync::oneshot::channel();
        let silent = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let request = read_request_line(&mut stream).await.expect("one request");
            assert!(matches!(
                parse_control_request(&request),
                Ok(Incoming::Request(ControlRequest::Status))
            ));
            // Hold the connection open, unanswered, until the client has given up on it.
            answered.await.expect("the client's verdict");
            drop(stream);
        });
        let within = Duration::from_millis(50);
        let client = GatewayControlClient::new(socket).expect("client");
        let refused = client.status_within(within).await;
        taken.send(()).expect("release the silent daemon");
        assert!(
            matches!(refused, Err(ControlError::Unanswered { within: bound }) if bound == within),
            "{refused:?}"
        );
        silent.await.expect("silent daemon");
    }

    #[tokio::test]
    async fn session_current_requires_an_explicit_boolean_answer() {
        let id = Uuid::new_v4().simple().to_string();
        let root = test_fixture_dir::secure_fixture_dir(&format!("cscurrent-{}", &id[..8]));
        let socket = root.path().join("gateway.sock");
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind");
        let answering = tokio::spawn(async move {
            for answer in [
                b"{\"ok\":true,\"current\":true}\n".as_slice(),
                b"{\"ok\":true,\"current\":false}\n".as_slice(),
                b"{\"ok\":true}\n".as_slice(),
            ] {
                let (mut stream, _) = listener.accept().await.expect("accept");
                let request = read_request_line(&mut stream).await.expect("one request");
                assert!(matches!(parse_control_request(&request),
                Ok(Incoming::Request(ControlRequest::SessionCurrent { workspace_id, revision: 7 }))
                    if workspace_id == "ws"));
                expect_eof(&mut stream).await.expect("one frame only");
                stream.write_all(answer).await.expect("answer");
            }
        });
        let client = GatewayControlClient::new(socket).expect("client");
        assert!(client.session_current("ws", 7).await.expect("current"));
        assert!(!client.session_current("ws", 7).await.expect("not current"));
        assert!(matches!(
            client.session_current("ws", 7).await,
            Err(ControlError::InvalidResponse)
        ));
        answering.await.expect("answers");
    }

    /// A first-launch probe that answers the same measurement every time it is asked.
    #[derive(Debug)]
    struct AnsweringFirstLaunch(FirstLaunch);

    impl FirstLaunchProbe for AnsweringFirstLaunch {
        fn measure(&self) -> Result<FirstLaunch, String> {
            Ok(self.0)
        }
    }

    #[tokio::test]
    async fn a_first_launch_answer_is_its_two_launch_times_on_the_wire() {
        let probe = AnsweringFirstLaunch(FirstLaunch {
            first_micros: 7,
            second_micros: 3,
        });
        let answered = measure_first_launch(Some(Arc::new(probe))).await;
        assert_eq!(
            serde_json::to_value(&answered).expect("encode"),
            serde_json::json!({
                "ok": true,
                "firstLaunch": {"firstMicros": 7, "secondMicros": 3},
            })
        );
        let unserved = measure_first_launch(None).await;
        assert_eq!(
            serde_json::to_value(&unserved).expect("encode"),
            serde_json::json!({
                "ok": false,
                "code": "rejected",
                "error": "this gateway measures no first launch: its daemon supplied no probe",
            })
        );
    }

    /// RAM disks whose detach starts, says so, and ends only once the test releases it, then says
    /// how it ended: a detach the test can hold in flight while its connection goes away.
    #[derive(Debug)]
    struct HeldDetach {
        started: std::sync::Mutex<std::sync::mpsc::SyncSender<()>>,
        release: std::sync::Mutex<std::sync::mpsc::Receiver<()>>,
        ended: std::sync::Mutex<std::sync::mpsc::SyncSender<RamDetached>>,
    }

    impl RamDisks for HeldDetach {
        fn attach(&self, sectors: RamDiskSectors) -> Result<RamDiskAttached, RamAttachRefusal> {
            unreachable!("this fixture only detaches; asked to attach {sectors}")
        }

        fn detach(
            &self,
            device: &RamDiskDevice,
            _entry: RamDiskEntry,
            output_limit: usize,
        ) -> RamDetached {
            self.started
                .lock()
                .unwrap()
                .send(())
                .expect("the test waits for the start");
            self.release
                .lock()
                .unwrap()
                .recv()
                .expect("the test releases the detach");
            let detached = RamDetached::terminal(
                cowshed_gateway_types::RamDetachStatus::Exit(0),
                format!(
                    "\"{}\" ejected.\n",
                    device.as_str().trim_start_matches("/dev/")
                )
                .as_bytes(),
                b"",
                output_limit,
            );
            self.ended
                .lock()
                .unwrap()
                .send(detached.clone())
                .expect("the test waits for the end");
            detached
        }

        fn registers(
            &self,
            _device: &RamDiskDevice,
            _entry: RamDiskEntry,
        ) -> std::io::Result<bool> {
            Ok(false)
        }
    }

    #[derive(Debug)]
    struct PanickingDetach;

    impl RamDisks for PanickingDetach {
        fn attach(&self, sectors: RamDiskSectors) -> Result<RamDiskAttached, RamAttachRefusal> {
            unreachable!("this fixture only detaches; asked to attach {sectors}")
        }

        fn detach(
            &self,
            device: &RamDiskDevice,
            _entry: RamDiskEntry,
            _limit: usize,
        ) -> RamDetached {
            panic!("the detach of {device} panicked after it started")
        }

        fn registers(
            &self,
            _device: &RamDiskDevice,
            _entry: RamDiskEntry,
        ) -> std::io::Result<bool> {
            Ok(false)
        }
    }

    /// RAM disks held in memory: an attach registers `/dev/disk9` as entry `0x9`, a detach
    /// takes it out of the registry, and both are counted.
    #[derive(Debug, Default)]
    struct MemoryDisks {
        registered: std::sync::Mutex<Vec<RamDiskEntry>>,
        attaches: std::sync::atomic::AtomicUsize,
        detaches: std::sync::atomic::AtomicUsize,
    }

    impl MemoryDisks {
        fn disk9() -> RamDiskAttached {
            RamDiskAttached {
                device: RamDiskDevice::parse("/dev/disk9").expect("a whole disk"),
                registry_entry: RamDiskEntry::parse("0x9").expect("an entry"),
            }
        }

        fn counts(&self) -> (usize, usize, usize) {
            (
                self.attaches.load(std::sync::atomic::Ordering::SeqCst),
                self.detaches.load(std::sync::atomic::Ordering::SeqCst),
                self.registered.lock().unwrap().len(),
            )
        }
    }

    impl RamDisks for MemoryDisks {
        fn attach(&self, _sectors: RamDiskSectors) -> Result<RamDiskAttached, RamAttachRefusal> {
            self.attaches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            let disk = Self::disk9();
            self.registered.lock().unwrap().push(disk.registry_entry);
            Ok(disk)
        }

        fn detach(
            &self,
            device: &RamDiskDevice,
            entry: RamDiskEntry,
            output_limit: usize,
        ) -> RamDetached {
            self.detaches
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.registered
                .lock()
                .unwrap()
                .retain(|registered| *registered != entry);
            RamDetached::terminal(
                cowshed_gateway_types::RamDetachStatus::Exit(0),
                format!(
                    "\"{}\" ejected.\n",
                    device.as_str().trim_start_matches("/dev/")
                )
                .as_bytes(),
                b"",
                output_limit,
            )
        }

        fn registers(&self, _device: &RamDiskDevice, entry: RamDiskEntry) -> std::io::Result<bool> {
            Ok(self.registered.lock().unwrap().contains(&entry))
        }
    }

    /// A lease record directory of the test's own, beside a socket path nothing binds.
    fn lease_records() -> (std::path::PathBuf, crate::ram_lease::Records) {
        let root = std::env::temp_dir().join(format!("cowshed-ram-lease-{}", Uuid::new_v4()));
        std::fs::create_dir(&root).expect("root");
        let records = crate::ram_lease::Records::beside(&root.join("gateway.sock"));
        (root, records)
    }

    /// Ask `hold_ram_lease` for a disk over an in-memory connection and read its answer; `end`
    /// is the line the owner writes then, or `None` for an owner that goes away. Answers the
    /// attach answer and what came after it, once the lease's task has ended.
    async fn lease_ending(
        disks: &Arc<MemoryDisks>,
        records: &crate::ram_lease::Records,
        end: Option<&str>,
    ) -> (serde_json::Value, String) {
        use tokio::io::{AsyncBufReadExt as _, AsyncReadExt as _, AsyncWriteExt as _};
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (server_reader, server_writer) = tokio::io::split(server);
        let served: Arc<dyn RamDisks> = disks.clone();
        let holding = tokio::spawn(hold_ram_lease(
            server_reader,
            server_writer,
            RamDiskSectors::new(std::num::NonZeroU64::new(16).unwrap()),
            Some(served),
            records.clone(),
        ));
        let mut client = tokio::io::BufReader::new(client);
        let mut answer = String::new();
        client
            .read_line(&mut answer)
            .await
            .expect("the attach answer");
        let after = match end {
            None => {
                drop(client);
                String::new()
            }
            Some(end) => {
                client
                    .get_mut()
                    .write_all(format!("{end}\n").as_bytes())
                    .await
                    .expect("the end line");
                client.get_mut().shutdown().await.expect("half-close");
                let mut after = String::new();
                client
                    .read_to_string(&mut after)
                    .await
                    .expect("the end answer");
                after
            }
        };
        holding.await.expect("the lease's task");
        (
            serde_json::from_str(&answer).expect("an answer line"),
            after,
        )
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_ram_lease_whose_owner_goes_away_is_detached_and_forgotten() {
        let (root, records) = lease_records();
        let disks = Arc::new(MemoryDisks::default());
        let (answer, after) = lease_ending(&disks, &records, None).await;
        assert_eq!(
            answer,
            serde_json::json!({"ok": true, "ramDisk": {"device": "/dev/disk9", "registryEntry": "0x9"}})
        );
        assert_eq!(after, "");
        assert_eq!(
            disks.counts(),
            (1, 1, 0),
            "attached once, detached once, registered none"
        );
        assert!(
            records.left().expect("records").is_empty(),
            "the record went with the disk"
        );
        std::fs::remove_dir_all(root).expect("remove root");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_kept_ram_lease_is_its_owners_and_every_other_end_detaches() {
        let (root, records) = lease_records();
        let kept = Arc::new(MemoryDisks::default());
        let (_, after) = lease_ending(&kept, &records, Some(r#"{"end":"keep"}"#)).await;
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&after).unwrap(),
            serde_json::json!({"ok": true})
        );
        assert_eq!(kept.counts(), (1, 0, 1), "a kept disk stays registered");
        assert!(
            records.left().expect("records").is_empty(),
            "a kept lease is forgotten"
        );

        let released = Arc::new(MemoryDisks::default());
        let (_, after) = lease_ending(&released, &records, Some(r#"{"end":"release"}"#)).await;
        let after: serde_json::Value = serde_json::from_str(&after).unwrap();
        assert_eq!(after["ok"], true, "{after}");
        assert_eq!(after["ramDetach"]["outcome"], "terminal", "{after}");
        assert_eq!(
            after["ramDetach"]["status"],
            serde_json::json!({"exit": 0}),
            "{after}"
        );
        assert_eq!(released.counts(), (1, 1, 0));

        let garbled = Arc::new(MemoryDisks::default());
        let (_, after) = lease_ending(&garbled, &records, Some(r#"{"end":"kept"}"#)).await;
        let after: serde_json::Value = serde_json::from_str(&after).unwrap();
        assert_eq!(after["ok"], false, "{after}");
        assert_eq!(after["code"], "invalid-request", "{after}");
        assert_eq!(garbled.counts(), (1, 1, 0));
        assert!(records.left().expect("records").is_empty());
        std::fs::remove_dir_all(root).expect("remove root");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_startup_scan_detaches_the_leases_a_previous_gateway_held() {
        let (root, records) = lease_records();
        let disks = Arc::new(MemoryDisks::default());
        // The previous gateway attached and recorded the disk, then stopped holding it.
        let lease = disks
            .attach(RamDiskSectors::new(std::num::NonZeroU64::new(16).unwrap()))
            .unwrap();
        records.record(&lease).expect("record");
        let served: Arc<dyn RamDisks> = disks.clone();
        crate::ram_lease::sweep(Some(served), &records, records.left())
            .expect("a lease to sweep")
            .await
            .expect("the sweep");
        assert_eq!(
            disks.counts(),
            (1, 1, 0),
            "the previous gateway's disk is detached"
        );
        assert!(
            records.left().expect("records").is_empty(),
            "and its record removed"
        );
        let quiet: Arc<dyn RamDisks> = disks.clone();
        assert!(crate::ram_lease::sweep(Some(quiet), &records, records.left()).is_none());
        std::fs::remove_dir_all(root).expect("remove root");
    }

    fn disk7() -> (RamDiskDevice, RamDiskEntry) {
        (
            RamDiskDevice::parse("/dev/disk7").expect("a whole disk"),
            RamDiskEntry::parse("0x100000abc").expect("an entry"),
        )
    }

    #[tokio::test]
    async fn a_ram_detach_answer_is_one_typed_outcome_on_the_wire() {
        let (device, entry) = disk7();
        let unserved = detach_ram_disk(None, device.clone(), entry).await;
        assert_eq!(
            serde_json::to_value(&unserved).expect("encode"),
            serde_json::json!({
                "ok": true,
                "ramDetach": {
                    "outcome": "not-started",
                    "reason": "this gateway detaches no RAM disk: its daemon supplied no disk runner",
                },
            })
        );
        let lost = detach_ram_disk(Some(Arc::new(PanickingDetach)), device, entry).await;
        assert!(
            matches!(&lost.ram_detach, Some(RamDetached::Unknown { reason }) if reason.contains("after it started")),
            "a thread lost after it started is unknown, never terminal or not-started: {:?}",
            serde_json::to_value(&lost)
        );
        let terminal = RamDetached::terminal(
            cowshed_gateway_types::RamDetachStatus::Signal(9),
            b"out",
            b"err",
            cowshed_core::api::dto::MAX_INLINE_OUTPUT_BYTES,
        );
        let wire = serde_json::json!({
            "outcome": "terminal",
            "status": {"signal": 9},
            "stdout": "out",
            "stderr": "err",
            "stdoutTruncated": false,
            "stderrTruncated": false,
        });
        assert_eq!(serde_json::to_value(&terminal).expect("encode"), wire);
        assert_eq!(
            serde_json::from_value::<RamDetached>(wire).expect("decode"),
            terminal
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_dispatched_ram_detach_outlives_the_connection_that_asked_for_it() {
        let (started, has_started) = std::sync::mpsc::sync_channel(1);
        let (release, released) = std::sync::mpsc::sync_channel(1);
        let (ended, has_ended) = std::sync::mpsc::sync_channel(1);
        let disks: Arc<dyn RamDisks> = Arc::new(HeldDetach {
            started: std::sync::Mutex::new(started),
            release: std::sync::Mutex::new(released),
            ended: std::sync::Mutex::new(ended),
        });
        let (device, entry) = disk7();
        let asking = tokio::spawn(detach_ram_disk(Some(disks), device, entry));
        tokio::task::spawn_blocking(move || has_started.recv())
            .await
            .expect("the start waiter")
            .expect("the detach started");
        // The requester's connection task goes away while the detach is in flight.
        asking.abort();
        assert!(matches!(asking.await, Err(error) if error.is_cancelled()));
        release
            .send(())
            .expect("the detach still waits for its release");
        let detached = tokio::task::spawn_blocking(move || has_ended.recv())
            .await
            .expect("the end waiter")
            .expect("the detach ran to its end with no one left to answer");
        assert_eq!(
            detached,
            RamDetached::Terminal {
                status: cowshed_gateway_types::RamDetachStatus::Exit(0),
                stdout: "\"disk7\" ejected.\n".to_owned(),
                stderr: String::new(),
                stdout_truncated: false,
                stderr_truncated: false,
            }
        );
    }
}
