use std::{
    collections::BTreeSet,
    fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
    sync::Arc,
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

use cowshed_os::deadline::{self, Peer, PeerStream, WaitError};

use cowshed_gateway_types::{
    DISK_LEASE_OP, DaemonStatus, DiskClass, EgressGrant, EgressMode, FIRST_LAUNCH_OP, FirstLaunch,
    FirstLaunchProbe, HostPattern, LeaseClass, LeaseState, MirrorRoute, PhaseShare, RAM_DETACH_OP,
    RAM_LEASE_OP, RamAttachRefusal, RamDetached, RamDiskAttached, RamDiskDevice, RamDiskEntry,
    RamDiskSectors, RamDisks, TASK_ADMIT_OP, TaskClass, TaskGrant, TaskOutcome, TaskQueueStatus,
    TaskRunCaps, WorkspaceCa, WorkspaceEndpoint, WorkspacePolicy, WorkspaceSession, WorkspaceToken,
};

use crate::{
    actor::{GatewayError, GatewayHandle},
    config::{CONTROL_TCP_ADDR, StartupProbe},
    disk_lease::{Claim, DiskLeases},
    interfaces::AuditEvent,
    repo_mirror::{MirrorInfo, RepoMirrorError, RepoMirrorHandle, RepoMirrorRequest},
    sim_broker::{
        SimBrokerError, SimBrokerHandle, SimDevice, SimInstallApproval, SimProjectConfig,
    },
    telemetry::{AuditTailHandle, AuditTailQuery},
};

const MAX_CONTROL_MESSAGE: u64 = 1024 * 1024;
/// The longest unit or owner name a `task-admit` request may carry; a longer one is refused.
const MAX_NAME: usize = 4096;
const MAX_AUDIT_TAIL_LIMIT: usize = 1024;
const CONTROLLER_CREDENTIAL_BYTES: usize = 32;
const CONTROLLER_CREDENTIAL_PREFIX: &str = "cctl1_";

trait ControlIo: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T> ControlIo for T where T: AsyncRead + AsyncWrite + Send + Unpin {}
type BoxControlIo = Box<dyn ControlIo>;
/// A control connection whose request is sent, read for its answer within its bound.
type ControlReader = BufReader<PeerStream<BoxControlIo>>;
/// The process a control client waits on, as its failures name it.
const GATEWAY: &str = "gateway daemon";
/// The silence a client allows a gateway before its first heartbeat line. A gateway of an earlier
/// build never heartbeats, and during an install a client talks to one: its slow answers
/// (`first-launch`, `ram-detach`) still arrive within the minute doctor always gave it. Once a
/// heartbeat has come, the peer has proved it heartbeats and [`deadline::HEARTBEAT_SILENCE`]
/// applies.
const UNPROVEN_SILENCE: std::time::Duration = std::time::Duration::from_secs(60);

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

    /// Every repository's land turns (05_gateway.md "Land turns"): its tickets in turn order, the
    /// holder first, and the tickets abandoned at or after `abandoned_since_unix_ms`.
    pub async fn land_turns(
        &self,
        abandoned_since_unix_ms: u64,
    ) -> Result<Vec<cowshed_core::api::dto::LandTurnQueue>, ControlError> {
        self.send(&ControlRequest::LandTurns {
            abandoned_since_unix_ms,
        })
        .await?
        .land_turns
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

    /// A first-launch measurement: the daemon makes it by two timed launches, heartbeating while
    /// it does.
    pub async fn first_launch(&self) -> Result<FirstLaunch, ControlError> {
        self.send(&ControlRequest::FirstLaunch)
            .await?
            .first_launch
            .ok_or(ControlError::InvalidResponse)
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
        let mut reader = self.ask(&request).await?;
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
                    // The gateway ended the follow: the stream is over.
                    Err(ControlError::Wait(WaitError::PeerLost { .. })) => return,
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
        read_answer_line(&mut self.ask(request).await?).await
    }

    /// Send `request` (05_gateway.md, "Deadlines"): connected, written and half-closed within the
    /// control bound, then the connection held for its answer, which may take as long as its work
    /// while the daemon heartbeats.
    async fn ask(&self, request: &ControlRequest) -> Result<ControlReader, ControlError> {
        let op = request.op();
        let (mut stream, encoded) = self.connect_and_encode(request, op).await?;
        let sent = async {
            stream.write_all(&encoded).await?;
            stream.shutdown().await
        }
        .await;
        sent.map_err(|error| ControlError::Wait(stream.fail(error)))?;
        stream.hold(deadline::idle(op, UNPROVEN_SILENCE));
        Ok(BufReader::new(stream))
    }

    async fn connect_and_encode(
        &self,
        request: &ControlRequest,
        op: &'static str,
    ) -> Result<(PeerStream<BoxControlIo>, Zeroizing<Vec<u8>>), ControlError> {
        let budget = deadline::start(op, deadline::CONTROL);
        let (stream, mut encoded) = match &self.endpoint {
            ControlEndpoint::Unix(socket) => {
                let encoded = serde_json::to_vec(request)
                    .map_err(|error| ControlError::Encoding(error.to_string()))?;
                let peer = Peer::unix(GATEWAY, socket);
                let connected = deadline::connect_unix(&budget, &peer, socket).await?;
                let peer = peer.credited(&connected);
                (
                    PeerStream::connected(Box::new(connected) as BoxControlIo, peer, budget),
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
                let peer = Peer::tcp(GATEWAY, *address);
                let connected =
                    deadline::establish(&budget, &peer, TcpStream::connect(address)).await?;
                (
                    PeerStream::connected(Box::new(connected) as BoxControlIo, peer, budget),
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
    /// One unit's terminal outcome: the fact its dependents read.
    TaskTerminal {
        unit: String,
        outcome: TaskOutcome,
    },
    /// Every repository's land turns, and the abandonments at or after a time.
    LandTurns {
        #[serde(rename = "abandonedSinceUnixMs")]
        abandoned_since_unix_ms: u64,
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
    /// Nudge the cleanup to list the store again (05_gateway.md, "Deleting released images").
    Released,
}

impl ControlRequest {
    /// The request as a wait on its answer names it: its wire `op`.
    const fn op(&self) -> &'static str {
        match self {
            Self::SessionCurrent { .. } => "session-current",
            Self::Status => "status",
            Self::Install { .. } => "install",
            Self::Remove { .. } => "remove",
            Self::AuditTail { .. } => "audit-tail",
            Self::RepoMirror { .. } => "repo-mirror",
            Self::SimConfigure { .. } => "sim-configure",
            Self::SimApprove { .. } => "sim-approve",
            Self::SimList { .. } => "sim-list",
            Self::SimBoot { .. } => "sim-boot",
            Self::TaskTerminal { .. } => "task-terminal",
            Self::TaskQueue => "task-queue",
            Self::NxCacheClear { .. } => "nx-cache-clear",
            Self::RamDetach { .. } => RAM_DETACH_OP,
            Self::FirstLaunch => FIRST_LAUNCH_OP,
            Self::Released => cowshed_gateway_types::RELEASED_OP,
            Self::LandTurns { .. } => cowshed_core::land_turn::LAND_TURNS_OP,
        }
    }
}

/// A request as the control socket reads it: a one-shot [`ControlRequest`] answered and closed,
/// or a RAM disk, disk or task-unit lease, held for as long as its client keeps the connection open.
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
    /// A land's place in its repository's queue, held for as long as its connection is open
    /// (05_gateway.md "Land turns").
    LandTurn {
        ask: cowshed_core::land_turn::TurnAsk,
    },
    TaskAdmit {
        unit: String,
        owner: String,
        incarnation: Option<String>,
        deps: Vec<String>,
        leases: Vec<LeaseClass>,
        priority: u32,
        urgent: bool,
        class: TaskClass,
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

/// A `task-admit` request's fields, read the way [`DiskLeaseIn`] is; `deadline` is an absolute
/// cache-domain deadline in epoch milliseconds.
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
    priority: u32,
    #[serde(default)]
    urgent: bool,
    #[serde(default)]
    class: TaskClass,
    #[serde(default)]
    deadline: Option<u64>,
    #[serde(default)]
    caps: Option<TaskRunCaps>,
    command: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct AuthenticatedControlRequestIn {
    controller_credential: String,
    request: serde_json::Value,
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
    /// What a selected task-server unit runs under.
    #[serde(skip_serializing_if = "Option::is_none")]
    task_grant: Option<TaskGrant>,
    /// The task server's queue fold.
    #[serde(skip_serializing_if = "Option::is_none")]
    task_queue: Option<TaskQueueStatus>,
    /// What an `nx-cache-clear` deleted.
    #[serde(skip_serializing_if = "Option::is_none")]
    nx_cache_cleared: Option<cowshed_core::api::NxStoreClearResult>,
    /// Every repository's land turns, as `land-turns` lists them.
    #[serde(skip_serializing_if = "Option::is_none")]
    land_turns: Option<Vec<cowshed_core::api::dto::LandTurnQueue>>,
    /// The whole disk a `ram-lease` attached and the registry entry it registers as.
    #[serde(skip_serializing_if = "Option::is_none")]
    ram_disk: Option<RamDiskAttached>,
    /// What is known of a `ram-detach`: `terminal`, `not-started` or `unknown`.
    #[serde(skip_serializing_if = "Option::is_none")]
    ram_detach: Option<RamDetached>,
    /// The measurement a `first-launch` made: the first and second launch of a fresh executable.
    #[serde(skip_serializing_if = "Option::is_none")]
    first_launch: Option<FirstLaunch>,
    /// A heartbeat's sequence number, on a line that answers nothing yet (05_gateway.md,
    /// "Deadlines").
    #[serde(skip_serializing_if = "Option::is_none")]
    progress: Option<u64>,
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
    pub land_turns: crate::land_turn::LandTurns,
    pub task_server: crate::task_server::TaskServer,
    pub ram_disks: Option<Arc<dyn RamDisks>>,
    pub ram_leases: crate::ram_lease::Records,
    pub first_launch: Option<Arc<dyn FirstLaunchProbe>>,
    /// The released-image cleanup a `released` request nudges, once its handle is published.
    pub cleanup: Arc<std::sync::OnceLock<crate::cleanup::ImageCleanup>>,
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
            .field("land_turns", &self.land_turns)
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
    // The descriptor stays open while either half lives, which is for as long as `serve` runs.
    let socket = std::os::fd::AsRawFd::as_raw_fd(&stream);
    let (reader, writer) = stream.into_split();
    serve(
        reader,
        writer,
        pid,
        Some(socket),
        parse_control_request,
        services,
    )
    .await;
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
    serve(reader, writer, None, None, parse, services).await;
}

/// Read the connection's one request line and answer it. A one-shot request must be followed by
/// the client's EOF, as it always was; a disk lease is held until that EOF comes.
async fn serve<R, W>(
    mut reader: R,
    writer: W,
    pid: Option<i32>,
    peer_socket: Option<std::os::fd::RawFd>,
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
        Ok(Incoming::TaskAdmit {
            unit,
            owner,
            incarnation,
            deps,
            leases,
            priority,
            urgent,
            class,
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
                priority,
                urgent,
                class,
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
        Ok(Incoming::LandTurn { ask }) => {
            // The asker is the process the kernel names at the connection's other end; a TCP
            // client names none, and is refused.
            let asker = peer_socket.and_then(|socket| {
                cowshed_core::runtime::job_groups::Process::of_socket_peer(&socket)
                    .map_err(|error| {
                        cowshed_os::eprintln!(
                            "cowshed: a land turn's asking process cannot be named: {error}"
                        );
                    })
                    .ok()
            });
            crate::land_turn::hold(reader, writer, asker, ask, services.land_turns.clone()).await;
        }
        Ok(Incoming::Request(request)) => {
            let request = expect_eof(&mut reader).await.map(|()| request);
            serve_request(writer, request, services).await;
        }
        Err(error) => serve_request(writer, Err(error), services).await,
    }
}

/// Hold one lease for this connection — a disk lease or a task unit — whose stake is `stake`:
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
        cowshed_core::land_turn::LAND_TURNS_OP => &["op", "abandonedSinceUnixMs"],
        cowshed_core::land_turn::LAND_TURN_OP => &["op", "repo", "workspace", "command"],
        "task-terminal" => &["op", "unit", "outcome"],
        "task-queue" => &["op"],
        "nx-cache-clear" => &["op", "repoId"],
        cowshed_gateway_types::RELEASED_OP => &["op"],
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
            "priority",
            "urgent",
            "class",
            "deadline",
            "caps",
            "command",
        ],
        DISK_LEASE_OP => &["op", "class", "share", "command"],
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
    if op == cowshed_core::land_turn::LAND_TURN_OP {
        #[derive(Deserialize)]
        struct LandTurnIn {
            repo: cowshed_core::api::dto::LandTurnName,
            workspace: String,
            command: String,
        }
        let LandTurnIn {
            repo,
            workspace,
            command,
        } = serde_json::from_value(value).map_err(encoding)?;
        return Ok(Incoming::LandTurn {
            ask: cowshed_core::land_turn::TurnAsk {
                repo,
                workspace,
                command,
            },
        });
    }
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
    if op == TASK_ADMIT_OP {
        let TaskAdmitIn {
            unit,
            owner,
            incarnation,
            deps,
            leases,
            priority,
            urgent,
            class,
            deadline,
            caps,
            command,
        } = serde_json::from_value(value).map_err(encoding)?;
        for named in [&unit, &owner] {
            if named.is_empty() || named.len() > MAX_NAME {
                return Err(ControlError::Encoding(format!(
                    "a task-admit unit and owner must name 1 to {MAX_NAME} bytes"
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
            priority,
            urgent,
            class,
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
        // `land-turns` answers from the queues at once, so it is never heartbeated.
        Ok(request @ ControlRequest::LandTurns { .. }) => dispatch(request, &services).await,
        Ok(request) => answered(&mut writer, &services.handle, dispatch(request, &services)).await,
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
        // A follow waits for events as long as none happen: the heartbeat says it still serves.
        let mut heartbeat = Heartbeat::new(&services.handle);
        loop {
            let event = tokio::select! {
                biased;
                event = receiver.recv() => event,
                () = heartbeat.due() => {
                    if heartbeat.beat(&mut writer).await.is_err() {
                        break;
                    }
                    continue;
                }
            };
            let Some(event) = event else {
                break;
            };
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

/// The heartbeat of one control connection (05_gateway.md, "Deadlines"): every
/// [`deadline::HEARTBEAT`], a `{"ok":true,"progress":n}` line, written once the actor answers a
/// ping, so an actor that stopped turning stops the heartbeat and the client's silence bound
/// names the daemon.
struct Heartbeat<'a> {
    handle: &'a GatewayHandle,
    beats: tokio::time::Interval,
    seq: u64,
}

impl<'a> Heartbeat<'a> {
    fn new(handle: &'a GatewayHandle) -> Self {
        Self {
            handle,
            beats: deadline::beats(),
            seq: 0,
        }
    }

    async fn due(&mut self) {
        self.beats.tick().await;
    }

    /// Whether the actor answers a ping. One that does not is said here, and no heartbeat goes
    /// out for it. Cancelling this writes nothing.
    async fn alive(&self) -> bool {
        match self.handle.ping().await {
            Ok(()) => true,
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: a control answer's heartbeat could not ping the gateway actor: {error}"
                );
                false
            }
        }
    }

    /// Write the next heartbeat line, whole: a line is never left half written for the answer
    /// to follow. Only a failed write is an error: the client is gone.
    async fn write<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> Result<(), ControlError> {
        self.seq += 1;
        let response = ControlResponse {
            progress: Some(self.seq),
            ..success()
        };
        write_response(writer, &response).await
    }

    /// Ping, and write a heartbeat when the actor answered.
    async fn beat<W: AsyncWrite + Unpin>(&mut self, writer: &mut W) -> Result<(), ControlError> {
        if self.alive().await {
            self.write(writer).await
        } else {
            Ok(())
        }
    }
}

/// Answer with `work`, heartbeating until it is done. The work is never dropped for a heartbeat:
/// a ping still pending runs beside it, and once a heartbeat cannot be written (the client left)
/// the work runs to its end unheartbeated, as an unanswered request always did.
async fn answered<T, W: AsyncWrite + Unpin>(
    writer: &mut W,
    handle: &GatewayHandle,
    work: impl Future<Output = T>,
) -> T {
    tokio::pin!(work);
    let mut heartbeat = Heartbeat::new(handle);
    loop {
        tokio::select! {
            biased;
            answer = &mut work => return answer,
            () = heartbeat.due() => {}
        }
        // Only the ping races the answer; a heartbeat once begun is written whole.
        let alive = tokio::select! {
            biased;
            answer = &mut work => return answer,
            alive = heartbeat.alive() => alive,
        };
        if alive && heartbeat.write(writer).await.is_err() {
            return work.await;
        }
    }
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
        ControlRequest::LandTurns {
            abandoned_since_unix_ms,
        } => (
            match services.land_turns.queues(abandoned_since_unix_ms).await {
                Some(queues) => ControlResponse {
                    land_turns: Some(queues),
                    ..success()
                },
                None => failure(
                    ControlFailureCode::Rejected,
                    "the gateway's land turns have stopped".to_owned(),
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
        ControlRequest::Released => (
            match services.cleanup.get() {
                None => failure(
                    ControlFailureCode::Rejected,
                    "the released-image cleanup has not started; its first listing finds the entry"
                        .to_owned(),
                ),
                Some(cleanup) => match cleanup.nudge() {
                    Ok(()) => success(),
                    Err(error) => failure(ControlFailureCode::Rejected, error),
                },
            },
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
                Refused::Attach(refusal @ RamAttachRefusal::Failed(_)) => failure(
                    ControlFailureCode::Rejected,
                    format!(
                        "{refusal}; a disk it may have made is left to the next gateway's \\
                         startup scan"
                    ),
                ),
                Refused::Unintended(error) => failure(
                    ControlFailureCode::Rejected,
                    format!(
                        "the gateway could not record the intent of a RAM disk attach ({error}); \\
                         nothing was attached"
                    ),
                ),
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
        ram_lease::detach(&*detacher, &detaching, &ended, &said).0
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

async fn read_response_line(reader: &mut ControlReader) -> Result<ControlResponse, ControlError> {
    read_answer_line(reader).await
}

/// A heartbeat line the daemon writes while an answer is not ready (05_gateway.md,
/// "Deadlines"). Read before the answer's own type, which never carries `progress`.
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProgressLine {
    #[serde(rename = "ok")]
    _ok: bool,
    #[serde(rename = "progress")]
    _progress: u64,
}

/// The next answer line of at most [`MAX_CONTROL_MESSAGE`] bytes, read as `T`, past the heartbeat
/// lines before it. The connection's silence bound decides how long the daemon may write none.
async fn read_answer_line<T>(reader: &mut ControlReader) -> Result<T, ControlError>
where
    T: serde::de::DeserializeOwned,
{
    loop {
        let mut bytes = Vec::new();
        let read = (&mut *reader)
            .take(MAX_CONTROL_MESSAGE + 1)
            .read_until(b'\n', &mut bytes)
            .await;
        read.map_err(|error| ControlError::Wait(reader.get_ref().fail(error)))?;
        if bytes.is_empty() {
            return Err(ControlError::Wait(reader.get_ref().lost()));
        }
        if bytes.len() > MAX_CONTROL_MESSAGE as usize || !bytes.ends_with(b"\n") {
            return Err(ControlError::InvalidResponse);
        }
        if serde_json::from_slice::<ProgressLine>(&bytes).is_ok() {
            reader.get_mut().silence(deadline::HEARTBEAT_SILENCE);
            continue;
        }
        return serde_json::from_slice(&bytes)
            .map_err(|error| ControlError::Encoding(error.to_string()));
    }
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
        task_grant: None,
        task_queue: None,
        nx_cache_cleared: None,
        land_turns: None,
        ram_disk: None,
        ram_detach: None,
        first_launch: None,
        progress: None,
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
    #[error("gateway control rejected operation ({code:?}): {message}")]
    Rejected {
        code: ControlFailureCode,
        message: String,
    },
    #[error("gateway control I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    Wait(#[from] WaitError),
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

    /// A daemon that takes the request and never answers, not even a heartbeat, is named
    /// `SILENT` once the silence a peer that has not shown it heartbeats is allowed elapses; that
    /// elapsing is this scenario's event.
    #[tokio::test(start_paused = true)]
    async fn a_daemon_that_takes_status_and_stays_silent_is_named_silent() {
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
        let client = GatewayControlClient::new(socket).expect("client");
        let refused = client.status().await;
        taken.send(()).expect("release the silent daemon");
        assert!(
            matches!(
                &refused,
                Err(ControlError::Wait(WaitError::Silent(silent)))
                    if silent.op == "status" && silent.silent_for == UNPROVEN_SILENCE
            ),
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

        fn registered(&self, _entry: RamDiskEntry) -> std::io::Result<bool> {
            Ok(false)
        }

        fn unmounted(&self, _sectors: RamDiskSectors) -> std::io::Result<Vec<RamDiskAttached>> {
            Ok(Vec::new())
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

        fn registered(&self, _entry: RamDiskEntry) -> std::io::Result<bool> {
            Ok(false)
        }

        fn unmounted(&self, _sectors: RamDiskSectors) -> std::io::Result<Vec<RamDiskAttached>> {
            Ok(Vec::new())
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

        fn registered(&self, entry: RamDiskEntry) -> std::io::Result<bool> {
            Ok(self.registered.lock().unwrap().contains(&entry))
        }

        /// Every disk it holds counts as mounted nowhere: a test names what it leaves.
        fn unmounted(&self, _sectors: RamDiskSectors) -> std::io::Result<Vec<RamDiskAttached>> {
            Ok(self
                .registered
                .lock()
                .unwrap()
                .iter()
                .map(|entry| RamDiskAttached {
                    device: Self::disk9().device,
                    registry_entry: *entry,
                })
                .collect())
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
        crate::ram_lease::sweep(Some(served), &records)
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
        assert!(crate::ram_lease::sweep(Some(quiet), &records).is_none());
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn the_startup_scan_detaches_a_disk_whose_attach_a_previous_gateway_never_answered() {
        let (root, records) = lease_records();
        let disks = Arc::new(MemoryDisks::default());
        let sectors = RamDiskSectors::new(std::num::NonZeroU64::new(16).unwrap());
        // The previous gateway recorded its intent and its attach made a disk, but it stopped
        // before it recorded the lease: only the intent names the attach.
        drop(records.intend(sectors).expect("intent"));
        disks.attach(sectors).unwrap();
        let served: Arc<dyn RamDisks> = disks.clone();
        crate::ram_lease::sweep(Some(served), &records)
            .expect("an intent to sweep")
            .await
            .expect("the sweep");
        assert_eq!(
            disks.counts(),
            (1, 1, 0),
            "the unanswered attach's disk is detached"
        );
        assert!(
            records.left().expect("records").is_empty(),
            "and its intent removed"
        );
        std::fs::remove_dir_all(root).expect("remove root");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_ram_lease_whose_intent_cannot_be_recorded_attaches_nothing() {
        let root = std::env::temp_dir().join(format!("cowshed-ram-lease-{}", Uuid::new_v4()));
        std::fs::create_dir(&root).expect("root");
        // The records' directory is a file: no intent can be written there.
        std::fs::write(root.join("ram-leases"), b"").expect("a file in the way");
        let records = crate::ram_lease::Records::beside(&root.join("gateway.sock"));
        let disks = Arc::new(MemoryDisks::default());
        let (client, server) = tokio::io::duplex(64 * 1024);
        let (reader, writer) = tokio::io::split(server);
        let served: Arc<dyn RamDisks> = disks.clone();
        hold_ram_lease(
            reader,
            writer,
            RamDiskSectors::new(std::num::NonZeroU64::new(16).unwrap()),
            Some(served),
            records,
        )
        .await;
        let mut answer = String::new();
        use tokio::io::AsyncReadExt as _;
        tokio::io::BufReader::new(client)
            .read_to_string(&mut answer)
            .await
            .expect("answer");
        let answer: serde_json::Value = serde_json::from_str(answer.trim()).expect("one line");
        assert_eq!(answer["ok"], false, "{answer}");
        assert_eq!(answer["code"], "rejected", "{answer}");
        assert_eq!(disks.counts(), (0, 0, 0), "nothing was attached");
        std::fs::remove_dir_all(root).expect("remove root");
    }
}
