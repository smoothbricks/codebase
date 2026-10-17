use std::{
    collections::{HashMap, VecDeque},
    fmt,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use cowshed_os::fork_lock::{self, Fenced};
use http::{Method, StatusCode};
use thiserror::Error;
use tokio::{
    net::{TcpListener, UnixListener},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::timeout,
};

use cowshed_gateway_types::{
    CanonicalHost, CanonicalTarget, ConfigError, DaemonStatus, EgressMode, MirrorProtocol,
    PolicyDenial, SessionStatus, TargetScheme, WorkspaceEndpoint, WorkspacePolicy,
    WorkspaceSession, WorkspaceToken,
};

use crate::{
    cache::{Cache, CacheError},
    config::{ControlTcpConfig, GatewayConfig, GatewayLimits},
    control::ControlError,
    interfaces::{
        AuditError, AuditEvent, AuditKind, AuditSink, AuditStatus, ConnectError, CredentialError,
        CredentialProvider, SystemConnector, UpstreamConnector,
    },
    mirror::{MirrorCacheStatus, MirrorService},
    nx_cache::{NxCacheBackend, NxPublication},
    proxy,
    repo_mirror::{Git2RepoTransport, RepoMirrorError, RepoMirrorHandle, RepoTransport},
    sim_broker::{SimBrokerError, SimBrokerHandle, SimRunner, XcrunSimRunner},
    telemetry::{ArrowAuditConfig, ArrowAuditSink, AuditTailHandle, validate_event_shape},
    tls::{CaSigner, LeafCache, TlsError},
};

#[cfg(target_os = "macos")]
use crate::platform::KeychainCredentialProvider;
#[cfg(target_os = "linux")]
use crate::platform::SystemdCredentialProvider;

#[derive(Clone)]
pub struct GatewayHandle {
    pub(crate) commands: mpsc::Sender<Command>,
}

impl fmt::Debug for GatewayHandle {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("GatewayHandle")
            .finish_non_exhaustive()
    }
}
#[derive(Clone, Debug)]
pub(crate) struct BrokerAuditEvent {
    pub workspace_id: String,
    pub kind: AuditKind,
    pub method: Option<String>,
    pub path: Option<String>,
    pub status: AuditStatus,
    pub classification: Option<String>,
    pub bytes: u64,
}

#[async_trait::async_trait]
pub(crate) trait BrokerAuditor: Send + Sync {
    async fn record_broker(&self, event: BrokerAuditEvent) -> Result<(), AuditError>;
}

impl GatewayHandle {
    pub async fn configure_nx_cache(&self, backend: NxCacheBackend) -> Result<(), GatewayError> {
        let (reply, receive) = oneshot::channel();
        self.send(Command::ConfigureNxCache { backend, reply })
            .await?;
        receive.await.map_err(|_| GatewayError::Stopped)?
    }

    pub async fn begin_nx_publication(
        &self,
        publication: NxPublication,
    ) -> Result<(), GatewayError> {
        let (reply, receive) = oneshot::channel();
        self.send(Command::BeginNxPublication { publication, reply })
            .await?;
        receive.await.map_err(|_| GatewayError::Stopped)?
    }

    pub async fn end_nx_publication(
        &self,
        workspace_id: String,
        token: WorkspaceToken,
    ) -> Result<(), GatewayError> {
        let (reply, receive) = oneshot::channel();
        self.send(Command::EndNxPublication {
            workspace_id,
            token,
            reply,
        })
        .await?;
        receive.await.map_err(|_| GatewayError::Stopped)?
    }

    pub async fn install(&self, session: WorkspaceSession) -> Result<(), GatewayError> {
        let (reply, receive) = oneshot::channel();
        self.send(Command::Install { session, reply }).await?;
        receive.await.map_err(|_| GatewayError::Stopped)?
    }

    pub async fn remove(
        &self,
        workspace_id: impl Into<String>,
        expected_revision: u64,
    ) -> Result<(), GatewayError> {
        let (reply, receive) = oneshot::channel();
        self.send(Command::Remove {
            workspace_id: workspace_id.into(),
            expected_revision,
            reply,
        })
        .await?;
        receive.await.map_err(|_| GatewayError::Stopped)?
    }

    pub async fn status(&self) -> Result<DaemonStatus, GatewayError> {
        let (reply, receive) = oneshot::channel();
        self.send(Command::Status { reply }).await?;
        receive.await.map_err(|_| GatewayError::Stopped)
    }

    async fn send(&self, command: Command) -> Result<(), GatewayError> {
        self.commands
            .send(command)
            .await
            .map_err(|_| GatewayError::Stopped)
    }
    async fn record_broker_event(&self, event: BrokerAuditEvent) -> Result<(), AuditError> {
        let (reply, receive) = oneshot::channel();
        self.send(Command::BrokerAudit { event, reply })
            .await
            .map_err(|error| AuditError(error.to_string()))?;
        receive
            .await
            .map_err(|_| AuditError("gateway actor stopped".to_owned()))?
    }
}

#[async_trait::async_trait]
impl BrokerAuditor for GatewayHandle {
    async fn record_broker(&self, event: BrokerAuditEvent) -> Result<(), AuditError> {
        self.record_broker_event(event).await
    }
}

/// Owning daemon object. Dropping it force-closes listeners and active streams.
pub struct Gateway {
    handle: GatewayHandle,
    actor: Option<JoinHandle<Result<(), GatewayError>>>,
    control: Option<ControlRuntime>,
    drain_timeout: Duration,
    repo_mirror: RepoMirrorHandle,
    sim_broker: SimBrokerHandle,
    repo_mirror_task: Option<JoinHandle<()>>,
    sim_broker_task: Option<JoinHandle<()>>,
}

impl Gateway {
    /// Starts the production host daemon with the platform credential provider,
    /// platform-verifying upstream TLS, durable Arrow telemetry, and native
    /// repository/simulator broker adapters.
    pub async fn start_host(
        config: GatewayConfig,
        telemetry: ArrowAuditConfig,
    ) -> Result<Self, GatewayError> {
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            let _ = (config, telemetry);
            return Err(GatewayError::UnsupportedProductionPlatform);
        }
        #[cfg(any(target_os = "macos", target_os = "linux"))]
        {
            config.validate()?;
            config.validate_host_cache_layout()?;
            let connector: Arc<dyn UpstreamConnector> = Arc::new(SystemConnector::new(
                config.timeouts.connect,
                config.timeouts.tls_handshake,
            )?);
            let audit = ArrowAuditSink::start(telemetry)?;
            #[cfg(target_os = "macos")]
            let credentials: Arc<dyn CredentialProvider> =
                Arc::new(KeychainCredentialProvider::new());
            #[cfg(target_os = "linux")]
            let credentials: Arc<dyn CredentialProvider> =
                Arc::new(SystemdCredentialProvider::from_environment()?);
            let tail = audit.tail_handle();
            let repo_transport: Arc<dyn RepoTransport> = Arc::new(Git2RepoTransport::new(
                config.git_helper_executable.clone(),
                Arc::clone(&connector),
                config.timeouts.connect,
                config.timeouts.response_headers,
            ));
            Self::start_runtime(
                config,
                credentials,
                connector,
                Arc::new(audit),
                repo_transport,
                Arc::new(XcrunSimRunner),
                Some(tail),
            )
            .await
        }
    }

    pub async fn start(
        config: GatewayConfig,
        credentials: Arc<dyn CredentialProvider>,
        connector: Arc<dyn UpstreamConnector>,
        audit: Arc<dyn AuditSink>,
    ) -> Result<Self, GatewayError> {
        let repo_transport: Arc<dyn RepoTransport> = Arc::new(Git2RepoTransport::new(
            config.git_helper_executable.clone(),
            Arc::clone(&connector),
            config.timeouts.connect,
            config.timeouts.response_headers,
        ));
        Self::start_runtime(
            config,
            credentials,
            connector,
            audit,
            repo_transport,
            Arc::new(XcrunSimRunner),
            None,
        )
        .await
    }

    pub async fn start_with_brokers(
        config: GatewayConfig,
        credentials: Arc<dyn CredentialProvider>,
        connector: Arc<dyn UpstreamConnector>,
        audit: Arc<dyn AuditSink>,
        repo_transport: Arc<dyn RepoTransport>,
        sim_runner: Arc<dyn SimRunner>,
    ) -> Result<Self, GatewayError> {
        Self::start_runtime(
            config,
            credentials,
            connector,
            audit,
            repo_transport,
            sim_runner,
            None,
        )
        .await
    }

    async fn start_runtime(
        config: GatewayConfig,
        credentials: Arc<dyn CredentialProvider>,
        connector: Arc<dyn UpstreamConnector>,
        audit: Arc<dyn AuditSink>,
        repo_transport: Arc<dyn RepoTransport>,
        sim_runner: Arc<dyn SimRunner>,
        audit_tail: Option<AuditTailHandle>,
    ) -> Result<Self, GatewayError> {
        config.validate()?;
        let mirror_service =
            MirrorService::new(Cache::open(config.mirror_cache.cache_config()).await?);
        let (completions, completion_receiver) =
            mpsc::channel(active_permit_capacity(&config.limits));
        let (cancellations, cancellation_receiver) = mpsc::channel(config.limits.global_queued);
        let (commands, receiver) = mpsc::channel(config.command_capacity.get());
        let handle = GatewayHandle { commands };
        let auditor: Arc<dyn BrokerAuditor> = Arc::new(handle.clone());
        let repo_root = config.mirror_cache.cache_root.join("repo");
        let (repo_mirror, repo_mirror_task) = RepoMirrorHandle::start(
            repo_root,
            config.command_capacity.get(),
            Arc::clone(&credentials),
            repo_transport,
            Arc::clone(&auditor),
            config.timeouts.response_headers,
            config.timeouts.request_total,
        )?;
        let (sim_broker, sim_broker_task) = match SimBrokerHandle::start(
            config.simulator_drop_root.clone(),
            config.command_capacity.get(),
            sim_runner,
            auditor,
        ) {
            Ok(runtime) => runtime,
            Err(error) => {
                repo_mirror.shutdown().await;
                let _ = repo_mirror_task.await;
                return Err(error.into());
            }
        };
        let state = Actor::new(
            config.clone(),
            ActorChannels {
                receiver,
                commands: handle.commands.clone(),
                completions,
                completion_receiver,
                cancellations,
                cancellation_receiver,
            },
            credentials,
            connector,
            audit,
            mirror_service,
            BrokerHandles {
                repo_mirror: repo_mirror.clone(),
                sim_broker: sim_broker.clone(),
            },
        );
        let actor = tokio::spawn(state.run());
        let cpu_budget = crate::cpu_budget::CpuBudget::start(config.cpu_budget);
        let control = match &config.control_socket {
            Some(path) => {
                match ControlRuntime::start(
                    path,
                    config.authorized_control_uid,
                    crate::control::ControlServices {
                        handle: handle.clone(),
                        repo_mirror: repo_mirror.clone(),
                        sim_broker: sim_broker.clone(),
                        audit_tail: audit_tail.clone(),
                        startup: config.startup.clone(),
                        disk_leases: crate::disk_lease::DiskLeases::start(config.disk_lease),
                        cpu_budget: cpu_budget.clone(),
                        task_server: crate::task_server::TaskServer::start(
                            config.task_server,
                            cpu_budget,
                        ),
                        jobserver: crate::jobserver::JobserverFifos::start(
                            path.parent()
                                .map(|parent| parent.join("jobserver"))
                                .unwrap_or_else(|| std::path::PathBuf::from("jobserver")),
                        ),
                    },
                    config.control_tcp.as_ref(),
                ) {
                    Ok(control) => Some(control),
                    Err(error) => {
                        let _ = handle.send(Command::ForceStop).await;
                        let _ = actor.await;
                        repo_mirror.shutdown().await;
                        sim_broker.shutdown().await;
                        let _ = repo_mirror_task.await;
                        let _ = sim_broker_task.await;
                        return Err(error);
                    }
                }
            }
            None => None,
        };
        Ok(Self {
            handle,
            actor: Some(actor),
            control,
            drain_timeout: config.timeouts.request_total,
            repo_mirror,
            sim_broker,
            repo_mirror_task: Some(repo_mirror_task),
            sim_broker_task: Some(sim_broker_task),
        })
    }

    pub fn handle(&self) -> GatewayHandle {
        self.handle.clone()
    }

    /// Resolves when the actor stops without being asked to — after an audit failure closed the
    /// gateway and its drain completed — with the error that stopped it. A daemon waits on this
    /// beside its shutdown signal so a gateway that can no longer serve exits and is restarted.
    /// Once it has resolved, the actor is gone and later calls answer [`GatewayError::Stopped`].
    pub async fn stopped(&mut self) -> Result<(), GatewayError> {
        let Some(actor) = self.actor.as_mut() else {
            return Err(GatewayError::Stopped);
        };
        let joined = actor.await;
        self.actor = None;
        joined.map_err(|error| GatewayError::Task(error.to_string()))?
    }

    pub async fn drain(mut self) -> Result<(), GatewayError> {
        if let Some(control) = self.control.take() {
            control.stop().await;
        }
        let (reply, receive) = oneshot::channel();
        self.handle.send(Command::BeginDrain { reply }).await?;
        if timeout(self.drain_timeout, receive).await.is_err() {
            let _ = self.handle.send(Command::ForceStop).await;
        }
        if let Some(actor) = self.actor.take() {
            actor
                .await
                .map_err(|error| GatewayError::Task(error.to_string()))??;
        }
        self.repo_mirror.shutdown().await;
        self.sim_broker.shutdown().await;
        if let Some(task) = self.repo_mirror_task.take() {
            let _ = task.await;
        }
        if let Some(task) = self.sim_broker_task.take() {
            let _ = task.await;
        }
        Ok(())
    }
}

impl Drop for Gateway {
    fn drop(&mut self) {
        let _ = self.handle.commands.try_send(Command::ForceStop);
        if let Some(actor) = &self.actor {
            actor.abort();
        }
        if let Some(control) = &self.control {
            for task in &control.tasks {
                task.abort();
            }
            let _ = std::fs::remove_file(&control.path);
        }
        if let Some(task) = &self.repo_mirror_task {
            task.abort();
        }
        if let Some(task) = &self.sim_broker_task {
            task.abort();
        }
    }
}

pub(crate) enum Command {
    ConfigureNxCache {
        backend: NxCacheBackend,
        reply: oneshot::Sender<Result<(), GatewayError>>,
    },
    NxBackendStarted {
        repo_id: String,
        result: Result<(NxCacheBackend, tokio::process::Child), String>,
    },
    BeginNxPublication {
        publication: NxPublication,
        reply: oneshot::Sender<Result<(), GatewayError>>,
    },
    EndNxPublication {
        workspace_id: String,
        token: WorkspaceToken,
        reply: oneshot::Sender<Result<(), GatewayError>>,
    },
    Install {
        session: WorkspaceSession,
        reply: oneshot::Sender<Result<(), GatewayError>>,
    },
    Remove {
        workspace_id: String,
        expected_revision: u64,
        reply: oneshot::Sender<Result<(), GatewayError>>,
    },
    Admit {
        workspace_id: String,
        authentication: Authentication,
        intent: RequestIntent,
        reply: oneshot::Sender<Result<Admission, AdmissionError>>,
        cancelled: oneshot::Receiver<()>,
    },
    AuditDenial {
        workspace_id: String,
        attempt: AuditAttempt,
        reply: oneshot::Sender<bool>,
    },
    BrokerAudit {
        event: BrokerAuditEvent,
        reply: oneshot::Sender<Result<(), AuditError>>,
    },
    QueueExpired {
        queue_id: u64,
    },
    MintLeaf {
        workspace_id: String,
        generation: u64,
        host: String,
        reply: oneshot::Sender<Result<Arc<rustls::ServerConfig>, GatewayError>>,
    },
    Status {
        reply: oneshot::Sender<DaemonStatus>,
    },
    BeginDrain {
        reply: oneshot::Sender<()>,
    },
    ForceStop,
}

#[derive(Clone, Debug)]
pub(crate) enum Authentication {
    Bearer(Option<String>),
    Generation(u64),
}

#[derive(Clone, Debug)]
pub(crate) enum RequestTarget {
    Generic(CanonicalTarget),
    LocalSim,
    LocalNx,
}

#[derive(Clone, Debug)]
pub(crate) struct RequestIntent {
    pub target: RequestTarget,
    pub method: Method,
    pub path: String,
    pub audit_kind: AuditKind,
    pub trace_id: Option<String>,
    pub npm_mirror: bool,
}

#[derive(Clone, Debug)]
pub(crate) struct AuditAttempt {
    pub kind: AuditKind,
    pub host: Option<String>,
    pub method: String,
    pub path: String,
    pub status: AuditStatus,
    pub http_status: StatusCode,
    pub classification: &'static str,
}

pub(crate) struct Admission {
    pub permit_id: u64,
    pub workspace_id: String,
    pub repo_id: String,
    pub revision: u64,
    pub endpoint: String,
    pub generation: u64,
    pub target: CanonicalTarget,
    pub mode: EgressMode,
    pub protocol: Option<MirrorProtocol>,
    pub credential_allowed: bool,
    pub private_network_authorized: bool,
    pub nx_token: Option<WorkspaceToken>,
    pub audit_kind: AuditKind,
    pub method: Method,
    pub request_path: String,
    pub upstream_path: String,
    pub trace_id: Option<String>,
    pub span_id: u64,
    pub trace_flags: u8,
    pub parent_span_id: Option<u64>,
    pub upstream_span_id: Option<u64>,
    pub tracestate: Option<String>,
    pub trace_classification: Option<String>,
    completion: Option<CompletionLease>,
}

#[derive(Clone, Debug)]
struct AdmissionSeed {
    workspace_id: String,
    repo_id: String,
    revision: u64,
    endpoint: String,
    generation: u64,
    target: CanonicalTarget,
    mode: EgressMode,
    protocol: Option<MirrorProtocol>,
    credential_allowed: bool,
    private_network_authorized: bool,
    nx_token: Option<WorkspaceToken>,
    audit_kind: AuditKind,
    method: Method,
    request_path: String,
    upstream_path: String,
    trace_id: Option<String>,
}

impl AdmissionSeed {
    fn activate(self, permit_id: u64, completion: CompletionLease) -> Admission {
        Admission {
            permit_id,
            workspace_id: self.workspace_id,
            repo_id: self.repo_id,
            revision: self.revision,
            endpoint: self.endpoint,
            generation: self.generation,
            target: self.target,
            mode: self.mode,
            protocol: self.protocol,
            credential_allowed: self.credential_allowed,
            private_network_authorized: self.private_network_authorized,
            nx_token: self.nx_token,
            audit_kind: self.audit_kind,
            method: self.method,
            request_path: self.request_path,
            upstream_path: self.upstream_path,
            trace_id: self.trace_id,
            span_id: (permit_id << 1) | 1,
            trace_flags: 1,
            parent_span_id: None,
            upstream_span_id: None,
            tracestate: None,
            trace_classification: None,
            completion: Some(completion),
        }
    }

    /// The pool this admission is charged to. Only an intercepted CONNECT is a tunnel: it
    /// terminates TLS locally and opens no upstream connection of its own. An opaque CONNECT
    /// is the upstream TCP connection, so it is origin-limited work like any request.
    fn class(&self) -> PermitClass {
        match self.mode {
            EgressMode::Intercept if self.method == Method::CONNECT => PermitClass::InterceptTunnel,
            EgressMode::Intercept | EgressMode::Opaque => PermitClass::Request {
                origin: self.target.origin(),
            },
        }
    }
}

pub(crate) struct CompletionMessage {
    permit_id: u64,
    draft: AuditDraft,
}

pub(crate) struct CompletionLease {
    permit: Option<mpsc::OwnedPermit<CompletionMessage>>,
    permit_id: u64,
    cancelled: Option<AuditDraft>,
}

impl CompletionLease {
    fn new(
        permit: mpsc::OwnedPermit<CompletionMessage>,
        permit_id: u64,
        cancelled: AuditDraft,
    ) -> Self {
        Self {
            permit: Some(permit),
            permit_id,
            cancelled: Some(cancelled),
        }
    }

    pub(crate) fn finish(mut self, draft: AuditDraft) {
        self.cancelled = None;
        if let Some(permit) = self.permit.take() {
            permit.send(CompletionMessage {
                permit_id: self.permit_id,
                draft,
            });
        }
    }
}

impl Drop for CompletionLease {
    fn drop(&mut self) {
        let (Some(permit), Some(draft)) = (self.permit.take(), self.cancelled.take()) else {
            return;
        };
        permit.send(CompletionMessage {
            permit_id: self.permit_id,
            draft,
        });
    }
}

impl Admission {
    pub(crate) fn finish(mut self, draft: AuditDraft) {
        if let Some(completion) = self.completion.take() {
            completion.finish(draft);
        }
    }

    pub(crate) fn take_completion(&mut self) -> CompletionLease {
        self.completion
            .take()
            .expect("admission completion lease is owned exactly once")
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AuditDraft {
    pub workspace_id: String,
    pub repo_id: String,
    pub revision: u64,
    pub endpoint: String,
    pub kind: AuditKind,
    pub host: Option<String>,
    pub method: Option<String>,
    pub path: Option<String>,
    pub status: AuditStatus,
    pub http_status: Option<u16>,
    pub bytes: u64,
    pub trace_id: Option<String>,
    pub span_id: u64,
    pub parent_span_id: Option<u64>,
    pub upstream_span_id: Option<u64>,
    pub tracestate: Option<String>,
    pub grant_hint: Option<String>,
    pub classification: Option<String>,
    pub mirror_cache_status: Option<MirrorCacheStatus>,
}

impl AuditDraft {
    fn into_event(self, sequence: u64) -> AuditEvent {
        AuditEvent {
            sequence,
            timestamp_unix_ms: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
            workspace_id: self.workspace_id,
            repo_id: self.repo_id,
            revision: self.revision,
            endpoint: self.endpoint,
            kind: self.kind,
            host: self.host,
            method: self.method,
            path: self.path,
            status: self.status,
            http_status: self.http_status,
            bytes: self.bytes,
            trace_id: self.trace_id,
            span_id: if self.span_id == 0 {
                sequence
            } else {
                self.span_id
            },
            parent_span_id: self.parent_span_id,
            upstream_span_id: self.upstream_span_id,
            tracestate: self.tracestate,
            grant_hint: self.grant_hint,
            classification: self.classification,
            mirror_cache_status: self.mirror_cache_status,
        }
    }
}

#[derive(Clone, Debug, Error)]
#[error("{message}")]
pub(crate) struct AdmissionError {
    pub status: StatusCode,
    pub message: &'static str,
    pub hint: Option<String>,
    pub audit_status: AuditStatus,
}

struct Pending {
    queue_id: u64,
    seed: AdmissionSeed,
    reply: oneshot::Sender<Result<Admission, AdmissionError>>,
    timer: Option<JoinHandle<()>>,
    cancellation: Option<JoinHandle<()>>,
    /// Set for a queued Nx publication, so ending that publication cancels exactly it.
    nx_put: bool,
}

/// A request waiting for its project's standalone Nx backend to start. It keeps the original
/// request rather than an admission seed, since the backend that seed needs does not exist yet.
/// It is charged to the same session and global queued counters as any queued request.
struct NxPending {
    queue_id: u64,
    workspace_id: String,
    repo_id: String,
    generation: u64,
    authentication: Authentication,
    intent: RequestIntent,
    reply: oneshot::Sender<Result<Admission, AdmissionError>>,
    timer: Option<JoinHandle<()>>,
    cancellation: Option<JoinHandle<()>>,
}

/// The reporting supervisor owns this worker even before its first poll.
struct NxStartWorker(JoinHandle<Result<(NxCacheBackend, tokio::process::Child), String>>);

impl Drop for NxStartWorker {
    fn drop(&mut self) {
        self.0.abort();
    }
}

fn supervise_nx_start(
    repo_id: String,
    commands: mpsc::Sender<Command>,
    worker: JoinHandle<Result<(NxCacheBackend, tokio::process::Child), String>>,
) -> JoinHandle<()> {
    let mut worker = NxStartWorker(worker);
    tokio::spawn(async move {
        let result = match (&mut worker.0).await {
            Ok(result) => result,
            Err(error) => Err(format!("Nx backend start task failed: {error}")),
        };
        let _ = commands
            .send(Command::NxBackendStarted { repo_id, result })
            .await;
    })
}

/// A project's standalone Nx cache backend, owned by the actor. `Starting` holds the one task
/// that reports its start; every removal aborts that task explicitly, because dropping a
/// `JoinHandle` does not abort it.
enum NxBackendState {
    Starting {
        task: JoinHandle<()>,
    },
    Ready {
        backend: NxCacheBackend,
        /// `None` only for a backend the host configured directly. A started server's child keeps
        /// its stdin lease, and its death closes that lease.
        process: Option<Box<tokio::process::Child>>,
    },
    Failed {
        reason: String,
        retry_after: std::time::Instant,
    },
}

/// One request's Nx backend admission: serve now, wait for the start, or refuse as an outage.
enum NxAdmission {
    Ready,
    Starting,
    Unavailable(NxRefusal),
}

/// Why an Nx request is refused before it is admitted: the audit status and classification, the
/// optional diagnostic, and the reply's message.
struct NxRefusal {
    status: AuditStatus,
    classification: &'static str,
    detail: Option<String>,
    message: &'static str,
}

impl NxRefusal {
    /// The refusal of a project backend that is down, cooling down, or failed to start.
    fn outage(reason: String) -> Self {
        Self {
            status: AuditStatus::Failed,
            classification: "nx-backend-unavailable",
            detail: Some(reason),
            message: "project Nx cache backend is unavailable",
        }
    }

    /// A full queue, whether the project's backend is starting or Ready. Stock Nx 23.2.1 retries
    /// no `GET` and reads any status but 200/404 as a fatal misconfiguration, so a busy gateway
    /// answers the method's miss or refusal (05_gateway.md "Busy gateway"), never 429.
    fn queue_limited() -> Self {
        Self {
            status: AuditStatus::Limited,
            classification: "nx-queue-limited",
            detail: None,
            message: "gateway queue is full",
        }
    }

    /// A startup-queue wait that reached its deadline: like a full queue, the method's miss or
    /// refusal, never 504.
    fn queue_timeout() -> Self {
        Self {
            status: AuditStatus::TimedOut,
            classification: "queue-timeout",
            detail: None,
            message: "gateway queue wait timed out",
        }
    }
}

/// Which bounded active pool a permit is charged to.
///
/// An intercepted CONNECT is a downstream TLS transport: every HTTP request it carries is
/// admitted separately as a [`PermitClass::Request`]. Charging the tunnel to the origin pool its
/// own inner requests need would let `origin_active` live tunnels starve every inner request
/// until the client gives up. Tunnels therefore draw from their own pool, bounded by the same
/// workspace and global limits, and never hold an origin slot; only requests carry one.
#[derive(Clone, Debug, PartialEq, Eq)]
enum PermitClass {
    InterceptTunnel,
    Request { origin: String },
}

/// Active permits per pool. Each pool is bounded independently by `workspace_active` and
/// `global_active`, so the active total is at most twice either limit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct ActiveCounts {
    tunnels: usize,
    requests: usize,
}

impl ActiveCounts {
    fn of(self, class: &PermitClass) -> usize {
        match class {
            PermitClass::InterceptTunnel => self.tunnels,
            PermitClass::Request { .. } => self.requests,
        }
    }

    fn slot(&mut self, class: &PermitClass) -> &mut usize {
        match class {
            PermitClass::InterceptTunnel => &mut self.tunnels,
            PermitClass::Request { .. } => &mut self.requests,
        }
    }

    fn charge(&mut self, class: &PermitClass) {
        *self.slot(class) += 1;
    }

    fn release(&mut self, class: &PermitClass) {
        let slot = self.slot(class);
        *slot = slot.saturating_sub(1);
    }

    fn total(self) -> usize {
        self.tunnels + self.requests
    }
}

/// Every active permit owns one reserved completion slot until the actor receives its
/// completion, so the channel holds both pools' global bounds.
/// [`GatewayLimits::validate`] keeps this product within the channel's permit range.
fn active_permit_capacity(limits: &GatewayLimits) -> usize {
    limits.global_active * 2
}

/// Whether one more permit of `class` fits its pool. `origin_active` is consulted only for
/// requests, the sole class holding an upstream origin slot.
fn pool_admits(
    limits: &GatewayLimits,
    session: ActiveCounts,
    global: ActiveCounts,
    class: &PermitClass,
    origin_active: impl FnOnce(&str) -> usize,
) -> bool {
    session.of(class) < limits.workspace_active
        && global.of(class) < limits.global_active
        && match class {
            PermitClass::InterceptTunnel => true,
            PermitClass::Request { origin } => origin_active(origin) < limits.origin_active,
        }
}

struct PermitState {
    workspace_id: String,
    class: PermitClass,
    generation: u64,
}

struct SessionState {
    repo_id: String,
    revision: u64,
    endpoint: WorkspaceEndpoint,
    endpoint_label: String,
    token: WorkspaceToken,
    policy: WorkspacePolicy,
    signer: CaSigner,
    generation: u64,
    active: ActiveCounts,
    queued: usize,
    accept_stop: watch::Sender<bool>,
    connection_stop: watch::Sender<bool>,
    audit_stop: watch::Sender<bool>,
    listener_task: JoinHandle<()>,
}

impl SessionState {
    async fn quiesce(&mut self) {
        let _ = self.accept_stop.send(true);
        let _ = self.connection_stop.send(true);
        let _ = self.audit_stop.send(true);
        let _ = (&mut self.listener_task).await;
        unlink_endpoint(&self.endpoint);
    }

    async fn stop(mut self) {
        self.quiesce().await;
    }
}

struct ActorChannels {
    receiver: mpsc::Receiver<Command>,
    commands: mpsc::Sender<Command>,
    completions: mpsc::Sender<CompletionMessage>,
    completion_receiver: mpsc::Receiver<CompletionMessage>,
    cancellations: mpsc::Sender<u64>,
    cancellation_receiver: mpsc::Receiver<u64>,
}
struct BrokerHandles {
    repo_mirror: RepoMirrorHandle,
    sim_broker: SimBrokerHandle,
}

struct Actor {
    config: GatewayConfig,
    receiver: mpsc::Receiver<Command>,
    commands: mpsc::Sender<Command>,
    completions: mpsc::Sender<CompletionMessage>,
    completion_receiver: mpsc::Receiver<CompletionMessage>,
    cancellations: mpsc::Sender<u64>,
    cancellation_receiver: mpsc::Receiver<u64>,
    credentials: Arc<dyn CredentialProvider>,
    connector: Arc<dyn UpstreamConnector>,
    audit: Arc<dyn AuditSink>,
    mirror_service: MirrorService,
    repo_mirror: RepoMirrorHandle,
    sim_broker: SimBrokerHandle,
    sessions: HashMap<String, SessionState>,
    nx_backends: HashMap<String, NxBackendState>,
    nx_publications: HashMap<String, NxPublication>,
    nx_queue: VecDeque<NxPending>,
    revisions: HashMap<String, u64>,
    permits: HashMap<u64, PermitState>,
    origins: HashMap<(String, String), usize>,
    queue: VecDeque<Pending>,
    global_active: ActiveCounts,
    global_queued: usize,
    next_generation: u64,
    next_permit: u64,
    next_queue: u64,
    next_audit: u64,
    leaf_cache: LeafCache,
    draining: bool,
    drain_reply: Option<oneshot::Sender<()>>,
    audit_failure: Option<AuditError>,
}

impl Actor {
    fn new(
        config: GatewayConfig,
        channels: ActorChannels,
        credentials: Arc<dyn CredentialProvider>,
        connector: Arc<dyn UpstreamConnector>,
        audit: Arc<dyn AuditSink>,
        mirror_service: MirrorService,
        brokers: BrokerHandles,
    ) -> Self {
        let ActorChannels {
            receiver,
            commands,
            completions,
            completion_receiver,
            cancellations,
            cancellation_receiver,
        } = channels;
        let BrokerHandles {
            repo_mirror,
            sim_broker,
        } = brokers;
        Self {
            leaf_cache: LeafCache::new(config.limits, config.timeouts),
            commands,
            completions,
            completion_receiver,
            cancellations,
            cancellation_receiver,
            config,
            receiver,
            credentials,
            connector,
            audit,
            mirror_service,
            repo_mirror,
            sim_broker,
            sessions: HashMap::new(),
            nx_backends: HashMap::new(),
            nx_publications: HashMap::new(),
            nx_queue: VecDeque::new(),
            revisions: HashMap::new(),
            permits: HashMap::new(),
            origins: HashMap::new(),
            queue: VecDeque::new(),
            global_active: ActiveCounts::default(),
            global_queued: 0,
            next_generation: 1,
            next_permit: 1,
            next_queue: 1,
            next_audit: 1,
            draining: false,
            drain_reply: None,
            audit_failure: None,
        }
    }

    async fn run(mut self) -> Result<(), GatewayError> {
        loop {
            tokio::select! {
                Some(completion) = self.completion_receiver.recv() => {
                    self.record(completion.draft).await;
                    self.complete(completion.permit_id);
                }
                Some(queue_id) = self.cancellation_receiver.recv() => {
                    self.cancel_queued_request(queue_id).await;
                }
                command = self.receiver.recv() => {
                    let Some(command) = command else {
                        self.force_stop().await?;
                        return Ok(());
                    };
                    match command {
                        Command::ConfigureNxCache { backend, reply } => {
                            let _ = reply.send(self.configure_nx_cache(backend));
                        }
                        Command::NxBackendStarted { repo_id, result } => {
                            self.nx_backend_started(repo_id, result).await;
                        }
                        Command::BeginNxPublication { publication, reply } => {
                            let result = match self.nx_backends.get(&publication.repo_id) {
                                Some(NxBackendState::Ready { .. }) => Ok(()),
                                Some(NxBackendState::Starting { .. }) => Err(GatewayError::NxAuthority(
                                    "the project's Nx cache backend is still starting".to_owned(),
                                )),
                                Some(NxBackendState::Failed { reason, .. }) => {
                                    Err(GatewayError::NxAuthority(reason.clone()))
                                }
                                None => Err(GatewayError::NxAuthority(
                                    "the project's Nx cache backend is not ready".to_owned(),
                                )),
                            }
                            .and_then(|()| {
                                publication
                                    .validate()
                                    .map_err(|reason| GatewayError::NxAuthority(reason.to_owned()))
                            })
                            .and_then(|()| {
                                let session = self
                                    .sessions
                                    .get(&publication.workspace_id)
                                    .ok_or(GatewayError::UnknownWorkspace)?;
                                if session.repo_id != publication.repo_id
                                    || session.revision != publication.expected_revision
                                    || self.nx_publications.contains_key(&publication.workspace_id)
                                    || session.token.matches(&publication.token)
                                {
                                    return Err(GatewayError::NxAuthority("Nx publication does not match the active project/session or already has a writer".to_owned()));
                                }
                                Ok(())
                            });
                            if result.is_ok() {
                                self.nx_publications
                                    .insert(publication.workspace_id.clone(), publication);
                            }
                            let _ = reply.send(result);
                        }
                        Command::EndNxPublication { workspace_id, token, reply } => {
                            if self.nx_publications.get(&workspace_id).is_some_and(|publication| publication.token.matches(&token)) {
                                self.nx_publications.remove(&workspace_id);
                                self.cancel_nx_writes(&workspace_id).await;
                            }
                            let _ = reply.send(Ok(()));
                        }
                        Command::Install { session, reply } => {
                            self.nx_publications.remove(&session.workspace_id);
                            let result = self.install(session).await;
                            let _ = reply.send(result);
                        }
                        Command::Remove {
                            workspace_id,
                            expected_revision,
                            reply,
                        } => {
                            self.nx_publications.remove(&workspace_id);
                            let result = self.remove(&workspace_id, expected_revision).await;
                            let _ = reply.send(result);
                        }
                        Command::Admit {
                            workspace_id,
                            authentication,
                            intent,
                            reply,
                            cancelled,
                        } => {
                            self.admit(workspace_id, authentication, intent, reply, cancelled)
                                .await;
                        }
                        Command::AuditDenial {
                            workspace_id,
                            attempt,
                            reply,
                        } => {
                            let recorded = self.audit_denial(&workspace_id, attempt).await;
                            let _ = reply.send(recorded);
                        }
                        Command::BrokerAudit { event, reply } => {
                            let result = self.broker_audit(event).await;
                            let _ = reply.send(result);
                        }
                        Command::QueueExpired { queue_id } => {
                            self.expire_queued(queue_id).await;
                        }
                        Command::MintLeaf {
                            workspace_id,
                            generation,
                            host,
                            reply,
                        } => {
                            let result = self.mint_leaf(&workspace_id, generation, &host);
                            let _ = reply.send(result);
                        }
                        Command::Status { reply } => {
                            let _ = reply.send(self.status());
                        }
                        Command::BeginDrain { reply } => {
                            self.begin_drain(reply).await;
                            if self.finish_drain().await? {
                                return Ok(());
                            }
                        }
                        Command::ForceStop => {
                            self.force_stop().await?;
                            return Ok(());
                        }
                    }
                }
            }
            // An audit failure drains exactly like a requested shutdown, and has to finish like
            // one: once the in-flight work is gone the actor stops with that failure, so the
            // daemon exits and its supervisor restarts it, instead of refusing every session
            // forever behind a control socket that still answers.
            if (self.drain_reply.is_some() || self.audit_failure.is_some())
                && self.finish_drain().await?
            {
                return Ok(());
            }
        }
    }

    async fn install(&mut self, session: WorkspaceSession) -> Result<(), GatewayError> {
        if self.draining {
            return Err(GatewayError::Draining);
        }
        session.validate()?;
        self.config.validate_session_endpoint(&session)?;
        let signer = CaSigner::parse(&session.ca)?;
        if (!self.sessions.contains_key(&session.workspace_id)
            && self.sessions.len() >= self.config.limits.max_sessions)
            || (!self.revisions.contains_key(&session.workspace_id)
                && self.revisions.len() >= self.config.limits.max_sessions)
        {
            return Err(GatewayError::SessionCapacity);
        }
        if self
            .revisions
            .get(&session.workspace_id)
            .is_some_and(|revision| session.revision <= *revision)
        {
            return Err(GatewayError::StaleRevision);
        }
        if self.sessions.iter().any(|(workspace_id, current)| {
            workspace_id != &session.workspace_id && current.endpoint == session.endpoint
        }) {
            return Err(GatewayError::EndpointInUse);
        }

        let workspace_id = session.workspace_id.clone();
        let same_endpoint = self
            .sessions
            .get(&workspace_id)
            .is_some_and(|current| current.endpoint == session.endpoint);
        let mut previous = self.sessions.remove(&workspace_id);
        let listener = if same_endpoint {
            if let Some(current) = previous.as_mut() {
                current.quiesce().await;
            }
            match bind_endpoint(&session.endpoint) {
                Ok(listener) => listener,
                Err(error) => {
                    if let Some(mut current) = previous.take() {
                        let restored = bind_endpoint(&current.endpoint)?;
                        self.attach_listener(&workspace_id, &mut current, restored);
                        self.sessions.insert(workspace_id, current);
                    }
                    return Err(error);
                }
            }
        } else {
            match bind_endpoint(&session.endpoint) {
                Ok(listener) => listener,
                Err(error) => {
                    if let Some(current) = previous.take() {
                        self.sessions.insert(workspace_id, current);
                    }
                    return Err(error);
                }
            }
        };

        if let Some(mut current) = previous {
            if !same_endpoint {
                current.quiesce().await;
            }
            self.leaf_cache.drop_workspace(&workspace_id);
            self.cancel_queued(&workspace_id).await;
        }
        if self.draining {
            drop(listener);
            unlink_endpoint(&session.endpoint);
            return Err(GatewayError::Draining);
        }

        self.repo_mirror
            .bind_session(
                workspace_id.clone(),
                session.repo_id.clone(),
                session.policy.clone(),
            )
            .await?;
        self.sim_broker
            .bind_session(workspace_id.clone(), session.repo_id.clone())
            .await?;

        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1).max(1);
        let endpoint_label = session.endpoint.to_string();
        let (accept_stop, accept_rx) = watch::channel(false);
        let (connection_stop, connection_rx) = watch::channel(false);
        let (audit_stop, audit_rx) = watch::channel(false);
        let listener_task = self.spawn_listener(
            workspace_id.clone(),
            listener,
            accept_rx,
            connection_rx.clone(),
            audit_rx.clone(),
        );
        let revision = session.revision;
        self.sessions.insert(
            workspace_id.clone(),
            SessionState {
                repo_id: session.repo_id,
                revision,
                endpoint: session.endpoint,
                endpoint_label,
                token: session.token,
                policy: session.policy,
                signer,
                generation,
                active: ActiveCounts::default(),
                queued: 0,
                accept_stop,
                connection_stop,
                audit_stop,
                listener_task,
            },
        );
        self.revisions.insert(workspace_id, revision);
        Ok(())
    }

    async fn remove(
        &mut self,
        workspace_id: &str,
        expected_revision: u64,
    ) -> Result<(), GatewayError> {
        let session = self
            .sessions
            .get(workspace_id)
            .ok_or(GatewayError::UnknownWorkspace)?;
        if session.revision != expected_revision {
            return Err(GatewayError::RevisionMismatch {
                expected: expected_revision,
                actual: session.revision,
            });
        }
        let session = self
            .sessions
            .remove(workspace_id)
            .expect("session was checked");
        session.stop().await;
        self.repo_mirror
            .unbind_session(workspace_id.to_owned())
            .await?;
        self.sim_broker
            .unbind_session(workspace_id.to_owned())
            .await?;
        self.leaf_cache.drop_workspace(workspace_id);
        self.cancel_queued(workspace_id).await;
        Ok(())
    }

    fn spawn_listener(
        &self,
        workspace_id: String,
        listener: BoundListener,
        accept_rx: watch::Receiver<bool>,
        connection_rx: watch::Receiver<bool>,
        audit_rx: watch::Receiver<bool>,
    ) -> JoinHandle<()> {
        let context = proxy::AcceptContext {
            workspace_id,
            commands: self.commands.clone(),
            credentials: Arc::clone(&self.credentials),
            connector: Arc::clone(&self.connector),
            timeouts: self.config.timeouts,
            connection_stop: connection_rx.clone(),
            audit_stop: audit_rx.clone(),
            mirror_service: self.mirror_service.clone(),
            repo_mirror: self.repo_mirror.clone(),
            sim_broker: self.sim_broker.clone(),
        };
        tokio::spawn(proxy::accept_loop(
            listener,
            context,
            accept_rx,
            connection_rx,
            audit_rx,
        ))
    }

    fn attach_listener(
        &self,
        workspace_id: &str,
        session: &mut SessionState,
        listener: BoundListener,
    ) {
        let (accept_stop, accept_rx) = watch::channel(false);
        let (connection_stop, connection_rx) = watch::channel(false);
        let (audit_stop, audit_rx) = watch::channel(false);
        let listener_task = self.spawn_listener(
            workspace_id.to_owned(),
            listener,
            accept_rx,
            connection_rx.clone(),
            audit_rx.clone(),
        );
        session.accept_stop = accept_stop;
        session.connection_stop = connection_stop;
        session.audit_stop = audit_stop;
        session.listener_task = listener_task;
    }

    fn configure_nx_cache(&mut self, backend: NxCacheBackend) -> Result<(), GatewayError> {
        if matches!(
            self.nx_backends.get(&backend.repo_id),
            Some(
                NxBackendState::Starting { .. }
                    | NxBackendState::Ready {
                        process: Some(_),
                        ..
                    }
            )
        ) {
            return Err(GatewayError::NxAuthority(
                "the project's Nx cache backend is managed by the gateway".to_owned(),
            ));
        }
        backend
            .validate()
            .map_err(|reason| GatewayError::NxAuthority(reason.to_owned()))?;
        self.nx_backends.insert(
            backend.repo_id.clone(),
            NxBackendState::Ready {
                backend,
                process: None,
            },
        );
        Ok(())
    }

    /// The project's Ready backend: the only backend state a publication or a request may use.
    fn nx_backend(&self, repo_id: &str) -> Option<&NxCacheBackend> {
        match self.nx_backends.get(repo_id) {
            Some(NxBackendState::Ready { backend, .. }) => Some(backend),
            _ => None,
        }
    }

    /// Starts the project's backend in its own task. The task owns a copy of the config and the
    /// repo, and reports through `NxBackendStarted`; the actor never awaits readiness.
    fn start_nx_backend(&mut self, repo_id: &str) -> Result<(), String> {
        let config = self
            .config
            .nx_cache
            .clone()
            .ok_or_else(|| "the installed Nx cache service is unavailable".to_owned())?;
        let repo = repo_id.to_owned();
        let worker_repo = repo.clone();
        let worker =
            tokio::spawn(
                async move { crate::nx_cache::start_backend(&config, &worker_repo).await },
            );
        let task = supervise_nx_start(repo, self.commands.clone(), worker);
        self.nx_backends
            .insert(repo_id.to_owned(), NxBackendState::Starting { task });
        Ok(())
    }

    /// The project backend's state for one request. A started child that has exited is replaced
    /// by a fresh start in this same call, and its writers are revoked. A failed start is an
    /// outage until its cooldown ends, after which one new start is attempted.
    async fn nx_admission(&mut self, repo_id: &str) -> NxAdmission {
        let exited = match self.nx_backends.get_mut(repo_id) {
            Some(NxBackendState::Ready {
                process: Some(process),
                ..
            }) => match process.try_wait() {
                Ok(None) => false,
                Ok(Some(status)) => {
                    eprintln!(
                        "cowshed: Nx cache backend for {repo_id} exited {status}; starting a replacement"
                    );
                    true
                }
                Err(error) => {
                    eprintln!(
                        "cowshed: cannot inspect the Nx cache backend for {repo_id}: {error}; starting a replacement"
                    );
                    true
                }
            },
            _ => false,
        };
        if exited {
            self.drop_nx_backend(repo_id).await;
        }
        match self.nx_backends.get(repo_id) {
            Some(NxBackendState::Ready { .. }) => return NxAdmission::Ready,
            Some(NxBackendState::Starting { .. }) => return NxAdmission::Starting,
            Some(NxBackendState::Failed {
                reason,
                retry_after,
            }) if std::time::Instant::now() < *retry_after => {
                return NxAdmission::Unavailable(NxRefusal::outage(reason.clone()));
            }
            Some(NxBackendState::Failed { .. }) | None => {}
        }
        match self.start_nx_backend(repo_id) {
            Ok(()) => NxAdmission::Starting,
            Err(reason) => NxAdmission::Unavailable(NxRefusal::outage(reason)),
        }
    }

    /// Removes a backend whose child is gone, revokes its writers, and cancels queued writes to it.
    async fn drop_nx_backend(&mut self, repo_id: &str) {
        self.nx_backends.remove(repo_id);
        let revoked: Vec<String> = self
            .nx_publications
            .iter()
            .filter(|(_, publication)| publication.repo_id == repo_id)
            .map(|(workspace_id, _)| workspace_id.clone())
            .collect();
        self.nx_publications
            .retain(|_, publication| publication.repo_id != repo_id);
        for workspace_id in revoked {
            self.cancel_nx_writes(&workspace_id).await;
        }
    }

    /// Installs the outcome of a start. Ready resumes this repo's queued requests; a failure
    /// becomes a cooldown, and its queued requests are refused as an outage.
    async fn nx_backend_started(
        &mut self,
        repo_id: String,
        result: Result<(NxCacheBackend, tokio::process::Child), String>,
    ) {
        if self.draining || self.audit_failure.is_some() {
            // A start that finishes while the gateway fails closed installs nothing: its child is
            // killed on drop, and its Starting state is removed and aborted if it still matches.
            if matches!(
                self.nx_backends.get(&repo_id),
                Some(NxBackendState::Starting { .. })
            ) && let Some(NxBackendState::Starting { task }) = self.nx_backends.remove(&repo_id)
            {
                task.abort();
            }
            return;
        }
        if !matches!(
            self.nx_backends.get(&repo_id),
            Some(NxBackendState::Starting { .. })
        ) {
            return;
        }
        match result {
            Ok((backend, process)) => {
                eprintln!(
                    "cowshed: Nx cache backend ready repo={} address={} reclaimed_staging_files={}",
                    backend.repo_id, backend.address, backend.reclaimed_staging_files,
                );
                self.nx_backends.insert(
                    repo_id.clone(),
                    NxBackendState::Ready {
                        backend,
                        process: Some(Box::new(process)),
                    },
                );
                self.resume_nx_pending(&repo_id).await;
            }
            Err(reason) => {
                eprintln!("cowshed: Nx cache backend for {repo_id} did not start: {reason}");
                let retry_after = std::time::Instant::now() + self.config.timeouts.connect;
                self.nx_backends.insert(
                    repo_id.clone(),
                    NxBackendState::Failed {
                        reason: reason.clone(),
                        retry_after,
                    },
                );
                self.fail_nx_pending(&repo_id, &reason).await;
            }
        }
    }

    /// Refuses one Nx request with the status its method owns: GET is 404, every non-GET is 403. The audit
    /// names the classification; the reply names the outcome.
    async fn refuse_nx(
        &mut self,
        workspace_id: &str,
        intent: &RequestIntent,
        refusal: NxRefusal,
        reply: oneshot::Sender<Result<Admission, AdmissionError>>,
    ) {
        let NxRefusal {
            status,
            classification,
            detail,
            message,
        } = refusal;
        let http_status = crate::nx_cache::unavailable_status(&intent.method);
        if let Some(detail) = &detail {
            eprintln!(
                "cowshed: Nx cache {} for workspace {workspace_id} refused ({classification}): {detail}",
                intent.method
            );
        }
        let recorded = self
            .audit_denial(
                workspace_id,
                AuditAttempt {
                    kind: intent.audit_kind,
                    host: None,
                    method: intent.method.to_string(),
                    path: intent.path.clone(),
                    status,
                    http_status,
                    classification,
                },
            )
            .await;
        let _ = reply.send(Err(if recorded {
            admission_error(http_status, message, status, detail)
        } else {
            audit_unavailable()
        }));
    }

    async fn admit(
        &mut self,
        workspace_id: String,
        authentication: Authentication,
        intent: RequestIntent,
        reply: oneshot::Sender<Result<Admission, AdmissionError>>,
        cancelled: oneshot::Receiver<()>,
    ) {
        if self.draining {
            let _ = reply.send(Err(admission_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "gateway is draining",
                AuditStatus::Cancelled,
                None,
            )));
            return;
        }
        let Some(session) = self.sessions.get(&workspace_id) else {
            let _ = reply.send(Err(audit_unavailable()));
            return;
        };
        let is_nx = matches!(intent.target, RequestTarget::LocalNx);
        let publication = self.nx_publications.get(&workspace_id);
        let writer = is_nx && publication.is_some_and(|publication| {
            publication.repo_id == session.repo_id && publication.expected_revision == session.revision
                && matches!(&authentication, Authentication::Bearer(Some(token)) if publication.token.matches_encoded(token))
        });
        let authenticated = writer
            || match &authentication {
                Authentication::Bearer(Some(token)) => session.token.matches_encoded(token),
                Authentication::Bearer(None) => false,
                Authentication::Generation(generation) => {
                    *generation == session.generation && !is_nx
                }
            };
        if !authenticated {
            let status = if is_nx {
                StatusCode::UNAUTHORIZED
            } else {
                StatusCode::PROXY_AUTHENTICATION_REQUIRED
            };
            let draft = denial_draft(
                &workspace_id,
                session,
                &intent,
                AuditStatus::Unauthorized,
                status,
                None,
            );
            let recorded = self.record(draft).await;
            let _ = reply.send(Err(if recorded {
                admission_error(
                    status,
                    "missing or invalid gateway credential",
                    AuditStatus::Unauthorized,
                    None,
                )
            } else {
                audit_unavailable()
            }));
            return;
        }
        let repo_id = session.repo_id.clone();
        if is_nx && intent.method != Method::GET && intent.method != Method::PUT {
            self.refuse_nx(
                &workspace_id,
                &intent,
                NxRefusal {
                    status: AuditStatus::Denied,
                    classification: "nx-method-denied",
                    detail: None,
                    message: "Nx cache accepts only GET and PUT",
                },
                reply,
            )
            .await;
            return;
        }
        if is_nx && intent.method == Method::PUT && !writer {
            self.refuse_nx(
                &workspace_id,
                &intent,
                NxRefusal {
                    status: AuditStatus::Denied,
                    classification: "nx-publication-required",
                    detail: None,
                    message: "Nx cache publication requires an active qualified landing",
                },
                reply,
            )
            .await;
            return;
        }
        if is_nx {
            match self.nx_admission(&repo_id).await {
                NxAdmission::Ready => {}
                NxAdmission::Starting => {
                    self.enqueue_nx(
                        workspace_id,
                        repo_id,
                        authentication,
                        intent,
                        reply,
                        cancelled,
                    )
                    .await;
                    return;
                }
                NxAdmission::Unavailable(refusal) => {
                    self.refuse_nx(&workspace_id, &intent, refusal, reply).await;
                    return;
                }
            }
        }
        let Some(session) = self.sessions.get(&workspace_id) else {
            let _ = reply.send(Err(audit_unavailable()));
            return;
        };
        let seed = match build_seed(
            &workspace_id,
            session,
            &intent,
            if is_nx {
                self.nx_backend(&repo_id)
            } else {
                None
            },
            if writer {
                self.nx_publications.get(&workspace_id)
            } else {
                None
            },
        ) {
            Ok(seed) => seed,
            Err((denial, hint)) => {
                let draft = denial_draft(
                    &workspace_id,
                    session,
                    &intent,
                    AuditStatus::Denied,
                    StatusCode::FORBIDDEN,
                    hint.clone(),
                );
                let recorded = self.record(draft).await;
                let _ = reply.send(Err(if recorded {
                    admission_error(StatusCode::FORBIDDEN, denial, AuditStatus::Denied, hint)
                } else {
                    audit_unavailable()
                }));
                return;
            }
        };
        let class = seed.class();
        if self.can_activate(&workspace_id, &class) {
            let admission = self.activate(seed, class);
            let _ = reply.send(Ok(admission));
            return;
        }
        if session.queued >= self.config.limits.workspace_queued
            || self.global_queued >= self.config.limits.global_queued
        {
            if is_nx {
                self.refuse_nx(&workspace_id, &intent, NxRefusal::queue_limited(), reply)
                    .await;
                return;
            }
            let draft = denial_draft(
                &workspace_id,
                session,
                &intent,
                AuditStatus::Limited,
                StatusCode::TOO_MANY_REQUESTS,
                None,
            );
            let recorded = self.record(draft).await;
            let _ = reply.send(Err(if recorded {
                admission_error(
                    StatusCode::TOO_MANY_REQUESTS,
                    "gateway queue is full",
                    AuditStatus::Limited,
                    None,
                )
            } else {
                audit_unavailable()
            }));
            return;
        }
        if let Some(session) = self.sessions.get_mut(&workspace_id) {
            session.queued += 1;
        }
        self.global_queued += 1;
        let queue_id = self.next_queue;
        self.next_queue = self.next_queue.wrapping_add(1).max(1);
        let (timer, cancellation) = self.spawn_queue_handles(queue_id, cancelled);
        self.queue.push_back(Pending {
            queue_id,
            nx_put: is_nx && intent.method == Method::PUT,
            seed,
            reply,
            timer: Some(timer),
            cancellation: Some(cancellation),
        });
    }

    fn can_activate(&self, workspace_id: &str, class: &PermitClass) -> bool {
        let Some(session) = self.sessions.get(workspace_id) else {
            return false;
        };
        pool_admits(
            &self.config.limits,
            session.active,
            self.global_active,
            class,
            |origin| {
                self.origins
                    .get(&(workspace_id.to_owned(), origin.to_owned()))
                    .copied()
                    .unwrap_or(0)
            },
        )
    }

    fn activate(&mut self, seed: AdmissionSeed, class: PermitClass) -> Admission {
        let permit_id = self.next_permit;
        self.next_permit = self.next_permit.wrapping_add(1).max(1);
        let workspace_id = seed.workspace_id.clone();
        let cancelled =
            pending_audit_draft(&seed, AuditStatus::Cancelled, None, Some("request-dropped"));
        let completion = self
            .completions
            .clone()
            .try_reserve_owned()
            .expect("completion capacity covers both active pools' global bounds");
        self.global_active.charge(&class);
        if let Some(session) = self.sessions.get_mut(&workspace_id) {
            session.active.charge(&class);
        }
        if let PermitClass::Request { origin } = &class {
            *self
                .origins
                .entry((workspace_id.clone(), origin.clone()))
                .or_default() += 1;
        }
        self.permits.insert(
            permit_id,
            PermitState {
                workspace_id,
                class,
                generation: seed.generation,
            },
        );
        seed.activate(
            permit_id,
            CompletionLease::new(completion, permit_id, cancelled),
        )
    }

    fn complete(&mut self, permit_id: u64) {
        let Some(permit) = self.permits.remove(&permit_id) else {
            return;
        };
        self.global_active.release(&permit.class);
        if let Some(session) = self.sessions.get_mut(&permit.workspace_id)
            && session.generation == permit.generation
        {
            session.active.release(&permit.class);
        }
        if let PermitClass::Request { origin } = permit.class {
            let origin_key = (permit.workspace_id, origin);
            if let Some(active) = self.origins.get_mut(&origin_key) {
                *active = active.saturating_sub(1);
                if *active == 0 {
                    self.origins.remove(&origin_key);
                }
            }
        }
        self.promote();
    }

    fn promote(&mut self) {
        let mut inspected = 0;
        while inspected < self.queue.len() {
            let Some(mut pending) = self.queue.pop_front() else {
                break;
            };
            let workspace_id = pending.seed.workspace_id.clone();
            if let Some(session) = self.sessions.get_mut(&workspace_id) {
                session.queued = session.queued.saturating_sub(1);
            }
            self.global_queued = self.global_queued.saturating_sub(1);
            if !self.sessions.contains_key(&workspace_id) {
                if let Some(timer) = pending.timer.take() {
                    timer.abort();
                }
                if let Some(cancellation) = pending.cancellation.take() {
                    cancellation.abort();
                }
                continue;
            }
            let class = pending.seed.class();
            if self.can_activate(&workspace_id, &class) {
                if let Some(timer) = pending.timer.take() {
                    timer.abort();
                }
                if let Some(cancellation) = pending.cancellation.take() {
                    cancellation.abort();
                }
                let admission = self.activate(pending.seed, class);
                let _ = pending.reply.send(Ok(admission));
                inspected = 0;
            } else {
                if let Some(session) = self.sessions.get_mut(&workspace_id) {
                    session.queued += 1;
                }
                self.global_queued += 1;
                self.queue.push_back(pending);
                inspected += 1;
            }
        }
    }

    /// Shares the timer and cancellation watch between ordinary and Nx-startup queues.
    /// Both report back through the actor's own channels.
    fn spawn_queue_handles(
        &self,
        queue_id: u64,
        cancelled: oneshot::Receiver<()>,
    ) -> (JoinHandle<()>, JoinHandle<()>) {
        let commands = self.commands.clone();
        let queue_timeout = self.config.timeouts.request_total;
        let timer = tokio::spawn(async move {
            tokio::time::sleep(queue_timeout).await;
            let _ = commands.send(Command::QueueExpired { queue_id }).await;
        });
        let cancellations = self.cancellations.clone();
        let cancellation = tokio::spawn(async move {
            let _ = cancelled.await;
            let _ = cancellations.send(queue_id).await;
        });
        (timer, cancellation)
    }

    /// Queues one request while its project backend starts. It is charged to the same session and
    /// global counters, and bounded by the same limits, as any queued request.
    async fn enqueue_nx(
        &mut self,
        workspace_id: String,
        repo_id: String,
        authentication: Authentication,
        intent: RequestIntent,
        reply: oneshot::Sender<Result<Admission, AdmissionError>>,
        cancelled: oneshot::Receiver<()>,
    ) {
        let Some(session) = self.sessions.get(&workspace_id) else {
            let _ = reply.send(Err(audit_unavailable()));
            return;
        };
        let generation = session.generation;
        if session.queued >= self.config.limits.workspace_queued
            || self.global_queued >= self.config.limits.global_queued
        {
            self.refuse_nx(&workspace_id, &intent, NxRefusal::queue_limited(), reply)
                .await;
            return;
        }
        if let Some(session) = self.sessions.get_mut(&workspace_id) {
            session.queued += 1;
        }
        self.global_queued += 1;
        let queue_id = self.next_queue;
        self.next_queue = self.next_queue.wrapping_add(1).max(1);
        let (timer, cancellation) = self.spawn_queue_handles(queue_id, cancelled);
        self.nx_queue.push_back(NxPending {
            queue_id,
            workspace_id,
            repo_id,
            generation,
            authentication,
            intent,
            reply,
            timer: Some(timer),
            cancellation: Some(cancellation),
        });
    }

    /// Releases one queued Nx request's handles and its charge to the shared queued counters.
    fn take_nx_pending(&mut self, mut pending: NxPending) -> NxPending {
        if let Some(timer) = pending.timer.take() {
            timer.abort();
        }
        if let Some(cancellation) = pending.cancellation.take() {
            cancellation.abort();
        }
        if let Some(session) = self.sessions.get_mut(&pending.workspace_id) {
            session.queued = session.queued.saturating_sub(1);
        }
        self.global_queued = self.global_queued.saturating_sub(1);
        pending
    }

    /// Releases one queued Nx request and refuses it with the outcome its method owns.
    async fn refuse_nx_pending(&mut self, pending: NxPending, refusal: NxRefusal) {
        let pending = self.take_nx_pending(pending);
        self.refuse_nx(
            &pending.workspace_id,
            &pending.intent,
            refusal,
            pending.reply,
        )
        .await;
    }

    /// Resumes this repo's queued Nx requests once its backend is Ready. Each is revalidated, then
    /// seeded against the Ready backend, then either activated or moved to the ordinary queue.
    async fn resume_nx_pending(&mut self, repo_id: &str) {
        let mut waiting = VecDeque::new();
        let mut resumed = Vec::new();
        while let Some(pending) = self.nx_queue.pop_front() {
            if pending.repo_id == repo_id {
                resumed.push(pending);
            } else {
                waiting.push_back(pending);
            }
        }
        self.nx_queue = waiting;
        for pending in resumed {
            self.resume_one(pending).await;
        }
        self.promote();
    }

    /// Revalidates one resumed Nx request against the session, its credential and the current
    /// publication, then seeds it. A request that no longer holds its authority is refused.
    async fn resume_one(&mut self, pending: NxPending) {
        let is_put = pending.intent.method == Method::PUT;
        let Some(session) = self.sessions.get(&pending.workspace_id) else {
            self.refuse_nx_pending(
                pending,
                NxRefusal {
                    status: AuditStatus::Cancelled,
                    classification: "session-rotated",
                    detail: None,
                    message: "workspace session rotated",
                },
            )
            .await;
            return;
        };
        let current =
            session.generation == pending.generation && session.repo_id == pending.repo_id;
        let authenticated = if is_put {
            self.nx_publications
                .get(&pending.workspace_id)
                .is_some_and(|publication| {
                    publication.repo_id == session.repo_id
                        && publication.expected_revision == session.revision
                        && matches!(
                            &pending.authentication,
                            Authentication::Bearer(Some(token)) if publication.token.matches_encoded(token)
                        )
                })
        } else {
            matches!(
                &pending.authentication,
                Authentication::Bearer(Some(token)) if session.token.matches_encoded(token)
            )
        };
        if !(current && authenticated) {
            let refusal = if is_put {
                NxRefusal {
                    status: AuditStatus::Cancelled,
                    classification: "nx-publication-ended",
                    detail: None,
                    message: "Nx publication ended before the backend was ready",
                }
            } else {
                NxRefusal {
                    status: AuditStatus::Unauthorized,
                    classification: "nx-credential-revoked",
                    detail: None,
                    message: "gateway credential was revoked before the backend was ready",
                }
            };
            self.refuse_nx_pending(pending, refusal).await;
            return;
        }
        let publication = if is_put {
            self.nx_publications.get(&pending.workspace_id)
        } else {
            None
        };
        let seed = match build_seed(
            &pending.workspace_id,
            session,
            &pending.intent,
            self.nx_backend(&pending.repo_id),
            publication,
        ) {
            Ok(seed) => seed,
            Err((denial, hint)) => {
                self.refuse_nx_pending(
                    pending,
                    NxRefusal {
                        status: AuditStatus::Denied,
                        classification: "nx-denied",
                        detail: hint,
                        message: denial,
                    },
                )
                .await;
                return;
            }
        };
        let class = seed.class();
        let NxPending {
            queue_id,
            workspace_id,
            reply,
            timer,
            cancellation,
            ..
        } = pending;
        let converted = Pending {
            queue_id,
            seed,
            reply,
            timer,
            cancellation,
            nx_put: is_put,
        };
        if self.can_activate(&workspace_id, &class) {
            let converted = self.take_pending(converted);
            let admission = self.activate(converted.seed, class);
            let _ = converted.reply.send(Ok(admission));
        } else {
            // The charge stays with the request: it moves to the ordinary queue, counters unchanged.
            self.queue.push_back(converted);
        }
    }

    /// A failed start refuses every queued request for its project as an outage, releasing each.
    async fn fail_nx_pending(&mut self, repo_id: &str, reason: &str) {
        let mut waiting = VecDeque::new();
        let mut failed = Vec::new();
        while let Some(pending) = self.nx_queue.pop_front() {
            if pending.repo_id == repo_id {
                failed.push(pending);
            } else {
                waiting.push_back(pending);
            }
        }
        self.nx_queue = waiting;
        for pending in failed {
            self.refuse_nx_pending(pending, NxRefusal::outage(reason.to_owned()))
                .await;
        }
    }

    /// Cancels every queued Nx publication for a workspace in both queues: the Nx queue holds one
    /// still waiting for its backend, and the ordinary queue holds one already seeded. Other queued
    /// requests are untouched.
    async fn cancel_nx_writes(&mut self, workspace_id: &str) {
        let mut waiting = VecDeque::new();
        let mut cancelled = Vec::new();
        while let Some(pending) = self.nx_queue.pop_front() {
            if pending.workspace_id == workspace_id && pending.intent.method == Method::PUT {
                cancelled.push(pending);
            } else {
                waiting.push_back(pending);
            }
        }
        self.nx_queue = waiting;
        for pending in cancelled {
            self.refuse_nx_pending(
                pending,
                NxRefusal {
                    status: AuditStatus::Cancelled,
                    classification: "nx-publication-ended",
                    detail: None,
                    message: "Nx publication ended before the backend was ready",
                },
            )
            .await;
        }
        let mut retained = VecDeque::new();
        while let Some(pending) = self.queue.pop_front() {
            if pending.nx_put && pending.seed.workspace_id == workspace_id {
                let pending = self.take_pending(pending);
                let draft = pending_audit_draft(
                    &pending.seed,
                    AuditStatus::Cancelled,
                    Some(StatusCode::FORBIDDEN),
                    Some("nx-publication-ended"),
                );
                let recorded = self.record(draft).await;
                let _ = pending.reply.send(Err(if recorded {
                    admission_error(
                        StatusCode::FORBIDDEN,
                        "Nx publication ended before it was admitted",
                        AuditStatus::Cancelled,
                        None,
                    )
                } else {
                    audit_unavailable()
                }));
            } else {
                retained.push_back(pending);
            }
        }
        self.queue = retained;
    }

    async fn cancel_queued(&mut self, workspace_id: &str) {
        let mut waiting = VecDeque::new();
        let mut rotated = Vec::new();
        while let Some(pending) = self.nx_queue.pop_front() {
            if pending.workspace_id == workspace_id {
                rotated.push(pending);
            } else {
                waiting.push_back(pending);
            }
        }
        self.nx_queue = waiting;
        for pending in rotated {
            self.refuse_nx_pending(
                pending,
                NxRefusal {
                    status: AuditStatus::Cancelled,
                    classification: "session-rotated",
                    detail: None,
                    message: "workspace session rotated",
                },
            )
            .await;
        }
        let mut retained = VecDeque::new();
        while let Some(pending) = self.queue.pop_front() {
            if pending.seed.workspace_id == workspace_id {
                let pending = self.take_pending(pending);
                let draft = pending_audit_draft(
                    &pending.seed,
                    AuditStatus::Cancelled,
                    Some(StatusCode::SERVICE_UNAVAILABLE),
                    Some("session-rotated"),
                );
                let recorded = self.record(draft).await;
                let _ = pending.reply.send(Err(if recorded {
                    admission_error(
                        StatusCode::SERVICE_UNAVAILABLE,
                        "workspace session rotated",
                        AuditStatus::Cancelled,
                        None,
                    )
                } else {
                    audit_unavailable()
                }));
            } else {
                retained.push_back(pending);
            }
        }
        self.queue = retained;
    }

    async fn cancel_queued_request(&mut self, queue_id: u64) {
        if let Some(position) = self
            .nx_queue
            .iter()
            .position(|pending| pending.queue_id == queue_id)
        {
            let pending = self
                .nx_queue
                .remove(position)
                .expect("queued position exists");
            self.refuse_nx_pending(
                pending,
                NxRefusal {
                    status: AuditStatus::Cancelled,
                    classification: "queue-cancelled",
                    detail: None,
                    message: "queued request was cancelled",
                },
            )
            .await;
            return;
        }
        let Some(position) = self
            .queue
            .iter()
            .position(|pending| pending.queue_id == queue_id)
        else {
            return;
        };
        let pending = self.queue.remove(position).expect("queued position exists");
        let pending = self.take_pending(pending);
        let draft = pending_audit_draft(
            &pending.seed,
            AuditStatus::Cancelled,
            None,
            Some("queue-cancelled"),
        );
        self.record(draft).await;
        self.promote();
    }

    async fn expire_queued(&mut self, queue_id: u64) {
        if let Some(position) = self
            .nx_queue
            .iter()
            .position(|pending| pending.queue_id == queue_id)
        {
            let pending = self
                .nx_queue
                .remove(position)
                .expect("queued position exists");
            self.refuse_nx_pending(pending, NxRefusal::queue_timeout())
                .await;
            return;
        }
        let Some(position) = self
            .queue
            .iter()
            .position(|pending| pending.queue_id == queue_id)
        else {
            return;
        };
        let pending = self.queue.remove(position).expect("queued position exists");
        let pending = self.take_pending(pending);
        // Only an Nx seed carries a backend token. Its deadline is its method's miss or refusal,
        // as in the startup queue (`NxRefusal::queue_timeout`); every other request's is 504.
        let http_status = if pending.seed.nx_token.is_some() {
            crate::nx_cache::unavailable_status(&pending.seed.method)
        } else {
            StatusCode::GATEWAY_TIMEOUT
        };
        let draft = pending_audit_draft(
            &pending.seed,
            AuditStatus::TimedOut,
            Some(http_status),
            Some("queue-timeout"),
        );
        let recorded = self.record(draft).await;
        let _ = pending.reply.send(Err(if recorded {
            admission_error(
                http_status,
                "gateway queue wait timed out",
                AuditStatus::TimedOut,
                None,
            )
        } else {
            audit_unavailable()
        }));
    }

    fn take_pending(&mut self, mut pending: Pending) -> Pending {
        if let Some(timer) = pending.timer.take() {
            timer.abort();
        }
        if let Some(cancellation) = pending.cancellation.take() {
            cancellation.abort();
        }
        if let Some(session) = self.sessions.get_mut(&pending.seed.workspace_id) {
            session.queued = session.queued.saturating_sub(1);
        }
        self.global_queued = self.global_queued.saturating_sub(1);
        pending
    }

    async fn audit_denial(&mut self, workspace_id: &str, attempt: AuditAttempt) -> bool {
        let Some(session) = self.sessions.get(workspace_id) else {
            return false;
        };
        let draft = AuditDraft {
            workspace_id: workspace_id.to_owned(),
            repo_id: session.repo_id.clone(),
            revision: session.revision,
            endpoint: session.endpoint_label.clone(),
            kind: attempt.kind,
            host: attempt.host,
            method: Some(attempt.method),
            path: Some(attempt.path),
            status: attempt.status,
            http_status: Some(attempt.http_status.as_u16()),
            bytes: 0,
            trace_id: None,
            span_id: 0,
            parent_span_id: None,
            upstream_span_id: None,
            tracestate: None,
            grant_hint: None,
            classification: Some(attempt.classification.to_owned()),
            mirror_cache_status: None,
        };
        self.record(draft).await
    }

    async fn broker_audit(&mut self, event: BrokerAuditEvent) -> Result<(), AuditError> {
        let Some(session) = self.sessions.get(&event.workspace_id) else {
            return Err(AuditError("broker workspace is not installed".to_owned()));
        };
        let (repo_id, revision, endpoint) = (
            session.repo_id.clone(),
            session.revision,
            session.endpoint_label.clone(),
        );
        let draft = AuditDraft {
            workspace_id: event.workspace_id,
            repo_id,
            revision,
            endpoint,
            kind: event.kind,
            host: None,
            method: event.method,
            path: event.path,
            status: event.status,
            http_status: None,
            bytes: event.bytes,
            trace_id: None,
            span_id: 0,
            parent_span_id: None,
            upstream_span_id: None,
            tracestate: None,
            grant_hint: None,
            classification: event.classification,
            mirror_cache_status: None,
        };
        if self.record(draft).await {
            Ok(())
        } else {
            Err(AuditError("gateway audit is unavailable".to_owned()))
        }
    }

    fn mint_leaf(
        &mut self,
        workspace_id: &str,
        generation: u64,
        host: &str,
    ) -> Result<Arc<rustls::ServerConfig>, GatewayError> {
        let session = self
            .sessions
            .get(workspace_id)
            .ok_or(GatewayError::UnknownWorkspace)?;
        if generation != session.generation {
            return Err(GatewayError::SessionRotated);
        }
        self.leaf_cache
            .get_or_mint(workspace_id, host, &session.signer)
            .map_err(GatewayError::Tls)
    }

    async fn record(&mut self, draft: AuditDraft) -> bool {
        if self.audit_failure.is_some() {
            return false;
        }
        let sequence = self.next_audit;
        let event = draft.into_event(sequence);
        if validate_event_shape(&event).is_err() {
            return false;
        }
        self.next_audit = self.next_audit.wrapping_add(1).max(1);
        match self.audit.record(event).await {
            Ok(()) => true,
            Err(error) => {
                self.fail_closed(error);
                false
            }
        }
    }

    fn fail_closed(&mut self, error: AuditError) {
        if self.audit_failure.is_some() {
            return;
        }
        self.audit_failure = Some(error);
        self.draining = true;
        for session in self.sessions.values() {
            let _ = session.accept_stop.send(true);
            let _ = session.audit_stop.send(true);
        }
        while let Some(pending) = self.queue.pop_front() {
            let pending = self.take_pending(pending);
            let _ = pending.reply.send(Err(audit_unavailable()));
        }
        while let Some(pending) = self.nx_queue.pop_front() {
            let pending = self.take_nx_pending(pending);
            let _ = pending.reply.send(Err(audit_unavailable()));
        }
        let starting: Vec<String> = self
            .nx_backends
            .iter()
            .filter(|(_, state)| matches!(state, NxBackendState::Starting { .. }))
            .map(|(repo_id, _)| repo_id.clone())
            .collect();
        for repo_id in starting {
            if let Some(NxBackendState::Starting { task }) = self.nx_backends.remove(&repo_id) {
                task.abort();
            }
        }
    }

    fn status(&self) -> DaemonStatus {
        let mut sessions: Vec<_> = self
            .sessions
            .iter()
            .map(|(workspace_id, session)| SessionStatus {
                workspace_id: workspace_id.clone(),
                revision: session.revision,
                endpoint: session.endpoint_label.clone(),
                active: session.active.total(),
                queued: session.queued,
            })
            .collect();
        sessions.sort_by(|left, right| left.workspace_id.cmp(&right.workspace_id));
        DaemonStatus {
            version: env!("CARGO_PKG_VERSION").to_owned(),
            draining: self.draining,
            drain_cause: self.draining.then(|| match &self.audit_failure {
                Some(error) => format!("audit sink failed, so the gateway fails closed: {error}"),
                None => "the gateway is shutting down".to_owned(),
            }),
            executable_sha256: self.config.executable_sha256.clone(),
            healing: self
                .config
                .startup
                .as_ref()
                .and_then(|probe| probe.healing()),
            recovering: self
                .config
                .startup
                .as_ref()
                .and_then(|probe| probe.recovering()),
            sessions,
            active: self.global_active.total(),
            queued: self.global_queued,
        }
    }

    async fn begin_drain(&mut self, reply: oneshot::Sender<()>) {
        self.draining = true;
        self.drain_reply = Some(reply);
        for session in self.sessions.values() {
            let _ = session.accept_stop.send(true);
            let _ = session.connection_stop.send(true);
        }
        while let Some(pending) = self.queue.pop_front() {
            let pending = self.take_pending(pending);
            let draft = pending_audit_draft(
                &pending.seed,
                AuditStatus::Cancelled,
                Some(StatusCode::SERVICE_UNAVAILABLE),
                Some("gateway-draining"),
            );
            let recorded = self.record(draft).await;
            let _ = pending.reply.send(Err(if recorded {
                admission_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "gateway is draining",
                    AuditStatus::Cancelled,
                    None,
                )
            } else {
                audit_unavailable()
            }));
        }
        while let Some(pending) = self.nx_queue.pop_front() {
            self.refuse_nx_pending(
                pending,
                NxRefusal {
                    status: AuditStatus::Cancelled,
                    classification: "gateway-draining",
                    detail: None,
                    message: "gateway is draining",
                },
            )
            .await;
        }
    }

    async fn finish_drain(&mut self) -> Result<bool, GatewayError> {
        if !self.draining || self.global_active.total() != 0 {
            return Ok(false);
        }
        self.nx_publications.clear();
        for (_, state) in self.nx_backends.drain() {
            match state {
                NxBackendState::Starting { task } => task.abort(),
                NxBackendState::Ready {
                    process: Some(mut process),
                    ..
                } => {
                    if let Some(pid) = process.id() {
                        // The actor owns this child; SIGTERM lets its existing server drain.
                        let pid = libc::pid_t::try_from(pid).map_err(|_| {
                            std::io::Error::other("Nx cache child pid is out of range")
                        })?;
                        let result = unsafe { libc::kill(pid, libc::SIGTERM) };
                        if result != 0 {
                            return Err(std::io::Error::last_os_error().into());
                        }
                    }
                    process.wait().await?;
                }
                NxBackendState::Ready { process: None, .. } | NxBackendState::Failed { .. } => {}
            }
        }
        let sessions = std::mem::take(&mut self.sessions);
        for (_, session) in sessions {
            session.stop().await;
        }
        self.audit.flush().await?;
        if let Some(error) = self.audit_failure.take() {
            return Err(GatewayError::Audit(error));
        }
        if let Some(reply) = self.drain_reply.take() {
            let _ = reply.send(());
        }
        Ok(true)
    }

    async fn force_stop(&mut self) -> Result<(), GatewayError> {
        self.nx_publications.clear();
        for (_, state) in self.nx_backends.drain() {
            if let NxBackendState::Starting { task } = state {
                task.abort();
            }
        }
        while let Some(pending) = self.nx_queue.pop_front() {
            self.take_nx_pending(pending);
        }
        let sessions = std::mem::take(&mut self.sessions);
        for (_, session) in sessions {
            session.stop().await;
        }
        while let Some(mut pending) = self.queue.pop_front() {
            if let Some(timer) = pending.timer.take() {
                timer.abort();
            }
            if let Some(cancellation) = pending.cancellation.take() {
                cancellation.abort();
            }
        }
        self.permits.clear();
        self.origins.clear();
        self.global_active = ActiveCounts::default();
        self.global_queued = 0;
        self.audit.flush().await?;
        if let Some(error) = self.audit_failure.take() {
            return Err(GatewayError::Audit(error));
        }
        Ok(())
    }
}

fn build_seed(
    workspace_id: &str,
    session: &SessionState,
    intent: &RequestIntent,
    nx_backend: Option<&NxCacheBackend>,
    publication: Option<&NxPublication>,
) -> Result<AdmissionSeed, (&'static str, Option<String>)> {
    let mut nx_token = None;
    let (target, mode, protocol, credential_allowed, private_network_authorized, upstream_path) =
        match &intent.target {
            RequestTarget::Generic(target) => {
                let grant = session
                    .policy
                    .authorize(target, &intent.method, &intent.path)
                    .map_err(|denial| match denial {
                        PolicyDenial::InvalidPath => ("request path is ambiguous", None),
                        PolicyDenial::NotGranted { hint } => {
                            ("destination is not granted", Some(hint))
                        }
                    })?;
                let npm = if grant.mode == EgressMode::Intercept && intent.npm_mirror {
                    session.policy.resolve_npm_registry(target, &intent.path)
                } else {
                    None
                };
                (
                    target.clone(),
                    grant.mode,
                    npm.as_ref().map(|_| MirrorProtocol::Npm),
                    npm.as_ref()
                        .map_or(grant.mode == EgressMode::Intercept, |route| {
                            route.credentialed
                        }),
                    grant.host.is_exact(),
                    intent.path.clone(),
                )
            }
            RequestTarget::LocalNx => {
                let hash = crate::nx_cache::route_hash(&intent.path)
                    .ok_or(("Nx cache path is invalid", None))?;
                let backend =
                    nx_backend.ok_or(("project Nx cache backend is unavailable", None))?;
                let token = if intent.method == Method::GET {
                    &backend.read_token
                } else if intent.method == Method::PUT {
                    publication
                        .filter(|publication| publication.hashes.contains(hash))
                        .ok_or((
                            "Nx cache publication requires an active qualified landing",
                            None,
                        ))?;
                    &backend.write_token
                } else {
                    return Err(("Nx cache accepts only GET and PUT", None));
                };
                nx_token = Some(token.clone());
                (
                    backend.target(),
                    EgressMode::Intercept,
                    None,
                    false,
                    true,
                    format!("/v1/cache/{hash}"),
                )
            }
            RequestTarget::LocalSim => {
                if intent.method != Method::POST || intent.path != "/sim" {
                    return Err(("simulator broker accepts only POST /sim", None));
                }
                (
                    CanonicalTarget {
                        scheme: TargetScheme::Http,
                        host: CanonicalHost::Dns("simulator.local".to_owned()),
                        port: 80,
                    },
                    EgressMode::Intercept,
                    None,
                    false,
                    false,
                    intent.path.clone(),
                )
            }
        };
    Ok(AdmissionSeed {
        workspace_id: workspace_id.to_owned(),
        repo_id: session.repo_id.clone(),
        revision: session.revision,
        endpoint: session.endpoint_label.clone(),
        generation: session.generation,
        target,
        mode,
        protocol,
        credential_allowed,
        private_network_authorized,
        nx_token,
        audit_kind: match protocol {
            Some(MirrorProtocol::Npm) => AuditKind::Npm,
            None => intent.audit_kind,
        },
        method: intent.method.clone(),
        request_path: intent.path.clone(),
        upstream_path,
        trace_id: intent.trace_id.clone(),
    })
}

fn pending_audit_draft(
    seed: &AdmissionSeed,
    status: AuditStatus,
    http_status: Option<StatusCode>,
    classification: Option<&str>,
) -> AuditDraft {
    AuditDraft {
        workspace_id: seed.workspace_id.clone(),
        repo_id: seed.repo_id.clone(),
        revision: seed.revision,
        endpoint: seed.endpoint.clone(),
        kind: seed.audit_kind,
        host: Some(seed.target.authority()),
        method: Some(seed.method.to_string()),
        path: Some(seed.request_path.clone()),
        status,
        http_status: http_status.map(|status| status.as_u16()),
        bytes: 0,
        trace_id: seed.trace_id.clone(),
        span_id: 0,
        parent_span_id: None,
        upstream_span_id: None,
        tracestate: None,
        grant_hint: None,
        classification: classification.map(str::to_owned),
        mirror_cache_status: None,
    }
}

fn denial_draft(
    workspace_id: &str,
    session: &SessionState,
    intent: &RequestIntent,
    status: AuditStatus,
    http_status: StatusCode,
    hint: Option<String>,
) -> AuditDraft {
    let host = match &intent.target {
        RequestTarget::Generic(target) => Some(target.authority()),
        RequestTarget::LocalSim | RequestTarget::LocalNx => None,
    };
    AuditDraft {
        workspace_id: workspace_id.to_owned(),
        repo_id: session.repo_id.clone(),
        revision: session.revision,
        endpoint: session.endpoint_label.clone(),
        kind: intent.audit_kind,
        host,
        method: Some(intent.method.to_string()),
        path: Some(intent.path.clone()),
        status,
        http_status: Some(http_status.as_u16()),
        bytes: 0,
        trace_id: intent.trace_id.clone(),
        span_id: 0,
        parent_span_id: None,
        upstream_span_id: None,
        tracestate: None,
        grant_hint: hint,
        classification: None,
        mirror_cache_status: None,
    }
}

fn admission_error(
    status: StatusCode,
    message: &'static str,
    audit_status: AuditStatus,
    hint: Option<String>,
) -> AdmissionError {
    AdmissionError {
        status,
        message,
        hint,
        audit_status,
    }
}

fn audit_unavailable() -> AdmissionError {
    admission_error(
        StatusCode::SERVICE_UNAVAILABLE,
        "audit service is unavailable",
        AuditStatus::Failed,
        None,
    )
}

fn bind_tcp(address: std::net::SocketAddr) -> std::io::Result<Fenced<TcpListener>> {
    Fenced::create(|| {
        let listener = std::net::TcpListener::bind(address)?;
        listener.set_nonblocking(true)?;
        TcpListener::from_std(listener)
    })
}

fn bind_unix(path: &Path) -> std::io::Result<Fenced<UnixListener>> {
    Fenced::create(|| {
        let listener = std::os::unix::net::UnixListener::bind(path)?;
        listener.set_nonblocking(true)?;
        UnixListener::from_std(listener)
    })
}

/// Socket accept and close-on-exec publication share the fork lock; waiting holds no guard.
pub(crate) async fn accept_locked<T>(
    mut accept: impl FnMut(&mut std::task::Context<'_>) -> std::task::Poll<std::io::Result<T>>,
) -> std::io::Result<T> {
    std::future::poll_fn(|context| match fork_lock::create(|| Ok(accept(context))) {
        Ok(result) => result,
        Err(error) => std::task::Poll::Ready(Err(error)),
    })
    .await
}

pub(crate) enum BoundListener {
    Tcp(Fenced<TcpListener>),
    Unix(Fenced<UnixListener>),
}

fn bind_endpoint(endpoint: &WorkspaceEndpoint) -> Result<BoundListener, GatewayError> {
    match endpoint {
        WorkspaceEndpoint::Tcp { address, .. } => Ok(BoundListener::Tcp(bind_tcp(*address)?)),
        WorkspaceEndpoint::Unix(path) => {
            remove_stale_socket(path)?;
            let listener = bind_unix(path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
            }
            Ok(BoundListener::Unix(listener))
        }
    }
}

fn remove_stale_socket(path: &Path) -> Result<(), std::io::Error> {
    match std::fs::symlink_metadata(path) {
        Ok(metadata) => {
            #[cfg(unix)]
            {
                use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _};

                if !metadata.file_type().is_socket()
                    || metadata.file_type().is_symlink()
                    || metadata.uid() != unsafe { libc::geteuid() }
                {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidInput,
                        "workspace socket path is not an owned stale socket",
                    ));
                }
            }
            std::fs::remove_file(path)
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error),
    }
}

fn unlink_endpoint(endpoint: &WorkspaceEndpoint) {
    if let WorkspaceEndpoint::Unix(path) = endpoint {
        let _ = std::fs::remove_file(path);
    }
}

struct ControlRuntime {
    path: std::path::PathBuf,
    stop: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl ControlRuntime {
    fn start(
        path: &Path,
        authorized_uid: u32,
        services: crate::control::ControlServices,
        tcp: Option<&ControlTcpConfig>,
    ) -> Result<Self, GatewayError> {
        use std::os::unix::fs::{FileTypeExt as _, MetadataExt as _, PermissionsExt as _};

        let parent = path.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "control socket has no parent",
            )
        })?;
        let metadata = std::fs::symlink_metadata(parent)?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || metadata.uid() != authorized_uid
            || metadata.permissions().mode() & 0o022 != 0
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "control socket parent must be an owned, non-writable real directory",
            )
            .into());
        }
        match std::fs::symlink_metadata(path) {
            Ok(existing)
                if existing.file_type().is_socket() && existing.uid() == authorized_uid =>
            {
                std::fs::remove_file(path)?;
            }
            Ok(_) => {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidInput,
                    "control socket path is not an owned Unix socket",
                )
                .into());
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let listener = bind_unix(path)?;
        if let Err(error) = std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600)) {
            let _ = std::fs::remove_file(path);
            return Err(error.into());
        }
        let tcp_runtime = if let Some(tcp) = tcp {
            let credential =
                match crate::control::ControllerCredential::from_file(&tcp.credential_file) {
                    Ok(credential) => Arc::new(credential),
                    Err(error) => {
                        let _ = std::fs::remove_file(path);
                        return Err(GatewayError::Control(error));
                    }
                };
            let listener = match bind_tcp(tcp.address) {
                Ok(listener) => listener,
                Err(error) => {
                    let _ = std::fs::remove_file(path);
                    return Err(error.into());
                }
            };
            Some((listener, credential))
        } else {
            None
        };

        let capacity = 1024;
        let (stop, stopped) = watch::channel(false);
        let unix_services = services.clone();
        let unix_stopped = stopped.clone();
        let unix_task = tokio::spawn(async move {
            run_unix_control(
                listener,
                authorized_uid,
                capacity,
                unix_services,
                unix_stopped,
            )
            .await;
        });
        let mut tasks = vec![unix_task];
        if let Some((listener, credential)) = tcp_runtime {
            let tcp_task = tokio::spawn(async move {
                run_tcp_control(listener, credential, capacity, services, stopped).await;
            });
            tasks.push(tcp_task);
        }
        Ok(Self {
            path: path.to_path_buf(),
            stop,
            tasks,
        })
    }

    async fn stop(mut self) {
        let _ = self.stop.send(true);
        for task in self.tasks.drain(..) {
            let _ = task.await;
        }
        let _ = std::fs::remove_file(self.path);
    }
}

async fn run_unix_control(
    listener: Fenced<UnixListener>,
    authorized_uid: u32,
    capacity: usize,
    services: crate::control::ControlServices,
    mut stopped: watch::Receiver<bool>,
) {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            result = accept_locked(|context| listener.poll_accept(context)) => {
                let Ok((stream, _)) = result else { break };
                let Ok(credentials) = stream.peer_cred() else { continue };
                if credentials.uid() != authorized_uid || connections.len() >= capacity {
                    continue;
                }
                let services = services.clone();
                connections.spawn(async move {
                    crate::control::serve_control_unix(stream, services).await;
                });
            }
            _ = connections.join_next(), if !connections.is_empty() => {}
            changed = stopped.changed() => {
                if changed.is_err() || *stopped.borrow() { break; }
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
}

async fn run_tcp_control(
    listener: Fenced<TcpListener>,
    credential: Arc<crate::control::ControllerCredential>,
    capacity: usize,
    services: crate::control::ControlServices,
    mut stopped: watch::Receiver<bool>,
) {
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            result = accept_locked(|context| listener.poll_accept(context)) => {
                let Ok((stream, peer)) = result else { break };
                if peer.ip() != std::net::IpAddr::V4(std::net::Ipv4Addr::LOCALHOST)
                    || connections.len() >= capacity
                {
                    continue;
                }
                let services = services.clone();
                let credential = Arc::clone(&credential);
                connections.spawn(async move {
                    crate::control::serve_control_tcp(stream, credential, services).await;
                });
            }
            _ = connections.join_next(), if !connections.is_empty() => {}
            changed = stopped.changed() => {
                if changed.is_err() || *stopped.borrow() { break; }
            }
        }
    }
    connections.abort_all();
    while connections.join_next().await.is_some() {}
}

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Cache(#[from] CacheError),
    #[error(transparent)]
    Tls(#[from] TlsError),
    #[error(transparent)]
    Audit(#[from] AuditError),
    #[error(transparent)]
    Connector(#[from] ConnectError),
    #[error(transparent)]
    Credential(#[from] CredentialError),
    #[error(transparent)]
    Control(#[from] ControlError),
    #[error(transparent)]
    RepoMirror(#[from] RepoMirrorError),
    #[error(transparent)]
    SimBroker(#[from] SimBrokerError),
    #[error("Nx cache authority refused: {0}")]
    NxAuthority(String),
    #[error("gateway session capacity is exhausted")]
    SessionCapacity,
    #[error("gateway I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("gateway is draining")]
    Draining,
    #[error("workspace session revision did not advance")]
    StaleRevision,
    #[error("workspace endpoint is already owned by another session")]
    EndpointInUse,
    #[error("workspace is not installed")]
    UnknownWorkspace,
    #[error("workspace revision fence mismatch: expected {expected}, current {actual}")]
    RevisionMismatch { expected: u64, actual: u64 },
    #[error("workspace session rotated")]
    SessionRotated,
    #[error("production gateway credentials are unsupported on this host platform")]
    UnsupportedProductionPlatform,
    #[error("gateway actor stopped")]
    Stopped,
    #[error("gateway task failed: {0}")]
    Task(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn seed(method: Method, mode: EgressMode) -> AdmissionSeed {
        AdmissionSeed {
            workspace_id: "workspace".to_owned(),
            repo_id: "repo".to_owned(),
            revision: 1,
            endpoint: "endpoint".to_owned(),
            generation: 1,
            target: CanonicalTarget {
                scheme: TargetScheme::Https,
                host: CanonicalHost::Dns("registry.example.test".to_owned()),
                port: 443,
            },
            mode,
            protocol: None,
            credential_allowed: false,
            private_network_authorized: false,
            nx_token: None,
            audit_kind: AuditKind::Connect,
            method,
            request_path: "/".to_owned(),
            upstream_path: "/".to_owned(),
            trace_id: None,
        }
    }

    /// Charges `class` the way `Actor::activate` does, after checking it fits.
    fn admit(
        limits: &GatewayLimits,
        session: &mut ActiveCounts,
        global: &mut ActiveCounts,
        origin_active: &mut usize,
        class: &PermitClass,
    ) -> bool {
        if !pool_admits(limits, *session, *global, class, |_| *origin_active) {
            return false;
        }
        session.charge(class);
        global.charge(class);
        if let PermitClass::Request { .. } = class {
            *origin_active += 1;
        }
        true
    }

    #[test]
    fn only_intercepted_connect_is_a_tunnel() {
        let origin = seed(Method::GET, EgressMode::Intercept).target.origin();
        assert_eq!(
            seed(Method::CONNECT, EgressMode::Intercept).class(),
            PermitClass::InterceptTunnel
        );
        assert_eq!(
            seed(Method::CONNECT, EgressMode::Opaque).class(),
            PermitClass::Request {
                origin: origin.clone()
            }
        );
        assert_eq!(
            seed(Method::GET, EgressMode::Intercept).class(),
            PermitClass::Request { origin }
        );
    }

    #[test]
    fn origin_limit_of_intercept_tunnels_leaves_inner_requests_admissible() {
        let limits = GatewayLimits::default();
        let tunnel = seed(Method::CONNECT, EgressMode::Intercept).class();
        let request = seed(Method::GET, EgressMode::Intercept).class();
        let (mut session, mut global, mut origin_active) =
            (ActiveCounts::default(), ActiveCounts::default(), 0_usize);
        for _ in 0..limits.origin_active {
            assert!(admit(
                &limits,
                &mut session,
                &mut global,
                &mut origin_active,
                &tunnel
            ));
        }
        assert_eq!(origin_active, 0, "tunnels never hold an origin slot");
        for _ in 0..limits.origin_active {
            assert!(admit(
                &limits,
                &mut session,
                &mut global,
                &mut origin_active,
                &request
            ));
        }
        assert!(
            !admit(
                &limits,
                &mut session,
                &mut global,
                &mut origin_active,
                &request
            ),
            "requests stay origin-limited"
        );
        assert_eq!(session.total(), limits.origin_active * 2);
    }

    #[test]
    fn single_active_limit_admits_one_tunnel_and_its_inner_request() {
        let limits = GatewayLimits {
            workspace_active: 1,
            global_active: 1,
            origin_active: 1,
            ..GatewayLimits::default()
        };
        let tunnel = seed(Method::CONNECT, EgressMode::Intercept).class();
        let request = seed(Method::GET, EgressMode::Intercept).class();
        let opaque = seed(Method::CONNECT, EgressMode::Opaque).class();
        let (mut session, mut global, mut origin_active) =
            (ActiveCounts::default(), ActiveCounts::default(), 0_usize);
        assert!(admit(
            &limits,
            &mut session,
            &mut global,
            &mut origin_active,
            &tunnel
        ));
        assert!(!pool_admits(&limits, session, global, &tunnel, |_| 0));
        assert!(admit(
            &limits,
            &mut session,
            &mut global,
            &mut origin_active,
            &request
        ));
        assert!(!pool_admits(&limits, session, global, &request, |_| 0));
        assert!(!pool_admits(&limits, session, global, &opaque, |_| {
            origin_active
        }));
        session.release(&request);
        global.release(&request);
        origin_active -= 1;
        assert!(pool_admits(&limits, session, global, &request, |_| {
            origin_active
        }));
        assert!(!pool_admits(&limits, session, global, &tunnel, |_| 0));
    }

    /// A private directory under the system temporary root, removed when the guard drops.
    #[cfg(target_os = "macos")]
    struct TestRoot(std::path::PathBuf);

    #[cfg(target_os = "macos")]
    impl TestRoot {
        fn new(tag: &str) -> Self {
            use std::os::unix::fs::DirBuilderExt as _;
            static NEXT: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
            let index = NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let path = std::env::temp_dir().join(format!(
                "cowshed-actor-{tag}-{}-{index}",
                std::process::id()
            ));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .recursive(true)
                .create(&path)
                .expect("create the actor fixture root");
            Self(path)
        }
    }

    #[cfg(target_os = "macos")]
    impl Drop for TestRoot {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_dir_all(&self.0)
                && error.kind() != std::io::ErrorKind::NotFound
            {
                eprintln!("remove actor fixture {}: {error}", self.0.display());
            }
        }
    }

    /// Records the actual actor audit events without changing its admission decisions.
    #[cfg(target_os = "macos")]
    #[derive(Default)]
    struct UnitAudit {
        events: std::sync::Mutex<Vec<crate::interfaces::AuditEvent>>,
    }

    #[cfg(target_os = "macos")]
    #[async_trait::async_trait]
    impl crate::interfaces::AuditSink for UnitAudit {
        async fn record(
            &self,
            event: crate::interfaces::AuditEvent,
        ) -> Result<(), crate::interfaces::AuditError> {
            self.events.lock().expect("unit audit").push(event);
            Ok(())
        }

        async fn flush(&self) -> Result<(), crate::interfaces::AuditError> {
            Ok(())
        }
    }

    /// The credential store for unit fixtures: every lookup finds no record.
    #[cfg(target_os = "macos")]
    struct NoCredentials;

    #[cfg(target_os = "macos")]
    #[async_trait::async_trait]
    impl crate::interfaces::CredentialProvider for NoCredentials {
        async fn lookup(
            &self,
            _query: &crate::interfaces::CredentialQuery,
        ) -> Result<Option<crate::interfaces::CredentialRecord>, crate::interfaces::CredentialError>
        {
            Ok(None)
        }
    }

    /// A self-signed CA for one fixture session, as the gateway's own tests make theirs.
    #[cfg(target_os = "macos")]
    fn fixture_ca() -> cowshed_gateway_types::WorkspaceCa {
        let key = rcgen::KeyPair::generate().expect("fixture CA key");
        let mut params = rcgen::CertificateParams::default();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "cowshed actor fixture CA");
        let certificate = params.self_signed(&key).expect("fixture CA certificate");
        cowshed_gateway_types::WorkspaceCa::new(certificate.pem(), key.serialize_pem())
            .expect("valid fixture CA")
    }

    /// The actor built the way `start_runtime` builds it, plus the brokers' tasks, which `finish`
    /// stops. The claimed loopback listeners are held until then, so no other claimant takes them.
    #[cfg(target_os = "macos")]
    struct TestActor {
        actor: Actor,
        audit: Arc<UnitAudit>,
        repo_task: JoinHandle<()>,
        sim_task: JoinHandle<()>,
        claims: Vec<Fenced<std::net::TcpListener>>,
    }

    #[cfg(target_os = "macos")]
    impl TestActor {
        async fn new(root: &TestRoot) -> Self {
            use std::os::unix::fs::DirBuilderExt as _;
            let mut config = GatewayConfig::default();
            config.mirror_cache = crate::config::MirrorCacheConfig::new(root.0.join("mirror"));
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(&config.mirror_cache.cache_root)
                .expect("create the private test mirror cache");
            config
                .validate()
                .expect("default test configuration is valid");
            let credentials: Arc<dyn crate::interfaces::CredentialProvider> =
                Arc::new(NoCredentials);
            let connector: Arc<dyn crate::interfaces::UpstreamConnector> = Arc::new(
                crate::interfaces::SystemConnector::new(
                    config.timeouts.connect,
                    config.timeouts.tls_handshake,
                )
                .expect("test connector"),
            );
            let audit = Arc::new(UnitAudit::default());
            let mirror_service = MirrorService::new(
                Cache::open(config.mirror_cache.cache_config())
                    .await
                    .expect("open the test mirror cache"),
            );
            let (completions, completion_receiver) =
                mpsc::channel(active_permit_capacity(&config.limits));
            let (cancellations, cancellation_receiver) = mpsc::channel(config.limits.global_queued);
            let (commands, receiver) = mpsc::channel(config.command_capacity.get());
            let handle = GatewayHandle {
                commands: commands.clone(),
            };
            let auditor: Arc<dyn BrokerAuditor> = Arc::new(handle.clone());
            let repo_transport: Arc<dyn RepoTransport> = Arc::new(Git2RepoTransport::new(
                config.git_helper_executable.clone(),
                Arc::clone(&connector),
                config.timeouts.connect,
                config.timeouts.response_headers,
            ));
            let (repo_mirror, repo_task) = RepoMirrorHandle::start(
                config.mirror_cache.cache_root.join("repo"),
                config.command_capacity.get(),
                Arc::clone(&credentials),
                repo_transport,
                Arc::clone(&auditor),
                config.timeouts.response_headers,
                config.timeouts.request_total,
            )
            .expect("start the test repo mirror");
            let (sim_broker, sim_task) = SimBrokerHandle::start(
                config.simulator_drop_root.clone(),
                config.command_capacity.get(),
                Arc::new(XcrunSimRunner),
                auditor,
            )
            .expect("start the test simulator broker");
            let actor = Actor::new(
                config,
                ActorChannels {
                    receiver,
                    commands,
                    completions,
                    completion_receiver,
                    cancellations,
                    cancellation_receiver,
                },
                credentials,
                connector,
                audit.clone(),
                mirror_service,
                BrokerHandles {
                    repo_mirror,
                    sim_broker,
                },
            );
            Self {
                actor,
                audit,
                repo_task,
                sim_task,
                claims: Vec::new(),
            }
        }

        /// Claims the adjacent service port first, then the session port in the same fork-lock
        /// creation step. The service claim reserves this candidate across installer processes,
        /// including the session port's release/rebind handoff, until its listener is quiesced.
        fn claim_ports(
            base: u16,
        ) -> std::io::Result<(Fenced<std::net::TcpListener>, Fenced<std::net::TcpListener>)>
        {
            fork_lock::create(|| {
                let loopback = std::net::Ipv4Addr::LOCALHOST;
                let service = std::net::TcpListener::bind((loopback, base + 1))?;
                let session = std::net::TcpListener::bind((loopback, base))?;
                Ok((session, service))
            })
            .map(|(session, service)| (Fenced::new(session), Fenced::new(service)))
        }

        fn claim_endpoint(&mut self) -> (Fenced<std::net::TcpListener>, u16) {
            if let (Some(base), Some(size)) = (
                std::env::var("COWSHED_PORT_BASE").ok(),
                std::env::var("COWSHED_PORT_BLOCK_SIZE").ok(),
            ) {
                let base: u16 = base.parse().expect("COWSHED_PORT_BASE is a port");
                let size: u16 = size.parse().expect("COWSHED_PORT_BLOCK_SIZE is a size");
                assert!(
                    size >= 4,
                    "a workspace block needs room for a 2-port session"
                );
                for offset in (2..size - 1).step_by(2) {
                    let subblock = base + offset;
                    match Self::claim_ports(subblock) {
                        Ok((session, service)) => {
                            self.claims.push(service);
                            return (session, 2);
                        }
                        Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {}
                        Err(error) => panic!("claim workspace subblock {subblock}+2: {error}"),
                    }
                }
                panic!("workspace port block {base}+{size} has no free two-port subblock");
            }
            use cowshed_gateway_types::{MACOS_PORT_MAX, MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE};
            let blocks = (MACOS_PORT_MAX - MACOS_PORT_MIN + 1) / NEW_PORT_BLOCK_SIZE;
            for index in 0..blocks {
                let base = MACOS_PORT_MIN + index * NEW_PORT_BLOCK_SIZE;
                match Self::claim_ports(base) {
                    Ok((session, service)) => {
                        self.claims.push(service);
                        return (session, NEW_PORT_BLOCK_SIZE);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => {}
                    Err(error) => panic!("claim global subblock {base}+2: {error}"),
                }
            }
            panic!("no free macOS gateway port block for the actor fixture");
        }

        /// Installs one session through the actor's own install path, and returns its encoded token.
        async fn install(
            &mut self,
            workspace_id: &str,
            repo_id: &str,
            token_byte: u8,
            revision: u64,
        ) -> String {
            let (endpoint_claim, block_size) = self.claim_endpoint();
            let address = endpoint_claim
                .local_addr()
                .expect("claimed session address");
            let token = cowshed_gateway_types::WorkspaceToken::from_bytes([token_byte; 32]);
            let encoded = token.encode();
            let session = cowshed_gateway_types::WorkspaceSession {
                workspace_id: workspace_id.to_owned(),
                repo_id: repo_id.to_owned(),
                revision,
                endpoint: cowshed_gateway_types::WorkspaceEndpoint::Tcp {
                    address,
                    block_size,
                },
                token,
                ca: fixture_ca(),
                policy: cowshed_gateway_types::WorkspacePolicy::default(),
            };
            drop(endpoint_claim);
            self.actor.install(session).await.unwrap_or_else(|error| {
                panic!(
                    "install fixture {workspace_id} at {address}: {error:?}; installed={:?}",
                    self.actor.status().sessions
                )
            });
            encoded
        }

        /// Stops both brokers, waits for their tasks, and releases the claimed endpoints.
        async fn finish(mut self) {
            for state in self.actor.nx_backends.values_mut() {
                match state {
                    NxBackendState::Starting { task } => task.abort(),
                    NxBackendState::Ready {
                        process: Some(process),
                        ..
                    } => {
                        if process.try_wait().expect("fixture child status").is_none() {
                            process.start_kill().expect("kill fixture child");
                            tokio::time::timeout(Duration::from_secs(2), process.wait())
                                .await
                                .expect("fixture reap deadline")
                                .expect("reap fixture child");
                        }
                    }
                    NxBackendState::Ready { process: None, .. } | NxBackendState::Failed { .. } => {
                    }
                }
            }
            for session in self.actor.sessions.values_mut() {
                session.quiesce().await;
            }
            self.actor.repo_mirror.shutdown().await;
            self.actor.sim_broker.shutdown().await;
            let _ = self.repo_task.await;
            let _ = self.sim_task.await;
            self.claims.clear();
        }
    }

    /// A bearer-authenticated Nx request intent for one hash.
    #[cfg(target_os = "macos")]
    fn nx_intent(method: Method, path: &str) -> RequestIntent {
        RequestIntent {
            target: RequestTarget::LocalNx,
            method,
            path: path.to_owned(),
            audit_kind: AuditKind::Http,
            trace_id: None,
            npm_mirror: false,
        }
    }

    #[cfg(target_os = "macos")]
    fn unit_backend(repo_id: &str) -> NxCacheBackend {
        NxCacheBackend {
            repo_id: repo_id.to_owned(),
            address: std::net::SocketAddr::from(([127, 0, 0, 1], 9)),
            read_token: WorkspaceToken::from_bytes([101; 32]),
            write_token: WorkspaceToken::from_bytes([102; 32]),
            reclaimed_staging_files: 3,
        }
    }

    #[cfg(target_os = "macos")]
    fn unit_starting(actor: &mut Actor, repo_id: &str) {
        actor.nx_backends.insert(
            repo_id.to_owned(),
            NxBackendState::Starting {
                task: tokio::spawn(async {}),
            },
        );
    }

    #[cfg(target_os = "macos")]
    async fn unit_child() -> tokio::process::Child {
        use cowshed_os::fork_lock::Spawn as _;
        tokio::process::Command::new("/bin/cat")
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn_locked()
            .expect("start the fixture child")
    }

    #[cfg(target_os = "macos")]
    async fn unit_queued_get(
        fixture: &mut TestActor,
        workspace: &str,
        token: String,
    ) -> (
        oneshot::Receiver<Result<Admission, AdmissionError>>,
        oneshot::Sender<()>,
    ) {
        let (reply, receive) = oneshot::channel();
        let (cancel, cancelled) = oneshot::channel();
        fixture
            .actor
            .admit(
                workspace.to_owned(),
                Authentication::Bearer(Some(token)),
                nx_intent(Method::GET, "/nx/v1/cache/42"),
                reply,
                cancelled,
            )
            .await;
        (receive, cancel)
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn nx_ready_resumes_a_queued_authenticated_get() {
        let root = TestRoot::new("ready-resume");
        let mut fixture = TestActor::new(&root).await;
        let token = fixture.install("nx-ready", "owner/nx-ready", 91, 1).await;
        unit_starting(&mut fixture.actor, "owner/nx-ready");
        let (mut receive, _cancel) = unit_queued_get(&mut fixture, "nx-ready", token).await;
        assert!(matches!(
            receive.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        fixture
            .actor
            .nx_backend_started(
                "owner/nx-ready".to_owned(),
                Ok((unit_backend("owner/nx-ready"), unit_child().await)),
            )
            .await;
        let admission = receive
            .try_recv()
            .expect("Ready answers immediately")
            .expect("the authenticated GET activates");
        assert_eq!(admission.upstream_path, "/v1/cache/42");
        assert!(admission.nx_token.is_some());
        assert_eq!(fixture.actor.nx_queue.len(), 0);
        assert_eq!(fixture.actor.global_queued, 0);
        assert_eq!(fixture.actor.sessions["nx-ready"].queued, 0);
        assert_eq!(fixture.actor.global_active.requests, 1);
        fixture.actor.complete(admission.permit_id);
        drop(admission);
        fixture.finish().await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn nx_ready_rechecks_the_saved_credential_without_a_generation_change() {
        let root = TestRoot::new("ready-recheck");
        let mut fixture = TestActor::new(&root).await;
        let token = fixture
            .install("nx-recheck", "owner/nx-recheck", 92, 1)
            .await;
        unit_starting(&mut fixture.actor, "owner/nx-recheck");
        let (mut receive, _cancel) = unit_queued_get(&mut fixture, "nx-recheck", token).await;
        // Isolate the credential predicate from the independent generation predicate.
        fixture
            .actor
            .nx_queue
            .front_mut()
            .expect("queued request")
            .authentication =
            Authentication::Bearer(Some(WorkspaceToken::from_bytes([93; 32]).encode()));
        fixture
            .actor
            .nx_backend_started(
                "owner/nx-recheck".to_owned(),
                Ok((unit_backend("owner/nx-recheck"), unit_child().await)),
            )
            .await;
        let error = receive
            .try_recv()
            .expect("Ready answers the revoked request")
            .err()
            .expect("a revoked credential must not activate");
        assert_eq!(error.audit_status, AuditStatus::Unauthorized);
        assert!(fixture.actor.permits.is_empty());
        assert_eq!(fixture.actor.global_queued, 0);
        fixture.finish().await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn nx_exited_child_restarts_and_queues_in_the_same_admission() {
        let root = TestRoot::new("exit-restart");
        let mut fixture = TestActor::new(&root).await;
        let token = fixture.install("nx-exit", "owner/nx-exit", 94, 1).await;
        let mut child = unit_child().await;
        child.start_kill().expect("kill fixture child");
        child.wait().await.expect("reap fixture child");
        fixture.actor.nx_backends.insert(
            "owner/nx-exit".to_owned(),
            NxBackendState::Ready {
                backend: unit_backend("owner/nx-exit"),
                process: Some(Box::new(child)),
            },
        );
        fixture.actor.config.nx_cache = Some(crate::nx_cache::NxCacheServiceConfig {
            executable: root.0.join("unavailable-server"),
            directory: root.0.join("nx"),
            max_bytes: 1024,
            cap_bytes: 4096,
        });
        let (mut receive, _cancel) = unit_queued_get(&mut fixture, "nx-exit", token).await;
        assert!(matches!(
            fixture.actor.nx_backends.get("owner/nx-exit"),
            Some(NxBackendState::Starting { .. })
        ));
        assert_eq!(fixture.actor.nx_queue.len(), 1);
        assert_eq!(fixture.actor.global_queued, 1);
        assert!(
            matches!(receive.try_recv(), Err(oneshot::error::TryRecvError::Empty)),
            "the exiting child's request must wait for its replacement, not be refused"
        );
        fixture.finish().await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn nx_failed_start_enforces_the_configured_cooldown() {
        let root = TestRoot::new("cooldown");
        let mut fixture = TestActor::new(&root).await;
        unit_starting(&mut fixture.actor, "owner/nx-cooldown");
        let before = std::time::Instant::now();
        fixture
            .actor
            .nx_backend_started(
                "owner/nx-cooldown".to_owned(),
                Err("failed start".to_owned()),
            )
            .await;
        let Some(NxBackendState::Failed { retry_after, .. }) =
            fixture.actor.nx_backends.get("owner/nx-cooldown")
        else {
            panic!("failure must be retained")
        };
        assert!(*retry_after >= before + fixture.actor.config.timeouts.connect);
        assert!(matches!(
            fixture.actor.nx_admission("owner/nx-cooldown").await,
            NxAdmission::Unavailable(_)
        ));
        assert!(matches!(
            fixture.actor.nx_backends.get("owner/nx-cooldown"),
            Some(NxBackendState::Failed { .. })
        ));
        fixture.finish().await;
    }

    #[tokio::test]
    async fn nx_start_supervisor_abort_drops_its_owned_worker() {
        let (held, closed) = oneshot::channel::<()>();
        let worker = tokio::spawn(async move {
            let _held = held;
            std::future::pending::<Result<(NxCacheBackend, tokio::process::Child), String>>().await
        });
        let (commands, _receive) = mpsc::channel(1);
        let task = supervise_nx_start("owner/nx-abort".to_owned(), commands, worker);
        // Abort before the reporting task's first poll as well as before readiness.
        task.abort();
        assert!(
            task.await
                .expect_err("the supervisor is cancelled")
                .is_cancelled()
        );
        assert!(
            tokio::time::timeout(Duration::from_secs(2), closed)
                .await
                .expect("aborting the supervisor must abort its worker")
                .is_err()
        );
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn nx_start_worker_panic_is_reported_and_enters_cooldown() {
        let root = TestRoot::new("start-panic");
        let mut fixture = TestActor::new(&root).await;
        let worker = tokio::spawn(async { panic!("planted startup panic") });
        let task = supervise_nx_start(
            "owner/nx-panic".to_owned(),
            fixture.actor.commands.clone(),
            worker,
        );
        fixture.actor.nx_backends.insert(
            "owner/nx-panic".to_owned(),
            NxBackendState::Starting { task },
        );
        let command = tokio::time::timeout(Duration::from_secs(2), fixture.actor.receiver.recv())
            .await
            .expect("startup panic must report promptly")
            .expect("completion command");
        let Command::NxBackendStarted { repo_id, result } = command else {
            panic!("expected startup completion")
        };
        assert!(
            result
                .as_ref()
                .expect_err("panic is a failure")
                .contains("planted startup panic")
        );
        fixture.actor.nx_backend_started(repo_id, result).await;
        assert!(matches!(
            fixture.actor.nx_admission("owner/nx-panic").await,
            NxAdmission::Unavailable(_)
        ));
        fixture.finish().await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn nx_managed_child_cannot_be_replaced_by_configuration() {
        let root = TestRoot::new("managed-config");
        let mut fixture = TestActor::new(&root).await;
        fixture.actor.nx_backends.insert(
            "owner/nx-managed".to_owned(),
            NxBackendState::Ready {
                backend: unit_backend("owner/nx-managed"),
                process: Some(Box::new(unit_child().await)),
            },
        );
        let mut replacement = unit_backend("owner/nx-managed");
        replacement.address.set_port(10);
        assert!(fixture.actor.configure_nx_cache(replacement).is_err());
        assert_eq!(
            fixture
                .actor
                .nx_backend("owner/nx-managed")
                .expect("managed backend")
                .address
                .port(),
            9
        );
        fixture.finish().await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn nx_unsupported_methods_never_start_a_backend() {
        let root = TestRoot::new("method-order");
        let mut fixture = TestActor::new(&root).await;
        let token = fixture.install("nx-method", "owner/nx-method", 95, 1).await;
        for method in [Method::HEAD, Method::POST, Method::DELETE] {
            let (reply, mut receive) = oneshot::channel();
            let (_cancel, cancelled) = oneshot::channel();
            fixture
                .actor
                .admit(
                    "nx-method".to_owned(),
                    Authentication::Bearer(Some(token.clone())),
                    nx_intent(method, "/nx/v1/cache/42"),
                    reply,
                    cancelled,
                )
                .await;
            let error = receive
                .try_recv()
                .expect("method refusal is immediate")
                .err()
                .expect("unsupported method");
            assert_eq!(error.status, StatusCode::FORBIDDEN);
            assert_eq!(error.audit_status, AuditStatus::Denied);
            assert!(fixture.actor.nx_backends.is_empty());
            assert!(fixture.actor.nx_queue.is_empty());
        }
        fixture.finish().await;
    }

    /// Stock Nx 23.2.1 reads every GET status but 200/404 as a fatal misconfiguration and retries
    /// no GET, so a full queue answers its method's miss/refusal whether the backend is starting
    /// or Ready: a 429 here ends the caller's whole run.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn nx_queue_capacity_is_a_cache_miss_not_429() {
        let root = TestRoot::new("queue-capacity");
        let mut fixture = TestActor::new(&root).await;
        for (index, ready) in [false, true].into_iter().enumerate() {
            let workspace = format!("nx-limited-{index}");
            let repo = format!("owner/nx-limited-{index}");
            let token = fixture
                .install(&workspace, &repo, 96 + index as u8, 1)
                .await;
            if ready {
                unit_ready(&mut fixture.actor, &repo);
            } else {
                unit_starting(&mut fixture.actor, &repo);
            }
            // The backend's origin pool and the global queue are both full: a Ready request can
            // neither activate nor wait.
            let origin = (workspace.clone(), unit_backend(&repo).target().origin());
            let origin_active = fixture.actor.config.limits.origin_active;
            fixture.actor.origins.insert(origin.clone(), origin_active);
            fixture.actor.global_queued = fixture.actor.config.limits.global_queued;
            let (mut receive, _cancel) = unit_queued_get(&mut fixture, &workspace, token).await;
            let error = receive
                .try_recv()
                .expect("capacity refusal is immediate")
                .err()
                .expect("queue capacity");
            assert_eq!(error.status, StatusCode::NOT_FOUND, "ready={ready}");
            assert_eq!(error.audit_status, AuditStatus::Limited);
            assert!(fixture.actor.nx_queue.is_empty());
            assert!(fixture.actor.queue.is_empty());
            assert!(unit_audited(
                &fixture,
                &workspace,
                404,
                AuditStatus::Limited,
                "nx-queue-limited"
            ));
            let writer = WorkspaceToken::from_bytes([110 + index as u8; 32]);
            let encoded = writer.encode();
            fixture.actor.nx_publications.insert(
                workspace.clone(),
                NxPublication {
                    workspace_id: workspace.clone(),
                    repo_id: repo.clone(),
                    expected_revision: 1,
                    accepted_head: "a".repeat(40),
                    hashes: std::collections::BTreeSet::from(["42".to_owned()]),
                    token: writer,
                },
            );
            let (reply, mut receive) = oneshot::channel();
            let (_cancel, cancelled) = oneshot::channel();
            fixture
                .actor
                .admit(
                    workspace.clone(),
                    Authentication::Bearer(Some(encoded)),
                    nx_intent(Method::PUT, "/nx/v1/cache/42"),
                    reply,
                    cancelled,
                )
                .await;
            let error = receive
                .try_recv()
                .expect("writer capacity refusal")
                .err()
                .expect("capacity is bounded");
            assert_eq!(error.status, StatusCode::FORBIDDEN, "ready={ready}");
            assert_eq!(error.audit_status, AuditStatus::Limited);
            assert!(unit_audited(
                &fixture,
                &workspace,
                403,
                AuditStatus::Limited,
                "nx-queue-limited"
            ));
            fixture.actor.global_queued = 0;
            fixture.actor.origins.remove(&origin);
        }
        fixture.finish().await;
    }

    /// A Ready backend's request that waits out its queue deadline is a miss, not a 504 the
    /// stock client cannot survive; the startup queue already answers its deadline this way.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn nx_ready_queue_deadline_is_a_cache_miss_not_504() {
        let root = TestRoot::new("ready-deadline");
        let mut fixture = TestActor::new(&root).await;
        let token = fixture
            .install("nx-deadline", "owner/nx-deadline", 98, 1)
            .await;
        unit_ready(&mut fixture.actor, "owner/nx-deadline");
        let origin = unit_backend("owner/nx-deadline").target().origin();
        let origin_active = fixture.actor.config.limits.origin_active;
        fixture
            .actor
            .origins
            .insert(("nx-deadline".to_owned(), origin.clone()), origin_active);
        let (mut receive, _cancel) = unit_queued_get(&mut fixture, "nx-deadline", token).await;
        assert!(matches!(
            receive.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        let queue_id = fixture.actor.queue.front().expect("queued Nx GET").queue_id;
        fixture.actor.expire_queued(queue_id).await;
        let error = receive
            .try_recv()
            .expect("deadline refusal")
            .err()
            .expect("queue deadline");
        assert_eq!(error.status, StatusCode::NOT_FOUND);
        assert_eq!(error.audit_status, AuditStatus::TimedOut);
        assert!(unit_audited(
            &fixture,
            "nx-deadline",
            404,
            AuditStatus::TimedOut,
            "queue-timeout"
        ));
        assert!(fixture.actor.queue.is_empty());
        assert_eq!(fixture.actor.global_queued, 0);
        assert_eq!(fixture.actor.sessions["nx-deadline"].queued, 0);
        fixture
            .actor
            .origins
            .remove(&("nx-deadline".to_owned(), origin));
        fixture.finish().await;
    }

    #[cfg(target_os = "macos")]
    fn unit_ready(actor: &mut Actor, repo_id: &str) {
        actor.nx_backends.insert(
            repo_id.to_owned(),
            NxBackendState::Ready {
                backend: unit_backend(repo_id),
                process: None,
            },
        );
    }

    /// Whether the audit trail holds one refusal of this workspace with exactly this outcome.
    #[cfg(target_os = "macos")]
    fn unit_audited(
        fixture: &TestActor,
        workspace: &str,
        http_status: u16,
        status: AuditStatus,
        classification: &str,
    ) -> bool {
        fixture
            .audit
            .events
            .lock()
            .expect("unit audit")
            .iter()
            .any(|event| {
                event.workspace_id == workspace
                    && event.http_status == Some(http_status)
                    && event.status == status
                    && event.classification.as_deref() == Some(classification)
            })
    }

    /// A failed single-flight start refuses only its own waiters. Another project's authenticated
    /// GET stays queued through that failure, then activates when its own backend becomes Ready.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn nx_single_flight_shares_one_start_and_leaves_other_repos_responsive() {
        let root = TestRoot::new("single-flight");
        let mut fixture = TestActor::new(&root).await;
        let encoded = fixture.install("nx-unit-a", "owner/nx-unit", 91, 1).await;
        let other_encoded = fixture
            .install("nx-unit-other", "owner/nx-other", 92, 1)
            .await;
        let actor = &mut fixture.actor;

        // The one start in flight, held by a task that finishes only when released.
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        let task = tokio::spawn(async move {
            let _ = gate.await;
        });
        actor.nx_backends.insert(
            "owner/nx-unit".to_owned(),
            NxBackendState::Starting { task },
        );

        unit_starting(actor, "owner/nx-other");
        let (other_reply, mut other_receive) = oneshot::channel();
        let (_other_cancel, other_cancelled) = oneshot::channel();
        actor
            .admit(
                "nx-unit-other".to_owned(),
                Authentication::Bearer(Some(other_encoded)),
                nx_intent(Method::GET, "/nx/v1/cache/44"),
                other_reply,
                other_cancelled,
            )
            .await;

        let mut receivers = Vec::new();
        let mut cancels = Vec::new();
        for hash in ["42", "43"] {
            let (reply, receive) = tokio::sync::oneshot::channel();
            let (cancel, cancelled) = tokio::sync::oneshot::channel::<()>();
            actor
                .admit(
                    "nx-unit-a".to_owned(),
                    Authentication::Bearer(Some(encoded.clone())),
                    nx_intent(Method::GET, &format!("/nx/v1/cache/{hash}")),
                    reply,
                    cancelled,
                )
                .await;
            receivers.push(receive);
            cancels.push(cancel);
        }
        // Two same-repo requests share one start; the other project's start and charge are separate.
        assert_eq!(actor.nx_queue.len(), 3);
        assert_eq!(actor.global_queued, 3);
        assert_eq!(actor.sessions["nx-unit-a"].queued, 2);
        assert!(matches!(
            actor.nx_backends.get("owner/nx-unit"),
            Some(NxBackendState::Starting { .. })
        ));

        // Release the start's completion: it fails, and every waiter is refused once as an outage.
        let _ = release.send(());
        actor
            .nx_backend_started(
                "owner/nx-unit".to_owned(),
                Err("unit start failure".to_owned()),
            )
            .await;
        for receive in receivers {
            let refused = receive.await.expect("each waiter is answered");
            let error = refused.err().expect("a failed start refuses the waiter");
            assert_eq!(error.status, StatusCode::NOT_FOUND);
            assert_eq!(error.audit_status, AuditStatus::Failed);
        }
        assert!(matches!(
            other_receive.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(actor.nx_queue.len(), 1);
        assert_eq!(actor.global_queued, 1);
        assert_eq!(actor.sessions["nx-unit-a"].queued, 0);
        assert_eq!(actor.sessions["nx-unit-other"].queued, 1);
        actor
            .nx_backend_started(
                "owner/nx-other".to_owned(),
                Ok((unit_backend("owner/nx-other"), unit_child().await)),
            )
            .await;
        let other = other_receive
            .try_recv()
            .expect("the other project's Ready transition answers its waiter")
            .expect("the other project's authenticated GET activates");
        assert_eq!(other.upstream_path, "/v1/cache/44");
        assert!(other.nx_token.is_some());
        assert_eq!(actor.nx_queue.len(), 0);
        assert_eq!(actor.global_queued, 0);
        assert_eq!(actor.sessions["nx-unit-other"].queued, 0);
        actor.complete(other.permit_id);
        drop(other);
        drop(cancels);
        fixture.finish().await;
    }

    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn nx_ready_resumes_only_its_project_waiters() {
        let root = TestRoot::new("ready-isolation");
        let mut fixture = TestActor::new(&root).await;
        let first_token = fixture.install("nx-first", "owner/nx-first", 91, 1).await;
        let other_token = fixture.install("nx-other", "owner/nx-other", 92, 1).await;
        unit_starting(&mut fixture.actor, "owner/nx-first");
        unit_starting(&mut fixture.actor, "owner/nx-other");
        let (mut first_receive, _first_cancel) =
            unit_queued_get(&mut fixture, "nx-first", first_token).await;
        let (mut other_receive, _other_cancel) =
            unit_queued_get(&mut fixture, "nx-other", other_token).await;
        fixture
            .actor
            .nx_backend_started(
                "owner/nx-first".to_owned(),
                Ok((unit_backend("owner/nx-first"), unit_child().await)),
            )
            .await;
        let first = first_receive
            .try_recv()
            .expect("Ready answers its own project")
            .expect("the first project's authenticated GET activates");
        assert!(matches!(
            other_receive.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        assert_eq!(fixture.actor.nx_queue.len(), 1);
        assert_eq!(fixture.actor.global_queued, 1);
        assert_eq!(fixture.actor.sessions["nx-first"].queued, 0);
        assert_eq!(fixture.actor.sessions["nx-other"].queued, 1);
        fixture.actor.complete(first.permit_id);
        drop(first);
        fixture
            .actor
            .nx_backend_started(
                "owner/nx-other".to_owned(),
                Err("other start failed".to_owned()),
            )
            .await;
        assert!(
            other_receive
                .try_recv()
                .expect("the other failure is answered")
                .is_err()
        );
        assert_eq!(fixture.actor.nx_queue.len(), 0);
        assert_eq!(fixture.actor.global_queued, 0);
        assert_eq!(fixture.actor.sessions["nx-other"].queued, 0);
        fixture.finish().await;
    }

    /// Queue end revokes only the publication's queued PUTs. Each ordinary queued request and each
    /// queued Nx GET stays, and the shared counters count exactly what remains.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn queue_end_cancels_only_queued_nx_puts() {
        let root = TestRoot::new("queue-end");
        let mut fixture = TestActor::new(&root).await;
        fixture.install("nx-unit-q", "owner/nx-queue", 93, 1).await;
        let actor = &mut fixture.actor;

        let generation = actor.sessions["nx-unit-q"].generation;
        let (put_reply, put_receive) = tokio::sync::oneshot::channel();
        let (get_reply, _get_receive) = tokio::sync::oneshot::channel();
        actor.nx_queue.push_back(NxPending {
            queue_id: 1,
            workspace_id: "nx-unit-q".to_owned(),
            repo_id: "owner/nx-queue".to_owned(),
            generation,
            authentication: Authentication::Bearer(None),
            intent: nx_intent(Method::PUT, "/nx/v1/cache/42"),
            reply: put_reply,
            timer: None,
            cancellation: None,
        });
        actor.nx_queue.push_back(NxPending {
            queue_id: 2,
            workspace_id: "nx-unit-q".to_owned(),
            repo_id: "owner/nx-queue".to_owned(),
            generation,
            authentication: Authentication::Bearer(None),
            intent: nx_intent(Method::GET, "/nx/v1/cache/42"),
            reply: get_reply,
            timer: None,
            cancellation: None,
        });
        let (generic_reply, _generic_receive) = tokio::sync::oneshot::channel();
        actor.queue.push_back(Pending {
            queue_id: 3,
            seed: seed(Method::GET, EgressMode::Intercept),
            reply: generic_reply,
            timer: None,
            cancellation: None,
            nx_put: false,
        });
        let (ordinary_put_reply, mut ordinary_put_receive) = oneshot::channel();
        let mut put_seed = seed(Method::PUT, EgressMode::Intercept);
        put_seed.workspace_id = "nx-unit-q".to_owned();
        actor.queue.push_back(Pending {
            queue_id: 4,
            seed: put_seed,
            reply: ordinary_put_reply,
            timer: None,
            cancellation: None,
            nx_put: true,
        });
        actor.sessions.get_mut("nx-unit-q").expect("session").queued = 3;
        actor.global_queued = 4;

        actor.cancel_nx_writes("nx-unit-q").await;

        let refused = put_receive.await.expect("the queued PUT is answered");
        let error = refused.err().expect("ended publication refuses its PUT");
        assert_eq!(error.status, StatusCode::FORBIDDEN);
        let ordinary_error = ordinary_put_receive
            .try_recv()
            .expect("the ordinary queued PUT is answered")
            .err()
            .expect("ended publication refuses an ordinary queued PUT");
        assert_eq!(ordinary_error.status, StatusCode::FORBIDDEN);
        assert_eq!(actor.nx_queue.len(), 1, "the queued Nx GET stays");
        assert_eq!(actor.queue.len(), 1, "the ordinary queued request stays");
        assert_eq!(actor.global_queued, 2);
        assert_eq!(actor.sessions["nx-unit-q"].queued, 1);
        fixture.finish().await;
    }

    #[test]
    fn completion_capacity_covers_both_full_pools() {
        let limits = GatewayLimits::default();
        let full = ActiveCounts {
            tunnels: limits.global_active,
            requests: limits.global_active,
        };
        assert_eq!(active_permit_capacity(&limits), full.total());
        let oversized = GatewayLimits {
            global_active: tokio::sync::Semaphore::MAX_PERMITS / 2 + 1,
            ..limits
        };
        assert!(oversized.validate().is_err());
    }
}
