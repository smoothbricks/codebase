use std::{
    collections::HashMap,
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
    admission::{
        Admitted, PermitClass, PermitId, Pool, QueueId, Queued, Refusal, Unadmitted,
        active_permit_capacity,
    },
    cache::{Cache, CacheError},
    config::{ControlTcpConfig, GatewayConfig},
    control::ControlError,
    interfaces::{
        AuditError, AuditEvent, AuditKind, AuditSink, AuditStatus, ConnectError, CredentialError,
        CredentialProvider, SystemConnector, UpstreamConnector,
    },
    mirror::{MirrorCacheStatus, MirrorService},
    nx_cache::{NxCacheBackend, NxCacheClearRefusal},
    nx_lifecycle::{Handle, Installed, NxAdmission, NxBackendState, NxBackends, Released},
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

    /// Clear the project's Nx accepted-task store, answering the archives deleted (03_caches.md,
    /// "Nx accepted-task artifact service"). Refused while the project's Nx work is in flight.
    pub async fn clear_nx_cache(&self, repo_id: String) -> Result<u64, GatewayError> {
        let (reply, receive) = oneshot::channel();
        self.send(Command::ClearNxCache { repo_id, reply }).await?;
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

    /// Whether this exact workspace revision is installed now. Like status, this is an
    /// observation, not a reservation against a later install or removal.
    pub async fn session_current(
        &self,
        workspace_id: impl Into<String>,
        revision: u64,
    ) -> Result<bool, GatewayError> {
        let (reply, receive) = oneshot::channel();
        self.send(Command::SessionCurrent {
            workspace_id: workspace_id.into(),
            revision,
            reply,
        })
        .await?;
        receive.await.map_err(|_| GatewayError::Stopped)
    }

    /// Close every listener an install carried across the restart that no restored session
    /// claimed; how many. The daemon calls this once its startup pass has restored every session
    /// (05_gateway.md "Startup contract" step 4): a connection queued on a listener nobody will
    /// accept would wait forever, so it is reset instead, as it was when nothing was carried.
    /// One round trip through the actor, for a control connection's heartbeat (05_gateway.md,
    /// "Deadlines"): an actor that stopped turning answers no ping, and the heartbeat stops.
    pub async fn ping(&self) -> Result<(), GatewayError> {
        let (reply, receive) = oneshot::channel();
        self.send(Command::Ping { reply }).await?;
        receive.await.map_err(|_| GatewayError::Stopped)
    }

    pub async fn release_unclaimed_listeners(&self) -> Result<usize, GatewayError> {
        let (reply, receive) = oneshot::channel();
        self.send(Command::ReleaseUnclaimedListeners { reply })
            .await?;
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
    /// The released-image cleanup, once [`Gateway::clean_up_released_images`] started it.
    cleanup: Arc<std::sync::OnceLock<crate::cleanup::ImageCleanup>>,
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
        // Before anything binds: a session restored later claims its carried listener rather
        // than racing the connections queued on it with a bind of its own.
        let carried = take_carried_listeners(&config).await;
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
        let state = Actor {
            carried,
            ..Actor::new(
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
            )
        };
        let actor = tokio::spawn(state.run());
        let cleanup = Arc::new(std::sync::OnceLock::new());
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
                        ram_disks: config.ram_disks.clone(),
                        ram_leases: crate::ram_lease::Records::beside(path),
                        first_launch: config.first_launch.clone(),
                        disk_leases: crate::disk_lease::DiskLeases::start(config.disk_lease),
                        task_server: crate::task_server::TaskServer::start(config.task_server),
                        cleanup: Arc::clone(&cleanup),
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
            // Not `request_total` (15 min): launchd SIGKILLs a draining daemon at its agent's
            // `ExitTimeOut`, so a drain past that bound is cut mid-way instead of finishing.
            drain_timeout: cowshed_gateway_types::GATEWAY_DRAIN_WITHIN,
            repo_mirror,
            sim_broker,
            repo_mirror_task: Some(repo_mirror_task),
            sim_broker_task: Some(sim_broker_task),
            cleanup,
        })
    }

    /// Start freeing the released images of the store at `store` (05_gateway.md, "Deleting
    /// released images"): one thread lists the store's `.deleting` directories and frees what they
    /// hold, one at a time, then lists again at every nudge. The handle is published before the
    /// thread's first listing, so no nudge it refuses names an entry that listing missed. A second
    /// call starts nothing.
    pub fn clean_up_released_images(&self, store: std::path::PathBuf) -> std::io::Result<()> {
        let (cleanup, heard) = crate::cleanup::ImageCleanup::new();
        if self.cleanup.set(cleanup).is_err() {
            return Ok(());
        }
        crate::cleanup::ImageCleanup::start(store, heard)
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
    ClearNxCache {
        repo_id: String,
        reply: oneshot::Sender<Result<u64, GatewayError>>,
    },
    NxCacheCleared {
        repo_id: String,
        result: Result<u64, NxCacheClearRefusal>,
        reply: oneshot::Sender<Result<u64, GatewayError>>,
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
    MintLeaf {
        workspace_id: String,
        generation: u64,
        host: String,
        reply: oneshot::Sender<Result<Arc<rustls::ServerConfig>, GatewayError>>,
    },
    SessionCurrent {
        workspace_id: String,
        revision: u64,
        reply: oneshot::Sender<bool>,
    },
    Status {
        reply: oneshot::Sender<DaemonStatus>,
    },
    BeginDrain {
        reply: oneshot::Sender<()>,
    },
    ReleaseUnclaimedListeners {
        reply: oneshot::Sender<usize>,
    },
    /// Answer at once: a control connection's heartbeat proves the actor still turns.
    Ping {
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

impl RequestIntent {
    /// The status this request hears when the gateway refuses it. The Nx cache route never blocks
    /// a build (05_gateway.md "`/nx/v1/cache/{hash}`"): every refusal there is its method's miss or
    /// not-stored answer, because stock Nx fails a task on any other status. Other routes keep
    /// `otherwise`.
    pub(crate) fn refusal_status(&self, otherwise: StatusCode) -> StatusCode {
        match self.target {
            RequestTarget::LocalNx => crate::nx_cache::unavailable_status(&self.method),
            RequestTarget::Generic(_) | RequestTarget::LocalSim => otherwise,
        }
    }
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
    /// Whether this request is the project Nx cache route's.
    pub nx: bool,
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

impl Admission {
    /// `RequestIntent::refusal_status` for an admitted request.
    pub(crate) fn refusal_status(&self, otherwise: StatusCode) -> StatusCode {
        if self.nx {
            crate::nx_cache::unavailable_status(&self.method)
        } else {
            otherwise
        }
    }
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
    nx: bool,
    audit_kind: AuditKind,
    method: Method,
    request_path: String,
    upstream_path: String,
    trace_id: Option<String>,
}

impl AdmissionSeed {
    /// `RequestIntent::refusal_status` for a queued request.
    fn refusal_status(&self, otherwise: StatusCode) -> StatusCode {
        if self.nx {
            crate::nx_cache::unavailable_status(&self.method)
        } else {
            otherwise
        }
    }

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
            nx: self.nx,
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

/// A request waiting for a permit: its seed and its reply.
struct Pending {
    seed: AdmissionSeed,
    reply: oneshot::Sender<Result<Admission, AdmissionError>>,
}

/// A request waiting for its project's standalone Nx backend to start. It keeps the original
/// request rather than an admission seed, since the backend that seed needs does not exist yet.
struct NxPending {
    workspace_id: String,
    intent: RequestIntent,
    reply: oneshot::Sender<Result<Admission, AdmissionError>>,
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

/// A project's backend start task, the one that reports it ([`supervise_nx_start`]), or a child
/// the gateway started for a project: what the Nx lifecycle holds for the actor and hands back.
type NxHandle = Handle<JoinHandle<()>, Box<tokio::process::Child>>;

/// Ends what the Nx lifecycle handed back. A start's task is aborted, its worker with it, because
/// dropping a `JoinHandle` does not abort it; a started child is dropped, which kills it
/// (`kill_on_drop`).
fn retire_nx(handle: NxHandle) {
    match handle {
        Handle::Start(task) => task.abort(),
        Handle::Child(process) => drop(process),
    }
}

/// The clock the actor's admission decisions read: tokio's, which its queue timer sleeps on.
fn now() -> std::time::Instant {
    tokio::time::Instant::now().into_std()
}

/// Whether `authentication` proves a request is the session's whose `token` and `generation` it
/// presents. The selected listener or socket already named the workspace, and with it the
/// project, so the Nx cache route takes no credential (05_gateway.md "`/nx/v1/cache/{hash}`");
/// a tunnel's generation never admits that route.
fn authenticated(
    authentication: &Authentication,
    token: &WorkspaceToken,
    generation: u64,
    nx: bool,
) -> bool {
    match authentication {
        Authentication::Bearer(_) if nx => true,
        Authentication::Bearer(Some(presented)) => token.matches_encoded(presented),
        Authentication::Bearer(None) => false,
        Authentication::Generation(presented) => *presented == generation && !nx,
    }
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
    accept_stop: watch::Sender<bool>,
    connection_stop: watch::Sender<bool>,
    audit_stop: watch::Sender<bool>,
    listener_task: JoinHandle<()>,
    /// A duplicate of a TCP session listener, which a draining gateway gives the install
    /// restarting it ([`Actor::give_listeners`]). Closed with the listener, in [`Self::quiesce`]:
    /// a copy that outlived it would keep the port listening for nobody, or refuse a rebind.
    handoff: Option<Fenced<std::os::fd::OwnedFd>>,
}

impl SessionState {
    async fn quiesce(&mut self) {
        let _ = self.accept_stop.send(true);
        let _ = self.connection_stop.send(true);
        let _ = self.audit_stop.send(true);
        let _ = (&mut self.listener_task).await;
        self.handoff = None;
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
    /// Every request's admission: permits, waiters and their deadlines.
    pool: Pool<Pending, NxPending>,
    /// Every project's Nx cache backend.
    nx: NxBackends<JoinHandle<()>, Box<tokio::process::Child>>,
    revisions: HashMap<String, u64>,
    next_generation: u64,
    next_audit: u64,
    leaf_cache: LeafCache,
    draining: bool,
    /// The drain's bound on the exit of the Nx backends it signalled: the reap class
    /// (05_gateway.md "Deadlines"), injected so a test passes a short one the same way.
    nx_backend_reap: cowshed_os::deadline::Bound,
    drain_reply: Option<oneshot::Sender<()>>,
    audit_failure: Option<AuditError>,
    /// The workspace listeners an install carried across the restart, by address, until a
    /// restored session claims each ([`Actor::bind_endpoint`]). Fenced: a peer observes their
    /// release, since the connections queued on one are reset when it closes.
    carried: HashMap<std::net::SocketAddr, Fenced<TcpListener>>,
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
            pool: Pool::new(config.limits, config.timeouts.request_total),
            nx: NxBackends::new(config.timeouts.connect),
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
            revisions: HashMap::new(),
            next_generation: 1,
            next_audit: 1,
            draining: false,
            nx_backend_reap: cowshed_os::deadline::REAP,
            drain_reply: None,
            audit_failure: None,
            carried: HashMap::new(),
        }
    }

    async fn run(mut self) -> Result<(), GatewayError> {
        loop {
            // The one queue timer: it wakes the actor at the earliest wait's end, read afresh
            // each turn, and the pool decides which waits ended.
            let deadline = self.pool.deadline();
            let expiry = async move {
                match deadline {
                    Some(deadline) => {
                        tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
                    }
                    None => std::future::pending().await,
                }
            };
            tokio::select! {
                Some(completion) = self.completion_receiver.recv() => {
                    self.record(completion.draft).await;
                    self.complete(completion.permit_id);
                }
                Some(queue_id) = self.cancellation_receiver.recv() => {
                    self.cancel_queued(queue_id).await;
                }
                () = expiry => self.expire_queued().await,
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
                        Command::ClearNxCache { repo_id, reply } => {
                            self.clear_nx_cache(repo_id, reply);
                        }
                        Command::NxCacheCleared { repo_id, result, reply } => {
                            self.nx_cache_cleared(&repo_id, result, reply);
                        }
                        Command::Install { session, reply } => {
                            let result = self.install(session).await;
                            let _ = reply.send(result);
                        }
                        Command::Remove {
                            workspace_id,
                            expected_revision,
                            reply,
                        } => {
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
                        Command::MintLeaf {
                            workspace_id,
                            generation,
                            host,
                            reply,
                        } => {
                            let result = self.mint_leaf(&workspace_id, generation, &host);
                            let _ = reply.send(result);
                        }
                        Command::SessionCurrent { workspace_id, revision, reply } => {
                            let current = self.sessions.get(&workspace_id)
                                .is_some_and(|session| session.revision == revision);
                            let _ = reply.send(current);
                        }
                        Command::Status { reply } => {
                            let _ = reply.send(self.status());
                        }
                        Command::Ping { reply } => {
                            let _ = reply.send(());
                        }
                        Command::ReleaseUnclaimedListeners { reply } => {
                            let released = self.carried.len();
                            self.carried.clear();
                            let _ = reply.send(released);
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
            match self.bind_endpoint(&session.endpoint) {
                Ok(listener) => listener,
                Err(error) => {
                    if let Some(mut current) = previous.take() {
                        let restored = self.bind_endpoint(&current.endpoint)?;
                        self.attach_listener(&workspace_id, &mut current, restored);
                        self.sessions.insert(workspace_id, current);
                    }
                    return Err(error);
                }
            }
        } else {
            match self.bind_endpoint(&session.endpoint) {
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
            self.refuse_rotated(&workspace_id).await;
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
        let (listener_task, handoff) = self.spawn_listener(
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
                accept_stop,
                connection_stop,
                audit_stop,
                listener_task,
                handoff,
            },
        );
        self.pool.install(&workspace_id, generation);
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
        let repo_id = session.repo_id.clone();
        session.stop().await;
        self.repo_mirror
            .unbind_session(workspace_id.to_owned())
            .await?;
        self.sim_broker
            .unbind_session(workspace_id.to_owned())
            .await?;
        self.leaf_cache.drop_workspace(workspace_id);
        self.refuse_rotated(workspace_id).await;
        self.release_nx_backend(&repo_id);
        Ok(())
    }

    /// Stops the Nx cache server the gateway started for `repo_id` once no session of that
    /// project is left: the server exists for the project's workspaces, so it ends with the last
    /// of them (05_gateway.md "`/nx/v1/cache/{hash}`"). A backend still starting is aborted; a
    /// running one's child is killed as it drops. A backend the gateway was handed, not started,
    /// owns no process and stays. Measured 2026-10-19: every scratch project a test adopted and
    /// discarded left its server running until the gateway restarted, seven of nine at once.
    fn release_nx_backend(&mut self, repo_id: &str) {
        if self
            .sessions
            .values()
            .any(|session| session.repo_id == repo_id)
        {
            return;
        }
        match self.nx.release(repo_id) {
            Released::Kept => {}
            Released::Forgot(handle) => {
                cowshed_os::eprintln!(
                    "cowshed: Nx cache backend for {repo_id} stopped: its project has no session left"
                );
                if let Some(handle) = handle {
                    retire_nx(handle);
                }
            }
        }
    }

    /// Serve `listener` for `workspace_id`; the task, and for a TCP listener a duplicate a
    /// draining gateway can give an install ([`SessionState::handoff`]). A duplicate the kernel
    /// refuses (no descriptor left) leaves that one listener uncarried, and says so: across a
    /// restart it is refused as before the handoff existed.
    fn spawn_listener(
        &self,
        workspace_id: String,
        listener: BoundListener,
        accept_rx: watch::Receiver<bool>,
        connection_rx: watch::Receiver<bool>,
        audit_rx: watch::Receiver<bool>,
    ) -> (JoinHandle<()>, Option<Fenced<std::os::fd::OwnedFd>>) {
        let handoff = match &listener {
            BoundListener::Tcp(tcp) => {
                use std::os::fd::AsFd as _;
                match tcp.as_fd().try_clone_to_owned() {
                    Ok(copy) => Some(Fenced::new(copy)),
                    Err(error) => {
                        cowshed_os::eprintln!(
                            "cowshed: workspace {workspace_id}'s listener is not carried across a \
                         gateway restart: duplicating it failed: {error}"
                        );
                        None
                    }
                }
            }
            BoundListener::Unix(_) => None,
        };
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
        let task = tokio::spawn(proxy::accept_loop(
            listener,
            context,
            accept_rx,
            connection_rx,
            audit_rx,
        ));
        (task, handoff)
    }

    /// A session's listener: the one an install carried across the restart for its address,
    /// when there is one, so the connections queued on it meanwhile are served; else a fresh
    /// bind (05_gateway.md "An install never refuses a client connection").
    fn bind_endpoint(
        &mut self,
        endpoint: &WorkspaceEndpoint,
    ) -> Result<BoundListener, GatewayError> {
        if let WorkspaceEndpoint::Tcp { address, .. } = endpoint
            && let Some(carried) = self.carried.remove(address)
        {
            return Ok(BoundListener::Tcp(carried));
        }
        bind_endpoint(endpoint)
    }

    /// As a drain begins, give the install restarting this gateway a duplicate of every TCP
    /// workspace listener, while this gateway still holds them: from then on the install's
    /// copies keep each port listening until the next gateway takes them. Nothing is given when
    /// no install serves the handoff socket, and a failed give is said and changes nothing else:
    /// the restart then refuses connections as it did before.
    #[cfg(target_os = "macos")]
    async fn give_listeners(&self) {
        let Some(path) = self.config.listener_handoff.clone() else {
            return;
        };
        let path_for_message = path.clone();
        let mut listeners = Vec::new();
        for session in self.sessions.values() {
            let (WorkspaceEndpoint::Tcp { address, .. }, Some(copy)) =
                (&session.endpoint, &session.handoff)
            else {
                continue;
            };
            use std::os::fd::AsFd as _;
            match copy.as_fd().try_clone_to_owned() {
                Ok(copy) => listeners.push((*address, copy)),
                Err(error) => cowshed_os::eprintln!(
                    "cowshed: the listener at {address} is not carried across this restart: {error}"
                ),
            }
        }
        let carried = listeners.len();
        let given = tokio::task::spawn_blocking(move || {
            use std::os::fd::AsFd as _;
            let borrowed: Vec<_> = listeners
                .iter()
                .map(|(address, copy)| (*address, copy.as_fd()))
                .collect();
            crate::listener_handoff::give(&path, &borrowed)
        })
        .await;
        match given {
            Ok(Ok(0)) if carried == 0 => {}
            // A stop, or a start onto a gateway nothing replaces: no install served the handoff,
            // so the workspace listeners close with this gateway, as designed. Said, so the
            // refused connections that follow read as that, not as a failed handoff.
            Ok(Ok(0)) => cowshed_os::eprintln!(
                "cowshed: {carried} workspace listener(s) not carried across this restart: no \
             install was serving the handoff socket {}; their clients are refused until the \
             next gateway binds",
                path_for_message.display()
            ),
            Ok(Ok(given)) => cowshed_os::eprintln!(
                "cowshed: gave {given} workspace listener(s) to the install restarting this gateway"
            ),
            Ok(Err(error)) => cowshed_os::eprintln!(
                "cowshed: could not give the workspace listeners to the install restarting this \
             gateway; its clients are refused until the next gateway binds: {error}"
            ),
            Err(error) => {
                cowshed_os::eprintln!("cowshed: the listener handoff task failed: {error}")
            }
        }
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
        let (listener_task, handoff) = self.spawn_listener(
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
        session.handoff = handoff;
    }

    fn configure_nx_cache(&mut self, backend: NxCacheBackend) -> Result<(), GatewayError> {
        let displaced = self
            .nx
            .configure(backend)
            .map_err(|reason| GatewayError::NxBackend(reason.to_owned()))?;
        if let Some(handle) = displaced {
            retire_nx(handle);
        }
        Ok(())
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
        if let Some(displaced) = self.nx.starting(repo_id, task) {
            retire_nx(displaced);
        }
        Ok(())
    }

    /// Whether the child the gateway started for `repo_id` has exited, as the kernel reports it
    /// now. A status that cannot be read counts as exited, so the request starts a replacement.
    fn nx_child_exited(&mut self, repo_id: &str) -> bool {
        let Some(process) = self.nx.managed_child(repo_id) else {
            return false;
        };
        match process.try_wait() {
            Ok(None) => false,
            Ok(Some(status)) => {
                cowshed_os::eprintln!(
                    "cowshed: Nx cache backend for {repo_id} exited {status}; starting a replacement"
                );
                true
            }
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: cannot inspect the Nx cache backend for {repo_id}: {error}; starting a replacement"
                );
                true
            }
        }
    }

    /// Clear `repo_id`'s accepted-task store (03_caches.md, "Nx accepted-task artifact service").
    /// Refused, with what is running named, while the project's Nx work is in flight, its backend
    /// is starting, or the host configured that backend. Otherwise the backend stops, the store is
    /// emptied under its own lease, and the next Nx request starts a fresh backend; requests
    /// meanwhile are misses, as in any backend outage. The work runs in its own task and reports
    /// through `NxCacheCleared`, so the actor never waits on it.
    fn clear_nx_cache(
        &mut self,
        repo_id: String,
        reply: oneshot::Sender<Result<u64, GatewayError>>,
    ) {
        if self.draining {
            let _ = reply.send(Err(GatewayError::Draining));
            return;
        }
        let Some(directory) = self
            .config
            .nx_cache
            .as_ref()
            .map(|config| config.directory.clone())
        else {
            let _ = reply.send(Err(NxCacheClearRefusal::Unconfigured.into()));
            return;
        };
        let process = match self.nx.clear(&repo_id, self.pool.nx_use(&repo_id)) {
            Ok(process) => process,
            Err(refusal) => {
                let _ = reply.send(Err(refusal.into()));
                return;
            }
        };
        let store = crate::nx_cache::project_store(&directory, &repo_id);
        let commands = self.commands.clone();
        tokio::spawn(async move {
            let result = async {
                if let Some(process) = process {
                    crate::nx_cache::stop_backend(&repo_id, *process).await?;
                }
                tokio::task::spawn_blocking(move || crate::nx_cache::clear_store(&store))
                    .await
                    .map_err(|error| {
                        NxCacheClearRefusal::Failed(format!(
                            "the Nx cache store clear task failed: {error}"
                        ))
                    })?
            }
            .await;
            let _ = commands
                .send(Command::NxCacheCleared {
                    repo_id,
                    result,
                    reply,
                })
                .await;
        });
    }

    /// The end of a clear: the project has no backend until its next Nx request starts one.
    fn nx_cache_cleared(
        &mut self,
        repo_id: &str,
        result: Result<u64, NxCacheClearRefusal>,
        reply: oneshot::Sender<Result<u64, GatewayError>>,
    ) {
        self.nx.cleared(repo_id);
        match &result {
            Ok(entries) => {
                cowshed_os::eprintln!(
                    "cowshed: Nx cache store for {repo_id} cleared: {entries} entries deleted"
                );
            }
            Err(refusal) => {
                cowshed_os::eprintln!(
                    "cowshed: Nx cache store for {repo_id} was not cleared: {refusal}"
                );
            }
        }
        let _ = reply.send(result.map_err(GatewayError::from));
    }

    /// Installs the outcome of a start. Ready resumes this repo's queued requests; a failure
    /// becomes a cooldown, and its queued requests are refused as an outage. A start that
    /// finishes while the gateway drains or fails closed installs nothing: its child is killed
    /// as it drops.
    async fn nx_backend_started(
        &mut self,
        repo_id: String,
        result: Result<(NxCacheBackend, tokio::process::Child), String>,
    ) {
        if self.draining || self.audit_failure.is_some() {
            if let Some(task) = self.nx.abandon(&repo_id) {
                retire_nx(Handle::Start(task));
            }
            // A started child is killed as it drops (`kill_on_drop`).
            drop(result);
            return;
        }
        match result {
            Ok((backend, process)) => {
                let (address, reclaimed) = (backend.address, backend.reclaimed_staging_files);
                match self.nx.ready(&repo_id, backend, Box::new(process)) {
                    // The start's task is the one reporting: it ends once its report is sent.
                    Installed::Ready { start } => drop(start),
                    Installed::Stale(process) => {
                        retire_nx(Handle::Child(process));
                        return;
                    }
                }
                cowshed_os::eprintln!(
                    "cowshed: Nx cache backend ready repo={repo_id} address={address} reclaimed_staging_files={reclaimed}"
                );
                self.resume_nx_waiters(&repo_id).await;
            }
            Err(reason) => {
                // The start's task is the one reporting: it ends once its report is sent.
                let Some(start) = self.nx.start_failed(&repo_id, reason.clone(), now()) else {
                    return;
                };
                drop(start);
                cowshed_os::eprintln!(
                    "cowshed: Nx cache backend for {repo_id} did not start: {reason}"
                );
                for waiting in self.pool.backend_failed(&repo_id) {
                    self.refuse_queued(
                        Queued::Backend(waiting),
                        Refusal::BackendOutage(reason.clone()),
                    )
                    .await;
                }
            }
        }
    }

    /// Resumes `repo_id`'s waiting Nx requests once its backend is Ready: each is seeded against
    /// it, then holds a permit or waits for one in the ordinary queue; one whose seed is refused
    /// is refused as the startup queue refuses.
    async fn resume_nx_waiters(&mut self, repo_id: &str) {
        let Some(backend) = self.nx.backend(repo_id) else {
            return;
        };
        // Every Nx seed is an intercepted request to its backend: `AdmissionSeed::class`.
        let class = PermitClass::Request {
            origin: backend.target().origin(),
        };
        let mut seeded = Vec::new();
        let mut refused = Vec::new();
        for resuming in self.pool.take_backend_waiters(repo_id) {
            let (place, waiting) = resuming.into_parts();
            let Some(session) = self.sessions.get(&waiting.workspace_id) else {
                refused.push((Refusal::SessionRotated, waiting));
                continue;
            };
            match build_seed(
                &waiting.workspace_id,
                session,
                &waiting.intent,
                Some(backend),
            ) {
                Ok(seed) => seeded.push(place.hold(Pending {
                    seed,
                    reply: waiting.reply,
                })),
                Err((message, hint)) => {
                    refused.push((Refusal::NxDenied { message, hint }, waiting))
                }
            }
        }
        for (permit, pending) in self.pool.resume(&class, seeded) {
            self.grant(permit, pending);
        }
        for (refusal, waiting) in refused {
            self.refuse_queued(Queued::Backend(waiting), refusal).await;
        }
    }

    /// Refuses one request before it is admitted, fresh or waiting for its backend: its audit
    /// records `refusal`, and its client hears the answer its route owns, or that the audit is
    /// unavailable. An Nx request is audited as any Nx refusal is ([`Self::audit_denial`]).
    async fn refuse_intent(
        &mut self,
        workspace_id: &str,
        intent: &RequestIntent,
        refusal: Refusal,
        reply: oneshot::Sender<Result<Admission, AdmissionError>>,
    ) {
        let hint = refusal.hint();
        if matches!(intent.target, RequestTarget::LocalNx) {
            let answer = refusal.nx_answer(&intent.method);
            if let Some(detail) = &hint {
                cowshed_os::eprintln!(
                    "cowshed: Nx cache {} for workspace {workspace_id} refused ({}): {detail}",
                    intent.method,
                    answer.classification
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
                        status: answer.audit,
                        http_status: answer.status,
                        classification: answer.classification,
                    },
                )
                .await;
            let _ = reply.send(Err(if recorded {
                admission_error(answer.status, answer.message, answer.audit, hint)
            } else {
                audit_unavailable(answer.status)
            }));
            return;
        }
        let answer = refusal.proxy_answer();
        let Some(status) = answer.status else {
            return;
        };
        let recorded = match self.sessions.get(workspace_id) {
            Some(session) => {
                let draft = denial_draft(workspace_id, session, intent, answer.audit, status, None);
                self.record(draft).await
            }
            None => false,
        };
        let _ = reply.send(Err(if recorded {
            admission_error(status, answer.message, answer.audit, hint)
        } else {
            audit_unavailable(StatusCode::SERVICE_UNAVAILABLE)
        }));
    }
    /// Refuses one request that left the queue unadmitted, as [`Self::refuse_intent`] does.
    async fn refuse_queued(&mut self, waiter: Queued<Pending, NxPending>, refusal: Refusal) {
        match waiter {
            Queued::Permit(Pending { seed, reply }) => {
                let answer = refusal.queued_answer(seed.nx.then_some(&seed.method));
                let draft =
                    pending_audit_draft(&seed, answer.audit, answer.status, answer.classification);
                let recorded = self.record(draft).await;
                let Some(status) = answer.status else {
                    return;
                };
                let _ = reply.send(Err(if recorded {
                    admission_error(status, answer.message, answer.audit, None)
                } else {
                    audit_unavailable(status)
                }));
            }
            Queued::Backend(NxPending {
                workspace_id,
                intent,
                reply,
            }) => {
                self.refuse_intent(&workspace_id, &intent, refusal, reply)
                    .await;
            }
        }
    }

    /// Every waiter of `workspace_id` leaves: its session was replaced or removed.
    async fn refuse_rotated(&mut self, workspace_id: &str) {
        for waiter in self.pool.remove(workspace_id) {
            self.refuse_queued(waiter, Refusal::SessionRotated).await;
        }
    }

    /// The client of queued request `queue_id` went away.
    async fn cancel_queued(&mut self, queue_id: QueueId) {
        if let Some(waiter) = self.pool.cancel(queue_id) {
            self.refuse_queued(waiter, Refusal::QueueCancelled).await;
        }
    }

    /// Every wait that ended by now is refused.
    async fn expire_queued(&mut self) {
        for waiter in self.pool.expire(now()) {
            self.refuse_queued(waiter, Refusal::QueueTimeout).await;
        }
    }

    /// Maps the client of queued request `queue_id` going away into a cancellation. The client's
    /// side drops its sender once it has its answer too, and a cancellation of a request that
    /// already left the queue changes nothing, so the watch ends with the request.
    fn watch_cancellation(&self, queue_id: QueueId, cancelled: oneshot::Receiver<()>) {
        let cancellations = self.cancellations.clone();
        tokio::spawn(async move {
            let _ = cancelled.await;
            let _ = cancellations.send(queue_id).await;
        });
    }

    /// Queues one Nx request while its project's backend starts, under the queue bounds every
    /// queued request shares.
    async fn wait_for_backend(
        &mut self,
        now: std::time::Instant,
        workspace_id: String,
        repo_id: &str,
        intent: RequestIntent,
        reply: oneshot::Sender<Result<Admission, AdmissionError>>,
        cancelled: oneshot::Receiver<()>,
    ) {
        let waiting = NxPending {
            workspace_id,
            intent,
            reply,
        };
        let workspace_id = waiting.workspace_id.clone();
        match self
            .pool
            .wait_for_backend(now, &workspace_id, repo_id, waiting)
        {
            Ok(queue_id) => self.watch_cancellation(queue_id, cancelled),
            Err((Unadmitted::Full, waiting)) => {
                self.refuse_queued(Queued::Backend(waiting), Refusal::QueueFull)
                    .await;
            }
            Err((Unadmitted::NoSession, waiting)) => {
                let _ = waiting.reply.send(Err(audit_unavailable(
                    waiting
                        .intent
                        .refusal_status(StatusCode::SERVICE_UNAVAILABLE),
                )));
            }
        }
    }

    /// Hands `pending` the permit the pool granted it. A client gone by now drops the admission,
    /// whose completion lease completes it at once.
    fn grant(&self, permit: PermitId, pending: Pending) {
        let Pending { seed, reply } = pending;
        let cancelled =
            pending_audit_draft(&seed, AuditStatus::Cancelled, None, Some("request-dropped"));
        let completion = self
            .completions
            .clone()
            .try_reserve_owned()
            .expect("completion capacity covers both active pools' global bounds");
        let admission = seed.activate(permit, CompletionLease::new(completion, permit, cancelled));
        let _ = reply.send(Ok(admission));
    }

    /// `permit`'s request ended: its places go back and the waiters that now fit are admitted.
    fn complete(&mut self, permit: PermitId) {
        for (permit, pending) in self.pool.complete(permit) {
            self.grant(permit, pending);
        }
    }

    async fn admit(
        &mut self,
        workspace_id: String,
        authentication: Authentication,
        intent: RequestIntent,
        reply: oneshot::Sender<Result<Admission, AdmissionError>>,
        cancelled: oneshot::Receiver<()>,
    ) {
        let is_nx = matches!(intent.target, RequestTarget::LocalNx);
        if self.draining {
            let _ = reply.send(Err(admission_error(
                intent.refusal_status(StatusCode::SERVICE_UNAVAILABLE),
                "gateway is draining",
                AuditStatus::Cancelled,
                None,
            )));
            return;
        }
        let Some(session) = self.sessions.get(&workspace_id) else {
            let _ = reply.send(Err(audit_unavailable(
                intent.refusal_status(StatusCode::SERVICE_UNAVAILABLE),
            )));
            return;
        };
        if !authenticated(&authentication, &session.token, session.generation, is_nx) {
            let status = intent.refusal_status(StatusCode::PROXY_AUTHENTICATION_REQUIRED);
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
                audit_unavailable(intent.refusal_status(StatusCode::SERVICE_UNAVAILABLE))
            }));
            return;
        }
        let repo_id = session.repo_id.clone();
        let now = now();
        if is_nx {
            let exited = self.nx_child_exited(&repo_id);
            let decision = self.nx.admit(&repo_id, &intent.method, now, exited);
            if let Some(process) = decision.exited {
                retire_nx(Handle::Child(process));
            }
            let refusal = match decision.admission {
                NxAdmission::Ready => None,
                NxAdmission::Starting => {
                    self.wait_for_backend(now, workspace_id, &repo_id, intent, reply, cancelled)
                        .await;
                    return;
                }
                NxAdmission::Start => match self.start_nx_backend(&repo_id) {
                    Ok(()) => {
                        self.wait_for_backend(
                            now,
                            workspace_id,
                            &repo_id,
                            intent,
                            reply,
                            cancelled,
                        )
                        .await;
                        return;
                    }
                    Err(reason) => Some(Refusal::BackendOutage(reason)),
                },
                NxAdmission::Refuse(refusal) => Some(refusal),
            };
            if let Some(refusal) = refusal {
                self.refuse_intent(&workspace_id, &intent, refusal, reply)
                    .await;
                return;
            }
        }
        let Some(session) = self.sessions.get(&workspace_id) else {
            let _ = reply.send(Err(audit_unavailable(
                intent.refusal_status(StatusCode::SERVICE_UNAVAILABLE),
            )));
            return;
        };
        let seed = match build_seed(
            &workspace_id,
            session,
            &intent,
            if is_nx {
                self.nx.backend(&repo_id)
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
                    audit_unavailable(intent.refusal_status(StatusCode::SERVICE_UNAVAILABLE))
                }));
                return;
            }
        };
        let class = seed.class();
        let nx_repo = seed.nx.then(|| seed.repo_id.clone());
        match self
            .pool
            .admit(now, &workspace_id, class, nx_repo, Pending { seed, reply })
        {
            Admitted::Active(permit, pending) => self.grant(permit, pending),
            Admitted::Queued(queue_id) => self.watch_cancellation(queue_id, cancelled),
            Admitted::Refused(Unadmitted::Full, Pending { reply, .. }) => {
                self.refuse_intent(&workspace_id, &intent, Refusal::QueueFull, reply)
                    .await;
            }
            Admitted::Refused(Unadmitted::NoSession, Pending { reply, .. }) => {
                let _ = reply.send(Err(audit_unavailable(
                    intent.refusal_status(StatusCode::SERVICE_UNAVAILABLE),
                )));
            }
        }
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
        // Nothing is admitted unaudited: every waiter hears that the audit is unavailable, an Nx
        // request as its method's miss or refusal.
        for waiter in self.pool.withdraw_all() {
            let (reply, status) = match waiter {
                Queued::Permit(Pending { seed, reply }) => {
                    (reply, seed.refusal_status(StatusCode::SERVICE_UNAVAILABLE))
                }
                Queued::Backend(NxPending { intent, reply, .. }) => (
                    reply,
                    intent.refusal_status(StatusCode::SERVICE_UNAVAILABLE),
                ),
            };
            let _ = reply.send(Err(audit_unavailable(status)));
        }
        for task in self.nx.abandon_starts() {
            retire_nx(Handle::Start(task));
        }
    }

    fn status(&self) -> DaemonStatus {
        let mut sessions: Vec<_> = self
            .sessions
            .iter()
            .map(|(workspace_id, session)| {
                let (active, queued) = self.pool.load(workspace_id);
                SessionStatus {
                    workspace_id: workspace_id.clone(),
                    revision: session.revision,
                    endpoint: session.endpoint_label.clone(),
                    active,
                    queued,
                }
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
            active: self.pool.active(),
            queued: self.pool.queued(),
        }
    }

    async fn begin_drain(&mut self, reply: oneshot::Sender<()>) {
        #[cfg(target_os = "macos")]
        self.give_listeners().await;
        self.draining = true;
        self.drain_reply = Some(reply);
        for session in self.sessions.values() {
            let _ = session.accept_stop.send(true);
            let _ = session.connection_stop.send(true);
        }
        for waiter in self.pool.withdraw_all() {
            self.refuse_queued(waiter, Refusal::Draining).await;
        }
    }

    /// Ends a drain once nothing is in flight: every Nx backend it started is signalled `SIGTERM`
    /// and waited for within one reap bound, the sessions stop and the audit is flushed. A
    /// backend still running past the bound fails the drain naming its pid and state, and is
    /// left running: its stdin closes, which asks it to drain and exit, and a task collects it.
    async fn finish_drain(&mut self) -> Result<bool, GatewayError> {
        if !self.draining || self.pool.active() != 0 {
            return Ok(false);
        }
        let mut signalled = Vec::new();
        for state in self.nx.take_all() {
            match state {
                NxBackendState::Starting { task } => retire_nx(Handle::Start(task)),
                NxBackendState::Ready {
                    process: Some(process),
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
                    signalled.push(process);
                }
                NxBackendState::Ready { process: None, .. }
                | NxBackendState::Failed { .. }
                | NxBackendState::Clearing => {}
            }
        }
        // One budget for all of them: they were signalled together, so they drain together, and
        // the drain stays inside launchd's exit timeout however many there are.
        let deadline = cowshed_os::deadline::start("drain nx cache backend", self.nx_backend_reap);
        let mut stuck = None;
        for mut process in signalled {
            if let Err(failure) = cowshed_os::deadline::reap_async(
                "nx cache backend",
                &mut process,
                &deadline,
                cowshed_os::deadline::Progress::Signalled {
                    signal: libc::SIGTERM,
                },
            )
            .await
            {
                cowshed_os::eprintln!("cowshed: {failure}");
                stuck.get_or_insert(failure);
                // Expiry ends the wait, never the backend: dropped, its `kill_on_drop` would kill it.
                // Its stdin closes, which asks it to drain and exit (`--exit-on-stdin-eof`).
                drop(process.stdin.take());
                // The collecting task is the backend's custodian from here (05_gateway.md,
                // "Deadlines"): nobody waits for it, and the runtime that ends with the gateway
                // drops it, whose `kill_on_drop` then ends the backend.
                drop(cowshed_os::deadline::collect_later_async(
                    "nx cache backend",
                    *process,
                ));
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
        if let Some(failure) = stuck {
            return Err(GatewayError::NxBackendExit(failure));
        }
        if let Some(reply) = self.drain_reply.take() {
            let _ = reply.send(());
        }
        Ok(true)
    }

    async fn force_stop(&mut self) -> Result<(), GatewayError> {
        for state in self.nx.take_all() {
            match state {
                NxBackendState::Starting { task } => retire_nx(Handle::Start(task)),
                NxBackendState::Ready {
                    process: Some(process),
                    ..
                } => retire_nx(Handle::Child(process)),
                NxBackendState::Ready { process: None, .. }
                | NxBackendState::Failed { .. }
                | NxBackendState::Clearing => {}
            }
        }
        let sessions = std::mem::take(&mut self.sessions);
        for (_, session) in sessions {
            session.stop().await;
        }
        self.pool.clear();
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
) -> Result<AdmissionSeed, (&'static str, Option<String>)> {
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
        nx: matches!(intent.target, RequestTarget::LocalNx),
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

/// The audit sink failed, so the gateway refuses without recording. `status` is the refused
/// request's route status (`RequestIntent::refusal_status`), so an Nx request still hears its
/// method's miss or not-stored answer.
fn audit_unavailable(status: StatusCode) -> AdmissionError {
    admission_error(
        status,
        "audit service is unavailable",
        AuditStatus::Failed,
        None,
    )
}

/// The workspace listeners the install restarting this host's gateway carried across the
/// restart, by address; empty when none was carried. A failed take is said and leaves every
/// session binding fresh, as before the handoff existed: `take` returns only once the install
/// has closed its copies, so no fresh bind meets them.
async fn take_carried_listeners(
    config: &GatewayConfig,
) -> HashMap<std::net::SocketAddr, Fenced<TcpListener>> {
    #[cfg(target_os = "macos")]
    if let Some(path) = config.listener_handoff.clone() {
        let taken = tokio::task::spawn_blocking(move || crate::listener_handoff::take(&path))
            .await
            .map_err(std::io::Error::other)
            .and_then(|taken| taken);
        let taken = match taken {
            Ok(taken) => taken,
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: could not take the workspace listeners the install carried; every \
                 session binds afresh: {error}"
                );
                return HashMap::new();
            }
        };
        let mut carried = HashMap::new();
        for (address, listener) in taken {
            // Already close-on-exec: the handoff received it under the fork lock.
            match listener
                .set_nonblocking(true)
                .and_then(|()| TcpListener::from_std(listener))
            {
                Ok(listener) => {
                    carried.insert(address, Fenced::new(listener));
                }
                Err(error) => cowshed_os::eprintln!(
                    "cowshed: the carried listener at {address} is closed, and its session binds \
                 afresh: {error}"
                ),
            }
        }
        if !carried.is_empty() {
            cowshed_os::eprintln!(
                "cowshed: took {} workspace listener(s) the install carried across the restart",
                carried.len()
            );
        }
        return carried;
    }
    let _ = config;
    HashMap::new()
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

        // What a previous gateway left is read before this one serves, so none of this gateway's
        // own leases is among it; the detaches run on in the background (05_gateway.md, "RAM disks").
        drop(crate::ram_lease::sweep(
            services.ram_disks.clone(),
            &services.ram_leases,
        ));
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

    /// Stop serving: end every control connection and remove the socket.
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
                let (stream, _) = match result {
                    Ok(accepted) => accepted,
                    Err(error) => {
                        cowshed_os::eprintln!("cowshed: gateway control socket stopped accepting: {error}");
                        break;
                    }
                };
                let credentials = match stream.peer_cred() {
                    Ok(credentials) => credentials,
                    Err(error) => {
                        cowshed_os::eprintln!("cowshed: gateway control connection dropped unanswered: its peer credentials could not be read: {error}");
                        continue;
                    }
                };
                if credentials.uid() != authorized_uid || connections.len() >= capacity {
                    cowshed_os::eprintln!(
                        "cowshed: gateway control connection dropped unanswered: uid {} (authorized {authorized_uid}), {} of {capacity} connections",
                        credentials.uid(),
                        connections.len()
                    );
                    continue;
                }
                let services = services.clone();
                connections.spawn(async move {
                    crate::control::serve_control_unix(stream, services).await;
                });
            }
            ended = connections.join_next(), if !connections.is_empty() => {
                if let Some(Err(error)) = ended {
                    cowshed_os::eprintln!("cowshed: gateway control connection task failed: {error}");
                }
            }
            changed = stopped.changed() => {
                if changed.is_err() || *stopped.borrow() {
                    let reason = if changed.is_err() { "shutdown sender dropped" } else { "shutdown requested" };
                    cowshed_os::eprintln!("cowshed: gateway control listener stopping: {reason}; closing {} connections", connections.len());
                    break;
                }
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
    #[error("Nx cache backend refused: {0}")]
    NxBackend(String),
    #[error("Nx cache store not cleared: {0}")]
    NxCacheClear(#[from] NxCacheClearRefusal),
    /// An Nx backend the drain signalled did not exit within the reap bound (code `TIMEOUT`), or
    /// waiting for it failed: its op, pid and the state the kernel showed it in.
    #[error(transparent)]
    NxBackendExit(cowshed_os::deadline::ReapError),
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
    #[cfg(target_os = "macos")]
    use cowshed_os::test_backstop::AsyncChildBackstop as _;

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
            nx: false,
            audit_kind: AuditKind::Connect,
            method,
            request_path: "/".to_owned(),
            upstream_path: "/".to_owned(),
            trace_id: None,
        }
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

    /// The listener names the workspace, so the Nx route needs no credential; every other route
    /// needs the session's token, and a tunnel's generation admits only its own session's inner
    /// requests, never the Nx route.
    #[test]
    fn only_the_sessions_credential_admits_a_request_and_the_nx_route_needs_none() {
        let token = WorkspaceToken::from_bytes([7; 32]);
        let encoded = token.encode();
        let other = WorkspaceToken::from_bytes([8; 32]).encode();
        let rows = [
            (Authentication::Bearer(Some(encoded.clone())), false, true),
            (Authentication::Bearer(Some(other.clone())), false, false),
            (Authentication::Bearer(None), false, false),
            (Authentication::Generation(3), false, true),
            (Authentication::Generation(2), false, false),
            (Authentication::Bearer(None), true, true),
            (Authentication::Bearer(Some(other)), true, true),
            (Authentication::Generation(3), true, false),
        ];
        for (authentication, nx, admitted) in rows {
            assert_eq!(
                authenticated(&authentication, &token, 3, nx),
                admitted,
                "{authentication:?} nx={nx}"
            );
        }
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
                cowshed_os::eprintln!("remove actor fixture {}: {error}", self.0.display());
            }
        }
    }

    /// The audit sink of the shell witnesses: they judge the drain and the clear, not the trail.
    #[cfg(target_os = "macos")]
    struct UnitAudit;

    #[cfg(target_os = "macos")]
    #[async_trait::async_trait]
    impl crate::interfaces::AuditSink for UnitAudit {
        async fn record(
            &self,
            _event: crate::interfaces::AuditEvent,
        ) -> Result<(), crate::interfaces::AuditError> {
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

    /// The actor built the way `start_runtime` builds it, plus the brokers' tasks, which `finish`
    /// stops: the shell its drain and store-clear witnesses run.
    #[cfg(target_os = "macos")]
    struct TestActor {
        actor: Actor,
        repo_task: JoinHandle<()>,
        sim_task: JoinHandle<()>,
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
            let audit: Arc<dyn crate::interfaces::AuditSink> = Arc::new(UnitAudit);
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
                audit,
                mirror_service,
                BrokerHandles {
                    repo_mirror,
                    sim_broker,
                },
            );
            Self {
                actor,
                repo_task,
                sim_task,
            }
        }

        /// Kills the fixture children still running, stops both brokers and waits for their tasks.
        async fn finish(mut self) {
            for state in self.actor.nx.take_all() {
                match state {
                    NxBackendState::Starting { task } => task.abort(),
                    NxBackendState::Ready {
                        process: Some(mut process),
                        ..
                    } => {
                        if process.try_wait().expect("fixture child status").is_none() {
                            process.start_kill().expect("kill fixture child");
                            process.wait_bounded().await.expect("reap fixture child");
                        }
                    }
                    NxBackendState::Ready { process: None, .. }
                    | NxBackendState::Failed { .. }
                    | NxBackendState::Clearing => {}
                }
            }
            for session in self.actor.sessions.values_mut() {
                session.quiesce().await;
            }
            self.actor.repo_mirror.shutdown().await;
            self.actor.sim_broker.shutdown().await;
            self.repo_task.await.expect("the repo mirror task ends");
            self.sim_task.await.expect("the simulator broker task ends");
        }

        /// `repo` served by `process`, a backend this actor started.
        fn serving(&mut self, repo: &str, process: tokio::process::Child) {
            let start = tokio::spawn(std::future::ready(()));
            assert!(
                self.actor.nx.starting(repo, start).is_none(),
                "nothing was displaced"
            );
            match self
                .actor
                .nx
                .ready(repo, unit_backend(repo), Box::new(process))
            {
                Installed::Ready { start } => start.abort(),
                Installed::Stale(_) => panic!("the start installs its backend"),
            }
        }
    }

    #[cfg(target_os = "macos")]
    fn unit_backend(repo_id: &str) -> NxCacheBackend {
        NxCacheBackend {
            repo_id: repo_id.to_owned(),
            address: std::net::SocketAddr::from(([127, 0, 0, 1], 9)),
            reclaimed_staging_files: 3,
        }
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
            closed.await.is_err(),
            "aborting the supervisor must abort its worker"
        );
    }

    /// A start whose worker panics reports a failed start, the transition that starts its
    /// cooldown (`NxBackends::start_failed`); it never leaves an unreported `Starting` state.
    #[tokio::test]
    async fn nx_start_worker_panic_is_reported_as_a_failed_start() {
        let worker = tokio::spawn(async { panic!("planted startup panic") });
        let (commands, mut receive) = mpsc::channel(1);
        let task = supervise_nx_start("owner/nx-panic".to_owned(), commands, worker);
        let Some(Command::NxBackendStarted { repo_id, result }) = receive.recv().await else {
            panic!("expected the start's report")
        };
        assert_eq!(repo_id, "owner/nx-panic");
        let reason = result.err().expect("a panic is a failed start");
        assert!(reason.contains("planted startup panic"), "{reason}");
        task.await.expect("the supervisor ends once it reported");
    }

    /// 05_gateway.md "Deadlines": the drain waits for an Nx backend it signalled within the reap
    /// bound. A backend that ignores `SIGTERM` fails the drain naming its pid at that bound, the
    /// actor turns again, so it is never parked past launchd's exit timeout, and the backend is
    /// left running. Paused time: the stand-in never exits, so nothing real ends the wait before
    /// its bound, and the drain's deadline runs on tokio's clock, so the wait lasts the injected
    /// bound exactly. A drain parked on the backend is a hang the no-progress bound names.
    #[cfg(target_os = "macos")]
    #[tokio::test(start_paused = true)]
    async fn drain_names_an_nx_backend_that_ignores_sigterm() {
        use cowshed_os::deadline::{Bound, LatencyClass, Progress, ReapError};
        use cowshed_os::fork_lock::Spawn as _;
        use tokio::io::AsyncBufReadExt as _;
        let root = TestRoot::new("drain-stuck");
        let mut fixture = TestActor::new(&root).await;
        let mut process = tokio::process::Command::new("/bin/sh")
            .args([
                "-c",
                "trap '' TERM; echo trapped; while :; do sleep 1; done",
            ])
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::null())
            .kill_on_drop(true)
            .spawn_locked()
            .expect("start the backend that ignores SIGTERM");
        // Signalled before its trap is set, the shell would end on SIGTERM, as a backend does.
        let mut trapped = String::new();
        tokio::io::BufReader::new(process.stdout.take().expect("the stand-in's stdout"))
            .read_line(&mut trapped)
            .await
            .expect("read the stand-in's trap line");
        assert_eq!(trapped, "trapped\n");
        let pid = process.id().expect("the stand-in's pid");
        fixture.serving("owner/nx-stuck", process);
        let bound = Duration::from_millis(500);
        fixture.actor.nx_backend_reap = Bound::new(LatencyClass::Reap, bound);
        fixture.actor.draining = true;
        let started = tokio::time::Instant::now();
        let drained = fixture.actor.finish_drain().await;
        let waited = started.elapsed();
        let Err(GatewayError::NxBackendExit(ReapError::Expired(expired))) = &drained else {
            panic!("the drain fails with the backend's reap expiry: {drained:?}");
        };
        assert_eq!(expired.op, "drain nx cache backend");
        assert_eq!(expired.peer.role, "nx cache backend");
        assert_eq!(expired.peer.pid, pid);
        assert_eq!(expired.bound, bound);
        assert_eq!(
            expired.last,
            Progress::Signalled {
                signal: libc::SIGTERM
            }
        );
        assert_eq!(
            waited, bound,
            "the drain gives up at its bound, neither before nor after it"
        );
        let said = drained.expect_err("matched above").to_string();
        assert!(said.contains(&format!("pid {pid}")), "{said}");
        assert!(fixture.actor.nx.take_all().is_empty());
        let pid = libc::pid_t::try_from(pid).expect("a pid_t");
        let state = cowshed_os::process::run_state(pid);
        assert!(
            matches!(
                state,
                cowshed_os::process::RunState::Running
                    | cowshed_os::process::RunState::Sleeping
                    | cowshed_os::process::RunState::Uninterruptible
            ),
            "the expiry leaves the backend running: {state}"
        );
        // SAFETY: `pid` is this test's own stand-in, still unreaped (its collector holds it).
        assert_eq!(
            unsafe { libc::kill(pid, libc::SIGKILL) },
            0,
            "kill the stand-in"
        );
        fixture.finish().await;
    }

    /// The clear's shell: an idle project's started backend stops, the store is emptied under its
    /// lease in the clear's own task, and its report forgets the backend, so the next request
    /// starts one. Which clears are refused is `NxBackends::clear`'s table.
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn nx_cache_clear_stops_the_started_backend_and_empties_its_store() {
        let root = TestRoot::new("nx-clear");
        let mut fixture = TestActor::new(&root).await;
        let repo = "owner/nx-clear";
        fixture.actor.config.nx_cache = Some(crate::nx_cache::NxCacheServiceConfig {
            script: root.0.join("unavailable-server"),
            path: std::ffi::OsString::new(),
            directory: root.0.join("nx"),
            max_bytes: 1024,
            cap_bytes: 4096,
        });
        let project = crate::nx_cache::project_store(&root.0.join("nx"), repo);
        let entries = project.join("entries");
        std::fs::create_dir_all(entries.join(".staging")).expect("store fixture");
        for key in ["101", "202", "abc_-9"] {
            std::fs::write(entries.join(key), b"archive").expect("archive fixture");
        }
        std::fs::write(entries.join(".owner.lock"), b"").expect("lease fixture");
        std::fs::write(project.join("sieve.hand"), b"1\n").expect("cursor fixture");
        fixture.serving(repo, unit_child().await);

        let (reply, cleared) = oneshot::channel();
        fixture.actor.clear_nx_cache(repo.to_owned(), reply);
        let (repo_id, result, reply) = loop {
            match fixture.actor.receiver.recv().await {
                Some(Command::NxCacheCleared {
                    repo_id,
                    result,
                    reply,
                }) => break (repo_id, result, reply),
                Some(_) => {}
                None => panic!("the actor's command channel closed"),
            }
        };
        fixture.actor.nx_cache_cleared(&repo_id, result, reply);
        assert_eq!(cleared.await.expect("answered").expect("cleared"), 3);
        for key in ["101", "202", "abc_-9"] {
            assert!(!entries.join(key).exists(), "{key} is deleted");
        }
        assert!(
            entries.join(".owner.lock").exists(),
            "the lease is never unlinked"
        );
        assert!(entries.join(".staging").exists());
        assert!(!project.join("sieve.hand").exists());
        assert_eq!(
            fixture
                .actor
                .nx
                .admit(repo, &Method::GET, now(), false)
                .admission,
            NxAdmission::Start,
            "the cleared project's next request starts a backend"
        );
        fixture.finish().await;
    }
}
