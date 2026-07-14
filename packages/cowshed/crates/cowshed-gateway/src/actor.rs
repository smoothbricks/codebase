use std::{
    collections::{HashMap, VecDeque},
    fmt,
    path::Path,
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use http::{Method, StatusCode};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    net::{TcpListener, UnixListener},
    sync::{mpsc, oneshot, watch},
    task::JoinHandle,
    time::timeout,
};

use crate::{
    config::{ConfigError, GatewayConfig, WorkspaceEndpoint, WorkspaceSession},
    interfaces::{
        AuditError, AuditEvent, AuditKind, AuditSink, AuditStatus, ConnectError,
        CredentialProvider, SystemConnector, UpstreamConnector,
    },
    platform::KeychainCredentialProvider,
    policy::{CanonicalTarget, EgressMode, MirrorProtocol, PolicyDenial},
    proxy,
    telemetry::{ArrowAuditConfig, ArrowAuditSink},
    tls::{CaSigner, LeafCache, TlsError},
};

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

impl GatewayHandle {
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

    pub async fn status(&self) -> Result<GatewayStatus, GatewayError> {
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
}

/// Owning daemon object. Dropping it force-closes listeners and active streams.
pub struct Gateway {
    handle: GatewayHandle,
    actor: Option<JoinHandle<Result<(), GatewayError>>>,
    control: Option<ControlRuntime>,
    drain_timeout: Duration,
}

impl Gateway {
    /// Starts the production host daemon with macOS Keychain credentials,
    /// platform-verifying upstream TLS, and durable Arrow telemetry.
    pub async fn start_host(
        config: GatewayConfig,
        telemetry: ArrowAuditConfig,
    ) -> Result<Self, GatewayError> {
        config.validate()?;
        let connector =
            SystemConnector::new(config.timeouts.connect, config.timeouts.tls_handshake)?;
        let audit = ArrowAuditSink::start(telemetry)?;
        Self::start(
            config,
            Arc::new(KeychainCredentialProvider::new()),
            Arc::new(connector),
            Arc::new(audit),
        )
        .await
    }

    pub async fn start(
        config: GatewayConfig,
        credentials: Arc<dyn CredentialProvider>,
        connector: Arc<dyn UpstreamConnector>,
        audit: Arc<dyn AuditSink>,
    ) -> Result<Self, GatewayError> {
        config.validate()?;
        let (commands, receiver) = mpsc::channel(config.command_capacity.get());
        let handle = GatewayHandle { commands };
        let state = Actor::new(
            config.clone(),
            receiver,
            handle.commands.clone(),
            credentials,
            connector,
            audit,
        );
        let actor = tokio::spawn(state.run());
        let control = match &config.control_socket {
            Some(path) => Some(
                ControlRuntime::start(path, config.authorized_control_uid, handle.clone()).await?,
            ),
            None => None,
        };
        Ok(Self {
            handle,
            actor: Some(actor),
            control,
            drain_timeout: config.timeouts.request_total,
        })
    }

    pub fn handle(&self) -> GatewayHandle {
        self.handle.clone()
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
            control.task.abort();
            let _ = std::fs::remove_file(&control.path);
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GatewayStatus {
    pub draining: bool,
    pub sessions: Vec<SessionStatus>,
    pub active: usize,
    pub queued: usize,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionStatus {
    pub workspace_id: String,
    pub revision: u64,
    pub endpoint: String,
    pub active: usize,
    pub queued: usize,
}

pub(crate) enum Command {
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
    },
    Complete {
        permit_id: u64,
        draft: AuditDraft,
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
        reply: oneshot::Sender<GatewayStatus>,
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
    LocalMirror,
}

#[derive(Clone, Debug)]
pub(crate) struct RequestIntent {
    pub target: RequestTarget,
    pub method: Method,
    pub path: String,
    pub audit_kind: AuditKind,
    pub trace_id: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) struct Admission {
    pub permit_id: u64,
    pub workspace_id: String,
    pub repo_id: String,
    pub revision: u64,
    pub endpoint: String,
    pub generation: u64,
    pub target: CanonicalTarget,
    pub mode: EgressMode,
    pub impersonate: bool,
    pub protocol: Option<MirrorProtocol>,
    pub credential_allowed: bool,
    pub private_network_authorized: bool,
    pub audit_kind: AuditKind,
    pub method: Method,
    pub request_path: String,
    pub upstream_path: String,
    pub trace_id: Option<String>,
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
    impersonate: bool,
    protocol: Option<MirrorProtocol>,
    credential_allowed: bool,
    private_network_authorized: bool,
    audit_kind: AuditKind,
    method: Method,
    request_path: String,
    upstream_path: String,
    trace_id: Option<String>,
}

impl AdmissionSeed {
    fn activate(self, permit_id: u64) -> Admission {
        Admission {
            permit_id,
            workspace_id: self.workspace_id,
            repo_id: self.repo_id,
            revision: self.revision,
            endpoint: self.endpoint,
            generation: self.generation,
            target: self.target,
            mode: self.mode,
            impersonate: self.impersonate,
            protocol: self.protocol,
            credential_allowed: self.credential_allowed,
            private_network_authorized: self.private_network_authorized,
            audit_kind: self.audit_kind,
            method: self.method,
            request_path: self.request_path,
            upstream_path: self.upstream_path,
            trace_id: self.trace_id,
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct AuditDraft {
    pub workspace_id: String,
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
    pub grant_hint: Option<String>,
    pub classification: Option<String>,
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
            grant_hint: self.grant_hint,
            classification: self.classification,
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
}

struct PermitState {
    workspace_id: String,
    origin: String,
    generation: u64,
}

struct SessionState {
    repo_id: String,
    revision: u64,
    endpoint: WorkspaceEndpoint,
    endpoint_label: String,
    token: crate::config::WorkspaceToken,
    policy: crate::policy::WorkspacePolicy,
    signer: CaSigner,
    generation: u64,
    active: usize,
    queued: usize,
    accept_stop: watch::Sender<bool>,
    connection_stop: watch::Sender<bool>,
    listener_task: JoinHandle<()>,
}

impl SessionState {
    fn stop(self) {
        let _ = self.accept_stop.send(true);
        let _ = self.connection_stop.send(true);
        self.listener_task.abort();
        unlink_endpoint(&self.endpoint);
    }
}

struct Actor {
    config: GatewayConfig,
    receiver: mpsc::Receiver<Command>,
    commands: mpsc::Sender<Command>,
    credentials: Arc<dyn CredentialProvider>,
    connector: Arc<dyn UpstreamConnector>,
    audit: Arc<dyn AuditSink>,
    sessions: HashMap<String, SessionState>,
    permits: HashMap<u64, PermitState>,
    origins: HashMap<(String, String), usize>,
    queue: VecDeque<Pending>,
    global_active: usize,
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
        receiver: mpsc::Receiver<Command>,
        commands: mpsc::Sender<Command>,
        credentials: Arc<dyn CredentialProvider>,
        connector: Arc<dyn UpstreamConnector>,
        audit: Arc<dyn AuditSink>,
    ) -> Self {
        Self {
            leaf_cache: LeafCache::new(config.limits, config.timeouts),
            commands,
            config,
            receiver,
            credentials,
            connector,
            audit,
            sessions: HashMap::new(),
            permits: HashMap::new(),
            origins: HashMap::new(),
            queue: VecDeque::new(),
            global_active: 0,
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
        while let Some(command) = self.receiver.recv().await {
            match command {
                Command::Install { session, reply } => {
                    let result = self.install(session).await;
                    let _ = reply.send(result);
                }
                Command::Remove {
                    workspace_id,
                    expected_revision,
                    reply,
                } => {
                    let result = self.remove(&workspace_id, expected_revision);
                    let _ = reply.send(result);
                }
                Command::Admit {
                    workspace_id,
                    authentication,
                    intent,
                    reply,
                } => {
                    self.admit(workspace_id, authentication, intent, reply)
                        .await;
                }
                Command::Complete { permit_id, draft } => {
                    self.record(draft).await;
                    self.complete(permit_id);
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
                    self.begin_drain(reply);
                    if self.finish_drain().await? {
                        return Ok(());
                    }
                }
                Command::ForceStop => {
                    self.force_stop().await?;
                    return Ok(());
                }
            }
            if self.draining && self.finish_drain().await? {
                return Ok(());
            }
        }
        self.force_stop().await?;
        Ok(())
    }

    async fn install(&mut self, session: WorkspaceSession) -> Result<(), GatewayError> {
        if self.draining {
            return Err(GatewayError::Draining);
        }
        session.validate()?;
        let signer = CaSigner::parse(&session.ca)?;
        if let Some(current) = self.sessions.get(&session.workspace_id)
            && session.revision <= current.revision
        {
            return Err(GatewayError::StaleRevision);
        }
        if self.sessions.iter().any(|(workspace_id, current)| {
            workspace_id != &session.workspace_id && current.endpoint == session.endpoint
        }) {
            return Err(GatewayError::EndpointInUse);
        }
        if let Some(previous) = self.sessions.remove(&session.workspace_id) {
            previous.stop();
            self.leaf_cache.drop_workspace(&session.workspace_id);
            self.cancel_queued(&session.workspace_id);
        }
        let generation = self.next_generation;
        self.next_generation = self.next_generation.wrapping_add(1).max(1);
        let endpoint_label = endpoint_label(&session.endpoint);
        let (accept_stop, accept_rx) = watch::channel(false);
        let (connection_stop, connection_rx) = watch::channel(false);
        let listener = bind_endpoint(&session.endpoint).await?;
        let context = proxy::AcceptContext {
            workspace_id: session.workspace_id.clone(),
            commands: self.commands.clone(),
            credentials: Arc::clone(&self.credentials),
            connector: Arc::clone(&self.connector),
            timeouts: self.config.timeouts,
            connection_stop: connection_rx.clone(),
        };
        let listener_task = tokio::spawn(proxy::accept_loop(
            listener,
            context,
            accept_rx,
            connection_rx,
        ));
        self.sessions.insert(
            session.workspace_id.clone(),
            SessionState {
                repo_id: session.repo_id,
                revision: session.revision,
                endpoint: session.endpoint,
                endpoint_label,
                token: session.token,
                policy: session.policy,
                signer,
                generation,
                active: 0,
                queued: 0,
                accept_stop,
                connection_stop,
                listener_task,
            },
        );
        Ok(())
    }

    fn remove(&mut self, workspace_id: &str, expected_revision: u64) -> Result<(), GatewayError> {
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
        session.stop();
        self.leaf_cache.drop_workspace(workspace_id);
        self.cancel_queued(workspace_id);
        Ok(())
    }

    async fn admit(
        &mut self,
        workspace_id: String,
        authentication: Authentication,
        intent: RequestIntent,
        reply: oneshot::Sender<Result<Admission, AdmissionError>>,
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
            let _ = reply.send(Err(admission_error(
                StatusCode::UNAUTHORIZED,
                "unknown workspace endpoint",
                AuditStatus::Unauthorized,
                None,
            )));
            return;
        };
        let authenticated = match authentication {
            Authentication::Bearer(Some(token)) => session.token.matches_encoded(&token),
            Authentication::Bearer(None) => false,
            Authentication::Generation(generation) => generation == session.generation,
        };
        if !authenticated {
            let draft = denial_draft(
                &workspace_id,
                session,
                &intent,
                AuditStatus::Unauthorized,
                StatusCode::UNAUTHORIZED,
                None,
            );
            self.record(draft).await;
            let _ = reply.send(Err(admission_error(
                StatusCode::UNAUTHORIZED,
                "missing or invalid proxy bearer token",
                AuditStatus::Unauthorized,
                None,
            )));
            return;
        }
        let seed = match build_seed(&workspace_id, session, &intent) {
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
                self.record(draft).await;
                let _ = reply.send(Err(admission_error(
                    StatusCode::FORBIDDEN,
                    denial,
                    AuditStatus::Denied,
                    hint,
                )));
                return;
            }
        };
        let origin = seed.target.origin();
        if self.can_activate(&workspace_id, &origin) {
            let admission = self.activate(seed);
            let _ = reply.send(Ok(admission));
            return;
        }
        if session.queued >= self.config.limits.workspace_queued
            || self.global_queued >= self.config.limits.global_queued
        {
            let draft = denial_draft(
                &workspace_id,
                session,
                &intent,
                AuditStatus::Limited,
                StatusCode::TOO_MANY_REQUESTS,
                None,
            );
            self.record(draft).await;
            let _ = reply.send(Err(admission_error(
                StatusCode::TOO_MANY_REQUESTS,
                "gateway queue is full",
                AuditStatus::Limited,
                None,
            )));
            return;
        }
        if let Some(session) = self.sessions.get_mut(&workspace_id) {
            session.queued += 1;
        }
        self.global_queued += 1;
        let queue_id = self.next_queue;
        self.next_queue = self.next_queue.wrapping_add(1).max(1);
        let commands = self.commands.clone();
        let queue_timeout = self.config.timeouts.request_total;
        let timer = tokio::spawn(async move {
            tokio::time::sleep(queue_timeout).await;
            let _ = commands.send(Command::QueueExpired { queue_id }).await;
        });
        self.queue.push_back(Pending {
            queue_id,
            seed,
            reply,
            timer: Some(timer),
        });
    }

    fn can_activate(&self, workspace_id: &str, origin: &str) -> bool {
        let Some(session) = self.sessions.get(workspace_id) else {
            return false;
        };
        session.active < self.config.limits.workspace_active
            && self.global_active < self.config.limits.global_active
            && self
                .origins
                .get(&(workspace_id.to_owned(), origin.to_owned()))
                .copied()
                .unwrap_or(0)
                < self.config.limits.origin_active
    }

    fn activate(&mut self, seed: AdmissionSeed) -> Admission {
        let permit_id = self.next_permit;
        self.next_permit = self.next_permit.wrapping_add(1).max(1);
        let origin = seed.target.origin();
        let workspace_id = seed.workspace_id.clone();
        self.global_active += 1;
        if let Some(session) = self.sessions.get_mut(&workspace_id) {
            session.active += 1;
        }
        *self
            .origins
            .entry((workspace_id.clone(), origin.clone()))
            .or_default() += 1;
        self.permits.insert(
            permit_id,
            PermitState {
                workspace_id,
                origin,
                generation: seed.generation,
            },
        );
        seed.activate(permit_id)
    }

    fn complete(&mut self, permit_id: u64) {
        let Some(permit) = self.permits.remove(&permit_id) else {
            return;
        };
        self.global_active = self.global_active.saturating_sub(1);
        if let Some(session) = self.sessions.get_mut(&permit.workspace_id)
            && session.generation == permit.generation
        {
            session.active = session.active.saturating_sub(1);
        }
        let origin_key = (permit.workspace_id, permit.origin);
        if let Some(active) = self.origins.get_mut(&origin_key) {
            *active = active.saturating_sub(1);
            if *active == 0 {
                self.origins.remove(&origin_key);
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
            if pending.reply.is_closed() || !self.sessions.contains_key(&workspace_id) {
                if let Some(timer) = pending.timer.take() {
                    timer.abort();
                }
                continue;
            }
            let origin = pending.seed.target.origin();
            if self.can_activate(&workspace_id, &origin) {
                if let Some(timer) = pending.timer.take() {
                    timer.abort();
                }
                let admission = self.activate(pending.seed);
                if pending.reply.send(Ok(admission.clone())).is_err() {
                    self.complete(admission.permit_id);
                }
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

    fn cancel_queued(&mut self, workspace_id: &str) {
        let mut retained = VecDeque::new();
        while let Some(mut pending) = self.queue.pop_front() {
            if pending.seed.workspace_id == workspace_id {
                if let Some(timer) = pending.timer.take() {
                    timer.abort();
                }
                self.global_queued = self.global_queued.saturating_sub(1);
                let _ = pending.reply.send(Err(admission_error(
                    StatusCode::SERVICE_UNAVAILABLE,
                    "workspace session rotated",
                    AuditStatus::Cancelled,
                    None,
                )));
            } else {
                retained.push_back(pending);
            }
        }
        self.queue = retained;
    }

    async fn expire_queued(&mut self, queue_id: u64) {
        let Some(position) = self
            .queue
            .iter()
            .position(|pending| pending.queue_id == queue_id)
        else {
            return;
        };
        let mut pending = self.queue.remove(position).expect("queued position exists");
        if let Some(timer) = pending.timer.take() {
            timer.abort();
        }
        if let Some(session) = self.sessions.get_mut(&pending.seed.workspace_id) {
            session.queued = session.queued.saturating_sub(1);
        }
        self.global_queued = self.global_queued.saturating_sub(1);
        let draft = AuditDraft {
            workspace_id: pending.seed.workspace_id.clone(),
            revision: pending.seed.revision,
            endpoint: pending.seed.endpoint.clone(),
            kind: pending.seed.audit_kind,
            host: Some(pending.seed.target.authority()),
            method: Some(pending.seed.method.to_string()),
            path: Some(pending.seed.request_path.clone()),
            status: AuditStatus::TimedOut,
            http_status: Some(StatusCode::GATEWAY_TIMEOUT.as_u16()),
            bytes: 0,
            trace_id: pending.seed.trace_id.clone(),
            grant_hint: None,
            classification: Some("queue-timeout".to_owned()),
        };
        self.record(draft).await;
        let _ = pending.reply.send(Err(admission_error(
            StatusCode::GATEWAY_TIMEOUT,
            "gateway queue wait timed out",
            AuditStatus::TimedOut,
            None,
        )));
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

    async fn record(&mut self, draft: AuditDraft) {
        let sequence = self.next_audit;
        self.next_audit = self.next_audit.wrapping_add(1).max(1);
        if let Err(error) = self.audit.record(draft.into_event(sequence)).await
            && self.audit_failure.is_none()
        {
            self.audit_failure = Some(error);
        }
    }

    fn status(&self) -> GatewayStatus {
        let mut sessions: Vec<_> = self
            .sessions
            .iter()
            .map(|(workspace_id, session)| SessionStatus {
                workspace_id: workspace_id.clone(),
                revision: session.revision,
                endpoint: session.endpoint_label.clone(),
                active: session.active,
                queued: session.queued,
            })
            .collect();
        sessions.sort_by(|left, right| left.workspace_id.cmp(&right.workspace_id));
        GatewayStatus {
            draining: self.draining,
            sessions,
            active: self.global_active,
            queued: self.global_queued,
        }
    }

    fn begin_drain(&mut self, reply: oneshot::Sender<()>) {
        self.draining = true;
        self.drain_reply = Some(reply);
        for session in self.sessions.values() {
            let _ = session.accept_stop.send(true);
        }
        while let Some(mut pending) = self.queue.pop_front() {
            if let Some(session) = self.sessions.get_mut(&pending.seed.workspace_id) {
                session.queued = session.queued.saturating_sub(1);
            }
            self.global_queued = self.global_queued.saturating_sub(1);
            if let Some(timer) = pending.timer.take() {
                timer.abort();
            }
            let _ = pending.reply.send(Err(admission_error(
                StatusCode::SERVICE_UNAVAILABLE,
                "gateway is draining",
                AuditStatus::Cancelled,
                None,
            )));
        }
    }

    async fn finish_drain(&mut self) -> Result<bool, GatewayError> {
        if !self.draining || self.global_active != 0 {
            return Ok(false);
        }
        let sessions = std::mem::take(&mut self.sessions);
        for (_, session) in sessions {
            session.stop();
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
        let sessions = std::mem::take(&mut self.sessions);
        for (_, session) in sessions {
            session.stop();
        }
        while let Some(mut pending) = self.queue.pop_front() {
            if let Some(timer) = pending.timer.take() {
                timer.abort();
            }
        }
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
) -> Result<AdmissionSeed, (&'static str, Option<String>)> {
    let (
        target,
        mode,
        impersonate,
        protocol,
        credential_allowed,
        private_network_authorized,
        upstream_path,
    ) = match &intent.target {
        RequestTarget::LocalMirror => {
            let resolved = session.policy.resolve_mirror(&intent.path).ok_or((
                "mirror route is not admitted",
                Some("trusted project policy must admit this registry scope".to_owned()),
            ))?;
            (
                resolved.target,
                EgressMode::Intercept,
                false,
                Some(resolved.protocol),
                resolved.credentialed,
                true,
                resolved.path,
            )
        }
        RequestTarget::Generic(target) => {
            let grant = session
                .policy
                .authorize(target, &intent.method, &intent.path)
                .map_err(|denial| match denial {
                    PolicyDenial::InvalidPath => ("request path is ambiguous", None),
                    PolicyDenial::NotGranted { hint } => ("destination is not granted", Some(hint)),
                })?;
            (
                target.clone(),
                grant.mode,
                grant.impersonate,
                None,
                grant.mode == EgressMode::Intercept && !grant.impersonate,
                grant.host.is_exact(),
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
        impersonate,
        protocol,
        credential_allowed,
        private_network_authorized,
        audit_kind: intent.audit_kind,
        method: intent.method.clone(),
        request_path: intent.path.clone(),
        upstream_path,
        trace_id: intent.trace_id.clone(),
    })
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
        RequestTarget::LocalMirror => None,
    };
    AuditDraft {
        workspace_id: workspace_id.to_owned(),
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
        grant_hint: hint,
        classification: None,
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

pub(crate) enum BoundListener {
    Tcp(TcpListener),
    Unix(UnixListener),
}

async fn bind_endpoint(endpoint: &WorkspaceEndpoint) -> Result<BoundListener, GatewayError> {
    match endpoint {
        WorkspaceEndpoint::Tcp(address) => {
            Ok(BoundListener::Tcp(TcpListener::bind(address).await?))
        }
        WorkspaceEndpoint::Unix(path) => {
            if path.exists() {
                std::fs::remove_file(path)?;
            }
            let listener = UnixListener::bind(path)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt as _;
                std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
            }
            Ok(BoundListener::Unix(listener))
        }
    }
}

fn endpoint_label(endpoint: &WorkspaceEndpoint) -> String {
    match endpoint {
        WorkspaceEndpoint::Tcp(address) => address.to_string(),
        WorkspaceEndpoint::Unix(path) => path.display().to_string(),
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
    task: JoinHandle<()>,
}

impl ControlRuntime {
    async fn start(
        path: &Path,
        authorized_uid: u32,
        handle: GatewayHandle,
    ) -> Result<Self, GatewayError> {
        let parent = path.parent().ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "control socket has no parent",
            )
        })?;
        let metadata = std::fs::symlink_metadata(parent)?;
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "control socket parent must be an existing real directory",
            )
            .into());
        }
        if path.exists() {
            std::fs::remove_file(path)?;
        }
        let listener = UnixListener::bind(path)?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt as _;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
        }
        let (stop, mut stopped) = watch::channel(false);
        let owned_path = path.to_path_buf();
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    result = listener.accept() => {
                        let Ok((stream, _)) = result else { break };
                        let Ok(credentials) = stream.peer_cred() else { continue };
                        if credentials.uid() != authorized_uid { continue; }
                        let handle = handle.clone();
                        tokio::spawn(async move { crate::control::serve_control(stream, handle).await; });
                    }
                    changed = stopped.changed() => {
                        if changed.is_err() || *stopped.borrow() { break; }
                    }
                }
            }
        });
        Ok(Self {
            path: owned_path,
            stop,
            task,
        })
    }

    async fn stop(self) {
        let _ = self.stop.send(true);
        let _ = self.task.await;
        let _ = std::fs::remove_file(self.path);
    }
}

#[derive(Debug, Error)]
pub enum GatewayError {
    #[error(transparent)]
    Config(#[from] ConfigError),
    #[error(transparent)]
    Tls(#[from] TlsError),
    #[error(transparent)]
    Audit(#[from] AuditError),
    #[error(transparent)]
    Connector(#[from] ConnectError),
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
    #[error("gateway actor stopped")]
    Stopped,
    #[error("gateway task failed: {0}")]
    Task(String),
}
