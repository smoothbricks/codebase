use std::{
    collections::BTreeSet,
    fmt,
    net::SocketAddr,
    path::{Path, PathBuf},
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

use crate::{
    actor::{GatewayError, GatewayHandle, GatewayStatus},
    config::{WorkspaceCa, WorkspaceEndpoint, WorkspaceSession, WorkspaceToken},
    interfaces::AuditEvent,
    policy::{EgressGrant, EgressMode, HostPattern, MirrorProtocol, MirrorRoute, WorkspacePolicy},
    repo_mirror::{MirrorInfo, RepoMirrorError, RepoMirrorHandle, RepoMirrorRequest},
    sim_broker::{
        SimBrokerError, SimBrokerHandle, SimDevice, SimInstallApproval, SimProjectConfig,
    },
    telemetry::{AuditTailHandle, AuditTailQuery},
};

const MAX_CONTROL_MESSAGE: u64 = 1024 * 1024;
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
        if address != "127.0.0.1:7644".parse().expect("literal control address")
            || !credential_file.is_absolute()
        {
            return Err(ControlError::InvalidTcpEndpoint);
        }
        Ok(Self {
            endpoint: ControlEndpoint::Tcp {
                address,
                credential_file,
            },
        })
    }

    pub async fn status(&self) -> Result<GatewayStatus, ControlError> {
        self.send(&ControlRequestOut::Status)
            .await?
            .status
            .ok_or(ControlError::InvalidResponse)
    }

    pub async fn install(&self, session: &WorkspaceSession) -> Result<(), ControlError> {
        session
            .validate()
            .map_err(|error| ControlError::InvalidSession(error.to_string()))?;
        let wire = SessionWire::from(session);
        self.send(&ControlRequestOut::Install { session: &wire })
            .await?;
        Ok(())
    }

    pub async fn remove(
        &self,
        workspace_id: &str,
        expected_revision: u64,
    ) -> Result<(), ControlError> {
        self.send(&ControlRequestOut::Remove {
            workspace_id,
            expected_revision,
        })
        .await?;
        Ok(())
    }

    pub async fn repo_mirror(
        &self,
        request: &RepoMirrorRequest,
    ) -> Result<MirrorInfo, ControlError> {
        self.send(&ControlRequestOut::RepoMirror { request })
            .await?
            .mirror_info
            .ok_or(ControlError::InvalidResponse)
    }

    pub async fn configure_simulator(&self, config: &SimProjectConfig) -> Result<(), ControlError> {
        self.send(&ControlRequestOut::SimConfigure { config })
            .await?;
        Ok(())
    }

    pub async fn approve_simulator_install(
        &self,
        approval: &SimInstallApproval,
    ) -> Result<(), ControlError> {
        self.send(&ControlRequestOut::SimApprove { approval })
            .await?;
        Ok(())
    }

    pub async fn list_simulator_devices(
        &self,
        repo_id: &str,
    ) -> Result<Vec<SimDevice>, ControlError> {
        self.send(&ControlRequestOut::SimList { repo_id })
            .await?
            .sim_devices
            .ok_or(ControlError::InvalidResponse)
    }

    pub async fn boot_simulator_device(
        &self,
        repo_id: &str,
        device: &str,
    ) -> Result<(), ControlError> {
        self.send(&ControlRequestOut::SimBoot { repo_id, device })
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
        let request = ControlRequestOut::AuditTail {
            workspace_id,
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

    async fn send(&self, request: &ControlRequestOut<'_>) -> Result<ControlResponse, ControlError> {
        let (mut stream, encoded) = self.connect_and_encode(request).await?;
        stream.write_all(&encoded).await?;
        stream.shutdown().await?;
        let mut reader = BufReader::new(stream);
        let response = read_response_line(&mut reader).await?;
        check_response(&response)?;
        Ok(response)
    }

    async fn connect_and_encode(
        &self,
        request: &ControlRequestOut<'_>,
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
    request: &'a ControlRequestOut<'a>,
}

#[derive(Serialize)]
#[serde(tag = "op", rename_all = "kebab-case")]
enum ControlRequestOut<'a> {
    Status,
    Install {
        session: &'a SessionWire,
    },
    Remove {
        #[serde(rename = "workspaceId")]
        workspace_id: &'a str,
        #[serde(rename = "expectedRevision")]
        expected_revision: u64,
    },
    AuditTail {
        #[serde(rename = "workspaceId")]
        workspace_id: Option<&'a str>,
        #[serde(rename = "afterSequence")]
        after_sequence: Option<u64>,
        limit: usize,
        follow: bool,
    },
    RepoMirror {
        request: &'a RepoMirrorRequest,
    },
    SimConfigure {
        config: &'a SimProjectConfig,
    },
    SimApprove {
        approval: &'a SimInstallApproval,
    },
    SimList {
        #[serde(rename = "repoId")]
        repo_id: &'a str,
    },
    SimBoot {
        #[serde(rename = "repoId")]
        repo_id: &'a str,
        device: &'a str,
    },
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "kebab-case", deny_unknown_fields)]
enum ControlRequestIn {
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
    status: Option<GatewayStatus>,
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
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ControlFailureCode {
    InvalidRequest,
    Unauthorized,
    InvalidSession,
    RevisionFence,
    EndpointConflict,
    NotAdmitted,
    BrokerRejected,
    Rejected,
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
    Tcp { address: String },
    Unix { path: PathBuf },
}

impl From<&WorkspaceEndpoint> for EndpointWire {
    fn from(endpoint: &WorkspaceEndpoint) -> Self {
        match endpoint {
            WorkspaceEndpoint::Tcp(address) => Self::Tcp {
                address: address.to_string(),
            },
            WorkspaceEndpoint::Unix(path) => Self::Unix { path: path.clone() },
        }
    }
}

impl EndpointWire {
    fn to_endpoint(&self) -> Result<WorkspaceEndpoint, ControlError> {
        match self {
            Self::Tcp { address } => address
                .parse::<SocketAddr>()
                .map(WorkspaceEndpoint::Tcp)
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
            mirrors: self.mirrors.iter().map(MirrorWire::to_route).collect(),
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
    impersonate: bool,
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
            impersonate: grant.impersonate,
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
            impersonate: self.impersonate,
        })
    }
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct MirrorWire {
    local_prefix: String,
    upstream_origin: String,
    protocol: MirrorProtocol,
    admitted_prefixes: Vec<String>,
    credentialed: bool,
}

impl From<&MirrorRoute> for MirrorWire {
    fn from(route: &MirrorRoute) -> Self {
        Self {
            local_prefix: route.local_prefix.clone(),
            upstream_origin: route.upstream_origin.clone(),
            protocol: route.protocol,
            admitted_prefixes: route.admitted_prefixes.clone(),
            credentialed: route.credentialed,
        }
    }
}

impl MirrorWire {
    fn to_route(&self) -> MirrorRoute {
        MirrorRoute {
            local_prefix: self.local_prefix.clone(),
            upstream_origin: self.upstream_origin.clone(),
            protocol: self.protocol,
            admitted_prefixes: self.admitted_prefixes.clone(),
            credentialed: self.credentialed,
        }
    }
}

#[derive(Clone)]
pub(crate) struct ControlServices {
    pub handle: GatewayHandle,
    pub repo_mirror: RepoMirrorHandle,
    pub sim_broker: SimBrokerHandle,
    pub audit_tail: Option<AuditTailHandle>,
}

impl fmt::Debug for ControlServices {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ControlServices")
            .field("handle", &self.handle)
            .field("repo_mirror", &self.repo_mirror)
            .field("sim_broker", &self.sim_broker)
            .field("audit_tail", &self.audit_tail)
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
    let (reader, writer) = stream.into_split();
    let request = read_frame(reader)
        .await
        .and_then(|bytes| parse_control_request(&bytes));
    serve_request(writer, request, services).await;
}

pub(crate) async fn serve_control_tcp(
    stream: TcpStream,
    credential: std::sync::Arc<ControllerCredential>,
    services: ControlServices,
) {
    let (reader, writer) = stream.into_split();
    let request = read_frame(reader).await.and_then(|bytes| {
        let mut envelope: AuthenticatedControlRequestIn = serde_json::from_slice(&bytes)
            .map_err(|error| ControlError::Encoding(error.to_string()))?;
        if !credential.matches(&envelope.controller_credential) {
            return Err(ControlError::Unauthorized);
        }
        envelope.controller_credential.zeroize();
        parse_control_request_value(envelope.request)
    });
    serve_request(writer, request, services).await;
}

fn parse_control_request(bytes: &[u8]) -> Result<ControlRequestIn, ControlError> {
    let value =
        serde_json::from_slice(bytes).map_err(|error| ControlError::Encoding(error.to_string()))?;
    parse_control_request_value(value)
}

fn parse_control_request_value(value: serde_json::Value) -> Result<ControlRequestIn, ControlError> {
    let object = value
        .as_object()
        .ok_or_else(|| ControlError::Encoding("control request must be an object".to_owned()))?;
    let op = object
        .get("op")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| ControlError::Encoding("control request op is required".to_owned()))?;
    let allowed: &[&str] = match op {
        "status" => &["op"],
        "install" => &["op", "session"],
        "remove" => &["op", "workspaceId", "expectedRevision"],
        "audit-tail" => &["op", "workspaceId", "afterSequence", "limit", "follow"],
        "repo-mirror" => &["op", "request"],
        "sim-configure" => &["op", "config"],
        "sim-approve" => &["op", "approval"],
        "sim-list" => &["op", "repoId"],
        "sim-boot" => &["op", "repoId", "device"],
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
    serde_json::from_value(value).map_err(|error| ControlError::Encoding(error.to_string()))
}

async fn serve_request<W>(
    mut writer: W,
    request: Result<ControlRequestIn, ControlError>,
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
                ok: true,
                status: None,
                audit_events: None,
                audit_event: Some(event),
                mirror_info: None,
                sim_devices: None,
                code: None,
                error: None,
            };
            if write_response(&mut writer, &response).await.is_err() {
                break;
            }
        }
    }
    let _ = writer.shutdown().await;
}

async fn dispatch(
    request: ControlRequestIn,
    services: &ControlServices,
) -> (ControlResponse, Option<mpsc::Receiver<AuditEvent>>) {
    match request {
        ControlRequestIn::Status => (
            match services.handle.status().await {
                Ok(status) => ControlResponse {
                    status: Some(status),
                    ..success()
                },
                Err(error) => rejected(error),
            },
            None,
        ),
        ControlRequestIn::Install { session } => (
            match session.into_session() {
                Ok(session) => match services.handle.install(session).await {
                    Ok(()) => success(),
                    Err(error) => rejected(error),
                },
                Err(error) => invalid_session(error.to_string()),
            },
            None,
        ),
        ControlRequestIn::Remove {
            workspace_id,
            expected_revision,
        } => (
            match services
                .handle
                .remove(workspace_id, expected_revision)
                .await
            {
                Ok(()) => success(),
                Err(error) => rejected(error),
            },
            None,
        ),
        ControlRequestIn::AuditTail {
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
        ControlRequestIn::RepoMirror { request } => (
            match services.repo_mirror.mirror(request).await {
                Ok(info) => ControlResponse {
                    mirror_info: Some(info),
                    ..success()
                },
                Err(error) => repo_rejected(error),
            },
            None,
        ),
        ControlRequestIn::SimConfigure { config } => (
            match services.sim_broker.configure(config).await {
                Ok(()) => success(),
                Err(error) => sim_rejected(error),
            },
            None,
        ),
        ControlRequestIn::SimApprove { approval } => (
            match services.sim_broker.approve(approval).await {
                Ok(()) => success(),
                Err(error) => sim_rejected(error),
            },
            None,
        ),
        ControlRequestIn::SimList { repo_id } => (
            match services.sim_broker.list_devices(repo_id).await {
                Ok(devices) => ControlResponse {
                    sim_devices: Some(devices),
                    ..success()
                },
                Err(error) => sim_rejected(error),
            },
            None,
        ),
        ControlRequestIn::SimBoot { repo_id, device } => (
            match services.sim_broker.boot_device(repo_id, device).await {
                Ok(()) => success(),
                Err(error) => sim_rejected(error),
            },
            None,
        ),
    }
}

async fn read_frame<R>(reader: R) -> Result<Zeroizing<Vec<u8>>, ControlError>
where
    R: AsyncRead + Unpin,
{
    let mut bytes = Zeroizing::new(Vec::new());
    reader
        .take(MAX_CONTROL_MESSAGE + 1)
        .read_to_end(&mut bytes)
        .await?;
    if bytes.is_empty()
        || bytes.len() > MAX_CONTROL_MESSAGE as usize
        || !bytes.ends_with(b"\n")
        || bytes[..bytes.len() - 1]
            .iter()
            .any(|byte| matches!(byte, b'\n' | b'\r'))
    {
        return Err(ControlError::MessageTooLarge);
    }
    Ok(bytes)
}

async fn read_response_line<R>(reader: &mut BufReader<R>) -> Result<ControlResponse, ControlError>
where
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

fn success() -> ControlResponse {
    ControlResponse {
        ok: true,
        status: None,
        audit_events: None,
        audit_event: None,
        mirror_info: None,
        sim_devices: None,
        code: None,
        error: None,
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

fn invalid_session(error: String) -> ControlResponse {
    failure(ControlFailureCode::InvalidSession, error)
}

fn rejected(error: GatewayError) -> ControlResponse {
    let code = match error {
        GatewayError::StaleRevision | GatewayError::RevisionMismatch { .. } => {
            ControlFailureCode::RevisionFence
        }
        GatewayError::EndpointInUse => ControlFailureCode::EndpointConflict,
        GatewayError::Config(_) | GatewayError::Tls(_) => ControlFailureCode::InvalidSession,
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
}

pub fn control_socket_path(home: &Path) -> PathBuf {
    home.join(".cowshed/gateway.sock")
}

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
            r#"{"op":"repo-mirror","request":{"workspaceId":"ws","repoId":"repo","remote":"https://example.test/repo"},"push":true}"#,
            r#"{"op":"sim-openurl","url":"demo://value"}"#,
            r#"{"op":"unknown"}"#,
        ] {
            assert!(parse_control_request(request.as_bytes()).is_err());
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
        let (mut writer, reader) = tokio::io::duplex(128);
        writer
            .write_all(b"{\"op\":\"status\"}\n{\"op\":\"status\"}\n")
            .await
            .expect("write");
        writer.shutdown().await.expect("shutdown");
        assert!(matches!(
            read_frame(reader).await,
            Err(ControlError::MessageTooLarge)
        ));
    }
}
