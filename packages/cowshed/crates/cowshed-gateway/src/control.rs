use std::{
    collections::BTreeSet,
    net::SocketAddr,
    path::{Path, PathBuf},
};

use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::{UnixStream, unix::OwnedReadHalf},
};
use zeroize::{Zeroize, Zeroizing};

use crate::{
    actor::{GatewayError, GatewayHandle, GatewayStatus},
    config::{WorkspaceCa, WorkspaceEndpoint, WorkspaceSession, WorkspaceToken},
    policy::{EgressGrant, EgressMode, HostPattern, MirrorProtocol, MirrorRoute, WorkspacePolicy},
};

const MAX_CONTROL_MESSAGE: u64 = 1024 * 1024;

/// Host-side client used by an independently running coordinator to fence and
/// rotate authoritative workspace sessions in the gateway daemon.
#[derive(Clone, Debug)]
pub struct GatewayControlClient {
    socket: PathBuf,
}

impl GatewayControlClient {
    pub fn new(socket: PathBuf) -> Result<Self, ControlError> {
        if !socket.is_absolute() {
            return Err(ControlError::InvalidSocketPath);
        }
        Ok(Self { socket })
    }

    pub async fn status(&self) -> Result<GatewayStatus, ControlError> {
        let response = self.send(&ControlRequestOut::Status).await?;
        response.status.ok_or(ControlError::InvalidResponse)
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

    async fn send(&self, request: &ControlRequestOut<'_>) -> Result<ControlResponse, ControlError> {
        let mut encoded = Zeroizing::new(
            serde_json::to_vec(request)
                .map_err(|error| ControlError::Encoding(error.to_string()))?,
        );
        encoded.push(b'\n');
        if encoded.len() > MAX_CONTROL_MESSAGE as usize {
            return Err(ControlError::MessageTooLarge);
        }
        let mut stream = UnixStream::connect(&self.socket).await?;
        stream.write_all(&encoded).await?;
        stream.shutdown().await?;
        let mut response = Vec::new();
        stream
            .take(MAX_CONTROL_MESSAGE + 1)
            .read_to_end(&mut response)
            .await?;
        if response.is_empty() || response.len() > MAX_CONTROL_MESSAGE as usize {
            return Err(ControlError::InvalidResponse);
        }
        let response: ControlResponse = serde_json::from_slice(&response)
            .map_err(|error| ControlError::Encoding(error.to_string()))?;
        if response.ok {
            Ok(response)
        } else {
            Err(ControlError::Rejected {
                code: response.code.ok_or(ControlError::InvalidResponse)?,
                message: response.error.ok_or(ControlError::InvalidResponse)?,
            })
        }
    }
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
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ControlResponse {
    ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    status: Option<GatewayStatus>,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<ControlFailureCode>,
    #[serde(skip_serializing_if = "Option::is_none")]
    error: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq)]
#[serde(rename_all = "kebab-case")]
pub enum ControlFailureCode {
    InvalidRequest,
    InvalidSession,
    RevisionFence,
    EndpointConflict,
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

pub(crate) async fn serve_control(stream: UnixStream, handle: GatewayHandle) {
    let (reader, mut writer) = stream.into_split();
    let response = match read_request(reader).await {
        Ok(ControlRequestIn::Status) => match handle.status().await {
            Ok(status) => ControlResponse {
                ok: true,
                status: Some(status),
                code: None,
                error: None,
            },
            Err(error) => rejected(error),
        },
        Ok(ControlRequestIn::Install { session }) => match session.into_session() {
            Ok(session) => match handle.install(session).await {
                Ok(()) => success(),
                Err(error) => rejected(error),
            },
            Err(error) => invalid_session(error.to_string()),
        },
        Ok(ControlRequestIn::Remove {
            workspace_id,
            expected_revision,
        }) => match handle.remove(workspace_id, expected_revision).await {
            Ok(()) => success(),
            Err(error) => rejected(error),
        },
        Err(error) => ControlResponse {
            ok: false,
            status: None,
            code: Some(ControlFailureCode::InvalidRequest),
            error: Some(error.to_string()),
        },
    };
    if let Ok(mut encoded) = serde_json::to_vec(&response) {
        encoded.push(b'\n');
        let _ = writer.write_all(&encoded).await;
    }
    let _ = writer.shutdown().await;
}

async fn read_request(reader: OwnedReadHalf) -> Result<ControlRequestIn, ControlError> {
    let mut bytes = Zeroizing::new(Vec::new());
    let mut reader = BufReader::new(reader).take(MAX_CONTROL_MESSAGE + 1);
    reader.read_until(b'\n', &mut bytes).await?;
    if bytes.is_empty() || bytes.len() > MAX_CONTROL_MESSAGE as usize || !bytes.ends_with(b"\n") {
        return Err(ControlError::MessageTooLarge);
    }
    serde_json::from_slice(&bytes).map_err(|error| ControlError::Encoding(error.to_string()))
}

fn success() -> ControlResponse {
    ControlResponse {
        ok: true,
        status: None,
        code: None,
        error: None,
    }
}

fn invalid_session(error: String) -> ControlResponse {
    ControlResponse {
        ok: false,
        status: None,
        code: Some(ControlFailureCode::InvalidSession),
        error: Some(error),
    }
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
    ControlResponse {
        ok: false,
        status: None,
        code: Some(code),
        error: Some(error.to_string()),
    }
}

#[derive(Debug, Error)]
pub enum ControlError {
    #[error("gateway control socket path must be absolute")]
    InvalidSocketPath,
    #[error("gateway control message exceeds 1 MiB")]
    MessageTooLarge,
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
}

pub fn control_socket_path(home: &Path) -> PathBuf {
    home.join(".cowshed/gateway.sock")
}
