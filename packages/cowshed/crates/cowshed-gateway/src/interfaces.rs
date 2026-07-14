use std::{collections::BTreeSet, fmt, io, net::IpAddr, sync::Arc, time::Duration};

use async_trait::async_trait;
use http::{HeaderName, Method};
use rustls::{ClientConfig, pki_types::ServerName};
use rustls_platform_verifier::ConfigVerifierExt;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::{
    io::{AsyncRead, AsyncWrite},
    net::TcpStream,
    time::timeout,
};
use tokio_rustls::TlsConnector;
use zeroize::Zeroizing;

use crate::{
    mirror::MirrorCacheStatus,
    policy::{CanonicalHost, CanonicalTarget, MirrorProtocol, normalize_path},
};

pub trait GatewayIo: AsyncRead + AsyncWrite + Send + Unpin + 'static {}
impl<T> GatewayIo for T where T: AsyncRead + AsyncWrite + Send + Unpin + 'static {}
pub type BoxIo = Box<dyn GatewayIo>;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum NegotiatedTransport {
    Http1,
    Http2,
    Raw,
}

pub struct UpstreamConnection {
    pub io: BoxIo,
    pub transport: NegotiatedTransport,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum UpstreamHealth {
    Healthy,
    Offline,
    Unknown,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum UpstreamPurpose {
    PlainHttp,
    TlsHttp,
    OpaqueTcp,
}

#[derive(Clone, Debug)]
pub struct AuthorizedTarget {
    pub target: CanonicalTarget,
    pub purpose: UpstreamPurpose,
    /// True only for an exact trusted host/IP grant, never a wildcard.
    pub private_network_authorized: bool,
}

#[async_trait]
pub trait UpstreamConnector: Send + Sync + 'static {
    async fn health(&self, target: &CanonicalTarget) -> UpstreamHealth;
    async fn connect(&self, target: &AuthorizedTarget) -> Result<UpstreamConnection, ConnectError>;
}

/// Production connector: authorizes before DNS, rejects wildcard-to-private rebinding,
/// pins one authorized address, and uses the operating system verifier for TLS.
pub struct SystemConnector {
    connect_timeout: Duration,
    tls_timeout: Duration,
    tls: Arc<ClientConfig>,
}

impl fmt::Debug for SystemConnector {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SystemConnector")
            .field("connect_timeout", &self.connect_timeout)
            .field("tls_timeout", &self.tls_timeout)
            .finish_non_exhaustive()
    }
}

impl SystemConnector {
    pub fn new(connect_timeout: Duration, tls_timeout: Duration) -> Result<Self, ConnectError> {
        let mut tls = ClientConfig::with_platform_verifier()
            .map_err(|error| ConnectError::TlsConfiguration(error.to_string()))?;
        tls.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
        Ok(Self {
            connect_timeout,
            tls_timeout,
            tls: Arc::new(tls),
        })
    }
}

#[async_trait]
impl UpstreamConnector for SystemConnector {
    async fn health(&self, _target: &CanonicalTarget) -> UpstreamHealth {
        UpstreamHealth::Unknown
    }

    async fn connect(
        &self,
        authorized: &AuthorizedTarget,
    ) -> Result<UpstreamConnection, ConnectError> {
        let target = &authorized.target;
        let addresses = tokio::net::lookup_host((target.host.as_str(), target.port))
            .await
            .map_err(ConnectError::Resolve)?;
        let mut last_error = None;
        let mut stream = None;
        for address in addresses {
            if is_private(address.ip()) && !authorized.private_network_authorized {
                last_error = Some(ConnectError::PrivateAddressDenied(address.ip()));
                continue;
            }
            match timeout(self.connect_timeout, TcpStream::connect(address)).await {
                Ok(Ok(candidate)) => {
                    stream = Some(candidate);
                    break;
                }
                Ok(Err(error)) => last_error = Some(ConnectError::Io(error)),
                Err(_) => last_error = Some(ConnectError::ConnectTimeout),
            }
        }
        let stream = stream.ok_or_else(|| last_error.unwrap_or(ConnectError::NoAddresses))?;
        stream.set_nodelay(true).map_err(ConnectError::Io)?;
        if authorized.purpose != UpstreamPurpose::TlsHttp {
            let transport = match authorized.purpose {
                UpstreamPurpose::PlainHttp => NegotiatedTransport::Http1,
                UpstreamPurpose::OpaqueTcp => NegotiatedTransport::Raw,
                UpstreamPurpose::TlsHttp => unreachable!("TLS HTTP returned above"),
            };
            return Ok(UpstreamConnection {
                io: Box::new(stream),
                transport,
            });
        }
        let server_name = match &target.host {
            CanonicalHost::Dns(host) => ServerName::try_from(host.clone()),
            CanonicalHost::Ip(ip) => Ok(ServerName::IpAddress((*ip).into())),
        }
        .map_err(|_| ConnectError::InvalidServerName)?;
        let tls = timeout(
            self.tls_timeout,
            TlsConnector::from(Arc::clone(&self.tls)).connect(server_name, stream),
        )
        .await
        .map_err(|_| ConnectError::TlsTimeout)?
        .map_err(|error| ConnectError::Tls(error.to_string()))?;
        let transport = match tls.get_ref().1.alpn_protocol() {
            Some(b"h2") => NegotiatedTransport::Http2,
            Some(b"http/1.1") => NegotiatedTransport::Http1,
            Some(_) => return Err(ConnectError::UnsupportedAlpn),
            None => return Err(ConnectError::MissingAlpn),
        };
        Ok(UpstreamConnection {
            io: Box::new(tls),
            transport,
        })
    }
}

fn is_private(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => {
            ip.is_private()
                || ip.is_loopback()
                || ip.is_link_local()
                || ip.is_broadcast()
                || ip.is_documentation()
                || ip.octets()[0] == 0
        }
        IpAddr::V6(ip) => {
            ip.is_loopback()
                || ip.is_unspecified()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
        }
    }
}

#[derive(Debug, Error)]
pub enum ConnectError {
    #[error("DNS resolution failed: {0}")]
    Resolve(io::Error),
    #[error("upstream connection failed: {0}")]
    Io(io::Error),
    #[error("upstream resolution returned no authorized addresses")]
    NoAddresses,
    #[error("resolved private address {0} requires an exact grant")]
    PrivateAddressDenied(IpAddr),
    #[error("upstream connect timed out")]
    ConnectTimeout,
    #[error("upstream TLS handshake timed out")]
    TlsTimeout,
    #[error("upstream TLS verification failed: {0}")]
    Tls(String),
    #[error("TLS verifier initialization failed: {0}")]
    TlsConfiguration(String),
    #[error("target cannot be represented as a TLS server name")]
    InvalidServerName,
    #[error("upstream TLS did not negotiate ALPN")]
    MissingAlpn,
    #[error("upstream TLS negotiated an unsupported ALPN protocol")]
    UnsupportedAlpn,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CredentialProtocol {
    Generic,
    Npm,
    Cargo,
    Go,
}

impl From<MirrorProtocol> for CredentialProtocol {
    fn from(value: MirrorProtocol) -> Self {
        match value {
            MirrorProtocol::Npm => Self::Npm,
            MirrorProtocol::Cargo => Self::Cargo,
            MirrorProtocol::Go => Self::Go,
        }
    }
}

#[derive(Clone, Debug)]
pub struct CredentialQuery {
    pub workspace_id: String,
    pub repo_id: String,
    pub protocol: CredentialProtocol,
    pub origin: String,
    pub method: Method,
    pub path: String,
}

pub struct CredentialRecord {
    pub repo_id: String,
    pub protocol: CredentialProtocol,
    pub origin: String,
    pub methods: BTreeSet<String>,
    pub path_prefixes: Vec<String>,
    pub header_name: HeaderName,
    pub header_value: Zeroizing<String>,
}

impl fmt::Debug for CredentialRecord {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CredentialRecord")
            .field("repo_id", &self.repo_id)
            .field("protocol", &self.protocol)
            .field("origin", &self.origin)
            .field("methods", &self.methods)
            .field("path_prefixes", &self.path_prefixes)
            .field("header_name", &self.header_name)
            .field("header_value", &"[REDACTED]")
            .finish()
    }
}

impl CredentialRecord {
    pub(crate) fn validate_for(&self, query: &CredentialQuery) -> bool {
        self.repo_id == query.repo_id
            && self.protocol == query.protocol
            && self.origin == query.origin
            && self.methods.contains(query.method.as_str())
            && normalize_path(&query.path).is_ok_and(|path| {
                self.path_prefixes.iter().any(|prefix| {
                    normalize_path(prefix).is_ok_and(|allowed| path.starts_with(&allowed))
                })
            })
            && !matches!(
                self.header_name.as_str(),
                "proxy-authorization" | "cookie" | "set-cookie"
            )
            && http::HeaderValue::from_str(self.header_value.as_str()).is_ok()
    }
}

#[async_trait]
pub trait CredentialProvider: Send + Sync + 'static {
    async fn lookup(
        &self,
        query: &CredentialQuery,
    ) -> Result<Option<CredentialRecord>, CredentialError>;
}

#[derive(Debug, Error)]
pub enum CredentialError {
    #[error("credential store unavailable: {0}")]
    Unavailable(String),
    #[error("credential record does not match the admitted request")]
    ScopeMismatch,
    #[error("credential value is not a valid HTTP header")]
    InvalidHeader,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AuditEvent {
    pub sequence: u64,
    pub timestamp_unix_ms: u64,
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
    pub upstream_span_id: Option<u64>,
    pub parent_span_id: Option<u64>,
    pub tracestate: Option<String>,
    pub grant_hint: Option<String>,
    pub classification: Option<String>,
    pub mirror_cache_status: Option<MirrorCacheStatus>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuditKind {
    Http,
    Connect,
    Intercept,
    Opaque,
    Npm,
    Cargo,
    Go,
    Sim,
    RepoMirror,
    AuditTail,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum AuditStatus {
    Allowed,
    Denied,
    Unauthorized,
    Limited,
    Offline,
    Failed,
    Completed,
    TimedOut,
    Cancelled,
}

#[async_trait]
pub trait AuditSink: Send + Sync + 'static {
    async fn record(&self, event: AuditEvent) -> Result<(), AuditError>;
    async fn flush(&self) -> Result<(), AuditError>;
}

#[derive(Debug, Error)]
#[error("audit sink failed: {0}")]
pub struct AuditError(pub String);

pub(crate) type BoxError = Box<dyn std::error::Error + Send + Sync>;
