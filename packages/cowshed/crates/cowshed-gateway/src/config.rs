use std::{fmt, net::SocketAddr, num::NonZeroUsize, path::PathBuf, time::Duration};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use subtle::ConstantTimeEq;
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

use crate::policy::WorkspacePolicy;

pub const TOKEN_BYTES: usize = 32;
pub const MACOS_PORT_MIN: u16 = 40_960;
pub const MACOS_PORT_MAX: u16 = 49_151;
pub const MACOS_PORT_BLOCK_SIZE: u16 = 16;

/// Host-side endpoint that selects a workspace before bearer authentication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkspaceEndpoint {
    Tcp(SocketAddr),
    Unix(PathBuf),
}

impl WorkspaceEndpoint {
    pub fn validate(&self) -> Result<(), ConfigError> {
        match self {
            Self::Tcp(address) if !address.ip().is_loopback() => {
                Err(ConfigError::NonLoopbackEndpoint)
            }
            Self::Tcp(address) if address.port() == 0 => Err(ConfigError::ZeroPort),
            Self::Tcp(_) => Ok(()),
            Self::Unix(path) if !path.is_absolute() => Err(ConfigError::RelativeSocketPath),
            Self::Unix(path) if path.as_os_str().is_empty() => Err(ConfigError::RelativeSocketPath),
            Self::Unix(_) => Ok(()),
        }
    }

    pub fn validate_for_current_platform(&self) -> Result<(), ConfigError> {
        #[cfg(target_os = "macos")]
        {
            self.validate_macos_port_block()
        }
        #[cfg(target_os = "linux")]
        {
            self.validate()?;
            match self {
                Self::Unix(_) => Ok(()),
                Self::Tcp(_) => Err(ConfigError::ExpectedUnixEndpoint),
            }
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            Err(ConfigError::UnsupportedHostPlatform)
        }
    }

    /// Enforces the frozen macOS 16-port allocation range for production sessions.
    pub fn validate_macos_port_block(&self) -> Result<(), ConfigError> {
        self.validate()?;
        let Self::Tcp(address) = self else {
            return Err(ConfigError::ExpectedTcpEndpoint);
        };
        let last = address
            .port()
            .checked_add(MACOS_PORT_BLOCK_SIZE - 1)
            .ok_or(ConfigError::InvalidMacosPortBlock)?;
        if address.port() < MACOS_PORT_MIN
            || last > MACOS_PORT_MAX
            || !(address.port() - MACOS_PORT_MIN).is_multiple_of(MACOS_PORT_BLOCK_SIZE)
        {
            return Err(ConfigError::InvalidMacosPortBlock);
        }
        Ok(())
    }
}

/// A validated 256-bit workspace bearer token. Debug output never contains the token.
#[derive(Clone)]
pub struct WorkspaceToken([u8; TOKEN_BYTES]);

impl WorkspaceToken {
    pub fn from_bytes(bytes: [u8; TOKEN_BYTES]) -> Self {
        Self(bytes)
    }

    pub fn parse(encoded: &str) -> Result<Self, ConfigError> {
        if encoded.contains('=') {
            return Err(ConfigError::MalformedToken);
        }
        let decoded = URL_SAFE_NO_PAD
            .decode(encoded)
            .map_err(|_| ConfigError::MalformedToken)?;
        let bytes: [u8; TOKEN_BYTES] = decoded
            .try_into()
            .map_err(|_| ConfigError::MalformedToken)?;
        Ok(Self(bytes))
    }

    pub fn encode(&self) -> String {
        URL_SAFE_NO_PAD.encode(self.0)
    }

    pub(crate) fn matches_encoded(&self, encoded: &str) -> bool {
        let Ok(candidate) = URL_SAFE_NO_PAD.decode(encoded) else {
            return false;
        };
        if candidate.len() != TOKEN_BYTES || encoded.contains('=') {
            return false;
        }
        self.0.ct_eq(candidate.as_slice()).into()
    }
}

impl Drop for WorkspaceToken {
    fn drop(&mut self) {
        self.0.zeroize();
    }
}

impl fmt::Debug for WorkspaceToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("WorkspaceToken([REDACTED])")
    }
}

/// Controller-owned CA material. The private key is never serialized or printed.
pub struct WorkspaceCa {
    pub certificate_pem: String,
    pub private_key_pem: Zeroizing<String>,
}

impl WorkspaceCa {
    pub fn new(certificate_pem: String, private_key_pem: String) -> Result<Self, ConfigError> {
        if !certificate_pem.contains("BEGIN CERTIFICATE")
            || !private_key_pem.contains("BEGIN PRIVATE KEY")
        {
            return Err(ConfigError::MalformedCa);
        }
        Ok(Self {
            certificate_pem,
            private_key_pem: Zeroizing::new(private_key_pem),
        })
    }
}

impl fmt::Debug for WorkspaceCa {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceCa")
            .field("certificate_pem", &"[PUBLIC CERTIFICATE]")
            .field("private_key_pem", &"[REDACTED]")
            .finish()
    }
}

/// Complete trusted session installation delivered by the host controller.
pub struct WorkspaceSession {
    pub workspace_id: String,
    pub repo_id: String,
    pub revision: u64,
    pub endpoint: WorkspaceEndpoint,
    pub token: WorkspaceToken,
    pub ca: WorkspaceCa,
    pub policy: WorkspacePolicy,
}

impl WorkspaceSession {
    pub fn validate(&self) -> Result<(), ConfigError> {
        validate_identifier("workspace_id", &self.workspace_id)?;
        validate_identifier("repo_id", &self.repo_id)?;
        self.endpoint.validate_for_current_platform()?;
        self.policy.validate()?;
        Ok(())
    }
}

impl fmt::Debug for WorkspaceSession {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("WorkspaceSession")
            .field("workspace_id", &self.workspace_id)
            .field("repo_id", &self.repo_id)
            .field("revision", &self.revision)
            .field("endpoint", &self.endpoint)
            .field("token", &self.token)
            .field("ca", &self.ca)
            .field("policy", &self.policy)
            .finish()
    }
}

fn validate_identifier(field: &'static str, value: &str) -> Result<(), ConfigError> {
    if value.is_empty()
        || value.len() > 128
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(ConfigError::InvalidIdentifier { field });
    }
    Ok(())
}

#[derive(Clone, Copy, Debug)]
pub struct GatewayLimits {
    pub workspace_active: usize,
    pub workspace_queued: usize,
    pub global_active: usize,
    pub global_queued: usize,
    pub origin_active: usize,
    pub leaf_cache_workspace: usize,
    pub leaf_cache_global: usize,
}

impl Default for GatewayLimits {
    fn default() -> Self {
        Self {
            workspace_active: 32,
            workspace_queued: 64,
            global_active: 256,
            global_queued: 512,
            origin_active: 8,
            leaf_cache_workspace: 256,
            leaf_cache_global: 4096,
        }
    }
}

impl GatewayLimits {
    pub fn validate(&self) -> Result<(), ConfigError> {
        let fields = [
            self.workspace_active,
            self.workspace_queued,
            self.global_active,
            self.global_queued,
            self.origin_active,
            self.leaf_cache_workspace,
            self.leaf_cache_global,
        ];
        if fields.into_iter().any(|value| value == 0) {
            return Err(ConfigError::ZeroLimit);
        }
        if self.workspace_active > self.global_active
            || self.workspace_queued > self.global_queued
            || self.leaf_cache_workspace > self.leaf_cache_global
        {
            return Err(ConfigError::InconsistentLimits);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GatewayTimeouts {
    pub request_headers: Duration,
    pub connect: Duration,
    pub tls_handshake: Duration,
    pub response_headers: Duration,
    pub body_idle: Duration,
    pub request_total: Duration,
    pub tunnel_total: Duration,
    pub leaf_lifetime: Duration,
}

impl Default for GatewayTimeouts {
    fn default() -> Self {
        Self {
            request_headers: Duration::from_secs(10),
            connect: Duration::from_secs(5),
            tls_handshake: Duration::from_secs(10),
            response_headers: Duration::from_secs(60),
            body_idle: Duration::from_secs(120),
            request_total: Duration::from_secs(15 * 60),
            tunnel_total: Duration::from_secs(60 * 60),
            leaf_lifetime: Duration::from_secs(24 * 60 * 60),
        }
    }
}

impl GatewayTimeouts {
    pub fn validate(&self) -> Result<(), ConfigError> {
        if [
            self.request_headers,
            self.connect,
            self.tls_handshake,
            self.response_headers,
            self.body_idle,
            self.request_total,
            self.tunnel_total,
            self.leaf_lifetime,
        ]
        .into_iter()
        .any(|value| value.is_zero())
        {
            return Err(ConfigError::ZeroTimeout);
        }
        if self.request_total < self.response_headers || self.tunnel_total < self.request_total {
            return Err(ConfigError::InconsistentTimeouts);
        }
        Ok(())
    }
}

/// Host daemon configuration. Runtime protocol limits are deliberately not configurable.
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    pub control_socket: Option<PathBuf>,
    pub authorized_control_uid: u32,
    pub limits: GatewayLimits,
    pub timeouts: GatewayTimeouts,
    pub command_capacity: NonZeroUsize,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            control_socket: None,
            authorized_control_uid: unsafe { libc::geteuid() },
            limits: GatewayLimits::default(),
            timeouts: GatewayTimeouts::default(),
            command_capacity: NonZeroUsize::new(1024).expect("1024 is non-zero"),
        }
    }
}

impl GatewayConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.limits.validate()?;
        self.timeouts.validate()?;
        if let Some(path) = &self.control_socket
            && !path.is_absolute()
        {
            return Err(ConfigError::RelativeSocketPath);
        }
        Ok(())
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{field} must be 1-128 ASCII identifier characters")]
    InvalidIdentifier { field: &'static str },
    #[error("workspace endpoint must be loopback")]
    NonLoopbackEndpoint,
    #[error("workspace endpoint port must be non-zero")]
    ZeroPort,
    #[error("Unix socket paths must be absolute")]
    RelativeSocketPath,
    #[error("a TCP endpoint is required")]
    ExpectedTcpEndpoint,
    #[error("a Unix endpoint is required")]
    ExpectedUnixEndpoint,
    #[error("gateway endpoints are unsupported on this host platform")]
    UnsupportedHostPlatform,
    #[error("macOS gateway base must reserve 16 ports within 40960-49151")]
    InvalidMacosPortBlock,
    #[error("workspace token must be exactly 32 bytes of unpadded base64url")]
    MalformedToken,
    #[error("workspace CA certificate or PKCS#8 private key is malformed")]
    MalformedCa,
    #[error("gateway limits must be non-zero")]
    ZeroLimit,
    #[error("per-workspace limits cannot exceed global limits")]
    InconsistentLimits,
    #[error("gateway timeouts must be non-zero")]
    ZeroTimeout,
    #[error("gateway timeout ordering is inconsistent")]
    InconsistentTimeouts,
    #[error(transparent)]
    Policy(#[from] crate::policy::PolicyError),
}
