//! The trusted session a host controller installs into the gateway.
//!
//! Every field is validated data with no runtime attached: an endpoint, a bearer token, CA
//! material, and an egress policy. The daemon's own configuration — listeners, limits, timeouts,
//! cache roots — stays in `cowshed-gateway`, because only the daemon has any use for it.

use std::{fmt, net::SocketAddr, path::PathBuf};

use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use subtle::ConstantTimeEq;
use thiserror::Error;
use zeroize::{Zeroize, Zeroizing};

use crate::{policy::WorkspacePolicy, repo_id::validate_repo_id};

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

/// The label the gateway reports for a session's endpoint in [`crate::SessionStatus`]; a
/// controller compares the endpoint it intends to install against this exact rendering.
impl fmt::Display for WorkspaceEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp(address) => write!(formatter, "{address}"),
            Self::Unix(path) => write!(formatter, "{}", path.display()),
        }
    }
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

    /// Constant-time comparison against an encoded candidate.
    ///
    /// The only sound way to check a bearer token, and public because the daemon that
    /// authenticates connections lives in another crate: the encoded form is decoded first so a
    /// wrong length or stray padding is rejected without touching the secret, and the byte
    /// comparison is constant-time so reply latency carries no information about how much of the
    /// token a caller guessed.
    pub fn matches_encoded(&self, encoded: &str) -> bool {
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
        validate_repo_id(&self.repo_id).map_err(|_| ConfigError::InvalidRepoId)?;
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

/// The identifier grammar shared by every name the control plane carries.
///
/// Public because the daemon validates its own configured socket names against the same grammar:
/// one definition, so a name the controller can install is exactly a name the daemon accepts.
pub fn validate_identifier(field: &'static str, value: &str) -> Result<(), ConfigError> {
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

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("{field} must be 1-128 ASCII identifier characters")]
    InvalidIdentifier { field: &'static str },
    #[error("repo_id must be exactly two 1-128 ASCII identifier components joined by '/'")]
    InvalidRepoId,
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
    #[error("Linux gateway data socket root is required")]
    MissingDataSocketRoot,
    #[error("Linux gateway data socket root must be an owned mode-0700 real directory")]
    InsecureDataSocketRoot,
    #[error("workspace data socket must be directly inside the authoritative root")]
    EndpointOutsideDataSocketRoot,
    #[error("workspace data socket name must be an identifier with .sock suffix")]
    InvalidDataSocketName,
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
    #[error("gateway mirror cache root is required and must be absolute")]
    MissingMirrorCacheRoot,
    #[error("gateway mirror cache root must be a pre-existing real directory")]
    InsecureMirrorCacheRoot,
    #[error("production gateway requires an explicit Git fetch helper executable")]
    MissingGitHelperExecutable,
    #[error(
        "gateway Git fetch helper must be an absolute owned executable without group/world write access"
    )]
    InvalidGitHelperExecutable,
    #[error("production gateway requires the canonical gateway.sock control endpoint")]
    MissingProductionControlSocket,
    #[error("production mirror cache must be the owned mode-0700 <cache-volume>/mirror directory")]
    InvalidProductionCacheRoot,
    #[error("gateway mirror cache low-water/TTL limits are invalid")]
    InvalidMirrorCacheLimits,
    #[error("gateway control TCP listener must be exactly 127.0.0.1:7644")]
    InvalidControlTcpAddress,
    #[error(
        "gateway controller credential must be a real mode-0600 file directly under the validated host root"
    )]
    InvalidControlCredentialFile,
    #[error(transparent)]
    Policy(#[from] crate::policy::PolicyError),
}

#[cfg(test)]
mod tests {
    use crate::repo_id::validate_repo_id;

    #[test]
    fn repository_ids_use_the_store_grammar() {
        for value in ["owner/repo", "owner/repo.name", "0owner/repo_1"] {
            assert!(validate_repo_id(value).is_ok(), "{value}");
        }
        let over_length = format!("owner/{}", "a".repeat(129));
        for value in [
            "",
            "owner",
            "Owner/repo",
            "owner/Repo",
            "-owner/repo",
            "owner/-repo",
            "owner/",
            over_length.as_str(),
        ] {
            assert!(validate_repo_id(value).is_err(), "{value}");
        }
    }
}
