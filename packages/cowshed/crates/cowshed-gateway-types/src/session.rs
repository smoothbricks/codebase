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
/// The size a newly allocated macOS workspace port block gets. A block's size is data recorded
/// with it when it is allocated, so raising this changes only what new workspaces get: every
/// check validates a live block against its own recorded size, never against this constant.
pub const NEW_PORT_BLOCK_SIZE: u16 = 64;

/// A port block is `size` contiguous ports from `base`, where `size` is a power of two of at
/// least two (the gateway listener plus one service port) and `base` is aligned to it. The
/// alignment is what makes blocks of different sizes nest instead of straddle: two aligned
/// power-of-two blocks either are disjoint or one contains the other.
pub const fn is_port_block(base: u16, size: u16) -> bool {
    size >= 2
        && size.is_power_of_two()
        && base.is_multiple_of(size)
        && base.checked_add(size - 1).is_some()
}

/// A macOS workspace port block additionally lies inside the reserved range.
pub const fn is_macos_port_block(base: u16, size: u16) -> bool {
    is_port_block(base, size) && base >= MACOS_PORT_MIN && base + (size - 1) <= MACOS_PORT_MAX
}

/// Host-side endpoint that selects a workspace before bearer authentication.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WorkspaceEndpoint {
    /// macOS: the gateway listens on the base of the workspace's port block, whose recorded size
    /// travels with it so the block is validated as allocated.
    Tcp {
        address: SocketAddr,
        block_size: u16,
    },
    Unix(PathBuf),
}

/// The label the gateway reports for a session's endpoint in [`crate::SessionStatus`]; a
/// controller compares the endpoint it intends to install against this exact rendering.
impl fmt::Display for WorkspaceEndpoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Tcp { address, .. } => write!(formatter, "{address}"),
            Self::Unix(path) => write!(formatter, "{}", path.display()),
        }
    }
}

impl WorkspaceEndpoint {
    pub fn validate(&self) -> Result<(), ConfigError> {
        match self {
            Self::Tcp { address, .. } if !address.ip().is_loopback() => {
                Err(ConfigError::NonLoopbackEndpoint)
            }
            Self::Tcp { address, .. } if address.port() == 0 => Err(ConfigError::ZeroPort),
            Self::Tcp { .. } => Ok(()),
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
                Self::Tcp { .. } => Err(ConfigError::ExpectedUnixEndpoint),
            }
        }
        #[cfg(not(any(target_os = "macos", target_os = "linux")))]
        {
            Err(ConfigError::UnsupportedHostPlatform)
        }
    }

    /// Enforces the macOS port-block grammar for production sessions against the block's own
    /// recorded size.
    pub fn validate_macos_port_block(&self) -> Result<(), ConfigError> {
        self.validate()?;
        let Self::Tcp {
            address,
            block_size,
        } = self
        else {
            return Err(ConfigError::ExpectedTcpEndpoint);
        };
        if !is_macos_port_block(address.port(), *block_size) {
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
    #[error(
        "macOS gateway endpoint must be the base of a power-of-two port block aligned to its size within 40960-49151"
    )]
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
    use std::{marker::PhantomData, mem::ManuallyDrop};

    use serde::Serialize;

    use super::{
        MACOS_PORT_MAX, MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE, TOKEN_BYTES, WorkspaceCa,
        WorkspaceToken, is_macos_port_block, is_port_block,
    };
    use crate::policy::EgressMode;
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

    /// `WorkspaceToken` must not gain a serializer.
    ///
    /// Every other value in a `WorkspaceSession` is serialized over the control socket; a
    /// `#[derive(Serialize)]` added here out of symmetry would put the live bearer token into
    /// control frames, logs and status output. Rust cannot express "does not implement", so this
    /// resolves the name twice: the inherent method on `Probe<T>` exists only where `T: Serialize`
    /// and shadows the blanket trait method when it applies. If the derive is ever added, the
    /// inherent method wins and the assertion fails.
    #[test]
    fn a_token_is_not_serializable() {
        struct Probe<T>(PhantomData<T>);

        trait NotSerialized {
            const SERIALIZABLE: bool = false;
            fn serializable() -> bool {
                Self::SERIALIZABLE
            }
        }
        impl<T> NotSerialized for Probe<T> {}

        impl<T: Serialize> Probe<T> {
            fn serializable() -> bool {
                true
            }
        }

        assert!(
            !Probe::<WorkspaceToken>::serializable(),
            "WorkspaceToken must stay unserializable"
        );
        // The probe reports honestly: `EgressMode` does derive `Serialize` and answers true.
        assert!(Probe::<EgressMode>::serializable());
    }

    /// Dropping a token must leave no copy of the secret in the memory it occupied.
    ///
    /// `drop_in_place` runs `Drop::drop` while the storage is still live and owned by this frame,
    /// so the zeroized bytes are observable; the payload is `[u8; 32]`, which has no invalid bit
    /// patterns and owns nothing, so reading it afterwards is defined. Nothing else drops it.
    #[test]
    fn dropping_a_token_zeroizes_the_secret() {
        let mut token = ManuallyDrop::new(WorkspaceToken::from_bytes([0xA5; TOKEN_BYTES]));
        assert_eq!(token.0, [0xA5; TOKEN_BYTES]);
        unsafe { std::ptr::drop_in_place(&mut *token) };
        assert_eq!(token.0, [0; TOKEN_BYTES]);
    }

    /// Diagnostics must never carry the token, and there must be no `Display` route around
    /// `Debug`: `encode` is the single deliberate way to render one.
    #[test]
    fn diagnostics_redact_the_token_and_the_ca_key() {
        let encoded = "3q2-796tvu_erb7v3q2-796tvu_erb7v3q2-796tvu8";
        let token = WorkspaceToken::parse(encoded).expect("valid token");
        let rendered = format!("{token:?}");
        assert_eq!(rendered, "WorkspaceToken([REDACTED])");
        assert!(!rendered.contains(encoded));
        assert_eq!(token.encode(), encoded);

        let ca = WorkspaceCa::new(
            "-----BEGIN CERTIFICATE-----\npublic\n-----END CERTIFICATE-----\n".to_owned(),
            "-----BEGIN PRIVATE KEY-----\nsecret\n-----END PRIVATE KEY-----\n".to_owned(),
        )
        .expect("valid CA");
        let rendered = format!("{ca:?}");
        assert!(!rendered.contains("secret"), "{rendered}");
        assert!(rendered.contains("[REDACTED]"), "{rendered}");
    }

    /// A token compares in constant time and rejects anything that is not exactly 32 unpadded
    /// base64url bytes, so a near-miss cannot be distinguished from a wild guess.
    #[test]
    fn token_comparison_admits_only_the_exact_encoding() {
        let bytes = [0x11; TOKEN_BYTES];
        let token = WorkspaceToken::from_bytes(bytes);
        let encoded = token.encode();
        assert!(token.matches_encoded(&encoded));

        let mut other = bytes;
        other[TOKEN_BYTES - 1] ^= 1;
        assert!(!token.matches_encoded(&WorkspaceToken::from_bytes(other).encode()));
        assert!(!token.matches_encoded(&encoded[..encoded.len() - 1]));
        assert!(!token.matches_encoded(&format!("{encoded}=")));
        assert!(!token.matches_encoded(""));
        assert!(!token.matches_encoded("not base64!"));
    }

    /// A block is valid by its own recorded size: the 16-port blocks live workspaces were
    /// allocated with and the 64-port blocks new ones get both validate, each against its own
    /// alignment. Anything that is not an aligned power of two of at least two ports is refused.
    #[test]
    fn port_blocks_validate_against_their_own_size() {
        for size in [16, NEW_PORT_BLOCK_SIZE] {
            assert!(is_macos_port_block(MACOS_PORT_MIN, size), "{size}");
            assert!(
                is_macos_port_block(MACOS_PORT_MAX - size + 1, size),
                "{size}"
            );
            assert!(!is_macos_port_block(MACOS_PORT_MIN - size, size), "{size}");
            assert!(
                !is_macos_port_block(MACOS_PORT_MAX - size + 2, size),
                "{size}"
            );
            assert!(
                !is_macos_port_block(MACOS_PORT_MIN + size / 2, size),
                "{size}"
            );
        }
        assert!(is_macos_port_block(MACOS_PORT_MIN + 16, 16));
        assert!(!is_macos_port_block(MACOS_PORT_MIN + 16, 64));
        for size in [0, 1, 3, 15, 48, 65] {
            assert!(!is_port_block(0, size), "{size}");
        }
        assert!(is_port_block(0, 2));
        assert!(is_port_block(u16::MAX - 63, 64));
        assert!(!is_port_block(u16::MAX - 63, 128));
    }
}
