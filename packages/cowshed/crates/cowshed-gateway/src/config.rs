use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    num::NonZeroUsize,
    path::PathBuf,
    time::Duration,
};

use cowshed_gateway_types::{ConfigError, WorkspaceSession};
// The workspace data socket only exists on Linux; macOS sessions are TCP port blocks.
#[cfg(target_os = "linux")]
use cowshed_gateway_types::{WorkspaceEndpoint, validate_identifier};

use crate::cache::{
    CacheConfig, DEFAULT_FILL_WAIT_TIMEOUT, DEFAULT_HIGH_WATER_BYTES, DEFAULT_LOW_WATER_BYTES,
    DEFAULT_METADATA_TTL,
};

pub const CONTROL_TCP_ADDR: SocketAddr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7_644);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlTcpConfig {
    pub address: SocketAddr,
    pub credential_file: PathBuf,
}

impl ControlTcpConfig {
    pub fn new(credential_file: PathBuf) -> Self {
        Self {
            address: CONTROL_TCP_ADDR,
            credential_file,
        }
    }

    fn validate(
        &self,
        host_root: Option<&std::path::Path>,
        authorized_uid: u32,
    ) -> Result<(), ConfigError> {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        if self.address != CONTROL_TCP_ADDR {
            return Err(ConfigError::InvalidControlTcpAddress);
        }
        let root = host_root.ok_or(ConfigError::MissingProductionControlSocket)?;
        if !self.credential_file.is_absolute() || self.credential_file.parent() != Some(root) {
            return Err(ConfigError::InvalidControlCredentialFile);
        }
        let metadata = std::fs::symlink_metadata(&self.credential_file)
            .map_err(|_| ConfigError::InvalidControlCredentialFile)?;
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.uid() != authorized_uid
            || metadata.permissions().mode() & 0o777 != 0o600
        {
            return Err(ConfigError::InvalidControlCredentialFile);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug)]
pub struct GatewayLimits {
    pub max_sessions: usize,
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
            max_sessions: 1024,
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
            self.max_sessions,
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
        if self.max_sessions > 4096
            || self.workspace_active > self.global_active
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MirrorCacheConfig {
    pub cache_root: PathBuf,
    pub high_water_bytes: u64,
    pub low_water_bytes: u64,
    pub metadata_ttl: Duration,
    pub fill_wait_timeout: Duration,
}

impl MirrorCacheConfig {
    pub fn new(cache_root: PathBuf) -> Self {
        Self {
            cache_root,
            high_water_bytes: DEFAULT_HIGH_WATER_BYTES,
            low_water_bytes: DEFAULT_LOW_WATER_BYTES,
            metadata_ttl: DEFAULT_METADATA_TTL,
            fill_wait_timeout: DEFAULT_FILL_WAIT_TIMEOUT,
        }
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if !self.cache_root.is_absolute() {
            return Err(ConfigError::MissingMirrorCacheRoot);
        }
        if self.low_water_bytes >= self.high_water_bytes
            || self.metadata_ttl.is_zero()
            || self.fill_wait_timeout.is_zero()
        {
            return Err(ConfigError::InvalidMirrorCacheLimits);
        }
        let metadata = std::fs::symlink_metadata(&self.cache_root)
            .map_err(|_| ConfigError::InsecureMirrorCacheRoot)?;
        if metadata.file_type().is_symlink() || !metadata.is_dir() {
            return Err(ConfigError::InsecureMirrorCacheRoot);
        }
        Ok(())
    }

    pub(crate) fn cache_config(&self) -> CacheConfig {
        CacheConfig {
            root: self.cache_root.clone(),
            high_water_bytes: self.high_water_bytes,
            low_water_bytes: self.low_water_bytes,
            metadata_ttl: self.metadata_ttl,
            fill_wait_timeout: self.fill_wait_timeout,
        }
    }
}

impl Default for MirrorCacheConfig {
    fn default() -> Self {
        Self::new(PathBuf::new())
    }
}

/// Host daemon configuration. Runtime protocol limits are deliberately not configurable.
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    pub control_socket: Option<PathBuf>,
    pub control_tcp: Option<ControlTcpConfig>,
    /// Controller-owned, one-way simulator artifact drop directory.
    pub simulator_drop_root: Option<PathBuf>,
    /// Exact trusted executable used for the isolated Git fetch helper.
    pub git_helper_executable: Option<PathBuf>,
    /// Authoritative private directory for Linux workspace data sockets.
    pub data_socket_root: Option<PathBuf>,
    /// Canonical root of the dedicated host cache volume in production.
    pub production_cache_volume: Option<PathBuf>,
    pub authorized_control_uid: u32,
    pub limits: GatewayLimits,
    pub timeouts: GatewayTimeouts,
    pub command_capacity: NonZeroUsize,
    pub mirror_cache: MirrorCacheConfig,
}

impl Default for GatewayConfig {
    fn default() -> Self {
        Self {
            control_socket: None,
            control_tcp: None,
            simulator_drop_root: None,
            git_helper_executable: None,
            data_socket_root: None,
            production_cache_volume: None,
            authorized_control_uid: unsafe { libc::geteuid() },
            limits: GatewayLimits::default(),
            timeouts: GatewayTimeouts::default(),
            command_capacity: NonZeroUsize::new(1024).expect("1024 is non-zero"),
            mirror_cache: MirrorCacheConfig::default(),
        }
    }
}

impl GatewayConfig {
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.limits.validate()?;
        self.timeouts.validate()?;
        self.mirror_cache.validate()?;
        if let Some(path) = &self.control_socket
            && !path.is_absolute()
        {
            return Err(ConfigError::RelativeSocketPath);
        }
        if let Some(tcp) = &self.control_tcp {
            tcp.validate(
                self.control_socket
                    .as_deref()
                    .and_then(std::path::Path::parent),
                self.authorized_control_uid,
            )?;
        }
        #[cfg(target_os = "linux")]
        {
            let root = self
                .data_socket_root
                .as_deref()
                .ok_or(ConfigError::MissingDataSocketRoot)?;
            Self::validate_data_socket_root(root)?;
        }
        Ok(())
    }

    pub fn validate_host_cache_layout(&self) -> Result<(), ConfigError> {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        let control = self
            .control_socket
            .as_deref()
            .ok_or(ConfigError::MissingProductionControlSocket)?;
        if control.file_name().and_then(|name| name.to_str()) != Some("gateway.sock") {
            return Err(ConfigError::InvalidProductionCacheRoot);
        }
        let cache_volume = self
            .production_cache_volume
            .as_deref()
            .ok_or(ConfigError::InvalidProductionCacheRoot)?;
        let expected = cache_volume.join("mirror");
        let canonical_expected = std::fs::canonicalize(&expected)
            .map_err(|_| ConfigError::InvalidProductionCacheRoot)?;
        let canonical_configured = std::fs::canonicalize(&self.mirror_cache.cache_root)
            .map_err(|_| ConfigError::InvalidProductionCacheRoot)?;
        if canonical_expected != canonical_configured || canonical_configured != expected {
            return Err(ConfigError::InvalidProductionCacheRoot);
        }
        let metadata = std::fs::symlink_metadata(&canonical_configured)
            .map_err(|_| ConfigError::InvalidProductionCacheRoot)?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || metadata.uid() != self.authorized_control_uid
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(ConfigError::InvalidProductionCacheRoot);
        }
        let helper = self
            .git_helper_executable
            .as_deref()
            .ok_or(ConfigError::MissingGitHelperExecutable)?;
        self.validate_git_helper_executable(helper)?;
        Ok(())
    }

    pub(crate) fn validate_session_endpoint(
        &self,
        session: &WorkspaceSession,
    ) -> Result<(), ConfigError> {
        #[cfg(target_os = "linux")]
        {
            let root = self
                .data_socket_root
                .as_deref()
                .ok_or(ConfigError::MissingDataSocketRoot)?;
            let WorkspaceEndpoint::Unix(path) = &session.endpoint else {
                return Err(ConfigError::ExpectedUnixEndpoint);
            };
            if path.parent() != Some(root)
                || self
                    .control_socket
                    .as_ref()
                    .is_some_and(|control| control == path)
            {
                return Err(ConfigError::EndpointOutsideDataSocketRoot);
            }
            let stem = path
                .file_stem()
                .and_then(|stem| stem.to_str())
                .ok_or(ConfigError::InvalidDataSocketName)?;
            if path.extension().and_then(|extension| extension.to_str()) != Some("sock")
                || validate_identifier("workspace socket", stem).is_err()
            {
                return Err(ConfigError::InvalidDataSocketName);
            }
        }
        #[cfg(not(target_os = "linux"))]
        let _ = session;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn validate_data_socket_root(root: &std::path::Path) -> Result<(), ConfigError> {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        if !root.is_absolute() {
            return Err(ConfigError::RelativeSocketPath);
        }
        let metadata =
            std::fs::symlink_metadata(root).map_err(|_| ConfigError::InsecureDataSocketRoot)?;
        if !metadata.is_dir()
            || metadata.file_type().is_symlink()
            || metadata.uid() != unsafe { libc::geteuid() }
            || metadata.permissions().mode() & 0o077 != 0
        {
            return Err(ConfigError::InsecureDataSocketRoot);
        }
        Ok(())
    }
    fn validate_git_helper_executable(&self, path: &std::path::Path) -> Result<(), ConfigError> {
        use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};

        if !path.is_absolute() {
            return Err(ConfigError::InvalidGitHelperExecutable);
        }
        let metadata =
            std::fs::symlink_metadata(path).map_err(|_| ConfigError::InvalidGitHelperExecutable)?;
        let mode = metadata.permissions().mode();
        if !metadata.is_file()
            || metadata.file_type().is_symlink()
            || metadata.uid() != self.authorized_control_uid
            || mode & 0o022 != 0
            || mode & 0o100 == 0
        {
            return Err(ConfigError::InvalidGitHelperExecutable);
        }
        Ok(())
    }
}
