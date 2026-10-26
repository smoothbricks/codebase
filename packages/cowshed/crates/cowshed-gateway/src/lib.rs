//! Actor-owned, host-only workspace egress gateway.
//!
//! [`Gateway`] is the owning daemon runtime. The controller installs complete
//! [`WorkspaceSession`] values through a cloneable [`GatewayHandle`]; replacing a
//! session rotates its token/CA generation and immediately cancels old sockets.
//! Secrets enter only through [`CredentialProvider`] and are injected after the
//! actor has authenticated the endpoint and token and admitted the exact target.

mod actor;
mod admission;
mod cache;
mod cleanup;
mod config;
mod control;
mod disk_lease;
mod interfaces;
#[cfg(target_os = "macos")]
mod listener_handoff;
mod mirror;
mod npm_index;
mod nx_cache;
mod nx_lifecycle;
mod platform;
mod proxy;
mod ram_lease;
mod repo_mirror;
mod sim_broker;
mod task_server;
mod telemetry;
mod tls;

pub use actor::{Gateway, GatewayError, GatewayHandle};
pub use cache::{
    Cache, CacheBodyError, CacheConfig, CacheError, CacheKey, CacheNamespace, CachedResponse,
    DEFAULT_HIGH_WATER_BYTES, DEFAULT_LOW_WATER_BYTES, ObjectDigest, ObjectExpectation,
};
pub use config::{
    CONTROL_TCP_ADDR, ControlTcpConfig, GatewayConfig, GatewayLimits, GatewayTimeouts,
    MirrorCacheConfig, StartupProbe,
};
pub use control::{ControlError, ControlFailureCode, GatewayControlClient};
/// The control-plane data model is re-exported at this root so `cowshed_gateway::WorkspaceSession`
/// and friends name the same items whether a caller links the daemon or only the types crate.
pub use cowshed_gateway_types::{
    CanonicalHost, CanonicalTarget, ConfigError, DaemonStatus, EgressGrant, EgressMode,
    HostPattern, InvalidRepoId, MACOS_PORT_MAX, MACOS_PORT_MIN, MirrorProtocol, MirrorRoute,
    NEW_PORT_BLOCK_SIZE, PolicyError, ResolvedMirrorRoute, SessionStatus, StartupHeal,
    SupervisorRecovery, TOKEN_BYTES, TargetScheme, WorkspaceCa, WorkspaceEndpoint, WorkspacePolicy,
    WorkspaceSession, WorkspaceToken, is_macos_port_block, normalize_path, validate_repo_id,
};
pub use disk_lease::DiskLeaseLimits;
pub use interfaces::{
    AuditError, AuditEvent, AuditKind, AuditSink, AuditStatus, AuthorizedTarget, BoxIo,
    ConnectError, CredentialError, CredentialProtocol, CredentialProvider, CredentialQuery,
    CredentialRecord, GatewayIo, NegotiatedTransport, SystemConnector, UpstreamConnection,
    UpstreamConnector, UpstreamHealth, UpstreamPurpose,
};
#[cfg(target_os = "macos")]
pub use listener_handoff::{HandoffServer, handoff_path};
pub use mirror::{
    MirrorBody, MirrorCacheScope, MirrorCacheStatus, MirrorError, MirrorFetchRequest,
    MirrorOutcome, MirrorProtocolMetadata, MirrorRedirect, MirrorRequest, MirrorResourceKind,
    MirrorResponse, MirrorService, MirrorUpstream,
};
pub use nx_cache::{NxCacheBackend, NxCacheServiceConfig, bun_on_path};
#[cfg(target_os = "macos")]
pub use platform::KeychainCredentialProvider;
#[cfg(target_os = "linux")]
pub use platform::SystemdCredentialProvider;
pub use platform::{
    CredentialPresence, ScopedCredential, remove_scoped_credential, scoped_credential_scope,
    store_scoped_credential, validate_scope,
};
pub use repo_mirror::{
    GATEWAY_GIT_FETCH_HELPER_ARG, GitFetchHelperError, MirrorInfo, RepoFetchOutcome, RepoFetchPlan,
    RepoMirrorError, RepoMirrorRequest, RepoTransport, run_gateway_git_fetch_helper,
};
pub use sim_broker::{
    SimBrokerError, SimCommand, SimCommandOutput, SimDevice, SimGrant, SimInstallApproval,
    SimProjectConfig, SimRequest, SimResult, SimRunner,
};
pub use telemetry::{ArrowAuditConfig, ArrowAuditSink};

pub use task_server::{LeasePlaces, TaskServerLimits};
