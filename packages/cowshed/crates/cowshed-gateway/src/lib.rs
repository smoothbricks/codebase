//! Actor-owned, host-only workspace egress gateway.
//!
//! [`Gateway`] is the owning daemon runtime. The controller installs complete
//! [`WorkspaceSession`] values through a cloneable [`GatewayHandle`]; replacing a
//! session rotates its token/CA generation and immediately cancels old sockets.
//! Secrets enter only through [`CredentialProvider`] and are injected after the
//! actor has authenticated the endpoint and token and admitted the exact target.

mod actor;
mod cache;
mod config;
mod control;
mod interfaces;
mod mirror;
mod platform;
mod policy;
mod proxy;
mod telemetry;
mod tls;

pub use actor::{Gateway, GatewayError, GatewayHandle, GatewayStatus, SessionStatus};
pub use cache::{
    Cache, CacheBodyError, CacheConfig, CacheError, CacheKey, CacheNamespace, CachedResponse,
    DEFAULT_HIGH_WATER_BYTES, DEFAULT_LOW_WATER_BYTES, ObjectDigest, ObjectExpectation,
};
pub use config::{
    ConfigError, GatewayConfig, GatewayLimits, GatewayTimeouts, MACOS_PORT_BLOCK_SIZE,
    MACOS_PORT_MAX, MACOS_PORT_MIN, MirrorCacheConfig, TOKEN_BYTES, WorkspaceCa, WorkspaceEndpoint,
    WorkspaceSession, WorkspaceToken,
};
pub use control::{ControlError, ControlFailureCode, GatewayControlClient, control_socket_path};
pub use interfaces::{
    AuditError, AuditEvent, AuditKind, AuditSink, AuditStatus, AuthorizedTarget, BoxIo,
    ConnectError, CredentialError, CredentialProtocol, CredentialProvider, CredentialQuery,
    CredentialRecord, GatewayIo, NegotiatedTransport, SystemConnector, UpstreamConnection,
    UpstreamConnector, UpstreamHealth, UpstreamPurpose,
};
pub use mirror::{
    MirrorBody, MirrorCacheScope, MirrorCacheStatus, MirrorError, MirrorFetchRequest,
    MirrorOutcome, MirrorProtocolMetadata, MirrorRedirect, MirrorRequest, MirrorResourceKind,
    MirrorResponse, MirrorService, MirrorUpstream,
};
pub use platform::KeychainCredentialProvider;
pub use policy::{
    CanonicalHost, CanonicalTarget, EgressGrant, EgressMode, HostPattern, MirrorProtocol,
    MirrorRoute, PolicyError, ResolvedMirrorRoute, TargetScheme, WorkspacePolicy, normalize_path,
};
pub use telemetry::{ArrowAuditConfig, ArrowAuditSink};
