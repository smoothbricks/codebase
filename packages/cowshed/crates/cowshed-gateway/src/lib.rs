//! Actor-owned, host-only workspace egress gateway.
//!
//! [`Gateway`] is the owning daemon runtime. The controller installs complete
//! [`WorkspaceSession`] values through a cloneable [`GatewayHandle`]; replacing a
//! session rotates its token/CA generation and immediately cancels old sockets.
//! Secrets enter only through [`CredentialProvider`] and are injected after the
//! actor has authenticated the endpoint and token and admitted the exact target.

mod actor;
mod config;
mod control;
mod interfaces;
mod platform;
mod policy;
mod proxy;
mod telemetry;
mod tls;

pub use actor::{Gateway, GatewayError, GatewayHandle, GatewayStatus, SessionStatus};
pub use config::{
    ConfigError, GatewayConfig, GatewayLimits, GatewayTimeouts, MACOS_PORT_BLOCK_SIZE,
    MACOS_PORT_MAX, MACOS_PORT_MIN, TOKEN_BYTES, WorkspaceCa, WorkspaceEndpoint, WorkspaceSession,
    WorkspaceToken,
};
pub use control::{ControlError, ControlFailureCode, GatewayControlClient, control_socket_path};
pub use interfaces::{
    AuditError, AuditEvent, AuditKind, AuditSink, AuditStatus, AuthorizedTarget, BoxIo,
    ConnectError, CredentialError, CredentialProtocol, CredentialProvider, CredentialQuery,
    CredentialRecord, GatewayIo, SystemConnector, UpstreamConnector, UpstreamHealth,
    UpstreamPurpose,
};
pub use platform::KeychainCredentialProvider;
pub use policy::{
    CanonicalHost, CanonicalTarget, EgressGrant, EgressMode, HostPattern, MirrorProtocol,
    MirrorRoute, PolicyError, TargetScheme, WorkspacePolicy, normalize_path,
};
pub use telemetry::{ArrowAuditConfig, ArrowAuditSink};
