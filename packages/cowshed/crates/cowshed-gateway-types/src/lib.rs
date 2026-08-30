//! The gateway's control-plane data model, with no daemon attached.
//!
//! A workspace's gateway session — its identity, endpoint, bearer token, CA material, and egress
//! policy — is derived by the host controller in `cowshed-core` and installed into the daemon in
//! `cowshed-gateway`. Both sides need the same shapes and the same validation, and neither needs
//! the other's machinery: the controller has no use for hyper, rustls or a TLS-terminating proxy,
//! and reconciliation is a pure function of these values.
//!
//! So the shapes live here, below both, and this crate stays a leaf: `base64`, `http` (headers and
//! methods, not hyper), `idna`, `serde`, `subtle`, `thiserror`, `url`, `zeroize`. Nothing async,
//! nothing that opens a socket. Adding a runtime dependency here re-links the daemon into every
//! controller and defeats the split.
//!
//! [`ConfigError`] covers the whole gateway configuration grammar, including the daemon-only
//! listener, limit and cache-root variants: one enum, so a value the controller can construct is
//! exactly a value the daemon accepts, and `cowshed-gateway`'s own validators report through it.

pub mod policy;
pub mod repo_id;
pub mod session;
pub mod status;

pub use policy::{
    CanonicalHost, CanonicalTarget, EgressGrant, EgressMode, HostPattern, MirrorProtocol,
    MirrorRoute, PolicyDenial, PolicyError, ResolvedMirrorRoute, TargetScheme, WorkspacePolicy,
    decode_percent, mirror_scope_matches, normalize_path,
};
pub use repo_id::{InvalidRepoId, validate_repo_id};
pub use session::{
    ConfigError, MACOS_PORT_BLOCK_SIZE, MACOS_PORT_MAX, MACOS_PORT_MIN, TOKEN_BYTES, WorkspaceCa,
    WorkspaceEndpoint, WorkspaceSession, WorkspaceToken, validate_identifier,
};
pub use status::{GatewayStatus, SessionStatus};
