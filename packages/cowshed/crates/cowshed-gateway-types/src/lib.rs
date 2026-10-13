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

pub mod cpu_budget;
pub mod disk_lease;
pub mod policy;
pub mod repo_id;
pub mod session;
pub mod status;

pub use cpu_budget::{CPU_TOKENS_OP, CpuBudgetStatus, CpuCheckoutStatus, CpuTokensRequest};
pub use disk_lease::{DISK_LEASE_OP, DiskClass, DiskLeaseAnswer, DiskLeaseRequest, LeaseState};
pub use policy::{
    CanonicalHost, CanonicalTarget, EgressGrant, EgressMode, HostPattern, MirrorProtocol,
    MirrorRoute, PolicyDenial, PolicyError, ResolvedMirrorRoute, TargetScheme, WorkspacePolicy,
    decode_percent, mirror_scope_matches, normalize_path, path_matches_prefix, raw_path_admissible,
};
pub use repo_id::{InvalidRepoId, validate_repo_id};
pub use session::{
    ConfigError, MACOS_PORT_MAX, MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE, TOKEN_BYTES, WorkspaceCa,
    WorkspaceEndpoint, WorkspaceSession, WorkspaceToken, is_macos_port_block, is_port_block,
    validate_identifier,
};
pub use status::{DaemonStatus, SessionStatus, StartupHeal, SupervisorRecovery};

use std::path::{Path, PathBuf};

/// Machine-global evidence roots shared by every user in the herd.
///
/// Bootstrap volumes cannot follow `$HOME`: doing so gives each user a different view of the
/// same machine-global herd and leaves the evidence layer outside the dedicated volumes.
pub const STORE_ROOT: &str = "/private/cowshed/store";

/// The gateway control socket, at the store root.
///
/// Root-level keeps `sun_path` short (01_storage.md): a Unix socket address is capped near 104
/// bytes, and a home-relative path spent that budget on the user's name before the gateway had
/// said anything. The store is also the only location every peer agrees on — a LaunchAgent, the
/// CLI, and a supervising runtime each know the store, while `HOME` differs between them.
///
/// Defined here, below the controller, the disk-image layer and the daemon, so each of them
/// names the one definition and no constant is copied across a crate boundary.
pub fn control_socket_path() -> PathBuf {
    Path::new(STORE_ROOT).join("gateway.sock")
}
