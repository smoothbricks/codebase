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
pub mod first_launch;
pub mod policy;
pub mod ram_disk;
pub mod repo_id;
pub mod session;
pub mod status;
pub mod task_server;

pub use cpu_budget::{
    CPU_POOL_ENV, CPU_TOKENS_ENV, CPU_TOKENS_OP, CpuBackgroundStake, CpuBudgetStatus,
    CpuCheckoutStatus, CpuGateJob, CpuGateWaiter, CpuHolder, CpuProbation, CpuTokensRequest,
    CpuWaiter, GateWork, HOLD_LIMIT, INSTALL_RESTART_CODE, INSTALL_SERVES_WITHIN, InstallRestart,
    JOBSERVER_OP, JobserverAnswer, JobserverClass, JobserverJob, JobserverRequest,
    PROBATION_OVER_OP, RESIDENT_POOL_OP, ResidentPoolRequest, background_cap, background_limit,
    background_share, cpu_pool_directory, host_tokens, inherited_cpu_pool,
};
pub use disk_lease::{
    DISK_LEASE_OP, DISK_PHASE_MEMBERS, DiskClass, DiskLeaseAnswer, DiskLeaseRequest, LeaseState,
    PhaseShare,
};
pub use first_launch::{FIRST_LAUNCH_OP, FirstLaunch, FirstLaunchProbe};
pub use policy::{
    CanonicalHost, CanonicalTarget, EgressGrant, EgressMode, HostPattern, MirrorProtocol,
    MirrorRoute, PolicyDenial, PolicyError, ResolvedMirrorRoute, TargetScheme, WorkspacePolicy,
    decode_percent, mirror_scope_matches, normalize_path, path_matches_prefix, raw_path_admissible,
};

pub use ram_disk::{
    InvalidRamDiskDevice, InvalidRamDiskEntry, RAM_DETACH_OP, RAM_DISK_SECTOR_BYTES, RAM_LEASE_OP,
    RamAttachRefusal, RamDetachRequest, RamDetachStatus, RamDetached, RamDiskAttached,
    RamDiskDevice, RamDiskEntry, RamDiskSectors, RamDisks, RamLeaseEnd, RamLeaseEndLine,
    RamLeaseRequest,
};
pub use repo_id::{InvalidRepoId, validate_repo_id};
pub use session::{
    ConfigError, MACOS_FIXTURE_PORT_MIN, MACOS_FIXTURE_PORTS, MACOS_PORT_MAX, MACOS_PORT_MIN,
    NEW_PORT_BLOCK_SIZE, TOKEN_BYTES, WorkspaceCa, WorkspaceEndpoint, WorkspaceSession,
    WorkspaceToken, is_macos_port_block, is_port_block, validate_identifier,
};
pub use status::{DaemonStatus, SessionStatus, StartupHeal, SupervisorRecovery};

pub use task_server::{
    LeaseClass, TASK_ADMIT_OP, TASK_QUEUE_OP, TASK_TERMINAL_OP, TaskAdmitRequest, TaskBand,
    TaskClass, TaskDepWait, TaskGrant, TaskLeaseStatus, TaskLeaseWait, TaskOutcome,
    TaskQueueStatus, TaskReadyStatus, TaskRunCaps, TaskRunningStatus, TaskTerminalRequest,
    TaskWaitingStatus,
};

use std::path::{Path, PathBuf};

/// Machine-global evidence roots shared by every user in the herd.
///
/// Bootstrap volumes cannot follow `$HOME`: doing so gives each user a different view of the
/// same machine-global herd and leaves the evidence layer outside the dedicated volumes.
pub const STORE_ROOT: &str = "/private/cowshed/store";

/// Production's gateway control socket, at production's store root: where production's disk tools
/// take their disk-lifecycle lease (`cowshed_apfs::disk_lease::lease_socket`; 05_gateway.md,
/// "Disk-lifecycle lease"). A test namespace leases from its own root's gateway, and every other
/// client derives its store's socket from the store it was given (`CanonicalRoots::control_socket`
/// in cowshed-core), so no test reaches production's gateway.
///
/// Root-level keeps `sun_path` short (01_storage.md): a Unix socket address is capped near 104
/// bytes. Defined here, below the controller, the disk-image layer and the daemon, so each of them
/// names the one definition and no constant is copied across a crate boundary.
pub fn production_control_socket() -> PathBuf {
    Path::new(STORE_ROOT).join("gateway.sock")
}

/// How long a gateway told to stop (SIGTERM) lets the work it already accepted finish before it
/// force-stops what is left: the drain deadline of every restart an install makes and of
/// `gateway stop` (05_gateway.md "Install order and rollback").
pub const GATEWAY_DRAIN_WITHIN: std::time::Duration = std::time::Duration::from_secs(20);

/// The gateway agent's launchd `ExitTimeOut`: how long launchd waits after the SIGTERM of a
/// bootout or `kickstart -k` before it SIGKILLs the daemon. The drain ([`GATEWAY_DRAIN_WITHIN`])
/// plus the force stop, the Nx backends' exit and the audit flush after it fit inside it.
/// launchd's own default is 5 s (measured 2026-10-22, `launchctl print`: "exit timeout = 5"),
/// which SIGKILLed a gateway mid-drain.
pub const GATEWAY_EXIT_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

// launchd's SIGKILL must never come before the drain's own deadline.
const _: () = assert!(GATEWAY_DRAIN_WITHIN.as_secs() < GATEWAY_EXIT_TIMEOUT.as_secs());
