//! Warm, copy-on-write workspaces with explicit controller authority.

pub mod apfs;
pub mod api;
pub mod build_volume;
pub mod caches_retirement;
pub mod capabilities;
pub mod checkout;
pub mod copy;
mod device;
pub mod disk_image_helpers;
pub mod error;
pub mod exec;
pub mod fork_lock;
mod fsio;
mod gateway_inventory;
pub mod gateway_sessions;
pub mod git;
pub mod host_dirs;
pub mod host_load;
mod inherited_daemons;
#[cfg(target_os = "macos")]
mod inherited_git_locks;
pub mod inherited_links;
pub mod landing;
pub mod metadata;
mod process;
pub mod project_policy;
pub mod repository;
pub mod resident;
pub mod runtime;
pub mod sandbox;
pub mod script;
pub mod secrets;
pub mod storage;
pub mod timing;
pub mod vnodes;
pub mod workspace_clients;
pub mod workspace_credentials;
pub mod workspace_environment;
pub mod workspace_git_fetch;

// Every real-image fixture in the lib test binary shares this one scratch-root protocol: one
// sweep, one sequence, whichever module attaches the image; and one blank image template.
#[cfg(all(test, target_os = "macos"))]
use crate::apfs::{
    ApfsBackend, CommandRunner, DetachIntent, DiskImageSource, MacOsApfsBackend,
    SystemCommandRunner,
};
#[cfg(all(test, target_os = "macos"))]
use crate::metadata::ImageCapacity;
#[cfg(all(test, target_os = "macos"))]
use crate::storage::apfs::{
    ApfsSubstrateConfig,
    native::{MacOsApfsExecutionHost, blank_template},
};
#[cfg(all(test, target_os = "macos"))]
#[path = "../tests/support/blank_image.rs"]
mod blank_image;
#[cfg(all(test, target_os = "macos"))]
#[path = "../tests/support/scratch_apfs.rs"]
mod scratch_apfs;

pub use error::{CowshedError, ErrorCode, FenceRefusal, MAX_FENCE_PATHS, OtherBuild, Result};
pub use gateway_inventory::{
    AdoptedProject, GatewayInventoryError, GatewaySessionFact, NativeGatewayInventory,
    ProjectHealOutcome, SessionHealOutcome, StartupHealState, UnreachableMain,
};
pub use storage::bootstrap::ValidatedHostStorage;
pub use storage::bootstrap::native::validate_existing_host_storage;
pub use workspace_credentials::GatewayWorkspaceCredentials;

pub use api::{
    Coordinator, CoordinatorToken, Cowshed, JobAttachment, JobHandle, JobStdin, JobStream, Project,
    RawByteStream, Session, WorkspaceHandle, WorkspaceRef,
};
