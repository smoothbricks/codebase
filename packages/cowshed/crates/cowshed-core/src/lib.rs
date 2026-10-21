//! Warm, copy-on-write workspaces with explicit controller authority.

pub mod api;
pub mod build_volume;
pub mod caches_retirement;
pub mod capabilities;
pub mod checkout;
pub mod copy;
pub mod developer_links;
pub mod doctor;
pub mod error;
pub mod exec;
mod gateway_inventory;
pub mod gateway_sessions;
pub mod git;
mod git_preservation;
mod held_change;
pub mod host_dirs;
pub mod host_load;
mod inherited_daemons;
#[cfg(target_os = "macos")]
mod inherited_git_locks;
pub mod inherited_links;
pub mod land_turn;
pub mod landing;
pub mod metadata;
pub mod project_policy;
pub mod project_retirement;
pub mod repository;
pub mod resident;
pub mod runtime;
pub mod sandbox;
pub mod script;
pub mod secrets;
pub mod storage;
pub mod vnodes;
pub mod workspace_clients;
pub mod workspace_credentials;
pub mod workspace_environment;
pub mod workspace_git_fetch;

// Every real-image fixture in the lib test binary shares this one scratch-root protocol: one
// sweep, one sequence, whichever module attaches the image; and one blank image template.
#[cfg(all(test, target_os = "macos"))]
use cowshed_apfs::image::ImageCapacity;
#[cfg(all(test, target_os = "macos"))]
use cowshed_apfs::{
    ApfsBackend, CommandRunner, DetachIntent, DiskImageSource, ImageCapture, MacOsApfsBackend,
    SystemCommandRunner, attach_budget,
};
#[cfg(all(test, target_os = "macos"))]
use cowshed_apfs::{store_lock::verify_no_symlinks, template::blank_template};
#[cfg(all(test, target_os = "macos"))]
#[path = "../../cowshed-apfs/tests/support/blank_image.rs"]
mod blank_image;
#[cfg(all(test, target_os = "macos"))]
#[path = "../../cowshed-apfs/tests/support/scratch_apfs.rs"]
mod scratch_apfs;
#[cfg(all(test, target_os = "macos"))]
#[expect(
    clippy::duplicate_mod,
    reason = "scratch_directory and scratch_apfs each include the stateless scratch_processes \
              helpers, so either support file also stands alone in a test binary"
)]
#[path = "../tests/support/scratch_directory.rs"]
mod scratch_directory;
#[cfg(test)]
#[path = "../tests/support/temp_parent.rs"]
mod temp_parent;
#[cfg(test)]
#[path = "../tests/support/temp_root.rs"]
mod temp_root;

pub use error::{
    AdmissionRefusal, CowshedError, ErrorCode, FenceRefusal, MAX_FENCE_PATHS, OtherBuild, Result,
    Retry, StdinRefusal,
};
pub use gateway_inventory::{
    AdoptedProject, GatewayInventoryError, GatewaySessionFact, GatewayStoreBackend,
    NativeGatewayInventory, ProjectHealOutcome, SessionHealOutcome, StartupHealState,
    UnreachableMain, forming_re_adoption_checkout,
};
pub use project_retirement::ProjectRetirementReport;
pub use storage::bootstrap::ValidatedHostStorage;
pub use storage::bootstrap::native::validate_existing_host_storage;
pub use workspace_credentials::GatewayWorkspaceCredentials;

pub use api::{
    Adopted, Coordinator, CoordinatorToken, Cowshed, EventStream, JobAttachment, JobHandle,
    JobStdin, JobStream, Project, RawByteStream, Session, WorkspaceHandle, WorkspaceRef,
};
