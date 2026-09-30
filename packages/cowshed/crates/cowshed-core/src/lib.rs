//! Warm, copy-on-write workspaces with explicit controller authority.

pub mod apfs;
pub mod api;
pub mod checkout;
pub mod copy;
mod device;
pub mod error;
pub mod exec;
mod fsio;
mod gateway_inventory;
pub mod gateway_sessions;
pub mod git;
pub mod host_caches;
mod inherited_daemons;
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

pub use error::{CowshedError, ErrorCode, Result};
pub use gateway_inventory::{
    AdoptedProject, GatewayInventoryError, GatewaySessionFact, NativeGatewayInventory,
    ProjectHealOutcome, SessionHealOutcome, UnreachableMain,
};
pub use storage::bootstrap::ValidatedHostStorage;
pub use storage::bootstrap::native::validate_existing_host_storage;
pub use workspace_credentials::GatewayWorkspaceCredentials;

pub use api::{
    Coordinator, CoordinatorToken, Cowshed, JobAttachment, JobHandle, JobStdin, JobStream, Project,
    RawByteStream, Session, WorkspaceHandle, WorkspaceRef,
};
