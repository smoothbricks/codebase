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
mod inherited_daemons;
pub mod inherited_links;
pub mod landing;
pub mod metadata;
mod process;
pub mod repository;
pub mod runtime;
pub mod sandbox;
pub mod secrets;
pub mod storage;
pub mod workspace_credentials;
pub mod workspace_environment;

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
