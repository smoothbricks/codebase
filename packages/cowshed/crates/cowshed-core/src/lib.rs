//! Warm, copy-on-write workspaces with explicit controller authority.

pub mod apfs;
pub mod api;
pub mod checkout;
pub mod copy;
pub mod error;
pub mod exec;
mod gateway_inventory;
pub mod gateway_sessions;
pub mod git;
pub mod metadata;
mod process;
pub mod repository;
pub mod runtime;
pub mod sandbox;
pub mod secrets;
pub mod storage;
pub mod workspace_credentials;

pub use error::{CowshedError, ErrorCode, Result};
pub use gateway_inventory::{
    AdoptedProject, GatewayInventoryError, GatewaySessionFact, NativeGatewayInventory,
};
pub use storage::bootstrap::ValidatedHostStorage;
pub use storage::bootstrap::native::validate_existing_host_storage;
pub use workspace_credentials::GatewayWorkspaceCredentials;

pub use api::{
    Coordinator, CoordinatorToken, Cowshed, JobAttachment, JobHandle, JobStdin, JobStream, Project,
    RawByteStream, Session, WorkspaceHandle, WorkspaceRef,
};
