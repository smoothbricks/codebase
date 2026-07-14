//! Warm, copy-on-write workspaces with explicit controller authority.

pub mod apfs;
pub mod api;
pub mod copy;
pub mod error;
pub mod exec;
pub mod git;
pub mod metadata;
pub mod repository;
pub mod runtime;
pub mod sandbox;
pub mod secrets;
pub mod storage;
pub mod workspace_credentials;

pub use error::{CowshedError, ErrorCode, Result};
pub use storage::bootstrap::ValidatedHostStorage;
pub use storage::bootstrap::native::validate_existing_host_storage;

pub use api::{
    Coordinator, CoordinatorToken, Cowshed, JobAttachment, JobHandle, JobStdin, JobStream, Project,
    RawByteStream, Session, WorkspaceHandle, WorkspaceRef,
};
