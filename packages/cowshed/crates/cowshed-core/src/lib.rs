//! Warm, copy-on-write workspaces with explicit controller authority.

pub mod api {
    pub mod capability;
    pub mod dto;

    pub use capability::{
        Coordinator, CoordinatorToken, Cowshed, JobAttachment, JobHandle, JobStdin, JobStream,
        Project, RawByteStream, Session, WorkspaceHandle, WorkspaceRef,
    };
    pub use dto::*;
}
pub mod apfs;
pub mod copy;
pub mod error;
pub mod exec;
pub mod git;
pub mod metadata;
pub mod repository;
pub mod sandbox;
pub mod secrets;
pub mod storage;

pub use error::{CowshedError, ErrorCode, Result};

pub use api::{
    Coordinator, CoordinatorToken, Cowshed, JobAttachment, JobHandle, JobStdin, JobStream, Project,
    RawByteStream, Session, WorkspaceHandle, WorkspaceRef,
};
