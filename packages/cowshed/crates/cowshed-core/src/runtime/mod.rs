#[cfg(target_os = "macos")]
mod macos;

pub mod project;
pub mod supervisor;

pub use project::{
    ProjectDescriptor, ProjectRuntime, ProjectRuntimeHost, RuntimeJobStream, RuntimeLogChunk,
    WorkspaceSnapshot,
};
pub use supervisor::{
    CheckpointBarrier, CommitmentDraft, CommitmentPublisher, CommitmentPublisherHandle, LogChunk,
    SessionSnapshot, SessionToken, WorkspaceAuthoritySnapshot, WorkspaceSupervisor,
    WorkspaceSupervisorConfig, WorkspaceSupervisorHandle,
};
