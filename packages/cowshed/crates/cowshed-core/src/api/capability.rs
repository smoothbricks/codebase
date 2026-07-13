#![allow(
    dead_code,
    reason = "sealed constructors and actor endpoints are reserved for crate-internal transports"
)]

use super::dto::{GrantSet, WorkspaceInfo};
use crate::metadata::WorkspaceName;
use crate::repository::{ProjectPaths, RepoId, RepositoryBinding};
use std::fmt;
use std::path::{Path, PathBuf};

/// Stateless namespace for cowshed entry points.
#[derive(Clone, Copy, Debug, Default)]
pub struct Cowshed;

/// Discovery-only project identity. It contains no controller or worker authority.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Project {
    repo_id: RepoId,
    binding: RepositoryBinding,
    git_root: PathBuf,
    paths: ProjectPaths,
}

impl Project {
    pub(crate) fn resolved(
        repo_id: RepoId,
        binding: RepositoryBinding,
        git_root: PathBuf,
        paths: ProjectPaths,
    ) -> Self {
        Self {
            repo_id,
            binding,
            git_root,
            paths,
        }
    }

    pub fn repo_id(&self) -> &RepoId {
        &self.repo_id
    }

    pub fn binding(&self) -> &RepositoryBinding {
        &self.binding
    }

    pub fn git_root(&self) -> &Path {
        &self.git_root
    }

    pub fn paths(&self) -> &ProjectPaths {
        &self.paths
    }
}

/// Read-only identity and detached snapshot for exactly one workspace.
///
/// It intentionally has no execution, detach, grant mutation, or lifecycle methods.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceRef {
    info: WorkspaceInfo,
    grants: GrantSet,
}

impl WorkspaceRef {
    pub(crate) fn discovered(info: WorkspaceInfo, grants: GrantSet) -> Self {
        Self { info, grants }
    }

    pub fn name(&self) -> &WorkspaceName {
        &self.info.workspace
    }

    pub fn mount_path(&self) -> &Path {
        &self.info.mount
    }

    pub fn info(&self) -> &WorkspaceInfo {
        &self.info
    }

    pub fn grants(&self) -> &GrantSet {
        &self.grants
    }
}

/// Affine proof returned only after the inherited controller descriptor's peer and one-use nonce handshake succeed.
///
/// There is deliberately no public constructor, `Clone`, `Copy`, `Default`, serde implementation, or byte/string
/// projection. The descriptor is consumed when the token is bound to a [`Coordinator`].
///
/// ```compile_fail
/// use cowshed_core::api::CoordinatorToken;
/// let _forged = CoordinatorToken::default();
/// ```
pub struct CoordinatorToken {
    repo_id: RepoId,
    channel: AuthenticatedControllerChannel,
}

impl CoordinatorToken {
    pub(crate) fn authenticated(repo_id: RepoId, channel: AuthenticatedControllerChannel) -> Self {
        Self { repo_id, channel }
    }
}

impl fmt::Debug for CoordinatorToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("CoordinatorToken")
            .field("repo_id", &self.repo_id)
            .field("channel", &"<authenticated controller channel>")
            .finish()
    }
}

/// An authenticated, single-owner actor channel. Construction is restricted to the controller transport handshake.
pub(crate) struct AuthenticatedControllerChannel {
    #[cfg(unix)]
    descriptor: std::os::fd::OwnedFd,
    #[cfg(not(unix))]
    private: (),
}

impl AuthenticatedControllerChannel {
    #[cfg(unix)]
    pub(crate) fn from_verified_descriptor(descriptor: std::os::fd::OwnedFd) -> Self {
        Self { descriptor }
    }
}

impl fmt::Debug for AuthenticatedControllerChannel {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("AuthenticatedControllerChannel(<redacted>)")
    }
}

/// Sole project mutation and cross-workspace authority.
///
/// The owned channel is affine. All mutable controller state remains in the remote single-owner actor.
pub struct Coordinator {
    project: Project,
    channel: AuthenticatedControllerChannel,
}

impl Coordinator {
    pub(crate) fn bind(
        project: Project,
        token: CoordinatorToken,
    ) -> Result<Self, CoordinatorToken> {
        if project.repo_id == token.repo_id {
            Ok(Self {
                project,
                channel: token.channel,
            })
        } else {
            Err(token)
        }
    }

    pub fn project(&self) -> &Project {
        &self.project
    }
}

impl fmt::Debug for Coordinator {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Coordinator")
            .field("project", &self.project)
            .field("channel", &self.channel)
            .finish()
    }
}

/// Non-escalating capability for exactly one workspace.
///
/// The handle carries the supervisor actor endpoint but no controller channel, so it cannot mutate grants, select a
/// sibling, detach, restore, destroy, land, rebase, or collect garbage.
///
/// ```compile_fail
/// fn cannot_escalate(handle: &cowshed_core::api::WorkspaceHandle) {
///     handle.gc();
/// }
/// ```
pub struct WorkspaceHandle {
    workspace: WorkspaceRef,
    supervisor_endpoint: PathBuf,
}

impl WorkspaceHandle {
    pub(crate) fn scoped(workspace: WorkspaceRef, supervisor_endpoint: PathBuf) -> Self {
        Self {
            workspace,
            supervisor_endpoint,
        }
    }

    pub fn name(&self) -> &WorkspaceName {
        self.workspace.name()
    }

    pub fn mount_path(&self) -> &Path {
        self.workspace.mount_path()
    }

    pub fn workspace(&self) -> &WorkspaceRef {
        &self.workspace
    }

    pub(crate) fn supervisor_endpoint(&self) -> &Path {
        &self.supervisor_endpoint
    }
}

/// Compile-time authority fence: project discovery cannot mutate policy.
///
/// ```compile_fail
/// fn cannot_grant(project: &cowshed_core::api::Project) {
///     project.grant("raven");
/// }
/// ```
const _: () = ();

#[cfg(test)]
mod tests {
    use super::*;
    use std::mem::{needs_drop, size_of};

    #[test]
    fn authority_tokens_and_handles_are_affine_owned_values() {
        assert!(needs_drop::<CoordinatorToken>());
        assert!(needs_drop::<Coordinator>());
        assert!(needs_drop::<WorkspaceHandle>());
        assert!(size_of::<CoordinatorToken>() > 0);
    }
}
