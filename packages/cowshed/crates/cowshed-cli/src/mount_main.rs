//! `cowshed mount main --repo-id <owner/repo>`.
//!
//! The one verb that mounts a main without a live checkout. Resolution reads
//! store records only — the repository binding (`repository.json`) and the
//! checkout-path record (the canonical main image's sidecar) — so an empty
//! stub directory left by a broken workspace still resolves, and no git
//! repository is ever consulted.
//!
//! The mount itself goes through the gateway's own path
//! ([`ApfsSubstrate::ensure_mounted`] with `browse` disabled, exactly the
//! call the gateway heal makes): canonical flags come from core's shared
//! mount constructor, and a volume mounted with other flags is detached and
//! remounted by the heal step rather than refused. This module states no
//! mount flags of its own.
//!
//! SliceBRekey note: [`resolve_main_mount`] is the store-record project
//! resolution other verbs can reuse; it takes the store root and a repo id
//! and touches nothing else.

use async_trait::async_trait;
use cowshed_core::apfs::{ApfsCaseSensitivity, SystemCommandRunner};
use cowshed_core::api::MountResult;
use cowshed_core::metadata::{
    CheckoutLayout, DetachedWorkspaceMetadata, ImageFormat, MetadataError, PublicationState,
    WorkspaceName, read_json,
};
use cowshed_core::repository::{RepoId, RepositoryBinding};
use cowshed_core::storage::StorageLayout;
use cowshed_core::storage::apfs::native::MacOsApfsExecutionHost;
use cowshed_core::storage::apfs::{ApfsStorageError, ApfsSubstrate, ApfsSubstrateConfig};
use cowshed_core::storage::lifecycle::{MountIntent, Substrate};
use cowshed_core::{CowshedError, Result, ValidatedHostStorage};
use std::io::Write;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};

use crate::output::Output;
use crate::runtime::DispatchExit;

/// A main resolved from store records, ready to mount.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResolvedMainMount {
    pub repo_id: RepoId,
    /// The adopted checkout path from the sidecar record — possibly an empty
    /// stub, never consulted for git state.
    pub checkout_path: PathBuf,
    pub checkout_layout: CheckoutLayout,
    /// Where main mounts: the checkout itself under direct mount, the uniform
    /// `mnt/<owner>/<repo>/main` under the symlink layout.
    pub mountpoint: PathBuf,
    /// Canonical main images present in the store, in scan order.
    pub images: Vec<PathBuf>,
    /// The project's store directory, where every record above lives.
    pub project_root: PathBuf,
}

/// Resolve a main from store records alone.
///
/// Order: the repository binding, then the checkout-path record carried by
/// the canonical main image's sidecar. A missing checkout-path record is an
/// error naming the expected checkout path, never a silent default. Git —
/// live or otherwise — is not consulted.
pub fn resolve_main_mount(store_root: &Path, repo_id: &RepoId) -> Result<ResolvedMainMount> {
    let layout = StorageLayout::new(store_root, repo_id).map_err(|error| {
        CowshedError::not_found(
            format!(
                "no adopted project {repo_id} in store {}: {error}",
                store_root.display()
            ),
            "cowshed ls --all",
        )
    })?;
    let project = layout.project().clone();
    let binding: RepositoryBinding = match read_json(&project.repository_binding) {
        Ok(binding) => binding,
        Err(MetadataError::Io { source, .. }) if source.kind() == std::io::ErrorKind::NotFound => {
            return Err(CowshedError::not_found(
                format!(
                    "no adopted project {repo_id}: no store record at {}",
                    project.repository_binding.display()
                ),
                "cowshed ls --all",
            ));
        }
        Err(error) => {
            return Err(integrity(format!(
                "invalid repository binding at {}: {error}",
                project.repository_binding.display()
            )));
        }
    };
    let owned = binding.owned_repo_ids().map_err(|error| {
        integrity(format!(
            "invalid repository binding at {}: {error}",
            project.repository_binding.display()
        ))
    })?;
    if !owned.accepts(repo_id) {
        return Err(integrity(format!(
            "repository binding at {} names {}, not {repo_id}",
            project.repository_binding.display(),
            owned.current()
        )));
    }

    let main = WorkspaceName::new("main").map_err(|error| {
        CowshedError::internal(format!("the fixed main workspace name is invalid: {error}"))
    })?;
    // Same derivation the gateway inventory uses: at most one canonical main
    // image, and the checkout path comes from its sidecar's Active snapshot.
    let mut found: Option<Option<PathBuf>> = None;
    let mut images = Vec::new();
    for format in [ImageFormat::Asif, ImageFormat::Sparse] {
        let image = layout
            .main_image(format)
            .map_err(|error| integrity(format!("invalid store layout for {repo_id}: {error}",)))?
            .image()
            .to_owned();
        if !image.try_exists().map_err(|source| {
            integrity(format!(
                "could not inspect canonical main image {}: {source}",
                image.display()
            ))
        })? {
            continue;
        }
        if found.is_some() {
            return Err(integrity(format!(
                "project {repo_id} holds duplicate canonical main image formats",
            )));
        }
        let metadata = DetachedWorkspaceMetadata::read_for_image(&image)
            .map_err(|error| integrity(format!("invalid main metadata for {repo_id}: {error}",)))?;
        if metadata.repo_id != *repo_id || !metadata.workspace.is_main() {
            return Err(integrity(format!(
                "canonical main metadata identity mismatch for {repo_id}",
            )));
        }
        if metadata.publication_state == PublicationState::Active {
            found = Some(
                metadata
                    .require_info_snapshot()
                    .ok()
                    .map(|snapshot| snapshot.project_root.clone()),
            );
        }
        images.push(image);
    }
    let checkout_path = found.flatten().ok_or_else(|| {
        let expected = match layout.checkout_layout() {
            Ok(CheckoutLayout::Symlink) => layout
                .workspace_mount(&main)
                .unwrap_or_else(|_| project.project_root.clone()),
            _ => project.project_root.clone(),
        };
        CowshedError::not_found(
            format!(
                "project {repo_id} records no adopted checkout path; its store record is at {} and its main belongs at {}",
                project.project_root.display(),
                expected.display()
            ),
            "cowshed doctor --json",
        )
    })?;

    let checkout_layout = layout
        .checkout_layout()
        .map_err(|error| integrity(format!("invalid checkout layout for {repo_id}: {error}",)))?;
    let mountpoint = layout
        .main_aware_workspace_mount(checkout_layout, &checkout_path, &main)
        .map_err(|error| integrity(format!("invalid mount layout for {repo_id}: {error}",)))?;
    Ok(ResolvedMainMount {
        repo_id: repo_id.clone(),
        checkout_path,
        checkout_layout,
        mountpoint,
        images,
        project_root: project.project_root.clone(),
    })
}

/// The mount step behind [`dispatch_mount_main`], so tests can prove the
/// verb's contract without a real volume.
#[async_trait]
pub trait MainMountBackend: Send + Sync {
    fn store_root(&self) -> &Path;
    /// Mount main through the gateway path with the given intent. The
    /// canonical implementation remounts a flags-mismatched volume rather
    /// than refusing it; fakes simulate that contract.
    async fn ensure_main_mounted(
        &self,
        resolved: &ResolvedMainMount,
        intent: MountIntent,
    ) -> Result<PathBuf>;
}

/// The production backend: the gateway heal's own mount call.
pub struct NativeMainMountBackend {
    storage: ValidatedHostStorage,
}

impl NativeMainMountBackend {
    pub fn new(storage: ValidatedHostStorage) -> Self {
        Self { storage }
    }
}

/// One mount call, shared by every backend invocation below: `browse`
/// disabled selects the gateway-canonical `nobrowse` flag, and `owners` is
/// unconditional in the shared mount constructor — so no flag is restated
/// here. A flags-mismatched volume is detached and remounted inside
/// `ensure_mounted`'s heal step rather than refused.
const CANONICAL_MOUNT_INTENT: MountIntent = MountIntent { browse: false };

#[async_trait]
impl MainMountBackend for NativeMainMountBackend {
    fn store_root(&self) -> &Path {
        self.storage.store()
    }

    async fn ensure_main_mounted(
        &self,
        resolved: &ResolvedMainMount,
        intent: MountIntent,
    ) -> Result<PathBuf> {
        let config = ApfsSubstrateConfig::new(
            self.storage.store(),
            self.storage.caches(),
            &resolved.checkout_path,
            resolved.checkout_layout,
            ApfsCaseSensitivity::Sensitive,
        );
        let host = MacOsApfsExecutionHost::new(SystemCommandRunner, config.clone())
            .map_err(storage_error)?;
        let substrate = ApfsSubstrate::new(config, host);
        let workspaces = substrate
            .list(&resolved.repo_id)
            .await
            .map_err(storage_error)?;
        let main = workspaces
            .into_iter()
            .find(|derived| derived.workspace.name().is_main())
            .ok_or_else(|| {
                CowshedError::not_found(
                    format!(
                        "project {} records no main workspace: expected its mount at {}",
                        resolved.repo_id,
                        resolved.mountpoint.display()
                    ),
                    "cowshed doctor --json",
                )
            })?;
        substrate
            .ensure_mounted(&main.workspace, intent)
            .await
            .map_err(storage_error)
    }
}

fn storage_error(error: ApfsStorageError) -> CowshedError {
    match error {
        ApfsStorageError::Conflict(conflict) => CowshedError::lifecycle_conflict(conflict),
        // Same mapping as the project bridge: only `rekey` rebuilds the
        // companion, and a quarantine is an operator decision rather than a
        // host defect, so neither must point at `doctor`.
        ref error @ ApfsStorageError::MissingCaCompanion { ref image, .. } => {
            let workspace = image
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| {
                    ImageFormat::from_image_path(image)
                        .ok()
                        .and_then(|format| name.strip_suffix(format.image_extension()))
                })
                .unwrap_or("workspace");
            CowshedError::integrity(error.to_string(), format!("cowshed rekey {workspace}"))
        }
        ref error @ ApfsStorageError::Quarantined { ref workspace, .. } => {
            CowshedError::integrity(error.to_string(), format!("cowshed rekey {workspace}"))
        }
        other => CowshedError::integrity(other.to_string(), "cowshed doctor --json"),
    }
}

fn integrity(message: impl Into<String>) -> CowshedError {
    CowshedError::integrity(message, "cowshed doctor --json")
}

fn write_error(error: std::io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("could not write mount result: {error}"),
        "check that the output consumer is still connected",
    )
}

/// Resolve from the backend's store root and mount main, reporting the live
/// mountpoint. No pre-flight mount or git checks: an empty or stale stub
/// proceeds to the mount path, which heals it.
pub async fn dispatch_mount_main<B, W, E>(
    backend: &B,
    repo_id: &RepoId,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<DispatchExit>
where
    B: MainMountBackend,
    W: Write,
    E: Write,
{
    let resolved = resolve_main_mount(backend.store_root(), repo_id)?;
    let mountpoint = backend
        .ensure_main_mounted(&resolved, CANONICAL_MOUNT_INTENT)
        .await?;
    let main = WorkspaceName::new("main").map_err(|error| {
        CowshedError::internal(format!("the fixed main workspace name is invalid: {error}"))
    })?;
    if json {
        output
            .success(MountResult {
                workspace: main,
                mount: mountpoint.clone(),
                base_commit: None,
            })
            .map_err(write_error)?;
    } else {
        output
            .bare_line(mountpoint.as_os_str().as_bytes())
            .map_err(write_error)?;
    }
    output
        .guidance(&format!(
            "mounted main for {repo_id} at {}",
            mountpoint.display()
        ))
        .map_err(write_error)?;
    output
        .hint(&format!("cd {}", mountpoint.display()))
        .map_err(write_error)?;
    Ok(DispatchExit { code: 0 })
}
