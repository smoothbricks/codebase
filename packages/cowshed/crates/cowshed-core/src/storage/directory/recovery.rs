//! Store recovery, retirement reclamation and garbage collection over directory payloads.
//!
//! Every decision reads the same records the image store writes - grants sidecars, CA
//! companions, trash names, lifecycle locks - so an
//! interrupted operation converges the same way. Only the payload differs: a directory, read
//! in place (a marker is a file in it, not a mount away) and removed as a tree.

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use sha2::{Digest, Sha256};

use super::{
    DirectoryExecutionHost, MountPointState, allocated_tree_bytes, host_error, mount_point_state,
    observed_mount, read_metadata, require_home,
};
use crate::metadata::{
    DetachedWorkspaceMetadata, GRANTS_SIDECAR_SUFFIX, PublicationState, WorkspaceIncarnation,
    WorkspaceName, sidecar_path,
};
use crate::repository::{EXEC_TEMP_DIRECTORY, QUARANTINE_DIRECTORY, RepoId, SESSIONS_DIRECTORY};
use crate::storage::apfs::native::{
    HeldScope, QuarantineTombstone, abandon_bundles, canonical_recovery_sidecars,
    directory_children, gc_candidate_failure_is_fatal, grants_sidecar_missing, image_from_sidecar,
    io_error, metadata_workspace_ref, path_exists, regular_file_children, sync_parent_path,
    warn_session_image_issue,
};
use crate::storage::apfs::{
    ApfsExecutionHost, ApfsStorageError, ApfsSubstrateConfig, companion_path, layout,
    retired_image_below, split_retired_stem, workspace_lock_path,
};
use crate::storage::deletion_log::{self, DeletionKind, DeletionOp};
use crate::storage::lifecycle::{
    RetiredRef, Revision, StorageGcCandidate, StorageGcDeferred, StorageGcOptions, StorageGcPlan,
    StorageGcReason, StorageGcReport, StorageGcRetained,
};
use crate::storage::recovery::{STAGING_NAMESPACE, TRASH_NAMESPACE};
use cowshed_apfs::image::{IMAGE_EXTENSION, is_image_path};
use cowshed_apfs::store_lock::{ImageLockGuard, LockMode, acquire_image_locks};

/// Whether a retirement may collect the artifacts keyed on its workspace name - its exec temp
/// directory and mountpoint - which re-minting the name hands the next
/// lifetime. They belong to the live canonical payload when there is one, else to the newest
/// retirement of that name in the trash.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NameScope {
    Owned,
    Foreign,
}

/// What a retired workspace's reclamation removes beyond its trash payload.
struct RetiredCleanup {
    paths: Vec<PathBuf>,
    mount_point: Option<PathBuf>,
}

fn is_grants_sidecar(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(GRANTS_SIDECAR_SUFFIX))
}

/// The workspace a staging stem (`<name>-<incarnation>`, or the `recover-` form a pre-cutover
/// recovery left) names. The prefix is stripped so such a mount still takes its workspace's lock.
fn staged_stem_workspace(stem: &str) -> Option<WorkspaceName> {
    let stem = stem.strip_prefix("recover-").unwrap_or(stem);
    let (workspace, incarnation) = stem.rsplit_once('-')?;
    WorkspaceIncarnation::new(incarnation).ok()?;
    WorkspaceName::new(workspace).ok()
}

/// Every path a reclaimed payload leaves behind it.
fn payload_gc_paths(image: &Path) -> Vec<PathBuf> {
    vec![image.to_owned(), sidecar_path(image), companion_path(image)]
}

/// The identity of one reclamation, fencing it against any change between preview and
/// execution: every associated path's kind, inode, size and modification time, the content of
/// every record, the target of every link, and each directory's allocated bytes.
fn gc_candidate(
    reason: StorageGcReason,
    path: &Path,
    associated: &[PathBuf],
    extra_identity: &[u8],
) -> Result<StorageGcCandidate, ApfsStorageError> {
    const RECORD_LIMIT: u64 = 64 * 1024;
    let mut hasher = Sha256::new();
    hasher.update(format!("{reason:?}").as_bytes());
    hasher.update(path.as_os_str().as_encoded_bytes());
    hasher.update(extra_identity);
    let mut bytes = 0_u64;
    for item in associated {
        let metadata = match fs::symlink_metadata(item) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => return Err(io_error("inspect GC candidate", item, error)),
        };
        hasher.update(item.as_os_str().as_encoded_bytes());
        hasher.update(metadata.dev().to_le_bytes());
        hasher.update(metadata.ino().to_le_bytes());
        hasher.update(metadata.mtime().to_le_bytes());
        hasher.update(metadata.mtime_nsec().to_le_bytes());
        let allocated = if metadata.is_dir() {
            hasher.update(b"directory");
            allocated_tree_bytes(item)
                .map_err(|error| io_error("measure GC candidate", item, error))?
        } else if metadata.file_type().is_symlink() {
            hasher.update(b"symlink");
            let target = fs::read_link(item)
                .map_err(|error| io_error("read GC candidate link", item, error))?;
            hasher.update(target.as_os_str().as_encoded_bytes());
            // A mount record at an image path stands for the payload it names.
            if is_image_path(item) {
                allocated_tree_bytes(item)
                    .map_err(|error| io_error("measure GC candidate", item, error))?
            } else {
                metadata.blocks().saturating_mul(512)
            }
        } else if metadata.is_file() {
            hasher.update(b"file");
            hasher.update(metadata.len().to_le_bytes());
            if metadata.len() <= RECORD_LIMIT {
                hasher.update(
                    fs::read(item)
                        .map_err(|error| io_error("read GC candidate identity", item, error))?,
                );
            }
            metadata.blocks().saturating_mul(512)
        } else {
            return Err(ApfsStorageError::Host(format!(
                "GC candidate path is neither a file, a directory nor a link: {}",
                item.display()
            )));
        };
        hasher.update(allocated.to_le_bytes());
        bytes = bytes
            .checked_add(allocated)
            .ok_or(ApfsStorageError::InvalidPlan(
                "GC candidate byte accounting overflow",
            ))?;
    }
    Ok(StorageGcCandidate::new(
        hasher.finalize().into(),
        path.to_owned(),
        bytes,
        reason,
    ))
}

fn checked_increment(count: &mut usize) -> Result<(), ApfsStorageError> {
    *count = count
        .checked_add(1)
        .ok_or(ApfsStorageError::InvalidPlan("GC count overflow"))?;
    Ok(())
}

/// Whether the root or a descendant is mounted independently of the root's parent.
/// Mount identities, not backing-device numbers, detect a root mount and Linux bind mounts.
fn tree_crosses_device(root: &Path) -> io::Result<Option<PathBuf>> {
    #[cfg(target_os = "macos")]
    {
        let root = fs::canonicalize(root)?;
        Ok(cowshed_apfs::mounts::kernel_mounts()?
            .into_iter()
            .find(|mount| mount.mount_point.starts_with(&root))
            .map(|mount| mount.mount_point))
    }
    #[cfg(target_os = "linux")]
    {
        use cowshed_os::fsio::{mount_id, open_directory_nofollow};
        use std::os::unix::fs::OpenOptionsExt as _;
        let parent = root.parent().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "owned tree has no parent mount",
            )
        })?;
        let expected = mount_id(&open_directory_nofollow(parent)?)?;
        let mut pending = vec![root.to_owned()];
        while let Some(directory) = pending.pop() {
            let held = open_directory_nofollow(&directory)?;
            if mount_id(&held)? != expected {
                return Ok(Some(directory));
            }
            for entry in fs::read_dir(&directory)? {
                let path = entry?.path();
                // O_PATH pins directory, file and symlink entries without reading or following
                // their contents. A bind-mounted file must refuse before its siblings disappear.
                let entry = fs::OpenOptions::new()
                    .read(true)
                    .custom_flags(libc::O_PATH | libc::O_NOFOLLOW | libc::O_CLOEXEC)
                    .open(&path)?;
                if mount_id(&entry)? != expected {
                    return Ok(Some(path));
                }
                if entry.metadata()?.is_dir() {
                    pending.push(path);
                }
            }
        }
        Ok(None)
    }
}

impl DirectoryExecutionHost {
    fn recovery_lock(
        &self,
        canonical: &Path,
        held_locks: &[PathBuf],
    ) -> Result<Option<Option<ImageLockGuard>>, ApfsStorageError> {
        let lock = crate::metadata::append_suffix(canonical, ".lock");
        if held_locks.contains(&lock) {
            Ok(Some(None))
        } else {
            acquire_image_locks(
                &self.config.store_root,
                &[lock],
                LockMode::Try,
                &crate::held_lock::ImageLockTaker {
                    census: &self.census,
                    holding: &crate::api::held_lock::LockHolding {
                        operation: crate::api::held_lock::LockOperation::Recover,
                        workspace: crate::storage::image_workspace(canonical),
                    },
                },
            )
            .map_err(Into::into)
            .map(|guard| guard.map(Some))
        }
    }

    /// Whether `lock` is free, or held by the very operation asking.
    fn lock_is_free(&self, lock: &Path, held: &[PathBuf]) -> Result<bool, ApfsStorageError> {
        if held.iter().any(|path| path == lock) {
            return Ok(true);
        }
        acquire_image_locks(
            &self.config.store_root,
            &[lock.to_owned()],
            LockMode::Try,
            &crate::held_lock::ImageLockTaker {
                census: &self.census,
                holding: &crate::api::held_lock::LockHolding::store(
                    crate::api::held_lock::LockOperation::Gc,
                ),
            },
        )
        .map_err(Into::into)
        .map(|guard| guard.is_some())
    }

    /// A published payload whose CA companion is missing or invalid has its sidecar (and
    /// companion, if any) quarantined with a tombstone, the payload kept in place, so one
    /// workspace's lost identity never stops the store-wide pass.
    fn check_canonical_companion(
        &self,
        project: &Path,
        metadata: &DetachedWorkspaceMetadata,
        canonical: &Path,
    ) -> Result<(), ApfsStorageError> {
        match self.recovery_companion(canonical, "published canonical payload") {
            Ok(_) => Ok(()),
            Err(ApfsStorageError::MissingCaCompanion {
                image, companion, ..
            }) => Self::quarantine_keyless_canonical(project, metadata, &image, &companion)
                .map(|_| ()),
            Err(other) => Err(other),
        }
    }

    fn quarantine_keyless_canonical(
        project: &Path,
        metadata: &DetachedWorkspaceMetadata,
        image: &Path,
        companion: &Path,
    ) -> Result<PathBuf, ApfsStorageError> {
        let now = SystemTime::now();
        let seconds = now
            .duration_since(SystemTime::UNIX_EPOCH)
            .map(|elapsed| elapsed.as_secs())
            .unwrap_or_default();
        let quarantine_root = project.join(QUARANTINE_DIRECTORY);
        let workspace = metadata.workspace.as_str();
        let mut directory = quarantine_root.join(format!("{workspace}-{seconds}"));
        let mut ordinal = 2_u64;
        while path_exists(&directory)? {
            directory = quarantine_root.join(format!("{workspace}-{seconds}-{ordinal}"));
            ordinal = ordinal.saturating_add(1);
        }
        fs::create_dir_all(&directory)
            .map_err(|error| io_error("create quarantine directory", &directory, error))?;
        sync_parent_path(&directory)?;
        // Tombstone first, then companion, then sidecar: the pass finds work by its sidecars.
        let sidecar = sidecar_path(image);
        let name = sidecar.file_name().ok_or(ApfsStorageError::InvalidPlan(
            "quarantined grants sidecar path has no file name",
        ))?;
        let quarantined_sidecar = directory.join(name);
        let tombstone = directory.join("tombstone.json");
        let record = QuarantineTombstone {
            version: 1,
            repo_id: metadata.repo_id.to_string(),
            workspace: workspace.to_owned(),
            incarnation: metadata.workspace_incarnation.as_str().to_owned(),
            revision: metadata.grants.revision,
            reason: "missing-ca-companion".to_owned(),
            image: image.display().to_string(),
            companion: companion.display().to_string(),
            quarantined_at: deletion_log::rfc3339_utc(now),
            sidecar: quarantined_sidecar.display().to_string(),
        };
        crate::metadata::write_json(&tombstone, &record).map_err(host_error)?;
        if path_exists(companion)? {
            let name = companion.file_name().ok_or(ApfsStorageError::InvalidPlan(
                "quarantined CA companion path has no file name",
            ))?;
            fs::rename(companion, directory.join(name))
                .map_err(|error| io_error("quarantine invalid CA companion", companion, error))?;
        }
        fs::rename(&sidecar, &quarantined_sidecar)
            .map_err(|error| io_error("quarantine keyless grants sidecar", &sidecar, error))?;
        Ok(tombstone)
    }

    /// Converge every interrupted publication and retirement in the store from what
    /// is on disk, under each canonical name's lock.
    pub(super) fn recover_store(
        &self,
        config: &ApfsSubstrateConfig,
        held_locks: &[PathBuf],
    ) -> Result<(), ApfsStorageError> {
        for (project, workspaces) in HeldScope::of_held_locks(&config.store_root, held_locks)?
            .projects(&config.store_root)?
        {
            self.recover_canonical_sidecars(config, &project, workspaces.as_ref(), held_locks)?;
            self.recover_staged_sidecars(config, &project, workspaces.as_ref(), held_locks)?;
        }
        Ok(())
    }

    fn recover_canonical_sidecars(
        &self,
        config: &ApfsSubstrateConfig,
        project: &Path,
        workspaces: Option<&BTreeSet<WorkspaceName>>,
        held_locks: &[PathBuf],
    ) -> Result<(), ApfsStorageError> {
        let sessions = project.join(SESSIONS_DIRECTORY);
        for directory in [project, sessions.as_path()] {
            for canonical_sidecar in canonical_recovery_sidecars(directory, project, workspaces)? {
                if !is_grants_sidecar(&canonical_sidecar) {
                    continue;
                }
                let canonical = image_from_sidecar(&canonical_sidecar)?;
                if !is_image_path(&canonical) {
                    continue;
                }
                if path_exists(&canonical)? {
                    let metadata = read_metadata(&canonical)?;
                    // A create, fork or adopt behind its init fence is resumed by its lifecycle
                    // intent; generic recovery neither blesses nor reclaims it.
                    if metadata.publication_state == PublicationState::PendingFence {
                        continue;
                    }
                    self.check_canonical_companion(project, &metadata, &canonical)?;
                    continue;
                }
                let metadata = read_metadata(&canonical)?;
                let storage = layout(config, &metadata.repo_id)?;
                let expected = storage
                    .canonical_image(&metadata.workspace)?
                    .image()
                    .to_owned();
                if storage.project().project_root != project || expected != canonical {
                    continue;
                }
                let Some(_guard) = self.recovery_lock(&canonical, held_locks)? else {
                    continue;
                };
                if path_exists(&canonical)? {
                    self.check_canonical_companion(project, &metadata, &canonical)?;
                    continue;
                }
                // The payload is gone - a publication that died before it, or a retirement that
                // already moved it to the trash - so its derived records describe nothing.
                // Companion before sidecar: the sidecar is what brings a later pass back here.
                Self::remove_companion(&canonical)?;
                fs::remove_file(&canonical_sidecar).map_err(|error| {
                    io_error(
                        "remove orphan canonical metadata",
                        &canonical_sidecar,
                        error,
                    )
                })?;
                sync_parent_path(&canonical_sidecar)?;
            }
        }
        Ok(())
    }

    fn recover_staged_sidecars(
        &self,
        config: &ApfsSubstrateConfig,
        project: &Path,
        workspaces: Option<&BTreeSet<WorkspaceName>>,
        held_locks: &[PathBuf],
    ) -> Result<(), ApfsStorageError> {
        let staging = project.join(STAGING_NAMESPACE);
        for path in regular_file_children(&staging)? {
            if !is_grants_sidecar(&path) {
                continue;
            }
            let staged = image_from_sidecar(&path)?;
            if !is_image_path(&staged) {
                continue;
            }
            if let Some(workspaces) = workspaces
                && !staged
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(split_retired_stem)
                    .is_some_and(|(workspace, _)| workspaces.contains(&workspace))
            {
                continue;
            }
            let metadata = read_metadata(&staged)?;
            let storage = layout(config, &metadata.repo_id)?;
            if storage.project().project_root != project {
                continue;
            }
            let canonical = storage
                .canonical_image(&metadata.workspace)?
                .image()
                .to_owned();
            let Some(_guard) = self.recovery_lock(&canonical, held_locks)? else {
                continue;
            };
            if read_metadata(&staged)? != metadata {
                continue;
            }
            if path_exists(&staged)? {
                // An unnamed staged pair is an orphan the GC sweep reclaims under its lock.
                if path_exists(&companion_path(&staged))? {
                    self.recovery_companion(&staged, "staged publication payload")?;
                }
                continue;
            }
            if path_exists(&canonical)? && !path_exists(&sidecar_path(&canonical))? {
                self.recovery_companion(&canonical, "canonical payload awaiting metadata")?;
                let destination = sidecar_path(&canonical);
                fs::rename(&path, &destination).map_err(|error| {
                    io_error(
                        "complete canonical sidecar publication",
                        &destination,
                        error,
                    )
                })?;
                sync_parent_path(&canonical)?;
            }
        }
        Ok(())
    }

    pub(super) fn reclaim_retired_record(
        &self,
        config: &ApfsSubstrateConfig,
        retired: &RetiredRef,
    ) -> Result<(), ApfsStorageError> {
        if config.store_root != self.config.store_root {
            return Err(ApfsStorageError::InvalidPlan(
                "retired cleanup config differs from host storage root",
            ));
        }
        let project = layout(config, retired.workspace().repo())?
            .project()
            .project_root
            .clone();
        let trash = retired_image_below(
            &project.join(SESSIONS_DIRECTORY),
            retired.workspace().name(),
            retired.workspace().incarnation(),
        );
        if !path_exists(&trash)? && !path_exists(&sidecar_path(&trash))? {
            return Ok(());
        }
        self.reclaim_retired_authority(
            &project,
            retired.workspace().repo(),
            &trash,
            Some(retired),
            None,
        )
    }

    /// The retirement a trash payload records, or `None` when its sidecar is already gone.
    fn retired_authority(
        &self,
        project: &Path,
        repo: &RepoId,
        trash_image: &Path,
    ) -> Result<Option<RetiredRef>, ApfsStorageError> {
        if grants_sidecar_missing(trash_image)? {
            return Ok(None);
        }
        let metadata = read_metadata(trash_image)?;
        if metadata.repo_id != *repo {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "retired metadata disagrees with trash payload {}",
                trash_image.display()
            )));
        }
        let workspace = metadata_workspace_ref(&metadata)?;
        let expected = retired_image_below(
            &project.join(SESSIONS_DIRECTORY),
            workspace.name(),
            workspace.incarnation(),
        );
        if trash_image != expected {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "retired metadata disagrees with trash path {}",
                trash_image.display()
            )));
        }
        let revision = Revision::new(workspace.revision().get().saturating_add(1));
        Ok(Some(RetiredRef::new(workspace, revision)))
    }

    fn retired_trash_images(trash: &Path) -> Result<Vec<PathBuf>, ApfsStorageError> {
        #[cfg(test)]
        crate::storage::apfs::record_retired_trash_observation();
        let mut images = BTreeSet::new();
        for path in directory_children(trash)? {
            if is_image_path(&path) {
                images.insert(path);
            }
        }
        for path in regular_file_children(trash)? {
            if is_grants_sidecar(&path) {
                let image = image_from_sidecar(&path)?;
                if is_image_path(&image) {
                    images.insert(image);
                }
            }
        }
        Ok(images.into_iter().collect())
    }

    fn retired_name_scope(
        &self,
        retired: &RetiredRef,
        newest_retired: Option<&WorkspaceIncarnation>,
    ) -> Result<NameScope, ApfsStorageError> {
        let workspace = retired.workspace();
        let canonical = layout(&self.config, workspace.repo())?
            .canonical_image(workspace.name())?
            .image()
            .to_owned();
        if path_exists(&canonical)? || path_exists(&sidecar_path(&canonical))? {
            return Ok(NameScope::Foreign);
        }
        Ok(match newest_retired {
            Some(newest) if newest != workspace.incarnation() => NameScope::Foreign,
            _ => NameScope::Owned,
        })
    }

    /// The mountpoint a retirement may remove: an empty directory. Anything a late writer left
    /// there is named, never deleted.
    fn retired_mount_point(
        &self,
        retired: &RetiredRef,
    ) -> Result<Option<PathBuf>, ApfsStorageError> {
        let mount_point = layout(&self.config, retired.workspace().repo())?
            .project()
            .mount_root
            .join(retired.workspace().name().as_str());
        match mount_point_state(&mount_point)? {
            MountPointState::Absent => Ok(None),
            MountPointState::Vacant => Ok(Some(mount_point)),
            MountPointState::Link => Err(ApfsStorageError::Host(format!(
                "retired mountpoint {} is a symbolic link cowshed did not make",
                mount_point.display()
            ))),
            MountPointState::Occupied => Err(ApfsStorageError::Host(format!(
                "retired mountpoint {} holds files written after its workspace was retired; move or delete them",
                mount_point.display()
            ))),
        }
    }

    fn retired_cleanup(
        &self,
        retired: &RetiredRef,
        trash_image: &Path,
        scope: NameScope,
    ) -> Result<RetiredCleanup, ApfsStorageError> {
        let mut cleanup = RetiredCleanup {
            paths: Vec::new(),
            mount_point: None,
        };
        if scope == NameScope::Owned {
            cleanup.mount_point = self.retired_mount_point(retired)?;
        }
        cleanup.paths.extend(payload_gc_paths(trash_image));
        cleanup.paths.sort();
        cleanup.paths.dedup();
        Ok(cleanup)
    }

    fn reclaim_retired_authority(
        &self,
        project: &Path,
        repo: &RepoId,
        trash_image: &Path,
        expected: Option<&RetiredRef>,
        observation: Option<&mut crate::storage::apfs::RetiredNameOwners>,
    ) -> Result<(), ApfsStorageError> {
        let authority = match (
            self.retired_authority(project, repo, trash_image)?,
            expected,
        ) {
            (Some(authority), Some(expected)) if expected.workspace() != authority.workspace() => {
                return Err(ApfsStorageError::MarkerMismatch(format!(
                    "retired cleanup authority changed for {}",
                    trash_image.display()
                )));
            }
            (Some(authority), _) => authority,
            (None, Some(expected)) => expected.clone(),
            // A payload whose record is gone names no workspace's environment: only its own
            // files are released.
            (None, None) => return self.reclaim_image(trash_image),
        };
        let reclaim = |owners: &crate::storage::apfs::RetiredNameOwners| {
            let scope =
                self.retired_name_scope(&authority, owners.get(authority.workspace().name()))?;
            self.reclaim_workspace_artifacts(project, &authority, trash_image, scope)
        };
        match observation {
            Some(owners) => {
                let result = reclaim(owners);
                owners.after_reclaim(&authority, trash_image, |kind| kind.is_dir(), result)
            }
            None => {
                let trash = project.join(SESSIONS_DIRECTORY).join(TRASH_NAMESPACE);
                let owners = crate::storage::apfs::RetiredNameOwners::from_images(
                    &Self::retired_trash_images(&trash)?,
                );
                reclaim(&owners)
            }
        }
    }

    pub(super) fn release_canonical_workspace(
        &self,
        config: &ApfsSubstrateConfig,
        retired: &RetiredRef,
    ) -> Result<(), ApfsStorageError> {
        if config.store_root != self.config.store_root {
            return Err(ApfsStorageError::InvalidPlan(
                "workspace release config differs from host storage root",
            ));
        }
        let workspace = retired.workspace();
        let canonical = layout(config, workspace.repo())?
            .canonical_image(workspace.name())?
            .image()
            .to_path_buf();
        self.require_store_image(&canonical)?;
        require_home(&canonical)?;
        if metadata_workspace_ref(&read_metadata(&canonical)?)? != *workspace {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "workspace release identity changed: {}",
                canonical.display()
            )));
        }
        self.validate_staged_companion(&companion_path(&canonical))?;
        let project = layout(config, workspace.repo())?
            .project()
            .project_root
            .clone();
        self.reclaim_workspace_artifacts(&project, retired, &canonical, NameScope::Owned)
    }

    /// Disposable environment only; the caller has already proved this retirement owns
    /// the name. Source payloads and their records remain separate custody.
    fn reclaim_exec_temp(
        project: &Path,
        authority: &RetiredRef,
        trash_image: &Path,
    ) -> Result<(), ApfsStorageError> {
        let name = authority.workspace().name().as_str();
        let exec_temp = project.join(EXEC_TEMP_DIRECTORY).join(name);
        match fs::symlink_metadata(&exec_temp) {
            Ok(metadata) => {
                let removed = if metadata.is_dir() {
                    if let Some(mounted) = tree_crosses_device(&exec_temp).map_err(|error| {
                        io_error("inspect retired exec temp dir", &exec_temp, error)
                    })? {
                        return Err(ApfsStorageError::Host(format!(
                            "retired exec temp dir {} carries a mounted filesystem at {}; detach it, then retry the retirement or recovery export",
                            exec_temp.display(),
                            mounted.display()
                        )));
                    }
                    cowshed_os::fsio::remove_owned_tree(&exec_temp)
                } else {
                    fs::remove_file(&exec_temp)
                };
                removed
                    .map_err(|error| io_error("remove retired exec temp dir", &exec_temp, error))?;
                deletion_log::log_deletion(
                    project,
                    DeletionOp::ReclaimRetiredArtifact,
                    DeletionKind::Other,
                    name,
                    Some(trash_image),
                    &exec_temp,
                );
                sync_parent_path(&exec_temp)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io_error("inspect retired exec temp dir", &exec_temp, error)),
        }
    }

    fn reclaim_workspace_artifacts(
        &self,
        project: &Path,
        authority: &RetiredRef,
        trash_image: &Path,
        scope: NameScope,
    ) -> Result<(), ApfsStorageError> {
        let cleanup = self.retired_cleanup(authority, trash_image, scope)?;
        let name = authority.workspace().name().as_str();
        let log = |artifact: &Path| {
            deletion_log::log_deletion(
                project,
                DeletionOp::ReclaimRetiredArtifact,
                DeletionKind::Other,
                name,
                Some(trash_image),
                artifact,
            );
        };
        if scope == NameScope::Owned {
            Self::reclaim_exec_temp(project, authority, trash_image)?;
        }
        if let Some(mount_point) = cleanup.mount_point {
            let removed = match mount_point_state(&mount_point)? {
                MountPointState::Vacant => fs::remove_dir(&mount_point),
                MountPointState::Absent => Ok(()),
                other => {
                    return Err(ApfsStorageError::Host(format!(
                        "retired mountpoint {} changed before its removal: {other:?}",
                        mount_point.display()
                    )));
                }
            };
            removed.map_err(|error| io_error("remove retired mountpoint", &mount_point, error))?;
            log(&mount_point);
            sync_parent_path(&mount_point)?;
        }
        self.reclaim_image(trash_image)
    }

    /// Whether a staging stem is still named by an interrupted publication: a canonical sidecar
    /// without its payload is completed from that stem by recovery.
    fn recoverable_staged_stems(project: &Path) -> Result<BTreeSet<String>, ApfsStorageError> {
        let mut stems = BTreeSet::new();
        let sessions = project.join(SESSIONS_DIRECTORY);
        for directory in [project, sessions.as_path()] {
            for sidecar in regular_file_children(directory)? {
                if !is_grants_sidecar(&sidecar) {
                    continue;
                }
                let canonical = image_from_sidecar(&sidecar)?;
                if path_exists(&canonical)? || !is_image_path(&canonical) {
                    continue;
                }
                let metadata = read_metadata(&canonical)?;
                stems.insert(format!(
                    "{}-{}",
                    metadata.workspace.as_str(),
                    metadata.workspace_incarnation
                ));
            }
        }
        Ok(stems)
    }

    pub(super) fn preview_gc_project(
        &self,
        project: &Path,
        repo: &RepoId,
        observed_at: SystemTime,
        held_locks: &[PathBuf],
        options: StorageGcOptions,
    ) -> Result<(StorageGcPlan, crate::storage::apfs::RetiredNameOwners), ApfsStorageError> {
        let mut candidates = Vec::new();
        let mut lock_paths = Vec::new();
        let mut examined = 0_usize;
        let mut retained = StorageGcRetained::default();

        // Retired workspaces in the trash.
        let sessions = project.join(SESSIONS_DIRECTORY);
        let trash_images = Self::retired_trash_images(&sessions.join(TRASH_NAMESPACE))?;
        let owners = crate::storage::apfs::RetiredNameOwners::from_images(&trash_images);
        for path in trash_images {
            let Some(retired) = self.retired_authority(project, repo, &path)? else {
                // A payload whose grants sidecar is gone names no workspace's environment, so only
                // its own files go.
                let Some((name, _)) = path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(split_retired_stem)
                else {
                    warn_session_image_issue(
                        &path,
                        "retired payload has no grants sidecar and no <workspace>-<incarnation> name",
                    );
                    continue;
                };
                checked_increment(&mut examined)?;
                lock_paths.push(workspace_lock_path(&self.config, repo, &name)?);
                candidates.push(gc_candidate(
                    StorageGcReason::RetiredWorkspace,
                    &path,
                    &payload_gc_paths(&path),
                    &[],
                )?);
                continue;
            };
            let scope =
                self.retired_name_scope(&retired, owners.get(retired.workspace().name()))?;
            let cleanup = self.retired_cleanup(&retired, &path, scope)?;
            let mut associated = cleanup.paths;
            associated.extend(cleanup.mount_point);
            checked_increment(&mut examined)?;
            lock_paths.push(workspace_lock_path(
                &self.config,
                repo,
                retired.workspace().name(),
            )?);
            candidates.push(gc_candidate(
                StorageGcReason::RetiredWorkspace,
                &path,
                &associated,
                &[],
            )?);
        }

        // The abandon bundles share the trash with retired payloads but are not payloads: each is
        // the recovery file of the commits its removal abandoned, whose tips the controller keeps
        // in main before executing such a plan. Only a sweep explicitly authorized for them names
        // them; a plain `gc` leaves them to the operator.
        if options.abandoned_bundles {
            for bundle in abandon_bundles(&sessions.join(TRASH_NAMESPACE))? {
                checked_increment(&mut examined)?;
                candidates.push(gc_candidate(
                    StorageGcReason::AbandonedBundle,
                    &bundle,
                    std::slice::from_ref(&bundle),
                    &[],
                )?);
            }
        }

        // Session payloads with no grants sidecar: never published, nothing names them.
        let session_entries = super::read_dir_paths(&sessions, "enumerate session payloads")?;
        for discovered in crate::storage::discover_session_images(session_entries) {
            if !grants_sidecar_missing(discovered.path())? {
                continue;
            }
            let lock = workspace_lock_path(&self.config, repo, discovered.workspace())?;
            if !self.lock_is_free(&lock, held_locks)? {
                checked_increment(&mut retained.active)?;
                continue;
            }
            if let Some(mount_point) = observed_mount(discovered.path())? {
                warn_session_image_issue(
                    discovered.path(),
                    format!(
                        "payload without a grants sidecar is still mounted at {}; it is left in place",
                        mount_point.display()
                    ),
                );
                continue;
            }
            checked_increment(&mut examined)?;
            lock_paths.push(lock);
            candidates.push(gc_candidate(
                StorageGcReason::OrphanSessionImage,
                discovered.path(),
                &payload_gc_paths(discovered.path()),
                &[],
            )?);
        }

        // Staging payloads and records no operation or recovery still names.
        let storage = layout(&self.config, repo)?;
        let staging = project.join(STAGING_NAMESPACE);
        let recoverable = Self::recoverable_staged_stems(project)?;
        // A staged payload may be mounted, its image path a mount record: entries of every kind.
        let staged_entries = read_entries(&staging)?;
        for path in staged_entries {
            let (image, reason) = if is_grants_sidecar(&path) {
                let image = image_from_sidecar(&path)?;
                if path_exists(&image)? {
                    continue;
                }
                (image, StorageGcReason::OrphanStagingMetadata)
            } else if is_image_path(&path) {
                (path.clone(), StorageGcReason::OrphanStagingImage)
            } else {
                continue;
            };
            let Some(stem) = image.file_stem().and_then(|stem| stem.to_str()) else {
                continue;
            };
            let Some(workspace) = staged_stem_workspace(stem) else {
                continue;
            };
            if recoverable.contains(stem) {
                continue;
            }
            let lock = workspace_lock_path(&self.config, repo, &workspace)?;
            if !self.lock_is_free(&lock, held_locks)? {
                checked_increment(&mut retained.active)?;
                continue;
            }
            checked_increment(&mut examined)?;
            lock_paths.push(lock);
            let associated = match reason {
                StorageGcReason::OrphanStagingMetadata => vec![path.clone()],
                _ => payload_gc_paths(&image),
            };
            candidates.push(gc_candidate(reason, &path, &associated, &[])?);
        }

        // Staging mountpoints nothing is mounted at: vacant directories. An empty payload
        // mounted there looks the same, so a mountpoint a staged or canonical payload's mount
        // record names is not an orphan.
        let staging_mounts = storage.project().mount_root.join(STAGING_NAMESPACE);
        for mount_point in read_entries(&staging_mounts)? {
            let Some(stem) = mount_point.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(workspace) = staged_stem_workspace(stem) else {
                continue;
            };
            if mount_point_state(&mount_point)? != MountPointState::Vacant {
                continue;
            }
            let staged_image = staging.join(format!("{stem}.{IMAGE_EXTENSION}"));
            let canonical = storage.canonical_image(&workspace)?.image().to_owned();
            if names_mount(&staged_image, &mount_point)? || names_mount(&canonical, &mount_point)? {
                continue;
            }
            let lock = workspace_lock_path(&self.config, repo, &workspace)?;
            if !self.lock_is_free(&lock, held_locks)? {
                checked_increment(&mut retained.active)?;
                continue;
            }
            checked_increment(&mut examined)?;
            lock_paths.push(lock);
            candidates.push(gc_candidate(
                StorageGcReason::OrphanStagingMount,
                &mount_point,
                std::slice::from_ref(&mount_point),
                &[],
            )?);
        }

        candidates.sort_by(|left, right| {
            left.path()
                .cmp(right.path())
                .then_with(|| format!("{:?}", left.reason()).cmp(&format!("{:?}", right.reason())))
        });
        lock_paths.sort();
        lock_paths.dedup();
        Ok((
            StorageGcPlan {
                repo: repo.clone(),
                observed_at,
                options,
                candidates,
                lock_paths,
                examined,
                retained,
                root_held_mountpoints: Vec::new(),
            },
            owners,
        ))
    }

    pub(super) fn execute_gc_plan(
        &self,
        project: &Path,
        plan: StorageGcPlan,
    ) -> Result<StorageGcReport, ApfsStorageError> {
        let _guard = acquire_image_locks(
            &self.config.store_root,
            plan.lock_paths(),
            LockMode::Try,
            &crate::held_lock::ImageLockTaker {
                census: &self.census,
                holding: &crate::api::held_lock::LockHolding::store(
                    crate::api::held_lock::LockOperation::Gc,
                ),
            },
        )?
        .ok_or(ApfsStorageError::GcPlanStale)?;
        let (current, mut owners) = self.preview_gc_project(
            project,
            plan.repo(),
            plan.observed_at(),
            plan.lock_paths(),
            plan.options(),
        )?;
        if current != plan {
            return Err(ApfsStorageError::GcPlanStale);
        }
        let mut report = StorageGcReport {
            examined: plan.examined(),
            retained_active: plan.retained_active(),
            ..StorageGcReport::default()
        };
        for candidate in plan.candidates() {
            let path = candidate.path();
            let result = match candidate.reason() {
                StorageGcReason::RetiredWorkspace => self.reclaim_retired_authority(
                    project,
                    plan.repo(),
                    path,
                    None,
                    Some(&mut owners),
                ),
                StorageGcReason::OrphanSessionImage | StorageGcReason::OrphanStagingImage => {
                    self.reclaim_image(path)
                }
                StorageGcReason::OrphanStagingMetadata => fs::remove_file(path)
                    .map_err(|error| io_error("remove orphan staging metadata", path, error))
                    .and_then(|()| {
                        deletion_log::log_deletion(
                            project,
                            DeletionOp::RemoveOrphanStagingMetadata,
                            DeletionKind::Sidecar,
                            "",
                            None,
                            path,
                        );
                        sync_parent_path(path)
                    }),
                StorageGcReason::OrphanStagingMount => {
                    let removed = match mount_point_state(path)? {
                        MountPointState::Vacant => fs::remove_dir(path),
                        MountPointState::Absent => Ok(()),
                        MountPointState::Link | MountPointState::Occupied => {
                            Err(io::Error::other("staging mountpoint is no longer vacant"))
                        }
                    };
                    removed
                        .map_err(|error| io_error("remove orphan staging mount", path, error))
                        .and_then(|()| {
                            deletion_log::log_deletion(
                                project,
                                DeletionOp::RemoveStagingMount,
                                DeletionKind::Other,
                                "",
                                None,
                                path,
                            );
                            sync_parent_path(path)
                        })
                }
                StorageGcReason::AbandonedBundle => fs::remove_file(path)
                    .map_err(|error| io_error("remove abandon bundle", path, error))
                    .and_then(|()| {
                        deletion_log::log_deletion(
                            project,
                            DeletionOp::RemoveAbandonedBundle,
                            DeletionKind::Other,
                            "",
                            None,
                            path,
                        );
                        sync_parent_path(path)
                    }),
                StorageGcReason::OrphanMountpoint => Err(ApfsStorageError::InvalidPlan(
                    "directory store plans never name orphan mountpoints",
                )),
            };
            match result {
                Ok(()) => {
                    report.reclaimed = report
                        .reclaimed
                        .checked_add(1)
                        .ok_or(ApfsStorageError::InvalidPlan("GC count overflow"))?;
                    report.released_bytes =
                        report.released_bytes.checked_add(candidate.bytes()).ok_or(
                            ApfsStorageError::InvalidPlan("GC freed byte accounting overflow"),
                        )?;
                }
                Err(error) if gc_candidate_failure_is_fatal(&error) => return Err(error),
                Err(error) => report.deferred.push(StorageGcDeferred {
                    path: path.to_owned(),
                    diagnostic: error.to_string(),
                }),
            }
        }
        Ok(report)
    }
}

/// Every entry directly in `directory`; none when it does not exist.
fn read_entries(directory: &Path) -> Result<Vec<PathBuf>, ApfsStorageError> {
    match fs::read_dir(directory) {
        Ok(entries) => entries
            .map(|entry| {
                entry
                    .map(|entry| entry.path())
                    .map_err(|error| io_error("read directory entry", directory, error))
            })
            .collect(),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(error) => Err(io_error("enumerate directory", directory, error)),
    }
}

/// Whether `image` is a mount record naming `mount_point`, read without following anything.
fn names_mount(image: &Path, mount_point: &Path) -> Result<bool, ApfsStorageError> {
    match fs::symlink_metadata(image) {
        Ok(metadata) if metadata.file_type().is_symlink() => fs::read_link(image)
            .map(|target| target == mount_point)
            .map_err(|error| io_error("read mount record", image, error)),
        Ok(_) => Ok(false),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("inspect payload", image, error)),
    }
}
