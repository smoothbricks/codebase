//! Store recovery, retirement reclamation and garbage collection over directory payloads.
//!
//! Every decision reads the same records the image store writes - grants sidecars, CA
//! companions, checkpoint and restore-recovery facts, trash names, lifecycle locks - so an
//! interrupted operation converges the same way. Only the payload differs: a directory, read
//! in place (a marker is a file in it, not a mount away) and removed as a tree.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use sha2::{Digest, Sha256};

use super::{
    DirectoryExecutionHost, MountPointState, allocated_tree_bytes, host_error, mount_point_state,
    read_metadata,
};
use cowshed_apfs::image::{IMAGE_EXTENSION, is_image_path};
use cowshed_apfs::store_lock::{ImageLockGuard, LockMode, acquire_image_locks};
use crate::metadata::{
    DetachedWorkspaceMetadata, GRANTS_SIDECAR_SUFFIX, PublicationState,
    WorkspaceIncarnation, WorkspaceMarker, WorkspaceName, sidecar_path,
};
use crate::repository::{
    CHECKPOINTS_DIRECTORY, EXEC_TEMP_DIRECTORY, QUARANTINE_DIRECTORY, RepoId, SESSIONS_DIRECTORY,
};
use crate::storage::apfs::native::{
    CHECKPOINT_FACT_VERSION, CheckpointFactWire, CheckpointPinWire,
    QuarantineTombstone, RESTORE_RECOVERY_FACT_VERSION, RestoreRecoveryFactWire,
    checkpoint_fact_path, collect_project_directories,
    collect_restore_sidecars, directory_children, gc_candidate_failure_is_fatal,
    grants_sidecar_missing, image_from_sidecar, io_error, metadata_workspace_ref, path_exists,
    regular_file_children, restore_recovery_fact_path, swap_paths, sync_parent_path,
    validate_restore_recovery_lineage, warn_session_image_issue,
};
use crate::storage::apfs::{
    ApfsExecutionHost, ApfsStorageError, ApfsSubstrateConfig, companion_path, layout,
    retired_image_below, split_retired_stem, workspace_lock_path,
};
use crate::storage::deletion_log::{self, DeletionKind, DeletionOp};
use crate::storage::lifecycle::{
    RetiredRef, Revision, StorageGcCandidate, StorageGcDeferred, StorageGcPlan, StorageGcReason,
    StorageGcReport, StorageGcRetained,
};
use crate::storage::recovery::{STAGING_NAMESPACE, TRASH_NAMESPACE};
use crate::storage::{CheckpointLabel, PRE_RESTORE_PREFIX, WORKSPACE_MARKER_PATH};

/// Whether a retirement may collect the artifacts keyed on its workspace name - its checkpoint
/// directory, exec temp directory and mountpoint - which re-minting the name hands the next
/// lifetime. They belong to the live canonical payload when there is one, else to the newest
/// retirement of that name in the trash.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NameScope {
    Owned,
    Foreign,
}

/// What a retired workspace's reclamation removes beyond its trash payload.
struct RetiredCleanup {
    checkpoint_images: Vec<PathBuf>,
    paths: Vec<PathBuf>,
    mount_point: Option<PathBuf>,
}

fn is_grants_sidecar(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| name.ends_with(GRANTS_SIDECAR_SUFFIX))
}

/// The workspace a staging stem (`<name>-<incarnation>`, or recovery's `recover-` form) names.
fn staged_stem_workspace(stem: &str) -> Option<WorkspaceName> {
    let stem = stem.strip_prefix("recover-").unwrap_or(stem);
    let (workspace, incarnation) = stem.rsplit_once('-')?;
    WorkspaceIncarnation::new(incarnation).ok()?;
    WorkspaceName::new(workspace).ok()
}

/// Every path a reclaimed payload leaves behind it.
fn payload_gc_paths(image: &Path) -> Vec<PathBuf> {
    vec![
        image.to_owned(),
        sidecar_path(image),
        companion_path(image),
        checkpoint_fact_path(image),
        restore_recovery_fact_path(image),
    ]
}

/// The newest retired incarnation of every name in the trash, read off the trash names so the
/// answer is identical at preview and at execution.
fn retired_name_owners(images: &[PathBuf]) -> BTreeMap<WorkspaceName, WorkspaceIncarnation> {
    let mut owners: BTreeMap<WorkspaceName, WorkspaceIncarnation> = BTreeMap::new();
    for image in images {
        let Some((name, incarnation)) = image
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(split_retired_stem)
        else {
            continue;
        };
        match owners.get_mut(&name) {
            Some(owner) if *owner < incarnation => *owner = incarnation,
            Some(_) => {}
            None => {
                owners.insert(name, incarnation);
            }
        }
    }
    owners
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
            metadata.blocks().saturating_mul(512)
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
        bytes = bytes.checked_add(allocated).ok_or(ApfsStorageError::InvalidPlan(
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

/// Whether any directory under `root` sits on another device: a filesystem mounted inside the
/// tree, which removing the tree would empty through its mountpoint.
fn tree_crosses_device(root: &Path) -> io::Result<Option<PathBuf>> {
    let device = fs::symlink_metadata(root)?.dev();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        for entry in fs::read_dir(&directory)? {
            let path = entry?.path();
            let metadata = fs::symlink_metadata(&path)?;
            if metadata.dev() != device {
                return Ok(Some(path));
            }
            if metadata.is_dir() {
                pending.push(path);
            }
        }
    }
    Ok(None)
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
            acquire_image_locks(&self.config.store_root, &[lock], LockMode::Try)
                .map(|guard| guard.map(Some))
        }
    }

    /// Whether `lock` is free, or held by the very operation asking.
    fn lock_is_free(&self, lock: &Path, held: &[PathBuf]) -> Result<bool, ApfsStorageError> {
        if held.iter().any(|path| path == lock) {
            return Ok(true);
        }
        acquire_image_locks(&self.config.store_root, &[lock.to_owned()], LockMode::Try)
            .map(|guard| guard.is_some())
    }

    /// The incarnation the marker inside a detached payload names: read in place.
    fn payload_incarnation(image: &Path) -> Result<String, ApfsStorageError> {
        WorkspaceMarker::read_from(&image.join(WORKSPACE_MARKER_PATH))
            .map(|marker| marker.workspace_incarnation.to_string())
            .map_err(host_error)
    }

    fn companions_match(left: &Path, right: &Path) -> Result<bool, ApfsStorageError> {
        let left_metadata = fs::symlink_metadata(left)
            .map_err(|error| io_error("inspect recovery CA identity", left, error))?;
        let right_metadata = fs::symlink_metadata(right)
            .map_err(|error| io_error("inspect recovery CA identity", right, error))?;
        Ok(left_metadata.dev() == right_metadata.dev()
            && left_metadata.ino() == right_metadata.ino())
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
        sync_parent_path(&tombstone)?;
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

    /// Converge every interrupted publication, restore and retirement in the store from what
    /// is on disk, under each canonical name's lock.
    pub(super) fn recover_store(
        &self,
        config: &ApfsSubstrateConfig,
        held_locks: &[PathBuf],
    ) -> Result<(), ApfsStorageError> {
        for project in collect_project_directories(&config.store_root)? {
            self.recover_canonical_sidecars(config, &project, held_locks)?;
            self.recover_staged_sidecars(config, &project, held_locks)?;
            self.recover_restores(config, &project, held_locks)?;
        }
        Ok(())
    }

    fn recover_canonical_sidecars(
        &self,
        config: &ApfsSubstrateConfig,
        project: &Path,
        held_locks: &[PathBuf],
    ) -> Result<(), ApfsStorageError> {
        let sessions = project.join(SESSIONS_DIRECTORY);
        for directory in [project, sessions.as_path()] {
            for canonical_sidecar in regular_file_children(directory)? {
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
                    if metadata.publication_state == PublicationState::PendingFence
                        && !path_exists(&restore_recovery_fact_path(&canonical))?
                    {
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
                // A restore in flight owns this state; the restore pass decides it.
                if path_exists(&restore_recovery_fact_path(&canonical))? {
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
            let restore_undo = storage
                .project()
                .checkpoints
                .join(metadata.workspace.as_str())
                .join(format!(
                    "{PRE_RESTORE_PREFIX}{}.{IMAGE_EXTENSION}",
                    metadata.workspace_incarnation,
                ));
            if path_exists(&sidecar_path(&restore_undo))? {
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
                    io_error("complete canonical sidecar publication", &destination, error)
                })?;
                sync_parent_path(&canonical)?;
            }
        }
        Ok(())
    }

    /// Finish or roll back every restore an undo generation still records.
    fn recover_restores(
        &self,
        config: &ApfsSubstrateConfig,
        project: &Path,
        held_locks: &[PathBuf],
    ) -> Result<(), ApfsStorageError> {
        let mut undo_sidecars = Vec::new();
        collect_restore_sidecars(project, &mut undo_sidecars)?;
        undo_sidecars.sort();
        for undo_sidecar in undo_sidecars {
            let undo = image_from_sidecar(&undo_sidecar)?;
            if !is_image_path(&undo) {
                continue;
            }
            let Some(replacement_incarnation) = undo
                .file_stem()
                .and_then(|stem| stem.to_str())
                .and_then(|stem| stem.strip_prefix(PRE_RESTORE_PREFIX))
                .filter(|stem| WorkspaceIncarnation::new(*stem).is_ok())
                .map(str::to_owned)
            else {
                continue;
            };
            let old_metadata = read_metadata(&undo)?;
            let storage = layout(config, &old_metadata.repo_id)?;
            let expected_directory = storage
                .project()
                .checkpoints
                .join(old_metadata.workspace.as_str());
            if storage.project().project_root != project
                || undo.parent() != Some(expected_directory.as_path())
            {
                continue;
            }
            let canonical = storage
                .canonical_image(&old_metadata.workspace)?
                .image()
                .to_owned();
            let Some(_guard) = self.recovery_lock(&canonical, held_locks)? else {
                continue;
            };
            if read_metadata(&undo)? != old_metadata {
                continue;
            }
            let staged = storage.project().project_root.join(STAGING_NAMESPACE).join(format!(
                "{}-{replacement_incarnation}.{IMAGE_EXTENSION}",
                old_metadata.workspace.as_str(),
            ));
            self.recover_restore(
                config,
                &old_metadata,
                &replacement_incarnation,
                &canonical,
                &staged,
                &undo,
                &undo_sidecar,
            )?;
        }
        Ok(())
    }

    #[allow(clippy::too_many_arguments)]
    fn recover_restore(
        &self,
        config: &ApfsSubstrateConfig,
        old_metadata: &DetachedWorkspaceMetadata,
        replacement_incarnation: &str,
        canonical: &Path,
        staged: &Path,
        undo: &Path,
        undo_sidecar: &Path,
    ) -> Result<(), ApfsStorageError> {
        let mismatch = |message: String| Err(ApfsStorageError::MarkerMismatch(message));
        let recovery_fact_path = restore_recovery_fact_path(canonical);
        if !path_exists(canonical)? {
            if !path_exists(staged)? && !path_exists(&recovery_fact_path)? {
                return Ok(());
            }
            return mismatch(format!(
                "restore recovery has no canonical payload: canonical={}, staged={}, fact={}, undo_sidecar={}",
                canonical.display(),
                staged.display(),
                recovery_fact_path.display(),
                undo_sidecar.display()
            ));
        }
        let canonical_sidecar = sidecar_path(canonical);
        if path_exists(&recovery_fact_path)? {
            let fact: RestoreRecoveryFactWire =
                crate::metadata::read_json(&recovery_fact_path).map_err(host_error)?;
            if fact.destination_incarnation.as_str() != replacement_incarnation {
                return Ok(());
            }
            if fact.version != RESTORE_RECOVERY_FACT_VERSION
                || fact.repo_id != old_metadata.repo_id
                || fact.workspace != old_metadata.workspace
                || fact.replaced_incarnation != old_metadata.workspace_incarnation
                || fact.replaced_incarnation == fact.destination_incarnation
                || fact.source_incarnation == fact.destination_incarnation
            {
                return mismatch(format!(
                    "restore recovery fact disagrees with selected undo generation: fact={}, undo_sidecar={}",
                    recovery_fact_path.display(),
                    undo_sidecar.display()
                ));
            }
            validate_restore_recovery_lineage(config, &fact)?;
        } else if !path_exists(staged)? && path_exists(&canonical_sidecar)? {
            if read_metadata(canonical)?.workspace_incarnation != old_metadata.workspace_incarnation
            {
                return Ok(());
            }
        }
        let canonical_companion =
            self.recovery_companion(canonical, "restore recovery canonical payload")?;
        let undo_companion = self.recovery_companion(undo, "restore recovery undo metadata")?;
        let staged_companion = companion_path(staged);
        let canonical_metadata = if path_exists(&canonical_sidecar)? {
            Some(read_metadata(canonical)?)
        } else {
            None
        };
        let published = canonical_metadata
            .as_ref()
            .is_some_and(|metadata| metadata.workspace_incarnation.as_str() == replacement_incarnation);
        if published {
            if Self::companions_match(&canonical_companion, &undo_companion)? {
                return mismatch(format!(
                    "published restore pairs replacement payload with old CA key: canonical={}, canonical_companion={}, undo_companion={}",
                    canonical.display(),
                    canonical_companion.display(),
                    undo_companion.display()
                ));
            }
            if path_exists(undo)? {
                if path_exists(staged)? {
                    self.delete_payload(staged)?;
                }
            } else if path_exists(staged)? {
                let displaced = self.recovery_companion(staged, "restore displaced staged payload")?;
                if !Self::companions_match(&displaced, &undo_companion)? {
                    return mismatch(format!(
                        "displaced restore payload has the wrong CA key: staged={}, staged_companion={}, undo_companion={}",
                        staged.display(),
                        displaced.display(),
                        undo_companion.display()
                    ));
                }
                fs::rename(staged, undo)
                    .map_err(|error| io_error("complete restore undo rename", undo, error))?;
            } else {
                return mismatch(format!(
                    "published restore is missing its old payload: canonical={}, undo={}, staged={}",
                    canonical.display(),
                    undo.display(),
                    staged.display()
                ));
            }
            Self::remove_sidecar(staged)?;
            Self::remove_companion(staged)?;
            return sync_parent_path(canonical);
        }
        if canonical_metadata.as_ref().is_some_and(|metadata| {
            metadata.workspace_incarnation != old_metadata.workspace_incarnation
        }) {
            return mismatch(format!(
                "restore metadata matches neither generation: canonical={}, undo_sidecar={}, staged={}",
                canonical.display(),
                undo_sidecar.display(),
                staged.display()
            ));
        }
        let incarnation = Self::payload_incarnation(canonical)?;
        let canonical_is_replacement = if incarnation == replacement_incarnation {
            true
        } else if incarnation == old_metadata.workspace_incarnation.as_str() {
            false
        } else {
            return mismatch(format!(
                "restore marker matches neither generation: canonical={}, marker={incarnation}, old={}, replacement={replacement_incarnation}",
                canonical.display(),
                old_metadata.workspace_incarnation,
            ));
        };
        let canonical_has_old_key = Self::companions_match(&canonical_companion, &undo_companion)?;
        let link_old_key = |label: &'static str| -> Result<(), ApfsStorageError> {
            if !path_exists(&staged_companion)? {
                fs::hard_link(&undo_companion, &staged_companion).map_err(|error| {
                    io_error("stage retained old CA key", &staged_companion, error)
                })?;
            }
            let staged_key = self.recovery_companion(staged, label)?;
            if Self::companions_match(&staged_key, &undo_companion)? {
                Ok(())
            } else {
                Err(ApfsStorageError::MarkerMismatch(format!(
                    "restore cannot identify old CA key: canonical_companion={}, staged_companion={}, undo_companion={}",
                    canonical_companion.display(),
                    staged_key.display(),
                    undo_companion.display()
                )))
            }
        };
        if canonical_is_replacement {
            if path_exists(undo)? {
                if path_exists(staged)? {
                    return mismatch(format!(
                        "restore has both staged and undo old payloads: staged={}, undo={}",
                        staged.display(),
                        undo.display()
                    ));
                }
                fs::rename(undo, staged)
                    .map_err(|error| io_error("stage interrupted restore undo", staged, error))?;
            } else if !path_exists(staged)? {
                return mismatch(format!(
                    "restore cannot roll back without old payload: canonical={}, staged={}, undo={}",
                    canonical.display(),
                    staged.display(),
                    undo.display()
                ));
            }
            if canonical_has_old_key {
                let staged_key =
                    self.recovery_companion(staged, "restore replacement staged payload")?;
                if Self::companions_match(&staged_key, &undo_companion)? {
                    return mismatch(format!(
                        "restore lost replacement CA key: canonical_companion={}, staged_companion={}, undo_companion={}",
                        canonical_companion.display(),
                        staged_key.display(),
                        undo_companion.display()
                    ));
                }
                swap_paths(canonical, staged)?;
            } else {
                link_old_key("restore displaced staged payload")?;
                swap_paths(canonical, staged)?;
                swap_paths(&canonical_companion, &staged_companion)?;
            }
        } else {
            if path_exists(undo)? {
                return mismatch(format!(
                    "old canonical restore layout still has undo payload: canonical={}, undo={}",
                    canonical.display(),
                    undo.display()
                ));
            }
            if !canonical_has_old_key {
                link_old_key("restore old staged CA key")?;
                swap_paths(&canonical_companion, &staged_companion)?;
            } else if path_exists(staged)? {
                let staged_key =
                    self.recovery_companion(staged, "restore replacement staged payload")?;
                if Self::companions_match(&staged_key, &undo_companion)? {
                    return mismatch(format!(
                        "old and replacement payloads share old CA key: canonical={}, staged={}, undo_companion={}",
                        canonical.display(),
                        staged.display(),
                        undo_companion.display()
                    ));
                }
            }
        }
        if path_exists(staged)? {
            self.delete_payload(staged)?;
        }
        Self::remove_sidecar(staged)?;
        Self::remove_companion(staged)?;
        if path_exists(&canonical_sidecar)? {
            fs::remove_file(undo_sidecar).map_err(|error| {
                io_error("remove redundant restore metadata", undo_sidecar, error)
            })?;
        } else {
            fs::rename(undo_sidecar, &canonical_sidecar).map_err(|error| {
                io_error(
                    "restore interrupted canonical metadata",
                    &canonical_sidecar,
                    error,
                )
            })?;
        }
        fs::remove_file(&undo_companion).map_err(|error| {
            io_error("remove redundant restore CA key", &undo_companion, error)
        })?;
        sync_parent_path(canonical)
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
        self.reclaim_retired_authority(&project, retired.workspace().repo(), &trash, Some(retired))
    }

    pub(super) fn reclaim_unrecorded_retired_payload(
        &self,
        config: &ApfsSubstrateConfig,
        repo: &RepoId,
        image: &Path,
    ) -> Result<(), ApfsStorageError> {
        if config.store_root != self.config.store_root {
            return Err(ApfsStorageError::InvalidPlan(
                "retired cleanup config differs from host storage root",
            ));
        }
        let project = layout(config, repo)?.project().project_root.clone();
        if image.parent()
            != Some(
                project
                    .join(SESSIONS_DIRECTORY)
                    .join(TRASH_NAMESPACE)
                    .as_path(),
            )
        {
            return Err(ApfsStorageError::InvalidPlan(
                "unrecorded retired payload is not in its project's trash",
            ));
        }
        if !path_exists(image)? {
            return Ok(());
        }
        self.reclaim_retired_authority(&project, repo, image, None)
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

    /// The checkpoint payloads and records of a retired name, every one accounted for: an
    /// artifact the retirement cannot name refuses the reclaim rather than being deleted.
    fn retired_checkpoint_artifacts(
        project: &Path,
        retired: &RetiredRef,
    ) -> Result<(Vec<PathBuf>, Vec<PathBuf>), ApfsStorageError> {
        let directory = project
            .join(CHECKPOINTS_DIRECTORY)
            .join(retired.workspace().name().as_str());
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries
                .map(|entry| {
                    entry.map(|entry| entry.path()).map_err(|error| {
                        io_error("read retired checkpoint entry", &directory, error)
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Ok((Vec::new(), Vec::new()));
            }
            Err(error) => {
                return Err(io_error("enumerate retired checkpoints", &directory, error));
            }
        };
        let mut images = BTreeSet::new();
        for path in &entries {
            if is_image_path(path) {
                images.insert(path.clone());
            } else if is_grants_sidecar(path) {
                let image = image_from_sidecar(path)?;
                if is_image_path(&image) {
                    images.insert(image);
                }
            }
        }
        let images = images.into_iter().collect::<Vec<_>>();
        let mut expected = BTreeSet::new();
        let mut labels = BTreeSet::new();
        for image in &images {
            let metadata = read_metadata(image)?;
            if metadata.repo_id != *retired.workspace().repo()
                || metadata.workspace != *retired.workspace().name()
                || metadata.publication_state != PublicationState::Active
            {
                return Err(ApfsStorageError::MarkerMismatch(format!(
                    "retired checkpoint metadata disagrees with {}",
                    image.display()
                )));
            }
            let stem = Self::checkpoint_label(image)?;
            if let Some(destination) = stem.strip_prefix(PRE_RESTORE_PREFIX) {
                WorkspaceIncarnation::new(destination).map_err(host_error)?;
            } else {
                let label = CheckpointLabel::new(stem).map_err(host_error)?;
                if !labels.insert(label.clone()) {
                    return Err(ApfsStorageError::Host(format!(
                        "duplicate retired checkpoint label {label}"
                    )));
                }
                let fact_path = checkpoint_fact_path(image);
                match crate::metadata::read_json::<CheckpointFactWire>(&fact_path) {
                    Ok(fact)
                        if fact.version == CHECKPOINT_FACT_VERSION
                            && fact.repo_id == *retired.workspace().repo()
                            && fact.workspace == *retired.workspace().name()
                            && fact.label == label => {}
                    Ok(_) => {
                        return Err(ApfsStorageError::Host(format!(
                            "checkpoint fact does not match retired payload path: {}",
                            fact_path.display()
                        )));
                    }
                    Err(_) if !path_exists(image)? && !path_exists(&fact_path)? => {}
                    Err(error) => return Err(host_error(error)),
                }
            }
            expected.extend(payload_gc_paths(image));
        }
        if let Some(unrecognized) = entries.iter().find(|path| !expected.contains(*path)) {
            return Err(ApfsStorageError::Host(format!(
                "unrecognized retired checkpoint artifact: {}",
                unrecognized.display()
            )));
        }
        Ok((images, expected.into_iter().collect()))
    }

    /// The mountpoint a retirement may remove: an empty directory, or a binding of a payload
    /// that no longer exists. Anything a late writer left there is named, never deleted.
    fn retired_mount_point(&self, retired: &RetiredRef) -> Result<Option<PathBuf>, ApfsStorageError> {
        let mount_point = layout(&self.config, retired.workspace().repo())?
            .project()
            .mount_root
            .join(retired.workspace().name().as_str());
        match mount_point_state(&mount_point)? {
            MountPointState::Absent => Ok(None),
            MountPointState::Vacant => Ok(Some(mount_point)),
            MountPointState::Bound(target) if !path_exists(&target)? => Ok(Some(mount_point)),
            MountPointState::Bound(target) => Err(ApfsStorageError::Host(format!(
                "retired mountpoint {} still binds {}",
                mount_point.display(),
                target.display()
            ))),
            MountPointState::Occupied => Err(ApfsStorageError::Host(format!(
                "retired mountpoint {} holds files written after its workspace was retired; move or delete them",
                mount_point.display()
            ))),
        }
    }

    fn retired_cleanup(
        &self,
        project: &Path,
        retired: &RetiredRef,
        trash_image: &Path,
        scope: NameScope,
    ) -> Result<RetiredCleanup, ApfsStorageError> {
        let mut cleanup = RetiredCleanup {
            checkpoint_images: Vec::new(),
            paths: Vec::new(),
            mount_point: None,
        };
        if scope == NameScope::Owned {
            let (images, paths) = Self::retired_checkpoint_artifacts(project, retired)?;
            cleanup.checkpoint_images = images;
            cleanup.paths = paths;
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
    ) -> Result<(), ApfsStorageError> {
        let authority = match (self.retired_authority(project, repo, trash_image)?, expected) {
            (Some(authority), Some(expected)) if expected.workspace() != authority.workspace() => {
                return Err(ApfsStorageError::MarkerMismatch(format!(
                    "retired cleanup authority changed for {}",
                    trash_image.display()
                )));
            }
            (Some(authority), _) => authority,
            (None, Some(expected)) => expected.clone(),
            (None, None) => {
                eprintln!(
                    "cowshed: retired payload {} has no grants sidecar; its retirement record was already reclaimed, so its remaining bytes are reclaimed too",
                    trash_image.display()
                );
                return self.reclaim_image(trash_image);
            }
        };
        let trash = project.join(SESSIONS_DIRECTORY).join(TRASH_NAMESPACE);
        let owners = retired_name_owners(&Self::retired_trash_images(&trash)?);
        let scope = self.retired_name_scope(&authority, owners.get(authority.workspace().name()))?;
        let cleanup = self.retired_cleanup(project, &authority, trash_image, scope)?;
        for image in &cleanup.checkpoint_images {
            self.reclaim_image(image)?;
        }
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
            let checkpoint_directory = project.join(CHECKPOINTS_DIRECTORY).join(name);
            match fs::remove_dir(&checkpoint_directory) {
                Ok(()) => {
                    log(&checkpoint_directory);
                    sync_parent_path(&checkpoint_directory)?;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(io_error(
                        "remove retired checkpoint directory",
                        &checkpoint_directory,
                        error,
                    ));
                }
            }
            let exec_temp = project.join(EXEC_TEMP_DIRECTORY).join(name);
            match fs::symlink_metadata(&exec_temp) {
                Ok(metadata) => {
                    let removed = if metadata.is_dir() {
                        if let Some(mounted) = tree_crosses_device(&exec_temp).map_err(|error| {
                            io_error("inspect retired exec temp dir", &exec_temp, error)
                        })? {
                            return Err(ApfsStorageError::Host(format!(
                                "retired exec temp dir {} carries a mounted filesystem at {}; detach it, then `cowshed gc` finishes the retirement",
                                exec_temp.display(),
                                mounted.display()
                            )));
                        }
                        cowshed_os::fsio::remove_owned_tree(&exec_temp)
                    } else {
                        fs::remove_file(&exec_temp)
                    };
                    removed.map_err(|error| {
                        io_error("remove retired exec temp dir", &exec_temp, error)
                    })?;
                    log(&exec_temp);
                    sync_parent_path(&exec_temp)?;
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(io_error("inspect retired exec temp dir", &exec_temp, error));
                }
            }
        }
        if let Some(mount_point) = cleanup.mount_point {
            let removed = match mount_point_state(&mount_point)? {
                MountPointState::Vacant => fs::remove_dir(&mount_point),
                MountPointState::Bound(target) if !path_exists(&target)? => {
                    fs::remove_file(&mount_point)
                }
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
    ) -> Result<StorageGcPlan, ApfsStorageError> {
        let mut candidates = Vec::new();
        let mut lock_paths = Vec::new();
        let mut examined = 0_usize;
        let mut retained = StorageGcRetained::default();

        // Retired workspaces in the trash.
        let sessions = project.join(SESSIONS_DIRECTORY);
        let trash_images = Self::retired_trash_images(&sessions.join(TRASH_NAMESPACE))?;
        let owners = retired_name_owners(&trash_images);
        for path in trash_images {
            let Some(retired) = self.retired_authority(project, repo, &path)? else {
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
            let cleanup = self.retired_cleanup(project, &retired, &path, scope)?;
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

        // Session payloads with no grants sidecar: never published, nothing names them.
        for discovered in crate::storage::discover_session_images(directory_children(&sessions)?) {
            if !grants_sidecar_missing(discovered.path())? {
                continue;
            }
            let lock = workspace_lock_path(&self.config, repo, discovered.workspace())?;
            if !self.lock_is_free(&lock, held_locks)? {
                checked_increment(&mut retained.active)?;
                continue;
            }
            let mount_point = crate::storage::apfs::main_aware_mount_point(
                &self.config,
                repo,
                discovered.workspace(),
            )?;
            if matches!(mount_point_state(&mount_point)?, MountPointState::Bound(target) if target == discovered.path())
            {
                warn_session_image_issue(
                    discovered.path(),
                    "payload without a grants sidecar is still bound; it is left in place",
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

        // Staging payloads and records no operation, restore or recovery still names.
        let storage = layout(&self.config, repo)?;
        let staging = project.join(STAGING_NAMESPACE);
        let recoverable = Self::recoverable_staged_stems(project)?;
        let staged_entries = directory_children(&staging)?
            .into_iter()
            .chain(regular_file_children(&staging)?);
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
            if let Some((_, incarnation)) = stem.rsplit_once('-') {
                let undo = storage
                    .project()
                    .checkpoints
                    .join(workspace.as_str())
                    .join(format!("{PRE_RESTORE_PREFIX}{incarnation}.{IMAGE_EXTENSION}"));
                if path_exists(&sidecar_path(&undo))? {
                    continue;
                }
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

        // Staging mountpoints whose payload is gone: vacant directories and dangling bindings.
        let staging_mounts = storage.project().mount_root.join(STAGING_NAMESPACE);
        for mount_point in read_entries(&staging_mounts)? {
            let Some(stem) = mount_point.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            let Some(workspace) = staged_stem_workspace(stem) else {
                continue;
            };
            match mount_point_state(&mount_point)? {
                MountPointState::Vacant => {}
                MountPointState::Bound(target) if !path_exists(&target)? => {}
                _ => continue,
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

        // Checkpoints: pinned and recent ones stay; the rest expire. A checkpoint's age is its
        // fact's, written once when it was published: a payload directory's own time is its
        // source's, which the clone carried over.
        for workspace_directory in directory_children(&storage.project().checkpoints)? {
            let Some(workspace_name) = workspace_directory
                .file_name()
                .and_then(|name| name.to_str())
                .and_then(|name| WorkspaceName::new(name).ok())
            else {
                continue;
            };
            let mut checkpoints = Vec::new();
            for image in Self::checkpoint_payloads(&workspace_directory)? {
                lock_paths.push(workspace_lock_path(&self.config, repo, &workspace_name)?);
                let fact_path = checkpoint_fact_path(&image);
                let fact_metadata = match fs::metadata(&fact_path) {
                    Ok(metadata) => metadata,
                    Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
                    Err(error) => return Err(io_error("read checkpoint age", &fact_path, error)),
                };
                let fact: CheckpointFactWire =
                    crate::metadata::read_json(&fact_path).map_err(host_error)?;
                if fact.version != CHECKPOINT_FACT_VERSION
                    || fact.repo_id != *repo
                    || fact.workspace != workspace_name
                    || fact.label.as_str() != Self::checkpoint_label(&image)?
                {
                    return Err(ApfsStorageError::Host(format!(
                        "checkpoint fact does not match payload path: {}",
                        fact_path.display()
                    )));
                }
                let modified = fact_metadata
                    .modified()
                    .map_err(|error| io_error("read checkpoint age", &fact_path, error))?;
                checkpoints.push((modified, image, fact));
            }
            checkpoints
                .sort_by(|left, right| right.0.cmp(&left.0).then_with(|| right.1.cmp(&left.1)));
            for (index, (modified, image, fact)) in checkpoints.into_iter().enumerate() {
                checked_increment(&mut examined)?;
                if fact.pin == CheckpointPinWire::Pinned {
                    checked_increment(&mut retained.pinned)?;
                    continue;
                }
                let young = observed_at.duration_since(modified).map_or(true, |age| {
                    age < std::time::Duration::from_secs(14 * 24 * 60 * 60)
                });
                if index < 5 || young {
                    checked_increment(&mut retained.recent)?;
                    continue;
                }
                let identity = serde_json::to_vec(&fact).map_err(host_error)?;
                candidates.push(gc_candidate(
                    StorageGcReason::ExpiredCheckpoint,
                    &image,
                    &payload_gc_paths(&image),
                    &identity,
                )?);
            }
        }

        candidates.sort_by(|left, right| {
            left.path()
                .cmp(right.path())
                .then_with(|| format!("{:?}", left.reason()).cmp(&format!("{:?}", right.reason())))
        });
        lock_paths.sort();
        lock_paths.dedup();
        Ok(StorageGcPlan::new(
            repo.clone(),
            observed_at,
            candidates,
            lock_paths,
            examined,
            retained,
            Vec::new(),
        ))
    }

    pub(super) fn execute_gc_plan(
        &self,
        project: &Path,
        plan: StorageGcPlan,
    ) -> Result<StorageGcReport, ApfsStorageError> {
        let _guard = acquire_image_locks(&self.config.store_root, plan.lock_paths(), LockMode::Try)?
            .ok_or(ApfsStorageError::GcPlanStale)?;
        let current =
            self.preview_gc_project(project, plan.repo(), plan.observed_at(), plan.lock_paths())?;
        if current != plan {
            return Err(ApfsStorageError::GcPlanStale);
        }
        let mut report = StorageGcReport {
            examined: plan.examined(),
            retained_pinned: plan.retained_pinned(),
            retained_recent: plan.retained_recent(),
            retained_active: plan.retained_active(),
            ..StorageGcReport::default()
        };
        for candidate in plan.candidates() {
            let path = candidate.path();
            let result = match candidate.reason() {
                StorageGcReason::RetiredWorkspace => {
                    self.reclaim_retired_authority(project, plan.repo(), path, None)
                }
                StorageGcReason::OrphanSessionImage
                | StorageGcReason::OrphanStagingImage
                | StorageGcReason::ExpiredCheckpoint => self.reclaim_image(path),
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
                        MountPointState::Bound(_) => fs::remove_file(path),
                        MountPointState::Absent => Ok(()),
                        MountPointState::Occupied => Err(io::Error::other(
                            "staging mountpoint is no longer vacant",
                        )),
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
                    report.freed_bytes = report
                        .freed_bytes
                        .checked_add(candidate.bytes())
                        .ok_or(ApfsStorageError::InvalidPlan(
                            "GC freed byte accounting overflow",
                        ))?;
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
