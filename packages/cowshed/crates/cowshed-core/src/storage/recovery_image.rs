//! `cowshed gc --recover <image> --to <dir>`: take one recovery image out of the store
//! (02_workspaces.md, `rm` step 5, "Recovering a recovery image"; 01_storage.md, "Retention").
//!
//! A removal that cannot prove every source byte held elsewhere keeps the complete image as
//! `sessions/.trash/<ws>-<incarnation>.dmg` with its companions (02, "Recovery source"). Nothing in
//! the store deletes one, and terminal project removal refuses while one remains, so its contents
//! leave the store only deliberately, through this transfer. Same-filesystem transfers rename
//! companions before the image. Cross-filesystem transfers sparsely copy regular files; the
//! existing source-side lifecycle journal alone owns progress and verified proofs. Destination
//! documents are immutable reports, never retirement authority. After complete verification,
//! sources move into journal-bound private custody and only validated staged inodes are freed.

use std::ffi::{CString, OsStr, OsString};
use std::fs;
use std::io::{self, Write as _};
use std::os::fd::{AsRawFd as _, FromRawFd as _};
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::api::dto::{RecoveredImage, RecoveryRetention};
use crate::copy::{CopyFileIdentity, copy_held_file, verify_held_file};
use crate::error::{CowshedError, Result};
use crate::metadata::{
    DetachedWorkspaceMetadata, WorkspaceIncarnation, WorkspaceName, sidecar_path,
};
use crate::repository::RepoId;
use crate::storage::StorageLayout;
use crate::storage::apfs::native::{checkpoint_fact_path, restore_recovery_fact_path};
use crate::storage::apfs::{companion_path, split_retired_stem};
use crate::storage::deletion_log::{self, DeletionKind};
use crate::storage::recovery::{ABANDON_BUNDLE_EXTENSION, TRASH_NAMESPACE};
use cowshed_apfs::image::IMAGE_EXTENSION;
use cowshed_apfs::store_lock::{LockMode, acquire_image_locks};
/// Backend-owned ephemeral cleanup under recovery's held workspace lifecycle lock.
pub trait RecoveryEnvironment {
    fn reclaim_recovery_environment(
        &self,
        repo: &RepoId,
        image: &Path,
    ) -> std::result::Result<(), crate::storage::apfs::ApfsStorageError>;
}

/// The manifest's name in the directory a recovery image moves to.
pub const MANIFEST_FILE: &str = "recovery.json";
/// Immutable complete-destination evidence; source retirement authority stays in the store.
pub const COPY_PROOF_FILE: &str = "recovery-copy.json";

pub(crate) mod custody;
use custody::RecoveryExport;
const MANIFEST_VERSION: u32 = 1;
const COPY_MANIFEST_VERSION: u32 = 2;

/// The admitted destination namespace, shared by planning, publication and its manifest.
struct RecoveryDestination<'a> {
    parent: &'a Path,
    directory: &'a Path,
    manifest: &'a Path,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct DirectoryIdentity {
    device: u64,
    inode: u64,
}

impl DirectoryIdentity {
    fn read(directory: &fs::File) -> io::Result<Self> {
        let metadata = directory.metadata()?;
        Ok(Self {
            device: metadata.dev(),
            inode: metadata.ino(),
        })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CopyTransfer {
    source_directory: DirectoryIdentity,
    destination_directory: DirectoryIdentity,
    artifacts: Vec<CopyArtifact>,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CopyArtifact {
    source: CopyFileIdentity,
    state: CopyState,
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(tag = "phase", rename_all = "camelCase", deny_unknown_fields)]
enum CopyState {
    Pending,
    Copying {
        stage: String,
        target: CopyFileIdentity,
    },
    Copied {
        stage: String,
        target: CopyFileIdentity,
    },
    Published {
        target: CopyFileIdentity,
        digest: crate::api::Sha256Digest,
    },
}

/// Transaction boundaries observed by the ordinary-file behavioral controls.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum CopyCheckpoint {
    ManifestDurable,
    CopyCreated,
    Staged,
    Published,
    BeforeRetire,
    BeforeSourceRetire,
    RetirementStaged,
    Retired,
}

/// Why a path is not a recovery image of this project.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NotRecoveryImage {
    /// Not an entry directly in this project's trash, `trash`.
    OutsideTrash { trash: PathBuf },
    /// One of the trash's `rm --abandon` bundles: the only copy of its commits, which
    /// `gc --abandoned` deletes.
    AbandonBundle,
    /// In the trash, but not named `<workspace>-<incarnation>.dmg` as a retirement names one.
    Unnamed,
}

impl NotRecoveryImage {
    fn refusal(self, image: &Path) -> CowshedError {
        match self {
            Self::OutsideTrash { trash } => CowshedError::usage(
                format!(
                    "{} is not one of this project's recovery images, the {}/<workspace>-<incarnation>.{IMAGE_EXTENSION} entries",
                    image.display(),
                    trash.display()
                ),
                "name a recovery image in the trash of the project the cwd or --project selects",
            ),
            Self::AbandonBundle => CowshedError::usage(
                format!(
                    "{} is an rm --abandon bundle, not a recovery image",
                    image.display()
                ),
                "the bundle is the only copy of its commits: cowshed gc --abandoned deletes it",
            ),
            Self::Unnamed => CowshedError::usage(
                format!(
                    "{} is not named <workspace>-<incarnation>.{IMAGE_EXTENSION}, as every recovery image is",
                    image.display()
                ),
                "name a recovery image in the project's sessions/.trash",
            ),
        }
    }
}

/// The workspace and incarnation `image` names when it is an entry of `trash` named the way a
/// retirement names the image it keeps. Lexical: the caller resolves both paths first.
pub fn classify(
    trash: &Path,
    image: &Path,
) -> std::result::Result<(WorkspaceName, WorkspaceIncarnation), NotRecoveryImage> {
    if image.parent() != Some(trash) {
        return Err(NotRecoveryImage::OutsideTrash {
            trash: trash.to_owned(),
        });
    }
    if image.extension() == Some(OsStr::new(ABANDON_BUNDLE_EXTENSION)) {
        return Err(NotRecoveryImage::AbandonBundle);
    }
    let suffix = format!(".{IMAGE_EXTENSION}");
    image
        .file_name()
        .and_then(OsStr::to_str)
        .and_then(|name| name.strip_suffix(suffix.as_str()))
        .and_then(split_retired_stem)
        .ok_or(NotRecoveryImage::Unnamed)
}

/// What a recovered directory holds, written into it before anything moves.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct Manifest {
    version: u32,
    repo_id: RepoId,
    workspace: WorkspaceName,
    incarnation: WorkspaceIncarnation,
    /// The trash path the image moved from.
    source: PathBuf,
    /// Every file that moves, by name: the companions, then the image.
    files: Vec<String>,
    /// Whether the image still had its grants sidecar when its recovery began.
    grants: bool,
    retained: RecoveryRetention,
    recovered_at: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    copy: Option<CopyTransfer>,
}

impl Manifest {
    fn describes(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
        incarnation: &WorkspaceIncarnation,
        source: &Path,
    ) -> bool {
        ((self.version == MANIFEST_VERSION && self.copy.is_none())
            || (self.version == COPY_MANIFEST_VERSION
                && self.copy.as_ref().is_some_and(|copy| {
                    copy.artifacts.len() == self.files.len()
                        && copy
                            .artifacts
                            .iter()
                            .zip(&self.files)
                            .all(|(artifact, name)| match &artifact.state {
                                CopyState::Copying { stage, .. }
                                | CopyState::Copied { stage, .. } => valid_stage(name, stage),
                                CopyState::Pending | CopyState::Published { .. } => true,
                            })
                })))
            && self.repo_id == *repo
            && self.workspace == *workspace
            && self.incarnation == *incarnation
            && self.source == source
            && in_moving_order(&self.files, source)
    }
}

/// Every file a recovery image's move can carry, in the order it moves: its companions, then the
/// image itself. Derived from the image's own path, never read back from a manifest.
fn movable(image: &Path) -> [PathBuf; 5] {
    [
        sidecar_path(image),
        companion_path(image),
        checkpoint_fact_path(image),
        restore_recovery_fact_path(image),
        image.to_owned(),
    ]
}

/// Whether `files` names only files [`movable`] derives for `image`, each once and in its order,
/// ending with the image: exactly what a manifest this module wrote can list. A retry acts only on
/// such a list, so no name it reads back can reach outside the trash or the recovered directory.
fn in_moving_order(files: &[String], image: &Path) -> bool {
    let names: Vec<OsString> = movable(image)
        .into_iter()
        .filter_map(|path| path.file_name().map(OsStr::to_owned))
        .collect();
    let mut candidates = names.iter();
    files.last().map(OsStr::new) == names.last().map(OsString::as_os_str)
        && files
            .iter()
            .all(|file| candidates.any(|name| name.as_os_str() == OsStr::new(file)))
}

/// Transfer one recovery image outside the store while holding its lifecycle lock.
/// Rename on the same filesystem; on another filesystem accept only regular artifacts and
/// retain every source until the complete destination is durable and verified.
pub fn recover(
    layout: &StorageLayout,
    repo: &RepoId,
    image: &Path,
    to: &Path,
    owner: &impl RecoveryEnvironment,
) -> Result<RecoveredImage> {
    recover_with(layout, repo, image, to, owner, false, &mut |_, _| Ok(()))
}

fn recover_with(
    layout: &StorageLayout,
    repo: &RepoId,
    image: &Path,
    to: &Path,
    owner: &impl RecoveryEnvironment,
    force_copy: bool,
    observe: &mut impl FnMut(CopyCheckpoint, &Path) -> Result<()>,
) -> Result<RecoveredImage> {
    for (role, path) in [("recovery image", image), ("--to directory", to)] {
        if !path.is_absolute() {
            return Err(CowshedError::usage(
                format!("the {role} {} is not an absolute path", path.display()),
                "name it from the filesystem root",
            ));
        }
    }
    let project = layout.project();
    let trash = match fs::canonicalize(project.sessions.join(TRASH_NAMESPACE)) {
        Ok(trash) => trash,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(CowshedError::not_found(
                format!(
                    "{} has no trash, so no recovery image",
                    project.sessions.display()
                ),
                "cowshed gc --dry-run names what the project's store holds",
            ));
        }
        Err(error) => return Err(io_failure("resolve", &project.sessions, &error)),
    };
    let image = resolved_entry(image)?;
    let (workspace, incarnation) =
        classify(&trash, &image).map_err(|refusal| refusal.refusal(&image))?;
    // The workspace name's lifecycle lock, which gc's collection of a retirement and every
    // lifecycle operation on the name take: held for the whole move, so no other recovery,
    // collection or retirement of this name runs inside it.
    let paths = if workspace.is_main() {
        layout.main_image()
    } else {
        layout.session_image(&workspace)
    }
    .map_err(|error| CowshedError::internal(format!("lay out {workspace}'s lock: {error}")))?;
    let lock = paths.lock().to_owned();
    let _custody = acquire_image_locks(
        &project.store_root,
        std::slice::from_ref(&lock),
        LockMode::Try,
        &crate::held_lock::ImageLockTaker {
            holding: &crate::api::held_lock::LockHolding::of(
                crate::api::held_lock::LockOperation::Recover,
                &workspace,
            ),
        },
    )
    .map_err(|error| {
        CowshedError::environment_missing(
            format!("cannot take {}: {error}", lock.display()),
            "check the store's permissions, then retry",
        )
    })?
    .ok_or_else(|| {
        CowshedError::conflict(
            format!("another operation on {workspace} holds {}", lock.display()),
            "retry once it has finished",
        )
    })?;
    let to = destination(&project.store_root, to)?;
    let (trash_device, to_device) = (device(&trash)?, device(&to)?);
    let cross_device = force_copy || trash_device != to_device;
    let held_trash = open_directory(&trash)?;
    let directory = to.join(format!("{}-{}", workspace.as_str(), incarnation.as_str()));
    let manifest_path = directory.join(MANIFEST_FILE);
    let destination = RecoveryDestination {
        parent: &to,
        directory: &directory,
        manifest: &manifest_path,
    };
    let journal_path = custody::journal_path(&project.project_root);
    let mut export = custody::load_export(&journal_path, &image)?;
    let manifest = if let Some(record) = &export {
        if record.directory != directory
            || !record
                .admission
                .describes(repo, &workspace, &incarnation, &image)
        {
            return Err(transfer_failure(
                "admit source-held recovery",
                &image,
                &directory,
                "the source journal admits a different destination or image identity",
            ));
        }
        record.admission.clone()
    } else {
        match fs::symlink_metadata(&directory) {
            Err(error) if error.kind() == io::ErrorKind::NotFound => plan(
                project.project_root.as_path(),
                repo,
                (&workspace, &incarnation),
                &image,
                &destination,
                cross_device,
                &held_trash,
            )?,
            Ok(metadata) if metadata.file_type().is_dir() => match read_manifest(&manifest_path)? {
                Some(manifest)
                    if manifest.copy.is_none()
                        && manifest.describes(repo, &workspace, &incarnation, &image) =>
                {
                    manifest
                }
                Some(_) | None => return Err(occupied(&directory)),
            },
            Ok(_) => return Err(occupied(&directory)),
            Err(error) => return Err(io_failure("inspect", &directory, &error)),
        }
    };
    // Both directories are held by descriptor from here on: every rename is relative to them, so
    // a link planted at either path afterwards cannot redirect a move.
    let held_directory = open_directory(&directory)?;
    if manifest.copy.is_some() {
        if export.is_none() {
            export = custody::load_export(&journal_path, &image)?;
        }
        let record = export.as_mut().ok_or_else(|| {
            transfer_failure(
                "admit copy authority",
                &image,
                &directory,
                "no source-held recovery record",
            )
        })?;
        custody::ensure_admission(record, &held_directory, &journal_path)?;
    } else if read_manifest_at(&held_directory, &manifest_path)?.as_ref() != Some(&manifest) {
        return Err(occupied(&directory));
    }
    if cross_device && manifest.copy.is_none() {
        return Err(CowshedError::integrity(
            "a rename manifest cannot authorize a cross-filesystem copy",
            "resume this recovery on its original filesystem; preserve both directories",
        ));
    }
    // An ordered list can still be short: every file derived for this image that exists on either
    // side must be one the manifest moves, or the image would leave without its companions.
    for path in movable(&image) {
        let name = file_name(&path)?;
        let entry = held_name(&name, &manifest_path)?;
        let exists = present_in(&held_trash, &entry, &trash.join(&name))?
            || present_in(&held_directory, &entry, &directory.join(&name))?;
        if exists && !manifest.files.contains(&name) {
            return Err(CowshedError::integrity(
                format!(
                    "{} omits {name}, which exists in {} or {}",
                    manifest_path.display(),
                    trash.display(),
                    directory.display()
                ),
                "move the destination directory aside, then retry: the recovery moved nothing",
            ));
        }
    }
    if manifest.grants != manifest.files.contains(&file_name(&sidecar_path(&image))?) {
        return Err(CowshedError::integrity(
            format!(
                "{} disagrees with itself about the grants sidecar",
                manifest_path.display()
            ),
            "move the destination directory aside, then retry: the recovery moved nothing",
        ));
    }
    if let Some(record) = &export {
        custody::admit_export(record, &held_trash, &held_directory, &trash)?;
    } else {
        admit_transfer(
            &manifest,
            &held_trash,
            &held_directory,
            &trash,
            &directory,
            &manifest_path,
        )?;
    }
    owner
        .reclaim_recovery_environment(repo, &image)
        .map_err(|error| {
            transfer_failure(
                "reclaim scoped recovery environment",
                &image,
                &directory,
                error,
            )
        })?;
    if let Some(record) = export.as_mut() {
        copy_recovery(
            record,
            &held_trash,
            &held_directory,
            &trash,
            &destination,
            &project.project_root,
            observe,
        )?;
        return Ok(RecoveredImage {
            workspace,
            incarnation,
            source: image,
            moved: manifest
                .files
                .iter()
                .map(|name| directory.join(name))
                .collect(),
            directory,
            manifest: manifest_path,
            grants: manifest.grants,
            retained: manifest.retained,
        });
    }
    let mut moved = Vec::with_capacity(manifest.files.len());
    for name in &manifest.files {
        let from = trash.join(name);
        let into = directory.join(name);
        let entry = held_name(name, &manifest_path)?;
        match (
            present_in(&held_trash, &entry, &from)?,
            present_in(&held_directory, &entry, &into)?,
        ) {
            (true, false) => {
                cowshed_os::fsio::rename_noreplace(
                    held_trash.as_raw_fd(),
                    &entry,
                    held_directory.as_raw_fd(),
                    &entry,
                )
                .map_err(|error| {
                    if error.kind() == io::ErrorKind::AlreadyExists {
                        both_exist(&from, &into)
                    } else {
                        io_failure("move", &from, &error)
                    }
                })?;
                cowshed_os::fsio::sync_held_directory(&held_directory)
                    .map_err(|error| io_failure("sync", &directory, &error))?;
                cowshed_os::fsio::sync_held_directory(&held_trash)
                    .map_err(|error| io_failure("sync", &trash, &error))?;
                deletion_log::log_recovery(
                    &project.project_root,
                    workspace.as_str(),
                    &image,
                    &from,
                    artifact_kind(&image, &from),
                    &into,
                );
            }
            (false, true) => {}
            (true, true) => return Err(both_exist(&from, &into)),
            (false, false) => {
                return Err(CowshedError::integrity(
                    format!(
                        "{} left the trash during its recovery, and {} does not hold it",
                        from.display(),
                        directory.display()
                    ),
                    "find where it went before retrying; the recovery moved nothing else there",
                ));
            }
        }
        moved.push(into);
    }
    // A resumed entry was renamed by a process that may have died before syncing: both
    // directories are synced again before any answer claims the move.
    cowshed_os::fsio::sync_held_directory(&held_directory)
        .map_err(|error| io_failure("sync", &directory, &error))?;
    cowshed_os::fsio::sync_held_directory(&held_trash)
        .map_err(|error| io_failure("sync", &trash, &error))?;
    Ok(RecoveredImage {
        workspace,
        incarnation,
        source: image,
        directory,
        manifest: manifest_path,
        moved,
        grants: manifest.grants,
        retained: manifest.retained,
    })
}

fn plan(
    project_root: &Path,
    repo: &RepoId,
    (workspace, incarnation): (&WorkspaceName, &WorkspaceIncarnation),
    image: &Path,
    destination: &RecoveryDestination<'_>,
    cross_device: bool,
    held_trash: &fs::File,
) -> Result<Manifest> {
    let to = destination.parent;
    let directory = destination.directory;
    match fs::symlink_metadata(image) {
        Ok(metadata) if !metadata.file_type().is_symlink() => {}
        Ok(_) => {
            return Err(CowshedError::integrity(
                format!("{} is a symlink, not a recovery image", image.display()),
                "inspect it: a retirement never keeps an image as a link",
            ));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Err(CowshedError::not_found(
                format!("no recovery image at {}", image.display()),
                "cowshed doctor names retained recovery images",
            ));
        }
        Err(error) => return Err(io_failure("inspect", image, &error)),
    }
    let sidecar = sidecar_path(image);
    let grants = present(&sidecar)?;
    if grants {
        let metadata = DetachedWorkspaceMetadata::read_for_image(image).map_err(|error| {
            CowshedError::integrity(
                format!("cannot read {}: {error}", sidecar.display()),
                "repair or move aside the grants sidecar, then retry",
            )
        })?;
        if metadata.repo_id != *repo
            || metadata.workspace != *workspace
            || metadata.workspace_incarnation != *incarnation
        {
            return Err(CowshedError::integrity(
                format!(
                    "{} records {} at {} in {}, not {workspace} at {incarnation} in {repo}",
                    sidecar.display(),
                    metadata.workspace,
                    metadata.workspace_incarnation,
                    metadata.repo_id
                ),
                "inspect the trash entry: its name and its record disagree",
            ));
        }
    }
    let mut files = Vec::new();
    let mut artifacts = Vec::new();
    for path in movable(image) {
        let name = file_name(&path)?;
        let entry = held_name(&name, directory)?;
        if present_in(held_trash, &entry, &path)? {
            if cross_device {
                let source = open_regular_at(held_trash, &entry)
                    .and_then(|file| CopyFileIdentity::read(&file))
                    .map_err(|error| {
                        transfer_failure(
                            "admit regular source",
                            &path,
                            &directory.join(&name),
                            error,
                        )
                    })?;
                artifacts.push(CopyArtifact {
                    source,
                    state: CopyState::Pending,
                });
            }
            files.push(name);
        }
    }
    if !in_moving_order(&files, image) {
        return Err(transfer_failure(
            "admit source set",
            image,
            directory,
            "the recovery image disappeared while its companions were inspected",
        ));
    }
    let held_to = open_directory(to)?;
    let leaf = held_name(&file_name(directory)?, directory)?;
    let (held_directory, created) =
        cowshed_os::fsio::open_or_create_child_directory(&held_to, &leaf)
            .map_err(|error| io_failure("create", directory, &error))?;
    if !created {
        return Err(occupied(directory));
    }
    cowshed_os::fsio::sync_held_directory(&held_to)
        .map_err(|error| io_failure("sync", to, &error))?;
    let copy = if cross_device {
        Some(CopyTransfer {
            source_directory: DirectoryIdentity::read(held_trash)
                .map_err(|error| io_failure("identify", image, &error))?,
            destination_directory: DirectoryIdentity::read(&held_directory)
                .map_err(|error| io_failure("identify", directory, &error))?,
            artifacts,
        })
    } else {
        None
    };
    let manifest = Manifest {
        version: if cross_device {
            COPY_MANIFEST_VERSION
        } else {
            MANIFEST_VERSION
        },
        repo_id: repo.clone(),
        workspace: workspace.clone(),
        incarnation: incarnation.clone(),
        source: image.to_owned(),
        files,
        grants,
        retained: deletion_log::recovery_retention(project_root, image),
        recovered_at: deletion_log::rfc3339_utc(SystemTime::now()),
        copy,
    };
    if manifest.copy.is_some() {
        let mut record = RecoveryExport::new(manifest.clone(), directory.to_owned());
        let journal_path = custody::journal_path(project_root);
        custody::save_export(&journal_path, None, &record)?;
        custody::ensure_admission(&mut record, &held_directory, &journal_path)?;
    } else {
        save_manifest(&held_directory, &directory.join(MANIFEST_FILE), &manifest)?;
    }
    Ok(manifest)
}

fn read_manifest(path: &Path) -> Result<Option<Manifest>> {
    let parent = path
        .parent()
        .ok_or_else(|| CowshedError::internal("manifest has no directory"))?;
    read_manifest_at(&open_directory(parent)?, path)
}

/// The path `create_dir_all(to)` makes, resolved through its longest existing prefix, created
/// only once it is known to lie outside the store.
fn destination(store: &Path, to: &Path) -> Result<PathBuf> {
    let store = fs::canonicalize(store).map_err(|error| io_failure("resolve", store, &error))?;
    let mut existing = to;
    let mut missing = Vec::new();
    let resolved = loop {
        match fs::canonicalize(existing) {
            Ok(real) => {
                break missing
                    .iter()
                    .rev()
                    .fold(real, |path, part| path.join(part));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                let (Some(parent), Some(Component::Normal(part))) =
                    (existing.parent(), existing.components().next_back())
                else {
                    return Err(CowshedError::usage(
                        format!(
                            "{} names a directory through a component that does not exist yet and is not a plain name",
                            to.display()
                        ),
                        "name the destination without .. after its last existing directory",
                    ));
                };
                missing.push(part.to_owned());
                existing = parent;
            }
            Err(error) => return Err(io_failure("resolve", existing, &error)),
        }
    };
    if resolved.starts_with(&store) {
        return Err(CowshedError::usage(
            format!(
                "{} is inside the store {}: a recovery image moved there is still the store's",
                resolved.display(),
                store.display()
            ),
            "choose a directory outside the store",
        ));
    }
    fs::create_dir_all(&resolved).map_err(|error| io_failure("create", &resolved, &error))?;
    // Each directory this made is durable in its parent before anything is moved into it.
    for parent in resolved.ancestors().skip(1).take(missing.len()) {
        sync_directory(parent)?;
    }
    Ok(resolved)
}

/// `path` with its directory resolved through symlinks, so it compares with the trash exactly.
fn resolved_entry(path: &Path) -> Result<PathBuf> {
    let (Some(parent), Some(name)) = (path.parent(), path.file_name()) else {
        return Err(CowshedError::usage(
            format!("{} names no file", path.display()),
            "name a recovery image in the project's sessions/.trash",
        ));
    };
    match fs::canonicalize(parent) {
        Ok(parent) => Ok(parent.join(name)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Err(CowshedError::not_found(
            format!("no recovery image at {}", path.display()),
            "cowshed doctor names the recovery images the project's trash keeps",
        )),
        Err(error) => Err(io_failure("resolve", parent, &error)),
    }
}

fn artifact_kind(image: &Path, artifact: &Path) -> DeletionKind {
    if artifact == image {
        DeletionKind::Image
    } else if artifact == sidecar_path(image) {
        DeletionKind::Sidecar
    } else if artifact == companion_path(image) {
        DeletionKind::Companion
    } else {
        DeletionKind::Other
    }
}

/// `name` as the entry a held-directory call takes; `manifest` names where it came from.
fn held_name(name: &str, manifest: &Path) -> Result<CString> {
    CString::new(name.as_bytes()).map_err(|_| {
        CowshedError::integrity(
            format!("{} names a file with a NUL byte", manifest.display()),
            "move the destination directory aside, then retry",
        )
    })
}

fn both_exist(from: &Path, into: &Path) -> CowshedError {
    CowshedError::conflict(
        format!(
            "both {} and {} exist; the recovery cannot tell which to keep",
            from.display(),
            into.display()
        ),
        "compare the two, move the one you do not keep out of both places, then retry",
    )
}

fn open_directory(directory: &Path) -> Result<fs::File> {
    let mut held = fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open("/")
        .map_err(|error| io_failure("open root for", directory, &error))?;
    for component in directory.components() {
        match component {
            Component::RootDir => continue,
            Component::Normal(name) => {
                use std::os::unix::ffi::OsStrExt as _;
                let name = CString::new(name.as_bytes())
                    .map_err(|_| CowshedError::internal("directory contains NUL"))?;
                // SAFETY: the held descriptor and leaf are live; each component refuses links.
                let fd = unsafe {
                    libc::openat(
                        held.as_raw_fd(),
                        name.as_ptr(),
                        libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC,
                    )
                };
                if fd < 0 {
                    return Err(io_failure("open", directory, &io::Error::last_os_error()));
                }
                // SAFETY: openat returned a fresh descriptor owned by this iteration.
                held = unsafe { fs::File::from_raw_fd(fd) };
            }
            _ => {
                return Err(CowshedError::internal(
                    "recovery directory is not canonical",
                ));
            }
        }
    }
    Ok(held)
}

/// Whether `entry` exists in the held `directory`, without following it; `path` names it in
/// diagnostics.
fn present_in(directory: &fs::File, entry: &CString, path: &Path) -> Result<bool> {
    // SAFETY: `directory` is an open descriptor for the call, `entry` is NUL-terminated, and
    // `stat` is a properly sized, writable buffer fstatat fills.
    let mut stat = std::mem::MaybeUninit::<libc::stat>::uninit();
    let result = unsafe {
        libc::fstatat(
            directory.as_raw_fd(),
            entry.as_ptr(),
            stat.as_mut_ptr(),
            libc::AT_SYMLINK_NOFOLLOW,
        )
    };
    if result == 0 {
        return Ok(true);
    }
    let error = io::Error::last_os_error();
    if error.kind() == io::ErrorKind::NotFound {
        Ok(false)
    } else {
        Err(io_failure("inspect", path, &error))
    }
}

fn occupied(directory: &Path) -> CowshedError {
    CowshedError::conflict(
        format!(
            "{} already exists and is not this image's interrupted recovery",
            directory.display()
        ),
        "choose another --to, or move the directory aside",
    )
}

fn present(path: &Path) -> Result<bool> {
    match fs::symlink_metadata(path) {
        Ok(_) => Ok(true),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_failure("inspect", path, &error)),
    }
}

fn device(path: &Path) -> Result<u64> {
    fs::metadata(path)
        .map(|metadata| metadata.dev())
        .map_err(|error| io_failure("inspect", path, &error))
}

fn file_name(path: &Path) -> Result<String> {
    path.file_name()
        .and_then(OsStr::to_str)
        .map(str::to_owned)
        .ok_or_else(|| CowshedError::internal(format!("{} has no UTF-8 name", path.display())))
}

fn sync_directory(path: &Path) -> Result<()> {
    fs::File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_failure("sync", path, &error))
}

fn io_failure(operation: &str, path: &Path, error: &io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("cannot {operation} {}: {error}", path.display()),
        "check the store and destination permissions, then retry: a retry finishes the move",
    )
}

#[cfg(test)]
mod tests {
    use super::{NotRecoveryImage, classify, in_moving_order};
    use std::path::Path;

    const INCARNATION: &str = "0123456789abcdef0123456789abcdef";

    #[test]
    fn a_trash_entry_named_like_a_retirement_is_a_recovery_image() {
        let trash = Path::new("/store/project/sessions/.trash");
        let (workspace, incarnation) = classify(
            trash,
            &trash.join(format!("fleet-worker-27529-{INCARNATION}.dmg")),
        )
        .expect("a retirement's name");
        assert_eq!(workspace.as_str(), "fleet-worker-27529");
        assert_eq!(incarnation.as_str(), INCARNATION);
    }

    #[test]
    fn only_this_projects_trash_entries_are_its_recovery_images() {
        let trash = Path::new("/store/project/sessions/.trash");
        let name = format!("feature-{INCARNATION}.dmg");
        for elsewhere in [
            Path::new("/store/project/sessions").join(&name),
            Path::new("/store/other/sessions/.trash").join(&name),
            trash.join("nested").join(&name),
            Path::new("/home/user").join(&name),
        ] {
            assert_eq!(
                classify(trash, &elsewhere),
                Err(NotRecoveryImage::OutsideTrash {
                    trash: trash.to_owned()
                }),
                "{}",
                elsewhere.display()
            );
        }
    }

    /// A retry acts only on a manifest list this module could have written: the image's own
    /// companions, each once and in moving order, ending with the image. Nothing else it could
    /// read back, a path that climbs or is absolute, a duplicate, a reordering or a list that
    /// leaves the image out, can name a file to move.
    #[test]
    fn a_retry_moves_only_the_names_derived_from_the_image() {
        let image =
            Path::new("/store/project/sessions/.trash").join(format!("feature-{INCARNATION}.dmg"));
        let named = |names: &[&str]| -> Vec<String> {
            names
                .iter()
                .map(|name| name.replace("IMAGE", &format!("feature-{INCARNATION}.dmg")))
                .collect()
        };
        for accepted in [
            &["IMAGE"][..],
            &["IMAGE.grants.json", "IMAGE"],
            &["IMAGE.grants.json", "IMAGE.ca.key", "IMAGE"],
            &["IMAGE.ca.key", "IMAGE"],
        ] {
            assert!(in_moving_order(&named(accepted), &image), "{accepted:?}");
        }
        for refused in [
            &[][..],
            &["IMAGE.grants.json"],
            &["IMAGE", "IMAGE.grants.json"],
            &["IMAGE.ca.key", "IMAGE.grants.json", "IMAGE"],
            &["IMAGE.grants.json", "IMAGE.grants.json", "IMAGE"],
            &["IMAGE", "IMAGE"],
            &["../main.dmg", "IMAGE"],
            &["/store/project/main.dmg", "IMAGE"],
            &["IMAGE.grants.json", "../IMAGE"],
            &["other.dmg"],
        ] {
            assert!(!in_moving_order(&named(refused), &image), "{refused:?}");
        }
    }

    #[test]
    fn bundles_and_misnamed_entries_are_not_recovery_images() {
        let trash = Path::new("/store/project/sessions/.trash");
        assert_eq!(
            classify(
                trash,
                &trash.join("feature-0123456789abcdef0123456789abcdef01234567.bundle")
            ),
            Err(NotRecoveryImage::AbandonBundle)
        );
        for name in [
            format!("feature-{INCARNATION}.dmg.grants.json"),
            format!("feature-{INCARNATION}.dmg.ca.key"),
            "feature.dmg".to_owned(),
            "feature-not-an-incarnation.dmg".to_owned(),
            format!("feature-{INCARNATION}"),
        ] {
            assert_eq!(
                classify(trash, &trash.join(&name)),
                Err(NotRecoveryImage::Unnamed),
                "{name}"
            );
        }
    }
}

#[cfg(test)]
mod copy_tests;

#[cfg(test)]
pub(crate) fn recover_by_copy(
    layout: &StorageLayout,
    repo: &RepoId,
    image: &Path,
    to: &Path,
    observe: &mut impl FnMut(CopyCheckpoint, &Path) -> Result<()>,
) -> Result<RecoveredImage> {
    struct CopyOnly;
    impl RecoveryEnvironment for CopyOnly {
        fn reclaim_recovery_environment(
            &self,
            _: &RepoId,
            _: &Path,
        ) -> std::result::Result<(), crate::storage::apfs::ApfsStorageError> {
            Ok(())
        }
    }
    recover_with(layout, repo, image, to, &CopyOnly, true, observe)
}

fn valid_stage(name: &str, stage: &str) -> bool {
    stage
        .strip_prefix(&format!(".{name}.recovery-"))
        .and_then(|suffix| suffix.strip_suffix(".partial"))
        .is_some_and(|id| id.len() == 32 && id.bytes().all(|byte| byte.is_ascii_hexdigit()))
}

fn open_regular_at(directory: &fs::File, name: &CString) -> io::Result<fs::File> {
    // SAFETY: the directory and name outlive openat. NONBLOCK prevents a planted FIFO hanging admission.
    let fd = unsafe {
        libc::openat(
            directory.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_NOFOLLOW | libc::O_CLOEXEC | libc::O_NONBLOCK,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: openat returned a newly owned descriptor.
    let file = unsafe { fs::File::from_raw_fd(fd) };
    CopyFileIdentity::read(&file)?;
    Ok(file)
}

fn read_manifest_at(directory: &fs::File, path: &Path) -> Result<Option<Manifest>> {
    let name = held_name(&file_name(path)?, path)?;
    match open_regular_at(directory, &name) {
        Ok(file) => serde_json::from_reader(file).map(Some).map_err(|error| {
            CowshedError::integrity(
                format!("cannot read recovery manifest {}: {error}", path.display()),
                "preserve the destination; choose another --to or inspect its manifest",
            )
        }),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(io_failure("open recovery manifest", path, &error)),
    }
}

fn save_manifest(directory: &fs::File, path: &Path, manifest: &Manifest) -> Result<()> {
    let name = held_name(&file_name(path)?, path)?;
    let temporary = held_name(
        &format!(
            ".{}.tmp.{}",
            file_name(path)?,
            uuid::Uuid::new_v4().simple()
        ),
        path,
    )?;
    let mut file = cowshed_os::fsio::create_private_file_at(directory, &temporary)
        .map_err(|error| io_failure("create manifest staging file for", path, &error))?;
    serde_json::to_writer_pretty(&mut file, manifest).map_err(|error| {
        CowshedError::internal(format!("serialize {}: {error}", path.display()))
    })?;
    file.write_all(b"\n")
        .map_err(|error| io_failure("write", path, &error))?;
    file.sync_all()
        .map_err(|error| io_failure("sync", path, &error))?;
    cowshed_os::fsio::rename_noreplace(
        directory.as_raw_fd(),
        &temporary,
        directory.as_raw_fd(),
        &name,
    )
    .map_err(|error| io_failure("publish immutable manifest", path, &error))?;
    cowshed_os::fsio::sync_held_directory(directory)
        .map_err(|error| io_failure("sync manifest directory", path, &error))
}

fn transfer_failure(
    operation: &str,
    source: &Path,
    destination: &Path,
    cause: impl std::fmt::Display,
) -> CowshedError {
    CowshedError::integrity(
        format!(
            "cannot {operation} recovery artifact {} -> {}: {cause}",
            source.display(),
            destination.display()
        ),
        "retain the source, destination and manifest; resolve the named cause and retry the same recovery",
    )
}

fn require_identity(
    file: &fs::File,
    expected: &CopyFileIdentity,
    source: &Path,
    destination: &Path,
    role: &str,
) -> Result<()> {
    let actual = CopyFileIdentity::read(file)
        .map_err(|error| transfer_failure("inspect identity", source, destination, error))?;
    if actual != *expected {
        return Err(transfer_failure(
            "validate identity",
            source,
            destination,
            format!("{role} changed: expected {expected:?}, observed {actual:?}"),
        ));
    }
    Ok(())
}

fn set_copy_state(
    record: &mut RecoveryExport,
    index: usize,
    state: CopyState,
    journal_path: &Path,
) -> Result<()> {
    let previous = record.clone();
    let copy = record
        .progress
        .copy
        .as_mut()
        .ok_or_else(|| CowshedError::internal("copy state without copy manifest"))?;
    copy.artifacts[index].state = state;
    custody::save_export(journal_path, Some(&previous), record)
}

fn admit_transfer(
    manifest: &Manifest,
    held_trash: &fs::File,
    held_directory: &fs::File,
    trash: &Path,
    directory: &Path,
    manifest_path: &Path,
) -> Result<()> {
    let Some(copy) = &manifest.copy else {
        for name in &manifest.files {
            let from = trash.join(name);
            let into = directory.join(name);
            let entry = held_name(name, manifest_path)?;
            match (
                present_in(held_trash, &entry, &from)?,
                present_in(held_directory, &entry, &into)?,
            ) {
                (true, true) => return Err(both_exist(&from, &into)),
                (false, false) => {
                    return Err(transfer_failure(
                        "admit rename",
                        &from,
                        &into,
                        "both entries are missing",
                    ));
                }
                _ => {}
            }
        }
        return Ok(());
    };
    require_copy_directories(copy, held_trash, held_directory, trash, directory)?;
    let published = copy
        .artifacts
        .iter()
        .all(|artifact| matches!(artifact.state, CopyState::Published { .. }));
    for (name, artifact) in manifest.files.iter().zip(&copy.artifacts) {
        let from = trash.join(name);
        let into = directory.join(name);
        let entry = held_name(name, manifest_path)?;
        if !published || present_in(held_trash, &entry, &from)? {
            let source = open_regular_at(held_trash, &entry)
                .map_err(|error| transfer_failure("admit source", &from, &into, error))?;
            require_identity(&source, &artifact.source, &from, &into, "source")?;
        }
        match &artifact.state {
            CopyState::Published { target, .. } => {
                let destination = open_regular_at(held_directory, &entry)
                    .map_err(|error| transfer_failure("admit destination", &from, &into, error))?;
                require_identity(&destination, target, &from, &into, "destination")?;
            }
            CopyState::Copied { stage, target } => {
                let stage_name = held_name(stage, manifest_path)?;
                if present_in(held_directory, &stage_name, &directory.join(stage))? {
                    if present_in(held_directory, &entry, &into)? {
                        return Err(both_exist(&from, &into));
                    }
                    let staged = open_regular_at(held_directory, &stage_name).map_err(|error| {
                        transfer_failure("admit staged copy", &from, &into, error)
                    })?;
                    require_identity(&staged, target, &from, &into, "staged copy")?;
                } else {
                    let destination = open_regular_at(held_directory, &entry).map_err(|error| {
                        transfer_failure("admit interrupted publication", &from, &into, error)
                    })?;
                    let actual = CopyFileIdentity::read(&destination).map_err(|error| {
                        transfer_failure("identify interrupted publication", &from, &into, error)
                    })?;
                    if !target.after_rename(&actual) {
                        return Err(transfer_failure(
                            "admit interrupted publication",
                            &from,
                            &into,
                            format!("expected {target:?}, observed {actual:?}"),
                        ));
                    }
                }
            }
            CopyState::Pending | CopyState::Copying { .. } => {
                if present_in(held_directory, &entry, &into)? {
                    return Err(both_exist(&from, &into));
                }
                if let CopyState::Copying { stage, target } = &artifact.state {
                    let stage_name = held_name(stage, manifest_path)?;
                    if present_in(held_directory, &stage_name, &directory.join(stage))? {
                        let staged =
                            open_regular_at(held_directory, &stage_name).map_err(|error| {
                                transfer_failure("admit interrupted copy", &from, &into, error)
                            })?;
                        let actual = CopyFileIdentity::read(&staged).map_err(|error| {
                            transfer_failure("identify interrupted copy", &from, &into, error)
                        })?;
                        if !target.same_inode(&actual) {
                            return Err(transfer_failure(
                                "admit interrupted copy",
                                &from,
                                &into,
                                format!("expected {target:?}, observed {actual:?}"),
                            ));
                        }
                    }
                }
            }
        }
    }
    Ok(())
}

fn require_copy_directories(
    copy: &CopyTransfer,
    held_trash: &fs::File,
    held_directory: &fs::File,
    trash: &Path,
    directory: &Path,
) -> Result<()> {
    for (path, held, expected) in [
        (trash, held_trash, &copy.source_directory),
        (directory, held_directory, &copy.destination_directory),
    ] {
        for file in [held, &open_directory(path)?] {
            let actual = DirectoryIdentity::read(file)
                .map_err(|error| io_failure("identify", path, &error))?;
            if actual != *expected {
                return Err(transfer_failure(
                    "admit directory identity",
                    trash,
                    directory,
                    format!(
                        "{}: expected {expected:?}, observed {actual:?}",
                        path.display()
                    ),
                ));
            }
        }
    }
    Ok(())
}

fn copy_recovery(
    record: &mut RecoveryExport,
    held_trash: &fs::File,
    held_directory: &fs::File,
    trash: &Path,
    destination: &RecoveryDestination<'_>,
    project_root: &Path,
    observe: &mut impl FnMut(CopyCheckpoint, &Path) -> Result<()>,
) -> Result<()> {
    let directory = destination.directory;
    let manifest_path = destination.manifest;
    let journal_path = custody::journal_path(project_root);
    custody::admit_export(record, held_trash, held_directory, trash)?;
    observe(CopyCheckpoint::ManifestDurable, &record.progress.source)?;
    for index in 0..record.progress.files.len() {
        let name = record.progress.files[index].clone();
        let from = trash.join(&name);
        let into = directory.join(&name);
        let entry = held_name(&name, manifest_path)?;
        let artifact = record
            .progress
            .copy
            .as_ref()
            .ok_or_else(|| CowshedError::internal("missing copy manifest"))?
            .artifacts[index]
            .clone();
        if let CopyState::Published { target, .. } = &artifact.state {
            let copied = open_regular_at(held_directory, &entry).map_err(|error| {
                transfer_failure("open published destination", &from, &into, error)
            })?;
            require_identity(&copied, target, &from, &into, "destination")?;
            continue;
        }
        let source = open_regular_at(held_trash, &entry)
            .map_err(|error| transfer_failure("open source", &from, &into, error))?;
        require_identity(&source, &artifact.source, &from, &into, "source")?;
        let (stage, staged_identity) = match artifact.state.clone() {
            CopyState::Copied { stage, target } => (stage, target),
            CopyState::Pending | CopyState::Copying { .. } => {
                if present_in(held_directory, &entry, &into)? {
                    return Err(both_exist(&from, &into));
                }
                if let CopyState::Copying { stage, target } = &artifact.state {
                    let partial_name = held_name(stage, manifest_path)?;
                    if present_in(held_directory, &partial_name, &directory.join(stage))? {
                        let partial =
                            open_regular_at(held_directory, &partial_name).map_err(|error| {
                                transfer_failure(
                                    "inspect partial copy",
                                    &from,
                                    &directory.join(stage),
                                    error,
                                )
                            })?;
                        let actual = CopyFileIdentity::read(&partial).map_err(|error| {
                            transfer_failure(
                                "identify partial copy",
                                &from,
                                &directory.join(stage),
                                error,
                            )
                        })?;
                        if !target.same_inode(&actual) {
                            return Err(transfer_failure(
                                "admit partial copy",
                                &from,
                                &directory.join(stage),
                                format!(
                                    "partial changed: expected {target:?}, observed {actual:?}"
                                ),
                            ));
                        }
                    }
                }
                let stage = format!(".{name}.recovery-{}.partial", uuid::Uuid::new_v4().simple());
                let stage_name = held_name(&stage, manifest_path)?;
                let output = cowshed_os::fsio::create_private_file_at(held_directory, &stage_name)
                    .map_err(|error| {
                        transfer_failure(
                            "create partial copy",
                            &from,
                            &directory.join(&stage),
                            error,
                        )
                    })?;
                output.sync_all().map_err(|error| {
                    transfer_failure("sync partial identity", &from, &into, error)
                })?;
                cowshed_os::fsio::sync_held_directory(held_directory)
                    .map_err(|error| transfer_failure("sync partial entry", &from, &into, error))?;
                let target = CopyFileIdentity::read(&output)
                    .map_err(|error| transfer_failure("identify partial", &from, &into, error))?;
                set_copy_state(
                    record,
                    index,
                    CopyState::Copying {
                        stage: stage.clone(),
                        target,
                    },
                    &journal_path,
                )?;
                observe(CopyCheckpoint::CopyCreated, &from)?;
                require_identity(&source, &artifact.source, &from, &into, "source")?;
                copy_held_file(&source, &output).map_err(|error| {
                    transfer_failure("copy sparse bytes", &from, &directory.join(&stage), error)
                })?;
                require_identity(&source, &artifact.source, &from, &into, "source")?;
                output
                    .sync_all()
                    .map_err(|error| transfer_failure("sync copied bytes", &from, &into, error))?;
                let target = CopyFileIdentity::read(&output).map_err(|error| {
                    transfer_failure("identify copied bytes", &from, &into, error)
                })?;
                set_copy_state(
                    record,
                    index,
                    CopyState::Copied {
                        stage: stage.clone(),
                        target: target.clone(),
                    },
                    &journal_path,
                )?;
                observe(CopyCheckpoint::Staged, &from)?;
                (stage, target)
            }
            CopyState::Published { .. } => unreachable!("published handled above"),
        };
        let stage_name = held_name(&stage, manifest_path)?;
        if present_in(held_directory, &stage_name, &directory.join(&stage))? {
            if present_in(held_directory, &entry, &into)? {
                return Err(both_exist(&from, &into));
            }
            let staged = open_regular_at(held_directory, &stage_name).map_err(|error| {
                transfer_failure("open staged copy", &from, &directory.join(&stage), error)
            })?;
            require_identity(
                &staged,
                &staged_identity,
                &from,
                &directory.join(&stage),
                "staged copy",
            )?;
            require_identity(&source, &artifact.source, &from, &into, "source")?;
            cowshed_os::fsio::rename_noreplace(
                held_directory.as_raw_fd(),
                &stage_name,
                held_directory.as_raw_fd(),
                &entry,
            )
            .map_err(|error| transfer_failure("publish copied artifact", &from, &into, error))?;
        }
        let copied = open_regular_at(held_directory, &entry)
            .map_err(|error| transfer_failure("open copied destination", &from, &into, error))?;
        let actual = CopyFileIdentity::read(&copied)
            .map_err(|error| transfer_failure("identify destination", &from, &into, error))?;
        if !staged_identity.after_rename(&actual) {
            return Err(transfer_failure(
                "admit published inode",
                &from,
                &into,
                format!("expected staged {staged_identity:?}, observed {actual:?}"),
            ));
        }
        copied
            .sync_all()
            .map_err(|error| transfer_failure("sync destination", &from, &into, error))?;
        cowshed_os::fsio::sync_held_directory(held_directory)
            .map_err(|error| transfer_failure("sync publication", &from, &into, error))?;
        require_identity(&source, &artifact.source, &from, &into, "source")?;
        let digest = verify_held_file(&source, &copied)
            .map_err(|error| transfer_failure("verify copied bytes", &from, &into, error))?;
        require_identity(&source, &artifact.source, &from, &into, "source")?;
        require_identity(&copied, &actual, &from, &into, "verified destination")?;
        set_copy_state(
            record,
            index,
            CopyState::Published {
                target: actual,
                digest,
            },
            &journal_path,
        )?;
        observe(CopyCheckpoint::Published, &from)?;
    }
    custody::ensure_copy_proof(record, held_directory, &journal_path)?;
    observe(CopyCheckpoint::BeforeRetire, &record.progress.source)?;
    custody::admit_export(record, held_trash, held_directory, trash)?;
    custody::retire_sources(
        record,
        held_trash,
        held_directory,
        trash,
        project_root,
        observe,
    )?;
    cowshed_os::fsio::sync_held_directory(held_trash)
        .map_err(|error| io_failure("sync", trash, &error))?;
    cowshed_os::fsio::sync_held_directory(held_directory)
        .map_err(|error| io_failure("sync", directory, &error))?;
    Ok(())
}
