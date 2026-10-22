//! `cowshed gc --recover <image> --to <dir>`: take one recovery image out of the store
//! (02_workspaces.md, `rm` step 5, "Recovering a recovery image"; 01_storage.md, "Retention").
//!
//! A removal that cannot prove every source byte held elsewhere keeps the complete image as
//! `sessions/.trash/<ws>-<incarnation>.dmg` with its companions (02, "Recovery source"). Nothing in
//! the store deletes one, and terminal project removal refuses while one remains, so its contents
//! leave the store only deliberately, through this move. It renames and never copies or deletes:
//! the image and every companion go to `<to>/<ws>-<incarnation>/`, beside a manifest naming the
//! project, the workspace, its incarnation and why it was kept.
//!
//! The renames are ordered so that every point a crash can stop at leaves the bytes retained: the
//! manifest first, then the companions, the image last. An image whose grants sidecar has left is
//! a recovery image without its record, which nothing reclaims; a sidecar whose image had left
//! would read as a retired record and be collected, so the image never goes first. The same
//! command finds the manifest and finishes the move.

use std::ffi::{CString, OsStr, OsString};
use std::fs;
use std::io;
use std::os::fd::AsRawFd as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::path::{Component, Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::api::dto::{RecoveredImage, RecoveryRetention};
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

/// The manifest's name in the directory a recovery image moves to.
pub const MANIFEST_FILE: &str = "recovery.json";
const MANIFEST_VERSION: u32 = 1;

/// Why a path is not a recovery image of this project.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum NotRecoveryImage {
    /// Not an entry directly in this project's trash, `trash`.
    OutsideTrash { trash: PathBuf },
    /// One of the trash's `rm --abandon` bundles: commits, which `gc --abandoned` keeps in main.
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
                "cowshed gc --abandoned keeps a bundle's commits in main's repository before it reclaims the bundle",
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
}

impl Manifest {
    fn describes(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
        incarnation: &WorkspaceIncarnation,
        source: &Path,
    ) -> bool {
        self.version == MANIFEST_VERSION
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

/// Move the recovery image `image` of the project `layout` names, bound to `repo`, out of the store
/// into `<to>/<workspace>-<incarnation>/`, beside its manifest. `image` and `to` must be absolute.
/// Refuses before anything moves a path that is not one of this project's recovery images, a
/// destination inside the store, on another filesystem or already occupied, and a workspace whose
/// lifecycle lock another operation holds; a retry after an interruption finishes the move its
/// manifest describes.
pub fn recover(
    layout: &StorageLayout,
    repo: &RepoId,
    image: &Path,
    to: &Path,
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
    if trash_device != to_device {
        return Err(CowshedError::usage(
            format!(
                "{} is on another filesystem than the store's trash {}; a recovery image moves by rename, never by copy",
                to.display(),
                trash.display()
            ),
            "choose a directory outside the store on the store's own filesystem",
        ));
    }
    let directory = to.join(format!("{}-{}", workspace.as_str(), incarnation.as_str()));
    let manifest_path = directory.join(MANIFEST_FILE);
    // Only a real directory holding this image's own manifest is a move to finish: a symlink there
    // could aim a retry anywhere, back into the store included.
    let manifest = match fs::symlink_metadata(&directory) {
        Err(error) if error.kind() == io::ErrorKind::NotFound => plan(
            project.project_root.as_path(),
            repo,
            (&workspace, &incarnation),
            &image,
            &to,
            &directory,
        )?,
        Ok(metadata) if metadata.file_type().is_dir() => match read_manifest(&manifest_path)? {
            Some(manifest) if manifest.describes(repo, &workspace, &incarnation, &image) => {
                manifest
            }
            Some(_) | None => return Err(occupied(&directory)),
        },
        Ok(_) => return Err(occupied(&directory)),
        Err(error) => return Err(io_failure("inspect", &directory, &error)),
    };
    // Both directories are held by descriptor from here on: every rename is relative to them, so
    // a link planted at either path afterwards cannot redirect a move.
    let held_trash = open_directory(&trash)?;
    let held_directory = open_directory(&directory)?;
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

/// Read the image's own record, name everything that moves, make the destination directory and
/// write the manifest into it, durably, before the first rename.
fn plan(
    project_root: &Path,
    repo: &RepoId,
    (workspace, incarnation): (&WorkspaceName, &WorkspaceIncarnation),
    image: &Path,
    to: &Path,
    directory: &Path,
) -> Result<Manifest> {
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
                "cowshed doctor names the recovery images the project's trash keeps",
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
    for path in movable(image) {
        if present(&path)? {
            files.push(file_name(&path)?);
        }
    }
    let manifest = Manifest {
        version: MANIFEST_VERSION,
        repo_id: repo.clone(),
        workspace: workspace.clone(),
        incarnation: incarnation.clone(),
        source: image.to_owned(),
        files,
        grants,
        retained: deletion_log::recovery_retention(project_root, image),
        recovered_at: deletion_log::rfc3339_utc(SystemTime::now()),
    };
    match fs::create_dir(directory) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            return Err(occupied(directory));
        }
        Err(error) => return Err(io_failure("create", directory, &error)),
    }
    // The directory's own entry is durable before the manifest in it claims the move.
    sync_directory(to)?;
    let manifest_path = directory.join(MANIFEST_FILE);
    crate::metadata::write_json(&manifest_path, &manifest).map_err(|error| {
        CowshedError::environment_missing(
            format!("cannot write {}: {error}", manifest_path.display()),
            "make the destination writable, remove the directory the recovery made, then retry",
        )
    })?;
    Ok(manifest)
}

fn read_manifest(path: &Path) -> Result<Option<Manifest>> {
    match crate::metadata::read_json::<Manifest>(path) {
        Ok(manifest) => Ok(Some(manifest)),
        Err(crate::metadata::MetadataError::Io { source, .. })
            if source.kind() == io::ErrorKind::NotFound =>
        {
            Ok(None)
        }
        Err(error) => Err(CowshedError::integrity(
            format!(
                "cannot read the recovery manifest {}: {error}",
                path.display()
            ),
            "move the destination directory aside, or choose another --to",
        )),
    }
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
            "choose a directory outside the store, on the store's own filesystem",
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

/// `directory`, opened without following a link at its last component.
fn open_directory(directory: &Path) -> Result<fs::File> {
    fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC)
        .open(directory)
        .map_err(|error| io_failure("open", directory, &error))
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
