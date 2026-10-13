//! A workspace store of real directories: the lifecycle host [`super::apfs::ApfsSubstrate`]
//! runs over when a workspace is not a kernel volume.
//!
//! Every payload a native store keeps in an image - a workspace, a checkpoint, a staged
//! restore, a retired workspace, a build volume - is here a real directory at exactly the path
//! the layout gives that image. The payload never moves while it is mounted, so every path the
//! store's records, locks and recovery derive (`<image>.grants.json`, `<image>.ca.key`,
//! `<image>.lock`, the trash and checkpoint grammars, a build volume's `<id>.dmg` and its hold)
//! means the same thing it means for an image, and the production sidecar, marker, credential,
//! lock and restore-recovery formats are used unchanged.
//!
//! Mounting is a symlink binding: the mountpoint becomes a symlink naming the payload directory,
//! exchanged atomically with the empty directory an unmounted mountpoint is
//! (`renameatx_np(RENAME_SWAP)`), and unmounting exchanges an empty directory back. The binding
//! is the durable mount record: what the filesystem says is bound is what is mounted, across a
//! crash or a restart, and nothing else claims a mount.
//!
//! A directory is not a volume. Nothing here answers for a volume label, a device, a capacity, a
//! read-only mount or an extent map: those requests are refused with
//! [`ApfsStorageError::Unsupported`], never answered with a success nothing backs.

mod build_volumes;
mod recovery;
#[cfg(target_os = "macos")]
mod runtime_host;

use std::collections::BTreeSet;
use std::fs;
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

use cowshed_apfs::{DetachIntent, MountAccess};

use cowshed_apfs::image::{ImageCapacity, is_image_path};
use cowshed_apfs::store_lock::{ImageLockGuard, LockMode, acquire_image_locks};
use crate::metadata::{
    DetachedWorkspaceMetadata, MARKER_VERSION, Platform,
    PublicationState, SIDECAR_VERSION, WorkspaceIncarnation, WorkspaceInfoSnapshot,
    WorkspaceMarker, WorkspaceName, WorkspaceRole, sidecar_path,
};
use crate::repository::{OwnedRepoIds, RepoId};
use crate::workspace_credentials::{
    mint_workspace_credentials, validate_private_key, validate_public_workspace_assets,
};

use super::apfs::native::{
    CHECKPOINT_FACT_VERSION, CheckpointFactWire, CheckpointPinWire,
    RESTORE_RECOVERY_FACT_VERSION, RestoreRecoveryFactWire,
    checkout_mountpoint_staging_path, checkpoint_fact_path, clone_lineage_from,
    directory_children, empty_mountpoint, grants_sidecar_missing, io_error,
    metadata_workspace_ref, path_exists, pre_cowshed_path, read_dir_paths,
    remove_checkout_staging_mountpoint, restore_adopted_checkout_paths, restore_recovery_fact,
    restore_recovery_fact_path, swap_paths, sync_parent_path, warn_missing_session_sidecar,
    warn_session_image_issue,
};
use super::apfs::{
    ApfsExecutionHost, ApfsStorageError, ApfsSubstrateConfig, MarkerExpectation,
    MetadataPolicy, PendingAdoption, PendingPublicationFact, PublicationError, ResumableClone,
    combine_cleanup, companion_path, layout, main_aware_mount_point, volume_key,
};
use super::lifecycle::{
    CheckpointFact, DefragmentOutcome, KernelMountFact, LifecycleFact, LifecycleWorkspace,
    OperationIdentity, Pin, ResizeOutcome, RetiredRef, Revision, StorageFact, StorageGcPlan,
    StorageGcReport, SubstrateStats,
};
use super::{
    CheckpointLabel, PRE_RESTORE_PREFIX, WORKSPACE_MARKER_PATH, discover_session_images,
    verify_no_symlinks,
};

/// The name every refusal of a physical-volume request gives this backend.
const BACKEND: &str = "directory";

fn unsupported(operation: &'static str) -> ApfsStorageError {
    ApfsStorageError::Unsupported {
        backend: BACKEND,
        operation,
    }
}

fn host_error(error: impl std::fmt::Display) -> ApfsStorageError {
    ApfsStorageError::Host(error.to_string())
}

/// The lifecycle host of a directory store. It holds no state but its configuration: the
/// filesystem - payload directories, their records and their bindings - is the whole truth.
#[derive(Debug)]
pub struct DirectoryExecutionHost {
    config: ApfsSubstrateConfig,
}

/// A payload directory the executor holds, and where this handle has bound it, if anywhere.
///
/// The binding itself is durable on disk; the handle only remembers which mountpoint it made,
/// so an unmount or detach through the handle releases exactly that one.
#[derive(Debug)]
pub struct DirectoryAttachment {
    image: PathBuf,
    bound: Mutex<Option<PathBuf>>,
}

impl DirectoryAttachment {
    fn new(image: PathBuf) -> Self {
        Self {
            image,
            bound: Mutex::new(None),
        }
    }

    pub fn image(&self) -> &Path {
        &self.image
    }

    fn bound(&self) -> Option<PathBuf> {
        self.bound
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn set_bound(&self, mount_point: Option<PathBuf>) {
        *self.bound.lock().unwrap_or_else(PoisonError::into_inner) = mount_point;
    }
}

/// What occupies a mountpoint path.
#[derive(Debug, Eq, PartialEq)]
enum MountPointState {
    Absent,
    /// An empty directory: an unmounted mountpoint.
    Vacant,
    /// A symlink: a binding of whatever directory it names.
    Bound(PathBuf),
    /// Anything else: a non-empty directory, a file, a device. Never a mountpoint of ours.
    Occupied,
}

fn mount_point_state(mount_point: &Path) -> Result<MountPointState, ApfsStorageError> {
    let metadata = match fs::symlink_metadata(mount_point) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(MountPointState::Absent);
        }
        Err(error) => return Err(io_error("inspect mountpoint", mount_point, error)),
    };
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return fs::read_link(mount_point)
            .map(MountPointState::Bound)
            .map_err(|error| io_error("read mount binding", mount_point, error));
    }
    if file_type.is_dir() && empty_mountpoint(mount_point)? {
        return Ok(MountPointState::Vacant);
    }
    Ok(MountPointState::Occupied)
}

/// The sibling a binding change stages its replacement entry under: `RENAME_SWAP` exchanges two
/// entries of one filesystem, so it must sit beside the mountpoint.
fn binding_sibling(mount_point: &Path, suffix: &str) -> PathBuf {
    crate::metadata::append_suffix(mount_point, suffix)
}

const BIND_SUFFIX: &str = ".cowshed-bind";
const UNBIND_SUFFIX: &str = ".cowshed-unbind";

/// Clear a staging sibling a crash left: only a symlink or an empty directory, the only two
/// things a binding change ever stages; anything else at that name is not ours and refuses.
fn clear_binding_sibling(sibling: &Path) -> Result<(), ApfsStorageError> {
    match mount_point_state(sibling)? {
        MountPointState::Absent => Ok(()),
        MountPointState::Bound(_) => fs::remove_file(sibling)
            .map_err(|error| io_error("remove stale binding entry", sibling, error)),
        MountPointState::Vacant => fs::remove_dir(sibling)
            .map_err(|error| io_error("remove stale binding entry", sibling, error)),
        MountPointState::Occupied => Err(ApfsStorageError::Host(format!(
            "binding staging name {} holds something cowshed did not stage",
            sibling.display()
        ))),
    }
}

/// Bind `mount_point` to the payload directory `image`. Idempotent for an existing binding of
/// the same payload; refuses a binding of anything else and any occupied path.
fn bind(image: &Path, mount_point: &Path) -> Result<(), ApfsStorageError> {
    require_payload_directory(image)?;
    match mount_point_state(mount_point)? {
        MountPointState::Bound(target) if target == image => Ok(()),
        MountPointState::Bound(target) => Err(ApfsStorageError::Host(format!(
            "mountpoint {} is already bound to {}",
            mount_point.display(),
            target.display()
        ))),
        MountPointState::Occupied => Err(ApfsStorageError::Host(format!(
            "mountpoint {} is occupied: it is neither empty nor a binding",
            mount_point.display()
        ))),
        MountPointState::Absent => {
            let parent = mount_point
                .parent()
                .ok_or(ApfsStorageError::InvalidPlan("mountpoint has no parent"))?;
            fs::create_dir_all(parent)
                .map_err(|error| io_error("create mountpoint parent", parent, error))?;
            let staged = binding_sibling(mount_point, BIND_SUFFIX);
            clear_binding_sibling(&staged)?;
            symlink_dir(image, &staged)?;
            if let Err(error) = fs::rename(&staged, mount_point) {
                let primary = io_error("publish mount binding", mount_point, error);
                return combine_cleanup(
                    "publish mount binding",
                    primary,
                    fs::remove_file(&staged)
                        .map_err(|error| io_error("remove staged binding", &staged, error)),
                );
            }
            sync_parent_path(mount_point)
        }
        MountPointState::Vacant => {
            let staged = binding_sibling(mount_point, BIND_SUFFIX);
            clear_binding_sibling(&staged)?;
            symlink_dir(image, &staged)?;
            if let Err(primary) = swap_paths(mount_point, &staged) {
                return combine_cleanup(
                    "publish mount binding",
                    primary,
                    fs::remove_file(&staged)
                        .map_err(|error| io_error("remove staged binding", &staged, error)),
                );
            }
            // The swap left the vacant directory at the staged name.
            fs::remove_dir(&staged)
                .map_err(|error| io_error("remove displaced mountpoint", &staged, error))?;
            sync_parent_path(mount_point)
        }
    }
}

/// Unbind `mount_point` from `image`, leaving the empty directory an unmounted mountpoint is.
/// Nothing bound there is nothing to unbind; a binding of anything else refuses.
fn unbind(image: &Path, mount_point: &Path) -> Result<(), ApfsStorageError> {
    match mount_point_state(mount_point)? {
        MountPointState::Absent | MountPointState::Vacant | MountPointState::Occupied => Ok(()),
        MountPointState::Bound(target) if target != image => Err(ApfsStorageError::Host(format!(
            "refusing to unbind {}: it binds {}, not {}",
            mount_point.display(),
            target.display(),
            image.display()
        ))),
        MountPointState::Bound(_) => {
            let staged = binding_sibling(mount_point, UNBIND_SUFFIX);
            clear_binding_sibling(&staged)?;
            fs::create_dir(&staged)
                .map_err(|error| io_error("stage vacant mountpoint", &staged, error))?;
            if let Err(primary) = swap_paths(mount_point, &staged) {
                return combine_cleanup(
                    "release mount binding",
                    primary,
                    fs::remove_dir(&staged)
                        .map_err(|error| io_error("remove staged mountpoint", &staged, error)),
                );
            }
            // The swap left the binding at the staged name; it is a symlink, never followed.
            match mount_point_state(&staged)? {
                MountPointState::Bound(target) if target == image => fs::remove_file(&staged)
                    .map_err(|error| io_error("remove released binding", &staged, error))?,
                other => {
                    return Err(ApfsStorageError::Host(format!(
                        "released binding at {} is not the binding of {}: {other:?}",
                        staged.display(),
                        image.display()
                    )));
                }
            }
            sync_parent_path(mount_point)
        }
    }
}

fn symlink_dir(target: &Path, link: &Path) -> Result<(), ApfsStorageError> {
    std::os::unix::fs::symlink(target, link)
        .map_err(|error| io_error("create mount binding", link, error))
}

/// `image` is a real directory: not a symlink, not a file.
fn require_payload_directory(image: &Path) -> Result<(), ApfsStorageError> {
    let metadata = fs::symlink_metadata(image)
        .map_err(|error| io_error("inspect payload directory", image, error))?;
    if metadata.file_type().is_dir() {
        Ok(())
    } else {
        Err(ApfsStorageError::Host(format!(
            "payload is not a directory: {}",
            image.display()
        )))
    }
}

/// Bytes allocated to the tree at `root`, from an `lstat` walk: symlinks are counted, never
/// followed, and an inode reachable by several names (a hard link) is counted once. A clone
/// shares blocks with its source on a copy-on-write filesystem; like `st_blocks`, this reports
/// each file's own allocation, not unique host blocks.
pub(crate) fn allocated_tree_bytes(root: &Path) -> io::Result<u64> {
    tree_usage(root).map(|usage| usage.allocated)
}

#[derive(Clone, Copy, Debug, Default)]
struct TreeUsage {
    logical: u64,
    allocated: u64,
}

fn tree_usage(root: &Path) -> io::Result<TreeUsage> {
    let mut usage = TreeUsage::default();
    let mut seen = BTreeSet::new();
    let mut pending = vec![root.to_owned()];
    while let Some(path) = pending.pop() {
        let metadata = fs::symlink_metadata(&path)?;
        if metadata.is_dir() {
            for entry in fs::read_dir(&path)? {
                pending.push(entry?.path());
            }
        }
        if !seen.insert((metadata.dev(), metadata.ino())) {
            continue;
        }
        let overflow = || io::Error::other("tree byte accounting overflow");
        if metadata.is_file() {
            usage.logical = usage.logical.checked_add(metadata.len()).ok_or_else(overflow)?;
        }
        usage.allocated = metadata
            .blocks()
            .checked_mul(512)
            .and_then(|bytes| usage.allocated.checked_add(bytes))
            .ok_or_else(overflow)?;
    }
    Ok(usage)
}

/// The sidecar platform of a payload this host writes: the platform it runs on.
const fn host_platform() -> Platform {
    if cfg!(target_os = "macos") {
        Platform::Macos
    } else {
        Platform::Linux
    }
}

fn read_metadata(image: &Path) -> Result<DetachedWorkspaceMetadata, ApfsStorageError> {
    DetachedWorkspaceMetadata::read_for_image(image).map_err(host_error)
}

fn ensure_parent(path: &Path) -> Result<(), ApfsStorageError> {
    let parent = path
        .parent()
        .ok_or(ApfsStorageError::InvalidPlan("image path has no parent"))?;
    fs::create_dir_all(parent).map_err(|error| io_error("create image directory", parent, error))
}

fn validate_image(image: &Path) -> Result<(), ApfsStorageError> {
    crate::metadata::validate_image_path(image).map_err(host_error)
}

impl DirectoryExecutionHost {
    /// A host over the directory store `config` names. The store root must be an absolute
    /// directory reached through no symlink; it is created when it does not exist yet.
    pub fn new(config: ApfsSubstrateConfig) -> Result<Self, ApfsStorageError> {
        let root = &config.store_root;
        if !root.is_absolute() || !crate::repository::is_lexically_canonical(root) {
            return Err(ApfsStorageError::Layout(
                super::StorageLayoutError::EscapesStoreRoot,
            ));
        }
        if !config.checkout_path.is_absolute() {
            return Err(ApfsStorageError::InvalidPlan(
                "directory store checkout path is not absolute",
            ));
        }
        match fs::symlink_metadata(root) {
            Ok(metadata) if metadata.file_type().is_dir() => {}
            Ok(metadata) if metadata.file_type().is_symlink() => {
                return Err(ApfsStorageError::Layout(
                    super::StorageLayoutError::SymlinkComponent(root.clone()),
                ));
            }
            Ok(_) => {
                return Err(ApfsStorageError::Host(format!(
                    "directory store root is not a directory: {}",
                    root.display()
                )));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir_all(root)
                    .map_err(|error| io_error("create directory store root", root, error))?;
            }
            Err(error) => return Err(io_error("inspect directory store root", root, error)),
        }
        Ok(Self { config })
    }

    pub fn config(&self) -> &ApfsSubstrateConfig {
        &self.config
    }

    fn verify_controller_path(&self, path: &Path) -> Result<(), ApfsStorageError> {
        if path == self.config.store_root {
            let metadata = fs::symlink_metadata(path)
                .map_err(|error| io_error("inspect controller store root", path, error))?;
            if metadata.file_type().is_symlink() {
                return Err(ApfsStorageError::Layout(
                    super::StorageLayoutError::SymlinkComponent(path.to_owned()),
                ));
            }
        } else if path.starts_with(&self.config.store_root) {
            verify_no_symlinks(&self.config.store_root, path)?;
        }
        Ok(())
    }

    /// A payload this host may create, bind or delete: strictly inside the store root, reached
    /// through no symlink. Everything outside the store is someone else's tree.
    fn require_store_image(&self, image: &Path) -> Result<(), ApfsStorageError> {
        if image == self.config.store_root || !image.starts_with(&self.config.store_root) {
            return Err(ApfsStorageError::Layout(
                super::StorageLayoutError::EscapesStoreRoot,
            ));
        }
        self.verify_controller_path(image)
    }

    fn find_canonical_image(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<(PathBuf, DetachedWorkspaceMetadata)>, ApfsStorageError> {
        let image = layout(&self.config, repo)?
            .canonical_image(workspace)?
            .image()
            .to_owned();
        if !path_exists(&image)? {
            return Ok(None);
        }
        let metadata = read_metadata(&image)?;
        if metadata.repo_id != *repo || metadata.workspace != *workspace {
            return Err(ApfsStorageError::Host(format!(
                "detached metadata identity mismatch for {}",
                image.display()
            )));
        }
        Ok(Some((image, metadata)))
    }

    fn expected_mount_point(
        &self,
        metadata: &DetachedWorkspaceMetadata,
    ) -> Result<PathBuf, ApfsStorageError> {
        main_aware_mount_point(&self.config, &metadata.repo_id, &metadata.workspace)
    }

    fn marker_names(
        &self,
        tree: &Path,
        repo: &RepoId,
        workspace: &WorkspaceName,
        incarnation: &WorkspaceIncarnation,
    ) -> bool {
        let owned = self.config.owned_repo_ids(repo);
        WorkspaceMarker::read_from(&tree.join(WORKSPACE_MARKER_PATH)).is_ok_and(|marker| {
            owned.accepts(&marker.repo_id)
                && marker.workspace == *workspace
                && marker.workspace_incarnation == *incarnation
        })
    }

    /// Every published workspace of `repo`: main when its sidecar is active, and each session
    /// payload with an active sidecar. A pending fence is invisible until it is activated.
    fn published_facts(&self, repo: &RepoId) -> Result<Vec<StorageFact>, ApfsStorageError> {
        let storage = layout(&self.config, repo)?;
        let mut facts = Vec::new();
        let main = WorkspaceName::main();
        if let Some((_, metadata)) = self.find_canonical_image(repo, &main)?
            && metadata.publication_state == PublicationState::Active
        {
            facts.push(StorageFact {
                workspace: metadata_workspace_ref(&metadata)?,
                volume_key: volume_key(repo, &main),
            });
        }
        let entries = read_dir_paths(&storage.project().sessions, "enumerate session payloads")?;
        for discovered in discover_session_images(entries) {
            match grants_sidecar_missing(discovered.path()) {
                Ok(false) => {}
                Ok(true) => {
                    warn_missing_session_sidecar(discovered.path());
                    continue;
                }
                Err(error) => {
                    warn_session_image_issue(
                        discovered.path(),
                        format!("cannot inspect grants sidecar: {error}"),
                    );
                    continue;
                }
            }
            let metadata = read_metadata(discovered.path())?;
            if metadata.repo_id != *repo || metadata.workspace != *discovered.workspace() {
                return Err(ApfsStorageError::Host(format!(
                    "detached metadata identity mismatch for {}",
                    discovered.path().display()
                )));
            }
            if metadata.publication_state == PublicationState::PendingFence {
                continue;
            }
            facts.push(StorageFact {
                workspace: metadata_workspace_ref(&metadata)?,
                volume_key: volume_key(repo, discovered.workspace()),
            });
        }
        Ok(facts)
    }

    /// Hand the checkout path over to a mountpoint, retaining the displaced tree at `retained`:
    /// one `RENAME_SWAP` of the original tree with an empty directory, then a rename of the tree
    /// to its retained name. Converges from an interrupted attempt and never moves a bound one.
    fn vacate_checkout_to(
        &self,
        source_checkout: &Path,
        retained: &Path,
    ) -> Result<(), PublicationError> {
        let staging = checkout_mountpoint_staging_path(source_checkout);
        let checkout = mount_point_state(source_checkout).map_err(PublicationError::rolled_back)?;
        // A Git checkout cannot be empty. An empty mountpoint is a completed swap only when its
        // displaced nonempty tree exists at a handoff name; a binding is a mounted main.
        let already_swapped = match checkout {
            MountPointState::Bound(_) => true,
            MountPointState::Vacant => [staging.as_path(), retained]
                .into_iter()
                .try_fold(false, |found, path| {
                    Ok::<_, ApfsStorageError>(
                        found || (path_exists(path)? && !empty_mountpoint(path)?),
                    )
                })
                .map_err(PublicationError::rolled_back)?,
            MountPointState::Absent | MountPointState::Occupied => false,
        };
        if already_swapped {
            if !path_exists(&staging).map_err(PublicationError::forward_only)? {
                return Ok(());
            }
            if path_exists(retained).map_err(PublicationError::forward_only)? {
                return Err(PublicationError::forward_only(
                    ApfsStorageError::InvalidPlan(
                        "displaced adopted checkout and retained tree both exist",
                    ),
                ));
            }
            return fs::rename(&staging, retained)
                .map_err(|error| io_error("retain displaced checkout", retained, error))
                .and_then(|()| sync_parent_path(retained))
                .map_err(PublicationError::forward_only);
        }
        let metadata = fs::symlink_metadata(source_checkout).map_err(|error| {
            PublicationError::rolled_back(io_error(
                "inspect adopted checkout",
                source_checkout,
                error,
            ))
        })?;
        if !metadata.file_type().is_dir() {
            return Err(PublicationError::rolled_back(
                ApfsStorageError::InvalidPlan("adopted checkout is not the original directory"),
            ));
        }
        if path_exists(retained).map_err(PublicationError::rolled_back)? {
            return Err(PublicationError::rolled_back(
                ApfsStorageError::InvalidPlan("retained checkout already exists"),
            ));
        }
        remove_checkout_staging_mountpoint(&staging).map_err(PublicationError::rolled_back)?;
        fs::create_dir(&staging)
            .map_err(|error| io_error("create checkout mountpoint", &staging, error))
            .and_then(|()| sync_parent_path(&staging))
            .map_err(PublicationError::rolled_back)?;
        if let Err(primary) = swap_paths(source_checkout, &staging) {
            return Err(match remove_checkout_staging_mountpoint(&staging) {
                Ok(()) => PublicationError::rolled_back(primary),
                Err(cleanup) => PublicationError::forward_only(ApfsStorageError::Cleanup {
                    operation: "remove staged adopted mountpoint",
                    primary: Box::new(primary),
                    cleanup: Box::new(cleanup),
                }),
            });
        }
        match fs::rename(&staging, retained)
            .map_err(|error| io_error("retain displaced checkout", retained, error))
            .and_then(|()| sync_parent_path(retained))
        {
            Ok(()) => Ok(()),
            Err(primary) => {
                let cleanup = swap_paths(source_checkout, &staging)
                    .and_then(|()| remove_checkout_staging_mountpoint(&staging));
                Err(match cleanup {
                    Ok(()) => PublicationError::rolled_back(primary),
                    Err(cleanup) => PublicationError::forward_only(ApfsStorageError::Cleanup {
                        operation: "restore adopted checkout after failed retention",
                        primary: Box::new(primary),
                        cleanup: Box::new(cleanup),
                    }),
                })
            }
        }
    }

    /// Clone the payload tree `source` to the new payload `destination`: every leaf by
    /// `clonefile` (or a reflink, or a copy where neither exists), until a pass observes the
    /// source unchanged. A failed clone leaves no destination.
    fn clone_tree(&self, source: &Path, destination: &Path) -> Result<(), ApfsStorageError> {
        require_payload_directory(source)?;
        if path_exists(destination)? {
            return Err(ApfsStorageError::Host(format!(
                "clone destination already exists: {}",
                destination.display()
            )));
        }
        ensure_parent(destination)?;
        fs::create_dir(destination)
            .map_err(|error| io_error("create clone payload", destination, error))?;
        match crate::copy::copy_until_quiescent_blocking(source, destination) {
            Ok(_) => sync_parent_path(destination),
            Err(error) => combine_cleanup(
                "clone payload",
                host_error(error),
                remove_payload_tree(destination),
            ),
        }
    }

    fn remove_sidecar(image: &Path) -> Result<(), ApfsStorageError> {
        use super::deletion_log::{self, DeletionKind, DeletionOp};
        let sidecar = sidecar_path(image);
        match fs::remove_file(&sidecar) {
            Ok(()) => {
                deletion_log::log_deletion_for_image(
                    image,
                    DeletionOp::RemoveSidecar,
                    DeletionKind::Sidecar,
                    &sidecar,
                );
                sync_parent_path(&sidecar)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io_error("remove detached metadata", &sidecar, error)),
        }
    }

    fn rename_sidecar(source_image: &Path, destination_image: &Path) -> Result<(), ApfsStorageError> {
        let source = sidecar_path(source_image);
        if !path_exists(&source)? {
            return Ok(());
        }
        let destination = sidecar_path(destination_image);
        ensure_parent(&destination)?;
        if path_exists(&destination)? {
            return Err(ApfsStorageError::Host(format!(
                "metadata destination already exists: {}",
                destination.display()
            )));
        }
        fs::rename(&source, &destination)
            .map_err(|error| io_error("rename detached metadata", &source, error))?;
        sync_parent_path(&destination)
    }

    fn remove_companion(image: &Path) -> Result<(), ApfsStorageError> {
        use super::deletion_log::{self, DeletionKind, DeletionOp};
        let companion = companion_path(image);
        match fs::remove_file(&companion) {
            Ok(()) => {
                deletion_log::log_deletion_for_image(
                    image,
                    DeletionOp::RemoveCompanion,
                    DeletionKind::Companion,
                    &companion,
                );
                sync_parent_path(&companion)
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io_error("remove CA companion", &companion, error)),
        }
    }

    /// Delete a payload tree inside the store. Its records stay for the caller.
    fn delete_payload(&self, image: &Path) -> Result<bool, ApfsStorageError> {
        self.require_store_image(image)?;
        match fs::symlink_metadata(image) {
            Ok(metadata) if metadata.file_type().is_dir() => {
                remove_payload_tree(image)?;
                Ok(true)
            }
            Ok(_) => Err(ApfsStorageError::Host(format!(
                "payload is not a directory: {}",
                image.display()
            ))),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(io_error("inspect payload", image, error)),
        }
    }

    fn checkpoint_label(image: &Path) -> Result<&str, ApfsStorageError> {
        image
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| {
                ApfsStorageError::Host(format!("invalid checkpoint name: {}", image.display()))
            })
    }

    /// Every checkpoint payload of `workspace`: the `<label>.dmg` directories under its
    /// checkpoint directory.
    fn checkpoint_payloads(directory: &Path) -> Result<Vec<PathBuf>, ApfsStorageError> {
        Ok(directory_children(directory)?
            .into_iter()
            .filter(|path| is_image_path(path))
            .collect())
    }
}

/// Remove a payload tree, read-only directories included; symlinks inside are unlinked, never
/// followed.
fn remove_payload_tree(image: &Path) -> Result<(), ApfsStorageError> {
    cowshed_os::fsio::remove_owned_tree(image)
        .map_err(|error| io_error("remove payload tree", image, error))?;
    sync_parent_path(image)
}

impl ApfsExecutionHost for DirectoryExecutionHost {
    const HAS_PHYSICAL_VOLUMES: bool = false;

    type LockGuard = ImageLockGuard;
    type Attachment = DirectoryAttachment;

    fn lock_images(
        &self,
        images: &[PathBuf],
        mode: LockMode,
    ) -> Result<Option<Self::LockGuard>, ApfsStorageError> {
        acquire_image_locks(&self.config.store_root, images, mode).map_err(Into::into)
    }

    fn observe(&self, expected: &[LifecycleFact]) -> Result<Vec<LifecycleFact>, ApfsStorageError> {
        let observed = |metadata: &DetachedWorkspaceMetadata, topology_revision| {
            LifecycleFact::Exists {
                repo: metadata.repo_id.clone(),
                name: metadata.workspace.clone(),
                incarnation: metadata.workspace_incarnation.clone(),
                revision: Revision::new(metadata.grants.revision),
                topology_revision,
                retired: false,
            }
        };
        expected
            .iter()
            .map(|fact| match fact {
                LifecycleFact::Exists {
                    repo,
                    name,
                    topology_revision,
                    ..
                } => self
                    .find_canonical_image(repo, name)?
                    .map(|(_, metadata)| observed(&metadata, *topology_revision))
                    .ok_or(ApfsStorageError::Host(format!(
                        "published workspace is missing: {repo}/{name}"
                    ))),
                LifecycleFact::Absent {
                    repo,
                    name,
                    topology_revision,
                } => match self.find_canonical_image(repo, name)? {
                    // A pending fence is invisible to enumeration and owned by its lifecycle
                    // intent: the planned verb reaches apply, where it resumes.
                    Some((_, metadata))
                        if metadata.publication_state == PublicationState::PendingFence =>
                    {
                        Ok(LifecycleFact::Absent {
                            repo: repo.clone(),
                            name: name.clone(),
                            topology_revision: *topology_revision,
                        })
                    }
                    Some((_, metadata)) => Ok(observed(&metadata, *topology_revision)),
                    None => Ok(LifecycleFact::Absent {
                        repo: repo.clone(),
                        name: name.clone(),
                        topology_revision: *topology_revision,
                    }),
                },
                LifecycleFact::Checkpoint {
                    repo,
                    workspace,
                    label,
                    revision,
                } => {
                    let image = layout(&self.config, repo)?
                        .checkpoint_image(workspace, label)?
                        .image()
                        .to_owned();
                    if !path_exists(&image)? {
                        return Err(ApfsStorageError::Host(format!(
                            "checkpoint is missing: {repo}/{workspace}/{label}@{}",
                            revision.get()
                        )));
                    }
                    let metadata = read_metadata(&image)?;
                    Ok(LifecycleFact::Checkpoint {
                        repo: metadata.repo_id,
                        workspace: metadata.workspace,
                        label: label.clone(),
                        revision: Revision::new(metadata.grants.revision),
                    })
                }
            })
            .collect()
    }

    /// A new empty payload directory at `image`, held but not bound. A directory has no
    /// capacity: it grows within its filesystem, so `capacity` describes nothing here.
    fn create_attached(
        &self,
        _capacity: ImageCapacity,
        image: &Path,
    ) -> Result<Self::Attachment, ApfsStorageError> {
        self.require_store_image(image)?;
        validate_image(image)?;
        if path_exists(image)? {
            return Err(ApfsStorageError::Host(format!(
                "canonical payload already exists: {}",
                image.display()
            )));
        }
        ensure_parent(image)?;
        fs::create_dir(image).map_err(|error| io_error("create payload", image, error))?;
        sync_parent_path(image)?;
        Ok(DirectoryAttachment::new(image.to_owned()))
    }

    /// The payload never moves while bound, so the source's tree is `source` itself whether or
    /// not it is mounted; `source_mount` names nothing a directory clone needs.
    fn clone_image(
        &self,
        source: &Path,
        _source_mount: Option<&Path>,
        destination: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.require_store_image(source)?;
        self.require_store_image(destination)?;
        read_metadata(source)?;
        validate_image(source)?;
        validate_image(destination)?;
        self.clone_tree(source, destination)
    }

    /// A directory clone shares no extent map with its source: there is nothing to copy before
    /// a first write. The executor skips this step for hosts without physical volumes.
    fn write_first(&self, _image: &Path) -> Result<(), ApfsStorageError> {
        Err(unsupported("taking a clone's own extent map"))
    }

    fn resumable_clone(
        &self,
        config: &ApfsSubstrateConfig,
        source: &LifecycleWorkspace,
        destination: &WorkspaceName,
        destination_topology: Revision,
        requested_identity: &OperationIdentity,
    ) -> Result<Option<ResumableClone>, ApfsStorageError> {
        let image = layout(config, source.repo())?
            .session_image(destination)?
            .image()
            .to_owned();
        if !path_exists(&sidecar_path(&image))? {
            return Ok(None);
        }
        let metadata = read_metadata(&image)?;
        if metadata.publication_state != PublicationState::PendingFence {
            return Ok(None);
        }
        if !path_exists(&image)? {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "pending clone metadata has no canonical payload: {}",
                image.display()
            )));
        }
        let expected_revision = Revision::new(source.revision().get() + 1);
        let workspace = LifecycleWorkspace::new(
            metadata.repo_id.clone(),
            metadata.workspace.clone(),
            metadata.workspace_incarnation.clone(),
            expected_revision,
            Revision::new(destination_topology.get() + 1),
            WorkspaceRole::Workspace,
        )
        .map_err(|_| {
            ApfsStorageError::MarkerMismatch(format!(
                "pending clone carries an invalid lifecycle identity: {}",
                image.display()
            ))
        })?;
        let info = &metadata.info_snapshot;
        // Base commit is provenance, not identity: a workspace forked from an older main resumes.
        if workspace.repo() != source.repo()
            || workspace.name() != destination
            || metadata.grants.revision != expected_revision.get()
            || info.project_root != requested_identity.project_root
            || info.branch != requested_identity.branch
            || info.forked_from != requested_identity.forked_from
            || info.git_worktree != requested_identity.git_worktree
        {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "pending clone does not match resumed operation: {}",
                image.display()
            )));
        }
        let identity = OperationIdentity {
            project_root: info.project_root.clone(),
            base_commit: info.base_commit.clone(),
            created_at: info.created_at.clone(),
            branch: info.branch.clone(),
            forked_from: info.forked_from.clone(),
            created_trace: requested_identity.created_trace.clone(),
            grants: metadata.grants.clone(),
            git_worktree: info.git_worktree,
        };
        Ok(Some(ResumableClone {
            workspace,
            identity,
            image,
        }))
    }

    fn pending_adoption(
        &self,
        config: &ApfsSubstrateConfig,
        repo: &RepoId,
    ) -> Result<Option<PendingAdoption>, ApfsStorageError> {
        let image = layout(config, repo)?.main_image()?.image().to_owned();
        let image_exists = path_exists(&image)?;
        if !path_exists(&sidecar_path(&image))? {
            return if image_exists {
                Err(ApfsStorageError::MarkerMismatch(format!(
                    "canonical main payload has no metadata: {}",
                    image.display()
                )))
            } else {
                Ok(None)
            };
        }
        let metadata = read_metadata(&image)?;
        if metadata.publication_state != PublicationState::PendingFence {
            return Err(ApfsStorageError::InvalidPlan("main is already published"));
        }
        if !image_exists {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "pending adoption metadata has no canonical payload: {}",
                image.display()
            )));
        }
        if metadata.repo_id != *repo
            || !metadata.workspace.is_main()
            || metadata.info_snapshot.role != WorkspaceRole::Main
            || metadata.grants.revision != 1
        {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "pending main does not record an adoption of {repo}: {}",
                image.display()
            )));
        }
        Ok(Some(PendingAdoption {
            incarnation: metadata.workspace_incarnation,
            image,
            info: metadata.info_snapshot,
            grants: metadata.grants,
        }))
    }

    /// Unbind whatever binding of `image` the adoption's staging made, then reclaim it.
    fn discard_pending(&self, image: &Path) -> Result<(), ApfsStorageError> {
        let metadata = read_metadata(image)?;
        let workspace = metadata_workspace_ref(&metadata)?;
        let staging = super::apfs::staging_mount(&self.config, &workspace)?;
        unbind(image, &staging)?;
        unbind(image, &self.expected_mount_point(&metadata)?)?;
        self.reclaim_image(image)
    }

    fn unmount_attachment(&self, attachment: &Self::Attachment) -> Result<(), ApfsStorageError> {
        if let Some(mount_point) = attachment.bound() {
            unbind(&attachment.image, &mount_point)?;
            attachment.set_bound(None);
        }
        Ok(())
    }

    fn copy_tree(&self, source: &Path, destination: &Path) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(destination)?;
        crate::copy::copy_until_quiescent_blocking(source, destination)
            .and_then(|_| crate::copy::verify_tree_blocking(source, destination))
            .map(|report| {
                eprintln!(
                    "cowshed: verified copy {} -> {}; regular-file allocated bytes: source {}, copied {}; content {}",
                    source.display(),
                    destination.display(),
                    report.source_allocated_bytes,
                    report.copied_allocated_bytes,
                    report.digest.to_hex()
                );
            })
            .map_err(host_error)
    }

    /// Hold the payload `image`: a real directory inside the store with readable metadata. A
    /// pending fence outside staging is a publication nobody may open.
    fn attach_verified(&self, image: &Path) -> Result<Self::Attachment, ApfsStorageError> {
        self.require_store_image(image)?;
        let metadata = read_metadata(image)?;
        if metadata.publication_state == PublicationState::PendingFence
            && !image
                .components()
                .any(|component| component.as_os_str() == super::recovery::STAGING_NAMESPACE)
        {
            return Err(ApfsStorageError::PendingPublication(image.to_owned()));
        }
        require_payload_directory(image)?;
        Ok(DirectoryAttachment::new(image.to_owned()))
    }

    /// Bind `mount_point` to the attachment's payload. A binding cannot be read-only: whoever
    /// reaches the tree through it can write it, so a read-only mount is refused rather than
    /// promised. `browse` is a Finder presentation flag with no meaning for a symlink.
    fn mount(
        &self,
        attachment: &Self::Attachment,
        mount_point: &Path,
        access: MountAccess,
        _browse: bool,
    ) -> Result<(), ApfsStorageError> {
        if access == MountAccess::ReadOnly {
            return Err(unsupported("read-only mounts"));
        }
        self.verify_controller_path(mount_point)?;
        if let Some(bound) = attachment.bound()
            && bound != mount_point
        {
            return Err(ApfsStorageError::Host(format!(
                "{} is already bound at {}",
                attachment.image.display(),
                bound.display()
            )));
        }
        bind(&attachment.image, mount_point)?;
        attachment.set_bound(Some(mount_point.to_owned()));
        Ok(())
    }

    /// Reuse a surviving binding of `image` at `mount_point`, or bind it.
    fn attach_and_mount_resumable(
        &self,
        image: &Path,
        mount_point: &Path,
        _workspace: &LifecycleWorkspace,
    ) -> Result<Self::Attachment, ApfsStorageError> {
        self.require_store_image(image)?;
        self.verify_controller_path(mount_point)?;
        require_payload_directory(image)?;
        bind(image, mount_point)?;
        let attachment = DirectoryAttachment::new(image.to_owned());
        attachment.set_bound(Some(mount_point.to_owned()));
        Ok(attachment)
    }

    fn rename_volume(
        &self,
        _attachment: &Self::Attachment,
        _volume_name: &str,
    ) -> Result<(), ApfsStorageError> {
        Err(unsupported("volume labels"))
    }

    fn mint_workspace_credentials(
        &self,
        workspace: &LifecycleWorkspace,
        image_path: &Path,
        mount_point: &Path,
        private_key_path: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(image_path)?;
        self.verify_controller_path(mount_point)?;
        self.verify_controller_path(private_key_path)?;
        let metadata = read_metadata(image_path)?;
        if metadata.repo_id != *workspace.repo()
            || metadata.workspace != *workspace.name()
            || metadata.workspace_incarnation != *workspace.incarnation()
        {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "credential environment metadata disagrees with workspace: {}",
                image_path.display()
            )));
        }
        mint_workspace_credentials(
            workspace,
            mount_point,
            metadata.platform,
            metadata.grants.port_block,
            private_key_path,
        )
        .map_err(host_error)
    }

    fn write_marker(
        &self,
        mount_point: &Path,
        workspace: &LifecycleWorkspace,
        forked_from: Option<&WorkspaceName>,
        identity: &OperationIdentity,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(mount_point)?;
        let marker_path = mount_point.join(WORKSPACE_MARKER_PATH);
        if let Ok(existing) = WorkspaceMarker::read_from(&marker_path)
            && existing.repo_id == *workspace.repo()
            && existing.project_root == identity.project_root
            && existing.workspace == *workspace.name()
            && existing.workspace_incarnation == *workspace.incarnation()
            && existing.role == workspace.role()
            && existing.base_commit == identity.base_commit
            && existing.created_at == identity.created_at
            && existing.forked_from.as_ref() == forked_from
        {
            existing
                .validate()
                .map_err(|error| ApfsStorageError::MarkerMismatch(error.to_string()))?;
            return validate_public_workspace_assets(
                &OwnedRepoIds::sole(workspace.repo().clone()),
                workspace.name(),
                workspace.incarnation(),
                mount_point,
            )
            .map_err(|error| ApfsStorageError::MarkerMismatch(error.to_string()));
        }
        // A clone arrives with its source's marker in place: that marker is the lineage.
        let lineage = clone_lineage_from(&marker_path, workspace.repo());
        let marker = WorkspaceMarker {
            version: MARKER_VERSION,
            repo_id: workspace.repo().clone(),
            project_root: identity.project_root.clone(),
            workspace: workspace.name().clone(),
            workspace_incarnation: workspace.incarnation().clone(),
            role: workspace.role(),
            base_commit: identity.base_commit.clone(),
            created_at: identity.created_at.clone(),
            forked_from: forked_from.cloned(),
            created_trace: identity.created_trace.clone(),
            lineage: Some(lineage),
        };
        marker.validate().map_err(host_error)?;
        let parent = marker_path.parent().ok_or(ApfsStorageError::InvalidPlan(
            "workspace marker has no parent",
        ))?;
        fs::create_dir_all(parent)
            .map_err(|error| io_error("create marker directory", parent, error))?;
        crate::metadata::write_json(&marker_path, &marker).map_err(host_error)
    }

    fn validate_marker(
        &self,
        mount_point: &Path,
        expected: &MarkerExpectation,
    ) -> Result<(), ApfsStorageError> {
        let marker_path = mount_point.join(WORKSPACE_MARKER_PATH);
        let marker = WorkspaceMarker::read_from(&marker_path)
            .map_err(|error| ApfsStorageError::MarkerMismatch(error.to_string()))?;
        if expected.repos.accepts(&marker.repo_id)
            && marker.workspace == expected.workspace
            && marker.workspace_incarnation == expected.incarnation
        {
            validate_public_workspace_assets(
                &expected.repos,
                &marker.workspace,
                &marker.workspace_incarnation,
                mount_point,
            )
            .map_err(|error| ApfsStorageError::MarkerMismatch(error.to_string()))
        } else {
            Err(ApfsStorageError::MarkerMismatch(format!(
                "{} does not match {}/{}",
                marker_path.display(),
                expected.repos,
                expected.workspace,
            )))
        }
    }

    fn validate_staged_companion(&self, path: &Path) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(path)?;
        validate_private_key(path).map_err(host_error)
    }

    /// Release the attachment: its binding, when it made one. A held directory has nothing
    /// else to let go of. A binding outlives no process: whoever is inside the tree keeps the
    /// directory they are in, so neither intent waits for idleness.
    fn detach(
        &self,
        attachment: Self::Attachment,
        _intent: DetachIntent,
    ) -> Result<(), ApfsStorageError> {
        self.unmount_attachment(&attachment)
    }

    /// A binding at the workspace's mountpoint must bind its own canonical payload and carry
    /// its marker; there are no mount flags to heal.
    fn heal_mount(
        &self,
        workspace: &LifecycleWorkspace,
        mount_point: &Path,
    ) -> Result<(), ApfsStorageError> {
        let MountPointState::Bound(target) = mount_point_state(mount_point)? else {
            return Ok(());
        };
        let canonical = layout(&self.config, workspace.repo())?
            .canonical_image(workspace.name())?
            .image()
            .to_owned();
        if target != canonical
            || !self.marker_names(
                &target,
                workspace.repo(),
                workspace.name(),
                workspace.incarnation(),
            )
        {
            return Err(ApfsStorageError::Host(format!(
                "cannot use unrelated binding at {}: it binds {}, which carries no {}/{} marker for incarnation {}",
                mount_point.display(),
                target.display(),
                workspace.repo(),
                workspace.name(),
                workspace.incarnation()
            )));
        }
        Ok(())
    }

    /// The workspace's mount, kept: its binding must be the canonical one at the workspace's
    /// mountpoint. The mount id is the binding's inode, which a new binding replaces.
    fn retain_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        attachment: Self::Attachment,
    ) -> Result<u64, ApfsStorageError> {
        let expected = main_aware_mount_point(&self.config, workspace.repo(), workspace.name())?;
        let canonical = layout(&self.config, workspace.repo())?
            .canonical_image(workspace.name())?
            .image()
            .to_owned();
        if attachment.image != canonical || attachment.bound().as_deref() != Some(expected.as_path())
        {
            return Err(ApfsStorageError::Host(format!(
                "retained attachment of {} is not bound at {}'s mountpoint {}",
                attachment.image.display(),
                workspace.name(),
                expected.display()
            )));
        }
        binding_id(&expected)
    }

    fn detach_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        _intent: DetachIntent,
    ) -> Result<(), ApfsStorageError> {
        let canonical = layout(&self.config, workspace.repo())?
            .canonical_image(workspace.name())?
            .image()
            .to_owned();
        let mount_point =
            main_aware_mount_point(&self.config, workspace.repo(), workspace.name())?;
        unbind(&canonical, &mount_point)
    }

    fn ensure_linked_build_volume(
        &self,
        workspace: &LifecycleWorkspace,
        checkout: &Path,
    ) -> Result<(), ApfsStorageError> {
        let Some(target) = crate::build_volume::link::linked(checkout).map_err(host_error)? else {
            return Ok(());
        };
        let build = crate::build_volume::BuildVolumeLayout::new(
            layout(&self.config, workspace.repo())?.project(),
        )?;
        let id = build.volume_at(&target).ok_or_else(|| {
            ApfsStorageError::Host(format!(
                "{} names {}, which is not one of this project's build volumes",
                checkout.join(crate::build_volume::BUILD_LINK).display(),
                target.display()
            ))
        })?;
        self.settle_build_link(
            &build,
            workspace.name(),
            workspace.incarnation(),
            checkout,
            &id,
        )
    }

    fn resize(
        &self,
        _workspace: &LifecycleWorkspace,
        _image: &Path,
        _mount_point: &Path,
        _capacity: ImageCapacity,
    ) -> Result<ResizeOutcome, ApfsStorageError> {
        Err(unsupported("resizing a workspace"))
    }

    fn defragment(
        &self,
        _workspace: &LifecycleWorkspace,
        _image: &Path,
        _mount_point: &Path,
    ) -> Result<DefragmentOutcome, ApfsStorageError> {
        Err(unsupported("defragmenting a workspace"))
    }

    fn restore_adopted_checkout(
        &self,
        workspace: &LifecycleWorkspace,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
    ) -> Result<(), ApfsStorageError> {
        if !workspace.name().is_main()
            || source_checkout != self.config.checkout_path
            || pre_cowshed_checkout != pre_cowshed_path(source_checkout)
        {
            return Err(ApfsStorageError::InvalidPlan(
                "adoption rollback does not target this host's canonical main",
            ));
        }
        self.detach_mounted(workspace, DetachIntent::WhenIdle)?;
        restore_adopted_checkout_paths(source_checkout, pre_cowshed_checkout)
    }

    fn vacate_adopted_checkout(
        &self,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
    ) -> Result<(), PublicationError> {
        if !source_checkout.is_absolute()
            || pre_cowshed_checkout != pre_cowshed_path(source_checkout)
        {
            return Err(PublicationError::rolled_back(
                ApfsStorageError::InvalidPlan("invalid adopt checkout or pre-cowshed handoff"),
            ));
        }
        self.vacate_checkout_to(source_checkout, pre_cowshed_checkout)
    }

    fn vacate_plain_checkout(
        &self,
        checkout: &Path,
        preserved: &Path,
    ) -> Result<(), PublicationError> {
        if !checkout.is_absolute()
            || !preserved.is_absolute()
            || preserved == checkout
            || preserved == pre_cowshed_path(checkout)
            || preserved.parent() != checkout.parent()
        {
            return Err(PublicationError::rolled_back(
                ApfsStorageError::InvalidPlan("invalid plain checkout or preserved handoff"),
            ));
        }
        self.vacate_checkout_to(checkout, preserved)
    }

    fn require_vacant_checkout(&self, checkout: &Path) -> Result<(), ApfsStorageError> {
        if !checkout.is_absolute() || checkout != self.config.checkout_path {
            return Err(ApfsStorageError::InvalidPlan(
                "a replacement mounts only at this host's canonical checkout",
            ));
        }
        match mount_point_state(checkout)? {
            MountPointState::Vacant => Ok(()),
            MountPointState::Bound(_) => Err(ApfsStorageError::InvalidPlan(
                "a payload is still bound at the checkout a replacement mounts on",
            )),
            MountPointState::Absent | MountPointState::Occupied => {
                Err(ApfsStorageError::InvalidPlan(
                    "the checkout a replacement mounts on is not an empty directory",
                ))
            }
        }
    }

    fn publish_metadata(
        &self,
        image: &Path,
        workspace: &LifecycleWorkspace,
        revision: Revision,
        policy: MetadataPolicy,
        identity: Option<&OperationIdentity>,
        source_image: Option<&Path>,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(image)?;
        validate_image(image)?;
        ensure_parent(image)?;
        let preserved = match (policy, source_image) {
            (MetadataPolicy::Preserve | MetadataPolicy::PendingFence, Some(source)) => {
                Some(read_metadata(source)?)
            }
            (MetadataPolicy::Preserve | MetadataPolicy::PendingFence, None) => {
                return Err(ApfsStorageError::InvalidPlan(
                    "preserved metadata requires a source image",
                ));
            }
            (MetadataPolicy::Fresh | MetadataPolicy::FreshPendingFence, _) => None,
        };
        let fresh_identity = || {
            identity.ok_or(ApfsStorageError::InvalidPlan(
                "fresh metadata requires operation identity",
            ))
        };
        let mut grants = match &preserved {
            Some(metadata) => metadata.grants.clone(),
            None => fresh_identity()?.grants.clone(),
        };
        grants.revision = revision.get();
        let updated_at = identity.map_or_else(
            || {
                preserved
                    .as_ref()
                    .map(|metadata| metadata.updated_at.clone())
                    .unwrap_or_default()
            },
            |identity| identity.created_at.clone(),
        );
        let info_snapshot = match &preserved {
            None => {
                let identity = fresh_identity()?;
                WorkspaceInfoSnapshot {
                    project_root: identity.project_root.clone(),
                    role: workspace.role(),
                    base_commit: identity.base_commit.clone(),
                    branch: identity.branch.clone(),
                    created_at: identity.created_at.clone(),
                    forked_from: identity.forked_from.clone(),
                    captured_at: identity.created_at.clone(),
                    stale: false,
                    git_worktree: identity.git_worktree,
                }
            }
            Some(metadata) => {
                let mut info = metadata.info_snapshot.clone();
                info.role = workspace.role();
                if let Some(identity) = identity {
                    info.captured_at.clone_from(&identity.created_at);
                    info.stale = false;
                }
                info
            }
        };
        DetachedWorkspaceMetadata {
            version: SIDECAR_VERSION,
            repo_id: workspace.repo().clone(),
            workspace: workspace.name().clone(),
            workspace_incarnation: workspace.incarnation().clone(),
            platform: host_platform(),
            publication_state: match policy {
                MetadataPolicy::FreshPendingFence | MetadataPolicy::PendingFence => {
                    PublicationState::PendingFence
                }
                MetadataPolicy::Fresh | MetadataPolicy::Preserve => PublicationState::Active,
            },
            updated_at,
            grants,
            info_snapshot,
        }
        .write_for_image(image)
        .map_err(host_error)
    }

    fn activate_pending(
        &self,
        image: &Path,
        workspace: &LifecycleWorkspace,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(image)?;
        let mut metadata = read_metadata(image)?;
        if metadata.repo_id != *workspace.repo()
            || metadata.workspace != *workspace.name()
            || metadata.workspace_incarnation != *workspace.incarnation()
            || metadata.grants.revision != workspace.revision().get()
            || metadata.publication_state != PublicationState::PendingFence
        {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "pending metadata disagrees with activation target: {}",
                image.display()
            )));
        }
        metadata.publication_state = PublicationState::Active;
        metadata.write_for_image(image).map_err(host_error)
    }

    fn publish_checkpoint_fact(
        &self,
        image: &Path,
        label: &CheckpointLabel,
        revision: Revision,
        pin: Pin,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(image)?;
        let metadata = read_metadata(image)?;
        let fact = CheckpointFactWire {
            version: CHECKPOINT_FACT_VERSION,
            repo_id: metadata.repo_id,
            workspace: metadata.workspace,
            label: label.clone(),
            revision: revision.get(),
            pin: pin.into(),
        };
        let path = checkpoint_fact_path(image);
        crate::metadata::write_json(&path, &fact).map_err(host_error)?;
        sync_parent_path(&path)
    }

    /// Exchange the staged replacement with the canonical payload and keep the old one as the
    /// undo generation: the same sequence, records and links the image store uses, every
    /// rename atomic, every step undone on failure.
    fn restore_swap(
        &self,
        staged: &Path,
        canonical: &Path,
        undo: &Path,
    ) -> Result<(), ApfsStorageError> {
        for path in [staged, canonical, undo] {
            self.require_store_image(path)?;
        }
        ensure_parent(undo)?;
        if path_exists(undo)? {
            return Err(ApfsStorageError::Host(format!(
                "restore undo payload already exists: {}",
                undo.display()
            )));
        }
        let undo_sidecar = sidecar_path(undo);
        let staged_companion = companion_path(staged);
        let canonical_companion = companion_path(canonical);
        let undo_companion = companion_path(undo);
        read_metadata(staged)?;
        read_metadata(canonical)?;
        self.validate_staged_companion(&staged_companion)?;
        self.validate_staged_companion(&canonical_companion)?;
        fs::hard_link(sidecar_path(canonical), &undo_sidecar)
            .map_err(|error| io_error("retain restore metadata", &undo_sidecar, error))?;
        let discard_undo_records = || {
            let _ = fs::remove_file(&undo_sidecar);
            let _ = fs::remove_file(&undo_companion);
        };
        if let Err(error) = fs::hard_link(&canonical_companion, &undo_companion) {
            discard_undo_records();
            return Err(io_error("retain restore CA key", &undo_companion, error));
        }
        if let Err(error) = swap_paths(canonical, staged) {
            discard_undo_records();
            return Err(error);
        }
        if let Err(primary) = swap_paths(&canonical_companion, &staged_companion) {
            let rollback = swap_paths(canonical, staged);
            discard_undo_records();
            return combine_cleanup("restore CA key swap", primary, rollback);
        }
        if let Err(error) = fs::rename(staged, undo) {
            let primary = io_error("retain restore undo payload", undo, error);
            let rollback = swap_paths(&canonical_companion, &staged_companion)
                .and_then(|()| swap_paths(canonical, staged));
            discard_undo_records();
            return combine_cleanup("restore swap", primary, rollback);
        }
        fs::remove_file(&staged_companion)
            .map_err(|error| io_error("remove displaced CA key", &staged_companion, error))?;
        sync_parent_path(canonical)?;
        sync_parent_path(undo)
    }

    fn publish_restored_metadata(
        &self,
        staged: &Path,
        canonical: &Path,
        workspace: &LifecycleWorkspace,
        revision: Revision,
        source_image: &Path,
        replaced_incarnation: &WorkspaceIncarnation,
    ) -> Result<PendingPublicationFact, ApfsStorageError> {
        for path in [staged, canonical, source_image] {
            self.verify_controller_path(path)?;
        }
        let source_metadata = read_metadata(source_image)?;
        let source_checkpoint = CheckpointLabel::new(Self::checkpoint_label(source_image)?)
            .map_err(host_error)?;
        if source_metadata.repo_id != *workspace.repo()
            || source_metadata.workspace != *workspace.name()
            || source_metadata.workspace_incarnation == *workspace.incarnation()
            || replaced_incarnation == workspace.incarnation()
        {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "restore checkpoint metadata disagrees with destination {}",
                canonical.display()
            )));
        }
        let recovery_fact = RestoreRecoveryFactWire {
            version: RESTORE_RECOVERY_FACT_VERSION,
            repo_id: workspace.repo().clone(),
            workspace: workspace.name().clone(),
            source_checkpoint,
            source_incarnation: source_metadata.workspace_incarnation,
            replaced_incarnation: replaced_incarnation.clone(),
            destination_incarnation: workspace.incarnation().clone(),
        };
        let recovery_path = restore_recovery_fact_path(canonical);
        crate::metadata::write_json(&recovery_path, &recovery_fact).map_err(host_error)?;
        sync_parent_path(&recovery_path)?;
        self.publish_metadata(
            canonical,
            workspace,
            revision,
            MetadataPolicy::PendingFence,
            None,
            Some(source_image),
        )?;
        sync_parent_path(canonical)?;
        Self::remove_sidecar(staged)?;
        sync_parent_path(canonical)?;
        let metadata = read_metadata(canonical)?;
        if metadata.repo_id != *workspace.repo()
            || metadata.workspace != *workspace.name()
            || metadata.workspace_incarnation != *workspace.incarnation()
            || metadata.grants.revision != revision.get()
            || metadata.publication_state != PublicationState::PendingFence
        {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "published restore metadata disagrees with authoritative workspace: {}",
                canonical.display()
            )));
        }
        Ok(PendingPublicationFact {
            workspace: workspace.clone(),
            image: canonical.to_owned(),
            mount_point: self.expected_mount_point(&metadata)?,
            source_checkpoint: recovery_fact.source_checkpoint.to_string(),
            source_incarnation: recovery_fact.source_incarnation,
            replaced_incarnation: recovery_fact.replaced_incarnation,
            destination_incarnation: recovery_fact.destination_incarnation,
        })
    }

    fn activate_restored_metadata(&self, canonical: &Path) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(canonical)?;
        let mut metadata = read_metadata(canonical)?;
        let fact_path = restore_recovery_fact_path(canonical);
        match metadata.publication_state {
            PublicationState::PendingFence => {
                restore_recovery_fact(&self.config, canonical, &metadata)?;
                metadata.publication_state = PublicationState::Active;
                metadata.write_for_image(canonical).map_err(host_error)?;
                sync_parent_path(canonical)?;
            }
            PublicationState::Active if path_exists(&fact_path)? => {
                restore_recovery_fact(&self.config, canonical, &metadata)?;
            }
            PublicationState::Active => return sync_parent_path(canonical),
        }
        match fs::remove_file(&fact_path) {
            Ok(()) => sync_parent_path(canonical),
            Err(error) if error.kind() == io::ErrorKind::NotFound => sync_parent_path(canonical),
            Err(error) => Err(io_error(
                "remove completed restore recovery fact",
                &fact_path,
                error,
            )),
        }
    }

    fn rollback_restore(
        &self,
        canonical: &Path,
        undo: &Path,
        staged: &Path,
    ) -> Result<(), ApfsStorageError> {
        for path in [canonical, undo, staged] {
            self.require_store_image(path)?;
        }
        let canonical_sidecar = sidecar_path(canonical);
        let undo_sidecar = sidecar_path(undo);
        let staged_sidecar = sidecar_path(staged);
        let canonical_companion = self.recovery_companion(canonical, "restore rollback canonical")?;
        let undo_companion = self.recovery_companion(undo, "restore rollback undo")?;
        let staged_companion = companion_path(staged);
        if path_exists(&staged_companion)? {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "restore rollback staging CA destination is occupied: canonical={}, undo={}, staged={}, companion={}",
                canonical.display(),
                undo.display(),
                staged.display(),
                staged_companion.display()
            )));
        }
        fs::rename(undo, staged)
            .map_err(|error| io_error("stage failed restore payload", staged, error))?;
        fs::hard_link(&undo_companion, &staged_companion).map_err(|error| {
            io_error("stage displaced restore CA key", &staged_companion, error)
        })?;
        swap_paths(canonical, staged)?;
        swap_paths(&canonical_companion, &staged_companion)?;
        fs::rename(&canonical_sidecar, &staged_sidecar).map_err(|error| {
            io_error("stage failed replacement metadata", &staged_sidecar, error)
        })?;
        fs::rename(&undo_sidecar, &canonical_sidecar)
            .map_err(|error| io_error("restore displaced metadata", &canonical_sidecar, error))?;
        self.delete_payload(staged)?;
        Self::remove_sidecar(staged)?;
        Self::remove_companion(staged)?;
        fs::remove_file(&undo_companion)
            .map_err(|error| io_error("remove restore undo CA link", &undo_companion, error))?;
        let recovery_path = restore_recovery_fact_path(canonical);
        match fs::remove_file(&recovery_path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => {
                return Err(io_error(
                    "remove rolled-back restore recovery fact",
                    &recovery_path,
                    error,
                ));
            }
        }
        sync_parent_path(canonical)
    }

    fn retire_image(&self, canonical: &Path, trash: &Path) -> Result<(), ApfsStorageError> {
        self.require_store_image(canonical)?;
        self.require_store_image(trash)?;
        ensure_parent(trash)?;
        let canonical_companion = companion_path(canonical);
        let trash_companion = companion_path(trash);
        if path_exists(trash)? || path_exists(&trash_companion)? {
            return Err(ApfsStorageError::Host(format!(
                "retired payload artifacts already exist: {}",
                trash.display()
            )));
        }
        self.validate_staged_companion(&canonical_companion)?;
        fs::rename(canonical, trash).map_err(|error| io_error("retire payload", trash, error))?;
        let roll_back_payload = || {
            fs::rename(trash, canonical)
                .map_err(|error| io_error("roll back retirement", canonical, error))
        };
        if let Err(primary) = Self::rename_sidecar(canonical, trash) {
            return combine_cleanup("retire payload", primary, roll_back_payload());
        }
        if let Err(error) = fs::rename(&canonical_companion, &trash_companion) {
            let primary = io_error("retire CA key", &trash_companion, error);
            let cleanup = Self::rename_sidecar(trash, canonical).and_then(|()| roll_back_payload());
            return combine_cleanup("retire payload", primary, cleanup);
        }
        sync_parent_path(trash)
    }

    fn reclaim_image(&self, image: &Path) -> Result<(), ApfsStorageError> {
        use super::deletion_log::{self, DeletionKind, DeletionOp};
        if self.delete_payload(image)? {
            deletion_log::log_deletion_for_image(
                image,
                DeletionOp::ReclaimImage,
                DeletionKind::Image,
                image,
            );
        }
        Self::remove_companion(image)?;
        for (artifact, operation, kind) in [
            (
                checkpoint_fact_path(image),
                "remove checkpoint fact",
                DeletionKind::Sidecar,
            ),
            (
                restore_recovery_fact_path(image),
                "remove restore recovery fact",
                DeletionKind::Other,
            ),
        ] {
            match fs::remove_file(&artifact) {
                Ok(()) => deletion_log::log_deletion_for_image(
                    image,
                    DeletionOp::ReclaimImage,
                    kind,
                    &artifact,
                ),
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(io_error(operation, &artifact, error)),
            }
        }
        Self::remove_sidecar(image)?;
        sync_parent_path(image)
    }

    fn reclaim_retired(
        &self,
        config: &ApfsSubstrateConfig,
        retired: &RetiredRef,
    ) -> Result<(), ApfsStorageError> {
        self.reclaim_retired_record(config, retired)
    }

    fn reclaim_unrecorded_retired(
        &self,
        config: &ApfsSubstrateConfig,
        repo: &RepoId,
        image: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.reclaim_unrecorded_retired_payload(config, repo, image)
    }

    fn list(&self, repo: &RepoId) -> Result<Vec<StorageFact>, ApfsStorageError> {
        self.published_facts(repo)
    }

    fn pending_publications(
        &self,
        repo: &RepoId,
    ) -> Result<Vec<PendingPublicationFact>, ApfsStorageError> {
        let storage = layout(&self.config, repo)?;
        let pending_fact = |image: &Path, metadata: &DetachedWorkspaceMetadata| {
            let fact = restore_recovery_fact(&self.config, image, metadata)?;
            Ok::<_, ApfsStorageError>(PendingPublicationFact {
                workspace: metadata_workspace_ref(metadata)?,
                mount_point: self.expected_mount_point(metadata)?,
                source_checkpoint: fact.source_checkpoint.to_string(),
                source_incarnation: fact.source_incarnation,
                replaced_incarnation: fact.replaced_incarnation,
                destination_incarnation: fact.destination_incarnation,
                image: image.to_owned(),
            })
        };
        let mut pending = Vec::new();
        if let Some((image, metadata)) = self.find_canonical_image(repo, &WorkspaceName::main())?
            && metadata.publication_state == PublicationState::PendingFence
            && path_exists(&restore_recovery_fact_path(&image))?
        {
            pending.push(pending_fact(&image, &metadata)?);
        }
        let entries = read_dir_paths(
            &storage.project().sessions,
            "enumerate pending session payloads",
        )?;
        for discovered in discover_session_images(entries) {
            if grants_sidecar_missing(discovered.path())? {
                warn_missing_session_sidecar(discovered.path());
                continue;
            }
            // A pending create or fork is resumed by its lifecycle intent, not here.
            if !path_exists(&restore_recovery_fact_path(discovered.path()))? {
                continue;
            }
            let metadata = read_metadata(discovered.path())?;
            if metadata.publication_state != PublicationState::PendingFence {
                continue;
            }
            if metadata.repo_id != *repo || metadata.workspace != *discovered.workspace() {
                return Err(ApfsStorageError::Host(format!(
                    "pending detached metadata identity mismatch for {}",
                    discovered.path().display()
                )));
            }
            pending.push(pending_fact(discovered.path(), &metadata)?);
        }
        Ok(pending)
    }

    /// Every published workspace bound at its own mountpoint to its own canonical payload,
    /// whose marker names it. A binding of anything else at a workspace's mountpoint refuses.
    fn mounts(&self, repo: &RepoId) -> Result<Vec<KernelMountFact>, ApfsStorageError> {
        let storage = layout(&self.config, repo)?;
        let mut mounts = Vec::new();
        for fact in self.published_facts(repo)? {
            let workspace = &fact.workspace;
            let mount_point =
                main_aware_mount_point(&self.config, workspace.repo(), workspace.name())?;
            let MountPointState::Bound(target) = mount_point_state(&mount_point)? else {
                continue;
            };
            let canonical = storage.canonical_image(workspace.name())?.image().to_owned();
            if target != canonical
                || !self.marker_names(
                    &target,
                    workspace.repo(),
                    workspace.name(),
                    workspace.incarnation(),
                )
            {
                return Err(ApfsStorageError::Host(format!(
                    "workspace binding identity mismatch at {}: it binds {}, which carries no {}/{} marker for incarnation {}",
                    mount_point.display(),
                    target.display(),
                    workspace.repo(),
                    workspace.name(),
                    workspace.incarnation()
                )));
            }
            mounts.push(KernelMountFact {
                mount_id: binding_id(&mount_point)?,
                volume_key: fact.volume_key,
            });
        }
        Ok(mounts)
    }

    fn checkpoints(&self, repo: &RepoId) -> Result<Vec<CheckpointFact>, ApfsStorageError> {
        let root = layout(&self.config, repo)?.project().checkpoints.clone();
        let mut facts = Vec::new();
        for workspace_directory in directory_children(&root)? {
            let Some(workspace_name) = workspace_directory
                .file_name()
                .and_then(|name| name.to_str())
                .map(WorkspaceName::new)
                .transpose()
                .map_err(host_error)?
            else {
                continue;
            };
            for image in Self::checkpoint_payloads(&workspace_directory)? {
                let fact_path = checkpoint_fact_path(&image);
                if !path_exists(&fact_path)? {
                    continue;
                }
                let wire: CheckpointFactWire =
                    crate::metadata::read_json(&fact_path).map_err(host_error)?;
                if wire.version != CHECKPOINT_FACT_VERSION
                    || wire.repo_id != *repo
                    || wire.workspace != workspace_name
                    || wire.label.as_str() != Self::checkpoint_label(&image)?
                {
                    return Err(ApfsStorageError::Host(format!(
                        "checkpoint fact does not match payload path: {}",
                        fact_path.display()
                    )));
                }
                facts.push(CheckpointFact {
                    repo: wire.repo_id,
                    workspace: wire.workspace,
                    label: wire.label,
                    revision: Revision::new(wire.revision),
                    pin: Pin::from(wire.pin),
                });
            }
        }
        facts.sort_by(|left, right| {
            (&left.workspace, &left.label).cmp(&(&right.workspace, &right.label))
        });
        Ok(facts)
    }

    fn recover_pending(
        &self,
        config: &ApfsSubstrateConfig,
        held_locks: &[PathBuf],
    ) -> Result<(), ApfsStorageError> {
        self.recover_store(config, held_locks)
    }

    /// The bytes the workspace's tree and its checkpoints' trees allocate, walked, never
    /// estimated: a directory has no volume statistics to read.
    fn stats(
        &self,
        workspace: &LifecycleWorkspace,
        image: &Path,
    ) -> Result<SubstrateStats, ApfsStorageError> {
        validate_image(image)?;
        let detached = read_metadata(image)?;
        if detached.publication_state == PublicationState::PendingFence {
            return Err(ApfsStorageError::PendingPublication(image.to_owned()));
        }
        if detached.repo_id != *workspace.repo()
            || detached.workspace != *workspace.name()
            || detached.workspace_incarnation != *workspace.incarnation()
        {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "detached metadata disagrees with active payload {}",
                image.display()
            )));
        }
        let usage =
            tree_usage(image).map_err(|error| io_error("measure payload", image, error))?;
        let checkpoint_directory = layout(&self.config, workspace.repo())?
            .project()
            .checkpoints
            .join(workspace.name().as_str());
        let overflow = || ApfsStorageError::InvalidPlan("checkpoint byte accounting overflow");
        let mut stats = SubstrateStats {
            logical_bytes: usage.logical,
            allocated_bytes: usage.allocated,
            ..SubstrateStats::default()
        };
        for checkpoint in Self::checkpoint_payloads(&checkpoint_directory)? {
            let metadata = read_metadata(&checkpoint)?;
            if metadata.repo_id != *workspace.repo()
                || metadata.workspace != *workspace.name()
                || metadata.publication_state != PublicationState::Active
            {
                return Err(ApfsStorageError::MarkerMismatch(format!(
                    "checkpoint metadata disagrees with payload path: {}",
                    checkpoint.display()
                )));
            }
            let label = Self::checkpoint_label(&checkpoint)?;
            let pinned = if let Some(destination) = label.strip_prefix(PRE_RESTORE_PREFIX) {
                let destination = WorkspaceIncarnation::new(destination).map_err(host_error)?;
                if metadata.workspace_incarnation == destination {
                    return Err(ApfsStorageError::MarkerMismatch(format!(
                        "pre-restore checkpoint retained the destination incarnation: {}",
                        checkpoint.display()
                    )));
                }
                false
            } else {
                let fact_path = checkpoint_fact_path(&checkpoint);
                let fact: CheckpointFactWire =
                    crate::metadata::read_json(&fact_path).map_err(host_error)?;
                if fact.version != CHECKPOINT_FACT_VERSION
                    || fact.repo_id != *workspace.repo()
                    || fact.workspace != *workspace.name()
                    || fact.label.as_str() != label
                {
                    return Err(ApfsStorageError::Host(format!(
                        "checkpoint fact does not match payload path: {}",
                        fact_path.display()
                    )));
                }
                fact.pin == CheckpointPinWire::Pinned
            };
            let bytes = allocated_tree_bytes(&checkpoint)
                .map_err(|error| io_error("measure checkpoint", &checkpoint, error))?;
            stats.checkpoint_count = stats.checkpoint_count.checked_add(1).ok_or(
                ApfsStorageError::InvalidPlan("checkpoint count overflow"),
            )?;
            stats.checkpoint_bytes = stats
                .checkpoint_bytes
                .checked_add(bytes)
                .ok_or_else(overflow)?;
            if pinned {
                stats.pinned_checkpoint_bytes = stats
                    .pinned_checkpoint_bytes
                    .checked_add(bytes)
                    .ok_or_else(overflow)?;
            }
        }
        Ok(stats)
    }

    fn preview_gc(
        &self,
        config: &ApfsSubstrateConfig,
        repo: &RepoId,
    ) -> Result<StorageGcPlan, ApfsStorageError> {
        if config.store_root != self.config.store_root {
            return Err(ApfsStorageError::InvalidPlan(
                "GC config differs from host storage root",
            ));
        }
        let project = layout(config, repo)?.project().project_root.clone();
        self.preview_gc_project(&project, repo, std::time::SystemTime::now(), &[])
    }

    fn execute_gc(
        &self,
        config: &ApfsSubstrateConfig,
        plan: StorageGcPlan,
    ) -> Result<StorageGcReport, ApfsStorageError> {
        if config.store_root != self.config.store_root {
            return Err(ApfsStorageError::InvalidPlan(
                "GC config differs from host storage root",
            ));
        }
        let project = layout(config, plan.repo())?.project().project_root.clone();
        self.execute_gc_plan(&project, plan)
    }
}

/// The identity of the binding at `mount_point`: its symlink's inode, which every new binding
/// replaces, as a remount replaces a kernel mount id.
fn binding_id(mount_point: &Path) -> Result<u64, ApfsStorageError> {
    fs::symlink_metadata(mount_point)
        .map(|metadata| metadata.ino())
        .map_err(|error| io_error("inspect mount binding", mount_point, error))
}

impl DirectoryExecutionHost {
    /// The CA companion beside `image`, which recovery requires to be a private regular file.
    fn recovery_companion(
        &self,
        image: &Path,
        layout_name: &'static str,
    ) -> Result<PathBuf, ApfsStorageError> {
        use std::os::unix::fs::PermissionsExt;
        let companion = companion_path(image);
        self.verify_controller_path(&companion)?;
        let missing = || ApfsStorageError::MissingCaCompanion {
            layout: layout_name,
            image: image.to_owned(),
            companion: companion.clone(),
        };
        match fs::symlink_metadata(&companion) {
            Ok(metadata)
                if metadata.file_type().is_file()
                    && metadata.permissions().mode() & 0o777 == 0o600 =>
            {
                Ok(companion.clone())
            }
            Ok(_) => Err(missing()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Err(missing()),
            Err(error) => Err(io_error("inspect recovery CA companion", &companion, error)),
        }
    }
}

#[cfg(test)]
mod tests;
