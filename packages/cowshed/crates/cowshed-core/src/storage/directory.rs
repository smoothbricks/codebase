//! A workspace store of real directories: the lifecycle host [`super::apfs::ApfsSubstrate`]
//! runs over when a workspace is not a kernel volume.
//!
//! Every payload a native store keeps in an image - a workspace, a checkpoint, a staged
//! restore, a retired workspace, a build volume - is here a real directory, found through
//! exactly the path the layout gives that image. That path always exists, so every path the
//! store's records, locks and recovery derive (`<image>.grants.json`, `<image>.ca.key`,
//! `<image>.lock`, the trash and checkpoint grammars, a build volume's `<id>.dmg` and its hold)
//! means the same thing it means for an image, and the production sidecar, marker, credential,
//! lock and restore-recovery formats are used unchanged.
//!
//! Mounting puts the payload where a mounted volume's root is: the payload directory is renamed
//! onto the mountpoint (the empty directory an unmounted mountpoint is, or no entry yet), and the
//! image path becomes a symlink naming the mountpoint - the durable mount record. Everything that
//! reaches the mounted tree resolves to the mountpoint itself: `getcwd`, `git rev-parse
//! --show-toplevel` and Seatbelt's path rules see the checkout, never the store. Unmounting
//! renames the payload back and leaves an empty mountpoint. A crash between a move's two steps
//! leaves the payload at `<image>.cowshed-move` with the image path already naming the
//! mountpoint; every operation on a payload first completes that move ([`settle`]). A payload
//! moves by rename only, so the store and its mountpoints must share a filesystem; a mount across
//! filesystems refuses.
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

use crate::metadata::{
    DetachedWorkspaceMetadata, MARKER_VERSION, Platform, PublicationState, SIDECAR_VERSION,
    WorkspaceIncarnation, WorkspaceInfoSnapshot, WorkspaceMarker, WorkspaceName, WorkspaceRole,
    sidecar_path,
};
use crate::repository::{OwnedRepoIds, RepoId};
use crate::workspace_credentials::{
    mint_workspace_credentials, validate_private_key, validate_public_workspace_assets,
};
use cowshed_apfs::image::{ImageCapacity, is_image_path};
use cowshed_apfs::store_lock::{ImageLockGuard, LockMode, acquire_image_locks};

use super::apfs::native::{
    CHECKPOINT_FACT_VERSION, CheckpointFactWire, CheckpointPinWire, RESTORE_RECOVERY_FACT_VERSION,
    RestoreRecoveryFactWire, checkout_mountpoint_staging_path, checkpoint_fact_path,
    clone_lineage_from, directory_children, empty_mountpoint, grants_sidecar_missing, io_error,
    metadata_workspace_ref, path_exists, pre_cowshed_path, read_canonical_record, read_dir_paths,
    remove_checkout_staging_mountpoint, restore_adopted_checkout_paths, restore_recovery_fact,
    restore_recovery_fact_path, retire_payload, swap_paths, sync_parent_path,
    warn_missing_session_sidecar, warn_session_image_issue, workspace_checkpoint_facts,
};
use super::apfs::{
    ApfsExecutionHost, ApfsStorageError, ApfsSubstrateConfig, FoundMount, MarkerExpectation,
    MetadataPolicy, PendingAdoption, PendingPublicationFact, PublicationError, ResumableClone,
    ResumedMount, combine_cleanup, companion_path, layout, main_aware_mount_point, volume_key,
};
use super::lifecycle::{
    CheckpointFact, DefragmentOutcome, DerivedWorkspace, KernelMountFact, LifecycleFact,
    LifecycleWorkspace, MountJoin, MountRefusal, MountRefusalReason, MountState, OperationIdentity,
    Pin, ResizeOutcome, RetiredRef, Revision, StorageFact, StorageGcOptions, StorageGcPlan,
    StorageGcReport, SubstrateStats, derive_workspaces,
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
    /// An empty directory: an unmounted mountpoint, or a mounted payload with nothing in it.
    Vacant,
    /// A symbolic link. A directory store never puts one at a mountpoint.
    Link,
    /// Anything else: a directory with entries, a file, a device.
    Occupied,
}

fn mount_point_state(path: &Path) -> Result<MountPointState, ApfsStorageError> {
    let metadata = match fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => {
            return Ok(MountPointState::Absent);
        }
        Err(error) => return Err(io_error("inspect mountpoint", path, error)),
    };
    let file_type = metadata.file_type();
    if file_type.is_symlink() {
        return Ok(MountPointState::Link);
    }
    if file_type.is_dir() && empty_mountpoint(path)? {
        return Ok(MountPointState::Vacant);
    }
    Ok(MountPointState::Occupied)
}

/// Where a payload is.
#[derive(Debug, Eq, PartialEq)]
enum Payload {
    /// At its image path: not mounted.
    Home,
    /// At this mountpoint, which the image path's mount record names.
    Mounted(PathBuf),
}

const MOVE_SUFFIX: &str = ".cowshed-move";

/// The name beside its image path a payload passes through while it moves.
fn move_sibling(image: &Path) -> PathBuf {
    crate::metadata::append_suffix(image, MOVE_SUFFIX)
}

fn is_real_directory(path: &Path) -> Result<bool, ApfsStorageError> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(metadata.file_type().is_dir()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("inspect payload", path, error)),
    }
}

/// Clear the move sibling of `image`: only a stale mount record, the one thing a finished or
/// rolled-back move can leave there. Anything else at that name refuses.
fn clear_move_sibling(sibling: &Path) -> Result<(), ApfsStorageError> {
    match mount_point_state(sibling)? {
        MountPointState::Absent => Ok(()),
        MountPointState::Link => fs::remove_file(sibling)
            .map_err(|error| io_error("remove stale mount record", sibling, error)),
        MountPointState::Vacant | MountPointState::Occupied => {
            Err(ApfsStorageError::Host(format!(
                "payload move name {} holds something cowshed did not leave there",
                sibling.display()
            )))
        }
    }
}

/// Where `image`'s payload is, after completing a move a crash interrupted.
///
/// A mount record whose mountpoint holds no directory, with no payload waiting beside it, names
/// a payload that is gone: refused, never repaired by guessing.
fn settle(image: &Path) -> Result<Payload, ApfsStorageError> {
    let metadata =
        fs::symlink_metadata(image).map_err(|error| io_error("inspect payload", image, error))?;
    let sibling = move_sibling(image);
    if metadata.file_type().is_dir() {
        // An unmount that died after its exchange leaves the old mount record beside the image.
        if mount_point_state(&sibling)? == MountPointState::Link {
            clear_move_sibling(&sibling)?;
        }
        return Ok(Payload::Home);
    }
    if !metadata.file_type().is_symlink() {
        return Err(ApfsStorageError::Host(format!(
            "payload is neither a directory nor a mount record: {}",
            image.display()
        )));
    }
    let mount_point =
        fs::read_link(image).map_err(|error| io_error("read mount record", image, error))?;
    if !mount_point.is_absolute() {
        return Err(ApfsStorageError::Host(format!(
            "mount record {} names the relative path {}",
            image.display(),
            mount_point.display()
        )));
    }
    if is_real_directory(&sibling)? {
        // A mount stopped between its exchange and its rename, or an unmount between its rename
        // and its exchange: the record names the mountpoint and the payload waits beside its
        // image path. Finishing the mount is the one step both converge through.
        match mount_point_state(&mount_point)? {
            MountPointState::Absent | MountPointState::Vacant => {}
            other => {
                return Err(ApfsStorageError::Host(format!(
                    "cannot finish moving {} onto {}: the mountpoint is {other:?}",
                    sibling.display(),
                    mount_point.display()
                )));
            }
        }
        fs::rename(&sibling, &mount_point)
            .map_err(|error| io_error("finish mounting payload", &mount_point, error))?;
        sync_parent_path(&mount_point)?;
        sync_parent_path(image)?;
    }
    if !is_real_directory(&mount_point)? {
        return Err(ApfsStorageError::Host(format!(
            "mount record {} names {}, which holds no payload",
            image.display(),
            mount_point.display()
        )));
    }
    Ok(Payload::Mounted(mount_point))
}

/// The directory `image`'s payload is at now: the image path, or the mountpoint it is mounted at.
fn payload_root(image: &Path) -> Result<PathBuf, ApfsStorageError> {
    Ok(match settle(image)? {
        Payload::Home => image.to_owned(),
        Payload::Mounted(mount_point) => mount_point,
    })
}

/// Where `image`'s payload is mounted, read without changing anything: for fact passes, which
/// hold no workspace lock and must not finish a move another operation is making. A move in
/// flight (the payload waiting at its move sibling) is not a mount yet.
fn observed_mount(image: &Path) -> Result<Option<PathBuf>, ApfsStorageError> {
    let metadata = match fs::symlink_metadata(image) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(io_error("inspect payload", image, error)),
    };
    if !metadata.file_type().is_symlink() || is_real_directory(&move_sibling(image))? {
        return Ok(None);
    }
    let mount_point =
        fs::read_link(image).map_err(|error| io_error("read mount record", image, error))?;
    Ok(is_real_directory(&mount_point)?.then_some(mount_point))
}

/// Where `image`'s payload directory is now, read without changing anything: its image path,
/// its move sibling while it moves, or its mountpoint.
fn observed_payload_root(image: &Path) -> Result<PathBuf, ApfsStorageError> {
    let metadata =
        fs::symlink_metadata(image).map_err(|error| io_error("inspect payload", image, error))?;
    if metadata.file_type().is_dir() {
        return Ok(image.to_owned());
    }
    let sibling = move_sibling(image);
    if is_real_directory(&sibling)? {
        return Ok(sibling);
    }
    observed_mount(image)?.ok_or_else(|| {
        ApfsStorageError::Host(format!("mount record {} names no payload", image.display()))
    })
}

/// Refuse unless `image`'s payload is at its image path: renaming a mount record into the
/// trash or an undo generation would move a symlink and leave the payload behind.
fn require_home(image: &Path) -> Result<(), ApfsStorageError> {
    match settle(image)? {
        Payload::Home => Ok(()),
        Payload::Mounted(mount_point) => Err(ApfsStorageError::Host(format!(
            "{} is still mounted at {}",
            image.display(),
            mount_point.display()
        ))),
    }
}

/// Mount `image`'s payload at `mount_point`. Idempotent where it is already mounted; refuses a
/// payload mounted elsewhere, an occupied mountpoint and a mountpoint on another filesystem.
fn bind(image: &Path, mount_point: &Path) -> Result<(), ApfsStorageError> {
    if !mount_point.is_absolute() {
        return Err(ApfsStorageError::InvalidPlan("mountpoint is not absolute"));
    }
    match settle(image)? {
        Payload::Mounted(current) if current == mount_point => return Ok(()),
        Payload::Mounted(current) => {
            return Err(ApfsStorageError::Host(format!(
                "{} is already mounted at {}",
                image.display(),
                current.display()
            )));
        }
        Payload::Home => {}
    }
    let parent = mount_point
        .parent()
        .ok_or(ApfsStorageError::InvalidPlan("mountpoint has no parent"))?;
    match mount_point_state(mount_point)? {
        MountPointState::Absent => fs::create_dir_all(parent)
            .map_err(|error| io_error("create mountpoint parent", parent, error))?,
        MountPointState::Vacant => {}
        MountPointState::Link => {
            return Err(ApfsStorageError::Host(format!(
                "mountpoint {} is a symbolic link",
                mount_point.display()
            )));
        }
        MountPointState::Occupied => {
            return Err(ApfsStorageError::Host(format!(
                "mountpoint {} is occupied: it is not an empty directory",
                mount_point.display()
            )));
        }
    }
    let store_device = fs::symlink_metadata(image)
        .map_err(|error| io_error("inspect payload", image, error))?
        .dev();
    let mount_device = fs::metadata(parent)
        .map_err(|error| io_error("inspect mountpoint parent", parent, error))?
        .dev();
    if store_device != mount_device {
        return Err(ApfsStorageError::Host(format!(
            "cannot mount {} at {}: a directory payload moves onto its mountpoint by rename, and \
             the store and the mountpoint are on different filesystems",
            image.display(),
            mount_point.display()
        )));
    }
    let sibling = move_sibling(image);
    clear_move_sibling(&sibling)?;
    std::os::unix::fs::symlink(mount_point, &sibling)
        .map_err(|error| io_error("create mount record", &sibling, error))?;
    // One exchange makes the image path the mount record and puts the payload beside it...
    if let Err(primary) = swap_paths(image, &sibling) {
        return combine_cleanup(
            "record mount",
            primary,
            fs::remove_file(&sibling)
                .map_err(|error| io_error("remove unused mount record", &sibling, error)),
        );
    }
    // ...and one rename puts it on the mountpoint, replacing the empty directory there.
    if let Err(error) = fs::rename(&sibling, mount_point) {
        let primary = io_error("mount payload", mount_point, error);
        let rollback = swap_paths(image, &sibling).and_then(|()| {
            fs::remove_file(&sibling)
                .map_err(|error| io_error("remove unused mount record", &sibling, error))
        });
        return combine_cleanup("mount payload", primary, rollback);
    }
    sync_parent_path(mount_point)?;
    sync_parent_path(image)
}

/// Unmount `image`'s payload from `mount_point`. Not mounted is nothing to do; mounted
/// elsewhere refuses.
fn unbind(image: &Path, mount_point: &Path, intent: DetachIntent) -> Result<(), ApfsStorageError> {
    match settle(image)? {
        Payload::Home => Ok(()),
        Payload::Mounted(current) if current != mount_point => {
            Err(ApfsStorageError::Host(format!(
                "refusing to unmount {} from {}: it is mounted at {}",
                image.display(),
                mount_point.display(),
                current.display()
            )))
        }
        Payload::Mounted(_) => move_home(image, mount_point, intent),
    }
}

/// Unmount `image`'s payload from wherever it is mounted.
fn release_mount(image: &Path, intent: DetachIntent) -> Result<(), ApfsStorageError> {
    match settle(image)? {
        Payload::Home => Ok(()),
        Payload::Mounted(mount_point) => move_home(image, &mount_point, intent),
    }
}

/// Every process holding anything inside the tree at `root`, counted as a directory backend
/// must count them: a census taken twice, keeping only the processes both censuses name. A
/// child caught between fork and exec still carries its parent's descriptors — the parent's
/// holds, not the child's — and drops them at exec, so the second census, a full scan later,
/// no longer names it. A process that truly holds the tree is named by both.
pub(super) fn confirmed_tree_holders(
    root: &Path,
) -> Result<Vec<cowshed_os::process::Holder>, ApfsStorageError> {
    let census = |root: &Path| {
        crate::build_volume::nx::tree_holders(root)
            .map_err(|error| io_error("list the processes holding a tree", root, error))
    };
    let first = census(root)?;
    if first.is_empty() {
        return Ok(first);
    }
    let second = census(root)?;
    Ok(first
        .into_iter()
        .filter(|holder| second.iter().any(|confirmed| confirmed.pid == holder.pid))
        .collect())
}

/// Move a mounted payload back to its image path and leave the empty directory an unmounted
/// mountpoint is. A mountpoint keeps no process out of a directory: whoever is inside the tree
/// moves with it. So `WhenIdle` refuses, naming them, while any process holds anything in the
/// tree - the refusal an unforced unmount of a busy volume gives - and `Release` goes ahead.
fn move_home(
    image: &Path,
    mount_point: &Path,
    intent: DetachIntent,
) -> Result<(), ApfsStorageError> {
    if intent == DetachIntent::WhenIdle {
        let open = confirmed_tree_holders(mount_point)?;
        if !open.is_empty() {
            return Err(ApfsStorageError::BuildVolumeHeld {
                image: image.to_owned(),
                hold: crate::metadata::append_suffix(image, ".hold"),
                holds: Vec::new(),
                open,
            });
        }
    }
    let sibling = move_sibling(image);
    clear_move_sibling(&sibling)?;
    fs::rename(mount_point, &sibling)
        .map_err(|error| io_error("unmount payload", mount_point, error))?;
    if let Err(primary) = swap_paths(image, &sibling) {
        let rollback = fs::rename(&sibling, mount_point)
            .map_err(|error| io_error("remount payload", mount_point, error));
        return combine_cleanup("unmount payload", primary, rollback);
    }
    // The exchange left the mount record at the sibling; it is a symlink, never followed.
    fs::remove_file(&sibling)
        .map_err(|error| io_error("remove released mount record", &sibling, error))?;
    fs::create_dir(mount_point)
        .map_err(|error| io_error("leave an empty mountpoint", mount_point, error))?;
    sync_parent_path(mount_point)?;
    sync_parent_path(image)
}

/// The identity of the mount of `image`: its mount record's inode, which every mount replaces,
/// as a remount replaces a kernel mount id.
fn binding_id(image: &Path) -> Result<u64, ApfsStorageError> {
    fs::symlink_metadata(image)
        .map(|metadata| metadata.ino())
        .map_err(|error| io_error("inspect mount record", image, error))
}

/// Bytes allocated to the payload at `image`, from an `lstat` walk: symlinks inside are counted,
/// never followed, and an inode reachable by several names (a hard link) is counted once. The
/// one link followed is a mounted payload's mount record at `image` itself, which names where
/// the payload is. A clone shares blocks with its source on a copy-on-write filesystem; like
/// `st_blocks`, this reports each file's own allocation, not unique host blocks.
pub(crate) fn allocated_tree_bytes(image: &Path) -> io::Result<u64> {
    let root = if fs::symlink_metadata(image)?.file_type().is_symlink() {
        fs::read_link(image)?
    } else {
        image.to_owned()
    };
    tree_usage(&root).map(|usage| usage.allocated)
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
            usage.logical = usage
                .logical
                .checked_add(metadata.len())
                .ok_or_else(overflow)?;
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

    /// No symlink between the store root and `path`. An image path's own final entry is exempt:
    /// while its payload is mounted it is the mount record, a symlink naming the mountpoint, and
    /// [`settle`] validates it before anything reaches the payload through it.
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
            let checked = match path.parent() {
                Some(parent) if is_image_path(path) && parent != self.config.store_root => parent,
                _ => path,
            };
            verify_no_symlinks(&self.config.store_root, checked)?;
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
        Ok(read_canonical_record(repo, workspace, &image)?.map(|metadata| (image, metadata)))
    }

    /// Read pending restores before any sibling sidecar is touched; global maintenance passes
    /// `None`, while named opens pass their explicit observation selection.
    pub(crate) fn pending_publications_for(
        &self,
        repo: &RepoId,
        workspaces: Option<&BTreeSet<WorkspaceName>>,
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
        let main = WorkspaceName::main();
        if workspaces.is_none_or(|names| names.contains(&main))
            && let Some((image, metadata)) = self.find_canonical_image(repo, &main)?
            && metadata.publication_state == PublicationState::PendingFence
            && path_exists(&restore_recovery_fact_path(&image))?
        {
            pending.push(pending_fact(&image, &metadata)?);
        }
        let entries = match workspaces {
            None => read_dir_paths(
                &storage.project().sessions,
                "enumerate pending session payloads",
            )?,
            Some(names) => names
                .iter()
                .filter(|name| !name.is_main())
                .map(|name| {
                    storage
                        .canonical_image(name)
                        .map(|image| image.image().to_owned())
                })
                .collect::<Result<Vec<_>, _>>()?,
        };
        for discovered in discover_session_images(entries) {
            if !path_exists(discovered.path())? {
                continue;
            }
            if grants_sidecar_missing(discovered.path())? {
                warn_missing_session_sidecar(discovered.path());
                continue;
            }
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

    /// Storage-only named observation, independent of all mount and checkpoint records.
    pub fn canonical_metadata(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<DetachedWorkspaceMetadata>, ApfsStorageError> {
        Ok(self
            .find_canonical_image(repo, workspace)?
            .map(|(_, metadata)| metadata))
    }

    /// Derive only this workspace from its canonical record, proven directory binding and checkpoints.
    pub fn workspace_snapshot(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<(DerivedWorkspace, DetachedWorkspaceMetadata)>, ApfsStorageError> {
        let storage = layout(&self.config, repo)?;
        let image = storage.canonical_image(workspace)?.image().to_owned();
        let mount_point = main_aware_mount_point(&self.config, repo, workspace)?;
        let checkpoints = storage.project().checkpoints.join(workspace.as_str());
        let published = || {
            self.canonical_metadata(repo, workspace).map(|record| {
                record.filter(|metadata| metadata.publication_state == PublicationState::Active)
            })
        };
        loop {
            let Some(metadata) = published()? else {
                return Ok(None);
            };
            let derived = (|| {
                let fact = StorageFact {
                    workspace: metadata_workspace_ref(&metadata)?,
                    volume_key: volume_key(repo, workspace),
                };
                let mut mounts = Vec::with_capacity(1);
                if let Some(current) = observed_mount(&image)? {
                    // This read names the workspace, so its own refusal is this read's failure.
                    mounts.push(
                        self.join_payload(
                            &image,
                            current,
                            mount_point.clone(),
                            &fact.workspace,
                            fact.volume_key.clone(),
                        )?
                        .named()?,
                    );
                }
                let mut checkpoint_facts = Vec::new();
                workspace_checkpoint_facts(
                    repo,
                    workspace,
                    Self::checkpoint_payloads(&checkpoints)?,
                    &mut checkpoint_facts,
                )?;
                derive_workspaces([fact], mounts, checkpoint_facts)?
                    .pop()
                    .ok_or(ApfsStorageError::InvalidPlan(
                        "one canonical record derived no workspace",
                    ))
            })();
            if published()?.as_ref() != Some(&metadata) {
                continue;
            }
            return derived.map(|derived| Some((derived, metadata)));
        }
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
        self.marker_judge(tree, repo, workspace, incarnation)
            .is_ok()
    }

    fn marker_judge(
        &self,
        tree: &Path,
        repo: &RepoId,
        workspace: &WorkspaceName,
        incarnation: &WorkspaceIncarnation,
    ) -> Result<(), MountRefusalReason> {
        MarkerExpectation {
            repos: self.config.owned_repo_ids(repo),
            workspace: workspace.clone(),
            incarnation: incarnation.clone(),
        }
        .judge(tree)
    }

    /// Join the payload `image`'s binding names (`current`) to `workspace`, whose mountpoint is
    /// `mount_point`, or say why it is not the workspace's mount. The refusal is a value; whether
    /// it fails anything is the reader's to decide.
    fn join_payload(
        &self,
        image: &Path,
        current: PathBuf,
        mount_point: PathBuf,
        workspace: &LifecycleWorkspace,
        volume_key: String,
    ) -> Result<MountJoin, ApfsStorageError> {
        let verdict = if current == mount_point {
            self.marker_judge(
                &current,
                workspace.repo(),
                workspace.name(),
                workspace.incarnation(),
            )
        } else {
            Err(MountRefusalReason::MountedElsewhere(current))
        };
        Ok(match verdict {
            Ok(()) => MountJoin::Proven(KernelMountFact {
                mount_id: binding_id(image)?,
                volume_key,
            }),
            Err(reason) => MountJoin::Refused {
                volume_key,
                refusal: Box::new(MountRefusal {
                    workspace: workspace.clone(),
                    mount_point,
                    mounted_from: image.display().to_string(),
                    reason,
                }),
            },
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
            let Some(metadata) =
                read_canonical_record(repo, discovered.workspace(), discovered.path())?
            else {
                continue;
            };
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

    /// Whether `path` is a payload of this store mounted there: the marker inside it names a
    /// workspace whose image path is a mount record naming `path`. A tree that merely carries a
    /// marker (a user's checkout with a stale `.cowshed/`) is not one.
    fn mounted_payload_at(&self, path: &Path) -> Result<bool, ApfsStorageError> {
        let Ok(marker) = WorkspaceMarker::read_from(&path.join(WORKSPACE_MARKER_PATH)) else {
            return Ok(false);
        };
        let image = layout(&self.config, &marker.repo_id)?
            .canonical_image(&marker.workspace)?
            .image()
            .to_owned();
        match fs::symlink_metadata(&image) {
            Ok(metadata) if metadata.file_type().is_symlink() => fs::read_link(&image)
                .map(|target| target == path)
                .map_err(|error| io_error("read mount record", &image, error)),
            Ok(_) => Ok(false),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(io_error("inspect payload", &image, error)),
        }
    }

    /// Hand the checkout path over to a mountpoint, retaining the displaced tree at `retained`:
    /// one `RENAME_SWAP` of the original tree with an empty directory, then a rename of the tree
    /// to its retained name. Converges from an interrupted attempt and never moves a mounted main.
    fn vacate_checkout_to(
        &self,
        source_checkout: &Path,
        retained: &Path,
    ) -> Result<(), PublicationError> {
        let staging = checkout_mountpoint_staging_path(source_checkout);
        // A main mounted at the checkout means the handoff already happened. Otherwise a Git
        // checkout cannot be empty: an empty mountpoint is a completed swap only when its
        // displaced nonempty tree exists at a handoff name.
        let already_swapped = if self
            .mounted_payload_at(source_checkout)
            .map_err(PublicationError::rolled_back)?
        {
            true
        } else {
            match mount_point_state(source_checkout).map_err(PublicationError::rolled_back)? {
                MountPointState::Vacant => [staging.as_path(), retained]
                    .into_iter()
                    .try_fold(false, |found, path| {
                        Ok::<_, ApfsStorageError>(
                            found || (path_exists(path)? && !empty_mountpoint(path)?),
                        )
                    })
                    .map_err(PublicationError::rolled_back)?,
                MountPointState::Absent | MountPointState::Link | MountPointState::Occupied => {
                    false
                }
            }
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

    /// Clone the payload `source` - read wherever it is, mounted or not - to the new payload
    /// `destination`: every leaf by `clonefile` (or a reflink, or a copy where neither exists),
    /// until a pass observes the source unchanged. A failed clone leaves no destination.
    fn clone_tree(&self, source: &Path, destination: &Path) -> Result<(), ApfsStorageError> {
        let source = payload_root(source)?;
        let source = source.as_path();
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

    /// Release a payload inside the store as every release is (05_gateway.md, "Deleting released
    /// images"): unmount it if it is mounted (the mount record goes back to being the payload
    /// directory), rename it into its project's `sessions/.deleting` under a name no other entry
    /// has. Nothing is deleted: a directory store starts no deletion child (said once per process),
    /// and a released payload stays in `.deleting`, named by nothing, until its test store goes.
    /// Its records stay for the caller.
    fn delete_payload(&self, image: &Path) -> Result<bool, ApfsStorageError> {
        self.require_store_image(image)?;
        match fs::symlink_metadata(image) {
            Ok(metadata) if metadata.file_type().is_dir() || metadata.file_type().is_symlink() => {
                release_mount(image, DetachIntent::Release)?;
                let entry = self.deleting_entry(image)?;
                let deleting = entry
                    .parent()
                    .expect("an entry lies in its deleting directory");
                fs::create_dir_all(deleting)
                    .map_err(|error| io_error("create the deleting directory", deleting, error))?;
                fs::rename(image, &entry)
                    .map_err(|error| io_error("release a payload to .deleting", image, error))?;
                sync_parent_path(image)?;
                sync_parent_path(&entry)?;
                static SAID: std::sync::Once = std::sync::Once::new();
                SAID.call_once(|| {
                    cowshed_os::eprintln!(
                        "cowshed: a directory store starts no deletion child; its released payloads wait in .deleting"
                    )
                });
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

    /// `<project>/sessions/.deleting/reclaimed-<fresh incarnation>.dmg` for a payload of a project
    /// in this store: a name no other entry has.
    fn deleting_entry(&self, image: &Path) -> Result<PathBuf, ApfsStorageError> {
        let store = &self.config().store_root;
        let outside = || {
            ApfsStorageError::Host(format!(
                "{} is not a payload of a project in the store {}",
                image.display(),
                store.display()
            ))
        };
        let mut parts = image
            .strip_prefix(store)
            .map_err(|_| outside())?
            .components();
        let (Some(owner), Some(repo)) = (parts.next(), parts.next()) else {
            return Err(outside());
        };
        let name = crate::metadata::WorkspaceName::new("reclaimed")
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        let incarnation =
            crate::metadata::WorkspaceIncarnation::new(uuid::Uuid::new_v4().simple().to_string())
                .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        Ok(super::apfs::deleting_image_below(
            &store
                .join(owner)
                .join(repo)
                .join(crate::repository::SESSIONS_DIRECTORY),
            &name,
            &incarnation,
        ))
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
        holding: &crate::api::held_lock::LockHolding,
    ) -> Result<Option<Self::LockGuard>, ApfsStorageError> {
        acquire_image_locks(
            &self.config.store_root,
            images,
            mode,
            &crate::held_lock::ImageLockTaker { holding },
        )
        .map_err(Into::into)
    }

    fn observe(&self, expected: &[LifecycleFact]) -> Result<Vec<LifecycleFact>, ApfsStorageError> {
        let observed =
            |metadata: &DetachedWorkspaceMetadata, topology_revision| LifecycleFact::Exists {
                repo: metadata.repo_id.clone(),
                name: metadata.workspace.clone(),
                incarnation: metadata.workspace_incarnation.clone(),
                revision: Revision::new(metadata.grants.revision),
                topology_revision,
                retired: false,
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

    /// A new empty payload directory at `image`, bound to `mount_point`. A directory has no
    /// capacity: it grows within its filesystem, so `capacity` describes nothing here.
    fn create_attached(
        &self,
        _capacity: ImageCapacity,
        image: &Path,
        mount_point: &Path,
    ) -> Result<Self::Attachment, ApfsStorageError> {
        self.require_store_image(image)?;
        self.verify_controller_path(mount_point)?;
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
        let attachment = DirectoryAttachment::new(image.to_owned());
        self.mount(&attachment, mount_point, MountAccess::ReadWrite, false)?;
        Ok(attachment)
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

    fn require_forkable_image(
        &self,
        _image: &Path,
        _mount: Option<&Path>,
    ) -> Result<(), ApfsStorageError> {
        Err(unsupported("proving a physical image's extent budget"))
    }

    fn validate_pending_clone(
        &self,
        image: &Path,
        expected: &DetachedWorkspaceMetadata,
    ) -> Result<(), ApfsStorageError> {
        self.require_store_image(image)?;
        validate_image(image)?;
        let actual = read_metadata(image)?;
        if actual != *expected || !path_exists(image)? {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "pending clone changed before retirement: {}",
                image.display()
            )));
        }
        Ok(())
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

    /// Unmount the unpublished payload from wherever it is mounted (its adoption's staging
    /// mountpoint, or the checkout), then reclaim it.
    fn discard_pending(&self, image: &Path) -> Result<(), ApfsStorageError> {
        self.require_store_image(image)?;
        if path_exists(image)? {
            release_mount(image, DetachIntent::Release)?;
        }
        self.reclaim_image(image)
    }

    fn unmount_attachment(&self, attachment: &Self::Attachment) -> Result<(), ApfsStorageError> {
        if let Some(mount_point) = attachment.bound() {
            unbind(&attachment.image, &mount_point, DetachIntent::Release)?;
            attachment.set_bound(None);
        }
        Ok(())
    }

    fn copy_tree(&self, source: &Path, destination: &Path) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(destination)?;
        crate::copy::copy_until_quiescent_blocking(source, destination)
            .and_then(|_| crate::copy::verify_tree_blocking(source, destination))
            .map(|report| {
                cowshed_os::eprintln!("cowshed: verified copy {} -> {}; regular-file allocated bytes: source {}, copied {}; content {}",
                source.display(),
                destination.display(),
                report.source_allocated_bytes,
                report.copied_allocated_bytes,
                report.digest.to_hex());
            })
            .map_err(host_error)
    }

    /// Hold the payload `image`: a directory inside the store, at its image path or mounted,
    /// with readable metadata. A pending fence outside staging is a publication nobody may open.
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
        payload_root(image)?;
        Ok(DirectoryAttachment::new(image.to_owned()))
    }

    fn attach_mounted(
        &self,
        image: &Path,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<Self::Attachment, ApfsStorageError> {
        let attachment = self.attach_verified(image)?;
        self.mount(&attachment, mount_point, access, browse)?;
        Ok(attachment)
    }

    /// Mount the attachment's payload at `mount_point` by moving it there. A directory cannot be
    /// mounted read-only: whoever reaches the tree can write it, so a read-only mount is refused
    /// rather than promised. `browse` is a Finder presentation flag with no meaning here.
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
                "{} is already mounted at {}",
                attachment.image.display(),
                bound.display()
            )));
        }
        bind(&attachment.image, mount_point)?;
        attachment.set_bound(Some(mount_point.to_owned()));
        Ok(())
    }

    /// Reuse a surviving mount of `image` at `mount_point`, or mount it there.
    fn attach_and_mount_resumable(
        &self,
        image: &Path,
        mount_point: &Path,
        _workspace: &LifecycleWorkspace,
    ) -> Result<ResumedMount<Self::Attachment>, ApfsStorageError> {
        self.require_store_image(image)?;
        self.verify_controller_path(mount_point)?;
        // A directory payload is either home or bound: there is no attached-but-unmounted state.
        let found = match settle(image)? {
            Payload::Mounted(current) if current == mount_point => FoundMount::Mounted,
            Payload::Mounted(_) | Payload::Home => FoundMount::Detached,
        };
        bind(image, mount_point)?;
        let attachment = DirectoryAttachment::new(image.to_owned());
        attachment.set_bound(Some(mount_point.to_owned()));
        Ok(ResumedMount { attachment, found })
    }

    fn rename_volume(
        &self,
        _attachment: &Self::Attachment,
        _mount_point: &Path,
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
        // Preserve a restored tree's revision; a newly minted tree already has its owned links.
        let copied = match WorkspaceMarker::read_from(&marker_path) {
            Ok(old) => Some(old).filter(|old| {
                forked_from.is_none()
                    && old.repo_id == *workspace.repo()
                    && old.workspace == *workspace.name()
            }),
            Err(crate::metadata::MetadataError::Io { source, .. })
                if source.kind() == io::ErrorKind::NotFound =>
            {
                None
            }
            Err(error) => return Err(ApfsStorageError::MarkerMismatch(error.to_string())),
        };
        let owned_links_revision = copied.map_or(crate::metadata::OWNED_LINKS_REVISION, |old| {
            old.owned_links_revision
        });
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
            owned_links_revision,
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

    /// Release the attachment: move its payload home from where this handle mounted it. A
    /// `WhenIdle` release refuses while any process holds anything in the tree.
    fn detach(
        &self,
        attachment: Self::Attachment,
        intent: DetachIntent,
    ) -> Result<(), ApfsStorageError> {
        if let Some(mount_point) = attachment.bound() {
            unbind(&attachment.image, &mount_point, intent)?;
        }
        Ok(())
    }

    /// A mounted workspace must be mounted at its own mountpoint and carry its own marker;
    /// there are no mount flags to heal.
    fn heal_mount(
        &self,
        workspace: &LifecycleWorkspace,
        mount_point: &Path,
    ) -> Result<(), ApfsStorageError> {
        let canonical = layout(&self.config, workspace.repo())?
            .canonical_image(workspace.name())?
            .image()
            .to_owned();
        let Some(current) = observed_mount(&canonical)? else {
            return Ok(());
        };
        if current != mount_point
            || !self.marker_names(
                &current,
                workspace.repo(),
                workspace.name(),
                workspace.incarnation(),
            )
        {
            return Err(ApfsStorageError::Host(format!(
                "cannot use the mount of {} at {}: {} is its mountpoint, and the tree there must carry the {}/{} marker for incarnation {}",
                canonical.display(),
                current.display(),
                mount_point.display(),
                workspace.repo(),
                workspace.name(),
                workspace.incarnation()
            )));
        }
        Ok(())
    }

    fn mount_settled(
        &self,
        workspace: &LifecycleWorkspace,
        mount_point: &Path,
    ) -> Result<bool, ApfsStorageError> {
        let storage = layout(&self.config, workspace.repo())?;
        let canonical = storage
            .canonical_image(workspace.name())?
            .image()
            .to_owned();
        if observed_mount(&canonical)?.as_deref() != Some(mount_point)
            || !self.marker_names(
                mount_point,
                workspace.repo(),
                workspace.name(),
                workspace.incarnation(),
            )
            || !matches!(
                self.workspace_mount_state(workspace)?,
                MountState::Mounted { .. }
            )
        {
            return Ok(false);
        }
        let Some(target) = crate::build_volume::link::linked(mount_point).map_err(host_error)?
        else {
            return Ok(true);
        };
        let build = crate::build_volume::BuildVolumeLayout::new(storage.project())?;
        let Some(id) = build.volume_at(&target) else {
            return Ok(false);
        };
        Ok(matches!(
            build
                .resolve_link(workspace.name(), &id)
                .map_err(host_error)?,
            crate::build_volume::LinkResolution::Keep
        ) && observed_mount(&build.image(&id))?.as_deref() == Some(build.mount(&id).as_path()))
    }

    /// The workspace's mount, kept: the canonical payload must be mounted, through this handle,
    /// at the workspace's mountpoint. The mount id is the mount record's inode.
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
        if attachment.image != canonical
            || attachment.bound().as_deref() != Some(expected.as_path())
            || settle(&canonical)? != Payload::Mounted(expected.clone())
        {
            return Err(ApfsStorageError::Host(format!(
                "retained attachment of {} is not mounted at {}'s mountpoint {}",
                attachment.image.display(),
                workspace.name(),
                expected.display()
            )));
        }
        binding_id(&canonical)
    }

    fn detach_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        intent: DetachIntent,
    ) -> Result<(), ApfsStorageError> {
        let canonical = layout(&self.config, workspace.repo())?
            .canonical_image(workspace.name())?
            .image()
            .to_owned();
        if !path_exists(&canonical)? {
            return Ok(());
        }
        let mount_point = main_aware_mount_point(&self.config, workspace.repo(), workspace.name())?;
        unbind(&canonical, &mount_point, intent)
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

    /// A directory build volume is a directory: nothing is attached to detach.
    fn detach_owned_build_volumes(&self, _: &LifecycleWorkspace) -> Result<(), ApfsStorageError> {
        Ok(())
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
        if self.mounted_payload_at(checkout)? {
            return Err(ApfsStorageError::InvalidPlan(
                "a payload is still mounted at the checkout a replacement mounts on",
            ));
        }
        match mount_point_state(checkout)? {
            MountPointState::Vacant => Ok(()),
            MountPointState::Absent | MountPointState::Link | MountPointState::Occupied => {
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
        require_home(staged)?;
        require_home(canonical)?;
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
        let source_checkpoint =
            CheckpointLabel::new(Self::checkpoint_label(source_image)?).map_err(host_error)?;
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
        for path in [canonical, undo] {
            require_home(path)?;
        }
        let canonical_sidecar = sidecar_path(canonical);
        let undo_sidecar = sidecar_path(undo);
        let staged_sidecar = sidecar_path(staged);
        let canonical_companion =
            self.recovery_companion(canonical, "restore rollback canonical")?;
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
        // Retirement moves the payload, never its mounted binding record.
        require_home(canonical)?;
        ensure_parent(trash)?;
        self.validate_staged_companion(&companion_path(canonical))?;
        retire_payload(canonical, trash)
    }

    fn release_workspace(
        &self,
        config: &ApfsSubstrateConfig,
        retired: &RetiredRef,
    ) -> Result<(), ApfsStorageError> {
        self.release_canonical_workspace(config, retired)
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
            sync_parent_path(image)?;
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

    fn list(&self, repo: &RepoId) -> Result<Vec<StorageFact>, ApfsStorageError> {
        self.published_facts(repo)
    }

    fn pending_publications(
        &self,
        repo: &RepoId,
    ) -> Result<Vec<PendingPublicationFact>, ApfsStorageError> {
        self.pending_publications_for(repo, None)
    }

    fn workspace_mount_state(
        &self,
        workspace: &LifecycleWorkspace,
    ) -> Result<MountState, ApfsStorageError> {
        match self.workspace_snapshot(workspace.repo(), workspace.name())? {
            Some((derived, _)) if derived.workspace == *workspace => Ok(derived.mount_state),
            _ => Err(ApfsStorageError::InvalidPlan("workspace is not published")),
        }
    }

    /// Every published workspace's payload binding, joined when it is bound at the workspace's
    /// own mountpoint and the tree there carries its marker, refused otherwise, workspace by
    /// workspace.
    fn mounts(&self, repo: &RepoId) -> Result<Vec<MountJoin>, ApfsStorageError> {
        let storage = layout(&self.config, repo)?;
        let mut mounts = Vec::new();
        for fact in self.published_facts(repo)? {
            let canonical = storage
                .canonical_image(fact.workspace.name())?
                .image()
                .to_owned();
            let Some(current) = observed_mount(&canonical)? else {
                continue;
            };
            let mount_point =
                main_aware_mount_point(&self.config, fact.workspace.repo(), fact.workspace.name())?;
            mounts.push(self.join_payload(
                &canonical,
                current,
                mount_point,
                &fact.workspace,
                fact.volume_key,
            )?);
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
            workspace_checkpoint_facts(
                repo,
                &workspace_name,
                Self::checkpoint_payloads(&workspace_directory)?,
                &mut facts,
            )?;
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
        let root = observed_payload_root(image)?;
        let usage = tree_usage(&root).map_err(|error| io_error("measure payload", &root, error))?;
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
            stats.checkpoint_count = stats
                .checkpoint_count
                .checked_add(1)
                .ok_or(ApfsStorageError::InvalidPlan("checkpoint count overflow"))?;
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
        options: StorageGcOptions,
    ) -> Result<StorageGcPlan, ApfsStorageError> {
        if config.store_root != self.config.store_root {
            return Err(ApfsStorageError::InvalidPlan(
                "GC config differs from host storage root",
            ));
        }
        let project = layout(config, repo)?.project().project_root.clone();
        self.preview_gc_project(&project, repo, std::time::SystemTime::now(), &[], options)
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
pub(crate) mod tests;
