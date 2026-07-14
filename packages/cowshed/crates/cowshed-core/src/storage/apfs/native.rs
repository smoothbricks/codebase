use std::collections::BTreeMap;
use std::ffi::{CStr, CString, OsString};
use std::fs::{self, File};
use std::io;
use std::path::{Component, Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering as AtomicOrdering};
use std::sync::mpsc::{self, Sender, SyncSender};
use std::thread;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, PermissionsExt};
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, FromRawFd};

use crate::apfs::{
    ApfsBackend, AttachedImage, CommandRunner, CreateImageRequest, CreatedImage, DetachTarget,
    ImageFormatSelection, MacOsApfsBackend, MountAccess,
};
use crate::copy::TreeCopier;
use crate::metadata::{
    DetachedWorkspaceMetadata, ImageFormat, METADATA_VERSION, Platform, PublicationState,
    WorkspaceIncarnation, WorkspaceMarker, WorkspaceName, WorkspaceRole, sidecar_path,
};
use crate::repository::RepoId;
use crate::workspace_credentials::{
    mint_workspace_credentials, validate_private_key, validate_public_workspace_assets,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::super::lifecycle::{
    CheckpointFact, ExpectedState, KernelMountFact, LifecycleWorkspace, ObservedState,
    OperationIdentity, Pin, Revision, StorageFact, StorageGcCandidate, StorageGcPlan,
    StorageGcReason, StorageGcReport, SubstrateStats,
};
use super::super::{
    CheckpointLabel, WORKSPACE_MARKER_PATH, discover_session_images, verify_no_symlinks,
};
use super::{
    ApfsExecutionHost, ApfsStorageError, ApfsSubstrateConfig, LockMode, MarkerExpectation,
    MetadataPolicy, PendingPublicationFact, PublicationDisposition, PublicationError,
    companion_path, layout, volume_name,
};

const CHECKPOINT_FACT_VERSION: u32 = 1;
const CHECKPOINT_FACT_SUFFIX: &str = ".checkpoint.json";
const SELF_HEALING_STUB: &[u8] = b"cowshed ensure --attach\n";

const ROOT_OPEN_FLAGS: libc::c_int =
    libc::O_RDONLY + libc::O_DIRECTORY + libc::O_NOFOLLOW + libc::O_CLOEXEC;
const DIRECTORY_OPEN_FLAGS: libc::c_int =
    libc::O_RDONLY + libc::O_DIRECTORY + libc::O_NOFOLLOW + libc::O_CLOEXEC;
const LOCK_FILE_OPEN_FLAGS: libc::c_int =
    libc::O_RDWR + libc::O_CREAT + libc::O_NOFOLLOW + libc::O_CLOEXEC;
const WAIT_LOCK_OPERATION: libc::c_int = libc::LOCK_EX;
const TRY_LOCK_OPERATION: libc::c_int = libc::LOCK_EX + libc::LOCK_NB;
const LOCK_FILE_MODE: libc::c_uint = 0o600;
const CONTROLLER_DIRECTORY_MODE: libc::mode_t = 0o700;

fn fd_failed(fd: libc::c_int) -> bool {
    fd == -1
}

fn flock_succeeded(result: libc::c_int) -> bool {
    result == 0
}

fn lock_is_busy(mode: LockMode, kind: io::ErrorKind) -> bool {
    matches!((mode, kind), (LockMode::Try, io::ErrorKind::WouldBlock))
}

fn should_create_directory(fd: libc::c_int, kind: io::ErrorKind) -> bool {
    matches!((fd_failed(fd), kind), (true, io::ErrorKind::NotFound))
}

fn mkdir_failed(result: libc::c_int, kind: io::ErrorKind) -> bool {
    !matches!(
        (fd_failed(result), kind),
        (false, _) | (true, io::ErrorKind::AlreadyExists)
    )
}

fn should_retry_lock(kind: io::ErrorKind) -> bool {
    matches!(kind, io::ErrorKind::Interrupted)
}

pub struct ImageLockGuard {
    _files: Vec<File>,
}

fn path_component(value: &std::ffi::OsStr, path: &Path) -> Result<CString, ApfsStorageError> {
    CString::new(value.as_bytes()).map_err(|_| {
        ApfsStorageError::Host(format!("controller path contains NUL: {}", path.display()))
    })
}

fn open_lock_file(root: &Path, path: &Path) -> Result<File, ApfsStorageError> {
    let relative = path.strip_prefix(root).map_err(|_| {
        ApfsStorageError::Layout(super::super::StorageLayoutError::EscapesStoreRoot)
    })?;
    let mut components = relative.components().peekable();
    if components.peek().is_none() {
        return Err(ApfsStorageError::InvalidPlan("lock path is the store root"));
    }
    let root_name = CString::new(root.as_os_str().as_bytes())
        .map_err(|_| ApfsStorageError::Host("store root contains NUL".to_owned()))?;
    let root_fd = unsafe { libc::open(root_name.as_ptr(), ROOT_OPEN_FLAGS) };
    if fd_failed(root_fd) {
        return Err(io_error(
            "open controller store without following symlinks",
            root,
            io::Error::last_os_error(),
        ));
    }
    let mut directory = unsafe { File::from_raw_fd(root_fd) };
    while let Some(component) = components.next() {
        let Component::Normal(name) = component else {
            return Err(ApfsStorageError::Layout(
                super::super::StorageLayoutError::EscapesStoreRoot,
            ));
        };
        let name = path_component(name, path)?;
        if components.peek().is_none() {
            let fd = unsafe {
                libc::openat(
                    directory.as_raw_fd(),
                    name.as_ptr(),
                    LOCK_FILE_OPEN_FLAGS,
                    LOCK_FILE_MODE,
                )
            };
            if fd_failed(fd) {
                return Err(io_error(
                    "open lifecycle lock without following symlinks",
                    path,
                    io::Error::last_os_error(),
                ));
            }
            return Ok(unsafe { File::from_raw_fd(fd) });
        }
        let mut fd =
            unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), DIRECTORY_OPEN_FLAGS) };
        if should_create_directory(fd, io::Error::last_os_error().kind()) {
            let created = unsafe {
                libc::mkdirat(
                    directory.as_raw_fd(),
                    name.as_ptr(),
                    CONTROLLER_DIRECTORY_MODE,
                )
            };
            if mkdir_failed(created, io::Error::last_os_error().kind()) {
                return Err(io_error(
                    "create controller directory without following symlinks",
                    path,
                    io::Error::last_os_error(),
                ));
            }
            fd =
                unsafe { libc::openat(directory.as_raw_fd(), name.as_ptr(), DIRECTORY_OPEN_FLAGS) };
        }
        if fd_failed(fd) {
            return Err(io_error(
                "open controller directory without following symlinks",
                path,
                io::Error::last_os_error(),
            ));
        }
        directory = unsafe { File::from_raw_fd(fd) };
    }
    Err(ApfsStorageError::InvalidPlan(
        "lock path has no final component",
    ))
}

fn acquire_image_locks(
    root: &Path,
    paths: &[PathBuf],
    mode: LockMode,
) -> Result<Option<ImageLockGuard>, ApfsStorageError> {
    let mut paths = paths.to_vec();
    paths.sort();
    paths.dedup();
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let file = open_lock_file(root, &path)?;
        let operation = match mode {
            LockMode::Wait => WAIT_LOCK_OPERATION,
            LockMode::Try => TRY_LOCK_OPERATION,
        };
        loop {
            let result = unsafe { libc::flock(file.as_raw_fd(), operation) };
            if flock_succeeded(result) {
                files.push(file);
                break;
            }
            let error = io::Error::last_os_error();
            if should_retry_lock(error.kind()) {
                continue;
            }
            if lock_is_busy(mode, error.kind()) {
                return Ok(None);
            }
            return Err(io_error("acquire lifecycle lock", &path, error));
        }
    }
    Ok(Some(ImageLockGuard { _files: files }))
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct CheckpointFactWire {
    version: u32,
    repo_id: RepoId,
    workspace: WorkspaceName,
    label: CheckpointLabel,
    revision: u64,
    pin: String,
}

fn checkpoint_fact_path(image: &Path) -> PathBuf {
    let mut path = image.as_os_str().to_owned();
    path.push(CHECKPOINT_FACT_SUFFIX);
    PathBuf::from(path)
}

fn allocated_file_bytes(metadata: &fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        metadata.blocks().saturating_mul(512)
    }
    #[cfg(not(unix))]
    {
        metadata.len()
    }
}

fn gc_reason_tag(reason: StorageGcReason) -> &'static [u8] {
    match reason {
        StorageGcReason::RetiredWorkspace => b"retired-workspace",
        StorageGcReason::OrphanStagingImage => b"orphan-staging-image",
        StorageGcReason::OrphanStagingMetadata => b"orphan-staging-metadata",
        StorageGcReason::ExpiredCheckpoint => b"expired-checkpoint",
        StorageGcReason::DetachedImageCompaction => b"detached-image-compaction",
    }
}

fn gc_candidate(
    reason: StorageGcReason,
    path: &Path,
    associated_paths: &[PathBuf],
    format: Option<ImageFormat>,
    extra_identity: &[u8],
) -> Result<StorageGcCandidate, ApfsStorageError> {
    let mut hasher = Sha256::new();
    hasher.update(gc_reason_tag(reason));
    hasher.update(path.as_os_str().as_encoded_bytes());
    hasher.update(extra_identity);
    let mut bytes = 0_u64;
    for associated in associated_paths {
        let metadata = match fs::symlink_metadata(associated) {
            Ok(metadata) if metadata.file_type().is_file() => metadata,
            Ok(_) => {
                return Err(ApfsStorageError::Host(format!(
                    "GC candidate path is not a regular file: {}",
                    associated.display()
                )));
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(io_error("inspect GC candidate", associated, error));
            }
        };
        let allocated = allocated_file_bytes(&metadata);
        bytes = bytes
            .checked_add(allocated)
            .ok_or(ApfsStorageError::InvalidPlan(
                "GC candidate byte accounting overflow",
            ))?;
        hasher.update(associated.as_os_str().as_encoded_bytes());
        hasher.update(metadata.len().to_le_bytes());
        hasher.update(allocated.to_le_bytes());
        let modified = metadata
            .modified()
            .map_err(|error| io_error("read GC candidate timestamp", associated, error))?
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        hasher.update(modified.as_secs().to_le_bytes());
        hasher.update(modified.subsec_nanos().to_le_bytes());
        if associated != path {
            hasher.update(
                fs::read(associated)
                    .map_err(|error| io_error("read GC candidate identity", associated, error))?,
            );
        }
    }
    Ok(StorageGcCandidate::new(
        hasher.finalize().into(),
        path.to_owned(),
        bytes,
        reason,
        format,
    ))
}

fn image_gc_paths(image: &Path) -> Vec<PathBuf> {
    vec![
        image.to_owned(),
        sidecar_path(image),
        companion_path(image),
        checkpoint_fact_path(image),
    ]
}

fn pre_cowshed_path(project_root: &Path) -> PathBuf {
    let mut path = project_root.as_os_str().to_owned();
    path.push(".pre-cowshed");
    PathBuf::from(path)
}

macro_rules! sync_parent {
    ($path:expr) => {{
        let path: &Path = $path;
        let parent = path
            .parent()
            .ok_or(ApfsStorageError::InvalidPlan("image path has no parent"))?;
        fs::File::open(parent)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| io_error("sync image directory", parent, error))
    }};
}

fn sync_parent_path(path: &Path) -> Result<(), ApfsStorageError> {
    let parent = path
        .parent()
        .ok_or(ApfsStorageError::InvalidPlan("image path has no parent"))?;
    fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| io_error("sync image directory", parent, error))
}

const MNT_DONTBROWSE: u64 = 0x0010_0000;
const MNT_IGNORE_OWNERS: u64 = 0x0020_0000;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KernelMountSnapshot {
    pub mount_id: u64,
    pub mount_point: PathBuf,
    pub source_device: String,
    flags: u64,
}

impl KernelMountSnapshot {
    pub fn new(
        mount_id: u64,
        mount_point: impl Into<PathBuf>,
        source_device: impl Into<String>,
        nobrowse: bool,
        owners: bool,
    ) -> Self {
        let mut flags = 0;
        if nobrowse {
            flags |= MNT_DONTBROWSE;
        }
        if !owners {
            flags |= MNT_IGNORE_OWNERS;
        }
        Self {
            mount_id,
            mount_point: mount_point.into(),
            source_device: source_device.into(),
            flags,
        }
    }
}

pub trait KernelMountSource: Send + Sync + 'static {
    fn mounts(&self) -> Result<Vec<KernelMountSnapshot>, ApfsStorageError>;
}

pub trait RecoveryMarkerSource: Send + Sync + 'static {
    fn incarnation(&self, image: &Path) -> Result<String, ApfsStorageError>;
}
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemKernelMountSource;

impl KernelMountSource for SystemKernelMountSource {
    fn mounts(&self) -> Result<Vec<KernelMountSnapshot>, ApfsStorageError> {
        system_kernel_mounts()
    }
}

fn canonical_mount_flags(flags: u64) -> bool {
    flags & MNT_IGNORE_OWNERS == 0
}

fn system_kernel_mounts() -> Result<Vec<KernelMountSnapshot>, ApfsStorageError> {
    #[cfg(target_os = "macos")]
    {
        use std::hash::{Hash, Hasher};

        let mut mounts = std::ptr::null_mut();
        let count = unsafe { libc::getmntinfo(&mut mounts, libc::MNT_NOWAIT) };
        if count == 0 {
            return Err(ApfsStorageError::Host(format!(
                "getmntinfo failed: {}",
                io::Error::last_os_error()
            )));
        }
        let entries = unsafe { std::slice::from_raw_parts(mounts, count as usize) };
        entries
            .iter()
            .map(|entry| {
                let bytes = unsafe { CStr::from_ptr(entry.f_mntonname.as_ptr()) }.to_bytes();
                let source_device = unsafe { CStr::from_ptr(entry.f_mntfromname.as_ptr()) }
                    .to_string_lossy()
                    .into_owned();
                let mount_point = PathBuf::from(std::ffi::OsStr::from_bytes(bytes));
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                mount_point.hash(&mut hasher);
                Ok(KernelMountSnapshot {
                    mount_id: hasher.finish(),
                    mount_point,
                    source_device,
                    flags: entry.f_flags as u64,
                })
            })
            .collect()
    }
    #[cfg(not(target_os = "macos"))]
    {
        Ok(Vec::new())
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestoreFailpoint {
    Disabled = 0,
    CanonicalSidecarRollbackFailure = 10,
    AfterUndoSidecar = 1,
    AfterImageSwap = 2,
    AfterUndoRename = 3,
    AfterMetadataPublish = 4,
    AfterMetadataFsync = 5,
    AfterCanonicalImageRename = 6,
    AfterCanonicalSidecarRename = 7,
    CanonicalParentFsyncFailure = 8,
    PersistentCanonicalParentFsyncFailure = 9,
    AfterCanonicalCompanionRename = 11,
    AfterCanonicalParentFsync = 12,
    AfterRestoreImageSwap = 13,
    AfterRestoreUndoImageRename = 14,
    AfterRestoreCanonicalParentFsync = 15,
    AfterRestoreUndoParentFsync = 16,
    AfterStagedMetadataRemoval = 17,
    AfterRestoreMetadataParentFsync = 18,
}

fn directory_children(directory: &Path) -> Result<Vec<PathBuf>, ApfsStorageError> {
    match fs::symlink_metadata(directory) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(metadata) if metadata.file_type().is_symlink() => return Ok(Vec::new()),
        Ok(_) => {
            return Err(ApfsStorageError::Host(format!(
                "recovery path is not a directory: {}",
                directory.display()
            )));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_error("inspect recovery directory", directory, error)),
    }
    let entries = fs::read_dir(directory)
        .map_err(|error| io_error("enumerate recovery directory", directory, error))?;
    entries
        .filter_map(|entry| match entry {
            Ok(entry) => match fs::symlink_metadata(entry.path()) {
                Ok(metadata) if metadata.file_type().is_dir() => Some(Ok(entry.path())),
                Ok(_) => None,
                Err(error) => Some(Err(io_error(
                    "inspect recovery directory",
                    &entry.path(),
                    error,
                ))),
            },
            Err(error) => Some(Err(io_error(
                "read recovery directory entry",
                directory,
                error,
            ))),
        })
        .collect()
}

fn regular_file_children(directory: &Path) -> Result<Vec<PathBuf>, ApfsStorageError> {
    match fs::symlink_metadata(directory) {
        Ok(metadata) if metadata.file_type().is_dir() => {}
        Ok(metadata) if metadata.file_type().is_symlink() => return Ok(Vec::new()),
        Ok(_) => {
            return Err(ApfsStorageError::Host(format!(
                "recovery path is not a directory: {}",
                directory.display()
            )));
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_error("inspect recovery directory", directory, error)),
    }
    let entries = fs::read_dir(directory)
        .map_err(|error| io_error("enumerate recovery directory", directory, error))?;
    entries
        .filter_map(|entry| match entry {
            Ok(entry) => match fs::symlink_metadata(entry.path()) {
                Ok(metadata) if metadata.file_type().is_file() => Some(Ok(entry.path())),
                Ok(_) => None,
                Err(error) => Some(Err(io_error(
                    "inspect recovery directory",
                    &entry.path(),
                    error,
                ))),
            },
            Err(error) => Some(Err(io_error(
                "read recovery directory entry",
                directory,
                error,
            ))),
        })
        .collect()
}

fn staged_image_format(path: &Path) -> Option<ImageFormat> {
    let format = ImageFormat::from_image_path(path).ok()?;
    let stem = path.file_stem()?.to_str()?;
    let (workspace, incarnation) = stem.rsplit_once('-')?;
    WorkspaceName::new(workspace).ok()?;
    WorkspaceIncarnation::new(incarnation).ok()?;
    Some(format)
}
fn is_project_owner_directory(path: &Path) -> bool {
    path.file_name()
        .and_then(|name| name.to_str())
        .is_some_and(|name| {
            !name.starts_with('.')
                && !matches!(name, "mnt" | "caches" | "gateway" | "telemetry" | "run")
        })
}

fn collect_project_directories(store_root: &Path) -> Result<Vec<PathBuf>, ApfsStorageError> {
    let mut projects = Vec::new();
    for owner in directory_children(store_root)? {
        if !is_project_owner_directory(&owner) {
            continue;
        }
        projects.extend(directory_children(&owner)?);
    }
    Ok(projects)
}

fn collect_restore_sidecars(
    project: &Path,
    sidecars: &mut Vec<PathBuf>,
) -> Result<(), ApfsStorageError> {
    for workspace in directory_children(&project.join("checkpoints"))? {
        for entry in fs::read_dir(&workspace)
            .map_err(|error| io_error("enumerate restore sidecars", &workspace, error))?
        {
            let path = entry
                .map_err(|error| io_error("read restore sidecar", &workspace, error))?
                .path();
            let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
                continue;
            };
            if name.starts_with("pre-restore-") && name.ends_with(".grants.json") {
                sidecars.push(path);
            }
        }
    }
    Ok(())
}

fn image_from_sidecar(sidecar: &Path) -> Result<PathBuf, ApfsStorageError> {
    let value = sidecar
        .as_os_str()
        .to_str()
        .and_then(|value| value.strip_suffix(".grants.json"))
        .ok_or_else(|| {
            ApfsStorageError::Host(format!(
                "invalid detached restore sidecar path: {}",
                sidecar.display()
            ))
        })?;
    Ok(PathBuf::from(value))
}

struct MountedAttachment {
    mount_id: u64,
    attachment: AttachedImage,
}

type MountKey = (RepoId, WorkspaceName);

enum RegistryCommand {
    Insert {
        key: MountKey,
        entry: MountedAttachment,
        reply: SyncSender<Result<u64, MountedAttachment>>,
    },
    Remove {
        key: MountKey,
        reply: SyncSender<Option<MountedAttachment>>,
    },
    Restore {
        key: MountKey,
        entry: MountedAttachment,
        reply: SyncSender<Result<(), MountedAttachment>>,
    },
    Drain {
        reply: SyncSender<BTreeMap<MountKey, MountedAttachment>>,
    },
}

struct MountedRegistry {
    sender: Sender<RegistryCommand>,
}

impl MountedRegistry {
    fn start() -> Result<Self, ApfsStorageError> {
        let (sender, receiver) = mpsc::channel();
        thread::Builder::new()
            .name("cowshed-apfs-mount-registry".to_owned())
            .spawn(move || {
                let mut mounted = BTreeMap::<MountKey, MountedAttachment>::new();
                let mut next_mount_id = 1_u64;
                while let Ok(command) = receiver.recv() {
                    match command {
                        RegistryCommand::Insert {
                            key,
                            mut entry,
                            reply,
                        } => match mounted.entry(key) {
                            std::collections::btree_map::Entry::Occupied(_) => {
                                let _ = reply.send(Err(entry));
                            }
                            std::collections::btree_map::Entry::Vacant(slot) => {
                                entry.mount_id = next_mount_id;
                                next_mount_id = next_mount_id.saturating_add(1);
                                let mount_id = entry.mount_id;
                                slot.insert(entry);
                                let _ = reply.send(Ok(mount_id));
                            }
                        },
                        RegistryCommand::Remove { key, reply } => {
                            let _ = reply.send(mounted.remove(&key));
                        }
                        RegistryCommand::Restore { key, entry, reply } => {
                            match mounted.entry(key) {
                                std::collections::btree_map::Entry::Occupied(_) => {
                                    let _ = reply.send(Err(entry));
                                }
                                std::collections::btree_map::Entry::Vacant(slot) => {
                                    slot.insert(entry);
                                    let _ = reply.send(Ok(()));
                                }
                            }
                        }
                        RegistryCommand::Drain { reply } => {
                            let _ = reply.send(std::mem::take(&mut mounted));
                        }
                    }
                }
            })
            .map_err(|error| {
                ApfsStorageError::Host(format!("start APFS mount registry actor failed: {error}"))
            })?;
        Ok(Self { sender })
    }

    fn send(&self, command: RegistryCommand) -> Result<(), ApfsStorageError> {
        self.sender
            .send(command)
            .map_err(|_| ApfsStorageError::Host("APFS mount registry actor stopped".to_owned()))
    }

    fn insert(
        &self,
        key: MountKey,
        entry: MountedAttachment,
    ) -> Result<Result<u64, MountedAttachment>, ApfsStorageError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.send(RegistryCommand::Insert { key, entry, reply })?;
        response.recv().map_err(|_| {
            ApfsStorageError::Host("APFS mount registry insert response was dropped".to_owned())
        })
    }

    fn remove(&self, key: MountKey) -> Result<Option<MountedAttachment>, ApfsStorageError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.send(RegistryCommand::Remove { key, reply })?;
        response.recv().map_err(|_| {
            ApfsStorageError::Host("APFS mount registry remove response was dropped".to_owned())
        })
    }

    fn restore(
        &self,
        key: MountKey,
        entry: MountedAttachment,
    ) -> Result<Result<(), MountedAttachment>, ApfsStorageError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.send(RegistryCommand::Restore { key, entry, reply })?;
        response.recv().map_err(|_| {
            ApfsStorageError::Host("APFS mount registry restore response was dropped".to_owned())
        })
    }

    fn drain(&self) -> Result<BTreeMap<MountKey, MountedAttachment>, ApfsStorageError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.send(RegistryCommand::Drain { reply })?;
        response.recv().map_err(|_| {
            ApfsStorageError::Host("APFS mount registry drain response was dropped".to_owned())
        })
    }
}

/// Real filesystem adapter for [`super::ApfsSubstrate`]. Native image commands are never
/// reconstructed here: every create/clone/attach/fsck/mount/detach/delete/compact operation is
/// delegated to [`MacOsApfsBackend`].
pub struct MacOsApfsExecutionHost<R> {
    backend: MacOsApfsBackend<R>,
    config: ApfsSubstrateConfig,
    mounted: MountedRegistry,
    restore_failpoint: AtomicU8,
    mount_source: Arc<dyn KernelMountSource>,
    recovery_marker_source: Option<Arc<dyn RecoveryMarkerSource>>,
}

impl<R: CommandRunner> MacOsApfsExecutionHost<R> {
    pub fn new(runner: R, config: ApfsSubstrateConfig) -> Result<Self, ApfsStorageError> {
        Self::with_mount_source(runner, config, SystemKernelMountSource)
    }

    pub fn with_mount_source(
        runner: R,
        config: ApfsSubstrateConfig,
        mount_source: impl KernelMountSource,
    ) -> Result<Self, ApfsStorageError> {
        Ok(Self {
            backend: MacOsApfsBackend::new(runner),
            config,
            mounted: MountedRegistry::start()?,
            mount_source: Arc::new(mount_source),
            recovery_marker_source: None,
            restore_failpoint: AtomicU8::new(RestoreFailpoint::Disabled as u8),
        })
    }

    pub fn with_recovery_sources(
        runner: R,
        config: ApfsSubstrateConfig,
        mount_source: impl KernelMountSource,
        recovery_marker_source: impl RecoveryMarkerSource,
    ) -> Result<Self, ApfsStorageError> {
        Ok(Self {
            backend: MacOsApfsBackend::new(runner),
            config,
            mounted: MountedRegistry::start()?,
            mount_source: Arc::new(mount_source),
            recovery_marker_source: Some(Arc::new(recovery_marker_source)),
            restore_failpoint: AtomicU8::new(RestoreFailpoint::Disabled as u8),
        })
    }

    pub fn set_restore_failpoint(&self, failpoint: RestoreFailpoint) {
        self.restore_failpoint
            .store(failpoint as u8, AtomicOrdering::SeqCst);
    }

    fn trip_restore_failpoint(&self, failpoint: RestoreFailpoint) -> Result<(), ApfsStorageError> {
        if self
            .restore_failpoint
            .compare_exchange(
                failpoint as u8,
                RestoreFailpoint::Disabled as u8,
                AtomicOrdering::SeqCst,
                AtomicOrdering::SeqCst,
            )
            .is_ok()
        {
            Err(ApfsStorageError::Host(format!(
                "injected restore failure after {}",
                match failpoint {
                    RestoreFailpoint::Disabled => "disabled",
                    RestoreFailpoint::AfterUndoSidecar => "undo sidecar",
                    RestoreFailpoint::AfterImageSwap => "image and CA companion swap",
                    RestoreFailpoint::AfterUndoRename => "undo rename",
                    RestoreFailpoint::AfterMetadataPublish => "metadata publish",
                    RestoreFailpoint::AfterMetadataFsync => "metadata fsync",
                    RestoreFailpoint::AfterCanonicalImageRename => "canonical image rename",
                    RestoreFailpoint::CanonicalSidecarRollbackFailure => {
                        "canonical sidecar rollback"
                    }
                    RestoreFailpoint::AfterCanonicalSidecarRename => {
                        "canonical sidecar rename"
                    }
                    RestoreFailpoint::CanonicalParentFsyncFailure => {
                        "canonical parent fsync"
                    }
                    RestoreFailpoint::PersistentCanonicalParentFsyncFailure => {
                        "persistent canonical parent fsync"
                    }
                    RestoreFailpoint::AfterCanonicalCompanionRename => {
                        "canonical CA companion rename"
                    }
                    RestoreFailpoint::AfterCanonicalParentFsync => "canonical parent fsync",
                    RestoreFailpoint::AfterRestoreImageSwap => "restore image swap",
                    RestoreFailpoint::AfterRestoreUndoImageRename => {
                        "restore undo image rename"
                    }
                    RestoreFailpoint::AfterRestoreCanonicalParentFsync => {
                        "restore canonical parent fsync"
                    }
                    RestoreFailpoint::AfterRestoreUndoParentFsync => {
                        "restore undo parent fsync"
                    }
                    RestoreFailpoint::AfterStagedMetadataRemoval => {
                        "staged restore metadata removal"
                    }
                    RestoreFailpoint::AfterRestoreMetadataParentFsync => {
                        "restore metadata parent fsync"
                    }
                }
            )))
        } else {
            Ok(())
        }
    }
    fn sync_canonical_parent(&self, canonical: &Path) -> Result<(), ApfsStorageError> {
        if self.restore_failpoint.load(AtomicOrdering::SeqCst)
            == RestoreFailpoint::PersistentCanonicalParentFsyncFailure as u8
        {
            return Err(ApfsStorageError::Host(format!(
                "injected persistent canonical parent fsync failure: {}",
                canonical.display()
            )));
        }
        sync_parent!(canonical)
    }

    pub fn backend(&self) -> &MacOsApfsBackend<R> {
        &self.backend
    }

    fn find_canonical_image(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<(PathBuf, DetachedWorkspaceMetadata)>, ApfsStorageError> {
        let layout = layout(&self.config, repo)?;
        let mut found = None;
        for format in [ImageFormat::Asif, ImageFormat::Sparse] {
            let image = if workspace.is_main() {
                layout.main_image(format)?.image().to_owned()
            } else {
                layout.session_image(workspace, format)?.image().to_owned()
            };
            if image.exists() {
                if found.is_some() {
                    return Err(ApfsStorageError::Host(format!(
                        "duplicate ASIF/SPARSE stem for {repo}/{workspace}"
                    )));
                }
                let metadata = DetachedWorkspaceMetadata::read_for_image(&image)
                    .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
                if metadata.repo_id != *repo || metadata.workspace != *workspace {
                    return Err(ApfsStorageError::Host(format!(
                        "detached metadata identity mismatch for {}",
                        image.display()
                    )));
                }
                found = Some((image, metadata));
            }
        }
        Ok(found)
    }

    fn find_checkpoint(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
        label: &CheckpointLabel,
    ) -> Result<Option<DetachedWorkspaceMetadata>, ApfsStorageError> {
        let layout = layout(&self.config, repo)?;
        let mut found = None;
        for format in [ImageFormat::Asif, ImageFormat::Sparse] {
            let image = layout
                .checkpoint_image(workspace, label, format)?
                .image()
                .to_owned();
            if image.exists() {
                if found.is_some() {
                    return Err(ApfsStorageError::Host(format!(
                        "duplicate checkpoint formats for {repo}/{workspace}/{label}"
                    )));
                }
                found = Some(
                    DetachedWorkspaceMetadata::read_for_image(&image)
                        .map_err(|error| ApfsStorageError::Host(error.to_string()))?,
                );
            }
        }
        Ok(found)
    }

    fn observed_workspace(
        &self,
        metadata: &DetachedWorkspaceMetadata,
        topology_revision: Revision,
        retired: bool,
    ) -> ObservedState {
        ObservedState::Exists {
            repo: metadata.repo_id.clone(),
            name: metadata.workspace.clone(),
            incarnation: metadata.workspace_incarnation.clone(),
            revision: Revision::new(metadata.grants.revision),
            topology_revision,
            retired,
        }
    }
    fn verify_controller_path(&self, path: &Path) -> Result<(), ApfsStorageError> {
        if path == self.config.store_root {
            let metadata = fs::symlink_metadata(path)
                .map_err(|error| io_error("inspect controller store root", path, error))?;
            if metadata.file_type().is_symlink() {
                return Err(ApfsStorageError::Layout(
                    super::super::StorageLayoutError::SymlinkComponent(path.to_owned()),
                ));
            }
        } else if path.starts_with(&self.config.store_root) {
            verify_no_symlinks(&self.config.store_root, path)?;
        }
        Ok(())
    }

    fn ensure_parent(path: &Path) -> Result<(), ApfsStorageError> {
        let parent = path
            .parent()
            .ok_or(ApfsStorageError::InvalidPlan("image path has no parent"))?;
        fs::create_dir_all(parent)
            .map_err(|error| io_error("create image directory", parent, error))
    }

    fn remove_sidecar(image: &Path) -> Result<(), ApfsStorageError> {
        let sidecar = sidecar_path(image);
        match fs::remove_file(&sidecar) {
            Ok(()) => sync_parent!(&sidecar),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io_error("remove detached metadata", &sidecar, error)),
        }
    }

    fn rename_sidecar(
        source_image: &Path,
        destination_image: &Path,
    ) -> Result<(), ApfsStorageError> {
        let source = sidecar_path(source_image);
        if !source.exists() {
            return Ok(());
        }
        let destination = sidecar_path(destination_image);
        Self::ensure_parent(&destination)?;
        if destination.exists() {
            return Err(ApfsStorageError::Host(format!(
                "metadata destination already exists: {}",
                destination.display()
            )));
        }
        fs::rename(&source, &destination)
            .map_err(|error| io_error("rename detached metadata", &source, error))?;
        sync_parent!(&destination)
    }

    fn recovery_companion(
        &self,
        image: &Path,
        layout: &'static str,
    ) -> Result<PathBuf, ApfsStorageError> {
        let companion = companion_path(image);
        self.verify_controller_path(&companion)?;
        let metadata = match fs::symlink_metadata(&companion) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                return Err(ApfsStorageError::MarkerMismatch(format!(
                    "{layout} is missing its CA companion: image={}, companion={}",
                    image.display(),
                    companion.display()
                )));
            }
            Err(error) => {
                return Err(io_error("inspect recovery CA companion", &companion, error));
            }
        };
        if !metadata.file_type().is_file() || metadata.permissions().mode() & 0o777 != 0o600 {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "{layout} has an invalid CA companion: image={}, companion={}",
                image.display(),
                companion.display()
            )));
        }
        Ok(companion)
    }

    fn companions_match(left: &Path, right: &Path) -> Result<bool, ApfsStorageError> {
        let left_metadata = fs::symlink_metadata(left)
            .map_err(|error| io_error("inspect recovery CA identity", left, error))?;
        let right_metadata = fs::symlink_metadata(right)
            .map_err(|error| io_error("inspect recovery CA identity", right, error))?;
        Ok(left_metadata.dev() == right_metadata.dev()
            && left_metadata.ino() == right_metadata.ino())
    }

    fn remove_companion(image: &Path) -> Result<(), ApfsStorageError> {
        let companion = companion_path(image);
        match fs::remove_file(&companion) {
            Ok(()) => sync_parent!(&companion),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io_error("remove CA companion", &companion, error)),
        }
    }

    fn ensure_adopt_mountpoint(&self, mount_point: &Path) -> Result<(), ApfsStorageError> {
        if self
            .mount_source
            .mounts()?
            .iter()
            .any(|mount| mount.mount_point == mount_point)
        {
            return Ok(());
        }
        match fs::metadata(mount_point) {
            Ok(metadata) if !metadata.is_dir() => {
                return Err(ApfsStorageError::Host(format!(
                    "adopt mountpoint is not a directory: {}",
                    mount_point.display()
                )));
            }
            Ok(_) => {
                for entry in fs::read_dir(mount_point)
                    .map_err(|error| io_error("read adopt mountpoint", mount_point, error))?
                {
                    let entry = entry.map_err(|error| {
                        io_error("read adopt mountpoint entry", mount_point, error)
                    })?;
                    if entry.file_name() != ".envrc" {
                        return Err(ApfsStorageError::Host(format!(
                            "adopt mountpoint contains unexpected data: {}",
                            entry.path().display()
                        )));
                    }
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(mount_point)
                    .map_err(|error| io_error("create adopt mountpoint", mount_point, error))?;
            }
            Err(error) => return Err(io_error("inspect adopt mountpoint", mount_point, error)),
        }
        let stub = mount_point.join(".envrc");
        fs::write(&stub, SELF_HEALING_STUB)
            .map_err(|error| io_error("write self-healing mount stub", &stub, error))?;
        fs::File::open(mount_point)
            .and_then(|directory| directory.sync_all())
            .map_err(|error| io_error("sync adopt mountpoint", mount_point, error))?;
        sync_parent!(mount_point)
    }

    fn published_facts(&self, repo: &RepoId) -> Result<Vec<StorageFact>, ApfsStorageError> {
        let layout = layout(&self.config, repo)?;
        let mut facts = Vec::new();
        let main = WorkspaceName::new("main").expect("fixed main name is valid");
        if let Some((_, metadata)) = self.find_canonical_image(repo, &main)?
            && metadata.publication_state == PublicationState::Active
        {
            facts.push(StorageFact {
                workspace: metadata_workspace_ref(&metadata)?,
                volume_name: volume_name(repo, &main),
            });
        }
        let entries = match fs::read_dir(&layout.project().sessions) {
            Ok(entries) => entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .collect::<Vec<_>>(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                return Err(io_error(
                    "enumerate session images",
                    &layout.project().sessions,
                    error,
                ));
            }
        };
        for discovered in discover_session_images(entries)? {
            let metadata = DetachedWorkspaceMetadata::read_for_image(discovered.path())
                .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
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
                volume_name: volume_name(repo, discovered.workspace()),
            });
        }
        Ok(facts)
    }
    fn expected_mount_point(
        &self,
        metadata: &DetachedWorkspaceMetadata,
    ) -> Result<PathBuf, ApfsStorageError> {
        if metadata.workspace.is_main() {
            Ok(self.config.main_mount.clone())
        } else {
            layout(&self.config, &metadata.repo_id)?
                .workspace_mount(&metadata.workspace)
                .map_err(Into::into)
        }
    }

    fn validate_kernel_mount(
        &self,
        mount: &KernelMountSnapshot,
        expected_path: &Path,
        expected_volume: &str,
    ) -> Result<(), ApfsStorageError>
    where
        R: CommandRunner,
    {
        let actual_volume = self.backend.volume_name(&mount.source_device)?;
        if actual_volume != expected_volume {
            return Err(ApfsStorageError::Host(format!(
                "workspace mount source mismatch at {}: expected volume {expected_volume}, device {} resolves to {actual_volume}",
                expected_path.display(),
                mount.source_device
            )));
        }
        if !canonical_mount_flags(mount.flags) {
            return Err(ApfsStorageError::Host(format!(
                "workspace mount has non-canonical flags at {}: expected nobrowse with owners, flags={:#x}",
                expected_path.display(),
                mount.flags
            )));
        }
        Ok(())
    }

    fn kernel_mount_fact(
        &self,
        metadata: &DetachedWorkspaceMetadata,
    ) -> Result<Option<KernelMountFact>, ApfsStorageError> {
        let expected = self.expected_mount_point(metadata)?;
        let Some(mount) = self
            .mount_source
            .mounts()?
            .into_iter()
            .find(|mount| mount.mount_point == expected)
        else {
            return Ok(None);
        };
        let expected_volume = volume_name(&metadata.repo_id, &metadata.workspace);
        self.validate_kernel_mount(&mount, &expected, &expected_volume)?;
        Ok(Some(KernelMountFact {
            mount_id: mount.mount_id,
            volume_name: expected_volume,
        }))
    }

    fn image_is_kernel_mounted(&self, image: &Path) -> Result<bool, ApfsStorageError> {
        let metadata = DetachedWorkspaceMetadata::read_for_image(image)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        Ok(self.kernel_mount_fact(&metadata)?.is_some())
    }

    fn detached_image_incarnation(
        &self,
        image: &Path,
        format: ImageFormat,
        mount_point: &Path,
    ) -> Result<String, ApfsStorageError>
    where
        R: CommandRunner,
    {
        if let Some(source) = &self.recovery_marker_source {
            return source.incarnation(image);
        }
        fs::create_dir_all(mount_point)
            .map_err(|error| io_error("create recovery mount", mount_point, error))?;
        let attachment = self.backend.attach_verified(image, format)?;
        if let Err(primary) =
            self.backend
                .mount(&attachment, mount_point, MountAccess::ReadOnly, false)
        {
            let cleanup = self.backend.detach(&attachment, true).map_err(Into::into);
            return super::combine_cleanup("recovery marker mount", primary.into(), cleanup);
        }
        let incarnation = WorkspaceMarker::read_from(&mount_point.join(WORKSPACE_MARKER_PATH))
            .map(|marker| marker.workspace_incarnation.to_string())
            .map_err(|error| ApfsStorageError::Host(error.to_string()));
        let detach = match self.backend.detach(&attachment, false) {
            Ok(()) => Ok(()),
            Err(primary) => self.backend.detach(&attachment, true).map_err(|cleanup| {
                ApfsStorageError::Cleanup {
                    operation: "force recovery marker detach",
                    primary: Box::new(primary.into()),
                    cleanup: Box::new(cleanup.into()),
                }
            }),
        };
        let cleanup = match (incarnation, detach) {
            (Ok(incarnation), Ok(())) => Ok(incarnation),
            (Err(primary), cleanup) => {
                super::combine_cleanup("recovery marker read", primary, cleanup)
            }
            (Ok(_), Err(error)) => Err(error),
        };
        let remove = fs::remove_dir(mount_point)
            .map_err(|error| io_error("remove recovery mount", mount_point, error));
        match (cleanup, remove) {
            (Ok(incarnation), Ok(())) => Ok(incarnation),
            (Err(primary), cleanup) => {
                super::combine_cleanup("recovery marker cleanup", primary, cleanup)
            }
            (Ok(_), Err(error)) => Err(error),
        }
    }

    fn transient_lock_path(
        project: &Path,
        image: &Path,
        format: ImageFormat,
    ) -> Result<PathBuf, ApfsStorageError> {
        let stem = image
            .file_stem()
            .and_then(|value| value.to_str())
            .ok_or_else(|| {
                ApfsStorageError::Host(format!(
                    "transient image has non-UTF-8 stem: {}",
                    image.display()
                ))
            })?;
        let workspace = match stem.rsplit_once('-') {
            Some((workspace, incarnation))
                if incarnation.len() == 32
                    && incarnation.bytes().all(|byte| byte.is_ascii_hexdigit()) =>
            {
                workspace
            }
            _ => stem,
        };
        let image = if workspace == "main" {
            project.join(format!("main.{}", format.extension()))
        } else {
            project
                .join("sessions")
                .join(format!("{workspace}.{}", format.extension()))
        };
        let mut lock: OsString = image.as_os_str().to_owned();
        lock.push(".lock");
        Ok(PathBuf::from(lock))
    }

    fn recovery_lock(
        &self,
        canonical: &Path,
        held_locks: &[PathBuf],
    ) -> Result<Option<Option<ImageLockGuard>>, ApfsStorageError> {
        let lock = Self::lock_path_for_image(canonical);
        if held_locks.contains(&lock) {
            Ok(Some(None))
        } else {
            acquire_image_locks(&self.config.store_root, &[lock], LockMode::Try)
                .map(|guard| guard.map(Some))
        }
    }

    fn lock_path_for_image(image: &Path) -> PathBuf {
        let mut lock: OsString = image.as_os_str().to_owned();
        lock.push(".lock");
        PathBuf::from(lock)
    }

    fn preview_gc_project(
        &self,
        project: &Path,
        repo: &RepoId,
        observed_at: std::time::SystemTime,
    ) -> Result<StorageGcPlan, ApfsStorageError>
    where
        R: CommandRunner + Send + Sync + 'static,
    {
        let sessions = project.join("sessions");
        let session_entries = match fs::read_dir(&sessions) {
            Ok(entries) => entries
                .map(|entry| {
                    entry
                        .map(|entry| entry.path())
                        .map_err(|error| io_error("read session entry", &sessions, error))
                })
                .collect::<Result<Vec<_>, _>>()?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(error) => return Err(io_error("enumerate sessions", &sessions, error)),
        };
        let mut candidates = Vec::new();
        let mut lock_paths = Vec::new();
        let mut examined = 0_usize;
        let mut retained_pinned = 0_usize;
        let mut retained_recent = 0_usize;

        let trash = sessions.join(super::TRASH_NAMESPACE);
        for path in regular_file_children(&trash)? {
            let Ok(format) = ImageFormat::from_image_path(&path) else {
                continue;
            };
            examined = examined
                .checked_add(1)
                .ok_or(ApfsStorageError::InvalidPlan("GC examined count overflow"))?;
            lock_paths.push(Self::transient_lock_path(project, &path, format)?);
            candidates.push(gc_candidate(
                StorageGcReason::RetiredWorkspace,
                &path,
                &image_gc_paths(&path),
                Some(format),
                &[],
            )?);
        }

        let staging = project.join(super::STAGING_NAMESPACE);
        let mut staged_images = BTreeMap::new();
        let mut staged_sidecars = BTreeMap::new();
        for path in regular_file_children(&staging)? {
            if let Some(format) = staged_image_format(&path) {
                staged_images.insert(path, format);
                continue;
            }
            if path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".grants.json"))
            {
                let image = image_from_sidecar(&path)?;
                if staged_image_format(&image).is_some() {
                    staged_sidecars.insert(image, path);
                }
            }
        }
        for (image, format) in &staged_images {
            examined = examined
                .checked_add(1)
                .ok_or(ApfsStorageError::InvalidPlan("GC examined count overflow"))?;
            if staged_sidecars.contains_key(image) {
                continue;
            }
            lock_paths.push(Self::transient_lock_path(project, image, *format)?);
            candidates.push(gc_candidate(
                StorageGcReason::OrphanStagingImage,
                image,
                &image_gc_paths(image),
                Some(*format),
                &[],
            )?);
        }
        for (image, sidecar) in &staged_sidecars {
            if staged_images.contains_key(image) {
                continue;
            }
            examined = examined
                .checked_add(1)
                .ok_or(ApfsStorageError::InvalidPlan("GC examined count overflow"))?;
            let Some(format) = staged_image_format(image) else {
                continue;
            };
            lock_paths.push(Self::transient_lock_path(project, image, format)?);
            candidates.push(gc_candidate(
                StorageGcReason::OrphanStagingMetadata,
                sidecar,
                std::slice::from_ref(sidecar),
                None,
                &[],
            )?);
        }

        let checkpoint_root = project.join("checkpoints");
        for workspace_directory in directory_children(&checkpoint_root)? {
            let workspace_name = workspace_directory
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| {
                    ApfsStorageError::Host(format!(
                        "checkpoint workspace directory is not UTF-8: {}",
                        workspace_directory.display()
                    ))
                })
                .and_then(|name| {
                    WorkspaceName::new(name)
                        .map_err(|error| ApfsStorageError::Host(error.to_string()))
                })?;
            let mut checkpoints = Vec::new();
            for image in regular_file_children(&workspace_directory)? {
                let Ok(format) = ImageFormat::from_image_path(&image) else {
                    continue;
                };
                lock_paths.push(super::workspace_lock_path(
                    &self.config,
                    repo,
                    &workspace_name,
                    format,
                )?);
                let fact_path = checkpoint_fact_path(&image);
                if !fact_path.exists() {
                    continue;
                }
                let fact: CheckpointFactWire = crate::metadata::read_json(&fact_path)
                    .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
                let expected_label = image
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .ok_or_else(|| {
                        ApfsStorageError::Host(format!(
                            "invalid checkpoint image name: {}",
                            image.display()
                        ))
                    })?;
                if fact.version != CHECKPOINT_FACT_VERSION
                    || fact.repo_id != *repo
                    || fact.workspace != workspace_name
                    || fact.label.as_str() != expected_label
                    || !matches!(fact.pin.as_str(), "pinned" | "automatic")
                {
                    return Err(ApfsStorageError::Host(format!(
                        "checkpoint fact does not match image path: {}",
                        fact_path.display()
                    )));
                }
                let modified = fs::metadata(&image)
                    .and_then(|metadata| metadata.modified())
                    .map_err(|error| io_error("read checkpoint age", &image, error))?;
                checkpoints.push((modified, image, format, fact));
            }
            checkpoints
                .sort_by(|left, right| right.0.cmp(&left.0).then_with(|| right.1.cmp(&left.1)));
            for (index, (modified, image, format, fact)) in checkpoints.into_iter().enumerate() {
                examined = examined
                    .checked_add(1)
                    .ok_or(ApfsStorageError::InvalidPlan("GC examined count overflow"))?;
                if fact.pin == "pinned" {
                    retained_pinned = retained_pinned
                        .checked_add(1)
                        .ok_or(ApfsStorageError::InvalidPlan("GC retained count overflow"))?;
                    continue;
                }
                let younger_than_fourteen_days =
                    observed_at.duration_since(modified).map_or(true, |age| {
                        age < std::time::Duration::from_secs(14 * 24 * 60 * 60)
                    });
                if index < 5 || younger_than_fourteen_days {
                    retained_recent = retained_recent
                        .checked_add(1)
                        .ok_or(ApfsStorageError::InvalidPlan("GC retained count overflow"))?;
                    continue;
                }
                let identity = serde_json::to_vec(&fact)
                    .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
                candidates.push(gc_candidate(
                    StorageGcReason::ExpiredCheckpoint,
                    &image,
                    &image_gc_paths(&image),
                    Some(format),
                    &identity,
                )?);
            }
        }

        for discovered in discover_session_images(session_entries)? {
            if discovered.format() != ImageFormat::Sparse {
                continue;
            }
            let path = discovered.path();
            lock_paths.push(Self::lock_path_for_image(path));
            if self.image_is_kernel_mounted(path)? {
                continue;
            }
            let metadata = DetachedWorkspaceMetadata::read_for_image(path)
                .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
            if metadata.repo_id != *repo
                || metadata.workspace != *discovered.workspace()
                || metadata.image_format != discovered.format()
                || metadata.publication_state != PublicationState::Active
            {
                return Err(ApfsStorageError::MarkerMismatch(format!(
                    "detached metadata disagrees with session image {}",
                    path.display()
                )));
            }
            examined = examined
                .checked_add(1)
                .ok_or(ApfsStorageError::InvalidPlan("GC examined count overflow"))?;
            let identity = serde_json::to_vec(&metadata)
                .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
            candidates.push(gc_candidate(
                StorageGcReason::DetachedImageCompaction,
                path,
                std::slice::from_ref(&path.to_owned()),
                Some(ImageFormat::Sparse),
                &identity,
            )?);
        }

        candidates.sort_by(|left, right| {
            left.path()
                .cmp(right.path())
                .then_with(|| gc_reason_tag(left.reason()).cmp(gc_reason_tag(right.reason())))
        });
        lock_paths.sort();
        lock_paths.dedup();
        Ok(StorageGcPlan::new(
            repo.clone(),
            observed_at,
            candidates,
            lock_paths,
            examined,
            retained_pinned,
            retained_recent,
        ))
    }

    fn execute_gc_plan(
        &self,
        project: &Path,
        plan: StorageGcPlan,
    ) -> Result<StorageGcReport, ApfsStorageError>
    where
        R: CommandRunner + Send + Sync + 'static,
    {
        let _guard =
            acquire_image_locks(&self.config.store_root, plan.lock_paths(), LockMode::Try)?
                .ok_or(ApfsStorageError::GcPlanStale)?;
        let current = self.preview_gc_project(project, plan.repo(), plan.observed_at())?;
        if current != plan {
            return Err(ApfsStorageError::GcPlanStale);
        }
        let mut report = StorageGcReport {
            examined: plan.examined(),
            retained_pinned: plan.retained_pinned(),
            retained_recent: plan.retained_recent(),
            ..StorageGcReport::default()
        };
        for candidate in plan.candidates() {
            match candidate.reason() {
                StorageGcReason::RetiredWorkspace
                | StorageGcReason::OrphanStagingImage
                | StorageGcReason::ExpiredCheckpoint => {
                    let format = candidate.format().ok_or(ApfsStorageError::InvalidPlan(
                        "image GC candidate has no format",
                    ))?;
                    self.reclaim_image(candidate.path(), format)?;
                    report.freed_bytes = report.freed_bytes.checked_add(candidate.bytes()).ok_or(
                        ApfsStorageError::InvalidPlan("GC freed byte accounting overflow"),
                    )?;
                }
                StorageGcReason::OrphanStagingMetadata => {
                    fs::remove_file(candidate.path()).map_err(|error| {
                        io_error("remove orphan staging metadata", candidate.path(), error)
                    })?;
                    sync_parent!(candidate.path())?;
                    report.freed_bytes = report.freed_bytes.checked_add(candidate.bytes()).ok_or(
                        ApfsStorageError::InvalidPlan("GC freed byte accounting overflow"),
                    )?;
                }
                StorageGcReason::DetachedImageCompaction => {
                    let before =
                        allocated_file_bytes(&fs::metadata(candidate.path()).map_err(|error| {
                            io_error("read pre-compaction statistics", candidate.path(), error)
                        })?);
                    self.backend
                        .compact_image(candidate.path(), ImageFormat::Sparse)?;
                    let after =
                        allocated_file_bytes(&fs::metadata(candidate.path()).map_err(|error| {
                            io_error("read post-compaction statistics", candidate.path(), error)
                        })?);
                    report.freed_bytes = report
                        .freed_bytes
                        .checked_add(before.saturating_sub(after))
                        .ok_or(ApfsStorageError::InvalidPlan(
                            "GC freed byte accounting overflow",
                        ))?;
                }
            }
            report.reclaimed = report
                .reclaimed
                .checked_add(1)
                .ok_or(ApfsStorageError::InvalidPlan("GC reclaimed count overflow"))?;
        }
        Ok(report)
    }
}

impl<R: CommandRunner> MacOsApfsExecutionHost<R> {
    pub fn detach_all_reverse(&self) -> Result<(), ApfsStorageError> {
        let mounted = self.mounted.drain()?;
        let mut attachments: Vec<_> = mounted.into_values().collect();
        attachments.sort_by_key(|entry| std::cmp::Reverse(entry.mount_id));
        let mut first_error = None;
        for entry in attachments {
            if let Err(error) = self.backend.detach(&entry.attachment, false)
                && let Err(force_error) = self.backend.detach(&entry.attachment, true)
                && first_error.is_none()
            {
                first_error = Some(ApfsStorageError::Cleanup {
                    operation: "reverse-order APFS teardown",
                    primary: Box::new(error.into()),
                    cleanup: Box::new(force_error.into()),
                });
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }
}

impl<R> ApfsExecutionHost for MacOsApfsExecutionHost<R>
where
    R: CommandRunner + Send + Sync + 'static,
{
    type LockGuard = ImageLockGuard;

    fn lock_images(
        &self,
        paths: &[PathBuf],
        mode: LockMode,
    ) -> Result<Option<Self::LockGuard>, ApfsStorageError> {
        acquire_image_locks(&self.config.store_root, paths, mode)
    }

    type Attachment = AttachedImage;

    fn observe(&self, expected: &[ExpectedState]) -> Result<Vec<ObservedState>, ApfsStorageError> {
        expected
            .iter()
            .map(|fact| match fact {
                ExpectedState::Exists {
                    repo,
                    name,
                    topology_revision,
                    ..
                } => self
                    .find_canonical_image(repo, name)?
                    .map(|(_, metadata)| {
                        self.observed_workspace(&metadata, *topology_revision, false)
                    })
                    .ok_or(ApfsStorageError::Host(format!(
                        "published workspace is missing: {repo}/{name}"
                    ))),
                ExpectedState::Absent {
                    repo,
                    name,
                    topology_revision,
                } => match self.find_canonical_image(repo, name)? {
                    Some((_, metadata)) => {
                        Ok(self.observed_workspace(&metadata, *topology_revision, false))
                    }
                    None => Ok(ObservedState::Absent {
                        repo: repo.clone(),
                        name: name.clone(),
                        topology_revision: *topology_revision,
                    }),
                },
                ExpectedState::Checkpoint {
                    repo,
                    workspace,
                    label,
                    revision,
                } => self
                    .find_checkpoint(repo, workspace, label)?
                    .map(|metadata| ObservedState::Checkpoint {
                        repo: metadata.repo_id,
                        workspace: metadata.workspace,
                        label: label.clone(),
                        revision: Revision::new(metadata.grants.revision),
                    })
                    .ok_or_else(|| {
                        ApfsStorageError::Host(format!(
                            "checkpoint is missing: {repo}/{workspace}/{label}@{}",
                            revision.get()
                        ))
                    }),
            })
            .collect()
    }

    fn resolve_format(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<ImageFormat, ApfsStorageError> {
        let (image, metadata) =
            self.find_canonical_image(repo, workspace)?
                .ok_or(ApfsStorageError::Host(format!(
                    "published workspace is missing: {repo}/{workspace}"
                )))?;
        if metadata.publication_state == PublicationState::PendingFence {
            return Err(ApfsStorageError::PendingPublication(image));
        }
        Ok(metadata.image_format)
    }

    fn create_staged(
        &self,
        request: &CreateImageRequest,
        requested: ImageFormat,
    ) -> Result<CreatedImage, ApfsStorageError> {
        let valid_selection = match requested {
            ImageFormat::Asif => request.image_format == ImageFormatSelection::Auto,
            ImageFormat::Sparse => {
                request.image_format == ImageFormatSelection::Exact(ImageFormat::Sparse)
            }
        };
        if !valid_selection {
            return Err(ApfsStorageError::InvalidPlan(
                "lifecycle image format selection disagrees with the plan",
            ));
        }
        self.verify_controller_path(&request.staged_stem)?;
        Self::ensure_parent(&request.staged_stem)?;
        self.backend
            .create_staged_image(request)
            .map_err(Into::into)
    }

    fn clone_image(
        &self,
        source: &Path,
        destination: &Path,
        format: ImageFormat,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(source)?;
        self.verify_controller_path(destination)?;
        DetachedWorkspaceMetadata::read_for_image(source)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        format
            .validate_path(source)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        format
            .validate_path(destination)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        Self::ensure_parent(destination)?;
        self.backend
            .sync_and_clone(source, destination, format)
            .map_err(Into::into)
    }

    fn copy_tree(&self, source: &Path, destination: &Path) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(destination)?;
        TreeCopier::new(self.backend.runner())
            .copy_until_quiescent(source, destination)
            .map(|_| ())
            .map_err(|error| ApfsStorageError::Host(error.to_string()))
    }

    fn attach_verified(
        &self,
        image: &Path,
        format: ImageFormat,
    ) -> Result<Self::Attachment, ApfsStorageError> {
        self.verify_controller_path(image)?;
        let metadata = DetachedWorkspaceMetadata::read_for_image(image)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        if metadata.image_format != format {
            return Err(ApfsStorageError::Host(format!(
                "detached metadata format disagrees with requested attachment for {}",
                image.display()
            )));
        }
        if metadata.publication_state == PublicationState::PendingFence
            && !image
                .components()
                .any(|component| component.as_os_str() == super::STAGING_NAMESPACE)
        {
            return Err(ApfsStorageError::PendingPublication(image.to_owned()));
        }
        self.backend
            .attach_verified(image, format)
            .map_err(Into::into)
    }

    fn mount(
        &self,
        attachment: &Self::Attachment,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(mount_point)?;
        self.backend
            .mount(attachment, mount_point, access, browse)
            .map_err(Into::into)
    }

    fn chown_volume_root(&self, mount_point: &Path) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(mount_point)?;
        #[cfg(unix)]
        {
            let path = CString::new(mount_point.as_os_str().as_bytes())
                .map_err(|_| ApfsStorageError::Host("mount point contains NUL".to_owned()))?;
            let result = unsafe { libc::chown(path.as_ptr(), libc::getuid(), libc::getgid()) };
            if result == 0 {
                Ok(())
            } else {
                Err(io_error(
                    "chown ASIF volume root",
                    mount_point,
                    io::Error::last_os_error(),
                ))
            }
        }
        #[cfg(not(unix))]
        {
            let _ = mount_point;
            Err(ApfsStorageError::Host(
                "ASIF ownership transfer requires a Unix host".to_owned(),
            ))
        }
    }

    fn rename_volume(&self, mount_point: &Path, volume_name: &str) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(mount_point)?;
        self.backend
            .rename_volume(mount_point, volume_name)
            .map_err(Into::into)
    }

    fn mint_workspace_credentials(
        &self,
        workspace: &LifecycleWorkspace,
        mount_point: &Path,
        private_key_path: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(mount_point)?;
        self.verify_controller_path(private_key_path)?;
        mint_workspace_credentials(workspace, mount_point, private_key_path)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))
    }

    fn write_marker(
        &self,
        mount_point: &Path,
        workspace: &LifecycleWorkspace,
        forked_from: Option<&WorkspaceName>,
        identity: &OperationIdentity,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(mount_point)?;
        let marker = WorkspaceMarker {
            version: METADATA_VERSION,
            repo_id: workspace.repo().clone(),
            project_root: identity.project_root.clone(),
            workspace: workspace.name().clone(),
            workspace_incarnation: workspace.incarnation().clone(),
            role: workspace.role(),
            image_format: workspace.format(),
            base_commit: identity.base_commit.clone(),
            created_at: identity.created_at.clone(),
            forked_from: forked_from.cloned(),
            created_trace: identity.created_trace.clone(),
        };
        marker
            .validate()
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        let marker_path = mount_point.join(WORKSPACE_MARKER_PATH);
        let parent = marker_path.parent().ok_or(ApfsStorageError::InvalidPlan(
            "workspace marker has no parent",
        ))?;
        fs::create_dir_all(parent)
            .map_err(|error| io_error("create marker directory", parent, error))?;
        crate::metadata::write_json(&marker_path, &marker)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))
    }

    fn validate_marker(
        &self,
        mount_point: &Path,
        expected: &MarkerExpectation,
    ) -> Result<(), ApfsStorageError> {
        let marker_path = mount_point.join(WORKSPACE_MARKER_PATH);
        let marker = WorkspaceMarker::read_from(&marker_path)
            .map_err(|error| ApfsStorageError::MarkerMismatch(error.to_string()))?;
        if marker.repo_id == expected.repo
            && marker.workspace == expected.workspace
            && marker.workspace_incarnation == expected.incarnation
            && marker.image_format == expected.format
        {
            validate_public_workspace_assets(
                &marker.repo_id,
                &marker.workspace,
                &marker.workspace_incarnation,
                mount_point,
            )
            .map_err(|error| ApfsStorageError::MarkerMismatch(error.to_string()))
        } else {
            Err(ApfsStorageError::MarkerMismatch(format!(
                "{} does not match {}/{}/{:?}",
                marker_path.display(),
                expected.repo,
                expected.workspace,
                expected.format
            )))
        }
    }

    fn validate_staged_companion(&self, path: &Path) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(path)?;
        validate_private_key(path).map_err(|error| ApfsStorageError::Host(error.to_string()))
    }

    fn heal_mount(
        &self,
        workspace: &LifecycleWorkspace,
        mount_point: &Path,
    ) -> Result<(), ApfsStorageError> {
        let Some(mount) = self
            .mount_source
            .mounts()?
            .into_iter()
            .find(|mount| mount.mount_point == mount_point)
        else {
            return Ok(());
        };
        let expected_volume = volume_name(workspace.repo(), workspace.name());
        let actual_volume = self.backend.volume_name(&mount.source_device)?;
        if actual_volume != expected_volume {
            return Err(ApfsStorageError::Host(format!(
                "refusing to heal unrelated mount at {}: expected volume {expected_volume}, device {} resolves to {actual_volume}",
                mount_point.display(),
                mount.source_device
            )));
        }
        if canonical_mount_flags(mount.flags) {
            return Ok(());
        }

        self.detach_mounted(workspace, true)
    }

    fn detach(&self, attachment: Self::Attachment, force: bool) -> Result<(), ApfsStorageError> {
        self.backend.detach(&attachment, force).map_err(Into::into)
    }

    fn retain_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        attachment: Self::Attachment,
    ) -> Result<u64, ApfsStorageError> {
        let key = (workspace.repo().clone(), workspace.name().clone());
        let entry = MountedAttachment {
            mount_id: 0,
            attachment,
        };
        match self.mounted.insert(key, entry)? {
            Ok(mount_id) => Ok(mount_id),
            Err(entry) => {
                let primary = ApfsStorageError::Host(format!(
                    "workspace is already mounted: {}/{}",
                    workspace.repo(),
                    workspace.name()
                ));
                match self.backend.detach(&entry.attachment, false) {
                    Ok(()) => Err(primary),
                    Err(cleanup) => Err(ApfsStorageError::Cleanup {
                        operation: "reject duplicate mounted attachment",
                        primary: Box::new(primary),
                        cleanup: Box::new(cleanup.into()),
                    }),
                }
            }
        }
    }

    fn detach_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        force: bool,
    ) -> Result<(), ApfsStorageError> {
        let key = (workspace.repo().clone(), workspace.name().clone());
        let Some(entry) = self.mounted.remove(key.clone())? else {
            let Some((_, metadata)) =
                self.find_canonical_image(workspace.repo(), workspace.name())?
            else {
                return Ok(());
            };
            let mount_point = self.expected_mount_point(&metadata)?;
            let Some(mount) = self
                .mount_source
                .mounts()?
                .into_iter()
                .find(|mount| mount.mount_point == mount_point)
            else {
                return Ok(());
            };
            let expected_volume = volume_name(&metadata.repo_id, &metadata.workspace);
            let actual_volume = self.backend.volume_name(&mount.source_device)?;
            if actual_volume != expected_volume {
                return Err(ApfsStorageError::Host(format!(
                    "refusing to detach unrelated mount at {}: expected volume {expected_volume}, device {} resolves to {actual_volume}",
                    mount_point.display(),
                    mount.source_device
                )));
            }
            return match self.backend.detach_target(
                workspace.format(),
                DetachTarget::MountPoint(&mount_point),
                false,
            ) {
                Ok(()) => Ok(()),
                Err(_) if force => self
                    .backend
                    .detach_target(
                        workspace.format(),
                        DetachTarget::MountPoint(&mount_point),
                        true,
                    )
                    .map_err(Into::into),
                Err(error) => Err(error.into()),
            };
        };
        let result = match self.backend.detach(&entry.attachment, false) {
            Ok(()) => return Ok(()),
            Err(_) if force => self.backend.detach(&entry.attachment, true),
            Err(error) => Err(error),
        };
        match result {
            Ok(()) => Ok(()),
            Err(error) => {
                let primary = ApfsStorageError::from(error);
                match self.mounted.restore(key, entry)? {
                    Ok(()) => Err(primary),
                    Err(orphaned) => match self.backend.detach(&orphaned.attachment, true) {
                        Ok(()) => Err(primary),
                        Err(cleanup) => Err(ApfsStorageError::Cleanup {
                            operation: "restore failed mounted attachment",
                            primary: Box::new(primary),
                            cleanup: Box::new(cleanup.into()),
                        }),
                    },
                }
            }
        }
    }

    fn publish_image(&self, staged: &Path, canonical: &Path) -> Result<(), PublicationError> {
        self.verify_controller_path(staged)
            .map_err(PublicationError::rolled_back)?;
        self.verify_controller_path(canonical)
            .map_err(PublicationError::rolled_back)?;
        Self::ensure_parent(canonical).map_err(PublicationError::rolled_back)?;
        let staged_format = ImageFormat::from_image_path(staged).map_err(|error| {
            PublicationError::rolled_back(ApfsStorageError::Host(error.to_string()))
        })?;
        let canonical_format = ImageFormat::from_image_path(canonical).map_err(|error| {
            PublicationError::rolled_back(ApfsStorageError::Host(error.to_string()))
        })?;
        if staged_format != canonical_format {
            return Err(PublicationError::rolled_back(
                ApfsStorageError::InvalidPlan("canonical publication must preserve image format"),
            ));
        }
        if canonical.exists() {
            return Err(PublicationError::forward_only(ApfsStorageError::Host(
                format!("canonical image already exists: {}", canonical.display()),
            )));
        }
        let expected_metadata =
            DetachedWorkspaceMetadata::read_for_image(staged).map_err(|error| {
                PublicationError::rolled_back(ApfsStorageError::Host(error.to_string()))
            })?;
        let staged_companion = companion_path(staged);
        let canonical_companion = companion_path(canonical);
        self.validate_staged_companion(&staged_companion)
            .map_err(PublicationError::rolled_back)?;
        if canonical_companion.exists() {
            return Err(PublicationError::forward_only(ApfsStorageError::Host(
                format!(
                    "canonical CA key already exists: {}",
                    canonical_companion.display()
                ),
            )));
        }
        let canonical_sidecar = sidecar_path(canonical);
        if canonical_sidecar.exists() {
            return Err(PublicationError::forward_only(ApfsStorageError::Host(
                format!(
                    "canonical metadata already exists: {}",
                    canonical_sidecar.display()
                ),
            )));
        }
        let publication = (|| {
            let staged_sidecar = sidecar_path(staged);
            let canonical_sidecar = sidecar_path(canonical);
            fs::rename(&staged_sidecar, &canonical_sidecar).map_err(|error| {
                io_error("publish canonical metadata", &canonical_sidecar, error)
            })?;
            if self.restore_failpoint.load(AtomicOrdering::SeqCst)
                == RestoreFailpoint::CanonicalSidecarRollbackFailure as u8
            {
                return Err(ApfsStorageError::Host(
                    "injected failure after canonical sidecar rename".to_owned(),
                ));
            }
            self.trip_restore_failpoint(RestoreFailpoint::AfterCanonicalSidecarRename)?;
            sync_parent!(&canonical_sidecar)?;
            self.trip_restore_failpoint(RestoreFailpoint::AfterMetadataFsync)?;
            fs::rename(&staged_companion, &canonical_companion).map_err(|error| {
                io_error("publish canonical CA key", &canonical_companion, error)
            })?;
            self.trip_restore_failpoint(RestoreFailpoint::AfterCanonicalCompanionRename)?;
            fs::rename(staged, canonical)
                .map_err(|error| io_error("publish canonical image", canonical, error))?;
            self.trip_restore_failpoint(RestoreFailpoint::AfterCanonicalImageRename)?;
            self.trip_restore_failpoint(RestoreFailpoint::CanonicalParentFsyncFailure)?;
            self.sync_canonical_parent(canonical)?;
            self.trip_restore_failpoint(RestoreFailpoint::AfterCanonicalParentFsync)
        })();
        let Err(primary) = publication else {
            return Ok(());
        };

        let canonical_sidecar = sidecar_path(canonical);
        if canonical.exists() && canonical_sidecar.exists() && canonical_companion.exists() {
            let verified = (|| {
                let actual = DetachedWorkspaceMetadata::read_for_image(canonical)
                    .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
                if actual != expected_metadata || actual.image_format != canonical_format {
                    return Err(ApfsStorageError::Host(format!(
                        "durable canonical publication identity mismatch: {}",
                        canonical.display()
                    )));
                }
                self.sync_canonical_parent(canonical)
            })();
            return match verified {
                Ok(()) => Ok(()),
                Err(cleanup) => Err(PublicationError::forward_only(ApfsStorageError::Cleanup {
                    operation: "verify durable canonical publication",
                    primary: Box::new(primary),
                    cleanup: Box::new(cleanup),
                })),
            };
        }

        if !canonical.exists() && canonical_sidecar.exists() && staged.exists() {
            let staged_sidecar = sidecar_path(staged);
            let inject_rollback_failure = self.restore_failpoint.load(AtomicOrdering::SeqCst)
                == RestoreFailpoint::CanonicalSidecarRollbackFailure as u8;
            if inject_rollback_failure {
                self.restore_failpoint
                    .store(RestoreFailpoint::Disabled as u8, AtomicOrdering::SeqCst);
            }
            let rollback = if inject_rollback_failure {
                Err(ApfsStorageError::Host(
                    "injected canonical sidecar rollback rename failure".to_owned(),
                ))
            } else {
                (|| {
                    if canonical_companion.exists() {
                        fs::rename(&canonical_companion, &staged_companion).map_err(|error| {
                            io_error(
                                "roll back prepublication canonical CA key",
                                &staged_companion,
                                error,
                            )
                        })?;
                    }
                    fs::rename(&canonical_sidecar, &staged_sidecar)
                        .map_err(|error| {
                            io_error(
                                "roll back prepublication canonical metadata",
                                &staged_sidecar,
                                error,
                            )
                        })
                        .and_then(|()| sync_parent!(&staged_sidecar))
                })()
            };
            return match rollback {
                Ok(()) => Err(PublicationError::rolled_back(primary)),
                Err(cleanup) => Err(PublicationError::forward_only(ApfsStorageError::Cleanup {
                    operation: "roll back partial canonical publication",
                    primary: Box::new(primary),
                    cleanup: Box::new(cleanup),
                })),
            };
        }

        if canonical.exists() || canonical_sidecar.exists() || canonical_companion.exists() {
            Err(PublicationError::forward_only(primary))
        } else {
            Err(PublicationError::rolled_back(primary))
        }
    }

    fn publish_adopt(
        &self,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
        staged: &Path,
        canonical: &Path,
    ) -> Result<(), PublicationError> {
        if !source_checkout.is_absolute()
            || pre_cowshed_checkout != pre_cowshed_path(source_checkout)
            || !source_checkout.is_dir()
            || pre_cowshed_checkout.exists()
        {
            return Err(PublicationError::rolled_back(
                ApfsStorageError::InvalidPlan("invalid adopt source or pre-cowshed handoff"),
            ));
        }
        fs::rename(source_checkout, pre_cowshed_checkout).map_err(|error| {
            PublicationError::rolled_back(io_error(
                "move original checkout aside",
                pre_cowshed_checkout,
                error,
            ))
        })?;

        let publication = (|| {
            sync_parent_path(pre_cowshed_checkout).map_err(PublicationError::rolled_back)?;
            fs::create_dir(source_checkout).map_err(|error| {
                PublicationError::rolled_back(io_error(
                    "create canonical mountpoint",
                    source_checkout,
                    error,
                ))
            })?;
            let stub = source_checkout.join(".envrc");
            fs::write(&stub, SELF_HEALING_STUB).map_err(|error| {
                PublicationError::rolled_back(io_error(
                    "write self-healing mount stub",
                    &stub,
                    error,
                ))
            })?;
            fs::File::open(source_checkout)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| {
                    PublicationError::rolled_back(io_error(
                        "sync canonical mountpoint",
                        source_checkout,
                        error,
                    ))
                })?;
            sync_parent_path(source_checkout).map_err(PublicationError::rolled_back)?;
            self.publish_image(staged, canonical)
        })();

        let Err(primary) = publication else {
            return Ok(());
        };
        if primary.disposition() == PublicationDisposition::ForwardOnly {
            return Err(primary);
        }
        let source = primary.into_source();
        let cleanup = (|| {
            let stub = source_checkout.join(".envrc");
            match fs::remove_file(&stub) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => return Err(io_error("remove mount stub", &stub, error)),
            }
            match fs::remove_dir(source_checkout) {
                Ok(()) => {}
                Err(error) if error.kind() == io::ErrorKind::NotFound => {}
                Err(error) => {
                    return Err(io_error(
                        "remove failed canonical mountpoint",
                        source_checkout,
                        error,
                    ));
                }
            }
            fs::rename(pre_cowshed_checkout, source_checkout)
                .map_err(|error| io_error("restore original checkout", source_checkout, error))?;
            sync_parent!(source_checkout)
        })();
        match cleanup {
            Ok(()) => Err(PublicationError::rolled_back(source)),
            Err(cleanup) => Err(PublicationError::forward_only(ApfsStorageError::Cleanup {
                operation: "adopt publication rollback",
                primary: Box::new(source),
                cleanup: Box::new(cleanup),
            })),
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
        workspace
            .format()
            .validate_path(image)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        let preserved = match (policy, source_image) {
            (MetadataPolicy::Preserve | MetadataPolicy::PendingFence, Some(source)) => Some(
                DetachedWorkspaceMetadata::read_for_image(source)
                    .map_err(|error| ApfsStorageError::Host(error.to_string()))?,
            ),
            (MetadataPolicy::Preserve | MetadataPolicy::PendingFence, None) => {
                return Err(ApfsStorageError::InvalidPlan(
                    "preserved metadata requires a source image",
                ));
            }
            (MetadataPolicy::Fresh, _) => None,
        };
        let mut grants = match &preserved {
            Some(metadata) => metadata.grants.clone(),
            None => identity
                .ok_or(ApfsStorageError::InvalidPlan(
                    "fresh metadata requires operation identity",
                ))?
                .grants
                .clone(),
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
        let metadata = DetachedWorkspaceMetadata {
            version: METADATA_VERSION,
            repo_id: workspace.repo().clone(),
            workspace: workspace.name().clone(),
            workspace_incarnation: workspace.incarnation().clone(),
            image_format: workspace.format(),
            platform: Platform::Macos,
            publication_state: match policy {
                MetadataPolicy::PendingFence => PublicationState::PendingFence,
                MetadataPolicy::Fresh | MetadataPolicy::Preserve => PublicationState::Active,
            },
            updated_at,
            grants,
            info_snapshot: preserved.and_then(|metadata| metadata.info_snapshot),
        };
        metadata
            .write_for_image(image)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))
    }

    fn publish_checkpoint_fact(
        &self,
        image: &Path,
        label: &CheckpointLabel,
        revision: Revision,
        pin: Pin,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(image)?;
        let metadata = DetachedWorkspaceMetadata::read_for_image(image)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        let fact = CheckpointFactWire {
            version: CHECKPOINT_FACT_VERSION,
            repo_id: metadata.repo_id,
            workspace: metadata.workspace,
            label: label.clone(),
            revision: revision.get(),
            pin: match pin {
                Pin::Pinned => "pinned",
                Pin::Automatic => "automatic",
            }
            .to_owned(),
        };
        let path = checkpoint_fact_path(image);
        crate::metadata::write_json(&path, &fact)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        sync_parent!(&path)
    }

    fn restore_swap(
        &self,
        staged: &Path,
        canonical: &Path,
        undo: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(staged)?;
        self.verify_controller_path(canonical)?;
        self.verify_controller_path(undo)?;
        Self::ensure_parent(undo)?;
        if undo.exists() {
            return Err(ApfsStorageError::Host(format!(
                "restore undo image already exists: {}",
                undo.display()
            )));
        }
        let canonical_sidecar = sidecar_path(canonical);
        let undo_sidecar = sidecar_path(undo);
        let staged_companion = companion_path(staged);
        let canonical_companion = companion_path(canonical);
        let undo_companion = companion_path(undo);
        DetachedWorkspaceMetadata::read_for_image(staged)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        DetachedWorkspaceMetadata::read_for_image(canonical)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        self.validate_staged_companion(&staged_companion)?;
        self.validate_staged_companion(&canonical_companion)?;
        fs::hard_link(&canonical_sidecar, &undo_sidecar)
            .map_err(|error| io_error("retain restore metadata", &undo_sidecar, error))?;
        if let Err(error) = fs::hard_link(&canonical_companion, &undo_companion) {
            let _ = fs::remove_file(&undo_sidecar);
            return Err(io_error("retain restore CA key", &undo_companion, error));
        }
        self.trip_restore_failpoint(RestoreFailpoint::AfterUndoSidecar)?;
        if let Err(error) = swap_paths(canonical, staged) {
            let _ = fs::remove_file(&undo_sidecar);
            let _ = fs::remove_file(&undo_companion);
            return Err(error);
        }
        self.trip_restore_failpoint(RestoreFailpoint::AfterRestoreImageSwap)?;
        if let Err(primary) = swap_paths(&canonical_companion, &staged_companion) {
            let rollback = swap_paths(canonical, staged);
            let _ = fs::remove_file(&undo_sidecar);
            let _ = fs::remove_file(&undo_companion);
            return super::combine_cleanup("restore CA key swap", primary, rollback);
        }
        self.trip_restore_failpoint(RestoreFailpoint::AfterImageSwap)?;
        if let Err(error) = fs::rename(staged, undo) {
            let primary = io_error("retain restore undo image", undo, error);
            let rollback = swap_paths(&canonical_companion, &staged_companion)
                .and_then(|()| swap_paths(canonical, staged));
            let _ = fs::remove_file(&undo_sidecar);
            let _ = fs::remove_file(&undo_companion);
            return super::combine_cleanup("restore swap", primary, rollback);
        }
        self.trip_restore_failpoint(RestoreFailpoint::AfterRestoreUndoImageRename)?;
        fs::remove_file(&staged_companion)
            .map_err(|error| io_error("remove displaced CA key", &staged_companion, error))?;
        self.trip_restore_failpoint(RestoreFailpoint::AfterUndoRename)?;
        sync_parent!(canonical)?;
        self.trip_restore_failpoint(RestoreFailpoint::AfterRestoreCanonicalParentFsync)?;
        sync_parent!(undo)?;
        self.trip_restore_failpoint(RestoreFailpoint::AfterRestoreUndoParentFsync)
    }

    fn activate_restored_metadata(&self, canonical: &Path) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(canonical)?;
        let mut metadata = DetachedWorkspaceMetadata::read_for_image(canonical)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        if metadata.publication_state != PublicationState::PendingFence {
            return Err(ApfsStorageError::InvalidPlan(
                "only a pending restore publication can be activated",
            ));
        }
        metadata.publication_state = PublicationState::Active;
        metadata
            .write_for_image(canonical)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        sync_parent!(canonical)
    }
    fn publish_restored_metadata(
        &self,
        staged: &Path,
        canonical: &Path,
        workspace: &LifecycleWorkspace,
        revision: Revision,
        source_image: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(staged)?;
        self.verify_controller_path(canonical)?;
        self.verify_controller_path(source_image)?;
        self.publish_metadata(
            canonical,
            workspace,
            revision,
            MetadataPolicy::PendingFence,
            None,
            Some(source_image),
        )?;
        self.trip_restore_failpoint(RestoreFailpoint::AfterMetadataPublish)?;
        sync_parent!(canonical)?;
        self.trip_restore_failpoint(RestoreFailpoint::AfterMetadataFsync)?;
        Self::remove_sidecar(staged)?;
        self.trip_restore_failpoint(RestoreFailpoint::AfterStagedMetadataRemoval)?;
        sync_parent!(canonical)?;
        self.trip_restore_failpoint(RestoreFailpoint::AfterRestoreMetadataParentFsync)
    }

    fn rollback_restore(
        &self,
        canonical: &Path,
        undo: &Path,
        staged: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(canonical)?;
        self.verify_controller_path(undo)?;
        self.verify_controller_path(staged)?;
        let canonical_sidecar = sidecar_path(canonical);
        let undo_sidecar = sidecar_path(undo);
        let staged_sidecar = sidecar_path(staged);
        let canonical_companion =
            self.recovery_companion(canonical, "restore rollback canonical image")?;
        let undo_companion = self.recovery_companion(undo, "restore rollback undo image")?;
        let staged_companion = companion_path(staged);
        if staged_companion.exists() {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "restore rollback staging CA destination is occupied: canonical={}, undo={}, staged={}, companion={}",
                canonical.display(),
                undo.display(),
                staged.display(),
                staged_companion.display()
            )));
        }
        fs::rename(undo, staged)
            .map_err(|error| io_error("stage failed restore image", staged, error))?;
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
        self.backend.delete_image(
            staged,
            ImageFormat::from_image_path(staged)
                .map_err(|error| ApfsStorageError::Host(error.to_string()))?,
        )?;
        Self::remove_sidecar(staged)?;
        Self::remove_companion(staged)?;
        fs::remove_file(&undo_companion)
            .map_err(|error| io_error("remove restore undo CA link", &undo_companion, error))?;
        sync_parent!(canonical)
    }

    fn retire_image(&self, canonical: &Path, trash: &Path) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(canonical)?;
        self.verify_controller_path(trash)?;
        Self::ensure_parent(trash)?;
        let canonical_companion = companion_path(canonical);
        let trash_companion = companion_path(trash);
        if trash.exists() || trash_companion.exists() {
            return Err(ApfsStorageError::Host(format!(
                "retired image artifacts already exist: {}",
                trash.display()
            )));
        }
        self.validate_staged_companion(&canonical_companion)?;
        fs::rename(canonical, trash).map_err(|error| io_error("retire image", trash, error))?;
        if let Err(primary) = Self::rename_sidecar(canonical, trash) {
            return super::combine_cleanup(
                "retire image",
                primary,
                fs::rename(trash, canonical)
                    .map_err(|error| io_error("roll back retirement", canonical, error)),
            );
        }
        if let Err(error) = fs::rename(&canonical_companion, &trash_companion) {
            let primary = io_error("retire CA key", &trash_companion, error);
            let cleanup = Self::rename_sidecar(trash, canonical).and_then(|()| {
                fs::rename(trash, canonical)
                    .map_err(|error| io_error("roll back retirement", canonical, error))
            });
            return super::combine_cleanup("retire image", primary, cleanup);
        }
        sync_parent!(trash)
    }

    fn reclaim_image(&self, image: &Path, format: ImageFormat) -> Result<(), ApfsStorageError> {
        self.verify_controller_path(image)?;
        self.backend.delete_image(image, format)?;
        let companion = companion_path(image);
        match fs::remove_file(&companion) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error("remove CA key", &companion, error)),
        }
        Self::remove_sidecar(image)?;
        let fact = checkpoint_fact_path(image);
        match fs::remove_file(&fact) {
            Ok(()) => sync_parent!(image),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io_error("remove checkpoint fact", &fact, error)),
        }
    }

    fn list(&self, repo: &RepoId) -> Result<Vec<StorageFact>, ApfsStorageError> {
        self.published_facts(repo)
    }

    fn pending_publications(
        &self,
        repo: &RepoId,
    ) -> Result<Vec<PendingPublicationFact>, ApfsStorageError> {
        let storage = layout(&self.config, repo)?;
        let mut pending = Vec::new();
        let main = WorkspaceName::new("main").expect("fixed main name is valid");
        if let Some((image, metadata)) = self.find_canonical_image(repo, &main)?
            && metadata.publication_state == PublicationState::PendingFence
        {
            pending.push(PendingPublicationFact {
                workspace: metadata_workspace_ref(&metadata)?,
                mount_point: self.expected_mount_point(&metadata)?,
                image,
            });
        }
        let entries = match fs::read_dir(&storage.project().sessions) {
            Ok(entries) => entries
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .collect::<Vec<_>>(),
            Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                return Err(io_error(
                    "enumerate pending session images",
                    &storage.project().sessions,
                    error,
                ));
            }
        };
        for discovered in discover_session_images(entries)? {
            let metadata = DetachedWorkspaceMetadata::read_for_image(discovered.path())
                .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
            if metadata.publication_state != PublicationState::PendingFence {
                continue;
            }
            if metadata.repo_id != *repo || metadata.workspace != *discovered.workspace() {
                return Err(ApfsStorageError::Host(format!(
                    "pending detached metadata identity mismatch for {}",
                    discovered.path().display()
                )));
            }
            pending.push(PendingPublicationFact {
                workspace: metadata_workspace_ref(&metadata)?,
                mount_point: self.expected_mount_point(&metadata)?,
                image: discovered.path().to_owned(),
            });
        }
        Ok(pending)
    }

    fn mounts(&self, repo: &RepoId) -> Result<Vec<KernelMountFact>, ApfsStorageError> {
        let kernel = self.mount_source.mounts()?;
        self.published_facts(repo)?
            .into_iter()
            .filter_map(|fact| {
                let expected = if fact.workspace.name().is_main() {
                    Ok(self.config.main_mount.clone())
                } else {
                    layout(&self.config, fact.workspace.repo()).and_then(|layout| {
                        layout
                            .workspace_mount(fact.workspace.name())
                            .map_err(Into::into)
                    })
                };
                let expected = match expected {
                    Ok(path) => path,
                    Err(error) => return Some(Err(error)),
                };
                let mount = kernel.iter().find(|mount| mount.mount_point == expected)?;
                if let Err(error) = self.validate_kernel_mount(mount, &expected, &fact.volume_name)
                {
                    return Some(Err(error));
                }
                Some(Ok(KernelMountFact {
                    mount_id: mount.mount_id,
                    volume_name: fact.volume_name,
                }))
            })
            .collect()
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
                .map_err(|error| ApfsStorageError::Host(error.to_string()))?
            else {
                continue;
            };
            for image in regular_file_children(&workspace_directory)? {
                if ImageFormat::from_image_path(&image).is_err() {
                    continue;
                }
                let fact_path = checkpoint_fact_path(&image);
                if !fact_path.exists() {
                    continue;
                }
                let wire: CheckpointFactWire = crate::metadata::read_json(&fact_path)
                    .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
                let pin = match wire.pin.as_str() {
                    "pinned" => Pin::Pinned,
                    "automatic" => Pin::Automatic,
                    _ => {
                        return Err(ApfsStorageError::Host(format!(
                            "invalid checkpoint pin in {}",
                            fact_path.display()
                        )));
                    }
                };
                let expected_label = image
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .ok_or_else(|| {
                        ApfsStorageError::Host(format!(
                            "invalid checkpoint image name: {}",
                            image.display()
                        ))
                    })?;
                if wire.version != CHECKPOINT_FACT_VERSION
                    || wire.repo_id != *repo
                    || wire.workspace != workspace_name
                    || wire.label.as_str() != expected_label
                {
                    return Err(ApfsStorageError::Host(format!(
                        "checkpoint fact does not match image path: {}",
                        fact_path.display()
                    )));
                }
                facts.push(CheckpointFact {
                    repo: wire.repo_id,
                    workspace: wire.workspace,
                    label: wire.label,
                    revision: Revision::new(wire.revision),
                    pin,
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
        for project in collect_project_directories(&config.store_root)? {
            for directory in [project.as_path(), project.join("sessions").as_path()] {
                for canonical_sidecar in regular_file_children(directory)? {
                    if !canonical_sidecar
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.ends_with(".grants.json"))
                    {
                        continue;
                    }
                    let canonical = image_from_sidecar(&canonical_sidecar)?;
                    if canonical.exists() {
                        self.recovery_companion(&canonical, "published canonical image")?;
                        continue;
                    }
                    let Ok(format) = ImageFormat::from_image_path(&canonical) else {
                        continue;
                    };
                    let metadata = DetachedWorkspaceMetadata::read_for_image(&canonical)
                        .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
                    if metadata.image_format != format {
                        return Err(ApfsStorageError::Host(format!(
                            "canonical sidecar format disagrees with image path: {}",
                            canonical_sidecar.display()
                        )));
                    }
                    let storage = layout(config, &metadata.repo_id)?;
                    let expected = if metadata.workspace.is_main() {
                        storage.main_image(format)?.image().to_owned()
                    } else {
                        storage
                            .session_image(&metadata.workspace, format)?
                            .image()
                            .to_owned()
                    };
                    if storage.project().project_root != project || expected != canonical {
                        continue;
                    }
                    let Some(_guard) = self.recovery_lock(&canonical, held_locks)? else {
                        continue;
                    };
                    if canonical.exists() {
                        self.recovery_companion(&canonical, "published canonical image")?;
                        continue;
                    }
                    let staged = project.join(super::STAGING_NAMESPACE).join(format!(
                        "{}-{}.{}",
                        metadata.workspace,
                        metadata.workspace_incarnation,
                        format.extension()
                    ));
                    let staged_companion = companion_path(&staged);
                    let canonical_companion = companion_path(&canonical);
                    if staged.exists() {
                        match (staged_companion.exists(), canonical_companion.exists()) {
                            (true, false) => {
                                self.recovery_companion(&staged, "staged publication image")?;
                                fs::rename(&staged_companion, &canonical_companion).map_err(
                                    |error| {
                                        io_error(
                                            "complete canonical CA companion publication",
                                            &canonical_companion,
                                            error,
                                        )
                                    },
                                )?;
                                sync_parent!(&canonical_companion)?;
                            }
                            (false, true) => {
                                self.recovery_companion(
                                    &canonical,
                                    "canonical companion before image publication",
                                )?;
                            }
                            (staged_exists, canonical_exists) => {
                                return Err(ApfsStorageError::MarkerMismatch(format!(
                                    "canonical publication has contradictory CA companions: staged_image={}, staged_companion={} (exists={staged_exists}), canonical_image={}, canonical_companion={} (exists={canonical_exists})",
                                    staged.display(),
                                    staged_companion.display(),
                                    canonical.display(),
                                    canonical_companion.display()
                                )));
                            }
                        }
                        let staged_sidecar = sidecar_path(&staged);
                        if staged_sidecar.exists() {
                            let staged_metadata =
                                DetachedWorkspaceMetadata::read_for_image(&staged)
                                    .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
                            if staged_metadata != metadata {
                                return Err(ApfsStorageError::MarkerMismatch(format!(
                                    "staged and canonical metadata disagree during recovery: staged={}, canonical={}",
                                    staged.display(),
                                    canonical.display()
                                )));
                            }
                            fs::remove_file(&staged_sidecar).map_err(|error| {
                                io_error("remove duplicate staged metadata", &staged_sidecar, error)
                            })?;
                        }
                        fs::rename(&staged, &canonical).map_err(|error| {
                            io_error(
                                "complete sidecar-first image publication",
                                &canonical,
                                error,
                            )
                        })?;
                        sync_parent!(&canonical)?;
                    } else if canonical_companion.exists() {
                        return Err(ApfsStorageError::MarkerMismatch(format!(
                            "canonical metadata and CA companion have no publication image: canonical_image={}, canonical_sidecar={}, canonical_companion={}, staged_image={}",
                            canonical.display(),
                            canonical_sidecar.display(),
                            canonical_companion.display(),
                            staged.display()
                        )));
                    } else {
                        fs::remove_file(&canonical_sidecar).map_err(|error| {
                            io_error(
                                "remove orphan canonical metadata",
                                &canonical_sidecar,
                                error,
                            )
                        })?;
                        sync_parent!(&canonical_sidecar)?;
                    }
                }
            }

            let staging = project.join(super::STAGING_NAMESPACE);
            if let Ok(entries) = fs::read_dir(&staging) {
                for entry in entries {
                    let path = entry
                        .map_err(|error| io_error("read staging recovery entry", &staging, error))?
                        .path();
                    if !path
                        .file_name()
                        .and_then(|name| name.to_str())
                        .is_some_and(|name| name.ends_with(".grants.json"))
                    {
                        continue;
                    }
                    let staged = image_from_sidecar(&path)?;
                    let metadata = DetachedWorkspaceMetadata::read_for_image(&staged)
                        .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
                    let storage = layout(config, &metadata.repo_id)?;
                    if storage.project().project_root != project {
                        continue;
                    }
                    let canonical = if metadata.workspace.is_main() {
                        storage
                            .main_image(metadata.image_format)?
                            .image()
                            .to_owned()
                    } else {
                        storage
                            .session_image(&metadata.workspace, metadata.image_format)?
                            .image()
                            .to_owned()
                    };
                    let Some(_guard) = self.recovery_lock(&canonical, held_locks)? else {
                        continue;
                    };
                    let refreshed = DetachedWorkspaceMetadata::read_for_image(&staged)
                        .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
                    if refreshed != metadata {
                        continue;
                    }
                    let restore_undo = storage
                        .project()
                        .checkpoints
                        .join(metadata.workspace.as_str())
                        .join(format!(
                            "pre-restore-{}.{}",
                            metadata.workspace_incarnation,
                            metadata.image_format.extension()
                        ));
                    if sidecar_path(&restore_undo).exists() {
                        continue;
                    }
                    if metadata.workspace.is_main() {
                        let pre_cowshed = pre_cowshed_path(&config.main_mount);
                        if staged.exists() {
                            self.recovery_companion(&staged, "staged main publication image")?;
                        }
                        if staged.exists() {
                            if !canonical.exists() && pre_cowshed.exists() {
                                self.ensure_adopt_mountpoint(&config.main_mount)?;
                                self.publish_image(&staged, &canonical)?;
                            }
                            continue;
                        }
                        if pre_cowshed.exists() {
                            self.ensure_adopt_mountpoint(&config.main_mount)?;
                        }
                    } else if staged.exists() {
                        self.recovery_companion(&staged, "staged session publication image")?;
                        continue;
                    }
                    if canonical.exists() && !sidecar_path(&canonical).exists() {
                        self.recovery_companion(
                            &canonical,
                            "canonical image awaiting metadata recovery",
                        )?;
                        fs::rename(&path, sidecar_path(&canonical)).map_err(|error| {
                            io_error(
                                "complete canonical sidecar publication",
                                &sidecar_path(&canonical),
                                error,
                            )
                        })?;
                        sync_parent!(&canonical)?;
                    }
                }
            }
            let mut undo_sidecars = Vec::new();
            collect_restore_sidecars(&project, &mut undo_sidecars)?;
            undo_sidecars.sort();
            for undo_sidecar in undo_sidecars {
                let undo = image_from_sidecar(&undo_sidecar)?;
                let Some(replacement_incarnation) = undo
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .and_then(|stem| stem.strip_prefix("pre-restore-"))
                else {
                    continue;
                };
                if replacement_incarnation.len() != 32
                    || !replacement_incarnation
                        .bytes()
                        .all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
                {
                    continue;
                }
                let old_metadata = DetachedWorkspaceMetadata::read_for_image(&undo)
                    .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
                let storage = layout(config, &old_metadata.repo_id)?;
                let expected_checkpoint_directory = storage
                    .project()
                    .checkpoints
                    .join(old_metadata.workspace.as_str());
                if storage.project().project_root != project
                    || undo.parent() != Some(expected_checkpoint_directory.as_path())
                {
                    continue;
                }
                let canonical = if old_metadata.workspace.is_main() {
                    storage
                        .main_image(old_metadata.image_format)?
                        .image()
                        .to_owned()
                } else {
                    storage
                        .session_image(&old_metadata.workspace, old_metadata.image_format)?
                        .image()
                        .to_owned()
                };
                let Some(_guard) = self.recovery_lock(&canonical, held_locks)? else {
                    continue;
                };
                let refreshed = DetachedWorkspaceMetadata::read_for_image(&undo)
                    .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
                if refreshed != old_metadata {
                    continue;
                }
                if !canonical.exists() {
                    return Err(ApfsStorageError::MarkerMismatch(format!(
                        "restore recovery has no canonical image: canonical={}, undo_sidecar={}",
                        canonical.display(),
                        undo_sidecar.display()
                    )));
                }
                let staged = storage
                    .project()
                    .project_root
                    .join(super::STAGING_NAMESPACE)
                    .join(format!(
                        "{}-{}.{}",
                        old_metadata.workspace.as_str(),
                        replacement_incarnation,
                        old_metadata.image_format.extension()
                    ));
                let canonical_sidecar = sidecar_path(&canonical);
                let canonical_companion =
                    self.recovery_companion(&canonical, "restore recovery canonical image")?;
                let undo_companion =
                    self.recovery_companion(&undo, "restore recovery undo metadata")?;
                let staged_companion = companion_path(&staged);
                let canonical_metadata = if canonical_sidecar.exists() {
                    Some(
                        DetachedWorkspaceMetadata::read_for_image(&canonical)
                            .map_err(|error| ApfsStorageError::Host(error.to_string()))?,
                    )
                } else {
                    None
                };
                let published = canonical_metadata.as_ref().is_some_and(|metadata| {
                    metadata.workspace_incarnation.as_str() == replacement_incarnation
                });
                if published {
                    if Self::companions_match(&canonical_companion, &undo_companion)? {
                        return Err(ApfsStorageError::MarkerMismatch(format!(
                            "published restore pairs replacement image with old CA key: canonical={}, canonical_companion={}, undo_companion={}",
                            canonical.display(),
                            canonical_companion.display(),
                            undo_companion.display()
                        )));
                    }
                    if undo.exists() {
                        if staged.exists() {
                            self.backend
                                .delete_image(&staged, old_metadata.image_format)?;
                        }
                    } else if staged.exists() {
                        let displaced_companion =
                            self.recovery_companion(&staged, "restore displaced staged image")?;
                        if !Self::companions_match(&displaced_companion, &undo_companion)? {
                            return Err(ApfsStorageError::MarkerMismatch(format!(
                                "displaced restore image has the wrong CA key: staged={}, staged_companion={}, undo_companion={}",
                                staged.display(),
                                displaced_companion.display(),
                                undo_companion.display()
                            )));
                        }
                        fs::rename(&staged, &undo).map_err(|error| {
                            io_error("complete restore undo rename", &undo, error)
                        })?;
                    } else {
                        return Err(ApfsStorageError::MarkerMismatch(format!(
                            "published restore is missing its old image: canonical={}, undo={}, staged={}",
                            canonical.display(),
                            undo.display(),
                            staged.display()
                        )));
                    }
                    Self::remove_sidecar(&staged)?;
                    Self::remove_companion(&staged)?;
                    sync_parent!(&canonical)?;
                    continue;
                }
                if canonical_metadata.as_ref().is_some_and(|metadata| {
                    metadata.workspace_incarnation != old_metadata.workspace_incarnation
                }) {
                    return Err(ApfsStorageError::MarkerMismatch(format!(
                        "restore metadata matches neither generation: canonical={}, undo_sidecar={}, staged={}",
                        canonical.display(),
                        undo_sidecar.display(),
                        staged.display()
                    )));
                }

                let recovery_mount = storage
                    .project()
                    .mount_root
                    .join(super::STAGING_NAMESPACE)
                    .join(format!(
                        "recover-{}-{}",
                        old_metadata.workspace.as_str(),
                        replacement_incarnation
                    ));
                let incarnation = self.detached_image_incarnation(
                    &canonical,
                    old_metadata.image_format,
                    &recovery_mount,
                )?;
                let canonical_is_replacement = if incarnation == replacement_incarnation {
                    true
                } else if incarnation == old_metadata.workspace_incarnation.as_str() {
                    false
                } else {
                    return Err(ApfsStorageError::MarkerMismatch(format!(
                        "restore marker matches neither generation: canonical={}, marker={}, old={}, replacement={}",
                        canonical.display(),
                        incarnation,
                        old_metadata.workspace_incarnation,
                        replacement_incarnation
                    )));
                };
                let canonical_has_old_key =
                    Self::companions_match(&canonical_companion, &undo_companion)?;

                if canonical_is_replacement {
                    if undo.exists() {
                        if staged.exists() {
                            return Err(ApfsStorageError::MarkerMismatch(format!(
                                "restore has both staged and undo old images: staged={}, undo={}",
                                staged.display(),
                                undo.display()
                            )));
                        }
                        fs::rename(&undo, &staged).map_err(|error| {
                            io_error("stage interrupted restore undo", &staged, error)
                        })?;
                    } else if !staged.exists() {
                        return Err(ApfsStorageError::MarkerMismatch(format!(
                            "restore cannot roll back without old image: canonical={}, staged={}, undo={}",
                            canonical.display(),
                            staged.display(),
                            undo.display()
                        )));
                    }
                    if canonical_has_old_key {
                        let staged_key =
                            self.recovery_companion(&staged, "restore replacement staged image")?;
                        if Self::companions_match(&staged_key, &undo_companion)? {
                            return Err(ApfsStorageError::MarkerMismatch(format!(
                                "restore lost replacement CA key: canonical_companion={}, staged_companion={}, undo_companion={}",
                                canonical_companion.display(),
                                staged_key.display(),
                                undo_companion.display()
                            )));
                        }
                        swap_paths(&canonical, &staged)?;
                    } else {
                        if !staged_companion.exists() {
                            fs::hard_link(&undo_companion, &staged_companion).map_err(|error| {
                                io_error("stage retained old CA key", &staged_companion, error)
                            })?;
                        }
                        let staged_key =
                            self.recovery_companion(&staged, "restore displaced staged image")?;
                        if !Self::companions_match(&staged_key, &undo_companion)? {
                            return Err(ApfsStorageError::MarkerMismatch(format!(
                                "restore cannot identify old CA key: canonical_companion={}, staged_companion={}, undo_companion={}",
                                canonical_companion.display(),
                                staged_key.display(),
                                undo_companion.display()
                            )));
                        }
                        swap_paths(&canonical, &staged)?;
                        swap_paths(&canonical_companion, &staged_companion)?;
                    }
                } else {
                    if undo.exists() {
                        return Err(ApfsStorageError::MarkerMismatch(format!(
                            "old canonical restore layout still has undo image: canonical={}, undo={}",
                            canonical.display(),
                            undo.display()
                        )));
                    }
                    if !canonical_has_old_key {
                        if !staged_companion.exists() {
                            fs::hard_link(&undo_companion, &staged_companion).map_err(|error| {
                                io_error("stage retained old CA key", &staged_companion, error)
                            })?;
                        }
                        let staged_key =
                            self.recovery_companion(&staged, "restore old staged CA key")?;
                        if !Self::companions_match(&staged_key, &undo_companion)? {
                            return Err(ApfsStorageError::MarkerMismatch(format!(
                                "old image has no recoverable old CA key: canonical={}, canonical_companion={}, staged_companion={}, undo_companion={}",
                                canonical.display(),
                                canonical_companion.display(),
                                staged_key.display(),
                                undo_companion.display()
                            )));
                        }
                        swap_paths(&canonical_companion, &staged_companion)?;
                    } else if staged.exists() {
                        let staged_key =
                            self.recovery_companion(&staged, "restore replacement staged image")?;
                        if Self::companions_match(&staged_key, &undo_companion)? {
                            return Err(ApfsStorageError::MarkerMismatch(format!(
                                "old and replacement images share old CA key: canonical={}, staged={}, undo_companion={}",
                                canonical.display(),
                                staged.display(),
                                undo_companion.display()
                            )));
                        }
                    }
                }

                if staged.exists() {
                    self.backend
                        .delete_image(&staged, old_metadata.image_format)?;
                }
                Self::remove_sidecar(&staged)?;
                Self::remove_companion(&staged)?;
                if canonical_sidecar.exists() {
                    fs::remove_file(&undo_sidecar).map_err(|error| {
                        io_error("remove redundant restore metadata", &undo_sidecar, error)
                    })?;
                } else {
                    fs::rename(&undo_sidecar, &canonical_sidecar).map_err(|error| {
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
                sync_parent!(&canonical)?;
            }
        }
        Ok(())
    }

    fn stats(
        &self,
        workspace: &LifecycleWorkspace,
        image: &Path,
    ) -> Result<SubstrateStats, ApfsStorageError> {
        workspace
            .format()
            .validate_path(image)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        let detached = DetachedWorkspaceMetadata::read_for_image(image)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        if detached.publication_state == PublicationState::PendingFence {
            return Err(ApfsStorageError::PendingPublication(image.to_owned()));
        }
        if detached.repo_id != *workspace.repo()
            || detached.workspace != *workspace.name()
            || detached.workspace_incarnation != *workspace.incarnation()
            || detached.image_format != workspace.format()
        {
            return Err(ApfsStorageError::MarkerMismatch(format!(
                "detached metadata disagrees with active image {}",
                image.display()
            )));
        }
        let metadata =
            fs::metadata(image).map_err(|error| io_error("read image statistics", image, error))?;
        let allocated_bytes = allocated_file_bytes(&metadata);
        let storage = layout(&self.config, workspace.repo())?;
        let checkpoint_directory = storage
            .project()
            .checkpoints
            .join(workspace.name().as_str());
        let mut checkpoint_count = 0_u64;
        let mut checkpoint_bytes = 0_u64;
        let mut pinned_checkpoint_bytes = 0_u64;
        for checkpoint_image in regular_file_children(&checkpoint_directory)? {
            if ImageFormat::from_image_path(&checkpoint_image).is_err() {
                continue;
            }
            let fact_path = checkpoint_fact_path(&checkpoint_image);
            let fact: CheckpointFactWire = crate::metadata::read_json(&fact_path)
                .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
            let expected_label = checkpoint_image
                .file_stem()
                .and_then(|stem| stem.to_str())
                .ok_or_else(|| {
                    ApfsStorageError::Host(format!(
                        "invalid checkpoint image name: {}",
                        checkpoint_image.display()
                    ))
                })?;
            if fact.version != CHECKPOINT_FACT_VERSION
                || fact.repo_id != *workspace.repo()
                || fact.workspace != *workspace.name()
                || fact.label.as_str() != expected_label
            {
                return Err(ApfsStorageError::Host(format!(
                    "checkpoint fact does not match image path: {}",
                    fact_path.display()
                )));
            }
            let bytes =
                allocated_file_bytes(&fs::metadata(&checkpoint_image).map_err(|error| {
                    io_error("read checkpoint statistics", &checkpoint_image, error)
                })?);
            checkpoint_count = checkpoint_count
                .checked_add(1)
                .ok_or(ApfsStorageError::InvalidPlan("checkpoint count overflow"))?;
            checkpoint_bytes =
                checkpoint_bytes
                    .checked_add(bytes)
                    .ok_or(ApfsStorageError::InvalidPlan(
                        "checkpoint byte accounting overflow",
                    ))?;
            match fact.pin.as_str() {
                "pinned" => {
                    pinned_checkpoint_bytes = pinned_checkpoint_bytes.checked_add(bytes).ok_or(
                        ApfsStorageError::InvalidPlan("pinned checkpoint byte accounting overflow"),
                    )?;
                }
                "automatic" => {}
                _ => {
                    return Err(ApfsStorageError::Host(format!(
                        "invalid checkpoint pin in {}",
                        fact_path.display()
                    )));
                }
            }
        }
        Ok(SubstrateStats {
            logical_bytes: metadata.len(),
            allocated_bytes,
            checkpoint_count,
            checkpoint_bytes,
            pinned_checkpoint_bytes,
        })
    }

    fn compact(&self, image: &Path, format: ImageFormat) -> Result<bool, ApfsStorageError> {
        self.verify_controller_path(image)?;
        if format != ImageFormat::Sparse {
            return Err(ApfsStorageError::InvalidPlan(
                "detached compaction is supported only for SPARSE images",
            ));
        }
        if self.image_is_kernel_mounted(image)? {
            return Err(ApfsStorageError::Host(format!(
                "cannot compact mounted image: {}",
                image.display()
            )));
        }
        self.backend.compact_image(image, format)?;
        Ok(true)
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
        self.preview_gc_project(&project, repo, std::time::SystemTime::now())
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

fn metadata_workspace_ref(
    metadata: &DetachedWorkspaceMetadata,
) -> Result<LifecycleWorkspace, ApfsStorageError> {
    let role = if metadata.workspace.is_main() {
        WorkspaceRole::Main
    } else {
        WorkspaceRole::Workspace
    };
    let revision = Revision::new(metadata.grants.revision);
    LifecycleWorkspace::new(
        metadata.repo_id.clone(),
        metadata.workspace.clone(),
        metadata.workspace_incarnation.clone(),
        revision,
        revision,
        role,
        metadata.image_format,
    )
    .map_err(|_| ApfsStorageError::Host("invalid detached workspace identity".to_owned()))
}

fn io_error(operation: &'static str, path: &Path, source: io::Error) -> ApfsStorageError {
    ApfsStorageError::Io {
        operation,
        path: path.to_owned(),
        source,
    }
}

fn swap_paths(left: &Path, right: &Path) -> Result<(), ApfsStorageError> {
    #[cfg(target_os = "macos")]
    {
        let left = CString::new(left.as_os_str().as_bytes())
            .map_err(|_| ApfsStorageError::Host("canonical image path contains NUL".to_owned()))?;
        let right = CString::new(right.as_os_str().as_bytes())
            .map_err(|_| ApfsStorageError::Host("staged image path contains NUL".to_owned()))?;
        const RENAME_SWAP: u32 = 0x0000_0002;
        let result = unsafe {
            libc::renameatx_np(
                libc::AT_FDCWD,
                left.as_ptr(),
                libc::AT_FDCWD,
                right.as_ptr(),
                RENAME_SWAP,
            )
        };
        if result == 0 {
            Ok(())
        } else {
            Err(io_error(
                "atomically swap restore images",
                Path::new(left.to_str().unwrap_or("<invalid>")),
                io::Error::last_os_error(),
            ))
        }
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (left, right);
        Err(ApfsStorageError::Host(
            "atomic APFS restore swap requires macOS renameatx_np".to_owned(),
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn native_flag_and_result_tables_are_exact() {
        assert_eq!(
            ROOT_OPEN_FLAGS,
            libc::O_RDONLY | libc::O_DIRECTORY | libc::O_NOFOLLOW | libc::O_CLOEXEC
        );
        assert_eq!(DIRECTORY_OPEN_FLAGS, ROOT_OPEN_FLAGS);
        assert_eq!(
            LOCK_FILE_OPEN_FLAGS,
            libc::O_RDWR | libc::O_CREAT | libc::O_NOFOLLOW | libc::O_CLOEXEC
        );
        assert_eq!(WAIT_LOCK_OPERATION, libc::LOCK_EX);
        assert_eq!(TRY_LOCK_OPERATION, libc::LOCK_EX | libc::LOCK_NB);
        assert_eq!(LOCK_FILE_MODE, 0o600);
        assert_eq!(CONTROLLER_DIRECTORY_MODE, 0o700);

        for (fd, failed) in [(-1, true), (0, false), (1, false)] {
            assert_eq!(fd_failed(fd), failed, "fd={fd}");
            assert_eq!(flock_succeeded(fd), fd == 0, "result={fd}");
        }
        for (mode, kind, busy) in [
            (LockMode::Try, io::ErrorKind::WouldBlock, true),
            (LockMode::Wait, io::ErrorKind::WouldBlock, false),
            (LockMode::Try, io::ErrorKind::Other, false),
            (LockMode::Wait, io::ErrorKind::Other, false),
        ] {
            assert_eq!(lock_is_busy(mode, kind), busy, "{mode:?}/{kind:?}");
        }
        for (fd, kind, create) in [
            (-1, io::ErrorKind::NotFound, true),
            (-1, io::ErrorKind::PermissionDenied, false),
            (0, io::ErrorKind::NotFound, false),
            (0, io::ErrorKind::Other, false),
        ] {
            assert_eq!(should_create_directory(fd, kind), create);
        }
        for (result, kind, failed) in [
            (-1, io::ErrorKind::PermissionDenied, true),
            (-1, io::ErrorKind::AlreadyExists, false),
            (0, io::ErrorKind::PermissionDenied, false),
            (0, io::ErrorKind::Other, false),
        ] {
            assert_eq!(mkdir_failed(result, kind), failed);
        }
        for (kind, retry) in [
            (io::ErrorKind::Interrupted, true),
            (io::ErrorKind::WouldBlock, false),
            (io::ErrorKind::Other, false),
        ] {
            assert_eq!(should_retry_lock(kind), retry);
        }
    }

    #[cfg(unix)]
    #[test]
    fn recovery_enumerators_are_no_follow_and_type_exact() {
        let root =
            std::env::temp_dir().join(format!("cowshed-apfs-enumerators-{}", uuid::Uuid::new_v4()));
        let directory = root.join("directory");
        let child_directory = directory.join("child");
        let regular_file = directory.join("entry.asif");
        let directory_link = root.join("directory-link");
        let file_link = directory.join("entry-link.asif");
        let loop_a = root.join("loop-a");
        let loop_b = root.join("loop-b");
        let loop_child = loop_a.join("child");
        std::fs::create_dir_all(&child_directory).expect("child directory");
        std::fs::write(&regular_file, b"image").expect("regular file");
        std::os::unix::fs::symlink(&directory, &directory_link).expect("directory symlink");
        std::os::unix::fs::symlink(&regular_file, &file_link).expect("file symlink");
        std::os::unix::fs::symlink("loop-b", &loop_a).expect("first loop symlink");
        std::os::unix::fs::symlink("loop-a", &loop_b).expect("second loop symlink");

        assert!(
            directory_children(&root.join("missing"))
                .expect("missing directory")
                .is_empty()
        );
        assert!(
            directory_children(&directory_link)
                .expect("directory symlink")
                .is_empty()
        );
        assert!(directory_children(&regular_file).is_err());
        assert!(directory_children(&loop_child).is_err());
        assert_eq!(
            directory_children(&directory).expect("directory children"),
            vec![child_directory]
        );

        assert!(
            regular_file_children(&root.join("missing"))
                .expect("missing directory")
                .is_empty()
        );
        assert!(
            regular_file_children(&directory_link)
                .expect("directory symlink")
                .is_empty()
        );
        assert!(regular_file_children(&regular_file).is_err());
        assert!(regular_file_children(&loop_child).is_err());
        assert_eq!(
            regular_file_children(&directory).expect("regular file children"),
            vec![regular_file]
        );

        std::fs::remove_dir_all(root).expect("fixture cleanup");
    }

    #[cfg(unix)]
    #[test]
    fn project_recovery_ignores_unreadable_apfs_metadata_directories() {
        use std::os::unix::fs::PermissionsExt;

        let root = std::env::temp_dir().join(format!(
            "cowshed-apfs-project-enumerators-{}",
            uuid::Uuid::new_v4()
        ));
        let system_metadata = root.join(".Trashes");
        let project = root.join("acme").join("widget");
        std::fs::create_dir_all(&system_metadata).expect("APFS metadata fixture");
        std::fs::create_dir_all(&project).expect("project fixture");
        std::fs::set_permissions(&system_metadata, std::fs::Permissions::from_mode(0o000))
            .expect("make APFS metadata unreadable");

        assert_eq!(
            collect_project_directories(&root).expect("project recovery scan"),
            vec![project]
        );

        std::fs::set_permissions(&system_metadata, std::fs::Permissions::from_mode(0o700))
            .expect("restore metadata fixture permissions");
        std::fs::remove_dir_all(root).expect("fixture cleanup");
    }

    #[test]
    fn parent_sync_rejects_root_and_missing_parent() {
        assert!(matches!(
            sync_parent_path(Path::new("/")),
            Err(ApfsStorageError::InvalidPlan(_))
        ));
        let missing = PathBuf::from(format!(
            "/tmp/cowshed-missing-parent-{}/child",
            std::process::id()
        ));
        assert!(matches!(
            sync_parent_path(&missing),
            Err(ApfsStorageError::Io { .. })
        ));
    }
}
