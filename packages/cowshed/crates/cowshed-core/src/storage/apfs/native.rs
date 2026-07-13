use std::collections::BTreeMap;
use std::ffi::{CStr, CString, OsString};
use std::fs::{self, File, OpenOptions};
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering as AtomicOrdering};
use std::sync::mpsc::{self, Sender, SyncSender};
use std::thread;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::{MetadataExt, OpenOptionsExt};
#[cfg(unix)]
use std::os::unix::io::AsRawFd;

use crate::apfs::{
    ApfsBackend, AttachedImage, CommandRunner, CreateImageRequest, CreatedImage, DetachTarget,
    ImageFormatSelection, MacOsApfsBackend,
};
use crate::copy::TreeCopier;
use crate::metadata::{
    DetachedWorkspaceMetadata, ImageFormat, METADATA_VERSION, Platform, WorkspaceIncarnation,
    WorkspaceMarker, WorkspaceName, WorkspaceRole, sidecar_path,
};
use crate::repository::RepoId;
use serde::{Deserialize, Serialize};

use super::super::lifecycle::{
    CheckpointFact, ExpectedState, KernelMountFact, LifecycleWorkspace, ObservedState,
    OperationIdentity, Pin, Revision, StorageFact, StorageGcReport, SubstrateStats,
};
use super::super::{CheckpointLabel, WORKSPACE_MARKER_PATH, discover_session_images};
use super::{
    ApfsExecutionHost, ApfsStorageError, ApfsSubstrateConfig, LockMode, MarkerExpectation,
    MetadataPolicy, layout, volume_name,
};

const CHECKPOINT_FACT_VERSION: u32 = 1;
const CHECKPOINT_FACT_SUFFIX: &str = ".checkpoint.json";
const SELF_HEALING_STUB: &[u8] = b"cowshed ensure --attach\n";

pub struct ImageLockGuard {
    files: Vec<File>,
}

impl Drop for ImageLockGuard {
    fn drop(&mut self) {
        for file in self.files.iter().rev() {
            unsafe {
                libc::flock(file.as_raw_fd(), libc::LOCK_UN);
            }
        }
    }
}

fn acquire_image_locks(
    paths: &[PathBuf],
    mode: LockMode,
) -> Result<Option<ImageLockGuard>, ApfsStorageError> {
    let mut paths = paths.to_vec();
    paths.sort();
    paths.dedup();
    let mut files = Vec::with_capacity(paths.len());
    for path in paths {
        let parent = path
            .parent()
            .ok_or(ApfsStorageError::InvalidPlan("lock path has no parent"))?;
        fs::create_dir_all(parent)
            .map_err(|error| io_error("create lifecycle lock directory", parent, error))?;
        if fs::symlink_metadata(parent)
            .map_err(|error| io_error("inspect lifecycle lock directory", parent, error))?
            .file_type()
            .is_symlink()
        {
            return Err(ApfsStorageError::Host(format!(
                "lifecycle lock parent must not be a symlink: {}",
                parent.display()
            )));
        }
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .mode(0o600)
            .custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
            .open(&path)
            .map_err(|error| io_error("open lifecycle lock", &path, error))?;
        let operation = match mode {
            LockMode::Wait => libc::LOCK_EX,
            LockMode::Try => libc::LOCK_EX | libc::LOCK_NB,
        };
        loop {
            let result = unsafe { libc::flock(file.as_raw_fd(), operation) };
            if result == 0 {
                files.push(file);
                break;
            }
            let error = io::Error::last_os_error();
            if error.kind() == io::ErrorKind::Interrupted {
                continue;
            }
            if mode == LockMode::Try && error.kind() == io::ErrorKind::WouldBlock {
                return Ok(None);
            }
            return Err(io_error("acquire lifecycle lock", &path, error));
        }
    }
    Ok(Some(ImageLockGuard { files }))
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
    AfterUndoSidecar = 1,
    AfterImageSwap = 2,
    AfterUndoRename = 3,
    AfterMetadataPublish = 4,
    AfterMetadataFsync = 5,
    AfterCanonicalImageRename = 6,
}

fn store_directory_exists(store_root: &Path) -> Result<bool, ApfsStorageError> {
    match fs::symlink_metadata(store_root) {
        Ok(metadata) if metadata.file_type().is_dir() => Ok(true),
        Ok(_) => Err(ApfsStorageError::Host(format!(
            "APFS store root is not a directory: {}",
            store_root.display()
        ))),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("inspect APFS store root", store_root, error)),
    }
}

fn directory_exists_no_follow(directory: &Path) -> Result<bool, ApfsStorageError> {
    match fs::symlink_metadata(directory) {
        Ok(metadata) => Ok(metadata.file_type().is_dir()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(io_error("inspect recovery directory", directory, error)),
    }
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
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_error("enumerate recovery directory", directory, error)),
    };
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
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(io_error("enumerate recovery directory", directory, error)),
    };
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
fn collect_project_directories(store_root: &Path) -> Result<Vec<PathBuf>, ApfsStorageError> {
    let mut projects = Vec::new();
    if !store_directory_exists(store_root)? {
        return Ok(projects);
    }
    for owner in directory_children(store_root)? {
        let Some(owner_name) = owner.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if matches!(
            owner_name,
            "mnt" | "caches" | "gateway" | "telemetry" | "run"
        ) {
            continue;
        }
        for project in directory_children(&owner)? {
            if directory_exists_no_follow(&project.join("checkpoints"))?
                || directory_exists_no_follow(&project.join(super::STAGING_NAMESPACE))?
            {
                projects.push(project);
            }
        }
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
                    RestoreFailpoint::AfterImageSwap => "image swap",
                    RestoreFailpoint::AfterUndoRename => "undo rename",
                    RestoreFailpoint::AfterMetadataPublish => "metadata publish",
                    RestoreFailpoint::AfterMetadataFsync => "metadata fsync",
                    RestoreFailpoint::AfterCanonicalImageRename => "canonical image rename",
                }
            )))
        } else {
            Ok(())
        }
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
        if let Some((_, metadata)) = self.find_canonical_image(repo, &main)? {
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
        if let Err(primary) = self.backend.mount(&attachment, mount_point, false) {
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

    fn lock_path_for_image(image: &Path) -> PathBuf {
        let mut lock: OsString = image.as_os_str().to_owned();
        lock.push(".lock");
        PathBuf::from(lock)
    }

    fn gc_project(
        &self,
        project: &Path,
        report: &mut StorageGcReport,
    ) -> Result<(), ApfsStorageError>
    where
        R: CommandRunner + Send + Sync + 'static,
    {
        let sessions = project.join("sessions");
        let session_images = match fs::read_dir(&sessions) {
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

        let trash = sessions.join(super::TRASH_NAMESPACE);
        match fs::read_dir(&trash) {
            Ok(entries) => {
                for entry in entries {
                    let entry =
                        entry.map_err(|error| io_error("read trash entry", &trash, error))?;
                    let path = entry.path();
                    let Ok(format) = ImageFormat::from_image_path(&path) else {
                        continue;
                    };
                    report.examined += 1;
                    let lock = Self::transient_lock_path(project, &path, format)?;
                    let Some(_guard) = acquire_image_locks(&[lock], LockMode::Try)? else {
                        continue;
                    };
                    self.reclaim_image(&path, format)?;
                    report.reclaimed += 1;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error("enumerate trash", &trash, error)),
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
            report.examined += 1;
            if staged_sidecars.contains_key(image) {
                continue;
            }
            let lock = Self::transient_lock_path(project, image, *format)?;
            let Some(_guard) = acquire_image_locks(&[lock], LockMode::Try)? else {
                continue;
            };
            self.reclaim_image(image, *format)?;
            report.reclaimed += 1;
        }
        for (image, sidecar) in &staged_sidecars {
            report.examined += 1;
            if staged_images.contains_key(image) {
                continue;
            }
            let Some(format) = staged_image_format(image) else {
                continue;
            };
            let lock = Self::transient_lock_path(project, image, format)?;
            let Some(_guard) = acquire_image_locks(&[lock], LockMode::Try)? else {
                continue;
            };
            fs::remove_file(sidecar)
                .map_err(|error| io_error("remove orphan staging metadata", sidecar, error))?;
            sync_parent!(sidecar)?;
            report.reclaimed += 1;
        }

        let checkpoint_root = project.join("checkpoints");
        for workspace_directory in directory_children(&checkpoint_root)? {
            let mut checkpoints = Vec::new();
            for image in regular_file_children(&workspace_directory)? {
                let Ok(format) = ImageFormat::from_image_path(&image) else {
                    continue;
                };
                let fact_path = checkpoint_fact_path(&image);
                if !fact_path.exists() {
                    continue;
                }
                let fact: CheckpointFactWire = crate::metadata::read_json(&fact_path)
                    .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
                let modified = fs::metadata(&image)
                    .and_then(|metadata| metadata.modified())
                    .map_err(|error| io_error("read checkpoint age", &image, error))?;
                checkpoints.push((modified, image, format, fact));
            }
            checkpoints.sort_by(|left, right| {
                right
                    .0
                    .cmp(&left.0)
                    .then_with(|| right.1.cmp(&left.1))
            });
            for (index, (modified, image, format, fact)) in checkpoints.into_iter().enumerate() {
                report.examined += 1;
                if fact.pin == "pinned" {
                    report.retained_pinned += 1;
                    continue;
                }
                let younger_than_fourteen_days = std::time::SystemTime::now()
                    .duration_since(modified)
                    .map_or(true, |age| {
                        age < std::time::Duration::from_secs(14 * 24 * 60 * 60)
                    });
                if index < 5 || younger_than_fourteen_days {
                    report.retained_recent += 1;
                    continue;
                }
                let lock = super::workspace_lock_path(
                    &self.config,
                    &fact.repo_id,
                    &fact.workspace,
                    format,
                )?;
                let Some(_guard) = acquire_image_locks(&[lock], LockMode::Try)? else {
                    continue;
                };
                self.reclaim_image(&image, format)?;
                report.reclaimed += 1;
            }
        }

        for path in session_images {
            if matches!(ImageFormat::from_image_path(&path), Ok(ImageFormat::Sparse)) {
                let lock = Self::lock_path_for_image(&path);
                let Some(_guard) = acquire_image_locks(&[lock], LockMode::Try)? else {
                    continue;
                };
                if self.image_is_kernel_mounted(&path)? {
                    continue;
                }
                report.examined += 1;
                self.backend.compact_image(&path, ImageFormat::Sparse)?;
            }
        }
        Ok(())
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
        acquire_image_locks(paths, mode)
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
        self.find_canonical_image(repo, workspace)?
            .map(|(_, metadata)| metadata.image_format)
            .ok_or(ApfsStorageError::Host(format!(
                "published workspace is missing: {repo}/{workspace}"
            )))
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
        let metadata = DetachedWorkspaceMetadata::read_for_image(image)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        if metadata.image_format != format {
            return Err(ApfsStorageError::Host(format!(
                "detached metadata format disagrees with requested attachment for {}",
                image.display()
            )));
        }
        self.backend
            .attach_verified(image, format)
            .map_err(Into::into)
    }

    fn mount(
        &self,
        attachment: &Self::Attachment,
        mount_point: &Path,
        browse: bool,
    ) -> Result<(), ApfsStorageError> {
        self.backend
            .mount(attachment, mount_point, browse)
            .map_err(Into::into)
    }

    fn chown_volume_root(&self, mount_point: &Path) -> Result<(), ApfsStorageError> {
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
        self.backend
            .rename_volume(mount_point, volume_name)
            .map_err(Into::into)
    }

    fn write_marker(
        &self,
        mount_point: &Path,
        workspace: &LifecycleWorkspace,
        forked_from: Option<&WorkspaceName>,
        identity: &OperationIdentity,
    ) -> Result<(), ApfsStorageError> {
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
            Ok(())
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

    fn publish_image(&self, staged: &Path, canonical: &Path) -> Result<(), ApfsStorageError> {
        Self::ensure_parent(canonical)?;
        let staged_format = ImageFormat::from_image_path(staged)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        let canonical_format = ImageFormat::from_image_path(canonical)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        if staged_format != canonical_format {
            return Err(ApfsStorageError::InvalidPlan(
                "canonical publication must preserve image format",
            ));
        }
        if canonical.exists() {
            return Err(ApfsStorageError::Host(format!(
                "canonical image already exists: {}",
                canonical.display()
            )));
        }
        DetachedWorkspaceMetadata::read_for_image(staged)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        fs::rename(staged, canonical)
            .map_err(|error| io_error("publish canonical image", canonical, error))?;
        self.trip_restore_failpoint(RestoreFailpoint::AfterCanonicalImageRename)?;
        if let Err(primary) = Self::rename_sidecar(staged, canonical) {
            let cleanup = fs::rename(canonical, staged)
                .map_err(|error| io_error("roll back image publication", staged, error));
            return super::combine_cleanup("publish canonical sidecar", primary, cleanup);
        }
        if let Err(primary) = sync_parent!(canonical) {
            let cleanup = Self::rename_sidecar(canonical, staged).and_then(|()| {
                fs::rename(canonical, staged).map_err(|error| {
                    io_error("roll back unsynced image publication", staged, error)
                })
            });
            return super::combine_cleanup("sync canonical image publication", primary, cleanup);
        }
        Ok(())
    }

    fn publish_adopt(
        &self,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
        staged: &Path,
        canonical: &Path,
    ) -> Result<(), ApfsStorageError> {
        if !source_checkout.is_absolute()
            || pre_cowshed_checkout != pre_cowshed_path(source_checkout)
            || !source_checkout.is_dir()
            || pre_cowshed_checkout.exists()
        {
            return Err(ApfsStorageError::InvalidPlan(
                "invalid adopt source or pre-cowshed handoff",
            ));
        }
        fs::rename(source_checkout, pre_cowshed_checkout).map_err(|error| {
            io_error("move original checkout aside", pre_cowshed_checkout, error)
        })?;
        sync_parent!(pre_cowshed_checkout)?;

        let publication = (|| {
            fs::create_dir(source_checkout)
                .map_err(|error| io_error("create canonical mountpoint", source_checkout, error))?;
            let stub = source_checkout.join(".envrc");
            fs::write(&stub, SELF_HEALING_STUB)
                .map_err(|error| io_error("write self-healing mount stub", &stub, error))?;
            fs::File::open(source_checkout)
                .and_then(|directory| directory.sync_all())
                .map_err(|error| io_error("sync canonical mountpoint", source_checkout, error))?;
            sync_parent!(source_checkout)?;
            self.publish_image(staged, canonical)
        })();

        if let Err(primary) = publication {
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
                fs::rename(pre_cowshed_checkout, source_checkout).map_err(|error| {
                    io_error("restore original checkout", source_checkout, error)
                })?;
                sync_parent!(source_checkout)
            })();
            return super::combine_cleanup("adopt publication", primary, cleanup);
        }
        Ok(())
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
        workspace
            .format()
            .validate_path(image)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        let preserved = match (policy, source_image) {
            (MetadataPolicy::Preserve, Some(source)) => Some(
                DetachedWorkspaceMetadata::read_for_image(source)
                    .map_err(|error| ApfsStorageError::Host(error.to_string()))?,
            ),
            (MetadataPolicy::Preserve, None) => {
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
        Self::ensure_parent(undo)?;
        if undo.exists() {
            return Err(ApfsStorageError::Host(format!(
                "restore undo image already exists: {}",
                undo.display()
            )));
        }
        let canonical_sidecar = sidecar_path(canonical);
        let undo_sidecar = sidecar_path(undo);
        DetachedWorkspaceMetadata::read_for_image(staged)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        DetachedWorkspaceMetadata::read_for_image(canonical)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        fs::hard_link(&canonical_sidecar, &undo_sidecar)
            .map_err(|error| io_error("retain restore metadata", &undo_sidecar, error))?;
        self.trip_restore_failpoint(RestoreFailpoint::AfterUndoSidecar)?;
        if let Err(error) = swap_paths(canonical, staged) {
            let _ = fs::remove_file(&undo_sidecar);
            return Err(error);
        }
        self.trip_restore_failpoint(RestoreFailpoint::AfterImageSwap)?;
        if let Err(error) = fs::rename(staged, undo) {
            let primary = io_error("retain restore undo image", undo, error);
            let rollback = swap_paths(canonical, staged);
            let _ = fs::remove_file(&undo_sidecar);
            return match rollback {
                Ok(()) => Err(primary),
                Err(cleanup) => Err(ApfsStorageError::Cleanup {
                    operation: "restore swap",
                    primary: Box::new(primary),
                    cleanup: Box::new(cleanup),
                }),
            };
        }
        self.trip_restore_failpoint(RestoreFailpoint::AfterUndoRename)?;
        sync_parent!(canonical)?;
        sync_parent!(undo)
    }

    fn publish_restored_metadata(
        &self,
        staged: &Path,
        canonical: &Path,
        workspace: &LifecycleWorkspace,
        revision: Revision,
        source_image: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.publish_metadata(
            canonical,
            workspace,
            revision,
            MetadataPolicy::Preserve,
            None,
            Some(source_image),
        )?;
        self.trip_restore_failpoint(RestoreFailpoint::AfterMetadataPublish)?;
        sync_parent!(canonical)?;
        self.trip_restore_failpoint(RestoreFailpoint::AfterMetadataFsync)?;
        Self::remove_sidecar(staged)?;
        sync_parent!(canonical)
    }

    fn rollback_restore(
        &self,
        canonical: &Path,
        undo: &Path,
        staged: &Path,
    ) -> Result<(), ApfsStorageError> {
        let canonical_sidecar = sidecar_path(canonical);
        let undo_sidecar = sidecar_path(undo);
        let staged_sidecar = sidecar_path(staged);
        fs::rename(undo, staged)
            .map_err(|error| io_error("stage failed restore image", staged, error))?;
        swap_paths(canonical, staged)?;
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
        sync_parent!(canonical)
    }

    fn retire_image(&self, canonical: &Path, trash: &Path) -> Result<(), ApfsStorageError> {
        Self::ensure_parent(trash)?;
        if trash.exists() {
            return Err(ApfsStorageError::Host(format!(
                "retired image already exists: {}",
                trash.display()
            )));
        }
        fs::rename(canonical, trash).map_err(|error| io_error("retire image", trash, error))?;
        if let Err(error) = Self::rename_sidecar(canonical, trash) {
            return match fs::rename(trash, canonical) {
                Ok(()) => Err(error),
                Err(rollback) => Err(ApfsStorageError::Cleanup {
                    operation: "retire image",
                    primary: Box::new(error),
                    cleanup: Box::new(io_error("roll back retirement", canonical, rollback)),
                }),
            };
        }
        sync_parent!(trash)
    }

    fn reclaim_image(&self, image: &Path, format: ImageFormat) -> Result<(), ApfsStorageError> {
        self.backend.delete_image(image, format)?;
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

    fn recover_pending(&self, config: &ApfsSubstrateConfig) -> Result<(), ApfsStorageError> {
        for project in collect_project_directories(&config.store_root)? {
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
                        continue;
                    }
                    if canonical.exists() && !sidecar_path(&canonical).exists() {
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
                if !canonical.exists() {
                    continue;
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
                    if undo.exists() {
                        if staged.exists() {
                            self.backend
                                .delete_image(&staged, old_metadata.image_format)?;
                        }
                    } else if staged.exists() {
                        fs::rename(&staged, &undo).map_err(|error| {
                            io_error("complete restore undo rename", &undo, error)
                        })?;
                    }
                    Self::remove_sidecar(&staged)?;
                    sync_parent!(&canonical)?;
                    continue;
                }
                if canonical_metadata.as_ref().is_some_and(|metadata| {
                    metadata.workspace_incarnation != old_metadata.workspace_incarnation
                }) {
                    continue;
                }

                let image_was_published = if undo.exists() {
                    true
                } else if staged.exists() {
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
                    if incarnation == replacement_incarnation {
                        true
                    } else if incarnation == old_metadata.workspace_incarnation.as_str() {
                        false
                    } else {
                        return Err(ApfsStorageError::Host(format!(
                            "restore candidate marker does not match canonical or replacement: {}",
                            canonical.display()
                        )));
                    }
                } else {
                    continue;
                };

                if undo.exists() {
                    fs::rename(&undo, &staged).map_err(|error| {
                        io_error("stage interrupted restore undo", &staged, error)
                    })?;
                }
                if image_was_published {
                    swap_paths(&canonical, &staged)?;
                }
                if staged.exists() {
                    self.backend
                        .delete_image(&staged, old_metadata.image_format)?;
                }
                Self::remove_sidecar(&staged)?;
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
        let metadata =
            fs::metadata(image).map_err(|error| io_error("read image statistics", image, error))?;
        #[cfg(unix)]
        let allocated_bytes = metadata.blocks().saturating_mul(512);
        #[cfg(not(unix))]
        let allocated_bytes = metadata.len();
        let checkpoint_directory = layout(&self.config, workspace.repo())?
            .project()
            .checkpoints
            .join(workspace.name().as_str());
        let checkpoint_count = match fs::read_dir(&checkpoint_directory) {
            Ok(entries) => entries
                .filter_map(Result::ok)
                .filter(|entry| ImageFormat::from_image_path(&entry.path()).is_ok())
                .count() as u64,
            Err(error) if error.kind() == io::ErrorKind::NotFound => 0,
            Err(error) => {
                return Err(io_error(
                    "enumerate checkpoints",
                    &checkpoint_directory,
                    error,
                ));
            }
        };
        Ok(SubstrateStats {
            logical_bytes: metadata.len(),
            allocated_bytes,
            checkpoint_count,
        })
    }

    fn compact(&self, image: &Path, format: ImageFormat) -> Result<bool, ApfsStorageError> {
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

    fn gc(&self, config: &ApfsSubstrateConfig) -> Result<StorageGcReport, ApfsStorageError> {
        if config.store_root != self.config.store_root {
            return Err(ApfsStorageError::InvalidPlan(
                "GC config differs from host storage root",
            ));
        }
        let mut report = StorageGcReport::default();
        if !store_directory_exists(&config.store_root)? {
            return Ok(report);
        }
        for owner in directory_children(&config.store_root)? {
            if owner == config.caches_root
                || owner.file_name().is_some_and(|name| {
                    matches!(name.to_str(), Some("mnt" | "caches" | "gateway" | "telemetry" | "run"))
                })
            {
                continue;
            }
            for project in directory_children(&owner)? {
                self.gc_project(&project, &mut report)?;
            }
        }
        Ok(report)
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
