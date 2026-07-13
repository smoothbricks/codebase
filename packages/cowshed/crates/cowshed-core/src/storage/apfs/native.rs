use std::collections::BTreeMap;
use std::ffi::{CStr, CString};
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering as AtomicOrdering};
use std::sync::mpsc::{self, Sender, SyncSender};
use std::thread;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use crate::apfs::{
    ApfsBackend, AttachedImage, CommandRunner, CreateImageRequest, CreatedImage, DetachTarget,
    ImageFormatSelection, MacOsApfsBackend,
};
use crate::metadata::{
    DetachedWorkspaceMetadata, GrantSet, ImageFormat, METADATA_VERSION, Platform, WorkspaceMarker,
    WorkspaceName, WorkspaceRole, sidecar_path,
};
use crate::repository::RepoId;

use super::super::lifecycle::{
    ExpectedState, GcReport, KernelMountFact, ObservedState, Revision, StorageFact, SubstrateStats,
    WorkspaceRef,
};
use super::super::{CheckpointLabel, WORKSPACE_MARKER_PATH, discover_session_images};
use super::{
    ApfsExecutionHost, ApfsStorageError, ApfsSubstrateConfig, MarkerExpectation, MetadataPolicy,
    layout, volume_name,
};

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
    flags & MNT_DONTBROWSE != 0 && flags & MNT_IGNORE_OWNERS == 0
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
}

fn collect_restore_sidecars(
    directory: &Path,
    sidecars: &mut Vec<PathBuf>,
) -> Result<(), ApfsStorageError> {
    if !directory.exists() {
        return Ok(());
    }
    let mut pending = vec![directory.to_owned()];
    while let Some(directory) = pending.pop() {
        let entries = fs::read_dir(&directory)
            .map_err(|error| io_error("enumerate restore sidecars", &directory, error))?;
        for entry in entries {
            let entry =
                entry.map_err(|error| io_error("read restore sidecar entry", &directory, error))?;
            let path = entry.path();
            if path.is_dir() {
                pending.push(path);
                continue;
            }
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceMetadataTemplate {
    pub project_root: PathBuf,
    pub base_commit: String,
    pub created_at: String,
    pub created_trace: String,
    pub grants: GrantSet,
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
    metadata: WorkspaceMetadataTemplate,
    mounted: MountedRegistry,
    restore_failpoint: AtomicU8,
    mount_source: Arc<dyn KernelMountSource>,
    recovery_marker_source: Option<Arc<dyn RecoveryMarkerSource>>,
}

impl<R: CommandRunner> MacOsApfsExecutionHost<R> {
    pub fn new(
        runner: R,
        config: ApfsSubstrateConfig,
        metadata: WorkspaceMetadataTemplate,
    ) -> Result<Self, ApfsStorageError> {
        Self::with_mount_source(runner, config, metadata, SystemKernelMountSource)
    }

    pub fn with_mount_source(
        runner: R,
        config: ApfsSubstrateConfig,
        metadata: WorkspaceMetadataTemplate,
        mount_source: impl KernelMountSource,
    ) -> Result<Self, ApfsStorageError> {
        Ok(Self {
            backend: MacOsApfsBackend::new(runner),
            config,
            metadata,
            mounted: MountedRegistry::start()?,
            mount_source: Arc::new(mount_source),
            recovery_marker_source: None,
            restore_failpoint: AtomicU8::new(RestoreFailpoint::Disabled as u8),
        })
    }

    pub fn with_recovery_sources(
        runner: R,
        config: ApfsSubstrateConfig,
        metadata: WorkspaceMetadataTemplate,
        mount_source: impl KernelMountSource,
        recovery_marker_source: impl RecoveryMarkerSource,
    ) -> Result<Self, ApfsStorageError> {
        Ok(Self {
            backend: MacOsApfsBackend::new(runner),
            config,
            metadata,
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
        let detach = self.backend.detach(&attachment, false).map_err(Into::into);
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

    fn gc_project(&self, project: &Path, report: &mut GcReport) -> Result<(), ApfsStorageError>
    where
        R: CommandRunner,
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
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
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
                    self.backend.delete_image(&path, format)?;
                    Self::remove_sidecar(&path)?;
                    report.reclaimed += 1;
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {}
            Err(error) => return Err(io_error("enumerate trash", &trash, error)),
        }

        for path in session_images {
            if matches!(ImageFormat::from_image_path(&path), Ok(ImageFormat::Sparse))
                && !self.image_is_kernel_mounted(&path)?
            {
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
        workspace: &WorkspaceRef,
        forked_from: Option<&WorkspaceName>,
    ) -> Result<(), ApfsStorageError> {
        let marker = WorkspaceMarker {
            version: METADATA_VERSION,
            repo_id: workspace.repo().clone(),
            project_root: self.metadata.project_root.clone(),
            workspace: workspace.name().clone(),
            workspace_incarnation: workspace.incarnation().clone(),
            role: workspace.role(),
            image_format: workspace.format(),
            base_commit: self.metadata.base_commit.clone(),
            created_at: self.metadata.created_at.clone(),
            forked_from: forked_from.cloned(),
            created_trace: self.metadata.created_trace.clone(),
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
        workspace: &WorkspaceRef,
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

        self.detach_mounted(workspace, true)?;
        if self
            .mount_source
            .mounts()?
            .into_iter()
            .any(|mount| mount.mount_point == mount_point)
            && let Err(primary) = self.backend.detach_target(
                workspace.format(),
                DetachTarget::MountPoint(mount_point),
                false,
            )
        {
            self.backend
                .detach_target(
                    workspace.format(),
                    DetachTarget::MountPoint(mount_point),
                    true,
                )
                .map_err(|cleanup| ApfsStorageError::Cleanup {
                    operation: "heal wrong APFS mount flags",
                    primary: Box::new(primary.into()),
                    cleanup: Box::new(cleanup.into()),
                })?;
        }
        Ok(())
    }

    fn detach(&self, attachment: Self::Attachment, force: bool) -> Result<(), ApfsStorageError> {
        self.backend.detach(&attachment, force).map_err(Into::into)
    }

    fn retain_mounted(
        &self,
        workspace: &WorkspaceRef,
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
        workspace: &WorkspaceRef,
        force: bool,
    ) -> Result<(), ApfsStorageError> {
        let key = (workspace.repo().clone(), workspace.name().clone());
        let Some(entry) = self.mounted.remove(key.clone())? else {
            return Ok(());
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
        Self::rename_sidecar(staged, canonical)?;
        if let Err(error) = fs::rename(staged, canonical) {
            let primary = io_error("publish canonical image", canonical, error);
            return match Self::rename_sidecar(canonical, staged) {
                Ok(()) => Err(primary),
                Err(cleanup) => Err(ApfsStorageError::Cleanup {
                    operation: "publish canonical image",
                    primary: Box::new(primary),
                    cleanup: Box::new(cleanup),
                }),
            };
        }
        if let Err(primary) = sync_parent!(canonical) {
            let cleanup = fs::rename(canonical, staged)
                .map_err(|error| io_error("roll back unsynced image publication", staged, error))
                .and_then(|()| Self::rename_sidecar(canonical, staged));
            return match cleanup {
                Ok(()) => Err(primary),
                Err(cleanup) => Err(ApfsStorageError::Cleanup {
                    operation: "sync canonical image publication",
                    primary: Box::new(primary),
                    cleanup: Box::new(cleanup),
                }),
            };
        }
        Ok(())
    }

    fn publish_metadata(
        &self,
        image: &Path,
        workspace: &WorkspaceRef,
        revision: Revision,
        policy: MetadataPolicy,
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
        let mut grants = preserved.as_ref().map_or_else(
            || self.metadata.grants.clone(),
            |metadata| metadata.grants.clone(),
        );
        grants.revision = revision.get();
        let metadata = DetachedWorkspaceMetadata {
            version: METADATA_VERSION,
            repo_id: workspace.repo().clone(),
            workspace: workspace.name().clone(),
            workspace_incarnation: workspace.incarnation().clone(),
            image_format: workspace.format(),
            platform: Platform::Macos,
            updated_at: self.metadata.created_at.clone(),
            grants,
            info_snapshot: preserved.and_then(|metadata| metadata.info_snapshot),
        };
        metadata
            .write_for_image(image)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))
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
        workspace: &WorkspaceRef,
        revision: Revision,
        source_image: &Path,
    ) -> Result<(), ApfsStorageError> {
        self.publish_metadata(
            canonical,
            workspace,
            revision,
            MetadataPolicy::Preserve,
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
        Self::remove_sidecar(image)
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

    fn recover_pending(&self, config: &ApfsSubstrateConfig) -> Result<(), ApfsStorageError> {
        let mut undo_sidecars = Vec::new();
        collect_restore_sidecars(&config.store_root, &mut undo_sidecars)?;
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
            let old_metadata = DetachedWorkspaceMetadata::read_for_image(&undo)
                .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
            let storage = layout(config, &old_metadata.repo_id)?;
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
                    fs::rename(&staged, &undo)
                        .map_err(|error| io_error("complete restore undo rename", &undo, error))?;
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
                fs::rename(&undo, &staged)
                    .map_err(|error| io_error("stage interrupted restore undo", &staged, error))?;
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
        Ok(())
    }

    fn stats(
        &self,
        workspace: &WorkspaceRef,
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

    fn gc(&self, config: &ApfsSubstrateConfig) -> Result<GcReport, ApfsStorageError> {
        if config.store_root != self.config.store_root {
            return Err(ApfsStorageError::InvalidPlan(
                "GC config differs from host storage root",
            ));
        }
        let mut report = GcReport::default();
        let owners = match fs::read_dir(&config.store_root) {
            Ok(entries) => entries,
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(report),
            Err(error) => return Err(io_error("enumerate APFS store", &config.store_root, error)),
        };
        for owner in owners {
            let owner = owner
                .map_err(|error| io_error("read APFS owner", &config.store_root, error))?
                .path();
            if !owner.is_dir()
                || owner == config.caches_root
                || owner.file_name().is_some_and(|name| name == "mnt")
            {
                continue;
            }
            for repository in fs::read_dir(&owner)
                .map_err(|error| io_error("enumerate APFS repositories", &owner, error))?
            {
                let project = repository
                    .map_err(|error| io_error("read APFS repository", &owner, error))?
                    .path();
                if project.is_dir() {
                    self.gc_project(&project, &mut report)?;
                }
            }
        }
        Ok(report)
    }
}

fn metadata_workspace_ref(
    metadata: &DetachedWorkspaceMetadata,
) -> Result<WorkspaceRef, ApfsStorageError> {
    let role = if metadata.workspace.is_main() {
        WorkspaceRole::Main
    } else {
        WorkspaceRole::Workspace
    };
    let revision = Revision::new(metadata.grants.revision);
    WorkspaceRef::new(
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

fn io_error(operation: &'static str, path: &Path, error: io::Error) -> ApfsStorageError {
    ApfsStorageError::Host(format!("{operation} {} failed: {error}", path.display()))
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
