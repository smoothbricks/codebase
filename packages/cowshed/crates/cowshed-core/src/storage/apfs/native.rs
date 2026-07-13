use std::collections::{BTreeMap, BTreeSet};
use std::ffi::CString;
use std::fs;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{self, Sender, SyncSender};
use std::thread;

#[cfg(unix)]
use std::os::unix::ffi::OsStrExt;
#[cfg(unix)]
use std::os::unix::fs::MetadataExt;

use crate::apfs::{
    ApfsBackend, AttachedImage, CommandRunner, CreateImageRequest, CreatedImage,
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
    volume_name: String,
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
    Facts {
        repo: RepoId,
        reply: SyncSender<Vec<KernelMountFact>>,
    },
    Images {
        reply: SyncSender<BTreeSet<PathBuf>>,
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
                        RegistryCommand::Facts { repo, reply } => {
                            let facts = mounted
                                .iter()
                                .filter(|((mounted_repo, _), _)| mounted_repo == &repo)
                                .map(|(_, entry)| KernelMountFact {
                                    mount_id: entry.mount_id,
                                    volume_name: entry.volume_name.clone(),
                                })
                                .collect();
                            let _ = reply.send(facts);
                        }
                        RegistryCommand::Images { reply } => {
                            let images = mounted
                                .values()
                                .map(|entry| entry.attachment.image().to_owned())
                                .collect();
                            let _ = reply.send(images);
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

    fn facts(&self, repo: &RepoId) -> Result<Vec<KernelMountFact>, ApfsStorageError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.send(RegistryCommand::Facts {
            repo: repo.clone(),
            reply,
        })?;
        response.recv().map_err(|_| {
            ApfsStorageError::Host("APFS mount registry facts response was dropped".to_owned())
        })
    }

    fn images(&self) -> Result<BTreeSet<PathBuf>, ApfsStorageError> {
        let (reply, response) = mpsc::sync_channel(1);
        self.send(RegistryCommand::Images { reply })?;
        response.recv().map_err(|_| {
            ApfsStorageError::Host("APFS mount registry images response was dropped".to_owned())
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
}

impl<R> MacOsApfsExecutionHost<R> {
    pub fn new(
        runner: R,
        config: ApfsSubstrateConfig,
        metadata: WorkspaceMetadataTemplate,
    ) -> Result<Self, ApfsStorageError> {
        Ok(Self {
            backend: MacOsApfsBackend::new(runner),
            config,
            metadata,
            mounted: MountedRegistry::start()?,
        })
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

    fn mounted_images(&self) -> Result<BTreeSet<PathBuf>, ApfsStorageError> {
        self.mounted.images()
    }

    fn gc_project(
        &self,
        project: &Path,
        mounted: &BTreeSet<PathBuf>,
        report: &mut GcReport,
    ) -> Result<(), ApfsStorageError>
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
                && !mounted.contains(&path)
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
            volume_name: volume_name(workspace.repo(), workspace.name()),
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
        let staged_sidecar = sidecar_path(staged);
        let canonical_sidecar = sidecar_path(canonical);
        let undo_sidecar = sidecar_path(undo);
        DetachedWorkspaceMetadata::read_for_image(staged)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        DetachedWorkspaceMetadata::read_for_image(canonical)
            .map_err(|error| ApfsStorageError::Host(error.to_string()))?;
        fs::hard_link(&canonical_sidecar, &undo_sidecar)
            .map_err(|error| io_error("retain restore metadata", &undo_sidecar, error))?;
        if let Err(error) = swap_paths(canonical, staged) {
            let _ = fs::remove_file(&undo_sidecar);
            return Err(error);
        }
        if let Err(error) = fs::rename(&staged_sidecar, &canonical_sidecar) {
            let rollback = swap_paths(canonical, staged);
            let _ = fs::remove_file(&undo_sidecar);
            return match rollback {
                Ok(()) => Err(io_error(
                    "publish restored metadata",
                    &canonical_sidecar,
                    error,
                )),
                Err(cleanup) => Err(ApfsStorageError::Cleanup {
                    operation: "publish restored metadata",
                    primary: Box::new(io_error(
                        "publish restored metadata",
                        &canonical_sidecar,
                        error,
                    )),
                    cleanup: Box::new(cleanup),
                }),
            };
        }
        if let Err(error) = fs::rename(staged, undo) {
            let primary = io_error("retain restore undo image", undo, error);
            let rollback = fs::rename(&canonical_sidecar, &staged_sidecar)
                .map_err(|error| {
                    io_error(
                        "stage replacement metadata during rollback",
                        &staged_sidecar,
                        error,
                    )
                })
                .and_then(|()| {
                    fs::rename(&undo_sidecar, &canonical_sidecar).map_err(|error| {
                        io_error("restore canonical metadata", &canonical_sidecar, error)
                    })
                })
                .and_then(|()| swap_paths(canonical, staged));
            return match rollback {
                Ok(()) => Err(primary),
                Err(cleanup) => Err(ApfsStorageError::Cleanup {
                    operation: "restore swap",
                    primary: Box::new(primary),
                    cleanup: Box::new(cleanup),
                }),
            };
        }
        sync_parent!(canonical)?;
        sync_parent!(undo)
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
        self.mounted.facts(repo)
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
        if self.mounted_images()?.contains(image) {
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
        let mounted = self.mounted_images()?;
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
                    self.gc_project(&project, &mounted, &mut report)?;
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
