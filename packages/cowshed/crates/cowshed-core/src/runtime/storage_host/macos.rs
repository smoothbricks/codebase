//! macOS's native project storage, chosen per project when its host is built: one APFS image
//! per workspace, attached and mounted through the system disk tools (01a_apfs.md), or one ZFS
//! dataset per workspace under the directory host (09_substrates.md). The store itself is APFS
//! either way; which of the two a project is, is asked of the host's filesystems each time
//! ([`DirectoryExecutionHost::for_datasets`]) and recorded nowhere. [`NativeHost`] is one of the
//! two hosts and answers every lifecycle and runtime operation with that host's own.

use super::*;
use crate::api::held_lock::LockHolding;
use crate::storage::apfs::native::MacOsApfsExecutionHost;
use crate::storage::apfs::{
    ApfsSubstrateConfig, MarkerExpectation, MetadataPolicy, PendingAdoption, PublicationError,
    ResumableClone, ResumedMount,
};
use crate::storage::bootstrap::SelectedSubstrate;
use crate::storage::directory::{DirectoryAttachment, DirectoryExecutionHost};
use crate::storage::lifecycle::{
    DefragmentOutcome, LifecycleFact, LifecycleWorkspace, MountJoin, MountState, OperationIdentity,
    RetiredRef, Revision, StorageFact, StorageGcOptions, StorageGcPlan, StorageGcReport,
    SubstrateStats,
};
use cowshed_apfs::store_lock::{ExistingLocks, ImageLockGuard, LockMode};
use cowshed_apfs::{AttachedImage, CommandRunner, DetachIntent, MountAccess};

/// The image host of a project whose workspaces are APFS disk images.
type ImageHost = MacOsApfsExecutionHost<cowshed_apfs::SystemCommandRunner>;

/// The execution host a native opening runs its lifecycle on: the host of the backend the
/// project keeps its workspaces on.
pub enum NativeHost {
    /// APFS disk images, attached as kernel devices.
    Images(ImageHost),
    /// ZFS datasets, mounted where their workspaces are.
    Datasets(DirectoryExecutionHost),
}

impl NativeHost {
    /// The published record of `workspace`, as either host reads its own
    /// ([`RuntimeStorageHost::canonical_metadata`]).
    pub fn canonical_metadata(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<DetachedWorkspaceMetadata>, ApfsStorageError> {
        RuntimeStorageHost::canonical_metadata(self, repo, workspace)
    }

    /// `workspace` as its record and its mount stand, as either host reads its own
    /// ([`RuntimeStorageHost::workspace_snapshot`]).
    pub fn workspace_snapshot(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<(DerivedWorkspace, DetachedWorkspaceMetadata)>, ApfsStorageError> {
        RuntimeStorageHost::workspace_snapshot(self, repo, workspace)
    }
}

/// What a [`NativeHost`] holds a payload attached as: its backend's own handle.
pub enum NativeAttachment {
    Image(AttachedImage),
    Dataset(DirectoryAttachment),
}

/// What a macOS host keeps a project on: APFS images unless its adoption names ZFS. Never a
/// plain directory, which is a caller-owned store's alone.
pub(super) const PLATFORM_FILESYSTEMS: AdoptFilesystems = AdoptFilesystems {
    default: StoreSubstrate::Apfs,
    others: &[StoreSubstrate::Zfs],
};

/// The native host of the project `repo` in the store `config` names: the dataset host when the
/// project keeps its workspaces as ZFS datasets, the APFS image host, running its disk tools in
/// the config's namespace, for every other project. A project with a main says which by what
/// its main is in the store; one with none yet is being adopted, onto `adopting` when that
/// adoption names a filesystem and onto APFS images when it names none. Every opening but an
/// adoption's passes `None`.
pub fn native_host(
    config: ApfsSubstrateConfig,
    repo: &RepoId,
    adopting: Option<StoreSubstrate>,
) -> Result<NativeHost, ApfsStorageError> {
    if let Some(datasets) = DirectoryExecutionHost::for_datasets(config.clone(), repo, adopting)? {
        return Ok(NativeHost::Datasets(datasets));
    }
    MacOsApfsExecutionHost::new(
        cowshed_apfs::SystemCommandRunner::new(config.namespace),
        config,
    )
    .map(NativeHost::Images)
}

/// The macOS store is APFS whatever its projects keep their workspaces on: a store selected onto
/// any other substrate is refused before a host is built over it. A project's own backend is
/// never selected here; [`native_host`] asks the host for it.
pub(crate) fn require_native_substrate(substrate: &SelectedSubstrate) -> crate::Result<()> {
    if matches!(substrate, SelectedSubstrate::Apfs { .. }) {
        return Ok(());
    }
    Err(crate::CowshedError::environment_missing(
        "the macOS runtime requires the APFS image substrate",
        "remove the unsupported substrate override and retry",
    ))
}

/// The refusal of an attachment handed to the host of the other backend: a host's attachments
/// are only ever its own, so this names a caller that mixed two projects' hosts.
const FOREIGN_ATTACHMENT: ApfsStorageError =
    ApfsStorageError::InvalidPlan("attachment belongs to the host's other storage backend");

/// Answer each listed method of the trait with the same method of whichever host this is. An
/// impl lists every method of its trait, the ones the trait defaults included: a default left
/// unlisted would answer for both hosts instead of asking the one this is.
macro_rules! delegate {
    ($delegated:ident: $(
        fn $name:ident(&self $(, $argument:ident: $kind:ty)* $(,)?) $(-> $answer:ty)?;
    )+) => {$(
        fn $name(&self $(, $argument: $kind)*) $(-> $answer)? {
            match self {
                Self::Images(host) => $delegated::$name(host $(, $argument)*),
                Self::Datasets(host) => $delegated::$name(host $(, $argument)*),
            }
        }
    )+};
}

impl ApfsExecutionHost for NativeHost {
    type LockGuard = ImageLockGuard;
    type Attachment = NativeAttachment;

    delegate! { ApfsExecutionHost:
        fn has_physical_volumes(&self) -> bool;
        fn substrate(&self) -> crate::api::dto::StoreSubstrate;
        fn census(&self) -> &crate::census::CensusScope;
        fn lock_images(
            &self,
            images: &[PathBuf],
            mode: LockMode,
            holding: &LockHolding,
        ) -> Result<Option<ImageLockGuard>, ApfsStorageError>;
        fn lock_existing_images(
            &self,
            images: &[PathBuf],
            mode: LockMode,
            holding: &LockHolding,
        ) -> Result<ExistingLocks<ImageLockGuard>, ApfsStorageError>;
        fn observe(&self, expected: &[LifecycleFact]) -> Result<Vec<LifecycleFact>, ApfsStorageError>;
        fn clone_image(
            &self,
            source: &Path,
            source_mount: Option<&Path>,
            destination: &Path,
        ) -> Result<(), ApfsStorageError>;
        fn require_forkable_image(
            &self,
            image: &Path,
            mount: Option<&Path>,
        ) -> Result<(), ApfsStorageError>;
        fn resumable_clone(
            &self,
            config: &ApfsSubstrateConfig,
            source: &LifecycleWorkspace,
            destination: &WorkspaceName,
            destination_topology: Revision,
            identity: &OperationIdentity,
        ) -> Result<Option<ResumableClone>, ApfsStorageError>;
        fn validate_pending_clone(
            &self,
            image: &Path,
            expected: &DetachedWorkspaceMetadata,
        ) -> Result<(), ApfsStorageError>;
        fn pending_adoption(
            &self,
            config: &ApfsSubstrateConfig,
            repo: &RepoId,
        ) -> Result<Option<PendingAdoption>, ApfsStorageError>;
        fn discard_pending(&self, image: &Path) -> Result<(), ApfsStorageError>;
        fn copy_tree(&self, source: &Path, destination: &Path) -> Result<(), ApfsStorageError>;
        fn mint_workspace_credentials(
            &self,
            workspace: &LifecycleWorkspace,
            image_path: &Path,
            mount_point: &Path,
            private_key_path: &Path,
        ) -> Result<(), ApfsStorageError>;
        fn write_marker(
            &self,
            mount_point: &Path,
            workspace: &LifecycleWorkspace,
            forked_from: Option<&WorkspaceName>,
            identity: &OperationIdentity,
        ) -> Result<(), ApfsStorageError>;
        fn validate_marker(
            &self,
            mount_point: &Path,
            expected: &MarkerExpectation,
        ) -> Result<(), ApfsStorageError>;
        fn validate_staged_companion(&self, path: &Path) -> Result<(), ApfsStorageError>;
        fn heal_mount(
            &self,
            workspace: &LifecycleWorkspace,
            mount_point: &Path,
        ) -> Result<(), ApfsStorageError>;
        fn mount_settled(
            &self,
            workspace: &LifecycleWorkspace,
            mount_point: &Path,
        ) -> Result<bool, ApfsStorageError>;
        fn lost_mount(
            &self,
            workspace: &LifecycleWorkspace,
            mount_point: &Path,
        ) -> Result<Option<String>, ApfsStorageError>;
        fn detach_mounted(
            &self,
            workspace: &LifecycleWorkspace,
            intent: DetachIntent,
        ) -> Result<(), ApfsStorageError>;
        fn ensure_linked_build_volume(
            &self,
            workspace: &LifecycleWorkspace,
            checkout: &Path,
        ) -> Result<(), ApfsStorageError>;
        fn release_project_containers(&self, repo: &RepoId) -> Result<(), ApfsStorageError>;
        fn detach_owned_build_volumes(
            &self,
            workspace: &LifecycleWorkspace,
        ) -> Result<(), ApfsStorageError>;
        fn resize(
            &self,
            workspace: &LifecycleWorkspace,
            image: &Path,
            mount_point: &Path,
            capacity: ImageCapacity,
        ) -> Result<ResizeOutcome, ApfsStorageError>;
        fn defragment(
            &self,
            workspace: &LifecycleWorkspace,
            image: &Path,
            mount_point: &Path,
        ) -> Result<DefragmentOutcome, ApfsStorageError>;
        fn restore_adopted_checkout(
            &self,
            workspace: &LifecycleWorkspace,
            source_checkout: &Path,
            pre_cowshed_checkout: &Path,
        ) -> Result<(), ApfsStorageError>;
        fn vacate_adopted_checkout(
            &self,
            source_checkout: &Path,
            pre_cowshed_checkout: &Path,
        ) -> Result<(), PublicationError>;
        fn vacate_plain_checkout(
            &self,
            checkout: &Path,
            preserved: &Path,
        ) -> Result<(), PublicationError>;
        fn require_vacant_checkout(&self, checkout: &Path) -> Result<(), ApfsStorageError>;
        fn publish_metadata(
            &self,
            image: &Path,
            workspace: &LifecycleWorkspace,
            revision: Revision,
            policy: MetadataPolicy,
            identity: Option<&OperationIdentity>,
            source_image: Option<&Path>,
        ) -> Result<(), ApfsStorageError>;
        fn activate_pending(
            &self,
            image: &Path,
            workspace: &LifecycleWorkspace,
        ) -> Result<(), ApfsStorageError>;
        fn retire_image(&self, canonical: &Path, trash: &Path) -> Result<(), ApfsStorageError>;
        fn release_workspace(
            &self,
            config: &ApfsSubstrateConfig,
            retired: &RetiredRef,
        ) -> Result<(), ApfsStorageError>;
        fn reclaim_image(&self, image: &Path) -> Result<(), ApfsStorageError>;
        fn reclaim_retired(
            &self,
            config: &ApfsSubstrateConfig,
            retired: &RetiredRef,
        ) -> Result<(), ApfsStorageError>;
        fn list(&self, repo: &RepoId) -> Result<Vec<StorageFact>, ApfsStorageError>;
        fn workspace_mount_state(
            &self,
            workspace: &LifecycleWorkspace,
        ) -> Result<MountState, ApfsStorageError>;
        fn mounts(&self, repo: &RepoId) -> Result<Vec<MountJoin>, ApfsStorageError>;
        fn recover_pending(
            &self,
            config: &ApfsSubstrateConfig,
            held_locks: &[PathBuf],
        ) -> Result<(), ApfsStorageError>;
        fn stats(
            &self,
            workspace: &LifecycleWorkspace,
            image: &Path,
        ) -> Result<SubstrateStats, ApfsStorageError>;
        fn preview_gc(
            &self,
            config: &ApfsSubstrateConfig,
            repo: &RepoId,
            options: StorageGcOptions,
        ) -> Result<StorageGcPlan, ApfsStorageError>;
        fn execute_gc(
            &self,
            config: &ApfsSubstrateConfig,
            plan: StorageGcPlan,
        ) -> Result<StorageGcReport, ApfsStorageError>;
    }

    fn create_attached(
        &self,
        capacity: ImageCapacity,
        image: &Path,
        mount_point: &Path,
    ) -> Result<NativeAttachment, ApfsStorageError> {
        match self {
            Self::Images(host) => host
                .create_attached(capacity, image, mount_point)
                .map(NativeAttachment::Image),
            Self::Datasets(host) => host
                .create_attached(capacity, image, mount_point)
                .map(NativeAttachment::Dataset),
        }
    }

    fn resume_pending_adopt(
        &self,
        image: &Path,
        mount_point: &Path,
        workspace: &LifecycleWorkspace,
    ) -> Result<Option<NativeAttachment>, ApfsStorageError> {
        match self {
            Self::Images(host) => host
                .resume_pending_adopt(image, mount_point, workspace)
                .map(|resumed| resumed.map(NativeAttachment::Image)),
            Self::Datasets(host) => host
                .resume_pending_adopt(image, mount_point, workspace)
                .map(|resumed| resumed.map(NativeAttachment::Dataset)),
        }
    }

    fn unmount_attachment(&self, attachment: &NativeAttachment) -> Result<(), ApfsStorageError> {
        match (self, attachment) {
            (Self::Images(host), NativeAttachment::Image(attachment)) => {
                host.unmount_attachment(attachment)
            }
            (Self::Datasets(host), NativeAttachment::Dataset(attachment)) => {
                host.unmount_attachment(attachment)
            }
            (Self::Images(_), NativeAttachment::Dataset(_))
            | (Self::Datasets(_), NativeAttachment::Image(_)) => Err(FOREIGN_ATTACHMENT),
        }
    }

    fn attach_verified(&self, image: &Path) -> Result<NativeAttachment, ApfsStorageError> {
        match self {
            Self::Images(host) => host.attach_verified(image).map(NativeAttachment::Image),
            Self::Datasets(host) => host.attach_verified(image).map(NativeAttachment::Dataset),
        }
    }

    fn attach_mounted(
        &self,
        image: &Path,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<NativeAttachment, ApfsStorageError> {
        match self {
            Self::Images(host) => host
                .attach_mounted(image, mount_point, access, browse)
                .map(NativeAttachment::Image),
            Self::Datasets(host) => host
                .attach_mounted(image, mount_point, access, browse)
                .map(NativeAttachment::Dataset),
        }
    }

    fn mount(
        &self,
        attachment: &NativeAttachment,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<(), ApfsStorageError> {
        match (self, attachment) {
            (Self::Images(host), NativeAttachment::Image(attachment)) => {
                host.mount(attachment, mount_point, access, browse)
            }
            (Self::Datasets(host), NativeAttachment::Dataset(attachment)) => {
                host.mount(attachment, mount_point, access, browse)
            }
            (Self::Images(_), NativeAttachment::Dataset(_))
            | (Self::Datasets(_), NativeAttachment::Image(_)) => Err(FOREIGN_ATTACHMENT),
        }
    }

    fn attach_and_mount_resumable(
        &self,
        image: &Path,
        mount_point: &Path,
        workspace: &LifecycleWorkspace,
    ) -> Result<ResumedMount<NativeAttachment>, ApfsStorageError> {
        match self {
            Self::Images(host) => host
                .attach_and_mount_resumable(image, mount_point, workspace)
                .map(|resumed| resumed_as(resumed, NativeAttachment::Image)),
            Self::Datasets(host) => host
                .attach_and_mount_resumable(image, mount_point, workspace)
                .map(|resumed| resumed_as(resumed, NativeAttachment::Dataset)),
        }
    }

    fn rename_volume(
        &self,
        attachment: &NativeAttachment,
        mount_point: &Path,
        volume_name: &str,
    ) -> Result<(), ApfsStorageError> {
        match (self, attachment) {
            (Self::Images(host), NativeAttachment::Image(attachment)) => {
                host.rename_volume(attachment, mount_point, volume_name)
            }
            (Self::Datasets(host), NativeAttachment::Dataset(attachment)) => {
                host.rename_volume(attachment, mount_point, volume_name)
            }
            (Self::Images(_), NativeAttachment::Dataset(_))
            | (Self::Datasets(_), NativeAttachment::Image(_)) => Err(FOREIGN_ATTACHMENT),
        }
    }

    fn detach(
        &self,
        attachment: NativeAttachment,
        intent: DetachIntent,
    ) -> Result<(), ApfsStorageError> {
        match (self, attachment) {
            (Self::Images(host), NativeAttachment::Image(attachment)) => {
                host.detach(attachment, intent)
            }
            (Self::Datasets(host), NativeAttachment::Dataset(attachment)) => {
                host.detach(attachment, intent)
            }
            (Self::Images(_), NativeAttachment::Dataset(_))
            | (Self::Datasets(_), NativeAttachment::Image(_)) => Err(FOREIGN_ATTACHMENT),
        }
    }

    fn retain_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        attachment: NativeAttachment,
    ) -> Result<u64, ApfsStorageError> {
        match (self, attachment) {
            (Self::Images(host), NativeAttachment::Image(attachment)) => {
                host.retain_mounted(workspace, attachment)
            }
            (Self::Datasets(host), NativeAttachment::Dataset(attachment)) => {
                host.retain_mounted(workspace, attachment)
            }
            (Self::Images(_), NativeAttachment::Dataset(_))
            | (Self::Datasets(_), NativeAttachment::Image(_)) => Err(FOREIGN_ATTACHMENT),
        }
    }
}

/// `resumed` with its attachment named as its backend's variant.
fn resumed_as<A>(
    resumed: ResumedMount<A>,
    variant: fn(A) -> NativeAttachment,
) -> ResumedMount<NativeAttachment> {
    ResumedMount {
        attachment: variant(resumed.attachment),
        found: resumed.found,
        attach_mount_work: resumed.attach_mount_work,
    }
}

impl RuntimeStorageHost for NativeHost {
    /// What an adoption may keep its project on is the platform's, whichever host this is.
    fn adopt_filesystems(&self) -> AdoptFilesystems {
        PLATFORM_FILESYSTEMS
    }

    delegate! { RuntimeStorageHost:
        fn sidecar_platform(&self) -> Platform;
        fn canonical_metadata(
            &self,
            repo: &RepoId,
            workspace: &WorkspaceName,
        ) -> Result<Option<DetachedWorkspaceMetadata>, ApfsStorageError>;
        fn workspace_snapshot(
            &self,
            repo: &RepoId,
            workspace: &WorkspaceName,
        ) -> Result<Option<(DerivedWorkspace, DetachedWorkspaceMetadata)>, ApfsStorageError>;
        fn label_image_volume(&self, image: &Path, label: &str) -> Result<Labelled, ApfsStorageError>;
        fn create_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            id: &BuildVolumeId,
            capacity: ImageCapacity,
            registration: &Registration,
        ) -> Result<(PathBuf, ReleaseClaim), ApfsStorageError>;
        fn create_claimed_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            claim: &ReleaseClaim,
            capacity: ImageCapacity,
            registration: &Registration,
        ) -> Result<PathBuf, ApfsStorageError>;
        fn clone_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            source: &BuildVolumeId,
            destination: &BuildVolumeId,
            registration: &Registration,
        ) -> Result<Option<std::time::Duration>, ApfsStorageError>;
        fn clone_shared_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            source: &crate::runtime::build_volumes::HeldVolume,
            destination: &BuildVolumeId,
            registration: &Registration,
        ) -> Result<Option<std::time::Duration>, ApfsStorageError>;
        fn clone_quiet_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            quiet: &crate::runtime::build_volumes::Quiet,
            destination: &BuildVolumeId,
            registration: &Registration,
        ) -> Result<Option<std::time::Duration>, ApfsStorageError>;
        fn clone_claimed_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            claim: &ReleaseClaim,
            destination: &BuildVolumeId,
            registration: &Registration,
        ) -> Result<Option<std::time::Duration>, ApfsStorageError>;
        fn build_volume_written(
            &self,
            layout: &BuildVolumeLayout,
            id: &BuildVolumeId,
        ) -> Result<SystemTime, ApfsStorageError>;
        fn capture_fork_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            seed: &BuildVolumeId,
            registration: &Registration,
        ) -> Result<CapturedBuildVolume, ApfsStorageError>;
        fn ready_fork_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            captured: CapturedBuildVolume,
            own_seed: Option<(&BuildVolumeId, &Registration)>,
        ) -> Result<(BuildVolumeId, PathBuf, Option<std::time::Duration>), ApfsStorageError>;
        fn abandon_captured_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            captured: CapturedBuildVolume,
        ) -> Result<BuildVolumeRelease, ApfsStorageError>;
        fn mount_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            id: &BuildVolumeId,
        ) -> Result<(PathBuf, Option<std::time::Duration>), ApfsStorageError>;
        fn release_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            id: &BuildVolumeId,
        ) -> Result<BuildVolumeRelease, ApfsStorageError>;
        fn claim_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            id: &BuildVolumeId,
        ) -> Result<BuildVolumeClaim, ApfsStorageError>;
        fn try_claim_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            id: &BuildVolumeId,
        ) -> Result<Option<ReleaseClaim>, ApfsStorageError>;
        fn detach_claimed_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            claim: &ReleaseClaim,
        ) -> Result<(), ApfsStorageError>;
        fn release_claimed(
            &self,
            layout: &BuildVolumeLayout,
            claim: ReleaseClaim,
        ) -> Result<Option<BuildVolumeRefusal>, ApfsStorageError>;
        fn build_volume_capacity(
            &self,
            layout: &BuildVolumeLayout,
            id: &BuildVolumeId,
        ) -> Result<ImageCapacity, ApfsStorageError>;
        fn resize_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            id: &BuildVolumeId,
            capacity: ImageCapacity,
        ) -> Result<ResizeOutcome, ApfsStorageError>;
        fn re_adopt_volume_holders(
            &self,
            image: &Path,
            mount: &Path,
        ) -> Result<Vec<cowshed_os::process::Holder>, ApfsStorageError>;
        fn retain_re_adopt_source(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError>;
        fn attached_capacity_bytes(&self, image: &Path) -> Result<u64, ApfsStorageError>;
        fn retained_re_adopt_main_is_mounted(
            &self,
            source: &ReAdoptSource,
        ) -> Result<bool, ApfsStorageError>;
        fn mount_re_adopt_sources(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError>;
        fn release_re_adopt_sources(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError>;
        fn copy_re_adopt_build(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError>;
        fn require_sparse_replacement(
            &self,
            replacement: &Path,
            retained: &Path,
            capacity_bytes: u64,
        ) -> Result<(u64, u64), ApfsStorageError>;
        fn publish_re_adopt_build(
            &self,
            layout: &BuildVolumeLayout,
            source: &ReAdoptSource,
            build_claim: Option<&ReleaseClaim>,
            mbx: Option<&Path>,
            retention: &[crate::capabilities::BuildStatePath],
        ) -> Result<(), ApfsStorageError>;
        fn defragment_build_volume(
            &self,
            layout: &BuildVolumeLayout,
            claim: &ReleaseClaim,
        ) -> Result<DefragmentOutcome, ApfsStorageError>;
        fn release_seed_compaction_leftovers(
            &self,
            layout: &BuildVolumeLayout,
        ) -> Result<(), ApfsStorageError>;
        fn stage_seed_compaction(
            &self,
            layout: &BuildVolumeLayout,
            seed: &BuildVolumeId,
        ) -> Result<CompactionSource, ApfsStorageError>;
        fn copy_seed_compaction(
            &self,
            source: CompactionSource,
            minimum_extents: u64,
            keep_going: &mut dyn FnMut(u64) -> std::ops::ControlFlow<String>,
        ) -> Result<SeedCompaction, ApfsStorageError>;
        fn publish_seed_compaction(
            &self,
            layout: &BuildVolumeLayout,
            compacted: CompactedSeed,
            destination: &BuildVolumeId,
            registration: Registration,
        ) -> Result<(), ApfsStorageError>;
        fn catch_up_seed_compaction(
            &self,
            compacted: CompactedSeed,
            next: CompactionSource,
            keep_going: &mut dyn FnMut(u64) -> std::ops::ControlFlow<String>,
        ) -> Result<SeedCatchUp, ApfsStorageError>;
    }
}

impl<R> RuntimeStorageHost for MacOsApfsExecutionHost<R>
where
    R: CommandRunner + Send + Sync + 'static,
{
    fn sidecar_platform(&self) -> Platform {
        crate::storage::apfs::native::SIDECAR_PLATFORM
    }

    fn canonical_metadata(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<DetachedWorkspaceMetadata>, ApfsStorageError> {
        MacOsApfsExecutionHost::canonical_metadata(self, repo, workspace)
    }

    fn workspace_snapshot(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<(DerivedWorkspace, DetachedWorkspaceMetadata)>, ApfsStorageError> {
        MacOsApfsExecutionHost::workspace_snapshot(self, repo, workspace)
    }

    fn label_image_volume(&self, image: &Path, label: &str) -> Result<Labelled, ApfsStorageError> {
        MacOsApfsExecutionHost::label_image_volume(self, image, label)
    }

    fn create_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
        registration: &Registration,
    ) -> Result<(PathBuf, ReleaseClaim), ApfsStorageError> {
        MacOsApfsExecutionHost::create_build_volume(self, layout, id, capacity, registration)
    }
    fn create_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
        capacity: ImageCapacity,
        registration: &Registration,
    ) -> Result<PathBuf, ApfsStorageError> {
        MacOsApfsExecutionHost::create_claimed_build_volume(
            self,
            layout,
            claim,
            capacity,
            registration,
        )
    }
    fn clone_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &BuildVolumeId,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError> {
        MacOsApfsExecutionHost::clone_build_volume(self, layout, source, destination, registration)
    }

    fn clone_shared_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        source: &crate::runtime::build_volumes::HeldVolume,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError> {
        MacOsApfsExecutionHost::clone_shared_build_volume(
            self,
            layout,
            source,
            destination,
            registration,
        )
    }

    fn clone_quiet_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        quiet: &crate::runtime::build_volumes::Quiet,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError> {
        MacOsApfsExecutionHost::clone_quiet_build_volume(
            self,
            layout,
            quiet,
            destination,
            registration,
        )
    }

    fn clone_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
        destination: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<Option<std::time::Duration>, ApfsStorageError> {
        MacOsApfsExecutionHost::clone_claimed_build_volume(
            self,
            layout,
            claim,
            destination,
            registration,
        )
    }

    fn build_volume_written(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<SystemTime, ApfsStorageError> {
        MacOsApfsExecutionHost::build_volume_written(self, layout, id)
    }

    fn capture_fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
        registration: &Registration,
    ) -> Result<CapturedBuildVolume, ApfsStorageError> {
        MacOsApfsExecutionHost::capture_fork_build_volume(self, layout, seed, registration)
    }

    fn ready_fork_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        captured: CapturedBuildVolume,
        own_seed: Option<(&BuildVolumeId, &Registration)>,
    ) -> Result<(BuildVolumeId, PathBuf, Option<std::time::Duration>), ApfsStorageError> {
        MacOsApfsExecutionHost::ready_fork_build_volume(self, layout, captured, own_seed)
    }

    fn abandon_captured_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        captured: CapturedBuildVolume,
    ) -> Result<BuildVolumeRelease, ApfsStorageError> {
        MacOsApfsExecutionHost::abandon_captured_build_volume(self, layout, captured)
    }

    fn mount_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<(PathBuf, Option<std::time::Duration>), ApfsStorageError> {
        MacOsApfsExecutionHost::mount_build_volume(self, layout, id)
    }

    fn release_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<BuildVolumeRelease, ApfsStorageError> {
        MacOsApfsExecutionHost::release_build_volume(self, layout, id)
    }

    fn claim_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<BuildVolumeClaim, ApfsStorageError> {
        MacOsApfsExecutionHost::claim_build_volume(self, layout, id)
    }

    fn try_claim_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<Option<ReleaseClaim>, ApfsStorageError> {
        MacOsApfsExecutionHost::try_claim_build_volume(self, layout, id)
    }

    fn detach_claimed_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
    ) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::detach_claimed_build_volume(self, layout, claim)
    }

    fn release_claimed(
        &self,
        layout: &BuildVolumeLayout,
        claim: ReleaseClaim,
    ) -> Result<Option<BuildVolumeRefusal>, ApfsStorageError> {
        MacOsApfsExecutionHost::release_claimed(self, layout, claim)
    }

    fn build_volume_capacity(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
    ) -> Result<ImageCapacity, ApfsStorageError> {
        MacOsApfsExecutionHost::build_volume_capacity(self, layout, id)
    }

    fn resize_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        id: &BuildVolumeId,
        capacity: ImageCapacity,
    ) -> Result<ResizeOutcome, ApfsStorageError> {
        MacOsApfsExecutionHost::resize_build_volume(self, layout, id, capacity)
    }

    fn re_adopt_volume_holders(
        &self,
        image: &Path,
        mount: &Path,
    ) -> Result<Vec<cowshed_os::process::Holder>, ApfsStorageError> {
        MacOsApfsExecutionHost::re_adopt_volume_holders(self, image, mount)
    }

    fn retain_re_adopt_source(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::retain_re_adopt_source(self, source)
    }

    fn attached_capacity_bytes(&self, image: &Path) -> Result<u64, ApfsStorageError> {
        MacOsApfsExecutionHost::attached_capacity_bytes(self, image)
    }

    fn retained_re_adopt_main_is_mounted(
        &self,
        source: &ReAdoptSource,
    ) -> Result<bool, ApfsStorageError> {
        MacOsApfsExecutionHost::retained_re_adopt_main_is_mounted(self, source)
    }

    fn mount_re_adopt_sources(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::mount_re_adopt_sources(self, source)
    }

    fn release_re_adopt_sources(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::release_re_adopt_sources(self, source)
    }

    fn copy_re_adopt_build(&self, source: &ReAdoptSource) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::copy_re_adopt_build(self, source)
    }

    fn require_sparse_replacement(
        &self,
        replacement: &Path,
        retained: &Path,
        capacity_bytes: u64,
    ) -> Result<(u64, u64), ApfsStorageError> {
        MacOsApfsExecutionHost::require_sparse_replacement(
            self,
            replacement,
            retained,
            capacity_bytes,
        )
    }

    fn publish_re_adopt_build(
        &self,
        layout: &BuildVolumeLayout,
        source: &ReAdoptSource,
        build_claim: Option<&ReleaseClaim>,
        mbx: Option<&Path>,
        retention: &[crate::capabilities::BuildStatePath],
    ) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::publish_re_adopt_build(
            self,
            layout,
            source,
            build_claim,
            mbx,
            retention,
        )
    }

    fn defragment_build_volume(
        &self,
        layout: &BuildVolumeLayout,
        claim: &ReleaseClaim,
    ) -> Result<crate::storage::lifecycle::DefragmentOutcome, ApfsStorageError> {
        MacOsApfsExecutionHost::defragment_build_volume(self, layout, claim)
    }

    fn release_seed_compaction_leftovers(
        &self,
        layout: &BuildVolumeLayout,
    ) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::release_seed_compaction_leftovers(self, layout)
    }
    fn stage_seed_compaction(
        &self,
        layout: &BuildVolumeLayout,
        seed: &BuildVolumeId,
    ) -> Result<CompactionSource, ApfsStorageError> {
        MacOsApfsExecutionHost::stage_seed_compaction(self, layout, seed)
    }

    fn copy_seed_compaction(
        &self,
        source: CompactionSource,
        minimum_extents: u64,
        keep_going: &mut dyn FnMut(u64) -> std::ops::ControlFlow<String>,
    ) -> Result<SeedCompaction, ApfsStorageError> {
        MacOsApfsExecutionHost::copy_seed_compaction(self, source, minimum_extents, keep_going)
    }

    fn publish_seed_compaction(
        &self,
        layout: &BuildVolumeLayout,
        compacted: CompactedSeed,
        destination: &BuildVolumeId,
        registration: Registration,
    ) -> Result<(), ApfsStorageError> {
        MacOsApfsExecutionHost::publish_seed_compaction(
            self,
            layout,
            compacted,
            destination,
            registration,
        )
    }

    fn catch_up_seed_compaction(
        &self,
        compacted: CompactedSeed,
        next: CompactionSource,
        keep_going: &mut dyn FnMut(u64) -> std::ops::ControlFlow<String>,
    ) -> Result<SeedCatchUp, ApfsStorageError> {
        MacOsApfsExecutionHost::catch_up_seed_compaction(self, compacted, next, keep_going)
    }
}
