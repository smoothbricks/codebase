pub mod extents;
pub mod native;
pub mod rekey;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::apfs::{ApfsError, DetachIntent, MountAccess, timed_apfs_step};
use crate::metadata::{
    IMAGE_EXTENSION, ImageCapacity, WorkspaceIncarnation, WorkspaceName, WorkspaceRole,
};
use crate::repository::{OwnedRepoIds, RepoId};
use crate::timing::timed_async;

use super::lifecycle::{
    AdoptPlan, AdoptRequest, CheckpointFact, CheckpointPlan, CheckpointRef, CreatePlan,
    DefragmentOutcome, DerivedWorkspace, Destination, ExecuteError, ForkPlan, ImmutablePlan,
    KernelMountFact, LifecycleBackend, LifecycleFact, LifecyclePlanner, LifecycleReceipt,
    LifecycleWorkspace, MountIntent, MountState, Operation, OperationIdentity, Pin, PlanError,
    PurePlanner, ResizeOutcome, RestoreMode, RestorePlan, RestoreReceipt, RetirePlan, RetiredRef,
    Revision, StorageFact, StorageGcPlan, StorageGcReport, Substrate, SubstrateStats,
    execute_checked, revalidate,
};
use super::{CheckpointLabel, PRE_RESTORE_PREFIX, StorageLayout, StorageLayoutError};

pub const DEFAULT_IMAGE_CAPACITY: ImageCapacity = ImageCapacity::from_gibibytes(100);
use super::recovery::{STAGING_NAMESPACE, TRASH_NAMESPACE};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApfsSubstrateConfig {
    pub store_root: PathBuf,
    /// The adopted checkout's path — the place in the user's source tree that adoption took over,
    /// and main's mountpoint.
    pub checkout_path: PathBuf,
    pub capacity: ImageCapacity,
}

impl ApfsSubstrateConfig {
    pub fn new(store_root: impl Into<PathBuf>, checkout_path: impl Into<PathBuf>) -> Self {
        Self {
            store_root: store_root.into(),
            checkout_path: checkout_path.into(),
            capacity: DEFAULT_IMAGE_CAPACITY,
        }
    }

    /// The same project, checked out somewhere else.
    ///
    /// The checkout path is the only field a live project can change — that is what `cowshed mv
    /// main` does, and what `cowshed attach` converges onto after a checkout is respelt. Everything
    /// else (store root, capacity) is fixed for the project's lifetime.
    ///
    /// It is a whole-config clone rather than a mutable field because the config is shared by
    /// value: `ApfsSubstrate` holds it behind an `Arc` that every clone of the substrate shares,
    /// and `MacOsApfsExecutionHost` holds its own copy. Mutating it in place would let an
    /// outstanding clone observe a half-applied move — a mount point derived from the new checkout
    /// path against an execution host still validating against the old one. Rebinding instead
    /// builds a new config, a new host, and a new substrate, and the caller swaps all three at
    /// once, at the one point in the move transaction where nothing is mounted.
    pub fn rebind_checkout(&self, checkout_path: impl Into<PathBuf>) -> Self {
        Self {
            checkout_path: checkout_path.into(),
            ..self.clone()
        }
    }

    /// Every identity the project answering to `current` owns, read from its own binding.
    ///
    /// Read at the point of use rather than carried in the config, because the config is built in
    /// two places — the project runtime, which holds the binding, and the gateway inventory host,
    /// which does not — and a set that only one of them could populate is a set the other would
    /// silently narrow to nothing. The binding beside the project directory is the authority both
    /// can reach.
    ///
    /// A binding that is absent, unreadable, or does not answer to `current` yields the tightest
    /// possible set. Widening acceptance on no evidence is never the safe direction.
    pub fn owned_repo_ids(&self, current: &RepoId) -> OwnedRepoIds {
        let Ok(layout) = StorageLayout::new(&self.store_root, current) else {
            return OwnedRepoIds::sole(current.clone());
        };
        let Ok(binding) = crate::metadata::read_json::<crate::repository::RepositoryBinding>(
            &layout.project().repository_binding,
        ) else {
            return OwnedRepoIds::sole(current.clone());
        };
        match binding.owned_repo_ids() {
            Ok(owned) if owned.current() == current => owned,
            _ => OwnedRepoIds::sole(current.clone()),
        }
    }

    pub fn with_capacity(mut self, capacity: ImageCapacity) -> Self {
        self.capacity = capacity;
        self
    }
}
pub trait IncarnationSource: Send + Sync + 'static {
    fn mint(&self) -> Result<WorkspaceIncarnation, ApfsStorageError>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct UuidIncarnationSource;

impl IncarnationSource for UuidIncarnationSource {
    fn mint(&self) -> Result<WorkspaceIncarnation, ApfsStorageError> {
        WorkspaceIncarnation::new(uuid::Uuid::new_v4().simple().to_string())
            .map_err(|error| ApfsStorageError::Host(error.to_string()))
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MetadataPolicy {
    Fresh,
    FreshPendingFence,
    Preserve,
    PendingFence,
}

/// What a mounted volume's in-image marker has to say for the volume to be this workspace's.
///
/// The repository axis is a set rather than one identity: an in-place identity change cannot reach
/// the marker sealed inside a detached image, so a renamed project legitimately mounts an image
/// still stamped with an identity it used to hold. Every other field is exact.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarkerExpectation {
    pub repos: OwnedRepoIds,
    pub workspace: WorkspaceName,
    pub incarnation: WorkspaceIncarnation,
}

impl MarkerExpectation {
    /// For a marker written at some earlier time, which is every already-published image. The
    /// identity it carries may be one the project has since changed away from.
    fn owned(config: &ApfsSubstrateConfig, workspace: &LifecycleWorkspace) -> Self {
        Self {
            repos: config.owned_repo_ids(workspace.repo()),
            workspace: workspace.name().clone(),
            incarnation: workspace.incarnation().clone(),
        }
    }

    /// For a marker this same operation just stamped, where the current identity is the only one
    /// the marker can possibly carry, so the expectation stays exact.
    fn freshly_stamped(workspace: &LifecycleWorkspace) -> Self {
        Self {
            repos: OwnedRepoIds::sole(workspace.repo().clone()),
            workspace: workspace.name().clone(),
            incarnation: workspace.incarnation().clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PublishedImage {
    pub workspace: LifecycleWorkspace,
    pub image: PathBuf,
    pub mount_point: PathBuf,
}

/// Mounted, controller-private workspace stage. It is not published into workspace enumeration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceStage {
    pub workspace: LifecycleWorkspace,
    pub mount_point: PathBuf,
    pub companion: PathBuf,
    /// True when this callback is continuing a durable pending clone rather than initializing
    /// bytes created by this invocation. Callers use this proof to admit only their own prior
    /// branch/worktree state; a fresh operation must still reject pre-existing state.
    pub resuming: bool,
}

pub type AdoptStage = WorkspaceStage;
pub type CreateStage = WorkspaceStage;
pub type ForkStage = WorkspaceStage;
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointStage {
    pub checkpoint: CheckpointRef,
    pub image: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingPublicationFact {
    pub workspace: LifecycleWorkspace,
    pub image: PathBuf,
    pub mount_point: PathBuf,
    pub source_checkpoint: String,
    pub source_incarnation: WorkspaceIncarnation,
    pub replaced_incarnation: WorkspaceIncarnation,
    pub destination_incarnation: WorkspaceIncarnation,
}

/// An adoption a crash interrupted: the canonical main image its `PendingFence` sidecar still
/// fences, the incarnation that sidecar was minted for, and what it records of the adoption.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PendingAdoption {
    pub incarnation: WorkspaceIncarnation,
    pub image: PathBuf,
    pub info: crate::metadata::WorkspaceInfoSnapshot,
    pub grants: crate::metadata::GrantSet,
}

impl PendingAdoption {
    /// The identity the interrupted attempt recorded, resumed under `trace`.
    pub fn identity(&self, trace: &str) -> OperationIdentity {
        OperationIdentity {
            project_root: self.info.project_root.clone(),
            base_commit: self.info.base_commit.clone(),
            created_at: self.info.created_at.clone(),
            branch: self.info.branch.clone(),
            forked_from: self.info.forked_from.clone(),
            created_trace: trace.to_owned(),
            grants: self.grants.clone(),
            git_worktree: self.info.git_worktree,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResumableClone {
    pub workspace: LifecycleWorkspace,
    pub identity: OperationIdentity,
    pub image: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreFence {
    pub pending: PendingPublicationFact,
}

#[derive(Debug, Error)]
pub enum StagedExecutionError<E> {
    #[error("staged lifecycle execution failed: {0}")]
    Storage(#[source] ApfsStorageError),
    #[error("staged lifecycle initializer failed: {0}")]
    Initializer(E),
    #[error(
        "staged lifecycle initializer failed and cleanup also failed: initializer={initializer}; cleanup={cleanup}"
    )]
    InitializerCleanup {
        initializer: E,
        #[source]
        cleanup: ApfsStorageError,
    },
}

#[derive(Debug, Error)]
pub enum RestoreExecutionError<F> {
    #[error("restore staging failed: {0}")]
    Storage(#[source] ApfsStorageError),
    #[error("restore fence failed with a pending forward-only publication: {source}")]
    Fence {
        source: F,
        pending: Box<PendingPublicationFact>,
    },
    #[error("restore fence succeeded but pending publication activation failed: {source}")]
    Activation {
        #[source]
        source: Box<ApfsStorageError>,
        pending: Box<PendingPublicationFact>,
    },
}

#[derive(Debug, Error)]
pub enum RetireExecutionError<F> {
    #[error("workspace retirement failed: {0}")]
    Storage(#[source] ApfsStorageError),
    #[error("workspace retired but durable lifecycle publication failed: {source}")]
    Fence { source: F, retired: RetiredRef },
}

impl<F> From<ApfsStorageError> for RetireExecutionError<F> {
    fn from(error: ApfsStorageError) -> Self {
        Self::Storage(error)
    }
}

impl<F> From<ApfsStorageError> for RestoreExecutionError<F> {
    fn from(error: ApfsStorageError) -> Self {
        Self::Storage(error)
    }
}
pub type AdoptExecutionError<E> = StagedExecutionError<E>;
pub type CreateExecutionError<E> = StagedExecutionError<E>;
pub type ForkExecutionError<E> = StagedExecutionError<E>;
pub type CheckpointExecutionError<E> = StagedExecutionError<E>;

impl<E> From<ApfsStorageError> for StagedExecutionError<E> {
    fn from(error: ApfsStorageError) -> Self {
        Self::Storage(error)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetiredImage {
    pub retired: RetiredRef,
    pub image: PathBuf,
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum LockMode {
    Wait,
    Try,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PublicationDisposition {
    RolledBack,
    ForwardOnly,
}

#[derive(Debug, Error)]
#[error("{source}")]
pub struct PublicationError {
    disposition: PublicationDisposition,
    #[source]
    source: Box<ApfsStorageError>,
}

impl PublicationError {
    pub fn rolled_back(source: ApfsStorageError) -> Self {
        Self {
            disposition: PublicationDisposition::RolledBack,
            source: Box::new(source),
        }
    }

    pub fn forward_only(source: ApfsStorageError) -> Self {
        Self {
            disposition: PublicationDisposition::ForwardOnly,
            source: Box::new(source),
        }
    }

    pub fn disposition(&self) -> PublicationDisposition {
        self.disposition
    }

    pub fn into_source(self) -> ApfsStorageError {
        *self.source
    }
}

impl From<PublicationError> for ApfsStorageError {
    fn from(error: PublicationError) -> Self {
        error.into_source()
    }
}

/// Synchronous macOS/filesystem boundary. Implementations must use the primitives in
/// `crate::apfs`; the storage executor calls this trait only through [`ApfsBlockingLane`].
pub trait ApfsExecutionHost: Send + Sync + 'static {
    type LockGuard: Send + 'static;
    fn lock_images(
        &self,
        images: &[PathBuf],
        mode: LockMode,
    ) -> Result<Option<Self::LockGuard>, ApfsStorageError>;
    type Attachment: Send + 'static;

    fn observe(&self, expected: &[LifecycleFact]) -> Result<Vec<LifecycleFact>, ApfsStorageError>;
    /// Mint `image`, which must not exist, and hand it back attached and verified but not
    /// mounted: one `clonefile` of the store's formatted blank template at `capacity`, then one
    /// attach (01_storage.md, "Images"). The clone takes the canonical name whole, so the name
    /// only ever holds a complete, formatted volume; it carries the template's label until the
    /// workspace's supervisor relabels it. A failure leaves no image and no attachment.
    fn create_attached(
        &self,
        capacity: ImageCapacity,
        image: &Path,
    ) -> Result<Self::Attachment, ApfsStorageError>;
    /// Clone `source` to `destination` with the source's latest writes in it. `source_mount` is
    /// where the source is mounted when it is a live workspace — its volume is what gets flushed
    /// — and `None` for an image nothing mounts (a staged image, a checkpoint).
    fn clone_image(
        &self,
        source: &Path,
        source_mount: Option<&Path>,
        destination: &Path,
    ) -> Result<(), ApfsStorageError>;
    /// Take a fresh clone's own copy of the extent map it shares with its source, before
    /// anything attaches it. The first write into a clone copies that map, at a cost that grows
    /// with the source's extent count — seconds for a fragmented main — and whatever writes
    /// first pays it: for a clone of a mounted image, the kernel's recovery write inside the
    /// mount. Paid here, it is a step of its own instead of a slow mount.
    fn write_first(&self, image: &Path) -> Result<(), ApfsStorageError>;
    fn resumable_clone(
        &self,
        config: &ApfsSubstrateConfig,
        source: &LifecycleWorkspace,
        destination: &WorkspaceName,
        destination_topology: Revision,
        identity: &OperationIdentity,
    ) -> Result<Option<ResumableClone>, ApfsStorageError> {
        let _ = (config, source, destination, destination_topology, identity);
        Ok(None)
    }
    /// The unfinished adoption whose canonical main still carries `PendingFence`, if any. A main
    /// that is already published is not an adoption to resume and is refused.
    fn pending_adoption(
        &self,
        config: &ApfsSubstrateConfig,
        repo: &RepoId,
    ) -> Result<Option<PendingAdoption>, ApfsStorageError>;
    /// Attach and mount an unfinished adoption's canonical image at its staging mountpoint the
    /// way [`Self::attach_and_mount_resumable`] does, or answer `None` when its payload never
    /// became a verified APFS volume (creation died before formatting finished, or `fsck_apfs`
    /// refuses it); whatever the attempt attached is released first.
    fn resume_pending_adopt(
        &self,
        image: &Path,
        mount_point: &Path,
        workspace: &LifecycleWorkspace,
    ) -> Result<Option<Self::Attachment>, ApfsStorageError> {
        self.attach_and_mount_resumable(image, mount_point, workspace)
            .map(Some)
    }
    /// Release and reclaim an unpublished canonical image no attachment handle is held for:
    /// whatever the kernel still attaches from it, then the image, its companion and sidecar.
    fn discard_pending(&self, image: &Path) -> Result<(), ApfsStorageError>;
    /// Unmount the volume `attachment` holds through the kernel's own unmount, leaving the image
    /// attached for its next mount. No Disk Arbitration round trip.
    fn unmount_attachment(&self, attachment: &Self::Attachment) -> Result<(), ApfsStorageError>;
    fn copy_tree(&self, source: &Path, destination: &Path) -> Result<(), ApfsStorageError>;
    fn attach_verified(&self, image: &Path) -> Result<Self::Attachment, ApfsStorageError>;
    fn mount(
        &self,
        attachment: &Self::Attachment,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<(), ApfsStorageError>;
    /// Attach and mount a pending canonical image, reusing an exact surviving kernel mount and
    /// otherwise replacing an unmounted crash-left attachment before mounting. Implementations
    /// own cleanup when either the mount or replacement attach fails.
    fn attach_and_mount_resumable(
        &self,
        image: &Path,
        mount_point: &Path,
        _workspace: &LifecycleWorkspace,
    ) -> Result<Self::Attachment, ApfsStorageError> {
        let attachment = self.attach_verified(image)?;
        match self.mount(&attachment, mount_point, MountAccess::ReadWrite, false) {
            Ok(()) => Ok(attachment),
            Err(primary) => match self.detach(attachment, DetachIntent::Release) {
                Ok(()) => Err(primary),
                Err(cleanup) => Err(ApfsStorageError::Cleanup {
                    operation: "pending clone mount",
                    primary: Box::new(primary),
                    cleanup: Box::new(cleanup),
                }),
            },
        }
    }
    fn rename_volume(&self, mount_point: &Path, volume_name: &str) -> Result<(), ApfsStorageError>;
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
    fn detach(
        &self,
        attachment: Self::Attachment,
        intent: DetachIntent,
    ) -> Result<(), ApfsStorageError>;
    fn heal_mount(
        &self,
        workspace: &LifecycleWorkspace,
        mount_point: &Path,
    ) -> Result<(), ApfsStorageError>;
    fn retain_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        attachment: Self::Attachment,
    ) -> Result<u64, ApfsStorageError>;
    fn detach_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        intent: DetachIntent,
    ) -> Result<(), ApfsStorageError>;
    /// Mount the build volume the checkout mounted at `checkout` links, when it links one
    /// (16_build_volumes.md, "One link per checkout"): a mounted checkout's build-state paths
    /// always resolve into a mounted volume, and a stale link is re-pointed at `workspace`'s own
    /// volume first (`BuildVolumeLayout::resolve_link`).
    fn ensure_linked_build_volume(
        &self,
        workspace: &LifecycleWorkspace,
        checkout: &Path,
    ) -> Result<(), ApfsStorageError>;
    /// Grow the workspace's image to `capacity` and restore the mount state it was found in.
    ///
    /// Refuses before touching the image when `capacity` does not exceed what the image already
    /// holds: resize only ever grows. A mounted workspace is detached non-forcibly first, so a
    /// volume with work in flight refuses the resize instead of being torn out from under it.
    fn resize(
        &self,
        workspace: &LifecycleWorkspace,
        image: &Path,
        mount_point: &Path,
        capacity: ImageCapacity,
    ) -> Result<ResizeOutcome, ApfsStorageError>;
    /// Rewrite the workspace's image contiguously and restore the mount state it was found in.
    ///
    /// Exactly as `resize` does, a mounted workspace is detached non-forcibly first, so a volume
    /// with work in flight refuses the rewrite before the image is touched. The data is copied
    /// with plain reads and writes into a sibling no enumeration reads as an image, flushed,
    /// renamed over the image, and verified by attaching it before it is mounted again. A copy
    /// that fails leaves the image untouched and puts the workspace back where it was.
    fn defragment(
        &self,
        workspace: &LifecycleWorkspace,
        image: &Path,
        mount_point: &Path,
    ) -> Result<DefragmentOutcome, ApfsStorageError>;
    /// Detach adopted main and atomically restore its exact retained host checkout.
    ///
    /// Implementations derive retry state solely from `source_checkout` — main's mountpoint — and
    /// its exact `pre_cowshed_checkout` sibling. They must never recursively copy or merge either
    /// tree.
    fn restore_adopted_checkout(
        &self,
        workspace: &LifecycleWorkspace,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
    ) -> Result<(), ApfsStorageError>;
    /// Hand the checkout path over to main's mountpoint.
    ///
    /// Builds an empty mountpoint directory under a staging sibling, exchanges it with the
    /// original checkout in one `renameatx_np(RENAME_SWAP)`, and renames the displaced original
    /// to `pre_cowshed_checkout`. The checkout path is never absent. Main mounts there afterwards;
    /// a failed publication is completed explicitly by the controller, not by a repository hook.
    fn vacate_adopted_checkout(
        &self,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
    ) -> Result<(), PublicationError>;
    fn publish_metadata(
        &self,
        image: &Path,
        workspace: &LifecycleWorkspace,
        revision: Revision,
        policy: MetadataPolicy,
        identity: Option<&OperationIdentity>,
        source_image: Option<&Path>,
    ) -> Result<(), ApfsStorageError>;
    /// Publish a pending image: one atomic sidecar rewrite from `PendingFence` to `Active`.
    fn activate_pending(
        &self,
        image: &Path,
        workspace: &LifecycleWorkspace,
    ) -> Result<(), ApfsStorageError> {
        let _ = (image, workspace);
        Ok(())
    }
    fn publish_checkpoint_fact(
        &self,
        image: &Path,
        label: &CheckpointLabel,
        revision: Revision,
        pin: Pin,
    ) -> Result<(), ApfsStorageError>;
    fn restore_swap(
        &self,
        staged: &Path,
        canonical: &Path,
        undo: &Path,
    ) -> Result<(), ApfsStorageError>;
    fn publish_restored_metadata(
        &self,
        staged: &Path,
        canonical: &Path,
        workspace: &LifecycleWorkspace,
        revision: Revision,
        source_image: &Path,
        replaced_incarnation: &WorkspaceIncarnation,
    ) -> Result<PendingPublicationFact, ApfsStorageError>;
    fn activate_restored_metadata(&self, canonical: &Path) -> Result<(), ApfsStorageError>;
    fn rollback_restore(
        &self,
        canonical: &Path,
        undo: &Path,
        staged: &Path,
    ) -> Result<(), ApfsStorageError>;
    fn retire_image(&self, canonical: &Path, trash: &Path) -> Result<(), ApfsStorageError>;
    fn reclaim_image(&self, image: &Path) -> Result<(), ApfsStorageError>;
    fn reclaim_retired(
        &self,
        config: &ApfsSubstrateConfig,
        retired: &RetiredRef,
    ) -> Result<(), ApfsStorageError>;
    /// Reclaims a trash image whose retirement record, its grants sidecar, is already gone. The
    /// caller holds the lock of the workspace name the file carries.
    fn reclaim_unrecorded_retired(
        &self,
        config: &ApfsSubstrateConfig,
        repo: &RepoId,
        image: &Path,
    ) -> Result<(), ApfsStorageError>;
    fn list(&self, repo: &RepoId) -> Result<Vec<StorageFact>, ApfsStorageError>;
    fn pending_publications(
        &self,
        repo: &RepoId,
    ) -> Result<Vec<PendingPublicationFact>, ApfsStorageError>;
    fn mounts(&self, repo: &RepoId) -> Result<Vec<KernelMountFact>, ApfsStorageError>;
    fn checkpoints(&self, repo: &RepoId) -> Result<Vec<CheckpointFact>, ApfsStorageError>;
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
    ) -> Result<StorageGcPlan, ApfsStorageError>;
    fn execute_gc(
        &self,
        config: &ApfsSubstrateConfig,
        plan: StorageGcPlan,
    ) -> Result<StorageGcReport, ApfsStorageError>;
}

#[async_trait]
pub trait ApfsBlockingLane: Send + Sync + 'static {
    async fn dispatch<T, F>(&self, job: F) -> Result<T, ApfsStorageError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, ApfsStorageError> + Send + 'static;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct TokioApfsBlockingLane;

#[async_trait]
impl ApfsBlockingLane for TokioApfsBlockingLane {
    async fn dispatch<T, F>(&self, job: F) -> Result<T, ApfsStorageError>
    where
        T: Send + 'static,
        F: FnOnce() -> Result<T, ApfsStorageError> + Send + 'static,
    {
        tokio::task::spawn_blocking(crate::timing::carried(job))
            .await
            .map_err(|error| ApfsStorageError::BlockingTask(error.to_string()))?
    }
}

#[derive(Debug, Error)]
pub enum ApfsStorageError {
    #[error("APFS operation failed: {0}")]
    Apfs(#[from] ApfsError),
    #[error("storage layout failed: {0}")]
    Layout(#[from] StorageLayoutError),
    #[error("{operation} {path} failed: {source}")]
    Io {
        operation: &'static str,
        path: PathBuf,
        #[source]
        source: io::Error,
    },
    #[error("lifecycle conflict: {0}")]
    Conflict(#[from] super::lifecycle::Conflict),
    #[error("derived APFS state is inconsistent: {0}")]
    Derivation(#[from] super::lifecycle::DerivationError),
    #[error("workspace publication is pending its controller fence: {0}")]
    PendingPublication(PathBuf),
    #[error("blocking APFS task failed: {0}")]
    BlockingTask(String),
    #[error("requested capacity {requested} does not exceed the image's current {current}")]
    CapacityNotGrowing {
        current: ImageCapacity,
        requested: ImageCapacity,
    },
    #[error("resized image reports {observed}, short of the requested {requested}")]
    ResizeNotObserved {
        requested: ImageCapacity,
        observed: ImageCapacity,
    },
    #[error(
        "rewriting {path} needs {needed} free bytes on its volume and {available} are available"
    )]
    InsufficientSpace {
        path: PathBuf,
        needed: u64,
        available: u64,
    },
    #[error("unexpected lifecycle operation result")]
    UnexpectedResult,
    #[error("invalid APFS lifecycle plan: {0}")]
    InvalidPlan(&'static str),
    #[error("APFS host operation failed: {0}")]
    Host(String),
    #[error("garbage-collection plan is stale")]
    GcPlanStale,
    #[error("{layout} is missing its CA companion: image={image}, companion={companion}")]
    MissingCaCompanion {
        layout: &'static str,
        image: PathBuf,
        companion: PathBuf,
    },
    #[error("marker does not match detached APFS metadata: {0}")]
    MarkerMismatch(String),
    #[error("workspace {workspace} quarantined ({reason}): {tombstone}")]
    Quarantined {
        workspace: WorkspaceName,
        reason: String,
        tombstone: PathBuf,
    },
    #[error("cleanup after {operation} failed: primary={primary}; cleanup={cleanup}")]
    Cleanup {
        operation: &'static str,
        primary: Box<ApfsStorageError>,
        cleanup: Box<ApfsStorageError>,
    },
}

impl From<ExecuteError<ApfsStorageError>> for ApfsStorageError {
    fn from(error: ExecuteError<ApfsStorageError>) -> Self {
        match error {
            ExecuteError::Conflict(conflict) => Self::Conflict(conflict),
            ExecuteError::Backend(error) => error,
        }
    }
}

#[derive(Debug)]
enum Applied {
    Lifecycle(LifecycleReceipt),
    Retired(RetiredRef),
}

pub struct ApfsSubstrate<H, L = TokioApfsBlockingLane> {
    planner: PurePlanner,
    host: Arc<H>,
    lane: Arc<L>,
    config: Arc<ApfsSubstrateConfig>,
    incarnations: Arc<dyn IncarnationSource>,
}

impl<H, L> Clone for ApfsSubstrate<H, L> {
    fn clone(&self) -> Self {
        Self {
            planner: self.planner,
            host: Arc::clone(&self.host),
            lane: Arc::clone(&self.lane),
            config: Arc::clone(&self.config),
            incarnations: Arc::clone(&self.incarnations),
        }
    }
}

impl<H> ApfsSubstrate<H, TokioApfsBlockingLane>
where
    H: ApfsExecutionHost,
{
    pub fn new(config: ApfsSubstrateConfig, host: H) -> Self {
        Self::with_lane(config, host, TokioApfsBlockingLane)
    }
}

impl<H, L> ApfsSubstrate<H, L>
where
    H: ApfsExecutionHost,
    L: ApfsBlockingLane,
{
    pub fn with_lane(config: ApfsSubstrateConfig, host: H, lane: L) -> Self {
        Self::with_lane_and_incarnations(config, host, lane, UuidIncarnationSource)
    }

    pub fn with_lane_and_incarnations(
        config: ApfsSubstrateConfig,
        host: H,
        lane: L,
        incarnations: impl IncarnationSource,
    ) -> Self {
        Self {
            planner: PurePlanner,
            host: Arc::new(host),
            lane: Arc::new(lane),
            config: Arc::new(config),
            incarnations: Arc::new(incarnations),
        }
    }

    pub fn config(&self) -> &ApfsSubstrateConfig {
        &self.config
    }
    pub fn host(&self) -> &H {
        &self.host
    }

    /// The host, for work that outlives the call that started it.
    pub fn shared_host(&self) -> Arc<H> {
        Arc::clone(&self.host)
    }

    /// Detach main and atomically restore the exact checkout retained by adoption.
    pub async fn restore_adopted_checkout(
        &self,
        workspace: &LifecycleWorkspace,
        pre_cowshed_checkout: &Path,
    ) -> Result<(), ApfsStorageError> {
        if !workspace.name().is_main()
            || self.config.checkout_path == pre_cowshed_checkout
            || pre_cowshed_checkout.parent() != self.config.checkout_path.parent()
        {
            return Err(ApfsStorageError::InvalidPlan(
                "adoption rollback requires main and its exact pre-cowshed sibling",
            ));
        }
        let mut expected_pre = self.config.checkout_path.as_os_str().to_owned();
        expected_pre.push(".pre-cowshed");
        if Path::new(&expected_pre) != pre_cowshed_checkout {
            return Err(ApfsStorageError::InvalidPlan(
                "adoption rollback requires main and its exact pre-cowshed sibling",
            ));
        }

        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let workspace = workspace.clone();
        let source_checkout = self.config.checkout_path.clone();
        let pre_cowshed_checkout = pre_cowshed_checkout.to_owned();
        self.dispatch_with_locks(lock_paths, true, move |host, _| {
            host.restore_adopted_checkout(&workspace, &source_checkout, &pre_cowshed_checkout)
        })
        .await
    }

    /// Complete an adoption whose main is published but whose checkout swap or mount at the
    /// checkout was interrupted: swap the checkout for main's mountpoint if that has not happened,
    /// then mount main there, reusing a surviving attachment. Idempotent once main is mounted.
    pub async fn finish_adoption(
        &self,
        workspace: &LifecycleWorkspace,
        pre_cowshed_checkout: &Path,
    ) -> Result<(), ApfsStorageError> {
        if !workspace.name().is_main() {
            return Err(ApfsStorageError::InvalidPlan(
                "only main's adoption has a checkout to finish",
            ));
        }
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let workspace = workspace.clone();
        let pre_cowshed_checkout = pre_cowshed_checkout.to_owned();
        self.dispatch_with_locks(lock_paths, true, move |host, config| {
            finish_adoption(host.as_ref(), &config, &workspace, &pre_cowshed_checkout)
        })
        .await
    }

    /// Retire an unpublished main nothing will finish: release whatever the kernel still attaches
    /// from it and reclaim the image with its sidecar and companion. Only the exact `incarnation`
    /// the caller judged abandoned is touched, re-read under main's lifecycle lock; the checkout
    /// is never involved, because an unpublished adoption never changed it.
    pub async fn discard_pending_adoption(
        &self,
        repo: &RepoId,
        incarnation: &WorkspaceIncarnation,
    ) -> Result<(), ApfsStorageError> {
        let lock_paths = vec![workspace_lock_path(&self.config, repo, &main_name())?];
        let repo = repo.clone();
        let incarnation = incarnation.clone();
        self.dispatch_with_locks(lock_paths, true, move |host, config| {
            match host.pending_adoption(&config, &repo)? {
                Some(pending) if pending.incarnation == incarnation => {
                    host.discard_pending(&pending.image)
                }
                Some(_) => Err(ApfsStorageError::InvalidPlan(
                    "unpublished main changed incarnation before its retirement",
                )),
                None => Ok(()),
            }
        })
        .await
    }

    /// Create main's canonical image behind its `PendingFence`, mount it at a staging mountpoint
    /// for the controller to initialize, then publish it and mount it at the checkout.
    ///
    /// The lifecycle lock remains owned across the callback. The image stays unpublished and the
    /// checkout untouched until `initialize` returns success.
    pub async fn execute_adopt_staged<F, Fut, E>(
        &self,
        plan: AdoptPlan,
        initialize: F,
    ) -> Result<LifecycleReceipt, AdoptExecutionError<E>>
    where
        F: FnOnce(AdoptStage) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send,
    {
        let backend = CheckedApfsBackend {
            host: Arc::clone(&self.host),
            lane: Arc::clone(&self.lane),
            config: Arc::clone(&self.config),
            incarnations: Arc::clone(&self.incarnations),
            expected: plan.expected().to_vec(),
        };
        let mut guard = backend.acquire(plan.operation()).await?;
        let actual = backend
            .read_authoritative(&mut guard, plan.expected())
            .await?;
        revalidate(plan.expected(), &actual).map_err(ApfsStorageError::from)?;

        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let incarnations = Arc::clone(&self.incarnations);
        let expected = plan.expected().to_vec();
        let operation = plan.operation().clone();
        let prepared = self
            .lane
            .dispatch(move || {
                let Operation::Adopt {
                    repo,
                    capacity,
                    source_checkout,
                    pre_cowshed_checkout,
                    identity,
                } = &operation
                else {
                    return Err(ApfsStorageError::InvalidPlan(
                        "staged adopt executor requires an adopt operation",
                    ));
                };
                prepare_adopt_stage(
                    host.as_ref(),
                    &config,
                    &expected,
                    AdoptExecution {
                        repo,
                        capacity: *capacity,
                        source_checkout,
                        pre_cowshed_checkout,
                        identity,
                    },
                    incarnations.as_ref(),
                )
            })
            .await?;
        let prepared =
            StagedCallbackGuard::new(Arc::clone(&self.host), prepared, abort_prepared_adopt::<H>);

        if let Err(initializer) = initialize(prepared.get().stage.clone()).await {
            let prepared = prepared.into_prepared();
            let host = Arc::clone(&self.host);
            let cleanup = self
                .lane
                .dispatch(move || abort_prepared_adopt(host.as_ref(), prepared))
                .await;
            return Err(match cleanup {
                Ok(()) => StagedExecutionError::Initializer(initializer),
                Err(cleanup) => StagedExecutionError::InitializerCleanup {
                    initializer,
                    cleanup,
                },
            });
        }

        let prepared = prepared.into_prepared();
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let applied = self
            .lane
            .dispatch(move || commit_prepared_adopt(host.as_ref(), &config, prepared))
            .await?;
        match applied {
            Applied::Lifecycle(receipt) => Ok(receipt),
            _ => Err(ApfsStorageError::UnexpectedResult.into()),
        }
    }

    pub async fn execute_create_staged<F, Fut, E>(
        &self,
        plan: CreatePlan,
        initialize: F,
    ) -> Result<LifecycleReceipt, CreateExecutionError<E>>
    where
        F: FnOnce(CreateStage) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send + std::fmt::Display,
    {
        self.execute_clone_staged(plan, CloneKind::Create, initialize)
            .await
    }

    pub async fn execute_fork_staged<F, Fut, E>(
        &self,
        plan: ForkPlan,
        initialize: F,
    ) -> Result<LifecycleReceipt, ForkExecutionError<E>>
    where
        F: FnOnce(ForkStage) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send + std::fmt::Display,
    {
        self.execute_clone_staged(plan, CloneKind::Fork, initialize)
            .await
    }

    async fn execute_clone_staged<P, F, Fut, E>(
        &self,
        plan: P,
        kind: CloneKind,
        initialize: F,
    ) -> Result<LifecycleReceipt, StagedExecutionError<E>>
    where
        P: ImmutablePlan,
        F: FnOnce(WorkspaceStage) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send + std::fmt::Display,
    {
        let backend = CheckedApfsBackend {
            host: Arc::clone(&self.host),
            lane: Arc::clone(&self.lane),
            config: Arc::clone(&self.config),
            incarnations: Arc::clone(&self.incarnations),
            expected: plan.expected().to_vec(),
        };
        let mut guard = backend.acquire(plan.operation()).await?;
        let actual = backend
            .read_authoritative(&mut guard, plan.expected())
            .await?;
        revalidate(plan.expected(), &actual).map_err(ApfsStorageError::from)?;

        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let incarnations = Arc::clone(&self.incarnations);
        let expected = plan.expected().to_vec();
        let operation = plan.operation().clone();
        let prepared = self
            .lane
            .dispatch(move || {
                let (source, destination, identity, operation_kind) = match &operation {
                    Operation::Create {
                        source,
                        destination,
                        identity,
                    } => (source, destination, identity, CloneKind::Create),
                    Operation::Fork {
                        source,
                        destination,
                        identity,
                    } => (source, destination, identity, CloneKind::Fork),
                    _ => {
                        return Err(ApfsStorageError::InvalidPlan(
                            "staged clone executor requires a create or fork operation",
                        ));
                    }
                };
                if operation_kind != kind {
                    return Err(ApfsStorageError::InvalidPlan(
                        "staged clone executor operation kind mismatch",
                    ));
                }
                prepare_clone_stage(
                    host.as_ref(),
                    &config,
                    &expected,
                    CloneExecution {
                        source,
                        destination,
                        fork: kind == CloneKind::Fork,
                        identity,
                    },
                    incarnations.as_ref(),
                    false,
                )
            })
            .await?;
        let prepared = StagedCallbackGuard::new(
            Arc::clone(&self.host),
            prepared,
            preserve_prepared_clone::<H>,
        );

        let initialized = timed_async(
            "apfs",
            "canonical/init",
            initialize(prepared.get().stage.clone()),
        )
        .await;
        if let Err(initializer) = initialized {
            // The callback can already have changed state outside the image (Git branches,
            // worktree registrations, remotes). The PendingFence is the rollback boundary: keep
            // the exact mounted clone so the durable lifecycle intent can re-enter the
            // initializer with resume authority rather than orphaning those external effects.
            let _prepared = prepared.into_prepared();
            return Err(StagedExecutionError::Initializer(initializer));
        }
        let prepared = prepared.into_prepared();
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let applied = self
            .lane
            .dispatch(move || commit_prepared_clone(host.as_ref(), &config, prepared))
            .await?;
        match applied {
            Applied::Lifecycle(receipt) => Ok(receipt),
            _ => Err(ApfsStorageError::UnexpectedResult.into()),
        }
    }

    /// Retire an unfinished clone without ever publishing it as runnable. Inspection runs
    /// on its verified mount under the image lock; a refusal preserves the pending payload.
    pub async fn execute_pending_clone_retirement<P, F, Fut, R, E>(
        &self,
        plan: P,
        inspect: F,
    ) -> Result<(RetiredRef, R), StagedExecutionError<E>>
    where
        P: ImmutablePlan,
        F: FnOnce(WorkspaceStage) -> Fut + Send,
        Fut: Future<Output = Result<R, E>> + Send,
        R: Send,
        E: Send,
    {
        let backend = CheckedApfsBackend {
            host: Arc::clone(&self.host),
            lane: Arc::clone(&self.lane),
            config: Arc::clone(&self.config),
            incarnations: Arc::clone(&self.incarnations),
            expected: plan.expected().to_vec(),
        };
        let mut guard = backend.acquire(plan.operation()).await?;
        let actual = backend
            .read_authoritative(&mut guard, plan.expected())
            .await?;
        revalidate(plan.expected(), &actual).map_err(ApfsStorageError::from)?;

        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let incarnations = Arc::clone(&self.incarnations);
        let expected = plan.expected().to_vec();
        let operation = plan.operation().clone();
        let prepared = self
            .lane
            .dispatch(move || {
                let (source, destination, identity, fork) = match &operation {
                    Operation::Create {
                        source,
                        destination,
                        identity,
                    } => (source, destination, identity, false),
                    Operation::Fork {
                        source,
                        destination,
                        identity,
                    } => (source, destination, identity, true),
                    _ => {
                        return Err(ApfsStorageError::InvalidPlan(
                            "pending clone retirement requires a create or fork plan",
                        ));
                    }
                };
                prepare_clone_stage(
                    host.as_ref(),
                    &config,
                    &expected,
                    CloneExecution {
                        source,
                        destination,
                        fork,
                        identity,
                    },
                    incarnations.as_ref(),
                    true,
                )
            })
            .await?;
        let prepared = StagedCallbackGuard::new(
            Arc::clone(&self.host),
            prepared,
            preserve_prepared_clone::<H>,
        );
        let value = match inspect(prepared.get().stage.clone()).await {
            Ok(value) => value,
            Err(error) => {
                let _prepared = prepared.into_prepared();
                return Err(StagedExecutionError::Initializer(error));
            }
        };
        let prepared = prepared.into_prepared();
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let retired = self
            .lane
            .dispatch(move || {
                let PreparedClone {
                    stage,
                    attachment,
                    image,
                } = prepared;
                let trash = retired_image_path(&config, &stage.workspace)?;
                let revision = stage.workspace.revision().get().checked_add(1).ok_or(
                    ApfsStorageError::InvalidPlan("pending retirement revision overflow"),
                )?;
                host.detach(attachment, DetachIntent::Release)?;
                host.retire_image(&image, &trash)?;
                Ok(RetiredRef::new(stage.workspace, Revision::new(revision)))
            })
            .await?;
        Ok((retired, value))
    }

    pub async fn execute_checkpoint_staged<F, Fut, E>(
        &self,
        plan: CheckpointPlan,
        initialize: F,
    ) -> Result<CheckpointRef, CheckpointExecutionError<E>>
    where
        F: FnOnce(CheckpointStage) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send,
    {
        let backend = CheckedApfsBackend {
            host: Arc::clone(&self.host),
            lane: Arc::clone(&self.lane),
            config: Arc::clone(&self.config),
            incarnations: Arc::clone(&self.incarnations),
            expected: plan.expected().to_vec(),
        };
        let mut guard = backend.acquire(plan.operation()).await?;
        let actual = backend
            .read_authoritative(&mut guard, plan.expected())
            .await?;
        revalidate(plan.expected(), &actual).map_err(ApfsStorageError::from)?;

        let config = Arc::clone(&self.config);
        let expected = plan.expected().to_vec();
        let operation = plan.operation().clone();
        let planned = {
            let Operation::Checkpoint {
                workspace,
                label,
                pin,
            } = &operation
            else {
                return Err(ApfsStorageError::InvalidPlan(
                    "staged checkpoint executor requires a checkpoint operation",
                )
                .into());
            };
            plan_checkpoint_stage(&config, &expected, workspace, label, *pin)?
        };

        if let Err(initializer) = initialize(planned.stage.clone()).await {
            return Err(StagedExecutionError::Initializer(initializer));
        }

        let host = Arc::clone(&self.host);
        self.lane
            .dispatch(move || {
                let prepared = prepare_checkpoint_stage(host.as_ref(), planned)?;
                commit_prepared_checkpoint(host.as_ref(), prepared)
            })
            .await
            .map_err(Into::into)
    }

    /// Restore `plan`'s checkpoint, then publish a replacement through `fence` before it becomes
    /// discoverable. Staging and the swap run as one lane step, as a checkpoint does, so nothing
    /// observes a half-prepared restore; a fence failure is forward-only and names the pending
    /// publication for the caller or startup recovery to finish.
    pub async fn execute_restore_staged<Fence, FenceFut, FenceError>(
        &self,
        plan: RestorePlan,
        fence: Fence,
    ) -> Result<RestoreReceipt, RestoreExecutionError<FenceError>>
    where
        Fence: FnOnce(RestoreFence) -> FenceFut + Send,
        FenceFut: Future<Output = Result<(), FenceError>> + Send,
        FenceError: Send,
    {
        let backend = CheckedApfsBackend {
            host: Arc::clone(&self.host),
            lane: Arc::clone(&self.lane),
            config: Arc::clone(&self.config),
            incarnations: Arc::clone(&self.incarnations),
            expected: plan.expected().to_vec(),
        };
        let mut guard = backend.acquire(plan.operation()).await?;
        let actual = backend
            .read_authoritative(&mut guard, plan.expected())
            .await?;
        revalidate(plan.expected(), &actual).map_err(ApfsStorageError::from)?;

        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let incarnations = Arc::clone(&self.incarnations);
        let expected = plan.expected().to_vec();
        let operation = plan.operation().clone();
        let committed = self
            .lane
            .dispatch(move || {
                let Operation::Restore {
                    workspace,
                    label,
                    mode,
                    identity,
                } = &operation
                else {
                    return Err(ApfsStorageError::InvalidPlan(
                        "staged restore executor requires a restore operation",
                    ));
                };
                let prepared = prepare_restore_stage(
                    host.as_ref(),
                    &config,
                    &expected,
                    RestoreExecution {
                        workspace,
                        label,
                        mode: *mode,
                        identity,
                    },
                    incarnations.as_ref(),
                )?;
                commit_prepared_restore(host.as_ref(), &config, prepared)
            })
            .await?;
        let CommittedRestore::Pending(pending) = committed else {
            let CommittedRestore::Verified(receipt) = committed else {
                unreachable!()
            };
            return Ok(receipt);
        };

        let fence_input = RestoreFence {
            pending: pending.fact.clone(),
        };
        if let Err(source) = fence(fence_input).await {
            return Err(RestoreExecutionError::Fence {
                source,
                pending: Box::new(pending.fact),
            });
        }
        let host = Arc::clone(&self.host);
        if let Err(source) = self
            .lane
            .dispatch({
                let image = pending.fact.image.clone();
                move || host.activate_restored_metadata(&image)
            })
            .await
        {
            return Err(RestoreExecutionError::Activation {
                source: Box::new(source),
                pending: Box::new(pending.fact),
            });
        }
        Ok(pending.receipt)
    }
    /// Make a workspace durably undiscoverable, then publish its retirement before reclamation.
    ///
    /// A callback failure is forward-only: the returned retired reference names the preserved
    /// trash image, allowing the caller or startup recovery to retry lifecycle publication before
    /// idempotent reclamation.
    pub async fn execute_retire_staged<F, Fut, E>(
        &self,
        plan: RetirePlan,
        fence: F,
    ) -> Result<RetiredRef, RetireExecutionError<E>>
    where
        F: FnOnce(RetiredRef) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send,
    {
        let retired = match self.execute(&plan).await? {
            Applied::Retired(retired) => retired,
            _ => return Err(ApfsStorageError::UnexpectedResult.into()),
        };
        if let Err(source) = fence(retired.clone()).await {
            return Err(RetireExecutionError::Fence { source, retired });
        }
        Ok(retired)
    }

    /// Retire adopted main only after its exact pre-cowshed checkout has been restored.
    ///
    /// Ordinary lifecycle planning keeps main permanent. This narrow terminal path requires the
    /// main image to be detached and still match the exact current lifecycle identity, then moves
    /// its image, sidecar, and CA key to recoverable trash before publishing the retirement fence.
    pub async fn execute_restored_main_retirement<F, Fut, E>(
        &self,
        workspace: &LifecycleWorkspace,
        fence: F,
    ) -> Result<RetiredRef, RetireExecutionError<E>>
    where
        F: FnOnce(RetiredRef) -> Fut + Send,
        Fut: Future<Output = Result<(), E>> + Send,
        E: Send,
    {
        if !workspace.name().is_main() {
            return Err(
                ApfsStorageError::InvalidPlan("restored-main retirement requires main").into(),
            );
        }
        let workspace = workspace.clone();
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let retired = self
            .dispatch_with_locks(lock_paths, true, move |host, config| {
                let volume = volume_key(workspace.repo(), workspace.name());
                if host
                    .mounts(workspace.repo())?
                    .iter()
                    .any(|mount| mount.volume_key == volume)
                {
                    return Err(ApfsStorageError::InvalidPlan(
                        "restored main image remains mounted",
                    ));
                }
                if !host
                    .list(workspace.repo())?
                    .into_iter()
                    .any(|fact| fact.workspace == workspace)
                {
                    return Err(ApfsStorageError::MarkerMismatch(
                        "restored main image no longer matches its lifecycle identity".to_owned(),
                    ));
                }
                let canonical = canonical_image_path(&config, &workspace)?;
                let trash = retired_image_path(&config, &workspace)?;
                host.retire_image(&canonical, &trash)?;
                let revision = workspace.revision().get().checked_add(1).ok_or(
                    ApfsStorageError::InvalidPlan("restored main retirement revision overflow"),
                )?;
                Ok(RetiredRef::new(workspace, Revision::new(revision)))
            })
            .await?;
        if let Err(source) = fence(retired.clone()).await {
            return Err(RetireExecutionError::Fence { source, retired });
        }
        Ok(retired)
    }

    async fn execute<P: ImmutablePlan>(&self, plan: &P) -> Result<Applied, ApfsStorageError> {
        let backend = CheckedApfsBackend {
            host: Arc::clone(&self.host),
            lane: Arc::clone(&self.lane),
            config: Arc::clone(&self.config),
            incarnations: Arc::clone(&self.incarnations),
            expected: plan.expected().to_vec(),
        };
        execute_checked(&backend, plan).await.map_err(Into::into)
    }

    async fn dispatch_read<T, F>(&self, job: F) -> Result<T, ApfsStorageError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<H>, Arc<ApfsSubstrateConfig>) -> Result<T, ApfsStorageError> + Send + 'static,
    {
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        self.lane.dispatch(move || job(host, config)).await
    }

    async fn dispatch_with_locks<T, F>(
        &self,
        lock_paths: Vec<PathBuf>,
        recover: bool,
        job: F,
    ) -> Result<T, ApfsStorageError>
    where
        T: Send + 'static,
        F: FnOnce(Arc<H>, Arc<ApfsSubstrateConfig>) -> Result<T, ApfsStorageError> + Send + 'static,
    {
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        self.lane
            .dispatch(move || {
                let _guard = host.lock_images(&lock_paths, LockMode::Wait)?.ok_or(
                    ApfsStorageError::InvalidPlan("blocking image lock unexpectedly unavailable"),
                )?;
                if recover {
                    host.recover_pending(&config, &lock_paths)?;
                }
                job(host, config)
            })
            .await
    }
    #[cfg(any(target_os = "macos", test))]
    /// Runs a non-lifecycle metadata mutation while holding the same hardened image lock used by
    /// APFS lifecycle operations. The job's result is deliberately opaque to this layer so callers
    /// can preserve their own error taxonomy while lock acquisition remains an APFS concern.
    pub(crate) async fn dispatch_with_image_lock<T, F>(
        &self,
        lock_path: PathBuf,
        job: F,
    ) -> Result<T, ApfsStorageError>
    where
        T: Send + 'static,
        F: FnOnce() -> T + Send + 'static,
    {
        self.dispatch_with_locks(vec![lock_path], false, move |_, _| Ok(job()))
            .await
    }

    /// Reclaims a trash image whose grants sidecar is gone, under the lock of the name its
    /// `<workspace>-<incarnation>` file name carries: a retirement holds that lock between moving
    /// the image and moving its sidecar, so waiting on it means never deleting one mid-flight.
    pub async fn reclaim_unrecorded_retired(
        &self,
        repo: &RepoId,
        image: PathBuf,
    ) -> Result<(), ApfsStorageError> {
        let (name, _) = image
            .file_stem()
            .and_then(|stem| stem.to_str())
            .and_then(split_retired_stem)
            .ok_or_else(|| {
                ApfsStorageError::Host(format!(
                    "trash image is not named <workspace>-<incarnation>: {}",
                    image.display()
                ))
            })?;
        let lock_paths = vec![workspace_lock_path(&self.config, repo, &name)?];
        let repo = repo.clone();
        self.dispatch_with_locks(lock_paths, true, move |host, config| {
            host.reclaim_unrecorded_retired(&config, &repo, &image)
        })
        .await
    }
}

impl<H, L> LifecyclePlanner for ApfsSubstrate<H, L>
where
    H: ApfsExecutionHost,
    L: ApfsBlockingLane,
{
    fn plan_adopt(&self, request: AdoptRequest) -> Result<AdoptPlan, PlanError> {
        self.planner.plan_adopt(request)
    }

    fn plan_create(
        &self,
        from: &LifecycleWorkspace,
        destination: Destination,
    ) -> Result<CreatePlan, PlanError> {
        self.planner.plan_create(from, destination)
    }

    fn plan_fork(
        &self,
        from: &LifecycleWorkspace,
        destination: Destination,
    ) -> Result<ForkPlan, PlanError> {
        self.planner.plan_fork(from, destination)
    }

    fn plan_checkpoint(
        &self,
        workspace: &LifecycleWorkspace,
        label: CheckpointLabel,
        pin: Pin,
    ) -> Result<CheckpointPlan, PlanError> {
        self.planner.plan_checkpoint(workspace, label, pin)
    }
    fn plan_restore(
        &self,
        workspace: &LifecycleWorkspace,
        checkpoint: &CheckpointRef,
        mode: RestoreMode,
        identity: OperationIdentity,
    ) -> Result<RestorePlan, PlanError> {
        self.planner
            .plan_restore(workspace, checkpoint, mode, identity)
    }

    fn plan_retire(&self, workspace: &LifecycleWorkspace) -> Result<RetirePlan, PlanError> {
        self.planner.plan_retire(workspace)
    }
}

#[async_trait]
impl<H, L> Substrate for ApfsSubstrate<H, L>
where
    H: ApfsExecutionHost,
    L: ApfsBlockingLane,
{
    type Error = ApfsStorageError;

    async fn execute_retire(&self, plan: RetirePlan) -> Result<RetiredRef, Self::Error> {
        match self.execute(&plan).await? {
            Applied::Retired(retired) => Ok(retired),
            _ => Err(ApfsStorageError::UnexpectedResult),
        }
    }

    async fn reclaim(&self, retired: RetiredRef) -> Result<(), Self::Error> {
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            retired.workspace().repo(),
            retired.workspace().name(),
        )?];
        self.dispatch_with_locks(lock_paths, true, move |host, config| {
            host.reclaim_retired(&config, &retired)
        })
        .await
    }

    async fn list(&self, repo: &RepoId) -> Result<Vec<DerivedWorkspace>, Self::Error> {
        let repo = repo.clone();
        self.dispatch_read(move |host, _| {
            let storage = host.list(&repo)?;
            let mounts = host.mounts(&repo)?;
            let checkpoints = host.checkpoints(&repo)?;
            Ok(super::lifecycle::derive_workspaces(
                storage,
                mounts,
                checkpoints,
            )?)
        })
        .await
    }

    async fn mount_state(&self, workspace: &LifecycleWorkspace) -> Result<MountState, Self::Error> {
        let workspace = workspace.clone();
        self.dispatch_read(move |host, _| {
            let storage = host.list(workspace.repo())?;
            let mounts = host.mounts(workspace.repo())?;
            let checkpoints = host.checkpoints(workspace.repo())?;
            let derived = super::lifecycle::derive_workspaces(storage, mounts, checkpoints)?;
            derived
                .into_iter()
                .find(|candidate| candidate.workspace == workspace)
                .map(|candidate| candidate.mount_state)
                .ok_or(ApfsStorageError::InvalidPlan("workspace is not published"))
        })
        .await
    }

    async fn ensure_mounted(
        &self,
        workspace: &LifecycleWorkspace,
        intent: MountIntent,
    ) -> Result<PathBuf, Self::Error> {
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let workspace = workspace.clone();
        self.dispatch_with_locks(lock_paths, true, move |host, config| {
            let mount_point = mount_point(&config, &workspace)?;
            host.heal_mount(&workspace, &mount_point)?;
            let storage = host.list(workspace.repo())?;
            let mounts = host.mounts(workspace.repo())?;
            let checkpoints = host.checkpoints(workspace.repo())?;
            let derived = super::lifecycle::derive_workspaces(storage, mounts, checkpoints)?;
            let state = derived
                .into_iter()
                .find(|candidate| candidate.workspace == workspace)
                .map(|candidate| candidate.mount_state)
                .ok_or(ApfsStorageError::InvalidPlan("workspace is not published"))?;
            if matches!(state, MountState::Mounted { .. }) {
                host.validate_marker(&mount_point, &MarkerExpectation::owned(&config, &workspace))?;
                host.ensure_linked_build_volume(&workspace, &mount_point)?;
                return Ok(mount_point);
            }
            let canonical = canonical_image_path(&config, &workspace)?;
            let attachment = host.attach_verified(&canonical)?;
            if let Err(primary) = host
                .mount(
                    &attachment,
                    &mount_point,
                    MountAccess::ReadWrite,
                    intent.browse,
                )
                .and_then(|()| {
                    host.validate_marker(
                        &mount_point,
                        &MarkerExpectation::owned(&config, &workspace),
                    )
                })
            {
                return detach_after_failure(host.as_ref(), attachment, primary, "mount workspace");
            }
            host.retain_mounted(&workspace, attachment)?;
            host.ensure_linked_build_volume(&workspace, &mount_point)?;
            Ok(mount_point)
        })
        .await
    }

    async fn unmount(&self, workspace: &LifecycleWorkspace) -> Result<(), Self::Error> {
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let workspace = workspace.clone();
        // The volume is the user's to be working in: an explicit unmount that cannot land is a
        // busy conflict for the caller to report, never grounds to force it out from under them.
        self.dispatch_with_locks(lock_paths, true, move |host, _| {
            host.detach_mounted(&workspace, DetachIntent::WhenIdle)
        })
        .await
    }

    async fn resize(
        &self,
        workspace: &LifecycleWorkspace,
        capacity: ImageCapacity,
    ) -> Result<ResizeOutcome, Self::Error> {
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let workspace = workspace.clone();
        self.dispatch_with_locks(lock_paths, true, move |host, config| {
            let image = canonical_image_path(&config, &workspace)?;
            let mount_point = mount_point(&config, &workspace)?;
            host.resize(&workspace, &image, &mount_point, capacity)
        })
        .await
    }

    async fn defragment(
        &self,
        workspace: &LifecycleWorkspace,
    ) -> Result<DefragmentOutcome, Self::Error> {
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
        )?];
        let workspace = workspace.clone();
        self.dispatch_with_locks(lock_paths, true, move |host, config| {
            let image = canonical_image_path(&config, &workspace)?;
            let mount_point = mount_point(&config, &workspace)?;
            host.defragment(&workspace, &image, &mount_point)
        })
        .await
    }

    async fn stats(&self, workspace: &LifecycleWorkspace) -> Result<SubstrateStats, Self::Error> {
        let workspace = workspace.clone();
        self.dispatch_read(move |host, config| {
            let image = canonical_image_path(&config, &workspace)?;
            host.stats(&workspace, &image)
        })
        .await
    }

    async fn preview_gc(&self, repo: &RepoId) -> Result<StorageGcPlan, Self::Error> {
        let repo = repo.clone();
        self.dispatch_read(move |host, config| host.preview_gc(&config, &repo))
            .await
    }

    async fn execute_gc(&self, plan: StorageGcPlan) -> Result<StorageGcReport, Self::Error> {
        self.dispatch_read(move |host, config| host.execute_gc(&config, plan))
            .await
    }
}

struct StagedCallbackGuard<H, P>
where
    H: ApfsExecutionHost,
    P: Send,
{
    host: Arc<H>,
    prepared: Option<P>,
    abort: fn(&H, P) -> Result<(), ApfsStorageError>,
}

impl<H, P> StagedCallbackGuard<H, P>
where
    H: ApfsExecutionHost,
    P: Send,
{
    fn new(host: Arc<H>, prepared: P, abort: fn(&H, P) -> Result<(), ApfsStorageError>) -> Self {
        Self {
            host,
            prepared: Some(prepared),
            abort,
        }
    }

    fn get(&self) -> &P {
        self.prepared.as_ref().expect("armed staged callback guard")
    }

    fn into_prepared(mut self) -> P {
        self.prepared.take().expect("armed staged callback guard")
    }
}

impl<H, P> Drop for StagedCallbackGuard<H, P>
where
    H: ApfsExecutionHost,
    P: Send,
{
    fn drop(&mut self) {
        let Some(prepared) = self.prepared.take() else {
            return;
        };
        let host = Arc::clone(&self.host);
        let abort = self.abort;
        std::thread::scope(|scope| {
            let _ = scope.spawn(move || abort(host.as_ref(), prepared)).join();
        });
    }
}

struct CheckedGuard<G> {
    _lock: G,
    paths: Vec<PathBuf>,
}

struct CheckedApfsBackend<H, L> {
    host: Arc<H>,
    lane: Arc<L>,
    config: Arc<ApfsSubstrateConfig>,
    incarnations: Arc<dyn IncarnationSource>,
    expected: Vec<LifecycleFact>,
}

#[async_trait]
impl<H, L> LifecycleBackend for CheckedApfsBackend<H, L>
where
    H: ApfsExecutionHost,
    L: ApfsBlockingLane,
{
    type Guard = CheckedGuard<H::LockGuard>;
    type Output = Applied;
    type Error = ApfsStorageError;

    async fn acquire(&self, operation: &Operation) -> Result<Self::Guard, Self::Error> {
        let paths = operation_lock_paths(&self.config, &self.expected, operation)?;
        let host = Arc::clone(&self.host);
        self.lane
            .dispatch(move || {
                let lock = host.lock_images(&paths, LockMode::Wait)?.ok_or(
                    ApfsStorageError::InvalidPlan("blocking image lock unexpectedly unavailable"),
                )?;
                Ok(CheckedGuard { _lock: lock, paths })
            })
            .await
    }

    async fn read_authoritative(
        &self,
        guard: &mut Self::Guard,
        expected: &[LifecycleFact],
    ) -> Result<Vec<LifecycleFact>, Self::Error> {
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let held_locks = guard.paths.clone();
        let expected = expected.to_vec();
        self.lane
            .dispatch(move || {
                host.recover_pending(&config, &held_locks)?;
                host.observe(&expected)
            })
            .await
    }

    async fn apply(
        &self,
        _: &mut Self::Guard,
        operation: &Operation,
    ) -> Result<Self::Output, Self::Error> {
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let incarnations = Arc::clone(&self.incarnations);
        let expected = self.expected.clone();
        let operation = operation.clone();
        self.lane
            .dispatch(move || {
                apply_operation(
                    host.as_ref(),
                    &config,
                    &expected,
                    &operation,
                    incarnations.as_ref(),
                )
            })
            .await
    }
}

struct AdoptExecution<'a> {
    repo: &'a RepoId,
    capacity: ImageCapacity,
    source_checkout: &'a Path,
    pre_cowshed_checkout: &'a Path,
    identity: &'a OperationIdentity,
}

struct PreparedAdopt<A> {
    stage: AdoptStage,
    attachment: A,
    /// Main's canonical image, still behind its `PendingFence` sidecar.
    image: PathBuf,
    canonical_mount: PathBuf,
    source_checkout: PathBuf,
    pre_cowshed_checkout: PathBuf,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum CloneKind {
    Create,
    Fork,
}

struct PreparedClone<A> {
    stage: WorkspaceStage,
    attachment: A,
    image: PathBuf,
}

struct PendingRestore {
    receipt: RestoreReceipt,
    fact: PendingPublicationFact,
}

enum CommittedRestore {
    Verified(RestoreReceipt),
    Pending(Box<PendingRestore>),
}

struct PreparedCheckpoint {
    stage: CheckpointStage,
    source: PathBuf,
    /// Where the checkpointed workspace is mounted, whose last writes the checkpoint must hold.
    source_mount: PathBuf,
    label: CheckpointLabel,
    revision: Revision,
    pin: Pin,
}

struct PreparedVerifyRestore<A> {
    attachment: A,
    receipt: RestoreReceipt,
}

struct PreparedReplaceRestore<A> {
    stage: WorkspaceStage,
    attachment: A,
    staged_image: PathBuf,
    canonical_image: PathBuf,
    canonical_mount: PathBuf,
    checkpoint_image: PathBuf,
    undo_image: PathBuf,
    current: LifecycleWorkspace,
    previous_incarnation: WorkspaceIncarnation,
    source_checkpoint: String,
}

enum PreparedRestore<A> {
    Verify(PreparedVerifyRestore<A>),
    Replace(Box<PreparedReplaceRestore<A>>),
}
fn workspace_lock_path(
    config: &ApfsSubstrateConfig,
    repo: &RepoId,
    workspace: &WorkspaceName,
) -> Result<PathBuf, ApfsStorageError> {
    let storage = layout(config, repo)?;
    if workspace.is_main() {
        Ok(storage.main_image()?.lock().to_owned())
    } else {
        Ok(storage.session_image(workspace)?.lock().to_owned())
    }
}

fn operation_lock_paths(
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    operation: &Operation,
) -> Result<Vec<PathBuf>, ApfsStorageError> {
    let repo = match operation {
        Operation::Adopt { repo, .. } => repo,
        _ => expected_repo(expected)?,
    };
    let mut locks = match operation {
        Operation::Adopt { .. } => vec![workspace_lock_path(config, repo, &main_name())?],
        Operation::Create {
            source,
            destination,
            ..
        }
        | Operation::Fork {
            source,
            destination,
            ..
        } => vec![
            workspace_lock_path(config, repo, source)?,
            workspace_lock_path(config, repo, destination)?,
        ],
        Operation::Checkpoint { workspace, .. }
        | Operation::Restore { workspace, .. }
        | Operation::Retire { workspace, .. } => {
            vec![workspace_lock_path(config, repo, workspace)?]
        }
    };
    locks.sort();
    locks.dedup();
    Ok(locks)
}

struct CloneExecution<'a> {
    source: &'a WorkspaceName,
    destination: &'a WorkspaceName,
    fork: bool,
    identity: &'a OperationIdentity,
}

struct RestoreExecution<'a> {
    workspace: &'a WorkspaceName,
    label: &'a CheckpointLabel,
    mode: RestoreMode,
    identity: &'a OperationIdentity,
}

fn apply_operation<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    operation: &Operation,
    _incarnations: &dyn IncarnationSource,
) -> Result<Applied, ApfsStorageError> {
    match operation {
        Operation::Adopt { .. } => Err(ApfsStorageError::InvalidPlan(
            "adopt operations require the staged controller executor",
        )),
        Operation::Create { .. } | Operation::Fork { .. } => Err(ApfsStorageError::InvalidPlan(
            "create and fork operations require the staged controller executor",
        )),
        Operation::Checkpoint { .. } => Err(ApfsStorageError::InvalidPlan(
            "checkpoint operations require the staged controller executor",
        )),
        Operation::Restore { .. } => Err(ApfsStorageError::InvalidPlan(
            "restore operations require the staged controller executor",
        )),
        Operation::Retire { workspace, .. } => apply_retire(host, config, expected, workspace),
    }
}

fn adopted_workspace(
    repo: &RepoId,
    incarnation: WorkspaceIncarnation,
    topology: Revision,
) -> Result<LifecycleWorkspace, ApfsStorageError> {
    LifecycleWorkspace::new(
        repo.clone(),
        main_name(),
        incarnation,
        Revision::new(1),
        Revision::new(topology.get() + 1),
        WorkspaceRole::Main,
    )
    .map_err(|_| ApfsStorageError::InvalidPlan("invalid adopted workspace identity"))
}

/// Main's image is created at its canonical path, behind a `PendingFence` sidecar, and attached
/// exactly once: the copy runs on a staging mount of that attachment, and publication is the
/// sidecar's activation followed by a remount of the same attachment at the checkout. Nothing
/// is renamed under an attachment, whose identity is the backing path it was opened with.
fn prepare_adopt_stage<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    execution: AdoptExecution<'_>,
    incarnations: &dyn IncarnationSource,
) -> Result<PreparedAdopt<H::Attachment>, ApfsStorageError> {
    let AdoptExecution {
        repo,
        capacity,
        source_checkout,
        pre_cowshed_checkout,
        identity: requested,
    } = execution;
    if requested.project_root != source_checkout || config.checkout_path != source_checkout {
        return Err(ApfsStorageError::InvalidPlan(
            "adopt source must equal operation project root and the configured checkout path",
        ));
    }
    if pre_cowshed_checkout.exists() {
        return Err(ApfsStorageError::InvalidPlan(
            "pre-cowshed checkout already exists",
        ));
    }
    let topology = absent_expected(expected)?;
    let image = layout(config, repo)?.main_image()?.image().to_owned();

    // An adoption a crash interrupted resumes in place: the delta copier skips every leaf the
    // last attempt finished, so the repository copy never starts over. Its sidecar names the
    // source it was copying; a different checkout root or commit is a different adoption, and
    // the unpublished image is replaced — the checkout itself was never touched.
    let mut resumed = None;
    if let Some(pending) = host.pending_adoption(config, repo)? {
        let workspace = adopted_workspace(repo, pending.incarnation.clone(), topology)?;
        let staging = staging_mount(config, &workspace)?;
        let same_source = pending.info.project_root == requested.project_root
            && pending.info.base_commit == requested.base_commit;
        let attachment = if same_source {
            timed_apfs_step("staging", "resume", || {
                host.resume_pending_adopt(&pending.image, &staging, &workspace)
            })?
        } else {
            None
        };
        match attachment {
            Some(attachment) => {
                // The recorded identity, not the retry's: the marker the last attempt may have
                // written then validates as already current instead of growing its own lineage.
                let identity = pending.identity(&requested.created_trace);
                resumed = Some((workspace, identity, attachment, staging));
            }
            None => {
                eprintln!(
                    "cowshed: replacing unpublished main image {} ({}); the checkout was never touched",
                    pending.image.display(),
                    if same_source {
                        "its payload never became a verified APFS volume"
                    } else {
                        "it was copying a different checkout or commit"
                    }
                );
                host.discard_pending(&pending.image)?;
            }
        }
    }
    let (workspace, identity, attachment, staging, resuming) = match resumed {
        Some((workspace, identity, attachment, staging)) => {
            (workspace, identity, attachment, staging, true)
        }
        None => {
            let workspace = adopted_workspace(repo, incarnations.mint()?, topology)?;
            let staging = staging_mount(config, &workspace)?;
            // The sidecar is the publication fence and is durable before the payload name
            // appears. A crash in the gap leaves a sidecar-only record that recover_pending
            // removes; once the payload exists, PendingFence keeps it out of enumeration.
            if let Err(primary) = timed_apfs_step("canonical", "metadata-pending", || {
                host.publish_metadata(
                    &image,
                    &workspace,
                    workspace.revision(),
                    MetadataPolicy::FreshPendingFence,
                    Some(requested),
                    None,
                )
            }) {
                return combine_cleanup(
                    "adopt metadata preparation",
                    primary,
                    host.reclaim_image(&image),
                );
            }
            let attachment = match host.create_attached(capacity, &image) {
                Ok(attachment) => attachment,
                // Creation releases what it attached; anything it could not is released here,
                // before the image beneath it is removed.
                Err(primary) => {
                    return combine_cleanup(
                        "adopt image creation",
                        primary,
                        host.discard_pending(&image),
                    );
                }
            };
            if let Err(primary) = host.mount(&attachment, &staging, MountAccess::ReadWrite, false) {
                return combine_cleanup(
                    "adopt staging mount",
                    primary,
                    detach_and_reclaim(host, attachment, &image, "adopt detach"),
                );
            }
            (workspace, requested.clone(), attachment, staging, false)
        }
    };
    let canonical_mount = mount_point(config, &workspace)?;
    let companion = companion_path(&image);
    let prepared = timed_apfs_step("staging", "copy", || {
        host.copy_tree(source_checkout, &staging)
    })
    .and_then(|()| {
        timed_apfs_step("staging", "creds", || {
            host.mint_workspace_credentials(&workspace, &image, &staging, &companion)
        })
    })
    .and_then(|()| {
        timed_apfs_step("staging", "marker", || {
            host.write_marker(&staging, &workspace, None, &identity)
        })
    })
    .and_then(|()| {
        timed_apfs_step("staging", "validate", || {
            host.validate_marker(&staging, &MarkerExpectation::freshly_stamped(&workspace))
        })
    });
    let adopt = PreparedAdopt {
        stage: WorkspaceStage {
            workspace,
            mount_point: staging,
            companion,
            resuming,
        },
        attachment,
        image,
        canonical_mount,
        source_checkout: source_checkout.to_owned(),
        pre_cowshed_checkout: pre_cowshed_checkout.to_owned(),
    };
    match prepared {
        Ok(()) => Ok(adopt),
        Err(primary) => combine_cleanup(
            "adopt preparation",
            primary,
            abort_prepared_adopt(host, adopt),
        ),
    }
}

fn abort_prepared_adopt<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedAdopt<H::Attachment>,
) -> Result<(), ApfsStorageError> {
    release_unpublished_adopt(
        host,
        prepared.attachment,
        &prepared.image,
        prepared.stage.resuming,
    )
}

/// Let go of an adoption that will not be published. A fresh image is reclaimed with its sidecar
/// and companion. A resumed one keeps them: it holds the copy the next attempt continues from.
fn release_unpublished_adopt<H: ApfsExecutionHost>(
    host: &H,
    attachment: H::Attachment,
    image: &Path,
    resuming: bool,
) -> Result<(), ApfsStorageError> {
    if resuming {
        host.detach(attachment, DetachIntent::Release)
    } else {
        detach_and_reclaim(host, attachment, image, "adopt detach")
    }
}

fn detach_and_reclaim<H: ApfsExecutionHost>(
    host: &H,
    attachment: H::Attachment,
    image: &Path,
    operation: &'static str,
) -> Result<(), ApfsStorageError> {
    let detached = host.detach(attachment, DetachIntent::Release);
    let reclaimed = host.reclaim_image(image);
    match detached {
        Ok(()) => reclaimed,
        Err(primary) => combine_cleanup(operation, primary, reclaimed),
    }
}

fn commit_prepared_adopt<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    prepared: PreparedAdopt<H::Attachment>,
) -> Result<Applied, ApfsStorageError> {
    if let Err(primary) = host
        .validate_staged_companion(&prepared.stage.companion)
        .and_then(|()| {
            host.validate_marker(
                &prepared.stage.mount_point,
                &MarkerExpectation::freshly_stamped(&prepared.stage.workspace),
            )
        })
    {
        return combine_cleanup(
            "adopt post-initialization validation",
            primary,
            abort_prepared_adopt(host, prepared),
        );
    }
    let PreparedAdopt {
        stage,
        attachment,
        image,
        canonical_mount,
        source_checkout,
        pre_cowshed_checkout,
    } = prepared;
    // The staging mount comes down through the kernel's unmount and the image stays attached, so
    // the checkout mount below needs no detach, second attach or second fsck.
    if let Err(primary) = timed_apfs_step("staging", "unmount", || {
        host.unmount_attachment(&attachment)
    }) {
        return combine_cleanup(
            "adopt staging unmount",
            primary,
            release_unpublished_adopt(host, attachment, &image, stage.resuming),
        );
    }
    // Publication is the sidecar's one atomic rewrite from PendingFence to Active; until it lands
    // the image is invisible to every verb and the user's tree is untouched.
    if let Err(primary) = timed_apfs_step("canonical", "activate", || {
        host.activate_pending(&image, &stage.workspace)
    }) {
        return combine_cleanup(
            "adopt activation",
            primary,
            release_unpublished_adopt(host, attachment, &image, stage.resuming),
        );
    }
    // Only now does the checkout path change hands, in one atomic swap. The mountpoint *is* the
    // checkout path and cannot exist until the swap creates it, so the swap comes first and the
    // mount follows. A failure leaves a published main that `ApfsSubstrate::finish_adoption` completes.
    host.vacate_adopted_checkout(&source_checkout, &pre_cowshed_checkout)
        .map_err(PublicationError::into_source)?;
    if let Err(primary) = host
        .mount(&attachment, &canonical_mount, MountAccess::ReadWrite, false)
        .and_then(|()| {
            timed_apfs_step("canonical", "validate", || {
                host.validate_marker(
                    &canonical_mount,
                    &MarkerExpectation::owned(config, &stage.workspace),
                )
            })
        })
    {
        return detach_after_failure(host, attachment, primary, "canonical validation");
    }
    timed_apfs_step("canonical", "retain", || {
        host.retain_mounted(&stage.workspace, attachment)
    })?;
    Ok(Applied::Lifecycle(LifecycleReceipt {
        resulting_revision: stage.workspace.revision(),
        workspace: stage.workspace,
    }))
}

/// Finish an adoption whose image is published but whose checkout swap or mount a crash or
/// failure interrupted: every step converges from what is already on disk and in the kernel.
fn finish_adoption<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
    pre_cowshed_checkout: &Path,
) -> Result<(), ApfsStorageError> {
    host.vacate_adopted_checkout(&config.checkout_path, pre_cowshed_checkout)
        .map_err(PublicationError::into_source)?;
    let checkout = mount_point(config, workspace)?;
    let derived = super::lifecycle::derive_workspaces(
        host.list(workspace.repo())?,
        host.mounts(workspace.repo())?,
        host.checkpoints(workspace.repo())?,
    )?;
    let mounted = derived.into_iter().any(|candidate| {
        candidate.workspace == *workspace
            && matches!(candidate.mount_state, MountState::Mounted { .. })
    });
    let expected = MarkerExpectation::owned(config, workspace);
    if mounted {
        return host.validate_marker(&checkout, &expected);
    }
    let image = canonical_image_path(config, workspace)?;
    let attachment = host.attach_and_mount_resumable(&image, &checkout, workspace)?;
    if let Err(primary) = host.validate_marker(&checkout, &expected) {
        return detach_after_failure(host, attachment, primary, "canonical validation");
    }
    host.retain_mounted(workspace, attachment).map(|_| ())
}

fn prepare_clone_stage<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    execution: CloneExecution<'_>,
    incarnations: &dyn IncarnationSource,
    require_resume: bool,
) -> Result<PreparedClone<H::Attachment>, ApfsStorageError> {
    let CloneExecution {
        source: source_name,
        destination: destination_name,
        fork,
        identity: requested_identity,
    } = execution;
    let source = active_expected(expected, source_name)?;
    let destination_topology = absent_expected(expected)?;
    let resumed = host.resumable_clone(
        config,
        &source,
        destination_name,
        destination_topology,
        requested_identity,
    )?;
    if require_resume && resumed.is_none() {
        return Err(ApfsStorageError::InvalidPlan(
            "pending clone retirement requires the exact unfinished image",
        ));
    }
    let (workspace, identity, canonical_image, resuming) = match resumed {
        Some(resumed) => (resumed.workspace, resumed.identity, resumed.image, true),
        None => {
            let workspace = LifecycleWorkspace::new(
                source.repo().clone(),
                destination_name.clone(),
                incarnations.mint()?,
                Revision::new(source.revision().get() + 1),
                Revision::new(destination_topology.get() + 1),
                WorkspaceRole::Workspace,
            )
            .map_err(|_| ApfsStorageError::InvalidPlan("invalid cloned workspace identity"))?;
            let canonical_image = canonical_image_path(config, &workspace)?;
            (
                workspace,
                requested_identity.clone(),
                canonical_image,
                false,
            )
        }
    };
    let source_image = canonical_image_path(config, &source)?;
    let canonical_mount = mount_point(config, &workspace)?;
    let canonical_companion = companion_path(&canonical_image);

    if !resuming {
        // The sidecar is the publication fence and is durable before the canonical payload name
        // appears. A crash in the gap leaves a sidecar-only record that recover_pending removes;
        // once the payload exists, PendingFence keeps it out of enumeration until init completes.
        timed_apfs_step("canonical", "metadata-pending", || {
            host.publish_metadata(
                &canonical_image,
                &workspace,
                workspace.revision(),
                MetadataPolicy::FreshPendingFence,
                Some(&identity),
                Some(&source_image),
            )
        })?;
        let source_mount = mount_point(config, &source)?;
        // The first write happens only on a clone nothing has attached yet: rewriting a block
        // under an attached image's driver could put back bytes it had just changed.
        let cloned = host
            .clone_image(&source_image, Some(&source_mount), &canonical_image)
            .and_then(|()| {
                timed_apfs_step("canonical", "first-write", || {
                    host.write_first(&canonical_image)
                })
            });
        if let Err(primary) = cloned {
            return combine_cleanup(
                "clone canonical payload",
                primary,
                host.reclaim_image(&canonical_image),
            );
        }
    }

    // An attach/mount refusal can be reporting a crash-left live kernel mount or ambiguous
    // inventory. Preserve the PendingFence payload for the next authoritative recovery pass;
    // reclaiming a possibly mounted backing image would trade a diagnosable retry for data loss.
    let attachment =
        host.attach_and_mount_resumable(&canonical_image, &canonical_mount, &workspace)?;
    // The clone keeps its source's volume label here. Relabelling goes through Disk Arbitration,
    // which serializes every client on the host — 10–28 s behind a busy fleet, right where a
    // workspace is being provisioned — and the label is human-facing only, so the workspace's
    // supervisor relabels it once the clone is in use (`relabel_off_the_path` in the runtime).
    let prepared = timed_apfs_step("canonical", "creds", || {
        host.mint_workspace_credentials(
            &workspace,
            &canonical_image,
            &canonical_mount,
            &canonical_companion,
        )
    })
    .and_then(|()| {
        timed_apfs_step("canonical", "marker", || {
            host.write_marker(
                &canonical_mount,
                &workspace,
                fork.then_some(source.name()),
                &identity,
            )
        })
    })
    .and_then(|()| {
        timed_apfs_step("canonical", "validate", || {
            host.validate_marker(
                &canonical_mount,
                &MarkerExpectation::freshly_stamped(&workspace),
            )
        })
    });
    if resuming {
        // A failed retry must not destroy the original pending image: initialization may
        // already have changed Git state outside the image.
        return prepared.map(|()| PreparedClone {
            stage: WorkspaceStage {
                workspace,
                mount_point: canonical_mount,
                companion: canonical_companion,
                resuming,
            },
            attachment,
            image: canonical_image,
        });
    }
    if let Err(primary) = prepared {
        return combine_cleanup(
            "clone preparation",
            primary,
            detach_and_reclaim(host, attachment, &canonical_image, "clone canonical detach"),
        );
    }
    Ok(PreparedClone {
        stage: WorkspaceStage {
            workspace,
            mount_point: canonical_mount,
            companion: canonical_companion,
            resuming,
        },
        attachment,
        image: canonical_image,
    })
}

fn preserve_prepared_clone<H: ApfsExecutionHost>(
    _host: &H,
    _prepared: PreparedClone<H::Attachment>,
) -> Result<(), ApfsStorageError> {
    // Cancellation can occur after the initializer changed Git state outside this image. Dropping
    // the userspace attachment handle leaves the kernel mount and PendingFence intact for the
    // durable lifecycle intent to resume; detaching and reclaiming here would orphan those effects.
    Ok(())
}

fn commit_prepared_clone<H: ApfsExecutionHost>(
    host: &H,
    _config: &ApfsSubstrateConfig,
    prepared: PreparedClone<H::Attachment>,
) -> Result<Applied, ApfsStorageError> {
    let PreparedClone {
        stage,
        attachment,
        image,
    } = prepared;
    // The callback has already run and may have changed external Git state. On failure, `?`
    // leaves the exact PendingFence clone mounted so recovery can rerun initialization and
    // validation.
    timed_apfs_step("canonical", "validate-companion", || {
        host.validate_staged_companion(&stage.companion)
    })
    .and_then(|()| {
        timed_apfs_step("canonical", "validate", || {
            host.validate_marker(
                &stage.mount_point,
                &MarkerExpectation::freshly_stamped(&stage.workspace),
            )
        })
    })?;
    timed_apfs_step("canonical", "activate", || {
        host.activate_pending(&image, &stage.workspace)
    })?;
    timed_apfs_step("canonical", "retain", || {
        host.retain_mounted(&stage.workspace, attachment)
    })
    .map(|_| ())?;
    Ok(Applied::Lifecycle(LifecycleReceipt {
        resulting_revision: stage.workspace.revision(),
        workspace: stage.workspace,
    }))
}

fn plan_checkpoint_stage(
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    workspace_name: &WorkspaceName,
    label: &CheckpointLabel,
    pin: Pin,
) -> Result<PreparedCheckpoint, ApfsStorageError> {
    let workspace = active_expected(expected, workspace_name)?;
    let source = canonical_image_path(config, &workspace)?;
    let source_mount = mount_point(config, &workspace)?;
    let image = checkpoint_image(config, &workspace, label)?;
    let revision = Revision::new(expected_revision(expected)? + 1);
    let checkpoint = CheckpointRef::new(workspace, label.clone(), revision, pin == Pin::Pinned);
    Ok(PreparedCheckpoint {
        stage: CheckpointStage { checkpoint, image },
        source,
        source_mount,
        label: label.clone(),
        revision,
        pin,
    })
}

fn prepare_checkpoint_stage<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedCheckpoint,
) -> Result<PreparedCheckpoint, ApfsStorageError> {
    host.clone_image(
        &prepared.source,
        Some(&prepared.source_mount),
        &prepared.stage.image,
    )?;
    if let Err(primary) = host.publish_metadata(
        &prepared.stage.image,
        prepared.stage.checkpoint.workspace(),
        prepared.revision,
        MetadataPolicy::Preserve,
        None,
        Some(&prepared.source),
    ) {
        return combine_cleanup(
            "checkpoint metadata",
            primary,
            host.reclaim_image(&prepared.stage.image),
        );
    }
    let attachment = match host.attach_verified(&prepared.stage.image) {
        Ok(attachment) => attachment,
        Err(primary) => {
            return combine_cleanup(
                "checkpoint verification",
                primary,
                host.reclaim_image(&prepared.stage.image),
            );
        }
    };
    if let Err(primary) = host.detach(attachment, DetachIntent::Release) {
        return combine_cleanup(
            "checkpoint verification detach",
            primary,
            host.reclaim_image(&prepared.stage.image),
        );
    }
    Ok(prepared)
}

fn commit_prepared_checkpoint<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedCheckpoint,
) -> Result<CheckpointRef, ApfsStorageError> {
    if let Err(primary) = host.publish_checkpoint_fact(
        &prepared.stage.image,
        &prepared.label,
        prepared.revision,
        prepared.pin,
    ) {
        return combine_cleanup(
            "checkpoint fact",
            primary,
            host.reclaim_image(&prepared.stage.image),
        );
    }
    Ok(prepared.stage.checkpoint)
}

fn prepare_restore_stage<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    execution: RestoreExecution<'_>,
    incarnations: &dyn IncarnationSource,
) -> Result<PreparedRestore<H::Attachment>, ApfsStorageError> {
    let RestoreExecution {
        workspace: workspace_name,
        label,
        mode,
        identity,
    } = execution;
    let current = active_expected(expected, workspace_name)?;
    let checkpoint_image = checkpoint_image(config, &current, label)?;
    if mode == RestoreMode::VerifyOnly {
        let mount_point = staging_mount(config, &current)?;
        let attachment = host.attach_verified(&checkpoint_image)?;
        let mounted = host
            .mount(&attachment, &mount_point, MountAccess::ReadOnly, false)
            .and_then(|()| {
                host.validate_marker(&mount_point, &MarkerExpectation::owned(config, &current))
            });
        if let Err(primary) = mounted {
            return detach_after_failure(host, attachment, primary, "restore verification mount");
        }
        let previous_incarnation = current.incarnation().clone();
        return Ok(PreparedRestore::Verify(PreparedVerifyRestore {
            attachment,
            receipt: RestoreReceipt {
                previous_incarnation,
                workspace: current,
            },
        }));
    }

    let previous_incarnation = current.incarnation().clone();
    let replacement = LifecycleWorkspace::new(
        current.repo().clone(),
        current.name().clone(),
        incarnations.mint()?,
        Revision::new(current.revision().get() + 1),
        current.topology_revision(),
        current.role(),
    )
    .map_err(|_| ApfsStorageError::InvalidPlan("invalid restore replacement identity"))?;
    let canonical_image = canonical_image_path(config, &current)?;
    let canonical_mount = mount_point(config, &replacement)?;
    let staged_image = staging_image(config, &replacement)?;
    let staging_mount = staging_mount(config, &replacement)?;
    let undo_image = undo_image(config, &current, &replacement)?;
    let staged_companion = companion_path(&staged_image);

    host.clone_image(&checkpoint_image, None, &staged_image)?;
    if let Err(primary) = host.publish_metadata(
        &staged_image,
        &replacement,
        replacement.revision(),
        MetadataPolicy::Preserve,
        Some(identity),
        Some(&checkpoint_image),
    ) {
        return combine_cleanup(
            "restore staging metadata",
            primary,
            host.reclaim_image(&staged_image),
        );
    }
    let attachment = match host.attach_verified(&staged_image) {
        Ok(attachment) => attachment,
        Err(primary) => {
            return combine_cleanup(
                "restore staging attachment",
                primary,
                host.reclaim_image(&staged_image),
            );
        }
    };
    let prepared = host
        .mount(&attachment, &staging_mount, MountAccess::ReadWrite, false)
        .and_then(|()| {
            host.rename_volume(
                &staging_mount,
                &volume_label(replacement.repo(), replacement.name()),
            )?;
            host.mint_workspace_credentials(
                &replacement,
                &staged_image,
                &staging_mount,
                &staged_companion,
            )?;
            host.write_marker(&staging_mount, &replacement, None, identity)?;
            host.validate_marker(
                &staging_mount,
                &MarkerExpectation::freshly_stamped(&replacement),
            )
        });
    if let Err(primary) = prepared {
        return combine_cleanup(
            "restore preparation",
            primary,
            detach_and_reclaim(host, attachment, &staged_image, "restore staging detach"),
        );
    }
    Ok(PreparedRestore::Replace(Box::new(PreparedReplaceRestore {
        stage: WorkspaceStage {
            workspace: replacement,
            mount_point: staging_mount,
            companion: staged_companion,
            resuming: false,
        },
        attachment,
        staged_image,
        canonical_image,
        canonical_mount,
        checkpoint_image,
        undo_image,
        current,
        previous_incarnation,
        source_checkpoint: label.to_string(),
    })))
}

fn commit_prepared_restore<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    prepared: PreparedRestore<H::Attachment>,
) -> Result<CommittedRestore, ApfsStorageError> {
    let PreparedRestore::Replace(prepared) = prepared else {
        let PreparedRestore::Verify(prepared) = prepared else {
            unreachable!()
        };
        host.detach(prepared.attachment, DetachIntent::Release)?;
        return Ok(CommittedRestore::Verified(prepared.receipt));
    };
    let PreparedReplaceRestore {
        stage,
        attachment,
        staged_image,
        canonical_image,
        canonical_mount,
        checkpoint_image,
        undo_image,
        current,
        previous_incarnation,
        source_checkpoint,
    } = *prepared;
    if let Err(primary) = host
        .validate_staged_companion(&stage.companion)
        .and_then(|()| {
            host.validate_marker(
                &stage.mount_point,
                &MarkerExpectation::freshly_stamped(&stage.workspace),
            )
        })
    {
        return combine_cleanup(
            "restore post-callback validation",
            primary,
            detach_and_reclaim(host, attachment, &staged_image, "restore staging detach"),
        );
    }
    if let Err(primary) = host.detach(attachment, DetachIntent::Release) {
        return combine_cleanup(
            "restore staging detach",
            primary,
            host.reclaim_image(&staged_image),
        );
    }
    if let Err(primary) = host.detach_mounted(&current, DetachIntent::Release) {
        return combine_cleanup(
            "restore canonical detach",
            primary,
            host.reclaim_image(&staged_image),
        );
    }
    if let Err(primary) = host.restore_swap(&staged_image, &canonical_image, &undo_image) {
        let cleanup = host.reclaim_image(&staged_image).and_then(|()| {
            mount_canonical(host, config, &canonical_image, &canonical_mount, &current)
        });
        return combine_cleanup("restore swap", primary, cleanup);
    }
    if let Err(primary) = mount_canonical(
        host,
        config,
        &canonical_image,
        &canonical_mount,
        &stage.workspace,
    ) {
        let cleanup = host
            .detach_mounted(&stage.workspace, DetachIntent::Release)
            .and_then(|()| host.rollback_restore(&canonical_image, &undo_image, &staged_image))
            .and_then(|()| {
                mount_canonical(host, config, &canonical_image, &canonical_mount, &current)
            });
        return combine_cleanup("restore rollback", primary, cleanup);
    }
    let fact = match host.publish_restored_metadata(
        &staged_image,
        &canonical_image,
        &stage.workspace,
        stage.workspace.revision(),
        &checkpoint_image,
        current.incarnation(),
    ) {
        Ok(fact) => fact,
        Err(primary) => {
            let cleanup = host
                .detach_mounted(&stage.workspace, DetachIntent::Release)
                .and_then(|()| host.rollback_restore(&canonical_image, &undo_image, &staged_image))
                .and_then(|()| {
                    mount_canonical(host, config, &canonical_image, &canonical_mount, &current)
                });
            return combine_cleanup("restore metadata publication", primary, cleanup);
        }
    };
    if fact.workspace != stage.workspace {
        return Err(ApfsStorageError::MarkerMismatch(format!(
            "restored publication workspace mismatch: expected={:?}, actual={:?}",
            stage.workspace, fact.workspace
        )));
    }
    if fact.image != canonical_image {
        return Err(ApfsStorageError::MarkerMismatch(format!(
            "restored publication image mismatch: expected={}, actual={}",
            canonical_image.display(),
            fact.image.display()
        )));
    }
    if fact.mount_point != canonical_mount {
        return Err(ApfsStorageError::MarkerMismatch(format!(
            "restored publication mount point mismatch: expected={}, actual={}",
            canonical_mount.display(),
            fact.mount_point.display()
        )));
    }
    if fact.source_checkpoint != source_checkpoint {
        return Err(ApfsStorageError::MarkerMismatch(format!(
            "restored publication source checkpoint mismatch: expected={source_checkpoint}, actual={}",
            fact.source_checkpoint
        )));
    }
    if fact.replaced_incarnation != *current.incarnation() {
        return Err(ApfsStorageError::MarkerMismatch(format!(
            "restored publication replaced incarnation mismatch: expected={}, actual={}",
            current.incarnation(),
            fact.replaced_incarnation
        )));
    }
    if fact.destination_incarnation != *stage.workspace.incarnation() {
        return Err(ApfsStorageError::MarkerMismatch(format!(
            "restored publication destination incarnation mismatch: expected={}, actual={}",
            stage.workspace.incarnation(),
            fact.destination_incarnation
        )));
    }
    if fact.source_incarnation == *stage.workspace.incarnation() {
        return Err(ApfsStorageError::MarkerMismatch(format!(
            "restored publication source incarnation equals destination: {}",
            fact.source_incarnation
        )));
    }
    Ok(CommittedRestore::Pending(Box::new(PendingRestore {
        receipt: RestoreReceipt {
            previous_incarnation,
            workspace: stage.workspace,
        },
        fact,
    })))
}

fn apply_retire<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    workspace_name: &WorkspaceName,
) -> Result<Applied, ApfsStorageError> {
    let current = active_expected(expected, workspace_name)?;
    let canonical = canonical_image_path(config, &current)?;
    let trash = retired_image_path(config, &current)?;
    host.detach_mounted(&current, DetachIntent::Release)?;
    host.retire_image(&canonical, &trash)?;
    Ok(Applied::Retired(RetiredRef::new(
        current.clone(),
        Revision::new(current.revision().get() + 1),
    )))
}

fn mount_canonical<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    image: &Path,
    mount_point: &Path,
    workspace: &LifecycleWorkspace,
) -> Result<(), ApfsStorageError> {
    let attachment = host.attach_verified(image)?;
    if let Err(primary) = host
        .mount(&attachment, mount_point, MountAccess::ReadWrite, false)
        .and_then(|()| {
            timed_apfs_step("canonical", "validate", || {
                host.validate_marker(mount_point, &MarkerExpectation::owned(config, workspace))
            })
        })
    {
        return detach_after_failure(host, attachment, primary, "canonical validation");
    }
    timed_apfs_step("canonical", "retain", || {
        host.retain_mounted(workspace, attachment)
    })
    .map(|_| ())?;
    Ok(())
}
fn detach_after_failure<H: ApfsExecutionHost, T>(
    host: &H,
    attachment: H::Attachment,
    primary: ApfsStorageError,
    operation: &'static str,
) -> Result<T, ApfsStorageError> {
    match host.detach(attachment, DetachIntent::Release) {
        Ok(()) => Err(primary),
        Err(cleanup) => Err(ApfsStorageError::Cleanup {
            operation,
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        }),
    }
}

fn combine_cleanup<T>(
    operation: &'static str,
    primary: ApfsStorageError,
    cleanup: Result<(), ApfsStorageError>,
) -> Result<T, ApfsStorageError> {
    match cleanup {
        Ok(()) => Err(primary),
        Err(cleanup) => Err(ApfsStorageError::Cleanup {
            operation,
            primary: Box::new(primary),
            cleanup: Box::new(cleanup),
        }),
    }
}

fn expected_repo(expected: &[LifecycleFact]) -> Result<&RepoId, ApfsStorageError> {
    expected
        .iter()
        .find_map(|fact| match fact {
            LifecycleFact::Exists {
                repo,
                retired: false,
                ..
            } => Some(repo),
            _ => None,
        })
        .ok_or(ApfsStorageError::InvalidPlan(
            "active workspace expectation is missing",
        ))
}

fn active_expected(
    expected: &[LifecycleFact],
    name: &WorkspaceName,
) -> Result<LifecycleWorkspace, ApfsStorageError> {
    let Some(LifecycleFact::Exists {
        repo,
        name: expected_name,
        incarnation,
        revision,
        topology_revision,
        retired: false,
    }) = expected.iter().find(
        |fact| matches!(fact, LifecycleFact::Exists { name: candidate, .. } if candidate == name),
    )
    else {
        return Err(ApfsStorageError::InvalidPlan(
            "active workspace expectation is missing",
        ));
    };
    let role = WorkspaceRole::for_name(expected_name);
    LifecycleWorkspace::new(
        repo.clone(),
        expected_name.clone(),
        incarnation.clone(),
        *revision,
        *topology_revision,
        role,
    )
    .map_err(|_| ApfsStorageError::InvalidPlan("invalid active workspace identity"))
}

/// The suffix of the CA private key that rides beside a workspace image.
const COMPANION_SUFFIX: &str = ".ca.key";

fn companion_path(image: &Path) -> PathBuf {
    crate::metadata::append_suffix(image, COMPANION_SUFFIX)
}

fn absent_expected(expected: &[LifecycleFact]) -> Result<Revision, ApfsStorageError> {
    expected
        .iter()
        .find_map(|fact| match fact {
            LifecycleFact::Absent {
                topology_revision, ..
            } => Some(*topology_revision),
            _ => None,
        })
        .ok_or(ApfsStorageError::InvalidPlan(
            "absent destination expectation is missing",
        ))
}

fn expected_revision(expected: &[LifecycleFact]) -> Result<u64, ApfsStorageError> {
    expected
        .iter()
        .find_map(|fact| match fact {
            LifecycleFact::Exists { revision, .. } => Some(revision.get()),
            _ => None,
        })
        .ok_or(ApfsStorageError::InvalidPlan(
            "workspace revision expectation is missing",
        ))
}

fn main_name() -> WorkspaceName {
    WorkspaceName::main()
}

fn layout(config: &ApfsSubstrateConfig, repo: &RepoId) -> Result<StorageLayout, ApfsStorageError> {
    StorageLayout::new(&config.store_root, repo).map_err(Into::into)
}

fn canonical_image_path(
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
) -> Result<PathBuf, ApfsStorageError> {
    let layout = layout(config, workspace.repo())?;
    Ok(layout.canonical_image(workspace.name())?.image().to_owned())
}

fn staging_stem(
    config: &ApfsSubstrateConfig,
    repo: &RepoId,
    workspace: &WorkspaceName,
    incarnation: &WorkspaceIncarnation,
) -> Result<PathBuf, ApfsStorageError> {
    let project = layout(config, repo)?.project().project_root.clone();
    Ok(project.join(STAGING_NAMESPACE).join(format!(
        "{}-{}",
        workspace.as_str(),
        incarnation.as_str()
    )))
}

fn staging_image(
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
) -> Result<PathBuf, ApfsStorageError> {
    let stem = staging_stem(
        config,
        workspace.repo(),
        workspace.name(),
        workspace.incarnation(),
    )?;
    Ok(stem.with_extension(IMAGE_EXTENSION))
}

fn staging_mount(
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
) -> Result<PathBuf, ApfsStorageError> {
    Ok(layout(config, workspace.repo())?
        .project()
        .mount_root
        .join(STAGING_NAMESPACE)
        .join(format!(
            "{}-{}",
            workspace.name().as_str(),
            workspace.incarnation().as_str()
        )))
}

/// The staging mountpoint recovery uses to inspect a detached canonical image.
///
/// Deliberately not [`staging_mount`]: recovery runs while the interrupted publication it is
/// unwinding may still hold the plain `<name>-<incarnation>` staging path, so inspecting the
/// canonical image under that same stem could collide with or be mistaken for the staged clone.
/// The `recover-` prefix keeps the mount inside [`STAGING_NAMESPACE`] — an abandoned one is still
/// reclaimed by the ordinary staging sweep — while guaranteeing it never shadows the live path.
fn recovery_staging_mount(
    layout: &StorageLayout,
    workspace: &WorkspaceName,
    incarnation: &str,
) -> PathBuf {
    layout
        .project()
        .mount_root
        .join(STAGING_NAMESPACE)
        .join(format!("recover-{}-{}", workspace.as_str(), incarnation))
}

fn checkpoint_image(
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
    label: &CheckpointLabel,
) -> Result<PathBuf, ApfsStorageError> {
    Ok(layout(config, workspace.repo())?
        .checkpoint_image(workspace.name(), label)?
        .image()
        .to_owned())
}

fn undo_image(
    config: &ApfsSubstrateConfig,
    current: &LifecycleWorkspace,
    replacement: &LifecycleWorkspace,
) -> Result<PathBuf, ApfsStorageError> {
    Ok(layout(config, current.repo())?
        .project()
        .checkpoints
        .join(current.name().as_str())
        .join(format!(
            "{PRE_RESTORE_PREFIX}{}.{IMAGE_EXTENSION}",
            replacement.incarnation().as_str(),
        )))
}

/// `<sessions>/<TRASH_NAMESPACE>/<name>-<incarnation>.asif`. `sessions` is
/// [`crate::repository::ProjectPaths::sessions`]; this helper does not name that directory.
///
/// The `-` between name and incarnation is the separator [`split_retired_stem`] reverses; the two
/// live side by side so the pair cannot drift. Incarnations are fixed-width lowercase hex, which
/// is what keeps `rsplit_once` unambiguous even though workspace names contain hyphens.
fn retired_image_below(
    sessions: &Path,
    workspace: &WorkspaceName,
    incarnation: &WorkspaceIncarnation,
) -> PathBuf {
    sessions.join(TRASH_NAMESPACE).join(format!(
        "{}-{}.{IMAGE_EXTENSION}",
        workspace.as_str(),
        incarnation.as_str(),
    ))
}

/// Splits a `<name>-<incarnation>` trash stem: the exact inverse of the stem written by
/// [`retired_image_below`], kept adjacent so neither side can change the separator alone.
fn split_retired_stem(stem: &str) -> Option<(WorkspaceName, WorkspaceIncarnation)> {
    let (name, incarnation) = stem.rsplit_once('-')?;
    Some((
        WorkspaceName::new(name).ok()?,
        WorkspaceIncarnation::new(incarnation).ok()?,
    ))
}

fn retired_image_path(
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
) -> Result<PathBuf, ApfsStorageError> {
    let sessions = layout(config, workspace.repo())?.project().sessions.clone();
    Ok(retired_image_below(
        &sessions,
        workspace.name(),
        workspace.incarnation(),
    ))
}

/// Main mounts at the user's checkout path; every other workspace under the host-configured
/// `<mount-root>/<owner>/<repo>/`.
fn mount_point(
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
) -> Result<PathBuf, ApfsStorageError> {
    main_aware_mount_point(config, workspace.repo(), workspace.name())
}

fn main_aware_mount_point(
    config: &ApfsSubstrateConfig,
    repo: &RepoId,
    workspace: &WorkspaceName,
) -> Result<PathBuf, ApfsStorageError> {
    layout(config, repo)?
        .main_aware_workspace_mount(&config.checkout_path, workspace)
        .map_err(Into::into)
}

/// Internal join key for a workspace's volume, derived from metadata and never read back off a
/// volume. Enumeration is keyed by image location and mount identity by the in-image marker, so
/// this key exists only to pair a `StorageFact` with a `KernelMountFact` inside one project.
pub fn volume_key(repo: &RepoId, workspace: &WorkspaceName) -> String {
    format!(
        "cowshed.{}--{}.{}",
        repo.owner(),
        repo.repo(),
        workspace.as_str()
    )
}

/// The APFS volume label, which Finder shows for a mounted volume's directory in place of the
/// directory's own name. It is purely human-facing: it carries the full identity so volumes from
/// different repositories and workspaces stay distinguishable, but nothing parses it and nothing
/// classifies a volume by it — renaming a volume by hand changes nothing but the label. No `/`
/// on purpose: a slash in a label renders as `:` in POSIX-path contexts.
///
/// An identity change still relabels every volume — but because the label is not an authority, a
/// relabel interrupted partway leaves nothing but a cosmetic disagreement, which is why recovery
/// does not have to redo it. Uniform across main and sessions: every volume names its workspace,
/// a fresh clone from the moment its supervisor first serves rather than from its publication —
/// relabelling is a Disk Arbitration round trip that a busy host queues for tens of seconds.
pub fn volume_label(repo: &RepoId, workspace: &WorkspaceName) -> String {
    format!(
        "[cowshed] {} · {} — {}",
        repo.owner(),
        repo.repo(),
        workspace.as_str()
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::GrantSet;

    fn identity() -> OperationIdentity {
        OperationIdentity {
            project_root: PathBuf::from("/project"),
            base_commit: "0123456789abcdef".to_owned(),
            created_at: "2026-07-13T00:00:00Z".to_owned(),
            branch: Some("main".to_owned()),
            forked_from: None,
            created_trace: "lock-table".to_owned(),
            git_worktree: false,
            grants: GrantSet::default(),
        }
    }

    #[test]
    fn every_mutating_operation_maps_to_its_exact_canonical_lock_set() {
        let config = ApfsSubstrateConfig::new(
            "/tmp/cowshed-lock-table/store",
            "/tmp/cowshed-lock-table/main",
        );
        let repo = RepoId::parse("acme/widget").expect("repo");
        let main = main_name();
        let source = WorkspaceName::session("source").expect("source");
        let destination = WorkspaceName::session("destination").expect("destination");
        let expected = vec![LifecycleFact::Exists {
            repo: repo.clone(),
            name: source.clone(),
            incarnation: WorkspaceIncarnation::new("00000000000000000000000000000001")
                .expect("incarnation"),
            revision: Revision::new(1),
            topology_revision: Revision::new(1),
            retired: false,
        }];
        let main_lock = workspace_lock_path(&config, &repo, &main).expect("main");
        assert_eq!(
            main_lock,
            Path::new("/tmp/cowshed-lock-table/store/acme/widget/main.asif.lock")
        );
        let source_lock = workspace_lock_path(&config, &repo, &source).expect("source");
        let destination_lock =
            workspace_lock_path(&config, &repo, &destination).expect("destination");
        let mut clone_locks = vec![source_lock.clone(), destination_lock.clone()];
        clone_locks.sort();
        let cases = [
            (
                Operation::Adopt {
                    repo: repo.clone(),
                    capacity: DEFAULT_IMAGE_CAPACITY,
                    source_checkout: PathBuf::from("/project"),
                    pre_cowshed_checkout: PathBuf::from("/project.pre-cowshed"),
                    identity: identity(),
                },
                vec![main_lock],
            ),
            (
                Operation::Create {
                    source: source.clone(),
                    destination: destination.clone(),
                    identity: identity(),
                },
                clone_locks.clone(),
            ),
            (
                Operation::Fork {
                    source: source.clone(),
                    destination: destination.clone(),
                    identity: identity(),
                },
                clone_locks,
            ),
            (
                Operation::Checkpoint {
                    workspace: source.clone(),
                    label: CheckpointLabel::new("automatic").expect("label"),
                    pin: Pin::Automatic,
                },
                vec![source_lock.clone()],
            ),
            (
                Operation::Restore {
                    workspace: source.clone(),
                    label: CheckpointLabel::new("automatic").expect("label"),
                    mode: RestoreMode::Replace,
                    identity: identity(),
                },
                vec![source_lock.clone()],
            ),
            (Operation::Retire { workspace: source }, vec![source_lock]),
        ];

        for (operation, mut wanted) in cases {
            wanted.sort();
            assert_eq!(
                operation_lock_paths(&config, &expected, &operation).expect("lock mapping"),
                wanted,
                "{operation:?}"
            );
        }
    }

    #[tokio::test]
    async fn image_lock_serializes_metadata_read_modify_write() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-grant-lock-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        let store = root.join("store");
        std::fs::create_dir_all(&store).expect("store");
        let counter = root.join("grant-revision");
        std::fs::write(&counter, "0").expect("initial revision");
        let lock = store.join("sessions/raven.asif.lock");
        let config = ApfsSubstrateConfig::new(&store, root.join("checkout"));
        let host =
            native::MacOsApfsExecutionHost::new(crate::apfs::SystemCommandRunner, config.clone())
                .expect("native host");
        let substrate = ApfsSubstrate::new(config, host);

        let (first_entered_tx, first_entered_rx) = std::sync::mpsc::sync_channel(1);
        let (release_first_tx, release_first_rx) = std::sync::mpsc::sync_channel(1);
        let first_substrate = substrate.clone();
        let first_lock = lock.clone();
        let first_counter = counter.clone();
        let first = tokio::spawn(async move {
            first_substrate
                .dispatch_with_image_lock(first_lock, move || {
                    let observed = std::fs::read_to_string(&first_counter)?
                        .parse::<u64>()
                        .expect("numeric revision");
                    first_entered_tx.send(()).expect("announce first lock");
                    release_first_rx.recv().expect("release first mutation");
                    std::fs::write(first_counter, (observed + 1).to_string())
                })
                .await
        });
        tokio::task::yield_now().await;
        first_entered_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("first mutation entered its lock");

        let (second_attempt_tx, second_attempt_rx) = std::sync::mpsc::sync_channel(1);
        let (second_entered_tx, second_entered_rx) = std::sync::mpsc::sync_channel(1);
        let second_substrate = substrate.clone();
        let second_counter = counter.clone();
        let second = tokio::spawn(async move {
            second_attempt_tx.send(()).expect("announce second attempt");
            second_substrate
                .dispatch_with_image_lock(lock, move || {
                    second_entered_tx.send(()).expect("announce second lock");
                    let observed = std::fs::read_to_string(&second_counter)?
                        .parse::<u64>()
                        .expect("numeric revision");
                    std::fs::write(second_counter, (observed + 1).to_string())
                })
                .await
        });
        tokio::task::yield_now().await;
        second_attempt_rx
            .recv_timeout(std::time::Duration::from_secs(1))
            .expect("second mutation attempted the lock");
        let entered_before_release =
            second_entered_rx.recv_timeout(std::time::Duration::from_millis(100));

        release_first_tx.send(()).expect("release first mutation");
        first
            .await
            .expect("first task")
            .expect("first lock")
            .expect("first mutation");
        if entered_before_release.is_err() {
            second_entered_rx
                .recv_timeout(std::time::Duration::from_secs(1))
                .expect("second mutation entered after release");
        }
        second
            .await
            .expect("second task")
            .expect("second lock")
            .expect("second mutation");

        assert!(
            matches!(
                entered_before_release,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout)
            ),
            "the second mutation entered while the first held the image lock"
        );
        assert_eq!(
            std::fs::read_to_string(&counter).expect("final revision"),
            "2",
            "both serialized read-modify-write operations must survive"
        );
        drop(substrate);
        std::fs::remove_dir_all(root).expect("fixture");
    }
}
