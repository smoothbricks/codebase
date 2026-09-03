pub mod native;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

use async_trait::async_trait;
use thiserror::Error;

use crate::apfs::{
    ApfsCaseSensitivity, ApfsError, CreateImageRequest, CreatedImage, DetachIntent,
    ImageFormatSelection, MountAccess, timed_apfs_step,
};
pub use crate::metadata::CheckoutLayout;
use crate::metadata::{
    ImageCapacity, ImageFormat, WorkspaceIncarnation, WorkspaceName, WorkspaceRole,
};
use crate::repository::{OwnedRepoIds, RepoId};

use super::lifecycle::{
    AdoptPlan, AdoptRequest, CheckpointFact, CheckpointPlan, CheckpointRef, CreatePlan,
    DerivedWorkspace, Destination, ExecuteError, ForkPlan, ImmutablePlan, KernelMountFact,
    LifecycleBackend, LifecycleFact, LifecyclePlanner, LifecycleReceipt, LifecycleWorkspace,
    MountIntent, MountState, Operation, OperationIdentity, Pin, PlanError, PurePlanner,
    ResizeOutcome, RestoreMode, RestorePlan, RestoreReceipt, RetirePlan, RetiredRef, Revision,
    StorageFact, StorageGcPlan, StorageGcReport, Substrate, SubstrateStats, execute_checked,
    revalidate,
};
use super::{CheckpointLabel, PRE_RESTORE_PREFIX, StorageLayout, StorageLayoutError};

pub const DEFAULT_IMAGE_CAPACITY: ImageCapacity = ImageCapacity::from_gibibytes(100);
use super::recovery::{STAGING_NAMESPACE, TRASH_NAMESPACE};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApfsSubstrateConfig {
    pub store_root: PathBuf,
    pub caches_root: PathBuf,
    /// The adopted checkout's original path — the place in the user's source tree that adoption
    /// took over. Under `CheckoutLayout::DirectMount` it is main's mountpoint; under
    /// `CheckoutLayout::Symlink` it is not a mountpoint at all and holds a symlink into main's
    /// mount under `<mount-root>/<owner>/<repo>/main`.
    pub checkout_path: PathBuf,
    pub checkout_layout: CheckoutLayout,
    pub case_sensitivity: ApfsCaseSensitivity,
    pub capacity: ImageCapacity,
}

impl ApfsSubstrateConfig {
    pub fn new(
        store_root: impl Into<PathBuf>,
        caches_root: impl Into<PathBuf>,
        checkout_path: impl Into<PathBuf>,
        checkout_layout: CheckoutLayout,
        case_sensitivity: ApfsCaseSensitivity,
    ) -> Self {
        Self {
            store_root: store_root.into(),
            caches_root: caches_root.into(),
            checkout_path: checkout_path.into(),
            checkout_layout,
            case_sensitivity,
            capacity: DEFAULT_IMAGE_CAPACITY,
        }
    }

    /// The same project, checked out somewhere else.
    ///
    /// The checkout path and the layout are the only two fields a live project can change — that
    /// is what `cowshed mv main` does, and what `cowshed attach` converges onto after a checkout is
    /// rearranged by hand. Everything else (store root, caches root, capacity, case sensitivity) is
    /// fixed for the project's lifetime.
    ///
    /// It is a whole-config clone rather than a mutable field because the config is shared by
    /// value: `ApfsSubstrate` holds it behind an `Arc` that every clone of the substrate shares,
    /// and `MacOsApfsExecutionHost` holds its own copy. Mutating it in place would let an
    /// outstanding clone observe a half-applied move — a mount point derived from the new checkout
    /// path against an execution host still validating against the old one. Rebinding instead
    /// builds a new config, a new host, and a new substrate, and the caller swaps all three at
    /// once, at the one point in the move transaction where nothing is mounted.
    pub fn rebind_checkout(
        &self,
        checkout_path: impl Into<PathBuf>,
        checkout_layout: CheckoutLayout,
    ) -> Self {
        Self {
            checkout_path: checkout_path.into(),
            checkout_layout,
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
    pub format: ImageFormat,
}

impl MarkerExpectation {
    /// For a marker written at some earlier time, which is every already-published image. The
    /// identity it carries may be one the project has since changed away from.
    fn owned(config: &ApfsSubstrateConfig, workspace: &LifecycleWorkspace) -> Self {
        Self {
            repos: config.owned_repo_ids(workspace.repo()),
            workspace: workspace.name().clone(),
            incarnation: workspace.incarnation().clone(),
            format: workspace.format(),
        }
    }

    /// For a marker this same operation just stamped, where the current identity is the only one
    /// the marker can possibly carry, so the expectation stays exact.
    fn freshly_stamped(workspace: &LifecycleWorkspace) -> Self {
        Self {
            repos: OwnedRepoIds::sole(workspace.repo().clone()),
            workspace: workspace.name().clone(),
            incarnation: workspace.incarnation().clone(),
            format: workspace.format(),
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
}

pub type AdoptStage = WorkspaceStage;
pub type CreateStage = WorkspaceStage;
pub type ForkStage = WorkspaceStage;
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RestoreStage {
    Verify {
        workspace: LifecycleWorkspace,
        label: CheckpointLabel,
        revision: Revision,
        image: PathBuf,
        mount_point: PathBuf,
    },
    Replace(WorkspaceStage),
}

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
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ResumableStage {
    pub image: PathBuf,
    pub format: ImageFormat,
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
pub enum RestoreExecutionError<P, F> {
    #[error("restore staging failed: {0}")]
    Storage(#[source] ApfsStorageError),
    #[error("restore prepare callback failed: {0}")]
    Prepare(P),
    #[error(
        "restore prepare callback failed and cleanup also failed: prepare={prepare}; cleanup={cleanup}"
    )]
    PrepareCleanup {
        prepare: P,
        #[source]
        cleanup: ApfsStorageError,
    },
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

impl<P, F> From<ApfsStorageError> for RestoreExecutionError<P, F> {
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
    fn resolve_format(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<ImageFormat, ApfsStorageError>;
    fn create_staged(
        &self,
        request: &CreateImageRequest,
        requested: ImageFormat,
    ) -> Result<CreatedImage, ApfsStorageError>;
    fn clone_image(
        &self,
        source: &Path,
        destination: &Path,
        format: ImageFormat,
    ) -> Result<(), ApfsStorageError>;
    fn resumable_staged_adopt(
        &self,
        config: &ApfsSubstrateConfig,
        repo: &RepoId,
        identity: &OperationIdentity,
    ) -> Result<Option<ResumableStage>, ApfsStorageError> {
        let _ = (config, repo, identity);
        Ok(None)
    }
    fn copy_tree(&self, source: &Path, destination: &Path) -> Result<(), ApfsStorageError>;
    fn attach_verified(
        &self,
        image: &Path,
        format: ImageFormat,
    ) -> Result<Self::Attachment, ApfsStorageError>;
    fn mount(
        &self,
        attachment: &Self::Attachment,
        mount_point: &Path,
        access: MountAccess,
        browse: bool,
    ) -> Result<(), ApfsStorageError>;
    fn chown_volume_root(&self, mount_point: &Path) -> Result<(), ApfsStorageError>;
    fn rename_volume(&self, mount_point: &Path, volume_name: &str) -> Result<(), ApfsStorageError>;
    fn mint_workspace_credentials(
        &self,
        workspace: &LifecycleWorkspace,
        image_path: &Path,
        mount_point: &Path,
        workspace_mount: &Path,
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
    /// Detach adopted main and atomically restore its exact retained host checkout.
    ///
    /// Implementations derive retry state solely from `source_checkout`, its exact
    /// `pre_cowshed_checkout` sibling, and the canonical mount the checkout symlink names. They
    /// must never recursively copy or merge either tree.
    fn restore_adopted_checkout(
        &self,
        workspace: &LifecycleWorkspace,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
    ) -> Result<(), ApfsStorageError>;
    fn publish_image(&self, staged: &Path, canonical: &Path) -> Result<(), PublicationError>;
    /// Hand the checkout path over to a direct mountpoint (`CheckoutLayout::DirectMount`).
    ///
    /// Builds the mountpoint directory with its self-healing stub under a staging sibling,
    /// exchanges it with the original checkout in one `renameatx_np(RENAME_SWAP)`, and renames the
    /// displaced original to `pre_cowshed_checkout`. The checkout path transitions straight from
    /// the user's directory to the mountpoint, so it is never absent. The mount is attached
    /// afterwards — it cannot be attached before, because the mountpoint does not exist until this
    /// swap creates it — and until it is, the stub inside heals on the next `cd`.
    fn vacate_adopted_checkout(
        &self,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
    ) -> Result<(), PublicationError>;
    /// Prepare adoption's durable state without touching the user's tree.
    ///
    /// Creates `canonical_mount` with its self-healing stub and publishes the canonical image.
    /// The adopted checkout is untouched: it is still the user's original directory when this
    /// returns, and `link_adopted_checkout` takes it over only once the mount is live.
    fn publish_adopt(
        &self,
        canonical_mount: &Path,
        staged: &Path,
        canonical: &Path,
    ) -> Result<(), PublicationError>;
    /// Take over the adopted checkout path atomically, once `canonical_mount` is mounted.
    ///
    /// Swaps a symlink naming `canonical_mount` into the checkout path and lands the displaced
    /// original tree at `pre_cowshed_checkout`. The checkout path is at every instant either the
    /// original directory or a symlink to the live mount — never absent, never dangling.
    fn link_adopted_checkout(
        &self,
        canonical_mount: &Path,
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
    fn reclaim_image(&self, image: &Path, format: ImageFormat) -> Result<(), ApfsStorageError>;
    fn reclaim_retired(
        &self,
        config: &ApfsSubstrateConfig,
        retired: &RetiredRef,
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
    fn compact(&self, image: &Path, format: ImageFormat) -> Result<bool, ApfsStorageError>;
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
        tokio::task::spawn_blocking(job)
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
    #[error("unexpected lifecycle operation result")]
    UnexpectedResult,
    #[error("invalid APFS lifecycle plan: {0}")]
    InvalidPlan(&'static str),
    #[error("APFS host operation failed: {0}")]
    Host(String),
    #[error("garbage-collection plan is stale")]
    GcPlanStale,
    #[error("marker does not match detached APFS metadata: {0}")]
    MarkerMismatch(String),
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
            workspace.format(),
        )?];
        let workspace = workspace.clone();
        let source_checkout = self.config.checkout_path.clone();
        let pre_cowshed_checkout = pre_cowshed_checkout.to_owned();
        self.dispatch_with_locks(lock_paths, true, move |host, _| {
            host.restore_adopted_checkout(&workspace, &source_checkout, &pre_cowshed_checkout)
        })
        .await
    }

    /// Prepare an unenumerated mounted clone, let the controller initialize it, then publish it.
    ///
    /// The lifecycle lock remains owned across the callback. The canonical image and main
    /// mountpoint are not changed until `initialize` returns success.
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
                    format,
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
                        requested_format: *format,
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
        E: Send,
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
        E: Send,
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
                let (source, destination, format, identity, operation_kind) = match &operation {
                    Operation::Create {
                        source,
                        destination,
                        format,
                        identity,
                    } => (source, destination, *format, identity, CloneKind::Create),
                    Operation::Fork {
                        source,
                        destination,
                        format,
                        identity,
                    } => (source, destination, *format, identity, CloneKind::Fork),
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
                        format,
                        fork: kind == CloneKind::Fork,
                        identity,
                    },
                    incarnations.as_ref(),
                )
            })
            .await?;
        let prepared =
            StagedCallbackGuard::new(Arc::clone(&self.host), prepared, abort_prepared_clone::<H>);

        eprintln!("cowshed: apfs staging/init start");
        let init_started = Instant::now();
        let initialized = initialize(prepared.get().stage.clone()).await;
        eprintln!(
            "cowshed: apfs staging/init done elapsed={:?} status={}",
            init_started.elapsed(),
            if initialized.is_ok() { "ok" } else { "err" }
        );
        if let Err(initializer) = initialized {
            let prepared = prepared.into_prepared();
            let host = Arc::clone(&self.host);
            let cleanup = self
                .lane
                .dispatch(move || abort_prepared_clone(host.as_ref(), prepared))
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
            .dispatch(move || commit_prepared_clone(host.as_ref(), &config, prepared))
            .await?;
        match applied {
            Applied::Lifecycle(receipt) => Ok(receipt),
            _ => Err(ApfsStorageError::UnexpectedResult.into()),
        }
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
                format,
            } = &operation
            else {
                return Err(ApfsStorageError::InvalidPlan(
                    "staged checkpoint executor requires a checkpoint operation",
                )
                .into());
            };
            plan_checkpoint_stage(&config, &expected, workspace, label, *pin, *format)?
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

    pub async fn execute_restore_staged<
        Prepare,
        PrepareFut,
        PrepareError,
        Fence,
        FenceFut,
        FenceError,
    >(
        &self,
        plan: RestorePlan,
        prepare: Prepare,
        fence: Fence,
    ) -> Result<RestoreReceipt, RestoreExecutionError<PrepareError, FenceError>>
    where
        Prepare: FnOnce(RestoreStage) -> PrepareFut + Send,
        PrepareFut: Future<Output = Result<(), PrepareError>> + Send,
        PrepareError: Send,
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
        let prepared = self
            .lane
            .dispatch(move || {
                let Operation::Restore {
                    workspace,
                    label,
                    mode,
                    format,
                    identity,
                } = &operation
                else {
                    return Err(ApfsStorageError::InvalidPlan(
                        "staged restore executor requires a restore operation",
                    ));
                };
                prepare_restore_stage(
                    host.as_ref(),
                    &config,
                    &expected,
                    RestoreExecution {
                        workspace,
                        label,
                        mode: *mode,
                        format: *format,
                        identity,
                    },
                    incarnations.as_ref(),
                )
            })
            .await?;
        let prepared = StagedCallbackGuard::new(
            Arc::clone(&self.host),
            prepared,
            abort_prepared_restore::<H>,
        );

        let stage = match prepared.get() {
            PreparedRestore::Verify(prepared) => prepared.stage.clone(),
            PreparedRestore::Replace(prepared) => RestoreStage::Replace(prepared.stage.clone()),
        };
        if let Err(prepare_error) = prepare(stage).await {
            let prepared = prepared.into_prepared();
            let host = Arc::clone(&self.host);
            let cleanup = self
                .lane
                .dispatch(move || abort_prepared_restore(host.as_ref(), prepared))
                .await;
            return Err(match cleanup {
                Ok(()) => RestoreExecutionError::Prepare(prepare_error),
                Err(cleanup) => RestoreExecutionError::PrepareCleanup {
                    prepare: prepare_error,
                    cleanup,
                },
            });
        }
        let prepared = prepared.into_prepared();
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let committed = self
            .lane
            .dispatch(move || commit_prepared_restore(host.as_ref(), &config, prepared))
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
            workspace.format(),
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
            retired.workspace().format(),
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
            workspace.format(),
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
                return Ok(mount_point);
            }
            let canonical = canonical_image_path(&config, &workspace)?;
            let attachment = host.attach_verified(&canonical, workspace.format())?;
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
            Ok(mount_point)
        })
        .await
    }

    async fn unmount(&self, workspace: &LifecycleWorkspace) -> Result<(), Self::Error> {
        let lock_paths = vec![workspace_lock_path(
            &self.config,
            workspace.repo(),
            workspace.name(),
            workspace.format(),
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
            workspace.format(),
        )?];
        let workspace = workspace.clone();
        self.dispatch_with_locks(lock_paths, true, move |host, config| {
            let image = canonical_image_path(&config, &workspace)?;
            let mount_point = mount_point(&config, &workspace)?;
            host.resize(&workspace, &image, &mount_point, capacity)
        })
        .await
    }

    async fn caches_root(&self) -> Result<PathBuf, Self::Error> {
        Ok(self.config.caches_root.clone())
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
    requested_format: ImageFormat,
    capacity: ImageCapacity,
    source_checkout: &'a Path,
    pre_cowshed_checkout: &'a Path,
    identity: &'a OperationIdentity,
}

struct PreparedAdopt<A> {
    stage: AdoptStage,
    attachment: A,
    staged_image: PathBuf,
    canonical_image: PathBuf,
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
    staged_image: PathBuf,
    canonical_image: PathBuf,
    canonical_mount: PathBuf,
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
    label: CheckpointLabel,
    revision: Revision,
    pin: Pin,
    format: ImageFormat,
}

struct PreparedVerifyRestore<A> {
    stage: RestoreStage,
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
    Replace(PreparedReplaceRestore<A>),
}
fn workspace_lock_path(
    config: &ApfsSubstrateConfig,
    repo: &RepoId,
    workspace: &WorkspaceName,
    format: ImageFormat,
) -> Result<PathBuf, ApfsStorageError> {
    let storage = layout(config, repo)?;
    if workspace.is_main() {
        Ok(storage.main_image(format)?.lock().to_owned())
    } else {
        Ok(storage.session_image(workspace, format)?.lock().to_owned())
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
        Operation::Adopt { format, .. } => {
            let main = main_name();
            let mut locks = vec![workspace_lock_path(config, repo, &main, *format)?];
            if *format == ImageFormat::Asif {
                locks.push(workspace_lock_path(
                    config,
                    repo,
                    &main,
                    ImageFormat::Sparse,
                )?);
            }
            locks
        }
        Operation::Create {
            source,
            destination,
            format,
            ..
        }
        | Operation::Fork {
            source,
            destination,
            format,
            ..
        } => vec![
            workspace_lock_path(config, repo, source, *format)?,
            workspace_lock_path(config, repo, destination, *format)?,
        ],
        Operation::Checkpoint {
            workspace, format, ..
        }
        | Operation::Restore {
            workspace, format, ..
        }
        | Operation::Retire {
            workspace, format, ..
        } => vec![workspace_lock_path(config, repo, workspace, *format)?],
    };
    locks.sort();
    locks.dedup();
    Ok(locks)
}

struct CloneExecution<'a> {
    source: &'a WorkspaceName,
    destination: &'a WorkspaceName,
    format: ImageFormat,
    fork: bool,
    identity: &'a OperationIdentity,
}

struct RestoreExecution<'a> {
    workspace: &'a WorkspaceName,
    label: &'a CheckpointLabel,
    mode: RestoreMode,
    format: ImageFormat,
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

fn prepare_adopt_stage<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    execution: AdoptExecution<'_>,
    incarnations: &dyn IncarnationSource,
) -> Result<PreparedAdopt<H::Attachment>, ApfsStorageError> {
    let AdoptExecution {
        repo,
        requested_format,
        capacity,
        source_checkout,
        pre_cowshed_checkout,
        identity,
    } = execution;
    if identity.project_root != source_checkout || config.checkout_path != source_checkout {
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
    let incarnation = incarnations.mint()?;
    let staged_stem = staging_stem(config, repo, &main_name(), &incarnation)?;
    let resumable = host.resumable_staged_adopt(config, repo, identity)?;
    let created = match resumable {
        Some(resumable) => {
            let path = staged_stem.with_extension(resumable.format.extension());
            host.clone_image(&resumable.image, &path, resumable.format)?;
            CreatedImage {
                path,
                format: resumable.format,
            }
        }
        None => {
            let request = CreateImageRequest {
                staged_stem,
                capacity,
                volume_name: volume_label(repo, &main_name()),
                case_sensitivity: config.case_sensitivity,
                // SAFETY: `getuid`/`getgid` read this process's credentials;
                // they take no pointers and cannot fail.
                owner_uid: unsafe { libc::getuid() },
                // SAFETY: `getgid` reads this process's credentials; it takes no
                // pointers and cannot fail.
                owner_gid: unsafe { libc::getgid() },
                image_format: match requested_format {
                    ImageFormat::Asif => ImageFormatSelection::Auto,
                    ImageFormat::Sparse => ImageFormatSelection::Exact(ImageFormat::Sparse),
                },
            };
            host.create_staged(&request, requested_format)?
        }
    };
    let workspace = LifecycleWorkspace::new(
        repo.clone(),
        main_name(),
        incarnation,
        Revision::new(1),
        Revision::new(topology.get() + 1),
        WorkspaceRole::Main,
        created.format,
    )
    .map_err(|_| ApfsStorageError::InvalidPlan("invalid adopted workspace identity"))?;
    let canonical_image = canonical_image_path(config, &workspace)?;
    // Adoption publishes main where the project's checkout layout says it lives, and the layout
    // is the only thing that decides. Cross-check the two resolvers that reach that path — the
    // substrate's `mount_point` and the layout's own answer for the chosen shape — because a
    // disagreement would aim the handoff at a directory nothing ever mounts, and that is not
    // discoverable after the user's tree has been touched.
    let canonical_mount = mount_point(config, &workspace)?;
    let expected_mount = if config.checkout_layout.mounts_at_checkout() {
        config.checkout_path.clone()
    } else {
        layout(config, repo)?
            .workspace_mount(&main_name())
            .map_err(ApfsStorageError::from)?
    };
    if canonical_mount != expected_mount {
        return Err(ApfsStorageError::InvalidPlan(
            "main's layout mount disagrees with the substrate mount point",
        ));
    }
    let mount_point = staging_mount(config, &workspace)?;
    let staged_companion = companion_path(&created.path);

    if let Err(primary) = host.publish_metadata(
        &created.path,
        &workspace,
        workspace.revision(),
        MetadataPolicy::Fresh,
        Some(identity),
        None,
    ) {
        return combine_cleanup(
            "adopt metadata preparation",
            primary,
            host.reclaim_image(&created.path, created.format),
        );
    }
    let attachment = match host.attach_verified(&created.path, workspace.format()) {
        Ok(attachment) => attachment,
        Err(primary) => {
            return combine_cleanup(
                "adopt attachment preparation",
                primary,
                host.reclaim_image(&created.path, created.format),
            );
        }
    };
    let prepared = host
        .mount(&attachment, &mount_point, MountAccess::ReadWrite, false)
        .and_then(|()| {
            if created.format == ImageFormat::Asif {
                host.chown_volume_root(&mount_point)?;
            }
            host.copy_tree(source_checkout, &mount_point)?;
            host.mint_workspace_credentials(
                &workspace,
                &created.path,
                &mount_point,
                &canonical_mount,
                &staged_companion,
            )?;
            host.write_marker(&mount_point, &workspace, None, identity)?;
            host.validate_marker(
                &mount_point,
                &MarkerExpectation::freshly_stamped(&workspace),
            )
        });
    if let Err(primary) = prepared {
        let cleanup = detach_and_reclaim(
            host,
            attachment,
            &created.path,
            created.format,
            "adopt staging detach",
        );
        return combine_cleanup("adopt preparation", primary, cleanup);
    }

    Ok(PreparedAdopt {
        stage: WorkspaceStage {
            workspace,
            mount_point,
            companion: staged_companion,
        },
        attachment,
        staged_image: created.path,
        canonical_image,
        canonical_mount,
        source_checkout: source_checkout.to_owned(),
        pre_cowshed_checkout: pre_cowshed_checkout.to_owned(),
    })
}

fn abort_prepared_adopt<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedAdopt<H::Attachment>,
) -> Result<(), ApfsStorageError> {
    detach_and_reclaim(
        host,
        prepared.attachment,
        &prepared.staged_image,
        prepared.stage.workspace.format(),
        "adopt staging detach",
    )
}

fn detach_and_reclaim<H: ApfsExecutionHost>(
    host: &H,
    attachment: H::Attachment,
    staged_image: &Path,
    format: ImageFormat,
    operation: &'static str,
) -> Result<(), ApfsStorageError> {
    let detached = host.detach(attachment, DetachIntent::Release);
    let reclaimed = host.reclaim_image(staged_image, format);
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
    let PreparedAdopt {
        stage,
        attachment,
        staged_image,
        canonical_image,
        canonical_mount,
        source_checkout,
        pre_cowshed_checkout,
    } = prepared;
    if let Err(primary) = host
        .validate_staged_companion(&stage.companion)
        .and_then(|()| {
            host.validate_marker(
                &stage.mount_point,
                &MarkerExpectation::freshly_stamped(&stage.workspace),
            )
        })
    {
        let cleanup = detach_and_reclaim(
            host,
            attachment,
            &staged_image,
            stage.workspace.format(),
            "adopt staging detach",
        );
        return combine_cleanup("adopt post-initialization validation", primary, cleanup);
    }
    if let Err(primary) = host.detach(attachment, DetachIntent::Release) {
        return combine_cleanup(
            "adopt staging detach",
            primary,
            host.reclaim_image(&staged_image, stage.workspace.format()),
        );
    }
    // Publication order is the transaction, and both layouts obey the same rule: every durable
    // artifact is built before the user's tree is touched, and the checkout path changes hands in
    // one atomic swap. They differ only in what the swap puts there and therefore in when the
    // mount can happen. Under the symlink layout the mountpoint is cowshed's own, so the mount is
    // already live when the symlink appears. Under direct mount the mountpoint *is* the checkout
    // path and cannot exist until the swap creates it, so the swap comes first and the attach
    // follows; the self-healing stub the swap plants covers that window. A failure or crash before
    // the swap leaves the user's directory exactly as it was under either layout.
    let direct = config.checkout_layout.mounts_at_checkout();
    let published = if direct {
        host.publish_image(&staged_image, &canonical_image)
    } else {
        host.publish_adopt(&canonical_mount, &staged_image, &canonical_image)
    };
    if let Err(primary) = published {
        let cleanup = match primary.disposition() {
            PublicationDisposition::RolledBack => {
                host.reclaim_image(&staged_image, stage.workspace.format())
            }
            PublicationDisposition::ForwardOnly => Ok(()),
        };
        return combine_cleanup("adopt publication", primary.into_source(), cleanup);
    }
    if direct {
        if let Err(primary) = host.vacate_adopted_checkout(&source_checkout, &pre_cowshed_checkout)
        {
            return Err(primary.into_source());
        }
        mount_canonical(
            host,
            config,
            &canonical_image,
            &canonical_mount,
            &stage.workspace,
        )?;
    } else {
        mount_canonical(
            host,
            config,
            &canonical_image,
            &canonical_mount,
            &stage.workspace,
        )?;
        if let Err(primary) =
            host.link_adopted_checkout(&canonical_mount, &source_checkout, &pre_cowshed_checkout)
        {
            return Err(primary.into_source());
        }
    }
    Ok(Applied::Lifecycle(LifecycleReceipt {
        resulting_revision: stage.workspace.revision(),
        workspace: stage.workspace,
    }))
}

fn prepare_clone_stage<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[LifecycleFact],
    execution: CloneExecution<'_>,
    incarnations: &dyn IncarnationSource,
) -> Result<PreparedClone<H::Attachment>, ApfsStorageError> {
    let CloneExecution {
        source: source_name,
        destination: destination_name,
        format,
        fork,
        identity,
    } = execution;
    let source = active_expected(expected, source_name, format)?;
    let destination_topology = absent_expected(expected)?;
    let workspace = LifecycleWorkspace::new(
        source.repo().clone(),
        destination_name.clone(),
        incarnations.mint()?,
        Revision::new(source.revision().get() + 1),
        Revision::new(destination_topology.get() + 1),
        WorkspaceRole::Workspace,
        format,
    )
    .map_err(|_| ApfsStorageError::InvalidPlan("invalid cloned workspace identity"))?;
    let source_image = canonical_image_path(config, &source)?;
    let canonical_image = canonical_image_path(config, &workspace)?;
    let canonical_mount = mount_point(config, &workspace)?;
    let staged_image = staging_image(config, &workspace)?;
    let staging_mount = staging_mount(config, &workspace)?;
    let staged_companion = companion_path(&staged_image);

    // No span here: the backend times sync+clonefile itself, and a second staging/clone
    // pair would read as one nested step. Same rule for attach (attach+fsck) and mount
    // below — storage spans only the logical legs the backend does not time.
    host.clone_image(&source_image, &staged_image, format)?;
    if let Err(primary) = timed_apfs_step("staging", "metadata", || {
        host.publish_metadata(
            &staged_image,
            &workspace,
            workspace.revision(),
            MetadataPolicy::Fresh,
            Some(identity),
            Some(&source_image),
        )
    }) {
        return combine_cleanup(
            "clone staging metadata",
            primary,
            host.reclaim_image(&staged_image, format),
        );
    }
    let attachment = match host.attach_verified(&staged_image, format) {
        Ok(attachment) => attachment,
        Err(primary) => {
            return combine_cleanup(
                "clone staging attachment",
                primary,
                host.reclaim_image(&staged_image, format),
            );
        }
    };
    let prepared = host
        .mount(&attachment, &staging_mount, MountAccess::ReadWrite, false)
        .and_then(|()| {
            timed_apfs_step("staging", "rename", || {
                host.rename_volume(
                    &staging_mount,
                    &volume_label(workspace.repo(), workspace.name()),
                )
            })?;
            timed_apfs_step("staging", "creds", || {
                host.mint_workspace_credentials(
                    &workspace,
                    &staged_image,
                    &staging_mount,
                    &canonical_mount,
                    &staged_companion,
                )
            })?;
            timed_apfs_step("staging", "marker", || {
                host.write_marker(
                    &staging_mount,
                    &workspace,
                    fork.then_some(source.name()),
                    identity,
                )
            })?;
            timed_apfs_step("staging", "validate", || {
                host.validate_marker(
                    &staging_mount,
                    &MarkerExpectation::freshly_stamped(&workspace),
                )
            })
        });
    if let Err(primary) = prepared {
        return combine_cleanup(
            "clone preparation",
            primary,
            detach_and_reclaim(
                host,
                attachment,
                &staged_image,
                format,
                "clone staging detach",
            ),
        );
    }
    Ok(PreparedClone {
        stage: WorkspaceStage {
            workspace,
            mount_point: staging_mount,
            companion: staged_companion,
        },
        attachment,
        staged_image,
        canonical_image,
        canonical_mount,
    })
}

fn abort_prepared_clone<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedClone<H::Attachment>,
) -> Result<(), ApfsStorageError> {
    detach_and_reclaim(
        host,
        prepared.attachment,
        &prepared.staged_image,
        prepared.stage.workspace.format(),
        "clone staging detach",
    )
}

fn commit_prepared_clone<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    prepared: PreparedClone<H::Attachment>,
) -> Result<Applied, ApfsStorageError> {
    let PreparedClone {
        stage,
        attachment,
        staged_image,
        canonical_image,
        canonical_mount,
    } = prepared;
    if let Err(primary) = timed_apfs_step("staging", "validate-companion", || {
        host.validate_staged_companion(&stage.companion)
    })
    .and_then(|()| {
        timed_apfs_step("staging", "validate", || {
            host.validate_marker(
                &stage.mount_point,
                &MarkerExpectation::freshly_stamped(&stage.workspace),
            )
        })
    }) {
        return combine_cleanup(
            "clone post-callback validation",
            primary,
            detach_and_reclaim(
                host,
                attachment,
                &staged_image,
                stage.workspace.format(),
                "clone staging detach",
            ),
        );
    }
    if let Err(primary) = timed_apfs_step("staging", "detach", || {
        host.detach(attachment, DetachIntent::Release)
    }) {
        return combine_cleanup(
            "clone staging detach",
            primary,
            host.reclaim_image(&staged_image, stage.workspace.format()),
        );
    }
    if let Err(primary) = timed_apfs_step("canonical", "publish", || {
        host.publish_image(&staged_image, &canonical_image)
    }) {
        let cleanup = match primary.disposition() {
            PublicationDisposition::RolledBack => {
                host.reclaim_image(&staged_image, stage.workspace.format())
            }
            PublicationDisposition::ForwardOnly => Ok(()),
        };
        return combine_cleanup("clone publication", primary.into_source(), cleanup);
    }
    mount_canonical(
        host,
        config,
        &canonical_image,
        &canonical_mount,
        &stage.workspace,
    )?;
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
    format: ImageFormat,
) -> Result<PreparedCheckpoint, ApfsStorageError> {
    let workspace = active_expected(expected, workspace_name, format)?;
    let source = canonical_image_path(config, &workspace)?;
    let image = checkpoint_image(config, &workspace, label)?;
    let revision = Revision::new(expected_revision(expected)? + 1);
    let checkpoint = CheckpointRef::new(workspace, label.clone(), revision, pin == Pin::Pinned);
    Ok(PreparedCheckpoint {
        stage: CheckpointStage { checkpoint, image },
        source,
        label: label.clone(),
        revision,
        pin,
        format,
    })
}

fn prepare_checkpoint_stage<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedCheckpoint,
) -> Result<PreparedCheckpoint, ApfsStorageError> {
    host.clone_image(&prepared.source, &prepared.stage.image, prepared.format)?;
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
            host.reclaim_image(&prepared.stage.image, prepared.format),
        );
    }
    let attachment = match host.attach_verified(&prepared.stage.image, prepared.format) {
        Ok(attachment) => attachment,
        Err(primary) => {
            return combine_cleanup(
                "checkpoint verification",
                primary,
                host.reclaim_image(&prepared.stage.image, prepared.format),
            );
        }
    };
    if let Err(primary) = host.detach(attachment, DetachIntent::Release) {
        return combine_cleanup(
            "checkpoint verification detach",
            primary,
            host.reclaim_image(&prepared.stage.image, prepared.format),
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
            host.reclaim_image(&prepared.stage.image, prepared.format),
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
        format,
        identity,
    } = execution;
    let current = active_expected(expected, workspace_name, format)?;
    let checkpoint_image = checkpoint_image(config, &current, label)?;
    if mode == RestoreMode::VerifyOnly {
        let mount_point = staging_mount(config, &current)?;
        let attachment = host.attach_verified(&checkpoint_image, format)?;
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
            stage: RestoreStage::Verify {
                workspace: current.clone(),
                label: label.clone(),
                revision: checkpoint_expected_revision(expected, workspace_name, label)?,
                image: checkpoint_image,
                mount_point,
            },
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
        format,
    )
    .map_err(|_| ApfsStorageError::InvalidPlan("invalid restore replacement identity"))?;
    let canonical_image = canonical_image_path(config, &current)?;
    let canonical_mount = mount_point(config, &replacement)?;
    let staged_image = staging_image(config, &replacement)?;
    let staging_mount = staging_mount(config, &replacement)?;
    let undo_image = undo_image(config, &current, &replacement)?;
    let staged_companion = companion_path(&staged_image);

    host.clone_image(&checkpoint_image, &staged_image, format)?;
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
            host.reclaim_image(&staged_image, format),
        );
    }
    let attachment = match host.attach_verified(&staged_image, format) {
        Ok(attachment) => attachment,
        Err(primary) => {
            return combine_cleanup(
                "restore staging attachment",
                primary,
                host.reclaim_image(&staged_image, format),
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
                &canonical_mount,
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
            detach_and_reclaim(
                host,
                attachment,
                &staged_image,
                format,
                "restore staging detach",
            ),
        );
    }
    Ok(PreparedRestore::Replace(PreparedReplaceRestore {
        stage: WorkspaceStage {
            workspace: replacement,
            mount_point: staging_mount,
            companion: staged_companion,
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
    }))
}

fn abort_prepared_restore<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedRestore<H::Attachment>,
) -> Result<(), ApfsStorageError> {
    match prepared {
        PreparedRestore::Verify(prepared) => {
            host.detach(prepared.attachment, DetachIntent::Release)
        }
        PreparedRestore::Replace(prepared) => detach_and_reclaim(
            host,
            prepared.attachment,
            &prepared.staged_image,
            prepared.stage.workspace.format(),
            "restore staging detach",
        ),
    }
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
    } = prepared;
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
            detach_and_reclaim(
                host,
                attachment,
                &staged_image,
                stage.workspace.format(),
                "restore staging detach",
            ),
        );
    }
    if let Err(primary) = host.detach(attachment, DetachIntent::Release) {
        return combine_cleanup(
            "restore staging detach",
            primary,
            host.reclaim_image(&staged_image, stage.workspace.format()),
        );
    }
    if let Err(primary) = host.detach_mounted(&current, DetachIntent::Release) {
        return combine_cleanup(
            "restore canonical detach",
            primary,
            host.reclaim_image(&staged_image, stage.workspace.format()),
        );
    }
    if let Err(primary) = host.restore_swap(&staged_image, &canonical_image, &undo_image) {
        let cleanup = host
            .reclaim_image(&staged_image, stage.workspace.format())
            .and_then(|()| {
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
    let format =
        expected_repo(expected).and_then(|repo| host.resolve_format(repo, workspace_name))?;
    let current = active_expected_with_format(expected, workspace_name, format)?;
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
    let attachment = host.attach_verified(image, workspace.format())?;
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
    format: ImageFormat,
) -> Result<LifecycleWorkspace, ApfsStorageError> {
    let workspace = active_expected_with_format(expected, name, format)?;
    Ok(workspace)
}

fn active_expected_with_format(
    expected: &[LifecycleFact],
    name: &WorkspaceName,
    format: ImageFormat,
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
        format,
    )
    .map_err(|_| ApfsStorageError::InvalidPlan("invalid active workspace identity"))
}

fn companion_path(image: &Path) -> PathBuf {
    crate::metadata::append_suffix(image, ".ca.key")
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

fn checkpoint_expected_revision(
    expected: &[LifecycleFact],
    workspace: &WorkspaceName,
    label: &CheckpointLabel,
) -> Result<Revision, ApfsStorageError> {
    expected
        .iter()
        .find_map(|fact| match fact {
            LifecycleFact::Checkpoint {
                workspace: expected_workspace,
                label: expected_label,
                revision,
                ..
            } if expected_workspace == workspace && expected_label == label => Some(*revision),
            _ => None,
        })
        .ok_or(ApfsStorageError::InvalidPlan(
            "checkpoint revision expectation is missing",
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
    Ok(layout
        .canonical_image(workspace.name(), workspace.format())?
        .image()
        .to_owned())
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
    Ok(stem.with_extension(workspace.format().extension()))
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
        .checkpoint_image(workspace.name(), label, workspace.format())?
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
            "{PRE_RESTORE_PREFIX}{}.{}",
            replacement.incarnation().as_str(),
            current.format().extension()
        )))
}

/// `<sessions>/<TRASH_NAMESPACE>/<name>-<incarnation>.<ext>`. `sessions` is
/// [`crate::repository::ProjectPaths::sessions`]; this helper does not name that directory.
///
/// The `-` between name and incarnation is the separator [`split_retired_stem`] reverses; the two
/// live side by side so the pair cannot drift. Incarnations are fixed-width lowercase hex, which
/// is what keeps `rsplit_once` unambiguous even though workspace names contain hyphens.
fn retired_image_below(
    sessions: &Path,
    workspace: &WorkspaceName,
    incarnation: &WorkspaceIncarnation,
    format: ImageFormat,
) -> PathBuf {
    sessions.join(TRASH_NAMESPACE).join(format!(
        "{}-{}.{}",
        workspace.as_str(),
        incarnation.as_str(),
        format.extension()
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
        workspace.format(),
    ))
}

/// Main's mountpoint is the one place the checkout layout is visible to the substrate: under
/// `DirectMount` it is the user's checkout path, under `Symlink` it is the uniform
/// `<mount-root>/<owner>/<repo>/main` and the checkout path holds a symlink to it. Every other
/// workspace mounts under the same host-configured root in both layouts.
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
        .main_aware_workspace_mount(config.checkout_layout, &config.checkout_path, workspace)
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
/// does not have to redo it. Uniform across main and sessions: every volume names its workspace.
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
            "/tmp/cowshed-lock-table/caches",
            "/tmp/cowshed-lock-table/main",
            CheckoutLayout::Symlink,
            ApfsCaseSensitivity::Sensitive,
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
        let main_asif =
            workspace_lock_path(&config, &repo, &main, ImageFormat::Asif).expect("main asif");
        let main_sparse =
            workspace_lock_path(&config, &repo, &main, ImageFormat::Sparse).expect("main sparse");
        let source_sparse =
            workspace_lock_path(&config, &repo, &source, ImageFormat::Sparse).expect("source");
        let destination_sparse =
            workspace_lock_path(&config, &repo, &destination, ImageFormat::Sparse)
                .expect("destination");
        let mut clone_locks = vec![source_sparse.clone(), destination_sparse.clone()];
        clone_locks.sort();
        let cases = [
            (
                Operation::Adopt {
                    repo: repo.clone(),
                    format: ImageFormat::Asif,
                    capacity: DEFAULT_IMAGE_CAPACITY,
                    source_checkout: PathBuf::from("/project"),
                    pre_cowshed_checkout: PathBuf::from("/project.pre-cowshed"),
                    identity: identity(),
                },
                vec![main_asif, main_sparse.clone()],
            ),
            (
                Operation::Adopt {
                    repo: repo.clone(),
                    format: ImageFormat::Sparse,
                    capacity: DEFAULT_IMAGE_CAPACITY,
                    source_checkout: PathBuf::from("/project"),
                    pre_cowshed_checkout: PathBuf::from("/project.pre-cowshed"),
                    identity: identity(),
                },
                vec![main_sparse],
            ),
            (
                Operation::Create {
                    source: source.clone(),
                    destination: destination.clone(),
                    format: ImageFormat::Sparse,
                    identity: identity(),
                },
                clone_locks.clone(),
            ),
            (
                Operation::Fork {
                    source: source.clone(),
                    destination: destination.clone(),
                    format: ImageFormat::Sparse,
                    identity: identity(),
                },
                clone_locks,
            ),
            (
                Operation::Checkpoint {
                    workspace: source.clone(),
                    label: CheckpointLabel::new("automatic").expect("label"),
                    pin: Pin::Automatic,
                    format: ImageFormat::Sparse,
                },
                vec![source_sparse.clone()],
            ),
            (
                Operation::Restore {
                    workspace: source.clone(),
                    label: CheckpointLabel::new("automatic").expect("label"),
                    mode: RestoreMode::Replace,
                    format: ImageFormat::Sparse,
                    identity: identity(),
                },
                vec![source_sparse.clone()],
            ),
            (
                Operation::Retire {
                    workspace: source,
                    format: ImageFormat::Sparse,
                },
                vec![source_sparse],
            ),
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
        let lock = store.join("sessions/raven.sparse.lock");
        let config = ApfsSubstrateConfig::new(
            &store,
            root.join("caches"),
            root.join("checkout"),
            CheckoutLayout::Symlink,
            ApfsCaseSensitivity::Sensitive,
        );
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
