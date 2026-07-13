pub mod native;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::apfs::{
    ApfsCaseSensitivity, ApfsError, CreateImageRequest, CreatedImage, ImageFormatSelection,
    MountAccess,
};
use crate::metadata::{ImageFormat, WorkspaceIncarnation, WorkspaceName, WorkspaceRole};
use crate::repository::RepoId;

use super::lifecycle::{
    AdoptPlan, AdoptRequest, CheckpointFact, CheckpointPlan, CheckpointRef, CreatePlan,
    DerivedWorkspace, Destination, ExecuteError, ExpectedState, ForkPlan, ImmutablePlan,
    KernelMountFact, LifecycleBackend, LifecyclePlanner, LifecycleReceipt, LifecycleWorkspace,
    MountIntent, MountState, ObservedState, Operation, OperationIdentity, Pin, PlanError,
    PurePlanner, RestoreMode, RestorePlan, RestoreReceipt, RetirePlan, RetiredRef, Revision,
    StorageFact, StorageGcReport, Substrate, SubstrateStats, execute_checked, revalidate,
};
use super::{CheckpointLabel, StorageLayout, StorageLayoutError};

pub const DEFAULT_IMAGE_CAPACITY: &str = "100g";
const STAGING_NAMESPACE: &str = ".staging";
const TRASH_NAMESPACE: &str = ".trash";
const PRE_RESTORE_PREFIX: &str = "pre-restore-";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApfsSubstrateConfig {
    pub store_root: PathBuf,
    pub caches_root: PathBuf,
    pub main_mount: PathBuf,
    pub case_sensitivity: ApfsCaseSensitivity,
    pub capacity: String,
}

impl ApfsSubstrateConfig {
    pub fn new(
        store_root: impl Into<PathBuf>,
        caches_root: impl Into<PathBuf>,
        main_mount: impl Into<PathBuf>,
        case_sensitivity: ApfsCaseSensitivity,
    ) -> Self {
        Self {
            store_root: store_root.into(),
            caches_root: caches_root.into(),
            main_mount: main_mount.into(),
            case_sensitivity,
            capacity: DEFAULT_IMAGE_CAPACITY.to_owned(),
        }
    }

    pub fn with_capacity(mut self, capacity: impl Into<String>) -> Result<Self, ApfsStorageError> {
        let capacity = capacity.into();
        if capacity.trim().is_empty() {
            return Err(ApfsStorageError::InvalidCapacity);
        }
        self.capacity = capacity;
        Ok(self)
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct MarkerExpectation {
    pub repo: RepoId,
    pub workspace: WorkspaceName,
    pub incarnation: WorkspaceIncarnation,
    pub format: ImageFormat,
}

impl MarkerExpectation {
    fn from_workspace(workspace: &LifecycleWorkspace) -> Self {
        Self {
            repo: workspace.repo().clone(),
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
        pending: PendingPublicationFact,
    },
    #[error("restore fence succeeded but pending publication activation failed: {source}")]
    Activation {
        #[source]
        source: ApfsStorageError,
        pending: PendingPublicationFact,
    },
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

    fn observe(&self, expected: &[ExpectedState]) -> Result<Vec<ObservedState>, ApfsStorageError>;
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
    fn detach(&self, attachment: Self::Attachment, force: bool) -> Result<(), ApfsStorageError>;
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
        force: bool,
    ) -> Result<(), ApfsStorageError>;
    fn publish_image(&self, staged: &Path, canonical: &Path) -> Result<(), PublicationError>;
    fn publish_adopt(
        &self,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
        staged: &Path,
        canonical: &Path,
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
    ) -> Result<(), ApfsStorageError>;
    fn activate_restored_metadata(&self, canonical: &Path) -> Result<(), ApfsStorageError>;
    fn rollback_restore(
        &self,
        canonical: &Path,
        undo: &Path,
        staged: &Path,
    ) -> Result<(), ApfsStorageError>;
    fn retire_image(&self, canonical: &Path, trash: &Path) -> Result<(), ApfsStorageError>;
    fn reclaim_image(&self, image: &Path, format: ImageFormat) -> Result<(), ApfsStorageError>;
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
    fn gc(&self, config: &ApfsSubstrateConfig) -> Result<StorageGcReport, ApfsStorageError>;
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
    #[error("image capacity must not be empty")]
    InvalidCapacity,
    #[error("unexpected lifecycle operation result")]
    UnexpectedResult,
    #[error("invalid APFS lifecycle plan: {0}")]
    InvalidPlan(&'static str),
    #[error("APFS host operation failed: {0}")]
    Host(String),
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
                        source_checkout,
                        pre_cowshed_checkout,
                        identity,
                    },
                    incarnations.as_ref(),
                )
            })
            .await?;

        if let Err(initializer) = initialize(prepared.stage.clone()).await {
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

        let host = Arc::clone(&self.host);
        let applied = self
            .lane
            .dispatch(move || commit_prepared_adopt(host.as_ref(), prepared))
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

        if let Err(initializer) = initialize(prepared.stage.clone()).await {
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
        let host = Arc::clone(&self.host);
        let applied = self
            .lane
            .dispatch(move || commit_prepared_clone(host.as_ref(), prepared))
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

        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let expected = plan.expected().to_vec();
        let operation = plan.operation().clone();
        let prepared = self
            .lane
            .dispatch(move || {
                let Operation::Checkpoint {
                    workspace,
                    label,
                    pin,
                    format,
                } = &operation
                else {
                    return Err(ApfsStorageError::InvalidPlan(
                        "staged checkpoint executor requires a checkpoint operation",
                    ));
                };
                prepare_checkpoint_stage(
                    host.as_ref(),
                    &config,
                    &expected,
                    workspace,
                    label,
                    *pin,
                    *format,
                )
            })
            .await?;

        if let Err(initializer) = initialize(prepared.stage.clone()).await {
            let host = Arc::clone(&self.host);
            let cleanup = self
                .lane
                .dispatch(move || abort_prepared_checkpoint(host.as_ref(), prepared))
                .await;
            return Err(match cleanup {
                Ok(()) => StagedExecutionError::Initializer(initializer),
                Err(cleanup) => StagedExecutionError::InitializerCleanup {
                    initializer,
                    cleanup,
                },
            });
        }
        let host = Arc::clone(&self.host);
        self.lane
            .dispatch(move || commit_prepared_checkpoint(host.as_ref(), prepared))
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

        let stage = match &prepared {
            PreparedRestore::Verify(prepared) => prepared.stage.clone(),
            PreparedRestore::Replace(prepared) => RestoreStage::Replace(prepared.stage.clone()),
        };
        if let Err(prepare_error) = prepare(stage).await {
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
        let host = Arc::clone(&self.host);
        let committed = self
            .lane
            .dispatch(move || commit_prepared_restore(host.as_ref(), prepared))
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
                pending: pending.fact,
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
                source,
                pending: pending.fact,
            });
        }
        Ok(pending.receipt)
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
            let image = retired_image_path(&config, retired.workspace())?;
            host.reclaim_image(&image, retired.workspace().format())
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
                host.validate_marker(&mount_point, &MarkerExpectation::from_workspace(&workspace))?;
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
                        &MarkerExpectation::from_workspace(&workspace),
                    )
                })
            {
                return detach_after_failure(host.as_ref(), attachment, primary, "ensure mounted");
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
        self.dispatch_with_locks(lock_paths, true, move |host, _| {
            host.detach_mounted(&workspace, false)
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

    async fn gc(&self) -> Result<StorageGcReport, Self::Error> {
        self.dispatch_read(move |host, config| host.gc(&config))
            .await
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
    expected: Vec<ExpectedState>,
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
        expected: &[ExpectedState],
    ) -> Result<Vec<ObservedState>, Self::Error> {
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
    source_checkout: &'a Path,
    pre_cowshed_checkout: &'a Path,
    identity: &'a OperationIdentity,
}

struct PreparedAdopt<A> {
    stage: AdoptStage,
    attachment: A,
    staged_image: PathBuf,
    canonical_image: PathBuf,
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
    Pending(PendingRestore),
}

struct PreparedCheckpoint {
    stage: CheckpointStage,
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
    expected: &[ExpectedState],
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
    expected: &[ExpectedState],
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
    expected: &[ExpectedState],
    execution: AdoptExecution<'_>,
    incarnations: &dyn IncarnationSource,
) -> Result<PreparedAdopt<H::Attachment>, ApfsStorageError> {
    let AdoptExecution {
        repo,
        requested_format,
        source_checkout,
        pre_cowshed_checkout,
        identity,
    } = execution;
    if identity.project_root != source_checkout || config.main_mount != source_checkout {
        return Err(ApfsStorageError::InvalidPlan(
            "adopt source must equal operation project root and canonical main mount",
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
    let request = CreateImageRequest {
        staged_stem,
        capacity: config.capacity.clone(),
        volume_name: volume_name(repo, &main_name()),
        case_sensitivity: config.case_sensitivity,
        owner_uid: unsafe { libc::getuid() },
        owner_gid: unsafe { libc::getgid() },
        image_format: match requested_format {
            ImageFormat::Asif => ImageFormatSelection::Auto,
            ImageFormat::Sparse => ImageFormatSelection::Exact(ImageFormat::Sparse),
        },
    };
    let created = host.create_staged(&request, requested_format)?;
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
    let mount_point = staging_mount(config, &workspace)?;

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
            host.write_marker(&mount_point, &workspace, None, identity)?;
            host.validate_marker(&mount_point, &MarkerExpectation::from_workspace(&workspace))
        });
    if let Err(primary) = prepared {
        let cleanup = detach_and_reclaim_adopt(host, attachment, &created.path, created.format);
        return combine_cleanup("adopt preparation", primary, cleanup);
    }

    Ok(PreparedAdopt {
        stage: WorkspaceStage {
            workspace,
            mount_point,
            companion: companion_path(&created.path),
        },
        attachment,
        staged_image: created.path,
        canonical_image,
        source_checkout: source_checkout.to_owned(),
        pre_cowshed_checkout: pre_cowshed_checkout.to_owned(),
    })
}

fn abort_prepared_adopt<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedAdopt<H::Attachment>,
) -> Result<(), ApfsStorageError> {
    detach_and_reclaim_adopt(
        host,
        prepared.attachment,
        &prepared.staged_image,
        prepared.stage.workspace.format(),
    )
}

fn detach_and_reclaim_adopt<H: ApfsExecutionHost>(
    host: &H,
    attachment: H::Attachment,
    staged_image: &Path,
    format: ImageFormat,
) -> Result<(), ApfsStorageError> {
    let detached = host.detach(attachment, false);
    let reclaimed = host.reclaim_image(staged_image, format);
    match detached {
        Ok(()) => reclaimed,
        Err(primary) => combine_cleanup("adopt staging detach", primary, reclaimed),
    }
}

fn commit_prepared_adopt<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedAdopt<H::Attachment>,
) -> Result<Applied, ApfsStorageError> {
    let PreparedAdopt {
        stage,
        attachment,
        staged_image,
        canonical_image,
        source_checkout,
        pre_cowshed_checkout,
    } = prepared;
    if let Err(primary) = host
        .validate_staged_companion(&stage.companion)
        .and_then(|()| {
            host.validate_marker(
                &stage.mount_point,
                &MarkerExpectation::from_workspace(&stage.workspace),
            )
        })
    {
        let cleanup =
            detach_and_reclaim_adopt(host, attachment, &staged_image, stage.workspace.format());
        return combine_cleanup("adopt post-initialization validation", primary, cleanup);
    }
    if let Err(primary) = host.detach(attachment, false) {
        return combine_cleanup(
            "adopt staging detach",
            primary,
            host.reclaim_image(&staged_image, stage.workspace.format()),
        );
    }
    if let Err(primary) = host.publish_adopt(
        &source_checkout,
        &pre_cowshed_checkout,
        &staged_image,
        &canonical_image,
    ) {
        let cleanup = match primary.disposition() {
            PublicationDisposition::RolledBack => {
                host.reclaim_image(&staged_image, stage.workspace.format())
            }
            PublicationDisposition::ForwardOnly => Ok(()),
        };
        return combine_cleanup("adopt publication", primary.into_source(), cleanup);
    }
    mount_canonical(host, &canonical_image, &source_checkout, &stage.workspace)?;
    Ok(Applied::Lifecycle(LifecycleReceipt {
        resulting_revision: stage.workspace.revision(),
        workspace: stage.workspace,
    }))
}

fn prepare_clone_stage<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[ExpectedState],
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

    host.clone_image(&source_image, &staged_image, format)?;
    if let Err(primary) = host.publish_metadata(
        &staged_image,
        &workspace,
        workspace.revision(),
        MetadataPolicy::Fresh,
        Some(identity),
        Some(&source_image),
    ) {
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
            host.rename_volume(
                &staging_mount,
                &volume_name(workspace.repo(), workspace.name()),
            )?;
            host.write_marker(
                &staging_mount,
                &workspace,
                fork.then_some(source.name()),
                identity,
            )?;
            host.validate_marker(
                &staging_mount,
                &MarkerExpectation::from_workspace(&workspace),
            )
        });
    if let Err(primary) = prepared {
        return combine_cleanup(
            "clone preparation",
            primary,
            detach_and_reclaim_clone(host, attachment, &staged_image, format),
        );
    }
    Ok(PreparedClone {
        stage: WorkspaceStage {
            workspace,
            mount_point: staging_mount,
            companion: companion_path(&staged_image),
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
    detach_and_reclaim_clone(
        host,
        prepared.attachment,
        &prepared.staged_image,
        prepared.stage.workspace.format(),
    )
}

fn detach_and_reclaim_clone<H: ApfsExecutionHost>(
    host: &H,
    attachment: H::Attachment,
    staged_image: &Path,
    format: ImageFormat,
) -> Result<(), ApfsStorageError> {
    let detached = host.detach(attachment, false);
    let reclaimed = host.reclaim_image(staged_image, format);
    match detached {
        Ok(()) => reclaimed,
        Err(primary) => combine_cleanup("clone staging detach", primary, reclaimed),
    }
}

fn commit_prepared_clone<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedClone<H::Attachment>,
) -> Result<Applied, ApfsStorageError> {
    let PreparedClone {
        stage,
        attachment,
        staged_image,
        canonical_image,
        canonical_mount,
    } = prepared;
    if let Err(primary) = host
        .validate_staged_companion(&stage.companion)
        .and_then(|()| {
            host.validate_marker(
                &stage.mount_point,
                &MarkerExpectation::from_workspace(&stage.workspace),
            )
        })
    {
        return combine_cleanup(
            "clone post-callback validation",
            primary,
            detach_and_reclaim_clone(host, attachment, &staged_image, stage.workspace.format()),
        );
    }
    if let Err(primary) = host.detach(attachment, false) {
        return combine_cleanup(
            "clone staging detach",
            primary,
            host.reclaim_image(&staged_image, stage.workspace.format()),
        );
    }
    if let Err(primary) = host.publish_image(&staged_image, &canonical_image) {
        let cleanup = match primary.disposition() {
            PublicationDisposition::RolledBack => {
                host.reclaim_image(&staged_image, stage.workspace.format())
            }
            PublicationDisposition::ForwardOnly => Ok(()),
        };
        return combine_cleanup("clone publication", primary.into_source(), cleanup);
    }
    mount_canonical(host, &canonical_image, &canonical_mount, &stage.workspace)?;
    Ok(Applied::Lifecycle(LifecycleReceipt {
        resulting_revision: stage.workspace.revision(),
        workspace: stage.workspace,
    }))
}

fn prepare_checkpoint_stage<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[ExpectedState],
    workspace_name: &WorkspaceName,
    label: &CheckpointLabel,
    pin: Pin,
    format: ImageFormat,
) -> Result<PreparedCheckpoint, ApfsStorageError> {
    let workspace = active_expected(expected, workspace_name, format)?;
    let source = canonical_image_path(config, &workspace)?;
    let image = checkpoint_image(config, &workspace, label)?;
    host.clone_image(&source, &image, format)?;
    let revision = Revision::new(expected_revision(expected)? + 1);
    if let Err(primary) = host.publish_metadata(
        &image,
        &workspace,
        revision,
        MetadataPolicy::Preserve,
        None,
        Some(&source),
    ) {
        return combine_cleanup(
            "checkpoint metadata",
            primary,
            host.reclaim_image(&image, format),
        );
    }
    let attachment = match host.attach_verified(&image, format) {
        Ok(attachment) => attachment,
        Err(primary) => {
            return combine_cleanup(
                "checkpoint verification",
                primary,
                host.reclaim_image(&image, format),
            );
        }
    };
    if let Err(primary) = host.detach(attachment, false) {
        return combine_cleanup(
            "checkpoint verification detach",
            primary,
            host.reclaim_image(&image, format),
        );
    }
    let checkpoint = CheckpointRef::new(workspace, label.clone(), revision, pin == Pin::Pinned);
    Ok(PreparedCheckpoint {
        stage: CheckpointStage { checkpoint, image },
        label: label.clone(),
        revision,
        pin,
        format,
    })
}

fn abort_prepared_checkpoint<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedCheckpoint,
) -> Result<(), ApfsStorageError> {
    host.reclaim_image(&prepared.stage.image, prepared.format)
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
    expected: &[ExpectedState],
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
                host.validate_marker(&mount_point, &MarkerExpectation::from_workspace(&current))
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
                &volume_name(replacement.repo(), replacement.name()),
            )?;
            host.write_marker(&staging_mount, &replacement, None, identity)?;
            host.validate_marker(
                &staging_mount,
                &MarkerExpectation::from_workspace(&replacement),
            )
        });
    if let Err(primary) = prepared {
        return combine_cleanup(
            "restore preparation",
            primary,
            detach_and_reclaim_restore(host, attachment, &staged_image, format),
        );
    }
    Ok(PreparedRestore::Replace(PreparedReplaceRestore {
        stage: WorkspaceStage {
            workspace: replacement,
            mount_point: staging_mount,
            companion: companion_path(&staged_image),
        },
        attachment,
        staged_image,
        canonical_image,
        canonical_mount,
        checkpoint_image,
        undo_image,
        current,
        previous_incarnation,
    }))
}

fn abort_prepared_restore<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedRestore<H::Attachment>,
) -> Result<(), ApfsStorageError> {
    match prepared {
        PreparedRestore::Verify(prepared) => host.detach(prepared.attachment, false),
        PreparedRestore::Replace(prepared) => detach_and_reclaim_restore(
            host,
            prepared.attachment,
            &prepared.staged_image,
            prepared.stage.workspace.format(),
        ),
    }
}

fn detach_and_reclaim_restore<H: ApfsExecutionHost>(
    host: &H,
    attachment: H::Attachment,
    staged_image: &Path,
    format: ImageFormat,
) -> Result<(), ApfsStorageError> {
    let detached = host.detach(attachment, false);
    let reclaimed = host.reclaim_image(staged_image, format);
    match detached {
        Ok(()) => reclaimed,
        Err(primary) => combine_cleanup("restore staging detach", primary, reclaimed),
    }
}

fn commit_prepared_restore<H: ApfsExecutionHost>(
    host: &H,
    prepared: PreparedRestore<H::Attachment>,
) -> Result<CommittedRestore, ApfsStorageError> {
    let PreparedRestore::Replace(prepared) = prepared else {
        let PreparedRestore::Verify(prepared) = prepared else {
            unreachable!()
        };
        host.detach(prepared.attachment, false)?;
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
    } = prepared;
    if let Err(primary) = host
        .validate_staged_companion(&stage.companion)
        .and_then(|()| {
            host.validate_marker(
                &stage.mount_point,
                &MarkerExpectation::from_workspace(&stage.workspace),
            )
        })
    {
        return combine_cleanup(
            "restore post-callback validation",
            primary,
            detach_and_reclaim_restore(host, attachment, &staged_image, stage.workspace.format()),
        );
    }
    if let Err(primary) = host.detach(attachment, false) {
        return combine_cleanup(
            "restore staging detach",
            primary,
            host.reclaim_image(&staged_image, stage.workspace.format()),
        );
    }
    if let Err(primary) = host.detach_mounted(&current, false) {
        return combine_cleanup(
            "restore canonical detach",
            primary,
            host.reclaim_image(&staged_image, stage.workspace.format()),
        );
    }
    if let Err(primary) = host.restore_swap(&staged_image, &canonical_image, &undo_image) {
        let cleanup = host
            .reclaim_image(&staged_image, stage.workspace.format())
            .and_then(|()| mount_canonical(host, &canonical_image, &canonical_mount, &current));
        return combine_cleanup("restore swap", primary, cleanup);
    }
    if let Err(primary) =
        mount_canonical(host, &canonical_image, &canonical_mount, &stage.workspace)
    {
        let cleanup = host
            .detach_mounted(&stage.workspace, false)
            .and_then(|()| host.rollback_restore(&canonical_image, &undo_image, &staged_image))
            .and_then(|()| mount_canonical(host, &canonical_image, &canonical_mount, &current));
        return combine_cleanup("restore rollback", primary, cleanup);
    }
    if let Err(primary) = host.publish_restored_metadata(
        &staged_image,
        &canonical_image,
        &stage.workspace,
        stage.workspace.revision(),
        &checkpoint_image,
    ) {
        let cleanup = host
            .detach_mounted(&stage.workspace, false)
            .and_then(|()| host.rollback_restore(&canonical_image, &undo_image, &staged_image))
            .and_then(|()| mount_canonical(host, &canonical_image, &canonical_mount, &current));
        return combine_cleanup("restore metadata publication", primary, cleanup);
    }
    let fact = PendingPublicationFact {
        workspace: stage.workspace.clone(),
        image: canonical_image,
        mount_point: canonical_mount,
    };
    Ok(CommittedRestore::Pending(PendingRestore {
        receipt: RestoreReceipt {
            previous_incarnation,
            workspace: stage.workspace,
        },
        fact,
    }))
}

fn apply_retire<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[ExpectedState],
    workspace_name: &WorkspaceName,
) -> Result<Applied, ApfsStorageError> {
    let format =
        expected_repo(expected).and_then(|repo| host.resolve_format(repo, workspace_name))?;
    let current = active_expected_with_format(expected, workspace_name, format)?;
    let canonical = canonical_image_path(config, &current)?;
    let trash = retired_image_path(config, &current)?;
    host.detach_mounted(&current, true)?;
    host.retire_image(&canonical, &trash)?;
    Ok(Applied::Retired(RetiredRef::new(
        current.clone(),
        Revision::new(current.revision().get() + 1),
    )))
}

fn mount_canonical<H: ApfsExecutionHost>(
    host: &H,
    image: &Path,
    mount_point: &Path,
    workspace: &LifecycleWorkspace,
) -> Result<(), ApfsStorageError> {
    let attachment = host.attach_verified(image, workspace.format())?;
    if let Err(primary) = host
        .mount(&attachment, mount_point, MountAccess::ReadWrite, false)
        .and_then(|()| {
            host.validate_marker(mount_point, &MarkerExpectation::from_workspace(workspace))
        })
    {
        return detach_after_failure(host, attachment, primary, "canonical validation");
    }
    host.retain_mounted(workspace, attachment)?;
    Ok(())
}

fn detach_after_failure<H: ApfsExecutionHost, T>(
    host: &H,
    attachment: H::Attachment,
    primary: ApfsStorageError,
    operation: &'static str,
) -> Result<T, ApfsStorageError> {
    match host.detach(attachment, false) {
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

fn expected_repo(expected: &[ExpectedState]) -> Result<&RepoId, ApfsStorageError> {
    expected
        .iter()
        .find_map(|fact| match fact {
            ExpectedState::Exists {
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
    expected: &[ExpectedState],
    name: &WorkspaceName,
    format: ImageFormat,
) -> Result<LifecycleWorkspace, ApfsStorageError> {
    let workspace = active_expected_with_format(expected, name, format)?;
    Ok(workspace)
}

fn active_expected_with_format(
    expected: &[ExpectedState],
    name: &WorkspaceName,
    format: ImageFormat,
) -> Result<LifecycleWorkspace, ApfsStorageError> {
    let Some(ExpectedState::Exists {
        repo,
        name: expected_name,
        incarnation,
        revision,
        topology_revision,
        retired: false,
    }) = expected.iter().find(
        |fact| matches!(fact, ExpectedState::Exists { name: candidate, .. } if candidate == name),
    )
    else {
        return Err(ApfsStorageError::InvalidPlan(
            "active workspace expectation is missing",
        ));
    };
    let role = if expected_name.is_main() {
        WorkspaceRole::Main
    } else {
        WorkspaceRole::Workspace
    };
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
    let mut path = image.as_os_str().to_owned();
    path.push(".ca.key");
    PathBuf::from(path)
}

fn absent_expected(expected: &[ExpectedState]) -> Result<Revision, ApfsStorageError> {
    expected
        .iter()
        .find_map(|fact| match fact {
            ExpectedState::Absent {
                topology_revision, ..
            } => Some(*topology_revision),
            _ => None,
        })
        .ok_or(ApfsStorageError::InvalidPlan(
            "absent destination expectation is missing",
        ))
}

fn expected_revision(expected: &[ExpectedState]) -> Result<u64, ApfsStorageError> {
    expected
        .iter()
        .find_map(|fact| match fact {
            ExpectedState::Exists { revision, .. } => Some(revision.get()),
            _ => None,
        })
        .ok_or(ApfsStorageError::InvalidPlan(
            "workspace revision expectation is missing",
        ))
}

fn checkpoint_expected_revision(
    expected: &[ExpectedState],
    workspace: &WorkspaceName,
    label: &CheckpointLabel,
) -> Result<Revision, ApfsStorageError> {
    expected
        .iter()
        .find_map(|fact| match fact {
            ExpectedState::Checkpoint {
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
    WorkspaceName::new("main").expect("fixed main workspace name is valid")
}

fn layout(config: &ApfsSubstrateConfig, repo: &RepoId) -> Result<StorageLayout, ApfsStorageError> {
    StorageLayout::new(&config.store_root, repo).map_err(Into::into)
}

fn canonical_image_path(
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
) -> Result<PathBuf, ApfsStorageError> {
    let layout = layout(config, workspace.repo())?;
    if workspace.name().is_main() {
        Ok(layout.main_image(workspace.format())?.image().to_owned())
    } else {
        Ok(layout
            .session_image(workspace.name(), workspace.format())?
            .image()
            .to_owned())
    }
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

fn retired_image_path(
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
) -> Result<PathBuf, ApfsStorageError> {
    let project = layout(config, workspace.repo())?
        .project()
        .project_root
        .clone();
    Ok(project.join("sessions").join(TRASH_NAMESPACE).join(format!(
        "{}-{}.{}",
        workspace.name().as_str(),
        workspace.incarnation().as_str(),
        workspace.format().extension()
    )))
}

fn mount_point(
    config: &ApfsSubstrateConfig,
    workspace: &LifecycleWorkspace,
) -> Result<PathBuf, ApfsStorageError> {
    if workspace.name().is_main() {
        Ok(config.main_mount.clone())
    } else {
        layout(config, workspace.repo())?
            .workspace_mount(workspace.name())
            .map_err(Into::into)
    }
}

pub fn volume_name(repo: &RepoId, workspace: &WorkspaceName) -> String {
    format!(
        "cowshed.{}--{}.{}",
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
            created_trace: "lock-table".to_owned(),
            grants: GrantSet::default(),
        }
    }

    #[test]
    fn every_mutating_operation_maps_to_its_exact_canonical_lock_set() {
        let config = ApfsSubstrateConfig::new(
            "/tmp/cowshed-lock-table/store",
            "/tmp/cowshed-lock-table/caches",
            "/tmp/cowshed-lock-table/main",
            ApfsCaseSensitivity::Sensitive,
        );
        let repo = RepoId::parse("acme/widget").expect("repo");
        let main = main_name();
        let source = WorkspaceName::session("source").expect("source");
        let destination = WorkspaceName::session("destination").expect("destination");
        let expected = vec![ExpectedState::Exists {
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
}
