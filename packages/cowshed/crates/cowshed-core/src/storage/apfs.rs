pub mod native;

use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use thiserror::Error;

use crate::apfs::{
    ApfsCaseSensitivity, ApfsError, CreateImageRequest, CreatedImage, ImageFormatSelection,
};
use crate::metadata::{ImageFormat, WorkspaceIncarnation, WorkspaceName, WorkspaceRole};
use crate::repository::RepoId;

use super::lifecycle::{
    AdoptPlan, AdoptRequest, CheckpointFact, CheckpointPlan, CheckpointRef, CreatePlan,
    DerivedWorkspace, Destination, ExecuteError, ExpectedState, ForkPlan, ImmutablePlan,
    KernelMountFact, LifecycleBackend, LifecyclePlanner, LifecycleReceipt, LifecycleWorkspace,
    MountIntent, MountState, ObservedState, Operation, OperationIdentity, Pin, PlanError,
    PurePlanner, RestoreMode, RestorePlan, RestoreReceipt, RetirePlan, RetiredRef, Revision,
    StorageFact, StorageGcReport, Substrate, SubstrateStats, execute_checked,
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
    fn publish_image(&self, staged: &Path, canonical: &Path) -> Result<(), ApfsStorageError>;
    fn publish_adopt(
        &self,
        source_checkout: &Path,
        pre_cowshed_checkout: &Path,
        staged: &Path,
        canonical: &Path,
    ) -> Result<(), ApfsStorageError>;
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
    fn rollback_restore(
        &self,
        canonical: &Path,
        undo: &Path,
        staged: &Path,
    ) -> Result<(), ApfsStorageError>;
    fn retire_image(&self, canonical: &Path, trash: &Path) -> Result<(), ApfsStorageError>;
    fn reclaim_image(&self, image: &Path, format: ImageFormat) -> Result<(), ApfsStorageError>;
    fn list(&self, repo: &RepoId) -> Result<Vec<StorageFact>, ApfsStorageError>;
    fn mounts(&self, repo: &RepoId) -> Result<Vec<KernelMountFact>, ApfsStorageError>;
    fn checkpoints(&self, repo: &RepoId) -> Result<Vec<CheckpointFact>, ApfsStorageError>;
    fn recover_pending(&self, config: &ApfsSubstrateConfig) -> Result<(), ApfsStorageError>;
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
    Checkpoint(CheckpointRef),
    Restore(RestoreReceipt),
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
                    host.recover_pending(&config)?;
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

    async fn execute_adopt(&self, plan: AdoptPlan) -> Result<LifecycleReceipt, Self::Error> {
        match self.execute(&plan).await? {
            Applied::Lifecycle(receipt) => Ok(receipt),
            _ => Err(ApfsStorageError::UnexpectedResult),
        }
    }

    async fn execute_create(&self, plan: CreatePlan) -> Result<LifecycleReceipt, Self::Error> {
        match self.execute(&plan).await? {
            Applied::Lifecycle(receipt) => Ok(receipt),
            _ => Err(ApfsStorageError::UnexpectedResult),
        }
    }

    async fn execute_checkpoint(&self, plan: CheckpointPlan) -> Result<CheckpointRef, Self::Error> {
        match self.execute(&plan).await? {
            Applied::Checkpoint(checkpoint) => Ok(checkpoint),
            _ => Err(ApfsStorageError::UnexpectedResult),
        }
    }

    async fn execute_restore(&self, plan: RestorePlan) -> Result<RestoreReceipt, Self::Error> {
        match self.execute(&plan).await? {
            Applied::Restore(receipt) => Ok(receipt),
            _ => Err(ApfsStorageError::UnexpectedResult),
        }
    }

    async fn execute_fork(&self, plan: ForkPlan) -> Result<LifecycleReceipt, Self::Error> {
        match self.execute(&plan).await? {
            Applied::Lifecycle(receipt) => Ok(receipt),
            _ => Err(ApfsStorageError::UnexpectedResult),
        }
    }

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
                .mount(&attachment, &mount_point, intent.browse)
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
    type Guard = H::LockGuard;
    type Output = Applied;
    type Error = ApfsStorageError;

    async fn acquire(&self, operation: &Operation) -> Result<Self::Guard, Self::Error> {
        let lock_paths = operation_lock_paths(&self.config, &self.expected, operation)?;
        let host = Arc::clone(&self.host);
        self.lane
            .dispatch(move || {
                host.lock_images(&lock_paths, LockMode::Wait)?
                    .ok_or(ApfsStorageError::InvalidPlan(
                        "blocking image lock unexpectedly unavailable",
                    ))
            })
            .await
    }

    async fn read_authoritative(
        &self,
        _: &mut Self::Guard,
        expected: &[ExpectedState],
    ) -> Result<Vec<ObservedState>, Self::Error> {
        let host = Arc::clone(&self.host);
        let config = Arc::clone(&self.config);
        let expected = expected.to_vec();
        self.lane
            .dispatch(move || {
                host.recover_pending(&config)?;
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

struct Preparation<'a> {
    image: &'a Path,
    mount_point: &'a Path,
    workspace: &'a LifecycleWorkspace,
    forked_from: Option<&'a WorkspaceName>,
    identity: &'a OperationIdentity,
    copy_source: Option<&'a Path>,
    chown: bool,
    relabel: bool,
}

fn apply_operation<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[ExpectedState],
    operation: &Operation,
    incarnations: &dyn IncarnationSource,
) -> Result<Applied, ApfsStorageError> {
    match operation {
        Operation::Adopt {
            repo,
            format,
            source_checkout,
            pre_cowshed_checkout,
            identity,
        } => apply_adopt(
            host,
            config,
            expected,
            AdoptExecution {
                repo,
                requested_format: *format,
                source_checkout,
                pre_cowshed_checkout,
                identity,
            },
            incarnations,
        ),
        Operation::Create {
            source,
            destination,
            format,
            identity,
        } => apply_clone(
            host,
            config,
            expected,
            CloneExecution {
                source,
                destination,
                format: *format,
                fork: false,
                identity,
            },
            incarnations,
        ),
        Operation::Fork {
            source,
            destination,
            format,
            identity,
        } => apply_clone(
            host,
            config,
            expected,
            CloneExecution {
                source,
                destination,
                format: *format,
                fork: true,
                identity,
            },
            incarnations,
        ),
        Operation::Checkpoint {
            workspace,
            label,
            pin,
            format,
        } => apply_checkpoint(host, config, expected, workspace, label, *pin, *format),
        Operation::Restore {
            workspace,
            label,
            mode,
            format,
            identity,
        } => apply_restore(
            host,
            config,
            expected,
            RestoreExecution {
                workspace,
                label,
                mode: *mode,
                format: *format,
                identity,
            },
            incarnations,
        ),
        Operation::Retire { workspace, .. } => apply_retire(host, config, expected, workspace),
    }
}

fn apply_adopt<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[ExpectedState],
    execution: AdoptExecution<'_>,
    incarnations: &dyn IncarnationSource,
) -> Result<Applied, ApfsStorageError> {
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
    let canonical = canonical_image_path(config, &workspace)?;
    let staging_mount = staging_mount(config, &workspace)?;

    host.publish_metadata(
        &created.path,
        &workspace,
        workspace.revision(),
        MetadataPolicy::Fresh,
        Some(identity),
        None,
    )?;
    if let Err(primary) = prepare_image(
        host,
        Preparation {
            image: &created.path,
            mount_point: &staging_mount,
            workspace: &workspace,
            forked_from: None,
            identity,
            copy_source: Some(source_checkout),
            chown: created.format == ImageFormat::Asif,
            relabel: false,
        },
    ) {
        let cleanup = host.reclaim_image(&created.path, created.format);
        return combine_cleanup("adopt preparation", primary, cleanup);
    }
    if let Err(primary) = host.publish_adopt(
        source_checkout,
        pre_cowshed_checkout,
        &created.path,
        &canonical,
    ) {
        let cleanup = host.reclaim_image(&created.path, created.format);
        return combine_cleanup("adopt publication", primary, cleanup);
    }
    mount_canonical(host, &canonical, source_checkout, &workspace)?;
    Ok(Applied::Lifecycle(LifecycleReceipt {
        resulting_revision: workspace.revision(),
        workspace,
    }))
}

fn apply_clone<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[ExpectedState],
    execution: CloneExecution<'_>,
    incarnations: &dyn IncarnationSource,
) -> Result<Applied, ApfsStorageError> {
    let CloneExecution {
        source: source_name,
        destination: destination_name,
        format,
        fork,
        identity,
    } = execution;
    let source = active_expected(expected, source_name, format)?;
    let destination_topology = absent_expected(expected)?;
    let incarnation = incarnations.mint()?;
    let workspace = LifecycleWorkspace::new(
        source.repo().clone(),
        destination_name.clone(),
        incarnation,
        Revision::new(source.revision().get() + 1),
        Revision::new(destination_topology.get() + 1),
        WorkspaceRole::Workspace,
        format,
    )
    .map_err(|_| ApfsStorageError::InvalidPlan("invalid cloned workspace identity"))?;
    let source_image = canonical_image_path(config, &source)?;
    let destination = canonical_image_path(config, &workspace)?;
    let staged = staging_image(config, &workspace)?;
    let staging_mount = staging_mount(config, &workspace)?;

    host.clone_image(&source_image, &staged, format)?;
    let forked_from = fork.then_some(source.name());
    if let Err(primary) = host.publish_metadata(
        &staged,
        &workspace,
        workspace.revision(),
        MetadataPolicy::Fresh,
        Some(identity),
        Some(&source_image),
    ) {
        let cleanup = host.reclaim_image(&staged, format);
        return combine_cleanup("clone staging metadata", primary, cleanup);
    }
    if let Err(primary) = prepare_image(
        host,
        Preparation {
            image: &staged,
            mount_point: &staging_mount,
            workspace: &workspace,
            forked_from,
            identity,
            copy_source: None,
            chown: false,
            relabel: true,
        },
    ) {
        let cleanup = host.reclaim_image(&staged, format);
        return combine_cleanup("clone preparation", primary, cleanup);
    }
    if let Err(primary) = host.publish_image(&staged, &destination) {
        let cleanup = host.reclaim_image(&staged, format);
        return combine_cleanup("clone publication", primary, cleanup);
    }
    let mount = mount_point(config, &workspace)?;
    mount_canonical(host, &destination, &mount, &workspace)?;
    Ok(Applied::Lifecycle(LifecycleReceipt {
        resulting_revision: workspace.revision(),
        workspace,
    }))
}

fn apply_checkpoint<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[ExpectedState],
    workspace_name: &WorkspaceName,
    label: &CheckpointLabel,
    pin: Pin,
    format: ImageFormat,
) -> Result<Applied, ApfsStorageError> {
    let workspace = active_expected(expected, workspace_name, format)?;
    let source = canonical_image_path(config, &workspace)?;
    let checkpoint = checkpoint_image(config, &workspace, label)?;
    host.clone_image(&source, &checkpoint, format)?;
    let checkpoint_revision = Revision::new(expected_revision(expected)? + 1);
    if let Err(primary) = host.publish_metadata(
        &checkpoint,
        &workspace,
        checkpoint_revision,
        MetadataPolicy::Preserve,
        None,
        Some(&source),
    ) {
        let cleanup = host.reclaim_image(&checkpoint, format);
        return combine_cleanup("checkpoint metadata", primary, cleanup);
    }
    if let Err(primary) = host.publish_checkpoint_fact(&checkpoint, label, checkpoint_revision, pin)
    {
        let cleanup = host.reclaim_image(&checkpoint, format);
        return combine_cleanup("checkpoint fact", primary, cleanup);
    }
    let attachment = match host.attach_verified(&checkpoint, format) {
        Ok(attachment) => attachment,
        Err(primary) => {
            let cleanup = host.reclaim_image(&checkpoint, format);
            return combine_cleanup("checkpoint verification", primary, cleanup);
        }
    };
    host.detach(attachment, false)?;
    Ok(Applied::Checkpoint(CheckpointRef::new(
        workspace,
        label.clone(),
        checkpoint_revision,
        pin == Pin::Pinned,
    )))
}

fn apply_restore<H: ApfsExecutionHost>(
    host: &H,
    config: &ApfsSubstrateConfig,
    expected: &[ExpectedState],
    execution: RestoreExecution<'_>,
    incarnations: &dyn IncarnationSource,
) -> Result<Applied, ApfsStorageError> {
    let RestoreExecution {
        workspace: workspace_name,
        label,
        mode,
        format,
        identity,
    } = execution;
    let current = active_expected(expected, workspace_name, format)?;
    let checkpoint = checkpoint_image(config, &current, label)?;
    if mode == RestoreMode::VerifyOnly {
        let attachment = host.attach_verified(&checkpoint, format)?;
        host.detach(attachment, false)?;
        return Ok(Applied::Restore(RestoreReceipt {
            previous_incarnation: current.incarnation().clone(),
            workspace: current,
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
    let canonical = canonical_image_path(config, &current)?;
    let staged = staging_image(config, &replacement)?;
    let staging_mount = staging_mount(config, &replacement)?;
    let undo = undo_image(config, &current, &replacement)?;

    host.detach_mounted(&current, false)?;
    if let Err(primary) = host.clone_image(&checkpoint, &staged, format) {
        let cleanup = mount_canonical(host, &canonical, &mount_point(config, &current)?, &current);
        return combine_cleanup("restore clone", primary, cleanup);
    }
    if let Err(primary) = host.publish_metadata(
        &staged,
        &replacement,
        replacement.revision(),
        MetadataPolicy::Preserve,
        Some(identity),
        Some(&checkpoint),
    ) {
        let cleanup = host.reclaim_image(&staged, format).and_then(|()| {
            mount_canonical(host, &canonical, &mount_point(config, &current)?, &current)
        });
        return combine_cleanup("restore staging metadata", primary, cleanup);
    }
    if let Err(primary) = prepare_image(
        host,
        Preparation {
            image: &staged,
            mount_point: &staging_mount,
            workspace: &replacement,
            forked_from: None,
            identity,
            copy_source: None,
            chown: false,
            relabel: true,
        },
    ) {
        let cleanup = host.reclaim_image(&staged, format).and_then(|()| {
            mount_canonical(host, &canonical, &mount_point(config, &current)?, &current)
        });
        return combine_cleanup("restore preparation", primary, cleanup);
    }
    if let Err(primary) = host.restore_swap(&staged, &canonical, &undo) {
        let cleanup = host.reclaim_image(&staged, format).and_then(|()| {
            mount_canonical(host, &canonical, &mount_point(config, &current)?, &current)
        });
        return combine_cleanup("restore swap", primary, cleanup);
    }
    let canonical_mount = mount_point(config, &replacement)?;
    if let Err(primary) = mount_canonical(host, &canonical, &canonical_mount, &replacement) {
        let cleanup = host
            .detach_mounted(&replacement, false)
            .and_then(|()| host.rollback_restore(&canonical, &undo, &staged))
            .and_then(|()| mount_canonical(host, &canonical, &canonical_mount, &current));
        return combine_cleanup("restore rollback", primary, cleanup);
    }
    if let Err(primary) = host.publish_restored_metadata(
        &staged,
        &canonical,
        &replacement,
        replacement.revision(),
        &checkpoint,
    ) {
        let cleanup = host
            .detach_mounted(&replacement, false)
            .and_then(|()| host.rollback_restore(&canonical, &undo, &staged))
            .and_then(|()| mount_canonical(host, &canonical, &canonical_mount, &current));
        return combine_cleanup("restore metadata publication", primary, cleanup);
    }
    Ok(Applied::Restore(RestoreReceipt {
        previous_incarnation,
        workspace: replacement,
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

fn prepare_image<H: ApfsExecutionHost>(
    host: &H,
    preparation: Preparation<'_>,
) -> Result<(), ApfsStorageError> {
    let Preparation {
        image,
        mount_point,
        workspace,
        forked_from,
        identity,
        copy_source,
        chown,
        relabel,
    } = preparation;
    let attachment = host.attach_verified(image, workspace.format())?;
    let prepared = host.mount(&attachment, mount_point, false).and_then(|()| {
        if chown {
            host.chown_volume_root(mount_point)?;
        }
        if relabel {
            host.rename_volume(
                mount_point,
                &volume_name(workspace.repo(), workspace.name()),
            )?;
        }
        if let Some(source) = copy_source {
            host.copy_tree(source, mount_point)?;
        }
        host.write_marker(mount_point, workspace, forked_from, identity)?;
        host.validate_marker(mount_point, &MarkerExpectation::from_workspace(workspace))
    });
    match prepared {
        Ok(()) => host.detach(attachment, false),
        Err(primary) => detach_after_failure(host, attachment, primary, "staged validation"),
    }
}

fn mount_canonical<H: ApfsExecutionHost>(
    host: &H,
    image: &Path,
    mount_point: &Path,
    workspace: &LifecycleWorkspace,
) -> Result<(), ApfsStorageError> {
    let attachment = host.attach_verified(image, workspace.format())?;
    if let Err(primary) = host.mount(&attachment, mount_point, false).and_then(|()| {
        host.validate_marker(mount_point, &MarkerExpectation::from_workspace(workspace))
    }) {
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
