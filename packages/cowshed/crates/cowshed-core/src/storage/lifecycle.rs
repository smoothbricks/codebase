use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use async_trait::async_trait;
use thiserror::Error;

use crate::metadata::{ImageFormat, WorkspaceIncarnation, WorkspaceName, WorkspaceRole};
use crate::repository::RepoId;

use super::CheckpointLabel;

/// A monotonic controller-visible revision.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct Revision(u64);

impl Revision {
    pub const fn new(value: u64) -> Self {
        Self(value)
    }
    pub const fn get(self) -> u64 {
        self.0
    }
}

/// Logical identity and the exact version a lifecycle plan was made against.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceRef {
    repo: RepoId,
    name: WorkspaceName,
    incarnation: WorkspaceIncarnation,
    revision: Revision,
    topology_revision: Revision,
    role: WorkspaceRole,
    format: ImageFormat,
}

impl WorkspaceRef {
    pub fn new(
        repo: RepoId,
        name: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        revision: Revision,
        topology_revision: Revision,
        role: WorkspaceRole,
        format: ImageFormat,
    ) -> Result<Self, PlanError> {
        if name.is_main() != (role == WorkspaceRole::Main) {
            return Err(PlanError::RoleNameMismatch { name, role });
        }
        Ok(Self {
            repo,
            name,
            incarnation,
            revision,
            topology_revision,
            role,
            format,
        })
    }
    pub fn repo(&self) -> &RepoId {
        &self.repo
    }
    pub fn name(&self) -> &WorkspaceName {
        &self.name
    }
    pub fn incarnation(&self) -> &WorkspaceIncarnation {
        &self.incarnation
    }
    pub const fn revision(&self) -> Revision {
        self.revision
    }
    pub const fn topology_revision(&self) -> Revision {
        self.topology_revision
    }
    pub const fn role(&self) -> WorkspaceRole {
        self.role
    }
    pub const fn format(&self) -> ImageFormat {
        self.format
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CheckpointRef {
    workspace: WorkspaceRef,
    label: CheckpointLabel,
    revision: Revision,
    pinned: bool,
}

impl CheckpointRef {
    pub fn new(
        workspace: WorkspaceRef,
        label: CheckpointLabel,
        revision: Revision,
        pinned: bool,
    ) -> Self {
        Self {
            workspace,
            label,
            revision,
            pinned,
        }
    }
    pub fn workspace(&self) -> &WorkspaceRef {
        &self.workspace
    }
    pub fn label(&self) -> &CheckpointLabel {
        &self.label
    }
    pub const fn revision(&self) -> Revision {
        self.revision
    }
    pub const fn pinned(&self) -> bool {
        self.pinned
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Pin {
    Pinned,
    Automatic,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RestoreMode {
    Replace,
    VerifyOnly,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExpectedState {
    Exists {
        repo: RepoId,
        name: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        revision: Revision,
        topology_revision: Revision,
        retired: bool,
    },
    Absent {
        repo: RepoId,
        name: WorkspaceName,
        topology_revision: Revision,
    },
    Checkpoint {
        repo: RepoId,
        workspace: WorkspaceName,
        label: CheckpointLabel,
        revision: Revision,
    },
}

impl ExpectedState {
    fn active(workspace: &WorkspaceRef) -> Self {
        Self::Exists {
            repo: workspace.repo.clone(),
            name: workspace.name.clone(),
            incarnation: workspace.incarnation.clone(),
            revision: workspace.revision,
            topology_revision: workspace.topology_revision,
            retired: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ObservedState {
    Exists {
        repo: RepoId,
        name: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        revision: Revision,
        topology_revision: Revision,
        retired: bool,
    },
    Absent {
        repo: RepoId,
        name: WorkspaceName,
        topology_revision: Revision,
    },
    Checkpoint {
        repo: RepoId,
        workspace: WorkspaceName,
        label: CheckpointLabel,
        revision: Revision,
    },
}

/// A structured stale-plan refusal. Executors return this before mutation.
#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum Conflict {
    #[error("authoritative state has {actual} facts, expected {expected}")]
    FactCount { expected: usize, actual: usize },
    #[error("authoritative fact {index} changed")]
    Stale {
        index: usize,
        expected: Box<ExpectedState>,
        actual: Box<ObservedState>,
    },
}

pub fn revalidate(expected: &[ExpectedState], actual: &[ObservedState]) -> Result<(), Conflict> {
    if expected.len() != actual.len() {
        return Err(Conflict::FactCount {
            expected: expected.len(),
            actual: actual.len(),
        });
    }
    for (index, (expected, actual)) in expected.iter().zip(actual).enumerate() {
        let matches = match (expected, actual) {
            (
                ExpectedState::Exists {
                    repo: er,
                    name: en,
                    incarnation: ei,
                    revision: ev,
                    topology_revision: et,
                    retired: ex,
                },
                ObservedState::Exists {
                    repo: ar,
                    name: an,
                    incarnation: ai,
                    revision: av,
                    topology_revision: at,
                    retired: ax,
                },
            ) => er == ar && en == an && ei == ai && ev == av && et == at && ex == ax,
            (
                ExpectedState::Absent {
                    repo: er,
                    name: en,
                    topology_revision: et,
                },
                ObservedState::Absent {
                    repo: ar,
                    name: an,
                    topology_revision: at,
                },
            ) => er == ar && en == an && et == at,
            (
                ExpectedState::Checkpoint {
                    repo: er,
                    workspace: ew,
                    label: el,
                    revision: ev,
                },
                ObservedState::Checkpoint {
                    repo: ar,
                    workspace: aw,
                    label: al,
                    revision: av,
                },
            ) => er == ar && ew == aw && el == al && ev == av,
            _ => false,
        };
        if !matches {
            return Err(Conflict::Stale {
                index,
                expected: Box::new(expected.clone()),
                actual: Box::new(actual.clone()),
            });
        }
    }
    Ok(())
}

macro_rules! plan_type {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, PartialEq)]
        pub struct $name {
            expected: Vec<ExpectedState>,
            operation: Operation,
        }
        impl $name {
            pub fn expected(&self) -> &[ExpectedState] {
                &self.expected
            }
            pub fn operation(&self) -> &Operation {
                &self.operation
            }
        }
    };
}

plan_type!(AdoptPlan);
plan_type!(CreatePlan);
plan_type!(ForkPlan);
plan_type!(CheckpointPlan);
plan_type!(RestorePlan);
plan_type!(RetirePlan);

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Operation {
    Adopt {
        repo: RepoId,
        format: ImageFormat,
    },
    Create {
        source: WorkspaceName,
        destination: WorkspaceName,
        format: ImageFormat,
    },
    Fork {
        source: WorkspaceName,
        destination: WorkspaceName,
        format: ImageFormat,
    },
    Checkpoint {
        workspace: WorkspaceName,
        label: CheckpointLabel,
        pin: Pin,
        format: ImageFormat,
    },
    Restore {
        workspace: WorkspaceName,
        label: CheckpointLabel,
        mode: RestoreMode,
        format: ImageFormat,
    },
    Retire {
        workspace: WorkspaceName,
    },
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct AdoptRequest {
    pub repo: RepoId,
    pub format: ImageFormat,
    pub topology_revision: Revision,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Destination {
    pub repo: RepoId,
    pub name: WorkspaceName,
    pub topology_revision: Revision,
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum PlanError {
    #[error("workspace {name} does not match role {role:?}")]
    RoleNameMismatch {
        name: WorkspaceName,
        role: WorkspaceRole,
    },
    #[error("main is not a valid session destination")]
    MainDestination,
    #[error("source and destination repositories differ")]
    RepositoryMismatch,
    #[error("checkpoint belongs to a different workspace version")]
    CheckpointMismatch,
    #[error("main cannot be retired or reclaimed")]
    MainIsPermanent,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct PurePlanner;

pub trait LifecyclePlanner: Send + Sync {
    fn plan_adopt(&self, request: AdoptRequest) -> Result<AdoptPlan, PlanError>;
    fn plan_create(
        &self,
        from: &WorkspaceRef,
        destination: Destination,
    ) -> Result<CreatePlan, PlanError>;
    fn plan_fork(
        &self,
        from: &WorkspaceRef,
        destination: Destination,
    ) -> Result<ForkPlan, PlanError>;
    fn plan_checkpoint(
        &self,
        workspace: &WorkspaceRef,
        label: CheckpointLabel,
        pin: Pin,
    ) -> Result<CheckpointPlan, PlanError>;
    fn plan_restore(
        &self,
        workspace: &WorkspaceRef,
        checkpoint: &CheckpointRef,
        mode: RestoreMode,
    ) -> Result<RestorePlan, PlanError>;
    fn plan_retire(&self, workspace: &WorkspaceRef) -> Result<RetirePlan, PlanError>;
}

fn destination_expected(
    source: &WorkspaceRef,
    destination: &Destination,
) -> Result<ExpectedState, PlanError> {
    if destination.name.is_main() {
        return Err(PlanError::MainDestination);
    }
    if source.repo != destination.repo {
        return Err(PlanError::RepositoryMismatch);
    }
    Ok(ExpectedState::Absent {
        repo: destination.repo.clone(),
        name: destination.name.clone(),
        topology_revision: destination.topology_revision,
    })
}

impl LifecyclePlanner for PurePlanner {
    fn plan_adopt(&self, request: AdoptRequest) -> Result<AdoptPlan, PlanError> {
        let main = WorkspaceName::new("main").expect("fixed main name is valid");
        Ok(AdoptPlan {
            expected: vec![ExpectedState::Absent {
                repo: request.repo.clone(),
                name: main,
                topology_revision: request.topology_revision,
            }],
            operation: Operation::Adopt {
                repo: request.repo,
                format: request.format,
            },
        })
    }
    fn plan_create(
        &self,
        from: &WorkspaceRef,
        destination: Destination,
    ) -> Result<CreatePlan, PlanError> {
        let absent = destination_expected(from, &destination)?;
        Ok(CreatePlan {
            expected: vec![ExpectedState::active(from), absent],
            operation: Operation::Create {
                source: from.name.clone(),
                destination: destination.name,
                format: from.format,
            },
        })
    }
    fn plan_fork(
        &self,
        from: &WorkspaceRef,
        destination: Destination,
    ) -> Result<ForkPlan, PlanError> {
        let absent = destination_expected(from, &destination)?;
        Ok(ForkPlan {
            expected: vec![ExpectedState::active(from), absent],
            operation: Operation::Fork {
                source: from.name.clone(),
                destination: destination.name,
                format: from.format,
            },
        })
    }
    fn plan_checkpoint(
        &self,
        workspace: &WorkspaceRef,
        label: CheckpointLabel,
        pin: Pin,
    ) -> Result<CheckpointPlan, PlanError> {
        Ok(CheckpointPlan {
            expected: vec![ExpectedState::active(workspace)],
            operation: Operation::Checkpoint {
                workspace: workspace.name.clone(),
                label,
                pin,
                format: workspace.format,
            },
        })
    }
    fn plan_restore(
        &self,
        workspace: &WorkspaceRef,
        checkpoint: &CheckpointRef,
        mode: RestoreMode,
    ) -> Result<RestorePlan, PlanError> {
        if checkpoint.workspace.repo != workspace.repo
            || checkpoint.workspace.name != workspace.name
            || checkpoint.workspace.incarnation != workspace.incarnation
        {
            return Err(PlanError::CheckpointMismatch);
        }
        Ok(RestorePlan {
            expected: vec![
                ExpectedState::active(workspace),
                ExpectedState::Checkpoint {
                    repo: workspace.repo.clone(),
                    workspace: workspace.name.clone(),
                    label: checkpoint.label.clone(),
                    revision: checkpoint.revision,
                },
            ],
            operation: Operation::Restore {
                workspace: workspace.name.clone(),
                label: checkpoint.label.clone(),
                mode,
                format: workspace.format,
            },
        })
    }
    fn plan_retire(&self, workspace: &WorkspaceRef) -> Result<RetirePlan, PlanError> {
        if workspace.name.is_main() {
            return Err(PlanError::MainIsPermanent);
        }
        Ok(RetirePlan {
            expected: vec![ExpectedState::active(workspace)],
            operation: Operation::Retire {
                workspace: workspace.name.clone(),
            },
        })
    }
}

pub trait ImmutablePlan: Send + Sync {
    fn expected(&self) -> &[ExpectedState];
    fn operation(&self) -> &Operation;
}
macro_rules! immutable_plan { ($($ty:ty),+ $(,)?) => {$ (impl ImmutablePlan for $ty { fn expected(&self) -> &[ExpectedState] { self.expected() } fn operation(&self) -> &Operation { self.operation() } })+}; }
immutable_plan!(
    AdoptPlan,
    CreatePlan,
    ForkPlan,
    CheckpointPlan,
    RestorePlan,
    RetirePlan,
);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LifecycleReceipt {
    pub workspace: WorkspaceRef,
    pub resulting_revision: Revision,
}

/// Stable identity returned by retirement and consumed by reclamation.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RetiredRef {
    workspace: WorkspaceRef,
    resulting_revision: Revision,
}

impl RetiredRef {
    pub fn new(workspace: WorkspaceRef, resulting_revision: Revision) -> Self {
        Self {
            workspace,
            resulting_revision,
        }
    }

    pub fn workspace(&self) -> &WorkspaceRef {
        &self.workspace
    }

    pub const fn resulting_revision(&self) -> Revision {
        self.resulting_revision
    }
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RestoreReceipt {
    pub previous_incarnation: WorkspaceIncarnation,
    pub workspace: WorkspaceRef,
}
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GcReport {
    pub examined: usize,
    pub reclaimed: usize,
    pub retained_pinned: usize,
    pub retained_recent: usize,
}
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct SubstrateStats {
    pub logical_bytes: u64,
    pub allocated_bytes: u64,
    pub checkpoint_count: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MountState {
    Detached,
    Mounted { mount_id: u64 },
}

/// Canonical persistent fact read from an image/dataset and its sidecar.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StorageFact {
    pub workspace: WorkspaceRef,
    pub volume_name: String,
}
/// A kernel mount-table fact supplied by the platform adapter.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KernelMountFact {
    pub mount_id: u64,
    pub volume_name: String,
}
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DerivedWorkspace {
    pub workspace: WorkspaceRef,
    pub mount_state: MountState,
}

/// Derive enumeration from canonical storage and kernel facts. Duplicate canonical identities are conflicts.
pub fn derive_workspaces(
    storage: impl IntoIterator<Item = StorageFact>,
    mounts: impl IntoIterator<Item = KernelMountFact>,
) -> Result<Vec<DerivedWorkspace>, DerivationError> {
    let storage: Vec<_> = storage.into_iter().collect();
    let mut seen_workspaces = BTreeSet::new();
    let mut seen_volume_names = BTreeSet::new();
    for fact in &storage {
        let workspace_key = (&fact.workspace.repo, &fact.workspace.name);
        if !seen_workspaces.insert(workspace_key) {
            return Err(DerivationError::DuplicateWorkspace(
                fact.workspace.name.clone(),
            ));
        }
        if !seen_volume_names.insert(fact.volume_name.as_str()) {
            return Err(DerivationError::DuplicateVolumeName(
                fact.volume_name.clone(),
            ));
        }
    }

    let mounts: BTreeMap<_, _> = mounts
        .into_iter()
        .map(|mount| (mount.volume_name, mount.mount_id))
        .collect();
    let mut result = Vec::with_capacity(storage.len());
    for fact in storage {
        let mount_state = mounts
            .get(&fact.volume_name)
            .copied()
            .map_or(MountState::Detached, |mount_id| MountState::Mounted {
                mount_id,
            });
        result.push(DerivedWorkspace {
            workspace: fact.workspace,
            mount_state,
        });
    }
    result.sort_by(|a, b| a.workspace.name.cmp(&b.workspace.name));
    Ok(result)
}

#[derive(Clone, Debug, Eq, Error, PartialEq)]
pub enum DerivationError {
    #[error("duplicate canonical workspace {0}")]
    DuplicateWorkspace(WorkspaceName),
    #[error("duplicate canonical volume name {0}")]
    DuplicateVolumeName(String),
}

/// Dispatch a blocking command or filesystem operation away from async runtime workers.
pub async fn dispatch_blocking<F, T>(task: F) -> Result<T, tokio::task::JoinError>
where
    F: FnOnce() -> T + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(task).await
}

/// Platform mutation boundary. `acquire` must return only after the lifecycle lock is held.
/// `read_authoritative` rereads disk/dataset and kernel facts under that lock. `apply` owns all
/// subprocess and blocking-filesystem dispatch and must use the platform blocking lane.
#[async_trait]
pub trait LifecycleBackend: Send + Sync {
    type Guard: Send;
    type Output: Send;
    type Error: Send;
    async fn acquire(&self, operation: &Operation) -> Result<Self::Guard, Self::Error>;
    async fn read_authoritative(
        &self,
        guard: &mut Self::Guard,
        expected: &[ExpectedState],
    ) -> Result<Vec<ObservedState>, Self::Error>;
    async fn apply(
        &self,
        guard: &mut Self::Guard,
        operation: &Operation,
    ) -> Result<Self::Output, Self::Error>;
}

#[derive(Debug, Error)]
pub enum ExecuteError<E> {
    #[error("lifecycle conflict: {0}")]
    Conflict(#[from] Conflict),
    #[error("substrate backend failed")]
    Backend(E),
}

/// Acquire, reread, and revalidate before the first possible mutation.
pub async fn execute_checked<B: LifecycleBackend, P: ImmutablePlan>(
    backend: &B,
    plan: &P,
) -> Result<B::Output, ExecuteError<B::Error>> {
    let mut guard = backend
        .acquire(plan.operation())
        .await
        .map_err(ExecuteError::Backend)?;
    let actual = backend
        .read_authoritative(&mut guard, plan.expected())
        .await
        .map_err(ExecuteError::Backend)?;
    revalidate(plan.expected(), &actual)?;
    backend
        .apply(&mut guard, plan.operation())
        .await
        .map_err(ExecuteError::Backend)
}

/// Public substrate boundary: synchronous pure planning and asynchronous execution.
#[async_trait]
pub trait Substrate: LifecyclePlanner {
    type Error: Send;
    async fn execute_adopt(&self, plan: AdoptPlan) -> Result<LifecycleReceipt, Self::Error>;
    async fn execute_create(&self, plan: CreatePlan) -> Result<LifecycleReceipt, Self::Error>;
    async fn execute_checkpoint(&self, plan: CheckpointPlan) -> Result<CheckpointRef, Self::Error>;
    async fn execute_restore(&self, plan: RestorePlan) -> Result<RestoreReceipt, Self::Error>;
    async fn execute_fork(&self, plan: ForkPlan) -> Result<LifecycleReceipt, Self::Error>;
    async fn execute_retire(&self, plan: RetirePlan) -> Result<RetiredRef, Self::Error>;
    async fn reclaim(&self, retired: RetiredRef) -> Result<(), Self::Error>;
    async fn list(&self, repo: &RepoId) -> Result<Vec<WorkspaceRef>, Self::Error>;
    async fn mount_state(&self, workspace: &WorkspaceRef) -> Result<MountState, Self::Error>;
    async fn ensure_mounted(&self, workspace: &WorkspaceRef) -> Result<PathBuf, Self::Error>;
    async fn unmount(&self, workspace: &WorkspaceRef) -> Result<(), Self::Error>;
    async fn caches_root(&self) -> Result<PathBuf, Self::Error>;
    async fn stats(&self, workspace: &WorkspaceRef) -> Result<SubstrateStats, Self::Error>;
    async fn gc(&self) -> Result<GcReport, Self::Error>;
}
