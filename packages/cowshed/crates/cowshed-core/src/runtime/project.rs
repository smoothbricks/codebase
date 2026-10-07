#[cfg(target_os = "macos")]
use std::fs;
use std::num::NonZeroUsize;
use std::path::{Path, PathBuf};
use std::pin::Pin;

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use url::Url;

#[cfg(target_os = "macos")]
use crate::api::dto::AbandonedWork;
#[cfg(target_os = "macos")]
use crate::api::dto::GitOid;
#[cfg(target_os = "macos")]
use crate::api::dto::LandingCommits;
#[cfg(target_os = "macos")]
use crate::api::dto::RunSandboxMode;
use crate::api::dto::{
    AdoptOptions, AttachOptions, CheckpointOptions, CheckpointQuota, CheckpointResult, CommandArg,
    CreateOptions, DoctorReport, EmptyResult, ExecRequest, GcOptions, GcReport, GrantDelta,
    GrantSet, JobId, JobInfo, LandOptions, LandReport, MirrorInfo, ProjectGrantDelta,
    ProjectGrants, PushOptions, PushReport, RebaseOptions, RebaseReport, RemoveOptions,
    RemoveProjectOptions, RemoveProjectReport, RemoveReport, RemovedWorkspace, SealedJob,
    StdinSource, WorkspaceIncarnation, WorkspaceInfo, WorkspaceState, WorkspaceTarget,
};
use crate::api::operations::{
    self, AdoptRequest, BuildVolume, ExecParams, ExecStdin, GrantRequest, JobRequest, JobStream,
    LogsChunk, LogsRequest, Operation, OperationRequest, ProjectOpenRequest, ProjectOpened,
    RepoRequest, Scope, WorkerScope, WorkspaceAtRequest, WorkspaceGrantsRequest, WorkspaceRequest,
    WorkspaceView, encode_result,
};
use crate::api::server::{
    ConnectionAuthority, RouterCommand, RouterHandle, RouterRequest, RouterResponse,
};
use crate::error::{CowshedError, ErrorCode, Result};
#[cfg(target_os = "macos")]
use crate::fork_lock::RunAsync as _;
use crate::metadata::WorkspaceName;
#[cfg(target_os = "macos")]
use crate::repository::OwnedRepoIds;
use crate::repository::{RepoId, RepositoryBinding};
#[cfg(target_os = "macos")]
use crate::timing::timed_async;

const ROUTER_CAPACITY: usize = 64;
const STARTUP_LIFECYCLE_ATTEMPTS: usize = 8;
const MAX_LOG_CHUNK_BYTES: usize = 64 * 1024;

/// The branch a workspace's commits are expected to reach.
///
/// One constant for two questions that must never disagree: where `land` merges by default, and
/// which branch `rm` requires to contain a workspace's head before destroying its object store.
/// If they diverged, `land` would satisfy a check `rm` does not make.
pub const DEFAULT_LANDING_BRANCH: &str = "main";

/// Immutable facts returned by one authoritative substrate enumeration.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkspaceSnapshot {
    pub info: WorkspaceInfo,
    pub grants: GrantSet,
    pub lifecycle_revision: u64,
    pub topology_revision: u64,
}

/// Project identity fixed for the lifetime of one controller actor.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProjectDescriptor {
    pub repo_id: RepoId,
    pub binding: std::sync::Arc<RepositoryBinding>,
    pub git_root: std::sync::Arc<Path>,
    pub storage: crate::storage::bootstrap::ValidatedHostStorage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeLogChunk {
    pub bytes: Bytes,
    pub next_offset: u64,
    pub eof: bool,
}

/// Actor-owned platform seam. Implementations must reread authoritative repository, storage,
/// metadata, mount, gateway, and supervisor facts inside every effectful method before mutation.
///
/// The seam exists for deterministic lifecycle/failpoint tests; production uses one of
/// [`ProjectRuntime::open_for_adopt`] or [`ProjectRuntime::open_existing`]. It deliberately
/// requires `&mut self`, preventing a backend from being shared behind a lock or mutated outside
/// the project actor.
#[async_trait]
pub trait ProjectRuntimeHost: Send + 'static {
    fn descriptor(&self) -> &ProjectDescriptor;

    async fn recover(&mut self) -> Result<()>;
    /// The verb (or startup recovery) that claimed this host's lifecycle-intent leases returned:
    /// release them, so an intent it left unfinished becomes crash residue for the next open
    /// rather than an operation this idle process still appears to be running.
    fn release_intent_leases(&mut self) {}
    async fn snapshots(&mut self) -> Result<Vec<WorkspaceSnapshot>>;
    async fn workspace_at(&mut self, path: PathBuf) -> Result<WorkspaceSnapshot>;

    async fn adopt(&mut self, options: AdoptOptions) -> Result<WorkspaceSnapshot>;
    async fn create(
        &mut self,
        workspace: WorkspaceName,
        options: CreateOptions,
    ) -> Result<WorkspaceSnapshot>;
    async fn fork(
        &mut self,
        source: WorkspaceName,
        destination: WorkspaceName,
    ) -> Result<WorkspaceSnapshot>;
    async fn rename(
        &mut self,
        source: WorkspaceName,
        destination: WorkspaceName,
    ) -> Result<WorkspaceSnapshot>;
    /// Move the project's checkout to a new path, keeping every record of where it lives in step.
    async fn move_checkout(&mut self, destination: PathBuf) -> Result<WorkspaceSnapshot>;
    async fn change_repo_id(&mut self, _repo_id: RepoId) -> Result<WorkspaceSnapshot> {
        Err(CowshedError::internal(
            "repository identity changes are unavailable for this runtime host",
        ))
    }
    /// Serve `workspace`'s supervisor from this process on its socket until it is retired.
    async fn serve_supervisor(&mut self, _workspace: WorkspaceName) -> Result<()> {
        Err(CowshedError::internal(
            "serving a workspace supervisor is unavailable for this runtime host",
        ))
    }
    async fn attach(&mut self, workspace: WorkspaceName, options: AttachOptions) -> Result<()>;
    async fn detach(&mut self, workspace: WorkspaceName) -> Result<()>;
    async fn resize(
        &mut self,
        workspace: WorkspaceName,
        capacity: String,
        volume: crate::api::dto::ResizeVolume,
    ) -> Result<crate::api::dto::ResizeResult>;
    async fn defragment(
        &mut self,
        workspace: WorkspaceName,
    ) -> Result<crate::api::dto::DefragmentResult>;
    /// Refreeze `workspace`'s seed from its live build volume when the seed is behind it and the
    /// volume has no writer (16_build_volumes.md, "Targets and seeds").
    async fn reseed(&mut self, workspace: WorkspaceName) -> Result<crate::api::dto::ReseedResult>;
    async fn checkpoint(
        &mut self,
        workspace: WorkspaceName,
        expected_incarnation: Option<WorkspaceIncarnation>,
        options: CheckpointOptions,
    ) -> Result<CheckpointResult>;
    async fn restore(&mut self, workspace: WorkspaceName, label: String) -> Result<()>;
    async fn remove(
        &mut self,
        workspace: WorkspaceName,
        options: RemoveOptions,
    ) -> Result<RemoveReport>;
    async fn gc(&mut self, options: GcOptions) -> Result<GcReport>;
    /// The session workspaces a listing does not show: clones whose create or fork never
    /// published them. `remove` retires them like any other.
    async fn unpublished_workspaces(&mut self) -> Result<Vec<WorkspaceName>>;
    /// Wait for every image reclamation this host started in the background, so a collection
    /// that follows plans against a settled store instead of racing it.
    async fn settle_reclaims(&mut self) -> Result<()>;
    /// Delete every `rm --abandon` bundle in the project's trash, answering the paths deleted.
    async fn delete_abandon_bundles(&mut self) -> Result<Vec<PathBuf>>;
    async fn grant(
        &mut self,
        workspace: WorkspaceName,
        delta: GrantDelta,
        revoke: bool,
    ) -> Result<GrantSet>;
    /// The project's standing grants from the trusted project policy.
    async fn project_grants(&mut self) -> Result<ProjectGrants>;
    /// Add (or, with `revoke`, remove) project-standing grants; every workspace of the project
    /// runs under them from its next supervisor launch.
    async fn grant_project(
        &mut self,
        delta: ProjectGrantDelta,
        revoke: bool,
    ) -> Result<ProjectGrants>;
    async fn assign_slot(&mut self, workspace: WorkspaceName, slot: u32) -> Result<()>;
    async fn set_checkpoint_quota(
        &mut self,
        workspace: WorkspaceName,
        quota: CheckpointQuota,
    ) -> Result<()>;
    /// Rebase `workspace` onto what it lands into: `into`'s checked-out branch, or main's when
    /// `into` is `None`.
    async fn rebase(
        &mut self,
        workspace: WorkspaceName,
        into: Option<WorkspaceTarget>,
        options: RebaseOptions,
    ) -> Result<RebaseReport>;
    /// Land `workspace` into `into`, or into main when `into` is `None`.
    async fn land(
        &mut self,
        workspace: WorkspaceName,
        into: Option<WorkspaceTarget>,
        options: LandOptions,
    ) -> Result<LandReport>;
    async fn push(
        &mut self,
        workspace: WorkspaceName,
        expected_incarnation: WorkspaceIncarnation,
        options: PushOptions,
    ) -> Result<PushReport>;
    async fn repo_mirror(&mut self, workspace: WorkspaceName, url: Url) -> Result<MirrorInfo>;
    async fn doctor(&mut self) -> Result<DoctorReport>;
    /// Bring the mounted `workspace`'s build volume in line with what capability detection names
    /// now (16_build_volumes.md, "One link per checkout"): its first volume when it has none,
    /// newly discovered paths linked, displaced links restored. Exec, land checks and the
    /// adoption check run it first; `cowshed setup` runs it for every mounted workspace.
    async fn refresh_build_state(
        &mut self,
        _workspace: WorkspaceName,
    ) -> Result<crate::build_volume::BuildStateRefresh> {
        Err(CowshedError::internal(
            "build volumes are unavailable for this runtime host",
        ))
    }

    /// The build volume a job of the mounted `workspace` admitted now is granted
    /// ([`crate::build_volume::BuildVolumeLayout::grant`]), read without refreshing anything;
    /// `None` when its checkout links none. The router has already proven the workspace mounted
    /// at the caller's incarnation.
    async fn build_volume(&mut self, _workspace: WorkspaceName) -> Result<Option<PathBuf>> {
        Err(CowshedError::internal(
            "build volumes are unavailable for this runtime host",
        ))
    }

    async fn open_worker(&mut self, workspace: WorkspaceName) -> Result<WorkspaceSnapshot>;
    async fn open_session(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        name: Option<String>,
    ) -> Result<()>;
    async fn close_session(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        name: Option<String>,
    ) -> Result<()>;
    async fn exec(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        session: Option<String>,
        request: ExecRequest,
    ) -> Result<JobId>;
    async fn stdin_write(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
        bytes: Bytes,
    ) -> Result<()>;
    async fn stdin_close(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<()>;
    async fn list_jobs(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
    ) -> Result<Vec<JobInfo>>;
    async fn job_info(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<JobInfo>;
    /// The job's terminal record from the workspace's durable records — answered for a job an
    /// earlier supervisor of the incarnation ran and sealed, which `job_info` is not.
    async fn sealed_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<SealedJob>;
    /// The job's terminal record, once it has one.
    async fn wait_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<JobAnswer<JobInfo>>;
    /// Stop the job; answered once it has ended.
    async fn kill_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<JobAnswer<()>>;
    async fn detach_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<()>;
    /// Bytes of one stream from `offset`; a follow read is answered once there are some or
    /// the stream has ended.
    async fn read_log(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
        stream: JobStream,
        offset: u64,
        follow: bool,
    ) -> Result<JobAnswer<RuntimeLogChunk>>;
}

/// An answer a job gives when it reaches a point — its end, its next output — and so may take
/// as long as the job runs.
///
/// The host resolves the job under the router's `&mut self` and hands this back; the router
/// awaits it on a task of its own. A router that awaited it in place would hold every other
/// request, of every client, until the job reached that point.
pub type JobAnswer<T> = Pin<Box<dyn Future<Output = Result<T>> + Send + 'static>>;

/// Cloneable ingress plus ownership of the single Tokio actor task.
pub struct ProjectRuntime {
    descriptor: ProjectDescriptor,
    router: RouterHandle,
    actor: JoinHandle<()>,
}

fn continuity_from_environment() -> Result<crate::storage::audit::ContinuityAudit> {
    crate::storage::audit::ContinuityAudit::from_environment().map_err(|error| {
        CowshedError::usage(
            error.to_string(),
            "unset COWSHED_CONTINUITY_AUDIT or set it to arrow|off",
        )
    })
}

/// How opening a recorded project reconciles its binding's remote URLs with Git configuration.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BindingRemoteValidation {
    /// The default: a transport move — the same owner/repo identity behind a new URL — heals the
    /// recorded URL in place, because identity is the owner/repo and the URL is only how to reach
    /// it. An identity move still refuses, naming the rebind verb.
    #[default]
    Strict,
    /// The identity-change verb only (`cowshed mv … --repo-id`). That verb supersedes the recorded
    /// identity and deliberately never touches the remote, so a remote already naming the new
    /// identity must not block the one verb that can record it. Nothing is healed or persisted on
    /// this path — the identity change rewrites the binding durably itself.
    ForIdentityChange,
}

/// Which unfinished lifecycle intents an opening replays, and whose failures are its own.
///
/// Every open finishes crash residue and starts a supervisor for every mounted workspace before
/// it serves, but both belong to a workspace. A verb answers for the workspaces it names and for
/// `main`, which every verb stands on. Another workspace's unfinished clone, or a mounted
/// workspace whose supervisor cannot start, is not its work: finishing or repairing it can take
/// minutes and can fail on a fault that has nothing to do with the verb, and neither may block
/// the verb. Such a failure is reported, and the verb that names that workspace meets it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RecoveryScope {
    /// Store-wide maintenance (`gc`): every unfinished intent is replayed. Only `main`'s is the
    /// opening's own; any other intent that fails to replay is reported and stays journaled for
    /// a later pass.
    Store,
    /// A verb naming these workspaces (none, for a verb that names none): only their intents and
    /// `main`'s are replayed, and each failure fails the verb. Every other intent stays journaled,
    /// untouched.
    Workspaces(std::collections::BTreeSet<WorkspaceName>),
    /// A named retirement: [`Self::Workspaces`] of its one target, except that the target's own
    /// unpublished create or fork is left for the retirement to retire rather than finished
    /// first, so nothing that stopped the clone can also stop its removal.
    Removal(WorkspaceName),
    /// Inspection (`doctor` without `--repair`): the opening finishes nothing. No lifecycle intent
    /// is replayed, `main`'s included; no interrupted publication or restore is completed; no
    /// retired image is reclaimed; no identity change is finished; no healed binding is written.
    /// The opening reads the store as it is, and doctor reports what any other opening would
    /// finish as findings.
    Inspect,
}

/// What recovery does with one unfinished intent under a [`RecoveryScope`]. Only the native
/// macOS host recovers, so only it asks.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IntentReplay {
    /// Another verb's work: left journaled and unleased.
    Leave,
    /// The opening's own work: replayed, and a failure fails the opening.
    Own,
    /// Store-wide residue: replayed, and a failure is reported and the record retained.
    Residue,
}

#[cfg(target_os = "macos")]
impl RecoveryScope {
    fn replay(&self, workspace: &WorkspaceName) -> IntentReplay {
        match self {
            Self::Inspect => IntentReplay::Leave,
            _ if workspace.is_main() => IntentReplay::Own,
            Self::Store => IntentReplay::Residue,
            Self::Workspaces(named) if named.contains(workspace) => IntentReplay::Own,
            Self::Removal(target) if target == workspace => IntentReplay::Own,
            Self::Workspaces(_) | Self::Removal(_) => IntentReplay::Leave,
        }
    }

    fn removal_target(&self) -> Option<&WorkspaceName> {
        match self {
            Self::Removal(target) => Some(target),
            Self::Store | Self::Workspaces(_) | Self::Inspect => None,
        }
    }

    /// Whether the opening may finish what interrupted work left behind.
    fn repairs(&self) -> bool {
        match self {
            Self::Store | Self::Workspaces(_) | Self::Removal(_) => true,
            Self::Inspect => false,
        }
    }
}

#[cfg(all(test, target_os = "macos"))]
mod recovery_scope_tests {
    use super::{IntentReplay, RecoveryScope};
    use crate::metadata::WorkspaceName;

    fn name(value: &str) -> WorkspaceName {
        WorkspaceName::new(value).unwrap()
    }

    /// `cowshed land a` must not finish, or fail on, workspace b's unfinished clone.
    #[test]
    fn a_verb_replays_its_named_workspaces_and_main_and_leaves_every_other_intent() {
        let scope = RecoveryScope::Workspaces([name("a"), name("c")].into());
        assert_eq!(scope.replay(&name("a")), IntentReplay::Own);
        assert_eq!(scope.replay(&name("c")), IntentReplay::Own);
        assert_eq!(scope.replay(&name("main")), IntentReplay::Own);
        assert_eq!(scope.replay(&name("b")), IntentReplay::Leave);

        let unnamed = RecoveryScope::Workspaces(Default::default());
        assert_eq!(unnamed.replay(&name("main")), IntentReplay::Own);
        assert_eq!(unnamed.replay(&name("b")), IntentReplay::Leave);
    }

    #[test]
    fn a_removal_owns_only_its_target_and_is_the_only_scope_naming_one() {
        let scope = RecoveryScope::Removal(name("gone"));
        assert_eq!(scope.replay(&name("gone")), IntentReplay::Own);
        assert_eq!(scope.replay(&name("main")), IntentReplay::Own);
        assert_eq!(scope.replay(&name("other")), IntentReplay::Leave);
        assert_eq!(scope.removal_target(), Some(&name("gone")));
        assert_eq!(
            RecoveryScope::Workspaces([name("gone")].into()).removal_target(),
            None
        );
        assert_eq!(RecoveryScope::Store.removal_target(), None);
    }

    /// `gc` finishes residue everywhere, but only `main`'s failure is its own.
    #[test]
    fn the_store_scope_replays_everything_and_owns_only_main() {
        assert_eq!(
            RecoveryScope::Store.replay(&name("main")),
            IntentReplay::Own
        );
        assert_eq!(
            RecoveryScope::Store.replay(&name("b")),
            IntentReplay::Residue
        );
    }

    /// `doctor` reads the store as it is: not even `main`'s unfinished work is replayed.
    #[test]
    fn inspection_replays_nothing_and_repairs_nothing() {
        assert_eq!(
            RecoveryScope::Inspect.replay(&name("main")),
            IntentReplay::Leave
        );
        assert_eq!(
            RecoveryScope::Inspect.replay(&name("b")),
            IntentReplay::Leave
        );
        assert_eq!(RecoveryScope::Inspect.removal_target(), None);
        assert!(!RecoveryScope::Inspect.repairs());
        assert!(RecoveryScope::Workspaces(Default::default()).repairs());
    }
}

impl ProjectRuntime {
    /// Opens the production runtime with foreground provisioning authority.
    ///
    /// Only the parsed `cowshed adopt` command may call this entrypoint.
    pub async fn open_for_adopt(
        project_root: impl AsRef<Path>,
        requested_repo_id: Option<RepoId>,
    ) -> Result<Self> {
        Self::open_native(
            project_root.as_ref(),
            crate::storage::bootstrap::native::NativeBootstrapMode::Provision,
            requested_repo_id,
            continuity_from_environment()?,
            BindingRemoteValidation::Strict,
            RecoveryScope::Workspaces(std::collections::BTreeSet::new()),
            None,
        )
        .await
    }

    /// Opens the production runtime without storage provisioning authority, for one verb.
    ///
    /// Ordinary commands must use this entrypoint, naming the workspaces the verb acts on in
    /// `scope`: recovery then finishes only their unfinished lifecycle work and `main`'s. Missing
    /// or incorrectly mounted storage fails closed without creating or mounting anything. The
    /// audit sink is the standalone default (`COWSHED_CONTINUITY_AUDIT`,
    /// §[`crate::storage::audit`]).
    pub async fn open_existing(
        project_root: impl AsRef<Path>,
        scope: RecoveryScope,
    ) -> Result<Self> {
        Self::open_native(
            project_root.as_ref(),
            crate::storage::bootstrap::native::NativeBootstrapMode::ExistingOnly,
            None,
            continuity_from_environment()?,
            BindingRemoteValidation::Strict,
            scope,
            None,
        )
        .await
    }

    /// Opens for the identity-change verb alone. The verb rebinds the project rather than any
    /// session workspace, so it finishes only `main`'s unfinished lifecycle work.
    ///
    /// Only the parsed `cowshed mv … --repo-id` command may call this entrypoint: it relaxes the
    /// binding's remote check to [`BindingRemoteValidation::ForIdentityChange`] so the verb stays
    /// reachable after the remote already moved to the identity it is about to record.
    pub async fn open_existing_for_identity_change(project_root: impl AsRef<Path>) -> Result<Self> {
        Self::open_native(
            project_root.as_ref(),
            crate::storage::bootstrap::native::NativeBootstrapMode::ExistingOnly,
            None,
            continuity_from_environment()?,
            BindingRemoteValidation::ForIdentityChange,
            RecoveryScope::Workspaces(std::collections::BTreeSet::new()),
            None,
        )
        .await
    }

    /// Opens the resident controller with the host's own audit sink — the entrypoint a
    /// supervising runtime uses to route controller audit records into its durable log instead of
    /// Arrow files. The controller starts with only `main`'s unfinished lifecycle work: residue
    /// another workspace left is finished by `gc` or by the verb that names it, and never delays
    /// or fails the controller that every other workspace is waiting on.
    pub async fn open_existing_with_audit(
        project_root: impl AsRef<Path>,
        continuity: crate::storage::audit::ContinuityAudit,
    ) -> Result<Self> {
        Self::open_native(
            project_root.as_ref(),
            crate::storage::bootstrap::native::NativeBootstrapMode::ExistingOnly,
            None,
            continuity,
            BindingRemoteValidation::Strict,
            RecoveryScope::Workspaces(std::collections::BTreeSet::new()),
            None,
        )
        .await
    }

    /// Open an isolated caller-provisioned APFS store without touching machine-global volumes.
    /// The caller owns the supplied roots and their teardown; production uses `open_for_adopt`.
    pub async fn open_for_adopt_at(
        project_root: impl AsRef<Path>,
        requested_repo_id: Option<RepoId>,
        storage: crate::storage::bootstrap::ValidatedHostStorage,
    ) -> Result<Self> {
        Self::open_native(
            project_root.as_ref(),
            crate::storage::bootstrap::native::NativeBootstrapMode::Provision,
            requested_repo_id,
            continuity_from_environment()?,
            BindingRemoteValidation::Strict,
            RecoveryScope::Workspaces(std::collections::BTreeSet::new()),
            Some(storage),
        )
        .await
    }

    /// Reopen the same caller-owned store under ordinary existing-only recovery rules.
    pub async fn open_existing_at(
        project_root: impl AsRef<Path>,
        scope: RecoveryScope,
        storage: crate::storage::bootstrap::ValidatedHostStorage,
    ) -> Result<Self> {
        Self::open_native(
            project_root.as_ref(),
            crate::storage::bootstrap::native::NativeBootstrapMode::ExistingOnly,
            None,
            continuity_from_environment()?,
            BindingRemoteValidation::Strict,
            scope,
            Some(storage),
        )
        .await
    }

    async fn open_native(
        project_root: &Path,
        mode: crate::storage::bootstrap::native::NativeBootstrapMode,
        requested_repo_id: Option<RepoId>,
        continuity: crate::storage::audit::ContinuityAudit,
        validation: BindingRemoteValidation,
        recovery_scope: RecoveryScope,
        storage: Option<crate::storage::bootstrap::ValidatedHostStorage>,
    ) -> Result<Self> {
        #[cfg(target_os = "macos")]
        {
            let host = NativeProjectRuntimeHost::open(
                project_root,
                mode,
                requested_repo_id.as_ref(),
                continuity,
                validation,
                recovery_scope,
                storage,
            )
            .await?;
            Self::start(host).await
        }
        #[cfg(not(target_os = "macos"))]
        {
            let _ = (
                project_root,
                mode,
                requested_repo_id,
                continuity,
                validation,
                recovery_scope,
                storage,
            );
            Err(CowshedError::environment_missing(
                "the native cowshed project runtime requires macOS APFS",
                "run the controller on macOS or use an injected test host",
            ))
        }
    }

    /// Starts a runtime from an injected host after strict recovery completes.
    pub async fn start(mut host: impl ProjectRuntimeHost) -> Result<Self> {
        for attempt in 1..=STARTUP_LIFECYCLE_ATTEMPTS {
            match host.recover().await {
                Ok(()) => break,
                Err(error) => match error.lifecycle_conflict_source() {
                    Some(_) if attempt < STARTUP_LIFECYCLE_ATTEMPTS => {
                        // Recovery is the refresh boundary: it reloads the intent journal and
                        // authoritative workspace inventory before constructing any new plan.
                        continue;
                    }
                    Some(conflict) => return Err(startup_conflict_exhausted(conflict)),
                    None => return Err(error),
                },
            }
        }
        host.release_intent_leases();
        let descriptor = host.descriptor().clone();
        let capacity = NonZeroUsize::new(ROUTER_CAPACITY)
            .ok_or_else(|| CowshedError::internal("project router capacity is zero"))?;
        let (router, receiver) = RouterHandle::channel(capacity);
        let actor = tokio::spawn(ProjectActor::new(Box::new(host), receiver).run());
        Ok(Self {
            descriptor,
            router,
            actor,
        })
    }

    pub fn descriptor(&self) -> &ProjectDescriptor {
        &self.descriptor
    }

    pub fn router(&self) -> RouterHandle {
        self.router.clone()
    }

    /// Serve `workspace`'s supervisor from this process on its socket until a client retires
    /// it. The runtime serves nothing else meanwhile: this is the whole of the
    /// `cowshed __workspace-supervisor` process.
    pub async fn serve_supervisor(&self, workspace: &WorkspaceName) -> Result<()> {
        self.router
            .call::<operations::CoordinatorServeSupervisor>(
                self.coordinator(),
                self.workspace_request(workspace),
            )
            .await
            .map(|EmptyResult {}| ())
    }

    /// Refresh the mounted `workspace`'s build state ([`ProjectRuntimeHost::refresh_build_state`]):
    /// what `cowshed setup` runs for each mounted workspace.
    pub async fn refresh_build_state(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<crate::build_volume::BuildStateRefresh> {
        self.router
            .call::<operations::CoordinatorRefreshBuildState>(
                self.coordinator(),
                self.workspace_request(workspace),
            )
            .await
    }

    fn coordinator(&self) -> ConnectionAuthority {
        ConnectionAuthority::Coordinator {
            repo_id: self.descriptor.repo_id.clone(),
        }
    }

    fn workspace_request(&self, workspace: &WorkspaceName) -> WorkspaceRequest {
        WorkspaceRequest {
            repo_id: self.descriptor.repo_id.clone(),
            workspace: workspace.clone(),
        }
    }

    pub async fn shutdown(self) -> Result<()> {
        drop(self.router);
        self.actor
            .await
            .map_err(|error| CowshedError::internal(format!("project actor failed: {error}")))
    }
}

fn startup_conflict_exhausted(conflict: &crate::storage::lifecycle::Conflict) -> CowshedError {
    use crate::storage::lifecycle::Conflict;

    let changing_fact = match conflict {
        Conflict::FactCount { expected, actual } => {
            format!(
                "authoritative fact count kept changing (last expected {expected}, actual {actual})"
            )
        }
        Conflict::Stale { index, .. } => format!("authoritative fact {index} kept changing"),
    };
    CowshedError::conflict(
        format!(
            "controller startup exhausted {STARTUP_LIFECYCLE_ATTEMPTS} lifecycle attempts; \
             {changing_fact}"
        ),
        "retry startup when workspace lifecycle state stabilizes",
    )
}

/// The router's answer to one request: ready now, or when the job it waits on gets there.
enum Routed {
    Now(RouterResponse),
    Later(JobAnswer<RouterResponse>),
}

struct ProjectActor {
    host: Box<dyn ProjectRuntimeHost>,
    receiver: mpsc::Receiver<RouterCommand>,
}

impl ProjectActor {
    fn new(host: Box<dyn ProjectRuntimeHost>, receiver: mpsc::Receiver<RouterCommand>) -> Self {
        Self { host, receiver }
    }

    async fn run(mut self) {
        while let Some(command) = self.receiver.recv().await {
            let (request, reply) = command.into_parts();
            let routed = match request.steps().cloned() {
                Some(steps) => crate::timing::reporting(steps, self.route(request)).await,
                None => self.route(request).await,
            };
            self.host.release_intent_leases();
            match routed {
                Ok(Routed::Now(response)) => {
                    let _ = reply.send(Ok(response));
                }
                Ok(Routed::Later(answer)) => {
                    tokio::spawn(async move {
                        let _ = reply.send(answer.await);
                    });
                }
                Err(error) => {
                    let _ = reply.send(Err(error));
                }
            }
        }
    }

    async fn route(&mut self, request: RouterRequest) -> Result<Routed> {
        self.validate_connection_authority(request.authority())?;
        let method = request.method();
        let _span = crate::timing::span_named("route", || method.to_owned());
        let (authority, operation, upload, _) = request.into_parts();
        let scope = operations::operation(method)
            .map(|info| info.scope)
            .ok_or_else(|| CowshedError::internal(format!("{method} has no declaration")))?;
        if scope != Scope::Worker {
            require_coordinator(&authority)?;
        }
        use OperationRequest as Op;
        let response = match operation {
            Op::JobLogs(params) => {
                return self.job_logs(&authority, params).await.map(Routed::Later);
            }
            Op::JobWait(params) => {
                return self.job_wait(&authority, params).await.map(Routed::Later);
            }
            Op::JobKill(params) => {
                return self.job_kill(&authority, params).await.map(Routed::Later);
            }
            Op::ProjectOpen(params) => {
                respond::<operations::ProjectOpen>(&self.project_open(params).await?)
            }
            Op::ProjectWorkspace(params) => respond::<operations::ProjectWorkspace>(
                &self.project_workspace(&authority, params).await?,
            ),
            Op::ProjectWorkspaceAt(params) => respond::<operations::ProjectWorkspaceAt>(
                &self.project_workspace_at(&authority, params).await?,
            ),
            Op::ProjectList(params) => {
                respond::<operations::ProjectList>(&self.project_list(params).await?)
            }
            Op::WorkspaceInfoRead(params) => respond::<operations::WorkspaceInfoRead>(
                &self.workspace_info(&authority, params).await?,
            ),
            Op::WorkspaceAttach(params) => {
                self.require_scoped_workspace(&authority, &params.repo_id, &params.workspace)
                    .await?;
                self.host.attach(params.workspace, params.options).await?;
                respond::<operations::WorkspaceAttach>(&EmptyResult {})
            }
            Op::WorkspaceGrants(params) => respond::<operations::WorkspaceGrants>(
                &self.workspace_grants(&authority, params).await?,
            ),
            Op::WorkspaceBuildVolume(params) => respond::<operations::WorkspaceBuildVolume>(
                &self.workspace_build_volume(params).await?,
            ),
            Op::CoordinatorAdopt(params) => {
                respond::<operations::CoordinatorAdopt>(&self.coordinator_adopt(params).await?)
            }
            Op::CoordinatorCreate(params) => {
                self.require_repo(&params.repo_id)?;
                let snapshot = self.host.create(params.workspace, params.options).await?;
                respond::<operations::CoordinatorCreate>(&workspace_view(snapshot))
            }
            Op::CoordinatorFork(params) => {
                self.require_repo(&params.repo_id)?;
                let snapshot = self.host.fork(params.source, params.destination).await?;
                respond::<operations::CoordinatorFork>(&workspace_view(snapshot))
            }
            Op::CoordinatorRename(params) => {
                self.require_repo(&params.repo_id)?;
                let snapshot = self.host.rename(params.source, params.destination).await?;
                respond::<operations::CoordinatorRename>(&workspace_view(snapshot))
            }
            Op::CoordinatorMoveCheckout(params) => {
                self.require_repo(&params.repo_id)?;
                let snapshot = self.host.move_checkout(params.destination).await?;
                respond::<operations::CoordinatorMoveCheckout>(&workspace_view(snapshot))
            }
            Op::CoordinatorChangeRepoId(params) => {
                self.require_repo(&params.repo_id)?;
                let snapshot = self.host.change_repo_id(params.new_repo_id).await?;
                respond::<operations::CoordinatorChangeRepoId>(&workspace_view(snapshot))
            }
            Op::CoordinatorGrant(params) => respond::<operations::CoordinatorGrant>(
                &self.coordinator_grant(params, false).await?,
            ),
            Op::CoordinatorRevoke(params) => respond::<operations::CoordinatorRevoke>(
                &self.coordinator_grant(params, true).await?,
            ),
            Op::CoordinatorProjectGrants(params) => {
                self.require_repo(&params.repo_id)?;
                respond::<operations::CoordinatorProjectGrants>(&self.host.project_grants().await?)
            }
            Op::CoordinatorGrantProject(params) => {
                self.require_repo(&params.repo_id)?;
                respond::<operations::CoordinatorGrantProject>(
                    &self.host.grant_project(params.delta, false).await?,
                )
            }
            Op::CoordinatorRevokeProject(params) => {
                self.require_repo(&params.repo_id)?;
                respond::<operations::CoordinatorRevokeProject>(
                    &self.host.grant_project(params.delta, true).await?,
                )
            }
            Op::CoordinatorRebase(params) => {
                self.require_repo(&params.repo_id)?;
                let report = self
                    .host
                    .rebase(params.workspace, params.into, params.options)
                    .await?;
                respond::<operations::CoordinatorRebase>(&report)
            }
            Op::CoordinatorLand(params) => {
                self.require_repo(&params.repo_id)?;
                let report = self
                    .host
                    .land(params.workspace, params.into, params.options)
                    .await?;
                respond::<operations::CoordinatorLand>(&report)
            }
            Op::CoordinatorRestore(params) => {
                self.require_repo(&params.repo_id)?;
                self.host.restore(params.workspace, params.label).await?;
                respond::<operations::CoordinatorRestore>(&EmptyResult {})
            }
            Op::CoordinatorResize(params) => {
                self.require_repo(&params.repo_id)?;
                let result = self
                    .host
                    .resize(params.workspace, params.capacity, params.volume)
                    .await?;
                respond::<operations::CoordinatorResize>(&result)
            }
            Op::CoordinatorDefragment(params) => {
                self.require_repo(&params.repo_id)?;
                respond::<operations::CoordinatorDefragment>(
                    &self.host.defragment(params.workspace).await?,
                )
            }
            Op::CoordinatorReseed(params) => {
                self.require_repo(&params.repo_id)?;
                respond::<operations::CoordinatorReseed>(&self.host.reseed(params.workspace).await?)
            }
            Op::CoordinatorDetach(params) => {
                self.require_repo(&params.repo_id)?;
                self.host.detach(params.workspace).await?;
                respond::<operations::CoordinatorDetach>(&EmptyResult {})
            }
            Op::CoordinatorServeSupervisor(params) => {
                self.require_repo(&params.repo_id)?;
                self.host.serve_supervisor(params.workspace).await?;
                respond::<operations::CoordinatorServeSupervisor>(&EmptyResult {})
            }
            Op::CoordinatorRefreshBuildState(params) => {
                self.require_repo(&params.repo_id)?;
                respond::<operations::CoordinatorRefreshBuildState>(
                    &self.host.refresh_build_state(params.workspace).await?,
                )
            }
            Op::CoordinatorAssignSlot(params) => {
                self.require_repo(&params.repo_id)?;
                self.host.assign_slot(params.workspace, params.slot).await?;
                respond::<operations::CoordinatorAssignSlot>(&EmptyResult {})
            }
            Op::CoordinatorDestroy(params) => {
                self.require_repo(&params.repo_id)?;
                let report = self.host.remove(params.workspace, params.options).await?;
                respond::<operations::CoordinatorDestroy>(&report)
            }
            Op::CoordinatorGc(params) => {
                self.require_repo(&params.repo_id)?;
                respond::<operations::CoordinatorGc>(&self.host.gc(params.options).await?)
            }
            Op::CoordinatorRemoveProject(params) => {
                self.require_repo(&params.repo_id)?;
                respond::<operations::CoordinatorRemoveProject>(
                    &remove_project(self.host.as_mut(), params.options).await?,
                )
            }
            Op::CoordinatorRepoMirror(params) => {
                self.require_repo(&params.repo_id)?;
                let url = Url::parse(&params.url).map_err(|error| {
                    CowshedError::usage(
                        format!("invalid repository mirror URL: {error}"),
                        "use an absolute supported repository URL",
                    )
                })?;
                respond::<operations::CoordinatorRepoMirror>(
                    &self.host.repo_mirror(params.workspace, url).await?,
                )
            }
            Op::CoordinatorSetCheckpointQuota(params) => {
                self.require_repo(&params.repo_id)?;
                self.host
                    .set_checkpoint_quota(params.workspace, params.quota)
                    .await?;
                respond::<operations::CoordinatorSetCheckpointQuota>(&EmptyResult {})
            }
            Op::CoordinatorDoctor(params) => {
                self.require_repo(&params.repo_id)?;
                respond::<operations::CoordinatorDoctor>(&self.host.doctor().await?)
            }
            Op::CoordinatorWorker(params) => {
                self.require_repo(&params.repo_id)?;
                let snapshot = self.host.open_worker(params.workspace).await?;
                respond::<operations::CoordinatorWorker>(&workspace_view(snapshot))
            }
            Op::WorkerExec(params) => respond::<operations::WorkerExec>(
                &self.worker_exec(&authority, params, upload).await?,
            ),
            Op::WorkerStdinChunk(params) => {
                self.stdin_chunk(&authority, params, upload).await?;
                respond::<operations::WorkerStdinChunk>(&EmptyResult {})
            }
            Op::JobAttachWrite(params) => {
                self.stdin_chunk(&authority, params, upload).await?;
                respond::<operations::JobAttachWrite>(&EmptyResult {})
            }
            Op::WorkerStdinClose(params) => {
                self.require_scoped_workspace(&authority, &params.repo_id, &params.workspace)
                    .await?;
                self.host
                    .stdin_close(
                        params.workspace,
                        params.workspace_incarnation,
                        params.job_id,
                    )
                    .await?;
                respond::<operations::WorkerStdinClose>(&EmptyResult {})
            }
            Op::WorkerShell(params) => {
                self.require_scoped_workspace(&authority, &params.repo_id, &params.workspace)
                    .await?;
                self.host
                    .open_session(
                        params.workspace,
                        params.workspace_incarnation,
                        params.session,
                    )
                    .await?;
                respond::<operations::WorkerShell>(&EmptyResult {})
            }
            Op::WorkerListJobs(params) => {
                self.require_scoped_workspace(&authority, &params.repo_id, &params.workspace)
                    .await?;
                respond::<operations::WorkerListJobs>(
                    &self
                        .host
                        .list_jobs(params.workspace, params.workspace_incarnation)
                        .await?,
                )
            }
            Op::WorkerJob(params) => {
                respond::<operations::WorkerJob>(&self.job_info(&authority, params).await?)
            }
            Op::JobStatus(params) => {
                respond::<operations::JobStatus>(&self.job_info(&authority, params).await?)
            }
            Op::JobSealed(params) => {
                self.require_scoped_workspace(&authority, &params.repo_id, &params.workspace)
                    .await?;
                respond::<operations::JobSealed>(
                    &self
                        .host
                        .sealed_job(
                            params.workspace,
                            params.workspace_incarnation,
                            params.job_id,
                        )
                        .await?,
                )
            }
            Op::WorkerCheckpoint(params) => {
                self.require_scoped_workspace(&authority, &params.repo_id, &params.workspace)
                    .await?;
                respond::<operations::WorkerCheckpoint>(
                    &self
                        .host
                        .checkpoint(
                            params.workspace,
                            Some(params.workspace_incarnation),
                            params.options,
                        )
                        .await?,
                )
            }
            Op::WorkerPush(params) => {
                self.require_scoped_workspace(&authority, &params.repo_id, &params.workspace)
                    .await?;
                respond::<operations::WorkerPush>(
                    &self
                        .host
                        .push(
                            params.workspace,
                            params.workspace_incarnation,
                            params.options,
                        )
                        .await?,
                )
            }
            Op::JobDetach(params) => {
                self.require_scoped_workspace(&authority, &params.repo_id, &params.workspace)
                    .await?;
                self.host
                    .detach_job(
                        params.workspace,
                        params.workspace_incarnation,
                        params.job_id,
                    )
                    .await?;
                respond::<operations::JobDetach>(&EmptyResult {})
            }
            Op::SessionClose(params) => {
                self.require_scoped_workspace(&authority, &params.repo_id, &params.workspace)
                    .await?;
                self.host
                    .close_session(
                        params.workspace,
                        params.workspace_incarnation,
                        params.session,
                    )
                    .await?;
                respond::<operations::SessionClose>(&EmptyResult {})
            }
        };
        response.map(Routed::Now)
    }

    fn validate_connection_authority(&self, authority: &ConnectionAuthority) -> Result<()> {
        if authority.repo_id() != &self.host.descriptor().repo_id {
            return Err(CowshedError::conflict(
                "connection repository authority does not match this project runtime",
                "reopen the project through its bound controller descriptor",
            ));
        }
        Ok(())
    }

    async fn project_open(&mut self, params: ProjectOpenRequest) -> Result<ProjectOpened> {
        let requested = canonical_input_path(&params.path)?;
        // The caller names where it is; the descriptor names the project. They are the same
        // directory when the caller stands in main's checkout, and different ones whenever it
        // stands in a workspace mount — which is the arrangement `cowshed rebase` and friends
        // support by inferring their workspace from the cwd. String identity therefore refused the
        // very callers the inference exists for, so the question asked is the one that matters:
        // does the requested path belong to this project? A path resolving to the bound root does,
        // and so does a workspace mount whose marker records that root.
        let bound = self.host.descriptor().git_root.clone();
        let belongs = names_one_root(&requested, &bound)
            || read_workspace_marker(&requested)
                .await?
                .is_some_and(|marker| names_one_root(&marker.project_root, &bound));
        if !belongs {
            return Err(CowshedError::conflict(
                format!(
                    "project path {} does not belong to the bound project at {}",
                    requested.display(),
                    bound.display()
                ),
                "reopen the controller for the requested repository",
            ));
        }
        let descriptor = self.host.descriptor();
        Ok(ProjectOpened {
            repo_id: descriptor.repo_id.clone(),
            binding: std::sync::Arc::clone(&descriptor.binding),
            git_root: std::sync::Arc::clone(&descriptor.git_root),
            store_root: std::sync::Arc::clone(descriptor.storage.roots().shared_store()),
        })
    }

    /// The named workspace, proven to be the one a worker connection is fenced to.
    async fn scoped_snapshot(
        &mut self,
        authority: &ConnectionAuthority,
        repo_id: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<WorkspaceSnapshot> {
        self.require_repo(repo_id)?;
        let snapshot = take_workspace(self.host.snapshots().await?, workspace)?;
        self.validate_worker_snapshot(authority, &snapshot)?;
        Ok(snapshot)
    }

    async fn project_workspace(
        &mut self,
        authority: &ConnectionAuthority,
        params: WorkspaceRequest,
    ) -> Result<WorkspaceView> {
        self.scoped_snapshot(authority, &params.repo_id, &params.workspace)
            .await
            .map(workspace_view)
    }

    async fn project_workspace_at(
        &mut self,
        authority: &ConnectionAuthority,
        params: WorkspaceAtRequest,
    ) -> Result<WorkspaceView> {
        self.require_repo(&params.repo_id)?;
        let path = canonical_input_path(&params.path)?;
        let snapshot = self.host.workspace_at(path).await?;
        self.validate_worker_snapshot(authority, &snapshot)?;
        Ok(workspace_view(snapshot))
    }

    async fn project_list(&mut self, params: RepoRequest) -> Result<Vec<WorkspaceView>> {
        self.require_repo(&params.repo_id)?;
        Ok(self
            .host
            .snapshots()
            .await?
            .into_iter()
            .map(workspace_view)
            .collect())
    }

    async fn workspace_info(
        &mut self,
        authority: &ConnectionAuthority,
        params: WorkspaceRequest,
    ) -> Result<WorkspaceInfo> {
        self.scoped_snapshot(authority, &params.repo_id, &params.workspace)
            .await
            .map(|snapshot| snapshot.info)
    }

    /// A workspace's grants. A request that holds an incarnation is fenced on it whatever its
    /// connection: a worker handle minted on the coordinator's connection holds one too, and a
    /// name recreated meanwhile never answers for the incarnation it was minted for.
    async fn workspace_grants(
        &mut self,
        authority: &ConnectionAuthority,
        params: WorkspaceGrantsRequest,
    ) -> Result<GrantSet> {
        let snapshot = self
            .scoped_snapshot(authority, &params.repo_id, &params.workspace)
            .await?;
        if let Some(held) = &params.workspace_incarnation {
            Self::require_held_incarnation(&snapshot, held)?;
        }
        Ok(snapshot.grants)
    }

    /// The build volume a job of the workspace would be granted now: what a coordinator that
    /// confines its own reads to a workspace follows the checkout's build link into
    /// (16_build_volumes.md, "Process lifetime across a swap"). Coordinator-only, and fenced on
    /// the incarnation, so a name recreated meanwhile never answers for the one the caller holds.
    async fn workspace_build_volume(&mut self, params: WorkerScope) -> Result<BuildVolume> {
        self.require_repo(&params.repo_id)?;
        let snapshots = self.host.snapshots().await?;
        let snapshot = find_workspace(&snapshots, &params.workspace)?;
        Self::require_held_incarnation(snapshot, &params.workspace_incarnation)?;
        if snapshot.info.state != WorkspaceState::Attached {
            return Err(CowshedError::conflict(
                format!(
                    "workspace {} is detached; its build link cannot be read",
                    params.workspace
                ),
                format!("cowshed attach {}, then retry", params.workspace),
            ));
        }
        let volume = self.host.build_volume(params.workspace).await?;
        Ok(BuildVolume { volume })
    }

    /// Refuses a request that holds another incarnation of `snapshot`'s workspace than the one it
    /// is now.
    fn require_held_incarnation(
        snapshot: &WorkspaceSnapshot,
        held: &WorkspaceIncarnation,
    ) -> Result<()> {
        if &snapshot.info.workspace_incarnation == held {
            return Ok(());
        }
        Err(CowshedError::fence_refusal(
            crate::error::FenceRefusal::IncarnationMoved {
                workspace: snapshot.info.workspace.clone(),
                observed: snapshot.info.workspace_incarnation.clone(),
            },
            "workspace incarnation is stale",
            "resolve the workspace again and retry",
        ))
    }

    async fn coordinator_adopt(&mut self, params: AdoptRequest) -> Result<WorkspaceView> {
        self.require_repo(&params.repo_id)?;
        if params
            .options
            .repo_id
            .as_ref()
            .is_some_and(|repo_id| repo_id != &self.host.descriptor().repo_id)
        {
            return Err(CowshedError::conflict(
                "adopt repository identity differs from the provisional project binding",
                "retry with the repository identity selected while opening the project",
            ));
        }
        self.host.adopt(params.options).await.map(workspace_view)
    }

    async fn coordinator_grant(&mut self, params: GrantRequest, revoke: bool) -> Result<GrantSet> {
        self.require_repo(&params.repo_id)?;
        requested_port_block_size(&params.delta, revoke)?;
        self.host
            .grant(params.workspace, params.delta, revoke)
            .await
    }

    async fn worker_exec(
        &mut self,
        authority: &ConnectionAuthority,
        params: ExecParams,
        upload: Option<Bytes>,
    ) -> Result<JobId> {
        let (scope, session, exec) = exec_request(params, upload)?;
        self.require_scoped_workspace(authority, &scope.repo_id, &scope.workspace)
            .await?;
        self.host
            .exec(scope.workspace, scope.workspace_incarnation, session, exec)
            .await
    }

    async fn stdin_chunk(
        &mut self,
        authority: &ConnectionAuthority,
        params: JobRequest,
        upload: Option<Bytes>,
    ) -> Result<()> {
        self.require_scoped_workspace(authority, &params.repo_id, &params.workspace)
            .await?;
        let bytes = upload.ok_or_else(|| {
            CowshedError::usage(
                "stdin chunk is missing binary data",
                "retry the stdin write",
            )
        })?;
        self.host
            .stdin_write(
                params.workspace,
                params.workspace_incarnation,
                params.job_id,
                bytes,
            )
            .await
    }

    async fn job_info(
        &mut self,
        authority: &ConnectionAuthority,
        params: JobRequest,
    ) -> Result<JobInfo> {
        self.require_scoped_workspace(authority, &params.repo_id, &params.workspace)
            .await?;
        self.host
            .job_info(
                params.workspace,
                params.workspace_incarnation,
                params.job_id,
            )
            .await
    }

    async fn job_logs(
        &mut self,
        authority: &ConnectionAuthority,
        params: LogsRequest,
    ) -> Result<JobAnswer<RouterResponse>> {
        self.require_scoped_workspace(authority, &params.repo_id, &params.workspace)
            .await?;
        let chunk = self
            .host
            .read_log(
                params.workspace,
                params.workspace_incarnation,
                params.job_id,
                params.stream,
                params.offset,
                params.follow,
            )
            .await?;
        Ok(Box::pin(async move {
            let chunk = chunk.await?;
            if chunk.bytes.len() > MAX_LOG_CHUNK_BYTES {
                return Err(CowshedError::internal(
                    "supervisor returned a log chunk larger than the transport frame",
                ));
            }
            RouterResponse::binary(
                encode_result::<operations::JobLogs>(&LogsChunk {
                    eof: chunk.eof,
                    next_offset: chunk.next_offset,
                })?,
                chunk.bytes,
            )
        }))
    }

    async fn job_wait(
        &mut self,
        authority: &ConnectionAuthority,
        params: JobRequest,
    ) -> Result<JobAnswer<RouterResponse>> {
        self.require_scoped_workspace(authority, &params.repo_id, &params.workspace)
            .await?;
        let info = self
            .host
            .wait_job(
                params.workspace,
                params.workspace_incarnation,
                params.job_id,
            )
            .await?;
        Ok(Box::pin(async move {
            respond::<operations::JobWait>(&info.await?)
        }))
    }

    async fn job_kill(
        &mut self,
        authority: &ConnectionAuthority,
        params: JobRequest,
    ) -> Result<JobAnswer<RouterResponse>> {
        self.require_scoped_workspace(authority, &params.repo_id, &params.workspace)
            .await?;
        let ended = self
            .host
            .kill_job(
                params.workspace,
                params.workspace_incarnation,
                params.job_id,
            )
            .await?;
        Ok(Box::pin(async move {
            ended.await?;
            respond::<operations::JobKill>(&EmptyResult {})
        }))
    }

    fn require_repo(&self, repo_id: &RepoId) -> Result<()> {
        if repo_id != &self.host.descriptor().repo_id {
            return Err(CowshedError::conflict(
                "request repository does not match the project binding",
                "reopen the project and retry with its bound repository identity",
            ));
        }
        Ok(())
    }

    async fn require_scoped_workspace(
        &mut self,
        authority: &ConnectionAuthority,
        repo_id: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<()> {
        self.require_repo(repo_id)?;
        let snapshots = self.host.snapshots().await?;
        let snapshot = find_workspace(&snapshots, workspace)?;
        self.validate_worker_snapshot(authority, snapshot)
    }

    fn validate_worker_snapshot(
        &self,
        authority: &ConnectionAuthority,
        snapshot: &WorkspaceSnapshot,
    ) -> Result<()> {
        if let ConnectionAuthority::Worker {
            workspace,
            workspace_incarnation,
            ..
        } = authority
            && (workspace != &snapshot.info.workspace
                || workspace_incarnation != &snapshot.info.workspace_incarnation)
        {
            return Err(CowshedError::conflict(
                "workspace capability is stale or belongs to another workspace incarnation",
                "reacquire a worker handle from the coordinator",
            ));
        }
        Ok(())
    }
}

/// Remove an adopted project end to end: retire every session workspace, listed or never
/// published; wait for their images' reclamation; collect; delete the abandon bundles the
/// removals left (only under `abandon`, which is what authorized them); then restore main, which
/// unbinds the project. Every step is idempotent, so a refusal at any step leaves the rest for
/// the same call to finish — and a stale collection says so as [`crate::error::Retry`].
async fn remove_project(
    host: &mut dyn ProjectRuntimeHost,
    options: RemoveProjectOptions,
) -> Result<RemoveProjectReport> {
    let mut sessions: std::collections::BTreeSet<WorkspaceName> = host
        .snapshots()
        .await?
        .into_iter()
        .map(|snapshot| snapshot.info.workspace)
        .filter(|workspace| !workspace.is_main())
        .collect();
    sessions.extend(
        host.unpublished_workspaces()
            .await?
            .into_iter()
            .filter(|workspace| !workspace.is_main()),
    );
    let session_removal = RemoveOptions {
        force: options.force,
        restore: false,
        abandon: options.abandon,
    };
    let mut removed = Vec::with_capacity(sessions.len());
    for workspace in sessions {
        let report = host.remove(workspace.clone(), session_removal).await?;
        removed.push(RemovedWorkspace { workspace, report });
    }
    host.settle_reclaims().await?;
    let collected = host.gc(GcOptions::default()).await?;
    let deleted_bundles = if options.abandon {
        host.delete_abandon_bundles().await?
    } else {
        Vec::new()
    };
    let restored = host
        .remove(
            WorkspaceName::main(),
            RemoveOptions {
                force: false,
                restore: true,
                abandon: options.abandon,
            },
        )
        .await?;
    Ok(RemoveProjectReport {
        removed,
        collected,
        deleted_bundles,
        restored,
    })
}

fn require_coordinator(authority: &ConnectionAuthority) -> Result<()> {
    if matches!(authority, ConnectionAuthority::Coordinator { .. }) {
        Ok(())
    } else {
        Err(CowshedError::new(
            ErrorCode::SandboxDenied,
            "workspace capability cannot perform coordinator operation",
            "request coordinator authority from the controller owner",
        ))
    }
}

fn find_workspace<'a>(
    snapshots: &'a [WorkspaceSnapshot],
    workspace: &WorkspaceName,
) -> Result<&'a WorkspaceSnapshot> {
    snapshots
        .iter()
        .find(|snapshot| &snapshot.info.workspace == workspace)
        .ok_or_else(|| {
            CowshedError::not_found(
                format!("workspace {workspace} does not exist"),
                "list workspaces and retry with a published name",
            )
        })
}

/// The named workspace's snapshot, taken out of the listing it was found in.
fn take_workspace(
    snapshots: Vec<WorkspaceSnapshot>,
    workspace: &WorkspaceName,
) -> Result<WorkspaceSnapshot> {
    snapshots
        .into_iter()
        .find(|snapshot| &snapshot.info.workspace == workspace)
        .ok_or_else(|| {
            CowshedError::not_found(
                format!("workspace {workspace} does not exist"),
                "list workspaces and retry with a published name",
            )
        })
}

fn workspace_view(snapshot: WorkspaceSnapshot) -> WorkspaceView {
    WorkspaceView {
        info: snapshot.info,
        grants: snapshot.grants,
    }
}

/// Answers a call with its operation's declared result.
fn respond<O: Operation>(result: &O::Result) -> Result<RouterResponse> {
    encode_result::<O>(result).map(RouterResponse::json)
}

fn canonical_input_path(path: &str) -> Result<PathBuf> {
    let path = PathBuf::from(path);
    if !path.is_absolute()
        || path.components().any(|component| {
            matches!(
                component,
                std::path::Component::CurDir | std::path::Component::ParentDir
            )
        })
    {
        return Err(CowshedError::usage(
            "project path must be absolute and lexically normalized",
            "retry with the discovered git repository root",
        ));
    }
    Ok(path)
}

/// The one command an exec request carries, validated before anything is admitted.
fn exec_command(
    argv: Option<Vec<CommandArg>>,
    script: Option<crate::api::dto::ScriptCommand>,
) -> Result<crate::api::dto::ExecCommand> {
    let command = crate::api::dto::ExecCommand::from_fields(argv, script)
        .and_then(|command| command.validate().map(|()| command))
        .map_err(|error| {
            CowshedError::usage(error.to_string(), "provide a valid bounded command")
        })?;
    Ok(command)
}

/// The admission an exec request carries: its fence, its session, and the validated request,
/// with inline stdin taken from the call's upload frame.
fn exec_request(
    params: ExecParams,
    upload: Option<Bytes>,
) -> Result<(WorkerScope, Option<String>, ExecRequest)> {
    let command = exec_command(params.argv, params.script)?;
    let stdin = match params.stdin {
        ExecStdin::Empty => {
            if upload.is_some() {
                return Err(CowshedError::usage(
                    "empty stdin request unexpectedly included binary data",
                    "retry without an upload frame",
                ));
            }
            StdinSource::Empty
        }
        ExecStdin::Inline => StdinSource::Inline(upload.ok_or_else(|| {
            CowshedError::usage(
                "inline stdin request is missing binary data",
                "retry with the declared upload frame",
            )
        })?),
        ExecStdin::Stream => {
            if upload.is_some() {
                return Err(CowshedError::usage(
                    "stream stdin admission unexpectedly included binary data",
                    "send stream chunks after job admission",
                ));
            }
            return Err(CowshedError::usage(
                "stream stdin requires the controller streaming channel",
                "retry through WorkspaceHandle::exec",
            ));
        }
        ExecStdin::WorkspaceFile { workspace_path } => {
            if upload.is_some() {
                return Err(CowshedError::usage(
                    "workspace-file stdin unexpectedly included binary data",
                    "retry without an upload frame",
                ));
            }
            StdinSource::WorkspaceFile(workspace_path)
        }
    };
    let scope = WorkerScope {
        repo_id: params.repo_id,
        workspace: params.workspace,
        workspace_incarnation: params.workspace_incarnation,
    };
    Ok((
        scope,
        params.session,
        ExecRequest {
            command,
            cwd: params.cwd,
            mode: params.mode,
            env: params.env,
            trace: params.trace,
            stdin,
            stdout_copy: params.stdout_copy,
            stderr_copy: params.stderr_copy,
        },
    ))
}

#[cfg(target_os = "macos")]
type NativeSubstrate = crate::storage::apfs::ApfsSubstrate<
    crate::storage::apfs::native::MacOsApfsExecutionHost<crate::apfs::SystemCommandRunner>,
>;

#[cfg(target_os = "macos")]
struct NativeProjectRuntimeHost {
    descriptor: ProjectDescriptor,
    git: crate::git::GitRepository,
    layout: crate::storage::StorageLayout,
    substrate_config: crate::storage::apfs::ApfsSubstrateConfig,
    substrate: NativeSubstrate,
    commitments: super::supervisor::CommitmentPublisherHandle,
    /// The handle each workspace's commands go through: to the supervisor serving the
    /// workspace's socket, whichever process serves it.
    supervisors:
        std::collections::BTreeMap<WorkspaceName, super::supervisor::WorkspaceSupervisorHandle>,
    /// The supervisors this process serves: only in a `cowshed __workspace-supervisor` process.
    served: std::collections::BTreeMap<WorkspaceName, ServedSupervisor>,
    /// Who runs the workspace supervisors this host's commands reach.
    supervisors_run_in: SupervisorHome,
    /// With a sink of the host's own: per supervisor socket, the task forwarding that
    /// supervisor's commitments into it. `None` when the host's default sink already has them.
    forwarders: Option<std::collections::BTreeMap<PathBuf, tokio::task::JoinHandle<()>>>,
    sessions: std::collections::BTreeMap<
        (WorkspaceName, Option<String>),
        super::supervisor::SessionToken,
    >,
    home: PathBuf,
    telemetry_root: PathBuf,
    lifecycle_intents_path: PathBuf,
    lifecycle_intents: crate::storage::recovery::LifecycleIntentJournal,
    /// The workspaces whose lifecycle operation this process is executing in the current verb.
    /// Released when the verb returns, so an unfinished intent it leaves behind becomes an
    /// ordinary crash residue for the next open instead of staying owned by an idle process.
    intent_leases: std::collections::BTreeMap<WorkspaceName, crate::storage::recovery::IntentLease>,
    /// How this host reconciles the binding's remotes, both at recovery and per verb. A host
    /// opened for the identity change skips the pairing that verb exists to supersede; every
    /// other host heals transport moves exactly like a fresh open would.
    binding_remote_validation: BindingRemoteValidation,
    /// Which unfinished lifecycle intents this opening replays; see [`RecoveryScope`].
    recovery_scope: RecoveryScope,
    /// The image reclamations this host started in the background and has not yet seen finish:
    /// what `settle_reclaims` waits for. Finished ones are dropped as new ones start.
    reclaims: Vec<JoinHandle<()>>,
}

/// Who runs a workspace's supervisor.
#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SupervisorHome {
    /// The gateway daemon's manager starts it as a process of its own: every controller.
    Daemon,
    /// This process: the `cowshed __workspace-supervisor` process the manager started.
    ThisProcess,
}

/// How long a lost supervisor's jobs get between TERM and KILL.
#[cfg(target_os = "macos")]
const LOST_JOB_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

/// How long a served supervisor stays idle (no named session, no running job, and its keeper
/// holds no Nx daemon) before it retires; the next command starts another.
#[cfg(target_os = "macos")]
const SUPERVISOR_IDLE: std::time::Duration = std::time::Duration::from_secs(30 * 60);

/// What the supervisor process's serving loop woke for.
#[cfg(target_os = "macos")]
enum ServeEvent {
    Ended(std::result::Result<Result<()>, tokio::task::JoinError>),
    Advance(tokio::sync::oneshot::Sender<Result<super::supervisor::WorkspaceAuthoritySnapshot>>),
    Tick(
        tokio::time::Instant,
        super::supervisor::WorkspaceSupervisorHandle,
    ),
}

/// A supervisor this process runs and serves on the workspace's socket.
#[cfg(target_os = "macos")]
struct ServedSupervisor {
    /// The actor itself, for what only its owner may do: advance its grant revision in place.
    actor: super::supervisor::WorkspaceSupervisorHandle,
    socket: PathBuf,
    /// Ends once a `retire` call retired the actor and was answered.
    server: tokio::task::JoinHandle<Result<()>>,
    /// `advance` requests the server received, for this process to answer.
    advance_requests: tokio::sync::mpsc::Receiver<
        tokio::sync::oneshot::Sender<Result<super::supervisor::WorkspaceAuthoritySnapshot>>,
    >,
}

/// A controller that stops serving a supervisor ends it, as dropping an in-process supervisor
/// always did: the server lets go of its handle, and the actor ends the jobs it still runs.
#[cfg(target_os = "macos")]
impl Drop for ServedSupervisor {
    fn drop(&mut self) {
        self.server.abort();
    }
}

#[cfg(target_os = "macos")]
struct PortGrantReservation {
    grants: GrantSet,
    markers: Vec<PathBuf>,
    listeners: Vec<std::net::TcpListener>,
}

#[cfg(target_os = "macos")]
impl Drop for PortGrantReservation {
    fn drop(&mut self) {
        self.listeners.clear();
        for marker in &self.markers {
            if let Err(error) = std::fs::remove_file(marker) {
                eprintln!(
                    "cowshed: cannot release port reservation {}: {error}",
                    marker.display()
                );
            }
        }
    }
}

/// Whether the process owning a reservation marker has not exited; a zombie owner has, and its
/// cell is reclaimed. An owner whose state cannot be read keeps its cell.
#[cfg(target_os = "macos")]
fn process_is_alive(pid: u32) -> bool {
    let Ok(pid) = i32::try_from(pid) else {
        return false;
    };
    crate::process::running(pid).unwrap_or(true)
}

#[cfg(target_os = "macos")]
fn claim_port_block(staging: &Path, base: u16) -> std::io::Result<Option<PathBuf>> {
    claim_port_block_with(staging, base, |marker| std::fs::read_link(marker))
}

/// Claims `base`'s grid cell with a symlink naming this process, reclaiming a dead owner's.
///
/// Every creator tries the lowest free cell first, so a contended cell's marker comes and goes
/// between our `symlink` and our look at it: its owner released it (or another claimant reclaimed
/// a dead one) in that gap. A marker that is gone when read or removed is a released cell, never an
/// error, and the claim is tried again. `read_owner` reads the marker's link; tests interpose there
/// to release the cell inside that gap.
#[cfg(target_os = "macos")]
fn claim_port_block_with(
    staging: &Path,
    base: u16,
    mut read_owner: impl FnMut(&Path) -> std::io::Result<PathBuf>,
) -> std::io::Result<Option<PathBuf>> {
    use std::os::unix::fs::symlink;

    let released = |error: &std::io::Error| error.kind() == std::io::ErrorKind::NotFound;
    std::fs::create_dir_all(staging)?;
    let marker = staging.join(format!("port-{base}.reservation"));
    let owner = std::process::id().to_string();
    for _ in 0..3 {
        match symlink(&owner, &marker) {
            Ok(()) => return Ok(Some(marker)),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                let existing = match read_owner(&marker) {
                    Ok(existing) => existing,
                    Err(error) if released(&error) => continue,
                    Err(error) => return Err(error),
                };
                let existing = existing
                    .to_str()
                    .and_then(|value| value.parse::<u32>().ok())
                    .ok_or_else(|| {
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            format!("invalid port reservation marker {}", marker.display()),
                        )
                    })?;
                if process_is_alive(existing) {
                    return Ok(None);
                }
                match std::fs::remove_file(&marker) {
                    Ok(()) => {}
                    Err(error) if released(&error) => {}
                    Err(error) => return Err(error),
                }
            }
            Err(error) => return Err(error),
        }
    }
    Ok(None)
}

#[cfg(target_os = "macos")]
fn bind_port_block(
    block: crate::metadata::PortBlock,
    existing: Option<&GrantSet>,
) -> std::io::Result<Option<Vec<std::net::TcpListener>>> {
    let mut listeners = Vec::with_capacity(usize::from(block.size()));
    for port in block.base()..block.base() + block.size() {
        if existing.is_some_and(|grants| {
            grants
                .port_blocks()
                .any(|owned| port >= owned.base() && port - owned.base() < owned.size())
        }) {
            continue;
        }
        match std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)) {
            Ok(listener) => listeners.push(listener),
            Err(error) if error.kind() == std::io::ErrorKind::AddrInUse => return Ok(None),
            Err(error) => return Err(error),
        }
    }
    Ok(Some(listeners))
}

#[cfg(target_os = "macos")]
async fn reserve_specific_port_grants(
    inventory: &crate::gateway_inventory::NativeGatewayInventory,
    reservation_root: &Path,
    block: crate::metadata::PortBlock,
) -> Result<Option<PortGrantReservation>> {
    reserve_port_grant_replacement(inventory, reservation_root, block, None).await
}

#[cfg(target_os = "macos")]
async fn reserve_port_grant_replacement(
    inventory: &crate::gateway_inventory::NativeGatewayInventory,
    reservation_root: &Path,
    block: crate::metadata::PortBlock,
    existing: Option<&GrantSet>,
) -> Result<Option<PortGrantReservation>> {
    let grants = GrantSet::closed_baseline(Some(block)).map_err(native_integrity_error)?;
    let mut reservation = PortGrantReservation {
        grants,
        markers: Vec::with_capacity(
            usize::from(block.size()).div_ceil(usize::from(crate::metadata::NEW_PORT_BLOCK_SIZE)),
        ),
        listeners: Vec::new(),
    };
    // Own every covered initial-size grid cell before publication: a smaller allocator
    // cannot claim a block in the middle of a larger allocation.
    let claim_base = block.base() - block.base() % crate::metadata::NEW_PORT_BLOCK_SIZE;
    for base in (claim_base..block.base() + block.size())
        .step_by(usize::from(crate::metadata::NEW_PORT_BLOCK_SIZE))
    {
        let Some(marker) = claim_port_block(reservation_root, base).map_err(|error| {
            CowshedError::internal(format!(
                "claim macOS port block {base} at {}: {error}",
                reservation_root.display()
            ))
        })?
        else {
            return Ok(None);
        };
        reservation.markers.push(marker);
    }
    reservation.listeners = match bind_port_block(block, existing).map_err(|error| {
        CowshedError::environment_missing(
            format!("cannot bind macOS port block {block}: {error}"),
            "check the host's local port availability",
        )
    })? {
        Some(listeners) => listeners,
        None => return Ok(None),
    };
    // Claims and kernel listeners stay held across the authoritative re-read.
    // Only blocks already owned by this same workspace may overlap.
    let used = inventory
        .all_reserved_port_blocks()
        .await
        .map_err(native_integrity_error)?;
    Ok((!used.blocks().any(|held| {
        held.overlaps(block)
            && !existing.is_some_and(|grants| grants.port_blocks().any(|owned| owned == held))
    }))
    .then_some(reservation))
}

/// Claims the lowest initial-size block disjoint from every published block. All allocation
/// sizes claim the same grid cells, so unpublished reservations exclude overlapping creators.
#[cfg(target_os = "macos")]
async fn reserve_port_grants(
    inventory: &crate::gateway_inventory::NativeGatewayInventory,
    reservation_root: &Path,
    used: crate::metadata::ReservedPortBlocks,
) -> Result<PortGrantReservation> {
    for block in crate::metadata::PortBlock::macos_candidates() {
        if used.overlapping(block).is_some() {
            continue;
        }
        if let Some(reservation) =
            reserve_specific_port_grants(inventory, reservation_root, block).await?
        {
            return Ok(reservation);
        }
    }
    Err(CowshedError::conflict(
        "no macOS workspace port block remains",
        "remove an unused workspace",
    ))
}

#[cfg(target_os = "macos")]
fn retain_port_authority(grants: &mut GrantSet, block: crate::metadata::PortBlock) {
    grants
        .retained_port_blocks
        .retain(|owned| !block.overlaps(*owned));
    if let Some(previous) = grants.port_block
        && !block.overlaps(previous)
    {
        grants.retained_port_blocks.push(previous);
    }
    grants
        .retained_port_blocks
        .sort_unstable_by_key(|owned| owned.base());
    grants.port_block = Some(block);
}

#[cfg(target_os = "macos")]
async fn reserve_grown_port_grants(
    inventory: &crate::gateway_inventory::NativeGatewayInventory,
    reservation_root: &Path,
    owned: &GrantSet,
    size: u16,
) -> Result<PortGrantReservation> {
    let existing = owned.port_block.ok_or_else(|| {
        CowshedError::integrity("workspace has no port block", "cowshed doctor --json")
    })?;
    let used = inventory
        .all_reserved_port_blocks()
        .await
        .map_err(native_integrity_error)?;
    let overlaps_sibling = |block| {
        used.blocks()
            .any(|held| held.overlaps(block) && !owned.port_blocks().any(|own| own == held))
    };
    let containing_base = existing.base() - existing.base() % size;
    if let Ok(block) = crate::metadata::PortBlock::new(containing_base, size)
        && cowshed_gateway_types::is_macos_port_block(block.base(), block.size())
        && !overlaps_sibling(block)
        && let Some(reservation) =
            reserve_port_grant_replacement(inventory, reservation_root, block, Some(owned)).await?
    {
        return Ok(reservation);
    }
    for block in crate::metadata::PortBlock::macos_candidates_with_size(size)
        .map_err(native_integrity_error)?
    {
        if block.base() == containing_base || overlaps_sibling(block) {
            continue;
        }
        if let Some(reservation) =
            reserve_port_grant_replacement(inventory, reservation_root, block, Some(owned)).await?
        {
            return Ok(reservation);
        }
    }
    Err(CowshedError::conflict(
        format!(
            "no disjoint macOS workspace port block with {} service ports remains; this workspace owns {} ports in its current block and {} retained ports in {} blocks",
            size - 1,
            existing.size(),
            owned
                .retained_port_blocks
                .iter()
                .map(|block| u32::from(block.size()))
                .sum::<u32>(),
            owned.retained_port_blocks.len(),
        ),
        "remove an unused workspace to release its current and retained ports, then request capacity again",
    ))
}

/// One entry an unbinding may delete from terminal project storage.
#[cfg(target_os = "macos")]
enum TerminalEntry {
    Lock(PathBuf),
    Directory(PathBuf),
}

/// Plans the removal of a terminal storage tree, children before their directory: only empty
/// directories and the zero-length locks retired workspaces leave behind qualify. Anything else is
/// refused before a single entry is removed.
#[cfg(target_os = "macos")]
fn plan_terminal_storage_tree(path: &Path, plan: &mut Vec<TerminalEntry>) -> Result<()> {
    let metadata = match std::fs::symlink_metadata(path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(CowshedError::environment_missing(
                format!(
                    "cannot inspect terminal project storage {}: {error}",
                    path.display()
                ),
                "check controller storage permissions and retry",
            ));
        }
    };
    if metadata.file_type().is_dir() {
        let entries = std::fs::read_dir(path).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot enumerate terminal project storage {}: {error}",
                    path.display()
                ),
                "check controller storage permissions and retry",
            )
        })?;
        for entry in entries {
            let entry = entry.map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot read terminal project storage {}: {error}",
                        path.display()
                    ),
                    "check controller storage permissions and retry",
                )
            })?;
            plan_terminal_storage_tree(&entry.path(), plan)?;
        }
        plan.push(TerminalEntry::Directory(path.to_owned()));
        Ok(())
    } else if metadata.file_type().is_file()
        && metadata.len() == 0
        && path
            .file_name()
            .and_then(|name| name.to_str())
            .is_some_and(|name| name.ends_with(".lock"))
    {
        plan.push(TerminalEntry::Lock(path.to_owned()));
        Ok(())
    } else {
        Err(CowshedError::integrity(
            format!(
                "terminal project storage contains an unexpected retained artifact: {}",
                path.display()
            ),
            "run cowshed doctor --json before removing the project binding",
        ))
    }
}

#[cfg(target_os = "macos")]
fn remove_terminal_storage_tree(path: &Path) -> Result<()> {
    let mut plan = Vec::new();
    plan_terminal_storage_tree(path, &mut plan)?;
    for entry in plan {
        let (removed, path) = match &entry {
            TerminalEntry::Lock(path) => (std::fs::remove_file(path), path),
            TerminalEntry::Directory(path) => (std::fs::remove_dir(path), path),
        };
        removed.map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot remove terminal project storage {}: {error}",
                    path.display()
                ),
                "check controller storage permissions and retry",
            )
        })?;
    }
    Ok(())
}

/// The project store trees an unbinding deletes once main is retired; they may hold nothing but
/// empty directories and zero-length locks by then.
#[cfg(target_os = "macos")]
const TERMINAL_STORAGE_TREES: [&str; 4] = [
    crate::storage::recovery::STAGING_NAMESPACE,
    crate::repository::CHECKPOINTS_DIRECTORY,
    crate::repository::EXEC_TEMP_DIRECTORY,
    crate::repository::SESSIONS_DIRECTORY,
];

/// Refuses a main restore whose terminal storage the unbinding could not delete: a live
/// workspace's image, grants or temp dir, an unreclaimed retired image, a staged image, another
/// workspace's checkpoint, or an `rm --abandon` bundle kept for its owner. Main's own checkpoints
/// and temp dir are exempt: main's retirement reclaims them with its image. The restore checks
/// this before it retires main, because the same refusal after it leaves a binding no command can
/// remove.
#[cfg(target_os = "macos")]
fn require_terminal_storage(project_root: &Path) -> Result<()> {
    let mut plan = Vec::new();
    let planned = TERMINAL_STORAGE_TREES.into_iter().try_for_each(|name| {
        let tree = project_root.join(name);
        if name != crate::repository::CHECKPOINTS_DIRECTORY
            && name != crate::repository::EXEC_TEMP_DIRECTORY
        {
            return plan_terminal_storage_tree(&tree, &mut plan);
        }
        let entries = match std::fs::read_dir(&tree) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(error) => {
                return Err(CowshedError::environment_missing(
                    format!(
                        "cannot enumerate terminal project storage {}: {error}",
                        tree.display()
                    ),
                    "check controller storage permissions and retry",
                ));
            }
        };
        for entry in entries {
            let entry = entry.map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot read terminal project storage {}: {error}",
                        tree.display()
                    ),
                    "check controller storage permissions and retry",
                )
            })?;
            if entry.file_name() != main_name().as_str() {
                plan_terminal_storage_tree(&entry.path(), &mut plan)?;
            }
        }
        Ok(())
    });
    planned.map_err(|error| match error.code {
        ErrorCode::Integrity => CowshedError::conflict(
            format!(
                "main cannot be restored while the project's storage holds more than locks: {}",
                error.message
            ),
            "remove every session workspace (cowshed rm <ws>), delete the rm --abandon bundles in \
             the project's sessions/.trash you no longer need, run cowshed gc, then retry",
        ),
        _ => error,
    })
}

#[cfg(target_os = "macos")]
fn clean_terminal_project_storage(project_root: &Path, binding: &Path) -> Result<()> {
    for name in TERMINAL_STORAGE_TREES {
        remove_terminal_storage_tree(&project_root.join(name))?;
    }
    for entry in std::fs::read_dir(project_root).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "cannot enumerate terminal project root {}: {error}",
                project_root.display()
            ),
            "check controller storage permissions and retry",
        )
    })? {
        let path = entry
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot read terminal project root {}: {error}",
                        project_root.display()
                    ),
                    "check controller storage permissions and retry",
                )
            })?
            .path();
        if path != binding
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with(".lock"))
        {
            remove_terminal_storage_tree(&path)?;
        }
    }
    Ok(())
}

/// Everything the controller keeps for a project it is unbinding. Nothing reopens an unbound
/// project, so its lifecycle journal, deletion log and slot bindings go, and with them its mount
/// tree and each owner directory they leave empty; the store directory goes once
/// the binding and the checkout root record after it do. What the user put in the store —
/// `policy.json`, `waivers.json`, `quarantine/` — stays, and keeps the store directory with it.
#[cfg(target_os = "macos")]
fn remove_unbound_project_state(paths: &crate::repository::ProjectPaths) -> Result<()> {
    for file in [
        paths
            .project_root
            .join(crate::storage::recovery::LIFECYCLE_INTENTS_FILE),
        paths
            .project_root
            .join(crate::storage::deletion_log::DELETION_LOG_FILE),
        paths.slot_bindings.clone(),
    ] {
        remove_unbound_file(&file)?;
    }
    remove_directory_if_empty(&paths.project_root)?;
    if let Some(owner) = paths.project_root.parent() {
        remove_directory_if_empty(owner)?;
    }
    remove_empty_mount_tree(&paths.mount_root)?;
    if let Some(owner) = paths.mount_root.parent() {
        remove_directory_if_empty(owner)?;
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn remove_unbound_file(file: &Path) -> Result<()> {
    match std::fs::remove_file(file) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(CowshedError::environment_missing(
            format!(
                "cannot remove unbound project state {}: {error}",
                file.display()
            ),
            "check controller storage permissions and retry",
        )),
    }
}

#[cfg(target_os = "macos")]
fn remove_directory_if_empty(directory: &Path) -> Result<()> {
    match std::fs::remove_dir(directory) {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.kind(),
                std::io::ErrorKind::NotFound | std::io::ErrorKind::DirectoryNotEmpty
            ) =>
        {
            Ok(())
        }
        Err(error) => Err(CowshedError::environment_missing(
            format!(
                "cannot remove unbound project directory {}: {error}",
                directory.display()
            ),
            "check controller storage permissions and retry",
        )),
    }
}

/// An unbound project's mount tree holds only the mountpoints of retired workspaces and
/// `.staging`: empty directories on the volume the tree lives on. The whole tree is planned
/// first; a file, or a directory on another device (a volume still mounted there), is reported and
/// nothing of the tree is removed.
#[cfg(target_os = "macos")]
fn remove_empty_mount_tree(root: &Path) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let device = match std::fs::symlink_metadata(root) {
        Ok(metadata) => metadata.dev(),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(error) => {
            return Err(CowshedError::environment_missing(
                format!(
                    "cannot inspect unbound project mount tree {}: {error}",
                    root.display()
                ),
                "check mount root permissions and retry",
            ));
        }
    };
    let mut plan = Vec::new();
    plan_empty_directories_on(root, device, &mut plan)?;
    for directory in plan {
        std::fs::remove_dir(&directory).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot remove unbound project mount tree {}: {error}",
                    directory.display()
                ),
                "check mount root permissions and retry",
            )
        })?;
    }
    Ok(())
}

/// Plans the removal of `path`, children before their directory, if it and everything below it are
/// directories on `device`.
#[cfg(target_os = "macos")]
fn plan_empty_directories_on(path: &Path, device: u64, plan: &mut Vec<PathBuf>) -> Result<()> {
    use std::os::unix::fs::MetadataExt;

    let inspect = |error: std::io::Error| {
        CowshedError::environment_missing(
            format!(
                "cannot inspect unbound project mount tree {}: {error}",
                path.display()
            ),
            "check mount root permissions and retry",
        )
    };
    let metadata = std::fs::symlink_metadata(path).map_err(inspect)?;
    if !metadata.file_type().is_dir() || metadata.dev() != device {
        return Err(CowshedError::integrity(
            format!(
                "the project stays bound: its mount tree still holds {}",
                path.display()
            ),
            "move that entry aside, then retry cowshed rm main --restore",
        ));
    }
    for entry in std::fs::read_dir(path).map_err(inspect)? {
        plan_empty_directories_on(&entry.map_err(inspect)?.path(), device, plan)?;
    }
    plan.push(path.to_owned());
    Ok(())
}

#[cfg(any(test, target_os = "macos"))]
fn verified_recovery_facts<'a>(
    facts: &'a [crate::storage::lifecycle::StorageFact],
    pending: &[crate::storage::apfs::PendingPublicationFact],
) -> Vec<&'a crate::storage::lifecycle::StorageFact> {
    let pending_destinations = pending
        .iter()
        .map(|fact| fact.destination_incarnation.clone())
        .collect::<std::collections::BTreeSet<_>>();
    facts
        .iter()
        .filter(|fact| !pending_destinations.contains(fact.workspace.incarnation()))
        .collect()
}

#[cfg(target_os = "macos")]
impl NativeProjectRuntimeHost {
    async fn open(
        project_root: &Path,
        bootstrap_mode: crate::storage::bootstrap::native::NativeBootstrapMode,
        requested_repo_id: Option<&RepoId>,
        continuity: crate::storage::audit::ContinuityAudit,
        validation: BindingRemoteValidation,
        recovery_scope: RecoveryScope,
        supplied_storage: Option<crate::storage::bootstrap::ValidatedHostStorage>,
    ) -> Result<Self> {
        use crate::storage::apfs::ApfsExecutionHost;
        use crate::timing::spanned;

        let git = spanned("open", "git-discover", async {
            let git = crate::git::GitRepository::discover(project_root).await?;
            git.ensure_adoptable().await?;
            Ok::<_, CowshedError>(git)
        })
        .await?;
        let git_root = git.root().to_path_buf();
        let supervisors_run_in = if supplied_storage.is_some() {
            SupervisorHome::ThisProcess
        } else {
            SupervisorHome::Daemon
        };
        let storage = match supplied_storage {
            Some(storage) => storage,
            None => {
                let home = std::env::var_os("HOME")
                    .map(PathBuf::from)
                    .filter(|path| path.is_absolute())
                    .ok_or_else(|| {
                        CowshedError::environment_missing(
                            "HOME is missing or is not absolute",
                            "launch the controller with a canonical HOME",
                        )
                    })?;
                let bootstrap = spanned(
                    "open",
                    "bootstrap",
                    crate::storage::bootstrap::native::bootstrap_system_storage(
                        &git_root,
                        &home,
                        bootstrap_mode,
                    ),
                )
                .await
                .map_err(native_environment_error)?;
                if !matches!(
                    bootstrap.substrate(),
                    crate::storage::bootstrap::SelectedSubstrate::Apfs { .. }
                ) {
                    return Err(CowshedError::environment_missing(
                        "the macOS runtime requires the APFS image substrate",
                        "remove the unsupported substrate override and retry",
                    ));
                }
                crate::storage::bootstrap::ValidatedHostStorage::new(
                    home,
                    bootstrap.roots().clone(),
                )
            }
        };
        let home = storage.home().to_path_buf();
        let existing_only = matches!(
            &bootstrap_mode,
            crate::storage::bootstrap::native::NativeBootstrapMode::ExistingOnly
        );
        // An inspecting open reads the store as it stands: doctor names an unfinished identity
        // change rather than finishing it.
        if recovery_scope.repairs() {
            spanned(
                "open",
                "identity-intent",
                recover_repository_identity_intent(storage.store()),
            )
            .await?;
        }
        let origin_span = crate::timing::span("open", "origin");
        let origin = if existing_only {
            workspace_origin_from_marker(&git_root).await?
        } else {
            None
        };
        // A session marker identifies the project from store authority. Its recorded checkout may
        // be a missing direct mount, so resolving that identity must happen before replacing the
        // invocation repository handle with one rooted at the recorded checkout.
        let session_project = if existing_only {
            project_binding_from_workspace_origin(storage.store(), &git_root, origin.as_ref())
                .await?
        } else {
            None
        };
        drop(origin_span);
        let mut binding_repo_id = if existing_only {
            origin.as_ref().map(|origin| origin.repo_id.clone())
        } else {
            requested_repo_id.cloned()
        };
        // Where the caller stands and where the project is checked out are two different facts,
        // and only the first is what Git discovery reports. A coordinator verb invoked from inside
        // a session workspace discovers that workspace's mount — a standalone repository in its own
        // right — while everything downstream (the binding's remotes, the substrate's checkout
        // path, the recorded-project-root agreement every enumeration checks) means the project's
        // checkout. Every marker records that checkout, so when the two differ the marker is the
        // authority and the invocation root is left behind here.
        let (git_root, git) = match origin.as_ref() {
            Some(origin) if !names_one_root(&origin.project_root, &git_root) => {
                let root = origin.project_root.clone();
                let git = crate::git::GitRepository::from_root(&root);
                (root, git)
            }
            _ => (git_root, git),
        };
        if existing_only && binding_repo_id.is_none() {
            let inventory_storage = storage.clone();
            binding_repo_id = spanned(
                "open",
                "repository-for-root",
                crate::gateway_inventory::NativeGatewayInventory::new(inventory_storage)
                    .repository_for_project_root(&git_root),
            )
            .await
            .map_err(native_integrity_error)?;
        }
        let binding_span = crate::timing::span("open", "binding");
        let (repo_id, layout, binding) = if let Some(resolved) = session_project {
            resolved
        } else {
            let recorded = match binding_repo_id.as_ref() {
                Some(repo_id) => project_owning_repo_id(storage.store(), repo_id).await?,
                None => None,
            };
            match recorded {
                // The store already records what this project is called, so that record decides.
                // The remotes are still checked, but only against the remote each identity itself
                // recorded — an identity carrying none (which is what an in-place rename leaves)
                // has nothing for Git to contradict.
                Some((layout, binding)) => {
                    let repo_id = binding
                        .primary()
                        .map_err(native_integrity_error)?
                        .repo_id
                        .clone();
                    let remotes = git.remotes().await?;
                    // A transport move heals here, at the one place the binding is loaded, so
                    // everything downstream — the descriptor, the gateway inventory — reads the
                    // URL Git actually uses. An identity move refuses with the rebind verb, and
                    // that verb reaches this arm under `ForIdentityChange` without tripping it.
                    // An inspecting open keeps the recorded binding, and doctor reports the move.
                    let binding = match reconcile_binding_with_remotes(
                        &binding,
                        &remotes,
                        validation,
                        git.root(),
                    )? {
                        Some(updated) if recovery_scope.repairs() => {
                            persist_binding(&layout, &updated).await?;
                            updated
                        }
                        Some(_) | None => binding,
                    };
                    (repo_id, layout, binding)
                }
                // Nothing adopted under this identity: the remotes are the only source left, and
                // this is the adoption path that derives an identity from them for the first time.
                None => {
                    let candidate = binding_from_git(&git, binding_repo_id.as_ref()).await?;
                    let repo_id = candidate
                        .primary()
                        .map_err(native_integrity_error)?
                        .repo_id
                        .clone();
                    let layout = crate::storage::StorageLayout::new(storage.store(), &repo_id)
                        .map_err(native_integrity_error)?;
                    let binding = load_or_validate_binding(&layout, candidate, &git).await?;
                    (repo_id, layout, binding)
                }
            }
        };
        drop(binding_span);
        if !existing_only {
            let provision_layout = layout.clone();
            let project_root = layout.project().project_root.clone();
            crate::storage::lifecycle::dispatch_blocking(move || {
                provision_layout.provision_project()
            })
            .await
            .map_err(|error| {
                CowshedError::internal(format!("project storage provisioning task failed: {error}"))
            })?
            .map_err(|error| {
                CowshedError::storage_failure(
                    format!(
                        "cannot provision project storage {}: {error}",
                        project_root.display()
                    ),
                    &error,
                    "repair cowshed storage and retry adoption",
                )
            })?;
        }
        let config = crate::storage::apfs::ApfsSubstrateConfig::new(storage.store(), &git_root);
        let host = crate::storage::apfs::native::MacOsApfsExecutionHost::new(
            crate::apfs::SystemCommandRunner,
            config.clone(),
        )
        .map_err(native_storage_error)?;
        let lifecycle_intents_path = layout
            .project()
            .project_root
            .join(crate::storage::recovery::LIFECYCLE_INTENTS_FILE);
        let recovery_intents_path = lifecycle_intents_path.clone();
        let recovery_config = config.clone();
        let recovery_repo = repo_id.clone();
        let recovery_span = crate::timing::span("open", "inventory");
        let repairs = recovery_scope.repairs();
        let (host, facts, pending, lifecycle_intents) =
            crate::storage::lifecycle::dispatch_blocking(move || {
                let lifecycle_intents =
                    crate::storage::recovery::LifecycleIntentJournal::load(&recovery_intents_path)?;
                // Store-wide completion of interrupted publications; an inspecting open reads the
                // store as it stands, and doctor names what this would have completed.
                if repairs {
                    host.recover_pending(&recovery_config, &[])
                        .map_err(native_storage_error)?;
                }
                let facts = host.list(&recovery_repo).map_err(native_storage_error)?;
                let pending = host
                    .pending_publications(&recovery_repo)
                    .map_err(native_storage_error)?;
                Ok::<_, CowshedError>((host, facts, pending, lifecycle_intents))
            })
            .await
            .map_err(|error| {
                CowshedError::internal(format!("APFS recovery task failed: {error}"))
            })??;
        drop(recovery_span);
        let retired_project_root = layout.project().project_root.clone();
        let retired_repo = repo_id.clone();
        let retired = spanned(
            "open",
            "retired",
            crate::storage::lifecycle::dispatch_blocking(move || {
                native_retired_refs(&retired_project_root, &retired_repo)
            }),
        )
        .await
        .map_err(|error| {
            CowshedError::internal(format!("retired workspace recovery task failed: {error}"))
        })??;
        let verified_facts = verified_recovery_facts(&facts, &pending);
        if let Some(origin) = origin.as_ref() {
            validate_workspace_origin_against_inventory(
                origin,
                &binding.owned_repo_ids().map_err(native_integrity_error)?,
                &verified_facts,
            )?;
        }
        // Authority is the inventory itself: an incarnation that is both an active storage fact
        // and a retired (trashed) one is a host-side integrity fault, found here in one pass —
        // no log replay has anything to add to what the images say.
        {
            let retired_incarnations = retired
                .recorded
                .iter()
                .map(|fact| fact.workspace().incarnation())
                .collect::<std::collections::BTreeSet<_>>();
            if let Some(conflict) = verified_facts
                .iter()
                .find(|fact| retired_incarnations.contains(fact.workspace.incarnation()))
            {
                return Err(CowshedError::integrity(
                    format!(
                        "active storage fact references a retired workspace incarnation {}",
                        conflict.workspace.incarnation()
                    ),
                    "cowshed doctor --json",
                ));
            }
        }
        let telemetry_root = storage.telemetry().to_path_buf();
        // A sink of the host's own takes the workspaces' commitments too: the supervisors that
        // record them are processes of their own, so this controller forwards them.
        let forwards_commitments = matches!(
            continuity,
            crate::storage::audit::ContinuityAudit::External(_)
        );
        let mut commitments = {
            let _span = crate::timing::span("open", "commitments");
            super::supervisor::CommitmentPublisher::open(
                &telemetry_root,
                continuity,
                ROUTER_CAPACITY,
            )?
        };
        let substrate = finish_store_residue(
            host,
            config.clone(),
            &repo_id,
            &pending,
            retired,
            &mut commitments,
            &recovery_scope,
        )
        .await?;
        let descriptor = ProjectDescriptor {
            repo_id,
            binding: std::sync::Arc::new(binding),
            git_root: std::sync::Arc::from(git_root),
            storage,
        };
        Ok(Self {
            descriptor,
            git,
            layout,
            substrate_config: config,
            substrate,
            commitments,
            supervisors: std::collections::BTreeMap::new(),
            served: std::collections::BTreeMap::new(),
            supervisors_run_in,
            forwarders: if forwards_commitments {
                Some(std::collections::BTreeMap::new())
            } else {
                None
            },
            sessions: std::collections::BTreeMap::new(),
            home,
            telemetry_root,
            lifecycle_intents_path,
            lifecycle_intents,
            intent_leases: std::collections::BTreeMap::new(),
            binding_remote_validation: validation,
            recovery_scope,
            reclaims: Vec::new(),
        })
    }
    /// Apply `change` to the journal on disk under its lock and adopt the result, which also
    /// carries every record other processes wrote since this host last read it.
    async fn update_lifecycle_intents<T: Send + 'static>(
        &mut self,
        change: impl FnOnce(&mut crate::storage::recovery::LifecycleIntentJournal) -> Result<T>
        + Send
        + 'static,
    ) -> Result<T> {
        let path = self.lifecycle_intents_path.clone();
        let (journal, value) = crate::storage::lifecycle::dispatch_blocking(move || {
            crate::storage::recovery::LifecycleIntentJournal::update(&path, change)
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("lifecycle intent persistence task failed: {error}"))
        })??;
        self.lifecycle_intents = journal;
        Ok(value)
    }

    async fn reload_lifecycle_intents(&mut self) -> Result<()> {
        let path = self.lifecycle_intents_path.clone();
        self.lifecycle_intents = crate::storage::lifecycle::dispatch_blocking(move || {
            crate::storage::recovery::LifecycleIntentJournal::load(&path)
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("lifecycle intent read task failed: {error}"))
        })??;
        Ok(())
    }

    /// Hold `workspace`'s intent lease for the rest of the current verb. `false` when another
    /// live process holds it: that process is executing a lifecycle operation on `workspace`.
    fn claim_intent_lease(&mut self, workspace: &WorkspaceName) -> Result<bool> {
        if self.intent_leases.contains_key(workspace) {
            return Ok(true);
        }
        match crate::storage::recovery::IntentLease::try_claim(
            &self.layout.project().sessions,
            workspace,
        )? {
            Some(lease) => {
                self.intent_leases.insert(workspace.clone(), lease);
                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Journals `operation` under this process's intent lease and returns the record it
    /// superseded, for a create or fork refused before its first mutation to put back.
    async fn begin_lifecycle_intent(
        &mut self,
        operation: crate::storage::recovery::LifecycleIntent,
    ) -> Result<Option<crate::storage::recovery::LifecycleIntentRecord>> {
        let target = operation.target().clone();
        if !self.claim_intent_lease(&target)? {
            return Err(another_process_is_running(&target));
        }
        self.update_lifecycle_intents(move |journal| Ok(journal.begin(operation)))
            .await
    }
    async fn mark_lifecycle_intent_mutating(&mut self, workspace: &WorkspaceName) -> Result<()> {
        let workspace = workspace.clone();
        self.update_lifecycle_intents(move |journal| journal.mark_mutating(&workspace))
            .await
    }
    /// Records the publication-fence sub-steps as complete after staged execution
    /// succeeded. Staged success implies the sidecar, companion, and image are all
    /// durable (prepare wrote them; commit activated them), so marking all three is a
    /// statement of observed fact, not optimism.
    ///
    /// Evidence only and best-effort: the fence already succeeded, so a journal write
    /// failure must not fail the op. Unmarked steps read as unknown and classify as a
    /// crash window — the safe direction.
    async fn mark_lifecycle_fence_complete(&mut self, workspace: &WorkspaceName) {
        use crate::storage::recovery::FenceStep;
        let workspace = workspace.clone();
        let _ = self
            .update_lifecycle_intents(move |journal| {
                [FenceStep::Sidecar, FenceStep::Companion, FenceStep::Image]
                    .into_iter()
                    .try_for_each(|step| journal.mark_fence_step(&workspace, step))
            })
            .await;
    }
    async fn discard_prepared_retire_intent(&mut self, workspace: &WorkspaceName) -> Result<()> {
        let workspace = workspace.clone();
        self.update_lifecycle_intents(move |journal| {
            if journal.discard_prepared_retirement(&workspace) {
                Ok(())
            } else {
                Err(CowshedError::internal(format!(
                    "prepared retirement for {workspace} disappeared during recovery"
                )))
            }
        })
        .await
    }
    async fn restore_prepared_clone_intent(&mut self, workspace: &WorkspaceName) -> Result<()> {
        let workspace = workspace.clone();
        self.update_lifecycle_intents(move |journal| {
            if journal.restore_prepared_clone_intent(&workspace) {
                Ok(())
            } else {
                Err(CowshedError::integrity(
                    format!("prepared clone retirement for {workspace} changed during recovery"),
                    "cowshed doctor --json",
                ))
            }
        })
        .await
    }

    /// Withdraws an unfinished create or fork of an unpublished `workspace` that never mutated
    /// anything, putting back `superseded`, the record its intent displaced (`None` forgets the
    /// name). Returns whether it did; an intent it keeps is recovery's to resume or retire.
    ///
    /// Such an intent is the residue of a refusal — no port block left, say — and replaying it
    /// would create a workspace whose caller was told it failed. It qualifies only while it is
    /// `Prepared` and nothing durable carries its name: no `PendingFence` image (the staged
    /// clone's first write is that sidecar) and no slot binding. The phase alone does not
    /// decide it, for two reasons. Binaries before the `Mutating` mark left every create and
    /// fork `Prepared`, including ones that crashed mid-clone. And a create marks its intent
    /// only once its slot is bound, so a crash or journal failure between the two leaves a
    /// `Prepared` intent whose binding is its evidence.
    ///
    /// The verb calls this when it fails, recovery when it finds such an intent unfinished; the
    /// caller holds the workspace's intent lease either way.
    async fn discard_unmutated_clone_intent(
        &mut self,
        workspace: &WorkspaceName,
        superseded: Option<crate::storage::recovery::LifecycleIntentRecord>,
    ) -> Result<bool> {
        let prepared = self.lifecycle_intents.get(workspace).is_some_and(|record| {
            record.completion.is_none()
                && record.phase == crate::storage::recovery::LifecycleIntentPhase::Prepared
                && matches!(
                    record.operation,
                    crate::storage::recovery::LifecycleIntent::Create { .. }
                        | crate::storage::recovery::LifecycleIntent::Fork { .. }
                )
        });
        if !prepared
            || self
                .pending_metadata()
                .await?
                .iter()
                .any(|(_, metadata)| &metadata.workspace == workspace)
        {
            return Ok(false);
        }
        let layout = self.layout.clone();
        let named = workspace.clone();
        let slotted = crate::storage::lifecycle::dispatch_blocking(move || {
            layout
                .slot_bindings()
                .map(|bindings| bindings.slot_of(&named).is_some())
        })
        .await
        .map_err(|error| CowshedError::internal(format!("slot binding task failed: {error}")))?
        .map_err(native_integrity_error)?;
        if slotted {
            return Ok(false);
        }
        let target = workspace.clone();
        if self
            .update_lifecycle_intents(move |journal| {
                Ok(journal.discard_prepared_clone_intent(&target, superseded))
            })
            .await?
        {
            Ok(true)
        } else {
            Err(CowshedError::internal(format!(
                "the prepared lifecycle intent for {workspace} changed under its lease"
            )))
        }
    }

    /// The failure path of `new` and `fork`: a refusal that mutated nothing leaves the journal as
    /// the verb found it. Best-effort, and said when it fails: the error the caller sees stays
    /// the verb's own, and recovery discards a leftover intent on the same evidence.
    async fn withdraw_refused_clone_intent(
        &mut self,
        workspace: &WorkspaceName,
        superseded: Option<crate::storage::recovery::LifecycleIntentRecord>,
    ) {
        if let Err(error) = self
            .discard_unmutated_clone_intent(workspace, superseded)
            .await
        {
            eprintln!(
                "cowshed: the failed lifecycle intent for {workspace} stays journaled ({}: {}); recovery discards it if nothing durable carries its name",
                error.code.as_str(),
                error.message
            );
        }
    }

    async fn complete_lifecycle_intent(
        &mut self,
        workspace: &WorkspaceName,
        completion: crate::storage::recovery::LifecycleIntentCompletion,
    ) -> Result<()> {
        let workspace = workspace.clone();
        self.update_lifecycle_intents(move |journal| journal.complete(&workspace, completion))
            .await
    }

    fn completed_workspace_intent(
        &self,
        operation: &crate::storage::recovery::LifecycleIntent,
    ) -> Option<&WorkspaceIncarnation> {
        let record = self.lifecycle_intents.get(operation.target())?;
        if record.operation != *operation {
            return None;
        }
        match record.completion.as_ref()? {
            crate::storage::recovery::LifecycleIntentCompletion::Workspace(incarnation) => {
                Some(incarnation)
            }
            crate::storage::recovery::LifecycleIntentCompletion::Retire(_) => None,
        }
    }

    fn completed_retire_intent(
        &self,
        operation: &crate::storage::recovery::LifecycleIntent,
    ) -> Option<&RemoveReport> {
        let record = self.lifecycle_intents.get(operation.target())?;
        if record.operation != *operation {
            return None;
        }
        match record.completion.as_ref()? {
            crate::storage::recovery::LifecycleIntentCompletion::Retire(report) => Some(report),
            crate::storage::recovery::LifecycleIntentCompletion::Workspace(_) => None,
        }
    }

    /// Finishes create/fork/adopt work and authorized retire mutations a crash left pending, then
    /// records the exact result so a later start does not repeat it. A prepared retirement has not
    /// mutated anything and is discarded: it may be the residue of a safety refusal, not durable
    /// authorization to delete on every later command. So is a prepared create or fork that
    /// published nothing and left no `PendingFence` image or slot binding: it was refused before
    /// its first mutation, and replaying it would create a workspace its caller was told failed
    /// (see [`Self::discard_unmutated_clone_intent`]). An unfinished intent whose lease another
    /// process holds is no residue at all: that process is running the operation right now, and
    /// replaying it here would run it a second time beside the first. A removal never finishes its
    /// own target's unpublished clone: the `rm` retires it instead. Only intents inside this
    /// opening's [`RecoveryScope`] are touched. Reports whether recovery mutated images or mounts,
    /// so a caller can discard an inventory read only when necessary.
    async fn recover_lifecycle_intents(&mut self) -> Result<bool> {
        use super::supervisor::{CommitmentDraft, CommitmentSink};
        use crate::storage::recovery::{
            LifecycleIntent, LifecycleIntentCompletion, LifecycleIntentPhase,
        };

        self.reload_lifecycle_intents().await?;
        let (unfinished, left): (Vec<_>, Vec<_>) = self
            .lifecycle_intents
            .records()
            .filter(|(_, record)| record.completion.is_none())
            .map(|(workspace, record)| (workspace.clone(), record.operation.verb()))
            .partition(|(workspace, _)| {
                self.recovery_scope.replay(workspace) != IntentReplay::Leave
            });
        for (workspace, verb) in left {
            eprintln!("cowshed: leaving unfinished {verb} of {workspace} to a verb that names it");
        }
        let mut claimed = Vec::with_capacity(unfinished.len());
        for (workspace, _) in unfinished {
            if self.claim_intent_lease(&workspace)? {
                claimed.push(workspace);
            } else {
                eprintln!(
                    "cowshed: leaving {workspace} to the cowshed process running its lifecycle operation"
                );
            }
        }
        // Read again under the leases: an owner may have finished between the first read and
        // the claim, and its result is the record to act on.
        self.reload_lifecycle_intents().await?;
        let pending = claimed
            .iter()
            .filter_map(|workspace| self.lifecycle_intents.get(workspace))
            .filter(|record| record.completion.is_none())
            .cloned()
            .collect::<Vec<_>>();
        if pending.is_empty() {
            return Ok(false);
        }
        for record in pending {
            let workspace = record.operation.target().clone();
            let verb = record.operation.verb();
            let replay = self.recovery_scope.replay(&workspace);
            // A named retirement retires its target's unpublished clone itself, from the pending
            // image or the bare intent (`remove_contained_in`), and never finishes it first:
            // whatever stopped the clone — a duplicate port block, a start revision the source
            // lost — would stop the replay again, and the name could never be removed.
            let retiring = self.recovery_scope.removal_target() == Some(&workspace);
            let replayed: Result<()> = async {
                let phase = record.phase;
                match record.operation {
                    // Adopt resumes itself: an unpublished main continues its copy in place, and a
                    // published one has its checkout swap and mount finished before completion.
                    LifecycleIntent::Adopt { options } => {
                        self.adopt(options).await?;
                    }
                    LifecycleIntent::Create { workspace, options } => {
                        match self.current(&workspace).await {
                            Ok(current) => {
                                // Activation is the image fence, not the end of the verb. Resume
                                // host-side registration and the commitment before completing the
                                // intent; both sinks are idempotent/append-safe on replay.
                                if options.register {
                                    self.register_workspace_in_main(&workspace).await?;
                                }
                                self.commitments
                                    .record(CommitmentDraft::WorkspaceIntroduced {
                                        repo_id: self.descriptor.repo_id.clone(),
                                        workspace_incarnation: current
                                            .derived
                                            .workspace
                                            .incarnation()
                                            .clone(),
                                    })
                                    .await?;
                                self.complete_lifecycle_intent(
                                    &workspace,
                                    LifecycleIntentCompletion::Workspace(
                                        current.derived.workspace.incarnation().clone(),
                                    ),
                                )
                                .await?;
                            }
                            Err(error) if error.code == ErrorCode::NotFound => {
                                if self.discard_unmutated_clone_intent(&workspace, None).await? {
                                    eprintln!(
                                        "cowshed: discarded the unfinished {verb} of {workspace}: it ended before changing anything, so nothing is left to finish"
                                    );
                                } else if retiring {
                                    eprintln!(
                                        "cowshed: leaving the unfinished {verb} of {workspace} to the rm retiring it"
                                    );
                                } else {
                                    self.create(workspace, options).await?;
                                }
                            }
                            Err(error) => return Err(error),
                        }
                    }
                    LifecycleIntent::Fork {
                        source,
                        destination,
                    } => match self.current(&destination).await {
                        Ok(current) => {
                            let source_incarnation = self
                                .current(&source)
                                .await?
                                .derived
                                .workspace
                                .incarnation()
                                .clone();
                            self.commitments
                                .record(CommitmentDraft::Fork {
                                    repo_id: self.descriptor.repo_id.clone(),
                                    source_incarnation,
                                    destination_incarnation: current
                                        .derived
                                        .workspace
                                        .incarnation()
                                        .clone(),
                                })
                                .await?;
                            self.complete_lifecycle_intent(
                                &destination,
                                LifecycleIntentCompletion::Workspace(
                                    current.derived.workspace.incarnation().clone(),
                                ),
                            )
                            .await?;
                        }
                        Err(error) if error.code == ErrorCode::NotFound => {
                            if self.discard_unmutated_clone_intent(&destination, None).await? {
                                eprintln!(
                                    "cowshed: discarded the unfinished {verb} of {destination}: it ended before changing anything, so nothing is left to finish"
                                );
                            } else if retiring {
                                eprintln!(
                                    "cowshed: leaving the unfinished {verb} of {destination} to the rm retiring it"
                                );
                            } else {
                                self.fork(source, destination).await?;
                            }
                        }
                        Err(error) => return Err(error),
                    },
                    LifecycleIntent::Retire {
                        workspace,
                        options: _,
                        origin,
                    } if phase == LifecycleIntentPhase::Prepared => {
                        let expected_mount = self.workspace_mount_path(&workspace)?;
                        match self.current(&workspace).await {
                            // Existing authoritative state proves retirement never published.
                            // Discarding this request prevents a refusal from becoming a deferred
                            // deletion on every later command.
                            Ok(_) if origin.is_some() => {
                                return Err(CowshedError::integrity(
                                    format!("pending retirement target {workspace} became active"),
                                    "cowshed doctor --json",
                                ));
                            }
                            Ok(_) => {
                                self.discard_prepared_retire_intent(&workspace).await?;
                            }
                            // Absence is the publication fence: an older process crossed it but
                            // died before recording the result, so retain idempotent completion.
                            Err(error) if error.code == ErrorCode::NotFound => {
                                if origin.is_some()
                                    && self
                                        .pending_metadata()
                                        .await?
                                        .iter()
                                        .any(|(_, metadata)| metadata.workspace == workspace)
                                {
                                    // A prepared retirement has not authorized deletion.
                                    self.restore_prepared_clone_intent(&workspace).await?;
                                } else {
                                    self.complete_lifecycle_intent(
                                        &workspace,
                                        LifecycleIntentCompletion::Retire(RemoveReport::default()),
                                    )
                                    .await?;
                                }
                            }
                            // An unreadable target is not evidence either way. Fail closed rather
                            // than throwing away the only recovery record, and say which target
                            // must become readable before startup can decide safely.
                            Err(error) => {
                                return Err(
                                    crate::storage::recovery::prepared_retirement_unreadable(
                                        &workspace,
                                        &expected_mount,
                                        error,
                                    ),
                                );
                            }
                        }
                    }
                    LifecycleIntent::Retire {
                        workspace,
                        options,
                        origin,
                    } => match self.current(&workspace).await {
                        Ok(_) if origin.is_some() => {
                            return Err(CowshedError::integrity(
                                format!("pending retirement target {workspace} became active"),
                                "cowshed doctor --json",
                            ));
                        }
                        Ok(_) => {
                            self.remove(workspace, options).await?;
                        }
                        Err(error) if error.code == ErrorCode::NotFound => {
                            if origin.is_some()
                                && self
                                    .pending_metadata()
                                    .await?
                                    .iter()
                                    .any(|(_, metadata)| metadata.workspace == workspace)
                            {
                                self.remove(workspace, options).await?;
                            } else {
                                self.complete_lifecycle_intent(
                                    &workspace,
                                    LifecycleIntentCompletion::Retire(RemoveReport::default()),
                                )
                                .await?;
                            }
                        }
                        Err(error) => return Err(error),
                    },
                }
                Ok(())
            }
            .await;
            match replayed {
                Ok(()) => {}
                Err(error) if replay == IntentReplay::Residue => eprintln!(
                    "cowshed: unfinished {verb} of {workspace} could not be completed ({}: {}); it stays journaled for a later pass",
                    error.code.as_str(),
                    error.message
                ),
                Err(error) => return Err(error),
            }
        }
        Ok(true)
    }

    /// The binding gate every verb (and recovery) runs on a live host.
    ///
    /// Reuses the open-time reconcile so a resident host follows a transport move the same way a
    /// fresh open does — healed, persisted, and reflected in its own descriptor — instead of
    /// refusing every verb until a reopen. A host opened for the identity change skips the remote
    /// pairing entirely: recovery runs before the verb dispatches, and the pairing it would
    /// enforce is exactly the one `mv … --repo-id` exists to supersede. An inspecting host checks
    /// the pairing and writes nothing; doctor reports the move.
    async fn validate_binding(&mut self) -> Result<()> {
        if self.binding_remote_validation == BindingRemoteValidation::ForIdentityChange {
            return Ok(());
        }
        if let Some(moved) = self.binding_move().await?
            && self.recovery_scope.repairs()
        {
            self.record_binding(moved).await?;
        }
        Ok(())
    }

    /// The binding Git's remotes now call for when a transport move left the recorded one
    /// behind; `None` when they agree. An identity move is an error naming the rebind verb.
    async fn binding_move(&self) -> Result<Option<RepositoryBinding>> {
        let remotes = self.git.remotes().await?;
        reconcile_binding_with_remotes(
            &self.descriptor.binding,
            &remotes,
            BindingRemoteValidation::Strict,
            self.git.root(),
        )
    }

    /// Persist a transport move and serve under it from now on.
    async fn record_binding(&mut self, moved: RepositoryBinding) -> Result<()> {
        persist_binding(&self.layout, &moved).await?;
        self.descriptor.binding = std::sync::Arc::new(moved);
        Ok(())
    }

    async fn authoritative(&self) -> Result<Vec<NativeWorkspace>> {
        self.authoritative_with_project_root_validation(ProjectRootValidation::Strict)
            .await
    }

    async fn authoritative_allowing_detached_main_relocation(
        &self,
    ) -> Result<Vec<NativeWorkspace>> {
        self.authoritative_with_project_root_validation(
            ProjectRootValidation::AllowDetachedMainRelocation,
        )
        .await
    }

    async fn authoritative_with_project_root_validation(
        &self,
        root_validation: ProjectRootValidation,
    ) -> Result<Vec<NativeWorkspace>> {
        use crate::storage::lifecycle::Substrate;

        let derived = self
            .substrate
            .list(&self.descriptor.repo_id)
            .await
            .map_err(native_storage_error)?;
        let layout = self.layout.clone();
        let project_root = self.descriptor.git_root.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            derived
                .into_iter()
                .map(|derived| {
                    let image = layout
                        .canonical_image(derived.workspace.name())?
                        .image()
                        .to_path_buf();
                    let metadata =
                        crate::metadata::DetachedWorkspaceMetadata::read_for_image(&image)
                            .map_err(|error| {
                                crate::storage::apfs::ApfsStorageError::Host(error.to_string())
                            })?;
                    if metadata.repo_id != *derived.workspace.repo()
                        || metadata.workspace != *derived.workspace.name()
                        || metadata.workspace_incarnation != *derived.workspace.incarnation()
                    {
                        return Err(crate::storage::apfs::ApfsStorageError::MarkerMismatch(
                            format!("detached metadata disagrees with {}", image.display()),
                        ));
                    }
                    validate_workspace_controller_root(
                        &derived,
                        &metadata,
                        &project_root,
                        root_validation,
                    )?;
                    Ok(NativeWorkspace {
                        derived,
                        metadata,
                        image,
                    })
                })
                .collect::<std::result::Result<Vec<_>, _>>()
        })
        .await
        .map_err(|error| CowshedError::internal(format!("metadata read task failed: {error}")))?
        .map_err(native_storage_error)
    }

    async fn current(&self, name: &WorkspaceName) -> Result<NativeWorkspace> {
        self.authoritative()
            .await?
            .into_iter()
            .find(|workspace| workspace.derived.workspace.name() == name)
            .ok_or_else(|| {
                CowshedError::not_found(
                    format!("workspace {name} does not exist"),
                    "list published workspaces and retry",
                )
            })
    }

    async fn pending_metadata(
        &self,
    ) -> Result<Vec<(PathBuf, crate::metadata::DetachedWorkspaceMetadata)>> {
        let main_image = self
            .layout
            .main_image()
            .map_err(native_integrity_error)?
            .image()
            .to_path_buf();
        let sessions = self.layout.project().sessions.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let mut images = Vec::from_iter(main_image.exists().then_some(main_image));
            let entries = match std::fs::read_dir(&sessions) {
                Ok(entries) => entries
                    .map(|entry| {
                        entry.map(|entry| entry.path()).map_err(|error| {
                            CowshedError::environment_missing(
                                format!(
                                    "cannot enumerate session metadata in {}: {error}",
                                    sessions.display()
                                ),
                                "check controller storage permissions",
                            )
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
                Err(error) => {
                    return Err(CowshedError::environment_missing(
                        format!(
                            "cannot enumerate session metadata in {}: {error}",
                            sessions.display()
                        ),
                        "check controller storage permissions",
                    ));
                }
            };
            images.extend(
                crate::storage::discover_session_images(entries)
                    .into_iter()
                    .map(|image| image.path().to_path_buf()),
            );
            let mut pending = Vec::new();
            for image in images {
                let metadata = crate::metadata::DetachedWorkspaceMetadata::read_for_image(&image)
                    .map_err(native_integrity_error)?;
                if metadata.publication_state == crate::metadata::PublicationState::PendingFence {
                    pending.push((image, metadata));
                }
            }
            Ok(pending)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("pending metadata task failed: {error}")))?
    }

    /// Finish an adoption whose main is already published: swap the checkout for main's
    /// mountpoint if a crash or failure stopped before it, mount main there, and conclude.
    async fn finish_adoption(&mut self, current: NativeWorkspace) -> Result<WorkspaceSnapshot> {
        let pre_cowshed = pre_cowshed_path(&self.descriptor.git_root)?;
        timed_async(
            "adopt",
            "finish",
            self.substrate
                .finish_adoption(&current.derived.workspace, &pre_cowshed),
        )
        .await
        .map_err(native_storage_error)?;
        self.conclude_adoption(
            &current.derived.workspace,
            super::build_volumes::FirstMint::Nothing,
        )
        .await
    }

    /// Everything adoption does once main is mounted at the checkout. Each step is safe to repeat
    /// on a replay: the commitment is append-safe, completion overwrites, and the first touch
    /// finds the volume an interrupted one linked.
    ///
    /// Main's first touch runs before the intent completes: until then the unfinished adopt
    /// keeps collection off the volume in `first`, which [`Self::adopt`] minted beside main's
    /// image and which nothing links and no record names before the first touch links it.
    /// Whatever the first touch did, and when the commitment fails before it runs, `first` is
    /// settled before this returns: released unless the checkout links it. The intent completes
    /// even when the first touch failed: main is adopted, and its build state is refreshed again
    /// before its first job.
    async fn conclude_adoption(
        &mut self,
        workspace: &crate::storage::lifecycle::LifecycleWorkspace,
        first: super::build_volumes::FirstMint,
    ) -> Result<WorkspaceSnapshot> {
        use super::supervisor::CommitmentSink;
        let introduced = self
            .commitments
            .record(super::supervisor::CommitmentDraft::WorkspaceIntroduced {
                repo_id: self.descriptor.repo_id.clone(),
                workspace_incarnation: workspace.incarnation().clone(),
            })
            .await
            .and_then(|()| self.workspace_mount_path(workspace.name()));
        let mount = match introduced {
            Ok(mount) => mount,
            Err(error) => {
                first.settle().await;
                return Err(error);
            }
        };
        let touched = self
            .first_build_state(workspace.name(), &mount, first.lend())
            .await;
        crate::timing::spanned("adopt", "build-volume-release", first.settle()).await;
        self.complete_lifecycle_intent(
            workspace.name(),
            crate::storage::recovery::LifecycleIntentCompletion::Workspace(
                workspace.incarnation().clone(),
            ),
        )
        .await?;
        touched?;
        timed_async("adopt", "snapshot", self.snapshot_named(workspace.name())).await
    }

    /// Main's first touch: its supervisor, then its build state moves onto its first build
    /// volume, the one `lent` names when it fits, and main gets the seed its forks clone
    /// (16_build_volumes.md, "Targets and seeds").
    async fn first_build_state(
        &mut self,
        name: &WorkspaceName,
        mount: &Path,
        lent: super::build_volumes::Lent,
    ) -> Result<()> {
        timed_async("adopt", "supervisor", self.ensure_supervisor(name)).await?;
        let current = self.current(name).await?;
        timed_async(
            "adopt",
            "build-state",
            self.refresh_build_state_for(&current, mount, lent),
        )
        .await?;
        Ok(())
    }

    fn workspace_mount_path(&self, workspace: &WorkspaceName) -> Result<PathBuf> {
        self.layout
            .main_aware_workspace_mount(&self.substrate_config.checkout_path, workspace)
            .map_err(native_integrity_error)
    }
    fn build_volume_layout(&self) -> Result<crate::build_volume::BuildVolumeLayout> {
        crate::build_volume::BuildVolumeLayout::new(self.layout.project())
            .map_err(native_integrity_error)
    }

    fn build_volumes(&self) -> Result<super::build_volumes::BuildVolumes> {
        Ok(super::build_volumes::BuildVolumes::new(
            self.substrate.shared_host(),
            self.build_volume_layout()?,
        ))
    }

    /// Every build volume image now, and then what links them (16_build_volumes.md, "Garbage
    /// collection"): each mounted checkout's build link, every workspace at its incarnation
    /// (whose seed a seed may be), the detached workspaces, whose links cannot be read and whose
    /// volumes their records name, and the workspaces an unfinished create, fork or adopt is
    /// forming, in this process or any other.
    ///
    /// A create or fork forks its build volume and seed into its staged checkout, which no other
    /// process can see, and an adopt mints main's first volume beside main's image; each completes
    /// its intent only after publishing the workspace. Without the journal, an `rm` in another
    /// process collected a fresh fork's volume and seed as unlinked garbage, and the fork's own
    /// mount then refused its dangling link. The reads go images, journal, workspaces: such a verb
    /// joins the journal's forming set before it makes a volume and leaves it only once its
    /// workspace is published, so every listed volume is named by one of the two later reads.
    /// Listed last, an image made after the journal read was decided from links that predate it.
    async fn build_volume_links(
        &self,
    ) -> Result<(
        Vec<crate::build_volume::BuildVolumeId>,
        super::build_volumes::Links,
    )> {
        let volumes = self.build_volumes()?;
        let journal = self.lifecycle_intents_path.clone();
        let layout = volumes.layout.clone();
        let (images, creating) = crate::storage::lifecycle::dispatch_blocking(move || {
            let images = layout.list()?;
            let creating = crate::storage::recovery::LifecycleIntentJournal::load(&journal)?
                .forming()
                .cloned()
                .collect();
            Ok::<_, CowshedError>((images, creating))
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("lifecycle intent read task failed: {error}"))
        })??;
        let mut links = super::build_volumes::Links {
            creating,
            ..super::build_volumes::Links::default()
        };
        for workspace in self.authoritative().await? {
            let name = workspace.derived.workspace.name().clone();
            let checkout = self.workspace_mount_path(&name)?;
            links.owners.insert(super::build_volumes::Owner {
                name: name.clone(),
                incarnation: workspace.derived.workspace.incarnation().clone(),
            });
            if matches!(
                workspace.derived.mount_state,
                crate::storage::lifecycle::MountState::Mounted { .. }
            ) {
                if let Some(id) = volumes.layout.linked(&checkout)? {
                    links.volumes.insert(id);
                }
            } else {
                links.detached.insert(name.clone());
            }
            links.checkouts.insert(name, checkout);
        }
        Ok((images, links))
    }

    /// The build-volume collection `rm` and `land` run once a checkout let go of its volume
    /// (16_build_volumes.md, "Garbage collection"): every volume no checkout links is reclaimed
    /// now unless a job holds it, and every one left is said on stderr with the reason, the
    /// routine ones (a detached or still-forming workspace's own) counted on one line, so a
    /// volume that stays is never a silent skip.
    async fn collect_build_volumes(&self) -> Result<()> {
        let (images, links) = self.build_volume_links().await?;
        let collection = self.build_volumes()?.collect(images, links, false).await?;
        let mut routine = 0_usize;
        for deferred in collection.deferred {
            if deferred.deferral.is_routine() {
                routine += 1;
                continue;
            }
            eprintln!(
                "cowshed: build volume {} stays: {}{}",
                deferred.path.display(),
                deferred.deferral,
                if deferred.deferral.is_pending() {
                    "; `cowshed gc` retries it"
                } else {
                    ""
                }
            );
        }
        if routine > 0 {
            eprintln!(
                "cowshed: {routine} build volume{} of detached or still-forming workspaces stay until those attach or finish forming; `cowshed gc --dry-run` names them",
                if routine == 1 { "" } else { "s" }
            );
        }
        Ok(())
    }

    /// Land steps 4–7 (16_build_volumes.md, "Land"): quiesce the landing workspace, close the
    /// target's Nx state and carry the target's cache entries into the landing volume, freeze
    /// the target's seed from that volume, move the target's build link onto it, and re-run the
    /// landed checks in the target, where every Nx task should hit.
    async fn land_build_volume(
        &mut self,
        workspace: &WorkspaceName,
        landing: &Path,
        into: &NativeLandingInto,
        checks: &[String],
        retire: bool,
    ) -> Result<crate::api::dto::LandBuildVolume> {
        use crate::api::dto::{Adoption, AdoptionSkip, LandBuildVolume};
        let volumes = self.build_volumes()?;
        let skipped = |reason| LandBuildVolume {
            seeded: false,
            adoption: Adoption::Skipped { reason },
        };
        let Some(landing_volume) = volumes.hold_landing(landing)? else {
            return Ok(skipped(AdoptionSkip::NoLandingVolume));
        };
        if volumes.layout.linked(&into.mount)?.is_none() {
            return Ok(skipped(AdoptionSkip::NoTargetVolume));
        }
        // Step 4: the supervisor stops every job of the landing workspace; the volume's own
        // daemon and database holders are then the build volume module's to settle. The land
        // holds the volume from before the jobs let go of it until the target's link and record
        // name it, so no collection takes it between, whatever links that collection read.
        let stopped = self.stop_supervisor_for_removal(workspace, true).await?;
        require_lost_groups_released(workspace, &stopped, "quiesce").await?;
        drop(stopped);
        let quiet = match timed_async(
            "land",
            "quiesce",
            volumes.quiesce(landing_volume, into.mount.clone()),
        )
        .await?
        {
            Ok(quiet) => quiet,
            Err(reason) => return Ok(skipped(reason)),
        };
        let tree = git_revision_oid(landing, "HEAD^{tree}").await?;
        let target = self.current(&into.name).await?;
        let owner = super::build_volumes::Owner {
            name: into.name.clone(),
            incarnation: target.derived.workspace.incarnation().clone(),
        };
        // The carry's copies run while the target still runs; only its commit needs the
        // target closed. Both precede the seed, so the seed holds what was carried.
        let staging = timed_async(
            "land",
            "carry-stage",
            volumes.stage(&quiet, into.mount.clone()),
        )
        .await?;
        let closed = timed_async(
            "land",
            "close-target",
            volumes.close_target(into.mount.clone()),
        )
        .await?;
        let carried = match closed {
            Ok(closed) => {
                let carried =
                    timed_async("land", "carry-commit", volumes.commit(staging, &quiet)).await?;
                Ok((closed, carried))
            }
            Err(reason) => {
                volumes.unstage(&quiet).await?;
                Err(reason)
            }
        };
        timed_async(
            "land",
            "freeze-seed",
            volumes.freeze_seed(&quiet, owner.clone(), tree.clone()),
        )
        .await?;
        let (closed, carried) = match carried {
            Ok(carried) => carried,
            Err(reason) => {
                return Ok(LandBuildVolume {
                    seeded: true,
                    adoption: Adoption::Skipped { reason },
                });
            }
        };
        let adoption = match timed_async(
            "land",
            "adopt",
            volumes.adopt(&quiet, closed, into.name.clone(), into.mount.clone(), tree),
        )
        .await?
        {
            Err(reason) => Adoption::Skipped { reason },
            Ok(super::build_volumes::Adopted {
                elapsed_ms,
                previous,
            }) => {
                // Nothing links the previous volume and its record says Unlinked, so its release
                // (an unmount, detach and delete, each waiting on storagekitd) needs nothing the
                // target's next steps touch, and runs beside them.
                let release = timed_async(
                    "land",
                    "release-previous",
                    volumes.release_previous(previous),
                );
                let in_target = async {
                    // Each moved link's volume carries the label of the checkout that built it:
                    // the target's supervisor names the adopted one now, in the background, and a
                    // kept workspace's supervisor, started on its fresh clone, names that one as
                    // it starts.
                    let target = self.ensure_supervisor(&into.name).await?;
                    target
                        .name_build_volume(volumes.layout.grant(&into.name, &into.mount)?)
                        .await?;
                    if !retire {
                        volumes
                            .refork(owner, workspace.clone(), landing.to_owned())
                            .await?;
                        self.ensure_supervisor(workspace).await?;
                    }
                    timed_async(
                        "land",
                        "adoption-check",
                        self.check_adoption(&into.name, &into.mount, checks),
                    )
                    .await
                };
                let check = match tokio::join!(release, in_target) {
                    (Ok(()), check) => check?,
                    (Err(release), Ok(_)) => return Err(release),
                    // The land fails for what failed in the target; the volume nothing links is
                    // collection's to retry.
                    (Err(release), Err(error)) => {
                        eprintln!(
                            "cowshed: {}'s previous build volume stays: {release}; `cowshed gc` retries it",
                            into.name
                        );
                        return Err(error);
                    }
                };
                Adoption::Adopted {
                    elapsed_ms,
                    carried,
                    check,
                }
            }
        };
        Ok(LandBuildVolume {
            seeded: true,
            adoption,
        })
    }

    /// Land step 7 (2b): each landed check re-run in the target on the adopted volume. Hits and
    /// misses come from the run summary stock Nx writes into the target's cache directory, and
    /// only from one attributed to the check itself (`nx::attribute`); each miss carries the
    /// inputs Nx hashes for it.
    async fn check_adoption(
        &mut self,
        target: &WorkspaceName,
        mount: &Path,
        checks: &[String],
    ) -> Result<crate::api::dto::AdoptionCheck> {
        use crate::build_volume::nx::{Attribution, CacheStatus, attribute};
        let mut result = crate::api::dto::AdoptionCheck::default();
        let (handle, build_volume) = self.admit_build_state(target).await?;
        let state = crate::build_volume::BuildVolumeState::read(
            &mount.join(crate::build_volume::BUILD_LINK),
        )?;
        let caches = state
            .paths
            .iter()
            .filter(|path| {
                crate::build_volume::BuildStateTool::of(path)
                    == crate::build_volume::BuildStateTool::Nx
                    && path.checkout.as_path().file_name() == Some(std::ffi::OsStr::new("cache"))
            })
            .map(|path| path.checkout.as_path().to_owned())
            .collect::<Vec<_>>();
        for check in checks {
            let spawned = std::time::SystemTime::now();
            let job = handle
                .exec(None, build_volume.clone(), land_check_request(check))
                .await?;
            let info = handle.wait(job).await?;
            let exited = std::time::SystemTime::now();
            match info.exit {
                Some(crate::api::dto::ExitStatus::Exited { code: 0 }) => {}
                Some(crate::api::dto::ExitStatus::Exited { code }) => {
                    result.failed.push(crate::api::dto::FailedCheck {
                        check: check.clone(),
                        exit: Some(code),
                    });
                }
                _ => result.failed.push(crate::api::dto::FailedCheck {
                    check: check.clone(),
                    exit: None,
                }),
            }
            let mut unattributed = Vec::new();
            let mut attributed = false;
            for cache in &caches {
                match attribute(&mount.join(cache), check, spawned, exited) {
                    Attribution::Ours(run) => {
                        attributed = true;
                        for task in run.tasks {
                            match task.cache {
                                CacheStatus::LocalHit | CacheStatus::RemoteHit => {
                                    result.hits += 1;
                                }
                                CacheStatus::Miss => result.misses.push(
                                    task_inputs(
                                        &handle,
                                        build_volume.clone(),
                                        task.task_id,
                                        task.hash,
                                    )
                                    .await?,
                                ),
                            }
                        }
                    }
                    Attribution::Unattributed(reason) => {
                        unattributed.push(crate::api::dto::UnattributedCheck {
                            check: check.clone(),
                            cache: cache.to_string_lossy().into_owned(),
                            reason,
                        });
                    }
                }
            }
            if !attributed {
                result.unattributed.extend(unattributed);
            }
        }
        Ok(result)
    }

    /// Refresh `current`'s build state at its mounted checkout `mount`
    /// ([`ProjectRuntimeHost::refresh_build_state`]). Detection reads the same canonical context
    /// the supervisor's capability admission reads, and Cargo is asked as a job of the workspace
    /// ([`crate::capabilities::JobCargo`]): sandboxed, after the workspace shell's activation, and
    /// without the caller's variables, so a checkout whose toolchain comes from its dev
    /// environment is asked with that toolchain, and a `CARGO_TARGET_DIR` of the controller's
    /// shell names no checkout's build state. Discovery runs only when the tracked build inputs'
    /// fingerprint moved off the one the volume's state records; otherwise only displaced links
    /// are restored. A first touch takes the volume `lent` names when it fits rather than
    /// minting one of its own ([`super::build_volumes::Lent`]).
    async fn refresh_build_state_for(
        &mut self,
        current: &NativeWorkspace,
        mount: &Path,
        lent: super::build_volumes::Lent,
    ) -> Result<crate::build_volume::BuildStateRefresh> {
        use super::build_volumes::Discovered;
        let name = current.derived.workspace.name().clone();
        let volumes = self.build_volumes()?;
        let (fingerprint, recorded) = {
            let (volumes, mount) = (volumes.clone(), mount.to_owned());
            crate::storage::lifecycle::dispatch_blocking(move || {
                let fingerprint = crate::capabilities::tracked_manifest_fingerprint(&mount)?;
                Ok::<_, CowshedError>((fingerprint, volumes.state_of(&mount)?))
            })
            .await
            .map_err(|error| {
                CowshedError::internal(format!("build state task failed: {error}"))
            })??
        };
        let unchanged = recorded
            .as_ref()
            .is_some_and(|(_, state)| state.fingerprint.as_deref() == Some(fingerprint.as_str()));
        let (discovered, findings) = if unchanged {
            (Discovered::Unchanged, Vec::new())
        } else {
            let main_mount = self.workspace_mount_path(&main_name())?;
            let grants = effective_workspace_grants(&self.layout, &current.metadata.grants)?;
            let sandbox = supervisor_sandbox(
                &self.home,
                &self.layout,
                &self.telemetry_root,
                current,
                &grants,
                mount.to_owned(),
                main_mount.clone(),
                volumes.layout.grant(&name, mount)?,
            )?;
            let discovery = {
                let mount = mount.to_owned();
                let runtime = tokio::runtime::Handle::current();
                crate::storage::lifecycle::dispatch_blocking(move || {
                    let mut cargo = crate::capabilities::JobCargo::new(&sandbox, runtime);
                    sandbox.with_detection_context(&mount, |context| {
                        crate::capabilities::discover_build_state(context, &mut cargo)
                    })
                })
                .await
                .map_err(|error| {
                    CowshedError::internal(format!("build state discovery failed: {error}"))
                })??
            };
            (
                Discovered::Changed {
                    paths: discovery.paths,
                    fingerprint,
                    capacity: crate::storage::bootstrap::main_cowshed_config(&main_mount)?
                        .build_capacity(),
                },
                discovery.findings,
            )
        };
        let mut refresh = volumes
            .refresh(
                super::build_volumes::Owner {
                    name: name.clone(),
                    incarnation: current.derived.workspace.incarnation().clone(),
                },
                mount.to_owned(),
                discovered,
                lent,
            )
            .await?
            .map_err(|refusal| {
                CowshedError::conflict(
                    format!("{name}: {refusal}"),
                    "move the tracked files out of that build-state path, or configure the tool \
                     to keep its state elsewhere, then retry",
                )
            })?;
        refresh.findings = findings;
        if refresh.created {
            // First touch can happen after the supervisor started with no build volume.
            // Name the new volume now, even when refresh admitted no job on it.
            let build = volumes.layout.grant(&name, mount)?;
            self.ensure_supervisor(&name)
                .await?
                .name_build_volume(build)
                .await?;
        }
        for displaced in &refresh.displaced {
            eprintln!("cowshed: {name}: {displaced}");
        }
        for finding in &refresh.findings {
            eprintln!("cowshed: {name}: {finding}");
        }
        Ok(refresh)
    }

    /// Before a job of `workspace` is admitted: its supervisor, its build state refreshed, and
    /// the build-volume grant the job runs with, resolved now (16_build_volumes.md, "Process
    /// lifetime across a swap"): a job keeps the volume it was admitted on, and one admitted
    /// after an adoption gets the adopted one, without relaunching the supervisor.
    async fn admit_build_state(
        &mut self,
        workspace: &WorkspaceName,
    ) -> Result<(
        super::supervisor::WorkspaceSupervisorHandle,
        Option<PathBuf>,
    )> {
        let handle = self.ensure_supervisor(workspace).await?;
        let current = self.current(workspace).await?;
        let mount = self.workspace_mount_path(workspace)?;
        self.refresh_build_state_for(&current, &mount, super::build_volumes::Lent::Nothing)
            .await?;
        let grant = self.build_volume_layout()?.grant(workspace, &mount)?;
        Ok((handle, grant))
    }

    /// Give `workspace` the stable mount path of `slot`.
    ///
    /// Recorded before the workspace's first mount, because the record is what every mount path
    /// derivation reads: binding a workspace that is already mounted would leave the live volume at
    /// one path and the whole controller looking at another.
    async fn bind_slot(
        &self,
        workspace: &WorkspaceName,
        slot: crate::metadata::SlotId,
    ) -> Result<()> {
        let layout = self.layout.clone();
        let workspace = workspace.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let mut bindings = layout.slot_bindings()?;
            bindings.bind(slot, workspace)?;
            layout.record_slot_bindings(&bindings)?;
            Ok::<_, crate::storage::StorageLayoutError>(())
        })
        .await
        .map_err(|error| CowshedError::internal(format!("slot binding task failed: {error}")))?
        .map_err(slot_binding_error)
    }

    /// Vacate whatever slot `workspace` held, reporting it. Idempotent: an unbound workspace is
    /// already in the desired state.
    async fn release_slot(
        &self,
        workspace: &WorkspaceName,
    ) -> Result<Option<crate::metadata::SlotId>> {
        let layout = self.layout.clone();
        let workspace = workspace.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let mut bindings = layout.slot_bindings()?;
            let released = bindings.release(&workspace);
            if released.is_some() {
                layout.record_slot_bindings(&bindings)?;
            }
            Ok::<_, crate::storage::StorageLayoutError>(released)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("slot release task failed: {error}")))?
        .map_err(native_integrity_error)
    }

    /// Where this project's durable record of the checkout path lives.
    ///
    /// The marker is read through main's mount, so this is only usable while main is mounted; the
    /// sidecar half is store-side and always reachable.
    fn checkout_record(&self) -> Result<crate::checkout::CheckoutRecord> {
        Ok(crate::checkout::CheckoutRecord {
            mount_point: self.workspace_mount_path(&main_name())?,
            image: self
                .layout
                .main_image()
                .map_err(native_integrity_error)?
                .image()
                .to_path_buf(),
        })
    }

    /// Rebuild the substrate around a checkout that now lives somewhere else.
    ///
    /// `ApfsSubstrate` and `MacOsApfsExecutionHost` each capture the substrate config by value, and
    /// the substrate shares its copy with every clone it has handed out, so there is no in-place
    /// mutation that could not be observed half-applied. The whole triple — config, execution host,
    /// substrate — is therefore rebuilt and swapped at once. `&mut self` is what makes that safe:
    /// the project actor owns the runtime host exclusively, so the swap cannot race a concurrent
    /// operation, and the move transaction performs it at the one moment nothing is mounted.
    ///
    /// The Git repository handle and the descriptor's project root move with it. They are the same
    /// fact spelled three ways, and leaving any one behind would send the next operation to the old
    /// path.
    fn rebind_checkout(&mut self, checkout_path: &Path) -> Result<()> {
        let config = self.substrate_config.rebind_checkout(checkout_path);
        self.rebind_substrate(config)?;
        self.descriptor.git_root = std::sync::Arc::from(checkout_path);
        self.git = crate::git::GitRepository::from_root(checkout_path);
        Ok(())
    }

    /// Rebuild the substrate after the repository namespace has moved.
    ///
    /// Carries the same swap-the-whole-triple discipline as [`Self::rebind_checkout`], and adds the
    /// same reason. The Git checkout stays where it is and its remote is deliberately untouched.
    fn rebind_repo_id(
        &mut self,
        repo_id: RepoId,
        binding: RepositoryBinding,
        layout: crate::storage::StorageLayout,
    ) -> Result<()> {
        // The config carries no identity: `owned_repo_ids` reads the binding beside the project
        // directory, which the namespace move has already put in place. Rebuilding the triple is
        // still required, because the layout the host derives paths from has changed.
        let config = self.substrate_config.clone();
        self.rebind_substrate(config)?;
        self.layout = layout;
        self.descriptor.repo_id = repo_id;
        self.descriptor.binding = std::sync::Arc::new(binding);
        self.lifecycle_intents_path = self
            .layout
            .project()
            .project_root
            .join(crate::storage::recovery::LIFECYCLE_INTENTS_FILE);
        Ok(())
    }

    /// The one place the config, the execution host and the substrate are swapped together. Every
    /// rebind goes through here so no caller can leave the three disagreeing.
    fn rebind_substrate(
        &mut self,
        config: crate::storage::apfs::ApfsSubstrateConfig,
    ) -> Result<()> {
        let host = crate::storage::apfs::native::MacOsApfsExecutionHost::new(
            crate::apfs::SystemCommandRunner,
            config.clone(),
        )
        .map_err(native_storage_error)?;
        self.substrate = crate::storage::apfs::ApfsSubstrate::new(config.clone(), host);
        self.substrate_config = config;
        Ok(())
    }

    /// Every identity this project owns, read from the binding it is holding right now.
    fn owned_repo_ids(&self) -> Result<OwnedRepoIds> {
        self.descriptor
            .binding
            .owned_repo_ids()
            .map_err(native_integrity_error)
    }

    /// Bring every workspace's own record of the project into line with where the project is now.
    ///
    /// A workspace records the project in four independent places, and a relocation invalidates all
    /// four at once:
    ///
    /// * its **marker** (`.cowshed/workspace.json`) and its **detached sidecar**, both of which
    ///   name `projectRoot`. Rewriting only main's pair would leave every session of a relocated
    ///   project naming a directory that has stopped being a repository, and `doctor` reporting
    ///   "workspace marker identity does not match" with no remedy in sight;
    /// * its **`main` remote**, or its **linked-worktree registration** when it is a git-worktree
    ///   workspace;
    /// * its **merge drivers**, whose absolute program paths die with the old checkout and take
    ///   every rebase in the project with them.
    ///
    /// Under direct mount main's mount *is* the checkout, so moving the checkout moves the URL
    /// every workspace fetches from and the gitdir every git-worktree workspace points at; under
    /// the symlink layout the mount never moves and the remote repair is the idempotent re-run
    /// `configure_main_remote` is built for. One code path covers both because the layout is
    /// exactly the thing `workspace_mount_path` already answers.
    ///
    /// A detached workspace has no reachable marker and no reachable config, so only its sidecar
    /// moves; `attach` finishes the pair. Every workspace is attempted before any failure is
    /// raised: a project half-repaired by a refusal in the middle is worse than one fully
    /// repaired except for the workspace that genuinely could not be written, and re-running the
    /// same verb is the remedy either way.
    async fn repair_workspace_records(&mut self, project_root: &Path) -> Result<()> {
        let main_mount = self.workspace_mount_path(&main_name())?;
        let mut failures = Vec::new();
        for workspace in self.authoritative().await? {
            let name = workspace.derived.workspace.name().clone();
            if let Err(error) = self
                .repair_one_workspace_record(&workspace, &name, &main_mount, project_root)
                .await
            {
                failures.push(format!("{name}: {}", error.message));
            }
        }
        if failures.is_empty() {
            return Ok(());
        }
        Err(CowshedError::integrity(
            format!(
                "could not repair every workspace's record of {}: {}",
                project_root.display(),
                failures.join("; ")
            ),
            "cowshed doctor --json",
        ))
    }

    async fn repair_one_workspace_record(
        &self,
        workspace: &NativeWorkspace,
        name: &WorkspaceName,
        main_mount: &Path,
        project_root: &Path,
    ) -> Result<()> {
        let record = crate::checkout::CheckoutRecord {
            mount_point: self.workspace_mount_path(name)?,
            image: workspace.image.clone(),
        };
        let mounted = matches!(
            workspace.derived.mount_state,
            crate::storage::lifecycle::MountState::Mounted { .. }
        );
        let rewrite_root = project_root.to_owned();
        let repo_id = self.descriptor.repo_id.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            // Identity moves with the project root because both are the same kind of fact: this
            // workspace's own record of the project it belongs to. A mounted workspace has both
            // copies in reach, the in-image marker and the store-side sidecar; a detached one has
            // only the sidecar, and its marker keeps naming an identity the binding records as
            // former until it is next attached and lands here with the marker in reach.
            if mounted {
                record.rewrite_project_root(&rewrite_root)?;
                record.rewrite_repo_id(&repo_id).map(|_| ())
            } else {
                record.rewrite_detached_project_root(&rewrite_root)?;
                record.rewrite_detached_repo_id(&repo_id).map(|_| ())
            }
        })
        .await
        .map_err(|error| CowshedError::internal(format!("checkout record task failed: {error}")))?
        .map_err(native_integrity_error)?;
        if !mounted {
            return Ok(());
        }
        let mount = self.workspace_mount_path(name)?;
        crate::git::GitRepository::from_root(&mount)
            .repair_merge_drivers()
            .await?;
        if name.is_main() {
            return Ok(());
        }
        if is_git_worktree(&workspace.metadata) {
            return repair_git_worktree_link(main_mount, &mount).await;
        }
        crate::git::GitRepository::from_root(&mount)
            .configure_main_remote(main_mount)
            .await
            .map(|_| ())
    }

    /// Converge the recorded checkout path onto where the checkout is actually observed.
    ///
    /// `mv` is the sanctioned front door for moving a checkout; this is the safety net under it. A
    /// user who reaches the project through another spelling of its path — another case on a
    /// case-insensitive volume, a firmlinked parent — has broken nothing, because the spelling
    /// still resolves to main's volume. The record simply differs from the path the user uses, and
    /// every later answer that quotes it (`doctor`, the gateway inventory's project-root lookup, a
    /// cold open from the checkout directory) quotes a path the user does not.
    ///
    /// Convergence fires only when all of these hold, which together mean "the same checkout, spelt
    /// differently" and nothing else:
    ///
    /// - `observed` sits inside main's mount, so the caller really is in this project;
    /// - the checkout root above it resolves to main's mount and is not a symlink, so it is the
    ///   mountpoint itself and not some deeper directory or an alias of it
    ///   ([`crate::checkout::observed_checkout`]);
    /// - that root lies outside cowshed's own storage;
    /// - it differs from the record, so an agreeing record is never rewritten.
    async fn converge_checkout_record(&mut self, observed: &Path) -> Result<()> {
        let main = main_name();
        let current = self.current(&main).await?;
        if !matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Mounted { .. }
        ) {
            return Ok(());
        }
        let mount_point = self.workspace_mount_path(&main)?;
        let record = self.checkout_record()?;
        let store_root = self.descriptor.storage.store().to_path_buf();
        let observed = observed.to_owned();
        let probe_mount = mount_point.clone();
        let Some(checkout) = crate::storage::lifecycle::dispatch_blocking(move || {
            crate::checkout::observed_checkout(&observed, &probe_mount)
                .filter(|checkout| !checkout.starts_with(&store_root))
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("observed checkout task failed: {error}"))
        })?
        else {
            return Ok(());
        };
        let converge_record = record.clone();
        let converge_checkout = checkout.clone();
        let changed = crate::storage::lifecycle::dispatch_blocking(move || {
            converge_record.rewrite_project_root(&converge_checkout)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("checkout record task failed: {error}")))?
        .map_err(native_integrity_error)?;
        if !changed {
            return Ok(());
        }
        self.rebind_checkout(&checkout)?;
        // A hand-moved checkout invalidates every workspace's record of the project, not just
        // main's, so the convergence that repairs main's has to repair theirs in the same breath.
        // Reached only when the record actually changed: the guard above returns early otherwise,
        // so an agreeing project pays nothing for this.
        self.repair_workspace_records(&checkout).await
    }

    /// Refuse a checkout destination that cannot be moved onto, before anything is mutated.
    async fn validate_move_destination(&self, source: &Path, destination: &Path) -> Result<()> {
        if !destination.is_absolute()
            || destination
                .components()
                .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err(CowshedError::usage(
                format!(
                    "{} is not an absolute, resolved path",
                    destination.display()
                ),
                "pass an absolute destination path with no `..` segments",
            ));
        }
        if destination == source {
            return Err(CowshedError::usage(
                format!("the checkout is already at {}", source.display()),
                "nothing to move; run cowshed doctor to see whether a workspace's records lag",
            ));
        }
        if destination.starts_with(self.descriptor.storage.store())
            || self.descriptor.storage.store().starts_with(destination)
            || destination.starts_with(source)
        {
            return Err(CowshedError::usage(
                format!(
                    "{} overlaps cowshed storage or the current checkout",
                    destination.display()
                ),
                format!(
                    "choose a destination outside {} and outside the current checkout",
                    self.descriptor.storage.store().display()
                ),
            ));
        }
        let destination = destination.to_owned();
        crate::storage::lifecycle::dispatch_blocking(move || {
            require_vacant_move_destination(&destination)?;
            let parent = destination.parent().ok_or_else(|| {
                CowshedError::usage(
                    format!("{} has no parent directory", destination.display()),
                    "choose a destination inside an existing directory",
                )
            })?;
            if !parent.is_dir() {
                return Err(CowshedError::not_found(
                    format!("{} is not an existing directory", parent.display()),
                    "create the parent directory first",
                ));
            }
            Ok(())
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("checkout destination task failed: {error}"))
        })?
    }

    /// Detach main, rename its mountpoint directory, and leave the substrate rebound to the new
    /// path with nothing mounted.
    ///
    /// Split out because it is the half that has an inverse: if the rename fails, main is put back
    /// where it was and remounted, so a failed move is indistinguishable from one never attempted.
    async fn move_direct_mount(
        &mut self,
        current: &NativeWorkspace,
        source: &Path,
        destination: &Path,
    ) -> Result<()> {
        use crate::storage::lifecycle::{MountIntent, Substrate};

        let stopped = self.stop_supervisor(&main_name()).await?;
        self.substrate
            .unmount(&current.derived.workspace)
            .await
            .map_err(native_storage_error)?;
        let rename_source = source.to_owned();
        let rename_destination = destination.to_owned();
        let renamed = crate::storage::lifecycle::dispatch_blocking(move || {
            std::fs::rename(&rename_source, &rename_destination).map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot move the checkout mountpoint to {}: {error}",
                        rename_destination.display()
                    ),
                    "choose a destination on the same filesystem as the current checkout",
                )
            })
        })
        .await
        .map_err(|error| CowshedError::internal(format!("checkout rename task failed: {error}")))?;
        if let Err(error) = renamed {
            // Nothing moved. Restore the mount while fenced, then release our own listener
            // before restarting main; ensuring under that listener could only refuse or hang.
            let restored = self
                .substrate
                .ensure_mounted(&current.derived.workspace, MountIntent { browse: false })
                .await
                .map_err(native_storage_error);
            drop(stopped);
            let rollback = match restored {
                Ok(_) => self.ensure_supervisor(&main_name()).await.map(|_| ()),
                Err(rollback) => Err(rollback),
            };
            return match rollback {
                Ok(()) => Err(error),
                Err(rollback) => Err(CowshedError::new(
                    error.code,
                    format!("{}; restoring main also failed: {rollback}", error.message),
                    error.hint,
                )),
            };
        }
        Ok(())
    }

    /// Add `workspace`'s mount as a remote in main's repository, for `cowshed new --register`.
    ///
    /// Opt-in because it is host-side state that accumulates one entry per workspace: a
    /// coordinator minting and retiring all day would silt up the user's config.
    async fn register_workspace_in_main(&self, workspace: &WorkspaceName) -> Result<()> {
        let main_mount = self.workspace_mount_path(&main_name())?;
        let workspace_mount = self.workspace_mount_path(workspace)?;
        crate::git::GitRepository::from_root(&main_mount)
            .register_workspace_remote(workspace.as_str(), &workspace_mount)
            .await
    }

    /// Drop the host-side state `workspace` put in main's repository: its reverse remote, and its
    /// linked-worktree registration if it is a git-worktree workspace.
    ///
    /// Main being unmounted is not a failure: there is nothing to clean up in a repository that is
    /// not there, and `gc` re-runs this from the same revalidated retirement metadata that
    /// authorizes the rest of cleanup.
    async fn unregister_workspace_in_main(
        &self,
        workspace: &WorkspaceName,
        git_worktree: bool,
    ) -> Result<()> {
        if workspace.is_main() {
            return Ok(());
        }
        let main_mount = self.workspace_mount_path(&main_name())?;
        if !main_mount.join(".git").exists() {
            return Ok(());
        }
        let main = crate::git::GitRepository::from_root(&main_mount);
        main.unregister_workspace_remote(workspace.as_str()).await?;
        if git_worktree {
            main.unregister_linked_worktree(workspace.as_str()).await?;
        }
        Ok(())
    }

    /// Refuse a git-worktree operation while main is not mounted.
    ///
    /// The gitdir lives outside the workspace's volume, so with main detached the workspace has
    /// files and no repository: `git status` fails and everything built on it fails with it.
    /// Handing back a mount whose git is broken is worse than saying which command fixes it.
    async fn require_main_mounted_for_git_worktree(
        &mut self,
        workspace: &WorkspaceName,
    ) -> Result<()> {
        let main = self.current(&main_name()).await?;
        if matches!(
            main.derived.mount_state,
            crate::storage::lifecycle::MountState::Detached
        ) {
            return Err(CowshedError::conflict(
                format!(
                    "git-worktree workspace {workspace} needs main mounted: its repository lives in main"
                ),
                "cowshed attach main",
            ));
        }
        Ok(())
    }

    /// The commit `revision` names in the repository a clone of `source` inherits.
    ///
    /// Read at the source's mount, so a detached source is refused by name instead of being
    /// read as an empty directory. A revision that repository does not hold is refused here,
    /// before `create` journals anything, because no later replay could ever branch from it.
    async fn resolve_create_start(
        &self,
        source: &NativeWorkspace,
        source_name: &WorkspaceName,
        revision: &crate::api::dto::RevisionTarget,
    ) -> Result<GitOid> {
        let named = revision_target(revision);
        match source.derived.mount_state {
            crate::storage::lifecycle::MountState::Mounted { .. } => {}
            crate::storage::lifecycle::MountState::Detached => {
                return Err(CowshedError::conflict(
                    format!(
                        "--ref {named} is resolved in {source_name}'s repository, and {source_name} is detached"
                    ),
                    format!("cowshed attach {source_name}"),
                ));
            }
        }
        let mount = self.workspace_mount_path(source_name)?;
        git_optional_ref_oid(&mount, &format!("{named}^{{commit}}"))
            .await?
            .ok_or_else(|| {
                CowshedError::not_found(
                    format!("--ref {named} names no commit in {source_name}'s repository"),
                    format!("fetch {named} into {source_name}, or start from a revision it holds"),
                )
            })
    }

    fn snapshot(&self, workspace: &NativeWorkspace) -> Result<WorkspaceSnapshot> {
        let info = WorkspaceInfo::from_current_metadata(
            &workspace.derived,
            self.workspace_mount_path(workspace.derived.workspace.name())?,
            &workspace.metadata,
        )
        .map_err(native_integrity_error)?;
        Ok(WorkspaceSnapshot {
            info,
            grants: workspace.metadata.grants.clone(),
            lifecycle_revision: workspace.derived.workspace.revision().get(),
            topology_revision: workspace.derived.workspace.topology_revision().get(),
        })
    }

    async fn checkpoint_quota(&self, workspace: &WorkspaceName) -> Result<Option<CheckpointQuota>> {
        let path = self.layout.project().policy.clone();
        let workspace = workspace.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            read_project_policy(&path)
                .map(|policy| policy.checkpoint_quotas.get(&workspace).copied())
        })
        .await
        .map_err(|error| CowshedError::internal(format!("checkpoint quota task failed: {error}")))?
    }

    async fn enforce_checkpoint_quota(&self, workspace: &NativeWorkspace) -> Result<()> {
        use crate::storage::lifecycle::Substrate;

        let Some(quota) = self
            .checkpoint_quota(workspace.derived.workspace.name())
            .await?
        else {
            return Ok(());
        };
        let stats = self
            .substrate
            .stats(&workspace.derived.workspace)
            .await
            .map_err(native_storage_error)?;
        if stats.pinned_checkpoint_bytes > stats.checkpoint_bytes {
            return Err(CowshedError::integrity(
                "pinned checkpoint bytes exceed total checkpoint bytes",
                "run cowshed doctor --json",
            ));
        }
        let projected_count = stats.checkpoint_count.checked_add(1).ok_or_else(|| {
            CowshedError::integrity("checkpoint count overflow", "run cowshed gc")
        })?;
        let projected_bytes = stats
            .checkpoint_bytes
            .checked_add(stats.allocated_bytes)
            .ok_or_else(|| {
                CowshedError::integrity("checkpoint byte accounting overflow", "run cowshed gc")
            })?;
        if projected_count > u64::from(quota.max_count) || projected_bytes > quota.max_bytes {
            return Err(CowshedError::conflict(
                format!(
                    "checkpoint quota exceeded for {}: projected {projected_count} checkpoints and {projected_bytes} bytes, limit {} checkpoints and {} bytes",
                    workspace.derived.workspace.name(),
                    quota.max_count,
                    quota.max_bytes
                ),
                "remove or unpin checkpoints, raise the workspace quota, or run cowshed gc",
            ));
        }
        Ok(())
    }

    async fn operation_identity(
        &self,
        grants: GrantSet,
        branch: Option<String>,
        forked_from: Option<WorkspaceName>,
        git_worktree: bool,
    ) -> Result<crate::storage::lifecycle::OperationIdentity> {
        Ok(crate::storage::lifecycle::OperationIdentity {
            project_root: self.descriptor.git_root.to_path_buf(),
            base_commit: self.git.head_oid().await?.as_str().to_owned(),
            // One clock for the runtime module. Spawning `/bin/date` was a process, a pipe, and
            // a UTF-8 parse to render what `SystemTime` already holds.
            created_at: super::supervisor::utc_now()?.as_str().to_owned(),
            branch,
            forked_from,
            created_trace: uuid::Uuid::new_v4().simple().to_string(),
            grants,
            git_worktree,
        })
    }

    async fn removal_git_fence(
        &self,
        workspace: &NativeWorkspace,
    ) -> Result<NativeRemovalGitFence> {
        self.removal_git_fence_at(
            &current_snapshot_mount(self, workspace)?,
            workspace.derived.workspace.incarnation(),
        )
        .await
    }

    async fn removal_git_fence_at(
        &self,
        mount: &Path,
        incarnation: &WorkspaceIncarnation,
    ) -> Result<NativeRemovalGitFence> {
        let git = crate::git::GitRepository::from_root(mount);
        let head = git.head_oid().await?;
        Ok(NativeRemovalGitFence {
            incarnation: incarnation.clone(),
            head,
            dirty: git
                .is_dirty_by(Some(&self.substrate_config.checkout_path))
                .await?,
            in_progress: git.in_progress_operation().await?,
        })
    }

    /// Every gate a removal must pass, in the order that puts the cheapest refusal first.
    ///
    /// Answers `Some` when an authorized abandonment has commits to bundle. Run twice per removal —
    /// once before the supervisor stops and again on the revalidated fence — because both halves of
    /// the answer can move underneath a removal: the workspace can pick up work, and main can land
    /// or rewind it.
    async fn require_removal_safe(
        &self,
        workspace: &WorkspaceName,
        options: RemoveOptions,
        fence: &NativeRemovalGitFence,
        containment: &NativeContainment,
    ) -> Result<Option<NativeLandedState>> {
        if workspace.is_main() {
            // Main's removal has no landed gate: main *is* the branch a session has to reach, and
            // its own preservation proof is the retained checkout or a remote ref, which the
            // `--restore` path checks. What remains here is transient state — and unlike a session,
            // main's is not overridable, because there is no fork of it to fall back on.
            Self::require_session_state_clean(workspace, fence)?;
            return Ok(None);
        }
        if !options.force {
            Self::require_session_state_clean(workspace, fence)?;
        }
        self.require_session_landed(workspace, fence, options.abandon, containment)
            .await
    }

    /// Stop the supervisor, then prove the workspace is still the one that was checked.
    ///
    /// The gap between a safety decision and the deletion it authorizes is where a workspace can
    /// pick up a commit, so both the incarnation and the head are re-read *after* the only thing
    /// that could still be writing to the volume has stopped.
    async fn revalidated_removal_fence(
        &mut self,
        workspace: &WorkspaceName,
        initial: &NativeRemovalGitFence,
        force: bool,
    ) -> Result<(
        NativeWorkspace,
        NativeRemovalGitFence,
        super::supervisor_socket::BoundSocket,
    )> {
        let stopped = self.stop_supervisor_for_removal(workspace, force).await?;
        require_lost_groups_released(workspace, &stopped, "remove").await?;
        let current = self.current(workspace).await?;
        Self::require_exact_incarnation(&current, &initial.incarnation)?;
        let fence = self.removal_git_fence(&current).await?;
        if fence.head != initial.head {
            return Err(removal_head_moved_refusal(
                workspace,
                &initial.head,
                &fence.head,
            ));
        }
        Ok((current, fence, stopped))
    }

    /// Drop the workspace's host-side registrations, then retire its image.
    ///
    /// Host-side state goes before the image does. A remote naming a trashed mount is a broken
    /// fetch in the user's own checkout — the one piece of this teardown that lives where they can
    /// see it.
    ///
    /// The checkout's Nx daemon is stopped as `detach` stops it before the image's volume is
    /// unmounted. Stopping the supervisor does not end it: the daemon detaches from the job that
    /// started it, and its cwd is the checkout, so every removal of a shed waited out the whole
    /// unmount grace and then forced (measured: 43 refused unmounts, 12.8 s, per `rm`). Anything
    /// else holding the volume is still the unmount's to wait for and force, as removal always
    /// has; it is named here instead of refused.
    async fn finish_retirement(&mut self, current: NativeWorkspace) -> Result<()> {
        let workspace = current.derived.workspace.name().clone();
        if matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Mounted { .. }
        ) {
            let mount = self.workspace_mount_path(&workspace)?;
            if let Err(busy) = close_checkout_nx(&workspace, &mount).await? {
                eprintln!(
                    "cowshed: {workspace}'s Nx state is in use: {busy}; its unmount waits, then forces"
                );
            }
        }
        let git_worktree = is_git_worktree(&current.metadata);
        self.unregister_workspace_in_main(&workspace, git_worktree)
            .await?;
        self.retire_workspace(current).await
    }

    /// Refuse a session removal whose workspace is in transient Git state.
    ///
    /// Transient means recoverable-by-hand: uncommitted edits, a half-finished merge. This is the
    /// class `--force` overrides, and the hint names only remedies that lose nothing — naming the
    /// override here is what taught coordinator scripts to reach for it by reflex.
    fn require_session_state_clean(
        workspace: &WorkspaceName,
        fence: &NativeRemovalGitFence,
    ) -> Result<()> {
        if let Some(operation) = fence.in_progress.as_deref() {
            return Err(removal_in_progress_refusal(workspace, operation));
        }
        if fence.dirty {
            return Err(removal_dirty_refusal(workspace));
        }
        Ok(())
    }

    /// Main as the branch a session's work has to reach: what `rm` measures against.
    fn main_containment(&self) -> Result<NativeContainment> {
        Ok(NativeContainment {
            mount: self.workspace_mount_path(&main_name())?,
            branch: DEFAULT_LANDING_BRANCH.to_owned(),
        })
    }

    /// Resolve what `unit` lands into, or rebases onto: main when `into` is `None`, otherwise the
    /// named workspace, but only at the incarnation `into` was resolved at and never `unit`
    /// itself. A detached target is attached, since delivering into it means running Git in it.
    async fn landing_into(
        &mut self,
        unit: &WorkspaceName,
        into: Option<WorkspaceTarget>,
    ) -> Result<NativeLandingInto> {
        let main = || -> Result<NativeLandingInto> {
            Ok(NativeLandingInto {
                name: main_name(),
                root: self.descriptor.git_root.to_path_buf(),
                mount: self.workspace_mount_path(&main_name())?,
            })
        };
        let Some(into) = into else {
            return main();
        };
        require_distinct_target(unit, into.workspace())?;
        let current = self.current(into.workspace()).await?;
        require_target_incarnation(
            into.workspace(),
            current.derived.workspace.incarnation(),
            into.incarnation(),
        )?;
        if into.workspace().is_main() {
            return main();
        }
        if matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Detached
        ) {
            self.attach(into.workspace().clone(), AttachOptions::default())
                .await?;
        }
        let mount = self.workspace_mount_path(into.workspace())?;
        Ok(NativeLandingInto {
            name: into.workspace().clone(),
            root: mount.clone(),
            mount,
        })
    }

    /// Where a session's commits stand relative to the branch that has to hold them.
    ///
    /// The target tip is read out of *the target's own repository* — main's, or the lane base a
    /// unit landed into: the object store that survives this workspace — and never out of a
    /// `refs/remotes/*` cache inside the workspace, which is a clone-time snapshot that has been
    /// observed hundreds of commits stale. The comparison then runs inside the workspace with the
    /// target's object store attached read-only, so its commits are visible without fetching and
    /// without writing anything anywhere.
    ///
    /// Containment is by patch identity, not only by ancestry. That is the correction this gate
    /// needed: a workspace whose work reached main by squash-merge or a history rewrite is not an
    /// ancestor of anything, and demanding `--abandon` to retire it taught callers to pass a
    /// commit-destroying flag for a safe operation.
    async fn landed_state(
        &self,
        workspace: &WorkspaceName,
        head: &GitOid,
        containment: &NativeContainment,
    ) -> Result<NativeLandedState> {
        let target = crate::landing::resolve_target(&containment.mount, &containment.branch).await;
        let mount = self.workspace_mount_path(workspace)?;
        Ok(NativeLandedState {
            branch: containment.branch.clone(),
            commits: crate::landing::measure_commits(&target, &mount, head.as_str()).await,
        })
    }

    /// Refuse a session removal that would destroy commits the containment target does not hold.
    ///
    /// `--abandon` is the only authorization: `--force` covers transient state and deliberately
    /// stops there, so a script that carries `--force` to get past a stuck workspace cannot also
    /// delete work with no other home. Answers `Some` when the caller authorized an abandonment
    /// and there is genuinely something to abandon, so the caller can bundle it before deleting.
    async fn require_session_landed(
        &self,
        workspace: &WorkspaceName,
        fence: &NativeRemovalGitFence,
        abandon: bool,
        containment: &NativeContainment,
    ) -> Result<Option<NativeLandedState>> {
        let landed = self
            .landed_state(workspace, &fence.head, containment)
            .await?;
        removal_landed_decision(workspace, &fence.head, landed, abandon)
    }

    /// Write and verify the commits that are about to lose their only workspace ref.
    ///
    /// The live main tip is both the report's baseline and a positive revision in the bundle.
    /// Carrying both histories makes the bundle self-contained even after main was rewritten, and
    /// lets an empty recovery repository reconstruct the exact `main_tip..HEAD` range the report
    /// counted. The landing gate keeps its patch-identity policy; this artifact count is the
    /// conservative oid range whose objects are actually being retired.
    async fn bundle_abandoned_work(
        &self,
        workspace: &WorkspaceName,
        fence: &NativeRemovalGitFence,
        landed: NativeLandedState,
        containment: &NativeContainment,
    ) -> Result<AbandonedWork> {
        let mount = self.workspace_mount_path(workspace)?;
        let target_head = landed.commits.target_head().cloned();
        let git = crate::git::GitRepository::from_root(&mount);
        let git = match target_head.as_ref() {
            Some(_) => {
                let target_objects = crate::git::GitRepository::from_root(&containment.mount)
                    .object_directory()
                    .await?;
                git.with_alternate_objects(target_objects)?
            }
            None => git,
        };
        let trash = self
            .layout
            .project()
            .sessions
            .join(crate::storage::recovery::TRASH_NAMESPACE);
        let bundle = trash.join(format!("{}-{}.bundle", workspace.as_str(), fence.head));
        let directory = trash.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            std::fs::create_dir_all(&directory).map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot create the retirement trash directory {}: {error}",
                        directory.display()
                    ),
                    "repair the cowshed store and retry",
                )
            })
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("trash directory task failed: {error}"))
        })??;
        // `HEAD`, not the fence oid, is load-bearing: a raw oid does not advertise a fetchable ref
        // in a bundle. The fence already proved HEAD is exactly `fence.head`, and bundle creation
        // verifies that exact tip and commit range by fetching it into an empty repository.
        let unlanded_commits = git
            .bundle_commits(&bundle, target_head.as_ref().map(GitOid::as_str), "HEAD")
            .await?;
        Ok(AbandonedWork {
            head: fence.head.clone(),
            target_branch: landed.branch,
            target_head,
            unlanded_commits,
            bundle,
        })
    }

    /// Remove `workspace`, gating a session's destruction on `containment` holding its commits:
    /// main for `rm`, the branch a unit just landed on for `land`'s retire.
    async fn remove_contained_in(
        &mut self,
        workspace: WorkspaceName,
        options: RemoveOptions,
        containment: &NativeContainment,
    ) -> Result<RemoveReport> {
        use crate::storage::lifecycle::{MountIntent, MountState, Substrate};

        if options.restore && options.force {
            return Err(CowshedError::usage(
                "--force and --restore select conflicting main removal modes",
                "choose exactly one main removal mode",
            ));
        }
        if options.restore && !workspace.is_main() {
            return Err(CowshedError::usage(
                "--restore is only valid for the adopted main workspace",
                "remove a session without --restore",
            ));
        }
        // `--abandon` authorizes destroying commits nothing else holds. On a session that is
        // commits the project's main branch does not contain; main *is* that branch, so on main
        // the flag authorizes only what a restore would otherwise refuse to lose, and without
        // `--restore` it would let a script carry one spelling for both and lose main to a typo.
        if options.abandon && workspace.is_main() && !options.restore {
            return Err(CowshedError::usage(
                "--abandon on main needs --restore: it keeps main's unpreserved commits in a \
                 bundle inside the restored checkout",
                "cowshed rm main --restore --abandon",
            ));
        }
        if workspace.is_main() && !options.restore && !options.force {
            return Err(main_removal_mode_refusal());
        }
        self.validate_binding().await?;
        let intent = crate::storage::recovery::LifecycleIntent::Retire {
            workspace: workspace.clone(),
            options,
            origin: None,
        };
        let was_pending = self
            .lifecycle_intents
            .get(&workspace)
            .is_some_and(|record| record.operation == intent && record.completion.is_none());
        if let Some(report) = self.completed_retire_intent(&intent).cloned()
            && self
                .current(&workspace)
                .await
                .is_err_and(|error| error.code == ErrorCode::NotFound)
        {
            return Ok(report);
        }

        let mut current = match self.current(&workspace).await {
            Err(error) if error.code == ErrorCode::NotFound && !workspace.is_main() => {
                if let Some((image, metadata)) = self
                    .pending_metadata()
                    .await?
                    .into_iter()
                    .find(|(_, metadata)| metadata.workspace == workspace)
                {
                    let unfinished = self
                        .lifecycle_intents
                        .get(&workspace)
                        .filter(|record| record.completion.is_none())
                        .map(|record| record.operation.clone());
                    let origin = match unfinished {
                        // No intent names this clone, so nothing will ever finish it; once no
                        // process is still creating it, it is retired from its own metadata.
                        None => {
                            if !self.claim_intent_lease(&workspace)?
                                || image_lifecycle_lock_is_held(&image)?
                            {
                                return Err(another_process_is_running(&workspace));
                            }
                            abandoned_clone_origin(&metadata)
                        }
                        Some(
                            operation @ (crate::storage::recovery::LifecycleIntent::Create {
                                ..
                            }
                            | crate::storage::recovery::LifecycleIntent::Fork { .. }),
                        ) => operation,
                        Some(crate::storage::recovery::LifecycleIntent::Retire {
                            options: original,
                            origin: Some(origin),
                            ..
                        }) if original == options => *origin,
                        Some(_) => {
                            return Err(CowshedError::integrity(
                                format!(
                                    "pending workspace {workspace} has no unfinished matching lifecycle intent"
                                ),
                                "cowshed doctor --json",
                            ));
                        }
                    };
                    let (report, retired) = self
                        .retire_pending_workspace(&workspace, options, origin, metadata)
                        .await?;
                    self.reclaim_in_background(retired);
                    return Ok(report);
                }
                // An unfinished create or fork that left no clone — refused after binding its
                // slot, or stopped before the clone's first write — has only its slot and its
                // intent to retire. Under the intent lease no live creator is mid-clone, and the
                // lookup is repeated there so a clone staged since is left for a retry to retire
                // rather than orphaned. The retirement supersedes the clone intent, so no later
                // open replays it.
                if self
                    .lifecycle_intents
                    .get(&workspace)
                    .is_some_and(|record| {
                        record.completion.is_none()
                            && matches!(
                                record.operation,
                                crate::storage::recovery::LifecycleIntent::Create { .. }
                                    | crate::storage::recovery::LifecycleIntent::Fork { .. }
                            )
                    })
                {
                    if !self.claim_intent_lease(&workspace)? {
                        return Err(another_process_is_running(&workspace));
                    }
                    if self
                        .pending_metadata()
                        .await?
                        .iter()
                        .any(|(_, metadata)| metadata.workspace == workspace)
                    {
                        return Err(CowshedError::conflict(
                            format!(
                                "an unfinished clone of {workspace} was staged while rm read it"
                            ),
                            format!("cowshed rm {workspace}"),
                        ));
                    }
                    self.begin_lifecycle_intent(intent).await?;
                    self.mark_lifecycle_intent_mutating(&workspace).await?;
                    self.release_slot(&workspace).await?;
                    let report = RemoveReport::default();
                    self.complete_lifecycle_intent(
                        &workspace,
                        crate::storage::recovery::LifecycleIntentCompletion::Retire(report.clone()),
                    )
                    .await?;
                    return Ok(report);
                }
                if was_pending {
                    let report = RemoveReport::default();
                    self.complete_lifecycle_intent(
                        &workspace,
                        crate::storage::recovery::LifecycleIntentCompletion::Retire(report.clone()),
                    )
                    .await?;
                    return Ok(report);
                }
                return Err(error);
            }
            Ok(current) => current,
            // A restore's retry reaches here once main is retired. It must finish the unbinding,
            // so it is tried before the pending-intent completion below, which would answer the
            // retry with success while the binding still stands.
            Err(error) if options.restore && error.code == ErrorCode::NotFound => {
                // The binding's absence is the restore's completion. Recovery at open replays a
                // pending restore, so this very process may already have finished it and removed
                // the project directory with the binding; there is nothing left to record or
                // journal, and writing either would recreate that directory.
                if !self.project_is_bound().await? {
                    return Ok(RemoveReport::default());
                }
                let pre_cowshed = pre_cowshed_path(&self.descriptor.git_root)?;
                let pre_cowshed_absent = match tokio::fs::symlink_metadata(&pre_cowshed).await {
                    Ok(_) => false,
                    Err(inspect) if inspect.kind() == std::io::ErrorKind::NotFound => true,
                    Err(inspect) => {
                        return Err(CowshedError::environment_missing(
                            format!(
                                "cannot inspect retained checkout {}: {inspect}",
                                pre_cowshed.display()
                            ),
                            "check parent-directory permissions and retry",
                        ));
                    }
                };
                let restored = pre_cowshed_absent
                    && self
                        .verify_checkout_identity(
                            &self.descriptor.git_root,
                            "restored project checkout",
                        )
                        .await
                        .is_ok();
                if restored {
                    // This open may have been the one that reclaimed main's retired image, and
                    // with it the last image naming this checkout; a restore stranded before the
                    // record existed gets it here, before anything else can fail.
                    self.record_checkout_root().await?;
                    if !was_pending {
                        self.begin_lifecycle_intent(intent.clone()).await?;
                    }
                    self.mark_lifecycle_intent_mutating(&workspace).await?;
                    self.unbind_restored_project().await?;
                    return Ok(RemoveReport::default());
                }
                return Err(error);
            }
            Err(error) if error.code == ErrorCode::NotFound && was_pending => {
                let report = RemoveReport::default();
                self.complete_lifecycle_intent(
                    &workspace,
                    crate::storage::recovery::LifecycleIntentCompletion::Retire(report.clone()),
                )
                .await?;
                return Ok(report);
            }
            Err(error) => return Err(error),
        };
        if workspace.is_main() {
            self.record_checkout_root().await?;
        }

        if options.restore {
            let pre_cowshed = pre_cowshed_path(&self.descriptor.git_root)?;
            let project_root = self.layout.project().project_root.clone();
            crate::storage::lifecycle::dispatch_blocking(move || {
                require_terminal_storage(&project_root)
            })
            .await
            .map_err(|error| {
                CowshedError::internal(format!("terminal storage check failed: {error}"))
            })??;
            let initial_rollback_state = self.adopt_rollback_state(&current, &pre_cowshed).await?;
            let mut abandoned = None;
            if !options.force && initial_rollback_state != NativeAdoptRollbackState::Complete {
                self.verify_checkout_identity(&pre_cowshed, "retained pre-cowshed checkout")
                    .await?;
                let initially_detached =
                    matches!(current.derived.mount_state, MountState::Detached);
                if initially_detached {
                    self.substrate
                        .ensure_mounted(&current.derived.workspace, MountIntent { browse: false })
                        .await
                        .map_err(native_storage_error)?;
                    current = self.current(&workspace).await?;
                }
                let preserved = match self.main_restore_preservation(&current, &pre_cowshed).await {
                    Ok(MainPreservation::Preserved) => Ok(()),
                    Ok(MainPreservation::Unpreserved {
                        head,
                        retained_head,
                    }) if options.abandon => self
                        .bundle_abandoned_main(&current, &pre_cowshed, head, retained_head)
                        .await
                        .map(|work| abandoned = Some(work)),
                    Ok(MainPreservation::Unpreserved { head, .. }) => Err(CowshedError::conflict(
                        format!(
                            "main head {head} is not preserved by the retained checkout or a \
                                 remote ref"
                        ),
                        "push main to its remote so its commits survive, or keep them only \
                             as a bundle in the restored checkout: cowshed rm main --restore \
                             --abandon",
                    )),
                    Err(error) => Err(error),
                };
                if let Err(error) = preserved {
                    if initially_detached {
                        self.substrate
                            .unmount(&current.derived.workspace)
                            .await
                            .map_err(native_storage_error)?;
                    }
                    return Err(error);
                }
            }
            let incarnation = current.derived.workspace.incarnation().clone();
            let stopped = self.stop_supervisor(&workspace).await?;
            require_lost_groups_released(&workspace, &stopped, "restore main").await?;
            let current = self.current(&workspace).await?;
            Self::require_exact_incarnation(&current, &incarnation)?;
            let rollback_state = self.adopt_rollback_state(&current, &pre_cowshed).await?;
            if !was_pending {
                self.begin_lifecycle_intent(intent.clone()).await?;
            }
            self.mark_lifecycle_intent_mutating(&workspace).await?;
            if rollback_state != NativeAdoptRollbackState::Complete {
                self.substrate
                    .restore_adopted_checkout(&current.derived.workspace, &pre_cowshed)
                    .await
                    .map_err(native_storage_error)?;
            }
            let current = self.current(&workspace).await?;
            Self::require_exact_incarnation(&current, &incarnation)?;
            self.verify_checkout_identity(&self.descriptor.git_root, "restored project checkout")
                .await?;
            if tokio::fs::symlink_metadata(&pre_cowshed).await.is_ok() {
                return Err(CowshedError::integrity(
                    "pre-cowshed path remains after atomic checkout restoration",
                    "retry adoption rollback before removing project state",
                ));
            }
            self.retire_restored_main(current).await?;
            self.unbind_restored_project().await?;
            return Ok(RemoveReport { abandoned });
        }

        let initially_detached = matches!(current.derived.mount_state, MountState::Detached);
        if initially_detached {
            self.substrate
                .ensure_mounted(&current.derived.workspace, MountIntent { browse: false })
                .await
                .map_err(native_storage_error)?;
        }
        // The landed proof lives in main's repository, so main has to be readable for the whole
        // removal — including the revalidation after the supervisor stops. A project whose main is
        // detached still gets the proof: main is mounted for the duration and put back as found,
        // because the answer to "would this destroy work" must not depend on mount posture.
        let main_initially_detached = !workspace.is_main() && {
            let main = self.current(&main_name()).await?;
            let detached = matches!(main.derived.mount_state, MountState::Detached);
            if detached {
                self.substrate
                    .ensure_mounted(&main.derived.workspace, MountIntent { browse: false })
                    .await
                    .map_err(native_storage_error)?;
            }
            detached
        };

        let removal = async {
            let current = self.current(&workspace).await?;
            let initial_fence = self.removal_git_fence(&current).await?;
            self.require_removal_safe(&workspace, options, &initial_fence, containment)
                .await?;
            let (current, final_fence, _stopped) = self
                .revalidated_removal_fence(&workspace, &initial_fence, options.force)
                .await?;
            let abandoning = self
                .require_removal_safe(&workspace, options, &final_fence, containment)
                .await?;
            if !was_pending {
                self.begin_lifecycle_intent(intent).await?;
            }
            self.mark_lifecycle_intent_mutating(&workspace).await?;
            // Creation and an empty-repository fetch both finish before anything is destroyed: a
            // preservation artifact that has not proved its own recoverability authorizes nothing.
            let abandoned = match abandoning {
                Some(landed) => Some(
                    self.bundle_abandoned_work(&workspace, &final_fence, landed, containment)
                        .await?,
                ),
                None => None,
            };
            self.finish_retirement(current).await?;
            // The workspace's build volume and seed retire with it, unless a target adopted the
            // volume (16_build_volumes.md, "Garbage collection"), and with them whatever else no
            // checkout links.
            self.collect_build_volumes().await?;
            Ok(RemoveReport { abandoned })
        }
        .await;

        if main_initially_detached {
            let main = self.current(&main_name()).await?;
            let _stopped = self.stop_supervisor(&main_name()).await?;
            self.substrate
                .unmount(&main.derived.workspace)
                .await
                .map_err(native_storage_error)?;
        }
        let report = match removal {
            Ok(report) => report,
            Err(primary) => {
                let cleanup = match self.current(&workspace).await {
                    Ok(current) if initially_detached => self
                        .substrate
                        .unmount(&current.derived.workspace)
                        .await
                        .map_err(native_storage_error),
                    Ok(_) => self.ensure_supervisor(&workspace).await.map(|_| ()),
                    Err(_) => Ok(()),
                };
                return match cleanup {
                    Ok(()) => Err(primary),
                    Err(cleanup) => Err(CowshedError::internal(format!(
                        "workspace removal failed: {primary}; state restoration also failed: {cleanup}"
                    ))),
                };
            }
        };
        self.complete_lifecycle_intent(
            &workspace,
            crate::storage::recovery::LifecycleIntentCompletion::Retire(report.clone()),
        )
        .await?;
        Ok(report)
    }

    /// Whether main's head survives the restore: held by the retained checkout (a branch or a
    /// Cowshed preservation ref) or by a remote-tracking ref. Transient Git work refuses outright.
    async fn main_restore_preservation(
        &self,
        workspace: &NativeWorkspace,
        pre_cowshed_checkout: &Path,
    ) -> Result<MainPreservation> {
        let fence = self.removal_git_fence(workspace).await?;
        if fence.dirty || fence.in_progress.is_some() {
            return Err(CowshedError::conflict(
                "main has uncommitted or in-progress Git work",
                "commit or discard the changes, then retry",
            ));
        }
        let current_git =
            crate::git::GitRepository::from_root(current_snapshot_mount(self, workspace)?);
        let retained_git = crate::git::GitRepository::discover(pre_cowshed_checkout)
            .await
            .map_err(|_| {
                CowshedError::conflict(
                    "retained pre-cowshed checkout cannot prove main commit preservation",
                    "restore the exact retained checkout, or push main to its remote, then retry",
                )
            })?;
        let preserved_locally = retained_git
            .commit_is_preserved(fence.head.as_str())
            .await?;
        let preserved_remotely = current_git
            .commit_is_remote_preserved(fence.head.as_str())
            .await?;
        if preserved_locally || preserved_remotely {
            return Ok(MainPreservation::Preserved);
        }
        // A retained checkout with no resolvable HEAD (an unborn branch) only widens an
        // abandonment's bundle to main's whole history; it never makes a restore lose commits.
        // Any other failure to read that HEAD is a broken repository, and says so.
        let retained_head = git_optional_ref_oid(retained_git.root(), "HEAD").await?;
        Ok(MainPreservation::Unpreserved {
            head: fence.head,
            retained_head,
        })
    }

    /// Keeps main's unpreserved commits where the restore puts them: a bundle in the retained
    /// checkout's Git directory, which the restore makes the project checkout's own. The bundle
    /// carries the retained head as well, so it fetches into an empty repository, and it is
    /// verified that way before anything is restored. Git names that directory: a linked worktree
    /// keeps it outside the tree, where the swap leaves it, rather than under `.git`.
    async fn bundle_abandoned_main(
        &self,
        workspace: &NativeWorkspace,
        pre_cowshed_checkout: &Path,
        head: GitOid,
        retained_head: Option<GitOid>,
    ) -> Result<AbandonedWork> {
        let main_git =
            crate::git::GitRepository::from_root(current_snapshot_mount(self, workspace)?);
        let base = match retained_head {
            Some(retained)
                if main_git
                    .commit_is_ancestor(retained.as_str(), head.as_str())
                    .await? =>
            {
                Some(retained)
            }
            _ => None,
        };
        let name = format!("abandoned-main-{head}.bundle");
        let git_dir = invoke_git(
            pre_cowshed_checkout,
            &["rev-parse", "--path-format=absolute", "--git-dir"],
        )
        .await?;
        require_git_success("locate the retained checkout's Git directory", &git_dir)?;
        let git_dir = {
            use std::os::unix::ffi::OsStringExt;
            let mut bytes = git_dir.stdout;
            while bytes.last().is_some_and(|byte| *byte == b'\n') {
                bytes.pop();
            }
            PathBuf::from(std::ffi::OsString::from_vec(bytes))
        };
        let staged_directory = git_dir.join("cowshed");
        // The report names where the bundle is once the restore has swapped the trees.
        let retained_root = tokio::fs::canonicalize(pre_cowshed_checkout)
            .await
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot resolve the retained checkout {}: {error}",
                        pre_cowshed_checkout.display()
                    ),
                    "check parent-directory permissions and retry",
                )
            })?;
        let final_directory = match staged_directory.strip_prefix(&retained_root) {
            Ok(inside) => self.descriptor.git_root.join(inside),
            Err(_) => staged_directory.clone(),
        };
        let target_branch = crate::git::GitRepository::from_root(pre_cowshed_checkout)
            .current_branch()
            .await?
            .unwrap_or_else(|| "HEAD".to_owned());
        let directory = staged_directory.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            std::fs::create_dir_all(&directory).map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot create the abandoned-main directory {}: {error}",
                        directory.display()
                    ),
                    "make the retained checkout's Git directory writable and retry",
                )
            })
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("abandoned-main directory task failed: {error}"))
        })??;
        // `HEAD`, not the fence oid: a raw oid advertises no fetchable ref in a bundle, and the
        // fence already proved main's HEAD is exactly `head`.
        let unlanded_commits = main_git
            .bundle_commits(
                &staged_directory.join(&name),
                base.as_ref().map(GitOid::as_str),
                "HEAD",
            )
            .await?;
        Ok(AbandonedWork {
            head,
            target_branch,
            target_head: base,
            unlanded_commits,
            bundle: final_directory.join(name),
        })
    }

    async fn verify_checkout_identity(&self, path: &Path, description: &str) -> Result<()> {
        let path_metadata = tokio::fs::symlink_metadata(path).await.map_err(|_| {
            CowshedError::conflict(
                format!("{description} is not the exact retained checkout directory"),
                "restore the exact .pre-cowshed tree or move the collision aside",
            )
        })?;
        let main_mount = self.workspace_mount_path(&main_name())?;
        let resolved =
            resolve_checkout_identity_path(path, &path_metadata, &main_mount, description).await?;
        let path = resolved.as_path();
        let git = crate::git::GitRepository::discover(path)
            .await
            .map_err(|_| {
                CowshedError::conflict(
                    format!("{description} is not the retained standalone Git checkout"),
                    "restore the exact .pre-cowshed tree or move the collision aside",
                )
            })?;
        let candidate_root = tokio::fs::canonicalize(path).await.map_err(|_| {
            CowshedError::conflict(
                format!("{description} cannot be resolved as an exact checkout root"),
                "restore the exact .pre-cowshed tree or move the collision aside",
            )
        })?;
        let discovered_root = tokio::fs::canonicalize(git.root()).await.map_err(|_| {
            CowshedError::conflict(
                format!("{description} has no resolvable Git root"),
                "restore the exact .pre-cowshed tree or move the collision aside",
            )
        })?;
        if candidate_root != discovered_root {
            return Err(CowshedError::conflict(
                format!("{description} is nested inside another checkout"),
                "restore the exact .pre-cowshed checkout root and retry",
            ));
        }
        let binding = binding_from_git(&git, Some(&self.descriptor.repo_id)).await?;
        if binding.primary().map_err(native_integrity_error)?.repo_id != self.descriptor.repo_id {
            return Err(CowshedError::conflict(
                format!("{description} belongs to a different repository"),
                "move the unrelated path aside and retry",
            ));
        }
        Ok(())
    }

    /// Unbinds a project whose main was restored: the terminal storage, the controller's own
    /// state (lifecycle journal included) and the mount tree go first, and the binding last, so a
    /// process that dies part way leaves a project still bound and a retry that finishes the
    /// job. The binding's absence is the removal's completion. The checkout root record goes right
    /// after it — it is what reopens a still-bound project whose main image is gone — and the store
    /// directory and emptied owner directories after that.
    async fn unbind_restored_project(&self) -> Result<()> {
        let paths = self.layout.project().clone();
        let expected = self.descriptor.binding.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            let path = &paths.repository_binding;
            let bound = match std::fs::symlink_metadata(path) {
                Ok(metadata) => Some(metadata),
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
                Err(error) => {
                    return Err(CowshedError::environment_missing(
                        format!(
                            "cannot inspect repository binding {}: {error}",
                            path.display()
                        ),
                        "check controller storage permissions and retry",
                    ));
                }
            };
            if let Some(metadata) = bound {
                if !metadata.file_type().is_file() {
                    return Err(CowshedError::integrity(
                        format!(
                            "repository binding is not a regular file: {}",
                            path.display()
                        ),
                        "move the collision aside and retry",
                    ));
                }
                let actual = crate::metadata::read_json::<RepositoryBinding>(path)
                    .map_err(native_integrity_error)?;
                if &actual != expected.as_ref() {
                    return Err(CowshedError::integrity(
                        "repository binding changed during adoption rollback",
                        "restore the exact binding and retry",
                    ));
                }
                clean_terminal_project_storage(&paths.project_root, path)?;
                remove_unbound_project_state(&paths)?;
                std::fs::remove_file(path).map_err(|error| {
                    CowshedError::environment_missing(
                        format!(
                            "cannot remove repository binding {}: {error}",
                            path.display()
                        ),
                        "check controller storage permissions and retry",
                    )
                })?;
                std::fs::File::open(&paths.project_root)
                    .and_then(|directory| directory.sync_all())
                    .map_err(|error| {
                        CowshedError::environment_missing(
                            format!(
                                "cannot sync repository binding directory {}: {error}",
                                paths.project_root.display()
                            ),
                            "check controller storage permissions and retry",
                        )
                    })?;
            }
            remove_unbound_file(&paths.checkout_root)?;
            remove_directory_if_empty(&paths.project_root)?;
            match paths.project_root.parent() {
                Some(owner) => remove_directory_if_empty(owner),
                None => Ok(()),
            }
        })
        .await
        .map_err(|error| CowshedError::internal(format!("binding cleanup task failed: {error}")))?
    }

    /// Main's images are the record of where the checkout is, and removing main takes them away.
    /// The root goes into the store first, so a removal that dies before it unbinds leaves a
    /// project its checkout still reopens, remote or no remote.
    async fn record_checkout_root(&self) -> Result<()> {
        let layout = self.layout.clone();
        let checkout_root = self.descriptor.git_root.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            layout.record_checkout_root(&checkout_root)
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("checkout root record task failed: {error}"))
        })?
        .map_err(native_integrity_error)
    }

    /// Whether the project's binding still stands; its absence is an unbinding's completion.
    async fn project_is_bound(&self) -> Result<bool> {
        let binding = &self.layout.project().repository_binding;
        match tokio::fs::symlink_metadata(binding).await {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(CowshedError::environment_missing(
                format!(
                    "cannot inspect repository binding {}: {error}",
                    binding.display()
                ),
                "check controller storage permissions and retry",
            )),
        }
    }

    async fn adopt_rollback_state(
        &self,
        workspace: &NativeWorkspace,
        pre_cowshed_checkout: &Path,
    ) -> Result<NativeAdoptRollbackState> {
        let source = &self.descriptor.git_root;
        let pre_exists = tokio::fs::symlink_metadata(pre_cowshed_checkout)
            .await
            .map(|_| true)
            .or_else(|error| {
                if error.kind() == std::io::ErrorKind::NotFound {
                    Ok(false)
                } else {
                    Err(CowshedError::environment_missing(
                        format!(
                            "cannot inspect retained checkout {}: {error}",
                            pre_cowshed_checkout.display()
                        ),
                        "check parent-directory permissions and retry",
                    ))
                }
            })?;
        let detached = matches!(
            workspace.derived.mount_state,
            crate::storage::lifecycle::MountState::Detached
        );
        if !pre_exists {
            if !detached {
                return Err(CowshedError::conflict(
                    format!(
                        "retained checkout {} is missing",
                        pre_cowshed_checkout.display()
                    ),
                    "restore the exact .pre-cowshed tree before retrying adoption rollback",
                ));
            }
            self.verify_checkout_identity(source, "restored project checkout")
                .await?;
            return Ok(NativeAdoptRollbackState::Complete);
        }

        match self
            .verify_checkout_identity(pre_cowshed_checkout, "retained .pre-cowshed checkout")
            .await
        {
            Ok(()) if detached => {
                if self
                    .verify_checkout_identity(source, "canonical project path")
                    .await
                    .is_ok()
                {
                    return Err(CowshedError::conflict(
                        "canonical project path contains a checkout while the retained checkout still exists",
                        "move the unrelated canonical-path checkout aside and retry",
                    ));
                }
                Ok(NativeAdoptRollbackState::Retained)
            }
            Ok(()) => Ok(NativeAdoptRollbackState::Retained),
            Err(retained_error) if detached => {
                if self
                    .verify_checkout_identity(source, "restored project checkout")
                    .await
                    .is_ok()
                {
                    Ok(NativeAdoptRollbackState::Swapped)
                } else {
                    Err(retained_error)
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn fresh_grants(&self) -> Result<PortGrantReservation> {
        let storage = self.descriptor.storage.clone();
        let reservation_root = storage.store().join(".staging");
        let inventory = crate::gateway_inventory::NativeGatewayInventory::new(storage);
        let used = inventory
            .all_reserved_port_blocks()
            .await
            .map_err(native_integrity_error)?;
        reserve_port_grants(&inventory, &reservation_root, used).await
    }
    /// A retried clone uses the grant already fenced into its canonical image. A fresh
    /// operation alone needs a process-lifetime claim until that image owns the block.
    async fn destination_grants(
        &self,
        destination: &WorkspaceName,
        resuming: bool,
    ) -> Result<(GrantSet, Option<PortGrantReservation>)> {
        if resuming
            && let Some((_, metadata)) = self
                .pending_metadata()
                .await?
                .into_iter()
                .find(|(_, metadata)| &metadata.workspace == destination)
        {
            // A pending sidecar owns this grant; an independently published duplicate must
            // refuse before the pending workspace can be activated or served.
            let storage = self.descriptor.storage.clone();
            crate::gateway_inventory::NativeGatewayInventory::new(storage)
                .all_reserved_port_blocks()
                .await
                .map_err(native_integrity_error)?;
            return Ok((metadata.grants, None));
        }
        let reservation = self.fresh_grants().await?;
        Ok((reservation.grants.clone(), Some(reservation)))
    }

    async fn snapshot_named(&self, name: &WorkspaceName) -> Result<WorkspaceSnapshot> {
        let current = self.current(name).await?;
        self.snapshot(&current)
    }

    fn require_exact_incarnation(
        workspace: &NativeWorkspace,
        expected: &WorkspaceIncarnation,
    ) -> Result<()> {
        let observed = workspace.derived.workspace.incarnation();
        if observed != expected {
            return Err(CowshedError::fence_refusal(
                crate::error::FenceRefusal::IncarnationMoved {
                    workspace: workspace.derived.workspace.name().clone(),
                    observed: observed.clone(),
                },
                "workspace incarnation is stale",
                "reacquire the worker handle and retry",
            ));
        }
        Ok(())
    }

    async fn advance_gateway_revision(&self, workspace: &NativeWorkspace) -> Result<()> {
        let mut metadata = workspace.metadata.clone();
        metadata.grants.revision = metadata
            .grants
            .revision
            .checked_add(1)
            .ok_or_else(|| CowshedError::internal("gateway session revision overflow"))?;
        let image = workspace.image.clone();
        crate::storage::lifecycle::dispatch_blocking(move || metadata.write_for_image(&image))
            .await
            .map_err(|error| CowshedError::internal(error.to_string()))?
            .map_err(native_integrity_error)
    }

    async fn ensure_supervisor(
        &mut self,
        name: &WorkspaceName,
    ) -> Result<super::supervisor::WorkspaceSupervisorHandle> {
        crate::timing::spanned("supervisor", "binding", self.validate_binding()).await?;
        let current = crate::timing::spanned("supervisor", "inventory", self.current(name)).await?;
        self.ensure_supervisor_for(current).await
    }

    /// Start or reuse the supervisor for a workspace whose current state the caller already
    /// read under a validated binding.
    async fn ensure_supervisor_for(
        &mut self,
        mut current: NativeWorkspace,
    ) -> Result<super::supervisor::WorkspaceSupervisorHandle> {
        use crate::storage::lifecycle::{MountIntent, Substrate};

        let name = current.derived.workspace.name().clone();
        let name = &name;
        // Every verb that runs work in a workspace arrives here, so this is where the
        // git-worktree precondition belongs: exec, sessions, and `path`'s implicit attach all get
        // the same refusal rather than a mount whose git is broken.
        if is_git_worktree(&current.metadata) {
            self.require_main_mounted_for_git_worktree(name).await?;
            current = self.current(name).await?;
        }
        let was_detached = matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Detached
        );
        let mount = crate::timing::spanned(
            "supervisor",
            "ensure-mounted",
            self.substrate
                .ensure_mounted(&current.derived.workspace, MountIntent { browse: false }),
        )
        .await
        .map_err(native_storage_error)?;
        if was_detached {
            self.advance_gateway_revision(&current).await?;
            current = self.current(name).await?;
        }
        // The effective revision covers the project's standing grants too, so a project grant
        // change relaunches the supervisor exactly as a workspace grant change does.
        let grants = effective_workspace_grants(&self.layout, &current.metadata.grants)?;
        let socket = super::supervisor_socket::socket_path(
            self.descriptor.storage.store(),
            &self.descriptor.repo_id,
            name,
        );
        if let Some(handle) = self.supervisors.get(name)
            && handle.snapshot().workspace_incarnation == *current.derived.workspace.incarnation()
            && handle.snapshot().grant_revision == grants.revision
        {
            // A supervisor another process serves may have gone with that process.
            let live = self.served.contains_key(name)
                || super::supervisor_socket::hello(&socket)
                    .await
                    .is_ok_and(|hello| &hello.authority == handle.snapshot());
            if live {
                return Ok(handle.clone());
            }
        }
        if let Some(old) = self.supervisors.remove(name) {
            // Only a supervisor this process serves is retired here; another process's is its
            // own to retire, and the hello below says whose it is.
            if self.served.contains_key(name) {
                old.quiesce().await?;
                old.retire().await?;
                self.forget_served(name).await?;
            }
            self.sessions.retain(|(workspace, _), _| workspace != name);
        }
        crate::timing::spanned(
            "supervisor",
            "excludes",
            crate::git::GitRepository::from_root(&mount).ensure_cowshed_excludes(),
        )
        .await?;
        let needed = super::supervisor::WorkspaceAuthoritySnapshot {
            repo_id: self.descriptor.repo_id.clone(),
            workspace: name.clone(),
            workspace_incarnation: current.derived.workspace.incarnation().clone(),
            grant_revision: grants.revision,
            lifecycle_revision: current.derived.workspace.revision().get(),
        };
        if self.supervisors_run_in == SupervisorHome::Daemon {
            let ensured = crate::timing::spanned(
                "supervisor",
                "manager-ensure",
                super::supervisor_manager::ensure(
                    self.descriptor.storage.store(),
                    &self.descriptor.git_root,
                    &needed,
                ),
            )
            .await?;
            self.forward_commitments(&ensured.socket);
            let handle = super::supervisor_socket::connect(ensured.socket, ensured.authority);
            self.supervisors.insert(name.clone(), handle.clone());
            return Ok(handle);
        }
        // One builder, so a grant advance cannot hand the supervisor a different policy than
        // its first start did. A deny, socket, or grant field added to only one of two inline
        // copies is a silent sandbox-policy fork.
        let build_volume_layout = self.build_volume_layout()?;
        let sandbox = supervisor_sandbox(
            &self.home,
            &self.layout,
            &self.telemetry_root,
            &current,
            &grants,
            mount.clone(),
            self.workspace_mount_path(&main_name())?,
            // The volume the checkout links at start; each admitted job then carries its own.
            build_volume_layout.grant(name, &mount)?,
        )?;
        let historical_incarnations = workspace_lineage(
            &mount,
            current.derived.workspace.incarnation(),
            crate::storage::job_artifact::ArtifactConfig::default().retained_recovery_budget_bytes,
        )?;
        let mut config = super::supervisor::WorkspaceSupervisorConfig {
            authority: needed,
            owned_repo_ids: self.owned_repo_ids()?,
            workspace_root: mount,
            default_cwd: None,
            sandbox,
            build_volume_layout: Some(build_volume_layout),
            artifacts: crate::storage::job_artifact::ArtifactConfig {
                historical_incarnations,
                ..crate::storage::job_artifact::ArtifactConfig::default()
            },
            term_grace: std::time::Duration::from_secs(2),
            actor_capacity: ROUTER_CAPACITY,
            event_capacity: ROUTER_CAPACITY,
            // Host state, read at supervisor start: which environment variable names this
            // project's approved gateway credentials came from, so no child receives an ambient
            // copy of a token the gateway already holds.
            credential_env_names: crate::storage::host_config::HostConfig::load_for_store(
                self.descriptor.storage.store(),
            )
            .map_err(|error| {
                CowshedError::integrity(
                    format!("host configuration is unreadable: {error}"),
                    "cowshed doctor --json",
                )
            })?
            .credential_env_names(self.descriptor.repo_id.as_str()),
            shell_host: super::shell_host::registered(),
            // This process is the workspace's supervisor for as long as the workspace is in
            // use: a spare activated ahead of demand is the next command's warm shell.
            shell_pool: super::shell_pool::ShellPoolConfig::default(),
            group_ledger: Some(super::job_groups::ledger_path(&socket)),
            // Filled from the lost predecessor's ledger once this process holds the socket.
            inherited_groups: Vec::new(),
            volume_labels: Some(super::supervisor::VolumeLabels {
                workspace: crate::storage::apfs::volume_label(&self.descriptor.repo_id, name),
                build: crate::storage::apfs::build_volume_label(&self.descriptor.repo_id, name),
                labeller: std::sync::Arc::new(ApfsLabeller(self.substrate.shared_host())),
            }),
        };
        // A workspace has one supervisor, its one job allocator: when another controller
        // process already serves it under this authority, or under grants published since this
        // command read them, its commands go there.
        match super::supervisor_socket::hello(&socket).await {
            Ok(hello) => {
                match super::supervisor_manager::standing(&hello.authority, &config.authority) {
                    super::supervisor_manager::Standing::Serves => {
                        let handle = super::supervisor_socket::connect(socket, hello.authority);
                        self.supervisors.insert(name.clone(), handle.clone());
                        return Ok(handle);
                    }
                    super::supervisor_manager::Standing::Behind
                    | super::supervisor_manager::Standing::Elsewhere => {
                        return Err(CowshedError::conflict(
                            format!(
                                "process {} serves workspace {name}'s supervisor under incarnation \
                             {} and grant revision {}; this command needs incarnation {} and \
                             grant revision {} or newer",
                                hello.pid,
                                hello.authority.workspace_incarnation,
                                hello.authority.grant_revision,
                                config.authority.workspace_incarnation,
                                config.authority.grant_revision,
                            ),
                            format!(
                                "let that command finish, or run `cowshed detach {name}`, then retry"
                            ),
                        ));
                    }
                }
            }
            // A supervisor of another cowshed build: refused by name.
            Err(error) if error.code == crate::error::ErrorCode::Conflict => return Err(error),
            // Nobody serves it; this process does.
            Err(_) => {}
        }
        let authority = config.authority.clone();
        // The socket first: holding it is what makes this the workspace's one supervisor, so
        // what the last one left running is this one's to end and seal.
        let listener = super::supervisor_socket::bind(&socket).await?;
        self.seal_lost_jobs(&socket, &mut config).await?;
        // Every commitment goes to this process's own sink, the host's default, and is kept for
        // controllers that forward the workspace's commitments into sinks of their own.
        let feed = super::commitment_feed::CommitmentFeed::default();
        let actor = super::supervisor::WorkspaceSupervisor::start(
            config,
            super::commitment_feed::FeedingSink {
                inner: self.commitments.clone(),
                feed: feed.clone(),
            },
        )?;
        let (advances, advance_requests) = tokio::sync::mpsc::channel(1);
        let server = tokio::spawn(super::supervisor_socket::serve(
            listener,
            actor.clone(),
            Some(advances),
            Some(feed),
        ));
        let handle = super::supervisor_socket::connect(socket.clone(), authority);
        self.served.insert(
            name.clone(),
            ServedSupervisor {
                actor,
                socket,
                server,
                advance_requests,
            },
        );
        self.supervisors.insert(name.clone(), handle.clone());
        Ok(handle)
    }

    /// Forward the commitments the supervisor at `socket` records into this host's own sink,
    /// once per supervisor process: from the first it has not acknowledged, until it is gone.
    fn forward_commitments(&mut self, socket: &Path) {
        let Some(forwarders) = self.forwarders.as_mut() else {
            return;
        };
        if forwarders
            .get(socket)
            .is_some_and(|forwarder| !forwarder.is_finished())
        {
            return;
        }
        let socket = socket.to_path_buf();
        let mut publisher = self.commitments.clone();
        let task = tokio::spawn({
            let socket = socket.clone();
            async move {
                use super::supervisor::CommitmentSink as _;
                let mut after = 0;
                loop {
                    let page = match super::supervisor_socket::commitments(&socket, after).await {
                        Ok(page) => page,
                        // The supervisor is gone; the next ensure starts a forwarder for its
                        // successor, which has sealed and recorded what this one lost.
                        Err(_) => return,
                    };
                    if let Some(lost) = page.lost_through {
                        eprintln!(
                            "cowshed: commitments through {lost} of workspace supervisor {} were dropped before they were forwarded",
                            socket.display()
                        );
                        after = after.max(lost);
                    }
                    for entry in page.entries {
                        if publisher.record(entry.draft).await.is_err() {
                            // The project is detaching; nothing takes records any more.
                            return;
                        }
                        after = entry.cursor;
                    }
                    if super::supervisor_socket::acknowledge_commitments(&socket, after)
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        });
        forwarders.insert(socket, task);
    }

    /// Before a supervisor starts on the socket it now holds: end the process groups its lost
    /// predecessor's ledger still names and can prove the job's, hand the supervisor the ones it
    /// cannot (`inherited_groups`), then seal `failed` with `supervisorLost` every job this
    /// incarnation admitted and never sealed, recording their terminal commitments. Holding the
    /// socket means no other supervisor serves the workspace; a job the ledger does not name lost
    /// its ledger entry, or its terminal record, to the power loss that ended it.
    async fn seal_lost_jobs(
        &mut self,
        socket: &Path,
        config: &mut super::supervisor::WorkspaceSupervisorConfig,
    ) -> Result<()> {
        use super::supervisor::{CommitmentDraft, CommitmentSink as _};

        let ledger = super::job_groups::ledger_path(socket);
        let unresolved = tokio::task::spawn_blocking(move || {
            super::job_groups::take_lost(&ledger, LOST_JOB_GRACE)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("ending lost jobs failed: {error}")))?
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("cannot end the jobs a lost supervisor left running: {error}"),
                "cowshed doctor --json",
            )
        })?;
        for group in &unresolved {
            eprintln!(
                "cowshed: workspace {}: job {} process group {} (recorded by lost supervisor process \
                 {}) still has processes but its leader is gone, so they cannot be told from \
                 another group that took the id; not signalled, kept in the ledger and reported by \
                 `cowshed doctor`",
                config.authority.workspace,
                group.job_id(),
                group.pgid(),
                group.lost_supervisor()
            );
        }
        config.inherited_groups = unresolved;
        let (root, owned, incarnation, artifacts) = (
            config.workspace_root.clone(),
            config.owned_repo_ids.clone(),
            config.authority.workspace_incarnation.clone(),
            config.artifacts.clone(),
        );
        let sealed = tokio::task::spawn_blocking(move || {
            crate::storage::job_artifact::ArtifactStore::open(root, owned, incarnation, artifacts)?
                .seal_unterminated()
        })
        .await
        .map_err(|error| CowshedError::internal(format!("sealing lost jobs failed: {error}")))?
        .map_err(|error| {
            let error = super::supervisor::map_artifact_error(error);
            CowshedError::new(
                error.code,
                format!(
                    "cannot seal the jobs a lost supervisor ran: {}",
                    error.message
                ),
                error.hint,
            )
        })?;
        if sealed.is_empty() {
            return Ok(());
        }
        eprintln!(
            "cowshed: sealed jobs {:?} of workspace {} as lost with the supervisor that ran them",
            sealed
                .iter()
                .map(|(record, _)| record.job_id.get())
                .collect::<Vec<_>>(),
            config.authority.workspace
        );
        for (record, batch_sha256) in sealed {
            self.commitments
                .record(CommitmentDraft::Terminal {
                    repo_id: config.authority.repo_id.clone(),
                    workspace_incarnation: record.workspace_incarnation,
                    job_id: record.job_id,
                    state: record.state,
                    grant_revision: record.grant_revision,
                    stdout_bytes: record.stdout.bytes,
                    stdout_sha256: record.stdout.sha256,
                    stderr_bytes: record.stderr.bytes,
                    stderr_sha256: record.stderr.sha256,
                    batch_sha256,
                    output_limit: None,
                })
                .await?;
        }
        Ok(())
    }

    /// The ledger of the process groups `workspace`'s supervisor runs, and of the groups a lost
    /// one left unresolved ([`super::job_groups`]): what any change to the workspace's substrate
    /// reads first to learn whether processes it cannot account for may still use it.
    fn job_group_ledger(&self, workspace: &WorkspaceName) -> PathBuf {
        super::job_groups::ledger_path(&super::supervisor_socket::socket_path(
            self.descriptor.storage.store(),
            &self.descriptor.repo_id,
            workspace,
        ))
    }

    async fn stop_supervisor(
        &mut self,
        name: &WorkspaceName,
    ) -> Result<super::supervisor_socket::BoundSocket> {
        self.stop_supervisor_with_mode(name, false).await
    }

    /// Force removal cannot quiesce first: quiescing waits for running jobs to finish naturally.
    /// Retiring first closes admission and asks each job to terminate with TERM then KILL.
    async fn stop_supervisor_for_removal(
        &mut self,
        name: &WorkspaceName,
        force: bool,
    ) -> Result<super::supervisor_socket::BoundSocket> {
        self.stop_supervisor_with_mode(name, force).await
    }

    async fn stop_supervisor_handle(
        handle: &super::supervisor::WorkspaceSupervisorHandle,
        terminate_jobs: bool,
    ) -> Result<()> {
        if terminate_jobs {
            handle.retire().await
        } else {
            handle.quiesce().await?;
            handle.retire().await
        }
    }

    async fn stop_supervisor_with_mode(
        &mut self,
        name: &WorkspaceName,
        terminate_jobs: bool,
    ) -> Result<super::supervisor_socket::BoundSocket> {
        let socket = super::supervisor_socket::socket_path(
            self.descriptor.storage.store(),
            &self.descriptor.repo_id,
            name,
        );
        // An unknown hello or retirement outcome is never proof a supervisor stopped. The
        // returned socket lease closes replacement admission until the caller's mutation ends.
        let handle = match self.supervisors.remove(name) {
            Some(handle) => Some(handle),
            None => match super::supervisor_socket::hello_if_present(&socket).await {
                Ok(Some(hello)) => Some(super::supervisor_socket::connect(
                    socket.clone(),
                    hello.authority,
                )),
                Ok(None) => None,
                Err(error) if error.code == ErrorCode::Conflict => {
                    let stopped = super::supervisor_manager::stop_other_build(&socket).await?;
                    eprintln!(
                        "cowshed: stopped workspace {name}'s supervisor (pid {}) of another cowshed build",
                        stopped.pid
                    );
                    self.forget_served(name).await?;
                    self.sessions.retain(|(workspace, _), _| workspace != name);
                    return Ok(stopped.socket);
                }
                Err(error) => return Err(error),
            },
        };
        if let Some(handle) = handle {
            Self::stop_supervisor_handle(&handle, terminate_jobs).await?;
        }
        self.forget_served(name).await?;
        self.sessions.retain(|(workspace, _), _| workspace != name);
        super::supervisor_socket::bind(&socket).await
    }

    /// After retirement, abort and join this process's socket server before another owner binds.
    async fn forget_served(&mut self, name: &WorkspaceName) -> Result<()> {
        if let Some(mut served) = self.served.remove(name) {
            // A completed server already released its bound socket. Its result may have been
            // consumed by the serving loop, so it must not be polled for a second time.
            if !served.server.is_finished() {
                served.server.abort();
                match (&mut served.server).await {
                    Ok(ended) => ended?,
                    Err(error) if error.is_cancelled() => {}
                    Err(error) => {
                        return Err(CowshedError::internal(format!(
                            "workspace {name}'s socket server could not retire: {error}"
                        )));
                    }
                }
            }
        }
        Ok(())
    }

    /// Serve `name`'s supervisor from this process until it retires: the
    /// `cowshed __workspace-supervisor` verb. It retires when a client retires it, or once it
    /// has held no named session, no running job and no Nx daemon for [`SUPERVISOR_IDLE`]; a
    /// shed's idle Nx daemon is stopped by its keeper first, so none outlives the supervisor
    /// that watched it. Meanwhile it answers `advance` requests by re-reading the workspace's
    /// grants.
    async fn serve_supervisor_until_retired(&mut self, name: WorkspaceName) -> Result<()> {
        self.supervisors_run_in = SupervisorHome::ThisProcess;
        self.validate_binding().await?;
        let current = self.current(&name).await?;
        self.ensure_supervisor_for(current).await?;
        if !self.served.contains_key(&name) {
            let socket = super::supervisor_socket::socket_path(
                self.descriptor.storage.store(),
                &self.descriptor.repo_id,
                &name,
            );
            let pid = super::supervisor_socket::hello(&socket).await?.pid;
            return Err(CowshedError::conflict(
                format!("process {pid} already serves workspace {name}'s supervisor"),
                "stop that process first",
            ));
        }
        self.supervisors.remove(&name);
        let mut idle_since: Option<tokio::time::Instant> = None;
        let mut ticks = tokio::time::interval(std::time::Duration::from_secs(60));
        let ended = loop {
            let served = self
                .served
                .get_mut(&name)
                .ok_or_else(|| CowshedError::internal("the served supervisor vanished"))?;
            let event = tokio::select! {
                ended = &mut served.server => ServeEvent::Ended(ended),
                Some(reply) = served.advance_requests.recv() => ServeEvent::Advance(reply),
                now = ticks.tick() => ServeEvent::Tick(now, served.actor.clone()),
            };
            match event {
                ServeEvent::Ended(ended) => {
                    break ended.map_err(|error| {
                        CowshedError::internal(format!(
                            "workspace supervisor server failed: {error}"
                        ))
                    })?;
                }
                ServeEvent::Advance(reply) => {
                    let _ = reply.send(self.advance_served(&name).await);
                }
                ServeEvent::Tick(now, actor) => {
                    if !actor.idle().await? {
                        idle_since = None;
                        continue;
                    }
                    let since = *idle_since.get_or_insert(now);
                    if now.duration_since(since) >= SUPERVISOR_IDLE {
                        // Quiesce first: a command admitted since the last look finishes
                        // before the supervisor retires.
                        actor.quiesce().await?;
                        break actor.retire().await;
                    }
                }
            }
        };
        self.forget_served(&name).await?;
        ended
    }

    /// Serve the served supervisor of `name` under the workspace's grants as they are now.
    async fn advance_served(
        &mut self,
        name: &WorkspaceName,
    ) -> Result<super::supervisor::WorkspaceAuthoritySnapshot> {
        self.validate_binding().await?;
        let current = self.current(name).await?;
        let grants = effective_workspace_grants(&self.layout, &current.metadata.grants)?;
        let sandbox = supervisor_sandbox(
            &self.home,
            &self.layout,
            &self.telemetry_root,
            &current,
            &grants,
            self.workspace_mount_path(name)?,
            self.workspace_mount_path(&main_name())?,
            self.build_volume_layout()?
                .grant(name, &self.workspace_mount_path(name)?)?,
        )?;
        let served = self
            .served
            .get_mut(name)
            .ok_or_else(|| CowshedError::internal("the served supervisor vanished"))?;
        let advanced = served
            .actor
            .advance_authority(
                grants.revision,
                current.derived.workspace.revision().get(),
                sandbox,
            )
            .await?;
        let authority = advanced.snapshot().clone();
        served.actor = advanced;
        Ok(authority)
    }

    /// Unpublished workspaces nothing will ever finish: no unfinished lifecycle intent names them
    /// (a crash residue that one names is finished by recovery), and no process is creating them —
    /// neither under an intent lease nor under the image's lifecycle lock, which a create, fork or
    /// adopt of any cowshed version holds for as long as it works on the image. Such a workspace
    /// was never published, so nothing ever ran in it, and an unpublished main never touched its
    /// checkout. Each one comes back with its intent lease held by this verb, so no other process
    /// can take it up while it is retired.
    async fn abandoned_pending_workspaces(
        &mut self,
    ) -> Result<Vec<(PathBuf, crate::metadata::DetachedWorkspaceMetadata)>> {
        self.reload_lifecycle_intents().await?;
        let mut abandoned = Vec::new();
        for (image, metadata) in self.pending_metadata().await? {
            let workspace = &metadata.workspace;
            if self
                .lifecycle_intents
                .get(workspace)
                .is_some_and(|record| record.completion.is_none())
                || !self.claim_intent_lease(workspace)?
                || image_lifecycle_lock_is_held(&image)?
            {
                continue;
            }
            abandoned.push((image, metadata));
        }
        Ok(abandoned)
    }

    /// Retire every [abandoned](Self::abandoned_pending_workspaces) workspace, or with `dry_run`
    /// only name them. A clone is retired as the create or fork its metadata records; one whose
    /// retirement is refused — say its history is not in main — stays, named with the refusal and
    /// its next step, and no longer blocks the rest of `gc`. An unpublished main is retired as the
    /// adopt it records: its image, sidecar and companion are reclaimed.
    async fn retire_abandoned_pending_workspaces(&mut self, dry_run: bool) -> Result<()> {
        for (image, metadata) in self.abandoned_pending_workspaces().await? {
            let workspace = metadata.workspace.clone();
            let kind = if workspace.is_main() {
                "adoption"
            } else {
                "clone"
            };
            if dry_run {
                eprintln!(
                    "cowshed: would retire {workspace}, an unfinished {kind} its process \
                     abandoned before publishing it: {}",
                    image.display()
                );
                continue;
            }
            let retired = if workspace.is_main() {
                self.substrate
                    .discard_pending_adoption(&metadata.repo_id, &metadata.workspace_incarnation)
                    .await
                    .map_err(native_storage_error)
            } else {
                let origin = abandoned_clone_origin(&metadata);
                let options = RemoveOptions {
                    force: true,
                    ..RemoveOptions::default()
                };
                self.retire_pending_workspace(&workspace, options, origin, metadata)
                    .await
                    .map(|_| ())
            };
            match retired {
                Ok(()) => eprintln!(
                    "cowshed: retired {workspace}, an unfinished {kind} its process abandoned \
                     before publishing it"
                ),
                Err(error) => eprintln!(
                    "cowshed: kept unfinished {kind} {workspace}: {}\nnext: {}",
                    error.message, error.hint
                ),
            }
        }
        Ok(())
    }

    /// Retire an unpublished clone without ever publishing it. The retired image is left in trash
    /// for the caller to reclaim: `rm` does so in the background, while `gc` sweeps it in the same
    /// pass instead of racing a background reclaim with its own plan.
    async fn retire_pending_workspace(
        &mut self,
        workspace: &WorkspaceName,
        options: RemoveOptions,
        origin: crate::storage::recovery::LifecycleIntent,
        metadata: crate::metadata::DetachedWorkspaceMetadata,
    ) -> Result<(RemoveReport, crate::storage::lifecycle::RetiredRef)> {
        use super::supervisor::{CommitmentDraft, CommitmentSink};
        use crate::storage::lifecycle::{
            Destination, LifecyclePlanner, MountIntent, MountState, Substrate,
        };

        if metadata.repo_id != self.descriptor.repo_id
            || metadata.workspace != *workspace
            || metadata.publication_state != crate::metadata::PublicationState::PendingFence
        {
            return Err(CowshedError::integrity(
                format!("pending retirement metadata does not name {workspace}"),
                "cowshed doctor --json",
            ));
        }
        let (source_name, fork) = match &origin {
            crate::storage::recovery::LifecycleIntent::Create {
                workspace: target,
                options,
            } if target == workspace => (
                options.from_workspace.clone().unwrap_or_else(main_name),
                false,
            ),
            crate::storage::recovery::LifecycleIntent::Fork {
                source,
                destination,
            } if destination == workspace => (source.clone(), true),
            _ => {
                return Err(CowshedError::integrity(
                    format!("pending workspace {workspace} has no matching create or fork intent"),
                    "cowshed doctor --json",
                ));
            }
        };
        let info = &metadata.info_snapshot;
        let source = self.current(&source_name).await?;
        let identity = self
            .operation_identity(
                metadata.grants.clone(),
                info.branch.clone(),
                info.forked_from.clone(),
                info.git_worktree,
            )
            .await?;
        let destination = Destination {
            repo: self.descriptor.repo_id.clone(),
            name: workspace.clone(),
            topology_revision: source.derived.workspace.topology_revision(),
            identity,
        };
        let plan = if fork {
            PendingClonePlan::Fork(
                self.substrate
                    .plan_fork(&source.derived.workspace, destination)
                    .map_err(native_integrity_error)?,
            )
        } else {
            PendingClonePlan::Create(
                self.substrate
                    .plan_create(&source.derived.workspace, destination)
                    .map_err(native_integrity_error)?,
            )
        };
        let main = self.current(&main_name()).await?;
        let main_detached = matches!(main.derived.mount_state, MountState::Detached);
        if main_detached {
            self.substrate
                .ensure_mounted(&main.derived.workspace, MountIntent { browse: false })
                .await
                .map_err(native_storage_error)?;
        }
        let retire_intent = crate::storage::recovery::LifecycleIntent::Retire {
            workspace: workspace.clone(),
            options,
            origin: Some(Box::new(origin)),
        };
        let previous = self
            .lifecycle_intents
            .get(workspace)
            .is_some_and(|record| record.operation == retire_intent && record.completion.is_none());
        let substrate = self.substrate.clone();
        // An unpublished clone never landed anywhere: main is what has to hold its work.
        let containment = self.main_containment()?;
        let result = async {
            let (retired, abandoned) = substrate
                .execute_pending_clone_retirement(plan, |stage| {
                    let this = &mut *self;
                    let containment = &containment;
                    async move {
                        let first = this
                            .removal_git_fence_at(&stage.mount_point, stage.workspace.incarnation())
                            .await?;
                        if !options.force {
                            Self::require_session_state_clean(workspace, &first)?;
                        }
                        this.require_session_landed(
                            workspace,
                            &first,
                            options.abandon,
                            containment,
                        )
                        .await?;
                        let _stopped = this
                            .stop_supervisor_for_removal(workspace, options.force)
                            .await?;
                        let last = this
                            .removal_git_fence_at(&stage.mount_point, stage.workspace.incarnation())
                            .await?;
                        if last.head != first.head {
                            return Err(removal_head_moved_refusal(
                                workspace,
                                &first.head,
                                &last.head,
                            ));
                        }
                        if !options.force {
                            Self::require_session_state_clean(workspace, &last)?;
                        }
                        let abandoning = this
                            .require_session_landed(workspace, &last, options.abandon, containment)
                            .await?;
                        if !previous {
                            this.begin_lifecycle_intent(retire_intent).await?;
                        }
                        this.mark_lifecycle_intent_mutating(workspace).await?;
                        let abandoned = match abandoning {
                            Some(landed) => Some(
                                this.bundle_abandoned_work(workspace, &last, landed, containment)
                                    .await?,
                            ),
                            None => None,
                        };
                        this.unregister_workspace_in_main(workspace, info.git_worktree)
                            .await?;
                        Ok::<_, CowshedError>(abandoned)
                    }
                })
                .await
                .map_err(native_staged_error)?;
            self.commitments
                .record(CommitmentDraft::WorkspaceRetired {
                    repo_id: self.descriptor.repo_id.clone(),
                    workspace_incarnation: retired.workspace().incarnation().clone(),
                })
                .await?;
            self.release_slot(workspace).await?;
            let report = RemoveReport { abandoned };
            self.complete_lifecycle_intent(
                workspace,
                crate::storage::recovery::LifecycleIntentCompletion::Retire(report.clone()),
            )
            .await?;
            Ok((report, retired))
        }
        .await;
        if main_detached {
            let main = self.current(&main_name()).await?;
            let _stopped = self.stop_supervisor(&main_name()).await?;
            self.substrate
                .unmount(&main.derived.workspace)
                .await
                .map_err(native_storage_error)?;
        }
        result
    }

    async fn retire_workspace(&mut self, current: NativeWorkspace) -> Result<()> {
        use super::supervisor::CommitmentSink;
        use crate::storage::lifecycle::LifecyclePlanner;

        let plan = self
            .substrate
            .plan_retire(&current.derived.workspace)
            .map_err(native_integrity_error)?;
        let mut commitments = self.commitments.clone();
        let repo_id = self.descriptor.repo_id.clone();
        let retired = self
            .substrate
            .execute_retire_staged(plan, move |retired| async move {
                commitments
                    .record(super::supervisor::CommitmentDraft::WorkspaceRetired {
                        repo_id,
                        workspace_incarnation: retired.workspace().incarnation().clone(),
                    })
                    .await
            })
            .await
            .map_err(native_retire_error)?;
        // The volume is detached, so the slot is free for its next tenant. Released before
        // reclamation rather than after: reclamation only ever removes an empty mountpoint the
        // *name* derives, and a slot mountpoint is meant to outlive its tenants — the next
        // workspace to take the slot mounts at exactly the same absolute path, which is the whole
        // point of a slot.
        self.release_slot(current.derived.workspace.name()).await?;
        self.reclaim_in_background(retired);
        Ok(())
    }

    /// Reclaim a retired image without holding up the verb that retired it. Best-effort by
    /// design: an interrupted reclamation leaves trash for the next idempotent gc pass, and
    /// `settle_reclaims` is how a caller that collects next waits for this one first.
    fn reclaim_in_background(&mut self, retired: crate::storage::lifecycle::RetiredRef) {
        use crate::storage::lifecycle::Substrate;

        let substrate = self.substrate.clone();
        self.reclaims.retain(|reclaim| !reclaim.is_finished());
        self.reclaims.push(tokio::spawn(async move {
            let _ = substrate.reclaim(retired).await;
        }));
    }

    async fn retire_restored_main(&mut self, current: NativeWorkspace) -> Result<()> {
        use super::supervisor::CommitmentSink;
        use crate::storage::lifecycle::Substrate;

        let mut commitments = self.commitments.clone();
        let repo_id = self.descriptor.repo_id.clone();
        let retired = self
            .substrate
            .execute_restored_main_retirement(
                &current.derived.workspace,
                move |retired| async move {
                    commitments
                        .record(super::supervisor::CommitmentDraft::WorkspaceRetired {
                            repo_id,
                            workspace_incarnation: retired.workspace().incarnation().clone(),
                        })
                        .await
                },
            )
            .await
            .map_err(native_retire_error)?;
        self.substrate
            .reclaim(retired)
            .await
            .map_err(native_storage_error)?;
        Ok(())
    }

    fn session(
        &self,
        workspace: &WorkspaceName,
        name: &Option<String>,
    ) -> Option<&super::supervisor::SessionToken> {
        self.sessions.get(&(workspace.clone(), name.clone()))
    }
}

/// The token a session open replaced, when it names another session. The supervisor answers a
/// name it still holds with that session's own identity, so a reopen's previous token is the
/// session just opened, and closing it would close the session the caller is about to run in.
#[cfg(target_os = "macos")]
fn superseded_session(
    previous: super::supervisor::SessionToken,
    reopened: u64,
) -> Option<super::supervisor::SessionToken> {
    (previous.identity() != reopened).then_some(previous)
}

#[cfg(target_os = "macos")]
struct NativeWorkspace {
    derived: crate::storage::lifecycle::DerivedWorkspace,
    metadata: crate::metadata::DetachedWorkspaceMetadata,
    image: PathBuf,
}

/// Whether a main restore loses commits. `retained_head` is where the retained checkout stands,
/// when it names one; an abandonment bundles main's history beyond it.
#[cfg(target_os = "macos")]
enum MainPreservation {
    Preserved,
    Unpreserved {
        head: GitOid,
        retained_head: Option<GitOid>,
    },
}

#[cfg(target_os = "macos")]
enum PendingClonePlan {
    Create(crate::storage::lifecycle::CreatePlan),
    Fork(crate::storage::lifecycle::ForkPlan),
}

#[cfg(target_os = "macos")]
impl crate::storage::lifecycle::ImmutablePlan for PendingClonePlan {
    fn expected(&self) -> &[crate::storage::lifecycle::LifecycleFact] {
        match self {
            Self::Create(plan) => crate::storage::lifecycle::ImmutablePlan::expected(plan),
            Self::Fork(plan) => crate::storage::lifecycle::ImmutablePlan::expected(plan),
        }
    }

    fn operation(&self) -> &crate::storage::lifecycle::Operation {
        match self {
            Self::Create(plan) => crate::storage::lifecycle::ImmutablePlan::operation(plan),
            Self::Fork(plan) => crate::storage::lifecycle::ImmutablePlan::operation(plan),
        }
    }
}

/// Refuse a checkout destination anything already occupies: a move never replaces what is there.
#[cfg(target_os = "macos")]
fn require_vacant_move_destination(destination: &Path) -> Result<()> {
    match std::fs::symlink_metadata(destination) {
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(CowshedError::environment_missing(
            format!(
                "cannot inspect checkout destination {}: {error}",
                destination.display()
            ),
            "check the destination parent permissions and retry",
        )),
        Ok(_) => Err(CowshedError::conflict(
            format!("{} already exists", destination.display()),
            "remove the occupant or choose another destination",
        )),
    }
}

/// Finishes what interrupted work left in the project's store that the open found, before the
/// controller serves: restores whose image was published but whose metadata was never activated,
/// and retired images still in the trash. Returns the substrate over `host`.
///
/// An inspecting open finishes neither: doctor reports both from the store itself, as
/// `pending-publication` and `retired-trash` findings.
#[cfg(target_os = "macos")]
async fn finish_store_residue<H: crate::storage::apfs::ApfsExecutionHost>(
    host: H,
    config: crate::storage::apfs::ApfsSubstrateConfig,
    repo_id: &RepoId,
    restored: &[crate::storage::apfs::PendingPublicationFact],
    retired: RetiredTrash,
    commitments: &mut super::supervisor::CommitmentPublisherHandle,
    scope: &RecoveryScope,
) -> Result<crate::storage::apfs::ApfsSubstrate<H>> {
    use super::supervisor::{CommitmentDraft, CommitmentSink};
    use crate::storage::lifecycle::Substrate;

    if !scope.repairs() {
        return Ok(crate::storage::apfs::ApfsSubstrate::new(config, host));
    }
    for publication in restored {
        commitments
            .record(CommitmentDraft::Restore {
                repo_id: repo_id.clone(),
                source_checkpoint: publication.source_checkpoint.clone(),
                source_incarnation: publication.source_incarnation.clone(),
                replaced_incarnation: publication.replaced_incarnation.clone(),
                destination_incarnation: publication.destination_incarnation.clone(),
            })
            .await?;
        host.activate_restored_metadata(&publication.image)
            .map_err(native_storage_error)?;
    }
    let substrate = crate::storage::apfs::ApfsSubstrate::new(config, host);
    let _span = crate::timing::span("open", "reclaim");
    for retirement in retired.recorded {
        // Trash reclamation is best effort: the retirement is already a fact of the inventory,
        // and an image left in the trash is reclaimed by the next open or by `gc`. A failure is
        // said, never dropped.
        let workspace = retirement.workspace().name().clone();
        let incarnation = retirement.workspace().incarnation().clone();
        if let Err(error) = substrate.reclaim(retirement).await {
            eprintln!(
                "cowshed: retired workspace {workspace} (incarnation {incarnation}) stays in \
                 sessions/.trash: reclaiming it failed ({error}); the next cowshed command or \
                 cowshed gc retries, and cowshed doctor reports it as retired-trash"
            );
        }
    }
    // An image whose record is already gone is already reclaimed as a retirement; its bytes go
    // the same best-effort way.
    for image in retired.unrecorded {
        if let Err(error) = substrate
            .reclaim_unrecorded_retired(repo_id, image.clone())
            .await
        {
            eprintln!(
                "cowshed: retired image {} stays in sessions/.trash: reclaiming it failed \
                 ({error}); the next cowshed command or cowshed gc retries",
                image.display()
            );
        }
    }
    Ok(substrate)
}

#[cfg(target_os = "macos")]
async fn recover_repository_identity_intent(store_root: &Path) -> Result<()> {
    let store_root = store_root.to_owned();
    crate::storage::lifecycle::dispatch_blocking(move || {
        let Some(intent) = crate::storage::recovery::RepositoryIdentityIntent::load(&store_root)?
        else {
            return Ok(());
        };
        // `Prepared` is written before the mutation fence, with nothing renamed and the project's
        // volumes already unmounted, so there is nothing to finish and nothing to undo. Detached is
        // a state `cowshed attach` resolves.
        if intent.phase == crate::storage::recovery::LifecycleIntentPhase::Prepared {
            return crate::storage::recovery::RepositoryIdentityIntent::clear(&store_root);
        }
        apply_identity_change(&intent)?;
        crate::storage::recovery::RepositoryIdentityIntent::clear(&store_root)
    })
    .await
    .map_err(|error| {
        CowshedError::internal(format!("repository identity recovery task failed: {error}"))
    })?
}

/// Apply the durable half of an identity change, from whatever state the store is in right now.
///
/// Every step derives what to do from authoritative state — which directory exists, what the
/// binding says — and every step is a no-op once applied. The forward transaction and recovery
/// therefore call this one function rather than keeping two implementations that can disagree,
/// and the journal is asked only *whether* a change is owed, never how far it got.
///
/// In-image workspace markers are deliberately not touched. A detached image's marker is out of
/// reach by definition, and reaching it would mean mounting images mid-transaction — which is what
/// stranded detached sessions behind a doctor hint before. The marker keeps naming an identity the
/// binding now records as former, [`OwnedRepoIds`] accepts it, and the next attach converges it.
///
/// Synchronous and blocking on purpose: from the first rename until the last sidecar is rewritten
/// the store has two namespaces in play, and nothing else may enumerate it in between.
#[cfg(target_os = "macos")]
fn apply_identity_change(
    intent: &crate::storage::recovery::RepositoryIdentityIntent,
) -> Result<()> {
    move_identity_namespace(
        "store",
        &intent.old_project_root,
        &intent.new_project_root,
        intent,
    )?;
    converge_binding_identity(
        &intent
            .new_project_root
            .join(crate::repository::REPOSITORY_BINDING_FILE),
        &intent.old_repo_id,
        &intent.new_repo_id,
    )?;
    // A project whose workspaces were never mounted has no mount root at all, which is why this is
    // conditional rather than required.
    if intent.old_mount_root.exists() || intent.new_mount_root.exists() {
        move_identity_namespace(
            "workspace mount",
            &intent.old_mount_root,
            &intent.new_mount_root,
            intent,
        )?;
    }
    rewrite_sidecar_identities(
        &intent.new_project_root,
        &intent.old_repo_id,
        &intent.new_repo_id,
    )
}

/// Rename one of the project's two namespaces, refusing every state that is not one-or-the-other.
///
/// Both present is the state that must never be guessed at: one of them holds the project and the
/// other holds whatever a previous interruption left, and picking wrong loses a workspace image.
/// Neither present is equally unguessable — the directory the intent names is simply gone.
#[cfg(target_os = "macos")]
fn move_identity_namespace(
    namespace: &str,
    old: &Path,
    new: &Path,
    intent: &crate::storage::recovery::RepositoryIdentityIntent,
) -> Result<()> {
    match (old.exists(), new.exists()) {
        (false, true) => return Ok(()),
        (true, true) => {
            return Err(CowshedError::integrity(
                format!(
                    "repository identity change {} -> {} has both {namespace} namespaces: {} and {}",
                    intent.old_repo_id,
                    intent.new_repo_id,
                    old.display(),
                    new.display()
                ),
                format!(
                    "inspect {} and {} with `cowshed doctor`, then remove whichever is not the \
                     project — cowshed will not choose",
                    old.display(),
                    new.display()
                ),
            ));
        }
        (false, false) => {
            return Err(CowshedError::integrity(
                format!(
                    "repository identity change {} -> {} has neither {namespace} namespace: \
                     neither {} nor {} exists",
                    intent.old_repo_id,
                    intent.new_repo_id,
                    old.display(),
                    new.display()
                ),
                "restore the project directory from backup, then reopen cowshed",
            ));
        }
        (true, false) => {}
    }
    if let Some(parent) = new.parent() {
        fs::create_dir_all(parent).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot create {namespace} identity directory {}: {error}",
                    parent.display()
                ),
                "repair the store filesystem and reopen cowshed",
            )
        })?;
    }
    fs::rename(old, new).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "cannot move {namespace} namespace {} to {}: {error}",
                old.display(),
                new.display()
            ),
            "repair the store filesystem and reopen cowshed",
        )
    })
}

/// Bring the binding at `path` onto the new identity, recording the old one as former.
///
/// Idempotent by inspection of what it already says: still the old identity means the rename is
/// owed, already the new one means it is done. Anything else is a binding this transaction has no
/// business rewriting, and is refused rather than overwritten.
#[cfg(target_os = "macos")]
fn converge_binding_identity(
    path: &Path,
    old_repo_id: &RepoId,
    new_repo_id: &RepoId,
) -> Result<()> {
    let binding: RepositoryBinding = crate::metadata::read_json(path).map_err(|error| {
        CowshedError::integrity(
            format!("cannot read repository binding {}: {error}", path.display()),
            "repair the repository binding, then reopen cowshed",
        )
    })?;
    let owned = binding.owned_repo_ids().map_err(native_integrity_error)?;
    if owned.current() == new_repo_id && owned.accepts(old_repo_id) {
        return Ok(());
    }
    if owned.current() != old_repo_id {
        return Err(CowshedError::integrity(
            format!(
                "repository binding {} names {owned}, which is neither {old_repo_id} nor \
                 {new_repo_id}",
                path.display()
            ),
            "repair the repository binding, then reopen cowshed",
        ));
    }
    let renamed = binding
        .rename_primary(new_repo_id.clone())
        .map_err(native_integrity_error)?;
    crate::metadata::write_json(path, &renamed).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "cannot publish repository binding {}: {error}",
                path.display()
            ),
            "repair the store filesystem and reopen cowshed",
        )
    })
}

/// Move every workspace image's store-side identity stamp onto the new identity.
///
/// The sidecar is always in reach — it lives beside its image in the store, mounted or not — and it
/// is what enumeration, publication recovery and mount-path derivation read. Leaving one behind
/// makes the image invisible to the project that owns it, which is why this walks the whole project
/// directory rather than only the workspaces the caller happened to enumerate.
#[cfg(target_os = "macos")]
fn rewrite_sidecar_identities(
    project_root: &Path,
    old_repo_id: &RepoId,
    new_repo_id: &RepoId,
) -> Result<()> {
    for sidecar in project_sidecars(project_root)? {
        let mut metadata: crate::metadata::DetachedWorkspaceMetadata =
            crate::metadata::read_json(&sidecar).map_err(|error| {
                CowshedError::integrity(
                    format!(
                        "cannot read workspace sidecar {}: {error}",
                        sidecar.display()
                    ),
                    "repair the workspace sidecar, then reopen cowshed",
                )
            })?;
        if metadata.repo_id != *old_repo_id {
            continue;
        }
        let image = crate::metadata::image_from_sidecar_path(&sidecar).ok_or_else(|| {
            CowshedError::integrity(
                format!("workspace sidecar {} names no image", sidecar.display()),
                "repair the workspace sidecar, then reopen cowshed",
            )
        })?;
        metadata.repo_id = new_repo_id.clone();
        metadata.validate(&image).map_err(|error| {
            CowshedError::integrity(
                format!(
                    "cannot validate workspace sidecar {}: {error}",
                    sidecar.display()
                ),
                "repair the workspace sidecar, then reopen cowshed",
            )
        })?;
        crate::metadata::write_json(&sidecar, &metadata).map_err(|error| {
            CowshedError::environment_missing(
                format!(
                    "cannot publish workspace sidecar {}: {error}",
                    sidecar.display()
                ),
                "repair the store filesystem and reopen cowshed",
            )
        })?;
    }
    Ok(())
}

/// Every detached-metadata sidecar under a project directory, including staged and retired images.
///
/// Symlinks are never followed and never descended: the store is cowshed's own tree, and a symlink
/// inside it is either not ours or is an attempt to make this walk write outside the project.
#[cfg(target_os = "macos")]
fn project_sidecars(root: &Path) -> Result<Vec<PathBuf>> {
    let mut sidecars = Vec::new();
    let mut pending = vec![root.to_owned()];
    while let Some(directory) = pending.pop() {
        let entries = match fs::read_dir(&directory) {
            Ok(entries) => entries,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => {
                return Err(CowshedError::environment_missing(
                    format!(
                        "cannot enumerate project directory {}: {error}",
                        directory.display()
                    ),
                    "repair the store filesystem and reopen cowshed",
                ));
            }
        };
        for entry in entries {
            let entry = entry.map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot read project directory entry in {}: {error}",
                        directory.display()
                    ),
                    "repair the store filesystem and reopen cowshed",
                )
            })?;
            let path = entry.path();
            let metadata = fs::symlink_metadata(&path).map_err(|error| {
                CowshedError::environment_missing(
                    format!("cannot inspect {}: {error}", path.display()),
                    "repair the store filesystem and reopen cowshed",
                )
            })?;
            if metadata.file_type().is_symlink() {
                continue;
            }
            if metadata.is_dir() {
                pending.push(path);
            } else if crate::metadata::image_from_sidecar_path(&path).is_some() {
                sidecars.push(path);
            }
        }
    }
    Ok(sidecars)
}

#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ProjectRootValidation {
    Strict,
    AllowDetachedMainRelocation,
}

#[cfg(target_os = "macos")]
fn validate_workspace_controller_root(
    derived: &crate::storage::lifecycle::DerivedWorkspace,
    metadata: &crate::metadata::DetachedWorkspaceMetadata,
    project_root: &Path,
    validation: ProjectRootValidation,
) -> std::result::Result<(), crate::storage::apfs::ApfsStorageError> {
    if !derived.workspace.name().is_main() {
        return Ok(());
    }
    let permits_relocated_root = matches!(
        validation,
        ProjectRootValidation::AllowDetachedMainRelocation
    ) && matches!(
        derived.mount_state,
        crate::storage::lifecycle::MountState::Detached
    );
    let info = &metadata.info_snapshot;
    if !permits_relocated_root && !names_one_root(&info.project_root, project_root) {
        return Err(crate::storage::apfs::ApfsStorageError::MarkerMismatch(
            format!(
                "persisted project root {} disagrees with controller root {}",
                info.project_root.display(),
                project_root.display()
            ),
        ));
    }
    Ok(())
}

#[cfg(target_os = "macos")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeRemovalGitFence {
    incarnation: WorkspaceIncarnation,
    head: GitOid,
    dirty: bool,
    in_progress: Option<String>,
}

/// Whether the branch that outlives a workspace already holds the workspace's work.
#[cfg(target_os = "macos")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeLandedState {
    branch: String,
    /// The measurement, or the reason there is none. A missing measurement is never landed: there
    /// is no shape here in which the absence of an answer can be read as a permissive one.
    commits: LandingCommits,
}

/// The branch a removal measures a session against, in the repository that holds it: main's
/// `main` for `rm`; for `land`'s retire, the branch the unit just landed on.
#[cfg(target_os = "macos")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeContainment {
    /// The target repository's canonical mount, whose object store the measurement borrows.
    mount: PathBuf,
    branch: String,
}

/// What a unit lands into or rebases onto: main, or a lane base.
#[cfg(target_os = "macos")]
#[derive(Clone, Debug, Eq, PartialEq)]
struct NativeLandingInto {
    name: WorkspaceName,
    /// Where Git runs to move the target: main's checkout, or the lane base's mount.
    root: PathBuf,
    /// The target's canonical mount, which the retire's containment check reads.
    mount: PathBuf,
}

#[cfg(target_os = "macos")]
impl NativeLandingInto {
    /// The branch a land moves when the caller names none: `main` for main, and whatever branch
    /// a lane base has checked out.
    async fn checked_out_branch(&self) -> Result<String> {
        if self.name.is_main() {
            return Ok(DEFAULT_LANDING_BRANCH.to_owned());
        }
        target_checked_out_branch(&self.root, &self.name).await
    }

    fn containment(&self, branch: String) -> NativeContainment {
        NativeContainment {
            mount: self.mount.clone(),
            branch,
        }
    }
}

#[cfg(target_os = "macos")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum NativeAdoptRollbackState {
    Retained,
    Swapped,
    Complete,
}

#[cfg(all(test, target_os = "macos"))]
mod rebase_recovery_tests {
    use super::*;
    use crate::error::FenceRefusal;
    use crate::fork_lock::Run as _;

    fn git(root: &Path, args: &[&str]) -> std::process::Output {
        std::process::Command::new("git")
            .args(args)
            .current_dir(root)
            .output_locked()
            .expect("run fixture git command")
    }

    fn run_git(root: &Path, args: &[&str]) {
        let output = git(root, args);
        assert!(
            output.status.success(),
            "git {} failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr)
        );
    }

    fn commit_file(root: &Path, contents: &str, message: &str) {
        std::fs::write(root.join("README.md"), contents).expect("write fixture file");
        run_git(root, &["add", "README.md"]);
        run_git(root, &["commit", "-m", message]);
    }

    #[tokio::test]
    async fn land_moves_only_the_branch_main_has_checked_out() {
        let root = crate::temp_root::TempRoot::new("cowshed-land-target");
        run_git(&root, &["init", "--initial-branch=main"]);
        run_git(&root, &["config", "user.name", "Cowshed Test"]);
        run_git(&root, &["config", "user.email", "cowshed@example.invalid"]);
        commit_file(&root, "base\n", "base");
        require_target_checked_out(&root, "main")
            .await
            .expect("main is checked out");

        run_git(&root, &["checkout", "-b", "probe"]);
        let error = require_target_checked_out(&root, "main")
            .await
            .expect_err("a checkout on another branch would move that branch");
        assert_eq!(error.code, crate::error::ErrorCode::Conflict);
        assert!(
            error.message.contains("branch probe") && error.message.contains("target main"),
            "the refusal names both branches: {}",
            error.message
        );
        assert!(
            error.hint.contains("check out main"),
            "the hint names the fix: {}",
            error.hint
        );
        assert_eq!(
            error.fence_source(),
            Some(&FenceRefusal::TargetNotCheckedOut {
                checked_out: Some("probe".to_owned())
            })
        );

        run_git(&root, &["checkout", "--detach"]);
        let error = require_target_checked_out(&root, "main")
            .await
            .expect_err("a detached checkout has no branch to move");
        assert!(
            error.message.contains("a detached HEAD"),
            "{}",
            error.message
        );
        assert_eq!(
            error.fence_source(),
            Some(&FenceRefusal::TargetNotCheckedOut { checked_out: None })
        );
    }

    #[tokio::test]
    async fn a_failed_rebase_restores_the_attached_branch_and_allows_the_next_rebase() {
        let root = crate::temp_root::TempRoot::new("cowshed-rebase-recovery");
        run_git(&root, &["init", "--initial-branch=main"]);
        run_git(&root, &["config", "user.name", "Cowshed Test"]);
        run_git(&root, &["config", "user.email", "cowshed@example.invalid"]);
        commit_file(&root, "base\n", "base");
        run_git(&root, &["checkout", "-b", "squashed-feature"]);
        commit_file(&root, "feature\n", "feature side");
        let source_head = git_oid(&root).await.expect("source head");
        run_git(&root, &["checkout", "main"]);
        commit_file(&root, "main\n", "main side");
        run_git(&root, &["checkout", "squashed-feature"]);

        let error = run_git_rebase_atomically(&root, "main", &source_head)
            .await
            .expect_err("the conflicting rebase fails");
        assert!(
            error.message.contains("could not apply"),
            "git's conflict remains the reported failure: {}",
            error.message
        );
        assert!(
            error.hint.contains("rolled back") && error.hint.contains("git rebase main"),
            "the hint names the by-hand replay, not markers the rollback removed: {}",
            error.hint
        );
        assert_eq!(
            error.fence_source(),
            Some(&FenceRefusal::ReplayConflicted {
                rolled_back_to: source_head.clone()
            }),
            "the fence names the head the rollback restored"
        );

        let branch = git(&root, &["symbolic-ref", "--short", "HEAD"]);
        assert!(
            branch.status.success(),
            "workspace HEAD remained detached after failed rebase: {}",
            String::from_utf8_lossy(&branch.stderr)
        );
        assert_eq!(
            String::from_utf8_lossy(&branch.stdout).trim(),
            "squashed-feature"
        );
        assert_eq!(git_oid(&root).await.expect("restored head"), source_head);
        assert!(!root.join(".git/rebase-merge").exists());
        assert!(!root.join(".git/rebase-apply").exists());

        run_git_rebase_atomically(&root, source_head.as_str(), &source_head)
            .await
            .expect("a following cowshed rebase can start");
    }

    #[tokio::test]
    async fn a_dirty_tree_refuses_even_under_autostash_instead_of_succeeding_over_conflicts() {
        let root = crate::temp_root::TempRoot::new("cowshed-rebase-autostash");
        run_git(&root, &["init", "--initial-branch=main"]);
        run_git(&root, &["config", "user.name", "Cowshed Test"]);
        run_git(&root, &["config", "user.email", "cowshed@example.invalid"]);
        // Set in the fixture rather than inherited from the host: a repository can enable it for
        // every clone through a tracked config include, and the verb must refuse either way.
        run_git(&root, &["config", "rebase.autoStash", "true"]);
        commit_file(&root, "base\n", "base");
        run_git(&root, &["checkout", "-b", "workspace"]);
        let source_head = git_oid(&root).await.expect("source head");
        run_git(&root, &["checkout", "main"]);
        commit_file(&root, "main\n", "main side");
        run_git(&root, &["checkout", "workspace"]);
        std::fs::write(root.join("README.md"), "uncommitted\n").expect("dirty the tree");

        let error = run_git_rebase_atomically(&root, "main", &source_head)
            .await
            .expect_err("a dirty tree is refused, not autostashed");
        assert_eq!(error.code, crate::error::ErrorCode::Conflict);
        assert!(
            error.hint.contains("uncommitted work"),
            "the refusal names the move: {}",
            error.hint
        );
        assert_eq!(
            error.fence_source(),
            Some(&FenceRefusal::SourceDirty {
                paths: vec![crate::api::dto::WorkspacePath::new("README.md").unwrap()],
                total: 1,
            }),
            "the fence names the tracked change that blocks the rebase"
        );

        let status = git(&root, &["status", "--porcelain"]);
        assert_eq!(
            String::from_utf8_lossy(&status.stdout),
            " M README.md\n",
            "the uncommitted change is still in place and nothing is unmerged"
        );
        assert_eq!(git_oid(&root).await.expect("unmoved head"), source_head);
        assert!(git(&root, &["stash", "list"]).stdout.is_empty());
    }

    /// A lane: main, a lane base cloned from it on `cowshed/<lane>`, and unit clones of the lane
    /// base on `cowshed/<unit>`, each its own repository exactly as cowshed's clones are.
    struct Lane {
        base: crate::temp_root::TempRoot,
        main: PathBuf,
        lane: PathBuf,
    }

    impl Lane {
        fn new(label: &str) -> Self {
            let base = crate::temp_root::TempRoot::new(&format!("cowshed-lane-{label}"));
            let main = base.join("main");
            std::fs::create_dir_all(&main).expect("create main");
            run_git(&main, &["init", "--initial-branch=main"]);
            Self::identify(&main);
            commit_file(&main, "base\n", "base");
            let lane = Self::clone_of(&base, &main, "lane");
            Self { base, main, lane }
        }

        fn identify(root: &Path) {
            run_git(root, &["config", "user.name", "Cowshed Test"]);
            run_git(root, &["config", "user.email", "cowshed@example.invalid"]);
        }

        fn clone_of(base: &Path, source: &Path, name: &str) -> PathBuf {
            let root = base.join(name);
            let output = std::process::Command::new("git")
                .args(["clone", "--quiet"])
                .arg(source)
                .arg(&root)
                .output_locked()
                .expect("clone");
            assert!(output.status.success(), "{output:?}");
            Self::identify(&root);
            run_git(&root, &["checkout", "-b", &format!("cowshed/{name}")]);
            root
        }

        fn unit(&self, name: &str) -> PathBuf {
            Self::clone_of(&self.base, &self.lane, name)
        }

        fn name(name: &str) -> WorkspaceName {
            WorkspaceName::new(name).expect("workspace name")
        }
    }

    fn unit_commit(root: &Path, file: &str, message: &str) {
        std::fs::write(root.join(file), format!("{message}\n")).expect("write unit file");
        run_git(root, &["add", file]);
        run_git(root, &["commit", "-m", message]);
    }

    async fn contained_in(
        target: &Path,
        branch: &str,
        unit: &Path,
    ) -> Result<Option<NativeLandedState>> {
        let head = git_oid(unit).await.expect("unit head");
        let resolved = crate::landing::resolve_target(target, branch).await;
        let landed = NativeLandedState {
            branch: branch.to_owned(),
            commits: crate::landing::measure_commits(&resolved, unit, head.as_str()).await,
        };
        removal_landed_decision(&Lane::name("unit"), &head, landed, false)
    }

    #[tokio::test]
    async fn a_unit_lands_into_its_lane_base_and_is_then_contained_there_not_in_main() {
        let lane = Lane::new("land");
        let unit = lane.unit("unit");
        unit_commit(&unit, "unit.txt", "unit work");
        let unit_head = git_oid(&unit).await.expect("unit head");
        let main_before = git_oid(&lane.main).await.expect("main head");

        deliver_into(
            &Lane::name("lane"),
            &lane.lane,
            &unit,
            &Lane::name("unit"),
            "cowshed/unit",
            &unit_head,
            "cowshed/lane",
        )
        .await
        .expect("land into the lane base");

        assert_eq!(
            git_oid(&lane.lane).await.expect("lane head"),
            unit_head,
            "the lane base's checked-out branch fast-forwarded to the unit"
        );
        assert_eq!(
            git_oid(&lane.main).await.expect("main head"),
            main_before,
            "main is untouched until the lane itself lands"
        );
        // The retire gate, measured against the branch the unit landed on, lets it go…
        assert!(
            contained_in(&lane.lane, "cowshed/lane", &unit)
                .await
                .expect("contained in the lane base")
                .is_none()
        );
        // …and the same gate measured against main would refuse it: that is why land passes the
        // target it delivered into, and why a standalone `rm` still measures against main.
        let refused = contained_in(&lane.main, "main", &unit)
            .await
            .expect_err("main does not hold the unit's commit");
        assert_eq!(refused.code, crate::error::ErrorCode::Conflict);
    }

    #[tokio::test]
    async fn a_land_into_a_lane_base_refuses_a_unit_that_moved_after_validation() {
        let lane = Lane::new("moved");
        let unit = lane.unit("unit");
        unit_commit(&unit, "unit.txt", "validated");
        let validated = git_oid(&unit).await.expect("validated head");
        unit_commit(&unit, "unit.txt", "unvalidated");
        let lane_before = git_oid(&lane.lane).await.expect("lane head");

        let error = deliver_into(
            &Lane::name("lane"),
            &lane.lane,
            &unit,
            &Lane::name("unit"),
            "cowshed/unit",
            &validated,
            "cowshed/lane",
        )
        .await
        .expect_err("what arrived is not what was validated");
        assert_eq!(error.code, crate::error::ErrorCode::Conflict);
        assert_eq!(
            error.fence_source(),
            Some(&FenceRefusal::SourceMoved {
                observed: git_oid(&unit).await.expect("unvalidated head")
            })
        );
        assert_eq!(git_oid(&lane.lane).await.expect("lane head"), lane_before);
    }

    #[tokio::test]
    async fn a_unit_behind_its_lane_base_is_told_to_rebase_into_it_and_then_lands() {
        let lane = Lane::new("rebase");
        let first = lane.unit("first");
        let second = lane.unit("second");
        unit_commit(&first, "first.txt", "first unit");
        let first_head = git_oid(&first).await.expect("first head");
        deliver_into(
            &Lane::name("lane"),
            &lane.lane,
            &first,
            &Lane::name("first"),
            "cowshed/first",
            &first_head,
            "cowshed/lane",
        )
        .await
        .expect("first unit lands");
        unit_commit(&second, "second.txt", "second unit");
        let second_head = git_oid(&second).await.expect("second head");
        let lane_before = git_oid(&lane.lane).await.expect("lane head");

        // The lane base moved past the second unit's base, so it cannot fast-forward; the next
        // move named is the rebase onto the lane base, since a bare rebase would go onto main.
        let behind = deliver_into(
            &Lane::name("lane"),
            &lane.lane,
            &second,
            &Lane::name("second"),
            "cowshed/second",
            &second_head,
            "cowshed/lane",
        )
        .await
        .expect_err("the second unit is not based on the lane base's tip");
        assert_eq!(behind.code, crate::error::ErrorCode::Conflict);
        assert_eq!(
            behind.fence_source(),
            Some(&FenceRefusal::NotFastForward {
                target_head: first_head.clone()
            })
        );
        assert!(
            behind.hint.contains("cowshed rebase second --into lane"),
            "{}",
            behind.hint
        );
        assert_eq!(git_oid(&lane.lane).await.expect("lane head"), lane_before);

        let onto = fetch_target_branch(&second, &lane.lane, &Lane::name("lane"))
            .await
            .expect("fetch the lane base's branch");
        run_git_rebase_atomically(&second, &onto, &second_head)
            .await
            .expect("rebase into the lane");

        let parent = git_revision_oid(&second, "HEAD^").await.expect("parent");
        assert_eq!(parent, first_head, "the second unit now sits on the first");
        assert!(second.join("first.txt").exists() && second.join("second.txt").exists());
        // And it lands into the lane base as a fast-forward.
        let rebased = git_oid(&second).await.expect("rebased head");
        deliver_into(
            &Lane::name("lane"),
            &lane.lane,
            &second,
            &Lane::name("second"),
            "cowshed/second",
            &rebased,
            "cowshed/lane",
        )
        .await
        .expect("second unit lands");
        assert_eq!(git_oid(&lane.lane).await.expect("lane head"), rebased);
    }

    #[test]
    fn a_lane_base_recreated_under_its_name_is_refused() {
        let resolved = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap();
        let recreated = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c81").unwrap();
        let lane = Lane::name("lane");
        require_target_incarnation(&lane, &resolved, &resolved).expect("the same workspace");
        let error = require_target_incarnation(&lane, &recreated, &resolved)
            .expect_err("a recreated lane base is another workspace");
        assert_eq!(error.code, crate::error::ErrorCode::Conflict);
        assert_eq!(
            error.fence_source(),
            Some(&FenceRefusal::IncarnationMoved {
                workspace: lane.clone(),
                observed: recreated.clone(),
            })
        );
        assert!(
            error.hint.contains("resolve workspace lane again"),
            "{}",
            error.hint
        );
    }

    #[tokio::test]
    async fn a_target_tree_with_work_the_fast_forward_would_overwrite_names_it() {
        let lane = Lane::new("target-dirty");
        let unit = lane.unit("unit");
        commit_file(&unit, "unit\n", "unit rewrites the readme");
        let unit_head = git_oid(&unit).await.expect("unit head");
        let lane_before = git_oid(&lane.lane).await.expect("lane head");
        std::fs::write(lane.lane.join("README.md"), "lane edit\n").expect("dirty the lane base");

        let error = deliver_into(
            &Lane::name("lane"),
            &lane.lane,
            &unit,
            &Lane::name("unit"),
            "cowshed/unit",
            &unit_head,
            "cowshed/lane",
        )
        .await
        .expect_err("the fast-forward would overwrite the lane base's uncommitted edit");
        assert_eq!(
            error.fence_source(),
            Some(&FenceRefusal::TargetDirty {
                paths: vec![crate::api::dto::WorkspacePath::new("README.md").unwrap()],
                total: 1,
            })
        );
        assert_eq!(git_oid(&lane.lane).await.expect("lane head"), lane_before);
        assert_eq!(
            std::fs::read_to_string(lane.lane.join("README.md")).expect("lane readme"),
            "lane edit\n"
        );
    }

    #[test]
    fn each_head_fence_carries_the_head_it_observed() {
        let expected = GitOid::new("1".repeat(40)).unwrap();
        let observed = GitOid::new("2".repeat(40)).unwrap();
        require_source_head(None, &observed, "land").expect("no expectation");
        require_source_head(Some(&observed), &observed, "land").expect("as expected");
        let error =
            require_source_head(Some(&expected), &observed, "land").expect_err("the source moved");
        assert_eq!(error.code, crate::error::ErrorCode::Conflict);
        assert_eq!(
            error.fence_source(),
            Some(&FenceRefusal::SourceMoved {
                observed: observed.clone()
            })
        );

        require_onto_head(Some(&observed), "main/main", &observed).expect("as expected");
        let error = require_onto_head(Some(&expected), "main/main", &observed)
            .expect_err("the destination moved");
        assert_eq!(
            error.fence_source(),
            Some(&FenceRefusal::OntoMoved {
                observed: observed.clone()
            })
        );

        use crate::api::dto::ExpectedRefHead;
        require_target_head(Some(&ExpectedRefHead::Missing), None).expect("still unborn");
        let born = require_target_head(Some(&ExpectedRefHead::Missing), Some(&observed))
            .expect_err("the target was born");
        assert_eq!(
            born.fence_source(),
            Some(&FenceRefusal::TargetMoved {
                observed: Some(observed.clone())
            })
        );
        let gone = require_target_head(Some(&ExpectedRefHead::Oid(expected)), None)
            .expect_err("the target branch is gone");
        assert_eq!(
            gone.fence_source(),
            Some(&FenceRefusal::TargetMoved { observed: None })
        );
    }

    #[test]
    fn a_unit_has_one_destination_and_it_is_not_itself() {
        let lane = WorkspaceTarget::new(
            Lane::name("lane"),
            WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap(),
        );
        let onto = crate::api::dto::RevisionTarget::Oid(
            GitOid::new("1111111111111111111111111111111111111111").unwrap(),
        );
        let both = require_single_destination(Some(&onto), Some(&lane))
            .expect_err("onto and into are exclusive");
        assert_eq!(both.code, crate::error::ErrorCode::Usage);
        require_single_destination(Some(&onto), None).expect("onto alone");
        require_single_destination(None, Some(&lane)).expect("into alone");

        let itself = require_distinct_target(&Lane::name("unit"), &Lane::name("unit"))
            .expect_err("a unit does not land into itself");
        assert_eq!(itself.code, crate::error::ErrorCode::Usage);
        require_distinct_target(&Lane::name("unit"), &Lane::name("lane")).expect("its lane base");
    }
}
#[cfg(target_os = "macos")]
#[async_trait]
impl ProjectRuntimeHost for NativeProjectRuntimeHost {
    fn descriptor(&self) -> &ProjectDescriptor {
        &self.descriptor
    }

    fn release_intent_leases(&mut self) {
        self.intent_leases.clear();
    }

    async fn recover(&mut self) -> Result<()> {
        // One inventory read for the whole recovery. A detached direct-mounted main has no Git
        // repository at its recorded checkout by definition; its session marker and persisted
        // binding were validated during open, so querying remotes there would prevent the move
        // operation that repairs it. Every other state retains the ordinary live-Git check.
        let authoritative = crate::timing::spanned(
            "recover",
            "inventory",
            self.authoritative_allowing_detached_main_relocation(),
        )
        .await?;
        let detached_direct_main = authoritative.iter().any(|workspace| {
            workspace.derived.workspace.name().is_main()
                && matches!(
                    workspace.derived.mount_state,
                    crate::storage::lifecycle::MountState::Detached
                )
        });
        if !detached_direct_main {
            crate::timing::spanned("recover", "binding", self.validate_binding()).await?;
        }
        // Intent recovery finishes interrupted create/fork/remove work by creating and destroying
        // images. Workspace supervisors are the daemon's, started by the first command a workspace
        // gets: opening a project starts none, and waits on none. An inspecting host replays
        // nothing; doctor reports each unfinished intent instead.
        if self.recovery_scope.repairs() {
            crate::timing::spanned("recover", "intents", self.recover_lifecycle_intents()).await?;
        }
        Ok(())
    }

    async fn snapshots(&mut self) -> Result<Vec<WorkspaceSnapshot>> {
        self.validate_binding().await?;
        self.authoritative()
            .await?
            .iter()
            .map(|workspace| self.snapshot(workspace))
            .collect()
    }

    async fn adopt(&mut self, options: AdoptOptions) -> Result<WorkspaceSnapshot> {
        use crate::storage::lifecycle::LifecyclePlanner;
        let intent = crate::storage::recovery::LifecycleIntent::Adopt {
            options: options.clone(),
        };
        // Publication is main's sidecar activation, and the checkout swap and mount follow it. An
        // adoption that published main and then stopped is finished, not repeated — before the
        // binding gate, which reads Git at a checkout path the swap may already have vacated.
        let unfinished = self
            .lifecycle_intents
            .get(intent.target())
            .is_some_and(|record| record.operation == intent && record.completion.is_none());
        if unfinished {
            match self.current(&main_name()).await {
                Ok(current) => return self.finish_adoption(current).await,
                Err(error) if error.code == ErrorCode::NotFound => {}
                Err(error) => return Err(error),
            }
        }
        timed_async("adopt", "binding", self.validate_binding()).await?;
        if let Some(expected) = self.completed_workspace_intent(&intent).cloned() {
            let current = self.current(&main_name()).await?;
            Self::require_exact_incarnation(&current, &expected)?;
            return self.snapshot(&current);
        }

        if !timed_async("adopt", "inventory", self.authoritative())
            .await?
            .is_empty()
        {
            return Err(CowshedError::conflict(
                "repository is already adopted",
                "list the existing main workspace",
            ));
        }
        timed_async(
            "adopt",
            "identity-owner",
            refuse_identity_owned_by_a_live_project(
                &self.substrate_config.store_root,
                &self.descriptor.repo_id,
                &self.descriptor.repo_id,
            ),
        )
        .await?;
        if options
            .repo_id
            .as_ref()
            .is_some_and(|repo| repo != &self.descriptor.repo_id)
        {
            return Err(CowshedError::conflict(
                "adopt repository identity differs from the bound remote",
                "retry with the bound repository identity",
            ));
        }
        timed_async(
            "adopt",
            "secrets",
            enforce_adopt_secret_policy(
                self.descriptor.git_root.to_path_buf(),
                self.layout.project().waivers.clone(),
                self.layout.project().quarantine.clone(),
                options.quarantine,
            ),
        )
        .await?;
        // Capacity is fixed for the image's lifetime at creation; `cowshed resize` is what moves
        // it afterwards. An unset option means the project default rather than "no capacity".
        let capacity = match options.capacity.as_deref() {
            Some(requested) => parse_capacity(requested)?,
            None => self.substrate_config.capacity,
        };
        let pre_cowshed = pre_cowshed_path(&self.descriptor.git_root)?;
        timed_async("adopt", "intent", self.begin_lifecycle_intent(intent)).await?;

        let reservation = timed_async("adopt", "grants", self.fresh_grants()).await?;
        let mut grants = reservation.grants.clone();
        grants.revision = 0;
        let identity = self
            .operation_identity(grants, self.git.current_branch().await?, None, false)
            .await?;
        let plan = self
            .substrate
            .plan_adopt(crate::storage::lifecycle::AdoptRequest {
                repo: self.descriptor.repo_id.clone(),
                capacity,
                topology_revision: crate::storage::lifecycle::Revision::new(0),
                source_checkout: self.descriptor.git_root.to_path_buf(),
                pre_cowshed_checkout: pre_cowshed,
                identity,
            })
            .map_err(native_integrity_error)?;
        // Before main's first build volume is minted below: from here until the intent
        // completes, collection in any process leaves that volume alone, which nothing links and
        // no record names until main's first touch links it.
        self.mark_lifecycle_intent_mutating(&main_name()).await?;
        let binding = self.descriptor.binding.clone();
        let binding_path = self.layout.project().repository_binding.clone();
        let home = &self.home;
        // Main's first build volume is minted beside main's image, so the two attaches wait on
        // `storagekitd` once (16_build_volumes.md, "Targets and seeds"), at the capacity the
        // checkout's `.cowshed.toml` asks, which main's image is copied from; none for a checkout
        // in which nothing can name build state. The answer owns the volume until main's first
        // touch has run, and releases it when the adopt is dropped first.
        let volumes = self.build_volumes()?;
        let mint = crate::timing::spanned(
            "adopt",
            "build-volume",
            volumes.mint_first(
                self.descriptor.git_root.to_path_buf(),
                self.workspace_mount_path(&main_name())?,
            ),
        );
        let (receipt, first) = self
            .substrate
            .execute_adopt_staged(
                plan,
                crate::storage::apfs::Alongside {
                    work: mint,
                    abandon: super::build_volumes::FirstMint::abandon,
                },
                move |stage| async move {
                    timed_async(
                        "adopt",
                        "locks",
                        crate::inherited_git_locks::discard_in(&stage.mount_point),
                    )
                    .await?;
                    timed_async(
                        "adopt",
                        "daemons",
                        crate::inherited_daemons::macos::discard_in(
                            &stage.mount_point,
                            crate::capabilities::mint_daemon_states(&stage.mount_point, home)?,
                        ),
                    )
                    .await?;
                    let repository = crate::git::GitRepository::from_root(&stage.mount_point);
                    // The image's volume is case-sensitive; the tree it was copied from may not be.
                    timed_async(
                        "adopt",
                        "case",
                        repository.record_case_sensitive_filesystem(),
                    )
                    .await?;
                    crate::storage::lifecycle::dispatch_blocking(move || {
                        crate::metadata::write_json(&binding_path, &binding)
                    })
                    .await
                    .map_err(|error| CowshedError::internal(error.to_string()))?
                    .map_err(native_integrity_error)
                },
            )
            .await
            .map_err(native_staged_error)?;
        // Published, main's image owns its block: the kernel claim ends here, before anything
        // that lets another process's gateway reconcile install main's session on the block.
        drop(reservation);
        self.conclude_adoption(&receipt.workspace, first).await
    }

    async fn create(
        &mut self,
        workspace: WorkspaceName,
        options: CreateOptions,
    ) -> Result<WorkspaceSnapshot> {
        use super::supervisor::CommitmentSink;
        use crate::storage::lifecycle::LifecyclePlanner;
        self.validate_binding().await?;
        let intent = crate::storage::recovery::LifecycleIntent::Create {
            workspace: workspace.clone(),
            options: options.clone(),
        };
        if let Some(expected) = self.completed_workspace_intent(&intent).cloned() {
            let current = self.current(&workspace).await?;
            Self::require_exact_incarnation(&current, &expected)?;
            return self.snapshot(&current);
        }

        if self
            .authoritative()
            .await?
            .iter()
            .any(|current| current.derived.workspace.name() == &workspace)
        {
            return Err(CowshedError::conflict(
                format!("workspace {workspace} already exists"),
                "choose another workspace name",
            ));
        }
        let source_name = options.from_workspace.clone().unwrap_or_else(main_name);
        let source = self.current(&source_name).await?;
        // Git-worktree-ness is inherited, not just requested. A clone of a git-worktree workspace
        // carries no repository of its own — only a pointer file naming the *source's*
        // registration — so it has to be re-registered as one whatever the caller asked for.
        let git_worktree = options.git_worktree || is_git_worktree(&source.metadata);
        if git_worktree {
            self.require_main_mounted_for_git_worktree(&workspace)
                .await?;
        }
        if options.register && git_worktree {
            return Err(CowshedError::usage(
                "--register has nothing to fetch on a git-worktree workspace: main already holds its branch",
                format!("cowshed new {workspace} --git-worktree"),
            ));
        }
        // `--ref` is resolved before anything is journaled, in the repository the clone inherits:
        // the source's, which for a git-worktree workspace is main's. A revision that repository
        // does not hold is the caller's mistake, not an operation recovery could ever finish;
        // journaled, every later replay re-ran the same failing `git switch` and wedged the name.
        // The clone then branches from the resolved commit itself, so what was checked is what is
        // used — after the clone drops its inherited remotes, a remote-tracking name would no
        // longer resolve there.
        let start = match options.revision.as_ref() {
            Some(revision) => Some(
                self.resolve_create_start(&source, &source_name, revision)
                    .await?,
            ),
            None => None,
        };
        // The slot is recorded before anything derives a mount path, because the record is what
        // decides where this workspace mounts. It is part of the durable lifecycle intent: an
        // exact retry binds it idempotently, while releasing it after a PendingFence clone exists
        // could let another workspace claim the slot and make recovery impossible.
        let slot = options
            .slot
            .map(crate::metadata::SlotId::new)
            .transpose()
            .map_err(|error| {
                CowshedError::usage(
                    error.to_string(),
                    "choose a slot within the project's range",
                )
            })?;
        let resuming = self
            .lifecycle_intents
            .get(&workspace)
            .is_some_and(|record| record.operation == intent && record.completion.is_none());
        // The intent precedes every mutation but stays `Prepared` until the verb has something
        // durable to recover: it turns `Mutating` once the slot is bound, or else just before the
        // staged clone writes its `PendingFence` sidecar. A failure while still `Prepared` was a
        // refusal and withdraws the intent; a `Mutating` one stays for recovery to finish.
        let superseded = self.begin_lifecycle_intent(intent).await?;
        let outcome = async {
            if let Some(slot) = slot {
                self.bind_slot(&workspace, slot).await?;
                self.mark_lifecycle_intent_mutating(&workspace).await?;
            }
            let (grants, reservation) = self.destination_grants(&workspace, resuming).await?;
            let identity = self
                .operation_identity(
                    grants,
                    Some(format!("cowshed/{workspace}")),
                    None,
                    git_worktree,
                )
                .await?;
            let plan = self
                .substrate
                .plan_create(
                    &source.derived.workspace,
                    crate::storage::lifecycle::Destination {
                        repo: self.descriptor.repo_id.clone(),
                        name: workspace.clone(),
                        topology_revision: source.derived.workspace.topology_revision(),
                        identity,
                    },
                )
                .map_err(native_integrity_error)?;
            // Main's canonical mount, never `descriptor.git_root`: under the symlink layout the
            // recorded checkout is a symlink outside this workspace's read grants that dangles as
            // soon as the checkout moves, and only the canonical mount is maintained by
            // `cowshed mv`.
            let main_mount = self.workspace_mount_path(&main_name())?;
            // The tree the image came from: main under a plain `new`, the sibling under `--from`.
            // It produced every byte the clone inherited, so it is what inherited links and the
            // Git identity resolve against — never main unless main is the source.
            let source_mount = self.workspace_mount_path(&source_name)?;
            let repository_shape = if git_worktree {
                crate::git::WorkspaceRepository::LinkedWorktree
            } else {
                crate::git::WorkspaceRepository::Standalone
            };
            let start = start.as_ref().map(GitOid::as_str);
            let destination = workspace.clone();
            if slot.is_none() {
                self.mark_lifecycle_intent_mutating(&workspace).await?;
            }
            let home = &self.home;
            let build_volumes = self.build_volumes()?;
            // Under the source's image lock, which every fork of the source holds while its
            // clone is staged: the seed the fork clones is first the source's own work up to
            // now, unless something still writes the source's volume. The fork's live volume is
            // attached beside the clone's own attach, so the two wait on `storagekitd` once.
            let preparing = build_volumes.clone();
            let abandoning = build_volumes.clone();
            let prepare = {
                let (source_mount, destination) = (source_mount.clone(), destination.clone());
                let seed_source = super::build_volumes::Owner {
                    name: source_name.clone(),
                    incarnation: source.derived.workspace.incarnation().clone(),
                };
                async move {
                    timed_async(
                        "new",
                        "build-volume",
                        preparing.prepare_fork(seed_source, source_mount, destination),
                    )
                    .await
                }
            };
            let receipt = self
                .substrate
                .execute_create_staged(
                    plan,
                    crate::storage::apfs::Alongside {
                        work: prepare,
                        abandon: move |prepared| async move {
                            abandoning.abandon_fork(prepared).await;
                        },
                    },
                    move |stage, prepared| async move {
                        // Before any Git runs in the clone: a lock a source writer held when the
                        // image was cloned is held by nobody here, and would refuse every write to
                        // the file it guards.
                        timed_async(
                            "new",
                            "locks",
                            crate::inherited_git_locks::discard_in(&stage.mount_point),
                        )
                        .await?;
                        timed_async(
                            "new",
                            "daemons",
                            crate::inherited_daemons::macos::discard_in(
                                &stage.mount_point,
                                crate::capabilities::mint_daemon_states(&stage.mount_point, home)?,
                            ),
                        )
                        .await?;
                        timed_async(
                            "new",
                            "build-volume-seed",
                            build_volumes.finish_fork(
                                prepared?,
                                super::build_volumes::Owner {
                                    name: destination.clone(),
                                    incarnation: stage.workspace.incarnation().clone(),
                                },
                                stage.mount_point.clone(),
                            ),
                        )
                        .await?;
                        crate::git::GitRepository::from_root(&stage.mount_point)
                            .mint_workspace(
                                &destination.to_string(),
                                crate::git::CloneOrigin {
                                    source: &source_mount,
                                    main: &main_mount,
                                },
                                repository_shape,
                                start,
                                stage.resuming,
                            )
                            .await
                    },
                )
                .await
                .map_err(native_staged_error)?;
            // Published, the image owns its block: the kernel claim ends with publication, or a
            // gateway reconcile in another process installing the workspace's session finds the
            // block's base port taken and moves the workspace to another block (05_gateway.md).
            drop(reservation);
            self.mark_lifecycle_fence_complete(&workspace).await;
            if options.register {
                self.register_workspace_in_main(&workspace).await?;
            }
            timed_async(
                "new",
                "commitment",
                self.commitments
                    .record(super::supervisor::CommitmentDraft::WorkspaceIntroduced {
                        repo_id: self.descriptor.repo_id.clone(),
                        workspace_incarnation: receipt.workspace.incarnation().clone(),
                    }),
            )
            .await?;
            timed_async(
                "new",
                "journal",
                self.complete_lifecycle_intent(
                    &workspace,
                    crate::storage::recovery::LifecycleIntentCompletion::Workspace(
                        receipt.workspace.incarnation().clone(),
                    ),
                ),
            )
            .await?;
            timed_async("new", "supervisor", self.ensure_supervisor(&workspace)).await?;
            timed_async("new", "snapshot", self.snapshot_named(&workspace)).await
        }
        .await;
        if outcome.is_err() {
            self.withdraw_refused_clone_intent(&workspace, superseded)
                .await;
        }
        outcome
    }

    async fn workspace_at(&mut self, path: PathBuf) -> Result<WorkspaceSnapshot> {
        self.validate_binding().await?;
        let workspaces = self.authoritative().await?;
        let active_mounts = workspaces
            .iter()
            .enumerate()
            .filter(|(_, workspace)| {
                matches!(
                    workspace.derived.mount_state,
                    crate::storage::lifecycle::MountState::Mounted { .. }
                )
            })
            .map(|(index, workspace)| {
                self.workspace_mount_path(workspace.derived.workspace.name())
                    .map(|mount| (index, mount))
            })
            .collect::<Result<Vec<_>>>()?;
        let requested = path.clone();
        let matching = crate::storage::lifecycle::dispatch_blocking(move || {
            let requested = std::fs::canonicalize(&requested).map_err(|error| {
                CowshedError::not_found(
                    format!(
                        "workspace path {} is not accessible: {error}",
                        requested.display()
                    ),
                    "retry from inside an attached workspace",
                )
            })?;
            let mut matching = Vec::new();
            for (index, mount) in active_mounts {
                let mount = std::fs::canonicalize(&mount).map_err(|error| {
                    CowshedError::integrity(
                        format!(
                            "authoritatively mounted workspace path {} is not accessible: {error}",
                            mount.display()
                        ),
                        "run cowshed doctor --json",
                    )
                })?;
                if requested.starts_with(&mount) {
                    matching.push(index);
                }
            }
            Ok::<_, CowshedError>(matching)
        })
        .await
        .map_err(|error| {
            CowshedError::internal(format!("workspace path task failed: {error}"))
        })??;
        match matching.as_slice() {
            [index] => self.snapshot(&workspaces[*index]),
            [] => Err(CowshedError::not_found(
                format!(
                    "{} is not contained in an active workspace mount for project {}",
                    path.display(),
                    self.descriptor.repo_id
                ),
                "retry from inside an attached workspace",
            )),
            _ => Err(CowshedError::conflict(
                format!(
                    "{} is contained in multiple active workspace mounts",
                    path.display()
                ),
                "repair overlapping workspace mounts and retry",
            )),
        }
    }

    async fn fork(
        &mut self,
        source: WorkspaceName,
        destination: WorkspaceName,
    ) -> Result<WorkspaceSnapshot> {
        use super::supervisor::{CommitmentDraft, CommitmentSink};
        use crate::storage::lifecycle::LifecyclePlanner;
        self.validate_binding().await?;
        let intent = crate::storage::recovery::LifecycleIntent::Fork {
            source: source.clone(),
            destination: destination.clone(),
        };
        if let Some(expected) = self.completed_workspace_intent(&intent).cloned() {
            let current = self.current(&destination).await?;
            Self::require_exact_incarnation(&current, &expected)?;
            return self.snapshot(&current);
        }

        let source_fact = self.current(&source).await?;
        if self
            .authoritative()
            .await?
            .iter()
            .any(|current| current.derived.workspace.name() == &destination)
        {
            return Err(CowshedError::conflict(
                format!("workspace {destination} already exists"),
                "choose another workspace name",
            ));
        }
        // A fork of a git-worktree workspace is one too, and has to be: the cloned image carries
        // a pointer file naming the *source's* registration, so the destination is re-registered
        // under its own id rather than left as a second claim on one worktree.
        let source_is_git_worktree = is_git_worktree(&source_fact.metadata);
        if source_is_git_worktree {
            self.require_main_mounted_for_git_worktree(&destination)
                .await?;
        }
        let resuming = self
            .lifecycle_intents
            .get(&destination)
            .is_some_and(|record| record.operation == intent && record.completion.is_none());
        // `Prepared` until just before the staged clone writes its `PendingFence` sidecar, so a
        // refusal before it (no port block left, say) withdraws the intent; see `create`.
        let superseded = self.begin_lifecycle_intent(intent).await?;
        let outcome = async {
            let (grants, reservation) = self.destination_grants(&destination, resuming).await?;
            let identity = self
                .operation_identity(
                    grants,
                    Some(format!("cowshed/{destination}")),
                    Some(source.clone()),
                    source_is_git_worktree,
                )
                .await?;
            let main_mount = self.workspace_mount_path(&main_name())?;
            let source_mount = self.workspace_mount_path(&source)?;
            let forked = destination.clone();
            let plan = self
                .substrate
                .plan_fork(
                    &source_fact.derived.workspace,
                    crate::storage::lifecycle::Destination {
                        repo: self.descriptor.repo_id.clone(),
                        name: destination.clone(),
                        topology_revision: source_fact.derived.workspace.topology_revision(),
                        identity,
                    },
                )
                .map_err(native_integrity_error)?;
            self.mark_lifecycle_intent_mutating(&destination).await?;
            let home = &self.home;
            let build_volumes = self.build_volumes()?;
            // Under the source's image lock, as for `create`: the source's seed first catches up
            // with its live volume, and the fork's live volume attaches beside the clone.
            let preparing = build_volumes.clone();
            let abandoning = build_volumes.clone();
            let prepare = {
                let (source_mount, forked) = (source_mount.clone(), forked.clone());
                let seed_source = super::build_volumes::Owner {
                    name: source.clone(),
                    incarnation: source_fact.derived.workspace.incarnation().clone(),
                };
                async move {
                    timed_async(
                        "fork",
                        "build-volume",
                        preparing.prepare_fork(seed_source, source_mount, forked),
                    )
                    .await
                }
            };
            let receipt = self
                .substrate
                .execute_fork_staged(
                    plan,
                    crate::storage::apfs::Alongside {
                        work: prepare,
                        abandon: move |prepared| async move {
                            abandoning.abandon_fork(prepared).await;
                        },
                    },
                    move |stage, prepared| async move {
                        crate::inherited_git_locks::discard_in(&stage.mount_point).await?;
                        crate::inherited_daemons::macos::discard_in(
                            &stage.mount_point,
                            crate::capabilities::mint_daemon_states(&stage.mount_point, home)?,
                        )
                        .await?;
                        build_volumes
                            .finish_fork(
                                prepared?,
                                super::build_volumes::Owner {
                                    name: forked.clone(),
                                    incarnation: stage.workspace.incarnation().clone(),
                                },
                                stage.mount_point.clone(),
                            )
                            .await?;
                        let repository = crate::git::GitRepository::from_root(&stage.mount_point);
                        repository.restore_inherited_links(&source_mount).await?;
                        if source_is_git_worktree {
                            repository
                                .adopt_as_linked_worktree_resumable(
                                    &forked.to_string(),
                                    &main_mount,
                                    None,
                                    stage.resuming,
                                )
                                .await?;
                        }
                        Ok::<_, CowshedError>(())
                    },
                )
                .await
                .map_err(native_staged_error)?;
            // Published, the image owns its block; see `create`.
            drop(reservation);
            self.mark_lifecycle_fence_complete(&destination).await;
            self.commitments
                .record(CommitmentDraft::Fork {
                    repo_id: self.descriptor.repo_id.clone(),
                    source_incarnation: source_fact.derived.workspace.incarnation().clone(),
                    destination_incarnation: receipt.workspace.incarnation().clone(),
                })
                .await?;
            self.complete_lifecycle_intent(
                &destination,
                crate::storage::recovery::LifecycleIntentCompletion::Workspace(
                    receipt.workspace.incarnation().clone(),
                ),
            )
            .await?;
            self.ensure_supervisor(&destination).await?;
            self.snapshot_named(&destination).await
        }
        .await;
        if outcome.is_err() {
            self.withdraw_refused_clone_intent(&destination, superseded)
                .await;
        }
        outcome
    }

    /// Renaming is a lifecycle operation for the same reason removal is: the name decides the
    /// image path, the volume label, the marker, and the mount point, and a workspace cannot be
    /// renamed out from under its own mount.
    ///
    /// It is composed rather than open-coded. A fork to the destination already clones the image,
    /// mints a fresh incarnation, relabels the volume, rewrites the marker, and publishes the
    /// result under one crash-safe transaction with its commitment; retiring the source is the
    /// other half. Same-volume `clonefile` makes the copy free, so the composition costs an
    /// incarnation and nothing else — and it inherits both transactions' recovery instead of
    /// needing its own.
    async fn rename(
        &mut self,
        source: WorkspaceName,
        destination: WorkspaceName,
    ) -> Result<WorkspaceSnapshot> {
        if source.is_main() || destination.is_main() {
            return Err(CowshedError::usage(
                "main cannot be renamed; its name is fixed by the project layout",
                "move the project checkout instead: cowshed mv main <path>",
            ));
        }
        if source == destination {
            return Err(CowshedError::usage(
                format!("workspace {source} already has that name"),
                "choose a different destination name",
            ));
        }
        self.validate_binding().await?;
        let current = self.current(&source).await?;
        if self
            .authoritative()
            .await?
            .iter()
            .any(|existing| existing.derived.workspace.name() == &destination)
        {
            return Err(CowshedError::conflict(
                format!("workspace {destination} already exists"),
                "choose another destination name, or remove the occupant first",
            ));
        }

        // Fence before either half runs. The source is about to be retired, so uncommitted or
        // in-progress work has to be refused here rather than discovered halfway through.
        let fence = self.removal_git_fence(&current).await?;
        if fence.dirty || fence.in_progress.is_some() {
            return Err(CowshedError::conflict(
                format!("workspace {source} has uncommitted or in-progress Git work"),
                format!("commit or stash the work, then retry: cowshed mv {source} {destination}"),
            ));
        }

        self.fork(source.clone(), destination.clone()).await?;
        // The source's commits are not being discarded, they are being republished under the
        // destination, whose image is a copy of this one — so the landed-ancestry gate that guards
        // a real removal would refuse a rename that loses nothing. This retires the source directly
        // instead of laundering that through a removal override: the fork is the preservation, and
        // the fence above is what makes the retirement safe.
        let retirement = async {
            let (retiring, _, _stopped) = self
                .revalidated_removal_fence(&source, &fence, false)
                .await?;
            self.finish_retirement(retiring).await
        }
        .await;
        if let Err(error) = retirement {
            // The destination now holds the work; leaving the source usable is the recoverable
            // half of a half-done rename.
            let _ = self.ensure_supervisor(&source).await;
            return Err(error);
        }
        self.snapshot_named(&destination).await
    }

    /// Move the project's checkout to `destination`, the `main` half of `cowshed mv`.
    ///
    /// The checkout path *is* main's mountpoint. A mounted main is detached, its stub directory is
    /// renamed, the substrate is rebound, and the image is re-attached. A main that was already
    /// detached is recovered from its image and detached sidecar instead: the old path need not
    /// exist, and no Git command is sent there. In either case the destination fact is durable
    /// before the final mount, so a crash can only leave a forward-recoverable detach.
    ///
    /// The durable record is rewritten **before** the tree moves: the source path stops existing
    /// the instant the rename lands, so a record still naming it would be unrecoverable — nothing
    /// left to resolve — whereas a record naming the destination becomes true the moment the
    /// rename completes, and `attach` converges the rest. Recording ahead of the move is what makes
    /// the crash window recoverable in the forward direction instead of the dead one.
    async fn move_checkout(&mut self, destination: PathBuf) -> Result<WorkspaceSnapshot> {
        use crate::storage::lifecycle::{MountIntent, Substrate};

        let main = main_name();
        let source = self.substrate_config.checkout_path.clone();
        let current = self
            .authoritative_allowing_detached_main_relocation()
            .await?
            .into_iter()
            .find(|workspace| workspace.derived.workspace.name() == &main)
            .ok_or_else(|| {
                CowshedError::not_found(
                    "workspace main does not exist",
                    "list published workspaces and retry",
                )
            })?;
        let detached = matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Detached
        );
        // A detached main has no repository at the recorded checkout path. Its persisted binding
        // was validated while opening from the session marker; querying Git here would turn the
        // exact recovery state into "cannot change to <old path>".
        if !detached {
            self.validate_binding().await?;
        }
        self.validate_move_destination(&source, &destination)
            .await?;

        let mount_point = self.workspace_mount_path(&main)?;
        if !detached && !crate::checkout::resolves_to(&source, &mount_point) {
            return Err(CowshedError::conflict(
                format!(
                    "the recorded checkout {} does not resolve to main's mount {}",
                    source.display(),
                    mount_point.display()
                ),
                "cowshed doctor --json",
            ));
        }
        let record = self.checkout_record()?;

        if detached {
            let prepare_record = record.clone();
            let prepare_source = source.clone();
            let prepare_destination = destination.clone();
            crate::storage::lifecycle::dispatch_blocking(move || {
                prepare_detached_checkout_relocation(
                    &prepare_record,
                    &prepare_source,
                    &prepare_destination,
                )
            })
            .await
            .map_err(|error| {
                CowshedError::internal(format!("detached checkout move task failed: {error}"))
            })??;

            self.rebind_checkout(&destination)?;
            let current = self.current(&main).await?;
            self.substrate
                .ensure_mounted(&current.derived.workspace, MountIntent { browse: false })
                .await
                .map_err(native_storage_error)?;
            self.advance_gateway_revision(&current).await?;
            let mounted_record = self.checkout_record()?;
            let mounted_destination = destination.clone();
            crate::storage::lifecycle::dispatch_blocking(move || {
                mounted_record.rewrite_project_root(&mounted_destination)
            })
            .await
            .map_err(|error| {
                CowshedError::internal(format!("checkout record task failed: {error}"))
            })?
            .map_err(native_integrity_error)?;
            self.repair_workspace_records(&destination).await?;
            self.ensure_supervisor(&main).await?;
            return self.snapshot_named(&main).await;
        }

        // The record moves first; see the method comment for why this direction is the recoverable
        // one. It is also the only step that can fail for a reason the filesystem cannot undo, so
        // failing here costs nothing but a refusal.
        let rewrite_record = record.clone();
        let rewrite_destination = destination.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            rewrite_record.rewrite_project_root(&rewrite_destination)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("checkout record task failed: {error}")))?
        .map_err(native_integrity_error)?;

        if let Err(error) = self
            .move_direct_mount(&current, &source, &destination)
            .await
        {
            // The tree never moved, so the only thing to undo is the record.
            let rollback = record.clone();
            let rollback_source = source.clone();
            let _ = crate::storage::lifecycle::dispatch_blocking(move || {
                rollback.rewrite_project_root(&rollback_source)
            })
            .await;
            return Err(error);
        }

        self.rebind_checkout(&destination)?;
        let current = self.current(&main).await?;
        // Past the rename there is no way back worth taking: the record and the tree both name the
        // destination, so a failure to re-attach here is a detached project at the right path,
        // which `cowshed attach` mounts. Rolling back would move the tree a second time to reach a
        // state that is strictly further from where the user asked to be.
        self.substrate
            .ensure_mounted(&current.derived.workspace, MountIntent { browse: false })
            .await
            .map_err(native_storage_error)?;
        self.repair_workspace_records(&destination).await?;
        self.ensure_supervisor(&main).await?;
        self.snapshot_named(&main).await
    }

    /// Change the project's repository identity in place.
    ///
    /// Identity is a cowshed record, not a Git fact: the remote is deliberately untouched, and the
    /// binding records the identity it is leaving as a former one. That record is what keeps every
    /// stamp this operation cannot reach — a detached image's marker, a CA certificate subject, an
    /// artifact frame already appended — valid afterwards, through [`OwnedRepoIds`].
    ///
    /// The durable half is [`apply_identity_change`], shared with recovery. Everything here is the
    /// live half: refuse what must not proceed, get the volumes down, cross the fence, and bring the
    /// project back up under its new name.
    async fn change_repo_id(&mut self, new_repo_id: RepoId) -> Result<WorkspaceSnapshot> {
        use crate::storage::apfs::ApfsExecutionHost;
        use crate::storage::lifecycle::{MountIntent, MountState, Substrate};
        use crate::storage::recovery::{LifecycleIntentPhase, RepositoryIdentityIntent};

        let old_repo_id = self.descriptor.repo_id.clone();
        if old_repo_id == new_repo_id {
            return Err(CowshedError::usage(
                format!("project already uses repository identity {new_repo_id}"),
                "choose a different identity or run cowshed ls",
            ));
        }
        let target_layout =
            crate::storage::StorageLayout::new(&self.substrate_config.store_root, &new_repo_id)
                .map_err(native_integrity_error)?;
        let old_project_root = self.layout.project().project_root.clone();
        let new_project_root = target_layout.project().project_root.clone();
        let old_mount_root = self.layout.project().mount_root.clone();
        let new_mount_root = target_layout.project().mount_root.clone();
        refuse_identity_owned_by_a_live_project(
            &self.substrate_config.store_root,
            &new_repo_id,
            &old_repo_id,
        )
        .await?;
        for (namespace, path) in [
            ("repository store", &new_project_root),
            ("workspace mount root", &new_mount_root),
        ] {
            if fs::symlink_metadata(path).is_ok() {
                return Err(CowshedError::conflict(
                    format!(
                        "{namespace} {} for repository identity {new_repo_id} is already in use",
                        path.display()
                    ),
                    format!(
                        "move {} aside, then retry the identity change",
                        path.display()
                    ),
                ));
            }
        }

        let workspaces = self.authoritative().await?;
        let main = workspaces
            .iter()
            .find(|workspace| workspace.derived.workspace.name().is_main())
            .ok_or_else(|| {
                CowshedError::not_found(
                    "workspace main does not exist",
                    "run cowshed ls and repair the adopted project before changing its identity",
                )
            })?;
        // Main has to be mounted so its in-image marker can be brought onto the new identity in the
        // same operation. Every other workspace may stay detached: its marker keeps the identity the
        // binding now records as former, and the next attach converges it.
        if !matches!(main.derived.mount_state, MountState::Mounted { .. }) {
            return Err(CowshedError::conflict(
                "workspace main is detached, so its in-image identity marker is out of reach",
                "run cowshed attach main, then retry the identity change",
            ));
        }
        let attached_sessions: Vec<_> = workspaces
            .iter()
            .filter(|workspace| !workspace.derived.workspace.name().is_main())
            .filter(|workspace| matches!(workspace.derived.mount_state, MountState::Mounted { .. }))
            .map(|workspace| workspace.derived.workspace.name().to_string())
            .collect();
        if !attached_sessions.is_empty() {
            return Err(CowshedError::conflict(
                format!(
                    "project {old_repo_id} has attached session workspace(s): {}",
                    attached_sessions.join(", ")
                ),
                format!(
                    "detach those workspaces first: {}",
                    attached_sessions
                        .iter()
                        .map(|workspace| format!("cowshed detach {workspace}"))
                        .collect::<Vec<_>>()
                        .join(" && ")
                ),
            ));
        }
        let new_binding = self
            .descriptor
            .binding
            .rename_primary(new_repo_id.clone())
            .map_err(|error| {
                CowshedError::integrity(
                    format!("cannot rewrite repository binding for {old_repo_id}: {error}"),
                    "repair the repository binding, then retry the identity change",
                )
            })?;
        let main_workspace = main.derived.workspace.clone();
        let intent = RepositoryIdentityIntent {
            old_repo_id: old_repo_id.clone(),
            new_repo_id: new_repo_id.clone(),
            old_project_root,
            new_project_root,
            old_mount_root,
            new_mount_root,
            phase: LifecycleIntentPhase::Prepared,
        };
        intent.persist(&self.substrate_config.store_root)?;

        // Everything above the fence is reversible by doing nothing: a `Prepared` record is
        // discarded on the next open, leaving a project that is merely detached.
        let _stopped = self.stop_supervisor(&main_name()).await?;
        self.substrate
            .unmount(&main_workspace)
            .await
            .map_err(|error| {
                let message = error.to_string();
                if message.to_ascii_lowercase().contains("resource busy") {
                    // The retry must come from outside the checkout, where cwd discovery cannot
                    // find the project — so `--project` is part of the command, not an option.
                    CowshedError::conflict(
                        format!("project {old_repo_id} main mount is busy: {message}"),
                        format!(
                            "leave the checkout and every workspace mount, then retry from \
                             outside it: cowshed --project {} mv main --repo-id {new_repo_id}",
                            self.substrate_config.checkout_path.display()
                        ),
                    )
                } else {
                    native_storage_error(error)
                }
            })?;

        // The fence. Nothing of this project is mounted, so every namespace rename below is free to
        // proceed and recovery from here is always forward.
        let mutating = RepositoryIdentityIntent {
            phase: LifecycleIntentPhase::Mutating,
            ..intent.clone()
        };
        mutating.persist(&self.substrate_config.store_root)?;
        let durable = mutating.clone();
        crate::storage::lifecycle::dispatch_blocking(move || apply_identity_change(&durable))
            .await
            .map_err(|error| {
                CowshedError::internal(format!("repository identity change task failed: {error}"))
            })??;

        self.rebind_repo_id(new_repo_id, new_binding, target_layout)?;
        let current = self.current(&main_name()).await?;
        self.substrate
            .ensure_mounted(&current.derived.workspace, MountIntent { browse: false })
            .await
            .map_err(native_storage_error)?;
        let main_mount = self.workspace_mount_path(&main_name())?;
        let project_root = self.descriptor.git_root.clone();
        // Re-read after mounting: `current` was resolved while main was still detached, and the
        // repair takes the mounted branch — the one that reaches the in-image marker — only for a
        // workspace it observes as mounted.
        let mounted_main = self.current(&main_name()).await?;
        self.repair_one_workspace_record(&mounted_main, &main_name(), &main_mount, &project_root)
            .await?;
        // The volume label is human-facing and carries no identity — nothing parses it and nothing
        // classifies a volume by it — so relabelling is cosmetic and needs no recovery. It is done
        // here, with main mounted, because Finder shows this string in place of the checkout
        // directory's name and leaving the old repository name there is simply wrong.
        self.substrate
            .host()
            .rename_volume(
                &main_mount,
                &crate::storage::apfs::volume_label(&self.descriptor.repo_id, &main_name()),
            )
            .map_err(native_storage_error)?;
        self.ensure_supervisor(&main_name()).await?;
        RepositoryIdentityIntent::clear(&self.substrate_config.store_root)?;
        self.snapshot_named(&main_name()).await
    }

    async fn attach(&mut self, workspace: WorkspaceName, options: AttachOptions) -> Result<()> {
        use crate::storage::lifecycle::{MountIntent, Substrate};
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        let was_detached = matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Detached
        );
        if is_git_worktree(&current.metadata) {
            self.require_main_mounted_for_git_worktree(&workspace)
                .await?;
        }
        self.substrate
            .ensure_mounted(
                &current.derived.workspace,
                MountIntent {
                    browse: options.browse,
                },
            )
            .await
            .map_err(native_storage_error)?;
        if was_detached {
            self.advance_gateway_revision(&current).await?;
        }
        self.ensure_supervisor(&workspace).await?;
        // `observed` is where the caller actually stands, which is the only evidence that a
        // hand-moved checkout produces. Repairing against `descriptor.git_root` instead would
        // rewrite the record to the path the controller already believes, so a moved checkout
        // would never be discovered and every later verb would keep aiming at the old path.
        if let Some(observed) = options.observed_path {
            self.converge_checkout_record(&observed).await?;
        }
        // A workspace that was detached while the project moved still records main's old mount as
        // its `projectRoot`, still fetches from the old path, and still runs merge drivers spelt
        // against it. Attachment is the reconciliation front door and is what `doctor` sends the
        // operator to, so the whole record is repaired here rather than the remote alone — a hint
        // that names a command which cannot fix the condition it names is worse than no hint.
        let attached = self.current(&workspace).await?;
        let main_mount = self.workspace_mount_path(&main_name())?;
        let project_root = self.descriptor.git_root.clone();
        self.repair_one_workspace_record(&attached, &workspace, &main_mount, &project_root)
            .await?;
        Ok(())
    }

    /// Detach `workspace`: its supervisor stops its jobs, the checkout's Nx daemon is stopped as
    /// a land stops it (the daemon the checkout's own record names, `nx daemon --stop`'s
    /// `SIGTERM`), and the volume leaves the kernel. A daemon started from any shell keeps its
    /// cwd on the checkout and serves nothing once the volume is gone; Nx starts a fresh one on
    /// the next run. Whatever else still holds the volume is named, pid and argv, in the refusal.
    async fn detach(&mut self, workspace: WorkspaceName) -> Result<()> {
        use crate::storage::lifecycle::Substrate;
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        let mount = self.workspace_mount_path(&workspace)?;
        let _stopped = self.stop_supervisor(&workspace).await?;
        if matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Mounted { .. }
        ) && let Err(busy) = close_checkout_nx(&workspace, &mount).await?
        {
            return Err(CowshedError::conflict(
                format!("{workspace} cannot detach while its Nx state is in use: {busy}"),
                format!("stop those processes, then `cowshed detach {workspace}`"),
            ));
        }
        self.substrate
            .unmount(&current.derived.workspace)
            .await
            .map_err(|error| {
                if let crate::storage::apfs::ApfsStorageError::Apfs(apfs) = &error
                    && crate::apfs::detach_was_dissented(apfs)
                {
                    detach_refused(&workspace, &mount, &error)
                } else {
                    native_storage_error(error)
                }
            })
    }

    /// Grow a workspace's image, or its build volume and seed, restoring the mount state the
    /// verb found it in.
    ///
    /// The supervisor is stopped first for the same reason `detach` stops it: the image has to
    /// leave the kernel for the resize, and a supervisor holding the mount would either keep it
    /// busy or come back pointed at a volume that went away underneath it. Its jobs and daemons
    /// hold the build volume the same way. A build volume is reached through the checkout's
    /// build link, so a detached workspace has to be attached first.
    async fn resize(
        &mut self,
        workspace: WorkspaceName,
        capacity: String,
        volume: crate::api::dto::ResizeVolume,
    ) -> Result<crate::api::dto::ResizeResult> {
        use crate::storage::lifecycle::Substrate;
        self.validate_binding().await?;
        let requested = parse_capacity(&capacity)?;
        let current = self.current(&workspace).await?;
        let was_mounted = matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Mounted { .. }
        );
        let outcome = match volume {
            crate::api::dto::ResizeVolume::Workspace => {
                let stopped = self.stop_supervisor(&workspace).await?;
                let outcome = self
                    .substrate
                    .resize(&current.derived.workspace, requested)
                    .await
                    .map_err(native_storage_error)?;
                drop(stopped);
                outcome
            }
            crate::api::dto::ResizeVolume::Build => {
                if !was_mounted {
                    return Err(CowshedError::conflict(
                        format!(
                            "workspace {workspace} is detached, so its build link cannot be read"
                        ),
                        format!("cowshed attach {workspace}, then retry the resize"),
                    ));
                }
                let checkout = self.workspace_mount_path(&workspace)?;
                let owner = super::build_volumes::Owner {
                    name: workspace.clone(),
                    incarnation: current.derived.workspace.incarnation().clone(),
                };
                let volumes = self.build_volumes()?;
                let stopped = self.stop_supervisor(&workspace).await?;
                let outcome = volumes.resize(owner, checkout, requested).await?;
                drop(stopped);
                outcome
            }
        };
        if was_mounted {
            self.ensure_supervisor(&workspace).await?;
        }
        Ok(crate::api::dto::ResizeResult {
            workspace,
            volume,
            previous_capacity: outcome.previous.to_string(),
            capacity: outcome.capacity.to_string(),
        })
    }

    /// Rewrite a workspace's image contiguously, restoring the mount state the verb found it in.
    ///
    /// The supervisor is stopped first for the reason `resize` stops it: the image has to leave
    /// the kernel to be copied, and a supervisor holding the mount would keep it busy.
    async fn defragment(
        &mut self,
        workspace: WorkspaceName,
    ) -> Result<crate::api::dto::DefragmentResult> {
        use crate::storage::lifecycle::Substrate;
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        let was_mounted = matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Mounted { .. }
        );
        let stopped = self.stop_supervisor(&workspace).await?;
        let outcome = self
            .substrate
            .defragment(&current.derived.workspace)
            .await
            .map_err(native_storage_error)?;
        drop(stopped);
        if was_mounted {
            self.ensure_supervisor(&workspace).await?;
        }
        Ok(crate::api::dto::DefragmentResult {
            workspace,
            previous_extents: outcome.previous.get(),
            extents: outcome.extents.get(),
            bytes: outcome.bytes,
        })
    }

    /// `cowshed reseed`: under `workspace`'s image lock, which every fork of it holds while it
    /// clones the seed, so no fork clones a seed this deletes.
    async fn reseed(&mut self, workspace: WorkspaceName) -> Result<crate::api::dto::ReseedResult> {
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        if !matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Mounted { .. }
        ) {
            return Err(CowshedError::conflict(
                format!("workspace {workspace} is detached, so its build link cannot be read"),
                format!("cowshed attach {workspace}, then retry the reseed"),
            ));
        }
        let owner = super::build_volumes::Owner {
            name: workspace.clone(),
            incarnation: current.derived.workspace.incarnation().clone(),
        };
        let checkout = self.workspace_mount_path(&workspace)?;
        let lock_path = self
            .layout
            .canonical_image(&workspace)
            .map_err(native_integrity_error)?
            .lock()
            .to_owned();
        let volumes = self.build_volumes()?;
        let outcome = self
            .substrate
            .dispatch_with_image_lock(lock_path, move || volumes.reseed_now(&owner, &checkout))
            .await
            .map_err(native_storage_error)??;
        Ok(crate::api::dto::ReseedResult { workspace, outcome })
    }

    async fn checkpoint(
        &mut self,
        workspace: WorkspaceName,
        expected_incarnation: Option<WorkspaceIncarnation>,
        options: CheckpointOptions,
    ) -> Result<CheckpointResult> {
        use crate::storage::lifecycle::LifecyclePlanner;

        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        if let Some(expected) = expected_incarnation.as_ref() {
            Self::require_exact_incarnation(&current, expected)?;
        }
        require_checkpointable(&workspace, &current.metadata, "checkpoint")?;
        let explicitly_labeled = options.label.is_some();
        let label = match options.label {
            Some(value) => {
                crate::storage::CheckpointLabel::new(value).map_err(native_integrity_error)?
            }
            None => crate::storage::CheckpointLabel::utc_default(
                std::time::SystemTime::now(),
                |candidate| {
                    current
                        .derived
                        .checkpoints
                        .iter()
                        .any(|fact| fact.label.as_str() == candidate)
                },
            ),
        };
        self.enforce_checkpoint_quota(&current).await?;
        let handle = self.ensure_supervisor(&workspace).await?;
        let barrier = handle.checkpoint_barrier(label.to_string()).await?;
        let plan = self
            .substrate
            .plan_checkpoint(
                &current.derived.workspace,
                label.clone(),
                if options.keep || explicitly_labeled {
                    crate::storage::lifecycle::Pin::Pinned
                } else {
                    crate::storage::lifecycle::Pin::Automatic
                },
            )
            .map_err(native_integrity_error)?;
        self.substrate
            .execute_checkpoint_staged(plan, move |stage| async move {
                if stage.checkpoint.label().as_str() != barrier.checkpoint_id {
                    return Err(CowshedError::integrity(
                        "supervisor checkpoint barrier identity changed",
                        "cowshed doctor --json",
                    ));
                }
                Ok(())
            })
            .await
            .map_err(native_staged_error)?;
        Ok(CheckpointResult {
            label: label.to_string(),
        })
    }

    async fn restore(&mut self, workspace: WorkspaceName, label: String) -> Result<()> {
        use super::supervisor::{CommitmentDraft, CommitmentSink};
        use crate::storage::lifecycle::LifecyclePlanner;

        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        require_checkpointable(&workspace, &current.metadata, "restore")?;
        let label = crate::storage::CheckpointLabel::new(label).map_err(native_integrity_error)?;
        let checkpoint = current
            .derived
            .checkpoints
            .iter()
            .find(|checkpoint| checkpoint.label == label)
            .cloned()
            .ok_or_else(|| {
                CowshedError::not_found(
                    format!("checkpoint {label} does not exist"),
                    "list workspace checkpoints and retry",
                )
            })?;
        let info = &current.metadata.info_snapshot;
        let identity = self
            .operation_identity(
                current.metadata.grants.clone(),
                info.branch.clone(),
                info.forked_from.clone(),
                info.git_worktree,
            )
            .await?;
        let stopped = self.stop_supervisor(&workspace).await?;
        require_lost_groups_released(&workspace, &stopped, "restore").await?;
        let checkpoint_ref = crate::storage::lifecycle::CheckpointRef::new(
            current.derived.workspace.clone(),
            checkpoint.label.clone(),
            checkpoint.revision,
            matches!(checkpoint.pin, crate::storage::lifecycle::Pin::Pinned),
        );
        let plan = self
            .substrate
            .plan_restore(
                &current.derived.workspace,
                &checkpoint_ref,
                crate::storage::lifecycle::RestoreMode::Replace,
                identity,
            )
            .map_err(native_integrity_error)?;
        let mut commitments = self.commitments.clone();
        let result = self
            .substrate
            .execute_restore_staged(plan, move |fence| async move {
                commitments
                    .record(CommitmentDraft::Restore {
                        repo_id: fence.pending.workspace.repo().clone(),
                        source_checkpoint: fence.pending.source_checkpoint,
                        source_incarnation: fence.pending.source_incarnation,
                        replaced_incarnation: fence.pending.replaced_incarnation,
                        destination_incarnation: fence.pending.workspace.incarnation().clone(),
                    })
                    .await
                    .map(|_| ())
            })
            .await;
        drop(stopped);
        match result {
            // The restored image carries the build link it had when the checkpoint was taken;
            // mounting it re-points a stale one at the workspace's own volume
            // (`BuildVolumeLayout::resolve_link`), so a restore never rewinds the build volume.
            Ok(_) => {
                self.ensure_supervisor(&workspace).await?;
                Ok(())
            }
            Err(error) => Err(native_restore_error(error)),
        }
    }

    async fn remove(
        &mut self,
        workspace: WorkspaceName,
        options: RemoveOptions,
    ) -> Result<RemoveReport> {
        let containment = self.main_containment()?;
        self.remove_contained_in(workspace, options, &containment)
            .await
    }

    async fn gc(&mut self, options: GcOptions) -> Result<GcReport> {
        use crate::storage::lifecycle::{StorageGcReason, Substrate};

        self.validate_binding().await?;
        self.retire_abandoned_pending_workspaces(options.dry_run)
            .await?;
        let plan = self
            .substrate
            .preview_gc(&self.descriptor.repo_id)
            .await
            .map_err(native_storage_error)?;
        let candidates = plan
            .candidates()
            .iter()
            .map(|candidate| crate::api::dto::GcCandidate {
                identity: crate::api::dto::Sha256Digest::from_bytes(candidate.identity()),
                path: candidate.path().to_owned(),
                bytes: candidate.bytes(),
                reason: match candidate.reason() {
                    StorageGcReason::RetiredWorkspace => {
                        crate::api::dto::GcReason::RetiredWorkspace
                    }
                    StorageGcReason::OrphanStagingImage => {
                        crate::api::dto::GcReason::OrphanStagingImage
                    }
                    StorageGcReason::OrphanStagingMetadata => {
                        crate::api::dto::GcReason::OrphanStagingMetadata
                    }
                    StorageGcReason::OrphanStagingMount => {
                        crate::api::dto::GcReason::OrphanStagingMount
                    }
                    StorageGcReason::OrphanMountpoint => {
                        crate::api::dto::GcReason::OrphanMountpoint
                    }
                    StorageGcReason::OrphanSessionImage => {
                        crate::api::dto::GcReason::OrphanSessionImage
                    }
                    StorageGcReason::ExpiredCheckpoint => {
                        crate::api::dto::GcReason::ExpiredCheckpoint
                    }
                },
            })
            .collect::<Vec<_>>();
        let (images, links) = self.build_volume_links().await?;
        let build = timed_async(
            "gc",
            "build-volumes",
            self.build_volumes()?
                .collect(images, links, options.dry_run),
        )
        .await?;
        if options.dry_run {
            let freed_bytes = candidates
                .iter()
                .try_fold(0_u64, |sum, candidate| sum.checked_add(candidate.bytes))
                .ok_or_else(|| CowshedError::internal("GC candidate byte accounting overflow"))?;
            return Ok(GcReport {
                examined: u64::try_from(plan.examined())
                    .map_err(|_| CowshedError::internal("GC count overflow"))?
                    + build.examined,
                reclaimed: 0,
                retained_pinned: u64::try_from(plan.retained_pinned())
                    .map_err(|_| CowshedError::internal("GC count overflow"))?,
                retained_active: u64::try_from(plan.retained_active())
                    .map_err(|_| CowshedError::internal("GC count overflow"))?,
                freed_bytes: freed_bytes.saturating_add(build.freed_bytes),
                dry_run: true,
                deferred: build.deferred.into_iter().map(Into::into).collect(),
                candidates: candidates.into_iter().chain(build.candidates).collect(),
            });
        }
        // Old build state a refresh moved aside and did not live to delete (`discard`): gc is
        // the verb that waits for it, saying what it deletes.
        for workspace in self.authoritative().await? {
            if !matches!(
                workspace.derived.mount_state,
                crate::storage::lifecycle::MountState::Mounted { .. }
            ) {
                continue;
            }
            let mount = self.workspace_mount_path(workspace.derived.workspace.name())?;
            let checkout = mount.clone();
            timed_async(
                "gc",
                "discard",
                crate::storage::lifecycle::dispatch_blocking(move || {
                    crate::build_volume::discard::finish(&checkout, |discard| {
                        eprintln!("cowshed: deleting old build state {}", discard.display());
                    })
                }),
            )
            .await
            .map_err(|error| CowshedError::internal(format!("discard task failed: {error}")))?
            .map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot delete old build state under {}: {error}",
                        mount
                            .join(crate::build_volume::discard::DISCARD_DIRECTORY)
                            .display()
                    ),
                    "repair the named path, then cowshed gc",
                )
            })?;
        }
        // Host-side state goes before the image does here too, for the same reason retirement
        // orders it that way: an image `gc` has already deleted leaves no authority to clean up
        // what it left behind in main. The authority is the retired image's own revalidated
        // sidecar — never the observation that a registered worktree's path is missing, which is
        // also what a merely detached workspace looks like.
        for candidate in plan.candidates() {
            if !matches!(candidate.reason(), StorageGcReason::RetiredWorkspace) {
                continue;
            }
            let Ok(metadata) =
                crate::metadata::DetachedWorkspaceMetadata::read_for_image(candidate.path())
            else {
                continue;
            };
            if metadata.info_snapshot.git_worktree {
                self.unregister_workspace_in_main(&metadata.workspace, true)
                    .await?;
            }
        }
        let report = timed_async("gc", "substrate", self.substrate.execute_gc(plan))
            .await
            .map_err(native_storage_error)?;
        Ok(GcReport {
            examined: u64::try_from(report.examined)
                .map_err(|_| CowshedError::internal("GC count overflow"))?
                + build.examined,
            reclaimed: u64::try_from(report.reclaimed)
                .map_err(|_| CowshedError::internal("GC count overflow"))?
                + build.reclaimed,
            retained_pinned: u64::try_from(report.retained_pinned)
                .map_err(|_| CowshedError::internal("GC count overflow"))?,
            retained_active: u64::try_from(report.retained_active)
                .map_err(|_| CowshedError::internal("GC count overflow"))?,
            freed_bytes: report.freed_bytes.saturating_add(build.freed_bytes),
            dry_run: false,
            candidates: candidates.into_iter().chain(build.candidates).collect(),
            deferred: report
                .deferred
                .into_iter()
                .map(|deferred| crate::api::dto::GcDeferred {
                    path: deferred.path,
                    diagnostic: deferred.diagnostic,
                })
                .chain(build.deferred.into_iter().map(Into::into))
                .collect(),
        })
    }

    async fn unpublished_workspaces(&mut self) -> Result<Vec<WorkspaceName>> {
        Ok(self
            .pending_metadata()
            .await?
            .into_iter()
            .map(|(_, metadata)| metadata.workspace)
            .filter(|workspace| !workspace.is_main())
            .collect())
    }

    async fn settle_reclaims(&mut self) -> Result<()> {
        for reclaim in std::mem::take(&mut self.reclaims) {
            reclaim.await.map_err(|error| {
                CowshedError::internal(format!("image reclamation task failed: {error}"))
            })?;
        }
        Ok(())
    }

    async fn delete_abandon_bundles(&mut self) -> Result<Vec<PathBuf>> {
        let trash = self
            .layout
            .project()
            .sessions
            .join(crate::storage::recovery::TRASH_NAMESPACE);
        crate::storage::lifecycle::dispatch_blocking(move || -> Result<Vec<PathBuf>> {
            let unreadable = |error: std::io::Error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot read the project's trash {}: {error}",
                        trash.display()
                    ),
                    "check controller storage permissions and retry",
                )
            };
            let entries = match std::fs::read_dir(&trash) {
                Ok(entries) => entries,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return Ok(Vec::new());
                }
                Err(error) => return Err(unreadable(error)),
            };
            let mut bundles = Vec::new();
            for entry in entries {
                let entry = entry.map_err(unreadable)?;
                let path = entry.path();
                if entry.file_type().map_err(unreadable)?.is_file()
                    && path
                        .extension()
                        .is_some_and(|extension| extension == "bundle")
                {
                    bundles.push(path);
                }
            }
            bundles.sort();
            for bundle in &bundles {
                std::fs::remove_file(bundle).map_err(|error| {
                    CowshedError::environment_missing(
                        format!(
                            "cannot delete the abandon bundle {}: {error}",
                            bundle.display()
                        ),
                        "check controller storage permissions and retry",
                    )
                })?;
            }
            Ok(bundles)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("abandon bundle task failed: {error}")))?
    }

    // Port capacity is a grant, but never a revocation or a silently shrinking allocation.
    async fn grant(
        &mut self,
        workspace: WorkspaceName,
        mut delta: GrantDelta,
        revoke: bool,
    ) -> Result<GrantSet> {
        self.validate_binding().await?;
        let mut current = self.current(&workspace).await?;
        let required_size = requested_port_block_size(&delta, revoke)?;
        delta.service_ports = None;
        let previous_block = current.metadata.grants.port_block.ok_or_else(|| {
            CowshedError::integrity("workspace has no port block", "cowshed doctor --json")
        })?;
        let growth_size = required_size.filter(|size| *size > previous_block.size());
        // Port authority changes require an idle workspace. The actor rejects a busy
        // request atomically, without queuing it or stopping any existing workload.
        let port_lease = if growth_size.is_some() {
            self.ensure_supervisor(&workspace)
                .await?
                .quiesce_if_idle()
                .await?;
            Some(self.stop_supervisor(&workspace).await?)
        } else {
            None
        };
        let reservation = if let Some(size) = growth_size {
            let inventory = crate::gateway_inventory::NativeGatewayInventory::new(
                self.descriptor.storage.clone(),
            );
            Some(
                reserve_grown_port_grants(
                    &inventory,
                    &self.descriptor.storage.store().join(".staging"),
                    &current.metadata.grants,
                    size,
                )
                .await?,
            )
        } else {
            None
        };
        let replacement_block = reservation
            .as_ref()
            .and_then(|reservation| reservation.grants.port_block);
        let previous_retained_blocks =
            std::mem::take(&mut current.metadata.grants.retained_port_blocks);
        let lock_path = self
            .layout
            .canonical_image(&workspace)
            .map_err(native_integrity_error)?
            .lock()
            .to_owned();
        let image = current.image.clone();
        let topology_revision = current.derived.workspace.revision().get();
        let mount = self.workspace_mount_path(&workspace)?;
        let main_mount = self.workspace_mount_path(&main_name())?;
        let home = self.home.clone();
        let layout = self.layout.clone();
        let telemetry_root = self.telemetry_root.clone();

        let (published, replacement_config) = self
            .substrate
            .dispatch_with_image_lock(lock_path, move || {
                (|| -> Result<_> {
                    current.metadata =
                        crate::metadata::DetachedWorkspaceMetadata::read_for_image(&image)
                            .map_err(native_integrity_error)?;
                    if delta
                        .expected_revision
                        .is_some_and(|revision| revision != current.metadata.grants.revision)
                    {
                        return Err(CowshedError::conflict(
                            "grant revision is stale",
                            "refresh grants and retry",
                        ));
                    }
                    normalize_grant_delta(&mut delta)?;
                    let previous = current.metadata.grants.clone();
                    if let Some(block) = replacement_block {
                        if previous.port_block != Some(previous_block)
                            || previous.retained_port_blocks != previous_retained_blocks
                        {
                            return Err(CowshedError::conflict(
                                "workspace port allocation changed during capacity reservation",
                                "refresh workspace grants and request capacity again",
                            ));
                        }
                        // Immutable profiles can outlive a completed job. A relocated block
                        // stays owned and connectable; containing growth subsumes it instead.
                        retain_port_authority(&mut current.metadata.grants, block);
                    }
                    apply_grant_delta(&mut current.metadata.grants, delta, revoke);

                    // Validated as the workspace will run: its own grants plus the project's.
                    let effective = effective_workspace_grants(&layout, &current.metadata.grants)?;
                    let config = supervisor_sandbox(
                        &home,
                        &layout,
                        &telemetry_root,
                        &current,
                        &effective,
                        mount,
                        main_mount,
                        None,
                    )?;
                    validate_grant_sandbox(&config)?;
                    if current.metadata.grants == previous {
                        return Ok((previous, None));
                    }

                    current.metadata.grants.revision = current
                        .metadata
                        .grants
                        .revision
                        .checked_add(1)
                        .ok_or_else(|| CowshedError::internal("grant revision overflow"))?;
                    let effective_revision = effective
                        .revision
                        .checked_add(1)
                        .ok_or_else(|| CowshedError::internal("grant revision overflow"))?;
                    let published = current.metadata.grants.clone();
                    current
                        .metadata
                        .write_for_image(&image)
                        .map_err(native_integrity_error)?;
                    if replacement_block.is_some() {
                        crate::workspace_credentials::publish_workspace_environment(
                            crate::workspace_environment::EnvironmentMount::Served {
                                workspace_mount: &config.workspace_mount,
                                temp_dir: &config.exec_temp_dir,
                            },
                            current.metadata.platform,
                            published.port_block,
                        )
                        .map_err(|error| {
                            CowshedError::internal(format!(
                                "publish grown workspace port environment: {error}"
                            ))
                        })?;
                    }
                    Ok((published, Some((config, effective_revision))))
                })()
            })
            .await
            .map_err(native_storage_error)??;

        if port_lease.is_some() {
            // Publish ownership before releasing the kernel reservations; release the
            // supervisor socket fence only after the new authority is durable.
            drop(reservation);
            drop(port_lease);
            self.ensure_supervisor(&workspace).await?;
        } else if let Some((config, effective_revision)) = replacement_config
            && self.supervisors.remove(&workspace).is_some()
        {
            // Only the process running the actor can advance it. A supervisor another process
            // serves keeps its revision; the next command here finds it stale and says so.
            if let Some(served) = self.served.get_mut(&workspace) {
                let advanced = served
                    .actor
                    .advance_authority(effective_revision, topology_revision, config)
                    .await?;
                let handle = super::supervisor_socket::connect(
                    served.socket.clone(),
                    advanced.snapshot().clone(),
                );
                served.actor = advanced;
                self.supervisors.insert(workspace, handle);
            }
        }
        Ok(published)
    }

    async fn project_grants(&mut self) -> Result<crate::project_policy::ProjectGrants> {
        self.validate_binding().await?;
        let path = self.layout.project().policy.clone();
        crate::storage::lifecycle::dispatch_blocking(move || read_project_policy(&path))
            .await
            .map_err(|error| CowshedError::internal(error.to_string()))?
            .map(|policy| policy.grants)
    }

    async fn grant_project(
        &mut self,
        mut delta: ProjectGrantDelta,
        revoke: bool,
    ) -> Result<crate::project_policy::ProjectGrants> {
        self.validate_binding().await?;
        normalize_grant_paths(&mut delta.read)?;
        normalize_relative_denies(&mut delta.deny_write)?;
        normalize_relative_denies(&mut delta.deny)?;
        // Every workspace runs under the project's grants, so the candidate is validated as the
        // one workspace every project has runs: main, with its own grants plus the candidate. The
        // denies that differ between workspaces are their own mounts, which the mount-root deny
        // covers for all of them alike, and main's mount, which every other workspace denies by
        // name: that one is added to main's shape below.
        let main = self.current(&main_name()).await?;
        let main_mount = self.workspace_mount_path(&main_name())?;
        let path = self.layout.project().policy.clone();
        let home = self.home.clone();
        let layout = self.layout.clone();
        let telemetry_root = self.telemetry_root.clone();
        crate::storage::lifecycle::dispatch_blocking(move || -> Result<_> {
            let mut policy = read_project_policy(&path)?;
            if delta
                .expected_revision
                .is_some_and(|revision| revision != policy.grants.revision)
            {
                return Err(CowshedError::conflict(
                    "project grant revision is stale",
                    "refresh project grants and retry",
                ));
            }
            let previous = policy.grants.clone();
            update_ordered_set(&mut policy.grants.read, delta.read, revoke);
            update_ordered_set(&mut policy.grants.deny_write, delta.deny_write, revoke);
            update_ordered_set(&mut policy.grants.deny, delta.deny, revoke);
            update_egress(&mut policy.grants.egress, delta.egress, revoke);
            if policy.grants == previous {
                return Ok(previous);
            }
            // A host the gateway cannot turn into a grant would take every session of the
            // project down at its next reconcile; refuse it here instead.
            crate::gateway_sessions::policy_from_grants(&GrantSet {
                egress: policy.grants.egress.clone(),
                ..GrantSet::default()
            })
            .map_err(|error| {
                CowshedError::usage(error.message, "name a DNS host or IP address to egress to")
            })?;
            let effective =
                crate::project_policy::effective_grants(&main.metadata.grants, &policy.grants)
                    .map_err(|error| CowshedError::internal(error.to_string()))?;
            let config = supervisor_sandbox(
                &home,
                &layout,
                &telemetry_root,
                &main,
                &effective,
                main_mount.clone(),
                main_mount,
                None,
            )?;
            validate_grant_sandbox(&config)?;
            let mut sibling = config;
            sibling
                .additional_denies
                .push(sibling.workspace_mount.clone());
            validate_grant_sandbox(&sibling)?;
            policy.grants.revision = policy
                .grants
                .revision
                .checked_add(1)
                .ok_or_else(|| CowshedError::internal("project grant revision overflow"))?;
            policy.write(&path).map_err(native_integrity_error)?;
            Ok(policy.grants)
        })
        .await
        .map_err(|error| CowshedError::internal(error.to_string()))?
    }

    async fn assign_slot(&mut self, workspace: WorkspaceName, slot: u32) -> Result<()> {
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        let base = u16::try_from(
            slot.checked_mul(u32::from(crate::metadata::NEW_PORT_BLOCK_SIZE))
                .ok_or_else(|| {
                    CowshedError::usage("slot overflows port space", "choose a smaller slot")
                })?,
        )
        .map_err(|_| CowshedError::usage("slot overflows port space", "choose a smaller slot"))?;
        let size = current
            .metadata
            .grants
            .port_block
            .ok_or_else(|| {
                CowshedError::integrity("workspace has no port block", "cowshed doctor --json")
            })?
            .size();
        let block = crate::metadata::PortBlock::new(base, size)
            .map_err(|error| CowshedError::usage(error.to_string(), "choose another slot"))?;
        if !cowshed_gateway_types::is_macos_port_block(block.base(), block.size()) {
            return Err(CowshedError::usage(
                format!("port block {block} is outside the macOS workspace range"),
                "choose a macOS workspace slot",
            ));
        }
        if current.metadata.grants.port_block == Some(block) {
            return Ok(());
        }
        let inventory =
            crate::gateway_inventory::NativeGatewayInventory::new(self.descriptor.storage.clone());
        let used = inventory
            .all_reserved_port_blocks()
            .await
            .map_err(native_integrity_error)?;
        if let Some(held) = used.blocks().find(|held| {
            held.overlaps(block)
                && !current
                    .metadata
                    .grants
                    .port_blocks()
                    .any(|owned| owned == *held)
        }) {
            return Err(CowshedError::conflict(
                format!("port block {block} overlaps workspace port block {held}"),
                "choose an unassigned workspace slot",
            ));
        }
        let reservation_root = self.descriptor.storage.store().join(".staging");
        let _reservation = reserve_port_grant_replacement(
            &inventory,
            &reservation_root,
            block,
            Some(&current.metadata.grants),
        )
        .await?
        .ok_or_else(|| {
            CowshedError::conflict(
                format!("port block {block} is already held or bound"),
                "choose an unassigned workspace slot",
            )
        })?;
        let mut metadata = current.metadata;
        retain_port_authority(&mut metadata.grants, block);
        metadata.grants.revision = metadata
            .grants
            .revision
            .checked_add(1)
            .ok_or_else(|| CowshedError::internal("grant revision overflow"))?;
        let image = current.image;
        crate::storage::lifecycle::dispatch_blocking(move || metadata.write_for_image(&image))
            .await
            .map_err(|error| CowshedError::internal(error.to_string()))?
            .map_err(native_integrity_error)?;
        if self.supervisors.contains_key(&workspace) {
            drop(self.stop_supervisor(&workspace).await?);
            self.ensure_supervisor(&workspace).await?;
        }
        Ok(())
    }

    async fn set_checkpoint_quota(
        &mut self,
        workspace: WorkspaceName,
        quota: CheckpointQuota,
    ) -> Result<()> {
        self.validate_binding().await?;
        self.current(&workspace).await?;
        let path = self.layout.project().policy.clone();
        crate::storage::lifecycle::dispatch_blocking(move || -> Result<()> {
            let mut policy = read_project_policy(&path)?;
            policy.checkpoint_quotas.insert(workspace, quota);
            policy.write(&path).map_err(native_integrity_error)
        })
        .await
        .map_err(|error| CowshedError::internal(error.to_string()))?
    }

    async fn rebase(
        &mut self,
        workspace: WorkspaceName,
        into: Option<WorkspaceTarget>,
        options: RebaseOptions,
    ) -> Result<RebaseReport> {
        require_single_destination(options.onto.as_ref(), into.as_ref())?;
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        if let Some(expected) = options.expected_workspace_incarnation.as_ref() {
            Self::require_exact_incarnation(&current, expected)?;
        }
        let into = self.landing_into(&workspace, into).await?;
        let root = current_snapshot_mount(self, &current)?;
        let source_head = git_oid(&root).await?;
        require_source_head(
            options.expected_source_head.as_ref(),
            &source_head,
            "rebase",
        )?;
        let onto = if !into.name.is_main() {
            // A lane base is its own repository: its branch reaches the unit by a fetch from its
            // mount into a cowshed-owned ref, which is also what keeps it current. `onto` was
            // refused above, so this is the only destination.
            fetch_target_branch(&root, &into.root, &into.name).await?
        } else if is_git_worktree(&current.metadata) {
            // A git-worktree workspace reads main's branches straight out of the shared ref
            // namespace, so there is nothing to refresh and no remote to refresh it from.
            options
                .onto
                .as_ref()
                .map_or_else(|| DEFAULT_LANDING_BRANCH.to_owned(), revision_target)
        } else {
            // The default destination follows the remote's name, which is `main` — and
            // `cowshed-main` in a workspace where something else already held that name. The
            // refresh runs for an explicit `onto` too, since that is how main's commits reach the
            // workspace: rebasing onto `main/main` resolves a ref only a fetch creates, and a stale
            // one silently replays onto yesterday's base.
            let main_mount = self.workspace_mount_path(&main_name())?;
            let main_remote = crate::git::GitRepository::from_root(&root)
                .configure_main_remote(&main_mount)
                .await?;
            let remote = main_remote.remote_name();
            run_git_with_read(&root, &main_mount, ["fetch", "--no-tags", remote]).await?;
            options.onto.as_ref().map_or_else(
                || format!("{remote}/{DEFAULT_LANDING_BRANCH}"),
                revision_target,
            )
        };
        let onto_head = git_revision_oid(&root, &onto).await?;
        require_onto_head(options.expected_onto_head.as_ref(), &onto, &onto_head)?;
        run_git_rebase_atomically(&root, &onto, &source_head).await?;
        let oid = git_oid(&root).await?;
        // The rebase has happened: a failed carry reads as such, never as a refused rebase.
        let build_volume = timed_async(
            "rebase",
            "carry",
            self.build_volumes()?
                .rebase_carry(root.clone(), into.mount.clone()),
        )
        .await
        .map_err(|failed| {
            CowshedError::new(
                failed.code,
                format!(
                    "rebased {workspace} to {oid}, but carrying {}'s Nx cache into its build volume failed: {}",
                    into.name, failed.message
                ),
                failed.hint,
            )
        })?;
        Ok(RebaseReport { oid, build_volume })
    }

    async fn land(
        &mut self,
        workspace: WorkspaceName,
        into: Option<WorkspaceTarget>,
        options: LandOptions,
    ) -> Result<LandReport> {
        timed_async("land", "binding", self.validate_binding()).await?;
        let current = timed_async("land", "source", self.current(&workspace)).await?;
        if let Some(expected) = options.expected_workspace_incarnation.as_ref() {
            Self::require_exact_incarnation(&current, expected)?;
        }
        // Resolved before any check runs: a lane base that was recreated under its name refuses
        // here, not after minutes of checks.
        let into = timed_async("land", "target", self.landing_into(&workspace, into)).await?;
        let source_mount = current_snapshot_mount(self, &current)?;
        let source_repository = crate::git::GitRepository::from_root(&source_mount);
        let source_head = timed_async("land", "source-head", git_oid(&source_mount)).await?;
        require_source_head(options.expected_source_head.as_ref(), &source_head, "land")?;
        // The check runs in the working tree but only the head commit lands, so uncommitted work
        // would pass validation without landing, and would then refuse the retire after main had
        // already moved. The same reading of "work" as `rm` makes, so land refuses exactly the
        // trees retirement would.
        let work = timed_async(
            "land",
            "source-dirty",
            source_repository.dirty_paths_by(Some(&self.substrate_config.checkout_path)),
        )
        .await?;
        if !work.is_empty() {
            return Err(CowshedError::fence_refusal(
                crate::error::FenceRefusal::dirty(false, &work),
                format!(
                    "workspace {workspace} has uncommitted work, which the check would validate but land would leave behind"
                ),
                format!(
                    "commit the work in the workspace or discard it, then retry: cowshed land {workspace}"
                ),
            ));
        }
        let target_branch = match options.target_branch.clone() {
            Some(branch) => branch,
            None => timed_async("land", "target-branch", into.checked_out_branch()).await?,
        };
        timed_async(
            "land",
            "target-check",
            require_target_checked_out(&into.root, &target_branch),
        )
        .await?;
        let target_ref = format!("refs/heads/{target_branch}");
        let previous = timed_async(
            "land",
            "target-head",
            git_optional_ref_oid(&into.root, &target_ref),
        )
        .await?;
        require_target_head(options.expected_target_head.as_ref(), previous.as_ref())?;
        let retire = options.retire;
        let (handle, build_volume) =
            timed_async("land", "supervisor", self.admit_build_state(&workspace)).await?;
        let checks = options.check.unwrap_or_default();
        for check in &checks {
            let job_id = timed_async(
                "land",
                "check-exec",
                handle.exec(None, build_volume.clone(), land_check_request(check)),
            )
            .await?;
            let info = timed_async("land", "check-wait", handle.wait(job_id)).await?;
            let exit_code = match info.exit {
                Some(crate::api::dto::ExitStatus::Exited { code }) => Some(code),
                _ => None,
            };
            if exit_code != Some(0) {
                // The check's own words are the diagnosis. Read them back through the
                // supervisor's bounded log so a failing check can never report as a bare
                // category, then decide whose fault this is: the workspace's, or an
                // environment that refused to run it at all.
                let stderr = read_job_stderr_tail(&handle, job_id).await;
                if let Some(denial) = sandbox_denial_in(&stderr) {
                    return Err(CowshedError::environment_missing(
                        format!(
                            "land check `{check}` exited {exit} inside the sandbox: {denial}",
                            exit = exit_code
                                .map(|code| code.to_string())
                                .unwrap_or_else(|| "killed".into()),
                            denial = denial,
                        ),
                        format!(
                            "the sandbox refused this command (workspace grants do not cover it), not the code: run `cowshed exec {ws} -- {check}` unsandboxed-equivalent or `cowshed grant {ws} --read <path>`",
                            ws = workspace,
                        ),
                    ));
                }
                return Err(CowshedError::conflict(
                    format!(
                        "land check `{check}` failed with exit {exit}: {stderr}",
                        exit = exit_code
                            .map(|code| code.to_string())
                            .unwrap_or_else(|| "killed".into()),
                    ),
                    "fix the workspace and retry land",
                ));
            }
        }
        // Land has no branch-name contract with the workspace: it delivers whatever branch the
        // workspace has checked out, whether an agent named it `cowshed/<ws>`, `wt/<ws>`, or
        // anything else. Only the resolved head is load-bearing.
        let source_branch =
            timed_async("land", "source-branch", source_repository.current_branch())
                .await?
                .ok_or_else(|| {
                    CowshedError::conflict(
                        format!("workspace {workspace} has no checked-out branch to land"),
                        "check out a branch in the workspace and retry land",
                    )
                })?;
        timed_async(
            "land",
            "delivery",
            deliver_into(
                &into.name,
                &into.root,
                &source_mount,
                &workspace,
                &source_branch,
                &source_head,
                &target_branch,
            ),
        )
        .await?;
        // The target has moved: a failed build-volume step reads as such, never as a refused land.
        let build_volume = timed_async(
            "land",
            "build-volume",
            self.land_build_volume(&workspace, &source_mount, &into, &checks, retire),
        )
        .await
        .map_err(|failed| {
            CowshedError::new(
                failed.code,
                format!(
                    "landed {source_head} on {target_branch}, but its build volume was not adopted: {}",
                    failed.message
                ),
                failed.hint,
            )
        })?;
        // Each 2b miss is a durable finding (13_telemetry.md, `landAdoption`), from the same
        // value the report carries.
        if let crate::api::dto::Adoption::Adopted { check, .. } = &build_volume.adoption
            && !check.misses.is_empty()
        {
            use super::supervisor::{CommitmentDraft, CommitmentSink};
            let landing_incarnation = self
                .current(&workspace)
                .await?
                .derived
                .workspace
                .incarnation()
                .clone();
            let target_incarnation = self
                .current(&into.name)
                .await?
                .derived
                .workspace
                .incarnation()
                .clone();
            for miss in &check.misses {
                self.commitments
                    .record(CommitmentDraft::LandAdoption {
                        repo_id: self.descriptor.repo_id.clone(),
                        landing_incarnation: landing_incarnation.clone(),
                        target_incarnation: target_incarnation.clone(),
                        landed_head: source_head.clone(),
                        task: miss.task.clone(),
                        task_hash: miss.hash.clone(),
                        inputs_digest: miss.inputs_digest,
                    })
                    .await?;
            }
        }
        if retire {
            // The target has already moved, so a refused retire must not read as a refused land:
            // the retry its hint names would land nothing. Containment is measured against the
            // branch the unit landed on, so a lane unit whose commits are in its lane base retires.
            let containment = into.containment(target_branch.clone());
            self.remove_contained_in(workspace.clone(), RemoveOptions::default(), &containment)
                .await
                .map_err(|kept| {
                    CowshedError::new(
                        kept.code,
                        format!(
                            "landed {source_head} on {target_branch}, but workspace {workspace} was kept: {}",
                            kept.message
                        ),
                        kept.hint,
                    )
                })?;
        } else {
            // Retiring collects; a land that keeps the workspace collects here, so the target's
            // previous volume and whatever else no checkout links goes with every land.
            self.collect_build_volumes().await.map_err(|failed| {
                CowshedError::new(
                    failed.code,
                    format!(
                        "landed {source_head} on {target_branch}, but its build volumes were not collected: {}",
                        failed.message
                    ),
                    failed.hint,
                )
            })?;
        }
        Ok(LandReport {
            landed_head: source_head,
            target_branch,
            previous_target_head: previous,
            target_was_checked_out: true,
            retired: retire,
            build_volume,
        })
    }

    async fn push(
        &mut self,
        workspace: WorkspaceName,
        expected_incarnation: WorkspaceIncarnation,
        options: PushOptions,
    ) -> Result<PushReport> {
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &expected_incarnation)?;
        let source_mount = current_snapshot_mount(self, &current)?;
        let source_branch = match options.branch {
            Some(branch) => branch,
            None => crate::api::dto::BranchName::new(format!("cowshed/{workspace}"))
                .map_err(|error| CowshedError::internal(error.to_string()))?,
        };
        preserve_workspace_branch(
            &self.descriptor.git_root,
            &source_mount,
            &workspace,
            &source_branch,
            options.expected_source_head.as_ref(),
            options.expected_destination_head.as_ref(),
        )
        .await
    }

    async fn repo_mirror(&mut self, workspace: WorkspaceName, url: Url) -> Result<MirrorInfo> {
        self.validate_binding().await?;
        self.current(&workspace).await?;
        let root = crate::host_dirs::repo_mirrors(&self.home).join(
            crate::repository::encode_component(url.as_str()).map_err(native_integrity_error)?,
        );
        if tokio::fs::try_exists(&root).await.map_err(|error| {
            CowshedError::environment_missing(error.to_string(), "check cache permissions")
        })? {
            run_git(&root, ["remote", "update", "--prune"]).await?;
        } else {
            let parent = root
                .parent()
                .ok_or_else(|| CowshedError::internal("mirror root has no parent"))?
                .to_path_buf();
            tokio::fs::create_dir_all(&parent).await.map_err(|error| {
                CowshedError::environment_missing(error.to_string(), "check cache permissions")
            })?;
            let output = tokio::process::Command::new("git")
                .arg("clone")
                .arg("--mirror")
                .arg(url.as_str())
                .arg(&root)
                .output_locked()
                .await
                .map_err(|error| {
                    CowshedError::environment_missing(error.to_string(), "install git")
                })?;
            require_git_success("clone mirror", &output)?;
        }
        Ok(MirrorInfo {
            url: url.to_string(),
            mirror: root,
        })
    }

    async fn serve_supervisor(&mut self, workspace: WorkspaceName) -> Result<()> {
        self.serve_supervisor_until_retired(workspace).await
    }

    async fn refresh_build_state(
        &mut self,
        workspace: WorkspaceName,
    ) -> Result<crate::build_volume::BuildStateRefresh> {
        self.validate_binding().await?;
        let current = self.current(&workspace).await?;
        // Mounting is the supervisor's (it advances the gateway revision of a workspace it
        // attaches), so a detached workspace is refused rather than mounted behind its back.
        if !matches!(
            current.derived.mount_state,
            crate::storage::lifecycle::MountState::Mounted { .. }
        ) {
            return Err(CowshedError::conflict(
                format!("workspace {workspace} is detached; its build link cannot be read"),
                format!("cowshed attach {workspace}, then retry"),
            ));
        }
        let mount = self.workspace_mount_path(&workspace)?;
        self.refresh_build_state_for(&current, &mount, super::build_volumes::Lent::Nothing)
            .await
    }

    async fn build_volume(&mut self, workspace: WorkspaceName) -> Result<Option<PathBuf>> {
        let mount = self.workspace_mount_path(&workspace)?;
        self.build_volume_layout()?.grant(&workspace, &mount)
    }

    async fn doctor(&mut self) -> Result<DoctorReport> {
        use crate::storage::lifecycle::{StorageGcReason, Substrate};

        let mut findings = Vec::new();
        match self.binding_move().await {
            Ok(None) => {}
            Ok(Some(moved)) if self.recovery_scope.repairs() => {
                if let Err(error) = self.record_binding(moved).await {
                    findings.push(native_finding(
                        "binding",
                        crate::api::dto::FindingSeverity::Error,
                        error,
                    ));
                }
            }
            Ok(Some(moved)) => findings.push(binding_move_finding(
                &self.descriptor.binding,
                &moved,
                self.layout.project().repository_binding.clone(),
            )),
            Err(error) => findings.push(native_finding(
                "binding",
                crate::api::dto::FindingSeverity::Error,
                error,
            )),
        }
        // What any other opening would have finished first, read where it is recorded: an
        // inspecting open (doctor without --repair) finishes none of it.
        let identity_intent = crate::storage::recovery::RepositoryIdentityIntent::path(
            self.descriptor.storage.store(),
        );
        if identity_intent.symlink_metadata().is_ok() {
            findings.push(crate::api::dto::Finding {
                code: "identity-change-unfinished".into(),
                severity: crate::api::dto::FindingSeverity::Warning,
                message: format!(
                    "a repository identity change is journaled in {} and not finished",
                    identity_intent.display()
                ),
                hint: "any cowshed command other than doctor finishes it as it opens".into(),
                path: Some(identity_intent),
            });
        }
        match self.reload_lifecycle_intents().await {
            Ok(()) => findings.extend(
                self.lifecycle_intents
                    .records()
                    .filter(|(_, record)| record.completion.is_none())
                    .map(|(workspace, record)| {
                        unfinished_intent_finding(
                            workspace,
                            record.operation.verb(),
                            self.lifecycle_intents_path.clone(),
                        )
                    }),
            ),
            Err(error) => findings.push(native_finding(
                "lifecycle-intents",
                crate::api::dto::FindingSeverity::Error,
                error,
            )),
        }
        let project_root = self.layout.project().project_root.clone();
        match crate::storage::lifecycle::dispatch_blocking(move || {
            crate::storage::apfs::native::interrupted_publications(&project_root)
        })
        .await
        {
            Ok(Ok(sidecars)) => findings.extend(sidecars.into_iter().map(|sidecar| {
                crate::api::dto::Finding {
                    code: "interrupted-publication".into(),
                    severity: crate::api::dto::FindingSeverity::Warning,
                    message: format!(
                        "{} names an image that does not exist: a publication or a retirement \
                         a crash interrupted",
                        sidecar.display()
                    ),
                    hint: "any cowshed command other than doctor publishes the image from \
                           staging or removes the orphaned metadata as it opens"
                        .into(),
                    path: Some(sidecar),
                }
            })),
            Ok(Err(error)) => findings.push(native_finding(
                "interrupted-publication",
                crate::api::dto::FindingSeverity::Error,
                native_storage_error(error),
            )),
            Err(error) => findings.push(native_finding(
                "interrupted-publication",
                crate::api::dto::FindingSeverity::Error,
                CowshedError::internal(format!("publication scan task failed: {error}")),
            )),
        }
        // Gateway reachability is host-scoped, not project-scoped, and is diagnosed once by the
        // CLI's host diagnosis with a launchd-aware message and hint. A second `gateway-down`
        // author here produced two Error rows with the same code and contradictory recovery
        // steps; a project cannot answer a host question, so it no longer tries.
        match self.commitments.health().await {
            Ok(health) if health.failed > 0 => findings.push(crate::api::dto::Finding {
                code: "audit-sink".into(),
                severity: crate::api::dto::FindingSeverity::Warning,
                message: format!(
                    "the {} audit sink refused {} of {} records; last: {}",
                    health.sink,
                    health.failed,
                    health.failed.saturating_add(health.recorded),
                    health.last_failure.as_deref().unwrap_or("(no message)")
                ),
                hint: "verify telemetry storage, or set COWSHED_CONTINUITY_AUDIT=off — the audit trail gates nothing"
                    .into(),
                path: Some(self.telemetry_root.clone()),
            }),
            Ok(_) => {}
            Err(error) => findings.push(native_finding(
                "audit-sink",
                crate::api::dto::FindingSeverity::Error,
                error,
            )),
        }
        let abandoned = match self.abandoned_pending_workspaces().await {
            Ok(abandoned) => abandoned
                .into_iter()
                .map(|(image, _)| image)
                .collect::<std::collections::BTreeSet<_>>(),
            Err(error) => {
                findings.push(native_finding(
                    "pending-integrity",
                    crate::api::dto::FindingSeverity::Error,
                    error,
                ));
                std::collections::BTreeSet::new()
            }
        };
        match self.pending_metadata().await {
            Ok(pending) => {
                for (image, metadata) in pending {
                    let (message, hint) = if abandoned.contains(&image) {
                        (
                            format!(
                                "workspace {} is an unfinished {} its process abandoned; it was \
                                 never published",
                                metadata.workspace,
                                if metadata.workspace.is_main() {
                                    "adoption"
                                } else {
                                    "clone"
                                }
                            ),
                            "cowshed gc retires it (so does cowshed doctor --repair)".to_owned(),
                        )
                    } else {
                        (
                            format!(
                                "workspace {} is pending publication by a lifecycle operation",
                                metadata.workspace
                            ),
                            "the operation finishes it, or the next cowshed command does if its \
                             process died"
                                .to_owned(),
                        )
                    };
                    findings.push(crate::api::dto::Finding {
                        code: "pending-publication".into(),
                        severity: crate::api::dto::FindingSeverity::Warning,
                        message,
                        hint,
                        path: Some(image),
                    });
                }
            }
            Err(error) => findings.push(native_finding(
                "pending-integrity",
                crate::api::dto::FindingSeverity::Error,
                error,
            )),
        }
        // Companion inputs for the quarantine scan below, collected here so `doctor` lists
        // the store once. An authoritative failure leaves this empty — and is already a
        // finding above — while the tombstone half still runs.
        let mut companion_images: Vec<(WorkspaceName, PathBuf)> = Vec::new();

        match self.authoritative().await {
            Ok(workspaces) => {
                let main_mount = self.workspace_mount_path(&main_name()).ok();
                companion_images.extend(workspaces.iter().map(|workspace| {
                    (
                        workspace.derived.workspace.name().clone(),
                        workspace.image.clone(),
                    )
                }));
                for workspace in workspaces {
                    let workspace_name = workspace.derived.workspace.name().clone();
                    findings.extend(
                        unresolved_group_findings(
                            &workspace_name,
                            self.job_group_ledger(&workspace_name),
                        )
                        .await,
                    );
                    let expected_mount = match self.workspace_mount_path(&workspace_name) {
                        Ok(path) => path,
                        Err(error) => {
                            findings.push(native_finding(
                                "mount",
                                crate::api::dto::FindingSeverity::Error,
                                error,
                            ));
                            continue;
                        }
                    };
                    match &workspace.derived.mount_state {
                        crate::storage::lifecycle::MountState::Detached => {
                            findings.push(crate::api::dto::Finding {
                                code: "mount".into(),
                                severity: crate::api::dto::FindingSeverity::Info,
                                message: format!(
                                    "workspace {workspace_name} is detached; expected mount {}",
                                    expected_mount.display()
                                ),
                                hint: format!("cowshed attach {workspace_name}"),
                                path: Some(expected_mount),
                            });
                            if self.supervisors.contains_key(&workspace_name) {
                                findings.push(crate::api::dto::Finding {
                                    code: "mount-supervisor".into(),
                                    severity: crate::api::dto::FindingSeverity::Error,
                                    message: format!(
                                        "detached workspace {workspace_name} still has a supervisor"
                                    ),
                                    hint: format!(
                                        "cowshed detach {workspace_name} && cowshed attach {workspace_name}"
                                    ),
                                    path: Some(workspace.image),
                                });
                            }
                        }
                        crate::storage::lifecycle::MountState::Mounted { .. } => {
                            let marker_path =
                                expected_mount.join(crate::storage::WORKSPACE_MARKER_PATH);
                            let expected_repos = self.owned_repo_ids()?;
                            let expected_workspace = workspace_name.clone();
                            let expected_incarnation =
                                workspace.derived.workspace.incarnation().clone();
                            let expected_project_root = self.descriptor.git_root.clone();
                            let checked_marker_path = marker_path.clone();
                            let marker = crate::storage::lifecycle::dispatch_blocking(move || {
                                crate::metadata::WorkspaceMarker::read_from(&checked_marker_path)
                                    .map_err(|error| error.to_string())
                            })
                            .await;
                            match marker {
                                Ok(Ok(marker)) => findings.extend(diagnose_mounted_marker(
                                    &workspace_name,
                                    &marker,
                                    &expected_repos,
                                    &expected_workspace,
                                    &expected_incarnation,
                                    &expected_project_root,
                                    marker_path.clone(),
                                )),
                                Ok(Err(error)) => findings.push(crate::api::dto::Finding {
                                    code: "marker".into(),
                                    severity: crate::api::dto::FindingSeverity::Error,
                                    message: format!(
                                        "workspace {workspace_name} marker is unreadable: {error}"
                                    ),
                                    hint: "inspect the workspace image; an unreadable marker is not rewritten by detach or attach".into(),
                                    path: Some(marker_path),
                                }),
                                Err(error) => findings.push(crate::api::dto::Finding {
                                    code: "marker".into(),
                                    severity: crate::api::dto::FindingSeverity::Error,
                                    message: format!(
                                        "could not read workspace {workspace_name} marker: {error}"
                                    ),
                                    hint: "inspect the workspace image; cowshed could not read the marker".into(),
                                    path: Some(marker_path),
                                }),
                            }
                            match crate::git::GitRepository::from_root(&expected_mount)
                                .inspect_merge_drivers()
                                .await
                            {
                                Ok(drivers) => {
                                    for driver in drivers {
                                        if let Some(finding) = merge_driver_finding(
                                            &workspace_name,
                                            &driver,
                                            expected_mount.clone(),
                                        ) {
                                            findings.push(finding);
                                        }
                                    }
                                }
                                Err(error) => findings.push(native_finding(
                                    "merge-driver",
                                    crate::api::dto::FindingSeverity::Error,
                                    error,
                                )),
                            }
                            if !workspace_name.is_main()
                                && let Some(main_mount) = main_mount.as_ref()
                            {
                                match crate::git::GitRepository::from_root(&expected_mount)
                                    .inspect_cowshed_upstream(main_mount)
                                    .await
                                {
                                    Ok(upstream) => {
                                        if let Some(finding) = cowshed_upstream_finding(
                                            &workspace_name,
                                            &upstream,
                                            expected_mount.clone(),
                                        ) {
                                            findings.push(finding);
                                        }
                                    }
                                    Err(error) => findings.push(native_finding(
                                        "main-remote",
                                        crate::api::dto::FindingSeverity::Error,
                                        error,
                                    )),
                                }
                            }
                            let owner = super::build_volumes::Owner {
                                name: workspace_name.clone(),
                                incarnation: workspace.derived.workspace.incarnation().clone(),
                            };
                            let age = match self.build_volumes() {
                                Ok(volumes) => {
                                    volumes.seed_age(owner, expected_mount.clone()).await
                                }
                                Err(error) => Err(error),
                            };
                            match age {
                                Ok(Some(age)) => findings.extend(seed_age_finding(
                                    &workspace_name,
                                    &age,
                                    expected_mount.clone(),
                                )),
                                Ok(None) => {}
                                Err(error) => findings.push(native_finding(
                                    "seed-age",
                                    crate::api::dto::FindingSeverity::Error,
                                    error,
                                )),
                            }
                            let checkout = expected_mount.clone();
                            let links = crate::storage::lifecycle::dispatch_blocking(move || {
                                unsettled_build_links(&checkout)
                            })
                            .await
                            .map_err(|error| {
                                CowshedError::internal(format!(
                                    "build link inspection failed: {error}"
                                ))
                            })
                            .and_then(|links| links);
                            match links {
                                Ok(links) => {
                                    findings.extend(links.into_iter().map(|(path, unsettled)| {
                                        build_link_finding(
                                            &workspace_name,
                                            &expected_mount,
                                            path,
                                            unsettled,
                                        )
                                    }))
                                }
                                Err(error) => findings.push(native_finding(
                                    "build-link",
                                    crate::api::dto::FindingSeverity::Error,
                                    error,
                                )),
                            }
                        }
                    }
                }
            }
            Err(error) => findings.push(native_finding(
                "marker",
                crate::api::dto::FindingSeverity::Error,
                error,
            )),
        }
        // A blocked collection sends the operator here, so this reads gc's own preview rather than a
        // second scan that could disagree with the command that forwarded to it. A preview that
        // cannot be taken at all is itself the finding — reporting a healthy host while `gc` refuses
        // leaves the operator with no next move.
        match self.substrate.preview_gc(&self.descriptor.repo_id).await {
            Ok(plan) => {
                let orphaned = plan
                    .candidates()
                    .iter()
                    .filter(|candidate| {
                        matches!(
                            candidate.reason(),
                            StorageGcReason::OrphanStagingImage
                                | StorageGcReason::OrphanStagingMetadata
                                | StorageGcReason::OrphanStagingMount
                                | StorageGcReason::OrphanMountpoint
                        )
                    })
                    .collect::<Vec<_>>();
                if let Some(first) = orphaned.first() {
                    let bytes = orphaned
                        .iter()
                        .try_fold(0_u64, |sum, candidate| sum.checked_add(candidate.bytes()))
                        .ok_or_else(|| {
                            CowshedError::internal("GC candidate byte accounting overflow")
                        })?;
                    let mounts = orphaned
                        .iter()
                        .filter(|candidate| {
                            matches!(
                                candidate.reason(),
                                StorageGcReason::OrphanStagingMount
                                    | StorageGcReason::OrphanMountpoint
                            )
                        })
                        .count();
                    findings.push(crate::api::dto::Finding {
                        code: "staging-orphans".into(),
                        severity: crate::api::dto::FindingSeverity::Warning,
                        message: format!(
                            "staging holds {} orphaned {} totalling {bytes} bytes, {mounts} of them {} no operation owns",
                            orphaned.len(),
                            if orphaned.len() == 1 { "entry" } else { "entries" },
                            if mounts == 1 { "a mountpoint" } else { "mountpoints" }
                        ),
                        hint: "cowshed gc (or cowshed doctor --repair)".into(),
                        path: Some(first.path().to_owned()),
                    });
                }
                findings.extend(orphan_session_image_findings(plan.candidates()));
                if plan.retained_active() > 0 {
                    findings.push(crate::api::dto::Finding {
                        code: "staging-active".into(),
                        severity: crate::api::dto::FindingSeverity::Info,
                        message: format!(
                            "{} staging {} an operation still holds the lifecycle lock for",
                            plan.retained_active(),
                            if plan.retained_active() == 1 {
                                "entry"
                            } else {
                                "entries"
                            }
                        ),
                        hint: "nothing to do; gc retains them until the lock is released".into(),
                        path: None,
                    });
                }
                let stranded = plan
                    .candidates()
                    .iter()
                    .filter(|candidate| {
                        matches!(candidate.reason(), StorageGcReason::RetiredWorkspace)
                    })
                    .collect::<Vec<_>>();
                if let Some(first) = stranded.first() {
                    let bytes = stranded
                        .iter()
                        .try_fold(0_u64, |sum, candidate| sum.checked_add(candidate.bytes()))
                        .ok_or_else(|| {
                            CowshedError::internal("GC candidate byte accounting overflow")
                        })?;
                    findings.push(crate::api::dto::Finding {
                        code: "retired-trash".into(),
                        severity: crate::api::dto::FindingSeverity::Warning,
                        message: format!(
                            "sessions/.trash holds {} retired workspace {} totalling {bytes} bytes",
                            stranded.len(),
                            if stranded.len() == 1 {
                                "entry"
                            } else {
                                "entries"
                            }
                        ),
                        hint: "cowshed gc".into(),
                        path: Some(first.path().to_owned()),
                    });
                }
            }
            Err(error) => findings.push(native_finding(
                "gc-preview",
                crate::api::dto::FindingSeverity::Error,
                native_storage_error(error),
            )),
        }
        // Every clone of main copies main's extent map on its first write, so the count predicts
        // what the next `new` will pay before it is paid.
        if let Some(image) = companion_images
            .iter()
            .find(|(name, _)| name.is_main())
            .map(|(_, image)| image.clone())
        {
            let counted = image.clone();
            let extents = crate::storage::lifecycle::dispatch_blocking(move || {
                crate::storage::apfs::extents::count_extents(&counted)
            })
            .await;
            findings.push(match extents {
                Ok(Ok(extents)) => main_extents_finding(image, extents),
                Ok(Err(error)) => main_extents_unread(image, error.to_string()),
                Err(error) => main_extents_unread(image, error.to_string()),
            });
        }
        // An interrupted rewrite leaves a full copy of the image beside it, which nothing lists as
        // an image and `gc` does not collect: say so, with its size, until a rewrite replaces it.
        let siblings = companion_images
            .iter()
            .map(|(name, image)| {
                (
                    name.clone(),
                    crate::storage::apfs::extents::rewrite_sibling(image),
                )
            })
            .collect::<Vec<_>>();
        findings.extend(
            crate::storage::lifecycle::dispatch_blocking(move || {
                siblings
                    .into_iter()
                    .filter_map(|(name, sibling)| {
                        use std::os::unix::fs::MetadataExt;
                        let metadata = std::fs::symlink_metadata(&sibling).ok()?;
                        Some(crate::api::dto::Finding {
                            code: "defrag-leftover".into(),
                            severity: crate::api::dto::FindingSeverity::Warning,
                            message: format!(
                                "{} is the copy an interrupted or still-running cowshed defrag {name} was writing; it holds {} bytes",
                                sibling.display(),
                                metadata.blocks().saturating_mul(crate::apfs::SECTOR_BYTES)
                            ),
                            hint: format!(
                                "cowshed defrag {name} replaces it; with no defrag running, deleting it is safe"
                            ),
                            path: Some(sibling),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .await
            .unwrap_or_default(),
        );
        // Recovery quarantines a companion-less workspace and continues, so the failure never
        // reaches `doctor` as an error: the tombstones and the live images are read here, from
        // the same store-side facts, instead.
        let store_root = self.descriptor.storage.store().to_path_buf();
        let repo_id = self.descriptor.repo_id.clone();
        findings.extend(
            crate::storage::lifecycle::dispatch_blocking(move || {
                quarantine_and_companion_findings_blocking(&store_root, &repo_id, &companion_images)
            })
            .await
            .unwrap_or_default(),
        );
        Ok(DoctorReport::from_findings(findings))
    }

    async fn open_worker(&mut self, workspace: WorkspaceName) -> Result<WorkspaceSnapshot> {
        self.ensure_supervisor(&workspace).await?;
        self.snapshot_named(&workspace).await
    }

    async fn open_session(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        name: Option<String>,
    ) -> Result<()> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        let handle = self.ensure_supervisor(&workspace).await?;
        let token = handle.open_session(name.clone()).await?;
        let reopened = token.identity();
        if let Some(previous) = self.sessions.insert((workspace.clone(), name), token)
            && let Some(previous) = superseded_session(previous, reopened)
        {
            let superseded = previous.identity();
            match handle.close_session(previous).await {
                // Every Conflict a close answers says the supervisor holds no session for this
                // token under its authority (it restarted, or the session was closed): the
                // session is already closed. Said, never dropped silently.
                Err(error) if error.code == ErrorCode::Conflict => eprintln!(
                    "cowshed: session {superseded} of workspace {workspace}, superseded by session {reopened}, was already closed by its supervisor: {error}"
                ),
                closed => closed?,
            }
        }
        Ok(())
    }

    async fn close_session(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        name: Option<String>,
    ) -> Result<()> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        let token = self
            .sessions
            .remove(&(workspace.clone(), name))
            .ok_or_else(|| {
                CowshedError::not_found(
                    "session does not exist",
                    "open the session before closing it",
                )
            })?;
        self.ensure_supervisor(&workspace)
            .await?
            .close_session(token)
            .await
    }

    async fn exec(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        session: Option<String>,
        request: ExecRequest,
    ) -> Result<JobId> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        let token = self.session(&workspace, &session).cloned();
        let (handle, build_volume) = self.admit_build_state(&workspace).await?;
        handle.exec(token.as_ref(), build_volume, request).await
    }

    async fn stdin_write(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
        bytes: Bytes,
    ) -> Result<()> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        self.ensure_supervisor(&workspace)
            .await?
            .stdin_write(job, bytes)
            .await
    }

    async fn stdin_close(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<()> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        self.ensure_supervisor(&workspace)
            .await?
            .stdin_close(job)
            .await
    }

    async fn list_jobs(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
    ) -> Result<Vec<JobInfo>> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        self.ensure_supervisor(&workspace).await?.list().await
    }

    async fn job_info(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<JobInfo> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        self.ensure_supervisor(&workspace).await?.info(job).await
    }

    async fn sealed_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<SealedJob> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        self.ensure_supervisor(&workspace).await?.sealed(job).await
    }

    async fn wait_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<JobAnswer<JobInfo>> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        let supervisor = self.ensure_supervisor(&workspace).await?;
        Ok(Box::pin(async move { supervisor.wait(job).await }))
    }

    async fn kill_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<JobAnswer<()>> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        let supervisor = self.ensure_supervisor(&workspace).await?;
        Ok(Box::pin(async move { supervisor.kill(job).await }))
    }

    async fn detach_job(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
    ) -> Result<()> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        self.ensure_supervisor(&workspace).await?.info(job).await?;
        Ok(())
    }

    async fn read_log(
        &mut self,
        workspace: WorkspaceName,
        incarnation: WorkspaceIncarnation,
        job: JobId,
        stream: JobStream,
        offset: u64,
        follow: bool,
    ) -> Result<JobAnswer<RuntimeLogChunk>> {
        let current = self.current(&workspace).await?;
        Self::require_exact_incarnation(&current, &incarnation)?;
        let stream = crate::storage::job_artifact::StreamKind::from(stream);
        let supervisor = self.ensure_supervisor(&workspace).await?;
        Ok(Box::pin(async move {
            let chunk = supervisor.log_read(job, stream, offset, follow).await?;
            Ok(RuntimeLogChunk {
                bytes: chunk.bytes,
                next_offset: chunk.next_offset,
                eof: chunk.eof,
            })
        }))
    }
}

#[cfg(target_os = "macos")]
async fn binding_from_git(
    git: &crate::git::GitRepository,
    requested_repo_id: Option<&RepoId>,
) -> Result<RepositoryBinding> {
    let remotes = git.remotes().await?;
    binding_from_remotes(&remotes, requested_repo_id)
}

#[cfg(any(target_os = "macos", test))]
fn binding_from_remotes(
    remotes: &[crate::git::RemoteUrl],
    requested_repo_id: Option<&RepoId>,
) -> Result<RepositoryBinding> {
    if remotes.is_empty() {
        let repo_id = requested_repo_id.cloned().ok_or_else(|| {
            CowshedError::environment_missing(
                "repository has no remote from which to derive its identity",
                "retry adoption with --repo-id owner/repo",
            )
        })?;
        return RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id,
            remote_name: None,
            remote_url: None,
            primary: true,
        }])
        .map_err(binding_integrity_error);
    }

    // A remote that yields no owner/repo identity is not an error: local-path
    // mirrors and backup remotes are ordinary Git and carry no identity to
    // derive. They are skipped as identity candidates, and only reported if
    // nothing else identifies the repository — one unusable remote must not
    // brick read-only commands in an otherwise well-formed checkout.
    let mut candidates = Vec::with_capacity(remotes.len());
    let mut unusable = Vec::new();
    for remote in remotes {
        match remote
            .url
            .to_str()
            .map(crate::repository::normalize_remote_url)
        {
            Some(Ok(repo_id)) => candidates.push((remote, repo_id)),
            Some(Err(error)) => unusable.push(format!("{} ({error})", remote.name)),
            None => unusable.push(format!("{} (remote URL is not UTF-8)", remote.name)),
        }
    }

    if candidates.is_empty() {
        let repo_id = requested_repo_id.cloned().ok_or_else(|| {
            CowshedError::environment_missing(
                format!(
                    "no Git remote yields a repository identity; skipped: {}",
                    unusable.join(", ")
                ),
                "retry adoption with --repo-id owner/repo",
            )
        })?;
        return RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id,
            remote_name: None,
            remote_url: None,
            primary: true,
        }])
        .map_err(binding_integrity_error);
    }

    let available = candidates
        .iter()
        .map(|(_, repo_id)| repo_id.clone())
        .collect::<std::collections::BTreeSet<_>>();
    let selected_repo_id = if let Some(requested_repo_id) = requested_repo_id {
        if !available.contains(requested_repo_id) {
            return Err(CowshedError::conflict(
                format!(
                    "explicit repository identity {requested_repo_id} does not match any Git remote"
                ),
                format!(
                    "retry with --repo-id matching one of: {}",
                    available
                        .iter()
                        .map(RepoId::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
        requested_repo_id.clone()
    } else {
        if available.len() != 1 {
            return Err(CowshedError::conflict(
                "Git remotes resolve to multiple repository identities",
                format!(
                    "retry with --repo-id selecting one of: {}",
                    available
                        .iter()
                        .map(RepoId::as_str)
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
            ));
        }
        available
            .first()
            .cloned()
            .ok_or_else(|| CowshedError::internal("repository candidate set is empty"))?
    };

    let selected = candidates
        .iter()
        .filter(|(_, repo_id)| repo_id == &selected_repo_id)
        .min_by(|(left, _), (right, _)| {
            (left.name != "origin", &left.name, &left.url).cmp(&(
                right.name != "origin",
                &right.name,
                &right.url,
            ))
        })
        .map(|(remote, _)| *remote)
        .ok_or_else(|| CowshedError::internal("selected repository candidate is missing"))?;

    let remote_url = persistable_remote_url(&selected.url)
        .ok_or_else(|| CowshedError::internal("normalized repository URL cannot be persisted"))?;
    RepositoryBinding::new(vec![crate::repository::BoundIdentity {
        repo_id: selected_repo_id,
        remote_name: Some(selected.name.clone()),
        remote_url: Some(remote_url),
        primary: true,
    }])
    .map_err(binding_integrity_error)
}

/// The remote URL as a binding records it: without query or fragment, and without userinfo except
/// an SSH login name. The recorded URL keys a fetch route (`workspace_git_fetch`), and Git rewrites
/// a URL only when it starts with the recorded one, so the login that addresses the server's
/// account stays. An HTTPS username can itself be a token, and a password always is one.
#[cfg(any(target_os = "macos", test))]
fn persistable_remote_url(value: &Path) -> Option<String> {
    let value = value.to_str()?;
    let suffix = value
        .char_indices()
        .find_map(|(index, character)| matches!(character, '?' | '#').then_some(index));
    let without_suffix = suffix.map_or(value, |index| &value[..index]);
    let Some((scheme, remainder)) = without_suffix.split_once("://") else {
        // SCP-like `login@host:path` is SSH, and its userinfo cannot carry a password: `:` is
        // where the path begins.
        without_suffix.split_once(':')?;
        return Some(without_suffix.to_owned());
    };
    let (authority, path) = remainder.split_once('/')?;
    let authority = match authority.rsplit_once('@') {
        Some((login, _)) if scheme == "ssh" && !login.is_empty() && !login.contains(':') => {
            authority
        }
        Some((_, host)) => host,
        None => authority,
    };
    Some(format!("{scheme}://{authority}/{path}"))
}

#[cfg(any(target_os = "macos", test))]
fn binding_integrity_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(error.to_string(), "repair the repository binding")
}

/// Read a directory's workspace marker, if it has one.
///
/// Only an absent marker is `None`. A marker that exists but cannot be parsed is an integrity
/// error, because the two answers are not interchangeable: swallowing a damaged
/// `.cowshed/workspace.json` as "no marker" made a workspace cwd fail the belongs-to-project
/// check with "path does not belong", naming the wrong problem and pointing the operator at the
/// wrong repair.
///
/// Portable because the router needs it on every target: a marker is plain metadata, and the
/// question "which project does this directory belong to" has nothing platform-specific in it.
async fn read_workspace_marker(path: &Path) -> Result<Option<crate::metadata::WorkspaceMarker>> {
    let marker_path = path.join(crate::storage::WORKSPACE_MARKER_PATH);
    crate::storage::lifecycle::dispatch_blocking(move || {
        match crate::metadata::WorkspaceMarker::read_from(&marker_path) {
            Ok(marker) => Ok(Some(marker)),
            Err(crate::metadata::MetadataError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    })
    .await
    .map_err(|error| CowshedError::internal(format!("workspace marker task failed: {error}")))?
    .map_err(native_integrity_error)
}

#[cfg(target_os = "macos")]
fn prepare_detached_checkout_relocation(
    record: &crate::checkout::CheckoutRecord,
    source: &Path,
    destination: &Path,
) -> Result<()> {
    let source_existed = match std::fs::symlink_metadata(source) {
        Ok(metadata) if metadata.file_type().is_dir() => {
            std::fs::rename(source, destination).map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot move the detached checkout mountpoint to {}: {error}",
                        destination.display()
                    ),
                    "choose a destination on the same writable filesystem",
                )
            })?;
            true
        }
        Ok(_) => {
            return Err(CowshedError::conflict(
                format!(
                    "the detached checkout path {} is not a directory",
                    source.display()
                ),
                "remove the occupant or run cowshed doctor --json",
            ));
        }
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            std::fs::create_dir(destination).map_err(|error| {
                CowshedError::environment_missing(
                    format!(
                        "cannot create checkout mountpoint {}: {error}",
                        destination.display()
                    ),
                    "choose a destination in a writable directory",
                )
            })?;
            false
        }
        Err(error) => {
            return Err(CowshedError::environment_missing(
                format!(
                    "cannot inspect detached checkout path {}: {error}",
                    source.display()
                ),
                "check the old checkout parent permissions and retry",
            ));
        }
    };
    if let Err(error) = record.rewrite_detached_project_root(destination) {
        if source_existed {
            let _ = std::fs::rename(destination, source);
        } else {
            let _ = std::fs::remove_dir(destination);
        }
        return Err(native_integrity_error(error));
    }
    Ok(())
}

/// What a workspace marker tells an opening controller about the project it belongs to.
#[cfg(target_os = "macos")]
struct WorkspaceOrigin {
    repo_id: RepoId,
    workspace: WorkspaceName,
    workspace_incarnation: WorkspaceIncarnation,
    /// The project's checkout path, which every marker records — main's own for main, and main's
    /// for a session, since a session is a clone of the project rather than a project of its own.
    project_root: PathBuf,
}

#[cfg(target_os = "macos")]
async fn workspace_origin_from_marker(project_root: &Path) -> Result<Option<WorkspaceOrigin>> {
    let Some(marker) = read_workspace_marker(project_root).await? else {
        return Ok(None);
    };
    // The marker's job here is to name the repository, and every workspace's marker names it. A
    // coordinator verb is routinely invoked from inside a session workspace — `cowshed rebase`
    // infers its workspace from the cwd precisely so it can be — and that directory is a session
    // mount carrying a session marker.
    //
    // The recorded project root cannot be compared against the invocation root for a session:
    // every marker records the project's checkout path, which for a session is main's checkout and
    // therefore a different directory than the mount the caller is standing in. Comparing them
    // rejected every workspace cwd, and no amount of repairing main's marker could satisfy it,
    // because main's marker was never the file being read.
    //
    // What remains checkable is real incoherence: a marker whose role and workspace name disagree
    // is corrupt either way, and main's marker must still name the root it sits in, since for main
    // — and only for main — the recorded project root and the invocation root are the same
    // directory.
    let claims_main = marker.workspace.is_main();
    if claims_main != (marker.role == crate::metadata::WorkspaceRole::Main) {
        return Err(CowshedError::conflict(
            format!(
                "workspace marker at {} names {} with role {:?}",
                project_root.display(),
                marker.workspace,
                marker.role
            ),
            "repair the workspace marker, or reopen from the canonical main checkout",
        ));
    }
    if claims_main && !names_one_root(&marker.project_root, project_root) {
        return Err(CowshedError::conflict(
            format!(
                "main workspace marker records {} but was read at {}",
                marker.project_root.display(),
                project_root.display()
            ),
            "cowshed attach",
        ));
    }
    Ok(Some(WorkspaceOrigin {
        repo_id: marker.repo_id,
        workspace: marker.workspace,
        workspace_incarnation: marker.workspace_incarnation,
        project_root: marker.project_root,
    }))
}

/// Does the marker the caller was reached through name a workspace the store still has?
///
/// The repository axis is a membership test, because the marker is the one stamp an identity change
/// cannot reach inside an image it is not mounting. A project that has been renamed is reached
/// through a marker naming an identity it recorded as former, and refusing that would refuse to open
/// the very project the change produced. The workspace name and its random incarnation still have to
/// match a live storage fact exactly, so a marker from some other workspace is still refused.
#[cfg(target_os = "macos")]
fn validate_workspace_origin_against_inventory(
    origin: &WorkspaceOrigin,
    owned_repo_ids: &OwnedRepoIds,
    facts: &[&crate::storage::lifecycle::StorageFact],
) -> Result<()> {
    if !owned_repo_ids.accepts(&origin.repo_id) {
        return Err(CowshedError::conflict(
            format!(
                "workspace marker identity {} is not owned by project {owned_repo_ids}",
                origin.repo_id
            ),
            "reopen from a workspace whose marker matches active storage",
        ));
    }
    if facts.iter().any(|fact| {
        owned_repo_ids.accepts(fact.workspace.repo())
            && fact.workspace.name() == &origin.workspace
            && fact.workspace.incarnation() == &origin.workspace_incarnation
    }) {
        return Ok(());
    }
    Err(CowshedError::conflict(
        format!(
            "workspace marker identity {}/{}/{} differs from active storage inventory",
            origin.repo_id, origin.workspace, origin.workspace_incarnation
        ),
        "reopen from a workspace whose marker matches active storage",
    ))
}

/// Do a recorded project root and an observed one name the same directory?
///
/// Recorded metadata holds the adopted checkout path, while the controller and Git report the
/// physical root — since main mounts under `mnt/<owner>/<repo>/main` and the checkout is a symlink
/// into it, those two strings legitimately differ for the same workspace. They still describe one
/// directory, so a disagreement is only real when the paths do not resolve to the same place.
fn names_one_root(recorded: &Path, observed: &Path) -> bool {
    if recorded == observed {
        return true;
    }
    let (Ok(recorded), Ok(observed)) = (
        std::fs::canonicalize(recorded),
        std::fs::canonicalize(observed),
    ) else {
        return false;
    };
    recorded == observed
}

/// Reconciles a loaded binding's recorded remote URLs with the checkout's Git configuration.
///
/// Identity is the owner/repo ([`crate::repository::normalize_remote_url`] strips the host on
/// purpose); the URL is only transport. So a recorded remote whose current URL still derives the
/// same identity has merely moved servers, and the recorded transport follows it — the healed
/// binding is returned for the caller to persist. A current URL deriving a *different* identity is
/// a real divergence: the refusal names both identities and the rebind verb, which is the correct
/// next move because [`ProjectRuntimeHost::change_repo_id`] records identity while deliberately
/// never touching the remote.
///
/// `Ok(None)` means the binding already matches (or the caller opened for the identity change,
/// where the recorded pairing is about to be superseded and must not block its own supersession).
#[cfg(any(target_os = "macos", test))]
fn reconcile_binding_with_remotes(
    binding: &RepositoryBinding,
    remotes: &[crate::git::RemoteUrl],
    validation: BindingRemoteValidation,
    checkout: &Path,
) -> Result<Option<RepositoryBinding>> {
    binding.validate().map_err(binding_integrity_error)?;
    if validation == BindingRemoteValidation::ForIdentityChange {
        return Ok(None);
    }
    let mut identities = binding.identities.clone();
    let mut healed = false;
    for identity in &mut identities {
        let (Some(name), Some(url)) = (identity.remote_name.clone(), identity.remote_url.clone())
        else {
            continue;
        };
        let Some(remote) = remotes.iter().find(|remote| remote.name == name) else {
            return Err(CowshedError::conflict(
                format!("repository binding remote {name} does not match Git configuration"),
                "restore the recorded remote before opening cowshed",
            ));
        };
        let current = persistable_remote_url(&remote.url);
        if current.as_deref() == Some(url.as_str()) {
            continue;
        }
        match remote
            .url
            .to_str()
            .map(crate::repository::normalize_remote_url)
        {
            Some(Ok(derived)) if derived == identity.repo_id => {
                // Same identity, new transport: follow the move. The persistable form is what
                // comparisons above use, so it is what gets recorded; a URL it cannot express
                // was already refused by the parse succeeding only on supported transports.
                identity.remote_url = current.or_else(|| remote.url.to_str().map(str::to_owned));
                healed = true;
            }
            Some(Ok(derived)) => {
                return Err(CowshedError::conflict(
                    format!(
                        "repository binding names {} for remote {name}, but Git configuration now names {derived}",
                        identity.repo_id
                    ),
                    // The identity change detaches main's volume, so the retry necessarily runs
                    // from outside the checkout — where cwd discovery cannot find the project.
                    // `--project` is therefore part of the command, not an option.
                    format!(
                        "adopt the new identity from outside the checkout: cowshed --project {} mv main --repo-id {derived} (or restore the recorded remote)",
                        checkout.display()
                    ),
                ));
            }
            Some(Err(_)) | None => {
                return Err(CowshedError::conflict(
                    format!("repository binding remote {name} does not match Git configuration"),
                    "restore the recorded remote before opening cowshed",
                ));
            }
        }
    }
    if !healed {
        return Ok(None);
    }
    let updated = RepositoryBinding {
        version: binding.version,
        identities,
        former_identities: binding.former_identities.clone(),
    };
    updated.validate().map_err(binding_integrity_error)?;
    Ok(Some(updated))
}

/// Binds `remote_name`, a remote of the project's main checkout, as a non-primary identity.
///
/// Returns the identity the remote names and the binding to persist, or `None` when the remote is
/// already bound as recorded — the open-time reconcile has already paired every bound remote with
/// the checkout, so an existing entry is this remote's. A remote that names no owner/repo (a local
/// path, a backup clone), an identity this binding already holds or once held, and a URL any other
/// adopted project binds are refused: a fetch URL routes to exactly one clone
/// (`workspace_git_fetch`), so no two bindings may share a route.
#[cfg(any(target_os = "macos", test))]
fn bind_remote(
    binding: &RepositoryBinding,
    remotes: &[crate::git::RemoteUrl],
    remote_name: &str,
    others: &[(RepoId, RepositoryBinding)],
) -> Result<(crate::repository::BoundIdentity, Option<RepositoryBinding>)> {
    let remote = remotes
        .iter()
        .find(|remote| remote.name == remote_name)
        .ok_or_else(|| {
            CowshedError::usage(
                format!("the main checkout has no remote named {remote_name}"),
                format!(
                    "bind one of its remotes: cowshed identity add <{}>",
                    remotes
                        .iter()
                        .map(|remote| remote.name.as_str())
                        .collect::<Vec<_>>()
                        .join("|")
                ),
            )
        })?;
    let url = remote.url.to_str().ok_or_else(|| {
        CowshedError::usage(
            format!("the URL of remote {remote_name} is not UTF-8"),
            "bind a remote whose URL names a hosted repository",
        )
    })?;
    let repo_id = crate::repository::normalize_remote_url(url).map_err(|error| {
        CowshedError::usage(
            format!("remote {remote_name} ({url}) names no owner/repo identity: {error}"),
            "bind a remote whose URL names a hosted repository",
        )
    })?;
    let recorded = persistable_remote_url(&remote.url)
        .ok_or_else(|| CowshedError::internal("normalized repository URL cannot be persisted"))?;
    if let Some(bound) = binding
        .identities
        .iter()
        .find(|bound| bound.remote_name.as_deref() == Some(remote_name))
    {
        return Ok((bound.clone(), None));
    }
    let routes: std::collections::BTreeSet<_> =
        crate::workspace_git_fetch::fetch_route_keys(&recorded).collect();
    for (other, other_binding) in others {
        if let Some(taken) = other_binding
            .identities
            .iter()
            .filter_map(|identity| identity.remote_url.as_deref())
            .find(|url| {
                crate::workspace_git_fetch::fetch_route_keys(url).any(|key| routes.contains(&key))
            })
        {
            return Err(CowshedError::conflict(
                format!(
                    "{taken} is already bound by {other}, and a fetch URL routes to exactly one clone"
                ),
                format!("bind a remote {other} does not already bind (git remote -v lists them)"),
            ));
        }
    }
    let identity = crate::repository::BoundIdentity {
        repo_id,
        remote_name: Some(remote_name.to_owned()),
        remote_url: Some(recorded),
        primary: false,
    };
    let mut identities = binding.identities.clone();
    identities.push(identity.clone());
    let updated = RepositoryBinding {
        version: binding.version,
        identities,
        former_identities: binding.former_identities.clone(),
    };
    updated.validate().map_err(|error| {
        CowshedError::conflict(
            format!("remote {remote_name} cannot be bound: {error}"),
            "bind a remote naming a repository this project does not already hold",
        )
    })?;
    Ok((identity, Some(updated)))
}

/// `cowshed identity add`: binds a configured remote of the project's main checkout as a
/// non-primary identity and persists the binding.
///
/// The binding is read back from the store rather than taken from `descriptor`, so an identity
/// recorded since this project opened is kept, and every other adopted project's binding is read
/// to refuse a URL that already routes to another clone.
pub async fn bind_remote_identity(
    descriptor: &ProjectDescriptor,
    remote_name: &str,
) -> Result<crate::api::IdentityReport> {
    #[cfg(target_os = "macos")]
    {
        let layout =
            crate::storage::StorageLayout::new(descriptor.storage.store(), &descriptor.repo_id)
                .map_err(native_integrity_error)?;
        let binding = read_persisted_binding(&layout).await?.ok_or_else(|| {
            CowshedError::integrity(
                format!("project {} has no repository binding", descriptor.repo_id),
                "cowshed doctor --json",
            )
        })?;
        let remotes = crate::git::GitRepository::from_root(&*descriptor.git_root)
            .remotes()
            .await?;
        let store = descriptor.storage.store().to_path_buf();
        let project = descriptor.repo_id.clone();
        let others =
            crate::storage::lifecycle::dispatch_blocking(move || other_bindings(&store, &project))
                .await
                .map_err(|error| CowshedError::internal(error.to_string()))??;
        let (identity, updated) = bind_remote(&binding, &remotes, remote_name, &others)?;
        let added = updated.is_some();
        let binding = match updated {
            Some(updated) => {
                persist_binding(&layout, &updated).await?;
                updated
            }
            None => binding,
        };
        Ok(crate::api::IdentityReport {
            repo_id: descriptor.repo_id.clone(),
            added,
            identity,
            identities: binding.identities,
        })
    }
    #[cfg(not(target_os = "macos"))]
    {
        let _ = (descriptor, remote_name);
        Err(CowshedError::environment_missing(
            "the native cowshed project runtime requires macOS APFS",
            "run the controller on macOS",
        ))
    }
}

/// Every adopted project's binding but `project`'s, read as the fetch-route refresh reads them.
#[cfg(target_os = "macos")]
fn other_bindings(store: &Path, project: &RepoId) -> Result<Vec<(RepoId, RepositoryBinding)>> {
    crate::gateway_inventory::discover_repositories(store)
        .map_err(native_integrity_error)?
        .into_iter()
        .filter(|repo| repo != project)
        .map(|repo| {
            let layout =
                crate::storage::StorageLayout::new(store, &repo).map_err(native_integrity_error)?;
            let binding = crate::gateway_inventory::read_typed_json_nofollow(
                &layout.project().repository_binding,
                crate::gateway_inventory::MAX_BINDING_BYTES,
            )
            .map_err(|error| {
                CowshedError::integrity(
                    format!("the repository binding of {repo} cannot be read: {error}"),
                    "cowshed doctor --json",
                )
            })?;
            Ok((repo, binding))
        })
        .collect()
}

/// Persists a binding atomically, off the async runtime: a transport-move heal and
/// `cowshed identity add` both write through here.
#[cfg(target_os = "macos")]
async fn persist_binding(
    layout: &crate::storage::StorageLayout,
    binding: &RepositoryBinding,
) -> Result<()> {
    let path = layout.project().repository_binding.clone();
    let binding = binding.clone();
    crate::storage::lifecycle::dispatch_blocking(move || {
        crate::metadata::write_json(&path, &binding)
    })
    .await
    .map_err(|error| CowshedError::internal(error.to_string()))?
    .map_err(native_integrity_error)
}

#[cfg(target_os = "macos")]
async fn read_persisted_binding(
    layout: &crate::storage::StorageLayout,
) -> Result<Option<RepositoryBinding>> {
    let path = layout.project().repository_binding.clone();
    crate::storage::lifecycle::dispatch_blocking(move || {
        match crate::metadata::read_json::<RepositoryBinding>(&path) {
            Ok(binding) => Ok(Some(binding)),
            Err(crate::metadata::MetadataError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Ok(None)
            }
            Err(error) => Err(error),
        }
    })
    .await
    .map_err(|error| CowshedError::internal(error.to_string()))?
    .map_err(native_integrity_error)
}

/// The adopted project that owns `repo_id`, which outranks its Git remotes.
///
/// Identity is a cowshed record, not a Git fact. `cowshed mv main --repo-id` changes it in place
/// and deliberately never touches the remote, so afterwards the recorded identity and the remote
/// legitimately disagree — that divergence is the feature, not a fault. Deriving identity from the
/// remotes whenever the store already records one would refuse to open the very project a rename
/// had just produced.
///
/// The lookup is by ownership rather than by path because the identity being asked about may be one
/// the project has left behind: a detached session's marker, or main's own marker when recovery
/// finished a change the marker rewrite never reached. The store path is tried first, since it
/// answers in one stat for every project that has never been renamed, and the scan runs only when
/// that misses.
///
/// `None` means no adopted project owns this identity, which is the only case where the remotes get
/// to decide what the project is called — that is adoption.
#[cfg(target_os = "macos")]
async fn project_owning_repo_id(
    store_root: &Path,
    repo_id: &RepoId,
) -> Result<Option<(crate::storage::StorageLayout, RepositoryBinding)>> {
    let layout =
        crate::storage::StorageLayout::new(store_root, repo_id).map_err(native_integrity_error)?;
    if let Some(binding) = read_persisted_binding(&layout).await? {
        let owned = binding.owned_repo_ids().map_err(native_integrity_error)?;
        if owned.current() != repo_id {
            return Err(CowshedError::conflict(
                format!(
                    "repository binding under {} names {owned}",
                    layout.project().project_root.display()
                ),
                "repair the repository binding before opening cowshed",
            ));
        }
        return Ok(Some((layout, binding)));
    }
    let Some(owner) = identity_owner_in_store(store_root, repo_id).await? else {
        return Ok(None);
    };
    let layout = crate::storage::StorageLayout::new(store_root, owner.current())
        .map_err(native_integrity_error)?;
    let binding = read_persisted_binding(&layout).await?.ok_or_else(|| {
        CowshedError::integrity(
            format!(
                "adopted project {} lost its repository binding while it was being read",
                owner.current()
            ),
            "cowshed doctor --json",
        )
    })?;
    Ok(Some((layout, binding)))
}

/// The live project that owns `repo_id`, found by scanning the store's bindings.
#[cfg(target_os = "macos")]
async fn identity_owner_in_store(
    store_root: &Path,
    repo_id: &RepoId,
) -> Result<Option<OwnedRepoIds>> {
    let store_root = store_root.to_owned();
    let repo_id = repo_id.clone();
    crate::storage::lifecycle::dispatch_blocking(move || {
        crate::gateway_inventory::identity_owner(&store_root, &repo_id)
            .map_err(native_integrity_error)
    })
    .await
    .map_err(|error| CowshedError::internal(error.to_string()))?
}

/// Refuse an identity a live project already owns, as its current identity or a recorded former one.
///
/// Identity uniqueness is scoped to live projects on purpose. Retirement deletes the binding, so a
/// retired project's former identities stop being anybody's and an archived original or a fork may
/// be adopted under a name a since-renamed project once used. Several repositories merged into one
/// monorepo likewise leave their old identities recorded in the monorepo alone, where they block
/// nothing but a second live claim on the same name.
#[cfg(target_os = "macos")]
async fn refuse_identity_owned_by_a_live_project(
    store_root: &Path,
    repo_id: &RepoId,
    // The project asking, which may of course own the identity: adoption re-running against its own
    // binding, and an identity change back onto a name this project itself recorded as former.
    asking: &RepoId,
) -> Result<()> {
    let Some(owner) = identity_owner_in_store(store_root, repo_id).await? else {
        return Ok(());
    };
    if owner.current() == asking {
        return Ok(());
    }
    Err(CowshedError::conflict(
        format!("repository identity {repo_id} is already owned by adopted project {owner}"),
        format!(
            "choose another identity, or retire {} first",
            owner.current()
        ),
    ))
}

/// Resolve a session invocation through the store binding, without opening the recorded checkout.
///
/// A session's marker names main's checkout, not the session repository the caller is standing
/// in. When those roots differ, the marker identity and persisted binding are the complete project
/// authority. In particular, the recorded checkout may be a missing direct mount; no Git command
/// may be aimed at it merely to learn an identity the store already records.
#[cfg(target_os = "macos")]
async fn project_binding_from_workspace_origin(
    store_root: &Path,
    invocation_root: &Path,
    origin: Option<&WorkspaceOrigin>,
) -> Result<Option<(RepoId, crate::storage::StorageLayout, RepositoryBinding)>> {
    let Some(origin) = origin else {
        return Ok(None);
    };
    if names_one_root(&origin.project_root, invocation_root) {
        return Ok(None);
    }
    let repo_id = origin.repo_id.clone();
    let (layout, binding) = project_owning_repo_id(store_root, &repo_id)
        .await?
        .ok_or_else(|| {
            CowshedError::integrity(
                format!("adopted project {repo_id} has no persisted repository binding"),
                "cowshed doctor --json",
            )
        })?;
    Ok(Some((repo_id, layout, binding)))
}

#[cfg(target_os = "macos")]
async fn load_or_validate_binding(
    layout: &crate::storage::StorageLayout,
    candidate: RepositoryBinding,
    git: &crate::git::GitRepository,
) -> Result<RepositoryBinding> {
    let candidate_repo_id = candidate
        .primary()
        .map_err(native_integrity_error)?
        .repo_id
        .clone();
    let loaded = read_persisted_binding(layout).await?;
    let binding = loaded.unwrap_or(candidate);
    if binding.primary().map_err(native_integrity_error)?.repo_id != candidate_repo_id {
        return Err(CowshedError::conflict(
            "persisted repository identity differs from the opened storage layout",
            "repair the repository binding before opening cowshed",
        ));
    }
    let remotes = git.remotes().await?;
    // Re-adoption after a server move lands here with a persisted binding recording the old
    // transport; the same heal that fixes open fixes it, persisted immediately for the same
    // reason: every later reader must see the URL Git actually uses.
    match reconcile_binding_with_remotes(
        &binding,
        &remotes,
        BindingRemoteValidation::Strict,
        git.root(),
    )? {
        Some(updated) => {
            persist_binding(layout, &updated).await?;
            Ok(updated)
        }
        None => Ok(binding),
    }
}

#[cfg(target_os = "macos")]
async fn enforce_adopt_secret_policy(
    root: PathBuf,
    waivers_path: PathBuf,
    quarantine_root: PathBuf,
    quarantine: bool,
) -> Result<()> {
    crate::storage::lifecycle::dispatch_blocking(move || {
        let waivers = match crate::metadata::read_json::<Vec<crate::secrets::SecretWaiver>>(
            &waivers_path,
        ) {
            Ok(waivers) => waivers,
            Err(crate::metadata::MetadataError::Io { source, .. })
                if source.kind() == std::io::ErrorKind::NotFound =>
            {
                Vec::new()
            }
            Err(error @ crate::metadata::MetadataError::Json { .. }) => {
                return Err(CowshedError::integrity(
                    error.to_string(),
                    format!(
                        "repair the waivers file first (or delete it to start without waivers): {}",
                        crate::secrets::waiver_guidance(&waivers_path),
                    ),
                ));
            }
            Err(error) => return Err(native_integrity_error(error)),
        };
        let scan = crate::secrets::scan_tree(&root, &waivers)
            .map_err(|error| secret_scan_error(&waivers_path, error))?;
        if scan.findings.is_empty() {
            return Ok(());
        }
        if !quarantine {
            return Err(secret_findings_error(&scan.findings, &waivers_path));
        }
        quarantine_secret_files(&root, &quarantine_root, &scan.findings)?;
        let remaining = crate::secrets::scan_tree(&root, &waivers)
            .map_err(|error| secret_scan_error(&waivers_path, error))?;
        if remaining.findings.is_empty() {
            Ok(())
        } else {
            Err(secret_findings_error(&remaining.findings, &waivers_path))
        }
    })
    .await
    .map_err(|error| CowshedError::internal(format!("secret scan task failed: {error}")))?
}

#[cfg(target_os = "macos")]
fn secret_scan_error(waivers_path: &Path, error: crate::secrets::SecretScanError) -> CowshedError {
    match error {
        crate::secrets::SecretScanError::InvalidWaiver { .. }
        | crate::secrets::SecretScanError::DuplicateWaiver { .. } => CowshedError::integrity(
            error.to_string(),
            format!(
                "repair the controller-owned waivers file first: {}",
                crate::secrets::waiver_guidance(waivers_path),
            ),
        ),
        crate::secrets::SecretScanError::InvalidRoot { .. }
        | crate::secrets::SecretScanError::Walk { .. }
        | crate::secrets::SecretScanError::Read { .. } => CowshedError::environment_missing(
            error.to_string(),
            "make the complete repository tree readable and retry adopt",
        ),
    }
}

#[cfg(target_os = "macos")]
fn secret_findings_error(
    findings: &[crate::secrets::SecretFinding],
    waivers_path: &Path,
) -> CowshedError {
    let paths = findings
        .iter()
        .map(|finding| finding.path.display().to_string())
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect::<Vec<_>>()
        .join(", ");
    CowshedError::conflict(
        format!("repository contains secrets in: {paths}"),
        format!(
            "remove the files, or waive a false positive: {}; otherwise retry adopt with --quarantine",
            crate::secrets::waiver_guidance(waivers_path),
        ),
    )
}

#[cfg(target_os = "macos")]
fn quarantine_secret_files(
    root: &Path,
    quarantine_root: &Path,
    findings: &[crate::secrets::SecretFinding],
) -> Result<()> {
    let paths = findings
        .iter()
        .map(|finding| finding.path.clone())
        .collect::<std::collections::BTreeSet<_>>();
    secure_quarantine_directory(quarantine_root, Path::new(""))?;
    for relative in paths {
        let source = root.join(&relative);
        let source_metadata = match std::fs::symlink_metadata(&source) {
            Ok(metadata) => metadata,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
            Err(error) => return Err(quarantine_io_error("inspect secret source", &source, error)),
        };
        if !source_metadata.is_file() || source_metadata.file_type().is_symlink() {
            return Err(CowshedError::conflict(
                format!(
                    "secret source {} changed after the full-tree scan",
                    relative.display()
                ),
                "stop repository writers and retry adopt",
            ));
        }
        let parent = relative.parent().unwrap_or_else(|| Path::new(""));
        let destination_parent = secure_quarantine_directory(quarantine_root, parent)?;
        let file_name = relative.file_name().ok_or_else(|| {
            CowshedError::integrity(
                format!("secret finding has no file name: {}", relative.display()),
                "run cowshed doctor --json",
            )
        })?;
        let destination = destination_parent.join(file_name);
        if destination.exists() {
            if files_equal(&source, &destination)? {
                std::fs::set_permissions(
                    &destination,
                    std::os::unix::fs::PermissionsExt::from_mode(0o600),
                )
                .map_err(|error| {
                    quarantine_io_error("secure quarantined secret", &destination, error)
                })?;
                std::fs::remove_file(&source).map_err(|error| {
                    quarantine_io_error("remove quarantined source", &source, error)
                })?;
                sync_parent(&source)?;
                continue;
            }
            return Err(CowshedError::conflict(
                format!(
                    "quarantine destination {} already contains different bytes",
                    destination.display()
                ),
                "move the existing quarantine artifact aside and retry adopt",
            ));
        }
        let temporary = destination_parent.join(crate::fsio::temp_name(
            destination
                .file_name()
                .unwrap_or_else(|| std::ffi::OsStr::new("cowshed-quarantine")),
            uuid::Uuid::new_v4().simple(),
        ));
        if let Err(error) = std::fs::copy(&source, &temporary) {
            return Err(quarantine_io_error(
                "copy secret into quarantine",
                &temporary,
                error,
            ));
        }
        let prepared = (|| {
            std::fs::set_permissions(
                &temporary,
                std::os::unix::fs::PermissionsExt::from_mode(0o600),
            )
            .map_err(|error| quarantine_io_error("secure quarantined secret", &temporary, error))?;
            std::fs::File::open(&temporary)
                .and_then(|file| file.sync_all())
                .map_err(|error| {
                    quarantine_io_error("sync quarantined secret", &temporary, error)
                })?;
            if !files_equal(&source, &temporary)? {
                return Err(CowshedError::conflict(
                    format!(
                        "secret source {} changed while it was quarantined",
                        relative.display()
                    ),
                    "stop repository writers and retry adopt",
                ));
            }
            std::fs::rename(&temporary, &destination).map_err(|error| {
                quarantine_io_error("publish quarantined secret", &destination, error)
            })?;
            sync_parent(&destination)?;
            std::fs::remove_file(&source).map_err(|error| {
                quarantine_io_error("remove quarantined source", &source, error)
            })?;
            sync_parent(&source)
        })();
        if prepared.is_err() {
            let _ = std::fs::remove_file(&temporary);
        }
        prepared?;
    }
    Ok(())
}

#[cfg(target_os = "macos")]
fn secure_quarantine_directory(root: &Path, relative: &Path) -> Result<PathBuf> {
    let mut current = root.to_path_buf();
    for component in std::iter::once(None).chain(relative.components().map(Some)) {
        if let Some(component) = component {
            let std::path::Component::Normal(component) = component else {
                return Err(CowshedError::integrity(
                    format!(
                        "secret quarantine path escapes its root: {}",
                        relative.display()
                    ),
                    "run cowshed doctor --json",
                ));
            };
            current.push(component);
        }
        match std::fs::symlink_metadata(&current) {
            Ok(metadata) if metadata.is_dir() && !metadata.file_type().is_symlink() => {}
            Ok(_) => {
                return Err(CowshedError::integrity(
                    format!(
                        "secret quarantine directory is not a real directory: {}",
                        current.display()
                    ),
                    "repair the controller-owned quarantine tree",
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                std::fs::create_dir(&current).map_err(|error| {
                    quarantine_io_error("create secret quarantine directory", &current, error)
                })?;
            }
            Err(error) => {
                return Err(quarantine_io_error(
                    "inspect secret quarantine directory",
                    &current,
                    error,
                ));
            }
        }
        std::fs::set_permissions(
            &current,
            std::os::unix::fs::PermissionsExt::from_mode(0o700),
        )
        .map_err(|error| {
            quarantine_io_error("secure secret quarantine directory", &current, error)
        })?;
    }
    Ok(current)
}

#[cfg(target_os = "macos")]
fn files_equal(left: &Path, right: &Path) -> Result<bool> {
    use std::io::Read;

    let mut left = std::io::BufReader::new(
        std::fs::File::open(left)
            .map_err(|error| quarantine_io_error("open secret source", left, error))?,
    );
    let mut right = std::io::BufReader::new(
        std::fs::File::open(right)
            .map_err(|error| quarantine_io_error("open quarantined secret", right, error))?,
    );
    let mut left_buffer = [0_u8; 16 * 1024];
    let mut right_buffer = [0_u8; 16 * 1024];
    loop {
        let left_read = left
            .read(&mut left_buffer)
            .map_err(|error| CowshedError::environment_missing(error.to_string(), "retry adopt"))?;
        let right_read = right
            .read(&mut right_buffer)
            .map_err(|error| CowshedError::environment_missing(error.to_string(), "retry adopt"))?;
        if left_read != right_read || left_buffer[..left_read] != right_buffer[..right_read] {
            return Ok(false);
        }
        if left_read == 0 {
            return Ok(true);
        }
    }
}

#[cfg(target_os = "macos")]
fn sync_parent(path: &Path) -> Result<()> {
    let parent = path.parent().ok_or_else(|| {
        CowshedError::integrity(
            format!("path has no parent: {}", path.display()),
            "run cowshed doctor --json",
        )
    })?;
    std::fs::File::open(parent)
        .and_then(|directory| directory.sync_all())
        .map_err(|error| quarantine_io_error("sync directory", parent, error))
}

#[cfg(target_os = "macos")]
fn quarantine_io_error(operation: &str, path: &Path, error: std::io::Error) -> CowshedError {
    CowshedError::environment_missing(
        format!("{operation} at {} failed: {error}", path.display()),
        "check repository and controller storage permissions, then retry adopt",
    )
}

#[cfg(target_os = "macos")]
fn pre_cowshed_path(root: &Path) -> Result<PathBuf> {
    if root.file_name().is_none() {
        return Err(CowshedError::usage(
            "repository root has no final component",
            "move the repository to a supported path",
        ));
    }
    let mut path = root.as_os_str().to_owned();
    path.push(".pre-cowshed");
    Ok(PathBuf::from(path))
}

#[cfg(all(test, target_os = "macos"))]
mod pre_cowshed_tests {
    use std::ffi::OsString;
    use std::os::unix::ffi::{OsStrExt, OsStringExt};

    use super::*;

    #[test]
    fn handoff_suffix_preserves_the_exact_repository_path_bytes() {
        assert_eq!(
            pre_cowshed_path(Path::new("/tmp/widget")).expect("UTF-8 root"),
            Path::new("/tmp/widget.pre-cowshed")
        );

        let mut root = PathBuf::from("/tmp");
        root.push(OsString::from_vec(vec![b'w', 0x80, b's']));
        let mut expected = root.as_os_str().as_bytes().to_vec();
        expected.extend_from_slice(b".pre-cowshed");
        assert_eq!(
            pre_cowshed_path(&root)
                .expect("opaque Unix root")
                .as_os_str()
                .as_bytes(),
            expected
        );
    }
}

#[cfg(all(test, target_os = "macos"))]
mod removal_refusal_tests {
    use super::*;

    fn workspace() -> WorkspaceName {
        WorkspaceName::new("raven").expect("fixed workspace name")
    }

    fn oid(fill: char) -> GitOid {
        GitOid::new(fill.to_string().repeat(40)).expect("fixed oid")
    }

    fn fence(dirty: bool, in_progress: Option<&str>) -> NativeRemovalGitFence {
        NativeRemovalGitFence {
            incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
                .expect("fixed incarnation"),
            head: oid('4'),
            dirty,
            in_progress: in_progress.map(str::to_owned),
        }
    }

    fn state(commits: LandingCommits) -> NativeLandedState {
        NativeLandedState {
            branch: DEFAULT_LANDING_BRANCH.to_owned(),
            commits,
        }
    }

    fn measured(unlanded: u64, landed: u64) -> NativeLandedState {
        state(LandingCommits::Measured {
            target_branch: DEFAULT_LANDING_BRANCH.to_owned(),
            target_head: oid('1'),
            unlanded,
            landed,
            behind: 0,
        })
    }

    fn indeterminate() -> NativeLandedState {
        state(LandingCommits::Indeterminate {
            reason: String::from("main's repository has no main branch"),
        })
    }

    /// Every refusal a removal can answer with, so the sweep below cannot miss one.
    fn every_removal_refusal() -> Vec<CowshedError> {
        vec![
            removal_in_progress_refusal(&workspace(), "rebase-merge"),
            removal_dirty_refusal(&workspace()),
            removal_unlanded_refusal(&workspace(), &oid('4'), &measured(3, 1)),
            removal_unlanded_refusal(&workspace(), &oid('4'), &indeterminate()),
            removal_head_moved_refusal(&workspace(), &oid('1'), &oid('4')),
            main_removal_mode_refusal(),
            NativeProjectRuntimeHost::require_session_state_clean(&workspace(), &fence(true, None))
                .expect_err("dirty is refused"),
            NativeProjectRuntimeHost::require_session_state_clean(
                &workspace(),
                &fence(false, Some("MERGE_HEAD")),
            )
            .expect_err("an in-progress operation is refused"),
        ]
    }

    /// The regression that produced this gate: a refusal that prescribed `--force` taught
    /// coordinator scripts to retry with it, and the retry destroyed unlanded work. A refusal may
    /// never name the flag that overrides it — the flags are documented in `cowshed rm`'s usage
    /// text, where a human reads options deliberately.
    #[test]
    fn no_removal_refusal_prescribes_the_flag_that_overrides_it() {
        for refusal in every_removal_refusal() {
            for (field, value) in [("message", &refusal.message), ("hint", &refusal.hint)] {
                for flag in ["--force", "--abandon"] {
                    assert!(
                        !value.contains(flag),
                        "removal refusal {field} prescribes {flag}: {value}"
                    );
                }
            }
            assert_eq!(refusal.code, ErrorCode::Conflict);
        }
    }

    /// The refusal has to be actionable on its own: it names the head that is at risk, how much of
    /// it is unheld, the branch that does not hold it, and where that branch stands.
    #[test]
    fn the_unlanded_refusal_names_the_head_the_count_the_branch_and_the_tip() {
        let refusal = removal_unlanded_refusal(&workspace(), &oid('4'), &measured(3, 1));
        assert_eq!(
            refusal.message,
            format!(
                "workspace raven head {} carries 3 commits that main does not hold, by ancestry or \
                 by patch equivalence (main is at {})",
                oid('4'),
                oid('1')
            )
        );
        assert_eq!(refusal.hint, "land the workspace: cowshed land raven");

        // One commit reads as one commit. A gate that says "1 commits" is a gate nobody trusts.
        assert!(
            removal_unlanded_refusal(&workspace(), &oid('4'), &measured(1, 0))
                .message
                .contains("carries 1 commit that main"),
        );

        // No measurement names the unanswered question rather than printing an absence, because the
        // caller is being refused for a missing proof and not for work they can see.
        let refusal = removal_unlanded_refusal(&workspace(), &oid('4'), &indeterminate());
        assert_eq!(
            refusal.message,
            format!(
                "workspace raven head {} cannot be proven to be in main: main's repository has no \
                 main branch",
                oid('4')
            )
        );
    }

    /// The half of the gate this change exists to fix. Patch equivalence satisfies "landed", so a
    /// workspace whose work reached main by squash-merge or a history rewrite retires with no flag
    /// at all — while genuinely unheld work still requires the one flag that authorizes losing it.
    #[test]
    fn the_landed_gate_accepts_patch_equivalence_and_still_refuses_unheld_work() {
        // Landed by patch identity alone: nothing ahead is unheld, though one commit is not an
        // ancestor of main. No flag, and nothing to bundle.
        assert_eq!(
            removal_landed_decision(&workspace(), &oid('4'), measured(0, 1), false)
                .expect("patch-equivalent work needs no authorization"),
            None
        );

        // Nothing ahead at all — landed by ancestry — is the same answer by the same rule.
        assert_eq!(
            removal_landed_decision(&workspace(), &oid('4'), measured(0, 0), false)
                .expect("a workspace with nothing ahead needs no authorization"),
            None
        );

        // Partly landed is not landed, and the refusal stands without `--abandon`.
        let refused = removal_landed_decision(&workspace(), &oid('4'), measured(2, 1), false)
            .expect_err("unheld commits must be refused");
        assert_eq!(refused.code, ErrorCode::Conflict);

        // `--abandon` is what turns that refusal into an authorized loss, and it answers with the
        // state to bundle rather than with a bare go-ahead.
        assert_eq!(
            removal_landed_decision(&workspace(), &oid('4'), measured(2, 1), true)
                .expect("--abandon authorizes the loss"),
            Some(measured(2, 1))
        );

        // An unanswered question is refused exactly as unheld work is, and `--abandon` is still the
        // only way past it — so a stale or unreadable target can never authorize a deletion.
        assert!(
            removal_landed_decision(&workspace(), &oid('4'), indeterminate(), false).is_err(),
            "an indeterminate verdict must never read as landed"
        );
        assert_eq!(
            removal_landed_decision(&workspace(), &oid('4'), indeterminate(), true)
                .expect("--abandon authorizes a loss it cannot measure"),
            Some(indeterminate())
        );
    }

    /// `--force` and `--abandon` authorize different losses, and the dispatcher must not let either
    /// stand in for the other. Only the transient half is overridable by `--force`.
    #[test]
    fn force_overrides_transient_state_and_nothing_else() {
        let clean = fence(false, None);
        NativeProjectRuntimeHost::require_session_state_clean(&workspace(), &clean)
            .expect("a clean workspace passes the transient gate");
        assert!(
            NativeProjectRuntimeHost::require_session_state_clean(&workspace(), &fence(true, None))
                .is_err(),
            "dirt is exactly what the transient gate is for"
        );
    }
}

#[cfg(all(test, target_os = "macos"))]
mod removal_supervisor_tests {
    use std::{
        collections::HashMap,
        sync::{Arc, Mutex},
        time::Duration,
    };

    use super::NativeProjectRuntimeHost;
    use crate::{
        api::dto::{
            BinaryData, CommandArg, ExecCommand, ExecRequest, ExitStatus, JobId, OutputStorage,
            OutputSummary, ProtectedOutput, RunSandboxMode, SealedJob, Sha256Digest, StdinSource,
            StreamInfo,
        },
        error::{CowshedError, Result},
        runtime::supervisor::{
            ArtifactSeal, ArtifactSink, ArtifactWrite, CheckpointBarrier, CommitmentDraft,
            CommitmentSink, ProcessEvent, ProcessSignal, ProcessSpawnRequest, RunningProcess,
            SpawnSink, WorkspaceSupervisor, WorkspaceSupervisorConfig,
        },
        storage::job_artifact::{JobEnding, StreamKind},
    };

    fn empty_stream() -> StreamInfo {
        let data = BinaryData::new(Vec::<u8>::new()).expect("empty output");
        StreamInfo {
            storage: OutputStorage::Captured {
                artifact: ProtectedOutput::Inline { data },
            },
            bytes: 0,
            sha256: Sha256Digest::compute(&[]),
            summary: OutputSummary {
                version: 1,
                text: String::new(),
                truncated: false,
            },
        }
    }

    struct TestArtifacts;

    impl ArtifactSink for TestArtifacts {
        fn next_job_id(&self) -> Result<JobId> {
            Ok(JobId::new(1).expect("test job id"))
        }

        fn admit(
            &mut self,
            _job_id: JobId,
            _grant_revision: u64,
            _command: &ExecCommand,
        ) -> Result<()> {
            Ok(())
        }

        fn prepare_background(&mut self, _job_id: JobId) -> Result<()> {
            Ok(())
        }

        fn write(
            &mut self,
            _job_id: JobId,
            _stream: StreamKind,
            bytes: &[u8],
        ) -> Result<ArtifactWrite> {
            Ok(ArtifactWrite {
                accepted_bytes: bytes.len(),
                output_limit: None,
            })
        }

        fn seal(
            &mut self,
            _job_id: JobId,
            _ending: JobEnding,
            _stdout_copy: Option<crate::api::dto::OutputPublication>,
            _stderr_copy: Option<crate::api::dto::OutputPublication>,
        ) -> Result<ArtifactSeal> {
            Ok(ArtifactSeal {
                stdout: empty_stream(),
                stderr: empty_stream(),
                terminal_batch_sha256: Sha256Digest::compute(&[]),
                output_limit: None,
                publication_failure: None,
            })
        }

        fn checkpoint(&mut self) -> Result<CheckpointBarrier> {
            Ok(CheckpointBarrier {
                checkpoint_id: "test".into(),
                barrier_id: 0,
                manifest_batch_sha256: Sha256Digest::compute(&[]),
            })
        }

        fn sealed(&self, _job_id: JobId) -> Option<SealedJob> {
            None
        }
    }

    struct TestCommitments;

    #[async_trait::async_trait]
    impl CommitmentSink for TestCommitments {
        async fn record(&mut self, _draft: CommitmentDraft) -> Result<()> {
            Ok(())
        }
    }

    struct TestSpawner(Arc<Mutex<Vec<ProcessSignal>>>);

    #[async_trait::async_trait]
    impl SpawnSink for TestSpawner {
        async fn spawn(
            &mut self,
            request: ProcessSpawnRequest,
            events: tokio::sync::mpsc::Sender<ProcessEvent>,
        ) -> Result<Box<dyn RunningProcess>> {
            // Not a process: its pid names nothing this test owns, so no group is identified.
            let birth = super::super::job_groups::Birth::Unobserved {
                pid: 42,
                reason: "a test process has no process group".into(),
            };
            events
                .send(ProcessEvent::Started {
                    job_id: request.job_id,
                    birth: birth.clone(),
                })
                .await
                .map_err(|_| CowshedError::internal("test process event channel closed"))?;
            Ok(Box::new(TestProcess {
                job_id: request.job_id,
                birth,
                events,
                signals: self.0.clone(),
            }))
        }
    }

    struct TestProcess {
        job_id: JobId,
        birth: super::super::job_groups::Birth,
        events: tokio::sync::mpsc::Sender<ProcessEvent>,
        signals: Arc<Mutex<Vec<ProcessSignal>>>,
    }

    impl TestProcess {
        fn send(&self, event: ProcessEvent) -> Result<()> {
            self.events
                .try_send(event)
                .map_err(|_| CowshedError::internal("test process event channel closed"))
        }
    }

    impl RunningProcess for TestProcess {
        fn birth(&self) -> Option<&super::super::job_groups::Birth> {
            Some(&self.birth)
        }

        fn try_write_stdin(&mut self, _bytes: bytes::Bytes) -> Result<bool> {
            Ok(true)
        }

        fn close_stdin(&mut self) -> Result<()> {
            Ok(())
        }

        fn end_stdin(&mut self) {}

        fn signal_process_tree(&mut self, signal: ProcessSignal) -> Result<()> {
            self.signals.lock().expect("signal log").push(signal);
            if signal == ProcessSignal::Kill {
                self.send(ProcessEvent::Exited {
                    job_id: self.job_id,
                    exit: ExitStatus::Signaled {
                        signal: libc::SIGKILL,
                        core_dumped: false,
                    },
                })?;
                self.send(ProcessEvent::OutputEof {
                    job_id: self.job_id,
                    stream: StreamKind::Stdout,
                })?;
                self.send(ProcessEvent::OutputEof {
                    job_id: self.job_id,
                    stream: StreamKind::Stderr,
                })?;
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn forced_removal_terminates_running_jobs_with_term_then_kill() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-rm-force-jobs-{}",
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir(&root).expect("create workspace");
        // Capability detection refuses a directory that resolves outside the workspace, and
        // `/var/folders` resolves into `/private/var`.
        let root = std::fs::canonicalize(&root).expect("canonical workspace");
        let defaults = WorkspaceSupervisorConfig::default();
        let config = WorkspaceSupervisorConfig {
            workspace_root: root.clone(),
            default_cwd: None,
            sandbox: crate::sandbox::SandboxConfig {
                workspace_mount: root.clone(),
                ..defaults.sandbox
            },
            term_grace: Duration::from_millis(10),
            ..defaults
        };
        let signals = Arc::new(Mutex::new(Vec::new()));
        let supervisor = WorkspaceSupervisor::start_with_sinks(
            config,
            Box::new(TestSpawner(signals.clone())),
            Box::new(TestArtifacts),
            Box::new(TestCommitments),
        )
        .expect("start supervisor");
        let job = supervisor
            .exec_background(
                None,
                None,
                ExecRequest {
                    command: ExecCommand::Argv(vec![CommandArg::new("waiting-child")]),
                    cwd: None,
                    mode: RunSandboxMode::ReadWrite,
                    env: HashMap::new(),
                    trace: None,
                    stdin: StdinSource::Empty,
                    stdout_copy: None,
                    stderr_copy: None,
                },
            )
            .await
            .expect("start job");
        let started = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if supervisor.info(job).await.expect("job info").pid == Some(42) {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await;
        if started.is_err() {
            supervisor
                .retire()
                .await
                .expect("clean up test supervisor after start timeout");
        }
        started.expect("test job started");

        let stopped = tokio::time::timeout(
            Duration::from_secs(10),
            NativeProjectRuntimeHost::stop_supervisor_handle(&supervisor, true),
        )
        .await;
        if !matches!(&stopped, Ok(Ok(()))) {
            supervisor
                .retire()
                .await
                .expect("clean up test supervisor after force-stop failure");
        }
        stopped
            .expect("force retirement completed")
            .expect("retire supervisor");

        assert_eq!(
            signals.lock().expect("signal log").as_slice(),
            &[ProcessSignal::Term, ProcessSignal::Kill]
        );
        std::fs::remove_dir_all(root).expect("remove workspace");
    }
}

#[cfg(target_os = "macos")]
fn revision_target(target: &crate::api::dto::RevisionTarget) -> String {
    match target {
        crate::api::dto::RevisionTarget::Branch(branch) => branch.as_str().to_owned(),
        crate::api::dto::RevisionTarget::Ref(reference) => reference.as_str().to_owned(),
        crate::api::dto::RevisionTarget::Oid(oid) => oid.as_str().to_owned(),
    }
}

#[cfg(target_os = "macos")]
fn current_snapshot_mount(
    host: &NativeProjectRuntimeHost,
    workspace: &NativeWorkspace,
) -> Result<PathBuf> {
    host.workspace_mount_path(workspace.derived.workspace.name())
}

/// Refuse a land whose target is not the branch the target's checkout has checked out.
///
/// Land fast-forwards through the target's checkout, and `merge` moves whichever branch that
/// checkout is on, so any other target would be reported as landed while a different branch moved.
/// Updating a branch that is not checked out is a separate path this runtime does not have.
#[cfg(target_os = "macos")]
async fn require_target_checked_out(git_root: &Path, target_branch: &str) -> Result<()> {
    let checked_out = crate::git::GitRepository::from_root(git_root)
        .current_branch()
        .await?;
    if checked_out.as_deref() == Some(target_branch) {
        return Ok(());
    }
    let actual = checked_out.as_ref().map_or_else(
        || "a detached HEAD".to_owned(),
        |branch| format!("branch {branch}"),
    );
    Err(CowshedError::fence_refusal(
        crate::error::FenceRefusal::TargetNotCheckedOut { checked_out },
        format!(
            "the checkout at {} has {actual} checked out, not the land target {target_branch}",
            git_root.display()
        ),
        format!(
            "check out {target_branch} in {}, then retry land",
            git_root.display()
        ),
    ))
}

/// The branch a lane base has checked out: what its units land on and rebase onto.
#[cfg(target_os = "macos")]
async fn target_checked_out_branch(root: &Path, target: &WorkspaceName) -> Result<String> {
    crate::git::GitRepository::from_root(root)
        .current_branch()
        .await?
        .ok_or_else(|| {
            CowshedError::fence_refusal(
                crate::error::FenceRefusal::TargetNotCheckedOut { checked_out: None },
                format!("workspace {target} has a detached HEAD, so it has no branch to land on"),
                format!("check out a branch in workspace {target}, then retry"),
            )
        })
}

/// Hand a unit's validated head to the target and fast-forward the target's checked-out branch to
/// it.
///
/// A workspace is a standalone repository — nothing has ever replicated its commits — so a bare
/// `merge` against the validated head would resolve to an object the target has never seen. The
/// fetch is the hand-back, pull-based like every other direction cowshed moves work, into the
/// unit's preservation ref in the target's repository. It is also the revalidation: if the unit
/// advanced since `source_head` was validated, what arrived is not what was checked.
#[cfg(target_os = "macos")]
async fn deliver_into(
    target: &WorkspaceName,
    target_root: &Path,
    source_mount: &Path,
    unit: &WorkspaceName,
    source_branch: &str,
    source_head: &GitOid,
    target_branch: &str,
) -> Result<()> {
    let preservation_ref = format!("refs/cowshed/{unit}/heads/{source_branch}");
    run_git_with_read(
        target_root,
        source_mount,
        [
            "fetch",
            "--no-tags",
            source_mount
                .to_str()
                .ok_or_else(|| CowshedError::internal("workspace mount path is not valid UTF-8"))?,
            &format!("+refs/heads/{source_branch}:{preservation_ref}"),
        ],
    )
    .await?;
    let fetched = git_revision_oid(target_root, &preservation_ref).await?;
    if &fetched != source_head {
        return Err(CowshedError::fence_refusal(
            crate::error::FenceRefusal::SourceMoved {
                observed: fetched.clone(),
            },
            format!("workspace {unit} advanced from {source_head} to {fetched} during land"),
            "re-run the check against the new head and retry land",
        ));
    }
    // Read again at the merge: the check can run for minutes, and the target's checkout can be
    // switched to another branch while it does.
    require_target_checked_out(target_root, target_branch).await?;
    // A target that moved past the unit's base cannot fast-forward. The next move is the unit's
    // rebase onto that target, and for a lane unit a bare `cowshed rebase` would rebase onto main,
    // so the hint names the destination.
    let repository = crate::git::GitRepository::from_root(target_root);
    if let Some(tip) = repository.branch_tip(target_branch).await?
        && !repository
            .commit_is_ancestor(tip.as_str(), source_head.as_str())
            .await?
    {
        let into = if target.is_main() {
            String::new()
        } else {
            format!(" --into {target}")
        };
        return Err(CowshedError::fence_refusal(
            crate::error::FenceRefusal::NotFastForward {
                target_head: tip.clone(),
            },
            format!(
                "{target}'s {target_branch} is at {tip}, which workspace {unit} is not based on, so \
                 it cannot fast-forward"
            ),
            format!("cowshed rebase {unit}{into}, re-run the check, then retry land"),
        ));
    }
    let merge = invoke_git(target_root, &["merge", "--ff-only", source_head.as_str()]).await?;
    let Err(refused) = require_git_success("git operation", &merge) else {
        return Ok(());
    };
    // git names the paths in prose; the target's own dirty reading names them as data.
    if String::from_utf8_lossy(&merge.stderr).contains("would be overwritten") {
        let work = repository.dirty_paths_by(None).await?;
        return Err(CowshedError::fence_refusal(
            crate::error::FenceRefusal::dirty(true, &work),
            refused.message,
            refused.hint,
        ));
    }
    Err(refused)
}

/// Preserve the workspace branch `source_branch` in main's repository at `main_root`, as
/// `refs/cowshed/<workspace>/heads/<source_branch>`.
///
/// Pull-based like every other direction cowshed moves work: git runs in main's repository and
/// only reads the workspace mount, so nothing the workspace configured — its remotes, its hooks —
/// takes part, and no remote name has to exist on either side. The fetch lands in a staging ref,
/// so the destination only ever moves to the exact object the caller's expectations were checked
/// against, in one compare-and-swap `update-ref`. The staging ref is removed on every path.
#[cfg(target_os = "macos")]
async fn preserve_workspace_branch(
    main_root: &Path,
    source_mount: &Path,
    workspace: &WorkspaceName,
    source_branch: &crate::api::dto::BranchName,
    expected_source_head: Option<&GitOid>,
    expected_destination_head: Option<&crate::api::dto::ExpectedRefHead>,
) -> Result<PushReport> {
    let branch = source_branch.as_str();
    let source_ref = format!("refs/heads/{branch}");
    if git_optional_ref_oid(source_mount, &source_ref)
        .await?
        .is_none()
    {
        return Err(CowshedError::conflict(
            format!("workspace {workspace} has no branch {branch} to push"),
            format!(
                "commit on {branch} in the workspace, or name the branch to preserve: cowshed push {workspace} --branch <name>"
            ),
        ));
    }
    let destination_ref = format!("refs/cowshed/{workspace}/heads/{branch}");
    let staging_ref = format!(
        "refs/cowshed/{workspace}/staging/{}",
        uuid::Uuid::new_v4().simple()
    );
    run_git_with_read(
        main_root,
        source_mount,
        [
            "fetch",
            "--no-tags",
            "--no-write-fetch-head",
            source_mount
                .to_str()
                .ok_or_else(|| CowshedError::internal("workspace mount path is not valid UTF-8"))?,
            &format!("+{source_ref}:{staging_ref}"),
        ],
    )
    .await?;
    let installed = install_preserved(
        main_root,
        &staging_ref,
        destination_ref,
        expected_source_head,
        expected_destination_head,
    )
    .await;
    let unstaged = run_git(main_root, ["update-ref", "-d", &staging_ref]).await;
    match (installed, unstaged) {
        (installed, Ok(())) => installed,
        (installed, Err(leak)) => Err(CowshedError::integrity(
            format!(
                "{}; the staging ref {staging_ref} in {} remains: {}",
                match installed {
                    Ok(report) => format!(
                        "preserved {} at {}",
                        report.destination_ref,
                        report.source_head.as_str()
                    ),
                    Err(error) => error.message,
                },
                main_root.display(),
                leak.message
            ),
            format!("git -C {} update-ref -d {staging_ref}", main_root.display()),
        )),
    }
}

/// Move `destination_ref` to what `staging_ref` holds, refusing any expectation that moved.
#[cfg(target_os = "macos")]
async fn install_preserved(
    main_root: &Path,
    staging_ref: &str,
    destination_ref: String,
    expected_source_head: Option<&GitOid>,
    expected_destination_head: Option<&crate::api::dto::ExpectedRefHead>,
) -> Result<PushReport> {
    let source_head = git_revision_oid(main_root, staging_ref).await?;
    require_source_head(expected_source_head, &source_head, "push")?;
    let previous_destination_head = git_optional_ref_oid(main_root, &destination_ref).await?;
    require_expected_ref(
        expected_destination_head,
        previous_destination_head.as_ref(),
        "push destination",
    )?;
    // The old value makes the install a compare-and-swap under git's ref lock: a destination that
    // moved after it was read refuses rather than being overwritten. Empty means "must not exist".
    let previous = previous_destination_head
        .as_ref()
        .map_or("", GitOid::as_str);
    run_git(
        main_root,
        [
            "update-ref",
            &destination_ref,
            source_head.as_str(),
            previous,
        ],
    )
    .await?;
    Ok(PushReport {
        source_head,
        destination_ref,
        previous_destination_head,
    })
}

/// Bring the branch a lane base has checked out into the unit at `unit_root`, as
/// `refs/cowshed/targets/<target>/<branch>`, and answer that ref: what the unit rebases onto.
///
/// A lane base is a repository of its own, so its branch reaches a unit only by a fetch from its
/// mount — the same pull the land uses in the other direction. Fetching on every rebase is what
/// keeps the destination current as lane-mates land.
#[cfg(target_os = "macos")]
async fn fetch_target_branch(
    unit_root: &Path,
    target_root: &Path,
    target: &WorkspaceName,
) -> Result<String> {
    let branch = target_checked_out_branch(target_root, target).await?;
    let reference = format!("refs/cowshed/targets/{target}/{branch}");
    run_git_with_read(
        unit_root,
        target_root,
        [
            "fetch",
            "--no-tags",
            target_root
                .to_str()
                .ok_or_else(|| CowshedError::internal("workspace mount path is not valid UTF-8"))?,
            &format!("+refs/heads/{branch}:{reference}"),
        ],
    )
    .await?;
    Ok(reference)
}

/// A rebase has one destination: what the unit lands into, or an explicit revision.
#[cfg(target_os = "macos")]
fn require_single_destination(
    onto: Option<&crate::api::dto::RevisionTarget>,
    into: Option<&WorkspaceTarget>,
) -> Result<()> {
    if onto.is_some() && into.is_some() {
        return Err(CowshedError::usage(
            "a rebase takes onto or into, not both: into rebases onto the branch its target has \
             checked out",
            "drop onto to rebase onto what the unit lands into, or drop into to rebase onto a \
             revision",
        ));
    }
    Ok(())
}

/// A unit never lands into, or rebases onto, itself.
#[cfg(target_os = "macos")]
fn require_distinct_target(unit: &WorkspaceName, target: &WorkspaceName) -> Result<()> {
    if unit == target {
        return Err(CowshedError::usage(
            format!("workspace {unit} cannot land into itself"),
            "name the lane base this unit was forked from, or main",
        ));
    }
    Ok(())
}

/// Refuse a target that is no longer the workspace its reference was resolved to: a lane base
/// removed and recreated under the same name is a different workspace, and delivering into it
/// would put a unit's work where its coordinator never aimed it.
#[cfg(target_os = "macos")]
fn require_target_incarnation(
    target: &WorkspaceName,
    current: &WorkspaceIncarnation,
    resolved: &WorkspaceIncarnation,
) -> Result<()> {
    if current == resolved {
        return Ok(());
    }
    Err(CowshedError::fence_refusal(
        crate::error::FenceRefusal::IncarnationMoved {
            workspace: target.clone(),
            observed: current.clone(),
        },
        format!(
            "workspace {target} is incarnation {current}, not the {resolved} this reference was \
             resolved at: it was removed and recreated under the same name"
        ),
        format!(
            "resolve workspace {target} again and retry, if its new incarnation is the one this \
             unit belongs to"
        ),
    ))
}

/// Refuse a source workspace whose head is not the one the caller expected `verb` to act on.
#[cfg(target_os = "macos")]
fn require_source_head(expected: Option<&GitOid>, observed: &GitOid, verb: &str) -> Result<()> {
    match expected {
        Some(expected) if expected != observed => Err(CowshedError::fence_refusal(
            crate::error::FenceRefusal::SourceMoved {
                observed: observed.clone(),
            },
            format!("workspace source head is stale: it is {observed}, not {expected}"),
            format!("refresh the workspace revision and retry {verb}"),
        )),
        _ => Ok(()),
    }
}

/// Refuse a rebase whose destination `onto` resolved to another commit than the caller expected.
#[cfg(target_os = "macos")]
fn require_onto_head(expected: Option<&GitOid>, onto: &str, observed: &GitOid) -> Result<()> {
    match expected {
        Some(expected) if expected != observed => Err(CowshedError::fence_refusal(
            crate::error::FenceRefusal::OntoMoved {
                observed: observed.clone(),
            },
            format!("rebase destination head is stale: {onto} is at {observed}, not {expected}"),
            "refresh the destination revision and retry rebase",
        )),
        _ => Ok(()),
    }
}

/// Refuse a land whose target branch is not at the head the caller expected.
#[cfg(target_os = "macos")]
fn require_target_head(
    expected: Option<&crate::api::dto::ExpectedRefHead>,
    observed: Option<&GitOid>,
) -> Result<()> {
    require_expected_ref(expected, observed, "land target").map_err(|stale| {
        CowshedError::fence_refusal(
            crate::error::FenceRefusal::TargetMoved {
                observed: observed.cloned(),
            },
            stale.message,
            stale.hint,
        )
    })
}

#[cfg(target_os = "macos")]
async fn run_git<const N: usize>(root: &Path, args: [&str; N]) -> Result<()> {
    let output = invoke_git(root, &args).await?;
    require_git_success("git operation", &output)
}

#[cfg(target_os = "macos")]
async fn run_git_with_read<const N: usize>(
    root: &Path,
    read: &Path,
    args: [&str; N],
) -> Result<()> {
    let mut command =
        tokio::process::Command::from(crate::git::sandboxed_git_command_with_read(root, read)?);
    let output = command.args(args).output_locked().await.map_err(|error| {
        CowshedError::environment_missing(
            error.to_string(),
            "restore /usr/bin/git and sandbox-exec",
        )
    })?;
    require_git_success("git operation", &output)
}

/// Repository-selected Git commands (including merge drivers and filters) run
/// with a write boundary around the checkout, not with controller host reach.
#[cfg(target_os = "macos")]
async fn invoke_git(root: &Path, args: &[&str]) -> Result<std::process::Output> {
    let mut command = tokio::process::Command::from(crate::git::sandboxed_git_command_at(root)?);
    command.args(args).output_locked().await.map_err(|error| {
        CowshedError::environment_missing(
            error.to_string(),
            "restore /usr/bin/git and sandbox-exec",
        )
    })
}

#[cfg(target_os = "macos")]
async fn run_git_rebase_atomically(root: &Path, onto: &str, source_head: &GitOid) -> Result<()> {
    use self::invoke_git as invoke;

    let source_ref_output = invoke(root, &["symbolic-ref", "--quiet", "HEAD"]).await?;
    require_git_success("resolve workspace branch", &source_ref_output)?;
    let source_ref = String::from_utf8(source_ref_output.stdout)
        .map_err(|error| CowshedError::integrity(error.to_string(), "repair the git repository"))?;
    let source_ref = source_ref.trim_end();

    let git_dir_output = invoke(root, &["rev-parse", "--absolute-git-dir"]).await?;
    require_git_success("resolve git directory", &git_dir_output)?;
    let git_dir = String::from_utf8(git_dir_output.stdout)
        .map_err(|error| CowshedError::integrity(error.to_string(), "repair the git repository"))?;
    let git_dir = PathBuf::from(git_dir.trim_end());
    let rebase_state = [git_dir.join("rebase-merge"), git_dir.join("rebase-apply")];
    let had_rebase_state = rebase_state.iter().any(|path| path.exists());

    // `--no-autostash` overrides `rebase.autoStash` from every config layer, including a
    // repository's tracked include that enables it for all its clones. Autostash stashes a dirty
    // tree, rebases, then re-applies the stash; a re-apply that conflicts leaves unmerged paths
    // and a parked stash yet exits 0, so this verb would report success over a conflicted tree.
    // Without it git refuses a dirty tree before touching anything, which the classifier below
    // turns into "commit or discard the uncommitted work".
    let output = invoke(root, &["rebase", "--no-autostash", onto]).await?;
    let primary = match require_git_success("git operation", &output) {
        Ok(()) => return Ok(()),
        Err(error) => error,
    };
    // git's "could not apply <commit>" is a replayed commit that conflicted. The rollback below
    // removes every conflict marker, so the classifier's "resolve the git conflict" would send
    // the reader after markers that are gone; a replay conflict gets the by-hand replay instead.
    let replay_conflicted = String::from_utf8_lossy(&output.stderr).contains("could not apply");

    // A state that predates this command belongs to the user. Never turn a refused retry into
    // authority to abort or delete an operation cowshed did not start.
    if had_rebase_state {
        return Err(primary);
    }

    let owns_rebase_state = rebase_state.iter().any(|path| path.exists());
    let current_ref = invoke(root, &["symbolic-ref", "--quiet", "HEAD"]).await?;
    let current_ref_matches = current_ref.status.success()
        && String::from_utf8_lossy(&current_ref.stdout).trim_end() == source_ref;
    let current_head = invoke(root, &["rev-parse", "--verify", "HEAD"]).await?;
    let current_head_matches = current_head.status.success()
        && String::from_utf8_lossy(&current_head.stdout).trim_end() == source_head.as_str();
    if !owns_rebase_state && current_ref_matches && current_head_matches {
        // git refused before touching anything. A dirty tree is the refusal a caller acts on, so
        // it names the paths that block the rebase.
        if String::from_utf8_lossy(&output.stderr).contains("cannot rebase: ") {
            let work = crate::git::GitRepository::from_root(root)
                .tracked_change_paths()
                .await?;
            return Err(CowshedError::fence_refusal(
                crate::error::FenceRefusal::dirty(false, &work),
                primary.message,
                primary.hint,
            ));
        }
        return Err(primary);
    }

    // Abort is the lossless path because Git owns the sequencer state. A damaged sequencer may
    // refuse abort, so cowshed then removes only the two rebase directories it just created and
    // restores the exact ref and commit captured before mutation.
    let _ = invoke(root, &["rebase", "--abort"]).await;
    for path in &rebase_state {
        if path.exists() {
            let _ = tokio::fs::remove_dir_all(path).await;
        }
    }
    let _ = invoke(root, &["symbolic-ref", "HEAD", source_ref]).await;
    let _ = invoke(root, &["reset", "--hard", source_head.as_str()]).await;

    let mut failures = Vec::new();
    for path in &rebase_state {
        match tokio::fs::try_exists(path).await {
            Ok(false) => {}
            Ok(true) => failures.push(format!("{} still exists", path.display())),
            Err(error) => failures.push(format!("cannot inspect {}: {error}", path.display())),
        }
    }
    let restored_ref = invoke(root, &["symbolic-ref", "--quiet", "HEAD"]).await?;
    if !restored_ref.status.success()
        || String::from_utf8_lossy(&restored_ref.stdout).trim_end() != source_ref
    {
        failures.push(format!("HEAD is not attached to {source_ref}"));
    }
    match git_oid(root).await {
        Ok(restored) if &restored == source_head => {}
        Ok(restored) => failures.push(format!(
            "HEAD is {}, expected {}",
            restored.as_str(),
            source_head.as_str()
        )),
        Err(error) => failures.push(format!("cannot resolve restored HEAD: {}", error.message)),
    }

    if failures.is_empty() && replay_conflicted {
        Err(CowshedError::fence_refusal(
            crate::error::FenceRefusal::ReplayConflicted {
                rolled_back_to: source_head.clone(),
            },
            primary.message,
            format!(
                "the rebase was rolled back and the workspace is as it was: run `git rebase {onto}` inside the workspace, resolve the conflicts git names, and finish that rebase there"
            ),
        ))
    } else if failures.is_empty() {
        Err(primary)
    } else {
        Err(CowshedError::integrity(
            format!(
                "{}; automatic rebase rollback failed: {}",
                primary.message,
                failures.join("; ")
            ),
            "inspect git status and run cowshed doctor --json",
        ))
    }
}

#[cfg(target_os = "macos")]
async fn git_oid(root: &Path) -> Result<GitOid> {
    git_revision_oid(root, "HEAD").await
}

#[cfg(target_os = "macos")]
async fn git_revision_oid(root: &Path, revision: &str) -> Result<GitOid> {
    let output = invoke_git(root, &["rev-parse", "--verify", revision]).await?;
    require_git_success("resolve git revision", &output)?;
    let value = String::from_utf8(output.stdout)
        .map_err(|error| CowshedError::integrity(error.to_string(), "repair the git repository"))?;
    GitOid::new(value.trim_end()).map_err(native_integrity_error)
}

/// Resolve `reference`, answering `None` when this repository simply does not have it.
///
/// `rev-parse --verify --quiet` is the spelling that distinguishes those two outcomes: `show-ref
/// --verify` is *fatal* (exit 128) on an absent ref, which would turn "the target branch does not
/// exist yet" into an internal error instead of the `None` every caller here is written for.
#[cfg(target_os = "macos")]
async fn git_optional_ref_oid(root: &Path, reference: &str) -> Result<Option<GitOid>> {
    let output = invoke_git(root, &["rev-parse", "--verify", "--quiet", reference]).await?;
    if output.status.code() == Some(1) {
        return Ok(None);
    }
    require_git_success("resolve git reference", &output)?;
    let value = String::from_utf8(output.stdout)
        .map_err(|error| CowshedError::integrity(error.to_string(), "repair the git repository"))?;
    GitOid::new(value.trim_end())
        .map(Some)
        .map_err(native_integrity_error)
}

#[cfg(target_os = "macos")]
fn require_expected_ref(
    expected: Option<&crate::api::dto::ExpectedRefHead>,
    actual: Option<&GitOid>,
    dimension: &str,
) -> Result<()> {
    let matches = match (expected, actual) {
        (None, _) => true,
        (Some(crate::api::dto::ExpectedRefHead::Missing), None) => true,
        (Some(crate::api::dto::ExpectedRefHead::Oid(expected)), Some(actual)) => expected == actual,
        _ => false,
    };
    if matches {
        Ok(())
    } else {
        Err(CowshedError::conflict(
            format!("{dimension} revision is stale"),
            "refresh repository revisions and retry",
        ))
    }
}

/// The job one `land --check` command runs as: an ordinary read-write child of the workspace.
///
/// It carries no environment of its own, so a check builds the units an interactive command
/// builds. Forcing `CARGO_INCREMENTAL=0` here once made every workspace member a separate unit,
/// recompiled on each landing.
#[cfg(target_os = "macos")]
fn land_check_request(check: &str) -> ExecRequest {
    ExecRequest {
        command: crate::api::dto::ExecCommand::Argv(vec![
            "/bin/sh".into(),
            "-c".into(),
            check.into(),
        ]),
        cwd: None,
        mode: RunSandboxMode::ReadWrite,
        env: std::collections::HashMap::new(),
        trace: None,
        stdin: StdinSource::Empty,
        stdout_copy: None,
        stderr_copy: None,
    }
}

#[cfg(all(test, target_os = "macos"))]
mod land_check_tests {
    use super::land_check_request;

    /// A check's child gets what every child gets and nothing of its own: a check that forced
    /// `CARGO_INCREMENTAL=0` made every workspace member a second unit, recompiled on each land.
    #[test]
    fn a_land_check_leaves_incremental_to_the_profile() {
        assert!(land_check_request("cargo test").env.is_empty());
    }
}

#[cfg(target_os = "macos")]
/// Turn a failed git invocation into an error whose `next:` names what actually went wrong.
///
/// A single catch-all hint is worse than no hint: it covers several unrelated failures, so a
/// reader cannot tell which situation they are in — a dirty worktree that blocks a merge, a
/// branch pair that can no longer fast-forward, a real conflict, or something else entirely —
/// and one of those recourses is wrong for every other cause. Git's own diagnosis is always
/// quoted verbatim; what this classification adds is cowshed's recourse for the cause git
/// names, in cowshed's vocabulary (`cowshed rebase`, not git's merge menu).
fn require_git_success(operation: &str, output: &std::process::Output) -> Result<()> {
    if output.status.success() {
        return Ok(());
    }
    let stderr = String::from_utf8_lossy(&output.stderr);
    let message = format!("{operation} failed: {stderr}");
    // Each marker below is git naming a distinct situation; each gets the recourse for that
    // situation and no other. Anything unclassified keeps git's words plus the generic
    // inspect-state fallback rather than guessing.
    Err(
        if stderr.contains("CONFLICT")
            || stderr.contains("Automatic merge failed")
            || stderr.contains("could not apply")
            || stderr.contains("needs merge")
        {
            CowshedError::conflict(message, "resolve the git conflict and retry")
        } else if stderr.contains("would be overwritten by merge")
            || stderr.contains("would be overwritten by checkout")
            || stderr.contains("untracked working tree files would be overwritten")
            || stderr.contains("cannot rebase: Your index contains uncommitted changes")
            || stderr.contains("cannot rebase: You have unstaged changes")
        {
            CowshedError::conflict(
                message,
                "the target tree has uncommitted work: commit or discard it there, then retry",
            )
        } else if stderr.contains("Not possible to fast-forward")
            || stderr.contains("Diverging branches")
        {
            CowshedError::conflict(
                message,
                "the workspace base is behind the target: rebase first (cowshed rebase <ws>), then retry land",
            )
        } else {
            CowshedError::conflict(message, "inspect the repository state: git status")
        },
    )
}

/// The inputs Nx hashes for the missed task `task` in the workspace `handle` serves, from
/// `nx show target inputs <task> --json` run there (`nx::task_inputs` pins its shape), and their
/// digest. A task Nx cannot describe, or whose description cannot be read whole, keeps its hash
/// and says why; nothing is inferred from partial output.
#[cfg(target_os = "macos")]
async fn task_inputs(
    handle: &crate::runtime::supervisor::WorkspaceSupervisorHandle,
    build_volume: Option<PathBuf>,
    task: String,
    hash: String,
) -> Result<crate::api::dto::CacheMiss> {
    use crate::storage::job_artifact::StreamKind;
    let request = land_check_request(&format!(
        "exec node_modules/.bin/nx show target inputs '{}' --json",
        task.replace('\'', "'\\''")
    ));
    let job = handle.exec(None, build_volume, request).await?;
    let info = handle.wait(job).await?;
    let mut stdout = Vec::new();
    let mut offset = 0_u64;
    let read = loop {
        match handle
            .log_read(job, StreamKind::Stdout, offset, false)
            .await
        {
            Ok(chunk) => {
                stdout.extend_from_slice(&chunk.bytes);
                offset = chunk.next_offset;
                if chunk.eof {
                    break Ok(());
                }
            }
            Err(error) => break Err(error),
        }
    };
    let described = match (&info.exit, read) {
        (_, Err(error)) => Err(format!(
            "cannot read the output of nx show target inputs {task} at byte {offset}: {error}"
        )),
        (Some(crate::api::dto::ExitStatus::Exited { code: 0 }), Ok(())) => {
            crate::build_volume::nx::task_inputs(&task, &stdout)
        }
        (exit, Ok(())) => Err(format!(
            "nx show target inputs {task} ended {exit:?}: {}",
            read_job_stderr_tail(handle, job).await
        )),
    };
    let (inputs, inputs_error) = match described {
        Ok(inputs) => (inputs, None),
        Err(error) => (std::collections::BTreeMap::new(), Some(error)),
    };
    let inputs_digest = crate::api::dto::Sha256Digest::compute(
        &serde_json::to_vec(&inputs).map_err(|error| CowshedError::internal(error.to_string()))?,
    );
    Ok(crate::api::dto::CacheMiss {
        task,
        hash,
        inputs,
        inputs_digest,
        inputs_error,
    })
}

#[cfg(target_os = "macos")]
/// Read back the bounded tail of a finished job's stderr for diagnostic purposes.
///
/// Best effort by design: a check whose output cannot be read still fails with its exit
/// status; the tail only sharpens the message. The log API bounds each read, so a chatty
/// check cannot balloon this error.
async fn read_job_stderr_tail(
    handle: &crate::runtime::supervisor::WorkspaceSupervisorHandle,
    job_id: JobId,
) -> String {
    use crate::storage::job_artifact::StreamKind;
    let mut collected = Vec::new();
    let mut offset = 0_u64;
    while let Ok(chunk) = handle
        .log_read(job_id, StreamKind::Stderr, offset, false)
        .await
    {
        collected.extend_from_slice(&chunk.bytes);
        offset = chunk.next_offset;
        if chunk.eof || collected.len() >= DIAGNOSTIC_STDERR_LIMIT {
            break;
        }
    }
    if collected.len() > DIAGNOSTIC_STDERR_LIMIT {
        collected.drain(..collected.len() - DIAGNOSTIC_STDERR_LIMIT);
    }
    String::from_utf8_lossy(&collected).trim().to_owned()
}

#[cfg(target_os = "macos")]
/// The bound on child stderr kept in a land-check/exec diagnostic.
const DIAGNOSTIC_STDERR_LIMIT: usize = 2048;

#[cfg(target_os = "macos")]
/// Recognize a Seatbelt-class denial in a child's own stderr, quoted verbatim in the
/// resulting diagnostic.
///
/// The kernel surfaces a sandboxed denial to the child as EPERM ("Operation not permitted");
/// the same command outside the sandbox succeeds. That signature is what distinguishes "the
/// environment refused this" from "the workspace's code failed" — the misreporting that sends
/// callers to fix working code. Matching the child's words keeps this honest: no signature,
/// no environment claim.
fn sandbox_denial_in(stderr: &str) -> Option<String> {
    const DENIAL_MARKERS: [&str; 3] = [
        "Operation not permitted",
        "operation not permitted",
        "Permission denied",
    ];
    DENIAL_MARKERS.iter().find_map(|marker| {
        stderr
            .lines()
            .find(|line| line.contains(marker))
            .map(str::to_owned)
    })
}

fn requested_port_block_size(delta: &GrantDelta, revoke: bool) -> Result<Option<u16>> {
    let Some(service_ports) = delta.service_ports else {
        return Ok(None);
    };
    if revoke {
        return Err(CowshedError::usage(
            "workspace service port capacity cannot be revoked",
            "request a minimum capacity with cowshed grant --ports",
        ));
    }
    if cfg!(target_os = "linux") {
        return Err(CowshedError::usage(
            "Linux workspaces already have private network namespaces, not port blocks",
            "bind service ports directly inside the workspace",
        ));
    }
    crate::metadata::PortBlock::size_for_service_ports(service_ports)
        .map(Some)
        .map_err(|_| {
            CowshedError::usage(
                format!(
                    "{service_ports} service ports do not fit the host's macOS workspace port range"
                ),
                "request a positive capacity within the available host port space",
            )
        })
}

#[cfg(target_os = "macos")]
fn normalize_grant_delta(delta: &mut GrantDelta) -> Result<()> {
    normalize_grant_paths(&mut delta.read)?;
    normalize_grant_paths(&mut delta.write)?;
    normalize_relative_denies(&mut delta.deny_write)?;
    normalize_relative_denies(&mut delta.deny)
}

#[cfg(target_os = "macos")]
fn normalize_relative_denies(paths: &mut Vec<PathBuf>) -> Result<()> {
    for path in paths.iter_mut() {
        if path.as_os_str().is_empty()
            || path
                .components()
                .any(|component| !matches!(component, std::path::Component::Normal(_)))
        {
            return Err(CowshedError::usage(
                format!(
                    "workspace deny {} must be a relative path without traversal",
                    path.display()
                ),
                "name a path beneath the workspace without . or ..",
            ));
        }
        *path = path.components().collect();
    }
    paths.sort();
    paths.dedup();
    Ok(())
}

#[cfg(target_os = "macos")]
fn normalize_grant_paths(paths: &mut Vec<PathBuf>) -> Result<()> {
    for path in paths.iter_mut() {
        let original = std::mem::take(path);
        *path = resolve_grant_path(&original)?;
    }
    paths.sort();
    paths.dedup();
    Ok(())
}

#[cfg(target_os = "macos")]
fn grant_path_usage(path: &Path, detail: impl std::fmt::Display) -> CowshedError {
    CowshedError::usage(
        format!("grant path {} {detail}", path.display()),
        "choose an absolute filesystem path",
    )
}

#[cfg(target_os = "macos")]
fn resolve_grant_path(path: &Path) -> Result<PathBuf> {
    if !path.is_absolute() || crate::sandbox::canonical_lexical_absolute(path).is_none() {
        return Err(grant_path_usage(
            path,
            "must be absolute and must not traverse above /",
        ));
    }
    match std::fs::canonicalize(path) {
        Ok(real) => require_canonical_grant_path(path, real),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            resolve_missing_grant_path(path)
        }
        Err(error) => Err(grant_path_usage(
            path,
            format!("cannot be resolved: {error}"),
        )),
    }
}

#[cfg(target_os = "macos")]
fn require_canonical_grant_path(original: &Path, real: PathBuf) -> Result<PathBuf> {
    if !crate::repository::is_lexically_canonical(&real) {
        return Err(grant_path_usage(
            original,
            format!("resolved to non-canonical {}", real.display()),
        ));
    }
    Ok(real)
}

#[cfg(target_os = "macos")]
fn ambiguous_grant_parent_dir(path: &Path) -> Result<PathBuf> {
    Err(grant_path_usage(path, "has '..' after a missing component"))
}

#[cfg(target_os = "macos")]
fn resolve_missing_grant_path(path: &Path) -> Result<PathBuf> {
    let mut resolved = PathBuf::from("/");
    let mut remaining = path.components();
    if !matches!(remaining.next(), Some(std::path::Component::RootDir)) {
        return Err(grant_path_usage(
            path,
            "must be absolute and must not traverse above /",
        ));
    }
    let mut remaining = remaining.peekable();
    while let Some(component) = remaining.peek().copied() {
        match component {
            std::path::Component::CurDir => {
                remaining.next();
            }
            std::path::Component::Prefix(_) | std::path::Component::RootDir => {
                return Err(grant_path_usage(path, "is not canonical"));
            }
            std::path::Component::ParentDir => match std::fs::canonicalize(resolved.join("..")) {
                Ok(real) => {
                    resolved = real;
                    remaining.next();
                }
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                    return ambiguous_grant_parent_dir(path);
                }
                Err(error) => {
                    return Err(grant_path_usage(
                        path,
                        format!("cannot be resolved: {error}"),
                    ));
                }
            },
            std::path::Component::Normal(name) => {
                match std::fs::canonicalize(resolved.join(name)) {
                    Ok(real) => {
                        resolved = real;
                        remaining.next();
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::NotFound => break,
                    Err(error) => {
                        return Err(grant_path_usage(
                            path,
                            format!("cannot be resolved: {error}"),
                        ));
                    }
                }
            }
        }
    }
    for component in remaining {
        match component {
            std::path::Component::ParentDir => return ambiguous_grant_parent_dir(path),
            std::path::Component::CurDir => {}
            std::path::Component::Normal(name) => resolved.push(name),
            std::path::Component::RootDir | std::path::Component::Prefix(_) => {
                return Err(grant_path_usage(path, "is not canonical"));
            }
        }
    }
    require_canonical_grant_path(path, resolved)
}

#[cfg(target_os = "macos")]
fn apply_grant_delta(grants: &mut GrantSet, delta: GrantDelta, revoke: bool) {
    update_ordered_set(&mut grants.read, delta.read, revoke);
    update_ordered_set(&mut grants.write, delta.write, revoke);
    update_ordered_set(&mut grants.deny_write, delta.deny_write, revoke);
    update_ordered_set(&mut grants.deny, delta.deny, revoke);
    update_egress(&mut grants.egress, delta.egress, revoke);
    update_ordered_set(&mut grants.repos, delta.repos, revoke);
    update_ordered_set(&mut grants.sim, delta.sim, revoke);
}

#[cfg(target_os = "macos")]
fn update_ordered_set<T: Ord>(current: &mut Vec<T>, delta: Vec<T>, revoke: bool) {
    update_set(current, delta, revoke);
    current.sort();
    current.dedup();
}

#[cfg(target_os = "macos")]
fn update_set<T: PartialEq>(current: &mut Vec<T>, delta: Vec<T>, revoke: bool) {
    if revoke {
        current.retain(|value| !delta.contains(value));
    } else {
        for value in delta {
            if !current.contains(&value) {
                current.push(value);
            }
        }
    }
}

/// A host holds one egress rule: its mode and ports are that rule. Granting a host
/// restates its rule in place, or appends it for a host not yet granted; revoking a host removes
/// its rule whatever it holds. Comparing whole rules instead left an intercepted and an opaque rule
/// side by side for one host, so no grant could ever change a host's mode.
#[cfg(target_os = "macos")]
fn update_egress(
    current: &mut Vec<crate::metadata::EgressRule>,
    delta: Vec<crate::metadata::EgressRule>,
    revoke: bool,
) {
    for rule in delta {
        match (
            current.iter().position(|held| held.host == rule.host),
            revoke,
        ) {
            (Some(index), true) => {
                current.remove(index);
            }
            (Some(index), false) => current[index] = rule,
            (None, false) => current.push(rule),
            (None, true) => {}
        }
    }
}

#[cfg(all(test, target_os = "macos"))]
mod grant_unit_tests {
    use super::*;
    use crate::sandbox::{
        RunSandboxMode, SandboxConfig, SandboxError, SandboxGrants, validate_sandbox_config,
    };
    use std::os::unix::fs::PermissionsExt;

    fn unique_root(name: &str) -> crate::temp_root::TempRoot {
        crate::temp_root::TempRoot::new(&format!("cowshed-grant-{name}"))
    }

    fn canonical_dir(path: &Path) -> PathBuf {
        std::fs::create_dir_all(path).unwrap();
        std::fs::canonicalize(path).unwrap()
    }

    fn sandbox_at(
        home: &Path,
        mount_root: &Path,
        workspace_mount: &Path,
        project_root: &Path,
    ) -> SandboxConfig {
        SandboxConfig {
            home: home.to_path_buf(),
            mount_root: mount_root.to_path_buf(),
            workspace_mount: workspace_mount.to_path_buf(),
            shed_links: Vec::new(),
            exec_temp_dir: PathBuf::from("/private/tmp/cowshed-grant-unit"),
            port_block: crate::metadata::PortBlock::new(40_960, 16).unwrap(),
            retained_port_blocks: Vec::new(),
            mode: RunSandboxMode::ReadWrite,
            grants: SandboxGrants::default(),
            allowed_unix_sockets: Vec::new(),
            additional_denies: vec![project_root.to_path_buf()],
            git_worktree_repository: None,
            build_volume_mount: None,
            repository_caches: Vec::new(),
            capabilities: Default::default(),
        }
    }

    fn normalize_read(path: PathBuf) -> PathBuf {
        let mut delta = GrantDelta {
            read: vec![path],
            ..GrantDelta::default()
        };
        normalize_grant_delta(&mut delta).expect("grant path should resolve");
        delta.read.pop().expect("one resolved path")
    }

    #[test]
    fn filesystem_grants_are_normalized_deduplicated_and_sorted() {
        let root = unique_root("norm");
        let a = canonical_dir(&root.join("a"));
        let z = canonical_dir(&root.join("z"));
        let output = canonical_dir(&root.join("output"));
        let mut grants = GrantSet {
            read: vec![z.clone()],
            write: vec![output.clone()],
            ..GrantSet::default()
        };
        let mut delta = GrantDelta {
            read: vec![z.clone(), root.join("a/../a"), a.clone()],
            write: vec![output.join("./reports"), output.clone()],
            ..GrantDelta::default()
        };

        normalize_grant_delta(&mut delta).expect("valid filesystem grant paths");
        apply_grant_delta(&mut grants, delta, false);

        assert_eq!(grants.read, [a, z]);
        assert_eq!(grants.write, [output.clone(), output.join("reports")]);
    }

    #[test]
    fn workspace_delta_cannot_remove_project_write_denies() {
        let project = crate::project_policy::ProjectGrants {
            deny_write: vec![PathBuf::from(".git/hooks")],
            ..crate::project_policy::ProjectGrants::default()
        };
        let mut workspace = GrantSet {
            deny_write: vec![PathBuf::from(".workspace-policy")],
            ..GrantSet::default()
        };
        apply_grant_delta(
            &mut workspace,
            GrantDelta {
                deny_write: vec![
                    PathBuf::from(".git/hooks"),
                    PathBuf::from(".workspace-policy"),
                ],
                ..GrantDelta::default()
            },
            true,
        );
        let effective =
            crate::project_policy::effective_grants(&workspace, &project).expect("revisions fit");
        assert_eq!(effective.deny_write, [PathBuf::from(".git/hooks")]);
    }

    #[test]
    fn workspace_denies_refuse_absolute_and_traversing_paths() {
        for path in ["/tmp/elsewhere", "../.git/config", "."] {
            let mut paths = vec![PathBuf::from(path)];
            let error = normalize_relative_denies(&mut paths).expect_err("not workspace-relative");
            assert_eq!(error.code.as_str(), "usage", "{path}");
        }
        let mut paths = vec![PathBuf::from(".git/hooks/"), PathBuf::from(".git/hooks")];
        normalize_relative_denies(&mut paths).expect("valid workspace deny");
        assert_eq!(paths, [PathBuf::from(".git/hooks")]);
    }

    /// A read+write deny is granted and revoked like a write deny, and a workspace cannot revoke
    /// the project's.
    #[test]
    fn read_write_denies_merge_like_write_denies() {
        let project = crate::project_policy::ProjectGrants {
            deny: vec![PathBuf::from(".runtime")],
            ..crate::project_policy::ProjectGrants::default()
        };
        let mut workspace = GrantSet::default();
        apply_grant_delta(
            &mut workspace,
            GrantDelta {
                deny: vec![PathBuf::from(".env"), PathBuf::from("secrets")],
                ..GrantDelta::default()
            },
            false,
        );
        apply_grant_delta(
            &mut workspace,
            GrantDelta {
                deny: vec![PathBuf::from(".runtime"), PathBuf::from("secrets")],
                ..GrantDelta::default()
            },
            true,
        );
        assert_eq!(workspace.deny, [PathBuf::from(".env")]);
        let effective =
            crate::project_policy::effective_grants(&workspace, &project).expect("revisions fit");
        assert_eq!(
            effective.deny,
            [PathBuf::from(".env"), PathBuf::from(".runtime")]
        );
    }

    /// A host has one egress rule: its mode and ports are that rule. Granting a
    /// host again states its rule anew — that is how an operator turns an intercepted host opaque
    /// — and revoking a host removes its rule whatever mode it holds.
    #[test]
    fn an_egress_grant_restates_its_hosts_rule_and_a_revoke_removes_it_whatever_its_mode() {
        use crate::metadata::EgressMode;
        let rule = |host: &str, mode| crate::metadata::EgressRule {
            host: host.to_owned(),
            ports: Vec::new(),
            mode,
        };
        let mut grants = GrantSet {
            egress: vec![
                rule("proxy.golang.org", EgressMode::Intercept),
                rule("registry.example.test", EgressMode::Intercept),
            ],
            ..GrantSet::default()
        };

        apply_grant_delta(
            &mut grants,
            GrantDelta {
                egress: vec![rule("proxy.golang.org", EgressMode::Opaque)],
                ..GrantDelta::default()
            },
            false,
        );
        assert_eq!(
            grants.egress,
            [
                rule("proxy.golang.org", EgressMode::Opaque),
                rule("registry.example.test", EgressMode::Intercept),
            ]
        );

        apply_grant_delta(
            &mut grants,
            GrantDelta {
                egress: vec![rule("proxy.golang.org", EgressMode::Intercept)],
                ..GrantDelta::default()
            },
            true,
        );
        assert_eq!(
            grants.egress,
            [rule("registry.example.test", EgressMode::Intercept)]
        );
    }

    #[test]
    fn filesystem_grants_reject_relative_paths_and_root_escape() {
        for path in ["relative/path", "/../../private"] {
            let mut delta = GrantDelta {
                read: vec![PathBuf::from(path)],
                ..GrantDelta::default()
            };
            let error = normalize_grant_delta(&mut delta).expect_err("invalid grant path");
            assert_eq!(error.code, ErrorCode::Usage);
            assert!(error.message.contains(path));
        }
    }

    #[test]
    fn missing_grant_leaf_resolves_through_the_existing_ancestor() {
        let ancestor = std::fs::canonicalize(std::env::temp_dir()).unwrap();
        let missing = std::env::temp_dir().join(format!(
            "cowshed-grant-missing-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let resolved = normalize_read(missing.clone());
        assert_eq!(
            resolved,
            ancestor.join(missing.file_name().expect("leaf name"))
        );
    }

    #[test]
    fn unresolved_parent_dir_after_a_missing_component_is_usage() {
        let sneaky = std::env::temp_dir()
            .join(format!("cowshed-grant-no-such-{}", std::process::id()))
            .join("..")
            .join("passwd");
        let mut delta = GrantDelta {
            read: vec![sneaky.clone()],
            ..GrantDelta::default()
        };
        let error = normalize_grant_delta(&mut delta).expect_err("ambiguous ..");
        assert_eq!(error.code, ErrorCode::Usage);
        assert!(
            error.message.contains("missing component"),
            "{}",
            error.message
        );
    }

    #[test]
    fn non_not_found_resolution_failure_is_explicit_usage() {
        let root = unique_root("perm");
        let locked = canonical_dir(&root.join("locked"));
        let child = locked.join("secret");
        std::fs::create_dir_all(&child).unwrap();
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o000)).unwrap();
        let mut delta = GrantDelta {
            read: vec![child],
            ..GrantDelta::default()
        };
        let error = normalize_grant_delta(&mut delta);
        std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o700)).unwrap();
        let error = error.expect_err("permission denied is not a lexical fallback");
        assert_eq!(error.code, ErrorCode::Usage);
        assert!(
            error.message.contains("cannot be resolved"),
            "{}",
            error.message
        );
    }

    #[test]
    fn grant_pipeline_allows_an_external_tree_and_denies_workspace_project_and_secrets() {
        let root = unique_root("deny");
        let home = canonical_dir(&root.join("home"));
        let mount_root = canonical_dir(&root.join("mnt"));
        let workspace = canonical_dir(&mount_root.join("acme/widget/workspaces/raven/mount"));
        let project = canonical_dir(&root.join("project"));
        let allowed = canonical_dir(&root.join("fork/minigraf"));
        let secret = canonical_dir(&home.join(".ssh"));
        let alias = root.join("alias-ssh");
        std::os::unix::fs::symlink(&secret, &alias).unwrap();

        let mut config = sandbox_at(&home, &mount_root, &workspace, &project);
        config.grants.read = vec![normalize_read(allowed.clone())];
        validate_sandbox_config(&config).expect("external tree is grantable");

        for denied in [
            workspace.join("src"),
            project.join("src"),
            secret.clone(),
            alias,
        ] {
            let mut denied_config = sandbox_at(&home, &mount_root, &workspace, &project);
            denied_config.grants.read = vec![normalize_read(denied)];
            assert!(
                matches!(
                    validate_sandbox_config(&denied_config),
                    Err(SandboxError::GrantIntersectsDeny { .. })
                ),
                "expected intersection deny"
            );
        }
    }
}

/// The grants `workspace` runs under: its own plus the project's standing grants from the trusted
/// project policy. One reader for every consumer — the supervisor sandbox, its reuse check, and
/// the grant verbs' validation — so none of them can see a different snapshot than the gateway
/// session built from the same policy.
#[cfg(target_os = "macos")]
fn effective_workspace_grants(
    layout: &crate::storage::StorageLayout,
    workspace: &GrantSet,
) -> Result<GrantSet> {
    let project = read_project_policy(&layout.project().policy)?;
    crate::project_policy::effective_grants(workspace, &project.grants)
        .map_err(|error| CowshedError::integrity(error.to_string(), "cowshed doctor --json"))
}

/// The trusted project policy; a policy that cannot be read completely fails closed before any
/// supervisor launches or any grant is recorded against it.
#[cfg(target_os = "macos")]
fn read_project_policy(path: &Path) -> Result<crate::project_policy::ProjectPolicy> {
    crate::project_policy::ProjectPolicy::read(path).map_err(|error| {
        CowshedError::integrity(
            format!("project policy {} is unreadable: {error}", path.display()),
            "repair or remove the project policy file",
        )
    })
}

/// Refuse a grant snapshot the sandbox would refuse at launch, with the remedy for each cause.
#[cfg(target_os = "macos")]
fn validate_grant_sandbox(config: &crate::sandbox::SandboxConfig) -> Result<()> {
    crate::sandbox::validate_sandbox_config(config).map_err(|error| match error {
        crate::sandbox::SandboxError::GrantIntersectsDeny { .. } => CowshedError::sandbox_denied(
            error.to_string(),
            "choose a path outside workspace, controller, project, and credential roots",
        ),
        crate::sandbox::SandboxError::InvalidPath { .. } => {
            CowshedError::usage(error.to_string(), "choose an absolute filesystem path")
        }
        crate::sandbox::SandboxError::InvalidPortBlock { .. } => native_integrity_error(error),
        crate::sandbox::SandboxError::DenyResolution { .. } => CowshedError::environment_missing(
            error.to_string(),
            "repair the named deny path and retry",
        ),
    })
}

/// `build_volume_mount` is the checkout's build volume as resolved now
/// ([`crate::build_volume::BuildVolumeLayout::grant`]); a grant-only validation passes `None`.
#[cfg(target_os = "macos")]
#[allow(clippy::too_many_arguments)]
fn supervisor_sandbox(
    home: &Path,
    layout: &crate::storage::StorageLayout,
    telemetry_root: &Path,
    current: &NativeWorkspace,
    grants: &GrantSet,
    mount: PathBuf,
    main_mount: PathBuf,
    build_volume_mount: Option<PathBuf>,
) -> Result<crate::sandbox::SandboxConfig> {
    let repository = crate::storage::bootstrap::main_cowshed_config(&main_mount)?;
    crate::sandbox::workspace_sandbox(crate::sandbox::WorkspaceSandbox {
        home,
        mount_root: &layout.project().host_mount_root,
        project_root: &layout.project().project_root,
        main_mount: &main_mount,
        telemetry_root,
        grants,
        repository_deny: repository.sandbox_deny(),
        repository_caches: repository.caches_home(),
        git_worktree_repository: git_worktree_repository(&current.metadata, main_mount.clone()),
        build_volume_mount,
        workspace_mount: mount,
        exec_temp_dir: layout
            .exec_temp_dir(&current.metadata.workspace)
            .map_err(native_integrity_error)?,
    })
}

/// What the project's `sessions/.trash` holds: retirements whose sidecar records them, and
/// images whose sidecar is already gone. The latter's record was reclaimed (cowshed removes the
/// sidecar last, so only an outside deletion leaves this), and only their bytes remain.
#[cfg(target_os = "macos")]
#[derive(Debug, Default)]
struct RetiredTrash {
    recorded: Vec<crate::storage::lifecycle::RetiredRef>,
    unrecorded: Vec<PathBuf>,
}

#[cfg(target_os = "macos")]
fn native_retired_refs(project_root: &Path, repo_id: &RepoId) -> Result<RetiredTrash> {
    use crate::metadata::{
        DetachedWorkspaceMetadata, IMAGE_EXTENSION, WorkspaceRole, is_image_path, sidecar_path,
    };
    use crate::storage::lifecycle::{LifecycleWorkspace, RetiredRef, Revision};

    let trash = project_root
        .join("sessions")
        .join(crate::storage::recovery::TRASH_NAMESPACE);
    let entries = match std::fs::read_dir(&trash) {
        Ok(entries) => entries
            .collect::<std::result::Result<Vec<_>, _>>()
            .map_err(|error| {
                CowshedError::integrity(
                    format!("cannot enumerate retired workspace trash: {error}"),
                    "cowshed doctor --json",
                )
            })?,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            return Ok(RetiredTrash::default());
        }
        Err(error) => {
            return Err(CowshedError::integrity(
                format!("cannot enumerate retired workspace trash: {error}"),
                "cowshed doctor --json",
            ));
        }
    };
    let mut images = entries
        .into_iter()
        .filter(|entry| is_image_path(&entry.path()))
        .collect::<Vec<_>>();
    images.sort_by_key(std::fs::DirEntry::file_name);

    let mut retired = Vec::new();
    retired
        .try_reserve(images.len())
        .map_err(|_| CowshedError::internal("cannot reserve retired workspace recovery facts"))?;
    let mut unrecorded = Vec::new();
    for entry in images {
        let file_type = entry.file_type().map_err(|error| {
            CowshedError::integrity(
                format!("cannot inspect retired workspace image: {error}"),
                "cowshed doctor --json",
            )
        })?;
        if !file_type.is_file() {
            return Err(CowshedError::integrity(
                format!(
                    "retired workspace image is not a regular file: {}",
                    entry.path().display()
                ),
                "cowshed doctor --json",
            ));
        }
        let sidecar = sidecar_path(&entry.path());
        match std::fs::symlink_metadata(&sidecar) {
            Ok(_) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                unrecorded.push(entry.path());
                continue;
            }
            Err(error) => {
                return Err(CowshedError::integrity(
                    format!(
                        "cannot inspect retired workspace metadata {}: {error}",
                        sidecar.display()
                    ),
                    "cowshed doctor --json",
                ));
            }
        }
        let metadata = DetachedWorkspaceMetadata::read_for_image(&entry.path())
            .map_err(native_integrity_error)?;
        // The exact trash name, sidecar and repository are the retirement fence. A clone
        // retired before activation retains PendingFence; it was never a runnable image.
        if metadata.repo_id != *repo_id {
            return Err(CowshedError::integrity(
                format!(
                    "retired workspace metadata identity mismatch: {}",
                    entry.path().display()
                ),
                "cowshed doctor --json",
            ));
        }
        let expected = trash.join(format!(
            "{}-{}.{IMAGE_EXTENSION}",
            metadata.workspace.as_str(),
            metadata.workspace_incarnation.as_str(),
        ));
        if entry.path() != expected {
            return Err(CowshedError::integrity(
                format!(
                    "retired workspace path disagrees with metadata identity: {}",
                    entry.path().display()
                ),
                "cowshed doctor --json",
            ));
        }
        let role = WorkspaceRole::for_name(&metadata.workspace);
        let revision = Revision::new(metadata.grants.revision);
        let workspace = LifecycleWorkspace::new(
            metadata.repo_id,
            metadata.workspace,
            metadata.workspace_incarnation,
            revision,
            revision,
            role,
        )
        .map_err(native_integrity_error)?;
        let resulting_revision = revision
            .get()
            .checked_add(1)
            .map(Revision::new)
            .ok_or_else(|| {
                CowshedError::integrity(
                    "retired workspace revision overflow",
                    "cowshed doctor --json",
                )
            })?;
        retired.push(RetiredRef::new(workspace, resulting_revision));
    }
    Ok(RetiredTrash {
        recorded: retired,
        unrecorded,
    })
}

#[cfg(all(test, target_os = "macos"))]
mod retired_recovery_tests {
    use super::*;
    use crate::metadata::{
        DetachedWorkspaceMetadata, GrantSet, Platform, PortBlock, PublicationState, SIDECAR_VERSION,
    };

    #[test]
    fn retired_trash_is_a_verified_restart_baseline_fact() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-retired-recovery-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let project_root = root.join("acme/widget");
        let trash = project_root.join("sessions/.trash");
        std::fs::create_dir_all(&trash).unwrap();
        let repo_id = RepoId::parse("acme/widget").unwrap();
        let incarnation = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap();
        let image = trash.join(format!("raven-{}.asif", incarnation.as_str()));
        std::fs::write(&image, b"retired image").unwrap();
        let mut grants =
            GrantSet::closed_baseline(Some(PortBlock::new(49_136, 16).unwrap())).unwrap();
        grants.revision = 4;
        DetachedWorkspaceMetadata {
            version: SIDECAR_VERSION,
            repo_id: repo_id.clone(),
            workspace: WorkspaceName::new("raven").unwrap(),
            workspace_incarnation: incarnation.clone(),
            platform: Platform::Macos,
            publication_state: PublicationState::Active,
            updated_at: "2026-07-14T00:00:00Z".into(),
            grants,
            info_snapshot: crate::metadata::WorkspaceInfoSnapshot {
                project_root: std::path::PathBuf::from("/project"),
                role: crate::metadata::WorkspaceRole::Workspace,
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                branch: None,
                created_at: "2026-07-14T00:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-07-14T00:00:00Z".to_owned(),
                stale: false,
                git_worktree: false,
            },
        }
        .write_for_image(&image)
        .unwrap();

        let retired = native_retired_refs(&project_root, &repo_id)
            .unwrap()
            .recorded;
        assert_eq!(retired.len(), 1);
        assert_eq!(retired[0].workspace().incarnation(), &incarnation);
        assert_eq!(
            retired[0].resulting_revision(),
            crate::storage::lifecycle::Revision::new(5)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn retired_pending_clone_uses_exact_trash_identity_even_after_name_reuse() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-retired-pending-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let project_root = root.join("example-org/example-app");
        let trash = project_root.join("sessions/.trash");
        std::fs::create_dir_all(&trash).expect("trash");
        let repo_id = RepoId::parse("example-org/example-app").expect("repo");
        let name = WorkspaceName::new("idle").expect("workspace");
        let incarnation =
            WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").expect("incarnation");
        let image = trash.join(format!("idle-{}.asif", incarnation.as_str()));
        std::fs::write(&image, b"retired pending clone").expect("retired image");
        let mut grants =
            GrantSet::closed_baseline(Some(PortBlock::new(49_136, 16).expect("port block")))
                .expect("grants");
        grants.revision = 4;
        DetachedWorkspaceMetadata {
            version: SIDECAR_VERSION,
            repo_id: repo_id.clone(),
            workspace: name.clone(),
            workspace_incarnation: incarnation.clone(),
            platform: Platform::Macos,
            publication_state: PublicationState::PendingFence,
            updated_at: "2026-07-14T00:00:00Z".into(),
            grants,
            info_snapshot: crate::metadata::WorkspaceInfoSnapshot {
                project_root: std::path::PathBuf::from("/project"),
                role: crate::metadata::WorkspaceRole::Workspace,
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                branch: None,
                created_at: "2026-07-14T00:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-07-14T00:00:00Z".to_owned(),
                stale: false,
                git_worktree: false,
            },
        }
        .write_for_image(&image)
        .expect("retired sidecar");

        let retired = native_retired_refs(&project_root, &repo_id)
            .expect("retired pending image is recoverable from its exact trash identity")
            .recorded;
        assert_eq!(retired.len(), 1);
        assert_eq!(retired[0].workspace().incarnation(), &incarnation);
        assert_eq!(
            retired[0].resulting_revision(),
            crate::storage::lifecycle::Revision::new(5)
        );
        assert!(image.exists(), "recovery does not delete the image");

        let mut mismatched =
            DetachedWorkspaceMetadata::read_for_image(&image).expect("read retired metadata");
        mismatched.workspace = WorkspaceName::new("other").expect("foreign workspace");
        mismatched
            .write_for_image(&image)
            .expect("change metadata identity");
        native_retired_refs(&project_root, &repo_id)
            .expect_err("a mismatched workspace identity cannot authorize reclamation");
        mismatched.workspace = name.clone();
        mismatched
            .write_for_image(&image)
            .expect("restore metadata identity");

        let live = project_root.join("sessions/idle.asif");
        std::fs::write(&live, b"new workspace").expect("new canonical image");
        mismatched.workspace_incarnation =
            WorkspaceIncarnation::new("1198f2c0b7e34dc795f17b238b331c80").expect("new incarnation");
        mismatched.publication_state = PublicationState::Active;
        mismatched
            .write_for_image(&live)
            .expect("new canonical metadata");
        let retired = native_retired_refs(&project_root, &repo_id)
            .expect("new use of the name cannot invalidate old retired trash")
            .recorded;
        assert_eq!(retired[0].workspace().incarnation(), &incarnation);
        assert!(live.exists(), "discovery cannot touch the new workspace");
        assert!(image.exists(), "discovery cannot delete old retired bytes");
        std::fs::remove_dir_all(root).expect("cleanup fixture");
    }

    #[test]
    fn retired_main_trash_preserves_main_role_for_restart_reclamation() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-retired-main-recovery-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let project_root = root.join("acme/widget");
        let trash = project_root.join("sessions/.trash");
        std::fs::create_dir_all(&trash).unwrap();
        let repo_id = RepoId::parse("acme/widget").unwrap();
        let incarnation = WorkspaceIncarnation::new("2198f2c0b7e34dc795f17b238b331c80").unwrap();
        let image = trash.join(format!("main-{}.asif", incarnation.as_str()));
        std::fs::write(&image, b"retired main image").unwrap();
        let mut grants = GrantSet::closed_baseline(Some(
            PortBlock::new(crate::metadata::MACOS_PORT_MAX - 15, 16).unwrap(),
        ))
        .unwrap();
        grants.revision = 8;
        DetachedWorkspaceMetadata {
            version: SIDECAR_VERSION,
            repo_id: repo_id.clone(),
            workspace: WorkspaceName::new("main").unwrap(),
            workspace_incarnation: incarnation.clone(),
            platform: Platform::Macos,
            publication_state: PublicationState::Active,
            updated_at: "2026-07-14T00:00:00Z".into(),
            grants,
            info_snapshot: crate::metadata::WorkspaceInfoSnapshot {
                project_root: std::path::PathBuf::from("/project"),
                role: crate::metadata::WorkspaceRole::Main,
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                branch: None,
                created_at: "2026-07-14T00:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-07-14T00:00:00Z".to_owned(),
                stale: false,
                git_worktree: false,
            },
        }
        .write_for_image(&image)
        .unwrap();

        let retired = native_retired_refs(&project_root, &repo_id)
            .unwrap()
            .recorded;
        assert_eq!(retired.len(), 1);
        assert_eq!(retired[0].workspace().incarnation(), &incarnation);
        assert_eq!(
            retired[0].workspace().role(),
            crate::metadata::WorkspaceRole::Main
        );
        assert_eq!(
            retired[0].resulting_revision(),
            crate::storage::lifecycle::Revision::new(9)
        );
        std::fs::remove_dir_all(root).unwrap();
    }

    /// Reclaiming a detached retired image runs no disk command; one that does is a test bug.
    struct NoDiskCommands;

    impl crate::apfs::CommandRunner for NoDiskCommands {
        fn run(
            &self,
            request: &crate::apfs::CommandRequest,
        ) -> std::result::Result<crate::apfs::CommandOutput, crate::apfs::CommandRunError> {
            panic!("reclaiming a detached retired image ran a disk command: {request:?}");
        }
        fn image_lease(
            &self,
            identity: &Path,
        ) -> std::io::Result<Option<crate::fork_lock::Fenced<std::fs::File>>> {
            panic!("reclaiming a detached retired image took an APFS image lease: {identity:?}");
        }
        fn pin_raw_device(
            &self,
            device: &Path,
        ) -> std::io::Result<Option<crate::fork_lock::Fenced<std::fs::File>>> {
            panic!("reclaiming a detached retired image pinned a raw APFS device: {device:?}");
        }
        fn attached_disk_images(&self) -> std::io::Result<Vec<crate::apfs::AttachedDiskImage>> {
            panic!("reclaiming a detached retired image read the kernel disk-image inventory");
        }
        fn grow_image(
            &self,
            image: &Path,
            _: crate::metadata::ImageCapacity,
        ) -> std::io::Result<()> {
            panic!("reclaiming a detached retired image grew an image: {image:?}");
        }
    }

    /// `doctor` opens the project to inspect it. Its open must leave a retired image where it is
    /// for doctor to report; every other open reclaims it.
    #[tokio::test]
    async fn an_inspecting_open_leaves_retired_trash_that_a_repairing_open_reclaims() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-open-residue-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let repo_id = RepoId::parse("acme/widget").unwrap();
        let layout = crate::storage::StorageLayout::new(&root, &repo_id).unwrap();
        let project_root = layout.project().project_root.clone();
        let trash = project_root.join("sessions/.trash");
        std::fs::create_dir_all(&trash).unwrap();
        let incarnation = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").unwrap();
        let image = trash.join(format!("raven-{}.asif", incarnation.as_str()));
        std::fs::write(&image, b"retired image").unwrap();
        DetachedWorkspaceMetadata {
            version: SIDECAR_VERSION,
            repo_id: repo_id.clone(),
            workspace: WorkspaceName::new("raven").unwrap(),
            workspace_incarnation: incarnation,
            platform: Platform::Macos,
            publication_state: PublicationState::Active,
            updated_at: "2026-07-14T00:00:00Z".into(),
            grants: GrantSet::closed_baseline(Some(PortBlock::new(49_136, 16).unwrap())).unwrap(),
            info_snapshot: crate::metadata::WorkspaceInfoSnapshot {
                project_root: PathBuf::from("/project"),
                role: crate::metadata::WorkspaceRole::Workspace,
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                branch: None,
                created_at: "2026-07-14T00:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-07-14T00:00:00Z".to_owned(),
                stale: false,
                git_worktree: false,
            },
        }
        .write_for_image(&image)
        .unwrap();
        let config = crate::storage::apfs::ApfsSubstrateConfig::new(&root, root.join("checkout"));
        let mut commitments = super::super::supervisor::CommitmentPublisher::open(
            root.join("telemetry"),
            crate::storage::audit::ContinuityAudit::Off,
            ROUTER_CAPACITY,
        )
        .unwrap();
        let mut open = async |scope: RecoveryScope| {
            let host = crate::storage::apfs::native::MacOsApfsExecutionHost::new(
                NoDiskCommands,
                config.clone(),
            )
            .unwrap();
            let retired = native_retired_refs(&project_root, &repo_id).unwrap();
            assert_eq!(retired.recorded.len(), 1, "the retired image is found");
            finish_store_residue(
                host,
                config.clone(),
                &repo_id,
                &[],
                retired,
                &mut commitments,
                &scope,
            )
            .await
            .unwrap();
        };

        open(RecoveryScope::Inspect).await;
        assert!(
            image.exists(),
            "an inspecting open reclaimed {}",
            image.display()
        );
        assert!(crate::metadata::sidecar_path(&image).exists());

        open(RecoveryScope::Workspaces(Default::default())).await;
        assert!(!image.exists(), "a repairing open left {}", image.display());
        std::fs::remove_dir_all(root).unwrap();
    }

    /// A retired entry whose grants sidecar was deleted by hand (its image and CA key left behind)
    /// once failed every open of the project with a metadata I/O error, which blocked every land
    /// and doctor in the repository. The record is gone, so the retirement is already reclaimed:
    /// an open of any kind succeeds, and a repairing open reclaims the bytes that remain.
    #[tokio::test]
    async fn a_trash_image_without_its_sidecar_is_reclaimed_not_an_open_failure() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-unrecorded-trash-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let repo_id = RepoId::parse("acme/widget").unwrap();
        let layout = crate::storage::StorageLayout::new(&root, &repo_id).unwrap();
        let project_root = layout.project().project_root.clone();
        let trash = project_root.join("sessions/.trash");
        std::fs::create_dir_all(&trash).unwrap();
        let image = trash.join("raven-0198f2c0b7e34dc795f17b238b331c80.asif");
        let companion = crate::metadata::append_suffix(&image, ".ca.key");
        std::fs::write(&image, b"retired image").unwrap();
        std::fs::write(&companion, b"retired CA key").unwrap();
        let config = crate::storage::apfs::ApfsSubstrateConfig::new(&root, root.join("checkout"));
        let mut commitments = super::super::supervisor::CommitmentPublisher::open(
            root.join("telemetry"),
            crate::storage::audit::ContinuityAudit::Off,
            ROUTER_CAPACITY,
        )
        .unwrap();
        let mut open = async |scope: RecoveryScope| {
            let host = crate::storage::apfs::native::MacOsApfsExecutionHost::new(
                NoDiskCommands,
                config.clone(),
            )
            .unwrap();
            let retired = native_retired_refs(&project_root, &repo_id)
                .expect("a sidecarless trash image does not fail the open");
            assert!(retired.recorded.is_empty());
            assert_eq!(retired.unrecorded, std::slice::from_ref(&image));
            finish_store_residue(
                host,
                config.clone(),
                &repo_id,
                &[],
                retired,
                &mut commitments,
                &scope,
            )
            .await
            .unwrap();
        };

        open(RecoveryScope::Inspect).await;
        assert!(
            image.exists(),
            "an inspecting open reclaimed {}",
            image.display()
        );

        open(RecoveryScope::Workspaces(Default::default())).await;
        assert!(!image.exists(), "a repairing open left {}", image.display());
        assert!(
            !companion.exists(),
            "a repairing open left {}",
            companion.display()
        );
        let log = std::fs::read_to_string(project_root.join("deletion-log.jsonl"))
            .expect("the reclaim is recorded in the deletion log");
        assert!(
            log.lines()
                .any(|line| line.contains("\"op\":\"reclaim-image\"")
                    && line.contains(image.to_str().unwrap())),
            "deletion log does not record the reclaimed image: {log}"
        );
        std::fs::remove_dir_all(root).unwrap();
    }
}

#[cfg(target_os = "macos")]
fn native_staged_error(
    error: crate::storage::apfs::StagedExecutionError<CowshedError>,
) -> CowshedError {
    match error {
        crate::storage::apfs::StagedExecutionError::Storage(error) => native_storage_error(error),
        crate::storage::apfs::StagedExecutionError::Initializer(error) => error,
        crate::storage::apfs::StagedExecutionError::InitializerCleanup {
            initializer,
            cleanup,
        } => CowshedError::integrity(
            format!("{initializer}; cleanup also failed: {cleanup}"),
            "cowshed doctor --json",
        ),
    }
}

#[cfg(target_os = "macos")]
fn native_retire_error(
    error: crate::storage::apfs::RetireExecutionError<CowshedError>,
) -> CowshedError {
    match error {
        crate::storage::apfs::RetireExecutionError::Storage(error) => native_storage_error(error),
        crate::storage::apfs::RetireExecutionError::Fence { source, .. } => source,
    }
}

#[cfg(target_os = "macos")]
fn native_restore_error(
    error: crate::storage::apfs::RestoreExecutionError<CowshedError>,
) -> CowshedError {
    match error {
        crate::storage::apfs::RestoreExecutionError::Storage(error) => native_storage_error(error),
        crate::storage::apfs::RestoreExecutionError::Activation { source: error, .. } => {
            native_storage_error(*error)
        }
        crate::storage::apfs::RestoreExecutionError::Fence { source: error, .. } => error,
    }
}

/// Read a capacity the way both image verbs spell it, refusing anything the tools would have to
/// round or reinterpret before the caller's workspace is touched.
#[cfg(target_os = "macos")]
fn parse_capacity(value: &str) -> Result<crate::metadata::ImageCapacity> {
    crate::metadata::ImageCapacity::parse(value).map_err(|error| {
        CowshedError::usage(
            error.to_string(),
            "use a capacity such as 100g, 200g, or 1t",
        )
    })
}

/// The ancestor incarnations a workspace's records may carry, read from the marker the image
/// itself holds (the clone source's lineage plus the source, written when the incarnation was
/// minted). A marker from before lineage was recorded is healed once: its ancestors are exactly
/// the foreign origins already in its records — every one was admitted by the controller that
/// wrote it — so the marker is rewritten with them and is strict from then on.
#[cfg(target_os = "macos")]
fn workspace_lineage(
    mount: &Path,
    current: &WorkspaceIncarnation,
    retained_recovery_budget_bytes: usize,
) -> Result<std::collections::BTreeSet<WorkspaceIncarnation>> {
    let marker_path = mount.join(crate::storage::WORKSPACE_MARKER_PATH);
    let mut marker = crate::metadata::WorkspaceMarker::read_from(&marker_path)
        .map_err(|error| CowshedError::integrity(error.to_string(), "cowshed doctor --json"))?;
    if marker.workspace_incarnation != *current {
        return Err(CowshedError::integrity(
            format!(
                "workspace marker names incarnation {} but the inventory says {current}",
                marker.workspace_incarnation
            ),
            "cowshed doctor --json",
        ));
    }
    if marker.lineage.is_none() {
        let recorded = crate::storage::job_artifact::recorded_historical_incarnations(
            mount,
            current,
            retained_recovery_budget_bytes,
        )
        .map_err(|error| CowshedError::integrity(error.to_string(), "cowshed doctor --json"))?;
        marker.lineage = Some(recorded.into_iter().collect());
        marker
            .validate()
            .map_err(|error| CowshedError::integrity(error.to_string(), "cowshed doctor --json"))?;
        // Persisting the healed marker is an optimization — the next open recomputes the same
        // lineage from the same records — so a write failure (a full disk, a read-only mount)
        // must not take the workspace down with it.
        let _ = crate::metadata::write_json(&marker_path, &marker);
    }
    Ok(marker.lineage.unwrap_or_default().into_iter().collect())
}

#[cfg(target_os = "macos")]
fn native_storage_error(error: crate::storage::apfs::ApfsStorageError) -> CowshedError {
    match error {
        crate::storage::apfs::ApfsStorageError::Conflict(error) => {
            CowshedError::lifecycle_conflict(error)
        }
        crate::storage::apfs::ApfsStorageError::GcPlanStale => CowshedError::retryable(
            crate::error::Retry::GcPlanStale,
            "garbage-collection plan became stale: an image was reclaimed or retired between the \
             plan and its execution, and nothing was collected",
            "retry: garbage collection plans again from the store as it is now",
        ),
        crate::storage::apfs::ApfsStorageError::PendingPublication(path) => CowshedError::conflict(
            format!("restore publication is pending at {}", path.display()),
            "repair the image or gateway evidence and retry restore",
        ),
        // Asking to shrink, or to resize to the size it already is, is a mistake in the request,
        // not a broken host: report it as usage so the caller is told to name a larger capacity.
        error @ crate::storage::apfs::ApfsStorageError::CapacityNotGrowing { .. } => {
            CowshedError::usage(
                error.to_string(),
                "cowshed resize <workspace> <capacity larger than the current one>",
            )
        }
        // A volume too full for the rewrite's copy is the operator's to free, and nothing was
        // touched: the storage is healthy, so the generic repair hint would mislead.
        error @ crate::storage::apfs::ApfsStorageError::InsufficientSpace { .. } => {
            CowshedError::environment_missing(
                error.to_string(),
                "free space on the store volume (cowshed gc reclaims what cowshed can), then retry",
            )
        }
        // A missing CA companion names its own remedy: only `rekey` rebuilds the companion,
        // so the generic doctor hint would send the reader after the wrong problem. The
        // workspace rides in the hint from the image's file stem, while both paths stay in the
        // message, where neither is guessable from the other.
        ref error @ crate::storage::apfs::ApfsStorageError::MissingCaCompanion {
            ref image, ..
        } => {
            let workspace = image
                .file_stem()
                .and_then(|name| name.to_str())
                .unwrap_or("workspace");
            CowshedError::integrity(error.to_string(), format!("cowshed rekey {workspace}"))
        }
        // A quarantine is an operator decision pending, not a host defect to diagnose: the
        // tombstone is in the message, and `rekey` consumes it — point at the repair.
        ref error @ crate::storage::apfs::ApfsStorageError::Quarantined { ref workspace, .. } => {
            CowshedError::integrity(error.to_string(), format!("cowshed rekey {workspace}"))
        }

        crate::storage::apfs::ApfsStorageError::MarkerMismatch(message)
        | crate::storage::apfs::ApfsStorageError::Host(message) => {
            CowshedError::integrity(message, "cowshed doctor --json")
        }
        other => CowshedError::storage_failure(
            other.to_string(),
            &other,
            "repair APFS storage and retry",
        ),
    }
}

/// Stop the Nx daemon `workspace`'s checkout at `mount` records, as a land stops it, before its
/// volume detaches: the daemon's cwd is the checkout, so a live one alone keeps it busy.
/// `.nx/workspace-data` is read through the checkout, so a build-volume link is followed.
/// [`crate::build_volume::nx::Busy`] is the caller's to judge: `detach` refuses on it, while a
/// retirement unmounts with its grace and force as it always has.
#[cfg(target_os = "macos")]
async fn close_checkout_nx(
    workspace: &WorkspaceName,
    mount: &Path,
) -> Result<std::result::Result<(), crate::build_volume::nx::Busy>> {
    let data = mount.join(".nx/workspace-data");
    let closed = {
        let data = data.clone();
        crate::storage::lifecycle::dispatch_blocking(move || {
            crate::build_volume::nx::close_workspace_data(&data)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("Nx close task failed: {error}")))?
    };
    closed.map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "cannot close {workspace}'s Nx state at {}: {error}",
                data.display()
            ),
            "cowshed doctor --json",
        )
    })
}

/// Whether a file system is mounted exactly at `path`: its device differs from its parent's.
/// A path nothing is mounted at is a directory of the volume that holds it.
#[cfg(target_os = "macos")]
fn is_mount_point(path: &Path) -> std::io::Result<bool> {
    use std::os::unix::fs::MetadataExt;
    let Some(parent) = path.parent() else {
        return Ok(true);
    };
    match std::fs::metadata(path) {
        Ok(metadata) => Ok(metadata.dev() != std::fs::metadata(parent)?.dev()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
        Err(error) => Err(error),
    }
}

/// Names a mounted volume through the APFS host's Disk Arbitration rename, after reading the
/// file system's own name for it so a name already right costs no round trip.
#[cfg(target_os = "macos")]
struct ApfsLabeller(
    std::sync::Arc<
        crate::storage::apfs::native::MacOsApfsExecutionHost<crate::apfs::SystemCommandRunner>,
    >,
);

#[cfg(target_os = "macos")]
impl super::supervisor::VolumeLabeller for ApfsLabeller {
    fn ensure_label(
        &self,
        mount: &Path,
        label: &str,
    ) -> std::io::Result<super::supervisor::Labelled> {
        use super::supervisor::Labelled;
        use crate::storage::apfs::ApfsExecutionHost;
        // A path nothing is mounted at names the volume that holds the directory: never renamed.
        if !is_mount_point(mount)? {
            return Ok(Labelled::NotMounted);
        }
        if crate::apfs::volume_name(mount)? == label {
            return Ok(Labelled::Already);
        }
        self.0
            .rename_volume(mount, label)
            .map_err(|error| std::io::Error::other(error.to_string()))?;
        Ok(Labelled::Renamed)
    }
}

/// The kernel refused to unmount `workspace` at `mount` because something holds it: name every
/// holder, pid and argv, so the next move is stopping them rather than repairing storage that is
/// not broken.
#[cfg(target_os = "macos")]
fn detach_refused(
    workspace: &WorkspaceName,
    mount: &Path,
    error: &crate::storage::apfs::ApfsStorageError,
) -> CowshedError {
    match crate::build_volume::nx::volume_holders(mount) {
        Ok(holders) if !holders.is_empty() => {
            let named = holders
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join("; ");
            let pids = holders
                .iter()
                .map(|holder| holder.pid.to_string())
                .collect::<Vec<_>>()
                .join(" ");
            CowshedError::conflict(
                format!(
                    "{workspace} is in use at {}, held by: {named}",
                    mount.display()
                ),
                format!(
                    "stop pid {pids} (or move their working directories off the workspace), \
                     then `cowshed detach {workspace}`"
                ),
            )
        }
        Ok(_) => CowshedError::conflict(
            format!(
                "{workspace} was busy at {} ({error}), and nothing holds it now",
                mount.display()
            ),
            format!("cowshed detach {workspace}"),
        ),
        Err(query) => CowshedError::conflict(
            format!(
                "{workspace} is in use at {} ({error}); its holders could not be listed: {query}",
                mount.display()
            ),
            format!(
                "`lsof +f -- {}` names them; stop them, then `cowshed detach {workspace}`",
                mount.display()
            ),
        ),
    }
}

#[cfg(target_os = "macos")]
fn native_environment_error(
    error: crate::storage::bootstrap::native::NativeBootstrapError,
) -> CowshedError {
    match error {
        crate::storage::bootstrap::native::NativeBootstrapError::StorageSetupRequired {
            actions,
            hint,
        } => CowshedError::environment_missing(
            format!("cowshed storage setup is required: {}", actions.join("; ")),
            hint,
        ),
        error => {
            CowshedError::environment_missing(error.to_string(), "repair host storage and retry")
        }
    }
}

// Not platform-specific: the body only builds a CowshedError. It was gated when every caller
// happened to be macOS-only, which broke the Linux cross-lint once the shared workspace-marker
// reader started using it.
fn native_integrity_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(error.to_string(), "cowshed doctor --json")
}

#[cfg(target_os = "macos")]
fn main_name() -> WorkspaceName {
    WorkspaceName::main()
}

/// Every refusal the removal path can answer with, in one place.
///
/// They are free functions rather than inline `CowshedError::conflict` calls for one reason: the
/// invariant that *no removal refusal may name the flag that overrides it* is only enforceable if
/// the refusals can be enumerated and swept. A coordinator script that reads a destructive flag in
/// a refusal's hint learns to reach for it by reflex, so the hints here name safe remedies only —
/// land it, commit it, finish the merge — and the destructive flag is documented where a human
/// reads options deliberately, in `cowshed rm`'s usage text.
#[cfg(target_os = "macos")]
fn removal_in_progress_refusal(workspace: &WorkspaceName, operation: &str) -> CowshedError {
    CowshedError::conflict(
        format!("workspace {workspace} has an in-progress {operation} Git operation"),
        "finish or abort the Git operation, then retry",
    )
}

#[cfg(target_os = "macos")]
fn removal_dirty_refusal(workspace: &WorkspaceName) -> CowshedError {
    CowshedError::conflict(
        format!("workspace {workspace} has uncommitted Git work"),
        format!("commit the work and land it: cowshed land {workspace}"),
    )
}

/// The one place that decides whether a removal may destroy a session's object store.
///
/// Pure, and separated from the measurement on purpose: this is the decision that destroys commits
/// or refuses to, and a decision worth testing directly is worth being able to test without a
/// substrate. `Some` means the caller authorized an abandonment and there is genuinely something to
/// bundle before deleting.
#[cfg(target_os = "macos")]
fn removal_landed_decision(
    workspace: &WorkspaceName,
    head: &GitOid,
    landed: NativeLandedState,
    abandon: bool,
) -> Result<Option<NativeLandedState>> {
    if landed.commits.fully_landed() {
        return Ok(None);
    }
    if abandon {
        return Ok(Some(landed));
    }
    Err(removal_unlanded_refusal(workspace, head, &landed))
}

/// Why the gate exists: these commits exist nowhere but the image about to be deleted.
#[cfg(target_os = "macos")]
fn removal_unlanded_refusal(
    workspace: &WorkspaceName,
    head: &GitOid,
    landed: &NativeLandedState,
) -> CowshedError {
    let branch = &landed.branch;
    CowshedError::conflict(
        match &landed.commits {
            LandingCommits::Measured {
                target_head,
                unlanded,
                ..
            } => format!(
                "workspace {workspace} head {head} carries {unlanded} commit{} that {branch} does \
                 not hold, by ancestry or by patch equivalence ({branch} is at {target_head})",
                if *unlanded == 1 { "" } else { "s" }
            ),
            // Saying which question went unanswered is the whole value of this branch: the caller
            // is being refused for a missing proof, not for work they can see.
            LandingCommits::Indeterminate { reason } => format!(
                "workspace {workspace} head {head} cannot be proven to be in {branch}: {reason}"
            ),
        },
        format!("land the workspace: cowshed land {workspace}"),
    )
}

/// A refused slot binding is the caller's problem, not the store's: the slot is taken, or the
/// workspace already has one. Only genuine record damage becomes an integrity error.
#[cfg(target_os = "macos")]
fn slot_binding_error(error: crate::storage::StorageLayoutError) -> CowshedError {
    use crate::metadata::MetadataError;
    use crate::storage::StorageLayoutError;

    match &error {
        StorageLayoutError::Metadata(
            MetadataError::SlotAlreadyBound { .. }
            | MetadataError::WorkspaceAlreadySlotted { .. }
            | MetadataError::MainIsNotSlottable
            | MetadataError::SlotOutOfRange(_),
        ) => CowshedError::conflict(error.to_string(), "choose a free slot: cowshed ls"),
        _ => native_integrity_error(error),
    }
}

#[cfg(target_os = "macos")]
fn removal_head_moved_refusal(
    workspace: &WorkspaceName,
    from: &GitOid,
    to: &GitOid,
) -> CowshedError {
    CowshedError::conflict(
        format!("workspace {workspace} HEAD changed from {from} to {to} during removal"),
        "review the new HEAD and retry removal",
    )
}

/// Removing main without `--restore` throws the project's warm image away for good, so the gate
/// points at the mode that recovers the pre-adoption checkout instead.
#[cfg(target_os = "macos")]
fn main_removal_mode_refusal() -> CowshedError {
    CowshedError::conflict(
        "removing main without --restore destroys this project's warm main image",
        "recover the pre-adoption checkout instead: cowshed rm main --restore",
    )
}

/// The repository a git-worktree workspace's sandbox must reach into, if it is one.
///
/// Narrowed to `.git`: the workspace needs main's object store and its own administrative
/// directory, and nothing in main's working tree.
#[cfg(target_os = "macos")]
fn git_worktree_repository(
    metadata: &crate::metadata::DetachedWorkspaceMetadata,
    main_mount: PathBuf,
) -> Option<PathBuf> {
    is_git_worktree(metadata).then(|| main_mount.join(".git"))
}

/// Re-aim a git-worktree workspace's registration at main's current mount, both directions.
///
/// `git worktree repair`, run from main, is the primitive for exactly this: it rewrites the
/// pointer file in the worktree and the `gitdir` file in main's administrative directory from
/// whichever of the two is still intact, which is what makes it correct whether main moved or the
/// workspace did.
#[cfg(target_os = "macos")]
async fn repair_git_worktree_link(main_mount: &Path, mount: &Path) -> Result<()> {
    crate::git::GitRepository::from_root(main_mount)
        .repair_linked_worktree(mount)
        .await
}

/// Refuse `checkpoint` and `restore` on a git-worktree workspace.
///
/// Its image is not self-contained: the tree is here and the history is in main. A checkpoint
/// clone would capture half a repository, and restoring one would resurrect a registration for a
/// worktree id main has since pruned, quietly claiming a branch another workspace may now hold.
/// The refusal lands before any quota read or barrier, because nothing about the workspace's size
/// or its supervisor changes the answer, and the hints name the two honest substitutes.
#[cfg(target_os = "macos")]
fn require_checkpointable(
    name: &WorkspaceName,
    metadata: &crate::metadata::DetachedWorkspaceMetadata,
    verb: &str,
) -> Result<()> {
    if !is_git_worktree(metadata) {
        return Ok(());
    }
    Err(CowshedError::conflict(
        format!(
            "git-worktree workspace {name} cannot {verb}: its history lives in main, not in its image"
        ),
        format!(
            "commit and cowshed land {name}, or cowshed new <name> for a checkpointable workspace"
        ),
    ))
}

/// Whether this workspace is a registered linked worktree of main's repository.
///
/// Read from the store-side sidecar, so it answers while the workspace is detached — which is
/// exactly when retirement and `gc` need it.
#[cfg(target_os = "macos")]
fn is_git_worktree(metadata: &crate::metadata::DetachedWorkspaceMetadata) -> bool {
    metadata.info_snapshot.git_worktree
}

#[cfg(target_os = "macos")]
fn another_process_is_running(workspace: &WorkspaceName) -> CowshedError {
    CowshedError::conflict(
        format!("another cowshed process is running a lifecycle operation on {workspace}"),
        "wait for that operation to finish, then retry",
    )
}

/// The clone intent an abandoned clone's own metadata implies: a fork when it records its
/// source, otherwise a create from main. A create `--from` a workspace records no source, so it
/// is retired as a create from main: the source only anchors the pending image's identity checks,
/// and the retirement's containment check against main still guards the clone's history.
#[cfg(target_os = "macos")]
fn abandoned_clone_origin(
    metadata: &crate::metadata::DetachedWorkspaceMetadata,
) -> crate::storage::recovery::LifecycleIntent {
    match &metadata.info_snapshot.forked_from {
        Some(source) => crate::storage::recovery::LifecycleIntent::Fork {
            source: source.clone(),
            destination: metadata.workspace.clone(),
        },
        None => crate::storage::recovery::LifecycleIntent::Create {
            workspace: metadata.workspace.clone(),
            options: CreateOptions {
                git_worktree: metadata.info_snapshot.git_worktree,
                ..CreateOptions::default()
            },
        },
    }
}

/// Whether a process holds `image`'s lifecycle lock right now — the lock a create, fork, restore
/// or retirement holds on the image for as long as it works on it. A probe that takes the lock
/// holds it until it is dropped, so it is fenced like every lease (`fork_lock`).
#[cfg(target_os = "macos")]
fn image_lifecycle_lock_is_held(image: &Path) -> Result<bool> {
    use std::os::unix::fs::OpenOptionsExt as _;

    let mut lock = image.as_os_str().to_owned();
    lock.push(".lock");
    let lock = PathBuf::from(lock);
    let file = match std::fs::OpenOptions::new()
        .read(true)
        .custom_flags(libc::O_NOFOLLOW)
        .open(&lock)
    {
        Ok(file) => crate::fork_lock::Fenced::new(file),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(CowshedError::environment_missing(
                format!("cannot open lifecycle lock {}: {error}", lock.display()),
                "check controller storage permissions and retry",
            ));
        }
    };
    match file.try_lock() {
        Ok(()) => Ok(false),
        Err(std::fs::TryLockError::WouldBlock) => Ok(true),
        Err(std::fs::TryLockError::Error(error)) => Err(CowshedError::environment_missing(
            format!("cannot probe lifecycle lock {}: {error}", lock.display()),
            "check controller storage permissions and retry",
        )),
    }
}

/// Split a mounted workspace marker into identity vs project-root findings.
///
/// Those are different conditions with different remedies: remounting replaces a stale volume,
/// but `projectRoot` lives inside the image and is rewritten by `attach`, not by unmounting.
#[cfg(target_os = "macos")]
fn diagnose_mounted_marker(
    workspace_name: &WorkspaceName,
    marker: &crate::metadata::WorkspaceMarker,
    expected_repos: &OwnedRepoIds,
    expected_workspace: &WorkspaceName,
    expected_incarnation: &crate::metadata::WorkspaceIncarnation,
    expected_project_root: &Path,
    marker_path: PathBuf,
) -> Vec<crate::api::dto::Finding> {
    let mut findings = Vec::new();
    // A marker naming an identity this project recorded as former is a stamp an identity change
    // could not reach, not a fault: the volume is this workspace's, and attach converges the stamp.
    // Reporting it as an error would leave a healthy renamed project permanently red.
    if !expected_repos.accepts(&marker.repo_id)
        || marker.workspace != *expected_workspace
        || marker.workspace_incarnation != *expected_incarnation
    {
        findings.push(crate::api::dto::Finding {
            code: "marker".into(),
            severity: crate::api::dto::FindingSeverity::Error,
            message: format!(
                "workspace {workspace_name} marker identity does not match the store: recorded {}/{}/{}",
                marker.repo_id, marker.workspace, marker.workspace_incarnation
            ),
            hint: format!(
                "cowshed detach {workspace_name} && cowshed attach {workspace_name}"
            ),
            path: Some(marker_path.clone()),
        });
    }
    if !names_one_root(&marker.project_root, expected_project_root) {
        findings.push(crate::api::dto::Finding {
            code: "project-root".into(),
            severity: crate::api::dto::FindingSeverity::Error,
            message: format!(
                "workspace {workspace_name} records project root {} but the checkout is {}",
                marker.project_root.display(),
                expected_project_root.display()
            ),
            hint: format!("cowshed attach {workspace_name}"),
            path: Some(marker_path),
        });
    }
    findings
}

#[cfg(target_os = "macos")]
fn merge_driver_finding(
    workspace_name: &WorkspaceName,
    driver: &crate::git::MergeDriver,
    path: PathBuf,
) -> Option<crate::api::dto::Finding> {
    match &driver.state {
        crate::git::MergeDriverState::Relative => None,
        crate::git::MergeDriverState::Relativized { to } => Some(crate::api::dto::Finding {
            code: "merge-driver".into(),
            severity: crate::api::dto::FindingSeverity::Error,
            message: format!(
                "workspace {workspace_name} merge driver {} uses an absolute program; the repository-relative spelling is {to}",
                driver.name
            ),
            hint: format!("cowshed attach {workspace_name}"),
            path: Some(path),
        }),
        crate::git::MergeDriverState::Unresolvable { program } => Some(crate::api::dto::Finding {
            code: "merge-driver".into(),
            severity: crate::api::dto::FindingSeverity::Error,
            message: format!(
                "workspace {workspace_name} merge driver {} names {program}, which is not in this repository; cowshed will not guess a replacement",
                driver.name
            ),
            hint: format!(
                "point merge.{}.driver at a program that exists in the repository, or remove the driver — cowshed attach cannot invent one",
                driver.name
            ),
            path: Some(path),
        }),
    }
}

/// The first-write cost at which main's extent map alone spends the budget `cowshed new` has for
/// its whole cold path (08_testing.md), and the rewrite starts paying for itself.
#[cfg(target_os = "macos")]
const CLONE_FIRST_WRITE_BUDGET: std::time::Duration = std::time::Duration::from_secs(1);

/// Main's extent count and the first-write cost each new clone of it is predicted to pay.
///
/// Below the budget the count is reported for the record, with nothing to do; at or above it the
/// finding warns and names the rewrite.
#[cfg(target_os = "macos")]
fn main_extents_finding(
    image: PathBuf,
    extents: crate::storage::lifecycle::ExtentCount,
) -> crate::api::dto::Finding {
    let cost = extents.first_write_cost();
    let costly = cost >= CLONE_FIRST_WRITE_BUDGET;
    crate::api::dto::Finding {
        code: "main-extents".into(),
        severity: if costly {
            crate::api::dto::FindingSeverity::Warning
        } else {
            crate::api::dto::FindingSeverity::Info
        },
        message: format!(
            "main's image has {extents} extents; the first write into each new clone copies that map, predicted to take {cost:.1?}"
        ),
        hint: if costly {
            "cowshed defrag main".into()
        } else {
            String::new()
        },
        path: Some(image),
    }
}

/// Main's extents could not be read, so the clone cost is unknown rather than fine.
#[cfg(target_os = "macos")]
fn main_extents_unread(image: PathBuf, error: String) -> crate::api::dto::Finding {
    crate::api::dto::Finding {
        code: "main-extents".into(),
        severity: crate::api::dto::FindingSeverity::Warning,
        message: format!(
            "could not count main's image extents, so the first-write cost of a new clone is unknown: {error}"
        ),
        hint: "check that the store volume is readable, then rerun cowshed doctor".into(),
        path: Some(image),
    }
}

#[cfg(target_os = "macos")]
fn cowshed_upstream_finding(
    workspace_name: &WorkspaceName,
    upstream: &crate::git::CowshedUpstream,
    path: PathBuf,
) -> Option<crate::api::dto::Finding> {
    if upstream.repository {
        return None;
    }
    let location = upstream
        .url
        .as_ref()
        .map(|url| format!(" ({})", url.display()))
        .unwrap_or_default();
    Some(crate::api::dto::Finding {
        code: "main-remote".into(),
        severity: crate::api::dto::FindingSeverity::Error,
        message: format!(
            "workspace {workspace_name} remote {} does not resolve to a repository{location}",
            upstream.remote_name
        ),
        hint: format!("cowshed attach {workspace_name}"),
        path: Some(path),
    })
}

/// Decide which path a checkout-identity check should actually inspect.
///
/// Exactly one symlink is legitimate at a checkout path: the one adoption plants there, aimed at
/// main's own mount. It is accepted only when it resolves to precisely that mount, and the
/// identity checks then run against the resolved target. Every other symlink is an unrelated path
/// standing where the checkout belongs, and stays a conflict — the check narrows, never inverts.
#[cfg(target_os = "macos")]
async fn resolve_checkout_identity_path(
    path: &Path,
    path_metadata: &std::fs::Metadata,
    main_mount: &Path,
    description: &str,
) -> Result<PathBuf> {
    if path_metadata.file_type().is_symlink() {
        let target = tokio::fs::canonicalize(path).await.map_err(|_| {
            CowshedError::conflict(
                format!("{description} is a symlink that does not resolve"),
                "restore the exact .pre-cowshed tree or move the collision aside",
            )
        })?;
        let canonical_main = tokio::fs::canonicalize(main_mount).await.map_err(|_| {
            CowshedError::conflict(
                format!("{description} cannot be compared against main's mount"),
                "restore the exact .pre-cowshed tree or move the collision aside",
            )
        })?;
        if target != canonical_main {
            return Err(CowshedError::conflict(
                format!("{description} is a symlink to something other than main's mount"),
                "move the unrelated symlink aside and retry",
            ));
        }
        return Ok(target);
    }
    if path_metadata.file_type().is_dir() {
        return Ok(path.to_owned());
    }
    Err(CowshedError::conflict(
        format!("{description} is not the exact retained checkout directory"),
        "restore the exact .pre-cowshed tree or move the collision aside",
    ))
}

#[cfg(target_os = "macos")]
fn native_finding(
    code: &str,
    severity: crate::api::dto::FindingSeverity,
    error: CowshedError,
) -> crate::api::dto::Finding {
    crate::api::dto::Finding {
        code: code.into(),
        severity,
        message: error.message,
        hint: error.hint,
        path: None,
    }
}

/// Every build-state path of the checkout at `checkout` that is not what a refresh leaves it,
/// against the volume its build link names; none for a checkout that links no volume. Reads only.
#[cfg(target_os = "macos")]
fn unsettled_build_links(
    checkout: &Path,
) -> Result<Vec<(PathBuf, crate::build_volume::link::Unsettled)>> {
    use crate::build_volume::{BuildVolumeState, link};
    let Some(volume) = link::linked(checkout)? else {
        return Ok(Vec::new());
    };
    let state = BuildVolumeState::read(&volume)?;
    Ok(link::unsettled(checkout, &volume, &state.paths))
}

/// Doctor's report of a build-state path that is not its link onto the workspace's build volume
/// (16_build_volumes.md, "One link per checkout"). A tool that removes the link -- `nx reset`, an
/// `rm -rf` of the path -- leaves its next run to make a real directory there, off the volume:
/// nothing it writes reaches a fork, a land or the volume's carry until a refresh relinks it.
#[cfg(target_os = "macos")]
fn build_link_finding(
    workspace: &WorkspaceName,
    checkout: &Path,
    path: PathBuf,
    unsettled: crate::build_volume::link::Unsettled,
) -> crate::api::dto::Finding {
    crate::api::dto::Finding {
        code: "build-link".into(),
        severity: crate::api::dto::FindingSeverity::Warning,
        message: format!(
            "{workspace}'s {} {unsettled}: what its tools write there misses the build volume, so \
             no fork, land or seed of {workspace} gets it",
            path.display()
        ),
        hint: format!(
            "cowshed exec {workspace} -- true relinks it (the refresh before every exec discards \
             what is there; `cowshed setup` refreshes every workspace)"
        ),
        path: Some(checkout.join(path)),
    }
}

/// Doctor's report of a target whose seed is behind its live build volume
/// (16_build_volumes.md, "Targets and seeds"); a seed that holds every write is no finding.
#[cfg(target_os = "macos")]
fn seed_age_finding(
    workspace: &WorkspaceName,
    age: &super::build_volumes::SeedAge,
    checkout: PathBuf,
) -> Option<crate::api::dto::Finding> {
    use crate::storage::deletion_log::rfc3339_utc;
    if !age.stale() {
        return None;
    }
    let hint = format!(
        "cowshed reseed {workspace} refreezes it now; every fork of {workspace} does the same \
         first, unless an Nx run or a Cargo build is writing the volume"
    );
    Some(match &age.seed {
        Some((seed, frozen)) => crate::api::dto::Finding {
            code: "seed-age".into(),
            severity: crate::api::dto::FindingSeverity::Info,
            message: format!(
                "{workspace}'s seed {seed} holds its build volume {} as written at {}; the volume \
                 was last written {:.1} s later, at {}, and a fork cloning the seed now misses \
                 what ran in {workspace} since",
                age.live,
                rfc3339_utc(*frozen),
                age.behind().unwrap_or_default().as_secs_f64(),
                rfc3339_utc(age.written),
            ),
            hint,
            path: Some(checkout),
        },
        None => crate::api::dto::Finding {
            code: "seed-age".into(),
            severity: crate::api::dto::FindingSeverity::Warning,
            message: format!(
                "{workspace} links build volume {} and has no seed: a fork of {workspace} freezes \
                 one first when nothing writes the volume, and refuses otherwise",
                age.live
            ),
            hint,
            path: Some(checkout),
        },
    })
}

/// A stopped supervisor is not proof that every process it started has gone. Recovery may keep
/// groups whose leader it cannot identify. Ordinary admission remains safe, but replacing or
/// destroying their image would cut the substrate out from under an unaccounted-for writer.
#[cfg(target_os = "macos")]
async fn require_lost_groups_released(
    workspace: &WorkspaceName,
    socket: &super::supervisor_socket::BoundSocket,
    operation: &str,
) -> Result<()> {
    let ledger = super::job_groups::ledger_path(socket.path());
    let read = ledger.clone();
    let groups = crate::storage::lifecycle::dispatch_blocking(move || {
        super::job_groups::take_lost(&read, LOST_JOB_GRACE)
    })
    .await
    .map_err(|error| {
        CowshedError::internal(format!(
            "inspecting stopped workspace {workspace}'s groups: {error}"
        ))
    })?
    .map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "cannot {operation} workspace {workspace}: its stopped supervisor's process-group \
                 ledger {} cannot be reconciled: {error}",
                ledger.display()
            ),
            "cowshed doctor --json",
        )
    })?;
    if let Some(group) = groups.first() {
        return Err(CowshedError::conflict(
            format!(
                "cannot {operation} workspace {workspace}: {} unresolved process group(s) remain; \
                 job {} group {} from lost supervisor {} may still use its image, retained in {}",
                groups.len(),
                group.job_id(),
                group.pgid(),
                group.lost_supervisor(),
                ledger.display()
            ),
            format!(
                "cowshed doctor --json; inspect `ps -g {}` and retry only after the group releases \
                 the image; do not force-detach or remove its ledger",
                group.pgid()
            ),
        ));
    }
    Ok(())
}

#[cfg(all(test, target_os = "macos"))]
mod unresolved_retirement_tests {
    use super::*;
    use crate::fork_lock::Spawn as _;
    use std::io::{BufRead as _, Read as _, Write as _};
    use std::os::unix::process::CommandExt as _;
    use std::time::Duration;

    #[tokio::test]
    async fn destructive_changes_retain_a_leaderless_writer_until_it_releases_naturally() {
        // Unix socket addresses need their own short namespace, independent of the host's
        // potentially long TMPDIR. The owned fixture still carries its ledger and real writer.
        let root = PathBuf::from("/tmp").join(format!(
            "cowshed-unresolved-{}",
            &uuid::Uuid::new_v4().simple().to_string()[..12]
        ));
        std::fs::create_dir(&root).unwrap();
        let ledger = root.join("supervisor.groups");
        let workspace = WorkspaceName::new("retained").unwrap();
        let mut child = std::process::Command::new("/bin/sh")
            .args([
                "-c",
                "exec 3<&0; (printf 'READY\\n'; cat <&3; printf 'RELEASED\\n') & exit 0",
            ])
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .unwrap();
        let birth = super::super::job_groups::GroupLeader::observe(child.id()).unwrap();
        super::super::job_groups::record(&ledger, &[(9, birth)], &[]).unwrap();
        let mut release = child.stdin.take().unwrap();
        let mut output = std::io::BufReader::new(child.stdout.take().unwrap());
        let mut ready = String::new();
        output.read_line(&mut ready).unwrap();
        assert_eq!(ready, "READY\n");
        assert!(child.wait().unwrap().success());
        let stopped = super::super::supervisor_socket::bind(&root.join("supervisor.sock"))
            .await
            .unwrap();

        for operation in ["remove", "restore", "restore main"] {
            let error = require_lost_groups_released(&workspace, &stopped, operation)
                .await
                .unwrap_err();
            assert_eq!(error.code, ErrorCode::Conflict);
            assert!(
                error
                    .message
                    .contains(&format!("job 9 group {}", birth.pgid()))
            );
            let unresolved = super::super::job_groups::unresolved(&ledger).unwrap();
            assert_eq!(unresolved.len(), 1);
            assert_eq!(unresolved[0].job_id(), 9);
            assert_eq!(unresolved[0].pgid(), birth.pgid());
        }
        release.write_all(b"the writer is still alive\n").unwrap();
        drop(release);
        let mut tail = String::new();
        output.read_to_string(&mut tail).unwrap();
        assert_eq!(tail, "the writer is still alive\nRELEASED\n");
        let deadline = std::time::Instant::now() + Duration::from_secs(1);
        while !super::super::job_groups::unresolved(&ledger)
            .unwrap()
            .is_empty()
        {
            assert!(
                std::time::Instant::now() < deadline,
                "the writer did not release its group"
            );
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        require_lost_groups_released(&workspace, &stopped, "remove")
            .await
            .unwrap();
        assert!(
            !ledger.exists(),
            "released group evidence is reconciled before retirement"
        );
        drop(stopped);
        std::fs::remove_dir_all(root).unwrap();
    }
}

/// The groups a lost supervisor left that processes still hold although their leader is gone,
/// observed now from the workspace's ledger ([`super::job_groups::UnresolvedGroup`]). Read-only:
/// the workspace's next ledger writer drops the ones nothing holds any more.
#[cfg(target_os = "macos")]
async fn unresolved_group_findings(
    workspace: &WorkspaceName,
    ledger: PathBuf,
) -> Vec<crate::api::dto::Finding> {
    let read = ledger.clone();
    let observed =
        crate::storage::lifecycle::dispatch_blocking(move || super::job_groups::unresolved(&read))
            .await;
    match observed {
        Ok(Ok(groups)) => groups
            .into_iter()
            .map(|group| crate::api::dto::Finding {
                code: "unresolved-job-group".into(),
                severity: crate::api::dto::FindingSeverity::Warning,
                message: format!(
                    "workspace {workspace}: job {} process group {} still has processes, but the \
                     supervisor that ran it (process {}) was lost and {}; they cannot be told \
                     from another group that took the id, so cowshed has not signalled them",
                    group.job_id(),
                    group.pgid(),
                    group.lost_supervisor(),
                    match group.reason() {
                        super::job_groups::UnresolvedReason::LeaderGone =>
                            "the group's recorded leader has been reaped",
                        super::job_groups::UnresolvedReason::LeaderNeverIdentified =>
                            "an earlier cowshed recorded the group without its leader's identity",
                    }
                ),
                hint: format!(
                    "inspect them with `ps -g {}`; cowshed drops this record once nothing holds \
                     the group id",
                    group.pgid()
                ),
                path: Some(ledger.clone()),
            })
            .collect(),
        Ok(Err(error)) => vec![crate::api::dto::Finding {
            code: "unresolved-job-group".into(),
            severity: crate::api::dto::FindingSeverity::Error,
            message: format!(
                "workspace {workspace}: the job process group ledger {} cannot be inspected: \
                 {error}",
                ledger.display()
            ),
            hint: "cowshed doctor --json".into(),
            path: Some(ledger),
        }],
        Err(error) => vec![crate::api::dto::Finding {
            code: "unresolved-job-group".into(),
            severity: crate::api::dto::FindingSeverity::Error,
            message: format!(
                "workspace {workspace}: inspecting the job process group ledger {} failed: {error}",
                ledger.display()
            ),
            hint: "cowshed doctor --json".into(),
            path: Some(ledger),
        }],
    }
}

/// The transport move an inspecting open left unrecorded: each remote whose recorded URL Git no
/// longer uses, and the URL it uses now.
#[cfg(target_os = "macos")]
fn binding_move_finding(
    recorded: &RepositoryBinding,
    moved: &RepositoryBinding,
    binding: PathBuf,
) -> crate::api::dto::Finding {
    let moves = recorded
        .identities
        .iter()
        .zip(&moved.identities)
        .filter_map(|(before, after)| {
            match (&before.remote_name, &before.remote_url, &after.remote_url) {
                (Some(name), Some(from), Some(to)) if from != to => {
                    Some(format!("{name}: {from} -> {to}"))
                }
                _ => None,
            }
        })
        .collect::<Vec<_>>()
        .join(", ");
    crate::api::dto::Finding {
        code: "binding-moved".into(),
        severity: crate::api::dto::FindingSeverity::Info,
        message: format!("the project binding records remote URLs Git no longer uses ({moves})"),
        hint: "any cowshed command other than doctor records the URLs Git uses as it opens".into(),
        path: Some(binding),
    }
}

/// A lifecycle operation journaled and not completed: either its process is still running it,
/// or it died and the next opening that may finish it does.
#[cfg(target_os = "macos")]
fn unfinished_intent_finding(
    workspace: &WorkspaceName,
    verb: &str,
    journal: PathBuf,
) -> crate::api::dto::Finding {
    crate::api::dto::Finding {
        code: "unfinished-intent".into(),
        severity: crate::api::dto::FindingSeverity::Warning,
        message: format!(
            "{verb} of {workspace} is journaled and not finished: its process is still running it, \
             or it died"
        ),
        hint: if workspace.is_main() {
            "any cowshed command other than doctor finishes it as it opens".to_owned()
        } else {
            format!(
                "the next cowshed command naming {workspace} finishes it, and so does cowshed gc"
            )
        },
        path: Some(journal),
    }
}

#[cfg(target_os = "macos")]
fn orphan_session_image_findings(
    candidates: &[crate::storage::lifecycle::StorageGcCandidate],
) -> impl Iterator<Item = crate::api::dto::Finding> + '_ {
    candidates
        .iter()
        .filter(|candidate| {
            candidate.reason() == crate::storage::lifecycle::StorageGcReason::OrphanSessionImage
        })
        .map(|candidate| crate::api::dto::Finding {
            code: "session-orphan".into(),
            severity: crate::api::dto::FindingSeverity::Warning,
            message: format!(
                "session image {} has no grants metadata",
                candidate.path().display()
            ),
            hint: "cowshed gc".into(),
            path: Some(candidate.path().to_owned()),
        })
}

/// Blocking quarantine-and-companion scan for `doctor`: quarantine tombstones first,
/// then live images whose sidecar survived without their CA companion.
///
/// Tombstones this reader does not understand are skipped, not reported: the schema is the
/// quarantine writer's to evolve, and `doctor` must not misreport a future record. A
/// tombstone whose workspace name no longer parses is skipped for the same reason — the
/// rekey hint needs the exact name.
#[cfg(target_os = "macos")]
fn quarantine_and_companion_findings_blocking(
    store_root: &Path,
    repo_id: &RepoId,
    images: &[(WorkspaceName, PathBuf)],
) -> Vec<crate::api::dto::Finding> {
    let mut findings = Vec::new();
    if let Ok(layout) = crate::storage::StorageLayout::new(store_root, repo_id) {
        let quarantine_root = layout
            .project()
            .project_root
            .join(crate::repository::QUARANTINE_DIRECTORY);
        match std::fs::read_dir(&quarantine_root) {
            Ok(entries) => {
                for entry in entries.flatten() {
                    let tombstone_path = entry.path().join("tombstone.json");
                    if !tombstone_path.is_file() {
                        continue;
                    }
                    let Ok(tombstone) = crate::metadata::read_json::<
                        crate::storage::apfs::native::QuarantineTombstone,
                    >(&tombstone_path) else {
                        continue;
                    };
                    if tombstone.version != 1 {
                        continue;
                    }
                    let Ok(workspace) = WorkspaceName::new(&tombstone.workspace) else {
                        continue;
                    };
                    findings.push(quarantined_workspace_finding(
                        &workspace,
                        &tombstone.reason,
                        tombstone_path,
                    ));
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => findings.push(crate::api::dto::Finding {
                code: "quarantine".into(),
                severity: crate::api::dto::FindingSeverity::Error,
                message: format!(
                    "could not inspect quarantine directory {}: {error}",
                    quarantine_root.display()
                ),
                hint: "inspect the store".into(),
                path: Some(quarantine_root.clone()),
            }),
        }
    }
    for (workspace, image) in images {
        let sidecar = crate::metadata::sidecar_path(image);
        // The companion rides beside its image with a `.ca.key` suffix (the store's companion
        // convention); a live image whose sidecar survived without one can no longer pass
        // CA-gated verbs.
        let companion = crate::metadata::append_suffix(image, ".ca.key");
        if image.is_file() && sidecar.is_file() && !companion.exists() {
            findings.push(missing_ca_companion_finding(
                workspace,
                "published canonical image",
                image,
                &companion,
            ));
        }
    }
    findings
}

/// A quarantined workspace, as `doctor` reports it.
///
/// Warning, not error: the workspace's bytes are accounted for — the tombstone says where —
/// and the condition needs an operator decision rather than a retry. The message names the
/// hold on the record, not sickness in the data: quarantine never touches the image, so the
/// data is intact by construction. The hint names the repair: `rekey` consumes the tombstone
/// and rebuilds the companion, so a quarantined workspace points at its own fix.
#[cfg(target_os = "macos")]
fn quarantined_workspace_finding(
    workspace: &WorkspaceName,
    reason: &str,
    tombstone: PathBuf,
) -> crate::api::dto::Finding {
    crate::api::dto::Finding {
        code: "workspace-quarantined".into(),
        severity: crate::api::dto::FindingSeverity::Warning,
        message: format!(
            "workspace {workspace} held: {reason} (data intact): {}",
            tombstone.display()
        ),
        hint: format!("cowshed rekey {workspace}"),
        path: Some(tombstone),
    }
}

/// A workspace whose image lost its CA companion, as `doctor` reports it.
///
/// Error: the workspace's CA-gated verbs refuse until the companion is rebuilt, and only
/// `rekey` rebuilds it — `doctor` merely observes, so the hint must not name it.
#[cfg(target_os = "macos")]
fn missing_ca_companion_finding(
    workspace: &WorkspaceName,
    layout: &str,
    image: &Path,
    companion: &Path,
) -> crate::api::dto::Finding {
    crate::api::dto::Finding {
        code: "ca-companion-missing".into(),
        severity: crate::api::dto::FindingSeverity::Error,
        message: format!(
            "workspace {workspace} {layout} is missing its CA companion: image={}, companion={}",
            image.display(),
            companion.display()
        ),
        hint: format!("cowshed rekey {workspace}"),
        path: Some(image.to_owned()),
    }
}

#[cfg(all(test, target_os = "macos"))]
mod doctor_hint_tests {
    use super::*;
    use crate::git::{CowshedUpstream, MergeDriver, MergeDriverState};
    use crate::metadata::{MARKER_VERSION, WorkspaceIncarnation, WorkspaceMarker, WorkspaceRole};
    use std::path::PathBuf;

    fn marker(project_root: &str, workspace: &str) -> WorkspaceMarker {
        WorkspaceMarker {
            version: MARKER_VERSION,
            repo_id: crate::repository::RepoId::parse("acme/widget").expect("repo"),
            project_root: PathBuf::from(project_root),
            workspace: WorkspaceName::new(workspace).expect("workspace"),
            workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
                .expect("incarnation"),
            role: WorkspaceRole::Workspace,
            base_commit: "8f31c2d".into(),
            created_at: "2026-07-11T12:00:00Z".into(),
            forked_from: None,
            created_trace: "trace".into(),
            lineage: None,
        }
    }

    fn expected() -> (
        crate::repository::RepoId,
        WorkspaceName,
        WorkspaceIncarnation,
    ) {
        (
            crate::repository::RepoId::parse("acme/widget").expect("repo"),
            WorkspaceName::new("raven").expect("workspace"),
            WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").expect("incarnation"),
        )
    }

    #[test]
    fn stale_project_root_is_its_own_finding_and_hints_attach_without_detach() {
        let (repo, workspace, incarnation) = expected();
        let recorded = PathBuf::from("/tmp/recorded-checkout");
        let actual = PathBuf::from("/tmp/actual-checkout");
        let findings = diagnose_mounted_marker(
            &workspace,
            &marker("/tmp/recorded-checkout", "raven"),
            &OwnedRepoIds::sole(repo.clone()),
            &workspace,
            &incarnation,
            &actual,
            PathBuf::from("/mnt/raven/.cowshed/workspace.json"),
        );
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].code, "project-root");
        assert_eq!(
            findings[0].severity,
            crate::api::dto::FindingSeverity::Error
        );
        assert!(
            findings[0].message.contains(recorded.to_str().unwrap()),
            "{}",
            findings[0].message
        );
        assert!(
            findings[0].message.contains(actual.to_str().unwrap()),
            "{}",
            findings[0].message
        );
        assert_eq!(findings[0].hint, "cowshed attach raven");
        assert!(
            !findings[0].hint.contains("detach"),
            "detach cannot rewrite projectRoot inside the image; hint was {}",
            findings[0].hint
        );
    }

    #[test]
    fn matching_project_root_is_not_a_finding() {
        let (repo, workspace, incarnation) = expected();
        let root = PathBuf::from("/tmp/same-checkout");
        let findings = diagnose_mounted_marker(
            &workspace,
            &marker("/tmp/same-checkout", "raven"),
            &OwnedRepoIds::sole(repo.clone()),
            &workspace,
            &incarnation,
            &root,
            PathBuf::from("/mnt/raven/.cowshed/workspace.json"),
        );
        assert!(findings.is_empty(), "{findings:?}");
    }

    #[test]
    fn marker_identity_mismatch_still_hints_remount() {
        let (repo, workspace, incarnation) = expected();
        let root = PathBuf::from("/tmp/same-checkout");
        let findings = diagnose_mounted_marker(
            &workspace,
            &marker("/tmp/same-checkout", "other"),
            &OwnedRepoIds::sole(repo.clone()),
            &workspace,
            &incarnation,
            &root,
            PathBuf::from("/mnt/raven/.cowshed/workspace.json"),
        );
        assert_eq!(findings.len(), 1, "{findings:?}");
        assert_eq!(findings[0].code, "marker");
        assert!(
            findings[0].message.contains("acme/widget/other/"),
            "{}",
            findings[0].message
        );
        assert_eq!(
            findings[0].hint,
            "cowshed detach raven && cowshed attach raven"
        );
    }

    #[test]
    fn relativized_merge_driver_hints_attach_and_unresolvable_requires_a_decision() {
        let workspace = WorkspaceName::new("raven").expect("workspace");
        let relativized = merge_driver_finding(
            &workspace,
            &MergeDriver {
                name: "ledger-union".into(),
                state: MergeDriverState::Relativized {
                    to: "scripts/merge-ledger.py %O %A %B".into(),
                },
            },
            PathBuf::from("/mnt/raven"),
        )
        .expect("relativized is a finding");
        assert_eq!(relativized.code, "merge-driver");
        assert_eq!(relativized.hint, "cowshed attach raven");
        assert!(relativized.message.contains("scripts/merge-ledger.py"));

        let unresolvable = merge_driver_finding(
            &workspace,
            &MergeDriver {
                name: "appenddoc-union".into(),
                state: MergeDriverState::Unresolvable {
                    program: "/gone/scripts/merge-append-doc.py".into(),
                },
            },
            PathBuf::from("/mnt/raven"),
        )
        .expect("unresolvable is a finding");
        assert_eq!(unresolvable.code, "merge-driver");
        assert!(
            !unresolvable.hint.starts_with("cowshed attach"),
            "attach cannot invent a missing program; hint was {}",
            unresolvable.hint
        );
        assert!(unresolvable.hint.contains("merge.appenddoc-union.driver"));
        assert!(
            unresolvable
                .message
                .contains("/gone/scripts/merge-append-doc.py")
        );

        assert!(
            merge_driver_finding(
                &workspace,
                &MergeDriver {
                    name: "already-relative".into(),
                    state: MergeDriverState::Relative,
                },
                PathBuf::from("/mnt/raven"),
            )
            .is_none()
        );
    }

    #[test]
    fn every_orphan_session_image_gets_its_own_named_doctor_finding() {
        use crate::storage::lifecycle::{StorageGcCandidate, StorageGcReason};

        let first = PathBuf::from("/store/acme/widget/sessions/ghost.asif");
        let second = PathBuf::from("/store/acme/widget/sessions/scratch.sparseimage");
        let candidates = [
            StorageGcCandidate::new(
                [1; 32],
                first.clone(),
                17,
                StorageGcReason::OrphanSessionImage,
            ),
            StorageGcCandidate::new(
                [2; 32],
                PathBuf::from("/store/acme/widget/.staging/orphan.asif"),
                100,
                StorageGcReason::OrphanStagingImage,
            ),
            StorageGcCandidate::new(
                [3; 32],
                second.clone(),
                23,
                StorageGcReason::OrphanSessionImage,
            ),
        ];
        let findings = orphan_session_image_findings(&candidates).collect::<Vec<_>>();
        assert_eq!(
            findings
                .iter()
                .map(|finding| finding.path.clone())
                .collect::<Vec<_>>(),
            [Some(first.clone()), Some(second.clone())]
        );
        assert!(findings.iter().all(|finding| {
            finding.code == "session-orphan"
                && finding.severity == crate::api::dto::FindingSeverity::Warning
                && finding.hint == "cowshed gc"
                && finding
                    .path
                    .as_ref()
                    .is_some_and(|path| finding.message.contains(&path.display().to_string()))
        }));
    }

    #[test]
    fn main_extents_warn_with_the_rewrite_once_a_clone_first_write_spends_the_new_budget() {
        use crate::api::dto::FindingSeverity;
        use crate::storage::lifecycle::ExtentCount;

        let image = PathBuf::from("/store/acme/widget/main.asif");
        // 12 µs per extent: 83,333 extents copy in just under a second, 83,334 in just over.
        let under = main_extents_finding(image.clone(), ExtentCount::new(83_333));
        assert_eq!(under.code, "main-extents");
        assert_eq!(under.severity, FindingSeverity::Info);
        assert_eq!(under.hint, "");
        assert!(under.message.contains("83333 extents"), "{}", under.message);

        let over = main_extents_finding(image.clone(), ExtentCount::new(83_334));
        assert_eq!(over.severity, FindingSeverity::Warning);
        assert_eq!(over.hint, "cowshed defrag main");
        assert_eq!(over.path, Some(image.clone()));

        let measured = main_extents_finding(image, ExtentCount::new(2_110_000));
        assert!(
            measured.message.contains("25.3s"),
            "the 2.11M-extent main that took 25.7 s predicts about that: {}",
            measured.message
        );
    }

    #[test]
    fn dead_cowshed_upstream_hints_attach() {
        let workspace = WorkspaceName::new("raven").expect("workspace");
        let finding = cowshed_upstream_finding(
            &workspace,
            &CowshedUpstream {
                remote_name: "main".into(),
                url: Some(PathBuf::from("/tmp/gone-checkout")),
                repository: false,
            },
            PathBuf::from("/mnt/raven"),
        )
        .expect("dead upstream is a finding");
        assert_eq!(finding.code, "main-remote");
        assert!(finding.message.contains("/tmp/gone-checkout"));
        assert_eq!(finding.hint, "cowshed attach raven");
        assert!(
            cowshed_upstream_finding(
                &workspace,
                &CowshedUpstream {
                    remote_name: "main".into(),
                    url: Some(PathBuf::from("/tmp/live")),
                    repository: true,
                },
                PathBuf::from("/mnt/raven"),
            )
            .is_none()
        );
    }
    #[test]
    fn quarantined_workspace_is_a_warning_pointing_at_the_tombstone() {
        let workspace = WorkspaceName::new("raven").expect("workspace");
        let tombstone =
            PathBuf::from("/store/acme/widget/quarantine/raven-1754000000/tombstone.json");
        let finding =
            quarantined_workspace_finding(&workspace, "CA companion missing", tombstone.clone());
        assert_eq!(finding.code, "workspace-quarantined");
        assert_eq!(finding.severity, crate::api::dto::FindingSeverity::Warning);
        assert!(finding.message.contains("raven"), "{}", finding.message);
        assert!(
            finding.message.contains("CA companion missing"),
            "{}",
            finding.message
        );
        assert!(
            finding.message.contains("data intact"),
            "the hold is on the record, not sickness in the data: {}",
            finding.message
        );
        assert!(
            finding.message.contains(tombstone.to_str().unwrap()),
            "{}",
            finding.message
        );
        assert_eq!(finding.path, Some(tombstone));
        assert!(
            finding.hint.contains("cowshed rekey"),
            "hint was {}",
            finding.hint
        );
        assert!(
            !finding.hint.contains("doctor"),
            "doctor never repairs a quarantine; hint was {}",
            finding.hint
        );
    }

    #[test]
    fn missing_ca_companion_is_an_error_hinting_rekey_not_doctor() {
        let workspace = WorkspaceName::new("raven").expect("workspace");
        let image = PathBuf::from("/store/acme/widget/raven.asif");
        let companion = PathBuf::from("/store/acme/widget/raven.asif.ca.key");
        let finding = missing_ca_companion_finding(&workspace, "canonical", &image, &companion);
        assert_eq!(finding.code, "ca-companion-missing");
        assert_eq!(finding.severity, crate::api::dto::FindingSeverity::Error);
        assert!(
            finding.message.contains(image.to_str().unwrap()),
            "{}",
            finding.message
        );
        assert!(
            finding.message.contains(companion.to_str().unwrap()),
            "{}",
            finding.message
        );
        assert_eq!(finding.hint, "cowshed rekey raven");
        assert!(
            !finding.hint.contains("doctor"),
            "rekey owns this condition, not doctor; hint was {}",
            finding.hint
        );
    }

    #[test]
    fn missing_ca_companion_error_names_both_paths_and_hints_rekey() {
        let image = PathBuf::from("/store/acme/widget/raven.asif");
        let companion = PathBuf::from("/store/acme/widget/raven.asif.ca.key");
        let rendered =
            native_storage_error(crate::storage::apfs::ApfsStorageError::MissingCaCompanion {
                layout: "canonical",
                image: image.clone(),
                companion: companion.clone(),
            });
        assert_eq!(rendered.code, ErrorCode::Integrity);
        assert!(
            rendered.message.contains(image.to_str().unwrap()),
            "{}",
            rendered.message
        );
        assert!(
            rendered.message.contains(companion.to_str().unwrap()),
            "{}",
            rendered.message
        );
        assert_eq!(rendered.hint, "cowshed rekey raven");
        assert!(
            !rendered.hint.contains("doctor"),
            "rekey owns this condition, not doctor; hint was {}",
            rendered.hint
        );
    }

    #[test]
    fn quarantined_error_points_at_the_tombstone_instead_of_doctor() {
        let tombstone =
            PathBuf::from("/store/acme/widget/quarantine/raven-1754000000/tombstone.json");
        let rendered = native_storage_error(crate::storage::apfs::ApfsStorageError::Quarantined {
            workspace: WorkspaceName::new("raven").expect("workspace"),
            reason: "CA companion missing".into(),
            tombstone: tombstone.clone(),
        });
        assert_eq!(rendered.code, ErrorCode::Integrity);
        assert!(rendered.message.contains("raven"), "{}", rendered.message);
        assert!(
            rendered.message.contains("CA companion missing"),
            "{}",
            rendered.message
        );
        assert!(
            rendered.message.contains(tombstone.to_str().unwrap()),
            "{}",
            rendered.message
        );
        assert!(
            rendered.hint.contains("cowshed rekey"),
            "hint was {}",
            rendered.hint
        );
        assert!(
            !rendered.hint.contains("doctor"),
            "doctor never repairs a quarantine; hint was {}",
            rendered.hint
        );
    }

    #[test]
    fn missing_companion_hint_names_the_workspace_from_the_image_stem() {
        let rendered =
            native_storage_error(crate::storage::apfs::ApfsStorageError::MissingCaCompanion {
                layout: "canonical",
                image: PathBuf::from("/store/acme/widget/cargo-wasmbench.asif"),
                companion: PathBuf::from("/store/acme/widget/cargo-wasmbench.asif.ca.key"),
            });
        assert_eq!(rendered.code, ErrorCode::Integrity);
        assert_eq!(rendered.hint, "cowshed rekey cargo-wasmbench");
    }

    /// The quarantine scan reports v1 tombstones and companion-less live images, and stays
    /// silent when there is nothing held: `doctor` lists what the store holds, not what it
    /// looked for.
    #[test]
    fn quarantine_scan_reports_tombstones_and_companion_less_images() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-doctor-quarantine-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let store = root.join("store");
        std::fs::create_dir_all(&store).expect("store dir");
        let repo = crate::repository::RepoId::parse("acme/widget").expect("repo");
        let layout = crate::storage::StorageLayout::new(&store, &repo).expect("layout");
        let quarantine = layout
            .project()
            .project_root
            .join(crate::repository::QUARANTINE_DIRECTORY);
        std::fs::create_dir_all(&quarantine).expect("quarantine dir");

        // A held workspace: the tombstone names it, with no live files the scan could
        // mistake for a session.
        let held = quarantine.join("raven-1754000000");
        std::fs::create_dir_all(&held).expect("quarantine entry");
        let tombstone = held.join("tombstone.json");
        std::fs::write(
            &tombstone,
            serde_json::json!({
                "version": 1,
                "repoId": "acme/widget",
                "workspace": "raven",
                "incarnation": "0198f2c0b7e34dc795f17b238b331c80",
                "revision": 3,
                "reason": "missing-ca-companion",
                "image": "/store/acme/widget/raven.asif",
                "companion": "/store/acme/widget/raven.asif.ca.key",
                "quarantinedAt": "2026-09-04T00:00:00Z",
                "sidecar": "/store/acme/widget/quarantine/raven-1754000000/sidecar.json",
            })
            .to_string(),
        )
        .expect("tombstone");

        // A future schema version is skipped, not misreported.
        let future = quarantine.join("kestrel-1754000001");
        std::fs::create_dir_all(&future).expect("future entry");
        std::fs::write(
            future.join("tombstone.json"),
            serde_json::json!({
                "version": 2,
                "repoId": "acme/widget",
                "workspace": "kestrel",
                "incarnation": "0198f2c0b7e34dc795f17b238b331c80",
                "revision": 3,
                "reason": "missing-ca-companion",
                "image": "/store/acme/widget/kestrel.asif",
                "companion": "/store/acme/widget/kestrel.asif.ca.key",
                "quarantinedAt": "2026-09-04T00:00:00Z",
                "sidecar": "/store/acme/widget/quarantine/kestrel-1754000001/sidecar.json",
            })
            .to_string(),
        )
        .expect("future tombstone");

        // Live images: kestrel's sidecar survived without its companion; falcon is whole.
        let live = root.join("live");
        std::fs::create_dir_all(&live).expect("live dir");
        let bare_image = live.join("kestrel.asif");
        std::fs::write(&bare_image, b"image").expect("bare image");
        std::fs::write(crate::metadata::sidecar_path(&bare_image), b"sidecar")
            .expect("bare sidecar");
        let whole_image = live.join("falcon.asif");
        std::fs::write(&whole_image, b"image").expect("whole image");
        std::fs::write(crate::metadata::sidecar_path(&whole_image), b"sidecar")
            .expect("whole sidecar");
        std::fs::write(
            crate::metadata::append_suffix(&whole_image, ".ca.key"),
            b"key",
        )
        .expect("whole companion");

        let images = vec![
            (
                WorkspaceName::new("kestrel").expect("workspace"),
                bare_image.clone(),
            ),
            (
                WorkspaceName::new("falcon").expect("workspace"),
                whole_image.clone(),
            ),
        ];
        let findings = quarantine_and_companion_findings_blocking(&store, &repo, &images);

        let codes: Vec<_> = findings
            .iter()
            .map(|finding| finding.code.as_str())
            .collect();
        assert_eq!(
            codes,
            ["workspace-quarantined", "ca-companion-missing"],
            "{findings:?}"
        );
        assert_eq!(findings[0].path, Some(tombstone));
        assert!(
            findings[0].message.contains("data intact"),
            "{}",
            findings[0].message
        );
        assert_eq!(findings[1].hint, "cowshed rekey kestrel");

        let _ = std::fs::remove_dir_all(&root);
    }
}

/// The git-worktree decisions are all read off one store-side fact, so they are tested off one
/// too: a sidecar. Nothing here needs a mount, which is the point — every one of these answers has
/// to be available while the workspace is detached.
#[cfg(all(test, target_os = "macos"))]
mod git_worktree_tests {
    use super::*;
    use crate::metadata::{
        DetachedWorkspaceMetadata, GrantSet, Platform, PortBlock, PublicationState,
        SIDECAR_VERSION, WorkspaceIncarnation, WorkspaceInfoSnapshot, WorkspaceRole,
    };

    fn sidecar(git_worktree: bool) -> DetachedWorkspaceMetadata {
        DetachedWorkspaceMetadata {
            version: SIDECAR_VERSION,
            repo_id: RepoId::parse("acme/widget").expect("repo identity"),
            workspace: WorkspaceName::new("raven").expect("workspace name"),
            workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
                .expect("incarnation"),
            platform: Platform::Macos,
            publication_state: PublicationState::Active,
            updated_at: "2026-07-13T00:00:00Z".to_owned(),
            grants: GrantSet::closed_baseline(Some(
                PortBlock::new(40_960, 16).expect("port block"),
            ))
            .expect("grants"),
            info_snapshot: WorkspaceInfoSnapshot {
                project_root: std::path::PathBuf::from("/project"),
                role: WorkspaceRole::Workspace,
                base_commit: "0123456789abcdef".to_owned(),
                branch: Some("cowshed/raven".to_owned()),
                created_at: "2026-07-13T00:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-07-13T00:00:00Z".to_owned(),
                stale: false,
                git_worktree,
            },
        }
    }

    #[test]
    fn checkpoint_and_restore_refuse_a_git_worktree_workspace_and_name_both_substitutes() {
        let name = WorkspaceName::new("raven").expect("workspace name");
        require_checkpointable(&name, &sidecar(false), "checkpoint")
            .expect("a standalone workspace checkpoints");
        require_checkpointable(&name, &sidecar(false), "restore")
            .expect("a standalone workspace restores");

        for verb in ["checkpoint", "restore"] {
            let error = require_checkpointable(&name, &sidecar(true), verb)
                .expect_err("a git-worktree workspace must refuse");
            // Exit 4: the workspace is fine, the operation is the thing that cannot be done.
            assert_eq!(error.exit_code(), 4);
            assert!(error.message.contains(verb));
            // Both honest substitutes: keep the work, or mint something checkpointable.
            assert!(error.hint.contains("cowshed land raven"), "{}", error.hint);
            assert!(error.hint.contains("cowshed new"), "{}", error.hint);
        }
    }

    #[test]
    fn only_a_git_worktree_workspace_reaches_into_mains_repository() {
        let main_mount = PathBuf::from("/Users/tester/.cowshed/mnt/acme/widget/main");
        assert_eq!(
            git_worktree_repository(&sidecar(true), main_mount.clone()),
            Some(main_mount.join(".git"))
        );
        assert_eq!(git_worktree_repository(&sidecar(false), main_mount), None);
    }

    /// A sidecar written before the mode existed describes a standalone workspace, and must read
    /// as one rather than failing closed: the absent field is an answer, not a gap.
    #[test]
    fn a_sidecar_without_the_field_is_a_standalone_workspace() {
        let mut wire = serde_json::to_value(sidecar(false)).expect("encode sidecar");
        wire["infoSnapshot"]
            .as_object_mut()
            .expect("info snapshot object")
            .remove("gitWorktree");
        let decoded: DetachedWorkspaceMetadata =
            serde_json::from_value(wire).expect("decode legacy sidecar");
        assert!(!is_git_worktree(&decoded));
    }
}

/// Portable: a marker is plain metadata and the reader is not platform-specific.
#[cfg(test)]
mod workspace_marker_reader_tests {
    use super::*;

    fn temp_directory(test: &str) -> PathBuf {
        let path = std::env::temp_dir().join(format!(
            "cowshed-marker-{test}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&path).expect("temp directory");
        path
    }

    /// "No marker" and "a marker I cannot read" are different facts and must not share an answer.
    /// The swallowing reader this replaced turned a damaged `.cowshed/workspace.json` into
    /// `None`, so `project.open` from that directory refused with "path does not belong to the
    /// bound project" -- naming the wrong problem and sending the operator to the wrong repair.
    #[tokio::test]
    async fn an_absent_marker_is_none_and_a_damaged_marker_is_an_integrity_error() {
        let root = temp_directory("damaged");

        assert!(
            read_workspace_marker(&root)
                .await
                .expect("an absent marker is not a failure")
                .is_none()
        );

        let marker = root.join(crate::storage::WORKSPACE_MARKER_PATH);
        std::fs::create_dir_all(marker.parent().expect("marker parent")).expect("marker directory");
        std::fs::write(&marker, b"{ this is not a marker").expect("damaged marker");

        let error = read_workspace_marker(&root)
            .await
            .expect_err("a marker that exists but cannot be parsed is damage, not absence");
        assert_eq!(error.code, ErrorCode::Integrity);

        std::fs::remove_dir_all(&root).ok();
    }
}

#[cfg(all(test, target_os = "macos"))]
mod workspace_origin_tests {
    use super::*;
    use crate::metadata::{
        DetachedWorkspaceMetadata, GrantSet, MARKER_VERSION, Platform, PortBlock, PublicationState,
        SIDECAR_VERSION, WorkspaceIncarnation, WorkspaceInfoSnapshot, WorkspaceMarker,
        WorkspaceRole,
    };
    use crate::storage::lifecycle::{
        DerivedWorkspace, LifecycleWorkspace, MountState, Revision, StorageFact,
    };

    fn temp_directory(test: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let path = std::env::temp_dir().join(format!(
            "cowshed-origin-{test}-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir(&path).expect("temp directory");
        path
    }

    fn write_marker(root: &Path, workspace: &str, role: WorkspaceRole, project_root: &Path) {
        let path = root.join(crate::storage::WORKSPACE_MARKER_PATH);
        std::fs::create_dir_all(path.parent().expect("marker parent")).expect("marker directory");
        crate::metadata::write_json(
            &path,
            &WorkspaceMarker {
                version: MARKER_VERSION,
                repo_id: RepoId::parse("acme/widget").expect("repo"),
                project_root: project_root.to_owned(),
                workspace: WorkspaceName::new(workspace).expect("workspace"),
                workspace_incarnation: WorkspaceIncarnation::new(
                    "00000000000000000000000000000001",
                )
                .expect("incarnation"),
                role,
                base_commit: "0123456789abcdef".to_owned(),
                created_at: "2026-07-13T00:00:00Z".to_owned(),
                forked_from: None,
                created_trace: "fixture".to_owned(),
                lineage: Some(Vec::new()),
            },
        )
        .expect("write marker");
    }

    fn incarnation(value: &str) -> WorkspaceIncarnation {
        WorkspaceIncarnation::new(value).expect("incarnation")
    }

    fn lifecycle_workspace(
        workspace: &str,
        incarnation: WorkspaceIncarnation,
        role: WorkspaceRole,
    ) -> LifecycleWorkspace {
        LifecycleWorkspace::new(
            RepoId::parse("acme/widget").expect("repo"),
            WorkspaceName::new(workspace).expect("workspace"),
            incarnation,
            Revision::new(1),
            Revision::new(1),
            role,
        )
        .expect("lifecycle workspace")
    }

    fn main_metadata(
        project_root: &Path,
        incarnation: WorkspaceIncarnation,
    ) -> DetachedWorkspaceMetadata {
        DetachedWorkspaceMetadata {
            version: SIDECAR_VERSION,
            repo_id: RepoId::parse("acme/widget").expect("repo"),
            workspace: WorkspaceName::new("main").expect("main"),
            workspace_incarnation: incarnation,
            platform: Platform::Macos,
            publication_state: PublicationState::Active,
            updated_at: "2026-07-13T00:00:00Z".to_owned(),
            grants: GrantSet::closed_baseline(Some(
                PortBlock::new(49_136, 16).expect("port block"),
            ))
            .expect("grants"),
            info_snapshot: WorkspaceInfoSnapshot {
                project_root: project_root.to_owned(),
                role: WorkspaceRole::Main,
                base_commit: "0123456789abcdef".to_owned(),
                branch: None,
                created_at: "2026-07-13T00:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-07-13T00:00:00Z".to_owned(),
                stale: false,
                git_worktree: false,
            },
        }
    }

    /// A coordinator verb invoked from inside a session workspace must open the project, not be
    /// refused. The session's marker records main's checkout — a different directory than the mount
    /// the caller stands in — and that difference is the normal case, not a mismatch.
    #[tokio::test]
    async fn a_session_mount_names_its_project_and_reports_mains_checkout() {
        let temp = temp_directory("session");
        let checkout = temp.join("checkout");
        let mount = temp.join("mnt/task");
        std::fs::create_dir_all(&checkout).expect("checkout");
        std::fs::create_dir_all(&mount).expect("mount");
        write_marker(&mount, "task", WorkspaceRole::Workspace, &checkout);

        let origin = workspace_origin_from_marker(&mount)
            .await
            .expect("a session marker identifies its project")
            .expect("marker present");
        assert_eq!(origin.repo_id, RepoId::parse("acme/widget").expect("repo"));
        assert_eq!(
            origin.project_root, checkout,
            "the project checkout comes from the marker, never from the invocation directory"
        );

        std::fs::remove_dir_all(&temp).ok();
    }

    #[tokio::test]
    async fn a_session_resolves_a_missing_main_from_the_store_without_opening_old_git() {
        let temp = temp_directory("stale-main-binding");
        let store = temp.join("store");
        let missing_checkout = temp.join("missing-main");
        let session = temp.join("mnt/slot@1");
        std::fs::create_dir_all(&session).expect("session mount");
        write_marker(
            &session,
            "task",
            WorkspaceRole::Workspace,
            &missing_checkout,
        );
        let origin = workspace_origin_from_marker(&session)
            .await
            .expect("session marker")
            .expect("origin");
        let repo_id = RepoId::parse("acme/widget").expect("repo");
        let layout = crate::storage::StorageLayout::new(&store, &repo_id).expect("layout");
        std::fs::create_dir_all(&layout.project().project_root).expect("project store");
        let binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id.clone(),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://example.test/acme/widget.git".to_owned()),
            primary: true,
        }])
        .expect("binding");
        crate::metadata::write_json(&layout.project().repository_binding, &binding)
            .expect("persist binding");

        let (resolved_repo, _, resolved_binding) =
            project_binding_from_workspace_origin(&store, &session, Some(&origin))
                .await
                .expect("store binding resolves a detached main")
                .expect("session roots differ");
        assert_eq!(resolved_repo, repo_id);
        assert_eq!(resolved_binding, binding);
        assert!(
            !missing_checkout.exists(),
            "the old checkout path remains absent; resolving identity never opens it"
        );

        std::fs::remove_dir_all(&temp).ok();
    }

    #[tokio::test]
    async fn detached_main_relocation_accepts_retired_roots_after_session_identity_validation() {
        let temp = temp_directory("retired-roots");
        let persisted_old_root = temp.join("historical-a");
        let retired_controller_root = temp.join("historical-b");
        let session = temp.join("mnt/slot@1");
        let destination = temp.join("destination-d");
        let store = temp.join("store");
        std::fs::create_dir_all(&session).expect("session mount");
        write_marker(
            &session,
            "task",
            WorkspaceRole::Workspace,
            &retired_controller_root,
        );

        let origin = workspace_origin_from_marker(&session)
            .await
            .expect("session marker")
            .expect("origin");
        let repo_id = RepoId::parse("acme/widget").expect("repo");
        let layout =
            crate::storage::StorageLayout::with_mount_root(&store, temp.join("mnt"), &repo_id)
                .expect("layout");
        std::fs::create_dir_all(&layout.project().project_root).expect("project store");
        let binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id.clone(),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://example.test/acme/widget.git".to_owned()),
            primary: true,
        }])
        .expect("binding");
        let mismatched_binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: RepoId::parse("other/widget").expect("other repo"),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://example.test/other/widget.git".to_owned()),
            primary: true,
        }])
        .expect("mismatched binding");
        crate::metadata::write_json(&layout.project().repository_binding, &mismatched_binding)
            .expect("persist mismatched binding");
        project_binding_from_workspace_origin(&store, &session, Some(&origin))
            .await
            .expect_err("marker and binding repository identities must agree");
        crate::metadata::write_json(&layout.project().repository_binding, &binding)
            .expect("persist binding");
        project_binding_from_workspace_origin(&store, &session, Some(&origin))
            .await
            .expect("store binding")
            .expect("session invocation");

        let session_fact = StorageFact {
            workspace: lifecycle_workspace(
                "task",
                incarnation("00000000000000000000000000000001"),
                WorkspaceRole::Workspace,
            ),
            volume_key: "disk-session".to_owned(),
        };
        let stale_session_fact = StorageFact {
            workspace: lifecycle_workspace(
                "task",
                incarnation("00000000000000000000000000000009"),
                WorkspaceRole::Workspace,
            ),
            volume_key: "disk-stale-session".to_owned(),
        };
        let owned = OwnedRepoIds::sole(origin.repo_id.clone());
        validate_workspace_origin_against_inventory(&origin, &owned, &[&stale_session_fact])
            .expect_err("marker and active storage incarnations must agree");
        validate_workspace_origin_against_inventory(&origin, &owned, &[&session_fact])
            .expect("marker repo, workspace, and incarnation match storage");
        // A marker naming an identity the project does not own is still refused, which is what
        // keeps the membership test from becoming "any repo id".
        validate_workspace_origin_against_inventory(
            &origin,
            &OwnedRepoIds::sole(RepoId::parse("acme/unrelated").expect("repo")),
            &[&session_fact],
        )
        .expect_err("a marker identity the project does not own must be refused");

        let session_incarnation = incarnation("00000000000000000000000000000001");
        let session_workspace = DerivedWorkspace {
            workspace: lifecycle_workspace(
                "task",
                session_incarnation.clone(),
                WorkspaceRole::Workspace,
            ),
            mount_state: MountState::Mounted { mount_id: 7 },
            checkpoints: Vec::new(),
        };
        let mut session_metadata = main_metadata(&persisted_old_root, session_incarnation);
        session_metadata.workspace = WorkspaceName::new("task").expect("task");
        session_metadata.info_snapshot.role = WorkspaceRole::Workspace;
        validate_workspace_controller_root(
            &session_workspace,
            &session_metadata,
            &retired_controller_root,
            ProjectRootValidation::Strict,
        )
        .expect("session sidecars do not claim the controller checkout");

        let main_incarnation = incarnation("00000000000000000000000000000002");
        let main = DerivedWorkspace {
            workspace: lifecycle_workspace("main", main_incarnation.clone(), WorkspaceRole::Main),
            mount_state: MountState::Detached,
            checkpoints: Vec::new(),
        };
        let metadata = main_metadata(&persisted_old_root, main_incarnation);
        validate_workspace_controller_root(
            &main,
            &metadata,
            &retired_controller_root,
            ProjectRootValidation::AllowDetachedMainRelocation,
        )
        .expect("explicit detached-main relocation accepts retired roots");

        let image = layout.main_image().expect("main paths").image().to_owned();
        std::fs::write(&image, b"main image").expect("image");
        metadata.write_for_image(&image).expect("sidecar");
        let record = crate::checkout::CheckoutRecord {
            mount_point: retired_controller_root.clone(),
            image,
        };
        require_vacant_move_destination(&destination).expect("a vacant destination");
        prepare_detached_checkout_relocation(&record, &retired_controller_root, &destination)
            .expect("relocate detached main");

        assert!(
            destination.is_dir(),
            "the explicit destination becomes the mountpoint"
        );
        assert_eq!(
            DetachedWorkspaceMetadata::read_for_image(&record.image)
                .expect("updated sidecar")
                .info_snapshot
                .project_root,
            destination
        );
        assert!(
            !retired_controller_root.exists(),
            "the absent retired checkout is not recreated"
        );
        std::fs::remove_dir_all(&temp).ok();
    }

    #[test]
    fn detached_relocation_refuses_every_occupied_destination() {
        let temp = temp_directory("occupied-destinations");
        let destination = temp.join("destination");

        std::fs::create_dir(&destination).expect("occupied directory");
        require_vacant_move_destination(&destination)
            .expect_err("an ordinary directory remains an occupant");
        std::fs::remove_dir(&destination).expect("remove directory");

        std::fs::write(&destination, b"occupant").expect("occupied file");
        require_vacant_move_destination(&destination)
            .expect_err("an ordinary file remains an occupant");
        std::fs::remove_file(&destination).expect("remove file");

        std::os::unix::fs::symlink(temp.join("missing"), &destination).expect("dangling link");
        require_vacant_move_destination(&destination)
            .expect_err("a dangling symlink remains an occupant");

        std::fs::remove_dir_all(&temp).ok();
    }

    #[test]
    fn mounted_main_still_rejects_disagreeing_persisted_and_controller_roots() {
        let main_incarnation = incarnation("00000000000000000000000000000003");
        let main = DerivedWorkspace {
            workspace: lifecycle_workspace("main", main_incarnation.clone(), WorkspaceRole::Main),
            mount_state: MountState::Mounted { mount_id: 42 },
            checkpoints: Vec::new(),
        };
        let metadata = main_metadata(Path::new("/historical/a"), main_incarnation);

        let error = validate_workspace_controller_root(
            &main,
            &metadata,
            Path::new("/retired/controller/b"),
            ProjectRootValidation::AllowDetachedMainRelocation,
        )
        .expect_err("mounted main retains strict root agreement");
        assert!(
            error
                .to_string()
                .contains("persisted project root /historical/a disagrees with controller root /retired/controller/b"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn mains_marker_must_still_name_the_root_it_sits_in_and_roles_must_agree() {
        let temp = temp_directory("main");
        let checkout = temp.join("checkout");
        std::fs::create_dir_all(&checkout).expect("checkout");

        write_marker(&checkout, "main", WorkspaceRole::Main, &checkout);
        let origin = workspace_origin_from_marker(&checkout)
            .await
            .expect("coherent main marker")
            .expect("marker present");
        assert_eq!(origin.project_root, checkout);

        // Main recorded somewhere it is not: the one project-root disagreement that is still real.
        write_marker(
            &checkout,
            "main",
            WorkspaceRole::Main,
            &temp.join("elsewhere"),
        );
        assert!(workspace_origin_from_marker(&checkout).await.is_err());

        // Role and name disagreeing is corruption in either direction.
        write_marker(&checkout, "task", WorkspaceRole::Main, &checkout);
        assert!(workspace_origin_from_marker(&checkout).await.is_err());
        write_marker(&checkout, "main", WorkspaceRole::Workspace, &checkout);
        assert!(workspace_origin_from_marker(&checkout).await.is_err());

        std::fs::remove_dir_all(&temp).ok();
    }

    /// The dirty-target merge block must be distinguishable from a conflict and from the
    /// generic fallback: its recourse is commit-or-discard in that tree, not "resolve the
    /// conflict" and not bare `git status`.
    #[test]
    fn dirty_target_merge_block_names_its_own_recourse() {
        use std::os::unix::process::ExitStatusExt;

        let failed = |stderr: &str| std::process::Output {
            status: std::process::ExitStatus::from_raw(256),
            stdout: Vec::new(),
            stderr: stderr.as_bytes().to_vec(),
        };

        let error = require_git_success(
            "git operation",
            &failed(
                "error: Your local changes to the following files would be overwritten by merge:\n\tREADME.md\nPlease commit your changes or stash them before you merge.",
            ),
        )
        .expect_err("a blocked merge is a failure");
        assert!(
            error.hint.contains("commit or discard"),
            "dirty-target hint must name the remedy: {}",
            error.hint
        );
        assert!(
            error.message.contains("would be overwritten by merge"),
            "git's own diagnosis survives verbatim: {}",
            error.message
        );
        assert!(!error.hint.contains("resolve the git conflict"));

        // The diverged case routes to cowshed's own verb, not git's merge menu.
        let error = require_git_success(
            "git operation",
            &failed(
                "hint: Diverging branches can't be fast-forwarded, you need to either:\nfatal: Not possible to fast-forward, aborting.",
            ),
        )
        .expect_err("diverged land is a failure");
        assert!(
            error.hint.contains("cowshed rebase"),
            "divergence recourse is cowshed's vocabulary: {}",
            error.hint
        );
        assert!(!error.hint.contains("git status"));
    }

    /// The sandbox-denial detector fires on the kernel's EPERM wording (the only evidence a
    /// denied child produces) and stays silent on ordinary failures — a detector that fired
    /// on everything would mislabel every genuine check failure as environmental.
    #[test]
    fn sandbox_denial_detection_fires_on_eperm_and_nothing_else() {
        let denial = sandbox_denial_in(
            "error: failed to run custom build command for `foo`\n  cat: /Users/dev/projects/example-app/Cargo.toml: Operation not permitted",
        )
        .expect("EPERM wording must classify as a denial");
        assert!(denial.contains("Operation not permitted"));

        assert!(sandbox_denial_in("cat: /x: Permission denied").is_some());
        assert!(
            sandbox_denial_in("thread 'main' panicked at src/lib.rs:1:1:\nexplicit panic")
                .is_none(),
            "a plain test failure is not an environment refusal"
        );
        assert!(
            sandbox_denial_in("").is_none(),
            "empty output owes no claim"
        );
    }

    /// A failed git invocation must not be reported as a conflict unless git said so: the phantom
    /// "resolve the git conflict" hint sent users looking for markers and a rebase that never
    /// existed, and hid git's own diagnosis behind it.
    #[test]
    fn only_a_real_conflict_gets_the_conflict_hint() {
        use std::os::unix::process::ExitStatusExt;

        let failed = |stderr: &str| std::process::Output {
            status: std::process::ExitStatus::from_raw(256),
            stdout: Vec::new(),
            stderr: stderr.as_bytes().to_vec(),
        };

        let error = require_git_success("rebase", &failed("fatal: invalid upstream 'main/main'"))
            .expect_err("a missing upstream is a failure");
        assert!(
            !error.hint.contains("resolve the git conflict"),
            "a missing upstream is not a conflict: {}",
            error.hint
        );
        assert!(
            error.message.contains("invalid upstream"),
            "git's own diagnosis survives: {}",
            error.message
        );

        let error = require_git_success(
            "rebase",
            &failed("CONFLICT (content): Merge conflict in src/lib.rs"),
        )
        .expect_err("a conflict is a failure");
        assert!(
            error.hint.contains("resolve the git conflict"),
            "{}",
            error.hint
        );
    }
}

#[cfg(all(test, target_os = "macos"))]
mod binding_heal_persistence_tests {
    use super::*;

    /// The healed transport survives a re-read from disk: open persists what it reconciled, so
    /// every later reader — and the next open — sees the URL Git actually uses.
    #[tokio::test]
    async fn a_healed_binding_round_trips_through_the_persisted_file() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let store =
            std::env::temp_dir().join(format!("cowshed-heal-{}-{nonce}", std::process::id()));
        let repo_id = RepoId::parse("acme/widget").expect("repo");
        let layout = crate::storage::StorageLayout::new(&store, &repo_id).expect("layout");
        std::fs::create_dir_all(&layout.project().project_root).expect("project store");
        let recorded = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id.clone(),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://github.com/acme/widget.git".to_owned()),
            primary: true,
        }])
        .expect("binding");
        crate::metadata::write_json(&layout.project().repository_binding, &recorded)
            .expect("persist recorded binding");

        let remotes = [crate::git::RemoteUrl {
            name: "origin".to_owned(),
            url: "ssh://git@forge.example.test:2223/acme/widget.git".into(),
        }];
        let healed = reconcile_binding_with_remotes(
            &recorded,
            &remotes,
            BindingRemoteValidation::Strict,
            Path::new("/checkout"),
        )
        .expect("transport move reconciles")
        .expect("recorded transport follows the move");
        persist_binding(&layout, &healed)
            .await
            .expect("persist healed binding");

        let reread = read_persisted_binding(&layout)
            .await
            .expect("read persisted binding")
            .expect("binding file exists");
        assert_eq!(reread, healed);
        assert_eq!(
            reread.primary().expect("primary").remote_url.as_deref(),
            Some("ssh://git@forge.example.test:2223/acme/widget.git"),
        );

        std::fs::remove_dir_all(&store).ok();
    }
}

#[cfg(test)]
mod binding_tests {
    use super::*;

    fn repo_id(value: &str) -> RepoId {
        RepoId::parse(value).expect("valid repository identity")
    }

    fn remote(name: &str, url: &str) -> crate::git::RemoteUrl {
        crate::git::RemoteUrl {
            name: name.to_owned(),
            url: url.into(),
        }
    }

    #[test]
    fn a_transport_move_heals_the_recorded_remote_url() {
        let binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id("acme/widget"),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://github.com/acme/widget.git".to_owned()),
            primary: true,
        }])
        .expect("binding");
        // Same owner/repo, entirely new server and transport.
        let remotes = [remote(
            "origin",
            "ssh://git@forge.example.test:2223/acme/widget.git",
        )];
        let updated = reconcile_binding_with_remotes(
            &binding,
            &remotes,
            BindingRemoteValidation::Strict,
            Path::new("/checkout"),
        )
        .expect("transport move reconciles")
        .expect("recorded transport follows the move");
        assert_eq!(
            updated.primary().expect("primary").remote_url.as_deref(),
            Some("ssh://git@forge.example.test:2223/acme/widget.git"),
        );
        assert_eq!(
            updated.primary().expect("primary").repo_id,
            repo_id("acme/widget")
        );

        // A second reconcile against the healed binding is a no-op.
        assert!(
            reconcile_binding_with_remotes(
                &updated,
                &remotes,
                BindingRemoteValidation::Strict,
                Path::new("/checkout")
            )
            .expect("healed binding matches")
            .is_none()
        );
    }

    /// The recorded URL is the fetch route's key: Git rewrites a dependency URL only when it
    /// starts with the recorded one, so an SSH login name — which addresses the server's account —
    /// is kept. An HTTPS username (and any password) can be a token and is never recorded.
    #[test]
    fn a_recorded_remote_keeps_its_ssh_login_and_drops_https_userinfo() {
        for (remote, recorded) in [
            (
                "ssh://forgejo@forge.example.test:2223/acme/widget.git",
                "ssh://forgejo@forge.example.test:2223/acme/widget.git",
            ),
            (
                "git@github.com:acme/widget.git",
                "git@github.com:acme/widget.git",
            ),
            (
                "ssh://git:secret@forge.example.test/acme/widget.git",
                "ssh://forge.example.test/acme/widget.git",
            ),
            (
                "https://x-access-token:ghp_secret@github.com/acme/widget.git",
                "https://github.com/acme/widget.git",
            ),
            (
                "https://ghp_secret@github.com/acme/widget.git?ref=main#readme",
                "https://github.com/acme/widget.git",
            ),
        ] {
            assert_eq!(
                persistable_remote_url(Path::new(remote)).as_deref(),
                Some(recorded),
                "{remote}"
            );
        }
    }

    fn adopted_widget() -> RepositoryBinding {
        RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id("acme/widget"),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://github.com/acme/widget.git".to_owned()),
            primary: true,
        }])
        .expect("binding")
    }

    fn widget_remotes() -> [crate::git::RemoteUrl; 3] {
        [
            remote("origin", "https://github.com/acme/widget.git"),
            remote(
                "forge",
                "ssh://git@forge.example.test:2223/forge/widget.git",
            ),
            remote("backup", "/Volumes/Backup/widget.git"),
        ]
    }

    #[test]
    fn identity_add_binds_a_configured_remote_as_a_non_primary_identity_once() {
        let remotes = widget_remotes();
        let (identity, updated) =
            bind_remote(&adopted_widget(), &remotes, "forge", &[]).expect("forge binds");
        let updated = updated.expect("the forge remote is newly bound");
        assert_eq!(
            identity,
            crate::repository::BoundIdentity {
                repo_id: repo_id("forge/widget"),
                remote_name: Some("forge".to_owned()),
                remote_url: Some("ssh://git@forge.example.test:2223/forge/widget.git".to_owned()),
                primary: false,
            }
        );
        assert_eq!(
            updated.identities,
            [adopted_widget().identities[0].clone(), identity.clone()]
        );

        // Binding it again, or the adopted remote, changes nothing; and the next open's reconcile
        // pairs the new identity with the checkout without touching it.
        let (again, unchanged) = bind_remote(&updated, &remotes, "forge", &[]).expect("rebind");
        assert_eq!((again, unchanged), (identity, None));
        let (primary, unchanged) = bind_remote(&updated, &remotes, "origin", &[]).expect("primary");
        assert!(primary.primary && unchanged.is_none());
        assert!(
            reconcile_binding_with_remotes(
                &updated,
                &remotes,
                BindingRemoteValidation::Strict,
                Path::new("/checkout"),
            )
            .expect("the bound remote is configured")
            .is_none()
        );
    }

    /// A fetch URL routes to exactly one clone, in each of its SSH spellings.
    #[test]
    fn identity_add_refuses_a_url_another_project_already_binds() {
        let other = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id("forge/widget"),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("ssh://forge.example.test:2223/forge/widget.git".to_owned()),
            primary: true,
        }])
        .expect("other binding");
        let error = bind_remote(
            &adopted_widget(),
            &widget_remotes(),
            "forge",
            &[(repo_id("forge/widget"), other)],
        )
        .expect_err("the URL already routes to forge/widget's clone");
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(error.message.contains("forge/widget"), "{}", error.message);
    }

    #[test]
    fn identity_add_refuses_a_remote_that_names_no_new_repository() {
        let missing = bind_remote(&adopted_widget(), &widget_remotes(), "upstream", &[])
            .expect_err("no such remote");
        assert_eq!(missing.code, ErrorCode::Usage);
        assert!(
            missing.hint.contains("origin|forge|backup"),
            "{}",
            missing.hint
        );

        let local = bind_remote(&adopted_widget(), &widget_remotes(), "backup", &[])
            .expect_err("a local path names no owner/repo");
        assert_eq!(local.code, ErrorCode::Usage);

        let mirror = [remote(
            "mirror",
            "https://gitlab.example.test/acme/widget.git",
        )];
        let same = bind_remote(&adopted_widget(), &mirror, "mirror", &[])
            .expect_err("acme/widget is already this project's identity");
        assert_eq!(same.code, ErrorCode::Conflict);
    }

    #[test]
    fn an_identity_move_refuses_and_names_the_rebind_verb() {
        let binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id("acme/widget"),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://github.com/acme/widget.git".to_owned()),
            primary: true,
        }])
        .expect("binding");
        let remotes = [remote(
            "origin",
            "ssh://git@forge.example.test:2223/other/widget.git",
        )];
        let error = reconcile_binding_with_remotes(
            &binding,
            &remotes,
            BindingRemoteValidation::Strict,
            Path::new("/checkout"),
        )
        .expect_err("a different owner/repo is a real divergence");
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(error.message.contains("acme/widget"), "{}", error.message);
        assert!(error.message.contains("other/widget"), "{}", error.message);
        // The retry runs from outside the checkout, so the hint must carry --project.
        assert!(
            error
                .hint
                .contains("cowshed --project /checkout mv main --repo-id other/widget"),
            "{}",
            error.hint
        );
    }

    #[test]
    fn the_identity_change_open_tolerates_a_moved_remote() {
        let binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id("acme/widget"),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://github.com/acme/widget.git".to_owned()),
            primary: true,
        }])
        .expect("binding");
        let remotes = [remote(
            "origin",
            "ssh://git@forge.example.test:2223/other/widget.git",
        )];
        assert!(
            reconcile_binding_with_remotes(
                &binding,
                &remotes,
                BindingRemoteValidation::ForIdentityChange,
                Path::new("/checkout"),
            )
            .expect("the rebind verb must stay reachable")
            .is_none()
        );
    }

    /// The live-host regression: recovery runs this same gate before the identity-change verb
    /// can dispatch, so the mode must win over every refusal arm — a moved identity, a deleted
    /// remote name, and an unparseable URL alike. (The strict per-pair validator this replaces
    /// refused all three and made `mv … --repo-id` unreachable on a live host.)
    #[test]
    fn the_identity_change_mode_outranks_every_refusal_arm() {
        let binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id("acme/widget"),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://github.com/acme/widget.git".to_owned()),
            primary: true,
        }])
        .expect("binding");
        for remotes in [
            // Identity moved.
            vec![remote(
                "origin",
                "ssh://git@forge.example.test:2223/other/widget.git",
            )],
            // Recorded remote deleted.
            vec![],
            // Unparseable URL.
            vec![remote("origin", "not a remote url \\")],
        ] {
            assert!(
                reconcile_binding_with_remotes(
                    &binding,
                    &remotes,
                    BindingRemoteValidation::ForIdentityChange,
                    Path::new("/checkout"),
                )
                .expect("the rebind verb must stay reachable")
                .is_none()
            );
        }
    }

    #[test]
    fn a_matching_or_absent_remote_pairing_reconciles_to_nothing_or_refuses() {
        let binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id("acme/widget"),
            remote_name: Some("origin".to_owned()),
            remote_url: Some("https://example.test/acme/widget.git".to_owned()),
            primary: true,
        }])
        .expect("binding");
        let matching = [remote("origin", "https://example.test/acme/widget.git")];
        assert!(
            reconcile_binding_with_remotes(
                &binding,
                &matching,
                BindingRemoteValidation::Strict,
                Path::new("/checkout")
            )
            .expect("matching remotes")
            .is_none()
        );
        // The recorded remote name no longer exists at all: identity cannot be derived from
        // anything, so the original restore guidance stands.
        let renamed = [remote("upstream", "https://example.test/acme/widget.git")];
        let error = reconcile_binding_with_remotes(
            &binding,
            &renamed,
            BindingRemoteValidation::Strict,
            Path::new("/checkout"),
        )
        .expect_err("a deleted remote pairing still refuses");
        assert!(
            error.hint.contains("restore the recorded remote"),
            "{}",
            error.hint
        );
    }

    #[test]
    fn local_only_binding_requires_and_preserves_explicit_identity() {
        let requested = repo_id("acme/widget");
        let binding = binding_from_remotes(&[], Some(&requested)).expect("local-only binding");
        assert_eq!(
            binding.primary().expect("primary"),
            &crate::repository::BoundIdentity {
                repo_id: requested,
                remote_name: None,
                remote_url: None,
                primary: true,
            }
        );

        let error = binding_from_remotes(&[], None).expect_err("missing identity must fail");
        assert_eq!(error.code, ErrorCode::EnvironmentMissing);
        assert!(error.hint.contains("--repo-id"));
    }

    #[test]
    fn explicit_identity_must_match_a_normalized_remote_candidate() {
        let remotes = [remote(
            "origin",
            "https://user:secret@example.com/Acme/Widget.git?token=secret#fragment",
        )];
        let requested = repo_id("acme/widget");
        let binding =
            binding_from_remotes(&remotes, Some(&requested)).expect("matching explicit identity");
        let primary = binding.primary().expect("primary");
        assert_eq!(primary.repo_id, requested);
        assert_eq!(primary.remote_name.as_deref(), Some("origin"));
        assert_eq!(
            primary.remote_url.as_deref(),
            Some("https://example.com/Acme/Widget.git")
        );

        let error = binding_from_remotes(&remotes, Some(&repo_id("other/repo")))
            .expect_err("mismatching explicit identity must fail");
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(error.message.contains("does not match any Git remote"));
    }

    #[test]
    fn distinct_remote_candidates_require_explicit_selection() {
        let remotes = [
            remote("origin", "https://example.com/acme/widget.git"),
            remote("upstream", "ssh://git@example.com/upstream/widget.git"),
        ];
        let error =
            binding_from_remotes(&remotes, None).expect_err("ambiguous identities must fail");
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(error.hint.contains("--repo-id"));
        assert!(error.hint.contains("acme/widget"));
        assert!(error.hint.contains("upstream/widget"));
    }

    /// A local-path backup remote is ordinary Git and yields no owner/repo. It
    /// must not brick a checkout whose other remotes identify it perfectly well
    /// — that failure reached every command, including read-only ones.
    #[test]
    fn a_remote_without_a_derivable_identity_is_skipped_not_fatal() {
        let remotes = [
            remote("origin", "https://example.com/example-org/example-app.git"),
            remote("backup", "/Volumes/Backup/example-app.git"),
        ];

        let binding =
            binding_from_remotes(&remotes, None).expect("the usable remote identifies it");

        let primary = binding.primary().expect("primary");
        assert_eq!(primary.repo_id, repo_id("example-org/example-app"));
        assert_eq!(primary.remote_name.as_deref(), Some("origin"));
    }

    #[test]
    fn a_checkout_whose_every_remote_is_unusable_reports_what_it_skipped() {
        let remotes = [remote("backup", "/Volumes/Backup/example-app.git")];

        let error =
            binding_from_remotes(&remotes, None).expect_err("nothing identifies the repository");
        assert_eq!(error.code.as_str(), "environment-missing");
        assert!(error.message.contains("backup"), "{}", error.message);

        // An explicit identity is still enough to proceed.
        let requested = repo_id("example-org/example-app");
        let binding =
            binding_from_remotes(&remotes, Some(&requested)).expect("explicit identity suffices");
        assert_eq!(binding.primary().expect("primary").repo_id, requested);
    }

    #[test]
    fn duplicate_same_identity_remotes_are_unambiguous_and_prefer_origin() {
        let remotes = [
            remote("backup", "ssh://git@mirror.example/acme/widget.git"),
            remote("origin", "https://example.com/acme/widget.git"),
            remote("upstream", "git://example.net/acme/widget.git"),
        ];
        let binding = binding_from_remotes(&remotes, None).expect("one normalized identity");
        let primary = binding.primary().expect("primary");
        assert_eq!(primary.repo_id, repo_id("acme/widget"));
        assert_eq!(primary.remote_name.as_deref(), Some("origin"));
        assert_eq!(
            primary.remote_url.as_deref(),
            Some("https://example.com/acme/widget.git")
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn matching_repository_identity_with_a_renamed_remote_is_a_binding_mismatch() {
        let binding = RepositoryBinding::new(vec![crate::repository::BoundIdentity {
            repo_id: repo_id("smoothbricks/codebase"),
            remote_name: Some("codebase".to_owned()),
            remote_url: Some("https://github.com/smoothbricks/codebase.git".to_owned()),
            primary: true,
        }])
        .expect("recorded binding");
        let remotes = [remote(
            "origin",
            "https://github.com/smoothbricks/codebase.git",
        )];

        let error = reconcile_binding_with_remotes(
            &binding,
            &remotes,
            BindingRemoteValidation::Strict,
            Path::new("/checkout"),
        )
        .expect_err("the recorded remote name is part of the binding");

        assert_eq!(error.code, ErrorCode::Conflict);
        assert_eq!(
            error.message,
            "repository binding remote codebase does not match Git configuration"
        );
    }
    #[cfg(target_os = "macos")]
    #[tokio::test]
    async fn checkout_identity_accepts_only_the_symlink_into_mains_mount() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-checkout-identity-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let main_mount = root.join("mnt").join("main");
        let elsewhere = root.join("elsewhere");
        let checkout = root.join("project");
        std::fs::create_dir_all(&main_mount).expect("main mount");
        std::fs::create_dir_all(&elsewhere).expect("foreign target");

        // The symlink adoption plants: accepted, and it resolves to main's mount.
        std::os::unix::fs::symlink(&main_mount, &checkout).expect("adopted symlink");
        let metadata = std::fs::symlink_metadata(&checkout).expect("symlink metadata");
        let resolved =
            resolve_checkout_identity_path(&checkout, &metadata, &main_mount, "checkout")
                .await
                .expect("adopted symlink is accepted");
        assert_eq!(
            resolved,
            std::fs::canonicalize(&main_mount).expect("canonical main mount")
        );

        // A symlink to anything else stays a conflict.
        std::fs::remove_file(&checkout).expect("clear symlink");
        std::os::unix::fs::symlink(&elsewhere, &checkout).expect("foreign symlink");
        let metadata = std::fs::symlink_metadata(&checkout).expect("symlink metadata");
        resolve_checkout_identity_path(&checkout, &metadata, &main_mount, "checkout")
            .await
            .expect_err("a foreign symlink is not the adopted checkout");

        // A real directory is inspected in place, exactly as before.
        std::fs::remove_file(&checkout).expect("clear symlink");
        std::fs::create_dir_all(&checkout).expect("real checkout");
        let metadata = std::fs::symlink_metadata(&checkout).expect("directory metadata");
        assert_eq!(
            resolve_checkout_identity_path(&checkout, &metadata, &main_mount, "checkout")
                .await
                .expect("a real directory is accepted"),
            checkout
        );

        std::fs::remove_dir_all(&root).expect("fixture cleanup");
    }

    #[tokio::test]
    async fn startup_pending_restore_destination_is_absent_until_restore_fence() {
        use crate::metadata::WorkspaceRole;
        use crate::runtime::supervisor::{CommitmentDraft, CommitmentPublisher, CommitmentSink};
        use crate::storage::apfs::PendingPublicationFact;
        use crate::storage::lifecycle::{LifecycleWorkspace, Revision, StorageFact};

        let root = std::env::temp_dir().join(format!(
            "cowshed-project-pending-restore-{}",
            uuid::Uuid::new_v4().simple()
        ));
        let telemetry = root.join("telemetry");
        let repo = repo_id("acme/widget");
        let source = WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80").expect("source");
        let destination =
            WorkspaceIncarnation::new("1198f2c0b7e34dc795f17b238b331c80").expect("destination");
        let source_workspace = LifecycleWorkspace::new(
            repo.clone(),
            WorkspaceName::new("main").expect("main"),
            source.clone(),
            Revision::new(1),
            Revision::new(11),
            WorkspaceRole::Main,
        )
        .expect("source workspace");
        let destination_workspace = LifecycleWorkspace::new(
            repo.clone(),
            WorkspaceName::new("main").expect("main"),
            destination.clone(),
            Revision::new(2),
            Revision::new(11),
            WorkspaceRole::Main,
        )
        .expect("destination workspace");
        let facts = vec![
            StorageFact {
                workspace: source_workspace,
                volume_key: "cowshed.acme--widget.main".to_owned(),
            },
            StorageFact {
                workspace: destination_workspace.clone(),
                volume_key: "cowshed.acme--widget.main".to_owned(),
            },
        ];
        let pending = PendingPublicationFact {
            workspace: destination_workspace,
            image: root.join("main.asif"),
            mount_point: root.join("mount"),
            source_checkpoint: "baseline".to_owned(),
            source_incarnation: source.clone(),
            replaced_incarnation: source.clone(),
            destination_incarnation: destination.clone(),
        };
        let pending_slice = std::slice::from_ref(&pending);
        let verified = verified_recovery_facts(&facts, pending_slice);
        assert_eq!(verified.len(), 1);
        assert_eq!(verified[0].workspace.incarnation(), &source);

        // The pending destination is not an active fact until its restore fence activates the
        // image; the audit record of the restore is telemetry and gates nothing.
        let mut commitments =
            CommitmentPublisher::open(&telemetry, crate::storage::audit::ContinuityAudit::Arrow, 8)
                .expect("open audit publisher");
        commitments
            .record(CommitmentDraft::Restore {
                repo_id: repo,
                source_checkpoint: pending.source_checkpoint.clone(),
                source_incarnation: pending.source_incarnation.clone(),
                replaced_incarnation: pending.replaced_incarnation.clone(),
                destination_incarnation: pending.destination_incarnation.clone(),
            })
            .await
            .expect("record recovered restore");
        let health = commitments.health().await.expect("audit health");
        assert_eq!((health.recorded, health.failed), (1, 0));
        drop(destination);
        drop(commitments);
        let _ = std::fs::remove_dir_all(root);
    }
}

#[cfg(all(test, target_os = "macos"))]
mod port_reservation_tests {
    use super::{
        claim_port_block, claim_port_block_with, reserve_grown_port_grants,
        reserve_port_grant_replacement, reserve_port_grants,
    };
    use crate::gateway_inventory::NativeGatewayInventory;
    use crate::metadata::{
        DetachedWorkspaceMetadata, GrantSet, MACOS_PORT_MIN, NEW_PORT_BLOCK_SIZE, Platform,
        PortBlock, PublicationState, SIDECAR_VERSION, WorkspaceIncarnation, WorkspaceName,
    };
    use crate::repository::{BoundIdentity, RepoId, RepositoryBinding};
    use crate::storage::StorageLayout;
    use crate::storage::bootstrap::{CanonicalRoots, ValidatedHostStorage};
    use std::os::unix::fs::symlink;
    use std::path::PathBuf;

    fn root(label: &str) -> crate::temp_root::TempRoot {
        crate::temp_root::TempRoot::new(&format!("cowshed-port-reservation-{label}"))
    }

    fn inventory(root: &std::path::Path) -> (NativeGatewayInventory, StorageLayout) {
        let roots = CanonicalRoots::at(root.join("store"));
        std::fs::create_dir_all(roots.store()).expect("store");
        let repo = RepoId::parse("acme/widget").expect("repo");
        let layout = StorageLayout::new(roots.store(), &repo).expect("layout");
        std::fs::create_dir_all(&layout.project().project_root).expect("project");
        let binding = RepositoryBinding::new(vec![BoundIdentity {
            repo_id: repo,
            remote_name: None,
            remote_url: None,
            primary: true,
        }])
        .expect("binding");
        crate::metadata::write_json(&layout.project().repository_binding, &binding)
            .expect("publish binding");
        let storage = ValidatedHostStorage::new(root.join("home"), roots);
        (NativeGatewayInventory::new(storage), layout)
    }

    #[test]
    fn requested_capacity_is_not_limited_to_the_initial_block_size() {
        for (services, size) in [
            (1, 2),
            (63, 64),
            (64, 128),
            (80, 128),
            (255, 256),
            (8191, 8192),
            (16383, 16384),
        ] {
            assert_eq!(PortBlock::size_for_service_ports(services).unwrap(), size);
            let blocks = PortBlock::macos_candidates_with_size(size)
                .unwrap()
                .collect::<Vec<_>>();
            assert!(!blocks.is_empty());
            assert!(blocks.windows(2).all(|pair| !pair[0].overlaps(pair[1])));
        }
        for services in [0, 16384, u16::MAX] {
            assert!(PortBlock::size_for_service_ports(services).is_err());
        }
    }

    #[test]
    fn relocation_retains_authority_and_containing_growth_coalesces_it() {
        let first = PortBlock::new(40960, 64).unwrap();
        let second = PortBlock::new(41216, 128).unwrap();
        let third = PortBlock::new(41472, 256).unwrap();
        let containing = PortBlock::new(40960, 1024).unwrap();
        let mut grants = GrantSet::closed_baseline(Some(first)).unwrap();
        super::retain_port_authority(&mut grants, second);
        assert_eq!(grants.retained_port_blocks, [first]);
        super::retain_port_authority(&mut grants, third);
        assert_eq!(grants.retained_port_blocks, [first, second]);
        grants.validate(Platform::Macos).unwrap();
        super::retain_port_authority(&mut grants, containing);
        assert_eq!(grants.port_block, Some(containing));
        assert!(grants.retained_port_blocks.is_empty());
        grants.validate(Platform::Macos).unwrap();
    }

    #[test]
    fn reusing_a_retained_slot_does_not_accumulate_authority() {
        let first = PortBlock::new(40960, 64).unwrap();
        let second = PortBlock::new(41024, 64).unwrap();
        let mut grants = GrantSet::closed_baseline(Some(first)).unwrap();
        for _ in 0..128 {
            super::retain_port_authority(&mut grants, second);
            assert_eq!(grants.retained_port_blocks, [first]);
            super::retain_port_authority(&mut grants, first);
            assert_eq!(grants.retained_port_blocks, [second]);
        }
        grants.validate(Platform::Macos).unwrap();
    }

    #[tokio::test]
    async fn legacy_growth_claims_the_containing_initial_size_grid_cell() {
        let root = root("legacy-growth-claim");
        let (inventory, _) = inventory(&root);
        let staging = root.join("store/.staging");
        let mut initial = reserve_port_grants(&inventory, &staging, Default::default())
            .await
            .expect("find a free initial-size grid cell");
        let grid_base = initial.grants.port_block.unwrap().base();
        let target = PortBlock::new(grid_base + 32, 32).unwrap();
        let mut owned =
            GrantSet::closed_baseline(Some(PortBlock::new(grid_base + 48, 16).unwrap())).unwrap();
        owned
            .retained_port_blocks
            .push(PortBlock::new(grid_base + 32, 16).unwrap());
        owned.validate(Platform::Macos).unwrap();
        // Keep legacy services bound through growth without an unbound handoff.
        let old_listeners = initial
            .listeners
            .drain(..)
            .filter(|listener| listener.local_addr().unwrap().port() >= target.base())
            .collect::<Vec<_>>();
        drop(initial);
        let grown = reserve_grown_port_grants(&inventory, &staging, &owned, 32)
            .await
            .expect("grow a legacy 16-port block into its containing 32-port block");
        assert_eq!(grown.grants.port_block, Some(target));
        let probe = claim_port_block(&staging, grid_base).unwrap();
        let common_cell_claimed = probe.is_none();
        if let Some(marker) = probe {
            std::fs::remove_file(marker).unwrap();
        }
        drop(grown);
        drop(old_listeners);
        assert!(
            common_cell_claimed,
            "legacy growth must fence the same 64-port grid cell as a new allocator"
        );
    }

    #[tokio::test]
    async fn larger_unpublished_claim_excludes_every_smaller_grid_cell() {
        let root = root("large-claim");
        let (inventory, _) = inventory(&root);
        let staging = root.join("store/.staging");
        let existing = reserve_port_grants(&inventory, &staging, Default::default())
            .await
            .expect("initial allocation");
        let owned = existing.grants.clone();
        drop(existing);
        let grown = reserve_grown_port_grants(&inventory, &staging, &owned, 128)
            .await
            .expect("grow beyond 64");
        let block = grown.grants.port_block.unwrap();
        for base in [block.base(), block.base() + 64] {
            assert!(claim_port_block(&staging, base).unwrap().is_none());
            let smaller = PortBlock::new(base, 64).unwrap();
            assert!(
                reserve_port_grant_replacement(&inventory, &staging, smaller, None)
                    .await
                    .unwrap()
                    .is_none()
            );
        }
        drop(grown);
        for base in [block.base(), block.base() + 64] {
            let marker = claim_port_block(&staging, base)
                .unwrap()
                .expect("all grid claims release together");
            std::fs::remove_file(marker).unwrap();
        }
    }

    #[tokio::test]
    async fn publication_after_snapshot_and_before_claim_cannot_reuse_a_port_block() {
        let root = root("publication");
        let (inventory, layout) = inventory(&root);
        let staging = root.join("store/.staging");
        let stale = inventory
            .all_reserved_port_blocks()
            .await
            .expect("snapshot");
        assert!(stale.is_empty());

        // Pause the second allocator at its snapshot boundary. The first completes the
        // real claim -> canonical image/metadata publication -> reservation release handoff.
        let mut first = reserve_port_grants(&inventory, &staging, stale.clone())
            .await
            .expect("first allocation");
        let first_block = first.grants.port_block.expect("first block");
        let first_base = first_block.base();
        assert!(PortBlock::macos_candidates().any(|candidate| candidate.base() == first_base));
        let image = layout.main_image().expect("main image");
        std::fs::write(image.image(), b"detached image fixture").expect("image");
        DetachedWorkspaceMetadata {
            version: SIDECAR_VERSION,
            repo_id: RepoId::parse("acme/widget").expect("repo"),
            workspace: WorkspaceName::main(),
            workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
                .expect("incarnation"),
            platform: Platform::Macos,
            publication_state: PublicationState::Active,
            updated_at: "2026-07-14T00:00:00Z".to_owned(),
            grants: first.grants.clone(),
            info_snapshot: crate::metadata::WorkspaceInfoSnapshot {
                project_root: std::path::PathBuf::from("/project"),
                role: crate::metadata::WorkspaceRole::Main,
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                branch: None,
                created_at: "2026-07-14T00:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-07-14T00:00:00Z".to_owned(),
                stale: false,
                git_worktree: false,
            },
        }
        .write_for_image(image.image())
        .expect("publish allocation");
        // Transfer one reserved listener without an unbound interval: another concurrent
        // fixture may claim every newly free host port immediately.
        let service_index = first
            .listeners
            .iter()
            .position(|listener| listener.local_addr().unwrap().port() == first_base + 1)
            .expect("reserved service listener");
        let old_listener = first.listeners.swap_remove(service_index);
        drop(first);
        assert_eq!(
            inventory
                .all_reserved_port_blocks()
                .await
                .expect("publication")
                .blocks()
                .map(|block| block.base())
                .collect::<Vec<_>>(),
            [first_base]
        );

        // The marker is gone, so live-claim exclusion alone cannot protect this stale read.
        let second = reserve_port_grants(&inventory, &staging, stale)
            .await
            .expect("allocation after publication");
        let second_block = second.grants.port_block.expect("second block");
        let second_base = second_block.base();
        assert!(!second_block.overlaps(first_block));
        // Growth must exclude the second creator's unpublished claim as well as every
        // published sibling. Publish its variable size before releasing the reservation.
        let mut metadata =
            DetachedWorkspaceMetadata::read_for_image(image.image()).expect("current allocation");
        let grown = reserve_grown_port_grants(&inventory, &staging, &metadata.grants, 128)
            .await
            .expect("grow around a concurrent creator");
        let grown_block = grown.grants.port_block.unwrap();
        assert!(!grown_block.overlaps(second_block));
        assert_eq!(grown_block.size(), 128);
        super::retain_port_authority(&mut metadata.grants, grown_block);
        metadata.grants.revision += 1;
        metadata.write_for_image(image.image()).unwrap();
        drop(grown);
        let held = inventory
            .all_reserved_port_blocks()
            .await
            .unwrap()
            .blocks()
            .collect::<Vec<_>>();
        assert!(held.contains(&grown_block));
        if !grown_block.overlaps(first_block) {
            assert!(
                held.contains(&first_block),
                "relocation never releases old authority"
            );
        }
        assert!(!held.iter().any(|block| block.overlaps(second_block)));
        let third = reserve_port_grants(&inventory, &staging, Default::default())
            .await
            .expect("a stale allocator still excludes current and retained authority");
        let third_block = third.grants.port_block.unwrap();
        assert!(
            !metadata
                .grants
                .port_blocks()
                .any(|owned| owned.overlaps(third_block))
        );
        drop(third);
        assert!(
            claim_port_block(&staging, second_base)
                .expect("competing claim")
                .is_none()
        );
        let rejected = claim_port_block(&staging, first_base)
            .expect("rejected candidate cleanup")
            .expect("conflicting marker was released");
        std::fs::remove_file(rejected).expect("release probe");
        drop(second);
        let released = claim_port_block(&staging, second_base)
            .expect("claim after owner release")
            .expect("successful owner releases marker");
        std::fs::remove_file(released).expect("release probe");
        drop(old_listener);
    }

    #[tokio::test]
    async fn interrupted_pending_clone_keeps_its_port_after_the_creator_exits() {
        let root = root("pending-clone");
        let (inventory, layout) = inventory(&root);
        let staging = root.join("store/.staging");
        let first = reserve_port_grants(&inventory, &staging, Default::default())
            .await
            .expect("initial allocation");
        let first_block = first.grants.port_block.expect("first block");
        let first_base = first_block.base();
        let name = WorkspaceName::session("unfinished").expect("session");
        let image = layout.session_image(&name).expect("session image");
        std::fs::create_dir_all(image.image().parent().expect("session directory"))
            .expect("session directory");
        std::fs::write(image.image(), b"interrupted clone").expect("image");
        DetachedWorkspaceMetadata {
            version: SIDECAR_VERSION,
            repo_id: RepoId::parse("acme/widget").expect("repo"),
            workspace: name,
            workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
                .expect("incarnation"),
            platform: Platform::Macos,
            publication_state: PublicationState::PendingFence,
            updated_at: "2026-07-14T00:00:00Z".to_owned(),
            grants: first.grants.clone(),
            info_snapshot: crate::metadata::WorkspaceInfoSnapshot {
                project_root: std::path::PathBuf::from("/project"),
                role: crate::metadata::WorkspaceRole::Workspace,
                base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                branch: None,
                created_at: "2026-07-14T00:00:00Z".to_owned(),
                forked_from: None,
                captured_at: "2026-07-14T00:00:00Z".to_owned(),
                stale: false,
                git_worktree: false,
            },
        }
        .write_for_image(image.image())
        .expect("pending metadata");
        drop(first);
        let marker = staging.join(format!("port-{first_base}.reservation"));
        symlink(i32::MAX.to_string(), &marker).expect("dead creator marker");

        // A pending image is not a runnable workspace, but its persisted grant remains owned.
        assert!(
            inventory.all_projects().await.expect("published inventory")[0]
                .workspaces
                .is_empty()
        );
        assert_eq!(
            inventory
                .all_reserved_port_blocks()
                .await
                .expect("reserved inventory")
                .blocks()
                .map(|block| block.base())
                .collect::<Vec<_>>(),
            [first_base]
        );
        let second = reserve_port_grants(&inventory, &staging, Default::default())
            .await
            .expect("allocation after creator death");
        assert!(
            !second
                .grants
                .port_block
                .expect("second block")
                .overlaps(first_block),
            "the pending image still owns its first block"
        );
        drop(second);
        let main = layout.main_image().expect("main image");
        std::fs::write(main.image(), b"published clone").expect("main payload");
        let mut published =
            DetachedWorkspaceMetadata::read_for_image(image.image()).expect("pending sidecar");
        published.workspace = WorkspaceName::main();
        published.info_snapshot.role = crate::metadata::WorkspaceRole::Main;
        published.publication_state = PublicationState::Active;
        published
            .write_for_image(main.image())
            .expect("conflicting published grant");
        assert!(matches!(
            inventory.all_reserved_port_blocks().await,
            Err(crate::gateway_inventory::GatewayInventoryError::OverlappingPortBlocks {
                held,
                claimed,
            }) if held.base() == first_base && claimed.base() == first_base
        ));
        std::fs::remove_file(main.image()).expect("remove conflicting payload");
        std::fs::remove_file(crate::metadata::sidecar_path(main.image()))
            .expect("remove conflicting sidecar");
        published.workspace = WorkspaceName::session("foreign").expect("foreign name");
        published.info_snapshot.role = crate::metadata::WorkspaceRole::Workspace;
        published.publication_state = PublicationState::PendingFence;
        published
            .write_for_image(image.image())
            .expect("mismatched pending identity");
        assert!(matches!(
            inventory.all_reserved_port_blocks().await,
            Err(crate::gateway_inventory::GatewayInventoryError::Apfs(
                crate::storage::apfs::ApfsStorageError::Host(_)
            ))
        ));
    }

    #[tokio::test]
    async fn inventory_error_after_claim_releases_the_port_reservation() {
        let root = root("inventory-error");
        let (inventory, layout) = inventory(&root);
        let staging = root.join("store/.staging");
        let stale = inventory
            .all_reserved_port_blocks()
            .await
            .expect("snapshot");
        std::fs::write(&layout.project().repository_binding, b"{broken")
            .expect("corrupt publication");

        let error = match reserve_port_grants(&inventory, &staging, stale).await {
            Ok(_) => panic!("invalid current inventory must refuse allocation"),
            Err(error) => error,
        };
        assert_eq!(error.code.as_str(), "integrity");
        let marker = claim_port_block(&staging, MACOS_PORT_MIN)
            .expect("claim after inventory error")
            .expect("failed allocation must release its marker");
        std::fs::remove_file(marker).expect("release probe");
    }

    /// A workspace keeps its recorded grant size until an explicit capacity grant, so the store
    /// holds several sizes at once. Live 16-port blocks sit inside the first two 64-port candidates —
    /// one published, one fenced into a pending clone — and a new workspace gets a 64-port block
    /// that overlaps neither, then the next one after it.
    #[tokio::test]
    async fn a_new_block_takes_the_new_size_around_live_blocks_of_any_size() {
        let root = root("mixed-sizes");
        let (inventory, layout) = inventory(&root);
        let staging = root.join("store/.staging");
        let publish = |image: &std::path::Path,
                       workspace: WorkspaceName,
                       publication_state: PublicationState,
                       block: PortBlock| {
            std::fs::create_dir_all(image.parent().expect("image directory")).expect("directory");
            std::fs::write(image, b"detached image fixture").expect("image");
            let role = crate::metadata::WorkspaceRole::for_name(&workspace);
            DetachedWorkspaceMetadata {
                version: SIDECAR_VERSION,
                repo_id: RepoId::parse("acme/widget").expect("repo"),
                workspace,
                workspace_incarnation: WorkspaceIncarnation::new(
                    "0198f2c0b7e34dc795f17b238b331c80",
                )
                .expect("incarnation"),
                platform: Platform::Macos,
                publication_state,
                updated_at: "2026-07-14T00:00:00Z".to_owned(),
                grants: GrantSet::closed_baseline(Some(block)).expect("grants"),
                info_snapshot: crate::metadata::WorkspaceInfoSnapshot {
                    project_root: std::path::PathBuf::from("/project"),
                    role,
                    base_commit: "0123456789abcdef0123456789abcdef01234567".to_owned(),
                    branch: None,
                    created_at: "2026-07-14T00:00:00Z".to_owned(),
                    forked_from: None,
                    captured_at: "2026-07-14T00:00:00Z".to_owned(),
                    stale: false,
                    git_worktree: false,
                },
            }
            .write_for_image(image)
            .expect("publish block");
        };
        let published = PortBlock::new(MACOS_PORT_MIN + 16, 16).expect("live 16-port block");
        let main = layout.main_image().expect("main image");
        publish(
            main.image(),
            WorkspaceName::main(),
            PublicationState::Active,
            published,
        );
        let pending_name = WorkspaceName::session("unfinished").expect("session");
        let pending_block =
            PortBlock::new(MACOS_PORT_MIN + 64 + 48, 16).expect("pending 16-port block");
        let pending = layout.session_image(&pending_name).expect("session image");
        publish(
            pending.image(),
            pending_name,
            PublicationState::PendingFence,
            pending_block,
        );

        let first = reserve_port_grants(&inventory, &staging, Default::default())
            .await
            .expect("allocation around live blocks");
        let first_block = first.grants.port_block.expect("first block");
        assert!(first_block.base() >= MACOS_PORT_MIN + 128);
        assert!(!first_block.overlaps(published));
        assert!(!first_block.overlaps(pending_block));
        let second_name = WorkspaceName::session("second").expect("session");
        let second_image = layout.session_image(&second_name).expect("session image");
        publish(
            second_image.image(),
            second_name,
            PublicationState::PendingFence,
            first_block,
        );
        drop(first);
        let second = reserve_port_grants(&inventory, &staging, Default::default())
            .await
            .expect("allocation after the first new block");
        assert!(
            !second
                .grants
                .port_block
                .expect("second block")
                .overlaps(first_block),
            "publication of the first block excludes it from the next allocation"
        );
        drop(second);
    }

    /// The host's port range is shared with live workspaces and every concurrent allocator, which
    /// all bind the lowest free block base first. So the test takes its block from the allocator,
    /// observes release on connections only its own listeners accepted, and never asserts that a
    /// port it let go of is still free when it looks again.
    #[tokio::test]
    async fn a_port_bound_outside_inventory_cannot_be_allocated_to_a_workspace() {
        use std::io::{ErrorKind, Read};
        use std::net::TcpStream;

        let root = root("external-listener");
        let (inventory, _) = inventory(&root);
        let staging = root.join("store/.staging");
        let mut reservation = reserve_port_grants(&inventory, &staging, Default::default())
            .await
            .expect("a block no listener on the host holds");
        let block = reservation.grants.port_block.expect("selected block");
        // A listener no inventory records: one service port of that block, outliving the guard.
        let service = reservation
            .listeners
            .iter()
            .position(|listener| {
                listener.local_addr().expect("listener address").port() == block.base() + 1
            })
            .expect("the guard listens on every port of its block");
        let outside = reservation.listeners.remove(service);
        // Each connection waits, unaccepted, in one guard listener's queue; closing that listener
        // resets it, so the reset is the kernel's own word that the listener is gone.
        let mut clients = reservation
            .listeners
            .iter()
            .map(|listener| {
                TcpStream::connect(listener.local_addr().expect("listener address"))
                    .expect("connect to a guard listener")
            })
            .collect::<Vec<_>>();
        drop(reservation);
        for client in &mut clients {
            let reset = client
                .read(&mut [0])
                .expect_err("a connection queued on a closed listener is reset");
            assert_eq!(
                reset.kind(),
                ErrorKind::ConnectionReset,
                "dropping the publication guard closes its kernel listeners"
            );
        }
        // Every other candidate is held, so the block with the outside listener is the only offer.
        let mut others = crate::metadata::ReservedPortBlocks::default();
        for candidate in PortBlock::macos_candidates().filter(|candidate| *candidate != block) {
            others.insert(candidate).expect("disjoint candidate blocks");
        }
        let refused = match reserve_port_grants(&inventory, &staging, others).await {
            Ok(granted) => panic!(
                "allocated {:?} although another listener holds port {}",
                granted.grants.port_block,
                block.base() + 1
            ),
            Err(error) => error,
        };
        assert_eq!(refused.code, crate::error::ErrorCode::Conflict);
        drop(outside);
    }

    /// Every block is durable workspace authority until retirement, attached or detached, so the
    /// range bounds the workspaces a host holds. A host whose workspaces fill every initial-size
    /// cell of 40960-49151 — the whole range once, 128 workspaces — still allocates the next one.
    #[tokio::test]
    async fn a_host_holding_128_initial_blocks_still_allocates_a_workspace() {
        let root = root("past-128");
        let (inventory, _) = inventory(&root);
        let staging = root.join("store/.staging");
        let held = || {
            (40_960..=49_151 - (NEW_PORT_BLOCK_SIZE - 1))
                .step_by(usize::from(NEW_PORT_BLOCK_SIZE))
                .map(|base| PortBlock::new(base, NEW_PORT_BLOCK_SIZE).expect("held block"))
        };
        assert_eq!(held().count(), 128);
        let mut used = crate::metadata::ReservedPortBlocks::default();
        for block in held() {
            used.insert(block).expect("disjoint held blocks");
        }
        let granted = reserve_port_grants(&inventory, &staging, used)
            .await
            .expect("a 129th workspace gets a block");
        let block = granted.grants.port_block.expect("granted block");
        assert!(cowshed_gateway_types::is_macos_port_block(
            block.base(),
            block.size()
        ));
        assert!(held().all(|owned| !owned.overlaps(block)));
        drop(granted);
    }

    #[test]
    fn live_reservation_excludes_a_second_allocator_until_release() {
        let root = root("live");
        let first = claim_port_block(&root, 40_960)
            .expect("first claim")
            .expect("reservation");
        assert!(
            claim_port_block(&root, 40_960)
                .expect("second claim")
                .is_none()
        );
        std::fs::remove_file(first).expect("release");
        assert!(
            claim_port_block(&root, 40_960)
                .expect("claim after release")
                .is_some()
        );
    }

    #[test]
    fn dead_process_reservation_is_reclaimed() {
        let root = root("stale");
        let marker = root.join("port-40960.reservation");
        symlink(i32::MAX.to_string(), &marker).expect("stale marker");
        assert!(claim_port_block(&root, 40_960).expect("reclaim").is_some());
    }

    /// The cell's owner releases it between this claimant's refused `symlink` and its read of the
    /// marker -- the window every creator contending for the lowest free cell races through. That
    /// is a free cell, and the claim takes it rather than failing with the vanished marker's ENOENT.
    #[test]
    fn a_marker_released_while_it_is_read_is_claimed() {
        let root = root("vanishing");
        let holder = claim_port_block(&root, 40_960)
            .expect("holder claim")
            .expect("holder reservation");
        let mut released = Some(holder);
        let claimed = claim_port_block_with(&root, 40_960, |marker| {
            if let Some(holder) = released.take() {
                std::fs::remove_file(holder).expect("the holder releases its cell");
            }
            std::fs::read_link(marker)
        })
        .expect("a released cell is no error")
        .expect("and is claimed");
        assert_eq!(
            std::fs::read_link(&claimed).expect("claimed marker"),
            PathBuf::from(std::process::id().to_string())
        );
    }
}

#[cfg(all(test, target_os = "macos"))]
mod terminal_project_cleanup_tests {
    use super::{
        clean_terminal_project_storage, remove_unbound_project_state, require_terminal_storage,
    };
    use crate::repository::{ProjectPaths, RepoId};

    fn root(label: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "cowshed-terminal-project-{label}-{}",
            uuid::Uuid::new_v4()
        ))
    }

    #[test]
    fn cleanup_removes_only_empty_structure_and_zero_length_locks() {
        let root = root("safe");
        let binding = root.join("repository.json");
        std::fs::create_dir_all(root.join(".staging")).expect("staging");
        std::fs::create_dir_all(root.join("checkpoints/main")).expect("checkpoints");
        std::fs::create_dir_all(root.join("sessions/.trash")).expect("trash");
        std::fs::write(root.join("sessions/raven.asif.lock"), b"").expect("session lock");
        std::fs::write(root.join("main.asif.lock"), b"").expect("main lock");
        std::fs::write(&binding, b"binding").expect("binding");
        std::fs::write(root.join("policy.json"), b"preserve").expect("policy");

        clean_terminal_project_storage(&root, &binding).expect("terminal cleanup");
        assert!(binding.is_file());
        assert_eq!(
            std::fs::read(root.join("policy.json")).expect("policy"),
            b"preserve"
        );
        assert!(!root.join(".staging").exists());
        assert!(!root.join("checkpoints").exists());
        assert!(!root.join("sessions").exists());
        assert!(!root.join("main.asif.lock").exists());
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn cleanup_preserves_and_rejects_an_unreclaimed_image() {
        let root = root("blocked");
        let binding = root.join("repository.json");
        let image = root.join("sessions/.trash/main-retired.asif");
        std::fs::create_dir_all(image.parent().expect("trash")).expect("trash");
        std::fs::write(&image, b"image").expect("retained image");
        std::fs::write(&binding, b"binding").expect("binding");

        let error = clean_terminal_project_storage(&root, &binding)
            .expect_err("unreclaimed image must block binding cleanup");
        assert_eq!(error.code.as_str(), "integrity");
        assert_eq!(std::fs::read(&image).expect("image preserved"), b"image");
        assert!(binding.is_file());
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    /// A store root and a mount root side by side under one scratch directory, the way the
    /// host lays them out, holding one project each.
    fn project(label: &str) -> (std::path::PathBuf, ProjectPaths) {
        let root = root(label);
        let repo = RepoId::parse("example-org/example-app").expect("repo id");
        let paths = ProjectPaths::with_mount_root(root.join("store"), root.join("mnt"), &repo)
            .expect("project paths");
        std::fs::create_dir_all(&paths.project_root).expect("project root");
        std::fs::create_dir_all(paths.mount_root.join(".staging/main-orphan")).expect("staging");
        std::fs::create_dir_all(paths.mount_root.join("raven")).expect("retired mountpoint");
        for file in [
            paths.project_root.join("lifecycle-intents.json"),
            paths.project_root.join("deletion-log.jsonl"),
            paths.slot_bindings.clone(),
        ] {
            std::fs::write(file, b"controller state").expect("controller state");
        }
        (root, paths)
    }

    #[test]
    fn an_unbound_project_leaves_no_store_or_mount_directory() {
        let (root, paths) = project("unbound");

        remove_unbound_project_state(&paths).expect("unbound state removal");
        assert!(!paths.project_root.exists(), "store directory remains");
        assert!(!paths.mount_root.exists(), "mount tree remains");
        assert!(
            !paths.project_root.parent().expect("owner").exists(),
            "empty owner directory remains in the store"
        );
        assert!(
            !paths.mount_root.parent().expect("owner").exists(),
            "empty owner directory remains under the mount root"
        );
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn an_unbound_project_keeps_what_its_user_wrote() {
        let (root, paths) = project("user-files");
        std::fs::write(&paths.policy, b"policy").expect("policy");
        std::fs::write(&paths.waivers, b"waivers").expect("waivers");
        std::fs::create_dir_all(&paths.quarantine).expect("quarantine");
        std::fs::write(paths.quarantine.join(".env"), b"moved aside").expect("quarantined");
        let sibling = paths
            .project_root
            .parent()
            .expect("owner")
            .join("other-app");
        std::fs::create_dir_all(&sibling).expect("sibling project");

        remove_unbound_project_state(&paths).expect("unbound state removal");
        assert_eq!(std::fs::read(&paths.policy).expect("policy"), b"policy");
        assert_eq!(std::fs::read(&paths.waivers).expect("waivers"), b"waivers");
        assert_eq!(
            std::fs::read(paths.quarantine.join(".env")).expect("quarantined"),
            b"moved aside"
        );
        for controller in [
            paths.project_root.join("lifecycle-intents.json"),
            paths.project_root.join("deletion-log.jsonl"),
            paths.slot_bindings.clone(),
        ] {
            assert!(!controller.exists(), "{} remains", controller.display());
        }
        assert!(sibling.is_dir(), "another project's directory went too");
        assert!(!paths.mount_root.exists(), "mount tree remains");
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    /// The unbinding removes this state while the binding still stands; a process that dies right
    /// after must leave the project reopenable from its checkout, and main's image is already gone.
    #[test]
    fn unbound_state_removal_keeps_what_reopens_a_still_bound_project() {
        let (root, paths) = project("still-bound");
        std::fs::write(&paths.repository_binding, b"binding").expect("binding");
        std::fs::write(&paths.checkout_root, b"checkout root").expect("checkout root");

        remove_unbound_project_state(&paths).expect("unbound state removal");
        assert!(paths.repository_binding.is_file(), "binding went");
        assert_eq!(
            std::fs::read(&paths.checkout_root).expect("checkout root record"),
            b"checkout root"
        );
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn an_unbound_mount_tree_holding_a_file_is_reported_and_kept_whole() {
        let (root, paths) = project("mount-file");
        let stray = paths.mount_root.join("raven/notes.txt");
        std::fs::write(&stray, b"left in a mountpoint").expect("stray file");

        let error = remove_unbound_project_state(&paths)
            .expect_err("a file under the mount tree is not an empty mountpoint");
        assert_eq!(error.code.as_str(), "integrity");
        assert!(error.message.contains("notes.txt"), "{}", error.message);
        assert_eq!(
            std::fs::read(&stray).expect("stray kept"),
            b"left in a mountpoint"
        );
        assert!(
            paths.mount_root.join(".staging/main-orphan").is_dir(),
            "the refused tree was partly removed"
        );
        assert!(
            !paths.project_root.exists(),
            "the store side must go before the mount tree is refused"
        );
        std::fs::remove_dir_all(root).expect("cleanup");
    }

    #[test]
    fn anything_the_unbinding_could_not_delete_refuses_before_anything_is_removed() {
        for (label, artifact) in [
            ("bundle", "sessions/.trash/raven-0123.bundle"),
            ("session", "sessions/raven.asif.grants.json"),
            ("staged", ".staging/raven-4567.asif"),
            ("checkpoint", "checkpoints/raven/before.asif"),
            ("temp", "tmp/raven/scratch"),
        ] {
            let (root, paths) = project(label);
            let lock = paths.sessions.join("raven.asif.lock");
            let artifact = paths.project_root.join(artifact);
            std::fs::create_dir_all(artifact.parent().expect("parent")).expect("parent");
            std::fs::create_dir_all(&paths.sessions).expect("sessions");
            std::fs::write(&lock, b"").expect("lock");
            std::fs::write(&artifact, b"retained").expect("artifact");

            let error = require_terminal_storage(&paths.project_root)
                .expect_err("a retained artifact must refuse the restore");
            assert_eq!(error.code.as_str(), "conflict", "{label}");
            assert!(
                error
                    .message
                    .contains(artifact.file_name().expect("name").to_str().expect("utf-8")),
                "{label}: {}",
                error.message
            );
            assert!(lock.is_file(), "{label}: the check removed a lock");
            assert_eq!(
                std::fs::read(&artifact).expect("artifact kept"),
                b"retained"
            );

            std::fs::remove_file(&artifact).expect("artifact removed by its owner");
            require_terminal_storage(&paths.project_root)
                .expect("locks and empty directories pass");
            assert!(lock.is_file(), "{label}: the check removed a lock");
            std::fs::remove_dir_all(root).expect("cleanup");
        }
    }

    #[test]
    fn mains_own_checkpoints_and_temp_dir_do_not_refuse_its_restore() {
        let (root, paths) = project("main-checkpoint");
        let checkpoint = paths.checkpoints.join("main/before.asif");
        let scratch = paths.exec_temp.join("main/scratch");
        for owned in [&checkpoint, &scratch] {
            std::fs::create_dir_all(owned.parent().expect("parent")).expect("parent");
            std::fs::write(owned, b"main's own").expect("owned artifact");
        }

        require_terminal_storage(&paths.project_root)
            .expect("main's retirement reclaims its own checkpoints and temp dir");
        for owned in [&checkpoint, &scratch] {
            assert_eq!(std::fs::read(owned).expect("kept"), b"main's own");
        }
        std::fs::remove_dir_all(root).expect("cleanup");
    }
}

#[cfg(all(test, target_os = "macos"))]
mod adopt_secret_policy_tests {
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::enforce_adopt_secret_policy;
    use crate::error::ErrorCode;
    use crate::secrets::WAIVER_EXAMPLE;

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    /// A throwaway repository tree plus the controller-owned paths the policy reads.
    struct PolicyTree(PathBuf);

    impl PolicyTree {
        fn new(name: &str) -> Self {
            let sequence = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
            let root = std::env::temp_dir().join(format!(
                "cowshed-adopt-policy-{name}-{}-{sequence}",
                std::process::id()
            ));
            std::fs::create_dir_all(&root).expect("temporary policy tree is created");
            Self(root)
        }

        fn path(&self) -> &Path {
            &self.0
        }

        fn waivers_path(&self) -> PathBuf {
            self.0.join("waivers.json")
        }

        fn write(&self, relative: &str, contents: &str) {
            let path = self.0.join(relative);
            std::fs::create_dir_all(path.parent().expect("parent exists"))
                .expect("fixture parent is created");
            std::fs::write(path, contents).expect("fixture is written");
        }

        fn write_waivers(&self, contents: &str) {
            std::fs::write(self.waivers_path(), contents).expect("waivers file is written");
        }
    }

    impl Drop for PolicyTree {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }

    async fn policy(tree: &PolicyTree, quarantine: bool) -> Result<(), crate::error::CowshedError> {
        enforce_adopt_secret_policy(
            tree.path().to_path_buf(),
            tree.waivers_path(),
            tree.path().join("quarantine"),
            quarantine,
        )
        .await
    }

    #[tokio::test]
    async fn clean_tree_without_a_waivers_file_is_accepted() {
        let tree = PolicyTree::new("clean");
        policy(&tree, false)
            .await
            .expect("no findings and no waivers file means no refusal");
    }

    #[tokio::test]
    async fn findings_refusal_prints_the_complete_waiver_contract() {
        let tree = PolicyTree::new("refusal");
        tree.write(".env.local", "DATABASE_PASSWORD=hunter2");

        let error = policy(&tree, false)
            .await
            .expect_err("findings must refuse adoption without a waiver");
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(error.message.contains(".env.local"), "{}", error.message);
        for expected in [
            tree.waivers_path().display().to_string(),
            WAIVER_EXAMPLE.to_owned(),
            "exact repository-relative path".to_owned(),
            "non-empty reason".to_owned(),
            "can never hold live credentials".to_owned(),
            "developer-local".to_owned(),
            "retained for audit".to_owned(),
            "--quarantine".to_owned(),
        ] {
            assert!(
                error.hint.contains(&expected),
                "hint must contain {expected:?}: {}",
                error.hint
            );
        }
    }

    #[tokio::test]
    async fn valid_reasoned_waiver_suppresses_blocking() {
        let tree = PolicyTree::new("waived");
        tree.write(".env.local", "DATABASE_PASSWORD=hunter2");
        tree.write_waivers(
            r#"[{"path": ".env.local", "reason": "intentionally committed synthetic detector fixture"}]"#,
        );

        policy(&tree, false)
            .await
            .expect("an exact, reasoned waiver unblocks adoption");
    }

    #[tokio::test]
    async fn malformed_waivers_file_fails_closed_with_the_contract() {
        let tree = PolicyTree::new("malformed");
        tree.write(".env.local", "DATABASE_PASSWORD=hunter2");
        tree.write_waivers("{not json");

        let error = policy(&tree, false)
            .await
            .expect_err("a malformed waivers file must fail closed");
        assert_eq!(error.code, ErrorCode::Integrity);
        assert!(
            error
                .message
                .contains(&tree.waivers_path().display().to_string()),
            "{}",
            error.message
        );
        assert!(error.hint.contains(WAIVER_EXAMPLE), "{}", error.hint);
        assert!(error.hint.contains("delete it"), "{}", error.hint);
    }

    #[tokio::test]
    async fn empty_reason_waiver_names_the_file_and_the_contract() {
        let tree = PolicyTree::new("blank-reason");
        tree.write(".env.local", "DATABASE_PASSWORD=hunter2");
        tree.write_waivers(r#"[{"path": ".env.local", "reason": "   "}]"#);

        let error = policy(&tree, false)
            .await
            .expect_err("a blank reason must not waive anything");
        assert_eq!(error.code, ErrorCode::Integrity);
        assert!(
            error.message.contains("reason is required"),
            "{}",
            error.message
        );
        assert!(
            error
                .hint
                .contains(&tree.waivers_path().display().to_string()),
            "{}",
            error.hint
        );
        assert!(error.hint.contains(WAIVER_EXAMPLE), "{}", error.hint);
    }

    #[tokio::test]
    async fn duplicate_waiver_names_the_file_and_the_contract() {
        let tree = PolicyTree::new("duplicate");
        tree.write(".env.local", "DATABASE_PASSWORD=hunter2");
        tree.write_waivers(
            r#"[{"path": ".env.local", "reason": "one"}, {"path": ".env.local", "reason": "two"}]"#,
        );

        let error = policy(&tree, false)
            .await
            .expect_err("a duplicate waiver entry must be refused");
        assert_eq!(error.code, ErrorCode::Integrity);
        assert!(
            error.message.contains("duplicate waiver"),
            "{}",
            error.message
        );
        assert!(
            error
                .hint
                .contains(&tree.waivers_path().display().to_string()),
            "{}",
            error.hint
        );
        assert!(error.hint.contains(WAIVER_EXAMPLE), "{}", error.hint);
    }
}

#[cfg(test)]
mod exec_admission_tests {
    use super::exec_command;
    use crate::api::dto::ExecCommand;
    use crate::api::operations::ExecParams;
    use crate::error::ErrorCode;

    fn request(command: serde_json::Value) -> ExecParams {
        let mut wire = serde_json::json!({
            "repoId": "acme/widget",
            "workspace": "widget",
            "workspaceIncarnation": "0123456789abcdef0123456789abcdef",
            "session": null,
            "cwd": null,
            "mode": "readWrite",
            "env": {},
            "trace": null,
            "stdin": {"kind": "empty"},
            "stdoutCopy": null,
            "stderrCopy": null,
        });
        wire.as_object_mut()
            .expect("an object")
            .extend(command.as_object().expect("command fields").clone());
        serde_json::from_value(wire).expect("the request decodes")
    }

    #[test]
    fn the_controller_admits_a_script_exec_request() {
        let wire = request(serde_json::json!({
            "script": {"parts": ["echo ", ""], "values": [{"word": "a b"}]},
        }));
        let command = exec_command(wire.argv, wire.script).expect("a script is admitted");
        let ExecCommand::Script(script) = command else {
            panic!("a script request became {command:?}");
        };
        assert_eq!(script.parts(), ["echo ", ""]);
    }

    #[test]
    fn an_exec_request_carries_exactly_one_command() {
        let argv = serde_json::json!([{"encoding": "utf8", "data": "true"}]);
        let script = serde_json::json!({"parts": ["true"], "values": []});
        for fields in [
            serde_json::json!({"argv": argv, "script": script}),
            serde_json::json!({}),
            serde_json::json!({"argv": null, "script": null}),
        ] {
            let wire = request(fields.clone());
            let error = exec_command(wire.argv, wire.script).expect_err("refused");
            assert_eq!(error.code, ErrorCode::Usage, "{fields}: {error:?}");
        }
    }
}

#[cfg(all(test, target_os = "macos"))]
mod session_reopen_tests {
    use super::super::supervisor::{SessionToken, WorkspaceAuthoritySnapshot};
    use super::superseded_session;
    use crate::metadata::{WorkspaceIncarnation, WorkspaceName};
    use crate::repository::RepoId;

    fn token(identity: u64) -> SessionToken {
        SessionToken::remote(
            &WorkspaceAuthoritySnapshot {
                repo_id: RepoId::parse("acme/widget").expect("repo"),
                workspace: WorkspaceName::new("task").expect("workspace"),
                workspace_incarnation: WorkspaceIncarnation::new(
                    "0123456789abcdef0123456789abcdef",
                )
                .expect("incarnation"),
                grant_revision: 1,
                lifecycle_revision: 1,
            },
            identity,
            Some("build".to_owned()),
        )
    }

    /// Reopening a named session the supervisor still holds answers that session's own identity:
    /// the token it replaces is the same session and must stay open, or every second command of
    /// the session meets "session identity is closed or stale". A token of another identity — a
    /// session closed and reopened under the name — is superseded and closed.
    #[test]
    fn a_reopened_named_session_is_never_closed_as_its_own_predecessor() {
        assert_eq!(superseded_session(token(7), 7), None);
        assert_eq!(superseded_session(token(7), 8), Some(token(7)));
    }
}
