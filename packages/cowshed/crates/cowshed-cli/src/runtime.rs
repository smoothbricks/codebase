use crate::args::{
    AdoptArgs, CacheCommand, Cli, Command, ExecArgs, GrantTarget, MoveDestination,
    ProjectDiscovery, RevokeArgs, RevokeSelection, StdinSource as CliStdinSource,
};
use crate::forget;
use crate::gateway_service;
use crate::mount_main;
use crate::output::Output;
use crate::probe;
use async_trait::async_trait;
use base64::Engine as _;
use bytes::Bytes;
use cowshed_apfs::SystemCommandRunner;
use cowshed_apfs::image::{IMAGE_EXTENSION, ImageCapacity};
pub use cowshed_core::api::ProjectWorkspaces;
use cowshed_core::api::operations::{ListLanding, ReAdoption};
use cowshed_core::api::server::{ConnectionAuthority, serve_controller_connection};
use cowshed_core::api::{
    AbandonedPreservation, AbandonedWork, AdoptOptions, AttachOptions, BranchLandings, BranchName,
    CheckpointInfo, CheckpointOptions, CheckpointResult, CommandArg, Coordinator, DefragmentResult,
    DoctorReport, EmptyResult, ExecRequest, ExpectedRefHead, Finding, FindingSeverity, ForkOptions,
    GatewayStatus, GcOptions, GcReason, GcReport, GitOid, GrantDelta, GrantSet, JobInfo, JobStream,
    LandOptions, LandReport, LandingCommits, MountResult, OutputPublication, ProjectGrantDelta,
    ProjectGrants, PublicationPolicy, PushOptions, PushReport, RebaseBuildVolume, RebaseOptions,
    RebaseReport, RefLockPath, RekeyResult, RemoveOptions, RemoveProjectOptions,
    RemoveProjectReport, RemoveReport, ReseedResult, ResizeResult, ResizeVolume, RevisionTarget,
    RunSandboxMode, SccacheStatus, StdinSource as CoreStdinSource, UnlandedPaths,
    UnprovenWorkspace, UtcTimestamp, WorkspaceInfo, WorkspacePath, WorkspaceState,
    validate_command_argv,
};
use cowshed_core::git::GitRepository;
use cowshed_core::metadata::{
    DetachedWorkspaceMetadata, WorkspaceIncarnation, WorkspaceName, WorkspaceRole,
};
use cowshed_core::metadata::{EgressMode, EgressRule};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::supervisor_socket::JobHold;
use cowshed_core::runtime::{ProjectRuntime, RecoveryScope};
use cowshed_core::storage::apfs::native::MacOsApfsExecutionHost;
use cowshed_core::storage::apfs::{ApfsSubstrate, ApfsSubstrateConfig, DEFAULT_IMAGE_CAPACITY};
use cowshed_core::storage::bootstrap::{
    CachesVolume, CanonicalRoots, HostAction, HostSetupPlan, HostSetupReport, MOUNT_SERVICE_PLIST,
    RETIRED_CACHES_MOUNTPOINT, VOLUME_MARKER_FILE, ValidatedHostStorage, VolumeRole, VolumeState,
    execute_host_setup, plan_host_setup,
};
use cowshed_core::storage::host_config::{
    RETIRED_LAYOUT_HINT, RetiredLayoutRecord, retired_layout_paths,
};
use cowshed_core::storage::job_artifact::{ArtifactConfig, repair_workspace_record_sequences};
use cowshed_core::storage::lifecycle::{DerivedWorkspace, MountIntent, MountState, Pin, Substrate};
use cowshed_core::storage::recovery::KeptAbandonBundles;
use cowshed_core::storage::{StorageLayout, discover_session_images};
use cowshed_core::{
    Adopted, AdoptedProject, CowshedError, ErrorCode, GatewayInventoryError, GatewayStoreBackend,
    JobHandle, NativeGatewayInventory, Result, UnreachableMain, validate_existing_host_storage,
};
use cowshed_gateway_types::STORE_ROOT;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Write};
use std::num::NonZeroU16;
use std::os::fd::OwnedFd;
use std::os::unix::ffi::OsStrExt;
use std::path::{Path, PathBuf};
use std::time::Duration;
use tokio::io::AsyncRead;
use tokio::task::JoinHandle;

pub struct ExecCommand {
    pub workspace: String,
    pub request: ExecRequest,
    pub session: Option<String>,
    pub background: bool,
    /// How long to wait for the command (`--timeout`); `None` waits until it ends.
    pub timeout: Option<Duration>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecPresentation {
    Raw,
    Control,
}

pub struct ExecResult {
    pub info: JobInfo,
    pub end: ExecEnd,
}

/// How one `cowshed exec` left its job.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ExecEnd {
    /// The job ended while the exec relayed it; its end is the command's.
    Finished,
    /// The caller asked not to wait (`--background`).
    Backgrounded,
    /// The job still ran when the caller's `--timeout` passed. It keeps running, and the exec
    /// has no output or status of the command's to give.
    StillRunning { after: Duration },
}

#[async_trait]
pub trait CliService: Send {
    /// Main, and what the replacement kept when `options` re-adopted it.
    async fn adopt(&mut self, options: AdoptOptions)
    -> Result<(WorkspaceInfo, Option<ReAdoption>)>;
    async fn fork(
        &mut self,
        workspace: &str,
        source: Option<&str>,
        options: ForkOptions,
    ) -> Result<WorkspaceInfo>;
    async fn rename(&mut self, source: &str, destination: &str) -> Result<WorkspaceInfo>;
    async fn move_checkout(&mut self, destination: &Path) -> Result<WorkspaceInfo>;
    async fn change_repo_id(&mut self, _repo_id: RepoId) -> Result<WorkspaceInfo> {
        Err(CowshedError::internal(
            "repository identity changes are unavailable for this CLI service",
        ))
    }
    async fn checkpoint(&mut self, workspace: &str, options: CheckpointOptions) -> Result<String>;
    async fn restore(&mut self, workspace: &str, label: &str) -> Result<WorkspaceInfo>;
    async fn workspace_at(&mut self, path: PathBuf) -> Result<WorkspaceInfo>;
    /// The project's rows, and each workspace whose mount is unproven, which has none.
    async fn list(&mut self, landing: Option<ListLanding>) -> Result<ProjectWorkspaces>;
    /// The name of the workspace that currently occupies build slot `slot`.
    async fn workspace_in_slot(&mut self, slot: u32) -> Result<String>;
    async fn list_all(&mut self) -> Result<Vec<ProjectWorkspaces>> {
        Err(CowshedError::internal(
            "CLI service does not support store-wide workspace discovery",
        ))
    }
    /// What each project's trash keeps of `rm --abandon`, which `ls --all` names after its table.
    async fn kept_abandon_bundles(&mut self) -> Result<Vec<(RepoId, KeptAbandonBundles)>> {
        Ok(Vec::new())
    }
    async fn other_adopted_project_count(&mut self) -> Result<usize> {
        Ok(0)
    }
    async fn path(&mut self, workspace: &str, no_attach: bool) -> Result<WorkspaceInfo>;
    async fn remove(&mut self, workspace: &str, options: RemoveOptions) -> Result<RemoveReport>;
    async fn remove_project(
        &mut self,
        options: RemoveProjectOptions,
    ) -> Result<RemoveProjectReport>;
    async fn attach(&mut self, workspace: &str, options: AttachOptions) -> Result<WorkspaceInfo>;
    async fn detach(&mut self, workspace: &str) -> Result<()>;
    async fn resize(
        &mut self,
        workspace: &str,
        capacity: &str,
        volume: ResizeVolume,
    ) -> Result<ResizeResult>;
    async fn defragment(&mut self, workspace: &str) -> Result<DefragmentResult>;
    async fn reseed(&mut self, workspace: &str, from: &str, dry_run: bool) -> Result<ReseedResult>;
    /// Delete the workspace's Nx state on its build volume (`coordinator.clearNxState`).
    async fn clear_nx_state(
        &mut self,
        workspace: &str,
    ) -> Result<cowshed_core::api::NxStateClearResult>;
    /// Ask the gateway to clear the project's Nx accepted-task store.
    async fn clear_nx_store(&mut self) -> Result<cowshed_core::api::NxStoreClearResult>;
    async fn rekey(&mut self, workspace: &str) -> Result<RekeyResult>;
    async fn doctor(&mut self) -> Result<DoctorReport>;
    async fn gc(&mut self, options: GcOptions) -> Result<GcReport>;
    /// Move one recovery image out of the store (`coordinator.recoverImage`).
    async fn recover_image(
        &mut self,
        image: &Path,
        to: &Path,
    ) -> Result<cowshed_core::api::RecoveredImage>;
    async fn grants(&mut self, workspace: &str) -> Result<GrantSet>;
    async fn grant(&mut self, workspace: &str, delta: GrantDelta) -> Result<GrantSet>;
    async fn revoke(&mut self, workspace: &str, delta: GrantDelta) -> Result<GrantSet>;
    async fn project_grants(&mut self) -> Result<ProjectGrants>;
    async fn grant_project(&mut self, delta: ProjectGrantDelta) -> Result<ProjectGrants>;
    async fn revoke_project(&mut self, delta: ProjectGrantDelta) -> Result<ProjectGrants>;
    async fn push(&mut self, workspace: &str, options: PushOptions) -> Result<PushReport>;
    /// `into` names what the unit lands into (main when `None`); the service resolves it to a
    /// workspace reference once, before the call.
    async fn rebase(
        &mut self,
        workspace: &str,
        into: Option<&str>,
        options: RebaseOptions,
    ) -> Result<RebaseReport>;
    async fn land(
        &mut self,
        workspace: &str,
        into: Option<&str>,
        options: LandOptions,
    ) -> Result<LandReport>;
    async fn exec(
        &mut self,
        command: ExecCommand,
        presentation: ExecPresentation,
        stdout: &mut (dyn Write + Send),
        stderr: &mut (dyn Write + Send),
    ) -> Result<ExecResult>;
    async fn reconcile_gateway(&mut self) -> Result<()> {
        Ok(())
    }
    async fn shutdown(self) -> Result<()>
    where
        Self: Sized;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RuntimeOpenMode {
    Provision,
    ExistingOnly,
    /// `mv … --repo-id` alone: opens with the binding's remote check relaxed, because the verb is
    /// about to supersede the recorded identity a moved remote would otherwise contradict.
    IdentityChange,
}

/// Whether the verb changes workspace or lifecycle state, or runs a job, through workspace
/// supervisors — which only the host daemon starts, and which it starts only for a cowshed of
/// its own build.
///
/// Exhaustive, so a verb added later is placed on purpose. The ones that answer `false` must keep
/// working while the two disagree: `doctor` is how the disagreement is diagnosed, `setup` and
/// `gateway` are what ends it, the rest read, or never reach a supervisor.
fn needs_daemon_of_this_build(command: &Command) -> bool {
    match command {
        Command::Adopt(_)
        | Command::Fork(_)
        | Command::Move(_)
        | Command::Checkpoint(_)
        | Command::Restore(_)
        | Command::Path(_)
        | Command::Exec(_)
        | Command::Grant(_)
        | Command::Revoke(_)
        | Command::Remove(_)
        // A retirement fences opens and drains supervisors, and only a daemon of a build that
        // knows the record enforces that fence (06_cli.md, "Project retirement").
        | Command::Forget(_)
        | Command::Attach(_)
        | Command::Detach(_)
        | Command::Resize(_)
        | Command::Defrag(_)
        | Command::Reseed(_)
        | Command::Cache(_)
        | Command::Rekey(_)
        | Command::Gc(_)
        | Command::Push(_)
        | Command::Rebase(_) => true,
        Command::Land(args) => !args.preflight,
        Command::List(_)
        | Command::BuildState
        | Command::Mount(_)
        | Command::Setup(_)
        | Command::Gateway(_)
        | Command::DiskCommand(_)
        | Command::LandTurn(_)
        | Command::SubtreeRoot(_)
        | Command::RetireDescendants(_)
        | Command::Lock(_)
        | Command::LockHolders(_)
        | Command::Hold(_)
        | Command::TreeHolders(_)
        | Command::FdHolders(_)
        | Command::Controller
        | Command::Host(_)
        | Command::Credential(_)
        | Command::Identity(_)
        | Command::Sccache(_)
        | Command::Skill(_)
        | Command::Version
        | Command::Help(_)
        | Command::Doctor(_) => false,
    }
}

/// Refuse, before `command` changes anything, a host daemon that runs another build than this
/// cowshed.
///
/// The daemon refuses another build's requests by name at the first supervisor a verb asks for.
/// By then a removal has stopped its workspace's supervisor and journaled its intent, and the
/// refusal arrives again on the path that would put that back — "state restoration also failed",
/// a workspace half removed. Asking first leaves the host as it was, with the refusal that names
/// `cowshed setup`.
pub async fn require_daemon_of_this_build(command: &Command) -> Result<()> {
    require_daemon_of_this_build_at(Path::new(cowshed_gateway_types::STORE_ROOT), command).await
}

async fn require_daemon_of_this_build_at(store_root: &Path, command: &Command) -> Result<()> {
    if needs_daemon_of_this_build(command) {
        cowshed_core::runtime::supervisor_manager::require_same_build(store_root).await
    } else {
        Ok(())
    }
}

fn runtime_open_mode(command: &Command) -> RuntimeOpenMode {
    match command {
        Command::Adopt(_) => RuntimeOpenMode::Provision,
        Command::Move(args) if matches!(args.destination, MoveDestination::RepoId(_)) => {
            RuntimeOpenMode::IdentityChange
        }
        Command::Fork(_)
        | Command::Move(_)
        | Command::Checkpoint(_)
        | Command::Restore(_)
        | Command::List(_)
        | Command::Path(_)
        | Command::BuildState
        | Command::Exec(_)
        | Command::Grant(_)
        | Command::Revoke(_)
        | Command::Remove(_)
        | Command::Forget(_)
        | Command::Attach(_)
        | Command::Detach(_)
        | Command::Resize(_)
        | Command::Defrag(_)
        | Command::Reseed(_)
        | Command::Cache(_)
        | Command::Rekey(_)
        | Command::Gc(_)
        | Command::Push(_)
        | Command::Rebase(_)
        | Command::Land(_)
        | Command::Mount(_)
        | Command::Setup(_)
        | Command::Gateway(_)
        | Command::DiskCommand(_)
        | Command::LandTurn(_)
        | Command::SubtreeRoot(_)
        | Command::RetireDescendants(_)
        | Command::Lock(_)
        | Command::LockHolders(_)
        | Command::Hold(_)
        | Command::TreeHolders(_)
        | Command::FdHolders(_)
        | Command::Controller
        | Command::Host(_)
        | Command::Credential(_)
        | Command::Identity(_)
        | Command::Sccache(_)
        | Command::Skill(_)
        | Command::Version
        | Command::Help(_)
        | Command::Doctor(_) => RuntimeOpenMode::ExistingOnly,
    }
}

fn runtime_open_repo_id(command: &Command) -> Result<Option<RepoId>> {
    match command {
        Command::Adopt(args) => args.repo_id.as_deref().map(os_repo_id_ref).transpose(),
        _ => Ok(None),
    }
}

/// The primary identity of the project an explicit `adopt --re-adopt` with no `--repo-id` replaces.
///
/// The checkout's in-image marker, the controller's Active main sidecar and the owning binding must
/// agree on it, so no remote chooses between several identities (02_workspaces.md, "Re-adoption"
/// step 1, explicit `--re-adopt` only). The opened project still reconciles
/// its recorded remotes with Git. `None` when no main marker sits at `checkout` or its owner holds
/// no RAW `main.dmg`, which leaves the ordinary derivation from the remotes in charge.
async fn re_adoption_primary(checkout: &Path) -> Result<Option<RepoId>> {
    NativeGatewayInventory::new(host_storage().await?)
        .re_adoption_primary_for_checkout(checkout)
        .await
        .map_err(project_inventory_error)
}

/// Whether this open binds the checkout's recorded primary instead of deriving one from the
/// remotes: only for a fresh `adopt --re-adopt` that names no repository. A resumed re-adoption is
/// named by its journal (`resolved`), never by the marker.
fn binds_re_adoption_primary(command: &Command, resuming_re_adoption: bool) -> bool {
    !resuming_re_adoption
        && matches!(command, Command::Adopt(args) if args.re_adopt && args.repo_id.is_none())
}

/// The workspaces whose unfinished lifecycle work this verb's open may finish (see
/// [`RecoveryScope`]): the ones it names. A name the verb itself will reject cannot name a
/// journaled intent, so it adds nothing; a removal alone refuses one before opening, because
/// its scope is the name. `gc` is store maintenance and finishes residue everywhere. `doctor`
/// never mutates: its open finishes nothing and it reports what another open would finish; only
/// `doctor --repair` opens the way other mutating verbs do. `reseed` inspects too: its operation
/// must refuse unfinished create/fork/adopt work, never finish it before making that decision.
/// `defrag` likewise opens without replay: it must be able to rewrite a pending clone whose
/// physical-generation extent gate is the reason publication cannot finish. `land --preflight`
/// inspects as well: it reads land's fences and must finish nothing.
fn runtime_recovery_scope(command: &Command) -> Result<RecoveryScope> {
    fn named<'a>(names: impl IntoIterator<Item = Option<&'a str>>) -> RecoveryScope {
        RecoveryScope::Workspaces(
            names
                .into_iter()
                .flatten()
                .filter_map(|name| WorkspaceName::new(name).ok())
                .collect(),
        )
    }
    Ok(match command {
        Command::Remove(args) => WorkspaceName::new(&args.workspace)
            .map(RecoveryScope::Removal)
            .map_err(|error| usage(error.to_string(), "use a valid workspace name"))?,
        Command::Gc(_) => RecoveryScope::Store,
        Command::Doctor(args) if !args.repair => RecoveryScope::Inspect,
        Command::Reseed(_) | Command::Defrag(_) => RecoveryScope::Inspect,
        Command::Land(args) if args.preflight => RecoveryScope::Inspect,
        Command::Fork(args) => named([args.source.as_deref(), Some(args.destination.as_str())]),
        Command::Move(args) => match &args.destination {
            MoveDestination::Workspace(destination) => {
                named([Some(args.source.as_str()), Some(destination.as_str())])
            }
            MoveDestination::Checkout(_) | MoveDestination::RepoId(_) => {
                named([Some(args.source.as_str())])
            }
        },
        Command::Restore(args) => named([Some(args.workspace.as_str())]),
        Command::Exec(args) => named([Some(args.workspace.as_str())]),
        Command::Grant(args) => match &args.target {
            GrantTarget::Workspace(workspace) => named([Some(workspace.as_str())]),
            GrantTarget::Project => named([]),
        },
        Command::Revoke(args) => match &args.target {
            GrantTarget::Workspace(workspace) => named([Some(workspace.as_str())]),
            GrantTarget::Project => named([]),
        },
        Command::Resize(args) => named([Some(args.workspace.as_str())]),
        Command::Rekey(args) => named([Some(args.workspace.as_str())]),
        Command::Land(args) => named([Some(args.workspace.as_str())]),
        Command::Checkpoint(args) => named([args.workspace.as_deref()]),
        Command::Path(args) => named([args.workspace.as_deref()]),
        Command::Attach(args) => named([args.workspace.as_deref()]),
        Command::Detach(args) => named([args.workspace.as_deref()]),
        Command::Push(args) => named([args.workspace.as_deref()]),
        Command::Rebase(args) => named([args.workspace.as_deref()]),
        Command::Cache(CacheCommand::Clear { workspace }) => named([workspace.as_deref()]),
        Command::Adopt(_)
        | Command::List(_)
        | Command::BuildState
        | Command::Doctor(_)
        | Command::Credential(_)
        | Command::Identity(_)
        | Command::Mount(_)
        | Command::Forget(_)
        | Command::Setup(_)
        | Command::Gateway(_)
        | Command::DiskCommand(_)
        | Command::LandTurn(_)
        | Command::SubtreeRoot(_)
        | Command::RetireDescendants(_)
        | Command::Lock(_)
        | Command::LockHolders(_)
        | Command::Hold(_)
        | Command::TreeHolders(_)
        | Command::FdHolders(_)
        | Command::Controller
        | Command::Host(_)
        | Command::Sccache(_)
        | Command::Skill(_)
        | Command::Version
        | Command::Help(_) => named([]),
    })
}

pub struct ActorBridge {
    coordinator: Option<Coordinator>,
    connection: Option<JoinHandle<Result<()>>>,
    runtime: Option<ProjectRuntime>,
    backend: GatewayStoreBackend,
}

impl ActorBridge {
    pub async fn open_for_adopt(
        project_root: &Path,
        requested_repo_id: Option<RepoId>,
    ) -> Result<Self> {
        let runtime = ProjectRuntime::open_for_adopt(
            project_root,
            requested_repo_id,
            gateway_service::runtime_control,
        )
        .await?;
        Self::from_runtime(project_root, runtime, GatewayStoreBackend::Native).await
    }

    /// Drive dispatch against caller-owned APFS images rather than machine-global storage.
    pub async fn open_for_adopt_at(
        project_root: &Path,
        requested_repo_id: Option<RepoId>,
        storage: ValidatedHostStorage,
    ) -> Result<Self> {
        let runtime = cowshed_os::timing::timed_async(
            "controller-open",
            "runtime",
            ProjectRuntime::open_for_adopt_at(
                project_root,
                requested_repo_id,
                storage,
                gateway_service::runtime_control,
            ),
        )
        .await?;
        Self::from_runtime(project_root, runtime, GatewayStoreBackend::Native).await
    }

    pub async fn open_existing_at(
        project_root: &Path,
        scope: RecoveryScope,
        storage: ValidatedHostStorage,
    ) -> Result<Self> {
        let runtime = ProjectRuntime::open_existing_at(
            project_root,
            scope,
            storage,
            gateway_service::runtime_control,
        )
        .await?;
        Self::from_runtime(project_root, runtime, GatewayStoreBackend::Native).await
    }

    /// Drive the same dispatch workflow against a caller-owned directory store.
    pub async fn open_directory_for_adopt_at(
        project_root: &Path,
        requested_repo_id: Option<RepoId>,
        storage: ValidatedHostStorage,
    ) -> Result<Self> {
        let runtime = ProjectRuntime::open_directory_for_adopt_at(
            project_root,
            requested_repo_id,
            storage,
            gateway_service::runtime_control,
        )
        .await?;
        Self::from_runtime(project_root, runtime, GatewayStoreBackend::Directory).await
    }

    /// Reopen a directory store without acquiring disk-image capabilities.
    pub async fn open_directory_existing_at(
        project_root: &Path,
        scope: RecoveryScope,
        storage: ValidatedHostStorage,
    ) -> Result<Self> {
        let runtime = ProjectRuntime::open_directory_existing_at(
            project_root,
            scope,
            storage,
            gateway_service::runtime_control,
        )
        .await?;
        Self::from_runtime(project_root, runtime, GatewayStoreBackend::Directory).await
    }

    pub async fn open_existing(project_root: &Path, scope: RecoveryScope) -> Result<Self> {
        let runtime =
            ProjectRuntime::open_existing(project_root, scope, gateway_service::runtime_control)
                .await?;
        Self::from_runtime(project_root, runtime, GatewayStoreBackend::Native).await
    }

    /// [`Self::open_existing`] for the identity-change verb alone (`cowshed mv … --repo-id`).
    pub async fn open_existing_for_identity_change(project_root: &Path) -> Result<Self> {
        let runtime = ProjectRuntime::open_existing_for_identity_change(
            project_root,
            gateway_service::runtime_control,
        )
        .await?;
        Self::from_runtime(project_root, runtime, GatewayStoreBackend::Native).await
    }

    async fn from_runtime(
        project_root: &Path,
        runtime: ProjectRuntime,
        backend: GatewayStoreBackend,
    ) -> Result<Self> {
        let (client, server) = match std::os::unix::net::UnixStream::pair() {
            Ok(pair) => pair,
            Err(error) => {
                let primary = CowshedError::environment_missing(
                    format!("could not create the in-process controller socket: {error}"),
                    "check the per-process file descriptor limit",
                );
                return Err(merge_primary(primary, runtime.shutdown().await.err()));
            }
        };
        let authority = ConnectionAuthority::Coordinator {
            repo_id: runtime.descriptor().repo_id.clone(),
        };
        let descriptor: OwnedFd = server.into();
        let connection = tokio::spawn(serve_controller_connection(
            descriptor,
            authority,
            runtime.router(),
        ));
        let client_descriptor: OwnedFd = client.into();

        let (cowshed, token) = match cowshed_os::timing::timed_async(
            "controller-open",
            "connect",
            cowshed_core::Cowshed::connect(client_descriptor),
        )
        .await
        {
            Ok(connection) => connection,
            Err(primary) => {
                return Err(cleanup_open_failure(primary, connection, runtime).await);
            }
        };
        let project = match cowshed_os::timing::timed_async(
            "controller-open",
            "project",
            cowshed.open(project_root),
        )
        .await
        {
            Ok(project) => project,
            Err(primary) => {
                drop(token);
                drop(cowshed);
                return Err(cleanup_open_failure(primary, connection, runtime).await);
            }
        };
        let coordinator = match cowshed.coordinator(&project, token) {
            Ok(coordinator) => coordinator,
            Err(primary) => {
                drop(project);
                drop(cowshed);
                return Err(cleanup_open_failure(primary, connection, runtime).await);
            }
        };
        drop(project);
        drop(cowshed);
        Ok(Self {
            coordinator: Some(coordinator),
            connection: Some(connection),
            runtime: Some(runtime),
            backend,
        })
    }

    fn coordinator(&self) -> Result<&Coordinator> {
        self.coordinator.as_ref().ok_or_else(|| {
            CowshedError::internal("the CLI controller bridge has already been shut down")
        })
    }

    fn repo_id(&self) -> Result<&RepoId> {
        self.runtime
            .as_ref()
            .map(|runtime| &runtime.descriptor().repo_id)
            .ok_or_else(|| {
                CowshedError::internal("the CLI project runtime has already been shut down")
            })
    }

    fn git_root(&self) -> Result<&Path> {
        self.runtime
            .as_ref()
            .map(|runtime| &*runtime.descriptor().git_root)
            .ok_or_else(|| {
                CowshedError::internal("the CLI project runtime has already been shut down")
            })
    }

    fn store_root(&self) -> Result<&Path> {
        self.runtime
            .as_ref()
            .map(|runtime| runtime.descriptor().storage.store())
            .ok_or_else(|| {
                CowshedError::internal("the CLI project runtime has already been shut down")
            })
    }

    async fn adopted_projects(&self) -> Result<Vec<AdoptedProject>> {
        adopted_projects().await
    }

    pub async fn shutdown(mut self) -> Result<()> {
        drop(self.coordinator.take());
        let connection_error = match self.connection.take() {
            Some(connection) => join_connection(connection).await.err(),
            None => None,
        };
        let runtime_error = match self.runtime.take() {
            Some(runtime) => runtime.shutdown().await.err(),
            None => None,
        };
        combine_teardown(connection_error, runtime_error)
    }
}

pub(crate) async fn host_storage() -> Result<ValidatedHostStorage> {
    let home = gateway_service::canonical_home()?;
    validate_existing_host_storage(&home).await
}

async fn host_inventory() -> Result<NativeGatewayInventory> {
    Ok(NativeGatewayInventory::new(host_storage().await?))
}

async fn adopted_projects() -> Result<Vec<AdoptedProject>> {
    host_inventory()
        .await?
        .adopted_projects()
        .await
        .map_err(project_inventory_error)
}

async fn doctor_projects() -> Result<(
    Vec<AdoptedProject>,
    Vec<UnreachableMain>,
    Vec<RepoId>,
    Vec<(RepoId, GatewayInventoryError)>,
)> {
    host_inventory()
        .await?
        .doctor_projects()
        .await
        .map_err(project_inventory_error)
}

/// Every adopted project's listing, sorted, measured for landing as `landing` asks: what
/// `ls --all` and `host.listProjects` both read (`landed` keeps only the provably landed rows).
pub(crate) async fn list_projects(landing: Option<ListLanding>) -> Result<Vec<ProjectWorkspaces>> {
    let mut projects = list_all_adopted_projects().await?;
    sort_project_listing(&mut projects);
    if let Some(landing) = landing {
        // These are the host's projects, so their Git runs for the host store.
        let roots = cowshed_core::storage::bootstrap::CanonicalRoots::global();
        let host_store = roots.shared_store();
        for project in &mut projects {
            cowshed_core::landing::annotate_listing(host_store, &mut project.workspaces).await;
            if landing == ListLanding::Landed {
                cowshed_core::landing::retain_fully_landed(&mut project.workspaces);
            }
        }
    }
    Ok(projects)
}

/// The root of the git checkout containing `start`, as `--project` resolves it.
pub(crate) async fn discover_project_root(start: &Path) -> Result<PathBuf> {
    // The CLI process serves the host store; its controller Git writes profiles there.
    GitRepository::discover(
        cowshed_core::storage::bootstrap::CanonicalRoots::global().shared_store(),
        start,
    )
    .await
    .map(|repository| repository.root().to_path_buf())
}

async fn list_all_adopted_projects() -> Result<Vec<ProjectWorkspaces>> {
    host_inventory()
        .await?
        .all_projects()
        .await
        .map_err(project_inventory_error)
}

async fn kept_abandon_bundles() -> Result<Vec<(RepoId, KeptAbandonBundles)>> {
    host_inventory()
        .await?
        .kept_abandon_bundles()
        .await
        .map_err(project_inventory_error)
}

#[async_trait]
impl CliService for ActorBridge {
    async fn adopt(
        &mut self,
        options: AdoptOptions,
    ) -> Result<(WorkspaceInfo, Option<ReAdoption>)> {
        let Adopted {
            workspace,
            re_adoption,
        } = self.coordinator()?.adopt(options).await?;
        Ok((workspace.into_info(), re_adoption))
    }

    async fn fork(
        &mut self,
        workspace: &str,
        source: Option<&str>,
        options: ForkOptions,
    ) -> Result<WorkspaceInfo> {
        Ok(self
            .coordinator()?
            .fork(workspace, source, options)
            .await?
            .into_info())
    }

    async fn rename(&mut self, source: &str, destination: &str) -> Result<WorkspaceInfo> {
        Ok(self
            .coordinator()?
            .rename(source, destination)
            .await?
            .into_info())
    }

    async fn move_checkout(&mut self, destination: &Path) -> Result<WorkspaceInfo> {
        Ok(self
            .coordinator()?
            .move_checkout(destination)
            .await?
            .into_info())
    }

    async fn change_repo_id(&mut self, repo_id: RepoId) -> Result<WorkspaceInfo> {
        Ok(self
            .coordinator()?
            .change_repo_id(&repo_id)
            .await?
            .into_info())
    }

    async fn checkpoint(&mut self, workspace: &str, options: CheckpointOptions) -> Result<String> {
        self.coordinator()?
            .worker(workspace)
            .await?
            .checkpoint(options)
            .await
    }

    async fn restore(&mut self, workspace: &str, label: &str) -> Result<WorkspaceInfo> {
        self.coordinator()?.restore(workspace, label).await?;
        self.coordinator()?
            .project()
            .workspace(workspace)
            .await?
            .refresh_info()
            .await
    }

    async fn workspace_at(&mut self, path: PathBuf) -> Result<WorkspaceInfo> {
        self.coordinator()?
            .project()
            .workspace_at(path)
            .await?
            .refresh_info()
            .await
    }

    async fn list(&mut self, landing: Option<ListLanding>) -> Result<ProjectWorkspaces> {
        let listing = self.coordinator()?.project().list(landing).await?;
        Ok(ProjectWorkspaces {
            repo_id: self.repo_id()?.clone(),
            workspaces: listing
                .workspaces
                .into_iter()
                .map(|workspace| workspace.into_info())
                .collect(),
            unproven: listing.unproven,
        })
    }

    async fn workspace_in_slot(&mut self, slot: u32) -> Result<String> {
        Ok(self
            .coordinator()?
            .project()
            .workspace_in_slot(slot)
            .await?
            .name()
            .to_string())
    }

    async fn list_all(&mut self) -> Result<Vec<ProjectWorkspaces>> {
        list_all_adopted_projects().await
    }

    async fn kept_abandon_bundles(&mut self) -> Result<Vec<(RepoId, KeptAbandonBundles)>> {
        kept_abandon_bundles().await
    }

    async fn other_adopted_project_count(&mut self) -> Result<usize> {
        let current_repo = self.repo_id()?.clone();
        Ok(self
            .adopted_projects()
            .await?
            .into_iter()
            .filter(|project| project.repo_id != current_repo)
            .count())
    }

    async fn path(&mut self, workspace: &str, no_attach: bool) -> Result<WorkspaceInfo> {
        let snapshot = self.coordinator()?.project().workspace(workspace).await?;
        if no_attach {
            return Ok(snapshot.into_info());
        }
        snapshot.attach(AttachOptions::default()).await?;
        snapshot.refresh_info().await
    }

    async fn remove(&mut self, workspace: &str, options: RemoveOptions) -> Result<RemoveReport> {
        self.coordinator()?.destroy(workspace, options).await
    }

    async fn remove_project(
        &mut self,
        options: RemoveProjectOptions,
    ) -> Result<RemoveProjectReport> {
        self.coordinator()?.remove_project(options).await
    }

    async fn attach(&mut self, workspace: &str, options: AttachOptions) -> Result<WorkspaceInfo> {
        let snapshot = self.coordinator()?.project().workspace(workspace).await?;
        snapshot.attach(options).await?;
        snapshot.refresh_info().await
    }

    async fn detach(&mut self, workspace: &str) -> Result<()> {
        self.coordinator()?.detach(workspace).await.map(|_| ())
    }

    async fn resize(
        &mut self,
        workspace: &str,
        capacity: &str,
        volume: ResizeVolume,
    ) -> Result<ResizeResult> {
        self.coordinator()?
            .resize(workspace, capacity, volume)
            .await
    }

    async fn defragment(&mut self, workspace: &str) -> Result<DefragmentResult> {
        self.coordinator()?.defragment(workspace).await
    }

    async fn reseed(&mut self, workspace: &str, from: &str, dry_run: bool) -> Result<ReseedResult> {
        self.coordinator()?.reseed(workspace, from, dry_run).await
    }

    async fn clear_nx_state(
        &mut self,
        workspace: &str,
    ) -> Result<cowshed_core::api::NxStateClearResult> {
        self.coordinator()?.clear_nx_state(workspace).await
    }

    async fn clear_nx_store(&mut self) -> Result<cowshed_core::api::NxStoreClearResult> {
        let descriptor = self
            .runtime
            .as_ref()
            .ok_or_else(|| {
                CowshedError::internal("the CLI project runtime has already been shut down")
            })?
            .descriptor();
        gateway_service::ControlSocket::at(descriptor.storage.control_socket())?
            .clear_nx_cache(&descriptor.repo_id)
            .await
    }

    async fn rekey(&mut self, workspace: &str) -> Result<RekeyResult> {
        self.coordinator()?.rekey(workspace).await
    }

    async fn doctor(&mut self) -> Result<DoctorReport> {
        self.coordinator()?.doctor().await
    }

    async fn gc(&mut self, options: GcOptions) -> Result<GcReport> {
        self.coordinator()?.gc(options).await
    }

    async fn recover_image(
        &mut self,
        image: &Path,
        to: &Path,
    ) -> Result<cowshed_core::api::RecoveredImage> {
        self.coordinator()?.recover_image(image, to).await
    }

    async fn grants(&mut self, workspace: &str) -> Result<GrantSet> {
        self.coordinator()?
            .project()
            .workspace(workspace)
            .await?
            .refresh_grants()
            .await
    }

    async fn grant(&mut self, workspace: &str, delta: GrantDelta) -> Result<GrantSet> {
        self.coordinator()?.grant(workspace, delta).await
    }

    async fn revoke(&mut self, workspace: &str, delta: GrantDelta) -> Result<GrantSet> {
        self.coordinator()?.revoke(workspace, delta).await
    }

    async fn project_grants(&mut self) -> Result<ProjectGrants> {
        self.coordinator()?.project_grants().await
    }

    async fn grant_project(&mut self, delta: ProjectGrantDelta) -> Result<ProjectGrants> {
        self.coordinator()?.grant_project(delta).await
    }

    async fn revoke_project(&mut self, delta: ProjectGrantDelta) -> Result<ProjectGrants> {
        self.coordinator()?.revoke_project(delta).await
    }

    async fn push(&mut self, workspace: &str, options: PushOptions) -> Result<PushReport> {
        self.coordinator()?
            .worker(workspace)
            .await?
            .push(options)
            .await
    }

    async fn rebase(
        &mut self,
        workspace: &str,
        into: Option<&str>,
        options: RebaseOptions,
    ) -> Result<RebaseReport> {
        let coordinator = self.coordinator()?;
        let into = match into {
            Some(name) => Some(coordinator.project().workspace(name).await?),
            None => None,
        };
        coordinator.rebase(workspace, into.as_ref(), options).await
    }

    async fn land(
        &mut self,
        workspace: &str,
        into: Option<&str>,
        options: LandOptions,
    ) -> Result<LandReport> {
        let coordinator = self.coordinator()?;
        let into = match into {
            Some(name) => Some(coordinator.project().workspace(name).await?),
            None => None,
        };
        coordinator.land(workspace, into.as_ref(), options).await
    }

    async fn exec(
        &mut self,
        command: ExecCommand,
        presentation: ExecPresentation,
        stdout: &mut (dyn Write + Send),
        stderr: &mut (dyn Write + Send),
    ) -> Result<ExecResult> {
        let worker = cowshed_os::timing::spanned(
            "exec",
            "worker",
            self.coordinator()?.worker(&command.workspace),
        )
        .await?;
        let submit = cowshed_os::timing::span("exec", "submit");
        // The CLI is the coordinator route (07_api.md, "CLI and API exec parity"): it holds the
        // operator's coordinator authority, so the route is who asks, never the class asked for.
        // A named session runs in its worker shell; the parser refused a session with a gate or
        // --host, the two things only this route admits.
        let job = match command.session.as_deref() {
            Some(session_name) => {
                worker
                    .shell(Some(session_name))
                    .await?
                    .run(command.request)
                    .await?
            }
            None => self.coordinator()?.exec(&worker, command.request).await?,
        };
        drop(submit);

        if command.background {
            let info = job.status().await?;
            job.detach().await?;
            return Ok(ExecResult {
                info,
                end: ExecEnd::Backgrounded,
            });
        }
        // Every foreground job is held for as long as this process lives, so a client that dies
        // -- a caller's timeout killing it, `SIGKILL` included -- takes its job and its tokens
        // with it (05_gateway.md "A job lives with its requester").
        let descriptor = self
            .runtime
            .as_ref()
            .ok_or_else(|| {
                CowshedError::internal("the CLI project runtime has already been shut down")
            })?
            .descriptor();
        let socket = cowshed_core::runtime::supervisor_socket::socket_path(
            descriptor.storage.store(),
            &descriptor.repo_id,
            worker.name(),
        );
        let hold = hold_foreground(
            &job,
            &socket,
            &worker.info().workspace_incarnation,
            job.id(),
        )
        .await?;
        let _relay = cowshed_os::timing::span("exec", "relay");
        relay_foreground(
            &job,
            Some(hold),
            command.timeout,
            presentation,
            stdout,
            stderr,
        )
        .await
    }

    async fn reconcile_gateway(&mut self) -> Result<()> {
        let descriptor = self
            .runtime
            .as_ref()
            .ok_or_else(|| {
                CowshedError::internal("the CLI project runtime has already been shut down")
            })?
            .descriptor();
        match self.backend {
            GatewayStoreBackend::Native => gateway_service::reconcile_native_project(
                &descriptor.repo_id,
                &descriptor.storage,
                self.coordinator()?,
            )
            .await
            .map(|_| ()),
            GatewayStoreBackend::Directory => gateway_service::reconcile_directory_project(
                &descriptor.repo_id,
                &descriptor.storage,
                self.coordinator()?,
            )
            .await
            .map(|_| ()),
        }
    }
    async fn shutdown(self) -> Result<()> {
        ActorBridge::shutdown(self).await
    }
}

/// A job one `cowshed exec` relays in the foreground, reached through the controller
/// ([`JobHandle`]) or straight through a resident workspace's supervisor.
#[async_trait]
pub(crate) trait ForegroundJob: Sync {
    /// The job's terminal record, once it has one.
    async fn wait(&self) -> Result<JobInfo>;
    async fn status(&self) -> Result<JobInfo>;
    /// Leave the job running with no client attached.
    async fn detach(&self) -> Result<()>;
    async fn kill(&self) -> Result<()>;
    /// Copy one of the job's streams to `writer` as it is produced, to its end.
    async fn relay(&self, stream: JobStream, writer: &mut (dyn Write + Send)) -> Result<()>;
}

#[async_trait]
impl ForegroundJob for JobHandle {
    async fn wait(&self) -> Result<JobInfo> {
        JobHandle::wait(self).await
    }

    async fn status(&self) -> Result<JobInfo> {
        JobHandle::status(self).await
    }

    async fn detach(&self) -> Result<()> {
        JobHandle::detach(self).await
    }

    async fn kill(&self) -> Result<()> {
        JobHandle::kill(self).await
    }

    async fn relay(&self, stream: JobStream, writer: &mut (dyn Write + Send)) -> Result<()> {
        let mut stream = self.logs(stream, 0, true).await?;
        while let Some(chunk) = stream.next().await {
            writer.write_all(&chunk?).map_err(output_error)?;
        }
        writer.flush().map_err(output_error)
    }
}

/// Hold a foreground job for as long as this process lives (05_gateway.md, "A job lives with its
/// requester"): a job nobody waits for any more would keep its tokens, and a gate its place at
/// the head of the gate queue. A job that cannot be held is killed rather than left running for
/// nobody.
pub(crate) async fn hold_foreground(
    job: &impl ForegroundJob,
    socket: &Path,
    incarnation: &cowshed_core::metadata::WorkspaceIncarnation,
    id: cowshed_core::api::JobId,
) -> Result<JobHold> {
    match cowshed_core::runtime::supervisor_socket::hold(socket, incarnation, id).await {
        Ok(hold) => Ok(hold),
        Err(error) => Err(merge_primary(error, job.kill().await.err())),
    }
}

/// Relay one foreground job to this process until it ends, or until the caller's `timeout`.
///
/// The job's output streams to `stdout` and `stderr` the whole time. With no timeout the relay
/// lasts as long as the job. When a timeout passes, the job's status decides: a job still
/// running is released from `hold`, detached and reported still running; a job that ended
/// meanwhile has nothing to reattach to, so its output is drained to EOF and its end is the
/// command's. A `hold` dropped before the job ends — a failed relay, or this process ending —
/// kills the job. Every CLI foreground exec passes one; only relay tests without a supervisor
/// pass none.
pub(crate) async fn relay_foreground(
    job: &impl ForegroundJob,
    hold: Option<JobHold>,
    timeout: Option<Duration>,
    presentation: ExecPresentation,
    stdout: &mut (dyn Write + Send),
    stderr: &mut (dyn Write + Send),
) -> Result<ExecResult> {
    let finished = async {
        match presentation {
            ExecPresentation::Control => job.wait().await,
            ExecPresentation::Raw => {
                let (info, (), ()) = tokio::try_join!(
                    job.wait(),
                    job.relay(JobStream::Stdout, stdout),
                    job.relay(JobStream::Stderr, stderr),
                )?;
                Ok(info)
            }
        }
    };
    tokio::pin!(finished);
    let finished_now = |info| ExecResult {
        info,
        end: ExecEnd::Finished,
    };
    let Some(timeout) = timeout else {
        return Ok(finished_now(finished.await?));
    };
    tokio::select! {
        info = &mut finished => return Ok(finished_now(info?)),
        () = tokio::time::sleep(timeout) => {}
    }
    let info = job.status().await?;
    if info.state.is_terminal() {
        return Ok(finished_now(finished.await?));
    }
    if let Some(hold) = hold {
        hold.release().await?;
    }
    job.detach().await?;
    Ok(ExecResult {
        info,
        end: ExecEnd::StillRunning { after: timeout },
    })
}

/// A write of the child's output that the descriptor refused. A full pipe never ends here: the
/// CLI's stdio waits for its reader ([`cowshed_os::stdio`]), so only a reader that is gone is told to
/// check its consumer.
pub(crate) fn output_error(error: io::Error) -> CowshedError {
    let hint = match error.kind() {
        io::ErrorKind::BrokenPipe | io::ErrorKind::ConnectionReset => {
            "check that the output consumer is still connected"
        }
        _ => "check the file or device the command's output is written to",
    };
    CowshedError::environment_missing(format!("could not write child output: {error}"), hint)
}

fn project_inventory_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(
        format!("project inventory failed: {error}"),
        "cowshed doctor --json",
    )
}

async fn cleanup_open_failure(
    primary: CowshedError,
    connection: JoinHandle<Result<()>>,
    runtime: ProjectRuntime,
) -> CowshedError {
    let connection_error = join_connection(connection).await.err();
    let runtime_error = runtime.shutdown().await.err();
    merge_primary(
        primary,
        combine_teardown(connection_error, runtime_error).err(),
    )
}

async fn join_connection(connection: JoinHandle<Result<()>>) -> Result<()> {
    match connection.await {
        Ok(result) => result,
        Err(error) => Err(CowshedError::internal(format!(
            "controller connection actor did not join: {error}"
        ))),
    }
}

fn combine_teardown(first: Option<CowshedError>, second: Option<CowshedError>) -> Result<()> {
    match (first, second) {
        (None, None) => Ok(()),
        (Some(error), None) | (None, Some(error)) => Err(error),
        (Some(first), Some(second)) => Err(CowshedError::new(
            ErrorCode::Internal,
            format!(
                "controller connection teardown failed: {}; project runtime shutdown also failed: {}",
                first.message, second.message
            ),
            format!("{}; {}", first.hint, second.hint),
        )),
    }
}

pub fn merge_primary(primary: CowshedError, teardown: Option<CowshedError>) -> CowshedError {
    match teardown {
        None => primary,
        Some(teardown) => CowshedError::new(
            primary.code,
            format!(
                "{}; controller teardown also failed: {}",
                primary.message, teardown.message
            ),
            format!("{}; teardown: {}", primary.hint, teardown.hint),
        ),
    }
}

struct ResolvedProjectRoot {
    root: PathBuf,
    resuming_re_adoption: bool,
}

pub async fn resolve_project_root(cli: &Cli) -> Result<PathBuf> {
    resolve_project_context(cli)
        .await
        .map(|resolved| resolved.root)
}

async fn resolve_project_context(cli: &Cli) -> Result<ResolvedProjectRoot> {
    let candidate = cli.global.project.as_deref().or(match &cli.command {
        Command::Adopt(args) => args.path.as_deref(),
        _ => None,
    });
    let start = match candidate {
        Some(path) => path.to_path_buf(),
        None => std::env::current_dir().map_err(|error| {
            CowshedError::environment_missing(
                format!("could not determine the current directory: {error}"),
                "use --project <git-root>",
            )
        })?,
    };
    let root = discover_project_root(&start).await;
    // A retained source can be restored read-only while recovery is pending. Its Git
    // checkout then exists again, but this invocation still finishes the recorded intent.
    if matches!(&cli.command, Command::Adopt(args) if args.re_adopt) {
        let inventory = NativeGatewayInventory::new(host_storage().await?);
        if let Some(root) = inventory
            .forming_re_adoption_root(&start)
            .await
            .map_err(project_inventory_error)?
        {
            return Ok(ResolvedProjectRoot {
                root,
                resuming_re_adoption: true,
            });
        }
    }
    match (&cli.command, root) {
        (_, Ok(root)) => Ok(ResolvedProjectRoot {
            root,
            resuming_re_adoption: false,
        }),
        (Command::Adopt(_), Err(error)) => Err(error),
        // A named removal outside any project addresses its workspace the way `detach <ws>`
        // does: from the store readdir, accepted only when exactly one project holds a session
        // of that name. Only the address changes; the project then runs its own containment gate.
        (Command::Remove(args), Err(error))
            if cli.global.project.is_none() && WorkspaceName::session(&args.workspace).is_ok() =>
        {
            let storage = host_storage().await?;
            named_removal_root(storage.store(), &args.workspace, error).map(|root| {
                ResolvedProjectRoot {
                    root,
                    resuming_re_adoption: false,
                }
            })
        }
        (_, Err(error)) => Err(project_context_error(error)),
    }
}

fn project_context_error(error: CowshedError) -> CowshedError {
    CowshedError::new(
        error.code,
        error.message,
        "cowshed ls; cowshed --project <git-root> <command>",
    )
}

/// The project a named removal belongs to when cwd discovery found none: the store readdir's
/// unique owner of `workspace`. A miss or an ambiguity says both what discovery saw and what the
/// store holds, so the refusal names why neither address resolved.
fn named_removal_root(store: &Path, workspace: &str, discovery: CowshedError) -> Result<PathBuf> {
    resolve_session_project_root(store, workspace, "rm").map_err(|lookup| {
        CowshedError::new(
            lookup.code,
            format!("{}; {}", discovery.message, lookup.message),
            lookup.hint,
        )
    })
}

fn optional_project_unavailable(error: &CowshedError) -> bool {
    matches!(
        error.code,
        ErrorCode::EnvironmentMissing | ErrorCode::NotFound
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DispatchExit {
    pub code: i32,
}

enum AdoptionOutcome<'a> {
    Created { capacity: &'a str },
    Recovered,
    Replaced(&'a ReAdoption),
}

async fn report_adoption<S, W, E>(
    service: &mut S,
    info: &WorkspaceInfo,
    outcome: AdoptionOutcome<'_>,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<DispatchExit>
where
    S: CliService,
    W: Write + Send,
    E: Write + Send,
{
    // The durable adoption/recovery is complete. A gateway refusal is a separate,
    // recoverable condition, not a reason to issue another adoption.
    let gateway = service.reconcile_gateway().await;
    emit_mount(output, json, info)?;
    match outcome {
        AdoptionOutcome::Created { capacity } => output.guidance(&format!(
            "created {}.{IMAGE_EXTENSION} for {} (capacity {capacity})",
            info.workspace, info.repo_id,
        )),
        AdoptionOutcome::Recovered => output.guidance(&format!(
            "resumed recorded replacement for {} at {}",
            info.workspace,
            info.mount.display(),
        )),
        AdoptionOutcome::Replaced(re_adoption) => report_re_adoption(output, re_adoption),
    }
    .map_err(output_error)?;
    match gateway {
        Ok(()) => output.hint("cowshed fork <ws>").map_err(output_error)?,
        Err(error) => {
            output
                .guidance(&format!(
                    "adopted; the gateway is not ready yet: {}",
                    error.message,
                ))
                .map_err(output_error)?;
            output.hint(&error.hint).map_err(output_error)?;
        }
    }
    Ok(success())
}

/// What a re-adoption kept (02_workspaces.md, "Re-adoption", step 5): every retained path, and,
/// as the next command quiet mode never suppresses, the removal that returns their space once the
/// replacement is checked.
fn report_re_adoption<W, E>(output: &mut Output<W, E>, re_adoption: &ReAdoption) -> io::Result<()>
where
    W: Write + Send,
    E: Write + Send,
{
    output.guidance(&format!(
        "re-adopted main as {}; this re-adoption kept what it replaced at:",
        re_adoption.replacement.as_str()
    ))?;
    for path in &re_adoption.retained {
        output.note(&format!("  {}", path.display()))?;
    }
    output.guidance(
        "once you have checked the replacement, removing exactly those paths returns their space",
    )?;
    output.hint(&format!(
        "rm -R -- {}",
        re_adoption
            .retained
            .iter()
            .map(|path| path.display().to_string())
            .collect::<Vec<_>>()
            .join(" ")
    ))
}

async fn report_recovered_adoption<S, W, E>(
    service: &mut S,
    root: &Path,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<DispatchExit>
where
    S: CliService,
    W: Write + Send,
    E: Write + Send,
{
    let info = service.workspace_at(root.to_path_buf()).await?;
    report_adoption(service, &info, AdoptionOutcome::Recovered, json, output).await
}

pub async fn dispatch<S, R, W, E>(
    service: &mut S,
    cli: Cli,
    stdin: R,
    output: &mut Output<W, E>,
) -> Result<DispatchExit>
where
    S: CliService,
    R: AsyncRead + Send + 'static,
    W: Write + Send,
    E: Write + Send,
{
    let json = cli.global.json;
    match cli.command {
        Command::Adopt(args) => {
            let options = adopt_options(args)?;
            // The capacity is resolved here rather than left implicit, so the line that reports
            // the adoption states the size the image was actually minted at.
            let capacity = options
                .capacity
                .clone()
                .unwrap_or_else(|| DEFAULT_IMAGE_CAPACITY.to_string());
            let (info, re_adoption) = service.adopt(options).await?;
            let outcome = match &re_adoption {
                Some(re_adoption) => AdoptionOutcome::Replaced(re_adoption),
                None => AdoptionOutcome::Created {
                    capacity: &capacity,
                },
            };
            report_adoption(service, &info, outcome, json, output).await
        }
        Command::Move(args) => {
            let info = match &args.destination {
                MoveDestination::Workspace(name) => service.rename(&args.source, name).await?,
                MoveDestination::Checkout(path) => service.move_checkout(path).await?,
                MoveDestination::RepoId(repo_id) => service.change_repo_id(repo_id.clone()).await?,
            };
            if !matches!(&args.destination, MoveDestination::RepoId(_)) {
                service.reconcile_gateway().await?;
            }
            emit_mount(output, json, &info)?;
            let hint = match &args.destination {
                MoveDestination::Workspace(name) => format!("cowshed exec {name} -- <cmd>"),
                MoveDestination::Checkout(path) => format!("cd {}", path.display()),
                MoveDestination::RepoId(repo_id) => format!("cowshed --project {} ls", repo_id),
            };
            output.hint(&hint).map_err(output_error)?;
            Ok(success())
        }
        Command::Fork(args) => {
            let options = ForkOptions {
                revision: args.reference.map(os_revision).transpose()?,
                clean: args.clean,
                browse: args.browse,
                slot: args.slot,
                register: args.register,
                git_worktree: args.git_worktree,
                markdown_only: args.markdown_only,
            };
            let info = service
                .fork(&args.destination, args.source.as_deref(), options)
                .await?;
            service.reconcile_gateway().await?;
            emit_mount(output, json, &info)?;
            output
                .hint(&format!("cowshed exec {} -- <cmd>", args.destination))
                .map_err(output_error)?;
            Ok(success())
        }
        Command::Checkpoint(args) => {
            let workspace = resolve_workspace(service, args.workspace, "checkpoint").await?;
            let label = service
                .checkpoint(
                    &workspace,
                    CheckpointOptions {
                        label: args.label.map(os_utf8).transpose()?,
                        keep: args.keep,
                    },
                )
                .await?;
            if json {
                output
                    .success(CheckpointResult { label })
                    .map_err(output_error)?;
            } else {
                output.bare_line(label.as_bytes()).map_err(output_error)?;
            }
            Ok(success())
        }
        Command::Restore(args) => {
            let label = os_utf8(args.label)?;
            let info = service.restore(&args.workspace, &label).await?;
            service.reconcile_gateway().await?;
            emit_mount(output, json, &info)?;
            output
                .hint(&format!("cowshed exec {} -- git status", args.workspace))
                .map_err(output_error)?;
            Ok(success())
        }
        Command::List(args) => {
            if args.all {
                let projects = list_projects(listing_landing(args)).await?;
                emit_project_listing(output, json, args, projects).await?;
                if !json {
                    emit_kept_abandon_bundles(output, &service.kept_abandon_bundles().await?)?;
                }
            } else {
                let ProjectWorkspaces {
                    repo_id,
                    mut workspaces,
                    unproven,
                } = service.list(listing_landing(args)).await?;
                sort_workspace_listing(&mut workspaces);
                let empty = workspaces.is_empty() && unproven.is_empty();
                emit_workspace_listing(output, json, args, None, workspaces)?;
                emit_unproven(output, &repo_id, &unproven)?;
                if empty {
                    let others = service.other_adopted_project_count().await?;
                    let noun = if others == 1 {
                        "project exists"
                    } else {
                        "projects exist"
                    };
                    output
                        .guidance(&format!(
                            "current repository has no workspaces or is not adopted; {others} \
                             other adopted {noun}; run cowshed ls --all"
                        ))
                        .map_err(output_error)?;
                }
            }
            Ok(success())
        }
        Command::Path(args) => {
            let workspace = match args.slot {
                Some(slot) => service.workspace_in_slot(slot).await?,
                None => resolve_workspace(service, args.workspace, "path").await?,
            };
            let info = service.path(&workspace, args.no_attach).await?;
            if !args.no_attach {
                cowshed_os::timing::spanned(
                    "path",
                    "reconcile-gateway",
                    service.reconcile_gateway(),
                )
                .await?;
            }
            if args.no_attach && info.state == WorkspaceState::Detached {
                output
                    .guidance("workspace is detached; returning its configured mount path")
                    .map_err(output_error)?;
            }
            emit_mount(output, json, &info)?;
            Ok(success())
        }
        Command::Exec(args) => {
            let workspace = args.workspace.clone();
            let command = exec_command(args, stdin)?;
            let (stdout, stderr) = output.writers_mut();
            let result = service
                .exec(command, exec_presentation(json), stdout, stderr)
                .await?;
            report_exec(output, json, &workspace, result)
        }
        Command::Grant(args) => {
            let changed = !args.read.is_empty()
                || !args.write.is_empty()
                || !args.deny_write.is_empty()
                || !args.deny.is_empty()
                || !args.egress.is_empty()
                || args.service_ports.is_some()
                || args.loopback_any;
            let workspace = match args.target {
                GrantTarget::Workspace(workspace) => workspace,
                GrantTarget::Project => {
                    return grant_project(
                        service,
                        output,
                        json,
                        args.read,
                        args.deny_write,
                        args.deny,
                        egress_rules(args.egress, args.egress_mode),
                    )
                    .await;
                }
            };
            let requested: Vec<PathBuf> =
                args.read.iter().chain(args.write.iter()).cloned().collect();
            let egress = egress_rules(args.egress, args.egress_mode);
            let grants = if changed {
                service
                    .grant(
                        &workspace,
                        GrantDelta {
                            read: args.read,
                            write: args.write,
                            deny_write: args.deny_write,
                            deny: args.deny,
                            egress,
                            service_ports: args.service_ports.map(NonZeroU16::get),
                            loopback_any: args.loopback_any,
                            ..GrantDelta::default()
                        },
                    )
                    .await?
            } else {
                service.grants(&workspace).await?
            };
            if json {
                output.success(grants.clone()).map_err(output_error)?;
            } else if !changed {
                emit_grants(output, &grants)?;
            }
            if changed {
                // A grant is recorded under its resolved spelling. Saying so when that differs
                // from what was typed keeps `cowshed grant <ws>` listings explicable, and names
                // the rule that makes the typed spelling work anyway.
                for path in requested {
                    let Ok(resolved) = std::fs::canonicalize(&path) else {
                        continue;
                    };
                    if resolved != path {
                        output
                            .guidance(&format!(
                                "recorded {} as {}; a symlink beside the workspace that reaches a granted path is readable through the link",
                                path.display(),
                                resolved.display()
                            ))
                            .map_err(output_error)?;
                    }
                }
                output
                    .guidance(&format!(
                        "grants for {} now: {} read, {} write, {} denied writes, {} denied paths, {} egress",
                        workspace,
                        grants.read.len(),
                        grants.write.len(),
                        grants.deny_write.len(),
                        grants.deny.len(),
                        grants.egress.len()
                    ))
                    .map_err(output_error)?;
                if let (Some(_), Some(block)) = (args.service_ports, grants.port_block) {
                    // The block, not the request, is what jobs run under: it is rounded up to a
                    // whole aligned block and may have moved, so say where it now is.
                    output
                        .guidance(&format!(
                            "port block for {} now: COWSHED_PORT_BASE={} COWSHED_PORT_BLOCK_SIZE={} ({} service ports)",
                            workspace,
                            block.base(),
                            block.size(),
                            block.size() - 1
                        ))
                        .map_err(output_error)?;
                }
                if grants.loopback_any {
                    output
                        .warning(&format!(
                            "every job of {workspace} may now connect to any loopback port, the control plane's and every sibling workspace's included; cowshed revoke {workspace} --loopback-any withdraws it"
                        ))
                        .map_err(output_error)?;
                }
                output
                    .guidance("grants apply from the next exec or shell")
                    .map_err(output_error)?;
                output
                    .hint(&format!(
                        "cowshed exec {} -- <retry your command>",
                        workspace
                    ))
                    .map_err(output_error)?;
            }
            Ok(success())
        }
        Command::Revoke(args) => revoke(service, output, json, args).await,
        Command::Remove(args) if args.purge => {
            let report = service
                .remove_project(RemoveProjectOptions {
                    force: args.force,
                    abandon: args.abandon,
                })
                .await?;
            service.reconcile_gateway().await?;
            if json {
                output.success(report.clone()).map_err(output_error)?;
            }
            for removed in &report.removed {
                output
                    .guidance(&format!("removed {}", removed.workspace))
                    .map_err(output_error)?;
                if let Some(warning) =
                    removed.report.abandoned.as_ref().and_then(|abandoned| {
                        predating_warning(removed.workspace.as_str(), abandoned)
                    })
                {
                    output.warning(&warning).map_err(output_error)?;
                }
            }
            for bundle in &report.deleted_bundles {
                output
                    .guidance(&format!("deleted abandon bundle {}", bundle.display()))
                    .map_err(output_error)?;
            }
            if let Some(abandoned) = report.restored.abandoned.as_ref() {
                output
                    .guidance(&format!(
                        "main's unpreserved commits bundled to {}",
                        abandoned.bundle.display()
                    ))
                    .map_err(output_error)?;
                if let Some(warning) = predating_warning("main", abandoned) {
                    output.warning(&warning).map_err(output_error)?;
                }
            }
            Ok(success())
        }
        Command::Remove(args) => {
            let report = service
                .remove(
                    &args.workspace,
                    RemoveOptions {
                        force: args.force,
                        restore: args.restore,
                        abandon: args.abandon,
                    },
                )
                .await?;
            service.reconcile_gateway().await?;
            if json {
                output.success(report.clone()).map_err(output_error)?;
            }
            // An authorized abandonment still says out loud what it destroyed and where the only
            // remaining copy went. The refusal it replaced would have said the same thing; passing
            // the flag buys the deletion, not silence.
            if let Some(abandoned) = report.abandoned.as_ref() {
                output
                    .guidance(&format!(
                        "abandoned {} commit{} at {}{} that {} ({}) did not contain",
                        abandoned.unlanded_commits,
                        if abandoned.unlanded_commits == 1 {
                            ""
                        } else {
                            "s"
                        },
                        abandoned.head,
                        match abandoned.branches.len() {
                            0 => String::new(),
                            1 => String::from(" and 1 other branch"),
                            count => format!(" and {count} other branches"),
                        },
                        abandoned.target_branch,
                        match abandoned.target_head.as_ref() {
                            Some(head) => format!("at {head}"),
                            None => "no such branch".to_owned(),
                        }
                    ))
                    .map_err(output_error)?;
                for branch in &abandoned.branches {
                    output
                        .guidance(&format!(
                            "abandoned branch {} at {}",
                            branch.branch, branch.tip
                        ))
                        .map_err(output_error)?;
                }
                for kept in abandoned.preserved.refs() {
                    output
                        .guidance(&format!(
                            "kept {} at {} in main as {}",
                            kept.name, kept.tip, kept.reference
                        ))
                        .map_err(output_error)?;
                }
                output
                    .guidance(&format!("bundled to {}", abandoned.bundle.display()))
                    .map_err(output_error)?;
                if let Some(warning) = predating_warning(args.workspace.as_str(), abandoned) {
                    output.warning(&warning).map_err(output_error)?;
                }
            }
            output.hint("cowshed gc").map_err(output_error)?;
            Ok(success())
        }
        Command::Attach(args) => {
            let options = AttachOptions {
                browse: args.browse,
                observed_path: if args.all {
                    None
                } else {
                    Some(invocation_cwd()?)
                },
                aggregate: args.workspace.is_none(),
            };
            let infos = if let Some(workspace) = args.workspace {
                vec![service.attach(&workspace, options).await?]
            } else {
                attach_scoped_sessions(service, args.all, options).await?
            };
            service.reconcile_gateway().await?;
            emit_attached(output, json, &infos)?;
            Ok(success())
        }
        Command::Detach(args) => {
            if args.all {
                detach_scoped_sessions(service).await?;
            } else {
                let workspace = args.workspace.ok_or_else(|| {
                    usage(
                        "detach requires a workspace",
                        "cowshed detach <ws>; cowshed detach --all",
                    )
                })?;
                service.detach(&workspace).await?;
            }
            service.reconcile_gateway().await?;
            if json {
                output.success(EmptyResult {}).map_err(output_error)?;
            }
            Ok(success())
        }
        Command::Resize(args) => {
            let capacity = os_capacity(args.capacity)?;
            let result = service
                .resize(&args.workspace, &capacity, args.volume)
                .await?;
            // Resize leaves the workspace mounted exactly as it found it, but the mount it comes
            // back on is a new attachment, so the gateway's view has to be refreshed.
            service.reconcile_gateway().await?;
            if json {
                output.success(result.clone()).map_err(output_error)?;
            } else {
                output
                    .bare_line(result.capacity.as_bytes())
                    .map_err(output_error)?;
            }
            let volume = match result.volume {
                ResizeVolume::Workspace => "",
                ResizeVolume::Build => "'s build volume",
            };
            output
                .guidance(&format!(
                    "workspace {}{volume} grew from {} to {}",
                    result.workspace, result.previous_capacity, result.capacity
                ))
                .map_err(output_error)?;
            Ok(success())
        }
        Command::Defrag(args) => {
            use cowshed_core::storage::lifecycle::ExtentCount;

            let result = service.defragment(&args.workspace).await?;
            // Like resize: the workspace is back as it was found, on a new attachment.
            service.reconcile_gateway().await?;
            if json {
                output.success(result.clone()).map_err(output_error)?;
            } else {
                output
                    .bare_line(result.extents.to_string().as_bytes())
                    .map_err(output_error)?;
            }
            let previous = ExtentCount::new(result.previous_extents);
            let extents = ExtentCount::new(result.extents);
            output
                .guidance(&format!(
                    "workspace {} rewritten contiguously: {previous} extents -> {extents}, {} bytes copied; a clone's first write now copies a map whose loaded RAW sensitivity is {:.3?} instead of {:.3?}",
                    result.workspace,
                    result.bytes,
                    extents.observed_first_write_cost(),
                    previous.observed_first_write_cost()
                ))
                .map_err(output_error)?;
            Ok(success())
        }
        Command::Reseed(args) => {
            let result = service
                .reseed(&args.workspace, &args.from, args.dry_run)
                .await?;
            if !result.dry_run {
                service.reconcile_gateway().await?;
            }
            emit_reseed_result(output, json, result)?;
            Ok(success())
        }
        Command::Cache(CacheCommand::Clear { workspace }) => {
            let workspace = resolve_workspace(service, workspace, "cache clear").await?;
            let report = clear_caches(service, &workspace).await?;
            emit_cache_clear_report(output, json, report)?;
            Ok(success())
        }
        Command::Rekey(args) => {
            let result = service.rekey(&args.workspace).await?;
            if json {
                output.success(result.clone()).map_err(output_error)?;
            } else {
                output
                    .bare_line(result.workspace.as_bytes())
                    .map_err(output_error)?;
            }
            output
                .guidance(&format!(
                    "workspace {} rekeyed at revision {}{}",
                    result.workspace,
                    result.revision,
                    result
                        .tombstone_removed
                        .as_ref()
                        .map(|removed| format!("; quarantine entry {removed} consumed"))
                        .unwrap_or_default(),
                ))
                .map_err(output_error)?;
            output
                .hint(&format!("cowshed attach {}", result.workspace))
                .map_err(output_error)?;
            Ok(success())
        }
        Command::Gc(args) => {
            if let Some(recover) = args.recover {
                let absolute = |path: &Path| {
                    std::path::absolute(path).map_err(|error| {
                        CowshedError::usage(
                            format!("cannot make {} absolute: {error}", path.display()),
                            "name the path from an existing working directory",
                        )
                    })
                };
                let recovered = service
                    .recover_image(&absolute(&recover.image)?, &absolute(&recover.to)?)
                    .await?;
                if json {
                    output.success(recovered).map_err(output_error)?;
                } else {
                    output
                        .bare_line(recovered.directory.as_os_str().as_bytes())
                        .and_then(|()| {
                            output.guidance(&format!(
                                "moved {} ({} at {}) and {} companion file(s) out of the store; {}; manifest {}",
                                recovered.source.display(),
                                recovered.workspace,
                                recovered.incarnation,
                                recovered.moved.len().saturating_sub(1),
                                recovered.retained,
                                recovered.manifest.display()
                            ))
                        })
                        .map_err(output_error)?;
                }
                return Ok(success());
            }
            let report = service
                .gc(GcOptions {
                    dry_run: args.dry_run,
                    abandoned: args.abandoned,
                    supersede: args.supersede,
                })
                .await?;
            for deferred in &report.deferred {
                output
                    .guidance(&format!(
                        "deferred {}: {}",
                        deferred.path.display(),
                        deferred.diagnostic
                    ))
                    .map_err(output_error)?;
            }
            emit_abandoned_refs(output, &report)?;
            if json {
                output.success(report.clone()).map_err(output_error)?;
            } else {
                output
                    .bare_line(report.released_bytes.to_string().as_bytes())
                    .map_err(output_error)?;
            }
            if report.dry_run {
                emit_gc_candidates(output, &report)?;
                let candidate_noun = if report.candidates.len() == 1 {
                    "candidate"
                } else {
                    "candidates"
                };
                output
                    .guidance(&format!(
                        "dry run examined {} objects; {} {}, {} bytes deletable",
                        report.examined,
                        report.candidates.len(),
                        candidate_noun,
                        report.released_bytes
                    ))
                    .map_err(output_error)?;
            }
            Ok(success())
        }
        Command::Push(args) => {
            let workspace = resolve_workspace(service, args.workspace, "push").await?;
            let options = PushOptions {
                branch: args.branch.map(os_branch).transpose()?,
                expected_workspace_incarnation: args
                    .expected_workspace_incarnation
                    .map(os_incarnation)
                    .transpose()?,
                expected_source_head: args.expected_source_head.map(os_git_oid).transpose()?,
                expected_destination_head: args
                    .expected_destination_head
                    .map(os_expected_ref)
                    .transpose()?,
            };
            let report = service.push(&workspace, options).await?;
            if json {
                output.success(report.clone()).map_err(output_error)?;
            } else {
                emit_push(output, &report)?;
            }
            Ok(success())
        }
        Command::Rebase(args) => {
            let workspace = resolve_workspace(service, args.workspace, "rebase").await?;
            let options = RebaseOptions {
                onto: args.onto.map(os_revision).transpose()?,
                expected_workspace_incarnation: args
                    .expected_workspace_incarnation
                    .map(os_incarnation)
                    .transpose()?,
                expected_source_head: args.expected_source_head.map(os_git_oid).transpose()?,
                expected_onto_head: args.expected_onto_head.map(os_git_oid).transpose()?,
                gate: args.gate,
                mode: args.mode,
            };
            let report = service
                .rebase(&workspace, args.into.as_deref(), options)
                .await?;
            if json {
                output.success(report).map_err(output_error)?;
            } else {
                output
                    .bare_line(report.oid.as_str().as_bytes())
                    .and_then(|()| output.guidance(&rebase_build_volume_line(&report.build_volume)))
                    .map_err(output_error)?;
            }
            Ok(success())
        }
        Command::Land(args) => {
            // A preflight retires nothing, so it leaves the gateway as it is.
            let reconcile_gateway = args.retire && !args.preflight;
            let options = LandOptions {
                target_branch: args
                    .target
                    .map(os_branch)
                    .transpose()?
                    .map(|branch| branch.as_str().to_owned()),
                check: (!args.checks.is_empty())
                    .then(|| {
                        args.checks
                            .into_iter()
                            .map(os_utf8)
                            .collect::<Result<Vec<_>>>()
                    })
                    .transpose()?,
                retire: args.retire,
                push_only: args.push_only,
                expected_workspace_incarnation: args
                    .expected_workspace_incarnation
                    .map(os_incarnation)
                    .transpose()?,
                expected_source_head: args.expected_source_head.map(os_git_oid).transpose()?,
                expected_target_head: args.expected_target_head.map(os_expected_ref).transpose()?,
                ref_lock: args.ref_lock.map(os_ref_lock).transpose()?,
                mode: args.mode,
                preflight: args.preflight,
            };
            let report = service
                .land(&args.workspace, args.into.as_deref(), options)
                .await?;
            if reconcile_gateway {
                service.reconcile_gateway().await?;
            }
            if json {
                output.success(report.clone()).map_err(output_error)?;
            } else {
                emit_land(output, &report)?;
            }
            Ok(success())
        }
        Command::Doctor(args) => {
            let report = service.doctor().await?;
            let healthy = report.healthy;
            if json {
                output.success(report).map_err(output_error)?;
            } else {
                emit_doctor(output, &report)?;
            }
            // Repair retires what doctor named as reclaimable the same way `gc` does — one
            // sweep, one set of reasons — rather than a second deleter with its own rules.
            if args.repair {
                let swept = service
                    .gc(GcOptions {
                        dry_run: false,
                        abandoned: false,
                        supersede: None,
                    })
                    .await?;
                output
                    .guidance(&format!(
                        "repair examined {} objects; reclaimed {}, {} bytes freed, {} retained for a running operation",
                        swept.examined, swept.reclaimed, swept.released_bytes, swept.retained_active
                    ))
                    .map_err(output_error)?;
            }
            Ok(DispatchExit {
                code: if healthy { 0 } else { 5 },
            })
        }
        Command::Gateway(_) => Err(CowshedError::internal(
            "gateway commands must be dispatched by the host service entrypoint",
        )),
        Command::DiskCommand(_) => Err(CowshedError::internal(
            "disk-command names no project and is dispatched before the runtime bridge",
        )),
        Command::LandTurn(_) => Err(CowshedError::internal(
            "land-turn names no project and is dispatched before the runtime bridge",
        )),
        Command::SubtreeRoot(_) | Command::RetireDescendants(_) => Err(CowshedError::internal(
            "subtree commands use the native supervisor socket before the runtime bridge",
        )),
        Command::Lock(_)
        | Command::LockHolders(_)
        | Command::Hold(_)
        | Command::TreeHolders(_)
        | Command::FdHolders(_) => Err(CowshedError::internal(
            "lock, lock-holders, hold, tree-holders and fd-holders name no project and are dispatched before the runtime bridge",
        )),
        Command::Controller | Command::Host(_) => Err(CowshedError::internal(
            "a connection-serving verb serves its own connection and is dispatched before the runtime bridge",
        )),
        Command::BuildState => Err(CowshedError::internal(
            "build-state is read-only and must be dispatched before the runtime bridge",
        )),
        Command::Credential(_) => Err(CowshedError::internal(
            "credential commands must be dispatched by the host service entrypoint",
        )),
        Command::Identity(_) => Err(CowshedError::internal(
            "identity commands must be dispatched by the host service entrypoint",
        )),
        Command::Setup(_) => Err(CowshedError::internal(
            "setup must be dispatched by the host service entrypoint",
        )),
        Command::Sccache(_) => Err(CowshedError::internal(
            "sccache commands must be dispatched by the host service entrypoint",
        )),
        Command::Mount(_) => Err(CowshedError::internal(
            "mount must be dispatched by the host service entrypoint",
        )),
        Command::Forget(_) => Err(CowshedError::internal(
            "forget must be dispatched by the host service entrypoint",
        )),
        Command::Skill(_) => Err(CowshedError::internal(
            "skill commands must be dispatched before the runtime bridge",
        )),
        Command::Help(_) => Err(CowshedError::internal(
            "help is answered before the runtime bridge",
        )),
        Command::Version => Err(CowshedError::internal(
            "version is answered before the runtime bridge",
        )),
    }
}

pub(crate) fn success() -> DispatchExit {
    DispatchExit { code: 0 }
}

fn adopt_options(args: AdoptArgs) -> Result<AdoptOptions> {
    Ok(AdoptOptions {
        path: args.path,
        repo_id: args.repo_id.map(os_repo_id).transpose()?,
        capacity: args.capacity.map(os_capacity).transpose()?,
        quarantine: args.quarantine,
        re_adopt: args.re_adopt,
    })
}

/// Validate and normalise a capacity at the CLI boundary, so a malformed size is refused before
/// any host state is touched and the size the CLI reports is the one the image is sized to.
fn os_capacity(value: std::ffi::OsString) -> Result<String> {
    let text = os_utf8(value)?;
    ImageCapacity::parse(&text)
        .map(|capacity| capacity.to_string())
        .map_err(|error| {
            usage(
                error.to_string(),
                "use a capacity such as 100g, 200g, or 1t",
            )
        })
}

fn os_repo_id(value: std::ffi::OsString) -> Result<RepoId> {
    os_repo_id_ref(&value)
}

fn os_repo_id_ref(value: &std::ffi::OsStr) -> Result<RepoId> {
    let value = value.to_str().ok_or_else(|| {
        usage(
            "this option requires valid UTF-8",
            "use UTF-8 for control options; child argv may contain arbitrary Unix bytes",
        )
    })?;
    RepoId::parse(value).map_err(|error| {
        usage(
            format!("invalid repository identity: {error}"),
            "use an explicit owner/repository identity",
        )
    })
}

fn os_revision(value: std::ffi::OsString) -> Result<RevisionTarget> {
    RevisionTarget::parse_cli(os_utf8(value)?).map_err(|error| {
        usage(
            format!("invalid revision: {error}"),
            "use a branch, full ref, or full object id",
        )
    })
}

fn os_branch(value: std::ffi::OsString) -> Result<BranchName> {
    BranchName::new(os_utf8(value)?)
        .map_err(|error| usage(error.to_string(), "use a valid local branch name"))
}

fn os_git_oid(value: std::ffi::OsString) -> Result<GitOid> {
    GitOid::new(os_utf8(value)?).map_err(|error| {
        usage(
            error.to_string(),
            "use a full lowercase 40- or 64-hex object id",
        )
    })
}

/// `--ref-lock`, resolved against this process's directory: the land opens it by that path.
fn os_ref_lock(value: std::ffi::OsString) -> Result<RefLockPath> {
    let path = std::path::absolute(&value).map_err(|error| {
        usage(
            format!(
                "cannot resolve the ref lock {}: {error}",
                std::path::Path::new(&value).display()
            ),
            "name the ref lock file by an absolute path",
        )
    })?;
    RefLockPath::new(path)
        .map_err(|error| usage(error.to_string(), "name the ref lock file by a UTF-8 path"))
}

fn os_incarnation(value: std::ffi::OsString) -> Result<WorkspaceIncarnation> {
    WorkspaceIncarnation::new(os_utf8(value)?).map_err(|error| {
        usage(
            error.to_string(),
            "find it in `cowshed ls --json` (workspaceIncarnation)",
        )
    })
}

fn os_expected_ref(value: std::ffi::OsString) -> Result<ExpectedRefHead> {
    if value == std::ffi::OsStr::new("missing") {
        Ok(ExpectedRefHead::Missing)
    } else {
        os_git_oid(value).map(ExpectedRefHead::Oid)
    }
}

/// Resolve a `<ws>` argument, falling back to the workspace the command was run inside.
///
/// An explicit argument always wins — inference never overrides what the caller named. Otherwise
/// the cwd is resolved by containment in exactly one currently mounted workspace, which is
/// authoritative because mount identity is keyed off the in-image marker, and which already
/// refuses an ambiguous match rather than picking one.
///
/// Only verbs that act on a workspace *in place* get this. Verbs that retire, replace, rename, or
/// unmount one require it to be named, so that losing the workspace you are standing in is always
/// something you asked for by name rather than something the cwd decided for you.
async fn resolve_workspace(
    service: &mut dyn CliService,
    workspace: Option<String>,
    verb: &str,
) -> Result<String> {
    if let Some(workspace) = workspace {
        return Ok(workspace);
    }
    let cwd = invocation_cwd()?;
    match service.workspace_at(cwd).await {
        Ok(info) => Ok(info.workspace.to_string()),
        Err(error) if error.code == ErrorCode::NotFound => Err(usage(
            format!("{verb} requires a workspace"),
            format!("name one — cowshed {verb} <ws> — or run it inside a mounted workspace"),
        )),
        Err(error) => Err(error),
    }
}

fn invocation_cwd() -> Result<PathBuf> {
    std::env::current_dir().map_err(|error| {
        CowshedError::environment_missing(
            format!("could not determine the invocation directory: {error}"),
            "run cowshed from an accessible workspace directory",
        )
    })
}

fn os_utf8(value: std::ffi::OsString) -> Result<String> {
    value.into_string().map_err(|_| {
        usage(
            "this option requires valid UTF-8",
            "use UTF-8 for control options; child argv may contain arbitrary Unix bytes",
        )
    })
}

pub(crate) fn exec_command<R: AsyncRead + Send + 'static>(
    args: ExecArgs,
    stdin: R,
) -> Result<ExecCommand> {
    let argv: Vec<CommandArg> = args.argv.into_iter().map(CommandArg::from).collect();
    validate_command_argv(&argv).map_err(|error| {
        usage(
            format!("invalid child argv: {error}"),
            "remove NUL bytes and keep argv within the documented size limits",
        )
    })?;
    let stdin = match args.stdin {
        None => CoreStdinSource::Empty,
        Some(CliStdinSource::Stream) => CoreStdinSource::Stream(Box::pin(stdin)),
        Some(CliStdinSource::WorkspaceFile(path)) => {
            CoreStdinSource::WorkspaceFile(workspace_path(path, "stdin file")?)
        }
        Some(CliStdinSource::InlineBase64(data)) => {
            let text = os_utf8(data)?;
            let decoded = base64::engine::general_purpose::STANDARD
                .decode(text.as_bytes())
                .map_err(|error| {
                    usage(
                        format!("invalid --stdin-base64 data: {error}"),
                        "use canonical base64 input",
                    )
                })?;
            CoreStdinSource::Inline(Bytes::from(decoded))
        }
    };
    let policy = if args.replace_output {
        PublicationPolicy::Replace
    } else {
        PublicationPolicy::CreateNew
    };
    let publication = |path: PathBuf, label| {
        Ok(OutputPublication {
            path: workspace_path(path, label)?,
            policy,
        })
    };
    let timeout = args.timeout.map(parse_duration).transpose()?;
    Ok(ExecCommand {
        workspace: args.workspace,
        request: ExecRequest {
            command: cowshed_core::api::ExecCommand::Argv(argv),
            cwd: args
                .cwd
                .map(|path| workspace_path(path, "cwd"))
                .transpose()?,
            mode: if args.read_only {
                RunSandboxMode::ReadOnly
            } else {
                RunSandboxMode::ReadWrite
            },
            env: host_gate_environment(args.scope, std::env::vars_os())?,
            trace: None,
            stdin,
            stdout_copy: args
                .stdout_copy
                .map(|path| publication(path, "stdout copy"))
                .transpose()?,
            stderr_copy: args
                .stderr_copy
                .map(|path| publication(path, "stderr copy"))
                .transpose()?,
            admission_key: None,
            class: args.class,
            want: args.want,
            scope: args.scope,
        },
        session: args.session,
        background: args.background,
        timeout,
    })
}

/// A HostGate request carries the operator's ambient environment, including custom project
/// variables, because its child starts from an empty environment. Other scopes forward nothing
/// here, preserving ordinary Workspace behavior. The supervisor's existing caller_environment
/// owns reserved/credential filtering; the public-name list limits records, not execution.
/// The request carries UTF-8 names and values; an unrepresentable variable is refused, not dropped.
fn host_gate_environment(
    scope: cowshed_core::api::ExecScope,
    ambient: impl Iterator<Item = (std::ffi::OsString, std::ffi::OsString)>,
) -> Result<HashMap<String, String>> {
    if scope != cowshed_core::api::ExecScope::HostGate {
        return Ok(HashMap::new());
    }
    let mut forwarded = HashMap::new();
    for (name, value) in ambient {
        let name = name.into_string().map_err(|_| {
            usage(
                "the host gate forwards only UTF-8 environment names",
                "export the variable with a UTF-8 name or unset it",
            )
        })?;
        let value = value.into_string().map_err(|_| {
            usage(
                format!("the host gate forwards only UTF-8 values, and {name} is not"),
                "export the variable as UTF-8 or unset it",
            )
        })?;
        forwarded.insert(name, value);
    }
    Ok(forwarded)
}

fn workspace_path(path: PathBuf, label: &str) -> Result<WorkspacePath> {
    WorkspacePath::new(path).map_err(|error| {
        usage(
            format!("invalid {label} path: {error}"),
            "use a normalized workspace-relative path",
        )
    })
}

fn parse_duration(value: std::ffi::OsString) -> Result<Duration> {
    let value = os_utf8(value)?;
    let (digits, multiplier) = if let Some(digits) = value.strip_suffix("ms") {
        (digits, 1_u64)
    } else if let Some(digits) = value.strip_suffix('s') {
        (digits, 1_000)
    } else if let Some(digits) = value.strip_suffix('m') {
        (digits, 60_000)
    } else if let Some(digits) = value.strip_suffix('h') {
        (digits, 3_600_000)
    } else {
        return Err(usage(
            "timeout must end in ms, s, m, or h",
            "for example: --timeout 500ms or --timeout 2m",
        ));
    };
    let count = digits.parse::<u64>().map_err(|_| {
        usage(
            "timeout must be a non-negative integer duration",
            "for example: --timeout 500ms or --timeout 2m",
        )
    })?;
    let millis = count.checked_mul(multiplier).ok_or_else(|| {
        usage(
            "timeout is too large",
            "choose a timeout that fits in 64-bit milliseconds",
        )
    })?;
    Ok(Duration::from_millis(millis))
}

pub(crate) fn emit_mount<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    info: &WorkspaceInfo,
) -> Result<()> {
    emit_mount_path(
        output,
        json,
        &info.workspace,
        &info.mount,
        info.base_commit.as_ref(),
    )
}

/// What `path` and every verb that makes a workspace answer with: its mount.
pub(crate) fn emit_mount_path<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    workspace: &WorkspaceName,
    mount: &Path,
    base_commit: Option<&GitOid>,
) -> Result<()> {
    if json {
        output
            .success(MountResult {
                workspace: workspace.clone(),
                mount: mount.to_path_buf(),
                base_commit: base_commit.cloned(),
            })
            .map_err(output_error)
    } else {
        output
            .bare_line(mount.as_os_str().as_bytes())
            .map_err(output_error)
    }
}

/// JSON callers get the job's record when it settles; everyone else gets its bytes as they come.
pub(crate) fn exec_presentation(json: bool) -> ExecPresentation {
    if json {
        ExecPresentation::Control
    } else {
        ExecPresentation::Raw
    }
}

/// How an exec ends: the child's own exit status, the job id of a job asked to run on, or a
/// conflict for a job still running when the caller's timeout passed — never a success that
/// stands in for a command that has not ended.
pub(crate) fn report_exec<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    workspace: &str,
    result: ExecResult,
) -> Result<DispatchExit> {
    let job = result.info.job_id.get();
    let reach = format!(
        "reach it through the cowshed API: worker(\"{workspace}\").job({job}) waits for it, \
         reads its logs or kills it; no CLI verb reattaches to a job"
    );
    match result.end {
        ExecEnd::StillRunning { after } => {
            let error = CowshedError::conflict(
                format!(
                    "job {job} still runs in workspace {workspace} after the {after:?} timeout; \
                     its output and exit status are not this command's"
                ),
                reach,
            );
            if json {
                output.json_error(error).map_err(output_error)?;
            } else {
                output.error(&error.message).map_err(output_error)?;
                output.hint(&error.hint).map_err(output_error)?;
            }
            Ok(DispatchExit {
                code: i32::from(ErrorCode::Conflict.exec_wrapper_exit_code()),
            })
        }
        ExecEnd::Backgrounded if json => {
            output.success(result.info).map_err(output_error)?;
            Ok(success())
        }
        ExecEnd::Backgrounded => {
            output
                .bare_line(job.to_string().as_bytes())
                .map_err(output_error)?;
            output
                .guidance(&format!("job {job} runs in workspace {workspace}; {reach}"))
                .map_err(output_error)?;
            Ok(success())
        }
        ExecEnd::Finished if json => {
            output.success(result.info).map_err(output_error)?;
            Ok(success())
        }
        ExecEnd::Finished => Ok(DispatchExit {
            code: child_exit_code(&result.info)?,
        }),
    }
}

/// Egress rules for hosts named on the command line: the ports a grant defaults to, in the mode
/// the invocation names (`--opaque`, or intercepted). A host holds one rule, so the controller
/// replaces any rule the host already has with this one.
fn egress_rules(hosts: Vec<String>, mode: EgressMode) -> Vec<EgressRule> {
    hosts
        .into_iter()
        .map(|host| EgressRule {
            host,
            ports: Vec::new(),
            mode,
        })
        .collect()
}

async fn revoke<S: CliService, W: Write, E: Write>(
    service: &mut S,
    output: &mut Output<W, E>,
    json: bool,
    args: RevokeArgs,
) -> Result<DispatchExit> {
    let revision = match args.target {
        GrantTarget::Workspace(workspace) => {
            let delta = match args.selection {
                RevokeSelection::Items(delta) => delta,
                RevokeSelection::All => {
                    let grants = service.grants(&workspace).await?;
                    GrantDelta {
                        read: grants.read,
                        write: grants.write,
                        deny_write: grants.deny_write,
                        deny: grants.deny,
                        egress: grants.egress,
                        repos: grants.repos,
                        sim: grants.sim,
                        loopback_any: grants.loopback_any,
                        expected_revision: Some(grants.revision),
                        ..GrantDelta::default()
                    }
                }
            };
            let grants = service.revoke(&workspace, delta).await?;
            let revision = grants.revision;
            if json {
                output.success(grants).map_err(output_error)?;
            }
            output
                .hint(&format!("cowshed exec {workspace} -- <retry your command>"))
                .map_err(output_error)?;
            revision
        }
        GrantTarget::Project => {
            let delta = match args.selection {
                RevokeSelection::Items(delta) => ProjectGrantDelta {
                    read: delta.read,
                    deny_write: delta.deny_write,
                    deny: delta.deny,
                    egress: delta.egress,
                    expected_revision: None,
                },
                RevokeSelection::All => {
                    let grants = service.project_grants().await?;
                    ProjectGrantDelta {
                        read: grants.read,
                        deny_write: grants.deny_write,
                        deny: grants.deny,
                        egress: grants.egress,
                        expected_revision: Some(grants.revision),
                    }
                }
            };
            let grants = service.revoke_project(delta).await?;
            let revision = grants.revision;
            if json {
                output.success(grants).map_err(output_error)?;
            }
            output
                .hint("cowshed grant --project-wide")
                .map_err(output_error)?;
            revision
        }
    };
    if !json {
        output
            .bare_line(revision.to_string().as_bytes())
            .map_err(output_error)?;
    }
    output.guidance("egress policy applies immediately; effective filesystem changes apply at the next supervisor revision").map_err(output_error)?;
    Ok(success())
}

/// `cowshed grant --project-wide`: print or extend the project's standing grants.
async fn grant_project<S: CliService, W: Write, E: Write>(
    service: &mut S,
    output: &mut Output<W, E>,
    json: bool,
    read: Vec<PathBuf>,
    deny_write: Vec<PathBuf>,
    deny: Vec<PathBuf>,
    egress: Vec<EgressRule>,
) -> Result<DispatchExit> {
    let changed =
        !read.is_empty() || !deny_write.is_empty() || !deny.is_empty() || !egress.is_empty();
    let grants = if changed {
        service
            .grant_project(ProjectGrantDelta {
                read,
                deny_write,
                deny,
                egress,
                expected_revision: None,
            })
            .await?
    } else {
        service.project_grants().await?
    };
    if json {
        output.success(grants.clone()).map_err(output_error)?;
    } else if !changed {
        emit_grants(
            output,
            &GrantSet {
                read: grants.read.clone(),
                deny_write: grants.deny_write.clone(),
                deny: grants.deny.clone(),
                egress: grants.egress.clone(),
                ..GrantSet::default()
            },
        )?;
    }
    if changed {
        output
            .guidance(&format!(
                "project grants now: {} read, {} denied writes, {} denied paths, {} egress (revision {})",
                grants.read.len(),
                grants.deny_write.len(),
                grants.deny.len(),
                grants.egress.len(),
                grants.revision
            ))
            .map_err(output_error)?;
        output
            .guidance(
                "every workspace of this project holds them from its next exec or shell, on top of its own grants",
            )
            .map_err(output_error)?;
        output
            .hint("cowshed exec <ws> -- <retry your command>")
            .map_err(output_error)?;
    }
    Ok(success())
}

/// Every grant this workspace holds, one per line: what it may read, write, and reach.
///
/// Network reach is listed beside filesystem reach because it is a grant like any other — a
/// host an operator admitted and can be asked to justify. Leaving it out of the listing was how
/// `cowshed grant <ws>` could answer "nothing here" for a workspace that could reach a
/// registry. The ports are the effective ones, so the line says what is admitted rather than
/// what happened to be typed.
fn emit_grants<W: Write, E: Write>(output: &mut Output<W, E>, grants: &GrantSet) -> Result<()> {
    for (kind, paths) in [
        (b"read".as_slice(), &grants.read),
        (b"write".as_slice(), &grants.write),
        (b"deny-write".as_slice(), &grants.deny_write),
        (b"deny".as_slice(), &grants.deny),
    ] {
        for path in paths {
            output.bare(kind).map_err(output_error)?;
            output.bare(b"\t").map_err(output_error)?;
            output
                .bare(path.as_os_str().as_bytes())
                .map_err(output_error)?;
            output.bare(b"\n").map_err(output_error)?;
        }
    }
    for rule in &grants.egress {
        let ports = rule
            .effective_ports()
            .iter()
            .map(u16::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let mode = match rule.mode {
            EgressMode::Intercept => "intercept",
            EgressMode::Opaque => "opaque",
        };
        output
            .bare_line(format!("egress\t{}\t{ports}\t{mode}", rule.host).as_bytes())
            .map_err(output_error)?;
    }
    if grants.loopback_any {
        output.bare_line(b"loopback\tany").map_err(output_error)?;
    }
    Ok(())
}

/// Say out loud which projects the gateway was not told about, without failing the command.
///
/// The mounts happened; the daemon's view of them did not. A caller who does not hear this
/// believes the gateway can see workspaces it cannot, which is the failure mode that made this
/// worth reporting rather than swallowing.
pub(crate) fn report_stale_gateway_views<W: Write, E: Write>(
    output: &mut Output<W, E>,
    stale: &[(RepoId, CowshedError)],
) -> Result<()> {
    for (repo_id, error) in stale {
        output
            .guidance(&format!(
                "{repo_id}: mounts changed, but the gateway was not told: {}",
                error.message
            ))
            .map_err(output_error)?;
        output.hint(&error.hint).map_err(output_error)?;
    }
    Ok(())
}

fn emit_attached<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    infos: &[WorkspaceInfo],
) -> Result<()> {
    match infos {
        [] => Err(no_detached_sessions()),
        [info] => emit_mount(output, json, info),
        many => {
            if json {
                output.success(many.to_vec()).map_err(output_error)
            } else {
                for info in many {
                    output
                        .bare_line(info.mount.as_os_str().as_bytes())
                        .map_err(output_error)?;
                }
                Ok(())
            }
        }
    }
}

fn is_detached_session(info: &WorkspaceInfo) -> bool {
    info.role != WorkspaceRole::Main && info.state == WorkspaceState::Detached
}

fn no_detached_sessions() -> CowshedError {
    CowshedError::not_found(
        "no detached session workspace found",
        "cowshed ls; cowshed attach <ws>; cowshed attach --all",
    )
}

/// What `attach --all` reports when no session needed attaching.
///
/// Mains are always-mounted, never user-detachable, so zero detached sessions alongside
/// unmounted mains means the bare "no detached session" line would hide the actual defect.
/// The mains status line names it instead, with the gateway pass that mounts mains as the
/// remedy — the same hint `doctor` gives an unmounted main, which
/// `attach_mains_hint_matches_doctor_main_hint` pins.
fn no_detached_with_mains(unmounted: &[UnreachableMain]) -> CowshedError {
    if unmounted.is_empty() {
        return no_detached_sessions();
    }
    let repos = unmounted
        .iter()
        .map(|main| main.repo_id.to_string())
        .collect::<Vec<_>>()
        .join(", ");
    CowshedError::not_found(
        format!(
            "no detached session workspace found; {} main workspace{} not mounted: {repos}",
            unmounted.len(),
            if unmounted.len() == 1 { "" } else { "s" },
        ),
        "cowshed gateway start",
    )
}
async fn attach_scoped_sessions(
    service: &mut dyn CliService,
    all: bool,
    options: AttachOptions,
) -> Result<Vec<WorkspaceInfo>> {
    if !all {
        match service.workspace_at(invocation_cwd()?).await {
            Ok(_) => {}
            Err(error) if error.code == ErrorCode::Conflict => {
                return Err(CowshedError::conflict(
                    error.message,
                    "name one workspace or repair overlapping mounts",
                ));
            }
            Err(error) if error.code == ErrorCode::NotFound => {}
            Err(error) => return Err(error),
        }
    }
    let candidates = if all {
        service
            .list_all()
            .await?
            .into_iter()
            .flat_map(|project| project.workspaces)
            .collect()
    } else {
        service.list(None).await?.workspaces
    };
    let targets: Vec<WorkspaceInfo> = candidates.into_iter().filter(is_detached_session).collect();
    if targets.is_empty() {
        return Err(no_detached_sessions());
    }
    let mut attached = Vec::with_capacity(targets.len());
    let mut first_error = None;
    for target in targets {
        match service
            .attach(target.workspace.as_str(), options.clone())
            .await
        {
            Ok(info) => attached.push(info),
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: skipping workspace {}/{} during attach: {error}",
                    target.repo_id,
                    target.workspace
                );
                first_error.get_or_insert(error);
            }
        }
    }
    if attached.is_empty()
        && let Some(error) = first_error
    {
        return Err(error);
    }
    Ok(attached)
}

/// What a store-wide mount change did, and which projects the gateway was not told about.
///
/// The mounts have already happened by the time this is built, so a gateway that does not answer
/// cannot turn the command into a failure — reporting one for work that completed teaches the
/// caller to retry an idempotent operation forever. It cannot be silence either: the daemon's view
/// of those sessions is stale until something reconciles it, and this is the only place that knows.
pub(crate) struct StoreWideMounts<T> {
    pub(crate) result: T,
    pub(crate) stale: Vec<(RepoId, CowshedError)>,
}

/// Tell the gateway that a project's session mounts changed, keeping any refusal as a value.
///
/// Project-scoped attach reaches this through `ActorBridge::reconcile_gateway`; the store-wide
/// arms never enter that path, which is why they call it here. Without it a host-wide attach
/// mounts volumes the daemon does not know exist until the next project-scoped exec.
struct StoreWidePortSlotAssigner<'a>(&'a Path);

#[async_trait]
impl gateway_service::PortSlotAssigner for StoreWidePortSlotAssigner<'_> {
    async fn assign_port_slot(&self, workspace: &str, slot: u32) -> Result<()> {
        let scope = RecoveryScope::Workspaces(
            [WorkspaceName::new(workspace)
                .map_err(|error| CowshedError::usage(error.to_string(), "name a workspace"))?]
            .into(),
        );
        let bridge = ActorBridge::open_existing(self.0, scope).await?;
        let assigned = match bridge.coordinator() {
            Ok(coordinator) => coordinator.assign_slot(workspace, slot).await,
            Err(error) => Err(error),
        };
        let shutdown = bridge.shutdown().await;
        match assigned {
            Ok(()) => shutdown,
            Err(primary) => Err(merge_primary(primary, shutdown.err())),
        }
    }
}

async fn reconcile_after_mount_change(
    project: &AdoptedProject,
    storage: &ValidatedHostStorage,
) -> Option<CowshedError> {
    let assigner = StoreWidePortSlotAssigner(&project.project_root);
    gateway_service::reconcile_native_project(&project.repo_id, storage, &assigner)
        .await
        .err()
}

pub(crate) async fn attach_store_wide(browse: bool) -> Result<StoreWideMounts<Vec<WorkspaceInfo>>> {
    let storage = host_storage().await?;
    let projects = NativeGatewayInventory::new(storage.clone())
        .adopted_projects()
        .await
        .map_err(project_inventory_error)?;
    let mut attached = Vec::new();
    let mut stale = Vec::new();
    let mut first_error = None;
    for project in &projects {
        let mounted = match attach_project_sessions_from_store(&storage, project, browse).await {
            Ok(mounted) => mounted,
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: skipping project {} during attach --all: {error}",
                    project.repo_id
                );
                first_error.get_or_insert(error);
                continue;
            }
        };
        if mounted.is_empty() {
            continue;
        }
        attached.extend(mounted);
        if let Some(error) = reconcile_after_mount_change(project, &storage).await {
            stale.push((project.repo_id.clone(), error));
        }
    }
    if attached.is_empty() {
        if let Some(error) = first_error {
            return Err(error);
        }
        let unmounted = NativeGatewayInventory::new(storage.clone())
            .unmounted_mains()
            .await
            .unwrap_or_default();
        return Err(no_detached_with_mains(&unmounted));
    }
    Ok(StoreWideMounts {
        result: attached,
        stale,
    })
}

/// Attach a project's detached sessions from store metadata and APFS facts alone.
///
/// Store-wide attach must not open `project.project_root`: a stale or missing checkout remote
/// does not invalidate the image sidecar, repository binding, or canonical mount layout needed to
/// mount a session. Main is intentionally excluded because it is an always-mounted host invariant,
/// not a user-detachable session.
async fn attach_project_sessions_from_store(
    storage: &ValidatedHostStorage,
    project: &AdoptedProject,
    browse: bool,
) -> Result<Vec<WorkspaceInfo>> {
    let layout = StorageLayout::new(storage.store(), &project.repo_id)
        .map_err(attach_store_storage_error)?;
    let config = ApfsSubstrateConfig::for_storage(storage, &project.project_root);
    let host =
        MacOsApfsExecutionHost::new(SystemCommandRunner::new(config.namespace), config.clone())
            .map_err(attach_store_storage_error)?;
    let substrate = ApfsSubstrate::new(config, host);
    let derived = substrate
        .list(&project.repo_id)
        .await
        .map_err(attach_store_storage_error)?;
    report_unproven(&project.repo_id, &derived, "attach --all");
    let targets = derived
        .into_iter()
        .filter(|derived| {
            derived.workspace.role() != WorkspaceRole::Main
                && derived.mount_state == MountState::Detached
        })
        .collect::<Vec<_>>();
    let mut attached = Vec::with_capacity(targets.len());
    let mut first_error = None;
    for target in targets {
        let name = target.workspace.name();
        let result = async {
            let mut info = store_workspace_info(&layout, &target)?;
            info.mount = substrate
                .ensure_mounted(&target.workspace, MountIntent { browse })
                .await
                .map_err(attach_store_storage_error)?;
            info.state = WorkspaceState::Attached;
            Ok::<_, CowshedError>(info)
        }
        .await;
        match result {
            Ok(info) => attached.push(info),
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: skipping workspace {}/{name} during attach --all: {error}",
                    project.repo_id
                );
                first_error.get_or_insert(error);
            }
        }
    }
    if attached.is_empty()
        && let Some(error) = first_error
    {
        return Err(error);
    }
    Ok(attached)
}

/// Say each unproven session a store-wide verb leaves alone. Its mount does not prove itself the
/// workspace's, so it is neither attached nor detached and none of `attach --all`'s or
/// `detach --all`'s to act on.
fn report_unproven(repo_id: &RepoId, derived: &[DerivedWorkspace], verb: &str) {
    for workspace in derived {
        if let MountState::Unproven(refusal) = &workspace.mount_state {
            cowshed_os::eprintln!(
                "cowshed: skipping workspace {repo_id}/{} during {verb}: {refusal}",
                workspace.workspace.name()
            );
        }
    }
}

fn store_workspace_info(
    layout: &StorageLayout,
    derived: &DerivedWorkspace,
) -> Result<WorkspaceInfo> {
    let image = layout
        .session_image(derived.workspace.name())
        .map_err(attach_store_storage_error)?;
    let metadata = DetachedWorkspaceMetadata::read_for_image(image.image())
        .map_err(attach_store_storage_error)?;
    let snapshot = &metadata.info_snapshot;
    let base_commit =
        Some(GitOid::new(snapshot.base_commit.clone()).map_err(attach_store_storage_error)?);
    let created_at =
        Some(UtcTimestamp::new(snapshot.created_at.clone()).map_err(attach_store_storage_error)?);
    Ok(WorkspaceInfo {
        repo_id: derived.workspace.repo().clone(),
        workspace: derived.workspace.name().clone(),
        workspace_incarnation: derived.workspace.incarnation().clone(),
        role: derived.workspace.role(),
        mount: layout
            .workspace_mount(derived.workspace.name())
            .map_err(attach_store_storage_error)?,
        state: WorkspaceState::Detached,
        branch: snapshot.branch.clone(),
        base_commit,
        created_at,
        checkpoints: derived
            .checkpoints
            .iter()
            .map(|checkpoint| CheckpointInfo {
                label: checkpoint.label.to_string(),
                revision: checkpoint.revision.get(),
                pinned: checkpoint.pin == Pin::Pinned,
            })
            .collect(),
        snapshot_stale: snapshot.stale,
        landing: None,
    })
}

fn attach_store_storage_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(
        format!("store-wide attach could not read or mount a session: {error}"),
        "cowshed doctor --json",
    )
}

fn is_attached_session(info: &WorkspaceInfo) -> bool {
    info.role != WorkspaceRole::Main && info.state == WorkspaceState::Attached
}

fn no_attached_sessions() -> CowshedError {
    CowshedError::not_found(
        "no attached session workspace found",
        "cowshed ls; cowshed detach <ws>; cowshed detach --all",
    )
}

async fn detach_scoped_sessions(service: &mut dyn CliService) -> Result<()> {
    let targets: Vec<WorkspaceInfo> = service
        .list_all()
        .await?
        .into_iter()
        .flat_map(|project| project.workspaces)
        .filter(is_attached_session)
        .collect();
    if targets.is_empty() {
        return Err(no_attached_sessions());
    }
    let mut detached = false;
    let mut first_error = None;
    for target in targets {
        match service.detach(target.workspace.as_str()).await {
            Ok(()) => detached = true,
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: skipping workspace {}/{} during detach: {error}",
                    target.repo_id,
                    target.workspace
                );
                first_error.get_or_insert(error);
            }
        }
    }
    if !detached && let Some(error) = first_error {
        return Err(error);
    }
    Ok(())
}

pub(crate) async fn detach_store_wide() -> Result<StoreWideMounts<()>> {
    let storage = host_storage().await?;
    let projects = NativeGatewayInventory::new(storage.clone())
        .adopted_projects()
        .await
        .map_err(project_inventory_error)?;
    let mut detached = 0usize;
    let mut stale = Vec::new();
    let mut first_error = None;
    for project in &projects {
        let unmounted = match detach_project_sessions_from_store(&storage, project).await {
            Ok(unmounted) => unmounted,
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: skipping project {} during detach --all: {error}",
                    project.repo_id
                );
                first_error.get_or_insert(error);
                continue;
            }
        };
        if unmounted == 0 {
            continue;
        }
        detached += unmounted;
        if let Some(error) = reconcile_after_mount_change(project, &storage).await {
            stale.push((project.repo_id.clone(), error));
        }
    }
    if detached == 0 {
        if let Some(error) = first_error {
            return Err(error);
        }
        return Err(no_attached_sessions());
    }
    Ok(StoreWideMounts { result: (), stale })
}

/// Detach a project's mounted sessions from store metadata and kernel mount facts alone.
///
/// In particular, this does not open `project.project_root` as a Git repository. Store-wide
/// detach must keep working when an adopted checkout's recorded remote name is stale or absent:
/// the image sidecar and repository binding already provide every identity unmount needs.
async fn detach_project_sessions_from_store(
    storage: &ValidatedHostStorage,
    project: &AdoptedProject,
) -> Result<usize> {
    let config = ApfsSubstrateConfig::for_storage(storage, &project.project_root);
    let host =
        MacOsApfsExecutionHost::new(SystemCommandRunner::new(config.namespace), config.clone())
            .map_err(detach_store_storage_error)?;
    let substrate = ApfsSubstrate::new(config, host);
    let derived = substrate
        .list(&project.repo_id)
        .await
        .map_err(detach_store_storage_error)?;
    report_unproven(&project.repo_id, &derived, "detach --all");
    let targets = derived
        .into_iter()
        .filter(|derived| {
            !derived.workspace.name().is_main()
                && matches!(derived.mount_state, MountState::Mounted { .. })
        })
        .map(|derived| derived.workspace)
        .collect::<Vec<_>>();
    let mut detached = 0;
    let mut first_error = None;
    for target in &targets {
        match substrate
            .unmount(target)
            .await
            .map_err(detach_store_storage_error)
        {
            Ok(()) => detached += 1,
            Err(error) => {
                cowshed_os::eprintln!(
                    "cowshed: skipping workspace {}/{} during detach --all: {error}",
                    project.repo_id,
                    target.name()
                );
                first_error.get_or_insert(error);
            }
        }
    }
    if detached == 0
        && let Some(error) = first_error
    {
        return Err(error);
    }
    Ok(detached)
}

fn detach_store_storage_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(
        format!("store-wide detach could not read or unmount a session: {error}"),
        "cowshed doctor --json",
    )
}

/// Resolve a session workspace's owning checkout from the store readdir.
///
/// Identity is the image sidecar's `infoSnapshot.projectRoot` (the same project root the
/// in-image marker records). Remotes and cwd git discovery are not consulted. Owners are the
/// store's marked owners, the one enumeration every store walker uses; a reserved name is
/// never read as a repository.
pub(crate) fn resolve_session_project_root(
    store: &Path,
    workspace: &str,
    verb: &str,
) -> Result<PathBuf> {
    let wanted = WorkspaceName::session(workspace).map_err(|error| {
        usage(
            error.to_string(),
            format!("cowshed {verb} <ws>; cowshed ls --all"),
        )
    })?;
    let mut found: Vec<(RepoId, PathBuf)> = Vec::new();
    let owners = cowshed_core::storage::bootstrap::project_owner_directories(store)
        .map_err(|error| CowshedError::environment_missing(error.to_string(), "cowshed setup"))?;
    for owner in owners {
        let repos = fs::read_dir(&owner).map_err(|error| {
            CowshedError::environment_missing(
                format!("could not read {}: {error}", owner.display()),
                "cowshed setup",
            )
        })?;
        for repo in repos {
            let repo = repo.map_err(|error| {
                CowshedError::environment_missing(
                    format!("could not read {}: {error}", owner.display()),
                    "cowshed setup",
                )
            })?;
            let reserved = repo
                .file_name()
                .to_str()
                .is_none_or(cowshed_core::storage::bootstrap::is_reserved_store_namespace);
            if reserved || !dirent_is_plain_dir(&repo) {
                continue;
            }
            let sessions = repo.path().join("sessions");
            let Ok(entries) = fs::read_dir(&sessions) else {
                continue;
            };
            let images = discover_session_images(
                entries.filter_map(|entry| entry.ok().map(|entry| entry.path())),
            );
            for image in images {
                if image.workspace() != &wanted {
                    continue;
                }
                let metadata =
                    DetachedWorkspaceMetadata::read_for_image(image.path()).map_err(|error| {
                        CowshedError::integrity(
                            format!(
                                "workspace {wanted} sidecar at {} is unreadable: {error}",
                                image.path().display()
                            ),
                            "cowshed doctor",
                        )
                    })?;
                let snapshot = &metadata.info_snapshot;
                found.push((metadata.repo_id.clone(), snapshot.project_root.clone()));
            }
        }
    }
    match found.as_slice() {
        [] => Err(CowshedError::not_found(
            format!("no session workspace {workspace} in the store"),
            format!("cowshed ls --all; cowshed --project <git-root> {verb} <ws>"),
        )),
        [(_, root)] => Ok(root.clone()),
        many => {
            let projects = many
                .iter()
                .map(|(repo_id, _)| repo_id.to_string())
                .collect::<Vec<_>>()
                .join(", ");
            Err(CowshedError::conflict(
                format!("workspace {workspace} exists in more than one project ({projects})"),
                format!("cowshed --project <git-root> {verb} <ws>"),
            ))
        }
    }
}

fn dirent_is_plain_dir(entry: &fs::DirEntry) -> bool {
    let name = entry.file_name();
    let Some(name) = name.to_str() else {
        return false;
    };
    if name.starts_with('.') {
        return false;
    }
    entry.file_type().is_ok_and(|kind| kind.is_dir())
}

fn git_identity_findings(bridge: &ActorBridge) -> Result<Vec<Finding>> {
    let layout = StorageLayout::new(bridge.store_root()?, bridge.repo_id()?)
        .map_err(|error| CowshedError::environment_missing(error.to_string(), "cowshed setup"))?;
    let gaps = probe::probe_project(bridge.git_root()?, &layout)?;
    let mount_root = &layout.project().host_mount_root;
    Ok(gaps.iter().map(|gap| gap.finding(mount_root)).collect())
}

async fn resolve_detach_root(cli: &Cli) -> Result<PathBuf> {
    if cli.global.project.is_some() {
        return resolve_project_root(cli).await;
    }
    let Command::Detach(args) = &cli.command else {
        return Err(CowshedError::internal(
            "detach project resolution ran for a non-detach command",
        ));
    };
    let workspace = args.workspace.as_deref().ok_or_else(|| {
        usage(
            "detach requires a workspace",
            "cowshed detach <ws>; cowshed detach --all",
        )
    })?;
    let storage = host_storage().await?;
    resolve_session_project_root(storage.store(), workspace, "detach")
}

/// `cache clear` (06_cli.md, "Cache clear"): the workspace's Nx state, then the project's store,
/// each under its own refusal. A workspace refusal leaves both caches as they were. A store
/// refusal after the workspace was cleared says so, so the failure is never read as nothing
/// having changed.
async fn clear_caches(
    service: &mut dyn CliService,
    workspace: &str,
) -> Result<cowshed_core::api::CacheClearReport> {
    let state = service.clear_nx_state(workspace).await?;
    match service.clear_nx_store().await {
        Ok(store) => Ok(cowshed_core::api::CacheClearReport {
            workspace: state,
            store,
        }),
        Err(mut error) => {
            error.message = format!(
                "workspace {}'s Nx state was cleared ({}), but the project's Nx store was not: {}",
                state.workspace,
                nx_state_entries(&state),
                error.message
            );
            Err(error)
        }
    }
}

fn nx_state_entries(state: &cowshed_core::api::NxStateClearResult) -> String {
    format!(
        "{} entries in .nx/cache, {} in .nx/workspace-data, build volume {}",
        state.cache_entries, state.workspace_data_entries, state.volume
    )
}

/// One stdout line per cache cleared, with its entry count; `--json` is the named report.
fn emit_cache_clear_report<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    report: cowshed_core::api::CacheClearReport,
) -> Result<()> {
    if json {
        return output.success(report).map_err(output_error);
    }
    output
        .bare_line(
            format!(
                "cleared workspace {} Nx state: {}",
                report.workspace.workspace,
                nx_state_entries(&report.workspace)
            )
            .as_bytes(),
        )
        .map_err(output_error)?;
    output
        .bare_line(
            format!(
                "cleared project {} Nx store: {} entries",
                report.store.repo_id.as_str(),
                report.store.entries
            )
            .as_bytes(),
        )
        .map_err(output_error)
}

fn emit_reseed_result<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    result: ReseedResult,
) -> Result<()> {
    if result.dry_run {
        let reduction = i128::from(result.previous_bytes) - i128::from(result.replacement_bytes);
        output
            .guidance(&format!(
                "would discard workspace {} build volume {} ({} allocated bytes) and clone {}'s existing seed {} ({} allocated bytes estimated); estimated allocation reduction: {reduction:+} bytes; no changes made",
                result.workspace,
                result.previous_volume,
                result.previous_bytes,
                result.from,
                result.seed,
                result.replacement_bytes,
            ))
            .map_err(output_error)?;
    } else {
        output
            .guidance(&format!(
                "workspace {} build state replaced from {}'s existing seed {}: {} allocated bytes before, {} after; {} allocated image bytes reclaimed",
                result.workspace,
                result.from,
                result.seed,
                result.previous_bytes,
                result.replacement_bytes,
                result.released_bytes,
            ))
            .map_err(output_error)?;
    }
    output
        .guidance(if result.dry_run {
            "source tree, HEAD, index, dirty and untracked files untouched; execution would discard live build-volume contents, missing incremental work rebuilds cold; no seed refreshed"
        } else {
            "source tree, HEAD, index, dirty and untracked files untouched; build-volume contents discarded, missing incremental work rebuilds cold; no seed refreshed"
        })
        .map_err(output_error)?;
    output
        .guidance("allocated image bytes are GC accounting, not unique physical bytes or a promise of physical free space: APFS clones share extents")
        .map_err(output_error)?;
    if json {
        output.success(result).map_err(output_error)
    } else {
        output
            .bare_line(result.released_bytes.to_string().as_bytes())
            .map_err(output_error)
    }
}

fn emit_gc_candidates<W: Write, E: Write>(
    output: &mut Output<W, E>,
    report: &GcReport,
) -> Result<()> {
    for candidate in &report.candidates {
        output
            .guidance(&format!(
                "would delete {} ({} bytes; reason: {})",
                candidate.path.display(),
                candidate.bytes,
                gc_reason(candidate.reason)
            ))
            .map_err(output_error)?;
    }
    Ok(())
}

const fn gc_reason(reason: GcReason) -> &'static str {
    match reason {
        GcReason::RetiredWorkspace => "workspace was retired",
        GcReason::OrphanStagingImage => "orphaned staging image",
        GcReason::OrphanStagingMetadata => "orphaned staging metadata",
        GcReason::OrphanStagingMount => "orphaned staging mountpoint",
        GcReason::OrphanSessionImage => "orphaned session image without metadata",
        GcReason::OrphanMountpoint => "orphaned mountpoint",
        GcReason::ExpiredCheckpoint => "expired checkpoint",
        GcReason::AbandonedBundle => {
            "abandon bundle of commits main does not contain; main keeps its tips as refs"
        }
        GcReason::UnlinkedBuildVolume => "build volume no checkout links",
        GcReason::SupersededSeed => "seed superseded by its target's latest, or its target retired",
        GcReason::InterruptedBuildVolume => {
            "build volume an interrupted creation registered and never finished"
        }
    }
}

fn emit_push<W: Write, E: Write>(output: &mut Output<W, E>, report: &PushReport) -> Result<()> {
    output
        .bare(report.destination_ref.as_bytes())
        .and_then(|()| output.bare(b"\t"))
        .and_then(|()| output.bare(report.source_head.as_str().as_bytes()))
        .and_then(|()| output.bare(b"\n"))
        .map_err(output_error)
}

/// A passed land preflight as the land's own pins: the target branch, the source head
/// (`--expected-source-head`), the target head (`--expected-target-head`, `missing` for a branch
/// that does not exist yet) and the workspace incarnation (`--expected-workspace-incarnation`).
fn emit_land_preflight<W: Write, E: Write>(
    output: &mut Output<W, E>,
    report: &cowshed_core::api::dto::LandPreflight,
) -> Result<()> {
    let target_head = match &report.target_head {
        cowshed_core::api::dto::ExpectedRefHead::Missing => "missing",
        cowshed_core::api::dto::ExpectedRefHead::Oid(oid) => oid.as_str(),
    };
    output
        .bare(report.target_branch.as_bytes())
        .and_then(|()| output.bare(b"\t"))
        .and_then(|()| output.bare(report.source_head.as_str().as_bytes()))
        .and_then(|()| output.bare(b"\t"))
        .and_then(|()| output.bare(target_head.as_bytes()))
        .and_then(|()| output.bare(b"\t"))
        .and_then(|()| output.bare(report.workspace_incarnation.as_str().as_bytes()))
        .and_then(|()| output.bare(b"\n"))
        .map_err(output_error)
}

fn emit_land<W: Write, E: Write>(output: &mut Output<W, E>, report: &LandReport) -> Result<()> {
    let report = match report {
        LandReport::Delivered(delivery) => return emit_push(output, delivery),
        LandReport::Preflighted(preflight) => return emit_land_preflight(output, preflight),
        LandReport::Landed(landed) => landed,
    };
    output
        .bare(report.target_branch.as_bytes())
        .and_then(|()| output.bare(b"\t"))
        .and_then(|()| output.bare(report.landed_head.as_str().as_bytes()))
        .and_then(|()| output.bare(b"\t"))
        .and_then(|()| {
            output.bare(if report.target_was_checked_out {
                b"true"
            } else {
                b"false"
            })
        })
        .and_then(|()| output.bare(b"\n"))
        .and_then(|()| {
            for line in land_build_volume_lines(&report.build_volume) {
                output.guidance(&line)?;
            }
            Ok(())
        })
        .map_err(output_error)
}

/// What the rebase carried into the workspace's build volume, as one stderr line
/// (16_build_volumes.md, "Rebase carry"); the same value the JSON report carries.
fn rebase_build_volume_line(build: &RebaseBuildVolume) -> String {
    match build {
        RebaseBuildVolume::Skipped { reason } => {
            format!("build volume: carried nothing of the target's Nx cache: {reason}")
        }
        RebaseBuildVolume::Carried { carried, deferred } => format!(
            "build volume: carried {} Nx cache entr{} ({} by hash, {deferred} by deferred task; {} bytes) the target held into the rebased workspace in {} ms{}",
            carried.entries,
            if carried.entries == 1 { "y" } else { "ies" },
            carried.entries - deferred,
            carried.bytes,
            carried.elapsed_ms,
            carried
                .stopped
                .as_ref()
                .map(|stopped| format!("; the carry stopped short: {stopped}"))
                .unwrap_or_default()
        ),
    }
}

/// What the land did with build volumes, one stderr line each (16_build_volumes.md, Land steps
/// 4–7); the seeded/adopted/skipped state is also the controller's durable `LandOutcome` record.
fn land_build_volume_lines(build: &cowshed_core::api::dto::LandBuildVolume) -> Vec<String> {
    use cowshed_core::api::dto::{Adoption, LandSeed};
    let mut lines = Vec::new();
    match &build.seed {
        LandSeed::Unchanged => {}
        LandSeed::Frozen => {
            lines
                .push("build volume the target's seed is frozen from the landed volume".to_owned());
        }
        LandSeed::SealBusy { reason } => lines.push(format!(
            "build volume: the target keeps its previous seed, because the landed volume could not be sealed for capture: {reason}; the next fork's reseed refreezes it once that holder lets go"
        )),
    }
    match &build.adoption {
        Adoption::Skipped { reason } => {
            lines.push(format!("build volume adoption skipped: {reason}"));
        }
        Adoption::Adopted {
            elapsed_ms,
            carried,
            check,
        } => {
            lines.push(format!(
                "build volume: carried {} Nx cache entr{} ({} bytes) the target held into the landed volume in {} ms",
                carried.entries,
                if carried.entries == 1 { "y" } else { "ies" },
                carried.bytes,
                carried.elapsed_ms
            ));
            if let Some(stopped) = &carried.stopped {
                lines.push(format!("build volume: the carry stopped short: {stopped}"));
            }
            lines.push(format!(
                "build volume adopted by the target in {elapsed_ms} ms"
            ));
            lines.push(format!(
                "build volume: cache-only output restoration hit {} Nx task(s) and missed {}",
                check.hits,
                check.misses.len()
            ));

            for miss in &check.misses {
                lines.push(format!(
                    "build volume: cache miss {} (checked hash {}, inputs {}): {}{}",
                    miss.task,
                    miss.hash,
                    miss.inputs_digest.to_hex(),
                    miss.reason,
                    miss.inputs_error
                        .as_ref()
                        .map(|error| format!(": {error}"))
                        .unwrap_or_default()
                ));
            }
            for unattributed in &check.unattributed {
                lines.push(format!(
                    "build volume: `{}` is not judged by {}: {}",
                    unattributed.check, unattributed.cache, unattributed.reason
                ));
            }
            for failed in &check.failed {
                lines.push(format!(
                    "build volume: output restoration for `{}` failed in the target ({}): {}",
                    failed.check,
                    failed
                        .exit
                        .map_or_else(|| "killed".to_owned(), |code| format!("exit {code}")),
                    failed.error
                ));
            }
        }
    }
    lines
}

/// The branch `ls` measures against, which is the same constant `land` merges into and `rm` gates
/// on. Sharing it is the point: a listing that reported against a different branch than the gate
/// enforces would be a listing that lies about what `rm` will do.
const LANDING_TARGET_BRANCH: &str = cowshed_core::runtime::project::DEFAULT_LANDING_BRANCH;

/// The landing measurement is opt-in, and `--landed` opts in by implication: a filter on a fact
/// nobody measured would filter on nothing.
const fn landing_requested(args: crate::args::ListArgs) -> bool {
    args.landing || args.landed
}

/// What a project-scoped `ls` asks the controller to measure. `--landed` asks for the measurement
/// rather than the controller's landed filter, because the CLI names every row the filter withholds
/// for want of an answer, and applies the same `landing::retain_fully_landed` to do so.
const fn listing_landing(args: crate::args::ListArgs) -> Option<ListLanding> {
    if landing_requested(args) {
        Some(ListLanding::Measure)
    } else {
        None
    }
}

fn sort_workspace_listing(workspaces: &mut [WorkspaceInfo]) {
    workspaces.sort_by(|left, right| left.workspace.cmp(&right.workspace));
}

fn sort_project_listing(projects: &mut [ProjectWorkspaces]) {
    projects.sort_by(|left, right| left.repo_id.cmp(&right.repo_id));
    for project in projects {
        sort_workspace_listing(&mut project.workspaces);
    }
}

async fn emit_project_listing<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    args: crate::args::ListArgs,
    mut projects: Vec<ProjectWorkspaces>,
) -> Result<()> {
    if args.landed {
        for project in &mut projects {
            let withheld = cowshed_core::landing::retain_fully_landed(&mut project.workspaces);
            emit_withheld(output, Some(&project.repo_id), &withheld)?;
        }
    }
    if json {
        return output.success(projects).map_err(output_error);
    }
    if args.landing {
        let rows = std::iter::once(landing_header(true))
            .chain(projects.iter().flat_map(|project| {
                project
                    .workspaces
                    .iter()
                    .map(|workspace| landing_row(Some(&project.repo_id), workspace))
            }))
            .collect::<Vec<_>>();
        emit_aligned_rows(output, &rows)?;
    } else if args.landed {
        emit_bare_names(
            output,
            projects.iter().flat_map(|project| &project.workspaces),
        )?;
    } else {
        emit_project_workspaces_table(output, &projects)?;
    }
    emit_kept_abandoned_refs(output, &projects).await
}

/// Name, after `ls --all`'s table, every abandonment each attached project's main keeps refs
/// for because main does not hold it (01_storage.md); gc retires the rest, so they go unnamed.
async fn emit_kept_abandoned_refs<W: Write, E: Write>(
    output: &mut Output<W, E>,
    projects: &[ProjectWorkspaces],
) -> Result<()> {
    // `ls` reads the host's projects, so its Git runs for the host store.
    let roots = cowshed_core::storage::bootstrap::CanonicalRoots::global();
    let host_store = roots.shared_store();
    for project in projects {
        let Some(main) = project.workspaces.iter().find(|workspace| {
            workspace.role == WorkspaceRole::Main && workspace.state != WorkspaceState::Detached
        }) else {
            continue;
        };
        let measured = match cowshed_core::landing::resolve_target(
            host_store,
            &main.mount,
            LANDING_TARGET_BRANCH,
        )
        .await
        {
            Ok(target) => cowshed_core::landing::abandoned_refs(host_store, &target).await,
            Err(reason) => Err(reason),
        };
        match measured {
            Ok((abandonments, unreadable)) => {
                for kept in abandonments.iter().filter(|refs| !refs.landed()) {
                    output
                        .guidance(&format!(
                            "{}: {kept}; {}",
                            project.repo_id,
                            kept.supersede_hint()
                        ))
                        .map_err(output_error)?;
                }
                for reference in unreadable {
                    output
                        .warning(&format!(
                            "{}: {reference} is under an abandoned/ namespace no abandonment writes",
                            project.repo_id
                        ))
                        .map_err(output_error)?;
                }
            }
            Err(reason) => output
                .warning(&format!(
                    "{}: the abandoned refs its main keeps cannot be measured: {reason}",
                    project.repo_id
                ))
                .map_err(output_error)?,
        }
    }
    Ok(())
}
/// What a gc did with the abandonments main's repository keeps: each it retired (or, in a dry
/// run, would) and why, and each it keeps with what main lacks of it and how to let it go.
fn emit_abandoned_refs<W: Write, E: Write>(
    output: &mut Output<W, E>,
    report: &GcReport,
) -> Result<()> {
    let sweep = &report.abandoned_refs;
    let verb = if report.dry_run {
        "would retire"
    } else {
        "retired"
    };
    for retired in &sweep.retired {
        output
            .guidance(&format!(
                "{verb} refs/cowshed/{}/abandoned/{}/: {}",
                retired.workspace, retired.head, retired.by
            ))
            .map_err(output_error)?;
    }
    for kept in &sweep.kept {
        output
            .guidance(&format!("kept {kept}; {}", kept.supersede_hint()))
            .map_err(output_error)?;
    }
    for unreadable in &sweep.unreadable {
        output
            .warning(&format!(
                "{unreadable} is under an abandoned/ namespace no abandonment writes; gc leaves it"
            ))
            .map_err(output_error)?;
    }
    if let Some(reason) = &sweep.unmeasured {
        output
            .warning(&format!(
                "gc kept every abandoned ref: main's repository cannot be measured: {reason}"
            ))
            .map_err(output_error)?;
    }
    Ok(())
}

/// Name what each project's trash keeps of `rm --abandon`: the recovery bundles of commits that
/// removal gave up, in a dot-directory no listing shows and no plain gc deletes.
fn emit_kept_abandon_bundles<W: Write, E: Write>(
    output: &mut Output<W, E>,
    kept: &[(RepoId, KeptAbandonBundles)],
) -> Result<()> {
    for (repo_id, bundles) in kept {
        output
            .guidance(&format!(
                "{repo_id}: {bundles}; {}",
                KeptAbandonBundles::HINT
            ))
            .map_err(output_error)?;
    }
    Ok(())
}

/// One project's listing, already measured by the controller when `listing_landing` asked.
fn emit_workspace_listing<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    args: crate::args::ListArgs,
    repo_id: Option<&RepoId>,
    mut workspaces: Vec<WorkspaceInfo>,
) -> Result<()> {
    if args.landed {
        let withheld = cowshed_core::landing::retain_fully_landed(&mut workspaces);
        emit_withheld(output, repo_id, &withheld)?;
    }
    if json {
        return output.success(workspaces).map_err(output_error);
    }
    if args.landing {
        let rows = std::iter::once(landing_header(repo_id.is_some()))
            .chain(
                workspaces
                    .iter()
                    .map(|workspace| landing_row(repo_id, workspace)),
            )
            .collect::<Vec<_>>();
        return emit_aligned_rows(output, &rows);
    }
    if args.landed {
        return emit_bare_names(output, workspaces.iter());
    }
    emit_workspace_table(output, &workspaces)
}

/// Name each workspace the project listing has no row for because its mount is unproven: every
/// other row is listed, and `doctor` says what is mounted there (`mount-identity`). `ls --all`
/// needs none of this: the host inventory that reads each project says it (07_api.md, "Frozen
/// wire projections").
fn emit_unproven<W: Write, E: Write>(
    output: &mut Output<W, E>,
    repo_id: &RepoId,
    unproven: &[UnprovenWorkspace],
) -> Result<()> {
    for held in unproven {
        output
            .warning(&format!(
                "{repo_id}/{} is not listed: it is neither attached nor detached: {}",
                held.workspace, held.reason
            ))
            .map_err(output_error)?;
    }
    if !unproven.is_empty() {
        output.hint("cowshed doctor").map_err(output_error)?;
    }
    Ok(())
}

fn emit_withheld<W: Write, E: Write>(
    output: &mut Output<W, E>,
    repo_id: Option<&RepoId>,
    withheld: &[cowshed_core::landing::Withheld],
) -> Result<()> {
    for cowshed_core::landing::Withheld { workspace, reason } in withheld {
        let scope = match repo_id {
            Some(repo_id) => format!("{}/{workspace}", repo_id.as_str()),
            None => workspace.to_string(),
        };
        output
            .guidance(&format!(
                "{scope} withheld: cannot determine whether its work is landed — {reason}"
            ))
            .map_err(output_error)?;
    }
    Ok(())
}

fn emit_bare_names<'a, W: Write, E: Write>(
    output: &mut Output<W, E>,
    workspaces: impl Iterator<Item = &'a WorkspaceInfo>,
) -> Result<()> {
    for workspace in workspaces {
        output
            .bare_line(workspace.workspace.as_str().as_bytes())
            .map_err(output_error)?;
    }
    Ok(())
}

/// The one place `--landing` prints a header, because four bare integers per row are unreadable
/// without one. `--landed`, whose output is consumed by `rm`, prints names and never a header.
fn landing_header(with_repo: bool) -> Vec<String> {
    let mut row = Vec::with_capacity(9);
    if with_repo {
        row.push(String::from("REPOSITORY"));
    }
    for column in [
        "WORKSPACE",
        "STATE",
        "UNLANDED",
        "LANDED",
        "BEHIND",
        "DIRTY",
        "UNLANDED-BRANCHES",
        "BRANCH",
        "MOUNT",
    ] {
        row.push(String::from(column));
    }
    row
}

fn landing_row(repo_id: Option<&RepoId>, workspace: &WorkspaceInfo) -> Vec<String> {
    let mut row = workspace_row(repo_id, workspace);
    // `workspace_row` ends with branch then mount; the counts belong before them, where a reader
    // finds them without crossing a mountpoint.
    let tail = row.split_off(row.len() - 2);
    row.extend(landing_cells(workspace));
    row.extend(tail);
    row
}
/// What a reported abandonment does not record, as one warning: a journal record an earlier build
/// wrote, before abandonments recorded what keeps their tips or measurements named files, is
/// history read back, not a clean result (02_workspaces.md, `rm` step 1). `None` when the record
/// is complete.
fn predating_warning(workspace: &str, abandoned: &AbandonedWork) -> Option<String> {
    let mut gaps = Vec::new();
    if matches!(
        abandoned.preserved,
        AbandonedPreservation::PredatesPreservation
    ) {
        gaps.push(format!(
            "no record of what keeps head {}: the record predates kept refs, and {} is the copy it \
             names",
            abandoned.head,
            abandoned.bundle.display()
        ));
    }
    let unnamed: Vec<&str> = abandoned
        .branches
        .iter()
        .filter(|branch| {
            matches!(
                &branch.commits,
                LandingCommits::Measured {
                    unlanded_paths: UnlandedPaths::PredatesPaths,
                    ..
                }
            )
        })
        .map(|branch| branch.branch.as_str())
        .collect();
    if !unnamed.is_empty() {
        gaps.push(format!(
            "branch measurements predate named files and count commits by patch identity: {}",
            unnamed.join(", ")
        ));
    }
    (!gaps.is_empty()).then(|| {
        format!(
            "{workspace}'s abandonment was recorded by an earlier cowshed: {}",
            gaps.join("; ")
        )
    })
}

/// `-` where nothing was measured by design, `?` where the answer could not be had. Keeping those
/// two apart is the whole point: one is main, the other is a workspace nobody can vouch for.
fn landing_cells(workspace: &WorkspaceInfo) -> [String; 5] {
    let unknown = || String::from("?");
    let Some(landing) = workspace.landing.as_ref() else {
        return std::array::from_fn(|_| String::from("-"));
    };
    let dirty = landing
        .dirty_files
        .map_or_else(unknown, |count| count.to_string());
    let branches = match &landing.branches {
        BranchLandings::Measured { unlanded } => unlanded.len().to_string(),
        BranchLandings::Indeterminate { .. } => unknown(),
    };
    match &landing.commits {
        LandingCommits::Measured {
            unlanded,
            landed,
            behind,
            ..
        } => [
            unlanded.to_string(),
            landed.to_string(),
            behind.to_string(),
            dirty,
            branches,
        ],
        LandingCommits::Indeterminate { .. } => [unknown(), unknown(), unknown(), dirty, branches],
    }
}

fn emit_workspace_table<W: Write, E: Write>(
    output: &mut Output<W, E>,
    workspaces: &[WorkspaceInfo],
) -> Result<()> {
    let rows: Vec<Vec<String>> = workspaces
        .iter()
        .map(|workspace| workspace_row(None, workspace))
        .collect();
    emit_aligned_rows(output, &rows)
}

fn emit_project_workspaces_table<W: Write, E: Write>(
    output: &mut Output<W, E>,
    projects: &[ProjectWorkspaces],
) -> Result<()> {
    let rows: Vec<Vec<String>> = projects
        .iter()
        .flat_map(|project| {
            project
                .workspaces
                .iter()
                .map(|workspace| workspace_row(Some(&project.repo_id), workspace))
        })
        .collect();
    emit_aligned_rows(output, &rows)
}

fn workspace_row(repo_id: Option<&RepoId>, workspace: &WorkspaceInfo) -> Vec<String> {
    let mut row = Vec::with_capacity(5);
    if let Some(repo_id) = repo_id {
        row.push(repo_id.as_str().to_owned());
    }
    row.push(workspace.workspace.as_str().to_owned());
    row.push(
        match workspace.state {
            WorkspaceState::Attached => "mounted",
            WorkspaceState::Detached => "detached",
        }
        .to_owned(),
    );
    row.push(workspace.branch.clone().unwrap_or_default());
    row.push(if workspace.state == WorkspaceState::Attached {
        workspace.mount.display().to_string()
    } else {
        String::new()
    });
    row
}

/// Human list output: columns padded with spaces to equal width (two-space
/// gutter), so `ls` reads as a table. The final column — and any trailing
/// empty cell — is never padded, keeping lines free of trailing whitespace.
fn emit_aligned_rows<W: Write, E: Write>(
    output: &mut Output<W, E>,
    rows: &[Vec<String>],
) -> Result<()> {
    let columns = rows.first().map(Vec::len).unwrap_or(0);
    let mut widths = vec![0usize; columns];
    for row in rows {
        for (index, cell) in row.iter().enumerate() {
            widths[index] = widths[index].max(cell.chars().count());
        }
    }
    for row in rows {
        let mut line = String::new();
        let last_filled = row.iter().rposition(|cell| !cell.is_empty()).unwrap_or(0);
        for (index, cell) in row.iter().enumerate().take(last_filled + 1) {
            if index > 0 {
                line.push_str("  ");
            }
            line.push_str(cell);
            if index < last_filled {
                for _ in cell.chars().count()..widths[index] {
                    line.push(' ');
                }
            }
        }
        line.push('\n');
        output.bare(line.as_bytes()).map_err(output_error)?;
    }
    Ok(())
}

fn emit_doctor<W: Write, E: Write>(output: &mut Output<W, E>, report: &DoctorReport) -> Result<()> {
    output
        .bare_line(if report.healthy {
            b"healthy"
        } else {
            b"unhealthy"
        })
        .map_err(output_error)?;
    let mut hints = Vec::new();
    for finding in &report.findings {
        let severity = match finding.severity {
            FindingSeverity::Info => "info",
            FindingSeverity::Warning => "warning",
            FindingSeverity::Error => "error",
        };
        output
            .guidance(&format!(
                "[{severity} {}] {}",
                finding.code, finding.message
            ))
            .map_err(output_error)?;
        if !finding.hint.is_empty() && !hints.contains(&finding.hint.as_str()) {
            hints.push(finding.hint.as_str());
        }
    }
    for hint in hints {
        output.hint(hint).map_err(output_error)?;
    }
    Ok(())
}

fn child_exit_code(info: &JobInfo) -> Result<i32> {
    match &info.exit {
        Some(exit) => Ok(exit.shell_code()),
        None => Err(CowshedError::internal(format!(
            "terminal job {} has no child exit status",
            info.job_id.get()
        ))),
    }
}

fn usage(message: impl Into<String>, hint: impl Into<String>) -> CowshedError {
    CowshedError::usage(message, hint)
}

#[async_trait]
trait AdoptHostSetup: Send {
    async fn plan(&mut self) -> Result<HostSetupPlan>;
    async fn execute(&mut self) -> Result<HostSetupReport>;
}

struct NativeAdoptHostSetup {
    home: PathBuf,
}

impl NativeAdoptHostSetup {
    fn for_canonical_home() -> Result<Self> {
        Ok(Self {
            home: gateway_service::canonical_home()?,
        })
    }
}

#[async_trait]
impl AdoptHostSetup for NativeAdoptHostSetup {
    async fn plan(&mut self) -> Result<HostSetupPlan> {
        plan_host_setup(&self.home, CachesVolume::Keep).await
    }

    async fn execute(&mut self) -> Result<HostSetupReport> {
        execute_host_setup(&self.home, CachesVolume::Keep).await
    }
}

async fn prepare_adopt_host_storage<S, W, E>(
    setup: &mut S,
    output: &mut Output<W, E>,
) -> Result<HostSetupReport>
where
    S: AdoptHostSetup,
    W: Write,
    E: Write,
{
    let plan = setup.plan().await?;
    if plan.requires_authorization {
        output
            .announce(
                "adopt will request administrator authorization once to set up cowshed host storage",
            )
            .map_err(output_error)?;
        for action in &plan.actions {
            output
                .announce(&host_action_evidence(action))
                .map_err(output_error)?;
        }
    }
    let report = setup.execute().await?;
    if let Some(error) = report.failure() {
        return Err(error.clone());
    }
    Ok(report)
}

fn host_action_evidence(action: &HostAction) -> String {
    match action {
        HostAction::EnsureMountRoot { path, uid, gid } => format!(
            "provision or repair workspace mount root {} for uid {uid}, gid {gid}",
            path.display()
        ),
        HostAction::CreateVolume {
            name,
            container,
            mount_at,
        } => format!(
            "create {name} in APFS container {container} and mount it at {}",
            mount_at.display()
        ),
        HostAction::MountExisting {
            name,
            uuid,
            size_bytes,
            mount_at,
        } => format!(
            "mount existing {name} ({uuid}, {size_bytes} bytes) at {}",
            mount_at.display()
        ),
        HostAction::RepairMounted {
            name,
            uuid,
            size_bytes,
            mounted_at,
            mount_at,
        } => format!(
            "repair {name} ({uuid}, {size_bytes} bytes) mounted at {}; canonical mount is {}",
            mounted_at.display(),
            mount_at.display()
        ),
        HostAction::EncryptVolume {
            name,
            uuid,
            size_bytes,
        } => format!(
            "FileVault-encrypt existing {name} ({uuid}, {size_bytes} bytes) in place and store its passphrase in System.keychain"
        ),
        HostAction::PinFstab { uuid, mount_at } => {
            format!("pin volume {uuid} at {} in /etc/fstab", mount_at.display())
        }
        HostAction::DeleteVolume {
            name,
            uuid,
            mounted_at,
        } => format!(
            "delete the retired {name} ({uuid}) mounted at {}, which holds nothing but its marker",
            mounted_at.display()
        ),
        HostAction::InstallMountService { label } => {
            format!(
                "converge system LaunchDaemon {label} and verified encrypted-volume credentials for boot and login discovery"
            )
        }
        HostAction::ReclaimStubs { paths } => format!(
            "reclaim mountpoint stubs: {}",
            paths
                .iter()
                .map(|path| path.display().to_string())
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// Per-volume findings, reporting the mountpoint that was *observed* and carrying the one that is
/// *expected*.
///
/// The distinction is the whole point of the finding. A volume mounted somewhere other than its
/// canonical root is the case an operator has to see, and rendering the canonical root in the
/// message tells them their bytes are at a path they are not — so every message names
/// `mounted_at`, the place the volume actually is, and `path` carries the canonical root, which is
/// what a repair works towards.
///
/// Classification comes from the planner's own [`VolumeState`] on the plan, never from the action
/// list: an action-derived guess collapses states the planner distinguishes (a volume in a
/// foreign container plans the same `MountExisting` as a merely detached one, and doctor must
/// name the foreign container the way `setup` does). Actions are consulted only to enrich a
/// message with the uuid and size the state does not carry.
///
/// The expected roots come from [`STORE_ROOT`] and [`RETIRED_CACHES_MOUNTPOINT`] rather than from
/// literals here: a second copy of them in the CLI is a copy that can disagree with the volume
/// the planner actually looked at.
fn host_storage_findings(plan: &HostSetupPlan) -> Vec<Finding> {
    let mut findings = Vec::new();
    for volume in &plan.volumes {
        let name = volume.name.as_str();
        let expected = match volume.role {
            VolumeRole::Store => Path::new(STORE_ROOT),
            VolumeRole::Caches => Path::new(RETIRED_CACHES_MOUNTPOINT),
            // No canonical root to compare against on this doctor surface.
            VolumeRole::Projects => continue,
        };
        // uuid/size ride on the planned mount/repair action, not on the state.
        let identity = plan
            .actions
            .iter()
            .find_map(|action| match action {
                HostAction::MountExisting {
                    name: action_name,
                    uuid,
                    size_bytes,
                    ..
                }
                | HostAction::RepairMounted {
                    name: action_name,
                    uuid,
                    size_bytes,
                    ..
                } if action_name == name => Some(format!(" ({uuid}, {size_bytes} bytes)")),
                _ => None,
            })
            .unwrap_or_default();
        let finding = match &volume.state_before {
            VolumeState::MountedValid => Finding {
                code: "host-volume".into(),
                severity: FindingSeverity::Info,
                message: format!(
                    "{name}: present, mounted at {}, marker valid",
                    expected.display()
                ),
                hint: String::new(),
                path: Some(expected.to_owned()),
            },
            VolumeState::Absent => {
                let container = plan.actions.iter().find_map(|action| match action {
                    HostAction::CreateVolume {
                        name: action_name,
                        container,
                        ..
                    } if action_name == name => Some(container.as_str()),
                    _ => None,
                });
                Finding {
                    code: "host-volume-absent".into(),
                    severity: FindingSeverity::Error,
                    message: match container {
                        Some(container) => format!(
                            "{name}: absent from APFS container {container}; expected mount {}; marker absent",
                            expected.display()
                        ),
                        None => format!(
                            "{name}: absent; expected mount {}; marker absent",
                            expected.display()
                        ),
                    },
                    hint: "cowshed setup".into(),
                    path: Some(expected.to_owned()),
                }
            }
            VolumeState::Detached => Finding {
                code: "mount".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "{name}: present{identity}, not mounted; expected {}; marker unavailable while detached",
                    expected.display()
                ),
                hint: "cowshed setup".into(),
                path: Some(expected.to_owned()),
            },
            VolumeState::MountedIncomplete => Finding {
                code: "marker".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "{name}: present{identity}, mounted at {}; marker missing or invalid",
                    expected.display()
                ),
                hint: "cowshed setup".into(),
                path: Some(expected.to_owned()),
            },
            VolumeState::MisMounted { mounted_at } => Finding {
                code: "mount".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "{name}: present{identity}, mounted at {}; expected {}; marker will be validated after remount",
                    mounted_at.display(),
                    expected.display()
                ),
                hint: "cowshed setup".into(),
                path: Some(expected.to_owned()),
            },
            // The same vocabulary `setup` uses for this state: the container and device identify
            // where the bytes actually are, and the trailing clause is the reassurance that
            // nothing will touch them.
            VolumeState::FoundElsewhere {
                container,
                device,
                mounted_at,
            } => Finding {
                code: "mount".into(),
                severity: FindingSeverity::Error,
                message: match mounted_at {
                    Some(mounted_at) => format!(
                        "{name}: found outside this host's container (container {container}, device {device}), mounted at {}; expected {}; data is safe on {device}",
                        mounted_at.display(),
                        expected.display()
                    ),
                    None => format!(
                        "{name}: found outside this host's container (container {container}, device {device}), not mounted; expected {}; data is safe on {device}",
                        expected.display()
                    ),
                },
                hint: "cowshed setup".into(),
                path: Some(expected.to_owned()),
            },
        };
        findings.push(finding);
    }
    for action in &plan.actions {
        match action {
            HostAction::EnsureMountRoot { path, uid, gid } => findings.push(Finding {
                code: "host-mount-root".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "workspace mount root {} needs provisioning or repair (uid {uid}, gid {gid})",
                    path.display()
                ),
                hint: "cowshed setup".into(),
                path: Some(path.clone()),
            }),
            HostAction::EncryptVolume {
                name,
                uuid,
                size_bytes,
            } => findings.push(Finding {
                code: "host-filevault".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "{name}: FileVault encryption or its System.keychain passphrase is not configured ({uuid}, {size_bytes} bytes)"
                ),
                hint: "cowshed setup".into(),
                path: None,
            }),
            HostAction::PinFstab { uuid, mount_at } => findings.push(Finding {
                code: "host-fstab".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "volume {uuid} should mount at {} but /etc/fstab has no canonical pin",
                    mount_at.display()
                ),
                hint: "cowshed setup".into(),
                path: Some(mount_at.clone()),
            }),
            HostAction::ReclaimStubs { paths } => findings.push(Finding {
                code: "mount-stubs".into(),
                severity: FindingSeverity::Error,
                message: format!(
                    "reclaimable mountpoint stubs: {}",
                    paths
                        .iter()
                        .map(|path| path.display().to_string())
                        .collect::<Vec<_>>()
                        .join(", ")
                ),
                hint: "cowshed setup".into(),
                path: paths.first().cloned(),
            }),
            HostAction::InstallMountService { label } => findings.push(Finding {
                code: "host-mount-service".into(),
                severity: FindingSeverity::Error,
                message: format!("system LaunchDaemon {label} or its encrypted-volume discovery credentials need repair"),
                hint: "cowshed setup".into(),
                path: Some(PathBuf::from(MOUNT_SERVICE_PLIST)),
            }),
            HostAction::CreateVolume { .. }
            | HostAction::MountExisting { .. }
            | HostAction::RepairMounted { .. }
            | HostAction::DeleteVolume { .. } => {}
        }
    }
    findings
}

fn gateway_findings(status: &GatewayStatus) -> Vec<Finding> {
    let mut findings = vec![if status.running {
        Finding {
            code: "gateway".into(),
            severity: FindingSeverity::Info,
            message: format!(
                "gateway: launchd loaded; control socket answers at {}; cli version {}; daemon version {}",
                status.socket.display(),
                status.cli_version,
                status.daemon_version.as_deref().unwrap_or("unavailable")
            ),
            hint: String::new(),
            path: Some(status.socket.clone()),
        }
    } else {
        Finding {
            code: "gateway-down".into(),
            severity: FindingSeverity::Error,
            message: format!(
                "gateway: launchd {}; control socket does not answer at {}; cli version {}; daemon version unavailable",
                if status.installed {
                    "loaded"
                } else {
                    "not loaded"
                },
                status.socket.display(),
                status.cli_version
            ),
            hint: if status.installed {
                "cowshed gateway stop && cowshed gateway start".into()
            } else {
                "cowshed gateway start".into()
            },
            path: Some(status.socket.clone()),
        }
    }];
    if let Some(cause) = status.drain_cause.as_deref() {
        findings.push(Finding {
            code: "gateway-draining".into(),
            severity: FindingSeverity::Error,
            message: format!("gateway is draining and refuses new sessions: {cause}"),
            hint: "wait for launchd to restart the gateway; if it keeps draining: cowshed gateway stop && cowshed gateway start".into(),
            path: Some(status.socket.clone()),
        });
    }
    // It answers, but nothing that needs a workspace is served until its startup pass is over:
    // a warning, because the pass ends on its own, and the count says how far it has got.
    if let Some(heal) = status.healing {
        findings.push(Finding {
            code: "gateway-starting".into(),
            severity: FindingSeverity::Warning,
            message: format!(
                "gateway is still starting, {heal}; commands that need a workspace are refused until it has finished"
            ),
            hint: "retry shortly; cowshed gateway status".into(),
            path: Some(status.socket.clone()),
        });
    }
    // Every build reports the same package version, so only the executables' bytes tell a
    // daemon left running an older build apart from this one.
    if let Some(stale) = status.stale_daemon.as_ref() {
        findings.push(Finding {
            code: "gateway-stale-binary".into(),
            severity: FindingSeverity::Warning,
            message: format!(
                "gateway runs a different cowshed binary than this CLI: daemon sha256 {}; cli sha256 {}",
                stale.daemon_sha256.as_deref().unwrap_or("unreported"),
                stale.cli_sha256
            ),
            hint: "cowshed setup".into(),
            path: Some(status.socket.clone()),
        });
    }
    findings
}

/// An unmounted main is critical, not informational.
///
/// Mains are always-mounted (02_workspaces.md): the gateway mounts every one across every adopted
/// project before it serves, so this is the user's own checkout missing from their shell, editor,
/// and Finder — not a workspace they chose to detach. Both paths are named because they answer
/// different questions: the mountpoint is the directory that looks wrong, the image is the volume
/// that belongs there. The remedy is the gateway's startup pass, which is what mounts mains.
fn main_mount_finding(main: &UnreachableMain) -> Finding {
    Finding {
        code: "main-not-mounted".into(),
        severity: FindingSeverity::Error,
        message: format!(
            "{}: main is not mounted at {} (image {}): {}",
            main.repo_id,
            main.mountpoint.display(),
            main.image.display(),
            main.reason
        ),
        hint: "cowshed gateway start".into(),
        path: Some(main.mountpoint.clone()),
    }
}

/// A store project with no adopted checkout path, as `doctor` reports it.
///
/// Info, and hintless: the inventory skips the project, but nothing is broken that `doctor`
/// can name — the checkout evidence simply is not there — and an info finding keeps the
/// verdict green while the signal survives where it belongs.
fn no_adopted_checkout_finding(repo: &RepoId, store_root: &Path) -> Finding {
    let project_dir = StorageLayout::new(store_root, repo)
        .map(|layout| layout.project().project_root.clone())
        .ok();
    Finding {
        code: "no-adopted-checkout".into(),
        severity: FindingSeverity::Info,
        message: format!(
            "{repo}: recorded in the store but has no adopted checkout path, so the project inventory skips it"
        ),
        hint: String::new(),
        path: project_dir,
    }
}

/// One detached-metadata record naming a path under the retired `<store>/mnt` layout. An error,
/// not advice: the record has to be audited against the fixed workspace root before reattachment.
fn retired_layout_finding(record: RetiredLayoutRecord) -> Finding {
    Finding {
        code: "retired-mount-layout".into(),
        severity: FindingSeverity::Error,
        message: record.doctor_message(),
        hint: RETIRED_LAYOUT_HINT.into(),
        path: Some(record.metadata_path),
    }
}

fn retired_mount_layout_findings(store_root: &Path) -> Vec<Finding> {
    match cowshed_core::doctor::probe_sync(format_args!("retired-mount-layout"), || {
        retired_layout_paths(store_root)
    }) {
        Ok(records) => records.into_iter().map(retired_layout_finding).collect(),
        Err(error) => vec![Finding {
            code: "retired-mount-layout-scan".into(),
            severity: FindingSeverity::Error,
            message: format!(
                "could not inspect detached metadata for retired mount paths: {error}"
            ),
            hint: "inspect the store; re-running doctor does not repair unread detached metadata"
                .into(),
            path: Some(store_root.to_path_buf()),
        }],
    }
}

fn sccache_finding(status: &SccacheStatus) -> Finding {
    let (severity, hint) = if status.running {
        (FindingSeverity::Info, String::new())
    } else if status.installed {
        (
            FindingSeverity::Warning,
            "cowshed sccache stop && cowshed sccache start".into(),
        )
    } else {
        (FindingSeverity::Info, "cowshed sccache start".into())
    };
    Finding {
        code: if status.running {
            "sccache"
        } else {
            "sccache-down"
        }
        .into(),
        severity,
        message: format!(
            "sccache: launchd {}; socket {} at {}{}",
            if status.installed {
                "loaded"
            } else {
                "not loaded"
            },
            if status.running {
                "answers"
            } else {
                "does not answer"
            },
            status.socket.display(),
            status
                .stats
                .as_ref()
                .map_or_else(String::new, |stats| format!(
                    "; {} compile requests, {} executed, {} configured base directories",
                    stats.compile_requests,
                    stats.requests_executed,
                    stats.base_directories.len()
                ))
        ),
        hint,
        path: Some(status.socket.clone()),
    }
}

/// The retired caches volume, while it exists, with the command that finishes its retirement:
/// `cowshed setup` moves what it still holds, and `cowshed setup --retire-caches-volume` deletes
/// it once only its marker is left (03_caches.md).
fn caches_volume_finding() -> Option<Finding> {
    let volume = Path::new(RETIRED_CACHES_MOUNTPOINT);
    fs::symlink_metadata(volume.join(VOLUME_MARKER_FILE)).ok()?;
    let (message, hint) = match cowshed_core::caches_retirement::holds_only_marker(volume) {
        Ok(true) => (
            format!(
                "the retired caches volume at {} holds nothing but its marker",
                volume.display()
            ),
            "cowshed setup --retire-caches-volume",
        ),
        Ok(false) => (
            format!(
                "the retired caches volume at {} still holds caches that belong in the host HOME",
                volume.display()
            ),
            "cowshed setup, then cowshed setup --retire-caches-volume",
        ),
        Err(error) => (
            format!(
                "cannot inspect the retired caches volume at {}: {error}",
                volume.display()
            ),
            "cowshed setup, then cowshed setup --retire-caches-volume",
        ),
    };
    Some(Finding {
        code: "caches-volume".into(),
        severity: FindingSeverity::Warning,
        message,
        hint: hint.into(),
        path: Some(volume.to_path_buf()),
    })
}

/// A saturated kernel vnode table, which stalls every mount, attach and workspace command on
/// the host until a vnode can be recycled — the cause behind a disk child that hangs past its
/// deadline on an image that is fine.
fn vnode_table_finding(
    table: std::io::Result<cowshed_core::vnodes::VnodeTable>,
) -> Option<Finding> {
    match table {
        Ok(table) if table.saturated() => Some(Finding {
            code: "vnode-table-saturated".into(),
            severity: FindingSeverity::Warning,
            message: format!(
                "{table}; mounts, attaches and workspace commands stall behind vnode recycling"
            ),
            hint: table.remedy(),
            path: None,
        }),
        Ok(_) => None,
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => None,
        Err(error) => Some(Finding {
            code: "vnode-table".into(),
            severity: FindingSeverity::Warning,
            message: format!(
                "could not read kern.num_vnodes, kern.free_vnodes and kern.maxvnodes: {error}"
            ),
            hint: "sysctl kern.num_vnodes kern.free_vnodes kern.maxvnodes".into(),
            path: None,
        }),
    }
}

/// `diskimagesiod` helpers that outlived their disk image, each still holding the 72 MiB of
/// kernel mapping whose exhaustion fails every attach with error code 150. Only root can end
/// them, so the finding names the pids and the command, and cowshed signals nothing itself. A
/// pid is named only when two reads agree with no disk-image tool running; while one runs, a
/// helper may be attaching, and the finding says so without naming any.
fn orphaned_disk_image_helpers_finding(
    reading: std::io::Result<cowshed_apfs::disk_image_helpers::HelperReading>,
) -> Option<Finding> {
    use cowshed_apfs::disk_image_helpers::HelperReading;
    match reading {
        Ok(HelperReading::Settled(orphans)) if orphans.is_empty() => None,
        Ok(HelperReading::Settled(orphans)) => Some(Finding {
            code: "orphaned-disk-image-helpers".into(),
            severity: FindingSeverity::Warning,
            message: orphans.to_string(),
            hint: orphans.remedy(),
            path: None,
        }),
        Ok(HelperReading::Busy { suspected: 0 }) => None,
        Ok(HelperReading::Busy { suspected }) => Some(Finding {
            code: "orphaned-disk-image-helpers".into(),
            severity: FindingSeverity::Info,
            message: format!(
                "{suspected} diskimagesiod helper(s) serve no attached disk image, but a diskutil \
                 or hdiutil was running, so they may still be attaching: no pid is named, \
                 because killing a helper mid-attach leaks its device until reboot"
            ),
            hint: "cowshed doctor once no diskutil or hdiutil is running".into(),
            path: None,
        }),
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => None,
        Err(error) => Some(Finding {
            code: "disk-image-helpers".into(),
            severity: FindingSeverity::Warning,
            message: format!(
                "could not tell orphaned diskimagesiod helpers from the ones serving attached \
                 disk images: {error}"
            ),
            hint: "ioreg -r -c DIDeviceIOUserClient -l | grep IOUserClientCreator".into(),
            path: None,
        }),
    }
}

/// The host's disk-image device classes (attach_budget.rs): leaked image devices, user clients,
/// volumes and media. Cowshed runs no DiskImages2 attach, so nothing is gated on them: a leaked
/// image device warns and names what frees it. Departed volumes and media take no attach-pool
/// slot, so [`departed_volume_references_finding`] reports them, never this finding.
fn disk_image_attach_budget_finding(
    reading: std::io::Result<cowshed_apfs::attach_budget::KernelReading>,
) -> Option<Finding> {
    match reading {
        Ok(reading) => Some(Finding {
            code: "disk-image-attach-budget".into(),
            severity: if reading.is_leaking() {
                FindingSeverity::Warning
            } else {
                FindingSeverity::Info
            },
            message: reading.to_string(),
            hint: reading.remedy().into(),
            path: None,
        }),
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => None,
        Err(error) => Some(Finding {
            code: "disk-image-attach-budget".into(),
            severity: FindingSeverity::Warning,
            message: format!("could not read the disk-image device classes: {error}"),
            hint: "ioclasscount AppleDiskImageDevice DIDeviceIOUserClient AppleAPFSVolume IOMedia"
                .into(),
            path: None,
        }),
    }
}

/// Fewer images than this left under the media cap warn: about ten workspaces with build volumes.
const MEDIA_LOW_IMAGES: u64 = 20;

/// The registered IOMedia against IOKit's match-notification ceiling (media_cap.rs), a soft one:
/// cowshed's in-kernel attaches wait on no IOMedia notification, so verbs make room best effort
/// and attach past it. A warning while fewer than [`MEDIA_LOW_IMAGES`] images fit, none past it.
fn media_notification_cap_finding(
    reading: std::io::Result<cowshed_apfs::media_cap::MediaReading>,
) -> Option<Finding> {
    use cowshed_apfs::media_cap::{MEDIA_CAP_HINT, NOTIFICATION_CAP};
    match reading {
        Ok(reading) => {
            let left = reading.images_left(NOTIFICATION_CAP);
            Some(Finding {
                code: "media-notification-cap".into(),
                severity: if left < MEDIA_LOW_IMAGES {
                    FindingSeverity::Warning
                } else {
                    FindingSeverity::Info
                },
                message: format!(
                    "{reading}; with the host's reserve that commits {} of IOKit's \
                     {NOTIFICATION_CAP}-entry IOMedia match-notification queue (XNU \
                     IOServiceUserNotification::kMaxOutstanding), room for {left} more image(s), \
                     {} workspace(s) with a build volume. Past it an unfiltered IOMedia \
                     notification misses matches; cowshed's in-kernel attaches wait on none, so a \
                     verb makes room where it can and attaches anyway",
                    reading.committed(),
                    left / 2
                ),
                hint: if left < MEDIA_LOW_IMAGES {
                    MEDIA_CAP_HINT.into()
                } else {
                    "nothing to free".into()
                },
                path: None,
            })
        }
        Err(error) if error.kind() == std::io::ErrorKind::Unsupported => None,
        Err(error) => Some(Finding {
            code: "media-notification-cap".into(),
            severity: FindingSeverity::Warning,
            message: format!(
                "could not read the registered IOMedia and the running image attaches that \
                 IOKit's match-notification cap is counted from: {error}"
            ),
            hint: "ioreg -r -c IOMedia -d 1 -w0 | grep -c '^+-o' counts the registered media"
                .into(),
            path: None,
        }),
    }
}

/// Departed APFS volumes and media the kernel has not freed (attach_budget.rs, `Departed`),
/// read from the same snapshot as the attach pool. A warning from the first one, never an error:
/// they take no attach-pool slot, so they stay out of `disk-image-attach-budget` and its remedy.
/// What holds them is not assumed: the hint attributes them by registry ID and changes nothing
/// (01a_apfs.md, "Spotlight media references").
fn departed_volume_references_finding(
    reading: &cowshed_apfs::attach_budget::KernelReading,
) -> Option<Finding> {
    reading.departed().map(|departed| Finding {
        code: "departed-volume-references".into(),
        severity: FindingSeverity::Warning,
        message: departed.to_string(),
        hint: cowshed_apfs::attach_budget::DEPARTED_ATTRIBUTION_HINT.into(),
        path: None,
    })
}

/// The System Settings pane a Developer Tools entry is added in.
const DEVELOPER_TOOLS_PANE: &str =
    "open 'x-apple.systempreferences:com.apple.preference.security?Privacy_DevTools'";

/// Whether workspace jobs pay macOS's first-launch scan, judged from doctor's probe
/// (05_gateway.md "The gateway host"): `first-launch-exempt` (info) when the gateway's fresh
/// executable starts at host-shell speed, `first-launch-scan` (warning) naming the one entry to
/// add when it waits, `first-launch-unmeasured` (warning) when either side measured nothing.
fn first_launch_finding(reading: Result<gateway_service::FirstLaunchReading>) -> Finding {
    let unmeasured = |message: String, hint: String| Finding {
        code: "first-launch-unmeasured".into(),
        severity: FindingSeverity::Warning,
        message,
        hint,
        path: None,
    };
    let reading = match reading {
        Ok(reading) => reading,
        Err(error) => return unmeasured(error.message, error.hint),
    };
    let gateway = match reading.gateway {
        Ok(gateway) => gateway,
        Err(error) => {
            return unmeasured(
                format!("the gateway measured no first launch: {error}"),
                "cowshed setup".into(),
            );
        }
    };
    let host = reading.host;
    let ms = |micros: u64| format!("{}.{} ms", micros / 1000, micros % 1000 / 100);
    let measured = format!(
        "a fresh executable's first launch took {} under the gateway (then {}) and {} in this shell (then {})",
        ms(gateway.first_micros),
        ms(gateway.second_micros),
        ms(host.first_micros),
        ms(host.second_micros)
    );
    let path = reading.gateway_host.display();
    match crate::first_launch::judge(host, gateway) {
        crate::first_launch::Exemption::Exempt => Finding {
            code: "first-launch-exempt".into(),
            severity: FindingSeverity::Info,
            message: format!(
                "{measured}: workspace jobs launch new executables at host-shell speed"
            ),
            hint: String::new(),
            path: Some(reading.gateway_host.clone()),
        },
        crate::first_launch::Exemption::Charged => Finding {
            code: "first-launch-scan".into(),
            severity: FindingSeverity::Warning,
            message: format!(
                "{measured}: macOS scans every new executable a workspace job launches, because the gateway host {path} runs without a Developer Tools entry; add it once in System Settings → Privacy & Security → Developer Tools → + (⇧⌘G takes the path {path}), then restart the gateway: an entry applies only to a gateway host started after it was added ({restart}, or cowshed gateway start, which refuses while a land or gate runs)",
                restart = reading.restart
            ),
            hint: format!("{DEVELOPER_TOOLS_PANE}; then {}", reading.restart),
            path: Some(reading.gateway_host.clone()),
        },
    }
}

async fn diagnose_host() -> Result<Vec<Finding>> {
    use cowshed_core::doctor::{probe, probe_sync, probe_value, waits_on};
    let home = gateway_service::canonical_home()?;
    // Doctor reads the host it runs on: production's roots, and so production's names and sockets.
    let roots = CanonicalRoots::global();
    let plan = probe("host-storage", async {
        waits_on(
            format_args!("host-storage"),
            "securityd (security find-generic-password, System.keychain) and launchd (system/dev.cowshed.storage)",
        );
        plan_host_setup(&home, CachesVolume::Keep).await
    })
    .await;
    let mut findings = match plan {
        Ok(plan) => host_storage_findings(&plan),
        Err(error) => vec![Finding {
            code: "host-storage".into(),
            severity: FindingSeverity::Error,
            message: error.message,
            hint: "cowshed setup".into(),
            path: None,
        }],
    };
    findings.extend(probe_value("caches-volume", async { caches_volume_finding() }).await);
    let gateway = probe("gateway-status", async {
        waits_on(
            format_args!("gateway-status"),
            format_args!(
                "launchd ({}), then the gateway daemon at {}",
                roots.namespace().launchd_label("gateway"),
                roots.control_socket().display()
            ),
        );
        gateway_service::doctor_service_status(&roots).await
    })
    .await;
    let serves = matches!(&gateway, Ok(Ok(status)) if status.running);
    match gateway {
        Ok(Ok(status)) => findings.extend(gateway_findings(&status)),
        Ok(Err(unanswered)) => findings.push(unanswered_finding(
            "gateway-unanswered",
            FindingSeverity::Error,
            &unanswered,
            GATEWAY_UNANSWERED_HINT,
        )),
        Err(error) => findings.push(Finding {
            code: "gateway-status".into(),
            severity: FindingSeverity::Error,
            message: error.message,
            hint: "cowshed gateway start".into(),
            path: None,
        }),
    }
    // Only a gateway that serves can measure: one that is down or silent is its own finding.
    if serves {
        findings.push(first_launch_finding(
            probe("first-launch", async {
                waits_on(
                    format_args!("first-launch"),
                    format_args!("the gateway daemon at {}", roots.control_socket().display()),
                );
                gateway_service::first_launch(&roots).await
            })
            .await,
        ));
    }
    findings.extend(vnode_table_finding(probe_sync(
        format_args!("vnode-table"),
        cowshed_core::vnodes::VnodeTable::read,
    )));
    findings.extend(orphaned_disk_image_helpers_finding(probe_sync(
        format_args!("disk-image-helpers"),
        cowshed_apfs::disk_image_helpers::OrphanedHelpers::read,
    )));
    // One snapshot answers both findings: departed volumes need no census of their own.
    let classes = probe_sync(
        format_args!("attach-budget"),
        cowshed_apfs::attach_budget::read_kernel,
    );
    let departed = classes
        .as_ref()
        .ok()
        .and_then(departed_volume_references_finding);
    findings.extend(disk_image_attach_budget_finding(classes));
    findings.extend(media_notification_cap_finding(probe_sync(
        format_args!("media-notification-cap"),
        cowshed_apfs::media_cap::read,
    )));
    findings.extend(departed);
    let sccache = probe("sccache-status", async {
        waits_on(
            format_args!("sccache-status"),
            format_args!(
                "launchd ({}), then the sccache server at {}",
                roots.namespace().launchd_label("sccache"),
                cowshed_core::capabilities::sccache::server_socket().display()
            ),
        );
        crate::capabilities::sccache::service::service_status_within(
            roots.namespace(),
            DOCTOR_PEER_ANSWER,
        )
        .await
    })
    .await;
    match sccache {
        Ok((status, unanswered)) => {
            findings.push(sccache_finding(&status));
            findings.extend(unanswered.map(|unanswered| {
                unanswered_finding(
                    "sccache-unanswered",
                    FindingSeverity::Warning,
                    &unanswered,
                    "cowshed sccache stop && cowshed sccache start",
                )
            }));
        }
        Err(error) => findings.push(Finding {
            code: "sccache-status".into(),
            severity: FindingSeverity::Warning,
            message: error.message,
            hint: "inspect sccache; the status query failed".into(),
            path: None,
        }),
    }
    // Whether the agent still has a program to run, which the status above cannot answer: a loaded
    // agent whose store path was collected looks identical to a healthy one until launchd next
    // tries to exec it, which may be a reboot away.
    let pinning = probe_sync(format_args!("sccache-agent"), || {
        crate::capabilities::sccache::service::control_target(roots.namespace(), &home)
            .map(|target| crate::capabilities::sccache::nix::pinning_findings(&home, &target))
    });
    match pinning {
        Ok(pinning) => findings.extend(pinning),
        Err(error) => findings.push(Finding {
            code: "sccache-agent".into(),
            severity: FindingSeverity::Error,
            message: error.message,
            hint: error.hint,
            path: None,
        }),
    }
    // Inventory validates the existing store itself. A pending host setup action is a finding,
    // not permission to hide project state. One registry traversal supplies every view below.
    match probe("inventory", doctor_projects()).await {
        Ok((projects, mains, checkoutless, unreadable)) => {
            let store = roots.store();
            findings.extend(retired_mount_layout_findings(store));
            findings.push(Finding {
                code: "workspace-inventory".into(),
                severity: FindingSeverity::Info,
                message: format!(
                    "{} readable adopted project(s) recorded; {} unreadable project(s)",
                    projects.len(),
                    unreadable.len()
                ),
                hint: if projects.is_empty() && unreadable.is_empty() {
                    "cowshed adopt <git-root>".into()
                } else {
                    String::new()
                },
                path: None,
            });
            findings.extend(mains.iter().map(main_mount_finding));
            findings.extend(
                checkoutless
                    .iter()
                    .map(|repo| no_adopted_checkout_finding(repo, store)),
            );
            findings.extend(unreadable.into_iter().map(|(repo, error)| Finding {
                code: "workspace-inventory".into(),
                severity: FindingSeverity::Error,
                message: format!("project {repo}: {error}"),
                hint: "repair this project's store records, then retry cowshed doctor".into(),
                path: Some(store.join(repo.as_str())),
            }));
        }
        Err(error) => findings.push(Finding {
            code: "workspace-inventory".into(),
            severity: FindingSeverity::Error,
            message: error.message,
            hint: error.hint,
            path: None,
        }),
    }
    Ok(findings)
}

/// How long doctor waits for the sccache server, which took its request, to answer before it
/// reports that server as unanswered (specs/cowshed/06_cli.md, "doctor"). The gateway heartbeats
/// its answers instead, and is unanswered at its silence bound (05_gateway.md, "Deadlines").
/// Long rather than absent: a socket that takes a request and answers late may be a live,
/// saturated server; a minute is still inside the 180 s a caller once gave a silent doctor before
/// killing it.
const DOCTOR_PEER_ANSWER: Duration = Duration::from_secs(60);

/// What to do about a gateway that took doctor's request and did not answer: it may be saturated
/// and answer later, so look before restarting it, which would cut off every gate it serves.
const GATEWAY_UNANSWERED_HINT: &str = "cowshed gateway status shows whether it answers late; if it never answers: cowshed gateway stop && cowshed gateway start";

/// A daemon that took doctor's request and gave no answer within [`DOCTOR_PEER_ANSWER`].
fn unanswered_finding(
    code: &'static str,
    severity: FindingSeverity,
    unanswered: &gateway_service::Unanswered,
    hint: &str,
) -> Finding {
    Finding {
        code: code.into(),
        severity,
        message: format!(
            "{} accepted doctor's request at {} and gave no answer in {:?}",
            unanswered.peer,
            unanswered.socket.display(),
            unanswered.within
        ),
        hint: hint.into(),
        path: Some(unanswered.socket.clone()),
    }
}

fn doctor_report(findings: Vec<Finding>) -> DoctorReport {
    DoctorReport::from_findings(findings)
}

fn record_project_checks_skipped<W: Write, E: Write>(
    output: &mut Output<W, E>,
    findings: &mut Vec<Finding>,
    error: Option<&CowshedError>,
    path: Option<PathBuf>,
) -> Result<()> {
    let message = error.map_or_else(
        || "project checks skipped: no adopted checkout at cwd".to_owned(),
        |error| format!("project checks skipped: {}", error.message),
    );
    findings.push(Finding {
        code: "project-checks-skipped".into(),
        severity: FindingSeverity::Error,
        message: message.clone(),
        hint: error.map_or_else(
            || "run from an adopted checkout or pass --project <git-root>".to_owned(),
            |error| error.hint.clone(),
        ),
        path,
    });
    output.note(&message).map_err(output_error)
}

fn emit_doctor_report<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    report: DoctorReport,
) -> Result<DispatchExit> {
    let healthy = report.healthy;
    if json {
        output.success(report).map_err(output_error)?;
    } else {
        emit_doctor(output, &report)?;
    }
    Ok(DispatchExit {
        code: if healthy { 0 } else { 5 },
    })
}

async fn repair_project_artifacts(project_root: &Path) -> Result<Vec<Finding>> {
    let canonical_root = fs::canonicalize(project_root).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "cannot resolve project root {} for artifact repair: {error}",
                project_root.display()
            ),
            "cowshed --project <git-root> doctor --repair",
        )
    })?;
    let projects = list_all_adopted_projects().await?;
    let project = projects
        .into_iter()
        .find(|project| {
            project.workspaces.iter().any(|workspace| {
                workspace.role == WorkspaceRole::Main
                    && fs::canonicalize(&workspace.mount).is_ok_and(|mount| mount == canonical_root)
            })
        })
        .ok_or_else(|| {
            CowshedError::not_found(
                format!(
                    "{} is not the main mount of an adopted project",
                    project_root.display()
                ),
                "cowshed ls --all --json",
            )
        })?;
    let retained_budget = ArtifactConfig::default().retained_recovery_budget_bytes;
    for workspace in project
        .workspaces
        .into_iter()
        .filter(|workspace| workspace.state == WorkspaceState::Attached)
    {
        let mount = workspace.mount.clone();
        tokio::task::spawn_blocking(move || {
            repair_workspace_record_sequences(&mount, retained_budget)
        })
        .await
        .map_err(|error| CowshedError::internal(format!("artifact repair task failed: {error}")))?
        .map_err(|error| {
            CowshedError::integrity(
                error.to_string(),
                "the log was not replaced; create a fresh store if record ordering failed",
            )
        })?;
    }
    Ok(Vec::new())
}

async fn run_doctor_command<W, E>(cli: Cli, output: &mut Output<W, E>) -> Result<DispatchExit>
where
    W: Write + Send,
    E: Write + Send,
{
    use cowshed_core::doctor::{probe, probe_sync};
    let repair = matches!(&cli.command, Command::Doctor(args) if args.repair);
    let project_root = probe("project-root", resolve_project_root(&cli)).await;
    let mut findings = diagnose_host().await?;
    if repair && let Ok(root) = project_root.as_ref() {
        match probe("artifact-repair", repair_project_artifacts(root)).await {
            Ok(repaired) => findings.extend(repaired),
            Err(error) => findings.push(Finding {
                code: "artifact-repair".into(),
                severity: FindingSeverity::Error,
                message: error.message,
                hint: error.hint,
                path: Some(root.clone()),
            }),
        }
    }
    // Project opening validates storage independently of setup's repair plan. An unavailable
    // project is reported explicitly rather than silently skipped because a host needs repair.
    match project_root {
        Ok(root) => {
            let scope = runtime_recovery_scope(&cli.command)?;
            match probe("project-open", ActorBridge::open_existing(&root, scope)).await {
                Ok(mut bridge) => {
                    let identity = probe_sync(format_args!("git-identity"), || {
                        git_identity_findings(&bridge)
                    });
                    let project = probe("project", bridge.doctor()).await;
                    let teardown = probe("project-shutdown", bridge.shutdown()).await.err();
                    match identity {
                        Ok(identity) => findings.extend(identity),
                        Err(error) => findings.push(Finding {
                            code: "git-identity".into(),
                            severity: FindingSeverity::Warning,
                            message: error.message,
                            hint: error.hint,
                            path: Some(root.clone()),
                        }),
                    }
                    match project {
                        Ok(report) => findings.extend(report.findings),
                        Err(error) => findings.push(Finding {
                            code: "project-doctor".into(),
                            severity: FindingSeverity::Error,
                            message: error.message,
                            hint: error.hint,
                            path: Some(root),
                        }),
                    }
                    if let Some(error) = teardown {
                        findings.push(Finding {
                            code: "project-shutdown".into(),
                            severity: FindingSeverity::Error,
                            message: error.message,
                            hint: error.hint,
                            path: None,
                        });
                    }
                }
                // A project-open failure means none of its invariants ran. Host findings remain
                // useful, but an unobserved project is never a healthy project.
                Err(error) => {
                    record_project_checks_skipped(output, &mut findings, Some(&error), Some(root))?
                }
            }
        }
        Err(error) => record_project_checks_skipped(
            output,
            &mut findings,
            Some(&error),
            cli.global.project.clone(),
        )?,
    }
    emit_doctor_report(output, cli.global.json, doctor_report(findings))
}

pub async fn dispatch_and_shutdown<S, R, W, E>(
    mut service: S,
    cli: Cli,
    stdin: R,
    output: &mut Output<W, E>,
) -> Result<DispatchExit>
where
    S: CliService,
    R: AsyncRead + Send + 'static,
    W: Write + Send,
    E: Write + Send,
{
    let primary = cowshed_os::timing::spanned(
        "cli",
        "dispatch",
        dispatch(&mut service, cli, stdin, output),
    )
    .await;
    finish_dispatch(service, primary).await
}

async fn finish_dispatch<S: CliService>(
    service: S,
    primary: Result<DispatchExit>,
) -> Result<DispatchExit> {
    let teardown = cowshed_os::timing::spanned("cli", "shutdown", service.shutdown())
        .await
        .err();
    match primary {
        Ok(exit) => match teardown {
            None => Ok(exit),
            Some(error) => Err(error),
        },
        Err(primary) => Err(merge_primary(primary, teardown)),
    }
}

pub async fn run_bridge_command<R, W, E>(
    cli: Cli,
    stdin: R,
    output: &mut Output<W, E>,
) -> Result<DispatchExit>
where
    R: AsyncRead + Send + 'static,
    W: Write + Send,
    E: Write + Send,
{
    if matches!(&cli.command, Command::Doctor(_)) {
        return run_doctor_command(cli, output).await;
    }
    if let Command::Detach(args) = &cli.command
        && !args.all
    {
        let root = resolve_detach_root(&cli).await?;
        let bridge =
            ActorBridge::open_existing(&root, runtime_recovery_scope(&cli.command)?).await?;
        return dispatch_and_shutdown(bridge, cli, stdin, output).await;
    }
    let stdin = match crate::resident::answer(&cli, stdin, output).await? {
        crate::resident::Answer::Answered(exit) => return Ok(exit),
        crate::resident::Answer::Declined(stdin) => stdin,
    };
    let discovery = cli.command.project_discovery();
    if discovery == ProjectDiscovery::NotUsed {
        return dispatch_host_command(cli, output, false).await;
    }
    let resolved =
        match cowshed_os::timing::spanned("cli", "resolve-root", resolve_project_context(&cli))
            .await
        {
            Ok(resolved) => resolved,
            Err(_) if discovery == ProjectDiscovery::Optional => {
                return dispatch_host_command(cli, output, true).await;
            }
            Err(error) => return Err(error),
        };
    let root = &resolved.root;
    let mode = runtime_open_mode(&cli.command);
    let requested_repo_id = runtime_open_repo_id(&cli.command)?;
    if mode == RuntimeOpenMode::Provision {
        let mut setup = NativeAdoptHostSetup::for_canonical_home()?;
        let _ = prepare_adopt_host_storage(&mut setup, output).await?;
    }
    let requested_repo_id =
        if binds_re_adoption_primary(&cli.command, resolved.resuming_re_adoption) {
            re_adoption_primary(root).await?
        } else {
            requested_repo_id
        };
    let open_span = cowshed_os::timing::span("cli", "open");
    let bridge = match mode {
        RuntimeOpenMode::Provision => ActorBridge::open_for_adopt(root, requested_repo_id).await,
        RuntimeOpenMode::ExistingOnly => {
            ActorBridge::open_existing(root, runtime_recovery_scope(&cli.command)?).await
        }
        RuntimeOpenMode::IdentityChange => {
            ActorBridge::open_existing_for_identity_change(root).await
        }
    };
    drop(open_span);
    let mut bridge = match bridge {
        Ok(bridge) => bridge,
        Err(error)
            if discovery == ProjectDiscovery::Optional && optional_project_unavailable(&error) =>
        {
            return dispatch_host_command(cli, output, true).await;
        }
        Err(error) => return Err(error),
    };
    if resolved.resuming_re_adoption {
        let primary = cowshed_os::timing::spanned(
            "cli",
            "dispatch",
            report_recovered_adoption(&mut bridge, root, cli.global.json, output),
        )
        .await;
        finish_dispatch(bridge, primary).await
    } else {
        dispatch_and_shutdown(bridge, cli, stdin, output).await
    }
}

pub async fn run_host_command<W, E>(cli: Cli, output: &mut Output<W, E>) -> Result<DispatchExit>
where
    W: Write + Send,
    E: Write + Send,
{
    dispatch_host_command(cli, output, false).await
}

async fn dispatch_host_command<W, E>(
    cli: Cli,
    output: &mut Output<W, E>,
    project_checks_skipped: bool,
) -> Result<DispatchExit>
where
    W: Write + Send,
    E: Write + Send,
{
    match cli.command {
        Command::Attach(args) if args.all && args.workspace.is_none() => {
            let mounts = attach_store_wide(args.browse).await?;
            emit_attached(output, cli.global.json, &mounts.result)?;
            report_stale_gateway_views(output, &mounts.stale)?;
            Ok(success())
        }
        Command::Detach(args) if args.all && args.workspace.is_none() => {
            let mounts = detach_store_wide().await?;
            if cli.global.json {
                output.success(EmptyResult {}).map_err(output_error)?;
            }
            report_stale_gateway_views(output, &mounts.stale)?;
            Ok(success())
        }
        Command::Mount(args) => {
            let storage = host_storage().await?;
            let backend = mount_main::NativeMainMountBackend::new(storage);
            mount_main::dispatch_mount_main(&backend, &args.repo_id, cli.global.json, output).await
        }
        Command::Forget(args) => {
            let storage = host_storage().await?;
            let backend = forget::NativeForgetBackend::new(storage);
            forget::dispatch_forget(&backend, &args.repo_id, cli.global.json, output).await
        }
        Command::List(args) => {
            let projects = list_projects(listing_landing(args)).await?;
            emit_project_listing(output, cli.global.json, args, projects).await?;
            if !cli.global.json {
                emit_kept_abandon_bundles(output, &kept_abandon_bundles().await?)?;
            }
            Ok(success())
        }
        Command::Doctor(_) => {
            let mut findings = diagnose_host().await?;
            if project_checks_skipped {
                record_project_checks_skipped(
                    output,
                    &mut findings,
                    None,
                    cli.global.project.clone(),
                )?;
            }
            emit_doctor_report(output, cli.global.json, doctor_report(findings))
        }
        _ => Err(CowshedError::internal(
            "command without project context was not dispatched by its host service",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cowshed_core::storage::bootstrap::VolumeOutcome;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    #[test]
    fn reseed_control_output_preserves_the_named_envelope_and_allocation_units() {
        for dry_run in [false, true] {
            for json in [false, true] {
                for quiet in [false, true] {
                    let result = ReseedResult {
                        workspace: WorkspaceName::new("raven").unwrap(),
                        from: WorkspaceName::main(),
                        previous_volume: "old-volume".into(),
                        seed: "main-seed".into(),
                        replacement_volume: (!dry_run).then(|| "replacement-volume".into()),
                        previous_bytes: 128,
                        replacement_bytes: 256,
                        released_bytes: if dry_run { 0 } else { 128 },
                        dry_run,
                    };
                    let expected = result.clone();
                    let mut output = Output::new(Vec::new(), Vec::new(), quiet);
                    emit_reseed_result(&mut output, json, result).expect("reseed output");
                    let (stdout, stderr) = output.into_inner();
                    if json {
                        assert_eq!(stdout.iter().filter(|byte| **byte == b'\n').count(), 1);
                        let envelope: cowshed_core::api::JsonEnvelope<ReseedResult> =
                            serde_json::from_slice(&stdout).expect("named reseed envelope");
                        assert_eq!(envelope, cowshed_core::api::JsonEnvelope::success(expected));
                        let value: serde_json::Value = serde_json::from_slice(&stdout).unwrap();
                        assert_eq!(value.as_object().unwrap().len(), 2);
                        assert_eq!(value["ok"], true);
                        assert_eq!(value["result"].get("replacementVolume").is_none(), dry_run);
                    } else {
                        assert_eq!(
                            stdout,
                            if dry_run {
                                b"0\n".to_vec()
                            } else {
                                b"128\n".to_vec()
                            }
                        );
                    }
                    let stderr = String::from_utf8(stderr).unwrap();
                    if quiet {
                        assert!(stderr.is_empty());
                    } else {
                        for fact in [
                            "source tree, HEAD, index, dirty and untracked files untouched",
                            "missing incremental work rebuilds cold",
                            "no seed refreshed",
                            "not unique physical bytes",
                        ] {
                            assert!(stderr.contains(fact), "{stderr}");
                        }
                        if dry_run {
                            assert!(stderr.contains("estimated allocation reduction: -128 bytes"));
                            assert!(stderr.contains("no changes made"));
                        } else {
                            assert!(stderr.contains("128 allocated image bytes reclaimed"));
                        }
                    }
                }
            }
        }
    }

    /// The defect: a daemon of another build refused `cowshed rm`'s supervisor ensure *after* the
    /// removal had stopped the workspace's supervisor and journaled its intent, and the path that
    /// would put that back was refused too — "state restoration also failed", a workspace half
    /// removed. The verbs that change state or run a job are asked about the daemon's build before
    /// they begin; the ones that diagnose or repair must keep working under the disagreement.
    #[test]
    fn only_verbs_that_change_state_or_run_jobs_ask_the_daemon_for_its_build() {
        let needs = |argv: &[&str]| {
            let cli = crate::args::parse_args(argv.iter().copied()).expect("a valid command line");
            needs_daemon_of_this_build(&cli.command)
        };
        for argv in [
            &["rm", "raven"][..],
            &["rm", "raven", "--force"],
            &["forget", "acme/widget", "--abandon"],
            &["exec", "raven", "--", "true"],
            &["fork", "raven"],
            &["fork", "raven", "crow"],
            &["mv", "raven", "crow"],
            &["checkpoint", "raven", "before"],
            &["restore", "raven", "before"],
            &["path", "raven"],
            &["attach", "raven"],
            &["detach", "raven"],
            &["resize", "raven", "200g"],
            &["reseed", "raven"],
            &["reseed", "raven", "--dry-run"],
            &["land", "raven", "--target", "main"],
            &["rebase", "raven"],
            &["push", "raven", "--branch", "review"],
            &["gc"],
            &["revoke", "raven", "--egress", "api.example.test:443"],
            &[
                "revoke",
                "--project-wide",
                "--egress",
                "api.example.test:443",
            ],
            &["revoke", "raven", "--all"],
            &["revoke", "--project-wide", "--all"],
        ] {
            assert!(needs(argv), "{argv:?} changes state through the daemon");
        }
        for argv in [
            &["doctor"][..],
            &["land", "raven", "--preflight"],
            &[
                "land",
                "raven",
                "--preflight",
                "--into",
                "lane",
                "--ungated",
            ],
            &[
                "--json",
                "land",
                "raven",
                "--preflight",
                "--target",
                "main",
                "--push-only",
            ],
            &["doctor", "--repair"],
            &["ls"],
            &["ls", "--all"],
            &["setup"],
            &["setup", "--downgrade"],
            &["gateway", "status"],
            &["gateway", "start"],
            &["build-state"],
            &["sccache", "status"],
            &["skill", "install"],
        ] {
            assert!(!needs(argv), "{argv:?} must work while the builds disagree");
        }
    }

    /// Only a fresh `adopt --re-adopt` that names no repository asks the store for main's recorded
    /// primary. A resumed one is named by its journal, `--repo-id` names its own, and a plain adopt
    /// or any other verb derives nothing from the marker.
    #[test]
    fn only_a_fresh_re_adoption_without_a_repo_id_binds_the_recorded_primary() {
        let binds = |argv: &[&str], resuming| {
            let cli = crate::args::parse_args(argv.iter().copied()).expect("a valid command line");
            binds_re_adoption_primary(&cli.command, resuming)
        };
        assert!(binds(&["adopt", "--re-adopt"], false));
        assert!(binds(&["adopt", "checkout", "--re-adopt"], false));
        for (argv, resuming) in [
            (&["adopt", "--re-adopt"][..], true),
            (&["adopt", "checkout", "--re-adopt"], true),
            (&["adopt", "--re-adopt", "--repo-id", "acme/widget"], false),
            (&["adopt", "--re-adopt", "--repo-id", "acme/widget"], true),
            (&["adopt"], false),
            (&["adopt", "--repo-id", "acme/widget"], false),
            (&["mount", "main", "--repo-id", "acme/widget"], false),
        ] {
            assert!(
                !binds(argv, resuming),
                "{argv:?} (resuming: {resuming}) must not bind the recorded primary"
            );
        }
    }

    /// A manager that answers every call with the refusal a daemon of another build gives, and
    /// reports what it was asked.
    fn manager_of_another_build(
        store: &Path,
    ) -> (
        tokio::task::JoinHandle<()>,
        tokio::sync::mpsc::UnboundedReceiver<String>,
    ) {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
        let run = store.join("run");
        fs::create_dir_all(&run).expect("run directory");
        let listener = tokio::net::UnixListener::bind(run.join("manager.sock")).expect("bind");
        let refusal = CowshedError::other_build(cowshed_core::OtherBuild {
            daemon: serde_json::from_value(serde_json::json!("daemon-build")).unwrap(),
            caller: Some(serde_json::from_value(serde_json::json!("this-build")).unwrap()),
        });
        let answer = serde_json::to_vec(&serde_json::json!({ "refused": refusal })).unwrap();
        let (asked, questions) = tokio::sync::mpsc::unbounded_channel();
        let served = tokio::spawn(async move {
            while let Ok((mut stream, _)) = listener.accept().await {
                let mut length = [0_u8; 4];
                stream.read_exact(&mut length).await.expect("a length");
                let mut request = vec![0_u8; u32::from_be_bytes(length) as usize];
                stream.read_exact(&mut request).await.expect("a request");
                asked
                    .send(String::from_utf8(request).expect("a JSON request"))
                    .expect("the test listens");
                stream
                    .write_all(&(answer.len() as u32).to_be_bytes())
                    .await
                    .expect("answer length");
                stream.write_all(&answer).await.expect("answer");
            }
        });
        (served, questions)
    }

    /// The refusal arrives before the verb does anything — it is the answer to the first thing
    /// asked — and names the install that ends the disagreement. A verb that diagnoses never asks.
    #[tokio::test]
    async fn a_removal_is_refused_up_front_by_a_daemon_of_another_build_naming_setup() {
        let store = std::env::temp_dir().join(format!("cs-gate-{}", std::process::id()));
        let (served, mut asked) = manager_of_another_build(&store);
        let remove = crate::args::parse_args(["rm", "raven"]).unwrap().command;
        let doctor = crate::args::parse_args(["doctor"]).unwrap().command;
        let setup = crate::args::parse_args(["setup"]).unwrap().command;
        let preflight = crate::args::parse_args(["land", "raven", "--preflight"])
            .unwrap()
            .command;
        let land = crate::args::parse_args(["land", "raven"]).unwrap().command;

        require_daemon_of_this_build_at(&store, &doctor)
            .await
            .expect("doctor diagnoses the disagreement");
        require_daemon_of_this_build_at(&store, &setup)
            .await
            .expect("setup ends the disagreement");
        require_daemon_of_this_build_at(&store, &preflight)
            .await
            .expect("land preflight only inspects and needs no matching daemon");
        assert!(
            asked.try_recv().is_err(),
            "a verb that diagnoses, repairs or inspects does not ask the daemon anything"
        );

        let error = require_daemon_of_this_build_at(&store, &remove)
            .await
            .expect_err("a daemon of another build refuses the removal before it begins");
        assert_eq!(error.code, ErrorCode::Conflict);
        assert!(error.other_build_source().is_some(), "{error:?}");
        assert!(
            error
                .message
                .contains("the cowshed daemon is build daemon-build")
                && error.message.contains("this cowshed is build this-build"),
            "{}",
            error.message
        );
        assert!(error.hint.contains("`cowshed setup`"), "{}", error.hint);
        let first = asked.try_recv().expect("the build check was asked");
        assert!(
            first.contains(r#""verb":"sameBuild""#),
            "the first thing asked is the build check: {first}"
        );
        assert!(asked.try_recv().is_err(), "and nothing after it, no ensure");

        let land_error = require_daemon_of_this_build_at(&store, &land)
            .await
            .expect_err("ordinary land still requires the matching daemon");
        assert_eq!(land_error, error);
        let land_check = asked
            .try_recv()
            .expect("ordinary land asked the build check");
        assert!(land_check.contains(r#""verb":"sameBuild""#), "{land_check}");
        assert!(
            asked.try_recv().is_err(),
            "land asked nothing after the refusal"
        );

        served.abort();
        fs::remove_dir_all(&store).ok();
    }

    /// With no daemon answering there is nothing to disagree: the verbs that need one report that
    /// themselves, and `adopt` on a fresh host must not be refused for a daemon that is not there.
    #[tokio::test]
    async fn no_daemon_is_no_disagreement() {
        let store = std::env::temp_dir().join(format!("cs-none-{}", std::process::id()));
        fs::create_dir_all(store.join("run")).expect("run directory");
        for argv in [
            &["rm", "raven"][..],
            &["adopt"],
            &["exec", "raven", "--", "true"],
        ] {
            let command = crate::args::parse_args(argv.iter().copied())
                .unwrap()
                .command;
            require_daemon_of_this_build_at(&store, &command)
                .await
                .expect("nothing answers the manager socket");
        }
        fs::remove_dir_all(&store).ok();
    }

    /// `cowshed exec`'s foreground relay against a real controller connection, whose router
    /// answers for one scripted job.
    mod foreground_relay {
        use super::*;
        use bytes::Bytes;
        use cowshed_core::api::dto::{
            CommandArg, ExecCommand as JobCommand, ExitStatus, GrantSet, RunSandboxMode,
            StdinSource, WorkspaceState,
        };
        use cowshed_core::api::server::{RouterHandle, RouterResponse};
        use cowshed_core::metadata::{WorkspaceIncarnation, WorkspaceName, WorkspaceRole};
        use cowshed_core::repository::{BoundIdentity, RepositoryBinding};
        use serde_json::{Value, json};
        use std::num::NonZeroUsize;

        const INCARNATION: &str = "0198f2c0b7e34dc795f17b238b331c80";
        const FIRST: &[u8] = b"first\n";

        /// One job behind the router. Its stdout holds `first\n` from the start; it ends when
        /// the test calls [`ScriptedJob::end`] or, when `ends_on_status`, as a status read
        /// arrives: the job finishing just as the soft timeout fires. Every call is answered
        /// on a task of its own, as the project router answers job calls.
        struct ScriptedJob {
            ended: tokio::sync::watch::Sender<bool>,
            ends_on_status: bool,
            exit_code: i32,
            detached: AtomicBool,
            /// Each exec admission: the route's method and the params it carried.
            admissions: Mutex<Vec<(String, Value)>>,
        }

        impl ScriptedJob {
            fn new(ends_on_status: bool, exit_code: i32) -> Arc<Self> {
                Arc::new(Self {
                    ended: tokio::sync::watch::channel(false).0,
                    ends_on_status,
                    exit_code,
                    detached: AtomicBool::new(false),
                    admissions: Mutex::new(Vec::new()),
                })
            }

            fn end(&self) {
                self.ended.send_replace(true);
            }

            fn is_ended(&self) -> bool {
                *self.ended.borrow()
            }

            async fn until_ended(&self) {
                let mut ended = self.ended.subscribe();
                ended
                    .wait_for(|ended| *ended)
                    .await
                    .expect("the job outlives its calls");
            }

            fn info(&self) -> Value {
                let ended = self.is_ended();
                let stream = |data: &str, bytes: usize, sha256: &str| {
                    json!({
                        "storage": {"kind": "captured", "artifact": {"kind": "inline", "data": {"encoding": "utf8", "data": data}}},
                        "bytes": bytes,
                        "sha256": sha256,
                        "summary": {"version": 1, "text": "", "truncated": false}
                    })
                };
                let mut info = json!({
                    "repoId": "acme/widget",
                    "workspaceIncarnation": INCARNATION,
                    "jobId": 1,
                    "state": if ended { "exited" } else { "running" },
                    "class": "background",
                    "scope": "workspace",
                    "grantRevision": 1,
                    "argv": [{"encoding": "utf8", "data": "build"}],
                    "cwd": null,
                    "started": "2026-07-13T00:00:00Z",
                    "stdout": stream("first\n", 6, "b640e840b19d378660b32fb51ae18d67dccb4a8596a29e7bd72c1b2ae5928f41"),
                    "stderr": stream("", 0, "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"),
                    "trace": {"traceId": "4bf92f3577b34da6a3ce929d0e0e4736", "spanId": "00f067aa0ba902b7"},
                    "stdin": {"kind": "empty", "bytes": 0, "complete": true}
                });
                if ended {
                    info["durationMs"] = json!(1);
                    info["exit"] = json!({"kind": "exited", "code": self.exit_code});
                }
                info
            }

            /// A read of `params`' stream at its offset now, or `None` when a follow read waits.
            fn chunk(&self, params: &Value) -> Result<Option<RouterResponse>> {
                let bytes: &[u8] = match params["stream"].as_str() {
                    Some("stdout") => FIRST,
                    _ => b"",
                };
                let offset = params["offset"].as_u64().expect("a log offset");
                let start = usize::try_from(offset).expect("a test offset");
                let rest = &bytes[start.min(bytes.len())..];
                let ended = self.is_ended();
                if rest.is_empty() && !ended && params["follow"] == json!(true) {
                    return Ok(None);
                }
                RouterResponse::binary(
                    json!({"eof": ended, "nextOffset": offset + rest.len() as u64}),
                    Bytes::copy_from_slice(rest),
                )
                .map(Some)
            }

            async fn answer(
                self: Arc<Self>,
                method: String,
                params: Value,
            ) -> Result<RouterResponse> {
                let repo_id = RepoId::parse("acme/widget").expect("repo");
                match method.as_str() {
                    "project.open" => Ok(RouterResponse::json(json!({
                        "repoId": repo_id,
                        "binding": RepositoryBinding::new(vec![BoundIdentity {
                            repo_id: repo_id.clone(),
                            remote_name: None,
                            remote_url: None,
                            primary: true,
                        }])
                        .expect("binding"),
                        "gitRoot": "/w/widget",
                        "storeRoot": "/w/store",
                    }))),
                    "coordinator.worker" => Ok(RouterResponse::json(json!({
                        "info": WorkspaceInfo {
                            repo_id,
                            workspace: WorkspaceName::new("main").expect("name"),
                            workspace_incarnation: WorkspaceIncarnation::new(INCARNATION)
                                .expect("incarnation"),
                            role: WorkspaceRole::Main,
                            mount: PathBuf::from("/w/widget"),
                            state: WorkspaceState::Attached,
                            branch: None,
                            base_commit: None,
                            created_at: None,
                            checkpoints: Vec::new(),
                            snapshot_stale: false,
                            landing: None,
                        },
                        "grants": GrantSet::default(),
                    }))),
                    "worker.exec" | "coordinator.exec" => {
                        self.admissions
                            .lock()
                            .expect("admissions")
                            .push((method.clone(), params));
                        Ok(RouterResponse::json(json!(1)))
                    }
                    "job.status" => {
                        if self.ends_on_status {
                            self.end();
                        }
                        Ok(RouterResponse::json(self.info()))
                    }
                    "job.wait" => {
                        self.until_ended().await;
                        Ok(RouterResponse::json(self.info()))
                    }
                    "job.detach" => {
                        self.detached.store(true, Ordering::SeqCst);
                        Ok(RouterResponse::json(json!({})))
                    }
                    "job.logs" => match self.chunk(&params)? {
                        Some(chunk) => Ok(chunk),
                        None => {
                            self.until_ended().await;
                            Ok(self.chunk(&params)?.expect("an ended job's reads answer"))
                        }
                    },
                    other => Err(CowshedError::internal(format!("unscripted call {other}"))),
                }
            }
        }

        /// The operator's coordinator over a real controller connection to `job`'s router.
        async fn coordinator(job: &Arc<ScriptedJob>) -> Coordinator {
            let repo_id = RepoId::parse("acme/widget").expect("repo");
            let (router, mut commands) =
                RouterHandle::channel(NonZeroUsize::new(16).expect("capacity"));
            let script = Arc::clone(job);
            tokio::spawn(async move {
                while let Some(command) = commands.recv().await {
                    let (request, reply) = command.into_parts();
                    let (_, operation, _, _) = request.into_parts();
                    let method = operation.method().to_owned();
                    let params = operation.params().expect("a decoded request encodes");
                    let script = Arc::clone(&script);
                    tokio::spawn(async move {
                        let _ = reply.send(script.answer(method, params).await);
                    });
                }
            });
            let (client, server) = std::os::unix::net::UnixStream::pair().expect("socket pair");
            tokio::spawn(serve_controller_connection(
                server.into(),
                ConnectionAuthority::Coordinator { repo_id },
                router,
            ));
            let (cowshed, token) = cowshed_core::Cowshed::connect(client.into())
                .await
                .expect("handshake");
            let project = cowshed.open("/w/widget").await.expect("open");
            cowshed.coordinator(&project, token).expect("coordinator")
        }

        async fn connect(job: &Arc<ScriptedJob>) -> JobHandle {
            let worker = coordinator(job).await.worker("main").await.expect("worker");
            worker
                .exec(ExecRequest {
                    command: JobCommand::Argv(vec![CommandArg::from("build")]),
                    cwd: None,
                    mode: RunSandboxMode::ReadWrite,
                    env: HashMap::new(),
                    trace: None,
                    stdin: StdinSource::Empty,
                    stdout_copy: None,
                    stderr_copy: None,
                    admission_key: None,
                    class: cowshed_core::api::ExecClass::Background,
                    want: None,
                    scope: cowshed_core::api::ExecScope::Workspace,
                })
                .await
                .expect("exec")
        }

        #[derive(Clone, Default)]
        struct Terminal(Arc<Mutex<Vec<u8>>>);

        impl Terminal {
            fn contents(&self) -> Vec<u8> {
                self.0.lock().expect("terminal").clone()
            }
        }

        impl Write for Terminal {
            fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
                self.0.lock().expect("terminal").extend_from_slice(bytes);
                Ok(bytes.len())
            }

            fn flush(&mut self) -> io::Result<()> {
                Ok(())
            }
        }

        /// The CLI is the coordinator route (07_api.md, "CLI and API exec parity"): every exec
        /// it admits travels as coordinator.exec, whatever its class, so an operator's `--host`
        /// at the default class reaches admission as background work at the host scope. Who asks
        /// picks the route; the class it asks for never does.
        #[tokio::test]
        async fn the_cli_admits_every_exec_through_the_coordinator_route() {
            for (argv, class, scope) in [
                (
                    &["exec", "main", "--background", "--host", "--", "build"][..],
                    None,
                    Some("hostGate"),
                ),
                (
                    &["exec", "main", "--background", "--", "build"][..],
                    None,
                    None,
                ),
                (
                    &[
                        "exec",
                        "main",
                        "--background",
                        "--class",
                        "gate",
                        "--host",
                        "--",
                        "gate",
                    ][..],
                    Some("gate"),
                    Some("hostGate"),
                ),
            ] {
                let job = ScriptedJob::new(false, 0);
                let mut bridge = ActorBridge {
                    coordinator: Some(coordinator(&job).await),
                    connection: None,
                    runtime: None,
                    backend: GatewayStoreBackend::Native,
                };
                let Command::Exec(args) = crate::args::parse_args(argv.iter().copied())
                    .expect("arguments")
                    .command
                else {
                    unreachable!("an exec invocation parses as exec");
                };
                let command = exec_command(args, tokio::io::empty()).expect("exec command");
                let result = bridge
                    .exec(
                        command,
                        ExecPresentation::Raw,
                        &mut Terminal::default(),
                        &mut Terminal::default(),
                    )
                    .await
                    .unwrap_or_else(|error| panic!("{argv:?} is admitted: {}", error.message));
                assert_eq!(result.end, ExecEnd::Backgrounded, "{argv:?}");
                let admissions = job.admissions.lock().expect("admissions").clone();
                let [(method, params)] = admissions.as_slice() else {
                    panic!("{argv:?} admitted one job: {admissions:?}");
                };
                assert_eq!(
                    (
                        method.as_str(),
                        params.get("class").and_then(Value::as_str),
                        params.get("scope").and_then(Value::as_str)
                    ),
                    ("coordinator.exec", class, scope),
                    "{argv:?}"
                );
            }
        }

        #[tokio::test]
        async fn a_foreground_jobs_output_reaches_the_terminal_while_it_runs() {
            let job = ScriptedJob::new(false, 0);
            let handle = connect(&job).await;
            let terminal = Terminal::default();
            let (mut stdout, mut stderr) = (terminal.clone(), Terminal::default());
            let relay = relay_foreground(
                &handle,
                None,
                None,
                ExecPresentation::Raw,
                &mut stdout,
                &mut stderr,
            );
            tokio::pin!(relay);
            let first_line = async {
                while terminal.contents() != FIRST {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            };
            tokio::select! {
                _ = &mut relay => panic!("the relay returned while the job still ran"),
                () = first_line => {}
            }
            job.end();
            let result = relay.await.expect("relay");
            assert_eq!(result.end, ExecEnd::Finished);
            assert_eq!(result.info.exit, Some(ExitStatus::Exited { code: 0 }));
            assert_eq!(terminal.contents(), FIRST);
        }

        /// A job whose wait fails while its output streams stay open: the first failure ends
        /// the relay. Joining all three instead waited for streams that end only with the job's
        /// end, which the failed wait means nobody will see.
        #[tokio::test(start_paused = true)]
        async fn a_failed_wait_ends_the_relay_while_its_streams_stay_open() {
            struct Refused;
            #[async_trait]
            impl ForegroundJob for Refused {
                async fn wait(&self) -> Result<JobInfo> {
                    Err(CowshedError::environment_missing(
                        "job 7 membership retirement refused",
                        "retain the job's claims",
                    ))
                }
                async fn status(&self) -> Result<JobInfo> {
                    Err(CowshedError::internal("a failed wait reads no status"))
                }
                async fn detach(&self) -> Result<()> {
                    Ok(())
                }
                async fn kill(&self) -> Result<()> {
                    Ok(())
                }
                async fn relay(&self, _: JobStream, _: &mut (dyn Write + Send)) -> Result<()> {
                    std::future::pending().await
                }
            }
            let (mut stdout, mut stderr) = (Terminal::default(), Terminal::default());
            let Err(refused) = relay_foreground(
                &Refused,
                None,
                None,
                ExecPresentation::Raw,
                &mut stdout,
                &mut stderr,
            )
            .await
            else {
                panic!("the wait failed, so the relay fails");
            };
            assert_eq!(refused.message, "job 7 membership retirement refused");
        }

        /// The soft timeout fires, and by the time its status read is answered the job has
        /// ended: there is nothing to background. Its output and exit code are the command's.
        #[tokio::test]
        async fn a_job_that_ends_as_the_soft_timeout_fires_keeps_its_output_and_exit_code() {
            let job = ScriptedJob::new(true, 3);
            let handle = connect(&job).await;
            let terminal = Terminal::default();
            let (mut stdout, mut stderr) = (terminal.clone(), Terminal::default());
            let result = relay_foreground(
                &handle,
                None,
                Some(Duration::from_millis(50)),
                ExecPresentation::Raw,
                &mut stdout,
                &mut stderr,
            )
            .await
            .expect("relay");
            assert_eq!(
                result.end,
                ExecEnd::Finished,
                "a finished job is not left running"
            );
            assert_eq!(result.info.exit, Some(ExitStatus::Exited { code: 3 }));
            assert_eq!(terminal.contents(), FIRST);
            assert!(!job.detached.load(Ordering::SeqCst));
        }

        /// The exec a caller writes with no `--timeout` waits for its job however long the job
        /// runs: a day of paused time passes with the job still running and the exec still
        /// relaying it, and it then reports the job's own end.
        #[tokio::test(start_paused = true)]
        async fn a_foreground_exec_waits_for_its_job_however_long_it_runs() {
            let job = ScriptedJob::new(false, 0);
            let handle = connect(&job).await;
            let Command::Exec(args) = crate::args::parse_args(["exec", "main", "--", "build"])
                .expect("arguments")
                .command
            else {
                unreachable!("an exec invocation parses as exec");
            };
            let command = exec_command(args, tokio::io::empty()).expect("exec command");
            let terminal = Terminal::default();
            let (mut stdout, mut stderr) = (terminal.clone(), Terminal::default());
            let relay = relay_foreground(
                &handle,
                None,
                command.timeout,
                ExecPresentation::Raw,
                &mut stdout,
                &mut stderr,
            );
            tokio::pin!(relay);
            tokio::select! {
                result = &mut relay => panic!(
                    "the exec stopped waiting while its job ran: {:?}",
                    result.map(|result| result.info.state)
                ),
                () = tokio::time::sleep(Duration::from_secs(24 * 60 * 60)) => {}
            }
            job.end();
            let result = relay.await.expect("relay");
            assert_eq!(result.info.exit, Some(ExitStatus::Exited { code: 0 }));
            assert_eq!(terminal.contents(), FIRST);
            assert!(!job.detached.load(Ordering::SeqCst));
        }

        /// A job still running when the caller's `--timeout` passes keeps running, and the exec
        /// fails: it has no output or status of the command's to give, so it exits with the
        /// conflict wrapper code and names the job and how to reach it.
        #[tokio::test]
        async fn a_job_still_running_at_the_callers_timeout_fails_the_exec_and_names_the_job() {
            for json in [false, true] {
                let job = ScriptedJob::new(false, 0);
                let handle = connect(&job).await;
                let (mut stdout, mut stderr) = (Terminal::default(), Terminal::default());
                let result = relay_foreground(
                    &handle,
                    None,
                    Some(Duration::from_millis(50)),
                    ExecPresentation::Raw,
                    &mut stdout,
                    &mut stderr,
                )
                .await
                .expect("relay");
                assert!(job.detached.load(Ordering::SeqCst));
                let mut output = Output::new(Vec::new(), Vec::new(), false);
                let exit = report_exec(&mut output, json, "main", result).expect("report");
                let (stdout, stderr) = output.into_inner();
                let (stdout, stderr) = (
                    String::from_utf8(stdout).expect("utf-8"),
                    String::from_utf8(stderr).expect("utf-8"),
                );
                assert_eq!(
                    exit.code,
                    i32::from(ErrorCode::Conflict.exec_wrapper_exit_code()),
                    "stdout {stdout:?}, stderr {stderr:?}"
                );
                let said = if json {
                    let envelope: serde_json::Value =
                        serde_json::from_str(&stdout).expect("a JSON envelope");
                    assert_eq!(envelope["ok"], json!(false));
                    assert_eq!(envelope["error"]["code"], json!("conflict"));
                    format!(
                        "{} {}",
                        envelope["error"]["message"], envelope["error"]["hint"]
                    )
                } else {
                    assert_eq!(stdout, "", "no job id passes for the command's output");
                    stderr
                };
                assert!(said.contains("job 1"), "{said}");
                assert!(
                    said.contains(".job(1)"),
                    "names how to reach the job: {said}"
                );
            }
        }

        /// `--background` asks not to wait: the job id is the command's output and the exec
        /// succeeds, and stderr says how to reach the job.
        #[test]
        fn a_backgrounded_exec_prints_its_job_and_how_to_reach_it() {
            let job = ScriptedJob::new(false, 0);
            let info: JobInfo = serde_json::from_value(job.info()).expect("a job record");
            let mut output = Output::new(Vec::new(), Vec::new(), false);
            let exit = report_exec(
                &mut output,
                false,
                "main",
                ExecResult {
                    info,
                    end: ExecEnd::Backgrounded,
                },
            )
            .expect("report");
            let (stdout, stderr) = output.into_inner();
            assert_eq!(exit.code, 0);
            assert_eq!(stdout, b"1\n");
            let stderr = String::from_utf8(stderr).expect("utf-8");
            assert!(stderr.contains(".job(1)"), "{stderr}");
        }
    }

    fn planned_volume(name: &str, role: VolumeRole, state_before: VolumeState) -> VolumeOutcome {
        VolumeOutcome {
            name: name.to_owned(),
            role,
            state_before,
            action: "planned".to_owned(),
        }
    }

    #[test]
    fn doctor_names_a_saturated_vnode_table_and_the_limit_to_raise() {
        use cowshed_core::vnodes::VnodeTable;
        let finding = vnode_table_finding(Ok(VnodeTable {
            allocated: 272_631,
            free: 0,
            limit: 263_168,
        }))
        .expect("a saturated table is a finding");
        assert_eq!(finding.code, "vnode-table-saturated");
        assert_eq!(finding.severity, FindingSeverity::Warning);
        assert!(finding.hint.contains("kern.maxvnodes="), "{finding:?}");

        // This host's steady state when it was reported saturated: a full cache, mostly free.
        let full_cache = VnodeTable {
            allocated: 1_048_649,
            free: 893_580,
            limit: 1_048_576,
        };
        assert!(vnode_table_finding(Ok(full_cache)).is_none());
        let linux = std::io::Error::new(std::io::ErrorKind::Unsupported, "no vnode table");
        assert!(vnode_table_finding(Err(linux)).is_none());
    }

    fn reading(
        host: (u64, u64),
        gateway: std::result::Result<(u64, u64), &str>,
    ) -> Result<gateway_service::FirstLaunchReading> {
        let launch = |(first, second): (u64, u64)| cowshed_gateway_types::FirstLaunch {
            first_micros: first * 1000,
            second_micros: second * 1000,
        };
        Ok(gateway_service::FirstLaunchReading {
            host: launch(host),
            gateway: gateway.map(launch).map_err(str::to_owned),
            gateway_host: PathBuf::from(
                "/home/Library/Application Support/dev.cowshed/bin/cowshed-gateway-host",
            ),
            restart: "launchctl kickstart -k gui/501/dev.cowshed.gateway".into(),
        })
    }

    /// A gateway whose fresh executable waits for the scan names the one entry to add, by path,
    /// and the pane to add it in.
    #[test]
    fn a_charged_first_launch_names_the_gateway_host_to_add_to_developer_tools() {
        let finding = first_launch_finding(reading((25, 27), Ok((6_800, 29))));
        assert_eq!(finding.code, "first-launch-scan");
        assert_eq!(finding.severity, FindingSeverity::Warning);
        assert!(
            finding.message.contains("6800.0 ms under the gateway (then 29.0 ms) and 25.0 ms in this shell (then 27.0 ms)"),
            "{finding:?}"
        );
        assert!(finding.message.contains("Developer Tools"), "{finding:?}");
        assert!(
            finding
                .message
                .contains("/home/Library/Application Support/dev.cowshed/bin/cowshed-gateway-host"),
            "{finding:?}"
        );
        assert!(
            finding.message.contains(
                "started after it was added (launchctl kickstart -k gui/501/dev.cowshed.gateway"
            ),
            "{finding:?}"
        );
        assert_eq!(
            finding.hint,
            format!(
                "{DEVELOPER_TOOLS_PANE}; then launchctl kickstart -k gui/501/dev.cowshed.gateway"
            )
        );
        assert!(
            doctor_report(vec![finding]).healthy,
            "a scan is a cost, not a broken host"
        );
    }

    #[test]
    fn an_exempt_first_launch_is_information_and_a_missing_measurement_a_warning() {
        let exempt = first_launch_finding(reading((25, 27), Ok((31, 26))));
        assert_eq!(exempt.code, "first-launch-exempt");
        assert_eq!(exempt.severity, FindingSeverity::Info);
        assert!(exempt.hint.is_empty());
        let older =
            first_launch_finding(reading((25, 27), Err("unknown gateway control operation")));
        assert_eq!(older.code, "first-launch-unmeasured");
        assert_eq!(older.severity, FindingSeverity::Warning);
        assert_eq!(older.hint, "cowshed setup");
    }

    #[test]
    fn doctor_reports_the_disk_image_classes_and_warns_on_a_leaked_device() {
        use cowshed_apfs::attach_budget::{
            ClassCount, DEPARTED_ATTRIBUTION_HINT, KernelReading, LEAKED_DEVICES_HINT,
        };
        let count = |instances, registered| ClassCount {
            instances,
            registered,
        };
        // This host at its first 150 on 2026-10-10, and just after the reboot.
        let leaking_host = KernelReading {
            image_devices: count(1090, 66),
            user_clients: count(1077, 66),
            apfs_volumes: count(1065, 81),
            media: count(1075, 80),
        };
        let rebooted_host = KernelReading {
            image_devices: count(178, 178),
            user_clients: count(178, 178),
            apfs_volumes: count(193, 193),
            media: count(185, 185),
        };
        let classes = |reading: KernelReading| {
            disk_image_attach_budget_finding(Ok(reading)).expect("the classes are always reported")
        };

        let fresh = classes(rebooted_host);
        assert_eq!(fresh.code, "disk-image-attach-budget");
        assert_eq!(fresh.severity, FindingSeverity::Info);
        assert_eq!(fresh.hint, "nothing to free");
        assert!(
            fresh.message.starts_with(
                "AppleDiskImageDevice 178 instances, 178 registered, 0 leaked; DIDeviceIOUserClient"
            ),
            "{fresh:?}"
        );
        // Leaked image devices warn and name what frees them; nothing is ever an error.
        let leaked = classes(leaking_host);
        assert_eq!(leaked.severity, FindingSeverity::Warning);
        assert_eq!(leaked.hint, LEAKED_DEVICES_HINT);
        assert!(
            leaked
                .message
                .starts_with("AppleDiskImageDevice 1090 instances, 66 registered, 1024 leaked"),
            "{leaked:?}"
        );

        let linux = std::io::Error::new(std::io::ErrorKind::Unsupported, "no classes");
        assert!(disk_image_attach_budget_finding(Err(linux)).is_none());
        let refused = std::io::Error::other("IOServiceMatching refused");
        let unread = disk_image_attach_budget_finding(Err(refused)).expect("a failed read");
        assert_eq!(unread.severity, FindingSeverity::Warning);

        // Departed volumes are a warning of their own whose hint attributes them.
        let departed = departed_volume_references_finding(&leaking_host)
            .expect("departed volumes are a finding");
        assert_eq!(departed.code, "departed-volume-references");
        assert_eq!(departed.severity, FindingSeverity::Warning);
        assert!(
            departed
                .message
                .starts_with("984 departed AppleAPFSVolume and 995 departed IOMedia instances: "),
            "{departed:?}"
        );
        assert_eq!(departed.hint, DEPARTED_ATTRIBUTION_HINT);
        assert_eq!(departed.path, None);

        // The first departed instance of either class warns, and the device classes do not move
        // for it.
        for (reading, counts) in [
            (
                KernelReading {
                    apfs_volumes: count(194, 193),
                    ..rebooted_host
                },
                "1 departed AppleAPFSVolume and 0 departed IOMedia instances: ",
            ),
            (
                KernelReading {
                    media: count(186, 185),
                    ..rebooted_host
                },
                "0 departed AppleAPFSVolume and 1 departed IOMedia instances: ",
            ),
        ] {
            let first = departed_volume_references_finding(&reading)
                .expect("the first departed instance is a finding");
            assert_eq!(first.severity, FindingSeverity::Warning);
            assert!(first.message.starts_with(counts), "{first:?}");
            let unmoved = classes(reading);
            assert_eq!(unmoved.severity, FindingSeverity::Info);
            assert_eq!(unmoved.hint, "nothing to free");
        }
        // Nothing departed, nothing to report.
        assert!(departed_volume_references_finding(&rebooted_host).is_none());

        // A warning never makes the host unhealthy: only an error does.
        assert!(doctor_report(vec![departed, leaked]).healthy);
    }

    /// The media finding counts what making room counts, warns while few images fit, past the
    /// ceiling too, never errs, and says nothing to free while many fit.
    #[test]
    fn doctor_reports_the_media_notification_cap_and_what_frees_media() {
        use cowshed_apfs::media_cap::{MEDIA_CAP_HINT, MediaReading};
        let finding = |registered| {
            media_notification_cap_finding(Ok(MediaReading {
                registered,
                attaching: 0,
            }))
            .expect("a reading is always reported")
        };
        // 16 media for the host's own disks are committed too.
        let roomy = finding(900);
        assert_eq!(roomy.code, "media-notification-cap");
        assert_eq!(roomy.severity, FindingSeverity::Info);
        assert_eq!(roomy.hint, "nothing to free");
        assert!(
            roomy.message.starts_with(
                "900 IOMedia registered (subclasses included) and 0 image attach(es) running; \
                 with the host's reserve that commits 916 of IOKit's 1024-entry"
            ) && roomy
                .message
                .contains("room for 35 more image(s), 17 workspace(s)"),
            "{roomy:?}"
        );
        let low = finding(960);
        assert_eq!(low.severity, FindingSeverity::Warning);
        assert_eq!(low.hint, MEDIA_CAP_HINT);
        assert!(low.message.contains("room for 15 more image(s)"), "{low:?}");
        let full = finding(1_100);
        assert_eq!(full.severity, FindingSeverity::Warning);
        assert_eq!(full.hint, MEDIA_CAP_HINT);
        assert!(
            full.message.contains("room for 0 more image(s)"),
            "{full:?}"
        );
        assert_eq!(
            media_notification_cap_finding(Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "linux"
            ))),
            None
        );
    }

    #[test]
    fn doctor_names_orphaned_disk_image_helpers_and_the_root_command() {
        use cowshed_apfs::disk_image_helpers::{HelperReading, OrphanedHelpers};
        use std::collections::BTreeSet;
        let helpers = BTreeSet::from([4353, 25101, 75251]);
        let orphans = OrphanedHelpers::among(&helpers, &BTreeSet::from([25101]));
        let finding =
            orphaned_disk_image_helpers_finding(Ok(HelperReading::Settled(orphans.clone())))
                .expect("orphans are a finding");
        assert_eq!(finding.code, "orphaned-disk-image-helpers");
        assert_eq!(finding.severity, FindingSeverity::Warning);
        assert_eq!(finding.hint, "sudo kill -9 4353 75251");
        assert!(finding.message.contains("(pid 4353, 75251)"), "{finding:?}");

        // While a disk-image tool runs, no pid is named.
        let busy = orphaned_disk_image_helpers_finding(Ok(HelperReading::from_reads(
            (orphans.clone(), true),
            (orphans, false),
        )))
        .expect("suspects are said");
        assert_eq!(busy.severity, FindingSeverity::Info);
        assert!(!busy.message.contains("4353"), "{busy:?}");
        assert!(!busy.hint.contains("kill"), "{busy:?}");

        let served = OrphanedHelpers::among(&helpers, &helpers);
        assert!(orphaned_disk_image_helpers_finding(Ok(HelperReading::Settled(served))).is_none());
        let linux = std::io::Error::new(std::io::ErrorKind::Unsupported, "no helpers");
        assert!(orphaned_disk_image_helpers_finding(Err(linux)).is_none());
        let refused = std::io::Error::other("proc_pidpath(1): refused");
        let finding = orphaned_disk_image_helpers_finding(Err(refused)).expect("a failed read");
        assert_eq!(finding.code, "disk-image-helpers");
    }

    /// Compile-visible seam over core's `StdinSource`: every variant must name the CLI flag that
    /// produces it (see `exec_command`, which maps the other direction and therefore cannot be
    /// exhaustive over core). Adding a core variant breaks this match, pointing at the flag that
    /// has to exist instead of silently becoming a mode the CLI cannot express.
    fn cli_stdin_spelling(source: &CoreStdinSource) -> &'static str {
        match source {
            CoreStdinSource::Empty => "(stdin omitted)",
            CoreStdinSource::Stream(_) => "--stdin",
            CoreStdinSource::WorkspaceFile(_) => "--stdin-file",
            CoreStdinSource::Inline(_) => "--stdin-base64",
            CoreStdinSource::Open => "(no CLI spelling: an attachment feeds open stdin)",
        }
    }

    /// Every core variant is named, and every flag it names is one `exec` actually accepts.
    ///
    /// The match above is the compile-time seam; constructing every variant is what makes it a
    /// test.
    /// Leaving `WorkspaceFile` out meant a wrong `--stdin-file` spelling could not go red, and
    /// asserting the string alone would still not prove the flag exists — so the second half
    /// parses each spelling and checks it selects the CLI source that maps back to that variant.
    #[test]
    fn every_core_stdin_variant_has_a_cli_spelling_the_parser_accepts() {
        assert_eq!(
            cli_stdin_spelling(&CoreStdinSource::Empty),
            "(stdin omitted)"
        );
        assert_eq!(
            cli_stdin_spelling(&CoreStdinSource::Stream(Box::pin(tokio::io::empty()))),
            "--stdin"
        );
        assert_eq!(
            cli_stdin_spelling(&CoreStdinSource::WorkspaceFile(
                WorkspacePath::new("input.txt").expect("workspace-relative path")
            )),
            "--stdin-file"
        );
        assert_eq!(
            cli_stdin_spelling(&CoreStdinSource::Inline(bytes::Bytes::new())),
            "--stdin-base64"
        );
        assert_eq!(
            cli_stdin_spelling(&CoreStdinSource::Open),
            "(no CLI spelling: an attachment feeds open stdin)"
        );

        let parsed = |argv: &[&str]| {
            let Command::Exec(exec) = crate::args::parse_args(argv.iter().copied())
                .unwrap_or_else(|error| panic!("{argv:?}: {error}"))
                .command
            else {
                panic!("{argv:?} is an exec")
            };
            exec.stdin
        };
        assert_eq!(
            parsed(&["exec", "raven", "--stdin", "--", "true"]),
            Some(CliStdinSource::Stream)
        );
        assert_eq!(
            parsed(&["exec", "raven", "--stdin-file", "input.txt", "--", "true"]),
            Some(CliStdinSource::WorkspaceFile(PathBuf::from("input.txt")))
        );
        assert_eq!(
            parsed(&["exec", "raven", "--stdin-base64", "aGk=", "--", "true"]),
            Some(CliStdinSource::InlineBase64(std::ffi::OsString::from(
                "aGk="
            )))
        );
        assert_eq!(parsed(&["exec", "raven", "--", "true"]), None);
    }

    /// `attach --all` with nothing detached but mains down must name the mains, not print the
    /// bare no-sessions line: mains are always-mounted, so their absence is the actual defect.
    #[test]
    fn attach_all_without_detached_sessions_names_unmounted_mains() {
        let mains = vec![
            UnreachableMain {
                repo_id: RepoId::parse("acme/alpha").expect("repo"),
                image: PathBuf::from("/store/acme/alpha/main.dmg"),
                mountpoint: PathBuf::from("/checkouts/alpha"),
                reason: "main's volume is not mounted".into(),
            },
            UnreachableMain {
                repo_id: RepoId::parse("acme/beta").expect("repo"),
                image: PathBuf::from("/store/acme/beta/main.dmg"),
                mountpoint: PathBuf::from("/checkouts/beta"),
                reason: "main's volume is not mounted".into(),
            },
        ];
        let error = no_detached_with_mains(&mains);
        assert_eq!(error.code, ErrorCode::NotFound);
        assert!(error.message.contains("acme/alpha"), "{}", error.message);
        assert!(error.message.contains("acme/beta"), "{}", error.message);
        assert!(
            error.message.contains("2 main workspace"),
            "{}",
            error.message
        );
        assert_eq!(error.hint, "cowshed gateway start");
    }

    #[test]
    fn attach_all_without_detached_sessions_or_mains_stays_bare() {
        let error = no_detached_with_mains(&[]);
        assert_eq!(error.message, "no detached session workspace found");
        assert_eq!(
            error.hint,
            "cowshed ls; cowshed attach <ws>; cowshed attach --all"
        );
    }

    /// The mains line's remedy is the same command `doctor` prescribes for an unmounted main:
    /// a hint that named anything else would be blind to the condition it reports.
    #[test]
    fn attach_mains_hint_matches_doctor_main_hint() {
        let main = UnreachableMain {
            repo_id: RepoId::parse("acme/alpha").expect("repo"),
            image: PathBuf::from("/store/acme/alpha/main.dmg"),
            mountpoint: PathBuf::from("/checkouts/alpha"),
            reason: "main's volume is not mounted".into(),
        };
        assert_eq!(
            no_detached_with_mains(std::slice::from_ref(&main)).hint,
            main_mount_finding(&main).hint,
        );
    }

    /// A checkout-less project stays visible as an info finding with no remedy hint: the
    /// inventory skips it, but nothing is broken that `doctor` can name.
    #[test]
    fn checkoutless_project_is_an_info_finding_without_a_hint() {
        let repo = RepoId::parse("acme/widget").expect("repo");
        let store = std::env::temp_dir().join("cowshed-checkoutless-test");
        let finding = no_adopted_checkout_finding(&repo, &store);
        assert_eq!(finding.code, "no-adopted-checkout");
        assert_eq!(finding.severity, FindingSeverity::Info);
        assert!(
            finding.message.contains("acme/widget"),
            "{}",
            finding.message
        );
        assert!(
            finding.hint.is_empty(),
            "no command restores a checkout path the store never recorded; hint was {}",
            finding.hint
        );
        assert!(
            !finding.hint.contains("doctor"),
            "hint was {}",
            finding.hint
        );
        assert_eq!(
            finding.path,
            StorageLayout::new(&store, &repo)
                .map(|layout| layout.project().project_root.clone())
                .ok()
        );
    }

    /// A store-wide mount change that could not reach the gateway is said out loud, on stderr,
    /// naming the project and carrying the recovery — and it does not fail the command, because
    /// the mounts happened.
    ///
    /// Silence here is the bug the audit found: `attach --all` and `detach --all` never called
    /// `reconcile_native_project` at all, so the daemon's view stayed stale until the next
    /// project-scoped exec and nobody was told.
    #[test]
    fn a_gateway_that_could_not_be_told_is_named_without_failing_the_mount() {
        let repo_id = RepoId::parse("acme/widget").expect("repo identity");
        let refusal = CowshedError::environment_missing(
            "gateway control socket does not answer",
            "cowshed gateway start",
        );
        let mut output = Output::new(Vec::new(), Vec::new(), false);

        report_stale_gateway_views(&mut output, &[(repo_id, refusal)])
            .expect("reporting a stale view is not itself a failure");

        let (stdout, stderr) = output.into_inner();
        assert!(stdout.is_empty(), "the JSON channel stays clean");
        let stderr = String::from_utf8(stderr).expect("utf-8 guidance");
        assert!(
            stderr.contains("acme/widget"),
            "the project whose mounts moved has to be named: {stderr:?}"
        );
        assert!(
            stderr.contains("the gateway was not told"),
            "the operator has to learn the daemon's view is stale: {stderr:?}"
        );
        assert!(
            stderr.contains("cowshed gateway start"),
            "and what to run about it: {stderr:?}"
        );

        // Nothing to report is nothing written.
        let mut quiet = Output::new(Vec::new(), Vec::new(), false);
        report_stale_gateway_views(&mut quiet, &[]).expect("no stale views");
        let (stdout, stderr) = quiet.into_inner();
        assert!(stdout.is_empty() && stderr.is_empty());
    }

    /// The bare `cowshed grant <ws>` listing is where an operator audits a workspace's reach, so a
    /// loopback-any grant is a row of its own, after the paths and egress, and absent otherwise.
    #[test]
    fn the_grant_listing_names_a_loopback_any_grant() {
        let listed = |loopback_any| {
            let mut output = Output::new(Vec::new(), Vec::new(), false);
            emit_grants(
                &mut output,
                &GrantSet {
                    read: vec![PathBuf::from("/opt/shared")],
                    loopback_any,
                    ..GrantSet::default()
                },
            )
            .expect("listing");
            String::from_utf8(output.into_inner().0).expect("utf-8 listing")
        };
        assert_eq!(listed(true), "read\t/opt/shared\nloopback\tany\n");
        assert_eq!(listed(false), "read\t/opt/shared\n");
    }

    /// A project listing gives an unproven workspace no row and names it, its refusal and `doctor` on
    /// stderr (06_cli.md, "`cowshed ls` unproven workspaces"), so one shed's own detach never
    /// fails its project's `ls`.
    #[test]
    fn an_unproven_workspace_is_named_on_stderr_and_never_on_stdout() {
        let repo_id = RepoId::parse("acme/widget").expect("repo identity");
        let held = UnprovenWorkspace {
            workspace: WorkspaceName::new("detaching").expect("workspace name"),
            workspace_incarnation: WorkspaceIncarnation::new("f221ac9777c546caba9b48a20f9e68b4")
                .expect("workspace incarnation"),
            mount: PathBuf::from("/mnt/acme/widget/detaching"),
            reason: "workspace mount identity mismatch at /mnt/acme/widget/detaching: no readable marker"
                .to_owned(),
        };
        let mut output = Output::new(Vec::new(), Vec::new(), false);

        emit_unproven(&mut output, &repo_id, &[held])
            .expect("naming an unproven workspace is not a failure");

        let (stdout, stderr) = output.into_inner();
        assert!(stdout.is_empty(), "stdout carries only rows");
        let stderr = String::from_utf8(stderr).expect("utf-8 warning");
        assert!(
            stderr.contains("cowshed: warning: acme/widget/detaching is not listed"),
            "{stderr:?}"
        );
        assert!(stderr.contains("no readable marker"), "{stderr:?}");
        assert!(
            stderr.ends_with(
                "next: cowshed doctor
"
            ),
            "{stderr:?}"
        );

        let mut none = Output::new(Vec::new(), Vec::new(), false);
        emit_unproven(&mut none, &repo_id, &[]).expect("no unproven workspace");
        let (stdout, stderr) = none.into_inner();
        assert!(stdout.is_empty() && stderr.is_empty());
    }

    /// `--json`, `--landing` and `--landed` have one precedence, and the project-scoped and
    /// store-wide listings answer to it identically.
    ///
    /// Two emitters is deliberate — the two JSON bodies are different frozen shapes and the tables
    /// carry different columns — but the order they consult the flags in is one user-visible
    /// contract, and it had two authors and no guard.
    #[tokio::test]
    async fn both_listings_resolve_the_output_flags_in_the_same_order() {
        use crate::args::ListArgs;

        async fn rendered(args: ListArgs, json: bool) -> (String, String) {
            let mut workspaces = Output::new(Vec::new(), Vec::new(), false);
            emit_workspace_listing(&mut workspaces, json, args, None, Vec::new())
                .expect("workspace listing");
            let mut projects = Output::new(Vec::new(), Vec::new(), false);
            emit_project_listing(&mut projects, json, args, Vec::new())
                .await
                .expect("project listing");
            (
                String::from_utf8(workspaces.into_inner().0).expect("utf-8"),
                String::from_utf8(projects.into_inner().0).expect("utf-8"),
            )
        }

        let every = ListArgs {
            all: true,
            landing: true,
            landed: true,
        };

        // JSON outranks both table forms, in both listings.
        let (workspaces, projects) = rendered(every, true).await;
        assert!(
            workspaces.starts_with("{\"ok\":true"),
            "--json outranks the table forms: {workspaces:?}"
        );
        assert!(
            projects.starts_with("{\"ok\":true"),
            "--json outranks the table forms: {projects:?}"
        );

        // Without JSON, --landing outranks --landed: the table, not bare names.
        let (workspaces, projects) = rendered(every, false).await;
        assert!(
            workspaces.contains("UNLANDED"),
            "--landing outranks --landed: {workspaces:?}"
        );
        assert!(
            projects.contains("UNLANDED"),
            "--landing outranks --landed: {projects:?}"
        );
        // Only the project listing carries the repository column.
        assert!(
            !workspaces.contains("REPOSITORY"),
            "a project-scoped listing has one repository: {workspaces:?}"
        );
        assert!(
            projects.contains("REPOSITORY"),
            "a store-wide listing has to name the repository: {projects:?}"
        );

        // --landed alone is bare names, so nothing is printed for an empty listing.
        let landed = ListArgs {
            all: true,
            landing: false,
            landed: true,
        };
        let (workspaces, projects) = rendered(landed, false).await;
        assert!(
            workspaces.is_empty(),
            "--landed alone prints bare names and nothing else: {workspaces:?}"
        );
        assert!(
            projects.is_empty(),
            "--landed alone prints bare names and nothing else: {projects:?}"
        );
    }

    #[derive(Clone, Default)]
    struct SharedWriter(Arc<Mutex<Vec<u8>>>);

    impl Write for SharedWriter {
        fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
            self.0
                .lock()
                .expect("writer lock")
                .extend_from_slice(buffer);
            Ok(buffer.len())
        }

        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    struct RefusingAdoptSetup {
        plan: HostSetupPlan,
        stderr: SharedWriter,
        saw_announcement_before_execute: Arc<AtomicBool>,
    }

    #[async_trait]
    impl AdoptHostSetup for RefusingAdoptSetup {
        async fn plan(&mut self) -> Result<HostSetupPlan> {
            Ok(self.plan.clone())
        }

        async fn execute(&mut self) -> Result<HostSetupReport> {
            let announced = String::from_utf8(self.stderr.0.lock().expect("writer lock").clone())
                .expect("utf8 announcement")
                .contains("will request administrator authorization");
            self.saw_announcement_before_execute
                .store(announced, Ordering::SeqCst);
            Err(CowshedError::sandbox_denied(
                "authorization declined",
                "cowshed setup",
            ))
        }
    }

    #[tokio::test]
    async fn adopt_announces_an_escalating_plan_before_authorization() {
        let stderr = SharedWriter::default();
        let observed = Arc::new(AtomicBool::new(false));
        let mut setup = RefusingAdoptSetup {
            plan: HostSetupPlan {
                actions: vec![HostAction::PinFstab {
                    uuid: "1111-2222".into(),
                    mount_at: PathBuf::from("/private/cowshed/store"),
                }],
                volumes: Vec::new(),
                requires_authorization: true,
                non_destructive: true,
            },
            stderr: stderr.clone(),
            saw_announcement_before_execute: Arc::clone(&observed),
        };
        let mut output = Output::new(Vec::new(), stderr, false);

        let error = prepare_adopt_host_storage(&mut setup, &mut output)
            .await
            .expect_err("authorization refusal");

        assert_eq!(error.code, ErrorCode::SandboxDenied);
        assert!(observed.load(Ordering::SeqCst));
    }

    #[test]
    fn setup_required_becomes_per_volume_findings_with_evidence() {
        let plan = HostSetupPlan {
            actions: vec![
                HostAction::ReclaimStubs {
                    paths: vec![
                        PathBuf::from("/private/cowshed/store/gateway.stderr.log"),
                        PathBuf::from("/private/cowshed/store/stale.sock"),
                    ],
                },
                HostAction::RepairMounted {
                    name: "cowshed.store".into(),
                    uuid: "STORE-UUID".into(),
                    size_bytes: 4096,
                    mounted_at: PathBuf::from("/Volumes/cowshed.store"),
                    mount_at: PathBuf::from("/private/cowshed/store"),
                },
                HostAction::MountExisting {
                    name: "cowshed.caches".into(),
                    uuid: "CACHE-UUID".into(),
                    size_bytes: 8192,
                    mount_at: PathBuf::from("/private/cowshed/caches"),
                },
                HostAction::InstallMountService {
                    label: "dev.cowshed.storage".into(),
                },
            ],
            volumes: vec![
                planned_volume(
                    "cowshed.store",
                    VolumeRole::Store,
                    VolumeState::MisMounted {
                        mounted_at: PathBuf::from("/Volumes/cowshed.store"),
                    },
                ),
                planned_volume("cowshed.caches", VolumeRole::Caches, VolumeState::Detached),
            ],
            requires_authorization: true,
            non_destructive: true,
        };

        let findings = host_storage_findings(&plan);

        assert!(findings.iter().any(|finding| {
            finding.code == "mount"
                && finding.message.contains("STORE-UUID")
                && finding.message.contains("/Volumes/cowshed.store")
                && finding.message.contains("/private/cowshed/store")
                && finding.hint == "cowshed setup"
        }));
        assert!(findings.iter().any(|finding| {
            finding.code == "mount"
                && finding.message.contains("CACHE-UUID")
                && finding
                    .message
                    .contains("marker unavailable while detached")
        }));
        assert!(findings.iter().any(|finding| {
            finding.code == "mount-stubs"
                && finding.message.contains("gateway.stderr.log")
                && finding.message.contains("stale.sock")
        }));
        assert!(findings.iter().any(|finding| {
            finding.code == "host-mount-service"
                && finding.message.contains("dev.cowshed.storage")
                && finding.hint == "cowshed setup"
        }));
        assert!(findings.iter().all(|finding| !finding.message.is_empty()));
    }

    /// A mis-mounted volume is the case the distinction exists for: the message has to name where
    /// the bytes actually are, because that is what looks wrong, while the JSON `path` carries the
    /// canonical root a repair works towards. Rendering the canonical root as the observed
    /// mountpoint would tell an operator their store is somewhere it is not.
    #[test]
    fn a_mis_mounted_volume_reports_the_observed_mountpoint_and_expects_the_canonical_root() {
        let observed = PathBuf::from("/Volumes/cowshed.store");
        let plan = HostSetupPlan {
            actions: vec![HostAction::RepairMounted {
                name: "cowshed.store".into(),
                uuid: "STORE-UUID".into(),
                size_bytes: 4096,
                mounted_at: observed.clone(),
                mount_at: PathBuf::from(STORE_ROOT),
            }],
            volumes: vec![planned_volume(
                "cowshed.store",
                VolumeRole::Store,
                VolumeState::MisMounted {
                    mounted_at: observed.clone(),
                },
            )],
            requires_authorization: true,
            non_destructive: true,
        };

        let findings = host_storage_findings(&plan);
        let store = findings
            .iter()
            .find(|finding| finding.message.starts_with("cowshed.store:"))
            .expect("a finding for the store");

        assert_eq!(store.code, "mount");
        assert_eq!(store.path.as_deref(), Some(Path::new(STORE_ROOT)));
        assert!(
            store
                .message
                .contains(&format!("mounted at {}", observed.display())),
            "{}",
            store.message
        );
        assert!(
            store.message.contains(&format!("expected {STORE_ROOT}")),
            "{}",
            store.message
        );
    }

    /// A store found in a foreign container is the state the planner-classification hand-off
    /// exists for: planned actions render it identically to a merely detached volume, so a doctor
    /// reading actions would hide the one fact — whose container holds the bytes — that decides
    /// whether `cowshed setup` is safe to run blind.
    #[test]
    fn a_volume_in_a_foreign_container_is_named_with_its_container_and_device() {
        let plan = HostSetupPlan {
            actions: vec![HostAction::MountExisting {
                name: "cowshed.store".into(),
                uuid: "STORE-UUID".into(),
                size_bytes: 4096,
                mount_at: PathBuf::from(STORE_ROOT),
            }],
            volumes: vec![planned_volume(
                "cowshed.store",
                VolumeRole::Store,
                VolumeState::FoundElsewhere {
                    container: "disk4".into(),
                    device: "disk4s7".into(),
                    mounted_at: None,
                },
            )],
            requires_authorization: true,
            non_destructive: true,
        };

        let findings = host_storage_findings(&plan);
        let store = findings
            .iter()
            .find(|finding| finding.message.starts_with("cowshed.store:"))
            .expect("a finding for the store");

        assert_eq!(store.code, "mount");
        assert_eq!(store.severity, FindingSeverity::Error);
        assert!(
            store.message.contains("container disk4"),
            "{}",
            store.message
        );
        assert!(
            store.message.contains("device disk4s7"),
            "{}",
            store.message
        );
        assert!(
            store.message.contains("data is safe on disk4s7"),
            "{}",
            store.message
        );
        assert!(
            store.message.contains(&format!("expected {STORE_ROOT}")),
            "{}",
            store.message
        );
    }

    /// The expected roots are core's, not a second copy in the CLI: a copy can disagree with the
    /// volume the planner actually looked at, and then the finding describes a host nobody has.
    #[test]
    fn expected_volume_roots_come_from_the_canonical_constants() {
        let findings = host_storage_findings(&HostSetupPlan {
            actions: Vec::new(),
            volumes: vec![
                planned_volume(
                    "cowshed.store",
                    VolumeRole::Store,
                    VolumeState::MountedValid,
                ),
                planned_volume(
                    "cowshed.caches",
                    VolumeRole::Caches,
                    VolumeState::MountedValid,
                ),
            ],
            requires_authorization: false,
            non_destructive: true,
        });

        assert_eq!(
            findings
                .iter()
                .map(|finding| finding.path.clone().expect("a path per volume"))
                .collect::<Vec<_>>(),
            vec![
                PathBuf::from(STORE_ROOT),
                PathBuf::from(RETIRED_CACHES_MOUNTPOINT)
            ]
        );
        assert_eq!(
            CanonicalRoots::global().store(),
            Path::new(STORE_ROOT),
            "the canonical roots and the doctor's expected roots must be one definition"
        );
        for finding in &findings {
            assert_eq!(finding.code, "host-volume");
            let expected = finding.path.as_deref().expect("a path per volume");
            assert!(
                finding
                    .message
                    .contains(&format!("mounted at {}", expected.display())),
                "{}",
                finding.message
            );
        }
    }

    #[test]
    fn setup_required_report_emits_json_findings_and_environment_exit() {
        let plan = HostSetupPlan {
            actions: vec![HostAction::CreateVolume {
                name: "cowshed.store".into(),
                container: "disk3".into(),
                mount_at: PathBuf::from("/private/cowshed/store"),
            }],
            volumes: vec![planned_volume(
                "cowshed.store",
                VolumeRole::Store,
                VolumeState::Absent,
            )],
            requires_authorization: true,
            non_destructive: false,
        };
        let mut output = Output::new(Vec::new(), Vec::new(), false);

        let exit = emit_doctor_report(
            &mut output,
            true,
            doctor_report(host_storage_findings(&plan)),
        )
        .expect("doctor emits");

        assert_eq!(exit.code, 5);
        let (stdout, stderr) = output.into_inner();
        assert!(stderr.is_empty());
        let envelope: serde_json::Value =
            serde_json::from_slice(&stdout).expect("doctor JSON envelope");
        assert_eq!(envelope["ok"], true);
        assert_eq!(envelope["result"]["healthy"], false);
        assert!(
            envelope["result"]["findings"]
                .as_array()
                .expect("findings")
                .iter()
                .any(|finding| finding["code"] == "host-volume-absent"
                    && finding["hint"] == "cowshed setup"
                    && finding["message"]
                        .as_str()
                        .is_some_and(|message| message.contains("disk3")))
        );
    }

    /// Two builds of one release report the same version, so a daemon left running an older
    /// build is only visible by its bytes; a plain restart would keep the installed copy.
    #[test]
    fn a_daemon_running_other_bytes_is_a_stale_binary_with_the_purge_remedy() {
        let status = GatewayStatus {
            installed: true,
            running: true,
            socket: PathBuf::from("/private/cowshed/store/gateway.sock"),
            cli_version: "0.1.0".into(),
            daemon_version: Some("0.1.0".into()),
            active_workspaces: 0,
            drain_cause: None,
            healing: None,
            recovering: None,
            stale_daemon: Some(cowshed_core::api::StaleDaemonBinary {
                daemon_sha256: Some("13f1eec0".into()),
                cli_sha256: "b4223cd0".into(),
            }),
            cpu_budget: None,
            pending_install: false,
            land_turns: Vec::new(),
            land_turns_error: None,
        };

        let findings = gateway_findings(&status);
        let stale = findings
            .iter()
            .find(|finding| finding.code == "gateway-stale-binary")
            .expect("stale binary finding");
        assert_eq!(stale.severity, FindingSeverity::Warning);
        assert_eq!(stale.hint, "cowshed setup");
        assert!(stale.message.contains("13f1eec0") && stale.message.contains("b4223cd0"));
    }

    /// A draining daemon still answers its control socket, but it refuses every session: doctor
    /// must say so as an error rather than list it as a healthy gateway.
    #[test]
    fn a_draining_gateway_is_an_error_naming_its_cause() {
        let findings = gateway_findings(&GatewayStatus {
            installed: true,
            running: true,
            socket: PathBuf::from("/private/cowshed/store/gateway.sock"),
            cli_version: "0.1.0".into(),
            daemon_version: Some("0.1.0".into()),
            active_workspaces: 0,
            drain_cause: Some("audit sink failed, so the gateway fails closed: disk full".into()),
            healing: None,
            recovering: None,
            stale_daemon: None,
            cpu_budget: None,
            pending_install: false,
            land_turns: Vec::new(),
            land_turns_error: None,
        });
        let draining = findings
            .iter()
            .find(|finding| finding.code == "gateway-draining")
            .expect("draining finding");
        assert_eq!(draining.severity, FindingSeverity::Error);
        assert!(draining.message.contains("disk full"));
    }

    #[test]
    fn service_findings_name_launchd_socket_and_recovery() {
        let gateway = gateway_findings(&GatewayStatus {
            installed: true,
            running: false,
            socket: PathBuf::from("/private/cowshed/store/gateway.sock"),
            cli_version: "2.0.0".into(),
            daemon_version: None,
            active_workspaces: 0,
            drain_cause: None,
            healing: None,
            recovering: None,
            stale_daemon: None,
            cpu_budget: None,
            pending_install: false,
            land_turns: Vec::new(),
            land_turns_error: None,
        });
        assert_eq!(gateway[0].code, "gateway-down");
        assert_eq!(gateway[0].severity, FindingSeverity::Error);
        assert!(gateway[0].message.contains("launchd loaded"));
        assert!(gateway[0].message.contains("gateway.sock"));
        assert_eq!(
            gateway[0].hint,
            "cowshed gateway stop && cowshed gateway start"
        );

        let sccache = sccache_finding(&SccacheStatus {
            installed: true,
            running: false,
            socket: PathBuf::from("/private/cowshed/store/sccache.sock"),
            stats: None,
        });
        assert_eq!(sccache.code, "sccache-down");
        assert_eq!(sccache.severity, FindingSeverity::Warning);
        assert!(sccache.message.contains("launchd loaded"));
        assert!(sccache.message.contains("sccache.sock"));
        assert_eq!(
            sccache.hint,
            "cowshed sccache stop && cowshed sccache start"
        );
    }

    #[test]
    fn durations_are_exact_and_checked() {
        assert_eq!(
            parse_duration("500ms".into()).unwrap(),
            Duration::from_millis(500)
        );
        assert_eq!(
            parse_duration("2m".into()).unwrap(),
            Duration::from_secs(120)
        );
        assert!(parse_duration("1.5s".into()).is_err());
        assert!(parse_duration("9d".into()).is_err());
    }

    #[test]
    fn only_adopt_receives_provisioning_authority() {
        let cases = [
            (vec!["adopt", "/repo"], RuntimeOpenMode::Provision),
            (vec!["mv", "raven", "falcon"], RuntimeOpenMode::ExistingOnly),
            (
                // The rebind verb must open even when the remote already names the new identity.
                vec!["mv", "main", "--repo-id", "other/widget"],
                RuntimeOpenMode::IdentityChange,
            ),
            (vec!["fork", "raven"], RuntimeOpenMode::ExistingOnly),
            (
                vec!["fork", "raven", "falcon"],
                RuntimeOpenMode::ExistingOnly,
            ),
            (vec!["checkpoint", "raven"], RuntimeOpenMode::ExistingOnly),
            (
                vec!["restore", "raven", "stable"],
                RuntimeOpenMode::ExistingOnly,
            ),
            (vec!["attach", "--all"], RuntimeOpenMode::ExistingOnly),
            (vec!["ls"], RuntimeOpenMode::ExistingOnly),
            (vec!["path", "raven"], RuntimeOpenMode::ExistingOnly),
            (
                vec!["exec", "raven", "--", "true"],
                RuntimeOpenMode::ExistingOnly,
            ),
            (vec!["rm", "raven"], RuntimeOpenMode::ExistingOnly),
            (
                vec!["forget", "acme/widget", "--abandon"],
                RuntimeOpenMode::ExistingOnly,
            ),
            (vec!["attach", "raven"], RuntimeOpenMode::ExistingOnly),
            (vec!["detach", "raven"], RuntimeOpenMode::ExistingOnly),
            (vec!["gc"], RuntimeOpenMode::ExistingOnly),
            (vec!["push", "raven"], RuntimeOpenMode::ExistingOnly),
            (vec!["rebase", "raven"], RuntimeOpenMode::ExistingOnly),
            (vec!["land", "raven"], RuntimeOpenMode::ExistingOnly),
            (vec!["doctor"], RuntimeOpenMode::ExistingOnly),
        ];

        for (arguments, expected) in cases {
            let parsed = crate::args::parse_args(arguments).unwrap();
            assert_eq!(runtime_open_mode(&parsed.command), expected);
        }
    }

    #[test]
    fn optional_context_falls_back_only_when_an_adopted_checkout_is_absent() {
        for code in [ErrorCode::EnvironmentMissing, ErrorCode::NotFound] {
            assert!(optional_project_unavailable(&CowshedError::new(
                code, "missing", "old hint"
            )));
        }
        for code in [
            ErrorCode::Internal,
            ErrorCode::Usage,
            ErrorCode::Conflict,
            ErrorCode::SandboxDenied,
            ErrorCode::Integrity,
        ] {
            assert!(!optional_project_unavailable(&CowshedError::new(
                code,
                "real failure",
                "repair it"
            )));
        }
    }

    #[test]
    fn skipped_project_checks_are_an_explicit_unhealthy_finding() {
        let mismatch = CowshedError::conflict(
            "repository binding remote codebase does not match Git configuration",
            "restore the recorded remote before opening cowshed",
        );
        let mut output = Output::new(Vec::new(), Vec::new(), false);
        let mut findings = Vec::new();

        record_project_checks_skipped(
            &mut output,
            &mut findings,
            Some(&mismatch),
            Some(PathBuf::from("/repo")),
        )
        .expect("skip finding");
        let report = doctor_report(findings);

        assert!(!report.healthy);
        assert_eq!(report.findings.len(), 1);
        assert_eq!(report.findings[0].code, "project-checks-skipped");
        assert_eq!(report.findings[0].severity, FindingSeverity::Error);
        let (_, stderr) = output.into_inner();
        assert_eq!(
            stderr,
            b"project checks skipped: repository binding remote codebase does not match Git configuration\n"
        );
    }

    #[test]
    fn project_discovery_failure_points_to_inventory_and_explicit_context() {
        let original = CowshedError::environment_missing(
            "/tmp is not inside a standalone git repository",
            "cowshed adopt <git-root>",
        );
        let mapped = project_context_error(original);
        assert_eq!(
            mapped.hint,
            "cowshed ls; cowshed --project <git-root> <command>"
        );
        assert_eq!(mapped.code, ErrorCode::EnvironmentMissing);
        assert_eq!(
            mapped.message,
            "/tmp is not inside a standalone git repository"
        );
    }

    #[test]
    fn adopt_runtime_open_receives_the_parsed_repository_identity() {
        let parsed = crate::args::parse_args(["adopt", "/repo", "--repo-id", "acme/widget"])
            .expect("adopt arguments");
        assert_eq!(
            runtime_open_repo_id(&parsed.command).expect("runtime open identity"),
            Some(RepoId::parse("acme/widget").expect("repository identity"))
        );

        let parsed = crate::args::parse_args(["adopt", "/repo"]).expect("adopt arguments");
        assert_eq!(
            runtime_open_repo_id(&parsed.command).expect("optional runtime open identity"),
            None
        );
    }

    /// A verb's open finishes only the unfinished lifecycle work of the workspaces it names:
    /// `cowshed land a` must neither wait on nor fail with workspace b's deferred clone.
    #[test]
    fn each_verb_recovers_only_the_workspaces_it_names() {
        let scope = |argv: &[&str]| {
            let parsed = crate::args::parse_args(argv.iter().copied()).expect("arguments");
            runtime_recovery_scope(&parsed.command)
        };
        let names = |values: &[&str]| {
            RecoveryScope::Workspaces(
                values
                    .iter()
                    .map(|value| WorkspaceName::new(*value).expect("workspace name"))
                    .collect(),
            )
        };

        assert_eq!(
            scope(&["land", "coord-hygiene"]).unwrap(),
            names(&["coord-hygiene"])
        );
        assert_eq!(scope(&["fork", "a", "b"]).unwrap(), names(&["a", "b"]));
        assert_eq!(scope(&["mv", "a", "b"]).unwrap(), names(&["a", "b"]));
        assert_eq!(scope(&["ls"]).unwrap(), names(&[]));
        // A retirement names a project, never a workspace, and opens no project runtime at all.
        assert_eq!(
            scope(&["forget", "acme/widget", "--abandon"]).unwrap(),
            names(&[])
        );
        assert_eq!(scope(&["gc"]).unwrap(), RecoveryScope::Store);
        // doctor inspects: its open finishes nothing, not even main's unfinished work, and
        // reclaims nothing. Only an explicit repair opens the way every other verb does.
        assert_eq!(scope(&["doctor"]).unwrap(), RecoveryScope::Inspect);
        assert_eq!(scope(&["doctor", "--repair"]).unwrap(), names(&[]));
        for argv in [
            &["reseed", "raven"][..],
            &["reseed", "raven", "--from", "lane", "--dry-run"],
            &["defrag", "raven"],
            &["land", "raven", "--preflight"],
            &["land", "raven", "--into", "lane", "--preflight"],
        ] {
            assert_eq!(scope(argv).unwrap(), RecoveryScope::Inspect);
        }
        assert_eq!(
            scope(&["rm", "gone"]).unwrap(),
            RecoveryScope::Removal(WorkspaceName::new("gone").expect("workspace name"))
        );
        let invalid = runtime_recovery_scope(&Command::Remove(crate::args::RemoveArgs {
            workspace: "not/a/name".into(),
            force: false,
            restore: false,
            abandon: false,
            purge: false,
        }))
        .expect_err("a removal's scope is its name");
        assert_eq!(invalid.code, ErrorCode::Usage);
    }

    #[test]
    fn teardown_error_preserves_primary_taxonomy_and_both_messages() {
        let primary = CowshedError::not_found("missing", "cowshed adopt");
        let teardown = CowshedError::internal("shutdown failed");
        let merged = merge_primary(primary, Some(teardown));
        assert_eq!(merged.code, ErrorCode::NotFound);
        assert!(merged.message.contains("missing"));
        assert!(merged.message.contains("shutdown failed"));
    }

    #[tokio::test]
    async fn real_unix_controller_connection_shuts_down_every_actor() {
        use cowshed_core::api::server::RouterHandle;
        use cowshed_core::repository::RepoId;
        use std::num::NonZeroUsize;

        let repo_id = RepoId::parse("acme/widget").unwrap();
        let (router, mut receiver) = RouterHandle::channel(NonZeroUsize::new(4).unwrap());
        let router_actor = tokio::spawn(async move {
            while let Some(command) = receiver.recv().await {
                let (_, reply) = command.into_parts();
                let _ = reply.send(Err(CowshedError::internal(
                    "handshake-only test routed an unexpected request",
                )));
            }
        });
        let (client, server) = std::os::unix::net::UnixStream::pair().unwrap();
        let connection = tokio::spawn(serve_controller_connection(
            server.into(),
            ConnectionAuthority::Coordinator { repo_id },
            router.clone(),
        ));
        let (cowshed, token) = cowshed_core::Cowshed::connect(client.into()).await.unwrap();
        drop(token);
        drop(cowshed);
        connection.await.unwrap().unwrap();
        drop(router);
        router_actor.await.unwrap();
    }

    /// An unmounted main is a critical finding: it names both paths, carries a remedy, and makes
    /// the report unhealthy. Mains are always-mounted (02_workspaces.md), so `healthy: true` over
    /// a checkout the user cannot open would be the one verdict doctor must never give.
    #[test]
    fn an_unmounted_main_is_a_critical_doctor_finding() {
        use cowshed_core::repository::RepoId;

        let main = UnreachableMain {
            repo_id: RepoId::parse("acme/widget").expect("repo"),
            image: PathBuf::from("/private/cowshed/store/acme/widget/main.dmg"),
            mountpoint: PathBuf::from("/Users/dev/src/widget"),
            reason: String::from("main's volume is not mounted"),
        };

        let finding = main_mount_finding(&main);

        assert_eq!(finding.code, "main-not-mounted");
        assert_eq!(finding.severity, FindingSeverity::Error);
        assert_eq!(
            finding.message,
            "acme/widget: main is not mounted at /Users/dev/src/widget \
             (image /private/cowshed/store/acme/widget/main.dmg): main's volume is not mounted"
        );
        assert_eq!(finding.hint, "cowshed gateway start");
        assert_eq!(finding.path.as_deref(), Some(main.mountpoint.as_path()));
        let report = doctor_report(vec![finding]);
        assert!(
            !report.healthy,
            "a project whose checkout is not mounted is not a healthy host"
        );
    }

    /// Detached metadata that still records a path under the retired layout is a critical finding.
    /// The record's own sentence and metadata path travel unchanged, the hint is the audit step,
    /// and no setup option is named: the root is fixed, so there is none to run.
    #[test]
    fn retired_mount_metadata_is_a_critical_host_doctor_finding() {
        let record = RetiredLayoutRecord {
            metadata_path: PathBuf::from(
                "/private/cowshed/store/acme/widget/sessions/raven.dmg.grants.json",
            ),
            recorded_path: PathBuf::from("/private/cowshed/store/mnt/acme/widget/raven"),
        };

        let finding = retired_layout_finding(record.clone());

        assert_eq!(finding.code, "retired-mount-layout");
        assert_eq!(finding.severity, FindingSeverity::Error);
        assert_eq!(finding.message, record.doctor_message());
        assert!(
            finding
                .message
                .contains("/private/cowshed/store/mnt/acme/widget/raven")
        );
        assert_eq!(finding.hint, RETIRED_LAYOUT_HINT);
        assert!(!finding.message.contains("--mount-root"));
        assert!(!finding.hint.contains("--mount-root"));
        assert_eq!(
            finding.path.as_deref(),
            Some(record.metadata_path.as_path())
        );
        assert!(!doctor_report(vec![finding]).healthy);
    }

    #[test]
    fn a_gateway_that_accepted_and_never_answered_is_an_unhealthy_finding() {
        let unanswered = gateway_service::Unanswered {
            peer: "the gateway daemon",
            socket: PathBuf::from("/private/cowshed/store/gateway.sock"),
            within: DOCTOR_PEER_ANSWER,
        };
        let finding = unanswered_finding(
            "gateway-unanswered",
            FindingSeverity::Error,
            &unanswered,
            GATEWAY_UNANSWERED_HINT,
        );
        assert_eq!(finding.code, "gateway-unanswered");
        assert_eq!(
            finding.message,
            "the gateway daemon accepted doctor's request at /private/cowshed/store/gateway.sock and gave no answer in 60s"
        );
        assert!(finding.hint.contains("cowshed gateway status"));
        assert!(!doctor_report(vec![finding]).healthy);
    }

    #[test]
    fn unread_retired_layout_scan_does_not_hint_re_running_doctor() {
        let findings = retired_mount_layout_findings(Path::new("/dev/null"));
        assert_eq!(findings.len(), 1);
        let finding = &findings[0];
        assert_eq!(finding.code, "retired-mount-layout-scan");
        assert_eq!(finding.severity, FindingSeverity::Error);
        assert!(
            !finding.hint.contains("cowshed doctor"),
            "re-running doctor cannot repair unread metadata; hint was {}",
            finding.hint
        );
        assert!(finding.hint.contains("inspect the store"));
        assert!(!doctor_report(findings).healthy);
    }

    #[test]
    fn store_readdir_resolves_a_session_in_another_project_without_cwd() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-detach-store-readdir-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = root.join("store");
        let other_checkout = PathBuf::from("/other/project");
        write_session_image(&store, "acme", "widget", "fox", Path::new("/cwd/project"));
        write_session_image(&store, "zeta", "tool", "raven", &other_checkout);

        let resolved =
            resolve_session_project_root(&store, "raven", "detach").expect("found raven");
        assert_eq!(resolved, other_checkout);

        let missing = resolve_session_project_root(&store, "absent", "detach").unwrap_err();
        assert_eq!(missing.code, ErrorCode::NotFound);

        write_session_image(&store, "acme", "widget", "raven", Path::new("/cwd/project"));
        let conflict = resolve_session_project_root(&store, "raven", "detach").unwrap_err();
        assert_eq!(conflict.code, ErrorCode::Conflict);
        assert!(conflict.message.contains("zeta/tool"));
        assert!(conflict.message.contains("acme/widget"));

        std::fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn rm_outside_any_project_addresses_a_session_by_its_unique_name_in_the_store() {
        let root = std::env::temp_dir().join(format!(
            "cowshed-rm-store-readdir-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = root.join("store");
        let checkout = PathBuf::from("/fixtures/snapshots/project");
        write_session_image(&store, "fixtures", "snapshots", "fixture", &checkout);
        let discovery = || {
            CowshedError::environment_missing(
                "/Users/dev is not inside a standalone git repository",
                "cowshed adopt <git-root>",
            )
        };

        let resolved = named_removal_root(&store, "fixture", discovery()).expect("found fixture");
        assert_eq!(resolved, checkout);

        let missing = named_removal_root(&store, "absent", discovery()).unwrap_err();
        assert_eq!(missing.code, ErrorCode::NotFound);
        assert_eq!(
            missing.message,
            "/Users/dev is not inside a standalone git repository; no session workspace absent in the store"
        );
        assert_eq!(
            missing.hint,
            "cowshed ls --all; cowshed --project <git-root> rm <ws>"
        );

        write_session_image(&store, "fixtures", "retirement", "fixture", &checkout);
        let conflict = named_removal_root(&store, "fixture", discovery()).unwrap_err();
        assert_eq!(conflict.code, ErrorCode::Conflict);
        assert!(conflict.message.contains("fixtures/snapshots"));
        assert!(conflict.message.contains("fixtures/retirement"));
        assert_eq!(conflict.hint, "cowshed --project <git-root> rm <ws>");

        std::fs::remove_dir_all(root).unwrap();
    }

    fn write_session_image(
        store: &Path,
        owner: &str,
        repo: &str,
        workspace: &str,
        project_root: &Path,
    ) {
        let sessions = store.join(owner).join(repo).join("sessions");
        std::fs::create_dir_all(&sessions).unwrap();
        cowshed_core::storage::bootstrap::write_owner_marker(&store.join(owner)).unwrap();
        let image = sessions.join(format!("{workspace}.dmg"));
        std::fs::write(&image, b"image").unwrap();
        let mut sidecar = serde_json::json!({
            "version": 1,
            "repoId": format!("{owner}/{repo}"),
            "workspace": workspace,
            "workspaceIncarnation": "0198f2c0b7e34dc795f17b238b331c80",
            "publicationState": "active",
            "platform": "macos",
            "updatedAt": "2026-07-11T12:34:56Z",
            "revision": 1,
            "portBlock": { "base": 40976, "size": 16 },
            "infoSnapshot": {
                "projectRoot": project_root,
                "role": "workspace",
                "baseCommit": "8f31c2d",
                "createdAt": "2026-07-11T12:00:00Z",
                "capturedAt": "2026-07-11T12:34:00Z",
                "stale": false
            }
        });
        if !cfg!(target_os = "macos") {
            sidecar["platform"] = serde_json::json!("linux");
            sidecar.as_object_mut().unwrap().remove("portBlock");
        }
        std::fs::write(
            format!("{}.grants.json", image.display()),
            serde_json::to_vec(&sidecar).unwrap(),
        )
        .unwrap();
    }

    #[test]
    fn doctor_prints_status_then_findings_then_unique_hints() {
        let mut output = Output::new(Vec::new(), Vec::new(), false);
        emit_doctor(
            &mut output,
            &DoctorReport {
                healthy: false,
                findings: vec![
                    Finding {
                        code: "mount".into(),
                        severity: FindingSeverity::Error,
                        message: "cowshed.store: present, not mounted".into(),
                        hint: "cowshed setup".into(),
                        path: None,
                    },
                    Finding {
                        code: "mount".into(),
                        severity: FindingSeverity::Error,
                        message: "cowshed.caches: present, not mounted".into(),
                        hint: "cowshed setup".into(),
                        path: None,
                    },
                    Finding {
                        code: "gateway-down".into(),
                        severity: FindingSeverity::Error,
                        message: "gateway: launchd loaded; control socket does not answer".into(),
                        hint: "cowshed gateway stop && cowshed gateway start".into(),
                        path: None,
                    },
                ],
            },
        )
        .unwrap();
        let (stdout, stderr) = output.into_inner();
        assert_eq!(stdout, b"unhealthy\n");
        assert_eq!(
            String::from_utf8(stderr).unwrap(),
            "cowshed: [error mount] cowshed.store: present, not mounted\n\
             cowshed: [error mount] cowshed.caches: present, not mounted\n\
             cowshed: [error gateway-down] gateway: launchd loaded; control socket does not answer\n\
             next: cowshed setup\n\
             next: cowshed gateway stop && cowshed gateway start\n"
        );
    }

    /// A removal report read back from an earlier build's journal record is history, not a clean
    /// result: the warning names the head no ref keeps, the bundle the record names and each
    /// branch whose measurement named no files. Read from the records the core's journal test
    /// captured, so the warning answers the shapes an earlier build actually wrote.
    #[test]
    fn an_abandonment_an_earlier_build_recorded_warns_once_and_a_complete_one_does_not() {
        let journal: serde_json::Value = serde_json::from_str(include_str!(
            "../../cowshed-core/tests/fixtures/lifecycle-intents-before-preserved-refs.json"
        ))
        .expect("captured journal");
        let abandoned = |name: &str| -> AbandonedWork {
            serde_json::from_value(
                journal["entries"][name]["completion"]["result"]["abandoned"].clone(),
            )
            .expect("an earlier build's abandonment reads")
        };

        let plain = abandoned("accounting-process-fence-proof");
        let warning = predating_warning("accounting-process-fence-proof", &plain)
            .expect("a predating record warns");
        assert!(warning.contains(plain.head.as_str()), "{warning}");
        assert!(
            warning.contains(&plain.bundle.display().to_string()),
            "{warning}"
        );
        assert!(!warning.contains("branch measurements"), "{warning}");

        let branched = abandoned("nx-cache-advisor");
        let warning =
            predating_warning("nx-cache-advisor", &branched).expect("a predating record warns");
        assert!(
            warning.contains("predate named files")
                && warning.contains(&branched.branches[0].branch),
            "{warning}"
        );

        let kept = AbandonedWork {
            preserved: AbandonedPreservation::Preserved { refs: Vec::new() },
            branches: Vec::new(),
            ..plain.clone()
        };
        assert_eq!(predating_warning("raven", &kept), None);
        let main = AbandonedWork {
            preserved: AbandonedPreservation::NothingToPreserve,
            ..plain
        };
        assert_eq!(predating_warning("main", &main), None);
    }
}

#[cfg(test)]
mod host_gate_environment_tests {

    /// Exercise the real checkout's entry hooks with the operator environment the CLI carries,
    /// without treating the record projection as an execution allowlist.
    #[test]
    #[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
    fn host_controller_operator_environment_activates_the_checkout() {
        let forwarded = host_gate_environment(ExecScope::HostGate, std::env::vars_os())
            .expect("the operator's environment");
        let package = std::path::PathBuf::from(
            std::env::var_os("CARGO_MANIFEST_DIR").expect("runtime package root"),
        );
        let root = package.ancestors().nth(4).expect("workspace root");
        let activated = std::process::Command::new("direnv")
            .env_clear().envs(&forwarded).current_dir(root)
            .args(["exec", ".", "/bin/sh", "-c", "command -v cargo && command -v bun && command -v rustc && bun --version && rustc --version"])
            .output().expect("run the real host activation");
        let mut names: Vec<&str> = forwarded.keys().map(String::as_str).collect();
        names.sort_unstable();
        cowshed_os::eprintln!("HostGate activation forwarded names: {}", names.join(", "));
        cowshed_os::eprintln!("{}", String::from_utf8_lossy(&activated.stdout));
        assert!(
            activated.status.success(),
            "activation failed: {}",
            String::from_utf8_lossy(&activated.stderr)
        );
    }
    use super::host_gate_environment;
    use cowshed_core::api::ExecScope;
    use std::ffi::OsString;

    fn ambient(pairs: &[(&str, &str)]) -> Vec<(OsString, OsString)> {
        pairs
            .iter()
            .map(|(name, value)| (OsString::from(name), OsString::from(value)))
            .collect()
    }

    /// Custom operator/project variables travel in the normal HostGate request; the CLI does
    /// not duplicate the supervisor's runtime filters. Workspace forwarding remains unchanged.
    #[test]
    fn a_host_gate_preserves_custom_ambient_names_and_workspace_forwards_nothing() {
        let operator = ambient(&[
            ("PATH", "/nix/profile/bin:/usr/bin"),
            ("HOME", "/Users/operator"),
            ("LC_ALL", "C"),
            ("PROJECT_SETTING", "custom-value"),
            ("CI", "true"),
            ("NX_DAEMON", "false"),
            ("NODE_OPTIONS", "--max-old-space-size=4096"),
            ("SSL_CERT_FILE", "/etc/ssl/cert.pem"),
            ("SDKROOT", "/operator/sdk"),
        ]);
        let forwarded = host_gate_environment(ExecScope::HostGate, operator.iter().cloned())
            .expect("a host gate environment");
        let expected: std::collections::HashMap<String, String> = operator
            .iter()
            .map(|(name, value)| {
                (
                    name.to_str().unwrap().to_owned(),
                    value.to_str().unwrap().to_owned(),
                )
            })
            .collect();
        assert_eq!(forwarded, expected);
        assert!(
            host_gate_environment(ExecScope::Workspace, operator.into_iter())
                .expect("an ordinary environment")
                .is_empty()
        );
    }

    /// A custom variable whose value the request cannot carry is refused by name, not dropped.
    #[cfg(unix)]
    #[test]
    fn a_custom_value_that_is_not_utf8_is_refused_by_name() {
        use std::os::unix::ffi::OsStringExt;
        let operator = vec![(
            OsString::from("PROJECT_SETTING"),
            OsString::from_vec(vec![0xff, b'/']),
        )];
        let error = host_gate_environment(ExecScope::HostGate, operator.into_iter())
            .expect_err("a non-UTF-8 custom value cannot travel");
        assert!(
            error.message.contains("PROJECT_SETTING"),
            "{}",
            error.message
        );
    }

    /// The public-name record helper does not determine which names the CLI may execute with.
    #[test]
    fn the_record_projection_is_not_a_host_gate_execution_allowlist() {
        let operator = ambient(&[
            ("PATH", "/usr/bin"),
            ("LC_CTYPE", "C"),
            ("PROJECT_SETTING", "custom-value"),
            ("SSL_CERT_FILE", "/etc/ssl/cert.pem"),
        ]);
        let forwarded = host_gate_environment(ExecScope::HostGate, operator.into_iter())
            .expect("a host gate environment");
        assert!(cowshed_core::api::host_gate_env_name_allowed("PATH"));
        assert!(cowshed_core::api::host_gate_env_name_allowed("LC_CTYPE"));
        for name in ["PROJECT_SETTING", "SSL_CERT_FILE"] {
            assert!(!cowshed_core::api::host_gate_env_name_allowed(name));
            assert!(
                forwarded.contains_key(name),
                "{name} travels in the request"
            );
        }
    }
}
