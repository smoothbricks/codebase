use crate::agent_cycle::{self, Bootout, Held, LoadedAgent, Reload, Remains};
use crate::args::GatewayCommand;
use crate::build_record::{BuildRecord, unproven_refusal, unrecorded_refusal};
use crate::code_signing::{self, Keychain, SigningIdentity};
use crate::install_basis::{self, InstallSlot};
use crate::land_turn::local_clock;
use crate::launchd::{
    BunDirectory, CARGO_LINK_NAME, COWSHED_BINARY_NAME, DevenvProfile, ExecutableDigest,
    ExecutableInstallState, ExistingPlist, HostStableExecutable, InstallOutcome, InstallState,
    InstalledExecutable, LaunchAgentSpec, LaunchAgentTarget, LaunchctlCommand, LaunchdExecutor,
    LaunchdFilesystem, LaunchdServiceStatus, NativeFilesystem, NativeLaunchctlCommand,
    PRIVATE_DIRECTORY_MODE, RemovalOutcome, StableLink, kickstart_hint, plan_cargo_link_removal,
    plan_executable_install, plan_executable_remove, plan_gateway_host_install, plan_install,
    plan_remove, replace_symlink,
};
use crate::output::Output;
use crate::path_entry::PathEntryOutcome;
use async_trait::async_trait;
use cowshed_core::api::Coordinator;
use cowshed_core::api::{
    EmptyResult, ExecClass, GatewayStatus as CliGatewayStatus, HostJobs, HostScheduling,
    ImageCleanup, JobCount, JobState, LandTurnQueue, StaleDaemonBinary, UnansweredSupervisor,
};
use cowshed_core::host_dirs::support_directory;
use cowshed_core::land_turn as turns;
use cowshed_core::metadata::{NEW_PORT_BLOCK_SIZE, PortBlock};
use cowshed_core::project_retirement::ProjectRetirementReport;
use cowshed_core::runtime::supervisor_manager::{
    self, ProgramSpawner, SocketPeers, SupervisorManager,
};
use cowshed_core::runtime::supervisor_socket;
use cowshed_core::runtime::wait_spans::WaitFailure;
use cowshed_core::storage::bootstrap::CanonicalRoots;
use cowshed_core::{
    CowshedError, ErrorCode, GatewayStoreBackend, NativeGatewayInventory, Result, StartupHealState,
    ValidatedHostStorage, validate_existing_host_storage,
};
use cowshed_gateway::{
    ArrowAuditConfig, ControlError, ControlFailureCode, DaemonStatus, Gateway, GatewayConfig,
    GatewayControlClient, GatewayError, GatewayHandle, MirrorCacheConfig, StartupHeal,
    StartupProbe, SupervisorRecovery, WorkspaceSession,
};
use cowshed_gateway_types::host_status::{LoadReading, MemoryPressureReading, SwapReading};
use cowshed_os::deadline::WaitError;
use cowshed_os::namespace::HostNamespace;
use cowshed_os::stdio::Blocking;
use cowshed_types::repository::RepoId;
use sha2::{Digest as _, Sha256};
use std::convert::Infallible;
use std::fs;
use std::io::{self, Read as _, Write};
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

pub use cowshed_core::gateway_sessions::{
    ControlRefusal, GATEWAY_START_HINT, GatewayControl, GatewayInstaller, GatewayPortReallocator,
    GatewayStatusError, NativeSessionInventory, ReconcileReport, SessionInventory,
    SessionRestoreRefusal, SessionRestoreReport, canonical_home, control_error, effective_uid,
    gateway_absent, install_all_sessions, policy_from_grants, project_session_prefix,
    reconcile_against_status, reconcile_project, reconcile_project_with_reallocator,
    session_from_fact, sessions_from_facts, stable_workspace_id,
};
use cowshed_gateway_types::FirstLaunch;
use std::collections::BTreeSet;

/// Bind the runtime's gateway dependency once, without making a control request during open.
pub fn runtime_control(storage: &ValidatedHostStorage) -> Result<Box<dyn GatewayControl>> {
    Ok(Box::new(ControlSocket::at(storage.control_socket())?))
}

/// A running daemon reached over its control socket.
///
/// Reconciliation lives in `cowshed-core` and is a pure function of host inventory plus the three
/// answers [`GatewayControl`] gives, so the controller crate depends only on
/// `cowshed-gateway-types` and never links the daemon's hyper/rustls closure. That leaves neither
/// crate able to write the impl — the trait belongs to one, the client to the other — so it lands
/// here in the composition root, the one place holding both halves.
pub struct ControlSocket(GatewayControlClient);

impl ControlSocket {
    pub fn at(socket: PathBuf) -> Result<Self> {
        GatewayControlClient::new(socket)
            .map(Self)
            .map_err(control_error)
    }

    /// Observe only the installed revision the resident admission path needs.
    pub async fn session_current(&self, workspace_id: &str, revision: u64) -> Result<bool> {
        self.0
            .session_current(workspace_id, revision)
            .await
            .map_err(|error| match expired(&error) {
                Some(failure) => CowshedError::environment_missing(
                    error.to_string(),
                    "cowshed doctor --json names the gateway and its socket",
                )
                .with_wait(Some(failure)),
                None => control_error(error),
            })
    }

    /// Clear the project's Nx accepted-task store. A refusal while the project's Nx work is in
    /// flight is a conflict naming that work; a gateway that is not running, or one of an earlier
    /// build that does not serve `nx-cache-clear`, is said as the setup it needs.
    pub async fn clear_nx_cache(
        &self,
        repo_id: &RepoId,
    ) -> Result<cowshed_core::api::NxStoreClearResult> {
        self.0
            .clear_nx_cache(repo_id)
            .await
            .map_err(|error| match error {
                ControlError::Wait(WaitError::Absent { .. }) => CowshedError::environment_missing(
                    "no gateway is running, so the project's Nx store has no owner to clear it",
                    GATEWAY_START_HINT,
                ),
                ControlError::Rejected {
                    code: ControlFailureCode::Busy,
                    message,
                } => CowshedError::conflict(
                    format!("the gateway did not clear the project's Nx store: {message}"),
                    "let the named Nx work finish, then run `cowshed cache clear` again",
                ),
                ControlError::Rejected {
                    code: ControlFailureCode::InvalidRequest,
                    message,
                } => CowshedError::environment_missing(
                    format!("the running gateway does not serve nx-cache-clear: {message}"),
                    "cowshed setup",
                ),
                error => CowshedError::environment_missing(
                    format!("the gateway did not clear the project's Nx store: {error}"),
                    "cowshed gateway status",
                )
                .with_wait(expired(&error)),
            })
    }
}

#[async_trait]
impl GatewayControl for ControlSocket {
    async fn status(&self) -> std::result::Result<DaemonStatus, GatewayStatusError> {
        self.0.status().await.map_err(status_error)
    }

    async fn install(&self, session: &WorkspaceSession) -> std::result::Result<(), ControlRefusal> {
        self.0.install(session).await.map_err(control_refusal)
    }

    async fn remove(
        &self,
        workspace_id: &str,
        expected_revision: u64,
    ) -> std::result::Result<(), ControlRefusal> {
        self.0
            .remove(workspace_id, expected_revision)
            .await
            .map_err(control_refusal)
    }
}

/// The fence's two refusals are another reconcile's newer decision: an install whose revision the
/// workspace has already held or passed, a removal whose session was replaced (`RevisionFence`) or
/// already removed (`NotInstalled`). Everything else is this write failing.
fn control_refusal(error: ControlError) -> ControlRefusal {
    match error {
        ControlError::Rejected {
            code: ControlFailureCode::RevisionFence | ControlFailureCode::NotInstalled,
            ..
        } => ControlRefusal::Superseded(error.to_string()),
        ControlError::Rejected {
            code: ControlFailureCode::AddressInUse,
            ..
        } => ControlRefusal::AddressInUse(error.to_string()),
        error => match expired(&error) {
            Some(failure) => ControlRefusal::Waited(Box::new(failure)),
            None => ControlRefusal::Failed(error.to_string()),
        },
    }
}

/// A status read's failure as [`GatewayControl`] answers it. A missing socket is not a fault: no
/// daemon is running, so no gate job runs, and the remedy is to start one rather than to report
/// a broken control plane. An expired wait is said as one.
fn status_error(error: ControlError) -> GatewayStatusError {
    match error {
        ControlError::Wait(WaitError::Absent { .. }) => GatewayStatusError::Absent,
        error => match expired(&error) {
            Some(failure) => GatewayStatusError::Waited(Box::new(failure)),
            None => GatewayStatusError::Control(error.to_string()),
        },
    }
}

/// The wait `error` reports, when it is one that expired (05_gateway.md, "Deadlines", Spans).
fn expired(error: &ControlError) -> Option<WaitFailure> {
    match error {
        ControlError::Wait(failure) => WaitFailure::of(failure),
        ControlError::InvalidSocketPath
        | ControlError::InvalidTcpEndpoint
        | ControlError::InvalidControllerCredential
        | ControlError::Unauthorized
        | ControlError::MessageTooLarge
        | ControlError::InvalidAuditTailLimit
        | ControlError::InvalidSession(_)
        | ControlError::Encoding(_)
        | ControlError::InvalidResponse
        | ControlError::Rejected { .. }
        | ControlError::Io(_) => None,
    }
}

/// The daemon this process itself runs, installed into through its actor handle rather than over
/// the socket — the startup heal, before anything outside can connect.
pub struct OwnedGateway(GatewayHandle);

impl OwnedGateway {
    pub fn new(handle: GatewayHandle) -> Self {
        Self(handle)
    }
}

#[async_trait]
impl GatewayInstaller for OwnedGateway {
    async fn install_session(
        &self,
        session: WorkspaceSession,
    ) -> std::result::Result<(), SessionRestoreRefusal> {
        self.0.install(session).await.map_err(|error| match error {
            GatewayError::Io(source) if source.kind() == io::ErrorKind::AddrInUse => {
                SessionRestoreRefusal::AddressInUse(source.to_string())
            }
            error => SessionRestoreRefusal::Failed(error.to_string()),
        })
    }
}

/// The owning project actor publishes a replacement port grant; gateway reconciliation reads
/// that published revision back rather than manufacturing a session from the requested slot.
#[async_trait]
pub trait PortSlotAssigner: Send + Sync {
    async fn assign_port_slot(&self, workspace: &str, slot: u32) -> Result<()>;
}

#[async_trait]
impl PortSlotAssigner for Coordinator {
    async fn assign_port_slot(&self, workspace: &str, slot: u32) -> Result<()> {
        self.assign_slot(workspace, slot).await
    }
}

struct NativePortReallocator<'a> {
    storage: &'a ValidatedHostStorage,
    repo_id: &'a RepoId,
    assigner: &'a dyn PortSlotAssigner,
    backend: GatewayStoreBackend,
}

#[async_trait]
impl GatewayPortReallocator for NativePortReallocator<'_> {
    async fn reallocate(
        &self,
        session: &WorkspaceSession,
        rejected: &BTreeSet<u16>,
    ) -> Result<WorkspaceSession> {
        let inventory = NativeGatewayInventory::with_backend(self.storage.clone(), self.backend);
        let attached = inventory
            .project_attached(self.repo_id)
            .await
            .map_err(|error| {
                CowshedError::integrity(
                    format!("cannot inspect gateway port owners: {error}"),
                    "cowshed doctor --json",
                )
            })?;
        let fact = attached
            .attached
            .into_iter()
            .find(|fact| {
                stable_workspace_id(&fact.repo_id, fact.workspace.as_str(), &fact.incarnation)
                    == session.workspace_id
            })
            .ok_or_else(|| {
                CowshedError::conflict(
                    format!(
                        "gateway workspace {} is no longer attached",
                        session.workspace_id
                    ),
                    "retry against current workspace inventory",
                )
            })?;
        if fact.revision > session.revision {
            return session_from_fact(fact);
        }
        let used = inventory
            .all_reserved_port_blocks()
            .await
            .map_err(|error| {
                CowshedError::integrity(
                    format!("cannot inspect gateway port blocks: {error}"),
                    "cowshed doctor --json",
                )
            })?;
        let candidates = PortBlock::macos_candidates_with_size(
            self.storage.port_allocation(),
            fact.port_block.size(),
        )
        .map_err(|error| {
            CowshedError::integrity(
                format!("cannot enumerate workspace port capacity: {error}"),
                "cowshed doctor --json",
            )
        })?;
        for block in candidates {
            if used.overlapping(block).is_some() || rejected.contains(&block.base()) {
                continue;
            }
            let slot = u32::from(block.base()) / u32::from(NEW_PORT_BLOCK_SIZE);
            match self
                .assigner
                .assign_port_slot(fact.workspace.as_str(), slot)
                .await
            {
                Ok(()) => {
                    return NativeSessionInventory::with_backend(
                        self.storage.clone(),
                        self.backend,
                    )
                    .project_sessions(self.repo_id)
                    .await?
                    .serve
                    .into_iter()
                    .find(|current| current.workspace_id == session.workspace_id)
                    .ok_or_else(|| {
                        CowshedError::conflict(
                            format!(
                                "gateway workspace {} disappeared during port reallocation",
                                session.workspace_id
                            ),
                            "retry against current workspace inventory",
                        )
                    });
                }
                Err(error) if error.code == ErrorCode::Conflict => continue,
                Err(error) => return Err(error),
            }
        }
        Err(CowshedError::conflict(
            format!(
                "no free macOS gateway port block remains for {}",
                fact.workspace
            ),
            "release an unused workspace port block or stop the process holding a service port",
        ))
    }
}

pub async fn reconcile_native_project(
    repo_id: &RepoId,
    storage: &ValidatedHostStorage,
    assigner: &dyn PortSlotAssigner,
) -> Result<ReconcileReport> {
    reconcile_project_for_backend(repo_id, storage, assigner, GatewayStoreBackend::Native).await
}

/// Reconcile a caller-owned directory store's project: its sessions derive from the
/// directory host's proven mount records, not the kernel mount table.
pub async fn reconcile_directory_project(
    repo_id: &RepoId,
    storage: &ValidatedHostStorage,
    assigner: &dyn PortSlotAssigner,
) -> Result<ReconcileReport> {
    reconcile_project_for_backend(repo_id, storage, assigner, GatewayStoreBackend::Directory).await
}

async fn reconcile_project_for_backend(
    repo_id: &RepoId,
    storage: &ValidatedHostStorage,
    assigner: &dyn PortSlotAssigner,
    backend: GatewayStoreBackend,
) -> Result<ReconcileReport> {
    let inventory = NativeSessionInventory::with_backend(storage.clone(), backend);
    let control = ControlSocket::at(storage.control_socket())?;
    let reallocator = NativePortReallocator {
        storage,
        repo_id,
        assigner,
        backend,
    };
    cowshed_os::timing::spanned(
        "reconcile",
        "sessions",
        reconcile_project_with_reallocator(
            &control,
            &inventory,
            repo_id,
            effective_uid(),
            &reallocator,
        ),
    )
    .await
}

/// How long `gateway start` waits for the daemon to become healthy.
///
/// Sized for the startup heal rather than for a process start: the daemon answers at once, but
/// it attaches, checks, and mounts every recorded project's images and restores their sessions
/// before it serves a workspace (05_gateway.md), and a host carrying several multi-gigabyte mains
/// needs minutes for that pass. Ten seconds timed out mid-heal and told the user to kickstart a
/// gateway that was working exactly as intended.
const START_DEADLINE: Duration = Duration::from_secs(180);
const START_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// How often the wait says that it is still waiting, and on what.
const START_PROGRESS_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayPaths {
    pub home: PathBuf,
    pub store: PathBuf,
    /// cowshed's user cache directory, which holds the gateway's mirrors (03_caches.md, layer 1).
    pub cache_dir: PathBuf,
    pub mirror_cache: PathBuf,
    pub telemetry: PathBuf,
    pub control_socket: PathBuf,
}

impl GatewayPaths {
    pub fn from_storage(storage: &ValidatedHostStorage) -> Self {
        Self {
            home: storage.home().to_path_buf(),
            store: storage.store().to_path_buf(),
            cache_dir: cowshed_core::host_dirs::cache_directory(storage.home()),
            mirror_cache: cowshed_core::host_dirs::gateway_mirror(storage.home()),
            telemetry: storage.telemetry().join("gateway"),
            control_socket: storage.control_socket(),
        }
    }

    pub fn config(&self, uid: u32, git_helper_executable: PathBuf) -> GatewayConfig {
        GatewayConfig {
            control_socket: Some(self.control_socket.clone()),
            control_tcp: None,
            simulator_drop_root: None,
            data_socket_root: None,
            production_cache_dir: Some(self.cache_dir.clone()),
            git_helper_executable: Some(git_helper_executable),
            authorized_control_uid: uid,
            mirror_cache: MirrorCacheConfig::new(self.mirror_cache.clone()),
            #[cfg(target_os = "macos")]
            listener_handoff: Some(cowshed_gateway::handoff_path(&self.control_socket)),
            ..GatewayConfig::default()
        }
    }
}

#[async_trait]
pub trait GatewayDrain: Send {
    async fn drain(self) -> Result<()>;
    /// Resolves only if the gateway stops without being asked to, with why it stopped.
    async fn stopped(&mut self) -> CowshedError;
}

#[async_trait]
impl GatewayDrain for Gateway {
    async fn drain(self) -> Result<()> {
        Gateway::drain(self)
            .await
            .map_err(|error| CowshedError::internal(format!("could not drain gateway: {error}")))
    }

    async fn stopped(&mut self) -> CowshedError {
        match Gateway::stopped(self).await {
            Ok(()) => CowshedError::internal("the gateway stopped without being asked to drain"),
            Err(error) => CowshedError::internal(format!("the gateway stopped: {error}")),
        }
    }
}

/// Serve until the shutdown signal, then drain — or until the gateway stops on its own, which
/// ends the daemon with that failure so launchd restarts it. A gateway that has failed closed
/// refuses every session; a daemon that outlived it would answer status as if it were serving.
pub async fn drain_after_shutdown<D, F>(mut daemon: D, shutdown: F) -> Result<()>
where
    D: GatewayDrain,
    F: Future<Output = Result<()>>,
{
    let stopped = tokio::select! {
        signal = shutdown => {
            signal?;
            None
        }
        reason = daemon.stopped() => Some(reason),
    };
    match stopped {
        Some(reason) => Err(reason),
        None => daemon.drain().await,
    }
}

/// How often a bootout reads launchd's hold on the job again, and how long each wait on the old
/// process's exit lasts between those reads.
const BOOTOUT_POLL_INTERVAL: Duration = Duration::from_millis(100);

fn launch_agent_is_loaded<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    uid: u32,
    target: &LaunchAgentTarget,
) -> Result<bool>
where
    C: LaunchctlCommand,
{
    executor
        .execute_status(&crate::launchd::ControlPlan::print(uid, target))
        .map(|status| matches!(status, LaunchdServiceStatus::Loaded { .. }))
        .map_err(launchd_error)
}

/// The loaded agent's process and exit timeout, as `launchctl print` reports them; `None` when
/// launchd does not hold the agent.
fn loaded_agent<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    uid: u32,
    target: &LaunchAgentTarget,
) -> Result<Option<LoadedAgent>>
where
    C: LaunchctlCommand,
{
    match executor
        .execute_status(&crate::launchd::ControlPlan::print(uid, target))
        .map_err(launchd_error)?
    {
        LaunchdServiceStatus::NotLoaded { .. } => Ok(None),
        LaunchdServiceStatus::Loaded { stdout, .. } => {
            agent_cycle::parse_print(&String::from_utf8_lossy(&stdout))
                .map(Some)
                .map_err(|error| {
                    CowshedError::environment_missing(
                        format!("{} is loaded, but {error}", target.label()),
                        format!(
                            "read `launchctl print {}`",
                            crate::launchd::service_target(uid, target.label())
                        ),
                    )
                })
        }
    }
}

/// Why an agent was not activated: a step failed, or a bootout launchd did not finish by its own
/// deadline. Kept apart because a restore after the second must not touch the agent launchd
/// still tears down.
#[derive(Debug)]
enum ActivationError {
    Failed(CowshedError),
    BootoutOverdue {
        label: String,
        remains: Remains,
        exit_timeout: Duration,
        /// What `launchctl bootout` itself answered, when it answered a failure: context only.
        /// launchctl can exit non-zero while launchd tears the job down, so a failed bootout is
        /// no proof the job is untouched, and never licenses a kickstart of it.
        refused: Option<String>,
        /// How the agent is loaded again once launchd has let go of it.
        bootstrap: String,
    },
}

// A method, not `From`: a second `From<_> for CowshedError` in this crate leaves every `?` on an
// uninferred error type ambiguous.
impl ActivationError {
    fn into_error(self) -> CowshedError {
        match self {
            Self::Failed(error) => error,
            Self::BootoutOverdue {
                label,
                remains,
                exit_timeout,
                refused,
                bootstrap,
            } => CowshedError::environment_missing(
                format!(
                    "launchd did not finish booting out {label} within its {}s exit timeout and \
                     the {}s its unload may trail it by: {remains}{}",
                    exit_timeout.as_secs(),
                    agent_cycle::UNLOAD_GRACE.as_secs(),
                    refused
                        .map(|refused| format!(" (the bootout itself answered: {refused})"))
                        .unwrap_or_default()
                ),
                format!("once that has gone, {bootstrap}"),
            ),
        }
    }
}

/// Load the agent and leave launchd running *this* plist.
///
/// A rewritten plist is only a file. launchd keeps the definition it bootstrapped, so a kickstart
/// on its own restarts the program the agent was loaded with — which, for a plist rewritten to
/// name the host-stable binary, is exactly the vanished path the rewrite exists to stop naming.
/// A changed plist is therefore booted out first, and the bootstrap that follows reads it.
///
/// Bootstrap already starts a `RunAtLoad` agent. `kickstart -k` on the heels of that returns
/// launchctl 37 (operation already in progress) and fails a setup that already copied the binary.
pub fn activate_launch_agent<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    uid: u32,
    target: &LaunchAgentTarget,
    plist: InstallOutcome,
) -> Result<()>
where
    C: LaunchctlCommand,
{
    activate(executor, uid, target, plist).map_err(ActivationError::into_error)
}

fn activate<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    uid: u32,
    target: &LaunchAgentTarget,
    plist: InstallOutcome,
) -> std::result::Result<(), ActivationError>
where
    C: LaunchctlCommand,
{
    if plist == InstallOutcome::Changed {
        boot_out(executor, uid, target)?;
    }
    let held = loaded_agent(executor, uid, target)
        .map_err(ActivationError::Failed)?
        .map_or(Held::Absent, Held::Loaded);
    load(executor, uid, target, agent_cycle::reload(held))
}

/// Take the one launchctl step that loads the agent again. A bootstrap that fails while launchd
/// reports the agent loaded anyway loaded it: a `RunAtLoad` agent can race its own bootstrap.
fn load<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    uid: u32,
    target: &LaunchAgentTarget,
    step: Reload,
) -> std::result::Result<(), ActivationError>
where
    C: LaunchctlCommand,
{
    match step {
        Reload::Bootstrap => {
            if let Err(error) =
                executor.execute_control(&crate::launchd::ControlPlan::bootstrap(uid, target))
                && !launch_agent_is_loaded(executor, uid, target)
                    .map_err(ActivationError::Failed)?
            {
                return Err(ActivationError::Failed(launchd_error(error)));
            }
            Ok(())
        }
        Reload::Kickstart(agent) => executor
            .execute_control(&crate::launchd::ControlPlan::kickstart(uid, target, agent))
            .map(|_| ())
            .map_err(|error| ActivationError::Failed(launchd_error(error))),
        Reload::Blocked(remains) => {
            Err(ActivationError::Failed(CowshedError::environment_missing(
                format!(
                    "{} is not loaded again: launchd still holds it after its bootout went \
                     overdue, and {remains}",
                    target.label()
                ),
                format!(
                    "once that has gone, {}",
                    crate::launchd::bootstrap_hint(uid, target)
                ),
            )))
        }
    }
}

/// Boot the agent out and wait until its process has exited and launchd has dropped the job.
///
/// `launchctl bootout` returns before the service has finished tearing down: launchd sends
/// SIGTERM and lets the process drain until the agent's exit timeout, then SIGKILLs it. So the
/// wait is for the real condition, both halves of it: the kernel's word that the process exited,
/// which it posts once the process has released every descriptor, its listeners included, and
/// launchd's word that it no longer holds the job. Watched from before the bootout signals it,
/// so no exit is missed and no later process given the pid is waited on. The only bound is
/// launchd's own: the exit timeout of the definition it loaded, read from `launchctl print`, and
/// the grace its unload may trail its kill by ([`agent_cycle::bootout_deadline`]). A bootout
/// that outlives it is [`ActivationError::BootoutOverdue`], naming what is left. A fixed 5 s
/// wait stood here; launchd's default exit timeout is also 5 s, so it gave up at the very
/// instant launchd killed a draining gateway, and the restore that followed kickstarted a job
/// still in teardown (launchctl 37). `Ok` when launchd never had the agent loaded.
pub fn deactivate_launch_agent<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    uid: u32,
    target: &LaunchAgentTarget,
) -> Result<()>
where
    C: LaunchctlCommand,
{
    boot_out(executor, uid, target).map_err(ActivationError::into_error)
}

fn boot_out<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    uid: u32,
    target: &LaunchAgentTarget,
) -> std::result::Result<(), ActivationError>
where
    C: LaunchctlCommand,
{
    let Some(agent) = loaded_agent(executor, uid, target).map_err(ActivationError::Failed)? else {
        return Ok(());
    };
    let exit = match agent.pid {
        Some(pid) => cowshed_os::process::ExitWatch::of(pid)
            .map_err(|error| {
                ActivationError::Failed(CowshedError::internal(format!(
                    "could not watch the exit of {}'s process {pid}: {error}",
                    target.label()
                )))
            })?
            .map(|watch| (pid, watch)),
        None => None,
    };
    let booted_out =
        executor.execute_control(&crate::launchd::ControlPlan::bootout(uid, target, agent));
    let started = std::time::Instant::now();
    let mut running = exit.as_ref().map(|(pid, _)| *pid);
    loop {
        if running.is_some()
            && let Some((pid, watch)) = &exit
            && watch.within(BOOTOUT_POLL_INTERVAL).map_err(|error| {
                ActivationError::Failed(CowshedError::internal(format!(
                    "could not wait for the exit of {}'s process {pid}: {error}",
                    target.label()
                )))
            })?
        {
            running = None;
        }
        // A process that still runs is launchd's job by definition; only once it exited is the
        // job's unload asked.
        let job_loaded = running.is_some()
            || launch_agent_is_loaded(executor, uid, target).map_err(ActivationError::Failed)?;
        match agent_cycle::bootout_progress(agent, running, job_loaded, started.elapsed()) {
            Bootout::Done => return Ok(()),
            Bootout::Overdue(remains) => {
                // Overdue whatever launchctl answered: it can exit non-zero while launchd tears
                // the job down, and a restore that read that as "untouched" would kickstart a job
                // in teardown (launchctl 37). Its answer is kept as context.
                return Err(ActivationError::BootoutOverdue {
                    label: target.label().to_owned(),
                    remains,
                    exit_timeout: agent.exit_timeout,
                    refused: booted_out.err().map(|error| launchd_error(error).message),
                    bootstrap: crate::launchd::bootstrap_hint(uid, target),
                });
            }
            Bootout::Waiting if running.is_none() => std::thread::sleep(BOOTOUT_POLL_INTERVAL),
            Bootout::Waiting => {}
        }
    }
}

pub async fn dispatch<W, E>(
    action: GatewayCommand,
    json: bool,
    output: &mut Output<W, E>,
) -> Result<i32>
where
    W: Write + Send,
    E: Write + Send,
{
    match action {
        GatewayCommand::Start => {
            let status = start(output).await?;
            emit_gateway_status(output, json, status)?;
        }
        GatewayCommand::Stop { purge } => {
            let purged = stop(CanonicalRoots::global().namespace(), purge).await?;
            if json {
                output.success(EmptyResult {}).map_err(output_error)?;
            } else {
                output
                    .guidance("gateway is stopped")
                    .map_err(output_error)?;
                if purge {
                    output
                        .guidance(&match purged {
                            RemovalOutcome::Removed => {
                                String::from("removed the installed cowshed binary")
                            }
                            RemovalOutcome::AlreadyAbsent => {
                                String::from("no installed cowshed binary to remove")
                            }
                        })
                        .map_err(output_error)?;
                }
            }
        }
        GatewayCommand::Status => {
            let status = service_status(&CanonicalRoots::global()).await?;
            emit_gateway_status(output, json, status)?;
        }
        GatewayCommand::Run => run_daemon().await?,
    }
    Ok(0)
}

/// `gateway start` and `host.gatewayStart`: take the install slot, then install the running build
/// and start the gateway onto it, refusing a downgrade.
pub(crate) async fn start<W, E>(output: &mut Output<W, E>) -> Result<CliGatewayStatus>
where
    W: Write + Send,
    E: Write + Send,
{
    let slot = take_install_slot(&canonical_home()?).await?;
    start_service(output, &slot, Downgrade::Refuse).await
}

/// `gateway stop [--purge]` and `host.gatewayStop`. A purge empties the install slot, so it waits
/// its turn like any install does. `namespace` is the host's, whose agent is stopped.
pub(crate) async fn stop(namespace: HostNamespace, purge: bool) -> Result<RemovalOutcome> {
    let _slot = match purge {
        true => Some(take_install_slot(&canonical_home()?).await?),
        false => None,
    };
    stop_service(namespace, purge)
}

/// Install the running build and start the gateway onto it. `slot` is the install slot this
/// install holds ([`take_install_slot`]).
async fn start_service<W, E>(
    output: &mut Output<W, E>,
    slot: &InstallSlot,
    downgrade: Downgrade,
) -> Result<CliGatewayStatus>
where
    W: Write + Send,
    E: Write + Send,
{
    let home = canonical_home()?;
    let storage = validate_existing_host_storage(&home).await?;
    let paths = GatewayPaths::from_storage(&storage);
    ensure_private_directory(&paths.telemetry)?;
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    // The candidate path is derived first because the plist has to name it before the agent can be
    // activated, and the install-then-activate pair is what has to be undoable as a whole.
    let candidate = Candidate::of(supervisable_running_executable()?, BuildRecord::running())?;
    let executable =
        HostStableExecutable::new(&home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    check_basis(&executable, &candidate, downgrade, async || {
        adopted_mains(&home).await
    })
    .await?;
    let identity = gateway_host_identity(&home)?;
    let bun = invoking_bun()?;
    let client = GatewayControlClient::new(paths.control_socket.clone()).map_err(control_error)?;
    // Activation restarts a gateway that runs; one that answers is refused the restart while a
    // land runs, before the install changes anything.
    if client.status().await.is_ok() {
        refuse_restart_while_busy(slot, &home)?;
    }
    install_nx_cache_server(&mut executor, &home, &candidate.source)?;
    let spec =
        LaunchAgentSpec::gateway(storage.namespace(), &executable, &bun).map_err(launchd_error)?;
    let observed = inspect_install_state(spec.target())?;
    let plan = plan_install(
        &spec,
        InstallState {
            launch_agents_directory_mode: observed.directory_mode,
            plist: observed.plist.as_ref().map(|plist| ExistingPlist {
                bytes: &plist.bytes,
                mode: plist.mode,
            }),
        },
    );
    // Held until the restarted gateway serves: it took the listeners before it bound anything.
    let _carrying = carry_listeners(&paths.control_socket, downgrade);
    let written = executor.execute_install(&plan).map_err(launchd_error)?;
    install_and_activate_gateway(
        &mut executor,
        &home,
        &candidate,
        spec.target(),
        written,
        identity,
    )?;
    let reported = await_healthy(
        output,
        slot,
        &client,
        &paths.control_socket,
        &candidate.digest.to_string(),
        &mut executor,
        spec.target(),
    )
    .await?;
    Ok(CliGatewayStatus {
        scheduling: Some(read_scheduling(storage.store()).await),
        ..reported
    })
}

/// The install's side of the listener handoff (05_gateway.md "An install never refuses a client
/// connection"): serving it from before the restart until the restarted gateway serves carries
/// every workspace listener across the restart. A downgrade serves nothing: an older gateway may
/// not take the listeners, and its own bind would then meet this install's copies. A handoff that
/// cannot serve is said, and the restart refuses connections, as it did before.
#[cfg(target_os = "macos")]
fn carry_listeners(
    control_socket: &Path,
    downgrade: Downgrade,
) -> Option<cowshed_gateway::HandoffServer> {
    if downgrade == Downgrade::Allow {
        return None;
    }
    match cowshed_gateway::HandoffServer::start(&cowshed_gateway::handoff_path(control_socket)) {
        Ok(server) => Some(server),
        Err(error) => {
            cowshed_os::eprintln!(
                "cowshed: the handoff socket cannot serve, so this restart carries no workspace \
             listener: {error}"
            );
            None
        }
    }
}

/// Linux binds no TCP workspace listener, so a restart has none to carry and serves no handoff.
#[cfg(not(target_os = "macos"))]
const fn carry_listeners(_control_socket: &Path, _downgrade: Downgrade) -> Option<()> {
    None
}

/// Wait until the gateway just activated serves: its control socket answers from the bytes
/// `cli_sha256` names, its startup pass is over and it is not draining. The install holds
/// `_slot` throughout, so a land step that waited on the slot reaches a gateway that serves
/// workspaces, never a socket nothing listens on yet (05_gateway.md "A land step waits for an
/// install in progress"). launchd keeps the process it had when activation did not change the
/// plist; that daemon is restarted once.
async fn await_healthy<W, E, C>(
    output: &mut Output<W, E>,
    _slot: &InstallSlot,
    client: &GatewayControlClient,
    control_socket: &Path,
    cli_sha256: &str,
    executor: &mut LaunchdExecutor<NativeFilesystem, C>,
    target: &LaunchAgentTarget,
) -> Result<CliGatewayStatus>
where
    W: Write + Send,
    E: Write + Send,
    C: LaunchctlCommand,
{
    let uid = effective_uid();
    let mut progress = StartProgress::new();
    let mut started = tokio::time::Instant::now();
    let mut restarted = false;
    loop {
        let waiting = match client.status().await {
            Err(_) => StartWait::ControlSocket,
            Ok(status) => {
                let reported = cli_status(
                    true,
                    control_socket.to_path_buf(),
                    Some(&status),
                    cli_sha256,
                );
                match (
                    &reported.drain_cause,
                    &reported.stale_daemon,
                    reported.healing,
                ) {
                    (None, None, None) => return Ok(reported),
                    // It answers, and tells how far its startup pass has got; nothing that
                    // needs a workspace is served until that pass is over.
                    (None, None, Some(heal)) => StartWait::Startup(heal),
                    // launchd kept the process it already had: the plist did not change, so
                    // activation did not restart it onto the bytes just installed. Restart it
                    // once.
                    (None, Some(_), _) if !restarted => {
                        // The land turns were judged under this slot before the install
                        // changed anything, and every land step since waits for the slot, so this
                        // restart cuts nothing off. The start deadline is the new daemon's to
                        // meet, not the old one's drain.
                        started = tokio::time::Instant::now();
                        activate_launch_agent(executor, uid, target, InstallOutcome::Changed)?;
                        restarted = true;
                        StartWait::ControlSocket
                    }
                    (None, Some(stale), _) => {
                        return Err(CowshedError::conflict(
                            format!(
                                "the gateway still runs a different cowshed binary after a restart (daemon sha256 {}, cli sha256 {})",
                                stale.daemon_sha256.as_deref().unwrap_or("unreported"),
                                stale.cli_sha256
                            ),
                            STALE_DAEMON_REMEDY,
                        ));
                    }
                    // A draining daemon exits once its in-flight work ends and launchd restarts
                    // it; it is not healthy until then.
                    (Some(_), _, _) => StartWait::Drain,
                }
            }
        };
        let waited = started.elapsed();
        if waited >= START_DEADLINE {
            return Err(CowshedError::environment_missing(
                format!(
                    "gateway did not become healthy within {}s of starting",
                    START_DEADLINE.as_secs()
                ),
                kickstart_hint(uid, target.label()),
            ));
        }
        if let Some(line) = progress.line(waited, waiting) {
            output.guidance(&line).map_err(output_error)?;
        }
        tokio::time::sleep(START_POLL_INTERVAL).await;
    }
}

/// What `gateway start` is still waiting for, as the last poll found it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum StartWait {
    /// Nothing answers the control socket yet.
    ControlSocket,
    /// The daemon answers while its startup pass still mounts projects or restores sessions.
    Startup(StartupHeal),
    /// The daemon answers while it drains; launchd restarts it once it exits.
    Drain,
}

/// The wait's own reporting, at most one line per [`START_PROGRESS_INTERVAL`].
///
/// A first start after a reboot mounts every recorded project before its workspaces are served,
/// which is minutes on a host with several multi-gigabyte mains — long enough that a silent wait
/// leaves only the conclusion that cowshed has hung. So the wait says what it is waiting for, in
/// the daemon's own count, and how long it has waited.
struct StartProgress {
    next: Duration,
}

impl StartProgress {
    const fn new() -> Self {
        Self {
            next: START_PROGRESS_INTERVAL,
        }
    }

    /// The line to emit after waiting `waited` on `waiting`, or `None` while the last one is
    /// still current.
    fn line(&mut self, waited: Duration, waiting: StartWait) -> Option<String> {
        if waited < self.next {
            return None;
        }
        // Anchored to `waited` rather than advanced by one interval: a poll that returns late —
        // a mount saturating the disk — then reports once instead of flushing a backlog of lines
        // for intervals that have already passed.
        self.next = waited + START_PROGRESS_INTERVAL;
        Some(format!(
            "waited {}s for the gateway: {}",
            waited.as_secs(),
            match waiting {
                StartWait::ControlSocket => String::from("its control socket does not answer yet…"),
                StartWait::Startup(heal) => format!("{heal}…"),
                StartWait::Drain => {
                    String::from("the running gateway is draining before launchd restarts it…")
                }
            }
        ))
    }
}

/// The binary this process is running from.
fn running_executable() -> Result<PathBuf> {
    let path = std::env::current_exe().map_err(|error| {
        CowshedError::environment_missing(
            format!("could not identify the cowshed executable: {error}"),
            "reinstall cowshed",
        )
    })?;
    fs::canonicalize(&path).map_err(|error| {
        CowshedError::environment_missing(
            format!("could not resolve the cowshed executable: {error}"),
            "reinstall cowshed",
        )
    })
}

/// Boot the agent out and delete its plist, reporting whether a plist was there.
///
/// Shared by `gateway stop`, `sccache stop`, and `setup --uninstall`: an agent is deactivated
/// before its definition is removed, or launchd keeps running a service whose plist has gone.
pub fn remove_launch_agent(target: &LaunchAgentTarget) -> Result<RemovalOutcome> {
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    deactivate_launch_agent(&mut executor, effective_uid(), target)?;
    let installed = fs::symlink_metadata(target.plist_path()).is_ok();
    executor
        .execute_install(&plan_remove(target, installed))
        .map_err(launchd_error)?;
    Ok(if installed {
        RemovalOutcome::Removed
    } else {
        RemovalOutcome::AlreadyAbsent
    })
}

/// Delete the installed cowshed a LaunchAgent ran: the stable name first, then every stored copy
/// with its build record. Only ever called once its agent is gone: the gateway agent is
/// `KeepAlive`, so removing the binary under a loaded agent would leave launchd respawning a path
/// that no longer resolves.
pub fn remove_host_stable_executable(executable: &HostStableExecutable) -> Result<RemovalOutcome> {
    let present = installed_files(executable)?;
    LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand)
        .execute_install(&plan_executable_remove(executable, &present))
        .map_err(launchd_error)?;
    Ok(if present.is_empty() {
        RemovalOutcome::AlreadyAbsent
    } else {
        RemovalOutcome::Removed
    })
}

/// Everything an install of cowshed leaves in the host-stable directory, in the order to remove
/// it: the links (the `cargo` link an earlier install planted, then the stable name, so nothing
/// launchd could start dangles), then each stored copy and the build record beside it. A `cargo`
/// no install planted is left, as every other neighbour is.
pub(crate) fn installed_files(executable: &HostStableExecutable) -> Result<Vec<PathBuf>> {
    let mut present = Vec::new();
    if observe_cargo_link(executable)? == CargoLink::Planted {
        present.push(executable.directory().join(CARGO_LINK_NAME));
    }
    // The gateway host before the stable name it runs: launchd starts the host, so nothing it
    // could start is left once the host is gone.
    if executable.name() == COWSHED_BINARY_NAME
        && inspect_existing(&executable.gateway_host())?.is_some()
    {
        present.push(executable.gateway_host());
    }
    if inspect_existing(executable.path())?.is_some() {
        present.push(executable.path().to_path_buf());
    }
    present.extend(stored_files(executable, |_| true)?);
    Ok(present)
}

/// The stored copies in the host-stable directory, each beside its build record, for the builds
/// `selected` picks.
fn stored_files(
    executable: &HostStableExecutable,
    mut selected: impl FnMut(&ExecutableDigest) -> bool,
) -> Result<Vec<PathBuf>> {
    let entries = match fs::read_dir(executable.directory()) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => {
            return Err(CowshedError::internal(format!(
                "could not list {}: {error}",
                executable.directory().display()
            )));
        }
    };
    let mut files = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|error| {
            CowshedError::internal(format!(
                "could not list {}: {error}",
                executable.directory().display()
            ))
        })?;
        let name = entry.file_name();
        let digest = executable.stored_digest(&name).or_else(|| {
            let stem = name.to_str()?.strip_suffix(BUILD_RECORD_SUFFIX)?;
            executable.stored_digest(std::ffi::OsStr::new(stem))
        });
        if digest.is_some_and(|digest| selected(&digest)) {
            files.push(entry.path());
        }
    }
    files.sort();
    Ok(files)
}

/// The gateway agent's control target and installed cowshed, resolved from the canonical home
/// rather than the running binary.
///
/// Deterministic on purpose: stop and uninstall have to reach the agent `start` installed however
/// this process was invoked, and need neither the plist's contents nor a `bun` on this `PATH`.
///
/// The agent is `namespace`'s ([`HostNamespace::launchd_label`]): production's is
/// `dev.cowshed.gateway`.
pub fn gateway_launch_agent(
    namespace: HostNamespace,
    home: &Path,
) -> Result<(HostStableExecutable, LaunchAgentTarget)> {
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let target =
        LaunchAgentTarget::new(home, &namespace.launchd_label("gateway")).map_err(launchd_error)?;
    Ok((executable, target))
}

/// The `bun` this invocation's `PATH` provides, for the gateway agent's `PATH`, named as a profile
/// nix or devenv keeps ([`BunDirectory`]). No `bun`, or one only a bare store path names, is a setup
/// error refused before anything is installed: cowshed bundles no interpreter (the owner's ruling,
/// 2026-10-18), and keeps no store path alive for one.
fn invoking_bun() -> Result<BunDirectory> {
    BunDirectory::on_path(
        &std::env::var_os("PATH").unwrap_or_default(),
        DevenvProfile::of_environment().as_ref(),
    )
    .map_err(|reason| {
        CowshedError::environment_missing(
            format!("the gateway runs its Nx cache server with bun: {reason}"),
            "run this cowshed's setup from main's devenv shell, or from a shell whose PATH has bun in a nix profile (such as /etc/profiles/per-user/$USER/bin)",
        )
    })
}

/// Stop the gateway; with `purge`, also delete the installed cowshed and its installed Nx cache
/// server companion it ran — and the links on `PATH` that name cowshed, which would otherwise
/// answer `command not found` from the next shell.
///
/// Without `purge` the copy stays: it is host state rather than agent state, and leaving it makes
/// the next `start` a plist write instead of a fresh multi-megabyte copy.
fn stop_service(namespace: HostNamespace, purge: bool) -> Result<RemovalOutcome> {
    let home = canonical_home()?;
    let (executable, target) = gateway_launch_agent(namespace, &home)?;
    remove_launch_agent(&target)?;
    if purge {
        remove_path_entries(executable.path())?;
        let companion =
            HostStableExecutable::new(&home, "nx-cache-server").map_err(launchd_error)?;
        let removed = [
            remove_host_stable_executable(&companion)?,
            remove_host_stable_executable(&executable)?,
        ];
        return Ok(
            if removed
                .iter()
                .any(|outcome| matches!(outcome, RemovalOutcome::Removed))
            {
                RemovalOutcome::Removed
            } else {
                RemovalOutcome::AlreadyAbsent
            },
        );
    }
    Ok(RemovalOutcome::AlreadyAbsent)
}

/// Remove the entries on the operator's `PATH` that name the installed cowshed, before it goes:
/// a link that outlived its target would answer `command not found` in the middle of the next
/// shell's work. Links that merely happen to arrive there are not cowshed's to remove.
pub(crate) fn remove_path_entries(stable: &Path) -> Result<RemovalOutcome> {
    let path = std::env::var_os("PATH").unwrap_or_default();
    let removed = crate::path_entry::remove_pointing_at(&path, stable).map_err(|error| {
        CowshedError::internal(format!(
            "could not remove the cowshed link on PATH that names {}: {error}",
            stable.display()
        ))
    })?;
    Ok(if removed.is_empty() {
        RemovalOutcome::AlreadyAbsent
    } else {
        RemovalOutcome::Removed
    })
}

/// Stop the gateway while setup moves its mirrors; the installed binary stays. A gateway that
/// answers is refused the stop while a land runs ([`refuse_restart_while_busy`]): stopping it
/// cuts the land off as a restart does.
pub(crate) async fn stop_for_host_move(slot: &InstallSlot, roots: &CanonicalRoots) -> Result<()> {
    let client = GatewayControlClient::new(roots.control_socket()).map_err(control_error)?;
    if client.status().await.is_ok() {
        refuse_restart_while_busy(slot, &canonical_home()?)?;
    }
    stop_service(roots.namespace(), false).map(|_| ())
}

/// Start the gateway again after setup moved its mirrors, saying nothing: setup reports the
/// move itself, and the gateway's own start output belongs to `cowshed gateway start`.
pub(crate) async fn start_after_host_move(slot: &InstallSlot, downgrade: Downgrade) -> Result<()> {
    let mut quiet = Output::new(io::sink(), io::sink(), true);
    start_service(&mut quiet, slot, downgrade).await.map(|_| ())
}

pub(crate) async fn service_status(roots: &CanonicalRoots) -> Result<CliGatewayStatus> {
    let Ok(status) = observe_service(roots, |client| async move {
        Ok::<_, Infallible>(client.status().await.ok())
    })
    .await?;
    Ok(status)
}

/// A daemon that took a request over its socket and gave no answer within its asker's bound.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Unanswered {
    /// Who took the request: `the gateway daemon`, `the sccache server`.
    pub(crate) peer: &'static str,
    pub(crate) socket: PathBuf,
    pub(crate) within: Duration,
}

/// What doctor's first-launch probe read (05_gateway.md "The gateway host"): a fresh
/// executable's first and second launch in this process, the host shell's, and in the gateway
/// daemon's, whose responsible process every workspace job has, or why the daemon measured none;
/// and the gateway host a Developer Tools entry has to name.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct FirstLaunchReading {
    pub(crate) host: FirstLaunch,
    pub(crate) gateway: std::result::Result<FirstLaunch, String>,
    pub(crate) gateway_host: PathBuf,
    /// The command that restarts the gateway agent: a Developer Tools entry exempts only a
    /// gateway host started after it was added.
    pub(crate) restart: String,
}

/// Measure a first launch here, then ask the gateway to measure one. One after the other: the
/// scans of both would otherwise wait in one queue and charge each other.
pub(crate) async fn first_launch(roots: &CanonicalRoots) -> Result<FirstLaunchReading> {
    let gateway_host = HostStableExecutable::new(&canonical_home()?, COWSHED_BINARY_NAME)
        .map_err(launchd_error)?
        .gateway_host();
    // Under the gateway host, inside a workspace job, this process's launches are a job's: there
    // is no host-shell launch here to compare the gateway's with.
    if crate::first_launch::responsible_program().is_some_and(|program| program == gateway_host) {
        return Err(CowshedError::conflict(
            format!(
                "this doctor runs under the gateway host {}, as a workspace job does, so it has no host-shell launch to compare the gateway's with",
                gateway_host.display()
            ),
            "run cowshed doctor from a terminal, outside any workspace job",
        ));
    }
    let template = running_executable()?;
    let host = tokio::task::spawn_blocking(move || crate::first_launch::measure(&template))
        .await
        .map_err(|error| {
            CowshedError::internal(format!("the first-launch probe thread failed: {error}"))
        })?
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("could not measure a first launch in this shell: {error}"),
                "codesign --version",
            )
        })?;
    let client = GatewayControlClient::new(roots.control_socket()).map_err(control_error)?;
    let gateway = client
        .first_launch()
        .await
        .map_err(|error| error.to_string());
    Ok(FirstLaunchReading {
        host,
        gateway,
        gateway_host,
        restart: kickstart_hint(effective_uid(), &roots.namespace().launchd_label("gateway")),
    })
}
/// [`service_status`] for `doctor` (specs/cowshed/06_cli.md, "doctor"): a gateway that took the
/// request and then said nothing, not even a heartbeat, for the control silence bound is
/// [`Unanswered`] (05_gateway.md, "Deadlines").
pub(crate) async fn doctor_service_status(
    roots: &CanonicalRoots,
) -> Result<std::result::Result<CliGatewayStatus, Unanswered>> {
    let socket = roots.control_socket();
    observe_service(roots, |client| async move {
        match client.status().await {
            Ok(status) => Ok(Some(status)),
            Err(ControlError::Wait(WaitError::Silent(silent))) => Err(Unanswered {
                peer: "the gateway daemon",
                socket,
                within: silent.idle,
            }),
            Err(_) => Ok(None),
        }
    })
    .await
}

/// The gateway's status as launchd and the daemon report it, `ask` putting the questions to a
/// loaded daemon; what `ask` cannot answer for is its own error.
async fn observe_service<E, Asked>(
    roots: &CanonicalRoots,
    ask: impl FnOnce(GatewayControlClient) -> Asked,
) -> Result<std::result::Result<CliGatewayStatus, E>>
where
    Asked: Future<Output = std::result::Result<Option<DaemonStatus>, E>>,
{
    let home = canonical_home()?;
    let socket = roots.control_socket();
    let (executable, target) = gateway_launch_agent(roots.namespace(), &home)?;
    let pending_install = InstallSlot::observe(executable.support_directory())?;
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    let installed = matches!(
        executor
            .execute_status(&crate::launchd::ControlPlan::print(
                effective_uid(),
                &target
            ))
            .map_err(launchd_error)?,
        LaunchdServiceStatus::Loaded { .. }
    );
    let answered = if installed {
        let client = GatewayControlClient::new(socket.clone()).map_err(control_error)?;
        match ask(client).await {
            Ok(answered) => answered,
            Err(unanswered) => return Ok(Err(unanswered)),
        }
    } else {
        None
    };
    let (land_turns, land_turns_error) = observe_land_turns(&home);
    let scheduling = read_scheduling(roots.store()).await;
    let cli_sha256 = executable_digest(&running_executable()?)?;
    Ok(Ok(CliGatewayStatus {
        scheduling: Some(scheduling),
        pending_install,
        land_turns,
        land_turns_error,
        ..cli_status(installed, socket, answered.as_ref(), cli_sha256.as_str())
    }))
}

/// How far back `gateway status` lists tickets found abandoned: a day of lands.
const LAND_TURN_ABANDONED_WINDOW: Duration = Duration::from_secs(24 * 60 * 60);

/// The land queues on this host and the tickets found abandoned in the last day, read without
/// taking a turn. A queue that cannot be read is the status's error string, not a failed status:
/// the rest of the status still answers.
fn observe_land_turns(home: &Path) -> (Vec<LandTurnQueue>, Option<String>) {
    let since = std::time::SystemTime::now()
        .checked_sub(LAND_TURN_ABANDONED_WINDOW)
        .and_then(|since| since.duration_since(std::time::UNIX_EPOCH).ok())
        .map_or(0, |since| {
            u64::try_from(since.as_millis()).unwrap_or(u64::MAX)
        });
    match turns::observe_all(
        &turns::land_turn_root(&support_directory(home)),
        Some(since),
    ) {
        Ok(queues) => (
            queues
                .into_iter()
                .map(|turns| LandTurnQueue {
                    repo: turns.repo,
                    tickets: turns.tickets,
                    abandoned: turns.abandoned,
                })
                .collect(),
            None,
        ),
        Err(error) => (Vec::new(), Some(error.to_string())),
    }
}

/// The host's scheduling as `gateway status` reports it (05_gateway.md "Status"): the kernel's
/// readings, the live jobs every workspace supervisor lists, and the released images waiting in
/// the store's `.deleting` directories. All are read here, beside the daemon; none is asked of it.
async fn read_scheduling(store: &Path) -> HostScheduling {
    HostScheduling {
        readings: cowshed_core::host_readings::read(),
        jobs: read_jobs(store).await,
        image_cleanup: read_image_cleanup(store),
    }
}

/// The live jobs of every workspace supervisor serving in `store`, read from each supervisor's
/// own job list: the store's `run` directory holds one socket per supervisor
/// ([`supervisor_socket::socket_path`]). A socket nothing serves any more is no supervisor; one
/// that does not answer is named with why, and its jobs are in no count.
async fn read_jobs(store: &Path) -> HostJobs {
    let run = store.join("run");
    let sockets = match fs::read_dir(&run) {
        Ok(entries) => entries,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return HostJobs::default(),
        Err(error) => {
            return HostJobs {
                counts: Vec::new(),
                unanswered: vec![UnansweredSupervisor {
                    socket: run,
                    error: format!("the supervisor socket directory cannot be listed: {error}"),
                }],
            };
        }
    };
    let mut live = Vec::new();
    let mut unanswered = Vec::new();
    for entry in sockets {
        let path = match entry {
            Ok(entry) => entry.path(),
            Err(error) => {
                unanswered.push(UnansweredSupervisor {
                    socket: run.clone(),
                    error: format!("the supervisor socket directory cannot be listed: {error}"),
                });
                continue;
            }
        };
        if !is_supervisor_socket(store, &path) {
            continue;
        }
        match supervisor_jobs(&path).await {
            Ok(jobs) => live.extend(jobs.into_iter().map(|job| (job.class, job.state))),
            Err(error) => unanswered.push(UnansweredSupervisor {
                socket: path,
                error: error.to_string(),
            }),
        }
    }
    HostJobs {
        counts: count_jobs(live),
        unanswered,
    }
}

/// A workspace supervisor's own socket in the store's `run` directory: neither the membership
/// socket beside it ([`supervisor_socket::membership_socket_path`]) nor the supervisor manager's
/// ([`supervisor_manager::manager_socket_path`]).
fn is_supervisor_socket(store: &Path, path: &Path) -> bool {
    path.extension()
        .is_some_and(|extension| extension == "sock")
        && !path
            .file_stem()
            .is_some_and(|stem| Path::new(stem).extension().is_some())
        && path != supervisor_manager::manager_socket_path(store)
}

/// The jobs the supervisor serving `socket` lists, or none when nothing serves it any more.
async fn supervisor_jobs(socket: &Path) -> Result<Vec<cowshed_core::api::JobInfo>> {
    let Some(hello) = supervisor_socket::hello_if_present(socket).await? else {
        return Ok(Vec::new());
    };
    supervisor_socket::connect(socket.to_path_buf(), hello.authority)
        .list()
        .await
}

/// One row per class and state among `jobs` that are still live: queued, running or draining.
/// An ended job holds nothing the kernel schedules, so it is in no row.
fn count_jobs(jobs: impl IntoIterator<Item = (ExecClass, JobState)>) -> Vec<JobCount> {
    let mut counts: Vec<JobCount> = Vec::new();
    for (class, state) in jobs {
        if state.is_terminal() {
            continue;
        }
        match counts
            .iter_mut()
            .find(|count| count.class == class && count.state == state)
        {
            Some(count) => count.jobs += 1,
            None => counts.push(JobCount {
                class,
                state,
                jobs: 1,
            }),
        }
    }
    counts
}

/// The released images the store's `.deleting` directories hold for the gateway's cleanup, and
/// each directory the listing could not read.
fn read_image_cleanup(store: &Path) -> ImageCleanup {
    let listing = cowshed_core::storage::apfs::deleter::leftovers(store);
    ImageCleanup {
        waiting: listing.entries.len() as u64,
        unread: listing
            .left
            .into_iter()
            .filter_map(|left| match left.why {
                cowshed_core::storage::apfs::release::Why::Failed(error) => {
                    Some(format!("{}: {error}", left.path.display()))
                }
                _ => None,
            })
            .collect(),
    }
}

/// Refuse an install about to restart the running gateway while a land holds its repository's
/// turn (05_gateway.md "An install refuses while a land runs"); the decision is
/// [`agent_cycle::restart_blockers`]. It never waits for a land, and it runs before the install
/// copies, moves or restarts anything, so a refused install leaves the installed cowshed and the
/// running gateway as they were. `setup`'s storage repair, which runs before its install, is not
/// undone.
///
/// A restart between a land's steps refused its next step's connection. Checked under the install
/// slot, which every later land step waits for, so nothing this check did not see starts before
/// the restart. The land turns are read from their tickets without taking a turn; a queue that
/// cannot be read refuses too, since not knowing whether a land runs is no permission.
fn refuse_restart_while_busy(_slot: &InstallSlot, home: &Path) -> Result<()> {
    let root = turns::land_turn_root(&support_directory(home));
    let lands = turns::observe_all(&root, None).map_err(|error| {
        CowshedError::environment_missing(
            format!(
                "this install cannot tell whether a land runs, so it installed and restarted \
                 nothing: the land turns in {} cannot be read: {error}",
                root.display()
            ),
            format!("make {} readable, then rerun", root.display()),
        )
    })?;
    match agent_cycle::refusal(&agent_cycle::restart_blockers(&lands)) {
        Some(refusal) => Err(refusal),
        None => Ok(()),
    }
}

/// Queue this daemon's stderr lines for a writer thread of their own (13_telemetry.md, "The one
/// text-file survivor"), or say why they stay in place.
pub(crate) fn detach_stderr(daemon: &str) {
    if let Err(error) = cowshed_os::stderr::detach() {
        cowshed_os::eprintln!(
            "cowshed: the {daemon}'s stderr is written in place, so a stalled log stalls the {daemon}: {error}"
        );
    }
}

fn reap_scratch_from_now_on(store: PathBuf) {
    let started = std::thread::Builder::new()
        .name("scratch-reaper".to_owned())
        .spawn(move || {
            let mut reaper = match cowshed_os::scratch::Reaper::host() {
                Ok(reaper) => reaper,
                Err(error) => {
                    cowshed_os::eprintln!("cowshed: cannot start scratch reaper: {error}");
                    return;
                }
            };
            let mut trash = cowshed_os::scratch::TrashReaper::default();
            loop {
                reaper.sweep(&store, false).print();
                match trash.sweep(&store, std::time::SystemTime::now()) {
                    Ok(0) => {}
                    Ok(removed) => {
                        cowshed_os::eprintln!(
                            "cowshed: deleted {removed} expired scratch archive entries"
                        )
                    }
                    Err(error) => {
                        cowshed_os::eprintln!("cowshed: kept scratch archive entries: {error}")
                    }
                }
                std::thread::sleep(cowshed_os::scratch::INTERVAL);
            }
        });
    if let Err(error) = started {
        cowshed_os::eprintln!(
            "cowshed: scratch reaper could not start: {error}; cowshed gc still reaps scratch"
        );
    }
}

async fn run_daemon() -> Result<()> {
    // This thread runs the whole runtime: a log write the container stalls must not stall it.
    detach_stderr("gateway");
    let home = canonical_home()?;
    let storage = validate_existing_host_storage(&home).await?;
    let paths = GatewayPaths::from_storage(&storage);
    ensure_private_directory(&paths.cache_dir)?;
    ensure_private_directory(&paths.mirror_cache)?;
    ensure_private_directory(&paths.telemetry)?;
    let store_root = storage.store().to_path_buf();
    // Startup contract (05_gateway.md): validated store, then serve at once while every
    // recorded project's mounts are healed and the attached workspaces' sessions restored from
    // them. The gateway is RunAtLoad, so this pass is what closes the reboot window in which a
    // checkout path would otherwise dangle until something touched it. The projects are counted
    // before the control socket binds, so its first status already says how many are left, and
    // every request that depends on what the pass restores is refused by type until it is over.
    let heal_inventory = NativeGatewayInventory::new(storage.clone());
    let repositories = heal_inventory
        .recorded_projects()
        .await
        .unwrap_or_else(|error| {
            cowshed_os::eprintln!(
                "cowshed: could not list adopted projects at gateway startup: {error}"
            );
            Vec::new()
        });
    let heal = Arc::new(StartupHealState::mounting(repositories.len()));
    // The git credential helper is this same binary, which launchd started from the host-stable
    // path: a helper spawned by the daemon has to keep resolving for as long as the daemon runs.
    // Workspace supervisors are this binary too, for the same reason.
    let executable = running_executable()?;
    // Own the host's workspace supervisors (11_shell.md "Supervisor"). The ones still serving
    // from before this daemon are listed before the control socket binds, so its first status
    // already counts them, and recovered only once this daemon holds that socket: a second
    // daemon that fails to start never drains the first one's supervisors, nor heals a mount.
    let manager = SupervisorManager::new(
        &store_root,
        Box::new(ProgramSpawner::new(
            executable.clone(),
            vec![crate::workspace_supervisor::VERB.into()],
        )),
        Arc::clone(&heal),
    );
    let recovery = manager.recovery();
    let config = GatewayConfig {
        executable_sha256: Some(executable_digest(&executable)?.to_string()),
        startup: Some(Arc::new(DaemonStartup {
            heal: Arc::clone(&heal),
            manager: Arc::clone(&manager),
        })),
        nx_cache: Some(cowshed_gateway::NxCacheServiceConfig {
            script: HostStableExecutable::new(&home, "nx-cache-server")
                .map_err(launchd_error)?
                .path()
                .to_path_buf(),
            // The agent's PATH, which setup led with the directory of the bun it found.
            path: std::env::var_os("PATH").unwrap_or_default(),
            directory: paths.cache_dir.join("nx"),
            max_bytes: 1024 * 1024 * 1024,
            cap_bytes: 10 * 1024 * 1024 * 1024,
        }),
        // The RAM temp volume's disk is attached here, so no job owns its helper (05_gateway.md,
        // "RAM disks").
        #[cfg(target_os = "macos")]
        ram_disks: Some(Arc::new(cowshed_apfs::ram_disk::SystemRamDisks::new(
            storage.namespace(),
        ))),
        // Doctor's first-launch probe runs here, so its launches have the responsible process
        // every job has (05_gateway.md "The gateway host").
        #[cfg(target_os = "macos")]
        first_launch: Some(Arc::new(crate::first_launch::DaemonFirstLaunch::new(
            executable.clone(),
        ))),
        ..paths.config(effective_uid(), executable)
    };
    let telemetry = ArrowAuditConfig::new(paths.telemetry.clone())
        .map_err(|error| CowshedError::internal(format!("invalid gateway telemetry: {error}")))?;
    let gateway = Gateway::start_host(config, telemetry)
        .await
        .map_err(|error| CowshedError::internal(format!("could not start gateway: {error}")))?;
    // Detached and concurrent: each recovery reports its own span, status counts what is left,
    // and nothing below waits for it.
    drop(recovery.start(Arc::new(SocketPeers)));
    reap_scratch_from_now_on(store_root.clone());
    let handle = OwnedGateway::new(gateway.handle());
    let started = async {
        serve_supervisors(&store_root, manager).await?;
        // Both host sockets serve before the released-image cleanup lists what an earlier gateway
        // left (05_gateway.md, "Deleting released images"); it runs on a thread of its own.
        if let Err(error) = gateway.clean_up_released_images(store_root.clone()) {
            cowshed_os::eprintln!(
                "cowshed: released images wait in .deleting: this gateway's cleanup did not start: {error}"
            );
        }
        // Both host sockets are serving before storage recovery can wait on an owned process.
        // Only pre-existing positive authorizations resume; a refusal leaves that project
        // fenced and cannot prevent unrelated projects from healing.
        resume_project_retirements(&storage).await;
        let inventory = NativeSessionInventory::new(storage);
        heal_recorded_projects(&heal_inventory, repositories, &heal).await;
        heal_sccache_daemon().await;
        report_session_restore(&install_all_sessions(&inventory, &handle).await?);
        heal.restored();
        // Every session that will be restored is: a carried listener none claimed would queue
        // its clients for nobody.
        match gateway.handle().release_unclaimed_listeners().await {
            Ok(0) => {}
            Ok(released) => cowshed_os::eprintln!("cowshed: closed {released} carried workspace listener(s) no restored session claimed"),
            Err(error) => cowshed_os::eprintln!("cowshed: could not close the unclaimed carried listeners: {error}"),
        }
        Ok::<(), CowshedError>(())
    }
    .await;
    if let Err(primary) = started {
        return match gateway.drain().await {
            Ok(()) => Err(primary),
            Err(error) => Err(CowshedError::internal(format!(
                "{}; gateway drain also failed: {error}",
                primary.message
            ))),
        };
    }

    drain_after_shutdown(gateway, wait_for_shutdown_signal()).await
}

/// The daemon's startup pass, as the gateway's status and its session changes see it: the
/// mounts it still heals and the supervisors it still recovers. Like [`ControlSocket`], this
/// lands in the composition root, the one place holding the gateway and both halves.
struct DaemonStartup {
    heal: Arc<StartupHealState>,
    manager: Arc<SupervisorManager>,
}

impl std::fmt::Debug for DaemonStartup {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("DaemonStartup")
            .field("healing", &self.healing())
            .field("recovering", &self.recovering())
            .finish()
    }
}

impl StartupProbe for DaemonStartup {
    fn healing(&self) -> Option<StartupHeal> {
        self.heal.current()
    }

    fn recovering(&self) -> Option<SupervisorRecovery> {
        self.manager.recovering()
    }
}

/// Take ensures on the manager socket from the daemon's first moment: an ensure is refused by
/// type while the startup pass still heals, and for a workspace whose supervisor from before
/// this daemon is still being recovered.
async fn serve_supervisors(store_root: &Path, manager: Arc<SupervisorManager>) -> Result<()> {
    let listener =
        supervisor_socket::bind(&supervisor_manager::manager_socket_path(store_root)).await?;
    tokio::spawn(async move {
        if let Err(error) = supervisor_manager::serve(listener, manager).await {
            cowshed_os::eprintln!(
                "cowshed: workspace supervisors are unavailable: {}",
                error.message
            );
        }
    });
    Ok(())
}

/// Resume every explicit project retirement an earlier run recorded and left unfinished,
/// reporting rather than raising.
///
/// Only a durable, positive `--abandon` record is resumed (`cowshed forget`): a missing checkout,
/// a dead pid or an old registration authorizes nothing, and nothing here writes a record. A
/// retirement that cannot finish stays fenced for the operator to retry, and it must never stop
/// the gateway from healing and serving every other project (05_gateway.md).
async fn resume_project_retirements(storage: &ValidatedHostStorage) {
    let resumed =
        cowshed_core::project_retirement::native::resume_retirements(storage.clone()).await;
    for line in retirement_resume_lines(&resumed) {
        cowshed_os::eprintln!("{line}");
    }
}

/// What the startup pass says about the retirements it resumed: one line for each project, and
/// after each failure the message and hint the CLI would have printed for it.
fn retirement_resume_lines(
    resumed: &Result<Vec<(RepoId, Result<ProjectRetirementReport>)>>,
) -> Vec<String> {
    fn failure(lines: &mut Vec<String>, what: String, error: &CowshedError) {
        lines.push(format!("cowshed: {what}: {}", error.message));
        if !error.hint.is_empty() {
            lines.push(format!("next: {}", error.hint));
        }
    }

    let mut lines = Vec::new();
    match resumed {
        Err(error) => failure(
            &mut lines,
            "could not read the recorded project retirements at gateway startup".to_owned(),
            error,
        ),
        Ok(outcomes) => {
            for (repo_id, outcome) in outcomes {
                match outcome {
                    Ok(report) => lines.push(format!(
                        "cowshed: resumed at gateway startup: {}",
                        crate::forget::retired(report)
                    )),
                    Err(error) => failure(
                        &mut lines,
                        format!(
                            "could not finish retiring {repo_id} at gateway startup; it stays fenced"
                        ),
                        error,
                    ),
                }
            }
        }
    }
    lines
}

/// Heal every one of `repositories`' mains, counting each one down in `heal` and reporting rather
/// than raising. Sessions attach on use, not here (05_gateway.md "Startup contract").
///
/// A project that cannot be healed is a finding for `cowshed doctor`; it must never stop the
/// gateway from serving the healthy ones (05_gateway.md). An unmounted main is the user's own
/// checkout missing from their shell and editor, which is why `doctor` reports it as critical
/// and this line carries the remedy with it.
async fn heal_recorded_projects(
    inventory: &NativeGatewayInventory,
    repositories: Vec<RepoId>,
    heal: &StartupHealState,
) {
    for outcome in inventory.heal(repositories, heal).await {
        if let Err(error) = &outcome.main {
            cowshed_os::eprintln!(
                "cowshed: {}: main checkout is not mounted after gateway startup: {error}",
                outcome.repo_id
            );
            cowshed_os::eprintln!("next: cowshed doctor");
        }
    }
}

/// Bring the compile cache up with the gateway, reporting rather than raising.
///
/// The daemon is part of the host's serving posture, not an opt-in: a workspace shell exports
/// `SCCACHE_SERVER_UDS` unconditionally and a client that finds nothing there either compiles
/// uncached or tries to bind the socket itself, which the store-wide sandbox deny refuses. Starting
/// it here is also what re-establishes it after a reboot, since the sccache agent is only ever
/// installed by a cowshed that could resolve the sccache binary on PATH.
///
/// A host without sccache installed is not a broken host, so failure is a log line: the gateway's
/// job is to serve, and every workspace works without a compile cache.
async fn heal_sccache_daemon() {
    if let Err(error) = crate::capabilities::sccache::service::start_service(None).await {
        cowshed_os::eprintln!(
            "cowshed: could not start the sccache daemon at startup: {}",
            error.message
        );
    }
}

/// Name every session the startup pass left out, with its typed reason. The daemon serves on:
/// a workspace whose base port another process took is that workspace's problem, and the
/// project's next reconcile moves it to a free block (05_gateway.md, port block).
fn report_session_restore(report: &SessionRestoreReport) {
    for failure in &report.refused {
        cowshed_os::eprintln!(
            "cowshed: {}: gateway session {} at {} was not restored at startup: {}",
            failure.repo_id,
            failure.workspace_id,
            failure.endpoint,
            failure.refusal
        );
    }
    if !report.refused.is_empty() {
        cowshed_os::eprintln!("next: cowshed doctor");
    }
}

async fn wait_for_shutdown_signal() -> Result<()> {
    let mut terminate = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .map_err(|error| {
        CowshedError::internal(format!("could not install SIGTERM handler: {error}"))
    })?;
    let interrupt = tokio::signal::ctrl_c();
    tokio::pin!(interrupt);
    tokio::select! {
        _ = terminate.recv() => Ok(()),
        result = &mut interrupt => result.map_err(|error| {
            CowshedError::internal(format!("could not install SIGINT handler: {error}"))
        }),
    }
}

/// What `gateway status` reports, from launchd's answer, the daemon's own answer, and the digest
/// of this CLI's executable. A daemon that answers is healthy only when it is not draining, has
/// finished its startup pass, and runs the same bytes as the CLI asking.
fn cli_status(
    installed: bool,
    socket: PathBuf,
    status: Option<&DaemonStatus>,
    cli_sha256: &str,
) -> CliGatewayStatus {
    CliGatewayStatus {
        installed,
        running: status.is_some(),
        socket,
        cli_version: env!("CARGO_PKG_VERSION").to_owned(),
        daemon_version: status.map(|status| status.version.clone()),
        active_workspaces: status.map_or(0, |status| status.sessions.len() as u64),
        drain_cause: status.and_then(|status| {
            status.draining.then(|| {
                status
                    .drain_cause
                    .clone()
                    .unwrap_or_else(|| "the daemon reports draining without a cause".to_owned())
            })
        }),
        healing: status.and_then(|status| status.healing),
        recovering: status.and_then(|status| status.recovering),
        stale_daemon: status.and_then(|status| {
            (status.executable_sha256.as_deref() != Some(cli_sha256)).then(|| StaleDaemonBinary {
                daemon_sha256: status.executable_sha256.clone(),
                cli_sha256: cli_sha256.to_owned(),
            })
        }),
        // The host's scheduling is read beside the daemon, not asked of it.
        scheduling: None,
        // The slot is observed beside the daemon, not asked of it.
        pending_install: false,
        // The land queues are read beside the daemon, not asked of it.
        land_turns: Vec::new(),
        land_turns_error: None,
    }
}

/// The remedy for a daemon running other bytes than the CLI: `setup` installs one build as the
/// CLI on `PATH` and the daemon together, and restarts a daemon that runs another.
const STALE_DAEMON_REMEDY: &str = "cowshed setup";

/// SHA-256 of an executable's contents.
pub(crate) fn executable_digest(path: &Path) -> Result<ExecutableDigest> {
    let mut file = open_for_compare(path)?;
    let mut hasher = Sha256::new();
    let mut chunk = vec![0u8; COMPARE_CHUNK_BYTES];
    loop {
        let read = fill(&mut file, &mut chunk).map_err(|error| compare_error(path, error))?;
        if read == 0 {
            break;
        }
        hasher.update(&chunk[..read]);
    }
    Ok(ExecutableDigest::from_bytes(&hasher.finalize().into()))
}

pub fn emit_gateway_status<W: Write, E: Write>(
    output: &mut Output<W, E>,
    json: bool,
    status: CliGatewayStatus,
) -> Result<()> {
    if json {
        output.success(status).map_err(output_error)?;
        return Ok(());
    }
    let state = if let Some(cause) = &status.drain_cause {
        format!(
            "gateway is draining and refuses new sessions: {cause}; it exits once its in-flight work ends, and launchd restarts it"
        )
    } else if let Some(stale) = &status.stale_daemon {
        format!(
            "gateway runs a different cowshed binary than this CLI (daemon sha256 {}, cli sha256 {}); replace it: {STALE_DAEMON_REMEDY}",
            stale.daemon_sha256.as_deref().unwrap_or("unreported"),
            stale.cli_sha256
        )
    } else if let Some(heal) = &status.healing {
        format!(
            "gateway answers at {} but is still starting: {heal}; a command that needs a workspace is refused until it has finished",
            status.socket.display(),
        )
    } else if let Some(recovery) = &status.recovering {
        format!(
            "gateway serves at {}, and is still recovering {} workspace supervisors from before it started; a command for one of their workspaces is refused until its supervisor is recovered",
            status.socket.display(),
            recovery.supervisors
        )
    } else if status.running {
        format!(
            "gateway is healthy: launchd loaded; control socket answers at {}",
            status.socket.display()
        )
    } else if status.installed {
        format!(
            "gateway is installed but its control socket does not answer at {}",
            status.socket.display()
        )
    } else {
        format!(
            "gateway is not installed; no control socket answers at {}",
            status.socket.display()
        )
    };
    output.guidance(&state).map_err(output_error)?;
    output
        .guidance(&format!(
            "gateway versions: cli {}; daemon {}",
            status.cli_version,
            status.daemon_version.as_deref().unwrap_or("unavailable")
        ))
        .map_err(output_error)?;
    if status.pending_install {
        output
            .guidance(
                "gateway install pending: an install holds the install slot (dev.cowshed/install.lock)",
            )
            .map_err(output_error)?;
    }
    for queue in &status.land_turns {
        if !queue.tickets.is_empty() {
            output
                .guidance(&land_turn_line(queue))
                .map_err(output_error)?;
        }
        for abandoned in &queue.abandoned {
            output
                .guidance(&format!(
                    "land turn {}: {} (pid {}) was abandoned, found {} after asking at {}: {}",
                    queue.repo,
                    abandoned.ticket.workspace,
                    abandoned.ticket.pid,
                    local_clock(abandoned.at_unix_ms),
                    local_clock(abandoned.ticket.asked_unix_ms),
                    abandoned
                        .outcome
                        .as_deref()
                        .unwrap_or("no outcome recorded")
                ))
                .map_err(output_error)?;
        }
    }
    if let Some(error) = &status.land_turns_error {
        output
            .guidance(&format!("gateway land turns unavailable: {error}"))
            .map_err(output_error)?;
    }
    for line in status.scheduling.iter().flat_map(scheduling_lines) {
        output.guidance(&line).map_err(output_error)?;
    }
    Ok(())
}

/// One repository's land queue in the operator's words: the ticket holding its turn, then the
/// tickets waiting behind it, oldest first.
fn land_turn_line(queue: &LandTurnQueue) -> String {
    let Some((holder, waiting)) = queue.tickets.split_first() else {
        return format!("land turn {}: no live ticket", queue.repo);
    };
    let holds = format!(
        "{} holds it (pid {}, asked {})",
        holder.workspace,
        holder.pid,
        local_clock(holder.asked_unix_ms)
    );
    if waiting.is_empty() {
        return format!("land turn {}: {holds}", queue.repo);
    }
    let behind = waiting
        .iter()
        .map(|ticket| format!("{} (pid {})", ticket.workspace, ticket.pid))
        .collect::<Vec<_>>()
        .join(", ");
    format!("land turn {}: {holds}; waiting: {behind}", queue.repo)
}

/// The host's scheduling in the operator's words (05_gateway.md "Status"): load, memory pressure
/// and swap as the kernel reports them, the live jobs by class and state, then the released images
/// waiting for cleanup. A reading that failed says why; it never prints as zero.
fn scheduling_lines(scheduling: &HostScheduling) -> Vec<String> {
    let readings = &scheduling.readings;
    let mut lines = vec![
        match &readings.load {
            LoadReading::Read { load } => format!(
                "gateway host load: {:.2} {:.2} {:.2} (1, 5 and 15 minutes)",
                load.one.get(),
                load.five.get(),
                load.fifteen.get()
            ),
            LoadReading::Unread { error } => format!("gateway host load unread: {error}"),
        },
        match &readings.memory_pressure {
            MemoryPressureReading::Read { pressure } => {
                format!("gateway host memory pressure: {pressure}")
            }
            MemoryPressureReading::Unread { error } => {
                format!("gateway host memory pressure unread: {error}")
            }
        },
        match &readings.swap {
            SwapReading::Read { swap } => format!(
                "gateway host swap: {} MiB of {} MiB in use",
                swap.used_bytes().get() >> 20,
                swap.total_bytes().get() >> 20
            ),
            SwapReading::Unread { error } => format!("gateway host swap unread: {error}"),
        },
    ];
    let jobs = &scheduling.jobs;
    let counted = jobs
        .counts
        .iter()
        .map(|count| {
            format!(
                "{} {} {}",
                wire_word(count.class),
                wire_word(count.state),
                count.jobs
            )
        })
        .collect::<Vec<_>>();
    let counted = match (counted.is_empty(), jobs.unanswered.len()) {
        (true, 0) => "none live".to_owned(),
        (true, unread) => format!("none counted; {unread} supervisors unread"),
        (false, 0) => counted.join(", "),
        (false, unread) => format!("{}; {unread} supervisors unread", counted.join(", ")),
    };
    lines.push(format!("gateway jobs: {counted}"));
    lines.extend(jobs.unanswered.iter().map(|unanswered| {
        format!(
            "gateway jobs unread: {}: {}",
            unanswered.socket.display(),
            unanswered.error
        )
    }));
    lines.push(format!(
        "gateway image cleanup: {} released images waiting",
        scheduling.image_cleanup.waiting
    ));
    lines.extend(
        scheduling
            .image_cleanup
            .unread
            .iter()
            .map(|unread| format!("gateway image cleanup unread: {unread}")),
    );
    lines
}

/// A class or a state as the wire spells it, so the status line and the JSON result name it alike.
fn wire_word(value: impl serde::Serialize) -> String {
    match serde_json::to_value(value) {
        Ok(serde_json::Value::String(word)) => word,
        other => unreachable!("a unit variant serializes to its wire name, not {other:?}"),
    }
}

/// The build an install would put in place: its bytes, what they hash to, and what the build
/// says of itself.
///
/// The digest is the stored copy's name, so it is taken once, from the bytes about to be copied,
/// and carried through the install rather than read again.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Candidate {
    pub source: PathBuf,
    pub digest: ExecutableDigest,
    pub record: Option<BuildRecord>,
}

impl Candidate {
    pub fn of(source: PathBuf, record: Option<BuildRecord>) -> Result<Self> {
        Ok(Self {
            digest: executable_digest(&source)?,
            source,
            record,
        })
    }
}

/// Whether an install may replace the installed cowshed with a build that does not contain it.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Downgrade {
    Refuse,
    Allow,
}

/// Where `@smoothbricks/nx-cache-server` keeps its server: one JavaScript bundle (its `bundle`
/// target), the same on every platform, run by the gateway's `bun` (the owner's ruling,
/// 2026-10-18).
const NX_CACHE_SERVER_BUNDLE: &str =
    "node_modules/@smoothbricks/nx-cache-server/dist/server/nx-cache-server.js";

/// The Nx cache server bundle that belongs to the cowshed build at `cowshed_source`.
///
/// A build the install slot holds (`installed`'s directory) has its server beside it, where its
/// install put it. Any other build is a distribution, `<package>/dist/bin/<platform>/cowshed`,
/// and its server is the bundle of the package's `@smoothbricks/nx-cache-server` dependency,
/// found the way Node resolves a dependency: `node_modules` in the package and then in each
/// ancestor. That covers an isolated Bun store, a hoisted install and a checkout's workspace link
/// alike. Neither package carries a compiled server: four `bun build --compile` binaries put
/// cowshed's tarball over npm's ceiling (Publish run 39150196802), and an interpreter is the
/// shell's to provide (the owner's ruling, 2026-10-17).
fn packaged_nx_cache_server(
    installed: &HostStableExecutable,
    cowshed_source: &Path,
) -> Result<PathBuf> {
    if cowshed_source.parent() == Some(installed.directory()) {
        return Ok(installed.path().to_path_buf());
    }
    let missing = |searched: String| {
        CowshedError::environment_missing(
            format!(
                "the cowshed build {} has no Nx cache server artifact: {searched}",
                cowshed_source.display()
            ),
            "install @smoothbricks/cowshed with its dependencies (in a checkout, build nx-cache-server), then run that cowshed's setup",
        )
    };
    let mut layout = cowshed_source.ancestors().skip(2);
    let (Some(bin), Some(dist), Some(package)) = (layout.next(), layout.next(), layout.next())
    else {
        return Err(missing(
            "it is not inside a package's dist/bin/<platform>".to_owned(),
        ));
    };
    if bin.file_name() != Some("bin".as_ref()) || dist.file_name() != Some("dist".as_ref()) {
        return Err(missing(
            "it is not inside a package's dist/bin/<platform>".to_owned(),
        ));
    }
    let artifact = Path::new(NX_CACHE_SERVER_BUNDLE);
    package
        .ancestors()
        .map(|directory| directory.join(artifact))
        .find(|candidate| candidate.is_file())
        .ok_or_else(|| {
            missing(format!(
                "no {} in {} or any ancestor",
                artifact.display(),
                package.display()
            ))
        })
}

/// Install the exact server artifact that belongs to this cowshed build
/// ([`packaged_nx_cache_server`]). Never resolve an ambient PATH server or a separate live
/// workspace at daemon startup.
fn install_nx_cache_server<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    home: &Path,
    cowshed_source: &Path,
) -> Result<HostStableExecutable>
where
    F: LaunchdFilesystem,
    C: LaunchctlCommand,
{
    let executable = HostStableExecutable::new(home, "nx-cache-server").map_err(launchd_error)?;
    let source = packaged_nx_cache_server(&executable, cowshed_source)?;
    let retained = retain_previous_executable(executor, &executable)?;
    let digest = executable_digest(&source)?;
    install_stored_executable(executor, &executable, &digest, &source)?;
    prune_superseded(executor, &executable, &digest, retained.as_deref());
    Ok(executable)
}

/// Install `source` as the host's cowshed: a copy of its bytes named for their digest, and the
/// stable name launchd runs and the operator's `PATH` reaches as a link to that copy.
///
/// The plist names the stable name on the volume that carries the plist itself, so the agent
/// starts after the build that installed it is gone. The source may live in a workspace or the
/// nix store: those paths are unreadable at boot, but the copy is not. A copy is written once and
/// never rewritten, so what runs from it is exactly those bytes for as long as it exists, and
/// the stable name moves from one whole build to the next in one rename.
pub fn install_host_stable_executable<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    home: &Path,
    source: &Path,
) -> Result<HostStableExecutable>
where
    F: LaunchdFilesystem,
    C: LaunchctlCommand,
{
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    remove_planted_cargo_link(executor, &executable)?;
    install_stored_executable(executor, &executable, &executable_digest(source)?, source)?;
    Ok(executable)
}

/// Install the stored copy of `source` and point the stable name at it: `NoChange` when the stable
/// name already ran it.
fn install_stored_executable<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    executable: &HostStableExecutable,
    digest: &ExecutableDigest,
    source: &Path,
) -> Result<InstallOutcome>
where
    F: LaunchdFilesystem,
    C: LaunchctlCommand,
{
    let state = observe_executable_install(executable, digest, source)?;
    executor
        .execute_install(&plan_executable_install(executable, digest, source, &state))
        .map_err(launchd_error)
}

/// This user's cowshed signing identity, made in cowshed's own keychain in `home` when it holds
/// none ([`code_signing::prepare`], [`code_signing::ensure_identity`]).
fn gateway_host_identity(home: &Path) -> Result<SigningIdentity> {
    let keychain = Keychain::cowshed(home);
    code_signing::prepare(&keychain)?;
    code_signing::ensure_identity(&keychain)
}

/// Install the gateway host beside `executable` from `source`, signed with `identity`
/// (05_gateway.md "The gateway host"): when it is absent, when it does not satisfy `identity`'s
/// designated requirement, or when `moved` says this install moved the stable name to another
/// build. Otherwise it is left as it is. A Developer Tools entry holds the host's path and
/// requirement, and a rewrite keeps both, so the entry outlives every install.
fn install_gateway_host<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    executable: &HostStableExecutable,
    source: &Path,
    identity: SigningIdentity,
    moved: bool,
) -> Result<()>
where
    F: LaunchdFilesystem,
    C: LaunchctlCommand,
{
    let host = executable.gateway_host();
    let current = match inspect_existing(&host)? {
        None => false,
        Some(metadata) if is_user_owned(&metadata, false) => {
            !moved
                && executor.satisfies(&host, identity).map_err(|error| {
                    CowshedError::environment_missing(
                        format!(
                            "could not check the gateway host {}'s signature: {error}",
                            host.display()
                        ),
                        format!("codesign --verify --verbose '{}'", host.display()),
                    )
                })?
        }
        Some(_) => {
            return Err(CowshedError::integrity(
                format!(
                    "the gateway host is not a user-owned regular file: {}",
                    host.display()
                ),
                "remove it and rerun cowshed setup",
            ));
        }
    };
    if !current {
        executor
            .execute_install(&plan_gateway_host_install(executable, source, identity))
            .map_err(launchd_error)?;
    }
    Ok(())
}

/// Remove the `cargo` link an earlier install planted beside the stable name. cowshed no longer
/// answers as `cargo` — a job's jobserver reaches Cargo as the make jobserver configuration Cargo
/// reads itself (05_gateway.md, "Parallelism inside a job") — so that link would make `cargo` on a `PATH` holding
/// the directory run the cowshed CLI. Anything else named `cargo` there is not cowshed's: it is
/// refused by its path and what it is, and left as found.
fn remove_planted_cargo_link<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    executable: &HostStableExecutable,
) -> Result<()>
where
    F: LaunchdFilesystem,
    C: LaunchctlCommand,
{
    match observe_cargo_link(executable)? {
        CargoLink::Absent => Ok(()),
        CargoLink::Planted => {
            executor
                .execute_install(&plan_cargo_link_removal(executable.directory()))
                .map_err(launchd_error)?;
            Ok(())
        }
        CargoLink::Foreign(found) => Err(CowshedError::integrity(
            format!(
                "{} is {}, not the `cargo` link an earlier cowshed install planted, and cowshed removes nothing else",
                executable.directory().join(CARGO_LINK_NAME).display(),
                found.describe()
            ),
            "move it out of that directory and rerun the command",
        )),
    }
}

/// What `cargo` in the host-stable directory is.
#[derive(Debug, Eq, PartialEq)]
enum CargoLink {
    Absent,
    /// The link an earlier install planted: a symbolic link this user owns saying, relative to the
    /// directory, the stable name or a stored copy of an executable cowshed installs there.
    Planted,
    /// Anything else, which is not cowshed's to remove.
    Foreign(ForeignCargo),
}

/// A `cargo` in the host-stable directory that no cowshed install planted.
#[derive(Debug, Eq, PartialEq)]
enum ForeignCargo {
    NotOwned { uid: u32 },
    Link(PathBuf),
    File,
    Other,
}

impl ForeignCargo {
    fn describe(&self) -> String {
        match self {
            Self::NotOwned { uid } => format!("owned by uid {uid}, not this user"),
            Self::Link(target) => format!("a link saying {}", target.display()),
            Self::File => "a regular file".to_owned(),
            Self::Other => "neither a regular file nor a link".to_owned(),
        }
    }
}

/// The executables whose names an install gave the `cargo` link it planted: the link said the
/// executable an install had just written, first as its stored copy and then as its stable name,
/// and the Nx cache server's install left it saying its own until the link was pinned to
/// `cowshed`.
const CARGO_LINK_PLANTERS: [&str; 2] = [COWSHED_BINARY_NAME, "nx-cache-server"];

fn observe_cargo_link(executable: &HostStableExecutable) -> Result<CargoLink> {
    let path = executable.directory().join(CARGO_LINK_NAME);
    let Some(metadata) = inspect_existing(&path)? else {
        return Ok(CargoLink::Absent);
    };
    if metadata.uid() != effective_uid() {
        return Ok(CargoLink::Foreign(ForeignCargo::NotOwned {
            uid: metadata.uid(),
        }));
    }
    if metadata.is_file() {
        return Ok(CargoLink::Foreign(ForeignCargo::File));
    }
    if !metadata.file_type().is_symlink() {
        return Ok(CargoLink::Foreign(ForeignCargo::Other));
    }
    let target = fs::read_link(&path).map_err(|error| {
        CowshedError::internal(format!(
            "could not read the link {}: {error}",
            path.display()
        ))
    })?;
    Ok(if planted_cargo_target(executable.home(), &target)? {
        CargoLink::Planted
    } else {
        CargoLink::Foreign(ForeignCargo::Link(target))
    })
}

/// `target` is what a `cargo` link a cowshed install planted says: one name, relative to the
/// directory, that is the stable name or a stored copy of one of [`CARGO_LINK_PLANTERS`].
fn planted_cargo_target(home: &Path, target: &Path) -> Result<bool> {
    let mut components = target.components();
    let (Some(std::path::Component::Normal(name)), None) = (components.next(), components.next())
    else {
        return Ok(false);
    };
    for planter in CARGO_LINK_PLANTERS {
        let planter = HostStableExecutable::new(home, planter).map_err(launchd_error)?;
        if name == planter.name() || planter.stored_digest(name).is_some() {
            return Ok(true);
        }
    }
    Ok(false)
}

/// Where the provenance of the installed cowshed is written down.
///
/// Beside the binary, on the volume that also carries the plists: whatever launchd can read the
/// agent definition from, an operator can read this from too. It exists because a supervised
/// binary with no recorded origin is unanswerable — "which build is this host running" had, until
/// this file, no answer other than a size comparison against a checkout that may have moved on.
const INSTALLED_SOURCE_RECORD: &str = "cowshed-source";

/// Refuse to hand launchd a build that must not be supervised.
///
/// This exists because it happened: a `setup` run from `target/debug/cowshed` copied a 94 MB debug
/// build over this host's supervised binary and then failed on `launchctl kickstart` (exit 37),
/// leaving the host with a debug gateway and no loaded agent. Nothing checked, nothing recorded,
/// nothing rolled back.
///
/// A debug build is not merely large. It carries `debug_assertions`, so it aborts on states a
/// release build tolerates — under a `KeepAlive` agent that is a respawn loop — and it is
/// invariably someone's scratch checkout, which is exactly what a host-stable copy exists to stop
/// depending on. Fail closed, with no escape hatch: a developer who genuinely means to supervise
/// their own build passes `--release`, and that one flag is the difference between "I meant this"
/// and "this is what happened to be running".
///
/// `debug_build` is a parameter rather than a direct `cfg!` read because every test binary is
/// itself a debug build and could otherwise never observe the accepting arm. The single production
/// caller passes `cfg!(debug_assertions)`, which is the compile-time truth about the binary
/// executing this line — unspoofable by a path, a name, or a size, and free, where scanning 90 MB
/// for `target/debug` strings is both slower and a heuristic.
pub fn refuse_unsupervisable_build(source: PathBuf, debug_build: bool) -> Result<PathBuf> {
    if debug_build {
        return Err(CowshedError::conflict(
            format!(
                "{} is a debug build (compiled with debug assertions) and will not be installed \
                 as this host's supervised binary",
                source.display()
            ),
            format!(
                "build a release binary with `nx run cowshed:{RELEASE_CLI_TARGET}:production` \
                 (or `cargo build --release -p cowshed-cli`) and run this from it; \
                 `nx run cowshed:build` and `{RELEASE_CLI_TARGET}`'s default configuration \
                 build a debug one"
            ),
        ));
    }
    Ok(source)
}

/// The package's Nx target that builds this host's `cowshed`; its `production` configuration is
/// the release build. One per platform the package ships (`napi.targets`).
pub const RELEASE_CLI_TARGET: &str =
    match (cfg!(target_os = "macos"), cfg!(target_arch = "aarch64")) {
        (true, true) => "cli-arm64-macos",
        (true, false) => "cli-x64-macos",
        (false, true) => "cli-arm64-linux",
        (false, false) => "cli-x64-linux",
    };

/// The running build, refused when it is unfit for launchd to supervise.
fn supervisable_running_executable() -> Result<PathBuf> {
    refuse_unsupervisable_build(running_executable()?, cfg!(debug_assertions))
}

/// Write down which build was installed, so "what is this host supervising" has an answer.
///
/// Best effort: an install that succeeded is not undone because a note could not be written. The
/// failure is still said out loud, because what it costs is the next operator guessing.
fn record_installed_source(executable: &HostStableExecutable, source: &Path) {
    let record = executable.support_directory().join(INSTALLED_SOURCE_RECORD);
    let contents = format!(
        "{}\ncowshed {}\n",
        source.display(),
        env!("CARGO_PKG_VERSION")
    );
    if let Err(error) = fs::write(&record, contents) {
        cowshed_os::eprintln!(
            "cowshed: could not record the installed cowshed path in {}: {error}",
            record.display()
        );
    }
}

/// What a stored copy's build record is named: the copy's own name and this suffix.
const BUILD_RECORD_SUFFIX: &str = ".build";

/// Where the build record of the stored copy of `digest` is written, beside the copy.
fn build_record_path(executable: &HostStableExecutable, digest: &ExecutableDigest) -> PathBuf {
    let mut path = executable.stored(digest).into_os_string();
    path.push(BUILD_RECORD_SUFFIX);
    PathBuf::from(path)
}

/// Write down which build the stored copy is, so a later install can tell whether it would take
/// the installed build's changes off the host ([`check_basis`]). A build that records nothing
/// writes nothing: the copy then has no record, and a copy with none is never refused a
/// replacement.
///
/// Best effort like [`record_installed_source`], and said out loud for the same reason.
fn record_installed_build(executable: &HostStableExecutable, candidate: &Candidate) {
    let Some(record) = &candidate.record else {
        return;
    };
    let path = build_record_path(executable, &candidate.digest);
    let written = serde_json::to_vec(record)
        .map_err(io::Error::other)
        .and_then(|bytes| fs::write(&path, bytes));
    if let Err(error) = written {
        cowshed_os::eprintln!(
            "cowshed: could not record the installed build in {}: {error}; a later setup cannot \
         tell whether a build it installs is older",
            path.display()
        );
    }
}

/// The build the stable name runs, and what it recorded of itself.
struct InstalledBuild {
    digest: ExecutableDigest,
    record: Option<BuildRecord>,
}

/// What the stable name runs now. `None` when nothing is installed, or the install is one from
/// before copies were content-addressed, which records nothing.
fn installed_build(executable: &HostStableExecutable) -> Result<Option<InstalledBuild>> {
    let Some(metadata) = inspect_existing(executable.path())? else {
        return Ok(None);
    };
    if !metadata.file_type().is_symlink() {
        return Ok(None);
    }
    let target = fs::read_link(executable.path()).map_err(|error| {
        CowshedError::internal(format!(
            "could not read the link {}: {error}",
            executable.path().display()
        ))
    })?;
    let Some(digest) = target
        .file_name()
        .and_then(|name| executable.stored_digest(name))
    else {
        return Ok(None);
    };
    let path = build_record_path(executable, &digest);
    let record = match fs::read(&path) {
        Ok(bytes) => Some(serde_json::from_slice(&bytes).map_err(|error| {
            CowshedError::integrity(
                format!(
                    "the installed cowshed's build record {} is unreadable: {error}",
                    path.display()
                ),
                "rewrite it as the installed build's {\"commit\",\"commitTime\"}, or run `cowshed \
                 setup --downgrade` from the build to install it without judging it",
            )
        })?),
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(CowshedError::internal(format!(
                "could not read {}: {error}",
                path.display()
            )));
        }
    };
    Ok(Some(InstalledBuild { digest, record }))
}

/// Take the install slot for `home`: one install at a time (`install_basis`). Every caller takes
/// it before it reads what is installed, so the basis it judges against is whatever the install
/// it waited for left behind.
pub(crate) async fn take_install_slot(home: &Path) -> Result<InstallSlot> {
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    ensure_private_directory(executable.support_directory())?;
    InstallSlot::take(executable.support_directory()).await
}

/// The checkout of every adopted project's main, read for the host store: where a build's commit
/// is judged.
pub(crate) async fn adopted_mains(home: &Path) -> Result<install_basis::Mains> {
    let storage = validate_existing_host_storage(home).await?;
    let store = std::sync::Arc::clone(storage.roots().shared_store());
    let projects = NativeGatewayInventory::new(storage)
        .adopted_projects()
        .await
        .map_err(|error| {
            CowshedError::environment_missing(
                format!("could not enumerate adopted mains: {error}"),
                "repair the adopted-project records, then rerun the install",
            )
        })?;
    Ok(install_basis::Mains {
        store,
        roots: projects
            .into_iter()
            .map(|project| project.project_root)
            .collect(),
    })
}

/// Refuse to replace the installed cowshed with a build that does not contain it, unless
/// `downgrade` allows it. Runs under the install slot ([`take_install_slot`]).
///
/// This exists because a guard that compared commit *times* let a sibling committed later
/// replace a build carrying a gc fix it lacked, and said nothing. What decides now is whether
/// the candidate holds every change of the installed commit, judged in main
/// ([`install_basis::judge`]).
///
/// What is installed decides whether there is anything to judge. Nothing installed, or a plain
/// binary of the layout before copies were content-addressed, is a host with nothing to lose: it
/// installs. A stored copy with no `.build` record beside it was installed before records
/// existed, or by `--downgrade` from a build that records nothing; it names no commit to keep,
/// so it installs too. A record that is there but unreadable is `Integrity` and refuses: it
/// says a commit was installed and will not say which, and reading it as absent would wave
/// through exactly the replacement it exists to judge. The same bytes, or another build of the
/// same commit, replace nothing. `mains` lists the adopted mains, and is asked only when there
/// is a commit to judge.
pub(crate) async fn check_basis<M>(
    executable: &HostStableExecutable,
    candidate: &Candidate,
    downgrade: Downgrade,
    mains: M,
) -> Result<()>
where
    M: AsyncFnOnce() -> Result<install_basis::Mains>,
{
    if downgrade == Downgrade::Allow {
        return Ok(());
    }
    let Some(installed) = installed_build(executable)? else {
        return Ok(());
    };
    let Some(record) = installed.record else {
        return Ok(());
    };
    if installed.digest == candidate.digest {
        return Ok(());
    }
    let Some(candidate_record) = &candidate.record else {
        return Err(unrecorded_refusal(&candidate.source, &record));
    };
    if candidate_record.commit == record.commit {
        return Ok(());
    }
    let mains = mains().await.map_err(|error| {
        unproven_refusal(
            &candidate.source,
            candidate_record,
            &record,
            &format!(
                "the adopted mains that judge it could not be listed: {}",
                error.message
            ),
        )
    })?;
    install_basis::judge(&candidate.source, candidate_record, &record, &mains).await
}

/// Keep the build the stable name runs now through the install that replaces it, and name its
/// stored copy.
///
/// A stored copy already is that: no install removes a copy until it is two builds old, so the
/// failed activation that has to put the previous build back finds it where it was. The install
/// from before copies were content-addressed left the binary itself at the stable name, and the
/// install replaces that name with a link; so that binary is stored under its digest first,
/// through the same copy-and-rename an install makes — written whole before anything names it,
/// and needing nothing a sandboxed or unprivileged process may not do. `None` means there was
/// nothing installed to retain, which is a first install and has nothing to roll back to.
pub fn retain_previous_executable<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    executable: &HostStableExecutable,
) -> Result<Option<PathBuf>>
where
    F: LaunchdFilesystem,
    C: LaunchctlCommand,
{
    let Some(metadata) = inspect_existing(executable.path())? else {
        return Ok(None);
    };
    if metadata.file_type().is_symlink() {
        let target = fs::read_link(executable.path()).map_err(|error| {
            CowshedError::internal(format!(
                "could not read the link {}: {error}",
                executable.path().display()
            ))
        })?;
        let stored = executable.directory().join(target);
        return Ok(inspect_existing(&stored)?.map(|_| stored));
    }
    let digest = executable_digest(executable.path())?;
    install_stored_executable(executor, executable, &digest, executable.path())?;
    Ok(Some(executable.stored(&digest)))
}

/// Remove the stored copies an install has made redundant: every one but the new build and the
/// one it replaced. The replaced build stays so a workspace supervisor of that build, still
/// draining its jobs, can start the shells those jobs need from the path it started from; the
/// next install removes it.
///
/// Said out loud and never fatal: an install that succeeded is not undone for 30 MB it could not
/// reclaim.
fn prune_superseded<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    executable: &HostStableExecutable,
    current: &ExecutableDigest,
    previous: Option<&Path>,
) where
    F: LaunchdFilesystem,
    C: LaunchctlCommand,
{
    let previous = previous
        .and_then(Path::file_name)
        .and_then(|name| executable.stored_digest(name));
    let pruned = stored_files(executable, |digest| {
        digest != current && previous.as_ref() != Some(digest)
    })
    .and_then(|superseded| {
        executor
            .execute_install(&plan_executable_remove(executable, &superseded))
            .map(|_| ())
            .map_err(launchd_error)
    });
    if let Err(error) = pruned {
        cowshed_os::eprintln!(
            "cowshed: could not remove superseded copies of {}: {}",
            executable.path().display(),
            error.message
        );
    }
}

/// Put the build `retained` names back as what the stable name runs, and say whether the host
/// was left as it was found.
///
/// Reported in the returned sentence rather than raised: the caller already has the failure that
/// prompted the rollback, and a rollback that itself failed must not replace that failure with a
/// second one — it must be appended to it, because the two together are the host's actual state.
pub fn restore_previous_executable(executable: &HostStableExecutable, retained: &Path) -> String {
    let restored = fs::symlink_metadata(retained).and_then(|_| match retained.file_name() {
        Some(name) => replace_symlink(Path::new(name), executable.path()),
        None => Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "the retained copy names no file",
        )),
    });
    match restored {
        Ok(()) => format!(
            "the previous {} was restored; this host is as it was found",
            executable.path().display()
        ),
        Err(error) => format!(
            "the previous binary could NOT be restored ({error}); {} still holds the new build and its agent is not loaded",
            executable.path().display()
        ),
    }
}

/// Install the candidate, activate its agent, and undo the install if activation fails.
///
/// The whole point is the last clause. `launchctl` failing after the copy is what left this host
/// running a debug gateway with no agent: the binary had already been replaced, and nothing put it
/// back. Activation is the step that can fail for reasons that have nothing to do with the bytes
/// (a bootout launchd did not finish, a bootstrap it refused), so it is the step whose failure
/// has to be recoverable.
fn install_and_activate_gateway<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    home: &Path,
    candidate: &Candidate,
    target: &LaunchAgentTarget,
    plist: InstallOutcome,
    identity: SigningIdentity,
) -> Result<HostStableExecutable>
where
    F: LaunchdFilesystem,
    C: LaunchctlCommand,
{
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    remove_planted_cargo_link(executor, &executable)?;
    let retained = retain_previous_executable(executor, &executable)?;
    let moved =
        install_stored_executable(executor, &executable, &candidate.digest, &candidate.source)?;
    install_gateway_host(
        executor,
        &executable,
        &candidate.source,
        identity,
        moved == InstallOutcome::Changed,
    )?;
    record_installed_source(&executable, &candidate.source);
    record_installed_build(&executable, candidate);
    let uid = effective_uid();
    match activate(executor, uid, target, plist) {
        Ok(()) => {
            prune_superseded(
                executor,
                &executable,
                &candidate.digest,
                retained.as_deref(),
            );
            Ok(executable)
        }
        Err(failure) => Err(match retained {
            Some(retained) => {
                let rollback = restore_previous_executable(&executable, &retained);
                record_installed_source(
                    &executable,
                    Path::new("restored after a failed activation"),
                );
                let reloaded = reload_restored(executor, uid, target, &failure);
                let error = failure.into_error();
                CowshedError::new(
                    error.code,
                    format!("{}; {rollback}; {reloaded}", error.message),
                    error.hint,
                )
            }
            // Nothing to roll back to: this host had no installed binary before the run, so the
            // new copy is not a regression and deleting it would only hide the failed activation.
            None => failure.into_error(),
        }),
    }
}

/// Load the agent again onto the restored build after `failure`, and say how that went.
///
/// Restoring the bytes is not enough: a host with the right binary and no supervisor is still
/// degraded. The plist names the stable name, which the restore pointed back, so loading the agent
/// runs the restored build. Which step loads it is decided by what launchd holds
/// ([`agent_cycle::reload`]): an agent it does not hold is bootstrapped, since `kickstart` answers
/// it "Could not find service" (exit 113); one it holds is kickstarted; one whose bootout went
/// overdue is still in teardown and is left alone, since a kickstart of it is launchctl 37.
fn reload_restored<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    uid: u32,
    target: &LaunchAgentTarget,
    failure: &ActivationError,
) -> String
where
    C: LaunchctlCommand,
{
    let held = match failure {
        ActivationError::BootoutOverdue { remains, .. } => Ok(Held::TearingDown(*remains)),
        ActivationError::Failed(_) => loaded_agent(executor, uid, target)
            .map(|agent| agent.map_or(Held::Absent, Held::Loaded)),
    };
    match held.map(agent_cycle::reload) {
        Ok(step) => match load(executor, uid, target, step) {
            Ok(()) => format!("{} is loaded again", target.label()),
            Err(reload) => format!(
                "{} could NOT be reloaded ({})",
                target.label(),
                reload.into_error().message
            ),
        },
        Err(error) => format!(
            "{} could NOT be reloaded: launchd's hold on it is unknown ({})",
            target.label(),
            error.message
        ),
    }
}

/// The outcome of reconciling one installed host service with the invoking build and definition.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceBinaryRefresh {
    /// The binary or agent definition was stale; it was reconciled and the service reloaded.
    Refreshed { service: String },
    /// The installed copy is stale, but this invocation cannot durably refresh it; the remedy
    /// names what can.
    Stale { service: String, remedy: String },
    /// The installed copy is stale and the invoking build must not be supervised, so it was left
    /// alone. Reported rather than raised: `setup`'s subject is host storage and refusing to
    /// repair a host over this would be worse than the drift. Reported rather than skipped: a
    /// silent decline would let `setup` claim the host is current when it knows it is not.
    Refused {
        service: String,
        reason: String,
        remedy: String,
    },
    /// The invoking build does not contain the installed cowshed ([`check_basis`]), and the run
    /// was not asked to go back. Reported like a refusal, and ends the run non-zero: the install
    /// was the point.
    Downgrade {
        service: String,
        reason: String,
        remedy: String,
    },
}

/// Whether the stable name does not yet run the candidate's stored copy.
pub fn installed_binary_is_stale(
    executable: &HostStableExecutable,
    digest: &ExecutableDigest,
    state: &ExecutableInstallState,
) -> bool {
    !state.is_current(executable, digest)
}

/// Reconcile the gateway's stable binary and generated agent definition with this command.
///
/// Setup never refuses to repair a host. `None` means no gateway agent is installed, or the
/// installed artifacts already match and the daemon runs them. Plist drift is repaired even when
/// the invoking build is byte-identical to the installed copy, including when setup runs from
/// that copy itself. Binary drift uses the same atomic install and activation rollback as
/// `gateway start`; so does a daemon that answers while running other bytes than the installed
/// copy, which launchd keeps for as long as the process lives — restarting it is what makes the
/// CLI on `PATH` and the daemon one artifact again.
///
/// The build has to be fit to supervise, and contain the installed one, *before* anything is
/// copied, and a failed activation puts the old binary back: this is the function that,
/// no loaded agent. The caller holds the install slot: `slot` ([`take_install_slot`]), and a
/// refresh that restarted the gateway returns only once it serves ([`await_healthy`]), so the
/// slot is released to a gateway a land step can reach. `roots` are the host's: their namespace
/// names the agent refreshed and their store holds the control socket it answers on.
pub async fn refresh_gateway_binary(
    slot: &InstallSlot,
    roots: &CanonicalRoots,
    home: &Path,
    downgrade: Downgrade,
) -> Result<Option<ServiceBinaryRefresh>> {
    let candidate = Candidate::of(running_executable()?, BuildRecord::running())?;
    let (executable, target) = gateway_launch_agent(roots.namespace(), home)?;
    let basis = check_basis(&executable, &candidate, downgrade, async || {
        adopted_mains(home).await
    })
    .await;
    let socket = roots.control_socket();
    let client = GatewayControlClient::new(socket.clone()).ok();
    let daemon = match &client {
        Some(client) => client.status().await.ok(),
        None => None,
    };
    // Only a daemon that answers is one a restart would cut a land off from.
    let answering = daemon.is_some().then_some(slot);
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    // Held until the restarted gateway serves (`await_healthy` below).
    let _carrying = carry_listeners(&socket, downgrade);
    let refresh = refresh_gateway_from(
        roots.namespace(),
        home,
        IncomingBuild {
            candidate: &candidate,
            debug_build: cfg!(debug_assertions),
            basis,
        },
        daemon.as_ref(),
        answering,
        &mut executor,
        invoking_bun,
        || gateway_host_identity(home),
    )
    .await?;
    if let Some(ServiceBinaryRefresh::Refreshed { .. }) = &refresh {
        let client = GatewayControlClient::new(socket.clone()).map_err(control_error)?;
        // Setup reports the refresh itself; the wait says only what it is still waiting for.
        let mut output = Output::new(io::sink(), Blocking::new(io::stderr()), false);
        await_healthy(
            &mut output,
            slot,
            &client,
            &socket,
            &candidate.digest.to_string(),
            &mut executor,
            &target,
        )
        .await?;
    }
    Ok(refresh)
}

/// The build a refresh would install, with the judgements about installing it.
struct IncomingBuild<'a> {
    candidate: &'a Candidate,
    /// A debug build is refused as a supervised binary ([`refuse_unsupervisable_build`]).
    debug_build: bool,
    /// [`check_basis`]'s judgement of replacing the installed build with `candidate`, consulted
    /// only when the binary is replaced.
    basis: Result<()>,
}

/// [`refresh_gateway_binary`] with its inputs given. `answering` is the install slot when a
/// daemon answered: every refresh that gets past the no-op return activates the agent, which
/// restarts that daemon, so it is first refused while a land runs
/// ([`refuse_restart_while_busy`]). `bun` is asked only when a gateway agent is installed: a host
/// without one needs no interpreter.
#[expect(
    clippy::too_many_arguments,
    reason = "each input is a seam setup's refresh tests inject: the host, the build, launchd, bun and the signing identity"
)]
async fn refresh_gateway_from<C: LaunchctlCommand>(
    namespace: HostNamespace,
    home: &Path,
    incoming: IncomingBuild<'_>,
    daemon: Option<&DaemonStatus>,
    answering: Option<&InstallSlot>,
    executor: &mut LaunchdExecutor<NativeFilesystem, C>,
    bun: impl FnOnce() -> Result<BunDirectory>,
    identity: impl FnOnce() -> Result<SigningIdentity>,
) -> Result<Option<ServiceBinaryRefresh>> {
    let IncomingBuild {
        candidate,
        debug_build,
        basis,
    } = incoming;
    let (executable, target) = gateway_launch_agent(namespace, home)?;
    let observed = inspect_install_state(&target)?;
    if observed.plist.is_none() {
        return Ok(None);
    }
    let spec = LaunchAgentSpec::gateway(namespace, &executable, &bun()?).map_err(launchd_error)?;
    let plan = plan_install(
        &spec,
        InstallState {
            launch_agents_directory_mode: observed.directory_mode,
            plist: observed.plist.as_ref().map(|plist| ExistingPlist {
                bytes: &plist.bytes,
                mode: plist.mode,
            }),
        },
    );
    let state = observe_executable_install(&executable, &candidate.digest, &candidate.source)?;
    let binary_is_stale = installed_binary_is_stale(&executable, &candidate.digest, &state);
    let daemon_is_stale = daemon.is_some_and(|status| {
        status.executable_sha256.as_deref() != Some(candidate.digest.as_str())
    });
    // Refuse only a binary replacement: reconciling an agent definition, or restarting a daemon
    // onto the copy already installed, does not install this invocation's bytes. Preserve the
    // existing supervised binary when it already matches.
    if binary_is_stale {
        if let Err(refusal) = refuse_unsupervisable_build(candidate.source.clone(), debug_build) {
            return Ok(Some(ServiceBinaryRefresh::Refused {
                service: spec.label().to_owned(),
                reason: refusal.message,
                remedy: refusal.hint,
            }));
        }
        if let Err(refusal) = basis {
            return Ok(Some(ServiceBinaryRefresh::Downgrade {
                service: spec.label().to_owned(),
                reason: refusal.message,
                remedy: refusal.hint,
            }));
        }
    }
    let restarts = binary_is_stale || daemon_is_stale || !plan.is_noop();
    // Before anything is installed: a refusal leaves the installed cowshed and the gateway as
    // they were.
    if restarts && let Some(slot) = answering {
        refuse_restart_while_busy(slot, home)?;
    }
    // The packaged Nx cache server is part of this build's install. A current agent is still
    // repaired, so this runs before the no-op return and before any activation: a missing
    // companion fails here rather than after the host has changed.
    install_nx_cache_server(executor, home, &candidate.source)?;
    // The gateway host is repaired like the companion, before the no-op return and without a
    // restart: a missing or foreign-signed host is rewritten, and launchd runs the rewritten file
    // at the next start. An install that moves the stable name rewrites it below.
    let identity = identity()?;
    if !binary_is_stale {
        install_gateway_host(executor, &executable, &candidate.source, identity, false)?;
    }
    if !restarts {
        return Ok(None);
    }
    let plist = executor.execute_install(&plan).map_err(launchd_error)?;
    if binary_is_stale {
        // Binary drift requires a restart even when the agent definition itself did not change.
        install_and_activate_gateway(
            executor,
            home,
            candidate,
            spec.target(),
            InstallOutcome::Changed,
            identity,
        )?;
    } else if daemon_is_stale {
        activate_launch_agent(
            executor,
            effective_uid(),
            spec.target(),
            InstallOutcome::Changed,
        )?;
    } else {
        activate_launch_agent(executor, effective_uid(), spec.target(), plist)?;
    }
    Ok(Some(ServiceBinaryRefresh::Refreshed {
        service: spec.label().to_owned(),
    }))
}

/// What `setup` did about the `cowshed` the operator types.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CliInstall {
    /// The invoking build is the installed cowshed, and the first `cowshed` on `PATH` was
    /// reconciled with it.
    Installed {
        /// The stable name: what the `PATH` entry names and the gateway agent runs.
        stable: PathBuf,
        /// This run copied the build in or moved the stable name to it. `false` when the stable
        /// name already ran it — including when the gateway refresh of the same run did so.
        replaced: bool,
        /// The invoking binary is itself a stored copy of the installed cowshed, so a setup from
        /// it can install nothing new.
        from_installed_copy: bool,
        path_entry: PathEntryOutcome,
    },
    /// The invoking build must not be installed ([`refuse_unsupervisable_build`]).
    Refused { reason: String, remedy: String },
    /// The invoking build does not contain the installed one ([`check_basis`]), and the run was
    /// not asked to go back.
    Downgrade { reason: String, remedy: String },
}

/// Make the invoking build the host's cowshed, and the first `cowshed` on `path` reach it.
///
/// The gateway's own refresh ([`refresh_gateway_binary`]) has already installed this build when an
/// agent is installed — with the rollback an agent needs — and this finds that install current.
/// On a host with no gateway agent this is the install: the copy, the stable name, the record.
/// The caller holds the install slot ([`take_install_slot`]).
pub async fn install_cli(
    home: &Path,
    downgrade: Downgrade,
    path: &std::ffi::OsStr,
) -> Result<CliInstall> {
    let candidate = Candidate::of(running_executable()?, BuildRecord::running())?;
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let basis = check_basis(&executable, &candidate, downgrade, async || {
        adopted_mains(home).await
    })
    .await;
    install_cli_from(
        home,
        &candidate,
        cfg!(debug_assertions),
        basis,
        path,
        &mut LaunchdExecutor::native(),
    )
}

fn install_cli_from<F, C>(
    home: &Path,
    candidate: &Candidate,
    debug_build: bool,
    basis: Result<()>,
    path: &std::ffi::OsStr,
    executor: &mut LaunchdExecutor<F, C>,
) -> Result<CliInstall>
where
    F: LaunchdFilesystem,
    C: LaunchctlCommand,
{
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    // Whatever the build: a setup that finds its cowshed current still removes the `cargo` link
    // an earlier install left.
    remove_planted_cargo_link(executor, &executable)?;
    let state = observe_executable_install(&executable, &candidate.digest, &candidate.source)?;
    let replaced = installed_binary_is_stale(&executable, &candidate.digest, &state);
    if replaced {
        if let Err(refusal) = refuse_unsupervisable_build(candidate.source.clone(), debug_build) {
            return Ok(CliInstall::Refused {
                reason: refusal.message,
                remedy: refusal.hint,
            });
        }
        if let Err(refusal) = basis {
            return Ok(CliInstall::Downgrade {
                reason: refusal.message,
                remedy: refusal.hint,
            });
        }
        let retained = retain_previous_executable(executor, &executable)?;
        install_stored_executable(executor, &executable, &candidate.digest, &candidate.source)?;
        record_installed_source(&executable, &candidate.source);
        record_installed_build(&executable, candidate);
        prune_superseded(
            executor,
            &executable,
            &candidate.digest,
            retained.as_deref(),
        );
    }
    let from_installed_copy = candidate
        .source
        .parent()
        .is_some_and(|directory| directory == executable.directory())
        && candidate
            .source
            .file_name()
            .and_then(|name| executable.stored_digest(name))
            .is_some();
    Ok(CliInstall::Installed {
        stable: executable.path().to_path_buf(),
        replaced,
        from_installed_copy,
        path_entry: crate::path_entry::point_at(path, executable.path()),
    })
}

fn is_user_owned(metadata: &fs::Metadata, want_dir: bool) -> bool {
    let kind_ok = if want_dir {
        metadata.is_dir()
    } else {
        metadata.is_file()
    };
    kind_ok && !metadata.file_type().is_symlink() && metadata.uid() == effective_uid()
}

fn inspect_existing(path: &Path) -> Result<Option<fs::Metadata>> {
    match fs::symlink_metadata(path) {
        Ok(metadata) => Ok(Some(metadata)),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(CowshedError::internal(format!(
            "could not inspect {}: {error}",
            path.display()
        ))),
    }
}

/// What the host has under the stable name's directory, and whether the stored copy of the
/// source is already there and the stable name already runs it.
fn observe_executable_install(
    executable: &HostStableExecutable,
    digest: &ExecutableDigest,
    source: &Path,
) -> Result<ExecutableInstallState> {
    let stored_path = executable.stored(digest);
    let stored = match inspect_existing(&stored_path)? {
        Some(metadata) if is_user_owned(&metadata, false) => Some(InstalledExecutable {
            mode: metadata.permissions().mode() & 0o777,
            matches_source: source == stored_path
                || same_contents(source, &stored_path, metadata.len())?,
        }),
        Some(_) => {
            return Err(CowshedError::integrity(
                format!(
                    "the stored {} copy is not a user-owned regular file: {}",
                    executable.name(),
                    stored_path.display()
                ),
                "remove it and rerun the service start command",
            ));
        }
        None => None,
    };
    let link = match inspect_existing(executable.path())? {
        None => StableLink::Absent,
        Some(metadata) if metadata.uid() != effective_uid() => {
            return Err(CowshedError::integrity(
                format!(
                    "the installed {} is not owned by this user: {}",
                    executable.name(),
                    executable.path().display()
                ),
                "remove it and rerun the service start command",
            ));
        }
        Some(metadata) if metadata.file_type().is_symlink() => {
            StableLink::To(fs::read_link(executable.path()).map_err(|error| {
                CowshedError::internal(format!(
                    "could not read the link {}: {error}",
                    executable.path().display()
                ))
            })?)
        }
        Some(metadata) if metadata.is_file() => StableLink::Foreign,
        Some(_) => {
            return Err(CowshedError::integrity(
                format!(
                    "the installed {} is not a regular file or a link: {}",
                    executable.name(),
                    executable.path().display()
                ),
                "remove it and rerun the service start command",
            ));
        }
    };
    Ok(ExecutableInstallState {
        support_directory_mode: private_directory_mode(executable.support_directory())?,
        binary_directory_mode: private_directory_mode(executable.directory())?,
        stored,
        link,
    })
}

/// Whether the installed binary already holds the source's bytes.
///
/// Length first, then a streaming comparison: the alternative is rewriting tens of megabytes on
/// every `start`, and any digest would have to read both files anyway.
fn same_contents(source: &Path, installed: &Path, installed_length: u64) -> Result<bool> {
    let mut source_file = open_for_compare(source)?;
    let source_length = source_file
        .metadata()
        .map_err(|error| compare_error(source, error))?
        .len();
    if source_length != installed_length {
        return Ok(false);
    }
    let mut installed_file = open_for_compare(installed)?;
    let mut source_chunk = vec![0u8; COMPARE_CHUNK_BYTES];
    let mut installed_chunk = vec![0u8; COMPARE_CHUNK_BYTES];
    loop {
        let read = fill(&mut source_file, &mut source_chunk)
            .map_err(|error| compare_error(source, error))?;
        let other = fill(&mut installed_file, &mut installed_chunk)
            .map_err(|error| compare_error(installed, error))?;
        if read != other || source_chunk[..read] != installed_chunk[..read] {
            return Ok(false);
        }
        if read == 0 {
            return Ok(true);
        }
    }
}

const COMPARE_CHUNK_BYTES: usize = 64 * 1024;

fn open_for_compare(path: &Path) -> Result<fs::File> {
    fs::File::open(path).map_err(|error| compare_error(path, error))
}

fn compare_error(path: &Path, error: io::Error) -> CowshedError {
    CowshedError::internal(format!("could not read {}: {error}", path.display()))
}

/// Read until the buffer is full or the file ends, so a short read is never mistaken for a
/// difference.
fn fill(file: &mut fs::File, buffer: &mut [u8]) -> io::Result<usize> {
    let mut filled = 0;
    while filled < buffer.len() {
        match file.read(&mut buffer[filled..])? {
            0 => break,
            read => filled += read,
        }
    }
    Ok(filled)
}

/// The mode of a cowshed-owned directory, `None` when it does not exist yet.
fn private_directory_mode(path: &Path) -> Result<Option<u32>> {
    match inspect_existing(path)? {
        Some(metadata) if is_user_owned(&metadata, true) => {
            Ok(Some(metadata.permissions().mode() & 0o777))
        }
        Some(_) => Err(CowshedError::integrity(
            format!("path is not a user-owned directory: {}", path.display()),
            format!("repair the ownership of {} and retry", path.display()),
        )),
        None => Ok(None),
    }
}

pub(crate) struct ObservedInstallState {
    pub(crate) directory_mode: Option<u32>,
    pub(crate) plist: Option<ObservedPlist>,
}

pub(crate) struct ObservedPlist {
    pub(crate) bytes: Vec<u8>,
    pub(crate) mode: u32,
}

pub(crate) fn inspect_install_state(target: &LaunchAgentTarget) -> Result<ObservedInstallState> {
    let directory_mode = private_directory_mode(target.launch_agents_directory())?;
    match inspect_existing(target.plist_path())? {
        Some(metadata) if is_user_owned(&metadata, false) => {
            let bytes = fs::read(target.plist_path()).map_err(|error| {
                CowshedError::internal(format!(
                    "could not read {}: {error}",
                    target.plist_path().display()
                ))
            })?;
            Ok(ObservedInstallState {
                directory_mode,
                plist: Some(ObservedPlist {
                    bytes,
                    mode: metadata.permissions().mode() & 0o777,
                }),
            })
        }
        Some(_) => Err(CowshedError::integrity(
            format!(
                "{} LaunchAgent plist is not a user-owned regular file: {}",
                target.label(),
                target.plist_path().display()
            ),
            "remove the unsafe plist and rerun the service start command",
        )),
        None => Ok(ObservedInstallState {
            directory_mode,
            plist: None,
        }),
    }
}

/// Create a gateway-owned directory at exactly [`PRIVATE_DIRECTORY_MODE`], then prove it is ours.
///
/// `LaunchdFilesystem::ensure_directory` sets the mode through an `O_NOFOLLOW` descriptor on the
/// named directory, so a symlink planted where one belongs fails the create instead of being
/// followed and then noticed afterwards. The check this replaced was `create_dir_all` followed by
/// `canonicalize(path) == path`, which wrote the tree before it looked and then refused every
/// legitimate directory reached through a symlinked ancestor — a home on a linked volume, or
/// anything under macOS's own `/var` link.
fn ensure_private_directory(path: &Path) -> Result<()> {
    NativeFilesystem::new()
        .ensure_directory(path, PRIVATE_DIRECTORY_MODE)
        .map_err(|error| {
            // `O_NOFOLLOW` refuses a symlink with `ELOOP`, and refuses it before anything on the
            // far side is touched. That is a plant, not a host problem, so it is named as one.
            if error.raw_os_error() == Some(libc::ELOOP) {
                CowshedError::integrity(
                    format!(
                        "gateway path is a symlink rather than a private directory: {}",
                        path.display()
                    ),
                    "cowshed doctor --json",
                )
            } else {
                CowshedError::internal(format!("could not create {}: {error}", path.display()))
            }
        })?;
    let metadata = inspect_existing(path)?.ok_or_else(|| {
        CowshedError::internal(format!(
            "{} vanished between being created and being inspected",
            path.display()
        ))
    })?;
    if !is_user_owned(&metadata, true) {
        return Err(CowshedError::integrity(
            format!(
                "gateway path is not a private directory: {}",
                path.display()
            ),
            "cowshed doctor --json",
        ));
    }
    Ok(())
}

pub(crate) fn launchd_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::internal(format!("LaunchAgent operation failed: {error}"))
}

pub(crate) fn output_error(error: io::Error) -> CowshedError {
    CowshedError::internal(format!("could not write command output: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::launchd::STABLE_BINARY_MODE;
    use cowshed_os::namespace::TestNamespace;
    #[cfg(target_os = "macos")]
    use cowshed_os::test_backstop::CommandBackstop as _;

    type RecordingExecutor = LaunchdExecutor<NativeFilesystem, RecordingGatewayLaunchctl>;

    /// A gateway that is not running answers a status read `Absent`: its socket is missing,
    /// which the bounded connect reports as `WaitError::Absent`.
    #[tokio::test]
    async fn a_missing_gateway_socket_reads_as_no_gateway() {
        let socket = PathBuf::from(format!(
            "/tmp/cowshed-no-gateway-{}.sock",
            std::process::id()
        ));
        let control = ControlSocket::at(socket).unwrap();
        assert!(matches!(
            control.status().await,
            Err(GatewayStatusError::Absent)
        ));
    }

    /// A build file under `home` with a record of the commit it was made from, as a release
    /// binary carries one; the commit is the time spelled as hex, for tests that judge nothing.
    fn build(home: &Path, name: &str, bytes: &[u8], commit_time: Option<u64>) -> Candidate {
        build_recording(
            home,
            name,
            bytes,
            commit_time.map(|commit_time| BuildRecord {
                commit: format!("{commit_time:040x}"),
                commit_time,
            }),
        )
    }

    /// A build file under `home` recording exactly `record`, laid out as a distribution:
    /// `<home>/package/dist/bin/<platform>/<name>`, with the package's Nx cache server dependency
    /// hoisted one level up ([`distribution_companion`]).
    fn build_recording(
        home: &Path,
        name: &str,
        bytes: &[u8],
        record: Option<BuildRecord>,
    ) -> Candidate {
        let directory = home.join("package/dist/bin/platform");
        fs::create_dir_all(&directory).expect("builds directory");
        let path = directory.join(name);
        fs::write(&path, bytes).expect("build");
        let companion = distribution_companion(home);
        fs::create_dir_all(companion.parent().unwrap()).expect("companion directory");
        fs::write(&companion, b"the packaged nx cache server").expect("companion");
        Candidate::of(path, record).expect("candidate")
    }

    /// Where [`build_recording`]'s distributions find `@smoothbricks/nx-cache-server`'s bundle.
    fn distribution_companion(home: &Path) -> PathBuf {
        home.join(NX_CACHE_SERVER_BUNDLE)
    }

    /// The `bun` directory a test home's gateway agent names: an executable `bun` under `home`,
    /// the same directory every call, so a refresh compares against the plist it was given.
    fn test_bun(home: &Path) -> BunDirectory {
        let directory = home.join("bun-bin");
        fs::create_dir_all(&directory).unwrap();
        let bun = directory.join("bun");
        fs::write(&bun, b"#!/bin/sh\n").unwrap();
        fs::set_permissions(&bun, std::os::unix::fs::PermissionsExt::from_mode(0o755)).unwrap();
        BunDirectory::on_path(directory.as_os_str(), None).unwrap()
    }

    /// A home whose gateway agent is installed with a current plist and no binary yet.
    fn host_with_agent(label: &str) -> (PathBuf, HostStableExecutable, LaunchAgentSpec) {
        let home = scratch_root(label);
        let executable = HostStableExecutable::new(&home, COWSHED_BINARY_NAME).unwrap();
        let spec =
            LaunchAgentSpec::gateway(HostNamespace::Production, &executable, &test_bun(&home))
                .unwrap();
        let mut filesystem = NativeFilesystem::new();
        for directory in [
            executable.support_directory(),
            executable.directory(),
            spec.launch_agents_directory(),
        ] {
            filesystem
                .ensure_directory(directory, PRIVATE_DIRECTORY_MODE)
                .unwrap();
        }
        fs::write(spec.plist_path(), spec.plist_bytes()).unwrap();
        fs::set_permissions(
            spec.plist_path(),
            std::os::unix::fs::PermissionsExt::from_mode(crate::launchd::PRIVATE_PLIST_MODE),
        )
        .unwrap();
        (home, executable, spec)
    }

    /// The stored copies in the host-stable directory, as their digests' first eight digits.
    fn stored(executable: &HostStableExecutable) -> Vec<String> {
        let mut copies: Vec<String> = fs::read_dir(executable.directory())
            .unwrap()
            .filter_map(|entry| {
                let name = entry.unwrap().file_name();
                let digest = executable.stored_digest(&name)?;
                Some(digest.as_str()[..8].to_owned())
            })
            .collect();
        copies.sort();
        copies
    }

    fn short(candidate: &Candidate) -> String {
        candidate.digest.as_str()[..8].to_owned()
    }

    fn sorted(mut copies: Vec<String>) -> Vec<String> {
        copies.sort();
        copies
    }

    /// The Aug drift in one test: a binary installed days earlier, byte-different from the build
    /// running setup, observed as exactly that — stale — and the plain binary an install from
    /// before copies were content-addressed left at the stable name stale even when its bytes are
    /// the build's, because the stable name must be a link to a stored copy. This is the decision
    /// `refresh_gateway_binary` acts on; the observation is pure filesystem, so it is provable
    /// without launchd.
    #[test]
    fn planted_binary_drift_is_observed_as_stale_until_the_stable_name_runs_the_build() {
        let home = scratch_root("drift");
        let executable = HostStableExecutable::new(&home, COWSHED_BINARY_NAME).expect("executable");
        fs::create_dir_all(executable.directory()).expect("bin directory");
        let candidate = build(&home, "fresh-build", b"the build running setup", None);
        let stale = |candidate: &Candidate| {
            let state =
                observe_executable_install(&executable, &candidate.digest, &candidate.source)
                    .expect("observe");
            installed_binary_is_stale(&executable, &candidate.digest, &state)
        };

        // Nothing installed at all is stale: there is no current copy to be running.
        assert!(stale(&candidate));

        // Drifted bytes at the stable path — the old layout — are stale.
        fs::write(executable.path(), b"the binary from days ago").expect("plant drift");
        fs::set_permissions(
            executable.path(),
            std::os::unix::fs::PermissionsExt::from_mode(STABLE_BINARY_MODE),
        )
        .expect("stable mode");
        assert!(stale(&candidate));

        // So are identical bytes in that layout: the install moves the stable name onto a stored
        // copy, so the CLI on PATH and the daemon change together from then on.
        fs::write(executable.path(), b"the build running setup").expect("refresh");
        assert!(stale(&candidate));

        install_host_stable_executable(&mut LaunchdExecutor::native(), &home, &candidate.source)
            .expect("install");
        assert!(!stale(&candidate), "the installed build is current");
        assert_eq!(
            fs::read_link(executable.path()).expect("the stable name is a link"),
            executable.stored_link(&candidate.digest)
        );
        assert_eq!(
            fs::read(executable.path()).expect("through the link"),
            b"the build running setup"
        );

        // Another build's bytes are stale again, and the same bytes from another file are not.
        assert!(stale(&build(&home, "newer", b"a newer build", None)));
        assert!(!stale(&build(
            &home,
            "same-bytes-elsewhere",
            b"the build running setup",
            None
        )));

        fs::remove_dir_all(&home).ok();
    }

    /// launchctl recorded and answered, and `codesign` recorded apart from it: answered as a
    /// success (a verification failing when `unsatisfied`), or run for real for a host proof.
    #[derive(Default)]
    struct RecordingGatewayLaunchctl {
        argv: Vec<Vec<std::ffi::OsString>>,
        codesign: Vec<Vec<std::ffi::OsString>>,
        unsatisfied: bool,
        real_codesign: bool,
    }

    /// A signing identity for tests whose `codesign` is recorded, never run.
    fn test_identity() -> SigningIdentity {
        SigningIdentity {
            certificate: code_signing::CertificateHash::parse(
                "8eadaf1cdf363f71ed26d32d033c4cf4ed65010c",
            )
            .unwrap(),
        }
    }

    impl LaunchctlCommand for RecordingGatewayLaunchctl {
        fn run(
            &mut self,
            executable: &Path,
            arguments: &[std::ffi::OsString],
            deadline: &cowshed_os::deadline::Deadline,
        ) -> std::result::Result<crate::launchd::LaunchctlOutput, cowshed_os::deadline::RunError>
        {
            if executable == Path::new(code_signing::CODESIGN) {
                self.codesign.push(arguments.to_vec());
                if self.real_codesign {
                    return NativeLaunchctlCommand.run(executable, arguments, deadline);
                }
                return Ok(crate::launchd::LaunchctlOutput {
                    status: if self.unsatisfied && arguments[0] == "--verify" {
                        crate::launchd::CommandStatus::ExitCode(3)
                    } else {
                        crate::launchd::CommandStatus::Success
                    },
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                });
            }
            assert_eq!(executable, Path::new("/bin/launchctl"));
            self.argv.push(arguments.to_vec());
            Ok(crate::launchd::LaunchctlOutput {
                status: if arguments[0] == "print" {
                    crate::launchd::CommandStatus::ExitCode(113)
                } else {
                    crate::launchd::CommandStatus::Success
                },
                stdout: Vec::new(),
                stderr: Vec::new(),
            })
        }
    }

    #[tokio::test]
    async fn setup_refresh_rewrites_an_old_plist_with_a_matching_installed_binary() {
        let (home, executable, spec) = host_with_agent("plist-drift");
        let candidate = build(
            &home,
            "matching-build",
            b"the already installed release",
            None,
        );
        install_host_stable_executable(&mut LaunchdExecutor::native(), &home, &candidate.source)
            .unwrap();
        let desired = spec.plist_bytes();
        let desired_text = std::str::from_utf8(&desired).unwrap();
        let limits_start = desired_text
            .find("  <key>SoftResourceLimits</key>")
            .unwrap();
        let limits_end = desired_text.rfind("</dict>\n</plist>\n").unwrap();
        let mut legacy = desired_text.to_owned();
        legacy.replace_range(limits_start..limits_end, "");
        assert_ne!(legacy.as_bytes(), desired);
        fs::write(spec.plist_path(), &legacy).unwrap();
        let stored_inode = fs::metadata(executable.stored(&candidate.digest))
            .unwrap()
            .ino();
        let mut executor: RecordingExecutor = LaunchdExecutor::new(
            NativeFilesystem::new(),
            RecordingGatewayLaunchctl::default(),
        );

        // An external build of the same bytes, and setup run from the installed copy itself.
        for source in [
            candidate.source.clone(),
            executable.stored(&candidate.digest),
        ] {
            let candidate = Candidate::of(source, None).unwrap();
            let refreshed = refresh_gateway_from(
                HostNamespace::Production,
                &home,
                IncomingBuild {
                    candidate: &candidate,
                    debug_build: false,
                    basis: Ok(()),
                },
                None,
                None,
                &mut executor,
                || Ok(test_bun(&home)),
                || Ok(test_identity()),
            )
            .await
            .unwrap();
            assert_eq!(
                refreshed,
                Some(ServiceBinaryRefresh::Refreshed {
                    service: "dev.cowshed.gateway".to_owned(),
                })
            );
            assert_eq!(fs::read(spec.plist_path()).unwrap(), desired);
            assert_eq!(
                fs::metadata(executable.stored(&candidate.digest))
                    .unwrap()
                    .ino(),
                stored_inode,
                "a stored copy is never rewritten"
            );
            assert_eq!(
                refresh_gateway_from(
                    HostNamespace::Production,
                    &home,
                    IncomingBuild {
                        candidate: &candidate,
                        debug_build: false,
                        basis: Ok(())
                    },
                    None,
                    None,
                    &mut executor,
                    || Ok(test_bun(&home)),
                    || Ok(test_identity())
                )
                .await
                .unwrap(),
                None
            );
            fs::write(spec.plist_path(), &legacy).unwrap();
        }
        let (_, command) = executor.into_parts();
        assert_eq!(command.argv.len(), 6);
        assert_eq!(command.argv[2][0], "bootstrap");
        assert_eq!(command.argv[5][0], "bootstrap");
        fs::remove_dir_all(home).unwrap();
    }

    /// A refresh in one host namespace reconciles that namespace's agent and no other's
    /// (01_storage.md, "Host namespace"): over a home that holds production's installed agent, a
    /// test namespace has no agent, so it plans nothing, runs no launchctl command, and leaves
    /// production's plist as it was.
    #[tokio::test]
    async fn a_refresh_in_a_test_namespace_leaves_the_production_agent_alone() {
        let (home, _, production) = host_with_agent("namespaced-refresh");
        let candidate = build(&home, "incoming", b"an incoming build", None);
        let plist = fs::read(production.plist_path()).unwrap();
        let namespace = HostNamespace::Test(TestNamespace::mint().expect("a test namespace"));
        let mut executor: RecordingExecutor = LaunchdExecutor::new(
            NativeFilesystem::new(),
            RecordingGatewayLaunchctl::default(),
        );

        let refreshed = refresh_gateway_from(
            namespace,
            &home,
            IncomingBuild {
                candidate: &candidate,
                debug_build: false,
                basis: Ok(()),
            },
            None,
            None,
            &mut executor,
            || Ok(test_bun(&home)),
            || Ok(test_identity()),
        )
        .await
        .unwrap();

        assert_eq!(refreshed, None, "{} installed no agent", namespace.name());
        assert_eq!(fs::read(production.plist_path()).unwrap(), plist);
        let (_, command) = executor.into_parts();
        assert!(command.argv.is_empty(), "{:?}", command.argv);
        fs::remove_dir_all(home).unwrap();
    }

    /// The agent a namespace's verbs reach is the one its label names, in the home's
    /// LaunchAgents: production's `dev.cowshed.gateway`, a test namespace's its own.
    #[test]
    fn the_gateway_agent_is_labelled_by_its_host_namespace() {
        let home = scratch_root("agent-label");
        let namespace = TestNamespace::mint().expect("a test namespace");

        let (_, production) = gateway_launch_agent(HostNamespace::Production, &home).unwrap();
        assert_eq!(production.label(), "dev.cowshed.gateway");
        assert_eq!(
            production.plist_path(),
            home.join("Library/LaunchAgents/dev.cowshed.gateway.plist")
        );

        let (_, test) = gateway_launch_agent(HostNamespace::Test(namespace), &home).unwrap();
        assert_eq!(
            test.label(),
            format!("dev.cowshed-test-{namespace}.gateway")
        );
        assert_eq!(
            test.plist_path(),
            home.join(format!(
                "Library/LaunchAgents/dev.cowshed-test-{namespace}.gateway.plist"
            ))
        );
        fs::remove_dir_all(home).unwrap();
    }

    /// Every setup removes the `cargo` link an earlier install planted, including a setup whose
    /// cowshed is already current, in each shape an install ever gave it: the stable name or a
    /// stored copy, of cowshed or of the Nx cache server. Left there, `cargo` on `PATH` would run
    /// the cowshed CLI. An install over a host with no `cargo` link creates none.
    #[tokio::test]
    async fn every_setup_removes_the_cargo_link_an_earlier_install_planted() {
        let (home, executable, _) = host_with_agent("cargo-planted");
        let candidate = build(&home, "release", b"the installed release", None);
        install_host_stable_executable(&mut LaunchdExecutor::native(), &home, &candidate.source)
            .unwrap();
        let cargo = executable.directory().join(CARGO_LINK_NAME);
        let absent = |run: &str| {
            assert_eq!(
                fs::symlink_metadata(&cargo).err().map(|error| error.kind()),
                Some(io::ErrorKind::NotFound),
                "{run}"
            );
        };
        absent("an install with no cargo link");
        let companion = HostStableExecutable::new(&home, "nx-cache-server").unwrap();
        let mut executor: RecordingExecutor = LaunchdExecutor::new(
            NativeFilesystem::new(),
            RecordingGatewayLaunchctl::default(),
        );

        for planted in [
            Some(PathBuf::from(COWSHED_BINARY_NAME)),
            Some(executable.stored_link(&candidate.digest)),
            Some(PathBuf::from(companion.name())),
            Some(companion.stored_link(&candidate.digest)),
            None,
        ] {
            if let Some(target) = &planted {
                replace_symlink(target, &cargo).unwrap();
            }
            refresh_gateway_from(
                HostNamespace::Production,
                &home,
                IncomingBuild {
                    candidate: &candidate,
                    debug_build: false,
                    basis: Ok(()),
                },
                None,
                None,
                &mut executor,
                || Ok(test_bun(&home)),
                || Ok(test_identity()),
            )
            .await
            .unwrap();
            let installed = install_cli_from(
                &home,
                &candidate,
                false,
                Ok(()),
                std::ffi::OsStr::new(""),
                &mut executor,
            )
            .unwrap();
            assert!(
                matches!(
                    installed,
                    CliInstall::Installed {
                        replaced: false,
                        ..
                    }
                ),
                "{planted:?}: {installed:?}"
            );
            absent(&format!("a setup over cargo -> {planted:?}"));
        }

        // The uninstall takes a planted link with everything else an install left.
        replace_symlink(Path::new(COWSHED_BINARY_NAME), &cargo).unwrap();
        assert_eq!(
            remove_host_stable_executable(&executable).unwrap(),
            RemovalOutcome::Removed
        );
        absent("an uninstall");
        fs::remove_dir_all(home).unwrap();
    }

    /// A `cargo` in the host-stable directory that no install planted is not cowshed's: every
    /// install refuses it by its path and what it is before it installs anything, and leaves it as
    /// it was found; the uninstall leaves it as every other neighbour.
    #[tokio::test]
    async fn an_install_refuses_a_cargo_no_install_planted_and_leaves_it() {
        #[derive(Clone, Copy)]
        enum Found {
            File(&'static [u8]),
            Link(&'static str),
        }
        for (label, found, said) in [
            ("cargo-file", Found::File(b"a cargo"), "a regular file"),
            (
                "cargo-absolute",
                Found::Link("/usr/local/bin/cargo"),
                "a link saying /usr/local/bin/cargo",
            ),
            ("cargo-other", Found::Link("rustup"), "a link saying rustup"),
            (
                "cargo-nested",
                Found::Link("../bin/cowshed"),
                "a link saying ../bin/cowshed",
            ),
        ] {
            let (home, executable, _) = host_with_agent(label);
            let candidate = build(&home, "release", b"the installed release", None);
            let cargo = executable.directory().join(CARGO_LINK_NAME);
            let left = |run: &str| match found {
                Found::File(bytes) => assert_eq!(fs::read(&cargo).unwrap(), bytes, "{run}"),
                Found::Link(target) => {
                    assert_eq!(fs::read_link(&cargo).unwrap(), Path::new(target), "{run}")
                }
            };
            match found {
                Found::File(bytes) => fs::write(&cargo, bytes).unwrap(),
                Found::Link(target) => replace_symlink(Path::new(target), &cargo).unwrap(),
            }
            let refused = |error: CowshedError, run: &str| {
                assert_eq!(error.code.as_str(), "integrity", "{run}: {}", error.message);
                assert!(
                    error.message.contains(&cargo.display().to_string())
                        && error.message.contains(said),
                    "{run}: {}",
                    error.message
                );
            };

            let error = install_host_stable_executable(
                &mut LaunchdExecutor::native(),
                &home,
                &candidate.source,
            )
            .expect_err("the install refuses");
            refused(error, "install");
            assert_eq!(
                fs::symlink_metadata(executable.path())
                    .err()
                    .map(|error| error.kind()),
                Some(io::ErrorKind::NotFound),
                "{label}: the refused install installed nothing"
            );
            left("install");

            let error = install_cli_from(
                &home,
                &candidate,
                false,
                Ok(()),
                std::ffi::OsStr::new(""),
                &mut LaunchdExecutor::native(),
            )
            .expect_err("the setup refuses");
            refused(error, "setup");
            left("setup");

            assert_eq!(
                remove_host_stable_executable(&executable).unwrap(),
                RemovalOutcome::AlreadyAbsent
            );
            left("uninstall");
            fs::remove_dir_all(home).unwrap();
        }
    }

    /// The installed artifact is authoritative: a daemon that answers while running other bytes
    /// than the installed copy — launchd keeps the process it has until it exits, whatever the
    /// stable name now says — is restarted by `setup`, so the refusal that names `cowshed setup`
    /// is one that `cowshed setup` ends.
    #[tokio::test]
    async fn setup_restarts_a_daemon_that_runs_other_bytes_than_the_installed_copy() {
        let (home, _, _) = host_with_agent("daemon-drift");
        let candidate = build(&home, "release", b"the installed release", None);
        let mut executor: RecordingExecutor = LaunchdExecutor::new(
            NativeFilesystem::new(),
            RecordingGatewayLaunchctl::default(),
        );
        let refresh = async |executor: &mut RecordingExecutor, daemon: Option<&DaemonStatus>| {
            refresh_gateway_from(
                HostNamespace::Production,
                &home,
                IncomingBuild {
                    candidate: &candidate,
                    debug_build: false,
                    basis: Ok(()),
                },
                daemon,
                None,
                executor,
                || Ok(test_bun(&home)),
                || Ok(test_identity()),
            )
            .await
            .unwrap()
        };
        // The first run installs the build.
        assert!(matches!(
            refresh(&mut executor, None).await,
            Some(ServiceBinaryRefresh::Refreshed { .. })
        ));

        // A daemon on the installed bytes, and no daemon at all, are nothing to do.
        let current = daemon(false, None, Some(candidate.digest.as_str()));
        assert_eq!(refresh(&mut executor, Some(&current)).await, None);
        assert_eq!(refresh(&mut executor, None).await, None);

        // A daemon on other bytes, or one too old to say, is restarted onto the installed copy.
        for stale in [daemon(false, None, Some("aa")), daemon(false, None, None)] {
            assert_eq!(
                refresh(&mut executor, Some(&stale)).await,
                Some(ServiceBinaryRefresh::Refreshed {
                    service: "dev.cowshed.gateway".to_owned(),
                })
            );
        }
        fs::remove_dir_all(home).unwrap();
    }

    /// The incident, end to end through the gateway refresh: a build of a later sibling on main,
    /// lacking the installed build's gc fix, is refused with the fix named and the host untouched;
    /// a build that records nothing is refused too; the build of main that contains the fix
    /// installs; and `--downgrade` goes back on purpose, keeping the replaced build one install
    /// longer for a supervisor still draining on it.
    ///
    /// What this exercises and what it does not: the judgement is git's own, made in a real
    /// repository standing in for an adopted main, and the install is the real copy, link and
    /// prune in a scratch home. launchd is the recording fake, the main is handed in rather than
    /// read from the host's project registry, and nothing is elevated or installed on this host.
    #[tokio::test]
    async fn setup_refuses_a_build_that_lacks_the_installed_fix_unless_told_to_downgrade() {
        let (home, executable, _) = host_with_agent("basis");
        let main = crate::install_basis::tests::Main::new("refresh");
        let landing = cowshed_core::runtime::project::DEFAULT_LANDING_BRANCH;
        main.git(["checkout", "-q", "-b", "gc-fix"]);
        let fix = main.commit(
            "gc.rs",
            "judge strays by the ignore rules\n",
            "fix(cowshed): judge strays by the project ignore rules",
            2_000,
        );
        main.git(["checkout", "-q", landing]);
        let sibling = main.commit(
            "release.txt",
            "next prerelease\n",
            "chore(release): prepare next prerelease",
            3_000,
        );
        main.git(["merge", "-q", "--no-edit", "gc-fix"]);
        let merged = main.head(4_000);

        let installed = build_recording(&home, "installed", b"the gc fix", Some(fix));
        let lacking = build_recording(&home, "sibling", b"the later sibling", Some(sibling));
        let blind = build(&home, "blind", b"records nothing", None);
        let containing = build_recording(&home, "merged", b"main with the fix", Some(merged));
        let mut executor: RecordingExecutor = LaunchdExecutor::new(
            NativeFilesystem::new(),
            RecordingGatewayLaunchctl::default(),
        );
        let mains = main.mains();
        let mut attempt = async |candidate: &Candidate, downgrade: Downgrade| {
            let basis = check_basis(
                &executable,
                candidate,
                downgrade,
                async || Ok(mains.clone()),
            )
            .await;
            refresh_gateway_from(
                HostNamespace::Production,
                &home,
                IncomingBuild {
                    candidate,
                    debug_build: false,
                    basis,
                },
                None,
                None,
                &mut executor,
                || Ok(test_bun(&home)),
                || Ok(test_identity()),
            )
            .await
            .unwrap()
        };
        let link = || fs::read_link(executable.path()).unwrap();

        // A host with nothing installed has nothing to lose: the first install is admitted.
        assert!(matches!(
            attempt(&installed, Downgrade::Refuse).await,
            Some(ServiceBinaryRefresh::Refreshed { .. })
        ));
        assert_eq!(link(), executable.stored_link(&installed.digest));

        for refused in [&lacking, &blind] {
            let Some(ServiceBinaryRefresh::Downgrade {
                service,
                reason,
                remedy,
            }) = attempt(refused, Downgrade::Refuse).await
            else {
                panic!("{} must be refused", refused.source.display());
            };
            assert_eq!(service, "dev.cowshed.gateway");
            assert!(
                reason.contains(refused.source.to_str().unwrap()),
                "the refusal names the binary it would have installed: {reason}"
            );
            assert!(remedy.contains("--downgrade"), "{remedy}");
            assert_eq!(
                link(),
                executable.stored_link(&installed.digest),
                "a refused install changes nothing"
            );
            assert_eq!(stored(&executable), [short(&installed)]);
        }
        let Some(ServiceBinaryRefresh::Downgrade { reason, .. }) =
            attempt(&lacking, Downgrade::Refuse).await
        else {
            panic!("the sibling is refused");
        };
        assert!(
            reason.contains("fix(cowshed): judge strays by the project ignore rules"),
            "the refusal names the fix the sibling would remove, although its clock is later: \
             {reason}"
        );

        // The installed build again replaces nothing, and the build of main holding the fix
        // installs.
        assert_eq!(attempt(&installed, Downgrade::Refuse).await, None);
        assert!(matches!(
            attempt(&containing, Downgrade::Refuse).await,
            Some(ServiceBinaryRefresh::Refreshed { .. })
        ));
        assert_eq!(link(), executable.stored_link(&containing.digest));
        // Each install keeps the build it replaced, and prunes the one before.
        assert_eq!(
            stored(&executable),
            sorted(vec![short(&installed), short(&containing)])
        );

        // Going back needs the flag, and then it is just an install.
        assert!(matches!(
            attempt(&lacking, Downgrade::Allow).await,
            Some(ServiceBinaryRefresh::Refreshed { .. })
        ));
        assert_eq!(link(), executable.stored_link(&lacking.digest));
        assert_eq!(
            stored(&executable),
            sorted(vec![short(&containing), short(&lacking)]),
            "the build before the replaced one is pruned"
        );
        assert!(
            !build_record_path(&executable, &installed.digest).exists(),
            "a pruned copy takes its build record with it"
        );
        fs::remove_dir_all(home).unwrap();
    }

    /// An installed cowshed that records nothing — one from before records existed, or the plain
    /// binary of the old layout — names no commit to keep, so it is never an obstacle and no main
    /// is consulted: the first install of a build that records is how the host starts having
    /// records at all.
    #[tokio::test]
    async fn an_installed_cowshed_that_records_nothing_refuses_no_replacement() {
        let (home, executable, _) = host_with_agent("legacy-install");
        fs::write(executable.path(), b"installed before records existed").unwrap();
        fs::set_permissions(
            executable.path(),
            std::os::unix::fs::PermissionsExt::from_mode(STABLE_BINARY_MODE),
        )
        .unwrap();
        let candidate = build(&home, "old-checkout", b"an old build", Some(10));
        let unconsulted = async || -> Result<install_basis::Mains> {
            unreachable!("nothing installed records a commit, so no main is consulted")
        };
        check_basis(&executable, &candidate, Downgrade::Refuse, unconsulted)
            .await
            .expect("the plain binary of the old layout records nothing");

        let mut executor: RecordingExecutor = LaunchdExecutor::new(
            NativeFilesystem::new(),
            RecordingGatewayLaunchctl::default(),
        );
        assert!(matches!(
            refresh_gateway_from(
                HostNamespace::Production,
                &home,
                IncomingBuild {
                    candidate: &candidate,
                    debug_build: false,
                    basis: Ok(())
                },
                None,
                None,
                &mut executor,
                || Ok(test_bun(&home)),
                || Ok(test_identity())
            )
            .await
            .unwrap(),
            Some(ServiceBinaryRefresh::Refreshed { .. })
        ));
        assert_eq!(
            fs::read_link(executable.path()).unwrap(),
            executable.stored_link(&candidate.digest),
            "the plain binary is replaced by a link"
        );
        assert_eq!(
            stored(&executable).len(),
            2,
            "the plain binary survives as the stored copy of the build it was"
        );

        // A stored copy with no record beside it names no commit either.
        let record = build_record_path(&executable, &candidate.digest);
        let written = fs::read(&record).expect("the install wrote its record");
        fs::remove_file(&record).unwrap();
        let other = build(&home, "other", b"another build", Some(5));
        let unconsulted = async || -> Result<install_basis::Mains> {
            unreachable!("a copy without a record names no commit, so no main is consulted")
        };
        check_basis(&executable, &other, Downgrade::Refuse, unconsulted)
            .await
            .expect("an absent record is nothing to keep");

        // With the record back, a build no main can judge is refused rather than guessed.
        fs::write(&record, &written).unwrap();
        let unadopted = crate::install_basis::tests::Main::new("unadopted");
        let refusal = check_basis(&executable, &other, Downgrade::Refuse, async || {
            Ok(install_basis::Mains {
                roots: Vec::new(),
                ..unadopted.mains()
            })
        })
        .await
        .expect_err("a build no main can judge is refused");
        assert!(
            refusal.message.contains("no project is adopted"),
            "{}",
            refusal.message
        );

        // A record that is there but unreadable says a commit was installed and will not say
        // which: Integrity, never read as absent.
        fs::write(&record, b"{\"commit\":").unwrap();
        let unconsulted = async || -> Result<install_basis::Mains> {
            unreachable!("an unreadable record refuses before any main is consulted")
        };
        let corrupt = check_basis(&executable, &other, Downgrade::Refuse, unconsulted)
            .await
            .expect_err("an unreadable record refuses");
        assert_eq!(corrupt.code.as_str(), "integrity", "{}", corrupt.message);
        assert!(
            corrupt.message.contains(&record.display().to_string()),
            "{}",
            corrupt.message
        );
        fs::remove_dir_all(home).unwrap();
    }

    /// A failed activation must leave the host's build exactly as it was found: the stable name
    /// runs the build it ran, and the copy of the build the failed install brought is no one's
    /// business. The `cargo` link an earlier install planted is gone before the agent is asked to
    /// load and stays gone: it is no build's, and nothing plants it again.
    #[test]
    fn a_failed_activation_puts_the_previous_build_back() {
        struct RefusingLaunchctl {
            stable: PathBuf,
            cargo: PathBuf,
            candidate: ExecutableDigest,
            saw_candidate: bool,
        }
        impl LaunchctlCommand for RefusingLaunchctl {
            fn run(
                &mut self,
                _: &Path,
                arguments: &[std::ffi::OsString],
                _: &cowshed_os::deadline::Deadline,
            ) -> std::result::Result<crate::launchd::LaunchctlOutput, cowshed_os::deadline::RunError>
            {
                if arguments[0] == "bootstrap" && !self.saw_candidate {
                    assert_eq!(executable_digest(&self.stable).unwrap(), self.candidate);
                    assert!(
                        fs::symlink_metadata(&self.cargo).is_err(),
                        "cargo is gone before the agent loads"
                    );
                    self.saw_candidate = true;
                }
                Ok(crate::launchd::LaunchctlOutput {
                    status: match arguments[0].to_str() {
                        Some("print") => crate::launchd::CommandStatus::ExitCode(113),
                        Some("bootstrap") => crate::launchd::CommandStatus::ExitCode(37),
                        _ => crate::launchd::CommandStatus::Success,
                    },
                    stdout: Vec::new(),
                    stderr: Vec::new(),
                })
            }
        }
        let (home, executable, spec) = host_with_agent("rollback");
        let first = build(&home, "first", b"the supervised release build", Some(100));
        let second = build(
            &home,
            "second",
            b"a build whose agent will not load",
            Some(200),
        );
        install_host_stable_executable(&mut LaunchdExecutor::native(), &home, &first.source)
            .unwrap();
        let cargo = executable.directory().join(CARGO_LINK_NAME);
        replace_symlink(Path::new(COWSHED_BINARY_NAME), &cargo).unwrap();
        assert_eq!(executable_digest(executable.path()).unwrap(), first.digest);
        let mut executor = LaunchdExecutor::new(
            NativeFilesystem::new(),
            RefusingLaunchctl {
                stable: executable.path().to_path_buf(),
                cargo: cargo.clone(),
                candidate: second.digest.clone(),
                saw_candidate: false,
            },
        );

        let error = install_and_activate_gateway(
            &mut executor,
            &home,
            &second,
            spec.target(),
            InstallOutcome::Changed,
            test_identity(),
        )
        .expect_err("launchctl refuses");
        assert!(
            error.message.contains("this host is as it was found"),
            "{}",
            error.message
        );
        assert_eq!(
            fs::read_link(executable.path()).unwrap(),
            executable.stored_link(&first.digest),
            "the stable name runs the build it ran"
        );
        assert_eq!(
            fs::read(executable.path()).unwrap(),
            b"the supervised release build"
        );
        assert_eq!(executable_digest(executable.path()).unwrap(), first.digest);
        assert_eq!(
            fs::symlink_metadata(&cargo).err().map(|error| error.kind()),
            Some(io::ErrorKind::NotFound),
            "the rollback plants no cargo link"
        );
        fs::remove_dir_all(home).unwrap();
    }

    /// The defect a review found: a bootout that went overdue after `launchctl bootout` exited
    /// non-zero was read as an ordinary failure, and the restore then kickstarted a job launchd
    /// still tore down (launchctl 37). Whatever the bootout answered, an overdue bootout is
    /// overdue: the error says so, carries a refused bootout's answer as context, and the
    /// restore puts the previous build back and leaves the agent alone — no kickstart and no
    /// bootstrap. launchd here holds the job throughout and reports an exit timeout of 0 s, so
    /// the bootout is overdue after the 1 s unload grace.
    #[test]
    fn an_overdue_bootout_is_never_kickstarted_whatever_launchctl_bootout_answered() {
        struct TearingDown {
            bootout: crate::launchd::CommandStatus,
            argv: Vec<String>,
        }
        impl LaunchctlCommand for TearingDown {
            fn run(
                &mut self,
                executable: &Path,
                arguments: &[std::ffi::OsString],
                _: &cowshed_os::deadline::Deadline,
            ) -> std::result::Result<crate::launchd::LaunchctlOutput, cowshed_os::deadline::RunError>
            {
                // The gateway host's signature is not launchd's business.
                if executable == Path::new(code_signing::CODESIGN) {
                    return Ok(crate::launchd::LaunchctlOutput::success());
                }
                let verb = arguments[0].to_string_lossy().into_owned();
                let (status, stdout) = match verb.as_str() {
                    "print" => (
                        crate::launchd::CommandStatus::Success,
                        b"gui/501/dev.cowshed.gateway = {\n\tstate = not running\n\texit timeout = 0\n}\n"
                            .to_vec(),
                    ),
                    "bootout" => (self.bootout, Vec::new()),
                    _ => (crate::launchd::CommandStatus::Success, Vec::new()),
                };
                self.argv.push(verb);
                Ok(crate::launchd::LaunchctlOutput {
                    status,
                    stdout,
                    stderr: b"Boot-out failed: 5: Input/output error".to_vec(),
                })
            }
        }
        for (case, bootout) in [
            ("refused", crate::launchd::CommandStatus::ExitCode(5)),
            ("accepted", crate::launchd::CommandStatus::Success),
        ] {
            let (home, executable, spec) = host_with_agent(&format!("overdue-{case}"));
            let first = build(&home, "first", b"the supervised release build", Some(100));
            let second = build(&home, "second", b"a build launchd never ran", Some(200));
            install_host_stable_executable(&mut LaunchdExecutor::native(), &home, &first.source)
                .unwrap();
            let mut executor = LaunchdExecutor::new(
                NativeFilesystem::new(),
                TearingDown {
                    bootout,
                    argv: Vec::new(),
                },
            );
            let error = install_and_activate_gateway(
                &mut executor,
                &home,
                &second,
                spec.target(),
                InstallOutcome::Changed,
                test_identity(),
            )
            .expect_err("the bootout went overdue");
            assert_eq!(error.code, ErrorCode::EnvironmentMissing, "{case}");
            assert!(
                error.message.contains("did not finish booting out")
                    && error
                        .message
                        .contains("its process exited and launchd still holds the job"),
                "{case}: {}",
                error.message
            );
            assert_eq!(
                error.message.contains("the bootout itself answered"),
                case == "refused",
                "{case}: {}",
                error.message
            );
            assert!(
                error.message.contains("this host is as it was found")
                    && error.message.contains("is not loaded again"),
                "{case}: {}",
                error.message
            );
            assert!(
                error.hint.contains("launchctl bootstrap gui/"),
                "{case}: {}",
                error.hint
            );
            let (_, command) = executor.into_parts();
            assert!(
                command
                    .argv
                    .iter()
                    .all(|verb| verb == "print" || verb == "bootout"),
                "{case}: nothing but print and one bootout: {:?}",
                command.argv
            );
            assert_eq!(
                command
                    .argv
                    .iter()
                    .filter(|verb| *verb == "bootout")
                    .count(),
                1,
                "{case}: {:?}",
                command.argv
            );
            assert_eq!(
                fs::read_link(executable.path()).unwrap(),
                executable.stored_link(&first.digest),
                "{case}: the stable name runs the build it ran"
            );
            fs::remove_dir_all(home).unwrap();
        }
    }

    /// What one setup refresh of `candidate` did on `home`: its answer, the launchctl verbs it
    /// ran and the `codesign` argv it ran, under the recording launchctl `command`.
    async fn refresh_recording(
        home: &Path,
        candidate: &Candidate,
        command: RecordingGatewayLaunchctl,
        identity: SigningIdentity,
    ) -> (
        Option<ServiceBinaryRefresh>,
        Vec<Vec<std::ffi::OsString>>,
        Vec<Vec<std::ffi::OsString>>,
    ) {
        let mut executor: RecordingExecutor =
            LaunchdExecutor::new(NativeFilesystem::new(), command);
        let refreshed = refresh_gateway_from(
            HostNamespace::Production,
            home,
            IncomingBuild {
                candidate,
                debug_build: false,
                basis: Ok(()),
            },
            None,
            None,
            &mut executor,
            || Ok(test_bun(home)),
            || Ok(identity),
        )
        .await
        .unwrap();
        let (_, command) = executor.into_parts();
        (refreshed, command.argv, command.codesign)
    }

    /// Setup installs the gateway host as a signed copy of the build, and rewrites it when a
    /// setup moves the stable name to another build, when it is gone, or when it no longer
    /// satisfies the identity's requirement; a setup of the build already installed only checks
    /// its signature and leaves the file alone. A rewrite never restarts the gateway by itself.
    #[tokio::test]
    async fn setup_signs_the_gateway_host_and_rewrites_it_only_when_it_must() {
        let (home, executable, _) = host_with_agent("gateway-host");
        let host = executable.gateway_host();
        let first = build(&home, "first", b"the first release", None);
        let second = build(&home, "second", b"the second release", None);
        let identity = test_identity();
        let signed_once = |codesign: &[Vec<std::ffi::OsString>]| {
            assert_eq!(codesign.len(), 1, "{codesign:?}");
            assert_eq!(
                codesign[0][..7],
                identity.sign_arguments(&host)[..7],
                "{codesign:?}"
            );
        };

        let (refreshed, _, codesign) = refresh_recording(
            &home,
            &first,
            RecordingGatewayLaunchctl::default(),
            identity,
        )
        .await;
        assert!(refreshed.is_some());
        signed_once(&codesign);
        assert_eq!(fs::read(&host).unwrap(), b"the first release");
        assert_eq!(
            fs::metadata(&host).unwrap().mode() & 0o777,
            STABLE_BINARY_MODE
        );
        let inode = fs::metadata(&host).unwrap().ino();

        let (refreshed, argv, codesign) = refresh_recording(
            &home,
            &first,
            RecordingGatewayLaunchctl::default(),
            identity,
        )
        .await;
        assert_eq!(refreshed, None);
        assert!(argv.is_empty(), "{argv:?}");
        assert_eq!(codesign, [identity.verify_arguments(&host)]);
        assert_eq!(
            fs::metadata(&host).unwrap().ino(),
            inode,
            "a current host is left alone"
        );

        let (_, _, codesign) = refresh_recording(
            &home,
            &first,
            RecordingGatewayLaunchctl {
                unsatisfied: true,
                ..RecordingGatewayLaunchctl::default()
            },
            identity,
        )
        .await;
        assert_eq!(codesign.len(), 2, "verified, then signed: {codesign:?}");
        assert_eq!(codesign[0], identity.verify_arguments(&host));
        signed_once(&codesign[1..]);

        let (refreshed, _, codesign) = refresh_recording(
            &home,
            &second,
            RecordingGatewayLaunchctl::default(),
            identity,
        )
        .await;
        assert!(refreshed.is_some());
        signed_once(&codesign);
        assert_eq!(fs::read(&host).unwrap(), b"the second release");

        fs::remove_file(&host).unwrap();
        let (refreshed, argv, codesign) = refresh_recording(
            &home,
            &second,
            RecordingGatewayLaunchctl::default(),
            identity,
        )
        .await;
        assert_eq!(refreshed, None, "a repaired host restarts nothing");
        assert!(argv.is_empty(), "{argv:?}");
        signed_once(&codesign);
        assert_eq!(fs::read(&host).unwrap(), b"the second release");
        fs::remove_dir_all(home).unwrap();
    }

    /// The proof with a throwaway keychain: its home is a scratch directory, and its search-list
    /// entry and keychain are removed afterwards. Run first, before the user's own:
    /// `cargo test -p cowshed-cli --lib host_proof -- --ignored --nocapture`.
    #[cfg(target_os = "macos")]
    #[ignore = "host proof: adds a throwaway keychain to this user's search list for its run"]
    #[tokio::test]
    async fn host_proof_a_throwaway_keychain_signs_without_a_dialog() {
        let keychain_home = scratch_root("host-proof-keychain");
        let keychain = Keychain::cowshed(&keychain_home);
        prove_one_requirement_across_two_setups(&keychain_home).await;
        let listed = std::process::Command::new(code_signing::SECURITY)
            .args(["list-keychains", "-d", "user"])
            .output_bounded()
            .unwrap();
        let kept: Vec<String> = String::from_utf8_lossy(&listed.stdout)
            .lines()
            .filter_map(|line| {
                line.trim()
                    .strip_prefix('"')?
                    .strip_suffix('"')
                    .map(str::to_owned)
            })
            .filter(|entry| Path::new(entry) != keychain.path())
            .collect();
        let restored = std::process::Command::new(code_signing::SECURITY)
            .args(["list-keychains", "-d", "user", "-s"])
            .args(&kept)
            .status_bounded()
            .unwrap();
        assert!(restored.success());
        let _ = std::process::Command::new(code_signing::SECURITY)
            .arg("delete-keychain")
            .arg(keychain.path())
            .status_bounded();
        fs::remove_dir_all(keychain_home).unwrap();
    }

    /// The same proof with this user's own cowshed keychain, the one setup uses.
    #[cfg(target_os = "macos")]
    #[ignore = "host proof: makes or reuses this user's cowshed signing keychain and identity"]
    #[tokio::test]
    async fn host_proof_the_gateway_host_keeps_its_designated_requirement_across_two_setups() {
        prove_one_requirement_across_two_setups(&canonical_home().unwrap()).await;
    }

    /// Host proof (05_gateway.md "The gateway host"): two setups of two different builds into an
    /// isolated home — its own `bin`, its own plist, launchd recorded and never asked — sign the
    /// gateway host with an identity in cowshed's keychain under `keychain_home`, made there when
    /// it holds none, and the host's designated requirement is the same after both while its code
    /// is not. Every step is given what it needs, so none can raise a dialog.
    #[cfg(target_os = "macos")]
    async fn prove_one_requirement_across_two_setups(keychain_home: &Path) {
        let identity = gateway_host_identity(keychain_home).unwrap();
        let (home, executable, _) = host_with_agent("host-proof");
        let host = executable.gateway_host();
        let mut seen = Vec::new();
        for (name, program) in [("first", "/usr/bin/true"), ("second", "/usr/bin/false")] {
            let candidate = build(&home, name, &fs::read(program).unwrap(), None);
            refresh_recording(
                &home,
                &candidate,
                RecordingGatewayLaunchctl {
                    real_codesign: true,
                    ..RecordingGatewayLaunchctl::default()
                },
                identity,
            )
            .await;
            let shown = std::process::Command::new(code_signing::CODESIGN)
                .args(["-dvvv", "-r-"])
                .arg(&host)
                .output_bounded()
                .unwrap();
            let text = format!(
                "{}{}",
                String::from_utf8_lossy(&shown.stderr),
                String::from_utf8_lossy(&shown.stdout)
            );
            cowshed_os::eprintln!(
                "setup {name} ({program}): codesign -dvvv -r- {}\n{text}",
                host.display()
            );
            let line = |prefix: &str| {
                text.lines()
                    .find_map(|line| line.strip_prefix(prefix))
                    .unwrap_or_else(|| panic!("no {prefix} in {text}"))
                    .to_owned()
            };
            seen.push((line("designated => "), line("CDHash=")));
        }
        assert_eq!(seen[0].0, identity.designated_requirement());
        assert_eq!(seen[0].0, seen[1].0, "one requirement across both setups");
        assert_ne!(seen[0].1, seen[1].1, "two setups installed two builds");
        fs::remove_dir_all(home).unwrap();
    }

    /// A build whose package has no Nx cache server dependency cannot be installed: the refresh
    /// names the missing companion and stops before any launchctl call or change to the stable name.
    #[tokio::test]
    async fn a_refresh_without_the_packaged_companion_fails_before_activation() {
        let (home, executable, _) = host_with_agent("missing-companion");
        let candidate = build(
            &home,
            "no-companion",
            b"a build with no server dependency",
            None,
        );
        fs::remove_file(distribution_companion(&home)).unwrap();
        let mut executor: RecordingExecutor = LaunchdExecutor::new(
            NativeFilesystem::new(),
            RecordingGatewayLaunchctl::default(),
        );
        let error = refresh_gateway_from(
            HostNamespace::Production,
            &home,
            IncomingBuild {
                candidate: &candidate,
                debug_build: false,
                basis: Ok(()),
            },
            None,
            None,
            &mut executor,
            || Ok(test_bun(&home)),
            || Ok(test_identity()),
        )
        .await
        .expect_err("the companion is required");
        assert!(
            error.message.contains("nx-cache-server"),
            "{}",
            error.message
        );
        let (_, command) = executor.into_parts();
        assert!(command.argv.is_empty(), "{:?}", command.argv);
        assert!(!executable.path().exists(), "the stable name is untouched");
        fs::remove_dir_all(home).unwrap();
    }

    /// A distribution's server is its package's `@smoothbricks/nx-cache-server` bundle, found as
    /// Node finds the dependency: the isolated store's sibling `node_modules` beside the package
    /// wins over a hoisted copy further up, and nothing beside the cowshed binary is consulted.
    #[test]
    fn a_distribution_resolves_its_server_from_the_nearest_dependency() {
        let home = scratch_root("companion-resolution");
        let installed = HostStableExecutable::new(&home, "nx-cache-server").unwrap();
        let store = home.join("node_modules/.bun/@smoothbricks+cowshed@0.2.4/node_modules");
        let binary = store.join("@smoothbricks/cowshed/dist/bin/darwin-arm64/cowshed");
        fs::create_dir_all(binary.parent().unwrap()).unwrap();
        fs::write(&binary, b"cowshed").unwrap();
        fs::write(binary.with_file_name("nx-cache-server"), b"a stale copy").unwrap();
        let artifact = Path::new(NX_CACHE_SERVER_BUNDLE)
            .strip_prefix("node_modules")
            .unwrap();
        let hoisted = home.join("node_modules").join(artifact);
        fs::create_dir_all(hoisted.parent().unwrap()).unwrap();
        fs::write(&hoisted, b"hoisted").unwrap();
        assert_eq!(
            packaged_nx_cache_server(&installed, &binary).unwrap(),
            hoisted
        );
        let isolated = store.join(artifact);
        fs::create_dir_all(isolated.parent().unwrap()).unwrap();
        fs::write(&isolated, b"isolated").unwrap();
        assert_eq!(
            packaged_nx_cache_server(&installed, &binary).unwrap(),
            isolated
        );
        fs::remove_dir_all(home).unwrap();
    }

    /// The install slot keeps the server its install put beside the stored copies, and a binary
    /// outside both a slot and a `dist/bin/<platform>` layout is refused by name.
    #[test]
    fn the_install_slot_keeps_its_server_and_a_loose_binary_has_none() {
        let home = scratch_root("companion-slot");
        let installed = HostStableExecutable::new(&home, "nx-cache-server").unwrap();
        let stored = installed.directory().join("cowshed-0123");
        assert_eq!(
            packaged_nx_cache_server(&installed, &stored).unwrap(),
            installed.path()
        );
        let loose = home.join("tools/cowshed");
        let error = packaged_nx_cache_server(&installed, &loose).expect_err("no layout");
        assert!(
            error.message.contains("dist/bin/<platform>"),
            "{}",
            error.message
        );
        fs::remove_dir_all(home).unwrap();
    }

    fn scratch_root(label: &str) -> PathBuf {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let root =
            std::env::temp_dir().join(format!("cowshed-{label}-{}-{nonce}", std::process::id()));
        fs::create_dir_all(&root).expect("scratch root");
        root
    }

    /// A gateway-owned directory is created through an `O_NOFOLLOW` open at exactly 0700, and a
    /// symlink planted where one belongs is refused rather than written through.
    ///
    /// The second half is why the ancestor case is here too: the check this replaced was
    /// `create_dir_all` and then `canonicalize(path) == path`, which refuses every legitimate
    /// directory reached through a symlinked ancestor — a home on a linked volume, or any path
    /// under macOS's own `/var` link — while still writing the tree before it noticed.
    #[test]
    fn a_gateway_directory_is_private_under_a_linked_ancestor_and_never_written_through_a_plant() {
        let root = scratch_root("private-dir");
        let real = root.join("real");
        fs::create_dir_all(&real).expect("real parent");
        let ancestor = root.join("linked");
        std::os::unix::fs::symlink(&real, &ancestor).expect("link the ancestor");

        let telemetry = ancestor.join("telemetry");
        ensure_private_directory(&telemetry).expect("a linked ancestor is not a plant");
        assert_eq!(
            fs::symlink_metadata(real.join("telemetry"))
                .expect("created")
                .permissions()
                .mode()
                & 0o777,
            PRIVATE_DIRECTORY_MODE
        );

        let elsewhere = root.join("elsewhere");
        fs::create_dir_all(&elsewhere).expect("target of the plant");
        let planted = root.join("planted");
        std::os::unix::fs::symlink(&elsewhere, &planted).expect("plant the symlink");

        ensure_private_directory(&planted).expect_err("a planted symlink is refused");
        assert!(
            fs::read_dir(&elsewhere)
                .expect("target still readable")
                .next()
                .is_none(),
            "nothing may be created through the link"
        );
        assert!(
            fs::symlink_metadata(&planted)
                .expect("plant survives")
                .file_type()
                .is_symlink(),
            "the plant is left for the operator to look at"
        );

        fs::remove_dir_all(&root).ok();
    }

    fn daemon(draining: bool, cause: Option<&str>, sha256: Option<&str>) -> DaemonStatus {
        DaemonStatus {
            version: "0.1.0".into(),
            draining,
            drain_cause: cause.map(str::to_owned),
            executable_sha256: sha256.map(str::to_owned),
            healing: None,
            recovering: None,
            sessions: Vec::new(),
            active: 0,
            queued: 0,
        }
    }

    /// Healthy means answering, serving, and running this CLI's bytes; the version string, the
    /// same for every build, decides nothing.
    #[test]
    fn a_daemon_is_healthy_only_when_it_serves_and_runs_the_cli_bytes() {
        let socket = PathBuf::from("/private/cowshed/store/gateway.sock");
        let healthy = cli_status(
            true,
            socket.clone(),
            Some(&daemon(false, None, Some("aa"))),
            "aa",
        );
        assert!(healthy.running);
        assert_eq!((healthy.drain_cause, healthy.stale_daemon), (None, None));

        let stale = cli_status(
            true,
            socket.clone(),
            Some(&daemon(false, None, Some("bb"))),
            "aa",
        );
        assert_eq!(
            stale.stale_daemon,
            Some(StaleDaemonBinary {
                daemon_sha256: Some("bb".into()),
                cli_sha256: "aa".into(),
            })
        );

        // A daemon from before the digest was reported is older than any build that asks.
        let unreported = cli_status(true, socket.clone(), Some(&daemon(false, None, None)), "aa");
        assert_eq!(
            unreported.stale_daemon.map(|stale| stale.daemon_sha256),
            Some(None)
        );

        let draining = cli_status(
            true,
            socket.clone(),
            Some(&daemon(true, Some("audit sink failed"), Some("aa"))),
            "aa",
        );
        assert_eq!(draining.drain_cause.as_deref(), Some("audit sink failed"));

        let silent = cli_status(true, socket.clone(), None, "aa");
        assert!(!silent.running);
        assert_eq!((silent.drain_cause, silent.stale_daemon), (None, None));
    }

    /// A job is counted while it is live, in one row per class and state; an ended job is in no
    /// row, however many there are.
    #[test]
    fn only_live_jobs_are_counted_one_row_per_class_and_state() {
        let counts = count_jobs([
            (ExecClass::Background, JobState::Running),
            (ExecClass::Gate, JobState::Running),
            (ExecClass::Background, JobState::Exited),
            (ExecClass::Background, JobState::Running),
            (ExecClass::Background, JobState::Queued),
            (ExecClass::Gate, JobState::Failed),
            (ExecClass::Resident, JobState::Draining),
        ]);
        assert_eq!(
            counts,
            [
                JobCount {
                    class: ExecClass::Background,
                    state: JobState::Running,
                    jobs: 2
                },
                JobCount {
                    class: ExecClass::Gate,
                    state: JobState::Running,
                    jobs: 1
                },
                JobCount {
                    class: ExecClass::Background,
                    state: JobState::Queued,
                    jobs: 1
                },
                JobCount {
                    class: ExecClass::Resident,
                    state: JobState::Draining,
                    jobs: 1
                },
            ]
        );
    }

    /// The daemon's control socket and the startup pass it reports — mounts still healing,
    /// supervisors still recovering — composed as `run_daemon` composes them, with each pass held
    /// where the test wants it.
    #[cfg(target_os = "macos")]
    mod startup {
        use std::num::NonZeroUsize;

        use cowshed_core::metadata::WorkspaceName;
        use cowshed_core::runtime::supervisor_manager::{
            Draining, Recovery, SupervisorPeers, SupervisorSpawner,
        };
        use cowshed_core::runtime::supervisor_socket::Hello;
        use cowshed_gateway::{
            AuditError, AuditEvent, AuditSink, AuthorizedTarget, CanonicalTarget, ConnectError,
            CredentialError, CredentialProvider, CredentialQuery, CredentialRecord,
            UpstreamConnection, UpstreamConnector, UpstreamHealth,
        };

        use super::*;

        struct NoCredentials;

        #[async_trait]
        impl CredentialProvider for NoCredentials {
            async fn lookup(
                &self,
                _: &CredentialQuery,
            ) -> std::result::Result<Option<CredentialRecord>, CredentialError> {
                Ok(None)
            }
        }

        struct NoConnector;

        #[async_trait]
        impl UpstreamConnector for NoConnector {
            async fn health(&self, _: &CanonicalTarget) -> UpstreamHealth {
                UpstreamHealth::Unknown
            }

            async fn connect(
                &self,
                _: &AuthorizedTarget,
            ) -> std::result::Result<UpstreamConnection, ConnectError> {
                Err(ConnectError::NoAddresses)
            }
        }

        struct DiscardAudit;

        #[async_trait]
        impl AuditSink for DiscardAudit {
            async fn record(&self, _: AuditEvent) -> std::result::Result<(), AuditError> {
                Ok(())
            }

            async fn flush(&self) -> std::result::Result<(), AuditError> {
                Ok(())
            }
        }

        struct NoSpawner;

        impl SupervisorSpawner for NoSpawner {
            fn spawn(
                &self,
                _: &Path,
                _: &WorkspaceName,
                _: io::PipeWriter,
            ) -> io::Result<tokio::process::Child> {
                Err(io::Error::other("this manager starts no supervisor"))
            }
        }

        /// Supervisors of another build whose drains answer only as the test opens `gate`.
        struct GatedPeers {
            gate: tokio::sync::Semaphore,
        }

        #[async_trait]
        impl SupervisorPeers for GatedPeers {
            async fn hello_if_present(&self, socket: &Path) -> Result<Option<Hello>> {
                Err(CowshedError::conflict(
                    format!("{} is served by another build", socket.display()),
                    "let it drain",
                ))
            }

            async fn drain(&self, _: &Path) -> Result<Draining> {
                self.gate.acquire().await.expect("the gate opens").forget();
                Ok(Draining {
                    pid: 4242,
                    exit: Box::new(|| Ok(())),
                })
            }
        }

        /// An inventory a refused reconcile never reaches.
        struct UnreadInventory;

        #[async_trait]
        impl SessionInventory for UnreadInventory {
            fn fixture_port_claims(
                &self,
            ) -> Option<&cowshed_core::storage::bootstrap::FixturePortClaims> {
                None
            }
            async fn all_sessions(
                &self,
            ) -> Result<cowshed_core::gateway_sessions::InventorySessions> {
                Err(CowshedError::internal("the inventory was read"))
            }

            async fn project_sessions(
                &self,
                _: &RepoId,
            ) -> Result<cowshed_core::gateway_sessions::InventorySessions> {
                Err(CowshedError::internal("the inventory was read"))
            }

            async fn workspace_session(
                &self,
                _: &RepoId,
                _: &cowshed_core::metadata::WorkspaceName,
            ) -> Result<Option<cowshed_core::gateway_sessions::WorkspaceSessionSnapshot>>
            {
                Err(CowshedError::internal("the inventory was read"))
            }
        }

        fn private_directory(path: &Path) {
            fs::create_dir(path).expect("a fixture directory");
            fs::set_permissions(path, fs::Permissions::from_mode(0o700))
                .expect("a private fixture directory");
        }

        /// A daemon's gateway, its supervisor manager, and the supervisors it found, composed
        /// as `run_daemon` composes them, under `root` with the socket of the supervisor of each
        /// of `workspaces` under `run`. The root goes with the daemon on every exit path.
        struct Daemon {
            _root: crate::temp_root::TempRoot,
            socket: PathBuf,
            gateway: Gateway,
            recovery: Recovery,
        }

        /// A daemon's private root, named after `name`.
        fn gateway_root(name: &str) -> crate::temp_root::TempRoot {
            // The scratch parent, not TMPDIR: the gateway socket's path has to fit in `sun_path`.
            crate::temp_root::TempRoot::new_in(&crate::temp_parent::scratch_parent(), name)
        }

        async fn daemon(
            root: crate::temp_root::TempRoot,
            workspaces: &[&str],
            heal: Arc<StartupHealState>,
        ) -> Daemon {
            let store = root.join("store");
            fs::create_dir_all(store.join("run")).expect("run directory");
            let repo = cowshed_types::repository::RepoId::parse("acme/widget").expect("repo id");
            for workspace in workspaces {
                let workspace =
                    cowshed_core::metadata::WorkspaceName::new(*workspace).expect("workspace");
                fs::write(
                    supervisor_socket::socket_path(&store, &repo, &workspace),
                    b"",
                )
                .expect("a socket file");
            }
            let cache = root.join("cache");
            private_directory(&cache);
            let socket = root.join("gateway.sock");
            let manager = SupervisorManager::new(&store, Box::new(NoSpawner), Arc::clone(&heal));
            let recovery = manager.recovery();
            let gateway = Gateway::start(
                GatewayConfig {
                    control_socket: Some(socket.clone()),
                    mirror_cache: MirrorCacheConfig::new(cache),
                    startup: Some(Arc::new(DaemonStartup { heal, manager })),
                    // The CLI asking runs the same bytes, so only the startup pass is reported.
                    executable_sha256: Some("aa".into()),
                    ..GatewayConfig::default()
                },
                Arc::new(NoCredentials),
                Arc::new(NoConnector),
                Arc::new(DiscardAudit),
            )
            .await
            .expect("start the gateway");
            Daemon {
                _root: root,
                socket,
                gateway,
                recovery,
            }
        }

        /// What `gateway status` prints for the daemon's answer.
        fn rendered(socket: &Path, status: &DaemonStatus) -> String {
            let mut output = Output::new(Vec::new(), Vec::new(), false);
            emit_gateway_status(
                &mut output,
                false,
                cli_status(true, socket.to_path_buf(), Some(status), "aa"),
            )
            .expect("render");
            String::from_utf8(output.into_inner().1).expect("utf-8")
        }

        /// The control socket answers while supervisors of another build from before the daemon
        /// still drain, and its status counts them; `gateway status` says so instead of calling
        /// the gateway unavailable, and once they are drained the count is gone.
        #[tokio::test]
        async fn the_control_socket_counts_supervisors_still_draining() {
            let daemon = daemon(
                gateway_root("csrecover"),
                &["first", "second"],
                Arc::new(StartupHealState::healed()),
            )
            .await;
            let peers = Arc::new(GatedPeers {
                gate: tokio::sync::Semaphore::new(0),
            });
            let gated: Arc<GatedPeers> = Arc::clone(&peers);
            let recovered = daemon.recovery.start(gated);
            let client =
                GatewayControlClient::new(daemon.socket.clone()).expect("a control client");

            let draining = client.status().await.expect("status while draining");
            let two = SupervisorRecovery {
                supervisors: NonZeroUsize::new(2).expect("two"),
            };
            assert_eq!(draining.recovering, Some(two));
            assert_eq!(
                cli_status(true, daemon.socket.clone(), Some(&draining), "aa").recovering,
                Some(two)
            );
            let text = rendered(&daemon.socket, &draining);
            assert!(
                text.contains("still recovering 2 workspace supervisors"),
                "{text}"
            );

            peers.gate.add_permits(2);
            recovered.await.expect("the recovery ends");
            let drained = client.status().await.expect("status once drained");
            assert_eq!(drained.recovering, None);

            daemon.gateway.drain().await.expect("drain the gateway");
        }

        /// The control socket answers from the first moment, while the startup pass still mounts
        /// projects: status says how many are left, `gateway status` says the gateway is still
        /// starting, and every session change — over the socket or through a command's reconcile
        /// — is refused by type until the pass has restored the sessions; then it is served.
        #[tokio::test]
        async fn the_control_socket_answers_while_the_startup_pass_mounts() {
            let heal = Arc::new(StartupHealState::mounting(2));
            let daemon = daemon(gateway_root("csheal"), &[], Arc::clone(&heal)).await;
            let client =
                GatewayControlClient::new(daemon.socket.clone()).expect("a control client");
            let mounting = StartupHeal::Mounting {
                projects: NonZeroUsize::new(2).expect("two"),
            };

            let starting = client.status().await.expect("status while mounting");
            assert_eq!(starting.healing, Some(mounting));
            let text = rendered(&daemon.socket, &starting);
            assert!(
                text.contains("still starting: mounting 2 adopted projects"),
                "{text}"
            );
            assert!(
                matches!(
                    client.remove("pnone.wnone", 1).await,
                    Err(ControlError::Rejected {
                        code: ControlFailureCode::Healing,
                        ..
                    })
                ),
                "a session change waits for the restore"
            );
            let repo = RepoId::parse("acme/widget").expect("repo");
            let control = ControlSocket::at(daemon.socket.clone()).expect("control socket");
            let refused = reconcile_project(&control, &UnreadInventory, &repo, effective_uid())
                .await
                .expect_err("a command's reconcile is refused while the gateway is starting");
            assert_eq!(
                refused.healing_source(),
                Some(mounting),
                "{}",
                refused.message
            );

            heal.restored();
            assert_eq!(
                client.status().await.expect("status once healed").healing,
                None
            );
            assert!(
                matches!(
                    client.remove("pnone.wnone", 1).await,
                    Err(ControlError::Rejected {
                        code: ControlFailureCode::NotInstalled,
                        ..
                    })
                ),
                "the session change now reaches the gateway"
            );

            daemon.gateway.drain().await.expect("drain the gateway");
        }

        /// The measured failure: an install restarted the gateway between a land's steps, and the
        /// next step's connection was refused. An install now refuses at once, before it changes
        /// anything, while a land holds its turn, naming it; with none it goes on. It never waits.
        #[tokio::test]
        async fn an_install_refuses_while_a_land_turn_runs() {
            let home = super::scratch_root("install-refuses-land");
            let support = support_directory(&home);
            fs::create_dir_all(&support).expect("support directory");
            let slot = InstallSlot::take(&support).await.expect("the slot");
            refuse_restart_while_busy(&slot, &home).expect("nothing runs");

            let queue = turns::land_turn_root(&support).join("acme-widget");
            let turn = tokio::task::spawn_blocking(move || {
                turns::take(
                    &queue,
                    "widget-land",
                    "land",
                    None,
                    Duration::from_secs(90),
                    |_| {},
                )
            })
            .await
            .expect("the turn task")
            .expect("the turn");
            let refused =
                refuse_restart_while_busy(&slot, &home).expect_err("a land holds its turn");
            assert_eq!(refused.code, ErrorCode::Conflict);
            assert!(
                refused
                    .message
                    .contains("the land of widget-land holds the acme-widget land turn"),
                "{}",
                refused.message
            );
            drop(turn);
            refuse_restart_while_busy(&slot, &home).expect("nothing runs any more");
            drop(slot);
            fs::remove_dir_all(home).ok();
        }

        /// A gateway that takes the connection and never answers fails each status probe on its
        /// silence bound (05_gateway.md "Deadlines"), so `gateway start`'s ceiling fires and
        /// names the wait instead of waiting on one probe forever.
        #[tokio::test(start_paused = true)]
        async fn a_silent_gateway_fails_the_start_ceiling() {
            let support = gateway_root("csinstsilent");
            let slot = InstallSlot::take(&support).await.expect("the slot");
            let root = gateway_root("csinstsilent-gw");
            let socket = root.join("gateway.sock");
            // Bound and listening; nothing accepts a connection, reads a request or answers one.
            let _listener =
                tokio::net::UnixListener::bind(&socket).expect("the silent gateway's socket");
            let client = GatewayControlClient::new(socket.clone()).expect("client");
            let mut executor = LaunchdExecutor::new(
                NativeFilesystem::new(),
                RecordingGatewayLaunchctl::default(),
            );
            let target =
                LaunchAgentTarget::new(Path::new("/tmp"), "dev.cowshed.gateway").expect("target");
            let mut output = Output::new(Vec::new(), Vec::new(), false);
            // One probe fails on the silence a gateway that never heartbeated is allowed, as
            // `SILENT`: the probe is bounded by the silence, not cut short by a connect.
            let probe = client.status().await;
            assert!(
                matches!(
                    &probe,
                    Err(ControlError::Wait(WaitError::Silent(silent))) if silent.op == "status"
                ),
                "{probe:?}"
            );
            let started = tokio::time::Instant::now();
            let error = await_healthy(
                &mut output,
                &slot,
                &client,
                &socket,
                "aa",
                &mut executor,
                &target,
            )
            .await
            .expect_err("a silent gateway never becomes healthy");
            let waited = started.elapsed();
            assert!(
                error
                    .message
                    .contains("gateway did not become healthy within 180s of starting"),
                "{}",
                error.message
            );
            // The ceiling fires at the first probe that ends past it: within one probe's silence
            // bound of it.
            assert!(waited >= START_DEADLINE, "{waited:?}");
            assert!(
                waited < START_DEADLINE + Duration::from_secs(65),
                "{waited:?}"
            );
            let (_, launchctl) = executor.into_parts();
            assert!(
                launchctl.argv.is_empty(),
                "nothing restarts a gateway that never answered"
            );
        }

        /// The measured failure: `setup` restarted the gateway and released the install slot at
        /// once, and a land step that had waited on the slot found the daemon's socket refusing
        /// the connection, failing the land. An install now holds the slot until the gateway it
        /// started serves: before its control socket answers, and through its startup pass.
        #[tokio::test]
        async fn an_install_holds_the_slot_until_the_gateway_it_started_serves() {
            let support = gateway_root("csinstserve");
            let slot = InstallSlot::take(&support).await.expect("the slot");
            let root = gateway_root("csinstserve-gw");
            let socket = root.join("gateway.sock");
            let client = GatewayControlClient::new(socket.clone()).expect("client");
            let install = tokio::spawn({
                let (socket, client) = (socket.clone(), client.clone());
                async move {
                    let mut executor = LaunchdExecutor::new(
                        NativeFilesystem::new(),
                        RecordingGatewayLaunchctl::default(),
                    );
                    let target = LaunchAgentTarget::new(Path::new("/tmp"), "dev.cowshed.gateway")
                        .expect("target");
                    let mut output = Output::new(Vec::new(), Vec::new(), false);
                    let served = await_healthy(
                        &mut output,
                        &slot,
                        &client,
                        &socket,
                        "aa",
                        &mut executor,
                        &target,
                    )
                    .await;
                    drop(slot);
                    served
                }
            });
            let land = tokio::spawn({
                let (support, client) = (support.to_path_buf(), client.clone());
                async move {
                    crate::install_basis::wait_for_install(&support)
                        .await
                        .expect("the land step's wait");
                    client.status().await
                }
            });
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(!install.is_finished(), "no gateway answers yet");
            assert!(!land.is_finished(), "the land step waits for the install");

            let heal = Arc::new(StartupHealState::mounting(0));
            let daemon = daemon(root, &[], Arc::clone(&heal)).await;
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(
                !install.is_finished(),
                "the gateway answers but still heals"
            );
            assert!(!land.is_finished(), "the land step still waits");

            heal.restored();
            let served = install
                .await
                .expect("the install task")
                .expect("the gateway serves");
            assert_eq!(served.healing, None);
            let reached = land
                .await
                .expect("the land task")
                .expect("the land step reaches the gateway");
            assert_eq!(
                reached.healing, None,
                "the land step's first request finds the startup pass over"
            );

            daemon.gateway.drain().await.expect("drain the gateway");
        }
    }

    /// The guidance for an unavailable gateway has to work on a host where the
    /// launch agent was never installed, which is where it is reached from
    /// first. `launchctl kickstart` fails there with "service not found".
    #[test]
    fn absent_gateway_guidance_installs_rather_than_kickstarts() {
        let error = gateway_absent(501);

        assert_eq!(error.hint, GATEWAY_START_HINT);
        assert_eq!(error.hint, "cowshed gateway start");
        assert!(!error.hint.contains("launchctl"));
        assert_eq!(error.code.as_str(), "environment-missing");
    }

    /// The restart form stays available for guidance issued after a successful
    /// install, where the service does exist.
    #[test]
    fn kickstart_guidance_targets_the_per_user_domain() {
        assert_eq!(
            kickstart_hint(501, "dev.cowshed.gateway"),
            "launchctl kickstart -k gui/501/dev.cowshed.gateway"
        );
    }

    fn mounting(projects: usize) -> StartWait {
        StartWait::Startup(StartupHeal::Mounting {
            projects: std::num::NonZeroUsize::new(projects).expect("non-zero"),
        })
    }

    /// The wait stays quiet until an interval has passed, then speaks once per interval and names
    /// what the gateway says it is doing — mounting several multi-gigabyte images, not hanging.
    #[test]
    fn the_start_wait_reports_once_per_interval_with_the_project_count() {
        let mut progress = StartProgress::new();

        assert_eq!(progress.line(Duration::from_secs(0), mounting(7)), None);
        assert_eq!(
            progress.line(
                START_PROGRESS_INTERVAL - Duration::from_millis(1),
                mounting(7)
            ),
            None
        );
        assert_eq!(
            progress.line(START_PROGRESS_INTERVAL, mounting(7)),
            Some(String::from(
                "waited 5s for the gateway: mounting 7 adopted projects…"
            ))
        );
        assert_eq!(progress.line(START_PROGRESS_INTERVAL, mounting(7)), None);
        assert_eq!(
            progress.line(START_PROGRESS_INTERVAL * 2, mounting(6)),
            Some(String::from(
                "waited 10s for the gateway: mounting 6 adopted projects…"
            ))
        );
    }

    /// A poll that returns long after its interval reports the wait it actually observed, once,
    /// rather than one line for every interval that elapsed while it was blocked.
    #[test]
    fn a_late_poll_reports_the_observed_wait_once() {
        let mut progress = StartProgress::new();

        assert_eq!(
            progress.line(Duration::from_secs(90), mounting(2)),
            Some(String::from(
                "waited 90s for the gateway: mounting 2 adopted projects…"
            ))
        );
        assert_eq!(progress.line(Duration::from_secs(93), mounting(2)), None);
        assert_eq!(
            progress.line(Duration::from_secs(95), mounting(2)),
            Some(String::from(
                "waited 95s for the gateway: mounting 2 adopted projects…"
            ))
        );
    }

    /// Every line says what the daemon reported, in its own count: one project is not
    /// "1 projects", and a daemon that does not answer yet is not claimed to be mounting.
    #[test]
    fn the_start_wait_says_what_the_daemon_reported() {
        for (waiting, expected) in [
            (
                StartWait::ControlSocket,
                "waited 5s for the gateway: its control socket does not answer yet…",
            ),
            (
                mounting(1),
                "waited 5s for the gateway: mounting 1 adopted project…",
            ),
            (
                StartWait::Startup(StartupHeal::RestoringSessions),
                "waited 5s for the gateway: restoring workspace sessions…",
            ),
            (
                StartWait::Drain,
                "waited 5s for the gateway: the running gateway is draining before launchd restarts it…",
            ),
        ] {
            assert_eq!(
                StartProgress::new().line(START_PROGRESS_INTERVAL, waiting),
                Some(String::from(expected))
            );
        }
    }

    /// "heal" is cowshed's word for what it does to itself, not the user's word for what they are
    /// waiting on. A person watching `gateway start` is waiting for their workspaces to be
    /// mounted, and the line has to say that.
    #[test]
    fn the_start_wait_never_speaks_of_healing() {
        for waiting in [
            StartWait::ControlSocket,
            mounting(1),
            mounting(4),
            StartWait::Startup(StartupHeal::RestoringSessions),
            StartWait::Drain,
        ] {
            let line = StartProgress::new()
                .line(START_PROGRESS_INTERVAL, waiting)
                .expect("a line once the interval has passed");
            for jargon in ["heal", "unhealable", "reclaim", "provision", "incarnation"] {
                assert!(!line.contains(jargon), "{line} leaks {jargon}");
            }
        }
    }

    /// The deadline has to outlast a real heal: mounting several multi-gigabyte mains takes
    /// minutes, and a deadline shorter than that fails a start that was working.
    #[test]
    fn the_start_deadline_outlasts_a_multi_project_heal() {
        assert!(START_DEADLINE >= Duration::from_secs(120));
        assert!(START_DEADLINE > START_PROGRESS_INTERVAL * 4);
    }

    /// A retirement that cannot finish is a line and its hint for that project and nothing more:
    /// the outcomes after it are reported too, and a failure to read the records at all is lines
    /// as well, never an error the startup pass has to carry.
    #[test]
    fn startup_reports_every_resumed_retirement_and_names_each_refusal() {
        let repo = |name: &str| RepoId::parse(name).expect("repository");
        let report = |name: &str, images| ProjectRetirementReport {
            repo_id: repo(name),
            archive: PathBuf::from(format!("/store/.retired-projects/{name}")),
            images,
        };
        let resumed: Result<Vec<(RepoId, Result<ProjectRetirementReport>)>> = Ok(vec![
            (repo("acme/api"), Ok(report("acme/api", 1))),
            (
                repo("acme/web"),
                Err(CowshedError::conflict(
                    "workspace raven has not finished draining",
                    "cowshed forget acme/web --abandon",
                )),
            ),
            (repo("acme/worker"), Ok(report("acme/worker", 3))),
        ]);
        let lines = retirement_resume_lines(&resumed);
        assert_eq!(
            lines,
            [
                "cowshed: resumed at gateway startup: retired acme/api: 1 image detached; its whole project tree is preserved at /store/.retired-projects/acme/api",
                "cowshed: could not finish retiring acme/web at gateway startup; it stays fenced: workspace raven has not finished draining",
                "next: cowshed forget acme/web --abandon",
                "cowshed: resumed at gateway startup: retired acme/worker: 3 images detached; its whole project tree is preserved at /store/.retired-projects/acme/worker",
            ]
        );
        for line in &lines {
            assert!(!line.contains("delet"), "{line}");
        }

        let unreadable: Result<Vec<(RepoId, Result<ProjectRetirementReport>)>> = Err(
            CowshedError::integrity("a retirement record is damaged", "inspect the record"),
        );
        assert_eq!(
            retirement_resume_lines(&unreadable),
            [
                "cowshed: could not read the recorded project retirements at gateway startup: a retirement record is damaged",
                "next: inspect the record",
            ]
        );

        let hintless: Result<Vec<(RepoId, Result<ProjectRetirementReport>)>> = Ok(vec![(
            repo("acme/api"),
            Err(CowshedError::new(ErrorCode::Internal, "stopped", "")),
        )]);
        assert_eq!(
            retirement_resume_lines(&hintless),
            [
                "cowshed: could not finish retiring acme/api at gateway startup; it stays fenced: stopped"
            ]
        );

        let nothing: Result<Vec<(RepoId, Result<ProjectRetirementReport>)>> = Ok(Vec::new());
        assert!(retirement_resume_lines(&nothing).is_empty());
    }
}
