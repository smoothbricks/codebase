use crate::args::GatewayCommand;
use crate::build_record::{BuildRecord, unproven_refusal, unrecorded_refusal};
use crate::install_basis::{self, InstallSlot};
use crate::launchd::{
    BunDirectory, CARGO_LINK_NAME, COWSHED_BINARY_NAME, DevenvProfile, ExecutableDigest,
    ExecutableInstallState, ExistingPlist, GATEWAY_LABEL, HostStableExecutable, InstallOutcome,
    InstallState, InstalledExecutable, LaunchAgentSpec, LaunchAgentTarget, LaunchctlCommand,
    LaunchdExecutor, LaunchdFilesystem, LaunchdServiceStatus, NativeFilesystem,
    NativeLaunchctlCommand, PRIVATE_DIRECTORY_MODE, RemovalOutcome, StableLink, kickstart_hint,
    plan_cargo_link_removal, plan_executable_install, plan_executable_remove, plan_install,
    plan_remove, replace_symlink,
};
use crate::output::Output;
use crate::path_entry::PathEntryOutcome;
use async_trait::async_trait;
use cowshed_core::api::Coordinator;
use cowshed_core::api::{
    EmptyResult, GateJobRef, GatewayCpuBudget, GatewayStatus as CliGatewayStatus, PendingInstall,
    StaleDaemonBinary,
};
use cowshed_core::metadata::{NEW_PORT_BLOCK_SIZE, PortBlock};
use cowshed_core::project_retirement::ProjectRetirementReport;
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::supervisor_manager::{
    self, ProgramSpawner, SocketPeers, SupervisorManager,
};
use cowshed_core::runtime::supervisor_socket;
use cowshed_core::{
    CowshedError, ErrorCode, GatewayStoreBackend, NativeGatewayInventory, Result, StartupHealState,
    ValidatedHostStorage, validate_existing_host_storage,
};
use cowshed_gateway::{
    ArrowAuditConfig, ControlError, ControlFailureCode, DaemonStatus, Gateway, GatewayConfig,
    GatewayControlClient, GatewayError, GatewayHandle, MirrorCacheConfig, StartupHeal,
    StartupProbe, SupervisorRecovery, WorkspaceSession,
};
use sha2::{Digest as _, Sha256};
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
pub use cowshed_gateway_types::control_socket_path;
use std::collections::BTreeSet;

/// Bind the runtime's gateway dependency once, without making a control request during open.
pub fn runtime_control(storage: &ValidatedHostStorage) -> Result<Box<dyn GatewayControl>> {
    Ok(Box::new(ControlSocket::at(
        storage.store().join("gateway.sock"),
    )?))
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
            .map_err(control_error)
    }
}

#[async_trait]
impl GatewayControl for ControlSocket {
    async fn status(&self) -> std::result::Result<DaemonStatus, GatewayStatusError> {
        self.0.status().await.map_err(|error| match error {
            // A missing socket is not a fault: no daemon is running, and the remedy is to start
            // one rather than to report a broken control plane.
            ControlError::Io(source) if source.kind() == io::ErrorKind::NotFound => {
                GatewayStatusError::Absent
            }
            error => GatewayStatusError::Control(error.to_string()),
        })
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
        error => ControlRefusal::Failed(error.to_string()),
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
    let control = ControlSocket::at(storage.store().join("gateway.sock"))?;
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
            control_socket: storage.store().join("gateway.sock"),
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

/// How long to wait for `launchctl bootout` to actually finish.
///
/// launchd tears a service down asynchronously. Five seconds is generous for a process that has
/// already been sent its termination signal and short enough that a genuinely wedged agent is
/// reported rather than waited on.
const BOOTOUT_DEADLINE: std::time::Duration = std::time::Duration::from_secs(5);
const BOOTOUT_POLL_INTERVAL: std::time::Duration = std::time::Duration::from_millis(100);

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
    if plist == InstallOutcome::Changed {
        deactivate_launch_agent(executor, uid, target)?;
    }
    if !launch_agent_is_loaded(executor, uid, target)? {
        if let Err(error) =
            executor.execute_control(&crate::launchd::ControlPlan::bootstrap(uid, target))
            && !launch_agent_is_loaded(executor, uid, target)?
        {
            return Err(launchd_error(error));
        }
        return Ok(());
    }
    executor
        .execute_control(&crate::launchd::ControlPlan::kickstart(uid, target))
        .map_err(launchd_error)?;
    Ok(())
}

/// Boot the agent out and wait until launchd agrees it is gone.
///
/// The wait is the point. `launchctl bootout` returns before the service has finished tearing down,
/// so a caller that immediately re-reads the load state can still see it loaded and take the
/// "already loaded, kickstart it" branch — where `launchctl kickstart` answers 37, "operation
/// already in progress", because the bootout it raced is still running. Observed twice on this
/// host: a `setup` run failed on exactly that 37 and left the gateway agent unloaded, because the
/// bootout had in fact succeeded.
///
/// Bounded and then given up on: an agent that will not leave the loaded state within the deadline
/// is a launchd problem this function cannot fix, and returning the bootout's own error is more
/// use than blocking. `Ok` when launchd never had it loaded, which is the common case.
pub fn deactivate_launch_agent<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    uid: u32,
    target: &LaunchAgentTarget,
) -> Result<()>
where
    C: LaunchctlCommand,
{
    if !launch_agent_is_loaded(executor, uid, target)? {
        return Ok(());
    }
    let booted_out = executor.execute_control(&crate::launchd::ControlPlan::bootout(uid, target));
    let deadline = std::time::Instant::now() + BOOTOUT_DEADLINE;
    loop {
        if !launch_agent_is_loaded(executor, uid, target)? {
            return Ok(());
        }
        if std::time::Instant::now() >= deadline {
            // Still loaded after the deadline. If the bootout itself reported a failure, that is
            // the honest explanation; otherwise say what was observed rather than inventing a cause.
            return Err(match booted_out {
                Err(error) => launchd_error(error),
                Ok(_) => CowshedError::environment_missing(
                    format!(
                        "{} was booted out but launchd still reports it loaded after {}s",
                        target.label(),
                        BOOTOUT_DEADLINE.as_secs()
                    ),
                    kickstart_hint(uid, target.label()),
                ),
            });
        }
        std::thread::sleep(BOOTOUT_POLL_INTERVAL);
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
            let slot = take_install_slot(&canonical_home()?).await?;
            let status = start_service(output, &slot, Downgrade::Refuse).await?;
            emit_gateway_status(output, json, status)?;
        }
        GatewayCommand::Stop { purge } => {
            // A purge empties the install slot, so it waits its turn like any install does.
            let _slot = match purge {
                true => Some(take_install_slot(&canonical_home()?).await?),
                false => None,
            };
            let purged = stop_service(purge)?;
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
            let status = service_status().await?;
            emit_gateway_status(output, json, status)?;
        }
        GatewayCommand::Run => run_daemon().await?,
    }
    Ok(0)
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
    let bun = invoking_bun()?;
    install_nx_cache_server(&mut executor, &home, &candidate.source)?;
    let spec = LaunchAgentSpec::gateway(&executable, &bun).map_err(launchd_error)?;
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
    let uid = effective_uid();
    let client = GatewayControlClient::new(paths.control_socket.clone()).map_err(control_error)?;
    // Activation restarts a gateway that runs; one that answers first sees its gates through.
    if client.status().await.is_ok() {
        wait_for_gates(slot, &client, &mut io::stderr()).await?;
    }
    let written = executor.execute_install(&plan).map_err(launchd_error)?;
    install_and_activate_gateway(&mut executor, &home, &candidate, &spec, written)?;
    let cli_sha256 = candidate.digest.to_string();

    let mut progress = StartProgress::new();
    let mut started = tokio::time::Instant::now();
    let mut restarted = false;
    loop {
        let waiting = match client.status().await {
            Err(_) => StartWait::ControlSocket,
            Ok(status) => {
                let reported = cli_status(
                    true,
                    paths.control_socket.clone(),
                    Some(&status),
                    &cli_sha256,
                );
                match (
                    &reported.drain_cause,
                    &reported.stale_daemon,
                    reported.healing,
                ) {
                    (None, None, None) => {
                        return Ok(CliGatewayStatus {
                            cpu_budget: Some(read_cpu_budget(&client).await),
                            ..reported
                        });
                    }
                    // It answers, and tells how far its startup pass has got; nothing that
                    // needs a workspace is served until that pass is over.
                    (None, None, Some(heal)) => StartWait::Startup(heal),
                    // launchd kept the process it already had: the plist did not change, so
                    // activation did not restart it onto the bytes just installed. Restart it
                    // once.
                    (None, Some(_), _) if !restarted => {
                        wait_for_gates(slot, &client, &mut io::stderr()).await?;
                        // The start deadline is the daemon's to meet, not the gates'.
                        started = tokio::time::Instant::now();
                        activate_launch_agent(
                            &mut executor,
                            uid,
                            spec.target(),
                            InstallOutcome::Changed,
                        )?;
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
                kickstart_hint(uid, GATEWAY_LABEL),
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
pub fn gateway_launch_agent(home: &Path) -> Result<(HostStableExecutable, LaunchAgentTarget)> {
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let target = LaunchAgentTarget::new(home, GATEWAY_LABEL).map_err(launchd_error)?;
    Ok((executable, target))
}

/// The `bun` this invocation's `PATH` provides, for the gateway agent's `PATH`, named as a profile
/// nix or devenv keeps ([`BunDirectory`]). No `bun`, or one only a bare store path names, is a setup
/// error refused before anything is installed: cowshed bundles no interpreter (tg#7504), and keeps
/// no store path alive for one.
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
fn stop_service(purge: bool) -> Result<RemovalOutcome> {
    let home = canonical_home()?;
    let (executable, target) = gateway_launch_agent(&home)?;
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
/// answers first sees its gates through ([`wait_for_gates`]): stopping it cuts them off as a
/// restart does.
pub(crate) async fn stop_for_host_move(slot: &InstallSlot) -> Result<()> {
    let client = GatewayControlClient::new(control_socket_path()).map_err(control_error)?;
    if client.status().await.is_ok() {
        wait_for_gates(slot, &client, &mut io::stderr()).await?;
    }
    stop_service(false).map(|_| ())
}

/// Start the gateway again after setup moved its mirrors, saying nothing: setup reports the
/// move itself, and the gateway's own start output belongs to `cowshed gateway start`.
pub(crate) async fn start_after_host_move(slot: &InstallSlot, downgrade: Downgrade) -> Result<()> {
    let mut quiet = Output::new(io::sink(), io::sink(), true);
    start_service(&mut quiet, slot, downgrade).await.map(|_| ())
}

pub(crate) async fn service_status() -> Result<CliGatewayStatus> {
    let home = canonical_home()?;
    let socket = control_socket_path();
    let executable =
        HostStableExecutable::new(&home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let pending_install = InstallSlot::observe(executable.support_directory())?;
    let target = LaunchAgentTarget::new(&home, GATEWAY_LABEL).map_err(launchd_error)?;
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
        match client.status().await {
            Ok(status) => Some((status, read_cpu_budget(&client).await)),
            Err(_) => None,
        }
    } else {
        None
    };
    let (status, cpu_budget) = answered.unzip();
    let cli_sha256 = executable_digest(&running_executable()?)?;
    Ok(CliGatewayStatus {
        cpu_budget,
        pending_install,
        ..cli_status(installed, socket, status.as_ref(), cli_sha256.as_str())
    })
}

/// The host CPU ledger of a daemon that answered `status`, or why it did not answer
/// `cpu-budget`: a daemon from before that request refuses it, which is not an empty ledger.
async fn read_cpu_budget(client: &GatewayControlClient) -> GatewayCpuBudget {
    match client.cpu_budget().await {
        Ok(ledger) => GatewayCpuBudget::Ledger(ledger),
        Err(error) => GatewayCpuBudget::Unanswered(error.to_string()),
    }
}

/// How often an install that waits for gates reads the ledger again.
const GATE_POLL: Duration = Duration::from_secs(1);

/// Hold an install that is about to restart the running gateway until no gate-class work holds
/// or wants CPU tokens (05_gateway.md "An install waits for the gates in flight").
///
/// A restart closes every lease connection, and a gate's pool goes with it: every task that gate
/// admits afterwards is refused ("cpu-tokens parent pool refused: the gateway closed the lease
/// connection"), which killed a land mid-gate. The restart itself is unchanged ("Crash safety by
/// connection"); the installer only picks a moment with no gate in flight. There is no deadline,
/// as no admission has one (05 "No admission deadline"): the gates end, and the install goes on.
/// The gate jobs waited for are said once, and again whenever they change, and recorded in the
/// slot for `gateway status` until the wait ends. A gateway that does not answer `cpu-budget` is
/// not waited on, and that is said too.
async fn wait_for_gates(
    slot: &InstallSlot,
    client: &GatewayControlClient,
    stderr: &mut impl Write,
) -> Result<()> {
    let waited = watch_gates(slot, client, stderr).await;
    let cleared = slot.clear_wait();
    waited.and(cleared)
}

async fn watch_gates(
    slot: &InstallSlot,
    client: &GatewayControlClient,
    stderr: &mut impl Write,
) -> Result<()> {
    let mut named: Option<BTreeSet<GateJobRef>> = None;
    loop {
        let gates = match client.gates_in_flight().await {
            Ok(gates) => gates,
            Err(error) => {
                return writeln!(
                    stderr,
                    "cowshed: restarting the gateway without waiting for gates: it does not answer cpu-budget: {error}"
                )
                .map_err(output_error);
            }
        };
        if gates.is_empty() {
            if named.is_some() {
                writeln!(
                    stderr,
                    "cowshed: no gate job holds or wants CPU tokens any more; restarting the gateway"
                )
                .map_err(output_error)?;
            }
            return Ok(());
        }
        if named.as_ref() != Some(&gates) {
            writeln!(
                stderr,
                "cowshed: waiting to restart the gateway until no gate job holds or wants CPU tokens: {}",
                gate_jobs_named(&gates)
            )
            .map_err(output_error)?;
            slot.record_wait(&PendingInstall {
                waiting_for: gates.iter().cloned().collect(),
            })?;
            named = Some(gates);
        }
        tokio::time::sleep(GATE_POLL).await;
    }
}

/// `gate job <n> at <checkout>, …`: how the installer and `gateway status` name the gate jobs an
/// install waits for.
fn gate_jobs_named<'a>(gates: impl IntoIterator<Item = &'a GateJobRef>) -> String {
    gates
        .into_iter()
        .map(|gate| format!("gate job {} at {}", gate.job, gate.checkout))
        .collect::<Vec<_>>()
        .join(", ")
}

async fn run_daemon() -> Result<()> {
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
            eprintln!("cowshed: could not list adopted projects at gateway startup: {error}");
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
    let handle = OwnedGateway::new(gateway.handle());
    let started = async {
        serve_supervisors(&store_root, manager).await?;
        // Both host sockets are serving before storage recovery can wait on an owned process.
        // Only pre-existing positive authorizations resume; a refusal leaves that project
        // fenced and cannot prevent unrelated projects from healing.
        resume_project_retirements(&storage).await;
        let inventory = NativeSessionInventory::new(storage);
        heal_recorded_projects(&heal_inventory, repositories, &heal).await;
        heal_sccache_daemon().await;
        report_session_restore(&install_all_sessions(&inventory, &handle).await?);
        heal.restored();
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
            eprintln!(
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
        eprintln!("{line}");
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

/// Heal every one of `repositories`, mains before sessions, counting each one down in `heal` and
/// reporting rather than raising.
///
/// A project that cannot be healed is a finding for `cowshed doctor`; it must never stop the
/// gateway from serving the healthy ones (05_gateway.md). Mains are logged apart from sessions
/// because they are not equally load-bearing: an unmounted main is the user's own checkout missing
/// from their shell and editor, which is why `doctor` reports it as critical and this line carries
/// the remedy with it.
async fn heal_recorded_projects(
    inventory: &NativeGatewayInventory,
    repositories: Vec<RepoId>,
    heal: &StartupHealState,
) {
    for outcome in inventory.heal(repositories, heal).await {
        if let Err(error) = &outcome.main {
            eprintln!(
                "cowshed: {}: main checkout is not mounted after gateway startup: {error}",
                outcome.repo_id
            );
            eprintln!("next: cowshed doctor");
        }
        for session in &outcome.sessions {
            if let Err(error) = &session.mount {
                eprintln!(
                    "cowshed: could not mount {}/{} at gateway startup: {error}",
                    outcome.repo_id, session.workspace
                );
            }
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
        eprintln!(
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
        eprintln!(
            "cowshed: {}: gateway session {} at {} was not restored at startup: {}",
            failure.repo_id, failure.workspace_id, failure.endpoint, failure.refusal
        );
    }
    if !report.refused.is_empty() {
        eprintln!("next: cowshed doctor");
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
        // The ledger is a second request, asked only of a daemon that answered this one.
        cpu_budget: None,
        // The slot is observed beside the daemon, not asked of it.
        pending_install: None,
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
    if let Some(pending) = &status.pending_install {
        output
            .guidance(&pending_install_line(pending))
            .map_err(output_error)?;
    }
    for line in status.cpu_budget.iter().flat_map(cpu_budget_lines) {
        output.guidance(&line).map_err(output_error)?;
    }
    Ok(())
}

/// An install that holds the slot, in the operator's words: the restart it is about to make, and
/// the gate jobs it waits for before making it.
fn pending_install_line(pending: &PendingInstall) -> String {
    match pending.waiting_for.as_slice() {
        [] => {
            "gateway install pending: an install holds the install slot (dev.cowshed/install.lock)"
                .to_owned()
        }
        gates => format!(
            "gateway install pending: an install holds the install slot (dev.cowshed/install.lock) and waits to restart the gateway until no gate job holds or wants CPU tokens: {}",
            gate_jobs_named(gates)
        ),
    }
}

/// The host CPU ledger in the operator's words (05_gateway.md "Host CPU budget"), in the order the
/// ledger serves it: tokens held of the total; the leases held past the hold limit, oldest first,
/// so the first lines answer who holds the host and since when (05_gateway.md "The hold limit"),
/// each listed again under its own gate job or stake; the running gate jobs, the head — the
/// oldest — first; the waiting gate-class requests in the order returned tokens go to them; then
/// each checkout's background stake, served behind them. Each running gate job and background
/// stake is followed by its leases, oldest grant first, so the reader sees who holds the tokens
/// and since when rather than guessing from the checkout, and a background stake by its waiting
/// requests. A queue entry is named by its process and wait age as a lease is by its grant's: one
/// gate job's tasks are one entry each, and the job's number alone cannot tell them apart. Or why
/// the daemon gave no ledger.
fn cpu_budget_lines(budget: &GatewayCpuBudget) -> Vec<String> {
    let ledger = match budget {
        GatewayCpuBudget::Ledger(ledger) => ledger,
        GatewayCpuBudget::Unanswered(reason) => {
            return vec![format!(
                "gateway cpu budget unavailable: the daemon did not answer cpu-budget: {reason}"
            )];
        }
    };
    let mut lines = vec![format!(
        "gateway cpu budget: held {}/{} tokens, background at most {}",
        ledger.held,
        ledger.total,
        ledger.background_limit()
    )];
    let limit_minutes = cowshed_gateway_types::HOLD_LIMIT.as_secs() / 60;
    for (checkout, holder) in leases_past_the_hold_limit(ledger) {
        lines.push(format!(
            "gateway cpu hold past {limit_minutes}m: {checkout}: {}",
            lease_words(holder)
        ));
    }
    if ledger.gate_jobs.is_empty() {
        lines.push("gateway gate head: no gate job is running".to_owned());
    }
    for (place, job) in ledger.gate_jobs.iter().enumerate() {
        let title = if place == 0 {
            "gateway gate head"
        } else {
            "gateway gate job behind the head"
        };
        lines.push(format!(
            "{title}: {} gate job {}: held {}, running {}",
            job.checkout,
            job.job,
            job.held,
            job.holders.len()
        ));
        lines.extend(job.holders.iter().map(|holder| {
            format!(
                "gateway gate lease: {} gate job {}: {}",
                job.checkout,
                job.job,
                lease_words(holder)
            )
        }));
    }
    if ledger.gate_queue.is_empty() {
        lines.push("gateway gate queue: empty".to_owned());
    } else {
        lines.push(format!(
            "gateway gate queue: {} waiting, in the order returned tokens go to them",
            ledger.gate_queue.len()
        ));
        lines.extend(ledger.gate_queue.iter().enumerate().map(|(place, waiter)| {
            format!(
                "gateway gate queue {}: {} gate job {}: {}",
                place + 1,
                waiter.checkout,
                waiter.job,
                waiter_words(waiter.pid, waiter.want, waiter.waited_ms, &waiter.command)
            )
        }));
    }
    if ledger.background.is_empty() {
        lines.push("gateway background: none".to_owned());
    }
    for stake in &ledger.background {
        lines.push(format!(
            "gateway background: {}: held {}, running {}, waiting {}",
            stake.checkout,
            stake.held,
            stake.holders.len(),
            stake.waiters.len()
        ));
        lines.extend(stake.holders.iter().map(|holder| {
            format!(
                "gateway background lease: {}: {}",
                stake.checkout,
                lease_words(holder)
            )
        }));
        lines.extend(stake.waiters.iter().enumerate().map(|(place, waiter)| {
            format!(
                "gateway background queue {}: {}: {}",
                place + 1,
                stake.checkout,
                waiter_words(waiter.pid, waiter.want, waiter.waited_ms, &waiter.command)
            )
        }));
    }
    lines
}

/// Every lease held past the hold limit (05_gateway.md "The hold limit"), a gate job's or a
/// background stake's, with the checkout it holds for, the oldest grant first.
fn leases_past_the_hold_limit(
    ledger: &cowshed_gateway_types::CpuBudgetStatus,
) -> Vec<(&str, &cowshed_gateway_types::CpuHolder)> {
    let mut leases = Vec::new();
    for job in &ledger.gate_jobs {
        for holder in &job.holders {
            if holder.past_hold_limit() {
                leases.push((job.checkout.as_str(), holder));
            }
        }
    }
    for stake in &ledger.background {
        for holder in &stake.holders {
            if holder.past_hold_limit() {
                leases.push((stake.checkout.as_str(), holder));
            }
        }
    }
    leases.sort_by_key(|(_, holder)| std::cmp::Reverse(holder.held_ms));
    leases
}

/// `pid 71437, 15/18 tokens, granted 34m07s ago: cargo test …`: one lease as the operator reads
/// it. The age is the grant's; `ps -p <pid>` gives the process's.
fn lease_words(holder: &cowshed_gateway_types::CpuHolder) -> String {
    let pid = pid_words(holder.pid);
    let age = age_words(holder.held_ms);
    format!(
        "{pid}, {}/{} tokens, granted {age} ago: {}",
        holder.tokens, holder.want, holder.command
    )
}

/// `pid 71437, want 18, waiting 4m10s: cargo test …`: one waiting request as the operator reads
/// it. It holds nothing yet; the age is since it asked.
fn waiter_words(pid: Option<i32>, want: usize, waited_ms: u64, command: &str) -> String {
    let pid = pid_words(pid);
    let age = age_words(waited_ms);
    format!("{pid}, want {want}, waiting {age}: {command}")
}

/// `pid 71437`, or `pid unknown` when the client's socket named none.
fn pid_words(pid: Option<i32>) -> String {
    pid.map_or_else(|| "pid unknown".to_owned(), |pid| format!("pid {pid}"))
}

/// An age to the second, as the operator reads it: `3s`, `34m07s`, `1h02m05s`.
fn age_words(ms: u64) -> String {
    let seconds = ms / 1000;
    let (hours, minutes, seconds) = (seconds / 3600, seconds / 60 % 60, seconds % 60);
    if hours > 0 {
        format!("{hours}h{minutes:02}m{seconds:02}s")
    } else if minutes > 0 {
        format!("{minutes}m{seconds:02}s")
    } else {
        format!("{seconds}s")
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
/// target), the same on every platform, run by the gateway's `bun` (tg#7504).
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
/// shell's to provide (tg#6789).
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
{
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    remove_planted_cargo_link(executor, &executable)?;
    install_stored_executable(executor, &executable, &executable_digest(source)?, source)?;
    Ok(executable)
}

fn install_stored_executable<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    executable: &HostStableExecutable,
    digest: &ExecutableDigest,
    source: &Path,
) -> Result<()>
where
    F: LaunchdFilesystem,
{
    let state = observe_executable_install(executable, digest, source)?;
    executor
        .execute_install(&plan_executable_install(executable, digest, source, &state))
        .map_err(launchd_error)?;
    Ok(())
}

/// Remove the `cargo` link an earlier install planted beside the stable name. cowshed no longer
/// answers as `cargo` — a CPU grant reaches Cargo as the make jobserver configuration Cargo reads
/// itself (05_gateway.md, "Host CPU budget") — so that link would make `cargo` on a `PATH` holding
/// the directory run the cowshed CLI. Anything else named `cargo` there is not cowshed's: it is
/// refused by its path and what it is, and left as found.
fn remove_planted_cargo_link<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    executable: &HostStableExecutable,
) -> Result<()>
where
    F: LaunchdFilesystem,
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
        eprintln!(
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
        eprintln!(
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
        eprintln!(
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
/// (exit 37, "operation already in progress", is the one that happened), so it is the step whose
/// failure has to be recoverable.
fn install_and_activate_gateway<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    home: &Path,
    candidate: &Candidate,
    spec: &LaunchAgentSpec,
    plist: InstallOutcome,
) -> Result<HostStableExecutable>
where
    F: LaunchdFilesystem,
    C: LaunchctlCommand,
{
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    remove_planted_cargo_link(executor, &executable)?;
    let retained = retain_previous_executable(executor, &executable)?;
    install_stored_executable(executor, &executable, &candidate.digest, &candidate.source)?;
    record_installed_source(&executable, &candidate.source);
    record_installed_build(&executable, candidate);
    match activate_launch_agent(executor, effective_uid(), spec.target(), plist) {
        Ok(()) => {
            prune_superseded(
                executor,
                &executable,
                &candidate.digest,
                retained.as_deref(),
            );
            Ok(executable)
        }
        Err(error) => Err(match retained {
            Some(retained) => {
                let rollback = restore_previous_executable(&executable, &retained);
                record_installed_source(
                    &executable,
                    Path::new("restored after a failed activation"),
                );
                // Restoring the bytes is not enough: the failure that brought us here is a failed
                // activation, so the agent is very likely unloaded, and a host with the right
                // binary and no supervisor is still degraded. The plist is unchanged and names the
                // restored path, so `NoChange` bootstraps it if launchd no longer holds it.
                let reloaded = match activate_launch_agent(
                    executor,
                    effective_uid(),
                    spec.target(),
                    InstallOutcome::NoChange,
                ) {
                    Ok(()) => format!("{} is loaded again", spec.label()),
                    Err(reload) => format!(
                        "{} could NOT be reloaded ({})",
                        spec.label(),
                        reload.message
                    ),
                };
                CowshedError::new(
                    error.code,
                    format!("{}; {rollback}; {reloaded}", error.message),
                    error.hint,
                )
            }
            // Nothing to roll back to: this host had no installed binary before the run, so the
            // new copy is not a regression and deleting it would only hide the failed activation.
            None => error,
        }),
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
/// unguarded, replaced a host's supervised gateway with a debug build and then stranded it with
/// no loaded agent. The caller holds the install slot: `slot` ([`take_install_slot`]).
pub async fn refresh_gateway_binary(
    slot: &InstallSlot,
    home: &Path,
    downgrade: Downgrade,
) -> Result<Option<ServiceBinaryRefresh>> {
    let candidate = Candidate::of(running_executable()?, BuildRecord::running())?;
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let basis = check_basis(&executable, &candidate, downgrade, async || {
        adopted_mains(home).await
    })
    .await;
    let client = GatewayControlClient::new(control_socket_path()).ok();
    let daemon = match &client {
        Some(client) => client.status().await.ok(),
        None => None,
    };
    // Only a daemon that answers is one a restart would cut its gates off from.
    let gates = client
        .as_ref()
        .filter(|_| daemon.is_some())
        .map(|client| (slot, client));
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    refresh_gateway_from(
        home,
        IncomingBuild {
            candidate: &candidate,
            debug_build: cfg!(debug_assertions),
            basis,
        },
        daemon.as_ref(),
        gates,
        &mut executor,
        invoking_bun,
    )
    .await
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

/// [`refresh_gateway_binary`] with its inputs given. `gates` is the control client of a daemon
/// that answered: every refresh that gets past the no-op return activates the agent, which
/// restarts that daemon, so it first waits for the gates in flight ([`wait_for_gates`]). `bun` is
/// asked only when a gateway agent is installed: a host without one needs no interpreter.
async fn refresh_gateway_from<C: LaunchctlCommand>(
    home: &Path,
    incoming: IncomingBuild<'_>,
    daemon: Option<&DaemonStatus>,
    gates: Option<(&InstallSlot, &GatewayControlClient)>,
    executor: &mut LaunchdExecutor<NativeFilesystem, C>,
    bun: impl FnOnce() -> Result<BunDirectory>,
) -> Result<Option<ServiceBinaryRefresh>> {
    let IncomingBuild {
        candidate,
        debug_build,
        basis,
    } = incoming;
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let target = LaunchAgentTarget::new(home, GATEWAY_LABEL).map_err(launchd_error)?;
    let observed = inspect_install_state(&target)?;
    if observed.plist.is_none() {
        return Ok(None);
    }
    let spec = LaunchAgentSpec::gateway(&executable, &bun()?).map_err(launchd_error)?;
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
    // The packaged Nx cache server is part of this build's install. A current agent is still
    // repaired, so this runs before the no-op return and before any activation: a missing
    // companion fails here rather than after the host has changed.
    install_nx_cache_server(executor, home, &candidate.source)?;
    if !binary_is_stale && !daemon_is_stale && plan.is_noop() {
        return Ok(None);
    }
    if let Some((slot, client)) = gates {
        wait_for_gates(slot, client, &mut io::stderr()).await?;
    }
    let plist = executor.execute_install(&plan).map_err(launchd_error)?;
    if binary_is_stale {
        // Binary drift requires a restart even when the agent definition itself did not change.
        install_and_activate_gateway(executor, home, candidate, &spec, InstallOutcome::Changed)?;
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

    type RecordingExecutor = LaunchdExecutor<NativeFilesystem, RecordingGatewayLaunchctl>;

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
        let spec = LaunchAgentSpec::gateway(&executable, &test_bun(&home)).unwrap();
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

    #[derive(Default)]
    struct RecordingGatewayLaunchctl {
        argv: Vec<Vec<std::ffi::OsString>>,
    }

    impl LaunchctlCommand for RecordingGatewayLaunchctl {
        fn run(
            &mut self,
            executable: &Path,
            arguments: &[std::ffi::OsString],
        ) -> io::Result<crate::launchd::LaunchctlOutput> {
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
            )
            .await
            .unwrap();
            assert_eq!(
                refreshed,
                Some(ServiceBinaryRefresh::Refreshed {
                    service: GATEWAY_LABEL.to_owned(),
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
                    &home,
                    IncomingBuild {
                        candidate: &candidate,
                        debug_build: false,
                        basis: Ok(())
                    },
                    None,
                    None,
                    &mut executor,
                    || Ok(test_bun(&home))
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
                    service: GATEWAY_LABEL.to_owned(),
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
            assert_eq!(service, GATEWAY_LABEL);
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
                &home,
                IncomingBuild {
                    candidate: &candidate,
                    debug_build: false,
                    basis: Ok(())
                },
                None,
                None,
                &mut executor,
                || Ok(test_bun(&home))
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
            ) -> io::Result<crate::launchd::LaunchctlOutput> {
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
            &spec,
            InstallOutcome::Changed,
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

    /// A daemon from before `cpu-budget` refuses it as an unknown operation, exactly as a live
    /// daemon refuses any operation it does not know: `status` reports that refusal as the
    /// ledger it could not read, never as an idle host with an empty one.
    #[tokio::test]
    async fn a_daemon_that_refuses_cpu_budget_is_unanswered_not_an_empty_ledger() {
        use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};

        let dir = std::env::temp_dir().join(format!("cscpuview-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("scratch");
        let socket = dir.join("gateway.sock");
        let listener = tokio::net::UnixListener::bind(&socket).expect("bind");
        let older_daemon = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.expect("accept");
            let mut request = String::new();
            stream.read_to_string(&mut request).await.expect("request");
            stream
                .write_all(b"{\"ok\":false,\"code\":\"invalid-request\",\"error\":\"gateway control encoding failed: unknown gateway control operation\"}\n")
                .await
                .expect("refuse");
            request
        });
        let client = GatewayControlClient::new(socket).expect("absolute socket");
        let budget = read_cpu_budget(&client).await;
        assert_eq!(
            older_daemon.await.expect("daemon"),
            "{\"op\":\"cpu-budget\"}\n"
        );
        assert_eq!(
            budget,
            GatewayCpuBudget::Unanswered(
                "gateway control rejected operation (InvalidRequest): gateway control encoding failed: unknown gateway control operation"
                    .into()
            )
        );
        fs::remove_dir_all(&dir).ok();
    }

    /// The leases `gateway status` names right after the CPU ledger's count: those held past the
    /// hold limit (05_gateway.md "The hold limit").
    mod hold_limit {
        use cowshed_gateway_types::{
            CpuBackgroundStake, CpuBudgetStatus, CpuCheckoutStatus, CpuGateJob, CpuHolder,
        };

        use super::*;

        /// The `heldMs` of a lease whose line reads `granted {minutes}m{seconds:02}s ago`.
        fn granted_ago(minutes: u64, seconds: u64) -> u64 {
            (minutes * 60 + seconds) * 1_000
        }

        /// A lease held by `pid` for `command`, granted `held_ms` ago.
        fn lease(pid: i32, command: &str, tokens: usize, want: usize, held_ms: u64) -> CpuHolder {
            CpuHolder {
                pid: Some(pid),
                command: command.into(),
                tokens,
                want,
                held_ms,
            }
        }

        /// An 18-token host held whole by a gate job's lease of two minutes and a background
        /// `cargo test` lease of 12 tokens granted `background_held_ms` ago.
        fn ledger_with_background_lease(background_held_ms: u64) -> GatewayCpuBudget {
            let gate_lease = lease(31102, "nx run-many -t test", 6, 18, granted_ago(2, 0));
            let background_lease = lease(
                29272,
                "cargo --frozen test -p app_runtime",
                12,
                18,
                background_held_ms,
            );
            GatewayCpuBudget::Ledger(CpuBudgetStatus {
                total: 18,
                held: 18,
                checkouts: vec![
                    CpuCheckoutStatus {
                        checkout: "/mnt/app/land".into(),
                        held: 6,
                        running: 1,
                        waiting: 0,
                    },
                    CpuCheckoutStatus {
                        checkout: "/mnt/app/account-placement".into(),
                        held: 12,
                        running: 1,
                        waiting: 0,
                    },
                ],
                gate_jobs: vec![CpuGateJob {
                    checkout: "/mnt/app/land".into(),
                    job: 7,
                    held: 6,
                    holders: vec![gate_lease],
                }],
                gate_queue: Vec::new(),
                background: vec![CpuBackgroundStake {
                    checkout: "/mnt/app/account-placement".into(),
                    held: 12,
                    holders: vec![background_lease],
                    waiters: Vec::new(),
                }],
            })
        }

        /// The lines that name a lease past the limit.
        fn hold_lines(lines: &[String]) -> Vec<&str> {
            lines
                .iter()
                .map(String::as_str)
                .filter(|line| line.starts_with("gateway cpu hold past"))
                .collect()
        }

        /// A lease held past the hold limit is named right after the count, so the first lines of
        /// `gateway status` answer who holds the host and since when. Measured 2026-10-19: a
        /// runaway `cargo test` held 12 of 18 tokens for 48 minutes while the status gave only the
        /// count. The gate job's lease of two minutes is within the limit and is not named.
        #[test]
        fn a_lease_held_past_the_limit_is_named_right_after_the_count() {
            let ledger = ledger_with_background_lease(granted_ago(48, 18));
            let lines = cpu_budget_lines(&ledger);

            assert_eq!(
                lines[0],
                "gateway cpu budget: held 18/18 tokens, background at most 9"
            );
            assert_eq!(
                lines[1],
                "gateway cpu hold past 30m: /mnt/app/account-placement: pid 29272, 12/18 tokens, granted 48m18s ago: cargo --frozen test -p app_runtime"
            );
            assert_eq!(hold_lines(&lines).len(), 1, "{lines:#?}");
        }

        /// A lease a second inside the limit is not named: with every lease within it the count is
        /// followed by the gate head, as it was before the limit existed.
        #[test]
        fn a_ledger_with_every_lease_within_the_limit_names_none() {
            let ledger = ledger_with_background_lease(granted_ago(29, 59));
            let lines = cpu_budget_lines(&ledger);

            assert!(hold_lines(&lines).is_empty(), "{lines:#?}");
            assert_eq!(
                lines[1],
                "gateway gate head: /mnt/app/land gate job 7: held 6, running 1"
            );
        }

        /// A gate job's lease and a background stake's, both past the limit, are named together,
        /// the oldest first and each under the checkout it holds for. The background leases are
        /// older and younger than the gate's, so neither source's own order is the answer; a lease
        /// within the limit stays out.
        #[test]
        fn leases_past_the_limit_are_named_oldest_first_whether_gate_or_background() {
            let ledger = GatewayCpuBudget::Ledger(CpuBudgetStatus {
                total: 18,
                held: 18,
                checkouts: vec![
                    CpuCheckoutStatus {
                        checkout: "/mnt/app/land".into(),
                        held: 5,
                        running: 2,
                        waiting: 0,
                    },
                    CpuCheckoutStatus {
                        checkout: "/mnt/app/account-placement".into(),
                        held: 13,
                        running: 2,
                        waiting: 0,
                    },
                ],
                gate_jobs: vec![CpuGateJob {
                    checkout: "/mnt/app/land".into(),
                    job: 4,
                    held: 5,
                    holders: vec![
                        lease(31102, "nx run-many", 4, 18, granted_ago(41, 0)),
                        lease(31105, "cargo check", 1, 1, granted_ago(2, 0)),
                    ],
                }],
                gate_queue: Vec::new(),
                background: vec![CpuBackgroundStake {
                    checkout: "/mnt/app/account-placement".into(),
                    held: 13,
                    holders: vec![
                        lease(29272, "cargo test", 12, 18, granted_ago(48, 18)),
                        lease(29301, "bun test", 1, 1, granted_ago(31, 5)),
                    ],
                    waiters: Vec::new(),
                }],
            });
            let lines = cpu_budget_lines(&ledger);

            assert_eq!(
                lines[..4],
                [
                    "gateway cpu budget: held 18/18 tokens, background at most 9",
                    "gateway cpu hold past 30m: /mnt/app/account-placement: pid 29272, 12/18 tokens, granted 48m18s ago: cargo test",
                    "gateway cpu hold past 30m: /mnt/app/land: pid 31102, 4/18 tokens, granted 41m00s ago: nx run-many",
                    "gateway cpu hold past 30m: /mnt/app/account-placement: pid 29301, 1/1 tokens, granted 31m05s ago: bun test",
                ]
            );
            assert_eq!(hold_lines(&lines).len(), 3, "{lines:#?}");
        }
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
            async fn all_sessions(&self) -> Result<Vec<WorkspaceSession>> {
                Err(CowshedError::internal("the inventory was read"))
            }

            async fn project_sessions(&self, _: &RepoId) -> Result<Vec<WorkspaceSession>> {
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
        /// as `run_daemon` composes them, under `/tmp/<name>-<pid>` with `sockets` under `run`.
        struct Daemon {
            root: PathBuf,
            socket: PathBuf,
            gateway: Gateway,
            recovery: Recovery,
        }

        async fn daemon(name: &str, sockets: &[&str], heal: Arc<StartupHealState>) -> Daemon {
            // `/tmp`, not TMPDIR: the gateway socket's path has to fit in `sun_path`.
            let root = PathBuf::from(format!("/tmp/{name}-{}", std::process::id()));
            private_directory(&root);
            let store = root.join("store");
            fs::create_dir_all(store.join("run")).expect("run directory");
            for socket in sockets {
                fs::write(store.join("run").join(socket), b"").expect("a socket file");
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
                root,
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
                "csrecover",
                &["first.sock", "second.sock"],
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
            fs::remove_dir_all(&daemon.root).expect("cleanup");
        }

        /// The control socket answers from the first moment, while the startup pass still mounts
        /// projects: status says how many are left, `gateway status` says the gateway is still
        /// starting, and every session change — over the socket or through a command's reconcile
        /// — is refused by type until the pass has restored the sessions; then it is served.
        #[tokio::test]
        async fn the_control_socket_answers_while_the_startup_pass_mounts() {
            let heal = Arc::new(StartupHealState::mounting(2));
            let daemon = daemon("csheal", &[], Arc::clone(&heal)).await;
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
            fs::remove_dir_all(&daemon.root).expect("cleanup");
        }

        /// A land's gate job holding its orchestrator's token on `socket`, as the supervisor's
        /// gate admission holds it: granted, and held until the returned stream is dropped.
        async fn hold_gate(socket: &Path, checkout: &str) -> tokio::net::UnixStream {
            use tokio::io::{AsyncBufReadExt as _, AsyncWriteExt as _};

            let mut stream = tokio::net::UnixStream::connect(socket)
                .await
                .expect("connect");
            let request = serde_json::json!({ "op": "jobserver", "want": 1,
                "checkout": checkout, "command": "land gate", "class": "gate" });
            stream
                .write_all(format!("{request}\n").as_bytes())
                .await
                .expect("ask for the gate's token");
            {
                let mut lines = tokio::io::BufReader::new(&mut stream).lines();
                for lease in ["queued", "granted"] {
                    let line = lines.next_line().await.expect("read").expect("an answer");
                    assert!(line.contains(&format!("\"{lease}\"")), "{line}");
                }
            }
            stream
        }

        /// The measured failure: an install restarted the gateway mid-gate, and every task that
        /// land admitted afterwards was refused. An install now waits until no gate job holds
        /// or wants a token, names the gate jobs it waits for once and again when they change,
        /// and goes on once none is left; with none in flight it goes on at once, silently.
        /// Meanwhile `gateway status` sees the install hold the slot, and the gates it waits for.
        #[tokio::test]
        async fn an_install_waits_until_no_gate_job_holds_or_wants_cpu_tokens() {
            let heal = Arc::new(StartupHealState::mounting(0));
            heal.restored();
            let daemon = daemon("csgatewait", &[], heal).await;
            let client = GatewayControlClient::new(daemon.socket.clone()).expect("client");
            let support = daemon.root.join("dev.cowshed");
            fs::create_dir(&support).expect("support directory");
            assert_eq!(InstallSlot::observe(&support).expect("observe"), None);
            let slot = Arc::new(InstallSlot::take(&support).await.expect("the slot"));
            let observed = || InstallSlot::observe(&support).expect("observe");
            let pending = |gates: &[(u64, &str)]| {
                Some(PendingInstall {
                    waiting_for: gates
                        .iter()
                        .map(|(job, checkout)| GateJobRef {
                            job: *job,
                            checkout: (*checkout).to_owned(),
                        })
                        .collect(),
                })
            };
            assert_eq!(observed(), pending(&[]), "held, and waiting for nothing");

            let mut said = Vec::new();
            wait_for_gates(&slot, &client, &mut said)
                .await
                .expect("no gate in flight");
            assert!(said.is_empty(), "{}", String::from_utf8_lossy(&said));

            let older = hold_gate(&daemon.socket, "/w/older-land").await;
            let waiting = tokio::spawn({
                let (slot, client) = (Arc::clone(&slot), client.clone());
                async move {
                    let mut said = Vec::new();
                    wait_for_gates(&slot, &client, &mut said)
                        .await
                        .map(|()| said)
                }
            });
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert!(!waiting.is_finished(), "the install waits for the gate");
            let older_job = observed().expect("held").waiting_for[0].job;
            assert_eq!(observed(), pending(&[(older_job, "/w/older-land")]));
            let younger = hold_gate(&daemon.socket, "/w/younger-land").await;
            tokio::time::sleep(GATE_POLL * 2).await;
            assert!(!waiting.is_finished(), "the install waits for both gates");
            let both = observed().expect("held").waiting_for;
            assert_eq!(both.len(), 2, "{both:?}");
            assert_eq!(both[1].checkout, "/w/younger-land");
            drop(older);
            tokio::time::sleep(GATE_POLL * 2).await;
            assert!(!waiting.is_finished(), "the younger gate still runs");
            assert_eq!(observed(), pending(&[(both[1].job, "/w/younger-land")]));
            drop(younger);
            let said = waiting.await.expect("the wait task").expect("the wait");
            let said = String::from_utf8(said).expect("utf-8");
            let lines: Vec<&str> = said.lines().collect();
            assert_eq!(lines.len(), 4, "{said}");
            let waiting_for = |line: &str, checkouts: &[&str]| {
                line.starts_with(
                    "cowshed: waiting to restart the gateway until no gate job holds or wants CPU tokens: ",
                ) && checkouts
                    .iter()
                    .all(|checkout| line.contains(&format!(" at {checkout}")))
                    && line.matches(" at /w/").count() == checkouts.len()
            };
            assert!(waiting_for(lines[0], &["/w/older-land"]), "{said}");
            assert!(
                waiting_for(lines[1], &["/w/older-land", "/w/younger-land"]),
                "{said}"
            );
            assert!(waiting_for(lines[2], &["/w/younger-land"]), "{said}");
            assert_eq!(
                lines[3],
                "cowshed: no gate job holds or wants CPU tokens any more; restarting the gateway"
            );
            assert_eq!(
                observed(),
                pending(&[]),
                "the wait is over; the install still holds the slot"
            );
            drop(slot);
            assert_eq!(observed(), None, "the slot is free");

            daemon.gateway.drain().await.expect("drain the gateway");
            fs::remove_dir_all(&daemon.root).expect("cleanup");
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
            kickstart_hint(501, GATEWAY_LABEL),
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
