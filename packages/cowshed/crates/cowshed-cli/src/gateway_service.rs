use crate::args::GatewayCommand;
use crate::launchd::{
    COWSHED_BINARY_NAME, ExecutableInstallState, ExistingPlist, GATEWAY_LABEL,
    HostStableExecutable, InstallOutcome, InstallState, InstalledExecutable, LaunchAgentSpec,
    LaunchAgentTarget, LaunchctlCommand, LaunchdExecutor, LaunchdFilesystem, LaunchdServiceStatus,
    NativeFilesystem, NativeLaunchctlCommand, PRIVATE_DIRECTORY_MODE, RemovalOutcome,
    kickstart_hint, plan_executable_install, plan_executable_remove, plan_install, plan_remove,
};
use crate::output::Output;
use async_trait::async_trait;
use cowshed_core::api::Coordinator;
use cowshed_core::api::{EmptyResult, GatewayStatus as CliGatewayStatus, StaleDaemonBinary};
use cowshed_core::metadata::{NEW_PORT_BLOCK_SIZE, PortBlock};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::supervisor_manager::{
    self, ProgramSpawner, SocketPeers, SupervisorManager,
};
use cowshed_core::runtime::supervisor_socket;
use cowshed_core::{
    CowshedError, ErrorCode, NativeGatewayInventory, Result, StartupHealState,
    ValidatedHostStorage, validate_existing_host_storage,
};
use cowshed_gateway::{
    ArrowAuditConfig, ControlError, ControlFailureCode, Gateway, GatewayConfig,
    GatewayControlClient, GatewayHandle, GatewayStatus, MirrorCacheConfig, StartupHeal,
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
    GatewayStatusError, NativeSessionInventory, ReconcileReport, SessionInventory, canonical_home,
    control_error, control_socket_path, effective_uid, gateway_absent, install_all_sessions,
    policy_from_grants, project_session_prefix, reconcile_against_status, reconcile_project,
    reconcile_project_with_reallocator, session_from_fact, sessions_from_facts,
    stable_workspace_id,
};
use std::collections::BTreeSet;

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
}

#[async_trait]
impl GatewayControl for ControlSocket {
    async fn status(&self) -> std::result::Result<GatewayStatus, GatewayStatusError> {
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
    async fn install_session(&self, session: WorkspaceSession) -> Result<()> {
        self.0.install(session).await.map_err(|error| {
            CowshedError::internal(format!("could not restore gateway session: {error}"))
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
}

#[async_trait]
impl GatewayPortReallocator for NativePortReallocator<'_> {
    async fn reallocate(
        &self,
        session: &WorkspaceSession,
        rejected: &BTreeSet<u16>,
    ) -> Result<WorkspaceSession> {
        let inventory = NativeGatewayInventory::new(self.storage.clone());
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
        let candidates =
            PortBlock::macos_candidates_with_size(fact.port_block.size()).map_err(|error| {
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
                    return NativeSessionInventory::new(self.storage.clone())
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
    let inventory = NativeSessionInventory::new(storage.clone());
    let control = ControlSocket::at(storage.store().join("gateway.sock"))?;
    let reallocator = NativePortReallocator {
        storage,
        repo_id,
        assigner,
    };
    cowshed_core::timing::spanned(
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
            let status = start_service(output).await?;
            emit_gateway_status(output, json, status)?;
        }
        GatewayCommand::Stop { purge } => {
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

async fn start_service<W, E>(output: &mut Output<W, E>) -> Result<CliGatewayStatus>
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
    let source = supervisable_running_executable()?;
    let candidate = HostStableExecutable::new(&home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let spec = LaunchAgentSpec::gateway(&candidate).map_err(launchd_error)?;
    let observed = inspect_install_state(&spec)?;
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
    let written = executor.execute_install(&plan).map_err(launchd_error)?;
    install_and_activate_gateway(&mut executor, &home, &source, &spec, written)?;
    let cli_sha256 = executable_sha256(&source)?;

    let client = GatewayControlClient::new(paths.control_socket.clone()).map_err(control_error)?;
    let mut progress = StartProgress::new();
    let started = tokio::time::Instant::now();
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
                    (None, None, None) => return Ok(reported),
                    // It answers, and tells how far its startup pass has got; nothing that
                    // needs a workspace is served until that pass is over.
                    (None, None, Some(heal)) => StartWait::Startup(heal),
                    // launchd kept the process it already had: the plist did not change, so
                    // activation did not restart it onto the bytes just installed. Restart it
                    // once.
                    (None, Some(_), _) if !restarted => {
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

/// Delete the host-stable copy a LaunchAgent ran. Only ever called once its agent is gone: the
/// gateway agent is `KeepAlive`, so removing the binary under a loaded agent would leave launchd
/// respawning a path that no longer resolves.
pub fn remove_host_stable_executable(executable: &HostStableExecutable) -> Result<RemovalOutcome> {
    let installed = fs::symlink_metadata(executable.path()).is_ok();
    LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand)
        .execute_install(&plan_executable_remove(executable, installed))
        .map_err(launchd_error)?;
    Ok(if installed {
        RemovalOutcome::Removed
    } else {
        RemovalOutcome::AlreadyAbsent
    })
}

/// The gateway agent's own spec, resolved from the canonical home rather than the running binary.
///
/// Deterministic on purpose: stop and uninstall have to reach the agent `start` installed however
/// this process was invoked.
pub fn gateway_launch_agent(home: &Path) -> Result<(HostStableExecutable, LaunchAgentSpec)> {
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let spec = LaunchAgentSpec::gateway(&executable).map_err(launchd_error)?;
    Ok((executable, spec))
}

/// Stop the gateway; with `purge`, also delete the installed binary it ran.
///
/// Without `purge` the copy stays: it is host state rather than agent state, and leaving it makes
/// the next `start` a plist write instead of a fresh multi-megabyte copy.
fn stop_service(purge: bool) -> Result<RemovalOutcome> {
    let home = canonical_home()?;
    let (executable, spec) = gateway_launch_agent(&home)?;
    remove_launch_agent(spec.target())?;
    if purge {
        return remove_host_stable_executable(&executable);
    }
    Ok(RemovalOutcome::AlreadyAbsent)
}

/// Stop the gateway while setup moves its mirrors; the installed binary stays.
pub(crate) fn stop_for_host_move() -> Result<()> {
    stop_service(false).map(|_| ())
}

/// Start the gateway again after setup moved its mirrors, saying nothing: setup reports the
/// move itself, and the gateway's own start output belongs to `cowshed gateway start`.
pub(crate) async fn start_after_host_move() -> Result<()> {
    let mut quiet = Output::new(io::sink(), io::sink(), true);
    start_service(&mut quiet).await.map(|_| ())
}

pub(crate) async fn service_status() -> Result<CliGatewayStatus> {
    let home = canonical_home()?;
    let socket = control_socket_path();
    let executable =
        HostStableExecutable::new(&home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let spec = LaunchAgentSpec::gateway(&executable).map_err(launchd_error)?;
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    let installed = matches!(
        executor
            .execute_status(&crate::launchd::ControlPlan::print(
                effective_uid(),
                spec.target()
            ))
            .map_err(launchd_error)?,
        LaunchdServiceStatus::Loaded { .. }
    );
    let status = if installed {
        let client = GatewayControlClient::new(socket.clone()).map_err(control_error)?;
        client.status().await.ok()
    } else {
        None
    };
    let cli_sha256 = executable_sha256(&running_executable()?)?;
    Ok(cli_status(installed, socket, status.as_ref(), &cli_sha256))
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
        executable_sha256: Some(executable_sha256(&executable)?),
        startup: Some(Arc::new(DaemonStartup {
            heal: Arc::clone(&heal),
            manager: Arc::clone(&manager),
        })),
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
    let inventory = NativeSessionInventory::new(storage);
    let started = async {
        serve_supervisors(&store_root, manager).await?;
        heal_recorded_projects(&heal_inventory, repositories, &heal).await;
        heal_sccache_daemon().await;
        install_all_sessions(&inventory, &handle).await?;
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
    status: Option<&GatewayStatus>,
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
    }
}

/// The remedy for a daemon running other bytes than the CLI: a plain `stop` keeps the installed
/// copy and `start` does not restart a process whose plist did not change.
const STALE_DAEMON_REMEDY: &str = "cowshed gateway stop --purge && cowshed gateway start";

/// SHA-256 of an executable's contents, lowercase hex.
pub(crate) fn executable_sha256(path: &Path) -> Result<String> {
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
    Ok(hasher
        .finalize()
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect())
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
    Ok(())
}

/// Install `source` at the host-stable path launchd will run, and answer with that path.
///
/// The plist names a copy on the volume that carries the plist itself, so the agent
/// starts after the build that installed it is gone. The source may live in a workspace
/// or the nix store: those paths are unreadable at boot, but the copy is not.
pub fn install_host_stable_executable<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    home: &Path,
    name: &str,
    source: &Path,
) -> Result<HostStableExecutable>
where
    F: LaunchdFilesystem,
{
    let executable = HostStableExecutable::new(home, name).map_err(launchd_error)?;
    if source == executable.path() {
        // Already the installed copy: this is the steady state on a host launchd started, and
        // copying a file onto itself is the one publication the plan cannot express.
        return Ok(executable);
    }
    let state = observe_executable_install(&executable, source)?;
    executor
        .execute_install(&plan_executable_install(&executable, source, state))
        .map_err(launchd_error)?;
    Ok(executable)
}

/// Where the provenance of the installed cowshed is written down.
///
/// Beside the binary, on the volume that also carries the plists: whatever launchd can read the
/// agent definition from, an operator can read this from too. It exists because a supervised
/// binary with no recorded origin is unanswerable — "which build is this host running" had, until
/// this file, no answer other than a size comparison against a checkout that may have moved on.
const INSTALLED_SOURCE_RECORD: &str = "cowshed-source";

/// The hard link retained across one activation, so a failed `launchctl` can be undone.
const RETAINED_SUFFIX: &str = ".previous";

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

/// Retain the binary an install is about to replace, as a hard link beside it.
///
/// A hard link rather than a copy: it is O(1) whatever the binary's size, and it keeps the old
/// inode alive through the atomic rename that replaces the path — so the retained name still reads
/// the exact bytes the host was running. `None` means there was nothing installed to retain, which
/// is a first install and has nothing to roll back to.
pub fn retain_previous_executable(executable: &HostStableExecutable) -> Result<Option<PathBuf>> {
    let retained = retained_path(executable);
    if inspect_existing(executable.path())?.is_none() {
        return Ok(None);
    }
    // A leftover from an earlier interrupted run is stale by definition: the live binary is the
    // authority, and linking onto an existing name fails.
    match fs::remove_file(&retained) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => {}
        Err(error) => {
            return Err(CowshedError::internal(format!(
                "could not remove stale retained executable {}: {error}",
                retained.display()
            )));
        }
    }
    fs::hard_link(executable.path(), &retained).map_err(|error| {
        CowshedError::internal(format!(
            "could not retain {} as {}: {error}",
            executable.path().display(),
            retained.display()
        ))
    })?;
    Ok(Some(retained))
}

fn retained_path(executable: &HostStableExecutable) -> PathBuf {
    let mut name = executable.name().to_owned();
    name.push_str(RETAINED_SUFFIX);
    executable.directory().join(name)
}

/// Put the retained binary back, and say whether the host was left as it was found.
///
/// Reported in the returned sentence rather than raised: the caller already has the failure that
/// prompted the rollback, and a rollback that itself failed must not replace that failure with a
/// second one — it must be appended to it, because the two together are the host's actual state.
pub fn restore_previous_executable(executable: &HostStableExecutable, retained: &Path) -> String {
    match fs::rename(retained, executable.path()) {
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

/// Install the running build, activate its agent, and undo the install if activation fails.
///
/// The whole point is the last clause. `launchctl` failing after the copy is what left this host
/// running a debug gateway with no agent: the binary had already been replaced, and nothing put it
/// back. Activation is the step that can fail for reasons that have nothing to do with the bytes
/// (exit 37, "operation already in progress", is the one that happened), so it is the step whose
/// failure has to be recoverable.
fn install_and_activate_gateway<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    home: &Path,
    source: &Path,
    spec: &LaunchAgentSpec,
    plist: InstallOutcome,
) -> Result<HostStableExecutable>
where
    F: LaunchdFilesystem,
    C: LaunchctlCommand,
{
    let candidate = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let retained = retain_previous_executable(&candidate)?;
    let executable = install_host_stable_executable(executor, home, COWSHED_BINARY_NAME, source)?;
    record_installed_source(&executable, source);
    match activate_launch_agent(executor, effective_uid(), spec.target(), plist) {
        Ok(()) => {
            // The retained link is only good for the length of one activation: keeping it would
            // leave a second multi-megabyte copy nobody reclaims, and a stale one at that.
            if let Some(retained) = retained {
                let _ = fs::remove_file(retained);
            }
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
}

/// Whether the observed installed binary needs refreshing from the invoking build.
pub fn installed_binary_is_stale(state: &ExecutableInstallState) -> bool {
    !state.is_current()
}

/// Reconcile the gateway's stable binary and generated agent definition with this command.
///
/// Setup never refuses to repair a host. `None` means no gateway agent is installed, or both
/// installed artifacts already match. Plist drift is repaired even when the invoking build is
/// byte-identical to the installed copy, including when setup runs from that copy itself.
/// Binary drift uses the same atomic install and activation rollback as `gateway start`.
///
/// The build has to be fit to supervise *before* anything is copied, and a failed activation puts
/// the old binary back: this is the function that, unguarded, replaced a host's supervised gateway
/// with a debug build and then stranded it with no loaded agent.
pub fn refresh_gateway_binary(home: &Path) -> Result<Option<ServiceBinaryRefresh>> {
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    refresh_gateway_from(
        home,
        running_executable()?,
        cfg!(debug_assertions),
        &mut executor,
    )
}

fn refresh_gateway_from<C: LaunchctlCommand>(
    home: &Path,
    source: PathBuf,
    debug_build: bool,
    executor: &mut LaunchdExecutor<NativeFilesystem, C>,
) -> Result<Option<ServiceBinaryRefresh>> {
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let spec = LaunchAgentSpec::gateway(&executable).map_err(launchd_error)?;
    let observed = inspect_install_state(&spec)?;
    if observed.plist.is_none() {
        return Ok(None);
    }
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
    let state = observe_executable_install(&executable, &source)?;
    let binary_is_stale = installed_binary_is_stale(&state);
    if !binary_is_stale && plan.is_noop() {
        return Ok(None);
    }
    // Refuse only a binary replacement: reconciling an agent definition does not install this
    // invocation's bytes. Preserve the existing supervised binary when it already matches.
    if binary_is_stale
        && let Err(refusal) = refuse_unsupervisable_build(source.clone(), debug_build)
    {
        return Ok(Some(ServiceBinaryRefresh::Refused {
            service: spec.label().to_owned(),
            reason: refusal.message,
            remedy: refusal.hint,
        }));
    }
    let plist = executor.execute_install(&plan).map_err(launchd_error)?;
    if binary_is_stale {
        // Binary drift requires a restart even when the agent definition itself did not change.
        install_and_activate_gateway(executor, home, &source, &spec, InstallOutcome::Changed)?;
    } else {
        activate_launch_agent(executor, effective_uid(), spec.target(), plist)?;
    }
    Ok(Some(ServiceBinaryRefresh::Refreshed {
        service: spec.label().to_owned(),
    }))
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

/// What the host has at the stable path, and whether it is already this source.
fn observe_executable_install(
    executable: &HostStableExecutable,
    source: &Path,
) -> Result<ExecutableInstallState> {
    let installed = match inspect_existing(executable.path())? {
        Some(metadata) if is_user_owned(&metadata, false) => Some(InstalledExecutable {
            mode: metadata.permissions().mode() & 0o777,
            matches_source: source == executable.path()
                || same_contents(source, executable.path(), metadata.len())?,
        }),
        Some(_) => {
            return Err(CowshedError::integrity(
                format!(
                    "the installed {} binary is not a user-owned regular file: {}",
                    executable.name(),
                    executable.path().display()
                ),
                "remove it and rerun the service start command",
            ));
        }
        None => None,
    };
    Ok(ExecutableInstallState {
        support_directory_mode: private_directory_mode(executable.support_directory())?,
        binary_directory_mode: private_directory_mode(executable.directory())?,
        installed,
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

pub(crate) fn inspect_install_state(spec: &LaunchAgentSpec) -> Result<ObservedInstallState> {
    let directory_mode = private_directory_mode(spec.launch_agents_directory())?;
    match inspect_existing(spec.plist_path())? {
        Some(metadata) if is_user_owned(&metadata, false) => {
            let bytes = fs::read(spec.plist_path()).map_err(|error| {
                CowshedError::internal(format!(
                    "could not read {}: {error}",
                    spec.plist_path().display()
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
                spec.label(),
                spec.plist_path().display()
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

    /// The Aug drift in one test: a binary installed days earlier, byte-different from the build
    /// running setup, observed as exactly that — stale — while identical bytes are current. This
    /// is the decision `refresh_gateway_binary` acts on; the observation is pure filesystem, so
    /// it is provable without launchd.
    #[test]
    fn planted_binary_drift_is_observed_as_stale_and_identical_bytes_as_current() {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        let home =
            std::env::temp_dir().join(format!("cowshed-drift-{}-{nonce}", std::process::id()));
        let executable = HostStableExecutable::new(&home, COWSHED_BINARY_NAME).expect("executable");
        fs::create_dir_all(executable.directory()).expect("bin directory");
        let source = home.join("fresh-build");
        fs::write(&source, b"the build running setup").expect("source");

        // Nothing installed at all is stale: there is no current copy to be running.
        let state = observe_executable_install(&executable, &source).expect("observe absent");
        assert!(installed_binary_is_stale(&state));

        // Drifted bytes at the stable path are stale.
        fs::write(executable.path(), b"the binary from days ago").expect("plant drift");
        fs::set_permissions(
            executable.path(),
            std::os::unix::fs::PermissionsExt::from_mode(STABLE_BINARY_MODE),
        )
        .expect("stable mode");
        let state = observe_executable_install(&executable, &source).expect("observe drift");
        assert!(installed_binary_is_stale(&state));

        // Identical bytes are current, so a repair plans nothing for them.
        fs::write(executable.path(), b"the build running setup").expect("refresh");
        fs::set_permissions(
            executable.path(),
            std::os::unix::fs::PermissionsExt::from_mode(STABLE_BINARY_MODE),
        )
        .expect("stable mode");
        let state = observe_executable_install(&executable, &source).expect("observe current");
        assert!(!installed_binary_is_stale(&state));

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

    #[test]
    fn setup_refresh_rewrites_an_old_plist_with_a_matching_installed_binary() {
        let home = scratch_root("plist-drift");
        let executable = HostStableExecutable::new(&home, COWSHED_BINARY_NAME).unwrap();
        let spec = LaunchAgentSpec::gateway(&executable).unwrap();
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
        fs::write(executable.path(), b"the already installed release").unwrap();
        fs::set_permissions(
            executable.path(),
            std::os::unix::fs::PermissionsExt::from_mode(STABLE_BINARY_MODE),
        )
        .unwrap();
        let source = home.join("matching-build");
        fs::copy(executable.path(), &source).unwrap();
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
        fs::set_permissions(
            spec.plist_path(),
            std::os::unix::fs::PermissionsExt::from_mode(crate::launchd::PRIVATE_PLIST_MODE),
        )
        .unwrap();
        let installed_inode = fs::metadata(executable.path()).unwrap().ino();
        let mut executor = LaunchdExecutor::new(filesystem, RecordingGatewayLaunchctl::default());

        for source in [source, executable.path().to_path_buf()] {
            let refreshed =
                refresh_gateway_from(&home, source.clone(), true, &mut executor).unwrap();
            assert_eq!(
                refreshed,
                Some(ServiceBinaryRefresh::Refreshed {
                    service: GATEWAY_LABEL.to_owned(),
                })
            );
            assert_eq!(fs::read(spec.plist_path()).unwrap(), desired);
            assert_eq!(
                fs::metadata(executable.path()).unwrap().ino(),
                installed_inode
            );
            assert_eq!(
                refresh_gateway_from(&home, source, true, &mut executor).unwrap(),
                None
            );
            // Exercise setup invoked from the stable binary too, not just an identical external build.
            fs::write(spec.plist_path(), &legacy).unwrap();
        }
        let (_, command) = executor.into_parts();
        assert_eq!(command.argv.len(), 6);
        assert_eq!(command.argv[2][0], "bootstrap");
        assert_eq!(command.argv[5][0], "bootstrap");
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

    fn daemon(draining: bool, cause: Option<&str>, sha256: Option<&str>) -> GatewayStatus {
        GatewayStatus {
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
        fn rendered(socket: &Path, status: &GatewayStatus) -> String {
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
}
