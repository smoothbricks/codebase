use crate::args::GatewayCommand;
use crate::launchd::{
    COWSHED_BINARY_NAME, ExecutableInstallState, ExistingPlist, GATEWAY_LABEL,
    HostStableExecutable, InstallOutcome, InstallState, InstalledExecutable, LaunchAgentSpec,
    LaunchctlCommand, LaunchdExecutor, LaunchdFilesystem, LaunchdServiceStatus, NativeFilesystem,
    NativeLaunchctlCommand, RemovalOutcome, STABLE_BINARY_MODE, plan_executable_install,
    plan_executable_remove, plan_install, plan_remove,
};
use crate::output::Output;
use async_trait::async_trait;
use cowshed_core::api::{EmptyResult, GatewayStatus as CliGatewayStatus};
use cowshed_core::{
    CowshedError, NativeGatewayInventory, Result, ValidatedHostStorage,
    validate_existing_host_storage,
};
use cowshed_gateway::{
    ArrowAuditConfig, Gateway, GatewayConfig, GatewayControlClient, GatewayStatus,
    MirrorCacheConfig,
};
use std::fs;
use std::io::{self, Read as _, Write};
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub use cowshed_core::gateway_sessions::{
    GATEWAY_START_HINT, GatewayControl, GatewayInstaller, NativeSessionInventory, ReconcileReport,
    SessionInventory, canonical_home, control_error, control_socket_path, effective_uid,
    gateway_absent, install_all_sessions, policy_from_grants, project_session_prefix,
    reconcile_against_status, reconcile_inventory_project, reconcile_native_project,
    reconcile_project, session_from_fact, sessions_from_facts, stable_workspace_id,
};

/// How long `gateway start` waits for the daemon's control socket.
///
/// Sized for the startup heal rather than for a process start: the daemon attaches, checks, and
/// mounts every recorded project's images before it serves (05_gateway.md), and a host carrying
/// several multi-gigabyte mains needs minutes for that pass. Ten seconds timed out mid-heal and
/// told the user to kickstart a gateway that was working exactly as intended.
const START_DEADLINE: Duration = Duration::from_secs(180);
const START_POLL_INTERVAL: Duration = Duration::from_millis(100);
/// How often the wait says that it is still waiting, and on what.
const START_PROGRESS_INTERVAL: Duration = Duration::from_secs(5);
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayPaths {
    pub home: PathBuf,
    pub store: PathBuf,
    pub cache_volume: PathBuf,
    pub mirror_cache: PathBuf,
    pub telemetry: PathBuf,
    pub control_socket: PathBuf,
}

impl GatewayPaths {
    pub fn from_storage(storage: &ValidatedHostStorage) -> Self {
        Self {
            home: storage.home().to_path_buf(),
            store: storage.store().to_path_buf(),
            cache_volume: storage.caches().to_path_buf(),
            mirror_cache: storage.caches().join("mirror"),
            telemetry: storage.telemetry().join("gateway"),
            control_socket: control_socket_path(),
        }
    }

    pub fn config(&self, uid: u32, git_helper_executable: PathBuf) -> GatewayConfig {
        GatewayConfig {
            control_socket: Some(self.control_socket.clone()),
            control_tcp: None,
            simulator_drop_root: None,
            data_socket_root: None,
            production_cache_volume: Some(self.cache_volume.clone()),
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
}

#[async_trait]
impl GatewayDrain for Gateway {
    async fn drain(self) -> Result<()> {
        Gateway::drain(self)
            .await
            .map_err(|error| CowshedError::internal(format!("could not drain gateway: {error}")))
    }
}

pub async fn drain_after_shutdown<D, F>(daemon: D, shutdown: F) -> Result<()>
where
    D: GatewayDrain,
    F: Future<Output = Result<()>>,
{
    shutdown.await?;
    daemon.drain().await
}

fn launch_agent_is_loaded<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    uid: u32,
    spec: &LaunchAgentSpec,
) -> Result<bool>
where
    C: LaunchctlCommand,
{
    executor
        .execute_status(&crate::launchd::ControlPlan::print(uid, spec))
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
    spec: &LaunchAgentSpec,
    plist: InstallOutcome,
) -> Result<()>
where
    C: LaunchctlCommand,
{
    if plist == InstallOutcome::Changed {
        deactivate_launch_agent(executor, uid, spec)?;
    }
    if !launch_agent_is_loaded(executor, uid, spec)? {
        if let Err(error) =
            executor.execute_control(&crate::launchd::ControlPlan::bootstrap(uid, spec))
            && !launch_agent_is_loaded(executor, uid, spec)?
        {
            return Err(launchd_error(error));
        }
        return Ok(());
    }
    executor
        .execute_control(&crate::launchd::ControlPlan::kickstart(uid, spec))
        .map_err(launchd_error)?;
    Ok(())
}

pub fn deactivate_launch_agent<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    uid: u32,
    spec: &LaunchAgentSpec,
) -> Result<()>
where
    C: LaunchctlCommand,
{
    if launch_agent_is_loaded(executor, uid, spec)?
        && let Err(error) =
            executor.execute_control(&crate::launchd::ControlPlan::bootout(uid, spec))
        && launch_agent_is_loaded(executor, uid, spec)?
    {
        return Err(launchd_error(error));
    }
    Ok(())
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
    let executable = install_host_stable_executable(
        &mut executor,
        &home,
        COWSHED_BINARY_NAME,
        &running_executable()?,
    )?;
    let spec = LaunchAgentSpec::gateway(&executable).map_err(launchd_error)?;
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
    activate_launch_agent(&mut executor, uid, &spec, written)?;

    let client = GatewayControlClient::new(paths.control_socket.clone()).map_err(control_error)?;
    let mut progress = StartProgress::new(recorded_project_count(&storage).await);
    let started = tokio::time::Instant::now();
    loop {
        if let Ok(status) = client.status().await {
            return Ok(cli_status(true, true, paths.control_socket, Some(&status)));
        }
        let waited = started.elapsed();
        if waited >= START_DEADLINE {
            return Err(CowshedError::environment_missing(
                format!(
                    "gateway did not become healthy within {}s of starting",
                    START_DEADLINE.as_secs()
                ),
                kickstart_hint(uid),
            ));
        }
        if let Some(line) = progress.line(waited) {
            output.guidance(&line).map_err(output_error)?;
        }
        tokio::time::sleep(START_POLL_INTERVAL).await;
    }
}

/// How many projects the startup heal has to work through, or `None` when the store cannot say.
///
/// Counted from the store rather than asked of the gateway, because the gateway is precisely what
/// is not answering yet. A count that cannot be taken is not an error: it costs the wait its
/// number, and a wait that reports nothing at all is the defect being fixed.
async fn recorded_project_count(storage: &ValidatedHostStorage) -> Option<usize> {
    NativeGatewayInventory::new(storage.clone())
        .adopted_projects()
        .await
        .ok()
        .map(|projects| projects.len())
}

/// The wait's own reporting, at most one line per [`START_PROGRESS_INTERVAL`].
///
/// A first start after a reboot mounts every recorded project before the control socket answers,
/// which is minutes of silence on a host with several multi-gigabyte mains — long enough that the
/// only available conclusion is that cowshed has hung. So the wait says what it is waiting for and
/// how long it has waited.
struct StartProgress {
    projects: Option<usize>,
    next: Duration,
}

impl StartProgress {
    const fn new(projects: Option<usize>) -> Self {
        Self {
            projects,
            next: START_PROGRESS_INTERVAL,
        }
    }

    /// The line to emit after waiting `waited`, or `None` while the last one is still current.
    fn line(&mut self, waited: Duration) -> Option<String> {
        if waited < self.next {
            return None;
        }
        // Anchored to `waited` rather than advanced by one interval: a poll that returns late —
        // a heal saturating the disk — then reports once instead of flushing a backlog of lines
        // for intervals that have already passed.
        self.next = waited + START_PROGRESS_INTERVAL;
        Some(format!(
            "waited {}s for the gateway: {}",
            waited.as_secs(),
            match self.projects {
                None => String::from("mounting adopted projects…"),
                Some(0) => String::from("no adopted projects to mount"),
                Some(1) => String::from("mounting 1 adopted project…"),
                Some(count) => format!("mounting {count} adopted projects…"),
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
pub fn remove_launch_agent(spec: &LaunchAgentSpec) -> Result<RemovalOutcome> {
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    deactivate_launch_agent(&mut executor, effective_uid(), spec)?;
    let installed = fs::symlink_metadata(spec.plist_path()).is_ok();
    executor
        .execute_install(&plan_remove(spec, installed))
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
    remove_launch_agent(&spec)?;
    if purge {
        return remove_host_stable_executable(&executable);
    }
    Ok(RemovalOutcome::AlreadyAbsent)
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
            .execute_status(&crate::launchd::ControlPlan::print(effective_uid(), &spec))
            .map_err(launchd_error)?,
        LaunchdServiceStatus::Loaded { .. }
    );
    let status = if installed {
        let client = GatewayControlClient::new(socket.clone()).map_err(control_error)?;
        client.status().await.ok()
    } else {
        None
    };
    Ok(cli_status(
        installed,
        status.is_some(),
        socket,
        status.as_ref(),
    ))
}

async fn run_daemon() -> Result<()> {
    let home = canonical_home()?;
    let storage = validate_existing_host_storage(&home).await?;
    let paths = GatewayPaths::from_storage(&storage);
    ensure_private_directory(&paths.mirror_cache)?;
    ensure_private_directory(&paths.telemetry)?;
    // Startup contract (05_gateway.md): validated store, then heal every project's mounts, then
    // serve. The gateway is RunAtLoad, so this pass is what closes the reboot window in which a
    // checkout path would otherwise dangle until something touched it.
    heal_recorded_projects(&storage).await;
    heal_sccache_daemon().await;
    let inventory = NativeSessionInventory::new(storage);
    // The git credential helper is this same binary, which launchd started from the host-stable
    // path: a helper spawned by the daemon has to keep resolving for as long as the daemon runs.
    let config = paths.config(effective_uid(), running_executable()?);
    let telemetry = ArrowAuditConfig::new(paths.telemetry.clone())
        .map_err(|error| CowshedError::internal(format!("invalid gateway telemetry: {error}")))?;
    let gateway = Gateway::start_host(config, telemetry)
        .await
        .map_err(|error| CowshedError::internal(format!("could not start gateway: {error}")))?;
    let handle = gateway.handle();
    if let Err(primary) = install_all_sessions(&inventory, &handle).await {
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

/// Heal every recorded project, mains before sessions, reporting rather than raising.
///
/// A project that cannot be healed is a finding for `cowshed doctor`; it must never stop the
/// gateway from serving the healthy ones (05_gateway.md). Mains are logged apart from sessions
/// because they are not equally load-bearing: an unmounted main is the user's own checkout missing
/// from their shell and editor, which is why `doctor` reports it as critical and this line carries
/// the remedy with it.
async fn heal_recorded_projects(storage: &ValidatedHostStorage) {
    let inventory = NativeGatewayInventory::new(storage.clone());
    match inventory.heal_all().await {
        Ok(outcomes) => {
            for outcome in outcomes {
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
        Err(error) => {
            eprintln!("cowshed: could not list adopted projects at gateway startup: {error}");
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
    if let Err(error) = crate::sccache_service::start_service(None).await {
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

fn cli_status(
    installed: bool,
    running: bool,
    socket: PathBuf,
    status: Option<&GatewayStatus>,
) -> CliGatewayStatus {
    CliGatewayStatus {
        installed,
        running,
        socket,
        cli_version: env!("CARGO_PKG_VERSION").to_owned(),
        daemon_version: status.map(|status| status.version.clone()),
        cache_entries: 0,
        cache_bytes: 0,
        active_workspaces: status.map_or(0, |status| status.sessions.len() as u64),
    }
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
    let state = if status.running {
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

/// The outcome of reconciling one installed host-service binary with the invoking build.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ServiceBinaryRefresh {
    /// The installed copy was stale; it was reinstalled and the service kickstarted.
    Refreshed { service: String },
    /// The installed copy is stale, but this invocation cannot durably refresh it; the remedy
    /// names what can.
    Stale { service: String, remedy: String },
}

/// Whether the observed installed binary needs refreshing from the invoking build.
pub fn installed_binary_is_stale(state: &ExecutableInstallState) -> bool {
    !state
        .installed
        .is_some_and(|installed| installed.mode == STABLE_BINARY_MODE && installed.matches_source)
}

/// Reconcile the gateway's installed stable binary with the build running this command.
///
/// Setup never refuses to repair a host, and a service left running a binary from before the
/// build being invoked is exactly the drift a repair exists to end. `None` means there is
/// nothing to say: no gateway agent installed (nothing runs the binary), the bytes already
/// match, or this IS the installed copy speaking. A stale copy is reinstalled through the same
/// atomic plan `gateway start` uses and the agent is kickstarted so the running daemon picks the
/// new bytes up. The invoking build may live on a workspace volume; the copy does not.
pub fn refresh_gateway_binary(home: &Path) -> Result<Option<ServiceBinaryRefresh>> {
    let executable = HostStableExecutable::new(home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let source = running_executable()?;
    if source == executable.path() {
        return Ok(None);
    }
    let spec = LaunchAgentSpec::gateway(&executable).map_err(launchd_error)?;
    let observed = inspect_install_state(&spec)?;
    if observed.plist.is_none() {
        return Ok(None);
    }
    let state = observe_executable_install(&executable, &source)?;
    if !installed_binary_is_stale(&state) {
        return Ok(None);
    }
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    executor
        .execute_install(&plan_executable_install(&executable, &source, state))
        .map_err(launchd_error)?;
    // `Changed` forces the deactivate half of activation: the daemon currently running the old
    // bytes has to exit before the kickstart can start the new ones.
    activate_launch_agent(
        &mut executor,
        effective_uid(),
        &spec,
        InstallOutcome::Changed,
    )?;
    Ok(Some(ServiceBinaryRefresh::Refreshed {
        service: spec.label().to_owned(),
    }))
}

/// What the host has at the stable path, and whether it is already this source.
fn observe_executable_install(
    executable: &HostStableExecutable,
    source: &Path,
) -> Result<ExecutableInstallState> {
    let installed = match fs::symlink_metadata(executable.path()) {
        Ok(metadata) => {
            if !metadata.is_file()
                || metadata.file_type().is_symlink()
                || metadata.uid() != effective_uid()
            {
                return Err(CowshedError::integrity(
                    format!(
                        "the installed {} binary is not a user-owned regular file: {}",
                        executable.name(),
                        executable.path().display()
                    ),
                    "remove it and rerun the service start command",
                ));
            }
            Some(InstalledExecutable {
                mode: metadata.permissions().mode() & 0o777,
                matches_source: same_contents(source, executable.path(), metadata.len())?,
            })
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(CowshedError::internal(format!(
                "could not inspect {}: {error}",
                executable.path().display()
            )));
        }
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
    match fs::symlink_metadata(path) {
        Ok(metadata) => {
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || metadata.uid() != effective_uid()
            {
                return Err(CowshedError::integrity(
                    format!("path is not a user-owned directory: {}", path.display()),
                    format!("repair the ownership of {} and retry", path.display()),
                ));
            }
            Ok(Some(metadata.permissions().mode() & 0o777))
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(CowshedError::internal(format!(
            "could not inspect {}: {error}",
            path.display()
        ))),
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
    match fs::symlink_metadata(spec.plist_path()) {
        Ok(metadata) => {
            if !metadata.is_file()
                || metadata.file_type().is_symlink()
                || metadata.uid() != effective_uid()
            {
                return Err(CowshedError::integrity(
                    format!(
                        "{} LaunchAgent plist is not a user-owned regular file: {}",
                        spec.label(),
                        spec.plist_path().display()
                    ),
                    "remove the unsafe plist and rerun the service start command",
                ));
            }
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
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(ObservedInstallState {
            directory_mode,
            plist: None,
        }),
        Err(error) => Err(CowshedError::internal(format!(
            "could not inspect {}: {error}",
            spec.plist_path().display()
        ))),
    }
}

fn ensure_private_directory(path: &Path) -> Result<()> {
    fs::create_dir_all(path).map_err(|error| {
        CowshedError::internal(format!("could not create {}: {error}", path.display()))
    })?;
    let canonical = fs::canonicalize(path).map_err(|error| {
        CowshedError::internal(format!("could not resolve {}: {error}", path.display()))
    })?;
    if canonical != path {
        return Err(CowshedError::integrity(
            format!("gateway directory is not canonical: {}", path.display()),
            "cowshed doctor --json",
        ));
    }
    let metadata = fs::symlink_metadata(path).map_err(|error| {
        CowshedError::internal(format!("could not inspect {}: {error}", path.display()))
    })?;
    if !metadata.is_dir() || metadata.file_type().is_symlink() || metadata.uid() != effective_uid()
    {
        return Err(CowshedError::integrity(
            format!(
                "gateway path is not a private directory: {}",
                path.display()
            ),
            "cowshed doctor --json",
        ));
    }
    fs::set_permissions(path, fs::Permissions::from_mode(PRIVATE_DIRECTORY_MODE)).map_err(|error| {
        CowshedError::internal(format!(
            "could not secure gateway directory {}: {error}",
            path.display()
        ))
    })
}

/// Restarting an already-installed agent, for guidance that follows a
/// successful install.
fn kickstart_hint(uid: u32) -> String {
    format!("launchctl kickstart -k gui/{uid}/{GATEWAY_LABEL}")
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
            kickstart_hint(501),
            "launchctl kickstart -k gui/501/dev.cowshed.gateway"
        );
    }

    /// The wait stays quiet until an interval has passed, then speaks once per interval and names
    /// what the gateway is doing — a heal of several multi-gigabyte images, not a hung process.
    #[test]
    fn the_start_wait_reports_once_per_interval_with_the_project_count() {
        let mut progress = StartProgress::new(Some(7));

        assert_eq!(progress.line(Duration::from_secs(0)), None);
        assert_eq!(
            progress.line(START_PROGRESS_INTERVAL - Duration::from_millis(1)),
            None
        );
        assert_eq!(
            progress.line(START_PROGRESS_INTERVAL),
            Some(String::from(
                "waited 5s for the gateway: mounting 7 adopted projects…"
            ))
        );
        assert_eq!(progress.line(START_PROGRESS_INTERVAL), None);
        assert_eq!(
            progress.line(START_PROGRESS_INTERVAL * 2),
            Some(String::from(
                "waited 10s for the gateway: mounting 7 adopted projects…"
            ))
        );
    }

    /// A poll that returns long after its interval reports the wait it actually observed, once,
    /// rather than one line for every interval that elapsed while it was blocked.
    #[test]
    fn a_late_poll_reports_the_observed_wait_once() {
        let mut progress = StartProgress::new(Some(2));

        assert_eq!(
            progress.line(Duration::from_secs(90)),
            Some(String::from(
                "waited 90s for the gateway: mounting 2 adopted projects…"
            ))
        );
        assert_eq!(progress.line(Duration::from_secs(93)), None);
        assert_eq!(
            progress.line(Duration::from_secs(95)),
            Some(String::from(
                "waited 95s for the gateway: mounting 2 adopted projects…"
            ))
        );
    }

    /// The count is evidence, so the line never claims projects it did not count: an uncountable
    /// store says so, an empty one says so, and one project is not "1 projects".
    #[test]
    fn the_start_wait_never_overstates_what_it_counted() {
        for (projects, expected) in [
            (
                None,
                "waited 5s for the gateway: mounting adopted projects…",
            ),
            (
                Some(0),
                "waited 5s for the gateway: no adopted projects to mount",
            ),
            (
                Some(1),
                "waited 5s for the gateway: mounting 1 adopted project…",
            ),
        ] {
            assert_eq!(
                StartProgress::new(projects).line(START_PROGRESS_INTERVAL),
                Some(String::from(expected))
            );
        }
    }

    /// "heal" is cowshed's word for what it does to itself, not the user's word for what they are
    /// waiting on. A person watching `gateway start` is waiting for their workspaces to be
    /// mounted, and the line has to say that.
    #[test]
    fn the_start_wait_never_speaks_of_healing() {
        for projects in [None, Some(0), Some(1), Some(4)] {
            let line = StartProgress::new(projects)
                .line(START_PROGRESS_INTERVAL)
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
