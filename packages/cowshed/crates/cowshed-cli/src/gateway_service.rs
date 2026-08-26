use crate::args::GatewayCommand;
use crate::launchd::{
    COWSHED_BINARY_NAME, ExecutableInstallState, ExecutableSource, ExistingPlist,
    HostStableExecutable, InstallOutcome, InstallState, InstalledExecutable, LaunchAgentSpec,
    LaunchctlCommand, LaunchdExecutor, LaunchdFilesystem, LaunchdServiceStatus, NativeFilesystem,
    NativeLaunchctlCommand, classify_executable_source, containing_mount_point,
    plan_executable_install, plan_install, plan_remove,
};
use crate::output::Output;
use async_trait::async_trait;
use cowshed_core::api::{EmptyResult, GatewayStatus as CliGatewayStatus};
use cowshed_core::storage::WORKSPACE_MARKER_PATH;
use cowshed_core::{
    CowshedError, NativeGatewayInventory, Result, ValidatedHostStorage,
    validate_existing_host_storage,
};
use cowshed_gateway::{
    ArrowAuditConfig, Gateway, GatewayConfig, GatewayControlClient, GatewayStatus,
    MirrorCacheConfig, control_socket_path,
};
use std::fs;
use std::io::{self, Read as _, Write};
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::time::Duration;

pub use cowshed_core::gateway_sessions::{
    GATEWAY_START_HINT, GatewayControl, GatewayInstaller, NativeSessionInventory, ReconcileReport,
    SessionInventory, canonical_home, control_error, effective_uid, gateway_absent,
    install_all_sessions, policy_from_grants, project_session_prefix, reconcile_against_status,
    reconcile_inventory_project, reconcile_native_project, reconcile_project, session_from_fact,
    sessions_from_facts, stable_workspace_id,
};

const START_DEADLINE: Duration = Duration::from_secs(10);
const START_POLL_INTERVAL: Duration = Duration::from_millis(100);
const PRIVATE_DIRECTORY_MODE: u32 = 0o700;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GatewayPaths {
    pub home: PathBuf,
    pub store: PathBuf,
    pub cache: PathBuf,
    pub telemetry: PathBuf,
    pub control_socket: PathBuf,
}

impl GatewayPaths {
    pub fn from_storage(storage: &ValidatedHostStorage) -> Self {
        Self {
            home: storage.home().to_path_buf(),
            store: storage.store().to_path_buf(),
            cache: storage.caches().join("mirror"),
            telemetry: storage.telemetry().join("gateway"),
            control_socket: control_socket_path(storage.home()),
        }
    }

    pub fn config(&self, uid: u32, git_helper_executable: PathBuf) -> GatewayConfig {
        GatewayConfig {
            control_socket: Some(self.control_socket.clone()),
            control_tcp: None,
            simulator_drop_root: None,
            data_socket_root: None,
            git_helper_executable: Some(git_helper_executable),
            authorized_control_uid: uid,
            mirror_cache: MirrorCacheConfig::new(self.cache.clone()),
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
    if !launch_agent_is_loaded(executor, uid, spec)?
        && let Err(error) =
            executor.execute_control(&crate::launchd::ControlPlan::bootstrap(uid, spec))
        && !launch_agent_is_loaded(executor, uid, spec)?
    {
        return Err(launchd_error(error));
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
            let status = start_service().await?;
            emit_gateway_status(output, json, status)?;
        }
        GatewayCommand::Stop => {
            stop_service()?;
            if json {
                output.success(EmptyResult {}).map_err(output_error)?;
            } else {
                output
                    .guidance("gateway is stopped")
                    .map_err(output_error)?;
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

async fn start_service() -> Result<CliGatewayStatus> {
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
    let deadline = tokio::time::Instant::now() + START_DEADLINE;
    loop {
        if let Ok(status) = client.status().await {
            return Ok(cli_status(true, paths.control_socket, Some(&status)));
        }
        if tokio::time::Instant::now() >= deadline {
            return Err(CowshedError::environment_missing(
                "gateway did not become healthy before the startup deadline",
                kickstart_hint(uid),
            ));
        }
        tokio::time::sleep(START_POLL_INTERVAL).await;
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

fn stop_service() -> Result<()> {
    let home = canonical_home()?;
    // Deterministic, not derived from the running binary: stop has to reach the agent that
    // `start` installed however this process was invoked. The installed copy stays — it is host
    // state, not agent state, and leaving it makes the next start a plist write.
    let executable =
        HostStableExecutable::new(&home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let spec = LaunchAgentSpec::gateway(&executable).map_err(launchd_error)?;
    let uid = effective_uid();
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    deactivate_launch_agent(&mut executor, uid, &spec)?;
    let installed = fs::symlink_metadata(spec.plist_path()).is_ok();
    executor
        .execute_install(&plan_remove(&spec, installed))
        .map_err(launchd_error)?;
    Ok(())
}

async fn service_status() -> Result<CliGatewayStatus> {
    let home = canonical_home()?;
    let socket = control_socket_path(&home);
    let executable =
        HostStableExecutable::new(&home, COWSHED_BINARY_NAME).map_err(launchd_error)?;
    let spec = LaunchAgentSpec::gateway(&executable).map_err(launchd_error)?;
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    match executor
        .execute_status(&crate::launchd::ControlPlan::print(effective_uid(), &spec))
        .map_err(launchd_error)?
    {
        LaunchdServiceStatus::NotLoaded { .. } => Ok(cli_status(false, socket, None)),
        LaunchdServiceStatus::Loaded { .. } => {
            let client = GatewayControlClient::new(socket.clone()).map_err(control_error)?;
            let status = client
                .status()
                .await
                .map_err(|_| gateway_absent(effective_uid()))?;
            Ok(cli_status(true, socket, Some(&status)))
        }
    }
}

async fn run_daemon() -> Result<()> {
    let home = canonical_home()?;
    let storage = validate_existing_host_storage(&home).await?;
    let paths = GatewayPaths::from_storage(&storage);
    ensure_private_directory(&paths.cache)?;
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

/// Heal every recorded project, reporting rather than raising. A project that cannot be healed is
/// a finding for `cowshed doctor`; it must never stop the gateway from serving the healthy ones.
async fn heal_recorded_projects(storage: &ValidatedHostStorage) {
    let inventory = NativeGatewayInventory::new(storage.clone());
    match inventory.heal_all().await {
        Ok(outcomes) => {
            for (repo, outcome) in outcomes {
                if let Err(error) = outcome {
                    eprintln!("cowshed: could not heal {repo} at startup: {error}");
                }
            }
        }
        Err(error) => {
            eprintln!("cowshed: could not enumerate projects to heal at startup: {error}");
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

fn cli_status(running: bool, socket: PathBuf, status: Option<&GatewayStatus>) -> CliGatewayStatus {
    CliGatewayStatus {
        running,
        socket,
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
    } else if status.running {
        output
            .guidance("gateway is healthy")
            .map_err(output_error)?;
    } else {
        output
            .guidance("gateway is stopped")
            .map_err(output_error)?;
    }
    Ok(())
}

/// Install `source` at the host-stable path launchd will run, and answer with that path.
///
/// This is what keeps a LaunchAgent independent of wherever the user's cowshed happens to live:
/// the plist names a copy on the volume that carries the plist itself, so the agent starts on a
/// host that has since rebuilt, updated, or deleted the binary that installed it.
///
/// A binary on cowshed's own storage is refused rather than copied. That is the incident this
/// exists for — a gateway installed from inside a workspace mount exited 78 in a loop after a
/// reboot, with nothing left to mount what would have healed it — and a workspace's own build is
/// not the host's cowshed.
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
    require_durable_source(home, &executable, source)?;
    let state = observe_executable_install(&executable, source)?;
    executor
        .execute_install(&plan_executable_install(&executable, source, state))
        .map_err(launchd_error)?;
    Ok(executable)
}

/// Refuse a source launchd could not reach at boot, saying which volume and why.
///
/// The hint names the installed copy when there is one, because that is the case an operator
/// actually hits: a cowshed reached through a workspace-resident trampoline cannot install the
/// agent, while the copy already on the host volume can.
fn require_durable_source(
    home: &Path,
    executable: &HostStableExecutable,
    source: &Path,
) -> Result<()> {
    let mount_point = containing_mount_point(source).map_err(|error| {
        CowshedError::internal(format!(
            "could not resolve the volume holding {}: {error}",
            source.display()
        ))
    })?;
    let observed = ExecutableSource {
        path: source,
        mount_point: &mount_point,
        mount_is_workspace: fs::symlink_metadata(mount_point.join(WORKSPACE_MARKER_PATH)).is_ok(),
    };
    classify_executable_source(home, observed).map_err(|unstable| {
        let hint = if fs::symlink_metadata(executable.path()).is_ok() {
            format!("start the service from {}", executable.path().display())
        } else {
            format!(
                "install {} outside every cowshed workspace and run this command from that binary",
                executable.name()
            )
        };
        CowshedError::environment_missing(
            format!(
                "{unstable}, so a LaunchAgent installed from it would dangle after a reboot: \
                 launchd starts the agent before cowshed has mounted anything, and the service \
                 exits 78 in a KeepAlive loop with nothing left to mount what would heal it"
            ),
            hint,
        )
    })
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
    format!("launchctl kickstart -k gui/{uid}/dev.cowshed.gateway")
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
}
