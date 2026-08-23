use crate::args::GatewayCommand;
use crate::launchd::{
    ExistingPlist, InstallState, LaunchAgentSpec, LaunchctlCommand, LaunchdExecutor,
    LaunchdServiceStatus, NativeFilesystem, NativeLaunchctlCommand, plan_install, plan_remove,
};
use crate::output::Output;
use async_trait::async_trait;
use cowshed_core::api::{EmptyResult, GatewayStatus as CliGatewayStatus};
use cowshed_core::metadata::{EgressMode as CoreEgressMode, GrantSet, WorkspaceIncarnation};
use cowshed_core::repository::RepoId;
use cowshed_core::{
    CowshedError, GatewaySessionFact, NativeGatewayInventory, Result, ValidatedHostStorage,
    validate_existing_host_storage,
};
use cowshed_gateway::{
    ArrowAuditConfig, EgressGrant, Gateway, GatewayConfig, GatewayControlClient, GatewayHandle,
    GatewayStatus, MirrorCacheConfig, MirrorProtocol, MirrorRoute, WorkspaceCa, WorkspaceEndpoint,
    WorkspacePolicy, WorkspaceSession, WorkspaceToken, control_socket_path,
};
use sha2::{Digest as _, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs;
use std::io::{self, Write};
use std::net::{Ipv4Addr, SocketAddr};
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::time::Duration;

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

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReconcileReport {
    pub installed: usize,
    pub removed: usize,
}

#[async_trait]
pub trait GatewayControl: Send + Sync {
    async fn status(&self) -> std::result::Result<GatewayStatus, String>;
    async fn install(&self, session: &WorkspaceSession) -> std::result::Result<(), String>;
    async fn remove(
        &self,
        workspace_id: &str,
        expected_revision: u64,
    ) -> std::result::Result<(), String>;
}

#[async_trait]
impl GatewayControl for GatewayControlClient {
    async fn status(&self) -> std::result::Result<GatewayStatus, String> {
        GatewayControlClient::status(self)
            .await
            .map_err(|error| error.to_string())
    }

    async fn install(&self, session: &WorkspaceSession) -> std::result::Result<(), String> {
        GatewayControlClient::install(self, session)
            .await
            .map_err(|error| error.to_string())
    }

    async fn remove(
        &self,
        workspace_id: &str,
        expected_revision: u64,
    ) -> std::result::Result<(), String> {
        GatewayControlClient::remove(self, workspace_id, expected_revision)
            .await
            .map_err(|error| error.to_string())
    }
}

#[async_trait]
pub trait SessionInventory {
    async fn all_sessions(&self) -> Result<Vec<WorkspaceSession>>;
    async fn project_sessions(&self, repo_id: &RepoId) -> Result<Vec<WorkspaceSession>>;
}

pub struct NativeSessionInventory {
    inventory: NativeGatewayInventory,
}

impl NativeSessionInventory {
    pub fn new(storage: ValidatedHostStorage) -> Self {
        Self {
            inventory: NativeGatewayInventory::new(storage),
        }
    }
}

#[async_trait]
impl SessionInventory for NativeSessionInventory {
    async fn all_sessions(&self) -> Result<Vec<WorkspaceSession>> {
        sessions_from_facts(
            self.inventory
                .all_attached()
                .await
                .map_err(inventory_error)?,
        )
    }

    async fn project_sessions(&self, repo_id: &RepoId) -> Result<Vec<WorkspaceSession>> {
        sessions_from_facts(
            self.inventory
                .project_attached(repo_id)
                .await
                .map_err(inventory_error)?,
        )
    }
}

#[async_trait]
pub trait GatewayInstaller: Send + Sync {
    async fn install_session(&self, session: WorkspaceSession) -> Result<()>;
}

#[async_trait]
impl GatewayInstaller for GatewayHandle {
    async fn install_session(&self, session: WorkspaceSession) -> Result<()> {
        self.install(session).await.map_err(|error| {
            CowshedError::internal(format!("could not restore gateway session: {error}"))
        })
    }
}

pub async fn install_all_sessions<I, G>(inventory: &I, gateway: &G) -> Result<usize>
where
    I: SessionInventory,
    G: GatewayInstaller,
{
    let sessions = inventory.all_sessions().await?;
    let count = sessions.len();
    for session in sessions {
        gateway.install_session(session).await?;
    }
    Ok(count)
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

pub fn project_session_prefix(repo_id: &RepoId) -> String {
    let digest = Sha256::digest(repo_id.as_str().as_bytes());
    format!("p{}.", hex_prefix(&digest, 16))
}

/// Stable for one workspace incarnation. Restore and re-adopt rotate the identity so the gateway's
/// replay-protection tombstone cannot reject a legitimate lifecycle reset.
pub fn stable_workspace_id(
    repo_id: &RepoId,
    workspace: &str,
    incarnation: &WorkspaceIncarnation,
) -> String {
    let prefix = project_session_prefix(repo_id);
    let mut hasher = Sha256::new();
    hasher.update(repo_id.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(workspace.as_bytes());
    hasher.update([0]);
    hasher.update(incarnation.as_str().as_bytes());
    let digest = hasher.finalize();
    format!("{prefix}w{}", hex_prefix(&digest, 16))
}

fn hex_prefix(bytes: &[u8], count: usize) -> String {
    let mut encoded = String::with_capacity(count * 2);
    for byte in &bytes[..count] {
        write!(&mut encoded, "{byte:02x}").expect("writing to a String cannot fail");
    }
    encoded
}

pub fn policy_from_grants(grants: &GrantSet) -> Result<WorkspacePolicy> {
    let mut policy = WorkspacePolicy {
        grants: Vec::new(),
        mirrors: baseline_mirror_routes(),
    };
    for rule in &grants.egress {
        let ports: Vec<u16> = if rule.ports.is_empty() {
            vec![443, 80]
        } else {
            rule.ports.clone()
        };
        for port in ports {
            let mut grant = match rule.mode {
                CoreEgressMode::Intercept => EgressGrant::intercept(&rule.host, port),
                CoreEgressMode::Opaque => EgressGrant::opaque(&rule.host, port),
            }
            .map_err(|error| {
                CowshedError::integrity(
                    format!("gateway grant for {} is invalid: {error}", rule.host),
                    "cowshed doctor --json",
                )
            })?;
            grant.impersonate = rule.impersonate.is_some();
            policy.grants.push(grant);
        }
    }
    policy.validate().map_err(|error| {
        CowshedError::integrity(
            format!("gateway policy is invalid: {error}"),
            "cowshed doctor --json",
        )
    })?;
    Ok(policy)
}

fn baseline_mirror_routes() -> Vec<MirrorRoute> {
    [
        MirrorProtocol::Npm,
        MirrorProtocol::Cargo,
        MirrorProtocol::Go,
    ]
    .into_iter()
    .map(|protocol| MirrorRoute {
        local_prefix: protocol.local_prefix().to_owned(),
        upstream_origin: protocol.baseline_origin().to_owned(),
        protocol,
        admitted_prefixes: vec!["/".to_owned()],
        credentialed: true,
    })
    .collect()
}

pub fn session_from_fact(fact: GatewaySessionFact) -> Result<WorkspaceSession> {
    let endpoint = SocketAddr::from((Ipv4Addr::LOCALHOST, fact.port_block.base()));
    let token = WorkspaceToken::parse(fact.credentials.token()).map_err(|error| {
        CowshedError::integrity(
            format!(
                "gateway token for {}/{} is invalid: {error}",
                fact.repo_id, fact.workspace
            ),
            "cowshed doctor --json",
        )
    })?;
    let ca = WorkspaceCa::new(
        fact.credentials.certificate_pem().to_owned(),
        fact.credentials.private_key_pem().to_owned(),
    )
    .map_err(|error| {
        CowshedError::integrity(
            format!(
                "gateway CA for {}/{} is invalid: {error}",
                fact.repo_id, fact.workspace
            ),
            "cowshed doctor --json",
        )
    })?;
    let session = WorkspaceSession {
        workspace_id: stable_workspace_id(
            &fact.repo_id,
            fact.workspace.as_str(),
            &fact.incarnation,
        ),
        repo_id: fact.repo_id.as_str().to_owned(),
        revision: fact.revision,
        endpoint: WorkspaceEndpoint::Tcp(endpoint),
        token,
        ca,
        policy: policy_from_grants(&fact.grants)?,
    };
    session.validate().map_err(|error| {
        CowshedError::integrity(
            format!(
                "gateway session for {}/{} is invalid: {error}",
                fact.repo_id, fact.workspace
            ),
            "cowshed doctor --json",
        )
    })?;
    Ok(session)
}

pub fn sessions_from_facts(
    facts: impl IntoIterator<Item = GatewaySessionFact>,
) -> Result<Vec<WorkspaceSession>> {
    facts.into_iter().map(session_from_fact).collect()
}

pub async fn reconcile_project<C, I>(
    control: &C,
    host: &I,
    project_prefix: &str,
    desired: Vec<WorkspaceSession>,
    uid: u32,
) -> Result<ReconcileReport>
where
    C: GatewayControl + ?Sized,
    I: SessionInventory + ?Sized,
{
    let status = control.status().await.map_err(|_| gateway_absent(uid))?;
    reconcile_against_status(control, host, project_prefix, desired, status).await
}

/// Bring the gateway's sessions for one project in line with that project's inventory.
///
/// The gateway's session table is a cache of host inventory, never an authority of its own. The
/// project's own stale sessions are removed by identity. A session from *another* project that
/// holds an endpoint this project's inventory assigns to one of its workspaces is the leak left
/// behind when a project is deleted out of band (its sessions outlive its store directory while
/// the host-global port-block allocator hands the block to the next workspace); it is removed
/// only once `host` confirms no live workspace anywhere still claims that identity, so a genuine
/// two-projects-one-block inventory fault is reported instead of papered over. The host lookup
/// runs only when such a conflict exists. Installs are independent, so one workspace that cannot
/// be installed does not abandon the rest: every failure is reported together.
pub async fn reconcile_against_status<C, I>(
    control: &C,
    host: &I,
    project_prefix: &str,
    desired: Vec<WorkspaceSession>,
    status: GatewayStatus,
) -> Result<ReconcileReport>
where
    C: GatewayControl + ?Sized,
    I: SessionInventory + ?Sized,
{
    let mut desired_by_id = BTreeMap::new();
    for session in desired {
        let identity = session.workspace_id.clone();
        if !identity.starts_with(project_prefix) {
            return Err(CowshedError::integrity(
                "gateway session is outside the reconciled project namespace",
                "cowshed doctor --json",
            ));
        }
        if desired_by_id.insert(identity, session).is_some() {
            return Err(CowshedError::integrity(
                "gateway inventory contains a duplicate workspace identity",
                "cowshed doctor --json",
            ));
        }
    }

    let installed_by_id: BTreeMap<_, _> = status
        .sessions
        .iter()
        .filter(|session| session.workspace_id.starts_with(project_prefix))
        .map(|session| (session.workspace_id.as_str(), session))
        .collect();
    let mut report = ReconcileReport::default();
    let desired_ids: BTreeSet<_> = desired_by_id.keys().map(String::as_str).collect();
    for installed in status
        .sessions
        .iter()
        .filter(|session| session.workspace_id.starts_with(project_prefix))
    {
        if !desired_ids.contains(installed.workspace_id.as_str()) {
            control
                .remove(&installed.workspace_id, installed.revision)
                .await
                .map_err(|error| {
                    CowshedError::internal(format!(
                        "could not remove stale gateway session {}: {error}",
                        installed.workspace_id
                    ))
                })?;
            report.removed += 1;
        }
    }
    let desired_endpoints: BTreeMap<String, &str> = desired_by_id
        .iter()
        .map(|(identity, session)| (session.endpoint.to_string(), identity.as_str()))
        .collect();
    let foreign_owners: Vec<_> = status
        .sessions
        .iter()
        .filter(|session| !session.workspace_id.starts_with(project_prefix))
        .filter_map(|session| {
            desired_endpoints
                .get(&session.endpoint)
                .map(|claimant| (session, *claimant))
        })
        .collect();
    if !foreign_owners.is_empty() {
        let live: BTreeSet<String> = host
            .all_sessions()
            .await?
            .into_iter()
            .map(|session| session.workspace_id)
            .collect();
        for (owner, claimant) in foreign_owners {
            if live.contains(&owner.workspace_id) {
                return Err(CowshedError::integrity(
                    format!(
                        "gateway endpoint {} is assigned to workspace {claimant} by this project and still claimed by live workspace {} of another project",
                        owner.endpoint, owner.workspace_id
                    ),
                    "cowshed doctor --json",
                ));
            }
            control
                .remove(&owner.workspace_id, owner.revision)
                .await
                .map_err(|error| {
                    CowshedError::internal(format!(
                        "could not remove stale gateway session {} holding endpoint {}: {error}",
                        owner.workspace_id, owner.endpoint
                    ))
                })?;
            report.removed += 1;
        }
    }
    let mut failures = Vec::new();
    for (identity, session) in &desired_by_id {
        let unchanged = installed_by_id
            .get(identity.as_str())
            .is_some_and(|installed| installed.revision == session.revision);
        if unchanged {
            continue;
        }
        match control.install(session).await {
            Ok(()) => report.installed += 1,
            Err(error) => failures.push(format!(
                "could not install gateway session {identity}: {error}"
            )),
        }
    }
    if failures.is_empty() {
        Ok(report)
    } else {
        Err(CowshedError::internal(failures.join("; ")))
    }
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

pub fn activate_launch_agent<F, C>(
    executor: &mut LaunchdExecutor<F, C>,
    uid: u32,
    spec: &LaunchAgentSpec,
) -> Result<()>
where
    C: LaunchctlCommand,
{
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

pub async fn reconcile_inventory_project<I, C>(
    inventory: &I,
    control: &C,
    repo_id: &RepoId,
    uid: u32,
) -> Result<ReconcileReport>
where
    I: SessionInventory,
    C: GatewayControl + ?Sized,
{
    reconcile_project(
        control,
        inventory,
        &project_session_prefix(repo_id),
        inventory.project_sessions(repo_id).await?,
        uid,
    )
    .await
}

pub async fn reconcile_native_project(repo_id: &RepoId) -> Result<ReconcileReport> {
    let home = canonical_home()?;
    let storage = validate_existing_host_storage(&home).await?;
    let paths = GatewayPaths::from_storage(&storage);
    let inventory = NativeSessionInventory::new(storage);
    let control = GatewayControlClient::new(paths.control_socket.clone()).map_err(control_error)?;
    reconcile_inventory_project(&inventory, &control, repo_id, effective_uid()).await
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
    let executable = fs::canonicalize(std::env::current_exe().map_err(|error| {
        CowshedError::environment_missing(
            format!("could not identify the cowshed executable: {error}"),
            "reinstall cowshed",
        )
    })?)
    .map_err(|error| {
        CowshedError::environment_missing(
            format!("could not resolve the cowshed executable: {error}"),
            "reinstall cowshed",
        )
    })?;
    let spec = LaunchAgentSpec::gateway(&home, &executable).map_err(launchd_error)?;
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
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    executor.execute_install(&plan).map_err(launchd_error)?;
    activate_launch_agent(&mut executor, uid, &spec)?;

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

fn stop_service() -> Result<()> {
    let home = canonical_home()?;
    let executable = fs::canonicalize(std::env::current_exe().map_err(|error| {
        CowshedError::internal(format!(
            "could not identify the cowshed executable: {error}"
        ))
    })?)
    .map_err(|error| {
        CowshedError::internal(format!("could not resolve the cowshed executable: {error}"))
    })?;
    let spec = LaunchAgentSpec::gateway(&home, &executable).map_err(launchd_error)?;
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
    let executable = fs::canonicalize(std::env::current_exe().map_err(|error| {
        CowshedError::internal(format!(
            "could not identify the cowshed executable: {error}"
        ))
    })?)
    .map_err(|error| {
        CowshedError::internal(format!("could not resolve the cowshed executable: {error}"))
    })?;
    let spec = LaunchAgentSpec::gateway(&home, &executable).map_err(launchd_error)?;
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
    let executable = fs::canonicalize(std::env::current_exe().map_err(|error| {
        CowshedError::internal(format!(
            "could not identify the cowshed executable: {error}"
        ))
    })?)
    .map_err(|error| {
        CowshedError::internal(format!("could not resolve the cowshed executable: {error}"))
    })?;
    let config = paths.config(effective_uid(), executable);
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

pub(crate) struct ObservedInstallState {
    pub(crate) directory_mode: Option<u32>,
    pub(crate) plist: Option<ObservedPlist>,
}

pub(crate) struct ObservedPlist {
    pub(crate) bytes: Vec<u8>,
    pub(crate) mode: u32,
}

pub(crate) fn inspect_install_state(spec: &LaunchAgentSpec) -> Result<ObservedInstallState> {
    let directory_mode = match fs::symlink_metadata(spec.launch_agents_directory()) {
        Ok(metadata) => {
            if !metadata.is_dir()
                || metadata.file_type().is_symlink()
                || metadata.uid() != effective_uid()
            {
                return Err(CowshedError::integrity(
                    format!(
                        "LaunchAgents path is not a user-owned directory: {}",
                        spec.launch_agents_directory().display()
                    ),
                    "repair ~/Library/LaunchAgents ownership and retry",
                ));
            }
            Some(metadata.permissions().mode() & 0o777)
        }
        Err(error) if error.kind() == io::ErrorKind::NotFound => None,
        Err(error) => {
            return Err(CowshedError::internal(format!(
                "could not inspect {}: {error}",
                spec.launch_agents_directory().display()
            )));
        }
    };
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

pub(crate) fn canonical_home() -> Result<PathBuf> {
    let home = std::env::var_os("HOME").ok_or_else(|| {
        CowshedError::environment_missing("HOME is not set", "set HOME to your login directory")
    })?;
    let home = PathBuf::from(home);
    let canonical = fs::canonicalize(&home).map_err(|error| {
        CowshedError::environment_missing(
            format!("could not resolve HOME {}: {error}", home.display()),
            "set HOME to your login directory",
        )
    })?;
    if canonical != home {
        return Err(CowshedError::integrity(
            "HOME must be an absolute canonical path",
            "set HOME to your canonical login directory",
        ));
    }
    Ok(home)
}

pub(crate) fn effective_uid() -> u32 {
    unsafe { libc::geteuid() }
}

/// `cowshed gateway start` both installs the launch agent and starts it, so it
/// is the correct guidance whether or not the agent exists yet. A raw
/// `launchctl kickstart` fails with "service not found" on a host where the
/// agent was never installed, which is exactly the state this hint is reached
/// from most often.
const GATEWAY_START_HINT: &str = "cowshed gateway start";

/// Restarting an already-installed agent, for guidance that follows a
/// successful install.
fn kickstart_hint(uid: u32) -> String {
    format!("launchctl kickstart -k gui/{uid}/dev.cowshed.gateway")
}

fn gateway_absent(_uid: u32) -> CowshedError {
    CowshedError::environment_missing("cowshed gateway is not available", GATEWAY_START_HINT)
}

fn inventory_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(
        format!("gateway inventory failed: {error}"),
        "cowshed doctor --json",
    )
}

fn control_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::internal(format!("invalid gateway control configuration: {error}"))
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
