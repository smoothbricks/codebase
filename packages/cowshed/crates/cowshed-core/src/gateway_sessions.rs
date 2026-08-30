//! The gateway's session table is a cache of host inventory, never an authority.
//!
//! Every workspace's gateway session — its endpoint (port block), token, CA, and egress policy
//! from the grants file — is derived from the machine-global store under
//! `/private/cowshed/store`; the running gateway only caches those sessions so it can answer
//! data-plane connections. Reconcile repairs
//! the cache from the inventory: the project's stale sessions are removed by identity, a session a
//! deleted project left holding a port block this project now owns is evicted once no live
//! workspace claims it, a live collision is an integrity refusal naming both, and installs are
//! independent so one refusal does not abandon the rest. Every controller that needs the gateway
//! runs it first — the CLI before a gateway-requiring verb, a supervising runtime before an exec.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};

use async_trait::async_trait;
use cowshed_gateway_types::{
    EgressGrant, GatewayStatus, MirrorProtocol, MirrorRoute, WorkspaceCa, WorkspaceEndpoint,
    WorkspacePolicy, WorkspaceSession, WorkspaceToken,
};
use sha2::{Digest as _, Sha256};

use crate::api::dto::hex_lower;
use crate::error::{CowshedError, Result};
use crate::gateway_inventory::{GatewaySessionFact, NativeGatewayInventory};
use crate::metadata::{EgressMode as CoreEgressMode, GrantSet, WorkspaceIncarnation};
use crate::repository::RepoId;
use crate::storage::bootstrap::{STORE_ROOT, ValidatedHostStorage};

/// The gateway control socket, at the store root.
///
/// Root-level keeps `sun_path` short (01_storage.md): a Unix socket address is capped near 104
/// bytes, and a home-relative path spent that budget on the user's name before the gateway had
/// said anything. The store is also the only location every peer agrees on — a LaunchAgent, the
/// CLI, and a supervising runtime each know the store, while `HOME` differs between them.
///
/// Derived here rather than in `cowshed-gateway` because the canonical roots live in this crate
/// and the gateway crate sits below it: one definition, no constant copied across a crate
/// boundary.
pub fn control_socket_path() -> PathBuf {
    Path::new(STORE_ROOT).join("gateway.sock")
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReconcileReport {
    pub installed: usize,
    pub removed: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatewayStatusError {
    Absent,
    Control(String),
}

/// The running daemon, as reconciliation needs to see it.
///
/// Everything below is a pure function of inventory and this trait's three answers, so the
/// controller never links the daemon: `cowshed-cli` implements it over the real control client,
/// and the tests implement it over a fake. `GatewayStatusError::Absent` is a distinct answer
/// rather than an error string because "no gateway is running" is a normal state with its own
/// operator remedy, not a control-plane fault.
#[async_trait]
pub trait GatewayControl: Send + Sync {
    async fn status(&self) -> std::result::Result<GatewayStatus, GatewayStatusError>;
    async fn install(&self, session: &WorkspaceSession) -> std::result::Result<(), String>;
    async fn remove(
        &self,
        workspace_id: &str,
        expected_revision: u64,
    ) -> std::result::Result<(), String>;
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

/// Installing into a daemon this process itself owns, as opposed to one reached over the control
/// socket. Implemented in `cowshed-cli` over the in-process `GatewayHandle`, for the same reason
/// as [`GatewayControl`]: recovery is inventory-driven and needs no runtime of its own.
#[async_trait]
pub trait GatewayInstaller: Send + Sync {
    async fn install_session(&self, session: WorkspaceSession) -> Result<()>;
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

pub fn project_session_prefix(repo_id: &RepoId) -> String {
    let digest = Sha256::digest(repo_id.as_str().as_bytes());
    format!("p{}.", hex_lower(&digest[..16]))
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
    format!("{prefix}w{}", hex_lower(&digest[..16]))
}

pub fn policy_from_grants(grants: &GrantSet) -> Result<WorkspacePolicy> {
    let mut policy = WorkspacePolicy {
        grants: Vec::new(),
        mirrors: baseline_mirror_routes(),
    };
    for rule in &grants.egress {
        for &port in rule.effective_ports() {
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
    let status = control.status().await.map_err(|error| match error {
        GatewayStatusError::Absent => gateway_absent(uid),
        GatewayStatusError::Control(message) => {
            CowshedError::internal(format!("gateway status failed: {message}"))
        }
    })?;
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

pub fn canonical_home() -> Result<PathBuf> {
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

pub fn effective_uid() -> u32 {
    // SAFETY: geteuid has no preconditions and reads no caller-owned memory.
    unsafe { libc::geteuid() }
}

/// `cowshed gateway start` both installs the launch agent and starts it, so it
/// is the correct guidance whether or not the agent exists yet. A raw
/// `launchctl kickstart` fails with "service not found" on a host where the
/// agent was never installed, which is exactly the state this hint is reached
/// from most often.
pub const GATEWAY_START_HINT: &str = "cowshed gateway start";

pub fn gateway_absent(_uid: u32) -> CowshedError {
    CowshedError::environment_missing("cowshed gateway is not available", GATEWAY_START_HINT)
}

fn inventory_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::integrity(
        format!("gateway inventory failed: {error}"),
        "cowshed doctor --json",
    )
}

pub fn control_error(error: impl std::fmt::Display) -> CowshedError {
    CowshedError::internal(format!("invalid gateway control configuration: {error}"))
}
