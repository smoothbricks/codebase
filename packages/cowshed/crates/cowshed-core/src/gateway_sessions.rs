//! The gateway's session table is a cache of host inventory, never an authority.
//!
//! Every workspace's gateway session — its endpoint (port block), token, CA, and egress policy
//! from the grants file — is derived from the machine-global store under
//! `/private/cowshed/store`; the running gateway only caches those sessions so it can answer
//! data-plane connections. Reconcile removes the selected workspace's stale incarnations by
//! identity before a job starts.
//! Project lifecycle and maintenance additionally evict a deleted project's session holding a
//! reclaimed port block once no live workspace claims it. Workspace jobs never inspect or remove
//! sibling sessions. Reconciles read gateway status before their inventory snapshot, and a write
//! refused because a newer reconcile overtook it is superseded, not failed.

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;

use async_trait::async_trait;
use cowshed_gateway_types::{
    CanonicalHost, DaemonStatus, EgressGrant, WorkspaceCa, WorkspaceEndpoint, WorkspacePolicy,
    WorkspaceSession, WorkspaceToken,
};
use sha2::{Digest as _, Sha256};

use crate::api::dto::hex_lower;
use crate::error::{CowshedError, Result};
use crate::gateway_inventory::{
    AttachedInventory, GatewaySessionFact, HeldWorkspace, NativeGatewayInventory, PortCollision,
    served_sessions,
};
use crate::storage::bootstrap::{FixturePortClaims, ValidatedHostStorage};
use cowshed_types::metadata::{
    EgressMode as CoreEgressMode, GrantSet, WorkspaceIncarnation, WorkspaceName,
};
use cowshed_types::repository::RepoId;
use cowshed_types::wait::WaitFailure;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ReconcileReport {
    pub installed: usize,
    pub removed: usize,
    /// Writes the gateway refused because a concurrent reconcile had already acted on a newer
    /// snapshot of that workspace: its decision stands, and this reconcile's older one is dropped.
    pub superseded: usize,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum GatewayStatusError {
    Absent,
    /// The status request's wait expired (05_gateway.md, "Deadlines").
    Waited(Box<WaitFailure>),
    Control(String),
}

/// Why the gateway refused a reconcile write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ControlRefusal {
    /// The gateway already holds a decision about this workspace newer than the reconciling
    /// snapshot: the install's revision does not exceed one the workspace has held, or the session
    /// a removal names has since been replaced or removed. Another reconcile that read a newer
    /// snapshot made it, and it stands.
    Superseded(String),
    /// The host kernel already owns the listener; a publisher may choose another port block.
    AddressInUse(String),
    /// The write's wait expired (05_gateway.md, "Deadlines").
    Waited(Box<WaitFailure>),
    Failed(String),
}

impl std::fmt::Display for ControlRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Superseded(reason) | Self::Failed(reason) | Self::AddressInUse(reason) => {
                formatter.write_str(reason)
            }
            Self::Waited(failure) => formatter.write_str(&failure.message),
        }
    }
}

/// The running daemon, as reconciliation needs to see it.
///
/// Everything below is a pure function of inventory and this trait's three answers, so the
/// controller never links the daemon: `cowshed-cli` implements it over the real control client,
/// and the tests implement it over a fake. `GatewayStatusError::Absent` is a distinct answer
/// rather than an error string because "no gateway is running" is a normal state with its own
/// operator remedy, not a control-plane fault; `ControlRefusal::Superseded` likewise separates a
/// write another reconcile already overtook from a write that failed.
#[async_trait]
pub trait GatewayControl: Send + Sync {
    async fn status(&self) -> std::result::Result<DaemonStatus, GatewayStatusError>;
    async fn install(&self, session: &WorkspaceSession) -> std::result::Result<(), ControlRefusal>;
    async fn remove(
        &self,
        workspace_id: &str,
        expected_revision: u64,
    ) -> std::result::Result<(), ControlRefusal>;
}

/// The composition root constructs one owned control client from validated host storage.
/// The runtime borrows it for work; construction does not supply a default or a no-op gateway.
pub type GatewayControlFactory = fn(&ValidatedHostStorage) -> Result<Box<dyn GatewayControl>>;

/// Reassign a persisted workspace port block after the gateway reports `AddrInUse`, returning
/// the new authoritative session. `rejected` includes every candidate the kernel refused in this
/// reconciliation, so the allocator cannot bounce between a pair of occupied ports.
#[async_trait]
pub trait GatewayPortReallocator: Send + Sync {
    async fn reallocate(
        &self,
        session: &WorkspaceSession,
        rejected: &BTreeSet<u16>,
    ) -> Result<WorkspaceSession>;
}

/// One inventory pass's sessions: those it serves, and the workspaces it holds. A held workspace's
/// mount is unproven (01a_apfs.md, "Ownership, identity, and the volume label"), so reconciliation
/// neither installs a session for it nor removes the one it has.
#[derive(Debug, Default)]
pub struct InventorySessions {
    pub serve: Vec<WorkspaceSession>,
    pub held: Vec<HeldWorkspace>,
}

/// Whether `workspace_id` is a session of a held workspace, at any incarnation: what is mounted
/// there is unknown, so no incarnation of it is the reconcile's to remove.
fn held_session(held: &[HeldWorkspace], workspace_id: &str) -> bool {
    held.iter().any(|workspace| {
        workspace_id.starts_with(&workspace_session_prefix(
            &workspace.repo_id,
            workspace.workspace.as_str(),
        ))
    })
}

/// One named authoritative session observation. The incarnation is retained from the host fact,
/// not reconstructed from a cache identifier, so work can fence it before any gateway mutation.
#[derive(Debug)]
pub struct WorkspaceSessionSnapshot {
    pub incarnation: WorkspaceIncarnation,
    pub session: WorkspaceSession,
}

#[async_trait]
pub trait SessionInventory {
    async fn all_sessions(&self) -> Result<InventorySessions>;
    async fn project_sessions(&self, repo_id: &RepoId) -> Result<InventorySessions>;
    async fn workspace_session(
        &self,
        repo_id: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<WorkspaceSessionSnapshot>>;
    /// The kernel claims of the store's detached blocks: a fixture store's, never the host's.
    fn fixture_port_claims(&self) -> Option<&FixturePortClaims>;
}

pub struct NativeSessionInventory {
    inventory: NativeGatewayInventory,
}

impl NativeSessionInventory {
    pub fn new(storage: ValidatedHostStorage) -> Self {
        Self::from_inventory(NativeGatewayInventory::new(storage))
    }

    pub(crate) fn from_inventory(inventory: NativeGatewayInventory) -> Self {
        Self { inventory }
    }

    /// Every attached session on the host the gateway serves, and the collisions whose newer
    /// member it does not (05_gateway.md). Each collision `reporting` names, or every one when it
    /// names no project, is said on stderr with what repairs it: a per-project reconcile reads
    /// the whole host but speaks only for its own project.
    async fn served(
        &self,
        reporting: Option<&RepoId>,
    ) -> Result<(
        Vec<GatewaySessionFact>,
        Vec<PortCollision>,
        Vec<HeldWorkspace>,
    )> {
        let AttachedInventory { attached, held } = self
            .inventory
            .all_attached()
            .await
            .map_err(inventory_error)?;
        let (served, refused) = served_sessions(attached);
        for collision in refused
            .iter()
            .filter(|collision| reporting.is_none_or(|repo| collision.claimed.repo == *repo))
        {
            cowshed_os::eprintln!(
                "cowshed: not serving {}/{}: {collision}; next: cowshed doctor --repair",
                collision.claimed.repo,
                collision.claimed.workspace
            );
        }
        Ok((served, refused, held))
    }
    pub fn with_backend(
        storage: ValidatedHostStorage,
        backend: crate::gateway_inventory::GatewayStoreBackend,
    ) -> Self {
        Self {
            inventory: NativeGatewayInventory::with_backend(storage, backend),
        }
    }
}

#[async_trait]
impl SessionInventory for NativeSessionInventory {
    fn fixture_port_claims(&self) -> Option<&FixturePortClaims> {
        self.inventory.fixture_port_claims()
    }
    async fn all_sessions(&self) -> Result<InventorySessions> {
        let (served, _, held) = self.served(None).await?;
        Ok(InventorySessions {
            serve: sessions_from_facts(served)?,
            held,
        })
    }

    /// One project's sessions, less each one an older workspace of any project holds a port of:
    /// which member of a collision is served is a host-wide answer.
    async fn project_sessions(&self, repo_id: &RepoId) -> Result<InventorySessions> {
        let project = self
            .inventory
            .project_attached(repo_id)
            .await
            .map_err(inventory_error)?;
        let (_, refused, _) = self.served(Some(repo_id)).await?;
        Ok(InventorySessions {
            serve: sessions_from_facts(project.attached.into_iter().filter(|fact| {
                !refused
                    .iter()
                    .any(|collision| collision.claims(&fact.repo_id, &fact.workspace))
            }))?,
            held: project.held,
        })
    }

    async fn workspace_session(
        &self,
        repo_id: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<WorkspaceSessionSnapshot>> {
        self.inventory
            .workspace_attached(repo_id, workspace)
            .await
            .map_err(inventory_error)?
            .map(session_snapshot_from_fact)
            .transpose()
    }
}

/// Installing into a daemon this process itself owns, as opposed to one reached over the control
/// socket. Implemented in `cowshed-cli` over the in-process `GatewayHandle`, for the same reason
/// as [`GatewayControl`]: recovery is inventory-driven and needs no runtime of its own.
#[async_trait]
pub trait GatewayInstaller: Send + Sync {
    async fn install_session(
        &self,
        session: WorkspaceSession,
    ) -> std::result::Result<(), SessionRestoreRefusal>;
}

/// Why the startup pass could not restore one session into the daemon it owns.
///
/// There is no superseded case: nothing else can install into the daemon before its startup
/// pass ends (05_gateway.md "Startup contract"), so every refusal is this session's own.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SessionRestoreRefusal {
    /// Another host process holds the session's base port. The project's next reconcile moves
    /// the workspace to a free block (05_gateway.md, port block); until then it has no gateway.
    AddressInUse(String),
    Failed(String),
}

impl std::fmt::Display for SessionRestoreRefusal {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AddressInUse(reason) => write!(formatter, "address in use: {reason}"),
            Self::Failed(reason) => formatter.write_str(reason),
        }
    }
}

/// One session the startup pass left out, named the way the operator finds it.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SessionRestoreFailure {
    pub workspace_id: String,
    pub repo_id: String,
    pub endpoint: String,
    pub refusal: SessionRestoreRefusal,
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SessionRestoreReport {
    pub restored: usize,
    pub refused: Vec<SessionRestoreFailure>,
}

/// Restore every attached workspace's session, each on its own.
///
/// A session the daemon refuses is reported and skipped: one workspace whose port another
/// process took must never cost every other workspace its gateway (05_gateway.md "Startup
/// contract": failures are per-project). Only an inventory that cannot be read at all fails the
/// pass, because then no session is known to restore.
pub async fn install_all_sessions<I, G>(inventory: &I, gateway: &G) -> Result<SessionRestoreReport>
where
    I: SessionInventory,
    G: GatewayInstaller,
{
    let sessions = inventory.all_sessions().await?.serve;
    let mut report = SessionRestoreReport::default();
    for session in sessions {
        let workspace_id = session.workspace_id.clone();
        let repo_id = session.repo_id.clone();
        let endpoint = session.endpoint.to_string();
        match gateway.install_session(session).await {
            Ok(()) => report.restored += 1,
            Err(refusal) => report.refused.push(SessionRestoreFailure {
                workspace_id,
                repo_id,
                endpoint,
                refusal,
            }),
        }
    }
    Ok(report)
}

/// Removing a fixture workspace's session unbinds its block's base until the next attach installs
/// it again, so the store claims the block first (08_testing.md): no claimant on the host that
/// binds a block's first service port before its base takes this one meanwhile. A Unix endpoint
/// has no block, and only a fixture store holds claims.
fn claim_block_before_removal<I: SessionInventory + ?Sized>(inventory: &I, endpoint: &str) {
    let Some(claims) = inventory.fixture_port_claims() else {
        return;
    };
    let Some(port) = endpoint
        .parse::<SocketAddr>()
        .ok()
        .and_then(|address| address.port().checked_add(1))
    else {
        return;
    };
    if let Err(error) = claims.claim(port) {
        cowshed_os::eprintln!(
            "cowshed: cannot claim port {port} of the fixture block at {endpoint} before removing its session: {error}; another claimant can take the block before its workspace attaches again"
        );
    }
}

/// The workspace's own session holds its block's base again: the store's claim goes.
fn release_block_claim<I: SessionInventory + ?Sized>(inventory: &I, endpoint: &WorkspaceEndpoint) {
    if let (Some(claims), WorkspaceEndpoint::Tcp { address, .. }) =
        (inventory.fixture_port_claims(), endpoint)
        && let Some(port) = address.port().checked_add(1)
    {
        claims.release(port);
    }
}

pub fn project_session_prefix(repo_id: &RepoId) -> String {
    let digest = Sha256::digest(repo_id.as_str().as_bytes());
    format!("p{}.", hex_lower(&digest[..16]))
}

/// Exact-name namespace shared only by incarnations of one workspace in one repository.
pub fn workspace_session_prefix(repo_id: &RepoId, workspace: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(repo_id.as_str().as_bytes());
    hasher.update([0]);
    hasher.update(workspace.as_bytes());
    let digest = hasher.finalize();
    format!(
        "{}w{}.",
        project_session_prefix(repo_id),
        hex_lower(&digest[..16])
    )
}

/// Stable for one workspace incarnation. Restore and re-adopt rotate the identity so the gateway's
/// replay-protection tombstone cannot reject a legitimate lifecycle reset.
pub fn stable_workspace_id(
    repo_id: &RepoId,
    workspace: &str,
    incarnation: &WorkspaceIncarnation,
) -> String {
    let prefix = workspace_session_prefix(repo_id, workspace);
    let digest = Sha256::digest(incarnation.as_str().as_bytes());
    format!("{prefix}i{}", hex_lower(&digest[..16]))
}

/// Explicit egress grants plus anonymous public npm reads on HTTPS port 443.
///
/// This is the package-registry baseline every workspace gets, including a fresh workspace
/// with no standing grants. An explicit rule matching that host and port replaces the default,
/// including an opaque rule. Private registries still require their own grants and admission.
pub fn policy_from_grants(grants: &GrantSet) -> Result<WorkspacePolicy> {
    let mut policy = WorkspacePolicy {
        grants: Vec::new(),
        mirrors: Vec::new(),
    };
    for rule in &grants.egress {
        for &port in rule.effective_ports() {
            let grant = match rule.mode {
                CoreEgressMode::Intercept => EgressGrant::intercept(&rule.host, port),
                CoreEgressMode::Opaque => EgressGrant::opaque(&rule.host, port),
            }
            .map_err(|error| {
                CowshedError::integrity(
                    format!("gateway grant for {} is invalid: {error}", rule.host),
                    "cowshed doctor --json",
                )
            })?;
            policy.grants.push(grant);
        }
    }
    let npm_host = CanonicalHost::parse("registry.npmjs.org").expect("fixed npm registry host");
    if !policy
        .grants
        .iter()
        .any(|grant| grant.port == 443 && grant.host.matches(&npm_host))
    {
        policy.grants.push(
            EgressGrant::intercept("registry.npmjs.org", 443)
                .expect("fixed public npm registry grant"),
        );
    }
    policy.validate().map_err(|error| {
        CowshedError::integrity(
            format!("gateway policy is invalid: {error}"),
            "cowshed doctor --json",
        )
    })?;
    Ok(policy)
}

/// Convert typed macOS inventory, including off-host collision fixtures (05_gateway.md).
/// IDs, token, CA and policy are validated by their constructors here. Validate the recorded
/// port-block topology, not the runner's platform; the gateway validates the current host
/// again at installation, where Linux continues to require a Unix endpoint.
pub fn session_from_fact(fact: GatewaySessionFact) -> Result<WorkspaceSession> {
    session_snapshot_from_fact(fact).map(|snapshot| snapshot.session)
}

fn session_snapshot_from_fact(fact: GatewaySessionFact) -> Result<WorkspaceSessionSnapshot> {
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
        endpoint: WorkspaceEndpoint::Tcp {
            address: endpoint,
            block_size: fact.port_block.size(),
        },
        token,
        ca,
        policy: policy_from_grants(&fact.grants)?,
    };
    session
        .endpoint
        .validate_macos_port_block()
        .map_err(|error| {
            CowshedError::integrity(
                format!(
                    "gateway session for {}/{} is invalid: {error}",
                    fact.repo_id, fact.workspace
                ),
                "cowshed doctor --json",
            )
        })?;
    Ok(WorkspaceSessionSnapshot {
        incarnation: fact.incarnation,
        session,
    })
}

pub fn sessions_from_facts(
    facts: impl IntoIterator<Item = GatewaySessionFact>,
) -> Result<Vec<WorkspaceSession>> {
    facts.into_iter().map(session_from_fact).collect()
}

/// Reconcile one project: gateway status first, then the project's inventory snapshot.
///
/// The order is the linearization. Every session status reports was installed from a snapshot its
/// reconcile read before installing, so before this status read; a snapshot read after it is at
/// least as new as every one of them, and removing on its strength never revokes a newer decision.
/// Read the other way round, a snapshot from before an attach revokes the session the attach's
/// reconcile installed, and the tombstone that removal leaves refuses the attached workspace's
/// own revision on every later reconcile until it is detached and attached again.
pub async fn reconcile_project<C, I>(
    control: &C,
    inventory: &I,
    repo_id: &RepoId,
    uid: u32,
) -> Result<ReconcileReport>
where
    C: GatewayControl + ?Sized,
    I: SessionInventory + ?Sized,
{
    reconcile_project_inner(control, inventory, repo_id, uid, None).await
}

pub async fn reconcile_project_with_reallocator<C, I>(
    control: &C,
    inventory: &I,
    repo_id: &RepoId,
    uid: u32,
    reallocator: &dyn GatewayPortReallocator,
) -> Result<ReconcileReport>
where
    C: GatewayControl + ?Sized,
    I: SessionInventory + ?Sized,
{
    reconcile_project_inner(control, inventory, repo_id, uid, Some(reallocator)).await
}

async fn reconcile_project_inner<C, I>(
    control: &C,
    inventory: &I,
    repo_id: &RepoId,
    uid: u32,
    reallocator: Option<&dyn GatewayPortReallocator>,
) -> Result<ReconcileReport>
where
    C: GatewayControl + ?Sized,
    I: SessionInventory + ?Sized,
{
    let status = reconciliation_status(control, uid).await?;
    let desired = inventory.project_sessions(repo_id).await?;
    reconcile_against_status_inner(
        control,
        inventory,
        &project_session_prefix(repo_id),
        desired,
        status,
        reallocator,
        true,
    )
    .await
}

/// Reconcile only the selected workspace's namespace, with status before its named snapshot.
/// Sibling endpoint owners remain untouched: only project maintenance can establish that a
/// foreign owner is stale, because doing so requires host-wide inventory.
/// An occupied listener is a typed conflict requiring project maintenance, never a reason for a
/// workspace job to enumerate host-wide port grants.
pub async fn reconcile_workspace<C, I>(
    control: &C,
    inventory: &I,
    repo_id: &RepoId,
    workspace: &WorkspaceName,
    incarnation: &WorkspaceIncarnation,
    uid: u32,
) -> Result<ReconcileReport>
where
    C: GatewayControl + ?Sized,
    I: SessionInventory + ?Sized,
{
    let status = reconciliation_status(control, uid).await?;
    let observed = inventory
        .workspace_session(repo_id, workspace)
        .await?
        .ok_or_else(|| {
            CowshedError::not_found(
                format!("workspace {workspace} incarnation {incarnation} has no attached gateway session"),
                "attach the selected workspace or reacquire its worker handle",
            )
        })?;
    if observed.incarnation != *incarnation {
        return Err(CowshedError::fence_refusal(
            crate::error::FenceRefusal::IncarnationMoved {
                workspace: workspace.clone(),
                observed: observed.incarnation,
            },
            format!(
                "workspace {workspace} incarnation {incarnation} changed before gateway reconciliation"
            ),
            "reacquire the worker handle and retry",
        ));
    }
    reconcile_against_status_inner(
        control,
        inventory,
        &workspace_session_prefix(repo_id, workspace.as_str()),
        InventorySessions {
            serve: vec![observed.session],
            held: Vec::new(),
        },
        status,
        None,
        false,
    )
    .await
}

async fn reconciliation_status<C: GatewayControl + ?Sized>(
    control: &C,
    uid: u32,
) -> Result<DaemonStatus> {
    let status = control.status().await.map_err(|error| match error {
        GatewayStatusError::Absent => gateway_absent(uid),
        GatewayStatusError::Waited(failure) => CowshedError::environment_missing(
            format!("gateway status failed: {}", failure.message),
            "cowshed doctor --json names the gateway and its socket",
        )
        .with_wait(Some(*failure)),
        GatewayStatusError::Control(message) => {
            CowshedError::internal(format!("gateway status failed: {message}"))
        }
    })?;
    // Startup attachments are still moving until the daemon finishes restoring its sessions.
    if let Some(heal) = status.healing {
        return Err(CowshedError::healing(heal));
    }
    Ok(status)
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
/// be installed does not abandon the rest: every failure is reported together. A write the
/// gateway refuses as superseded lost a race to a reconcile that read a newer snapshot; the newer
/// decision stands and the write is counted in `superseded`, not failed.
pub async fn reconcile_against_status<C, I>(
    control: &C,
    host: &I,
    project_prefix: &str,
    desired: InventorySessions,
    status: DaemonStatus,
) -> Result<ReconcileReport>
where
    C: GatewayControl + ?Sized,
    I: SessionInventory + ?Sized,
{
    reconcile_against_status_inner(control, host, project_prefix, desired, status, None, true).await
}

async fn reconcile_against_status_inner<C, I>(
    control: &C,
    host: &I,
    scope_prefix: &str,
    desired: InventorySessions,
    status: DaemonStatus,
    reallocator: Option<&dyn GatewayPortReallocator>,
    reconcile_foreign: bool,
) -> Result<ReconcileReport>
where
    C: GatewayControl + ?Sized,
    I: SessionInventory + ?Sized,
{
    let mut desired_by_id = BTreeMap::new();
    let InventorySessions {
        serve: desired,
        held,
    } = desired;
    for session in desired {
        let identity = session.workspace_id.clone();
        if !identity.starts_with(scope_prefix) {
            return Err(CowshedError::integrity(
                if reconcile_foreign {
                    "gateway session is outside the reconciled project namespace"
                } else {
                    "gateway session is outside the reconciled workspace namespace"
                },
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
        .filter(|session| session.workspace_id.starts_with(scope_prefix))
        .map(|session| (session.workspace_id.as_str(), session))
        .collect();
    let mut report = ReconcileReport::default();
    let desired_ids: BTreeSet<_> = desired_by_id.keys().map(String::as_str).collect();
    for installed in status
        .sessions
        .iter()
        .filter(|session| session.workspace_id.starts_with(scope_prefix))
    {
        if !desired_ids.contains(installed.workspace_id.as_str())
            && !held_session(&held, &installed.workspace_id)
        {
            claim_block_before_removal(host, &installed.endpoint);
            match control
                .remove(&installed.workspace_id, installed.revision)
                .await
            {
                Ok(()) => report.removed += 1,
                Err(ControlRefusal::Superseded(_)) => report.superseded += 1,
                Err(ControlRefusal::Waited(failure)) => {
                    return Err(CowshedError::environment_missing(
                        format!(
                            "could not remove stale gateway session {}: {}",
                            installed.workspace_id, failure.message
                        ),
                        "cowshed doctor --json names the gateway and its socket",
                    )
                    .with_wait(Some(*failure)));
                }
                Err(ControlRefusal::Failed(error) | ControlRefusal::AddressInUse(error)) => {
                    return Err(CowshedError::internal(format!(
                        "could not remove stale gateway session {}: {error}",
                        installed.workspace_id
                    )));
                }
            }
        }
    }
    let desired_endpoints: BTreeMap<String, &str> = desired_by_id
        .iter()
        .map(|(identity, session)| (session.endpoint.to_string(), identity.as_str()))
        .collect();
    let foreign_owners: Vec<_> = status
        .sessions
        .iter()
        .filter(|session| !session.workspace_id.starts_with(scope_prefix))
        .filter_map(|session| {
            desired_endpoints
                .get(&session.endpoint)
                .map(|claimant| (session, *claimant))
        })
        .collect();
    if !foreign_owners.is_empty() {
        if !reconcile_foreign {
            let (owner, claimant) = foreign_owners[0];
            return Err(CowshedError::integrity(
                format!(
                    "gateway endpoint {} for workspace {claimant} is held by session {} outside the selected workspace; project maintenance must resolve its ownership",
                    owner.endpoint, owner.workspace_id
                ),
                "cowshed doctor --repair",
            ));
        }
        let host_sessions = host.all_sessions().await?;
        let live: BTreeSet<String> = host_sessions
            .serve
            .into_iter()
            .map(|session| session.workspace_id)
            .collect();
        for (owner, claimant) in foreign_owners {
            // A held workspace's session is not stale: what its mountpoint holds is unproven.
            if live.contains(&owner.workspace_id)
                || held_session(&host_sessions.held, &owner.workspace_id)
            {
                return Err(CowshedError::integrity(
                    format!(
                        "gateway endpoint {} is assigned to workspace {claimant} by this project and still claimed by live workspace {} of another project",
                        owner.endpoint, owner.workspace_id
                    ),
                    "cowshed doctor --json",
                ));
            }
            match control.remove(&owner.workspace_id, owner.revision).await {
                Ok(()) => report.removed += 1,
                Err(ControlRefusal::Superseded(_)) => report.superseded += 1,
                Err(ControlRefusal::Waited(failure)) => {
                    return Err(CowshedError::environment_missing(
                        format!(
                            "could not remove stale gateway session {} holding endpoint {}: {}",
                            owner.workspace_id, owner.endpoint, failure.message
                        ),
                        "cowshed doctor --json names the gateway and its socket",
                    )
                    .with_wait(Some(*failure)));
                }
                Err(ControlRefusal::Failed(error) | ControlRefusal::AddressInUse(error)) => {
                    return Err(CowshedError::internal(format!(
                        "could not remove stale gateway session {} holding endpoint {}: {error}",
                        owner.workspace_id, owner.endpoint
                    )));
                }
            }
        }
    }
    let mut failures = Vec::new();
    // The first install whose wait expired: the error the failures end in carries it.
    let mut waited = None;
    for (identity, session) in &desired_by_id {
        let unchanged = installed_by_id
            .get(identity.as_str())
            .is_some_and(|installed| installed.revision == session.revision);
        if unchanged {
            release_block_claim(host, &session.endpoint);
            continue;
        }
        let mut replacement: Option<WorkspaceSession> = None;
        let mut rejected = BTreeSet::new();
        loop {
            let attempted = replacement.as_ref().unwrap_or(session);
            match control.install(attempted).await {
                Ok(()) => {
                    release_block_claim(host, &attempted.endpoint);
                    report.installed += 1;
                    break;
                }
                Err(ControlRefusal::Superseded(_)) => {
                    report.superseded += 1;
                    break;
                }
                Err(ControlRefusal::Waited(failure)) => {
                    failures.push(format!(
                        "could not install gateway session {identity}: {}",
                        failure.message
                    ));
                    waited.get_or_insert(failure);
                    break;
                }
                Err(ControlRefusal::Failed(error)) => {
                    failures.push(format!(
                        "could not install gateway session {identity}: {error}"
                    ));
                    break;
                }
                Err(ControlRefusal::AddressInUse(error)) => {
                    if !reconcile_foreign {
                        return Err(CowshedError::conflict(
                            format!(
                                "gateway session {identity} cannot bind {} (AddrInUse): {error}",
                                attempted.endpoint
                            ),
                            "cowshed doctor --repair",
                        ));
                    }
                    let WorkspaceEndpoint::Tcp { address, .. } = &attempted.endpoint else {
                        failures.push(format!(
                            "could not install gateway session {identity}: {error}"
                        ));
                        break;
                    };
                    let Some(reallocator) = reallocator else {
                        failures.push(format!(
                            "could not install gateway session {identity}: {error}"
                        ));
                        break;
                    };
                    if !rejected.insert(address.port()) {
                        failures.push(format!(
                            "could not install gateway session {identity}: repeated occupied port {}",
                            address.port()
                        ));
                        break;
                    }
                    match reallocator.reallocate(attempted, &rejected).await {
                        Ok(next)
                            if next.workspace_id == session.workspace_id
                                && next.revision > attempted.revision =>
                        {
                            replacement = Some(next);
                        }
                        Ok(_) => {
                            failures.push(format!(
                                "could not install gateway session {identity}: reallocation did not advance its authority"
                            ));
                            break;
                        }
                        Err(error) => {
                            failures.push(format!(
                                "could not reallocate occupied gateway session {identity}: {error}"
                            ));
                            break;
                        }
                    }
                }
            }
        }
    }
    if failures.is_empty() {
        Ok(report)
    } else {
        Err(CowshedError::internal(failures.join("; ")).with_wait(waited.map(|failure| *failure)))
    }
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
