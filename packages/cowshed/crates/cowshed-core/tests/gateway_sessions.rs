//! Reconcile is authority repair: the gateway's session table is rebuilt from host inventory.
//! These tests drive it with fake control and inventory so every branch — rotate, remove stale,
//! idempotent unchanged revisions, evict a deleted project's session from a claimed endpoint,
//! refuse a live collision, continue past a refused install — is pinned without a daemon.

use async_trait::async_trait;
use cowshed_core::gateway_sessions::{
    ControlRefusal, GatewayControl, GatewayInstaller, GatewayPortReallocator, GatewayStatusError,
    InventorySessions, ReconcileReport, SessionInventory, SessionRestoreRefusal,
    WorkspaceSessionSnapshot, install_all_sessions, policy_from_grants, project_session_prefix,
    reconcile_against_status, reconcile_project, reconcile_project_with_reallocator,
    reconcile_workspace, stable_workspace_id, workspace_session_prefix,
};
use cowshed_core::metadata::{
    EgressMode, EgressRule, GrantSet, WorkspaceIncarnation, WorkspaceName,
};
use cowshed_core::repository::RepoId;
use cowshed_core::{CowshedError, HeldWorkspace, Result};
use cowshed_gateway_types::{
    CanonicalTarget, DaemonStatus, EgressMode as GatewayEgressMode, SessionStatus, TargetScheme,
    WorkspaceCa, WorkspaceEndpoint, WorkspacePolicy, WorkspaceSession, WorkspaceToken,
};
use std::collections::{BTreeMap, BTreeSet};
use std::net::{Ipv4Addr, SocketAddr};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

/// Reconcile a pass that holds no workspace: `serve` is every session it found attached.
async fn reconcile_serving<C, I>(
    control: &C,
    host: &I,
    prefix: &str,
    serve: Vec<WorkspaceSession>,
    status: DaemonStatus,
) -> Result<ReconcileReport>
where
    C: GatewayControl + ?Sized,
    I: SessionInventory + ?Sized,
{
    reconcile_against_status(
        control,
        host,
        prefix,
        InventorySessions {
            serve,
            held: Vec::new(),
        },
        status,
    )
    .await
}

fn incarnation(value: u8) -> WorkspaceIncarnation {
    WorkspaceIncarnation::new(format!("{value:032x}")).expect("fixture incarnation")
}

fn session_snapshot(session: WorkspaceSession, value: u8) -> WorkspaceSessionSnapshot {
    WorkspaceSessionSnapshot {
        incarnation: incarnation(value),
        session,
    }
}

fn workspace_id(repo: &RepoId, workspace: &str, value: u8) -> String {
    stable_workspace_id(repo, workspace, &incarnation(value))
}

fn session(identity: &str, revision: u64, token_byte: u8) -> WorkspaceSession {
    session_at(identity, revision, token_byte, 40_960)
}

fn session_at(identity: &str, revision: u64, token_byte: u8, port: u16) -> WorkspaceSession {
    WorkspaceSession {
        workspace_id: identity.to_owned(),
        repo_id: "project".to_owned(),
        revision,
        endpoint: WorkspaceEndpoint::Tcp {
            address: SocketAddr::from((Ipv4Addr::LOCALHOST, port)),
            block_size: 16,
        },
        token: WorkspaceToken::from_bytes([token_byte; 32]),
        ca: WorkspaceCa::new(
            "-----BEGIN CERTIFICATE-----\npublic\n-----END CERTIFICATE-----".to_owned(),
            "-----BEGIN PRIVATE KEY-----\nprivate\n-----END PRIVATE KEY-----".to_owned(),
        )
        .expect("fixture CA"),
        policy: WorkspacePolicy::default(),
    }
}

fn status(sessions: Vec<SessionStatus>) -> DaemonStatus {
    DaemonStatus {
        version: env!("CARGO_PKG_VERSION").to_owned(),
        draining: false,
        drain_cause: None,
        executable_sha256: None,
        healing: None,
        recovering: None,
        sessions,
        active: 0,
        queued: 0,
    }
}

fn installed(identity: &str, revision: u64) -> SessionStatus {
    installed_at(identity, revision, 40_960)
}

fn installed_at(identity: &str, revision: u64, port: u16) -> SessionStatus {
    SessionStatus {
        workspace_id: identity.to_owned(),
        revision,
        endpoint: format!("127.0.0.1:{port}"),
        active: 0,
        queued: 0,
    }
}

/// A host inventory that must not be consulted: reconcile only reads it when a foreign session
/// holds an endpoint this project claims.
fn untouched_host() -> FakeInventory {
    FakeInventory {
        all: Mutex::new(None),
        held: Vec::new(),
    }
}

fn host_with(live: Vec<WorkspaceSession>) -> FakeInventory {
    host_holding(live, Vec::new())
}

/// A host inventory serving `live` and holding `held`, whose mounts are unproven.
fn host_holding(live: Vec<WorkspaceSession>, held: Vec<HeldWorkspace>) -> FakeInventory {
    FakeInventory {
        all: Mutex::new(Some(live)),
        held,
    }
}

#[derive(Default)]
struct FakeControl {
    status: Mutex<Option<std::result::Result<DaemonStatus, GatewayStatusError>>>,
    installs: Mutex<Vec<(String, u64)>>,
    removes: Mutex<Vec<(String, u64)>>,
    refuse_installs: Mutex<Vec<String>>,
    occupied_ports: Mutex<BTreeSet<u16>>,
}

#[async_trait]
impl GatewayControl for FakeControl {
    /// A test gateway never says a session is current: no exec it serves is resident.
    async fn session_current(&self, _: &str, _: u64) -> cowshed_core::Result<bool> {
        Ok(false)
    }

    async fn land_turns(
        &self,
    ) -> std::result::Result<Vec<cowshed_core::api::dto::LandTurnQueue>, GatewayStatusError> {
        Ok(Vec::new())
    }
    async fn status(&self) -> std::result::Result<DaemonStatus, GatewayStatusError> {
        self.status
            .lock()
            .expect("status lock")
            .take()
            .unwrap_or_else(|| {
                Err(GatewayStatusError::Control(
                    "status called more than once".to_owned(),
                ))
            })
    }

    async fn install(&self, session: &WorkspaceSession) -> std::result::Result<(), ControlRefusal> {
        if let WorkspaceEndpoint::Tcp { address, .. } = &session.endpoint
            && self
                .occupied_ports
                .lock()
                .expect("port lock")
                .contains(&address.port())
        {
            return Err(ControlRefusal::AddressInUse(format!(
                "port {} is occupied",
                address.port()
            )));
        }
        if self
            .refuse_installs
            .lock()
            .expect("refusal lock")
            .contains(&session.workspace_id)
        {
            return Err(ControlRefusal::Failed(
                "gateway control rejected operation (EndpointConflict)".to_owned(),
            ));
        }
        self.installs
            .lock()
            .expect("install lock")
            .push((session.workspace_id.clone(), session.revision));
        Ok(())
    }

    async fn remove(
        &self,
        workspace_id: &str,
        expected_revision: u64,
    ) -> std::result::Result<(), ControlRefusal> {
        self.removes
            .lock()
            .expect("remove lock")
            .push((workspace_id.to_owned(), expected_revision));
        Ok(())
    }
}

struct SelectedInventory {
    repo: RepoId,
    workspace: WorkspaceName,
    target: Mutex<Option<WorkspaceSessionSnapshot>>,
}

#[async_trait]
impl SessionInventory for SelectedInventory {
    fn fixture_port_claims(&self) -> Option<&cowshed_core::storage::bootstrap::FixturePortClaims> {
        None
    }
    async fn all_sessions(&self) -> Result<InventorySessions> {
        Err(CowshedError::integrity(
            "unrelated removed or malformed session grants must not be read",
            "cowshed doctor --json",
        ))
    }

    async fn project_sessions(&self, _: &RepoId) -> Result<InventorySessions> {
        Err(CowshedError::integrity(
            "unrelated removed or malformed session grants must not be read",
            "cowshed doctor --json",
        ))
    }

    async fn workspace_session(
        &self,
        repo: &RepoId,
        workspace: &WorkspaceName,
    ) -> Result<Option<WorkspaceSessionSnapshot>> {
        assert_eq!(repo, &self.repo);
        assert_eq!(workspace, &self.workspace);
        Ok(self.target.lock().expect("selected session").take())
    }
}

#[test]
fn stable_workspace_namespace_fences_repository_name_and_incarnation() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let other = RepoId::parse("other/widget").expect("other repo");
    let prefix = workspace_session_prefix(&repo, "raven");
    let current = workspace_id(&repo, "raven", 2);
    assert!(current.starts_with(&prefix));
    assert!(workspace_id(&repo, "raven", 1).starts_with(&prefix));
    assert_ne!(current, workspace_id(&repo, "raven", 1));
    assert!(!workspace_id(&repo, "raven-more", 2).starts_with(&prefix));
    assert!(!workspace_id(&other, "raven", 2).starts_with(&prefix));
    assert!(current.starts_with(&project_session_prefix(&repo)));
    cowshed_gateway_types::validate_identifier("workspace_id", &current).expect("valid identity");
}

#[tokio::test]
async fn workspace_reconcile_ignores_unrelated_grants_and_preserves_sibling_sessions() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let target = workspace_id(&repo, "raven", 2);
    let prior = workspace_id(&repo, "raven", 1);
    let sibling = workspace_id(&repo, "raven-more", 1);
    let removed = workspace_id(&repo, "retired", 1);
    let control = FakeControl {
        status: Mutex::new(Some(Ok(status(vec![
            installed(&prior, 7),
            installed_at(&sibling, 9, 40_976),
            installed_at(&removed, 10, 40_992),
        ])))),
        ..FakeControl::default()
    };
    let inventory = SelectedInventory {
        repo: repo.clone(),
        workspace: WorkspaceName::new("raven").expect("workspace"),
        target: Mutex::new(Some(session_snapshot(session(&target, 8, 1), 2))),
    };
    let report = reconcile_workspace(
        &control,
        &inventory,
        &repo,
        &inventory.workspace,
        &incarnation(2),
        501,
    )
    .await
    .expect("only the selected grant is consulted");
    assert_eq!(report.installed, 1);
    assert_eq!(report.removed, 1);
    assert_eq!(*control.removes.lock().expect("removes"), vec![(prior, 7)]);
    assert_eq!(
        *control.installs.lock().expect("installs"),
        vec![(target, 8)]
    );
}

#[tokio::test]
async fn absent_workspace_work_refuses_and_maintenance_retires_only_stale_sessions() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let target = workspace_id(&repo, "raven", 1);
    let sibling = workspace_id(&repo, "other", 1);
    let control = FakeControl {
        status: Mutex::new(Some(Ok(status(vec![
            installed(&target, 7),
            installed_at(&sibling, 9, 40_976),
        ])))),
        ..FakeControl::default()
    };
    let inventory = SelectedInventory {
        repo: repo.clone(),
        workspace: WorkspaceName::new("raven").expect("workspace"),
        target: Mutex::new(None),
    };
    let error = reconcile_workspace(
        &control,
        &inventory,
        &repo,
        &inventory.workspace,
        &incarnation(1),
        501,
    )
    .await
    .expect_err("a retired work capability has no cleanup authority");
    assert_eq!(error.code, cowshed_core::ErrorCode::NotFound);
    assert!(control.removes.lock().expect("removes").is_empty());
    assert!(control.installs.lock().expect("installs").is_empty());

    let maintenance = FakeControl {
        status: Mutex::new(Some(Ok(status(vec![
            installed(&target, 7),
            installed_at(&sibling, 9, 40_976),
        ])))),
        ..FakeControl::default()
    };
    let report = reconcile_project(
        &maintenance,
        &host_with(vec![session_at(&sibling, 9, 2, 40_976)]),
        &repo,
        501,
    )
    .await
    .expect("project maintenance retires the stale session");
    assert_eq!(report.removed, 1);
    assert_eq!(
        *maintenance.removes.lock().expect("removes"),
        vec![(target, 7)]
    );
    assert!(maintenance.installs.lock().expect("installs").is_empty());
}

#[tokio::test]
async fn workspace_reconcile_never_evicts_a_sibling_endpoint_owner() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let other = RepoId::parse("other/widget").expect("other repo");
    for owner in [
        workspace_id(&repo, "other", 1),
        workspace_id(&other, "raven", 1),
    ] {
        let control = FakeControl {
            status: Mutex::new(Some(Ok(status(vec![installed(&owner, 7)])))),
            ..FakeControl::default()
        };
        let inventory = SelectedInventory {
            repo: repo.clone(),
            workspace: WorkspaceName::new("raven").expect("workspace"),
            target: Mutex::new(Some(session_snapshot(
                session(&workspace_id(&repo, "raven", 1), 8, 1),
                1,
            ))),
        };
        let error = reconcile_workspace(
            &control,
            &inventory,
            &repo,
            &inventory.workspace,
            &incarnation(1),
            501,
        )
        .await
        .expect_err("a workspace job cannot decide another session's ownership");
        assert_eq!(error.code, cowshed_core::ErrorCode::Integrity);
        assert!(error.message.contains(&owner));
        assert!(control.removes.lock().expect("removes").is_empty());
        assert!(control.installs.lock().expect("installs").is_empty());
    }
}

#[tokio::test]
async fn recreated_workspace_is_refused_before_any_gateway_mutation() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let workspace = WorkspaceName::new("raven").expect("workspace");
    let prior = workspace_id(&repo, workspace.as_str(), 1);
    let current = workspace_id(&repo, workspace.as_str(), 2);
    let control = FakeControl {
        status: Mutex::new(Some(Ok(status(vec![installed(&prior, 7)])))),
        ..FakeControl::default()
    };
    let inventory = SelectedInventory {
        repo: repo.clone(),
        workspace: workspace.clone(),
        target: Mutex::new(Some(session_snapshot(session(&current, 8, 1), 2))),
    };
    let error = reconcile_workspace(
        &control,
        &inventory,
        &repo,
        &workspace,
        &incarnation(1),
        501,
    )
    .await
    .expect_err("a held old incarnation cannot reconcile a recreated name");
    assert_eq!(error.code, cowshed_core::ErrorCode::Conflict);
    assert!(
        matches!(error.fence_source(), Some(cowshed_core::error::FenceRefusal::IncarnationMoved { workspace: found, observed }) if found == &workspace && observed == &incarnation(2))
    );
    assert!(control.installs.lock().expect("installs").is_empty());
    assert!(control.removes.lock().expect("removes").is_empty());
}

#[derive(Default)]
struct RecordingReallocator {
    rejected: Mutex<Vec<BTreeSet<u16>>>,
}

#[async_trait]
impl GatewayPortReallocator for RecordingReallocator {
    async fn reallocate(
        &self,
        session: &WorkspaceSession,
        rejected: &BTreeSet<u16>,
    ) -> Result<WorkspaceSession> {
        self.rejected
            .lock()
            .expect("rejected")
            .push(rejected.clone());
        let WorkspaceEndpoint::Tcp { address, .. } = &session.endpoint else {
            panic!("the fixture has a TCP endpoint");
        };
        Ok(session_at(
            &session.workspace_id,
            session.revision + 1,
            1,
            address.port() + 16,
        ))
    }
}

#[tokio::test]
async fn project_reallocation_runs_only_after_address_in_use_and_retains_rejected_ports() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let identity = workspace_id(&repo, "raven", 1);
    let inventory = AttachingInventory {
        status_read: Arc::new(AtomicBool::new(true)),
        attached: identity.clone(),
    };
    for occupied in [BTreeSet::new(), BTreeSet::from([40_960, 40_976])] {
        let attempts = occupied.len();
        let control = FakeControl {
            status: Mutex::new(Some(Ok(status(Vec::new())))),
            occupied_ports: Mutex::new(occupied),
            ..FakeControl::default()
        };
        let reallocator = RecordingReallocator::default();
        let report =
            reconcile_project_with_reallocator(&control, &inventory, &repo, 501, &reallocator)
                .await
                .expect("project port publication succeeds");
        assert_eq!(report.installed, 1);
        assert_eq!(
            *control.installs.lock().expect("installs"),
            vec![(
                identity.clone(),
                6 + u64::try_from(attempts).expect("small attempt count")
            )]
        );
        let rejected = reallocator.rejected.lock().expect("rejected");
        assert_eq!(rejected.len(), attempts);
        if attempts != 0 {
            assert_eq!(rejected[0], BTreeSet::from([40_960]));
            assert_eq!(rejected[1], BTreeSet::from([40_960, 40_976]));
        }
    }
}

#[tokio::test]
async fn workspace_address_in_use_is_a_typed_refusal_without_host_allocation() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let identity = workspace_id(&repo, "raven", 1);
    let inventory = SelectedInventory {
        repo: repo.clone(),
        workspace: WorkspaceName::new("raven").expect("workspace"),
        target: Mutex::new(Some(session_snapshot(session(&identity, 8, 1), 1))),
    };
    let control = FakeControl {
        status: Mutex::new(Some(Ok(status(Vec::new())))),
        occupied_ports: Mutex::new(BTreeSet::from([40_960])),
        ..FakeControl::default()
    };
    let error = reconcile_workspace(
        &control,
        &inventory,
        &repo,
        &inventory.workspace,
        &incarnation(1),
        501,
    )
    .await
    .expect_err("jobs cannot enter host-wide allocation even after AddrInUse");
    assert_eq!(error.code, cowshed_core::ErrorCode::Conflict);
    assert!(error.message.contains("AddrInUse"));
    assert!(error.message.contains(&identity));
    assert!(error.message.contains("127.0.0.1:40960"));
    assert_eq!(error.hint, "cowshed doctor --repair");
    assert!(control.removes.lock().expect("removes").is_empty());
    assert!(control.installs.lock().expect("installs").is_empty());
}

#[tokio::test]
async fn reconcile_rotates_workspace_incarnation_and_removes_stale_project_sessions() {
    let repo_a = RepoId::parse("acme/widget").expect("repo A");
    let repo_b = RepoId::parse("other/widget").expect("repo B");
    let prefix_a = project_session_prefix(&repo_a);
    let current = workspace_id(&repo_a, "raven", 2);
    let prior = workspace_id(&repo_a, "raven", 1);
    let stale = workspace_id(&repo_a, "retired", 3);
    let sibling = workspace_id(&repo_b, "raven", 1);
    assert_ne!(current, prior);
    let control = FakeControl::default();

    let report = reconcile_serving(
        &control,
        &untouched_host(),
        &prefix_a,
        vec![session(&current, 8, 9)],
        status(vec![
            installed(&prior, 7),
            installed(&stale, 3),
            installed_at(&sibling, 5, 40_976),
        ]),
    )
    .await
    .expect("reconcile succeeds");

    assert_eq!(report.installed, 1);
    assert_eq!(report.removed, 2);
    assert_eq!(
        *control.installs.lock().expect("install lock"),
        vec![(current, 8)]
    );
    assert_eq!(
        *control.removes.lock().expect("remove lock"),
        vec![(prior, 7), (stale, 3)]
    );
}

#[tokio::test]
async fn empty_attached_inventory_removes_detached_session() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let identity = workspace_id(&repo, "raven", 1);
    let control = FakeControl::default();
    let report = reconcile_serving(
        &control,
        &untouched_host(),
        &project_session_prefix(&repo),
        Vec::new(),
        status(vec![installed(&identity, 11)]),
    )
    .await
    .expect("reconcile succeeds");
    assert_eq!(report.installed, 0);
    assert_eq!(report.removed, 1);
    assert_eq!(
        *control.removes.lock().expect("remove lock"),
        vec![(identity, 11)]
    );
}

/// A workspace whose mount is unproven is held (01a_apfs.md, "Ownership, identity, and the
/// volume label"): the reconcile neither removes its installed session, at any incarnation, nor
/// installs one, and still removes a stale sibling's session.
#[tokio::test]
async fn reconcile_neither_installs_nor_removes_a_held_workspace_session() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let held = workspace_id(&repo, "ci-fhs-tools", 1);
    let held_prior = workspace_id(&repo, "ci-fhs-tools", 2);
    let stale = workspace_id(&repo, "retired", 3);
    let control = FakeControl::default();
    let report = reconcile_against_status(
        &control,
        &untouched_host(),
        &project_session_prefix(&repo),
        InventorySessions {
            serve: Vec::new(),
            held: vec![HeldWorkspace {
                repo_id: repo.clone(),
                workspace: WorkspaceName::new("ci-fhs-tools").expect("workspace"),
            }],
        },
        status(vec![
            installed(&held, 11),
            installed_at(&held_prior, 9, 40_976),
            installed_at(&stale, 3, 40_992),
        ]),
    )
    .await
    .expect("a held workspace fails nothing");
    assert_eq!(report.installed, 0);
    assert_eq!(report.removed, 1);
    assert_eq!(
        *control.removes.lock().expect("remove lock"),
        vec![(stale, 3)]
    );
    assert!(control.installs.lock().expect("install lock").is_empty());
}

#[tokio::test]
async fn unchanged_revision_is_idempotent() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let identity = workspace_id(&repo, "raven", 1);
    let control = FakeControl::default();
    let report = reconcile_serving(
        &control,
        &untouched_host(),
        &project_session_prefix(&repo),
        vec![session(&identity, 4, 1)],
        status(vec![installed(&identity, 4)]),
    )
    .await
    .expect("reconcile succeeds");
    assert_eq!(report.installed, 0);
    assert_eq!(report.removed, 0);
    assert!(control.installs.lock().expect("install lock").is_empty());
    assert!(control.removes.lock().expect("remove lock").is_empty());
}

#[tokio::test]
async fn absent_gateway_is_exit_five_and_guides_the_install() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let control = FakeControl {
        status: Mutex::new(Some(Err(GatewayStatusError::Absent))),
        ..FakeControl::default()
    };
    let error = reconcile_project(&control, &untouched_host(), &repo, 501)
        .await
        .expect_err("gateway absence fails");
    assert_eq!(error.exit_code(), 5);
    // `cowshed gateway start` installs the launch agent as well as starting it,
    // so it is correct on a host where the agent was never installed — which is
    // where this error is reached from first. `launchctl kickstart` fails there
    // with "service not found".
    assert_eq!(error.hint, "cowshed gateway start");
}

/// A gateway still mounting the projects it restores sessions from answers status, and every
/// command that needs those sessions is refused by type, with how far the pass has got and the
/// command that reports it — never as an absent gateway, and before it touches the inventory or
/// changes a session.
#[tokio::test]
async fn a_gateway_still_healing_refuses_reconcile_by_type() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let mounting = cowshed_gateway_types::StartupHeal::Mounting {
        projects: std::num::NonZeroUsize::new(4).expect("four"),
    };
    let control = FakeControl {
        status: Mutex::new(Some(Ok(DaemonStatus {
            healing: Some(mounting),
            ..status(Vec::new())
        }))),
        ..FakeControl::default()
    };
    let error = reconcile_project(&control, &untouched_host(), &repo, 501)
        .await
        .expect_err("a healing gateway refuses");
    assert_eq!(error.healing_source(), Some(mounting));
    assert_eq!(error.exit_code(), 4, "{}", error.message);
    assert!(
        error.message.contains("mounting 4 adopted projects"),
        "{}",
        error.message
    );
    assert!(
        error.hint.contains("cowshed gateway status"),
        "{}",
        error.hint
    );
    assert!(control.installs.lock().expect("install lock").is_empty());
    assert!(control.removes.lock().expect("remove lock").is_empty());
}

#[tokio::test]
async fn status_protocol_failures_are_not_reported_as_gateway_absence() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let control = FakeControl {
        status: Mutex::new(Some(Err(GatewayStatusError::Control(
            "gateway control response is invalid".to_owned(),
        )))),
        ..FakeControl::default()
    };

    let error = reconcile_project(&control, &untouched_host(), &repo, 501)
        .await
        .expect_err("a malformed response must remain distinguishable from an absent socket");

    assert_eq!(error.exit_code(), 1);
    assert!(
        error
            .message
            .contains("gateway control response is invalid"),
        "{}",
        error.message
    );
}

#[tokio::test]
async fn workspace_status_refusals_precede_any_named_inventory_read() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let workspace = WorkspaceName::new("raven").expect("workspace");
    let mounting = cowshed_gateway_types::StartupHeal::Mounting {
        projects: std::num::NonZeroUsize::new(4).expect("four"),
    };
    for (response, exit) in [
        (Err(GatewayStatusError::Absent), 5),
        (
            Err(GatewayStatusError::Control("invalid status".to_owned())),
            1,
        ),
        (
            Ok(DaemonStatus {
                healing: Some(mounting),
                ..status(Vec::new())
            }),
            4,
        ),
    ] {
        let control = FakeControl {
            status: Mutex::new(Some(response)),
            ..FakeControl::default()
        };
        let error = reconcile_workspace(
            &control,
            &untouched_host(),
            &repo,
            &workspace,
            &incarnation(1),
            501,
        )
        .await
        .expect_err("status refuses before any inventory method is reached");
        assert_eq!(error.exit_code(), exit);
        assert!(control.removes.lock().expect("removes").is_empty());
        assert!(control.installs.lock().expect("installs").is_empty());
    }
}

#[tokio::test]
async fn stale_foreign_session_holding_a_claimed_endpoint_is_evicted_before_install() {
    // `local/diag` was deleted out of band; its gateway session kept port block 41536, which the
    // host-global allocator then handed to this project's `lock-contracts`. Every reconcile hit
    // EndpointConflict on that install and abandoned the rest of the project.
    let repo = RepoId::parse("example-org/example-app").expect("repo");
    let deleted = RepoId::parse("local/diag").expect("deleted repo");
    let prefix = project_session_prefix(&repo);
    let claimant = workspace_id(&repo, "lock-contracts", 1);
    let sibling = workspace_id(&repo, "ring-path", 1);
    let leaked = workspace_id(&deleted, "ws", 1);
    let control = FakeControl::default();

    let report = reconcile_serving(
        &control,
        &host_with(vec![
            session_at(&claimant, 2, 1, 41_536),
            session_at(&sibling, 2, 2, 41_088),
        ]),
        &prefix,
        vec![
            session_at(&claimant, 2, 1, 41_536),
            session_at(&sibling, 2, 2, 41_088),
        ],
        status(vec![
            installed_at(&leaked, 1, 41_536),
            installed_at(&sibling, 2, 41_088),
        ]),
    )
    .await
    .expect("reconcile succeeds");

    assert_eq!(report.removed, 1);
    assert_eq!(report.installed, 1);
    assert_eq!(
        *control.removes.lock().expect("remove lock"),
        vec![(leaked, 1)]
    );
    assert_eq!(
        *control.installs.lock().expect("install lock"),
        vec![(claimant, 2)]
    );
}

/// A foreign session whose workspace the host inventory holds, its mount unproven, is not a
/// stale one: the endpoint conflict is reported and the session is never evicted.
#[tokio::test]
async fn held_foreign_session_on_a_claimed_endpoint_is_an_inventory_fault_not_an_eviction() {
    let repo = RepoId::parse("example-org/example-app").expect("repo");
    let other = RepoId::parse("example-org/other-app").expect("other repo");
    let claimant = workspace_id(&repo, "lock-contracts", 1);
    let held_owner = workspace_id(&other, "board-wave", 1);
    let control = FakeControl::default();

    let error = reconcile_serving(
        &control,
        &host_holding(
            vec![session_at(&claimant, 2, 1, 41_536)],
            vec![HeldWorkspace {
                repo_id: other.clone(),
                workspace: WorkspaceName::new("board-wave").expect("workspace"),
            }],
        ),
        &project_session_prefix(&repo),
        vec![session_at(&claimant, 2, 1, 41_536)],
        status(vec![installed_at(&held_owner, 3, 41_536)]),
    )
    .await
    .expect_err("a held owner of a claimed endpoint is an inventory fault");

    assert!(error.message.contains(&held_owner), "{}", error.message);
    assert!(control.removes.lock().expect("remove lock").is_empty());
    assert!(control.installs.lock().expect("install lock").is_empty());
}

#[tokio::test]
async fn live_foreign_session_on_a_claimed_endpoint_is_an_inventory_fault_not_an_eviction() {
    let repo = RepoId::parse("example-org/example-app").expect("repo");
    let other = RepoId::parse("example-org/other-app").expect("other repo");
    let claimant = workspace_id(&repo, "lock-contracts", 1);
    let live_owner = workspace_id(&other, "board-wave", 1);
    let control = FakeControl::default();

    let error = reconcile_serving(
        &control,
        &host_with(vec![
            session_at(&claimant, 2, 1, 41_536),
            session_at(&live_owner, 3, 2, 41_536),
        ]),
        &project_session_prefix(&repo),
        vec![session_at(&claimant, 2, 1, 41_536)],
        status(vec![installed_at(&live_owner, 3, 41_536)]),
    )
    .await
    .expect_err("two live workspaces on one port block is an inventory fault");

    assert_eq!(error.hint, "cowshed doctor --json");
    assert!(
        error.message.contains("127.0.0.1:41536"),
        "{}",
        error.message
    );
    assert!(error.message.contains(&live_owner), "{}", error.message);
    assert!(control.removes.lock().expect("remove lock").is_empty());
    assert!(control.installs.lock().expect("install lock").is_empty());
}

#[tokio::test]
async fn one_refused_install_does_not_abandon_the_other_workspaces() {
    let repo = RepoId::parse("example-org/example-app").expect("repo");
    let refused = workspace_id(&repo, "lock-contracts", 1);
    let first = workspace_id(&repo, "abi-reconcile", 1);
    let last = workspace_id(&repo, "ttsc-gate", 1);
    let control = FakeControl {
        refuse_installs: Mutex::new(vec![refused.clone()]),
        ..FakeControl::default()
    };
    let mut desired = vec![
        session_at(&first, 2, 1, 41_296),
        session_at(&refused, 2, 2, 41_536),
        session_at(&last, 2, 3, 41_264),
    ];
    desired.sort_by(|left, right| left.workspace_id.cmp(&right.workspace_id));

    let error = reconcile_serving(
        &control,
        &untouched_host(),
        &project_session_prefix(&repo),
        desired,
        status(Vec::new()),
    )
    .await
    .expect_err("the refused install is still reported");

    assert!(
        error
            .message
            .contains(&format!("could not install gateway session {refused}")),
        "{}",
        error.message
    );
    let mut installed: Vec<_> = control
        .installs
        .lock()
        .expect("install lock")
        .iter()
        .map(|(identity, _)| identity.clone())
        .collect();
    installed.sort();
    let mut expected = vec![first, last];
    expected.sort();
    assert_eq!(installed, expected);
}

/// The daemon's session table and revision fence as it enforces them: an install must exceed every
/// revision the workspace has ever held (a removal leaves that revision as a tombstone), and a
/// removal must name the installed revision. `concurrent` is another reconciler's install that
/// lands after this reconcile read status and before its first write.
#[derive(Default)]
struct FencedGateway {
    live: Mutex<BTreeMap<String, u64>>,
    high_water: Mutex<BTreeMap<String, u64>>,
    status_read: Arc<AtomicBool>,
    concurrent: Mutex<Option<(String, u64)>>,
}

impl FencedGateway {
    fn holding(identity: &str, revision: u64) -> Self {
        let gateway = Self::default();
        gateway
            .admit(identity, revision)
            .expect("seed installed session");
        gateway
    }

    fn admit(&self, identity: &str, revision: u64) -> std::result::Result<(), ControlRefusal> {
        let mut high_water = self.high_water.lock().expect("fence lock");
        if high_water
            .get(identity)
            .is_some_and(|fence| revision <= *fence)
        {
            return Err(ControlRefusal::Superseded(
                "gateway control rejected operation (RevisionFence): workspace session revision did not advance"
                    .to_owned(),
            ));
        }
        high_water.insert(identity.to_owned(), revision);
        self.live
            .lock()
            .expect("table lock")
            .insert(identity.to_owned(), revision);
        Ok(())
    }

    fn land_concurrent_write(&self) {
        if let Some((identity, revision)) = self.concurrent.lock().expect("script lock").take() {
            self.admit(&identity, revision)
                .expect("the concurrent reconciler's install is newer");
        }
    }

    fn live(&self) -> BTreeMap<String, u64> {
        self.live.lock().expect("table lock").clone()
    }
}

#[async_trait]
impl GatewayControl for FencedGateway {
    /// A test gateway never says a session is current: no exec it serves is resident.
    async fn session_current(&self, _: &str, _: u64) -> cowshed_core::Result<bool> {
        Ok(false)
    }

    async fn land_turns(
        &self,
    ) -> std::result::Result<Vec<cowshed_core::api::dto::LandTurnQueue>, GatewayStatusError> {
        Ok(Vec::new())
    }
    async fn status(&self) -> std::result::Result<DaemonStatus, GatewayStatusError> {
        self.status_read.store(true, Ordering::SeqCst);
        Ok(status(
            self.live()
                .into_iter()
                .map(|(identity, revision)| installed(&identity, revision))
                .collect(),
        ))
    }

    async fn install(&self, session: &WorkspaceSession) -> std::result::Result<(), ControlRefusal> {
        self.land_concurrent_write();
        self.admit(&session.workspace_id, session.revision)
    }

    async fn remove(
        &self,
        workspace_id: &str,
        expected_revision: u64,
    ) -> std::result::Result<(), ControlRefusal> {
        self.land_concurrent_write();
        let mut live = self.live.lock().expect("table lock");
        match live.get(workspace_id) {
            Some(&revision) if revision == expected_revision => {
                live.remove(workspace_id);
                Ok(())
            }
            Some(&revision) => Err(ControlRefusal::Superseded(format!(
                "gateway control rejected operation (RevisionFence): workspace session revision mismatch: expected {expected_revision}, got {revision}"
            ))),
            None => Err(ControlRefusal::Superseded(
                "gateway control rejected operation (NotInstalled): workspace is not installed"
                    .to_owned(),
            )),
        }
    }
}

/// Answers from the host store as it stands when asked: `raven` is detached until the moment the
/// reconcile reads gateway status, and attached at revision 6 from then on — the attach, and the
/// install of revision 6 by the reconcile that followed it, happened in between.
struct AttachingInventory {
    status_read: Arc<AtomicBool>,
    attached: String,
}

#[async_trait]
impl SessionInventory for AttachingInventory {
    fn fixture_port_claims(&self) -> Option<&cowshed_core::storage::bootstrap::FixturePortClaims> {
        None
    }
    async fn all_sessions(&self) -> Result<InventorySessions> {
        Err(CowshedError::internal("host inventory not expected"))
    }

    async fn project_sessions(&self, _repo_id: &RepoId) -> Result<InventorySessions> {
        Ok(InventorySessions {
            serve: if self.status_read.load(Ordering::SeqCst) {
                vec![session(&self.attached, 6, 1)]
            } else {
                Vec::new()
            },
            held: Vec::new(),
        })
    }

    async fn workspace_session(
        &self,
        _: &RepoId,
        _: &WorkspaceName,
    ) -> Result<Option<WorkspaceSessionSnapshot>> {
        Ok(self
            .status_read
            .load(Ordering::SeqCst)
            .then(|| session_snapshot(session(&self.attached, 6, 1), 1)))
    }
}

/// Every session a reconcile sees in gateway status was installed from an inventory snapshot older
/// than its own, so it may remove a session only on the strength of a snapshot taken after that
/// status. Reading the snapshot first lets a view from before an attach revoke the session the
/// attach installed, and the tombstone that removal leaves then refuses the attached workspace's
/// own revision on every later reconcile: `RevisionFence` on a healthy gateway until the workspace
/// is detached and attached again.
#[tokio::test]
async fn a_reconcile_never_revokes_a_session_installed_after_its_inventory_snapshot() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let raven = workspace_id(&repo, "raven", 1);
    let gateway = FencedGateway::holding(&raven, 6);
    let inventory = AttachingInventory {
        status_read: Arc::clone(&gateway.status_read),
        attached: raven.clone(),
    };

    reconcile_project(&gateway, &inventory, &repo, 501)
        .await
        .expect("first reconcile");
    reconcile_project(&gateway, &inventory, &repo, 501)
        .await
        .expect("the attached workspace's own revision stays installable");

    assert_eq!(gateway.live(), BTreeMap::from([(raven, 6)]));
}

#[tokio::test]
async fn workspace_reconcile_reads_status_before_its_named_snapshot() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let workspace = WorkspaceName::new("raven").expect("workspace");
    let identity = workspace_id(&repo, workspace.as_str(), 1);
    let gateway = FencedGateway::holding(&identity, 6);
    let inventory = AttachingInventory {
        status_read: Arc::clone(&gateway.status_read),
        attached: identity.clone(),
    };
    reconcile_workspace(
        &gateway,
        &inventory,
        &repo,
        &workspace,
        &incarnation(1),
        501,
    )
    .await
    .expect("first workspace reconcile");
    reconcile_workspace(
        &gateway,
        &inventory,
        &repo,
        &workspace,
        &incarnation(1),
        501,
    )
    .await
    .expect("the current revision was not tombstoned");
    assert_eq!(gateway.live(), BTreeMap::from([(identity, 6)]));
}

#[tokio::test]
async fn workspace_reconcile_preserves_superseded_install_and_removal_fences() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let workspace = WorkspaceName::new("raven").expect("workspace");
    let identity = workspace_id(&repo, workspace.as_str(), 1);
    for removing in [false, true] {
        let gateway = FencedGateway {
            concurrent: Mutex::new(Some((identity.clone(), 7))),
            ..if removing {
                FencedGateway::holding(&identity, 6)
            } else {
                FencedGateway::default()
            }
        };
        let report = if removing {
            reconcile_project(&gateway, &host_with(Vec::new()), &repo, 501).await
        } else {
            let inventory = SelectedInventory {
                repo: repo.clone(),
                workspace: workspace.clone(),
                target: Mutex::new(Some(session_snapshot(session(&identity, 6, 1), 1))),
            };
            reconcile_workspace(
                &gateway,
                &inventory,
                &repo,
                &workspace,
                &incarnation(1),
                501,
            )
            .await
        }
        .expect("the newer decision stands");
        assert_eq!(report.superseded, 1);
        assert_eq!(report.installed, 0);
        assert_eq!(report.removed, 0);
        assert_eq!(gateway.live(), BTreeMap::from([(identity.clone(), 7)]));
    }
}

/// A reconcile's writes race every other reconcile of the project: one that read a newer snapshot
/// can install a newer revision after this one read status. The gateway refusing the older
/// revision is that newer decision standing, not a failure of the command that happened to lose
/// the race — `land --check`'s preflight must not fail on another workspace's attach.
#[tokio::test]
async fn an_install_superseded_by_a_newer_reconcile_is_reported_not_failed() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let raven = workspace_id(&repo, "raven", 1);
    let gateway = FencedGateway {
        concurrent: Mutex::new(Some((raven.clone(), 7))),
        ..FencedGateway::default()
    };

    let report = reconcile_serving(
        &gateway,
        &untouched_host(),
        &project_session_prefix(&repo),
        vec![session(&raven, 6, 1)],
        status(Vec::new()),
    )
    .await
    .expect("a superseded install is not a reconcile failure");

    assert_eq!(report.installed, 0);
    assert_eq!(report.superseded, 1);
    assert_eq!(gateway.live(), BTreeMap::from([(raven, 7)]));
}

#[tokio::test]
async fn a_removal_superseded_by_a_newer_reconcile_is_reported_not_failed() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let raven = workspace_id(&repo, "raven", 1);
    let gateway = FencedGateway {
        concurrent: Mutex::new(Some((raven.clone(), 7))),
        ..FencedGateway::holding(&raven, 6)
    };

    let report = reconcile_serving(
        &gateway,
        &untouched_host(),
        &project_session_prefix(&repo),
        Vec::new(),
        status(vec![installed(&raven, 6)]),
    )
    .await
    .expect("a superseded removal is not a reconcile failure");

    assert_eq!(report.removed, 0);
    assert_eq!(report.superseded, 1);
    assert_eq!(gateway.live(), BTreeMap::from([(raven, 7)]));
}

struct FakeInventory {
    all: Mutex<Option<Vec<WorkspaceSession>>>,
    held: Vec<HeldWorkspace>,
}

#[async_trait]
impl SessionInventory for FakeInventory {
    fn fixture_port_claims(&self) -> Option<&cowshed_core::storage::bootstrap::FixturePortClaims> {
        None
    }
    async fn all_sessions(&self) -> Result<InventorySessions> {
        self.all
            .lock()
            .expect("inventory lock")
            .take()
            .map(|serve| InventorySessions {
                serve,
                held: self.held.clone(),
            })
            .ok_or_else(|| CowshedError::internal("inventory called twice"))
    }

    async fn project_sessions(&self, repo_id: &RepoId) -> Result<InventorySessions> {
        let prefix = project_session_prefix(repo_id);
        Ok(InventorySessions {
            serve: self
                .all_sessions()
                .await?
                .serve
                .into_iter()
                .filter(|session| session.workspace_id.starts_with(&prefix))
                .collect(),
            held: Vec::new(),
        })
    }

    async fn workspace_session(
        &self,
        _: &RepoId,
        _: &WorkspaceName,
    ) -> Result<Option<WorkspaceSessionSnapshot>> {
        Err(CowshedError::internal("workspace inventory not expected"))
    }
}

#[derive(Default)]
struct FakeInstaller {
    installed: Mutex<Vec<(String, u64)>>,
    /// Sessions whose base port another host process holds.
    occupied: Vec<String>,
}

#[async_trait]
impl GatewayInstaller for FakeInstaller {
    async fn install_session(
        &self,
        session: WorkspaceSession,
    ) -> std::result::Result<(), SessionRestoreRefusal> {
        if self.occupied.contains(&session.workspace_id) {
            return Err(SessionRestoreRefusal::AddressInUse(String::from(
                "Address already in use (os error 48)",
            )));
        }
        self.installed
            .lock()
            .expect("installer lock")
            .push((session.workspace_id, session.revision));
        Ok(())
    }
}

#[tokio::test]
async fn daemon_restart_restores_every_project_inventory_session() {
    let repo_a = RepoId::parse("acme/widget").expect("repo A");
    let repo_b = RepoId::parse("other/tool").expect("repo B");
    let a = workspace_id(&repo_a, "main", 1);
    let b = workspace_id(&repo_b, "raven", 2);
    let inventory = host_with(vec![session(&a, 2, 1), session(&b, 7, 2)]);
    let gateway = FakeInstaller::default();
    let report = install_all_sessions(&inventory, &gateway)
        .await
        .expect("recovery succeeds");
    assert_eq!(report.restored, 2);
    assert!(report.refused.is_empty());
    assert_eq!(
        *gateway.installed.lock().expect("installer lock"),
        vec![(a, 2), (b, 7)]
    );
}

/// A test fixture or any other host process holding one session's base port once took the whole
/// daemon down at startup, and launchd restarted it into the same refusal. The refused session is
/// reported with its typed reason and every other session is still restored.
#[tokio::test]
async fn one_occupied_session_port_does_not_stop_the_startup_restore() {
    let repo_a = RepoId::parse("acme/widget").expect("repo A");
    let repo_b = RepoId::parse("other/tool").expect("repo B");
    let a = workspace_id(&repo_a, "main", 1);
    let b = workspace_id(&repo_b, "raven", 2);
    let inventory = host_with(vec![session(&a, 2, 1), session(&b, 7, 2)]);
    let gateway = FakeInstaller {
        occupied: vec![a.clone()],
        ..FakeInstaller::default()
    };
    let report = install_all_sessions(&inventory, &gateway)
        .await
        .expect("one refused session does not fail the pass");
    assert_eq!(report.restored, 1);
    assert_eq!(report.refused.len(), 1);
    assert_eq!(report.refused[0].workspace_id, a);
    assert!(matches!(
        report.refused[0].refusal,
        SessionRestoreRefusal::AddressInUse(_)
    ));
    assert_eq!(
        *gateway.installed.lock().expect("installer lock"),
        vec![(b, 7)]
    );
}

#[test]
fn grant_policy_maps_default_ports_and_modes() {
    let grants = GrantSet {
        egress: vec![
            EgressRule {
                host: "*.example.com".to_owned(),
                ports: Vec::new(),
                mode: EgressMode::Intercept,
            },
            EgressRule {
                host: "pinned.example.com".to_owned(),
                ports: vec![8443],
                mode: EgressMode::Opaque,
            },
        ],
        ..GrantSet::default()
    };
    let policy = policy_from_grants(&grants).expect("policy maps");
    assert_eq!(policy.grants.len(), 4);
    assert_eq!(
        policy
            .grants
            .iter()
            .map(|grant| grant.port)
            .collect::<Vec<_>>(),
        vec![443, 80, 8443, 443]
    );
    assert_eq!(
        policy
            .grants
            .iter()
            .map(|grant| grant.mode)
            .collect::<Vec<_>>(),
        vec![
            GatewayEgressMode::Intercept,
            GatewayEgressMode::Intercept,
            GatewayEgressMode::Opaque,
            GatewayEgressMode::Intercept
        ]
    );
    assert!(
        policy.mirrors.is_empty(),
        "no mirror route is configured: the public registry is the gateway's own baseline"
    );
}

#[test]
fn fresh_workspaces_have_anonymous_public_npm_reads_without_probe_grants() {
    let policy = policy_from_grants(&GrantSet::default()).expect("default policy");
    let target = CanonicalTarget::from_authority("registry.npmjs.org:443", TargetScheme::Https)
        .expect("public registry");
    let grant = policy
        .authorize(
            &target,
            &"GET".parse().expect("read method"),
            "/@scope%2fpkg",
        )
        .expect("default npm read");
    assert_eq!(grant.mode, GatewayEgressMode::Intercept);
    assert!(
        !policy
            .resolve_npm_registry(&target, "/@scope%2fpkg")
            .expect("public npm route")
            .credentialed
    );
    assert!(
        policy
            .authorize(&target, &"PUT".parse().expect("write method"), "/pkg")
            .is_err()
    );
    let other_port =
        CanonicalTarget::from_authority("registry.npmjs.org:8443", TargetScheme::Https)
            .expect("other port");
    assert!(
        policy
            .authorize(&other_port, &"CONNECT".parse().expect("tunnel method"), "/")
            .is_err()
    );
}

#[test]
fn explicit_registry_rules_replace_the_package_default_including_wildcards() {
    for host in ["registry.npmjs.org", "*.npmjs.org"] {
        let policy = policy_from_grants(&GrantSet {
            egress: vec![EgressRule {
                host: host.to_owned(),
                ports: vec![443],
                mode: EgressMode::Opaque,
            }],
            ..GrantSet::default()
        })
        .expect("explicit registry policy");
        assert_eq!(policy.grants.len(), 1);
        let target = CanonicalTarget::from_authority("registry.npmjs.org:443", TargetScheme::Https)
            .expect("public registry");
        assert_eq!(
            policy
                .authorize(&target, &"CONNECT".parse().expect("tunnel method"), "/")
                .expect("explicit tunnel")
                .mode,
            GatewayEgressMode::Opaque
        );
        assert!(
            policy
                .authorize(&target, &"GET".parse().expect("read method"), "/pkg")
                .is_err()
        );
    }
}
