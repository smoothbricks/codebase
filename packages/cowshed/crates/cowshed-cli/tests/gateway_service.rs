use async_trait::async_trait;
use cowshed_cli::args::{Command, GatewayCommand, parse_args};
use cowshed_cli::gateway_service::{
    GatewayControl, GatewayDrain, GatewayInstaller, GatewayPaths, SessionInventory,
    activate_launch_agent, drain_after_shutdown, emit_gateway_status, install_all_sessions,
    policy_from_grants, project_session_prefix, reconcile_against_status, reconcile_project,
    stable_workspace_id,
};
use cowshed_cli::launchd::{
    CommandOutput, CommandStatus, LaunchAgentSpec, LaunchctlCommand, LaunchdExecutor,
};
use cowshed_cli::output::Output;
use cowshed_core::api::GatewayStatus as CliGatewayStatus;
use cowshed_core::metadata::{EgressMode, EgressRule, GrantSet};
use cowshed_core::repository::RepoId;
use cowshed_core::{CowshedError, Result};
use cowshed_gateway::{
    GatewayStatus, SessionStatus, WorkspaceCa, WorkspaceEndpoint, WorkspacePolicy,
    WorkspaceSession, WorkspaceToken,
};
use std::collections::VecDeque;
use std::ffi::OsString;
use std::io;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

fn session(identity: &str, revision: u64, token_byte: u8) -> WorkspaceSession {
    WorkspaceSession {
        workspace_id: identity.to_owned(),
        repo_id: "project".to_owned(),
        revision,
        endpoint: WorkspaceEndpoint::Tcp(SocketAddr::from((Ipv4Addr::LOCALHOST, 40_960))),
        token: WorkspaceToken::from_bytes([token_byte; 32]),
        ca: WorkspaceCa::new(
            "-----BEGIN CERTIFICATE-----\npublic\n-----END CERTIFICATE-----".to_owned(),
            "-----BEGIN PRIVATE KEY-----\nprivate\n-----END PRIVATE KEY-----".to_owned(),
        )
        .expect("fixture CA"),
        policy: WorkspacePolicy::default(),
    }
}

fn status(sessions: Vec<SessionStatus>) -> GatewayStatus {
    GatewayStatus {
        draining: false,
        sessions,
        active: 0,
        queued: 0,
    }
}

fn installed(identity: &str, revision: u64) -> SessionStatus {
    SessionStatus {
        workspace_id: identity.to_owned(),
        revision,
        endpoint: "127.0.0.1:40960".to_owned(),
        active: 0,
        queued: 0,
    }
}

#[derive(Default)]
struct FakeControl {
    status: Mutex<Option<std::result::Result<GatewayStatus, String>>>,
    installs: Mutex<Vec<(String, u64)>>,
    removes: Mutex<Vec<(String, u64)>>,
}

#[async_trait]
impl GatewayControl for FakeControl {
    async fn status(&self) -> std::result::Result<GatewayStatus, String> {
        self.status
            .lock()
            .expect("status lock")
            .take()
            .unwrap_or_else(|| Err("status called more than once".to_owned()))
    }

    async fn install(&self, session: &WorkspaceSession) -> std::result::Result<(), String> {
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
    ) -> std::result::Result<(), String> {
        self.removes
            .lock()
            .expect("remove lock")
            .push((workspace_id.to_owned(), expected_revision));
        Ok(())
    }
}

#[test]
fn gateway_parser_is_strict_and_accepts_status_json_after_action() {
    let parsed = parse_args(["gateway", "status", "--json"]).expect("valid gateway status");
    assert_eq!(parsed.command, Command::Gateway(GatewayCommand::Status));
    assert!(parsed.global.json);

    for argv in [
        vec!["gateway"],
        vec!["gateway", "restart"],
        vec!["gateway", "start", "extra"],
        vec!["--project", "/tmp/project", "gateway", "status"],
    ] {
        assert!(
            parse_args(argv).is_err(),
            "unexpectedly accepted invalid argv"
        );
    }
}

#[tokio::test]
async fn reconcile_replaces_restore_revision_and_removes_only_stale_project_sessions() {
    let repo_a = RepoId::parse("acme/widget").expect("repo A");
    let repo_b = RepoId::parse("other/widget").expect("repo B");
    let prefix_a = project_session_prefix(&repo_a);
    let current = stable_workspace_id(&repo_a, "raven");
    let stale = stable_workspace_id(&repo_a, "retired");
    let sibling = stable_workspace_id(&repo_b, "raven");
    let control = FakeControl::default();

    let report = reconcile_against_status(
        &control,
        &prefix_a,
        vec![session(&current, 8, 9)],
        status(vec![
            installed(&current, 7),
            installed(&stale, 3),
            installed(&sibling, 5),
        ]),
    )
    .await
    .expect("reconcile succeeds");

    assert_eq!(report.installed, 1);
    assert_eq!(report.removed, 1);
    assert_eq!(
        *control.installs.lock().expect("install lock"),
        vec![(current, 8)]
    );
    assert_eq!(
        *control.removes.lock().expect("remove lock"),
        vec![(stale, 3)]
    );
}

#[tokio::test]
async fn empty_attached_inventory_removes_detached_session() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let identity = stable_workspace_id(&repo, "raven");
    let control = FakeControl::default();
    let report = reconcile_against_status(
        &control,
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

#[tokio::test]
async fn unchanged_revision_is_idempotent() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let identity = stable_workspace_id(&repo, "raven");
    let control = FakeControl::default();
    let report = reconcile_against_status(
        &control,
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
async fn absent_gateway_is_exit_five_with_deterministic_kickstart_hint() {
    let repo = RepoId::parse("acme/widget").expect("repo");
    let control = FakeControl {
        status: Mutex::new(Some(Err("not found".to_owned()))),
        ..FakeControl::default()
    };
    let error = reconcile_project(&control, &project_session_prefix(&repo), Vec::new(), 501)
        .await
        .expect_err("gateway absence fails");
    assert_eq!(error.exit_code(), 5);
    assert_eq!(
        error.hint,
        "launchctl kickstart -k gui/501/dev.cowshed.gateway"
    );
}

struct FakeInventory {
    all: Mutex<Option<Vec<WorkspaceSession>>>,
}

#[async_trait]
impl SessionInventory for FakeInventory {
    async fn all_sessions(&self) -> Result<Vec<WorkspaceSession>> {
        self.all
            .lock()
            .expect("inventory lock")
            .take()
            .ok_or_else(|| CowshedError::internal("inventory called twice"))
    }

    async fn project_sessions(&self, _repo_id: &RepoId) -> Result<Vec<WorkspaceSession>> {
        Err(CowshedError::internal("project inventory not expected"))
    }
}

#[derive(Default)]
struct FakeInstaller {
    installed: Mutex<Vec<(String, u64)>>,
}

#[async_trait]
impl GatewayInstaller for FakeInstaller {
    async fn install_session(&self, session: WorkspaceSession) -> Result<()> {
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
    let a = stable_workspace_id(&repo_a, "main");
    let b = stable_workspace_id(&repo_b, "raven");
    let inventory = FakeInventory {
        all: Mutex::new(Some(vec![session(&a, 2, 1), session(&b, 7, 2)])),
    };
    let gateway = FakeInstaller::default();
    assert_eq!(
        install_all_sessions(&inventory, &gateway)
            .await
            .expect("recovery succeeds"),
        2
    );
    assert_eq!(
        *gateway.installed.lock().expect("installer lock"),
        vec![(a, 2), (b, 7)]
    );
}

#[test]
fn grant_policy_maps_default_ports_modes_and_credential_suppression() {
    let grants = GrantSet {
        egress: vec![
            EgressRule {
                host: "*.example.com".to_owned(),
                ports: Vec::new(),
                mode: EgressMode::Intercept,
                impersonate: Some("chrome".to_owned()),
            },
            EgressRule {
                host: "pinned.example.com".to_owned(),
                ports: vec![8443],
                mode: EgressMode::Opaque,
                impersonate: None,
            },
        ],
        ..GrantSet::default()
    };
    let policy = policy_from_grants(&grants).expect("policy maps");
    assert_eq!(policy.grants.len(), 3);
    assert_eq!(
        policy
            .grants
            .iter()
            .map(|grant| grant.port)
            .collect::<Vec<_>>(),
        vec![443, 80, 8443]
    );
    assert!(policy.grants[0].impersonate && policy.grants[1].impersonate);
    assert!(!policy.grants[2].impersonate);
    assert_eq!(policy.mirrors.len(), 3);
}

#[derive(Default)]
struct RecordingLaunchctl {
    outputs: VecDeque<io::Result<CommandOutput>>,
    argv: Vec<Vec<OsString>>,
}

impl RecordingLaunchctl {
    fn new(outputs: impl IntoIterator<Item = io::Result<CommandOutput>>) -> Self {
        Self {
            outputs: outputs.into_iter().collect(),
            argv: Vec::new(),
        }
    }
}

impl LaunchctlCommand for RecordingLaunchctl {
    fn run(&mut self, executable: &Path, arguments: &[OsString]) -> io::Result<CommandOutput> {
        assert_eq!(executable, Path::new("/bin/launchctl"));
        self.argv.push(arguments.to_vec());
        self.outputs
            .pop_front()
            .expect("one fake output per launchctl call")
    }
}

fn launch_spec() -> LaunchAgentSpec {
    LaunchAgentSpec::gateway(
        Path::new("/Users/test"),
        Path::new("/Applications/Cowshed.app/Contents/MacOS/cowshed"),
    )
    .expect("valid spec")
}

#[test]
fn launch_agent_activation_bootstraps_only_when_not_loaded() {
    let command = RecordingLaunchctl::new([
        Ok(CommandOutput {
            status: CommandStatus::ExitCode(3),
            stdout: Vec::new(),
            stderr: b"not loaded".to_vec(),
        }),
        Ok(CommandOutput::success()),
        Ok(CommandOutput::success()),
    ]);
    let mut executor = LaunchdExecutor::new((), command);
    activate_launch_agent(&mut executor, 501, &launch_spec()).expect("activation succeeds");
    let (_, command) = executor.into_parts();
    assert_eq!(command.argv.len(), 3);
    assert_eq!(command.argv[0][0], "print");
    assert_eq!(command.argv[1][0], "bootstrap");
    assert_eq!(command.argv[2][0], "kickstart");
    assert_eq!(command.argv[2][1], "-k");
}

#[test]
fn launch_agent_activation_is_idempotent_and_propagates_spawn_failure() {
    let command =
        RecordingLaunchctl::new([Ok(CommandOutput::success()), Ok(CommandOutput::success())]);
    let mut executor = LaunchdExecutor::new((), command);
    activate_launch_agent(&mut executor, 501, &launch_spec()).expect("activation succeeds");
    let (_, command) = executor.into_parts();
    assert_eq!(command.argv.len(), 2);
    assert_eq!(command.argv[0][0], "print");
    assert_eq!(command.argv[1][0], "kickstart");

    let command = RecordingLaunchctl::new([Err(io::Error::new(
        io::ErrorKind::NotFound,
        "launchctl missing",
    ))]);
    let mut executor = LaunchdExecutor::new((), command);
    assert!(activate_launch_agent(&mut executor, 501, &launch_spec()).is_err());
}

#[test]
fn production_config_disables_tcp_and_uses_validated_roots_and_fixed_helper() {
    let paths = GatewayPaths {
        home: PathBuf::from("/Users/test"),
        store: PathBuf::from("/Users/test/.cowshed"),
        cache: PathBuf::from("/Users/test/.cowshed/caches/mirror"),
        telemetry: PathBuf::from("/Users/test/.cowshed/telemetry/gateway"),
        control_socket: PathBuf::from("/Users/test/.cowshed/gateway.sock"),
    };
    let helper = PathBuf::from("/Applications/Cowshed.app/Contents/MacOS/cowshed");
    let config = paths.config(501, helper.clone());
    assert_eq!(config.control_socket, Some(paths.control_socket));
    assert_eq!(config.control_tcp, None);
    assert_eq!(config.authorized_control_uid, 501);
    assert_eq!(config.mirror_cache.cache_root, paths.cache);
    assert_eq!(config.git_helper_executable, Some(helper));
}

struct FakeDrainer(Arc<AtomicBool>);

#[async_trait]
impl GatewayDrain for FakeDrainer {
    async fn drain(self) -> Result<()> {
        self.0.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn termination_signal_path_drains_before_returning() {
    let drained = Arc::new(AtomicBool::new(false));
    drain_after_shutdown(FakeDrainer(Arc::clone(&drained)), async { Ok(()) })
        .await
        .expect("shutdown succeeds");
    assert!(drained.load(Ordering::SeqCst));
}

#[test]
fn gateway_status_json_uses_the_frozen_success_envelope_only() {
    let mut output = Output::new(Vec::new(), Vec::new(), false);
    emit_gateway_status(
        &mut output,
        true,
        CliGatewayStatus {
            running: true,
            socket: PathBuf::from("/Users/test/.cowshed/gateway.sock"),
            cache_entries: 0,
            cache_bytes: 0,
            active_workspaces: 2,
        },
    )
    .expect("status emits");
    let (stdout, stderr) = output.into_inner();
    assert_eq!(
        stdout,
        b"{\"ok\":true,\"result\":{\"running\":true,\"socket\":\"/Users/test/.cowshed/gateway.sock\",\"cacheEntries\":0,\"cacheBytes\":0,\"activeWorkspaces\":2}}\n"
    );
    assert!(stderr.is_empty());
}
