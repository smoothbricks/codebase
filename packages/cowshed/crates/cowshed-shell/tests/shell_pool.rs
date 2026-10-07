//! Warm exec hosts through the real supervisor, SystemSpawnSink, Seatbelt and direnv.
//!
//! Every fixture `.envrc` appends one line to `$TMPDIR/activations` each time direnv evaluates
//! it, so that file's line count is the number of shell activations the workspace paid for.
//! `$TMPDIR` is the exec temp dir, writable in both sandbox modes. Unless a test is about the
//! prewarmed spare, pools run without one, so every activation counted is one a command waited
//! on. Kernel-profile probes run in the host-controller lane: an enclosing workspace sandbox
//! cannot grant the fixture supervisor its independently declared authority.

#![cfg(target_os = "macos")]

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use cowshed_core::api::{
    CommandArg, ExecCommand, ExecRequest, ExitStatus, JobAccounting, JobFailure, JobInfo, JobState,
    RunSandboxMode, ScriptCommand, ScriptValue, StdinSource, WorkspacePath,
};
use cowshed_core::error::Result;
use cowshed_core::host_load::HostLoad;
use cowshed_core::metadata::{PortBlock, WorkspaceIncarnation, WorkspaceName};
use cowshed_core::repository::{OwnedRepoIds, RepoId};
use cowshed_core::runtime::shell_host::ShellHostProgram;
use cowshed_core::runtime::shell_pool::ShellPoolConfig;
use cowshed_core::runtime::supervisor::{
    ArtifactStoreSink, CommitmentDraft, CommitmentSink, SessionToken, SystemSpawnSink,
    WorkspaceAuthoritySnapshot, WorkspaceSupervisor, WorkspaceSupervisorConfig,
    WorkspaceSupervisorHandle,
};
use cowshed_core::sandbox::{SandboxConfig, SandboxGrants};
use cowshed_core::storage::job_artifact::{ArtifactConfig, StreamKind};
use cowshed_core::workspace_credentials::WORKSPACE_TOKEN_PATH;
use cowshed_gateway_types::WorkspaceToken;

const COUNT: &str = "printf 'activation\\n' >> \"$TMPDIR/activations\"\n";

/// The exec host binary under test, read when the test runs. `env!` would bake in the path of
/// the checkout that compiled this test, but the test runs from a cached nextest archive that
/// is relocated into every checkout, and nextest re-points `CARGO_BIN_EXE_cowshed-shell-host`
/// at the extracted binary only at runtime.
fn shell_host() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed-shell-host").expect(
            "cargo and nextest set CARGO_BIN_EXE_cowshed-shell-host for an integration test",
        ),
    )
}

struct DiscardedCommitments;

#[async_trait]
impl CommitmentSink for DiscardedCommitments {
    async fn record(&mut self, _draft: CommitmentDraft) -> Result<()> {
        Ok(())
    }
}

struct Workspace {
    root: PathBuf,
    sandbox: SandboxConfig,
}

impl Workspace {
    /// A workspace another process of this test already prepared under `root`.
    fn from_root(root: PathBuf) -> Self {
        let port_base = 41_248;
        Self {
            sandbox: Self::sandbox_at(&root, port_base),
            root,
        }
    }

    /// HOME and the mount root are siblings, as on a real host: the mount root is a hard deny
    /// (sibling workspaces), so a HOME beneath it would refuse every capability's HOME grant.
    fn sandbox_at(root: &Path, port_base: u16) -> SandboxConfig {
        let mount_root = root.join("mounts");
        SandboxConfig {
            home: root.join("home"),
            workspace_mount: mount_root.join("workspace"),
            mount_root,
            exec_temp_dir: root.join("tmp"),
            port_block: PortBlock::new(port_base, 16).expect("port block"),
            retained_port_blocks: Vec::new(),
            mode: cowshed_core::sandbox::RunSandboxMode::ReadWrite,
            grants: SandboxGrants::default(),
            allowed_unix_sockets: Vec::new(),
            additional_denies: Vec::new(),
            shed_links: Vec::new(),
            git_worktree_repository: None,
            build_volume_mount: None,
            repository_caches: Vec::new(),
            capabilities: Default::default(),
        }
    }

    fn new(label: &str, port_base: u16) -> Self {
        let alias = std::env::temp_dir().join(format!(
            "cowshed-{label}-{}-{}",
            std::process::id(),
            uuid::Uuid::new_v4().simple()
        ));
        std::fs::create_dir_all(&alias).expect("scratch root");
        // Seatbelt matches resolved paths; `/var/folders` is a symlink into `/private/var`.
        let root = std::fs::canonicalize(&alias).expect("canonical scratch root");
        let mount = root.join("mounts/workspace");
        std::fs::create_dir_all(mount.join(".cowshed/bin")).expect("private bin");
        std::fs::write(
            mount.join(WORKSPACE_TOKEN_PATH),
            WorkspaceToken::from_bytes([7; 32]).encode(),
        )
        .expect("workspace token");
        let home = root.join("home");
        let exec_temp_dir = root.join("tmp");
        std::fs::create_dir_all(&home).expect("home");
        std::fs::create_dir_all(&exec_temp_dir).expect("exec temp dir");
        let installed = std::env::split_paths(&std::env::var_os("PATH").expect("host PATH"))
            .map(|directory| directory.join("direnv"))
            .find(|candidate| candidate.is_file())
            .expect("direnv is on PATH");
        std::os::unix::fs::symlink(
            std::fs::canonicalize(installed).expect("resolve direnv"),
            mount.join(".cowshed/bin/direnv"),
        )
        .expect("make direnv reachable in the sandbox");
        let sandbox = Self::sandbox_at(&root, port_base);
        Self { root, sandbox }
    }

    fn mount(&self) -> &Path {
        &self.sandbox.workspace_mount
    }

    fn envrc(&self, body: &str) {
        std::fs::write(self.mount().join(".envrc"), format!("{COUNT}{body}"))
            .expect("write .envrc");
    }

    fn activations(&self) -> usize {
        std::fs::read_to_string(self.sandbox.exec_temp_dir.join("activations"))
            .map(|text| text.lines().count())
            .unwrap_or(0)
    }

    fn supervisor(&self, prewarm: bool) -> WorkspaceSupervisorHandle {
        self.supervisor_hosted_by(ShellHostProgram::dedicated(shell_host()), prewarm)
    }

    fn supervisor_hosted_by(
        &self,
        program: ShellHostProgram,
        prewarm: bool,
    ) -> WorkspaceSupervisorHandle {
        let config = WorkspaceSupervisorConfig {
            authority: authority(1),
            owned_repo_ids: OwnedRepoIds::sole(authority(1).repo_id),
            workspace_root: self.sandbox.workspace_mount.clone(),
            default_cwd: None,
            sandbox: self.sandbox.clone(),
            build_volume_layout: None,
            artifacts: ArtifactConfig::default(),
            term_grace: Duration::from_millis(300),
            actor_capacity: 16,
            event_capacity: 16,
            credential_env_names: std::collections::BTreeSet::new(),
            shell_host: None,
            shell_pool: ShellPoolConfig::default(),
            group_ledger: None,
            telemetry_root: None,
            inherited_groups: Vec::new(),
            volume_labels: None,
            workspace_volume: None,
        };
        let artifacts = ArtifactStoreSink::open(
            config.workspace_root.clone(),
            &config.owned_repo_ids,
            &config.authority,
            config.artifacts.clone(),
        )
        .expect("artifact store");
        WorkspaceSupervisor::start_with_sinks(
            config,
            Box::new(SystemSpawnSink::with_shell_host(
                program,
                ShellPoolConfig {
                    prewarm,
                    ..ShellPoolConfig::default()
                },
            )),
            Box::new(artifacts),
            Box::new(DiscardedCommitments),
        )
        .expect("start supervisor")
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

fn authority(grant_revision: u64) -> WorkspaceAuthoritySnapshot {
    WorkspaceAuthoritySnapshot {
        repo_id: RepoId::parse("acme/widget").expect("repo id"),
        workspace: WorkspaceName::new("main").expect("workspace name"),
        workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
            .expect("incarnation"),
        grant_revision,
        lifecycle_revision: 1,
    }
}

fn request(argv: &[&str]) -> ExecRequest {
    ExecRequest {
        command: ExecCommand::Argv(argv.iter().copied().map(CommandArg::from).collect()),
        cwd: None,
        mode: RunSandboxMode::ReadWrite,
        env: HashMap::new(),
        trace: None,
        stdin: StdinSource::Empty,
        stdout_copy: None,
        stderr_copy: None,
    }
}

fn sh(script: &str) -> ExecRequest {
    request(&["/bin/sh", "-c", script])
}

async fn read_stream(
    handle: &WorkspaceSupervisorHandle,
    info: &JobInfo,
    stream: StreamKind,
) -> Vec<u8> {
    let mut bytes = Vec::new();
    loop {
        let chunk = handle
            .log_read(
                info.job_id,
                stream,
                bytes.len().try_into().expect("offset"),
                false,
            )
            .await
            .expect("read job stream");
        bytes.extend_from_slice(&chunk.bytes);
        if chunk.eof || chunk.bytes.is_empty() {
            return bytes;
        }
    }
}

struct Ran {
    info: JobInfo,
    stdout: String,
    stderr: String,
}

impl Ran {
    fn ok(self) -> Self {
        assert_eq!(
            self.info.exit,
            Some(ExitStatus::Exited { code: 0 }),
            "stdout: {}\nstderr: {}",
            self.stdout,
            self.stderr
        );
        self
    }
}

async fn run_in(
    handle: &WorkspaceSupervisorHandle,
    session: Option<&SessionToken>,
    request: ExecRequest,
) -> Ran {
    let job = handle
        .exec(session, None, request)
        .await
        .expect("admit job");
    let info = tokio::time::timeout(Duration::from_secs(60), handle.wait(job))
        .await
        .expect("job terminates")
        .expect("job outcome");
    let stdout = read_stream(handle, &info, StreamKind::Stdout).await;
    let stderr = read_stream(handle, &info, StreamKind::Stderr).await;
    Ran {
        info,
        stdout: String::from_utf8_lossy(&stdout).into_owned(),
        stderr: String::from_utf8_lossy(&stderr).into_owned(),
    }
}

async fn run(handle: &WorkspaceSupervisorHandle, request: ExecRequest) -> Ran {
    run_in(handle, None, request).await
}

fn process_alive(pid: i32) -> bool {
    // SAFETY: signal 0 performs only the existence and permission check.
    unsafe { libc::kill(pid, 0) == 0 }
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_commands_reuse_one_activation_until_a_watched_file_changes() {
    let workspace = Workspace::new("shell-pool-reuse", 41_104);
    workspace.envrc("export FROM_ENVRC=loaded\n");
    let handle = workspace.supervisor(false);
    let probe = "printf %s \"$FROM_ENVRC\"";

    assert_eq!(run(&handle, sh(probe)).await.ok().stdout, "loaded");
    assert_eq!(workspace.activations(), 1);
    assert_eq!(run(&handle, sh(probe)).await.ok().stdout, "loaded");
    assert_eq!(
        workspace.activations(),
        1,
        "a second command in an unchanged workspace must not evaluate .envrc again"
    );

    workspace.envrc("export FROM_ENVRC=edited\n");
    assert_eq!(run(&handle, sh(probe)).await.ok().stdout, "edited");
    assert_eq!(
        workspace.activations(),
        2,
        "an edited watched file re-activates once"
    );
    assert_eq!(run(&handle, sh(probe)).await.ok().stdout, "edited");
    assert_eq!(workspace.activations(), 2);
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_watched_inputs_reactivate_exactly_once_and_others_never() {
    let workspace = Workspace::new("shell-pool-watches", 41_120);
    for lock in ["bun.lock", "uv.lock"] {
        std::fs::write(workspace.mount().join(lock), "v1").expect("lockfile");
    }
    workspace.envrc("watch_file bun.lock uv.lock\n");
    let handle = workspace.supervisor(false);
    run(&handle, sh("true")).await.ok();
    assert_eq!(workspace.activations(), 1);

    std::fs::write(workspace.mount().join("README.md"), "unwatched").expect("unwatched file");
    run(&handle, sh("true")).await.ok();
    assert_eq!(
        workspace.activations(),
        1,
        "an unwatched file costs nothing"
    );

    std::fs::write(workspace.mount().join("bun.lock"), "v2").expect("edit bun.lock");
    std::fs::write(workspace.mount().join("uv.lock"), "v2").expect("edit uv.lock");
    run(&handle, sh("true")).await.ok();
    assert_eq!(
        workspace.activations(),
        2,
        "two changed inputs, one activation"
    );

    // Same second, same size: only the exact stat identity tells these writes apart.
    std::fs::write(workspace.mount().join("bun.lock"), "v3").expect("rewrite bun.lock");
    run(&handle, sh("true")).await.ok();
    std::fs::write(workspace.mount().join("bun.lock"), "v4").expect("rewrite bun.lock again");
    run(&handle, sh("true")).await.ok();
    assert_eq!(
        workspace.activations(),
        4,
        "two writes inside one second are both observed"
    );
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_an_input_changed_during_activation_is_never_reused() {
    let workspace = Workspace::new("shell-pool-mid-activation", 41_136);
    std::fs::write(workspace.mount().join("flake.lock"), "v1").expect("lockfile");
    // The first evaluation rewrites one of its own watched inputs, as a generator would.
    workspace.envrc(
        "watch_file flake.lock\nif [ ! -e \"$TMPDIR/rewrote\" ]; then : > \"$TMPDIR/rewrote\"; printf v2 > flake.lock; fi\n",
    );
    let handle = workspace.supervisor(false);
    run(&handle, sh("true")).await.ok();
    assert_eq!(workspace.activations(), 1);
    run(&handle, sh("true")).await.ok();
    assert_eq!(
        workspace.activations(),
        2,
        "the shell whose inputs moved under it served one command only"
    );
    run(&handle, sh("true")).await.ok();
    assert_eq!(workspace.activations(), 2);
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_failed_activation_fails_only_the_job_that_triggered_it() {
    let workspace = Workspace::new("shell-pool-activation-failure", 41_152);
    workspace.envrc("echo 'activation broke here' >&2\nexit 3\n");
    let handle = workspace.supervisor(false);
    let failed = run(&handle, sh("printf ran > command-ran")).await;
    assert_ne!(failed.info.exit, Some(ExitStatus::Exited { code: 0 }));
    assert!(
        failed.stderr.contains("activation broke here"),
        "activation output belongs to the job that paid for it: {}",
        failed.stderr
    );
    assert!(!workspace.mount().join("command-ran").exists());

    workspace.envrc("export REPAIRED=yes\n");
    let repaired = run(&handle, sh("printf %s \"$REPAIRED\"")).await.ok();
    assert_eq!(repaired.stdout, "yes");
    assert!(
        !repaired.stderr.contains("activation broke here"),
        "a later job never sees an earlier activation's output"
    );
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_killing_a_job_ends_its_whole_group_and_keeps_the_warm_shell() {
    let workspace = Workspace::new("shell-pool-kill", 41_168);
    workspace.envrc("");
    let handle = workspace.supervisor(false);
    run(&handle, sh("true")).await.ok();
    let job = handle
        .exec(
            None,
            None,
            sh("sleep 300 & printf '%s %s\\n' $$ $! > pids; wait"),
        )
        .await
        .expect("admit");
    let pids = workspace.mount().join("pids");
    let recorded = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(&pids)
                && text.ends_with('\n')
            {
                return text;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the job records its pids");
    let pids: Vec<i32> = recorded
        .split_whitespace()
        .map(|pid| pid.parse().expect("pid"))
        .collect();
    handle.kill(job).await.expect("kill");
    let info = handle.wait(job).await.expect("killed job");
    assert_eq!(info.state, JobState::Killed);
    tokio::time::sleep(Duration::from_millis(100)).await;
    for pid in pids {
        assert!(
            !process_alive(pid),
            "process {pid} of the killed job survived"
        );
    }
    assert_eq!(run(&handle, sh("printf again")).await.ok().stdout, "again");
    assert_eq!(
        workspace.activations(),
        1,
        "the kill never reached the warm shell"
    );
}

/// A warm command whose leader exits while a descendant holds the job's output open is still the
/// job's: the job runs on, a kill reaches the descendant through the group the host still holds,
/// and the job ends Killed with the leader's own exit. The warm shell serves the next command
/// once the host released the command.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_kill_reaches_the_descendants_of_an_exited_leader() {
    let workspace = Workspace::new("shell-pool-exited-leader", 41_344);
    workspace.envrc("");
    let handle = workspace.supervisor(false);
    run(&handle, sh("true")).await.ok();
    let job = handle
        .exec(
            None,
            None,
            sh("sleep 300 & printf '%s\\n' $! > pid; exit 3"),
        )
        .await
        .expect("admit");
    let pid = workspace.mount().join("pid");
    let recorded = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(&pid)
                && text.ends_with('\n')
            {
                return text;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the job records its descendant");
    let descendant: i32 = recorded.trim().parse().expect("pid");
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        handle.info(job).await.expect("job status").state,
        JobState::Running,
        "the descendant holds the job's output open"
    );
    handle.kill(job).await.expect("kill");
    let info = tokio::time::timeout(Duration::from_secs(10), handle.wait(job))
        .await
        .expect("the killed job ends")
        .expect("killed job");
    assert_eq!(
        (info.state, info.exit),
        (JobState::Killed, Some(ExitStatus::Exited { code: 3 }))
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !process_alive(descendant),
        "the kill reached the exited leader's descendant"
    );
    assert_eq!(run(&handle, sh("printf again")).await.ok().stdout, "again");
    assert_eq!(workspace.activations(), 1, "the warm shell was kept");
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_read_only_and_read_write_never_share_a_shell() {
    let workspace = Workspace::new("shell-pool-modes", 41_184);
    workspace.envrc("");
    let handle = workspace.supervisor(false);
    let write = "printf x > written-by-job";
    let mode = |mode| ExecRequest { mode, ..sh(write) };
    run(&handle, mode(RunSandboxMode::ReadWrite)).await.ok();
    assert_eq!(workspace.activations(), 1);
    let read_only = run(&handle, mode(RunSandboxMode::ReadOnly)).await;
    assert_ne!(
        read_only.info.exit,
        Some(ExitStatus::Exited { code: 0 }),
        "a read-only job never runs in a read-write shell"
    );
    assert_eq!(workspace.activations(), 2);
    run(&handle, mode(RunSandboxMode::ReadWrite)).await.ok();
    let read_only = run(&handle, mode(RunSandboxMode::ReadOnly)).await;
    assert_ne!(read_only.info.exit, Some(ExitStatus::Exited { code: 0 }));
    assert_eq!(workspace.activations(), 2, "each mode reuses its own shell");
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_grant_revision_retires_every_older_shell() {
    let workspace = Workspace::new("shell-pool-grants", 41_200);
    workspace.envrc("");
    let handle = workspace.supervisor(false);
    run(&handle, sh("true")).await.ok();
    run(&handle, sh("true")).await.ok();
    assert_eq!(workspace.activations(), 1);
    let advanced = handle
        .advance_authority(2, 1, workspace.sandbox.clone())
        .await
        .expect("advance the grant revision");
    let ran = run(&advanced, sh("true")).await.ok();
    assert_eq!(ran.info.grant_revision, 2);
    assert_eq!(
        workspace.activations(),
        2,
        "no shell built under the old revision serves the new one"
    );
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_each_command_has_its_own_cwd_env_streams_status_and_stdin() {
    let workspace = Workspace::new("shell-pool-isolation", 41_216);
    std::fs::create_dir_all(workspace.mount().join("sub")).expect("subdirectory");
    workspace.envrc("export SHARED=from-envrc\n");
    let handle = workspace.supervisor(false);
    // The activating command's stderr carries direnv's own output; every command below is warm.
    run(&handle, sh("true")).await.ok();

    let with_env = run(
        &handle,
        ExecRequest {
            env: HashMap::from([("ONLY_HERE".to_owned(), "1".to_owned())]),
            cwd: Some(WorkspacePath::new("sub").expect("cwd")),
            ..sh("printf '%s|%s|%s' \"$ONLY_HERE\" \"$SHARED\" \"$PWD\"; printf err >&2; export LEAK=1; cd /")
        },
    )
    .await
    .ok();
    let sub = workspace.mount().join("sub");
    assert_eq!(
        with_env.stdout,
        format!("1|from-envrc|{}", sub.display()),
        "a command sees its own env overlay and cwd over the activated shell"
    );
    assert_eq!(with_env.stderr, "err", "stdout and stderr stay separate");
    let next = run(
        &handle,
        sh("printf '%s|%s|%s' \"${ONLY_HERE-}\" \"${LEAK-}\" \"$PWD\""),
    )
    .await
    .ok();
    assert_eq!(
        next.stdout,
        format!("||{}", workspace.mount().display()),
        "nothing one command set or changed reaches the next"
    );

    let session = handle
        .open_session(Some("builder".into()))
        .await
        .expect("session");
    run_in(
        &handle,
        Some(&session),
        ExecRequest {
            env: HashMap::from([("SESSION_VAR".to_owned(), "kept".to_owned())]),
            ..sh("true")
        },
    )
    .await
    .ok();
    let in_session = run_in(&handle, Some(&session), sh("printf %s \"$SESSION_VAR\""))
        .await
        .ok();
    assert_eq!(
        in_session.stdout, "kept",
        "a session's env applies to each of its commands"
    );

    let signalled = run(&handle, sh("kill -SEGV $$")).await;
    assert_eq!(
        signalled.info.exit,
        Some(ExitStatus::Signaled {
            signal: libc::SIGSEGV,
            core_dumped: false,
        }),
        "a signal death keeps its exact signal"
    );
    let exited = run(&handle, sh("exit 7")).await;
    assert_eq!(exited.info.exit, Some(ExitStatus::Exited { code: 7 }));

    let echoed = run(
        &handle,
        ExecRequest {
            stdin: StdinSource::Inline(bytes::Bytes::from_static(b"piped input")),
            ..request(&["/bin/cat"])
        },
    )
    .await
    .ok();
    assert_eq!(echoed.stdout, "piped input");

    let missing = run(&handle, request(&["/no/such/program"])).await;
    assert_eq!(missing.info.exit, Some(ExitStatus::Exited { code: 127 }));
    assert!(
        missing.stderr.contains("/no/such/program"),
        "{}",
        missing.stderr
    );
    assert_eq!(
        workspace.activations(),
        1,
        "every command above ran in one warm shell"
    );
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_concurrent_command_takes_the_prewarmed_spare() {
    let workspace = Workspace::new("shell-pool-spare", 41_232);
    workspace.envrc("");
    let handle = workspace.supervisor(true);
    let first = run(&handle, sh("true")).await.ok();
    assert!(
        first.stderr.contains("direnv: loading"),
        "the first command paid for activation: {}",
        first.stderr
    );
    let spare_ready = tokio::time::timeout(Duration::from_secs(30), async {
        while workspace.activations() < 2 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
    assert!(
        spare_ready.is_ok(),
        "a spare is activated once a generation exists"
    );
    // One command holds the returned shell; the next takes the spare without activating.
    let holding = handle.exec(None, None, sh("sleep 2")).await.expect("admit");
    let second = run(&handle, sh("true")).await.ok();
    assert!(
        !second.stderr.contains("direnv: loading"),
        "the spare was already activated: {}",
        second.stderr
    );
    handle.wait(holding).await.expect("holding job");
}

const ORPHAN_HELPER: &str = "COWSHED_SHELL_POOL_ORPHAN_HELPER";

/// The controller half of the test below: a process that starts an activation and exits
/// without running a destructor, exactly as a finished `cowshed exec` does.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_orphan_helper_exits_while_an_activation_runs() {
    let Some(root) = std::env::var_os(ORPHAN_HELPER) else {
        return;
    };
    let workspace = Workspace::from_root(PathBuf::from(root));
    let handle = workspace.supervisor(false);
    let _job = handle.exec(None, None, sh("true")).await.expect("admit");
    let pid_file = workspace.sandbox.exec_temp_dir.join("activation-pid");
    while !pid_file.exists() {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    std::process::exit(0);
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_an_activation_ends_when_its_supervisor_process_does() {
    let workspace = Workspace::new("shell-pool-orphan", 41_248);
    workspace.envrc("printf '%s\\n' $$ > \"$TMPDIR/activation-pid\"\nsleep 60\n");
    let status = std::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "host_controller_orphan_helper_exits_while_an_activation_runs",
            "--ignored",
            "--nocapture",
        ])
        .env(ORPHAN_HELPER, &workspace.root)
        .status()
        .expect("run the controller helper");
    assert!(status.success(), "helper: {status}");
    let pid: i32 = std::fs::read_to_string(workspace.sandbox.exec_temp_dir.join("activation-pid"))
        .expect("the activation recorded its pid")
        .trim()
        .parse()
        .expect("pid");
    let ended = tokio::time::timeout(Duration::from_secs(10), async {
        while process_alive(pid) {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(
        ended.is_ok(),
        "the activation of a supervisor that exited kept running as pid {pid}"
    );
}

fn script(parts: &[&str], values: Vec<ScriptValue>) -> ExecRequest {
    ExecRequest {
        command: ExecCommand::Script(
            ScriptCommand::new(
                parts.iter().map(|part| (*part).to_owned()).collect(),
                values,
            )
            .expect("a well-shaped script"),
        ),
        ..request(&["unused"])
    }
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_script_job_runs_in_the_warm_shell_with_its_values_as_data() {
    let workspace = Workspace::new("shell-pool-script", 41_264);
    workspace.envrc("export GREETING=hello\n");
    let handle = workspace.supervisor(false);
    run(&handle, sh("true")).await.ok();
    let ran = run(
        &handle,
        script(
            &["printf '%s:' \"$GREETING\" ", " | tr a-z A-Z"],
            vec![ScriptValue::Words(vec!["x y".into(), "$(id)".into()])],
        ),
    )
    .await
    .ok();
    assert_eq!(ran.stdout, "HELLO:X Y:$(ID):");
    assert!(
        matches!(ran.info.command, ExecCommand::Script(_)),
        "the job records the script it ran"
    );
    assert_eq!(
        workspace.activations(),
        1,
        "the script ran in the warm shell"
    );
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_script_that_does_not_parse_is_a_typed_failure() {
    let workspace = Workspace::new("shell-pool-script-syntax", 41_280);
    workspace.envrc("");
    let handle = workspace.supervisor(false);
    let ran = run(&handle, script(&["touch ran; if true; then"], Vec::new())).await;
    assert_eq!(ran.info.state, JobState::Failed);
    assert_eq!(ran.info.failure, Some(JobFailure::ScriptSyntax));
    assert_eq!(ran.info.exit, Some(ExitStatus::Exited { code: 2 }));
    assert!(ran.stderr.contains("does not parse"), "{}", ran.stderr);
    assert!(!workspace.mount().join("ran").exists());
    assert_eq!(
        run(&handle, script(&["printf still"], Vec::new()))
            .await
            .ok()
            .stdout,
        "still",
        "the host that refused a script keeps serving"
    );
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_killing_a_script_job_ends_every_process_it_started() {
    let workspace = Workspace::new("shell-pool-script-kill", 41_296);
    workspace.envrc("");
    let handle = workspace.supervisor(false);
    run(&handle, sh("true")).await.ok();
    let job = handle
        .exec(
            None,
            None,
            script(
                &["sleep 300 & (sleep 301; true) & printf '%s %s\\n' $$ $! > pids; wait"],
                Vec::new(),
            ),
        )
        .await
        .expect("admit");
    let pids = workspace.mount().join("pids");
    let recorded = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Ok(text) = std::fs::read_to_string(&pids)
                && text.ends_with('\n')
            {
                return text;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the script records its pids");
    let pids: Vec<i32> = recorded
        .split_whitespace()
        .map(|pid| pid.parse().expect("pid"))
        .collect();
    handle.kill(job).await.expect("kill");
    let info = handle.wait(job).await.expect("killed job");
    assert_eq!(info.state, JobState::Killed);
    assert_eq!(
        info.exit,
        Some(ExitStatus::Signaled {
            signal: libc::SIGTERM,
            core_dumped: false,
        })
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    for pid in pids {
        assert!(
            !process_alive(pid),
            "process {pid} of the killed script survived"
        );
    }
    assert_eq!(run(&handle, sh("printf again")).await.ok().stdout, "again");
    assert_eq!(
        workspace.activations(),
        1,
        "the kill never reached the warm shell"
    );
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_revoke_binds_every_later_command_and_no_running_one() {
    let mut workspace = Workspace::new("shell-pool-revoke", 41_312);
    workspace.envrc("");
    let granted = workspace
        .root
        .parent()
        .expect("scratch parent")
        .join(format!("cowshed-granted-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&granted).expect("granted directory");
    let granted_path = granted.display().to_string();
    workspace.sandbox.grants.write = vec![granted.clone()];
    let handle = workspace.supervisor(true);
    run(&handle, sh(&format!("printf n > {granted_path}/before")))
        .await
        .ok();
    // Admitted under revision 1; it writes only once the revoke is installed.
    let gate = workspace.mount().join("gate");
    let running = handle
        .exec(
            None,
            None,
            sh(&format!(
                "while [ ! -e {} ]; do sleep 0.05; done; printf n > {granted_path}/during",
                gate.display()
            )),
        )
        .await
        .expect("admit the running job");
    // The spare built ahead of demand under revision 1 exists before the revoke.
    let spare = tokio::time::timeout(Duration::from_secs(60), async {
        while workspace.activations() < 2 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await;
    assert!(spare.is_ok(), "a spare activates under revision 1");
    let before = workspace.activations();

    let mut revoked = workspace.sandbox.clone();
    revoked.grants.write.clear();
    let advanced = handle
        .advance_authority(2, 1, revoked)
        .await
        .expect("install revision 2");
    let denied = run(&advanced, sh(&format!("printf n > {granted_path}/after"))).await;
    assert_eq!(denied.info.grant_revision, 2);
    assert_ne!(
        denied.info.exit,
        Some(ExitStatus::Exited { code: 0 }),
        "the first command after the revoke is refused the revoked path"
    );
    assert!(!granted.join("after").exists());
    assert!(
        workspace.activations() > before,
        "no host built under revision 1, the spare included, serves revision 2"
    );

    std::fs::write(&gate, b"go").expect("release the running job");
    let finished = tokio::time::timeout(Duration::from_secs(60), advanced.wait(running))
        .await
        .expect("the running job ends")
        .expect("its outcome");
    assert_eq!(finished.exit, Some(ExitStatus::Exited { code: 0 }));
    assert_eq!(finished.grant_revision, 1);
    assert!(
        granted.join("during").exists(),
        "a job admitted under revision 1 finishes under the profile it started with"
    );
    std::fs::remove_dir_all(&granted).ok();
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_host_that_breaks_while_activating_fails_the_job_with_the_reason() {
    let workspace = Workspace::new("shell-pool-broken-host", 41_328);
    workspace.envrc("");
    // A "host" that exits with a code of its own before answering anything: the supervisor
    // sees its control socket close in the middle of approving the `.envrc`.
    let broken = workspace.root.join("broken-host");
    std::fs::write(&broken, "#!/bin/sh\nexit 3\n").expect("broken host");
    std::fs::set_permissions(&broken, std::os::unix::fs::PermissionsExt::from_mode(0o755))
        .expect("executable");
    let handle = workspace.supervisor_hosted_by(ShellHostProgram::dedicated(&broken), false);
    let ran = run(&handle, sh("printf never")).await;
    assert_eq!(
        ran.info.state,
        JobState::Failed,
        "the command never ran, so the job is a failed launch, not the host's own status: {:?}",
        ran.info.exit
    );
    assert!(
        ran.stderr.contains("exec host"),
        "the job says why it did not run: {:?}",
        ran.stderr
    );
    assert!(ran.stdout.is_empty());
}

/// A FIFO at `path`: its reader and its writer each wait for the other, so a process blocked on
/// one is held exactly until the test opens the other end.
fn fifo(path: &Path) {
    let raw = std::ffi::CString::new(path.as_os_str().as_encoded_bytes()).expect("fifo path");
    // SAFETY: `raw` is a NUL-terminated path that outlives the call.
    assert_eq!(
        unsafe { libc::mkfifo(raw.as_ptr(), 0o600) },
        0,
        "mkfifo {}: {}",
        path.display(),
        std::io::Error::last_os_error()
    );
}

/// The pid a job's process wrote into the FIFO at `path`, once it wrote it.
async fn read_pid(path: &Path) -> i32 {
    let path = path.to_path_buf();
    let text = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::task::spawn_blocking(move || std::fs::read_to_string(path)),
    )
    .await
    .expect("the job writes its pid")
    .expect("reader task")
    .expect("read the pid");
    text.trim().parse().expect("a pid")
}

/// Release the job's process blocked reading the FIFO at `path`.
async fn release(path: &Path) {
    let path = path.to_path_buf();
    tokio::time::timeout(
        Duration::from_secs(60),
        tokio::task::spawn_blocking(move || std::fs::write(path, "go\n")),
    )
    .await
    .expect("the job reads its release")
    .expect("writer task")
    .expect("write the release");
}

/// Wall microseconds from `earlier` to `later`.
fn micros(earlier: std::time::Instant, later: std::time::Instant) -> u64 {
    u64::try_from(later.duration_since(earlier).as_micros()).expect("micros")
}

/// A cold host's activation is its job's first process: the job is sampled while the activation
/// runs, led by the activation's group, and the command's group takes the lead when it starts.
/// The activation's spawn stays the wall baseline, and the terminal sample is the sealed one.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_cold_activation_is_sampled_before_its_command_starts() {
    let workspace = Workspace::new("shell-pool-resources", 41_360);
    let tmp = workspace.sandbox.exec_temp_dir.clone();
    for name in [
        "activation-pid",
        "activation-hold",
        "command-pid",
        "command-hold",
    ] {
        fifo(&tmp.join(name));
    }
    workspace.envrc(
        "printf '%s\\n' $$ > \"$TMPDIR/activation-pid\"\nread _ < \"$TMPDIR/activation-hold\"\n",
    );
    let handle = workspace.supervisor(false);
    let admitted = std::time::Instant::now();
    let job = handle
        .exec(
            None,
            None,
            sh("printf '%s\\n' $$ > \"$TMPDIR/command-pid\"; read _ < \"$TMPDIR/command-hold\""),
        )
        .await
        .expect("admit");

    // The activation runs, held on its FIFO; its group is the host's, which leads the job.
    let activation_pid = read_pid(&tmp.join("activation-pid")).await;
    let activation_seen = std::time::Instant::now();
    // SAFETY: getpgid only reads the process's group.
    let host = unsafe { libc::getpgid(activation_pid) };
    assert!(
        host > 0,
        "the activation's group: {}",
        std::io::Error::last_os_error()
    );
    let load_before = HostLoad::read().expect("independent getloadavg before the read");
    let activating = handle
        .resources(job)
        .await
        .expect("an activating job is sampled");
    let load_after = HostLoad::read().expect("independent getloadavg after the read");
    let read = std::time::Instant::now();
    assert_eq!(
        (activating.job_id, activating.leader_pid),
        (job, u32::try_from(host).expect("pid"))
    );
    // The sample's host is the kernel's at the read, not a constant or the spawn's: it lies
    // between two independent reads that bracket it.
    let load = activating.host.load1.get();
    assert!(
        (load_before.one.min(load_after.one) - 0.01..=load_before.one.max(load_after.one) + 0.01)
            .contains(&load),
        "sampled load {load}, independent before {}, after {}",
        load_before.one,
        load_after.one
    );
    // SAFETY: sysconf reads a system constant and borrows nothing.
    let online = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_ONLN) };
    assert_eq!(
        libc::c_long::from(activating.host.cores.get()),
        online,
        "the sample's cores are the host's online cores"
    );
    assert!(
        activating.wall_us.get() <= micros(admitted, read),
        "the job spawned after its admission"
    );
    assert_eq!(
        handle.info(job).await.expect("status").resources,
        Some(activating.clone()),
        "status carries the latest sample"
    );

    release(&tmp.join("activation-hold")).await;
    let command_pid = read_pid(&tmp.join("command-pid")).await;
    let before = std::time::Instant::now();
    let running = handle
        .resources(job)
        .await
        .expect("a running job is sampled");
    assert_eq!(
        running.leader_pid,
        u32::try_from(command_pid).expect("pid"),
        "the command leads its own group"
    );
    assert_eq!(
        running.host_start, activating.host_start,
        "the command keeps the activation's spawn host baseline"
    );
    assert!(
        running.wall_us.get() >= micros(activation_seen, before),
        "the wall still counts from the activation's spawn"
    );
    assert!(running.wall_us > activating.wall_us);

    release(&tmp.join("command-hold")).await;
    let ended = tokio::time::timeout(Duration::from_secs(60), handle.wait(job))
        .await
        .expect("the job ends")
        .expect("job outcome");
    assert_eq!(ended.exit, Some(ExitStatus::Exited { code: 0 }));
    let terminal = ended.resources.expect("a terminal sample");
    assert_eq!(
        terminal.leader_pid, running.leader_pid,
        "the leader is named after it exits"
    );
    assert_eq!(
        terminal.host_start, activating.host_start,
        "the terminal keeps the first owned process's host baseline"
    );
    assert!(terminal.wall_us >= running.wall_us);
    assert_eq!(
        handle.sealed(job).await.expect("sealed").resources,
        Some(terminal),
        "the sealed sample is the last one"
    );
}

/// A job's sample names every running process of its group: the command's shell and the two
/// children it started, each held on a FIFO the test never opens. A kill empties the group, and
/// the sealed sample still names the leader.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_job_s_sample_names_its_whole_group_and_none_after_a_kill() {
    let workspace = Workspace::new("shell-pool-members", 41_376);
    let tmp = workspace.sandbox.exec_temp_dir.clone();
    for name in ["pids", "hold-a", "hold-b"] {
        fifo(&tmp.join(name));
    }
    workspace.envrc("");
    let handle = workspace.supervisor(false);
    let job = handle
        .exec(
            None,
            None,
            sh(concat!(
                "(read _ < \"$TMPDIR/hold-a\") & a=$!; ",
                "(read _ < \"$TMPDIR/hold-b\") & b=$!; ",
                "printf '%s %s %s\\n' $$ $a $b > \"$TMPDIR/pids\"; wait",
            )),
        )
        .await
        .expect("admit");

    // Both children were born before the shell wrote their pids.
    let pids = tmp.join("pids");
    let text = tokio::time::timeout(
        Duration::from_secs(60),
        tokio::task::spawn_blocking(move || std::fs::read_to_string(pids)),
    )
    .await
    .expect("the job writes its pids")
    .expect("reader task")
    .expect("read the pids");
    let mut group: Vec<u32> = text
        .split_whitespace()
        .map(|pid| pid.parse().expect("a pid"))
        .collect();
    let leader = group[0];
    group.sort_unstable();
    let mut running = handle
        .resources(job)
        .await
        .expect("a running job is sampled");
    running.members.sort_unstable();
    assert_eq!(
        (running.leader_pid, running.members),
        (leader, group),
        "the leader and both children, nothing more"
    );

    handle.kill(job).await.expect("kill");
    let ended = tokio::time::timeout(Duration::from_secs(60), handle.wait(job))
        .await
        .expect("the killed job ends")
        .expect("job outcome");
    assert_eq!(ended.state, JobState::Killed);
    let terminal = ended.resources.expect("a terminal sample");
    assert_eq!(
        (terminal.leader_pid, terminal.members.clone()),
        (leader, Vec::new()),
        "the leader is named after its group emptied"
    );
    assert_eq!(
        handle.sealed(job).await.expect("sealed").resources,
        Some(terminal),
        "the sealed sample is the last one"
    );
}

/// A shell line that burns CPU, then writes what the shell measured itself spending (`times`:
/// its own user and system time) to `$TMPDIR/<name>`.
fn burn(name: &str) -> String {
    format!("i=0; while [ $i -lt 300000 ]; do i=$((i+1)); done; times > \"$TMPDIR/{name}\"")
}

/// The microseconds of user and system CPU the shell that ran [`burn`] measured for itself, at
/// least: `times` shows each in milliseconds, rounded (measured: 588.000 s shown for a shell
/// whose rusage held 587.988), so each field may stand up to a millisecond above it.
fn measured(workspace: &Workspace, name: &str) -> u64 {
    let times = std::fs::read_to_string(workspace.sandbox.exec_temp_dir.join(name))
        .expect("the shell's times");
    let own = times.lines().next().expect("the shell's own line");
    own.split_whitespace()
        .map(|field| {
            let (minutes, seconds) = field
                .strip_suffix('s')
                .and_then(|field| field.split_once('m'))
                .unwrap_or_else(|| panic!("a `times` field, not {field:?}"));
            let (whole, fraction) = seconds.split_once('.').expect("fractional seconds");
            let minutes: u64 = minutes.parse().expect("minutes");
            let whole: u64 = whole.parse().expect("seconds");
            let micros: u64 = format!("{fraction:0<6}")[..6].parse().expect("fraction");
            ((minutes * 60 + whole) * 1_000_000 + micros).saturating_sub(1_000)
        })
        .sum()
}

/// A sealed job's CPU from its accounting source; the source has no storage byte totals, so
/// they are unavailable, never zero.
fn accounted(info: &JobInfo) -> u64 {
    match info.resources.as_ref().map(|sample| sample.accounting) {
        Some(Some(JobAccounting::MacOsRusageChildren { cpu, io: None })) => {
            cpu.user_us.get() + cpu.sys_us.get()
        }
        other => panic!("a terminal sample of the leader/children rusage source: {other:?}"),
    }
}

/// A cold host's activation and the command it then runs each count once in the job's CPU,
/// though each burned in a process the host and the command's shell reaped before the end. A
/// second job on the warm host is charged its own command alone: never the host's activation,
/// its idle time, or the first command it reaped.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_job_counts_its_activation_and_command_once_and_a_warm_host_none() {
    let workspace = Workspace::new("shell-pool-accounting", 41_392);
    workspace.envrc(&format!("{}\n", burn("activation-times")));
    let handle = workspace.supervisor(false);
    let cold = run(&handle, sh(&burn("command-times"))).await.ok();
    let (activation, command) = (
        measured(&workspace, "activation-times"),
        measured(&workspace, "command-times"),
    );
    let total = accounted(&cold.info);
    assert!(
        total >= activation + command && total < activation + command + activation.min(command),
        "the job's {total} us are its activation's {activation} us and its command's {command} \
         us, once each"
    );
    assert_eq!(
        handle
            .sealed(cold.info.job_id)
            .await
            .expect("sealed")
            .resources,
        cold.info.resources,
        "the sealed sample keeps the totals"
    );

    let warm = run(&handle, sh(&burn("warm-times"))).await.ok();
    assert_eq!(
        workspace.activations(),
        1,
        "the second job reused the warm host"
    );
    let own = measured(&workspace, "warm-times");
    let charged = accounted(&warm.info);
    assert!(
        charged >= own && charged < own + activation.min(command),
        "the warm job's {charged} us are its own command's {own} us, without the host's \
         {activation} us activation or the {command} us command it reaped before"
    );
}

/// An activation that fails keeps what it cost in its job's sealed sample.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_failed_activation_keeps_its_cost() {
    let workspace = Workspace::new("shell-pool-accounting-failure", 41_408);
    workspace.envrc(&format!("{}\nexit 3\n", burn("activation-times")));
    let handle = workspace.supervisor(false);
    let failed = run(&handle, sh("printf ran > command-ran")).await;
    assert_ne!(failed.info.exit, Some(ExitStatus::Exited { code: 0 }));
    assert!(!workspace.mount().join("command-ran").exists());
    let activation = measured(&workspace, "activation-times");
    let total = accounted(&failed.info);
    assert!(
        total >= activation && total < 2 * activation,
        "the failed job's {total} us are its activation's {activation} us"
    );
    assert_eq!(
        handle
            .sealed(failed.info.job_id)
            .await
            .expect("sealed")
            .resources,
        failed.info.resources,
        "the sealed sample keeps the activation's cost"
    );
}
