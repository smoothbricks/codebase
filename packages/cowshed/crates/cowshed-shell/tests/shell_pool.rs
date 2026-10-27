//! Warm exec hosts through the real supervisor, SystemSpawnSink, Seatbelt and direnv.
//!
//! Every fixture `.envrc` appends one line to `$TMPDIR/activations` each time direnv evaluates
//! it, so that file's line count is the number of shell activations the workspace paid for.
//! `$TMPDIR` is the exec temp dir, writable in both sandbox modes. Unless a test is about the
//! prewarmed spare, pools run without one, so every activation counted is one a command waited
//! on. Kernel-profile probes run in the host-controller lane: an enclosing workspace sandbox
//! cannot grant the fixture supervisor its independently declared authority.

#![cfg(target_os = "macos")]

use cowshed_os::test_backstop::{AsyncChildBackstop as _, CommandBackstop as _};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;
use cowshed_core::api::ExecClass;
use cowshed_core::api::{
    CommandArg, ExecCommand, ExecRequest, ExecScope, ExitStatus, JobAccounting, JobFailure, JobId,
    JobInfo, JobState, RunSandboxMode, ScriptCommand, ScriptValue, StdinSource, WorkspacePath,
};
use cowshed_core::build_volume::nx::tree_holders;
use cowshed_core::error::Result;
use cowshed_core::metadata::{PortBlock, WorkspaceIncarnation, WorkspaceName};
use cowshed_core::repository::{OwnedRepoIds, RepoId};
use cowshed_core::runtime::job_cpu::cpu_pool_directory;
use cowshed_core::runtime::shell_host::ShellHostProgram;
use cowshed_core::runtime::shell_pool::ShellPoolConfig;
use cowshed_core::runtime::supervisor::{
    ArtifactStoreSink, CommitmentDraft, CommitmentSink, SessionToken, SystemSpawnSink,
    WorkspaceAuthoritySnapshot, WorkspaceSupervisor, WorkspaceSupervisorConfig,
    WorkspaceSupervisorHandle,
};
use cowshed_core::runtime::supervisor_socket::AdmissionSource;
use cowshed_core::sandbox::{SandboxConfig, SandboxGrants};
use cowshed_core::storage::job_artifact::{ArtifactConfig, StreamKind};
use cowshed_core::workspace_credentials::WORKSPACE_TOKEN_PATH;
use cowshed_gateway_types::WorkspaceToken;
use cowshed_os::host_load::HostLoad;
use cowshed_os::process::ExitWatch;

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

/// A private directory under a short parent, so a socket a few levels below it fits
/// `sun_path`; removed when dropped, on every way out of a test.
struct LedgerRoot(PathBuf);

impl Drop for LedgerRoot {
    fn drop(&mut self) {
        if let Err(error) = std::fs::remove_dir_all(&self.0)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            cowshed_os::eprintln!("remove ledger root {}: {error}", self.0.display());
        }
    }
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
            store_root: root.join("store"),
            port_block: PortBlock::new(port_base, 16).expect("port block"),
            retained_port_blocks: Vec::new(),
            mode: cowshed_core::sandbox::RunSandboxMode::ReadWrite,
            grants: SandboxGrants::default(),
            allowed_unix_sockets: Vec::new(),
            cpu_pool_directory: Some(Self::pool_directory(root)),
            additional_denies: Vec::new(),
            shed_links: Vec::new(),
            git_worktree_repository: None,
            build_volume_mount: None,
            repository_caches: Vec::new(),
            program_directories: Vec::new(),
            capabilities: Default::default(),
        }
    }

    /// The workspace's CPU pool directory under its fixture store, as the supervisor names one.
    fn pool_directory(root: &Path) -> PathBuf {
        cpu_pool_directory(&root.join("store"), &authority(1).workspace_incarnation)
    }

    fn new(label: &str, port_base: u16) -> Self {
        let parent = std::fs::canonicalize("/tmp").expect("canonical controller fixture parent");
        let alias = parent.join(format!(
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

    /// A workspace mounted beside `beside`, under the same mount root, as sibling workspaces of
    /// one repository are on a real host: they share the host user's identity home (`sandbox.home`,
    /// which names the installed cowshed directory on PATH), while each keeps its own private
    /// home, temp directory and store.
    fn new_beside(label: &str, port_base: u16, beside: &Workspace) -> Self {
        let mut workspace = Self::new(label, port_base);
        let mount = beside.sandbox.mount_root.join(label);
        std::fs::rename(workspace.mount(), &mount).expect("mount beside");
        workspace.sandbox.mount_root = beside.sandbox.mount_root.clone();
        workspace.sandbox.workspace_mount = mount;
        workspace.sandbox.home = beside.sandbox.home.clone();
        workspace
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
        self.supervisor_with(program, prewarm)
    }

    fn supervisor_with(
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
            group_ledger_lease: None,
            membership_socket: None,
            telemetry_root: None,
            inherited_groups: Vec::new(),
            volume_labels: None,
            workspace_volume: None,
            job_sampling: cowshed_core::runtime::supervisor::JobSamplingPolicy::default(),
            build_volume_released: None,
            nx_daemon_idle: cowshed_core::runtime::supervisor::NxDaemonIdle::DEFAULT,
        };
        let artifacts = ArtifactStoreSink::open(
            config.workspace_root.clone(),
            &config.owned_repo_ids,
            &config.authority,
            config.artifacts.clone(),
        )
        .expect("artifact store");
        let spawner = SystemSpawnSink::with_shell_host(
            program,
            ShellPoolConfig {
                prewarm,
                ..ShellPoolConfig::default()
            },
        );
        WorkspaceSupervisor::start_with_sinks(
            config,
            Box::new(spawner),
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
        admission_key: None,
        class: ExecClass::Background,
        want: None,
        scope: ExecScope::Workspace,
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
        .exec(session, None, request, AdmissionSource::Worker)
        .await
        .expect("admit job");
    ran(handle, job).await
}

/// `job` once it ended, with what it wrote.
async fn ran(handle: &WorkspaceSupervisorHandle, job: JobId) -> Ran {
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

/// Wait for the real command to seal before reading: every byte must cross the proven range path.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_sealed_ranges_page_a_bounded_command_and_latest_tail() {
    use cowshed_core::api::{JobTailBytes, JobTailLimits, ProtectedOutput};
    let workspace = Workspace::new("sealed-range-command", 41_504);
    workspace.envrc("");
    let handle = workspace.supervisor(false);
    let job = handle
        .exec(
            None,
            None,
            sh("printf '%0196609d' 0; printf 'sealed-range-stderr\\n' >&2"),
            AdmissionSource::Worker,
        )
        .await
        .expect("admit finite output command");
    let info = handle.wait(job).await.expect("command sealed");
    assert_eq!(info.exit, Some(ExitStatus::Exited { code: 0 }));
    assert_eq!(info.stdout.bytes, 196_609);
    assert!(matches!(
        info.stdout.storage.artifact(),
        ProtectedOutput::File { .. }
    ));
    let mut offset = 0;
    for want in [65_536, 65_536, 65_536, 1] {
        let chunk = handle
            .log_read(job, StreamKind::Stdout, offset, false)
            .await
            .expect("sealed positioned page");
        assert_eq!(chunk.bytes.len(), want);
        assert!(chunk.bytes.iter().all(|byte| *byte == b'0'));
        offset += u64::try_from(want).unwrap();
        assert_eq!(chunk.next_offset, offset);
        assert_eq!(chunk.eof, offset == info.stdout.bytes);
    }
    let end = handle
        .log_read(job, StreamKind::Stdout, offset, false)
        .await
        .expect("sealed end");
    assert!(end.bytes.is_empty() && end.eof);
    let tail = handle
        .tail(
            job,
            None,
            JobTailLimits {
                bytes_per_stream: JobTailBytes::new(65_536).unwrap(),
                lines_per_stream: std::num::NonZeroU32::new(1).unwrap(),
            },
        )
        .await
        .expect("latest sealed tail");
    assert_eq!(tail.stdout.as_bytes(), vec![b'0'; 65_536]);
    assert!(tail.stdout_truncated);
    assert_eq!(tail.next.stdout, offset);
    assert_eq!(tail.stderr.as_bytes(), b"sealed-range-stderr\n");
    assert_eq!(tail.next.stderr, info.stderr.bytes);
    handle.quiesce().await.expect("quiesce smoke supervisor");
    handle
        .retire(cowshed_core::runtime::supervisor::NxDaemonRetirement::Recorded)
        .await
        .expect("retire smoke supervisor");
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

/// A job's environment with the per-workspace places of its own fixture replaced by fixed
/// names, and what differs per job or per port block left out, so two workspaces' shells can
/// be compared.
fn normalized_environment(workspace: &Workspace, env: &str) -> Vec<String> {
    let link = cowshed_core::sandbox::workspace_runtime_link(workspace.mount());
    let mount = workspace.mount().display().to_string();
    let root = workspace.root.display().to_string();
    let host_home = workspace.sandbox.home.display().to_string();
    let mut lines: Vec<String> = env
        .lines()
        .filter(|line| {
            let name = line.split('=').next().unwrap_or_default();
            !matches!(
                name,
                "DIRENV_DIFF" | "DIRENV_WATCHES" | "SHLVL" | "_" | "PWD" | "OLDPWD"
            ) && ![
                "PORT",
                "PROXY",
                "NX_SELF_HOSTED",
                "MAKEFLAGS",
                "BOUNDED_EXEC",
                "INSTALL_OBSERVED",
            ]
            .iter()
            // `http_proxy` and `https_proxy` carry the port block too.
            .any(|part| name.to_ascii_uppercase().contains(part))
        })
        .map(|line| {
            line.replace(&host_home, "<host-home>")
                .replace(&mount, "<mount>")
                .replace(&root, "<root>")
                .replace(&*link.to_string_lossy(), "<link>")
        })
        .collect();
    lines.sort();
    lines
}

/// A workspace of the fixture tree, with `envrc` as its shell, carrying the record `from`
/// carries, mounted `beside` another workspace when given (as sibling sheds are).
fn inheritor(
    label: &str,
    port: u16,
    envrc: &str,
    from: Option<&Workspace>,
    beside: Option<&Workspace>,
) -> Workspace {
    let mut workspace = match beside {
        Some(beside) => Workspace::new_beside(label, port, beside),
        None => Workspace::new(label, port),
    };
    // One host user: a clone shares its origin's identity home, as every workspace on a host
    // shares the user's (it names the installed cowshed directory on the sandbox PATH).
    if let Some(from) = from {
        workspace.sandbox.home = from.sandbox.home.clone();
    }
    std::fs::write(workspace.mount().join("bun.lock"), "v1").expect("lockfile");
    std::fs::write(
        workspace.mount().join(".envrc-local"),
        "export LOCAL_TOKEN=from-local\n",
    )
    .expect(".envrc-local");
    workspace.envrc(envrc);
    // The controller's marker: a clone's lineage names the workspace it was cloned from.
    let incarnation = uuid::Uuid::new_v4().simple().to_string();
    let lineage = match from {
        Some(from) => {
            let marker: serde_json::Value = serde_json::from_slice(
                &std::fs::read(from.mount().join(".cowshed/workspace.json")).expect("marker"),
            )
            .expect("marker JSON");
            serde_json::json!([marker["workspaceIncarnation"]])
        }
        None => serde_json::json!([]),
    };
    std::fs::write(
        workspace.mount().join(".cowshed/workspace.json"),
        serde_json::to_vec(&serde_json::json!({
            "version": 1,
            "repoId": "owner/repo",
            "projectRoot": "/p",
            "workspace": label,
            "workspaceIncarnation": incarnation,
            "role": "workspace",
            "baseCommit": "0",
            "createdAt": "2026-01-01T00:00:00Z",
            "forkedFrom": null,
            "createdTrace": "t",
            "lineage": lineage,
            "ownedLinksRevision": 1,
        }))
        .expect("marker bytes"),
    )
    .expect("marker");
    if let Some(from) = from {
        std::fs::create_dir_all(inherited_record(&workspace).parent().unwrap())
            .expect("host directory");
        std::fs::copy(inherited_record(from), inherited_record(&workspace))
            .expect("the clone carries the record");
    }
    workspace
}

fn inherited_record(workspace: &Workspace) -> PathBuf {
    workspace
        .mount()
        .join(".cowshed/shell-host/activation.json")
}

/// The equivalence the inherited shell rests on (11_shell.md, "Inherited shells"). The origin
/// evaluates; its first clone adopts the record it carries without evaluating, and a later
/// clone ends with exactly the environment its own evaluation gives, credentials and
/// flag-glued paths included.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_clone_adopts_its_origins_shell_and_matches_its_own_evaluation() {
    // Flags glue the path to the flag, as NIX_CFLAGS_COMPILE and NIX_LDFLAGS do.
    let envrc = "export AT_ROOT=\"$PWD/tool\"\n\
                 export FLAGS=\"-I$PWD/include -L$PWD/lib -Wl,-rpath,$PWD/lib\"\n\
                 watch_file bun.lock\nsource_env_if_exists .envrc-local\n";
    let origin = inheritor("inherit-origin", 41_488, envrc, None, None);
    run(&origin.supervisor(false), sh("true")).await.ok();
    assert_eq!(origin.activations(), 1);
    let written =
        std::fs::read_to_string(inherited_record(&origin)).expect("the activation left its export");
    assert!(
        !written.contains("from-local"),
        "no credential value is recorded"
    );

    let first = inheritor("inherit-first", 41_504, envrc, Some(&origin), None);
    let ran = run(&first.supervisor(false), sh("true")).await.ok();
    assert_eq!(
        first.activations(),
        0,
        "the first clone adopts its origin's record without evaluating: {}",
        ran.stderr
    );

    let probe = "env";
    // Beside the first clone, as sibling sheds are.
    let adopter = inheritor(
        "inherit-adopter",
        41_520,
        envrc,
        Some(&origin),
        Some(&first),
    );
    let adopted = run(&adopter.supervisor(false), sh(probe)).await.ok();
    assert_eq!(
        adopter.activations(),
        0,
        "a later clone adopts it too: {}",
        adopted.stderr
    );
    let evaluator = inheritor("inherit-evaluator", 41_536, envrc, None, None);
    let evaluated = run(&evaluator.supervisor(false), sh(probe)).await.ok();
    assert_eq!(evaluator.activations(), 1);
    assert_eq!(
        normalized_environment(&adopter, &adopted.stdout),
        normalized_environment(&evaluator, &evaluated.stdout)
    );

    // One changed input byte, and the clone evaluates for itself.
    let changed = inheritor(
        "inherit-changed",
        41_552,
        envrc,
        Some(&origin),
        Some(&first),
    );
    std::fs::write(changed.mount().join("bun.lock"), "v2").expect("lockfile");
    let ran = run(&changed.supervisor(false), sh("true")).await.ok();
    assert_eq!(changed.activations(), 1);
    assert!(
        ran.stderr
            .contains("evaluating the workspace shell: the inherited shell's inputs differ"),
        "{}",
        ran.stderr
    );
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
            AdmissionSource::Worker,
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

/// A warm command whose leader exits while a descendant holds the job's output open does not
/// outlive its job (11_shell.md, "Process identity and signal ownership"): leader exit begins
/// `draining`, retirement ends the descendant through the group the host still holds, and the job
/// ends with the leader's own exit and a receipt naming the descendant. The warm shell serves the
/// next command once the host released the command.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_an_exited_leaders_descendants_are_retired_before_the_job_ends() {
    let workspace = Workspace::new("shell-pool-exited-leader", 41_344);
    workspace.envrc("");
    let handle = workspace.supervisor(false);
    run(&handle, sh("true")).await.ok();
    let job = handle
        .exec(
            None,
            None,
            sh("sleep 300 & printf '%s\\n' $! > pid; exit 3"),
            AdmissionSource::Worker,
        )
        .await
        .expect("admit");
    let pid = workspace.mount().join("pid");
    // The job records its backgrounded descendant's pid in the file.
    let recorded = async {
        loop {
            if let Ok(text) = std::fs::read_to_string(&pid)
                && text.ends_with('\n')
            {
                return text;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }
    .await;
    let descendant: i32 = recorded.trim().parse().expect("pid");
    let info = handle.wait(job).await.expect("drained job");
    assert_eq!(
        (info.state, info.exit.as_ref()),
        (JobState::Exited, Some(&ExitStatus::Exited { code: 3 }))
    );
    assert!(
        info.outlived
            .iter()
            .any(|row| i32::try_from(row.pid).ok() == Some(descendant)),
        "the receipt names the descendant retirement ended: {info:?}"
    );
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(
        !process_alive(descendant),
        "retirement reached the exited leader's descendant"
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
    // A new revision never reuses a shell built under the old one: its pool starts fresh. With
    // the same grants and inputs, the new host adopts the export its own checkout already
    // evaluated (11_shell.md, "Inherited shells") instead of evaluating again.
    let advanced = handle
        .advance_authority(2, 1, workspace.sandbox.clone())
        .await
        .expect("advance the grant revision");
    let ran = run(&advanced, sh("true")).await.ok();
    assert_eq!(ran.info.grant_revision, 2);
    assert!(!ran.stderr.contains("direnv: loading"), "{}", ran.stderr);
    assert_eq!(
        workspace.activations(),
        1,
        "same grants, same inputs: adopted"
    );
    // Other grants are another baseline: what the evaluation could read differs, so it runs.
    let granted = workspace
        .root
        .parent()
        .expect("scratch parent")
        .join(format!("cowshed-granted-{}", uuid::Uuid::new_v4().simple()));
    std::fs::create_dir_all(&granted).expect("granted directory");
    let mut widened = workspace.sandbox.clone();
    widened.grants.read.push(granted.clone());
    let advanced = advanced
        .advance_authority(3, 1, widened)
        .await
        .expect("advance the grant revision again");
    let ran = run(&advanced, sh("true")).await.ok();
    assert_eq!(ran.info.grant_revision, 3);
    assert!(
        ran.stderr.contains("<grants> differs"),
        "the miss names the grants: {}",
        ran.stderr
    );
    assert_eq!(
        workspace.activations(),
        2,
        "no shell evaluated under other grants serves these"
    );
    std::fs::remove_dir_all(&granted).ok();
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
    assert_eq!(
        exited.info.failure, None,
        "a command that ran owns its exit"
    );

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

    // Proven absent before the spawn: the supervisor refuses it (not-found, wrapper code 102),
    // names the program, instead of the host's exec failing 127, and types it unlaunched.
    let missing = run(&handle, request(&["/no/such/program"])).await;
    assert_eq!(missing.info.exit, Some(ExitStatus::Exited { code: 102 }));
    assert_eq!(missing.info.failure, Some(JobFailure::Unlaunched));
    assert_eq!(
        handle
            .sealed(missing.info.job_id)
            .await
            .expect("its sealed record")
            .failure,
        Some(JobFailure::Unlaunched),
        "the sealed record keeps why the command never ran"
    );
    assert!(
        missing
            .stderr
            .contains("/no/such/program does not exist, so the command did not start"),
        "{}",
        missing.stderr
    );
    assert_eq!(
        workspace.activations(),
        1,
        "every command above ran in one warm shell"
    );
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
    let _job = handle
        .exec(None, None, sh("true"), AdmissionSource::Worker)
        .await
        .expect("admit");
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
        .status_bounded()
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

const ESCAPE_HELPER: &str = "COWSHED_SHELL_POOL_ESCAPE_HELPER";

/// The controller half of the test below: a supervisor whose job's activation runs held, with a
/// descendant that left the host's session, and that exits without running a destructor once
/// the test watches both, exactly as a stopped supervisor's process does.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_escape_helper_exits_while_an_activation_runs() {
    use std::io::Write as _;
    use tokio::io::AsyncBufReadExt as _;

    let Some(root) = std::env::var_os(ESCAPE_HELPER) else {
        return;
    };
    let workspace = Workspace::from_root(PathBuf::from(root));
    let handle = workspace.supervisor(false);
    let job = handle
        .exec(None, None, sh("true"), AdmissionSource::Worker)
        .await
        .expect("admit");
    let exit = workspace.sandbox.exec_temp_dir.join("helper-exit");
    let release = tokio::net::unix::pipe::OpenOptions::new()
        .open_receiver(&exit)
        .expect("open the helper release FIFO");
    let mut release = tokio::io::BufReader::new(release);
    let mut released = String::new();
    let mut offset = 0;
    let mut stderr_open = true;
    loop {
        tokio::select! {
            read = release.read_line(&mut released) => {
                read.expect("the test releases the helper");
                std::process::exit(0);
            }
            ended = handle.wait(job) => {
                let info = ended.expect("activation job outcome");
                let stderr = read_stream(&handle, &info, StreamKind::Stderr).await;
                panic!("activation ended before the test released its helper: {info:?}; stderr: {}", String::from_utf8_lossy(&stderr));
            }
            chunk = handle.log_read(job, StreamKind::Stderr, offset, true), if stderr_open => {
                let chunk = chunk.expect("follow activation stderr");
                std::io::stderr().write_all(&chunk.bytes).expect("report activation stderr");
                offset += u64::try_from(chunk.bytes.len()).expect("stream offset");
                stderr_open = !chunk.eof;
            }
        }
    }
}

/// The parent of process pid, as the kernel records it.
fn parent_of(pid: i32) -> i32 {
    let mut info = std::mem::MaybeUninit::<libc::proc_bsdinfo>::uninit();
    let size = libc::c_int::try_from(size_of::<libc::proc_bsdinfo>()).expect("size");
    // SAFETY: info provides size writable bytes.
    let written = unsafe {
        libc::proc_pidinfo(
            pid,
            libc::PROC_PIDTBSDINFO,
            0,
            info.as_mut_ptr().cast(),
            size,
        )
    };
    assert_eq!(
        written,
        size,
        "read the parent of {pid}: {}",
        std::io::Error::last_os_error()
    );
    // SAFETY: proc_pidinfo filled all size bytes.
    i32::try_from(unsafe { info.assume_init() }.pbi_ppid).expect("a pid")
}

/// An activation whose supervisor goes away ends with every process it started, a descendant
/// that called setsid and so left the host's process group included (11_shell.md, "Activation
/// belongs to the job that waited on it" and "Teardown ordering"). The host ends only once that
/// teardown is over, so its exit orders the checks: by then the escaped descendant has exited
/// and nothing holds the workspace mount.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_an_activation_s_escaped_descendant_ends_with_its_supervisor() {
    let workspace = Workspace::new("shell-pool-escape", 41_504);
    let tmp = workspace.sandbox.exec_temp_dir.clone();
    for name in [
        "escaped-pid",
        "escape-parent",
        "escape-release",
        "direnv-pid",
        "activation-hold",
        "helper-exit",
    ] {
        fifo(&tmp.join(name));
    }
    workspace.envrc(
        r#"/usr/bin/perl -MPOSIX -e 'defined(my $child = fork()) or die "fork: $!"; if ($child) { open(my $p, ">", "$ENV{TMPDIR}/escape-parent") or die "parent pid: $!"; print $p "$$\n"; close $p; open(my $release, "<", "$ENV{TMPDIR}/escape-release") or die "release: $!"; <$release>; exit 0; } POSIX::setsid() > 0 or die "setsid: $!"; open(my $f, ">", "$ENV{TMPDIR}/escaped-pid") or die "open: $!"; print $f "$$\n"; close $f; exec "/bin/sleep", "600" or die "exec: $!"' &
printf '%s\n' "$PPID" > "$TMPDIR/direnv-pid"
read _ < "$TMPDIR/activation-hold"
"#,
    );
    let mut helper = tokio::process::Command::new(std::env::current_exe().expect("test binary"))
        .args([
            "--exact",
            "host_controller_escape_helper_exits_while_an_activation_runs",
            "--ignored",
            "--nocapture",
        ])
        .env(ESCAPE_HELPER, &workspace.root)
        .kill_on_drop(true)
        .spawn()
        .expect("start the controller helper");
    let ready = async {
        let escaped = tmp.join("escaped-pid");
        let parent = tmp.join("escape-parent");
        let direnv = tmp.join("direnv-pid");
        tokio::join!(read_pid(&escaped), read_pid(&parent), read_pid(&direnv))
    };
    let (escaped, parent, direnv) = tokio::select! {
        ready = ready => ready,
        status = helper.wait_bounded() => panic!("activation helper exited before publishing its pids: {status:?}"),
    };
    let host = parent_of(direnv);
    // SAFETY: getpgid reads two process group ids.
    let (escaped_group, host_group) = unsafe { (libc::getpgid(escaped), libc::getpgid(host)) };
    assert_ne!(
        escaped_group, host_group,
        "the fixture's descendant {escaped} never left the host's group"
    );
    let watch = |pid: i32| {
        ExitWatch::of(u32::try_from(pid).expect("a pid"))
            .expect("watch an exit")
            .expect("the process runs")
    };
    let (escaped_exit, host_exit) = (watch(escaped), watch(host));
    let parent_exit = watch(parent);
    release(&tmp.join("escape-release")).await;
    assert!(
        tokio::task::spawn_blocking(move || parent_exit.within(Duration::from_secs(60)))
            .await
            .expect("parent watch task")
            .expect("watch parent"),
        "the fixture parent must exit"
    );
    assert_eq!(
        parent_of(escaped),
        1,
        "the escaped child must be reparented before teardown"
    );
    release(&tmp.join("helper-exit")).await;
    let status = helper.wait_bounded().await.expect("wait for the helper");
    assert!(status.success(), "helper: {status}");
    let host_ended = tokio::task::spawn_blocking(move || host_exit.within(Duration::from_secs(60)))
        .await
        .expect("host watch task")
        .expect("watch the host");
    assert!(
        host_ended,
        "the host {host} of a supervisor that exited kept running"
    );
    let escaped_ended = escaped_exit
        .within(Duration::ZERO)
        .expect("read the escaped exit");
    let holders = tree_holders(workspace.mount()).expect("list the mount's holders");
    if !escaped_ended {
        // SAFETY: a plain kill of this test's own fixture process, which still runs.
        unsafe { libc::kill(escaped, libc::SIGKILL) };
    }
    assert!(
        escaped_ended,
        "the activation's descendant {escaped}, outside the host's group, outlived its host {host}"
    );
    assert!(
        holders.is_empty(),
        "processes hold the workspace after its host ended: {holders:?}"
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
            AdmissionSource::Worker,
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
            AdmissionSource::Worker,
        )
        .await
        .expect("admit the running job");
    // The spare built ahead of demand under revision 1 exists before the revoke (it adopts the
    // export the first command's evaluation left, so its activation is what shows it).
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
    let finished = advanced.wait(running).await.expect("its outcome");
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

/// A grant's jobserver fifo the sandbox does not let the command open — the profile without its
/// pool directory, as a profile whose pool rules a later deny undid — is said on the job's stderr
/// before the command runs, naming the fifo and the error, and the command still runs, argv and
/// script alike. A job whose fifo opens hears nothing of it.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_jobserver_fifo_the_sandbox_refuses_is_reported_and_the_command_runs() {
    let mut workspace = Workspace::new("shell-pool-jobserver", 41_392);
    workspace.envrc("");
    let opened = workspace.supervisor(false);
    run(&opened, sh("true")).await.ok();
    let quiet = run(&opened, sh("printf ran")).await.ok();
    assert_eq!((quiet.stdout.as_str(), quiet.stderr.as_str()), ("ran", ""));

    workspace.sandbox.cpu_pool_directory = None;
    let refused = workspace.supervisor(false);
    let report = "cannot be opened in the sandbox (Operation not permitted (os error 1)); \
                  Cargo will run without the job's jobserver\n";
    for request in [sh("printf ran"), script(&["printf ran"], Vec::new())] {
        let ran = run(&refused, request).await.ok();
        assert_eq!(ran.stdout, "ran");
        assert!(
            ran.stderr
                .starts_with("cowshed: the job's CPU jobserver fifo ")
                && ran.stderr.ends_with(report),
            "{:?}",
            ran.stderr
        );
    }
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

/// The newline-terminated pid a job's process wrote into the FIFO at `path`, without a
/// blocking thread left behind if readiness fails or the controlling helper exits.
async fn read_pid(path: &Path) -> i32 {
    use tokio::io::AsyncBufReadExt as _;

    let receiver = tokio::net::unix::pipe::OpenOptions::new()
        .open_receiver(path)
        .unwrap_or_else(|error| panic!("open pid FIFO {}: {error}", path.display()));
    let mut receiver = tokio::io::BufReader::new(receiver);
    let mut text = String::new();
    let read = tokio::time::timeout(Duration::from_secs(60), receiver.read_line(&mut text)).await;
    match read {
        Ok(Ok(_)) => text
            .trim()
            .parse()
            .unwrap_or_else(|error| panic!("pid FIFO {} held {text:?}: {error}", path.display())),
        Ok(Err(error)) => panic!(
            "read pid FIFO {}: {error}; received {text:?}",
            path.display()
        ),
        Err(error) => panic!(
            "wait for pid FIFO {}: {error}; received {text:?}",
            path.display()
        ),
    }
}

/// Dropping a pending FIFO wait must not leave spawn_blocking(open) behind for Runtime::drop
/// to join forever. The biased select polls the real reader once before cancelling it.
#[tokio::test]
async fn a_cancelled_pid_fifo_wait_leaves_no_blocking_reader() {
    let root = LedgerRoot(std::env::temp_dir().join(format!(
        "cowshed-pid-reader-{}",
        uuid::Uuid::new_v4().simple()
    )));
    std::fs::create_dir(&root.0).expect("private FIFO fixture");
    let path = root.0.join("pid");
    fifo(&path);
    {
        let mut reader = std::pin::pin!(read_pid(&path));
        tokio::select! {
            biased;
            _ = &mut reader => panic!("a FIFO without a writer cannot publish a pid"),
            _ = std::future::ready(()) => {}
        }
    }
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

/// A job cancelled while its cold host activates for it ends Killed and its command never runs.
/// Nothing of the command is born yet, so no membership can prove it ended: the kill reaches the
/// activation through the job's control instead of an empty membership read.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_job_cancelled_while_its_host_activates_never_runs_its_command() {
    let workspace = Workspace::new("shell-pool-activation-cancel", 41_488);
    let tmp = workspace.sandbox.exec_temp_dir.clone();
    for name in ["activation-pid", "activation-hold"] {
        fifo(&tmp.join(name));
    }
    workspace.envrc(
        "printf '%s\\n' $$ > \"$TMPDIR/activation-pid\"\nread _ < \"$TMPDIR/activation-hold\"\n",
    );
    let handle = workspace.supervisor(false);
    let job = handle
        .exec(
            None,
            None,
            sh("printf ran > command-ran"),
            AdmissionSource::Worker,
        )
        .await
        .expect("admit");
    // The activation runs, held on its FIFO; no command of the job exists yet.
    read_pid(&tmp.join("activation-pid")).await;
    handle.kill(job).await.expect("kill the activating job");
    let info = handle.wait(job).await.expect("the cancelled job ends");
    assert_eq!(info.state, JobState::Killed, "{info:?}");
    assert!(
        !workspace.mount().join("command-ran").exists(),
        "a job cancelled while activating ran its command"
    );
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
            AdmissionSource::Worker,
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
    let command_pid = u32::try_from(read_pid(&tmp.join("command-pid")).await).expect("pid");
    let before = std::time::Instant::now();
    // A new leader is the supervisor's own sample boundary, not JobState::Running
    // and not this read. The read leaves the open window open.
    let running = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let sample = handle
                .resources(job)
                .await
                .expect("a running job is sampled");
            if sample.leader_pid == command_pid {
                break sample;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the new-leader sample names the command");
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
            AdmissionSource::Worker,
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
    // The pid file proves the command started. The sample that names its
    // leader and held members is the supervisor's own boundary, not that write.
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let mut sample = handle
                .resources(job)
                .await
                .expect("a running job is sampled");
            sample.members.sort_unstable();
            if sample.leader_pid == leader && sample.members == group {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the command's own sample names its leader and both held children");

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
        Some(JobAccounting::MacOsRusageChildren { cpu, io: None, .. }) => {
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

/// A coordinator's host gate runs its argv at the host boundary, outside the workspace sandbox:
/// it writes where no executed child may, with its own jobserver count. A worker cannot ask for
/// it, and nothing of a refused gate runs.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_coordinator_host_gate_runs_outside_the_sandbox() {
    let workspace = Workspace::new("cpu-host-gate", 41_456);
    let handle = workspace.supervisor(false);
    let outside = workspace.root.join("host-gate-wrote");
    // Only public names reach a host gate: the path rides in the script, not the environment.
    let host = || ExecRequest {
        class: ExecClass::Gate,
        scope: ExecScope::HostGate,
        want: std::num::NonZeroUsize::new(2),
        ..sh(&format!(
            "printf host > '{}'; printf '%s' \"$BOUNDED_EXEC_CPU_TOKENS\"",
            outside.display()
        ))
    };

    let sandboxed = run(
        &handle,
        sh(&format!("printf sandboxed > '{}'", outside.display())),
    )
    .await;
    assert_ne!(
        sandboxed.info.exit,
        Some(ExitStatus::Exited { code: 0 }),
        "an executed child may not write outside its workspace"
    );

    let refused = handle
        .exec(None, None, host(), AdmissionSource::Worker)
        .await
        .expect_err("a worker cannot reach the host boundary");
    assert_eq!(refused.code, cowshed_core::ErrorCode::SandboxDenied);
    assert!(!outside.exists(), "the refused gate ran nothing");

    let job = handle
        .exec(None, None, host(), AdmissionSource::Coordinator)
        .await
        .expect("a coordinator's host gate is admitted");
    // The job's end is the event; the leg's no-progress bound decides a hang.
    let info = handle.wait(job).await.expect("job outcome");
    let stdout = read_stream(&handle, &info, StreamKind::Stdout).await;
    assert_eq!(info.exit, Some(ExitStatus::Exited { code: 0 }));
    assert_eq!(
        String::from_utf8_lossy(&stdout),
        "2",
        "its jobserver holds its want"
    );
    assert_eq!(
        std::fs::read_to_string(&outside).expect("the host gate wrote outside the sandbox"),
        "host"
    );
    assert_eq!(
        (info.class, info.scope),
        (ExecClass::Gate, ExecScope::HostGate)
    );
}

/// Real direnv activation and a warm worker retain project-defined/custom request variables.
/// Their plaintext never enters JobAdmission.env; the full authored request is bound by digest.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_worker_custom_environment_runs_but_is_absent_from_admission_records() {
    let workspace = Workspace::new("record-only-worker-env", 41_472);
    workspace.envrc("export PROJECT_ENVRC_CUSTOM=activated-custom-value\n");
    let handle = workspace.supervisor(false);
    let mut ask = sh(
        "printf '%s\\0' \"$PROJECT_ENVRC_CUSTOM\" \"$PROJECT_REQUEST_CUSTOM\" | /usr/bin/shasum -a 256",
    );
    ask.env.insert("LANG".to_owned(), "C".to_owned());
    ask.env.insert(
        "PROJECT_REQUEST_CUSTOM".to_owned(),
        "authored-custom-value".to_owned(),
    );
    ask.admission_key = Some(cowshed_core::api::AdmissionKey::new("custom-worker-env").unwrap());
    let ran = run(&handle, ask).await.ok();
    let expected = cowshed_core::api::Sha256Digest::compute(
        b"activated-custom-value\0authored-custom-value\0",
    );
    assert_eq!(
        ran.stdout,
        format!("{}  -\n", expected.to_hex()),
        "both actual worker values reached the child"
    );
    assert_eq!(workspace.activations(), 1, "the real .envrc was evaluated");
    let records = workspace.mount().join(".cowshed/job/records.arrow");
    let history = cowshed_core::storage::job_artifact::recover_records(&records).unwrap();
    assert!(!history.frames.is_empty());
    for frame in history.frames {
        let record = frame.record;
        let admitted = record.admission.expect("a keyed worker record");
        assert_eq!(
            admitted.env,
            std::collections::BTreeMap::from([("LANG".to_owned(), "C".to_owned())])
        );
        assert_ne!(
            admitted.env_sha256,
            cowshed_core::api::Sha256Digest::compute(&[])
        );
    }
    let bytes = std::fs::read(&records).unwrap();
    assert!(
        !bytes
            .windows(b"authored-custom-value".len())
            .any(|part| part == b"authored-custom-value")
    );
}

async fn activation_queued(handle: &WorkspaceSupervisorHandle, job: JobId) -> bool {
    let marker = b"cowshed: waiting for this workspace's shell evaluation";
    let mut stderr = Vec::new();
    let mut offset = 0;
    loop {
        let chunk = handle
            .log_read(job, StreamKind::Stderr, offset, true)
            .await
            .expect("activation diagnostics");
        stderr.extend_from_slice(&chunk.bytes);
        offset = chunk.next_offset;
        if stderr.windows(marker.len()).any(|part| part == marker) {
            return true;
        }
        if chunk.eof {
            return false;
        }
    }
}

/// Discovery and an ordinary command enter one canonical shell, with one real evaluation.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_cargo_discovery_shares_the_canonical_activation() {
    use cowshed_core::capabilities::{BuildStatePath, JobCargo, discover_build_state};
    use std::os::unix::fs::PermissionsExt as _;

    let workspace = Workspace::new("shell-discovery-shared", 41_616);
    let mount = workspace.mount();
    let tmp = &workspace.sandbox.exec_temp_dir;
    for name in ["evaluation-ready", "evaluation-release"] {
        fifo(&tmp.join(name));
    }
    std::fs::write(
        mount.join(".cowshed/bin/cargo"),
        "#!/bin/sh\necho 'bootstrap cargo is not the project toolchain' >&2\nexit 101\n",
    )
    .expect("bootstrap cargo");
    std::fs::set_permissions(
        mount.join(".cowshed/bin/cargo"),
        std::fs::Permissions::from_mode(0o755),
    )
    .expect("executable bootstrap cargo");
    let real_cargo =
        std::fs::canonicalize(std::env::var_os("CARGO").expect("Cargo publishes its driver"))
            .expect("real Cargo driver");
    std::fs::create_dir(mount.join("toolchain")).expect("toolchain directory");
    std::os::unix::fs::symlink(real_cargo, mount.join("toolchain/cargo")).expect("project cargo");
    workspace.envrc(
        r#"if [ ! -e "$TMPDIR/evaluation-entered" ]; then
  : > "$TMPDIR/evaluation-entered"
  printf '%s\n' $$ > "$TMPDIR/evaluation-ready"
  read _ < "$TMPDIR/evaluation-release"
fi
PATH_add "$PWD/toolchain"
"#,
    );
    std::fs::write(
        mount.join("Cargo.toml"),
        "[package]\nname = \"probe\"\nversion = \"0.1.0\"\nedition = \"2021\"\n",
    )
    .expect("manifest");
    std::fs::create_dir(mount.join("src")).expect("source directory");
    std::fs::write(mount.join("src/lib.rs"), "pub fn value() -> u8 { 1 }\n").expect("library");
    for args in [&["init", "--quiet"][..], &["add", "Cargo.toml"]] {
        let output = std::process::Command::new("git")
            .args(args)
            .current_dir(mount)
            .output_bounded()
            .expect("fixture git");
        assert!(
            output.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&output.stderr)
        );
    }
    let handle = workspace.supervisor(true);
    let (server, socket) = FixtureServer::start(handle.clone()).await;
    let sandbox = workspace.sandbox.clone();
    let canonical = handle.clone();
    let runtime = tokio::runtime::Handle::current();
    let (_cancel, cancelled) = tokio::sync::oneshot::channel::<()>();
    let mut discovery = tokio::task::spawn_blocking(move || {
        let mut cargo = JobCargo::new(&sandbox, &canonical, &socket, None, runtime, cancelled);
        sandbox.with_detection_context(&sandbox.workspace_mount, |context| {
            discover_build_state(context, &mut cargo)
        })
    });
    let ready = tmp.join("evaluation-ready");
    tokio::select! {
        _ = read_pid(&ready) => {},
        ended = &mut discovery => panic!("discovery ended before activation readiness: {ended:?}"),
    }
    let normal = handle
        .exec(
            None,
            None,
            sh("printf canonical"),
            AdmissionSource::Coordinator,
        )
        .await
        .expect("ordinary command");
    let queued = activation_queued(&handle, normal).await;
    release(&tmp.join("evaluation-release")).await;
    let discovered = discovery.await.expect("discovery task").expect("discovery");
    let normal = handle.wait(normal).await.expect("ordinary command ends");
    drop(server);
    assert_eq!(
        normal.exit,
        Some(ExitStatus::Exited { code: 0 }),
        "{normal:?}"
    );
    assert!(
        queued,
        "the second cold host evaluated instead of waiting for canonical publication"
    );
    assert!(discovered.findings.is_empty(), "{:?}", discovered.findings);
    assert_eq!(
        discovered.paths,
        [BuildStatePath::new("target", "target").expect("target path")]
    );
    assert_eq!(
        workspace.activations(),
        1,
        "discovery, ordinary work and the spare share one evaluation"
    );
}

#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_cancelling_a_waiting_activation_keeps_its_producer_alive() {
    let workspace = Workspace::new("shell-activation-wait-cancel", 41_632);
    let tmp = &workspace.sandbox.exec_temp_dir;
    for name in ["producer-ready", "producer-release"] {
        fifo(&tmp.join(name));
    }
    workspace.envrc(
        r#"if [ ! -e "$TMPDIR/producer-entered" ]; then
  : > "$TMPDIR/producer-entered"
  printf '%s\n' $$ > "$TMPDIR/producer-ready"
  read _ < "$TMPDIR/producer-release"
fi
export FROM_ENVRC=shared
"#,
    );
    let handle = workspace.supervisor(false);
    let producer = handle
        .exec(
            None,
            None,
            sh("printf producer"),
            AdmissionSource::Coordinator,
        )
        .await
        .expect("producer job");
    read_pid(&tmp.join("producer-ready")).await;
    let waiter = handle
        .exec(
            None,
            None,
            sh("printf waiter"),
            AdmissionSource::Coordinator,
        )
        .await
        .expect("waiting job");
    let queued = activation_queued(&handle, waiter).await;
    let killed = handle.kill(waiter).await;
    let cancelled = handle
        .wait(waiter)
        .await
        .expect("waiter retires while producer is held");
    release(&tmp.join("producer-release")).await;
    let produced = handle.wait(producer).await.expect("producer ends");
    killed.expect("cancel waiter");
    assert!(
        queued,
        "the waiter must observe the shared evaluation before cancellation"
    );
    assert_eq!(cancelled.state, JobState::Killed, "{cancelled:?}");
    assert_eq!(
        produced.exit,
        Some(ExitStatus::Exited { code: 0 }),
        "{produced:?}"
    );
    assert_eq!(
        run(&handle, sh("printf %s \"$FROM_ENVRC\""))
            .await
            .ok()
            .stdout,
        "shared"
    );
    assert_eq!(
        workspace.activations(),
        1,
        "cancelling a waiter neither cancels nor repeats its producer"
    );
}

struct FixtureServer {
    task: tokio::task::JoinHandle<Result<()>>,
    _root: LedgerRoot,
}

impl FixtureServer {
    async fn start(handle: WorkspaceSupervisorHandle) -> (Self, PathBuf) {
        // bind publishes through a 32-hex staging name, so keep its parent short too.
        let parent = std::fs::canonicalize("/tmp").expect("canonical socket parent");
        let root = LedgerRoot(parent.join(format!("cs-{}", uuid::Uuid::new_v4().simple())));
        std::fs::create_dir(&root.0).expect("private socket directory");
        let socket = root.0.join("s");
        let listener = cowshed_core::runtime::supervisor_socket::bind(&socket)
            .await
            .expect("canonical socket");
        let task = tokio::spawn(cowshed_core::runtime::supervisor_socket::serve(
            listener, handle, None, None,
        ));
        (Self { task, _root: root }, socket)
    }
}

impl Drop for FixtureServer {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// A dropped capture owns a real socket hold, so the canonical job ends with its requester.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_a_dropped_project_tool_future_ends_its_canonical_job() {
    use cowshed_core::runtime::supervisor::{
        JobRunState, RunControl, project_tool_request, run_in_supervisor,
    };
    let workspace = Workspace::new("shell-tool-drop", 41_648);
    let tmp = &workspace.sandbox.exec_temp_dir;
    for name in ["tool-ready", "tool-hold"] {
        fifo(&tmp.join(name));
    }
    workspace.envrc("");
    let handle = workspace.supervisor(false);
    let (server, socket) = FixtureServer::start(handle.clone()).await;
    let request = project_tool_request(
        workspace.mount(),
        workspace.mount(),
        vec![
            "/bin/sh".into(),
            "-c".into(),
            "printf '%s\\n' $$ > \"$TMPDIR/tool-ready\"; read _ < \"$TMPDIR/tool-hold\"".into(),
        ],
    )
    .expect("tool request");
    let canonical = handle.clone();
    let owned = tokio::spawn(async move {
        let observed = JobRunState::default();
        let (keep_alive, cancelled) = tokio::sync::oneshot::channel();
        let output = run_in_supervisor(
            &canonical,
            &socket,
            None,
            request,
            RunControl {
                silence: cowshed_os::deadline::HEARTBEAT_SILENCE,
                queued: Box::new(|| None),
                cancelled,
                observed: &observed,
            },
        )
        .await;
        drop(keep_alive);
        output
    });
    let pid = read_pid(&tmp.join("tool-ready")).await;
    let exited = ExitWatch::of(u32::try_from(pid).expect("tool pid"))
        .expect("observe tool exit")
        .expect("tool is held alive");
    let job = handle
        .list()
        .await
        .expect("canonical job list")
        .into_iter()
        .next()
        .expect("admitted tool")
        .job_id;
    owned.abort();
    assert!(
        owned
            .await
            .expect_err("capture task was dropped")
            .is_cancelled()
    );
    let ended = handle.wait(job).await.expect("socket EOF retires the tool");
    assert_eq!(ended.state, JobState::Killed, "{ended:?}");
    assert!(
        exited
            .within(Duration::ZERO)
            .expect("read the retained exit")
    );
    drop(server);
}

/// Pool identity includes the selected envrc; a root export is not a nested shell.
#[tokio::test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
async fn host_controller_each_selected_envrc_has_its_own_activation() {
    let workspace = Workspace::new("shell-selected-envrc", 41_664);
    workspace.envrc("export FROM_ENVRC=root\n");
    std::fs::create_dir(workspace.mount().join("nested")).expect("nested shell directory");
    std::fs::write(
        workspace.mount().join("nested/.envrc"),
        format!("{COUNT}export FROM_ENVRC=nested\n"),
    )
    .expect("nested envrc");
    let handle = workspace.supervisor(false);
    assert_eq!(
        run(&handle, sh("printf %s \"$FROM_ENVRC\""))
            .await
            .ok()
            .stdout,
        "root"
    );
    let mut nested = sh("printf %s \"$FROM_ENVRC\"");
    nested.cwd = Some(cowshed_core::api::dto::WorkspacePath::new("nested").expect("nested cwd"));
    assert_eq!(run(&handle, nested).await.ok().stdout, "nested");
    assert_eq!(
        workspace.activations(),
        2,
        "different selected shells each evaluate once"
    );
}
