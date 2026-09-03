//! Who decides `CARGO_INCREMENTAL` for a sandboxed child.
//!
//! Cargo decides it per profile, and cowshed leaves that decision alone: an interactive
//! `cowshed exec` build keeps `dev` incremental and local, which is the difference between a
//! one-line edit rebuilding in seconds and rebuilding in tens of seconds. The one context that
//! wants the opposite is a non-interactive acceptance check (`cowshed land --check`), and it says
//! so by name through `acceptance_check_environment` rather than by everyone else paying for it.
//!
//! Both halves run the REAL spawn path — `sandbox-exec`, the executed-child profile, the
//! environment `sandboxed_command` builds — and read the answer off a child that prints its own
//! environment. The sccache variables are asserted alongside because they are the ones the
//! sandbox still owns unconditionally: the child must not be able to lose them, and a caller
//! must not be able to override them.

#![cfg(target_os = "macos")]

use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::{Path, PathBuf};

use cowshed_core::api::{ExitStatus, JobId};
use cowshed_core::metadata::{PortBlock, WorkspaceIncarnation, WorkspaceName};
use cowshed_core::repository::RepoId;
use cowshed_core::runtime::supervisor::{
    ProcessEvent, ProcessSpawnRequest, SpawnSink, SystemSpawnSink, WorkspaceAuthoritySnapshot,
    acceptance_check_environment,
};
use cowshed_core::sandbox::{
    RunSandboxMode, SandboxConfig, SandboxGrants, SandboxProfileRole, seatbelt_profile,
};
use cowshed_core::storage::job_artifact::StreamKind;
use cowshed_core::workspace_credentials::WORKSPACE_TOKEN_PATH;
use cowshed_gateway_types::WorkspaceToken;
use tokio::sync::mpsc;

/// `${VAR-unset}` distinguishes absent from present-and-empty, which is the whole question here:
/// a forced `CARGO_INCREMENTAL=0` and an unset one are both "not 1" and only one of them lets
/// cargo choose.
const PRINT_BUILD_ENV: &str = r#"printf '%s|%s|%s' "${CARGO_INCREMENTAL-unset}" "${RUSTC_WRAPPER-unset}" "${SCCACHE_BASEDIR_CWD-unset}""#;

fn scratch(label: &str) -> PathBuf {
    let alias = std::env::temp_dir().join(format!(
        "cowshed-{label}-{}-{}",
        std::process::id(),
        uuid::Uuid::new_v4().simple()
    ));
    std::fs::create_dir_all(&alias).expect("scratch root");
    // Seatbelt matches resolved paths; `/var/folders` is a symlink into `/private/var`.
    std::fs::canonicalize(&alias).expect("canonical scratch root")
}

/// `sandbox_runtime_link` names `/tmp/cs-<port base>`, so two workspaces sharing a base fight
/// over one host-wide symlink. These bases sit at the top of the macOS block grid, where the
/// allocator only reaches after 512 live workspaces: a test must never replace the runtime link
/// of a workspace someone is working in.
const TOP_OF_GRID: u16 = 49_136;

fn workspace(root: &Path, port_base: u16) -> SandboxConfig {
    let mount = root.join("workspace");
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
    SandboxConfig {
        home,
        mount_root: root.to_path_buf(),
        workspace_mount: mount,
        exec_temp_dir,
        port_block: PortBlock::new(port_base, 16).expect("port block"),
        mode: RunSandboxMode::ReadWrite,
        grants: SandboxGrants::default(),
        allowed_unix_sockets: Vec::new(),
        additional_denies: Vec::new(),
        git_worktree_repository: None,
    }
}

fn authority() -> WorkspaceAuthoritySnapshot {
    WorkspaceAuthoritySnapshot {
        repo_id: RepoId::parse("acme/widget").expect("repo id"),
        workspace: WorkspaceName::new("raven").expect("workspace name"),
        workspace_incarnation: WorkspaceIncarnation::new("0198f2c0b7e34dc795f17b238b331c80")
            .expect("workspace incarnation"),
        grant_revision: 1,
        lifecycle_revision: 1,
    }
}

/// Run `sh -c script` as a sandboxed child of `sandbox` with exactly `env` from the caller, and
/// return what it wrote to stdout.
async fn sandboxed_stdout(sandbox: &SandboxConfig, env: BTreeMap<String, String>) -> String {
    let (events, mut received) = mpsc::channel(64);
    let process = SystemSpawnSink
        .spawn(
            ProcessSpawnRequest {
                authority: authority(),
                job_id: JobId::new(1).expect("job id"),
                argv: vec![
                    OsString::from("/bin/sh"),
                    OsString::from("-c"),
                    OsString::from(PRINT_BUILD_ENV),
                ],
                cwd: sandbox.workspace_mount.clone(),
                env,
                devenv_dir: None,
                sandbox: sandbox.clone(),
                trusted_supervisor_profile: seatbelt_profile(
                    sandbox,
                    SandboxProfileRole::TrustedSupervisor,
                )
                .expect("trusted-supervisor profile"),
                executed_child_profile: seatbelt_profile(
                    sandbox,
                    SandboxProfileRole::ExecutedChild,
                )
                .expect("executed-child profile"),
            },
            events,
        )
        .await
        .expect("the child spawns through the sandbox");
    // The stdin pump holds an event sender until the process handle is dropped, so the channel
    // would never close while this test still owns one. Nothing here writes stdin.
    drop(process);

    let mut stdout = Vec::new();
    let mut stderr = Vec::new();
    let mut exit = None;
    while let Some(event) = received.recv().await {
        match event {
            ProcessEvent::Output { stream, bytes, .. } => match stream {
                StreamKind::Stdout => stdout.extend_from_slice(&bytes),
                StreamKind::Stderr => stderr.extend_from_slice(&bytes),
            },
            ProcessEvent::Exited { exit: status, .. } => exit = Some(status),
            ProcessEvent::WaitFailed { error, .. } => panic!("the child was never reaped: {error}"),
            _ => {}
        }
    }
    assert_eq!(
        exit,
        Some(ExitStatus::Exited { code: 0 }),
        "the child must exit 0 inside the sandbox; stderr: {}",
        String::from_utf8_lossy(&stderr)
    );
    String::from_utf8(stdout).expect("the child prints its environment as UTF-8")
}

/// The interactive contract: cargo owns `CARGO_INCREMENTAL`, the sandbox owns sccache.
#[tokio::test]
async fn an_ordinary_sandboxed_child_is_handed_sccache_and_no_incremental_policy() {
    let root = scratch("build-env-exec");
    let sandbox = workspace(&root, TOP_OF_GRID);

    assert_eq!(
        sandboxed_stdout(&sandbox, BTreeMap::new()).await,
        "unset|sccache|1",
        "an ordinary `cowshed exec` child must reach cargo with no CARGO_INCREMENTAL at all"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// The acceptance contract: the check's own environment decides, and it cannot buy its way out of
/// the sccache wiring the sandbox owns.
#[tokio::test]
async fn an_acceptance_check_carries_its_non_incremental_policy_into_the_child() {
    let root = scratch("build-env-check");
    let sandbox = workspace(&root, TOP_OF_GRID - 16);

    assert_eq!(
        sandboxed_stdout(
            &sandbox,
            acceptance_check_environment().into_iter().collect()
        )
        .await,
        "0|sccache|1",
        "a land check asks for non-incremental units by name, and still gets the sandbox's sccache"
    );

    std::fs::remove_dir_all(&root).ok();
}

/// A caller cannot unwire sccache: those two are the sandbox's, applied after the caller's
/// environment, and a workspace that spawned its own server would sit outside the boundary the
/// Seatbelt profile draws.
#[tokio::test]
async fn a_caller_cannot_override_the_sandbox_owned_sccache_wiring() {
    let root = scratch("build-env-override");
    let sandbox = workspace(&root, TOP_OF_GRID - 32);

    let hostile = BTreeMap::from([
        ("RUSTC_WRAPPER".to_owned(), "/bin/false".to_owned()),
        ("SCCACHE_BASEDIR_CWD".to_owned(), "0".to_owned()),
    ]);
    assert_eq!(
        sandboxed_stdout(&sandbox, hostile).await,
        "unset|sccache|1",
        "the sandbox's sccache wiring wins over anything the caller names"
    );

    std::fs::remove_dir_all(&root).ok();
}
