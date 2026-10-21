//! What the host-installed CLI can and cannot do from inside a workspace job's sandbox: build-state
//! is checkout-local, and no exec of the coordinator route is reachable.
#![cfg(target_os = "macos")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use cowshed_core::build_volume::{BUILD_LINK, BuildVolumeState};
use cowshed_core::capabilities::BuildStatePath;
use cowshed_core::exec::{ProfileLaunch, SANDBOX_EXEC};
use cowshed_core::git::git_command_at;
use cowshed_core::host_dirs::support_directory;
use cowshed_core::metadata::PortBlock;
use cowshed_core::sandbox::{
    RunSandboxMode, SandboxConfig, SandboxGrants, SandboxProfileRole, seatbelt_profile,
};

struct Scratch(PathBuf);

impl Drop for Scratch {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            if std::thread::panicking() {
                eprintln!("remove build-state fixture {}: {error}", self.0.display());
            } else {
                panic!("remove build-state fixture {}: {error}", self.0.display());
            }
        }
    }
}

/// Launch `argv` as every launcher does (04_sandbox.md, "Exec pipeline" step 4): the profile
/// from its file in the fixture's own store, its build volume bound with `-D`. The text names
/// the volume only as a parameter; launched bare (`-p`), sandbox-exec refuses it, exit 65.
fn job(config: &SandboxConfig, tools: &Path, argv: &[&str]) -> Output {
    let profile = seatbelt_profile(config, SandboxProfileRole::ExecutedChild).unwrap();
    let launch = ProfileLaunch::write(&config.profile_directory(), &profile).unwrap();
    Command::new(SANDBOX_EXEC)
        .args(launch.args())
        .args(argv)
        .current_dir(&config.workspace_mount)
        .env_clear()
        .env("HOME", config.workspace_mount.join(".cowshed/home"))
        .env("TMPDIR", &config.exec_temp_dir)
        .env("PATH", tools)
        .output()
        .expect("run executed-child profile")
}

/// A checkout with a build volume, the integration-test binary installed as the host's cowshed,
/// and the executed-child sandbox of a read-write job in it, with the support directory the
/// install lives in. The root is outside the protected cowshed store, independently of the
/// runner's TMPDIR.
fn installed_cli_fixture(label: &str) -> (Scratch, SandboxConfig, PathBuf) {
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let scratch = PathBuf::from(format!(
        "/private/tmp/cowshed-{label}-cli-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir(&scratch).unwrap();
    let root = Scratch(scratch.clone());
    let checkout = scratch.join("mounts/workspace");
    let home = scratch.join("home");
    let temporary = scratch.join("tmp");
    let volume = scratch.join("mounts/.build/fixture/repo/0123456789abcdef0123456789abcdef");
    for directory in [
        checkout.join(".cowshed/home"),
        home.clone(),
        temporary.clone(),
        volume.clone(),
    ] {
        fs::create_dir_all(directory).unwrap();
    }
    let initialized = git_command_at(&checkout)
        .args(["init", "-q"])
        .output()
        .unwrap();
    assert!(initialized.status.success());
    assert!(
        checkout.join(".git").is_dir(),
        "fixture owns a standalone git directory"
    );
    std::os::unix::fs::symlink(&volume, checkout.join(BUILD_LINK)).unwrap();
    let state = BuildVolumeState {
        paths: vec![BuildStatePath::new(".nx/cache", "nx/cache").unwrap()],
        fingerprint: None,
    };
    state.write(&volume).unwrap();

    let support = support_directory(&home);
    let installed = support.join("bin/cowshed-test-build");
    fs::create_dir_all(installed.parent().unwrap()).unwrap();
    let binary = std::env::var_os("CARGO_BIN_EXE_cowshed")
        .expect("cargo and nextest provide the integration-test binary at runtime");
    fs::copy(binary, &installed).unwrap();
    std::os::unix::fs::symlink("cowshed-test-build", support.join("bin/cowshed")).unwrap();

    let mut config = SandboxConfig {
        home,
        mount_root: scratch.join("mounts"),
        workspace_mount: checkout,
        exec_temp_dir: temporary,
        store_root: scratch.join("store"),
        cpu_pool_directory: None,
        port_block: PortBlock::new(40_960, 16).unwrap(),
        retained_port_blocks: Vec::new(),
        mode: RunSandboxMode::ReadWrite,
        grants: SandboxGrants::default(),
        allowed_unix_sockets: Vec::new(),
        additional_denies: Vec::new(),
        shed_links: Vec::new(),
        git_worktree_repository: None,
        build_volume_mount: Some(volume),
        repository_caches: Vec::new(),
        capabilities: Default::default(),
    };
    config.configure_capabilities().unwrap();
    assert!(
        config.allowed_unix_sockets.is_empty(),
        "no controller socket is admitted"
    );
    (root, config, support)
}

/// The job's bootstrap tool directory for its mode, holding the capability's programs.
fn job_tools(config: &SandboxConfig) -> PathBuf {
    let tools = match config.mode {
        RunSandboxMode::ReadWrite => config.workspace_mount.join(".cowshed/tools/bin"),
        RunSandboxMode::ReadOnly => config.exec_temp_dir.join("tools/bin"),
    };
    fs::create_dir_all(&tools).unwrap();
    for program in &config.capabilities.contribution.bootstrap_programs {
        std::os::unix::fs::symlink(&program.target, tools.join(program.name)).unwrap();
    }
    tools
}

#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_build_state_answers_with_only_the_installed_executable_grant() {
    let (_root, mut config, support) = installed_cli_fixture("state");
    let secret = support.join("controller-state");
    fs::write(&secret, "controller sentinel").unwrap();
    for mode in [RunSandboxMode::ReadWrite, RunSandboxMode::ReadOnly] {
        config.mode = mode;
        let tools = job_tools(&config);
        let answer = job(
            &config,
            &tools,
            &["/usr/bin/env", "cowshed", "build-state", "--json"],
        );
        assert!(
            answer.status.success(),
            "{mode:?}: status={}, stdout={}, stderr={}",
            answer.status,
            String::from_utf8_lossy(&answer.stdout),
            String::from_utf8_lossy(&answer.stderr)
        );
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&answer.stdout).unwrap(),
            serde_json::json!({"paths": [{"checkout": ".nx/cache", "volume": "nx/cache"}]})
        );
        let denied = job(&config, &tools, &["/bin/cat", secret.to_str().unwrap()]);
        assert!(
            !denied.status.success(),
            "controller data must stay unreadable"
        );
        assert!(String::from_utf8_lossy(&denied.stderr).contains("Operation not permitted"));
        let denied = job(
            &config,
            &tools,
            &["/bin/ls", support.join("bin").to_str().unwrap()],
        );
        assert!(
            !denied.status.success(),
            "executable grant must not list its directory"
        );
        assert!(String::from_utf8_lossy(&denied.stderr).contains("Operation not permitted"));
    }
}

/// The CLI is the coordinator route (07_api.md, "CLI and API exec parity"), so a workspace's own
/// process must not reach it: the boundary is who can ask, never the class asked for. A job runs
/// the installed CLI (build-state above) but its sandbox denies cowshed's store, where every
/// project's records, the daemon's socket and every workspace supervisor's socket live, and
/// admits no controller socket (04_sandbox.md). So `exec --host` from inside a job is refused at
/// every class at the CLI's first write into that store, before any route is asked, and the
/// refusal says so: sandbox-denied, exit 6, naming the denied path. The host command never starts.
#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_a_jobs_cli_is_refused_the_host_scope_at_every_class() {
    let (root, config, _support) = installed_cli_fixture("host-route");
    let tools = job_tools(&config);
    let marker = root.0.join("ran-on-the-host");
    let checkout = config.workspace_mount.to_str().unwrap().to_owned();
    for class in [
        "background",
        "interactive",
        "gate",
        "resident",
        "interactive-resident",
    ] {
        let refused = job(
            &config,
            &tools,
            &[
                "/usr/bin/env",
                "cowshed",
                "--project",
                &checkout,
                "exec",
                "workspace",
                "--host",
                "--class",
                class,
                "--",
                "/usr/bin/touch",
                marker.to_str().unwrap(),
            ],
        );
        let stderr = String::from_utf8_lossy(&refused.stderr);
        assert_eq!(
            refused.status.code(),
            Some(6),
            "{class}: refused as sandbox-denied; stdout={}, stderr={stderr}",
            String::from_utf8_lossy(&refused.stdout)
        );
        assert!(
            stderr.contains("sandboxed away from cowshed's store")
                && stderr.contains(cowshed_gateway_types::STORE_ROOT),
            "{class}: the refusal names the sandbox and the store path it denied: {stderr}"
        );
        assert!(!marker.exists(), "{class}: the host command ran");
    }
}
