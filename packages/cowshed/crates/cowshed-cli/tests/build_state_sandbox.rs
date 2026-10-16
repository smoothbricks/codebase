//! The build-state CLI is checkout-local even when its executable is host-installed.
#![cfg(target_os = "macos")]

use std::fs;
use std::path::{Path, PathBuf};
use std::process::{Command, Output};

use cowshed_core::build_volume::{BUILD_LINK, BuildVolumeState};
use cowshed_core::capabilities::BuildStatePath;
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

fn job(config: &SandboxConfig, tools: &Path, argv: &[&str]) -> Output {
    Command::new("/usr/bin/sandbox-exec")
        .args([
            "-p",
            &seatbelt_profile(config, SandboxProfileRole::ExecutedChild).unwrap(),
            "--",
        ])
        .args(argv)
        .current_dir(&config.workspace_mount)
        .env_clear()
        .env("HOME", config.workspace_mount.join(".cowshed/home"))
        .env("TMPDIR", &config.exec_temp_dir)
        .env("PATH", tools)
        .output()
        .expect("run executed-child profile")
}

#[test]
#[ignore = "host-controller authority: nx run cowshed:host-controller-test outside every cow sandbox"]
fn host_controller_build_state_answers_with_only_the_installed_executable_grant() {
    // Host HOME must be outside the protected cowshed store, independently of the runner's TMPDIR.
    let nonce = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let root = Scratch(PathBuf::from(format!(
        "/private/tmp/cowshed-state-cli-{}-{nonce}",
        std::process::id()
    )));
    fs::create_dir(&root.0).unwrap();
    let scratch = &root.0;
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
    let secret = support.join("controller-state");
    fs::write(&secret, "controller sentinel").unwrap();

    let mut config = SandboxConfig {
        home,
        mount_root: scratch.join("mounts"),
        workspace_mount: checkout,
        exec_temp_dir: temporary,
        profile_directory: cowshed_core::exec::profile_directory(&scratch.join("store")),
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
    for mode in [RunSandboxMode::ReadWrite, RunSandboxMode::ReadOnly] {
        config.mode = mode;
        let tools = match mode {
            RunSandboxMode::ReadWrite => config.workspace_mount.join(".cowshed/tools/bin"),
            RunSandboxMode::ReadOnly => config.exec_temp_dir.join("tools/bin"),
        };
        fs::create_dir_all(&tools).unwrap();
        for program in &config.capabilities.contribution.bootstrap_programs {
            std::os::unix::fs::symlink(&program.target, tools.join(program.name)).unwrap();
        }
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
