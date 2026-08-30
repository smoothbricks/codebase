use async_trait::async_trait;
use cowshed_cli::args::{Command, GatewayCommand, parse_args};
use cowshed_cli::gateway_service::{
    GatewayDrain, GatewayPaths, activate_launch_agent, drain_after_shutdown, emit_gateway_status,
    install_host_stable_executable,
};
use cowshed_cli::launchd::{
    COWSHED_BINARY_NAME, CommandOutput, CommandStatus, HostStableExecutable, InstallOutcome,
    LaunchAgentSpec, LaunchctlCommand, LaunchdExecutor, NativeFilesystem, NativeLaunchctlCommand,
    STABLE_BINARY_MODE,
};
use cowshed_cli::output::Output;
use cowshed_core::Result;
use cowshed_core::api::GatewayStatus as CliGatewayStatus;

use std::collections::VecDeque;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::fs::{MetadataExt as _, PermissionsExt as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

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
        &HostStableExecutable::new(Path::new("/Users/test"), COWSHED_BINARY_NAME)
            .expect("valid host-stable binary"),
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
    activate_launch_agent(&mut executor, 501, &launch_spec(), InstallOutcome::NoChange)
        .expect("activation succeeds");
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
    activate_launch_agent(&mut executor, 501, &launch_spec(), InstallOutcome::NoChange)
        .expect("activation succeeds");
    let (_, command) = executor.into_parts();
    assert_eq!(command.argv.len(), 2);
    assert_eq!(command.argv[0][0], "print");
    assert_eq!(command.argv[1][0], "kickstart");

    let command = RecordingLaunchctl::new([Err(io::Error::new(
        io::ErrorKind::NotFound,
        "launchctl missing",
    ))]);
    let mut executor = LaunchdExecutor::new((), command);
    assert!(
        activate_launch_agent(&mut executor, 501, &launch_spec(), InstallOutcome::NoChange)
            .is_err()
    );
}

/// A rewritten plist is only a file: launchd runs the definition it bootstrapped, so the agent
/// has to be booted out and bootstrapped again or the kickstart restarts the old program — the
/// path the rewrite exists to stop naming.
#[test]
fn a_changed_plist_reloads_the_agent_instead_of_kickstarting_the_old_program() {
    let command = RecordingLaunchctl::new([
        Ok(CommandOutput::success()),
        Ok(CommandOutput::success()),
        Ok(CommandOutput {
            status: CommandStatus::ExitCode(3),
            stdout: Vec::new(),
            stderr: b"not loaded".to_vec(),
        }),
        Ok(CommandOutput::success()),
        Ok(CommandOutput::success()),
    ]);
    let mut executor = LaunchdExecutor::new((), command);
    activate_launch_agent(&mut executor, 501, &launch_spec(), InstallOutcome::Changed)
        .expect("activation succeeds");
    let (_, command) = executor.into_parts();
    assert_eq!(
        command
            .argv
            .iter()
            .map(|argv| argv[0].to_str().expect("utf-8 argv"))
            .collect::<Vec<_>>(),
        ["print", "bootout", "print", "bootstrap", "kickstart"]
    );
}

#[test]
fn production_config_disables_tcp_and_uses_validated_roots_and_fixed_helper() {
    let paths = GatewayPaths {
        home: PathBuf::from("/Users/test"),
        store: PathBuf::from("/private/cowshed/store"),
        cache_volume: PathBuf::from("/private/cowshed/caches"),
        mirror_cache: PathBuf::from("/private/cowshed/caches/mirror"),
        telemetry: PathBuf::from("/private/cowshed/store/telemetry/gateway"),
        control_socket: PathBuf::from("/private/cowshed/store/gateway.sock"),
    };
    let helper = PathBuf::from("/Applications/Cowshed.app/Contents/MacOS/cowshed");
    let config = paths.config(501, helper.clone());
    assert_eq!(config.control_socket, Some(paths.control_socket));
    assert_eq!(config.control_tcp, None);
    assert_eq!(config.authorized_control_uid, 501);
    assert_eq!(config.production_cache_volume, Some(paths.cache_volume));
    assert_eq!(config.mirror_cache.cache_root, paths.mirror_cache);
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
            installed: true,
            running: true,
            socket: PathBuf::from("/private/cowshed/store/gateway.sock"),
            cli_version: "1.4.0".into(),
            daemon_version: Some("1.3.0".into()),
            cache_entries: 0,
            cache_bytes: 0,
            active_workspaces: 2,
        },
    )
    .expect("status emits");
    let (stdout, stderr) = output.into_inner();
    assert_eq!(
        stdout,
        b"{\"ok\":true,\"result\":{\"installed\":true,\"running\":true,\"socket\":\"/private/cowshed/store/gateway.sock\",\"cliVersion\":\"1.4.0\",\"daemonVersion\":\"1.3.0\",\"cacheEntries\":0,\"cacheBytes\":0,\"activeWorkspaces\":2}}\n"
    );
    assert!(stderr.is_empty());
}

#[test]
fn gateway_status_names_launchd_socket_and_both_versions() {
    let cases = [
        (
            true,
            true,
            Some("1.3.0"),
            "gateway is healthy: launchd loaded; control socket answers",
        ),
        (
            true,
            false,
            None,
            "gateway is installed but its control socket does not answer",
        ),
        (
            false,
            false,
            None,
            "gateway is not installed; no control socket answers",
        ),
    ];
    for (installed, running, daemon_version, state) in cases {
        let mut output = Output::new(Vec::new(), Vec::new(), false);
        emit_gateway_status(
            &mut output,
            false,
            CliGatewayStatus {
                installed,
                running,
                socket: PathBuf::from("/private/cowshed/store/gateway.sock"),
                cli_version: "1.4.0".into(),
                daemon_version: daemon_version.map(str::to_owned),
                cache_entries: 0,
                cache_bytes: 0,
                active_workspaces: 0,
            },
        )
        .expect("status emits");
        let (stdout, stderr) = output.into_inner();
        assert!(stdout.is_empty());
        let stderr = String::from_utf8(stderr).expect("utf8 status");
        assert!(stderr.contains(state), "{stderr}");
        assert!(
            stderr.contains("/private/cowshed/store/gateway.sock"),
            "{stderr}"
        );
        assert!(stderr.contains("cli 1.4.0"), "{stderr}");
        assert!(
            stderr.contains(&format!(
                "daemon {}",
                daemon_version.unwrap_or("unavailable")
            )),
            "{stderr}"
        );
    }
}

fn scratch_home(label: &str) -> PathBuf {
    let home = std::env::temp_dir().join(format!(
        "cowshed-cli-gateway-{label}-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&home);
    fs::create_dir_all(&home).expect("scratch home");
    home.canonicalize().expect("canonical scratch home")
}

/// `gateway start` runs from wherever the user's cowshed happens to live — a nix store path, a
/// global npm prefix, a build directory — and launchd has to keep working after that path is
/// gone. The binary is copied onto the volume that carries the plist, and a second start reuses
/// that copy rather than rewriting the file launchd is running.
#[test]
fn installing_the_agent_binary_publishes_a_host_stable_copy_and_reuses_it() {
    let home = scratch_home("install");
    let source = home.join("build/cowshed");
    fs::create_dir_all(source.parent().expect("build directory")).expect("build directory");
    fs::write(&source, b"#!/bin/sh\nexit 0\n").expect("source binary");

    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    let executable =
        install_host_stable_executable(&mut executor, &home, COWSHED_BINARY_NAME, &source)
            .expect("install succeeds");

    assert_eq!(
        executable.path(),
        home.join("Library/Application Support/dev.cowshed/bin/cowshed")
    );
    let installed = fs::symlink_metadata(executable.path()).expect("installed binary");
    assert!(installed.is_file());
    assert_eq!(installed.permissions().mode() & 0o777, STABLE_BINARY_MODE);
    assert_eq!(
        fs::read(executable.path()).expect("installed bytes"),
        b"#!/bin/sh\nexit 0\n"
    );
    // The plist launchd reads names exactly this path.
    let spec = LaunchAgentSpec::gateway(&executable).expect("valid spec");
    assert_eq!(spec.program_arguments().next(), executable.path().to_str());

    // A current copy is left in place: same inode, no rewrite of a running binary.
    let reinstalled =
        install_host_stable_executable(&mut executor, &home, COWSHED_BINARY_NAME, &source)
            .expect("second install succeeds");
    assert_eq!(reinstalled.path(), executable.path());
    assert_eq!(
        fs::symlink_metadata(executable.path())
            .expect("installed binary")
            .ino(),
        installed.ino()
    );

    // A newer build replaces it, and the replacement is a different file: launchd never sees a
    // partially written binary at the path it runs.
    fs::write(&source, b"#!/bin/sh\nexit 1\n").expect("newer source binary");
    install_host_stable_executable(&mut executor, &home, COWSHED_BINARY_NAME, &source)
        .expect("upgrade succeeds");
    assert_eq!(
        fs::read(executable.path()).expect("installed bytes"),
        b"#!/bin/sh\nexit 1\n"
    );
    assert_ne!(
        fs::symlink_metadata(executable.path())
            .expect("installed binary")
            .ino(),
        installed.ino()
    );

    // Installing the copy from itself is the steady state and touches nothing.
    let same = install_host_stable_executable(
        &mut executor,
        &home,
        COWSHED_BINARY_NAME,
        executable.path(),
    )
    .expect("self install succeeds");
    assert_eq!(same.path(), executable.path());

    fs::remove_dir_all(&home).expect("remove scratch home");
}

/// A workspace or store build is a valid source of the copy. launchd runs the
/// copy on Application Support, not the source path, so a reboot does not need
/// that volume mounted.
#[test]
fn installing_from_a_workspace_build_copies_onto_the_host_volume() {
    let home = scratch_home("workspace-source");
    let source = home.join(".cowshed/mnt/acme/widget/main/target/release/cowshed");
    fs::create_dir_all(source.parent().expect("workspace directory")).expect("workspace directory");
    fs::write(&source, b"#!/bin/sh\nexit 0\n").expect("workspace binary");

    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    let executable =
        install_host_stable_executable(&mut executor, &home, COWSHED_BINARY_NAME, &source)
            .expect("copy from a workspace build succeeds");

    assert_eq!(
        executable.path(),
        home.join("Library/Application Support/dev.cowshed/bin/cowshed")
    );
    assert_eq!(
        fs::read(executable.path()).expect("installed bytes"),
        b"#!/bin/sh\nexit 0\n"
    );
    let spec = LaunchAgentSpec::gateway(&executable).expect("valid spec");
    assert_eq!(spec.program_arguments().next(), executable.path().to_str());

    fs::remove_dir_all(&home).expect("remove scratch home");
}
