use async_trait::async_trait;
use cowshed_cli::args::{Command, GatewayCommand, parse_args};
use cowshed_cli::gateway_service::{
    GatewayDrain, GatewayPaths, RELEASE_CLI_TARGET, activate_launch_agent, drain_after_shutdown,
    emit_gateway_status, install_host_stable_executable, refuse_unsupervisable_build,
    remove_host_stable_executable, restore_previous_executable, retain_previous_executable,
};
use cowshed_cli::launchd::{
    COWSHED_BINARY_NAME, CommandStatus, ExecutableDigest, HostStableExecutable, InstallOutcome,
    LaunchAgentSpec, LaunchctlCommand, LaunchctlOutput, LaunchdExecutor, NativeFilesystem,
    NativeLaunchctlCommand, RemovalOutcome, STABLE_BINARY_MODE,
};
use cowshed_cli::output::Output;
use cowshed_core::api::GatewayStatus as CliGatewayStatus;
use cowshed_core::{CowshedError, Result};

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
    outputs: VecDeque<io::Result<LaunchctlOutput>>,
    argv: Vec<Vec<OsString>>,
}

impl RecordingLaunchctl {
    fn new(outputs: impl IntoIterator<Item = io::Result<LaunchctlOutput>>) -> Self {
        Self {
            outputs: outputs.into_iter().collect(),
            argv: Vec::new(),
        }
    }
}

impl LaunchctlCommand for RecordingLaunchctl {
    fn run(&mut self, executable: &Path, arguments: &[OsString]) -> io::Result<LaunchctlOutput> {
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
        Ok(LaunchctlOutput {
            status: CommandStatus::ExitCode(3),
            stdout: Vec::new(),
            stderr: b"not loaded".to_vec(),
        }),
        Ok(LaunchctlOutput::success()),
    ]);
    let mut executor = LaunchdExecutor::new((), command);
    activate_launch_agent(
        &mut executor,
        501,
        launch_spec().target(),
        InstallOutcome::NoChange,
    )
    .expect("activation succeeds");
    let (_, command) = executor.into_parts();
    assert_eq!(command.argv.len(), 2);
    assert_eq!(command.argv[0][0], "print");
    assert_eq!(command.argv[1][0], "bootstrap");
}

#[test]
fn launch_agent_activation_is_idempotent_and_propagates_spawn_failure() {
    let command = RecordingLaunchctl::new([
        Ok(LaunchctlOutput::success()),
        Ok(LaunchctlOutput::success()),
    ]);
    let mut executor = LaunchdExecutor::new((), command);
    activate_launch_agent(
        &mut executor,
        501,
        launch_spec().target(),
        InstallOutcome::NoChange,
    )
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
        activate_launch_agent(
            &mut executor,
            501,
            launch_spec().target(),
            InstallOutcome::NoChange
        )
        .is_err()
    );
}

/// A rewritten plist is only a file: launchd runs the definition it bootstrapped, so the agent
/// has to be booted out and bootstrapped again or a later kickstart restarts the old program —
/// the path the rewrite exists to stop naming. Bootstrap starts `RunAtLoad`; kickstart after
/// that is a race (launchctl 37).
///
/// The launchctl sequence is print(loaded), bootout, print(gone) to confirm the bootout actually
/// finished, print(gone) again from the activation, then bootstrap. That confirming probe is what
/// stops the caller taking the kickstart branch against a teardown still in progress.
#[test]
fn a_changed_plist_reloads_the_agent_instead_of_kickstarting_the_old_program() {
    let not_loaded = || {
        Ok(LaunchctlOutput {
            status: CommandStatus::ExitCode(3),
            stdout: Vec::new(),
            stderr: b"not loaded".to_vec(),
        })
    };
    let command = RecordingLaunchctl::new([
        Ok(LaunchctlOutput::success()),
        Ok(LaunchctlOutput::success()),
        not_loaded(),
        not_loaded(),
        Ok(LaunchctlOutput::success()),
    ]);
    let mut executor = LaunchdExecutor::new((), command);
    activate_launch_agent(
        &mut executor,
        501,
        launch_spec().target(),
        InstallOutcome::Changed,
    )
    .expect("activation succeeds");
    let (_, command) = executor.into_parts();
    assert_eq!(
        command
            .argv
            .iter()
            .map(|argv| argv[0].to_str().expect("utf-8 argv"))
            .collect::<Vec<_>>(),
        ["print", "bootout", "print", "print", "bootstrap"]
    );
}

#[test]
fn production_config_disables_tcp_and_uses_validated_roots_and_fixed_helper() {
    let paths = GatewayPaths {
        home: PathBuf::from("/Users/test"),
        store: PathBuf::from("/private/cowshed/store"),
        cache_dir: PathBuf::from("/Users/test/Library/Caches/dev.cowshed"),
        mirror_cache: PathBuf::from("/Users/test/Library/Caches/dev.cowshed/mirror"),
        telemetry: PathBuf::from("/private/cowshed/store/telemetry/gateway"),
        control_socket: PathBuf::from("/private/cowshed/store/gateway.sock"),
    };
    let helper = PathBuf::from("/Applications/Cowshed.app/Contents/MacOS/cowshed");
    let config = paths.config(501, helper.clone());
    assert_eq!(config.control_socket, Some(paths.control_socket));
    assert_eq!(config.control_tcp, None);
    assert_eq!(config.authorized_control_uid, 501);
    assert_eq!(config.production_cache_dir, Some(paths.cache_dir));
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

    async fn stopped(&mut self) -> CowshedError {
        std::future::pending().await
    }
}

/// A gateway whose actor already stopped on its own, as an audit fail-closed one does.
struct StoppedGateway;

#[async_trait]
impl GatewayDrain for StoppedGateway {
    async fn drain(self) -> Result<()> {
        panic!("a gateway that stopped by itself is not drained again")
    }

    async fn stopped(&mut self) -> CowshedError {
        CowshedError::internal("the gateway stopped: audit sink failed")
    }
}

/// The daemon must not outlive a gateway that stopped by itself: it ends with that failure, so
/// launchd restarts it, instead of waiting for a shutdown signal that never comes.
#[tokio::test]
async fn a_gateway_that_stops_by_itself_ends_the_daemon_with_its_cause() {
    let error = drain_after_shutdown(StoppedGateway, std::future::pending())
        .await
        .expect_err("the daemon ends when its gateway stops");
    assert!(
        error.message.contains("audit sink failed"),
        "{}",
        error.message
    );
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
            active_workspaces: 2,
            drain_cause: None,
            healing: None,
            recovering: None,
            stale_daemon: None,
        },
    )
    .expect("status emits");
    let (stdout, stderr) = output.into_inner();
    assert_eq!(
        stdout,
        b"{\"ok\":true,\"result\":{\"installed\":true,\"running\":true,\"socket\":\"/private/cowshed/store/gateway.sock\",\"cliVersion\":\"1.4.0\",\"daemonVersion\":\"1.3.0\",\"activeWorkspaces\":2}}\n"
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
                active_workspaces: 0,
                drain_cause: None,
                healing: None,
                recovering: None,
                stale_daemon: None,
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

/// The digest of `bytes`, as the stored copy's name spells it.
fn digest_of(bytes: &[u8]) -> ExecutableDigest {
    use sha2::Digest as _;
    ExecutableDigest::from_bytes(&sha2::Sha256::digest(bytes).into())
}

/// `gateway start` runs from wherever the user's cowshed happens to live — a nix store path, a
/// global npm prefix, a build directory — and launchd has to keep working after that path is
/// gone. The build is copied onto the volume that carries the plist under the name its digest
/// gives it, the stable name the plist and the operator's PATH name becomes a link to that copy,
/// and a second start reuses the copy rather than rewriting the file launchd is running.
#[test]
fn installing_the_agent_binary_publishes_a_content_addressed_copy_and_reuses_it() {
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
    let first = executable.stored(&digest_of(b"#!/bin/sh\nexit 0\n"));
    assert_eq!(
        fs::read_link(executable.path()).expect("the stable name is a link"),
        first.file_name().map(PathBuf::from).expect("a file name"),
        "the stable name says which build it runs"
    );
    let stored = fs::symlink_metadata(&first).expect("stored copy");
    assert!(stored.is_file());
    assert_eq!(stored.permissions().mode() & 0o777, STABLE_BINARY_MODE);
    assert_eq!(
        fs::read(executable.path()).expect("installed bytes through the link"),
        b"#!/bin/sh\nexit 0\n"
    );
    assert_eq!(
        fs::canonicalize(executable.path()).expect("resolves"),
        fs::canonicalize(&first).expect("resolves"),
        "what the daemon and the PATH entry run is the stored copy"
    );
    // The plist launchd reads names the stable name, which never changes with the build.
    let spec = LaunchAgentSpec::gateway(&executable).expect("valid spec");
    assert_eq!(spec.program_arguments().next(), executable.path().to_str());

    // A current copy is left in place: same inode, no rewrite of a running binary.
    let reinstalled =
        install_host_stable_executable(&mut executor, &home, COWSHED_BINARY_NAME, &source)
            .expect("second install succeeds");
    assert_eq!(reinstalled.path(), executable.path());
    assert_eq!(
        fs::symlink_metadata(&first).expect("stored copy").ino(),
        stored.ino()
    );

    // A newer build is a different file under a different name, and the stable name moves to it
    // in one rename: launchd never sees a partially written binary or a missing name, and the
    // build that is replaced is not rewritten under a process still running it.
    fs::write(&source, b"#!/bin/sh\nexit 1\n").expect("newer source binary");
    install_host_stable_executable(&mut executor, &home, COWSHED_BINARY_NAME, &source)
        .expect("upgrade succeeds");
    let second = executable.stored(&digest_of(b"#!/bin/sh\nexit 1\n"));
    assert_ne!(first, second);
    assert_eq!(
        fs::read(executable.path()).expect("installed bytes"),
        b"#!/bin/sh\nexit 1\n"
    );
    assert_eq!(
        fs::read(&first).expect("the replaced copy"),
        b"#!/bin/sh\nexit 0\n",
        "the build it replaced is untouched"
    );
    assert_eq!(
        fs::symlink_metadata(&first).expect("stored copy").ino(),
        stored.ino()
    );

    // Installing the stable name from itself is the steady state and touches nothing.
    let before = fs::symlink_metadata(&second).expect("stored copy").ino();
    let same = install_host_stable_executable(
        &mut executor,
        &home,
        COWSHED_BINARY_NAME,
        executable.path(),
    )
    .expect("self install succeeds");
    assert_eq!(same.path(), executable.path());
    assert_eq!(
        fs::symlink_metadata(&second).expect("stored copy").ino(),
        before
    );

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
    let stored = executable.stored(&digest_of(b"#!/bin/sh\nexit 0\n"));
    assert_eq!(
        fs::symlink_metadata(&stored)
            .expect("installed metadata")
            .permissions()
            .mode()
            & 0o777,
        STABLE_BINARY_MODE
    );
    assert!(
        !stored.starts_with(home.join(".cowshed")),
        "nothing under the workspace is named"
    );
    let spec = LaunchAgentSpec::gateway(&executable).expect("valid spec");
    assert_eq!(spec.program_arguments().next(), executable.path().to_str());

    fs::remove_dir_all(&home).expect("remove scratch home");
}

/// The incident this rule exists for: a `setup` run from `target/debug/cowshed` copied a 94 MB
/// debug build over a host's supervised gateway and then failed on `launchctl kickstart`, leaving
/// the host with a debug binary and no loaded agent. A debug build carries `debug_assertions`, so
/// under a `KeepAlive` agent it is a respawn loop rather than merely a large file.
#[test]
fn a_debug_build_is_never_installed_as_the_supervised_binary() {
    let source = PathBuf::from("/Users/dev/checkout/target/debug/cowshed");

    let error = refuse_unsupervisable_build(source.clone(), true)
        .expect_err("a debug build must not be supervised");
    assert_eq!(error.code.as_str(), "conflict");
    assert!(
        error.message.contains("is a debug build"),
        "{}",
        error.message
    );
    assert!(
        error.message.contains(source.to_str().expect("utf-8")),
        "the refusal must name the build it refused; got {}",
        error.message
    );
    assert!(error.hint.contains("--release"), "{}", error.hint);
    // The remedy must name a build that this guard accepts. `nx run cowshed:build` builds the
    // platform `cli-*` target in its default configuration, which is a debug build: a hint
    // naming it sent the operator straight back here.
    // Resolved at run time: `env!` would compile the building checkout's path into the binary,
    // and an archive replayed from the cache into another checkout reads a path that is gone.
    let manifest = std::env::var_os("CARGO_MANIFEST_DIR")
        .expect("cargo and nextest export CARGO_MANIFEST_DIR to the test process");
    let package: serde_json::Value = serde_json::from_str(
        &fs::read_to_string(Path::new(&manifest).join("../../package.json"))
            .expect("cowshed package.json"),
    )
    .expect("package.json is JSON");
    let target = &package["nx"]["targets"][RELEASE_CLI_TARGET];
    let command = |configuration: &serde_json::Value| {
        configuration["command"]
            .as_str()
            .unwrap_or_else(|| panic!("{RELEASE_CLI_TARGET} names its command: {target}"))
            .to_owned()
    };
    assert!(
        command(&target["configurations"]["production"]).contains("--release"),
        "{RELEASE_CLI_TARGET}:production must be a release build: {target}"
    );
    assert!(
        !command(&target["options"]).contains("--release"),
        "{RELEASE_CLI_TARGET}'s default configuration is a debug build, as the hint says: {target}"
    );
    assert!(
        error
            .hint
            .contains(&format!("nx run cowshed:{RELEASE_CLI_TARGET}:production")),
        "{}",
        error.hint
    );

    // A release build is accepted unchanged: the guard is a gate, not a transformation.
    assert_eq!(
        refuse_unsupervisable_build(source.clone(), false)
            .expect("a release build is supervisable"),
        source
    );
}

/// A failed activation must leave the host exactly as it was found. The previous build is kept as
/// a stored copy under its digest — the plain binary the old layout left at the stable name
/// included, stored whole before the stable name is replaced by a link — so the restore points
/// the stable name at the exact bytes launchd was running rather than a re-copy.
#[test]
fn a_failed_activation_restores_the_binary_it_replaced() {
    let home = scratch_home("rollback");
    let executable =
        HostStableExecutable::new(&home, COWSHED_BINARY_NAME).expect("host-stable path");
    fs::create_dir_all(executable.directory()).expect("bin directory");
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);

    // Nothing installed yet: there is nothing to retain and nothing to roll back to, which is a
    // first install rather than a regression.
    assert_eq!(
        retain_previous_executable(&mut executor, &executable).expect("retain on an empty host"),
        None
    );

    // The plain binary of the old layout is stored under its digest, and the stable name already
    // runs that copy: the host is as it was, in the new layout.
    fs::write(executable.path(), b"the supervised release build\n").expect("installed binary");
    fs::set_permissions(
        executable.path(),
        fs::Permissions::from_mode(STABLE_BINARY_MODE),
    )
    .expect("mode");
    let retained = retain_previous_executable(&mut executor, &executable)
        .expect("retain succeeds")
        .expect("an installed binary is retained");
    assert_eq!(
        retained,
        executable.stored(&digest_of(b"the supervised release build\n"))
    );
    assert_eq!(
        fs::read(&retained).expect("retained bytes"),
        b"the supervised release build\n"
    );

    // A link to a stored copy is retained as it is: the copy is already what a restore needs.
    assert_eq!(
        retain_previous_executable(&mut executor, &executable).expect("retain again"),
        Some(retained.clone())
    );

    // The install moves the stable name to another build; the restore moves it back.
    let other = home.join("other-build");
    fs::write(&other, b"a build whose agent will not load\n").expect("replacement");
    install_host_stable_executable(&mut executor, &home, COWSHED_BINARY_NAME, &other)
        .expect("install");
    assert_eq!(
        fs::read(executable.path()).expect("replaced bytes"),
        b"a build whose agent will not load\n"
    );

    let sentence = restore_previous_executable(&executable, &retained);
    assert!(
        sentence.contains("this host is as it was found"),
        "got {sentence}"
    );
    assert_eq!(
        fs::read(executable.path()).expect("restored bytes"),
        b"the supervised release build\n"
    );
    assert_eq!(
        fs::canonicalize(executable.path()).expect("resolves"),
        fs::canonicalize(&retained).expect("resolves"),
        "rollback runs the original copy, not a re-copy"
    );

    // A rollback that cannot happen is reported, never silently swallowed: the caller's own
    // failure plus this sentence are the host's actual state.
    let sentence = restore_previous_executable(&executable, &home.join("never-retained"));
    assert!(sentence.contains("could NOT be restored"), "got {sentence}");

    let _ = fs::remove_dir_all(&home);
}

#[test]
fn retention_inspection_failure_is_not_an_empty_host() {
    let home = scratch_home("retention-inspection-error");
    let executable =
        HostStableExecutable::new(&home, COWSHED_BINARY_NAME).expect("host-stable path");
    let library = home.join("Library");
    fs::write(&library, b"not a directory").expect("blocked ancestor");
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);

    assert_eq!(
        fs::symlink_metadata(executable.path()).unwrap_err().kind(),
        io::ErrorKind::NotADirectory
    );
    retain_previous_executable(&mut executor, &executable)
        .expect_err("inspection failure must abort retention");
    assert_eq!(fs::read(&library).unwrap(), b"not a directory");
    fs::remove_dir_all(&home).unwrap();
}

/// Uninstall and `gateway stop --purge` remove everything an install leaves — the stable name,
/// every stored copy, the build record beside each — and leave the directory and whatever else
/// lives in it; a second removal finds nothing to remove.
#[test]
fn removing_the_installed_cowshed_takes_the_stable_name_every_copy_and_every_record() {
    let home = scratch_home("remove-installed");
    let mut executor = LaunchdExecutor::new(NativeFilesystem::new(), NativeLaunchctlCommand);
    let mut executable = None;
    for build in [&b"first build\n"[..], b"second build\n"] {
        let source = home.join("build");
        fs::write(&source, build).expect("build");
        executable = Some(
            install_host_stable_executable(&mut executor, &home, COWSHED_BINARY_NAME, &source)
                .expect("install"),
        );
    }
    let executable = executable.expect("installed");
    let first = executable.stored(&digest_of(b"first build\n"));
    fs::write(format!("{}.build", first.display()), b"{}").expect("a build record");
    let neighbour = executable.directory().join("sccache");
    fs::write(&neighbour, b"another service's binary").expect("neighbour");

    assert_eq!(
        remove_host_stable_executable(&executable).expect("remove"),
        RemovalOutcome::Removed
    );
    let left: Vec<_> = fs::read_dir(executable.directory())
        .expect("directory")
        .map(|entry| entry.expect("entry").file_name())
        .collect();
    assert_eq!(left, [OsString::from("sccache")]);
    assert_eq!(
        remove_host_stable_executable(&executable).expect("remove again"),
        RemovalOutcome::AlreadyAbsent
    );
    fs::remove_dir_all(&home).unwrap();
}
