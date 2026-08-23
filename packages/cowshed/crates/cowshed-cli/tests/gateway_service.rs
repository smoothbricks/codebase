use async_trait::async_trait;
use cowshed_cli::args::{Command, GatewayCommand, parse_args};
use cowshed_cli::gateway_service::{
    GatewayDrain, GatewayPaths, activate_launch_agent, drain_after_shutdown, emit_gateway_status,
};
use cowshed_cli::launchd::{
    CommandOutput, CommandStatus, LaunchAgentSpec, LaunchctlCommand, LaunchdExecutor,
};
use cowshed_cli::output::Output;
use cowshed_core::Result;
use cowshed_core::api::GatewayStatus as CliGatewayStatus;

use std::collections::VecDeque;
use std::ffi::OsString;
use std::io;
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
