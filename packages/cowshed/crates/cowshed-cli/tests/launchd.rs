use std::collections::VecDeque;
use std::ffi::OsString;
use std::io;
use std::path::{Path, PathBuf};

use cowshed_cli::launchd::{
    CommandOutput, CommandStatus, ControlAction, ControlExecutionError, ControlPlan, ExistingPlist,
    FilesystemOperation, GATEWAY_LABEL, InstallOutcome, InstallState, LAUNCHCTL_EXECUTABLE,
    LaunchAgentSpec, LaunchctlCommand, LaunchdError, LaunchdExecutor, LaunchdFilesystem, Mutation,
    PRIVATE_DIRECTORY_MODE, PRIVATE_PLIST_MODE, ServiceLifecycle, plan_install, plan_remove,
};

const HOME: &str = "/Users/cowshed-test";
const EXECUTABLE: &str = "/nix/store/abc-cowshed/bin/cowshed";

fn gateway() -> LaunchAgentSpec {
    LaunchAgentSpec::gateway(Path::new(HOME), Path::new(EXECUTABLE)).unwrap()
}

#[test]
fn gateway_definition_has_exact_paths_argv_lifecycle_and_plist_bytes() {
    let spec = gateway();

    assert_eq!(spec.label(), GATEWAY_LABEL);
    assert_eq!(spec.executable(), Path::new(EXECUTABLE));
    assert_eq!(spec.arguments(), ["gateway", "run"]);
    assert_eq!(spec.lifecycle(), ServiceLifecycle::KeepAlive);
    assert_eq!(
        spec.plist_path(),
        Path::new("/Users/cowshed-test/Library/LaunchAgents/dev.cowshed.gateway.plist")
    );
    assert_eq!(
        spec.launch_agents_directory(),
        Path::new("/Users/cowshed-test/Library/LaunchAgents")
    );
    assert_eq!(
        spec.standard_error_path(),
        Path::new("/Users/cowshed-test/.cowshed/telemetry/daemon-stderr.log")
    );
    assert_eq!(
        spec.program_arguments().collect::<Vec<_>>(),
        vec![EXECUTABLE, "gateway", "run"]
    );

    let expected = concat!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
        "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" ",
        "\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n",
        "<plist version=\"1.0\">\n",
        "<dict>\n",
        "  <key>Label</key>\n",
        "  <string>dev.cowshed.gateway</string>\n",
        "  <key>ProgramArguments</key>\n",
        "  <array>\n",
        "    <string>/nix/store/abc-cowshed/bin/cowshed</string>\n",
        "    <string>gateway</string>\n",
        "    <string>run</string>\n",
        "  </array>\n",
        "  <key>RunAtLoad</key>\n",
        "  <true/>\n",
        "  <key>KeepAlive</key>\n",
        "  <true/>\n",
        "  <key>ProcessType</key>\n",
        "  <string>Background</string>\n",
        "  <key>StandardErrorPath</key>\n",
        "  <string>/Users/cowshed-test/.cowshed/telemetry/daemon-stderr.log</string>\n",
        "</dict>\n",
        "</plist>\n",
    );
    assert_eq!(spec.plist_bytes(), expected.as_bytes());
}

#[test]
fn generic_run_at_load_definition_is_immutable_and_escapes_plist_strings() {
    let spec = LaunchAgentSpec::new_user(
        Path::new("/Users/a&b"),
        "dev.cowshed.future",
        Path::new("/Applications/Cowshed & Tools/cowshed"),
        vec!["future".into(), "a<b".into()],
        ServiceLifecycle::RunAtLoad,
    )
    .unwrap();

    let plist = String::from_utf8(spec.plist_bytes()).unwrap();
    assert!(plist.contains("<string>/Applications/Cowshed &amp; Tools/cowshed</string>"));
    assert!(plist.contains("<string>a&lt;b</string>"));
    assert!(plist.contains("<key>KeepAlive</key>\n  <false/>"));
}

#[test]
fn new_install_plan_is_restrictive_and_atomically_replaces_the_plist() {
    let spec = gateway();
    let plan = plan_install(&spec, InstallState::default());
    let desired = spec.plist_bytes();

    assert_eq!(
        plan.operations(),
        [
            Mutation::EnsureDirectory {
                path: PathBuf::from("/Users/cowshed-test/Library/LaunchAgents"),
                mode: PRIVATE_DIRECTORY_MODE,
            },
            Mutation::CreateExclusiveTemporaryFile {
                directory: PathBuf::from("/Users/cowshed-test/Library/LaunchAgents"),
                name_prefix: ".dev.cowshed.gateway.plist.".into(),
                bytes: desired,
                mode: PRIVATE_PLIST_MODE,
            },
            Mutation::SyncTemporaryFile,
            Mutation::RenameTemporaryFile {
                destination: PathBuf::from(
                    "/Users/cowshed-test/Library/LaunchAgents/dev.cowshed.gateway.plist"
                ),
            },
            Mutation::SyncDirectory {
                path: PathBuf::from("/Users/cowshed-test/Library/LaunchAgents"),
            },
        ]
    );
}

#[test]
fn current_install_is_a_noop_but_bad_permissions_are_repaired() {
    let spec = gateway();
    let desired = spec.plist_bytes();
    let current = InstallState {
        launch_agents_directory_mode: Some(PRIVATE_DIRECTORY_MODE),
        plist: Some(ExistingPlist {
            bytes: &desired,
            mode: PRIVATE_PLIST_MODE,
        }),
    };
    assert!(plan_install(&spec, current).is_noop());

    let wrong_plist_mode = InstallState {
        plist: Some(ExistingPlist {
            bytes: &desired,
            mode: 0o644,
        }),
        ..current
    };
    assert!(matches!(
        plan_install(&spec, wrong_plist_mode).operations(),
        [
            Mutation::CreateExclusiveTemporaryFile {
                mode: PRIVATE_PLIST_MODE,
                ..
            },
            Mutation::SyncTemporaryFile,
            Mutation::RenameTemporaryFile { .. },
            Mutation::SyncDirectory { .. }
        ]
    ));

    let wrong_directory_mode = InstallState {
        launch_agents_directory_mode: Some(0o755),
        ..current
    };
    assert_eq!(
        plan_install(&spec, wrong_directory_mode).operations(),
        [
            Mutation::SetPermissions {
                path: PathBuf::from("/Users/cowshed-test/Library/LaunchAgents"),
                mode: PRIVATE_DIRECTORY_MODE,
            },
            Mutation::SyncDirectory {
                path: PathBuf::from("/Users/cowshed-test/Library/LaunchAgents"),
            },
        ]
    );
}

#[test]
fn update_and_remove_plans_are_deterministic_and_filesystem_only() {
    let spec = gateway();
    let state = InstallState {
        launch_agents_directory_mode: Some(PRIVATE_DIRECTORY_MODE),
        plist: Some(ExistingPlist {
            bytes: b"stale plist",
            mode: PRIVATE_PLIST_MODE,
        }),
    };
    let first = plan_install(&spec, state);
    let second = plan_install(&spec, state);
    assert_eq!(first, second);
    assert!(matches!(
        first.operations(),
        [
            Mutation::CreateExclusiveTemporaryFile { .. },
            Mutation::SyncTemporaryFile,
            Mutation::RenameTemporaryFile { .. },
            Mutation::SyncDirectory { .. }
        ]
    ));

    assert!(plan_remove(&spec, false).is_noop());
    assert_eq!(
        plan_remove(&spec, true).operations(),
        [
            Mutation::RemoveFile {
                path: PathBuf::from(
                    "/Users/cowshed-test/Library/LaunchAgents/dev.cowshed.gateway.plist"
                ),
            },
            Mutation::SyncDirectory {
                path: PathBuf::from("/Users/cowshed-test/Library/LaunchAgents"),
            },
        ]
    );
}

#[test]
fn rejects_noncanonical_paths_empty_or_unsafe_inputs_and_provisioning() {
    let cases = [
        LaunchAgentSpec::gateway(Path::new("Users/me"), Path::new(EXECUTABLE)),
        LaunchAgentSpec::gateway(Path::new("/Users/me/../other"), Path::new(EXECUTABLE)),
        LaunchAgentSpec::gateway(Path::new("/Users/me/"), Path::new(EXECUTABLE)),
        LaunchAgentSpec::gateway(Path::new(HOME), Path::new("bin/cowshed")),
        LaunchAgentSpec::gateway(Path::new(HOME), Path::new("/opt/./cowshed")),
        LaunchAgentSpec::gateway(Path::new("/"), Path::new(EXECUTABLE)),
        LaunchAgentSpec::gateway(Path::new(HOME), Path::new("/")),
    ];
    for result in cases {
        assert!(matches!(result, Err(LaunchdError::InvalidPath { .. })));
    }

    for label in ["", ".dev.cowshed", "dev..cowshed", "dev/cowshed"] {
        assert_eq!(
            LaunchAgentSpec::new_user(
                Path::new(HOME),
                label,
                Path::new(EXECUTABLE),
                vec!["run".into()],
                ServiceLifecycle::RunAtLoad,
            ),
            Err(LaunchdError::InvalidLabel)
        );
    }

    assert!(matches!(
        LaunchAgentSpec::new_user(
            Path::new(HOME),
            "dev.cowshed.empty",
            Path::new(EXECUTABLE),
            Vec::new(),
            ServiceLifecycle::RunAtLoad,
        ),
        Err(LaunchdError::InvalidArgument { .. })
    ));
    assert!(matches!(
        LaunchAgentSpec::new_user(
            Path::new(HOME),
            "dev.cowshed.empty",
            Path::new(EXECUTABLE),
            vec!["run".into(), String::new()],
            ServiceLifecycle::RunAtLoad,
        ),
        Err(LaunchdError::InvalidArgument { index: 1, .. })
    ));
    assert_eq!(
        LaunchAgentSpec::new_user(
            Path::new(HOME),
            "dev.cowshed.provision",
            Path::new(EXECUTABLE),
            vec!["adopt".into()],
            ServiceLifecycle::KeepAlive,
        ),
        Err(LaunchdError::PrivilegedProvisioning)
    );
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum FilesystemEvent {
    EnsureDirectory(PathBuf, u32),
    SetPermissions(PathBuf, u32),
    CreateTemporary {
        directory: PathBuf,
        name_prefix: String,
        bytes: Vec<u8>,
        mode: u32,
    },
    SyncFile(PathBuf),
    Rename(PathBuf, PathBuf),
    Remove(PathBuf),
    SyncDirectory(PathBuf),
}

#[derive(Debug, Default)]
struct FakeFilesystem {
    events: Vec<FilesystemEvent>,
    fail_operation: Option<FilesystemOperation>,
    fail_cleanup: bool,
}

impl FakeFilesystem {
    fn failing(operation: FilesystemOperation) -> Self {
        Self {
            fail_operation: Some(operation),
            ..Self::default()
        }
    }

    fn result(&self, operation: FilesystemOperation) -> io::Result<()> {
        if self.fail_operation == Some(operation) {
            Err(io::Error::other(format!("{operation:?} failed")))
        } else {
            Ok(())
        }
    }
}

impl LaunchdFilesystem for FakeFilesystem {
    fn ensure_directory(&mut self, path: &Path, mode: u32) -> io::Result<()> {
        self.events
            .push(FilesystemEvent::EnsureDirectory(path.to_path_buf(), mode));
        self.result(FilesystemOperation::EnsureDirectory)
    }

    fn set_permissions(&mut self, path: &Path, mode: u32) -> io::Result<()> {
        self.events
            .push(FilesystemEvent::SetPermissions(path.to_path_buf(), mode));
        self.result(FilesystemOperation::SetPermissions)
    }

    fn create_exclusive_no_follow(
        &mut self,
        directory: &Path,
        name_prefix: &str,
        bytes: &[u8],
        mode: u32,
    ) -> io::Result<PathBuf> {
        self.events.push(FilesystemEvent::CreateTemporary {
            directory: directory.to_path_buf(),
            name_prefix: name_prefix.to_owned(),
            bytes: bytes.to_vec(),
            mode,
        });
        self.result(FilesystemOperation::CreateTemporaryFile)?;
        Ok(directory.join(".exclusive-no-follow-temp"))
    }

    fn sync_file(&mut self, path: &Path) -> io::Result<()> {
        self.events
            .push(FilesystemEvent::SyncFile(path.to_path_buf()));
        self.result(FilesystemOperation::SyncTemporaryFile)
    }

    fn rename(&mut self, source: &Path, destination: &Path) -> io::Result<()> {
        self.events.push(FilesystemEvent::Rename(
            source.to_path_buf(),
            destination.to_path_buf(),
        ));
        self.result(FilesystemOperation::RenameTemporaryFile)
    }

    fn remove_file(&mut self, path: &Path) -> io::Result<()> {
        self.events
            .push(FilesystemEvent::Remove(path.to_path_buf()));
        if self.fail_cleanup && path.ends_with(".exclusive-no-follow-temp") {
            Err(io::Error::other("cleanup failed"))
        } else {
            self.result(FilesystemOperation::RemoveFile)
        }
    }

    fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
        self.events
            .push(FilesystemEvent::SyncDirectory(path.to_path_buf()));
        self.result(FilesystemOperation::SyncDirectory)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct CommandInvocation {
    executable: PathBuf,
    arguments: Vec<OsString>,
}

#[derive(Debug, Default)]
struct FakeCommand {
    invocations: Vec<CommandInvocation>,
    outputs: VecDeque<io::Result<CommandOutput>>,
}

impl FakeCommand {
    fn with_outputs(outputs: impl IntoIterator<Item = io::Result<CommandOutput>>) -> Self {
        Self {
            outputs: outputs.into_iter().collect(),
            ..Self::default()
        }
    }
}

impl LaunchctlCommand for FakeCommand {
    fn run(&mut self, executable: &Path, arguments: &[OsString]) -> io::Result<CommandOutput> {
        self.invocations.push(CommandInvocation {
            executable: executable.to_path_buf(),
            arguments: arguments.to_vec(),
        });
        self.outputs
            .pop_front()
            .expect("test must provide one output per command")
    }
}

fn temp_path() -> PathBuf {
    PathBuf::from("/Users/cowshed-test/Library/LaunchAgents/.exclusive-no-follow-temp")
}

#[test]
fn executor_applies_new_install_in_exact_durable_order_and_reports_noop_idempotently() {
    let spec = gateway();
    let plan = plan_install(&spec, InstallState::default());
    let desired = spec.plist_bytes();
    let directory = spec.launch_agents_directory().to_path_buf();
    let mut executor = LaunchdExecutor::new(FakeFilesystem::default(), FakeCommand::default());

    assert_eq!(
        executor.execute_install(&plan).unwrap(),
        InstallOutcome::Changed
    );
    let no_change = plan_install(
        &spec,
        InstallState {
            launch_agents_directory_mode: Some(PRIVATE_DIRECTORY_MODE),
            plist: Some(ExistingPlist {
                bytes: &desired,
                mode: PRIVATE_PLIST_MODE,
            }),
        },
    );
    assert_eq!(
        executor.execute_install(&no_change).unwrap(),
        InstallOutcome::NoChange
    );

    let (filesystem, command) = executor.into_parts();
    assert!(command.invocations.is_empty());
    assert_eq!(
        filesystem.events,
        [
            FilesystemEvent::EnsureDirectory(directory.clone(), PRIVATE_DIRECTORY_MODE),
            FilesystemEvent::CreateTemporary {
                directory: directory.clone(),
                name_prefix: ".dev.cowshed.gateway.plist.".into(),
                bytes: desired,
                mode: PRIVATE_PLIST_MODE,
            },
            FilesystemEvent::SyncFile(temp_path()),
            FilesystemEvent::Rename(temp_path(), spec.plist_path().to_path_buf()),
            FilesystemEvent::SyncDirectory(directory),
        ]
    );
}

#[test]
fn update_failure_cleans_temporary_file_and_preserves_primary_and_cleanup_errors() {
    let spec = gateway();
    let plan = plan_install(
        &spec,
        InstallState {
            launch_agents_directory_mode: Some(PRIVATE_DIRECTORY_MODE),
            plist: Some(ExistingPlist {
                bytes: b"stale",
                mode: PRIVATE_PLIST_MODE,
            }),
        },
    );
    let mut filesystem = FakeFilesystem::failing(FilesystemOperation::RenameTemporaryFile);
    filesystem.fail_cleanup = true;
    let mut executor = LaunchdExecutor::new(filesystem, FakeCommand::default());

    let error = executor.execute_install(&plan).unwrap_err();
    assert_eq!(error.operation(), FilesystemOperation::RenameTemporaryFile);
    let cleanup = error
        .cleanup_failure()
        .expect("failed rollback must remain observable");
    assert_eq!(cleanup.path(), temp_path());
    assert_eq!(cleanup.source_error().kind(), io::ErrorKind::Other);

    let (filesystem, _) = executor.into_parts();
    assert!(matches!(
        filesystem.events.as_slice(),
        [
            FilesystemEvent::CreateTemporary { .. },
            FilesystemEvent::SyncFile(_),
            FilesystemEvent::Rename(_, _),
            FilesystemEvent::Remove(_),
        ]
    ));
}

#[test]
fn sync_failure_rolls_back_temp_while_create_and_remove_failures_stop_immediately() {
    let spec = gateway();
    let install = plan_install(&spec, InstallState::default());
    let mut executor = LaunchdExecutor::new(
        FakeFilesystem::failing(FilesystemOperation::SyncTemporaryFile),
        FakeCommand::default(),
    );
    let error = executor.execute_install(&install).unwrap_err();
    assert_eq!(error.operation(), FilesystemOperation::SyncTemporaryFile);
    assert!(error.cleanup_failure().is_none());
    let (filesystem, _) = executor.into_parts();
    assert!(matches!(
        filesystem.events.as_slice(),
        [
            FilesystemEvent::EnsureDirectory(_, _),
            FilesystemEvent::CreateTemporary { .. },
            FilesystemEvent::SyncFile(_),
            FilesystemEvent::Remove(_),
        ]
    ));

    let mut executor = LaunchdExecutor::new(
        FakeFilesystem::failing(FilesystemOperation::CreateTemporaryFile),
        FakeCommand::default(),
    );
    let error = executor.execute_install(&install).unwrap_err();
    assert_eq!(error.operation(), FilesystemOperation::CreateTemporaryFile);
    let (filesystem, _) = executor.into_parts();
    assert!(matches!(
        filesystem.events.as_slice(),
        [
            FilesystemEvent::EnsureDirectory(_, _),
            FilesystemEvent::CreateTemporary { .. },
        ]
    ));

    let remove = plan_remove(&spec, true);
    let mut executor = LaunchdExecutor::new(
        FakeFilesystem::failing(FilesystemOperation::RemoveFile),
        FakeCommand::default(),
    );
    let error = executor.execute_install(&remove).unwrap_err();
    assert_eq!(error.operation(), FilesystemOperation::RemoveFile);
    let (filesystem, _) = executor.into_parts();
    assert_eq!(
        filesystem.events,
        [FilesystemEvent::Remove(spec.plist_path().to_path_buf())]
    );
}

#[test]
fn remove_execution_is_durable_and_absent_remove_is_idempotent() {
    let spec = gateway();
    let mut executor = LaunchdExecutor::new(FakeFilesystem::default(), FakeCommand::default());

    assert_eq!(
        executor.execute_install(&plan_remove(&spec, true)).unwrap(),
        InstallOutcome::Changed
    );
    assert_eq!(
        executor
            .execute_install(&plan_remove(&spec, false))
            .unwrap(),
        InstallOutcome::NoChange
    );

    let (filesystem, _) = executor.into_parts();
    assert_eq!(
        filesystem.events,
        [
            FilesystemEvent::Remove(spec.plist_path().to_path_buf()),
            FilesystemEvent::SyncDirectory(spec.launch_agents_directory().to_path_buf()),
        ]
    );
}

#[test]
fn control_plans_execute_only_exact_unprivileged_launchctl_argv() {
    let spec = gateway();
    let plans = [
        ControlPlan::bootstrap(501, &spec),
        ControlPlan::bootout(501, &spec),
        ControlPlan::kickstart(501, &spec),
    ];
    let outputs = [
        Ok(CommandOutput {
            status: CommandStatus::Success,
            stdout: b"bootstrapped".to_vec(),
            stderr: Vec::new(),
        }),
        Ok(CommandOutput::success()),
        Ok(CommandOutput::success()),
    ];
    let mut executor = LaunchdExecutor::new(
        FakeFilesystem::default(),
        FakeCommand::with_outputs(outputs),
    );

    assert_eq!(
        executor.execute_control(&plans[0]).unwrap(),
        cowshed_cli::launchd::ControlOutcome {
            action: ControlAction::Bootstrap,
            stdout: b"bootstrapped".to_vec(),
            stderr: Vec::new(),
        }
    );
    assert_eq!(
        executor.execute_control(&plans[1]).unwrap().action,
        ControlAction::Bootout
    );
    assert_eq!(
        executor.execute_control(&plans[2]).unwrap().action,
        ControlAction::Kickstart
    );

    let (_, command) = executor.into_parts();
    assert_eq!(
        command.invocations,
        [
            CommandInvocation {
                executable: PathBuf::from(LAUNCHCTL_EXECUTABLE),
                arguments: vec![
                    "bootstrap".into(),
                    "gui/501".into(),
                    spec.plist_path().as_os_str().to_owned(),
                ],
            },
            CommandInvocation {
                executable: PathBuf::from(LAUNCHCTL_EXECUTABLE),
                arguments: vec!["bootout".into(), "gui/501/dev.cowshed.gateway".into()],
            },
            CommandInvocation {
                executable: PathBuf::from(LAUNCHCTL_EXECUTABLE),
                arguments: vec![
                    "kickstart".into(),
                    "-k".into(),
                    "gui/501/dev.cowshed.gateway".into(),
                ],
            },
        ]
    );
    for invocation in command.invocations {
        assert_eq!(invocation.executable, Path::new(LAUNCHCTL_EXECUTABLE));
        for argument in invocation.arguments {
            let argument = argument.to_string_lossy();
            assert!(
                ![
                    "sudo",
                    "diskutil",
                    "hdiutil",
                    "osascript",
                    "adopt",
                    "ensure"
                ]
                .contains(&argument.as_ref())
            );
        }
    }
}

#[test]
fn control_executor_classifies_exit_signal_and_spawn_failures_without_retrying() {
    let spec = gateway();
    let plan = ControlPlan::kickstart(502, &spec);
    let mut executor = LaunchdExecutor::new(
        FakeFilesystem::default(),
        FakeCommand::with_outputs([Ok(CommandOutput {
            status: CommandStatus::ExitCode(37),
            stdout: b"partial".to_vec(),
            stderr: b"service rejected".to_vec(),
        })]),
    );
    assert!(matches!(
        executor.execute_control(&plan),
        Err(ControlExecutionError::Rejected {
            action: ControlAction::Kickstart,
            status: CommandStatus::ExitCode(37),
            stdout,
            stderr,
        }) if stdout == b"partial" && stderr == b"service rejected"
    ));
    let (_, command) = executor.into_parts();
    assert_eq!(command.invocations.len(), 1);

    let mut executor = LaunchdExecutor::new(
        FakeFilesystem::default(),
        FakeCommand::with_outputs([Err(io::Error::new(
            io::ErrorKind::NotFound,
            "launchctl missing",
        ))]),
    );
    assert!(matches!(
        executor.execute_control(&ControlPlan::bootstrap(502, &spec)),
        Err(ControlExecutionError::Unavailable {
            action: ControlAction::Bootstrap,
            source,
        }) if source.kind() == io::ErrorKind::NotFound
    ));

    let mut executor = LaunchdExecutor::new(
        FakeFilesystem::default(),
        FakeCommand::with_outputs([Ok(CommandOutput {
            status: CommandStatus::Terminated,
            stdout: Vec::new(),
            stderr: Vec::new(),
        })]),
    );
    assert!(matches!(
        executor.execute_control(&ControlPlan::bootout(502, &spec)),
        Err(ControlExecutionError::Rejected {
            action: ControlAction::Bootout,
            status: CommandStatus::Terminated,
            ..
        })
    ));
}
