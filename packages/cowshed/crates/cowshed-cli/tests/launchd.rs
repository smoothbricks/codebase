use std::collections::VecDeque;
use std::ffi::OsString;
use std::fs;
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};

use cowshed_cli::launchd::{
    COWSHED_BINARY_NAME, CommandOutput, CommandStatus, ControlAction, ControlExecutionError,
    ControlPlan, ExecutableInstallState, ExecutableSource, ExistingPlist, FilesystemOperation,
    GATEWAY_LABEL, HostStableExecutable, InstallOutcome, InstallState, InstalledExecutable,
    LAUNCHCTL_EXECUTABLE, LaunchAgentSpec, LaunchctlCommand, LaunchdError, LaunchdExecutor,
    LaunchdFilesystem, LaunchdServiceStatus, Mutation, NativeFilesystem, PRIVATE_DIRECTORY_MODE,
    PRIVATE_PLIST_MODE, SCCACHE_BINARY_NAME, SCCACHE_LABEL, STABLE_BINARY_MODE, ServiceLifecycle,
    UnstableExecutableSource, classify_executable_source, containing_mount_point,
    plan_executable_install, plan_install, plan_remove,
};
use cowshed_core::metadata::ImageCapacity;

const HOME: &str = "/Users/cowshed-test";
/// The only path shape a cowshed LaunchAgent can name: on the volume that carries the plist.
const EXECUTABLE: &str = "/Users/cowshed-test/Library/Application Support/dev.cowshed/bin/cowshed";
const BINARY_DIRECTORY: &str = "/Users/cowshed-test/Library/Application Support/dev.cowshed/bin";
const SUPPORT_DIRECTORY: &str = "/Users/cowshed-test/Library/Application Support/dev.cowshed";

fn cowshed_binary() -> HostStableExecutable {
    HostStableExecutable::new(Path::new(HOME), COWSHED_BINARY_NAME).unwrap()
}

fn gateway() -> LaunchAgentSpec {
    LaunchAgentSpec::gateway(&cowshed_binary()).unwrap()
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
        Path::new("/Users/cowshed-test/Library/Logs/cowshed/daemon-stderr.log")
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
        "    <string>/Users/cowshed-test/Library/Application Support/dev.cowshed/bin/cowshed</string>\n",
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
        "  <string>/Users/cowshed-test/Library/Logs/cowshed/daemon-stderr.log</string>\n",
        "</dict>\n",
        "</plist>\n",
    );
    assert_eq!(spec.plist_bytes(), expected.as_bytes());
}

#[test]
fn generic_run_at_load_definition_is_immutable_and_escapes_plist_strings() {
    let spec = LaunchAgentSpec::new_user(
        &HostStableExecutable::new(Path::new("/Users/a&b"), COWSHED_BINARY_NAME).unwrap(),
        "dev.cowshed.future",
        vec!["future".into(), "a<b".into()],
        ServiceLifecycle::RunAtLoad,
    )
    .unwrap();

    let plist = String::from_utf8(spec.plist_bytes()).unwrap();
    assert!(plist.contains(
        "<string>/Users/a&amp;b/Library/Application Support/dev.cowshed/bin/cowshed</string>"
    ));
    assert!(plist.contains("<string>a&lt;b</string>"));
    assert!(plist.contains("<key>KeepAlive</key>\n  <false/>"));
}

/// The sccache agent is the one with an empty argv tail: server mode is
/// selected entirely through the environment, and the plist carries the full
/// foreground-server variable set with the shared cowshed paths.
///
/// The cap and the base directory are part of that set because sccache reads both once, at server
/// start: no client can supply them. `SCCACHE_BASEDIRS` is asserted by its plural name — sccache
/// 0.16 has no singular `SCCACHE_BASEDIR` and ignores it silently, so the name is the contract.
#[test]
fn sccache_definition_runs_a_foreground_uds_server_via_environment() {
    let spec = LaunchAgentSpec::sccache(
        &HostStableExecutable::new(Path::new(HOME), SCCACHE_BINARY_NAME).unwrap(),
        Path::new("/Users/cowshed-test/.cowshed/sccache.sock"),
        Path::new("/Users/cowshed-test/.cowshed/caches/sccache"),
        ImageCapacity::from_gibibytes(40),
        Path::new("/Users/cowshed-test/.cowshed"),
    )
    .unwrap();

    assert_eq!(spec.label(), SCCACHE_LABEL);
    assert_eq!(spec.arguments(), [] as [String; 0]);
    assert_eq!(spec.lifecycle(), ServiceLifecycle::KeepAlive);
    assert_eq!(
        spec.plist_path(),
        Path::new("/Users/cowshed-test/Library/LaunchAgents/dev.cowshed.sccache.plist")
    );
    assert_eq!(
        spec.standard_error_path(),
        Path::new("/Users/cowshed-test/Library/Logs/cowshed/sccache-stderr.log")
    );
    assert_eq!(
        spec.environment(),
        [
            ("SCCACHE_START_SERVER".to_owned(), "1".to_owned()),
            ("SCCACHE_NO_DAEMON".to_owned(), "1".to_owned()),
            ("SCCACHE_IDLE_TIMEOUT".to_owned(), "0".to_owned()),
            (
                "SCCACHE_SERVER_UDS".to_owned(),
                "/Users/cowshed-test/.cowshed/sccache.sock".to_owned()
            ),
            (
                "SCCACHE_DIR".to_owned(),
                "/Users/cowshed-test/.cowshed/caches/sccache".to_owned()
            ),
            ("SCCACHE_CACHE_SIZE".to_owned(), "40g".to_owned()),
            (
                "SCCACHE_BASEDIRS".to_owned(),
                "/Users/cowshed-test/.cowshed".to_owned()
            ),
        ]
    );

    let expected = concat!(
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
        "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" ",
        "\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n",
        "<plist version=\"1.0\">\n",
        "<dict>\n",
        "  <key>Label</key>\n",
        "  <string>dev.cowshed.sccache</string>\n",
        "  <key>ProgramArguments</key>\n",
        "  <array>\n",
        "    <string>/Users/cowshed-test/Library/Application Support/dev.cowshed/bin/sccache</string>\n",
        "  </array>\n",
        "  <key>RunAtLoad</key>\n",
        "  <true/>\n",
        "  <key>KeepAlive</key>\n",
        "  <true/>\n",
        "  <key>ProcessType</key>\n",
        "  <string>Background</string>\n",
        "  <key>StandardErrorPath</key>\n",
        "  <string>/Users/cowshed-test/Library/Logs/cowshed/sccache-stderr.log</string>\n",
        "  <key>EnvironmentVariables</key>\n",
        "  <dict>\n",
        "    <key>SCCACHE_START_SERVER</key>\n",
        "    <string>1</string>\n",
        "    <key>SCCACHE_NO_DAEMON</key>\n",
        "    <string>1</string>\n",
        "    <key>SCCACHE_IDLE_TIMEOUT</key>\n",
        "    <string>0</string>\n",
        "    <key>SCCACHE_SERVER_UDS</key>\n",
        "    <string>/Users/cowshed-test/.cowshed/sccache.sock</string>\n",
        "    <key>SCCACHE_DIR</key>\n",
        "    <string>/Users/cowshed-test/.cowshed/caches/sccache</string>\n",
        "    <key>SCCACHE_CACHE_SIZE</key>\n",
        "    <string>40g</string>\n",
        "    <key>SCCACHE_BASEDIRS</key>\n",
        "    <string>/Users/cowshed-test/.cowshed</string>\n",
        "  </dict>\n",
        "</dict>\n",
        "</plist>\n",
    );
    assert_eq!(spec.plist_bytes(), expected.as_bytes());

    // Idempotence rides on byte equality: a current plist plans no mutations.
    let bytes = spec.plist_bytes();
    let plan = plan_install(
        &spec,
        InstallState {
            launch_agents_directory_mode: Some(PRIVATE_DIRECTORY_MODE),
            plist: Some(ExistingPlist {
                bytes: &bytes,
                mode: PRIVATE_PLIST_MODE,
            }),
        },
    );
    assert!(plan.is_noop());

    // Socket and cache paths are validated like every other launchd path.
    assert!(matches!(
        LaunchAgentSpec::sccache(
            &HostStableExecutable::new(Path::new(HOME), SCCACHE_BINARY_NAME).unwrap(),
            Path::new("relative.sock"),
            Path::new("/Users/cowshed-test/.cowshed/caches/sccache"),
            ImageCapacity::from_gibibytes(40),
            Path::new("/Users/cowshed-test/.cowshed"),
        ),
        Err(LaunchdError::InvalidPath { .. })
    ));
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
    // The executable is derived from the home directory and a binary name, so a bad home or a
    // name that is not a single path component is the only way to name a bad executable.
    let cases = [
        HostStableExecutable::new(Path::new("Users/me"), COWSHED_BINARY_NAME),
        HostStableExecutable::new(Path::new("/Users/me/../other"), COWSHED_BINARY_NAME),
        HostStableExecutable::new(Path::new("/Users/me/"), COWSHED_BINARY_NAME),
        HostStableExecutable::new(Path::new("/"), COWSHED_BINARY_NAME),
        HostStableExecutable::new(Path::new(HOME), ""),
        HostStableExecutable::new(Path::new(HOME), "."),
        HostStableExecutable::new(Path::new(HOME), ".."),
        HostStableExecutable::new(Path::new(HOME), "bin/cowshed"),
        HostStableExecutable::new(Path::new(HOME), "cow\u{1}shed"),
    ];
    for result in cases {
        assert!(matches!(result, Err(LaunchdError::InvalidPath { .. })));
    }

    for label in ["", ".dev.cowshed", "dev..cowshed", "dev/cowshed"] {
        assert_eq!(
            LaunchAgentSpec::new_user(
                &cowshed_binary(),
                label,
                vec!["run".into()],
                ServiceLifecycle::RunAtLoad,
            ),
            Err(LaunchdError::InvalidLabel)
        );
    }

    assert!(matches!(
        LaunchAgentSpec::new_user(
            &cowshed_binary(),
            "dev.cowshed.empty",
            Vec::new(),
            ServiceLifecycle::RunAtLoad,
        ),
        Err(LaunchdError::InvalidArgument { .. })
    ));
    assert!(matches!(
        LaunchAgentSpec::new_user(
            &cowshed_binary(),
            "dev.cowshed.empty",
            vec!["run".into(), String::new()],
            ServiceLifecycle::RunAtLoad,
        ),
        Err(LaunchdError::InvalidArgument { index: 1, .. })
    ));
    assert_eq!(
        LaunchAgentSpec::new_user(
            &cowshed_binary(),
            "dev.cowshed.provision",
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
    CopyTemporary {
        directory: PathBuf,
        name_prefix: String,
        source: PathBuf,
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

    fn copy_exclusive_no_follow(
        &mut self,
        directory: &Path,
        name_prefix: &str,
        source: &Path,
        mode: u32,
    ) -> io::Result<PathBuf> {
        self.events.push(FilesystemEvent::CopyTemporary {
            directory: directory.to_path_buf(),
            name_prefix: name_prefix.to_owned(),
            source: source.to_path_buf(),
            mode,
        });
        self.result(FilesystemOperation::CopyTemporaryFile)?;
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
                !["sudo", "diskutil", "hdiutil", "osascript", "adopt",]
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

#[test]
fn print_status_uses_fixed_target_and_maps_loaded_and_absent_idempotently() {
    let spec = gateway();
    let plan = ControlPlan::print(503, &spec);
    let mut executor = LaunchdExecutor::new(
        FakeFilesystem::default(),
        FakeCommand::with_outputs([
            Ok(CommandOutput {
                status: CommandStatus::Success,
                stdout: b"service = dev.cowshed.gateway".to_vec(),
                stderr: Vec::new(),
            }),
            Ok(CommandOutput {
                status: CommandStatus::ExitCode(113),
                stdout: Vec::new(),
                stderr: b"Could not find service".to_vec(),
            }),
        ]),
    );

    assert_eq!(
        executor.execute_status(&plan).unwrap(),
        LaunchdServiceStatus::Loaded {
            stdout: b"service = dev.cowshed.gateway".to_vec(),
            stderr: Vec::new(),
        }
    );
    assert_eq!(
        executor.execute_status(&plan).unwrap(),
        LaunchdServiceStatus::NotLoaded {
            exit_code: 113,
            stdout: Vec::new(),
            stderr: b"Could not find service".to_vec(),
        }
    );
    assert!(matches!(
        executor.execute_status(&ControlPlan::bootstrap(503, &spec)),
        Err(ControlExecutionError::InvalidStatusPlan {
            action: ControlAction::Bootstrap,
        })
    ));

    let (_, command) = executor.into_parts();
    let expected = CommandInvocation {
        executable: PathBuf::from(LAUNCHCTL_EXECUTABLE),
        arguments: vec![
            OsString::from("print"),
            OsString::from("gui/503/dev.cowshed.gateway"),
        ],
    };
    assert_eq!(command.invocations, [expected.clone(), expected]);
}

#[test]
fn print_status_keeps_signal_and_spawn_failures_operationally_typed() {
    let spec = gateway();
    let plan = ControlPlan::print(504, &spec);
    let mut executor = LaunchdExecutor::new(
        FakeFilesystem::default(),
        FakeCommand::with_outputs([Ok(CommandOutput {
            status: CommandStatus::Terminated,
            stdout: Vec::new(),
            stderr: b"terminated".to_vec(),
        })]),
    );
    assert!(matches!(
        executor.execute_status(&plan),
        Err(ControlExecutionError::Rejected {
            action: ControlAction::Print,
            status: CommandStatus::Terminated,
            stderr,
            ..
        }) if stderr == b"terminated"
    ));

    let mut executor = LaunchdExecutor::new(
        FakeFilesystem::default(),
        FakeCommand::with_outputs([Err(io::Error::new(
            io::ErrorKind::NotFound,
            "launchctl missing",
        ))]),
    );
    assert!(matches!(
        executor.execute_status(&plan),
        Err(ControlExecutionError::Unavailable {
            action: ControlAction::Print,
            source,
        }) if source.kind() == io::ErrorKind::NotFound
    ));
}

/// The type is the guarantee: a spec can only be built from a path under
/// `~/Library/Application Support/dev.cowshed/bin`, so no caller can bake a checkout, a nix
/// store path, or a workspace image into a plist. Both agents are checked because both are
/// installed by a binary that may itself be running from anywhere.
#[test]
fn every_agent_plist_names_only_the_host_stable_binary() {
    let cowshed = cowshed_binary();
    assert_eq!(cowshed.path(), Path::new(EXECUTABLE));
    assert_eq!(cowshed.directory(), Path::new(BINARY_DIRECTORY));
    assert_eq!(cowshed.support_directory(), Path::new(SUPPORT_DIRECTORY));
    assert_eq!(cowshed.home(), Path::new(HOME));
    assert_eq!(cowshed.name(), "cowshed");

    let sccache = HostStableExecutable::new(Path::new(HOME), SCCACHE_BINARY_NAME).unwrap();
    let specs = [
        LaunchAgentSpec::gateway(&cowshed).unwrap(),
        LaunchAgentSpec::sccache(
            &sccache,
            Path::new("/Users/cowshed-test/.cowshed/sccache.sock"),
            Path::new("/Users/cowshed-test/.cowshed/caches/sccache"),
            ImageCapacity::from_gibibytes(40),
            Path::new("/Users/cowshed-test/.cowshed"),
        )
        .unwrap(),
    ];
    for (spec, expected) in specs.iter().zip([
        EXECUTABLE,
        "/Users/cowshed-test/Library/Application Support/dev.cowshed/bin/sccache",
    ]) {
        assert_eq!(spec.executable(), Path::new(expected));
        assert_eq!(spec.program_arguments().next(), Some(expected));
        let plist = String::from_utf8(spec.plist_bytes()).unwrap();
        assert!(plist.contains(&format!("  <array>\n    <string>{expected}</string>\n")));
    }
}

/// A host without the copy gets one, and a host that already has it is left alone: the source is
/// tens of megabytes, and every `start` would otherwise rewrite the binary launchd is running.
#[test]
fn stable_binary_install_plan_copies_atomically_and_repairs_modes() {
    let executable = cowshed_binary();
    let source = Path::new("/nix/store/abc-cowshed/bin/cowshed");
    let binary_directory = PathBuf::from(BINARY_DIRECTORY);

    assert_eq!(
        plan_executable_install(&executable, source, ExecutableInstallState::default())
            .operations(),
        [
            Mutation::EnsureDirectory {
                path: PathBuf::from(SUPPORT_DIRECTORY),
                mode: PRIVATE_DIRECTORY_MODE,
            },
            Mutation::EnsureDirectory {
                path: binary_directory.clone(),
                mode: PRIVATE_DIRECTORY_MODE,
            },
            Mutation::CopyToTemporaryFile {
                directory: binary_directory.clone(),
                name_prefix: ".cowshed.".into(),
                source: source.to_path_buf(),
                mode: STABLE_BINARY_MODE,
            },
            Mutation::SyncTemporaryFile,
            Mutation::RenameTemporaryFile {
                destination: PathBuf::from(EXECUTABLE),
            },
            Mutation::SyncDirectory {
                path: binary_directory.clone(),
            },
        ]
    );

    let current = ExecutableInstallState {
        support_directory_mode: Some(PRIVATE_DIRECTORY_MODE),
        binary_directory_mode: Some(PRIVATE_DIRECTORY_MODE),
        installed: Some(InstalledExecutable {
            mode: STABLE_BINARY_MODE,
            matches_source: true,
        }),
    };
    assert!(plan_executable_install(&executable, source, current).is_noop());

    // A newer build at the source, and an installed copy that lost its exec bit, both reinstall.
    for installed in [
        InstalledExecutable {
            mode: STABLE_BINARY_MODE,
            matches_source: false,
        },
        InstalledExecutable {
            mode: 0o644,
            matches_source: true,
        },
    ] {
        let plan = plan_executable_install(
            &executable,
            source,
            ExecutableInstallState {
                installed: Some(installed),
                ..current
            },
        );
        assert!(matches!(
            plan.operations(),
            [
                Mutation::CopyToTemporaryFile {
                    mode: STABLE_BINARY_MODE,
                    ..
                },
                Mutation::SyncTemporaryFile,
                Mutation::RenameTemporaryFile { .. },
                Mutation::SyncDirectory { .. },
            ]
        ));
    }

    // A world-readable directory is tightened without recopying a current binary.
    assert_eq!(
        plan_executable_install(
            &executable,
            source,
            ExecutableInstallState {
                binary_directory_mode: Some(0o755),
                ..current
            },
        )
        .operations(),
        [
            Mutation::SetPermissions {
                path: binary_directory.clone(),
                mode: PRIVATE_DIRECTORY_MODE,
            },
            Mutation::SyncDirectory {
                path: binary_directory,
            },
        ]
    );
}

/// The refusal the incident calls for: a binary on storage cowshed mounts itself cannot be the
/// thing that mounts it. Everything a host mounts before login is copied instead, because the
/// copy is what makes the agent independent of it.
#[test]
fn binary_sources_inside_cowshed_storage_are_refused_and_host_volumes_are_not() {
    let home = Path::new(HOME);

    assert_eq!(
        classify_executable_source(
            home,
            ExecutableSource {
                path: Path::new(
                    "/Users/cowshed-test/.cowshed/mnt/acme/widget/main/target/release/cowshed"
                ),
                mount_point: Path::new("/Users/cowshed-test/.cowshed/mnt/acme/widget/main"),
                mount_is_workspace: true,
            }
        ),
        Err(UnstableExecutableSource::Store {
            store: PathBuf::from("/Users/cowshed-test/.cowshed"),
        })
    );

    // A project mount outside the store, recognised by the marker every workspace root carries.
    assert_eq!(
        classify_executable_source(
            home,
            ExecutableSource {
                path: Path::new("/private/tmp/checkout/packages/cowshed/dist/native/cowshed"),
                mount_point: Path::new("/private/tmp/checkout"),
                mount_is_workspace: true,
            }
        ),
        Err(UnstableExecutableSource::Workspace {
            mount_point: PathBuf::from("/private/tmp/checkout"),
        })
    );

    // A volume mounted inside the home directory, marker or not: cowshed is the only thing that
    // mounts there, and launchd sees none of it at boot.
    assert_eq!(
        classify_executable_source(
            home,
            ExecutableSource {
                path: Path::new("/Users/cowshed-test/Dev/project/packages/cowshed/dist/cowshed"),
                mount_point: Path::new("/Users/cowshed-test/Dev/project"),
                mount_is_workspace: false,
            }
        ),
        Err(UnstableExecutableSource::HomeVolume {
            mount_point: PathBuf::from("/Users/cowshed-test/Dev/project"),
        })
    );

    for source in [
        // The nix store: its own volume, mounted before any user agent runs.
        ExecutableSource {
            path: Path::new("/nix/store/abc-cowshed/bin/cowshed"),
            mount_point: Path::new("/nix"),
            mount_is_workspace: false,
        },
        ExecutableSource {
            path: Path::new("/usr/local/bin/cowshed"),
            mount_point: Path::new("/"),
            mount_is_workspace: false,
        },
        // A global npm prefix in the home directory, on the home volume itself.
        ExecutableSource {
            path: Path::new("/Users/cowshed-test/.bun/install/global/node_modules/cowshed/cowshed"),
            mount_point: Path::new("/"),
            mount_is_workspace: false,
        },
        // The installed copy itself.
        ExecutableSource {
            path: Path::new(EXECUTABLE),
            mount_point: Path::new("/"),
            mount_is_workspace: false,
        },
    ] {
        assert_eq!(classify_executable_source(home, source), Ok(()));
    }
}

/// Binary installs ride the same temporary-file discipline as plists: launchd polls a KeepAlive
/// service hard enough that it must never observe a half-written binary at the path it runs.
#[test]
fn executor_publishes_a_binary_copy_through_a_temporary_file() {
    let executable = cowshed_binary();
    let source = PathBuf::from("/nix/store/abc-cowshed/bin/cowshed");
    let plan = plan_executable_install(&executable, &source, ExecutableInstallState::default());
    let binary_directory = PathBuf::from(BINARY_DIRECTORY);
    let temporary = binary_directory.join(".exclusive-no-follow-temp");

    let mut executor = LaunchdExecutor::new(FakeFilesystem::default(), FakeCommand::default());
    assert_eq!(
        executor.execute_install(&plan).unwrap(),
        InstallOutcome::Changed
    );
    let (filesystem, command) = executor.into_parts();
    assert!(command.invocations.is_empty());
    assert_eq!(
        filesystem.events,
        [
            FilesystemEvent::EnsureDirectory(
                PathBuf::from(SUPPORT_DIRECTORY),
                PRIVATE_DIRECTORY_MODE
            ),
            FilesystemEvent::EnsureDirectory(binary_directory.clone(), PRIVATE_DIRECTORY_MODE),
            FilesystemEvent::CopyTemporary {
                directory: binary_directory.clone(),
                name_prefix: ".cowshed.".into(),
                source,
                mode: STABLE_BINARY_MODE,
            },
            FilesystemEvent::SyncFile(temporary.clone()),
            FilesystemEvent::Rename(temporary.clone(), PathBuf::from(EXECUTABLE)),
            FilesystemEvent::SyncDirectory(binary_directory),
        ]
    );

    let mut executor = LaunchdExecutor::new(
        FakeFilesystem::failing(FilesystemOperation::CopyTemporaryFile),
        FakeCommand::default(),
    );
    let error = executor.execute_install(&plan).unwrap_err();
    assert_eq!(error.operation(), FilesystemOperation::CopyTemporaryFile);

    let mut executor = LaunchdExecutor::new(
        FakeFilesystem::failing(FilesystemOperation::RenameTemporaryFile),
        FakeCommand::default(),
    );
    let error = executor.execute_install(&plan).unwrap_err();
    assert_eq!(error.operation(), FilesystemOperation::RenameTemporaryFile);
    let (filesystem, _) = executor.into_parts();
    assert_eq!(
        filesystem.events.last(),
        Some(&FilesystemEvent::Remove(temporary))
    );
}

fn scratch(label: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!(
        "cowshed-cli-launchd-{label}-{}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).unwrap();
    path.canonicalize().unwrap()
}

/// The device walk is how a workspace image is recognised without parsing `mount(8)`: every
/// cowshed image is its own volume, so the walk stops exactly at its root.
#[test]
fn containing_mount_point_reports_the_volume_root_of_a_real_file() {
    let root = scratch("mount-point");
    let file = root.join("cowshed");
    fs::write(&file, b"binary").unwrap();

    let mount_point = containing_mount_point(&file).unwrap();
    assert!(mount_point.is_absolute());
    assert!(
        file.starts_with(&mount_point),
        "{} is not under {}",
        file.display(),
        mount_point.display()
    );
    // A file and the directory holding it are always on one volume.
    assert_eq!(mount_point, containing_mount_point(&root).unwrap());
    assert!(containing_mount_point(&root.join("absent")).is_err());

    fs::remove_dir_all(&root).unwrap();
}

/// The native adapter streams the copy into an exclusive temporary file carrying the exec bit,
/// and a copy that cannot be read leaves nothing behind for a later rename to publish.
#[test]
fn native_filesystem_copies_a_binary_with_the_exec_bit_and_exact_bytes() {
    let root = scratch("copy");
    let source = root.join("source");
    let bytes = vec![7u8; 300_000];
    fs::write(&source, &bytes).unwrap();
    let destination = root.join("cowshed");
    fs::write(&destination, b"stale").unwrap();

    let mut filesystem = NativeFilesystem::new();
    let temporary = filesystem
        .copy_exclusive_no_follow(&root, ".cowshed.", &source, STABLE_BINARY_MODE)
        .unwrap();
    filesystem.sync_file(&temporary).unwrap();
    filesystem.rename(&temporary, &destination).unwrap();

    assert_eq!(fs::read(&destination).unwrap(), bytes);
    assert_eq!(
        fs::metadata(&destination).unwrap().permissions().mode() & 0o777,
        STABLE_BINARY_MODE
    );

    assert!(
        filesystem
            .copy_exclusive_no_follow(&root, ".cowshed.", &root.join("absent"), STABLE_BINARY_MODE)
            .is_err()
    );
    let leftovers: Vec<_> = fs::read_dir(&root)
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .filter(|name| name.to_string_lossy().starts_with(".cowshed."))
        .collect();
    assert!(
        leftovers.is_empty(),
        "temporary files leaked: {leftovers:?}"
    );

    fs::remove_dir_all(&root).unwrap();
}
