//! Immutable launchd service definitions, filesystem plans, and native executors.
//!
//! The planner is pure. Execution is isolated behind injectable filesystem and
//! command adapters so callers can keep one mutable, actor-owned executor while
//! tests remain entirely host-independent.

use std::error::Error;
use std::ffi::OsString;
use std::fmt;
use std::fs::{self, File, OpenOptions};
use std::io::{self, Write};
use std::path::{Component, Path, PathBuf};
use std::process::Command;

pub const GATEWAY_LABEL: &str = "dev.cowshed.gateway";
pub const PRIVATE_DIRECTORY_MODE: u32 = 0o700;
pub const PRIVATE_PLIST_MODE: u32 = 0o600;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ServiceLifecycle {
    /// Start at login and restart whenever the service exits.
    KeepAlive,
    /// Run once when the agent is loaded.
    RunAtLoad,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LaunchAgentSpec {
    label: String,
    executable: PathBuf,
    arguments: Vec<String>,
    lifecycle: ServiceLifecycle,
    plist_path: PathBuf,
    standard_error_path: PathBuf,
}

impl LaunchAgentSpec {
    pub fn new_user(
        home: &Path,
        label: impl Into<String>,
        executable: &Path,
        arguments: Vec<String>,
        lifecycle: ServiceLifecycle,
    ) -> Result<Self, LaunchdError> {
        validate_canonical_absolute_path("home", home)?;
        validate_canonical_absolute_path("executable", executable)?;

        let label = label.into();
        validate_label(&label)?;
        validate_arguments(&arguments)?;

        let plist_path = home
            .join("Library")
            .join("LaunchAgents")
            .join(format!("{label}.plist"));
        let standard_error_path = home
            .join(".cowshed")
            .join("telemetry")
            .join("daemon-stderr.log");

        Ok(Self {
            label,
            executable: executable.to_path_buf(),
            arguments,
            lifecycle,
            plist_path,
            standard_error_path,
        })
    }

    pub fn gateway(home: &Path, executable: &Path) -> Result<Self, LaunchdError> {
        Self::new_user(
            home,
            GATEWAY_LABEL,
            executable,
            vec!["gateway".into(), "run".into()],
            ServiceLifecycle::KeepAlive,
        )
    }

    pub fn label(&self) -> &str {
        &self.label
    }

    pub fn executable(&self) -> &Path {
        &self.executable
    }

    pub fn arguments(&self) -> &[String] {
        &self.arguments
    }

    pub fn lifecycle(&self) -> ServiceLifecycle {
        self.lifecycle
    }

    pub fn plist_path(&self) -> &Path {
        &self.plist_path
    }

    pub fn launch_agents_directory(&self) -> &Path {
        self.plist_path
            .parent()
            .expect("validated plist paths always have a parent")
    }

    pub fn standard_error_path(&self) -> &Path {
        &self.standard_error_path
    }

    pub fn program_arguments(&self) -> impl Iterator<Item = &str> {
        std::iter::once(
            self.executable
                .to_str()
                .expect("validated executable paths are UTF-8"),
        )
        .chain(self.arguments.iter().map(String::as_str))
    }

    pub fn plist_bytes(&self) -> Vec<u8> {
        let mut plist = String::from(concat!(
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n",
            "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" ",
            "\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n",
            "<plist version=\"1.0\">\n",
            "<dict>\n",
            "  <key>Label</key>\n",
            "  ",
        ));
        push_xml_string(&mut plist, &self.label);
        plist.push_str("  <key>ProgramArguments</key>\n  <array>\n");
        for argument in self.program_arguments() {
            plist.push_str("    ");
            push_xml_string(&mut plist, argument);
        }
        plist.push_str("  </array>\n  <key>RunAtLoad</key>\n  <true/>\n  <key>KeepAlive</key>\n");
        match self.lifecycle {
            ServiceLifecycle::KeepAlive => plist.push_str("  <true/>\n"),
            ServiceLifecycle::RunAtLoad => plist.push_str("  <false/>\n"),
        }
        plist.push_str(
            "  <key>ProcessType</key>\n  <string>Background</string>\n  <key>StandardErrorPath</key>\n  ",
        );
        push_xml_string(
            &mut plist,
            self.standard_error_path
                .to_str()
                .expect("validated home paths are UTF-8"),
        );
        plist.push_str("</dict>\n</plist>\n");
        plist.into_bytes()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ExistingPlist<'a> {
    pub bytes: &'a [u8],
    pub mode: u32,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct InstallState<'a> {
    /// `None` means `~/Library/LaunchAgents` does not exist.
    pub launch_agents_directory_mode: Option<u32>,
    pub plist: Option<ExistingPlist<'a>>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InstallPlan {
    operations: Vec<Mutation>,
}

impl InstallPlan {
    pub fn operations(&self) -> &[Mutation] {
        &self.operations
    }

    pub fn is_noop(&self) -> bool {
        self.operations.is_empty()
    }
}

/// An ordered, filesystem-only mutation plan.
///
/// `CreateExclusiveTemporaryFile` produces the temporary file consumed by the
/// immediately following temporary-file operations. The executor must choose a
/// unique suffix, open with exclusive creation and no symlink following, and
/// clean up that file if a later operation fails.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Mutation {
    EnsureDirectory {
        path: PathBuf,
        mode: u32,
    },
    SetPermissions {
        path: PathBuf,
        mode: u32,
    },
    CreateExclusiveTemporaryFile {
        directory: PathBuf,
        name_prefix: String,
        bytes: Vec<u8>,
        mode: u32,
    },
    SyncTemporaryFile,
    RenameTemporaryFile {
        destination: PathBuf,
    },
    RemoveFile {
        path: PathBuf,
    },
    SyncDirectory {
        path: PathBuf,
    },
}

pub fn plan_install(spec: &LaunchAgentSpec, state: InstallState<'_>) -> InstallPlan {
    let directory = spec.launch_agents_directory().to_path_buf();
    let mut operations = Vec::new();

    match state.launch_agents_directory_mode {
        None => operations.push(Mutation::EnsureDirectory {
            path: directory.clone(),
            mode: PRIVATE_DIRECTORY_MODE,
        }),
        Some(mode) if mode != PRIVATE_DIRECTORY_MODE => {
            operations.push(Mutation::SetPermissions {
                path: directory.clone(),
                mode: PRIVATE_DIRECTORY_MODE,
            });
        }
        Some(_) => {}
    }

    let desired = spec.plist_bytes();
    let plist_is_current = state
        .plist
        .is_some_and(|plist| plist.mode == PRIVATE_PLIST_MODE && plist.bytes == desired);

    if !plist_is_current {
        operations.push(Mutation::CreateExclusiveTemporaryFile {
            directory: directory.clone(),
            name_prefix: format!(".{}.plist.", spec.label()),
            bytes: desired,
            mode: PRIVATE_PLIST_MODE,
        });
        operations.push(Mutation::SyncTemporaryFile);
        operations.push(Mutation::RenameTemporaryFile {
            destination: spec.plist_path().to_path_buf(),
        });
    }

    if !operations.is_empty() {
        operations.push(Mutation::SyncDirectory { path: directory });
    }

    InstallPlan { operations }
}

pub fn plan_remove(spec: &LaunchAgentSpec, installed: bool) -> InstallPlan {
    let operations = if installed {
        vec![
            Mutation::RemoveFile {
                path: spec.plist_path().to_path_buf(),
            },
            Mutation::SyncDirectory {
                path: spec.launch_agents_directory().to_path_buf(),
            },
        ]
    } else {
        Vec::new()
    };

    InstallPlan { operations }
}

pub const LAUNCHCTL_EXECUTABLE: &str = "/bin/launchctl";

/// Filesystem operations required to execute an [`InstallPlan`].
///
/// Implementations of `create_exclusive_no_follow` must either return a fully
/// written file with exactly `mode`, or remove any file they created before
/// returning an error.
pub trait LaunchdFilesystem {
    fn ensure_directory(&mut self, path: &Path, mode: u32) -> io::Result<()>;
    fn set_permissions(&mut self, path: &Path, mode: u32) -> io::Result<()>;
    fn create_exclusive_no_follow(
        &mut self,
        directory: &Path,
        name_prefix: &str,
        bytes: &[u8],
        mode: u32,
    ) -> io::Result<PathBuf>;
    fn sync_file(&mut self, path: &Path) -> io::Result<()>;
    fn rename(&mut self, source: &Path, destination: &Path) -> io::Result<()>;
    fn remove_file(&mut self, path: &Path) -> io::Result<()>;
    fn sync_directory(&mut self, path: &Path) -> io::Result<()>;
}

/// Native, no-shell filesystem adapter for per-user LaunchAgent files.
#[derive(Debug, Default)]
pub struct NativeFilesystem {
    next_temporary_id: u64,
}

impl NativeFilesystem {
    pub fn new() -> Self {
        Self::default()
    }
}

impl LaunchdFilesystem for NativeFilesystem {
    fn ensure_directory(&mut self, path: &Path, mode: u32) -> io::Result<()> {
        use std::os::unix::fs::DirBuilderExt;

        let mut builder = fs::DirBuilder::new();
        builder.mode(mode);
        match builder.create(path) {
            Ok(()) => {}
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        set_exact_permissions_no_follow(path, mode, FileKind::Directory)
    }

    fn set_permissions(&mut self, path: &Path, mode: u32) -> io::Result<()> {
        set_exact_permissions_no_follow(path, mode, FileKind::Directory)
    }

    fn create_exclusive_no_follow(
        &mut self,
        directory: &Path,
        name_prefix: &str,
        bytes: &[u8],
        mode: u32,
    ) -> io::Result<PathBuf> {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        for _ in 0..128 {
            let id = self.next_temporary_id;
            self.next_temporary_id = self.next_temporary_id.wrapping_add(1);
            let path = directory.join(format!("{name_prefix}{}.{}", std::process::id(), id));
            let opened = OpenOptions::new()
                .write(true)
                .create_new(true)
                .mode(mode)
                .custom_flags(no_follow_flag())
                .open(&path);
            let mut file = match opened {
                Ok(file) => file,
                Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            };

            let write_result = (|| {
                let metadata = file.metadata()?;
                if !metadata.file_type().is_file() {
                    return Err(wrong_file_kind("temporary path", FileKind::RegularFile));
                }
                file.set_permissions(fs::Permissions::from_mode(mode))?;
                file.write_all(bytes)
            })();
            if let Err(error) = write_result {
                drop(file);
                let _ = fs::remove_file(&path);
                return Err(error);
            }
            return Ok(path);
        }

        Err(io::Error::new(
            io::ErrorKind::AlreadyExists,
            "could not allocate an exclusive launchd temporary file",
        ))
    }

    fn sync_file(&mut self, path: &Path) -> io::Result<()> {
        let file = open_existing_no_follow(path, true)?;
        require_file_kind(&file, "temporary path", FileKind::RegularFile)?;
        file.sync_all()
    }

    fn rename(&mut self, source: &Path, destination: &Path) -> io::Result<()> {
        fs::rename(source, destination)
    }

    fn remove_file(&mut self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn sync_directory(&mut self, path: &Path) -> io::Result<()> {
        let directory = open_existing_no_follow(path, false)?;
        require_file_kind(&directory, "directory path", FileKind::Directory)?;
        directory.sync_all()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FileKind {
    Directory,
    RegularFile,
}

fn set_exact_permissions_no_follow(path: &Path, mode: u32, kind: FileKind) -> io::Result<()> {
    use std::os::unix::fs::PermissionsExt;

    let file = open_existing_no_follow(path, false)?;
    require_file_kind(&file, "launchd path", kind)?;
    file.set_permissions(fs::Permissions::from_mode(mode))
}

fn open_existing_no_follow(path: &Path, write: bool) -> io::Result<File> {
    use std::os::unix::fs::OpenOptionsExt;

    let mut options = OpenOptions::new();
    options
        .read(true)
        .write(write)
        .custom_flags(no_follow_flag());
    options.open(path)
}

const fn no_follow_flag() -> i32 {
    #[cfg(any(target_os = "linux", target_os = "android"))]
    {
        0x2_0000
    }
    #[cfg(not(any(target_os = "linux", target_os = "android")))]
    {
        0x100
    }
}

fn require_file_kind(file: &File, subject: &'static str, expected: FileKind) -> io::Result<()> {
    let actual = file.metadata()?.file_type();
    let matches = match expected {
        FileKind::Directory => actual.is_dir(),
        FileKind::RegularFile => actual.is_file(),
    };
    if matches {
        Ok(())
    } else {
        Err(wrong_file_kind(subject, expected))
    }
}

fn wrong_file_kind(subject: &'static str, expected: FileKind) -> io::Error {
    let expected = match expected {
        FileKind::Directory => "directory",
        FileKind::RegularFile => "regular file",
    };
    io::Error::new(
        io::ErrorKind::InvalidInput,
        format!("{subject} is not a {expected}"),
    )
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FilesystemOperation {
    EnsureDirectory,
    SetPermissions,
    CreateTemporaryFile,
    SyncTemporaryFile,
    RenameTemporaryFile,
    RemoveFile,
    SyncDirectory,
}

#[derive(Debug)]
pub struct CleanupFailure {
    path: PathBuf,
    source: io::Error,
}

impl CleanupFailure {
    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn source_error(&self) -> &io::Error {
        &self.source
    }
}

#[derive(Debug)]
pub enum InstallExecutionError {
    Filesystem {
        operation: FilesystemOperation,
        path: PathBuf,
        source: io::Error,
        cleanup_failure: Option<CleanupFailure>,
    },
    InvalidPlan {
        operation: FilesystemOperation,
        reason: &'static str,
    },
}

impl InstallExecutionError {
    pub fn operation(&self) -> FilesystemOperation {
        match self {
            Self::Filesystem { operation, .. } | Self::InvalidPlan { operation, .. } => *operation,
        }
    }

    pub fn cleanup_failure(&self) -> Option<&CleanupFailure> {
        match self {
            Self::Filesystem {
                cleanup_failure, ..
            } => cleanup_failure.as_ref(),
            Self::InvalidPlan { .. } => None,
        }
    }
}

impl fmt::Display for InstallExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Filesystem {
                operation,
                path,
                source,
                cleanup_failure,
            } => {
                write!(
                    formatter,
                    "launchd filesystem operation {operation:?} failed for {}: {source}",
                    path.display()
                )?;
                if let Some(cleanup) = cleanup_failure {
                    write!(
                        formatter,
                        "; cleanup of {} also failed: {}",
                        cleanup.path.display(),
                        cleanup.source
                    )?;
                }
                Ok(())
            }
            Self::InvalidPlan { operation, reason } => {
                write!(formatter, "invalid launchd plan at {operation:?}: {reason}")
            }
        }
    }
}

impl Error for InstallExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Filesystem { source, .. } => Some(source),
            Self::InvalidPlan { .. } => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum InstallOutcome {
    NoChange,
    Changed,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ControlAction {
    Bootstrap,
    Bootout,
    Kickstart,
    Print,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlPlan {
    action: ControlAction,
    arguments: Vec<OsString>,
}

impl ControlPlan {
    pub fn bootstrap(uid: u32, spec: &LaunchAgentSpec) -> Self {
        Self {
            action: ControlAction::Bootstrap,
            arguments: vec![
                OsString::from("bootstrap"),
                OsString::from(gui_domain(uid)),
                spec.plist_path().as_os_str().to_owned(),
            ],
        }
    }

    pub fn bootout(uid: u32, spec: &LaunchAgentSpec) -> Self {
        Self {
            action: ControlAction::Bootout,
            arguments: vec![
                OsString::from("bootout"),
                OsString::from(service_target(uid, spec.label())),
            ],
        }
    }

    pub fn kickstart(uid: u32, spec: &LaunchAgentSpec) -> Self {
        Self {
            action: ControlAction::Kickstart,
            arguments: vec![
                OsString::from("kickstart"),
                OsString::from("-k"),
                OsString::from(service_target(uid, spec.label())),
            ],
        }
    }

    pub fn print(uid: u32, spec: &LaunchAgentSpec) -> Self {
        Self {
            action: ControlAction::Print,
            arguments: vec![
                OsString::from("print"),
                OsString::from(service_target(uid, spec.label())),
            ],
        }
    }

    pub fn action(&self) -> ControlAction {
        self.action
    }

    pub fn executable(&self) -> &Path {
        Path::new(LAUNCHCTL_EXECUTABLE)
    }

    pub fn arguments(&self) -> &[OsString] {
        &self.arguments
    }
}

fn gui_domain(uid: u32) -> String {
    format!("gui/{uid}")
}

fn service_target(uid: u32, label: &str) -> String {
    format!("{}/{label}", gui_domain(uid))
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CommandStatus {
    Success,
    ExitCode(i32),
    Terminated,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CommandOutput {
    pub status: CommandStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl CommandOutput {
    pub fn success() -> Self {
        Self {
            status: CommandStatus::Success,
            stdout: Vec::new(),
            stderr: Vec::new(),
        }
    }
}

pub trait LaunchctlCommand {
    fn run(&mut self, executable: &Path, arguments: &[OsString]) -> io::Result<CommandOutput>;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NativeLaunchctlCommand;

impl LaunchctlCommand for NativeLaunchctlCommand {
    fn run(&mut self, executable: &Path, arguments: &[OsString]) -> io::Result<CommandOutput> {
        let output = Command::new(executable).args(arguments).output()?;
        let status = if output.status.success() {
            CommandStatus::Success
        } else if let Some(code) = output.status.code() {
            CommandStatus::ExitCode(code)
        } else {
            CommandStatus::Terminated
        };
        Ok(CommandOutput {
            status,
            stdout: output.stdout,
            stderr: output.stderr,
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ControlOutcome {
    pub action: ControlAction,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LaunchdServiceStatus {
    Loaded {
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
    NotLoaded {
        exit_code: i32,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
}

#[derive(Debug)]
pub enum ControlExecutionError {
    Unavailable {
        action: ControlAction,
        source: io::Error,
    },
    Rejected {
        action: ControlAction,
        status: CommandStatus,
        stdout: Vec<u8>,
        stderr: Vec<u8>,
    },
    InvalidStatusPlan {
        action: ControlAction,
    },
}

impl fmt::Display for ControlExecutionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unavailable { action, source } => {
                write!(formatter, "launchctl {action:?} could not start: {source}")
            }
            Self::Rejected { action, status, .. } => {
                write!(formatter, "launchctl {action:?} failed with {status:?}")
            }
            Self::InvalidStatusPlan { action } => {
                write!(
                    formatter,
                    "launchctl {action:?} is not a service status plan"
                )
            }
        }
    }
}

impl Error for ControlExecutionError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Unavailable { source, .. } => Some(source),
            Self::Rejected { .. } | Self::InvalidStatusPlan { .. } => None,
        }
    }
}

/// Single-owner executor for install plans and launchctl control plans.
///
/// All methods require `&mut self`; callers should keep this value inside their
/// coordinator actor rather than sharing it through a lock.
#[derive(Debug)]
pub struct LaunchdExecutor<F, C> {
    filesystem: F,
    command: C,
}

impl<F, C> LaunchdExecutor<F, C> {
    pub fn new(filesystem: F, command: C) -> Self {
        Self {
            filesystem,
            command,
        }
    }

    pub fn into_parts(self) -> (F, C) {
        (self.filesystem, self.command)
    }
}

impl LaunchdExecutor<NativeFilesystem, NativeLaunchctlCommand> {
    pub fn native() -> Self {
        Self::new(NativeFilesystem::new(), NativeLaunchctlCommand)
    }
}

impl<F: LaunchdFilesystem, C> LaunchdExecutor<F, C> {
    pub fn execute_install(
        &mut self,
        plan: &InstallPlan,
    ) -> Result<InstallOutcome, InstallExecutionError> {
        if plan.is_noop() {
            return Ok(InstallOutcome::NoChange);
        }

        let mut temporary_file: Option<PathBuf> = None;
        for mutation in plan.operations() {
            let (operation, path, result) = match mutation {
                Mutation::EnsureDirectory { path, mode } => (
                    FilesystemOperation::EnsureDirectory,
                    path.as_path(),
                    self.filesystem.ensure_directory(path, *mode),
                ),
                Mutation::SetPermissions { path, mode } => (
                    FilesystemOperation::SetPermissions,
                    path.as_path(),
                    self.filesystem.set_permissions(path, *mode),
                ),
                Mutation::CreateExclusiveTemporaryFile {
                    directory,
                    name_prefix,
                    bytes,
                    mode,
                } => {
                    if temporary_file.is_some() {
                        return Err(InstallExecutionError::InvalidPlan {
                            operation: FilesystemOperation::CreateTemporaryFile,
                            reason: "a temporary file is already active",
                        });
                    }
                    match self.filesystem.create_exclusive_no_follow(
                        directory,
                        name_prefix,
                        bytes,
                        *mode,
                    ) {
                        Ok(path) => {
                            temporary_file = Some(path);
                            continue;
                        }
                        Err(source) => (
                            FilesystemOperation::CreateTemporaryFile,
                            directory.as_path(),
                            Err(source),
                        ),
                    }
                }
                Mutation::SyncTemporaryFile => {
                    let Some(path) = temporary_file.as_deref() else {
                        return Err(InstallExecutionError::InvalidPlan {
                            operation: FilesystemOperation::SyncTemporaryFile,
                            reason: "there is no active temporary file",
                        });
                    };
                    (
                        FilesystemOperation::SyncTemporaryFile,
                        path,
                        self.filesystem.sync_file(path),
                    )
                }
                Mutation::RenameTemporaryFile { destination } => {
                    let Some(path) = temporary_file.as_deref() else {
                        return Err(InstallExecutionError::InvalidPlan {
                            operation: FilesystemOperation::RenameTemporaryFile,
                            reason: "there is no active temporary file",
                        });
                    };
                    let result = self.filesystem.rename(path, destination);
                    if result.is_ok() {
                        temporary_file = None;
                    }
                    (
                        FilesystemOperation::RenameTemporaryFile,
                        destination.as_path(),
                        result,
                    )
                }
                Mutation::RemoveFile { path } => (
                    FilesystemOperation::RemoveFile,
                    path.as_path(),
                    self.filesystem.remove_file(path),
                ),
                Mutation::SyncDirectory { path } => (
                    FilesystemOperation::SyncDirectory,
                    path.as_path(),
                    self.filesystem.sync_directory(path),
                ),
            };

            if let Err(source) = result {
                let failed_path = path.to_path_buf();
                let cleanup_failure = temporary_file.take().and_then(|path| {
                    self.filesystem
                        .remove_file(&path)
                        .err()
                        .map(|source| CleanupFailure { path, source })
                });
                return Err(InstallExecutionError::Filesystem {
                    operation,
                    path: failed_path,
                    source,
                    cleanup_failure,
                });
            }
        }

        if temporary_file.is_some() {
            return Err(InstallExecutionError::InvalidPlan {
                operation: FilesystemOperation::RenameTemporaryFile,
                reason: "the plan left a temporary file active",
            });
        }
        Ok(InstallOutcome::Changed)
    }
}

impl<F, C: LaunchctlCommand> LaunchdExecutor<F, C> {
    pub fn execute_control(
        &mut self,
        plan: &ControlPlan,
    ) -> Result<ControlOutcome, ControlExecutionError> {
        let output = self
            .command
            .run(plan.executable(), plan.arguments())
            .map_err(|source| ControlExecutionError::Unavailable {
                action: plan.action(),
                source,
            })?;
        match output.status {
            CommandStatus::Success => Ok(ControlOutcome {
                action: plan.action(),
                stdout: output.stdout,
                stderr: output.stderr,
            }),
            status => Err(ControlExecutionError::Rejected {
                action: plan.action(),
                status,
                stdout: output.stdout,
                stderr: output.stderr,
            }),
        }
    }

    /// Executes a `launchctl print` plan and classifies the idempotent loaded
    /// state. A normal non-zero exit means the service is absent; failure to
    /// spawn or signal termination remains an operational error.
    pub fn execute_status(
        &mut self,
        plan: &ControlPlan,
    ) -> Result<LaunchdServiceStatus, ControlExecutionError> {
        if plan.action() != ControlAction::Print {
            return Err(ControlExecutionError::InvalidStatusPlan {
                action: plan.action(),
            });
        }
        let output = self
            .command
            .run(plan.executable(), plan.arguments())
            .map_err(|source| ControlExecutionError::Unavailable {
                action: ControlAction::Print,
                source,
            })?;
        match output.status {
            CommandStatus::Success => Ok(LaunchdServiceStatus::Loaded {
                stdout: output.stdout,
                stderr: output.stderr,
            }),
            CommandStatus::ExitCode(exit_code) => Ok(LaunchdServiceStatus::NotLoaded {
                exit_code,
                stdout: output.stdout,
                stderr: output.stderr,
            }),
            CommandStatus::Terminated => Err(ControlExecutionError::Rejected {
                action: ControlAction::Print,
                status: CommandStatus::Terminated,
                stdout: output.stdout,
                stderr: output.stderr,
            }),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum LaunchdError {
    InvalidPath {
        field: &'static str,
        reason: &'static str,
    },
    InvalidLabel,
    InvalidArgument {
        index: usize,
        reason: &'static str,
    },
    PrivilegedProvisioning,
}

impl fmt::Display for LaunchdError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidPath { field, reason } => {
                write!(formatter, "invalid {field} path: {reason}")
            }
            Self::InvalidLabel => formatter.write_str("invalid launchd label"),
            Self::InvalidArgument { index, reason } => {
                write!(formatter, "invalid service argument {index}: {reason}")
            }
            Self::PrivilegedProvisioning => formatter
                .write_str("launchd services may not invoke foreground storage provisioning"),
        }
    }
}

impl Error for LaunchdError {}

fn validate_canonical_absolute_path(field: &'static str, path: &Path) -> Result<(), LaunchdError> {
    let value = path.to_str().ok_or(LaunchdError::InvalidPath {
        field,
        reason: "must be UTF-8",
    })?;
    if value.is_empty() || !path.is_absolute() {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "must be absolute",
        });
    }
    if path.parent().is_none() {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "must not be the filesystem root",
        });
    }
    if value.chars().any(is_unsafe_xml_control) {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "contains a control character",
        });
    }
    if path
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "must be lexically normalized",
        });
    }
    let normalized: PathBuf = path.components().collect();
    if normalized.as_os_str() != path.as_os_str() {
        return Err(LaunchdError::InvalidPath {
            field,
            reason: "must use its canonical lexical spelling",
        });
    }
    Ok(())
}

fn validate_label(label: &str) -> Result<(), LaunchdError> {
    let valid = !label.is_empty()
        && !label.starts_with('.')
        && !label.ends_with('.')
        && label.split('.').all(|component| !component.is_empty())
        && label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-'));
    if valid {
        Ok(())
    } else {
        Err(LaunchdError::InvalidLabel)
    }
}

fn validate_arguments(arguments: &[String]) -> Result<(), LaunchdError> {
    if arguments.is_empty() {
        return Err(LaunchdError::InvalidArgument {
            index: 0,
            reason: "at least one service argument is required",
        });
    }
    if arguments
        .first()
        .is_some_and(|argument| argument == "adopt")
    {
        return Err(LaunchdError::PrivilegedProvisioning);
    }
    for (index, argument) in arguments.iter().enumerate() {
        if argument.is_empty() {
            return Err(LaunchdError::InvalidArgument {
                index,
                reason: "must not be empty",
            });
        }
        if argument.chars().any(is_unsafe_xml_control) {
            return Err(LaunchdError::InvalidArgument {
                index,
                reason: "contains a control character",
            });
        }
    }
    Ok(())
}

fn is_unsafe_xml_control(character: char) -> bool {
    matches!(character, '\0'..='\u{8}' | '\u{b}' | '\u{c}' | '\u{e}'..='\u{1f}' | '\u{7f}')
}

fn push_xml_string(output: &mut String, value: &str) {
    output.push_str("<string>");
    for character in value.chars() {
        match character {
            '&' => output.push_str("&amp;"),
            '<' => output.push_str("&lt;"),
            '>' => output.push_str("&gt;"),
            '\'' => output.push_str("&apos;"),
            '"' => output.push_str("&quot;"),
            _ => output.push(character),
        }
    }
    output.push_str("</string>\n");
}
