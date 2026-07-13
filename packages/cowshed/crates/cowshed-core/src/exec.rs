use std::ffi::{OsStr, OsString};
use std::fmt;
use std::io;
use std::os::unix::process::CommandExt;
use std::path::{Component, Path, PathBuf};
use std::process::{Command, ExitStatus, Stdio};

use crate::sandbox::{SandboxConfig, SandboxError, SandboxProfileRole, seatbelt_profile};

pub const SANDBOX_EXEC: &str = "/usr/bin/sandbox-exec";

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SandboxExecRequest {
    pub argv: Vec<OsString>,
    /// Absolute, or relative to the workspace mount.
    pub cwd: PathBuf,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SpawnPlan {
    pub program: PathBuf,
    pub args: Vec<OsString>,
    pub cwd: PathBuf,
}

#[derive(Debug)]
pub struct ExecOutcome {
    /// The exact status returned by the sandboxed child, including signal state.
    pub status: ExitStatus,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WrapperStage {
    ValidateProfile,
    PrepareChildDescriptors,
    Spawn,
    Wait,
}

#[derive(Debug)]
pub enum ExecError {
    InvalidRequest {
        message: String,
    },
    SandboxDenied {
        message: String,
    },
    WrapperFailure {
        stage: WrapperStage,
        message: String,
        source: Option<io::Error>,
    },
}

impl fmt::Display for ExecError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { message }
            | Self::SandboxDenied { message }
            | Self::WrapperFailure { message, .. } => formatter.write_str(message),
        }
    }
}

impl std::error::Error for ExecError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::WrapperFailure {
                source: Some(source),
                ..
            } => Some(source),
            _ => None,
        }
    }
}

/// Converts authoritative controller inputs into an argv-only sandbox-exec launch.
/// No child-provided text or environment participates in this plan.
pub fn plan_exec(
    request: SandboxExecRequest,
    sandbox: &SandboxConfig,
) -> Result<SpawnPlan, ExecError> {
    validate_argv(&request.argv)?;
    let cwd = contained_cwd(&sandbox.workspace_mount, &request.cwd)?;
    let profile =
        seatbelt_profile(sandbox, SandboxProfileRole::ExecutedChild).map_err(map_sandbox_error)?;

    let mut args = Vec::with_capacity(request.argv.len() + 3);
    args.push(OsString::from("-p"));
    args.push(OsString::from(profile));
    args.push(OsString::from("--"));
    args.extend(request.argv);

    Ok(SpawnPlan {
        program: PathBuf::from(SANDBOX_EXEC),
        args,
        cwd,
    })
}

pub trait SpawnRunner {
    fn run(&self, plan: &SpawnPlan) -> Result<ExitStatus, SpawnFailure>;
}

#[derive(Debug)]
pub struct SpawnFailure {
    pub stage: WrapperStage,
    pub source: io::Error,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemSpawnRunner;

const DESCRIPTOR_PREPARATION_ERRNO: libc::c_int = libc::EOWNERDEAD;
const SUPERVISOR_FD_CEILING: usize = 4_096;

#[cfg(any(not(target_os = "macos"), test))]
fn descriptor_limit_with<GetLimit>(get_limit: GetLimit) -> io::Result<libc::rlim_t>
where
    GetLimit: FnOnce(*mut libc::rlimit) -> libc::c_int,
{
    let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
    if get_limit(limit.as_mut_ptr()) == -1 {
        return Err(io::Error::last_os_error());
    }
    Ok(unsafe { limit.assume_init() }.rlim_cur)
}

#[cfg(any(not(target_os = "macos"), test))]
fn descriptor_limit() -> io::Result<libc::rlim_t> {
    descriptor_limit_with(|limit| unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit) })
}

#[cfg(target_os = "macos")]
fn validate_fd_listing_size(bytes: libc::c_int, capacity: usize) -> io::Result<usize> {
    if bytes < 0 {
        return Err(io::Error::last_os_error());
    }
    let bytes = bytes as usize;
    if bytes > capacity || !bytes.is_multiple_of(std::mem::size_of::<libc::proc_fdinfo>()) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "open descriptor listing exceeds the supervisor FD ceiling",
        ));
    }
    Ok(bytes / std::mem::size_of::<libc::proc_fdinfo>())
}

#[cfg(target_os = "macos")]
fn mark_macos_non_stdio_close_on_exec(
    descriptors: &mut [std::mem::MaybeUninit<libc::proc_fdinfo>],
) -> io::Result<()> {
    let capacity = std::mem::size_of_val(descriptors);
    let required = unsafe {
        libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDLISTFDS,
            0,
            std::ptr::null_mut(),
            0,
        )
    };
    validate_fd_listing_size(required, capacity)?;
    let bytes = unsafe {
        libc::proc_pidinfo(
            libc::getpid(),
            libc::PROC_PIDLISTFDS,
            0,
            descriptors.as_mut_ptr().cast(),
            capacity as libc::c_int,
        )
    };
    let count = validate_fd_listing_size(bytes, capacity)?;
    for descriptor in &descriptors[..count] {
        let descriptor = unsafe { descriptor.assume_init_ref() }.proc_fd;
        if descriptor > libc::STDERR_FILENO {
            mark_descriptor_close_on_exec(descriptor)?;
        }
    }
    Ok(())
}

fn mark_descriptor_close_on_exec_with<Fcntl, LastError>(
    descriptor: libc::c_int,
    mut fcntl: Fcntl,
    last_error: LastError,
) -> io::Result<()>
where
    Fcntl: FnMut(libc::c_int, libc::c_int, libc::c_int) -> libc::c_int,
    LastError: Fn() -> io::Error,
{
    let flags = fcntl(descriptor, libc::F_GETFD, 0);
    if flags == -1 {
        let error = last_error();
        if error.raw_os_error() == Some(libc::EBADF) {
            return Ok(());
        }
        return Err(error);
    }
    if flags & libc::FD_CLOEXEC == 0 {
        let result = fcntl(descriptor, libc::F_SETFD, flags | libc::FD_CLOEXEC);
        if result == -1 {
            return Err(last_error());
        }
    }
    Ok(())
}

fn mark_descriptor_close_on_exec(descriptor: libc::c_int) -> io::Result<()> {
    mark_descriptor_close_on_exec_with(
        descriptor,
        |descriptor, command, argument| unsafe { libc::fcntl(descriptor, command, argument) },
        io::Error::last_os_error,
    )
}

#[cfg(any(not(target_os = "macos"), test))]
fn fallback_descriptor_limit(limit: libc::rlim_t) -> io::Result<libc::c_int> {
    if limit > SUPERVISOR_FD_CEILING as libc::rlim_t {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "descriptor fallback limit exceeds the supervisor FD ceiling",
        ));
    }
    Ok(limit as libc::c_int)
}

#[cfg(any(not(target_os = "macos"), test))]
fn mark_descriptor_range_close_on_exec_with<MarkDescriptor>(
    limit: libc::rlim_t,
    mut mark_descriptor: MarkDescriptor,
) -> io::Result<()>
where
    MarkDescriptor: FnMut(libc::c_int) -> io::Result<()>,
{
    let limit = fallback_descriptor_limit(limit)?;
    for descriptor in 3..limit {
        mark_descriptor(descriptor)?;
    }
    Ok(())
}

#[cfg(any(target_os = "linux", test))]
const CLOSE_RANGE_CLOEXEC: libc::c_uint = 1 << 2;

#[cfg(any(target_os = "linux", test))]
fn mark_non_stdio_close_on_exec_with<CloseRange, MarkDescriptor>(
    limit: libc::rlim_t,
    close_range: CloseRange,
    mark_descriptor: MarkDescriptor,
) -> io::Result<()>
where
    CloseRange: FnOnce(libc::c_uint, libc::c_uint, libc::c_uint) -> io::Result<()>,
    MarkDescriptor: FnMut(libc::c_int) -> io::Result<()>,
{
    match close_range(3, libc::c_uint::MAX, CLOSE_RANGE_CLOEXEC) {
        Ok(()) => Ok(()),
        Err(error)
            if matches!(
                error.raw_os_error(),
                Some(libc::ENOSYS) | Some(libc::EINVAL)
            ) =>
        {
            mark_descriptor_range_close_on_exec_with(limit, mark_descriptor)
        }
        Err(error) => Err(error),
    }
}

#[cfg(any(not(target_os = "macos"), test))]
fn mark_non_stdio_close_on_exec(limit: libc::rlim_t) -> io::Result<()> {
    #[cfg(target_os = "linux")]
    {
        return mark_non_stdio_close_on_exec_with(
            limit,
            |first, last, flags| {
                let result = unsafe { libc::syscall(libc::SYS_close_range, first, last, flags) };
                if result == 0 {
                    Ok(())
                } else {
                    Err(io::Error::last_os_error())
                }
            },
            mark_descriptor_close_on_exec,
        );
    }

    #[cfg(not(target_os = "linux"))]
    mark_descriptor_range_close_on_exec_with(limit, mark_descriptor_close_on_exec)
}

impl SpawnRunner for SystemSpawnRunner {
    fn run(&self, plan: &SpawnPlan) -> Result<ExitStatus, SpawnFailure> {
        #[cfg(not(target_os = "macos"))]
        let descriptor_limit = descriptor_limit().map_err(|source| SpawnFailure {
            stage: WrapperStage::PrepareChildDescriptors,
            source,
        })?;
        #[cfg(target_os = "macos")]
        let mut descriptors = Box::<[libc::proc_fdinfo]>::new_uninit_slice(SUPERVISOR_FD_CEILING);

        let mut command = Command::new(&plan.program);
        command
            .args(&plan.args)
            .current_dir(&plan.cwd)
            .stdin(Stdio::inherit())
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        unsafe {
            #[cfg(target_os = "macos")]
            command.pre_exec(move || {
                mark_macos_non_stdio_close_on_exec(&mut descriptors)
                    .map_err(|_| io::Error::from_raw_os_error(DESCRIPTOR_PREPARATION_ERRNO))
            });
            #[cfg(not(target_os = "macos"))]
            command.pre_exec(move || {
                mark_non_stdio_close_on_exec(descriptor_limit)
                    .map_err(|_| io::Error::from_raw_os_error(DESCRIPTOR_PREPARATION_ERRNO))
            });
        }

        let mut child = command.spawn().map_err(|source| SpawnFailure {
            stage: if source.raw_os_error() == Some(DESCRIPTOR_PREPARATION_ERRNO) {
                WrapperStage::PrepareChildDescriptors
            } else {
                WrapperStage::Spawn
            },
            source,
        })?;
        child.wait().map_err(|source| SpawnFailure {
            stage: WrapperStage::Wait,
            source,
        })
    }
}

pub fn execute_with<R: SpawnRunner>(
    request: SandboxExecRequest,
    sandbox: &SandboxConfig,
    runner: &R,
) -> Result<ExecOutcome, ExecError> {
    let plan = plan_exec(request, sandbox)?;
    let status = runner
        .run(&plan)
        .map_err(|failure| ExecError::WrapperFailure {
            stage: failure.stage,
            message: format!(
                "sandbox wrapper failed during {:?}: {}",
                failure.stage, failure.source
            ),
            source: Some(failure.source),
        })?;
    Ok(ExecOutcome { status })
}

pub fn execute(
    request: SandboxExecRequest,
    sandbox: &SandboxConfig,
) -> Result<ExecOutcome, ExecError> {
    execute_with(request, sandbox, &SystemSpawnRunner)
}

fn validate_argv(argv: &[OsString]) -> Result<(), ExecError> {
    if argv.is_empty() {
        return Err(ExecError::InvalidRequest {
            message: "exec requires a non-empty argv".into(),
        });
    }
    if argv[0].is_empty() {
        return Err(ExecError::InvalidRequest {
            message: "exec argv[0] must not be empty".into(),
        });
    }
    if argv.iter().any(|argument| contains_nul(argument)) {
        return Err(ExecError::InvalidRequest {
            message: "exec argv must not contain NUL".into(),
        });
    }
    Ok(())
}

fn contains_nul(value: &OsStr) -> bool {
    value.as_encoded_bytes().contains(&0)
}

fn contained_cwd(workspace_mount: &Path, requested: &Path) -> Result<PathBuf, ExecError> {
    if !workspace_mount.is_absolute() {
        return Err(ExecError::WrapperFailure {
            stage: WrapperStage::ValidateProfile,
            message: format!(
                "workspace mount is not absolute: {}",
                workspace_mount.display()
            ),
            source: None,
        });
    }
    if has_traversal(workspace_mount) {
        return Err(ExecError::WrapperFailure {
            stage: WrapperStage::ValidateProfile,
            message: format!(
                "workspace mount is not canonical: {}",
                workspace_mount.display()
            ),
            source: None,
        });
    }
    if has_traversal(requested) {
        return Err(ExecError::SandboxDenied {
            message: format!("cwd traversal is not allowed: {}", requested.display()),
        });
    }

    let workspace =
        std::fs::canonicalize(workspace_mount).map_err(|source| ExecError::WrapperFailure {
            stage: WrapperStage::ValidateProfile,
            message: format!(
                "could not resolve workspace mount {}: {source}",
                workspace_mount.display()
            ),
            source: Some(source),
        })?;
    if workspace != workspace_mount {
        return Err(ExecError::WrapperFailure {
            stage: WrapperStage::ValidateProfile,
            message: format!(
                "workspace mount is not canonical: {} resolves to {}",
                workspace_mount.display(),
                workspace.display()
            ),
            source: None,
        });
    }

    let candidate = if requested.is_absolute() {
        requested.to_path_buf()
    } else {
        workspace.join(requested)
    };
    let cwd = std::fs::canonicalize(&candidate).map_err(|source| ExecError::InvalidRequest {
        message: format!("could not resolve cwd {}: {source}", candidate.display()),
    })?;
    if !cwd.starts_with(&workspace) {
        return Err(ExecError::SandboxDenied {
            message: format!(
                "cwd {} is outside workspace {}",
                cwd.display(),
                workspace.display()
            ),
        });
    }
    Ok(cwd)
}

fn has_traversal(path: &Path) -> bool {
    path.components()
        .any(|component| matches!(component, Component::ParentDir | Component::CurDir))
}

fn map_sandbox_error(error: SandboxError) -> ExecError {
    match error {
        SandboxError::GrantIntersectsDeny { .. } => ExecError::SandboxDenied {
            message: error.to_string(),
        },
        SandboxError::InvalidPortBlock { .. } | SandboxError::InvalidPath { .. } => {
            ExecError::WrapperFailure {
                stage: WrapperStage::ValidateProfile,
                message: error.to_string(),
                source: None,
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::fs;
    use std::os::unix::process::ExitStatusExt;
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::*;
    use crate::sandbox::{PortBlock, RunSandboxMode, SandboxGrants};

    static NEXT_DIR: AtomicU64 = AtomicU64::new(0);

    struct TestTree {
        root: PathBuf,
        workspace: PathBuf,
        cwd: PathBuf,
    }

    impl TestTree {
        fn new() -> Self {
            let sequence = NEXT_DIR.fetch_add(1, Ordering::Relaxed);
            let root_alias = std::env::temp_dir().join(format!(
                "cowshed-exec-test-{}-{sequence}",
                std::process::id()
            ));
            let cwd_alias = root_alias.join("workspace/nested");
            fs::create_dir_all(&cwd_alias).unwrap();
            let root = fs::canonicalize(&root_alias).unwrap();
            let workspace = root.join("workspace");
            let cwd = workspace.join("nested");
            Self {
                root,
                workspace,
                cwd,
            }
        }

        fn sandbox(&self) -> SandboxConfig {
            SandboxConfig {
                home: PathBuf::from("/Users/tester"),
                workspace_mount: self.workspace.clone(),
                exec_temp_dir: self.root.join("tmp"),
                port_block: PortBlock::new(40_960, 16).unwrap(),
                mode: RunSandboxMode::ReadWrite,
                grants: SandboxGrants::default(),
                allowed_unix_sockets: vec![],
                additional_denies: vec![],
            }
        }
    }

    impl Drop for TestTree {
        fn drop(&mut self) {
            fs::remove_dir_all(&self.root).unwrap();
        }
    }

    #[derive(Debug)]
    struct FakeRunner {
        status: ExitStatus,
        plans: RefCell<Vec<SpawnPlan>>,
    }

    impl SpawnRunner for FakeRunner {
        fn run(&self, plan: &SpawnPlan) -> Result<ExitStatus, SpawnFailure> {
            self.plans.borrow_mut().push(plan.clone());
            Ok(self.status)
        }
    }

    #[test]
    fn exec_errors_preserve_messages_and_io_sources() {
        let invalid = ExecError::InvalidRequest {
            message: "bad argv".into(),
        };
        assert_eq!(invalid.to_string(), "bad argv");
        assert!(std::error::Error::source(&invalid).is_none());

        let denied = ExecError::SandboxDenied {
            message: "outside workspace".into(),
        };
        assert_eq!(denied.to_string(), "outside workspace");
        assert!(std::error::Error::source(&denied).is_none());

        let wrapper = ExecError::WrapperFailure {
            stage: WrapperStage::Spawn,
            message: "wrapper could not start".into(),
            source: Some(io::Error::new(
                io::ErrorKind::NotFound,
                "missing executable",
            )),
        };
        assert_eq!(wrapper.to_string(), "wrapper could not start");
        let source = std::error::Error::source(&wrapper).unwrap();
        assert_eq!(source.to_string(), "missing executable");
    }

    #[test]
    fn argv_rejects_empty_programs_and_nul_bytes() {
        let tree = TestTree::new();
        for (argv, expected_message) in [
            (vec![], "exec requires a non-empty argv"),
            (vec![OsString::new()], "exec argv[0] must not be empty"),
            (
                vec![OsString::from("printf"), OsString::from("bad\0argument")],
                "exec argv must not contain NUL",
            ),
        ] {
            let error = plan_exec(
                SandboxExecRequest {
                    argv,
                    cwd: tree.cwd.clone(),
                },
                &tree.sandbox(),
            )
            .unwrap_err();
            assert_eq!(error.to_string(), expected_message);
        }
    }

    #[test]
    fn system_runner_returns_the_real_child_status() {
        let tree = TestTree::new();
        let status = SystemSpawnRunner
            .run(&SpawnPlan {
                program: PathBuf::from("/usr/bin/false"),
                args: vec![],
                cwd: tree.cwd.clone(),
            })
            .unwrap();
        assert_eq!(status.code(), Some(1));
    }

    #[test]
    fn system_runner_reports_spawn_failures() {
        let tree = TestTree::new();
        let failure = SystemSpawnRunner
            .run(&SpawnPlan {
                program: tree.root.join("missing-executable"),
                args: vec![],
                cwd: tree.cwd.clone(),
            })
            .unwrap_err();
        assert_eq!(failure.stage, WrapperStage::Spawn);
        assert_eq!(failure.source.kind(), io::ErrorKind::NotFound);
    }

    #[test]
    fn system_runner_does_not_inherit_non_stdio_descriptors() {
        use std::os::fd::AsRawFd;

        let tree = TestTree::new();
        let descriptor_file = fs::File::create(tree.root.join("inheritable")).unwrap();
        let descriptor = descriptor_file.as_raw_fd();
        let original_flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        assert_ne!(original_flags, -1);
        let inheritable_flags = original_flags & !libc::FD_CLOEXEC;
        assert_ne!(
            unsafe { libc::fcntl(descriptor, libc::F_SETFD, inheritable_flags) },
            -1
        );

        let status = SystemSpawnRunner
            .run(&SpawnPlan {
                program: PathBuf::from("/bin/sh"),
                args: vec![
                    OsString::from("-c"),
                    OsString::from(format!("test ! -e /dev/fd/{descriptor}")),
                ],
                cwd: tree.cwd.clone(),
            })
            .unwrap();

        assert_ne!(
            unsafe { libc::fcntl(descriptor, libc::F_SETFD, original_flags) },
            -1
        );
        assert!(status.success());
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn descriptor_listing_accepts_low_and_high_bounds() {
        let entry_size = std::mem::size_of::<libc::proc_fdinfo>();
        let capacity = 2 * entry_size;

        assert_eq!(validate_fd_listing_size(0, capacity).unwrap(), 0);
        assert_eq!(
            validate_fd_listing_size(entry_size as libc::c_int, capacity).unwrap(),
            1
        );
        assert_eq!(
            validate_fd_listing_size(capacity as libc::c_int, capacity).unwrap(),
            2
        );
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn descriptor_listing_rejects_negative_misaligned_and_overflow_sizes() {
        let entry_size = std::mem::size_of::<libc::proc_fdinfo>();
        let capacity = 2 * entry_size;

        assert!(validate_fd_listing_size(-1, capacity).is_err());
        for invalid in [entry_size - 1, capacity + entry_size] {
            let error = validate_fd_listing_size(invalid as libc::c_int, capacity).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                error.to_string(),
                "open descriptor listing exceeds the supervisor FD ceiling"
            );
        }
    }

    #[test]
    fn descriptor_limit_reports_the_kernel_soft_limit() {
        let mut expected = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, expected.as_mut_ptr()) },
            0
        );
        let expected = unsafe { expected.assume_init() }.rlim_cur;

        assert!(expected > libc::STDERR_FILENO as libc::rlim_t);
        assert_eq!(descriptor_limit().unwrap(), expected);
    }

    #[test]
    fn descriptor_limit_injection_preserves_boundaries_and_errors() {
        for expected in [
            SUPERVISOR_FD_CEILING as libc::rlim_t - 1,
            SUPERVISOR_FD_CEILING as libc::rlim_t,
            libc::RLIM_INFINITY,
        ] {
            let actual = descriptor_limit_with(|limit| {
                unsafe {
                    limit.write(libc::rlimit {
                        rlim_cur: expected,
                        rlim_max: libc::RLIM_INFINITY,
                    });
                }
                0
            })
            .unwrap();
            assert_eq!(actual, expected);
        }

        unsafe {
            libc::close(-1);
        }
        let error = descriptor_limit_with(|_| -1).unwrap_err();
        assert_eq!(error.raw_os_error(), Some(libc::EBADF));
    }

    #[test]
    fn descriptor_fallback_enforces_infinity_ceiling_and_boundaries() {
        for accepted in [
            0,
            SUPERVISOR_FD_CEILING as libc::rlim_t - 1,
            SUPERVISOR_FD_CEILING as libc::rlim_t,
        ] {
            assert_eq!(
                fallback_descriptor_limit(accepted).unwrap(),
                accepted as libc::c_int
            );
        }

        for rejected in [
            SUPERVISOR_FD_CEILING as libc::rlim_t + 1,
            libc::RLIM_INFINITY,
        ] {
            let error = fallback_descriptor_limit(rejected).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert_eq!(
                error.to_string(),
                "descriptor fallback limit exceeds the supervisor FD ceiling"
            );
        }
    }

    #[test]
    fn descriptor_cloexec_preserves_existing_flags() {
        let descriptor = 17;
        let original_flags = 0b1010;
        let invocations = RefCell::new(Vec::new());

        mark_descriptor_close_on_exec_with(
            descriptor,
            |actual_descriptor, command, argument| {
                invocations
                    .borrow_mut()
                    .push((actual_descriptor, command, argument));
                if command == libc::F_GETFD {
                    original_flags
                } else {
                    0
                }
            },
            || panic!("successful fcntl calls must not read errno"),
        )
        .unwrap();

        assert_eq!(
            invocations.into_inner(),
            vec![
                (descriptor, libc::F_GETFD, 0),
                (descriptor, libc::F_SETFD, original_flags | libc::FD_CLOEXEC)
            ]
        );
    }

    #[test]
    fn descriptor_cloexec_skips_an_already_marked_descriptor() {
        let invocations = std::cell::Cell::new(0);

        mark_descriptor_close_on_exec_with(
            17,
            |_, command, _| {
                invocations.set(invocations.get() + 1);
                assert_eq!(command, libc::F_GETFD);
                0b1010 | libc::FD_CLOEXEC
            },
            || panic!("successful fcntl calls must not read errno"),
        )
        .unwrap();

        assert_eq!(invocations.get(), 1);
    }

    #[test]
    fn descriptor_cloexec_handles_closed_descriptors_and_propagates_errors() {
        mark_descriptor_close_on_exec(-1).unwrap();
        mark_descriptor_close_on_exec_with(
            17,
            |_, _, _| -1,
            || io::Error::from_raw_os_error(libc::EBADF),
        )
        .unwrap();

        let get_error = mark_descriptor_close_on_exec_with(
            17,
            |_, _, _| -1,
            || io::Error::from_raw_os_error(libc::EIO),
        )
        .unwrap_err();
        assert_eq!(get_error.raw_os_error(), Some(libc::EIO));

        let invocation = std::cell::Cell::new(0_u8);
        let set_error = mark_descriptor_close_on_exec_with(
            17,
            |_, _, _| {
                let current = invocation.get();
                invocation.set(current + 1);
                if current == 0 { 0 } else { -1 }
            },
            || io::Error::from_raw_os_error(libc::EPERM),
        )
        .unwrap_err();
        assert_eq!(set_error.raw_os_error(), Some(libc::EPERM));
        assert_eq!(invocation.get(), 2);
    }

    #[test]
    fn close_range_uses_the_cloexec_flag_and_full_non_stdio_bounds() {
        let invocation = RefCell::new(None);
        let fallback_called = std::cell::Cell::new(false);

        mark_non_stdio_close_on_exec_with(
            SUPERVISOR_FD_CEILING as libc::rlim_t,
            |first, last, flags| {
                invocation.replace(Some((first, last, flags)));
                Ok(())
            },
            |_| {
                fallback_called.set(true);
                Ok(())
            },
        )
        .unwrap();

        assert_eq!(
            invocation.into_inner(),
            Some((3, libc::c_uint::MAX, CLOSE_RANGE_CLOEXEC))
        );
        assert_eq!(CLOSE_RANGE_CLOEXEC, 4);
        assert!(!fallback_called.get());
    }

    #[test]
    fn close_range_fallback_visits_every_descriptor_below_the_bound() {
        for unavailable_errno in [libc::ENOSYS, libc::EINVAL] {
            let visited = RefCell::new(Vec::new());
            mark_non_stdio_close_on_exec_with(
                7,
                |_, _, _| Err(io::Error::from_raw_os_error(unavailable_errno)),
                |descriptor| {
                    visited.borrow_mut().push(descriptor);
                    Ok(())
                },
            )
            .unwrap();

            assert_eq!(visited.into_inner(), vec![3, 4, 5, 6]);
        }
    }

    #[test]
    fn close_range_and_fallback_errors_propagate() {
        let close_range_error = mark_non_stdio_close_on_exec_with(
            7,
            |_, _, _| Err(io::Error::from_raw_os_error(libc::EPERM)),
            |_| panic!("fallback must not run for an unexpected close_range error"),
        )
        .unwrap_err();
        assert_eq!(close_range_error.raw_os_error(), Some(libc::EPERM));

        let visited = RefCell::new(Vec::new());
        let fallback_error = mark_non_stdio_close_on_exec_with(
            7,
            |_, _, _| Err(io::Error::from_raw_os_error(libc::ENOSYS)),
            |descriptor| {
                visited.borrow_mut().push(descriptor);
                if descriptor == 5 {
                    Err(io::Error::from_raw_os_error(libc::EIO))
                } else {
                    Ok(())
                }
            },
        )
        .unwrap_err();

        assert_eq!(fallback_error.raw_os_error(), Some(libc::EIO));
        assert_eq!(visited.into_inner(), vec![3, 4, 5]);
    }

    #[test]
    fn descriptor_fallback_marks_every_descriptor_below_its_bound() {
        use std::os::fd::AsRawFd;

        let tree = TestTree::new();
        let file = fs::File::open(&tree.root).unwrap();
        let descriptor = file.as_raw_fd();
        let original = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        assert_ne!(original, -1);
        assert_ne!(
            unsafe { libc::fcntl(descriptor, libc::F_SETFD, original & !libc::FD_CLOEXEC) },
            -1
        );

        mark_non_stdio_close_on_exec(libc::rlim_t::try_from(descriptor).unwrap() + 1).unwrap();
        let prepared = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
        assert_ne!(
            unsafe { libc::fcntl(descriptor, libc::F_SETFD, original) },
            -1
        );

        assert_ne!(prepared, -1);
        assert_ne!(prepared & libc::FD_CLOEXEC, 0);
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn macos_descriptor_preparation_never_marks_stderr_close_on_exec() {
        let original = unsafe { libc::fcntl(libc::STDERR_FILENO, libc::F_GETFD) };
        assert_ne!(original, -1);
        assert_ne!(
            unsafe {
                libc::fcntl(
                    libc::STDERR_FILENO,
                    libc::F_SETFD,
                    original & !libc::FD_CLOEXEC,
                )
            },
            -1
        );
        let mut descriptors = Box::<[libc::proc_fdinfo]>::new_uninit_slice(SUPERVISOR_FD_CEILING);

        mark_macos_non_stdio_close_on_exec(&mut descriptors).unwrap();
        let prepared = unsafe { libc::fcntl(libc::STDERR_FILENO, libc::F_GETFD) };
        assert_ne!(
            unsafe { libc::fcntl(libc::STDERR_FILENO, libc::F_SETFD, original) },
            -1
        );

        assert_ne!(prepared, -1);
        assert_eq!(prepared & libc::FD_CLOEXEC, 0);
    }

    #[test]
    fn system_runner_closes_descriptors_opened_during_spawn() {
        use std::os::fd::AsRawFd;
        use std::sync::atomic::AtomicBool;
        use std::sync::{Arc, Barrier};
        use std::thread;

        let tree = TestTree::new();
        let marker = tree.root.join("raced-inheritable");
        fs::write(&marker, b"marker").unwrap();
        let running = Arc::new(AtomicBool::new(true));
        let ready = Arc::new(Barrier::new(2));
        let opener_running = Arc::clone(&running);
        let opener_ready = Arc::clone(&ready);
        let opener_marker = marker.clone();
        let opener = thread::spawn(move || {
            let mut held = Vec::with_capacity(64);
            let first = fs::File::open(&opener_marker).unwrap();
            let first_descriptor = first.as_raw_fd();
            let first_flags = unsafe { libc::fcntl(first_descriptor, libc::F_GETFD) };
            assert_ne!(first_flags, -1);
            assert_ne!(
                unsafe {
                    libc::fcntl(
                        first_descriptor,
                        libc::F_SETFD,
                        first_flags & !libc::FD_CLOEXEC,
                    )
                },
                -1
            );
            held.push(first);
            opener_ready.wait();

            while opener_running.load(Ordering::Acquire) {
                let file = fs::File::open(&opener_marker).unwrap();
                let descriptor = file.as_raw_fd();
                let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFD) };
                assert_ne!(flags, -1);
                assert_ne!(
                    unsafe { libc::fcntl(descriptor, libc::F_SETFD, flags & !libc::FD_CLOEXEC,) },
                    -1
                );
                if held.len() == held.capacity() {
                    held.remove(0);
                }
                held.push(file);
            }
        });
        ready.wait();

        let script = format!(
            "for fd in /dev/fd/*; do [ \"$(/usr/bin/readlink \"$fd\" 2>/dev/null)\" = \"{}\" ] && exit 1; done; exit 0",
            marker.display()
        );
        for _ in 0..8 {
            let status = SystemSpawnRunner
                .run(&SpawnPlan {
                    program: PathBuf::from("/bin/sh"),
                    args: vec![OsString::from("-c"), OsString::from(&script)],
                    cwd: tree.cwd.clone(),
                })
                .unwrap();
            assert!(status.success());
        }

        running.store(false, Ordering::Release);
        opener.join().unwrap();
    }

    #[test]
    fn argv_is_passed_as_distinct_values_without_a_shell() {
        let tree = TestTree::new();
        let request = SandboxExecRequest {
            argv: vec![
                OsString::from("printf"),
                OsString::from("%s"),
                OsString::from("$(touch /tmp/never); a b"),
            ],
            cwd: PathBuf::from("nested"),
        };
        let plan = plan_exec(request, &tree.sandbox()).unwrap();
        assert_eq!(plan.program, Path::new(SANDBOX_EXEC));
        assert_eq!(plan.args[0], "-p");
        assert_eq!(plan.args[2], "--");
        assert_eq!(
            &plan.args[3..],
            ["printf", "%s", "$(touch /tmp/never); a b"]
        );
        assert_eq!(plan.cwd, tree.cwd);
    }

    #[test]
    fn launch_plan_cannot_select_the_trusted_supervisor_profile() {
        let tree = TestTree::new();
        let sandbox = tree.sandbox();
        let plan = plan_exec(
            SandboxExecRequest {
                argv: vec![OsString::from("true")],
                cwd: tree.cwd.clone(),
            },
            &sandbox,
        )
        .unwrap();
        let planned_profile = plan.args[1].to_string_lossy();
        let child = seatbelt_profile(&sandbox, SandboxProfileRole::ExecutedChild).unwrap();
        let supervisor = seatbelt_profile(&sandbox, SandboxProfileRole::TrustedSupervisor).unwrap();

        assert_eq!(planned_profile, child);
        assert_ne!(planned_profile, supervisor);
        assert!(
            planned_profile
                .lines()
                .last()
                .unwrap()
                .starts_with("(deny file-write* (literal ")
        );
    }

    #[test]
    fn cwd_traversal_and_symlink_escape_are_authoritative_denials() {
        let tree = TestTree::new();
        let traversal = SandboxExecRequest {
            argv: vec![OsString::from("true")],
            cwd: PathBuf::from("../outside"),
        };
        assert!(matches!(
            plan_exec(traversal, &tree.sandbox()),
            Err(ExecError::SandboxDenied { .. })
        ));

        let outside = tree.root.join("outside");
        fs::create_dir_all(&outside).unwrap();
        std::os::unix::fs::symlink(&outside, tree.workspace.join("escape")).unwrap();
        let symlink = SandboxExecRequest {
            argv: vec![OsString::from("true")],
            cwd: PathBuf::from("escape"),
        };
        assert!(matches!(
            plan_exec(symlink, &tree.sandbox()),
            Err(ExecError::SandboxDenied { .. })
        ));
    }

    #[test]
    fn child_status_passes_through_unchanged() {
        let tree = TestTree::new();
        let status = ExitStatus::from_raw(37 << 8);
        let runner = FakeRunner {
            status,
            plans: RefCell::new(vec![]),
        };
        let request = SandboxExecRequest {
            argv: vec![OsString::from("false")],
            cwd: tree.cwd.clone(),
        };
        let outcome = execute_with(request, &tree.sandbox(), &runner).unwrap();
        assert_eq!(outcome.status, status);
        assert_eq!(runner.plans.borrow().len(), 1);
    }

    #[test]
    fn denied_grant_never_reaches_spawn() {
        let tree = TestTree::new();
        let runner = FakeRunner {
            status: ExitStatus::from_raw(0),
            plans: RefCell::new(vec![]),
        };
        let mut sandbox = tree.sandbox();
        sandbox
            .grants
            .read
            .push(PathBuf::from("/Users/tester/.ssh"));
        let request = SandboxExecRequest {
            argv: vec![OsString::from("true")],
            cwd: tree.cwd.clone(),
        };
        assert!(matches!(
            execute_with(request, &sandbox, &runner),
            Err(ExecError::SandboxDenied { .. })
        ));
        assert!(runner.plans.borrow().is_empty());
    }

    #[test]
    fn wrapper_io_failure_is_not_a_child_exit() {
        struct FailingRunner;
        impl SpawnRunner for FailingRunner {
            fn run(&self, _plan: &SpawnPlan) -> Result<ExitStatus, SpawnFailure> {
                Err(SpawnFailure {
                    stage: WrapperStage::Spawn,
                    source: io::Error::new(io::ErrorKind::NotFound, "missing wrapper"),
                })
            }
        }

        let tree = TestTree::new();
        let request = SandboxExecRequest {
            argv: vec![OsString::from("true")],
            cwd: tree.cwd.clone(),
        };
        assert!(matches!(
            execute_with(request, &tree.sandbox(), &FailingRunner),
            Err(ExecError::WrapperFailure {
                stage: WrapperStage::Spawn,
                ..
            })
        ));
    }
}
