use std::io::Read as _;
use std::process::Command;

use super::*;
use crate::error::ErrorCode;
use cowshed_os::fork_lock::Run as _;

const CHILD: &str = "COWSHED_DIAGNOSTICS_NOFILE_CHILD";
const TEST: &str = "runtime::shell_job::diagnostics_tests::descriptor_exhaustion_reports_the_diagnostics_clone_failure";

#[cfg(not(target_os = "linux"))]
fn isolated_command() -> Command {
    let mut command = Command::new(std::env::current_exe().expect("test executable"));
    command
        .args(["--exact", TEST, "--nocapture"])
        .env(CHILD, "1");
    command
}

#[cfg(target_os = "linux")]
fn delegated_command() -> Command {
    // SAFETY: getuid/getgid read this ordinary test runner's credentials.
    let (uid, gid) = unsafe { (libc::getuid(), libc::getgid()) };
    assert_ne!(uid, 0, "run the fixture as the ordinary CI user");
    let mut command = Command::new("sudo");
    command.args(["-n", "env"]).arg(format!("{CHILD}=delegate"));
    command.arg(format!("COWSHED_DIAGNOSTICS_UID={uid}"));
    command.arg(format!("COWSHED_DIAGNOSTICS_GID={gid}"));
    let mut path = OsString::from("PATH=");
    path.push(std::env::var_os("PATH").expect("PATH"));
    command.arg(path);
    command.args([
        "systemd-run",
        "--scope",
        "--quiet",
        "--collect",
        "--property=Delegate=yes",
        "--",
    ]);
    command.arg(std::env::current_exe().expect("test executable"));
    command.args(["--exact", TEST, "--nocapture"]);
    command
}

#[cfg(target_os = "linux")]
fn delegate_and_run() {
    use super::super::job_cgroup::{CGROUP_MOUNT, CONTROLLER_LEAF};

    let uid = std::env::var("COWSHED_DIAGNOSTICS_UID")
        .expect("uid")
        .parse::<u32>()
        .expect("uid");
    let gid = std::env::var("COWSHED_DIAGNOSTICS_GID")
        .expect("gid")
        .parse::<u32>()
        .expect("gid");
    // SAFETY: geteuid reads this scope setup process's credentials.
    assert_eq!(unsafe { libc::geteuid() }, 0, "scope setup requires root");
    let membership = std::fs::read_to_string("/proc/self/cgroup").expect("scope membership");
    let relative = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .filter(|path| path.starts_with('/'))
        .expect("cgroup v2 membership");
    let scope = Path::new(CGROUP_MOUNT).join(relative.trim_start_matches('/'));
    for name in [
        "",
        "cgroup.procs",
        "cgroup.threads",
        "cgroup.subtree_control",
    ] {
        std::os::unix::fs::chown(scope.join(name), Some(uid), Some(gid)).expect("delegate scope");
    }
    let leaf = scope.join(CONTROLLER_LEAF);
    std::fs::create_dir(&leaf).expect("controller leaf");
    for name in ["", "cgroup.procs", "cgroup.threads"] {
        std::os::unix::fs::chown(leaf.join(name), Some(uid), Some(gid)).expect("delegate leaf");
    }
    std::fs::write(leaf.join("cgroup.procs"), "0").expect("move waiting setup into leaf");
    let mut command = Command::new("setpriv");
    command
        .arg("--reuid")
        .arg(uid.to_string())
        .arg("--regid")
        .arg(gid.to_string());
    command.args(["--clear-groups", "--"]);
    command.arg(std::env::current_exe().expect("test executable"));
    command
        .args(["--exact", TEST, "--nocapture"])
        .env(CHILD, "1");
    run_isolated(command);
}

fn run_isolated(mut command: Command) {
    let output = command
        .output_locked()
        .expect("run isolated descriptor-limit supervisor");
    assert!(
        output.status.success(),
        "descriptor-limit supervisor failed: {}\nstdout: {}\nstderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
    assert!(
        String::from_utf8_lossy(&output.stdout).contains("COWSHED_DIAGNOSTICS_VERIFIED"),
        "the isolated subprocess must execute the selected case"
    );
    print!("{}", String::from_utf8_lossy(&output.stdout));
}

/// Only the isolated child changes its process-wide limit; every existing descriptor remains
/// usable, but the real F_DUPFD_CLOEXEC boundary answers EMFILE.
struct DescriptorLimit(libc::rlimit, Vec<std::fs::File>);

impl DescriptorLimit {
    fn exhausted(highest: RawFd) -> Self {
        let mut limit = std::mem::MaybeUninit::<libc::rlimit>::uninit();
        // SAFETY: getrlimit initializes the writable rlimit on success.
        assert_eq!(
            unsafe { libc::getrlimit(libc::RLIMIT_NOFILE, limit.as_mut_ptr()) },
            0,
            "read descriptor limit: {}",
            io::Error::last_os_error()
        );
        // SAFETY: getrlimit succeeded and initialized the complete rlimit.
        let original = unsafe { limit.assume_init() };
        let exhausted = libc::rlimit {
            rlim_cur: libc::rlim_t::try_from(highest).expect("positive descriptor") + 1,
            rlim_max: original.rlim_max,
        };
        // SAFETY: exhausted points to a complete rlimit; only this child process is changed.
        assert_eq!(
            unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &exhausted) },
            0,
            "exhaust descriptor limit: {}",
            io::Error::last_os_error()
        );
        let mut guard = Self(original, Vec::new());
        loop {
            match std::fs::File::open("/dev/null") {
                Ok(file) => guard.1.push(file),
                Err(error) => {
                    assert_eq!(error.raw_os_error(), Some(libc::EMFILE));
                    return guard;
                }
            }
        }
    }
}

impl Drop for DescriptorLimit {
    fn drop(&mut self) {
        // SAFETY: the saved rlimit is initialized, and restoring the unchanged hard limit is
        // permitted for this process. Restoration precedes runtime/test-harness teardown.
        assert_eq!(
            unsafe { libc::setrlimit(libc::RLIMIT_NOFILE, &self.0) },
            0,
            "restore descriptor limit: {}",
            io::Error::last_os_error()
        );
    }
}

#[test]
fn descriptor_exhaustion_reports_the_diagnostics_clone_failure() {
    #[cfg(target_os = "linux")]
    if std::env::var_os(CHILD).as_deref() == Some(std::ffi::OsStr::new("delegate")) {
        delegate_and_run();
        return;
    }
    if std::env::var_os(CHILD).is_none() {
        #[cfg(target_os = "linux")]
        let command = delegated_command();
        #[cfg(not(target_os = "linux"))]
        let command = isolated_command();
        run_isolated(command);
        return;
    }

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("supervisor runtime");
    runtime.block_on(async {
        let pool = ShellPool::start(
            Arc::new(HostActivator {
                program: ShellHostProgram::dedicated(
                    std::env::current_exe().expect("test executable"),
                ),
                workspace_mount: PathBuf::from("/"),
                profile: crate::runtime::supervisor::ChildProfile::unbound(
                    "",
                    &std::env::temp_dir().join("cowshed-diagnostics-profiles"),
                ),
                environment: BTreeMap::new(),
                grants: Default::default(),
                envrc_directory: None,
                cpu: Arc::new(crate::runtime::supervisor::ImmediateCpu),
            }),
            ShellPoolConfig {
                prewarm: false,
                ..ShellPoolConfig::default()
            },
        );
        let job_id = JobId::new(17).expect("job id");
        #[cfg(target_os = "linux")]
        let cgroup = {
            use super::super::job_cgroup::CgroupAuthority;
            let authority = CgroupAuthority::delegated().expect("real delegated cgroup");
            let incarnation = crate::metadata::WorkspaceIncarnation::new(
                uuid::Uuid::new_v4().simple().to_string(),
            )
            .expect("incarnation");
            Arc::new(
                authority
                    .incarnation(&incarnation)
                    .expect("incarnation cgroups")
                    .admit(job_id)
                    .expect("admit before descriptor exhaustion"),
            )
        };
        let (stdin, stdin_writer) = pipe().expect("job stdin");
        let (stdout_reader, stdout) = pipe().expect("job stdout");
        let (stderr_reader, stderr) = pipe().expect("job stderr");
        let control = Arc::new(JobControl {
            target: Mutex::new(Target::Pending),
            requested: AtomicI32::new(0),
            signalled: tokio::sync::Notify::new(),
            #[cfg(target_os = "linux")]
            cgroup: Arc::clone(&cgroup),
        });
        let (_release, released) = oneshot::channel();
        let (events, mut observed) = mpsc::channel(4);
        #[cfg(target_os = "macos")]
        let monitor = super::super::process_monitor::ProcessMonitor::prepare(job_id)
            .expect("prepare observation before exhausting descriptors");

        let limit = DescriptorLimit::exhausted(stderr.as_raw_fd());
        let clone_error = stderr.try_clone().expect_err("the real dup must fail");
        assert_eq!(clone_error.raw_os_error(), Some(libc::EMFILE));
        drive(
            pool,
            JobIo {
                stdin,
                stdout,
                stderr,
            },
            RunCommand {
                command: HostCommand::Argv(vec![OsString::from("true")]),
                cwd: PathBuf::from("/"),
                overlay: Vec::new(),
                nice: 10,
                activation: super::super::supervisor::ActivationCover::Held,
                #[cfg(target_os = "linux")]
                cgroup: Arc::clone(&cgroup),
                #[cfg(target_os = "macos")]
                monitor,
            },
            Arc::clone(&control),
            released,
            job_id,
            events,
        )
        .await;
        drop(limit);
        drop(stdin_writer);

        let event = observed.recv().await.expect("typed launch failure");
        let ProcessEvent::LaunchFailed {
            job_id: failed_job,
            error,
        } = event
        else {
            panic!("expected a launch failure, got {event:?}");
        };
        assert_eq!(failed_job, job_id);
        assert_eq!(error.code, ErrorCode::EnvironmentMissing);
        assert!(
            error
                .message
                .contains("clone the job's stderr for diagnostics"),
            "the failed syscall operation must be named: {error:?}"
        );
        assert!(
            error.message.contains(&clone_error.to_string()),
            "EMFILE must remain the syscall cause: {error:?}"
        );
        assert!(error.hint.contains("descriptor limit"), "{error:?}");
        assert!(matches!(
            *control
                .target
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
            Target::Finished
        ));
        assert!(observed.recv().await.is_none(), "no process was started");

        let mut diagnostics = String::new();
        std::fs::File::from(stderr_reader)
            .read_to_string(&mut diagnostics)
            .expect("stderr closes after reporting the clone failure");
        assert_eq!(diagnostics, format!("cowshed: {}\n", error.message));
        let mut stdout_bytes = Vec::new();
        std::fs::File::from(stdout_reader)
            .read_to_end(&mut stdout_bytes)
            .expect("stdout closes without a launch");
        assert!(stdout_bytes.is_empty());
        #[cfg(target_os = "linux")]
        cgroup.finish().expect("remove the refused empty admission");
    });
    println!("COWSHED_DIAGNOSTICS_VERIFIED");
}
