use std::io::Read as _;
use std::process::Command;

use super::*;
use crate::error::ErrorCode;
use crate::fork_lock::Run as _;

const CHILD: &str = "COWSHED_DIAGNOSTICS_NOFILE_CHILD";
const TEST: &str = "runtime::shell_job::diagnostics_tests::descriptor_exhaustion_reports_the_diagnostics_clone_failure";

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
    if std::env::var_os(CHILD).is_none() {
        let output = Command::new(std::env::current_exe().expect("test executable"))
            .args(["--exact", TEST, "--nocapture"])
            .env(CHILD, "1")
            .output_locked()
            .expect("run isolated descriptor-limit supervisor");
        assert!(
            output.status.success(),
            "descriptor-limit supervisor failed: {}\nstdout: {}\nstderr: {}",
            output.status,
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
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
                profile: String::new(),
                environment: BTreeMap::new(),
                envrc_directory: None,
            }),
            ShellPoolConfig {
                prewarm: false,
                ..ShellPoolConfig::default()
            },
        );
        let (stdin, stdin_writer) = pipe().expect("job stdin");
        let (stdout_reader, stdout) = pipe().expect("job stdout");
        let (stderr_reader, stderr) = pipe().expect("job stderr");
        let control = Arc::new(JobControl::default());
        let (_release, released) = oneshot::channel();
        let (events, mut observed) = mpsc::channel(4);
        let job_id = JobId::new(17).expect("job id");

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
    });
}
