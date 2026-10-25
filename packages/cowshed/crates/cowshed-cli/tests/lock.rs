//! `cowshed lock` and `cowshed lock-holders`, run as the binary a caller runs (02_workspaces.md
//! "Held locks"). The readiness proofs of the held lock:
//!
//! - killing the `lock` process alone never releases the lock while its command runs: the command
//!   inherited the descriptor, and only its end lets the next holder in;
//! - a holder record naming a pid that is not the proven holder proves no owner, and a waiter's
//!   captured authority never reaches it;
//! - a holder outside cowshed that takes the same `flock(2)` with no record (lockf(1), flock(1))
//!   still serializes with `lock`, is named with the custody the kernel shows of it, and is never
//!   signalled;
//! - a waiter signals only the proven owner whose life it captured at its start, through that
//!   captured identity, and only of the inode it is queued on; a captured life that has ended or a
//!   file that replaced its lock at the path is never signalled, and the owner forwards the signal
//!   to its command, so the lock goes only with the command;
//! - a waiter that signalled refuses its step: it never says ACQUIRED nor runs its command, while
//!   a waiter that did not signal keeps its place in the queue.
#![cfg(unix)]

use std::fs::{self, File, OpenOptions, TryLockError};
use std::io::{self, BufRead as _, BufReader, Read as _, Write as _};
use std::os::fd::AsRawFd as _;
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::{MetadataExt as _, OpenOptionsExt as _};
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStderr, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};
use std::thread;
use std::time::Duration;

use cowshed_core::api::dto::ExitStatus;
use cowshed_core::api::held_lock::LockWaiterEvent;
use cowshed_core::runtime::job_groups::{await_exit_unreaped, signal_unreaped_group};
use cowshed_core::storage::apfs::{ApfsExecutionHost as _, ApfsSubstrateConfig};
use cowshed_core::storage::directory::DirectoryExecutionHost;
use cowshed_os::namespace::{HostNamespace, TestNamespace};
use serde_json::Value;

const POLL: Duration = Duration::from_millis(20);

/// The `cowshed` binary under test, read when the test runs (see `disk_command.rs`).
fn cowshed() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set CARGO_BIN_EXE_cowshed for an integration test"),
    )
}

/// A directory of its own for one test, removed when the test ends.
struct Dir {
    path: PathBuf,
}

impl Dir {
    fn new(name: &str) -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("cowshed-lock-{name}-{}-{n}", std::process::id()));
        fs::create_dir_all(&path).expect("create the test directory");
        let path = fs::canonicalize(&path).expect("canonicalize the test directory");
        Self { path }
    }

    fn lock(&self) -> PathBuf {
        self.path.join("exact.lock")
    }

    /// A named pipe a command blocks reading until the test releases it.
    fn fifo(&self, name: &str) -> PathBuf {
        let fifo = self.path.join(name);
        let c_path = std::ffi::CString::new(fifo.as_os_str().as_encoded_bytes())
            .expect("a fifo path has no NUL");
        // SAFETY: mkfifo(3) on a path inside this test's own directory.
        let rc = unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) };
        assert_eq!(rc, 0, "mkfifo: {}", io::Error::last_os_error());
        fifo
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        let removed = fs::remove_dir_all(&self.path);
        if !thread::panicking() {
            removed.expect("remove the test directory");
        }
    }
}

/// Writes one line to a fifo a command reads, once the command has opened it.
fn release(fifo: &Path) {
    loop {
        match OpenOptions::new()
            .write(true)
            .custom_flags(libc::O_NONBLOCK)
            .open(fifo)
        {
            Ok(mut writer) => {
                writer.write_all(b"\n").expect("write the release line");
                return;
            }
            Err(error) if error.raw_os_error() == Some(libc::ENXIO) => thread::sleep(POLL),
            Err(error) => panic!("open {}: {error}", fifo.display()),
        }
    }
}

fn send(pid: u32, signal: i32) {
    let pid = i32::try_from(pid).expect("a pid fits in pid_t");
    // SAFETY: kill(2) takes no pointers; `pid` is a process this test spawned.
    let rc = unsafe { libc::kill(pid, signal) };
    assert_eq!(
        rc,
        0,
        "kill({pid}, {signal}): {}",
        io::Error::last_os_error()
    );
}

/// `cowshed lock <lock> -- sh -c <script> sh <args…>`, leading its own process group.
fn lock_command(lock: &Path, script: &str, args: &[&Path]) -> TestProcess {
    TestProcess::spawn(
        Command::new(cowshed())
            .arg("lock")
            .arg(lock)
            .args(["--", "sh", "-c", script, "sh"])
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped()),
    )
}

/// The next stdout line of a command, which must have one.
fn line(stdout: &mut BufReader<ChildStdout>) -> String {
    let mut line = String::new();
    let read = stdout.read_line(&mut line).expect("read a line");
    assert!(read > 0, "the command closed its output before the line");
    line.trim_end().to_owned()
}

/// `cowshed lock-holders <lock>`, parsed.
fn lock_holders(lock: &Path) -> Value {
    let output = Command::new(cowshed())
        .arg("lock-holders")
        .arg(lock)
        .output()
        .expect("run cowshed lock-holders");
    assert!(
        output.status.success(),
        "lock-holders failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    serde_json::from_slice(&output.stdout).expect("lock-holders writes one JSON object")
}

/// A `cowshed lock` waiter whose caller owns `owned`, with a real command-start marker.
struct OwnedWaiter {
    child: TestProcess,
    state: BufReader<File>,
    ran: PathBuf,
}

impl OwnedWaiter {
    fn start(dir: &Dir, name: &str, lock: &Path, owned: &[u32]) -> Self {
        let fifo = dir.fifo(name);
        let ran = dir.path.join(format!("{name}.ran"));
        let mut script = String::from(r#"exec "$0" lock "$1" --state-fd 3"#);
        for pid in owned {
            script.push_str(&format!(" --owned-pid {pid}"));
        }
        script.push_str(r#" -- sh -c ': > "$1"' step "$3" 3>"$2""#);
        let child = TestProcess::spawn(
            Command::new("sh")
                .args(["-c", &script])
                .arg(cowshed())
                .arg(lock)
                .arg(&fifo)
                .arg(&ran)
                .stdin(Stdio::null()),
        );
        let state = BufReader::new(File::open(&fifo).expect("open the state fifo"));
        let mut waiter = Self { child, state, ran };
        assert!(
            matches!(waiter.event(), LockWaiterEvent::Queued { .. }),
            "the owned waiter did not queue"
        );
        waiter
    }

    /// The next event on its state channel, which must be one whole JSON event on one line.
    fn event(&mut self) -> LockWaiterEvent {
        let mut line = String::new();
        assert!(
            self.state.read_line(&mut line).expect("read a state line") > 0,
            "state ended"
        );
        serde_json::from_str(line.trim_end_matches('\n'))
            .unwrap_or_else(|error| panic!("state line {line:?} is no waiter event: {error}"))
    }

    /// Ask the waiter to signal the lock's owner through the lives it captured; its answer.
    fn request(&mut self) -> LockWaiterEvent {
        send(self.child.id(), libc::SIGUSR1);
        self.event()
    }

    /// Ask, and require that it signalled nothing and kept its place; the reason it gave.
    fn refused_request(&mut self) -> String {
        match self.request() {
            LockWaiterEvent::NotSignalled { reason } => reason,
            other => panic!("the waiter answered {other:?}, not notSignalled"),
        }
    }

    /// Wait for it to be granted and let go.
    fn finish(mut self) {
        assert!(matches!(self.event(), LockWaiterEvent::Acquired { .. }));
        assert_eq!(
            self.child.wait().expect("wait for the waiter"),
            ExitStatus::Exited { code: 0 }
        );
        assert!(
            self.ran.exists(),
            "the acquired waiter never ran its command"
        );
    }

    /// Wait for it to refuse its step once it signalled: its state channel ends with no ACQUIRED,
    /// and it exits with cowshed's conflict code, so its command never ran.
    fn refused(mut self) {
        let mut rest = String::new();
        self.state
            .read_to_string(&mut rest)
            .expect("read the rest of the state");
        assert_eq!(rest, "", "the waiter that signalled said more");
        let status = self.child.wait().expect("wait for the waiter");
        assert_eq!(status, ExitStatus::Exited { code: 103 });
        assert!(!self.ran.exists(), "the cancelled waiter ran its command");
    }
}

/// The custody of `pid`'s descriptor in an observation, which must name it.
fn custody_of(observation: &Value, pid: u32) -> String {
    observation["descriptors"]
        .as_array()
        .expect("descriptors")
        .iter()
        .find(|descriptor| descriptor["pid"] == pid)
        .unwrap_or_else(|| panic!("no descriptor of pid {pid}: {observation}"))["custody"]
        .as_str()
        .expect("a custody")
        .to_owned()
}

/// What the kernel shows of a descriptor that holds the lock without having recorded it: Linux
/// shows its custody, Darwin exposes none.
fn unrecorded_holder() -> &'static str {
    if cfg!(target_os = "linux") {
        "holds"
    } else {
        "unexposed"
    }
}

/// Whether the lock at `path` is held by anyone, asked without waiting.
fn held(path: &Path) -> bool {
    match File::open(path).expect("open the lock").try_lock() {
        Ok(()) => false,
        Err(TryLockError::WouldBlock) => true,
        Err(TryLockError::Error(error)) => panic!("try the lock: {error}"),
    }
}

/// A process that holds `lock` with no holder record, as lockf(1) or flock(1) holds it: this test
/// takes the `flock(2)`, hands the descriptor to a `sleep` and lets go of its own copy, so the
/// sleep alone holds it.
fn unrecorded(lock: &Path) -> TestProcess {
    let file = File::create(lock).expect("create the lock");
    file.lock().expect("take the lock");
    let descriptor = file.as_raw_fd();
    let mut command = Command::new("sleep");
    command.arg("600");
    // SAFETY: the closure runs between fork and exec and calls only fcntl.
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(descriptor, libc::F_SETFD, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let child = TestProcess::spawn(&mut command);
    drop(file);
    child
}

/// A `sleep` with no descriptor on any lock.
fn bystander() -> TestProcess {
    TestProcess::spawn(Command::new("sleep").arg("600"))
}

/// The leader stays unreaped until cleanup, so its process-group id cannot be reused.
struct TestProcess {
    child: Child,
    stdout: Option<ChildStdout>,
    stderr: Option<ChildStderr>,
}

impl TestProcess {
    fn spawn(command: &mut Command) -> Self {
        let mut child = command
            .process_group(0)
            .spawn()
            .expect("spawn fixture process");
        Self {
            stdout: child.stdout.take(),
            stderr: child.stderr.take(),
            child,
        }
    }

    fn id(&self) -> u32 {
        self.child.id()
    }

    fn is_running(&self) -> bool {
        cowshed_core::runtime::job_groups::exit_unreaped(
            i32::try_from(self.id()).expect("a pid fits in pid_t"),
        )
        .expect("inspect fixture child without reaping")
        .is_none()
    }

    fn wait(&self) -> io::Result<ExitStatus> {
        await_exit_unreaped(i32::try_from(self.id()).expect("a pid fits in pid_t"))
    }

    fn stop(&self) -> io::Result<ExitStatus> {
        signal_unreaped_group(
            i32::try_from(self.id()).expect("a pid fits in pid_t"),
            libc::SIGKILL,
        )?;
        self.wait()
    }
}

impl Drop for TestProcess {
    fn drop(&mut self) {
        let pid = i32::try_from(self.id()).expect("a pid fits in pid_t");
        let result =
            signal_unreaped_group(pid, libc::SIGKILL).and_then(|()| self.child.wait().map(|_| ()));
        if let Err(error) = result {
            if thread::panicking() {
                cowshed_os::eprintln!("fixture process group {pid} cleanup failed: {error}");
            } else {
                panic!("fixture process group {pid} cleanup failed: {error}");
            }
        }
    }
}

#[test]
fn a_zombie_holder_is_not_reported_as_running() {
    let child = TestProcess::spawn(&mut Command::new("true"));
    assert_eq!(
        child.wait().expect("observe fixture exit without reaping"),
        ExitStatus::Exited { code: 0 }
    );
    assert!(
        !child.is_running(),
        "an unreaped zombie is not a live holder"
    );
}

/// A caller-opened state descriptor, separate from stdout and stderr, handed through exec.
fn state_command(dir: &Dir) -> Command {
    state_command_in(dir, Command::new("sh"))
}

/// [`state_command`] run by `shell`, an `sh` that may sit behind a wrapper.
fn state_command_in(dir: &Dir, mut shell: Command) -> Command {
    shell
        .args(["-c", r#"state=$1; shift; exec "$0" "$@" 3>"$state""#])
        .arg(cowshed())
        .arg(dir.path.join("state"))
        .arg("lock")
        .arg(dir.lock())
        .args(["--state-fd", "3"])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    shell
}

fn state_events(dir: &Dir) -> Vec<LockWaiterEvent> {
    fs::read_to_string(dir.path.join("state"))
        .expect("read the owned state channel")
        .lines()
        .map(|line| serde_json::from_str(line).expect("one complete state event per line"))
        .collect()
}

#[test]
fn a_missing_executable_reports_failed_without_acquired() {
    let dir = Dir::new("missing-command");
    let missing = dir.path.join("no-such-executable");
    let child = TestProcess::spawn(state_command(&dir).arg("--").arg(&missing));
    assert_eq!(
        child.wait().expect("observe failed spawn"),
        ExitStatus::Exited { code: 104 }
    );
    let events = state_events(&dir);
    let [LockWaiterEvent::Failed { reason }] = events.as_slice() else {
        panic!("a failed spawn must report only failed: {events:?}");
    };
    assert!(reason.contains("cannot run"), "{reason}");
    assert!(reason.contains("no-such-executable"), "{reason}");
    assert!(!held(&dir.lock()), "failed spawn retained the lock");
}

#[test]
fn an_invalid_git_config_count_is_refused_before_queued_without_starting_the_command() {
    let dir = Dir::new("invalid-count");
    let holder = unrecorded(&dir.lock());
    let ran = dir.path.join("ran");
    let child = TestProcess::spawn(
        state_command(&dir)
            .env("GIT_CONFIG_COUNT", "not-a-count")
            .args(["--", "sh", "-c", r#": > "$1""#, "step"])
            .arg(&ran),
    );
    assert_eq!(
        child.wait().expect("observe invalid environment refusal"),
        ExitStatus::Exited { code: 104 }
    );
    let events = state_events(&dir);
    let [LockWaiterEvent::Failed { reason }] = events.as_slice() else {
        panic!("invalid configuration must fail before queued: {events:?}");
    };
    assert!(reason.contains("GIT_CONFIG_COUNT"), "{reason}");
    assert!(!ran.exists(), "the refused command started");
    assert!(holder.is_running(), "validation signalled the holder");
}

#[test]
fn an_empty_git_config_count_is_zero_and_preserves_the_callers_configuration() {
    let dir = Dir::new("empty-count");
    let config = dir.path.join("gitconfig");
    fs::write(&config, "[user]\n\tlockfixture = kept\n").expect("write caller config");
    let mut child = TestProcess::spawn(
        state_command(&dir)
            .env("GIT_CONFIG_COUNT", "")
            .env("GIT_CONFIG_GLOBAL", &config)
            .env("GIT_CONFIG_NOSYSTEM", "1")
            .args([
                "--",
                "sh",
                "-c",
                "printf '%s\\n' \"$GIT_CONFIG_COUNT\"; git config --get user.lockfixture; \
                 git config --get gc.auto; git config --get maintenance.auto; \
                 git config --get core.fsmonitor",
            ]),
    );
    let mut stdout = Vec::new();
    child
        .stdout
        .take()
        .expect("stdout")
        .read_to_end(&mut stdout)
        .expect("read child output");
    assert_eq!(
        child.wait().expect("observe command exit"),
        ExitStatus::Exited { code: 0 }
    );
    assert_eq!(stdout, b"3\nkept\n0\nfalse\nfalse\n");
    assert!(matches!(
        state_events(&dir).as_slice(),
        [LockWaiterEvent::Acquired { .. }]
    ));
}

#[test]
fn an_unopened_state_descriptor_cannot_adopt_a_runtime_descriptor() {
    let dir = Dir::new("unopened-state");
    let ran = dir.path.join("ran");
    let mut child = TestProcess::spawn(
        Command::new("sh")
            .args([
                "-c",
                r#"exec 3>&-; exec "$0" lock "$1" --state-fd 3 -- sh -c ': > "$1"' step "$2""#,
            ])
            .arg(cowshed())
            .arg(dir.lock())
            .arg(&ran)
            .stdout(Stdio::piped())
            .stderr(Stdio::piped()),
    );
    assert_eq!(
        child.wait().expect("observe unopened descriptor refusal"),
        ExitStatus::Exited { code: 104 }
    );
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("stderr")
        .read_to_string(&mut stderr)
        .expect("read diagnostic");
    assert!(
        stderr.contains("--state-fd 3 is not an open descriptor"),
        "{stderr}"
    );
    assert!(
        !ran.exists(),
        "an invalid state descriptor let the command run"
    );
    assert!(
        !dir.lock().exists(),
        "an invalid state descriptor opened the lock"
    );
}

#[test]
fn a_capture_failure_reports_failed_on_the_owned_state_channel() {
    let dir = Dir::new("capture-failure");
    let ran = dir.path.join("ran");
    // Darwin answers launchd's identity to every uid, so the waiter runs under a Seatbelt profile
    // that denies it every read of another process's info: capturing pid 1 fails with EPERM, and
    // no mocked syscall is involved.
    #[cfg(target_os = "macos")]
    let mut command = {
        let mut sandboxed = Command::new("/usr/bin/sandbox-exec");
        sandboxed.args([
            "-p",
            "(version 1)(allow default)(deny process-info-pidinfo (target others))",
            "--",
            "sh",
        ]);
        let mut command = state_command_in(&dir, sandboxed);
        command.args(["--owned-pid", "1"]);
        command
    };
    #[cfg(target_os = "linux")]
    let mut command = state_command(&dir);
    #[cfg(target_os = "linux")]
    {
        let pid = std::process::id().to_string();
        for _ in 0..256 {
            command.args(["--owned-pid", &pid]);
        }
        // Capture retains one pidfd per entry. Leave room for the runtime and state channel,
        // but fewer descriptors than capture requires; no mocked syscall is involved.
        // SAFETY: pre_exec calls only setrlimit with a valid rlimit and reads errno on failure.
        unsafe {
            command.pre_exec(|| {
                let limit = libc::rlimit {
                    rlim_cur: 64,
                    rlim_max: 64,
                };
                if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) != 0 {
                    return Err(io::Error::last_os_error());
                }
                Ok(())
            });
        }
    }
    let child = TestProcess::spawn(
        command
            .args(["--", "sh", "-c", r#": > "$1""#, "step"])
            .arg(&ran),
    );
    assert_eq!(
        child.wait().expect("observe capture failure"),
        ExitStatus::Exited { code: 100 }
    );
    let events = state_events(&dir);
    let [LockWaiterEvent::Failed { reason }] = events.as_slice() else {
        panic!("capture must report failed once, then close its state: {events:?}");
    };
    assert!(
        reason.contains("cannot capture the owned processes"),
        "{reason}"
    );
    #[cfg(target_os = "linux")]
    assert!(reason.contains("Too many open files"), "{reason}");
    #[cfg(target_os = "macos")]
    assert!(reason.contains("Operation not permitted"), "{reason}");
    assert!(!ran.exists(), "failed capture started the command");
    assert!(!dir.lock().exists(), "failed capture opened the lock");
}

#[test]
fn lock_preserves_non_utf8_child_arguments() {
    let dir = Dir::new("raw-argv");
    let bytes = b"raw-\xff-\xfe";
    let mut child = TestProcess::spawn(
        state_command(&dir)
            .args(["--", "sh", "-c", r#"printf '%s' "$1""#, "step"])
            .arg(std::ffi::OsStr::from_bytes(bytes)),
    );
    let mut stdout = Vec::new();
    child
        .stdout
        .take()
        .expect("stdout")
        .read_to_end(&mut stdout)
        .expect("read raw child argument");
    assert_eq!(
        child.wait().expect("observe raw-argv command"),
        ExitStatus::Exited { code: 0 }
    );
    assert_eq!(stdout, bytes, "the executed argument was lossily converted");
    assert!(matches!(
        state_events(&dir).as_slice(),
        [LockWaiterEvent::Acquired { .. }]
    ));
}

#[test]
fn acquired_keeps_the_state_channel_until_the_command_ends() {
    let dir = Dir::new("state-cloexec");
    let state = dir.fifo("state");
    let release_fifo = dir.fifo("release");
    let child = TestProcess::spawn(
        state_command(&dir)
            .args(["--", "sh", "-c", r#"read x < "$1"; exit 7"#, "step"])
            .arg(&release_fifo),
    );
    let mut state = BufReader::new(File::open(&state).expect("open state reader"));
    let mut events = String::new();
    state.read_line(&mut events).expect("read acquisition");
    let event: LockWaiterEvent = serde_json::from_str(events.trim_end()).expect("acquired event");
    assert!(
        matches!(event, LockWaiterEvent::Acquired { .. }),
        "{events}"
    );
    assert!(held(&dir.lock()), "the child is still holding the lock");
    assert!(
        !at_end(state.get_ref()),
        "post-spawn diagnostics remain owned"
    );
    release(&release_fifo);
    assert_eq!(
        child.wait().expect("observe nonzero child exit"),
        ExitStatus::Exited { code: 7 }
    );
    let mut rest = String::new();
    state
        .read_to_string(&mut rest)
        .expect("state ends with the controller");
    assert_eq!(
        rest, "",
        "the child's nonzero exit is not a wrapper failure"
    );
}

/// An EOF oracle that never waits for a still-open writer and restores its reader's flags.
fn at_end(reader: &impl std::os::fd::AsRawFd) -> bool {
    let descriptor = reader.as_raw_fd();
    // SAFETY: fcntl operates on the reader this fixture owns.
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    assert_ne!(
        flags,
        -1,
        "read descriptor flags: {}",
        io::Error::last_os_error()
    );
    // SAFETY: preserve the flags and add nonblocking mode to this owned reader.
    assert_eq!(
        unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut byte = 0_u8;
    // SAFETY: byte is writable for the one byte requested.
    let read = unsafe { libc::read(descriptor, (&raw mut byte).cast(), 1) };
    let error = io::Error::last_os_error();
    // SAFETY: restore this reader's original descriptor flags.
    assert_eq!(unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags) }, 0);
    match read {
        0 => true,
        -1 => {
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock, "{error}");
            false
        }
        _ => panic!("unexpected output before EOF"),
    }
}

#[test]
fn a_stateful_lock_relinquishes_all_command_streams_before_its_command_ends() {
    let dir = Dir::new("stdio-handoff");
    let state_path = dir.fifo("state");
    let closed = dir.fifo("closed");
    let release_fifo = dir.fifo("release");
    let mut child = TestProcess::spawn(
        state_command(&dir)
            .args([
                "--",
                "sh",
                "-c",
                r#"exec 0<&- 1>&- 2>&-; printf closed > "$1"; read -r line < "$2"; exit 7"#,
                "step",
            ])
            .arg(&closed)
            .arg(&release_fifo)
            .stdin(Stdio::piped()),
    );
    let mut state = BufReader::new(File::open(state_path).expect("open state"));
    let mut event = String::new();
    state.read_line(&mut event).expect("read acquisition");
    assert!(matches!(
        serde_json::from_str::<LockWaiterEvent>(&event).expect("event"),
        LockWaiterEvent::Acquired { .. }
    ));
    let mut ready = String::new();
    File::open(closed)
        .expect("open close witness")
        .read_to_string(&mut ready)
        .expect("read close witness");
    assert_eq!(ready, "closed");
    assert!(
        at_end(child.stdout.as_ref().expect("stdout")),
        "only the command owned stdout"
    );
    assert!(
        at_end(child.stderr.as_ref().expect("stderr")),
        "only the command owned stderr"
    );
    let error = child
        .child
        .stdin
        .take()
        .expect("stdin")
        .write_all(b"unread")
        .expect_err("no process retains command stdin");
    assert_eq!(error.kind(), io::ErrorKind::BrokenPipe);
    assert!(held(&dir.lock()), "EOF must not release lock custody");
    assert!(
        child.is_running(),
        "the controller still waits for the live command"
    );
    release(&release_fifo);
    assert_eq!(
        child.wait().expect("command status"),
        ExitStatus::Exited { code: 7 }
    );
    let mut rest = String::new();
    state
        .read_to_string(&mut rest)
        .expect("state closes after command exit");
    assert_eq!(rest, "");
    assert!(!held(&dir.lock()));
}

#[test]
fn the_running_command_does_not_inherit_the_state_channel() {
    let dir = Dir::new("state-custody");
    let state_path = dir.fifo("state");
    let release_fifo = dir.fifo("release");
    let child = TestProcess::spawn(
        state_command(&dir)
            .args(["--", "sh", "-c", r#"read -r line < "$1""#, "step"])
            .arg(&release_fifo),
    );
    let mut state = BufReader::new(File::open(state_path).expect("open state"));
    let mut event = String::new();
    state.read_line(&mut event).expect("read acquisition");
    assert!(matches!(
        serde_json::from_str::<LockWaiterEvent>(&event).expect("event"),
        LockWaiterEvent::Acquired { .. }
    ));
    send(child.id(), libc::SIGKILL);
    assert_eq!(
        child.wait().expect("controller exit"),
        ExitStatus::Signaled {
            signal: libc::SIGKILL,
            core_dumped: false
        }
    );
    let mut rest = String::new();
    state
        .read_to_string(&mut rest)
        .expect("the live command owns no state writer");
    assert_eq!(rest, "");
    assert!(held(&dir.lock()), "the live command still holds the lock");
    release(&release_fifo);
}

#[test]
fn a_closed_state_reader_does_not_abandon_the_started_command() {
    let dir = Dir::new("closed-state-reader");
    let fifo = dir.fifo("release");
    let (state, reader) = std::os::unix::net::UnixStream::pair().expect("create state channel");
    drop(reader);
    let descriptor = state.as_raw_fd();
    let mut command = Command::new(cowshed());
    command
        .arg("lock")
        .arg(dir.lock())
        .args(["--state-fd", &descriptor.to_string()])
        .args([
            "--",
            "sh",
            "-c",
            r#"echo started; read x < "$1"; echo ended"#,
            "step",
        ])
        .arg(&fifo)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    // SAFETY: the child inherits this open socket; only the async-signal-safe fcntl runs here.
    unsafe {
        command.pre_exec(move || {
            if libc::fcntl(descriptor, libc::F_SETFD, 0) == -1 {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    let mut child = TestProcess::spawn(&mut command);
    drop(state);
    let mut stdout = BufReader::new(child.stdout.take().expect("stdout"));
    assert_eq!(line(&mut stdout), "started");
    assert!(
        cowshed_core::runtime::job_groups::exit_unreaped(
            i32::try_from(child.id()).expect("a pid fits")
        )
        .expect("inspect wrapper without reaping")
        .is_none(),
        "the wrapper exited instead of waiting for its started command"
    );
    assert!(held(&dir.lock()), "state reporting lost lock custody");
    release(&fifo);
    assert_eq!(line(&mut stdout), "ended");
    assert_eq!(
        child
            .wait()
            .expect("observe state-write failure after command exit"),
        ExitStatus::Exited { code: 100 }
    );
    let mut stderr = String::new();
    child
        .stderr
        .take()
        .expect("stderr")
        .read_to_string(&mut stderr)
        .expect("read state failure");
    assert_eq!(stderr, "", "the controller no longer owns command stderr");
    assert!(!held(&dir.lock()), "the ended command retained the lock");
}

#[test]
fn killing_the_lock_process_alone_leaves_the_lock_with_its_running_command() {
    let dir = Dir::new("helper-death");
    let lock = dir.lock();
    let fifo = dir.fifo("release");
    let mut helper = lock_command(
        &lock,
        r#"echo started; read x < "$1"; echo ended"#,
        &[&fifo],
    );
    let mut stdout = BufReader::new(helper.stdout.take().expect("stdout"));
    assert_eq!(line(&mut stdout), "started");
    let record: Value =
        serde_json::from_slice(&fs::read(&lock).expect("read the record")).expect("a record");
    assert_eq!(
        record["pid"],
        helper.id(),
        "the lock process recorded itself"
    );

    send(helper.id(), libc::SIGKILL);
    helper.wait().expect("observe the killed lock process exit");
    assert!(
        held(&lock),
        "the lock went with its taker while its command ran"
    );
    let observation = lock_holders(&lock);
    assert_eq!(observation["locked"], true, "{observation}");
    assert!(
        observation["owner"].is_null(),
        "a killed taker's record proved an owner: {observation}"
    );
    let command = observation["descriptors"]
        .as_array()
        .expect("descriptors")
        .iter()
        .find(|descriptor| {
            descriptor["command"]
                .as_str()
                .is_some_and(|command| command.contains("read x"))
        })
        .unwrap_or_else(|| panic!("the running command is not named: {observation}"));
    assert_eq!(command["custody"], unrecorded_holder(), "{observation}");

    release(&fifo);
    assert_eq!(line(&mut stdout), "ended");
    while held(&lock) {
        thread::sleep(POLL);
    }
}

#[test]
fn a_record_naming_a_pid_that_does_not_hold_the_lock_proves_no_owner_and_is_never_signalled() {
    let dir = Dir::new("foreign-record");
    let lock = dir.lock();
    let holder = unrecorded(&lock);
    let named = bystander();
    // A record naming a live process that holds no descriptor on the inode, with a unique id that
    // is not its life's. The waiter's caller even owns that process: nothing proves it the owner.
    let forged = serde_json::json!({
        "pid": named.id(),
        "uniqueId": "1",
        "since": "2026-10-21T00:00:00Z",
        "command": "a holder that has exited",
    });
    fs::write(&lock, forged.to_string()).expect("write the record");
    let observation = lock_holders(&lock);
    assert!(observation["owner"].is_null(), "{observation}");
    assert_eq!(observation["locked"], true, "{observation}");
    let mut waiter = OwnedWaiter::start(&dir, "state", &lock, &[named.id(), holder.id()]);
    waiter.refused_request();
    assert!(named.is_running(), "the named process was signalled");
    assert!(holder.is_running(), "the unrecorded holder was signalled");
    drop(holder);
    waiter.finish();
    drop(named);
}

/// The owner whose life the waiter captured has ended while its command still holds the lock
/// (the command inherited it): the record names a life that is gone, the kernel proves no owner,
/// and the request reaches nothing; the command, still holding the lock, is untouched.
#[test]
fn a_waiter_never_signals_once_the_life_it_captured_has_ended() {
    let dir = Dir::new("captured-ended");
    let lock = dir.lock();
    let fifo = dir.fifo("release");
    let mut owner = lock_command(
        &lock,
        r#"echo started; read x < "$1"; echo ended"#,
        &[&fifo],
    );
    let mut stdout = BufReader::new(owner.stdout.take().expect("stdout"));
    assert_eq!(line(&mut stdout), "started");
    let mut waiter = OwnedWaiter::start(&dir, "state", &lock, &[owner.id()]);
    send(owner.id(), libc::SIGKILL);
    owner.wait().expect("observe the owner exit");
    assert!(held(&lock), "the command holds the lock its owner left");
    waiter.refused_request();
    release(&fifo);
    assert_eq!(line(&mut stdout), "ended", "the command was signalled");
    waiter.finish();
}

#[test]
fn a_holder_outside_cowshed_serializes_with_lock_is_named_and_is_never_signalled() {
    let dir = Dir::new("outside-holder");
    let lock = dir.lock();
    let holder = unrecorded(&lock);
    let mut waiter = lock_command(&lock, "echo ran", &[]);
    let mut stderr = BufReader::new(waiter.stderr.take().expect("stderr"));
    let mut said = String::new();
    stderr.read_line(&mut said).expect("read the wait line");
    assert!(said.contains("held by another holder; waiting"), "{said}");

    let observation = &lock_holders(&lock);
    assert_eq!(observation["locked"], true, "{observation}");
    assert!(observation["owner"].is_null(), "{observation}");
    assert_eq!(
        observation["record"],
        serde_json::json!({"encoding": "utf8", "data": ""}),
        "{observation}"
    );
    assert_eq!(custody_of(observation, holder.id()), unrecorded_holder());
    let waiting = if cfg!(target_os = "linux") {
        "open"
    } else {
        "unexposed"
    };
    assert_eq!(custody_of(observation, waiter.id()), waiting);
    assert!(holder.is_running(), "an unrecorded holder was signalled");
    let mut owned = OwnedWaiter::start(&dir, "state", &lock, &[holder.id()]);
    owned.refused_request();
    assert!(holder.is_running(), "an unrecorded holder was signalled");

    let mut stdout = BufReader::new(waiter.stdout.take().expect("stdout"));
    drop(holder);
    assert_eq!(line(&mut stdout), "ran");
    assert_eq!(
        waiter.wait().expect("observe lock exit"),
        ExitStatus::Exited { code: 0 }
    );
    owned.finish();
}

#[test]
fn the_proven_owner_is_signalled_through_its_identity_and_its_command_ends_with_it() {
    let dir = Dir::new("owner-signal");
    let lock = dir.lock();
    let mut helper = lock_command(&lock, "echo started; exec sleep 600", &[]);
    let mut stdout = BufReader::new(helper.stdout.take().expect("stdout"));
    assert_eq!(line(&mut stdout), "started");
    let observation = lock_holders(&lock);
    assert_eq!(observation["owner"]["pid"], helper.id(), "{observation}");
    let recorded = if cfg!(target_os = "linux") {
        "holds"
    } else {
        "recorded"
    };
    assert_eq!(custody_of(&observation, helper.id()), recorded);

    let mut waiter = OwnedWaiter::start(&dir, "state", &lock, &[helper.id()]);
    let pid = i32::try_from(helper.id()).expect("a pid fits in pid_t");
    assert_eq!(waiter.request(), LockWaiterEvent::Signalled { pid });
    let status = helper.wait().expect("wait for lock");
    assert_eq!(
        status,
        ExitStatus::Exited {
            code: 128 + libc::SIGTERM
        }
    );
    // The lock its owner gave up is not spent on the step the caller is cancelling.
    waiter.refused();
}

/// The waiter queued on the inode it opened; a file renamed over the lock path since is another
/// lock. Its holder, whose life the caller even owns, proves nothing about the waiter's lock and
/// is never signalled. The waiter refuses to run its command against the replaced inode.
#[test]
fn a_file_that_replaced_the_lock_at_its_path_is_never_the_waiters_owner() {
    let dir = Dir::new("replaced");
    let lock = dir.lock();
    let mut helper = lock_command(&lock, "echo started; exec sleep 600", &[]);
    let mut stdout = BufReader::new(helper.stdout.take().expect("stdout"));
    assert_eq!(line(&mut stdout), "started");
    let mut waiter = OwnedWaiter::start(&dir, "state", &lock, &[helper.id()]);
    fs::rename(&lock, dir.path.join("aside")).expect("move the queued lock aside");
    let mut replacing = lock_command(&lock, "echo started; exec sleep 600", &[]);
    let mut replaced = BufReader::new(replacing.stdout.take().expect("stdout"));
    assert_eq!(line(&mut replaced), "started");

    let reason = waiter.refused_request();
    assert!(
        reason.starts_with("the lock path now names inode "),
        "{reason}"
    );
    assert!(
        helper.is_running(),
        "the owner of the waiter's own inode was signalled"
    );
    assert!(
        replacing.is_running(),
        "the holder of the replacing file was signalled"
    );
    helper.stop().expect("stop the original holder");
    let event = waiter.event();
    match event {
        LockWaiterEvent::Failed { reason } => {
            assert!(reason.contains("names inode"), "{reason}");
            assert!(reason.contains("not the inode"), "{reason}");
        }
        other => panic!("the replaced lock reported {other:?}, not a refusal"),
    }
    assert_eq!(
        waiter.child.wait().expect("wait for the refused waiter"),
        ExitStatus::Exited { code: 104 }
    );
    assert!(
        !waiter.ran.exists(),
        "the replaced-inode waiter ran its command"
    );
    replacing.stop().expect("stop the replacing holder");
}

/// Darwin exposes no lock state per descriptor. The command that inherited the lock holds it, but
/// the kernel cannot say so: it is named `unexposed`, never counted the owner, and a signal asked
/// of it is never sent, while the owner the record names is proven and signalled.
#[cfg(target_os = "macos")]
#[test]
fn darwin_names_an_inherited_descriptor_unexposed_and_never_signals_it() {
    let dir = Dir::new("unexposed");
    let lock = dir.lock();
    let mut helper = lock_command(&lock, "echo started; exec sleep 600", &[]);
    let mut stdout = BufReader::new(helper.stdout.take().expect("stdout"));
    assert_eq!(line(&mut stdout), "started");
    let observation = lock_holders(&lock);
    assert_eq!(observation["owner"]["pid"], helper.id(), "{observation}");
    let inherited = observation["descriptors"]
        .as_array()
        .expect("descriptors")
        .iter()
        .find(|descriptor| {
            descriptor["command"]
                .as_str()
                .is_some_and(|command| command.contains("sleep 600"))
        })
        .unwrap_or_else(|| panic!("the inherited command is not named: {observation}"));
    assert_eq!(inherited["custody"], "unexposed", "{observation}");
    let inherited = u32::try_from(inherited["pid"].as_u64().expect("a pid")).expect("a pid fits");

    let mut waiter = OwnedWaiter::start(&dir, "state", &lock, &[inherited]);
    waiter.refused_request();
    // This is the helper's child, not ours: the helper reaps it. The NotSignalled
    // event above proves refusal; signal 0 only checks that this inherited process still exists.
    let inherited_pid = i32::try_from(inherited).expect("a pid fits in pid_t");
    // SAFETY: signal 0 only checks existence and sends no signal.
    assert_eq!(
        unsafe { libc::kill(inherited_pid, 0) },
        0,
        "the inherited process disappeared: {}",
        io::Error::last_os_error()
    );
    helper.stop().expect("stop the holder");
    waiter.finish();
}

/// Git under `lock` resolves automatic gc, maintenance and fsmonitor off even when the repository
/// enables them. The caller's unrelated config survives, and the managed Nx no-start policy is
/// present. This finite command leaves no process holding the lock.
#[test]
fn a_command_holding_the_lock_receives_the_git_and_managed_nx_policies() {
    let dir = Dir::new("sealed-command");
    let lock = dir.lock();
    let repo = dir.path.join("repo");
    let git = |args: &[&str]| {
        let status = Command::new("git")
            .arg("-C")
            .arg(&repo)
            .args(args)
            .status()
            .expect("run git");
        assert!(status.success(), "git {args:?}");
    };
    fs::create_dir_all(&repo).expect("create the repository");
    git(&["init", "-q"]);
    for (key, value) in [
        ("gc.auto", "1"),
        ("maintenance.auto", "true"),
        ("core.fsmonitor", "true"),
    ] {
        git(&["config", key, value]);
    }
    let output = Command::new(cowshed())
        .arg("lock")
        .arg(&lock)
        .args(["--", "sh", "-c"])
        .arg(
            "git -C \"$1\" config --get gc.auto; git -C \"$1\" config --get maintenance.auto; \
             git -C \"$1\" config --get core.fsmonitor; git -C \"$1\" config --get user.lockfixture; \
             echo \"$NX_DAEMON_START\"",
        )
        .arg("sh")
        .arg(&repo)
        .env("GIT_CONFIG_COUNT", "1")
        .env("GIT_CONFIG_KEY_0", "user.lockfixture")
        .env("GIT_CONFIG_VALUE_0", "kept")
        .output()
        .expect("run cowshed lock");
    assert!(
        output.status.success(),
        "{}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        "0\nfalse\nfalse\nkept\nnever\n"
    );
    assert!(!held(&lock), "a process the command started kept the lock");
}

/// An operator wrapper can also take the ref lock with BSD `lockf -k`, which
/// holds the same `flock(2)` and writes no record. It and `cowshed lock` exclude each other in
/// both orders, the lockf holder is named with the custody the kernel shows of it and never
/// signalled, and the waiter runs once lockf lets go.
#[cfg(target_os = "macos")]
#[test]
fn bsd_lockf_and_cowshed_lock_exclude_each_other_on_the_same_file() {
    let dir = Dir::new("lockf");
    let lock = dir.lock();
    let fifo = dir.fifo("release");
    let mut lockf = TestProcess::spawn(
        Command::new("lockf")
            .arg("-k")
            .arg(&lock)
            .args(["sh", "-c", r#"echo held; read x < "$1""#, "sh"])
            .arg(&fifo)
            .stdout(Stdio::piped()),
    );
    let mut said = BufReader::new(lockf.stdout.take().expect("stdout"));
    assert_eq!(line(&mut said), "held");
    let mut waiter = lock_command(&lock, "echo ran", &[]);
    let mut wait_line = String::new();
    BufReader::new(waiter.stderr.take().expect("stderr"))
        .read_line(&mut wait_line)
        .expect("read the wait line");
    assert!(
        wait_line.contains("held by another holder; waiting"),
        "{wait_line}"
    );

    let observation = lock_holders(&lock);
    assert_eq!(observation["locked"], true, "{observation}");
    assert!(observation["owner"].is_null(), "{observation}");
    assert_eq!(custody_of(&observation, lockf.id()), "unexposed");
    let mut owned = OwnedWaiter::start(&dir, "state", &lock, &[lockf.id()]);
    owned.refused_request();
    assert!(lockf.is_running(), "lockf was signalled");

    release(&fifo);
    assert_eq!(
        lockf.wait().expect("observe lockf exit"),
        ExitStatus::Exited { code: 0 }
    );
    let mut stdout = BufReader::new(waiter.stdout.take().expect("stdout"));
    assert_eq!(line(&mut stdout), "ran");
    assert_eq!(
        waiter.wait().expect("observe lock exit"),
        ExitStatus::Exited { code: 0 }
    );
    owned.finish();

    // And the other order: lockf waits for a lock `cowshed lock` holds.
    let mut holder = lock_command(&lock, r#"echo started; read x < "$1""#, &[&fifo]);
    let mut started = BufReader::new(holder.stdout.take().expect("stdout"));
    assert_eq!(line(&mut started), "started");
    let refused = Command::new("lockf")
        .args(["-k", "-t", "0"])
        .arg(&lock)
        .arg("true")
        .status()
        .expect("run lockf");
    assert!(!refused.success(), "lockf took a lock cowshed lock holds");
    release(&fifo);
    assert_eq!(
        holder.wait().expect("observe lock exit"),
        ExitStatus::Exited { code: 0 }
    );
}

#[test]
fn lock_holders_preserves_non_utf8_record_bytes() {
    assert_raw_lock_observation(
        std::ffi::OsStr::new("held.lock"),
        serde_json::json!({"encoding": "utf8", "data": "held.lock"}),
    );
}

// Linux filesystems admit raw path bytes; APFS admits only valid UTF-8 names.
// Both platforms exercise arbitrary record bytes above.
#[cfg(target_os = "linux")]
#[test]
fn lock_holders_preserves_non_utf8_path_and_record_bytes() {
    assert_raw_lock_observation(
        std::ffi::OsStr::from_bytes(b"\xfe"),
        serde_json::json!({"encoding": "base64", "data": "/g=="}),
    );
}

fn assert_raw_lock_observation(name: &std::ffi::OsStr, path_wire: Value) {
    let dir = Dir::new("raw-observation");
    let observe = || {
        let output = Command::new(cowshed())
            .current_dir(&dir.path)
            .arg("lock-holders")
            .arg(name)
            .output()
            .expect("run lock-holders with a raw path");
        assert!(
            output.status.success(),
            "lock-holders {name:?} in {:?}: {}",
            dir.path,
            String::from_utf8_lossy(&output.stderr)
        );
        serde_json::from_slice::<Value>(&output.stdout).expect("one observation object")
    };
    assert_eq!(
        observe(),
        serde_json::json!({
            "path": path_wire,
            "inode": null,
            "locked": false,
            "record": {"encoding": "utf8", "data": ""},
            "owner": null,
            "holding": null,
            "descriptors": [],
        })
    );

    let path = dir.path.join(name);
    fs::write(&path, b"\xff")
        .unwrap_or_else(|error| panic!("write an arbitrary holder record at {path:?}: {error}"));
    let metadata = fs::metadata(&path).expect("read exact inode metadata");
    assert_eq!(
        observe(),
        serde_json::json!({
            "path": path_wire,
            "inode": {"dev": metadata.dev().to_string(), "ino": metadata.ino().to_string()},
            "locked": false,
            "record": {"encoding": "base64", "data": "/w=="},
            "owner": null,
            "holding": null,
            "descriptors": [],
        })
    );
}

/// Names the image lock, the directory store it is in, and the workspace whose fork it is taken
/// for, that this test binary takes when it runs as [`image_lock_taker`].
const IMAGE_LOCK_ENV: &str = "COWSHED_TEST_IMAGE_LOCK";
const IMAGE_STORE_ENV: &str = "COWSHED_TEST_IMAGE_STORE";
const IMAGE_WORKSPACE_ENV: &str = "COWSHED_TEST_IMAGE_WORKSPACE";

/// Not a test of its own: another process's lifecycle operation taking one image lock, as a fork
/// takes its source's (`main.dmg.lock`) while it clones it. Run as this binary with
/// [`IMAGE_LOCK_ENV`] set, it takes the lock through a directory store's execution host, the
/// path every lifecycle operation's image lock takes, for a fork of the workspace
/// [`IMAGE_WORKSPACE_ENV`] names, says `held` on stderr once it holds it, and holds it until its
/// stdin closes. A refused take panics, naming why on stderr. Run any other way it does nothing.
#[test]
fn image_lock_taker() {
    let Some(lock) = std::env::var_os(IMAGE_LOCK_ENV) else {
        return;
    };
    let store = PathBuf::from(std::env::var_os(IMAGE_STORE_ENV).expect("the image lock's store"));
    let workspace = cowshed_core::metadata::WorkspaceName::new(
        std::env::var(IMAGE_WORKSPACE_ENV).expect("the workspace the lock is taken for"),
    )
    .expect("a workspace name");
    // A directory store's image lock names no namespace; this process mints one for its config.
    let namespace = HostNamespace::Test(TestNamespace::mint().expect("a test namespace"));
    let host = DirectoryExecutionHost::new(ApfsSubstrateConfig::new(
        namespace,
        &store,
        store.join("checkout"),
    ))
    .expect("open the directory store");
    let _held = host
        .lock_images(
            &[PathBuf::from(lock)],
            cowshed_apfs::store_lock::LockMode::Wait,
            &cowshed_core::api::held_lock::LockHolding::of(
                cowshed_core::api::held_lock::LockOperation::Fork,
                &workspace,
            ),
        )
        .unwrap_or_else(|error| panic!("take the image lock: {error}"))
        .expect("a waited-for lock is taken");
    // On stderr, after every line the wait wrote there: libtest frames its own stdout.
    io::stderr()
        .write_all(b"held\n")
        .expect("say the lock is held");
    io::copy(&mut io::stdin(), &mut io::sink()).expect("wait for the release");
}

/// This test binary running as [`image_lock_taker`] of one lock, its stderr read line by line in
/// the order it wrote them.
struct ImageLockTaker {
    process: TestProcess,
    said: BufReader<ChildStderr>,
}

impl ImageLockTaker {
    fn start(store: &Path, lock: &Path, workspace: &str) -> Self {
        let mut process = TestProcess::spawn(
            Command::new(std::env::current_exe().expect("this test binary"))
                .args(["--exact", "image_lock_taker", "--nocapture"])
                .env(IMAGE_LOCK_ENV, lock)
                .env(IMAGE_STORE_ENV, store)
                .env(IMAGE_WORKSPACE_ENV, workspace)
                .stdin(Stdio::piped())
                .stdout(Stdio::null())
                .stderr(Stdio::piped()),
        );
        let said = BufReader::new(process.stderr.take().expect("the taker's stderr"));
        Self { process, said }
    }

    /// Its next stderr line, which it must write.
    fn next(&mut self) -> String {
        let mut line = String::new();
        let read = self
            .said
            .read_line(&mut line)
            .expect("read the taker's stderr");
        assert!(read > 0, "the image lock taker ended before its next line");
        line.trim_end().to_owned()
    }

    /// Close its stdin so it lets go of the lock, and require that it exits cleanly having said
    /// nothing more: letting go of an image lock is silent.
    fn release(mut self) {
        drop(self.process.child.stdin.take());
        let rest = self.rest();
        assert_eq!(
            self.process.wait().expect("wait for the taker"),
            ExitStatus::Exited { code: 0 },
            "{rest}"
        );
        assert_eq!(rest, "", "the taker said more once released");
    }

    /// Everything it writes on stderr from here until it ends.
    fn rest(&mut self) -> String {
        let mut rest = String::new();
        self.said
            .read_to_string(&mut rest)
            .expect("the taker's last output");
        rest
    }
}

/// The wait span's label for `lock`, and the custody the kernel shows of a recorded holder.
fn wait_step(lock: &Path) -> (String, &'static str) {
    let custody = if cfg!(target_os = "linux") {
        "holds it"
    } else {
        "holds it, as its record says"
    };
    (
        format!("cowshed: image-lock wait {}", lock.display()),
        custody,
    )
}

/// An image lock is a held lock (02_workspaces.md "Held locks"): the lifecycle operation that
/// holds one has recorded itself and the request it holds it for on it, so `lock-holders` names
/// it the owner with that request, and one that finds it held says, before it queues, which lock
/// it waits for and who holds it for what, and once granted how long it waited. The holder lets go
/// only after the waiter's `holders` line has been read off the waiter's pipe, so that line
/// precedes the release by order, not by a clock. Measured 2026-10-23: a clone waited 532.9 s on
/// `main.dmg.lock` while `lock-holders` answered `owner: null` and no line said what it waited
/// for.
#[test]
fn an_image_lock_names_its_holder_and_a_blocked_taker_says_whom_it_waits_for() {
    let dir = Dir::new("image-lock");
    let store = dir.path.join("store");
    let lock = store.join("acme/widget/main.dmg.lock");
    let mut holder = ImageLockTaker::start(&store, &lock, "raven");
    assert_eq!(
        holder.next(),
        "held",
        "an uncontended take says nothing first"
    );
    let observation = lock_holders(&lock);
    assert_eq!(observation["locked"], true, "{observation}");
    assert_eq!(
        observation["owner"]["pid"],
        holder.process.id(),
        "{observation}"
    );
    assert_eq!(
        observation["holding"],
        serde_json::json!({"operation": "fork", "workspace": "raven"}),
        "{observation}"
    );
    let record: Value = serde_json::from_str(
        observation["record"]["data"]
            .as_str()
            .expect("the holder record is UTF-8"),
    )
    .expect("the holder record is the held lock's");
    assert_eq!(record["pid"], holder.process.id(), "{observation}");
    assert_eq!(record["operation"], "fork", "{observation}");
    assert_eq!(record["workspace"], "raven", "{observation}");

    let mut waiter = ImageLockTaker::start(&store, &lock, "wren");
    let (step, custody) = wait_step(&lock);
    assert_eq!(waiter.next(), format!("{step} start"));
    let holders = waiter.next();
    assert!(
        holders.starts_with(&format!("{step} holders=pid {} (", holder.process.id()))
            && holders.ends_with(&format!(") {custody} for fork of raven")),
        "the wait did not name its holder and its request: {holders}"
    );

    holder.release();
    let done = waiter.next();
    assert!(
        done.starts_with(&format!("{step} done elapsed=")) && done.ends_with(" status=ok"),
        "{done}"
    );
    assert_eq!(waiter.next(), "held");
    let second = lock_holders(&lock);
    assert_eq!(second["owner"]["pid"], waiter.process.id(), "{second}");
    assert_eq!(
        second["holding"],
        serde_json::json!({"operation": "fork", "workspace": "wren"}),
        "{second}"
    );
    waiter.release();
    let released = lock_holders(&lock);
    assert_eq!(released["locked"], false, "{released}");
    assert_eq!(released["holding"], Value::Null, "{released}");
    assert_eq!(
        released["record"],
        serde_json::json!({"encoding": "utf8", "data": ""}),
        "the holder did not empty its record before it let go"
    );
}

/// A taker queued on an image lock whose file is replaced at its path before the holder lets go
/// is granted a lock on a file no path names: it refuses the grant, naming both inodes, and never
/// records itself or says `held`. The replacement happens after the waiter's `holders` line is
/// read off its pipe, so the waiter is queued on the old inode by order, not by a clock.
#[test]
fn a_taker_refuses_an_image_lock_whose_path_was_replaced_while_it_waited() {
    let dir = Dir::new("image-lock-replaced");
    let store = dir.path.join("store");
    let lock = store.join("acme/widget/main.dmg.lock");
    let mut holder = ImageLockTaker::start(&store, &lock, "raven");
    assert_eq!(holder.next(), "held");
    let mut waiter = ImageLockTaker::start(&store, &lock, "wren");
    let (step, _) = wait_step(&lock);
    assert_eq!(waiter.next(), format!("{step} start"));
    assert!(
        waiter.next().starts_with(&format!("{step} holders=")),
        "the waiter did not queue"
    );
    let queued = fs::metadata(&lock).expect("the queued inode").ino();
    let replacement = lock.with_file_name("replacement.lock");
    fs::write(&replacement, b"").expect("write the replacement");
    fs::rename(&replacement, &lock).expect("replace the lock file at its path");
    let named = fs::metadata(&lock).expect("the replacement inode").ino();
    assert_ne!(queued, named);

    holder.release();
    let done = waiter.next();
    assert!(
        done.starts_with(&format!("{step} done elapsed=")) && done.ends_with(" status=ok"),
        "the wait itself was granted: {done}"
    );
    let rest = waiter.rest();
    assert_ne!(
        waiter.process.wait().expect("wait for the waiter"),
        ExitStatus::Exited { code: 0 },
        "{rest}"
    );
    assert!(
        rest.contains("not the inode")
            && rest.contains(&format!(" {queued} just locked"))
            && rest.contains(&format!(" {named},")),
        "the refusal did not name both inodes: {rest}"
    );
    assert!(
        !rest.contains("held\n"),
        "the waiter held a replaced lock: {rest}"
    );
    assert_eq!(
        fs::read(&lock).expect("the replacement's record"),
        b"",
        "the refused waiter recorded itself on the file the path names"
    );
}
