//! The one bound that decides a test hang: the test target's no-progress backstop.
//!
//! A test waits for the event it is waiting for; no test carries a deadline of its own
//! (specs/cowshed/08_testing.md, the bounded test targets). A product bound a test must configure
//! that is not the case's subject — a gateway's connect or request bound around a fixture that
//! answers — is either disabled, where the product can disable it, or set to this backstop, so
//! it never races the host's load and the run's no-progress bound stays the one that decides.
//!
//! The value is the plugin's `BOUNDED_TEST_IDLE_TIMEOUT_MS`, read from its source at compile time,
//! so there is one number and no reader can pick a different one. The module is always compiled
//! (two constants and the test helpers below, which a binary that calls none of them never links), so a dependant's tests take it through a plain dependency,
//! with no feature that would split the build's units from the tests'.
//!
//! A test's own waits on another process take that bound too, through the helpers below
//! ([`ChildBackstop`], [`CommandBackstop`], their async twins, [`UnixStreamBackstop`],
//! [`FileBackstop`], [`waitpid`], [`waitid`], [`wait4`]): each crate's `clippy.toml` refuses the
//! raw waits everywhere, tests included (specs/cowshed/05_gateway.md, "Deadlines", Enforcement),
//! and a helper fails the one wait that stalled, naming its child, socket or lock, where the
//! runner's backstop would only name the test.

use std::io;
use std::path::Path;
use std::process::{ExitStatus, Output, Stdio};
use std::time::Duration;

use crate::deadline::{
    self, Bound, ChildPeer, Deadline, LatencyClass, OutputPumps, Progress, ReapError,
};
use crate::fork_lock::Spawn as _;

const POLICY: &str = include_str!("../../../../nx-plugin/src/test-backstop.ts");

/// The test target's no-progress backstop in milliseconds (`BOUNDED_TEST_IDLE_TIMEOUT_MS`), for
/// product bounds a test configures as a millisecond count.
pub const NO_PROGRESS_MS: u64 = declared_millis(
    POLICY.as_bytes(),
    b"export const BOUNDED_TEST_IDLE_TIMEOUT_MS = ",
);

/// The test target's no-progress backstop, for product bounds a test configures as a `Duration`.
pub const NO_PROGRESS: Duration = Duration::from_millis(NO_PROGRESS_MS);

/// The digits (`_` separators allowed) that follow `declaration` in `source`, up to `;`. A source
/// that does not declare it, or declares anything but a number literal, fails the build.
const fn declared_millis(source: &[u8], declaration: &[u8]) -> u64 {
    let mut start = 0;
    'search: while start + declaration.len() <= source.len() {
        let mut index = 0;
        while index < declaration.len() {
            if source[start + index] != declaration[index] {
                start += 1;
                continue 'search;
            }
            index += 1;
        }
        let mut position = start + declaration.len();
        let mut millis: u64 = 0;
        let mut digits = 0;
        while position < source.len() && source[position] != b';' {
            match source[position] {
                b'_' => {}
                byte @ b'0'..=b'9' => {
                    millis = millis * 10 + (byte - b'0') as u64;
                    digits += 1;
                }
                _ => panic!("{}", EXPECTED),
            }
            position += 1;
        }
        assert!(digits > 0 && position < source.len(), "{}", EXPECTED);
        return millis;
    }
    panic!("{}", EXPECTED)
}

const EXPECTED: &str = "packages/nx-plugin/src/test-backstop.ts must declare the test backstop \
     as one line `export const BOUNDED_TEST_IDLE_TIMEOUT_MS = <milliseconds>;` (digits and `_` only), \
     which cowshed-os/src/test_backstop.rs reads at compile time";

/// A test's wait on a child, a socket or a lock, ended by [`NO_PROGRESS`] (specs/cowshed/05_gateway.md,
/// "Deadlines", Enforcement): the bound the test runner would end the stalled test at anyway, here
/// failing the wait itself and naming the child, socket or lock it was on. The class is `reap`, a
/// wait on another process; the length is the backstop's, not the reap row's.
pub const BACKSTOP: Bound = Bound::new(LatencyClass::Reap, NO_PROGRESS);

const TEST_CHILD: &str = "test child";

fn child_expired(deadline: &Deadline, pid: u32) -> io::Error {
    io::Error::from(ReapError::Expired(Box::new(deadline.expired(
        ChildPeer::observed(TEST_CHILD, pid),
        Progress::Nothing,
        deadline::clock(),
    ))))
}

fn expired_on(op: &'static str, on: &str, bound: Bound) -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        format!("{op}: {on} did not answer within {:?}", bound.within),
    )
}

fn wait_within(child: &mut std::process::Child, bound: Bound) -> io::Result<ExitStatus> {
    drop(child.stdin.take());
    deadline::reap(
        TEST_CHILD,
        child,
        &deadline::start("test wait", bound),
        Progress::Nothing,
    )
    .map_err(io::Error::from)
}

/// Run the blocking `call` on a thread of its own and wait for its answer within `bound`. Past the
/// bound the wait fails `TimedOut` naming `op` and `on`; the thread stays with the call.
fn on_thread<T: Send + 'static>(
    op: &'static str,
    on: &str,
    bound: Bound,
    call: impl FnOnce() -> io::Result<T> + Send + 'static,
) -> io::Result<T> {
    let (answered, answer) = std::sync::mpsc::sync_channel(1);
    std::thread::Builder::new()
        .name(op.to_owned())
        .spawn(move || {
            // A waiter that gave up has already failed naming this call.
            let _ = answered.send(call());
        })?;
    match answer.recv_timeout(bound.within) {
        Ok(answer) => answer,
        Err(std::sync::mpsc::RecvTimeoutError::Timeout) => Err(expired_on(op, on, bound)),
        Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => Err(io::Error::other(format!(
            "{op}: the thread waiting on {on} ended without an answer"
        ))),
    }
}

/// `std::process::Child`'s waits, each ended by [`BACKSTOP`].
pub trait ChildBackstop {
    /// `wait`, which closes the child's stdin first, as `wait` does.
    fn wait_bounded(&mut self) -> io::Result<ExitStatus>;
    /// `wait_with_output`: stdin closed, then the exit and both piped streams' ends.
    fn wait_with_output_bounded(self) -> io::Result<Output>;
}

impl ChildBackstop for std::process::Child {
    fn wait_bounded(&mut self) -> io::Result<ExitStatus> {
        wait_within(self, BACKSTOP)
    }

    fn wait_with_output_bounded(mut self) -> io::Result<Output> {
        drop(self.stdin.take());
        let deadline = deadline::start("test wait", BACKSTOP);
        let pumps = OutputPumps::start(&mut self);
        let status = match deadline::reap(TEST_CHILD, &mut self, &deadline, Progress::Nothing) {
            Ok(status) => status,
            Err(failure) => {
                deadline::collect_later(TEST_CHILD, self);
                return Err(failure.into());
            }
        };
        let (stdout, stderr) = pumps
            .join(TEST_CHILD, self.id(), &deadline)
            .map_err(io::Error::from)?;
        Ok(Output {
            status,
            stdout,
            stderr,
        })
    }
}

/// [`crate::fork_lock::Run`], each run ended by [`BACKSTOP`].
pub trait CommandBackstop {
    /// `output_locked`: stdin null, stdout and stderr collected.
    fn output_bounded(&mut self) -> io::Result<Output>;
    /// `status_locked`: the child inherits every stream the command left unset.
    fn status_bounded(&mut self) -> io::Result<ExitStatus>;
}

impl CommandBackstop for std::process::Command {
    fn output_bounded(&mut self) -> io::Result<Output> {
        deadline::output_within(TEST_CHILD, self, &deadline::start("test run", BACKSTOP))
            .map_err(io::Error::from)
    }

    fn status_bounded(&mut self) -> io::Result<ExitStatus> {
        self.spawn_locked()?.wait_bounded()
    }
}

/// `tokio::process::Child`'s waits, each ended by [`BACKSTOP`].
pub trait AsyncChildBackstop {
    /// `wait`, which closes the child's stdin first, as `wait` does.
    fn wait_bounded(&mut self) -> impl Future<Output = io::Result<ExitStatus>> + Send;
    /// `wait_with_output`.
    fn wait_with_output_bounded(self) -> impl Future<Output = io::Result<Output>> + Send;
    /// `kill`: SIGKILL, then the reap.
    fn kill_bounded(&mut self) -> impl Future<Output = io::Result<()>> + Send;
}

impl AsyncChildBackstop for tokio::process::Child {
    fn wait_bounded(&mut self) -> impl Future<Output = io::Result<ExitStatus>> + Send {
        drop(self.stdin.take());
        async move {
            deadline::reap_async(
                TEST_CHILD,
                self,
                &deadline::start("test wait", BACKSTOP),
                Progress::Nothing,
            )
            .await
            .map_err(io::Error::from)
        }
    }

    fn wait_with_output_bounded(self) -> impl Future<Output = io::Result<Output>> + Send {
        let deadline = deadline::start("test wait", BACKSTOP);
        let pid = self.id().unwrap_or_default();
        async move {
            // WHY unbounded: the backstop's timeout around it ends it.
            #[allow(clippy::disallowed_methods)]
            let output = tokio::time::timeout(BACKSTOP.within, self.wait_with_output()).await;
            output.map_err(|_| child_expired(&deadline, pid))?
        }
    }

    fn kill_bounded(&mut self) -> impl Future<Output = io::Result<()>> + Send {
        let killed = self.start_kill();
        async move {
            killed?;
            self.wait_bounded().await.map(drop)
        }
    }
}

/// [`crate::fork_lock::RunAsync`], each run ended by [`BACKSTOP`].
pub trait AsyncCommandBackstop {
    /// `output_locked`: stdout and stderr collected.
    fn output_bounded(&mut self) -> impl Future<Output = io::Result<Output>> + Send;
    /// `status_locked`: the child's piped streams are closed before it is waited for.
    fn status_bounded(&mut self) -> impl Future<Output = io::Result<ExitStatus>> + Send;
}

impl AsyncCommandBackstop for tokio::process::Command {
    fn output_bounded(&mut self) -> impl Future<Output = io::Result<Output>> + Send {
        let child = self
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn_locked();
        async move { child?.wait_with_output_bounded().await }
    }

    fn status_bounded(&mut self) -> impl Future<Output = io::Result<ExitStatus>> + Send {
        let child = self.spawn_locked();
        async move {
            let mut child = child?;
            // A child blocked on a pipe this process holds would never exit.
            drop(child.stdout.take());
            drop(child.stderr.take());
            child.wait_bounded().await
        }
    }
}

/// `std::os::unix::net::UnixStream::connect`, ended by [`BACKSTOP`].
pub trait UnixStreamBackstop: Sized {
    fn connect_bounded(path: impl AsRef<Path>) -> io::Result<Self>;
}

impl UnixStreamBackstop for std::os::unix::net::UnixStream {
    // WHY unbounded: `on_thread` gives up on the connect at the backstop.
    #[allow(clippy::disallowed_methods)]
    fn connect_bounded(path: impl AsRef<Path>) -> io::Result<Self> {
        let path = path.as_ref().to_owned();
        let on = path.display().to_string();
        on_thread("test connect", &on, BACKSTOP, move || Self::connect(&path))
    }
}

/// `tokio::net::UnixStream::connect`, ended by [`BACKSTOP`].
pub trait AsyncUnixStreamBackstop: Sized {
    fn connect_bounded(path: impl AsRef<Path>) -> impl Future<Output = io::Result<Self>> + Send;
}

impl AsyncUnixStreamBackstop for tokio::net::UnixStream {
    fn connect_bounded(path: impl AsRef<Path>) -> impl Future<Output = io::Result<Self>> + Send {
        let path = path.as_ref().to_owned();
        async move {
            // WHY unbounded: the backstop's timeout around it ends it.
            #[allow(clippy::disallowed_methods)]
            let connected = tokio::time::timeout(BACKSTOP.within, Self::connect(&path)).await;
            connected
                .map_err(|_| expired_on("test connect", &path.display().to_string(), BACKSTOP))?
        }
    }
}

/// `std::fs::File`'s blocking locks, each ended by [`BACKSTOP`]. The lock is taken through a
/// duplicate of the descriptor, which shares the open file and so the lock.
pub trait FileBackstop {
    /// `lock`: exclusive.
    fn lock_bounded(&self) -> io::Result<()>;
    /// `lock_shared`.
    fn lock_shared_bounded(&self) -> io::Result<()>;
}

impl FileBackstop for std::fs::File {
    fn lock_bounded(&self) -> io::Result<()> {
        lock_within(self, false, BACKSTOP)
    }

    fn lock_shared_bounded(&self) -> io::Result<()> {
        lock_within(self, true, BACKSTOP)
    }
}

// WHY unbounded: `on_thread` gives up on the lock at `bound`.
#[allow(clippy::disallowed_methods)]
fn lock_within(file: &std::fs::File, shared: bool, bound: Bound) -> io::Result<()> {
    let duplicate = file.try_clone()?;
    on_thread("test lock", "the locked file", bound, move || {
        if shared {
            duplicate.lock_shared()
        } else {
            duplicate.lock()
        }
    })
}

/// `waitpid(pid, &status, options)`: the pid it answered and the status, ended by [`BACKSTOP`]. A
/// `WNOHANG` call never waits and runs here.
pub fn waitpid(pid: libc::pid_t, options: libc::c_int) -> io::Result<(libc::pid_t, libc::c_int)> {
    let call = move || {
        let mut status = 0;
        // WHY unbounded: `WNOHANG` returns at once; `on_thread` gives up on any other call at the
        // backstop.
        #[allow(clippy::disallowed_methods)]
        // SAFETY: `status` is a live out-pointer for the call.
        let answered = unsafe { libc::waitpid(pid, &raw mut status, options) };
        if answered == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok((answered, status))
        }
    };
    if options & libc::WNOHANG == 0 {
        on_thread("test waitpid", &format!("pid {pid}"), BACKSTOP, call)
    } else {
        call()
    }
}

/// A `siginfo_t` the kernel filled for `waitid`.
struct Filled(libc::siginfo_t);

// SAFETY: a filled `siginfo_t` is plain data; its one pointer field is an address the kernel
// reports and nothing dereferences, so moving it to the waiting thread shares nothing.
unsafe impl Send for Filled {}

/// `waitid(idtype, id, &info, options)`: the filled `siginfo_t`, ended by [`BACKSTOP`]. A
/// `WNOHANG` call never waits and runs here.
pub fn waitid(
    idtype: libc::idtype_t,
    id: libc::id_t,
    options: libc::c_int,
) -> io::Result<libc::siginfo_t> {
    let call = move || {
        // SAFETY: an all-zero `siginfo_t` is valid storage for the call to fill.
        let mut info: libc::siginfo_t = unsafe { std::mem::zeroed() };
        // WHY unbounded: `WNOHANG` returns at once; `on_thread` gives up on any other call at the
        // backstop.
        #[allow(clippy::disallowed_methods)]
        // SAFETY: `info` is live storage for the call to fill.
        let answered = unsafe { libc::waitid(idtype, id, &raw mut info, options) };
        if answered == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok(Filled(info))
        }
    };
    let filled = if options & libc::WNOHANG == 0 {
        on_thread("test waitid", &format!("id {id}"), BACKSTOP, call)?
    } else {
        call()?
    };
    Ok(filled.0)
}

/// `wait4(pid, &status, options, &usage)`: the pid it answered, the status and the usage, ended by
/// [`BACKSTOP`]. A `WNOHANG` call never waits and runs here.
pub fn wait4(
    pid: libc::pid_t,
    options: libc::c_int,
) -> io::Result<(libc::pid_t, libc::c_int, libc::rusage)> {
    let call = move || {
        let mut status = 0;
        // SAFETY: an all-zero `rusage` is valid storage for the call to fill.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        // WHY unbounded: `WNOHANG` returns at once; `on_thread` gives up on any other call at the
        // backstop.
        #[allow(clippy::disallowed_methods)]
        // SAFETY: `status` and `usage` are live storage for the call to fill.
        let answered = unsafe { libc::wait4(pid, &raw mut status, options, &raw mut usage) };
        if answered == -1 {
            Err(io::Error::last_os_error())
        } else {
            Ok((answered, status, usage))
        }
    };
    if options & libc::WNOHANG == 0 {
        on_thread("test wait4", &format!("pid {pid}"), BACKSTOP, call)
    } else {
        call()
    }
}
#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;

    /// A bound short against a peer that never answers, which is each case's subject.
    const SHORT: Bound = Bound::new(LatencyClass::Reap, Duration::from_millis(50));

    #[test]
    fn a_child_still_running_at_the_bound_fails_naming_it() {
        let mut child = std::process::Command::new("/bin/sleep")
            .arg("600")
            .spawn_locked()
            .expect("spawn sleep");
        let pid = child.id();
        let failure = wait_within(&mut child, SHORT).expect_err("sleep outlives the bound");
        assert_eq!(failure.kind(), io::ErrorKind::TimedOut, "{failure}");
        assert!(
            failure.to_string().contains(&format!("pid {pid}")),
            "{failure}"
        );
        child.kill().expect("end sleep");
        assert!(!child.wait_bounded().expect("reap sleep").success());
    }

    #[test]
    fn a_run_answers_its_status_and_both_streams() {
        let output = std::process::Command::new("/bin/sh")
            .args(["-c", "echo out; echo err >&2; exit 3"])
            .output_bounded()
            .expect("run sh");
        assert_eq!(output.status.code(), Some(3));
        assert_eq!(output.stdout, b"out\n");
        assert_eq!(output.stderr, b"err\n");
    }

    #[test]
    fn a_lock_held_elsewhere_at_the_bound_fails_naming_the_wait() {
        let fixture = Fixture::new();
        let holder = File::create(&fixture.0).expect("create the lock file");
        holder.try_lock().expect("hold the lock");
        let waiter = File::open(&fixture.0).expect("open the lock file");
        let failure = lock_within(&waiter, false, SHORT).expect_err("the lock stays held");
        assert_eq!(failure.kind(), io::ErrorKind::TimedOut, "{failure}");
        assert!(failure.to_string().contains("test lock"), "{failure}");
    }

    #[test]
    fn waitpid_answers_an_exited_child() {
        #[expect(
            clippy::zombie_processes,
            reason = "the case reaps it through waitpid, its subject"
        )]
        let child = std::process::Command::new("/bin/sh")
            .args(["-c", ":"])
            .spawn_locked()
            .expect("spawn sh");
        let pid = libc::pid_t::try_from(child.id()).expect("a pid");
        let (answered, status) = waitpid(pid, 0).expect("reap sh");
        assert_eq!(answered, pid);
        assert!(libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0);
    }

    /// A lock file of this test's own, removed on every exit path.
    struct Fixture(std::path::PathBuf);

    impl Fixture {
        fn new() -> Self {
            Self(std::env::temp_dir().join(format!("cowshed-os-backstop-{}", uuid::Uuid::new_v4())))
        }
    }

    impl Drop for Fixture {
        fn drop(&mut self) {
            if let Err(error) = std::fs::remove_file(&self.0)
                && error.kind() != io::ErrorKind::NotFound
            {
                panic!("remove {}: {error}", self.0.display());
            }
        }
    }

    #[test]
    fn the_declared_number_is_read_with_its_separators() {
        assert_eq!(
            declared_millis(b"x\nexport const B = 180_000;\n", b"export const B = "),
            180_000
        );
    }
}
