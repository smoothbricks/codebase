//! The gateway host: the process launchd runs for the gateway agent (05_gateway.md "The gateway
//! host").
//!
//! It runs from `bin/cowshed-gateway-host`, a path no install moves, signed with the user's
//! cowshed identity ([`crate::code_signing`]), and does three things only: spawn its arguments
//! as its child — `bin/cowshed gateway run`, so the daemon runs its own content-addressed copy —
//! pass the child the signals launchd stops a job with, and exit as the child did.
//!
//! It exists for macOS's first-launch scan. The scan is charged to a run's responsible process;
//! a launchd job is its own responsible process and everything it spawns inherits it. So every
//! workspace job's responsible process is this host, and one Developer Tools entry for its fixed
//! path and designated requirement exempts them all, across every install.
//!
//! It spawns and never execs: after an exec the process would take the new image's path and
//! signature, and the entry would no longer name it. The child stays in the host's process
//! group, so launchd's teardown of the job reaches it even when the host itself is killed.

use std::ffi::OsString;
use std::io;
use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
use std::process::{Command, ExitStatus};

use cowshed_os::fork_lock::Spawn as _;

/// The hidden verb launchd's `ProgramArguments` give the host, followed by its child's argv.
pub const VERB: &str = "__gateway-host";

/// The signals launchd and an operator end a job with, passed on to the child unchanged.
const FORWARDED: [libc::c_int; 4] = [libc::SIGTERM, libc::SIGINT, libc::SIGHUP, libc::SIGQUIT];

/// Serve as the gateway host for `child` (its program, then its arguments), and end this process
/// as the child ended. Runs before anything else in `main`, while the process has one thread:
/// the signal mask it blocks is then the whole process's.
pub fn run(child: &[OsString]) -> ! {
    match host(child) {
        Ok(status) => exit_as(status),
        Err(error) => {
            cowshed_os::eprintln!("cowshed gateway host: {error}");
            cowshed_os::stderr::flush();
            std::process::exit(EXIT_FAILED);
        }
    }
}

/// The host's own failure, before its child could run or be waited for: launchd's "exited with
/// code 1", with the reason on the agent's stderr log.
pub const EXIT_FAILED: i32 = 1;

fn host(child: &[OsString]) -> io::Result<ExitStatus> {
    let Some((program, arguments)) = child.split_first() else {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{VERB} needs the program to run: {VERB} <program> [arguments...]"),
        ));
    };
    // SIGCHLD's default action is to ignore it, and macOS discards an ignored signal even while
    // it is blocked, so sigwait would never see the child end. A handler makes it pend; blocked,
    // the handler never runs, and the exec that starts the child resets it.
    // SAFETY: the handler does nothing, so it is async-signal-safe.
    let handler = child_ended as extern "C" fn(libc::c_int) as libc::sighandler_t;
    let installed = unsafe { libc::signal(libc::SIGCHLD, handler) };
    if installed == libc::SIG_ERR {
        return Err(io::Error::last_os_error());
    }
    let mut waited = empty_signal_set();
    for signal in FORWARDED.iter().copied().chain([libc::SIGCHLD]) {
        // SAFETY: `waited` is an initialised signal set and `signal` a valid signal number.
        unsafe { libc::sigaddset(&mut waited, signal) };
    }
    // Block before the spawn, so no signal or exit lands between the spawn and the first wait.
    // The child inherits a spawner's blocked mask, so it is cleared again in the child before it
    // execs: the daemon must receive what is passed on.
    // SAFETY: `waited` is initialised; the old mask is not asked for.
    let blocked = unsafe { libc::pthread_sigmask(libc::SIG_BLOCK, &waited, std::ptr::null_mut()) };
    if blocked != 0 {
        return Err(io::Error::from_raw_os_error(blocked));
    }
    let mut daemon = Command::new(program);
    daemon.args(arguments);
    // SAFETY: pthread_sigmask is async-signal-safe, and `empty` is an initialised set moved in.
    let empty = empty_signal_set();
    unsafe {
        daemon.pre_exec(move || {
            match libc::pthread_sigmask(libc::SIG_SETMASK, &empty, std::ptr::null_mut()) {
                0 => Ok(()),
                error => Err(io::Error::from_raw_os_error(error)),
            }
        })
    };
    let mut daemon = daemon.spawn_locked().map_err(|error| {
        io::Error::new(
            error.kind(),
            format!("could not start {}: {error}", program.to_string_lossy()),
        )
    })?;
    let pid = libc::pid_t::try_from(daemon.id()).expect("a child pid fits pid_t");
    loop {
        let mut signal = 0;
        // SAFETY: `waited` is initialised and every signal in it is blocked.
        let waited_for = unsafe { libc::sigwait(&waited, &mut signal) };
        if waited_for != 0 {
            return Err(io::Error::from_raw_os_error(waited_for));
        }
        if signal != libc::SIGCHLD {
            // SAFETY: `pid` is this process's own unreaped child, so it names no other process.
            // A child that has already exited answers ESRCH and is reaped below.
            unsafe { libc::kill(pid, signal) };
        }
        if let Some(status) = daemon.try_wait()? {
            return Ok(status);
        }
    }
}

/// The SIGCHLD handler that makes the signal pend for `sigwait`; it never runs.
extern "C" fn child_ended(_: libc::c_int) {}

fn empty_signal_set() -> libc::sigset_t {
    let mut set = std::mem::MaybeUninit::<libc::sigset_t>::uninit();
    // SAFETY: sigemptyset initialises the whole set.
    unsafe {
        libc::sigemptyset(set.as_mut_ptr());
        set.assume_init()
    }
}

/// End this process as `status` says the child ended: its exit code, or death by its signal,
/// so launchd records for the agent exactly what it would have recorded for the daemon.
fn exit_as(status: ExitStatus) -> ! {
    cowshed_os::stderr::flush();
    if let Some(code) = status.code() {
        std::process::exit(code);
    }
    let signal = status
        .signal()
        .expect("a waited child exited or was signalled");
    let mut only = empty_signal_set();
    // SAFETY: SIG_DFL installs no handler; `only` is initialised; `raise` takes no memory
    // operands. Unblocked with its default action, the signal ends this process.
    unsafe {
        libc::signal(signal, libc::SIG_DFL);
        libc::sigaddset(&mut only, signal);
        libc::pthread_sigmask(libc::SIG_UNBLOCK, &only, std::ptr::null_mut());
        libc::raise(signal);
    }
    std::process::exit(128 + signal);
}
