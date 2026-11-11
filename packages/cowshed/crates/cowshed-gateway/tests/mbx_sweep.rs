#![cfg(target_os = "macos")]
//! The host's mbx store sweep belongs to no job (05_gateway.md, "The mbx store sweep").
//!
//! The gateway's sweep timer starts the installed mbx's `gc --automatic` while a job runs; the job
//! concludes, and its retirement signals no sweep: the sweep runs on, its stderr in mbx's own sweep
//! log. The same sweep forked inside a job, as a build's mbx forked it before every job carried
//! `MBX_GC_AUTO=0`, is that job's member, and its retirement ends it. The timer never starts a
//! sweep beside a running one, and starts the next once that one has ended. The installed mbx is a
//! script behind the link setup makes, under the fixture's own home, and its store is the
//! fixture's: nothing here reads or runs the user's mbx or store.

#[path = "support/fixture_dir.rs"]
mod fixture_dir;

use std::io::{BufRead as _, Write as _};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::fs::PermissionsExt as _;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::time::Duration;

use cowshed_core::api::dto::JobId;
use cowshed_core::api::process::JobProcessRetirement;
use cowshed_core::runtime::process_monitor::ProcessMonitor;
use cowshed_core::runtime::process_tree::{ProcessIdentity, TreeOwner};
use cowshed_gateway::MbxSweep;
use cowshed_os::deadline::{Progress, REAP};
use cowshed_os::test_backstop::ChildBackstop as _;

/// The installed mbx's stand-in. It records its environment and says one line on stderr, then
/// reports its start on its store's `started` fifo: its pid, whether another sweep of that store
/// was running (`alone` or `stacked`, by an atomic `mkdir`), and its arguments. It runs until a
/// line, or the end, arrives on its store's `release` fifo.
const FAKE_MBX: &str = r#"#!/bin/sh
env > "$MBX_CACHE_DIR/env.$$"
echo "fake sweep $$" >&2
if mkdir "$MBX_CACHE_DIR/running" 2>/dev/null; then alone=alone; else alone=stacked; fi
printf '%s %s %s\n' "$$" "$alone" "$*" > "$MBX_CACHE_DIR/started"
read -r _ < "$MBX_CACHE_DIR/release"
[ "$alone" = stacked ] || rmdir "$MBX_CACHE_DIR/running"
"#;

/// How long a fixture waits for a sweep to report or to take its release.
const BOUND: Duration = Duration::from_secs(30);

/// One sweep's report of its start.
#[derive(Debug)]
struct Start {
    pid: u32,
    alone: bool,
    args: String,
}

/// A store a sweep is pointed at, and the starts its sweeps report, in order.
struct Store {
    root: PathBuf,
    starts: mpsc::Receiver<Start>,
}

fn fifo(path: &Path) {
    let name = std::ffi::CString::new(path.as_os_str().as_bytes()).expect("a path without NUL");
    // SAFETY: a NUL-terminated path.
    let made = unsafe { libc::mkfifo(name.as_ptr(), 0o600) };
    assert_eq!(
        made,
        0,
        "mkfifo {}: {}",
        path.display(),
        std::io::Error::last_os_error()
    );
}

/// `work` on a thread of its own, answered within [`BOUND`]: a fifo's open blocks until its other
/// end opens, and only the bound ends a wait for an end that never comes.
fn within<T: Send + 'static>(what: &str, work: impl FnOnce() -> T + Send + 'static) -> T {
    let (sent, answer) = mpsc::channel();
    std::thread::spawn(move || {
        let _ = sent.send(work());
    });
    answer
        .recv_timeout(BOUND)
        .unwrap_or_else(|error| panic!("{what} within {BOUND:?}: {error}"))
}

impl Store {
    /// A store at `root` with its two fifos, and a reader of every start its sweeps report: each
    /// open of `started` blocks until a sweep opens it to write, and reads every line written
    /// before the last writer closes it.
    fn new(root: PathBuf) -> Self {
        std::fs::create_dir_all(&root).expect("the store");
        fifo(&root.join("started"));
        fifo(&root.join("release"));
        let (sent, starts) = mpsc::channel();
        let started = root.join("started");
        std::thread::spawn(move || {
            loop {
                let Ok(file) = std::fs::File::open(&started) else {
                    return;
                };
                for line in std::io::BufReader::new(file).lines() {
                    let Ok(line) = line else { return };
                    let mut words = line.splitn(3, ' ');
                    let start = Start {
                        pid: words
                            .next()
                            .and_then(|pid| pid.parse().ok())
                            .expect("a pid"),
                        alone: words.next() == Some("alone"),
                        args: words.next().unwrap_or_default().to_owned(),
                    };
                    if sent.send(start).is_err() {
                        return;
                    }
                }
            }
        });
        Self { root, starts }
    }

    /// The next sweep's start, within [`BOUND`].
    fn next(&self) -> Start {
        self.starts
            .recv_timeout(BOUND)
            .unwrap_or_else(|error| panic!("a sweep of {} started: {error}", self.root.display()))
    }

    /// Let the running sweep end.
    fn release(&self) {
        let release = self.root.join("release");
        within("a sweep took its release", move || {
            std::fs::OpenOptions::new()
                .write(true)
                .open(&release)
                .and_then(|mut fifo| fifo.write_all(b"\n"))
                .expect("release the sweep");
        });
    }
}

fn monitor(job: u64) -> ProcessMonitor {
    ProcessMonitor::prepare(TreeOwner::Job(JobId::new(job).expect("a job id")))
        .expect("prepare the job's process monitor")
}

/// Retire what the job retains: what a job's conclusion does to the members that outlive its
/// leader.
fn conclude(monitor: &ProcessMonitor, root: ProcessIdentity) -> JobProcessRetirement {
    monitor
        .retire_root(root, Duration::from_secs(1))
        .expect("retire the job")
}

fn alive(pid: u32) -> bool {
    let pid = libc::pid_t::try_from(pid).expect("a pid");
    // SAFETY: signal 0 checks for the process and sends nothing.
    unsafe { libc::kill(pid, 0) == 0 }
}

fn exited(pid: u32) {
    let deadline = cowshed_os::deadline::start("a fake sweep's exit", REAP);
    cowshed_os::deadline::await_exit("fake sweep", pid, &deadline, Progress::Nothing)
        .expect("the sweep exits");
}

/// The installed mbx under `root`: the fake behind the link setup makes, in a home of the
/// fixture's own. Answers that home and the link.
fn install_fake_mbx(root: &Path) -> (PathBuf, PathBuf) {
    let home = root.join("home");
    let program = root.join("mbx-pin/bin/mbx");
    std::fs::create_dir_all(program.parent().expect("a bin directory")).expect("the pin's bin");
    std::fs::write(&program, FAKE_MBX).expect("the fake mbx");
    std::fs::set_permissions(&program, std::fs::Permissions::from_mode(0o755))
        .expect("an executable mbx");
    let link = cowshed_core::capabilities::mbx::host_link(&home);
    std::fs::create_dir_all(link.parent().expect("the installed bin")).expect("the installed bin");
    std::os::unix::fs::symlink(&program, &link).expect("the installed mbx link");
    (home, link)
}

/// A test whose body fails leaves no sweep behind: every sweep the fixture saw start ends with
/// its fixture, on every way out of a test, a panic included. On 2026-11-11 a sweep of a run that
/// failed an assertion waited for its release an hour after its test had ended: it leads a
/// process group of its own, as every gateway sweep does, so nothing else ends it.
#[test]
fn a_failing_body_leaves_no_sweep_behind() {
    let root = fixture_dir::secure_fixture_dir(&format!("mbx-sweep-fail-{}", std::process::id()));
    let (home, _) = install_fake_mbx(root.path());
    let store = root.path().join("store");
    let (sent, started) = mpsc::channel();
    // The closure owns everything it touches; nothing it leaves half-written is read after it.
    let failed = std::panic::catch_unwind(std::panic::AssertUnwindSafe(move || {
        let swept = Store::new(store.clone());
        let _timer = MbxSweep {
            home,
            store,
            every: Duration::from_millis(10),
        }
        .start()
        .expect("start the sweep timer");
        let first = swept.next();
        sent.send(first.pid)
            .expect("the test hears the sweep's pid");
        panic!(
            "a failing body, while sweep pid {} waits for its release",
            first.pid
        );
    }));
    assert!(failed.is_err(), "the body failed");
    let pid = started
        .try_recv()
        .expect("a sweep started before the body failed");
    let deadline = cowshed_os::deadline::start("a failed body's sweep", REAP);
    let ended = cowshed_os::deadline::await_exit("fake sweep", pid, &deadline, Progress::Nothing);
    let left = cowshed_os::process::group_members(libc::pid_t::try_from(pid).expect("a pid"));
    assert!(
        ended.is_ok() && left.as_ref().is_ok_and(Vec::is_empty),
        "the sweep's group ended with its failed test: {ended:?}; still running: {left:?}"
    );
}

#[test]
fn the_gateway_sweep_belongs_to_no_job_and_never_stacks() {
    let root = fixture_dir::secure_fixture_dir(&format!("mbx-sweep-{}", std::process::id()));
    let (home, link) = install_fake_mbx(root.path());
    let swept = Store::new(root.path().join("store"));

    // A job runs, and the gateway's timer starts the sweep meanwhile.
    let monitor_one = monitor(1);
    let mut job = Command::new("/bin/sh");
    job.args(["-c", "read -r _"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let (mut child, job_root) = monitor_one.spawn_root(job).expect("spawn the job's root");
    let timer = MbxSweep {
        home: home.clone(),
        store: swept.root.clone(),
        every: Duration::from_millis(10),
    }
    .start()
    .expect("start the sweep timer");
    let first = swept.next();
    assert!(
        first.alone && first.args == "gc --automatic",
        "the timer ran the installed mbx's automatic sweep: {first:?}"
    );

    // The job ends on the line it waits for: `read` answers an input closed without one with 1.
    child
        .stdin
        .take()
        .expect("the job's stdin")
        .write_all(b"\n")
        .expect("end the job");
    let output = child
        .wait_with_output_bounded()
        .expect("the job's root ends");
    assert!(output.status.success(), "{output:?}");
    let retired = conclude(&monitor_one, job_root);
    assert!(
        retired
            .outlived
            .iter()
            .all(|member| member.pid != first.pid),
        "the job's retirement signalled no gateway sweep: {retired:?}"
    );
    assert!(
        alive(first.pid),
        "the sweep runs on after the job concluded"
    );
    let environment = std::fs::read_to_string(swept.root.join(format!("env.{}", first.pid)))
        .expect("the sweep's environment");
    for expected in [
        format!("HOME={}", home.display()),
        format!("MBX_CACHE_DIR={}", swept.root.display()),
        "MBX_TARGET_VIEWS=1".to_owned(),
    ] {
        assert!(
            environment.lines().any(|line| line == expected),
            "{expected} in the sweep's environment: {environment}"
        );
    }
    assert!(
        !environment
            .lines()
            .any(|line| line.starts_with("MBX_GC_AUTO=")),
        "the host's own mbx settings decide whether it sweeps: {environment}"
    );
    let log = std::fs::read_to_string(swept.root.join("actions/gc/v1/sweep.log"))
        .expect("mbx's sweep log");
    assert!(
        log.contains(&format!("fake sweep {}\n", first.pid)),
        "the sweep's stderr is mbx's sweep log: {log}"
    );

    // The same sweep forked inside a job, as a build's mbx forked it, is that job's member.
    let forked = Store::new(root.path().join("forked-store"));
    let monitor_two = monitor(2);
    let mut build = Command::new("python3");
    build
        .args([
            "-c",
            "import subprocess, sys\n\
             subprocess.Popen([sys.argv[1], 'gc', '--automatic'], start_new_session=True, \
             stdin=subprocess.DEVNULL, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)\n",
        ])
        .arg(&link)
        .env("MBX_CACHE_DIR", &forked.root)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    let (child, build_root) = monitor_two.spawn_root(build).expect("spawn the build");
    let inside = forked.next();
    let output = child.wait_with_output_bounded().expect("the build ends");
    assert!(output.status.success(), "{output:?}");
    let retired = conclude(&monitor_two, build_root);
    assert!(
        retired
            .outlived
            .iter()
            .any(|member| member.pid == inside.pid),
        "a sweep forked inside a job is its member, which the job's retirement ends: {retired:?}"
    );
    exited(inside.pid);

    // No sweep started beside the running one; the next starts once it has ended.
    swept.release();
    let second = swept.next();
    assert!(
        second.alone && second.pid != first.pid,
        "the timer started the next sweep only once the first had ended: {first:?}, then {second:?}"
    );
    exited(first.pid);
    drop(timer);
    swept.release();
    exited(second.pid);
}
