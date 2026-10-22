//! `cowshed hold`, run as the binary a caller runs (11_shell.md "Held leaders: `cowshed hold`").
//! The readiness proofs of the held leader:
//!
//! - the leader leads a session and a group of its own, which the holder is outside of: a leader
//!   that stops its whole group stops nothing of the holder, which still ends that group;
//! - an exited leader is observed without being reaped: its pid stays taken, and its group,
//!   descendants holding its output included, is still signalled until it is released;
//! - the end of the control descriptor releases, and a release reaps the leader once it exits;
//! - the leader's argv, environment, working directory and inherited descriptors are the
//!   caller's, byte for byte, while the control and state descriptors are never its;
//! - `started` names the leader's stdout and stderr as `lsof -F di` names them in any process
//!   holding them;
//! - a malformed request and a command that cannot start fail the hold with cowshed's own codes,
//!   and a signal the holder receives reaches the group as a request does;
//! - under `cowshed lock`, the hold is the lock's command and its group is the hold's;
//! - `cowshed fd-holders`, given the name `started` gave the leader's stdout, names a descendant
//!   that left the group and still holds it, once the leader exited.
#![cfg(unix)]

use std::ffi::OsStr;
use std::fs;
use std::io::{self, BufRead as _, BufReader, PipeReader, PipeWriter, Read as _, Write as _};
use std::os::fd::{AsRawFd as _, RawFd};
use std::os::unix::ffi::OsStrExt as _;
use std::os::unix::process::{CommandExt as _, ExitStatusExt as _};
use std::path::PathBuf;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use std::sync::atomic::{AtomicU32, Ordering};

use cowshed_core::api::dto::ExitStatus;
use cowshed_core::api::hold::{HeldDescriptor, HoldEvent, HoldSignal};

/// The `cowshed` binary under test, read when the test runs (see `disk_command.rs`).
fn cowshed() -> PathBuf {
    PathBuf::from(
        std::env::var_os("CARGO_BIN_EXE_cowshed")
            .expect("cargo and nextest set CARGO_BIN_EXE_cowshed for an integration test"),
    )
}

/// A directory of its own for one test, removed when the test ends.
struct Dir(PathBuf);

impl Dir {
    fn new(name: &str) -> Self {
        static NEXT: AtomicU32 = AtomicU32::new(0);
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let path =
            std::env::temp_dir().join(format!("cowshed-hold-{name}-{}-{n}", std::process::id()));
        fs::create_dir_all(&path).expect("create the test directory");
        Self(fs::canonicalize(&path).expect("canonicalize the test directory"))
    }
}

impl Drop for Dir {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_dir_all(&self.0) {
            cowshed_os::eprintln!("remove {}: {error}", self.0.display());
        }
    }
}

/// One `cowshed hold` this test runs: its control writer, its state reader, and the leader's
/// stdin and stdout, which the test holds the other ends of.
struct Hold {
    child: Child,
    control: Option<PipeWriter>,
    state: BufReader<PipeReader>,
    stdin: Option<ChildStdin>,
    stdout: Option<ChildStdout>,
}

impl Hold {
    /// `cowshed hold --control-fd 3 --state-fd 4 -- <argv>`.
    fn start(prefix: &[&OsStr], argv: &[&OsStr], configure: impl FnOnce(&mut Command)) -> Self {
        Self::start_with(prefix, (3, 4), argv, &[], configure)
    }

    /// `<prefix> hold --control-fd <control> --state-fd <state> -- <argv>`, the control and state
    /// descriptors handed over at those numbers, and each descriptor in `extra` at its own.
    fn start_with(
        prefix: &[&OsStr],
        (control_fd, state_fd): (RawFd, RawFd),
        argv: &[&OsStr],
        extra: &[(RawFd, RawFd)],
        configure: impl FnOnce(&mut Command),
    ) -> Self {
        let (control_read, control) = io::pipe().expect("control pipe");
        let (state, state_write) = io::pipe().expect("state pipe");
        let mut command = Command::new(prefix.first().copied().unwrap_or(cowshed().as_os_str()));
        command
            .args(prefix.iter().skip(1))
            .args(["hold", "--control-fd", &control_fd.to_string()])
            .args(["--state-fd", &state_fd.to_string(), "--"])
            .args(argv)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit());
        configure(&mut command);
        let mut placed = vec![
            (control_read.as_raw_fd(), control_fd),
            (state_write.as_raw_fd(), state_fd),
        ];
        placed.extend_from_slice(extra);
        // SAFETY: only fcntl, dup2 and close run between fork and exec, all async-signal-safe;
        // each source is first copied above every target, so no target overwrites a source.
        unsafe {
            command.pre_exec(move || {
                let mut copies = [0; 8];
                for (index, (source, _)) in placed.iter().enumerate() {
                    copies[index] = libc::fcntl(*source, libc::F_DUPFD, 64);
                    if copies[index] == -1 {
                        return Err(io::Error::last_os_error());
                    }
                }
                for (index, (_, target)) in placed.iter().enumerate() {
                    if libc::dup2(copies[index], *target) == -1 || libc::close(copies[index]) == -1
                    {
                        return Err(io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }
        let mut child = command.spawn().expect("spawn cowshed hold");
        drop((control_read, state_write));
        Self {
            control: Some(control),
            state: BufReader::new(state),
            stdin: child.stdin.take(),
            stdout: child.stdout.take(),
            child,
        }
    }

    /// The next event on the state channel, one whole JSON event on one line.
    fn event(&mut self) -> HoldEvent {
        let mut line = String::new();
        assert!(
            self.state.read_line(&mut line).expect("read a state line") > 0,
            "the state channel ended"
        );
        serde_json::from_str(line.trim_end_matches('\n'))
            .unwrap_or_else(|error| panic!("state line {line:?} is no hold event: {error}"))
    }

    /// `started`'s pid and the names of the leader's output.
    fn started(&mut self) -> (i32, HeldDescriptor, HeldDescriptor) {
        match self.event() {
            HoldEvent::Started { pid, output, .. } => (pid, output.stdout, output.stderr),
            other => panic!("the hold said {other:?} before started"),
        }
    }

    fn request(&mut self, line: &str) {
        writeln!(self.control.as_mut().expect("control is open"), "{line}")
            .expect("write a control line");
    }

    /// Release the leader explicitly, then close the control descriptor: the group may outlive
    /// this caller.
    fn release(&mut self) {
        self.request("release");
        drop(self.control.take());
    }

    /// Close the control descriptor with no release: the controller is gone.
    fn abandon(&mut self) {
        drop(self.control.take());
    }

    /// The state channel's remaining bytes, once cowshed closed it.
    fn rest(&mut self) -> String {
        let mut rest = String::new();
        self.state
            .read_to_string(&mut rest)
            .expect("read the rest of the state");
        rest
    }

    fn wait(&mut self) -> std::process::ExitStatus {
        self.child.wait().expect("wait for cowshed hold")
    }
}

/// What `ps` shows of `pid`'s state: `T` stopped, `Z` exited unreaped, and so on.
fn process_state(pid: i32) -> String {
    let shown = Command::new("ps")
        .args(["-o", "stat=", "-p", &pid.to_string()])
        .output()
        .expect("run ps");
    String::from_utf8(shown.stdout)
        .expect("ps prints ASCII")
        .trim()
        .to_owned()
}

/// The name a process holding `descriptor` shows for it in another process: `lsof -F di`'s `d`
/// field on Darwin, procfs's inode on Linux, which lsof prints as `i`.
#[cfg(target_os = "macos")]
fn named_in(pid: i32, descriptor: RawFd) -> HeldDescriptor {
    let listed = Command::new("/usr/sbin/lsof")
        .args(["-a", "-p", &pid.to_string(), "-d", &descriptor.to_string()])
        .args(["-F", "d"])
        .output()
        .expect("run lsof");
    let text = String::from_utf8(listed.stdout).expect("lsof prints ASCII");
    let device = text
        .lines()
        .find_map(|line| line.strip_prefix('d'))
        .expect("lsof names the pipe by its address");
    serde_json::from_value(serde_json::json!({"kind": "device", "device": device}))
        .expect("lsof's address is a kernel address")
}

#[cfg(target_os = "linux")]
fn named_in(pid: i32, descriptor: RawFd) -> HeldDescriptor {
    let link = fs::read_link(format!("/proc/{pid}/fd/{descriptor}")).expect("read the fd link");
    let inode = link
        .to_str()
        .and_then(|link| link.strip_prefix("pipe:["))
        .and_then(|link| link.strip_suffix(']'))
        .expect("a pipe's link names its inode");
    serde_json::from_value(serde_json::json!({"kind": "inode", "inode": inode}))
        .expect("procfs's inode is a decimal")
}

fn os(text: &str) -> &OsStr {
    OsStr::new(text)
}

/// Review finding #1 on the in-group custodian: a whole-group SIGSTOP stopped the custodian too,
/// so its cancel never ran. The holder is outside the leader's session and group, so when the
/// leader stops its whole group the holder still answers, and its `signal KILL` ends the group.
#[test]
fn a_group_that_stops_itself_is_still_ended_through_its_holder() {
    let mut hold = Hold::start(&[], &[os("sh"), os("-c"), os("kill -s STOP 0")], |_| {});
    let (pid, _, _) = hold.started();
    while !process_state(pid).starts_with('T') {
        std::thread::yield_now();
    }
    hold.request("signal KILL");
    assert_eq!(
        hold.event(),
        HoldEvent::Signalled {
            signal: HoldSignal::Kill
        }
    );
    assert_eq!(
        hold.event(),
        HoldEvent::Exited {
            status: ExitStatus::Signaled {
                signal: libc::SIGKILL,
                core_dumped: false
            }
        }
    );
    hold.release();
    assert_eq!(hold.rest(), "");
    assert_eq!(hold.wait().code(), Some(128 + libc::SIGKILL));
}

/// The leader exits while a descendant still holds its output. The exit is observed without
/// reaping: the leader's pid stays taken (a zombie), so its group id still names its group, and
/// `signal TERM` reaches the descendant, whose end ends the output: the holder kept no copy of
/// it. `started` named that output as the descendant's own descriptor shows it. Only `release`
/// reaps the leader, and the hold exits with the leader's status.
#[test]
fn an_exited_leader_is_held_until_released_and_its_group_still_signalled() {
    let mut hold = Hold::start(
        &[],
        &[os("sh"), os("-c"), os("sleep 600 & echo $!; exit 3")],
        |command| {
            command.stderr(Stdio::piped());
        },
    );
    let (pid, stdout, stderr) = hold.started();
    assert_ne!(stdout, stderr, "stdout and stderr are two pipes");
    assert_eq!(
        hold.event(),
        HoldEvent::Exited {
            status: ExitStatus::Exited { code: 3 }
        }
    );
    assert!(
        process_state(pid).starts_with('Z'),
        "the exited leader is held unreaped"
    );
    let mut output = BufReader::new(hold.stdout.take().expect("stdout"));
    let mut descendant = String::new();
    output
        .read_line(&mut descendant)
        .expect("the descendant's pid");
    let descendant: i32 = descendant.trim().parse().expect("a pid");
    assert_eq!(named_in(descendant, 1), stdout);
    assert_eq!(named_in(descendant, 2), stderr);
    hold.request("signal TERM");
    assert_eq!(
        hold.event(),
        HoldEvent::Signalled {
            signal: HoldSignal::Term
        }
    );
    let mut rest = String::new();
    output
        .read_to_string(&mut rest)
        .expect("the output ends with the descendant");
    assert_eq!(rest, "");
    assert!(
        process_state(pid).starts_with('Z'),
        "still held until released"
    );
    hold.request("release");
    assert_eq!(hold.wait().code(), Some(3));
    assert_eq!(hold.rest(), "");
}

/// An explicit release lets the leader outlive its controller: closing the control descriptor
/// after it changes nothing, the leader is reaped once it exits, and the hold exits with its
/// status.
#[test]
fn an_explicit_release_lets_a_running_leader_outlive_its_controller() {
    let mut hold = Hold::start(
        &[],
        &[os("sh"), os("-c"), os("read -r line; exit 5")],
        |_| {},
    );
    hold.started();
    hold.release();
    writeln!(hold.stdin.as_mut().expect("stdin"), "go").expect("let the leader exit");
    assert_eq!(
        hold.event(),
        HoldEvent::Exited {
            status: ExitStatus::Exited { code: 5 }
        }
    );
    assert_eq!(hold.rest(), "");
    assert_eq!(hold.wait().code(), Some(5));
}

/// The end of the control descriptor before a release is a controller gone, and the group ends as
/// a warm host's does: SIGTERM, the grace while anything of it still runs, then SIGKILL. The probe
/// says its pid only once it ignores SIGTERM, so only the SIGKILL after the grace can end it, and
/// the output it holds ends with it; the leader's death by SIGTERM is the hold's status.
#[test]
fn the_end_of_control_before_a_release_ends_the_whole_group() {
    let mut hold = Hold::start(
        &[],
        &[
            os("sh"),
            os("-c"),
            os(r#"sh -c 'trap "" TERM; echo $$; while :; do sleep 1; done' & wait"#),
        ],
        |_| {},
    );
    let (leader, _, _) = hold.started();
    let mut output = BufReader::new(hold.stdout.take().expect("stdout"));
    let mut probe = String::new();
    output.read_line(&mut probe).expect("the probe's pid");
    let probe: i32 = probe.trim().parse().expect("a pid");
    assert_eq!(
        group_of(probe),
        leader,
        "the probe that ignores SIGTERM is in the held group"
    );
    hold.abandon();
    for signal in [HoldSignal::Term, HoldSignal::Kill] {
        assert_eq!(hold.event(), HoldEvent::Signalled { signal });
    }
    assert_eq!(
        hold.event(),
        HoldEvent::Exited {
            status: ExitStatus::Signaled {
                signal: libc::SIGTERM,
                core_dumped: false
            }
        }
    );
    let mut rest = String::new();
    output
        .read_to_string(&mut rest)
        .expect("the output ends with the group");
    assert_eq!(rest, "");
    assert_eq!(hold.rest(), "");
    assert_eq!(hold.wait().code(), Some(128 + libc::SIGTERM));
}

/// A leader released while it runs is still signalled until it is reaped, and is reaped as soon
/// as it exits.
#[test]
fn a_leader_released_while_running_is_signalled_until_its_reap() {
    let mut hold = Hold::start(&[], &[os("sh"), os("-c"), os("read -r line")], |_| {});
    hold.started();
    hold.request("release");
    hold.request("signal KILL");
    assert_eq!(
        hold.event(),
        HoldEvent::Signalled {
            signal: HoldSignal::Kill
        }
    );
    assert_eq!(
        hold.event(),
        HoldEvent::Exited {
            status: ExitStatus::Signaled {
                signal: libc::SIGKILL,
                core_dumped: false
            }
        }
    );
    assert_eq!(hold.rest(), "");
    assert_eq!(hold.wait().code(), Some(128 + libc::SIGKILL));
}

/// The leader runs with the caller's argv, environment, working directory and descriptors, byte
/// for byte, and never with the control or state descriptor.
#[test]
fn the_leader_gets_the_callers_command_and_never_the_channels() {
    let dir = Dir::new("exact");
    let (_kept_read, kept) = io::pipe().expect("an inherited pipe");
    let odd = OsStr::from_bytes(b"a\xffb c");
    let script = r#"printf '%s\n' "$@" "$HOLD_PROBE" "$(pwd -P)"
for fd in 3 4 5; do
  if { true >&"$fd"; } 2>/dev/null; then echo "open $fd"; else echo "closed $fd"; fi
done"#;
    let mut hold = Hold::start_with(
        &[],
        (3, 4),
        &[os("sh"), os("-c"), os(script), os("probe"), odd, os("")],
        &[(kept.as_raw_fd(), 5)],
        |command| {
            command.env("HOLD_PROBE", odd).current_dir(&dir.0);
        },
    );
    hold.started();
    let mut expected = Vec::new();
    for line in [
        odd.as_bytes(),
        b"",
        odd.as_bytes(),
        dir.0.as_os_str().as_bytes(),
    ] {
        expected.extend_from_slice(line);
        expected.push(b'\n');
    }
    expected.extend_from_slice(b"closed 3\nclosed 4\nopen 5\n");
    let mut output = Vec::new();
    hold.stdout
        .take()
        .expect("stdout")
        .read_to_end(&mut output)
        .expect("the leader's output");
    assert_eq!(
        String::from_utf8_lossy(&output),
        String::from_utf8_lossy(&expected)
    );
    assert_eq!(output, expected);
    assert_eq!(
        hold.event(),
        HoldEvent::Exited {
            status: ExitStatus::Exited { code: 0 }
        }
    );
    hold.release();
    assert_eq!(hold.wait().code(), Some(0));
}

/// A line that is no request fails the holder once its leader runs: `commandFailed` names the
/// leader and the line, never `failed`, which says no command ran. No release can arrive any
/// more, so the group ends as a controller gone's does -- SIGTERM, then SIGKILL after the grace or
/// once nothing of it runs -- the leader's exit follows, and the holder exits with the usage
/// wrapper code once the leader is reaped.
#[test]
fn a_malformed_request_fails_the_holder_and_ends_its_group() {
    let mut hold = Hold::start(&[], &[os("sh"), os("-c"), os("read -r line")], |_| {});
    let (leader, _, _) = hold.started();
    hold.request("signal SIGKILL");
    let HoldEvent::CommandFailed { pid, reason } = hold.event() else {
        panic!("a malformed request fails a holder whose leader runs");
    };
    assert_eq!(pid, leader);
    assert!(reason.contains("\"signal SIGKILL\""), "{reason}");
    for signal in [HoldSignal::Term, HoldSignal::Kill] {
        assert_eq!(hold.event(), HoldEvent::Signalled { signal });
    }
    assert_eq!(
        hold.event(),
        HoldEvent::Exited {
            status: ExitStatus::Signaled {
                signal: libc::SIGTERM,
                core_dumped: false
            }
        }
    );
    assert_eq!(hold.rest(), "");
    assert_eq!(hold.wait().code(), Some(101));
}

/// A command that cannot start is `failed`, the only line, and the hold exits with the wrapper
/// code of a missing environment: nothing ran.
#[test]
fn a_command_that_cannot_start_fails_the_hold() {
    let mut hold = Hold::start(&[], &[os("/nonexistent/cowshed-hold-probe")], |_| {});
    let HoldEvent::Failed { reason } = hold.event() else {
        panic!("a command that cannot start is failed");
    };
    assert!(
        reason.contains("/nonexistent/cowshed-hold-probe"),
        "{reason}"
    );
    assert_eq!(hold.rest(), "");
    assert_eq!(hold.wait().code(), Some(104));
}

/// A signal the holder receives is sent to the group as a request is, and answered.
#[test]
fn a_signal_the_holder_receives_reaches_the_group() {
    let mut hold = Hold::start(&[], &[os("sh"), os("-c"), os("read -r line")], |_| {});
    hold.started();
    // SAFETY: SIGTERM to this test's own unreaped child.
    assert_eq!(
        unsafe {
            libc::kill(
                i32::try_from(hold.child.id()).expect("a pid fits"),
                libc::SIGTERM,
            )
        },
        0
    );
    assert_eq!(
        hold.event(),
        HoldEvent::Signalled {
            signal: HoldSignal::Term
        }
    );
    assert_eq!(
        hold.event(),
        HoldEvent::Exited {
            status: ExitStatus::Signaled {
                signal: libc::SIGTERM,
                core_dumped: false
            }
        }
    );
    hold.release();
    let status = hold.wait();
    assert_eq!(
        (status.code(), status.signal()),
        (Some(128 + libc::SIGTERM), None)
    );
}

/// Whether `reader` is at the end of its stream right now: every writer closed it. A writer still
/// open makes the read refuse to wait (`EAGAIN`), and this answers no rather than blocking.
fn at_end(reader: &impl std::os::fd::AsRawFd) -> bool {
    let descriptor = reader.as_raw_fd();
    // SAFETY: fcntl on a descriptor this test owns.
    let flags = unsafe { libc::fcntl(descriptor, libc::F_GETFL) };
    // SAFETY: as above.
    assert_eq!(
        unsafe { libc::fcntl(descriptor, libc::F_SETFL, flags | libc::O_NONBLOCK) },
        0
    );
    let mut byte = 0_u8;
    // SAFETY: `byte` has room for the one byte asked for.
    match unsafe { libc::read(descriptor, (&raw mut byte).cast(), 1) } {
        0 => true,
        -1 => {
            let error = io::Error::last_os_error();
            assert_eq!(error.kind(), io::ErrorKind::WouldBlock, "{error}");
            false
        }
        _ => panic!("the step wrote output it was not asked for"),
    }
}

/// Whether another holder holds the lock file's exclusive `flock(2)` now.
fn locked(path: &std::path::Path) -> bool {
    match fs::File::open(path).expect("open the lock file").try_lock() {
        Ok(()) => false,
        Err(fs::TryLockError::WouldBlock) => true,
        Err(fs::TryLockError::Error(error)) => panic!("probe the lock: {error}"),
    }
}

/// The process group `pid` is in.
fn group_of(pid: i32) -> i32 {
    // SAFETY: getpgid reads the kernel's view of a process; it touches no memory of ours.
    let group = unsafe { libc::getpgid(pid) };
    assert!(group > 0, "getpgid({pid}): {}", io::Error::last_os_error());
    group
}

/// What `cowshed fd-holders` prints for the object `name` names, parsed, and how long the census
/// took on this host, process start included.
fn fd_holders(name: HeldDescriptor) -> (serde_json::Value, std::time::Duration) {
    let (option, value) = match name {
        HeldDescriptor::Device { device } => ("--device", device.to_string()),
        HeldDescriptor::Inode { inode } => ("--inode", inode.to_string()),
        other => panic!("started names an output by its address or its inode, not {other:?}"),
    };
    let began = std::time::Instant::now();
    let printed = Command::new(cowshed())
        .args(["fd-holders", option, &value])
        .output()
        .expect("run cowshed fd-holders");
    let took = began.elapsed();
    assert!(
        printed.status.success(),
        "fd-holders failed: {}",
        String::from_utf8_lossy(&printed.stderr)
    );
    (
        serde_json::from_slice(&printed.stdout).expect("fd-holders prints one JSON object"),
        took,
    )
}

/// A process this test started in a session of its own, killed when the test lets it go.
struct Escaped(Option<i32>);

impl Escaped {
    fn end(&mut self) {
        if let Some(pid) = self.0.take() {
            // SAFETY: kill takes plain integers and touches no memory of ours.
            assert_eq!(
                unsafe { libc::kill(pid, libc::SIGKILL) },
                0,
                "kill {pid}: {}",
                io::Error::last_os_error()
            );
        }
    }
}

impl Drop for Escaped {
    fn drop(&mut self) {
        self.end();
    }
}

/// The watchdog's diagnosis (02_workspaces.md "Descriptor holders: `cowshed fd-holders`"): the
/// leader exits while a descendant that left its session and group with `setsid` keeps its
/// stdout. Given the name `started` gave that stdout, `fd-holders` names the descendant: its pid,
/// the life the kernel gives it (the same on a second census), its descriptor 1 and that object.
/// The held leader, exited, holds nothing, and the holder kept no copy, so on Darwin, where the
/// write end has an address of its own, the descendant is the only holder; on Linux the pipe's
/// one inode is also this test's read end. Once the descendant ends, it holds nothing. Each
/// census's wall time on this host is said on stderr for the record, never asserted.
#[test]
fn fd_holders_names_a_descendant_that_left_the_group_holding_the_output() {
    let mut hold = Hold::start(
        &[],
        &[
            os("sh"),
            os("-c"),
            os(
                "/usr/bin/perl -MPOSIX -e '$| = 1; POSIX::setsid() > 0 or die \"setsid: $!\"; \
                 close STDIN; print \"$$\\n\"; sleep 600' & exit 3",
            ),
        ],
        |_| {},
    );
    let (leader, stdout, _) = hold.started();
    assert_eq!(
        hold.event(),
        HoldEvent::Exited {
            status: ExitStatus::Exited { code: 3 }
        }
    );
    let mut output = BufReader::new(hold.stdout.take().expect("stdout"));
    let mut line = String::new();
    output.read_line(&mut line).expect("the descendant's pid");
    let pid: i32 = line.trim().parse().expect("a pid");
    let mut descendant = Escaped(Some(pid));
    assert_eq!(
        group_of(pid),
        pid,
        "the descendant leads a group of its own"
    );
    assert_ne!(
        group_of(pid),
        leader,
        "the descendant left the leader's group"
    );
    assert!(
        process_state(leader).starts_with('Z'),
        "the exited leader is held unreaped"
    );

    let (first, took) = fd_holders(stdout);
    cowshed_os::eprintln!("fd-holders census on this host: {took:?}");
    let holders = first["holders"].as_array().expect("holders");
    let named = holders
        .iter()
        .find(|holder| holder["pid"] == pid)
        .unwrap_or_else(|| panic!("the descendant is named: {first}"));
    assert_eq!(named["descriptors"], serde_json::json!([1]), "{first}");
    assert_eq!(named["matches"], serde_json::json!([stdout]), "{first}");
    let others: Vec<_> = holders
        .iter()
        .filter(|holder| holder["pid"] != pid)
        .map(|holder| holder["pid"].clone())
        .collect();
    let this = if cfg!(target_os = "linux") {
        vec![serde_json::json!(std::process::id())]
    } else {
        Vec::new()
    };
    assert_eq!(others, this, "{first}");
    assert!(first["uninspected"].is_u64(), "{first}");

    let (second, took) = fd_holders(stdout);
    cowshed_os::eprintln!("fd-holders census on this host: {took:?}");
    let again = second["holders"]
        .as_array()
        .expect("holders")
        .iter()
        .find(|holder| holder["pid"] == pid)
        .unwrap_or_else(|| panic!("the descendant is named again: {second}"));
    assert_eq!(
        again["uniqueId"], named["uniqueId"],
        "one life, one identity"
    );

    descendant.end();
    let mut rest = String::new();
    output
        .read_to_string(&mut rest)
        .expect("the output ends with the descendant");
    assert_eq!(rest, "");
    let (after, _) = fd_holders(stdout);
    let left: Vec<_> = after["holders"]
        .as_array()
        .expect("holders")
        .iter()
        .map(|holder| holder["pid"].clone())
        .collect();
    assert_eq!(left, this, "{after}");
    hold.request("release");
    assert_eq!(hold.wait().code(), Some(3));
    assert_eq!(hold.rest(), "");
}

/// The brief's composition, `cowshed lock <lock> --state-fd 3 -- cowshed hold --control-fd 4
/// --state-fd 5 -- <cmd>`: three distinct caller descriptors pass through the waiter to the
/// hold. The waiter holds the lock while the step runs, and the step's group is the hold's
/// leader's, neither the waiter's nor the holder's. The step's output ends with the step, before
/// any release, since neither the waiter nor the holder keeps a copy of it: a watchdog that
/// releases once the step exited and its output ended never waits on its own wrappers. Until the
/// release the leader stays held and the lock kept, and the leader's status is the lock's.
#[test]
fn a_hold_runs_as_the_command_of_a_lock() {
    let dir = Dir::new("lock");
    let lock = dir.0.join("step.lock");
    let (lock_state, lock_state_write) = io::pipe().expect("lock state pipe");
    let cowshed = cowshed();
    let mut hold = Hold::start_with(
        &[
            cowshed.as_os_str(),
            os("lock"),
            lock.as_os_str(),
            os("--state-fd"),
            os("3"),
            os("--"),
            cowshed.as_os_str(),
        ],
        (4, 5),
        &[os("sh"), os("-c"), os("read -r line; exit 6")],
        &[(lock_state_write.as_raw_fd(), 3)],
        |command| {
            command.stderr(Stdio::piped());
        },
    );
    drop(lock_state_write);
    let mut lock_state = BufReader::new(lock_state);
    let mut acquired = String::new();
    lock_state
        .read_line(&mut acquired)
        .expect("the waiter's state");
    assert!(acquired.starts_with(r#"{"kind":"acquired""#), "{acquired}");
    let (leader, _, _) = hold.started();
    assert!(locked(&lock), "the lock is held while the step runs");
    let waiter = i32::try_from(hold.child.id()).expect("a pid fits");
    let holder: i32 = Command::new("ps")
        .args(["-o", "ppid=", "-p", &leader.to_string()])
        .output()
        .map(|shown| String::from_utf8_lossy(&shown.stdout).trim().parse())
        .expect("run ps")
        .expect("the leader's parent, the holder");
    assert_ne!(
        holder, waiter,
        "the holder is the waiter's command, not the waiter"
    );
    assert_eq!(
        group_of(leader),
        leader,
        "the step's group is the leader's own"
    );
    assert_ne!(group_of(waiter), leader);
    assert_ne!(group_of(holder), leader);
    writeln!(hold.stdin.as_mut().expect("stdin"), "go").expect("let the step end");
    assert_eq!(
        hold.event(),
        HoldEvent::Exited {
            status: ExitStatus::Exited { code: 6 }
        }
    );
    let stdout = hold.stdout.take().expect("stdout");
    let stderr = hold.child.stderr.take().expect("stderr");
    assert!(at_end(&stdout), "the step's stdout ended with it");
    assert!(at_end(&stderr), "the step's stderr ended with it");
    assert!(
        process_state(leader).starts_with('Z'),
        "held until released"
    );
    assert!(locked(&lock), "the lock is kept until the release");
    hold.release();
    assert_eq!(hold.wait().code(), Some(6));
    let mut rest = String::new();
    lock_state
        .read_to_string(&mut rest)
        .expect("the waiter's state ends");
    assert_eq!(rest, "", "acquired is the waiter's last line");
    assert!(!locked(&lock), "the lock goes with the step");
}
