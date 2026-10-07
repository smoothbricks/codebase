//! Measures the Linux process event sources the supervisor chooses between (07_api.md,
//! "Complete job accounting and observation reconciliation"): proc connector `CN_PROC` and ptrace
//! fork/exec/exit tracing, each against an unobserved baseline and a census control that reads
//! the tree before and after a burst, as a one-second poll would.
//!
//! One workload runs unchanged under every mode. The harness re-executes this test binary as the
//! job's root, held on a gate pipe until the observer is ready. The root forks children one after
//! another; each forks a grandchild that execs `true proc-source <i>`. Every parent records each
//! child it reaps -- pid, parent, kernel start time read while the child is an unreaped zombie,
//! wait status and exec index -- into an oracle file, independently of any observer.
//!
//! A mode the kernel or sandbox refuses is reported with the refused call and its errno, never
//! as an observation with zero events. The report is printed and, when
//! `COWSHED_PROCESS_SOURCE_REPORT` names a file, written there.
//!
//! `PTRACE_O_EXITKILL` belongs to this fixture: its tracees are its own. It says nothing about
//! what a production observer may do to a job when it is lost.

#![cfg(target_os = "linux")]

use std::collections::{BTreeMap, HashMap, HashSet};
use std::ffi::{CString, c_char, c_int, c_void};
use std::fs;
use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::ptr;
use std::time::Instant;

use serde::Serialize;

const ROLE: &str = "COWSHED_PROCESS_SOURCE_ROLE";
const TRUE_PROGRAM: &str = "COWSHED_PROCESS_SOURCE_TRUE";
const REPORT: &str = "COWSHED_PROCESS_SOURCE_REPORT";
const ROOT_TEST: &str = "process_source_workload_root";

/// The fds the root inherits.
const GATE_FD: RawFd = 3;
const ORACLE_FD: RawFd = 4;
const DONE_FD: RawFd = 5;
const HOLD_FD: RawFd = 6;

/// A burst a one-second census cannot see, and a fork-heavy run for overhead.
const COVERAGE_BURST: u32 = 32;
const OVERHEAD_BURST: u32 = 512;
const REPETITIONS: u32 = 5;

const RECORD_BYTES: usize = 24;

/// The root's exit code when `PTRACE_TRACEME` fails; the errno follows on the error pipe.
const TRACEME_FAILED: c_int = 121;

const CN_IDX_PROC: u32 = 1;
const CN_VAL_PROC: u32 = 1;
const PROC_CN_MCAST_LISTEN: u32 = 1;
const PROC_EVENT_NONE: u32 = 0;
const PROC_EVENT_FORK: u32 = 1;
const PROC_EVENT_EXEC: u32 = 2;
const PROC_EVENT_EXIT: u32 = 0x8000_0000;
/// The `ack` of this fixture's listen request.
const LISTEN_ACK: u32 = 0x6373_6864;
const NLMSG_HEADER: usize = 16;
const CN_MSG_HEADER: usize = 20;

// ---------------------------------------------------------------------------------------------
// The workload root.

/// The job's root, run only when the harness re-executes this binary with [`ROLE`] set.
#[test]
fn process_source_workload_root() {
    let Some(burst) = std::env::var_os(ROLE) else {
        return;
    };
    let burst: u32 = burst
        .to_str()
        .and_then(|burst| burst.parse().ok())
        .expect("burst size");
    let program = CString::new(
        std::env::var_os(TRUE_PROGRAM)
            .expect("true program")
            .into_vec(),
    )
    .expect("program path");
    let arguments: Vec<[CString; 3]> = (0..burst)
        .map(|index| {
            [
                CString::new("true").expect("argv"),
                CString::new("proc-source").expect("argv"),
                CString::new(index.to_string()).expect("argv"),
            ]
        })
        .collect();
    let argv: Vec<[*const c_char; 4]> = arguments
        .iter()
        .map(|[a, b, c]| [a.as_ptr(), b.as_ptr(), c.as_ptr(), ptr::null()])
        .collect();
    let environment: [*const c_char; 1] = [ptr::null()];

    read_byte(GATE_FD).expect("gate");
    for (index, argv) in argv.iter().enumerate() {
        let index = i32::try_from(index).expect("index");
        // SAFETY: after fork the child calls only async-signal-safe functions on memory
        // prepared before the fork.
        match unsafe { libc::fork() } {
            -1 => panic!("fork: {}", io::Error::last_os_error()),
            0 => unsafe {
                let grandchild = libc::fork();
                if grandchild == 0 {
                    libc::execve(program.as_ptr(), argv.as_ptr(), environment.as_ptr());
                    libc::_exit(127);
                }
                if grandchild < 0 || !reap_recording(grandchild, index) {
                    libc::_exit(125);
                }
                libc::_exit(0);
            },
            child => assert!(reap_recording(child, -1), "record child {child}"),
        }
    }
    write_byte(DONE_FD).expect("done");
    read_byte(HOLD_FD).expect("hold");
}

/// Wait for `pid` to exit without reaping it, read its start time while it is a zombie, reap it
/// and append its record to the oracle. Async-signal-safe: no allocation.
fn reap_recording(pid: libc::pid_t, exec_index: i32) -> bool {
    let Ok(id) = libc::id_t::try_from(pid) else {
        return false;
    };
    // SAFETY: plain syscalls on stack memory.
    unsafe {
        let mut info: libc::siginfo_t = std::mem::zeroed();
        if libc::waitid(libc::P_PID, id, &mut info, libc::WEXITED | libc::WNOWAIT) != 0 {
            return false;
        }
        let Some(start) = start_time(pid) else {
            return false;
        };
        let mut status = 0;
        if libc::waitpid(pid, &mut status, 0) != pid {
            return false;
        }
        let Ok(pid) = u32::try_from(pid) else {
            return false;
        };
        let Ok(parent) = u32::try_from(libc::getpid()) else {
            return false;
        };
        let mut record = [0_u8; RECORD_BYTES];
        record[0..4].copy_from_slice(&pid.to_ne_bytes());
        record[4..8].copy_from_slice(&parent.to_ne_bytes());
        record[8..16].copy_from_slice(&start.to_ne_bytes());
        record[16..20].copy_from_slice(&status.to_ne_bytes());
        record[20..24].copy_from_slice(&exec_index.to_ne_bytes());
        let written = libc::write(ORACLE_FD, record.as_ptr().cast(), RECORD_BYTES);
        usize::try_from(written).is_ok_and(|written| written == RECORD_BYTES)
    }
}

/// Field 22 of `/proc/<pid>/stat`: clock ticks after boot. Async-signal-safe.
fn start_time(pid: libc::pid_t) -> Option<u64> {
    let mut path = [0_u8; 32];
    let mut length = 0;
    for &byte in b"/proc/" {
        path[length] = byte;
        length += 1;
    }
    let mut digits = [0_u8; 10];
    let mut count = 0;
    let mut value = u32::try_from(pid).ok()?;
    loop {
        digits[count] = b'0' + u8::try_from(value % 10).ok()?;
        count += 1;
        value /= 10;
        if value == 0 {
            break;
        }
    }
    for index in (0..count).rev() {
        path[length] = digits[index];
        length += 1;
    }
    for &byte in b"/stat\0" {
        path[length] = byte;
        length += 1;
    }
    let mut buffer = [0_u8; 1024];
    let mut filled = 0;
    // SAFETY: plain syscalls on stack memory.
    unsafe {
        let fd = libc::open(path.as_ptr().cast(), libc::O_RDONLY | libc::O_CLOEXEC);
        if fd < 0 {
            return None;
        }
        loop {
            let read = libc::read(
                fd,
                buffer[filled..].as_mut_ptr().cast(),
                buffer.len() - filled,
            );
            match usize::try_from(read) {
                Ok(0) => break,
                Ok(read) => {
                    filled += read;
                    if filled == buffer.len() {
                        break;
                    }
                }
                Err(_) => {
                    libc::close(fd);
                    return None;
                }
            }
        }
        libc::close(fd);
    }
    let stat = &buffer[..filled];
    let close = stat.iter().rposition(|&byte| byte == b')')?;
    // Field 3 (state) follows ") "; field 22 is the 20th from there.
    let field = stat[close + 2..].split(|&byte| byte == b' ').nth(19)?;
    let mut ticks: u64 = 0;
    for &byte in field {
        if !byte.is_ascii_digit() {
            return None;
        }
        ticks = ticks.checked_mul(10)?.checked_add(u64::from(byte - b'0'))?;
    }
    Some(ticks)
}

fn read_byte(fd: RawFd) -> io::Result<()> {
    let mut byte = 0_u8;
    loop {
        // SAFETY: one byte into a stack variable.
        let read = unsafe { libc::read(fd, ptr::from_mut(&mut byte).cast(), 1) };
        match read {
            1 => return Ok(()),
            0 => return Err(io::Error::from(io::ErrorKind::UnexpectedEof)),
            _ => {
                let error = io::Error::last_os_error();
                if error.kind() != io::ErrorKind::Interrupted {
                    return Err(error);
                }
            }
        }
    }
}

fn write_byte(fd: RawFd) -> io::Result<()> {
    // SAFETY: one byte from a static.
    match unsafe { libc::write(fd, b"x".as_ptr().cast(), 1) } {
        1 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

// ---------------------------------------------------------------------------------------------
// The report.

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
enum Mode {
    Baseline,
    Census,
    CnProc,
    Ptrace,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Report {
    kernel: String,
    namespaces: NamespaceFacts,
    runs: Vec<Run>,
    cn_proc_overflow: Overflow,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct NamespaceFacts {
    uid_map: Result<String, String>,
    ns_pid: Result<String, String>,
    yama_ptrace_scope: Result<String, String>,
    seccomp: Result<String, String>,
    cap_eff: Result<String, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Run {
    mode: Mode,
    burst: u32,
    repetition: u32,
    outcome: Outcome,
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum Outcome {
    Unavailable {
        call: String,
        errno: Option<i32>,
        detail: String,
    },
    Measured(Box<Measurement>),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Measurement {
    /// From the gate's release to the root's report that its burst is done.
    wall_ns: u64,
    observer_user_us: u64,
    observer_sys_us: u64,
    workload_user_us: u64,
    workload_sys_us: u64,
    expected: Expected,
    coverage: Coverage,
    /// Observations whose identity, argv or status could not be read, retained as observed.
    observed: Observed,
    /// Netlink only: buffer overflow reports and per-CPU sequence discontinuities.
    losses: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Expected {
    births: usize,
    execs: usize,
    exits: usize,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Coverage {
    births_matched: usize,
    births_missing: Vec<Identity>,
    births_wrong_parent: Vec<String>,
    execs_matched: usize,
    execs_missing: Vec<Identity>,
    execs_mismatched: Vec<String>,
    exits_matched: usize,
    exits_missing: Vec<Identity>,
    exits_mismatched: Vec<String>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
struct Identity {
    pid: u32,
    start: u64,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Observed {
    births: Vec<Birth>,
    execs: Vec<Exec>,
    exits: Vec<Exit>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Birth {
    pid: u32,
    ppid: u32,
    start: Result<u64, String>,
    /// Whether a pidfd could be opened when the birth was observed.
    pidfd: Result<(), String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Exec {
    pid: u32,
    start: Result<u64, String>,
    argv: Result<Vec<String>, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Exit {
    pid: u32,
    start: Result<u64, String>,
    wait_status: Option<i32>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Overflow {
    outcome: Result<OverflowCounts, String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct OverflowCounts {
    receive_buffer_bytes: i32,
    events: usize,
    enobufs: usize,
    sequence_gaps: Vec<String>,
}

// ---------------------------------------------------------------------------------------------
// The harness.

#[test]
fn linux_process_event_sources_are_measured() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let fixture = Fixture::new();
    let mut runs = Vec::new();
    for repetition in 0..REPETITIONS {
        for burst in [COVERAGE_BURST, OVERHEAD_BURST] {
            for mode in [Mode::Baseline, Mode::Census, Mode::CnProc, Mode::Ptrace] {
                let outcome = fixture.run(mode, burst);
                runs.push(Run {
                    mode,
                    burst,
                    repetition,
                    outcome,
                });
            }
        }
    }
    let report = Report {
        kernel: fs::read_to_string("/proc/sys/kernel/osrelease")
            .map(|release| release.trim().to_owned())
            .unwrap_or_else(|error| format!("unreadable: {error}")),
        namespaces: namespace_facts(),
        cn_proc_overflow: fixture.cn_proc_overflow(),
        runs,
    };
    let json = serde_json::to_string_pretty(&report).expect("report");
    println!("{json}");
    if let Some(path) = std::env::var_os(REPORT) {
        fs::write(path, &json).expect("write report");
    }

    for run in &report.runs {
        let Outcome::Measured(measured) = &run.outcome else {
            assert!(
                matches!(run.mode, Mode::CnProc | Mode::Ptrace),
                "{:?} cannot be unavailable",
                run.mode
            );
            continue;
        };
        if run.mode == Mode::Census && run.burst == COVERAGE_BURST {
            assert!(
                !measured.coverage.births_missing.is_empty(),
                "a census before and after the burst must miss its short-lived births"
            );
        }
    }
}

struct Fixture {
    executable: CString,
    argv: Vec<CString>,
    environment: Vec<CString>,
    harness: u32,
}

impl Fixture {
    fn new() -> Self {
        let executable = std::env::current_exe().expect("test binary");
        let true_program = find_program("true");
        let argv = [
            executable.as_os_str().as_bytes(),
            b"--exact",
            ROOT_TEST.as_bytes(),
            b"--nocapture",
        ]
        .iter()
        .map(|argument| CString::new(argument.to_vec()).expect("argv"))
        .collect();
        let environment = std::env::vars_os()
            .filter(|(key, _)| key != ROLE && key != TRUE_PROGRAM)
            .map(|(key, value)| {
                let mut entry = key.into_vec();
                entry.push(b'=');
                entry.extend(value.into_vec());
                CString::new(entry).expect("environment")
            })
            .chain([CString::new(
                [
                    TRUE_PROGRAM.as_bytes(),
                    b"=",
                    true_program.as_os_str().as_bytes(),
                ]
                .concat(),
            )
            .expect("environment")])
            .collect();
        Self {
            executable: CString::new(executable.into_os_string().into_vec()).expect("path"),
            argv,
            environment,
            harness: std::process::id(),
        }
    }

    fn run(&self, mode: Mode, burst: u32) -> Outcome {
        let netlink = match mode {
            Mode::CnProc => match Netlink::listen(None) {
                Ok(netlink) => Some(netlink),
                Err(unavailable) => return unavailable,
            },
            _ => None,
        };
        let root = match self.spawn(burst, mode == Mode::Ptrace) {
            Ok(root) => root,
            Err(unavailable) => return unavailable,
        };
        let workload_before = rusage(libc::RUSAGE_CHILDREN);
        let measured = match mode {
            Mode::Baseline => root.run_unobserved(),
            Mode::Census => root.run_with_census(),
            Mode::CnProc => root.run_with_netlink(netlink.expect("listening"), self.harness),
            Mode::Ptrace => match root.run_traced() {
                Ok(measured) => measured,
                Err(unavailable) => return unavailable,
            },
        };
        let workload = rusage(libc::RUSAGE_CHILDREN).minus(workload_before);
        let oracle = root.oracle();
        let root_exec = self
            .argv
            .iter()
            .map(|argument| String::from_utf8_lossy(argument.as_bytes()).into_owned())
            .collect();
        Outcome::Measured(Box::new(score(
            measured,
            workload,
            &oracle,
            root.identity,
            root_exec,
        )))
    }

    /// Fork the root, held on its gate; `traced` makes it ask this thread to trace it.
    fn spawn(&self, burst: u32, traced: bool) -> Result<Root, Outcome> {
        let gate = Pipe::new();
        let done = Pipe::new();
        let hold = Pipe::new();
        let error = Pipe::new();
        let oracle = OracleFile::new();
        let null = high(
            fs::File::options()
                .write(true)
                .open("/dev/null")
                .expect("/dev/null")
                .into(),
        );
        let mut environment = self.environment.clone();
        environment.push(CString::new(format!("{ROLE}={burst}")).expect("environment"));
        let argv: Vec<*const c_char> = self
            .argv
            .iter()
            .map(|argument| argument.as_ptr())
            .chain([ptr::null()])
            .collect();
        let envp: Vec<*const c_char> = environment
            .iter()
            .map(|entry| entry.as_ptr())
            .chain([ptr::null()])
            .collect();
        let moves = [
            (gate.read.as_raw_fd(), GATE_FD),
            (oracle.fd.as_raw_fd(), ORACLE_FD),
            (done.write.as_raw_fd(), DONE_FD),
            (hold.read.as_raw_fd(), HOLD_FD),
            (null.as_raw_fd(), libc::STDOUT_FILENO),
        ];
        let error_write = error.write.as_raw_fd();
        // SAFETY: after fork the child calls only async-signal-safe functions on memory
        // prepared before the fork. Every source fd is at or above 100, so no move clobbers a
        // later source.
        let pid = unsafe { libc::fork() };
        if pid == 0 {
            unsafe {
                if traced {
                    if libc::ptrace(
                        libc::PTRACE_TRACEME,
                        0,
                        ptr::null_mut::<c_void>(),
                        ptr::null_mut::<c_void>(),
                    ) == -1
                    {
                        let errno = *libc::__errno_location();
                        libc::write(
                            error_write,
                            ptr::from_ref(&errno).cast(),
                            size_of::<c_int>(),
                        );
                        libc::_exit(TRACEME_FAILED);
                    }
                    libc::raise(libc::SIGSTOP);
                }
                for (from, to) in moves {
                    if libc::dup2(from, to) != to {
                        libc::_exit(126);
                    }
                }
                libc::execve(self.executable.as_ptr(), argv.as_ptr(), envp.as_ptr());
                libc::_exit(127);
            }
        }
        assert!(pid > 0, "fork: {}", io::Error::last_os_error());
        drop(error.write);
        if traced {
            let mut status = 0;
            // SAFETY: waiting for our own child.
            let waited = unsafe { libc::waitpid(pid, &mut status, 0) };
            assert_eq!(waited, pid, "waitpid: {}", io::Error::last_os_error());
            if libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == TRACEME_FAILED {
                let mut errno = [0_u8; size_of::<c_int>()];
                let read = fs::File::from(error.read).read_exact_or_short(&mut errno);
                return Err(Outcome::Unavailable {
                    call: "ptrace(PTRACE_TRACEME)".to_owned(),
                    errno: read.then(|| c_int::from_ne_bytes(errno)),
                    detail: "the root could not ask to be traced".to_owned(),
                });
            }
            if !(libc::WIFSTOPPED(status) && libc::WSTOPSIG(status) == libc::SIGSTOP) {
                return Err(Outcome::Unavailable {
                    call: "ptrace(PTRACE_TRACEME)".to_owned(),
                    errno: None,
                    detail: format!("the root ended before its first stop: wait status {status}"),
                });
            }
        }
        let identity = Identity {
            pid: u32::try_from(pid).expect("pid"),
            start: start_time(pid).expect("the gated root's start time"),
        };
        Ok(Root {
            pid,
            identity,
            gate: gate.write,
            done: done.read,
            hold: hold.write,
            oracle,
        })
    }

    /// Fill a minimal receive buffer while the reader is held, then drain it.
    fn cn_proc_overflow(&self) -> Overflow {
        let netlink = match Netlink::listen(Some(0)) {
            Ok(netlink) => netlink,
            Err(Outcome::Unavailable {
                call,
                errno,
                detail,
            }) => {
                return Overflow {
                    outcome: Err(format!("{call}: errno {errno:?}: {detail}")),
                };
            }
            Err(Outcome::Measured(_)) => unreachable!("listen measures nothing"),
        };
        let root = match self.spawn(COVERAGE_BURST, false) {
            Ok(root) => root,
            Err(_) => unreachable!("an untraced spawn is never unavailable"),
        };
        write_byte(root.hold.as_raw_fd()).expect("hold");
        write_byte(root.gate.as_raw_fd()).expect("gate");
        let status = root.wait();
        assert_eq!(status, 0, "root wait status");
        let mut events = Vec::new();
        let mut losses = Vec::new();
        let mut sequence = Sequences::default();
        let enobufs = netlink.drain(&mut events, &mut losses, &mut sequence);
        Overflow {
            outcome: Ok(OverflowCounts {
                receive_buffer_bytes: netlink.receive_buffer(),
                events: events.len(),
                enobufs,
                sequence_gaps: sequence.gaps,
            }),
        }
    }
}

trait ReadShort {
    fn read_exact_or_short(self, buffer: &mut [u8]) -> bool;
}

impl ReadShort for fs::File {
    fn read_exact_or_short(mut self, buffer: &mut [u8]) -> bool {
        io::Read::read_exact(&mut self, buffer).is_ok()
    }
}

struct Root {
    pid: libc::pid_t,
    identity: Identity,
    gate: OwnedFd,
    done: OwnedFd,
    hold: OwnedFd,
    oracle: OracleFile,
}

/// What a mode saw while the burst ran.
struct Sight {
    wall_ns: u64,
    observer: Usage,
    observed: Observed,
    losses: Vec<String>,
}

impl Root {
    fn release(&self) -> Instant {
        let released = Instant::now();
        write_byte(self.gate.as_raw_fd()).expect("gate");
        released
    }

    fn await_done(&self, released: Instant) -> u64 {
        read_byte(self.done.as_raw_fd()).expect("done");
        u64::try_from(released.elapsed().as_nanos()).expect("wall")
    }

    fn wait(&self) -> c_int {
        let mut status = 0;
        // SAFETY: waiting for our own child.
        let waited = unsafe { libc::waitpid(self.pid, &mut status, 0) };
        assert_eq!(waited, self.pid, "waitpid: {}", io::Error::last_os_error());
        status
    }

    fn run_unobserved(&self) -> Sight {
        write_byte(self.hold.as_raw_fd()).expect("hold");
        let released = self.release();
        let wall_ns = self.await_done(released);
        assert_eq!(self.wait(), 0, "root wait status");
        Sight {
            wall_ns,
            observer: Usage::default(),
            observed: Observed::default(),
            losses: Vec::new(),
        }
    }

    /// Read the tree before the gate opens and once the burst is done, while the root is held:
    /// what a poll once a second sees of work shorter than a second.
    fn run_with_census(&self) -> Sight {
        let before = rusage(libc::RUSAGE_THREAD);
        let mut observed = Observed::default();
        let mut seen = HashSet::new();
        census(self.pid, &mut observed, &mut seen);
        let released = self.release();
        let wall_ns = self.await_done(released);
        census(self.pid, &mut observed, &mut seen);
        let observer = rusage(libc::RUSAGE_THREAD).minus(before);
        write_byte(self.hold.as_raw_fd()).expect("hold");
        assert_eq!(self.wait(), 0, "root wait status");
        Sight {
            wall_ns,
            observer,
            observed,
            losses: Vec::new(),
        }
    }

    fn run_with_netlink(&self, netlink: Netlink, harness: u32) -> Sight {
        write_byte(self.hold.as_raw_fd()).expect("hold");
        let stop = Pipe::new();
        let stop_read = stop.read;
        let reader = std::thread::spawn(move || {
            let before = rusage(libc::RUSAGE_THREAD);
            let mut events = Vec::new();
            let mut losses = Vec::new();
            let mut sequence = Sequences::default();
            let mut tree = TreeFilter::new(harness);
            let mut observed = Observed::default();
            loop {
                let mut fds = [
                    libc::pollfd {
                        fd: netlink.fd.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                    libc::pollfd {
                        fd: stop_read.as_raw_fd(),
                        events: libc::POLLIN,
                        revents: 0,
                    },
                ];
                // SAFETY: two pollfds on the stack; no deadline: the stop pipe ends the wait.
                let ready = unsafe { libc::poll(fds.as_mut_ptr(), 2, -1) };
                if ready < 0 {
                    let error = io::Error::last_os_error();
                    assert_eq!(error.kind(), io::ErrorKind::Interrupted, "poll: {error}");
                    continue;
                }
                netlink.drain(&mut events, &mut losses, &mut sequence);
                for event in events.drain(..) {
                    tree.observe(event, &mut observed);
                }
                if fds[1].revents != 0 {
                    netlink.drain(&mut events, &mut losses, &mut sequence);
                    for event in events.drain(..) {
                        tree.observe(event, &mut observed);
                    }
                    break;
                }
            }
            losses.extend(sequence.gaps);
            (observed, losses, rusage(libc::RUSAGE_THREAD).minus(before))
        });
        let released = self.release();
        let wall_ns = self.await_done(released);
        assert_eq!(self.wait(), 0, "root wait status");
        write_byte(stop.write.as_raw_fd()).expect("stop");
        let (observed, losses, observer) = reader.join().expect("netlink reader");
        Sight {
            wall_ns,
            observer,
            observed,
            losses,
        }
    }

    /// Trace the root and every descendant from this thread, which forked the root.
    fn run_traced(&self) -> Result<Sight, Outcome> {
        let options = libc::PTRACE_O_TRACEFORK
            | libc::PTRACE_O_TRACEVFORK
            | libc::PTRACE_O_TRACECLONE
            | libc::PTRACE_O_TRACEEXEC
            | libc::PTRACE_O_TRACEEXIT
            | libc::PTRACE_O_EXITKILL;
        let options = usize::try_from(options).expect("options");
        // SAFETY: the root is stopped and traced by this thread.
        if unsafe {
            libc::ptrace(
                libc::PTRACE_SETOPTIONS,
                self.pid,
                ptr::null_mut::<c_void>(),
                ptr::without_provenance_mut::<c_void>(options),
            )
        } == -1
        {
            let error = io::Error::last_os_error();
            // SAFETY: killing our own stopped child.
            unsafe { libc::kill(self.pid, libc::SIGKILL) };
            self.wait();
            return Err(Outcome::Unavailable {
                call: "ptrace(PTRACE_SETOPTIONS)".to_owned(),
                errno: error.raw_os_error(),
                detail: error.to_string(),
            });
        }
        write_byte(self.hold.as_raw_fd()).expect("hold");
        let before = rusage(libc::RUSAGE_THREAD);
        resume(self.pid, 0);
        let released = self.release();
        let mut observed = Observed::default();
        let mut started = HashSet::from([self.pid]);
        // The tracer must stay on this thread; another reads the root's DONE byte, so the wall
        // time ends at the same boundary as in every other mode.
        let wall_ns = std::thread::scope(|scope| {
            let done = scope.spawn(|| self.await_done(released));
            loop {
                let mut status = 0;
                // SAFETY: waiting for any tracee.
                let pid = unsafe { libc::waitpid(-1, &mut status, libc::__WALL) };
                if pid == -1 {
                    let error = io::Error::last_os_error();
                    match error.raw_os_error() {
                        Some(libc::ECHILD) => break,
                        Some(libc::EINTR) => continue,
                        _ => panic!("waitpid: {error}"),
                    }
                }
                if !libc::WIFSTOPPED(status) {
                    if pid == self.pid {
                        assert!(
                            libc::WIFEXITED(status) && libc::WEXITSTATUS(status) == 0,
                            "root wait status {status}"
                        );
                    }
                    continue;
                }
                let signal = libc::WSTOPSIG(status);
                let event = status >> 16;
                let tracee = u32::try_from(pid).expect("pid");
                match event {
                    libc::PTRACE_EVENT_FORK
                    | libc::PTRACE_EVENT_VFORK
                    | libc::PTRACE_EVENT_CLONE => {
                        let child = event_message(pid);
                        let child = libc::pid_t::try_from(child).expect("child pid");
                        observed.births.push(Birth {
                            pid: u32::try_from(child).expect("pid"),
                            ppid: thread_group(pid),
                            start: start_time(child).ok_or_else(|| "unreadable".to_owned()),
                            pidfd: open_pidfd(child),
                        });
                        resume(pid, 0);
                    }
                    libc::PTRACE_EVENT_EXEC => {
                        observed.execs.push(Exec {
                            pid: tracee,
                            start: start_time(pid).ok_or_else(|| "unreadable".to_owned()),
                            argv: command_line(pid),
                        });
                        resume(pid, 0);
                    }
                    libc::PTRACE_EVENT_EXIT => {
                        let wait_status = event_message(pid);
                        observed.exits.push(Exit {
                            pid: tracee,
                            start: start_time(pid).ok_or_else(|| "unreadable".to_owned()),
                            wait_status: i32::try_from(wait_status).ok(),
                        });
                        resume(pid, 0);
                    }
                    _ if signal == libc::SIGSTOP && started.insert(pid) => resume(pid, 0),
                    _ => resume(pid, signal),
                }
            }
            done.join().expect("done reader")
        });
        let observer = rusage(libc::RUSAGE_THREAD).minus(before);
        Ok(Sight {
            wall_ns,
            observer,
            observed,
            losses: Vec::new(),
        })
    }

    fn oracle(&self) -> Vec<OracleRecord> {
        let bytes = fs::read(&self.oracle.path).expect("oracle");
        let (records, torn) = bytes.as_chunks::<RECORD_BYTES>();
        assert!(torn.is_empty(), "torn oracle record");
        records
            .iter()
            .map(|record| OracleRecord {
                identity: Identity {
                    pid: u32::from_ne_bytes(record[0..4].try_into().expect("pid")),
                    start: u64::from_ne_bytes(record[8..16].try_into().expect("start")),
                },
                ppid: u32::from_ne_bytes(record[4..8].try_into().expect("ppid")),
                wait_status: i32::from_ne_bytes(record[16..20].try_into().expect("status")),
                exec_index: i32::from_ne_bytes(record[20..24].try_into().expect("index")),
            })
            .collect()
    }
}

struct OracleRecord {
    identity: Identity,
    ppid: u32,
    wait_status: i32,
    exec_index: i32,
}

fn score(
    sight: Sight,
    workload: Usage,
    oracle: &[OracleRecord],
    root: Identity,
    root_exec: Vec<String>,
) -> Measurement {
    assert!(
        oracle.iter().all(|record| record.wait_status == 0),
        "every fixture process exits 0"
    );
    let mut coverage = Coverage::default();
    let observed = sight.observed;

    let births: HashMap<Identity, u32> = observed
        .births
        .iter()
        .filter_map(|birth| {
            let start = *birth.start.as_ref().ok()?;
            Some((
                Identity {
                    pid: birth.pid,
                    start,
                },
                birth.ppid,
            ))
        })
        .collect();
    for record in oracle {
        match births.get(&record.identity) {
            Some(&ppid) if ppid == record.ppid => coverage.births_matched += 1,
            Some(&ppid) => coverage.births_wrong_parent.push(format!(
                "{:?}: observed parent {ppid}, expected {}",
                record.identity, record.ppid
            )),
            None => coverage.births_missing.push(record.identity),
        }
    }

    let mut expected_execs: BTreeMap<Identity, Vec<String>> = oracle
        .iter()
        .filter(|record| record.exec_index >= 0)
        .map(|record| {
            (
                record.identity,
                vec![
                    "true".to_owned(),
                    "proc-source".to_owned(),
                    record.exec_index.to_string(),
                ],
            )
        })
        .collect();
    expected_execs.insert(root, root_exec);
    let execs: HashMap<Identity, &Result<Vec<String>, String>> = observed
        .execs
        .iter()
        .filter_map(|exec| {
            Some((
                Identity {
                    pid: exec.pid,
                    start: *exec.start.as_ref().ok()?,
                },
                &exec.argv,
            ))
        })
        .collect();
    for (identity, argv) in &expected_execs {
        match execs.get(identity) {
            Some(Ok(observed)) if observed == argv => coverage.execs_matched += 1,
            Some(observed) => coverage.execs_mismatched.push(format!(
                "{identity:?}: observed {observed:?}, expected {argv:?}"
            )),
            None => coverage.execs_missing.push(*identity),
        }
    }

    let exits: HashMap<Identity, Option<i32>> = observed
        .exits
        .iter()
        .filter_map(|exit| {
            Some((
                Identity {
                    pid: exit.pid,
                    start: *exit.start.as_ref().ok()?,
                },
                exit.wait_status,
            ))
        })
        .collect();
    for (identity, status) in oracle
        .iter()
        .map(|record| (record.identity, record.wait_status))
        .chain([(root, 0)])
    {
        match exits.get(&identity) {
            Some(Some(observed)) if *observed == status => coverage.exits_matched += 1,
            Some(observed) => coverage.exits_mismatched.push(format!(
                "{identity:?}: observed {observed:?}, expected {status}"
            )),
            None => coverage.exits_missing.push(identity),
        }
    }

    Measurement {
        wall_ns: sight.wall_ns,
        observer_user_us: sight.observer.user_us,
        observer_sys_us: sight.observer.sys_us,
        workload_user_us: workload.user_us,
        workload_sys_us: workload.sys_us,
        expected: Expected {
            births: oracle.len(),
            execs: expected_execs.len(),
            exits: oracle.len() + 1,
        },
        coverage,
        observed: Observed {
            births: observed
                .births
                .into_iter()
                .filter(|birth| birth.start.is_err() || birth.pidfd.is_err())
                .collect(),
            execs: observed
                .execs
                .into_iter()
                .filter(|exec| exec.start.is_err() || exec.argv.is_err())
                .collect(),
            exits: observed
                .exits
                .into_iter()
                .filter(|exit| exit.start.is_err() || exit.wait_status.is_none())
                .collect(),
        },
        losses: sight.losses,
    }
}

/// Every process now in the tree under `root`, read through each of its threads'
/// `/proc/<pid>/task/<tid>/children`: a child belongs to the thread that forked it.
fn census(root: libc::pid_t, observed: &mut Observed, seen: &mut HashSet<Identity>) {
    let mut parents = vec![root];
    while let Some(parent) = parents.pop() {
        let Ok(threads) = fs::read_dir(format!("/proc/{parent}/task")) else {
            continue;
        };
        let children: Vec<String> = threads
            .filter_map(|thread| fs::read_to_string(thread.ok()?.path().join("children")).ok())
            .collect();
        for child in children
            .iter()
            .flat_map(|children| children.split_whitespace())
        {
            let child: libc::pid_t = child.parse().expect("child pid");
            let start = start_time(child).ok_or_else(|| "unreadable".to_owned());
            let pid = u32::try_from(child).expect("pid");
            parents.push(child);
            if let Ok(start) = start
                && !seen.insert(Identity { pid, start })
            {
                continue;
            }
            observed.births.push(Birth {
                pid,
                ppid: u32::try_from(parent).expect("pid"),
                start: start.clone(),
                pidfd: open_pidfd(child),
            });
            observed.execs.push(Exec {
                pid,
                start,
                argv: command_line(child),
            });
        }
    }
}

/// The process a thread belongs to: a ptrace event names the thread that forked.
fn thread_group(tid: libc::pid_t) -> u32 {
    fs::read_to_string(format!("/proc/{tid}/status"))
        .expect("a stopped tracee's status")
        .lines()
        .find_map(|line| line.strip_prefix("Tgid:"))
        .and_then(|tgid| tgid.trim().parse().ok())
        .expect("Tgid")
}

fn command_line(pid: libc::pid_t) -> Result<Vec<String>, String> {
    let bytes = fs::read(format!("/proc/{pid}/cmdline")).map_err(|error| error.to_string())?;
    if bytes.is_empty() {
        return Err("empty: the process exited".to_owned());
    }
    Ok(bytes
        .strip_suffix(b"\0")
        .unwrap_or(&bytes)
        .split(|&byte| byte == 0)
        .map(|argument| String::from_utf8_lossy(argument).into_owned())
        .collect())
}

fn open_pidfd(pid: libc::pid_t) -> Result<(), String> {
    // SAFETY: pidfd_open with no flags.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    match RawFd::try_from(fd) {
        Ok(fd) if fd >= 0 => {
            // SAFETY: the syscall returned a new fd we own.
            drop(unsafe { OwnedFd::from_raw_fd(fd) });
            Ok(())
        }
        _ => Err(io::Error::last_os_error().to_string()),
    }
}

fn event_message(pid: libc::pid_t) -> libc::c_ulong {
    let mut message: libc::c_ulong = 0;
    // SAFETY: the tracee is stopped at a ptrace event.
    let result = unsafe {
        libc::ptrace(
            libc::PTRACE_GETEVENTMSG,
            pid,
            ptr::null_mut::<c_void>(),
            ptr::from_mut(&mut message).cast::<c_void>(),
        )
    };
    assert_ne!(
        result,
        -1,
        "PTRACE_GETEVENTMSG: {}",
        io::Error::last_os_error()
    );
    message
}

fn resume(pid: libc::pid_t, signal: c_int) {
    let signal = usize::try_from(signal).expect("signal");
    // SAFETY: the tracee is stopped.
    let result = unsafe {
        libc::ptrace(
            libc::PTRACE_CONT,
            pid,
            ptr::null_mut::<c_void>(),
            ptr::without_provenance_mut::<c_void>(signal),
        )
    };
    // A tracee killed while stopped is gone; its exit still reaches waitpid.
    if result == -1 {
        let error = io::Error::last_os_error();
        assert_eq!(
            error.raw_os_error(),
            Some(libc::ESRCH),
            "PTRACE_CONT: {error}"
        );
    }
}

#[derive(Clone, Copy, Default)]
struct Usage {
    user_us: u64,
    sys_us: u64,
}

impl Usage {
    fn minus(self, before: Self) -> Self {
        Self {
            user_us: self.user_us - before.user_us,
            sys_us: self.sys_us - before.sys_us,
        }
    }
}

fn rusage(who: c_int) -> Usage {
    // SAFETY: getrusage into a zeroed struct.
    let usage = unsafe {
        let mut usage: libc::rusage = std::mem::zeroed();
        assert_eq!(libc::getrusage(who, &mut usage), 0, "getrusage");
        usage
    };
    let micros = |time: libc::timeval| {
        u64::try_from(time.tv_sec).expect("seconds") * 1_000_000
            + u64::try_from(time.tv_usec).expect("micros")
    };
    Usage {
        user_us: micros(usage.ru_utime),
        sys_us: micros(usage.ru_stime),
    }
}

struct Pipe {
    read: OwnedFd,
    write: OwnedFd,
}

impl Pipe {
    fn new() -> Self {
        let mut fds = [0; 2];
        // SAFETY: pipe2 into a two-element array.
        assert_eq!(unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) }, 0);
        // SAFETY: pipe2 returned two fds we now own.
        let (read, write) = unsafe { (OwnedFd::from_raw_fd(fds[0]), OwnedFd::from_raw_fd(fds[1])) };
        Self {
            read: high(read),
            write: high(write),
        }
    }
}

/// The same open file at an fd of at least 100, close-on-exec.
fn high(fd: OwnedFd) -> OwnedFd {
    // SAFETY: duplicating an fd we own.
    let raised = unsafe { libc::fcntl(fd.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 100) };
    assert!(
        raised >= 100,
        "F_DUPFD_CLOEXEC: {}",
        io::Error::last_os_error()
    );
    // SAFETY: fcntl returned a new fd we own.
    unsafe { OwnedFd::from_raw_fd(raised) }
}

struct OracleFile {
    path: PathBuf,
    fd: OwnedFd,
}

impl OracleFile {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "cowshed-process-source-{}-{}",
            std::process::id(),
            Instant::now().elapsed().as_nanos() ^ u128::from(next_oracle())
        ));
        let file = fs::File::options()
            .create_new(true)
            .append(true)
            .open(&path)
            .expect("oracle file");
        Self {
            fd: high(file.into()),
            path,
        }
    }
}

impl Drop for OracleFile {
    fn drop(&mut self) {
        fs::remove_file(&self.path).expect("remove oracle file");
    }
}

fn next_oracle() -> u64 {
    static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
}

fn find_program(name: &str) -> PathBuf {
    std::env::split_paths(&std::env::var_os("PATH").expect("PATH"))
        .map(|directory| directory.join(name))
        .find(|candidate| candidate.is_file())
        .unwrap_or_else(|| panic!("{name} on PATH"))
}

fn namespace_facts() -> NamespaceFacts {
    let read = |path: &str| {
        fs::read_to_string(path)
            .map(|text| text.split_whitespace().collect::<Vec<_>>().join(" "))
            .map_err(|error| error.to_string())
    };
    let status_line = |key: &str| {
        fs::read_to_string("/proc/self/status")
            .map_err(|error| error.to_string())
            .and_then(|status| {
                status
                    .lines()
                    .find_map(|line| line.strip_prefix(key))
                    .map(|value| value.trim().to_owned())
                    .ok_or_else(|| format!("no {key} line"))
            })
    };
    NamespaceFacts {
        uid_map: read("/proc/self/uid_map"),
        ns_pid: status_line("NSpid:"),
        yama_ptrace_scope: read("/proc/sys/kernel/yama/ptrace_scope"),
        seccomp: status_line("Seccomp:"),
        cap_eff: status_line("CapEff:"),
    }
}

// ---------------------------------------------------------------------------------------------
// Proc connector.

struct Netlink {
    fd: OwnedFd,
}

/// One decoded proc connector event.
enum ProcEvent {
    Fork { parent: u32, child: u32 },
    Exec { pid: u32 },
    Exit { pid: u32, wait_status: u32 },
}

#[derive(Default)]
struct Sequences {
    last: HashMap<u32, u32>,
    gaps: Vec<String>,
}

impl Sequences {
    fn observe(&mut self, cpu: u32, sequence: u32) {
        if let Some(previous) = self.last.insert(cpu, sequence)
            && previous.wrapping_add(1) != sequence
        {
            self.gaps
                .push(format!("cpu {cpu}: sequence {previous} then {sequence}"));
        }
    }
}

impl Netlink {
    /// Subscribe to every proc event; `receive_buffer` shrinks the socket's buffer.
    fn listen(receive_buffer: Option<c_int>) -> Result<Self, Outcome> {
        let unavailable = |call: &str, error: io::Error| Outcome::Unavailable {
            call: call.to_owned(),
            errno: error.raw_os_error(),
            detail: error.to_string(),
        };
        // SAFETY: socket(2).
        let fd = unsafe {
            libc::socket(
                libc::AF_NETLINK,
                libc::SOCK_DGRAM | libc::SOCK_CLOEXEC,
                libc::NETLINK_CONNECTOR,
            )
        };
        if fd < 0 {
            return Err(unavailable(
                "socket(AF_NETLINK, NETLINK_CONNECTOR)",
                io::Error::last_os_error(),
            ));
        }
        // SAFETY: socket returned a new fd we own.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        if let Some(bytes) = receive_buffer {
            // SAFETY: setsockopt with an int.
            let set = unsafe {
                libc::setsockopt(
                    fd.as_raw_fd(),
                    libc::SOL_SOCKET,
                    libc::SO_RCVBUF,
                    ptr::from_ref(&bytes).cast(),
                    libc::socklen_t::try_from(size_of::<c_int>()).expect("socklen"),
                )
            };
            assert_eq!(set, 0, "SO_RCVBUF: {}", io::Error::last_os_error());
        }
        // SAFETY: a zeroed sockaddr_nl is valid; its fields are set below.
        let mut address: libc::sockaddr_nl = unsafe { std::mem::zeroed() };
        address.nl_family = libc::sa_family_t::try_from(libc::AF_NETLINK).expect("family");
        address.nl_groups = CN_IDX_PROC;
        // SAFETY: bind with a sockaddr_nl.
        if unsafe {
            libc::bind(
                fd.as_raw_fd(),
                ptr::from_ref(&address).cast(),
                libc::socklen_t::try_from(size_of::<libc::sockaddr_nl>()).expect("socklen"),
            )
        } != 0
        {
            return Err(unavailable("bind(CN_IDX_PROC)", io::Error::last_os_error()));
        }
        let mut message = Vec::with_capacity(NLMSG_HEADER + CN_MSG_HEADER + 4);
        let length = u32::try_from(NLMSG_HEADER + CN_MSG_HEADER + 4).expect("length");
        message.extend(length.to_ne_bytes());
        message.extend(u16::try_from(libc::NLMSG_DONE).expect("type").to_ne_bytes());
        message.extend(0_u16.to_ne_bytes());
        message.extend(0_u32.to_ne_bytes());
        message.extend(std::process::id().to_ne_bytes());
        message.extend(CN_IDX_PROC.to_ne_bytes());
        message.extend(CN_VAL_PROC.to_ne_bytes());
        message.extend(0_u32.to_ne_bytes());
        // The acknowledgement answers with this plus one, which tells it from another
        // listener's.
        message.extend(LISTEN_ACK.to_ne_bytes());
        message.extend(4_u16.to_ne_bytes());
        message.extend(0_u16.to_ne_bytes());
        message.extend(PROC_CN_MCAST_LISTEN.to_ne_bytes());
        // SAFETY: send from a byte buffer.
        let sent = unsafe { libc::send(fd.as_raw_fd(), message.as_ptr().cast(), message.len(), 0) };
        if usize::try_from(sent).ok() != Some(message.len()) {
            return Err(unavailable(
                "send(PROC_CN_MCAST_LISTEN)",
                io::Error::last_os_error(),
            ));
        }
        // The connector runs the listen request in the sender's send(2), and multicasts its
        // acknowledgement to this socket's group before send returns: an acknowledgement not
        // queued now never arrives. The kernel sends none for a listener outside the initial
        // user and PID namespaces.
        let netlink = Self { fd };
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            // SAFETY: recv into a byte buffer, without blocking.
            let read = unsafe {
                libc::recv(
                    netlink.fd.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                    libc::MSG_DONTWAIT,
                )
            };
            let Ok(read) = usize::try_from(read) else {
                let error = io::Error::last_os_error();
                if error.kind() == io::ErrorKind::WouldBlock {
                    return Err(Outcome::Unavailable {
                        call: "send(PROC_CN_MCAST_LISTEN)".to_owned(),
                        errno: None,
                        detail: "no acknowledgement was queued by the listen request".to_owned(),
                    });
                }
                return Err(unavailable("recv(acknowledgement)", error));
            };
            for (what, _, _, ack, data) in messages(&buffer[..read]) {
                if what == PROC_EVENT_NONE && ack == LISTEN_ACK.wrapping_add(1) {
                    let error = u32::from_ne_bytes(data[0..4].try_into().expect("err"));
                    if error != 0 {
                        return Err(Outcome::Unavailable {
                            call: "send(PROC_CN_MCAST_LISTEN)".to_owned(),
                            errno: i32::try_from(error).ok(),
                            detail: "the acknowledgement carries an error".to_owned(),
                        });
                    }
                    return Ok(netlink);
                }
            }
        }
    }

    fn receive_buffer(&self) -> c_int {
        let mut bytes: c_int = 0;
        let mut length = libc::socklen_t::try_from(size_of::<c_int>()).expect("socklen");
        // SAFETY: getsockopt into an int.
        let got = unsafe {
            libc::getsockopt(
                self.fd.as_raw_fd(),
                libc::SOL_SOCKET,
                libc::SO_RCVBUF,
                ptr::from_mut(&mut bytes).cast(),
                &mut length,
            )
        };
        assert_eq!(got, 0, "SO_RCVBUF: {}", io::Error::last_os_error());
        bytes
    }

    /// Read every queued event without blocking; returns the number of overflow reports.
    fn drain(
        &self,
        events: &mut Vec<(u32, ProcEvent)>,
        losses: &mut Vec<String>,
        sequence: &mut Sequences,
    ) -> usize {
        let mut overflows = 0;
        let mut buffer = vec![0_u8; 64 * 1024];
        loop {
            // SAFETY: recv into a byte buffer, without blocking.
            let read = unsafe {
                libc::recv(
                    self.fd.as_raw_fd(),
                    buffer.as_mut_ptr().cast(),
                    buffer.len(),
                    libc::MSG_DONTWAIT,
                )
            };
            let Ok(read) = usize::try_from(read) else {
                let error = io::Error::last_os_error();
                match error.raw_os_error() {
                    Some(libc::EAGAIN) => return overflows,
                    Some(libc::EINTR) => continue,
                    Some(libc::ENOBUFS) => {
                        overflows += 1;
                        losses.push("ENOBUFS: the receive buffer overflowed".to_owned());
                        continue;
                    }
                    _ => panic!("recv: {error}"),
                }
            };
            for (what, cpu, seq, _, data) in messages(&buffer[..read]) {
                sequence.observe(cpu, seq);
                let word = |index: usize| {
                    u32::from_ne_bytes(data[index * 4..index * 4 + 4].try_into().expect("word"))
                };
                let event = match what {
                    PROC_EVENT_FORK if word(2) == word(3) => ProcEvent::Fork {
                        parent: word(1),
                        child: word(3),
                    },
                    PROC_EVENT_EXEC if word(0) == word(1) => ProcEvent::Exec { pid: word(1) },
                    PROC_EVENT_EXIT if word(0) == word(1) => ProcEvent::Exit {
                        pid: word(1),
                        wait_status: word(2),
                    },
                    _ => continue,
                };
                events.push((cpu, event));
            }
        }
    }
}

/// Each netlink message in a datagram: (`what`, `cpu`, cn_msg `seq` and `ack`, event data).
fn messages(datagram: &[u8]) -> impl Iterator<Item = (u32, u32, u32, u32, &[u8])> {
    let mut offset = 0;
    std::iter::from_fn(move || {
        let header = datagram.get(offset..offset + NLMSG_HEADER)?;
        let length = usize::try_from(u32::from_ne_bytes(header[0..4].try_into().ok()?)).ok()?;
        let message = datagram.get(offset..offset + length)?;
        offset += length.next_multiple_of(4).max(NLMSG_HEADER);
        let connector = message.get(NLMSG_HEADER..NLMSG_HEADER + CN_MSG_HEADER)?;
        let seq = u32::from_ne_bytes(connector[8..12].try_into().ok()?);
        let ack = u32::from_ne_bytes(connector[12..16].try_into().ok()?);
        let event = message.get(NLMSG_HEADER + CN_MSG_HEADER..)?;
        let what = u32::from_ne_bytes(event.get(0..4)?.try_into().ok()?);
        let cpu = u32::from_ne_bytes(event.get(4..8)?.try_into().ok()?);
        Some((what, cpu, seq, ack, event.get(16..)?))
    })
}

/// Keeps the events of processes descended from the harness's children, reading each one's
/// identity and argv when its event is handled -- after the fact, as a listener must.
struct TreeFilter {
    harness: u32,
    tree: HashSet<u32>,
}

impl TreeFilter {
    fn new(harness: u32) -> Self {
        Self {
            harness,
            tree: HashSet::new(),
        }
    }

    fn observe(&mut self, (_, event): (u32, ProcEvent), observed: &mut Observed) {
        match event {
            ProcEvent::Fork { parent, child } => {
                let root = parent == self.harness;
                if !root && !self.tree.contains(&parent) {
                    return;
                }
                self.tree.insert(child);
                if root {
                    return;
                }
                let pid = libc::pid_t::try_from(child).expect("pid");
                observed.births.push(Birth {
                    pid: child,
                    ppid: parent,
                    start: start_time(pid).ok_or_else(|| "unreadable".to_owned()),
                    pidfd: open_pidfd(pid),
                });
            }
            ProcEvent::Exec { pid } if self.tree.contains(&pid) => {
                let process = libc::pid_t::try_from(pid).expect("pid");
                observed.execs.push(Exec {
                    pid,
                    start: start_time(process).ok_or_else(|| "unreadable".to_owned()),
                    argv: command_line(process),
                });
            }
            ProcEvent::Exit { pid, wait_status } if self.tree.contains(&pid) => {
                let process = libc::pid_t::try_from(pid).expect("pid");
                observed.exits.push(Exit {
                    pid,
                    start: start_time(process).ok_or_else(|| "unreadable".to_owned()),
                    wait_status: i32::try_from(wait_status).ok(),
                });
            }
            ProcEvent::Exec { .. } | ProcEvent::Exit { .. } => {}
        }
    }
}
