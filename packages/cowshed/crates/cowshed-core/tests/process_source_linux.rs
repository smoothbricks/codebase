//! Measures ptrace fork/exec/exit tracing, the Linux process event source a supervisor can run
//! as the job's parent (07_api.md, "Complete job accounting and observation reconciliation"),
//! against an unobserved baseline and a census that reads the tree before and after a burst, as
//! a one-second poll would. The proc connector is not run here: no crate this workspace locks
//! carries its UAPI records, and a probe built against the kernel's own `cn_proc.h` found its
//! listen request refused (`ECONNREFUSED`) on the measured runner, so no workload ran under it.
//!
//! One workload runs unchanged under every mode. The harness re-executes this test binary as the
//! job's root, held on a gate pipe until the observer is ready. The root starts children one
//! after another, each this binary again in the child role; each child starts a grandchild that
//! execs `true proc-source <i>`. Every parent records each process it reaps -- pid, parent,
//! kernel start time read while it is an unreaped zombie, wait status and the argv it was given
//! -- as a JSON line in an oracle file, independently of any observer.
//!
//! A mode the kernel or sandbox refuses is reported with the refused call and its errno, never
//! as an observation with zero events. The report is printed and, when
//! `COWSHED_PROCESS_SOURCE_REPORT` names a file, written there.
//!
//! `PTRACE_O_EXITKILL` belongs to this fixture: its tracees are its own. It says nothing about
//! what a production observer may do to a job when it is lost.

#![cfg(target_os = "linux")]

use std::collections::{HashMap, HashSet};
use std::ffi::OsString;
use std::ffi::{CString, c_char, c_int, c_void};
use std::fs;
use std::io::{self, Write};
use std::mem::ManuallyDrop;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::ffi::{OsStrExt, OsStringExt};
use std::path::PathBuf;
use std::ptr;
use std::time::Instant;

use cowshed_core::api::CommandArg;
use serde::{Deserialize, Serialize};

const ROLE: &str = "COWSHED_PROCESS_SOURCE_ROLE";
const TRUE_PROGRAM: &str = "COWSHED_PROCESS_SOURCE_TRUE";
const REPORT: &str = "COWSHED_PROCESS_SOURCE_REPORT";
const ROLE_TEST: &str = "process_source_workload_role";

/// The fds the root inherits; a child inherits only the oracle.
const GATE_FD: RawFd = 3;
const ORACLE_FD: RawFd = 4;
const DONE_FD: RawFd = 5;
const HOLD_FD: RawFd = 6;

/// A burst a one-second census cannot see, and a fork-heavy run for overhead.
const COVERAGE_BURST: u32 = 32;
const OVERHEAD_BURST: u32 = 256;
const REPETITIONS: u32 = 5;

/// The root's exit code when `PTRACE_TRACEME` fails; the errno follows on the error pipe.
const TRACEME_FAILED: c_int = 121;

// ---------------------------------------------------------------------------------------------
// The workload.

/// What the harness asks a re-executed test binary to be.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "role", rename_all = "camelCase")]
enum Role {
    Root { burst: u32 },
    Child { index: u32 },
}

/// One process a fixture parent reaped.
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct OracleRecord {
    pid: u32,
    ppid: u32,
    start: u64,
    wait_status: i32,
    argv: Vec<CommandArg>,
}

/// The workload's root or child, run only when the harness re-executes this binary with
/// [`ROLE`] set.
#[test]
fn process_source_workload_role() {
    let Some(role) = std::env::var_os(ROLE) else {
        return;
    };
    let role: Role = serde_json::from_slice(role.as_bytes()).expect("role");
    match role {
        Role::Root { burst } => {
            let image = Image::test_binary();
            let children: Vec<Exec> = (0..burst)
                .map(|index| image.exec(Role::Child { index }))
                .collect();
            read_byte(GATE_FD).expect("gate");
            for child in &children {
                child.run_and_record();
            }
            write_byte(DONE_FD).expect("done");
            read_byte(HOLD_FD).expect("hold");
        }
        Role::Child { index } => {
            let program = std::env::var_os(TRUE_PROGRAM).expect("true program");
            // An empty and a non-UTF-8 argument: argv is compared byte for byte.
            Exec::new(
                program.into_vec(),
                vec![
                    b"true".to_vec(),
                    b"proc-source".to_vec(),
                    index.to_string().into_bytes(),
                    Vec::new(),
                    b"\xff\xfe".to_vec(),
                ],
                Vec::new(),
            )
            .run_and_record();
        }
    }
}

/// A program, argv and environment prepared before a fork, so the forked child of this
/// multithreaded process does nothing but `execve`.
struct Exec {
    program: CString,
    argv: Vec<CommandArg>,
    argv_c: Vec<CString>,
    envp_c: Vec<CString>,
}

impl Exec {
    fn new(program: Vec<u8>, argv: Vec<Vec<u8>>, environment: Vec<CString>) -> Self {
        Self {
            program: CString::new(program).expect("program"),
            argv_c: argv
                .iter()
                .map(|argument| CString::new(argument.clone()).expect("argv"))
                .collect(),
            argv: argv
                .into_iter()
                .map(|argument| CommandArg::new(OsString::from_vec(argument)))
                .collect(),
            envp_c: environment,
        }
    }

    /// Start it, wait for it to exit, read its start time while it is a zombie, reap it and
    /// append its record to the oracle.
    fn run_and_record(&self) {
        let argv: Vec<*const c_char> = self
            .argv_c
            .iter()
            .map(|argument| argument.as_ptr())
            .chain([ptr::null()])
            .collect();
        let envp: Vec<*const c_char> = self
            .envp_c
            .iter()
            .map(|entry| entry.as_ptr())
            .chain([ptr::null()])
            .collect();
        // SAFETY: the child only calls execve and _exit on memory prepared before the fork.
        let pid = unsafe { libc::fork() };
        if pid == 0 {
            unsafe {
                libc::execve(self.program.as_ptr(), argv.as_ptr(), envp.as_ptr());
                libc::_exit(127);
            }
        }
        assert!(pid > 0, "fork: {}", io::Error::last_os_error());
        let id = libc::id_t::try_from(pid).expect("pid");
        // SAFETY: waiting for our own child without reaping it.
        let waited = unsafe {
            let mut info: libc::siginfo_t = std::mem::zeroed();
            libc::waitid(libc::P_PID, id, &mut info, libc::WEXITED | libc::WNOWAIT)
        };
        assert_eq!(waited, 0, "waitid: {}", io::Error::last_os_error());
        let start = start_time(pid).expect("an unreaped child's start time");
        let mut status = 0;
        // SAFETY: reaping our own child.
        let reaped = unsafe { libc::waitpid(pid, &mut status, 0) };
        assert_eq!(reaped, pid, "waitpid: {}", io::Error::last_os_error());
        let mut line = serde_json::to_vec(&OracleRecord {
            pid: u32::try_from(pid).expect("pid"),
            ppid: std::process::id(),
            start,
            wait_status: status,
            argv: self.argv.clone(),
        })
        .expect("record");
        line.push(b'\n');
        // SAFETY: the oracle fd is inherited and stays open for this process's life.
        let mut oracle = ManuallyDrop::new(unsafe { fs::File::from_raw_fd(ORACLE_FD) });
        // One write of an O_APPEND file: records from concurrent writers never interleave.
        let written = oracle.write(&line).expect("oracle write");
        assert_eq!(written, line.len(), "short oracle write");
    }
}

/// This test binary, re-executed in a role.
struct Image {
    program: Vec<u8>,
    argv: Vec<Vec<u8>>,
    environment: Vec<CString>,
}

impl Image {
    fn test_binary() -> Self {
        let executable = std::env::current_exe().expect("test binary");
        let program = executable.as_os_str().as_bytes().to_vec();
        let argv = vec![
            program.clone(),
            b"--exact".to_vec(),
            ROLE_TEST.as_bytes().to_vec(),
            b"--nocapture".to_vec(),
        ];
        let true_program = match std::env::var_os(TRUE_PROGRAM) {
            Some(program) => PathBuf::from(program),
            None => find_program("true"),
        };
        let environment = std::env::vars_os()
            .filter(|(key, _)| key != ROLE && key != TRUE_PROGRAM)
            .map(|(key, value)| environment_entry(key.as_bytes(), value.as_bytes()))
            .chain([environment_entry(
                TRUE_PROGRAM.as_bytes(),
                true_program.as_os_str().as_bytes(),
            )])
            .collect();
        Self {
            program,
            argv,
            environment,
        }
    }

    fn exec(&self, role: Role) -> Exec {
        let role = serde_json::to_vec(&role).expect("role");
        let mut environment = self.environment.clone();
        environment.push(environment_entry(ROLE.as_bytes(), &role));
        Exec::new(self.program.clone(), self.argv.clone(), environment)
    }
}

fn environment_entry(key: &[u8], value: &[u8]) -> CString {
    CString::new([key, b"=", value].concat()).expect("environment")
}

/// Field 22 of `/proc/<pid>/stat`: clock ticks after boot.
fn start_time(pid: libc::pid_t) -> Option<u64> {
    let stat = fs::read(format!("/proc/{pid}/stat")).ok()?;
    let close = stat.iter().rposition(|&byte| byte == b')')?;
    // Field 3 (state) follows ") "; field 22 is the 20th from there.
    let field = stat.get(close + 2..)?.split(|&byte| byte == b' ').nth(19)?;
    std::str::from_utf8(field).ok()?.parse().ok()
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
    Ptrace,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Report {
    kernel: String,
    namespaces: NamespaceFacts,
    runs: Vec<Run>,
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
    /// Every observation, as observed.
    observed: Observed,
    /// Observations naming an identity observed before, or one no fixture process had.
    duplicates: Vec<String>,
    unexpected: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Expected {
    births: usize,
    execs: usize,
    exits: usize,
}

#[derive(Debug, Default, Serialize)]
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

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Serialize)]
struct Identity {
    pid: u32,
    start: u64,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Observed {
    births: Vec<Birth>,
    execs: Vec<ExecSeen>,
    exits: Vec<Exit>,
    /// Threads the tracer followed for their forks; they are not process lives.
    threads_born: usize,
    threads_exited: usize,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Birth {
    pid: u32,
    ppid: u32,
    start: Result<u64, String>,
    /// Whether a pidfd could be opened when the birth was observed; it is held to the end of
    /// the run, so the identity it names cannot be reused meanwhile.
    pidfd: Result<(), String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecSeen {
    pid: u32,
    start: Result<u64, String>,
    argv: Result<Vec<CommandArg>, String>,
    /// The thread id the process had before a non-leader thread's exec made it the leader.
    former_tid: Option<u32>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Exit {
    pid: u32,
    start: Result<u64, String>,
    wait_status: Option<i32>,
}

// ---------------------------------------------------------------------------------------------
// The harness.

#[test]
fn linux_process_event_sources_are_measured() {
    if std::env::var_os(ROLE).is_some() {
        return;
    }
    let image = Image::test_binary();
    let mut runs = Vec::new();
    for repetition in 0..REPETITIONS {
        for burst in [COVERAGE_BURST, OVERHEAD_BURST] {
            for mode in [Mode::Baseline, Mode::Census, Mode::Ptrace] {
                runs.push(Run {
                    mode,
                    burst,
                    repetition,
                    outcome: run(&image, mode, burst),
                });
            }
        }
    }
    let report = Report {
        kernel: fs::read_to_string("/proc/sys/kernel/osrelease")
            .map(|release| release.trim().to_owned())
            .unwrap_or_else(|error| format!("unreadable: {error}")),
        namespaces: namespace_facts(),
        runs,
    };
    let json = serde_json::to_string_pretty(&report).expect("report");
    println!("{json}");
    if let Some(path) = std::env::var_os(REPORT) {
        fs::write(path, &json).expect("write report");
    }

    for run in &report.runs {
        let Outcome::Measured(measured) = &run.outcome else {
            assert_eq!(run.mode, Mode::Ptrace, "only tracing can be refused");
            continue;
        };
        let coverage = &measured.coverage;
        match run.mode {
            Mode::Baseline => {}
            Mode::Census => {
                if run.burst == COVERAGE_BURST {
                    assert!(
                        !coverage.births_missing.is_empty(),
                        "a census before and after the burst must miss its short-lived births"
                    );
                }
            }
            // An event source that ran must have seen every life exactly, or it is not one.
            Mode::Ptrace => {
                let expected = &measured.expected;
                assert_eq!(
                    (
                        coverage.births_matched,
                        coverage.execs_matched,
                        coverage.exits_matched
                    ),
                    (expected.births, expected.execs, expected.exits),
                    "ptrace run {} of {}: {coverage:?}",
                    run.repetition,
                    run.burst
                );
                assert!(measured.duplicates.is_empty(), "{:?}", measured.duplicates);
                let unheld: Vec<_> = measured
                    .observed
                    .births
                    .iter()
                    .filter(|birth| birth.pidfd.is_err())
                    .map(|birth| (birth.pid, &birth.pidfd))
                    .collect();
                assert!(unheld.is_empty(), "births without a held pidfd: {unheld:?}");
                assert!(measured.unexpected.is_empty(), "{:?}", measured.unexpected);
            }
        }
    }
}

fn run(image: &Image, mode: Mode, burst: u32) -> Outcome {
    let root_exec = image.exec(Role::Root { burst });
    let root = match Root::spawn(&root_exec, mode == Mode::Ptrace) {
        Ok(root) => root,
        Err(unavailable) => return unavailable,
    };
    let workload_before = rusage(libc::RUSAGE_CHILDREN);
    let sight = match mode {
        Mode::Baseline => root.run_unobserved(),
        Mode::Census => root.run_with_census(),
        Mode::Ptrace => match root.run_traced() {
            Ok(sight) => sight,
            Err(unavailable) => return unavailable,
        },
    };
    let workload = rusage(libc::RUSAGE_CHILDREN).minus(workload_before);
    let mut oracle = root.oracle();
    oracle.push(OracleRecord {
        pid: root.identity.pid,
        ppid: std::process::id(),
        start: root.identity.start,
        wait_status: 0,
        argv: root_exec.argv,
    });
    Outcome::Measured(Box::new(score(sight, workload, &oracle)))
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
    /// The pidfds of observed births, held until the run is scored.
    pidfds: Vec<OwnedFd>,
}

impl Root {
    /// Fork the root, held on its gate; `traced` makes it ask this thread to trace it.
    fn spawn(exec: &Exec, traced: bool) -> Result<Self, Outcome> {
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
        let argv: Vec<*const c_char> = exec
            .argv_c
            .iter()
            .map(|argument| argument.as_ptr())
            .chain([ptr::null()])
            .collect();
        let envp: Vec<*const c_char> = exec
            .envp_c
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
                libc::execve(exec.program.as_ptr(), argv.as_ptr(), envp.as_ptr());
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
                let read = io::Read::read_exact(&mut fs::File::from(error.read), &mut errno);
                return Err(Outcome::Unavailable {
                    call: "ptrace(PTRACE_TRACEME)".to_owned(),
                    errno: read.is_ok().then(|| c_int::from_ne_bytes(errno)),
                    detail: "the root could not ask to be traced".to_owned(),
                });
            }
            assert!(
                libc::WIFSTOPPED(status) && libc::WSTOPSIG(status) == libc::SIGSTOP,
                "the root's first stop: wait status {status}"
            );
        }
        let identity = Identity {
            pid: u32::try_from(pid).expect("pid"),
            start: start_time(pid).expect("the gated root's start time"),
        };
        Ok(Self {
            pid,
            identity,
            gate: gate.write,
            done: done.read,
            hold: hold.write,
            oracle,
        })
    }

    fn release(&self) -> Instant {
        let released = Instant::now();
        write_byte(self.gate.as_raw_fd()).expect("gate");
        released
    }

    fn await_done(&self, released: Instant) -> u64 {
        read_byte(self.done.as_raw_fd()).expect("done");
        u64::try_from(released.elapsed().as_nanos()).expect("wall")
    }

    fn wait(&self) {
        let mut status = 0;
        // SAFETY: waiting for our own child.
        let waited = unsafe { libc::waitpid(self.pid, &mut status, 0) };
        assert_eq!(waited, self.pid, "waitpid: {}", io::Error::last_os_error());
        assert_eq!(status, 0, "root wait status");
    }

    /// The harness thread only waits: its CPU over the same window is the baseline observer's.
    fn run_unobserved(&self) -> Sight {
        write_byte(self.hold.as_raw_fd()).expect("hold");
        let before = rusage(libc::RUSAGE_THREAD);
        let released = self.release();
        let wall_ns = self.await_done(released);
        let observer = rusage(libc::RUSAGE_THREAD).minus(before);
        self.wait();
        Sight {
            wall_ns,
            observer,
            observed: Observed::default(),
            pidfds: Vec::new(),
        }
    }

    /// Read the tree before the gate opens and once the burst is done, while the root is held:
    /// what a poll once a second sees of work shorter than a second.
    fn run_with_census(&self) -> Sight {
        let before = rusage(libc::RUSAGE_THREAD);
        let mut observed = Observed::default();
        let mut seen = HashSet::new();
        let mut pidfds = Vec::new();
        census(self.pid, &mut observed, &mut seen, &mut pidfds);
        let released = self.release();
        let wall_ns = self.await_done(released);
        census(self.pid, &mut observed, &mut seen, &mut pidfds);
        let observer = rusage(libc::RUSAGE_THREAD).minus(before);
        write_byte(self.hold.as_raw_fd()).expect("hold");
        self.wait();
        Sight {
            wall_ns,
            observer,
            observed,
            pidfds,
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
            let killed = unsafe { libc::kill(self.pid, libc::SIGKILL) };
            assert_eq!(
                killed,
                0,
                "kill the untraceable root: {}",
                io::Error::last_os_error()
            );
            let mut status = 0;
            // SAFETY: reaping our own child.
            let reaped = unsafe { libc::waitpid(self.pid, &mut status, 0) };
            assert_eq!(
                reaped,
                self.pid,
                "reap the untraceable root: {}",
                io::Error::last_os_error()
            );
            return Err(Outcome::Unavailable {
                call: "ptrace(PTRACE_SETOPTIONS)".to_owned(),
                errno: error.raw_os_error(),
                detail: format!("{error}; the root was killed, wait status {status}"),
            });
        }
        write_byte(self.hold.as_raw_fd()).expect("hold");
        let before = rusage(libc::RUSAGE_THREAD);
        resume(self.pid, 0);
        let released = self.release();
        let mut observed = Observed::default();
        let mut pidfds = Vec::new();
        let mut started = HashSet::from([self.pid]);
        // The tracer must stay on this thread; another reads the root's DONE byte, so the wall
        // time ends at the same boundary as in every other mode.
        let (wall_ns, reader) = std::thread::scope(|scope| {
            let done = scope.spawn(|| {
                let before = rusage(libc::RUSAGE_THREAD);
                let wall_ns = self.await_done(released);
                (wall_ns, rusage(libc::RUSAGE_THREAD).minus(before))
            });
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
                        assert_eq!(status, 0, "root wait status");
                    }
                    continue;
                }
                let signal = libc::WSTOPSIG(status);
                let tracee = u32::try_from(pid).expect("pid");
                match status >> 16 {
                    libc::PTRACE_EVENT_FORK
                    | libc::PTRACE_EVENT_VFORK
                    | libc::PTRACE_EVENT_CLONE => {
                        let child = libc::pid_t::try_from(event_message(pid)).expect("child");
                        // A thread is traced for the forks it makes; it is not a process life.
                        if thread_group(child) == u32::try_from(child).expect("pid") {
                            observed.births.push(Birth {
                                pid: u32::try_from(child).expect("pid"),
                                ppid: thread_group(pid),
                                start: start_time(child).ok_or_else(|| "unreadable".to_owned()),
                                pidfd: hold_pidfd(child, &mut pidfds),
                            });
                        } else {
                            observed.threads_born += 1;
                        }
                        resume(pid, 0);
                    }
                    libc::PTRACE_EVENT_EXEC => {
                        let former = u32::try_from(event_message(pid)).expect("former tid");
                        observed.execs.push(ExecSeen {
                            pid: tracee,
                            start: start_time(pid).ok_or_else(|| "unreadable".to_owned()),
                            argv: command_line(pid),
                            former_tid: (former != tracee).then_some(former),
                        });
                        resume(pid, 0);
                    }
                    libc::PTRACE_EVENT_EXIT => {
                        let wait_status = event_message(pid);
                        if thread_group(pid) == tracee {
                            observed.exits.push(Exit {
                                pid: tracee,
                                start: start_time(pid).ok_or_else(|| "unreadable".to_owned()),
                                wait_status: i32::try_from(wait_status).ok(),
                            });
                        } else {
                            observed.threads_exited += 1;
                        }
                        resume(pid, 0);
                    }
                    _ if signal == libc::SIGSTOP && started.insert(pid) => resume(pid, 0),
                    _ => resume(pid, signal),
                }
            }
            done.join().expect("done reader")
        });
        // The tracer's thread and the thread that read DONE, which only this mode needs.
        let tracer = rusage(libc::RUSAGE_THREAD).minus(before);
        Ok(Sight {
            wall_ns,
            observer: Usage {
                user_us: tracer.user_us + reader.user_us,
                sys_us: tracer.sys_us + reader.sys_us,
            },
            observed,
            pidfds,
        })
    }

    fn oracle(&self) -> Vec<OracleRecord> {
        fs::read_to_string(&self.oracle.path)
            .expect("oracle")
            .lines()
            .map(|line| serde_json::from_str(line).expect("oracle record"))
            .collect()
    }
}

fn score(sight: Sight, workload: Usage, oracle: &[OracleRecord]) -> Measurement {
    assert!(
        oracle.iter().all(|record| record.wait_status == 0),
        "every fixture process exits 0"
    );
    let identity = |record: &OracleRecord| Identity {
        pid: record.pid,
        start: record.start,
    };
    let expected: HashSet<Identity> = oracle.iter().map(identity).collect();
    let observed = sight.observed;
    let mut duplicates = Vec::new();
    let mut unexpected = Vec::new();
    // Key each readable observation by identity, keeping every duplicate and stranger.
    let mut index = |kind: &str, pid: u32, start: &Result<u64, String>| {
        let start = *start.as_ref().ok()?;
        let key = Identity { pid, start };
        if !expected.contains(&key) {
            unexpected.push(format!("{kind} {key:?}"));
            return None;
        }
        Some(key)
    };
    let mut births = HashMap::new();
    for birth in &observed.births {
        if let Some(key) = index("birth", birth.pid, &birth.start)
            && births.insert(key, birth.ppid).is_some()
        {
            duplicates.push(format!("birth {key:?}"));
        }
    }
    let mut execs = HashMap::new();
    for exec in &observed.execs {
        if let Some(key) = index("exec", exec.pid, &exec.start)
            && execs.insert(key, &exec.argv).is_some()
        {
            duplicates.push(format!("exec {key:?}"));
        }
    }
    let mut exits = HashMap::new();
    for exit in &observed.exits {
        if let Some(key) = index("exit", exit.pid, &exit.start)
            && exits.insert(key, exit.wait_status).is_some()
        {
            duplicates.push(format!("exit {key:?}"));
        }
    }

    let mut coverage = Coverage::default();
    for record in oracle {
        let identity = identity(record);
        // The root's birth is the harness's own fork, not an observation.
        if record.ppid != std::process::id() {
            match births.get(&identity) {
                Some(&ppid) if ppid == record.ppid => coverage.births_matched += 1,
                Some(&ppid) => coverage.births_wrong_parent.push(format!(
                    "{identity:?}: observed parent {ppid}, expected {}",
                    record.ppid
                )),
                None => coverage.births_missing.push(identity),
            }
        }
        match execs.get(&identity) {
            Some(Ok(argv)) if *argv == record.argv => coverage.execs_matched += 1,
            Some(argv) => coverage.execs_mismatched.push(format!(
                "{identity:?}: observed {argv:?}, expected {:?}",
                record.argv
            )),
            None => coverage.execs_missing.push(identity),
        }
        match exits.get(&identity) {
            Some(Some(status)) if *status == record.wait_status => coverage.exits_matched += 1,
            Some(status) => coverage.exits_mismatched.push(format!(
                "{identity:?}: observed {status:?}, expected {}",
                record.wait_status
            )),
            None => coverage.exits_missing.push(identity),
        }
    }
    drop(sight.pidfds);

    Measurement {
        wall_ns: sight.wall_ns,
        observer_user_us: sight.observer.user_us,
        observer_sys_us: sight.observer.sys_us,
        workload_user_us: workload.user_us,
        workload_sys_us: workload.sys_us,
        expected: Expected {
            births: oracle.len() - 1,
            execs: oracle.len(),
            exits: oracle.len(),
        },
        coverage,
        observed,
        duplicates,
        unexpected,
    }
}

/// Every process now in the tree under `root`, read through each of its threads'
/// `/proc/<pid>/task/<tid>/children`: a child belongs to the thread that forked it.
fn census(
    root: libc::pid_t,
    observed: &mut Observed,
    seen: &mut HashSet<Identity>,
    pidfds: &mut Vec<OwnedFd>,
) {
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
                pidfd: hold_pidfd(child, pidfds),
            });
            observed.execs.push(ExecSeen {
                pid,
                start,
                argv: command_line(child),
                former_tid: None,
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

/// The argv bytes of a process; each argument is NUL-terminated, an empty one included.
fn command_line(pid: libc::pid_t) -> Result<Vec<CommandArg>, String> {
    let bytes = fs::read(format!("/proc/{pid}/cmdline")).map_err(|error| error.to_string())?;
    let Some(arguments) = bytes.strip_suffix(b"\0") else {
        return Err("empty: the process exited".to_owned());
    };
    Ok(arguments
        .split(|&byte| byte == 0)
        .map(|argument| CommandArg::new(OsString::from_vec(argument.to_vec())))
        .collect())
}

/// Open a pidfd for `pid` and keep it in `pidfds`.
fn hold_pidfd(pid: libc::pid_t, pidfds: &mut Vec<OwnedFd>) -> Result<(), String> {
    // SAFETY: pidfd_open with no flags.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    match RawFd::try_from(fd) {
        Ok(fd) if fd >= 0 => {
            // SAFETY: the syscall returned a new fd we own.
            pidfds.push(unsafe { OwnedFd::from_raw_fd(fd) });
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
        static NEXT: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let path = std::env::temp_dir().join(format!(
            "cowshed-process-source-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
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
