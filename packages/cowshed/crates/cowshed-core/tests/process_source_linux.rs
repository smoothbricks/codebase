//! Measures ptrace fork/exec/exit tracing, the Linux process event source a supervisor can run
//! as the job's parent (07_api.md, "Complete job accounting and observation reconciliation"),
//! against an unobserved baseline and a census that reads the tree before and after a burst, as
//! a one-second poll would. The proc connector is not run here: no crate this workspace locks
//! carries its UAPI records, and a probe built against the kernel's own `cn_proc.h` found its
//! listen request refused (`ECONNREFUSED`) on the measured runner, so no workload ran under it.
//!
//! One workload runs unchanged under every mode. The harness forks the job's root held on a
//! fence pipe; in the traced mode it attaches with `PTRACE_SEIZE` before opening the fence, so
//! the root's exec of this test binary in the root role is its first traced event. The root then
//! waits on a gate pipe until the observer is ready, and starts children one after another, each
//! this binary again in the child role. Each child makes a grandchild that execs `true` -- by
//! `fork`, by `clone` with `CLONE_VM | CLONE_VFORK`, or by a `clone` whose exit signal is not
//! `SIGCHLD`, in turn -- runs and joins a thread, then execs `true` from another, non-leader
//! thread, which takes the process's pid as its own tid. Every parent records each process it
//! reaps -- pid, parent, how it was made, kernel start time read while it is an unreaped zombie,
//! wait status and the argv it was given -- and every such thread records its own tid and the
//! argv it is about to exec, as JSON lines in an oracle file, independently of any observer.
//!
//! The coverage burst must end inside the census window, so a census that misses its lives
//! misses events, not time. Controls run once after the measured runs: an attach the kernel
//! refuses because another tracer owns the process, a tracer killed while tracing, and process
//! metadata read through files opened before the process was reaped. Each ends in a typed
//! failure that keeps what the kernel said.
//!
//! A mode the kernel or sandbox refuses is reported with the refused call and its errno, never
//! as an observation with zero events. The report is printed and, when
//! `COWSHED_PROCESS_SOURCE_REPORT` names a file, written there.
//!
//! `PTRACE_O_EXITKILL` belongs to this fixture: its tracees are its own. It says nothing about
//! what a production observer may do to a job when it is lost.

#![cfg(target_os = "linux")]

use std::collections::{BTreeMap, HashMap, HashSet};
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

/// The interval of the census this fixture stands for. A coverage burst must end inside it, or
/// a census missing its lives could be missing time rather than events.
const CENSUS_WINDOW_NS: u64 = 1_000_000_000;

/// A cloned grandchild's stack: it only calls execve and _exit.
const CLONE_STACK: usize = 64 * 1024;

/// What a ptrace source follows: births by fork, vfork and clone, every exec, and the exit stop.
const TRACE_OPTIONS: c_int = libc::PTRACE_O_TRACEFORK
    | libc::PTRACE_O_TRACEVFORK
    | libc::PTRACE_O_TRACECLONE
    | libc::PTRACE_O_TRACEEXEC
    | libc::PTRACE_O_TRACEEXIT;

// ---------------------------------------------------------------------------------------------
// The workload.

/// What the harness asks a re-executed test binary to be.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "role", rename_all = "camelCase")]
enum Role {
    Root { burst: u32 },
    Child { index: u32 },
}

/// How a process was made, and so the ptrace event that reports its birth.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "camelCase")]
enum Creation {
    /// `fork`: `PTRACE_EVENT_FORK`.
    Fork,
    /// `clone` with `CLONE_VM | CLONE_VFORK` and `SIGCHLD`: `PTRACE_EVENT_VFORK`.
    Vfork,
    /// `clone` of a process whose exit signal is not `SIGCHLD`: `PTRACE_EVENT_CLONE`, the event
    /// a new thread also has.
    Clone,
}

impl Creation {
    /// The grandchild of child `index`: the three creations in turn.
    fn of(index: u32) -> Self {
        match index % 3 {
            0 => Self::Fork,
            1 => Self::Vfork,
            _ => Self::Clone,
        }
    }
}

/// One line of the oracle file.
#[derive(Debug, Deserialize, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum OracleRecord {
    /// A process its fixture parent reaped.
    Reaped(Reaped),
    /// A thread that ran and was joined, written by the thread itself.
    Thread(ThreadIdentity),
    /// A non-leader thread about to exec, written by the thread itself. Its process keeps its
    /// pid and start time; the thread's tid becomes that pid.
    ThreadExec(ThreadExec),
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct Reaped {
    pid: u32,
    ppid: u32,
    start: u64,
    born: Creation,
    wait_status: i32,
    argv: Vec<CommandArg>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadExec {
    thread: ThreadIdentity,
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
                child.run_and_record(Creation::Fork);
            }
            write_byte(DONE_FD).expect("done");
            read_byte(HOLD_FD).expect("hold");
        }
        Role::Child { index } => {
            let program = std::env::var_os(TRUE_PROGRAM)
                .expect("true program")
                .into_vec();
            let process = Identity {
                pid: std::process::id(),
                start: start_time(libc::pid_t::try_from(std::process::id()).expect("pid"))
                    .expect("own start time"),
            };
            // An empty and a non-UTF-8 argument: argv is compared byte for byte.
            Exec::new(
                program.clone(),
                vec![
                    b"true".to_vec(),
                    b"proc-source".to_vec(),
                    index.to_string().into_bytes(),
                    Vec::new(),
                    b"\xff\xfe".to_vec(),
                ],
                Vec::new(),
            )
            .run_and_record(Creation::of(index));
            std::thread::spawn(move || {
                record(&OracleRecord::Thread(ThreadIdentity {
                    tid: current_tid(),
                    process,
                }));
            })
            .join()
            .expect("thread");
            let exec = Exec::new(
                program,
                vec![
                    b"true".to_vec(),
                    b"proc-source-thread-exec".to_vec(),
                    index.to_string().into_bytes(),
                ],
                Vec::new(),
            );
            let failed = std::thread::spawn(move || {
                let thread = ThreadIdentity {
                    tid: current_tid(),
                    process,
                };
                assert_ne!(
                    thread.tid, process.pid,
                    "a spawned thread is not the leader"
                );
                record(&OracleRecord::ThreadExec(ThreadExec {
                    thread,
                    argv: exec.argv.clone(),
                }));
                exec.launch().execve()
            })
            .join()
            .expect("exec thread");
            panic!("execve from a non-leader thread: {failed}");
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

    fn launch(&self) -> Launch<'_> {
        Launch {
            exec: self,
            argv: self
                .argv_c
                .iter()
                .map(|argument| argument.as_ptr())
                .chain([ptr::null()])
                .collect(),
            envp: self
                .envp_c
                .iter()
                .map(|entry| entry.as_ptr())
                .chain([ptr::null()])
                .collect(),
        }
    }

    /// Start it as `born` says, wait for it to exit, read its start time while it is a zombie,
    /// reap it and append its record to the oracle.
    fn run_and_record(&self, born: Creation) {
        let launch = self.launch();
        let pid = match born {
            // SAFETY: the child only calls execve and _exit on memory prepared before the fork.
            Creation::Fork => match unsafe { libc::fork() } {
                0 => launch.exec_or_exit(),
                pid => pid,
            },
            Creation::Vfork => {
                launch.start_clone(libc::CLONE_VM | libc::CLONE_VFORK | libc::SIGCHLD)
            }
            // No exit signal: its parent finds its end only by waiting with __WALL or __WCLONE.
            Creation::Clone => launch.start_clone(0),
        };
        assert!(pid > 0, "{born:?}: {}", io::Error::last_os_error());
        let id = libc::id_t::try_from(pid).expect("pid");
        // SAFETY: waiting for our own child without reaping it.
        let waited = unsafe {
            let mut info: libc::siginfo_t = std::mem::zeroed();
            libc::waitid(
                libc::P_PID,
                id,
                &mut info,
                libc::WEXITED | libc::WNOWAIT | libc::__WALL,
            )
        };
        assert_eq!(waited, 0, "waitid: {}", io::Error::last_os_error());
        let start = start_time(pid).expect("an unreaped child's start time");
        let wait_status = reap(pid);
        record(&OracleRecord::Reaped(Reaped {
            pid: u32::try_from(pid).expect("pid"),
            ppid: std::process::id(),
            start,
            born,
            wait_status,
            argv: self.argv.clone(),
        }));
    }
}

/// The pointer arrays `execve` takes, built before a fork or clone so the new process only
/// calls it.
struct Launch<'a> {
    exec: &'a Exec,
    argv: Vec<*const c_char>,
    envp: Vec<*const c_char>,
}

impl Launch<'_> {
    /// Replace this process's image; returns only why it could not.
    fn execve(&self) -> io::Error {
        // SAFETY: NUL-terminated strings and null-terminated arrays owned by the borrowed Exec.
        unsafe {
            libc::execve(
                self.exec.program.as_ptr(),
                self.argv.as_ptr(),
                self.envp.as_ptr(),
            )
        };
        io::Error::last_os_error()
    }

    /// In a forked or cloned child: execve, or `_exit(127)`.
    fn exec_or_exit(&self) -> ! {
        self.execve();
        // SAFETY: ending the child without running this process's exit handlers.
        unsafe { libc::_exit(127) }
    }

    /// Start a process that only runs [`Self::exec_or_exit`], on a stack of its own, by `clone`
    /// with `flags`: what it shares with this process, and its exit signal.
    fn start_clone(&self, flags: c_int) -> libc::pid_t {
        let mut stack = vec![0_u128; CLONE_STACK / size_of::<u128>()];
        // The stack grows down from its end, which u128's alignment keeps 16-byte aligned.
        let top = stack.as_mut_ptr_range().end.cast::<c_void>();
        // SAFETY: the child runs `launch_child` on `stack` and only calls execve and _exit. With
        // CLONE_VM it shares this memory, and CLONE_VFORK suspends the caller -- keeping `self`
        // and `stack` alive -- until it execs; without CLONE_VM it has its own copy of both.
        unsafe {
            libc::clone(
                launch_child,
                top,
                flags,
                ptr::from_ref(self).cast_mut().cast(),
            )
        }
    }
}

/// A cloned child's entry: `arg` is the [`Launch`] of the caller that cloned it.
extern "C" fn launch_child(arg: *mut c_void) -> c_int {
    // SAFETY: `Launch::start_clone` passes itself, alive in the shared or copied memory.
    let launch = unsafe { &*arg.cast::<Launch<'_>>() };
    launch.exec_or_exit()
}

/// Append one line to the oracle in a single write of the `O_APPEND` file: lines from
/// concurrent writers never interleave.
fn record(record: &OracleRecord) {
    let mut line = serde_json::to_vec(record).expect("record");
    line.push(b'\n');
    // SAFETY: the oracle fd is inherited and stays open for this process's life.
    let mut oracle = ManuallyDrop::new(unsafe { fs::File::from_raw_fd(ORACLE_FD) });
    let written = oracle.write(&line).expect("oracle write");
    assert_eq!(written, line.len(), "short oracle write");
}

fn current_tid() -> u32 {
    // SAFETY: gettid has no preconditions.
    u32::try_from(unsafe { libc::gettid() }).expect("tid")
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
// Process metadata.

/// A fact about a process that could not be read, with what the kernel said.
#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(tag = "kind", rename_all = "camelCase")]
enum Unread {
    /// The call failed with `errno`: the process was gone before or during the read, or this
    /// reader may not see it.
    Failed { call: String, errno: i32 },
    /// The read succeeded without the field: a malformed file, or the empty `cmdline` of a
    /// process whose address space is gone.
    Malformed { path: String, content: String },
}

impl Unread {
    fn failed(call: String, error: &io::Error) -> Self {
        let Some(errno) = error.raw_os_error() else {
            panic!("{call}: {error} carries no errno");
        };
        Self::Failed { call, errno }
    }
}

/// One opened `/proc/<pid>/<name>` file. It keeps naming that process: read after the process
/// is reaped, it fails with `ESRCH` rather than reading whatever later takes the pid.
struct ProcFile {
    path: String,
    file: fs::File,
}

impl ProcFile {
    fn open(pid: libc::pid_t, name: &str) -> Result<Self, Unread> {
        let path = format!("/proc/{pid}/{name}");
        match fs::File::open(&path) {
            Ok(file) => Ok(Self { path, file }),
            Err(error) => Err(Unread::failed(format!("open {path}"), &error)),
        }
    }

    fn read(mut self) -> Result<(String, Vec<u8>), Unread> {
        let mut bytes = Vec::new();
        match io::Read::read_to_end(&mut self.file, &mut bytes) {
            Ok(_) => Ok((self.path, bytes)),
            Err(error) => Err(Unread::failed(format!("read {}", self.path), &error)),
        }
    }

    /// Field 22 of `stat`: clock ticks after boot.
    fn start_time(self) -> Result<u64, Unread> {
        let (path, stat) = self.read()?;
        stat.iter()
            .rposition(|&byte| byte == b')')
            // Field 3 (state) follows ") "; field 22 is the 20th from there.
            .and_then(|close| stat.get(close + 2..))
            .and_then(|fields| fields.split(|&byte| byte == b' ').nth(19))
            .and_then(|field| std::str::from_utf8(field).ok())
            .and_then(|field| field.parse().ok())
            .ok_or_else(|| Unread::Malformed {
                content: String::from_utf8_lossy(&stat).into_owned(),
                path,
            })
    }

    /// The argv bytes of `cmdline`; each argument is NUL-terminated, an empty one included.
    fn command_line(self) -> Result<Vec<CommandArg>, Unread> {
        let (path, bytes) = self.read()?;
        let Some(arguments) = bytes.strip_suffix(b"\0") else {
            return Err(Unread::Malformed {
                content: String::from_utf8_lossy(&bytes).into_owned(),
                path,
            });
        };
        Ok(arguments
            .split(|&byte| byte == 0)
            .map(|argument| CommandArg::new(OsString::from_vec(argument.to_vec())))
            .collect())
    }

    /// The value of a `status` line such as `Tgid:`.
    fn status(self, key: &str) -> Result<String, Unread> {
        let (path, bytes) = self.read()?;
        let status = String::from_utf8_lossy(&bytes);
        status
            .lines()
            .find_map(|line| line.strip_prefix(key))
            .map(|value| value.trim().to_owned())
            .ok_or_else(|| Unread::Malformed {
                content: status.into_owned(),
                path,
            })
    }
}

fn start_time(pid: libc::pid_t) -> Result<u64, Unread> {
    ProcFile::open(pid, "stat")?.start_time()
}

fn command_line(pid: libc::pid_t) -> Result<Vec<CommandArg>, Unread> {
    ProcFile::open(pid, "cmdline")?.command_line()
}

/// A numeric `status` line such as `Tgid:` or `TracerPid:`.
fn status_number(pid: libc::pid_t, key: &str) -> Result<u32, Unread> {
    let value = ProcFile::open(pid, "status")?.status(key)?;
    value.parse().map_err(|_| Unread::Malformed {
        path: format!("/proc/{pid}/status"),
        content: format!("{key} {value}"),
    })
}

/// Open a pidfd for `pid` and keep it in `pidfds`. A held pidfd keeps naming that very process;
/// it does not stop the kernel from giving the numeric pid to a later one.
fn hold_pidfd(pid: libc::pid_t, pidfds: &mut Vec<OwnedFd>) -> Result<(), Unread> {
    // SAFETY: pidfd_open with no flags.
    let fd = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
    match RawFd::try_from(fd) {
        Ok(fd) if fd >= 0 => {
            // SAFETY: the syscall returned a new fd we own.
            pidfds.push(unsafe { OwnedFd::from_raw_fd(fd) });
            Ok(())
        }
        _ => Err(Unread::Failed {
            call: format!("pidfd_open({pid})"),
            errno: errno(),
        }),
    }
}

/// This thread's errno; async-signal-safe, so a forked child may read it.
fn errno() -> c_int {
    // SAFETY: the calling thread's errno location.
    unsafe { *libc::__errno_location() }
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
    census_window_ns: u64,
    runs: Vec<Run>,
    controls: Controls,
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
    /// The kernel refused to let the harness trace the root.
    Refused(AttachRejected),
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
    /// Observations naming an identity observed before, or a process no fixture process had.
    duplicates: Vec<String>,
    unexpected: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Expected {
    births: usize,
    execs: usize,
    exits: usize,
    threads: usize,
    thread_exits: usize,
    /// Execs by a non-leader thread, each also counted in `execs`.
    thread_execs: usize,
    /// Births by how the process was made.
    created: BTreeMap<Creation, usize>,
}

#[derive(Debug, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Coverage {
    births_matched: usize,
    births_missing: Vec<Identity>,
    /// Births observed with another parent, or as made another way.
    births_mismatched: Vec<String>,
    execs_matched: usize,
    /// Processes none of whose execs were observed.
    execs_missing: Vec<Identity>,
    /// Processes whose execs were observed in another order, with other argv or with another
    /// former thread id.
    execs_mismatched: Vec<String>,
    exits_matched: usize,
    exits_missing: Vec<Identity>,
    exits_mismatched: Vec<String>,
    threads_matched: usize,
    threads_missing: Vec<ThreadIdentity>,
    thread_exits_matched: usize,
    thread_exits_missing: Vec<ThreadIdentity>,
    thread_exits_mismatched: Vec<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct Identity {
    pid: u32,
    start: u64,
}

/// A thread by its tid within one process life.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, PartialEq, Serialize)]
struct ThreadIdentity {
    tid: u32,
    process: Identity,
}

#[derive(Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Observed {
    births: Vec<Birth>,
    execs: Vec<ExecSeen>,
    exits: Vec<Exit>,
    threads: Vec<ThreadIdentity>,
    thread_exits: Vec<ThreadExit>,
    /// Reads that failed, each with its call and errno: what they would have shown is missing.
    unread: Vec<Unread>,
    /// Tasks the source could not place: an event from a task no creation introduced, or a
    /// placed task whose end it never saw.
    strays: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Birth {
    process: Identity,
    ppid: u32,
    /// The creation its ptrace event names; a census sees the child but not how it was made.
    created: Option<Creation>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExecSeen {
    process: Identity,
    argv: Vec<CommandArg>,
    /// The thread id the exec'ing thread had before a non-leader exec made it the leader.
    former_tid: Option<u32>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Exit {
    process: Identity,
    wait_status: i32,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ThreadExit {
    thread: ThreadIdentity,
    wait_status: i32,
}

/// What the ownership and loss controls ended in.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Controls {
    attach_rejected: AttachRejected,
    tracer_lost: TracerLost,
    metadata_unread: MetadataUnread,
}

/// `PTRACE_SEIZE` refused: another tracer owns the process, or Yama, credentials or
/// dumpability forbid this one.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AttachRejected {
    tracee: Identity,
    errno: i32,
    /// The tracee's `TracerPid` when refused: the tracer that owns it, or 0.
    tracer_pid: Result<u32, Unread>,
}

/// A tracer died while tracing. The kernel detaches its tracees without failing any call, so
/// this loss has no errno: it is the tracer's wait status and the tracee's state after it.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TracerLost {
    tracee: Identity,
    tracer: u32,
    /// How the tracer ended: killed, without detaching.
    tracer_wait_status: i32,
    /// The tracee's `TracerPid` once its tracer was reaped: 0, nothing observes it.
    tracee_tracer_pid: Result<u32, Unread>,
    /// The tracee's `State` then: it lives on.
    tracee_state: Result<String, Unread>,
    /// How the tracee ended once released, untraced.
    tracee_wait_status: i32,
}

/// Metadata read through files opened while the process lived, after it was reaped.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MetadataUnread {
    process: Identity,
    start_time: Result<u64, Unread>,
    command_line: Result<Vec<CommandArg>, Unread>,
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
    let (attach_rejected, tracer_lost) = ownership_controls();
    let report = Report {
        kernel: fs::read_to_string("/proc/sys/kernel/osrelease")
            .map(|release| release.trim().to_owned())
            .unwrap_or_else(|error| format!("unreadable: {error}")),
        namespaces: namespace_facts(),
        census_window_ns: CENSUS_WINDOW_NS,
        runs,
        controls: Controls {
            attach_rejected,
            tracer_lost,
            metadata_unread: metadata_control(),
        },
    };
    let json = serde_json::to_string_pretty(&report).expect("report");
    println!("{json}");
    if let Some(path) = std::env::var_os(REPORT) {
        fs::write(path, &json).expect("write report");
    }

    for run in &report.runs {
        let measured = match &run.outcome {
            Outcome::Measured(measured) => measured,
            Outcome::Refused(_) => {
                assert_eq!(run.mode, Mode::Ptrace, "only tracing can be refused");
                continue;
            }
        };
        let expected = &measured.expected;
        let coverage = &measured.coverage;
        let made: Vec<_> = expected.created.keys().copied().collect();
        assert_eq!(
            made,
            [Creation::Fork, Creation::Vfork, Creation::Clone],
            "the workload makes processes every way"
        );
        assert!(
            expected.threads > 0 && expected.thread_exits > 0 && expected.thread_execs > 0,
            "the workload runs threads and execs from them"
        );
        if run.burst == COVERAGE_BURST {
            assert!(
                measured.wall_ns < CENSUS_WINDOW_NS,
                "{:?} coverage burst, run {}: {} ns is not inside the {CENSUS_WINDOW_NS} ns census window",
                run.mode,
                run.repetition,
                measured.wall_ns
            );
        }
        match run.mode {
            Mode::Baseline => {}
            Mode::Census => {
                if run.burst == COVERAGE_BURST {
                    assert_eq!(
                        coverage.births_missing.len(),
                        expected.births,
                        "a census before and after a burst inside its window misses every \
                         short-lived birth: {coverage:?}"
                    );
                }
            }
            // An event source that ran must have seen every life exactly, or it is not one.
            Mode::Ptrace => {
                assert_eq!(
                    (
                        coverage.births_matched,
                        coverage.execs_matched,
                        coverage.exits_matched,
                        coverage.threads_matched,
                        coverage.thread_exits_matched,
                    ),
                    (
                        expected.births,
                        expected.execs,
                        expected.exits,
                        expected.threads,
                        expected.thread_exits,
                    ),
                    "ptrace run {} of {}: {coverage:?}",
                    run.repetition,
                    run.burst
                );
                assert!(measured.duplicates.is_empty(), "{:?}", measured.duplicates);
                assert!(measured.unexpected.is_empty(), "{:?}", measured.unexpected);
                assert!(
                    measured.observed.unread.is_empty(),
                    "{:?}",
                    measured.observed.unread
                );
                assert!(
                    measured.observed.strays.is_empty(),
                    "{:?}",
                    measured.observed.strays
                );
            }
        }
    }

    let controls = &report.controls;
    let rejected = &controls.attach_rejected;
    let lost = &controls.tracer_lost;
    assert_eq!(
        (rejected.errno, &rejected.tracer_pid),
        (libc::EPERM, &Ok(lost.tracer)),
        "seizing a process another tracer owns"
    );
    assert!(
        libc::WIFSIGNALED(lost.tracer_wait_status)
            && libc::WTERMSIG(lost.tracer_wait_status) == libc::SIGKILL,
        "the tracer was killed: wait status {}",
        lost.tracer_wait_status
    );
    assert_eq!(lost.tracee_tracer_pid, Ok(0), "the lost tracee is untraced");
    // Its tracer seized it without stopping it, so it may not have reached its gate's read yet:
    // running or blocked there, but neither stopped, a zombie nor dead.
    assert!(
        lost.tracee_state
            .as_ref()
            .is_ok_and(|state| state.starts_with('S') || state.starts_with('R')),
        "the lost tracee lives on, untraced: {:?}",
        lost.tracee_state
    );
    assert_eq!(lost.tracee_wait_status, 0, "the lost tracee ran to its end");
    let unread = &controls.metadata_unread;
    let pid = unread.process.pid;
    assert_eq!(
        (&unread.start_time, &unread.command_line),
        (
            &Err(Unread::Failed {
                call: format!("read /proc/{pid}/stat"),
                errno: libc::ESRCH,
            }),
            &Err(Unread::Failed {
                call: format!("read /proc/{pid}/cmdline"),
                errno: libc::ESRCH,
            }),
        ),
        "metadata of a process reaped mid-read"
    );
}

fn run(image: &Image, mode: Mode, burst: u32) -> Outcome {
    let root_exec = image.exec(Role::Root { burst });
    let root = Root::spawn(&root_exec);
    if mode == Mode::Ptrace
        && let Err(rejected) = root.seize()
    {
        root.kill();
        return Outcome::Refused(rejected);
    }
    root.start();
    let workload_before = rusage(libc::RUSAGE_CHILDREN);
    let sight = match mode {
        Mode::Baseline => root.run_unobserved(),
        Mode::Census => root.run_with_census(),
        Mode::Ptrace => root.run_traced(),
    };
    let workload = rusage(libc::RUSAGE_CHILDREN).minus(workload_before);
    let mut oracle = root.oracle();
    oracle.push(OracleRecord::Reaped(Reaped {
        pid: root.identity.pid,
        ppid: std::process::id(),
        start: root.identity.start,
        born: Creation::Fork,
        wait_status: 0,
        argv: root_exec.argv,
    }));
    Outcome::Measured(Box::new(score(sight, workload, &oracle)))
}

struct Root {
    pid: libc::pid_t,
    identity: Identity,
    fence: OwnedFd,
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
    /// Fork the root, held on its fence before it execs.
    fn spawn(exec: &Exec) -> Self {
        let fence = Pipe::new();
        let gate = Pipe::new();
        let done = Pipe::new();
        let hold = Pipe::new();
        let oracle = OracleFile::new();
        let null = high(
            fs::File::options()
                .write(true)
                .open("/dev/null")
                .expect("/dev/null")
                .into(),
        );
        let launch = exec.launch();
        let moves = [
            (gate.read.as_raw_fd(), GATE_FD),
            (oracle.fd.as_raw_fd(), ORACLE_FD),
            (done.write.as_raw_fd(), DONE_FD),
            (hold.read.as_raw_fd(), HOLD_FD),
            (null.as_raw_fd(), libc::STDOUT_FILENO),
        ];
        let fence_read = fence.read.as_raw_fd();
        // SAFETY: after fork the child calls only async-signal-safe functions on memory
        // prepared before the fork. Every source fd is at or above 100, so no move clobbers a
        // later source.
        let pid = unsafe { libc::fork() };
        if pid == 0 {
            let mut byte = 0_u8;
            // SAFETY: as above.
            unsafe {
                if libc::read(fence_read, ptr::from_mut(&mut byte).cast(), 1) != 1 {
                    libc::_exit(125);
                }
                for (from, to) in moves {
                    if libc::dup2(from, to) != to {
                        libc::_exit(126);
                    }
                }
            }
            launch.exec_or_exit();
        }
        assert!(pid > 0, "fork: {}", io::Error::last_os_error());
        let identity = Identity {
            pid: u32::try_from(pid).expect("pid"),
            start: start_time(pid).expect("the fenced root's start time"),
        };
        Self {
            pid,
            identity,
            fence: fence.write,
            gate: gate.write,
            done: done.read,
            hold: hold.write,
            oracle,
        }
    }

    /// Attach before the root execs, as a source supervising its own child would.
    fn seize(&self) -> Result<(), AttachRejected> {
        seize(self.pid, TRACE_OPTIONS | libc::PTRACE_O_EXITKILL).map_err(|errno| AttachRejected {
            tracee: self.identity,
            errno,
            tracer_pid: status_number(self.pid, "TracerPid:"),
        })
    }

    /// Kill and reap a root that could not be traced.
    fn kill(&self) {
        kill(self.pid);
        let status = reap(self.pid);
        assert!(
            libc::WIFSIGNALED(status) && libc::WTERMSIG(status) == libc::SIGKILL,
            "the untraceable root's wait status {status}"
        );
    }

    /// Open the fence: the root execs.
    fn start(&self) {
        write_byte(self.fence.as_raw_fd()).expect("fence");
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
        assert_eq!(reap(self.pid), 0, "root wait status");
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

    /// Trace the root and every descendant from this thread, which seized the root.
    fn run_traced(&self) -> Sight {
        write_byte(self.hold.as_raw_fd()).expect("hold");
        let before = rusage(libc::RUSAGE_THREAD);
        let released = self.release();
        let mut tracer = Tracer {
            lives: HashMap::from([(self.pid, Life::Process(self.identity))]),
            unplaced: HashSet::new(),
            lost: false,
            observed: Observed::default(),
            pidfds: Vec::new(),
        };
        // The tracer must stay on this thread; another reads the root's DONE byte, so the wall
        // time ends at the same boundary as in every other mode.
        let (wall_ns, reader) = std::thread::scope(|scope| {
            let done = scope.spawn(|| {
                let before = rusage(libc::RUSAGE_THREAD);
                let wall_ns = self.await_done(released);
                (wall_ns, rusage(libc::RUSAGE_THREAD).minus(before))
            });
            tracer.run();
            done.join().expect("done reader")
        });
        // The tracer's thread and the thread that read DONE, which only this mode needs.
        let traced = rusage(libc::RUSAGE_THREAD).minus(before);
        Sight {
            wall_ns,
            observer: Usage {
                user_us: traced.user_us + reader.user_us,
                sys_us: traced.sys_us + reader.sys_us,
            },
            observed: tracer.observed,
            pidfds: tracer.pidfds,
        }
    }

    fn oracle(&self) -> Vec<OracleRecord> {
        fs::read_to_string(&self.oracle.path)
            .expect("oracle")
            .lines()
            .map(|line| serde_json::from_str(line).expect("oracle record"))
            .collect()
    }
}

/// A traced task the source has placed: a process life, or a thread of one.
#[derive(Clone, Copy, Debug)]
enum Life {
    Process(Identity),
    Thread(Identity),
}

impl Life {
    fn process(self) -> Identity {
        match self {
            Self::Process(process) | Self::Thread(process) => process,
        }
    }
}

/// The ptrace source: every traced task it has placed, and what it observed.
struct Tracer {
    /// Each placed task by tid, until its terminal wait status.
    lives: HashMap<libc::pid_t, Life>,
    /// New tracees whose first stop came before the event that made them; each stays stopped
    /// until that event places it.
    unplaced: HashSet<libc::pid_t>,
    /// A creation could not be placed, so no new tracee can be placed with certainty: each
    /// runs unplaced, a stray, rather than waiting forever for an event already lost.
    lost: bool,
    observed: Observed,
    /// The pidfds of observed births, held until the run is scored.
    pidfds: Vec<OwnedFd>,
}

impl Tracer {
    fn run(&mut self) {
        loop {
            let mut status = 0;
            // SAFETY: waiting for any tracee.
            let pid = unsafe { libc::waitpid(-1, &mut status, libc::__WALL) };
            if pid == -1 {
                match errno() {
                    libc::ECHILD => break,
                    libc::EINTR => continue,
                    errno => panic!("waitpid: {}", io::Error::from_raw_os_error(errno)),
                }
            }
            if libc::WIFSTOPPED(status) {
                self.stopped(pid, status);
            } else {
                self.ended(pid, status);
            }
        }
        for (pid, life) in self.lives.drain() {
            self.observed
                .strays
                .push(format!("{life:?} at tid {pid} never ended"));
        }
        for pid in self.unplaced.drain() {
            self.observed
                .strays
                .push(format!("tid {pid} stopped, but no event made it"));
        }
    }

    fn stopped(&mut self, pid: libc::pid_t, status: c_int) {
        let signal = libc::WSTOPSIG(status);
        match status >> 16 {
            libc::PTRACE_EVENT_FORK => self.creation(pid, Creation::Fork),
            libc::PTRACE_EVENT_VFORK => self.creation(pid, Creation::Vfork),
            libc::PTRACE_EVENT_CLONE => self.creation(pid, Creation::Clone),
            libc::PTRACE_EVENT_EXEC => {
                if let Err(unread) = self.exec(pid) {
                    self.observed.unread.push(unread);
                }
            }
            // Where a source reads a task's final usage. The task's end is its terminal wait
            // status, not this early stop.
            libc::PTRACE_EVENT_EXIT => {}
            // A new tracee's first stop: it runs once the event that made it has placed it.
            libc::PTRACE_EVENT_STOP if signal == libc::SIGTRAP => {
                if !self.lives.contains_key(&pid) {
                    if !self.lost {
                        self.unplaced.insert(pid);
                        return;
                    }
                    self.observed
                        .strays
                        .push(format!("tid {pid} started after a creation was lost"));
                }
            }
            // A group-stop: the tracee stays stopped, as it would untraced, until continued.
            libc::PTRACE_EVENT_STOP => return listen(pid),
            // A signal on its way: deliver it.
            0 => return resume(pid, signal),
            event => self
                .observed
                .strays
                .push(format!("tid {pid} stopped at unknown ptrace event {event}")),
        }
        resume(pid, 0);
    }

    fn creation(&mut self, parent: libc::pid_t, created: Creation) {
        match self.place(parent, created) {
            Ok(child) => {
                if self.unplaced.remove(&child) {
                    resume(child, 0);
                }
            }
            Err(unread) => {
                self.observed.unread.push(unread);
                self.lost = true;
                for pid in std::mem::take(&mut self.unplaced) {
                    self.observed
                        .strays
                        .push(format!("tid {pid} started before a creation was lost"));
                    resume(pid, 0);
                }
            }
        }
    }

    /// Place the child `parent`'s creation event names: a process with its birth, or a thread
    /// of `parent`'s process.
    fn place(&mut self, parent: libc::pid_t, created: Creation) -> Result<libc::pid_t, Unread> {
        let child = libc::pid_t::try_from(event_message(parent)?).expect("child tid");
        let tid = u32::try_from(child).expect("tid");
        let Some(process) = self.lives.get(&parent).map(|life| life.process()) else {
            self.observed
                .strays
                .push(format!("tid {parent}, never placed, made tid {child}"));
            return Ok(child);
        };
        let thread = match created {
            Creation::Clone => status_number(child, "Tgid:")? != tid,
            Creation::Fork | Creation::Vfork => false,
        };
        if thread {
            self.lives.insert(child, Life::Thread(process));
            self.observed.threads.push(ThreadIdentity { tid, process });
        } else {
            let born = Identity {
                pid: tid,
                start: start_time(child)?,
            };
            hold_pidfd(child, &mut self.pidfds)?;
            self.lives.insert(child, Life::Process(born));
            self.observed.births.push(Birth {
                process: born,
                ppid: process.pid,
                created: Some(created),
            });
        }
        Ok(child)
    }

    fn exec(&mut self, pid: libc::pid_t) -> Result<(), Unread> {
        let former = u32::try_from(event_message(pid)?).expect("former tid");
        let tracee = u32::try_from(pid).expect("pid");
        let former_tid = (former != tracee).then_some(former);
        if let Some(former) = former_tid {
            // A non-leader thread exec'd: it now has the process's pid; its own tid is gone.
            let former = libc::pid_t::try_from(former).expect("tid");
            match self.lives.remove(&former) {
                Some(Life::Thread(process)) if process.pid == tracee => {}
                other => self.observed.strays.push(format!(
                    "pid {pid} exec'd from tid {former}, which was placed as {other:?}"
                )),
            }
        }
        let process = Identity {
            pid: tracee,
            start: start_time(pid)?,
        };
        self.observed.execs.push(ExecSeen {
            process,
            argv: command_line(pid)?,
            former_tid,
        });
        Ok(())
    }

    /// A terminal wait status: the end of a placed process or thread.
    fn ended(&mut self, pid: libc::pid_t, wait_status: c_int) {
        match self.lives.remove(&pid) {
            Some(Life::Process(process)) => self.observed.exits.push(Exit {
                process,
                wait_status,
            }),
            Some(Life::Thread(process)) => self.observed.thread_exits.push(ThreadExit {
                thread: ThreadIdentity {
                    tid: u32::try_from(pid).expect("tid"),
                    process,
                },
                wait_status,
            }),
            None => self.observed.strays.push(format!(
                "tid {pid}, never placed, ended with wait status {wait_status}"
            )),
        }
    }
}

/// A process's execs in order: each argv, and the thread id a non-leader exec had before it.
type ExecSteps<'a> = Vec<(&'a [CommandArg], Option<u32>)>;

fn score(sight: Sight, workload: Usage, oracle: &[OracleRecord]) -> Measurement {
    let harness = std::process::id();
    let mut births = HashMap::new();
    let mut execs: HashMap<Identity, ExecSteps<'_>> = HashMap::new();
    let mut exits = HashMap::new();
    let mut threads = HashSet::new();
    let mut thread_exits = HashSet::new();
    let mut thread_execs = 0;
    let mut created = BTreeMap::new();
    // Every reaped process first, so its first exec leads its list.
    for record in oracle {
        let OracleRecord::Reaped(reaped) = record else {
            continue;
        };
        assert_eq!(
            reaped.wait_status, 0,
            "every fixture process exits 0: {reaped:?}"
        );
        let process = Identity {
            pid: reaped.pid,
            start: reaped.start,
        };
        // The root's birth is the harness's own fork, not an observation.
        if reaped.ppid != harness {
            births.insert(process, (reaped.ppid, reaped.born));
            *created.entry(reaped.born).or_insert(0) += 1;
        }
        execs
            .entry(process)
            .or_default()
            .push((reaped.argv.as_slice(), None));
        exits.insert(process, reaped.wait_status);
    }
    for record in oracle {
        match record {
            OracleRecord::Reaped(_) => {}
            OracleRecord::Thread(thread) => {
                threads.insert(*thread);
                thread_exits.insert(*thread);
            }
            OracleRecord::ThreadExec(exec) => {
                threads.insert(exec.thread);
                execs
                    .entry(exec.thread.process)
                    .or_default()
                    .push((exec.argv.as_slice(), Some(exec.thread.tid)));
                thread_execs += 1;
            }
        }
    }

    let observed = sight.observed;
    let mut duplicates = Vec::new();
    let mut unexpected = Vec::new();
    let mut seen_births = HashMap::new();
    for birth in &observed.births {
        if !exits.contains_key(&birth.process) {
            unexpected.push(format!("birth {:?}", birth.process));
        } else if seen_births
            .insert(birth.process, (birth.ppid, birth.created))
            .is_some()
        {
            duplicates.push(format!("birth {:?}", birth.process));
        }
    }
    let mut seen_execs: HashMap<Identity, ExecSteps<'_>> = HashMap::new();
    for exec in &observed.execs {
        if exits.contains_key(&exec.process) {
            seen_execs
                .entry(exec.process)
                .or_default()
                .push((exec.argv.as_slice(), exec.former_tid));
        } else {
            unexpected.push(format!("exec {:?}", exec.process));
        }
    }
    let mut seen_exits = HashMap::new();
    for exit in &observed.exits {
        if !exits.contains_key(&exit.process) {
            unexpected.push(format!("exit {:?}", exit.process));
        } else if seen_exits.insert(exit.process, exit.wait_status).is_some() {
            duplicates.push(format!("exit {:?}", exit.process));
        }
    }
    // Threads the runtime starts on its own are not in the oracle, and are not unexpected.
    let mut seen_threads = HashSet::new();
    for thread in &observed.threads {
        if !seen_threads.insert(*thread) {
            duplicates.push(format!("thread {thread:?}"));
        }
    }
    let mut seen_thread_exits = HashMap::new();
    for exit in &observed.thread_exits {
        if seen_thread_exits
            .insert(exit.thread, exit.wait_status)
            .is_some()
        {
            duplicates.push(format!("thread exit {:?}", exit.thread));
        }
    }

    let mut coverage = Coverage::default();
    for (process, &(ppid, born)) in &births {
        match seen_births.get(process) {
            Some(&(seen_ppid, seen_created)) if seen_ppid == ppid && seen_created == Some(born) => {
                coverage.births_matched += 1;
            }
            Some(seen) => coverage.births_mismatched.push(format!(
                "{process:?}: observed {seen:?}, expected ({ppid}, {born:?})"
            )),
            None => coverage.births_missing.push(*process),
        }
    }
    for (process, expected) in &execs {
        match seen_execs.get(process) {
            Some(seen) if seen == expected => coverage.execs_matched += expected.len(),
            Some(seen) => coverage.execs_mismatched.push(format!(
                "{process:?}: observed {seen:?}, expected {expected:?}"
            )),
            None => coverage.execs_missing.push(*process),
        }
    }
    for (process, &status) in &exits {
        match seen_exits.get(process) {
            Some(&seen) if seen == status => coverage.exits_matched += 1,
            Some(seen) => coverage
                .exits_mismatched
                .push(format!("{process:?}: observed {seen}, expected {status}")),
            None => coverage.exits_missing.push(*process),
        }
    }
    for thread in &threads {
        if seen_threads.contains(thread) {
            coverage.threads_matched += 1;
        } else {
            coverage.threads_missing.push(*thread);
        }
    }
    for thread in &thread_exits {
        match seen_thread_exits.get(thread) {
            Some(0) => coverage.thread_exits_matched += 1,
            Some(seen) => coverage
                .thread_exits_mismatched
                .push(format!("{thread:?}: observed {seen}, expected 0")),
            None => coverage.thread_exits_missing.push(*thread),
        }
    }
    let expected = Expected {
        births: births.len(),
        execs: execs.values().map(Vec::len).sum(),
        exits: exits.len(),
        threads: threads.len(),
        thread_exits: thread_exits.len(),
        thread_execs,
        created,
    };
    drop(sight.pidfds);

    Measurement {
        wall_ns: sight.wall_ns,
        observer_user_us: sight.observer.user_us,
        observer_sys_us: sight.observer.sys_us,
        workload_user_us: workload.user_us,
        workload_sys_us: workload.sys_us,
        expected,
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
        let threads = match fs::read_dir(format!("/proc/{parent}/task")) {
            Ok(threads) => threads,
            Err(error) => {
                observed.unread.push(Unread::failed(
                    format!("read_dir /proc/{parent}/task"),
                    &error,
                ));
                continue;
            }
        };
        let mut children = Vec::new();
        for thread in threads {
            let path = match thread {
                Ok(thread) => thread.path().join("children"),
                Err(error) => {
                    observed.unread.push(Unread::failed(
                        format!("read_dir /proc/{parent}/task"),
                        &error,
                    ));
                    continue;
                }
            };
            match fs::read_to_string(&path) {
                Ok(text) => children.push(text),
                Err(error) => observed
                    .unread
                    .push(Unread::failed(format!("read {}", path.display()), &error)),
            }
        }
        for child in children
            .iter()
            .flat_map(|children| children.split_whitespace())
        {
            let child: libc::pid_t = child.parse().expect("child pid");
            parents.push(child);
            let process = match start_time(child) {
                Ok(start) => Identity {
                    pid: u32::try_from(child).expect("pid"),
                    start,
                },
                Err(unread) => {
                    observed.unread.push(unread);
                    continue;
                }
            };
            if !seen.insert(process) {
                continue;
            }
            if let Err(unread) = hold_pidfd(child, pidfds) {
                observed.unread.push(unread);
            }
            observed.births.push(Birth {
                process,
                ppid: u32::try_from(parent).expect("pid"),
                created: None,
            });
            match command_line(child) {
                Ok(argv) => observed.execs.push(ExecSeen {
                    process,
                    argv,
                    former_tid: None,
                }),
                Err(unread) => observed.unread.push(unread),
            }
        }
    }
}

/// Attach to `pid` with `options`; the kernel's errno if it refuses.
fn seize(pid: libc::pid_t, options: c_int) -> Result<(), c_int> {
    let options = usize::try_from(options).expect("options");
    // SAFETY: PTRACE_SEIZE takes no address and the options word as its data.
    let seized = unsafe {
        libc::ptrace(
            libc::PTRACE_SEIZE,
            pid,
            ptr::null_mut::<c_void>(),
            ptr::without_provenance_mut::<c_void>(options),
        )
    };
    if seized == -1 { Err(errno()) } else { Ok(()) }
}

fn event_message(pid: libc::pid_t) -> Result<libc::c_ulong, Unread> {
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
    if result == -1 {
        return Err(Unread::Failed {
            call: format!("ptrace(PTRACE_GETEVENTMSG, {pid})"),
            errno: errno(),
        });
    }
    Ok(message)
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
    restarted(result, "PTRACE_CONT");
}

/// Leave a tracee in its group-stop, reporting what ends it.
fn listen(pid: libc::pid_t) {
    // SAFETY: the tracee is in a group-stop.
    let result = unsafe {
        libc::ptrace(
            libc::PTRACE_LISTEN,
            pid,
            ptr::null_mut::<c_void>(),
            ptr::null_mut::<c_void>(),
        )
    };
    restarted(result, "PTRACE_LISTEN");
}

/// A tracee killed while stopped is gone; its end still reaches waitpid.
fn restarted(result: libc::c_long, request: &str) {
    if result == -1 {
        let errno = errno();
        assert_eq!(
            errno,
            libc::ESRCH,
            "{request}: {}",
            io::Error::from_raw_os_error(errno)
        );
    }
}

fn kill(pid: libc::pid_t) {
    // SAFETY: signalling our own child.
    let killed = unsafe { libc::kill(pid, libc::SIGKILL) };
    assert_eq!(killed, 0, "kill {pid}: {}", io::Error::last_os_error());
}

/// Reap a child, whatever its exit signal, and return its wait status.
fn reap(pid: libc::pid_t) -> c_int {
    let mut status = 0;
    // SAFETY: waiting for our own child.
    let reaped = unsafe { libc::waitpid(pid, &mut status, libc::__WALL) };
    assert_eq!(reaped, pid, "waitpid {pid}: {}", io::Error::last_os_error());
    status
}

// ---------------------------------------------------------------------------------------------
// Ownership and loss controls.

/// A tracer the harness can kill. It is a forked child that makes its own tracee -- Yama scope 1
/// lets it trace a descendant -- held on a gate, seizes it without `PTRACE_O_EXITKILL` so its
/// death detaches rather than kills, reports the tracee's pid and the seize errno, and pauses.
/// While it lives, the harness's own seize is refused; once it is killed and reaped, the tracee
/// runs on untraced.
fn ownership_controls() -> (AttachRejected, TracerLost) {
    let gate = Pipe::new();
    let report = Pipe::new();
    let gate_read = gate.read.as_raw_fd();
    let gate_write = gate.write.as_raw_fd();
    let report_write = report.write.as_raw_fd();
    let options = usize::try_from(TRACE_OPTIONS).expect("options");
    let none: libc::c_long = 0;
    let fork_flags = libc::c_long::from(libc::SIGCHLD);
    // An orphan comes to the harness, which can then reap the tracee once its tracer is gone.
    set_subreaper(true);
    // SAFETY: the forked tracer makes only raw syscalls on memory prepared before the fork;
    // glibc's fork could block there on a lock another harness thread held.
    let tracer = unsafe { libc::fork() };
    if tracer == 0 {
        // SAFETY: as above.
        unsafe {
            let tracee = libc::syscall(libc::SYS_clone, fork_flags, none, none, none, none);
            if tracee == 0 {
                // Only the harness may open the gate: if it dies first, this read ends at EOF.
                libc::close(gate_write);
                let mut byte = 0_u8;
                libc::read(gate_read, ptr::from_mut(&mut byte).cast(), 1);
                libc::_exit(0);
            }
            let Ok(tracee) = libc::pid_t::try_from(tracee) else {
                libc::_exit(125);
            };
            let seized = tracee > 0
                && libc::ptrace(
                    libc::PTRACE_SEIZE,
                    tracee,
                    ptr::null_mut::<c_void>(),
                    ptr::without_provenance_mut::<c_void>(options),
                ) == 0;
            let error = if seized { 0 } else { errno() };
            let message = [tracee, error];
            libc::write(report_write, message.as_ptr().cast(), size_of_val(&message));
            libc::pause();
            libc::_exit(0);
        }
    }
    assert!(tracer > 0, "fork: {}", io::Error::last_os_error());
    drop(report.write);
    let mut message = [0_u8; 2 * size_of::<c_int>()];
    io::Read::read_exact(&mut fs::File::from(report.read), &mut message).expect("tracer report");
    let [p0, p1, p2, p3, e0, e1, e2, e3] = message;
    let (tracee, error) = (
        libc::pid_t::from_ne_bytes([p0, p1, p2, p3]),
        c_int::from_ne_bytes([e0, e1, e2, e3]),
    );
    assert_eq!(
        (tracee > 0, error),
        (true, 0),
        "the tracer's clone and seize of its tracee {tracee}: {}",
        io::Error::from_raw_os_error(error)
    );
    let identity = Identity {
        pid: u32::try_from(tracee).expect("pid"),
        start: start_time(tracee).expect("the held tracee's start time"),
    };
    let attach_rejected = match seize(tracee, TRACE_OPTIONS | libc::PTRACE_O_EXITKILL) {
        Ok(()) => panic!("PTRACE_SEIZE of a process another tracer owns succeeded"),
        Err(errno) => AttachRejected {
            tracee: identity,
            errno,
            tracer_pid: status_number(tracee, "TracerPid:"),
        },
    };
    // The tracer's exit detaches and reparents its tracee before the harness can reap it, so
    // the tracee's state read after `reap` is already the state the loss left.
    kill(tracer);
    let tracer_wait_status = reap(tracer);
    let tracee_tracer_pid = status_number(tracee, "TracerPid:");
    let tracee_state = ProcFile::open(tracee, "status").and_then(|status| status.status("State:"));
    write_byte(gate.write.as_raw_fd()).expect("tracee gate");
    let tracee_wait_status = reap(tracee);
    set_subreaper(false);
    (
        attach_rejected,
        TracerLost {
            tracee: identity,
            tracer: u32::try_from(tracer).expect("pid"),
            tracer_wait_status,
            tracee_tracer_pid,
            tracee_state,
            tracee_wait_status,
        },
    )
}

/// A process's `stat` and `cmdline`, opened while it lives and read after it is reaped: the
/// reads a source makes of a process gone mid-read.
fn metadata_control() -> MetadataUnread {
    // SAFETY: the forked child only pauses until it is killed.
    let pid = unsafe { libc::fork() };
    if pid == 0 {
        // SAFETY: as above.
        unsafe {
            libc::pause();
            libc::_exit(0);
        }
    }
    assert!(pid > 0, "fork: {}", io::Error::last_os_error());
    let process = Identity {
        pid: u32::try_from(pid).expect("pid"),
        start: start_time(pid).expect("the paused child's start time"),
    };
    let stat = ProcFile::open(pid, "stat").expect("the paused child's stat");
    let cmdline = ProcFile::open(pid, "cmdline").expect("the paused child's cmdline");
    kill(pid);
    reap(pid);
    MetadataUnread {
        process,
        start_time: stat.start_time(),
        command_line: cmdline.command_line(),
    }
}

fn set_subreaper(subreaper: bool) {
    // SAFETY: PR_SET_CHILD_SUBREAPER takes one flag.
    let set = unsafe { libc::prctl(libc::PR_SET_CHILD_SUBREAPER, libc::c_ulong::from(subreaper)) };
    assert_eq!(
        set,
        0,
        "PR_SET_CHILD_SUBREAPER: {}",
        io::Error::last_os_error()
    );
}

// ---------------------------------------------------------------------------------------------
// Support.

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
