use std::ffi::OsStr;
use std::fmt;
use std::io;
use std::process::{ExitStatus, Output};
use std::time::Duration;

#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

// What every platform reads of a process, each from its own kernel (`linux.rs`: procfs;
// `macos.rs`: libproc and sysctl), selected here and nowhere else. A platform also defines
// `ExitWatch::register` and the wait under `ExitWatch::wait` and `ExitWatch::within` (a pidfd; a
// kqueue).
pub use platform::{
    descriptor_name, descriptor_names, group_cpu, group_members, group_pids, lineage,
    listening_ports, process_arguments, resident_bytes, run_state, running, sandboxed,
};
// What Linux alone has, which only Linux's own callers name: its cgroup v2 readings.
#[cfg(target_os = "linux")]
pub use linux::{cgroup_cpu, cgroup_members, own_cgroup, signal_cgroup};

/// A prepared replacement for a controller's standard streams after its command inherits them.
/// Preparation happens before spawn. Release happens before acknowledging the handoff on a
/// separate diagnostic channel: from that point the controller must never write to stdio.
/// Keeping inert descriptors in 0..=2 prevents later opens from reusing those reserved numbers.
#[cfg(unix)]
pub struct StdioRelease(std::fs::File);

#[cfg(unix)]
impl StdioRelease {
    pub fn prepare() -> io::Result<Self> {
        use std::os::fd::{AsRawFd as _, FromRawFd as _};
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .open("/dev/null")?;
        // A caller may already have closed a standard stream. Keep the backing descriptor
        // outside the reserved range, so dropping it cannot undo the replacement below.
        let file = if file.as_raw_fd() < 3 {
            // SAFETY: duplicate the open descriptor into an unused number at least 3.
            let descriptor = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_DUPFD_CLOEXEC, 3) };
            if descriptor == -1 {
                return Err(io::Error::last_os_error());
            }
            // SAFETY: fcntl returned a new descriptor owned exclusively by this value.
            unsafe { std::fs::File::from_raw_fd(descriptor) }
        } else {
            file
        };
        Ok(Self(file))
    }

    /// Relinquish this process's copies, not the copies its spawned command inherited.
    /// The caller must retain and report an error through its separate diagnostic channel.
    pub fn release(self) -> io::Result<()> {
        use std::os::fd::AsRawFd as _;
        for descriptor in 0..=2 {
            // SAFETY: the backing descriptor is open and outside the standard-stream range.
            if unsafe { libc::dup2(self.0.as_raw_fd(), descriptor) } == -1 {
                return Err(io::Error::other(format!(
                    "cannot relinquish standard descriptor {descriptor}: {}",
                    io::Error::last_os_error()
                )));
            }
        }
        Ok(())
    }
}

/// A process's executable path and its byte-exact argument vector, as the kernel reports them
/// ([`process_arguments`]). The executable is the kernel's own record of the image, never
/// `argv[0]`: on Darwin the path exec was given, on Linux the resolved path of the file exec'd.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ProcessArguments {
    pub executable: Vec<u8>,
    pub argv: Vec<Vec<u8>>,
}

/// What the kernel shows a process doing, as `ps` names its state. A wait on a process that gave
/// up names it, so the reader tells a process stuck in the kernel apart from one merely running
/// long: an `Uninterruptible` one is in a kernel call no signal ends, SIGKILL included, until
/// that call returns.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunState {
    Running,
    Sleeping,
    Uninterruptible,
    Stopped,
    /// Exited, not yet reaped by its parent.
    Zombie,
    /// No process has the pid.
    Gone,
    /// The kernel's answer could not be read; why.
    Unread(String),
}

impl fmt::Display for RunState {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => formatter.write_str("running"),
            Self::Sleeping => formatter.write_str("sleeping"),
            Self::Uninterruptible => formatter.write_str("in an uninterruptible kernel wait"),
            Self::Stopped => formatter.write_str("stopped"),
            Self::Zombie => formatter.write_str("exited, not reaped"),
            Self::Gone => formatter.write_str("gone"),
            Self::Unread(why) => write!(formatter, "state unread: {why}"),
        }
    }
}

/// Where a process descends from, as the kernel's own record of it says: its parent, its group,
/// and whether it has exited unreaped (07_api.md, "Process lineage").
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ProcessLineage {
    pub pid: libc::pid_t,
    pub ppid: libc::pid_t,
    pub pgid: libc::pid_t,
    /// Exited, not yet reaped by its parent.
    pub zombie: bool,
}

/// One process of a group, named as a run that went silent names it: its pid, its command and
/// what the kernel shows it doing.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GroupMember {
    pub pid: libc::pid_t,
    pub command: String,
    pub state: RunState,
}

impl fmt::Display for GroupMember {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "pid {} `{}` ({})",
            self.pid, self.command, self.state
        )
    }
}

/// A process's exit, watched from a moment it ran. The kernel reports it only once the process
/// has released what it held, every descriptor included (measured on Darwin: a listener it alone
/// held is detached from its socket file by NOTE_EXIT; Linux makes a pidfd readable in
/// `exit_notify`, after `exit_files` and the deferred closes it queued). On Darwin it is a kqueue
/// with the process's `NOTE_EXIT` registered once; on Linux, the process's pidfd.
#[derive(Debug)]
pub struct ExitWatch(std::os::fd::OwnedFd);

impl ExitWatch {
    /// Watch the exit of the process `pid` names now, from this moment, so a later process given
    /// the same pid is never mistaken for it; `None` when no process holds `pid`, or (Darwin)
    /// the one that does has begun to exit. For a process this one does not own, such as a
    /// launchd agent's: taken before it is signalled, its exit is never missed.
    pub fn of(pid: u32) -> io::Result<Option<Self>> {
        let pid = libc::pid_t::try_from(pid).map_err(io::Error::other)?;
        match Self::register(pid) {
            Ok(watch) => Ok(Some(watch)),
            Err(error) if error.raw_os_error() == Some(libc::ESRCH) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Block until the process has exited.
    pub fn wait(&self) -> io::Result<()> {
        self.exited(None).map(|_| ())
    }

    /// Block until the process has exited or `within` passes; whether it exited.
    pub fn within(&self, within: Duration) -> io::Result<bool> {
        self.exited(Some(within))
    }
}

impl std::os::fd::AsRawFd for ExitWatch {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        std::os::fd::AsRawFd::as_raw_fd(&self.0)
    }
}

/// What `lsof -F di` names one descriptor of this process by: its `d` field, the kernel address
/// Darwin names a pipe or a socket by, or its `i` field, the inode of anything else, and of every
/// descriptor on Linux, where a pipe or a socket is an inode of its own filesystem. Every process
/// that shares the descriptor's open file description shows the same name, so a caller that read
/// it once finds every later holder of that description in one census ([`descriptor_names`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DescriptorName {
    /// lsof's `d` field: the kernel's address of a pipe end, of a TCP socket's control block, or
    /// of any other socket's protocol control block (Darwin).
    Device(u64),
    /// lsof's `i` field: the inode `fstat(2)` reports.
    Inode(u64),
    /// The descriptor is not open.
    Closed,
    /// Open, of a Darwin kind this reading names by neither field (a kqueue, a POSIX semaphore or
    /// shared memory object, ...): its `PROX_FDTYPE_*` number.
    Unnamed { fd_type: u32 },
}

/// Why another process's descriptors were not read ([`descriptor_names`]).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Unread {
    /// The process exited, or no process has the pid, before they were read.
    Gone,
    /// The kernel withholds them from this user: another user's process, as a census not run as
    /// root meets it.
    Withheld,
}

/// Which names a reading of another process's descriptors needs ([`descriptor_names`]). On
/// Darwin each name costs one `proc_pidfdinfo`, so a census matching only pipes and sockets reads
/// no file and one matching only files reads no pipe or socket. Linux names every descriptor by
/// its inode and has no address to read.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct NameKinds {
    /// Pipes and sockets, named by the address in lsof's `d` field (Darwin).
    pub devices: bool,
    /// Whatever is named by the inode in lsof's `i` field: a file on Darwin, every descriptor on
    /// Linux.
    pub inodes: bool,
}

/// How a child process terminated, without collapsing signals into a synthetic exit code.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProcessStatus {
    Exit(i32),
    Signal(i32),
    Unknown,
}

impl ProcessStatus {
    pub fn succeeded(self) -> bool {
        self == Self::Exit(0)
    }
}

impl Default for ProcessStatus {
    fn default() -> Self {
        Self::Exit(0)
    }
}

impl From<ExitStatus> for ProcessStatus {
    fn from(status: ExitStatus) -> Self {
        if let Some(code) = status.code() {
            return Self::Exit(code);
        }
        #[cfg(unix)]
        {
            use std::os::unix::process::ExitStatusExt;
            if let Some(signal) = status.signal() {
                return Self::Signal(signal);
            }
        }
        Self::Unknown
    }
}

impl fmt::Display for ProcessStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Exit(code) => write!(f, "exit status {code}"),
            Self::Signal(signal) => write!(f, "signal {signal}"),
            Self::Unknown => f.write_str("unknown termination status"),
        }
    }
}

#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CommandOutput {
    pub status: ProcessStatus,
    pub stdout: Vec<u8>,
    pub stderr: Vec<u8>,
}

impl CommandOutput {
    pub fn success(stdout: impl Into<Vec<u8>>) -> Self {
        Self {
            status: ProcessStatus::Exit(0),
            stdout: stdout.into(),
            stderr: Vec::new(),
        }
    }

    pub fn failure(status: i32, stderr: impl Into<Vec<u8>>) -> Self {
        Self::failure_with_streams(ProcessStatus::Exit(status), Vec::new(), stderr)
    }

    pub fn failure_with_streams(
        status: ProcessStatus,
        stdout: impl Into<Vec<u8>>,
        stderr: impl Into<Vec<u8>>,
    ) -> Self {
        debug_assert!(!status.succeeded());
        Self {
            status,
            stdout: stdout.into(),
            stderr: stderr.into(),
        }
    }

    pub fn succeeded(&self) -> bool {
        self.status.succeeded()
    }
}

impl From<Output> for CommandOutput {
    fn from(output: Output) -> Self {
        Self {
            status: output.status.into(),
            stdout: output.stdout,
            stderr: output.stderr,
        }
    }
}

/// Write a subprocess failure without decoding arbitrary output bytes or allocating a joined
/// command string. Valid UTF-8 stays readable; invalid bytes are escaped individually, so the
/// diagnostic retains every byte instead of replacing it with U+FFFD.
pub fn fmt_command_failure<A: AsRef<OsStr>>(
    f: &mut fmt::Formatter<'_>,
    operation: &str,
    program: &OsStr,
    args: &[A],
    output: &CommandOutput,
) -> fmt::Result {
    write!(f, "{operation} failed: ")?;
    fmt_command(f, program, args)?;
    write!(
        f,
        ", {}; stdout: {}; stderr: {}",
        output.status,
        DiagnosticBytes(&output.stdout),
        DiagnosticBytes(&output.stderr)
    )
}

pub fn fmt_command_spawn<A: AsRef<OsStr>>(
    f: &mut fmt::Formatter<'_>,
    program: &OsStr,
    args: &[A],
    source: &std::io::Error,
) -> fmt::Result {
    f.write_str("could not run ")?;
    fmt_command(f, program, args)?;
    write!(f, ": {source}")
}

/// `executable "<program>", argv [<args>]`: the command as every diagnostic names it.
pub fn fmt_command<A: AsRef<OsStr>>(
    f: &mut fmt::Formatter<'_>,
    program: &OsStr,
    args: &[A],
) -> fmt::Result {
    write!(f, "executable {program:?}, argv [")?;
    fmt_argv(f, args)?;
    f.write_str("]")
}

fn fmt_argv<A: AsRef<OsStr>>(f: &mut fmt::Formatter<'_>, args: &[A]) -> fmt::Result {
    for (index, arg) in args.iter().enumerate() {
        if index != 0 {
            f.write_str(", ")?;
        }
        write!(f, "{:?}", arg.as_ref())?;
    }
    Ok(())
}

struct DiagnosticBytes<'a>(&'a [u8]);

impl fmt::Display for DiagnosticBytes<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let bytes = self.0.trim_ascii();
        if bytes.is_empty() {
            return f.write_str("<empty>");
        }

        let mut remaining = bytes;
        while !remaining.is_empty() {
            match std::str::from_utf8(remaining) {
                Ok(text) => {
                    f.write_str(text)?;
                    break;
                }
                Err(error) => {
                    let valid = error.valid_up_to();
                    if valid != 0 {
                        let prefix = std::str::from_utf8(&remaining[..valid])
                            .expect("Utf8Error valid prefix must decode");
                        f.write_str(prefix)?;
                    }
                    let invalid = error
                        .error_len()
                        .unwrap_or_else(|| remaining.len().saturating_sub(valid));
                    for byte in &remaining[valid..valid + invalid] {
                        write!(f, "\\x{byte:02x}")?;
                    }
                    remaining = &remaining[valid + invalid..];
                }
            }
        }
        Ok(())
    }
}

/// A process of this user that holds a file open: a task database, a volume a release was refused.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Holder {
    pub pid: i32,
    pub command: String,
}

impl fmt::Display for Holder {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "pid {} ({})", self.pid, self.command)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_backstop::ChildBackstop as _;

    /// The state a process is read in follows its life: a live child is running, in an
    /// uninterruptible wait or asleep, an exited and unreaped one is a zombie, a reaped one is
    /// gone.
    #[test]
    fn a_childs_run_state_follows_its_life() {
        use crate::fork_lock::Spawn as _;

        let mut child = std::process::Command::new("sleep")
            .arg("600")
            .spawn_locked()
            .expect("spawn sleep");
        let pid = libc::pid_t::try_from(child.id()).expect("a pid");
        // The child is read the instant it has started: asleep in nanosleep once it has, and
        // until then running its start or in an uninterruptible kernel wait during it. Every state
        // is read before any is asserted, so a failed assertion leaves no sleeper running.
        let started = run_state(pid);
        child.kill().expect("kill sleep");
        // waitid(WNOWAIT) answers once the child is waitable, i.e. a zombie, and leaves it unreaped.
        // An exit watch is too early: xnu posts NOTE_EXIT in proc_exit before the process joins
        // the zombie list, so a read right after it can find no process at all.
        crate::test_backstop::waitid(libc::P_PID, child.id(), libc::WEXITED | libc::WNOWAIT)
            .expect("sleep exits, unreaped");
        let exited = run_state(pid);
        child.wait_bounded().expect("reap sleep");
        let reaped = run_state(pid);
        assert!(
            matches!(
                started,
                RunState::Running | RunState::Sleeping | RunState::Uninterruptible
            ),
            "{started}"
        );
        assert_eq!(exited, RunState::Zombie);
        assert_eq!(reaped, RunState::Gone);
    }

    /// A process's lineage is the kernel's own record of it: a child names this process as its
    /// parent and leads the group it was started in; exited and unreaped it is a zombie that
    /// still names its parent; reaped, no process holds its pid.
    #[test]
    fn a_childs_lineage_follows_its_life() {
        use crate::fork_lock::Spawn as _;
        use std::os::unix::process::CommandExt as _;

        let mut child = std::process::Command::new("sleep")
            .arg("600")
            .process_group(0)
            .spawn_locked()
            .expect("spawn sleep");
        let pid = libc::pid_t::try_from(child.id()).expect("a pid");
        // Every read happens before any assertion, so a failed assertion leaves no sleeper.
        let started = lineage(pid);
        child.kill().expect("kill sleep");
        // Waitable means zombie; see a_childs_run_state_follows_its_life for why an exit watch is too early.
        crate::test_backstop::waitid(libc::P_PID, child.id(), libc::WEXITED | libc::WNOWAIT)
            .expect("sleep exits, unreaped");
        let exited = lineage(pid);
        child.wait_bounded().expect("reap sleep");
        let reaped = lineage(pid);
        // SAFETY: getpid has no preconditions.
        let parent = unsafe { libc::getpid() };
        assert_eq!(
            started.expect("read the live child"),
            Some(ProcessLineage {
                pid,
                ppid: parent,
                pgid: pid,
                zombie: false
            })
        );
        let exited = exited
            .expect("read the exited child")
            .expect("an unreaped child still holds its pid");
        assert!(exited.zombie, "{exited:?}");
        assert_eq!(exited.ppid, parent, "{exited:?}");
        assert_eq!(reaped.expect("read the reaped pid"), None);
    }

    /// A run's group, read from the leader the caller holds: the leader and the two sleepers it
    /// forked, by the pids the leader says once both exist.
    #[test]
    fn a_group_reads_as_its_leader_and_the_members_it_started() {
        use crate::fork_lock::Spawn as _;
        use std::io::BufRead as _;
        use std::os::unix::process::CommandExt as _;

        let mut leader = std::process::Command::new("/bin/sh")
            .args(["-c", "sleep 600 & a=$!; sleep 600 & echo $a $!; wait"])
            .stdout(std::process::Stdio::piped())
            .process_group(0)
            .spawn_locked()
            .expect("spawn the group's leader");
        let pgid = libc::pid_t::try_from(leader.id()).expect("a pid");
        let mut said = String::new();
        std::io::BufReader::new(leader.stdout.take().expect("piped stdout"))
            .read_line(&mut said)
            .expect("read the leader's line");
        let members = group_members(pgid);
        let cpu = group_cpu(pgid);
        // SAFETY: `pgid` is the group this test's own child leads.
        unsafe { libc::killpg(pgid, libc::SIGKILL) };
        leader.wait_bounded().expect("reap the leader");
        let mut expected: Vec<libc::pid_t> = said
            .split_whitespace()
            .map(|pid| pid.parse().expect("the leader says pids"))
            .collect();
        expected.push(pgid);
        expected.sort_unstable();
        let mut read: Vec<libc::pid_t> = members
            .expect("read the group")
            .into_iter()
            .map(|member| member.pid)
            .collect();
        read.sort_unstable();
        assert_eq!(read, expected);
        cpu.expect("read the group's CPU");
    }

    /// Another process's descriptors are named as that process would name them itself: a child
    /// handed this process's pipe end as its stdout shows that end's name at 1. Nothing is read when
    /// no kind is asked for, a reaped child is gone, and pid 1 (launchd or init), another user's
    /// process, is withheld unless this process is root.
    #[test]
    fn another_processs_descriptors_are_named_as_it_names_them() {
        use std::os::fd::AsRawFd as _;

        use crate::fork_lock::Spawn as _;

        let all = NameKinds {
            devices: true,
            inodes: true,
        };
        let (_read, write) = std::io::pipe().expect("pipe");
        let name = descriptor_name(write.as_raw_fd()).expect("read");
        let mut child = std::process::Command::new("/bin/sh")
            .args(["-c", "read _"])
            .stdin(std::process::Stdio::piped())
            .stdout(write)
            .spawn_locked()
            .expect("spawn a child");
        let pid = libc::pid_t::try_from(child.id()).expect("a pid");
        let named = descriptor_names(pid, all)
            .expect("read")
            .expect("this process's own child is readable");
        assert!(named.contains(&(1, name)), "{named:?}");
        let none = NameKinds {
            devices: false,
            inodes: false,
        };
        assert_eq!(descriptor_names(pid, none).expect("read"), Ok(Vec::new()));
        drop(child.stdin.take());
        child.wait_bounded().expect("reap the child");
        assert_eq!(descriptor_names(pid, all).expect("read"), Err(Unread::Gone));
        // SAFETY: geteuid has no failure and touches no memory.
        let root = unsafe { libc::geteuid() } == 0;
        let init = descriptor_names(1, all).expect("read");
        assert_eq!(init.is_ok(), root, "{init:?}");
        assert!(root || init == Err(Unread::Withheld), "{init:?}");
    }

    /// An exited child its parent has not reaped is a zombie: the null signal still reaches it,
    /// and it has exited all the same. Reaped, it is gone. This process itself runs.
    #[test]
    fn a_zombie_has_exited_though_the_null_signal_still_reaches_it() {
        // `/bin/sh` is the one program path every supported host has: NixOS keeps no `/usr/bin/true`.
        let mut child = crate::fork_lock::Spawn::spawn_locked(
            std::process::Command::new("/bin/sh").args(["-c", ":"]),
        )
        .expect("spawn");
        let pid = libc::pid_t::try_from(child.id()).expect("pid");
        crate::test_backstop::waitid(
            libc::P_PID,
            pid.cast_unsigned(),
            libc::WEXITED | libc::WNOWAIT,
        )
        .expect("wait for the exit, leaving the child unreaped");
        // SAFETY: signal 0 only checks existence.
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            0,
            "the zombie answers the null signal"
        );
        assert!(!running(pid).unwrap(), "a zombie has exited");
        child.wait_bounded().expect("reap");
        assert!(!running(pid).unwrap(), "a reaped pid has exited");
        assert!(running(libc::pid_t::try_from(std::process::id()).unwrap()).unwrap());
        // Launchd's: not this user's to inspect, and running.
        assert!(running(1).unwrap());
    }

    #[test]
    fn diagnostic_bytes_trim_edges_and_preserve_invalid_bytes() {
        assert_eq!(DiagnosticBytes(b"  readable\n").to_string(), "readable");
        assert_eq!(
            DiagnosticBytes(b"\nleft\xffright\x80\n").to_string(),
            "left\\xffright\\x80"
        );
        assert_eq!(DiagnosticBytes(b" \t\n").to_string(), "<empty>");
    }

    #[test]
    fn command_failure_and_spawn_share_argv_rendering() {
        let args = ["--flag", "value with space"];
        struct Failure<'a>(&'a [&'a str]);
        impl fmt::Display for Failure<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt_command_failure(
                    f,
                    "copy",
                    OsStr::new("diskutil"),
                    self.0,
                    &CommandOutput::failure(1, "denied"),
                )
            }
        }
        struct Spawn<'a>(&'a [&'a str]);
        impl fmt::Display for Spawn<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                fmt_command_spawn(
                    f,
                    OsStr::new("diskutil"),
                    self.0,
                    &std::io::Error::from_raw_os_error(2),
                )
            }
        }
        let argv = r#"argv ["--flag", "value with space"]"#;
        assert!(Failure(&args).to_string().contains(argv));
        assert!(Spawn(&args).to_string().contains(argv));
    }
}
