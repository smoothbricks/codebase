//! Host process facts, each read from the kernel's own record of one process or one group:
//! libproc, sysctl and libsystem_sandbox on Darwin (`macos.rs`), procfs on Linux (`linux.rs`).
//! Nothing here spawns a process or walks the whole process table, so a process stalled in the
//! kernel cannot stall the reader (specs/procinfo/01_procinfo.md).

use std::fmt;
use std::sync::LazyLock;
use std::time::Instant;

mod host_load;
#[cfg(target_os = "linux")]
mod linux;
#[cfg(target_os = "macos")]
mod macos;
#[cfg(test)]
mod test_support;

pub use host_load::HostLoad;
#[cfg(target_os = "linux")]
use linux as platform;
#[cfg(target_os = "macos")]
use macos as platform;

// Each fact is read from its platform's own kernel, selected here and nowhere else.
pub use platform::{
    child_pids, cpu_time, descriptor_name, descriptor_names, lineage, listening_ports,
    process_arguments, resident_bytes, run_state, running, sandboxed, started,
};
// A process group's members, where the kernel lists them (`proc_listpgrppids`). Linux lists a
// group only to a census of `/proc`, so procinfo offers no group facts there: cowshed's Linux job
// runner reads its own (cowshed-os) and a job's cgroup.
#[cfg(target_os = "macos")]
pub use macos::{group_cpu, group_members, group_pids};
// What a Linux census of a process group or a cgroup reads of each pid it lists: the running
// processes among the pids, and the lineage of one with its stat line.
#[cfg(target_os = "linux")]
pub use linux::{running_members, stat_lineage};

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

/// Milliseconds since this process first asked, on a clock that stops while the host sleeps:
/// Rust's `Instant`, which reads `CLOCK_UPTIME_RAW` on Darwin and `CLOCK_MONOTONIC` on Linux.
/// Neither counts host suspension, so an idle bound measured on it never expires across a sleep.
pub fn awake_time_ms() -> f64 {
    static ORIGIN: LazyLock<Instant> = LazyLock::new(Instant::now);
    ORIGIN.elapsed().as_secs_f64() * 1_000.0
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_support::{SpawnReaped as _, exited_unreaped};
    /// The state a process is read in follows its life: a live child is running, in an
    /// uninterruptible wait or asleep, an exited and unreaped one is a zombie, a reaped one is
    /// gone.
    #[test]
    fn a_childs_run_state_follows_its_life() {
        let mut child = std::process::Command::new("sleep")
            .arg("600")
            .spawn_reaped()
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
        exited_unreaped(child.id()).expect("sleep exits, unreaped");
        let exited = run_state(pid);
        child.wait().expect("reap sleep");
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
        use std::os::unix::process::CommandExt as _;

        let mut child = std::process::Command::new("sleep")
            .arg("600")
            .process_group(0)
            .spawn_reaped()
            .expect("spawn sleep");
        let pid = libc::pid_t::try_from(child.id()).expect("a pid");
        // Every read happens before any assertion, so a failed assertion leaves no sleeper.
        let started = lineage(pid);
        child.kill().expect("kill sleep");
        // Waitable means zombie; see a_childs_run_state_follows_its_life for why an exit watch is too early.
        exited_unreaped(child.id()).expect("sleep exits, unreaped");
        let exited = lineage(pid);
        child.wait().expect("reap sleep");
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
    #[cfg(target_os = "macos")]
    #[test]
    fn a_group_reads_as_its_leader_and_the_members_it_started() {
        use std::io::BufRead as _;
        use std::os::unix::process::CommandExt as _;

        let mut leader = std::process::Command::new("/bin/sh")
            .args(["-c", "sleep 600 & a=$!; sleep 600 & echo $a $!; wait"])
            .stdout(std::process::Stdio::piped())
            .process_group(0)
            .spawn_reaped()
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
        leader.wait().expect("reap the leader");
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
            .spawn_reaped()
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
        child.wait().expect("reap the child");
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
        let mut child = crate::test_support::SpawnReaped::spawn_reaped(
            std::process::Command::new("/bin/sh").args(["-c", ":"]),
        )
        .expect("spawn");
        let pid = libc::pid_t::try_from(child.id()).expect("pid");
        exited_unreaped(pid.cast_unsigned())
            .expect("wait for the exit, leaving the child unreaped");
        // SAFETY: signal 0 only checks existence.
        assert_eq!(
            unsafe { libc::kill(pid, 0) },
            0,
            "the zombie answers the null signal"
        );
        assert!(!running(pid).unwrap(), "a zombie has exited");
        child.wait().expect("reap");
        assert!(!running(pid).unwrap(), "a reaped pid has exited");
        assert!(running(libc::pid_t::try_from(std::process::id()).unwrap()).unwrap());
        // Launchd's: not this user's to inspect, and running.
        assert!(running(1).unwrap());
    }

    /// A process's children are the processes it started, as the kernel links them now: a leader
    /// that forked two sleepers lists exactly those, a sleeper lists none, and once the leader is
    /// reaped no process names its pid as a parent.
    #[test]
    fn a_process_s_children_are_the_ones_it_started() {
        use std::io::BufRead as _;

        let mut leader = std::process::Command::new("/bin/sh")
            .args(["-c", "sleep 600 & a=$!; sleep 600 & echo $a $!; wait"])
            .stdout(std::process::Stdio::piped())
            .spawn_reaped()
            .expect("spawn the leader");
        let pid = libc::pid_t::try_from(leader.id()).expect("a pid");
        let mut said = String::new();
        std::io::BufReader::new(leader.stdout.take().expect("piped stdout"))
            .read_line(&mut said)
            .expect("read the leader's line");
        let mut sleepers: Vec<libc::pid_t> = said
            .split_whitespace()
            .map(|pid| pid.parse().expect("the leader says pids"))
            .collect();
        sleepers.sort_unstable();
        let sleeper = sleepers
            .first()
            .copied()
            .expect("the leader started sleepers");
        // Every read happens before any assertion, so a failed assertion leaves no sleeper.
        let children = child_pids(pid);
        let grandchildren = child_pids(sleeper);
        for sleeper in &sleepers {
            // SAFETY: each pid is a sleeper this test's own child started and has not reaped.
            unsafe { libc::kill(*sleeper, libc::SIGKILL) };
        }
        leader.kill().expect("kill the leader");
        leader.wait().expect("reap the leader");
        let reaped = child_pids(pid);
        let mut children = children.expect("read the leader's children");
        children.sort_unstable();
        assert_eq!(children, sleepers);
        assert_eq!(
            grandchildren.expect("read a sleeper's children"),
            Vec::new()
        );
        assert_eq!(reaped.expect("read a reaped pid's children"), Vec::new());
    }

    /// The CPU this process's threads used, user and system, as getrusage(2) counts it.
    fn own_rusage() -> std::time::Duration {
        // SAFETY: an all-zero `rusage` is valid storage for the call to fill.
        let mut usage: libc::rusage = unsafe { std::mem::zeroed() };
        // SAFETY: `usage` is live storage for the call to fill.
        assert_eq!(
            unsafe { libc::getrusage(libc::RUSAGE_SELF, &raw mut usage) },
            0
        );
        let time = |value: libc::timeval| {
            std::time::Duration::from_secs(value.tv_sec.cast_unsigned())
                + std::time::Duration::from_micros(u64::try_from(value.tv_usec).expect("usec"))
        };
        time(usage.ru_utime) + time(usage.ru_stime)
    }

    /// A process's CPU time is in real time units: across a stretch of work this process's own
    /// reading advances as getrusage(2) counts it, within the two readings' resolutions (Linux
    /// stat counts clock ticks), never in Mach ticks or any other unit of the kernel's own.
    #[test]
    fn a_process_s_cpu_time_advances_as_getrusage_counts_it() {
        let me = libc::pid_t::try_from(std::process::id()).expect("a pid");
        let read_before = cpu_time(me);
        let counted_before = own_rusage();
        let mut sum = 0_u64;
        for step in 0..100_000_000_u64 {
            sum = std::hint::black_box(sum.wrapping_mul(31).wrapping_add(step));
        }
        std::hint::black_box(sum);
        let counted = own_rusage() - counted_before;
        let read = cpu_time(me).expect("read").expect("this process runs")
            - read_before.expect("read").expect("this process runs");
        // Mach ticks read as nanoseconds would be 1/41 of the time on Apple silicon; Linux counts
        // 10 ms ticks.
        assert!(
            counted >= std::time::Duration::from_millis(30),
            "{counted:?}"
        );
        assert!(
            read >= counted / 2 && read <= counted * 3 / 2 + std::time::Duration::from_millis(20),
            "procinfo read {read:?} where getrusage counted {counted:?}"
        );
    }

    /// A process's start is fixed for its life and falls in the span its spawn took, so with its
    /// pid it names one process. Its CPU time is read while it runs; exited and unreaped it keeps
    /// its start but has no CPU time left to read; reaped, it has neither.
    #[test]
    fn a_childs_start_names_it_for_its_life() {
        let before = std::time::SystemTime::now();
        let mut child = std::process::Command::new("sleep")
            .arg("600")
            .spawn_reaped()
            .expect("spawn sleep");
        let after = std::time::SystemTime::now();
        let pid = libc::pid_t::try_from(child.id()).expect("a pid");
        // Every read happens before any assertion, so a failed assertion leaves no sleeper.
        let live = (started(pid), started(pid), cpu_time(pid));
        child.kill().expect("kill sleep");
        // Waitable means zombie; see a_childs_run_state_follows_its_life for why an exit watch is too early.
        exited_unreaped(child.id()).expect("sleep exits, unreaped");
        let exited = (started(pid), cpu_time(pid));
        child.wait().expect("reap sleep");
        let reaped = (started(pid), cpu_time(pid));
        let start = live.0.expect("read").expect("a live child started");
        assert_eq!(live.1.expect("read again"), Some(start));
        // Linux counts a start in clock ticks after a boot time it gives in whole seconds.
        let slack = std::time::Duration::from_secs(1);
        assert!(
            start + slack >= before && start <= after + slack,
            "{start:?}"
        );
        assert!(live.2.expect("read").is_some());
        assert_eq!(exited.0.expect("read the zombie"), Some(start));
        assert_eq!(exited.1.expect("read the zombie"), None);
        assert_eq!(reaped.0.expect("read the reaped pid"), None);
        assert_eq!(reaped.1.expect("read the reaped pid"), None);
    }
}
