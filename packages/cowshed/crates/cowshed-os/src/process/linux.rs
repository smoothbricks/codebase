//! Linux's readings of a process group, which procfs lists only to a census of `/proc`, the cgroup
//! v2 readings of a job's processes, and a pidfd for a process's exit. A process's own facts are
//! procinfo's.

use std::io;
use std::time::Duration;

use super::ExitWatch;
use procinfo::{GroupMember, running_members};

/// Every process whose group is `pgid`, exited and unreaped ones included, each by pid with the
/// fields of its stat line (`procinfo::stat_lineage`). The kernel lists a group's members to no other
/// process, so every process procfs shows is read once: one that exits before its read is no
/// member, and one that enters the group behind the walk is missed by this reading.
fn group_stats(pgid: libc::pid_t) -> io::Result<Vec<(libc::pid_t, String)>> {
    let unlisted = |error: io::Error| {
        io::Error::new(
            error.kind(),
            format!("the processes of group {pgid} cannot be listed from /proc: {error}"),
        )
    };
    let mut members = Vec::new();
    for entry in std::fs::read_dir("/proc").map_err(unlisted)? {
        // procfs names a process's directory by its pid: any other entry is no process.
        let Some(pid) = entry
            .map_err(unlisted)?
            .file_name()
            .to_str()
            .and_then(|name| name.parse::<libc::pid_t>().ok())
        else {
            continue;
        };
        let Some((lineage, fields)) = procinfo::stat_lineage(pid)? else {
            continue;
        };
        if lineage.pgid == pgid {
            members.push((pid, fields));
        }
    }
    Ok(members)
}

/// [`group_pids`] from procfs: the pids holding the group id `pgid`, exited processes included
/// (`group_stats`).
pub fn group_pids(pgid: libc::pid_t) -> io::Result<Vec<libc::pid_t>> {
    Ok(group_stats(pgid)?.into_iter().map(|(pid, _)| pid).collect())
}

/// [`group_cpu`] from procfs: the CPU the processes of group `pgid` have used, each with its
/// reaped children's, in clock ticks (`USER_HZ`, 10 ms): the `utime`, `stime`, `cutime` and
/// `cstime` of each member's stat line, which count every thread it has had and every child it
/// has reaped. Comparable only to another reading of this group, and advanced by work done
/// anywhere in it once that work fills a tick.
pub fn group_cpu(pgid: libc::pid_t) -> io::Result<u64> {
    let mut total = 0_u64;
    for (pid, fields) in group_stats(pgid)? {
        let ticks = cpu_ticks(&fields).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "read the CPU of process {pid} in group {pgid}: /proc/{pid}/stat holds no \
                     CPU times: {fields:?}"
                ),
            )
        })?;
        total = total.saturating_add(ticks);
    }
    Ok(total)
}

/// The CPU the fields of a stat line count (`procinfo::stat_lineage`): `utime`, `stime`, `cutime` and
/// `cstime`, the twelfth to the fifteenth after the command name (proc_pid_stat(5)).
fn cpu_ticks(fields: &str) -> Option<u64> {
    let mut times = fields.split_ascii_whitespace().skip(11);
    let mut total = 0_u64;
    for _ in 0..4 {
        total = total.saturating_add(times.next()?.parse().ok()?);
    }
    Some(total)
}

/// [`group_members`] from procfs: the running processes of group `pgid` ([`group_pids`]), each
/// named by its command line (`cmdline`) and its state.
pub fn group_members(pgid: libc::pid_t) -> io::Result<Vec<GroupMember>> {
    Ok(running_members(group_pids(pgid)?))
}

/// One cgroup v2 file of `cgroup`, read whole.
fn cgroup_file(cgroup: std::os::fd::BorrowedFd<'_>, name: &std::ffi::CStr) -> io::Result<String> {
    use std::io::Read as _;
    use std::os::fd::{AsRawFd as _, FromRawFd as _};
    // SAFETY: a NUL-terminated name relative to a live directory descriptor.
    let fd = unsafe {
        libc::openat(
            cgroup.as_raw_fd(),
            name.as_ptr(),
            libc::O_RDONLY | libc::O_CLOEXEC,
        )
    };
    if fd < 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: openat just returned this descriptor and nothing else owns it.
    let mut file = unsafe { std::fs::File::from_raw_fd(fd) };
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    Ok(text)
}

/// The members of `cgroup` (its `cgroup.procs`): every process in it, whatever parent it was
/// reparented to.
fn cgroup_pids(cgroup: std::os::fd::BorrowedFd<'_>) -> io::Result<Vec<libc::pid_t>> {
    cgroup_file(cgroup, c"cgroup.procs")?
        .lines()
        .map(|line| {
            line.trim().parse::<libc::pid_t>().map_err(|_| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("cgroup.procs holds {line:?}, no pid"),
                )
            })
        })
        .collect()
}

/// The `usage_usec` count of a cgroup's `cpu.stat`; `None` when no line holds one.
fn usage_usec(stat: &str) -> Option<u64> {
    stat.lines()
        .find_map(|line| line.strip_prefix("usage_usec "))
        .and_then(|value| value.trim().parse().ok())
}

/// The CPU every process of `cgroup` has used, live or exited, in microseconds (`cpu.stat`'s
/// `usage_usec`).
pub fn cgroup_cpu(cgroup: std::os::fd::BorrowedFd<'_>) -> io::Result<u64> {
    let stat = cgroup_file(cgroup, c"cpu.stat")?;
    usage_usec(&stat).ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidData,
            format!("cpu.stat holds no usage_usec: {stat:?}"),
        )
    })
}

/// The running members of `cgroup`, each named by its command line (`cmdline`) and its state.
pub fn cgroup_members(cgroup: std::os::fd::BorrowedFd<'_>) -> io::Result<Vec<GroupMember>> {
    Ok(procinfo::running_members(cgroup_pids(cgroup)?))
}

/// Send `signal` to every member of `cgroup`; a member that exited since the listing is passed
/// over.
pub fn signal_cgroup(cgroup: std::os::fd::BorrowedFd<'_>, signal: libc::c_int) -> io::Result<()> {
    for pid in cgroup_pids(cgroup)? {
        // SAFETY: plain integers; a member that already exited answers ESRCH.
        if unsafe { libc::kill(pid, signal) } != 0 {
            let error = io::Error::last_os_error();
            if error.raw_os_error() != Some(libc::ESRCH) {
                return Err(error);
            }
        }
    }
    Ok(())
}

/// This process's own cgroup v2 directory: `/proc/self/cgroup`'s unified line under
/// `/sys/fs/cgroup`. A job's, for a process running as one.
pub fn own_cgroup() -> io::Result<std::path::PathBuf> {
    let membership = std::fs::read_to_string("/proc/self/cgroup").map_err(|error| {
        io::Error::new(error.kind(), format!("read /proc/self/cgroup: {error}"))
    })?;
    let path = membership
        .lines()
        .find_map(|line| line.strip_prefix("0::"))
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::NotFound,
                format!("/proc/self/cgroup names no cgroup v2 membership: {membership:?}"),
            )
        })?;
    Ok(std::path::Path::new("/sys/fs/cgroup").join(path.trim().trim_start_matches('/')))
}

impl ExitWatch {
    /// [`Self::of`], answering `ESRCH` for a pid no process holds.
    pub fn register(pid: libc::pid_t) -> io::Result<Self> {
        use std::os::fd::FromRawFd as _;
        // SAFETY: pidfd_open takes plain integers; a non-negative answer is a new descriptor.
        let opened = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) };
        if opened < 0 {
            let error = io::Error::last_os_error();
            // A pid whose process was reaped while its process group lives on is EINVAL to
            // pidfd_open, not ESRCH: the kernel keeps the number for the group, with no process
            // to open (`pidfd_prepare` finds no thread-group leader on it). No process holds it.
            return Err(match error.raw_os_error() {
                Some(libc::EINVAL) => io::Error::from_raw_os_error(libc::ESRCH),
                _ => error,
            });
        }
        let descriptor = i32::try_from(opened).map_err(io::Error::other)?;
        // SAFETY: a fresh descriptor this function alone owns.
        Ok(Self(unsafe {
            std::os::fd::OwnedFd::from_raw_fd(descriptor)
        }))
    }

    /// The watch on the process `pidfd` names, exited or not.
    pub fn from_pidfd(pidfd: std::os::fd::OwnedFd) -> Self {
        Self(pidfd)
    }
}

impl ExitWatch {
    pub(super) fn exited(&self, within: Option<Duration>) -> io::Result<bool> {
        use std::os::fd::AsRawFd as _;

        let timeout = within.map_or(-1, crate::deadline::poll_timeout);
        let mut exited = libc::pollfd {
            fd: self.0.as_raw_fd(),
            events: libc::POLLIN,
            revents: 0,
        };
        // SAFETY: one live pollfd entry.
        let waited = unsafe { libc::poll(&mut exited, 1, timeout) };
        if waited < 0 {
            return Err(io::Error::last_os_error());
        }
        Ok(waited > 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::test_backstop::ChildBackstop as _;

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
    /// A stat line's CPU is its `utime`, `stime`, `cutime` and `cstime`, the twelfth to the
    /// fifteenth fields after the command name; a line with fewer fields names none.
    #[test]
    fn a_stat_lines_cpu_is_its_four_times() {
        assert_eq!(
            cpu_ticks(" S 3 9 9 0 -1 4194304 10 20 0 0 5 6 7 8 20 0 1 0\n"),
            Some(26)
        );
        assert_eq!(cpu_ticks(" Z 3 9 9"), None);
    }
    /// A cgroup's CPU is its `usage_usec` line's count, and a `cpu.stat` without one names none.
    #[test]
    fn a_cgroups_cpu_is_its_usage_usec() {
        let cases = [
            (
                "usage_usec 1234\nuser_usec 1000\nsystem_usec 234\n",
                Some(1234),
            ),
            ("user_usec 1000\nusage_usec 7\n", Some(7)),
            ("user_usec 1000\nsystem_usec 234\n", None),
            ("usage_usec many\n", None),
        ];
        for (stat, expected) in cases {
            assert_eq!(usage_usec(stat), expected, "{stat:?}");
        }
    }
}
