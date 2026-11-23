//! Linux's part of a run bounded by its progress and of a peer's credit: a run's members are its
//! child's process group, read from procfs, or a cgroup v2 leaf the child enters, and the peer of
//! a connected Unix stream is its `SO_PEERCRED`.

use std::io;
use std::os::fd::{AsRawFd as _, BorrowedFd};

use super::{answered, group_of, socklen_of};

/// Where a run's members are read ([`super::run_by_progress`]): who belongs to it, the CPU they
/// have used, and whom a silent run's SIGTERM reaches. Each is authoritative for every member,
/// whatever parent it was reparented to (05_gateway.md "Deadlines").
#[derive(Clone, Copy, Debug)]
pub enum Members<'a> {
    /// The child leads a process group of its own, whose members procfs shows: each process's
    /// stat line names its group.
    OwnGroup,
    /// A cgroup v2 leaf of this run alone, `path` opened as `cgroup`, which the child enters
    /// before it executes: the leaf an after-land job makes for each command.
    Leaf {
        cgroup: BorrowedFd<'a>,
        path: &'a std::path::Path,
    },
}

impl Members<'_> {
    /// The leaf a run's child enters, for an error to name.
    pub(super) fn leaf(self) -> Option<std::path::PathBuf> {
        match self {
            Self::OwnGroup => None,
            Self::Leaf { path, .. } => Some(path.to_path_buf()),
        }
    }

    /// Make `command`'s child a member before it executes anything.
    pub(super) fn admit(self, command: &mut std::process::Command) -> io::Result<()> {
        use std::os::unix::process::CommandExt as _;
        match self {
            Self::OwnGroup => {
                command.process_group(0);
                Ok(())
            }
            Self::Leaf { cgroup, .. } => {
                use std::os::fd::FromRawFd as _;
                // SAFETY: a NUL-terminated name relative to a live directory descriptor.
                let procs = unsafe {
                    libc::openat(
                        cgroup.as_raw_fd(),
                        c"cgroup.procs".as_ptr(),
                        libc::O_WRONLY | libc::O_CLOEXEC,
                    )
                };
                if procs < 0 {
                    return Err(io::Error::last_os_error());
                }
                // SAFETY: openat just returned this descriptor; the hook below owns it.
                let procs = unsafe { std::os::fd::OwnedFd::from_raw_fd(procs) };
                // SAFETY: write(2) is async-signal-safe; "0" names the writing process, the
                // child, which enters the leaf before it executes.
                unsafe {
                    command.pre_exec(move || {
                        if libc::write(procs.as_raw_fd(), c"0".as_ptr().cast(), 1) == 1 {
                            Ok(())
                        } else {
                            Err(io::Error::last_os_error())
                        }
                    });
                }
                Ok(())
            }
        }
    }

    /// The CPU the members have used so far, in a unit comparable only to another reading.
    pub(super) fn cpu(self, child: u32) -> io::Result<u64> {
        match self {
            Self::OwnGroup => crate::process::group_cpu(group_of(child)?),
            Self::Leaf { cgroup, .. } => crate::process::cgroup_cpu(cgroup),
        }
    }

    /// The running members, each by pid, command and state.
    pub(super) fn members(self, child: u32) -> io::Result<Vec<procinfo::GroupMember>> {
        match self {
            Self::OwnGroup => crate::process::group_members(group_of(child)?),
            Self::Leaf { cgroup, .. } => crate::process::cgroup_members(cgroup),
        }
    }

    /// Send SIGTERM to every member: a silent run's end.
    pub(super) fn terminate(self, child: u32) {
        match self {
            Self::OwnGroup => {
                if let Ok(group) = group_of(child) {
                    // SAFETY: `group` is the process group this process's own child leads.
                    unsafe { libc::killpg(group, libc::SIGTERM) };
                }
            }
            Self::Leaf { cgroup, .. } => {
                let _ = crate::process::signal_cgroup(cgroup, libc::SIGTERM);
            }
        }
        // The child itself, in every case: it may have left the source (setpgid, a cgroup move).
        if let Ok(pid) = libc::pid_t::try_from(child) {
            // SAFETY: `pid` is this process's own unreaped child.
            unsafe { libc::kill(pid, libc::SIGTERM) };
        }
    }
}

/// [`super::peer_pid`] by `SO_PEERCRED`.
pub(super) fn peer_pid(connection: BorrowedFd<'_>) -> io::Result<libc::pid_t> {
    let descriptor = connection.as_raw_fd();
    let mut credentials = libc::ucred {
        pid: 0,
        uid: 0,
        gid: 0,
    };
    let mut length = socklen_of::<libc::ucred>()?;
    // SAFETY: `connection` keeps `descriptor` open through the call, and the pointer and
    // length describe one initialized `ucred`.
    let read = unsafe {
        libc::getsockopt(
            descriptor,
            libc::SOL_SOCKET,
            libc::SO_PEERCRED,
            std::ptr::from_mut(&mut credentials).cast(),
            &mut length,
        )
    };
    answered::<libc::ucred>(read, length, "SO_PEERCRED")?;
    Ok(credentials.pid)
}

/// Linux's `sockaddr_un` carries no length of its own: the one passed beside it is all the
/// kernel reads.
pub(super) fn record_length(
    _address: &mut libc::sockaddr_un,
    _length: libc::socklen_t,
) -> io::Result<()> {
    Ok(())
}
