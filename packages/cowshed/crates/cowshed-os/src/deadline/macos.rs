//! Darwin's part of a run bounded by its progress and of a peer's credit: a run's members are its
//! child's process group, which `proc_listpgrppids` lists, and the peer of a connected Unix
//! stream is its `LOCAL_PEERPID`.

use std::io;
use std::os::fd::{AsRawFd as _, BorrowedFd};

use super::{answered, group_of, socklen_of};

/// Where a run's members are read ([`super::run_by_progress`]): who belongs to it, the CPU they
/// have used, and whom a silent run's SIGTERM reaches. Each is authoritative for every member,
/// whatever parent it was reparented to (05_gateway.md "Deadlines").
#[derive(Clone, Copy, Debug)]
pub enum Members<'a> {
    /// The child leads a process group of its own, which Darwin lists (`proc_listpgrppids`).
    OwnGroup,
    /// No source borrows on this platform: the lifetime's uninhabited carrier.
    #[doc(hidden)]
    Unborrowed(std::convert::Infallible, std::marker::PhantomData<&'a ()>),
}

impl Members<'_> {
    /// The leaf a run's child enters, for an error to name.
    pub(super) fn leaf(self) -> Option<std::path::PathBuf> {
        match self {
            Self::OwnGroup => None,
            Self::Unborrowed(never, _) => match never {},
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
            Self::Unborrowed(never, _) => match never {},
        }
    }

    /// The CPU the members have used so far, in a unit comparable only to another reading.
    pub(super) fn cpu(self, child: u32) -> io::Result<u64> {
        match self {
            Self::OwnGroup => crate::process::group_cpu(group_of(child)?),
            Self::Unborrowed(never, _) => match never {},
        }
    }

    /// The running members, each by pid, command and state.
    pub(super) fn members(self, child: u32) -> io::Result<Vec<crate::process::GroupMember>> {
        match self {
            Self::OwnGroup => crate::process::group_members(group_of(child)?),
            Self::Unborrowed(never, _) => match never {},
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
            Self::Unborrowed(never, _) => match never {},
        }
        // The child itself, in every case: it may have left the source (setpgid, a cgroup move).
        if let Ok(pid) = libc::pid_t::try_from(child) {
            // SAFETY: `pid` is this process's own unreaped child.
            unsafe { libc::kill(pid, libc::SIGTERM) };
        }
    }
}

/// [`super::peer_pid`] by `LOCAL_PEERPID`.
pub(super) fn peer_pid(connection: BorrowedFd<'_>) -> io::Result<libc::pid_t> {
    let descriptor = connection.as_raw_fd();
    let mut pid: libc::pid_t = 0;
    let mut length = socklen_of::<libc::pid_t>()?;
    // SAFETY: `connection` keeps `descriptor` open through the call, and the pointer and
    // length describe one initialized `pid_t`.
    let read = unsafe {
        libc::getsockopt(
            descriptor,
            libc::SOL_LOCAL,
            libc::LOCAL_PEERPID,
            std::ptr::from_mut(&mut pid).cast(),
            &mut length,
        )
    };
    answered::<libc::pid_t>(read, length, "LOCAL_PEERPID")?;
    Ok(pid)
}

/// Darwin's `sockaddr_un` carries its own length (`sun_len`), beside the one passed with it.
pub(super) fn record_length(
    address: &mut libc::sockaddr_un,
    length: libc::socklen_t,
) -> io::Result<()> {
    address.sun_len = u8::try_from(length).map_err(io::Error::other)?;
    Ok(())
}
